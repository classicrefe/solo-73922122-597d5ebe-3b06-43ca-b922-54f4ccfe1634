"""分区补传（记录仪重传波形）的接收、校验、暂存与原子发布。

设计要点（与清理/保全的并发语义配套）：

- **锁外接收与写文件**：HTTP 请求体读取、JSONL 校验、临时文件写入/fsync
  全部在数据库写锁之外完成；只有最后的发布动作进入一个很短的
  ``BEGIN IMMEDIATE`` 事务。
- **原子发布**：在同一个写事务内完成 ① 最终文件 rename（文件内容早已落盘且
  fsync）② 分区索引行插入 ③ 与同车、时间闭区间重叠的全部生效保全建立关联。
  因此查询/清理要么看不到这个分区，要么看到完整的“文件 + 索引 + 保全关联”。
- **幂等重放**：同 ID、同元数据（车号/到期时间）、同原文（SHA-256）只返回当前
  状态，不改文件、不新建；内容或元数据不同则 409。cleaned 行是墓碑，任何
  精确重放都不会重建它。
- **崩溃安全**：未发布的文件只在 staging 目录；发布前 rename 到 partitions/
  后若事务回滚，会立即删除该文件；进程在 rename 与 COMMIT 之间崩溃时，重启
  reconcile 会把无索引行的文件当作孤儿清掉。已 COMMIT 的数据（索引 + 文件 +
  保全关联）在重启后完整保留。
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, status as http_status
from starlette.datastructures import UploadFile

from . import config
from .db import immediate_txn, read_conn
from .models import PartitionUploadResult
from .timeutil import dt_to_ms, ms_to_iso, now_ms, to_utc
from .views import load_partition_row, partition_out, preservation_ids

# 分区 ID 同时用作文件名：拒绝路径分隔符/控制字符与 "."/".."，其余字符
# （包括连字符、点、冒号、中文等）按原样保留，避免对业务命名过度限制。
_INVALID_PARTITION_CHARS = re.compile(r"[/\\\x00-\x1f\x7f]")
_PARTITION_ID_MAX_LEN = 128
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# JSON envelope 需要容纳约 1 MiB 的 JSONL 字符串与少量元数据；超出后仍按
# 内容本身的 1 MiB 上限做精确检查。
_JSON_BODY_CAP = config.MAX_UPLOAD_BYTES + 512 * 1024

_PARTITION_NAMES = ("partition_id", "id", "partition")
_VEHICLE_NAMES = ("vehicle_id", "vehicle", "train_id")
_EXPIRES_NAMES = ("expires_at", "expiry", "expires", "expire_at", "expiration")
_CONTENT_NAMES = ("jsonl", "content", "data", "raw_jsonl", "ndjson", "body")
_FILE_FIELD_NAMES = ("file", "jsonl_file", "upload", "data_file")


def _fail(detail: str, code: int = http_status.HTTP_422_UNPROCESSABLE_ENTITY) -> None:
    raise HTTPException(status_code=code, detail=detail)


# ---------------------------------------------------------------- 元数据校验


def clean_partition_id(raw: Any) -> str:
    if not isinstance(raw, str) or not raw.strip():
        _fail("partition_id 必须为非空字符串")
    pid = raw.strip()
    if pid in (".", "..") or _INVALID_PARTITION_CHARS.search(pid):
        _fail("partition_id 不得包含路径分隔符、控制字符，也不能是 '.' 或 '..'")
    if len(pid) > _PARTITION_ID_MAX_LEN:
        _fail(f"partition_id 过长（最多 {_PARTITION_ID_MAX_LEN} 字符）")
    return pid


def clean_vehicle_id(raw: Any) -> str:
    if not isinstance(raw, str) or not raw.strip():
        _fail("vehicle_id 必须为非空字符串")
    vid = raw.strip()
    if len(vid) > 256:
        _fail("vehicle_id 过长（最多 256 字符）")
    return vid


def parse_datetime_text(raw: Any, field: str = "expires_at") -> datetime:
    if not isinstance(raw, str) or not raw.strip():
        _fail(f"{field} 必须为非空 ISO 8601 时间字符串")
    text = raw.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        _fail(f"{field} 不是合法的 ISO 8601 时间: {raw!r}")
    return to_utc(dt)


def _to_us(dt: datetime) -> int:
    delta = to_utc(dt) - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


# ---------------------------------------------------------------- JSONL 校验


@dataclass(frozen=True)
class ParsedJsonl:
    start_ms: int
    end_ms: int
    sample_count: int


def parse_jsonl(content: bytes, vehicle_id: str) -> ParsedJsonl:
    """校验并推导时段/条数；不重写原文（原文将逐字节落盘并按原文哈希）。"""
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        _fail(f"JSONL 必须为 UTF-8 编码: {exc}")
    if text.startswith("﻿"):  # 容忍 UTF-8 BOM，仅影响解析，不改原文
        text = text[1:]

    count = 0
    prev_us: int | None = None
    first_dt: datetime | None = None
    last_dt: datetime | None = None

    for lineno, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as exc:
            _fail(f"第 {lineno} 行不是合法 JSON: {exc.msg}")
        if not isinstance(rec, dict):
            _fail(f"第 {lineno} 行必须是 JSON 对象")

        ts = rec.get("ts")
        if not isinstance(ts, str) or not ts.strip():
            _fail(f"第 {lineno} 行缺少字符串字段 ts")
        try:
            dt = parse_datetime_text(ts, "ts")
        except HTTPException as exc:
            _fail(f"第 {lineno} 行 {exc.detail}")

        sample_vehicle = rec.get("vehicle_id")
        if not isinstance(sample_vehicle, str) or sample_vehicle != vehicle_id:
            _fail(
                f"第 {lineno} 行 vehicle_id 与补传车号不一致："
                f"期望 {vehicle_id!r}，实际 {sample_vehicle!r}"
            )

        for axis in ("ax", "ay", "az"):
            value = rec.get(axis)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                _fail(f"第 {lineno} 行 {axis} 必须是数值")
            try:
                numeric = float(value)
            except (OverflowError, ValueError):
                _fail(f"第 {lineno} 行 {axis} 不是有限数值")
            if not math.isfinite(numeric):
                _fail(f"第 {lineno} 行 {axis} 必须是有限数值（禁止 NaN/Infinity）")

        ts_us = _to_us(dt)
        if prev_us is not None and ts_us <= prev_us:
            _fail(f"第 {lineno} 行时间戳必须严格递增（不得重复或倒退）")
        prev_us = ts_us
        if first_dt is None:
            first_dt = dt
        last_dt = dt
        count += 1

    if count == 0:
        _fail("JSONL 不得为空（至少需要一个有效样本）")
    assert first_dt is not None and last_dt is not None
    return ParsedJsonl(
        start_ms=dt_to_ms(first_dt),
        end_ms=dt_to_ms(last_dt),
        sample_count=count,
    )


# ---------------------------------------------------------------- staging


def _check_content_size(content: bytes) -> None:
    if len(content) > config.MAX_UPLOAD_BYTES:
        _fail(
            f"JSONL 大小 {len(content)} 字节超过上限 {config.MAX_UPLOAD_BYTES} 字节（1 MiB）",
            http_status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        )
    if len(content) == 0:
        _fail("JSONL 不得为空（至少需要一个有效样本）")


def _staging_dir() -> Path:
    d = config.DATA_DIR / config.STAGING_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def stage_content(content: bytes) -> Path:
    """把已接收的原文写入 staging 临时文件并 fsync；调用时不得持有数据库写锁。"""
    staging = _staging_dir()
    path = staging / f"upload-{uuid.uuid4().hex}.jsonl"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
    except BaseException:
        try:
            path.unlink()
        except OSError:
            pass
        raise
    return path


def cleanup_stale_staging() -> list[str]:
    """启动时清理长时间未被发布的 staging 残留。

    只按年龄清理，绝不按进程名清理：另一 API 进程可能正在同一数据卷上传，
    其未完成文件不能被误删。
    """
    removed: list[str] = []
    staging = config.DATA_DIR / config.STAGING_SUBDIR
    if not staging.is_dir():
        return removed
    now = time.time()
    for path in sorted(staging.glob("upload-*.jsonl")):
        try:
            if now - path.stat().st_mtime <= config.STAGING_TTL_SECONDS:
                continue
            path.unlink()
            removed.append(str(path.relative_to(config.DATA_DIR)))
        except FileNotFoundError:
            continue
        except OSError:
            continue
    return removed


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


# ---------------------------------------------------------------- 原子发布


def _same_upload(row, vehicle_id: str, expires_ms: int, content_hash: str) -> bool:
    return (
        row["vehicle_id"] == vehicle_id
        and int(row["expires_ms"]) == int(expires_ms)
        and row["content_hash"] == content_hash
    )


def _conflict_detail(row, vehicle_id: str, expires_ms: int, content_hash: str) -> str:
    pid = row["partition_id"]
    if row["status"] == "cleaned":
        return (
            f"分区 {pid} 已回收（cleaned），不得重建；"
            "仅同 ID + 同元数据 + 同原文的幂等重放可查看当前状态"
        )
    if row["vehicle_id"] != vehicle_id or int(row["expires_ms"]) != int(expires_ms):
        return f"分区 {pid} 已存在且元数据不同（车号或到期时间不一致），拒绝覆盖"
    if row["content_hash"] != content_hash:
        return f"分区 {pid} 已存在且原文内容不同（SHA-256 不一致），拒绝覆盖"
    return f"分区 {pid} 已存在，拒绝重复发布"


def publish_upload(
    *,
    partition_id: str,
    vehicle_id: str,
    expires_ms: int,
    parsed: ParsedJsonl,
    content_hash: str,
    content_bytes: int,
    staged_path: Path,
) -> tuple[bool, bool]:
    """在短写事务内原子发布。返回 ``(created, replayed)``。

    文件 rename 发生在写锁内、COMMIT 之前；写事务期间清理无法认领该分区，
    因而不会出现“索引已可见但保全关联还没写入”而被误删的窗口。
    """
    rel_path = f"{config.PARTITIONS_SUBDIR}/{partition_id}.jsonl"
    final_path = config.DATA_DIR / rel_path

    with immediate_txn() as conn:
        existing = conn.execute(
            "SELECT * FROM partitions WHERE partition_id = ?", (partition_id,)
        ).fetchone()
        if existing is not None:
            if _same_upload(existing, vehicle_id, expires_ms, content_hash):
                return False, True
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=_conflict_detail(existing, vehicle_id, expires_ms, content_hash),
            )

        final_path.parent.mkdir(parents=True, exist_ok=True)
        renamed = False
        try:
            # 原子替换：最终路径上若有崩溃遗留的无索引孤儿文件，会被完整新文件覆盖。
            os.replace(staged_path, final_path)
            # staging 用 0600 防并发误触；发布到共享分区目录后放宽到与种子文件一致。
            try:
                os.chmod(final_path, 0o644)
            except OSError:
                pass
            renamed = True
            conn.execute(
                "INSERT INTO partitions "
                "(partition_id, vehicle_id, start_ms, end_ms, expires_ms, file_path, "
                " sample_count, status, file_materialized, content_hash, content_bytes, "
                " source, created_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 1, ?, ?, 'upload', ?)",
                (
                    partition_id,
                    vehicle_id,
                    parsed.start_ms,
                    parsed.end_ms,
                    expires_ms,
                    rel_path,
                    parsed.sample_count,
                    content_hash,
                    content_bytes,
                    now_ms(),
                ),
            )
            # 发布时的“当前生效保全”：同车 + 闭区间重叠，全部建立关联。
            # 与保全创建/解除共用同一把 SQLite 写锁，因此上传期间新建或解除的
            # 保全，以本事务提交时的状态为准。
            overlapping = conn.execute(
                "SELECT preservation_id FROM preservations "
                "WHERE vehicle_id = ? AND status = 'active' "
                "  AND start_ms <= ? AND end_ms >= ? "
                "ORDER BY preservation_id",
                (vehicle_id, parsed.end_ms, parsed.start_ms),
            ).fetchall()
            if overlapping:
                conn.executemany(
                    "INSERT INTO preservation_partitions "
                    "(preservation_id, partition_id) VALUES (?, ?)",
                    [(r["preservation_id"], partition_id) for r in overlapping],
                )
            # 索引 COMMIT 之前确保 rename 已持久化（进程崩溃也不会出现“有行无文件”）。
            _fsync_dir(final_path.parent)
        except BaseException:
            # 仍在写锁内：回滚前删除本次 rename 出来的文件。由于分区 ID 的发布
            # 已被写锁串行化，不会误删另一请求已提交的文件。
            if renamed:
                try:
                    final_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
            raise

    return True, False


# ---------------------------------------------------------------- 请求体解析


@dataclass(frozen=True)
class IncomingUpload:
    partition_id: Any
    vehicle_id: Any
    expires_at: Any
    content: bytes


async def _read_stream_limited(request, limit: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            _fail(
                f"请求体超过允许大小（>{limit} 字节）",
                http_status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            )
        chunks.append(chunk)
    return b"".join(chunks)


async def _read_upload_file(upload: UploadFile) -> bytes:
    chunks: list[bytes] = []
    total = 0
    try:
        while True:
            chunk = await upload.read(64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > config.MAX_UPLOAD_BYTES:
                _fail(
                    f"JSONL 超过上限 {config.MAX_UPLOAD_BYTES} 字节（1 MiB）",
                    http_status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                )
            chunks.append(chunk)
    finally:
        await upload.close()
    return b"".join(chunks)


def _first_string(source, names: tuple[str, ...]) -> Any:
    for name in names:
        try:
            value = source.get(name)
        except AttributeError:
            continue
        if value is not None:
            return value
    return None


def _request_meta(request, names: tuple[str, ...], header_name: str) -> Any:
    value = _first_string(request.query_params, names)
    if value is not None and value != "":
        return value
    header = request.headers.get(header_name)
    if header:
        return header
    return None


def _samples_to_jsonl(samples: Any) -> bytes:
    if not isinstance(samples, list) or not samples:
        _fail("samples 必须是非空数组")
    lines: list[str] = []
    for idx, sample in enumerate(samples, 1):
        if not isinstance(sample, dict):
            _fail(f"samples 第 {idx} 项必须是 JSON 对象")
        try:
            lines.append(
                json.dumps(
                    sample,
                    separators=(",", ":"),
                    ensure_ascii=False,
                    sort_keys=True,
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            _fail(f"samples 第 {idx} 项无法序列化为 JSONL: {exc}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _content_from_json_object(obj: dict) -> bytes | None:
    for name in _CONTENT_NAMES:
        value = obj.get(name)
        if isinstance(value, str):
            return value.encode("utf-8")
    for name in ("content_base64", "base64"):
        value = obj.get(name)
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except (ValueError, TypeError):
                _fail(f"{name} 不是合法的 base64")
    if "lines" in obj and isinstance(obj["lines"], list):
        lines = obj["lines"]
        if not lines:
            _fail("lines 不得为空")
        if all(isinstance(x, str) for x in lines):
            return ("\n".join(lines) + "\n").encode("utf-8")
        return _samples_to_jsonl(lines)
    if "samples" in obj:
        return _samples_to_jsonl(obj["samples"])
    return None


async def extract_upload(request) -> IncomingUpload:
    """兼容 multipart/form-data、JSON envelope 与 raw JSONL 三种提交方式。"""
    content_type = (
        request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    )

    if content_type == "multipart/form-data":
        form = await request.form()
        try:
            upload = None
            for name in _FILE_FIELD_NAMES:
                value = form.get(name)
                if isinstance(value, UploadFile):
                    upload = value
                    break
            if upload is not None:
                content = await _read_upload_file(upload)
            else:
                text_value = _first_string(form, _CONTENT_NAMES + ("file",))
                if not isinstance(text_value, str):
                    _fail("multipart 请求缺少 file 或 jsonl 字段")
                content = text_value.encode("utf-8")

            pid = _first_string(form, _PARTITION_NAMES)
            vehicle = _first_string(form, _VEHICLE_NAMES)
            expires = _first_string(form, _EXPIRES_NAMES)
            pid = pid or _request_meta(request, _PARTITION_NAMES, "x-partition-id")
            vehicle = vehicle or _request_meta(request, _VEHICLE_NAMES, "x-vehicle-id")
            expires = expires or _request_meta(request, _EXPIRES_NAMES, "x-expires-at")
            return IncomingUpload(pid, vehicle, expires, content)
        finally:
            close = getattr(form, "close", None)
            if close is not None:
                try:
                    await close()
                except Exception:  # noqa: BLE001 - 关闭失败不影响本次结果
                    pass

    if content_type in ("application/json",) or content_type.endswith("+json"):
        body = await _read_stream_limited(request, _JSON_BODY_CAP)
        try:
            parsed: Any = json.loads(body)
        except json.JSONDecodeError as exc:
            # 兼容：客户端标注 application/json 却直接发送多行 JSONL。
            if b"\n" in body:
                pid = _request_meta(request, _PARTITION_NAMES, "x-partition-id")
                vehicle = _request_meta(request, _VEHICLE_NAMES, "x-vehicle-id")
                expires = _request_meta(request, _EXPIRES_NAMES, "x-expires-at")
                _check_content_size(body)
                return IncomingUpload(pid, vehicle, expires, body)
            _fail(f"请求体不是合法 JSON: {exc.msg}")

        pid = vehicle = expires = None
        if isinstance(parsed, dict):
            pid = _first_string(parsed, _PARTITION_NAMES)
            vehicle = _first_string(parsed, _VEHICLE_NAMES)
            expires = _first_string(parsed, _EXPIRES_NAMES)
            content = _content_from_json_object(parsed)
            # 标注 application/json 但正文是单行 JSONL、元数据走 query/头：
            # 整个对象即一条样本（含 ts 字段）时按原文处理。
            if content is None and "ts" in parsed:
                meta_pid = _request_meta(request, _PARTITION_NAMES, "x-partition-id")
                meta_vehicle = _request_meta(request, _VEHICLE_NAMES, "x-vehicle-id")
                if meta_pid is not None and meta_vehicle is not None:
                    pid = pid or meta_pid
                    vehicle = vehicle or meta_vehicle
                    expires = expires or _request_meta(
                        request, _EXPIRES_NAMES, "x-expires-at"
                    )
                    content = body
        elif isinstance(parsed, list):
            content = _samples_to_jsonl(parsed)
        else:
            _fail("JSON 请求体必须是对象或样本数组")
        if content is None:
            _fail(
                "JSON 请求体缺少 JSONL 内容（可用 jsonl/content/data 字符串，"
                "或 samples/lines 数组）"
            )
        pid = pid or _request_meta(request, _PARTITION_NAMES, "x-partition-id")
        vehicle = vehicle or _request_meta(request, _VEHICLE_NAMES, "x-vehicle-id")
        expires = expires or _request_meta(request, _EXPIRES_NAMES, "x-expires-at")
        return IncomingUpload(pid, vehicle, expires, content)

    # 其余（application/x-ndjson、text/plain、octet-stream 等）：正文即 JSONL，
    # 元数据来自 query 参数或请求头。
    content = await _read_stream_limited(request, _JSON_BODY_CAP)
    pid = _request_meta(request, _PARTITION_NAMES, "x-partition-id")
    vehicle = _request_meta(request, _VEHICLE_NAMES, "x-vehicle-id")
    expires = _request_meta(request, _EXPIRES_NAMES, "x-expires-at")
    _check_content_size(content)
    return IncomingUpload(pid, vehicle, expires, content)


# ---------------------------------------------------------------- 对外入口


def _build_result(
    partition_id: str, *, created: bool, replayed: bool, message: str
) -> PartitionUploadResult:
    with read_conn() as conn:
        row = load_partition_row(conn, partition_id)
        if row is None:  # 发布事务提交后不应发生
            raise HTTPException(
                status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"分区 {partition_id} 发布后索引缺失",
            )
        out = partition_out(row)
        pres = preservation_ids(conn, partition_id)
    return PartitionUploadResult(
        partition=out,
        partition_id=out.partition_id,
        vehicle_id=out.vehicle_id,
        start=out.start,
        end=out.end,
        expires_at=out.expires_at,
        sample_count=out.sample_count,
        status=out.status,
        active_preservations=out.active_preservations,
        preservation_ids=pres,
        created=created,
        replayed=replayed,
        message=message,
    )


def ingest_upload(incoming: IncomingUpload) -> PartitionUploadResult:
    """完整执行一次补传：校验 → 暂存（锁外）→ 原子发布（短锁）。"""
    partition_id = clean_partition_id(incoming.partition_id)
    vehicle_id = clean_vehicle_id(incoming.vehicle_id)
    expires_dt = parse_datetime_text(incoming.expires_at)
    expires_ms = dt_to_ms(expires_dt)

    _check_content_size(incoming.content)
    parsed = parse_jsonl(incoming.content, vehicle_id)
    content_hash = hashlib.sha256(incoming.content).hexdigest()

    # 接收/校验/写文件均发生在数据库写锁之外。
    staged_path = stage_content(incoming.content)
    try:
        created, replayed = publish_upload(
            partition_id=partition_id,
            vehicle_id=vehicle_id,
            expires_ms=expires_ms,
            parsed=parsed,
            content_hash=content_hash,
            content_bytes=len(incoming.content),
            staged_path=staged_path,
        )
    finally:
        # 发布成功时文件已被 rename 走；重放/冲突时删除本次请求自己的临时文件。
        try:
            staged_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass

    if created:
        message = (
            f"补传分区 {partition_id} 已原子发布：{parsed.sample_count} 条样本，"
            f"时段 [{ms_to_iso(parsed.start_ms)}, {ms_to_iso(parsed.end_ms)}]；"
            "索引、完整文件与生效保全关联已同时可见"
        )
    else:
        message = (
            f"同 ID + 同元数据 + 同原文的幂等重放：分区 {partition_id} 当前状态为 "
            f"{_build_current_status(partition_id)}，未重建、未改动文件"
        )
    return _build_result(
        partition_id, created=created, replayed=replayed, message=message
    )


def _build_current_status(partition_id: str) -> str:
    with read_conn() as conn:
        row = conn.execute(
            "SELECT status FROM partitions WHERE partition_id = ?", (partition_id,)
        ).fetchone()
    return row["status"] if row is not None else "missing"
