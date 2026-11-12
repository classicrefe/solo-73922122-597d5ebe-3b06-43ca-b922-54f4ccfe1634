#!/usr/bin/env python3
"""补传（record upload）端到端测试：真实双进程 + 共享数据目录。

运行：python3 tests/upload_e2e_test.py [场景名片段...]
复用 tests/e2e_test.py 的 Server/http 基座；每个场景独立端口与数据目录。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
from e2e_test import Server, fresh_dir, http, ok  # noqa: E402

# 跨测试进程也不复用端口：每次运行的 PID 不同，避免上一次运行尚未退出的
# SERVER_LIFETIME 服务器占用同一批端口。
Server._next_port = 10000 + (os.getpid() % 2000) * 10

MAX = 1024 * 1024


def _decode(resp) -> dict:
    raw = resp.read()
    return json.loads(raw or b"{}")


def multipart(
    port: int,
    fields: dict,
    content: bytes,
    *,
    path: str = "/partitions/upload",
    filename: str = "wave.jsonl",
) -> tuple[int, dict]:
    boundary = "----claude" + uuid.uuid4().hex
    chunks: list[bytes] = []
    for key, value in fields.items():
        chunks.append(
            (
                f'--{boundary}\r\nContent-Disposition: form-data; name="{key}"\r\n\r\n'
                f"{value}\r\n"
            ).encode()
        )
    chunks.append(
        (
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
            f'filename="{filename}"\r\nContent-Type: application/x-ndjson\r\n\r\n'
        ).encode()
    )
    chunks.append(content)
    chunks.append(f"\r\n--{boundary}--\r\n".encode())
    body = b"".join(chunks)
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=body,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, _decode(resp)
    except urllib.error.HTTPError as exc:
        return exc.code, _decode(exc)


def raw(
    port: int,
    pid: str,
    vehicle: str,
    expires: str,
    content: bytes,
    *,
    path: str = "/partitions/upload/raw",
) -> tuple[int, dict]:
    query = urllib.parse.urlencode(
        {"partition_id": pid, "vehicle_id": vehicle, "expires_at": expires}
    )
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}?{query}",
        data=content,
        method="POST",
        headers={"Content-Type": "application/x-ndjson"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, _decode(resp)
    except urllib.error.HTTPError as exc:
        return exc.code, _decode(exc)


def json_upload(
    port: int, payload: dict, *, path: str = "/partitions/upload"
) -> tuple[int, dict]:
    return http("POST", port, path, payload)


def sample_lines(
    vehicle: str,
    start: str,
    count: int,
    step_seconds: int = 1,
    ax: float = 0.1,
    ay: float = 0.2,
    az: float = 9.8,
) -> bytes:
    from datetime import datetime, timedelta, timezone

    t0 = datetime.fromisoformat(start.replace("Z", "+00:00"))
    lines = []
    for i in range(count):
        ts = (t0 + timedelta(seconds=i * step_seconds)).astimezone(timezone.utc)
        ts_text = ts.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        lines.append(
            json.dumps(
                {"ts": ts_text, "vehicle_id": vehicle, "ax": ax, "ay": ay, "az": az},
                separators=(",", ":"),
            )
        )
    return ("\n".join(lines) + "\n").encode()


def content_sha(content: bytes) -> str:
    import hashlib

    return hashlib.sha256(content).hexdigest()


def partition(port: int, pid: str) -> dict | None:
    _, parts = http("GET", port, "/partitions")
    return next((p for p in parts if p["partition_id"] == pid), None)


def associations(port: int, pres_id: str) -> list[str]:
    code, body = http("GET", port, f"/preservations/{pres_id}")
    return body.get("partitions", []) if code == 200 else []


# ------------------------------------------------- U1 基本发布 + 保全 + 幂等
def scenario_basic_publish_and_preservation() -> None:
    print("== U1 补传原子发布 / 重叠保全关联 / 独立解除 / 幂等与冲突 ==")
    d = fresh_dir("lk-u1")
    a = Server(d, "u1a", lifetime=60, BACKGROUND_CLEANUP_ENABLED="false").start()
    b = Server(d, "u1b", lifetime=60, BACKGROUND_CLEANUP_ENABLED="false").start()

    # 两份保全都覆盖种子 P0001（00:00-01:00），可在补传前建立
    code1, p1 = http(
        "POST",
        a.port,
        "/preservations",
        {
            "vehicle_id": "T101",
            "start": "2026-10-01T00:35:00Z",
            "end": "2026-10-01T00:45:00Z",
            "investigator": "i1",
            "reason": "upload-1",
        },
    )
    code2, p2 = http(
        "POST",
        b.port,
        "/preservations",
        {
            "vehicle_id": "T101",
            "start": "2026-10-01T00:36:00Z",
            "end": "2026-10-01T00:44:00Z",
            "investigator": "i2",
            "reason": "upload-2",
        },
    )
    ok("U1 两份重叠保全均可建立", code1 == 201 and code2 == 201)
    prs1 = p1["preservation"]["preservation_id"]
    prs2 = p2["preservation"]["preservation_id"]

    content = sample_lines("T101", "2026-10-01T00:36:00Z", 9, step_seconds=60, ax=5.0)
    fields = {
        "partition_id": "U0001",
        "vehicle_id": "T101",
        "expires_at": "2026-11-01T00:00:00Z",
    }
    code, res = multipart(a.port, fields, content)
    ok(
        "U1 首次补传 201 且自动推导条数/时段",
        code == 201
        and res["created"] is True
        and res["sample_count"] == 9
        and res["start"] == "2026-10-01T00:36:00.000Z"
        and res["end"] == "2026-10-01T00:44:00.000Z",
        f"code={code} res={res.get('message')}",
    )
    ok(
        "U1 发布时关联全部同车重叠生效保全",
        sorted(res["preservation_ids"]) == sorted([prs1, prs2])
        and res["active_preservations"] == 2,
        str(res.get("preservation_ids")),
    )
    ok(
        "U1 文件已完整落盘且索引可见",
        partition(a.port, "U0001")["file_exists"] is True
        and (d / "partitions" / "U0001.jsonl").read_bytes() == content,
    )
    _, qres = http(
        "GET",
        b.port,
        "/samples?vehicle_id=T101&start=2026-10-01T00:36:00Z"
        "&end=2026-10-01T00:44:00Z",
    )
    ok(
        "U1 另一进程立即可查询补传波形",
        "U0001" in qres["partitions"]
        and qres["matched"] >= 9
        and any(abs(s["ax"] - 5.0) < 1e-9 for s in qres["samples"]),
        f"partitions={qres.get('partitions')} matched={qres.get('matched')}",
    )
    ok(
        "U1 保全详情含补传分区",
        "U0001" in associations(b.port, prs1) and "U0001" in associations(b.port, prs2),
    )

    # 到期但被保全：清理不得删除；解除一份后仍受另一份保护
    _, clean1 = http("POST", b.port, "/cleanup/run")
    cleaned = {
        o["partition_id"] for o in clean1["outcomes"] if o["result"] == "cleaned"
    }
    ok(
        "U1 到期补传分区因保全未被回收",
        "U0001" not in cleaned and partition(a.port, "U0001")["status"] == "active",
    )
    ok("U1 其他到期种子分区照常回收", all(f"P{i:04d}" in cleaned for i in (2, 4, 8)))

    http("POST", a.port, f"/preservations/{prs1}/release")
    _, clean2 = http("POST", b.port, "/cleanup/run")
    ok(
        "U1 解除一份后补传分区仍受第二份保护",
        partition(a.port, "U0001")["status"] == "active",
    )
    http("POST", b.port, f"/preservations/{prs2}/release")
    _, clean3 = http("POST", a.port, "/cleanup/run")
    ok("U1 两份都解除后可回收", partition(a.port, "U0001")["status"] == "cleaned")

    # 幂等重放：同 ID/同元数据/同原文 -> 200 返回 cleaned，不重建
    code, res = multipart(b.port, fields, content)
    ok(
        "U1 原文重放只返回当前状态（200 + cleaned），不重建",
        code == 200
        and res["replayed"] is True
        and res["created"] is False
        and res["status"] == "cleaned"
        and not (d / "partitions" / "U0001.jsonl").exists(),
        f"code={code} status={res.get('status')}",
    )
    # 异内容 / 异元数据 -> 409
    other = sample_lines("T101", "2026-10-01T00:36:00Z", 9, ax=6.0)
    ok("U1 同 ID 异内容 409", multipart(a.port, fields, other)[0] == 409)
    bad_meta = dict(fields, expires_at="2026-12-01T00:00:00Z")
    ok("U1 同 ID 异元数据 409", multipart(a.port, bad_meta, content)[0] == 409)
    # 已回收的种子 ID 也不能重建
    code, _ = multipart(
        a.port,
        {
            "partition_id": "P0001",
            "vehicle_id": "T101",
            "expires_at": "2026-12-01T00:00:00Z",
        },
        sample_lines("T101", "2026-10-01T00:10:00Z", 3),
    )
    ok("U1 已回收种子 ID 不得重建（409）", code == 409)


# ------------------------------------------------- U2 校验语义
def scenario_validation() -> None:
    print("== U2 补传校验：非空/同车/严格递增/有限/1MiB ==")
    d = fresh_dir("lk-u2")
    s = Server(d, "u2a", lifetime=50, BACKGROUND_CLEANUP_ENABLED="false").start()

    def attempt(pid: str, content: bytes, vehicle: str = "T101") -> tuple[int, dict]:
        return multipart(
            s.port,
            {
                "partition_id": pid,
                "vehicle_id": vehicle,
                "expires_at": "2027-01-01T00:00:00Z",
            },
            content,
        )

    ok("U2 空 JSONL 拒绝", attempt("V1", b"")[0] in (400, 413, 422))
    ok("U2 仅空行拒绝", attempt("V2", b"\n \r\n")[0] == 422)
    ok("U2 非法 JSON 拒绝", attempt("V3", b"not-json\n")[0] == 422)
    ok(
        "U2 缺字段拒绝",
        attempt("V4", b'{"ts":"2026-10-01T00:00:00Z","ax":1,"ay":2}\n')[0] == 422,
    )
    ok(
        "U2 异车拒绝",
        attempt("V5", sample_lines("T102", "2026-10-01T00:00:00Z", 2))[0] == 422,
    )
    duplicate = (
        b'{"ts":"2026-10-01T00:00:00Z","vehicle_id":"T101","ax":1,"ay":2,"az":3}\n'
        b'{"ts":"2026-10-01T00:00:00Z","vehicle_id":"T101","ax":1,"ay":2,"az":3}\n'
    )
    ok("U2 时间重复拒绝（严格递增）", attempt("V6", duplicate)[0] == 422)
    backwards = (
        b'{"ts":"2026-10-01T00:00:01Z","vehicle_id":"T101","ax":1,"ay":2,"az":3}\n'
        b'{"ts":"2026-10-01T00:00:00Z","vehicle_id":"T101","ax":1,"ay":2,"az":3}\n'
    )
    ok("U2 时间倒退拒绝", attempt("V7", backwards)[0] == 422)
    for axis in ("ax", "ay", "az"):
        base = {
            "ts": "2026-10-01T00:00:00Z",
            "vehicle_id": "T101",
            "ax": 1,
            "ay": 2,
            "az": 3,
        }
        nan_line = json.dumps(base, separators=(",", ":")).replace(
            f'"{axis}":{json.dumps(base[axis])}', f'"{axis}":NaN'
        )
        ok(
            f"U2 {axis}=NaN 拒绝",
            attempt(f"V8{axis}", (nan_line + "\n").encode())[0] == 422,
        )
    line = sample_lines("T101", "2026-10-01T00:00:00Z", 1)
    over = line + b" " * (MAX - len(line) + 10) + b"\n"
    code, _ = attempt("V9", over)
    ok("U2 超过 1 MiB 返回 413", code == 413, str(code))
    exactly = sample_lines("T101", "2026-10-01T00:00:00Z", 1)
    exactly += b" " * (MAX - len(exactly))
    ok("U2 恰好 1 MiB 可接受", attempt("V10", exactly)[0] == 201)
    ok(
        "U2 非法分区 ID 拒绝",
        attempt("../evil", sample_lines("T101", "2026-10-01T00:00:00Z", 1))[0] == 422,
    )
    ok(
        "U2 缺少到期时间拒绝",
        multipart(
            s.port,
            {"partition_id": "V11", "vehicle_id": "T101"},
            sample_lines("T101", "2026-10-01T00:00:00Z", 1),
        )[0]
        == 422,
    )


# ------------------------------------------------- U3 并发
def scenario_concurrency() -> None:
    print("== U3 双进程并发补传 / 保全 / 清理 ==")
    d = fresh_dir("lk-u3")
    a = Server(d, "u3a", lifetime=70, BACKGROUND_CLEANUP_ENABLED="false").start()
    b = Server(d, "u3b", lifetime=70, BACKGROUND_CLEANUP_ENABLED="false").start()

    content = sample_lines("T101", "2026-10-01T00:30:00Z", 5)
    fields = {
        "partition_id": "C0001",
        "vehicle_id": "T101",
        "expires_at": "2027-01-01T00:00:00Z",
    }
    out: dict = {}

    def up(port):
        out.setdefault("u", []).append(multipart(port, fields, content))

    ts = [threading.Thread(target=up, args=(p,)) for p in (a.port, b.port)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    codes = sorted(r[0] for r in out["u"])
    created = [r[1].get("created") for r in out["u"] if r[0] in (200, 201)]
    ok(
        "U3 同原文并发补传均成功且只创建一次",
        codes in ([200, 201], [200, 200], [201, 201]) and created.count(True) == 1,
        f"codes={codes} created={created}",
    )
    ok(
        "U3 只有一个索引行",
        sum(
            1
            for p in http("GET", a.port, "/partitions")[1]
            if p["partition_id"] == "C0001"
        )
        == 1,
    )

    # 同 ID 异内容并发：恰好一个赢家
    content2 = sample_lines("T101", "2026-10-01T00:30:00Z", 5, ax=7.0)
    f2 = dict(fields, partition_id="C0002")
    results: dict = {}

    def upload_content(port, body):
        results.setdefault("r", []).append(multipart(port, f2, body))

    ts = [
        threading.Thread(target=upload_content, args=(a.port, content)),
        threading.Thread(target=upload_content, args=(b.port, content2)),
    ]
    [t.start() for t in ts]
    [t.join() for t in ts]
    codes = sorted(r[0] for r in results["r"])
    ok("U3 同 ID 异内容并发：一个成功一个 409", codes == [201, 409], str(codes))

    # 补传与保全并发：以发布时状态为准，最终必须有保全关联
    cpid = "C0003"
    cstart = "2026-10-01T00:40:00Z"
    ccontent = sample_lines("T101", cstart, 6)
    cf = {
        "partition_id": cpid,
        "vehicle_id": "T101",
        "expires_at": "2026-11-01T00:00:00Z",
    }  # 已到期
    race: dict = {}

    def do_upload():
        race["u"] = multipart(a.port, cf, ccontent)

    def do_pres():
        race["p"] = http(
            "POST",
            b.port,
            "/preservations",
            {
                "vehicle_id": "T101",
                "start": "2026-10-01T00:39:00Z",
                "end": "2026-10-01T00:45:00Z",
                "investigator": "race",
                "reason": "upload-race",
            },
        )

    ts = [threading.Thread(target=do_upload), threading.Thread(target=do_pres)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    up_code = race["u"][0]
    prs_id = race["p"][1].get("preservation", {}).get("preservation_id")
    ok("U3 补传与保全并发无异常", up_code == 201 and race["p"][0] == 201)
    assoc = associations(a.port, prs_id) if prs_id else []
    ok("U3 并发保全最终关联到新分区（以发布时状态为准）", cpid in assoc, str(assoc))
    _, clean = http("POST", b.port, "/cleanup/run")
    ok("U3 并发建立保全的新分区未被清理", partition(a.port, cpid)["status"] == "active")

    # 补传（无保全、已到期）与清理并发：不出现 active 但文件缺失
    ep = "C0004"
    econtent = sample_lines("T101", "2026-10-01T03:10:00Z", 3)
    ef = {
        "partition_id": ep,
        "vehicle_id": "T101",
        "expires_at": "2026-11-01T00:00:00Z",
    }
    race2: dict = {}
    ts = [
        threading.Thread(
            target=lambda: race2.setdefault("u", multipart(a.port, ef, econtent))
        ),
        threading.Thread(
            target=lambda: race2.setdefault("c", http("POST", b.port, "/cleanup/run"))
        ),
    ]
    [t.start() for t in ts]
    [t.join() for t in ts]
    p = partition(a.port, ep)
    ok(
        "U3 补传与清理并发后状态一致（active 必有文件，cleaned 必无文件）",
        p is not None
        and (
            (p["status"] == "active" and p["file_exists"])
            or (p["status"] == "cleaned" and not p["file_exists"])
        )
        or (p["status"] == "deleting"),
        str(p and (p["status"], p["file_exists"])),
    )

    # 全量不变量：任一 active 分区必须有文件；cleaned 分区不得有文件
    _, parts = http("GET", a.port, "/partitions")
    ok(
        "U3 最终索引-文件一致性不变量",
        all(
            (p["status"] != "active" or p["file_exists"])
            and (p["status"] != "cleaned" or not p["file_exists"])
            for p in parts
        ),
    )


# ------------------------------------------------- U4 重启 / 残片
def scenario_restart_and_partial_files() -> None:
    print("== U4 重启保留已提交补传；未提交残片不暴露/被 reconcile 清理 ==")
    d = fresh_dir("lk-u4")
    s1 = Server(d, "u4a", lifetime=25, BACKGROUND_CLEANUP_ENABLED="false").start()
    # 先建立保全（覆盖种子 P0001），再补传已到期分区
    _, pr = http(
        "POST",
        s1.port,
        "/preservations",
        {
            "vehicle_id": "T101",
            "start": "2026-10-01T00:10:00Z",
            "end": "2026-10-01T00:20:00Z",
            "investigator": "i",
            "reason": "restart",
        },
    )
    assert pr["preservation"]["preservation_id"]
    content = sample_lines("T101", "2026-10-01T00:12:00Z", 4, ax=9.0)
    fields = {
        "partition_id": "R0001",
        "vehicle_id": "T101",
        "expires_at": "2026-11-01T00:00:00Z",
    }
    code, res = multipart(s1.port, fields, content)
    ok("U4 重启前补传成功并关联保全", code == 201 and res["active_preservations"] == 1)
    # 制造未提交残片：partitions/ 下无索引文件 + staging 中旧文件
    (d / "partitions" / "GhOST.jsonl").write_text('{"bad":true}\n')
    (d / "staging").mkdir(parents=True, exist_ok=True)
    stale = d / "staging" / "upload-deadbeef.jsonl"
    stale.write_text("{}\n")
    old = time.time() - 7200
    os.utime(stale, (old, old))
    s1.wait_dead()

    s2 = Server(
        d,
        "u4b",
        lifetime=35,
        BACKGROUND_CLEANUP_ENABLED="false",
        STAGING_TTL_SECONDS="1",
    ).start()
    p = partition(s2.port, "R0001")
    ok(
        "U4 重启后已提交补传数据完整保留",
        p is not None
        and p["status"] == "active"
        and p["file_exists"]
        and p["sample_count"] == 4
        and p["active_preservations"] == 1,
    )
    _, qres = http(
        "GET",
        s2.port,
        "/samples?vehicle_id=T101&start=2026-10-01T00:12:00Z"
        "&end=2026-10-01T00:15:00Z",
    )
    ok(
        "U4 重启后仍可查询补传波形",
        "R0001" in qres["partitions"] and qres["matched"] >= 4,
        f"partitions={qres.get('partitions')} matched={qres.get('matched')}",
    )
    ok(
        "U4 无索引残片未被暴露且被 reconcile 删除",
        partition(s2.port, "GhOST") is None
        and not (d / "partitions" / "GhOST.jsonl").exists(),
    )
    ok("U4 旧 staging 残留被清理", not stale.exists())
    _, clean = http("POST", s2.port, "/cleanup/run")
    ok("U4 重启后保全仍阻止清理", partition(s2.port, "R0001")["status"] == "active")


# ------------------------------------------------- U5 输入形态
def scenario_input_shapes() -> None:
    print("== U5 JSON envelope / raw NDJSON / 别名路径 ==")
    d = fresh_dir("lk-u5")
    s = Server(d, "u5a", lifetime=45, BACKGROUND_CLEANUP_ENABLED="false").start()
    content = sample_lines("T105", "2026-10-05T00:00:00Z", 3)
    meta = {
        "partition_id": "J0001",
        "vehicle_id": "T105",
        "expires_at": "2027-01-01T00:00:00Z",
    }

    code, res = json_upload(s.port, dict(meta, jsonl=content.decode()))
    ok("U5 JSON envelope(jsonl 字符串) 201", code == 201 and res["sample_count"] == 3)
    code, res = raw(
        s.port,
        "J0002",
        "T105",
        "2027-01-01T00:00:00Z",
        content,
        path="/partitions/upload/raw",
    )
    ok("U5 raw NDJSON + query 元数据 201", code == 201 and res["sample_count"] == 3)
    code, res = raw(
        s.port, "J0003", "T105", "2027-01-01T00:00:00Z", content, path="/partitions"
    )
    ok("U5 /partitions 别名接受 raw 正文", code == 201 and res["sample_count"] == 3)
    code, res = json_upload(
        s.port,
        dict(
            meta,
            partition_id="J0004",
            samples=[
                {
                    "ts": "2026-10-05T00:00:00Z",
                    "vehicle_id": "T105",
                    "ax": 1,
                    "ay": 2,
                    "az": 3,
                },
                {
                    "ts": "2026-10-05T00:00:01Z",
                    "vehicle_id": "T105",
                    "ax": 4,
                    "ay": 5,
                    "az": 6,
                },
            ],
        ),
    )
    ok("U5 JSON samples 数组 201", code == 201 and res["sample_count"] == 2)
    # JSON 原文重放同 hash
    code2, res2 = json_upload(s.port, dict(meta, jsonl=content.decode()))
    ok(
        "U5 JSON envelope 原文重放 200 且不重建",
        code2 == 200 and res2["replayed"] is True and res2["status"] == "active",
    )
    # 查询能读到所有形态
    _, samples = http(
        "GET",
        s.port,
        "/samples?vehicle_id=T105&start=2026-10-05T00:00:00Z"
        "&end=2026-10-05T00:00:02Z",
    )
    ok(
        "U5 查询覆盖全部补传分区",
        samples["matched"] >= 3 and len(samples["partitions"]) >= 3,
    )


def main() -> int:
    from e2e_test import FAILURES, PASSES  # noqa: F401
    import e2e_test

    scenarios = [
        scenario_basic_publish_and_preservation,
        scenario_validation,
        scenario_concurrency,
        scenario_restart_and_partial_files,
        scenario_input_shapes,
    ]
    only = sys.argv[1:] if len(sys.argv) > 1 else None
    for sc in scenarios:
        name = sc.__name__
        if only and not any(o in name for o in only):
            continue
        try:
            sc()
        except Exception as exc:  # noqa: BLE001
            ok(f"{name} 异常终止", False, repr(exc))
    print("\n========== 补传测试汇总 ==========")
    print(f"PASS: {len(e2e_test.PASSES)}  FAIL: {len(e2e_test.FAILURES)}")
    for f in e2e_test.FAILURES:
        print("FAIL:", f)
    return 1 if e2e_test.FAILURES else 0


if __name__ == "__main__":
    sys.exit(main())
