"""分区清单与波形查询接口。"""

from __future__ import annotations

import json
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request, Response, status

from .. import config, ingest
from ..db import read_conn
from ..models import PartitionOut, PartitionUploadResult, Sample, SamplesResponse
from ..timeutil import dt_to_ms, iso_to_ms, ms_to_iso
from ..views import partition_out

router = APIRouter(tags=["分区与波形"])

_UPLOAD_OPENAPI = {
    "requestBody": {
        "required": True,
        "content": {
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "required": ["partition_id", "vehicle_id", "expires_at", "file"],
                    "properties": {
                        "partition_id": {
                            "type": "string",
                            "example": "T203-20261003-0030",
                        },
                        "vehicle_id": {"type": "string", "example": "T203"},
                        "expires_at": {
                            "type": "string",
                            "format": "date-time",
                            "example": "2027-01-01T00:00:00Z",
                        },
                        "file": {
                            "type": "string",
                            "format": "binary",
                            "description": "JSONL（≤1 MiB），每行 ts/vehicle_id/ax/ay/az",
                        },
                    },
                }
            },
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["partition_id", "vehicle_id", "expires_at"],
                    "properties": {
                        "partition_id": {"type": "string"},
                        "vehicle_id": {"type": "string"},
                        "expires_at": {"type": "string", "format": "date-time"},
                        "jsonl": {
                            "type": "string",
                            "description": "原始 JSONL 文本（≤1 MiB）",
                        },
                        "samples": {
                            "type": "array",
                            "items": {"type": "object"},
                            "description": "也可直接传样本对象数组（服务端序列化为 JSONL）",
                        },
                    },
                }
            },
            "application/x-ndjson": {
                "schema": {
                    "type": "string",
                    "format": "binary",
                    "description": "正文即 JSONL；元数据用 query 参数或 X-Partition-Id 等请求头",
                }
            },
        },
    }
}


_LIST_SQL = """
SELECT p.*,
       (SELECT COUNT(*)
        FROM preservation_partitions pp
        JOIN preservations pr ON pr.preservation_id = pp.preservation_id
        WHERE pp.partition_id = p.partition_id AND pr.status = 'active'
       ) AS active_preservations
FROM partitions p
WHERE (? IS NULL OR p.vehicle_id = ?)
  AND (? IS NULL OR p.status = ?)
ORDER BY p.vehicle_id, p.start_ms, p.partition_id
"""


@router.get(
    "/partitions",
    response_model=list[PartitionOut],
    summary="列出分区（含状态、到期、保全数、文件是否在盘）",
    tags=["分区与波形"],
)
def list_partitions(
    vehicle_id: str | None = Query(default=None),
    status_filter: str | None = Query(
        default=None,
        alias="status",
        pattern="^(active|deleting|delete_failed|cleaned)$",
    ),
) -> list[PartitionOut]:
    with read_conn() as conn:
        rows = conn.execute(
            _LIST_SQL, (vehicle_id, vehicle_id, status_filter, status_filter)
        ).fetchall()
    return [partition_out(r) for r in rows]


@router.get(
    "/samples",
    response_model=SamplesResponse,
    summary="按车号 + 时间闭区间查询尚存的采样时间和值",
    tags=["分区与波形"],
)
def get_samples(
    vehicle_id: str = Query(examples=["T203"]),
    start: datetime = Query(
        examples=["2026-10-03T00:30:00Z"], description="区间起点（含）"
    ),
    end: datetime = Query(
        examples=["2026-10-03T00:35:00Z"], description="区间终点（含）"
    ),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> SamplesResponse:
    """只读取 status=active 的分区；deleting/delete_failed/cleaned 分区不对外提供波形。"""
    q_start, q_end = dt_to_ms(start), dt_to_ms(end)
    if q_end < q_start:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY, "end 必须不早于 start（闭区间）"
        )

    with read_conn() as conn:
        rows = conn.execute(
            "SELECT partition_id, file_path FROM partitions "
            "WHERE vehicle_id = ? AND status = 'active' AND start_ms <= ? AND end_ms >= ? "
            "ORDER BY start_ms, partition_id",
            (vehicle_id, q_end, q_start),
        ).fetchall()

    warnings: list[str] = []
    hits: list[tuple[int, dict]] = []
    used_partitions: list[str] = []
    for row in rows:
        path = config.DATA_DIR / row["file_path"]
        if not path.exists():
            warnings.append(
                f"分区 {row['partition_id']} 索引为 active 但文件缺失，已跳过"
            )
            continue
        try:
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    ts_ms = iso_to_ms(rec["ts"])
                    if q_start <= ts_ms <= q_end:
                        hits.append((ts_ms, rec))
        except OSError:
            # 读取期间该分区恰好被清理认领并删除（最终一致）：跳过并提示
            warnings.append(f"分区 {row['partition_id']} 读取期间文件被清理，已跳过")
            continue
        used_partitions.append(row["partition_id"])

    hits.sort(key=lambda item: item[0])
    page = hits[offset : offset + limit]
    return SamplesResponse(
        vehicle_id=vehicle_id,
        start=ms_to_iso(q_start),
        end=ms_to_iso(q_end),
        partitions=used_partitions,
        matched=len(hits),
        returned=len(page),
        samples=[
            Sample(
                ts=ms_to_iso(ts),
                vehicle_id=rec["vehicle_id"],
                ax=rec["ax"],
                ay=rec["ay"],
                az=rec["az"],
            )
            for ts, rec in page
        ],
        warnings=warnings,
    )


# ---------------------------------------------------------------- 分区补传


async def _upload(request: Request, response: Response) -> PartitionUploadResult:
    incoming = await ingest.extract_upload(request)
    result = ingest.ingest_upload(incoming)
    # 首次发布 201；同 ID/元数据/原文的幂等重放 200（只返回当前状态）。
    response.status_code = (
        status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    )
    return result


@router.post(
    "/partitions/upload",
    response_model=PartitionUploadResult,
    status_code=status.HTTP_201_CREATED,
    summary="补传分区（原子发布，最多 1 MiB JSONL）",
    description=(
        "接收分区 ID、车号、到期时间与 JSONL。样本须非空、同车、时间严格递增、"
        "三轴有限；时段与条数自动推导。\n\n"
        "支持 `multipart/form-data`（file 字段）、`application/json`"
        "（jsonl/content 字符串或 samples 数组）以及 `application/x-ndjson` 正文。\n\n"
        "同 ID + 同元数据 + 同原文的重试不会重建，只返回当前状态（200）；"
        "内容或元数据不同则 409；已回收（cleaned）的 ID 不会被重建。"
        "发布事务同时写入完整文件、索引与同车重叠生效保全关联，"
        "清理要么看不到它、要么能看到完整保全关联。"
    ),
    openapi_extra=_UPLOAD_OPENAPI,
    tags=["分区补传"],
)
async def upload_partition(
    request: Request, response: Response
) -> PartitionUploadResult:
    return await _upload(request, response)


@router.post(
    "/partitions",
    response_model=PartitionUploadResult,
    status_code=status.HTTP_201_CREATED,
    include_in_schema=False,
    openapi_extra=_UPLOAD_OPENAPI,
    tags=["分区补传"],
)
async def upload_partition_alias(
    request: Request, response: Response
) -> PartitionUploadResult:
    """``POST /partitions/upload`` 的别名，保持 REST 风格调用方便。"""
    return await _upload(request, response)


@router.post(
    "/partitions/upload/raw",
    response_model=PartitionUploadResult,
    status_code=status.HTTP_201_CREATED,
    include_in_schema=False,
    openapi_extra=_UPLOAD_OPENAPI,
    tags=["分区补传"],
)
async def upload_partition_raw(
    request: Request, response: Response
) -> PartitionUploadResult:
    """与 ``POST /partitions/upload`` 相同；正文字节即 JSONL。"""
    return await _upload(request, response)
