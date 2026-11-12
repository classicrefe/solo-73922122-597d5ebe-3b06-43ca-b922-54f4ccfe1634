"""索引行的共享视图构造（分区列表、补传结果、清理接口复用）。"""

from __future__ import annotations

import os

from . import config
from .models import PartitionOut
from .timeutil import ms_to_iso, now_ms

# 单行查询时一并算出生效保全数，避免调用方再写一遍相关子查询。
PARTITION_WITH_PRESERVATIONS_SQL = """
SELECT p.*,
       (SELECT COUNT(*)
        FROM preservation_partitions pp
        JOIN preservations pr ON pr.preservation_id = pp.preservation_id
        WHERE pp.partition_id = p.partition_id AND pr.status = 'active'
       ) AS active_preservations
FROM partitions p
WHERE p.partition_id = ?
"""


def partition_out(row) -> PartitionOut:
    """把带 active_preservations 的 partitions 行转换为 API 模型。"""
    file_exists = os.path.exists(config.DATA_DIR / row["file_path"])
    # 旧数据卷迁移后 Row 一定有新列；这里仍用 try 兼容直接构造的测试行。
    try:
        content_sha256 = row["content_hash"]
    except (IndexError, KeyError):
        content_sha256 = None
    try:
        content_bytes = row["content_bytes"]
    except (IndexError, KeyError):
        content_bytes = None
    try:
        source = row["source"] or "seed"
    except (IndexError, KeyError):
        source = "seed"
    return PartitionOut(
        partition_id=row["partition_id"],
        vehicle_id=row["vehicle_id"],
        start=ms_to_iso(row["start_ms"]),
        end=ms_to_iso(row["end_ms"]),
        expires_at=ms_to_iso(row["expires_ms"]),
        expired=row["expires_ms"] <= now_ms(),
        status=row["status"],
        file_exists=file_exists,
        sample_count=row["sample_count"],
        source=source,
        content_sha256=content_sha256,
        content_bytes=content_bytes,
        active_preservations=row["active_preservations"],
        delete_attempts=row["delete_attempts"],
        delete_owner=row["delete_owner"],
        delete_lease_expires_at=ms_to_iso(row["delete_lease_ms"]),
        last_error=row["last_error"],
        cleaned_at=ms_to_iso(row["cleaned_ms"]),
    )


def load_partition_row(conn, partition_id: str):
    return conn.execute(PARTITION_WITH_PRESERVATIONS_SQL, (partition_id,)).fetchone()


def preservation_ids(conn, partition_id: str) -> list[str]:
    return [
        r["preservation_id"]
        for r in conn.execute(
            "SELECT preservation_id FROM preservation_partitions "
            "WHERE partition_id = ? ORDER BY preservation_id",
            (partition_id,),
        ).fetchall()
    ]
