"""轨道交通振动数据湖 API。

FastAPI + SQLite（WAL）+ 本地 JSONL 分区。两个 API 进程共享同一数据目录，
通过 SQLite 写锁串行化“保全”与“删除资格认领”，保证确定先后。
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import config
from .cleanup import cleanup_loop
from .db import init_db
from .ingest import cleanup_stale_staging
from .routes import cleanup as cleanup_routes
from .routes import partitions as partitions_routes
from .routes import preservations as preservations_routes
from .seed import reconcile_files, seed_from_manifest
from .timeutil import ms_to_iso, now_ms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("datalake.main")

DESCRIPTION = """
轨道交通车辆振动数据湖：到期数据清理 + 故障调查波形保全 + 记录仪分区补传。

**演示流程（全部可在 /docs 内直接执行）**

1. `POST /partitions/upload` —— 补传一个分区。multipart 表单填写
   `partition_id`、`vehicle_id`、`expires_at` 并选择 JSONL 文件（≤1 MiB）；
   也可用 JSON 请求体 `{partition_id, vehicle_id, expires_at, jsonl}`。
   样本须非空、同车、`ts` 严格递增、`ax/ay/az` 有限；时段与条数自动推导。
2. `GET /partitions` —— 查看种子分区与新补传分区（状态、到期、保全数、文件在盘）。
3. `GET /samples` —— 按车号 + 时间闭区间查询波形（只读 active 分区）。
4. `POST /preservations` —— 按车号 + 时间闭区间保全全部重叠分区；
   与补传并发时以发布事务提交时的生效保全为准。
5. `POST /cleanup/run` —— 回收已到期且无生效保全的分区；刚补传且被保全的分区不会被删。
6. 对已清理分区再次补传：同 ID + 同元数据 + 同原文只返回当前状态（200，不重建），
   异内容/异元数据返回 **409**；已回收 ID 永不被重建。
7. `GET /cleanup/status` —— 状态统计、在途删除与失败记录。

**并发与崩溃语义**：补传在数据库写锁之外接收并完整落盘临时文件，发布时用
`BEGIN IMMEDIATE` 同一事务写入索引与生效保全关联；两个 API 进程共享同一
SQLite 与分区目录，补传、保全、清理的顺序由 SQLite 写锁全序化。未提交的
临时文件不会被查询看见，重启后已提交数据完整保留。
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    seed_stats = seed_from_manifest()
    reconcile_stats = reconcile_files()
    stale_staging = cleanup_stale_staging()
    log.info(
        "启动完成 seed=%s reconcile=%s stale_staging=%s",
        seed_stats,
        reconcile_stats,
        stale_staging,
    )

    app.state.cleanup_lock = asyncio.Lock()
    app.state.last_cleanup_run = None
    app.state.seed_stats = seed_stats

    task = None
    if config.BACKGROUND_CLEANUP_ENABLED:
        task = asyncio.create_task(cleanup_loop(app))
    try:
        yield
    finally:
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(
    title="轨道交通振动数据湖",
    description=DESCRIPTION,
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(preservations_routes.router)
app.include_router(partitions_routes.router)
app.include_router(cleanup_routes.router)


@app.get("/health", tags=["meta"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "process": config.OWNER, "now": ms_to_iso(now_ms())}


@app.get("/meta/info", tags=["meta"], summary="进程与存储信息")
def meta_info() -> dict:
    return {
        "process": config.OWNER,
        "data_dir": str(config.DATA_DIR),
        "db_path": str(config.DB_PATH),
        "seed_dir": str(config.SEED_DIR),
        "background_cleanup_enabled": config.BACKGROUND_CLEANUP_ENABLED,
        "cleanup_interval_seconds": config.CLEANUP_INTERVAL_SECONDS,
        "delete_lease_seconds": config.DELETE_LEASE_SECONDS,
        "delete_fail_partitions": sorted(config.DELETE_FAIL_PARTITIONS),
        "max_upload_bytes": config.MAX_UPLOAD_BYTES,
        "staging_ttl_seconds": config.STAGING_TTL_SECONDS,
    }
