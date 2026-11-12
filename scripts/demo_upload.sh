#!/usr/bin/env bash
# 补传演示：保全 -> 补传（multipart）-> 查询 -> 清理跳过 -> 解除回收 -> 幂等/冲突。
# 前提：docker compose up --build 已启动 api-a:8001 / api-b:8002。
# 用法：./scripts/demo_upload.sh [BASE_A] [BASE_B]
set -euo pipefail

A="${1:-http://localhost:8001}"
B="${2:-http://localhost:8002}"
PP='python3 -m json.tool'
PID="U-DEMO-$(date +%s)"
TMP="$(mktemp -d)"
JSONL="$TMP/$PID.jsonl"
trap 'rm -rf "$TMP"' EXIT

step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

step "0. 健康检查（api-a=$A, api-b=$B）"
curl -s "$A/health" | $PP
curl -s "$B/health" | $PP

step "1. 生成 5 条严格递增的 JSONL 样本（车号 T102，落在未到期种子 P0005 时段内）"
python3 - "$JSONL" <<'PY'
import json, sys
from datetime import datetime, timedelta, timezone
path = sys.argv[1]
t0 = datetime(2026, 10, 2, 1, 30, tzinfo=timezone.utc)
with open(path, "w", encoding="utf-8") as fh:
    for i in range(5):
        ts = (t0 + timedelta(seconds=60 * i)).isoformat().replace("+00:00", "Z")
        fh.write(json.dumps({"ts": ts, "vehicle_id": "T102",
                             "ax": 3.1 + i, "ay": -0.4, "az": 9.8},
                            separators=(",", ":")) + "\n")
PY
wc -c "$JSONL"
cat "$JSONL"

step "2. 先保全 T102 [01:25, 01:35]（覆盖种子 P0005，因此可以先于补传建立）"
PRES=$(curl -s -X POST "$A/preservations" -H 'Content-Type: application/json' -d '{
  "vehicle_id": "T102",
  "start": "2026-10-02T01:25:00Z",
  "end": "2026-10-02T01:35:00Z",
  "investigator": "upload-demo",
  "reason": "补传演示保全"
}')
echo "$PRES" | $PP
PRS_ID=$(echo "$PRES" | python3 -c "import json,sys; print(json.load(sys.stdin)['preservation']['preservation_id'])")

step "3. 补传新分区 $PID（到期时间已过，但与生效保全重叠）"
UP=$(curl -s -w '\n%{http_code}' -X POST "$B/partitions/upload" \
  -F "partition_id=$PID" \
  -F 'vehicle_id=T102' \
  -F 'expires_at=2026-11-01T00:00:00Z' \
  -F "file=@$JSONL;type=application/x-ndjson")
echo "$UP" | head -n -1 | $PP
echo "HTTP ${UP##*$'\n'}"

step "4. 在另一进程上查询补传波形与分区索引"
curl -s "$A/partitions?vehicle_id=T102" | $PP
curl -s "$A/samples?vehicle_id=T102&start=2026-10-02T01:30:00Z&end=2026-10-02T01:34:00Z" | $PP

step "5. 立即清理：到期但被保全的 $PID 不应被回收"
curl -s -X POST "$B/cleanup/run" | $PP
curl -s "$A/partitions?vehicle_id=T102&status=active" | python3 -c "
import json, sys
ids = [p['partition_id'] for p in json.load(sys.stdin)]
assert '$PID' in ids, f'$PID missing: {ids}'
print('$PID 仍为 active，补传-保全关联生效')"

step "6. 解除保全后再次清理 -> $PID 被回收"
curl -s -X POST "$A/preservations/$PRS_ID/release" | $PP
curl -s -X POST "$B/cleanup/run" | $PP

step "7. 同 ID + 同元数据 + 同原文重试：200 返回当前状态，不重建"
curl -s -w '\nHTTP %{http_code}\n' -X POST "$B/partitions/upload" \
  -F "partition_id=$PID" \
  -F 'vehicle_id=T102' \
  -F 'expires_at=2026-11-01T00:00:00Z' \
  -F "file=@$JSONL;type=application/x-ndjson"

step "8. 同 ID 异内容重试：409 拒绝"
printf '{"ts":"2026-10-02T01:30:00Z","vehicle_id":"T102","ax":99,"ay":0,"az":0}\n' > "$TMP/other.jsonl"
curl -s -w '\nHTTP %{http_code}\n' -X POST "$A/partitions/upload" \
  -F "partition_id=$PID" \
  -F 'vehicle_id=T102' \
  -F 'expires_at=2026-11-01T00:00:00Z' \
  -F "file=@$TMP/other.jsonl;type=application/x-ndjson"

echo
echo "补传演示完成。可在 http://localhost:8001/docs 的 POST /partitions/upload 手动继续。"
