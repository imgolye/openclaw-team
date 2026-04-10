#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
SERVICE_NAME="mission-control"
POSTGRES_SERVICE="postgres"
BACKUP_ROOT="$PROJECT_DIR/local/backups/mission-control-container"
EXPORT_DB=1

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/export_container_snapshot.sh [选项]

选项:
  --project-dir PATH
  --compose-file PATH
  --service NAME
  --postgres-service NAME
  --output-root PATH
  --skip-db
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --compose-file)
      COMPOSE_FILE="$2"
      shift 2
      ;;
    --service)
      SERVICE_NAME="$2"
      shift 2
      ;;
    --postgres-service)
      POSTGRES_SERVICE="$2"
      shift 2
      ;;
    --output-root)
      BACKUP_ROOT="$2"
      shift 2
      ;;
    --skip-db)
      EXPORT_DB=0
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "[✗] 未知参数: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

PROJECT_DIR="$(python3 - "$PROJECT_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
COMPOSE_FILE="$(python3 - "$COMPOSE_FILE" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
BACKUP_ROOT="$(python3 - "$BACKUP_ROOT" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

if ! command -v docker >/dev/null 2>&1; then
  echo "[✗] 未检测到 docker。" >&2
  exit 1
fi

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "[✗] 缺少 compose 文件: $COMPOSE_FILE" >&2
  exit 1
fi

SERVICE_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q "$SERVICE_NAME")"
if [[ -z "$SERVICE_CONTAINER" ]]; then
  echo "[✗] 未找到服务容器: $SERVICE_NAME" >&2
  exit 1
fi

POSTGRES_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q "$POSTGRES_SERVICE" 2>/dev/null || true)"

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SNAPSHOT_DIR="$BACKUP_ROOT/$TIMESTAMP"
LATEST_LINK="$BACKUP_ROOT/latest"

mkdir -p "$SNAPSHOT_DIR/openclaw" "$SNAPSHOT_DIR/bin"
if [[ "$EXPORT_DB" -eq 1 ]]; then
  mkdir -p "$SNAPSHOT_DIR/postgres"
fi

echo "[*] 导出运行态目录 /data/openclaw -> $SNAPSHOT_DIR/openclaw"
docker exec "$SERVICE_CONTAINER" tar -C /data -cf - openclaw | tar -C "$SNAPSHOT_DIR" -xf -

echo "[*] 导出产品脚本目录 /app/bin -> $SNAPSHOT_DIR/bin"
docker exec "$SERVICE_CONTAINER" tar -C /app -cf - bin | tar -C "$SNAPSHOT_DIR" -xf -

if [[ "$EXPORT_DB" -eq 1 ]]; then
  if [[ -z "$POSTGRES_CONTAINER" ]]; then
    echo "[✗] 未找到 Postgres 容器，无法导出数据库。" >&2
    exit 1
  fi
  echo "[*] 导出 Postgres -> $SNAPSHOT_DIR/postgres/mission_control.sql.gz"
  docker exec "$POSTGRES_CONTAINER" sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --no-owner --no-privileges' \
    | gzip > "$SNAPSHOT_DIR/postgres/mission_control.sql.gz"
fi

python3 - "$SNAPSHOT_DIR/metadata.json" "$SERVICE_CONTAINER" "$POSTGRES_CONTAINER" "$EXPORT_DB" <<'PY'
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

metadata_path, service_container, postgres_container, export_db = sys.argv[1:5]

def inspect_json(target):
    if not target:
        return {}
    result = subprocess.run(
        ["docker", "inspect", target],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    return payload[0] if payload else {}

service = inspect_json(service_container)
postgres = inspect_json(postgres_container)
metadata = {
    "createdAt": datetime.now(timezone.utc).isoformat(),
    "serviceContainer": {
        "name": service.get("Name", "").lstrip("/"),
        "image": service.get("Config", {}).get("Image", ""),
    },
    "postgresContainer": {
        "name": postgres.get("Name", "").lstrip("/"),
        "image": postgres.get("Config", {}).get("Image", ""),
    } if postgres else None,
    "includesDatabase": export_db == "1",
    "snapshotContents": [
        "openclaw",
        "bin",
        "postgres/mission_control.sql.gz" if export_db == "1" else None,
    ],
}
metadata["snapshotContents"] = [item for item in metadata["snapshotContents"] if item]
with open(metadata_path, "w", encoding="utf-8") as fh:
    json.dump(metadata, fh, ensure_ascii=False, indent=2)
PY

ln -sfn "$TIMESTAMP" "$LATEST_LINK"

echo "[✓] 容器快照已导出: $SNAPSHOT_DIR"
echo "[✓] latest -> $LATEST_LINK"
