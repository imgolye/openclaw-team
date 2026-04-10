#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
SERVICE_NAME="mission-control"
POSTGRES_SERVICE="postgres"
SHARED_STATE_SERVICE="sherpa-onnx-tts"
BACKUP_ROOT="$PROJECT_DIR/local/backups/mission-control-container"
INPUT_PATH="$BACKUP_ROOT/latest"
IMPORT_DB=1
IMPORT_STATE=1
IMPORT_BIN=1

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/import_container_snapshot.sh [选项]

选项:
  --project-dir PATH
  --compose-file PATH
  --service NAME
  --postgres-service NAME
  --shared-state-service NAME
  --input PATH
  --skip-db
  --skip-state
  --skip-bin
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
    --shared-state-service)
      SHARED_STATE_SERVICE="$2"
      shift 2
      ;;
    --input)
      INPUT_PATH="$2"
      shift 2
      ;;
    --skip-db)
      IMPORT_DB=0
      shift
      ;;
    --skip-state)
      IMPORT_STATE=0
      shift
      ;;
    --skip-bin)
      IMPORT_BIN=0
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
INPUT_PATH="$(python3 - "$INPUT_PATH" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

if ! command -v docker >/dev/null 2>&1; then
  echo "[✗] 未检测到 docker。" >&2
  exit 1
fi

if [[ ! -d "$INPUT_PATH" ]]; then
  echo "[✗] 快照目录不存在: $INPUT_PATH" >&2
  exit 1
fi

if [[ "$IMPORT_STATE" -eq 1 && ! -d "$INPUT_PATH/openclaw" ]]; then
  echo "[✗] 快照缺少 openclaw 目录: $INPUT_PATH/openclaw" >&2
  exit 1
fi

if [[ "$IMPORT_BIN" -eq 1 && ! -d "$INPUT_PATH/bin" ]]; then
  echo "[✗] 快照缺少 bin 目录: $INPUT_PATH/bin" >&2
  exit 1
fi

if [[ "$IMPORT_DB" -eq 1 && ! -f "$INPUT_PATH/postgres/mission_control.sql.gz" ]]; then
  echo "[✗] 快照缺少数据库导出: $INPUT_PATH/postgres/mission_control.sql.gz" >&2
  exit 1
fi

SERVICE_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q "$SERVICE_NAME")"
if [[ -z "$SERVICE_CONTAINER" ]]; then
  echo "[✗] 未找到服务容器: $SERVICE_NAME" >&2
  exit 1
fi

POSTGRES_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q "$POSTGRES_SERVICE" 2>/dev/null || true)"

STATE_MOUNT_SOURCE=""
if [[ "$IMPORT_STATE" -eq 1 ]]; then
  STATE_MOUNT_SOURCE="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data/openclaw"}}{{.Source}}{{end}}{{end}}' "$SERVICE_CONTAINER")"
  if [[ -z "$STATE_MOUNT_SOURCE" ]]; then
    echo "[✗] 未找到 /data/openclaw 对应的挂载源。" >&2
    exit 1
  fi
fi

echo "[*] 停止共享运行态服务"
docker compose -f "$COMPOSE_FILE" stop "$SERVICE_NAME" "$SHARED_STATE_SERVICE" >/dev/null 2>&1 || true

if [[ "$IMPORT_STATE" -eq 1 ]]; then
  echo "[*] 导入运行态目录 -> $STATE_MOUNT_SOURCE"
  mkdir -p "$STATE_MOUNT_SOURCE"
  rsync -a --delete "$INPUT_PATH/openclaw/" "$STATE_MOUNT_SOURCE/"
fi

if [[ "$IMPORT_BIN" -eq 1 ]]; then
  echo "[*] 导入产品脚本 -> $PROJECT_DIR/bin"
  rsync -a --delete "$INPUT_PATH/bin/" "$PROJECT_DIR/bin/"
fi

if [[ "$IMPORT_DB" -eq 1 ]]; then
  if [[ -z "$POSTGRES_CONTAINER" ]]; then
    echo "[✗] 未找到 Postgres 容器。" >&2
    exit 1
  fi
  echo "[*] 启动 Postgres 以恢复数据库"
  docker compose -f "$COMPOSE_FILE" up -d "$POSTGRES_SERVICE" >/dev/null
  echo "[*] 恢复数据库"
  docker exec "$POSTGRES_CONTAINER" sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; psql -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '\''$POSTGRES_DB'\'' AND pid <> pg_backend_pid();"' >/dev/null
  docker exec "$POSTGRES_CONTAINER" sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; dropdb -U "$POSTGRES_USER" --if-exists "$POSTGRES_DB" && createdb -U "$POSTGRES_USER" "$POSTGRES_DB"'
  gzip -dc "$INPUT_PATH/postgres/mission_control.sql.gz" \
    | docker exec -i "$POSTGRES_CONTAINER" sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1'
fi

echo "[*] 重启服务"
docker compose -f "$COMPOSE_FILE" up -d "$SHARED_STATE_SERVICE" "$SERVICE_NAME"

echo "[✓] 快照导入完成: $INPUT_PATH"
