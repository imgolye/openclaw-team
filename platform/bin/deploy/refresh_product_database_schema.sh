#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
POSTGRES_SERVICE="postgres"
OUTPUT_PATH="$PROJECT_DIR/platform/infra/database/postgres/schema.sql"

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/refresh_product_database_schema.sh [选项]

选项:
  --project-dir PATH
  --compose-file PATH
  --postgres-service NAME
  --output PATH
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
    --postgres-service)
      POSTGRES_SERVICE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="$2"
      shift 2
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
OUTPUT_PATH="$(python3 - "$OUTPUT_PATH" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

if ! command -v docker >/dev/null 2>&1; then
  echo "[✗] 未检测到 docker。" >&2
  exit 1
fi

POSTGRES_CONTAINER="$(docker compose -f "$COMPOSE_FILE" ps -q "$POSTGRES_SERVICE")"
if [[ -z "$POSTGRES_CONTAINER" ]]; then
  echo "[✗] 未找到 Postgres 容器: $POSTGRES_SERVICE" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"

TMP_PATH="${OUTPUT_PATH}.tmp"
echo "[*] 导出正式数据库 schema -> $OUTPUT_PATH"
docker exec "$POSTGRES_CONTAINER" sh -lc 'export PGPASSWORD="$POSTGRES_PASSWORD"; pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --schema-only --no-owner --no-privileges' > "$TMP_PATH"

python3 - "$TMP_PATH" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
lines = []
for line in text.splitlines():
    if line.startswith("\\restrict ") or line.startswith("\\unrestrict "):
        continue
    lines.append(line)
normalized = "\n".join(lines).rstrip() + "\n"
path.write_text(normalized, encoding="utf-8")
PY

mv "$TMP_PATH" "$OUTPUT_PATH"
echo "[✓] 已刷新正式数据库 schema: $OUTPUT_PATH"
