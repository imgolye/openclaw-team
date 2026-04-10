#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
SCHEMA_PATH="$PROJECT_DIR/platform/infra/database/postgres/schema.sql"
DATABASE_URL="${MISSION_CONTROL_DATABASE_URL:-}"

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/import_product_database_schema.sh [选项]

选项:
  --project-dir PATH
  --schema PATH
  --database-url URL
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --schema)
      SCHEMA_PATH="$2"
      shift 2
      ;;
    --database-url)
      DATABASE_URL="$2"
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
SCHEMA_PATH="$(python3 - "$SCHEMA_PATH" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

if [[ -z "$DATABASE_URL" ]]; then
  echo "[✗] 缺少数据库连接，请传 --database-url 或设置 MISSION_CONTROL_DATABASE_URL。" >&2
  exit 1
fi

if [[ ! -f "$SCHEMA_PATH" ]]; then
  echo "[✗] 缺少 schema 文件: $SCHEMA_PATH" >&2
  exit 1
fi

echo "[*] 导入正式数据库 schema -> $DATABASE_URL"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$SCHEMA_PATH"
echo "[✓] 已导入正式数据库 schema"
