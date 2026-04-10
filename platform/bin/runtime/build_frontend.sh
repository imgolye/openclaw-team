#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR=""
INSTALL_DEPS=0
JSON_OUTPUT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --install)
      INSTALL_DEPS=1
      shift
      ;;
    --json)
      JSON_OUTPUT=1
      shift
      ;;
    --help)
      echo "用法: bash build_frontend.sh [--project-dir /path/to/repo] [--install] [--json]"
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$ROOT_DIR}"
FRONTEND_DIR="$PROJECT_DIR/apps/frontend"
FRONTEND_DIST_DIR="$FRONTEND_DIR/dist"

emit_result() {
  local ok="$1"
  local status="$2"
  local message="$3"
  local dist_path="${4:-$FRONTEND_DIST_DIR}"
  if [[ "$JSON_OUTPUT" -eq 1 ]]; then
    python3 - "$ok" "$status" "$message" "$dist_path" <<'PY'
import json
import sys

ok = str(sys.argv[1]).strip().lower() in {"1", "true", "yes"}
print(json.dumps({
    "ok": ok,
    "status": sys.argv[2],
    "message": sys.argv[3],
    "distPath": sys.argv[4],
}, ensure_ascii=False))
PY
  else
    if [[ "$ok" == "1" || "$ok" == "true" ]]; then
      echo "[✓] $message"
    else
      echo "[!] $message"
    fi
  fi
}

if [[ ! -d "$FRONTEND_DIR" || ! -f "$FRONTEND_DIR/package.json" ]]; then
  if [[ -f "$FRONTEND_DIST_DIR/index.html" ]]; then
    emit_result 1 "prebuilt-dist" "未找到 apps/frontend/package.json，但检测到预构建 apps/frontend/dist，跳过源码构建。" "$FRONTEND_DIST_DIR"
  else
    emit_result 0 "missing-source" "未找到 apps/frontend/package.json，且没有可用的 apps/frontend/dist。" "$FRONTEND_DIST_DIR"
  fi
  exit 0
fi

if ! command -v npm >/dev/null 2>&1; then
  if [[ -f "$FRONTEND_DIST_DIR/index.html" ]]; then
    emit_result 1 "prebuilt-dist" "未检测到 npm，但检测到预构建 apps/frontend/dist，继续复用现有前端产物。" "$FRONTEND_DIST_DIR"
  else
    emit_result 0 "missing-npm" "未检测到 npm，且没有可用的 apps/frontend/dist；前端 UI 在构建前不可用。" "$FRONTEND_DIST_DIR"
  fi
  exit 0
fi

cd "$FRONTEND_DIR"

if [[ ! -d node_modules ]]; then
  if [[ "$INSTALL_DEPS" -eq 1 ]]; then
    echo "[*] apps/frontend/node_modules 不存在，开始安装依赖..."
    npm install
  else
    if [[ -f "$FRONTEND_DIST_DIR/index.html" ]]; then
      emit_result 1 "prebuilt-dist" "apps/frontend/node_modules 不存在，但检测到预构建 apps/frontend/dist，继续复用现有前端产物。" "$FRONTEND_DIST_DIR"
    else
      emit_result 0 "missing-deps" "apps/frontend/node_modules 不存在，且没有可用的 apps/frontend/dist；请先安装依赖后再构建。" "$FRONTEND_DIST_DIR"
    fi
    exit 0
  fi
fi

if [[ "$JSON_OUTPUT" -eq 0 ]]; then
  echo "[*] 构建前端..."
fi
if npm run build >/tmp/mission-control-frontend-build.log 2>&1; then
  if [[ "$JSON_OUTPUT" -eq 0 ]]; then
    cat /tmp/mission-control-frontend-build.log
  fi
  emit_result 1 "built" "前端构建完成: $FRONTEND_DIST_DIR" "$FRONTEND_DIST_DIR"
  exit 0
fi

if [[ "$JSON_OUTPUT" -eq 0 ]]; then
  cat /tmp/mission-control-frontend-build.log >&2
fi
emit_result 0 "build-failed" "前端构建失败，请检查 frontend 源码或依赖安装状态。" "$FRONTEND_DIST_DIR"
exit 1
