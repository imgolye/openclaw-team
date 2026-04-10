#!/usr/bin/env bash
set -euo pipefail

OPENCLAW_DIR="$HOME/.openclaw"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BUILD_FRONTEND=0
SKIP_DASHBOARD_PREWARM="${MISSION_CONTROL_SKIP_DASHBOARD_PREWARM:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      OPENCLAW_DIR="$2"
      shift 2
      ;;
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --build-frontend)
      BUILD_FRONTEND=1
      shift
      ;;
    --help)
      echo "用法: bash sync_runtime_assets.sh [--dir ~/.openclaw] [--project-dir /path/to/mission-control] [--build-frontend]"
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      exit 1
      ;;
  esac
done

OPENCLAW_DIR="$(python3 - "$OPENCLAW_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
PROJECT_DIR="$(python3 - "$PROJECT_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"

CONFIG_PATH="$OPENCLAW_DIR/openclaw.json"
BACKEND_DIR="$PROJECT_DIR/backend"
VENDORED_SKILLS_DIR="$PROJECT_DIR/platform/vendor/openclaw-skills"
MAX_PERMISSION_SCRIPT="$PROJECT_DIR/platform/bin/runtime/max_agent_permissions.py"
INSTALL_LIB_DIR="$PROJECT_DIR/platform/bin/install/lib"
AUTH_SYNC_SCRIPT="$PROJECT_DIR/platform/bin/runtime/sync_agent_auth.py"

sync_tree() {
  local source_dir="$1"
  local target_dir="$2"
  if command -v rsync >/dev/null 2>&1; then
    rsync -a --delete --exclude '__pycache__' --exclude '.DS_Store' "$source_dir/" "$target_dir/"
    return 0
  fi
  python3 - "$source_dir" "$target_dir" <<'PY'
import shutil
import sys
from pathlib import Path

source = Path(sys.argv[1]).resolve()
target = Path(sys.argv[2]).resolve()
target.mkdir(parents=True, exist_ok=True)

for child in list(target.iterdir()):
    if child.name in {"__pycache__", ".DS_Store"}:
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)
        continue
    if not (source / child.name).exists():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            child.unlink(missing_ok=True)

for child in source.iterdir():
    if child.name in {"__pycache__", ".DS_Store"}:
        continue
    destination = target / child.name
    if child.is_dir():
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(child, destination)
    else:
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy2(child, destination)
PY
}

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[✗] 缺少配置文件: $CONFIG_PATH" >&2
  exit 1
fi

if [[ ! -d "$BACKEND_DIR" ]]; then
  echo "[✗] 缺少后端脚本目录: $BACKEND_DIR" >&2
  exit 1
fi

if [[ ! -d "$VENDORED_SKILLS_DIR" ]]; then
  echo "[✗] 缺少托管 skills 目录: $VENDORED_SKILLS_DIR" >&2
  exit 1
fi

echo "[*] 同步运行时资源到: $OPENCLAW_DIR"
echo "[*] 仓库目录: $PROJECT_DIR"

DASHBOARD_SCRIPT_SOURCE="$BACKEND_DIR/collaboration_dashboard.py"
if [[ ! -f "$DASHBOARD_SCRIPT_SOURCE" ]]; then
  echo "[✗] 缺少 dashboard 主脚本: $BACKEND_DIR/collaboration_dashboard.py" >&2
  exit 1
fi

PYTHONPATH="$INSTALL_LIB_DIR${PYTHONPATH:+:$PYTHONPATH}" python3 - "$OPENCLAW_DIR" "$PROJECT_DIR" <<'PY'
import json, sys
from pathlib import Path

from project_metadata import apply_local_runtime_model_provider_config, load_project_metadata, sanitize_openclaw_config, write_project_metadata

openclaw_dir = Path(sys.argv[1]).resolve()
project_dir = str(Path(sys.argv[2]).resolve())
config_path = openclaw_dir / "openclaw.json"
config = json.loads(config_path.read_text())
metadata = load_project_metadata(openclaw_dir, existing_config=config)
metadata["projectDir"] = project_dir
metadata["routerAgentId"] = next(
    (agent.get("id", "") for agent in config.get("agents", {}).get("list", []) if agent.get("default")),
    metadata.get("routerAgentId", ""),
)
write_project_metadata(openclaw_dir, metadata)
sanitized = sanitize_openclaw_config(config)
sanitized = apply_local_runtime_model_provider_config(sanitized, metadata)
mission_control = sanitized.get("missionControl") if isinstance(sanitized.get("missionControl"), dict) else {}
mission_control["projectDir"] = project_dir
sanitized["missionControl"] = mission_control
config_path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n")
PY

WORKSPACES=()
while IFS= read -r workspace; do
  [[ -n "$workspace" ]] && WORKSPACES+=("$workspace")
done < <(
  python3 - "$CONFIG_PATH" <<'PY'
import json, os, sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text())
for agent in config.get("agents", {}).get("list", []):
    workspace = os.path.abspath(os.path.expanduser(agent.get("workspace", "")))
    if workspace:
        print(workspace)
PY
)

for workspace in "${WORKSPACES[@]}"; do
  mkdir -p "$workspace/scripts" "$workspace/data"
  rm -rf \
    "$workspace/scripts/backend" \
    "$workspace/scripts/application" \
    "$workspace/scripts/adapters" \
    "$workspace/scripts/domain" \
    "$workspace/scripts/presentation" \
    "$workspace/scripts/services" \
    "$workspace/scripts/dispatchers" \
    "$workspace/scripts/stores" \
    "$workspace/scripts/integrations"
  rm -f "$workspace/scripts/"{dashboard_store.py,admin_service.py,chat_data_service.py,customer_access_service.py,orchestration_service.py,dashboard_data_service.py,desktop_service.py,management_service.py,runtime_service.py,route_aliases.py,query_route_dispatcher.py,task_command_dispatcher.py,agent_command_dispatcher.py,management_command_dispatcher.py,chat_command_dispatcher.py,platform_command_dispatcher.py,command_route_dispatcher.py,rest_route_dispatcher.py,http_route_dispatcher.py,env_utils.py}
  cp "$BACKEND_DIR/kanban_update.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/file_lock.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/model_decision_adapter.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/refresh_live_data.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/health_dashboard.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/env_utils.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/monitoring.py" "$workspace/scripts/"
  cp "$BACKEND_DIR/openapi_spec.py" "$workspace/scripts/"
  cp "$DASHBOARD_SCRIPT_SOURCE" "$workspace/scripts/collaboration_dashboard.py"
  sync_tree "$BACKEND_DIR" "$workspace/scripts/backend"
done

mkdir -p "$OPENCLAW_DIR/skills"
while IFS= read -r skill_dir; do
  [[ -n "$skill_dir" ]] || continue
  skill_name="$(basename "$skill_dir")"
  rm -rf "$OPENCLAW_DIR/skills/$skill_name"
  cp -R "$skill_dir" "$OPENCLAW_DIR/skills/$skill_name"
done < <(find "$VENDORED_SKILLS_DIR" -mindepth 1 -maxdepth 1 -type d | sort)

if [[ -f "$AUTH_SYNC_SCRIPT" ]]; then
  AUTH_SYNC_ARGS=(--dir "$OPENCLAW_DIR" --json)
  AUTH_SYNC_OVERWRITE=0
  if [[ -n "${OPENCLAW_AUTH_SOURCE_FILE:-}" ]]; then
    AUTH_SYNC_ARGS+=(--source-file "$OPENCLAW_AUTH_SOURCE_FILE")
    AUTH_SYNC_OVERWRITE=1
  fi
  if [[ -n "${OPENCLAW_AUTH_PROFILES_JSON:-}" || -n "${OPENCLAW_AUTH_PROFILES_B64:-}" || -n "${ZAI_API_KEY:-}" || -n "${BIGMODEL_API_KEY:-}" || -n "${ZHIPUAI_API_KEY:-}" || -n "${OPENAI_API_KEY:-}" || -n "${ANTHROPIC_API_KEY:-}" || -n "${GOOGLE_API_KEY:-}" || -n "${GEMINI_API_KEY:-}" || -n "${DEEPSEEK_API_KEY:-}" || -n "${QWEN_API_KEY:-}" || -n "${DASHSCOPE_API_KEY:-}" || -n "${OPENROUTER_API_KEY:-}" || -n "${XAI_API_KEY:-}" || -n "${MINIMAX_API_KEY:-}" || -n "${MINIMAX_CN_API_KEY:-}" ]]; then
    AUTH_SYNC_OVERWRITE=1
  fi
  if [[ "$AUTH_SYNC_OVERWRITE" -eq 1 ]]; then
    AUTH_SYNC_ARGS+=(--overwrite)
  fi
  AUTH_SYNC_JSON="$(python3 "$AUTH_SYNC_SCRIPT" "${AUTH_SYNC_ARGS[@]}")"
  AUTH_SYNC_SUMMARY="$(python3 - "$AUTH_SYNC_JSON" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
print("\t".join([
    str(payload.get("source") or "missing"),
    str(payload.get("writtenCount") or 0),
    str(payload.get("targetCount") or 0),
    ",".join(payload.get("providers") or []),
]))
PY
)"
  IFS=$'\t' read -r AUTH_SOURCE AUTH_WRITTEN AUTH_TOTAL AUTH_PROVIDERS <<< "$AUTH_SYNC_SUMMARY"
  if [[ "$AUTH_SOURCE" == "missing" ]]; then
    echo "[!] 未检测到 Agent 鉴权配置；团队通信可能失败" >&2
  elif [[ "${AUTH_WRITTEN:-0}" -gt 0 ]]; then
    echo "[✓] Agent 鉴权已同步: ${AUTH_WRITTEN}/${AUTH_TOTAL}${AUTH_PROVIDERS:+ · ${AUTH_PROVIDERS}}"
  else
    echo "[✓] Agent 鉴权已校验: ${AUTH_TOTAL}"
  fi
fi

if [[ -f "$MAX_PERMISSION_SCRIPT" ]]; then
  MAX_PERMISSION_JSON="$(python3 "$MAX_PERMISSION_SCRIPT" --dir "$OPENCLAW_DIR" --json)"
  MAX_PERMISSION_COUNT="$(python3 - "$MAX_PERMISSION_JSON" <<'PY'
import json
import sys
payload = json.loads(sys.argv[1])
print(payload.get("agentCount") or 0)
PY
)"
  echo "[✓] Agent 权限已提升到最大: ${MAX_PERMISSION_COUNT}"
fi

if [[ "$BUILD_FRONTEND" -eq 1 && -f "$PROJECT_DIR/platform/bin/runtime/build_frontend.sh" ]]; then
  bash "$PROJECT_DIR/platform/bin/runtime/build_frontend.sh" --project-dir "$PROJECT_DIR"
fi

FIRST_WORKSPACE="${WORKSPACES[0]:-}"
if [[ "$SKIP_DASHBOARD_PREWARM" != "1" && -n "$FIRST_WORKSPACE" && -f "$FIRST_WORKSPACE/scripts/collaboration_dashboard.py" ]]; then
  MISSION_CONTROL_SKIP_AUTOMATION_CYCLE=1 \
    python3 "$FIRST_WORKSPACE/scripts/collaboration_dashboard.py" --dir "$OPENCLAW_DIR" --quiet || true
fi

echo "[✓] 运行时脚本同步完成"
