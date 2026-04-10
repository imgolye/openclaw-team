#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUNTIME_PROFILE="${MISSION_CONTROL_RUNTIME_PROFILE:-host}"
PROFILE_RESOLVER="$PROJECT_DIR/platform/bin/runtime/resolve_runtime_profile.py"
AUTH_SYNC_SCRIPT="$PROJECT_DIR/platform/bin/runtime/sync_agent_auth.py"
HOST="${MISSION_CONTROL_HOST_BIND:-}"
PORT="${MISSION_CONTROL_HOST_PORT:-}"
DATABASE_URL="${MISSION_CONTROL_DATABASE_URL:-postgresql://gaolei@127.0.0.1:5432/mission_control}"
FRONTEND_DIST="${MISSION_CONTROL_FRONTEND_DIST:-}"
OPENCLAW_DIR="${MISSION_CONTROL_OPENCLAW_DIR:-}"
HOST_PAIRING_SOURCE_DIR="${MISSION_CONTROL_HOST_PAIRING_SOURCE_DIR:-}"
LOCAL_STATE_DIR="$PROJECT_DIR/local"
LOG_DIR="$LOCAL_STATE_DIR/logs"
JSON_OUTPUT=0
PYTHON_BIN="${MISSION_CONTROL_PYTHON_BIN:-}"
NODE_BIN="${MISSION_CONTROL_NODE_BIN:-}"
OPENCLAW_BIN="${MISSION_CONTROL_OPENCLAW_BIN:-}"

resolve_runtime_profile_value() {
  local field="$1"
  local fallback="${2:-}"
  local value=""
  if [[ -f "$PROFILE_RESOLVER" ]]; then
    value="$(python3 "$PROFILE_RESOLVER" --project-dir "$PROJECT_DIR" --profile "$RUNTIME_PROFILE" --field "$field" 2>/dev/null || true)"
  fi
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf '%s' "$fallback"
  fi
}

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x /opt/homebrew/bin/python3 ]]; then
    PYTHON_BIN="/opt/homebrew/bin/python3"
  else
    PYTHON_BIN="$(command -v python3)"
  fi
fi

if [[ -z "$NODE_BIN" ]]; then
  if [[ -x /opt/homebrew/bin/node ]]; then
    NODE_BIN="/opt/homebrew/bin/node"
  else
    NODE_BIN="$(command -v node || true)"
  fi
fi

if [[ -z "$OPENCLAW_BIN" ]]; then
  if [[ -x /opt/homebrew/bin/openclaw ]]; then
    OPENCLAW_BIN="/opt/homebrew/bin/openclaw"
  else
    OPENCLAW_BIN="$(command -v openclaw || true)"
  fi
fi

emit_json() {
  local ok="$1"
  local state="$2"
  local pid="${3:-}"
  local url="${4:-}"
  local message="${5:-}"
  python3 - "$ok" "$state" "$pid" "$url" "$LOG_FILE" "$message" <<'PY'
import json, sys
ok, state, pid, url, log_file, message = sys.argv[1:]
payload = {
    "ok": ok == "1",
    "state": state,
    "pid": int(pid) if pid.isdigit() else None,
    "url": url or None,
    "logFile": log_file or None,
    "message": message or None,
}
print(json.dumps(payload, ensure_ascii=False))
PY
}

usage() {
  cat <<'EOF'
用法: bash platform/bin/deploy/start_host_product.sh [选项]

选项:
  --project-dir PATH
  --runtime-profile NAME
  --host HOST
  --port PORT
  --database-url URL
  --frontend-dist PATH
  --openclaw-dir PATH
  --pairing-source-dir PATH
  --python-bin PATH
  --node-bin PATH
  --openclaw-bin PATH
  --json
  --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --runtime-profile)
      RUNTIME_PROFILE="$2"
      shift 2
      ;;
    --host)
      HOST="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --database-url)
      DATABASE_URL="$2"
      shift 2
      ;;
    --frontend-dist)
      FRONTEND_DIST="$2"
      shift 2
      ;;
    --openclaw-dir)
      OPENCLAW_DIR="$2"
      shift 2
      ;;
    --pairing-source-dir)
      HOST_PAIRING_SOURCE_DIR="$2"
      shift 2
      ;;
    --python-bin)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --node-bin)
      NODE_BIN="$2"
      shift 2
      ;;
    --openclaw-bin)
      OPENCLAW_BIN="$2"
      shift 2
      ;;
    --json)
      JSON_OUTPUT=1
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

if [[ -z "$HOST" ]]; then
  HOST="$(resolve_runtime_profile_value product.bind "127.0.0.1")"
fi
if [[ -z "$PORT" ]]; then
  PORT="$(resolve_runtime_profile_value product.port "18891")"
fi
if [[ -z "$FRONTEND_DIST" ]]; then
  FRONTEND_DIST="$(resolve_runtime_profile_value product.frontendDist "$PROJECT_DIR/apps/frontend/dist")"
fi
if [[ -z "$OPENCLAW_DIR" ]]; then
  OPENCLAW_DIR="$(resolve_runtime_profile_value openclaw.stateDir "$PROJECT_DIR")"
fi
if [[ -z "$HOST_PAIRING_SOURCE_DIR" ]]; then
  HOST_PAIRING_SOURCE_DIR="$(resolve_runtime_profile_value openclaw.pairingSourceDir "$HOME/.openclaw")"
fi
if [[ -z "${MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL:-}" ]]; then
  export MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL
  MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL="$(resolve_runtime_profile_value speechRuntime.baseUrl "http://127.0.0.1:8090/v1")"
fi
if [[ -z "${MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL:-}" ]]; then
  export MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL
  MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL="$(resolve_runtime_profile_value localRuntime.baseUrl "http://127.0.0.1:11434/v1")"
fi
export MISSION_CONTROL_RUNTIME_PROFILE="$RUNTIME_PROFILE"

PID_FILE="$LOG_DIR/host-product-${PORT}.pid"
LOG_FILE="$LOG_DIR/host-product-${PORT}.log"

PROJECT_DIR="$(python3 - "$PROJECT_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
FRONTEND_DIST="$("$PYTHON_BIN" - "$FRONTEND_DIST" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
OPENCLAW_DIR="$("$PYTHON_BIN" - "$OPENCLAW_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
HOST_PAIRING_SOURCE_DIR="$("$PYTHON_BIN" - "$HOST_PAIRING_SOURCE_DIR" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
)"
NODE_BIN="$("$PYTHON_BIN" - "$NODE_BIN" <<'PY'
import os, sys
value = str(sys.argv[1] or "").strip()
print(os.path.abspath(os.path.expanduser(value)) if value else "")
PY
)"
OPENCLAW_BIN="$("$PYTHON_BIN" - "$OPENCLAW_BIN" <<'PY'
import os, sys
value = str(sys.argv[1] or "").strip()
print(os.path.abspath(os.path.expanduser(value)) if value else "")
PY
)"

PATH_PREFIXS=()
for candidate in "$PYTHON_BIN" "$NODE_BIN" "$OPENCLAW_BIN"; do
  if [[ -n "$candidate" ]]; then
    candidate_dir="$(dirname "$candidate")"
    if [[ -d "$candidate_dir" ]]; then
      PATH_PREFIXS+=("$candidate_dir")
    fi
  fi
done
if [[ ${#PATH_PREFIXS[@]} -gt 0 ]]; then
  UNIQUE_PATH_PREFIX=""
  for candidate_dir in "${PATH_PREFIXS[@]}"; do
    if [[ ":$UNIQUE_PATH_PREFIX:" != *":$candidate_dir:"* ]]; then
      if [[ -n "$UNIQUE_PATH_PREFIX" ]]; then
        UNIQUE_PATH_PREFIX="$UNIQUE_PATH_PREFIX:$candidate_dir"
      else
        UNIQUE_PATH_PREFIX="$candidate_dir"
      fi
    fi
  done
  export PATH="$UNIQUE_PATH_PREFIX:$PATH"
fi

mkdir -p "$LOG_DIR"
mkdir -p "$OPENCLAW_DIR"

"$PYTHON_BIN" - "$OPENCLAW_DIR/.env" "$DATABASE_URL" <<'PY'
import sys
from pathlib import Path

env_path = Path(sys.argv[1])
database_url = str(sys.argv[2] or "").strip()
if not database_url:
    raise SystemExit(0)
lines = []
if env_path.exists():
    lines = env_path.read_text(encoding="utf-8").splitlines()
updated = False
next_lines = []
for line in lines:
    if line.startswith("MISSION_CONTROL_DATABASE_URL="):
        next_lines.append(f"MISSION_CONTROL_DATABASE_URL={database_url}")
        updated = True
        continue
    next_lines.append(line)
if not updated:
    next_lines.append(f"MISSION_CONTROL_DATABASE_URL={database_url}")
env_path.write_text("\n".join(next_lines).rstrip() + "\n", encoding="utf-8")
try:
    env_path.chmod(0o600)
except Exception:
    pass
PY

"$PYTHON_BIN" - "$OPENCLAW_DIR" "$HOST_PAIRING_SOURCE_DIR" "$PROJECT_DIR" <<'PY'
import json
import shutil
import sys
from pathlib import Path

target_dir = Path(sys.argv[1]).expanduser().resolve()
source_dir = Path(sys.argv[2]).expanduser().resolve()
project_dir = Path(sys.argv[3]).expanduser().resolve()
target_dir.mkdir(parents=True, exist_ok=True)

def load_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def load_agent_list(config):
    agents_payload = config.get("agents") if isinstance(config.get("agents"), dict) else {}
    items = agents_payload.get("list")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict) and str(item.get("id") or "").strip()]

source_config_path = source_dir / "openclaw.json"
baseline_config_path = project_dir / "openclaw.json"
target_config_path = target_dir / "openclaw.json"
source_config = load_json(source_config_path)
baseline_config = load_json(baseline_config_path)
target_config = load_json(target_config_path)

changed = False
seed_config = baseline_config if load_agent_list(baseline_config) else source_config
seed_agents_payload = seed_config.get("agents") if isinstance(seed_config.get("agents"), dict) else {}
target_agents_payload = target_config.get("agents") if isinstance(target_config.get("agents"), dict) else {}
target_agents = load_agent_list(target_config)
if seed_agents_payload and not target_agents:
    merged = dict(seed_config) if isinstance(seed_config, dict) else {}
    for key, value in target_config.items():
        if key == "agents":
            continue
        merged[key] = value
    merged_agents = dict(seed_agents_payload)
    if target_agents_payload:
        for key, value in target_agents_payload.items():
            if key == "list":
                continue
            merged_agents[key] = value
    merged_agents["list"] = load_agent_list(seed_config)
    merged["agents"] = merged_agents
    target_config = merged
    changed = True

source_gateway = source_config.get("gateway") if isinstance(source_config.get("gateway"), dict) else {}
target_gateway = target_config.get("gateway") if isinstance(target_config.get("gateway"), dict) else {}
needs_gateway_seed = not any(
    str(target_gateway.get(key) or "").strip()
    for key in ("bind", "mode", "port")
) or not (
    isinstance(target_gateway.get("auth"), dict)
    and str((target_gateway.get("auth") or {}).get("token") or "").strip()
)
if source_gateway and needs_gateway_seed:
    merged_gateway = dict(source_gateway)
    if isinstance(target_gateway.get("http"), dict):
        merged_gateway["http"] = target_gateway["http"]
    target_config["gateway"] = merged_gateway
    changed = True

tools = target_config.get("tools") if isinstance(target_config.get("tools"), dict) else {}
sessions = tools.get("sessions") if isinstance(tools.get("sessions"), dict) else {}
if sessions.get("visibility") != "all":
    sessions["visibility"] = "all"
    tools["sessions"] = sessions
    target_config["tools"] = tools
    changed = True

if changed:
    target_config_path.write_text(json.dumps(target_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

for relative in (
    "local/identity/device.json",
    "local/identity/device-auth.json",
    "local/agents/assistant/agent/auth-profiles.json",
    "local/agents/main/agent/auth-profiles.json",
    "agents/assistant/agent/auth-profiles.json",
    "agents/main/agent/auth-profiles.json",
):
    src = source_dir / relative
    if not src.exists():
        continue
    dst = target_dir / relative
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not dst.exists() or dst.read_bytes() != src.read_bytes():
        shutil.copy2(src, dst)
PY

"$PYTHON_BIN" - "$OPENCLAW_DIR" "$PROJECT_DIR" <<'PY'
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

target_dir = Path(sys.argv[1]).expanduser().resolve()
project_dir = Path(sys.argv[2]).expanduser().resolve()
config_path = target_dir / "openclaw.json"
mission_control_paths = (
    target_dir / "mission-control.json",
    project_dir / "mission-control.json",
)
backend_dir = project_dir / "backend"
render_script = project_dir / "platform" / "bin" / "install" / "lib" / "render_templates.py"
helper_scripts = (
    "kanban_update.py",
    "file_lock.py",
    "model_decision_adapter.py",
    "refresh_live_data.py",
    "health_dashboard.py",
    "env_utils.py",
    "monitoring.py",
    "openapi_spec.py",
    "collaboration_dashboard.py",
)


def load_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_theme_name():
    for path in mission_control_paths:
        payload = load_json(path)
        theme = str(payload.get("theme") or "").strip()
        if theme:
            return theme
    return "corporate"


def read_task_prefix(config: dict):
    mission_control = config.get("missionControl")
    if isinstance(mission_control, dict):
        task_prefix = str(mission_control.get("taskPrefix") or "").strip()
        if task_prefix:
            return task_prefix
    metadata = config.get("metadata")
    if isinstance(metadata, dict):
        task_prefix = str(metadata.get("taskPrefix") or "").strip()
        if task_prefix:
            return task_prefix
    return "TASK"


config = load_json(config_path)
agents_payload = config.get("agents") if isinstance(config.get("agents"), dict) else {}
agent_list = agents_payload.get("list") if isinstance(agents_payload.get("list"), list) else []
agent_ids = [str((agent or {}).get("id") or "").strip() for agent in agent_list if str((agent or {}).get("id") or "").strip()]
changed = False
mission_control = config.get("missionControl") if isinstance(config.get("missionControl"), dict) else {}
if mission_control.get("projectDir") != str(project_dir):
    mission_control["projectDir"] = str(project_dir)
    config["missionControl"] = mission_control
    changed = True

if not agent_ids:
    raise SystemExit(0)

theme_name = read_theme_name()
theme_file = project_dir / "platform" / "config" / "themes" / theme_name / "theme.json"
task_prefix = read_task_prefix(config)

needs_templates = False
needs_scripts = False

for agent in agent_list:
    if not isinstance(agent, dict):
        continue
    agent_id = str(agent.get("id") or "").strip()
    if not agent_id:
        continue
    workspace = target_dir / f"workspace-{agent_id}"
    agent_dir = target_dir / "agents" / agent_id / "agent"
    workspace.mkdir(parents=True, exist_ok=True)
    agent_dir.mkdir(parents=True, exist_ok=True)
    for relative in ("scripts", "data", "memory", "shared-context/knowledge-base", "skills"):
        (workspace / relative).mkdir(parents=True, exist_ok=True)
    if agent.get("workspace") != str(workspace):
        agent["workspace"] = str(workspace)
        changed = True
    if agent.get("agentDir") != str(agent_dir):
        agent["agentDir"] = str(agent_dir)
        changed = True
    template_targets = (
        workspace / "SOUL.md",
        workspace / "MEMORY.md",
        workspace / "memory" / "preferences.md",
        workspace / "memory" / "decisions.md",
        workspace / "memory" / "project-knowledge.md",
        workspace / "shared-context" / "ORG-STRUCTURE.md",
        workspace / "shared-context" / "THESIS.md",
        workspace / "shared-context" / "FEEDBACK-LOG.md",
        workspace / "shared-context" / "knowledge-base" / "README.md",
        workspace / "data" / "kanban_config.json",
    )
    if any(not path.exists() for path in template_targets):
        needs_templates = True
    script_targets = [workspace / "scripts" / name for name in helper_scripts]
    backend_targets = (
        workspace / "scripts" / "backend" / "__init__.py",
        workspace / "scripts" / "backend" / "application" / "services" / "runtime_core.py",
        workspace / "scripts" / "backend" / "application" / "services" / "dashboard_core.py",
        workspace / "scripts" / "backend" / "application" / "services" / "http_shell_core.py",
        workspace / "scripts" / "backend" / "application" / "services" / "memory_core.py",
        workspace / "scripts" / "backend" / "adapters" / "storage" / "dashboard.py",
        workspace / "scripts" / "backend" / "adapters" / "integrations" / "openclaw.py",
        workspace / "scripts" / "backend" / "adapters" / "integrations" / "wechat.py",
        workspace / "scripts" / "backend" / "domain" / "core" / "__init__.py",
        workspace / "scripts" / "backend" / "presentation" / "http" / "http.py",
        workspace / "scripts" / "backend" / "presentation" / "http" / "query.py",
        workspace / "scripts" / "backend" / "presentation" / "http" / "task.py",
        workspace / "scripts" / "backend" / "presentation" / "http" / "handler.py",
    )
    if any(not path.exists() for path in script_targets) or any(not path.exists() for path in backend_targets):
        needs_scripts = True

if changed:
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

runtime_mission_control_path = target_dir / "mission-control.json"
runtime_mission_control = load_json(runtime_mission_control_path)
if runtime_mission_control.get("projectDir") != str(project_dir):
    runtime_mission_control["projectDir"] = str(project_dir)
    runtime_mission_control_path.write_text(
        json.dumps(runtime_mission_control, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

runtime_config = load_json(config_path)
runtime_mission_control_cfg = runtime_config.get("missionControl") if isinstance(runtime_config.get("missionControl"), dict) else {}
if runtime_mission_control_cfg.get("projectDir") != str(project_dir):
    runtime_mission_control_cfg["projectDir"] = str(project_dir)
    runtime_config["missionControl"] = runtime_mission_control_cfg
    config_path.write_text(json.dumps(runtime_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

if needs_scripts:
    for agent_id in agent_ids:
        workspace = target_dir / f"workspace-{agent_id}"
        scripts_dir = workspace / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        for script_name in helper_scripts:
            src = backend_dir / script_name
            if src.exists():
                shutil.copy2(src, scripts_dir / script_name)
        shutil.copytree(backend_dir, scripts_dir / "backend", dirs_exist_ok=True)

if needs_templates and render_script.exists() and theme_file.exists():
    subprocess.run(
        [
            sys.executable,
            str(render_script),
            "--theme",
            str(theme_file),
            "--openclaw-dir",
            str(target_dir),
            "--task-prefix",
            task_prefix,
        ],
        check=True,
        env=os.environ.copy(),
    )
PY

PERMISSION_SCRIPT="$PROJECT_DIR/platform/bin/runtime/max_agent_permissions.py"
if [[ -f "$PERMISSION_SCRIPT" ]]; then
  "$PYTHON_BIN" "$PERMISSION_SCRIPT" --dir "$OPENCLAW_DIR" --preset max >/dev/null 2>&1 || true
fi

if [[ -f "$AUTH_SYNC_SCRIPT" ]]; then
  "$PYTHON_BIN" "$AUTH_SYNC_SCRIPT" --dir "$OPENCLAW_DIR" >/dev/null 2>&1 || true
fi

if [[ ! -d "$FRONTEND_DIST" ]]; then
  echo "[✗] 缺少前端构建产物目录: $FRONTEND_DIST" >&2
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 1 "running" "$existing_pid" "http://$HOST:$PORT" "host 产品已在运行"
    else
      echo "[*] host 产品已在运行: pid=$existing_pid port=$PORT"
    fi
    exit 0
  fi
  rm -f "$PID_FILE"
fi

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  listener_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
  if curl -fsS --max-time 2 "http://$HOST:$PORT/api/auth/session" >/dev/null 2>&1; then
    if [[ -n "$listener_pid" ]]; then
      echo "$listener_pid" > "$PID_FILE"
    fi
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 1 "running" "$listener_pid" "http://$HOST:$PORT" "host 产品已在运行（复用现有监听）"
    else
      echo "[*] host 产品已在运行（复用现有监听）: pid=${listener_pid:-unknown} port=$PORT"
    fi
    exit 0
  fi
  if [[ "$JSON_OUTPUT" == "1" ]]; then
    emit_json 0 "port_conflict" "$listener_pid" "http://$HOST:$PORT" "端口已被占用，请先释放或改端口"
  else
    echo "[✗] 端口 $PORT 已被占用，请先释放或改端口。" >&2
  fi
  exit 1
fi

pid="$("$PYTHON_BIN" - "$PROJECT_DIR" "$OPENCLAW_DIR" "$DATABASE_URL" "$PORT" "$HOST" "$FRONTEND_DIST" "$LOG_FILE" "$PYTHON_BIN" "$HOST_PAIRING_SOURCE_DIR" "$NODE_BIN" "$OPENCLAW_BIN" "$PATH" <<'PY'
import os, subprocess, sys

project_dir, openclaw_dir, database_url, port, host, frontend_dist, log_file, python_bin, pairing_source_dir, node_bin, openclaw_bin, runtime_path = sys.argv[1:]
env = os.environ.copy()
env["MISSION_CONTROL_DATABASE_URL"] = database_url
env["MISSION_CONTROL_RUNTIME_PROFILE"] = os.environ.get("MISSION_CONTROL_RUNTIME_PROFILE", "host")
env["PYTHONPATH"] = project_dir
env["OPENCLAW_STATE_DIR"] = openclaw_dir
env["OPENCLAW_CONFIG_PATH"] = os.path.join(openclaw_dir, "openclaw.json")
env["MISSION_CONTROL_HOST_PAIRING_SOURCE_DIR"] = pairing_source_dir
if runtime_path:
    env["PATH"] = runtime_path
if node_bin:
    env["MISSION_CONTROL_NODE_BIN"] = node_bin
if openclaw_bin:
    env["MISSION_CONTROL_OPENCLAW_BIN"] = openclaw_bin
with open(log_file, "ab", buffering=0) as stream:
    proc = subprocess.Popen(
        [
            python_bin,
            "-m",
            "backend.presentation.http.runtime",
            "--dir",
            openclaw_dir,
            "--serve",
            "--port",
            port,
            "--host",
            host,
            "--frontend-dist",
            frontend_dist,
        ],
        cwd=project_dir,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=stream,
        stderr=stream,
        start_new_session=True,
        close_fds=True,
    )
    print(proc.pid)
PY
)"
echo "$pid" > "$PID_FILE"

for _ in $(seq 1 30); do
  if curl -fsS "http://$HOST:$PORT/api/auth/session" >/dev/null 2>&1; then
    listener_pid="$(lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN | head -n 1 || true)"
    if [[ -n "$listener_pid" ]]; then
      pid="$listener_pid"
      echo "$pid" > "$PID_FILE"
    fi
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 1 "running" "$pid" "http://$HOST:$PORT" "host 产品已启动"
    else
      echo "[✓] host 产品已启动: http://$HOST:$PORT"
      echo "[✓] pid=$pid log=$LOG_FILE"
    fi
    exit 0
  fi
  if ! kill -0 "$pid" >/dev/null 2>&1; then
    if [[ "$JSON_OUTPUT" == "1" ]]; then
      emit_json 0 "failed" "$pid" "http://$HOST:$PORT" "host 产品启动失败，请检查日志"
    else
      echo "[✗] host 产品启动失败，请检查日志: $LOG_FILE" >&2
    fi
    rm -f "$PID_FILE"
    exit 1
  fi
  sleep 1
done

kill "$pid" >/dev/null 2>&1 || true
sleep 1
kill -9 "$pid" >/dev/null 2>&1 || true
rm -f "$PID_FILE"
if [[ "$JSON_OUTPUT" == "1" ]]; then
  emit_json 0 "timeout" "$pid" "http://$HOST:$PORT" "host 产品启动超时，请检查日志"
else
  echo "[✗] host 产品启动超时，请检查日志: $LOG_FILE" >&2
fi
exit 1
