#!/usr/bin/env bash
set -euo pipefail

# ============================================================
#  OpenClaw Team · 安装验证脚本
#  用法: bash validate.sh [--dir ~/.openclaw] [~/.openclaw]
# ============================================================

OPENCLAW_DIR="$HOME/.openclaw"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)
      OPENCLAW_DIR="$2"
      shift 2
      ;;
    --help)
      echo "用法: bash validate.sh [--dir ~/.openclaw] [~/.openclaw]"
      exit 0
      ;;
    *)
      OPENCLAW_DIR="$1"
      shift
      ;;
  esac
done

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
fail() { echo -e "${RED}[✗]${NC} $*"; ERRORS=$((ERRORS + 1)); }
summary_fail() { echo -e "${RED}[✗]${NC} $*"; }
file_mode() {
  local path="$1"
  if stat --version >/dev/null 2>&1; then
    stat -c "%a" "$path"
  else
    stat -f "%Lp" "$path"
  fi
}
expand_path() {
  python3 - "$1" <<'PY'
import os, sys
print(os.path.abspath(os.path.expanduser(sys.argv[1])))
PY
}

ERRORS=0
WARNINGS=0
FRONTEND_WARN=0

echo ""
echo "=== OpenClaw Team · 安装验证 ==="
echo "检查目录: $OPENCLAW_DIR"
echo ""

# 1. openclaw.json
echo "--- 配置文件 ---"
if [[ -f "$OPENCLAW_DIR/openclaw.json" ]]; then
  ok "openclaw.json 存在"
  AGENT_COUNT=$(python3 -c "import json; d=json.load(open('$OPENCLAW_DIR/openclaw.json')); print(len(d.get('agents',{}).get('list',[])))" 2>/dev/null || echo "0")
  SANDBOX_ENABLED_COUNT=$(python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
count = 0
for agent in config.get("agents", {}).get("list", []) or []:
    sandbox = agent.get("sandbox") if isinstance(agent, dict) else {}
    if isinstance(sandbox, dict) and str(sandbox.get("mode") or "").strip().lower() not in {"", "off"}:
        count += 1
defaults = (config.get("agents", {}) or {}).get("defaults", {})
default_sandbox = defaults.get("sandbox") if isinstance(defaults, dict) else {}
if isinstance(default_sandbox, dict) and str(default_sandbox.get("mode") or "").strip().lower() not in {"", "off"}:
    count += 1
print(count)
PY
  )
  SUBAGENT_RESTRICTED_COUNT=$(python3 - "$OPENCLAW_DIR/openclaw.json" <<'PY'
import json
import sys
from pathlib import Path

config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
agents = [item for item in config.get("agents", {}).get("list", []) or [] if isinstance(item, dict)]
agent_ids = [str(item.get("id") or "").strip() for item in agents if str(item.get("id") or "").strip()]
restricted = 0
for agent in agents:
    agent_id = str(agent.get("id") or "").strip()
    allowed = (agent.get("subagents") or {}).get("allowAgents") if isinstance(agent.get("subagents"), dict) else []
    allowed = {str(item or "").strip() for item in allowed if str(item or "").strip()}
    expected = {item for item in agent_ids if item and item != agent_id}
    if allowed != expected:
        restricted += 1
print(restricted)
PY
  )
  if [[ "$AGENT_COUNT" -ge 10 ]]; then
    ok "Agent 数量: $AGENT_COUNT"
  else
    fail "Agent 数量过少: $AGENT_COUNT (预期 ≥ 10)"
  fi
else
  fail "openclaw.json 不存在"
fi

if ! command -v docker >/dev/null 2>&1 && [[ "${SANDBOX_ENABLED_COUNT:-0}" -gt 0 ]]; then
  fail "检测到 ${SANDBOX_ENABLED_COUNT} 条 sandbox 配置，但当前环境没有 docker；Agent 任务会直接失败"
fi

if [[ "${SUBAGENT_RESTRICTED_COUNT:-0}" -gt 0 ]]; then
  fail "检测到 ${SUBAGENT_RESTRICTED_COUNT} 条 sessions_spawn / subagents 权限限制，当前不符合最大权限要求"
fi

# 2. .env
if [[ -f "$OPENCLAW_DIR/.env" ]]; then
  PERMS="$(file_mode "$OPENCLAW_DIR/.env" 2>/dev/null || echo "?")"
  if [[ "$PERMS" == "600" ]]; then
    ok ".env 权限正确 (600)"
  else
    warn ".env 权限: $PERMS (建议 600)"; WARNINGS=$((WARNINGS + 1))
  fi
else
  warn ".env 不存在 (可能无 secrets 配置)"; WARNINGS=$((WARNINGS + 1))
fi

DB_URL_CHECK_OUTPUT="$(
  python3 - "$OPENCLAW_DIR" <<'PY'
import os
import sys
from pathlib import Path

openclaw_dir = Path(sys.argv[1]).expanduser()
env_path = openclaw_dir / ".env"
database_url = ""
if env_path.exists():
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in {"MISSION_CONTROL_DATABASE_URL", "DATABASE_URL"} and value.strip():
            database_url = value.strip()
            break
if not database_url:
    print("missing")
    raise SystemExit(0)
print(database_url)
PY
)"

if [[ "$DB_URL_CHECK_OUTPUT" == "missing" ]]; then
  fail "未配置 PostgreSQL 连接。请在 $OPENCLAW_DIR/.env 中设置 MISSION_CONTROL_DATABASE_URL"
else
  ok "已检测到 PostgreSQL 连接配置"
  if python3 - "$DB_URL_CHECK_OUTPUT" <<'PY' >/dev/null 2>&1
import sys
import psycopg

with psycopg.connect(sys.argv[1]) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
PY
  then
    ok "PostgreSQL 连接可用"
  else
    fail "无法连接 PostgreSQL，或本机缺少 psycopg"
  fi
fi

# 3-7. 按 openclaw.json 中的 agent 清单逐项核验
echo ""
echo "--- Agent 文件完整性 ---"

AGENT_ROWS=()
if [[ -f "$OPENCLAW_DIR/openclaw.json" ]]; then
  while IFS= read -r row; do
    AGENT_ROWS+=("$row")
  done < <(
    python3 - <<'PY' "$OPENCLAW_DIR/openclaw.json"
import json, sys
with open(sys.argv[1]) as f:
    config = json.load(f)
for agent in config.get("agents", {}).get("list", []):
    print("\t".join([
        agent["id"],
        agent.get("workspace", ""),
        agent.get("agentDir", ""),
    ]))
PY
  )
fi

SOUL_COUNT=0
MEMORY_COUNT=0
MEMORY_DIR_COUNT=0
ORG_COUNT=0
THESIS_COUNT=0
FEEDBACK_COUNT=0
KB_COUNT=0
KANBAN_CFG_COUNT=0
SCRIPT_OK=0
AUTH_COUNT=0

for row in "${AGENT_ROWS[@]}"; do
  IFS=$'\t' read -r agent workspace agentdir <<< "$row"
  workspace="$(expand_path "$workspace")"
  agentdir="$(expand_path "$agentdir")"

  if [[ -z "$workspace" || ! -d "$workspace" ]]; then
    fail "缺少 workspace 目录: $agent (${workspace:-未配置})"
    continue
  fi
  if [[ -z "$agentdir" || ! -d "$agentdir" ]]; then
    fail "缺少 agentDir 目录: $agent (${agentdir:-未配置})"
  fi

  if [[ -n "$agentdir" && -f "$agentdir/auth-profiles.json" ]]; then
    AUTH_COUNT=$((AUTH_COUNT + 1))
  fi

  if [[ -f "$workspace/SOUL.md" ]]; then
    SOUL_COUNT=$((SOUL_COUNT + 1))
  else
    fail "缺少 SOUL.md: $agent"
  fi

  if [[ -f "$workspace/MEMORY.md" ]]; then
    MEMORY_COUNT=$((MEMORY_COUNT + 1))
  else
    fail "缺少 MEMORY.md: $agent"
  fi

  if [[ -d "$workspace/memory" ]]; then
    MEMORY_DIR_COUNT=$((MEMORY_DIR_COUNT + 1))
  else
    fail "缺少 memory/ 目录: $agent"
  fi

  if [[ -f "$workspace/shared-context/ORG-STRUCTURE.md" ]]; then
    ORG_COUNT=$((ORG_COUNT + 1))
  else
    fail "缺少 ORG-STRUCTURE.md: $agent"
  fi

  if [[ -f "$workspace/shared-context/THESIS.md" ]]; then
    THESIS_COUNT=$((THESIS_COUNT + 1))
  else
    fail "缺少 THESIS.md: $agent"
  fi

  if [[ -f "$workspace/shared-context/FEEDBACK-LOG.md" ]]; then
    FEEDBACK_COUNT=$((FEEDBACK_COUNT + 1))
  else
    fail "缺少 FEEDBACK-LOG.md: $agent"
  fi

  if [[ -f "$workspace/shared-context/knowledge-base/README.md" ]]; then
    KB_COUNT=$((KB_COUNT + 1))
  else
    fail "缺少 knowledge-base/README.md: $agent"
  fi

  if [[ -f "$workspace/data/kanban_config.json" ]]; then
    KANBAN_CFG_COUNT=$((KANBAN_CFG_COUNT + 1))
  else
    fail "缺少 kanban_config.json: $agent"
  fi

  if [[ -f "$workspace/scripts/kanban_update.py" ]] && [[ -f "$workspace/scripts/file_lock.py" ]] && [[ -f "$workspace/scripts/model_decision_adapter.py" ]] && [[ -f "$workspace/scripts/refresh_live_data.py" ]] && [[ -f "$workspace/scripts/health_dashboard.py" ]] && [[ -f "$workspace/scripts/env_utils.py" ]] && [[ -f "$workspace/scripts/monitoring.py" ]] && [[ -f "$workspace/scripts/openapi_spec.py" ]] && [[ -f "$workspace/scripts/collaboration_dashboard.py" ]] && [[ -f "$workspace/scripts/backend/__init__.py" ]] && [[ -f "$workspace/scripts/backend/application/services/runtime_core.py" ]] && [[ -f "$workspace/scripts/backend/application/services/dashboard_core.py" ]] && [[ -f "$workspace/scripts/backend/application/services/http_shell_core.py" ]] && [[ -f "$workspace/scripts/backend/application/services/memory_core.py" ]] && [[ -f "$workspace/scripts/backend/adapters/storage/dashboard.py" ]] && [[ -f "$workspace/scripts/backend/adapters/integrations/openclaw.py" ]] && [[ -f "$workspace/scripts/backend/adapters/integrations/wechat.py" ]] && [[ -f "$workspace/scripts/backend/domain/core/__init__.py" ]] && [[ -f "$workspace/scripts/backend/presentation/http/http.py" ]] && [[ -f "$workspace/scripts/backend/presentation/http/query.py" ]] && [[ -f "$workspace/scripts/backend/presentation/http/task.py" ]] && [[ -f "$workspace/scripts/backend/presentation/http/handler.py" ]]; then
    SCRIPT_OK=$((SCRIPT_OK + 1))
  else
    fail "缺少看板脚本: $agent"
  fi

done

ok "SOUL.md: $SOUL_COUNT 个"
ok "MEMORY.md: $MEMORY_COUNT 个"
ok "memory/: $MEMORY_DIR_COUNT 个"
ok "ORG-STRUCTURE.md: $ORG_COUNT 个"
ok "THESIS.md: $THESIS_COUNT 个"
ok "FEEDBACK-LOG.md: $FEEDBACK_COUNT 个"
ok "knowledge-base/README.md: $KB_COUNT 个"
ok "kanban_config.json: $KANBAN_CFG_COUNT 个"
ok "看板脚本: $SCRIPT_OK 个 workspace 已部署"
if [[ "$DB_URL_CHECK_OUTPUT" == "missing" ]]; then
  fail "task_records 存储未就绪：缺少 PostgreSQL 连接"
else
  TASK_STORE_CHECK="$(
    python3 - "$DB_URL_CHECK_OUTPUT" <<'PY' 2>/dev/null || echo "error"
import sys
import psycopg

with psycopg.connect(sys.argv[1]) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT workspace_id) FROM task_records")
        total, workspace_count = cur.fetchone()
print(f"{total}\t{workspace_count}")
PY
  )"
  if [[ "$TASK_STORE_CHECK" == "error" ]]; then
    fail "task_records 存储不可读"
  else
    IFS=$'\t' read -r TASK_RECORD_COUNT TASK_WORKSPACE_COUNT <<< "$TASK_STORE_CHECK"
    ok "task_records: ${TASK_RECORD_COUNT} 条，覆盖 ${TASK_WORKSPACE_COUNT} 个 workspace"
  fi
fi
if [[ "$AUTH_COUNT" -eq 0 ]]; then
  fail "未发现任何 agent auth-profiles.json；团队通信和任务执行会失败"
elif [[ "$AUTH_COUNT" -lt "${#AGENT_ROWS[@]}" ]]; then
  warn "Agent 鉴权覆盖不足: $AUTH_COUNT/${#AGENT_ROWS[@]}"; WARNINGS=$((WARNINGS + 1))
else
  ok "Agent 鉴权: $AUTH_COUNT/${#AGENT_ROWS[@]}"
fi

# 8. 前端分离构建状态
echo ""
echo "--- Product Frontend ---"
PROJECT_DIR=$(python3 - <<'PY' "$OPENCLAW_DIR/openclaw.json" 2>/dev/null || true
import json, sys
from pathlib import Path

config_path = Path(sys.argv[1])
openclaw_dir = config_path.parent
sidecar = openclaw_dir / "mission-control.json"
if sidecar.exists():
    try:
        metadata = json.loads(sidecar.read_text())
        print(metadata.get("projectDir", ""))
    except Exception:
        pass
elif config_path.exists():
    config = json.loads(config_path.read_text())
    print(config.get("missionControl", {}).get("projectDir", ""))
PY
)
PROJECT_DIR="${PROJECT_DIR:+$(expand_path "$PROJECT_DIR")}"

if [[ -n "$PROJECT_DIR" && -d "$PROJECT_DIR/apps/frontend" ]]; then
  if [[ -f "$PROJECT_DIR/apps/frontend/dist/index.html" ]]; then
    ok "前后端分离前端已构建: $PROJECT_DIR/apps/frontend/dist"
  else
    warn "检测到 apps/frontend/ 但尚未构建 dist，当前 UI 路由会返回 503"; WARNINGS=$((WARNINGS + 1)); FRONTEND_WARN=1
  fi
else
  warn "未检测到关联 apps/frontend/ 目录，当前安装只保证 API 可用"; WARNINGS=$((WARNINGS + 1)); FRONTEND_WARN=1
fi

# Summary
echo ""
echo "=== 验证结果 ==="
if [[ $ERRORS -eq 0 ]]; then
  ok "全部通过 (${WARNINGS} 个警告)"
  if [[ $FRONTEND_WARN -eq 1 ]]; then
    echo "提示: 如需启用新的前后端分离界面，请运行:"
    echo "  bash $PROJECT_DIR/platform/bin/runtime/build_frontend.sh --project-dir $PROJECT_DIR"
  fi
else
  summary_fail "${ERRORS} 个错误, ${WARNINGS} 个警告"
fi
echo ""
exit $ERRORS
