#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://127.0.0.1:18890"
USERNAME="owner"
PASSWORD="Mission@2026!Owner"
TIMEOUT="20"
STARTUP_WAIT_SECONDS="120"
WITH_WRITE_CHECKS=0

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[✓]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
fail() { echo -e "${RED}[✗]${NC} $*"; exit 1; }

usage() {
  cat <<'EOF'
用法: bash platform/bin/verify/day_one_smoke_check.sh [--base-url URL] [--username owner] [--password PASSWORD] [--with-write-checks]

默认执行只读巡检：
1. /healthz
2. /readinessz
3. 登录
4. dashboard / memory system / 默认账号
5. dashboard 响应耗时

加上 --with-write-checks 时，会额外验证：
6. 创建一条临时任务
7. 创建并发送一条临时聊天线程
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="$2"
      shift 2
      ;;
    --username)
      USERNAME="$2"
      shift 2
      ;;
    --password)
      PASSWORD="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --startup-wait)
      STARTUP_WAIT_SECONDS="$2"
      shift 2
      ;;
    --with-write-checks)
      WITH_WRITE_CHECKS=1
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      fail "未知参数: $1"
      ;;
  esac
done

command -v curl >/dev/null 2>&1 || fail "缺少 curl"
command -v python3 >/dev/null 2>&1 || fail "缺少 python3"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
cookie_file="$tmp_dir/cookies.txt"
body_file="$tmp_dir/body.json"

request() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local code=""
  if [[ -n "$body" ]]; then
    code="$(
      curl -sS -L --max-time "$TIMEOUT" \
        -X "$method" \
        -H 'Content-Type: application/json' \
        -b "$cookie_file" \
        -c "$cookie_file" \
        --data "$body" \
        -o "$body_file" \
        -w '%{http_code}' \
        "${BASE_URL}${path}"
    )"
  else
    code="$(
      curl -sS -L --max-time "$TIMEOUT" \
        -X "$method" \
        -b "$cookie_file" \
        -c "$cookie_file" \
        -o "$body_file" \
        -w '%{http_code}' \
        "${BASE_URL}${path}"
    )"
  fi
  printf '%s' "$code"
}

json_get() {
  local expr="$1"
  python3 - "$body_file" "$expr" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8") or "{}")
value = eval(sys.argv[2], {"__builtins__": {}}, {"payload": payload})
if value is None:
    print("")
elif isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

assert_status() {
  local actual="$1"
  local expected="$2"
  local label="$3"
  [[ "$actual" == "$expected" ]] || fail "${label} 失败，HTTP ${actual}"
  ok "${label}"
}

wait_for_status() {
  local path="$1"
  local expected="$2"
  local label="$3"
  local deadline=$(( $(date +%s) + STARTUP_WAIT_SECONDS ))
  local code=""
  while [[ $(date +%s) -lt $deadline ]]; do
    code="$(request GET "$path" || true)"
    if [[ "$code" == "$expected" ]]; then
      ok "${label}"
      return 0
    fi
    sleep 2
  done
  fail "${label} 失败，HTTP ${code:-000}"
}

echo ""
echo "=== OpenClaw Team · 首日巡检 ==="
echo "地址: ${BASE_URL}"
echo ""

wait_for_status /healthz "200" "/healthz 可用"

wait_for_status /readinessz "200" "/readinessz 可用"

login_payload="$(python3 - "$USERNAME" "$PASSWORD" <<'PY'
import json
import sys
print(json.dumps({
    "mode": "password",
    "username": sys.argv[1],
    "password": sys.argv[2],
}, ensure_ascii=False))
PY
)"
code="$(request POST /api/auth/login "$login_payload")"
assert_status "$code" "200" "owner 登录"
login_ok="$(json_get "payload.get('ok')")"
[[ "$login_ok" == "true" ]] || fail "登录返回未成功"
action_token="$(json_get "payload.get('actionToken')")"
[[ -n "$action_token" ]] || fail "登录后没有拿到 actionToken"
ok "登录与 actionToken 正常"

dashboard_start="$(python3 - <<'PY'
import time
print(time.time())
PY
)"
code="$(request GET /api/dashboard)"
dashboard_end="$(python3 - <<'PY'
import time
print(time.time())
PY
)"
assert_status "$code" "200" "dashboard 可用"

dashboard_latency="$(
  python3 - "$dashboard_start" "$dashboard_end" <<'PY'
import sys
start = float(sys.argv[1])
end = float(sys.argv[2])
print(f"{end - start:.2f}")
PY
)"
ok "dashboard 响应时间 ${dashboard_latency}s"

memory_enabled="$(json_get "((payload.get('memorySystem') or {}).get('enabled'))")"
[[ "$memory_enabled" == "true" ]] || fail "memorySystem 未启用"
memory_authority="$(json_get "((payload.get('memorySystem') or {}).get('authority'))")"
ok "记忆系统正常 (${memory_authority})"

code="$(request GET /api/openclaw)"
assert_status "$code" "200" "openclaw 运行时快照可用"
agent_auth_ok="$(json_get "(((payload.get('openclaw') or {}).get('runtimeSync') or {}).get('auth') or {}).get('ok')")"
if [[ "$agent_auth_ok" != "true" ]]; then
  fail "Agent 鉴权未就绪：缺少 auth-profiles 或 provider API key"
fi
agent_auth_ready="$(json_get "(((payload.get('openclaw') or {}).get('runtimeSync') or {}).get('auth') or {}).get('readyCount')")"
agent_auth_total="$(json_get "(((payload.get('openclaw') or {}).get('runtimeSync') or {}).get('auth') or {}).get('targetCount')")"
ok "Agent 鉴权正常 (${agent_auth_ready}/${agent_auth_total})"

code="$(request GET /api/admin/bootstrap)"
assert_status "$code" "200" "后台治理数据可用"
users_ok="$(
  python3 - "$body_file" <<'PY'
import json
import sys
from pathlib import Path
payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8") or "{}")
users = {item.get("username") for item in ((payload.get("admin") or {}).get("users") or []) if isinstance(item, dict)}
print("true" if {"owner", "operator", "viewer"}.issubset(users) else "false")
PY
)"
[[ "$users_ok" == "true" ]] || fail "默认账号未补齐"
ok "默认账号正常"

deployment_mode="$(json_get "payload.get('deploymentMode') or ((payload.get('meta') or {}).get('deploymentMode'))")"
if [[ -n "$deployment_mode" ]]; then
  ok "部署模式 ${deployment_mode}"
fi

if [[ "$WITH_WRITE_CHECKS" == "1" ]]; then
  task_payload="$(python3 - "$action_token" <<'PY'
import json
import sys
import time
print(json.dumps({
    "actionToken": sys.argv[1],
    "title": f"首日巡检临时任务 {int(time.time())}",
    "goal": "验证任务创建链可用。",
    "details": "这是首日巡检自动创建的临时任务。",
    "includeDashboard": False,
    "preferFastRouting": True,
}, ensure_ascii=False))
PY
)"
  code="$(request POST /api/actions/task/create "$task_payload")"
  assert_status "$code" "200" "临时任务创建"
  task_id="$(json_get "((payload.get('task') or {}).get('id')) or payload.get('taskId')")"
  [[ -n "$task_id" ]] || fail "临时任务创建成功但没有 taskId"
  ok "临时任务已创建 (${task_id})"

  thread_payload="$(python3 - "$action_token" <<'PY'
import json
import sys
print(json.dumps({
    "actionToken": sys.argv[1],
    "title": "首日巡检临时线程",
    "primaryAgentId": "assistant",
    "participantAgentIds": ["assistant", "coo"],
}, ensure_ascii=False))
PY
)"
  code="$(request POST /api/actions/chat/thread/save "$thread_payload")"
  assert_status "$code" "200" "临时线程创建"
  thread_id="$(json_get "((payload.get('thread') or {}).get('id'))")"
  [[ -n "$thread_id" ]] || fail "临时线程创建成功但没有 threadId"
  ok "临时线程已创建 (${thread_id})"

  send_payload="$(python3 - "$action_token" "$thread_id" <<'PY'
import json
import sys
print(json.dumps({
    "actionToken": sys.argv[1],
    "threadId": sys.argv[2],
    "targetAgentId": "assistant",
    "dispatchMode": "direct",
    "message": "这是首日巡检消息，请确认聊天发送链可用。",
}, ensure_ascii=False))
PY
)"
  code="$(request POST /api/actions/chat/thread/send "$send_payload")"
  assert_status "$code" "200" "临时聊天发送"
  send_ok="$(json_get "payload.get('ok')")"
  [[ "$send_ok" == "true" ]] || fail "临时聊天发送返回未成功"
  ok "聊天发送链正常"
fi

echo ""
ok "首日巡检通过"
