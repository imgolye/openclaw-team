from __future__ import annotations

import re
import secrets
import sys
import threading


class _DelegatedSymbol:
    def __init__(self, name):
        self._name = name

    def _resolve(self):
        return getattr(_svc(), self._name)

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._resolve(), attr)

    def __iter__(self):
        return iter(self._resolve())

    def __bool__(self):
        return bool(self._resolve())

    def __len__(self):
        return len(self._resolve())

    def __contains__(self, item):
        return item in self._resolve()

    def __getitem__(self, key):
        return self._resolve()[key]

    def __enter__(self):
        return self._resolve().__enter__()

    def __exit__(self, exc_type, exc, tb):
        return self._resolve().__exit__(exc_type, exc, tb)

    def __eq__(self, other):
        return self._resolve() == other

    def __hash__(self):
        return hash(self._resolve())

    def __repr__(self):
        return repr(self._resolve())

    def __str__(self):
        return str(self._resolve())

    def __int__(self):
        return int(self._resolve())

    def __float__(self):
        return float(self._resolve())

    def __index__(self):
        return int(self._resolve())

    def __lt__(self, other):
        return self._resolve() < other

    def __le__(self, other):
        return self._resolve() <= other

    def __gt__(self, other):
        return self._resolve() > other

    def __ge__(self, other):
        return self._resolve() >= other


def _svc():
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        return module
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        return module
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        return main
    import importlib

    try:
        return importlib.import_module("backend.collaboration_dashboard")
    except ModuleNotFoundError:
        return importlib.import_module("collaboration_dashboard")

MANAGEMENT_AUTOMATION_MODE_ASSISTIVE = _DelegatedSymbol("MANAGEMENT_AUTOMATION_MODE_ASSISTIVE")
MANAGEMENT_AUTOMATION_MODE_FULL_AUTO = _DelegatedSymbol("MANAGEMENT_AUTOMATION_MODE_FULL_AUTO")
Path = _DelegatedSymbol("Path")
TASK_CREATE_MODEL_DECISION_TIMEOUT_SECONDS = _DelegatedSymbol("TASK_CREATE_MODEL_DECISION_TIMEOUT_SECONDS")
TASK_EXECUTION_DISPATCH_FUTURES = _DelegatedSymbol("TASK_EXECUTION_DISPATCH_FUTURES")
TASK_EXECUTION_DISPATCH_FUTURES_LOCK = _DelegatedSymbol("TASK_EXECUTION_DISPATCH_FUTURES_LOCK")
TASK_EXECUTION_DISPATCH_POOL = _DelegatedSymbol("TASK_EXECUTION_DISPATCH_POOL")
TASK_EXECUTION_REPAIR_BATCH_SIZE = _DelegatedSymbol("TASK_EXECUTION_REPAIR_BATCH_SIZE")
TASK_EXECUTION_REPAIR_DISPATCHED_MINUTES = _DelegatedSymbol("TASK_EXECUTION_REPAIR_DISPATCHED_MINUTES")
TASK_EXECUTION_REPAIR_FAILED_MINUTES = _DelegatedSymbol("TASK_EXECUTION_REPAIR_FAILED_MINUTES")
TASK_EXECUTION_REPAIR_MAX_ATTEMPTS = _DelegatedSymbol("TASK_EXECUTION_REPAIR_MAX_ATTEMPTS")
TASK_EXECUTION_REPAIR_RESET_MINUTES = _DelegatedSymbol("TASK_EXECUTION_REPAIR_RESET_MINUTES")
TASK_EXECUTION_REPAIR_SCHEDULED_MINUTES = _DelegatedSymbol("TASK_EXECUTION_REPAIR_SCHEDULED_MINUTES")
TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS = _DelegatedSymbol("TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS")
TEAM_CONVERSATION_MAX_PARALLEL = _DelegatedSymbol("TEAM_CONVERSATION_MAX_PARALLEL")
TEAM_CONVERSATION_RELAY_REPLY_LIMIT = _DelegatedSymbol("TEAM_CONVERSATION_RELAY_REPLY_LIMIT")
TEAM_CONVERSATION_RELAY_STAGGER_SECONDS = _DelegatedSymbol("TEAM_CONVERSATION_RELAY_STAGGER_SECONDS")
TEAM_CONVERSATION_STAGGER_SECONDS = _DelegatedSymbol("TEAM_CONVERSATION_STAGGER_SECONDS")
TERMINAL_STATES = _DelegatedSymbol("TERMINAL_STATES")
_discard_task_execution_dispatch_future = _DelegatedSymbol("_discard_task_execution_dispatch_future")
agent_runtime_identity_payload = _DelegatedSymbol("agent_runtime_identity_payload")
analyze_task_intelligence = _DelegatedSymbol("analyze_task_intelligence")
append_audit_event = _DelegatedSymbol("append_audit_event")
apply_conversation_fanout_stagger = _DelegatedSymbol("apply_conversation_fanout_stagger")
apply_team_working_memory = _DelegatedSymbol("apply_team_working_memory")
apply_turn_guidance_to_message = _DelegatedSymbol("apply_turn_guidance_to_message")
bootstrap_task_execution_state = _DelegatedSymbol("bootstrap_task_execution_state")
build_company_auto_operation_profile = _DelegatedSymbol("build_company_auto_operation_profile")
build_human_turn_anchor_payload = _DelegatedSymbol("build_human_turn_anchor_payload")
build_human_turn_profile_payload = _DelegatedSymbol("build_human_turn_profile_payload")
build_label_maps = _DelegatedSymbol("build_label_maps")
build_task_execution_message = _DelegatedSymbol("build_task_execution_message")
build_task_internal_discussion_message = _DelegatedSymbol("build_task_internal_discussion_message")
build_task_team_fanout_message = _DelegatedSymbol("build_task_team_fanout_message")
build_task_team_member_message = _DelegatedSymbol("build_task_team_member_message")
build_task_team_sync_message = _DelegatedSymbol("build_task_team_sync_message")
build_team_collaboration_summary = _DelegatedSymbol("build_team_collaboration_summary")
build_team_coordination_relay_message = _DelegatedSymbol("build_team_coordination_relay_message")
choose_task_workflow_resolution = _DelegatedSymbol("choose_task_workflow_resolution")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_auto_operation_profile = _DelegatedSymbol("compact_auto_operation_profile")
compact_company_auto_operation_runtime = _DelegatedSymbol("compact_company_auto_operation_runtime")
compact_task_long_term_memory = _DelegatedSymbol("compact_task_long_term_memory")
conversation_reply_preview = _DelegatedSymbol("conversation_reply_preview")
create_management_run_for_task = _DelegatedSymbol("create_management_run_for_task")
current_agent_for_task = _DelegatedSymbol("current_agent_for_task")
current_management_automation_mode = _DelegatedSymbol("current_management_automation_mode")
datetime = _DelegatedSymbol("datetime")
deepcopy = _DelegatedSymbol("deepcopy")
discussion_error = _DelegatedSymbol("discussion_error")
dispatch_error = _DelegatedSymbol("dispatch_error")
enrich_workflow_binding_with_branches = _DelegatedSymbol("enrich_workflow_binding_with_branches")
ensure_default_team_mode = _DelegatedSymbol("ensure_default_team_mode")
ensure_planning_bundle = _DelegatedSymbol("ensure_planning_bundle")
ensure_task_execution_team_thread = _DelegatedSymbol("ensure_task_execution_team_thread")
evaluate_routing_decision = _DelegatedSymbol("evaluate_routing_decision")
existing_task_team_thread = _DelegatedSymbol("existing_task_team_thread")
find_duplicate_active_task = _DelegatedSymbol("find_duplicate_active_task")
get_router_agent_id = _DelegatedSymbol("get_router_agent_id")
latest_management_run_for_task = _DelegatedSymbol("latest_management_run_for_task")
latest_routing_decision_for_task = _DelegatedSymbol("latest_routing_decision_for_task")
link_task_to_agent_team = _DelegatedSymbol("link_task_to_agent_team")
load_agents = _DelegatedSymbol("load_agents")
load_config = _DelegatedSymbol("load_config")
load_model_execution_architecture_payload = _DelegatedSymbol("load_model_execution_architecture_payload")
load_json = _DelegatedSymbol("load_json")
load_kanban_config = _DelegatedSymbol("load_kanban_config")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
logging = _DelegatedSymbol("logging")
merge_tasks = _DelegatedSymbol("merge_tasks")
merge_team_policy_state = _DelegatedSymbol("merge_team_policy_state")
normalize_chat_dispatch_mode = _DelegatedSymbol("normalize_chat_dispatch_mode")
now_iso = _DelegatedSymbol("now_iso")
now_utc = _DelegatedSymbol("now_utc")
order_agent_ids_for_human_turns = _DelegatedSymbol("order_agent_ids_for_human_turns")
parse_iso = _DelegatedSymbol("parse_iso")
parse_transcript_items = _DelegatedSymbol("parse_transcript_items")
patch_task_routing_metadata = _DelegatedSymbol("patch_task_routing_metadata")
patch_task_team_assignment_metadata = _DelegatedSymbol("patch_task_team_assignment_metadata")
perform_conversation_fanout = _DelegatedSymbol("perform_conversation_fanout")
planning_binding_from_payload = _DelegatedSymbol("planning_binding_from_payload")
reset_task_execution_bootstrap = _DelegatedSymbol("reset_task_execution_bootstrap")
resolve_agent_team_record = _DelegatedSymbol("resolve_agent_team_record")
resolve_default_task_team_record = _DelegatedSymbol("resolve_default_task_team_record")
resolve_planning_project_dir = _DelegatedSymbol("resolve_planning_project_dir")
resolve_team_execution_agent = _DelegatedSymbol("resolve_team_execution_agent")
run_python_script = _DelegatedSymbol("run_python_script")
runtime_script_path = _DelegatedSymbol("runtime_script_path")
safe_chat_attachments = _DelegatedSymbol("safe_chat_attachments")
safe_chat_mentions = _DelegatedSymbol("safe_chat_mentions")
safe_list = _DelegatedSymbol("safe_list")
seed_task_long_term_memory_payload = _DelegatedSymbol("seed_task_long_term_memory_payload")
select_human_turn_targets = _DelegatedSymbol("select_human_turn_targets")
session_id_for_transcript_path = _DelegatedSymbol("session_id_for_transcript_path")
session_transcript_path = _DelegatedSymbol("session_transcript_path")
should_use_model_task_intelligence = _DelegatedSymbol("should_use_model_task_intelligence")
store_list_routing_decisions = _DelegatedSymbol("store_list_routing_decisions")
store_get_task_record = _DelegatedSymbol("store_get_task_record")
store_save_chat_message = _DelegatedSymbol("store_save_chat_message")
store_save_chat_thread = _DelegatedSymbol("store_save_chat_thread")
store_save_routing_decision = _DelegatedSymbol("store_save_routing_decision")
submit_task_execution_background = _DelegatedSymbol("submit_task_execution_background")
submit_task_execution_background_deferred = _DelegatedSymbol("submit_task_execution_background_deferred")
summarize_internal_discussion_context = _DelegatedSymbol("summarize_internal_discussion_context")
summarize_task_execution_text = _DelegatedSymbol("summarize_task_execution_text")
task_coordination_protocol_snapshot = _DelegatedSymbol("task_coordination_protocol_snapshot")
task_effective_team_id = _DelegatedSymbol("task_effective_team_id")
task_execution_bootstrap_for_task = _DelegatedSymbol("task_execution_bootstrap_for_task")
task_execution_session_id = _DelegatedSymbol("task_execution_session_id")
task_execution_transcript_health = _DelegatedSymbol("task_execution_transcript_health")
task_has_meaningful_progress = _DelegatedSymbol("task_has_meaningful_progress")
task_internal_discussion_plan = _DelegatedSymbol("task_internal_discussion_plan")
build_task_long_term_memory_payload = _DelegatedSymbol("build_task_long_term_memory_payload")
task_route_meta_payload = _DelegatedSymbol("task_route_meta_payload")
task_team_participant_agent_ids = _DelegatedSymbol("task_team_participant_agent_ids")
team_memory_trace_payload = _DelegatedSymbol("team_memory_trace_payload")
team_state_packet_payload = _DelegatedSymbol("team_state_packet_payload")
timezone = _DelegatedSymbol("timezone")
update_task_execution_bootstrap = _DelegatedSymbol("update_task_execution_bootstrap")
update_task_team_dispatch_state = _DelegatedSymbol("update_task_team_dispatch_state")
task_execution_agent_lock = _DelegatedSymbol("task_execution_agent_lock")
store_list_chat_messages = _DelegatedSymbol("store_list_chat_messages")
store_list_agent_teams = _DelegatedSymbol("store_list_agent_teams")
store_list_chat_threads = _DelegatedSymbol("store_list_chat_threads")
store_get_chat_thread = _DelegatedSymbol("store_get_chat_thread")
publish_chat_thread_stream_event = _DelegatedSymbol("publish_chat_thread_stream_event")
project_memory_records_async = _DelegatedSymbol("project_memory_records_async")
perform_conversation_send = _DelegatedSymbol("perform_conversation_send")
select_persistable_conversation_session_id = _DelegatedSymbol("select_persistable_conversation_session_id")
normalize_team_context_lines = _DelegatedSymbol("normalize_team_context_lines")
maybe_prepare_chat_thread_context_compression = _DelegatedSymbol("maybe_prepare_chat_thread_context_compression")
maybe_forward_chat_thread_reply_to_wechat = _DelegatedSymbol("maybe_forward_chat_thread_reply_to_wechat")
maybe_attach_chat_thread_voice_reply = _DelegatedSymbol("maybe_attach_chat_thread_voice_reply")
invalidate_dashboard_bundle_cache = _DelegatedSymbol("invalidate_dashboard_bundle_cache")
chat_thread_session_id = _DelegatedSymbol("chat_thread_session_id")
build_memory_projection_payloads = _DelegatedSymbol("build_memory_projection_payloads")
L_DONE = _DelegatedSymbol("L_DONE")
atomic_task_store_update = _DelegatedSymbol("atomic_task_store_update")
company_auto_operation_prompt_lines = _DelegatedSymbol("company_auto_operation_prompt_lines")
dispatch_state_relay_target_ids = _DelegatedSymbol("dispatch_state_relay_target_ids")
record_routing_outcome = _DelegatedSymbol("record_routing_outcome")
resolve_team_default_dispatch_mode = _DelegatedSymbol("resolve_team_default_dispatch_mode")
should_task_start_with_internal_discussion = _DelegatedSymbol("should_task_start_with_internal_discussion")
task_long_term_memory_prompt_lines = _DelegatedSymbol("task_long_term_memory_prompt_lines")
task_workspace_for_task = _DelegatedSymbol("task_workspace_for_task")
team_collaboration_protocol = _DelegatedSymbol("team_collaboration_protocol")
team_runtime_meta = _DelegatedSymbol("team_runtime_meta")


TASK_EXECUTION_AGENT_LOCKS = {}
TASK_EXECUTION_AGENT_LOCKS_GUARD = threading.Lock()

def build_task_execution_message(task_id, title, remark="", workflow_binding=None, team=None, linked_run_id="", linked_run_title="", auto_operation_profile=None, auto_operation_runtime=None, task_long_term_memory=None):
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    team = team if isinstance(team, dict) else {}
    workflow_name = str(workflow_binding.get("workflowName") or "").strip()
    branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
    branch_label = str(branch.get("label") or "").strip()
    parts = [
        f"请接手任务 {task_id}。",
        f"任务标题：{title or task_id}",
    ]
    team_name = str(team.get("name") or "").strip()
    if team_name:
        parts.append(f"所属 Team：{team_name}")
    run_label = str(linked_run_title or linked_run_id or "").strip()
    if run_label:
        parts.append(f"协同 Run：{run_label}")
    team_memory = str(team_runtime_meta(team).get("teamMemory") or "").strip()
    if team_memory:
        parts.append(f"团队记忆：{team_memory}")
    team_decision_log = str(team_runtime_meta(team).get("decisionLog") or "").strip()
    if team_decision_log:
        parts.append(f"团队协作规则：{team_decision_log}")
    if remark:
        parts.append(f"任务说明：{remark}")
    if workflow_name:
        parts.append(f"建议流程：{workflow_name}")
    if branch_label:
        parts.append(f"建议分支：{branch_label}")
    parts.extend(company_auto_operation_prompt_lines(auto_operation_profile, audience="lead", runtime=auto_operation_runtime))
    parts.extend(task_long_term_memory_prompt_lines(task_long_term_memory, audience="lead"))
    parts.extend(task_coordination_prompt_lines(team, audience="lead"))
    if should_task_start_with_internal_discussion(team):
        parts.append("这是一条正式任务：先在 Team 线程里拉产品策略负责人和项目运营负责人做一轮内部讨论，对齐方向、范围和分工，再往下执行。")
        parts.append("不要先问用户要不要内部讨论；正式任务默认先讨论再执行，你直接组织这轮讨论并给出对齐后的执行安排。")
    parts.append("请先开始执行，并把进展同步回任务看板；如果需要拆分清单，请先整理出今天的代办事项。")
    parts.append("请先用一句自然同事语气确认你来牵头，然后说清：当前判断 / 立即下一步 / 需要谁配合或风险。")
    parts.append("你这轮先定一句方向，再点清谁先接哪一段，控制在 2-3 句。")
    parts.append("如果团队里已经有人给过判断或线索，先接住对方的意思再往下推进，不要像重新开题一样另起一套说法。")
    parts.append("如果你判断必须拉其他成员一起处理，请直接在 Team 线程点名并写明你需要他们接什么。")
    parts.extend(human_resolution_contract_lines("lead"))
    return "\n".join(parts)

def task_team_thread_id(task_id):
    slug = re.sub(r"[^a-z0-9]+", "-", str(task_id or "").strip().lower()).strip("-")
    return f"task-team-{slug or secrets.token_hex(4)}"

def task_execution_meta_for_thread(thread):
    if not isinstance(thread, dict):
        return {}
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    payload = meta.get("taskExecution") if isinstance(meta.get("taskExecution"), dict) else {}
    return payload if isinstance(payload, dict) else {}

def is_managed_task_execution_thread(thread, task_id="", team_id=""):
    if not isinstance(thread, dict):
        return False
    normalized_task_id = str(task_id or "").strip()
    normalized_team_id = str(team_id or "").strip()
    task_execution = task_execution_meta_for_thread(thread)
    linked_task_id = str(task_execution.get("taskId") or thread.get("linkedTaskId") or "").strip()
    if not linked_task_id:
        return False
    thread_id = str(thread.get("id") or "").strip()
    managed_team_id = str(
        task_execution.get("teamId")
        or (((thread.get("meta") or {}).get("teamId")) if isinstance(thread.get("meta"), dict) else "")
        or ""
    ).strip()
    if normalized_task_id and linked_task_id != normalized_task_id:
        return False
    if normalized_team_id and managed_team_id not in {"", normalized_team_id}:
        return False
    if thread_id == task_team_thread_id(linked_task_id):
        return True
    return (
        str(task_execution.get("managedBy") or "").strip() == "mission-control"
        and str(task_execution.get("source") or "").strip() == "task_dispatch"
    )

def task_team_participant_agent_ids(team, lead_agent_id=""):
    team = team if isinstance(team, dict) else {}
    participants = []
    for agent_id in [lead_agent_id, str(team.get("leadAgentId") or "").strip(), *safe_list(team.get("memberAgentIds"))]:
        normalized = str(agent_id or "").strip()
        if normalized and normalized not in participants:
            participants.append(normalized)
    return participants

def task_team_member_focus_hint(agent_id):
    normalized = str(agent_id or "").strip().lower()
    if not normalized:
        return ""
    focus_hints = (
        (("devops", "ops", "release", "deploy"), "环境、部署、CI/CD、运行稳定性"),
        (("qa", "quality", "test"), "测试计划、回归验证、验收风险"),
        (("data", "bi", "analytics"), "数据模型、指标口径、数据依赖"),
        (("engineering", "dev", "build", "backend", "frontend"), "架构设计、开发实现、交付落地"),
    )
    for keywords, hint in focus_hints:
        if any(token in normalized for token in keywords):
            return hint
    return ""


DEPLOYMENT_TASK_TOKENS = (
    "deploy",
    "deployment",
    "上线",
    "灰度",
    "回滚",
    "rollback",
    "部署",
)
ACCEPTANCE_TASK_TOKENS = (
    "acceptance",
    "verify",
    "verification",
    "validation",
    "qa",
    "smoke",
    "checklist",
    "验收",
    "验证",
    "回归",
    "测试",
)
INSPECTION_TASK_TOKENS = (
    "inspection",
    "health check",
    "巡检",
    "巡查",
    "巡检任务",
    "健康检查",
)
ACCEPTANCE_OBJECT_FIELD_PATTERN = re.compile(
    r"(?:验收对象|验收入口|验收链接|对象|链接|地址|url|域名|环境|实例|服务|应用|站点|项目|workspace|tenant|build|release|deployment|service|app|env)\s*[:：]\s*\S+",
    re.IGNORECASE,
)
URL_PATTERN = re.compile(r"https?://\S+", re.IGNORECASE)
DOMAIN_PATTERN = re.compile(r"\b[a-z0-9][a-z0-9.-]+\.[a-z]{2,}\b", re.IGNORECASE)
ENVIRONMENT_HINTS_EN = ("prod", "production", "staging", "stage", "uat", "dev", "test")
ENVIRONMENT_HINTS_CN = ("生产", "线上", "预发", "灰度", "测试", "开发", "沙箱")
ACCEPTANCE_OBJECT_HINTS_EN = ("service", "app", "site", "workspace", "tenant", "project", "build", "release")
ACCEPTANCE_OBJECT_HINTS_CN = ("服务", "应用", "站点", "小程序", "实例", "租户", "项目", "环境", "接口")


def task_dispatch_semantic_text(title="", remark="", workflow_binding=None, intelligence=None):
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    selected_branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
    fragments = [
        str(title or ""),
        str(remark or ""),
        str(workflow_binding.get("workflowName") or ""),
        str(selected_branch.get("label") or ""),
        str(selected_branch.get("targetLaneTitle") or ""),
        str(selected_branch.get("targetNodeTitle") or ""),
        str(intelligence.get("category") or ""),
        str(intelligence.get("categoryLabel") or ""),
        " ".join(str(item or "") for item in safe_list(intelligence.get("matchedKeywords"))),
        " ".join(str(item or "") for item in safe_list(intelligence.get("laneHints"))),
    ]
    return " ".join(fragment for fragment in fragments if str(fragment or "").strip()).strip()


def task_is_deployment_acceptance(title="", remark="", workflow_binding=None, intelligence=None):
    combined = task_dispatch_semantic_text(title, remark, workflow_binding=workflow_binding, intelligence=intelligence).lower()
    has_deploy = any(token in combined for token in DEPLOYMENT_TASK_TOKENS)
    has_acceptance = any(token in combined for token in ACCEPTANCE_TASK_TOKENS)
    return has_deploy and has_acceptance


def task_is_inspection(title="", remark="", workflow_binding=None, intelligence=None):
    combined = task_dispatch_semantic_text(title, remark, workflow_binding=workflow_binding, intelligence=intelligence).lower()
    return any(token in combined for token in INSPECTION_TASK_TOKENS)


def task_has_acceptance_object_info(title="", remark=""):
    raw = " ".join(part for part in (str(title or "").strip(), str(remark or "").strip()) if part).strip()
    if not raw:
        return False
    if URL_PATTERN.search(raw) or ACCEPTANCE_OBJECT_FIELD_PATTERN.search(raw) or DOMAIN_PATTERN.search(raw):
        return True
    lowered = raw.lower()
    has_environment = any(token in lowered for token in ENVIRONMENT_HINTS_EN) or any(token in raw for token in ENVIRONMENT_HINTS_CN)
    has_object_hint = any(token in lowered for token in ACCEPTANCE_OBJECT_HINTS_EN) or any(token in raw for token in ACCEPTANCE_OBJECT_HINTS_CN)
    return has_environment and has_object_hint


def has_blocking_task_dispatch_validation(validation):
    return bool((validation or {}).get("blocking"))


def build_task_dispatch_validation(
    title="",
    remark="",
    workflow_binding=None,
    intelligence=None,
    team=None,
    requested_lead_agent_id="",
    valid_agent_ids=None,
):
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    team = team if isinstance(team, dict) else {}
    requested_lead_agent_id = str(requested_lead_agent_id or "").strip()
    valid_agent_ids = {
        str(item or "").strip()
        for item in (valid_agent_ids or [])
        if str(item or "").strip()
    }
    participant_ids = task_team_participant_agent_ids(team, lead_agent_id=str(team.get("leadAgentId") or "").strip())
    deployment_acceptance = task_is_deployment_acceptance(title, remark, workflow_binding=workflow_binding, intelligence=intelligence)
    inspection = task_is_inspection(title, remark, workflow_binding=workflow_binding, intelligence=intelligence)
    acceptance_object_present = task_has_acceptance_object_info(title, remark)
    issues = []

    if deployment_acceptance and not acceptance_object_present:
        issues.append(
            {
                "code": "missing_acceptance_object",
                "field": "remark",
                "message": "部署验收任务缺少验收对象信息，请补充验收对象、环境，以及入口 URL 或实例。",
            }
        )

    if inspection:
        if not requested_lead_agent_id:
            issues.append(
                {
                    "code": "inspection_missing_lead",
                    "field": "leadAgentId",
                    "message": "巡检任务必须明确牵头负责人，缺少 leadAgentId，暂不进入自动派发。",
                }
            )
        elif valid_agent_ids and requested_lead_agent_id not in valid_agent_ids and requested_lead_agent_id not in participant_ids:
            issues.append(
                {
                    "code": "inspection_invalid_lead",
                    "field": "leadAgentId",
                    "message": f"巡检任务指定的牵头人 {requested_lead_agent_id} 不存在，暂不进入自动派发。",
                }
            )
        elif participant_ids and requested_lead_agent_id not in participant_ids:
            issues.append(
                {
                    "code": "inspection_lead_outside_team",
                    "field": "leadAgentId",
                    "message": f"巡检任务指定的牵头人 {requested_lead_agent_id} 不在当前 Team 成员中，暂不进入自动派发。",
                }
            )

    summary = ""
    if issues:
        summary = "；".join(str(item.get("message") or "").strip() for item in issues if str(item.get("message") or "").strip())
    return {
        "blocking": bool(issues),
        "summary": summary,
        "issues": issues,
        "taskKinds": {
            "deploymentAcceptance": deployment_acceptance,
            "inspection": inspection,
        },
        "acceptanceObjectPresent": acceptance_object_present,
        "requestedLeadAgentId": requested_lead_agent_id,
        "teamParticipantAgentIds": participant_ids,
    }

def human_resolution_contract_lines(audience="member"):
    normalized = str(audience or "").strip().lower()
    if normalized == "lead":
        return [
            "默认先把判断和分工收口，不要把原问题原样抛回给提问人。",
            "如果有多个可行方案，先给出当前最稳的一种，并用一句话说清为什么现在先这样做。",
            "不要把背景材料整段堆给对方，优先输出你的判断、取舍和依据。",
            "长期记忆和前情默认内化使用，不要把它们当成一块背景说明重新念给对方听。",
            "如果对方问这里的记忆机制，要按产品记忆系统来解释，不要说这里只靠 MEMORY.md 或 memory/*.md。",
            "不要把“全员协作会慢、你去单聊我更快”当成答复；在 Team 线程里先直接给当前判断，其他同事的补充由后台继续跟上。",
            "除非对方明确在问性能原因，否则不要主动解释内部 fanout、队列、token 或调度细节。",
            "如果遇到浏览器、Canvas、沙箱或运行环境问题，先判断有没有现成 fallback、替代路径或最小可行解，再给结论；不要把原始错误直接甩给用户。",
            "如果还有不确定，就说清你已经排查了什么、当前最稳的判断，以及你准备怎么继续解。",
            "收尾时显式写两行：结论：……  下一步：……，让大家一眼知道现在先按什么执行。",
        ]
    return [
        "默认先接住问题并往前推进，不要把原问题原样丢回同事或用户。",
        "如果你看到不止一种做法，先推荐当前最稳的一种，并用一句话解释为什么。",
        "不要只罗列信息，优先给判断、取舍和建议动作。",
        "长期记忆和前情默认内化使用，不要在可见回复里一条条复述背景。",
        "如果对方问这里的记忆机制，要按产品记忆系统来解释，不要说这里只靠 MEMORY.md 或 memory/*.md。",
        "不要把“全员协作会慢、你去单聊我更快”当成答复；在 Team 线程里先直接给当前判断，其他同事的补充由后台继续跟上。",
        "除非对方明确在问性能原因，否则不要主动解释内部 fanout、队列、token 或调度细节。",
        "如果遇到浏览器、Canvas、沙箱或运行环境问题，先尝试当前链路能走的 fallback 或替代验证方式，不要直接把原始错误丢给对方。",
        "如果你还没完全解决，就说清你已经试过什么、现在卡在哪里、接下来你准备怎么解。",
        "收尾时显式写两行：结论：……  下一步：……，让对方知道现在先怎么做。",
    ]

def task_coordination_prompt_lines(team, audience="lead"):
    protocol = team_collaboration_protocol(team)
    checklist_key = "memberChecklist" if str(audience or "").strip().lower() == "member" else "leadChecklist"
    checklist = clean_unique_strings(protocol.get(checklist_key) or [])
    proactive_rules = clean_unique_strings(protocol.get("proactiveRules") or [])
    lines = []
    human_tone = str(protocol.get("humanToneGuide") or "").strip()
    if human_tone:
        lines.append(f"沟通语气：{human_tone}")
    if checklist:
        lines.append("协作约定：")
        lines.extend(f"- {item}" for item in checklist)
    if proactive_rules:
        lines.append("主动沟通：")
        lines.extend(f"- {item}" for item in proactive_rules[:3])
    update_contract = str(protocol.get("updateContract") or "").strip()
    if update_contract:
        lines.append(f"进展回报：{update_contract}")
    escalation_rule = str(protocol.get("escalationRule") or "").strip()
    if escalation_rule:
        lines.append(f"卡点升级：{escalation_rule}")
    return lines

def task_coordination_protocol_snapshot(team):
    protocol = team_collaboration_protocol(team)
    return {
        "profile": str(protocol.get("profile") or "").strip(),
        "humanToneGuide": str(protocol.get("humanToneGuide") or "").strip(),
        "proactiveRules": clean_unique_strings(protocol.get("proactiveRules") or []),
        "updateContract": str(protocol.get("updateContract") or "").strip(),
        "escalationRule": str(protocol.get("escalationRule") or "").strip(),
        "leadChecklist": clean_unique_strings(protocol.get("leadChecklist") or []),
        "memberChecklist": clean_unique_strings(protocol.get("memberChecklist") or []),
    }

def build_task_team_member_message(
    task_id,
    title,
    agent_id,
    lead_agent_id="",
    remark="",
    workflow_binding=None,
    team=None,
    linked_run_id="",
    linked_run_title="",
    lead_display_name="",
    lead_reply_preview="",
    discussion_summary="",
    auto_operation_profile=None,
    auto_operation_runtime=None,
    task_long_term_memory=None,
):
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    team = team if isinstance(team, dict) else {}
    workflow_name = str(workflow_binding.get("workflowName") or "").strip()
    branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
    branch_label = str(branch.get("label") or "").strip()
    team_name = str(team.get("name") or "").strip()
    focus_hint = task_team_member_focus_hint(agent_id)
    parts = [
        f"请加入任务 {task_id} 的 Team 协作。",
        f"任务标题：{title or task_id}",
    ]
    if team_name:
        parts.append(f"所属 Team：{team_name}")
    run_label = str(linked_run_title or linked_run_id or "").strip()
    if run_label:
        parts.append(f"协同 Run：{run_label}")
    if lead_agent_id:
        parts.append(f"Team lead：{lead_agent_id}")
    if lead_display_name:
        parts.append(f"牵头同事：{lead_display_name}")
    if focus_hint:
        parts.append(f"建议关注：{focus_hint}")
    team_memory = str(team_runtime_meta(team).get("teamMemory") or "").strip()
    if team_memory:
        parts.append(f"团队记忆：{team_memory}")
    if remark:
        parts.append(f"任务说明：{remark}")
    if workflow_name:
        parts.append(f"建议流程：{workflow_name}")
    if branch_label:
        parts.append(f"建议分支：{branch_label}")
    if lead_reply_preview:
        parts.append(f"牵头同事刚刚的判断：{summarize_task_execution_text(lead_reply_preview, limit=280)}")
    if discussion_summary:
        parts.append(f"内部讨论先收口到：{summarize_task_execution_text(discussion_summary, limit=320)}")
    parts.extend(company_auto_operation_prompt_lines(auto_operation_profile, audience="member", runtime=auto_operation_runtime))
    parts.extend(task_long_term_memory_prompt_lines(task_long_term_memory, audience="member"))
    parts.extend(task_coordination_prompt_lines(team, audience="member"))
    parts.append("请先回应你理解到的 lead 判断或当前分工，再说你准备怎么接住这一段，不要各说各的。")
    parts.append("如果上一位已经说出了和你相同的判断，不要整段复述；先用一句认同或补充，再只说你新增的信息。")
    parts.append("如果你看到了多个可行做法，不要把选择题丢回来；先推荐你认为最稳的做法，并说一句为什么。")
    parts.append("如果遇到浏览器、Canvas、沙箱或运行环境问题，先判断当前链路有没有 fallback、替代验证方法或最小可行解，不要直接把原始报错甩给用户。")
    parts.append("请判断你是否需要介入该任务。若需要，请直接认领 1 个你负责的子项并开始推进。")
    parts.append("请用像真实同事协作的口吻回复，至少说清：负责范围 / 立即下一步 / 需要谁配合或风险。")
    parts.append("回复最后请显式写两行：结论：……  下一步：……。")
    parts.append("如果你是在接别人刚提到的依赖，请直接点名对方，并明确说“这块我接住，我先来处理什么”。")
    parts.append("如果当前无需介入，请明确回复 STANDBY，并说明什么条件下需要你接手。")
    parts.append("不是每个人都要长回复；如果你这一轮没有新增信息，就用一句简短待命，不要重复他人的整段内容。")
    parts.append("如果你被别人卡住，不要等待，请直接在 Team 线程说明卡点、影响和你需要谁来补位。")
    parts.append("注意：主任务看板由 Team lead 统一同步，请不要改主任务状态。")
    return "\n".join(parts)

def build_task_team_fanout_message(
    task_id,
    title,
    remark="",
    workflow_binding=None,
    team=None,
    lead_agent_id="",
    member_agent_ids=None,
    linked_run_id="",
    linked_run_title="",
    lead_display_name="",
    discussion_summary="",
    auto_operation_profile=None,
    auto_operation_runtime=None,
    task_long_term_memory=None,
):
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    team = team if isinstance(team, dict) else {}
    workflow_name = str(workflow_binding.get("workflowName") or "").strip()
    team_name = str(team.get("name") or "").strip()
    member_labels = [str(item or "").strip() for item in safe_list(member_agent_ids) if str(item or "").strip()]
    parts = [
        f"团队任务启动：{task_id}",
        f"任务标题：{title or task_id}",
    ]
    if team_name:
        parts.append(f"Team：{team_name}")
    run_label = str(linked_run_title or linked_run_id or "").strip()
    if run_label:
        parts.append(f"Run：{run_label}")
    if lead_agent_id:
        parts.append(f"Lead：{lead_agent_id}")
    if lead_display_name:
        parts.append(f"牵头同事：{lead_display_name}")
    if member_labels:
        parts.append("已通知成员：" + "、".join(member_labels))
    if remark:
        parts.append(f"任务说明：{remark}")
    if workflow_name:
        parts.append(f"建议流程：{workflow_name}")
    if discussion_summary:
        parts.append(f"内部讨论已先收口：{summarize_task_execution_text(discussion_summary, limit=320)}")
    parts.extend(company_auto_operation_prompt_lines(auto_operation_profile, audience="member", runtime=auto_operation_runtime))
    parts.extend(task_long_term_memory_prompt_lines(task_long_term_memory, audience="member"))
    protocol_lines = task_coordination_prompt_lines(team, audience="member")
    if protocol_lines:
        parts.extend(protocol_lines)
    parts.append("请成员先承接牵头同事的当前判断，再用自然同事语气回复负责范围 / 立即下一步 / 需要谁配合或风险；若暂不介入，请回复 STANDBY。")
    parts.append("如果前一位已经表达了和你一致的判断，就不要重复整段复述，直接补你新增的判断或你接住的那一段。")
    parts.append("如果有多个可行方案，优先给出当前最稳的一种，并用一句话解释为什么。")
    parts.append("如果遇到浏览器、Canvas、沙箱或运行环境问题，先尝试现成 fallback 或替代验证路径，再给结论，不要直接把原始报错转述给用户。")
    parts.append("每位成员收尾时都请显式写两行：结论：……  下一步：……。")
    parts.append("谁发现依赖或卡点，谁先在这个 Team 线程里发起沟通，不等别人追问。")
    return "\n".join(parts)

def build_task_internal_discussion_message(
    task_id,
    title,
    remark="",
    team=None,
    lead_agent_id="",
    discussion_agent_ids=None,
    linked_run_id="",
    linked_run_title="",
    lead_display_name="",
    auto_operation_profile=None,
    auto_operation_runtime=None,
    task_long_term_memory=None,
):
    team = team if isinstance(team, dict) else {}
    team_name = str(team.get("name") or "").strip()
    run_label = str(linked_run_title or linked_run_id or "").strip()
    participant_labels = [str(item or "").strip() for item in safe_list(discussion_agent_ids) if str(item or "").strip()]
    parts = [
        f"正式任务先做内部讨论：{task_id}",
        f"任务标题：{title or task_id}",
    ]
    if team_name:
        parts.append(f"当前小组：{team_name}")
    if run_label:
        parts.append(f"协同 Run：{run_label}")
    if lead_agent_id:
        parts.append(f"牵头人：{lead_agent_id}")
    if lead_display_name:
        parts.append(f"牵头同事：{lead_display_name}")
    if participant_labels:
        parts.append("本轮先参与内部讨论：" + "、".join(participant_labels))
    if remark:
        parts.append(f"任务说明：{remark}")
    parts.extend(company_auto_operation_prompt_lines(auto_operation_profile, audience="lead", runtime=auto_operation_runtime))
    parts.extend(task_long_term_memory_prompt_lines(task_long_term_memory, audience="lead"))
    protocol_lines = task_coordination_prompt_lines(team, audience="member")
    if protocol_lines:
        parts.extend(protocol_lines)
    parts.append("这一轮先不要直接开工出活，先把方向、范围、优先级和分工在内部对齐。")
    parts.append("请先回应牵头同事的初步判断，再补你建议先做什么、谁来负责哪一段。")
    parts.append("不要再问“要不要我先拉人讨论”；正式任务默认先内部讨论，再执行。")
    parts.append("每位参与者都请收尾写两行：结论：……  下一步：……。")
    return "\n".join(parts)

def conversation_reply_preview(result):
    payloads = ((result.get("result", {}) or {}).get("payloads", []) if isinstance(result, dict) else []) or []
    if payloads and isinstance(payloads[0], dict):
        return str(payloads[0].get("text") or "").strip()
    return ""

def coordination_reply_entries(responses, limit=4):
    entries = []
    seen = set()
    seen_signatures = []
    for item in safe_list(responses):
        if not isinstance(item, dict):
            continue
        agent_id = str(item.get("agentId") or "").strip()
        reply_preview = summarize_task_execution_text(item.get("replyPreview"), limit=180)
        if not agent_id or not reply_preview or agent_id in seen:
            continue
        signature = coordination_reply_signature(reply_preview)
        if signature and any(
            signature == existing
            or signature.startswith(existing[:48])
            or existing.startswith(signature[:48])
            for existing in seen_signatures
        ):
            continue
        seen.add(agent_id)
        if signature:
            seen_signatures.append(signature)
        agent_display_name = str(
            item.get("agentDisplayName")
            or item.get("agentHumanName")
            or item.get("agentLabel")
            or agent_id
        ).strip() or agent_id
        entries.append(
            {
                "agentId": agent_id,
                "agentDisplayName": agent_display_name,
                "replyPreview": reply_preview,
                "messageId": str(item.get("messageId") or "").strip(),
            }
        )
        if len(entries) >= max(1, int(limit or 4)):
            break
    return entries

def summarize_internal_discussion_context(responses, limit=4):
    response_entries = coordination_reply_entries(responses, limit=max(1, int(limit or 4)))
    if not response_entries:
        return ""
    return "；".join(
        f"{item['agentDisplayName']}：{item['replyPreview']}"
        for item in response_entries
        if item.get("replyPreview")
    )

def normalize_team_collaboration_reply(text):
    return re.sub(r"\s+", " ", str(text or "").strip())

def coordination_reply_signature(text):
    normalized = normalize_team_collaboration_reply(text).lower()
    normalized = re.sub(r"[，。、“”‘’；：！？,.!?:;·\\-_/]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized[:180]

def apply_conversation_fanout_stagger(requests, stagger_seconds=0.0):
    stagger_value = max(0.0, float(stagger_seconds or 0.0))
    staggered_requests = []
    for index, item in enumerate(safe_list(requests)):
        if not isinstance(item, dict):
            continue
        staged_item = {**item}
        try:
            base_delay = float(staged_item.get("delaySeconds") or 0.0)
        except (TypeError, ValueError):
            base_delay = 0.0
        staged_item["delaySeconds"] = round(max(0.0, base_delay) + (stagger_value * index), 3)
        staggered_requests.append(staged_item)
    return staggered_requests

def apply_turn_guidance_to_message(message, turn_index=0, participant_count=0, anchor=None, turn_profile=None):
    base_message = str(message or "").strip()
    total = max(0, int(participant_count or 0))
    index = max(0, int(turn_index or 0))
    if not base_message:
        return base_message
    anchor = anchor if isinstance(anchor, dict) else {}
    turn_profile = turn_profile if isinstance(turn_profile, dict) else {}
    anchor_label = str(anchor.get("acknowledgedAgentLabel") or "").strip()
    anchor_preview = summarize_task_execution_text(str(anchor.get("acknowledgedPreview") or "").strip(), limit=120)
    guidance_lines = []
    if anchor_label:
        guidance_lines.append(f"这轮先接一下 {anchor_label} 刚刚提到的判断，再补你的新增动作。")
        if anchor_preview:
            guidance_lines.append(f"{anchor_label} 刚刚提到：{anchor_preview}")
        if index >= 1:
            guidance_lines.append("如果你和 TA 的判断基本一致，就不要重复铺背景，直接说你新增的动作、风险或接手点。")
    elif index == 1:
        guidance_lines.append("前面已经有人开了头，这一轮先接一句你认同哪点，再补你的新增判断。")
    elif index >= 2:
        guidance_lines.append("前面已经有同事把背景铺开了，这一轮尽量只补新增信息、依赖或风险。")
        guidance_lines.append("如果没有新增判断，就简短说明你接住哪一段或先待命，不要再把背景从头复述。")
    guidance_lines.extend(clean_unique_strings(turn_profile.get("guidanceLines") or []))
    guidance_lines.extend(human_resolution_contract_lines("member"))
    if not guidance_lines:
        return base_message
    return f"{base_message}\n" + "\n".join(guidance_lines)

def classify_team_collaboration_reply(text):
    normalized = normalize_team_collaboration_reply(text)
    lowered = normalized.lower()
    if not normalized:
        return "silent"
    if normalized.startswith(("团队协作同步：", "请接手任务", "请加入任务", "团队任务启动：", "团队内同步更新", "我把 ", "请成员先承接")):
        return "committed"
    if re.search(r"\bstandby\b", lowered) or any(token in normalized for token in ("待命", "暂不介入", "先不介入", "候命")):
        return "standby"
    blocker_patterns = (
        r"\bblocked by\b",
        r"\bi am blocked\b",
        r"(我|目前|现在).{0,4}(被卡住|卡住|阻塞|无法推进|推进不了)",
        r"(当前|目前)卡点[:：]",
        r"(当前|目前)卡点为",
        r"(当前|目前)卡点在于",
        r"阻塞原因[:：]",
        r"缺少.+(导致|所以).+(无法推进|推进不了|阻塞)",
    )
    if any(re.search(pattern, normalized, re.IGNORECASE) for pattern in blocker_patterns):
        return "blocked"
    return "committed"

def build_team_collaboration_summary(dispatch_state):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    participant_ids = [
        str(item or "").strip()
        for item in safe_list(dispatch_state.get("participantAgentIds"))
        if str(item or "").strip()
    ]
    dispatch_agent_ids = [
        str(item or "").strip()
        for item in safe_list(
            dispatch_state.get("targetedAgentIds")
            or dispatch_state.get("dispatchAgentIds")
        )
        if str(item or "").strip()
    ]
    responded_fallback_ids = [
        str(item or "").strip()
        for item in safe_list(dispatch_state.get("respondedAgentIds"))
        if str(item or "").strip()
    ]
    failure_agent_ids = [
        str(item.get("agentId") or "").strip()
        for item in safe_list(dispatch_state.get("failedAgents"))
        if isinstance(item, dict) and str(item.get("agentId") or "").strip()
    ]
    tracked_agent_ids = clean_unique_strings(
        dispatch_agent_ids or participant_ids or responded_fallback_ids or failure_agent_ids
    )
    response_map = {}
    for item in safe_list(dispatch_state.get("responses")):
        if not isinstance(item, dict):
            continue
        agent_id = str(item.get("agentId") or "").strip()
        if not agent_id or agent_id in response_map:
            continue
        reply_preview = normalize_team_collaboration_reply(item.get("replyPreview") or item.get("text") or "")
        response_map[agent_id] = {
            "agentId": agent_id,
            "replyPreview": reply_preview,
            "kind": classify_team_collaboration_reply(reply_preview),
        }
    for agent_id in responded_fallback_ids:
        if agent_id and agent_id not in response_map:
            response_map[agent_id] = {
                "agentId": agent_id,
                "replyPreview": "",
                "kind": "committed",
            }
    responded_agent_ids = list(response_map.keys())
    waiting_agent_ids = [
        agent_id
        for agent_id in tracked_agent_ids
        if agent_id not in response_map and agent_id not in failure_agent_ids
    ]
    committed_agent_ids = [agent_id for agent_id, item in response_map.items() if item.get("kind") == "committed"]
    standby_agent_ids = [agent_id for agent_id, item in response_map.items() if item.get("kind") == "standby"]
    blocker_agent_ids = [agent_id for agent_id, item in response_map.items() if item.get("kind") == "blocked"]
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_sent = bool(relay.get("sent"))
    relay_target_ids = dispatch_state_relay_target_ids(dispatch_state)
    relay_reply_count = len(
        [
            item
            for item in safe_list(relay.get("responses"))
            if isinstance(item, dict) and str(item.get("agentId") or "").strip()
        ]
    )
    if failure_agent_ids or blocker_agent_ids:
        status = "blocked"
    elif not tracked_agent_ids or not responded_agent_ids:
        status = "pending"
    elif waiting_agent_ids or (relay_sent and relay_reply_count < len(relay_target_ids)):
        status = "watch"
    else:
        status = "healthy"
    headline_parts = []
    if committed_agent_ids:
        headline_parts.append(f"{len(committed_agent_ids)} 人已经接住")
    if standby_agent_ids:
        headline_parts.append(f"{len(standby_agent_ids)} 人先待命")
    if waiting_agent_ids:
        headline_parts.append(f"{len(waiting_agent_ids)} 人正在整理回复")
    if blocker_agent_ids:
        headline_parts.append(f"{len(blocker_agent_ids)} 人需要支援")
    if not headline_parts:
        headline_parts.append("团队正在接话中")
    return {
        "status": status,
        "headline": "，".join(headline_parts),
        "memberCount": len(tracked_agent_ids),
        "responseCount": len(responded_agent_ids),
        "waitingCount": len(waiting_agent_ids),
        "committedCount": len(committed_agent_ids),
        "standbyCount": len(standby_agent_ids),
        "blockerCount": len(blocker_agent_ids),
        "failureCount": len(failure_agent_ids),
        "relaySent": relay_sent,
        "relayReplyCount": relay_reply_count,
        "respondedAgentIds": responded_agent_ids,
        "waitingAgentIds": waiting_agent_ids,
        "committedAgentIds": committed_agent_ids,
        "standbyAgentIds": standby_agent_ids,
        "blockerAgentIds": blocker_agent_ids,
        "failedAgentIds": failure_agent_ids,
        "syncType": str(dispatch_state.get("syncType") or "dispatch").strip() or "dispatch",
        "updatedAt": str(dispatch_state.get("at") or "").strip(),
    }

def task_execution_session_id(task_id):
    return f"task-{str(task_id or '').strip().lower()}"


def task_execution_bootstrap_for_task(task):
    route_meta = {}
    if isinstance(task, dict):
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
        route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
        if not route_meta and isinstance(task.get("routeDecision"), dict):
            route_meta = task.get("routeDecision")
    execution = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
    return execution if isinstance(execution, dict) else {}


def task_execution_sync_for_task(task):
    route_meta = {}
    if isinstance(task, dict):
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
        route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
        if not route_meta and isinstance(task.get("routeDecision"), dict):
            route_meta = task.get("routeDecision")
    sync = route_meta.get("executionSync") if isinstance(route_meta.get("executionSync"), dict) else {}
    return sync if isinstance(sync, dict) else {}


TASK_EXECUTION_SENTINEL_TEXTS = {"NO_REPLY", "HEARTBEAT_OK"}


def normalize_task_progress_text(text):
    return re.sub(r"\s+", " ", str(text or "").strip())


def comparable_task_progress_text(text):
    normalized = normalize_task_progress_text(text)
    return normalized[:-1].rstrip() if normalized.endswith("…") else normalized


def task_progress_text_matches(left, right):
    left_text = comparable_task_progress_text(left)
    right_text = comparable_task_progress_text(right)
    if not left_text or not right_text:
        return False
    return left_text == right_text or left_text.startswith(right_text) or right_text.startswith(left_text)


def is_task_execution_placeholder_text(text):
    normalized = normalize_task_progress_text(text)
    if not normalized:
        return True
    return normalized.upper() in TASK_EXECUTION_SENTINEL_TEXTS


def task_has_meaningful_progress(task, bootstrap_at=None):
    threshold = parse_iso(bootstrap_at) if bootstrap_at else None
    for entry in safe_list(task.get("progress_log")):
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or "").strip()
        if is_task_execution_placeholder_text(text):
            continue
        if threshold:
            entry_at = parse_iso(entry.get("at"))
            if entry_at and entry_at < threshold:
                continue
        lowered = text.lower()
        if lowered.startswith("系统已将任务派发给"):
            continue
        if lowered.startswith("已向目标 agent 发出执行指令"):
            continue
        if lowered.startswith("已先在 team ") and "内部讨论" in lowered:
            continue
        if lowered.startswith("已向 team ") and "发出" in lowered:
            continue
        return True
    return False


def task_has_progress_after(task, threshold, ignore_texts=None):
    threshold_dt = parse_iso(threshold) if isinstance(threshold, str) else threshold
    if not threshold_dt:
        return False
    ignored = [item for item in safe_list(ignore_texts) if comparable_task_progress_text(item)]
    for entry in safe_list(task.get("progress_log")):
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("text") or "").strip()
        if is_task_execution_placeholder_text(text):
            continue
        if any(task_progress_text_matches(text, ignored_text) for ignored_text in ignored):
            continue
        lowered = text.lower()
        if lowered.startswith("系统已将任务派发给"):
            continue
        if lowered.startswith("已向目标 agent 发出执行指令"):
            continue
        if lowered.startswith("已先在 team ") and "内部讨论" in lowered:
            continue
        if lowered.startswith("已向 team ") and "发出" in lowered:
            continue
        entry_at = parse_iso(entry.get("at"))
        if entry_at and entry_at >= threshold_dt:
            return True
    return False


def task_execution_transcript_health(openclaw_dir, agent_id, session_id):
    path = session_transcript_path(openclaw_dir, agent_id, session_id)
    transcript = parse_transcript_items(path, limit=40)
    stats = transcript.get("stats", {}) if isinstance(transcript, dict) else {}
    return {
        "path": str(path) if path else "",
        "exists": bool(path),
        "userMessages": int(stats.get("userMessages") or 0),
        "assistantMessages": int(stats.get("assistantMessages") or 0),
        "turns": int(stats.get("turns") or 0),
    }


def task_execution_workspace_for_agent(openclaw_dir, config, agent_id):
    normalized = str(agent_id or "").strip()
    for agent in load_agents(config):
        if not isinstance(agent, dict) or str(agent.get("id") or "").strip() != normalized:
            continue
        workspace = Path(agent.get("workspace", "")) if agent.get("workspace") else Path(openclaw_dir) / f"workspace-{normalized}"
        return workspace.expanduser().resolve()
    if normalized:
        return (Path(openclaw_dir) / f"workspace-{normalized}").expanduser().resolve()
    return Path(openclaw_dir).expanduser().resolve()

def summarize_task_execution_tool_result(text):
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    if "[kanban]" not in lowered:
        return ""
    if " todo " in lowered or "todo [" in lowered:
        return "已更新任务待办清单。"
    if " progress:" in lowered or "📡" in normalized:
        match = re.search(r"progress:\s*(.+)", normalized, re.IGNORECASE)
        if match and match.group(1).strip():
            return summarize_task_execution_text(match.group(1).strip())
        return "已同步最新执行进展。"
    if " done" in lowered or "已完成" in normalized:
        return "已将任务标记为完成。"
    if " block" in lowered or "阻塞" in normalized:
        return "已将任务标记为阻塞。"
    if " state" in lowered:
        return "已更新任务状态。"
    return ""


def completion_summary_from_text(text):
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    patterns = [
        r"^\s*✅\s*Task completed:\s*(.+)$",
        r"^\s*(?:任务已完成|任务完成|交付完成)[:：]?\s*(.+)$",
        r"^\s*completed[:：]?\s*(.+)$",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if match and match.group(1).strip():
            return summarize_task_execution_text(match.group(1).strip())
    if "## ✅ 任务进展汇报" in normalized:
        status_match = re.search(r"\*\*状态\*\*[:：]\s*(.+)", normalized)
        if status_match:
            status_text = status_match.group(1).strip().lower()
            if any(token in status_text for token in ("已完成", "done", "completed")):
                title_match = re.search(r"\*\*任务\*\*[:：]\s*(.+)", normalized)
                if title_match and title_match.group(1).strip():
                    return summarize_task_execution_text(title_match.group(1).strip())
                return "任务已完成。"
    return ""


def extract_completion_output_path(config, openclaw_dir, agent_id, candidate_text):
    text = str(candidate_text or "").strip()
    if not text:
        return ""
    path_patterns = [
        r'"path"\s*:\s*"([^"]+)"',
        r"Successfully wrote \d+ bytes to ([^\s]+)",
        r"`(/[^`]+)`",
        r"`([^`\n]+\.(?:md|txt|html|json|csv|xlsx|pdf|docx))`",
    ]
    workspace = task_execution_workspace_for_agent(openclaw_dir, config, agent_id)
    for pattern in path_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            raw_path = str(match.group(1) or "").strip()
            if not raw_path:
                continue
            candidate = Path(raw_path).expanduser()
            if not candidate.is_absolute():
                candidate = (workspace / raw_path).resolve()
            else:
                candidate = candidate.resolve()
            if candidate.exists():
                return str(candidate)
    return ""


def task_execution_transcript_paths(openclaw_dir, agent_id, session_id="", task_id="", max_fallback=6):
    agent_path = Path(openclaw_dir) / "agents" / str(agent_id or "").strip() / "sessions"
    if not agent_path.exists():
        return []
    paths = []
    seen = set()

    def add(path):
        resolved = Path(path).resolve()
        key = str(resolved)
        if key in seen or not resolved.exists():
            return
        seen.add(key)
        paths.append(resolved)

    primary = session_transcript_path(openclaw_dir, agent_id, session_id) if session_id else None
    if primary:
        add(primary)
    normalized_task_id = str(task_id or "").strip().lower()
    if normalized_task_id:
        direct_task_path = agent_path / f"{task_execution_session_id(task_id)}.jsonl"
        if direct_task_path.exists():
            add(direct_task_path)
    candidates = sorted(agent_path.glob("*.jsonl"), key=lambda item: item.stat().st_mtime, reverse=True)
    for path in candidates:
        if len(paths) >= max_fallback:
            break
        if primary and path.resolve() == Path(primary).resolve():
            continue
        if normalized_task_id:
            try:
                if normalized_task_id not in path.read_text(encoding="utf-8", errors="ignore").lower():
                    continue
            except OSError:
                continue
        add(path)
    return paths


def transcript_window_matches_task(items, index, task):
    task = task if isinstance(task, dict) else {}
    markers = []
    task_id = str(task.get("id") or "").strip().lower()
    if task_id:
        markers.append(task_id)
    title = str(task.get("title") or "").strip().lower()
    if title and len(title) >= 4:
        markers.append(title)
    if not markers:
        return True
    current = items[index] if 0 <= index < len(items) and isinstance(items[index], dict) else {}
    current_text = str(current.get("text") or "").strip().lower()
    if current_text and any(marker in current_text for marker in markers):
        return True
    for pointer in range(index - 1, -1, -1):
        previous = items[pointer]
        if not isinstance(previous, dict):
            continue
        kind = str(previous.get("kind") or "").strip().lower()
        if kind not in {"user", "system"}:
            continue
        previous_text = str(previous.get("text") or "").strip().lower()
        if not previous_text:
            continue
        return any(marker in previous_text for marker in markers)
    return False


def latest_task_execution_sync_candidate(openclaw_dir, task):
    bootstrap = task_execution_bootstrap_for_task(task)
    task_id = str(task.get("id") or "").strip()
    agent_id = str(bootstrap.get("agentId") or task.get("targetAgentId") or "").strip()
    session_id = str(bootstrap.get("sessionId") or task_execution_session_id(task_id)).strip()
    if not agent_id:
        return {}
    bootstrap_at = parse_iso(bootstrap.get("firstScheduledAt") or bootstrap.get("at"))
    for path in task_execution_transcript_paths(openclaw_dir, agent_id, session_id=session_id, task_id=task_id):
        resolved_session_id = session_id_for_transcript_path(openclaw_dir, agent_id, path, default_session_id=session_id)
        dedicated_task_session = (
            str(resolved_session_id or "").strip() == task_execution_session_id(task_id)
            or Path(path).stem == task_execution_session_id(task_id)
        )
        transcript = parse_transcript_items(path, limit=160)
        items = transcript.get("items", []) if isinstance(transcript, dict) else []
        for index in range(len(items) - 1, -1, -1):
            item = items[index]
            if not isinstance(item, dict):
                continue
            item_at = parse_iso(item.get("at"))
            if bootstrap_at and item_at and item_at < bootstrap_at:
                continue
            kind = item.get("kind")
            if kind == "assistant":
                text = summarize_task_execution_text(item.get("text", ""))
                if not text or text.startswith("请接手任务"):
                    continue
                if not dedicated_task_session and not transcript_window_matches_task(items, index, task):
                    continue
                return {
                    "agentId": agent_id,
                    "sessionId": resolved_session_id,
                    "messageId": str(item.get("id") or "").strip(),
                    "text": text,
                    "at": item.get("at", ""),
                    "path": str(path) if path else "",
                    "source": "assistant",
                }
            if kind == "tool_result":
                text = summarize_task_execution_tool_result(item.get("text", ""))
                if not text:
                    continue
                if not dedicated_task_session and not transcript_window_matches_task(items, index, task):
                    continue
                return {
                    "agentId": agent_id,
                    "sessionId": resolved_session_id,
                    "messageId": str(item.get("id") or "").strip(),
                    "text": text,
                    "at": item.get("at", ""),
                    "path": str(path) if path else "",
                    "source": "tool_result",
                }
    return {}


def latest_task_execution_completion_candidate(openclaw_dir, config, task):
    bootstrap = task_execution_bootstrap_for_task(task)
    task_id = str(task.get("id") or "").strip()
    agent_id = str(bootstrap.get("agentId") or task.get("targetAgentId") or "").strip()
    session_id = str(bootstrap.get("sessionId") or task_execution_session_id(task_id)).strip()
    if not agent_id:
        return {}
    bootstrap_at = parse_iso(bootstrap.get("firstScheduledAt") or bootstrap.get("at"))
    for path in task_execution_transcript_paths(openclaw_dir, agent_id, session_id=session_id, task_id=task_id):
        resolved_session_id = session_id_for_transcript_path(openclaw_dir, agent_id, path, default_session_id=session_id)
        dedicated_task_session = (
            str(resolved_session_id or "").strip() == task_execution_session_id(task_id)
            or Path(path).stem == task_execution_session_id(task_id)
        )
        transcript = parse_transcript_items(path, limit=200)
        items = transcript.get("items", []) if isinstance(transcript, dict) else []
        for index in range(len(items) - 1, -1, -1):
            item = items[index]
            if not isinstance(item, dict):
                continue
            item_at = parse_iso(item.get("at"))
            if bootstrap_at and item_at and item_at < bootstrap_at:
                continue
            if not dedicated_task_session and not transcript_window_matches_task(items, index, task):
                continue
            text = str(item.get("text") or "").strip()
            summary = ""
            if item.get("kind") == "assistant":
                summary = completion_summary_from_text(text)
            elif item.get("kind") == "tool_result":
                tool_summary = summarize_task_execution_tool_result(text)
                if tool_summary == "已将任务标记为完成。":
                    summary = tool_summary
            if not summary:
                continue
            output_path = extract_completion_output_path(config, openclaw_dir, agent_id, text)
            if not output_path:
                window = items[max(0, index - 4) : min(len(items), index + 3)]
                for neighbor in window:
                    output_path = extract_completion_output_path(config, openclaw_dir, agent_id, neighbor.get("text", ""))
                    if output_path:
                        break
            return {
                "agentId": agent_id,
                "sessionId": resolved_session_id,
                "messageId": str(item.get("id") or "").strip(),
                "text": summary,
                "at": item.get("at", ""),
                "path": str(path) if path else "",
                "outputPath": output_path,
                "source": item.get("kind") or "assistant",
            }
    return {}


def update_task_execution_sync_state(openclaw_dir, task_id, sync_state, router_agent_id=""):
    if not task_id or not isinstance(sync_state, dict):
        return
    config = load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            route_meta["executionSync"] = dict(sync_state)
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            task["updatedAt"] = now_iso()
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])


def task_execution_agent_lock(agent_id):
    normalized = str(agent_id or "").strip() or "_default"
    with TASK_EXECUTION_AGENT_LOCKS_GUARD:
        lock = TASK_EXECUTION_AGENT_LOCKS.get(normalized)
        if lock is None:
            lock = threading.Lock()
            TASK_EXECUTION_AGENT_LOCKS[normalized] = lock
        return lock


def update_task_execution_bootstrap(openclaw_dir, task_id, agent_id, status, note="", session_id="", router_agent_id="", title=""):
    config = load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    agent_labels, _ = build_label_maps(load_agents(config), kanban_cfg, router_agent_id)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)
    timestamp = now_iso()
    agent_label = agent_labels.get(agent_id, agent_id)

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            execution = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
            requested_status = str(status or "").strip().lower()
            execution_anchor = execution.get("firstScheduledAt") or execution.get("at") or timestamp
            sync_state = task_execution_sync_for_task(task)
            sync_at = parse_iso(sync_state.get("at"))
            sync_after_anchor = False
            if sync_state.get("text"):
                anchor_dt = parse_iso(execution_anchor)
                sync_after_anchor = (not anchor_dt) or (sync_at and sync_at >= anchor_dt)
            progress_after_anchor = task_has_meaningful_progress(task, execution_anchor)
            next_status = requested_status
            next_note = str(note or "").strip()
            if requested_status == "failed" and (progress_after_anchor or sync_after_anchor):
                next_status = "dispatched"
                recovery_reason = "已检测到任务后续进展，忽略迟到的失败状态。"
                next_note = f"{recovery_reason} 原失败：{next_note}" if next_note else recovery_reason
            execution.update(
                {
                    "status": next_status,
                    "at": timestamp,
                    "agentId": agent_id,
                    "agentLabel": agent_label,
                    "sessionId": session_id,
                    "note": next_note,
                    "attempts": max(int(execution.get("attempts") or 0), 1),
                }
            )
            route_meta["executionBootstrap"] = execution
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            task["updatedAt"] = timestamp
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])


def reset_task_execution_bootstrap(openclaw_dir, task_id, router_agent_id=""):
    config = load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)
    changed = {"value": False}

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            execution = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
            if not execution:
                continue
            execution["attempts"] = 0
            execution["status"] = "scheduled"
            execution["at"] = now_iso()
            execution["note"] = "执行引导达到重试上限，已重置后重新进入自动补派发。"
            route_meta["executionBootstrap"] = execution
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            task["updatedAt"] = execution["at"]
            changed["value"] = True
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    return changed["value"]


def bootstrap_task_execution_state(openclaw_dir, task_id, agent_id, title="", note="", router_agent_id=""):
    if not task_id or not agent_id:
        return {}
    config = load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    agent_labels, _ = build_label_maps(load_agents(config), kanban_cfg, router_agent_id)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)
    timestamp = now_iso()
    agent_label = agent_labels.get(agent_id, agent_id)
    message = str(note or f"系统已将任务派发给 {agent_label} 开始执行。").strip()
    bootstrap = {}

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            state = str(task.get("state") or task.get("status") or "").strip()
            if state.lower() not in TERMINAL_STATES:
                task["state"] = "Doing"
            task["currentAgent"] = agent_id
            task["currentAgentLabel"] = agent_label
            task["targetAgentId"] = agent_id
            task["targetAgentLabel"] = agent_label
            task["org"] = agent_label
            task["currentUpdate"] = message
            task["now"] = message
            task["updatedAt"] = timestamp
            task.setdefault("flow_log", [])
            task["flow_log"].append(
                {
                    "at": timestamp,
                    "from": "OpenClaw Team",
                    "to": agent_label,
                    "remark": message,
                }
            )
            task.setdefault("progress_log", [])
            task["progress_log"].append(
                {
                    "at": timestamp,
                    "agent": agent_id,
                    "agentLabel": agent_label,
                    "text": message,
                    "state": "Doing",
                    "org": agent_label,
                    "todos": [],
                }
            )
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            existing_execution = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
            preserved_session_id = ""
            if str(existing_execution.get("agentId") or "").strip() == agent_id:
                preserved_session_id = str(existing_execution.get("sessionId") or "").strip()
            route_meta["targetAgentId"] = agent_id
            route_meta["targetAgentLabel"] = agent_label
            route_meta["executionBootstrap"] = {
                "status": "scheduled",
                "at": timestamp,
                "agentId": agent_id,
                "agentLabel": agent_label,
                "sessionId": preserved_session_id,
                "note": message,
                "attempts": max(int(existing_execution.get("attempts") or 0), 0) + 1,
                "firstScheduledAt": existing_execution.get("firstScheduledAt") or timestamp,
            }
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            bootstrap.update(
                {
                    "status": "scheduled",
                    "agentId": agent_id,
                    "agentLabel": agent_label,
                    "note": message,
                    "attempts": route_meta["executionBootstrap"]["attempts"],
                }
            )
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    return bootstrap


def resolve_task_dispatch_plan(
    openclaw_dir,
    title,
    remark="",
    preferred_team_id="",
    team_override_reason="",
    model_timeout_seconds=None,
    prefer_fast_routing=False,
    requested_lead_agent_id="",
):
    openclaw_dir = Path(openclaw_dir)
    config = load_config(openclaw_dir)
    agents = load_agents(config)
    valid_agent_ids = {str(agent.get("id") or "").strip() for agent in agents if isinstance(agent, dict)}
    router_agent_id = get_router_agent_id(config)
    project_dir = resolve_planning_project_dir(openclaw_dir, config=config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agent_labels, _label_to_ids = build_label_maps(agents, kanban_cfg, router_agent_id)
    bootstrap_agents = [
        {
            "id": str(agent.get("id") or "").strip(),
            "title": agent_labels.get(str(agent.get("id") or "").strip(), str(agent.get("id") or "").strip()),
            "name": ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", ""),
            "role": "",
            "skills": [],
        }
        for agent in agents
        if str(agent.get("id") or "").strip()
    ]
    _metadata, teams = ensure_default_team_mode(openclaw_dir, metadata, bootstrap_agents, router_agent_id)
    existing_tasks = merge_tasks(openclaw_dir, config)
    routing_decisions = store_list_routing_decisions(openclaw_dir, limit=240)
    planner_title = kanban_cfg.get("state_org_map", {}).get("Planning") or "Planner"
    if prefer_fast_routing:
        heuristic_intelligence = analyze_task_intelligence(
            title,
            remark,
            openclaw_dir=openclaw_dir,
            config=config,
            agents=agents,
            router_agent_id=router_agent_id,
            task_index=existing_tasks,
            allow_model=False,
            model_timeout_seconds=model_timeout_seconds,
        )
        intelligence = (
            analyze_task_intelligence(
                title,
                remark,
                openclaw_dir=openclaw_dir,
                config=config,
                agents=agents,
                router_agent_id=router_agent_id,
                task_index=existing_tasks,
                allow_model=True,
                model_timeout_seconds=model_timeout_seconds,
            )
            if should_use_model_task_intelligence(heuristic_intelligence, prefer_fast_routing=True)
            else heuristic_intelligence
        )
    else:
        intelligence = analyze_task_intelligence(
            title,
            remark,
            openclaw_dir=openclaw_dir,
            config=config,
            agents=agents,
            router_agent_id=router_agent_id,
            task_index=existing_tasks,
            allow_model=True,
            model_timeout_seconds=model_timeout_seconds,
        )
    workflow_resolution = enrich_workflow_binding_with_branches(
        choose_task_workflow_resolution(
            openclaw_dir,
            agents,
            router_agent_id,
            title=title,
            remark=remark,
            intelligence=intelligence,
        ),
        title=title,
        remark=remark,
    )
    decision = evaluate_routing_decision(
        openclaw_dir,
        title=title,
        remark=remark,
        agents=agents,
        task_index=existing_tasks,
        router_agent_id=router_agent_id,
        workflow_resolution=workflow_resolution,
        intelligence=intelligence,
    )
    if str(decision.get("targetAgentId") or "").strip() not in valid_agent_ids:
        decision["targetAgentId"] = router_agent_id
        decision["targetAgentLabel"] = router_agent_id
        decision.setdefault("trace", []).append("目标 Agent 不可用，已回退到当前路由 Agent。")

    auto_team_selection = resolve_default_task_team_record(
        openclaw_dir,
        target_agent_id=str(decision.get("targetAgentId") or "").strip(),
        preferred_team_id="",
        teams=teams,
        title=title,
        remark=remark,
        intelligence=intelligence,
        workflow_binding=workflow_resolution.get("binding", {}),
        agents=bootstrap_agents,
        task_index=existing_tasks,
        routing_decisions=routing_decisions,
        return_selection=True,
    )
    if str(preferred_team_id or "").strip():
        team_selection = resolve_default_task_team_record(
            openclaw_dir,
            target_agent_id=str(decision.get("targetAgentId") or "").strip(),
            preferred_team_id=preferred_team_id,
            teams=teams,
            title=title,
            remark=remark,
            intelligence=intelligence,
            workflow_binding=workflow_resolution.get("binding", {}),
            agents=bootstrap_agents,
            task_index=existing_tasks,
            routing_decisions=routing_decisions,
            return_selection=True,
        )
    else:
        team_selection = deepcopy(auto_team_selection)
    team = deepcopy(team_selection.get("team")) if isinstance(team_selection.get("team"), dict) else None
    team_selection_payload = deepcopy(team_selection.get("payload")) if isinstance(team_selection.get("payload"), dict) else {}
    auto_team_payload = deepcopy(auto_team_selection.get("payload")) if isinstance(auto_team_selection.get("payload"), dict) else {}
    preferred_team_id = str(preferred_team_id or "").strip()
    team_override_reason = str(team_override_reason or "").strip()
    recommended_team_id = str(auto_team_payload.get("selectedTeamId") or "").strip()
    selected_team_id = str(team_selection_payload.get("selectedTeamId") or "").strip()
    override_matches_recommendation = not preferred_team_id or not recommended_team_id or not selected_team_id or selected_team_id == recommended_team_id
    override_reason_required = bool(preferred_team_id and recommended_team_id and selected_team_id and selected_team_id != recommended_team_id)
    override_reason_missing = bool(override_reason_required and not team_override_reason)
    if team_selection_payload:
        team_selection_payload["recommendedTeamId"] = recommended_team_id
        team_selection_payload["recommendedTeamName"] = auto_team_payload.get("selectedTeamName", "")
        team_selection_payload["recommendedConfidence"] = auto_team_payload.get("confidence", 0)
        team_selection_payload["recommendedReasons"] = safe_list(auto_team_payload.get("reasons"))[:3]
        team_selection_payload["overrideMatchesRecommendation"] = override_matches_recommendation
        team_selection_payload["recommendedDifferent"] = bool(preferred_team_id and recommended_team_id and selected_team_id and selected_team_id != recommended_team_id)
        team_selection_payload["overrideSource"] = "manual" if preferred_team_id else "auto"
        team_selection_payload["overrideReason"] = team_override_reason
        team_selection_payload["overrideReasonRequired"] = override_reason_required
        team_selection_payload["overrideReasonMissing"] = override_reason_missing
        team_selection_payload["manualReviewRecommended"] = bool(team_selection_payload.get("manualReviewRecommended") or override_reason_missing)
    if team_selection_payload.get("selectedTeamId"):
        decision.setdefault("trace", []).append(
            "团队选择结果："
            f"{team_selection_payload.get('selectedTeamName') or team_selection_payload.get('selectedTeamId')} "
            f"(confidence={float(team_selection_payload.get('confidence') or 0):.2f}, "
            f"score={int(team_selection_payload.get('score') or 0)}, "
            f"gap={int(team_selection_payload.get('scoreGap') or 0)})。"
        )
        if safe_list(team_selection_payload.get("reasons")):
            decision.setdefault("trace", []).append(
                f"团队选择依据：{'；'.join(str(item or '').strip() for item in safe_list(team_selection_payload.get('reasons'))[:3] if str(item or '').strip())}。"
            )
        if team_selection_payload.get("recommendedDifferent"):
            decision.setdefault("trace", []).append(
                "当前显式指定的 Team 与系统推荐不一致："
                f"推荐 {team_selection_payload.get('recommendedTeamName') or team_selection_payload.get('recommendedTeamId') or '—'}，"
                f"当前选择 {team_selection_payload.get('selectedTeamName') or team_selection_payload.get('selectedTeamId') or '—'}。"
            )
        if team_override_reason:
            decision.setdefault("trace", []).append(f"人工指定 Team 的理由：{team_override_reason}")
        elif override_reason_missing:
            decision.setdefault("trace", []).append("当前显式指定的 Team 偏离系统推荐，但还没有填写人工 override 理由。")
        if team_selection_payload.get("manualReviewRecommended"):
            decision.setdefault("trace", []).append("团队选择分数接近或置信度偏低，建议人工复核后再继续推进。")

    routed_target_agent_id = str(decision.get("targetAgentId") or "").strip()
    routed_target_agent_label = str(decision.get("targetAgentLabel") or agent_labels.get(routed_target_agent_id, routed_target_agent_id)).strip()
    execution_target_agent_id = routed_target_agent_id
    if team:
        team_target_agent_id = resolve_team_execution_agent(
            team,
            preferred_agent_id=str(decision.get("targetAgentId") or "").strip(),
            agents=bootstrap_agents,
            title=title,
            remark=remark,
            intelligence=intelligence,
            workflow_binding=workflow_resolution.get("binding", {}),
        )
        if team_target_agent_id and team_target_agent_id != execution_target_agent_id:
            execution_target_agent_id = team_target_agent_id
            decision.setdefault("trace", []).append(
                f"按 Team {str(team.get('name') or team.get('id') or '').strip()} 的成员范围重定向执行目标到 {team_target_agent_id}。"
            )

    requested_lead_agent_id = str(requested_lead_agent_id or "").strip()
    selected_execution_agent_id = execution_target_agent_id
    team_candidate_ids = task_team_participant_agent_ids(team, lead_agent_id=str((team or {}).get("leadAgentId") or "").strip()) if team else []
    if requested_lead_agent_id:
        if (not team_candidate_ids and requested_lead_agent_id in valid_agent_ids) or requested_lead_agent_id in team_candidate_ids:
            selected_execution_agent_id = requested_lead_agent_id
            decision.setdefault("trace", []).append(f"已按显式指定的牵头人 {requested_lead_agent_id} 作为当前执行负责人。")
    if team_selection_payload:
        team_selection_payload["routedTargetAgentId"] = routed_target_agent_id
        team_selection_payload["routedTargetAgentLabel"] = routed_target_agent_label
        team_selection_payload["selectedExecutionAgentId"] = selected_execution_agent_id
        team_selection_payload["selectedExecutionAgentLabel"] = agent_labels.get(selected_execution_agent_id, selected_execution_agent_id)
        team_selection_payload["requestedLeadAgentId"] = requested_lead_agent_id

    validation = build_task_dispatch_validation(
        title,
        remark=remark,
        workflow_binding=workflow_resolution.get("binding", {}),
        intelligence=intelligence,
        team=team,
        requested_lead_agent_id=requested_lead_agent_id,
        valid_agent_ids=valid_agent_ids,
    )
    if has_blocking_task_dispatch_validation(validation):
        decision.setdefault("trace", []).append(validation.get("summary") or "任务校验未通过，暂不进入自动派发。")
        if team_selection_payload:
            team_selection_payload["manualReviewRecommended"] = True
            team_selection_payload["dispatchValidation"] = deepcopy(validation)

    return {
        "openclawDir": openclaw_dir,
        "config": config,
        "agents": agents,
        "routerAgentId": router_agent_id,
        "projectDir": project_dir,
        "kanbanCfg": kanban_cfg,
        "agentLabels": agent_labels,
        "existingTasks": existing_tasks,
        "plannerTitle": planner_title,
        "intelligence": intelligence,
        "workflowResolution": workflow_resolution,
        "decision": decision,
        "team": team,
        "teams": teams,
        "teamSelection": team_selection_payload,
        "selectedExecutionAgentId": selected_execution_agent_id,
        "validation": validation,
    }

def existing_task_team_thread(openclaw_dir, task_id, team_id=""):
    normalized_task_id = str(task_id or "").strip()
    normalized_team_id = str(team_id or "").strip()
    if not normalized_task_id:
        return {}
    fallback_thread_id = task_team_thread_id(task_id)
    fallback_thread = {}
    latest_managed_thread = {}
    latest_managed_dt = None
    for thread in store_list_chat_threads(openclaw_dir, limit=256):
        if not isinstance(thread, dict):
            continue
        if str(thread.get("id") or "").strip() == fallback_thread_id:
            if is_managed_task_execution_thread(thread, normalized_task_id, normalized_team_id):
                fallback_thread = thread
            continue
        if not is_managed_task_execution_thread(thread, normalized_task_id, normalized_team_id):
            continue
        thread_dt = parse_iso(thread.get("updatedAt"))
        if not latest_managed_thread or (thread_dt and (not latest_managed_dt or thread_dt >= latest_managed_dt)):
            latest_managed_thread = thread
            latest_managed_dt = thread_dt
    return fallback_thread or latest_managed_thread or {}


def ensure_task_execution_team_thread(openclaw_dir, task_id, title, team, lead_agent_id="", linked_run_id="", extra_participant_agent_ids=None):
    team = team if isinstance(team, dict) else {}
    team_id = str(team.get("id") or "").strip()
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    participants = order_agent_ids_for_human_turns(
        openclaw_dir,
        [*task_team_participant_agent_ids(team, lead_agent_id=lead_agent_id), *safe_list(extra_participant_agent_ids)],
        lead_agent_id=lead_agent_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    existing = existing_task_team_thread(openclaw_dir, task_id, team_id=team_id)
    existing_meta = deepcopy(existing.get("meta", {})) if isinstance(existing.get("meta"), dict) else {}
    existing_policy = existing_meta.get("teamPolicy") if isinstance(existing_meta.get("teamPolicy"), dict) else {}
    coordination_relay_policy = {"enabled": True, "strategy": "broadcast_summary"}
    next_meta = {
        **existing_meta,
        "teamId": team_id,
        "dispatchMode": normalize_chat_dispatch_mode(
            existing_meta.get("dispatchMode") or resolve_team_default_dispatch_mode(team),
            has_team=bool(team_id),
        ),
        "teamPolicy": merge_team_policy_state(team, existing_policy) if team_id else {},
        "coordinationRelay": existing_meta.get("coordinationRelay") if isinstance(existing_meta.get("coordinationRelay"), dict) else {},
        "coordinationRelayPolicy": coordination_relay_policy,
        "taskExecution": {
            "taskId": str(task_id or "").strip(),
            "teamId": team_id,
            "source": "task_dispatch",
            "managedBy": "mission-control",
            "coordinationProtocol": task_coordination_protocol_snapshot(team) if team_id else {},
        },
    }
    return store_save_chat_thread(
        openclaw_dir,
        {
            "id": existing.get("id", "") or task_team_thread_id(task_id),
            "title": existing.get("title", "") or (
                f"{title or task_id} · {str(team.get('name') or '').strip()}"
                if str(team.get("name") or "").strip()
                else f"{title or task_id} · Team"
            ),
            "status": existing.get("status", "") or "open",
            "channel": existing.get("channel", "") or "internal",
            "owner": existing.get("owner", "") or "OpenClaw Team",
            "primaryAgentId": lead_agent_id or existing.get("primaryAgentId", ""),
            "currentTargetAgentId": lead_agent_id or existing.get("currentTargetAgentId", ""),
            "linkedTaskId": str(task_id or "").strip(),
            "linkedDeliverableId": existing.get("linkedDeliverableId", ""),
            "linkedRunId": str(linked_run_id or existing.get("linkedRunId", "")).strip(),
            "participantAgentIds": participants,
            "participantHumans": existing.get("participantHumans", []),
            "meta": next_meta,
        },
    )
