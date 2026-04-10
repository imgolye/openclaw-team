from __future__ import annotations

import sys
from copy import deepcopy


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

    def __eq__(self, other):
        return self._resolve() == other

    def __hash__(self):
        return hash(self._resolve())

    def __repr__(self):
        return repr(self._resolve())

    def __str__(self):
        return str(self._resolve())


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


MANAGEMENT_AUTOMATION_MODE_FULL_AUTO = _DelegatedSymbol("MANAGEMENT_AUTOMATION_MODE_FULL_AUTO")
TERMINAL_STATES = _DelegatedSymbol("TERMINAL_STATES")
atomic_task_store_update = _DelegatedSymbol("atomic_task_store_update")
build_memory_projection_payloads = _DelegatedSymbol("build_memory_projection_payloads")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_auto_operation_profile = _DelegatedSymbol("compact_auto_operation_profile")
compact_company_auto_operation_runtime = _DelegatedSymbol("compact_company_auto_operation_runtime")
datetime = _DelegatedSymbol("datetime")
existing_task_team_thread = _DelegatedSymbol("existing_task_team_thread")
format_age = _DelegatedSymbol("format_age")
get_router_agent_id = _DelegatedSymbol("get_router_agent_id")
latest_routing_decision_for_task = _DelegatedSymbol("latest_routing_decision_for_task")
load_config = _DelegatedSymbol("load_config")
merge_tasks = _DelegatedSymbol("merge_tasks")
normalize_management_automation_mode = _DelegatedSymbol("normalize_management_automation_mode")
now_iso = _DelegatedSymbol("now_iso")
now_utc = _DelegatedSymbol("now_utc")
parse_iso = _DelegatedSymbol("parse_iso")
project_memory_records_async = _DelegatedSymbol("project_memory_records_async")
safe_list = _DelegatedSymbol("safe_list")
store_list_agent_teams = _DelegatedSymbol("store_list_agent_teams")
store_list_chat_threads = _DelegatedSymbol("store_list_chat_threads")
store_save_chat_thread = _DelegatedSymbol("store_save_chat_thread")
summarize_task_execution_text = _DelegatedSymbol("summarize_task_execution_text")
task_display_update = _DelegatedSymbol("task_display_update")
task_workspace_for_task = _DelegatedSymbol("task_workspace_for_task")
timezone = _DelegatedSymbol("timezone")

AUTO_OPERATION_COMPANY_TASK_TOKENS = (
    "经营公司",
    "经营这家公司",
    "运营公司",
    "公司经营",
    "公司运营",
    "经营团队",
    "经营节奏",
    "经营目标",
    "增长",
    "营收",
    "获客",
    "留存",
    "复购",
    "转化",
    "经营复盘",
    "business",
    "operate company",
    "run company",
    "company operation",
    "company ops",
    "growth",
    "revenue",
)


def is_company_auto_operation_task(title="", remark=""):
    values = " ".join([str(title or "").strip(), str(remark or "").strip()]).lower()
    if not values:
        return False
    return any(str(token or "").strip().lower() in values for token in AUTO_OPERATION_COMPANY_TASK_TOKENS)


def infer_company_operation_focus_areas(title="", remark=""):
    values = " ".join([str(title or "").strip(), str(remark or "").strip()]).lower()
    focus_areas = []

    def push(label):
        if label and label not in focus_areas:
            focus_areas.append(label)

    if any(token in values for token in ("增长", "growth", "获客", "拉新", "线索", "投放")):
        push("增长与获客")
    if any(token in values for token in ("产品", "体验", "转化", "留存", "功能", "用户反馈")):
        push("产品与用户体验")
    if any(token in values for token in ("营收", "收入", "revenue", "利润", "成本", "现金流")):
        push("营收与经营效率")
    if any(token in values for token in ("品牌", "市场", "内容", "公关", "微信", "公众号", "外部")):
        push("市场与对外沟通")
    if any(token in values for token in ("交付", "研发", "上线", "质量", "发布", "效率")):
        push("交付与组织效率")

    for fallback in ("增长与获客", "产品与用户体验", "营收与经营效率"):
        push(fallback)
        if len(focus_areas) >= 3:
            break
    return focus_areas[:3]


def build_company_auto_operation_profile(title="", remark="", automation_mode="", team=None):
    if not is_company_auto_operation_task(title, remark):
        return {}
    team = team if isinstance(team, dict) else {}
    normalized_mode = normalize_management_automation_mode(automation_mode)
    objective = summarize_task_execution_text(str(remark or title or "持续经营公司并产出经营结论。").strip(), limit=180)
    team_name = str(team.get("name") or "").strip()
    focus_areas = infer_company_operation_focus_areas(title, remark)
    profile = {
        "enabled": True,
        "taskType": "company_auto_operation",
        "mode": normalized_mode,
        "objective": objective or "持续经营公司并产出经营结论。",
        "teamName": team_name,
        "focusAreas": focus_areas,
        "operatingCadence": [
            "每天先看经营信号、风险和机会，再决定今天最该推进的动作。",
            "默认自己拆出下一步，不等用户逐条下指令。",
            "每轮动作后都要复盘结果，沉淀新的判断和经营规律。",
        ],
        "learningLoop": [
            "观察经营信号",
            "判断最值得推进的问题",
            "拆出动作并推进",
            "复盘结果并更新方法",
        ],
        "autonomyRules": [
            "把它当成长期经营任务，不是一次性交付物。",
            "默认先内部讨论，再决定主攻方向和今日分工。",
            "如果没有新的用户指令，也要基于现有上下文继续提出下一步。",
            "每天都要给出经营结论、关键变化和下一步动作。",
        ],
        "outputContract": "每轮至少给出：经营判断、今天动作、风险或阻塞、下一步。",
        "autoStart": normalized_mode == MANAGEMENT_AUTOMATION_MODE_FULL_AUTO,
    }
    return profile


def build_company_auto_operation_memory_payload(
    profile,
    runtime,
    current_focus="",
    summary_text="",
    next_move="",
    open_loop_items=None,
    active_owners=None,
    day_key="",
    reviewed_at="",
):
    profile = compact_auto_operation_profile(profile)
    runtime_payload = compact_company_auto_operation_runtime(runtime)
    focus_areas = clean_unique_strings(profile.get("focusAreas") or [])[:3]
    current_focus = summarize_task_execution_text(current_focus or runtime_payload.get("currentFocus") or "", limit=140)
    summary_text = summarize_task_execution_text(summary_text or runtime_payload.get("summaryText") or "", limit=180)
    next_move = summarize_task_execution_text(next_move or runtime_payload.get("nextMove") or "", limit=160)
    long_term_seed = clean_unique_strings(
        [
            f"长期围绕 {'、'.join(focus_areas)} 持续经营。" if focus_areas else "",
            runtime_payload.get("longTermMemory") or "",
            f"最近稳定判断：{summary_text}" if summary_text else "",
            f"当前推进：{next_move}" if next_move else "",
        ]
    )
    long_term_memory = summarize_task_execution_text(" ".join(long_term_seed), limit=220)
    learning_candidates = clean_unique_strings(
        [
            *(runtime_payload.get("learningHighlights") or []),
            current_focus,
            next_move,
            *[
                summarize_task_execution_text((item or {}).get("text") or "", limit=96)
                for item in safe_list(open_loop_items)
                if isinstance(item, dict)
            ],
        ]
    )[:4]
    review_summary = summarize_task_execution_text(summary_text or current_focus or next_move or "", limit=140)
    latest_note = {
        "dayKey": str(day_key or runtime_payload.get("lastReviewDayKey") or "").strip(),
        "focus": current_focus,
        "nextMove": next_move,
        "summary": review_summary,
        "ownerLabel": str((safe_list(active_owners)[0] if safe_list(active_owners) else "") or "").strip(),
    }
    merged_notes = []
    seen_review_keys = set()
    for item in [latest_note, *safe_list(runtime_payload.get("recentReviewNotes"))]:
        if not isinstance(item, dict):
            continue
        normalized = {
            "dayKey": str(item.get("dayKey") or "").strip(),
            "focus": summarize_task_execution_text(item.get("focus") or "", limit=96),
            "nextMove": summarize_task_execution_text(item.get("nextMove") or "", limit=96),
            "summary": summarize_task_execution_text(item.get("summary") or item.get("focus") or item.get("nextMove") or "", limit=140),
        }
        if not any(normalized.values()):
            continue
        review_key = (normalized.get("dayKey"), normalized.get("summary") or normalized.get("focus") or normalized.get("nextMove"))
        if review_key in seen_review_keys:
            continue
        seen_review_keys.add(review_key)
        merged_notes.append(normalized)
    return {
        "currentFocus": current_focus,
        "summaryText": summary_text,
        "nextMove": next_move,
        "longTermMemory": long_term_memory,
        "learningHighlights": learning_candidates,
        "recentReviewNotes": merged_notes[:4],
        "memoryUpdatedAt": str(reviewed_at or runtime_payload.get("memoryUpdatedAt") or now_iso()).strip(),
    }


def company_auto_operation_prompt_lines(profile, audience="lead", runtime=None):
    profile = compact_auto_operation_profile(profile)
    if not profile.get("enabled"):
        return []
    normalized_audience = str(audience or "").strip().lower()
    runtime_payload = compact_company_auto_operation_runtime(runtime)
    lines = [
        "这是“经营公司”类任务：把它当成持续经营，不是做完一轮就停。",
        f"经营目标：{profile.get('objective') or '持续经营公司并产出经营结论。'}",
    ]
    focus_areas = clean_unique_strings(profile.get("focusAreas") or [])
    if focus_areas:
        lines.append("本轮重点：")
        lines.extend(f"- {item}" for item in focus_areas[:3])
    cadence = clean_unique_strings(profile.get("operatingCadence") or [])
    if cadence:
        lines.append("经营节奏：")
        lines.extend(f"- {item}" for item in cadence[:3])
    learning_loop = clean_unique_strings(profile.get("learningLoop") or [])
    if learning_loop:
        lines.append("学习闭环：")
        lines.extend(f"- {item}" for item in learning_loop[:4])
    autonomy_rules = clean_unique_strings(profile.get("autonomyRules") or [])
    if autonomy_rules:
        lines.append("自动经营约定：")
        lines.extend(f"- {item}" for item in autonomy_rules[:4])
    long_term_memory = str(runtime_payload.get("longTermMemory") or "").strip()
    if long_term_memory:
        lines.append(f"长期经营记忆：{long_term_memory}")
    learning_highlights = clean_unique_strings(runtime_payload.get("learningHighlights") or [])
    if learning_highlights:
        lines.append("最近学到的经营规律：")
        lines.extend(f"- {item}" for item in learning_highlights[:3])
    recent_review_notes = safe_list(runtime_payload.get("recentReviewNotes"))
    if recent_review_notes:
        lines.append("近几轮经营复盘：")
        for item in recent_review_notes[:2]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("dayKey") or "").strip()
            summary = str(item.get("summary") or item.get("focus") or item.get("nextMove") or "").strip()
            if summary:
                lines.append(f"- {label + '：' if label else ''}{summary}")
    if normalized_audience == "lead":
        lines.append("你要像经营负责人一样先判断今天最该推进什么，再组织团队推进，不要等用户逐条派活。")
        lines.append("如果今天的方向已经清楚，就直接往前推进，并把新的经营结论沉淀下来。")
    else:
        lines.append("你要像真实经营团队成员一样补位：接住当前重点、主动提出动作，并把结果反馈回团队。")
        lines.append("不要只等 lead 点名；看到你能推进的经营动作就直接认领。")
    output_contract = str(profile.get("outputContract") or "").strip()
    if output_contract:
        lines.append(f"输出要求：{output_contract}")
    return lines


def task_route_meta_payload(task):
    task = task if isinstance(task, dict) else {}
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if route_meta:
        return route_meta
    meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
    return meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}


def task_auto_operation_profile_payload(task):
    route_meta = task_route_meta_payload(task)
    profile = compact_auto_operation_profile(route_meta.get("autoOperationProfile"))
    return profile if profile.get("enabled") else {}


def build_company_auto_operation_cycle_item(task, now, team=None, thread=None):
    task = task if isinstance(task, dict) else {}
    profile = task_auto_operation_profile_payload(task)
    if not profile.get("enabled"):
        return {}
    route_meta = task_route_meta_payload(task)
    runtime = compact_company_auto_operation_runtime(route_meta.get("autoOperationRuntime"))
    thread = thread if isinstance(thread, dict) else {}
    thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_policy = thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {}
    local_now = (now or now_utc()).astimezone()
    day_key = local_now.strftime("%Y-%m-%d")
    task_id = str(task.get("id") or "").strip()
    title = str(task.get("title") or task_id).strip() or task_id
    focus_areas = clean_unique_strings(profile.get("focusAreas") or [])[:3]
    current_update = summarize_task_execution_text(task_display_update(task), limit=160)
    team_name = (
        str((team or {}).get("name") or "").strip()
        or str(route_meta.get("teamName") or task.get("teamLabel") or "").strip()
    )
    execution_bootstrap = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
    status = str(execution_bootstrap.get("status") or task.get("state") or task.get("status") or "").strip()
    current_focus = summarize_task_execution_text(
        str(team_policy.get("currentFocus") or runtime.get("currentFocus") or current_update or "").strip(),
        limit=140,
    )
    if not current_focus:
        current_focus = (
            f"今天先看 {focus_areas[0]}，再决定最值得推进的一步。"
            if focus_areas
            else "今天先判断最值得推进的经营动作，再拉齐分工。"
        )
    open_loops = []
    raw_loop_items = team_policy.get("openLoopItems") if isinstance(team_policy.get("openLoopItems"), list) else runtime.get("openLoopItems")
    for item in safe_list(raw_loop_items):
        if isinstance(item, dict):
            text = summarize_task_execution_text(item.get("text") or "", limit=120)
            owner_label = str(item.get("ownerLabel") or "").strip()
        else:
            text = summarize_task_execution_text(item, limit=120)
            owner_label = ""
        if text:
            open_loops.append({"text": text, "ownerLabel": owner_label})
    if not open_loops:
        generated_loops = []
        if focus_areas:
            generated_loops.append(f"围绕 {focus_areas[0]} 拿到今天的经营判断")
        if len(focus_areas) > 1:
            generated_loops.append(f"把 {focus_areas[1]} 拆成一个今天能推进的动作")
        generated_loops.append("收尾同步今天结论和明日动作")
        open_loops = [{"text": text, "ownerLabel": ""} for text in generated_loops[:3]]
    active_owners = clean_unique_strings(team_policy.get("activeOwners") or route_meta.get("autoOperationRuntime", {}).get("activeOwners") or [])
    summary_text = summarize_task_execution_text(
        str(team_policy.get("workingMemory") or runtime.get("summaryText") or "").strip(),
        limit=180,
    )
    if not summary_text:
        summary_text = f"这是一条长期经营任务，默认围绕 {' / '.join(focus_areas) or '增长、产品和营收'} 持续观察、判断、推进、复盘。"
    next_move = summarize_task_execution_text(str(runtime.get("nextMove") or "").strip(), limit=160)
    if not next_move:
        next_move = open_loops[0]["text"] if open_loops else "先确认今天最值得推进的一步，再同步分工。"
    memory_payload = build_company_auto_operation_memory_payload(
        profile,
        runtime,
        current_focus=current_focus,
        summary_text=summary_text,
        next_move=next_move,
        open_loop_items=open_loops,
        active_owners=active_owners,
        day_key=day_key,
        reviewed_at=str(runtime.get("lastReviewedAt") or "").strip(),
    )
    return {
        "taskId": task_id,
        "title": title,
        "teamId": str((team or {}).get("id") or route_meta.get("teamId") or task.get("teamId") or "").strip(),
        "teamName": team_name,
        "mode": str(profile.get("mode") or "").strip(),
        "status": status,
        "focusAreas": focus_areas,
        "currentFocus": memory_payload.get("currentFocus") or current_focus,
        "summaryText": memory_payload.get("summaryText") or summary_text,
        "nextMove": memory_payload.get("nextMove") or next_move,
        "openLoopItems": open_loops[:3],
        "activeOwners": active_owners[:4],
        "longTermMemory": memory_payload.get("longTermMemory") or "",
        "learningHighlights": clean_unique_strings(memory_payload.get("learningHighlights") or [])[:4],
        "recentReviewNotes": safe_list(memory_payload.get("recentReviewNotes"))[:4],
        "memoryUpdatedAt": str(memory_payload.get("memoryUpdatedAt") or runtime.get("memoryUpdatedAt") or "").strip(),
        "updatedAt": str(task.get("updatedAt") or "").strip(),
        "updatedAgo": format_age(parse_iso(task.get("updatedAt")), now),
        "lastReviewDayKey": str(runtime.get("lastReviewDayKey") or "").strip(),
        "lastReviewedAt": str(runtime.get("lastReviewedAt") or "").strip(),
        "reviewDue": str(runtime.get("lastReviewDayKey") or "").strip() != day_key,
        "dayKey": day_key,
    }


def update_task_auto_operation_runtime_metadata(openclaw_dir, task_id, runtime_payload, router_agent_id=""):
    if not task_id or not isinstance(runtime_payload, dict):
        return False
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
            existing = route_meta.get("autoOperationRuntime") if isinstance(route_meta.get("autoOperationRuntime"), dict) else {}
            next_payload = {**existing, **runtime_payload}
            if next_payload == existing:
                continue
            route_meta["autoOperationRuntime"] = next_payload
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            changed["value"] = True
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    updated_task = next(
        (
            item
            for item in merge_tasks(openclaw_dir, config)
            if isinstance(item, dict) and str(item.get("id") or "").strip() == str(task_id or "").strip()
        ),
        {},
    )
    updated_route = task_route_meta_payload(updated_task)
    dispatch_state = updated_route.get("teamDispatch") if isinstance(updated_route.get("teamDispatch"), dict) else {}
    memory_records, memory_events = build_memory_projection_payloads(
        task_id=str(task_id or "").strip(),
        task_title=str(updated_task.get("title") or task_id).strip(),
        team_id=str(dispatch_state.get("teamId") or "").strip(),
        team_name=str(dispatch_state.get("teamName") or updated_task.get("teamLabel") or "").strip(),
        task_memory=updated_route.get("taskLongTermMemory"),
        dispatch_state=dispatch_state,
    )
    project_memory_records_async(openclaw_dir, memory_records, memory_events)
    return changed["value"]


def refresh_company_auto_operation_thread_policy(openclaw_dir, thread, runtime_item):
    thread = thread if isinstance(thread, dict) else {}
    runtime_item = runtime_item if isinstance(runtime_item, dict) else {}
    thread_id = str(thread.get("id") or "").strip()
    if not thread_id or not runtime_item.get("taskId"):
        return {}
    thread_meta = deepcopy(thread.get("meta", {})) if isinstance(thread.get("meta"), dict) else {}
    team_policy = thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {}
    updated_at = str(runtime_item.get("lastReviewedAt") or now_iso()).strip()
    current_focus = str(runtime_item.get("currentFocus") or "").strip()
    working_memory = str(runtime_item.get("summaryText") or "").strip()
    open_loop_items = [
        {
            "text": str(item.get("text") or "").strip(),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
        }
        for item in safe_list(runtime_item.get("openLoopItems"))
        if str((item or {}).get("text") or "").strip()
    ]
    next_team_policy = {
        **team_policy,
        **({"currentFocus": current_focus, "currentFocusUpdatedAt": updated_at} if current_focus else {}),
        **({"currentFocusItems": [{"text": current_focus, "ownerLabel": ""}]} if current_focus else {}),
        **({"workingMemory": working_memory, "workingMemoryUpdatedAt": updated_at} if working_memory else {}),
        "openLoopItems": open_loop_items,
        "openLoops": [item["text"] for item in open_loop_items],
        "activeOwners": clean_unique_strings(runtime_item.get("activeOwners") or []),
        "companyOperationMemory": {
            "longTermMemory": summarize_task_execution_text(runtime_item.get("longTermMemory") or "", limit=220),
            "learningHighlights": clean_unique_strings(runtime_item.get("learningHighlights") or [])[:4],
            "recentReviewNotes": safe_list(compact_company_auto_operation_runtime(runtime_item).get("recentReviewNotes"))[:3],
            "memoryUpdatedAt": str(runtime_item.get("memoryUpdatedAt") or updated_at).strip(),
        },
    }
    return store_save_chat_thread(
        openclaw_dir,
        {
            **thread,
            "updatedAt": updated_at,
            "meta": {
                **thread_meta,
                "teamPolicy": next_team_policy,
            },
        },
    )


def process_company_auto_operation_cycle(openclaw_dir, tasks, now, auto_refresh=False, config=None):
    config = config or load_config(openclaw_dir)
    team_map = {item.get("id"): item for item in store_list_agent_teams(openclaw_dir) if item.get("id")}
    items = []
    refreshed_count = 0
    for task in safe_list(tasks):
        if not isinstance(task, dict):
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES:
            continue
        route_meta = task_route_meta_payload(task)
        profile = compact_auto_operation_profile(route_meta.get("autoOperationProfile"))
        if not profile.get("enabled"):
            latest_decision = latest_routing_decision_for_task(openclaw_dir, str(task.get("id") or "").strip())
            latest_meta = (latest_decision.get("meta") or {}) if isinstance(latest_decision, dict) else {}
            profile = compact_auto_operation_profile(latest_meta.get("autoOperationProfile"))
            if profile.get("enabled"):
                patched_task = deepcopy(task)
                patched_task["routeDecision"] = {
                    **(route_meta if isinstance(route_meta, dict) else {}),
                    "autoOperationProfile": profile,
                }
                task = patched_task
                route_meta = task_route_meta_payload(task)
        if not profile.get("enabled"):
            continue
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = str(route_meta.get("teamId") or team_assignment.get("teamId") or task.get("teamId") or "").strip()
        team = team_map.get(team_id) if team_id else None
        thread = existing_task_team_thread(openclaw_dir, str(task.get("id") or "").strip(), team_id=team_id)
        item = build_company_auto_operation_cycle_item(task, now, team=team, thread=thread)
        if not item:
            continue
        if item.get("reviewDue"):
            memory_payload = build_company_auto_operation_memory_payload(
                profile,
                route_meta.get("autoOperationRuntime"),
                current_focus=item.get("currentFocus"),
                summary_text=item.get("summaryText"),
                next_move=item.get("nextMove"),
                open_loop_items=item.get("openLoopItems"),
                active_owners=item.get("activeOwners"),
                day_key=item.get("dayKey"),
                reviewed_at=now_iso(),
            )
            runtime_payload = {
                "lastReviewDayKey": item.get("dayKey"),
                "lastReviewedAt": now_iso(),
                "currentFocus": item.get("currentFocus"),
                "summaryText": item.get("summaryText"),
                "nextMove": item.get("nextMove"),
                "openLoopItems": item.get("openLoopItems"),
                "activeOwners": item.get("activeOwners"),
                "focusAreas": item.get("focusAreas"),
                "longTermMemory": memory_payload.get("longTermMemory"),
                "learningHighlights": memory_payload.get("learningHighlights"),
                "recentReviewNotes": memory_payload.get("recentReviewNotes"),
                "memoryUpdatedAt": memory_payload.get("memoryUpdatedAt"),
            }
            if update_task_auto_operation_runtime_metadata(
                openclaw_dir,
                str(task.get("id") or "").strip(),
                runtime_payload,
            ):
                refreshed_count += 1
            project_memory_records_async(
                openclaw_dir,
                [
                    {
                        "scope": "company",
                        "ownerId": "company",
                        "label": "公司级长期记忆",
                        "summary": str(memory_payload.get("longTermMemory") or "").strip(),
                        "learningHighlights": clean_unique_strings(memory_payload.get("learningHighlights") or []),
                        "recentNotes": safe_list(memory_payload.get("recentReviewNotes"))[:4],
                        "relatedTaskId": str(task.get("id") or "").strip(),
                        "relatedThreadId": str((thread or {}).get("id") or "").strip(),
                        "updatedAt": str(memory_payload.get("memoryUpdatedAt") or now_iso()).strip(),
                        "meta": {"source": "company-auto-operation"},
                    }
                ],
                [
                    {
                        "scope": "company",
                        "ownerId": "company",
                        "eventType": "review_update",
                        "summary": str(runtime_payload.get("summaryText") or runtime_payload.get("currentFocus") or "").strip(),
                        "relatedTaskId": str(task.get("id") or "").strip(),
                        "relatedThreadId": str((thread or {}).get("id") or "").strip(),
                        "meta": {
                            "source": "company-auto-operation",
                            "focusAreas": safe_list(runtime_payload.get("focusAreas")),
                        },
                    }
                ],
            )
            if auto_refresh and thread:
                refresh_company_auto_operation_thread_policy(
                    openclaw_dir,
                    thread,
                    {
                        **item,
                        **runtime_payload,
                    },
                )
                item["threadSynced"] = True
            item = {
                **item,
                **runtime_payload,
                "reviewDue": False,
            }
        items.append(item)
    items.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    return {
        "count": len(items),
        "activeCount": len(items),
        "refreshedCount": refreshed_count,
        "items": items[:8],
    }
