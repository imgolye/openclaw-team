"""Runtime part: task."""

import threading

from backend.domain.core.kanban import bundled_kanban_defaults


TASK_WORKSPACE_LOCKS = {}
TASK_WORKSPACE_LOCKS_GUARD = threading.Lock()


def task_workspace_lock(task_workspace):
    normalized = str(normalize_task_workspace_path(task_workspace))
    with TASK_WORKSPACE_LOCKS_GUARD:
        lock = TASK_WORKSPACE_LOCKS.get(normalized)
        if lock is None:
            lock = threading.Lock()
            TASK_WORKSPACE_LOCKS[normalized] = lock
    return lock

def seed_task_long_term_memory_payload(title="", remark="", team=None, memory_system=None, task_type=""):
    title_text = summarize_task_execution_text(title or "", limit=80)
    objective = summarize_task_execution_text(remark or title or "", limit=160)
    team_name = str((team or {}).get("name") or "").strip() if isinstance(team, dict) else ""
    template = task_memory_template(memory_system, task_type=task_type)
    summary_parts = clean_unique_strings(
        [
            f"这是一条需要持续推进的任务：{title_text}。" if title_text else "",
            f"当前目标：{objective}" if objective else "",
            f"默认由 {team_name} 持续跟进。" if team_name else "",
            str(template.get("bootstrapNote") or "").strip(),
        ]
    )
    return compact_task_long_term_memory(
        {
            "longTermMemory": " ".join(summary_parts),
            "learningHighlights": [],
            "recentNotes": (
                [
                    {
                        "at": now_iso(),
                        "summary": objective or title_text or "任务已建立，等待后续推进。",
                        "focus": objective or title_text,
                        "ownerLabel": team_name,
                    }
                ]
                if objective or title_text
                else []
            ),
            "updatedAt": now_iso(),
        }
    )


def build_task_long_term_memory_payload(existing_memory, dispatch_state, fallback_title="", fallback_note=""):
    existing = compact_task_long_term_memory(existing_memory)
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    updated_at = str(dispatch_state.get("at") or now_iso()).strip()
    summary_text = summarize_task_execution_text(dispatch_state.get("summaryText") or fallback_note or "", limit=180)
    response_entries = coordination_reply_entries(dispatch_state.get("responses"), limit=4)
    focus_items = build_team_current_focus_items(dispatch_state, limit=3)
    focus_lines = [str(item.get("text") or "").strip() for item in focus_items if isinstance(item, dict) and str(item.get("text") or "").strip()]
    open_loop_items = build_team_open_loop_items(dispatch_state, limit=3)
    learning_candidates = clean_unique_strings(
        [
            *(existing.get("learningHighlights") or []),
            *focus_lines[:2],
            *[
                summarize_task_execution_text((item or {}).get("text") or "", limit=96)
                for item in open_loop_items
                if isinstance(item, dict)
            ][:2],
        ]
    )[:4]
    long_term_parts = clean_unique_strings(
        [
            existing.get("longTermMemory") or "",
            f"持续推进任务：{summarize_task_execution_text(fallback_title or '', limit=80)}。" if fallback_title else "",
            f"最近稳定判断：{summary_text}" if summary_text else "",
        ]
    )
    latest_note = {
        "at": updated_at,
        "summary": summary_text or summarize_task_execution_text(fallback_note or fallback_title or "", limit=140),
        "focus": focus_lines[0] if focus_lines else summarize_task_execution_text(fallback_note or "", limit=96),
        "ownerLabel": str((response_entries[0] or {}).get("agentDisplayName") or "").strip() if response_entries else "",
    }
    merged_recent = []
    seen_recent = set()
    for item in [latest_note, *safe_list(existing.get("recentNotes"))]:
        if not isinstance(item, dict):
            continue
        normalized = {
            "at": str(item.get("at") or "").strip(),
            "summary": summarize_task_execution_text(item.get("summary") or "", limit=140),
            "focus": summarize_task_execution_text(item.get("focus") or "", limit=96),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
        }
        if not any(normalized.values()):
            continue
        note_key = (normalized.get("at"), normalized.get("summary") or normalized.get("focus"))
        if note_key in seen_recent:
            continue
        seen_recent.add(note_key)
        merged_recent.append(normalized)
    return {
        "longTermMemory": summarize_task_execution_text(" ".join(long_term_parts), limit=220),
        "learningHighlights": learning_candidates,
        "recentNotes": merged_recent[:4],
        "updatedAt": updated_at,
    }


def task_long_term_memory_prompt_lines(memory, audience="lead"):
    memory = compact_task_long_term_memory(memory)
    if not any([memory.get("longTermMemory"), memory.get("learningHighlights"), memory.get("recentNotes")]):
        return []
    normalized_audience = str(audience or "").strip().lower()
    highlight_limit = 2 if normalized_audience == "lead" else 1
    recent_limit = 2 if normalized_audience == "lead" else 1
    lines = []
    if memory.get("longTermMemory"):
        lines.append(f"长期记忆：{summarize_task_execution_text(memory.get('longTermMemory'), limit=120)}")
    learning_highlights = [
        summarize_task_execution_text(item, limit=72)
        for item in clean_unique_strings(memory.get("learningHighlights") or [])[:highlight_limit]
    ]
    learning_highlights = [item for item in learning_highlights if item]
    recent_notes = safe_list(memory.get("recentNotes"))
    rendered_recent = []
    if recent_notes:
        for item in recent_notes:
            if not isinstance(item, dict):
                continue
            summary = summarize_task_execution_text(item.get("summary") or item.get("focus") or "", limit=80)
            if not summary:
                continue
            label = str(item.get("at") or "").strip()
            rendered_recent.append(f"{label + '：' if label else ''}{summary}")
            if len(rendered_recent) >= recent_limit:
                break
    if learning_highlights:
        lines.append(f"最近学到：{'；'.join(learning_highlights)}")
    if rendered_recent:
        lines.append(f"最近推进：{'；'.join(rendered_recent)}")
    if normalized_audience == "lead":
        lines.append("先沿用这份长期记忆，再往下推进，不要每次都像第一次接手；除非对方明确在问历史、复盘或判断依据，否则不要逐条复述。")
    else:
        lines.append("补位时先接住这里已有的判断，再补你的新增动作；默认把这层长期记忆当作你已经记得的背景，不要在可见回复里把它重新讲一遍。")
    return lines


def is_company_auto_operation_task(title="", remark=""):
    values = " ".join([str(title or "").strip(), str(remark or "").strip()]).lower()
    if not values:
        return False
    return any(str(token or "").strip().lower() in values for token in AUTO_OPERATION_COMPANY_TASK_TOKENS)


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


def load_kanban_config(openclaw_dir, router_agent_id):
    router_cfg = Path(openclaw_dir) / f"workspace-{router_agent_id}" / "data" / "kanban_config.json"
    cfg = load_json(router_cfg, None)
    if cfg:
        return cfg

    for path in sorted(Path(openclaw_dir).glob("workspace-*/data/kanban_config.json")):
        cfg = load_json(path, None)
        if cfg:
            return cfg
    return bundled_kanban_defaults()


def load_tasks_from_workspace(workspace):
    workspace_path = Path(workspace).expanduser().resolve()
    openclaw_dir = workspace_path.parent
    workspace_id = workspace_path.name[len("workspace-"):] if workspace_path.name.startswith("workspace-") else workspace_path.name
    return store_list_task_records(
        openclaw_dir,
        workspace_id=workspace_id,
        workspace_path=str(workspace_path),
    )


def merge_task_records(preferred, fallback):
    preferred = preferred if isinstance(preferred, dict) else {}
    fallback = fallback if isinstance(fallback, dict) else {}
    if not fallback:
        return deepcopy(preferred)
    if not preferred:
        return deepcopy(fallback)

    merged = deepcopy(fallback)
    merged.update(deepcopy(preferred))

    fallback_meta = fallback.get("meta") if isinstance(fallback.get("meta"), dict) else {}
    preferred_meta = preferred.get("meta") if isinstance(preferred.get("meta"), dict) else {}
    if fallback_meta or preferred_meta:
        merged_meta = deepcopy(fallback_meta)
        merged_meta.update(deepcopy(preferred_meta))

        fallback_route = fallback_meta.get("routeDecision") if isinstance(fallback_meta.get("routeDecision"), dict) else {}
        preferred_route = preferred_meta.get("routeDecision") if isinstance(preferred_meta.get("routeDecision"), dict) else {}
        if fallback_route or preferred_route:
            merged_route = deepcopy(fallback_route)
            merged_route.update(deepcopy(preferred_route))
            for key in ("executionBootstrap", "teamAssignment", "executionSync", "teamDispatch", "outcome"):
                fallback_value = fallback_route.get(key) if isinstance(fallback_route.get(key), dict) else {}
                preferred_value = preferred_route.get(key) if isinstance(preferred_route.get(key), dict) else {}
                if fallback_value or preferred_value:
                    merged_value = deepcopy(fallback_value)
                    merged_value.update(deepcopy(preferred_value))
                    merged_route[key] = merged_value
            merged_meta["routeDecision"] = merged_route

        if fallback_meta.get("workflowBinding") and not preferred_meta.get("workflowBinding"):
            merged_meta["workflowBinding"] = deepcopy(fallback_meta.get("workflowBinding"))
        if fallback_meta.get("planningBundle") and not preferred_meta.get("planningBundle"):
            merged_meta["planningBundle"] = deepcopy(fallback_meta.get("planningBundle"))
        merged["meta"] = merged_meta

    if not isinstance(merged.get("routeDecision"), dict):
        meta_route = (merged.get("meta") or {}).get("routeDecision") if isinstance(merged.get("meta"), dict) else {}
        if isinstance(meta_route, dict) and meta_route:
            merged["routeDecision"] = deepcopy(meta_route)
    if not isinstance(merged.get("workflowBinding"), dict):
        meta_workflow = (merged.get("meta") or {}).get("workflowBinding") if isinstance(merged.get("meta"), dict) else {}
        if isinstance(meta_workflow, dict) and meta_workflow:
            merged["workflowBinding"] = deepcopy(meta_workflow)
    if not isinstance(merged.get("planningBundle"), dict):
        meta_planning = (merged.get("meta") or {}).get("planningBundle") if isinstance(merged.get("meta"), dict) else {}
        if isinstance(meta_planning, dict) and meta_planning:
            merged["planningBundle"] = deepcopy(meta_planning)

    for key in ("teamId", "teamLabel", "targetAgentId", "targetAgentLabel"):
        if not str(merged.get(key) or "").strip() and str(fallback.get(key) or "").strip():
            merged[key] = fallback.get(key)
    return merged


TASK_OWNER_REVIEW_KEYWORDS = (
    "待负责人审批",
    "待审批",
    "等待审批",
    "待审核",
    "待复核",
    "pending approval",
    "awaiting approval",
)


def task_waiting_owner_review(task):
    task = task if isinstance(task, dict) else {}
    state = str(task.get("state") or task.get("status") or "").strip().lower()
    if state in TERMINAL_STATES or state == "blocked":
        return False
    combined = " ".join(
        str(task.get(key) or "").strip()
        for key in ("now", "currentUpdate", "resultSummary", "remark")
    ).strip()
    if not combined:
        return False
    lowered = combined.lower()
    return any(keyword in combined or keyword in lowered for keyword in TASK_OWNER_REVIEW_KEYWORDS)


def normalize_task_review_handoff(task):
    task = deepcopy(task) if isinstance(task, dict) else {}
    if not task_waiting_owner_review(task):
        return task
    owner_label = (
        str(task.get("official") or "").strip()
        or str(task.get("owner") or "").strip()
        or str(task.get("org") or "").strip()
        or "负责人"
    )
    task["state"] = "Review"
    task["status"] = "Review"
    task["currentAgent"] = ""
    task["currentAgentLabel"] = owner_label
    if not str(task.get("org") or "").strip():
        task["org"] = owner_label
    return task


def merge_tasks(openclaw_dir, config):
    merged = {}
    for agent in load_agents(config):
        workspace = Path(agent.get("workspace", "")) if agent.get("workspace") else Path(openclaw_dir) / f"workspace-{agent['id']}"
        for task in load_tasks_from_workspace(workspace):
            if not isinstance(task, dict):
                continue
            task_id = task.get("id")
            if not task_id:
                continue
            previous = merged.get(task_id)
            previous_dt = parse_iso((previous or {}).get("updatedAt"))
            current_dt = parse_iso(task.get("updatedAt"))
            if previous is None:
                merged[task_id] = deepcopy(task)
                continue
            current_is_preferred = current_dt and (previous_dt is None or current_dt >= previous_dt)
            preferred = task if current_is_preferred else previous
            fallback = previous if current_is_preferred else task
            merged[task_id] = merge_task_records(preferred, fallback)
    normalized_items = [normalize_task_review_handoff(item) for item in merged.values()]
    return sorted(
        normalized_items,
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )


def is_control_plane_progress_entry(task, entry, router_agent_id=""):
    if not isinstance(task, dict) or not isinstance(entry, dict):
        return False
    normalized_router_agent_id = str(router_agent_id or "").strip()
    agent_id = str(entry.get("agent") or "").strip()
    if not normalized_router_agent_id or agent_id != normalized_router_agent_id:
        return False
    target_agent_id = str(task.get("targetAgentId") or "").strip()
    if not target_agent_id:
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
        route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
        target_agent_id = str(route_meta.get("targetAgentId") or "").strip()
    if not target_agent_id or target_agent_id == normalized_router_agent_id:
        return False
    text = str(entry.get("text") or "").strip()
    sync_state = task_execution_sync_for_task(task)
    sync_text = str(sync_state.get("text") or "").strip()
    if sync_text and text == sync_text:
        return True
    lowered = text.lower()
    return "看板已同步" in text or "已同步最新执行进展" in text or "captured" in lowered and "progress" in lowered


def current_agent_for_task(task, kanban_cfg, router_agent_id):
    if task_waiting_owner_review(task):
        return ""
    meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
    route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
    execution = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
    execution_agent = str(execution.get("agentId") or "").strip()
    explicit_current_agent = str(task.get("currentAgent") or "").strip()
    target_agent = str(task.get("targetAgentId") or "").strip()
    if not target_agent:
        target_agent = str(route_meta.get("targetAgentId") or "").strip()
    if explicit_current_agent and target_agent and explicit_current_agent == target_agent:
        return explicit_current_agent
    team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
    allowed_progress_agents = {
        agent_id
        for agent_id in [
            execution_agent,
            target_agent,
            str(team_assignment.get("leadAgentId") or "").strip(),
            *[str(item or "").strip() for item in safe_list(team_assignment.get("memberAgentIds"))],
        ]
        if agent_id
    }
    progress = latest_progress_event(task, router_agent_id=router_agent_id)
    if progress and progress.get("agent"):
        progress_agent = str(progress.get("agent") or "").strip()
        if progress_agent and (not allowed_progress_agents or progress_agent in allowed_progress_agents):
            return progress_agent

    if explicit_current_agent and (
        not allowed_progress_agents
        or explicit_current_agent in allowed_progress_agents
        or explicit_current_agent == target_agent
    ):
        return explicit_current_agent

    if execution_agent:
        return execution_agent
    if target_agent:
        return target_agent

    state = str(task.get("state", task.get("status", "")))
    org = task.get("org", "")
    if state in ("Doing", "Next") and kanban_cfg.get("org_agent_map", {}).get(org):
        return kanban_cfg["org_agent_map"][org]

    agent_id = kanban_cfg.get("state_agent_map", {}).get(state)
    if agent_id == "main":
        return router_agent_id
    return agent_id


def task_route(task):
    labels = []
    for entry in task.get("flow_log", []):
        if entry.get("from"):
            labels.append(str(entry["from"]))
        if entry.get("to"):
            labels.append(str(entry["to"]))
    deduped = []
    for label in labels:
        if not deduped or deduped[-1] != label:
            deduped.append(label)
    return deduped[-8:]


def summarize_task_output_file(output_path, limit=220, read_limit=8192):
    raw_path = str(output_path or "").strip()
    if not raw_path:
        return ""
    candidate = Path(raw_path).expanduser()
    if not candidate.exists() or not candidate.is_file():
        return ""
    if candidate.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".pdf", ".zip", ".xlsx", ".xls", ".docx", ".pptx"}:
        return ""
    try:
        raw_text = candidate.read_bytes()[:read_limit].decode("utf-8", errors="ignore")
    except OSError:
        return ""
    if not raw_text.strip():
        return ""
    lines = []
    in_frontmatter = False
    frontmatter_seen = False
    for raw_line in raw_text.splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        if line == "---" and not frontmatter_seen and not lines:
            in_frontmatter = True
            frontmatter_seen = True
            continue
        if in_frontmatter:
            if line == "---":
                in_frontmatter = False
            continue
        line = re.sub(r"<[^>]+>", " ", line)
        line = re.sub(r"^#{1,6}\s*", "", line)
        line = re.sub(r"^[-*+]\s+", "", line)
        line = re.sub(r"^\d+[.)]\s+", "", line)
        line = re.sub(r"[`*_>#]+", " ", line)
        line = re.sub(r"\s+", " ", line).strip()
        if not line:
            continue
        lines.append(line)
        if len(" ".join(lines)) >= limit * 2:
            break
    return summarize_task_execution_text(" ".join(lines), limit=limit)


def infer_deliverable_type(output_path):
    suffix = Path(str(output_path or "").strip()).suffix.lower()
    if suffix in {".json", ".csv", ".xlsx", ".xls"}:
        return "report"
    if suffix in {".py", ".ts", ".tsx", ".js", ".jsx", ".sh"}:
        return "code"
    if suffix in {".html", ".md", ".txt", ".docx", ".pdf"}:
        return "document"
    return "artifact"


def task_display_update(task):
    task = task if isinstance(task, dict) else {}
    current_update = str(task.get("now") or task.get("currentUpdate") or "").strip()
    normalized_status = str(task.get("state") or task.get("status") or "").strip().lower()
    if current_update and (normalized_status == "blocked" or current_update not in STALE_TASK_BLOCK_SUMMARIES):
        return current_update
    meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if not route_meta:
        route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
    team_dispatch = route_meta.get("teamDispatch") if isinstance(route_meta.get("teamDispatch"), dict) else {}
    dispatch_summary = summarize_task_execution_text(team_dispatch.get("summaryText") or "", limit=220)
    if dispatch_summary:
        return dispatch_summary
    execution = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
    execution_note = summarize_task_execution_text(execution.get("note") or "", limit=220)
    if execution_note and str(execution.get("status") or "").strip().lower() not in {"failed", "blocked"}:
        return execution_note
    replay_entries = sorted(
        [item for item in safe_list(task.get("replay")) if isinstance(item, dict)],
        key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    for entry in replay_entries:
        detail = summarize_task_execution_text(entry.get("detail") or entry.get("headline") or "", limit=220)
        if not detail or detail in STALE_TASK_BLOCK_SUMMARIES:
            continue
        kind = str(entry.get("kind") or "").strip().lower()
        if kind == "blocked":
            continue
        return detail
    route_reason = summarize_task_execution_text(route_meta.get("reason") or "", limit=220)
    if route_reason:
        return route_reason
    return current_update


def task_result_summary(task):
    task = task if isinstance(task, dict) else {}
    meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
    route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
    outcome = route_meta.get("outcome") if isinstance(route_meta.get("outcome"), dict) else {}
    candidates = [
        summarize_task_output_file(task.get("output")),
        str(outcome.get("note") or "").strip(),
        task_display_update(task),
    ]
    for candidate in candidates:
        if candidate:
            return summarize_task_execution_text(candidate)
    return ""


def routing_priority_rank(value):
    normalized = str(value or "normal").strip().lower()
    return {"low": 1, "normal": 2, "high": 3, "critical": 4}.get(normalized, 2)


def workflow_branch_expression_matches(source_text, expression):
    text = str(source_text or "").strip().lower()
    rule = str(expression or "").strip().lower()
    if not text or not rule:
        return False
    if "|" in rule:
        return any(workflow_branch_expression_matches(text, part) for part in rule.split("|"))
    normalized = rule
    for prefix in ("contains:", "contains=", "contains "):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):].strip()
            break
    terms = [part.strip() for part in re.split(r"[,+]", normalized) if part.strip()]
    if not terms:
        terms = [normalized]
    return all(term in text for term in terms)


def evaluate_workflow_branching(workflow, title="", remark=""):
    source_text = " ".join(part for part in [str(title or "").strip(), str(remark or "").strip()] if part).strip()
    lanes = {
        str(item.get("id", "")).strip(): item
        for item in (workflow.get("lanes") if isinstance(workflow.get("lanes"), list) else [])
        if isinstance(item, dict)
    }
    nodes = [item for item in (workflow.get("nodes") if isinstance(workflow.get("nodes"), list) else []) if isinstance(item, dict)]
    node_map = {str(item.get("id", "")).strip(): item for item in nodes if str(item.get("id", "")).strip()}
    matches = []
    for node in nodes:
        for branch in node.get("conditions", []) if isinstance(node.get("conditions"), list) else []:
            if not isinstance(branch, dict):
                continue
            expression = str(branch.get("expression", "")).strip()
            if not workflow_branch_expression_matches(source_text, expression):
                continue
            target_lane_id = str(branch.get("targetLaneId", "")).strip()
            target_node_id = str(branch.get("targetNodeId", "")).strip()
            target_lane = lanes.get(target_lane_id, {})
            target_node = node_map.get(target_node_id, {})
            matches.append(
                {
                    "id": str(branch.get("id", "")).strip() or secrets.token_hex(4),
                    "label": str(branch.get("label", "")).strip() or expression or "branch",
                    "expression": expression,
                    "sourceNodeId": str(node.get("id", "")).strip(),
                    "sourceNodeTitle": str(node.get("title", "")).strip(),
                    "targetLaneId": target_lane_id,
                    "targetLaneTitle": str(target_lane.get("title", "")).strip(),
                    "targetNodeId": target_node_id,
                    "targetNodeTitle": str(target_node.get("title", "")).strip(),
                }
            )
    return {
        "sourceText": source_text,
        "matches": matches,
        "selected": matches[0] if matches else {},
    }


def infer_model_task_decision(openclaw_dir, config, agents, router_agent_id, title, remark="", task_index=None, timeout_seconds=None):
    if not openclaw_dir or not config:
        return {}
    workflows = store_list_orchestration_workflows(openclaw_dir)
    policies = [
        item
        for item in store_list_routing_policies(openclaw_dir)
        if str(item.get("status") or "").lower() == "active"
    ]
    result = run_model_task_decision(
        openclaw_dir,
        config,
        title,
        remark=remark,
        agents=agents,
        workflows=workflows,
        policies=policies,
        router_agent_id=router_agent_id,
        paused_agent_ids=paused_agent_ids(openclaw_dir),
        timeout_seconds_override=timeout_seconds,
    )
    return result.get("decision", {}) if isinstance(result, dict) and result.get("ok") else {}


def analyze_task_intelligence(
    title,
    remark="",
    openclaw_dir=None,
    config=None,
    agents=None,
    router_agent_id="",
    task_index=None,
    allow_model=False,
    model_timeout_seconds=None,
):
    source_text = " ".join(part for part in [str(title or "").strip(), str(remark or "").strip()] if part).strip()
    source_text_lower = source_text.lower()
    priority = detect_requested_priority(source_text)
    ranked = []
    for profile in TASK_INTELLIGENCE_PROFILES:
        matches = [keyword for keyword in profile["keywords"] if keyword and keyword in source_text_lower]
        if matches:
            ranked.append((profile, matches))
    ranked.sort(key=lambda item: (-len(item[1]), item[0]["id"]))
    profile, matches = ranked[0] if ranked else (
        {
            "id": "general",
            "label": "通用协作",
            "workflowTemplate": "delivery",
            "laneHints": ("intake", "build"),
            "risk": "good",
        },
        [],
    )
    risk = profile.get("risk", "good")
    if priority in {"high", "critical"} and risk == "good":
        risk = "watch"
    if any(keyword in source_text_lower for keyword in HIGH_RISK_KEYWORDS):
        risk = "high"
    confidence = min(0.96, 0.42 + len(matches) * 0.14 + (0.08 if priority in {"high", "critical"} else 0.0))
    manual_review = risk == "high" or any(keyword in source_text_lower for keyword in MANUAL_REVIEW_KEYWORDS)
    intelligence = {
        "category": profile["id"],
        "categoryLabel": profile["label"],
        "matchedKeywords": matches,
        "priorityLevel": priority,
        "riskLevel": risk,
        "confidence": round(confidence, 2),
        "manualReview": manual_review,
        "suggestedWorkflowTemplate": profile.get("workflowTemplate", "delivery"),
        "laneHints": list(profile.get("laneHints", ()) or []),
        "sourceText": source_text,
        "decisionSource": "heuristic",
    }
    if allow_model and openclaw_dir:
        local_config = config or load_config(openclaw_dir)
        local_agents = agents or load_agents(local_config)
        local_router = router_agent_id or get_router_agent_id(local_config)
        model_decision = infer_model_task_decision(
            openclaw_dir,
            local_config,
            local_agents,
            local_router,
            title,
            remark=remark,
            task_index=task_index,
            timeout_seconds=model_timeout_seconds,
        )
        if model_decision.get("used"):
            intelligence.update(
                {
                    "category": model_decision.get("category") or intelligence["category"],
                    "categoryLabel": model_decision.get("categoryLabel") or intelligence["categoryLabel"],
                    "priorityLevel": model_decision.get("priorityLevel") or intelligence["priorityLevel"],
                    "riskLevel": model_decision.get("riskLevel") or intelligence["riskLevel"],
                    "confidence": model_decision.get("confidence", intelligence["confidence"]),
                    "manualReview": bool(model_decision.get("manualReview", intelligence["manualReview"])),
                    "suggestedWorkflowTemplate": model_decision.get("suggestedWorkflowTemplate") or intelligence["suggestedWorkflowTemplate"],
                    "laneHints": list(dict.fromkeys([*intelligence.get("laneHints", []), *(model_decision.get("laneHints", []) or [])]))[:6],
                    "decisionSource": "model",
                    "modelDecision": model_decision,
                }
            )
    return intelligence


def should_use_model_task_intelligence(intelligence, prefer_fast_routing=False):
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    if not prefer_fast_routing:
        return True
    if bool(intelligence.get("manualReview")):
        return True
    if str(intelligence.get("riskLevel") or "").strip().lower() == "high":
        return True
    matched_keywords = safe_list(intelligence.get("matchedKeywords"))
    lane_hints = safe_list(intelligence.get("laneHints"))
    confidence = float(intelligence.get("confidence") or 0)
    if confidence < FAST_TASK_CREATE_HEURISTIC_CONFIDENCE_THRESHOLD:
        return True
    if not matched_keywords and not lane_hints:
        return True
    return False


def workflow_semantic_score(workflow, intelligence):
    score = 0
    meta = workflow.get("meta") if isinstance(workflow.get("meta"), dict) else {}
    template_id = str(meta.get("templateId") or "").strip().lower()
    if intelligence.get("suggestedWorkflowTemplate") and template_id == intelligence.get("suggestedWorkflowTemplate"):
        score += 60
    haystack = " ".join(
        [
            str(workflow.get("name") or "").lower(),
            str(workflow.get("description") or "").lower(),
            " ".join(str(lane.get("title") or lane.get("id") or "").lower() for lane in workflow.get("lanes", []) if isinstance(lane, dict)),
        ]
    )
    for keyword in intelligence.get("matchedKeywords", []):
        if keyword in haystack:
            score += 16
    for hint in intelligence.get("laneHints", []):
        if hint and hint in haystack:
            score += 12
    if meta.get("starter"):
        score += 4
    return score


def choose_task_workflow_resolution(openclaw_dir, agents, router_agent_id, title="", remark="", intelligence=None):
    intelligence = intelligence or analyze_task_intelligence(title, remark)
    workflows = store_list_orchestration_workflows(openclaw_dir)
    if not workflows:
        workflow = default_orchestration_workflow(agents, router_agent_id)
        return {
            "workflow": workflow,
            "version": None,
            "binding": {
                "workflowId": workflow.get("id", ""),
                "workflowName": workflow.get("name", ""),
                "workflowVersionId": "",
                "workflowVersionNumber": 0,
                "stageSource": "default",
                "selectionReason": "当前没有自定义工作流，使用默认流程。",
                "suggestedTemplate": intelligence.get("suggestedWorkflowTemplate", ""),
            },
        }
    model_decision = intelligence.get("modelDecision", {}) if isinstance(intelligence.get("modelDecision"), dict) else {}
    preferred_workflow_id = str(model_decision.get("workflowId") or "").strip()
    if preferred_workflow_id:
        chosen = next((item for item in workflows if str(item.get("id") or "").strip() == preferred_workflow_id), None)
        if chosen:
            versions = store_list_orchestration_workflow_versions(openclaw_dir, workflow_id=chosen.get("id"), limit=1)
            version = versions[0] if versions else None
            return {
                "workflow": chosen,
                "version": version,
                "binding": {
                    "workflowId": chosen.get("id", ""),
                    "workflowName": chosen.get("name", ""),
                    "workflowVersionId": version.get("id", "") if version else "",
                    "workflowVersionNumber": version.get("versionNumber", 0) if version else 0,
                    "stageSource": "workflow_model",
                    "selectionReason": model_decision.get("reason", "") or f"模型判断优先匹配到 {chosen.get('name', '')}。",
                    "suggestedTemplate": intelligence.get("suggestedWorkflowTemplate", ""),
                },
            }
    ranked = sorted(workflows, key=lambda item: (-workflow_semantic_score(item, intelligence), item.get("updatedAt", "")), reverse=False)
    chosen = ranked[0]
    versions = store_list_orchestration_workflow_versions(openclaw_dir, workflow_id=chosen.get("id"), limit=1)
    version = versions[0] if versions else None
    return {
        "workflow": chosen,
        "version": version,
        "binding": {
            "workflowId": chosen.get("id", ""),
            "workflowName": chosen.get("name", ""),
            "workflowVersionId": version.get("id", "") if version else "",
            "workflowVersionNumber": version.get("versionNumber", 0) if version else 0,
            "stageSource": "workflow",
            "selectionReason": f"按任务语义优先匹配到 {chosen.get('name', '')}。",
            "suggestedTemplate": intelligence.get("suggestedWorkflowTemplate", ""),
        },
    }


def lane_match_score(lane, intelligence):
    lane_text = " ".join([str(lane.get("id") or "").lower(), str(lane.get("title") or "").lower(), str(lane.get("subtitle") or "").lower()])
    score = 0
    for hint in intelligence.get("laneHints", []):
        if hint and hint in lane_text:
            score += 10
    return score


def task_semantic_agent_preferences(intelligence=None):
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    ordered = []

    def append(agent_id):
        normalized = str(agent_id or "").strip()
        if normalized and normalized not in ordered:
            ordered.append(normalized)

    category = str(intelligence.get("category") or "").strip().lower()
    for agent_id in TASK_CATEGORY_AGENT_PRIORITY.get(category, ()):
        append(agent_id)
    for hint in safe_list(intelligence.get("laneHints")):
        normalized_hint = str(hint or "").strip().lower()
        for agent_id in LANE_HINT_AGENT_PRIORITY.get(normalized_hint, ()):
            append(agent_id)
    return ordered


def semantic_agent_token_score(agent_id, text):
    normalized_text = str(text or "").strip().lower()
    if not normalized_text:
        return 0
    return sum(1 for token in ROUTING_AGENT_TOKEN_GROUPS.get(str(agent_id or "").strip(), set()) if token and token in normalized_text)


def choose_semantic_agent_candidate(candidate_ids, text_fragments=None, intelligence=None):
    ordered_candidates = []
    for agent_id in safe_list(candidate_ids):
        normalized = str(agent_id or "").strip()
        if normalized and normalized not in ordered_candidates:
            ordered_candidates.append(normalized)
    if not ordered_candidates:
        return ""

    for agent_id in task_semantic_agent_preferences(intelligence):
        if agent_id in ordered_candidates:
            return agent_id

    normalized_text = " ".join(
        str(fragment or "").strip()
        for fragment in safe_list(text_fragments)
        if str(fragment or "").strip()
    ).lower()
    ranked = []
    for index, agent_id in enumerate(ordered_candidates):
        score = semantic_agent_token_score(agent_id, normalized_text)
        if score > 0:
            ranked.append((score, index, agent_id))
    if ranked:
        ranked.sort(key=lambda item: (-item[0], item[1], item[2]))
        return ranked[0][2]
    return ""


def resolve_workflow_target_agent(workflow_resolution, intelligence, router_agent_id, agents=None):
    workflow = workflow_resolution.get("workflow") if isinstance(workflow_resolution, dict) else {}
    lanes = [lane for lane in workflow.get("lanes", []) if isinstance(lane, dict)]
    nodes = [node for node in workflow.get("nodes", []) if isinstance(node, dict)]
    selected_branch = ((workflow_resolution or {}).get("binding") or {}).get("selectedBranch", {})
    branch_lane_id = str(selected_branch.get("targetLaneId") or "").strip()
    branch_node_id = str(selected_branch.get("targetNodeId") or "").strip()
    if branch_node_id:
        target_node = next((node for node in nodes if str(node.get("id") or "").strip() == branch_node_id and node.get("agentId")), None)
        if target_node and target_node.get("agentId") != router_agent_id:
            return {
                "targetAgentId": target_node.get("agentId", ""),
                "reason": f"命中工作流分支，转到节点 {target_node.get('title') or target_node.get('id')}",
            }
    if branch_lane_id:
        lane_nodes = [node for node in nodes if str(node.get("laneId") or "").strip() == branch_lane_id and node.get("agentId")]
        if lane_nodes and lane_nodes[0].get("agentId") != router_agent_id:
            return {
                "targetAgentId": lane_nodes[0].get("agentId", ""),
                "reason": f"命中工作流分支，进入泳道 {branch_lane_id}",
            }
    ranked_lanes = sorted(
        [(lane, lane_match_score(lane, intelligence)) for lane in lanes],
        key=lambda item: (-item[1], str(item[0].get("id") or "")),
    )
    candidate_ranks = [item for item in ranked_lanes if item[1] > 0] or ranked_lanes
    semantic_candidate_ids = [
        str(item.get("id") or "").strip()
        for item in safe_list(agents)
        if isinstance(item, dict)
        and str(item.get("id") or "").strip()
        and str(item.get("id") or "").strip() != str(router_agent_id or "").strip()
    ]
    for lane, lane_score in candidate_ranks:
        lane_nodes = [node for node in nodes if str(node.get("laneId") or "").strip() == str(lane.get("id") or "").strip() and node.get("agentId")]
        for node in lane_nodes:
            if node.get("agentId") and node.get("agentId") != router_agent_id:
                return {
                    "targetAgentId": node.get("agentId", ""),
                    "reason": f"按任务语义映射到工作流泳道 {lane.get('title') or lane.get('id')}",
                }
        if lane_score > 0 and semantic_candidate_ids:
            semantic_target_agent_id = choose_semantic_agent_candidate(
                semantic_candidate_ids,
                [lane.get("id", ""), lane.get("title", ""), lane.get("subtitle", "")],
                intelligence=intelligence,
            )
            if semantic_target_agent_id:
                return {
                    "targetAgentId": semantic_target_agent_id,
                    "reason": f"工作流泳道 {lane.get('title') or lane.get('id')} 当前未绑定可执行 Agent，按泳道语义回退到 {semantic_target_agent_id}",
                }
    return {}


def workflow_stages_from_workflow(workflow):
    lanes = workflow.get("lanes") if isinstance(workflow.get("lanes"), list) else []
    stages = []
    for index, lane in enumerate(lanes):
        if not isinstance(lane, dict):
            continue
        key = orchestration_slug(lane.get("id") or lane.get("title") or f"stage-{index + 1}") or f"stage-{index + 1}"
        stages.append(
            {
                "key": key,
                "title": str(lane.get("title") or lane.get("subtitle") or f"Stage {index + 1}").strip(),
                "status": "active" if not stages else "pending",
                "note": "",
                "updatedAt": now_iso() if not stages else "",
            }
        )
    return stages or [
        {"key": "intake", "title": "需求接入", "status": "active", "note": "", "updatedAt": now_iso()},
        {"key": "plan", "title": "方案编排", "status": "pending", "note": "", "updatedAt": ""},
        {"key": "execute", "title": "执行推进", "status": "pending", "note": "", "updatedAt": ""},
        {"key": "verify", "title": "验证验收", "status": "pending", "note": "", "updatedAt": ""},
        {"key": "release", "title": "发布收口", "status": "pending", "note": "", "updatedAt": ""},
    ]


def resolve_active_workflow_binding(openclaw_dir, agents, router_agent_id):
    workflows = store_list_orchestration_workflows(openclaw_dir)
    if not workflows:
        workflow = default_orchestration_workflow(agents, router_agent_id)
        return {
            "workflow": workflow,
            "version": None,
            "binding": {
                "workflowId": workflow.get("id", ""),
                "workflowName": workflow.get("name", ""),
                "workflowVersionId": "",
                "workflowVersionNumber": 0,
                "stageSource": "default",
            },
        }

    preferred = next((item for item in workflows if str(item.get("status") or "").lower() == "active"), workflows[0])
    versions = store_list_orchestration_workflow_versions(openclaw_dir, workflow_id=preferred.get("id"), limit=1)
    version = versions[0] if versions else None
    binding = {
        "workflowId": preferred.get("id", ""),
        "workflowName": preferred.get("name", ""),
        "workflowVersionId": version.get("id", "") if version else "",
        "workflowVersionNumber": version.get("versionNumber", 0) if version else 0,
        "stageSource": "workflow",
    }
    return {"workflow": preferred, "version": version, "binding": binding}


def enrich_workflow_binding_with_branches(workflow_resolution, title="", remark=""):
    workflow = workflow_resolution.get("workflow") if isinstance(workflow_resolution, dict) else {}
    binding = dict((workflow_resolution or {}).get("binding") or {})
    branch_preview = evaluate_workflow_branching(workflow or {}, title=title, remark=remark)
    binding["branchMatches"] = branch_preview.get("matches", [])
    binding["branchMatchCount"] = len(branch_preview.get("matches", []))
    if branch_preview.get("selected"):
        binding["selectedBranch"] = branch_preview.get("selected", {})
    return {
        **(workflow_resolution or {}),
        "binding": binding,
        "branching": branch_preview,
    }


def evaluate_routing_decision(openclaw_dir, title, remark, agents, task_index, router_agent_id, workflow_resolution=None, intelligence=None):
    paused_agents = paused_agent_ids(openclaw_dir)
    policies = [
        item
        for item in store_list_routing_policies(openclaw_dir)
        if str(item.get("status") or "").lower() == "active"
    ]
    source_text = " ".join(part for part in [str(title or "").strip(), str(remark or "").strip()] if part).strip()
    source_text_lower = source_text.lower()
    intelligence = intelligence or analyze_task_intelligence(title, remark)
    requested_priority = intelligence.get("priorityLevel") or detect_requested_priority(source_text)
    ranked = []
    trace = [
        f"任务语义判断为 {intelligence.get('categoryLabel', '通用协作')}，优先级 {requested_priority.upper()}。",
    ]
    if intelligence.get("matchedKeywords"):
        trace.append(f"命中的语义线索：{', '.join(intelligence.get('matchedKeywords', []))}。")
    for policy in policies:
        strategy_type = str(policy.get("strategyType") or "").lower()
        keyword = str(policy.get("keyword") or "").strip().lower()
        target_agent_id = str(policy.get("targetAgentId") or "").strip()
        reason = ""
        matched_keyword = ""
        if strategy_type == "keyword_department":
            if not keyword or keyword not in source_text_lower:
                continue
            matched_keyword = keyword
            reason = f"命中关键词 {keyword}"
        elif strategy_type == "priority_queue":
            if routing_priority_rank(requested_priority) < routing_priority_rank(policy.get("priorityLevel")):
                continue
            reason = f"请求优先级为 {requested_priority.upper()}，进入 {str(policy.get('priorityLevel') or 'normal').upper()} 队列"
        elif strategy_type == "load_balance":
            picked = choose_load_balanced_agent(agents, task_index, router_agent_id, excluded_agent_ids=paused_agents)
            if not picked and not target_agent_id:
                continue
            target_agent_id = target_agent_id or picked.get("id", "")
            reason = f"当前负载最低的 Agent 为 {target_agent_id}"
        else:
            continue
        if target_agent_id in paused_agents:
            trace.append(f"跳过策略 {policy.get('name') or policy.get('id') or 'unnamed'}，目标 Agent {target_agent_id} 当前已暂停接单。")
            continue
        if not target_agent_id:
            continue
        ranked.append(
            {
                "policyId": policy.get("id", ""),
                "policyName": policy.get("name", ""),
                "strategyType": strategy_type,
                "targetAgentId": target_agent_id,
                "queueName": policy.get("queueName", ""),
                "priorityLevel": policy.get("priorityLevel", "normal"),
                "matchedKeyword": matched_keyword,
                "reason": reason,
                "score": routing_priority_rank(policy.get("priorityLevel")) * 10 + (6 if matched_keyword else 0) + int(intelligence.get("confidence", 0) * 10),
            }
        )
    ranked.sort(key=lambda item: (-item["score"], item["policyName"], item["targetAgentId"]))
    if ranked:
        trace.append(f"最终命中策略 {ranked[0].get('policyName') or ranked[0].get('policyId') or 'unnamed'}。")
        return {
            **ranked[0],
            "sourceText": source_text,
            "fallback": False,
            "intelligence": intelligence,
            "trace": trace,
        }
    model_decision = intelligence.get("modelDecision", {}) if isinstance(intelligence.get("modelDecision"), dict) else {}
    if model_decision.get("used"):
        trace.append(
            f"模型判断来自 {model_decision.get('sourceLabel', model_decision.get('providerLabel', 'OpenClaw Runtime'))}，把握 {int((model_decision.get('confidence', 0) or 0) * 100)}%。"
        )
    model_target_agent_id = str(model_decision.get("targetAgentId") or "").strip()
    valid_agent_ids = {str(agent.get("id") or "").strip() for agent in agents if isinstance(agent, dict)}
    if model_target_agent_id and model_target_agent_id in valid_agent_ids and model_target_agent_id not in paused_agents:
        trace.append(model_decision.get("reason", "") or f"模型建议由 {model_target_agent_id} 接手。")
        return {
            "policyId": "",
            "policyName": "Model-assisted routing",
            "strategyType": "model_routing",
            "targetAgentId": model_target_agent_id,
            "queueName": "",
            "priorityLevel": requested_priority,
            "matchedKeyword": intelligence.get("matchedKeywords", [""])[0] if intelligence.get("matchedKeywords") else "",
            "reason": model_decision.get("reason", ""),
            "score": 9,
            "sourceText": source_text,
            "fallback": False,
            "intelligence": intelligence,
            "trace": trace,
        }
    if model_target_agent_id and model_target_agent_id in paused_agents:
        trace.append(f"模型建议 Agent {model_target_agent_id} 当前已暂停接单，改走工作流或可接单 Agent。")
    workflow_target = resolve_workflow_target_agent(workflow_resolution or {}, intelligence, router_agent_id, agents=agents)
    if workflow_target.get("targetAgentId") and workflow_target.get("targetAgentId") not in paused_agents:
        trace.append(workflow_target.get("reason", "按工作流泳道完成语义分流。"))
        return {
            "policyId": "",
            "policyName": "Workflow semantic routing",
            "strategyType": "workflow_semantic",
            "targetAgentId": workflow_target.get("targetAgentId", ""),
            "queueName": "",
            "priorityLevel": requested_priority,
            "matchedKeyword": intelligence.get("matchedKeywords", [""])[0] if intelligence.get("matchedKeywords") else "",
            "reason": workflow_target.get("reason", ""),
            "score": 8,
            "sourceText": source_text,
            "fallback": False,
            "intelligence": intelligence,
            "trace": trace,
        }
    if workflow_target.get("targetAgentId") in paused_agents:
        trace.append(f"工作流推荐 Agent {workflow_target.get('targetAgentId')} 当前已暂停接单，改走可接单 Agent。")
    picked = choose_load_balanced_agent(agents, task_index, router_agent_id, excluded_agent_ids=paused_agents)
    if picked:
        trace.append(f"没有可用显式规则，改由当前可接单 Agent {picked.get('id', '')} 接手。")
        return {
            "policyId": "",
            "policyName": "Availability fallback",
            "strategyType": "availability_fallback",
            "targetAgentId": picked.get("id", ""),
            "queueName": "",
            "priorityLevel": requested_priority,
            "matchedKeyword": "",
            "reason": f"目标 Agent 暂停接单，改由 {picked.get('id', '')} 接手。",
            "score": 2,
            "sourceText": source_text,
            "fallback": True,
            "intelligence": intelligence,
            "trace": trace,
        }
    trace.append("没有命中显式策略，回退到默认路由 Agent。")
    return {
        "policyId": "",
        "policyName": "Router fallback",
        "strategyType": "default_router",
        "targetAgentId": router_agent_id,
        "queueName": "",
        "priorityLevel": requested_priority,
        "matchedKeyword": "",
        "reason": "当前没有命中的分流规则，回退到默认路由 Agent。",
        "score": 0,
        "sourceText": source_text,
        "fallback": True,
        "intelligence": intelligence,
        "trace": trace,
    }


def normalize_task_workspace_path(task_workspace):
    resolved = Path(task_workspace).expanduser().resolve()
    if resolved.name == "data" and resolved.parent.name.startswith("workspace-"):
        return resolved.parent
    return resolved


def task_workspace_id_from_path(task_workspace):
    workspace = normalize_task_workspace_path(task_workspace)
    return workspace.name[len("workspace-"):] if workspace.name.startswith("workspace-") else workspace.name


def resolve_task_workspace_path(openclaw_dir, task_id="", config=None, router_agent_id=""):
    normalized_task_id = str(task_id or "").strip()
    config = config or load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    if normalized_task_id:
        stored = store_get_task_record(openclaw_dir, normalized_task_id)
        if stored:
            workspace_path = str(stored.get("workspacePath") or "").strip()
            if workspace_path:
                return Path(workspace_path).expanduser().resolve()
            workspace_id = str(stored.get("workspaceId") or "").strip()
            if workspace_id:
                return Path(openclaw_dir) / f"workspace-{workspace_id}"
    return router_workspace_path(openclaw_dir, router_agent_id)


def atomic_task_store_update(openclaw_dir, task_workspace, modifier, default=None):
    workspace = normalize_task_workspace_path(task_workspace)
    workspace_id = task_workspace_id_from_path(workspace)
    with task_workspace_lock(workspace):
        existing_tasks = store_list_task_records(
            openclaw_dir,
            workspace_id=workspace_id,
            workspace_path=str(workspace),
        )
        if not existing_tasks:
            payload = deepcopy(default if default is not None else [])
            existing_tasks = payload if isinstance(payload, list) else (payload.get("tasks", []) if isinstance(payload, dict) else [])
        next_tasks = modifier(deepcopy(existing_tasks))
        task_list = next_tasks if isinstance(next_tasks, list) else (next_tasks.get("tasks", []) if isinstance(next_tasks, dict) else [])
        store_replace_task_records_for_workspace(openclaw_dir, workspace_id, str(workspace), task_list)
        mark_dashboard_dirty(openclaw_dir)
        return task_list


def runtime_script_path_for_task(openclaw_dir, task_id, script_name, config=None, router_agent_id=""):
    config = config or load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    workspace = resolve_task_workspace_path(
        openclaw_dir,
        task_id=task_id,
        config=config,
        router_agent_id=router_agent_id,
    )
    if workspace:
        candidate = Path(workspace) / "scripts" / script_name
        if candidate.exists():
            return candidate
    return runtime_script_path(openclaw_dir, script_name)


def task_workspace_for_task(openclaw_dir, task_id, config=None, router_agent_id=""):
    return resolve_task_workspace_path(
        openclaw_dir,
        task_id=task_id,
        config=config,
        router_agent_id=router_agent_id,
    )


def patch_task_routing_metadata(
    openclaw_dir,
    task_id,
    decision,
    workflow_binding,
    planning_bundle,
    kanban_cfg,
    router_agent_id,
    title="",
    remark="",
    planner_title="",
    team_selection=None,
    manual_review=False,
    execution_target_agent_id="",
    execution_target_label="",
    auto_operation_profile=None,
    dispatch_validation=None,
    requested_lead_agent_id="",
):
    task_workspace = router_workspace_path(openclaw_dir, router_agent_id)
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    memory_system = current_memory_system(metadata)
    agent_labels, _ = build_label_maps(load_agents(config), kanban_cfg, router_agent_id)
    target_label = agent_labels.get(decision.get("targetAgentId"), decision.get("targetAgentId", ""))
    execution_target_agent_id = str(execution_target_agent_id or decision.get("targetAgentId") or "").strip()
    execution_target_label = str(execution_target_label or agent_labels.get(execution_target_agent_id, execution_target_agent_id)).strip()
    bootstrap_note = (
        f"任务 {task_id} 已创建，等待 {execution_target_label or execution_target_agent_id} 接收执行指令。"
        if (execution_target_label or execution_target_agent_id)
        else "任务已创建，等待执行指令。"
    )
    dispatch_validation = deepcopy(dispatch_validation) if isinstance(dispatch_validation, dict) else {}
    requested_lead_agent_id = str(requested_lead_agent_id or "").strip()

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        route_meta = {
            "policyId": decision.get("policyId", ""),
            "policyName": decision.get("policyName", ""),
            "strategyType": decision.get("strategyType", ""),
            "matchedKeyword": decision.get("matchedKeyword", ""),
            "targetAgentId": decision.get("targetAgentId", ""),
            "targetAgentLabel": target_label,
            "queueName": decision.get("queueName", ""),
            "priorityLevel": decision.get("priorityLevel", "normal"),
            "reason": decision.get("reason", ""),
            "sourceText": decision.get("sourceText", ""),
            "fallback": bool(decision.get("fallback")),
            "decidedAt": now_iso(),
            "intelligence": decision.get("intelligence", {}) if isinstance(decision.get("intelligence"), dict) else {},
            "decisionSource": ((decision.get("intelligence") or {}).get("decisionSource") if isinstance(decision.get("intelligence"), dict) else "") or "heuristic",
            "modelDecision": ((decision.get("intelligence") or {}).get("modelDecision") if isinstance(decision.get("intelligence"), dict) and isinstance((decision.get("intelligence") or {}).get("modelDecision"), dict) else {}),
            "trace": decision.get("trace", []) if isinstance(decision.get("trace"), list) else [],
            "manualReview": bool(manual_review or (decision.get("intelligence") or {}).get("manualReview")),
            "confidence": (decision.get("intelligence") or {}).get("confidence", 0),
            "category": (decision.get("intelligence") or {}).get("category", ""),
            "categoryLabel": (decision.get("intelligence") or {}).get("categoryLabel", ""),
            "riskLevel": (decision.get("intelligence") or {}).get("riskLevel", "good"),
            "suggestedWorkflowTemplate": (decision.get("intelligence") or {}).get("suggestedWorkflowTemplate", ""),
            "executionBootstrap": {
                "status": "scheduled",
                "at": now_iso(),
                "agentId": execution_target_agent_id,
                "agentLabel": execution_target_label,
                "note": bootstrap_note,
                "attempts": 0,
            },
        }
        if dispatch_validation:
            route_meta["dispatchValidation"] = deepcopy(dispatch_validation)
        if requested_lead_agent_id:
            route_meta["requestedLeadAgentId"] = requested_lead_agent_id
        compact_operation_profile = compact_auto_operation_profile(auto_operation_profile)
        if compact_operation_profile:
            route_meta["autoOperationProfile"] = compact_operation_profile
        route_meta["taskLongTermMemory"] = seed_task_long_term_memory_payload(
            title=title,
            remark=remark,
            memory_system=memory_system,
            task_type=str(compact_operation_profile.get("taskType") or "").strip(),
        )
        if isinstance(team_selection, dict) and team_selection:
            route_meta["teamSelection"] = deepcopy(team_selection)
        found = False
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            found = True
            previous_org = str(task.get("org") or "").strip()
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            meta["routeDecision"] = route_meta
            if workflow_binding:
                meta["workflowBinding"] = workflow_binding
            if planning_bundle:
                meta["planningBundle"] = planning_bundle
            task["meta"] = meta
            task["targetAgentId"] = decision.get("targetAgentId", "")
            task["targetAgentLabel"] = target_label
            task["workflowBinding"] = workflow_binding or {}
            task["planningBundle"] = planning_bundle or {}
            if target_label:
                task["org"] = target_label
            task_status_text = str(dispatch_validation.get("summary") or route_meta["reason"] or "").strip()
            if task_status_text:
                task["currentUpdate"] = task_status_text
                task["now"] = task_status_text
            task["createdAt"] = str(task.get("createdAt") or task.get("updatedAt") or now_iso()).strip()
            task.setdefault("flow_log", [])
            if previous_org and target_label and previous_org != target_label:
                task["flow_log"].append(
                    {
                        "at": now_iso(),
                        "from": previous_org,
                        "to": target_label,
                        "remark": route_meta["reason"] or "按分流规则自动分派。",
                    }
                )
            task["updatedAt"] = now_iso()
        if not found:
            initial_org = target_label or planner_title or "Planner"
            flow_log = []
            if planner_title and target_label and planner_title != target_label:
                flow_log.append(
                    {
                        "at": now_iso(),
                        "from": planner_title,
                        "to": target_label,
                        "remark": route_meta["reason"] or "按分流规则自动分派。",
                    }
                )
            tasks.append(
                {
                    "id": task_id,
                    "title": title or task_id,
                    "state": "Todo",
                    "org": initial_org,
                    "owner": planner_title or "Planner",
                    "remark": remark,
                    "meta": {
                        "routeDecision": route_meta,
                        **({"workflowBinding": workflow_binding} if workflow_binding else {}),
                        **({"planningBundle": planning_bundle} if planning_bundle else {}),
                    },
                    "targetAgentId": decision.get("targetAgentId", ""),
                    "targetAgentLabel": target_label,
                    "workflowBinding": workflow_binding or {},
                    "planningBundle": planning_bundle or {},
                    "currentUpdate": str(dispatch_validation.get("summary") or route_meta["reason"] or "").strip(),
                    "now": str(dispatch_validation.get("summary") or route_meta["reason"] or "").strip(),
                    "createdAt": now_iso(),
                    "updatedAt": now_iso(),
                    "flow_log": flow_log,
                    "progress_log": [],
                    "todos": [],
                }
            )
        return tasks

    atomic_task_store_update(openclaw_dir, router_workspace_path(openclaw_dir, router_agent_id), modifier, [])
    records, events = build_memory_projection_payloads(
        task_id=task_id,
        task_title=title,
        task_memory=seed_task_long_term_memory_payload(
            title=title,
            remark=remark,
            memory_system=memory_system,
            task_type=str((compact_auto_operation_profile(auto_operation_profile) or {}).get("taskType") or "").strip(),
        ),
    )
    project_memory_records_async(openclaw_dir, records, events)


def persist_task_planning_bundle(openclaw_dir, task_id, router_agent_id, planning_bundle):
    if not planning_bundle:
        return False
    task_workspace = router_workspace_path(openclaw_dir, router_agent_id)
    changed = {"value": False}

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            if meta.get("planningBundle") == planning_bundle and task.get("planningBundle") == planning_bundle:
                continue
            meta["planningBundle"] = planning_bundle
            task["meta"] = meta
            task["planningBundle"] = planning_bundle
            task["updatedAt"] = now_iso()
            changed["value"] = True
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    return changed["value"]


def latest_routing_decision_for_task(openclaw_dir, task_id):
    task_id = str(task_id or "").strip()
    if not task_id:
        return None
    for item in store_list_routing_decisions(openclaw_dir, limit=512):
        if item.get("taskId") == task_id:
            return item
    return None


def patch_task_route_outcome_metadata(openclaw_dir, task_id, outcome, router_agent_id=""):
    if not outcome:
        return False
    config = load_config(openclaw_dir)
    if not router_agent_id:
        router_agent_id = get_router_agent_id(config)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)
    changed = {"value": False}

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            route_meta["outcome"] = outcome
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            changed["value"] = True
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    return changed["value"]


def detect_all_hands_task_tokens(text):
    normalized_text = str(text or "").strip().lower()
    if not normalized_text:
        return []
    hits = []
    for token in ALL_HANDS_TASK_TOKENS:
        normalized_token = str(token or "").strip().lower()
        if normalized_token and normalized_token in normalized_text and normalized_token not in hits:
            hits.append(normalized_token)
    return hits[:6]


def build_routing_outcome_payload(task, decision, route_meta, status, note, current_agent, current_agent_label):
    now_text = now_iso()
    decided_at = parse_iso(route_meta.get("decidedAt") or decision.get("decidedAt"))
    updated_at = parse_iso(task.get("updatedAt")) or now_utc()
    elapsed_minutes = 0
    if decided_at and updated_at:
        elapsed_minutes = round(max((updated_at - decided_at).total_seconds(), 0) / 60, 1)
    confidence = route_meta.get("confidence")
    if confidence in {None, ""}:
        confidence = (route_meta.get("intelligence") or {}).get("confidence", 0)
    target_agent_id = route_meta.get("targetAgentId") or decision.get("targetAgentId", "")
    target_agent_label = route_meta.get("targetAgentLabel") or (decision.get("meta") or {}).get("targetAgentLabel", "")
    fallback = bool(route_meta.get("fallback") or (decision.get("meta") or {}).get("fallback"))
    manual_review = bool(route_meta.get("manualReview") or (route_meta.get("intelligence") or {}).get("manualReview"))
    return {
        "status": str(status or "active"),
        "note": str(note or "").strip(),
        "taskState": str(task.get("state") or ""),
        "updatedAt": now_text,
        "elapsedMinutes": elapsed_minutes,
        "currentAgentId": current_agent,
        "currentAgentLabel": current_agent_label,
        "targetAgentId": target_agent_id,
        "targetAgentLabel": target_agent_label,
        "fallback": fallback,
        "manualReview": manual_review,
        "confidence": float(confidence or 0),
        "completed": str(status or "") == "completed",
        "blocked": str(status or "") == "blocked",
        "completionMinutes": elapsed_minutes if str(status or "") == "completed" else 0,
        "blockedMinutes": elapsed_minutes if str(status or "") == "blocked" else 0,
    }


def record_routing_outcome(openclaw_dir, task_id, status, note=""):
    task_id = str(task_id or "").strip()
    if not task_id:
        return None
    config = load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    agents = load_agents(config)
    agent_labels, _label_to_ids = build_label_maps(agents, kanban_cfg, router_agent_id)
    tasks = merge_tasks(openclaw_dir, config)
    task = next((item for item in tasks if item.get("id") == task_id), None)
    if not isinstance(task, dict):
        return None
    decision = latest_routing_decision_for_task(openclaw_dir, task_id)
    if not isinstance(decision, dict):
        return None
    meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
    route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
    current_agent = current_agent_for_task(task, kanban_cfg, router_agent_id)
    current_agent_label = agent_labels.get(current_agent, current_agent or task.get("org", "?"))
    outcome = build_routing_outcome_payload(task, decision, route_meta, status, note, current_agent, current_agent_label)
    decision_meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
    decision_meta["outcome"] = outcome
    store_save_routing_decision(
        openclaw_dir,
        {
            "id": decision.get("id", ""),
            "taskId": decision.get("taskId", task_id),
            "taskTitle": decision.get("taskTitle", task.get("title", task_id)),
            "policyId": decision.get("policyId", ""),
            "policyName": decision.get("policyName", ""),
            "workflowId": decision.get("workflowId", ""),
            "workflowVersionId": decision.get("workflowVersionId", ""),
            "strategyType": decision.get("strategyType", ""),
            "matchedKeyword": decision.get("matchedKeyword", ""),
            "queueName": decision.get("queueName", ""),
            "priorityLevel": decision.get("priorityLevel", "normal"),
            "targetAgentId": decision.get("targetAgentId", ""),
            "sourceText": decision.get("sourceText", ""),
            "decidedAt": decision.get("decidedAt", now_iso()),
            "meta": decision_meta,
        },
    )
    patch_task_route_outcome_metadata(openclaw_dir, task_id, outcome, router_agent_id=router_agent_id)
    return outcome


def backfill_planning_bundles(openclaw_dir, config, tasks):
    project_dir = resolve_planning_project_dir(openclaw_dir, config=config)
    if not project_dir:
        return {"tasks": 0, "runs": 0}
    router_agent_id = get_router_agent_id(config)
    updated_tasks = 0
    for task in tasks:
        if not isinstance(task, dict) or not task.get("id") or planning_binding_from_payload(task):
            continue
        workflow_binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else (
            (task.get("meta") or {}).get("workflowBinding", {}) if isinstance(task.get("meta"), dict) else {}
        )
        decision = (task.get("meta") or {}).get("routeDecision", {}) if isinstance(task.get("meta"), dict) else {}
        bundle = ensure_planning_bundle(
            openclaw_dir,
            project_dir,
            "task",
            task.get("id"),
            title=task.get("title") or task.get("id"),
            goal=task.get("remark") or task.get("title") or task.get("id"),
            meta={
                "taskId": task.get("id"),
                "targetAgentId": task.get("targetAgentId") or decision.get("targetAgentId", ""),
                "workflowBinding": workflow_binding,
            },
        )
        task["planningBundle"] = bundle
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
        meta["planningBundle"] = bundle
        task["meta"] = meta
        if persist_task_planning_bundle(openclaw_dir, task.get("id"), router_agent_id, bundle):
            updated_tasks += 1

    updated_runs = 0
    for run in store_list_management_runs(openclaw_dir, limit=128):
        if planning_binding_from_payload(run):
            continue
        workflow_binding = run.get("workflowBinding") if isinstance(run.get("workflowBinding"), dict) else {}
        bundle = ensure_planning_bundle(
            openclaw_dir,
            project_dir,
            "run",
            run.get("id"),
            title=run.get("title") or run.get("id"),
            goal=run.get("goal") or run.get("title") or run.get("id"),
            meta={
                "runId": run.get("id"),
                "linkedTaskId": run.get("linkedTaskId", ""),
                "workflowBinding": workflow_binding,
            },
        )
        store_save_management_run_planning_binding(openclaw_dir, run.get("id"), bundle)
        updated_runs += 1

    return {"tasks": updated_tasks, "runs": updated_runs}


def backfill_task_intelligence(openclaw_dir, config, tasks):
    agents = load_agents(config)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    agent_labels, _ = build_label_maps(agents, kanban_cfg, router_agent_id)
    workflows = {
        str(item.get("id") or "").strip(): item
        for item in store_list_orchestration_workflows(openclaw_dir)
        if str(item.get("id") or "").strip()
    }
    existing_decision_task_ids = {
        item.get("taskId")
        for item in store_list_routing_decisions(openclaw_dir, limit=512)
        if item.get("taskId")
    }
    updated = 0
    created_decisions = 0
    task_workspace = router_workspace_path(openclaw_dir, router_agent_id)

    def build_resolution(task, intelligence):
        workflow_binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else {}
        meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
        if not workflow_binding and isinstance(meta.get("workflowBinding"), dict):
            workflow_binding = meta.get("workflowBinding", {})
        workflow_id = str(workflow_binding.get("workflowId") or "").strip()
        workflow = workflows.get(workflow_id)
        if workflow:
            return enrich_workflow_binding_with_branches(
                {"workflow": workflow, "version": None, "binding": dict(workflow_binding)},
                title=task.get("title", ""),
                remark=task.get("remark", ""),
            )
        return enrich_workflow_binding_with_branches(
            choose_task_workflow_resolution(
                openclaw_dir,
                agents,
                router_agent_id,
                title=task.get("title", ""),
                remark=task.get("remark", ""),
                intelligence=intelligence,
            ),
            title=task.get("title", ""),
            remark=task.get("remark", ""),
        )

    def modifier(data):
        nonlocal updated, created_decisions
        items = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in items:
            if not isinstance(task, dict) or not task.get("id") or not task.get("title"):
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            has_intelligence = isinstance(route_meta.get("intelligence"), dict) and bool(route_meta.get("intelligence", {}).get("category"))
            has_trace = bool(route_meta.get("trace"))
            if route_meta and has_intelligence and has_trace:
                continue
            intelligence = analyze_task_intelligence(
                task.get("title", ""),
                task.get("remark", ""),
                openclaw_dir=openclaw_dir,
                config=config,
                agents=agents,
                router_agent_id=router_agent_id,
                task_index=tasks,
                allow_model=False,
            )
            workflow_resolution = build_resolution(task, intelligence)
            decision = evaluate_routing_decision(
                openclaw_dir,
                task.get("title", ""),
                task.get("remark", ""),
                agents=agents,
                task_index=tasks,
                router_agent_id=router_agent_id,
                workflow_resolution=workflow_resolution,
                intelligence=intelligence,
            )
            preserved_agent_id = current_agent_for_task(task, kanban_cfg, router_agent_id) or decision.get("targetAgentId", "")
            preserved_label = agent_labels.get(preserved_agent_id, preserved_agent_id)
            decision["targetAgentId"] = preserved_agent_id or decision.get("targetAgentId", "")
            decision["targetAgentLabel"] = preserved_label
            decision["reason"] = route_meta.get("reason") or (
                f"历史任务保留当前负责人 {preserved_label or preserved_agent_id or router_agent_id}，补充智能判断。"
            )
            route_meta = {
                **route_meta,
                "policyId": route_meta.get("policyId", decision.get("policyId", "")),
                "policyName": route_meta.get("policyName", decision.get("policyName", "") or "Historical backfill"),
                "strategyType": route_meta.get("strategyType", decision.get("strategyType", "historical_backfill")),
                "matchedKeyword": decision.get("matchedKeyword", route_meta.get("matchedKeyword", "")),
                "targetAgentId": preserved_agent_id,
                "targetAgentLabel": preserved_label,
                "queueName": route_meta.get("queueName", decision.get("queueName", "")),
                "priorityLevel": route_meta.get("priorityLevel", decision.get("priorityLevel", intelligence.get("priorityLevel", "normal"))),
                "reason": decision.get("reason", ""),
                "sourceText": route_meta.get("sourceText", decision.get("sourceText", intelligence.get("sourceText", ""))),
                "fallback": bool(route_meta.get("fallback", decision.get("fallback"))),
                "decidedAt": route_meta.get("decidedAt") or task.get("updatedAt") or now_iso(),
                "intelligence": intelligence,
                "decisionSource": intelligence.get("decisionSource", "heuristic"),
                "modelDecision": intelligence.get("modelDecision", {}) if isinstance(intelligence.get("modelDecision"), dict) else {},
                "trace": decision.get("trace", []),
                "manualReview": bool(intelligence.get("manualReview")),
                "confidence": intelligence.get("confidence", 0),
                "category": intelligence.get("category", ""),
                "categoryLabel": intelligence.get("categoryLabel", ""),
                "riskLevel": intelligence.get("riskLevel", "good"),
                "suggestedWorkflowTemplate": intelligence.get("suggestedWorkflowTemplate", ""),
            }
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["targetAgentId"] = preserved_agent_id
            task["targetAgentLabel"] = preserved_label
            updated += 1
            task_id = task.get("id")
            if task_id not in existing_decision_task_ids:
                store_save_routing_decision(
                    openclaw_dir,
                    {
                        "taskId": task_id,
                        "taskTitle": task.get("title", task_id),
                        "policyId": route_meta.get("policyId", ""),
                        "policyName": route_meta.get("policyName", "") or "Historical backfill",
                        "workflowId": (workflow_resolution.get("binding") or {}).get("workflowId", ""),
                        "workflowVersionId": (workflow_resolution.get("binding") or {}).get("workflowVersionId", ""),
                        "strategyType": route_meta.get("strategyType", "historical_backfill"),
                        "matchedKeyword": route_meta.get("matchedKeyword", ""),
                        "queueName": route_meta.get("queueName", ""),
                        "priorityLevel": route_meta.get("priorityLevel", "normal"),
                        "targetAgentId": preserved_agent_id or router_agent_id,
                        "sourceText": route_meta.get("sourceText", ""),
                        "decidedAt": route_meta.get("decidedAt", now_iso()),
                        "meta": {
                            "reason": route_meta.get("reason", ""),
                            "fallback": bool(route_meta.get("fallback")),
                            "targetAgentLabel": preserved_label,
                            "intelligence": intelligence,
                            "decisionSource": intelligence.get("decisionSource", "heuristic"),
                            "modelDecision": intelligence.get("modelDecision", {}) if isinstance(intelligence.get("modelDecision"), dict) else {},
                            "trace": route_meta.get("trace", []),
                            "manualReview": bool(intelligence.get("manualReview")),
                            "confidence": intelligence.get("confidence", 0),
                        },
                    },
                )
                existing_decision_task_ids.add(task_id)
                created_decisions += 1
        return items

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    return {"tasks": updated, "decisions": created_decisions}


def build_task_intelligence_summary(task_index):
    active_tasks = [task for task in task_index if task.get("active")]
    manual_review_tasks = []
    low_confidence_tasks = []
    risky_fallback_tasks = []
    category_counter = Counter()
    for task in active_tasks:
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        intelligence = route.get("intelligence") if isinstance(route.get("intelligence"), dict) else {}
        category = route.get("category") or intelligence.get("category") or "general"
        category_label = route.get("categoryLabel") or intelligence.get("categoryLabel") or "通用协作"
        category_counter[(category, category_label)] += 1
        confidence = float(route.get("confidence") or intelligence.get("confidence") or 0)
        risk_level = str(route.get("riskLevel") or intelligence.get("riskLevel") or "good")
        manual_review = bool(route.get("manualReview") or intelligence.get("manualReview"))
        if manual_review:
            manual_review_tasks.append(task)
        if confidence and confidence < 0.65:
            low_confidence_tasks.append(task)
        if risk_level == "high" and route.get("fallback"):
            risky_fallback_tasks.append(task)
    return {
        "manualReviewCount": len(manual_review_tasks),
        "lowConfidenceCount": len(low_confidence_tasks),
        "riskyFallbackCount": len(risky_fallback_tasks),
        "topCategories": [
            {"category": category, "label": label, "count": count}
            for (category, label), count in category_counter.most_common(6)
        ],
        "manualReviewTasks": [compact_task_reference(task, include_route=True) for task in manual_review_tasks[:6]],
        "lowConfidenceTasks": [compact_task_reference(task, include_route=True) for task in low_confidence_tasks[:6]],
        "riskyFallbackTasks": [compact_task_reference(task, include_route=True) for task in risky_fallback_tasks[:6]],
    }


def build_routing_effectiveness_summary(task_index, routing_decisions):
    decision_by_task = {}
    for item in safe_list(routing_decisions):
        task_id = str(item.get("taskId") or "").strip()
        if task_id and task_id not in decision_by_task:
            decision_by_task[task_id] = item

    evaluated = 0
    completed = 0
    blocked = 0
    active = 0
    confidence_total = 0.0
    confidence_count = 0
    completed_confidence_total = 0.0
    completed_confidence_count = 0
    blocked_confidence_total = 0.0
    blocked_confidence_count = 0
    policy_rows = {}
    low_confidence_tasks = []
    risky_fallback_tasks = []
    manual_review_tasks = []

    for task in safe_list(task_index):
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            continue
        decision = decision_by_task.get(task_id)
        if not decision:
            continue
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        decision_meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
        outcome = decision_meta.get("outcome") if isinstance(decision_meta.get("outcome"), dict) else {}
        state = str(task.get("state") or "").lower()
        status = str(outcome.get("status") or ("blocked" if task.get("blocked") else ("completed" if state in TERMINAL_STATES else "active"))).lower()
        confidence = float(route.get("confidence") or decision_meta.get("confidence") or (route.get("intelligence") or {}).get("confidence") or 0)
        fallback = bool(route.get("fallback") or decision_meta.get("fallback"))
        manual_review = bool(route.get("manualReview") or decision_meta.get("manualReview") or (route.get("intelligence") or {}).get("manualReview"))
        policy_name = decision.get("policyName") or decision.get("strategyType") or "Router fallback"
        policy_row = policy_rows.setdefault(
            policy_name,
            {"policyName": policy_name, "evaluatedCount": 0, "completedCount": 0, "blockedCount": 0, "activeCount": 0},
        )
        if status in {"completed", "blocked"}:
            evaluated += 1
            policy_row["evaluatedCount"] += 1
        else:
            active += 1
            policy_row["activeCount"] += 1
        if status == "completed":
            completed += 1
            policy_row["completedCount"] += 1
        elif status == "blocked":
            blocked += 1
            policy_row["blockedCount"] += 1
        if confidence > 0:
            confidence_total += confidence
            confidence_count += 1
            if status == "completed":
                completed_confidence_total += confidence
                completed_confidence_count += 1
            elif status == "blocked":
                blocked_confidence_total += confidence
                blocked_confidence_count += 1
        if manual_review:
            manual_review_tasks.append(task)
        if confidence and confidence < 0.6 and status != "completed":
            low_confidence_tasks.append(task)
        if fallback and status != "completed" and str(route.get("riskLevel") or (route.get("intelligence") or {}).get("riskLevel") or "good") in {"watch", "high"}:
            risky_fallback_tasks.append(task)

    watch_policies = []
    for row in policy_rows.values():
        evaluated_count = row["evaluatedCount"]
        row["completionRate"] = int(round((row["completedCount"] / max(evaluated_count, 1)) * 100)) if evaluated_count else 0
        row["blockRate"] = int(round((row["blockedCount"] / max(evaluated_count, 1)) * 100)) if evaluated_count else 0
        if evaluated_count >= 1 and row["blockRate"] >= 30:
            watch_policies.append(row)
    watch_policies.sort(key=lambda item: (-item["blockRate"], -item["evaluatedCount"], item["policyName"]))

    completion_rate = int(round((completed / max(evaluated, 1)) * 100)) if evaluated else 0
    block_rate = int(round((blocked / max(evaluated, 1)) * 100)) if evaluated else 0
    suggestions = []

    if risky_fallback_tasks:
        sample = risky_fallback_tasks[0]
        sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
        sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
        keyword = (
            safe_list(sample_intelligence.get("matchedKeywords"))[:1]
            or safe_list(sample.get("title", "").split())[:1]
        )
        suggestions.append(
            {
                "title": "先补默认回退场景的分流规则",
                "detail": f"{len(risky_fallback_tasks)} 条高风险任务仍在走默认路由，优先给这类任务补关键词或专属工作流。",
                "severity": "warning",
                "action": {
                    "type": "create_policy",
                    "label": "生成分流规则",
                    "payload": {
                        "name": f"补充 {sample.get('id', '任务')} 分流",
                        "strategyType": "keyword_department",
                        "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                        "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                        "priorityLevel": sample_route.get("priorityLevel", "high"),
                        "queueName": "",
                        "status": "active",
                    },
                },
            }
        )
    if low_confidence_tasks:
        sample = low_confidence_tasks[0]
        suggestions.append(
            {
                "title": "低把握任务建议加人工复核",
                "detail": f"{len(low_confidence_tasks)} 条任务判断把握偏低，适合补人工审批或更明确的分流条件。",
                "severity": "info",
                "action": build_management_approval_action(sample, "低把握任务较多，建议前置人工复核节点。"),
            }
        )
    if watch_policies:
        watch = watch_policies[0]
        suggestions.append(
            {
                "title": f"检查策略 {watch['policyName']}",
                "detail": f"这条策略当前阻塞率 {watch['blockRate']}%，建议复盘目标 Agent 和命中条件。",
                "severity": "warning",
                "action": {
                    "type": "open_orchestration",
                    "label": "前往流程编排",
                    "path": "/orchestration",
                },
            }
        )
    if manual_review_tasks and completion_rate < 75:
        sample = manual_review_tasks[0]
        suggestions.append(
            {
                "title": "把人工复核前置成流程节点",
                "detail": "当前人工复核任务较多，且收口率还不高，建议在工作流里前置审批或验证节点。",
                "severity": "info",
                "action": build_management_approval_action(sample, "人工复核任务较多，建议把审批前置到流程里。"),
            }
        )

    return {
        "evaluatedCount": evaluated,
        "completedCount": completed,
        "blockedCount": blocked,
        "activeCount": active,
        "completionRate": completion_rate,
        "blockRate": block_rate,
        "avgConfidence": round(confidence_total / confidence_count, 2) if confidence_count else 0,
        "avgCompletedConfidence": round(completed_confidence_total / completed_confidence_count, 2) if completed_confidence_count else 0,
        "avgBlockedConfidence": round(blocked_confidence_total / blocked_confidence_count, 2) if blocked_confidence_count else 0,
        "watchPolicies": watch_policies[:6],
        "suggestions": suggestions[:4],
    }


def enrich_routing_recommendation_actions(summary):
    if not isinstance(summary, dict):
        return summary
    suggestions = []
    for item in safe_list(summary.get("suggestions")):
        suggestion = dict(item)
        suggestions.append(suggestion)
    summary["suggestions"] = suggestions
    return summary


def build_orchestration_workflow_review(workflows, task_index, replays):
    replay_map = {
        str(item.get("taskId") or "").strip(): item
        for item in safe_list(replays)
        if str(item.get("taskId") or "").strip()
    }
    rows = []
    for workflow in safe_list(workflows):
        workflow_id = str(workflow.get("id") or "").strip()
        if not workflow_id:
            continue
        related_tasks = []
        for task in safe_list(task_index):
            binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else {}
            if str(binding.get("workflowId") or "").strip() == workflow_id:
                related_tasks.append(task)
        if not related_tasks:
            continue
        completed = sum(1 for task in related_tasks if str(task.get("state") or "").lower() in TERMINAL_STATES)
        blocked = sum(1 for task in related_tasks if task.get("blocked"))
        active = max(len(related_tasks) - completed - blocked, 0)
        context_loss = 0
        duration_total = 0.0
        duration_count = 0
        for task in related_tasks:
            replay = replay_map.get(str(task.get("id") or "").strip())
            if replay:
                context_loss += int(replay.get("contextLossCount") or 0)
                if replay.get("durationMinutes"):
                    duration_total += float(replay.get("durationMinutes") or 0)
                    duration_count += 1
        evaluated = completed + blocked
        completion_rate = int(round((completed / max(evaluated, 1)) * 100)) if evaluated else 0
        block_rate = int(round((blocked / max(evaluated, 1)) * 100)) if evaluated else 0
        verdict = "stable"
        if block_rate >= 30 or context_loss >= 3:
            verdict = "watch"
        if block_rate >= 50:
            verdict = "critical"
        rows.append(
            {
                "workflowId": workflow_id,
                "workflowName": workflow.get("name", ""),
                "taskCount": len(related_tasks),
                "completedCount": completed,
                "blockedCount": blocked,
                "activeCount": active,
                "completionRate": completion_rate,
                "blockRate": block_rate,
                "avgDurationMinutes": round(duration_total / max(duration_count, 1), 1) if duration_count else 0,
                "contextLossCount": context_loss,
                "verdict": verdict,
            }
        )
    rows.sort(key=lambda item: (-item["blockRate"], -item["taskCount"], item["workflowName"]))
    return rows[:6]


def workflow_has_checkpoint_node(workflow):
    nodes = safe_list((workflow or {}).get("nodes"))
    return any(
        str(node.get("type") or "agent").strip() == "approval"
        or any(keyword in str(node.get("title") or "").lower() for keyword in ("verify", "review", "验收", "复核", "验证"))
        for node in nodes
    )


def find_workflow_weak_handoff_node(workflow):
    nodes = safe_list((workflow or {}).get("nodes"))
    return next(
        (
            node
            for node in nodes
            if str(node.get("type") or "agent").strip() == "agent"
            and len(str(node.get("handoffNote") or "").strip()) < 48
        ),
        None,
    )


def build_task_replay(task, label_to_agent_ids, now):
    replay = []

    for entry in task.get("flow_log", []):
        at = parse_iso(entry.get("at"))
        from_label = entry.get("from") or "?"
        to_label = entry.get("to") or "?"
        replay.append(
            {
                "kind": "handoff",
                "at": entry.get("at", ""),
                "atAgo": format_age(at, now) if at else "未知时间",
                "actorLabel": from_label,
                "actorId": sorted(label_to_agent_ids.get(from_label, []))[0] if label_to_agent_ids.get(from_label) else "",
                "targetLabel": to_label,
                "targetId": sorted(label_to_agent_ids.get(to_label, []))[0] if label_to_agent_ids.get(to_label) else "",
                "headline": f"{from_label} -> {to_label}",
                "detail": entry.get("remark", ""),
            }
        )

    for entry in task.get("progress_log", []):
        at = parse_iso(entry.get("at"))
        actor_label = entry.get("agentLabel") or entry.get("agent") or "Agent"
        replay.append(
            {
                "kind": "progress",
                "at": entry.get("at", ""),
                "atAgo": format_age(at, now) if at else "未知时间",
                "actorLabel": actor_label,
                "actorId": entry.get("agent", ""),
                "targetLabel": "",
                "targetId": "",
                "headline": f"{actor_label} 正在推进",
                "detail": entry.get("text", ""),
            }
        )

    replay.sort(key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc))
    return replay


def allowed_deliverable_roots(openclaw_dir, config):
    roots = []
    metadata = load_project_metadata(openclaw_dir, config=config)
    configured_project_dir = str(metadata.get("projectDir", "")).strip()
    if configured_project_dir:
        candidate = Path(configured_project_dir).expanduser().resolve()
        if candidate.exists():
            roots.append(candidate)
    project_dir = resolve_project_dir(openclaw_dir, config=config)
    if project_dir:
        roots.append(Path(project_dir).expanduser().resolve())
    roots.append(Path(openclaw_dir).expanduser().resolve())
    for agent in load_agents(config):
        workspace = Path(agent.get("workspace") or (Path(openclaw_dir) / f"workspace-{agent.get('id', '')}")).expanduser().resolve()
        roots.append(workspace)
    unique = []
    seen = set()
    for root in roots:
        key = str(root)
        if key in seen or not root.exists():
            continue
        seen.add(key)
        unique.append(root)
    return unique


def resolve_deliverable_output_target(openclaw_dir, data, deliverable_id):
    normalized_id = str(deliverable_id or "").strip()
    if not normalized_id:
        raise RuntimeError("交付物 ID 不能为空。")
    deliverable = next((item for item in safe_list((data or {}).get("deliverables")) if item.get("id") == normalized_id), None)
    if not deliverable:
        raise RuntimeError(f"未找到交付物：{normalized_id}")
    output = str(deliverable.get("output") or deliverable.get("path") or "").strip()
    if not output:
        raise RuntimeError("当前交付物没有可下载的产物路径。")
    config = load_config(openclaw_dir)
    allowed_roots = allowed_deliverable_roots(openclaw_dir, config)
    raw_candidate = Path(output).expanduser()
    candidates = []
    if raw_candidate.is_absolute():
        candidates.append(raw_candidate.resolve())
    else:
        for root in allowed_roots:
            candidates.append((root / output).resolve())
    for candidate in candidates:
        if not candidate.exists():
            continue
        for root in allowed_roots:
            if candidate == root or root in candidate.parents:
                return {"deliverable": deliverable, "path": candidate, "root": root}
    raise RuntimeError(f"交付物路径不存在或超出允许范围：{output}")


def build_deliverable_zip_bytes(openclaw_dir, data, deliverable_id):
    target = resolve_deliverable_output_target(openclaw_dir, data, deliverable_id)
    deliverable = target["deliverable"]
    candidate = target["path"]
    root = target["root"]
    if candidate.is_file() and candidate.suffix.lower() == ".zip":
        return {
            "body": candidate.read_bytes(),
            "filename": candidate.name,
            "deliverable": deliverable,
            "path": candidate,
        }
    archive_root = safe_download_name(deliverable.get("id") or deliverable.get("title") or candidate.stem, fallback="deliverable")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        if candidate.is_file():
            try:
                relative = candidate.relative_to(root)
                arcname = str(relative).replace(os.sep, "/")
            except ValueError:
                arcname = candidate.name
            archive.write(candidate, arcname or candidate.name)
        else:
            for child in sorted(candidate.rglob("*")):
                if not child.is_file():
                    continue
                try:
                    relative = child.relative_to(candidate)
                    arcname = f"{archive_root}/{str(relative).replace(os.sep, '/')}"
                except ValueError:
                    arcname = f"{archive_root}/{child.name}"
                archive.write(child, arcname)
    filename = f"{safe_download_name(deliverable.get('id') or deliverable.get('title') or candidate.stem)}.zip"
    return {"body": buffer.getvalue(), "filename": filename, "deliverable": deliverable, "path": candidate}


def normalize_task_risk_for_management_run(risk_level):
    normalized = str(risk_level or "").strip().lower()
    if normalized == "high":
        return "high"
    if normalized == "good":
        return "low"
    return "medium"


def latest_management_run_for_task(openclaw_dir, task_id, limit=96):
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return {}
    for run in store_list_management_runs(openclaw_dir, limit=limit):
        if str(run.get("linkedTaskId") or "").strip() == normalized_task_id:
            return run
    return {}


def normalize_task_dedupe_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).strip().lower()


def task_dedupe_signature(title="", remark=""):
    normalized_title = normalize_task_dedupe_text(title)
    normalized_remark = normalize_task_dedupe_text(remark)
    return " | ".join(part for part in (normalized_title, normalized_remark) if part)


def task_id_sequence(task_id):
    normalized = str(task_id or "").strip()
    match = re.search(r"(\d+)$", normalized)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return math.inf
    return math.inf


def duplicate_task_sort_key(task):
    if not isinstance(task, dict):
        return (math.inf, math.inf, "")
    duplicate_of_task_id = str(
        task.get("duplicateOfTaskId")
        or (((task.get("meta") or {}).get("duplicateOfTaskId")) if isinstance(task.get("meta"), dict) else "")
        or ""
    ).strip()
    preferred_dt = (
        parse_iso(task.get("createdAt"))
        or parse_iso(task.get("updatedAt"))
        or datetime.max.replace(tzinfo=timezone.utc)
    )
    task_id = str(task.get("id") or "").strip()
    return (
        1 if duplicate_of_task_id else 0,
        preferred_dt,
        task_id_sequence(task_id),
        task_id,
    )


def task_duplicate_of_task_id(task):
    if not isinstance(task, dict):
        return ""
    return str(
        task.get("duplicateOfTaskId")
        or (((task.get("meta") or {}).get("duplicateOfTaskId")) if isinstance(task.get("meta"), dict) else "")
        or ""
    ).strip()


def is_merged_duplicate_task(task):
    state = str((task or {}).get("state") or (task or {}).get("status") or "").strip().lower()
    return bool(task_duplicate_of_task_id(task) and state in TERMINAL_STATES)


def task_effective_goal_text(task):
    if not isinstance(task, dict):
        return ""
    direct_remark = str(task.get("remark") or "").strip()
    if direct_remark:
        return direct_remark
    planning_binding = planning_binding_from_payload(task)
    planned_goal = str(planning_binding.get("goal") or "").strip()
    if planned_goal and normalize_task_dedupe_text(planned_goal) != normalize_task_dedupe_text(task.get("title")):
        return planned_goal
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if not route_meta and isinstance(task.get("meta"), dict):
        meta_route = (task.get("meta") or {}).get("routeDecision")
        if isinstance(meta_route, dict):
            route_meta = meta_route
    source_text = str(route_meta.get("sourceText") or "").strip()
    title_text = str(task.get("title") or "").strip()
    if source_text:
        normalized_source = normalize_task_dedupe_text(source_text)
        normalized_title = normalize_task_dedupe_text(title_text)
        if normalized_title and normalized_source.startswith(normalized_title):
            remainder = normalized_source[len(normalized_title):].strip(" |，,.;:：")
            if remainder:
                return remainder
        return source_text
    return ""


def find_duplicate_active_task(tasks, title, remark="", team_id=""):
    expected_signature = task_dedupe_signature(title, remark)
    if not expected_signature:
        return {}
    matches = []
    for task in safe_list(tasks):
        if not isinstance(task, dict) or not str(task.get("id") or "").strip():
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES or task.get("output"):
            continue
        if task_dedupe_signature(task.get("title"), task_effective_goal_text(task)) != expected_signature:
            continue
        matches.append(task)
    if not matches:
        return {}
    return deepcopy(sorted(matches, key=duplicate_task_sort_key)[0])


def create_management_run_for_task(
    openclaw_dir,
    task_id,
    title,
    remark="",
    workflow_binding=None,
    team=None,
    target_agent_id="",
    owner="",
    risk_level="",
):
    openclaw_dir = Path(openclaw_dir)
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    team = team if isinstance(team, dict) else {}
    workflow_resolution = resolve_run_workflow_binding(
        openclaw_dir,
        workflow_id=str(workflow_binding.get("workflowId") or "").strip(),
    )
    resolved_binding = workflow_resolution.get("binding") if isinstance(workflow_resolution.get("binding"), dict) else {}
    if not resolved_binding and workflow_binding:
        resolved_binding = deepcopy(workflow_binding)
    run_id = secrets.token_hex(6)
    config = load_config(openclaw_dir)
    project_dir = resolve_planning_project_dir(openclaw_dir, config=config)
    team_id = str(team.get("id") or "").strip()
    goal = str(remark or title or task_id).strip() or task_id
    planning_binding = (
        ensure_planning_bundle(
            openclaw_dir,
            project_dir,
            "run",
            run_id,
            title=title or task_id,
            goal=goal,
            meta={
                "runId": run_id,
                "linkedTaskId": str(task_id or "").strip(),
                "linkedTeamId": team_id,
                "workflowBinding": resolved_binding,
            },
        )
        if project_dir
        else {}
    )
    return store_create_management_run(
        openclaw_dir,
        {
            "id": run_id,
            "title": str(title or task_id).strip() or task_id,
            "goal": goal,
            "owner": str(owner or "OpenClaw Team").strip() or "OpenClaw Team",
            "linkedTaskId": str(task_id or "").strip(),
            "linkedAgentId": str(target_agent_id or team.get("leadAgentId") or "").strip(),
            "linkedTeamId": team_id,
            "releaseChannel": "manual",
            "riskLevel": normalize_task_risk_for_management_run(risk_level),
            "stages": workflow_resolution.get("stages") or workflow_stages_from_workflow({}),
            "workflowBinding": resolved_binding,
            "planningBinding": planning_binding,
            "meta": {
                "source": "task_create",
            },
        },
    )


def hydrate_task_run_links(task_items, management_runs):
    task_items = task_items if isinstance(task_items, list) else []
    run_map_by_task = defaultdict(list)
    for run in management_runs if isinstance(management_runs, list) else []:
        if not isinstance(run, dict):
            continue
        linked_task_id = str(run.get("linkedTaskId") or "").strip()
        if linked_task_id:
            run_map_by_task[linked_task_id].append(run)
    for task in task_items:
        if not isinstance(task, dict):
            continue
        runs = run_map_by_task.get(str(task.get("id") or "").strip(), [])
        task["linkedRunIds"] = [str(item.get("id") or "").strip() for item in runs if str(item.get("id") or "").strip()]
        task["linkedRun"] = compact_task_run_reference(runs[0]) if runs else None
        task["linkedRunId"] = str((runs[0] if runs else {}).get("id") or "").strip()


def choose_workflow_adjustment_lane(workflow, target_lane_id="", target_agent_id=""):
    lanes = [lane for lane in safe_list(workflow.get("lanes")) if isinstance(lane, dict)]
    nodes = [node for node in safe_list(workflow.get("nodes")) if isinstance(node, dict)]
    normalized_lane_id = str(target_lane_id or "").strip()
    if normalized_lane_id and any(str(lane.get("id") or "").strip() == normalized_lane_id for lane in lanes):
        return normalized_lane_id
    normalized_target_agent_id = str(target_agent_id or "").strip()
    if normalized_target_agent_id:
        for node in nodes:
            if str(node.get("agentId") or "").strip() == normalized_target_agent_id and str(node.get("laneId") or "").strip():
                return str(node.get("laneId") or "").strip()
    preferred_keywords = ("review", "verify", "quality", "approval", "审批", "验收", "复核", "验证")
    for lane in lanes:
        lane_text = " ".join(
            [
                str(lane.get("id") or "").lower(),
                str(lane.get("title") or "").lower(),
                str(lane.get("subtitle") or "").lower(),
            ]
        )
        if any(keyword in lane_text for keyword in preferred_keywords):
            return str(lane.get("id") or "").strip()
    if len(lanes) >= 2:
        return str(lanes[min(1, len(lanes) - 1)].get("id") or "").strip()
    if lanes:
        return str(lanes[0].get("id") or "").strip()
    return ""


def perform_insert_workflow_approval_node(
    openclaw_dir,
    workflow_id,
    target_lane_id="",
    target_agent_id="",
    title="人工复核",
    approver="运营负责人",
    timeout=30,
    escalation_agent_id="",
    reason="",
):
    workflow_id = str(workflow_id or "").strip()
    if not workflow_id:
        raise RuntimeError("workflowId 不能为空。")
    workflow = next(
        (item for item in store_list_orchestration_workflows(openclaw_dir) if str(item.get("id") or "").strip() == workflow_id),
        None,
    )
    if not workflow:
        raise RuntimeError(f"未找到工作流：{workflow_id}")
    lanes = [lane for lane in safe_list(workflow.get("lanes")) if isinstance(lane, dict)]
    nodes = [node for node in safe_list(workflow.get("nodes")) if isinstance(node, dict)]
    resolved_lane_id = choose_workflow_adjustment_lane(
        workflow,
        target_lane_id=target_lane_id,
        target_agent_id=target_agent_id,
    )
    if not resolved_lane_id:
        raise RuntimeError("当前工作流没有可插入审批节点的泳道。")
    lane = next((item for item in lanes if str(item.get("id") or "").strip() == resolved_lane_id), {}) or {}
    node_title = str(title or "").strip() or "人工复核"
    existing = next(
        (
            node
            for node in nodes
            if str(node.get("laneId") or "").strip() == resolved_lane_id
            and str(node.get("type") or "agent").strip() == "approval"
            and str(node.get("title") or "").strip() == node_title
        ),
        None,
    )
    if existing:
        return {
            "workflow": workflow,
            "inserted": False,
            "node": existing,
            "targetLaneId": resolved_lane_id,
            "targetLaneTitle": lane.get("title") or resolved_lane_id,
        }

    approval_node = {
        "id": f"approval-{secrets.token_hex(6)}",
        "laneId": resolved_lane_id,
        "title": node_title,
        "type": "approval",
        "config": {
            "approver": str(approver or "").strip() or "运营负责人",
            "timeout": max(int(timeout or 0), 15),
            "escalationAgentId": str(escalation_agent_id or "").strip(),
        },
        "conditions": [],
    }
    updated_nodes = []
    inserted = False
    for node in nodes:
        if not inserted and str(node.get("laneId") or "").strip() == resolved_lane_id:
            updated_nodes.append(approval_node)
            inserted = True
        updated_nodes.append(node)
    if not inserted:
        updated_nodes.append(approval_node)

    meta = dict(workflow.get("meta") or {})
    adjustments = safe_list(meta.get("recommendedAdjustments"))
    adjustments.insert(
        0,
        {
            "type": "insert_approval_node",
            "reason": str(reason or "").strip() or "从运营建议自动插入审批节点。",
            "targetLaneId": resolved_lane_id,
            "targetLaneTitle": lane.get("title") or resolved_lane_id,
            "nodeTitle": node_title,
            "appliedAt": now_iso(),
        },
    )
    meta["recommendedAdjustments"] = adjustments[:12]
    meta["lastRecommendedAdjustmentAt"] = now_iso()
    meta["lastRecommendedAdjustmentType"] = "insert_approval_node"

    saved = store_save_orchestration_workflow(
        openclaw_dir,
        {
            "id": workflow.get("id", workflow_id),
            "name": workflow.get("name", workflow_id),
            "description": workflow.get("description", ""),
            "status": workflow.get("status", "active"),
            "lanes": lanes,
            "nodes": updated_nodes,
            "meta": meta,
        },
    )
    return {
        "workflow": saved or workflow,
        "inserted": True,
        "node": approval_node,
        "targetLaneId": resolved_lane_id,
        "targetLaneTitle": lane.get("title") or resolved_lane_id,
    }


def perform_strengthen_workflow_handoff_note(
    openclaw_dir,
    workflow_id,
    node_id="",
    title="",
    reason="",
):
    workflow_id = str(workflow_id or "").strip()
    if not workflow_id:
        raise RuntimeError("workflowId 不能为空。")
    workflow = next(
        (item for item in store_list_orchestration_workflows(openclaw_dir) if str(item.get("id") or "").strip() == workflow_id),
        None,
    )
    if not workflow:
        raise RuntimeError(f"未找到工作流：{workflow_id}")
    lanes = [lane for lane in safe_list(workflow.get("lanes")) if isinstance(lane, dict)]
    nodes = [node for node in safe_list(workflow.get("nodes")) if isinstance(node, dict)]
    normalized_node_id = str(node_id or "").strip()
    target_node = next((item for item in nodes if str(item.get("id") or "").strip() == normalized_node_id), None)
    if not target_node:
        raise RuntimeError("未找到需要强化交接说明的节点。")
    lane = next((item for item in lanes if str(item.get("id") or "").strip() == str(target_node.get("laneId") or "").strip()), {}) or {}
    recommended_note = build_recommended_handoff_note(target_node, lane)
    existing_note = str(target_node.get("handoffNote") or "").strip()
    if existing_note == recommended_note:
        return {
            "workflow": workflow,
            "updated": False,
            "node": target_node,
            "nodeId": normalized_node_id,
            "nodeTitle": title or target_node.get("title") or normalized_node_id,
        }

    updated_nodes = []
    for node in nodes:
        if str(node.get("id") or "").strip() == normalized_node_id:
            updated_nodes.append({**node, "handoffNote": recommended_note})
        else:
            updated_nodes.append(node)

    meta = dict(workflow.get("meta") or {})
    adjustments = safe_list(meta.get("recommendedAdjustments"))
    adjustments.insert(
        0,
        {
            "type": "strengthen_handoff_note",
            "reason": str(reason or "").strip() or "从运营建议自动强化交接模板。",
            "nodeId": normalized_node_id,
            "nodeTitle": title or target_node.get("title") or normalized_node_id,
            "appliedAt": now_iso(),
        },
    )
    meta["recommendedAdjustments"] = adjustments[:12]
    meta["lastRecommendedAdjustmentAt"] = now_iso()
    meta["lastRecommendedAdjustmentType"] = "strengthen_handoff_note"

    saved = store_save_orchestration_workflow(
        openclaw_dir,
        {
            "id": workflow.get("id", workflow_id),
            "name": workflow.get("name", workflow_id),
            "description": workflow.get("description", ""),
            "status": workflow.get("status", "active"),
            "lanes": lanes,
            "nodes": updated_nodes,
            "meta": meta,
        },
    )
    return {
        "workflow": saved or workflow,
        "updated": True,
        "node": next((item for item in updated_nodes if str(item.get("id") or "").strip() == normalized_node_id), target_node),
        "nodeId": normalized_node_id,
        "nodeTitle": title or target_node.get("title") or normalized_node_id,
    }


def perform_transfer_agent_tasks(openclaw_dir, source_agent_id, target_agent_id, limit=0, reason=""):
    config = load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    agent_labels, _ = build_label_maps(load_agents(config), kanban_cfg, router_agent_id)
    source_agent_id = str(source_agent_id or "").strip()
    target_agent_id = str(target_agent_id or "").strip()
    if not source_agent_id or not target_agent_id:
        raise RuntimeError("需要 sourceAgentId 和 targetAgentId。")
    if source_agent_id == target_agent_id:
        raise RuntimeError("源 Agent 和目标 Agent 不能相同。")
    transferred = []
    normalized_limit = max(int(limit or 0), 0)
    target_label = agent_labels.get(target_agent_id, target_agent_id)
    source_label = agent_labels.get(source_agent_id, source_agent_id)
    transfer_reason = str(reason or "").strip() or f"运营手动转移给 {target_label}。"
    candidate_tasks = merge_tasks(openclaw_dir, config)
    fallback_workspace = router_workspace_path(openclaw_dir, router_agent_id)
    for merged_task in candidate_tasks:
        if normalized_limit and len(transferred) >= normalized_limit:
            break
        if not isinstance(merged_task, dict):
            continue
        state = str(merged_task.get("state", merged_task.get("status", ""))).lower()
        if state in TERMINAL_STATES:
            continue
        current_agent = current_agent_for_task(merged_task, kanban_cfg, router_agent_id)
        target_agent = str(merged_task.get("targetAgentId") or "").strip()
        if source_agent_id not in {current_agent, target_agent}:
            continue
        task_id = str(merged_task.get("id") or "").strip()
        if not task_id:
            continue
        previous_label = agent_labels.get(current_agent or source_agent_id, merged_task.get("org", source_agent_id))
        updated_at = now_iso()
        changed = {"done": False}
        task_workspace = task_workspace_for_task(
            openclaw_dir,
            task_id,
            config=config,
            router_agent_id=router_agent_id,
        ) or fallback_workspace

        def modifier(data):
            tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
            for task in tasks:
                if not isinstance(task, dict) or str(task.get("id") or "").strip() != task_id:
                    continue
                meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
                route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
                route_meta["targetAgentId"] = target_agent_id
                route_meta["targetAgentLabel"] = target_label
                route_meta["reason"] = transfer_reason
                route_meta["fallback"] = False
                route_meta["decidedAt"] = updated_at
                route_meta["transferredAt"] = updated_at
                route_meta["transferredFromAgentId"] = source_agent_id
                route_meta["transferredBy"] = "operator"
                execution_bootstrap = route_meta.get("executionBootstrap") if isinstance(route_meta.get("executionBootstrap"), dict) else {}
                if execution_bootstrap:
                    execution_bootstrap["agentId"] = target_agent_id
                    execution_bootstrap["agentLabel"] = target_label
                    route_meta["executionBootstrap"] = execution_bootstrap
                meta["routeDecision"] = route_meta
                task["meta"] = meta
                task["targetAgentId"] = target_agent_id
                task["targetAgentLabel"] = target_label
                task["currentAgent"] = target_agent_id
                task["currentAgentLabel"] = target_label
                task["org"] = target_label
                task["currentUpdate"] = transfer_reason
                task["now"] = transfer_reason
                task.setdefault("flow_log", [])
                task["flow_log"].append(
                    {
                        "at": updated_at,
                        "from": previous_label or source_label,
                        "to": target_label,
                        "remark": transfer_reason,
                    }
                )
                task.setdefault("progress_log", [])
                task["progress_log"].append(
                    {
                        "at": updated_at,
                        "agent": target_agent_id,
                        "agentLabel": target_label,
                        "text": transfer_reason,
                    }
                )
                task["updatedAt"] = updated_at
                changed["done"] = True
            return tasks

        atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
        if changed["done"]:
            transferred.append({"id": task_id, "title": merged_task.get("title", ""), "targetAgentId": target_agent_id})
    return {"count": len(transferred), "tasks": transferred, "targetAgentLabel": target_label}


def enrich_skills_payload_with_role_workflows(openclaw_dir, payload):
    payload = payload if isinstance(payload, dict) else {}
    payload.setdefault("skills", [])
    payload.setdefault("guidance", [])
    payload.setdefault("commands", [])
    payload.setdefault("summary", {"total": 0, "ready": 0, "warning": 0, "error": 0, "packaged": 0, "categories": {}})
    payload = dedupe_skills_payload_entries(payload)
    payload = refresh_skill_catalog_summary(payload)
    store_profiles = store_list_skill_role_profiles(openclaw_dir)
    store_packs = store_list_workflow_packs(openclaw_dir)
    profile_map = {item.get("skillSlug", ""): item for item in store_profiles if item.get("skillSlug")}
    skills = safe_list(payload.get("skills"))
    for skill in skills:
        slug = str(skill.get("slug") or "").strip()
        catalog_profile = skill_role_profile_from_catalog(skill)
        inferred_profile = infer_skill_role_profile(skill)
        role_profile = profile_map.get(slug) or catalog_profile or inferred_profile or {}
        skill["mode"] = str(role_profile.get("mode") or "").strip()
        skill["stage"] = str(role_profile.get("stage") or "").strip()
        skill["recommendedEntry"] = str(role_profile.get("recommendedEntry") or "").strip()
        skill["outputContract"] = clean_unique_strings(role_profile.get("outputContract") or [])
        skill["requiresRuntime"] = clean_unique_strings(role_profile.get("requiresRuntime") or [])
        skill["handoffArtifacts"] = clean_unique_strings(role_profile.get("handoffArtifacts") or [])
        skill["roleProfileSource"] = (
            "saved" if slug in profile_map else
            "catalog" if catalog_profile else
            str((role_profile.get("meta") or {}).get("source") or "none")
        )
        skill["packIds"] = []
        skill["packCount"] = 0

    skill_map = {str(skill.get("slug") or "").strip(): skill for skill in skills if skill.get("slug")}
    merged_packs = {item.get("id"): deepcopy(item) for item in STARTER_WORKFLOW_PACKS if item.get("id")}
    for pack in store_packs:
        pack_id = str(pack.get("id") or "").strip()
        if pack_id:
            merged_packs[pack_id] = merge_workflow_pack_record(merged_packs.get(pack_id), pack)
    packs = []
    for pack in merged_packs.values():
        skill_slugs = clean_unique_strings(pack.get("skills") or [])
        resolved_skills = []
        missing_skill_slugs = []
        required_runtimes = []
        for slug in skill_slugs:
            catalog_skill = skill_map.get(slug)
            skill = catalog_skill or fallback_skill_record_for_slug(slug)
            if skill:
                resolved_skills.append(
                    {
                        "slug": slug,
                        "displayName": skill.get("displayName") or skill.get("name") or slug,
                        "mode": skill.get("mode") or "",
                        "stage": skill.get("stage") or "",
                        "recommendedEntry": skill.get("recommendedEntry") or "",
                        "requiresRuntime": clean_unique_strings(skill.get("requiresRuntime") or []),
                        "handoffArtifacts": clean_unique_strings(skill.get("handoffArtifacts") or []),
                        "outputContract": clean_unique_strings(skill.get("outputContract") or []),
                    }
                )
                required_runtimes.extend(clean_unique_strings(skill.get("requiresRuntime") or []))
                if catalog_skill:
                    catalog_skill.setdefault("packIds", []).append(pack.get("id"))
            else:
                missing_skill_slugs.append(slug)
        pack["skills"] = skill_slugs
        pack["skillRefs"] = resolved_skills
        pack["missingSkillSlugs"] = missing_skill_slugs
        pack["requiredRuntimes"] = sorted(set(required_runtimes))
        pack["skillCount"] = len(skill_slugs)
        pack["resolvedSkillCount"] = len(resolved_skills)
        pack["incomplete"] = bool(missing_skill_slugs)
        pack["hydrationStatus"] = "incomplete" if missing_skill_slugs else "ready"
        capabilities = workflow_pack_capabilities(pack)
        pack["reviewGates"] = capabilities.get("reviewGates", [])
        pack["artifactTemplates"] = capabilities.get("artifactTemplates", [])
        pack["runtimePolicy"] = capabilities.get("runtimePolicy", {})
        pack["releasePolicy"] = capabilities.get("releasePolicy", {})
        pack["qaPolicy"] = capabilities.get("qaPolicy", {})
        pack["modeAliases"] = capabilities.get("modeAliases", [])
        packs.append(pack)
    packs.sort(key=lambda item: (not bool(item.get("starter")), item.get("status") != "active", item.get("name") or ""))

    for skill in skills:
        skill["packIds"] = clean_unique_strings(skill.get("packIds") or [])
        skill["packCount"] = len(skill["packIds"])

    role_counter = Counter(skill.get("mode", "") for skill in skills if skill.get("mode"))
    stage_counter = Counter(skill.get("stage", "") for skill in skills if skill.get("stage"))
    runtime_counter = Counter(runtime for skill in skills for runtime in clean_unique_strings(skill.get("requiresRuntime") or []))
    payload["packs"] = packs
    payload["roleSummary"] = dict(role_counter)
    payload["stageSummary"] = dict(stage_counter)
    payload["runtimeSummary"] = dict(runtime_counter)
    payload["packSummary"] = {
        "total": len(packs),
        "starter": sum(1 for item in packs if item.get("starter")),
        "active": sum(1 for item in packs if item.get("status") == "active"),
        "incomplete": sum(1 for item in packs if item.get("incomplete")),
    }
    return payload


def hydrate_workflow_pack_context(workflow, pack_map):
    workflow = deepcopy(workflow) if isinstance(workflow, dict) else {}
    meta = workflow.get("meta") if isinstance(workflow.get("meta"), dict) else {}
    pack_template = meta.get("packTemplate") if isinstance(meta.get("packTemplate"), dict) else {}
    linked_pack = hydrate_linked_pack(pack_template, pack_map)
    workflow["packTemplate"] = pack_template
    workflow["linkedPackId"] = linked_pack.get("id", "")
    workflow["linkedPack"] = linked_pack
    workflow["reviewGates"] = safe_list(meta.get("reviewGates")) or safe_list(linked_pack.get("reviewGates"))
    workflow["artifactTemplates"] = safe_list(meta.get("artifactTemplates")) or safe_list(linked_pack.get("artifactTemplates"))
    return workflow


def perform_workflow_pack_launch_to_run(openclaw_dir, pack, payload, actor):
    payload = payload if isinstance(payload, dict) else {}
    config = load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    pack_binding = workflow_pack_binding_payload(pack, source=str(payload.get("source") or "skills"), target="run")
    run_id = str(payload.get("runId") or "").strip()
    if run_id:
        current = next((item for item in store_list_management_runs(openclaw_dir, limit=96) if item.get("id") == run_id), None)
        existing_meta = deepcopy(current.get("meta", {})) if isinstance((current or {}).get("meta"), dict) else {}
        return store_save_management_run_record(
            openclaw_dir,
            run_id,
            {
                "packBinding": pack_binding,
                "meta": seeded_run_meta_from_pack(pack, {**existing_meta, "packBinding": pack_binding}),
            },
        )

    linked_task_id = str(payload.get("linkedTaskId") or "").strip()
    current_tasks = merge_tasks(openclaw_dir, config)
    linked_task = next((item for item in current_tasks if item.get("id") == linked_task_id), None)
    workflow_resolution = resolve_run_workflow_binding(
        openclaw_dir,
        workflow_id=str(payload.get("workflowId", "")).strip(),
        linked_task=linked_task,
    )
    team_id = str(payload.get("teamId") or pack.get("recommendedTeamId") or "").strip()
    team = resolve_agent_team_record(openclaw_dir, team_id) if team_id else None
    project_dir = resolve_planning_project_dir(openclaw_dir, config=config)
    run_id = secrets.token_hex(6)
    title = str(payload.get("title") or pack.get("name") or "Workflow Pack Run").strip() or "Workflow Pack Run"
    goal = str(payload.get("brief") or payload.get("goal") or pack.get("description") or title).strip()
    planning_binding = (
        ensure_planning_bundle(
            openclaw_dir,
            project_dir,
            "run",
            run_id,
            title=title,
            goal=goal,
            meta={
                "runId": run_id,
                "linkedTaskId": linked_task_id,
                "linkedTeamId": team_id,
                "workflowBinding": workflow_resolution.get("binding", {}),
                "packBinding": pack_binding,
            },
        )
        if project_dir
        else {}
    )
    return store_create_management_run(
        openclaw_dir,
        {
            "id": run_id,
            "title": title,
            "goal": goal,
            "owner": str(payload.get("owner") or actor.get("displayName", "") or "OpenClaw Team").strip() or "OpenClaw Team",
            "linkedTaskId": linked_task_id,
            "linkedAgentId": str(payload.get("linkedAgentId") or ((team or {}).get("leadAgentId") if team else "") or router_agent_id).strip(),
            "linkedTeamId": team_id,
            "linkedSessionKey": str(payload.get("linkedSessionKey") or "").strip(),
            "releaseChannel": str(payload.get("releaseChannel") or "manual").strip() or "manual",
            "riskLevel": str(payload.get("riskLevel") or "medium").strip() or "medium",
            "stages": pack_seeded_management_stages(pack),
            "workflowBinding": workflow_resolution.get("binding", {}),
            "planningBinding": planning_binding,
            "packBinding": pack_binding,
            "meta": seeded_run_meta_from_pack(pack, {"packBinding": pack_binding}),
        },
    )


def perform_workflow_pack_launch_to_studio(openclaw_dir, pack, payload):
    payload = payload if isinstance(payload, dict) else {}
    config = load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    agents = load_agents(config)
    pack_binding = workflow_pack_binding_payload(pack, source=str(payload.get("source") or "skills"), target="studio")
    pack_capabilities = workflow_pack_capabilities(pack)
    workflow_id = str(payload.get("workflowId") or "").strip()
    existing_workflow = next(
        (item for item in store_list_orchestration_workflows(openclaw_dir) if str(item.get("id") or "").strip() == workflow_id),
        None,
    ) if workflow_id else None
    if existing_workflow:
        workflow_meta = deepcopy(existing_workflow.get("meta", {})) if isinstance(existing_workflow.get("meta"), dict) else {}
        workflow_meta["packTemplate"] = pack_binding
        workflow_meta["reviewGates"] = pack_capabilities.get("reviewGates", [])
        workflow_meta["artifactTemplates"] = pack_capabilities.get("artifactTemplates", [])
        return store_save_orchestration_workflow(
            openclaw_dir,
            {
                **existing_workflow,
                "meta": workflow_meta,
            },
        )
    name = str(payload.get("title") or pack.get("name") or "Workflow Pack Draft").strip() or "Workflow Pack Draft"
    return store_save_orchestration_workflow(
        openclaw_dir,
        {
            "id": "",
            "name": name,
            "description": str(payload.get("brief") or pack.get("description") or "").strip(),
            "status": "draft",
            "lanes": build_pack_workflow_lanes(pack),
            "nodes": build_pack_workflow_nodes(pack, agents, router_agent_id=router_agent_id),
            "meta": {
                "templateSource": "workflow-pack",
                "packTemplate": pack_binding,
                "reviewGates": pack_capabilities.get("reviewGates", []),
                "artifactTemplates": pack_capabilities.get("artifactTemplates", []),
            },
        },
    )


def resolve_management_run_context(openclaw_dir, run_id, skills_data=None):
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise RuntimeError("请先提供 Run 编号。")
    skills_payload = skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir)
    pack_map = workflow_pack_map_from_skills_payload(skills_payload)
    run = next((item for item in store_list_management_runs(openclaw_dir, limit=128) if item.get("id") == normalized_run_id), None)
    if not run:
        raise RuntimeError(f"未找到 Run：{normalized_run_id}")
    return hydrate_management_run_pack_context(run, pack_map), skills_payload


def persist_management_run_context(openclaw_dir, run, *, meta, status="", stage_key="", stages=None, completed_at=""):
    payload = {"meta": meta}
    if status:
        payload["status"] = status
    if stage_key:
        payload["stageKey"] = stage_key
    if stages is not None:
        payload["stages"] = stages
    if completed_at:
        payload["completedAt"] = completed_at
    return store_save_management_run_record(openclaw_dir, str(run.get("id") or "").strip(), payload)


def perform_management_run_gate_update(openclaw_dir, run_id, gate_id, action, note=""):
    run, _skills_payload = resolve_management_run_context(openclaw_dir, run_id)
    gates = normalize_run_review_gates(run.get("reviewGates"), run.get("linkedPack") or {})
    normalized_gate_id = str(gate_id or "").strip()
    target_gate = next((item for item in gates if item.get("id") == normalized_gate_id), None)
    if not target_gate:
        raise RuntimeError(f"未找到 review gate：{normalized_gate_id}")
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"pass", "block", "reset", "activate"}:
        raise RuntimeError(f"不支持的 gate 动作：{normalized_action or 'unknown'}")
    now = now_iso()
    for gate in gates:
        if gate.get("id") != normalized_gate_id:
            if normalized_action in {"reset", "activate"} and gate.get("status") == "active":
                gate["status"] = "pending"
            continue
        if normalized_action == "pass":
            gate["status"] = "passed"
            gate["passedAt"] = now
        elif normalized_action == "block":
            gate["status"] = "blocked"
        else:
            gate["status"] = "active" if normalized_action == "activate" else "pending"
            gate["passedAt"] = ""
        gate["updatedAt"] = now
        if note:
            gate["note"] = str(note).strip()
    if normalized_action == "pass" and not any(item.get("status") == "active" for item in gates):
        next_pending = next((item for item in gates if item.get("status") == "pending"), None)
        if next_pending:
            next_pending["status"] = "active"
            next_pending["updatedAt"] = now
    stages, next_stage_key, next_status = apply_run_gate_stage_transition(run, target_gate, normalized_action)
    if normalized_action == "pass" and gates and all(item.get("status") == "passed" for item in gates):
        next_status = "complete" if bool((run.get("releaseAutomation") or {}).get("completeOnSuccess")) else "active"
        if next_status == "complete":
            for stage in stages:
                stage["status"] = "done"
                stage["updatedAt"] = now
    meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
    meta["reviewGates"] = gates
    persisted = persist_management_run_context(
        openclaw_dir,
        run,
        meta=meta,
        status=next_status,
        stage_key=next_stage_key,
        stages=stages,
        completed_at=now if next_status == "complete" else "",
    )
    return persisted


def perform_management_run_artifact_save(openclaw_dir, run_id, payload):
    payload = payload if isinstance(payload, dict) else {}
    run, _skills_payload = resolve_management_run_context(openclaw_dir, run_id)
    meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
    templates = normalize_pack_artifact_templates(meta.get("artifactTemplates"), run.get("linkedPack") or {})
    template = next((item for item in templates if item.get("id") == str(payload.get("templateId") or "").strip()), {})
    artifacts = normalize_run_artifacts(meta.get("artifacts"))
    artifact_id = str(payload.get("id") or "").strip()
    existing = next((item for item in artifacts if item.get("id") == artifact_id), None) if artifact_id else None
    now = now_iso()
    artifact_type = str(payload.get("type") or template.get("type") or "").strip().lower()
    if not artifact_type:
        raise RuntimeError("请先选择 artifact 类型。")
    artifact = {
        "id": artifact_id or (existing or {}).get("id") or secrets.token_hex(6),
        "type": artifact_type,
        "title": str(payload.get("title") or template.get("title") or artifact_type_label(artifact_type)).strip(),
        "status": str(payload.get("status") or (existing or {}).get("status") or "ready").strip().lower() or "ready",
        "summary": str(payload.get("summary") or (existing or {}).get("summary") or "").strip(),
        "body": str(payload.get("body") or (existing or {}).get("body") or "").strip(),
        "stageKey": str(payload.get("stageKey") or template.get("stageKey") or (existing or {}).get("stageKey") or "").strip(),
        "path": str(payload.get("path") or payload.get("output") or (existing or {}).get("path") or "").strip(),
        "createdAt": str((existing or {}).get("createdAt") or now).strip(),
        "updatedAt": now,
        "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else ((existing or {}).get("meta") if isinstance((existing or {}).get("meta"), dict) else {}),
    }
    if existing:
        artifacts = [artifact if item.get("id") == existing.get("id") else item for item in artifacts]
    else:
        artifacts.append(artifact)
    runtime_sessions = normalize_run_runtime_sessions(meta.get("runtimeSessions"), run.get("linkedPack") or {})
    browser_session = runtime_sessions.get("browser") if isinstance(runtime_sessions.get("browser"), dict) else {}
    if artifact_type == "cookie-bootstrap" and browser_session:
        browser_session["cookieBootstrapStatus"] = "ready"
        browser_session["bootstrappedAt"] = now
    if artifact_type == "browser-snapshot" and browser_session:
        browser_session["lastSnapshotAt"] = now
        browser_session["lastSnapshotSummary"] = artifact.get("summary") or artifact.get("body", "")[:180]
    if browser_session:
        runtime_sessions["browser"] = browser_session
    review_gates = normalize_run_review_gates(meta.get("reviewGates"), run.get("linkedPack") or {})
    gate_id = str(payload.get("gateId") or "").strip()
    if gate_id:
        for gate in review_gates:
            if gate.get("id") == gate_id:
                gate["artifactIds"] = clean_unique_strings([*gate.get("artifactIds", []), artifact["id"]])
                gate["updatedAt"] = now
    qa_automation = normalize_run_qa_automation(meta.get("qaAutomation"), run.get("linkedPack") or {})
    if artifact_type == "qa-report":
        qa_automation["lastRunAt"] = now
        qa_automation["summary"] = artifact.get("summary") or artifact.get("body", "")[:180]
    release_automation = normalize_run_release_automation(meta.get("releaseAutomation"), run.get("linkedPack") or {})
    if artifact_type in {"release-note", "release-doc", "ship-pr"}:
        release_automation["lastAttemptAt"] = now
    meta = {
        **meta,
        "artifacts": artifacts,
        "runtimeSessions": runtime_sessions,
        "reviewGates": review_gates,
        "qaAutomation": qa_automation,
        "releaseAutomation": release_automation,
    }
    return persist_management_run_context(openclaw_dir, run, meta=meta)


def perform_management_run_browser_action(openclaw_dir, run_id, payload):
    payload = payload if isinstance(payload, dict) else {}
    run, _skills_payload = resolve_management_run_context(openclaw_dir, run_id)
    linked_pack = run.get("linkedPack") or {}
    meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
    runtime_sessions = normalize_run_runtime_sessions(meta.get("runtimeSessions"), linked_pack)
    browser_session = runtime_sessions.get("browser") if isinstance(runtime_sessions.get("browser"), dict) else {}
    if not browser_session:
        raise RuntimeError("当前 Run 没有 browser runtime 依赖。")
    action = str(payload.get("action") or "start").strip().lower()
    profile = str(payload.get("profile") or browser_session.get("profile") or default_pack_browser_profile(linked_pack)).strip()
    result = {}
    if action == "start":
        result = perform_browser_start(openclaw_dir, profile=profile)
        browser_session["status"] = "running"
    elif action == "open":
        result = perform_browser_open(openclaw_dir, url=str(payload.get("url") or "").strip(), profile=profile)
        browser_session["status"] = "running"
        browser_session["targetUrl"] = str(payload.get("url") or "").strip()
    elif action == "snapshot":
        result = perform_browser_snapshot(
            openclaw_dir,
            profile=profile,
            selector=str(payload.get("selector") or "").strip(),
            target_id=str(payload.get("targetId") or "").strip(),
            limit=int(payload.get("limit", 120) or 120),
        )
        browser_session["status"] = "running"
        browser_session["lastSnapshotAt"] = now_iso()
        browser_session["lastSnapshotSummary"] = str(result.get("output") or "").strip()[:180]
    elif action == "bootstrap":
        cookie_header = str(payload.get("cookieHeader") or "").strip()
        cookie_count = max(
            int(payload.get("cookieCount") or 0),
            len([item for item in cookie_header.split(";") if "=" in item]) if cookie_header else 0,
        )
        browser_session["status"] = "ready"
        browser_session["cookieBootstrapStatus"] = "ready"
        browser_session["bootstrappedAt"] = now_iso()
        browser_session["cookieCount"] = cookie_count
        browser_session["cookieSource"] = str(payload.get("cookieSource") or ("manual" if cookie_header else "profile")).strip() or "profile"
        if normalize_flag(payload.get("startBrowser"), default=False):
            result = perform_browser_start(openclaw_dir, profile=profile)
        else:
            result = {"output": "cookie bootstrap recorded"}
    else:
        raise RuntimeError(f"不支持的 browser 动作：{action}")
    browser_session["profile"] = profile
    runtime_sessions["browser"] = browser_session
    meta["runtimeSessions"] = runtime_sessions
    persisted = persist_management_run_context(openclaw_dir, run, meta=meta)
    if action == "snapshot":
        persisted = perform_management_run_artifact_save(
            openclaw_dir,
            run_id,
            {
                "type": "browser-snapshot",
                "title": "Browser snapshot",
                "summary": str(result.get("output") or "").strip()[:180],
                "body": str(result.get("output") or "").strip(),
                "stageKey": str(payload.get("stageKey") or "verify").strip(),
            },
        )
    elif action == "bootstrap":
        persisted = perform_management_run_artifact_save(
            openclaw_dir,
            run_id,
            {
                "type": "cookie-bootstrap",
                "title": "Cookie bootstrap",
                "summary": f"Profile {profile or 'default'} 已准备 {browser_session.get('cookieCount', 0)} 条 cookies。",
                "stageKey": str(payload.get("stageKey") or "verify").strip(),
            },
        )
    return persisted, result


def perform_management_run_release_ship(openclaw_dir, run_id, payload):
    payload = payload if isinstance(payload, dict) else {}
    run, _skills_payload = resolve_management_run_context(openclaw_dir, run_id)
    meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
    release_state = normalize_run_release_automation(meta.get("releaseAutomation"), run.get("linkedPack") or {})
    gates = normalize_run_review_gates(meta.get("reviewGates"), run.get("linkedPack") or {})
    if release_state.get("requireAllGates") and any(item.get("status") != "passed" for item in gates):
        raise RuntimeError("当前还有 review gates 未通过，暂时不能执行 release automation。")
    provider = str(payload.get("provider") or release_state.get("provider") or "manual").strip().lower()
    dry_run = normalize_flag(payload.get("dryRun"), default=release_state.get("dryRun", provider != "github-pr"))
    base_branch = str(payload.get("baseBranch") or release_state.get("baseBranch") or "main").strip() or "main"
    context = {
        "runTitle": str(run.get("title") or run.get("id") or "").strip(),
        "runId": str(run.get("id") or "").strip(),
    }
    pr_title = render_release_template(
        str(payload.get("prTitleTemplate") or release_state.get("prTitleTemplate") or "{runTitle}").strip(),
        context,
    )
    project_dir = resolve_planning_project_dir(openclaw_dir)
    head_branch = str(payload.get("headBranch") or release_state.get("headBranch") or "").strip()
    result = {"provider": provider}
    if provider == "fixture":
        pr_number = int(time.time()) % 100000
        result.update(
            {
                "status": "success",
                "prNumber": pr_number,
                "prUrl": f"https://example.invalid/mission-control/pull/{pr_number}",
                "baseBranch": base_branch,
                "headBranch": head_branch or f"mission-control/{orchestration_slug(context['runTitle']) or context['runId']}",
                "output": "fixture release automation completed",
            }
        )
    else:
        if not project_dir:
            raise RuntimeError("当前环境没有可用于 release automation 的项目目录。")
        if not head_branch:
            branch_result = run_command(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=project_dir)
            head_branch = (branch_result.stdout or "").strip()
        if not head_branch:
            head_branch = f"{release_state.get('branchPrefix') or 'mission-control/'}{orchestration_slug(context['runTitle']) or context['runId']}"
        command = [
            "gh", "pr", "create",
            "--base", base_branch,
            "--head", head_branch,
            "--title", pr_title,
            "--body", str(payload.get("body") or run.get("goal") or "").strip() or pr_title,
        ]
        if provider != "github-pr" or dry_run:
            result.update(
                {
                    "status": "preview",
                    "prNumber": "",
                    "prUrl": "",
                    "baseBranch": base_branch,
                    "headBranch": head_branch,
                    "output": shlex.join(command),
                }
            )
        else:
            process = run_command(command, cwd=project_dir, env={**os.environ, "GH_FORCE_TTY": "0"})
            output = join_command_output(process)
            if process.returncode != 0:
                raise RuntimeError(output or "GitHub PR 创建失败。")
            url_match = re.search(r"https?://\S+", output)
            result.update(
                {
                    "status": "success",
                    "prNumber": "",
                    "prUrl": url_match.group(0) if url_match else "",
                    "baseBranch": base_branch,
                    "headBranch": head_branch,
                    "output": output,
                }
            )
    release_state = {
        **release_state,
        "provider": provider,
        "dryRun": dry_run,
        "status": result.get("status", "preview"),
        "lastAttemptAt": now_iso(),
        "prNumber": str(result.get("prNumber") or "").strip(),
        "prUrl": str(result.get("prUrl") or "").strip(),
        "headBranch": str(result.get("headBranch") or "").strip(),
        "baseBranch": str(result.get("baseBranch") or "").strip(),
        "output": str(result.get("output") or "").strip(),
    }
    meta["releaseAutomation"] = release_state
    next_status = ""
    completed_at = ""
    stages = deepcopy(run.get("stages") or [])
    if result.get("status") == "success" and release_state.get("completeOnSuccess"):
        next_status = "complete"
        completed_at = now_iso()
        for stage in stages:
            stage["status"] = "done"
            stage["updatedAt"] = completed_at
    persisted = persist_management_run_context(
        openclaw_dir,
        run,
        meta=meta,
        status=next_status,
        stage_key=str(stages[-1].get("key") or run.get("stageKey") or "").strip() if next_status == "complete" and stages else "",
        stages=stages if stages else None,
        completed_at=completed_at,
    )
    persisted = perform_management_run_artifact_save(
        openclaw_dir,
        run_id,
        {
            "type": "ship-pr",
            "title": "Release automation",
            "summary": str(result.get("prUrl") or result.get("output") or "").strip()[:180],
            "body": str(result.get("output") or "").strip(),
            "stageKey": "release",
        },
    )
    return persisted, result


def perform_management_run_qa_self_heal(openclaw_dir, run_id, payload):
    payload = payload if isinstance(payload, dict) else {}
    run, _skills_payload = resolve_management_run_context(openclaw_dir, run_id)
    meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
    qa_state = normalize_run_qa_automation(meta.get("qaAutomation"), run.get("linkedPack") or {})
    issues = payload.get("issues") if isinstance(payload.get("issues"), list) else []
    issue_count = max(int(payload.get("issueCount") or 0), len(issues))
    apply_fixes = normalize_flag(payload.get("applyFixes"), default=qa_state.get("autoFix", False))
    summary = str(payload.get("summary") or "").strip() or (
        "No issues found during QA verification." if issue_count == 0 else f"Found {issue_count} issues that require follow-up."
    )
    artifact_body = summary
    if issues:
        artifact_body = "\n".join([summary, "", *[f"- {str(item).strip()}" for item in issues if str(item).strip()]])
    persisted = perform_management_run_artifact_save(
        openclaw_dir,
        run_id,
        {
            "type": "qa-report",
            "title": "QA report",
            "summary": summary,
            "body": artifact_body,
            "stageKey": "verify",
        },
    )
    followup_task = None
    if apply_fixes and issue_count > 0 and qa_state.get("createRemediationTask"):
        followup_task = perform_task_create(
            openclaw_dir,
            title=f"{run.get('title') or run.get('id')} QA remediation",
            remark=summary,
            preferred_team_id=str(run.get("linkedTeamId") or (run.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
            run_owner="QA Self-Heal",
            team_override_reason="自动从 QA Self-Heal 创建的修复任务。",
        )
        persisted = perform_management_run_artifact_save(
            openclaw_dir,
            run_id,
            {
                "type": "heal-plan",
                "title": "Self-heal plan",
                "summary": f"已创建修复任务 {followup_task.get('taskId', '')}。",
                "body": summary,
                "stageKey": "verify",
            },
        )
    resolved_run, _ = resolve_management_run_context(openclaw_dir, run_id)
    meta = resolved_run.get("meta") if isinstance(resolved_run.get("meta"), dict) else {}
    qa_state = normalize_run_qa_automation(meta.get("qaAutomation"), resolved_run.get("linkedPack") or {})
    qa_state.update(
        {
            "status": "passed" if issue_count == 0 else ("healing" if followup_task else "attention"),
            "lastRunAt": now_iso(),
            "issueCount": issue_count,
            "followupTaskId": (followup_task or {}).get("taskId", qa_state.get("followupTaskId", "")),
            "summary": summary,
        }
    )
    review_gates = normalize_run_review_gates(meta.get("reviewGates"), resolved_run.get("linkedPack") or {})
    if issue_count == 0:
        for gate in review_gates:
            if str(gate.get("stageKey") or "").strip() == "verify" or "qa" in str(gate.get("title") or "").lower():
                gate["status"] = "passed"
                gate["passedAt"] = now_iso()
                gate["updatedAt"] = now_iso()
    meta["qaAutomation"] = qa_state
    meta["reviewGates"] = review_gates
    next_status = "blocked" if issue_count > 0 else ""
    persisted = persist_management_run_context(openclaw_dir, resolved_run, meta=meta, status=next_status)
    return persisted, followup_task


def default_orchestration_workflow(agents, router_agent_id):
    preferred_order = [router_agent_id] + [agent.get("id") for agent in agents if agent.get("id") != router_agent_id]
    picked = [item for item in preferred_order if item][:4]
    lane_defs = [
        {"id": "intake", "title": "Intake", "subtitle": "需求进入与分拣"},
        {"id": "build", "title": "Engineering", "subtitle": "方案与执行"},
        {"id": "quality", "title": "Quality", "subtitle": "审议与验收"},
        {"id": "ops", "title": "Ops", "subtitle": "发布与运营收口"},
    ]
    nodes = []
    for index, lane in enumerate(lane_defs):
        agent_id = picked[index] if index < len(picked) else (picked[-1] if picked else "")
        nodes.append(
            {
                "id": f"node-{lane['id']}",
                "laneId": lane["id"],
                "title": lane["title"],
                "agentId": agent_id,
                "handoffNote": "在产品中可视化调整此阶段的负责 Agent 与交接语义。",
            }
        )
    return {
        "id": "starter-workflow",
        "name": "Starter Delivery Flow",
        "description": "默认的工程 -> 质量 -> 运维闭环，可作为流程编排工作台的起点。",
        "status": "draft",
        "lanes": lane_defs,
        "nodes": nodes,
        "meta": {"starter": True},
    }


def compact_orchestration_routing_decision(decision):
    decision = decision if isinstance(decision, dict) else {}
    return {
        "id": str(decision.get("id") or "").strip(),
        "taskId": str(decision.get("taskId") or "").strip(),
        "taskTitle": str(decision.get("taskTitle") or "").strip(),
        "policyId": str(decision.get("policyId") or "").strip(),
        "policyName": str(decision.get("policyName") or "").strip(),
        "strategyType": str(decision.get("strategyType") or "").strip(),
        "targetAgentId": str(decision.get("targetAgentId") or "").strip(),
        "priorityLevel": str(decision.get("priorityLevel") or "").strip(),
        "matchedKeyword": str(decision.get("matchedKeyword") or "").strip(),
        "decidedAt": str(decision.get("decidedAt") or "").strip(),
    }


def resolve_run_workflow_binding(openclaw_dir, workflow_id="", linked_task=None):
    workflow_id = str(workflow_id or "").strip()
    task_binding = linked_task.get("workflowBinding") if isinstance(linked_task, dict) else {}
    if not workflow_id and isinstance(task_binding, dict):
        workflow_id = str(task_binding.get("workflowId") or "").strip()
    if not workflow_id:
        return {"stages": [], "binding": {}}
    workflow = next((item for item in store_list_orchestration_workflows(openclaw_dir) if item.get("id") == workflow_id), None)
    if not workflow:
        return {"stages": [], "binding": {}}
    versions = store_list_orchestration_workflow_versions(openclaw_dir, workflow_id=workflow_id, limit=1)
    version = versions[0] if versions else None
    return {
        "stages": workflow_stages_from_workflow(workflow),
        "binding": {
            "workflowId": workflow.get("id", ""),
            "workflowName": workflow.get("name", ""),
            "workflowVersionId": version.get("id", "") if version else "",
            "workflowVersionNumber": version.get("versionNumber", 0) if version else 0,
            "stageSource": "workflow",
        },
    }
