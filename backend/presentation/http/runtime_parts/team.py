"""Runtime part: team."""

def normalize_team_wake_scope(value):
    scope = str(value or "").strip().lower()
    if scope in TEAM_WAKE_SCOPES:
        return scope
    return ""


def resolve_team_default_dispatch_mode(team):
    team_meta = team_runtime_meta(team)
    return normalize_chat_dispatch_mode(team_meta.get("defaultDispatchMode"), has_team=bool((team or {}).get("id")))


def team_operating_brief(team):
    team_meta = team_runtime_meta(team)
    return str(team_meta.get("operatingBrief") or "").strip()


def team_decision_log_text(team):
    team_meta = team_runtime_meta(team)
    return str(team_meta.get("decisionLog") or "").strip()


def normalize_team_context_lines(value, limit=4):
    items = safe_list(value) if isinstance(value, list) else str(value or "").splitlines()
    lines = []
    for raw_item in items:
        normalized = re.sub(r"^[\-\u2022]\s*", "", str(raw_item or "").strip())
        if normalized and normalized not in lines:
            lines.append(normalized)
        if len(lines) >= max(1, int(limit or 4)):
            break
    return lines


def dispatch_state_agent_labels(dispatch_state):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    labels = {}

    def collect(items):
        for item in safe_list(items):
            if not isinstance(item, dict):
                continue
            agent_id = str(item.get("agentId") or "").strip()
            label = str(
                item.get("agentDisplayName")
                or item.get("agentHumanName")
                or item.get("agentLabel")
                or agent_id
            ).strip()
            if agent_id and label and agent_id not in labels:
                labels[agent_id] = label

    collect(dispatch_state.get("responses"))
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    collect(relay.get("responses"))
    collect(dispatch_state.get("failedAgents"))
    return labels


def dispatch_state_relay_target_ids(dispatch_state):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    target_agent_ids = [
        str(item or "").strip()
        for item in safe_list(relay.get("targetAgentIds"))
        if str(item or "").strip()
    ]
    if target_agent_ids:
        return target_agent_ids
    participant_agent_ids = [
        str(item or "").strip()
        for item in safe_list(dispatch_state.get("participantAgentIds"))
        if str(item or "").strip()
    ]
    return participant_agent_ids


def team_policy_payload(team):
    protocol = team_collaboration_protocol(team)
    return {
        "defaultDispatchMode": resolve_team_default_dispatch_mode(team),
        "defaultWakeScope": requested_team_wake_scope(team),
        "operatingBrief": team_operating_brief(team),
        "teamMemory": team_memory_text(team),
        "decisionLog": team_decision_log_text(team),
        "workingMemory": "",
        "workingMemoryUpdatedAt": "",
        "currentFocus": "",
        "currentFocusUpdatedAt": "",
        "currentFocusItems": [],
        "openLoops": [],
        "openLoopItems": [],
        "activeOwners": [],
        "taskLongTermMemory": {
            "longTermMemory": "",
            "learningHighlights": [],
            "recentNotes": [],
            "updatedAt": "",
        },
        "humanToneGuide": str(protocol.get("humanToneGuide") or "").strip(),
        "proactiveRules": clean_unique_strings(protocol.get("proactiveRules") or []),
        "coordinationProtocol": protocol,
    }


def merge_team_policy_state(team, existing_policy=None):
    base_policy = team_policy_payload(team if isinstance(team, dict) else {})
    existing_policy = existing_policy if isinstance(existing_policy, dict) else {}
    next_policy = {**base_policy, **existing_policy}
    next_policy["proactiveRules"] = clean_unique_strings(
        existing_policy.get("proactiveRules") if isinstance(existing_policy.get("proactiveRules"), list) else base_policy.get("proactiveRules")
    )
    next_policy["coordinationProtocol"] = (
        existing_policy.get("coordinationProtocol")
        if isinstance(existing_policy.get("coordinationProtocol"), dict)
        else base_policy.get("coordinationProtocol")
    )
    next_policy["taskLongTermMemory"] = compact_task_long_term_memory(existing_policy.get("taskLongTermMemory"))
    return next_policy


def build_team_current_focus(dispatch_state, limit=3):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    focus_lines = []
    response_entries = coordination_reply_entries(dispatch_state.get("responses"), limit=max(2, int(limit or 3) + 1))
    for item in response_entries:
        preview = summarize_task_execution_text(item.get("replyPreview"), limit=140)
        if preview and preview not in focus_lines:
            focus_lines.append(preview)
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_entries = coordination_reply_entries(
        relay.get("responses"),
        limit=max(1, int(limit or 3) - len(focus_lines) + 1),
    )
    for item in relay_entries:
        preview = summarize_task_execution_text(item.get("replyPreview"), limit=140)
        if preview and preview not in focus_lines:
            focus_lines.append(preview)
    if not focus_lines:
        focus_lines.extend(
            normalize_team_context_lines(
                build_team_working_memory(dispatch_state, limit=max(2, int(limit or 3))),
                limit=max(2, int(limit or 3)),
            )
        )
    collaboration = dispatch_state.get("collaboration") if isinstance(dispatch_state.get("collaboration"), dict) else {}
    headline = summarize_task_execution_text(collaboration.get("headline"), limit=120)
    if headline and headline not in focus_lines:
        focus_lines.insert(0, headline)
    return focus_lines[: max(1, int(limit or 3))]


def build_team_current_focus_items(dispatch_state, limit=3):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    items = []

    def push_item(text, owner_label="", agent_id=""):
        normalized_text = summarize_task_execution_text(text, limit=140)
        if not normalized_text:
            return
        for existing in items:
            if existing.get("text") == normalized_text:
                return
        items.append(
            {
                "text": normalized_text,
                "ownerLabel": str(owner_label or "").strip(),
                "agentId": str(agent_id or "").strip(),
            }
        )

    collaboration = dispatch_state.get("collaboration") if isinstance(dispatch_state.get("collaboration"), dict) else {}
    headline = summarize_task_execution_text(collaboration.get("headline"), limit=120)
    if headline:
        push_item(headline)
    response_entries = coordination_reply_entries(dispatch_state.get("responses"), limit=max(2, int(limit or 3) + 1))
    for item in response_entries:
        push_item(item.get("replyPreview"), item.get("agentDisplayName"), item.get("agentId"))
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_entries = coordination_reply_entries(
        relay.get("responses"),
        limit=max(1, int(limit or 3) - len(items) + 1),
    )
    for item in relay_entries:
        push_item(item.get("replyPreview"), item.get("agentDisplayName"), item.get("agentId"))
    return items[: max(1, int(limit or 3))]


def build_team_open_loops(dispatch_state, limit=4):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    collaboration = dispatch_state.get("collaboration") if isinstance(dispatch_state.get("collaboration"), dict) else {}
    agent_labels = dispatch_state_agent_labels(dispatch_state)
    loop_lines = []
    blocker_agent_ids = [
        str(item or "").strip()
        for item in safe_list(collaboration.get("blockerAgentIds"))
        if str(item or "").strip()
    ]
    for agent_id in blocker_agent_ids:
        label = agent_labels.get(agent_id) or agent_id
        line = f"{label} 这轮还卡在支援或依赖确认。"
        if line not in loop_lines:
            loop_lines.append(line)
        if len(loop_lines) >= max(1, int(limit or 4)):
            return loop_lines
    normalized_failures = []
    for item in safe_list(collaboration.get("failedAgentIds") or dispatch_state.get("failedAgents")):
        if isinstance(item, dict):
            agent_id = str(item.get("agentId") or "").strip()
        else:
            agent_id = str(item or "").strip()
        if agent_id and agent_id not in normalized_failures:
            normalized_failures.append(agent_id)
    for agent_id in normalized_failures:
        label = agent_labels.get(agent_id) or agent_id
        line = f"{label} 这轮暂时没有回包，需要稍后补齐。"
        if line not in loop_lines:
            loop_lines.append(line)
        if len(loop_lines) >= max(1, int(limit or 4)):
            return loop_lines
    waiting_agent_ids = [
        str(item or "").strip()
        for item in safe_list(collaboration.get("waitingAgentIds"))
        if str(item or "").strip()
    ]
    if not waiting_agent_ids:
        participant_agent_ids = [
            str(item or "").strip()
            for item in safe_list(dispatch_state.get("participantAgentIds"))
            if str(item or "").strip()
        ]
        responded_agent_ids = [
            str(item or "").strip()
            for item in safe_list(collaboration.get("respondedAgentIds"))
            if str(item or "").strip()
        ]
        if not responded_agent_ids:
            responded_agent_ids = [
                str(item.get("agentId") or "").strip()
                for item in safe_list(dispatch_state.get("responses"))
                if isinstance(item, dict) and str(item.get("agentId") or "").strip()
            ]
        waiting_agent_ids = [
            agent_id
            for agent_id in participant_agent_ids
            if agent_id and agent_id not in responded_agent_ids
        ]
    if waiting_agent_ids:
        waiting_labels = [agent_labels.get(agent_id) or agent_id for agent_id in waiting_agent_ids[:3]]
        if len(waiting_agent_ids) == 1:
            line = f"{waiting_labels[0]} 还在整理这轮回复。"
        else:
            line = f"{'、'.join(waiting_labels)} 等 {len(waiting_agent_ids)} 位同事还在整理这轮回复。"
        if line not in loop_lines:
            loop_lines.append(line)
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_sent = bool(relay.get("sent"))
    relay_reply_count = int(collaboration.get("relayReplyCount") or 0)
    relay_target_count = len(dispatch_state_relay_target_ids(dispatch_state))
    if relay_sent and relay_target_count > relay_reply_count:
        remaining = max(0, relay_target_count - relay_reply_count)
        if remaining:
            line = f"还有 {remaining} 位同事的补位意见还没收齐。"
            if line not in loop_lines:
                loop_lines.append(line)
    return loop_lines[: max(1, int(limit or 4))]


def build_team_open_loop_items(dispatch_state, limit=4):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    collaboration = dispatch_state.get("collaboration") if isinstance(dispatch_state.get("collaboration"), dict) else {}
    agent_labels = dispatch_state_agent_labels(dispatch_state)
    items = []

    def push_item(kind, text, owner_label="", agent_id=""):
        normalized_text = str(text or "").strip()
        if not normalized_text:
            return
        for existing in items:
            if existing.get("text") == normalized_text:
                return
        items.append(
            {
                "kind": str(kind or "").strip() or "follow_up",
                "text": normalized_text,
                "ownerLabel": str(owner_label or "").strip(),
                "agentId": str(agent_id or "").strip(),
            }
        )

    blocker_agent_ids = [
        str(item or "").strip()
        for item in safe_list(collaboration.get("blockerAgentIds"))
        if str(item or "").strip()
    ]
    for agent_id in blocker_agent_ids:
        label = agent_labels.get(agent_id) or agent_id
        push_item("blocked", f"{label} 这轮还卡在支援或依赖确认。", label, agent_id)
        if len(items) >= max(1, int(limit or 4)):
            return items

    normalized_failures = []
    for item in safe_list(collaboration.get("failedAgentIds") or dispatch_state.get("failedAgents")):
        agent_id = str(item.get("agentId") or "").strip() if isinstance(item, dict) else str(item or "").strip()
        if agent_id and agent_id not in normalized_failures:
            normalized_failures.append(agent_id)
    for agent_id in normalized_failures:
        label = agent_labels.get(agent_id) or agent_id
        push_item("missing", f"{label} 这轮暂时没有回包，需要稍后补齐。", label, agent_id)
        if len(items) >= max(1, int(limit or 4)):
            return items

    waiting_agent_ids = [
        str(item or "").strip()
        for item in safe_list(collaboration.get("waitingAgentIds"))
        if str(item or "").strip()
    ]
    if not waiting_agent_ids:
        participant_agent_ids = [
            str(item or "").strip()
            for item in safe_list(dispatch_state.get("participantAgentIds"))
            if str(item or "").strip()
        ]
        responded_agent_ids = [
            str(item or "").strip()
            for item in safe_list(collaboration.get("respondedAgentIds"))
            if str(item or "").strip()
        ]
        if not responded_agent_ids:
            responded_agent_ids = [
                str(item.get("agentId") or "").strip()
                for item in safe_list(dispatch_state.get("responses"))
                if isinstance(item, dict) and str(item.get("agentId") or "").strip()
            ]
        waiting_agent_ids = [
            agent_id
            for agent_id in participant_agent_ids
            if agent_id and agent_id not in responded_agent_ids
        ]
    for agent_id in waiting_agent_ids:
        label = agent_labels.get(agent_id) or agent_id
        push_item("waiting", f"{label} 还在整理这轮回复。", label, agent_id)
        if len(items) >= max(1, int(limit or 4)):
            return items

    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_sent = bool(relay.get("sent"))
    relay_reply_count = int(collaboration.get("relayReplyCount") or 0)
    relay_target_count = len(dispatch_state_relay_target_ids(dispatch_state))
    if relay_sent and relay_target_count > relay_reply_count:
        remaining = max(0, relay_target_count - relay_reply_count)
        if remaining:
            push_item("follow_up", f"还有 {remaining} 位同事的补位意见还没收齐。")
    return items[: max(1, int(limit or 4))]


def build_team_active_owners(current_focus_items, open_loop_items, limit=4):
    owners = []

    def push_item(owner_label="", agent_id="", source="focus"):
        normalized_owner = str(owner_label or "").strip()
        normalized_agent_id = str(agent_id or "").strip()
        if not normalized_owner and not normalized_agent_id:
            return
        lookup_key = normalized_agent_id or normalized_owner
        for existing in owners:
            existing_key = str(existing.get("agentId") or "").strip() or str(existing.get("ownerLabel") or "").strip()
            if existing_key != lookup_key:
                continue
            existing["sources"] = clean_unique_strings(list(existing.get("sources") or []) + [source])
            if normalized_owner and not str(existing.get("ownerLabel") or "").strip():
                existing["ownerLabel"] = normalized_owner
            if normalized_agent_id and not str(existing.get("agentId") or "").strip():
                existing["agentId"] = normalized_agent_id
            return
        owners.append(
            {
                "ownerLabel": normalized_owner or normalized_agent_id,
                "agentId": normalized_agent_id,
                "sources": [str(source or "").strip() or "focus"],
            }
        )

    for item in safe_list(current_focus_items):
        if not isinstance(item, dict):
            continue
        push_item(item.get("ownerLabel"), item.get("agentId"), "focus")
        if len(owners) >= max(1, int(limit or 4)):
            return owners
    for item in safe_list(open_loop_items):
        if not isinstance(item, dict):
            continue
        push_item(item.get("ownerLabel"), item.get("agentId"), "open_loop")
        if len(owners) >= max(1, int(limit or 4)):
            return owners
    return owners[: max(1, int(limit or 4))]


def team_state_packet_payload(team_policy):
    team_policy = team_policy if isinstance(team_policy, dict) else {}
    packet = {}
    task_long_term_memory = compact_task_long_term_memory(team_policy.get("taskLongTermMemory"))
    current_focus_items = [
        {
            "text": str(item.get("text") or "").strip(),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
            "agentId": str(item.get("agentId") or "").strip(),
        }
        for item in safe_list(team_policy.get("currentFocusItems"))
        if isinstance(item, dict) and str(item.get("text") or "").strip()
    ][:2]
    open_loop_items = [
        {
            "kind": str(item.get("kind") or "").strip() or "follow_up",
            "text": str(item.get("text") or "").strip(),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
            "agentId": str(item.get("agentId") or "").strip(),
        }
        for item in safe_list(team_policy.get("openLoopItems"))
        if isinstance(item, dict) and str(item.get("text") or "").strip()
    ][:2]
    active_owners = [
        {
            "ownerLabel": str(item.get("ownerLabel") or item.get("agentId") or "").strip(),
            "agentId": str(item.get("agentId") or "").strip(),
            "sources": clean_unique_strings(item.get("sources") or []),
        }
        for item in safe_list(team_policy.get("activeOwners"))
        if isinstance(item, dict) and str(item.get("ownerLabel") or item.get("agentId") or "").strip()
    ][:3]
    if current_focus_items:
        packet["currentFocus"] = current_focus_items
    if open_loop_items:
        packet["openLoops"] = open_loop_items
    if active_owners:
        packet["activeOwners"] = active_owners
    if task_long_term_memory.get("longTermMemory"):
        packet["taskLongTermMemory"] = {
            "longTermMemory": task_long_term_memory.get("longTermMemory"),
            "learningHighlights": clean_unique_strings(task_long_term_memory.get("learningHighlights") or [])[:2],
        }
    return packet


def agent_runtime_overrides(metadata):
    overrides = metadata.get("agentOverrides") if isinstance(metadata, dict) else {}
    return overrides if isinstance(overrides, dict) else {}


def normalize_team_runtime_mode(value):
    mode = str(value or "").strip().lower()
    if mode in TEAM_RUNTIME_MODES:
        return mode
    return ""


def normalize_team_runtime_every(value):
    raw = str(value or "").strip().lower()
    if not raw:
        return TEAM_RUNTIME_DEFAULT_EVERY
    if re.fullmatch(r"[1-9]\d*[mh]", raw):
        return raw
    return TEAM_RUNTIME_DEFAULT_EVERY


def team_runtime_meta(team):
    return (team.get("meta") if isinstance(team.get("meta"), dict) else {}) if isinstance(team, dict) else {}


def team_collaboration_profile_key(team):
    team = team if isinstance(team, dict) else {}
    values = " ".join(
        [
            str(team.get("id") or ""),
            str(team.get("name") or ""),
            str(team.get("focus") or ""),
            str(team.get("description") or ""),
        ]
    ).lower()
    if any(token in values for token in ("release", "qa", "验收", "发布", "quality", "测试")):
        return "release"
    if any(token in values for token in ("delivery", "engineering", "交付", "开发", "实施", "build")):
        return "delivery"
    if any(token in values for token in ("signals", "brief", "marketing", "文档", "运营", "对外", "signal")):
        return "signals"
    if any(token in values for token in ("core", "指挥", "strategy", "compliance", "founder", "决策", "产品", "项目", "program", "project", "pm")):
        return "core"
    return "fallback"


def team_collaboration_protocol(team):
    team = team if isinstance(team, dict) else {}
    meta = team_runtime_meta(team)
    configured = meta.get("coordinationProtocol") if isinstance(meta.get("coordinationProtocol"), dict) else {}
    defaults = deepcopy(TEAM_COLLABORATION_PROFILE_DEFAULTS.get(team_collaboration_profile_key(team), TEAM_COLLABORATION_PROFILE_DEFAULTS["fallback"]))
    return {
        "profile": str(configured.get("profile") or defaults.get("profile") or team_collaboration_profile_key(team)).strip(),
        "humanToneGuide": str(
            configured.get("humanToneGuide")
            or meta.get("humanToneGuide")
            or team.get("humanToneGuide")
            or defaults.get("humanToneGuide")
            or ""
        ).strip(),
        "leadChecklist": clean_unique_strings(configured.get("leadChecklist") or defaults.get("leadChecklist") or []),
        "memberChecklist": clean_unique_strings(configured.get("memberChecklist") or defaults.get("memberChecklist") or []),
        "proactiveRules": clean_unique_strings(
            configured.get("proactiveRules")
            or meta.get("proactiveRules")
            or team.get("proactiveRules")
            or defaults.get("proactiveRules")
            or []
        ),
        "updateContract": str(
            configured.get("updateContract")
            or meta.get("updateContract")
            or defaults.get("updateContract")
            or ""
        ).strip(),
        "escalationRule": str(
            configured.get("escalationRule")
            or meta.get("escalationRule")
            or defaults.get("escalationRule")
            or ""
        ).strip(),
    }


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


def requested_team_runtime_mode(team):
    return normalize_team_runtime_mode(team_runtime_meta(team).get("runtimeMode"))


def requested_team_runtime_every(team):
    return normalize_team_runtime_every(team_runtime_meta(team).get("runtimeEvery"))


def team_heartbeat_prompt(team_name, agent_id, is_lead=False):
    role_hint = "Lead the team queue" if is_lead else "Check your team lane"
    return f"{role_hint} for {team_name or agent_id}. If no active team work is pending, reply HEARTBEAT_OK."


def team_wake_prompt(team_name, scope="lead"):
    normalized_scope = str(scope or "").strip().lower()
    if normalized_scope == "all":
        return f"Team wake check for {team_name or 'this team'}. Reply with your current lane status and the next action in one sentence."
    return f"Lead wake check for {team_name or 'this team'}. Reply with your current lane status and the next action in one sentence."


def team_member_status_group(value):
    status = str(value or "").strip().lower()
    if status in {"blocked", "error", "failed"}:
        return "blocked"
    if status in {"active", "running", "online"}:
        return "active"
    if status in {"standby"}:
        return "standby"
    if status in {"idle", "offline"}:
        return "idle"
    return "unknown"


def infer_team_runtime_state(team, members, active_tasks, blocked_tasks, heartbeat_enabled_ids):
    team_status = str((team or {}).get("status") or "").strip().lower()
    member_groups = [team_member_status_group((item or {}).get("status")) for item in safe_list(members)]
    if team_status in {"paused", "archived"}:
        return "paused"
    if int(blocked_tasks or 0) > 0 or "blocked" in member_groups:
        return "blocked"
    if int(active_tasks or 0) > 0 or "active" in member_groups:
        return "active"
    if heartbeat_enabled_ids:
        return "standby"
    if "idle" in member_groups or "standby" in member_groups:
        return "idle"
    return "quiet"


def requested_team_wake_scope(team):
    configured_scope = normalize_team_wake_scope(team_runtime_meta(team).get("defaultWakeScope"))
    if configured_scope:
        return configured_scope
    return "all" if requested_team_runtime_mode(team) == "all_standby" else "lead"


def resolve_agent_team_wake_targets(team, scope=""):
    normalized_scope = str(scope or "").strip().lower()
    if normalized_scope not in {"lead", "all"}:
        normalized_scope = requested_team_wake_scope(team)
    lead_agent_id = str((team or {}).get("leadAgentId") or "").strip()
    members = [
        str(item or "").strip()
        for item in safe_list((team or {}).get("memberAgentIds"))
        if str(item or "").strip()
    ]
    if normalized_scope == "all":
        targets = members or ([lead_agent_id] if lead_agent_id else [])
    else:
        targets = [lead_agent_id] if lead_agent_id else (members[:1] if members else [])
    deduped = []
    seen = set()
    for agent_id in targets:
        if not agent_id or agent_id in seen:
            continue
        seen.add(agent_id)
        deduped.append(agent_id)
    return {"scope": normalized_scope, "agentIds": deduped}


def infer_effective_team_runtime_mode(team, enabled_agent_ids):
    team = team or {}
    members = [str(item or "").strip() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()]
    enabled = {agent_id for agent_id in enabled_agent_ids if agent_id in members}
    lead_agent_id = str(team.get("leadAgentId") or "").strip()
    if not enabled:
        return "quiet"
    if members and len(enabled) == len(set(members)):
        return "all_standby"
    if lead_agent_id and enabled == {lead_agent_id}:
        return "lead_standby"
    return "custom"


def heartbeat_config_enabled(heartbeat):
    if not isinstance(heartbeat, dict) or not heartbeat:
        return False
    if "enabled" in heartbeat:
        return normalize_flag(heartbeat.get("enabled"), default=True)
    return True


def save_agent_team_preserving_meta(openclaw_dir, payload, existing=None):
    base_meta = team_runtime_meta(existing)
    next_meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    merged = {**base_meta, **next_meta}
    return store_save_agent_team(openclaw_dir, {**payload, "meta": merged})


def apply_agent_team_runtime_policy(openclaw_dir, team_id, runtime_mode, runtime_every="", restart_gateway=False):
    team = resolve_agent_team_record(openclaw_dir, team_id)
    if not team:
        raise RuntimeError("Agent Team 不存在。")
    normalized_mode = normalize_team_runtime_mode(runtime_mode)
    if normalized_mode not in {"quiet", "lead_standby", "all_standby"}:
        raise RuntimeError("暂不支持该 Team 运行模式。")
    normalized_every = normalize_team_runtime_every(runtime_every)
    updated_team = save_agent_team_preserving_meta(
        openclaw_dir,
        {
            "id": team.get("id", ""),
            "name": team.get("name", ""),
            "status": team.get("status", "active"),
            "leadAgentId": team.get("leadAgentId", ""),
            "memberAgentIds": safe_list(team.get("memberAgentIds")),
            "description": team.get("description", ""),
            "focus": team.get("focus", ""),
            "channel": team.get("channel", "internal"),
            "linkedTaskIds": safe_list(team.get("linkedTaskIds")),
            "meta": {
                "runtimeMode": normalized_mode,
                "runtimeEvery": normalized_every,
                "runtimeUpdatedAt": now_iso(),
                "runtimeManagedBy": "mission-control",
            },
        },
        existing=team,
    )
    config = load_config(openclaw_dir)
    config_agents = ((config.get("agents", {}) if isinstance(config, dict) else {}) or {}).get("list", [])
    all_teams = store_list_agent_teams(openclaw_dir)
    managed_agents = set()
    desired_heartbeats = {}
    for current_team in all_teams:
        current_mode = requested_team_runtime_mode(current_team)
        if current_mode not in {"quiet", "lead_standby", "all_standby"}:
            continue
        every = requested_team_runtime_every(current_team)
        lead_agent_id = str(current_team.get("leadAgentId") or "").strip()
        members = [str(item or "").strip() for item in safe_list(current_team.get("memberAgentIds")) if str(item or "").strip()]
        team_name = str(current_team.get("name") or "").strip()
        for agent_id in members:
            managed_agents.add(agent_id)
            if current_mode == "quiet":
                continue
            if current_mode == "lead_standby" and agent_id != lead_agent_id:
                continue
            desired = desired_heartbeats.get(agent_id)
            next_rank = 2 if current_mode == "all_standby" else 1
            if desired and desired.get("_rank", 0) > next_rank:
                continue
            desired_heartbeats[agent_id] = {
                "_rank": next_rank,
                "every": every,
                "target": "none",
                "lightContext": True,
                "directPolicy": "allow",
                "prompt": team_heartbeat_prompt(team_name, agent_id, is_lead=(agent_id == lead_agent_id)),
            }
    applied_agents = []
    cleared_agents = []
    for agent in config_agents if isinstance(config_agents, list) else []:
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id or agent_id not in managed_agents:
            continue
        desired = desired_heartbeats.get(agent_id)
        if desired:
            next_heartbeat = {key: value for key, value in desired.items() if key != "_rank"}
            if agent.get("heartbeat") != next_heartbeat:
                agent["heartbeat"] = next_heartbeat
            applied_agents.append(agent_id)
        else:
            if "heartbeat" in agent:
                agent.pop("heartbeat", None)
            cleared_agents.append(agent_id)
    save_config(openclaw_dir, config)
    gateway_result = None
    if restart_gateway:
        gateway_result = perform_gateway_service_action(openclaw_dir, "restart")
    return {
        "team": updated_team,
        "runtimeMode": normalized_mode,
        "runtimeEvery": normalized_every,
        "appliedAgents": applied_agents,
        "clearedAgents": cleared_agents,
        "gateway": gateway_result,
    }


def sync_requested_agent_team_runtime_policies(openclaw_dir, teams=None):
    current_teams = safe_list(teams) if teams is not None else store_list_agent_teams(openclaw_dir)
    local_config = load_config(openclaw_dir)
    config_agent_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list"))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    applied = []
    for team in current_teams:
        if not isinstance(team, dict):
            continue
        team_id = str(team.get("id") or "").strip()
        if not team_id:
            continue
        requested_mode = requested_team_runtime_mode(team)
        if requested_mode not in {"quiet", "lead_standby", "all_standby"}:
            continue
        requested_every = requested_team_runtime_every(team)
        member_ids = [str(item or "").strip() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()]
        enabled_agent_ids = []
        heartbeat_every_values = []
        for agent_id in member_ids:
            config_agent = config_agent_map.get(agent_id, {})
            heartbeat = config_agent.get("heartbeat") if isinstance(config_agent.get("heartbeat"), dict) else {}
            if heartbeat_config_enabled(heartbeat):
                enabled_agent_ids.append(agent_id)
                every = str(heartbeat.get("every") or "").strip()
                if every and every not in heartbeat_every_values:
                    heartbeat_every_values.append(every)
        effective_mode = infer_effective_team_runtime_mode(team, enabled_agent_ids)
        current_every = heartbeat_every_values[0] if len(heartbeat_every_values) == 1 else requested_every
        if effective_mode == requested_mode and current_every == requested_every:
            continue
        result = apply_agent_team_runtime_policy(
            openclaw_dir,
            team_id=team_id,
            runtime_mode=requested_mode,
            runtime_every=requested_every,
            restart_gateway=False,
        )
        applied.append(
            {
                "teamId": team_id,
                "runtimeMode": result.get("runtimeMode", ""),
                "runtimeEvery": result.get("runtimeEvery", ""),
                "appliedAgents": safe_list(result.get("appliedAgents")),
                "clearedAgents": safe_list(result.get("clearedAgents")),
            }
        )
        local_config = load_config(openclaw_dir)
        config_agent_map = {
            str(item.get("id") or "").strip(): item
            for item in safe_list(((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list"))
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        }
    return applied


def perform_agent_team_wake(openclaw_dir, team_id, scope="", message_text=""):
    team = resolve_agent_team_record(openclaw_dir, team_id)
    if not team:
        raise RuntimeError("Agent Team 不存在。")
    wake_targets = resolve_agent_team_wake_targets(team, scope)
    target_agent_ids = safe_list(wake_targets.get("agentIds"))
    if not target_agent_ids:
        raise RuntimeError("当前 Team 还没有可唤醒的成员。")
    team_name = str(team.get("name") or team_id).strip()
    normalized_scope = wake_targets.get("scope") or requested_team_wake_scope(team)
    wake_message = str(message_text or "").strip() or team_wake_prompt(team_name, normalized_scope)
    gateway = ensure_gateway_ready_for_team_wake(openclaw_dir)
    wake_agent_timeout_seconds = 18

    def send_wake(index, agent_id):
        try:
            result = perform_conversation_send(
                openclaw_dir,
                agent_id=agent_id,
                message=wake_message,
                thinking="low",
                agent_timeout_seconds=wake_agent_timeout_seconds,
            )
        except Exception as error:
            return (
                index,
                False,
                {"agentId": agent_id, "error": str(error or "唤醒失败。").strip()},
            )
        payloads = (result.get("result", {}) or {}).get("payloads", []) or []
        reply_text = payloads[0].get("text", "") if payloads and isinstance(payloads[0], dict) else ""
        agent_meta = (((result.get("result", {}) or {}).get("meta", {}) or {}).get("agentMeta", {}) or {})
        return (
            index,
            True,
            {
                "agentId": agent_id,
                "sessionId": str(agent_meta.get("sessionId") or "main").strip() or "main",
                "replyPreview": reply_text,
            },
        )

    ordered_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, min(4, len(target_agent_ids)))) as executor:
        futures = [executor.submit(send_wake, index, agent_id) for index, agent_id in enumerate(target_agent_ids)]
        for future in concurrent.futures.as_completed(futures):
            ordered_results.append(future.result())
    ordered_results.sort(key=lambda item: item[0])

    responses = [payload for _index, ok, payload in ordered_results if ok]
    failures = [payload for _index, ok, payload in ordered_results if not ok]
    wake_status = "ok" if responses and not failures else ("partial" if responses else "error")
    wake_meta = {
        "at": now_iso(),
        "scope": normalized_scope,
        "status": wake_status,
        "targetAgentIds": target_agent_ids,
        "respondedAgentIds": [item.get("agentId") for item in responses if item.get("agentId")],
        "responses": responses,
        "failedAgents": failures,
        "message": wake_message,
        "gatewayAction": gateway.get("action", ""),
        "deliveryMode": "parallel",
        "agentTimeoutSeconds": wake_agent_timeout_seconds,
    }
    updated_team = save_agent_team_preserving_meta(
        openclaw_dir,
        {
            "id": team.get("id", ""),
            "name": team.get("name", ""),
            "status": team.get("status", "active"),
            "leadAgentId": team.get("leadAgentId", ""),
            "memberAgentIds": safe_list(team.get("memberAgentIds")),
            "description": team.get("description", ""),
            "focus": team.get("focus", ""),
            "channel": team.get("channel", "internal"),
            "linkedTaskIds": safe_list(team.get("linkedTaskIds")),
            "meta": {
                "runtimeWake": wake_meta,
                "runtimeWakeAt": wake_meta["at"],
                "runtimeManagedBy": "mission-control",
            },
        },
        existing=team,
    )
    return {
        "team": updated_team,
        "scope": normalized_scope,
        "message": wake_message,
        "targetAgentIds": target_agent_ids,
        "responses": responses,
        "failures": failures,
        "status": wake_status,
        "gateway": gateway,
        "deliveryMode": "parallel",
        "agentTimeoutSeconds": wake_agent_timeout_seconds,
    }


def orchestration_slug(value):
    text = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or "").strip())
    while "--" in text:
        text = text.replace("--", "-")
    return text.strip("-")


def resolve_agent_team_record(openclaw_dir, team_id):
    normalized_team_id = str(team_id or "").strip()
    if not normalized_team_id:
        return None
    return next((item for item in store_list_agent_teams(openclaw_dir) if item.get("id") == normalized_team_id), None)


def task_team_selection_fragments(title="", remark="", intelligence=None, workflow_binding=None):
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    selected_branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
    return [
        str(title or ""),
        str(remark or ""),
        str(intelligence.get("category") or ""),
        str(intelligence.get("categoryLabel") or ""),
        " ".join(str(item or "") for item in safe_list(intelligence.get("matchedKeywords"))),
        " ".join(str(item or "") for item in safe_list(intelligence.get("laneHints"))),
        str(selected_branch.get("targetLaneId") or ""),
        str(selected_branch.get("targetLaneTitle") or ""),
        str(selected_branch.get("targetNodeTitle") or ""),
    ]


def task_team_selection_tokens(text_fragments):
    tokens = []
    for fragment in safe_list(text_fragments):
        for raw_token in re.findall(r"[a-z0-9_]+|[\u4e00-\u9fff]{2,}", str(fragment or "").lower()):
            token = str(raw_token or "").strip()
            if not token or token in TEAM_SELECTION_STOPWORDS:
                continue
            if token.isascii() and len(token) < 3:
                continue
            if token not in tokens:
                tokens.append(token)
    return tokens[:24]


def team_selection_text(team, agent_map=None):
    team = team if isinstance(team, dict) else {}
    agent_map = agent_map if isinstance(agent_map, dict) else {}
    lead_agent_id = str(team.get("leadAgentId") or "").strip()
    member_agent_ids = [str(item or "").strip() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()]
    parts = [
        str(team.get("id") or "").strip(),
        str(team.get("name") or "").strip(),
        str(team.get("focus") or "").strip(),
        str(team.get("description") or "").strip(),
        str(team.get("operatingBrief") or "").strip(),
        team_memory_text(team),
        team_decision_log_text(team),
    ]
    for agent_id in [lead_agent_id, *member_agent_ids]:
        if not agent_id:
            continue
        parts.append(agent_id)
        agent = agent_map.get(agent_id, {})
        identity = agent.get("identity") if isinstance(agent.get("identity"), dict) else {}
        parts.extend(
            [
                str(agent.get("title") or "").strip(),
                str(agent.get("name") or "").strip(),
                str(agent.get("role") or "").strip(),
                str(identity.get("name") or "").strip(),
                str(identity.get("title") or "").strip(),
            ]
        )
        parts.extend(str(item or "").strip() for item in safe_list(agent.get("skills")))
    return " ".join(part for part in parts if part).lower()


def summarize_team_workload(task_index):
    summary = defaultdict(lambda: {"activeCount": 0, "blockedCount": 0})
    for task in safe_list(task_index):
        if not isinstance(task, dict):
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES or task.get("output"):
            continue
        route_meta = {}
        if isinstance(task.get("meta"), dict) and isinstance((task.get("meta") or {}).get("routeDecision"), dict):
            route_meta = (task.get("meta") or {}).get("routeDecision", {})
        elif isinstance(task.get("routeDecision"), dict):
            route_meta = task.get("routeDecision", {})
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = str(task.get("teamId") or team_assignment.get("teamId") or route_meta.get("teamId") or "").strip()
        if not team_id:
            continue
        summary[team_id]["activeCount"] += 1
        if state == "blocked":
            summary[team_id]["blockedCount"] += 1
    return summary


def summarize_team_override_preferences(routing_decisions):
    summary = defaultdict(lambda: {"count": 0, "recommendedFrom": Counter(), "reasonSamples": []})
    for item in safe_list(routing_decisions):
        if not isinstance(item, dict):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        team_selection = meta.get("teamSelection") if isinstance(meta.get("teamSelection"), dict) else {}
        intelligence = meta.get("intelligence") if isinstance(meta.get("intelligence"), dict) else {}
        category = str(intelligence.get("category") or "").strip().lower()
        selected_team_id = str(team_selection.get("selectedTeamId") or "").strip()
        recommended_team_id = str(team_selection.get("recommendedTeamId") or "").strip()
        override_reason = str(team_selection.get("overrideReason") or "").strip()
        if not category or not selected_team_id or not recommended_team_id or not override_reason:
            continue
        if selected_team_id == recommended_team_id:
            continue
        bucket = summary[(category, selected_team_id)]
        bucket["count"] += 1
        bucket["recommendedFrom"][recommended_team_id] += 1
        if override_reason and override_reason not in bucket["reasonSamples"]:
            bucket["reasonSamples"].append(override_reason[:96])
    normalized = {}
    for key, bucket in summary.items():
        recommended_from = bucket["recommendedFrom"].most_common(1)[0][0] if bucket["recommendedFrom"] else ""
        normalized[key] = {
            "count": int(bucket["count"] or 0),
            "recommendedFrom": recommended_from,
            "reasonSamples": bucket["reasonSamples"][:2],
        }
    return normalized


def summarize_team_outcome_history(task_index, routing_decisions):
    decision_by_task = {}
    for item in safe_list(routing_decisions):
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("taskId") or "").strip()
        if task_id and task_id not in decision_by_task:
            decision_by_task[task_id] = item

    summary = defaultdict(
        lambda: {
            "evaluatedCount": 0,
            "completedCount": 0,
            "blockedCount": 0,
            "activeCount": 0,
            "completionMinutesTotal": 0.0,
            "completionMinutesCount": 0,
        }
    )
    for task in safe_list(task_index):
        if not isinstance(task, dict):
            continue
        task_id = str(task.get("id") or "").strip()
        route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        if not route_meta and isinstance(task.get("meta"), dict):
            meta_route = (task.get("meta") or {}).get("routeDecision")
            if isinstance(meta_route, dict):
                route_meta = meta_route
        team_selection = route_meta.get("teamSelection") if isinstance(route_meta.get("teamSelection"), dict) else {}
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = str(
            team_selection.get("selectedTeamId")
            or team_assignment.get("teamId")
            or task.get("teamId")
            or ""
        ).strip()
        if not team_id:
            continue

        decision = decision_by_task.get(task_id, {})
        decision_meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
        intelligence = route_meta.get("intelligence") if isinstance(route_meta.get("intelligence"), dict) else {}
        if not intelligence and isinstance(decision_meta.get("intelligence"), dict):
            intelligence = decision_meta.get("intelligence", {})
        category = str(intelligence.get("category") or "").strip().lower()
        if not category:
            continue

        outcome = route_meta.get("outcome") if isinstance(route_meta.get("outcome"), dict) else {}
        if not outcome and isinstance(decision_meta.get("outcome"), dict):
            outcome = decision_meta.get("outcome", {})
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        status = str(
            outcome.get("status")
            or ("blocked" if task.get("blocked") else ("completed" if state in TERMINAL_STATES else "active"))
        ).strip().lower()
        bucket = summary[(category, team_id)]
        if status in {"completed", "blocked"}:
            bucket["evaluatedCount"] += 1
        else:
            bucket["activeCount"] += 1
        if status == "completed":
            bucket["completedCount"] += 1
            completion_minutes = float(outcome.get("completionMinutes") or 0)
            if completion_minutes > 0:
                bucket["completionMinutesTotal"] += completion_minutes
                bucket["completionMinutesCount"] += 1
        elif status == "blocked":
            bucket["blockedCount"] += 1

    normalized = {}
    for key, bucket in summary.items():
        evaluated = int(bucket["evaluatedCount"] or 0)
        completed = int(bucket["completedCount"] or 0)
        blocked = int(bucket["blockedCount"] or 0)
        completion_minutes_count = int(bucket["completionMinutesCount"] or 0)
        normalized[key] = {
            "evaluatedCount": evaluated,
            "completedCount": completed,
            "blockedCount": blocked,
            "activeCount": int(bucket["activeCount"] or 0),
            "completionRate": int(round((completed / max(evaluated, 1)) * 100)) if evaluated else 0,
            "blockRate": int(round((blocked / max(evaluated, 1)) * 100)) if evaluated else 0,
            "avgCompletionMinutes": round(bucket["completionMinutesTotal"] / completion_minutes_count, 1)
            if completion_minutes_count
            else 0.0,
        }
    return normalized


def score_task_team_candidate(
    team,
    target_agent_id="",
    preferred_team_id="",
    title="",
    remark="",
    intelligence=None,
    workflow_binding=None,
    agents=None,
    task_index=None,
    override_history=None,
    outcome_history=None,
):
    team = team if isinstance(team, dict) else {}
    intelligence = intelligence if isinstance(intelligence, dict) else {}
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    override_history = override_history if isinstance(override_history, dict) else {}
    outcome_history = outcome_history if isinstance(outcome_history, dict) else {}
    agent_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(agents)
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    workload_map = summarize_team_workload(task_index)
    team_id = str(team.get("id") or "").strip()
    lead_agent_id = str(team.get("leadAgentId") or "").strip()
    member_agent_ids = [str(item or "").strip() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()]
    candidate_ids = []
    for agent_id in [lead_agent_id, *member_agent_ids]:
        if agent_id and agent_id not in candidate_ids:
            candidate_ids.append(agent_id)
    team_text = team_selection_text(team, agent_map=agent_map)
    query_fragments = task_team_selection_fragments(title=title, remark=remark, intelligence=intelligence, workflow_binding=workflow_binding)
    query_text = " ".join(str(fragment or "").strip() for fragment in query_fragments if str(fragment or "").strip()).lower()
    query_tokens = task_team_selection_tokens(query_fragments)
    reasons = []
    score = 0

    def apply(points, reason=""):
        nonlocal score
        if not points:
            return
        score += points
        if reason and reason not in reasons:
            reasons.append(reason)

    normalized_preferred_team_id = str(preferred_team_id or "").strip()
    normalized_target_agent_id = str(target_agent_id or "").strip()
    status = str(team.get("status") or "active").strip().lower()
    is_preferred = bool(normalized_preferred_team_id and team_id == normalized_preferred_team_id)
    if is_preferred:
        apply(1000, "用户显式指定了这个团队。")
    elif status == "active":
        apply(12, "团队当前处于 active 状态。")
    elif status == "paused":
        apply(-60, "团队当前处于 paused 状态。")
    elif status == "archived":
        apply(-120, "团队已经 archived，不适合作为自动分配目标。")

    category = str(intelligence.get("category") or "").strip().lower()
    prioritized_team_ids = TASK_CATEGORY_TEAM_PRIORITY.get(category, ())
    is_primary_team_for_category = bool(prioritized_team_ids and team_id == prioritized_team_ids[0])
    if team_id in prioritized_team_ids:
        index = prioritized_team_ids.index(team_id)
        apply(max(0, 52 - index * 20), f"团队职责和任务类别 {category or intelligence.get('categoryLabel') or 'unknown'} 更匹配。")

    all_hands_hits = detect_all_hands_task_tokens(query_text)
    if all_hands_hits:
        if team_id == "team-company":
            all_hands_team_name = str(team.get("name") or "全员协作组").strip() or "全员协作组"
            apply(
                min(168, 96 + len(all_hands_hits) * 18),
                f"任务明确要求全员协同，适合落到 {all_hands_team_name}。命中：{'、'.join(all_hands_hits[:4])}。",
            )
        elif team_id in {"team-core", "team-delivery", "team-release", "team-signals"} and not is_preferred:
            apply(-42, "任务更像全员联动场景，单一职能 Team 不是最优默认入口。")

    historical_override = override_history.get((category, team_id), {}) if category and team_id else {}
    historical_override_count = int(historical_override.get("count") or 0)
    if historical_override_count >= 2 and not is_preferred:
        if historical_override_count >= 3:
            learning_points = min(220, 76 + historical_override_count * 48)
        else:
            learning_points = min(92, 28 + historical_override_count * 24)
        recommended_from = str(historical_override.get("recommendedFrom") or "").strip()
        learning_reason = f"历史上有 {historical_override_count} 条 {category or intelligence.get('categoryLabel') or '同类'} 任务被人工改派到该团队。"
        if recommended_from:
            learning_reason = f"{learning_reason} 常见改派来源为 {recommended_from}。"
        apply(learning_points, learning_reason)

    historical_outcome = outcome_history.get((category, team_id), {}) if category and team_id else {}
    historical_outcome_count = int(historical_outcome.get("evaluatedCount") or 0)
    historical_completion_count = int(historical_outcome.get("completedCount") or 0)
    historical_blocked_count = int(historical_outcome.get("blockedCount") or 0)
    historical_completion_rate = int(historical_outcome.get("completionRate") or 0)
    historical_block_rate = int(historical_outcome.get("blockRate") or 0)
    historical_avg_completion_minutes = float(historical_outcome.get("avgCompletionMinutes") or 0)
    if historical_outcome_count >= 2 and not is_preferred:
        if historical_completion_rate >= 70 and historical_block_rate <= 25:
            if historical_outcome_count >= 3:
                performance_points = min(168, 48 + historical_completion_count * 20 + max(0, historical_completion_rate - 60))
            else:
                performance_points = min(76, 24 + historical_completion_count * 12 + max(0, historical_completion_rate - 60) // 2)
            performance_reason = f"同类任务近 {historical_outcome_count} 单收口 {historical_completion_rate}%，这支团队在这类任务上更稳定。"
            if historical_avg_completion_minutes > 0:
                performance_reason = (
                    f"同类任务近 {historical_outcome_count} 单收口 {historical_completion_rate}%，"
                    f"平均收口 {historical_avg_completion_minutes:.1f} 分钟。"
                )
            apply(performance_points, performance_reason)
        elif historical_block_rate >= 50:
            if historical_outcome_count >= 3:
                performance_penalty = min(124, 28 + historical_blocked_count * 20 + max(0, historical_block_rate - 40))
            else:
                performance_penalty = min(64, 16 + historical_blocked_count * 10 + max(0, historical_block_rate - 40) // 2)
            apply(performance_penalty * -1, f"同类任务近 {historical_outcome_count} 单阻塞 {historical_block_rate}%，自动分配要更谨慎。")
        elif historical_completion_rate > historical_block_rate:
            performance_points = min(22, 6 + historical_completion_count * 4)
            apply(performance_points, f"同类任务近 {historical_outcome_count} 单收口 {historical_completion_rate}%，可复用这支团队的经验。")
        elif historical_blocked_count > historical_completion_count:
            performance_penalty = min(18, 6 + historical_blocked_count * 4)
            apply(performance_penalty * -1, f"同类任务近 {historical_outcome_count} 单阻塞 {historical_block_rate}%，历史表现偏弱。")

    semantic_preferences = task_semantic_agent_preferences(intelligence)
    target_alignment_rank = (
        semantic_preferences.index(normalized_target_agent_id)
        if normalized_target_agent_id and normalized_target_agent_id in semantic_preferences
        else -1
    )
    target_alignment_multiplier = 1.0
    if category in {"release", "quality"} and not is_primary_team_for_category:
        target_alignment_multiplier = 0.2

    def scaled_alignment_points(points):
        return int(round(points * target_alignment_multiplier))

    if normalized_target_agent_id:
        if normalized_target_agent_id == lead_agent_id:
            if target_alignment_rank >= 0:
                apply(scaled_alignment_points(104), "团队 lead 同时覆盖当前任务目标 Agent 与任务语义偏好。")
            else:
                apply(scaled_alignment_points(76), "团队 lead 覆盖当前任务目标 Agent，可减少再次转派成本。")
        elif normalized_target_agent_id in candidate_ids:
            if target_alignment_rank >= 0:
                apply(scaled_alignment_points(92), "团队成员同时覆盖当前任务目标 Agent 与任务语义偏好。")
            else:
                apply(scaled_alignment_points(64), "团队成员覆盖当前任务目标 Agent，可减少再次转派成本。")

    for index, preferred_agent_id in enumerate(semantic_preferences):
        if preferred_agent_id not in candidate_ids:
            continue
        apply(max(0, 42 - index * 10) + (8 if preferred_agent_id == lead_agent_id else 0), f"团队成员覆盖任务语义偏好的角色 {preferred_agent_id}。")
        break

    for hint in safe_list(intelligence.get("laneHints")):
        normalized_hint = str(hint or "").strip().lower()
        lane_priority = LANE_HINT_AGENT_PRIORITY.get(normalized_hint, ())
        if any(agent_id in candidate_ids for agent_id in lane_priority):
            apply(24, f"团队成员覆盖工作流泳道提示 {normalized_hint}。")

    member_token_score = sum(min(semantic_agent_token_score(agent_id, query_text), 2) for agent_id in candidate_ids)
    if member_token_score:
        apply(min(member_token_score * 8, 24), "团队成员能力标签与任务语义有直接命中。")

    matched_tokens = [token for token in query_tokens if token in team_text]
    if matched_tokens:
        apply(min(len(matched_tokens) * 4, 20), f"团队职责文本命中任务关键词：{'、'.join(matched_tokens[:4])}。")

    specialism = TEAM_SPECIALISM_TOKEN_GROUPS.get(team_id, {})
    specialism_hits = [
        token
        for token in safe_list(specialism.get("tokens"))
        if token and token in query_text
    ]
    if specialism_hits:
        reason = str(specialism.get("reason") or "").strip() or f"任务描述命中 {team_id} 的岗位关键词。"
        apply(
            min(56, 20 + len(specialism_hits) * 10),
            f"{reason} 命中：{'、'.join(specialism_hits[:4])}。",
        )

    workload = workload_map.get(team_id, {})
    active_count = int(workload.get("activeCount") or 0)
    blocked_count = int(workload.get("blockedCount") or 0)
    if not active_count and not blocked_count:
        apply(8, "团队当前在手负载较轻。")
    else:
        apply(-min(active_count * 6 + blocked_count * 12, 40), f"团队当前在手 {active_count} 条任务，阻塞 {blocked_count} 条。")

    if not candidate_ids:
        apply(-80, "团队没有可用成员。")

    return {
        "team": deepcopy(team),
        "teamId": team_id,
        "teamName": str(team.get("name") or team_id).strip(),
        "leadAgentId": lead_agent_id,
        "memberAgentIds": candidate_ids,
        "score": score,
        "preferred": is_preferred,
        "status": status,
        "reasons": reasons[:6],
        "workload": {"activeCount": active_count, "blockedCount": blocked_count},
        "history": {
            "overrideCount": historical_override_count,
            "recommendedFrom": str(historical_override.get("recommendedFrom") or "").strip(),
            "reasonSamples": safe_list(historical_override.get("reasonSamples"))[:2],
            "outcome": {
                "evaluatedCount": historical_outcome_count,
                "completedCount": historical_completion_count,
                "blockedCount": historical_blocked_count,
                "completionRate": historical_completion_rate,
                "blockRate": historical_block_rate,
                "avgCompletionMinutes": historical_avg_completion_minutes,
            },
        },
    }


def select_task_team_candidate_record(
    teams,
    target_agent_id="",
    preferred_team_id="",
    title="",
    remark="",
    intelligence=None,
    workflow_binding=None,
    agents=None,
    task_index=None,
    routing_decisions=None,
):
    override_history = summarize_team_override_preferences(routing_decisions)
    outcome_history = summarize_team_outcome_history(task_index, routing_decisions)
    candidates = [
        score_task_team_candidate(
            item,
            target_agent_id=target_agent_id,
            preferred_team_id=preferred_team_id,
            title=title,
            remark=remark,
            intelligence=intelligence,
            workflow_binding=workflow_binding,
            agents=agents,
            task_index=task_index,
            override_history=override_history,
            outcome_history=outcome_history,
        )
        for item in safe_list(teams)
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    candidates.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            not bool(item.get("preferred")),
            item.get("teamName") or item.get("teamId") or "",
        )
    )
    selected = candidates[0] if candidates else {}
    second = candidates[1] if len(candidates) > 1 else {}
    top_score = int(selected.get("score") or 0)
    second_score = int(second.get("score") or 0)
    score_gap = top_score - second_score if selected else 0
    if selected.get("preferred"):
        confidence = 1.0
    else:
        confidence = 0.28
        confidence += min(max(top_score, 0), 120) / 200.0
        confidence += min(max(score_gap, 0), 30) / 100.0
        if str(selected.get("status") or "").strip().lower() in {"paused", "archived"}:
            confidence -= 0.18
        confidence = round(min(0.99, max(0.05, confidence)), 2)
    # Only ask for manual review when the route is genuinely uncertain.
    # A high-confidence semantic match should still auto-route even if the
    # runner-up score is close.
    manual_review_recommended = bool(selected) and not selected.get("preferred") and (
        confidence < 0.58 or (confidence < 0.72 and score_gap < 12)
    )
    payload = {
        "selectedTeamId": selected.get("teamId", ""),
        "selectedTeamName": selected.get("teamName", ""),
        "selectedLeadAgentId": selected.get("leadAgentId", ""),
        "score": top_score,
        "scoreGap": score_gap,
        "confidence": confidence,
        "preferred": bool(selected.get("preferred")),
        "manualReviewRecommended": manual_review_recommended,
        "reasons": selected.get("reasons", [])[:4],
        "historicalOverrideCount": int(((selected.get("history") or {}).get("overrideCount")) or 0),
        "historicalRecommendedFrom": str(((selected.get("history") or {}).get("recommendedFrom")) or ""),
        "historicalOverrideReasonSamples": safe_list(((selected.get("history") or {}).get("reasonSamples")) or [])[:2],
        "historicalOutcomeCount": int(((((selected.get("history") or {}).get("outcome")) or {}).get("evaluatedCount")) or 0),
        "historicalCompletionRate": int(((((selected.get("history") or {}).get("outcome")) or {}).get("completionRate")) or 0),
        "historicalBlockRate": int(((((selected.get("history") or {}).get("outcome")) or {}).get("blockRate")) or 0),
        "historicalAvgCompletionMinutes": float(((((selected.get("history") or {}).get("outcome")) or {}).get("avgCompletionMinutes")) or 0),
        "alternativeTeams": [
            {
                "teamId": item.get("teamId", ""),
                "teamName": item.get("teamName", ""),
                "score": int(item.get("score") or 0),
                "reasons": item.get("reasons", [])[:2],
                "historicalOverrideCount": int(((item.get("history") or {}).get("overrideCount")) or 0),
                "historicalRecommendedFrom": str(((item.get("history") or {}).get("recommendedFrom")) or ""),
                "historicalOutcomeCount": int(((((item.get("history") or {}).get("outcome")) or {}).get("evaluatedCount")) or 0),
                "historicalCompletionRate": int(((((item.get("history") or {}).get("outcome")) or {}).get("completionRate")) or 0),
                "historicalBlockRate": int(((((item.get("history") or {}).get("outcome")) or {}).get("blockRate")) or 0),
                "historicalAvgCompletionMinutes": float(((((item.get("history") or {}).get("outcome")) or {}).get("avgCompletionMinutes")) or 0),
            }
            for item in candidates[1:4]
        ],
    }
    return {
        "team": deepcopy(selected.get("team")) if isinstance(selected.get("team"), dict) else None,
        "payload": payload,
        "candidates": candidates,
    }


def resolve_default_task_team_record(
    openclaw_dir,
    target_agent_id="",
    preferred_team_id="",
    teams=None,
    title="",
    remark="",
    intelligence=None,
    workflow_binding=None,
    agents=None,
    task_index=None,
    routing_decisions=None,
    return_selection=False,
):
    teams = [item for item in safe_list(teams) if isinstance(item, dict)] or [item for item in store_list_agent_teams(openclaw_dir) if isinstance(item, dict)]
    if not teams:
        empty_selection = {
            "team": None,
            "payload": {
                "selectedTeamId": "",
                "selectedTeamName": "",
                "selectedLeadAgentId": "",
                "score": 0,
                "scoreGap": 0,
                "confidence": 0.0,
                "preferred": False,
                "manualReviewRecommended": False,
                "reasons": ["当前还没有可用团队。"],
                "historicalOverrideCount": 0,
                "historicalRecommendedFrom": "",
                "historicalOverrideReasonSamples": [],
                "historicalOutcomeCount": 0,
                "historicalCompletionRate": 0,
                "historicalBlockRate": 0,
                "historicalAvgCompletionMinutes": 0.0,
                "alternativeTeams": [],
            },
            "candidates": [],
        }
        return empty_selection if return_selection else None
    selection = select_task_team_candidate_record(
        teams,
        target_agent_id=target_agent_id,
        preferred_team_id=preferred_team_id,
        title=title,
        remark=remark,
        intelligence=intelligence,
        workflow_binding=workflow_binding,
        agents=agents,
        task_index=task_index,
        routing_decisions=routing_decisions,
    )
    return selection if return_selection else selection.get("team")


def resolve_team_execution_agent(team, preferred_agent_id="", agents=None, title="", remark="", intelligence=None, workflow_binding=None):
    if not isinstance(team, dict):
        return str(preferred_agent_id or "").strip()

    lead_agent_id = str(team.get("leadAgentId") or "").strip()
    member_agent_ids = [
        str(item or "").strip()
        for item in safe_list(team.get("memberAgentIds"))
        if str(item or "").strip()
    ]
    candidate_ids = []
    for agent_id in [lead_agent_id, *member_agent_ids]:
        if agent_id and agent_id not in candidate_ids:
            candidate_ids.append(agent_id)
    if not candidate_ids:
        return str(preferred_agent_id or "").strip()

    normalized_preferred = str(preferred_agent_id or "").strip()
    if normalized_preferred in candidate_ids:
        return normalized_preferred

    intelligence = intelligence if isinstance(intelligence, dict) else {}
    workflow_binding = workflow_binding if isinstance(workflow_binding, dict) else {}
    selected_branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
    agent_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(agents)
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    semantic_fragments = [
        str(title or ""),
        str(remark or ""),
        str(intelligence.get("category") or ""),
        str(intelligence.get("categoryLabel") or ""),
        " ".join(str(item or "") for item in safe_list(intelligence.get("matchedKeywords"))),
        " ".join(str(item or "") for item in safe_list(intelligence.get("laneHints"))),
        str(selected_branch.get("targetLaneId") or ""),
        str(selected_branch.get("targetLaneTitle") or ""),
        str(selected_branch.get("targetNodeTitle") or ""),
    ]
    semantic_target_agent_id = choose_semantic_agent_candidate(
        candidate_ids,
        semantic_fragments,
        intelligence=intelligence,
    )
    if semantic_target_agent_id:
        return semantic_target_agent_id

    routing_text = " ".join(
        [
            fragment
            for fragment in semantic_fragments
            if str(fragment or "").strip()
        ]
    ).lower()

    routing_terms = {term for term in re.split(r"[^a-z0-9_\u4e00-\u9fff]+", routing_text) if term}
    for agent_id in candidate_ids:
        agent = agent_map.get(agent_id, {})
        haystack = " ".join(
            [
                agent_id,
                str(((agent.get("identity") or {}) if isinstance(agent.get("identity"), dict) else {}).get("name") or ""),
                str(((agent.get("identity") or {}) if isinstance(agent.get("identity"), dict) else {}).get("title") or ""),
            ]
        ).lower()
        if any(term and term in haystack for term in routing_terms):
            return agent_id

    return lead_agent_id or candidate_ids[0]


def patch_task_team_assignment_metadata(openclaw_dir, task_id, team, router_agent_id=""):
    task_id = str(task_id or "").strip()
    if not task_id or not isinstance(team, dict) or not team.get("id"):
        return False
    config = load_config(openclaw_dir)
    if not router_agent_id:
        router_agent_id = get_router_agent_id(config)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)
    changed = {"value": False}
    team_payload = {
        "teamId": str(team.get("id") or "").strip(),
        "teamName": str(team.get("name") or "").strip(),
        "leadAgentId": str(team.get("leadAgentId") or "").strip(),
        "memberAgentIds": [str(item or "").strip() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()],
        "assignedAt": now_iso(),
    }

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            route_meta["teamAssignment"] = team_payload
            route_meta["teamId"] = team_payload["teamId"]
            route_meta["teamName"] = team_payload["teamName"]
            route_meta["teamLeadAgentId"] = team_payload["leadAgentId"]
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            task["teamId"] = team_payload["teamId"]
            task["teamLabel"] = team_payload["teamName"]
            task["updatedAt"] = now_iso()
            changed["value"] = True
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])
    return changed["value"]


def link_task_to_agent_team(openclaw_dir, team, task_id):
    if not isinstance(team, dict) or not team.get("id") or not task_id:
        return team
    linked_task_ids = [str(item or "").strip() for item in safe_list(team.get("linkedTaskIds")) if str(item or "").strip()]
    task_id = str(task_id).strip()
    if task_id in linked_task_ids:
        return team
    linked_task_ids.append(task_id)
    return save_agent_team_preserving_meta(
        openclaw_dir,
        {
            "id": team.get("id", ""),
            "name": team.get("name", ""),
            "status": team.get("status", "active"),
            "leadAgentId": team.get("leadAgentId", ""),
            "memberAgentIds": safe_list(team.get("memberAgentIds")),
            "description": team.get("description", ""),
            "focus": team.get("focus", ""),
            "channel": team.get("channel", "internal"),
            "linkedTaskIds": linked_task_ids,
        },
        existing=team,
    )


def build_decision_source_review(task_index, routing_decisions, now):
    decision_by_task = {}
    for item in safe_list(routing_decisions):
        task_id = str(item.get("taskId") or "").strip()
        if task_id and task_id not in decision_by_task:
            decision_by_task[task_id] = item

    source_rows = {}
    trend_buckets = {}
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).strftime("%m-%d")
        trend_buckets[day] = {
            "date": day,
            "model": {"total": 0, "completed": 0, "blocked": 0, "confidenceTotal": 0.0, "confidenceCount": 0},
            "heuristic": {"total": 0, "completed": 0, "blocked": 0, "confidenceTotal": 0.0, "confidenceCount": 0},
        }

    def ensure_row(source):
        normalized = "model" if str(source or "").strip().lower() == "model" else "heuristic"
        if normalized not in source_rows:
            source_rows[normalized] = {
                "source": normalized,
                "title": "模型判断" if normalized == "model" else "规则判断",
                "totalCount": 0,
                "evaluatedCount": 0,
                "completedCount": 0,
                "blockedCount": 0,
                "activeCount": 0,
                "manualReviewCount": 0,
                "fallbackCount": 0,
                "confidenceTotal": 0.0,
                "confidenceCount": 0,
            }
        return source_rows[normalized]

    for task in safe_list(task_index):
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            continue
        decision = decision_by_task.get(task_id)
        if not decision:
            continue
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        decision_meta = decision.get("meta") if isinstance(decision.get("meta"), dict) else {}
        intelligence = route.get("intelligence") if isinstance(route.get("intelligence"), dict) else {}
        source = (
            str(route.get("decisionSource") or "").strip().lower()
            or str(decision_meta.get("decisionSource") or "").strip().lower()
            or str(intelligence.get("decisionSource") or "").strip().lower()
            or "heuristic"
        )
        row = ensure_row(source)
        outcome = decision_meta.get("outcome") if isinstance(decision_meta.get("outcome"), dict) else {}
        state = str(task.get("state") or "").lower()
        status = str(
            outcome.get("status")
            or ("blocked" if task.get("blocked") else ("completed" if state in TERMINAL_STATES else "active"))
        ).lower()
        confidence = float(route.get("confidence") or intelligence.get("confidence") or decision_meta.get("confidence") or 0)
        manual_review = bool(route.get("manualReview") or intelligence.get("manualReview") or decision_meta.get("manualReview"))
        fallback = bool(route.get("fallback") or decision_meta.get("fallback"))

        row["totalCount"] += 1
        if status in {"completed", "blocked"}:
            row["evaluatedCount"] += 1
        else:
            row["activeCount"] += 1
        if status == "completed":
            row["completedCount"] += 1
        elif status == "blocked":
            row["blockedCount"] += 1
        if manual_review:
            row["manualReviewCount"] += 1
        if fallback:
            row["fallbackCount"] += 1
        if confidence > 0:
            row["confidenceTotal"] += confidence
            row["confidenceCount"] += 1

        updated_at = parse_iso(task.get("updatedAt")) or now
        bucket = trend_buckets.get(updated_at.strftime("%m-%d"))
        if bucket:
            source_bucket = bucket["model" if row["source"] == "model" else "heuristic"]
            source_bucket["total"] += 1
            if status == "completed":
                source_bucket["completed"] += 1
            elif status == "blocked":
                source_bucket["blocked"] += 1
            if confidence > 0:
                source_bucket["confidenceTotal"] += confidence
                source_bucket["confidenceCount"] += 1

    rows = []
    for key in ["model", "heuristic"]:
        row = ensure_row(key)
        evaluated = max(int(row["evaluatedCount"] or 0), 1)
        total = max(int(row["totalCount"] or 0), 1)
        completion_rate = int(round((int(row["completedCount"] or 0) / evaluated) * 100)) if row["evaluatedCount"] else 0
        block_rate = int(round((int(row["blockedCount"] or 0) / evaluated) * 100)) if row["evaluatedCount"] else 0
        fallback_rate = int(round((int(row["fallbackCount"] or 0) / total) * 100)) if row["totalCount"] else 0
        avg_confidence = int(round((row["confidenceTotal"] / row["confidenceCount"]) * 100)) if row["confidenceCount"] else 0
        rows.append(
            {
                **row,
                "completionRate": completion_rate,
                "blockRate": block_rate,
                "fallbackRate": fallback_rate,
                "avgConfidence": avg_confidence,
            }
        )

    model_row = next((item for item in rows if item["source"] == "model"), rows[0] if rows else {})
    heuristic_row = next((item for item in rows if item["source"] == "heuristic"), rows[-1] if rows else {})
    total_count = sum(int(item.get("totalCount") or 0) for item in rows)
    model_share = int(round((int(model_row.get("totalCount") or 0) / max(total_count, 1)) * 100)) if total_count else 0
    model_evaluated_count = int(model_row.get("evaluatedCount") or 0)
    heuristic_evaluated_count = int(heuristic_row.get("evaluatedCount") or 0)
    outcome_delta = (
        int(model_row.get("completionRate") or 0) - int(heuristic_row.get("completionRate") or 0)
        if model_evaluated_count and heuristic_evaluated_count
        else None
    )

    trend = []
    for bucket in trend_buckets.values():
        model_total = max(int(bucket["model"]["total"] or 0), 1)
        heuristic_total = max(int(bucket["heuristic"]["total"] or 0), 1)
        model_evaluated = max(int(bucket["model"]["completed"] or 0) + int(bucket["model"]["blocked"] or 0), 1)
        heuristic_evaluated = max(int(bucket["heuristic"]["completed"] or 0) + int(bucket["heuristic"]["blocked"] or 0), 1)
        model_score = max(
            0,
            min(
                100,
                int(
                    round(
                        100
                        - min(int(round((int(bucket["model"]["blocked"] or 0) / model_evaluated) * 100)) * 0.5, 30)
                        + min(int(round((int(bucket["model"]["completed"] or 0) / model_evaluated) * 100)) * 0.18, 15)
                    )
                ),
            ),
        ) if bucket["model"]["total"] else 0
        heuristic_score = max(
            0,
            min(
                100,
                int(
                    round(
                        100
                        - min(int(round((int(bucket["heuristic"]["blocked"] or 0) / heuristic_evaluated) * 100)) * 0.5, 30)
                        + min(int(round((int(bucket["heuristic"]["completed"] or 0) / heuristic_evaluated) * 100)) * 0.18, 15)
                    )
                ),
            ),
        ) if bucket["heuristic"]["total"] else 0
        trend.append(
            {
                "date": bucket["date"],
                "modelCount": int(bucket["model"]["total"] or 0),
                "heuristicCount": int(bucket["heuristic"]["total"] or 0),
                "modelScore": model_score,
                "heuristicScore": heuristic_score,
                "modelAvgConfidence": int(round((bucket["model"]["confidenceTotal"] / bucket["model"]["confidenceCount"]) * 100)) if bucket["model"]["confidenceCount"] else 0,
                "heuristicAvgConfidence": int(round((bucket["heuristic"]["confidenceTotal"] / bucket["heuristic"]["confidenceCount"]) * 100)) if bucket["heuristic"]["confidenceCount"] else 0,
            }
        )

    trend_direction = "stable"
    if trend:
        delta = int(trend[-1].get("modelScore") or 0) - int(trend[-1].get("heuristicScore") or 0)
        if delta >= 8:
            trend_direction = "up"
        elif delta <= -8:
            trend_direction = "down"

    suggestions = []
    if model_share < 35:
        suggestions.append(
            {
                "title": "模型判断覆盖还不够",
                "detail": f"当前只有 {model_share}% 的任务走了模型判断，仍有较多任务直接回到规则链。",
            }
        )
    if int(model_row.get("blockRate") or 0) > int(heuristic_row.get("blockRate") or 0) + 8 and model_evaluated_count >= 2:
        suggestions.append(
            {
                "title": "模型判断这轮还不够稳",
                "detail": f"模型链路阻塞率 {model_row.get('blockRate', 0)}%，高于规则链 {heuristic_row.get('blockRate', 0)}%，建议先看任务理解和 workflow 选择。",
            }
        )
    elif int(model_row.get("completionRate") or 0) >= int(heuristic_row.get("completionRate") or 0) + 8 and model_evaluated_count >= 2:
        suggestions.append(
            {
                "title": "模型判断已经开始拉开差距",
                "detail": f"模型链路收口率 {model_row.get('completionRate', 0)}%，比规则链高 {max(outcome_delta, 0)} 个点。",
            }
        )
    if int(model_row.get("fallbackRate") or 0) >= 20:
        suggestions.append(
            {
                "title": "模型判断后仍有较多回退",
                "detail": f"模型链路里还有 {model_row.get('fallbackRate', 0)}% 走了兜底，说明可执行去向或 workflow 选择还不够稳定。",
            }
        )

    causes = []
    if model_share < 35:
        causes.append(
            {
                "title": "模型判断覆盖还不够",
                "detail": f"当前只有 {model_share}% 的任务走了模型判断，绝大多数任务仍然直接落到规则链，先扩大模型覆盖面才看得清真实效果。",
                "band": "watch" if model_share >= 20 else "critical",
                "action": {"type": "open_orchestration", "label": "去看编排入口", "path": "/orchestration"},
            }
        )
    if model_evaluated_count == 0 and int(model_row.get("totalCount") or 0) > 0:
        causes.append(
            {
                "title": "模型链路还在观察期",
                "detail": f"模型已经接管了 {model_row.get('totalCount', 0)} 条任务，但还没有形成已收口样本，当前更适合先盯执行推进和人工复核信号。",
                "band": "watch",
            }
        )
    if int(model_row.get("fallbackRate") or 0) >= 20:
        causes.append(
            {
                "title": "模型推荐后仍经常回退",
                "detail": f"模型链路回退率 {model_row.get('fallbackRate', 0)}%，说明推荐的 workflow 或 target agent 还不够稳定。",
                "band": "critical" if int(model_row.get("fallbackRate") or 0) >= 35 else "watch",
                "action": {"type": "open_orchestration", "label": "复盘分流与流程", "path": "/orchestration"},
            }
        )
    if int(model_row.get("avgConfidence") or 0) and int(heuristic_row.get("avgConfidence") or 0) and int(model_row.get("avgConfidence") or 0) + 8 < int(heuristic_row.get("avgConfidence") or 0):
        causes.append(
            {
                "title": "模型把握度仍低于规则链",
                "detail": f"模型平均把握 {model_row.get('avgConfidence', 0)}%，低于规则链 {heuristic_row.get('avgConfidence', 0)}%，建议先补任务理解信号。",
                "band": "watch",
                "action": {"type": "open_orchestration", "label": "补任务理解", "path": "/orchestration"},
            }
        )
    if outcome_delta is not None and outcome_delta <= -8 and model_evaluated_count >= 2:
        causes.append(
            {
                "title": "模型收口率暂时落后",
                "detail": f"模型链路收口率比规则链低 {abs(outcome_delta)} 个点，建议优先看人工复核前置和 workflow 选择。",
                "band": "critical" if outcome_delta <= -15 else "watch",
                "action": {"type": "open_orchestration", "label": "查看联动复盘", "path": "/orchestration"},
            }
        )
    if not causes:
        causes.append(
            {
                "title": "当前没有明显的模型偏差来源",
                "detail": "模型判断和规则链当前没有出现明显的结构性偏差，可以继续观察趋势和收口样本。",
                "band": "stable",
            }
        )

    return {
        "summary": {
            "modelShare": model_share,
            "primarySource": "model" if model_share >= 50 else "heuristic",
            "outcomeDelta": outcome_delta,
            "modelEvaluatedCount": model_evaluated_count,
            "heuristicEvaluatedCount": heuristic_evaluated_count,
        },
        "rows": rows,
        "trend": trend,
        "trendDirection": trend_direction,
        "causes": causes[:4],
        "suggestions": suggestions[:3],
    }


def build_orchestration_policy_trends(routing_decisions, task_index):
    decision_by_task = {
        str(item.get("taskId") or "").strip(): item
        for item in safe_list(routing_decisions)
        if str(item.get("taskId") or "").strip()
    }
    rows = {}
    for task in safe_list(task_index):
        task_id = str(task.get("id") or "").strip()
        decision = decision_by_task.get(task_id)
        if not decision:
            continue
        policy_name = decision.get("policyName") or decision.get("strategyType") or "Router fallback"
        row = rows.setdefault(
            policy_name,
            {
                "policyName": policy_name,
                "recentCount": 0,
                "completedCount": 0,
                "blockedCount": 0,
                "activeCount": 0,
                "days": Counter(),
            },
        )
        row["recentCount"] += 1
        decided_at = parse_iso(decision.get("decidedAt"))
        if decided_at:
            row["days"][decided_at.strftime("%m-%d")] += 1
        state = str(task.get("state") or "").lower()
        if task.get("blocked"):
            row["blockedCount"] += 1
        elif state in TERMINAL_STATES:
            row["completedCount"] += 1
        else:
            row["activeCount"] += 1
    items = []
    for row in rows.values():
        evaluated = row["completedCount"] + row["blockedCount"]
        items.append(
            {
                "policyName": row["policyName"],
                "recentCount": row["recentCount"],
                "completionRate": int(round((row["completedCount"] / max(evaluated, 1)) * 100)) if evaluated else 0,
                "blockRate": int(round((row["blockedCount"] / max(evaluated, 1)) * 100)) if evaluated else 0,
                "activeCount": row["activeCount"],
                "trend": [{"day": day, "count": row["days"][day]} for day in sorted(row["days"].keys())[-7:]],
            }
        )
    items.sort(key=lambda item: (-item["recentCount"], -item["blockRate"], item["policyName"]))
    return items[:6]


def build_orchestration_review_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index):
    suggestions = []
    workflow_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(workflows)
        if str(item.get("id") or "").strip()
    }
    policy_map = {
        str(item.get("name") or item.get("id") or "").strip(): item
        for item in safe_list(routing_policies)
        if str(item.get("name") or item.get("id") or "").strip()
    }
    for item in safe_list(policy_trends):
        policy = policy_map.get(str(item.get("policyName") or "").strip())
        if not policy or str(policy.get("status") or "") != "active":
            continue
        if int(item.get("blockRate") or 0) < 40:
            continue
        suggestions.append(
            {
                "title": f"先停用策略 {item.get('policyName', '')}",
                "detail": f"这条策略最近阻塞率 {item.get('blockRate', 0)}%，建议先停用后复盘命中条件和目标 Agent。",
                "severity": "warning",
                "action": {
                    "type": "pause_policy",
                    "label": "停用策略",
                    "payload": {
                        "id": policy.get("id", ""),
                        "name": policy.get("name", ""),
                        "status": "disabled",
                        "strategyType": policy.get("strategyType", ""),
                        "keyword": policy.get("keyword", ""),
                        "targetAgentId": policy.get("targetAgentId", ""),
                        "priorityLevel": policy.get("priorityLevel", "normal"),
                        "queueName": policy.get("queueName", ""),
                    },
                },
            }
        )
        break
    for item in safe_list(policy_trends):
        policy = policy_map.get(str(item.get("policyName") or "").strip())
        if not policy or str(policy.get("status") or "") != "active":
            continue
        if int(item.get("blockRate") or 0) < 20 or int(item.get("blockRate") or 0) >= 40:
            continue
        priority = str(policy.get("priorityLevel") or "normal").lower()
        if priority not in {"high", "critical"}:
            continue
        next_priority = "normal" if priority == "high" else "high"
        suggestions.append(
            {
                "title": f"先下调策略 {item.get('policyName', '')} 的优先级",
                "detail": f"这条策略最近阻塞率 {item.get('blockRate', 0)}%，先从 {priority.upper()} 调到 {next_priority.upper()}，比直接停用更稳妥。",
                "severity": "info",
                "action": {
                    "type": "adjust_policy_priority",
                    "label": f"改为 {next_priority.upper()}",
                    "payload": {
                        "id": policy.get("id", ""),
                        "name": policy.get("name", ""),
                        "status": policy.get("status", "active"),
                        "strategyType": policy.get("strategyType", ""),
                        "keyword": policy.get("keyword", ""),
                        "targetAgentId": policy.get("targetAgentId", ""),
                        "priorityLevel": next_priority,
                        "queueName": policy.get("queueName", ""),
                    },
                },
            }
        )
        break
    for row in safe_list(workflow_review):
        if int(row.get("contextLossCount") or 0) < 2:
            continue
        workflow_id = str(row.get("workflowId") or "").strip()
        workflow = workflow_map.get(workflow_id, {})
        weak_node = find_workflow_weak_handoff_node(workflow)
        if not weak_node:
            continue
        suggestions.append(
            {
                "title": f"强化流程 {row.get('workflowName', '')} 的交接模板",
                "detail": f"这条流程最近出现 {row.get('contextLossCount', 0)} 次协作断点，先把 {weak_node.get('title') or '当前节点'} 的交接说明补完整，能更快减少上下文丢失。",
                "severity": "info",
                "action": {
                    "type": "strengthen_handoff_note",
                    "label": "强化交接模板",
                    "payload": {
                        "workflowId": workflow_id,
                        "nodeId": str(weak_node.get("id") or "").strip(),
                        "title": str(weak_node.get("title") or "").strip(),
                        "reason": "流程复盘显示上下文丢失偏高，建议先强化这一跳的交接模板。",
                    },
                },
            }
        )
        break
    for row in safe_list(workflow_review):
        if str(row.get("verdict") or "") not in {"watch", "critical"}:
            continue
        workflow_id = str(row.get("workflowId") or "").strip()
        workflow = workflow_map.get(workflow_id, {})
        has_checkpoint = workflow_has_checkpoint_node(workflow)
        if has_checkpoint:
            continue
        sample_task = next(
            (
                task for task in safe_list(task_index)
                if str(((task.get("workflowBinding") or {}) if isinstance(task.get("workflowBinding"), dict) else {}).get("workflowId") or "").strip() == workflow_id
                and (task.get("blocked") or str(task.get("state") or "").lower() not in TERMINAL_STATES)
            ),
            None,
        )
        if not sample_task:
            continue
        workflow_binding = sample_task.get("workflowBinding") if isinstance(sample_task.get("workflowBinding"), dict) else {}
        selected_branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
        suggestions.append(
            {
                "title": f"给流程 {row.get('workflowName', '')} 补验证检查点",
                "detail": f"这条流程当前阻塞率 {row.get('blockRate', 0)}%，协作断点 {row.get('contextLossCount', 0)} 次，建议先加验证检查点再继续放量。",
                "severity": "info",
                "action": {
                    "type": "insert_approval_node",
                    "label": "加入验证检查点",
                    "payload": {
                        "workflowId": workflow_id,
                        "targetLaneId": str(selected_branch.get("targetLaneId") or "").strip(),
                        "targetAgentId": str(sample_task.get("targetAgentId") or "").strip(),
                        "title": "验证检查点",
                        "approver": "质量负责人",
                        "timeout": 30,
                        "reason": "流程复盘显示这条工作流阻塞率偏高，建议补验证检查点。",
                    },
                },
            }
        )
        break
    return suggestions[:4]


def build_orchestration_next_step_suggestions(workflows, workflow_review, adjustment_review):
    workflow_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(workflows)
        if str(item.get("id") or "").strip()
    }
    review_map = {
        str(item.get("workflowId") or "").strip(): item
        for item in safe_list(workflow_review)
        if str(item.get("workflowId") or "").strip()
    }
    suggestions = []
    for item in safe_list((adjustment_review or {}).get("rows")):
        if str(item.get("verdict") or "") != "follow_up":
            continue
        workflow_id = str(item.get("workflowId") or "").strip()
        workflow = workflow_map.get(workflow_id, {})
        if not workflow:
            continue
        review = review_map.get(workflow_id, {})
        actions = []
        weak_node = find_workflow_weak_handoff_node(workflow)
        if weak_node:
            actions.append(
                {
                    "type": "strengthen_handoff_note",
                    "label": "先补交接模板",
                    "payload": {
                        "workflowId": workflow_id,
                        "nodeId": str(weak_node.get("id") or "").strip(),
                        "title": str(weak_node.get("title") or "").strip(),
                        "reason": "这条流程仍有协作断点，建议继续强化交接模板。",
                    },
                }
            )
        if int(review.get("blockRate") or 0) >= 30 and not workflow_has_checkpoint_node(workflow):
            first_agent_node = next(
                (node for node in safe_list(workflow.get("nodes")) if str(node.get("type") or "agent").strip() == "agent"),
                {},
            )
            actions.append(
                {
                    "type": "insert_approval_node",
                    "label": "补验证节点",
                    "payload": {
                        "workflowId": workflow_id,
                        "targetLaneId": str(first_agent_node.get("laneId") or "").strip(),
                        "targetAgentId": str(first_agent_node.get("agentId") or "").strip(),
                        "title": "验证检查点",
                        "approver": "质量负责人",
                        "timeout": 30,
                        "reason": "这条流程阻塞率仍然偏高，建议补验证节点兜底。",
                    },
                }
            )
        if not actions:
            continue
        suggestions.append(
            {
                "workflowId": workflow_id,
                "workflowName": item.get("workflowName", workflow_id),
                "title": f"先稳住流程 {item.get('workflowName', workflow_id)}",
                "detail": f"当前阻塞 {item.get('blockRate', 0)}%，协作断点 {item.get('contextLossCount', 0)} 次。建议先执行下面这组修复动作。",
                "actions": actions[:2],
            }
        )
    return suggestions[:4]


def build_orchestration_linked_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index):
    workflow_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(workflows)
        if str(item.get("id") or "").strip()
    }
    policy_map = {
        str(item.get("name") or item.get("id") or "").strip(): item
        for item in safe_list(routing_policies)
        if str(item.get("name") or item.get("id") or "").strip()
    }
    policy_trend_map = {
        str(item.get("policyName") or "").strip(): item
        for item in safe_list(policy_trends)
        if str(item.get("policyName") or "").strip()
    }
    policy_counter_by_workflow = defaultdict(Counter)
    for task in safe_list(task_index):
        binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else {}
        workflow_id = str(binding.get("workflowId") or "").strip()
        if not workflow_id:
            continue
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        policy_name = str(route.get("policyName") or route.get("policyId") or "").strip()
        if policy_name:
            policy_counter_by_workflow[workflow_id][policy_name] += 1

    suggestions = []
    for review in safe_list(workflow_review):
        workflow_id = str(review.get("workflowId") or "").strip()
        if not workflow_id or str(review.get("verdict") or "") not in {"watch", "critical"}:
            continue
        workflow = workflow_map.get(workflow_id, {})
        if not workflow:
            continue
        dominant_policy_name = ""
        if policy_counter_by_workflow.get(workflow_id):
            dominant_policy_name = policy_counter_by_workflow[workflow_id].most_common(1)[0][0]
        policy = policy_map.get(dominant_policy_name, {})
        policy_trend = policy_trend_map.get(dominant_policy_name, {})
        actions = []
        if policy and str(policy.get("status") or "") == "active":
            block_rate = int(policy_trend.get("blockRate") or 0)
            priority = str(policy.get("priorityLevel") or "normal").lower()
            if block_rate >= 40:
                actions.append(
                    {
                        "type": "pause_policy",
                        "label": "先停用策略",
                        "payload": {
                            "id": policy.get("id", ""),
                            "name": policy.get("name", ""),
                            "status": "disabled",
                            "strategyType": policy.get("strategyType", ""),
                            "keyword": policy.get("keyword", ""),
                            "targetAgentId": policy.get("targetAgentId", ""),
                            "priorityLevel": policy.get("priorityLevel", "normal"),
                            "queueName": policy.get("queueName", ""),
                        },
                    }
                )
            elif block_rate >= 20 and priority in {"high", "critical"}:
                next_priority = "normal" if priority == "high" else "high"
                actions.append(
                    {
                        "type": "adjust_policy_priority",
                        "label": f"策略改为 {next_priority.upper()}",
                        "payload": {
                            "id": policy.get("id", ""),
                            "name": policy.get("name", ""),
                            "status": policy.get("status", "active"),
                            "strategyType": policy.get("strategyType", ""),
                            "keyword": policy.get("keyword", ""),
                            "targetAgentId": policy.get("targetAgentId", ""),
                            "priorityLevel": next_priority,
                            "queueName": policy.get("queueName", ""),
                        },
                    }
                )
        weak_node = find_workflow_weak_handoff_node(workflow)
        if weak_node:
            actions.append(
                {
                    "type": "strengthen_handoff_note",
                    "label": "补交接模板",
                    "payload": {
                        "workflowId": workflow_id,
                        "nodeId": str(weak_node.get("id") or "").strip(),
                        "title": str(weak_node.get("title") or "").strip(),
                        "reason": "这条流程仍有协作断点，建议同步强化交接模板。",
                    },
                }
            )
        if int(review.get("blockRate") or 0) >= 30 and not workflow_has_checkpoint_node(workflow):
            first_agent_node = next(
                (node for node in safe_list(workflow.get("nodes")) if str(node.get("type") or "agent").strip() == "agent"),
                {},
            )
            actions.append(
                {
                    "type": "insert_approval_node",
                    "label": "补验证节点",
                    "payload": {
                        "workflowId": workflow_id,
                        "targetLaneId": str(first_agent_node.get("laneId") or "").strip(),
                        "targetAgentId": str(first_agent_node.get("agentId") or "").strip(),
                        "title": "验证检查点",
                        "approver": "质量负责人",
                        "timeout": 30,
                        "reason": "这条流程阻塞率仍然偏高，建议补验证节点兜底。",
                    },
                }
            )
        if len(actions) < 2:
            continue
        suggestions.append(
            {
                "workflowId": workflow_id,
                "workflowName": review.get("workflowName", workflow_id),
                "policyName": dominant_policy_name,
                "title": f"一起收紧 {review.get('workflowName', workflow_id)} 的分流和流程",
                "detail": (
                    f"这条流程当前阻塞 {review.get('blockRate', 0)}%，协作断点 {review.get('contextLossCount', 0)} 次。"
                    + (f"同时关联策略 {dominant_policy_name}，建议两边一起收口。" if dominant_policy_name else "建议同时从分流和流程两侧一起收口。")
                ),
                "actions": actions[:3],
            }
        )
    return suggestions[:4]


def build_orchestration_linked_review(workflows, routing_policies, policy_trends, workflow_review, task_index):
    workflow_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(workflows)
        if str(item.get("id") or "").strip()
    }
    workflow_review_map = {
        str(item.get("workflowId") or "").strip(): item
        for item in safe_list(workflow_review)
        if str(item.get("workflowId") or "").strip()
    }
    policy_trend_map = {
        str(item.get("policyName") or "").strip(): item
        for item in safe_list(policy_trends)
        if str(item.get("policyName") or "").strip()
    }
    policy_counter_by_workflow = defaultdict(Counter)
    for task in safe_list(task_index):
        binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else {}
        workflow_id = str(binding.get("workflowId") or "").strip()
        if not workflow_id:
            continue
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        policy_name = str(route.get("policyName") or route.get("policyId") or "").strip()
        if policy_name:
            policy_counter_by_workflow[workflow_id][policy_name] += 1

    rows = []
    for workflow in safe_list(workflows):
        workflow_id = str(workflow.get("id") or "").strip()
        if not workflow_id:
            continue
        meta = workflow.get("meta") if isinstance(workflow.get("meta"), dict) else {}
        adjustments = safe_list(meta.get("recommendedAdjustments"))
        if not adjustments:
            continue
        latest = next((item for item in adjustments if isinstance(item, dict)), None)
        if not latest:
            continue
        dominant_policy_name = ""
        if policy_counter_by_workflow.get(workflow_id):
            dominant_policy_name = policy_counter_by_workflow[workflow_id].most_common(1)[0][0]
        workflow_review_item = workflow_review_map.get(workflow_id, {})
        policy_review_item = policy_trend_map.get(dominant_policy_name, {})
        workflow_verdict = "watch"
        if int(workflow_review_item.get("blockRate") or 0) < 20 and int(workflow_review_item.get("contextLossCount") or 0) <= 1:
            workflow_verdict = "stabilized"
        elif int(workflow_review_item.get("blockRate") or 0) >= 35 or int(workflow_review_item.get("contextLossCount") or 0) >= 3:
            workflow_verdict = "follow_up"
        policy_verdict = "watch"
        if dominant_policy_name:
            if int(policy_review_item.get("blockRate") or 0) < 20:
                policy_verdict = "stabilized"
            elif int(policy_review_item.get("blockRate") or 0) >= 35:
                policy_verdict = "follow_up"
        overall_verdict = "watch"
        if workflow_verdict == "stabilized" and policy_verdict in {"", "stabilized", "watch"}:
            overall_verdict = "stabilized" if policy_verdict != "follow_up" else "watch"
        if workflow_verdict == "follow_up" or policy_verdict == "follow_up":
            overall_verdict = "follow_up"
        rows.append(
            {
                "workflowId": workflow_id,
                "workflowName": workflow.get("name", workflow_id),
                "policyName": dominant_policy_name,
                "adjustmentType": str(latest.get("type") or "").strip(),
                "workflowBlockRate": int(workflow_review_item.get("blockRate") or 0),
                "workflowContextLossCount": int(workflow_review_item.get("contextLossCount") or 0),
                "policyBlockRate": int(policy_review_item.get("blockRate") or 0),
                "policyRecentCount": int(policy_review_item.get("recentCount") or 0),
                "workflowVerdict": workflow_verdict,
                "policyVerdict": policy_verdict,
                "overallVerdict": overall_verdict,
            }
        )
    rows.sort(
        key=lambda item: (
            0 if item["overallVerdict"] == "follow_up" else 1 if item["overallVerdict"] == "watch" else 2,
            -item["workflowContextLossCount"],
            -item["workflowBlockRate"],
            -item["policyBlockRate"],
            item["workflowName"],
        )
    )
    return {
        "summary": {
            "total": len(rows),
            "stabilizedCount": sum(1 for item in rows if item["overallVerdict"] == "stabilized"),
            "watchCount": sum(1 for item in rows if item["overallVerdict"] == "watch"),
            "followUpCount": sum(1 for item in rows if item["overallVerdict"] == "follow_up"),
        },
        "rows": rows[:8],
    }


def build_recommendation_accuracy_review(openclaw_dir, task_index, now):
    intelligence = build_task_intelligence_summary(task_index)
    routing_decisions = store_list_routing_decisions(openclaw_dir, limit=180)
    decision_quality = build_routing_effectiveness_summary(task_index, routing_decisions)
    workflows = store_list_orchestration_workflows(openclaw_dir)
    routing_policies = store_list_routing_policies(openclaw_dir)
    replays = []
    for task in safe_list(task_index)[:24]:
        if not task.get("id"):
            continue
        replay = build_orchestration_replay(task)
        replay.update(
            {
                "state": task.get("state", ""),
                "updatedAgo": task.get("updatedAgo", ""),
                "route": task.get("route", []),
                "blocked": bool(task.get("blocked")),
            }
        )
        replays.append(replay)
    workflow_review = build_orchestration_workflow_review(workflows, task_index, replays)
    policy_trends = build_orchestration_policy_trends(routing_decisions, task_index)
    linked_review = build_orchestration_linked_review(workflows, routing_policies, policy_trends, workflow_review, task_index)

    confidence_band = "watch"
    avg_confidence = int(round(float(decision_quality.get("avgConfidence") or 0) * 100))
    if avg_confidence >= 75:
        confidence_band = "stable"
    elif avg_confidence >= 85:
        confidence_band = "excellent"
    routing_band = "watch"
    if int(decision_quality.get("blockRate") or 0) < 15 and int(decision_quality.get("completionRate") or 0) >= 75:
        routing_band = "stable"
    if int(decision_quality.get("blockRate") or 0) < 8 and int(decision_quality.get("completionRate") or 0) >= 85:
        routing_band = "excellent"
    linked_band = "watch"
    if int(linked_review["summary"].get("followUpCount") or 0) == 0 and int(linked_review["summary"].get("stabilizedCount") or 0) > 0:
        linked_band = "stable"
    if int(linked_review["summary"].get("followUpCount") or 0) == 0 and int(linked_review["summary"].get("watchCount") or 0) == 0 and int(linked_review["summary"].get("stabilizedCount") or 0) > 0:
        linked_band = "excellent"

    score = max(
        0,
        min(
            100,
            int(
                round(
                    100
                    - min(int(intelligence.get("lowConfidenceCount") or 0) * 6, 24)
                    - min(int(intelligence.get("riskyFallbackCount") or 0) * 8, 24)
                    - min(int(decision_quality.get("blockRate") or 0) * 0.45, 24)
                    - min(int(linked_review["summary"].get("followUpCount") or 0) * 10, 20)
                    + min(int(decision_quality.get("completionRate") or 0) * 0.18, 12)
                )
            ),
        ),
    )

    suggestions = []
    if int(intelligence.get("lowConfidenceCount") or 0) >= 2:
        suggestions.append(
            {
                "title": "低把握任务还偏多",
                "detail": f"当前还有 {intelligence.get('lowConfidenceCount', 0)} 条低把握任务，建议继续补分流条件或前置人工复核。",
                "action": {"type": "open_orchestration", "label": "去流程编排", "path": "/orchestration"},
            }
        )
    if int(decision_quality.get("blockRate") or 0) >= 25:
        suggestions.append(
            {
                "title": "分流收口率还有提升空间",
                "detail": f"当前分流阻塞率 {decision_quality.get('blockRate', 0)}%，建议优先复盘高阻塞策略。",
                "action": {"type": "open_orchestration", "label": "看分流策略", "path": "/orchestration"},
            }
        )
    if int(linked_review["summary"].get("followUpCount") or 0) > 0:
        suggestions.append(
            {
                "title": "联动收口还没完全稳住",
                "detail": f"还有 {linked_review['summary'].get('followUpCount', 0)} 条 workflow 需要同时调整分流和流程。",
                "action": {"type": "open_orchestration", "label": "看联动复盘", "path": "/orchestration"},
            }
        )

    causes = []
    if int(intelligence.get("riskyFallbackCount") or 0) > 0:
        causes.append(
            {
                "title": "默认回退仍然偏多",
                "detail": f"{intelligence.get('riskyFallbackCount', 0)} 条高风险任务还在走默认路由，说明分流规则覆盖还不够。",
                "band": "critical" if int(intelligence.get("riskyFallbackCount") or 0) >= 2 else "watch",
                "action": {"type": "open_orchestration", "label": "补分流规则", "path": "/orchestration"},
            }
        )
    if int(intelligence.get("lowConfidenceCount") or 0) > 0:
        causes.append(
            {
                "title": "任务理解把握还不稳",
                "detail": f"{intelligence.get('lowConfidenceCount', 0)} 条任务判断把握偏低，建议继续补关键词、类型信号或人工复核。",
                "band": "watch",
                "action": {"type": "open_orchestration", "label": "看流程编排", "path": "/orchestration"},
            }
        )
    if int(decision_quality.get("blockRate") or 0) >= 20:
        causes.append(
            {
                "title": "分流决策阻塞率偏高",
                "detail": f"当前分流阻塞率 {decision_quality.get('blockRate', 0)}%，说明策略命中后仍有较多任务卡在执行链路里。",
                "band": "critical" if int(decision_quality.get("blockRate") or 0) >= 35 else "watch",
                "action": {"type": "open_orchestration", "label": "看策略复盘", "path": "/orchestration"},
            }
        )
    if int(linked_review["summary"].get("followUpCount") or 0) > 0:
        causes.append(
            {
                "title": "流程联动还没完全稳住",
                "detail": f"{linked_review['summary'].get('followUpCount', 0)} 条 workflow 在分流和流程两侧仍需同时跟进。",
                "band": "watch",
                "action": {"type": "open_orchestration", "label": "看联动复盘", "path": "/orchestration"},
            }
        )
    if not causes:
        causes.append(
            {
                "title": "当前没有明显偏差来源",
                "detail": "最近这轮推荐准确率没有看到集中的漂移来源，可以继续观察趋势变化。",
                "band": "stable",
            }
        )

    repair_bundles = []
    risky_fallback_tasks = safe_list(intelligence.get("riskyFallbackTasks"))
    low_confidence_tasks = safe_list(intelligence.get("lowConfidenceTasks"))
    manual_review_tasks = safe_list(intelligence.get("manualReviewTasks"))
    if risky_fallback_tasks:
        sample = risky_fallback_tasks[0]
        sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
        sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
        keyword = (
            safe_list(sample_intelligence.get("matchedKeywords"))[:1]
            or safe_list(sample.get("title", "").split())[:1]
        )
        actions = [
            {
                "type": "create_policy",
                "label": "补分流规则",
                "payload": {
                    "name": f"补充 {sample.get('id', '任务')} 分流",
                    "strategyType": "keyword_department",
                    "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                    "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                    "priorityLevel": sample_route.get("priorityLevel", "high"),
                    "queueName": "",
                    "status": "active",
                },
            }
        ]
        review_sample = low_confidence_tasks[0] if low_confidence_tasks else (manual_review_tasks[0] if manual_review_tasks else None)
        if review_sample:
            actions.append(
                build_management_approval_action(
                    review_sample,
                    "推荐修复包：默认回退和低把握任务同时偏高，建议补分流并前置人工复核。",
                )
            )
        repair_bundles.append(
            {
                "title": "先收紧高风险回退",
                "detail": f"这一组动作会先补分流规则，再把低把握任务前置人工复核，优先收住高风险默认回退。",
                "actions": actions[:2],
            }
        )
    if int(linked_review["summary"].get("followUpCount") or 0) > 0:
        actions = [{"type": "open_orchestration", "label": "查看联动复盘", "path": "/orchestration"}]
        if low_confidence_tasks:
            actions.append(
                build_management_approval_action(
                    low_confidence_tasks[0],
                    "推荐修复包：联动链路仍需跟进，建议继续把低把握任务前置人工复核。",
                )
            )
        repair_bundles.append(
            {
                "title": "继续收口联动链路",
                "detail": f"当前还有 {linked_review['summary'].get('followUpCount', 0)} 条 workflow 没稳住，这组动作会带你先看联动复盘，再补人工复核兜底。",
                "actions": actions[:2],
            }
        )

    day_buckets = {}
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).strftime("%m-%d")
        day_buckets[day] = {
            "date": day,
            "total": 0,
            "lowConfidenceCount": 0,
            "manualReviewCount": 0,
            "blockedCount": 0,
            "completedCount": 0,
            "confidenceTotal": 0.0,
            "confidenceCount": 0,
        }
    for task in safe_list(task_index):
        route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        if not route:
            continue
        updated_at = parse_iso(task.get("updatedAt")) or now
        day = updated_at.strftime("%m-%d")
        if day not in day_buckets:
            continue
        bucket = day_buckets[day]
        bucket["total"] += 1
        confidence = float(route.get("confidence") or (route.get("intelligence") or {}).get("confidence") or 0)
        if confidence:
            bucket["confidenceTotal"] += confidence
            bucket["confidenceCount"] += 1
            if confidence < 0.65:
                bucket["lowConfidenceCount"] += 1
        if bool(route.get("manualReview") or (route.get("intelligence") or {}).get("manualReview")):
            bucket["manualReviewCount"] += 1
        if task.get("blocked"):
            bucket["blockedCount"] += 1
        elif str(task.get("state") or "").lower() in TERMINAL_STATES:
            bucket["completedCount"] += 1
    trend = []
    for bucket in day_buckets.values():
        total = max(bucket["total"], 1)
        evaluated = max(bucket["completedCount"] + bucket["blockedCount"], 1)
        trend_score = max(
            0,
            min(
                100,
                int(
                    round(
                        100
                        - min(bucket["lowConfidenceCount"] * 12, 36)
                        - min(int(round((bucket["blockedCount"] / evaluated) * 100)) * 0.4, 24)
                        + min(int(round((bucket["completedCount"] / evaluated) * 100)) * 0.15, 12)
                    )
                ),
            ),
        )
        trend.append(
            {
                "date": bucket["date"],
                "score": trend_score,
                "avgConfidence": int(round((bucket["confidenceTotal"] / bucket["confidenceCount"]) * 100)) if bucket["confidenceCount"] else 0,
                "lowConfidenceCount": bucket["lowConfidenceCount"],
                "manualReviewCount": bucket["manualReviewCount"],
                "blockRate": int(round((bucket["blockedCount"] / evaluated) * 100)) if evaluated else 0,
                "completionRate": int(round((bucket["completedCount"] / evaluated) * 100)) if evaluated else 0,
            }
        )
    trend_direction = "stable"
    if len(trend) >= 2:
        delta = trend[-1]["score"] - trend[0]["score"]
        if delta >= 8:
            trend_direction = "up"
        elif delta <= -8:
            trend_direction = "down"

    bundle_review = build_recommendation_bundle_review(
        openclaw_dir,
        now,
        {
            "score": score,
            "avgConfidence": avg_confidence,
            "lowConfidenceCount": int(intelligence.get("lowConfidenceCount") or 0),
            "manualReviewCount": int(intelligence.get("manualReviewCount") or 0),
            "completionRate": int(decision_quality.get("completionRate") or 0),
            "blockRate": int(decision_quality.get("blockRate") or 0),
            "linkedFollowUpCount": int(linked_review["summary"].get("followUpCount") or 0),
            "riskyFallbackCount": int(intelligence.get("riskyFallbackCount") or 0),
        },
    )
    bundle_follow_ups = build_recommendation_bundle_follow_ups(
        bundle_review,
        risky_fallback_tasks,
        low_confidence_tasks,
        manual_review_tasks,
        linked_review,
    )
    bundle_follow_up_trend = build_recommendation_bundle_follow_up_trend(bundle_review, now)
    bundle_follow_up_breakdown = build_recommendation_bundle_follow_up_breakdown(
        bundle_review,
        now,
        workflows,
        risky_fallback_tasks,
        low_confidence_tasks,
        manual_review_tasks,
        linked_review,
    )
    bundle_priority_queue = build_recommendation_bundle_priority_queue(bundle_follow_up_breakdown)
    bundle_priority_review = build_recommendation_bundle_priority_review(bundle_priority_queue, bundle_follow_up_breakdown)
    bundle_priority_handoff = build_recommendation_bundle_priority_handoff(bundle_priority_queue, bundle_priority_review)
    bundle_operating_summary = build_recommendation_operating_summary(
        bundle_priority_queue,
        bundle_priority_review,
        bundle_priority_handoff,
    )

    return {
        "score": score,
        "summary": {
            "avgConfidence": avg_confidence,
            "lowConfidenceCount": intelligence.get("lowConfidenceCount", 0),
            "manualReviewCount": intelligence.get("manualReviewCount", 0),
            "completionRate": decision_quality.get("completionRate", 0),
            "blockRate": decision_quality.get("blockRate", 0),
            "linkedStableCount": linked_review["summary"].get("stabilizedCount", 0),
            "linkedFollowUpCount": linked_review["summary"].get("followUpCount", 0),
        },
        "bands": {
            "understanding": confidence_band,
            "routing": routing_band,
            "orchestration": linked_band,
        },
        "items": [
            {
                "title": "任务理解",
                "score": avg_confidence,
                "band": confidence_band,
                "detail": f"低把握 {intelligence.get('lowConfidenceCount', 0)} 条 · 人工复核 {intelligence.get('manualReviewCount', 0)} 条",
            },
            {
                "title": "分流决策",
                "score": decision_quality.get("completionRate", 0),
                "band": routing_band,
                "detail": f"收口 {decision_quality.get('completionRate', 0)}% · 阻塞 {decision_quality.get('blockRate', 0)}%",
            },
            {
                "title": "流程联动",
                "score": max(0, 100 - int(linked_review['summary'].get('followUpCount', 0)) * 20),
                "band": linked_band,
                "detail": f"双线趋稳 {linked_review['summary'].get('stabilizedCount', 0)} 条 · 待跟进 {linked_review['summary'].get('followUpCount', 0)} 条",
            },
        ],
        "trend": trend,
        "trendDirection": trend_direction,
        "causes": causes[:4],
        "repairBundles": repair_bundles[:3],
        "bundleReview": bundle_review,
        "bundleFollowUps": bundle_follow_ups[:3],
        "bundleFollowUpTrend": bundle_follow_up_trend,
        "bundleFollowUpBreakdown": bundle_follow_up_breakdown,
        "bundlePriorityQueue": bundle_priority_queue,
        "bundlePriorityReview": bundle_priority_review,
        "bundlePriorityHandoff": bundle_priority_handoff,
        "bundleOperatingSummary": bundle_operating_summary,
        "suggestions": suggestions[:3],
    }


def build_recommendation_bundle_review(openclaw_dir, now, metrics):
    relevant_actions = {
        "orchestration_policy_save": "补分流规则",
        "orchestration_workflow_insert_approval": "前置人工复核",
        "orchestration_workflow_strengthen_handoff": "强化交接模板",
    }
    events = [
        item
        for item in load_audit_events(openclaw_dir, limit=180)
        if item.get("outcome") == "success" and item.get("action") in relevant_actions
    ]
    if not events:
        return {"summary": {"total": 0, "stabilizedCount": 0, "watchCount": 0, "followUpCount": 0}, "rows": []}

    grouped = []
    ordered = sorted(events, key=lambda item: (str(item.get("at") or ""), str(item.get("id") or "")))
    for event in ordered:
        event_at = parse_iso(event.get("at"))
        actor = event.get("actor") if isinstance(event.get("actor"), dict) else {}
        actor_name = str(actor.get("displayName") or actor.get("username") or "system").strip()
        if grouped:
            previous = grouped[-1]
            previous_at = previous.get("lastAt")
            if (
                event_at
                and previous_at
                and actor_name == previous.get("actorName")
                and (event_at - previous_at).total_seconds() <= 240
            ):
                previous["events"].append(event)
                previous["lastAt"] = event_at
                continue
        grouped.append(
            {
                "actorName": actor_name,
                "events": [event],
                "lastAt": event_at,
            }
        )

    def _verdict_for(actions):
        actions = set(actions)
        risky_fallback_count = int(metrics.get("riskyFallbackCount") or 0)
        low_confidence_count = int(metrics.get("lowConfidenceCount") or 0)
        manual_review_count = int(metrics.get("manualReviewCount") or 0)
        completion_rate = int(metrics.get("completionRate") or 0)
        block_rate = int(metrics.get("blockRate") or 0)
        linked_follow_up_count = int(metrics.get("linkedFollowUpCount") or 0)
        score = int(metrics.get("score") or 0)

        if "orchestration_policy_save" in actions:
            if risky_fallback_count == 0 and completion_rate >= 78 and block_rate < 18:
                return "stabilized"
            if risky_fallback_count > 0 or block_rate >= 28:
                return "follow_up"
        if "orchestration_workflow_insert_approval" in actions:
            if low_confidence_count <= 1 and manual_review_count <= 1 and completion_rate >= 75:
                return "stabilized"
            if low_confidence_count >= 4 or block_rate >= 30:
                return "follow_up"
        if "orchestration_workflow_strengthen_handoff" in actions:
            if linked_follow_up_count == 0 and block_rate < 18:
                return "stabilized"
            if linked_follow_up_count >= 2:
                return "follow_up"
        if score >= 82 and linked_follow_up_count == 0 and block_rate < 16:
            return "stabilized"
        if score < 68 or linked_follow_up_count > 0 or block_rate >= 25:
            return "follow_up"
        return "watch"

    rows = []
    for group in reversed(grouped[-6:]):
        actions = [str(item.get("action") or "").strip() for item in group["events"]]
        labels = []
        for action in actions:
            label = relevant_actions.get(action)
            if label and label not in labels:
                labels.append(label)
        latest = group["events"][-1]
        latest_at = parse_iso(latest.get("at"))
        verdict = _verdict_for(actions)
        rows.append(
            {
                "title": " + ".join(labels[:3]) if labels else "修复包执行",
                "detail": f"当前综合得分 {int(metrics.get('score') or 0)} · 分流收口 {int(metrics.get('completionRate') or 0)}% · 联动待跟进 {int(metrics.get('linkedFollowUpCount') or 0)} 条",
                "actorName": group.get("actorName") or "system",
                "appliedAt": latest.get("at", ""),
                "appliedAgo": format_age(latest_at, now) if latest_at else "",
                "actionCount": len(group["events"]),
                "actions": labels,
                "verdict": verdict,
            }
        )

    return {
        "summary": {
            "total": len(rows),
            "stabilizedCount": sum(1 for item in rows if item["verdict"] == "stabilized"),
            "watchCount": sum(1 for item in rows if item["verdict"] == "watch"),
            "followUpCount": sum(1 for item in rows if item["verdict"] == "follow_up"),
        },
        "rows": rows,
    }


def build_recommendation_bundle_follow_ups(bundle_review, risky_fallback_tasks, low_confidence_tasks, manual_review_tasks, linked_review):
    rows = safe_list((bundle_review or {}).get("rows"))
    follow_ups = []
    review_task = low_confidence_tasks[0] if low_confidence_tasks else (manual_review_tasks[0] if manual_review_tasks else None)
    for row in rows:
        verdict = str(row.get("verdict") or "").strip()
        if verdict not in {"follow_up", "watch"}:
            continue
        actions = []
        labels = [str(item or "").strip() for item in safe_list(row.get("actions")) if str(item or "").strip()]
        title = str(row.get("title") or "修复包").strip()
        if "补分流规则" in labels and risky_fallback_tasks:
            sample = risky_fallback_tasks[0]
            sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
            sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
            keyword = (
                safe_list(sample_intelligence.get("matchedKeywords"))[:1]
                or safe_list(sample.get("title", "").split())[:1]
            )
            actions.append(
                {
                    "type": "create_policy",
                    "label": "继续补分流规则",
                    "payload": {
                        "name": f"继续收口 {sample.get('id', '任务')} 分流",
                        "strategyType": "keyword_department",
                        "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                        "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                        "priorityLevel": sample_route.get("priorityLevel", "high"),
                        "queueName": "",
                        "status": "active",
                    },
                }
            )
        if "前置人工复核" in labels and review_task:
            actions.append(
                build_management_approval_action(
                    review_task,
                    f"{title} 复盘后仍需跟进，继续把低把握任务前置人工复核。",
                )
            )
        if "强化交接模板" in labels or int(linked_review["summary"].get("followUpCount") or 0) > 0:
            actions.append({"type": "open_orchestration", "label": "继续看联动复盘", "path": "/orchestration"})
        if not actions:
            continue
        follow_ups.append(
            {
                "title": f"{title} 还需续修",
                "detail": (
                    "这组修复包执行后还没有完全稳住，建议按顺序继续收紧分流、补人工复核或回到编排页继续收口。"
                    if verdict == "follow_up"
                    else "这组修复包还在观察期，建议先做一轮补强，再继续看趋势变化。"
                ),
                "verdict": verdict,
                "actions": actions[:3],
            }
        )
    return follow_ups


def build_recommendation_bundle_follow_up_trend(bundle_review, now):
    rows = safe_list((bundle_review or {}).get("rows"))
    buckets = {}
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).strftime("%m-%d")
        buckets[day] = {
            "date": day,
            "stabilizedCount": 0,
            "watchCount": 0,
            "followUpCount": 0,
            "actionCount": 0,
            "score": 70,
        }
    for row in rows:
        applied_at = parse_iso(row.get("appliedAt"))
        if not applied_at:
            continue
        day = applied_at.strftime("%m-%d")
        if day not in buckets:
            continue
        bucket = buckets[day]
        verdict = str(row.get("verdict") or "").strip()
        if verdict == "stabilized":
            bucket["stabilizedCount"] += 1
        elif verdict == "follow_up":
            bucket["followUpCount"] += 1
        else:
            bucket["watchCount"] += 1
        bucket["actionCount"] += int(row.get("actionCount") or 0)
    trend = []
    for bucket in buckets.values():
        score = 70 + bucket["stabilizedCount"] * 12 - bucket["followUpCount"] * 14 - bucket["watchCount"] * 4
        bucket["score"] = max(0, min(100, score))
        trend.append(bucket)
    direction = "stable"
    if len(trend) >= 2:
        delta = int(trend[-1]["score"] or 0) - int(trend[0]["score"] or 0)
        if delta >= 8:
            direction = "up"
        elif delta <= -8:
            direction = "down"
    return {
        "direction": direction,
        "rows": trend,
    }


def build_recommendation_bundle_follow_up_breakdown(bundle_review, now, workflows, risky_fallback_tasks, low_confidence_tasks, manual_review_tasks, linked_review):
    rows = safe_list((bundle_review or {}).get("rows"))
    groups = [
        ("补分流规则", "分流规则"),
        ("前置人工复核", "人工复核"),
        ("强化交接模板", "交接模板"),
    ]
    result = []
    for label, title in groups:
        matched = [
            row for row in rows if label in [str(item or "").strip() for item in safe_list(row.get("actions"))]
        ]
        review_task = low_confidence_tasks[0] if low_confidence_tasks else (manual_review_tasks[0] if manual_review_tasks else None)
        actions = []
        if label == "补分流规则" and risky_fallback_tasks:
            sample = risky_fallback_tasks[0]
            sample_route = sample.get("routeDecision") if isinstance(sample.get("routeDecision"), dict) else {}
            sample_intelligence = sample_route.get("intelligence") if isinstance(sample_route.get("intelligence"), dict) else {}
            keyword = (
                safe_list(sample_intelligence.get("matchedKeywords"))[:1]
                or safe_list(sample.get("title", "").split())[:1]
            )
            actions.append(
                {
                    "type": "create_policy",
                    "label": "补分流规则",
                    "payload": {
                        "name": f"继续收口 {sample.get('id', '任务')} 分流",
                        "strategyType": "keyword_department",
                        "keyword": (keyword[0] if keyword else "").strip(",.，。 "),
                        "targetAgentId": sample.get("targetAgentId") or sample_route.get("targetAgentId", ""),
                        "priorityLevel": sample_route.get("priorityLevel", "high"),
                        "queueName": "",
                        "status": "active",
                    },
                }
            )
        elif label == "前置人工复核" and review_task:
            actions.append(
                build_management_approval_action(
                    review_task,
                    "按原因续修：当前人工复核链路仍需加强，建议继续把低把握任务前置人工复核。",
                )
            )
        elif label == "强化交接模板":
            target_row = next((row for row in safe_list(linked_review.get("rows")) if row.get("overallVerdict") == "follow_up"), None)
            target_workflow_id = str((target_row or {}).get("workflowId") or "").strip()
            target_workflow = next((workflow for workflow in safe_list(workflows) if str(workflow.get("id") or "").strip() == target_workflow_id), None)
            if not target_workflow:
                target_workflow = next((workflow for workflow in safe_list(workflows) if find_workflow_weak_handoff_node(workflow)), None)
            weak_node = find_workflow_weak_handoff_node(target_workflow) if target_workflow else None
            if target_workflow and weak_node:
                actions.append(
                    {
                        "type": "strengthen_handoff_note",
                        "label": "强化交接模板",
                        "payload": {
                            "workflowId": str(target_workflow.get("id") or "").strip(),
                            "nodeId": str(weak_node.get("id") or "").strip(),
                            "title": str(weak_node.get("title") or weak_node.get("name") or "交接节点").strip(),
                            "reason": "按原因续修：交接模板仍然偏弱，建议继续补齐结构化交接清单。",
                        },
                    }
                )
            else:
                actions.append({"type": "open_orchestration", "label": "查看联动复盘", "path": "/orchestration"})
        if not matched:
            result.append(
                {
                    "key": label,
                    "title": title,
                    "direction": "stable",
                    "summary": {"stabilizedCount": 0, "watchCount": 0, "followUpCount": 0, "actionCount": 0},
                    "latest": None,
                    "actions": actions[:2],
                }
            )
            continue
        stabilized_count = sum(1 for row in matched if row.get("verdict") == "stabilized")
        watch_count = sum(1 for row in matched if row.get("verdict") == "watch")
        follow_up_count = sum(1 for row in matched if row.get("verdict") == "follow_up")
        action_count = sum(int(row.get("actionCount") or 0) for row in matched)
        direction = "stable"
        if stabilized_count > follow_up_count and follow_up_count == 0:
            direction = "up"
        elif follow_up_count > 0:
            direction = "down"
        latest = None
        latest_at = None
        for row in matched:
            row_at = parse_iso(row.get("appliedAt"))
            if row_at and (latest_at is None or row_at > latest_at):
                latest_at = row_at
                latest = row
        result.append(
            {
                "key": label,
                "title": title,
                "direction": direction,
                "summary": {
                    "stabilizedCount": stabilized_count,
                    "watchCount": watch_count,
                    "followUpCount": follow_up_count,
                    "actionCount": action_count,
                },
                "latest": {
                    "title": latest.get("title", "") if latest else "",
                    "appliedAgo": format_age(latest_at, now) if latest_at else "",
                    "verdict": latest.get("verdict", "watch") if latest else "watch",
                } if latest else None,
                "actions": actions[:2],
            }
        )
    return result


def build_recommendation_bundle_priority_queue(breakdown):
    items = []
    for item in safe_list(breakdown):
        summary = item.get("summary") if isinstance(item.get("summary"), dict) else {}
        follow_up_count = int(summary.get("followUpCount") or 0)
        watch_count = int(summary.get("watchCount") or 0)
        stabilized_count = int(summary.get("stabilizedCount") or 0)
        action_count = int(summary.get("actionCount") or 0)
        direction = str(item.get("direction") or "stable").strip()
        score = follow_up_count * 10 + watch_count * 4 + action_count
        if direction == "down":
            score += 8
        elif direction == "up":
            score -= 6
        priority = "watch"
        if score >= 20 or follow_up_count >= 2:
            priority = "critical"
        elif score <= 0 and stabilized_count > 0:
            priority = "stable"
        title = str(item.get("title") or "续修原因").strip()
        detail = (
            f"{title} 当前待跟进 {follow_up_count} 项，观察中 {watch_count} 项。"
            if priority != "stable"
            else f"{title} 当前已经基本稳住，可以降低优先级。"
        )
        items.append(
            {
                "title": title,
                "priority": priority,
                "score": max(score, 0),
                "detail": detail,
                "actions": safe_list(item.get("actions"))[:2],
            }
        )
    items.sort(
        key=lambda item: (
            0 if item["priority"] == "critical" else 1 if item["priority"] == "watch" else 2,
            -int(item.get("score") or 0),
            item.get("title", ""),
        )
    )
    return items[:3]


def build_recommendation_bundle_priority_review(priority_queue, breakdown):
    breakdown_map = {str(item.get("title") or "").strip(): item for item in safe_list(breakdown)}
    rows = []
    for item in safe_list(priority_queue):
        title = str(item.get("title") or "").strip()
        detail = breakdown_map.get(title) or {}
        latest = detail.get("latest") if isinstance(detail.get("latest"), dict) else {}
        summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
        direction = str(detail.get("direction") or "stable").strip()
        follow_up_count = int(summary.get("followUpCount") or 0)
        verdict = "watch"
        if direction == "up" and follow_up_count == 0:
            verdict = "stabilized"
        elif direction == "down" or follow_up_count > 0:
            verdict = "follow_up"
        rows.append(
            {
                "title": title,
                "priority": str(item.get("priority") or "watch").strip(),
                "verdict": verdict,
                "detail": (
                    f"最近优先处理的是 {title}，当前待跟进 {follow_up_count} 项。"
                    if verdict != "stabilized"
                    else f"{title} 这条优先链路已经基本稳住，可以把注意力转到下一类问题。"
                ),
                "latestAppliedAgo": str(latest.get("appliedAgo") or "").strip(),
            }
        )
    return rows


def build_recommendation_bundle_priority_handoff(priority_queue, priority_review):
    queue = safe_list(priority_queue)
    review_map = {str(item.get("title") or "").strip(): item for item in safe_list(priority_review)}
    if not queue:
        return None
    current = queue[0]
    current_title = str(current.get("title") or "").strip()
    current_review = review_map.get(current_title) or {}
    current_verdict = str(current_review.get("verdict") or "watch").strip()
    if current_verdict != "stabilized":
        return {
            "status": "stay",
            "title": current_title,
            "detail": f"{current_title} 还没稳住，先继续收这一类问题。",
            "actions": safe_list(current.get("actions"))[:2],
        }
    next_item = next(
        (
            item
            for item in queue[1:]
            if str((review_map.get(str(item.get('title') or '').strip()) or {}).get("verdict") or "watch").strip() != "stabilized"
        ),
        None,
    )
    if next_item:
        next_title = str(next_item.get("title") or "").strip()
        return {
            "status": "switch",
            "title": next_title,
            "detail": f"{current_title} 已经基本稳住，建议把下一优先级切到 {next_title}。",
            "actions": safe_list(next_item.get("actions"))[:2],
        }
    return {
        "status": "done",
        "title": current_title,
        "detail": "当前优先队列里的问题都已经基本稳住，可以把注意力转到更长期的优化项。",
        "actions": [],
    }


def build_recommendation_operating_summary(priority_queue, priority_review, priority_handoff):
    queue = safe_list(priority_queue)
    review_map = {str(item.get("title") or "").strip(): item for item in safe_list(priority_review)}
    current = queue[0] if queue else {}
    current_title = str(current.get("title") or "当前无优先项").strip()
    current_priority = str(current.get("priority") or "stable").strip()
    current_review = review_map.get(current_title) or {}
    current_verdict = str(current_review.get("verdict") or "watch").strip()
    handoff = priority_handoff if isinstance(priority_handoff, dict) else {}
    status = "stable"
    if current_priority == "critical" or current_verdict == "follow_up":
        status = "critical"
    elif current_priority == "watch" or current_verdict == "watch":
        status = "watch"
    headline = f"先盯住 {current_title}"
    detail = str(current.get("detail") or "").strip() or "当前没有新的优先续修项。"
    next_title = ""
    if str(handoff.get("status") or "").strip() == "switch":
        next_title = str(handoff.get("title") or "").strip()
    elif str(handoff.get("status") or "").strip() == "done":
        next_title = "长期优化"
    action_bundle = safe_list(current.get("actions"))[:2]
    if not action_bundle and safe_list(handoff.get("actions")):
        action_bundle = safe_list(handoff.get("actions"))[:2]
    return {
        "headline": headline,
        "status": status,
        "currentTitle": current_title,
        "currentPriority": current_priority,
        "currentVerdict": current_verdict,
        "detail": detail,
        "nextTitle": next_title,
        "handoffStatus": str(handoff.get("status") or "").strip() or "stay",
        "handoffDetail": str(handoff.get("detail") or "").strip(),
        "actions": action_bundle,
    }


def build_orchestration_adjustment_review(workflows, workflow_review, now):
    review_map = {
        str(item.get("workflowId") or "").strip(): item
        for item in safe_list(workflow_review)
        if str(item.get("workflowId") or "").strip()
    }
    rows = []
    for workflow in safe_list(workflows):
        workflow_id = str(workflow.get("id") or "").strip()
        if not workflow_id:
            continue
        meta = workflow.get("meta") if isinstance(workflow.get("meta"), dict) else {}
        adjustments = safe_list(meta.get("recommendedAdjustments"))
        if not adjustments:
            continue
        latest = next((item for item in adjustments if isinstance(item, dict)), None)
        if not latest:
            continue
        latest_at = parse_iso(latest.get("appliedAt"))
        review = review_map.get(workflow_id, {})
        block_rate = int(review.get("blockRate") or 0)
        context_loss = int(review.get("contextLossCount") or 0)
        verdict = "watch"
        if block_rate < 20 and context_loss <= 1:
            verdict = "stabilized"
        elif block_rate >= 35 or context_loss >= 3:
            verdict = "follow_up"
        rows.append(
            {
                "workflowId": workflow_id,
                "workflowName": workflow.get("name", workflow_id),
                "adjustmentType": str(latest.get("type") or "").strip(),
                "adjustmentLabel": (
                    "强化交接模板"
                    if str(latest.get("type") or "").strip() == "strengthen_handoff_note"
                    else "加入人工复核"
                    if str(latest.get("type") or "").strip() == "insert_approval_node"
                    else "流程调整"
                ),
                "targetLabel": str(latest.get("nodeTitle") or latest.get("targetLaneTitle") or "").strip(),
                "reason": str(latest.get("reason") or "").strip(),
                "appliedAt": latest.get("appliedAt", ""),
                "appliedAgo": format_age(latest_at, now) if latest_at else "",
                "blockRate": block_rate,
                "contextLossCount": context_loss,
                "verdict": verdict,
            }
        )
    rows.sort(
        key=lambda item: (
            0 if item["verdict"] == "follow_up" else 1 if item["verdict"] == "watch" else 2,
            -item["contextLossCount"],
            -item["blockRate"],
            item["workflowName"],
        )
    )
    return {
        "summary": {
            "total": len(rows),
            "stabilizedCount": sum(1 for item in rows if item["verdict"] == "stabilized"),
            "watchCount": sum(1 for item in rows if item["verdict"] == "watch"),
            "followUpCount": sum(1 for item in rows if item["verdict"] == "follow_up"),
        },
        "rows": rows[:8],
    }


def task_effective_team_id(task):
    if not isinstance(task, dict):
        return ""
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if not route_meta and isinstance(task.get("meta"), dict):
        meta_route = (task.get("meta") or {}).get("routeDecision")
        if isinstance(meta_route, dict):
            route_meta = meta_route
    team_selection = route_meta.get("teamSelection") if isinstance(route_meta.get("teamSelection"), dict) else {}
    team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
    return str(
        task.get("teamId")
        or team_selection.get("selectedTeamId")
        or team_assignment.get("teamId")
        or route_meta.get("teamId")
        or ""
    ).strip()


def team_reference_payload(team_map, team_id="", fallback_name=""):
    normalized_team_id = str(team_id or "").strip()
    team = team_map.get(normalized_team_id, {}) if isinstance(team_map, dict) and normalized_team_id else {}
    resolved_team_id = normalized_team_id or str((team or {}).get("id") or "").strip()
    resolved_team_name = (
        str((team or {}).get("name") or "").strip()
        or str(fallback_name or "").strip()
        or resolved_team_id
    )
    if not resolved_team_id and not resolved_team_name:
        return {}
    return {
        "teamId": resolved_team_id,
        "teamName": resolved_team_name,
    }


def default_execution_team_id(team_map, mode=""):
    normalized_mode = str(mode or "").strip().lower()
    preferred_ids = [
        recommended_team_id_for_pack_mode(normalized_mode),
        "team-delivery",
        "team-core",
        "team-release",
        "team-signals",
    ]
    for candidate in preferred_ids:
        if candidate and candidate in (team_map or {}):
            return candidate
    return next(iter((team_map or {}).keys()), "")


def build_team_ownership_payload(team_map, execution_team_id="", recommended_team_id="", mode="", source="derived"):
    team_map = team_map if isinstance(team_map, dict) else {}
    normalized_mode = str(mode or "").strip().lower()
    recommended_execution_team_id = str(recommended_team_id or "").strip() or recommended_team_id_for_pack_mode(normalized_mode)
    resolved_execution_team_id = (
        str(execution_team_id or "").strip()
        or recommended_execution_team_id
        or default_execution_team_id(team_map, normalized_mode)
    )
    command_team_id = "team-core" if "team-core" in team_map else resolved_execution_team_id
    gate_team_id = "team-release" if "team-release" in team_map else resolved_execution_team_id
    signals_team_id = "team-signals" if "team-signals" in team_map else resolved_execution_team_id

    command_team = team_reference_payload(team_map, command_team_id)
    execution_team = team_reference_payload(team_map, resolved_execution_team_id)
    gate_team = team_reference_payload(team_map, gate_team_id)
    signals_team = team_reference_payload(team_map, signals_team_id)
    recommended_execution_team = team_reference_payload(team_map, recommended_execution_team_id)

    roles = []
    role_map = {
        "command": command_team,
        "execution": execution_team,
        "gate": gate_team,
        "signals": signals_team,
    }
    execution_role_id = str(execution_team.get("teamId") or "").strip()
    for role in TEAM_OWNERSHIP_ROLE_ORDER:
        team_ref = role_map.get(role) if isinstance(role_map.get(role), dict) else {}
        team_ref_id = str(team_ref.get("teamId") or "").strip()
        team_ref_name = str(team_ref.get("teamName") or "").strip()
        if not team_ref_id and not team_ref_name:
            continue
        roles.append(
            {
                "role": role,
                "teamId": team_ref_id,
                "teamName": team_ref_name or team_ref_id,
                "sameAsExecution": bool(role != "execution" and team_ref_id and team_ref_id == execution_role_id),
            }
        )

    return {
        "mode": normalized_mode,
        "source": str(source or "derived").strip() or "derived",
        "commandTeam": command_team,
        "executionTeam": execution_team,
        "gateTeam": gate_team,
        "signalsTeam": signals_team,
        "recommendedExecutionTeam": recommended_execution_team,
        "executionDiffersFromRecommended": bool(
            recommended_execution_team.get("teamId")
            and execution_team.get("teamId")
            and recommended_execution_team.get("teamId") != execution_team.get("teamId")
        ),
        "roles": roles,
    }


def enrich_task_team_ownership(task_items, team_map):
    for task in safe_list(task_items):
        if not isinstance(task, dict):
            continue
        linked_run = task.get("linkedRun") if isinstance(task.get("linkedRun"), dict) else {}
        linked_pack = linked_run.get("linkedPack") if isinstance(linked_run.get("linkedPack"), dict) else {}
        route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        team_selection = route_meta.get("teamSelection") if isinstance(route_meta.get("teamSelection"), dict) else {}
        execution_team_id = (
            str(task.get("teamId") or "").strip()
            or str((task.get("linkedTeam") or {}).get("id") or "").strip()
            or str(linked_run.get("linkedTeamId") or "").strip()
            or str((linked_run.get("linkedTeam") or {}).get("id") or "").strip()
            or str(team_selection.get("selectedTeamId") or "").strip()
        )
        recommended_team_id = (
            str(team_selection.get("recommendedTeamId") or "").strip()
            or str(linked_pack.get("recommendedTeamId") or "").strip()
        )
        mode = str(linked_pack.get("mode") or "").strip()
        source = "run" if linked_run.get("id") else "task"
        task["teamOwnership"] = build_team_ownership_payload(
            team_map,
            execution_team_id=execution_team_id,
            recommended_team_id=recommended_team_id,
            mode=mode,
            source=source,
        )


def build_orchestration_replay(task):
    replay = sorted(
        [item for item in (task.get("replay") or []) if isinstance(item, dict)],
        key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
    )
    if not replay:
        workflow_binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else {}
        selected_branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
        entries = []
        if selected_branch:
            entries.append(
                {
                    "kind": "branch",
                    "at": task.get("updatedAt", ""),
                    "actorLabel": selected_branch.get("sourceNodeTitle") or "Workflow",
                    "targetLabel": selected_branch.get("targetLaneTitle") or selected_branch.get("targetNodeTitle") or "",
                    "headline": f"命中分支 · {selected_branch.get('label') or selected_branch.get('expression') or 'branch'}",
                    "detail": selected_branch.get("expression") or selected_branch.get("label") or "",
                    "durationToNextMinutes": 0,
                    "contextPacket": {"summary": "创建任务时已命中条件分支。", "risk": "good"},
                }
            )
        return {
            "taskId": task.get("id", ""),
            "title": task.get("title", ""),
            "entries": entries,
            "durationMinutes": 0,
            "initiator": task.get("route", [task.get("currentAgentLabel", "")])[0] if task.get("route") else task.get("currentAgentLabel", ""),
            "owner": task.get("currentAgentLabel", ""),
            "contextLossCount": 0,
        }
    first_dt = parse_iso(replay[0].get("at"))
    last_dt = parse_iso(replay[-1].get("at"))
    entries = []
    context_loss_count = 0
    for index, entry in enumerate(replay):
        next_dt = parse_iso(replay[index + 1].get("at")) if index + 1 < len(replay) else None
        current_dt = parse_iso(entry.get("at"))
        packet = summarize_context_packet(entry)
        if packet["risk"] != "good":
            context_loss_count += 1
        entries.append(
            {
                **entry,
                "durationToNextMinutes": round(max((next_dt - current_dt).total_seconds(), 0) / 60, 1) if current_dt and next_dt else 0,
                "contextPacket": packet,
            }
        )
    return {
        "taskId": task.get("id", ""),
        "title": task.get("title", ""),
        "entries": entries,
        "durationMinutes": round(max((last_dt - first_dt).total_seconds(), 0) / 60, 1) if first_dt and last_dt else 0,
        "initiator": (replay[0].get("actorLabel") or task.get("route", [""])[0] or task.get("currentAgentLabel", "")).strip(),
        "owner": task.get("currentAgentLabel", ""),
        "contextLossCount": context_loss_count,
    }


def compact_orchestration_replay_entry(entry):
    entry = entry if isinstance(entry, dict) else {}
    packet = entry.get("contextPacket") if isinstance(entry.get("contextPacket"), dict) else {}
    return {
        "kind": str(entry.get("kind") or "").strip(),
        "headline": str(entry.get("headline") or "").strip(),
        "atAgo": str(entry.get("atAgo") or "").strip(),
        "agentId": str(entry.get("agentId") or entry.get("actorId") or "").strip(),
        "nodeId": str(entry.get("nodeId") or "").strip(),
        "durationToNextMinutes": entry.get("durationToNextMinutes", 0),
        "contextPacket": {
            "summary": str(packet.get("summary") or "").strip(),
            "risk": str(packet.get("risk") or "").strip(),
        },
    }


def compact_orchestration_replay_payload(replay):
    replay = replay if isinstance(replay, dict) else {}
    return {
        "taskId": str(replay.get("taskId") or "").strip(),
        "title": str(replay.get("title") or "").strip(),
        "entries": [
            compact_orchestration_replay_entry(entry)
            for entry in safe_list(replay.get("entries"))
            if isinstance(entry, dict)
        ],
        "durationMinutes": replay.get("durationMinutes", 0),
        "contextLossCount": replay.get("contextLossCount", 0),
    }


def build_orchestration_data(openclaw_dir, agents, task_index, router_agent_id, now, skills_data=None):
    workflows = store_list_orchestration_workflows(openclaw_dir)
    routing_policies = store_list_routing_policies(openclaw_dir)
    routing_decisions = store_list_routing_decisions(openclaw_dir, limit=180)
    management_runs = store_list_management_runs(openclaw_dir, limit=96)
    pack_map = workflow_pack_map_from_skills_payload(skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir))
    team_map = {
        item.get("id"): item
        for item in store_list_agent_teams(openclaw_dir)
        if isinstance(item, dict) and item.get("id")
    }
    if not workflows:
        workflows = [default_orchestration_workflow(agents, router_agent_id)]
    workflow_versions = []
    for workflow in workflows:
        versions = store_list_orchestration_workflow_versions(openclaw_dir, workflow_id=workflow.get("id"), limit=12)
        hydrated = hydrate_workflow_pack_context(workflow, pack_map)
        hydrated["teamOwnership"] = build_team_ownership_payload(
            team_map,
            execution_team_id=str((hydrated.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
            recommended_team_id=str((hydrated.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
            mode=str((hydrated.get("linkedPack") or {}).get("mode") or "").strip(),
            source="workflow",
        )
        hydrated["versions"] = versions
        hydrated["latestVersion"] = versions[0] if versions else None
        workflow_versions.extend(versions[:4])
        workflow.update(hydrated)
    replays = []
    for task in task_index[:24]:
        if not task.get("id"):
            continue
        replay = build_orchestration_replay(task)
        replay.update(
            {
                "state": task.get("state", ""),
                "updatedAgo": task.get("updatedAgo", ""),
                "route": task.get("route", []),
                "blocked": bool(task.get("blocked")),
            }
        )
        replays.append(replay)
    replays.sort(key=lambda item: item.get("durationMinutes", 0), reverse=True)

    strategy_summary = Counter(policy.get("strategyType", "unknown") for policy in routing_policies)
    policy_hits = Counter()
    trend_counter = Counter()
    intelligence_summary = build_task_intelligence_summary(task_index)
    decision_quality = build_routing_effectiveness_summary(task_index, routing_decisions)
    policy_trends = build_orchestration_policy_trends(routing_decisions, task_index)
    for item in routing_decisions:
        policy_name = item.get("policyName") or item.get("policyId") or "Router fallback"
        policy_hits[policy_name] += 1
        decided_at = parse_iso(item.get("decidedAt"))
        if decided_at:
            trend_counter[decided_at.strftime("%Y-%m-%d")] += 1
    planned_tasks = [task for task in task_index if planning_binding_from_payload(task)]
    planned_runs = [run for run in management_runs if planning_binding_from_payload(run)]
    planning_recent = sorted(
        [
            {
                "id": item.get("id") or binding.get("bundleId", ""),
                "title": item.get("title") or item.get("taskTitle") or binding.get("title", ""),
                "kind": binding.get("kind", "task"),
                "relativeDir": binding.get("relativeDir", ""),
                "progressPercent": binding.get("progressPercent", 0),
                "currentPhase": binding.get("currentPhase", ""),
                "updatedAt": binding.get("updatedAt", ""),
            }
            for item in [*planned_tasks, *planned_runs]
            for binding in [planning_binding_from_payload(item)]
        ],
        key=lambda entry: parse_iso(entry.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    context_hotspots = sorted(
        [
            {
                "taskId": item.get("taskId", ""),
                "title": item.get("title", ""),
                "contextLossCount": item.get("contextLossCount", 0),
                "owner": item.get("owner", ""),
                "durationMinutes": item.get("durationMinutes", 0),
            }
            for item in replays
            if item.get("contextLossCount", 0) > 0
        ],
        key=lambda item: (-item["contextLossCount"], -item["durationMinutes"]),
    )[:8]
    workflow_review = build_orchestration_workflow_review(workflows, task_index, replays)
    review_suggestions = build_orchestration_review_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index)
    adjustment_review = build_orchestration_adjustment_review(workflows, workflow_review, now)
    next_step_suggestions = build_orchestration_next_step_suggestions(workflows, workflow_review, adjustment_review)
    linked_suggestions = build_orchestration_linked_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index)
    linked_review = build_orchestration_linked_review(workflows, routing_policies, policy_trends, workflow_review, task_index)
    return {
        "summary": {
            "workflowCount": len(workflows),
            "activePolicies": sum(1 for item in routing_policies if item.get("status") == "active"),
            "replayCount": len(replays),
            "contextLossHotspots": len(context_hotspots),
            "strategyBreakdown": dict(strategy_summary),
            "routingDecisionCount": len(routing_decisions),
            "plannedTaskCount": len(planned_tasks),
            "plannedRunCount": len(planned_runs),
            "manualReviewCount": intelligence_summary.get("manualReviewCount", 0),
            "lowConfidenceCount": intelligence_summary.get("lowConfidenceCount", 0),
            "riskyFallbackCount": intelligence_summary.get("riskyFallbackCount", 0),
            "decisionEvaluatedCount": decision_quality.get("evaluatedCount", 0),
            "decisionCompletionRate": decision_quality.get("completionRate", 0),
            "decisionBlockRate": decision_quality.get("blockRate", 0),
        },
        "workflows": workflows,
        "workflowVersions": workflow_versions[:24],
        "routingPolicies": routing_policies,
        "routingDecisions": [
            compact_orchestration_routing_decision(item)
            for item in routing_decisions[:48]
        ],
        "routingHitLeaders": [
            {"policyName": name, "count": count}
            for name, count in policy_hits.most_common(8)
        ],
        "policyTrends": policy_trends,
        "routingTrend": [
            {"date": date, "count": trend_counter[date]}
            for date in sorted(trend_counter.keys())[-7:]
        ],
        "planning": {
            "taskCount": len(planned_tasks),
            "runCount": len(planned_runs),
            "coverage": int(round((len(planned_tasks) / max(len(task_index), 1)) * 100)) if task_index else 0,
            "recent": planning_recent[:8],
        },
        "intelligence": intelligence_summary,
        "decisionQuality": decision_quality,
        "workflowReview": workflow_review,
        "adjustmentReview": adjustment_review,
        "reviewSuggestions": review_suggestions,
        "nextStepSuggestions": next_step_suggestions,
        "linkedSuggestions": linked_suggestions,
        "linkedReview": linked_review,
        "replays": [
            compact_orchestration_replay_payload(item)
            for item in replays[:18]
        ],
        "contextHotspots": context_hotspots,
        "commands": [
            {
                "label": "路由 Agent 现状",
                "command": f'OPENCLAW_STATE_DIR="{openclaw_dir}" openclaw agent --agent {router_agent_id} --message "summarize routing policy" --json',
                "description": "直接向当前路由 Agent 询问现有调度逻辑。",
            }
        ],
    }
