from __future__ import annotations

import sys

from .bootstrap_defaults import (
    TEAM_CONVERSATION_JOB_TITLE_PRIORITY,
    TEAM_CONVERSATION_ROLE_PRIORITY,
    clean_unique_strings,
    merged_agent_runtime_profile,
)


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


TEAM_CONVERSATION_BROADCAST_REPLY_LIMIT = _DelegatedSymbol("TEAM_CONVERSATION_BROADCAST_REPLY_LIMIT")
TEAM_CONVERSATION_RELAY_REPLY_LIMIT = _DelegatedSymbol("TEAM_CONVERSATION_RELAY_REPLY_LIMIT")
TEAM_CONVERSATION_SYNC_REPLY_LIMIT = _DelegatedSymbol("TEAM_CONVERSATION_SYNC_REPLY_LIMIT")
agent_runtime_overrides = _DelegatedSymbol("agent_runtime_overrides")
classify_team_collaboration_reply = _DelegatedSymbol("classify_team_collaboration_reply")
coordination_reply_entries = _DelegatedSymbol("coordination_reply_entries")
load_agents = _DelegatedSymbol("load_agents")
load_config = _DelegatedSymbol("load_config")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
safe_chat_mentions = _DelegatedSymbol("safe_chat_mentions")
safe_list = _DelegatedSymbol("safe_list")
team_collaboration_profile_key = _DelegatedSymbol("team_collaboration_profile_key")


def agent_runtime_profile_payload(openclaw_dir, agent_id, config=None, metadata=None, agents=None):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return {}
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    overrides = agent_runtime_overrides(metadata)
    override = overrides.get(normalized_agent_id) if isinstance(overrides.get(normalized_agent_id), dict) else {}
    return merged_agent_runtime_profile(normalized_agent_id, override=override)


def agent_identity_display_name(human_name="", job_title="", fallback=""):
    parts = [str(human_name or "").strip(), str(job_title or "").strip()]
    parts = [item for item in parts if item]
    if parts:
        return " · ".join(parts)
    return str(fallback or "").strip()


def agent_runtime_identity_payload(openclaw_dir, agent_id, config=None, metadata=None, agents=None):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return {
            "agentId": "",
            "humanName": "",
            "jobTitle": "",
            "roleLabel": "",
            "displayName": "",
        }
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    local_agents = safe_list(agents) if agents is not None else load_agents(config)
    agent = next(
        (
            item
            for item in local_agents
            if isinstance(item, dict) and str(item.get("id") or "").strip() == normalized_agent_id
        ),
        {},
    )
    runtime_profile = agent_runtime_profile_payload(
        openclaw_dir,
        normalized_agent_id,
        config=config,
        metadata=metadata,
        agents=local_agents,
    )
    human_name = str(
        runtime_profile.get("humanName")
        or ((agent.get("identity") or {}).get("name") if isinstance(agent.get("identity"), dict) else "")
        or normalized_agent_id
    ).strip()
    job_title = str(runtime_profile.get("jobTitle") or runtime_profile.get("roleLabel") or "").strip()
    return {
        "agentId": normalized_agent_id,
        "role": str(runtime_profile.get("role") or "").strip(),
        "humanName": human_name,
        "jobTitle": job_title,
        "roleLabel": str(runtime_profile.get("roleLabel") or "").strip(),
        "workingStyle": str(runtime_profile.get("workingStyle") or "").strip(),
        "displayName": agent_identity_display_name(human_name, job_title, fallback=normalized_agent_id),
    }


def agent_turn_priority_from_profile(runtime_profile):
    runtime_profile = runtime_profile if isinstance(runtime_profile, dict) else {}
    role = str(runtime_profile.get("role") or "").strip().lower()
    role_priority = TEAM_CONVERSATION_ROLE_PRIORITY.get(role, 9)
    job_title = str(runtime_profile.get("jobTitle") or "").strip()
    job_priority = 9
    for keywords, score in TEAM_CONVERSATION_JOB_TITLE_PRIORITY:
        if any(keyword in job_title for keyword in keywords):
            job_priority = score
            break
    return role_priority, job_priority


def order_agent_ids_for_human_turns(openclaw_dir, agent_ids, lead_agent_id="", config=None, metadata=None, agents=None):
    normalized_ids = []
    for item in safe_list(agent_ids):
        agent_id = str(item or "").strip()
        if agent_id and agent_id not in normalized_ids:
            normalized_ids.append(agent_id)
    if len(normalized_ids) < 2:
        return normalized_ids
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    lead_id = str(lead_agent_id or "").strip()

    def sort_key(index_and_agent_id):
        index, current_agent_id = index_and_agent_id
        runtime_profile = agent_runtime_profile_payload(
            openclaw_dir,
            current_agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        role_priority, job_priority = agent_turn_priority_from_profile(runtime_profile)
        return (
            0 if lead_id and current_agent_id == lead_id else 1,
            role_priority,
            job_priority,
            index,
        )

    return [agent_id for _index, agent_id in sorted(enumerate(normalized_ids), key=sort_key)]


def team_response_kind_map(responses):
    response_map = {}
    for item in safe_list(responses):
        if not isinstance(item, dict):
            continue
        agent_id = str(item.get("agentId") or "").strip()
        if not agent_id or agent_id in response_map:
            continue
        response_map[agent_id] = classify_team_collaboration_reply(
            item.get("replyPreview") or item.get("text") or ""
        )
    return response_map


def select_human_turn_targets(
    openclaw_dir,
    agent_ids,
    purpose="broadcast",
    strict_all_replies=False,
    lead_agent_id="",
    responses=None,
    mention_agent_ids=None,
    config=None,
    metadata=None,
    agents=None,
):
    ordered_agent_ids = order_agent_ids_for_human_turns(
        openclaw_dir,
        agent_ids,
        lead_agent_id=lead_agent_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    if not ordered_agent_ids:
        return []
    normalized_purpose = str(purpose or "broadcast").strip().lower()
    response_map = team_response_kind_map(responses)
    mention_ids = [agent_id for agent_id in safe_chat_mentions(mention_agent_ids) if agent_id in ordered_agent_ids]

    if normalized_purpose == "relay":
        prioritized = mention_ids[:]
        prioritized.extend(
            agent_id
            for agent_id in ordered_agent_ids
            if agent_id not in response_map or response_map.get(agent_id) in {"standby", "blocked"}
        )
        unique_targets = []
        for agent_id in prioritized:
            if agent_id and agent_id not in unique_targets:
                unique_targets.append(agent_id)
        return unique_targets[:TEAM_CONVERSATION_RELAY_REPLY_LIMIT]

    if strict_all_replies and normalized_purpose in {"broadcast", "sync"}:
        return ordered_agent_ids

    if normalized_purpose == "sync":
        prioritized = []
        lead_id = str(lead_agent_id or "").strip()
        if lead_id and lead_id in ordered_agent_ids:
            prioritized.append(lead_id)
        prioritized.extend(agent_id for agent_id in mention_ids if agent_id not in prioritized)
        prioritized.extend(
            agent_id
            for agent_id in ordered_agent_ids
            if agent_id not in response_map or response_map.get(agent_id) in {"standby", "blocked"}
        )
        prioritized.extend(agent_id for agent_id in ordered_agent_ids if agent_id not in prioritized)
        unique_targets = []
        for agent_id in prioritized:
            if agent_id and agent_id not in unique_targets:
                unique_targets.append(agent_id)
        return unique_targets[: max(TEAM_CONVERSATION_SYNC_REPLY_LIMIT, len(ordered_agent_ids))]

    if mention_ids:
        return mention_ids

    if response_map and len(ordered_agent_ids) > 2:
        prioritized = []
        lead_id = str(lead_agent_id or "").strip()
        if lead_id and lead_id in ordered_agent_ids:
            prioritized.append(lead_id)
        prioritized.extend(
            agent_id
            for agent_id in ordered_agent_ids
            if response_map.get(agent_id) in {"blocked", "standby"} and agent_id not in prioritized
        )
        prioritized.extend(
            agent_id
            for agent_id in ordered_agent_ids
            if agent_id not in response_map and agent_id not in prioritized
        )
        prioritized.extend(agent_id for agent_id in ordered_agent_ids if agent_id not in prioritized)
        unique_targets = []
        for agent_id in prioritized:
            if agent_id and agent_id not in unique_targets:
                unique_targets.append(agent_id)
        limit = TEAM_CONVERSATION_BROADCAST_REPLY_LIMIT
        if not any(response_map.get(agent_id) in {"blocked", "standby"} for agent_id in ordered_agent_ids):
            limit = min(2, TEAM_CONVERSATION_BROADCAST_REPLY_LIMIT)
        return unique_targets[:limit]

    return ordered_agent_ids[:TEAM_CONVERSATION_BROADCAST_REPLY_LIMIT]


def build_human_turn_anchor_payload(
    openclaw_dir,
    agent_id,
    turn_index=0,
    ordered_agent_ids=None,
    prior_responses=None,
    lead_agent_id="",
    config=None,
    metadata=None,
    agents=None,
):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return {}
    ordered_agent_ids = [
        str(item or "").strip()
        for item in safe_list(ordered_agent_ids)
        if str(item or "").strip()
    ]
    index = max(0, int(turn_index or 0))
    response_entries = coordination_reply_entries(
        prior_responses,
        limit=max(4, len(ordered_agent_ids) + 1),
    )
    candidate_entries = [item for item in response_entries if str(item.get("agentId") or "").strip() != normalized_agent_id]
    anchor = candidate_entries[min(index, len(candidate_entries) - 1)] if candidate_entries else {}
    if not anchor and ordered_agent_ids and index > 0 and index - 1 < len(ordered_agent_ids):
        previous_agent_id = str(ordered_agent_ids[index - 1] or "").strip()
        if previous_agent_id and previous_agent_id != normalized_agent_id:
            runtime_profile = agent_runtime_identity_payload(
                openclaw_dir,
                previous_agent_id,
                config=config,
                metadata=metadata,
                agents=agents,
            )
            anchor = {
                "agentId": previous_agent_id,
                "agentDisplayName": str(runtime_profile.get("displayName") or previous_agent_id).strip() or previous_agent_id,
                "replyPreview": "",
                "messageId": "",
            }
    if not anchor:
        lead_id = str(lead_agent_id or "").strip()
        if lead_id and lead_id != normalized_agent_id:
            runtime_profile = agent_runtime_identity_payload(
                openclaw_dir,
                lead_id,
                config=config,
                metadata=metadata,
                agents=agents,
            )
            anchor = {
                "agentId": lead_id,
                "agentDisplayName": str(runtime_profile.get("displayName") or lead_id).strip() or lead_id,
                "replyPreview": "",
                "messageId": "",
            }
    if not anchor:
        return {}
    return {
        "acknowledgedAgentId": str(anchor.get("agentId") or "").strip(),
        "acknowledgedAgentLabel": str(anchor.get("agentDisplayName") or "").strip(),
        "acknowledgedPreview": str(anchor.get("replyPreview") or "").strip(),
        "acknowledgedMessageId": str(anchor.get("messageId") or "").strip(),
    }


def build_human_turn_profile_payload(
    openclaw_dir,
    agent_id,
    turn_index=0,
    participant_count=0,
    lead_agent_id="",
    prior_responses=None,
    config=None,
    metadata=None,
    agents=None,
):
    normalized_agent_id = str(agent_id or "").strip().lower()
    runtime_profile = agent_runtime_profile_payload(
        openclaw_dir,
        agent_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    role = str(runtime_profile.get("role") or "").strip().lower()
    job_title = str(runtime_profile.get("jobTitle") or runtime_profile.get("roleLabel") or "").strip()
    response_map = team_response_kind_map(prior_responses)
    response_kind = str(response_map.get(str(agent_id or "").strip()) or "").strip()
    index = max(0, int(turn_index or 0))
    total = max(0, int(participant_count or 0))
    lead_id = str(lead_agent_id or "").strip()

    focus = "delivery"
    if normalized_agent_id in {"qa", "vp_compliance", "devops"}:
        focus = "risk"
    elif normalized_agent_id in {"engineering", "data_team"}:
        focus = "delivery"
    elif normalized_agent_id in {"assistant", "coo"}:
        focus = "coordination"
    elif role in {"reviewer", "monitor"} or any(keyword in job_title for keyword in ("测试", "质量", "风控", "合规")):
        focus = "risk"
    elif role in {"router", "operator"} or any(keyword in job_title for keyword in ("协调", "项目", "运营", "市场", "支持", "人力")):
        focus = "coordination"
    elif role in {"planner"} or any(keyword in job_title for keyword in ("产品", "策略", "分析", "情报")):
        focus = "judgment"
    elif role in {"executor"} or any(keyword in job_title for keyword in ("研发", "开发", "平台", "运维", "数据")):
        focus = "delivery"

    pace = "normal"
    if (
        index >= 2
        or (lead_id and str(agent_id or "").strip() != lead_id and index >= 1 and total > 1)
        or (response_kind in {"committed", "standby"} and total > 1)
    ):
        pace = "short"
    if lead_id and str(agent_id or "").strip() == lead_id and index == 0:
        pace = "lead"

    guidance_lines = []
    if pace == "lead":
        guidance_lines.append("你这轮先定一句方向，再点清谁先接哪一段，控制在 2-3 句。")
    elif pace == "short":
        guidance_lines.append("你这轮默认短答，尽量控制在两三句内，只补新增信息。")

    if response_kind == "blocked":
        guidance_lines.append("如果你还卡住，就直接说卡点、影响和需要谁支援。")
    elif response_kind == "standby":
        guidance_lines.append("如果你这轮继续待命，就一句说明什么条件会触发你接手。")

    if focus == "risk":
        guidance_lines.append("你这轮优先补风险、验证结论或放行条件，不要重复方案背景。")
    elif focus == "coordination":
        guidance_lines.append("你这轮优先拉齐分工、依赖和下一步，不必展开太多细节。")
    elif focus == "judgment":
        guidance_lines.append("你这轮优先补判断边界、取舍和下一步决策，不必重复执行细节。")
    else:
        guidance_lines.append("你这轮优先说马上推进什么、依赖谁、预计多久回话。")

    return {
        "turnFocus": focus,
        "turnPace": pace,
        "guidanceLines": clean_unique_strings(guidance_lines),
    }


def should_task_start_with_internal_discussion(team):
    team = team if isinstance(team, dict) else {}
    profile_key = team_collaboration_profile_key(team)
    member_ids = [str(item or "").strip().lower() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()]
    if profile_key in {"core", "fallback"}:
        return True
    if any(agent_id in {"vp_strategy", "coo"} for agent_id in member_ids):
        return True
    values = " ".join(
        [
            str(team.get("id") or ""),
            str(team.get("name") or ""),
            str(team.get("focus") or ""),
            str(team.get("description") or ""),
        ]
    ).lower()
    return any(token in values for token in ("产品", "项目", "strategy", "program", "project", "全员", "company", "协调"))


def pick_internal_discussion_specialist(
    openclaw_dir,
    candidate_agent_ids,
    preferred_agent_ids=None,
    preferred_roles=None,
    preferred_title_keywords=None,
    exclude=None,
    config=None,
    metadata=None,
    agents=None,
):
    ordered_ids = [str(item or "").strip() for item in safe_list(candidate_agent_ids) if str(item or "").strip()]
    excluded_ids = {str(item or "").strip() for item in safe_list(exclude) if str(item or "").strip()}
    preferred_agent_ids = [str(item or "").strip() for item in safe_list(preferred_agent_ids) if str(item or "").strip()]
    preferred_roles = {str(item or "").strip().lower() for item in safe_list(preferred_roles) if str(item or "").strip()}
    preferred_title_keywords = [str(item or "").strip() for item in safe_list(preferred_title_keywords) if str(item or "").strip()]

    for agent_id in preferred_agent_ids:
        if agent_id in ordered_ids and agent_id not in excluded_ids:
            return agent_id

    for agent_id in ordered_ids:
        if agent_id in excluded_ids:
            continue
        runtime_profile = agent_runtime_profile_payload(
            openclaw_dir,
            agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        role = str(runtime_profile.get("role") or "").strip().lower()
        title = str(runtime_profile.get("jobTitle") or runtime_profile.get("roleLabel") or "").strip()
        if preferred_roles and role in preferred_roles:
            return agent_id
        if preferred_title_keywords and any(keyword in title for keyword in preferred_title_keywords):
            return agent_id
    return ""


def task_internal_discussion_plan(
    openclaw_dir,
    team,
    lead_agent_id="",
    participant_agent_ids=None,
    config=None,
    metadata=None,
    agents=None,
):
    normalized_participants = [
        str(item or "").strip()
        for item in safe_list(participant_agent_ids)
        if str(item or "").strip()
    ]
    lead_id = str(lead_agent_id or "").strip()
    if not should_task_start_with_internal_discussion(team):
        return {
            "enabled": False,
            "participantAgentIds": normalized_participants,
            "discussionAgentIds": [],
            "advisorAgentIds": [],
            "executionAgentIds": [agent_id for agent_id in normalized_participants if agent_id and agent_id != lead_id],
            "extraParticipantAgentIds": [],
        }
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    agents = agents if isinstance(agents, list) else load_agents(config)
    available_agent_ids = [
        str((agent or {}).get("id") or "").strip()
        for agent in safe_list(agents)
        if str((agent or {}).get("id") or "").strip()
    ]
    candidate_ids = []
    for agent_id in [*normalized_participants, *available_agent_ids]:
        if agent_id and agent_id not in candidate_ids:
            candidate_ids.append(agent_id)
    ordered_candidates = order_agent_ids_for_human_turns(
        openclaw_dir,
        candidate_ids,
        lead_agent_id=lead_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    strategy_agent_id = pick_internal_discussion_specialist(
        openclaw_dir,
        ordered_candidates,
        preferred_agent_ids=["vp_strategy"],
        preferred_roles=["planner"],
        preferred_title_keywords=["产品", "策略", "项目"],
        exclude=[lead_id],
        config=config,
        metadata=metadata,
        agents=agents,
    )
    operations_agent_id = pick_internal_discussion_specialist(
        openclaw_dir,
        ordered_candidates,
        preferred_agent_ids=["coo"],
        preferred_roles=["operator", "router"],
        preferred_title_keywords=["运营", "协调", "项目"],
        exclude=[lead_id, strategy_agent_id],
        config=config,
        metadata=metadata,
        agents=agents,
    )
    discussion_agent_ids = order_agent_ids_for_human_turns(
        openclaw_dir,
        [lead_id, strategy_agent_id, operations_agent_id],
        lead_agent_id=lead_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    advisor_agent_ids = [agent_id for agent_id in discussion_agent_ids if agent_id and agent_id != lead_id]
    if len(discussion_agent_ids) < 2 or not advisor_agent_ids:
        return {
            "enabled": False,
            "participantAgentIds": normalized_participants,
            "discussionAgentIds": [],
            "advisorAgentIds": [],
            "executionAgentIds": [agent_id for agent_id in normalized_participants if agent_id and agent_id != lead_id],
            "extraParticipantAgentIds": [],
        }
    execution_agent_ids = [
        agent_id
        for agent_id in normalized_participants
        if agent_id and agent_id != lead_id and agent_id not in advisor_agent_ids
    ]
    combined_participants = order_agent_ids_for_human_turns(
        openclaw_dir,
        [*normalized_participants, *discussion_agent_ids],
        lead_agent_id=lead_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    return {
        "enabled": True,
        "participantAgentIds": combined_participants,
        "discussionAgentIds": discussion_agent_ids,
        "advisorAgentIds": advisor_agent_ids,
        "executionAgentIds": execution_agent_ids,
        "extraParticipantAgentIds": [agent_id for agent_id in discussion_agent_ids if agent_id not in normalized_participants],
    }
