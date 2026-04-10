from __future__ import annotations

import re
import sys
from copy import deepcopy
from pathlib import Path

from backend.adapters.storage.dashboard import (
    get_management_run_record as store_get_management_run_record,
    list_chat_threads_page as store_list_chat_threads_page,
    summarize_chat_threads as store_summarize_chat_threads,
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


def _compact_text_preview(value, limit=180):
    text = str(value or "").strip()
    max_length = max(int(limit or 0), 0)
    if len(text) <= max_length:
        return text
    return f"{text[: max(max_length - 1, 0)].rstrip()}…"


def _chat_preview_text_is_transient_system_notice(text):
    normalized = str(text or "").strip()
    if not normalized:
        return False
    return (
        "暂未回包：" in normalized
        or "暂未回应：" in normalized
        or "等待回复超时" in normalized
    )


def preferred_chat_preview_text(latest_body="", messages=None):
    latest_text = str(latest_body or "").strip()
    if latest_text and not _chat_preview_text_is_transient_system_notice(latest_text):
        return latest_text
    for message in reversed(safe_list(messages)):
        if not isinstance(message, dict):
            continue
        body = str(message.get("body") or "").strip()
        if not body:
            continue
        if _chat_preview_text_is_transient_system_notice(body):
            continue
        return body
    return latest_text


def _derive_chat_thread_conversation_binding(thread):
    thread = thread if isinstance(thread, dict) else {}
    thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    linked_team_ids = linked_team_ids_for_chat_thread(thread_meta)
    participant_agent_ids = [
        str(item or "").strip()
        for item in safe_list(thread.get("participantAgentIds"))
        if str(item or "").strip()
    ]
    agent_id = str(
        thread_meta.get("agentId")
        or thread.get("currentTargetAgentId")
        or thread.get("primaryAgentId")
        or (participant_agent_ids[0] if participant_agent_ids else "")
        or ""
    ).strip()
    conversation_key = str(thread_meta.get("conversationKey") or "").strip()
    session_id = str(thread_meta.get("sessionId") or "").strip()
    channel = str(thread.get("channel") or "").strip().lower()
    single_agent_direct_lane = (
        bool(agent_id)
        and len(participant_agent_ids) <= 1
        and not linked_team_ids
        and channel in {"", "internal"}
    )
    if not conversation_key and single_agent_direct_lane:
        conversation_key = f"agent:{agent_id}:dm:primary"
    if not agent_id and conversation_key.startswith("agent:"):
        parts = conversation_key.split(":")
        if len(parts) >= 3:
            agent_id = str(parts[1] or "").strip()
    if not session_id and conversation_key:
        primary_key = f"agent:{agent_id}:dm:primary" if agent_id else ""
        if conversation_key == primary_key:
            session_id = "dm:primary"
        elif agent_id and conversation_key == f"agent:{agent_id}:main":
            session_id = "main"
        elif agent_id and conversation_key.startswith(f"agent:{agent_id}:"):
            session_id = conversation_key[len(f"agent:{agent_id}:"):].strip() or "main"
        else:
            session_id = conversation_key
    talkable = str(thread.get("status") or "").strip().lower() not in {"archived", "closed"}
    return {
        "agentId": agent_id,
        "sessionId": session_id,
        "conversationKey": conversation_key,
        "talkable": talkable,
        "eligible": bool(agent_id and conversation_key),
    }


def _chat_thread_detail_team_member_reference(member, agent_map=None):
    member = member if isinstance(member, dict) else {}
    agent_map = agent_map if isinstance(agent_map, dict) else {}
    agent_id = str(member.get("id") or member.get("agentId") or "").strip()
    agent = agent_map.get(agent_id, {}) if agent_id else {}
    label = agent_identity_display_name(
        member.get("humanName")
        or member.get("name")
        or member.get("label")
        or agent.get("humanName")
        or agent.get("name"),
        member.get("jobTitle")
        or member.get("roleLabel")
        or agent.get("jobTitle")
        or agent.get("roleLabel"),
        fallback=member.get("label") or agent.get("title") or agent_id,
    )
    compact = {
        "id": agent_id,
        "label": str(label or "").strip(),
        "humanName": str(
            member.get("humanName")
            or agent.get("humanName")
            or member.get("name")
            or agent.get("name")
            or ""
        ).strip(),
        "name": str(member.get("name") or agent.get("name") or "").strip(),
        "roleLabel": str(member.get("roleLabel") or agent.get("roleLabel") or "").strip(),
        "jobTitle": str(member.get("jobTitle") or agent.get("jobTitle") or "").strip(),
        "status": str(member.get("status") or agent.get("status") or "").strip(),
        "focus": str(member.get("focus") or agent.get("focus") or "").strip(),
    }
    return {
        key: value
        for key, value in compact.items()
        if value not in ("", None)
    }


def build_chat_thread_detail_team_reference(team, agent_map=None):
    team = team if isinstance(team, dict) else {}
    agent_map = agent_map if isinstance(agent_map, dict) else {}
    member_agent_ids = [
        str(item or "").strip()
        for item in safe_list(team.get("memberAgentIds"))
        if str(item or "").strip()
    ]
    members = []
    seen_member_ids = set()
    raw_members = safe_list(team.get("members"))
    if raw_members:
        for item in raw_members:
            compact_member = _chat_thread_detail_team_member_reference(item, agent_map)
            member_id = str(compact_member.get("id") or "").strip()
            if not member_id or member_id in seen_member_ids:
                continue
            seen_member_ids.add(member_id)
            members.append(compact_member)
    for agent_id in member_agent_ids:
        if agent_id in seen_member_ids:
            continue
        compact_member = _chat_thread_detail_team_member_reference({"id": agent_id}, agent_map)
        if not compact_member.get("id"):
            continue
        seen_member_ids.add(agent_id)
        members.append(compact_member)

    compact = {
        "id": str(team.get("id") or "").strip(),
        "name": str(team.get("name") or "").strip(),
        "leadAgentId": str(team.get("leadAgentId") or "").strip(),
        "leadAgentLabel": str(team.get("leadAgentLabel") or "").strip(),
        "status": str(team.get("status") or "").strip(),
        "memberAgentIds": member_agent_ids,
        "memberCount": int(team.get("memberCount") or len(member_agent_ids) or len(members)),
        "members": members,
    }
    return {
        key: value
        for key, value in compact.items()
        if value not in ("", None)
    }


agent_identity_display_name = _DelegatedSymbol("agent_identity_display_name")
build_team_collaboration_summary = _DelegatedSymbol("build_team_collaboration_summary")
build_team_ownership_payload = _DelegatedSymbol("build_team_ownership_payload")
build_label_maps = _DelegatedSymbol("build_label_maps")
compact_chat_thread_deliverable_reference = _DelegatedSymbol("compact_chat_thread_deliverable_reference")
compact_chat_thread_run_reference = _DelegatedSymbol("compact_chat_thread_run_reference")
compact_chat_thread_summary_payload = _DelegatedSymbol("compact_chat_thread_summary_payload")
compact_chat_thread_task_reference = _DelegatedSymbol("compact_chat_thread_task_reference")
compact_chat_thread_team_reference = _DelegatedSymbol("compact_chat_thread_team_reference")
compact_chat_thread_team_references = _DelegatedSymbol("compact_chat_thread_team_references")
CHAT_THREAD_CONTEXT_COMPRESSION_CHARACTER_THRESHOLD = _DelegatedSymbol("CHAT_THREAD_CONTEXT_COMPRESSION_CHARACTER_THRESHOLD")
CHAT_THREAD_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES = _DelegatedSymbol("CHAT_THREAD_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES")
CHAT_THREAD_CONTEXT_COMPRESSION_MAX_HIGHLIGHTS = _DelegatedSymbol("CHAT_THREAD_CONTEXT_COMPRESSION_MAX_HIGHLIGHTS")
CHAT_THREAD_CONTEXT_COMPRESSION_MAX_NEW_HIGHLIGHTS = _DelegatedSymbol("CHAT_THREAD_CONTEXT_COMPRESSION_MAX_NEW_HIGHLIGHTS")
CHAT_THREAD_CONTEXT_COMPRESSION_MESSAGE_THRESHOLD = _DelegatedSymbol("CHAT_THREAD_CONTEXT_COMPRESSION_MESSAGE_THRESHOLD")
format_age = _DelegatedSymbol("format_age")
get_router_agent_id = _DelegatedSymbol("get_router_agent_id")
hydrate_chat_thread_pack_context = _DelegatedSymbol("hydrate_chat_thread_pack_context")
hydrate_thread_memory_authority = _DelegatedSymbol("hydrate_thread_memory_authority")
linked_team_ids_for_chat_thread = _DelegatedSymbol("linked_team_ids_for_chat_thread")
load_agents = _DelegatedSymbol("load_agents")
load_config = _DelegatedSymbol("load_config")
load_kanban_config = _DelegatedSymbol("load_kanban_config")
load_skills_catalog = _DelegatedSymbol("load_skills_catalog")
normalize_chat_dispatch_mode = _DelegatedSymbol("normalize_chat_dispatch_mode")
now_iso = _DelegatedSymbol("now_iso")
now_utc = _DelegatedSymbol("now_utc")
parse_iso = _DelegatedSymbol("parse_iso")
safe_list = _DelegatedSymbol("safe_list")
store_get_chat_thread = _DelegatedSymbol("store_get_chat_thread")
store_get_task_record = _DelegatedSymbol("store_get_task_record")
store_count_chat_messages_before = _DelegatedSymbol("store_count_chat_messages_before")
store_list_chat_messages = _DelegatedSymbol("store_list_chat_messages")
store_list_chat_messages_before = _DelegatedSymbol("store_list_chat_messages_before")
store_list_agent_teams = _DelegatedSymbol("store_list_agent_teams")
store_list_recent_chat_messages = _DelegatedSymbol("store_list_recent_chat_messages")
store_list_chat_thread_message_summaries = _DelegatedSymbol("store_list_chat_thread_message_summaries")
store_list_chat_threads = _DelegatedSymbol("store_list_chat_threads")
store_list_management_runs = _DelegatedSymbol("store_list_management_runs")
summarize_task_execution_text = _DelegatedSymbol("summarize_task_execution_text")
workflow_pack_map_from_skills_payload = _DelegatedSymbol("workflow_pack_map_from_skills_payload")
DEFAULT_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY = "balanced"
HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES = {"balanced", "aggressive", "full"}
HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_PROFILES = {
    "balanced": {
        "label": "Balanced",
        "summary": "Use the default hosted-provider budget line.",
        "messageThresholdMultiplier": 1.0,
        "characterThresholdMultiplier": 1.0,
        "keepRecentMessagesDelta": 0,
    },
    "aggressive": {
        "label": "Aggressive",
        "summary": "Compress hosted-provider threads earlier and keep a tighter live window.",
        "messageThresholdMultiplier": 0.7,
        "characterThresholdMultiplier": 0.7,
        "keepRecentMessagesDelta": -2,
    },
    "full": {
        "label": "Full",
        "summary": "Preserve a wider live window before compression kicks in.",
        "messageThresholdMultiplier": 1.35,
        "characterThresholdMultiplier": 1.35,
        "keepRecentMessagesDelta": 2,
    },
}


def chat_thread_session_id(thread_id, agent_id):
    normalized_thread = re.sub(r"[^a-z0-9_-]+", "-", str(thread_id or "").strip().lower()).strip("-")
    normalized_agent = re.sub(r"[^a-z0-9_-]+", "-", str(agent_id or "").strip().lower()).strip("-")
    return f"chat-{normalized_thread or 'thread'}-{normalized_agent or 'agent'}"


def chat_thread_rotation_session_id(thread_id, agent_id, rotation_index=0):
    base_session_id = chat_thread_session_id(thread_id, agent_id)
    try:
        normalized_rotation = max(0, int(rotation_index or 0))
    except (TypeError, ValueError):
        normalized_rotation = 0
    if normalized_rotation <= 0:
        return base_session_id
    return f"{base_session_id}-r{normalized_rotation}"


def chat_thread_context_compression_meta(thread):
    thread_meta = thread.get("meta") if isinstance(thread, dict) and isinstance(thread.get("meta"), dict) else {}
    compression_meta = thread_meta.get("contextCompression") if isinstance(thread_meta.get("contextCompression"), dict) else {}
    return deepcopy(compression_meta)


def chat_thread_context_message_label(message):
    message = message if isinstance(message, dict) else {}
    direction = str(message.get("direction") or "").strip().lower()
    sender_kind = str(message.get("senderKind") or "").strip().lower()
    sender_label = str(message.get("senderLabel") or message.get("senderId") or "").strip()
    if direction == "outbound":
        return "用户"
    if sender_kind == "agent":
        return sender_label or "Agent"
    if direction == "agent":
        return sender_label or "Agent"
    if sender_kind == "system" or direction == "system":
        return sender_label or "系统"
    return sender_label or "成员"


def chat_thread_context_message_line(message, limit=140):
    body = summarize_task_execution_text(str((message or {}).get("body") or "").strip(), limit=limit)
    if not body:
        return ""
    return f"{chat_thread_context_message_label(message)}：{body}"


def build_chat_thread_context_highlights(messages, max_items=CHAT_THREAD_CONTEXT_COMPRESSION_MAX_NEW_HIGHLIGHTS):
    relevant_lines = []
    for message in safe_list(messages):
        line = chat_thread_context_message_line(message, limit=140)
        if line:
            relevant_lines.append(line)
    if not relevant_lines:
        return []
    max_items = max(1, int(max_items or 1))
    if len(relevant_lines) <= max_items:
        return relevant_lines
    head_count = min(3, max_items - 1)
    tail_count = max_items - head_count - 1
    hidden_count = len(relevant_lines) - head_count - max(tail_count, 0)
    items = list(relevant_lines[:head_count])
    items.append(f"……中间还有 {hidden_count} 条更早的往返，已压缩保留。")
    if tail_count > 0:
        items.extend(relevant_lines[-tail_count:])
    return items


def render_chat_thread_context_summary(highlights):
    cleaned = [str(item or "").strip() for item in safe_list(highlights) if str(item or "").strip()]
    if not cleaned:
        return ""
    return "\n".join(f"- {item}" for item in cleaned)


def build_chat_thread_recent_exchange_lines(messages, limit=CHAT_THREAD_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES):
    relevant = []
    for message in safe_list(messages):
        line = chat_thread_context_message_line(message, limit=180)
        if line:
            relevant.append(line)
    limit = max(1, int(limit or 1))
    return relevant[-limit:]


def _normalize_hosted_provider_context_budget_policy(value):
    policy = str(value or "").strip().lower()
    if policy not in HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES:
        return DEFAULT_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY
    return policy


def _build_hosted_provider_context_budget_profile(policy, base_budget=None):
    normalized_policy = _normalize_hosted_provider_context_budget_policy(policy)
    profile = deepcopy(HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_PROFILES.get(normalized_policy, HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_PROFILES["balanced"]))
    base_budget = base_budget if isinstance(base_budget, dict) else {}
    message_threshold = max(1, int(base_budget.get("messageThreshold") or 0))
    character_threshold = max(1, int(base_budget.get("characterThreshold") or 0))
    keep_recent_messages = max(1, int(base_budget.get("keepRecentMessages") or 0))
    message_multiplier = float(profile.get("messageThresholdMultiplier") or 1.0)
    character_multiplier = float(profile.get("characterThresholdMultiplier") or 1.0)
    keep_recent_delta = int(profile.get("keepRecentMessagesDelta") or 0)
    effective_budget = {
        "messageThreshold": max(1, int(round(message_threshold * message_multiplier))),
        "characterThreshold": max(1, int(round(character_threshold * character_multiplier))),
        "keepRecentMessages": max(1, keep_recent_messages + keep_recent_delta),
    }
    profile.update(
        {
            "policy": normalized_policy,
            "appliesTo": ["provider_api"],
            "effectiveBudget": effective_budget,
            "messageThreshold": effective_budget["messageThreshold"],
            "characterThreshold": effective_budget["characterThreshold"],
            "keepRecentMessages": effective_budget["keepRecentMessages"],
        }
    )
    return profile


def chat_thread_context_compression_budget(dispatch_agent_ids=None, hosted_provider_context_budget_policy=None):
    dispatch_count = len(
        [
            str(item or "").strip()
            for item in safe_list(dispatch_agent_ids)
            if str(item or "").strip()
        ]
    )
    if dispatch_count >= 3:
        budget = {
            "messageThreshold": 12,
            "characterThreshold": 6000,
            "keepRecentMessages": 5,
        }
    elif dispatch_count == 2:
        budget = {
            "messageThreshold": 16,
            "characterThreshold": 8000,
            "keepRecentMessages": 6,
        }
    else:
        budget = {
            "messageThreshold": 20,
            "characterThreshold": 10000,
            "keepRecentMessages": 7,
        }
    policy = _normalize_hosted_provider_context_budget_policy(hosted_provider_context_budget_policy)
    profile = _build_hosted_provider_context_budget_profile(policy, budget)
    profile["baseBudget"] = deepcopy(budget)
    return profile


def build_chat_thread_compressed_dispatch_message(thread, summary_text, recent_lines, latest_message):
    blocks = []
    thread_title = str((thread or {}).get("title") or (thread or {}).get("id") or "").strip()
    if thread_title:
        blocks.append(f"线程标题\n{thread_title}")
    if summary_text:
        blocks.append("更早上下文摘要\n" + summary_text)
    if recent_lines:
        blocks.append("最近往返\n" + "\n".join(f"- {line}" for line in recent_lines))
    blocks.append(
        "最新待处理消息\n"
        + str(latest_message or "").strip()
        + "\n\n请把上面的压缩摘要当作背景，只围绕最新消息继续推进；如果历史结论和当前请求冲突，请直接指出。默认先自己消化问题并给出建议，不要把原问题原样抛回去。优先输出你的判断和取舍，不要只罗列背景。遇到浏览器、Canvas、沙箱或运行环境问题时，先尝试当前链路可行的 fallback 或替代验证方式，不要把原始错误直接甩给对方。收尾时显式写两行：结论：……  下一步：……，让人知道现在先怎么做。"
    )
    return "\n\n".join(block for block in blocks if str(block or "").strip())


def _chat_thread_context_compression_snapshot(detail):
    detail = detail if isinstance(detail, dict) else {}
    compression = detail.get("contextCompression") if isinstance(detail.get("contextCompression"), dict) else {}
    if compression:
        return deepcopy(compression)
    meta = detail.get("meta") if isinstance(detail.get("meta"), dict) else {}
    compression = meta.get("contextCompression") if isinstance(meta.get("contextCompression"), dict) else {}
    return deepcopy(compression)


def _chat_thread_participant_labels(detail, limit=6):
    labels = []
    for item in safe_list((detail or {}).get("participantAgents"))[: max(1, int(limit or 1))]:
        if not isinstance(item, dict):
            continue
        label = str(item.get("label") or "").strip()
        if label:
            labels.append(label)
    return labels


def _chat_thread_recent_message_snapshots(detail, limit=4):
    snapshots = []
    for message in safe_list((detail or {}).get("messages"))[-max(1, int(limit or 1)) :]:
        if not isinstance(message, dict):
            continue
        body = str(message.get("body") or "").strip()
        if not body and not str(message.get("senderId") or "").strip():
            continue
        snapshots.append(
            {
                "id": str(message.get("id") or "").strip(),
                "direction": str(message.get("direction") or "").strip(),
                "senderId": str(message.get("senderId") or "").strip(),
                "senderLabel": str(message.get("senderLabel") or "").strip(),
                "senderDisplay": str(message.get("senderDisplay") or "").strip(),
                "createdAt": str(message.get("createdAt") or "").strip(),
                "createdAgo": str(message.get("createdAgo") or "").strip(),
                "bodyPreview": summarize_task_execution_text(body, limit=160),
            }
        )
    return snapshots


def build_chat_thread_hot_context(detail):
    detail = detail if isinstance(detail, dict) else {}
    recent_messages = _chat_thread_recent_message_snapshots(detail, limit=4)
    latest_message = recent_messages[-1] if recent_messages else {}
    last_dispatch = _compact_chat_thread_last_dispatch(detail.get("lastDispatch"))
    collaboration = build_team_collaboration_summary(detail.get("lastDispatch") if isinstance(detail.get("lastDispatch"), dict) else {})
    compact = {
        "threadId": str(detail.get("id") or "").strip(),
        "title": str(detail.get("title") or "").strip(),
        "status": str(detail.get("status") or "").strip(),
        "channel": str(detail.get("channel") or "").strip(),
        "mode": str(detail.get("mode") or "").strip(),
        "teamId": str(detail.get("teamId") or "").strip(),
        "dispatchMode": str(detail.get("dispatchMode") or "").strip(),
        "primaryAgentId": str(detail.get("primaryAgentId") or "").strip(),
        "currentTargetAgentId": str(detail.get("currentTargetAgentId") or "").strip(),
        "participantAgentIds": [
            str(item.get("id") or "").strip()
            for item in safe_list(detail.get("participantAgents"))
            if isinstance(item, dict) and str(item.get("id") or "").strip()
        ]
        or [
            str(item or "").strip()
            for item in safe_list(detail.get("participantAgentIds"))
            if str(item or "").strip()
        ],
        "participantAgentLabels": _chat_thread_participant_labels(detail, limit=6),
        "messageCount": int(detail.get("messageCount") or 0),
        "loadedMessageCount": int(detail.get("loadedMessageCount") or len(safe_list(detail.get("messages")))),
        "messages": recent_messages,
        "lastMessageId": str(detail.get("lastMessageId") or latest_message.get("id") or "").strip(),
        "windowBefore": int(detail.get("remainingBeforeCount") or 0),
        "windowAfter": 0,
        "hasOlder": bool(detail.get("hasMoreBefore")),
        "latestMessage": latest_message,
        "recentMessages": recent_messages,
        "liveDispatch": last_dispatch,
        "collaboration": collaboration,
    }
    preserve_empty_fields = {"messages", "recentMessages", "lastMessageId", "windowBefore", "windowAfter", "hasOlder"}
    filtered = {}
    for key, value in compact.items():
        if value is None:
            continue
        if isinstance(value, str) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, list) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, dict) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, bool) and value is False and key not in preserve_empty_fields:
            continue
        filtered[key] = value
    return filtered


def build_chat_thread_warm_summary(detail):
    detail = detail if isinstance(detail, dict) else {}
    team_policy = _compact_chat_thread_team_policy(detail.get("teamPolicy"))
    last_dispatch = _compact_chat_thread_last_dispatch(detail.get("lastDispatch"))
    context_compression = _chat_thread_context_compression_snapshot(detail)
    collaboration = build_team_collaboration_summary(detail.get("lastDispatch") if isinstance(detail.get("lastDispatch"), dict) else {})
    linked_task = detail.get("linkedTask") if isinstance(detail.get("linkedTask"), dict) else {}
    summary_candidates = [
        str(context_compression.get("summary") or "").strip(),
        str(team_policy.get("currentFocus") or "").strip(),
        str(team_policy.get("decisionLog") or "").strip(),
        str(last_dispatch.get("summaryText") or "").strip(),
        str(team_policy.get("workingMemory") or "").strip(),
    ]
    summary_text = next((item for item in summary_candidates if item), "")
    if not summary_text and collaboration:
        summary_text = str(collaboration.get("headline") or "").strip()
    blockers = []
    for item in safe_list(team_policy.get("openLoopItems")):
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "").strip().lower() not in {"blocker", "blocked"}:
            continue
        text = str(item.get("text") or "").strip()
        if text:
            blockers.append(text)
    if not blockers:
        blockers = [str(item or "").strip() for item in safe_list(team_policy.get("openLoops")) if str(item or "").strip()][:4]
    recent_decisions = []
    decision_log = str(team_policy.get("decisionLog") or "").strip()
    if decision_log:
        recent_decisions.append(decision_log)
    recent_artifacts = []
    linked_task_update = str(linked_task.get("currentUpdate") or "").strip()
    if linked_task_update:
        recent_artifacts.append(linked_task_update)
    compact = {
        "threadId": str(detail.get("id") or "").strip(),
        "summary": summary_text,
        "threadSummary": summary_text,
        "workingMemory": str(team_policy.get("workingMemory") or "").strip(),
        "currentFocus": str(team_policy.get("currentFocus") or "").strip(),
        "linkedTaskSummary": {
            "id": str(linked_task.get("id") or "").strip(),
            "title": str(linked_task.get("title") or "").strip(),
            "currentUpdate": linked_task_update,
        },
        "blockers": blockers,
        "recentDecisions": recent_decisions,
        "recentArtifacts": recent_artifacts,
        "teamPolicy": team_policy,
        "liveDispatch": last_dispatch,
        "contextCompression": context_compression,
        "collaboration": collaboration,
    }
    preserve_empty_fields = {
        "threadSummary",
        "workingMemory",
        "currentFocus",
        "linkedTaskSummary",
        "blockers",
        "recentDecisions",
        "recentArtifacts",
    }
    filtered = {}
    for key, value in compact.items():
        if value is None:
            continue
        if isinstance(value, str) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, list) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, dict) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, bool) and value is False and key not in preserve_empty_fields:
            continue
        filtered[key] = value
    return filtered


def build_chat_thread_cold_history(detail):
    detail = detail if isinstance(detail, dict) else {}
    context_compression = _chat_thread_context_compression_snapshot(detail)
    compact = {
        "threadId": str(detail.get("id") or "").strip(),
        "messageCount": int(detail.get("messageCount") or 0),
        "loadedMessageCount": int(detail.get("loadedMessageCount") or len(safe_list(detail.get("messages")))),
        "hasMoreBefore": bool(detail.get("hasMoreBefore")),
        "remainingBeforeCount": int(detail.get("remainingBeforeCount") or 0),
        "cursorBefore": str(detail.get("oldestLoadedMessageId") or "").strip(),
        "cursorAfter": str(detail.get("newestLoadedMessageId") or detail.get("lastMessageId") or "").strip(),
        "searchReady": bool(
            int(detail.get("messageCount") or 0)
            or str(context_compression.get("summary") or "").strip()
            or str(detail.get("lastMessagePreview") or "").strip()
        ),
        "oldestLoadedMessageId": str(detail.get("oldestLoadedMessageId") or "").strip(),
        "newestLoadedMessageId": str(detail.get("newestLoadedMessageId") or "").strip(),
        "lastMessageId": str(detail.get("lastMessageId") or "").strip(),
        "lastMessageAt": str(detail.get("lastMessageAt") or "").strip(),
        "lastMessagePreview": str(detail.get("lastMessagePreview") or "").strip(),
        "contextCompression": context_compression,
        "compressedMessageCount": int(context_compression.get("compressedMessageCount") or 0),
        "rotationCount": int(context_compression.get("rotationCount") or 0),
        "lastCompressedAt": str(context_compression.get("lastCompressedAt") or "").strip(),
        "reason": str(context_compression.get("reason") or "").strip(),
    }
    preserve_empty_fields = {"cursorBefore", "cursorAfter", "searchReady"}
    filtered = {}
    for key, value in compact.items():
        if value is None:
            continue
        if isinstance(value, str) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, list) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, dict) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, bool) and value is False and key not in {"hasMoreBefore", *preserve_empty_fields}:
            continue
        filtered[key] = value
    return filtered


def build_chat_thread_layered_context(detail):
    detail = detail if isinstance(detail, dict) else {}
    hot_context = build_chat_thread_hot_context(detail)
    warm_summary = build_chat_thread_warm_summary(detail)
    cold_history = build_chat_thread_cold_history(detail)
    layered = {}
    if hot_context:
        layered["hotContext"] = hot_context
    if warm_summary:
        layered["warmSummary"] = warm_summary
    if cold_history:
        layered["coldHistory"] = cold_history
    return layered


def maybe_prepare_chat_thread_context_compression(thread, messages, dispatch_agent_ids, merged_message, hosted_provider_context_budget_policy=None, execution_path=""):
    thread = thread if isinstance(thread, dict) else {}
    relevant_messages = [
        deepcopy(message)
        for message in safe_list(messages)
        if isinstance(message, dict) and str(message.get("body") or "").strip()
    ]
    dispatch_agent_ids = [str(item or "").strip() for item in safe_list(dispatch_agent_ids) if str(item or "").strip()]
    if not relevant_messages or not dispatch_agent_ids:
        return {"applied": False}
    compression_budget = chat_thread_context_compression_budget(
        dispatch_agent_ids,
        hosted_provider_context_budget_policy=hosted_provider_context_budget_policy if str(execution_path or "").strip() == "provider_api" else None,
    )
    budget_profile = deepcopy(compression_budget)
    message_threshold = max(1, int(compression_budget.get("messageThreshold") or CHAT_THREAD_CONTEXT_COMPRESSION_MESSAGE_THRESHOLD))
    character_threshold = max(1, int(compression_budget.get("characterThreshold") or CHAT_THREAD_CONTEXT_COMPRESSION_CHARACTER_THRESHOLD))
    keep_recent_messages = max(1, int(compression_budget.get("keepRecentMessages") or CHAT_THREAD_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES))
    total_characters = sum(len(str(message.get("body") or "").strip()) for message in relevant_messages)
    if len(relevant_messages) < message_threshold and total_characters < character_threshold:
        return {"applied": False}
    if len(relevant_messages) <= keep_recent_messages:
        return {"applied": False}

    compression_meta = chat_thread_context_compression_meta(thread)
    try:
        compressed_message_count = max(0, int(compression_meta.get("compressedMessageCount") or 0))
    except (TypeError, ValueError):
        compressed_message_count = 0
    pending_cutoff = len(relevant_messages) - keep_recent_messages
    if pending_cutoff <= compressed_message_count:
        return {"applied": False}

    older_messages = relevant_messages[compressed_message_count:pending_cutoff]
    if not older_messages:
        return {"applied": False}

    previous_highlights = [
        str(item or "").strip()
        for item in safe_list(compression_meta.get("highlights"))
        if str(item or "").strip()
    ]
    previous_summary = str(compression_meta.get("summary") or "").strip()
    if previous_summary and not previous_highlights:
        previous_highlights = [line.strip("- ").strip() for line in previous_summary.splitlines() if line.strip()]
    next_highlights = previous_highlights + build_chat_thread_context_highlights(older_messages)
    max_highlights = max(1, int(CHAT_THREAD_CONTEXT_COMPRESSION_MAX_HIGHLIGHTS or 1))
    if len(next_highlights) > max_highlights:
        next_highlights = next_highlights[-max_highlights:]
    summary_text = render_chat_thread_context_summary(next_highlights)
    recent_lines = build_chat_thread_recent_exchange_lines(relevant_messages[pending_cutoff:], limit=keep_recent_messages)
    try:
        previous_rotation_count = max(0, int(compression_meta.get("rotationCount") or 0))
    except (TypeError, ValueError):
        previous_rotation_count = 0
    next_rotation_count = previous_rotation_count + 1
    next_sessions = {
        agent_id: chat_thread_rotation_session_id(thread.get("id", ""), agent_id, next_rotation_count)
        for agent_id in dispatch_agent_ids
    }
    return {
        "applied": True,
        "reason": "large_thread_context",
        "message": build_chat_thread_compressed_dispatch_message(thread, summary_text, recent_lines, merged_message),
        "sessionsByAgent": next_sessions,
        "meta": {
            "version": 1,
            "policy": str(budget_profile.get("policy") or DEFAULT_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY).strip(),
            "policyLabel": str(budget_profile.get("label") or "").strip(),
            "policySummary": str(budget_profile.get("summary") or "").strip(),
            "baseBudget": deepcopy(budget_profile.get("baseBudget") or {}),
            "effectiveBudget": deepcopy(budget_profile.get("effectiveBudget") or {}),
            "summary": summary_text,
            "highlights": next_highlights,
            "compressedMessageCount": pending_cutoff,
            "rotationCount": next_rotation_count,
            "lastCompressedAt": now_iso(),
            "triggerMessageCount": len(relevant_messages),
            "triggerCharacterCount": total_characters,
            "recentMessageCount": len(recent_lines),
            "messageThreshold": message_threshold,
            "characterThreshold": character_threshold,
            "keepRecentMessages": keep_recent_messages,
            "reason": "large_thread_context",
        },
    }

def build_chat_data(openclaw_dir, agents, task_index, deliverables, management_data, now, agent_teams_data=None, skills_data=None):
    agent_map = {item.get("id"): item for item in (agents or []) if item.get("id")}
    task_map = {item.get("id"): item for item in (task_index or []) if item.get("id")}
    deliverable_map = {item.get("id"): item for item in (deliverables or []) if item.get("id")}
    team_map = {item.get("id"): item for item in safe_list((agent_teams_data or {}).get("items")) if item.get("id")}
    pack_map = workflow_pack_map_from_skills_payload(skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir))
    run_map = {
        item.get("id"): item
        for item in safe_list((management_data or {}).get("runs"))
        if item.get("id")
    }
    threads = store_list_chat_threads(openclaw_dir, limit=120)
    thread_ids = [t["id"] for t in threads if t.get("id")]
    thread_message_summaries = {
        item.get("threadId"): item
        for item in store_list_chat_thread_message_summaries(openclaw_dir, thread_ids)
        if item.get("threadId")
    }

    items = []
    for thread in threads:
        thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
        team_id = str(thread_meta.get("teamId") or "").strip()
        linked_team_ids = linked_team_ids_for_chat_thread(thread_meta)
        dispatch_mode = normalize_chat_dispatch_mode(thread_meta.get("dispatchMode"), has_team=bool(linked_team_ids))
        last_dispatch = thread_meta.get("lastDispatch") if isinstance(thread_meta.get("lastDispatch"), dict) else {}
        last_message = thread_message_summaries.get(thread.get("id"), {})
        message_count = int(last_message.get("messageCount") or 0)
        last_dt = parse_iso(last_message.get("lastMessageAt") or thread.get("updatedAt"))
        participant_agents = []
        for agent_id in safe_list(thread.get("participantAgentIds")):
            agent = agent_map.get(agent_id, {})
            participant_agents.append(
                {
                    "id": agent_id,
                    "label": agent_identity_display_name(
                        agent.get("humanName") or agent.get("name"),
                        agent.get("jobTitle") or agent.get("roleLabel"),
                        fallback=agent.get("title") or agent_id,
                    ),
                    "status": agent.get("status", ""),
                    "focus": agent.get("focus", ""),
                }
            )
        primary_agent = next(
            (item for item in participant_agents if item.get("id") == thread.get("primaryAgentId")),
            {},
        )
        current_target = next(
            (item for item in participant_agents if item.get("id") == thread.get("currentTargetAgentId")),
            primary_agent,
        )
        items.append(
            compact_chat_thread_summary_payload(
                hydrate_chat_thread_pack_context(
                    {
                        **thread,
                        "updatedAgo": format_age(parse_iso(thread.get("updatedAt")), now),
                        "lastMessageId": last_message.get("lastMessageId", ""),
                        "lastMessageAt": last_message.get("lastMessageAt", ""),
                        "lastMessageAgo": format_age(last_dt, now) if last_dt else "未知时间",
                        "lastMessagePreview": summarize_task_execution_text(
                            preferred_chat_preview_text(last_message.get("lastMessageBody") or ""),
                            limit=220,
                        ),
                        "messageCount": message_count,
                        "participantAgents": participant_agents,
                        "primaryAgentLabel": primary_agent.get("label") or thread.get("primaryAgentId", ""),
                        "currentTargetAgentLabel": current_target.get("label") or thread.get("currentTargetAgentId", ""),
                        "teamId": team_id,
                        "teamLabel": (team_map.get(team_id) or {}).get("name", ""),
                        "dispatchMode": dispatch_mode,
                        "lastDispatch": last_dispatch,
                        "linkedTeam": compact_chat_thread_team_reference(team_map.get(team_id)),
                        "linkedTeamIds": linked_team_ids,
                        "linkedTeams": compact_chat_thread_team_references(team_map, linked_team_ids),
                        "linkedTask": compact_chat_thread_task_reference(task_map.get(thread.get("linkedTaskId"))),
                        "linkedDeliverable": compact_chat_thread_deliverable_reference(deliverable_map.get(thread.get("linkedDeliverableId"))),
                        "linkedRun": compact_chat_thread_run_reference(run_map.get(thread.get("linkedRunId"))),
                    },
                    pack_map,
                )
            )
        )
        items[-1]["collaboration"] = build_team_collaboration_summary(last_dispatch)
        items[-1]["teamOwnership"] = build_team_ownership_payload(
            team_map,
            execution_team_id=str(items[-1].get("teamId") or "").strip() or str(((items[-1].get("linkedRun") or {}).get("linkedTeam") or {}).get("id") or "").strip(),
            recommended_team_id=str((items[-1].get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
            mode=str(items[-1].get("mode") or (items[-1].get("linkedPack") or {}).get("mode") or "").strip(),
            source="chat",
        )

    waiting_internal = sum(1 for item in items if item.get("status") == "waiting_internal")
    waiting_external = sum(1 for item in items if item.get("status") == "waiting_external")
    return {
        "summary": {
            "threadCount": len(items),
            "openCount": sum(1 for item in items if item.get("status") == "open"),
            "waitingInternalCount": waiting_internal,
            "waitingExternalCount": waiting_external,
            "resolvedCount": sum(1 for item in items if item.get("status") == "resolved"),
        },
        "threads": items,
        "channels": sorted({item.get("channel", "internal") for item in items if item.get("channel")}),
    }


def build_chat_catalog_page_snapshot(
    openclaw_dir,
    page=1,
    page_size=24,
    status="",
    query_text="",
    config=None,
    now=None,
):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    page = max(int(page or 1), 1)
    page_size = max(min(int(page_size or 24), 60), 1)
    summary = store_summarize_chat_threads(openclaw_dir, status=status, query_text=query_text)
    threads = safe_list(
        store_list_chat_threads_page(
            openclaw_dir,
            page=page,
            page_size=page_size,
            status=status,
            query_text=query_text,
        )
    )
    thread_ids = [str(item.get("id") or "").strip() for item in threads if str(item.get("id") or "").strip()]
    thread_message_summaries = {
        item.get("threadId"): item
        for item in safe_list(store_list_chat_thread_message_summaries(openclaw_dir, thread_ids))
        if item.get("threadId")
    }
    agent_map = {
        str(agent.get("id") or "").strip(): agent
        for agent in safe_list(load_agents(config))
        if isinstance(agent, dict) and str(agent.get("id") or "").strip()
    }
    team_map = {
        str(team.get("id") or "").strip(): team
        for team in safe_list(store_list_agent_teams(openclaw_dir))
        if isinstance(team, dict) and str(team.get("id") or "").strip()
    }
    pack_map = workflow_pack_map_from_skills_payload(load_skills_catalog(openclaw_dir, config=config) or {})
    items = []
    for thread in threads:
        if not isinstance(thread, dict):
            continue
        thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
        team_id = str(thread_meta.get("teamId") or "").strip()
        linked_team_ids = linked_team_ids_for_chat_thread(thread_meta)
        dispatch_mode = normalize_chat_dispatch_mode(thread_meta.get("dispatchMode"), has_team=bool(linked_team_ids))
        last_dispatch = thread_meta.get("lastDispatch") if isinstance(thread_meta.get("lastDispatch"), dict) else {}
        last_message = thread_message_summaries.get(thread.get("id"), {})
        last_dt = parse_iso(last_message.get("lastMessageAt") or thread.get("updatedAt"))
        participant_agents = []
        for agent_id in safe_list(thread.get("participantAgentIds")):
            agent = agent_map.get(str(agent_id or "").strip(), {})
            participant_agents.append(
                {
                    "id": str(agent_id or "").strip(),
                    "label": agent_identity_display_name(
                        agent.get("humanName") or agent.get("name"),
                        agent.get("jobTitle") or agent.get("roleLabel"),
                        fallback=agent.get("title") or agent_id,
                    ),
                    "status": agent.get("status", ""),
                    "focus": agent.get("focus", ""),
                }
            )
        primary_agent = next(
            (item for item in participant_agents if item.get("id") == thread.get("primaryAgentId")),
            {},
        )
        current_target = next(
            (item for item in participant_agents if item.get("id") == thread.get("currentTargetAgentId")),
            primary_agent,
        )
        linked_task = compact_chat_thread_task_reference(
            _svc().store_get_task_record(openclaw_dir, thread.get("linkedTaskId"))
        )
        linked_run = compact_chat_thread_run_reference(
            store_get_management_run_record(openclaw_dir, thread.get("linkedRunId"))
        )
        linked_deliverable = compact_chat_thread_deliverable_reference(
            {
                "id": str(thread.get("linkedDeliverableId") or "").strip(),
                "title": str(thread.get("title") or thread.get("linkedDeliverableId") or "").strip(),
                "summary": _compact_text_preview(last_message.get("lastMessageBody") or thread.get("title") or "", 180),
            }
        )
        payload = hydrate_chat_thread_pack_context(
            {
                **thread,
                "updatedAgo": format_age(parse_iso(thread.get("updatedAt")), now),
                "lastMessageId": last_message.get("lastMessageId", ""),
                "lastMessageAt": last_message.get("lastMessageAt", ""),
                "lastMessageAgo": format_age(last_dt, now) if last_dt else "未知时间",
                "lastMessagePreview": summarize_task_execution_text(
                    preferred_chat_preview_text(last_message.get("lastMessageBody") or ""),
                    limit=220,
                ),
                "messageCount": int(last_message.get("messageCount") or 0),
                "participantAgents": participant_agents,
                "primaryAgentLabel": primary_agent.get("label") or thread.get("primaryAgentId", ""),
                "currentTargetAgentLabel": current_target.get("label") or thread.get("currentTargetAgentId", ""),
                "teamId": team_id,
                "teamLabel": (team_map.get(team_id) or {}).get("name", ""),
                "dispatchMode": dispatch_mode,
                "lastDispatch": last_dispatch,
                "linkedTeam": compact_chat_thread_team_reference(team_map.get(team_id)),
                "linkedTeamIds": linked_team_ids,
                "linkedTeams": compact_chat_thread_team_references(team_map, linked_team_ids),
                "linkedTask": linked_task,
                "linkedDeliverable": linked_deliverable,
                "linkedRun": linked_run,
                **_derive_chat_thread_conversation_binding(thread),
            },
            pack_map,
        )
        compacted = compact_chat_thread_summary_payload(payload)
        compacted["collaboration"] = build_team_collaboration_summary(last_dispatch)
        compacted["teamOwnership"] = build_team_ownership_payload(
            team_map,
            execution_team_id=str(compacted.get("teamId") or "").strip() or str(((compacted.get("linkedRun") or {}).get("linkedTeam") or {}).get("id") or "").strip(),
            recommended_team_id=str((compacted.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
            mode=str(compacted.get("mode") or (compacted.get("linkedPack") or {}).get("mode") or "").strip(),
            source="chat",
        )
        items.append(compacted)

    waiting_internal = sum(1 for item in items if item.get("status") == "waiting_internal")
    waiting_external = sum(1 for item in items if item.get("status") == "waiting_external")
    total_count = int(summary.get("threadCount") or 0)
    total_pages = max(1, (total_count + page_size - 1) // page_size) if total_count else 1
    return {
        "summary": {
            "threadCount": total_count,
            "pageThreadCount": len(items),
            "openCount": int(summary.get("openCount") or 0),
            "waitingInternalCount": int(summary.get("waitingInternalCount") or waiting_internal),
            "waitingExternalCount": int(summary.get("waitingExternalCount") or waiting_external),
            "resolvedCount": int(summary.get("resolvedCount") or 0),
        },
        "threads": items,
        "channels": sorted({str(item.get("channel") or "internal").strip() for item in items if str(item.get("channel") or "").strip()}),
        "page": page,
        "pageSize": page_size,
        "totalCount": total_count,
        "totalPages": total_pages,
        "hasMore": page < total_pages,
        "status": str(status or "").strip(),
        "query": str(query_text or "").strip(),
    }


def load_chat_thread_detail(
    openclaw_dir,
    thread_id,
    agents=None,
    tasks=None,
    deliverables=None,
    management_runs=None,
    agent_teams=None,
    skills_data=None,
    message_limit=120,
    before_message_id="",
):
    thread_id = str(thread_id or "").strip()
    if not thread_id:
        raise RuntimeError("需要 threadId。")
    thread = store_get_chat_thread(openclaw_dir, thread_id)
    if not thread:
        raise RuntimeError("聊天线程不存在。")
    if agents is None:
        config = load_config(openclaw_dir)
        router_agent_id = get_router_agent_id(config)
        kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
        agent_labels, _label_to_agent_ids = build_label_maps(load_agents(config), kanban_cfg, router_agent_id)
        agents = [
            {
                **agent,
                "title": agent_labels.get(str(agent.get("id") or "").strip(), str(agent.get("id") or "").strip()),
                "name": ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", ""),
                "humanName": ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", ""),
                "jobTitle": str(agent.get("jobTitle") or agent.get("roleLabel") or "").strip(),
                "status": str(agent.get("status") or "").strip(),
                "focus": str(agent.get("focus") or "").strip(),
            }
            for agent in safe_list(load_agents(config))
            if str(agent.get("id") or "").strip()
        ]
    if tasks is None:
        linked_task = store_get_task_record(openclaw_dir, thread.get("linkedTaskId"))
        tasks = [linked_task] if isinstance(linked_task, dict) and linked_task else []
    if management_runs is None:
        linked_run_id = str(thread.get("linkedRunId") or "").strip()
        management_runs = []
        if linked_run_id:
            for item in safe_list(store_list_management_runs(openclaw_dir, limit=128)):
                if str(item.get("id") or "").strip() == linked_run_id:
                    management_runs.append(item)
                    break
    if agent_teams is None:
        agent_teams = store_list_agent_teams(openclaw_dir)
    if skills_data is None:
        skills_data = load_skills_catalog(openclaw_dir)
    normalized_message_limit = max(1, int(message_limit or 120))
    normalized_before_message_id = str(before_message_id or "").strip()
    agent_map = {item.get("id"): item for item in (agents or []) if item.get("id")}
    task_map = {item.get("id"): item for item in (tasks or []) if item.get("id")}
    deliverable_map = {item.get("id"): item for item in (deliverables or []) if item.get("id")}
    run_map = {item.get("id"): item for item in (management_runs or []) if item.get("id")}
    team_map = {item.get("id"): item for item in safe_list(agent_teams) if item.get("id")}
    pack_map = workflow_pack_map_from_skills_payload(skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir))
    message_summary = next(
        (
            item
            for item in store_list_chat_thread_message_summaries(openclaw_dir, [thread_id])
            if str(item.get("threadId") or "").strip() == thread_id
        ),
        {},
    )
    total_message_count = int(message_summary.get("messageCount") or 0)
    available_before_count = 0
    if normalized_before_message_id:
        raw_messages = store_list_chat_messages_before(
            openclaw_dir,
            thread_id=thread_id,
            before_message_id=normalized_before_message_id,
            limit=normalized_message_limit,
        )
        available_before_count = store_count_chat_messages_before(
            openclaw_dir,
            thread_id=thread_id,
            before_message_id=normalized_before_message_id,
        )
        remaining_before_count = max(available_before_count - len(raw_messages), 0)
    else:
        raw_messages = store_list_recent_chat_messages(
            openclaw_dir,
            thread_id=thread_id,
            limit=normalized_message_limit,
        )
        remaining_before_count = max(total_message_count - len(raw_messages), 0)
    messages = []
    for message in raw_messages:
        at = parse_iso(message.get("createdAt"))
        sender_id = message.get("senderId", "")
        agent = agent_map.get(sender_id, {})
        message_meta = message.get("meta") if isinstance(message.get("meta"), dict) else {}
        sender_display = agent_identity_display_name(
            agent.get("humanName") or message_meta.get("agentHumanName"),
            agent.get("jobTitle") or message_meta.get("agentJobTitle"),
            fallback=message.get("senderLabel") or agent.get("title") or sender_id or "unknown",
        )
        messages.append(
            {
                **message,
                "attachments": safe_list(message_meta.get("attachments")),
                "createdAgo": format_age(at, now_utc()) if at else "未知时间",
                "senderDisplay": sender_display,
            }
        )
    participant_agents = []
    for agent_id in safe_list(thread.get("participantAgentIds")):
        agent = agent_map.get(agent_id, {})
        participant_agents.append(
            {
                "id": agent_id,
                "label": agent_identity_display_name(
                    agent.get("humanName") or agent.get("name"),
                    agent.get("jobTitle") or agent.get("roleLabel"),
                    fallback=agent.get("title") or agent_id,
                ),
                "status": agent.get("status", ""),
                "focus": agent.get("focus", ""),
            }
        )
    team_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    workspace_path = str(thread.get("workspacePath") or team_meta.get("workspacePath") or "").strip()
    workspace_authorized = bool(thread.get("workspaceAuthorized") or team_meta.get("workspaceAuthorized")) and bool(workspace_path)
    team_id = str(team_meta.get("teamId") or "").strip()
    linked_team_ids = linked_team_ids_for_chat_thread(team_meta)
    dispatch_mode = normalize_chat_dispatch_mode(team_meta.get("dispatchMode"), has_team=bool(linked_team_ids))
    team_policy = hydrate_thread_memory_authority(
        openclaw_dir,
        thread,
        team_meta.get("teamPolicy") if isinstance(team_meta.get("teamPolicy"), dict) else {},
    )
    last_dispatch = team_meta.get("lastDispatch") if isinstance(team_meta.get("lastDispatch"), dict) else {}
    linked_team = build_chat_thread_detail_team_reference(team_map.get(team_id), agent_map)
    linked_teams = [
        build_chat_thread_detail_team_reference(team_map.get(linked_team_id), agent_map)
        for linked_team_id in linked_team_ids
        if isinstance(team_map.get(linked_team_id), dict)
    ]
    detail = hydrate_chat_thread_pack_context(
        {
            **thread,
            "workspacePath": workspace_path,
            "workspaceAuthorized": workspace_authorized,
            "participantAgents": participant_agents,
            "messages": messages,
            "messageCount": total_message_count,
            "loadedMessageCount": len(messages),
            "oldestLoadedMessageId": messages[0]["id"] if messages else "",
            "newestLoadedMessageId": messages[-1]["id"] if messages else "",
            "hasMoreBefore": remaining_before_count > 0,
            "remainingBeforeCount": remaining_before_count,
            "lastMessageId": str(message_summary.get("lastMessageId") or (messages[-1]["id"] if messages else "")).strip(),
            "lastMessageAt": str(message_summary.get("lastMessageAt") or (messages[-1].get("createdAt", "") if messages else "")).strip(),
            "lastMessagePreview": summarize_task_execution_text(
                preferred_chat_preview_text(
                    message_summary.get("lastMessageBody") or (messages[-1].get("body") or "" if messages else ""),
                    messages=messages,
                ),
                limit=220,
            ) if (message_summary or messages) else "",
            "updatedAgo": format_age(parse_iso(thread.get("updatedAt")), now_utc()),
            "teamId": team_id,
            "dispatchMode": dispatch_mode,
            "teamPolicy": team_policy,
            "lastDispatch": last_dispatch,
            "linkedTeam": linked_team,
            "linkedTeamIds": linked_team_ids,
            "linkedTeams": linked_teams,
            "linkedTask": task_map.get(thread.get("linkedTaskId")),
            "linkedDeliverable": deliverable_map.get(thread.get("linkedDeliverableId")),
            "linkedRun": run_map.get(thread.get("linkedRunId")),
        },
        pack_map,
    )
    if workspace_path:
        detail["workspacePath"] = workspace_path
        detail["workspaceAuthorized"] = workspace_authorized
        detail["meta"] = {
            **(detail.get("meta") if isinstance(detail.get("meta"), dict) else {}),
            "workspacePath": workspace_path,
            "workspaceAuthorized": workspace_authorized,
        }
    detail["collaboration"] = build_team_collaboration_summary(last_dispatch or (team_meta.get("lastSync") if isinstance(team_meta.get("lastSync"), dict) else {}))
    detail["teamOwnership"] = build_team_ownership_payload(
        team_map,
        execution_team_id=str(detail.get("teamId") or "").strip() or str((((detail.get("linkedRun") or {}).get("linkedTeam")) or {}).get("id") or "").strip(),
        recommended_team_id=str((detail.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
        mode=str(detail.get("mode") or (detail.get("linkedPack") or {}).get("mode") or "").strip(),
        source="chat",
    )
    detail.update(build_chat_thread_layered_context(detail))
    return detail


def _compact_chat_thread_message_meta(meta):
    meta = meta if isinstance(meta, dict) else {}
    task_execution = meta.get("taskExecution") if isinstance(meta.get("taskExecution"), dict) else {}
    team_context = meta.get("teamContext") if isinstance(meta.get("teamContext"), dict) else {}
    attachments = []
    for item in safe_list(meta.get("attachments"))[:8]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        attachments.append(
            {
                "name": name[:240],
                "size": max(0, int(item.get("size") or 0)),
                "type": str(item.get("type") or "").strip()[:120],
                "preview": summarize_task_execution_text(item.get("preview") or "", limit=240),
            }
        )
    compact_task_execution = {
        "managedBy": str(task_execution.get("managedBy") or "").strip(),
        "source": str(task_execution.get("source") or "").strip(),
    }
    compact_team_context = {
        "hasOperatingBrief": bool(team_context.get("hasOperatingBrief")),
        "hasTeamMemory": bool(team_context.get("hasTeamMemory")),
        "hasDecisionLog": bool(team_context.get("hasDecisionLog")),
        "hasCurrentFocus": bool(team_context.get("hasCurrentFocus")),
        "hasOpenLoops": bool(team_context.get("hasOpenLoops")),
    }
    compact_team_context = {
        key: value for key, value in compact_team_context.items() if value is True
    }
    compact = {
        "coordinationRelay": bool(meta.get("coordinationRelay")),
        "manualTeamSync": bool(meta.get("manualTeamSync")),
        "voiceReplyPending": bool(meta.get("voiceReplyPending")),
        "voiceReplyFailed": bool(meta.get("voiceReplyFailed")),
        "replyToMessageId": str(meta.get("replyToMessageId") or "").strip(),
        "acknowledgedAgentId": str(meta.get("acknowledgedAgentId") or "").strip(),
        "acknowledgedAgentLabel": str(meta.get("acknowledgedAgentLabel") or "").strip(),
        "acknowledgedPreview": summarize_task_execution_text(meta.get("acknowledgedPreview") or "", limit=120),
        "acknowledgedMessageId": str(meta.get("acknowledgedMessageId") or "").strip(),
        "dispatchMode": str(meta.get("dispatchMode") or "").strip(),
        "dispatchAgentIds": [
            str(item or "").strip()
            for item in safe_list(meta.get("dispatchAgentIds"))
            if str(item or "").strip()
        ],
        "currentFocusApplied": bool(meta.get("currentFocusApplied")),
        "currentFocusPreview": summarize_task_execution_text(meta.get("currentFocusPreview") or "", limit=120),
        "currentFocusOwnerLabel": str(meta.get("currentFocusOwnerLabel") or "").strip(),
        "openLoopApplied": bool(meta.get("openLoopApplied")),
        "openLoopPreview": summarize_task_execution_text(meta.get("openLoopPreview") or "", limit=120),
        "openLoopOwnerLabel": str(meta.get("openLoopOwnerLabel") or "").strip(),
        "openLoopKind": str(meta.get("openLoopKind") or "").strip(),
        "workingMemoryApplied": bool(meta.get("workingMemoryApplied")),
        "workingMemoryPreview": summarize_task_execution_text(meta.get("workingMemoryPreview") or "", limit=120),
        "teamGuidanceApplied": bool(meta.get("teamGuidanceApplied")),
        "teamGuidancePreview": summarize_task_execution_text(meta.get("teamGuidancePreview") or "", limit=120),
        "coordinationState": meta.get("coordinationState") if isinstance(meta.get("coordinationState"), dict) else str(meta.get("coordinationState") or "").strip(),
        "turnPace": str(meta.get("turnPace") or "").strip(),
        "turnFocus": str(meta.get("turnFocus") or "").strip(),
    }
    compact = {key: value for key, value in compact.items() if value not in ("", None, False, [])}
    if attachments:
        compact["attachments"] = attachments
    compact_task_execution = {
        key: value for key, value in compact_task_execution.items() if value not in ("", None)
    }
    if compact_task_execution:
        compact["taskExecution"] = compact_task_execution
    if compact_team_context:
        compact["teamContext"] = compact_team_context
    return compact


def _compact_chat_thread_attachment_payload(item, thread_id="", message_id=""):
    item = item if isinstance(item, dict) else {}
    name = str(item.get("name") or "").strip()
    if not name:
        return {}
    attachment_id = str(item.get("id") or "").strip()
    content_path = str(item.get("contentPath") or "").strip()
    url = str(item.get("url") or item.get("downloadUrl") or "").strip()
    if not url and attachment_id and content_path and Path(content_path).exists():
        query = (
            f"threadId={str(thread_id or '').strip()}"
            f"&messageId={str(message_id or '').strip()}"
            f"&attachmentId={attachment_id}"
        )
        url = f"/api/chat/attachment/content?{query}"
    payload = {
        "id": attachment_id,
        "threadId": str(thread_id or "").strip(),
        "messageId": str(message_id or "").strip(),
        "kind": str(item.get("kind") or "file").strip() or "file",
        "name": name[:240],
        "size": max(0, int(item.get("size") or 0)),
        "type": str(item.get("mimeType") or item.get("type") or "").strip()[:120],
        "preview": summarize_task_execution_text(item.get("preview") or "", limit=240),
        "createdAt": str(item.get("createdAt") or "").strip(),
        "source": str(item.get("source") or "").strip(),
        "sourceLabel": str(item.get("sourceLabel") or "").strip(),
        "url": url,
        "downloadUrl": url,
    }
    return {key: value for key, value in payload.items() if value not in ("", None)}


def _compact_chat_thread_message(message):
    message = message if isinstance(message, dict) else {}
    thread_id = str(message.get("threadId") or "").strip()
    message_id = str(message.get("id") or "").strip()
    compact = {
        "id": message_id,
        "direction": str(message.get("direction") or "").strip(),
        "senderId": str(message.get("senderId") or "").strip(),
        "senderLabel": str(message.get("senderLabel") or "").strip(),
        "senderDisplay": str(message.get("senderDisplay") or "").strip(),
        "createdAt": str(message.get("createdAt") or "").strip(),
        "createdAgo": str(message.get("createdAgo") or "").strip(),
        "body": str(message.get("body") or ""),
    }
    compact_meta = _compact_chat_thread_message_meta(message.get("meta"))
    if compact_meta:
        compact["meta"] = compact_meta
    compact_attachments = [
        _compact_chat_thread_attachment_payload(item, thread_id=thread_id, message_id=message_id)
        for item in safe_list((message.get("meta") if isinstance(message.get("meta"), dict) else {}).get("attachments"))
    ]
    compact_attachments = [item for item in compact_attachments if item]
    if compact_attachments:
        compact["attachments"] = compact_attachments
    return {key: value for key, value in compact.items() if key == "body" or value not in ("", None, [])}


def chat_thread_attachment_content(openclaw_dir, thread_id="", message_id="", attachment_id=""):
    normalized_thread_id = str(thread_id or "").strip()
    normalized_message_id = str(message_id or "").strip()
    normalized_attachment_id = str(attachment_id or "").strip()
    if not normalized_thread_id or not normalized_message_id or not normalized_attachment_id:
        return None
    for message in safe_list(store_list_chat_messages(openclaw_dir, normalized_thread_id, limit=4096)):
        if str(message.get("id") or "").strip() != normalized_message_id:
            continue
        meta = message.get("meta") if isinstance(message.get("meta"), dict) else {}
        for item in safe_list(meta.get("attachments")):
            if not isinstance(item, dict):
                continue
            if str(item.get("id") or "").strip() != normalized_attachment_id:
                continue
            content_path = Path(str(item.get("contentPath") or "").strip())
            if not content_path.exists():
                return None
            return {
                "id": normalized_attachment_id,
                "name": str(item.get("name") or "attachment.bin").strip() or "attachment.bin",
                "mimeType": str(item.get("mimeType") or item.get("type") or "application/octet-stream").strip() or "application/octet-stream",
                "path": str(content_path),
                "bytes": content_path.read_bytes(),
            }
    return None


def _compact_chat_thread_team_policy(policy):
    policy = policy if isinstance(policy, dict) else {}
    preserve_empty_fields = {"openLoops"}

    def _normalize_lines(value, limit):
        if isinstance(value, list):
            return [
                str(item or "").strip()
                for item in value
                if str(item or "").strip()
            ][:limit]
        normalized = str(value or "").strip()
        return normalized if normalized else ""

    compact = {
        "teamMemory": str(policy.get("teamMemory") or "").strip(),
        "decisionLog": str(policy.get("decisionLog") or "").strip(),
        "workingMemory": str(policy.get("workingMemory") or "").strip(),
        "workingMemoryUpdatedAt": str(policy.get("workingMemoryUpdatedAt") or "").strip(),
        "currentFocus": str(policy.get("currentFocus") or "").strip(),
        "currentFocusUpdatedAt": str(policy.get("currentFocusUpdatedAt") or "").strip(),
        "openLoops": _normalize_lines(policy.get("openLoops"), 4),
    }

    current_focus_items = []
    for item in safe_list(policy.get("currentFocusItems"))[:3]:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        current_focus_items.append(
            {
                "text": text,
                "ownerLabel": str(item.get("ownerLabel") or "").strip(),
            }
        )
    if current_focus_items:
        compact["currentFocusItems"] = current_focus_items

    open_loop_items = []
    for item in safe_list(policy.get("openLoopItems"))[:4]:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        open_loop_items.append(
            {
                "text": text,
                "ownerLabel": str(item.get("ownerLabel") or "").strip(),
                "kind": str(item.get("kind") or "").strip() or "follow_up",
            }
        )
    if open_loop_items:
        compact["openLoopItems"] = open_loop_items

    active_owners = []
    for item in safe_list(policy.get("activeOwners"))[:3]:
        if not isinstance(item, dict):
            continue
        owner_label = str(item.get("ownerLabel") or item.get("agentId") or "").strip()
        if not owner_label:
            continue
        active_owners.append(
            {
                "ownerLabel": owner_label,
                "sources": [
                    str(source or "").strip()
                    for source in safe_list(item.get("sources"))
                    if str(source or "").strip()
                ][:2],
            }
        )
    if active_owners:
        compact["activeOwners"] = active_owners

    task_long_term_memory = policy.get("taskLongTermMemory") if isinstance(policy.get("taskLongTermMemory"), dict) else {}
    compact_task_long_term_memory = {
        "longTermMemory": str(task_long_term_memory.get("longTermMemory") or "").strip(),
        "updatedAt": str(task_long_term_memory.get("updatedAt") or "").strip(),
        "learningHighlights": [
            str(item or "").strip()
            for item in safe_list(task_long_term_memory.get("learningHighlights"))
            if str(item or "").strip()
        ][:2],
        "recentNotes": [
            str(
                (item.get("summary") if isinstance(item, dict) else item) or ""
            ).strip()
            for item in safe_list(task_long_term_memory.get("recentNotes"))
            if str((item.get("summary") if isinstance(item, dict) else item) or "").strip()
        ][:2],
    }
    compact_task_long_term_memory = {
        key: value for key, value in compact_task_long_term_memory.items() if value not in ("", None, [])
    }
    if compact_task_long_term_memory:
        compact["taskLongTermMemory"] = compact_task_long_term_memory

    return {
        key: value
        for key, value in compact.items()
        if key in preserve_empty_fields or value not in ("", None, [])
    }


def _compact_chat_thread_collaboration(collaboration):
    collaboration = collaboration if isinstance(collaboration, dict) else {}
    compact = {
        "status": str(collaboration.get("status") or "").strip(),
        "headline": str(collaboration.get("headline") or "").strip(),
        "memberCount": int(collaboration.get("memberCount") or 0),
        "responseCount": int(collaboration.get("responseCount") or 0),
        "waitingCount": int(collaboration.get("waitingCount") or 0),
        "committedCount": int(collaboration.get("committedCount") or 0),
        "blockerCount": int(collaboration.get("blockerCount") or 0),
        "relaySent": bool(collaboration.get("relaySent")),
        "relayReplyCount": int(collaboration.get("relayReplyCount") or 0),
        "respondedAgentIds": [
            str(item or "").strip()
            for item in safe_list(collaboration.get("respondedAgentIds"))
            if str(item or "").strip()
        ],
        "waitingAgentIds": [
            str(item or "").strip()
            for item in safe_list(collaboration.get("waitingAgentIds"))
            if str(item or "").strip()
        ],
        "committedAgentIds": [
            str(item or "").strip()
            for item in safe_list(collaboration.get("committedAgentIds"))
            if str(item or "").strip()
        ],
        "standbyAgentIds": [
            str(item or "").strip()
            for item in safe_list(collaboration.get("standbyAgentIds"))
            if str(item or "").strip()
        ],
        "blockerAgentIds": [
            str(item or "").strip()
            for item in safe_list(collaboration.get("blockerAgentIds"))
            if str(item or "").strip()
        ],
        "failedAgentIds": [
            str(item or "").strip()
            for item in safe_list(collaboration.get("failedAgentIds"))
            if str(item or "").strip()
        ],
    }
    filtered = {}
    for key, value in compact.items():
        if value is None:
            continue
        if isinstance(value, str) and not value:
            continue
        if isinstance(value, list) and not value:
            continue
        if isinstance(value, bool) and value is False:
            continue
        filtered[key] = value
    return filtered


def _compact_chat_thread_last_dispatch(dispatch):
    dispatch = dispatch if isinstance(dispatch, dict) else {}
    internal_discussion = (
        dispatch.get("internalDiscussion")
        if isinstance(dispatch.get("internalDiscussion"), dict)
        else {}
    )
    coordination_relay = (
        dispatch.get("coordinationRelay")
        if isinstance(dispatch.get("coordinationRelay"), dict)
        else {}
    )
    collaboration = (
        dispatch.get("collaboration")
        if isinstance(dispatch.get("collaboration"), dict)
        else {}
    )
    compact_responses = []
    for item in safe_list(dispatch.get("responses"))[:4]:
        if not isinstance(item, dict):
            continue
        compact_responses.append(
            {
                "agentId": str(item.get("agentId") or "").strip(),
                "agentLabel": str(item.get("agentLabel") or item.get("agentDisplayName") or item.get("agentId") or "").strip(),
                "sessionId": str(item.get("sessionId") or "").strip(),
                "replyPreview": summarize_task_execution_text(item.get("replyPreview") or item.get("text") or "", limit=120),
            }
        )
    compact = {
        "syncType": str(dispatch.get("syncType") or "").strip(),
        "summaryText": summarize_task_execution_text(dispatch.get("summaryText") or "", limit=220),
        "contextCompressed": bool(dispatch.get("contextCompressed")),
        "dispatchAgentIds": [
            str(item or "").strip()
            for item in safe_list(dispatch.get("dispatchAgentIds"))
            if str(item or "").strip()
        ],
        "requestedDispatchAgentIds": [
            str(item or "").strip()
            for item in safe_list(dispatch.get("requestedDispatchAgentIds"))
            if str(item or "").strip()
        ],
        "responses": [item for item in compact_responses if item.get("agentId")],
        "internalDiscussion": {
            "enabled": bool(internal_discussion.get("enabled")),
            "discussionAgentIds": [
                str(item or "").strip()
                for item in safe_list(internal_discussion.get("discussionAgentIds"))
                if str(item or "").strip()
            ],
            "advisorAgentIds": [
                str(item or "").strip()
                for item in safe_list(internal_discussion.get("advisorAgentIds"))
                if str(item or "").strip()
            ],
            "executionAgentIds": [
                str(item or "").strip()
                for item in safe_list(internal_discussion.get("executionAgentIds"))
                if str(item or "").strip()
            ],
            "summary": summarize_task_execution_text(internal_discussion.get("summary") or "", limit=180),
            "replyCount": int(internal_discussion.get("replyCount") or 0),
        },
        "coordinationRelay": {
            "sent": bool(coordination_relay.get("sent")),
            "replyCount": int(coordination_relay.get("replyCount") or 0),
            "targetAgentIds": [
                str(item or "").strip()
                for item in safe_list(coordination_relay.get("targetAgentIds"))
                if str(item or "").strip()
            ],
        },
        "collaboration": _compact_chat_thread_collaboration(collaboration),
    }
    filtered = {}
    for key, value in compact.items():
        if value is None:
            continue
        if isinstance(value, str) and not value:
            continue
        if isinstance(value, list) and not value:
            continue
        if isinstance(value, dict) and not value:
            continue
        if isinstance(value, bool) and value is False:
            continue
        filtered[key] = value
    return filtered


def compact_chat_thread_detail(detail):
    detail = detail if isinstance(detail, dict) else {}
    preserve_empty_fields = {"lastMessageId", "lastMessageAt", "lastMessagePreview"}
    workspace_path = str(
        detail.get("workspacePath")
        or ((detail.get("meta") if isinstance(detail.get("meta"), dict) else {}).get("workspacePath"))
        or ""
    ).strip()
    workspace_authorized = bool(
        detail.get("workspaceAuthorized")
        or ((detail.get("meta") if isinstance(detail.get("meta"), dict) else {}).get("workspaceAuthorized"))
    ) and bool(workspace_path)
    compact = {
        "id": str(detail.get("id") or "").strip(),
        "title": str(detail.get("title") or "").strip(),
        "owner": str(detail.get("owner") or "").strip(),
        "channel": str(detail.get("channel") or "").strip(),
        "status": str(detail.get("status") or "").strip(),
        "createdAt": str(detail.get("createdAt") or "").strip(),
        "updatedAt": str(detail.get("updatedAt") or "").strip(),
        "updatedAgo": str(detail.get("updatedAgo") or "").strip(),
        "primaryAgentId": str(detail.get("primaryAgentId") or "").strip(),
        "primaryAgentLabel": str(detail.get("primaryAgentLabel") or "").strip(),
        "currentTargetAgentId": str(detail.get("currentTargetAgentId") or "").strip(),
        "dispatchMode": str(detail.get("dispatchMode") or "").strip(),
        "mode": str(detail.get("mode") or "").strip(),
        "teamId": str(detail.get("teamId") or "").strip(),
        "linkedTaskId": str(detail.get("linkedTaskId") or "").strip(),
        "linkedRunId": str(detail.get("linkedRunId") or "").strip(),
        "linkedPackId": str(detail.get("linkedPackId") or "").strip(),
        "messageCount": int(detail.get("messageCount") or 0),
        "loadedMessageCount": int(detail.get("loadedMessageCount") or 0),
        "oldestLoadedMessageId": str(detail.get("oldestLoadedMessageId") or "").strip(),
        "newestLoadedMessageId": str(detail.get("newestLoadedMessageId") or "").strip(),
        "hasMoreBefore": bool(detail.get("hasMoreBefore")),
        "remainingBeforeCount": int(detail.get("remainingBeforeCount") or 0),
        "lastMessageId": str(detail.get("lastMessageId") or "").strip(),
        "lastMessageAt": str(detail.get("lastMessageAt") or "").strip(),
        "lastMessagePreview": str(detail.get("lastMessagePreview") or "").strip(),
        "managedTaskThread": bool(detail.get("managedTaskThread")),
        "workspacePath": workspace_path,
        "workspaceAuthorized": workspace_authorized,
    }
    task_reference = compact_chat_thread_task_reference(detail.get("linkedTask"))
    run_reference = compact_chat_thread_run_reference(detail.get("linkedRun"))
    team_reference = build_chat_thread_detail_team_reference(detail.get("linkedTeam"))
    linked_pack = detail.get("linkedPack") if isinstance(detail.get("linkedPack"), dict) else {}
    compact_pack = {
        "id": str(linked_pack.get("id") or "").strip(),
        "name": str(linked_pack.get("name") or "").strip(),
        "mode": str(linked_pack.get("mode") or "").strip(),
    }
    participant_agents = []
    for item in safe_list(detail.get("participantAgents")):
        if not isinstance(item, dict):
            continue
        participant_agents.append(
            {
                "id": str(item.get("id") or "").strip(),
                "label": str(item.get("label") or "").strip(),
                "status": str(item.get("status") or "").strip(),
                "focus": str(item.get("focus") or "").strip(),
            }
        )
    participant_humans = []
    for item in safe_list(detail.get("participantHumans")):
        if isinstance(item, dict):
            label = str(item.get("label") or item.get("name") or item.get("id") or "").strip()
        else:
            label = str(item or "").strip()
        if label:
            participant_humans.append(label)
    compact_messages = [_compact_chat_thread_message(message) for message in safe_list(detail.get("messages"))]
    compact_team_policy = _compact_chat_thread_team_policy(detail.get("teamPolicy"))
    compact_collaboration = _compact_chat_thread_collaboration(detail.get("collaboration"))
    compact_last_dispatch = _compact_chat_thread_last_dispatch(detail.get("lastDispatch"))
    compact_meta = {}
    meta = detail.get("meta") if isinstance(detail.get("meta"), dict) else {}
    task_execution = meta.get("taskExecution") if isinstance(meta.get("taskExecution"), dict) else {}
    context_compression = (
        meta.get("contextCompression")
        if isinstance(meta.get("contextCompression"), dict)
        else {}
    )
    sessions_by_agent = (
        meta.get("sessionsByAgent")
        if isinstance(meta.get("sessionsByAgent"), dict)
        else {}
    )
    if task_execution:
        compact_task_execution = {
            "managedBy": str(task_execution.get("managedBy") or "").strip(),
            "source": str(task_execution.get("source") or "").strip(),
        }
        compact_task_execution = {
            key: value for key, value in compact_task_execution.items() if value not in ("", None)
        }
        if compact_task_execution:
            compact_meta["taskExecution"] = compact_task_execution
    if context_compression:
        compact_context_compression = {
            "policy": str(context_compression.get("policy") or "").strip(),
            "policyLabel": str(context_compression.get("policyLabel") or "").strip(),
            "policySummary": str(context_compression.get("policySummary") or "").strip(),
            "baseBudget": context_compression.get("baseBudget") if isinstance(context_compression.get("baseBudget"), dict) else {},
            "effectiveBudget": context_compression.get("effectiveBudget") if isinstance(context_compression.get("effectiveBudget"), dict) else {},
            "summary": str(context_compression.get("summary") or "").strip(),
            "compressedMessageCount": int(context_compression.get("compressedMessageCount") or 0),
            "rotationCount": int(context_compression.get("rotationCount") or 0),
            "lastCompressedAt": str(context_compression.get("lastCompressedAt") or "").strip(),
            "reason": str(context_compression.get("reason") or "").strip(),
        }
        compact_context_compression = {
            key: value
            for key, value in compact_context_compression.items()
            if value not in ("", None)
        }
        if compact_context_compression:
            compact_meta["contextCompression"] = compact_context_compression
    if sessions_by_agent:
        compact_sessions_by_agent = {
            str(agent_id or "").strip(): str(session_id or "").strip()
            for agent_id, session_id in sessions_by_agent.items()
            if str(agent_id or "").strip() and str(session_id or "").strip()
        }
        if compact_sessions_by_agent:
            compact_meta["sessionsByAgent"] = compact_sessions_by_agent
    if workspace_path:
        compact_meta["workspacePath"] = workspace_path
        compact_meta["workspaceAuthorized"] = workspace_authorized

    if task_reference:
        compact["linkedTask"] = task_reference
    if run_reference:
        compact["linkedRun"] = run_reference
    if team_reference:
        compact["linkedTeam"] = team_reference
    compact_linked_teams = [
        build_chat_thread_detail_team_reference(item)
        for item in safe_list(detail.get("linkedTeams"))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    if compact_linked_teams:
        compact["linkedTeams"] = compact_linked_teams
    compact_pack = {key: value for key, value in compact_pack.items() if value not in ("", None)}
    if compact_pack:
        compact["linkedPack"] = compact_pack
    if participant_agents:
        compact["participantAgents"] = participant_agents
    if participant_humans:
        compact["participantHumans"] = participant_humans
    if compact_messages:
        compact["messages"] = compact_messages
    if compact_team_policy:
        compact["teamPolicy"] = compact_team_policy
    if compact_collaboration:
        compact["collaboration"] = compact_collaboration
    if compact_last_dispatch:
        compact["lastDispatch"] = compact_last_dispatch
    compact.update(build_chat_thread_layered_context(detail))
    if compact_meta:
        compact["meta"] = compact_meta
    filtered = {}
    for key, value in compact.items():
        if value is None:
            continue
        if isinstance(value, str) and not value and key not in preserve_empty_fields:
            continue
        if isinstance(value, list) and not value:
            continue
        if isinstance(value, bool) and value is False and key != "hasMoreBefore":
            continue
        filtered[key] = value
    return filtered
