from __future__ import annotations

import re
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


build_task_long_term_memory_payload = _DelegatedSymbol("build_task_long_term_memory_payload")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_chat_thread_team_reference = _DelegatedSymbol("compact_chat_thread_team_reference")
compact_task_long_term_memory = _DelegatedSymbol("compact_task_long_term_memory")
coordination_reply_entries = _DelegatedSymbol("coordination_reply_entries")
detect_all_hands_task_tokens = _DelegatedSymbol("detect_all_hands_task_tokens")
now_iso = _DelegatedSymbol("now_iso")
requested_team_wake_scope = _DelegatedSymbol("requested_team_wake_scope")
safe_chat_attachments = _DelegatedSymbol("safe_chat_attachments")
safe_chat_mentions = _DelegatedSymbol("safe_chat_mentions")
safe_chat_reply_context = _DelegatedSymbol("safe_chat_reply_context")
summarize_task_execution_text = _DelegatedSymbol("summarize_task_execution_text")
team_collaboration_protocol = _DelegatedSymbol("team_collaboration_protocol")
team_runtime_meta = _DelegatedSymbol("team_runtime_meta")

CHAT_THREAD_DISPATCH_MODES = {"direct", "mentions", "broadcast"}
TEAM_WAKE_SCOPES = {"lead", "all"}
CHAT_BROADCAST_INTENT_TOKENS = {
    "@all",
    "all hands",
    "broadcast",
    "everyone",
    "整个团队",
    "全体",
    "全部",
    "大家",
    "所有人",
    "都看一下",
    "同步一下",
    "一起看",
    "一起处理",
    "一起跟进",
    "所有同事",
    "整个组",
    "整组",
    "多位同事",
    "多个同事",
    "拉上大家",
    "团队一起",
    "一起协作",
}


def safe_list(value):
    return value if isinstance(value, list) else []


def normalize_chat_dispatch_mode(value, has_team=False):
    mode = str(value or "").strip().lower()
    if mode in CHAT_THREAD_DISPATCH_MODES:
        return mode
    return "direct"


def normalize_team_wake_scope(value):
    scope = str(value or "").strip().lower()
    if scope in TEAM_WAKE_SCOPES:
        return scope
    return ""


def resolve_team_default_dispatch_mode(team):
    team_meta = team_runtime_meta(team)
    return normalize_chat_dispatch_mode(team_meta.get("defaultDispatchMode"), has_team=bool((team or {}).get("id")))


def detect_chat_broadcast_intent_tokens(text):
    normalized_text = str(text or "").strip().lower()
    if not normalized_text:
        return []
    hits = []
    for token in CHAT_BROADCAST_INTENT_TOKENS:
        normalized_token = str(token or "").strip().lower()
        if normalized_token and normalized_token in normalized_text and normalized_token not in hits:
            hits.append(normalized_token)
    return hits[:6]


def detect_chat_strict_all_hands_reply_tokens(text):
    normalized_text = str(text or "").strip().lower()
    if not normalized_text:
        return []
    tokens = (
        "全员语音报道",
        "全员报道",
        "全员语音回复",
        "全员逐个回复",
        "全员逐个报到",
        "所有人逐个报到",
        "所有人都回复",
        "每个人都报到",
        "每个人都回复",
        "逐个报到",
    )
    hits = []
    for token in tokens:
        normalized_token = str(token or "").strip().lower()
        if normalized_token and normalized_token in normalized_text and normalized_token not in hits:
            hits.append(normalized_token)
    return hits[:6]


def team_operating_brief(team):
    team_meta = team_runtime_meta(team)
    return str(team_meta.get("operatingBrief") or "").strip()


def team_memory_text(team):
    team_meta = team_runtime_meta(team)
    return str(team_meta.get("teamMemory") or "").strip()


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


def normalize_chat_thread_linked_team_ids(team_id="", linked_team_ids=None, collaborator_team_ids=None):
    normalized_primary = str(team_id or "").strip()
    collected = []
    for candidate in [normalized_primary, *safe_list(linked_team_ids), *safe_list(collaborator_team_ids)]:
        value = str(candidate or "").strip()
        if value and value not in collected:
            collected.append(value)
    return collected


def linked_team_ids_for_chat_thread(thread_or_meta):
    source = thread_or_meta if isinstance(thread_or_meta, dict) else {}
    meta = source.get("meta") if isinstance(source.get("meta"), dict) else source
    team_id = str(meta.get("teamId") or source.get("teamId") or "").strip()
    return normalize_chat_thread_linked_team_ids(team_id, meta.get("linkedTeamIds"))


def compact_chat_thread_team_references(team_map, linked_team_ids):
    refs = []
    for team_id in normalize_chat_thread_linked_team_ids(linked_team_ids=linked_team_ids):
        team_ref = compact_chat_thread_team_reference(team_map.get(team_id))
        if team_ref.get("id"):
            refs.append(team_ref)
    return refs


def resolve_chat_dispatch_targets(
    thread,
    target_agent_id="",
    mention_agent_ids=None,
    dispatch_mode="",
    dispatch_explicit=False,
    message_text="",
):
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_id = str(meta.get("teamId") or "").strip()
    linked_team_ids = linked_team_ids_for_chat_thread(meta)
    team_policy = meta.get("teamPolicy") if isinstance(meta.get("teamPolicy"), dict) else {}
    participant_agent_ids = []
    for item in safe_list(thread.get("participantAgentIds")):
        agent_id = str(item or "").strip()
        if agent_id and agent_id not in participant_agent_ids:
            participant_agent_ids.append(agent_id)
    requested_target_agent_id = str(target_agent_id or "").strip()
    current_target_agent_id = (
        requested_target_agent_id
        or str(thread.get("currentTargetAgentId") or "").strip()
        or str(thread.get("primaryAgentId") or "").strip()
    )
    if current_target_agent_id and current_target_agent_id not in participant_agent_ids:
        participant_agent_ids.append(current_target_agent_id)
    mention_agent_ids = safe_chat_mentions(mention_agent_ids)
    for agent_id in mention_agent_ids:
        if agent_id not in participant_agent_ids:
            participant_agent_ids.append(agent_id)
    resolved_mode = normalize_chat_dispatch_mode(
        dispatch_mode or meta.get("dispatchMode") or team_policy.get("defaultDispatchMode"),
        has_team=bool(team_id or linked_team_ids),
    )
    requested_mode = str(dispatch_mode or "").strip().lower()
    dispatch_was_explicit = bool(dispatch_explicit or meta.get("dispatchModeExplicit"))
    broadcast_intent_hits = detect_chat_broadcast_intent_tokens(message_text)
    strict_all_hands_reply_hits = detect_chat_strict_all_hands_reply_tokens(message_text)
    if (
        resolved_mode != "broadcast"
        and not dispatch_was_explicit
        and not mention_agent_ids
        and (broadcast_intent_hits or strict_all_hands_reply_hits)
        and (team_id or linked_team_ids)
        and len(participant_agent_ids) > 1
    ):
        resolved_mode = "broadcast"
    if (
        resolved_mode == "broadcast"
        and not mention_agent_ids
        and not dispatch_was_explicit
        and not detect_all_hands_task_tokens(message_text)
        and not broadcast_intent_hits
        and not strict_all_hands_reply_hits
        and (team_id or linked_team_ids)
        and current_target_agent_id
    ):
        resolved_mode = "direct"
    if resolved_mode == "broadcast":
        dispatch_agent_ids = list(participant_agent_ids)
    elif resolved_mode == "mentions":
        dispatch_agent_ids = [agent_id for agent_id in mention_agent_ids if agent_id in participant_agent_ids]
        if not dispatch_agent_ids and current_target_agent_id:
            dispatch_agent_ids = [current_target_agent_id]
    else:
        dispatch_agent_ids = [current_target_agent_id] if current_target_agent_id else []
    if not dispatch_agent_ids and participant_agent_ids:
        dispatch_agent_ids = participant_agent_ids[:1]
    return {
        "dispatchMode": resolved_mode,
        "requestedDispatchMode": requested_mode or resolved_mode,
        "dispatchModeExplicit": dispatch_was_explicit,
        "broadcastIntentHits": broadcast_intent_hits,
        "strictAllHandsReplyHits": strict_all_hands_reply_hits,
        "strictAllHandsReplies": bool(strict_all_hands_reply_hits),
        "participantAgentIds": participant_agent_ids,
        "currentTargetAgentId": current_target_agent_id,
        "dispatchAgentIds": dispatch_agent_ids,
    }


def summarize_chat_dispatch_result(dispatch_mode, successes, failures):
    success_count = len(successes)
    failure_count = len(failures)
    if success_count == 1 and successes[0].get("replyPreview"):
        return successes[0]["replyPreview"][:160]
    if success_count > 1:
        if dispatch_mode == "broadcast":
            return f"团队广播已收到 {success_count} 位成员回复。"
        if dispatch_mode == "mentions":
            return f"被点名成员已收到 {success_count} 条回复。"
        return f"当前线程已收到 {success_count} 条回复。"
    if failure_count:
        return "消息已写入团队线程，但暂未收到成员回包。"
    return "消息已发送。"


def build_team_working_memory(dispatch_state, limit=4):
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    memory_lines = []
    response_entries = coordination_reply_entries(dispatch_state.get("responses"), limit=limit)
    for item in response_entries:
        line = f"{str(item.get('agentDisplayName') or item.get('agentId') or '').strip()}：{str(item.get('replyPreview') or '').strip()}"
        if line and line not in memory_lines:
            memory_lines.append(line)
    relay = dispatch_state.get("coordinationRelay") if isinstance(dispatch_state.get("coordinationRelay"), dict) else {}
    relay_entries = coordination_reply_entries(relay.get("responses"), limit=max(1, limit - len(memory_lines)))
    for item in relay_entries:
        line = f"{str(item.get('agentDisplayName') or item.get('agentId') or '').strip()}：{str(item.get('replyPreview') or '').strip()}"
        if line and line not in memory_lines:
            memory_lines.append(line)
    collaboration = dispatch_state.get("collaboration") if isinstance(dispatch_state.get("collaboration"), dict) else {}
    if collaboration.get("blockerCount"):
        memory_lines.append(f"当前提醒：{int(collaboration.get('blockerCount') or 0)} 人提到了卡点或需要支援。")
    if not memory_lines:
        return ""
    return "\n".join(f"- {line}" for line in memory_lines[: max(1, int(limit or 4))])


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


def apply_team_working_memory(team_policy, dispatch_state):
    next_policy = deepcopy(team_policy) if isinstance(team_policy, dict) else {}
    updated_at = str(dispatch_state.get("at") or now_iso()).strip()
    working_memory = build_team_working_memory(dispatch_state, limit=4)
    current_focus_items = build_team_current_focus_items(dispatch_state, limit=3)
    current_focus = [str(item.get("text") or "").strip() for item in current_focus_items if str(item.get("text") or "").strip()]
    if not current_focus:
        current_focus = build_team_current_focus(dispatch_state, limit=3)
    open_loop_items = build_team_open_loop_items(dispatch_state, limit=4)
    open_loops = [str(item.get("text") or "").strip() for item in open_loop_items if str(item.get("text") or "").strip()]
    if not open_loops:
        open_loops = build_team_open_loops(dispatch_state, limit=4)
    active_owners = build_team_active_owners(current_focus_items, open_loop_items, limit=4)
    next_policy["workingMemory"] = working_memory
    next_policy["workingMemoryUpdatedAt"] = updated_at
    next_policy["currentFocus"] = "\n".join(f"- {line}" for line in current_focus) if current_focus else ""
    next_policy["currentFocusUpdatedAt"] = updated_at if current_focus else ""
    next_policy["currentFocusItems"] = current_focus_items
    next_policy["openLoops"] = clean_unique_strings(open_loops)
    next_policy["openLoopItems"] = open_loop_items
    next_policy["activeOwners"] = active_owners
    next_policy["taskLongTermMemory"] = build_task_long_term_memory_payload(
        next_policy.get("taskLongTermMemory"),
        dispatch_state,
        fallback_title=str(dispatch_state.get("taskTitle") or dispatch_state.get("contextLabel") or "").strip(),
        fallback_note=str(dispatch_state.get("note") or "").strip(),
    )
    return next_policy


def team_memory_trace_payload(team_policy):
    team_policy = team_policy if isinstance(team_policy, dict) else {}
    working_memory = str(team_policy.get("workingMemory") or "").strip()
    team_memory = str(team_policy.get("teamMemory") or "").strip()
    decision_log = str(team_policy.get("decisionLog") or "").strip()
    current_focus = str(team_policy.get("currentFocus") or "").strip()
    current_focus_items = safe_list(team_policy.get("currentFocusItems"))
    open_loops = normalize_team_context_lines(team_policy.get("openLoops"), limit=4)
    open_loop_items = safe_list(team_policy.get("openLoopItems"))
    active_owners = safe_list(team_policy.get("activeOwners"))
    task_long_term_memory = compact_task_long_term_memory(team_policy.get("taskLongTermMemory"))

    def first_memory_line(value):
        raw_lines = value if isinstance(value, list) else str(value or "").splitlines()
        for raw_line in raw_lines:
            normalized = re.sub(r"^[\-\u2022]\s*", "", raw_line.strip())
            if normalized:
                return normalized
        return ""

    def first_owned_item(items):
        fallback_item = {}
        for item in safe_list(items):
            if not isinstance(item, dict):
                continue
            if not fallback_item:
                fallback_item = item
            owner_label = str(item.get("ownerLabel") or item.get("agentId") or "").strip()
            if owner_label:
                return item
        return fallback_item

    payload = {}
    if active_owners:
        payload["activeOwnerLabels"] = [
            str(item.get("ownerLabel") or item.get("agentId") or "").strip()
            for item in active_owners
            if isinstance(item, dict) and str(item.get("ownerLabel") or item.get("agentId") or "").strip()
        ][:3]
    if current_focus:
        payload["currentFocusApplied"] = True
        payload["currentFocusPreview"] = first_memory_line(current_focus)
        if current_focus_items:
            first_focus = first_owned_item(current_focus_items)
            owner_label = str(first_focus.get("ownerLabel") or first_focus.get("agentId") or "").strip()
            if owner_label:
                payload["currentFocusOwnerLabel"] = owner_label
    if open_loops:
        payload["openLoopApplied"] = True
        payload["openLoopPreview"] = first_memory_line(open_loops)
        if open_loop_items:
            first_loop = first_owned_item(open_loop_items)
            owner_label = str(first_loop.get("ownerLabel") or first_loop.get("agentId") or "").strip()
            if owner_label:
                payload["openLoopOwnerLabel"] = owner_label
            open_loop_kind = str(first_loop.get("kind") or "").strip()
            if open_loop_kind:
                payload["openLoopKind"] = open_loop_kind
    if working_memory:
        payload["workingMemoryApplied"] = True
        payload["workingMemoryPreview"] = first_memory_line(working_memory)
    if team_memory or decision_log:
        payload["teamGuidanceApplied"] = True
        payload["teamGuidancePreview"] = first_memory_line(team_memory) or first_memory_line(decision_log)
    if task_long_term_memory.get("longTermMemory"):
        payload["taskLongTermMemoryApplied"] = True
        payload["taskLongTermMemoryPreview"] = first_memory_line(task_long_term_memory.get("longTermMemory"))
    return payload


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


def format_attachment_size(size):
    value = max(0, int(size or 0))
    if value >= 1024 * 1024:
        return f"{value / (1024 * 1024):.1f} MB"
    if value >= 1024:
        return f"{round(value / 1024)} KB"
    return f"{value} B"


def merge_chat_message_with_attachments(message_text, attachments, mention_agent_ids=None, team_policy=None, reply_context=None):
    message_text = str(message_text or "").strip()
    attachments = safe_chat_attachments(attachments)
    mention_agent_ids = safe_chat_mentions(mention_agent_ids)
    reply_context = safe_chat_reply_context(reply_context)
    team_policy = team_policy if isinstance(team_policy, dict) else {}
    blocks = []
    operating_brief = summarize_task_execution_text(team_policy.get("operatingBrief") or "", limit=140)
    team_memory = summarize_task_execution_text(team_policy.get("teamMemory") or "", limit=140)
    working_memory = summarize_task_execution_text(team_policy.get("workingMemory") or "", limit=120)
    decision_log = summarize_task_execution_text(team_policy.get("decisionLog") or "", limit=140)
    current_focus = summarize_task_execution_text(team_policy.get("currentFocus") or "", limit=120)
    open_loops = [
        summarize_task_execution_text(item, limit=88)
        for item in normalize_team_context_lines(team_policy.get("openLoops"), limit=2)
    ]
    open_loops = [item for item in open_loops if item]
    task_long_term_memory = compact_task_long_term_memory(team_policy.get("taskLongTermMemory"))
    protocol = team_policy.get("coordinationProtocol") if isinstance(team_policy.get("coordinationProtocol"), dict) else {}
    human_tone = summarize_task_execution_text(team_policy.get("humanToneGuide") or protocol.get("humanToneGuide") or "", limit=120)
    proactive_rules = clean_unique_strings(team_policy.get("proactiveRules") or protocol.get("proactiveRules") or [])
    update_contract = summarize_task_execution_text(protocol.get("updateContract") or "", limit=120)
    reply_contract_lines = [
        "Reply with the direct answer or judgment first.",
        "Keep the visible reply short and conversational unless the user clearly asks for depth.",
        "Default to 2-6 concise sentences or a very short list when it helps clarity.",
        "If there is a best path, say it directly instead of listing every possibility first.",
        "Use remembered context implicitly. Do not re-list stored memory, old decisions, or long background unless it changes the current answer.",
        "If you need to mention the past, fold it into one short natural sentence instead of announcing 'memory', 'history', or 'context'.",
        "If the user asks how memory works here, explain the product memory system and projected task/team context; do not say it only relies on MEMORY.md or memory/*.md.",
        "If another teammate should step in, you can pull them into the thread or say who should take over this part.",
        "Do not say you lack permission to involve teammates unless the system explicitly returned a real permission error.",
        "If browser, canvas, or runtime capability errors show up, first try a reasonable fallback path or explain the smallest workable workaround; do not dump raw environment errors into the visible reply.",
        "Do not end with 'not my problem', 'environment limitation', or tell the user to handle DevOps/browser setup themselves unless you already tried the available fallback path and clearly say what was tried.",
        "Avoid role labels, status labels, coordination tags, or system-style formatting in the visible reply.",
    ]
    if operating_brief:
        blocks.append(f"Team brief\n{operating_brief}")
    if team_memory:
        blocks.append(f"Team memory\n{team_memory}")
    if current_focus:
        blocks.append(f"Current focus\n{current_focus}")
    if open_loops:
        blocks.append("Open loops\n" + "\n".join(f"- {item}" for item in open_loops))
    if working_memory:
        blocks.append(f"Recent team memory\n{working_memory}")
    if task_long_term_memory.get("longTermMemory"):
        blocks.append(f"Task long-term memory\n{summarize_task_execution_text(task_long_term_memory.get('longTermMemory'), limit=140)}")
    if task_long_term_memory.get("learningHighlights"):
        learning_items = [
            summarize_task_execution_text(item, limit=88)
            for item in safe_list(task_long_term_memory.get("learningHighlights"))[:1]
        ]
        learning_items = [item for item in learning_items if item]
        if learning_items:
            blocks.append("Task learnings\n" + "\n".join(f"- {item}" for item in learning_items))
    if decision_log:
        blocks.append(f"Decision log\n{decision_log}")
    if human_tone:
        blocks.append(f"Communication style\n{human_tone}")
    if proactive_rules:
        blocks.append("Proactive coordination\n" + "\n".join(f"- {item}" for item in proactive_rules))
    if update_contract:
        blocks.append(f"Update contract\n{update_contract}")
    blocks.append("Reply contract\n" + "\n".join(f"- {item}" for item in reply_contract_lines))
    if mention_agent_ids:
        blocks.append("Mentioned members\n" + "\n".join(f"- @{item}" for item in mention_agent_ids))
    if reply_context:
        reply_sender = str(reply_context.get("sender") or "Earlier message").strip() or "Earlier message"
        reply_text = str(reply_context.get("text") or "").strip()
        reply_lines = [f"Replying to\n- {reply_sender}"]
        if reply_text:
            reply_lines.append(f"- Context: {reply_text}")
        blocks.append("\n".join(reply_lines))
    if attachments:
        lines = []
        for item in attachments:
            meta = item["name"]
            size_text = format_attachment_size(item.get("size", 0))
            if size_text:
                meta = f"{meta} ({size_text}"
                if item.get("type"):
                    meta = f"{meta}, {item['type']}"
                meta = f"{meta})"
            elif item.get("type"):
                meta = f"{meta} ({item['type']})"
            if item.get("preview"):
                lines.append(f"- {meta}\n  {item['preview']}")
            else:
                lines.append(f"- {meta}")
        blocks.append("Attachments\n" + "\n".join(lines))
    if not blocks:
        return message_text
    if message_text:
        return f"{message_text}\n\n" + "\n\n".join(blocks)
    return "\n\n".join(blocks)
