from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

from . import orchestration_core as core
from . import orchestration_coordination_core as coordination_core
from .tool_enabled_dispatch import (
    perform_tool_enabled_conversation_send_sync,
    build_tool_query_config,
    build_tool_system_prompt,
)
from backend.application.services.harness_bootstrap import bootstrap_harness


_SHARED_NAMES = [
    "TASK_EXECUTION_DISPATCH_FUTURES",
    "TASK_EXECUTION_DISPATCH_FUTURES_LOCK",
    "TASK_EXECUTION_DISPATCH_POOL",
    "TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS",
    "TEAM_CONVERSATION_MAX_PARALLEL",
    "TEAM_CONVERSATION_STAGGER_SECONDS",
    "_discard_task_execution_dispatch_future",
    "_svc",
    "agent_runtime_identity_payload",
    "apply_conversation_fanout_stagger",
    "apply_team_working_memory",
    "apply_turn_guidance_to_message",
    "bootstrap_task_execution_state",
    "build_company_auto_operation_profile",
    "build_task_dispatch_validation",
    "build_human_turn_anchor_payload",
    "build_human_turn_profile_payload",
    "build_memory_projection_payloads",
    "build_task_execution_message",
    "build_task_internal_discussion_message",
    "build_task_team_fanout_message",
    "build_task_team_member_message",
    "build_team_collaboration_summary",
    "chat_thread_session_id",
    "clean_unique_strings",
    "compact_auto_operation_profile",
    "compact_company_auto_operation_runtime",
    "compact_task_long_term_memory",
    "conversation_reply_preview",
    "current_management_automation_mode",
    "deepcopy",
    "ensure_task_execution_team_thread",
    "get_router_agent_id",
    "invalidate_dashboard_bundle_cache",
    "load_agents",
    "load_config",
    "load_model_execution_architecture_payload",
    "load_project_metadata",
    "logging",
    "merge_tasks",
    "merge_team_policy_state",
    "maybe_forward_chat_thread_reply_to_wechat",
    "maybe_attach_chat_thread_voice_reply",
    "maybe_prepare_chat_thread_context_compression",
    "normalize_team_context_lines",
    "now_iso",
    "order_agent_ids_for_human_turns",
    "perform_conversation_fanout",
    "perform_conversation_send",
    "Path",
    "project_memory_records_async",
    "publish_chat_thread_stream_event",
    "safe_chat_attachments",
    "safe_chat_mentions",
    "safe_list",
    "select_persistable_conversation_session_id",
    "select_human_turn_targets",
    "load_json",
    "store_get_chat_thread",
    "store_get_task_record",
    "store_list_agent_teams",
    "store_list_chat_messages",
    "store_save_chat_message",
    "store_save_chat_thread",
    "submit_task_execution_background",
    "submit_task_execution_background_deferred",
    "summarize_internal_discussion_context",
    "summarize_task_execution_text",
    "task_coordination_protocol_snapshot",
    "task_execution_agent_lock",
    "task_execution_bootstrap_for_task",
    "task_execution_session_id",
    "task_internal_discussion_plan",
    "task_route_meta_payload",
    "task_team_participant_agent_ids",
    "team_memory_trace_payload",
    "team_state_packet_payload",
    "update_task_execution_bootstrap",
    "has_blocking_task_dispatch_validation",
]

globals().update({name: getattr(core, name) for name in _SHARED_NAMES})
relay_team_coordination_updates = coordination_core.relay_team_coordination_updates
schedule_chat_thread_coordination_relay = coordination_core.schedule_chat_thread_coordination_relay
update_task_team_dispatch_state = coordination_core.update_task_team_dispatch_state


def _strict_all_hands_reply_contract(request_text=""):
    normalized_request = str(request_text or "").strip().lower()
    voice_requested = any(token in normalized_request for token in ("语音", "音频", "voice", "audio"))
    lines = [
        "这是严格全员报到模式：每位成员都必须在当前线程里可见回复一次。",
        "不要回复 STANDBY、待命、候命、暂不介入，也不要把发言机会让给别人。",
        "请直接用简体中文给出一句短报到，结合你的岗位说明你已接住什么，不要超过 30 个字。",
    ]
    if voice_requested:
        lines.extend(
            [
                "产品会在后台把你的正文挂成语音附件；你只负责输出自然中文正文。",
                "不要提语音通道、TTS、provider、配置、补发、稍后再发或任何运行时限制。",
            ]
        )
    return "\n".join(lines)


def _strict_all_hands_fallback_reply(display_name="", job_title=""):
    label = str(display_name or "").strip() or str(job_title or "").strip() or "成员"
    return f"{label}已就位，已接住本岗位事项。"


def _sanitize_strict_all_hands_reply_text(text, display_name="", job_title=""):
    normalized = re.sub(r"\s+", " ", str(text or "").strip())
    fallback = _strict_all_hands_fallback_reply(display_name=display_name, job_title=job_title)
    if not normalized:
        return fallback
    lowered = normalized.lower()
    blocked_tokens = (
        "语音通道",
        "tts",
        "provider",
        "配置",
        "补发",
        "稍后再发",
        "暂未配置",
        "待命",
        "候命",
        "standby",
    )
    if any(token in lowered for token in blocked_tokens):
        return fallback
    sentences = [item.strip(" ，,。；;！!？?") for item in re.split(r"[。！？!?\\n]+", normalized) if item.strip()]
    candidate = sentences[0] if sentences else ""
    candidate = re.sub(r"^(负责人好|负责人|你好|各位好|大家好)[，,：:]?", "", candidate).strip()
    if not candidate:
        return fallback
    if any(token in candidate.lower() for token in blocked_tokens):
        return fallback
    if "已就位" not in candidate and "接住" not in candidate and "到位" not in candidate:
        return fallback
    if len(candidate) > 36:
        return fallback
    return f"{candidate}。"


def _is_tool_enabled_agent(openclaw_dir, agent_id, config=None, metadata=None):
    """Check if an agent should use tool-enabled dispatch.

    Returns True when the agent has ``toolEnabled: true`` in its config,
    OR when the thread's meta has ``enableToolUse: true``.
    """
    if config is None:
        config = load_config(openclaw_dir)
    agents = load_agents(config)
    for agent in safe_list(agents):
        if not isinstance(agent, dict):
            continue
        if str(agent.get("id") or "").strip() == str(agent_id or "").strip():
            if agent.get("toolEnabled") or agent.get("tool_enabled"):
                return True
            capabilities = safe_list(agent.get("capabilities"))
            if "tool_use" in capabilities or "tools" in capabilities:
                return True
    return False


def _perform_tool_enabled_dispatch(
    openclaw_dir,
    agent_id,
    message,
    thread_id="",
    workspace_path="",
    stream_observer=None,
    agent_display_name="",
    agent_job_title="",
    thinking="low",
    model="",
    workspace_authorized=False,
):
    """Dispatch a message through the tool-enabled QueryEngine path."""
    # When workspace_authorized is True, the user selected a local directory
    # on their desktop.  Route tools through the ToolBridge so they execute
    # on the desktop client instead of the server.
    effective_workspace = workspace_path or openclaw_dir
    is_remote_workspace = bool(
        workspace_authorized
        and workspace_path
        and workspace_path != openclaw_dir
    )
    if is_remote_workspace:
        logger.info(
            "[dispatch] using REMOTE tool executor (desktop proxy): thread=%s workspace=%s",
            thread_id, workspace_path,
        )
    harness = bootstrap_harness(
        openclaw_dir=openclaw_dir,
        workspace_path=effective_workspace,
        remote_thread_id=thread_id if is_remote_workspace else "",
    )

    system_prompt = build_tool_system_prompt(
        agent_display_name=agent_display_name,
        agent_job_title=agent_job_title,
        workspace_path=workspace_path or openclaw_dir,
    )

    config = build_tool_query_config(thinking=thinking, model=model, workspace_authorized=workspace_authorized)

    def stream_callback(event):
        if not callable(stream_observer):
            return
        event_type = str(event.get("type") or "").strip()
        if event_type == "delta":
            stream_observer({
                "stage": "delta",
                "agentId": agent_id,
                "content": event.get("content", ""),
                "delta": event.get("delta", ""),
            })
        elif event_type == "tool_call":
            stream_observer({
                "stage": "tool_call",
                "agentId": agent_id,
                "toolName": event.get("toolName", ""),
                "toolArgs": event.get("toolArgs", {}),
            })
        elif event_type == "tool_result":
            stream_observer({
                "stage": "tool_result",
                "agentId": agent_id,
                "toolName": event.get("toolName", ""),
                "result": event.get("result", ""),
            })
        elif event_type == "tool_error":
            stream_observer({
                "stage": "tool_error",
                "agentId": agent_id,
                "toolName": event.get("toolName", ""),
                "error": event.get("error", ""),
            })
        elif event_type == "permission_request":
            stream_observer({
                "stage": "permission_request",
                "agentId": agent_id,
                "requestId": event.get("requestId", ""),
                "toolName": event.get("toolName", ""),
                "toolArgs": event.get("toolArgs", {}),
            })
        elif event_type in ("query_started",):
            stream_observer({
                "stage": "started",
                "agentId": agent_id,
                "content": "",
            })

    result = perform_tool_enabled_conversation_send_sync(
        query_engine=harness.query_engine,
        agent_id=agent_id,
        message=message,
        system_prompt=system_prompt,
        config=config,
        thread_id=thread_id,
        stream_callback=stream_callback,
    )

    payloads = ((result.get("result", {}) or {}).get("payloads", []) or [])
    reply_text = str(result.get("reply_text") or "").strip()
    if not reply_text and payloads and isinstance(payloads[0], dict):
        reply_text = str(payloads[0].get("text") or "").strip()
    if str(result.get("status") or "").strip().lower() != "ok" or not reply_text:
        error_text = str(result.get("error") or "").strip()
        if not error_text:
            for event in reversed(result.get("events") if isinstance(result.get("events"), list) else []):
                if not isinstance(event, dict):
                    continue
                if str(event.get("type") or "").strip() == "query_error":
                    error_text = str(event.get("error") or "").strip()
                    if error_text:
                        break
        normalized_error = error_text.lower()
        if "credit balance is too low" in normalized_error:
            raise RuntimeError("Claude 当前账号额度不足，暂时无法回复。")
        if "exceeded your current quota" in normalized_error or "insufficient_quota" in normalized_error:
            raise RuntimeError("OpenAI 当前账号额度不足，暂时无法回复。")
        if "余额不足" in error_text or "无可用资源包" in error_text:
            raise RuntimeError("GLM 当前账号余额不足，暂时无法回复。")
        if "model_not_found" in normalized_error or "does not exist" in normalized_error:
            raise RuntimeError("当前成员模型不可用，请切换到有效模型后再试。")
        if error_text:
            raise RuntimeError(error_text.splitlines()[-1].strip() or "Agent 当前未返回可见回复。")
        raise RuntimeError("Agent 当前未返回可见回复。")

    return result


def _humanize_dispatch_error_message(error_message=""):
    normalized_error = str(error_message or "").strip()
    lowered = normalized_error.lower()
    if not normalized_error:
        return "会话发送失败。"
    if "credit balance is too low" in lowered:
        return "Claude 当前账号额度不足，暂时无法回复。"
    if "exceeded your current quota" in lowered or "insufficient_quota" in lowered:
        return "OpenAI 当前账号额度不足，暂时无法回复。"
    if "余额不足" in normalized_error or "无可用资源包" in normalized_error or "\"code\":\"1113\"" in lowered:
        return "GLM 当前账号余额不足，暂时无法回复。"
    if "model_not_found" in lowered or "does not exist" in lowered or "模型不存在" in normalized_error:
        return "当前成员模型不可用，请切换到有效模型后再试。"
    if "http/status/429" in lowered:
        return "当前模型服务额度不足或暂时限流，暂时无法回复。"
    lines = [line.strip() for line in normalized_error.splitlines() if line.strip()]
    return lines[-1] if lines else "会话发送失败。"


def _team_thread_wake_message(thread_title="", team_name=""):
    label = str(team_name or thread_title or "this team thread").strip() or "this team thread"
    return (
        f"Team thread wake check for {label}. "
        "You were asked to join the current collaboration. "
        "Reply with your current lane status and the next action in one sentence."
    )


def _dispatch_session_manifest_entry(openclaw_dir, agent_id, session_id):
    normalized_agent_id = str(agent_id or "").strip()
    normalized_session_id = str(session_id or "").strip()
    if not normalized_agent_id or not normalized_session_id:
        return {}
    sessions_dir = Path(openclaw_dir) / "agents" / normalized_agent_id / "sessions"
    payload = load_json(sessions_dir / "sessions.json", {})
    index = payload if isinstance(payload, dict) else {}
    direct_key = f"agent:{normalized_agent_id}:{normalized_session_id}"
    direct_match = index.get(direct_key)
    if isinstance(direct_match, dict):
        return direct_match
    for key, value in index.items():
        if not isinstance(value, dict):
            continue
        if key == normalized_session_id or str(value.get("sessionId") or "").strip() == normalized_session_id:
            return value
    return {}


def _is_reserved_dispatch_session_entry(entry):
    entry = entry if isinstance(entry, dict) else {}
    origin = entry.get("origin") if isinstance(entry.get("origin"), dict) else {}
    provider = str(origin.get("provider") or "").strip().lower()
    label = str(origin.get("label") or "").strip().lower()
    from_value = str(origin.get("from") or "").strip().lower()
    to_value = str(origin.get("to") or "").strip().lower()
    return "heartbeat" in {provider, label, from_value, to_value}


def _is_reserved_dispatch_session(openclaw_dir, agent_id, session_id):
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return False
    return _is_reserved_dispatch_session_entry(
        _dispatch_session_manifest_entry(openclaw_dir, agent_id, normalized_session_id)
    )


def _sanitize_dispatch_session_id(openclaw_dir, agent_id, session_id):
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return ""
    if _is_reserved_dispatch_session(openclaw_dir, agent_id, normalized_session_id):
        return ""
    return normalized_session_id


def _is_synthetic_dispatch_session_id(session_id):
    normalized_session_id = str(session_id or "").strip().lower()
    if not normalized_session_id:
        return False
    return (
        normalized_session_id == "main"
        or normalized_session_id.startswith("chat-")
        or normalized_session_id.startswith("task-")
    )


def _persisted_dispatch_session_id(openclaw_dir, agent_id, primary_session_id="", fallback_session_id=""):
    primary_value = str(primary_session_id or "").strip()
    if primary_value and not _is_synthetic_dispatch_session_id(primary_value):
        sanitized_primary = _sanitize_dispatch_session_id(openclaw_dir, agent_id, primary_value)
        if sanitized_primary:
            return sanitized_primary
    if primary_value:
        return ""
    fallback_value = str(fallback_session_id or "").strip()
    if fallback_value and not _is_synthetic_dispatch_session_id(fallback_value):
        sanitized_fallback = _sanitize_dispatch_session_id(openclaw_dir, agent_id, fallback_value)
        if sanitized_fallback:
            return sanitized_fallback
    return ""


def schedule_chat_thread_dispatch(
    openclaw_dir,
    thread,
    outbound_message,
    dispatch,
    merged_message,
    sessions_by_agent=None,
    team_policy=None,
    attachments=None,
    mention_agent_ids=None,
    thinking="low",
    output_dir=None,
    defer_seconds=0.0,
    server=None,
    selected_skill_slugs=None,
):
    thread = thread if isinstance(thread, dict) else {}
    outbound_message = outbound_message if isinstance(outbound_message, dict) else {}
    dispatch = dispatch if isinstance(dispatch, dict) else {}
    thread_id = str(thread.get("id") or "").strip()
    target_agent_id = str(dispatch.get("currentTargetAgentId") or "").strip()
    dispatch_mode = str(dispatch.get("dispatchMode") or "").strip()
    strict_all_hands_replies = bool(dispatch.get("strictAllHandsReplies"))
    dispatch_agent_ids = [str(item or "").strip() for item in safe_list(dispatch.get("dispatchAgentIds")) if str(item or "").strip()]
    participant_agent_ids = [str(item or "").strip() for item in safe_list(dispatch.get("participantAgentIds")) if str(item or "").strip()]
    if not thread_id or not dispatch_agent_ids:
        return {
            "scheduled": False,
            "mode": dispatch_mode,
            "requestedTargetAgentId": target_agent_id,
            "dispatchAgentIds": dispatch_agent_ids,
            "respondedAgentIds": [],
            "failedAgents": [],
            "replyCount": 0,
            "at": now_iso(),
        }
    team_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_id = str(team_meta.get("teamId") or "").strip()
    session_map = {
        str(agent_id or "").strip(): str(session_id or "").strip()
        for agent_id, session_id in (sessions_by_agent or {}).items()
        if str(agent_id or "").strip()
    }
    safe_attachments = safe_chat_attachments(attachments)
    safe_mentions = safe_chat_mentions(mention_agent_ids)
    selected_skill_slugs = clean_unique_strings(selected_skill_slugs or [])
    safe_policy = deepcopy(team_policy) if isinstance(team_policy, dict) else {}
    memory_trace_meta = team_memory_trace_payload(safe_policy)
    state_packet_meta = team_state_packet_payload(safe_policy)
    thinking_value = str(thinking or "").strip() or "low"
    direct_fast_path = dispatch_mode == "direct" and len(dispatch_agent_ids) == 1

    def worker():
        latest_thread = store_get_chat_thread(openclaw_dir, thread_id) or thread
        latest_meta = deepcopy(latest_thread.get("meta", {})) if isinstance(latest_thread.get("meta"), dict) else {}
        config = None
        metadata = None
        agents = None
        primary_execution_path = ""
        hosted_provider_context_budget_policy = ""
        ordered_dispatch_agent_ids = clean_unique_strings(dispatch_agent_ids)
        if not direct_fast_path:
            config = load_config(openclaw_dir)
            metadata = load_project_metadata(openclaw_dir, config=config)
            execution_architecture = load_model_execution_architecture_payload(openclaw_dir, config=config, metadata=metadata)
            primary_execution_path = str(execution_architecture.get("primaryPath") or "").strip()
            execution_policy = execution_architecture.get("executionPolicy") if isinstance(execution_architecture.get("executionPolicy"), dict) else {}
            provider_path = execution_architecture.get("providerPath") if isinstance(execution_architecture.get("providerPath"), dict) else {}
            hosted_provider_context_budget_policy = str(
                execution_policy.get("hostedProviderContextBudgetPolicy")
                or provider_path.get("hostedProviderContextBudgetPolicy")
                or ""
            ).strip()
            agents = load_agents(config)
            ordered_dispatch_agent_ids = order_agent_ids_for_human_turns(
                openclaw_dir,
                dispatch_agent_ids,
                lead_agent_id=target_agent_id,
                config=config,
                metadata=metadata,
                agents=agents,
            )
            if team_id and dispatch_mode == "broadcast" and not safe_mentions:
                ordered_dispatch_agent_ids = select_human_turn_targets(
                    openclaw_dir,
                    ordered_dispatch_agent_ids,
                    purpose="broadcast",
                    strict_all_replies=strict_all_hands_replies,
                    lead_agent_id=target_agent_id,
                    config=config,
                    metadata=metadata,
                    agents=agents,
                )
        targeted_dispatch_agent_ids = clean_unique_strings(ordered_dispatch_agent_ids)
        latest_sessions_source = latest_meta.get("sessionsByAgent") if isinstance(latest_meta.get("sessionsByAgent"), dict) else {}
        latest_sessions = {
            str(agent_id or "").strip(): sanitized_session_id
            for agent_id, session_id in latest_sessions_source.items()
            if str(agent_id or "").strip()
            for sanitized_session_id in [_sanitize_dispatch_session_id(openclaw_dir, agent_id, session_id)]
            if sanitized_session_id
        }
        for agent_id, session_id in session_map.items():
            sanitized_session_id = _sanitize_dispatch_session_id(openclaw_dir, agent_id, session_id)
            if agent_id and sanitized_session_id:
                latest_sessions[agent_id] = sanitized_session_id
        compression = {"applied": False}
        dispatch_message = merged_message
        if not direct_fast_path:
            recent_thread_messages = store_list_chat_messages(openclaw_dir, thread_id=thread_id, limit=512)
            compression = maybe_prepare_chat_thread_context_compression(
                latest_thread,
                recent_thread_messages,
                targeted_dispatch_agent_ids,
                merged_message,
                hosted_provider_context_budget_policy=hosted_provider_context_budget_policy,
                execution_path=primary_execution_path,
            )
            if compression.get("applied"):
                dispatch_message = str(compression.get("message") or "").strip() or merged_message
                for agent_id, session_id in (compression.get("sessionsByAgent") or {}).items():
                    normalized_agent_id = str(agent_id or "").strip()
                    sanitized_session_id = _sanitize_dispatch_session_id(openclaw_dir, normalized_agent_id, session_id)
                    if normalized_agent_id and sanitized_session_id:
                        latest_sessions[normalized_agent_id] = sanitized_session_id
        if strict_all_hands_replies:
            contract = _strict_all_hands_reply_contract(outbound_message.get("body", ""))
            if contract and contract not in dispatch_message:
                dispatch_message = f"{dispatch_message}\n{contract}".strip()
        publish_chat_thread_stream_event(
            server,
            thread_id,
            "dispatch_started",
            messageId=outbound_message.get("id", ""),
            dispatchMode=dispatch_mode,
            targetAgentId=target_agent_id,
            dispatchAgentIds=targeted_dispatch_agent_ids,
        )
        dispatch_successes = []
        dispatch_failures = []
        persisted_dispatch_agents = set()
        failed_dispatch_agents = set()
        wake_attempted_agent_ids = []
        wake_failures = []
        wake_responded_agent_ids = []
        lead_first_enabled = False
        primary_agent_id = ""

        def ensure_runtime_context():
            nonlocal config, metadata, agents
            if config is None:
                config = load_config(openclaw_dir)
            if metadata is None:
                metadata = load_project_metadata(openclaw_dir, config=config)
            if agents is None:
                agents = load_agents(config)
            return config, metadata, agents

        def handle_stream_event(event):
            if not isinstance(event, dict):
                return
            stage = str(event.get("stage") or "").strip()
            if not stage:
                return
            stream_agent_id = str(event.get("agentId") or "").strip()

            # Tool-related events — forward directly to frontend via SSE
            if stage in ("tool_call", "tool_result", "tool_error", "permission_request"):
                publish_chat_thread_stream_event(
                    server,
                    thread_id,
                    stage,
                    messageId=outbound_message.get("id", ""),
                    agentId=stream_agent_id,
                    dispatchMode=dispatch_mode,
                    toolName=str(event.get("toolName") or "").strip(),
                    toolArgs=event.get("toolArgs") if isinstance(event.get("toolArgs"), dict) else {},
                    result=str(event.get("result") or "").strip()[:500],
                    error=str(event.get("error") or "").strip(),
                    requestId=str(event.get("requestId") or "").strip(),
                )
                return

            if stage not in {"started", "delta"}:
                return
            publish_chat_thread_stream_event(
                server,
                thread_id,
                stage,
                messageId=outbound_message.get("id", ""),
                streamId=f"{outbound_message.get('id', '')}:{stream_agent_id}" if stream_agent_id else "",
                agentId=stream_agent_id,
                sessionId=str(event.get("sessionId") or "").strip(),
                dispatchMode=dispatch_mode,
                content=str(event.get("content") or ""),
                delta=str(event.get("delta") or ""),
            )

        def persist_dispatch_failure(dispatch_agent_id, error_message):
            normalized_agent_id = str(dispatch_agent_id or "").strip()
            normalized_error = _humanize_dispatch_error_message(error_message)
            if not normalized_agent_id or normalized_agent_id in failed_dispatch_agents:
                return
            failed_dispatch_agents.add(normalized_agent_id)
            dispatch_failures.append({"agentId": normalized_agent_id, "error": normalized_error})
            store_save_chat_message(
                openclaw_dir,
                {
                    "threadId": thread_id,
                    "senderKind": "system",
                    "senderId": "team-dispatch",
                    "senderLabel": "Team Dispatch",
                    "direction": "system",
                    "body": f"{normalized_agent_id} 暂未回包：{normalized_error}",
                    "meta": {
                        "agentId": normalized_agent_id,
                        "dispatchMode": dispatch_mode,
                        "replyToMessageId": outbound_message.get("id", ""),
                    },
                },
            )
            publish_chat_thread_stream_event(
                server,
                thread_id,
                "failed",
                messageId=outbound_message.get("id", ""),
                streamId=f"{outbound_message.get('id', '')}:{normalized_agent_id}" if normalized_agent_id else "",
                agentId=normalized_agent_id,
                dispatchMode=dispatch_mode,
                error=normalized_error,
            )

        def clear_dispatch_failure(dispatch_agent_id):
            normalized_agent_id = str(dispatch_agent_id or "").strip()
            if not normalized_agent_id or normalized_agent_id not in failed_dispatch_agents:
                return
            failed_dispatch_agents.discard(normalized_agent_id)
            dispatch_failures[:] = [
                item
                for item in dispatch_failures
                if str((item or {}).get("agentId") or "").strip() != normalized_agent_id
            ]

        def persist_dispatch_success(dispatch_agent_id, result_payload, anchor_payload=None, turn_profile=None, extra_meta=None):
            normalized_agent_id = str(dispatch_agent_id or "").strip()
            if not normalized_agent_id or normalized_agent_id in persisted_dispatch_agents:
                return
            ensure_runtime_context()
            result_payload = result_payload if isinstance(result_payload, dict) else {}
            extra_meta = extra_meta if isinstance(extra_meta, dict) else {}
            result_meta = ((result_payload.get("result", {}) or {}).get("meta", {}) or {})
            agent_meta = (result_meta.get("agentMeta", {}) or {}) if isinstance(result_meta, dict) else {}
            actual_session_id = _persisted_dispatch_session_id(
                openclaw_dir,
                normalized_agent_id,
                primary_session_id=agent_meta.get("sessionId"),
                fallback_session_id=latest_sessions.get(normalized_agent_id, ""),
            )
            if actual_session_id:
                latest_sessions[normalized_agent_id] = actual_session_id
            else:
                latest_sessions.pop(normalized_agent_id, None)
            payloads = ((result_payload.get("result", {}) or {}).get("payloads", []) or [])
            reply_preview = payloads[0].get("text", "") if payloads and isinstance(payloads[0], dict) else ""
            dispatch_identity = agent_runtime_identity_payload(
                openclaw_dir,
                normalized_agent_id,
                config=config,
                metadata=metadata,
                agents=agents,
            )
            if strict_all_hands_replies:
                reply_preview = _sanitize_strict_all_hands_reply_text(
                    reply_preview,
                    display_name=dispatch_identity.get("displayName", ""),
                    job_title=dispatch_identity.get("jobTitle", ""),
                )
            response_payload = {
                "agentId": normalized_agent_id,
                "agentLabel": dispatch_identity.get("displayName", "") or normalized_agent_id,
                "sessionId": actual_session_id,
                "replyPreview": reply_preview,
                "agentDisplayName": dispatch_identity.get("displayName", ""),
                "agentHumanName": dispatch_identity.get("humanName", ""),
                "agentJobTitle": dispatch_identity.get("jobTitle", ""),
            }
            response_payload.update(extra_meta)
            dispatch_successes.append(response_payload)
            persisted_dispatch_agents.add(normalized_agent_id)
            # Extract tool activity from query engine results for persistence
            tool_activity = []
            result_events = result_payload.get("events") if isinstance(result_payload.get("events"), list) else []
            for evt in result_events:
                if not isinstance(evt, dict):
                    continue
                evt_type = str(evt.get("type") or "").strip()
                if evt_type == "tool_call":
                    tool_activity.append({
                        "toolName": evt.get("toolName", ""),
                        "toolArgs": evt.get("toolArgs", {}),
                        "status": "completed",
                    })
                elif evt_type == "tool_error":
                    tool_activity.append({
                        "toolName": evt.get("toolName", ""),
                        "status": "error",
                        "error": str(evt.get("error") or "")[:200],
                    })
            tool_stats = result_payload.get("stats") if isinstance(result_payload.get("stats"), dict) else {}

            if reply_preview:
                message_meta = {
                    "sessionId": actual_session_id,
                    "dispatchMode": dispatch_mode,
                    "replyToMessageId": outbound_message.get("id", ""),
                    "agentHumanName": dispatch_identity.get("humanName", ""),
                    "agentJobTitle": dispatch_identity.get("jobTitle", ""),
                    "turnFocus": ((turn_profile or {}).get("turnFocus") or ""),
                    "turnPace": ((turn_profile or {}).get("turnPace") or ""),
                    **(anchor_payload or {}),
                    **extra_meta,
                    **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                    **memory_trace_meta,
                }
                if tool_activity:
                    message_meta["toolActivity"] = tool_activity
                if tool_stats.get("tool_calls"):
                    message_meta["toolStats"] = tool_stats

                saved_message = store_save_chat_message(
                    openclaw_dir,
                    {
                        "threadId": thread_id,
                        "senderKind": "agent",
                        "senderId": normalized_agent_id,
                        "senderLabel": dispatch_identity.get("displayName", "") or normalized_agent_id,
                        "direction": "agent",
                        "body": reply_preview,
                        "meta": message_meta,
                    },
                )
                saved_message = maybe_attach_chat_thread_voice_reply(
                    openclaw_dir,
                    latest_thread,
                    saved_message,
                    request_text=outbound_message.get("body", ""),
                    server=server,
                )
                response_payload["messageId"] = saved_message.get("id", "")
                try:
                    maybe_forward_chat_thread_reply_to_wechat(openclaw_dir, latest_thread, reply_preview)
                except Exception as error:
                    store_save_chat_message(
                        openclaw_dir,
                        {
                            "threadId": thread_id,
                            "senderKind": "system",
                            "senderId": "customer-channel",
                            "senderLabel": "Customer Channel",
                            "direction": "system",
                            "body": f"微信回发失败：{str(error or 'unknown error').strip()}",
                            "meta": {
                                "replyToMessageId": saved_message.get("id", ""),
                                "channel": "wechat_official",
                            },
                        },
                    )
            publish_chat_thread_stream_event(
                server,
                thread_id,
                "completed",
                messageId=outbound_message.get("id", ""),
                streamId=f"{outbound_message.get('id', '')}:{normalized_agent_id}" if normalized_agent_id else "",
                agentId=normalized_agent_id,
                sessionId=actual_session_id,
                dispatchMode=dispatch_mode,
                content=reply_preview,
            )

        def build_dispatch_plan(agent_ids, prior_responses=None):
            ensure_runtime_context()
            planned_agent_ids = [
                str(item or "").strip()
                for item in safe_list(agent_ids)
                if str(item or "").strip()
            ]
            turn_anchors = {
                dispatch_agent_id: build_human_turn_anchor_payload(
                    openclaw_dir,
                    dispatch_agent_id,
                    turn_index=index,
                    ordered_agent_ids=planned_agent_ids,
                    prior_responses=prior_responses if isinstance(prior_responses, list) else [],
                    lead_agent_id=target_agent_id,
                    config=config,
                    metadata=metadata,
                    agents=agents,
                )
                for index, dispatch_agent_id in enumerate(planned_agent_ids)
            }
            turn_profiles = {
                dispatch_agent_id: build_human_turn_profile_payload(
                    openclaw_dir,
                    dispatch_agent_id,
                    turn_index=index,
                    participant_count=len(planned_agent_ids),
                    lead_agent_id=target_agent_id,
                    prior_responses=prior_responses if isinstance(prior_responses, list) else [],
                    config=config,
                    metadata=metadata,
                    agents=agents,
                )
                for index, dispatch_agent_id in enumerate(planned_agent_ids)
            }
            dispatch_requests = apply_conversation_fanout_stagger(
                [
                    {
                        "agentId": dispatch_agent_id,
                        "sessionId": str(latest_sessions.get(dispatch_agent_id, "") or "").strip(),
                        "message": apply_turn_guidance_to_message(
                            dispatch_message,
                            turn_index=index,
                            participant_count=len(planned_agent_ids),
                            anchor=turn_anchors.get(dispatch_agent_id),
                            turn_profile=turn_profiles.get(dispatch_agent_id),
                        ),
                        "thinking": thinking_value,
                        "selectedSkillSlugs": selected_skill_slugs,
                    }
                    for index, dispatch_agent_id in enumerate(planned_agent_ids)
                ],
                stagger_seconds=TEAM_CONVERSATION_STAGGER_SECONDS if len(planned_agent_ids) > 1 else 0.0,
            )
            return dispatch_requests, turn_anchors, turn_profiles

        if direct_fast_path and targeted_dispatch_agent_ids:
            direct_agent_id = targeted_dispatch_agent_ids[0]
            use_tools = _is_tool_enabled_agent(openclaw_dir, direct_agent_id, config=config)
            logger.info("[dispatch] direct_agent=%s use_tools=%s", direct_agent_id, use_tools)
            ensure_runtime_context()
            dispatch_identity = agent_runtime_identity_payload(
                openclaw_dir, direct_agent_id,
                config=config, metadata=metadata, agents=agents,
            )
            # Resolve agent's configured model and workspace
            agent_model = ""
            agent_workspace = ""
            for _ag in safe_list(agents):
                if isinstance(_ag, dict) and str(_ag.get("id") or "") == direct_agent_id:
                    agent_model = str(_ag.get("model") or "").strip()
                    agent_workspace = str(_ag.get("workspace") or "").strip()
                    break
            resolved_workspace = str(latest_meta.get("workspacePath") or agent_workspace or openclaw_dir).strip()
            resolved_workspace_authorized = bool(latest_meta.get("workspaceAuthorized"))
            standard_error = None
            workspace_first = bool(resolved_workspace and resolved_workspace_authorized)
            if workspace_first:
                logger.info(
                    "[dispatch] workspace-aware direct path enabled: agent=%s model=%s workspace=%s authorized=%s",
                    direct_agent_id,
                    agent_model,
                    resolved_workspace,
                    resolved_workspace_authorized,
                )
                try:
                    direct_result = _perform_tool_enabled_dispatch(
                        openclaw_dir,
                        agent_id=direct_agent_id,
                        message=dispatch_message,
                        thread_id=thread_id,
                        workspace_path=resolved_workspace,
                        stream_observer=handle_stream_event,
                        agent_display_name=dispatch_identity.get("displayName", ""),
                        agent_job_title=dispatch_identity.get("jobTitle", ""),
                        thinking=thinking_value,
                        model=agent_model,
                        workspace_authorized=resolved_workspace_authorized,
                    )
                except Exception as error:
                    standard_error = error
                    logger.warning("[dispatch] workspace-aware dispatch failed for %s: %s", direct_agent_id, error)
                else:
                    persist_dispatch_success(direct_agent_id, direct_result)
                if standard_error is not None:
                    try:
                        direct_result = perform_conversation_send(
                            openclaw_dir,
                            agent_id=direct_agent_id,
                            session_id=str(latest_sessions.get(direct_agent_id, "") or "").strip(),
                            message=dispatch_message,
                            thinking=thinking_value,
                            agent_timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
                            stream_observer=handle_stream_event,
                            selected_skill_slugs=clean_unique_strings(selected_skill_slugs or []),
                        )
                    except Exception as error:
                        standard_error = error
                        logger.warning("[dispatch] standard direct fallback failed for %s: %s", direct_agent_id, error)
                    else:
                        persist_dispatch_success(direct_agent_id, direct_result)
            else:
                try:
                    direct_result = perform_conversation_send(
                        openclaw_dir,
                        agent_id=direct_agent_id,
                        session_id=str(latest_sessions.get(direct_agent_id, "") or "").strip(),
                        message=dispatch_message,
                        thinking=thinking_value,
                        agent_timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
                        stream_observer=handle_stream_event,
                        selected_skill_slugs=clean_unique_strings(selected_skill_slugs or []),
                    )
                except Exception as error:
                    standard_error = error
                    logger.warning("[dispatch] standard direct dispatch failed for %s: %s", direct_agent_id, error)
                else:
                    persist_dispatch_success(direct_agent_id, direct_result)
                if standard_error is not None and use_tools:
                    # ── Tool-enabled dispatch fallback path ──
                    logger.info("[dispatch] tool-enabled fallback: agent=%s model=%s workspace=%s authorized=%s", direct_agent_id, agent_model, resolved_workspace, resolved_workspace_authorized)
                    try:
                        direct_result = _perform_tool_enabled_dispatch(
                            openclaw_dir,
                            agent_id=direct_agent_id,
                            message=dispatch_message,
                            thread_id=thread_id,
                            workspace_path=resolved_workspace,
                            stream_observer=handle_stream_event,
                            agent_display_name=dispatch_identity.get("displayName", ""),
                            agent_job_title=dispatch_identity.get("jobTitle", ""),
                            thinking=thinking_value,
                            model=agent_model,
                            workspace_authorized=resolved_workspace_authorized,
                        )
                    except Exception as error:
                        logger.error("[dispatch] tool-enabled dispatch failed for %s: %s", direct_agent_id, error, exc_info=True)
                        persist_dispatch_failure(direct_agent_id, str(error or "工具调用失败。").strip())
                    else:
                        persist_dispatch_success(direct_agent_id, direct_result)
                elif standard_error is not None:
                    persist_dispatch_failure(direct_agent_id, str(standard_error or "会话发送失败。").strip())
        else:
            lead_first_enabled = (
                dispatch_mode == "broadcast"
                and len(ordered_dispatch_agent_ids) > 1
                and not strict_all_hands_replies
            )
            primary_agent_id = (
                target_agent_id
                if lead_first_enabled and target_agent_id in ordered_dispatch_agent_ids
                else (ordered_dispatch_agent_ids[0] if ordered_dispatch_agent_ids else "")
            )
            remaining_dispatch_agent_ids = list(ordered_dispatch_agent_ids)
            if lead_first_enabled and primary_agent_id:
                primary_requests, primary_anchors, primary_profiles = build_dispatch_plan([primary_agent_id])
                if primary_requests:
                    primary_request = primary_requests[0]
                    try:
                        primary_result = perform_conversation_send(
                            openclaw_dir,
                            agent_id=primary_request["agentId"],
                            session_id=primary_request["sessionId"],
                            message=primary_request["message"],
                            thinking=primary_request["thinking"],
                            agent_timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
                            stream_observer=handle_stream_event,
                            selected_skill_slugs=clean_unique_strings(primary_request.get("selectedSkillSlugs") or []),
                        )
                    except Exception as error:
                        persist_dispatch_failure(primary_agent_id, str(error or "会话发送失败。").strip())
                    else:
                        persist_dispatch_success(
                            primary_agent_id,
                            primary_result,
                            anchor_payload=primary_anchors.get(primary_agent_id),
                            turn_profile=primary_profiles.get(primary_agent_id),
                        )
                remaining_dispatch_agent_ids = [
                    item for item in ordered_dispatch_agent_ids if str(item or "").strip() and str(item or "").strip() != primary_agent_id
                ]

            dispatch_requests, dispatch_turn_anchors, dispatch_turn_profiles = build_dispatch_plan(
                remaining_dispatch_agent_ids if lead_first_enabled else ordered_dispatch_agent_ids,
                prior_responses=dispatch_successes,
            )
            if dispatch_requests:
                perform_conversation_fanout(
                    openclaw_dir,
                    dispatch_requests,
                    default_thinking=thinking_value,
                    stream_callback=handle_stream_event,
                    max_workers=TEAM_CONVERSATION_MAX_PARALLEL if len(dispatch_requests) > 1 else None,
                    result_callback=lambda event: (
                        persist_dispatch_success(
                            str(event.get("agentId") or "").strip(),
                            event.get("result"),
                            anchor_payload=dispatch_turn_anchors.get(str(event.get("agentId") or "").strip()),
                            turn_profile=dispatch_turn_profiles.get(str(event.get("agentId") or "").strip()),
                        )
                        if bool(event.get("ok"))
                        else persist_dispatch_failure(
                            str(event.get("agentId") or "").strip(),
                            str(event.get("error") or "会话发送失败。").strip(),
                        )
                    ),
                )
        missing_dispatch_agent_ids = [
            agent_id
            for agent_id in targeted_dispatch_agent_ids
            if agent_id
            and agent_id not in {str(item.get("agentId") or "").strip() for item in dispatch_successes if isinstance(item, dict)}
        ]
        if team_id and dispatch_mode in {"broadcast", "mentions"} and missing_dispatch_agent_ids:
            wake_attempted_agent_ids = list(missing_dispatch_agent_ids)
            wake_message = _team_thread_wake_message(
                thread_title=str(latest_thread.get("title") or thread_id).strip(),
                team_name=str(latest_meta.get("teamName") or "").strip(),
            )
            store_save_chat_message(
                openclaw_dir,
                {
                    "threadId": thread_id,
                    "senderKind": "system",
                    "senderId": "team-wake",
                    "senderLabel": "Team Wake",
                    "direction": "system",
                    "body": f"以下成员暂未回包，已主动叫起：{'、'.join(wake_attempted_agent_ids)}",
                    "meta": {
                        "replyToMessageId": outbound_message.get("id", ""),
                        "dispatchMode": dispatch_mode,
                        "wakeAttemptedAgentIds": wake_attempted_agent_ids,
                    },
                },
            )
            wake_requests = [
                {
                    "agentId": dispatch_agent_id,
                    "sessionId": str(latest_sessions.get(dispatch_agent_id, "") or "").strip(),
                    "message": wake_message,
                    "thinking": "low",
                }
                for dispatch_agent_id in wake_attempted_agent_ids
            ]
            perform_conversation_fanout(
                openclaw_dir,
                wake_requests,
                default_thinking="low",
                timeout_seconds=18,
                stream_callback=handle_stream_event,
                max_workers=TEAM_CONVERSATION_MAX_PARALLEL if len(wake_requests) > 1 else None,
                result_callback=lambda event: (
                    (
                        clear_dispatch_failure(str(event.get("agentId") or "").strip()),
                        wake_responded_agent_ids.append(str(event.get("agentId") or "").strip()),
                        persist_dispatch_success(
                            str(event.get("agentId") or "").strip(),
                            event.get("result"),
                            extra_meta={
                                "wakeFallback": True,
                                "wakeSourceMessageId": outbound_message.get("id", ""),
                            },
                        ),
                    )
                    if bool(event.get("ok"))
                    else wake_failures.append(
                        {
                            "agentId": str(event.get("agentId") or "").strip(),
                            "error": str(event.get("error") or "唤醒失败。").strip(),
                        }
                    )
                ),
            )
        dispatch_summary = {
            "mode": dispatch_mode,
            "requestedTargetAgentId": target_agent_id,
            "leadFirst": bool(lead_first_enabled and primary_agent_id),
            "strictAllHandsReplies": strict_all_hands_replies,
            "primaryAgentId": primary_agent_id,
            "requestedDispatchAgentIds": dispatch_agent_ids,
            "targetedAgentIds": targeted_dispatch_agent_ids,
            "dispatchAgentIds": targeted_dispatch_agent_ids,
            "participantAgentIds": participant_agent_ids,
            "respondedAgentIds": [item.get("agentId") for item in dispatch_successes if item.get("agentId")],
            "responses": deepcopy(dispatch_successes),
            "failedAgents": deepcopy(dispatch_failures),
            "replyCount": len(dispatch_successes),
            "at": now_iso(),
            "contextCompressed": bool(compression.get("applied")),
        }
        if wake_attempted_agent_ids:
            dispatch_summary["wakeSummary"] = {
                "attemptedAgentIds": wake_attempted_agent_ids,
                "respondedAgentIds": clean_unique_strings(wake_responded_agent_ids),
                "failedAgents": wake_failures,
                "replyCount": len(clean_unique_strings(wake_responded_agent_ids)),
                "at": now_iso(),
            }
        if compression.get("applied"):
            dispatch_summary["contextCompression"] = {
                "reason": str(compression.get("reason") or "large_thread_context").strip(),
                "rotationCount": int(((compression.get("meta") or {}).get("rotationCount")) or 0),
                "compressedMessageCount": int(((compression.get("meta") or {}).get("compressedMessageCount")) or 0),
                "recentMessageCount": int(((compression.get("meta") or {}).get("recentMessageCount")) or 0),
            }
        coordination_relay = {}
        if dispatch_mode == "broadcast" and len(participant_agent_ids) > 1:
            if len(participant_agent_ids) > 2:
                coordination_relay = schedule_chat_thread_coordination_relay(
                    openclaw_dir,
                    thread_id,
                    team_id=team_id,
                    participant_agent_ids=participant_agent_ids,
                    sessions_by_agent=latest_sessions,
                    responses=dispatch_successes,
                    context_label=str(latest_thread.get("title") or thread_id).strip(),
                    thinking=thinking_value,
                    output_dir=output_dir,
                )
            else:
                coordination_relay = _svc().relay_team_coordination_updates(
                    openclaw_dir,
                    latest_thread,
                    team=next(
                        (
                            item
                            for item in store_list_agent_teams(openclaw_dir)
                            if str(item.get("id") or "").strip() == team_id
                        ),
                        {},
                    ),
                    participant_agent_ids=participant_agent_ids,
                    sessions_by_agent=latest_sessions,
                    responses=dispatch_successes,
                    context_label=str(latest_thread.get("title") or thread_id).strip(),
                    thinking=thinking_value,
                    timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
                    sender_label="Team Coordination",
                )
        if coordination_relay.get("sent") or coordination_relay.get("scheduled"):
            dispatch_summary["coordinationRelay"] = coordination_relay
        dispatch_summary["collaboration"] = build_team_collaboration_summary(dispatch_summary)
        next_team_policy = apply_team_working_memory(safe_policy, dispatch_summary)
        remaining_agent_ids = [
            agent_id
            for agent_id in targeted_dispatch_agent_ids
            if agent_id
            and agent_id not in {item.get("agentId") for item in dispatch_successes if item.get("agentId")}
            and agent_id not in {item.get("agentId") for item in dispatch_failures if item.get("agentId")}
        ]
        if dispatch_successes:
            final_thread_status = "waiting_internal" if remaining_agent_ids else "open"
        elif targeted_dispatch_agent_ids:
            final_thread_status = "blocked" if dispatch_failures else "waiting_internal"
        else:
            final_thread_status = "blocked"
        store_save_chat_message(
            openclaw_dir,
            {
                "id": outbound_message.get("id", ""),
                "threadId": thread_id,
                "senderKind": outbound_message.get("senderKind", "user"),
                "senderId": outbound_message.get("senderId", ""),
                "senderLabel": outbound_message.get("senderLabel", ""),
                "direction": "outbound",
                "body": outbound_message.get("body", ""),
                "createdAt": outbound_message.get("createdAt", ""),
                "meta": {
                    "targetAgentId": target_agent_id,
                    "dispatchMode": dispatch_mode,
                    "dispatchAgentIds": targeted_dispatch_agent_ids,
                    "requestedDispatchAgentIds": dispatch_agent_ids,
                    "dispatchSummary": dispatch_summary,
                    "attachments": safe_attachments,
                    "mentionAgentIds": safe_mentions,
                    "teamContext": {
                        "hasOperatingBrief": bool(str(safe_policy.get("operatingBrief") or "").strip()),
                        "hasTeamMemory": bool(str(safe_policy.get("teamMemory") or "").strip()),
                        "hasDecisionLog": bool(str(safe_policy.get("decisionLog") or "").strip()),
                        "hasCurrentFocus": bool(str(next_team_policy.get("currentFocus") or "").strip()),
                        "hasOpenLoops": bool(normalize_team_context_lines(next_team_policy.get("openLoops"), limit=1)),
                    },
                },
            },
        )
        store_save_chat_thread(
            openclaw_dir,
            {
                **latest_thread,
                "currentTargetAgentId": target_agent_id,
                "participantAgentIds": participant_agent_ids,
                "meta": {
                    **latest_meta,
                    "dispatchMode": dispatch_mode,
                    "lastDispatch": dispatch_summary,
                    "sessionsByAgent": latest_sessions,
                    "teamPolicy": next_team_policy,
                    "contextCompression": (
                        deepcopy(compression.get("meta"))
                        if compression.get("applied") and isinstance(compression.get("meta"), dict)
                        else latest_meta.get("contextCompression", {})
                    ),
                    "coordinationRelay": (
                        coordination_relay
                        if coordination_relay.get("sent") or coordination_relay.get("scheduled")
                        else latest_meta.get("coordinationRelay", {})
                    ),
                },
                "status": "waiting_internal" if dispatch_successes or targeted_dispatch_agent_ids else "blocked",
                "updatedAt": now_iso(),
            },
        )
        latest_thread = store_save_chat_thread(
            openclaw_dir,
            {
                **latest_thread,
                "currentTargetAgentId": target_agent_id,
                "participantAgentIds": participant_agent_ids,
                "meta": {
                    **latest_meta,
                    "dispatchMode": dispatch_mode,
                    "lastDispatch": dispatch_summary,
                    "sessionsByAgent": latest_sessions,
                    "teamPolicy": next_team_policy,
                    "contextCompression": (
                        deepcopy(compression.get("meta"))
                        if compression.get("applied") and isinstance(compression.get("meta"), dict)
                        else latest_meta.get("contextCompression", {})
                    ),
                    "coordinationRelay": (
                        coordination_relay
                        if coordination_relay.get("sent") or coordination_relay.get("scheduled")
                        else latest_meta.get("coordinationRelay", {})
                    ),
                },
                "status": final_thread_status,
                "updatedAt": now_iso(),
            },
        )
        memory_records, memory_events = build_memory_projection_payloads(
            task_id=str(latest_thread.get("linkedTaskId") or "").strip(),
            thread_id=thread_id,
            task_title=str(latest_thread.get("title") or "").strip(),
            team_id=team_id,
            team_name=str((latest_meta.get("teamName") or "").strip() or ""),
            task_memory=next_team_policy.get("taskLongTermMemory"),
            team_policy=next_team_policy,
            dispatch_state=dispatch_summary,
        )
        project_memory_records_async(openclaw_dir, memory_records, memory_events)
        publish_chat_thread_stream_event(
            server,
            thread_id,
            "dispatch_completed",
            messageId=outbound_message.get("id", ""),
            dispatchMode=dispatch_mode,
            replyCount=len(dispatch_successes),
            respondedAgentIds=[item.get("agentId") for item in dispatch_successes if item.get("agentId")],
            failedAgents=dispatch_failures,
        )
        invalidate_dashboard_bundle_cache(openclaw_dir, output_dir)

    if float(defer_seconds or 0.0) > 0:
        submit_task_execution_background_deferred(worker, delay_seconds=defer_seconds)
    else:
        submit_task_execution_background(worker)
    return {
        "scheduled": True,
        "mode": dispatch_mode,
        "requestedTargetAgentId": target_agent_id,
        "dispatchAgentIds": dispatch_agent_ids,
        "respondedAgentIds": [],
        "failedAgents": [],
        "replyCount": 0,
        "at": now_iso(),
    }


def dispatch_task_execution_team_members(
    openclaw_dir,
    task_id,
    title,
    remark="",
    lead_agent_id="",
    workflow_binding=None,
    team=None,
    router_agent_id="",
    lead_result=None,
    linked_run_id="",
    linked_run_title="",
    auto_operation_profile=None,
    auto_operation_runtime=None,
    task_long_term_memory=None,
):
    team = team if isinstance(team, dict) else {}
    team_id = str(team.get("id") or "").strip()
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    auto_operation_runtime = compact_company_auto_operation_runtime(auto_operation_runtime)
    existing_task = next((item for item in merge_tasks(openclaw_dir, config) if item.get("id") == task_id), {})
    existing_route = task_route_meta_payload(existing_task)
    task_long_term_memory = compact_task_long_term_memory(task_long_term_memory)
    if not task_long_term_memory.get("longTermMemory"):
        task_long_term_memory = compact_task_long_term_memory(existing_route.get("taskLongTermMemory"))
    lead_id = str(lead_agent_id or "").strip()
    base_participants = order_agent_ids_for_human_turns(
        openclaw_dir,
        task_team_participant_agent_ids(team, lead_agent_id=lead_agent_id),
        lead_agent_id=lead_agent_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    discussion_plan = task_internal_discussion_plan(
        openclaw_dir,
        team,
        lead_agent_id=lead_agent_id,
        participant_agent_ids=base_participants,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    participants = discussion_plan.get("participantAgentIds") or base_participants
    discussion_agent_ids = [str(item or "").strip() for item in safe_list(discussion_plan.get("discussionAgentIds")) if str(item or "").strip()]
    discussion_advisor_ids = [str(item or "").strip() for item in safe_list(discussion_plan.get("advisorAgentIds")) if str(item or "").strip()]
    execution_agent_ids = [str(item or "").strip() for item in safe_list(discussion_plan.get("executionAgentIds")) if str(item or "").strip()]
    if not discussion_plan.get("enabled"):
        execution_agent_ids = [agent_id for agent_id in participants if agent_id and agent_id != lead_id]
    if not team_id or not participants:
        return {}

    lead_identity = agent_runtime_identity_payload(
        openclaw_dir,
        lead_agent_id,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    thread = ensure_task_execution_team_thread(
        openclaw_dir,
        task_id,
        title,
        team,
        lead_agent_id=lead_agent_id,
        linked_run_id=linked_run_id,
        extra_participant_agent_ids=discussion_plan.get("extraParticipantAgentIds"),
    )
    thread_meta = deepcopy(thread.get("meta", {})) if isinstance(thread.get("meta"), dict) else {}
    team_policy = merge_team_policy_state(team, thread_meta.get("teamPolicy"))
    if not task_long_term_memory.get("longTermMemory"):
        task_long_term_memory = compact_task_long_term_memory(
            (thread_meta.get("teamPolicy") or {}).get("taskLongTermMemory")
            if isinstance(thread_meta.get("teamPolicy"), dict)
            else {}
        )
    if task_long_term_memory.get("longTermMemory") and not compact_task_long_term_memory(team_policy.get("taskLongTermMemory")).get("longTermMemory"):
        team_policy["taskLongTermMemory"] = task_long_term_memory
    memory_trace_meta = team_memory_trace_payload(team_policy)
    state_packet_meta = team_state_packet_payload(team_policy)
    sessions_by_agent = thread_meta.get("sessionsByAgent") if isinstance(thread_meta.get("sessionsByAgent"), dict) else {}
    responses = []
    failures = []
    discussion_outbound_message = {}
    execution_outbound_message = {}

    def save_stage_failure(agent_id, error_message, reply_to_message_id="", role="member", sender_id="task-dispatch", sender_label="Task Dispatch"):
        failures.append({"agentId": agent_id, "error": error_message})
        store_save_chat_message(
            openclaw_dir,
            {
                "threadId": thread.get("id", ""),
                "senderKind": "system",
                "senderId": sender_id,
                "senderLabel": sender_label,
                "direction": "system",
                "body": f"{agent_id} 暂未回包：{error_message}",
                "meta": {
                    "agentId": agent_id,
                    "replyToMessageId": reply_to_message_id,
                    "taskDispatchRole": role,
                    **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                },
            },
        )

    def save_agent_reply(agent_id, actual_session_id, reply_preview, reply_to_message_id="", role="member", turn_profiles=None, turn_anchors=None):
        turn_profiles = turn_profiles if isinstance(turn_profiles, dict) else {}
        turn_anchors = turn_anchors if isinstance(turn_anchors, dict) else {}
        identity = lead_identity if str(agent_id or "").strip() == lead_id else agent_runtime_identity_payload(
            openclaw_dir,
            agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        response_payload = {
            "agentId": str(agent_id or "").strip(),
            "sessionId": str(actual_session_id or "main").strip() or "main",
            "replyPreview": reply_preview,
            "source": role,
            "agentDisplayName": identity.get("displayName", ""),
            "agentHumanName": identity.get("humanName", ""),
            "agentJobTitle": identity.get("jobTitle", ""),
        }
        responses.append(response_payload)
        if reply_preview:
            saved_message = store_save_chat_message(
                openclaw_dir,
                {
                    "threadId": thread.get("id", ""),
                    "senderKind": "agent",
                    "senderId": str(agent_id or "").strip(),
                    "senderLabel": identity.get("displayName", "") or str(agent_id or "").strip(),
                    "direction": "agent",
                    "body": reply_preview,
                    "meta": {
                        "sessionId": str(actual_session_id or "main").strip() or "main",
                        "replyToMessageId": reply_to_message_id,
                        "taskDispatchRole": role,
                        "agentHumanName": identity.get("humanName", ""),
                        "agentJobTitle": identity.get("jobTitle", ""),
                        "turnFocus": (turn_profiles.get(agent_id) or {}).get("turnFocus", ""),
                        "turnPace": (turn_profiles.get(agent_id) or {}).get("turnPace", ""),
                        **(turn_anchors.get(agent_id) or {}),
                        **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                        **memory_trace_meta,
                    },
                },
            )
            response_payload["messageId"] = saved_message.get("id", "")
        return response_payload

    lead_reply_preview = summarize_task_execution_text(conversation_reply_preview(lead_result), limit=320)
    lead_session_id = ""
    if isinstance(lead_result, dict):
        lead_agent_meta = (((lead_result.get("result", {}) or {}).get("meta", {}) or {}).get("agentMeta", {}) or {})
        lead_session_id = str(lead_agent_meta.get("sessionId") or "").strip()
    if lead_session_id:
        sessions_by_agent[lead_id] = lead_session_id
    if lead_reply_preview:
        save_agent_reply(
            lead_id,
            lead_session_id or "main",
            lead_reply_preview,
            reply_to_message_id="",
            role="lead",
        )

    discussion_summary_text = ""
    if discussion_plan.get("enabled"):
        try:
            discussion_outbound_message = store_save_chat_message(
                openclaw_dir,
                {
                    "threadId": thread.get("id", ""),
                    "senderKind": "system",
                    "senderId": "task-discussion",
                    "senderLabel": "Task Discussion",
                    "direction": "system",
                    "body": build_task_internal_discussion_message(
                        task_id,
                        title,
                        remark=remark,
                        team=team,
                        lead_agent_id=lead_agent_id,
                        discussion_agent_ids=discussion_agent_ids,
                        linked_run_id=linked_run_id,
                        linked_run_title=linked_run_title,
                        lead_display_name=lead_identity.get("displayName", ""),
                        auto_operation_profile=auto_operation_profile,
                        auto_operation_runtime=auto_operation_runtime,
                        task_long_term_memory=task_long_term_memory,
                    ),
                    "meta": {
                        "taskId": task_id,
                        "teamId": team_id,
                        "leadAgentId": lead_agent_id,
                        "discussionAgentIds": discussion_agent_ids,
                        "taskDispatchStage": "internal_discussion",
                        **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                    },
                },
            )
        except Exception as discussion_error:
            logging.warning("dispatch_task_execution_team_members: failed to save discussion message for task %s: %s", task_id, discussion_error)
            discussion_outbound_message = {}

        discussion_turn_anchors = {
            agent_id: build_human_turn_anchor_payload(
                openclaw_dir,
                agent_id,
                turn_index=index + 1,
                ordered_agent_ids=discussion_agent_ids,
                prior_responses=responses,
                lead_agent_id=lead_agent_id,
                config=config,
                metadata=metadata,
                agents=agents,
            )
            for index, agent_id in enumerate(discussion_advisor_ids)
        }
        discussion_turn_profiles = {
            agent_id: build_human_turn_profile_payload(
                openclaw_dir,
                agent_id,
                turn_index=index + 1,
                participant_count=len(discussion_agent_ids),
                lead_agent_id=lead_agent_id,
                prior_responses=responses,
                config=config,
                metadata=metadata,
                agents=agents,
            )
            for index, agent_id in enumerate(discussion_advisor_ids)
        }
        discussion_requests = apply_conversation_fanout_stagger(
            [
                {
                    "agentId": agent_id,
                    "sessionId": str(sessions_by_agent.get(agent_id, "") or "").strip(),
                    "message": apply_turn_guidance_to_message(
                        build_task_internal_discussion_message(
                            task_id,
                            title,
                            remark=remark,
                            team=team,
                            lead_agent_id=lead_agent_id,
                            discussion_agent_ids=discussion_agent_ids,
                            linked_run_id=linked_run_id,
                            linked_run_title=linked_run_title,
                            lead_display_name=lead_identity.get("displayName", ""),
                            auto_operation_profile=auto_operation_profile,
                            auto_operation_runtime=auto_operation_runtime,
                            task_long_term_memory=task_long_term_memory,
                        ),
                        turn_index=index + 1,
                        participant_count=len(discussion_agent_ids),
                        anchor=discussion_turn_anchors.get(agent_id),
                        turn_profile=discussion_turn_profiles.get(agent_id),
                    ),
                    "thinking": "low",
                }
                for index, agent_id in enumerate(discussion_advisor_ids)
            ],
            stagger_seconds=TEAM_CONVERSATION_STAGGER_SECONDS,
        )
        discussion_results = perform_conversation_fanout(
            openclaw_dir,
            discussion_requests,
            default_thinking="low",
            timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
            max_workers=TEAM_CONVERSATION_MAX_PARALLEL,
        )
        for failure in discussion_results["failures"]:
            save_stage_failure(
                str(failure.get("agentId") or "").strip(),
                str(failure.get("error") or "成员派发失败。").strip(),
                reply_to_message_id=discussion_outbound_message.get("id", ""),
                role="discussion",
                sender_id="task-discussion",
                sender_label="Task Discussion",
            )
        for success in discussion_results["successes"]:
            agent_id = str(success.get("agentId") or "").strip()
            actual_session_id = _persisted_dispatch_session_id(
                openclaw_dir,
                agent_id,
                primary_session_id=success.get("sessionId"),
                fallback_session_id=sessions_by_agent.get(agent_id, ""),
            )
            if actual_session_id:
                sessions_by_agent[agent_id] = actual_session_id
            else:
                sessions_by_agent.pop(agent_id, None)
            reply_preview = summarize_task_execution_text(conversation_reply_preview(success.get("result")), limit=320)
            save_agent_reply(
                agent_id,
                actual_session_id,
                reply_preview,
                reply_to_message_id=discussion_outbound_message.get("id", ""),
                role="discussion",
                turn_profiles=discussion_turn_profiles,
                turn_anchors=discussion_turn_anchors,
            )
        discussion_summary_text = summarize_internal_discussion_context(responses, limit=4)

    execution_turn_anchors = {
        agent_id: build_human_turn_anchor_payload(
            openclaw_dir,
            agent_id,
            turn_index=index + 1,
            ordered_agent_ids=participants,
            prior_responses=responses,
            lead_agent_id=lead_agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        for index, agent_id in enumerate(execution_agent_ids)
    }
    execution_turn_profiles = {
        agent_id: build_human_turn_profile_payload(
            openclaw_dir,
            agent_id,
            turn_index=index + 1,
            participant_count=len(participants),
            lead_agent_id=lead_agent_id,
            prior_responses=responses,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        for index, agent_id in enumerate(execution_agent_ids)
    }
    if execution_agent_ids:
        try:
            execution_outbound_message = store_save_chat_message(
                openclaw_dir,
                {
                    "threadId": thread.get("id", ""),
                    "senderKind": "system",
                    "senderId": "task-dispatch",
                    "senderLabel": "Task Dispatch",
                    "direction": "system",
                    "body": build_task_team_fanout_message(
                        task_id,
                        title,
                        remark=remark,
                        workflow_binding=workflow_binding,
                        team=team,
                        lead_agent_id=lead_agent_id,
                        member_agent_ids=execution_agent_ids,
                        linked_run_id=linked_run_id,
                        linked_run_title=linked_run_title,
                        lead_display_name=lead_identity.get("displayName", ""),
                        discussion_summary=discussion_summary_text,
                        auto_operation_profile=auto_operation_profile,
                        auto_operation_runtime=auto_operation_runtime,
                        task_long_term_memory=task_long_term_memory,
                    ),
                    "meta": {
                        "taskId": task_id,
                        "teamId": team_id,
                        "leadAgentId": lead_agent_id,
                        "dispatchAgentIds": execution_agent_ids,
                        "taskDispatchStage": "execution",
                        **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                    },
                },
            )
        except Exception as dispatch_error:
            logging.warning("dispatch_task_execution_team_members: failed to save outbound message for task %s: %s", task_id, dispatch_error)
            execution_outbound_message = {}

        member_requests = apply_conversation_fanout_stagger(
            [
                {
                    "agentId": agent_id,
                    "sessionId": str(sessions_by_agent.get(agent_id, "") or "").strip(),
                    "message": apply_turn_guidance_to_message(
                        build_task_team_member_message(
                            task_id,
                            title,
                            agent_id,
                            lead_agent_id=lead_agent_id,
                            remark=remark,
                            workflow_binding=workflow_binding,
                            team=team,
                            linked_run_id=linked_run_id,
                            linked_run_title=linked_run_title,
                            lead_display_name=lead_identity.get("displayName", ""),
                            lead_reply_preview=lead_reply_preview,
                            discussion_summary=discussion_summary_text,
                            auto_operation_profile=auto_operation_profile,
                            auto_operation_runtime=auto_operation_runtime,
                            task_long_term_memory=task_long_term_memory,
                        ),
                        turn_index=index + 1,
                        participant_count=len(participants),
                        anchor=execution_turn_anchors.get(agent_id),
                        turn_profile=execution_turn_profiles.get(agent_id),
                    ),
                    "thinking": "low",
                }
                for index, agent_id in enumerate(execution_agent_ids)
            ],
            stagger_seconds=TEAM_CONVERSATION_STAGGER_SECONDS,
        )
        member_results = perform_conversation_fanout(
            openclaw_dir,
            member_requests,
            default_thinking="low",
            timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
            max_workers=TEAM_CONVERSATION_MAX_PARALLEL,
        )
        for failure in member_results["failures"]:
            save_stage_failure(
                str(failure.get("agentId") or "").strip(),
                str(failure.get("error") or "成员派发失败。").strip(),
                reply_to_message_id=execution_outbound_message.get("id", ""),
                role="member",
                sender_id="task-dispatch",
                sender_label="Task Dispatch",
            )
        for success in member_results["successes"]:
            agent_id = str(success.get("agentId") or "").strip()
            actual_session_id = _persisted_dispatch_session_id(
                openclaw_dir,
                agent_id,
                primary_session_id=success.get("sessionId"),
                fallback_session_id=sessions_by_agent.get(agent_id, ""),
            )
            if actual_session_id:
                sessions_by_agent[agent_id] = actual_session_id
            else:
                sessions_by_agent.pop(agent_id, None)
            reply_preview = summarize_task_execution_text(conversation_reply_preview(success.get("result")), limit=320)
            save_agent_reply(
                agent_id,
                actual_session_id,
                reply_preview,
                reply_to_message_id=execution_outbound_message.get("id", ""),
                role="member",
                turn_profiles=execution_turn_profiles,
                turn_anchors=execution_turn_anchors,
            )

    responded_agent_ids = [item.get("agentId") for item in responses if item.get("agentId")]
    if discussion_plan.get("enabled"):
        summary_text = (
            f"已先在 Team {str(team.get('name') or team_id).strip()} 内拉产品/运营做内部讨论"
            + (f"（{'、'.join(discussion_advisor_ids)}）" if discussion_advisor_ids else "")
            + (f"，再通知执行成员：{'、'.join(execution_agent_ids)}" if execution_agent_ids else "，本轮先由讨论成员收口")
            + f"；当前已回执 {len(responded_agent_ids)}/{len(participants)} 人。"
        )
    else:
        summary_text = (
            f"已向 Team {str(team.get('name') or team_id).strip()} 成员发出协作指令："
            + ("、".join(execution_agent_ids) if execution_agent_ids else "仅 lead 接单")
            + f"；当前已回执 {len(responded_agent_ids)}/{len(participants)} 人。"
        )

    dispatch_summary = {
        "at": now_iso(),
        "taskTitle": title,
        "teamId": team_id,
        "teamName": str(team.get("name") or "").strip(),
        "threadId": thread.get("id", ""),
        "leadAgentId": str(lead_agent_id or "").strip(),
        "participantAgentIds": participants,
        "dispatchAgentIds": [*discussion_advisor_ids, *execution_agent_ids],
        "respondedAgentIds": responded_agent_ids,
        "responses": responses,
        "failedAgents": failures,
        "replyCount": len(responses),
        "summaryText": summary_text,
        "coordinationProtocol": task_coordination_protocol_snapshot(team),
        "internalDiscussion": {
            "enabled": bool(discussion_plan.get("enabled")),
            "discussionAgentIds": discussion_agent_ids,
            "advisorAgentIds": discussion_advisor_ids,
            "executionAgentIds": execution_agent_ids,
            "summary": discussion_summary_text,
            "replyCount": len([item for item in responses if str(item.get("agentId") or "").strip() in discussion_agent_ids]),
        },
    }
    coordination_relay = _svc().relay_team_coordination_updates(
        openclaw_dir,
        thread,
        team=team,
        participant_agent_ids=participants,
        sessions_by_agent=sessions_by_agent,
        responses=responses,
        context_label=f"任务 {task_id} · {title or task_id}",
        thinking="low",
        timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
        sender_label="Task Coordination",
    )
    if coordination_relay.get("sent"):
        dispatch_summary["coordinationRelay"] = coordination_relay
    dispatch_summary["collaboration"] = build_team_collaboration_summary(dispatch_summary)
    next_team_policy = apply_team_working_memory(team_policy, dispatch_summary)
    dispatch_summary["taskLongTermMemory"] = compact_task_long_term_memory(next_team_policy.get("taskLongTermMemory"))
    task_execution_meta = thread_meta.get("taskExecution") if isinstance(thread_meta.get("taskExecution"), dict) else {}
    updated_thread = store_save_chat_thread(
        openclaw_dir,
        {
            **thread,
            "status": "waiting_internal" if responses else ("blocked" if failures else "open"),
            "updatedAt": dispatch_summary["at"],
            "meta": {
                **thread_meta,
                "teamId": team_id,
                "dispatchMode": "broadcast" if dispatch_summary["dispatchAgentIds"] else "direct",
                "lastDispatch": dispatch_summary,
                "sessionsByAgent": sessions_by_agent,
                "teamPolicy": next_team_policy,
                "coordinationRelay": coordination_relay if coordination_relay.get("sent") else thread_meta.get("coordinationRelay", {}),
                "coordinationRelayPolicy": (
                    thread_meta.get("coordinationRelayPolicy")
                    if isinstance(thread_meta.get("coordinationRelayPolicy"), dict)
                    else {"enabled": True, "strategy": "broadcast_summary"}
                ),
                "taskExecution": {
                    **task_execution_meta,
                    "internalDiscussion": dispatch_summary.get("internalDiscussion", {}),
                },
            },
        },
    )
    dispatch_summary["threadId"] = updated_thread.get("id", "") or dispatch_summary["threadId"]
    update_task_team_dispatch_state(
        openclaw_dir,
        task_id,
        dispatch_summary,
        router_agent_id=router_agent_id,
    )
    return dispatch_summary


def start_task_execution_dispatch(
    openclaw_dir,
    task_id,
    title,
    remark="",
    target_agent_id="",
    workflow_binding=None,
    router_agent_id="",
    repair_reason="",
    team=None,
    promote_success_status=False,
    linked_run_id="",
    linked_run_title="",
    auto_operation_profile=None,
):
    if not target_agent_id:
        return {"scheduled": False, "reason": "missing_target_agent"}
    session_id = task_execution_session_id(task_id)
    config = load_config(openclaw_dir)
    resolved_router_agent_id = router_agent_id or get_router_agent_id(config)
    existing_task = store_get_task_record(openclaw_dir, task_id)
    if existing_task is None:
        existing_task = next((item for item in merge_tasks(openclaw_dir, config) if item.get("id") == task_id), None)
    existing_route = existing_task.get("routeDecision") if isinstance(existing_task, dict) and isinstance(existing_task.get("routeDecision"), dict) else {}
    if not existing_route and isinstance(existing_task, dict) and isinstance(existing_task.get("meta"), dict):
        meta_route = (existing_task.get("meta") or {}).get("routeDecision")
        if isinstance(meta_route, dict):
            existing_route = meta_route
    auto_operation_profile = compact_auto_operation_profile(auto_operation_profile)
    if not auto_operation_profile and isinstance(existing_route, dict):
        auto_operation_profile = compact_auto_operation_profile(existing_route.get("autoOperationProfile"))
    auto_operation_runtime = compact_company_auto_operation_runtime(existing_route.get("autoOperationRuntime"))
    task_long_term_memory = compact_task_long_term_memory(existing_route.get("taskLongTermMemory"))
    existing_bootstrap = task_execution_bootstrap_for_task(existing_task) if isinstance(existing_task, dict) else {}
    dispatch_session_id = ""
    if str(existing_bootstrap.get("agentId") or "").strip() == str(target_agent_id or "").strip():
        dispatch_session_id = str(existing_bootstrap.get("sessionId") or "").strip()
    elif promote_success_status:
        dispatch_session_id = session_id
    dispatch_validation = (
        deepcopy(existing_route.get("dispatchValidation"))
        if isinstance(existing_route.get("dispatchValidation"), dict)
        else {}
    )
    if not dispatch_validation:
        dispatch_validation = build_task_dispatch_validation(
            title,
            remark=remark,
            workflow_binding=workflow_binding,
            intelligence=existing_route.get("intelligence") if isinstance(existing_route, dict) else {},
            team=team,
            requested_lead_agent_id=(
                str(existing_route.get("requestedLeadAgentId") or "").strip()
                or str(((existing_route.get("teamSelection") or {}) if isinstance(existing_route.get("teamSelection"), dict) else {}).get("requestedLeadAgentId") or "").strip()
            ),
            valid_agent_ids=[
                str(agent.get("id") or "").strip()
                for agent in safe_list(load_agents(config))
                if isinstance(agent, dict) and str(agent.get("id") or "").strip()
            ],
        )
    if has_blocking_task_dispatch_validation(dispatch_validation):
        blocked_agent_id = str(target_agent_id or "").strip() or resolved_router_agent_id
        blocked_session_id = dispatch_session_id or session_id
        blocked_note = str(dispatch_validation.get("summary") or "任务缺少必要信息，暂未进入自动派发。").strip()
        update_task_execution_bootstrap(
            openclaw_dir,
            task_id,
            blocked_agent_id,
            "pending_info",
            note=blocked_note,
            session_id=blocked_session_id,
            router_agent_id=router_agent_id,
            title=title,
        )
        return {
            "scheduled": False,
            "reason": "dispatch_preflight_blocked",
            "sessionId": blocked_session_id,
            "agentId": blocked_agent_id,
            "teamId": str((team or {}).get("id") or "").strip(),
            "teamName": str((team or {}).get("name") or "").strip(),
            "threadId": "",
            "bootstrap": {
                "status": "pending_info",
                "agentId": blocked_agent_id,
                "agentLabel": blocked_agent_id,
                "sessionId": blocked_session_id,
                "note": blocked_note,
                "attempts": max(int(existing_bootstrap.get("attempts") or 0), 1),
            },
            "coordinationProtocol": task_coordination_protocol_snapshot(team) if isinstance(team, dict) and team.get("id") else {},
            "validation": dispatch_validation,
        }
    if not auto_operation_profile:
        auto_operation_profile = build_company_auto_operation_profile(
            title,
            remark,
            automation_mode=current_management_automation_mode(load_project_metadata(openclaw_dir, config=config)),
            team=team,
        )
    dispatch_message = build_task_execution_message(
        task_id,
        title,
        remark=remark,
        workflow_binding=workflow_binding,
        team=team,
        linked_run_id=linked_run_id,
        linked_run_title=linked_run_title,
        auto_operation_profile=auto_operation_profile,
        auto_operation_runtime=auto_operation_runtime,
        task_long_term_memory=task_long_term_memory,
    )
    team_name = str((team or {}).get("name") or "").strip()
    bootstrap_note = (
        f"系统已将任务派发给 Team {team_name}，由 {target_agent_id} 牵头执行。{repair_reason}".strip()
        if team_name
        else f"系统已将任务派发给 {target_agent_id} 开始执行。{repair_reason}".strip()
    )
    sent_note = (
        f"已向 Team {team_name} 发出执行指令，由 {target_agent_id} 牵头执行。".strip()
        if team_name
        else "已向目标 Agent 发出执行指令。"
    )
    bootstrap = bootstrap_task_execution_state(
        openclaw_dir,
        task_id,
        target_agent_id,
        title=title,
        note=bootstrap_note,
        router_agent_id=router_agent_id,
    )

    def worker():
        try:
            with task_execution_agent_lock(target_agent_id):
                result = perform_conversation_send(
                    openclaw_dir,
                    target_agent_id,
                    dispatch_message,
                    session_id=dispatch_session_id,
                    thinking="low",
                )
            agent_meta = (((result.get("result", {}) or {}).get("meta", {}) or {}).get("agentMeta", {}) or {})
            actual_session_id = str(agent_meta.get("sessionId") or dispatch_session_id or "main").strip() or "main"
            update_task_execution_bootstrap(
                openclaw_dir,
                task_id,
                target_agent_id,
                "dispatched" if promote_success_status else "scheduled",
                note=sent_note,
                session_id=actual_session_id,
                router_agent_id=router_agent_id,
                title=title,
            )
            dispatch_task_execution_team_members(
                openclaw_dir,
                task_id,
                title,
                remark=remark,
                lead_agent_id=target_agent_id,
                workflow_binding=workflow_binding,
                team=team,
                router_agent_id=router_agent_id,
                lead_result=result,
                linked_run_id=linked_run_id,
                linked_run_title=linked_run_title,
                auto_operation_profile=auto_operation_profile,
                auto_operation_runtime=auto_operation_runtime,
                task_long_term_memory=task_long_term_memory,
            )
        except Exception as error:
            update_task_execution_bootstrap(
                openclaw_dir,
                task_id,
                target_agent_id,
                "failed",
                note=str(error),
                session_id=dispatch_session_id,
                router_agent_id=router_agent_id,
                title=title,
            )

    dispatch_future = TASK_EXECUTION_DISPATCH_POOL.submit(worker)
    with TASK_EXECUTION_DISPATCH_FUTURES_LOCK:
        TASK_EXECUTION_DISPATCH_FUTURES.add(dispatch_future)
    if hasattr(dispatch_future, "add_done_callback"):
        dispatch_future.add_done_callback(_discard_task_execution_dispatch_future)
    else:
        _discard_task_execution_dispatch_future(dispatch_future)
    initial_thread = (
        ensure_task_execution_team_thread(
            openclaw_dir,
            task_id,
            title,
            team,
            lead_agent_id=target_agent_id,
            linked_run_id=linked_run_id,
        )
        if isinstance(team, dict) and team.get("id")
        else {}
    )
    return {
        "scheduled": True,
        "sessionId": session_id,
        "agentId": target_agent_id,
        "teamId": str((team or {}).get("id") or "").strip(),
        "teamName": team_name,
        "threadId": str(initial_thread.get("id") or "").strip(),
        "bootstrap": bootstrap,
        "coordinationProtocol": task_coordination_protocol_snapshot(team) if isinstance(team, dict) and team.get("id") else {},
    }
