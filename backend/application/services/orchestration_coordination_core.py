from __future__ import annotations

from . import orchestration_core as core


_SHARED_NAMES = [
    "TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS",
    "TEAM_CONVERSATION_MAX_PARALLEL",
    "TEAM_CONVERSATION_RELAY_REPLY_LIMIT",
    "TEAM_CONVERSATION_RELAY_STAGGER_SECONDS",
    "_svc",
    "agent_runtime_identity_payload",
    "apply_conversation_fanout_stagger",
    "apply_turn_guidance_to_message",
    "atomic_task_store_update",
    "build_human_turn_anchor_payload",
    "build_human_turn_profile_payload",
    "build_task_long_term_memory_payload",
    "conversation_reply_preview",
    "coordination_reply_entries",
    "deepcopy",
    "get_router_agent_id",
    "invalidate_dashboard_bundle_cache",
    "load_agents",
    "load_config",
    "load_project_metadata",
    "now_iso",
    "order_agent_ids_for_human_turns",
    "perform_conversation_fanout",
    "resolve_agent_team_record",
    "safe_list",
    "select_persistable_conversation_session_id",
    "select_human_turn_targets",
    "store_get_chat_thread",
    "store_save_chat_message",
    "submit_task_execution_background",
    "summarize_task_execution_text",
    "task_coordination_prompt_lines",
    "task_workspace_for_task",
    "team_memory_trace_payload",
    "team_state_packet_payload",
]

globals().update({name: getattr(core, name) for name in _SHARED_NAMES})


def _persisted_coordination_session_id(openclaw_dir, agent_id, primary_session_id="", fallback_session_id=""):
    return select_persistable_conversation_session_id(
        openclaw_dir,
        str(agent_id or "").strip(),
        primary_session_id=primary_session_id,
        fallback_session_id=fallback_session_id,
    )


def build_team_coordination_relay_message(team, context_label="", responses=None):
    team = team if isinstance(team, dict) else {}
    team_name = str(team.get("name") or "").strip()
    response_entries = coordination_reply_entries(responses)
    if len(response_entries) < 1:
        return ""
    group_label = team_name or "团队里"
    parts = [f"{group_label}刚刚已经聊到这些判断："]
    if context_label:
        parts.append(f"这轮先围绕：{context_label}")
    parts.append("先开口的同事提到：")
    parts.extend(f"- {item['agentDisplayName']}：{item['replyPreview']}" for item in response_entries)
    protocol_lines = task_coordination_prompt_lines(team, audience="member")
    if protocol_lines:
        parts.extend(protocol_lines)
    parts.append("这里允许同事之间直接接力协作；如果更适合别人接手，就直接点名谁来接，不要误说成没有权限。")
    parts.append("请先回应上面某位同事的判断或依赖，再补一句你是赞同、补充、接住，还是有不同看法。")
    parts.append("如果你的判断和上一位基本一致，就不要重复展开，补一句你新增的信息或你的接手动作就够了。")
    parts.append("如果你能接住别人的依赖，就直接说你来处理；如果你不同意，也先点明你不同意哪一点，再给替代建议。")
    parts.append("除非没有新增判断，否则不要只回已阅。")
    return "\n".join(parts)


def schedule_chat_thread_coordination_relay(
    openclaw_dir,
    thread_id,
    team_id="",
    participant_agent_ids=None,
    sessions_by_agent=None,
    responses=None,
    context_label="",
    thinking="low",
    output_dir=None,
):
    normalized_thread_id = str(thread_id or "").strip()
    normalized_team_id = str(team_id or "").strip()
    participant_ids = [
        str(item or "").strip()
        for item in safe_list(participant_agent_ids)
        if str(item or "").strip()
    ]
    if not normalized_thread_id or not normalized_team_id or len(participant_ids) < 3:
        return {"scheduled": False, "sent": False, "targetAgentIds": participant_ids, "respondedAgentIds": [], "failedAgents": [], "replyCount": 0}
    session_map = {
        str(agent_id or "").strip(): str(session_id or "").strip()
        for agent_id, session_id in (sessions_by_agent or {}).items()
        if str(agent_id or "").strip()
    }
    response_snapshot = [deepcopy(item) for item in safe_list(responses) if isinstance(item, dict)]

    def worker():
        thread = store_get_chat_thread(openclaw_dir, normalized_thread_id)
        if not thread:
            return
        team = resolve_agent_team_record(openclaw_dir, normalized_team_id) or {}
        coordination_relay = _svc().relay_team_coordination_updates(
            openclaw_dir,
            thread,
            team=team,
            participant_agent_ids=participant_ids,
            sessions_by_agent=session_map,
            responses=response_snapshot,
            context_label=context_label,
            thinking=thinking,
            timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
            sender_label="Team Coordination",
        )
        if not (coordination_relay.get("sent") or safe_list(coordination_relay.get("failedAgents"))):
            return
        latest_thread = store_get_chat_thread(openclaw_dir, normalized_thread_id) or thread
        latest_meta = deepcopy(latest_thread.get("meta", {})) if isinstance(latest_thread.get("meta"), dict) else {}
        latest_sessions = latest_meta.get("sessionsByAgent") if isinstance(latest_meta.get("sessionsByAgent"), dict) else {}
        for agent_id, session_id in session_map.items():
            normalized_agent_id = str(agent_id or "").strip()
            persisted_session_id = _persisted_coordination_session_id(
                openclaw_dir,
                normalized_agent_id,
                primary_session_id=session_id,
            )
            if normalized_agent_id and persisted_session_id:
                latest_sessions[normalized_agent_id] = persisted_session_id
            elif normalized_agent_id:
                latest_sessions.pop(normalized_agent_id, None)
        for item in safe_list(coordination_relay.get("responses")):
            if not isinstance(item, dict):
                continue
            agent_id = str(item.get("agentId") or "").strip()
            persisted_session_id = _persisted_coordination_session_id(
                openclaw_dir,
                agent_id,
                primary_session_id=item.get("sessionId"),
                fallback_session_id=latest_sessions.get(agent_id, ""),
            )
            if agent_id and persisted_session_id:
                latest_sessions[agent_id] = persisted_session_id
            elif agent_id:
                latest_sessions.pop(agent_id, None)
        last_dispatch = latest_meta.get("lastDispatch") if isinstance(latest_meta.get("lastDispatch"), dict) else {}
        if last_dispatch:
            latest_meta["lastDispatch"] = {
                **last_dispatch,
                "coordinationRelay": coordination_relay,
            }
        latest_meta["sessionsByAgent"] = latest_sessions
        latest_meta["coordinationRelay"] = coordination_relay
        core.store_save_chat_thread(
            openclaw_dir,
            {
                **latest_thread,
                "updatedAt": now_iso(),
                "meta": latest_meta,
            },
        )
        invalidate_dashboard_bundle_cache(openclaw_dir, output_dir)

    submit_task_execution_background(worker)
    return {
        "scheduled": True,
        "sent": False,
        "targetAgentIds": participant_ids,
        "respondedAgentIds": [],
        "failedAgents": [],
        "replyCount": 0,
    }


def update_task_team_dispatch_state(openclaw_dir, task_id, dispatch_state, router_agent_id=""):
    if not task_id or not isinstance(dispatch_state, dict):
        return
    config = load_config(openclaw_dir)
    router_agent_id = router_agent_id or get_router_agent_id(config)
    task_workspace = task_workspace_for_task(openclaw_dir, task_id, config=config, router_agent_id=router_agent_id)
    timestamp = now_iso()

    def modifier(data):
        tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
        for task in tasks:
            if not isinstance(task, dict) or task.get("id") != task_id:
                continue
            meta = task.get("meta") if isinstance(task.get("meta"), dict) else {}
            route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
            route_meta["teamDispatch"] = dict(dispatch_state)
            route_meta["taskLongTermMemory"] = build_task_long_term_memory_payload(
                route_meta.get("taskLongTermMemory"),
                dispatch_state,
                fallback_title=str(task.get("title") or task_id).strip(),
                fallback_note=str(task.get("remark") or "").strip(),
            )
            meta["routeDecision"] = route_meta
            task["meta"] = meta
            task["routeDecision"] = route_meta
            summary_text = str(dispatch_state.get("summaryText") or "").strip()
            team_name = str(dispatch_state.get("teamName") or task.get("teamLabel") or "").strip()
            if summary_text:
                task.setdefault("flow_log", [])
                last_flow = task["flow_log"][-1] if task["flow_log"] else {}
                if str(last_flow.get("remark") or "").strip() != summary_text:
                    task["flow_log"].append(
                        {
                            "at": timestamp,
                            "from": "OpenClaw Team",
                            "to": team_name or str(dispatch_state.get("leadAgentId") or "").strip() or "Team",
                            "remark": summary_text,
                        }
                    )
                task.setdefault("progress_log", [])
                last_progress = task["progress_log"][-1] if task["progress_log"] else {}
                if str(last_progress.get("text") or "").strip() != summary_text:
                    task["progress_log"].append(
                        {
                            "at": timestamp,
                            "agent": str(dispatch_state.get("leadAgentId") or task.get("targetAgentId") or "").strip(),
                            "agentLabel": team_name or str(task.get("targetAgentLabel") or "").strip(),
                            "text": summary_text,
                            "state": str(task.get("state") or task.get("status") or "").strip(),
                            "org": team_name or str(task.get("org") or "").strip(),
                            "todos": [],
                        }
                    )
            task["updatedAt"] = timestamp
        return tasks

    atomic_task_store_update(openclaw_dir, task_workspace, modifier, [])


def relay_team_coordination_updates(
    openclaw_dir,
    thread,
    team=None,
    participant_agent_ids=None,
    sessions_by_agent=None,
    responses=None,
    context_label="",
    thinking="low",
    timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
    sender_label="Team Coordination",
    force_targets=False,
):
    team = team if isinstance(team, dict) else {}
    thread = thread if isinstance(thread, dict) else {}
    thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_policy = thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {}
    memory_trace_meta = team_memory_trace_payload(team_policy)
    state_packet_meta = team_state_packet_payload(team_policy)
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    participant_ids = [
        str(item or "").strip()
        for item in safe_list(participant_agent_ids)
        if str(item or "").strip()
    ]
    participant_ids = order_agent_ids_for_human_turns(
        openclaw_dir,
        participant_ids,
        lead_agent_id=str(team.get("leadAgentId") or "").strip(),
        config=config,
        metadata=metadata,
        agents=agents,
    )
    relay_target_ids = select_human_turn_targets(
        openclaw_dir,
        participant_ids,
        purpose="relay",
        lead_agent_id=str(team.get("leadAgentId") or "").strip(),
        responses=responses,
        config=config,
        metadata=metadata,
        agents=agents,
    )
    if force_targets and not relay_target_ids:
        relay_target_ids = participant_ids[: max(1, min(TEAM_CONVERSATION_RELAY_REPLY_LIMIT, len(participant_ids)))]
    relay_turn_anchors = {
        agent_id: core.build_human_turn_anchor_payload(
            openclaw_dir,
            agent_id,
            turn_index=index,
            ordered_agent_ids=relay_target_ids,
            prior_responses=responses,
            lead_agent_id=str(team.get("leadAgentId") or "").strip(),
            config=config,
            metadata=metadata,
            agents=agents,
        )
        for index, agent_id in enumerate(relay_target_ids)
    }
    relay_turn_profiles = {
        agent_id: core.build_human_turn_profile_payload(
            openclaw_dir,
            agent_id,
            turn_index=index,
            participant_count=len(relay_target_ids),
            lead_agent_id=str(team.get("leadAgentId") or "").strip(),
            prior_responses=responses,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        for index, agent_id in enumerate(relay_target_ids)
    }
    if len(relay_target_ids) < 1:
        return {"sent": False, "targetAgentIds": [], "respondedAgentIds": [], "responses": [], "failedAgents": [], "replyCount": 0}
    relay_message = build_team_coordination_relay_message(team, context_label=context_label, responses=responses)
    if not relay_message:
        return {"sent": False, "targetAgentIds": [], "respondedAgentIds": [], "responses": [], "failedAgents": [], "replyCount": 0}
    session_map = sessions_by_agent if isinstance(sessions_by_agent, dict) else {}
    relay_root = store_save_chat_message(
        openclaw_dir,
        {
            "threadId": thread.get("id", ""),
            "senderKind": "system",
            "senderId": "team-coordination",
            "senderLabel": sender_label,
            "direction": "system",
            "body": relay_message,
            "meta": {
                "coordinationRelay": True,
                "targetAgentIds": relay_target_ids,
                **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
            },
        },
    )
    relay_requests = apply_conversation_fanout_stagger(
        [
            {
                "agentId": agent_id,
                "sessionId": str(session_map.get(agent_id, "") or "").strip(),
                "message": apply_turn_guidance_to_message(
                    relay_message,
                    turn_index=index,
                    participant_count=len(relay_target_ids),
                    anchor=relay_turn_anchors.get(agent_id),
                    turn_profile=relay_turn_profiles.get(agent_id),
                ),
                "thinking": thinking,
            }
            for index, agent_id in enumerate(relay_target_ids)
        ],
        stagger_seconds=TEAM_CONVERSATION_RELAY_STAGGER_SECONDS,
    )
    relay_results = perform_conversation_fanout(
        openclaw_dir,
        relay_requests,
        default_thinking=thinking,
        timeout_seconds=timeout_seconds,
        max_workers=TEAM_CONVERSATION_MAX_PARALLEL,
    )
    relay_responses = []
    relay_failures = []
    for failure in relay_results["failures"]:
        agent_id = str(failure.get("agentId") or "").strip()
        error_message = str(failure.get("error") or "团队协同同步失败。").strip()
        relay_failures.append({"agentId": agent_id, "error": error_message})
        store_save_chat_message(
            openclaw_dir,
            {
                "threadId": thread.get("id", ""),
                "senderKind": "system",
                "senderId": "team-coordination",
                "senderLabel": sender_label,
                "direction": "system",
                "body": f"{agent_id} 暂未接住同队同步：{error_message}",
                "meta": {
                    "coordinationRelay": True,
                    "agentId": agent_id,
                    "replyToMessageId": relay_root.get("id", ""),
                    **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                },
            },
        )
    for success in relay_results["successes"]:
        agent_id = str(success.get("agentId") or "").strip()
        actual_session_id = _persisted_coordination_session_id(
            openclaw_dir,
            agent_id,
            primary_session_id=success.get("sessionId"),
            fallback_session_id=session_map.get(agent_id, ""),
        )
        if actual_session_id:
            session_map[agent_id] = actual_session_id
        else:
            session_map.pop(agent_id, None)
        reply_preview = core.summarize_task_execution_text(conversation_reply_preview(success.get("result")), limit=320)
        agent_identity = agent_runtime_identity_payload(
            openclaw_dir,
            agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        relay_responses.append(
            {
                "agentId": agent_id,
                "sessionId": actual_session_id,
                "replyPreview": reply_preview,
                "agentDisplayName": agent_identity.get("displayName", ""),
                "agentHumanName": agent_identity.get("humanName", ""),
                "agentJobTitle": agent_identity.get("jobTitle", ""),
            }
        )
        if reply_preview:
            saved_message = store_save_chat_message(
                openclaw_dir,
                {
                    "threadId": thread.get("id", ""),
                    "senderKind": "agent",
                    "senderId": agent_id,
                    "senderLabel": agent_identity.get("displayName", "") or agent_id,
                    "direction": "agent",
                    "body": reply_preview,
                    "meta": {
                        "sessionId": actual_session_id,
                        "coordinationRelay": True,
                        "replyToMessageId": relay_root.get("id", ""),
                        "agentHumanName": agent_identity.get("humanName", ""),
                        "agentJobTitle": agent_identity.get("jobTitle", ""),
                        "turnFocus": (relay_turn_profiles.get(agent_id) or {}).get("turnFocus", ""),
                        "turnPace": (relay_turn_profiles.get(agent_id) or {}).get("turnPace", ""),
                        **(relay_turn_anchors.get(agent_id) or {}),
                        **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                        **memory_trace_meta,
                    },
                },
            )
            relay_responses[-1]["messageId"] = saved_message.get("id", "")
    return {
        "sent": True,
        "messageId": relay_root.get("id", ""),
        "targetAgentIds": relay_target_ids,
        "respondedAgentIds": [item.get("agentId") for item in relay_responses if item.get("agentId")],
        "responses": relay_responses,
        "failedAgents": relay_failures,
        "replyCount": len(relay_responses),
    }
