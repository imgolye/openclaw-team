from __future__ import annotations

from . import orchestration_core as core
from . import orchestration_coordination_core as coordination_core
from . import orchestration_dispatch_core as dispatch_core


# Keep these references bound to the shared delegated symbols while the
# orchestration domain is being split into smaller service modules.
_SHARED_NAMES = [
    "L_DONE",
    "MANAGEMENT_AUTOMATION_MODE_ASSISTIVE",
    "MANAGEMENT_AUTOMATION_MODE_FULL_AUTO",
    "Path",
    "TASK_CREATE_MODEL_DECISION_TIMEOUT_SECONDS",
    "TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS",
    "TEAM_CONVERSATION_MAX_PARALLEL",
    "TEAM_CONVERSATION_STAGGER_SECONDS",
    "_svc",
    "agent_runtime_identity_payload",
    "apply_conversation_fanout_stagger",
    "apply_team_working_memory",
    "apply_turn_guidance_to_message",
    "atomic_task_store_update",
    "build_company_auto_operation_profile",
    "build_human_turn_anchor_payload",
    "build_human_turn_profile_payload",
    "build_team_collaboration_summary",
    "compact_auto_operation_profile",
    "compact_task_long_term_memory",
    "conversation_reply_preview",
    "coordination_reply_entries",
    "create_management_run_for_task",
    "current_management_automation_mode",
    "deepcopy",
    "ensure_planning_bundle",
    "ensure_task_execution_team_thread",
    "existing_task_team_thread",
    "has_blocking_task_dispatch_validation",
    "find_duplicate_active_task",
    "get_router_agent_id",
    "latest_management_run_for_task",
    "latest_routing_decision_for_task",
    "link_task_to_agent_team",
    "load_agents",
    "load_config",
    "load_kanban_config",
    "load_project_metadata",
    "logging",
    "merge_team_policy_state",
    "now_iso",
    "patch_task_routing_metadata",
    "patch_task_team_assignment_metadata",
    "perform_conversation_fanout",
    "planning_binding_from_payload",
    "record_routing_outcome",
    "resolve_agent_team_record",
    "resolve_task_dispatch_plan",
    "run_python_script",
    "runtime_script_path",
    "seed_task_long_term_memory_payload",
    "select_persistable_conversation_session_id",
    "select_human_turn_targets",
    "store_save_chat_message",
    "store_save_chat_thread",
    "store_save_routing_decision",
    "summarize_task_execution_text",
    "task_coordination_prompt_lines",
    "task_coordination_protocol_snapshot",
    "task_effective_team_id",
    "task_execution_bootstrap_for_task",
    "task_execution_session_id",
    "task_long_term_memory_prompt_lines",
    "task_team_participant_agent_ids",
    "task_workspace_for_task",
    "team_memory_trace_payload",
    "team_state_packet_payload",
    "update_task_execution_bootstrap",
]

globals().update({name: getattr(core, name) for name in _SHARED_NAMES})


def _start_task_execution_dispatch(*args, **kwargs):
    patched = getattr(_svc(), "start_task_execution_dispatch")
    if patched is dispatch_core.start_task_execution_dispatch:
        return dispatch_core.start_task_execution_dispatch(*args, **kwargs)
    return patched(*args, **kwargs)


update_task_team_dispatch_state = coordination_core.update_task_team_dispatch_state


def perform_task_progress(openclaw_dir, task_id, message, todos="", mark_doing=False):
    task_id = str(task_id or "").strip()
    if not task_id:
        logging.warning("perform_task_progress: task_id is empty, ignoring progress update")
        return {}
    config = load_config(openclaw_dir)
    workspace = task_workspace_for_task(openclaw_dir, task_id, config=config)
    workspace_script = workspace / "scripts" / "kanban_update.py"
    if not workspace_script.exists():
        timestamp = now_iso()

        def modifier(data):
            tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
            for task in tasks:
                if not isinstance(task, dict) or task.get("id") != task_id:
                    continue
                if mark_doing:
                    task["state"] = "Doing"
                    task["status"] = "Doing"
                task["currentUpdate"] = str(message or "").strip()
                task["now"] = str(message or "").strip()
                task["updatedAt"] = timestamp
                task.setdefault("progress_log", [])
                task["progress_log"].append(
                    {
                        "at": timestamp,
                        "agent": str(task.get("targetAgentId") or "").strip(),
                        "agentLabel": str(task.get("targetAgentLabel") or task.get("org") or "").strip(),
                        "text": str(message or "").strip(),
                        "todos": [],
                        "state": str(task.get("state") or "Doing"),
                        "org": str(task.get("org") or "").strip(),
                    }
                )
            return tasks

        atomic_task_store_update(openclaw_dir, workspace, modifier, [])
        return
    kanban_script = workspace_script
    if mark_doing:
        state_result, state_output = run_python_script(kanban_script, ["state", task_id, "Doing", message], cwd=workspace)
        if state_result.returncode != 0:
            raise RuntimeError(state_output or "无法把任务切换到执行中。")
    args = ["progress", task_id, message]
    if todos:
        args.append(todos)
    progress_result, progress_output = run_python_script(kanban_script, args, cwd=workspace)
    if progress_result.returncode != 0:
        raise RuntimeError(progress_output or "进展同步失败。")


def perform_task_block(openclaw_dir, task_id, reason):
    config = load_config(openclaw_dir)
    workspace = task_workspace_for_task(openclaw_dir, task_id, config=config)
    workspace_script = workspace / "scripts" / "kanban_update.py"
    if not workspace_script.exists():
        timestamp = now_iso()

        def modifier(data):
            tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
            for task in tasks:
                if not isinstance(task, dict) or task.get("id") != task_id:
                    continue
                task["state"] = "Blocked"
                task["status"] = "Blocked"
                task["block"] = str(reason or "").strip()
                task["blockers"] = str(reason or "").strip()
                task["currentUpdate"] = str(reason or "").strip()
                task["now"] = str(reason or "").strip()
                task["updatedAt"] = timestamp
            return tasks

        atomic_task_store_update(openclaw_dir, workspace, modifier, [])
        record_routing_outcome(openclaw_dir, task_id, "blocked", note=reason)
        return
    kanban_script = workspace_script
    result, output = run_python_script(kanban_script, ["block", task_id, reason], cwd=workspace)
    if result.returncode != 0:
        raise RuntimeError(output or "阻塞标记失败。")
    record_routing_outcome(openclaw_dir, task_id, "blocked", note=reason)


def perform_task_done(openclaw_dir, task_id, output_path="", summary=""):
    task_id = str(task_id or "").strip()
    if not task_id:
        logging.warning("perform_task_done: task_id is empty, ignoring done update")
        return {}
    config = load_config(openclaw_dir)
    workspace = task_workspace_for_task(openclaw_dir, task_id, config=config)
    workspace_script = workspace / "scripts" / "kanban_update.py"
    if not workspace_script.exists():
        timestamp = now_iso()
        summary_text = str(summary or output_path or L_DONE).strip()

        def modifier(data):
            tasks = data if isinstance(data, list) else (data.get("tasks", []) if isinstance(data, dict) else [])
            for task in tasks:
                if not isinstance(task, dict) or task.get("id") != task_id:
                    continue
                task["state"] = "Done"
                task["status"] = "Done"
                task["currentUpdate"] = summary_text
                task["now"] = summary_text
                task["updatedAt"] = timestamp
                if output_path:
                    task["output"] = str(output_path).strip()
                task.setdefault("flow_log", [])
                task["flow_log"].append(
                    {
                        "at": timestamp,
                        "from": str(task.get("org") or task.get("targetAgentLabel") or "").strip(),
                        "to": "CEO",
                        "remark": f"✅ Task completed: {summary_text}",
                    }
                )
            return tasks

        atomic_task_store_update(openclaw_dir, workspace, modifier, [])
        record_routing_outcome(openclaw_dir, task_id, "completed", note=summary or output_path)
        return
    kanban_script = workspace_script
    args = ["done", task_id]
    if output_path or summary:
        args.append(output_path)
    if summary:
        args.append(summary)
    result, output = run_python_script(kanban_script, args, cwd=workspace)
    if result.returncode != 0:
        raise RuntimeError(output or "任务完成写回失败。")
    record_routing_outcome(openclaw_dir, task_id, "completed", note=summary or output_path)


def perform_task_preview(openclaw_dir, title, remark="", preferred_team_id="", team_override_reason="", requested_lead_agent_id=""):
    title = str(title or "").strip()
    if not title:
        raise RuntimeError("任务标题不能为空。")
    plan = resolve_task_dispatch_plan(
        openclaw_dir,
        title,
        remark=remark,
        preferred_team_id=preferred_team_id,
        team_override_reason=team_override_reason,
        model_timeout_seconds=TASK_CREATE_MODEL_DECISION_TIMEOUT_SECONDS,
        requested_lead_agent_id=requested_lead_agent_id,
    )
    decision = plan.get("decision") if isinstance(plan.get("decision"), dict) else {}
    workflow_resolution = plan.get("workflowResolution") if isinstance(plan.get("workflowResolution"), dict) else {}
    team = plan.get("team") if isinstance(plan.get("team"), dict) else None
    team_selection = plan.get("teamSelection") if isinstance(plan.get("teamSelection"), dict) else {}
    selected_execution_agent_id = str(plan.get("selectedExecutionAgentId") or "").strip()
    agent_labels = plan.get("agentLabels") if isinstance(plan.get("agentLabels"), dict) else {}
    automation_mode = current_management_automation_mode(load_project_metadata(openclaw_dir, config=plan.get("config")))
    auto_operation_profile = build_company_auto_operation_profile(
        title,
        remark,
        automation_mode=automation_mode,
        team=team,
    )
    task_long_term_memory = seed_task_long_term_memory_payload(title=title, remark=remark, team=team)
    validation = deepcopy(plan.get("validation")) if isinstance(plan.get("validation"), dict) else {}
    return {
        "routingDecision": {
            "policyId": decision.get("policyId", ""),
            "policyName": decision.get("policyName", ""),
            "workflowId": workflow_resolution.get("binding", {}).get("workflowId", ""),
            "workflowVersionId": workflow_resolution.get("binding", {}).get("workflowVersionId", ""),
            "strategyType": decision.get("strategyType", ""),
            "matchedKeyword": decision.get("matchedKeyword", ""),
            "queueName": decision.get("queueName", ""),
            "priorityLevel": decision.get("priorityLevel", "normal"),
            "targetAgentId": decision.get("targetAgentId", ""),
            "sourceText": decision.get("sourceText", ""),
            "meta": {
                "reason": decision.get("reason", ""),
                "fallback": bool(decision.get("fallback")),
                "targetAgentLabel": decision.get("targetAgentLabel", ""),
                "workflowName": workflow_resolution.get("binding", {}).get("workflowName", ""),
                "selectedBranch": workflow_resolution.get("binding", {}).get("selectedBranch", {}),
                "intelligence": decision.get("intelligence", {}),
                "decisionSource": ((decision.get("intelligence") or {}).get("decisionSource") if isinstance(decision.get("intelligence"), dict) else "") or "heuristic",
                "modelDecision": ((decision.get("intelligence") or {}).get("modelDecision") if isinstance(decision.get("intelligence"), dict) and isinstance((decision.get("intelligence") or {}).get("modelDecision"), dict) else {}),
                "trace": decision.get("trace", []),
                "manualReview": bool(((decision.get("intelligence") or {}).get("manualReview")) or team_selection.get("manualReviewRecommended")),
                "confidence": (decision.get("intelligence") or {}).get("confidence", 0),
                "teamSelection": team_selection,
                **(
                    {"autoOperationProfile": compact_auto_operation_profile(auto_operation_profile)}
                    if compact_auto_operation_profile(auto_operation_profile)
                    else {}
                ),
                "taskLongTermMemory": task_long_term_memory,
            },
        },
        "workflowBinding": workflow_resolution.get("binding", {}),
        "team": deepcopy(team) if team else None,
        "teamSelection": deepcopy(team_selection),
        "execution": {
            "agentId": selected_execution_agent_id,
            "agentLabel": agent_labels.get(selected_execution_agent_id, selected_execution_agent_id),
            "teamId": str((team or {}).get("id") or "").strip(),
            "teamName": str((team or {}).get("name") or "").strip(),
            "scheduled": not has_blocking_task_dispatch_validation(validation),
            "reason": "dispatch_preflight_blocked" if has_blocking_task_dispatch_validation(validation) else "preview",
        },
        "validation": validation,
    }


def perform_task_create(openclaw_dir, title, remark="", preferred_team_id="", run_owner="", team_override_reason="", prefer_fast_routing=False, requested_lead_agent_id=""):
    plan = resolve_task_dispatch_plan(
        openclaw_dir,
        title,
        remark=remark,
        preferred_team_id=preferred_team_id,
        team_override_reason=team_override_reason,
        model_timeout_seconds=TASK_CREATE_MODEL_DECISION_TIMEOUT_SECONDS,
        prefer_fast_routing=prefer_fast_routing,
        requested_lead_agent_id=requested_lead_agent_id,
    )
    openclaw_dir = Path(plan.get("openclawDir") or openclaw_dir)
    config = plan.get("config") if isinstance(plan.get("config"), dict) else load_config(openclaw_dir)
    project_dir = plan.get("projectDir")
    router_agent_id = str(plan.get("routerAgentId") or get_router_agent_id(config)).strip()
    router_workspace = Path(openclaw_dir) / f"workspace-{router_agent_id}"
    kanban_cfg = plan.get("kanbanCfg") if isinstance(plan.get("kanbanCfg"), dict) else load_kanban_config(openclaw_dir, router_agent_id)
    agent_labels = plan.get("agentLabels") if isinstance(plan.get("agentLabels"), dict) else {}
    kanban_script = runtime_script_path(openclaw_dir, "kanban_update.py")
    prefix = kanban_cfg.get("task_prefix", "TASK")
    planner_title = str(plan.get("plannerTitle") or kanban_cfg.get("state_org_map", {}).get("Planning") or "Planner").strip()
    intelligence = plan.get("intelligence") if isinstance(plan.get("intelligence"), dict) else {}
    workflow_resolution = plan.get("workflowResolution") if isinstance(plan.get("workflowResolution"), dict) else {}
    decision = plan.get("decision") if isinstance(plan.get("decision"), dict) else {}
    team = plan.get("team") if isinstance(plan.get("team"), dict) else None
    team_selection_payload = plan.get("teamSelection") if isinstance(plan.get("teamSelection"), dict) else {}
    selected_execution_agent_id = str(plan.get("selectedExecutionAgentId") or "").strip()
    validation = deepcopy(plan.get("validation")) if isinstance(plan.get("validation"), dict) else {}
    automation_mode = current_management_automation_mode(load_project_metadata(openclaw_dir, config=config))
    auto_operation_profile = build_company_auto_operation_profile(
        title,
        remark,
        automation_mode=automation_mode,
        team=team,
    )
    task_long_term_memory = seed_task_long_term_memory_payload(title=title, remark=remark, team=team)
    duplicate_task = find_duplicate_active_task(
        plan.get("existingTasks"),
        title,
        remark=remark,
        team_id=str(team_selection_payload.get("selectedTeamId") or "").strip(),
    )
    if duplicate_task:
        task_id = str(duplicate_task.get("id") or "").strip()
        existing_team_id = task_effective_team_id(duplicate_task) or str(team_selection_payload.get("selectedTeamId") or "").strip()
        existing_team = resolve_agent_team_record(openclaw_dir, existing_team_id) or team
        existing_run = (
            duplicate_task.get("linkedRun")
            if isinstance(duplicate_task.get("linkedRun"), dict) and str((duplicate_task.get("linkedRun") or {}).get("id") or "").strip()
            else latest_management_run_for_task(openclaw_dir, task_id)
        )
        existing_thread = existing_task_team_thread(openclaw_dir, task_id, team_id=existing_team_id)
        existing_execution = task_execution_bootstrap_for_task(duplicate_task)
        existing_route = duplicate_task.get("routeDecision") if isinstance(duplicate_task.get("routeDecision"), dict) else {}
        if not existing_route and isinstance(duplicate_task.get("meta"), dict):
            meta_route = (duplicate_task.get("meta") or {}).get("routeDecision")
            if isinstance(meta_route, dict):
                existing_route = meta_route
        existing_workflow_binding = (
            duplicate_task.get("workflowBinding")
            if isinstance(duplicate_task.get("workflowBinding"), dict)
            else ((duplicate_task.get("meta") or {}).get("workflowBinding", {}) if isinstance(duplicate_task.get("meta"), dict) else {})
        )
        return {
            "taskId": task_id,
            "routingDecision": latest_routing_decision_for_task(openclaw_dir, task_id) or {
                "taskId": task_id,
                "targetAgentId": str(existing_route.get("targetAgentId") or duplicate_task.get("targetAgentId") or selected_execution_agent_id).strip(),
                "meta": {
                    "teamSelection": deepcopy(existing_route.get("teamSelection")) if isinstance(existing_route.get("teamSelection"), dict) else deepcopy(team_selection_payload),
                },
            },
            "workflowBinding": deepcopy(existing_workflow_binding) if isinstance(existing_workflow_binding, dict) else {},
            "planningBundle": planning_binding_from_payload(duplicate_task) or planning_binding_from_payload(existing_run),
            "execution": {
                "scheduled": False,
                "reason": "duplicate_active_task",
                "sessionId": str(existing_execution.get("sessionId") or task_execution_session_id(task_id)).strip() or task_execution_session_id(task_id),
                "agentId": str(existing_execution.get("agentId") or duplicate_task.get("currentAgent") or selected_execution_agent_id).strip(),
                "teamId": existing_team_id,
                "teamName": str((existing_team or {}).get("name") or duplicate_task.get("teamLabel") or "").strip(),
                "threadId": str(existing_thread.get("id") or duplicate_task.get("teamThreadId") or "").strip(),
                "bootstrap": deepcopy(existing_execution),
                "coordinationProtocol": task_coordination_protocol_snapshot(existing_team) if isinstance(existing_team, dict) and existing_team.get("id") else {},
            },
            "team": deepcopy(existing_team) if isinstance(existing_team, dict) else None,
            "teamSelection": (
                deepcopy(existing_route.get("teamSelection"))
                if isinstance(existing_route.get("teamSelection"), dict)
                else deepcopy(team_selection_payload)
            ),
            "run": deepcopy(existing_run) if isinstance(existing_run, dict) else {},
            "deduped": True,
            "duplicateOfTaskId": task_id,
            "validation": (
                deepcopy(existing_route.get("dispatchValidation"))
                if isinstance(existing_route.get("dispatchValidation"), dict)
                else validation
            ),
        }

    next_id_result, next_id_output = run_python_script(kanban_script, ["next-id", prefix], cwd=router_workspace)
    if next_id_result.returncode != 0:
        raise RuntimeError(next_id_output or "无法生成新的任务号。")
    task_id = next_id_output.splitlines()[-1].strip()
    if not task_id:
        raise RuntimeError("任务号生成失败。")

    args = ["create", task_id, title, "Planning", planner_title, planner_title]
    if remark:
        args.append(remark)
    create_result, create_output = run_python_script(kanban_script, args, cwd=router_workspace)
    if create_result.returncode != 0:
        raise RuntimeError(create_output or "创建任务失败。")
    planning_bundle = (
        ensure_planning_bundle(
            openclaw_dir,
            project_dir,
            "task",
            task_id,
            title=title or task_id,
            goal=remark or title or task_id,
            meta={
                "taskId": task_id,
                "routerAgentId": router_agent_id,
                "targetAgentId": selected_execution_agent_id,
                "workflowBinding": workflow_resolution.get("binding", {}),
            },
        )
        if project_dir
        else {}
    )
    patch_task_routing_metadata(
        openclaw_dir,
        task_id=task_id,
        decision=decision,
        workflow_binding=workflow_resolution.get("binding", {}),
        planning_bundle=planning_bundle,
        kanban_cfg=kanban_cfg,
        router_agent_id=router_agent_id,
        title=title,
        remark=remark,
        planner_title=planner_title,
        team_selection=team_selection_payload,
        manual_review=bool(team_selection_payload.get("manualReviewRecommended")),
        execution_target_agent_id=selected_execution_agent_id,
        execution_target_label=agent_labels.get(selected_execution_agent_id, selected_execution_agent_id),
        auto_operation_profile=auto_operation_profile,
        dispatch_validation=validation,
        requested_lead_agent_id=requested_lead_agent_id,
    )
    routing_decision = store_save_routing_decision(
        openclaw_dir,
        {
            "taskId": task_id,
            "taskTitle": title,
            "policyId": decision.get("policyId", ""),
            "policyName": decision.get("policyName", ""),
            "workflowId": workflow_resolution.get("binding", {}).get("workflowId", ""),
            "workflowVersionId": workflow_resolution.get("binding", {}).get("workflowVersionId", ""),
            "strategyType": decision.get("strategyType", ""),
            "matchedKeyword": decision.get("matchedKeyword", ""),
            "queueName": decision.get("queueName", ""),
            "priorityLevel": decision.get("priorityLevel", "normal"),
            "targetAgentId": decision.get("targetAgentId", ""),
            "sourceText": decision.get("sourceText", ""),
            "meta": {
                "reason": decision.get("reason", ""),
                "fallback": bool(decision.get("fallback")),
                "targetAgentLabel": decision.get("targetAgentLabel", ""),
                "workflowName": workflow_resolution.get("binding", {}).get("workflowName", ""),
                "selectedBranch": workflow_resolution.get("binding", {}).get("selectedBranch", {}),
                "planningBundle": planning_bundle,
                "intelligence": decision.get("intelligence", {}),
                "decisionSource": ((decision.get("intelligence") or {}).get("decisionSource") if isinstance(decision.get("intelligence"), dict) else "") or "heuristic",
                "modelDecision": ((decision.get("intelligence") or {}).get("modelDecision") if isinstance(decision.get("intelligence"), dict) and isinstance((decision.get("intelligence") or {}).get("modelDecision"), dict) else {}),
                "trace": decision.get("trace", []),
                "manualReview": bool(((decision.get("intelligence") or {}).get("manualReview")) or team_selection_payload.get("manualReviewRecommended")),
                "confidence": (decision.get("intelligence") or {}).get("confidence", 0),
                "teamSelection": team_selection_payload,
                "dispatchValidation": validation,
                **(
                    {"autoOperationProfile": compact_auto_operation_profile(auto_operation_profile)}
                    if compact_auto_operation_profile(auto_operation_profile)
                    else {}
                ),
                "taskLongTermMemory": task_long_term_memory,
            },
        },
    )
    if team:
        patch_task_team_assignment_metadata(
            openclaw_dir,
            task_id,
            team,
            router_agent_id=router_agent_id,
        )
        team = link_task_to_agent_team(openclaw_dir, team, task_id)
    run = {}
    if has_blocking_task_dispatch_validation(validation):
        session_id = task_execution_session_id(task_id)
        blocking_note = str(validation.get("summary") or "任务缺少必要信息，暂未进入自动派发。").strip()
        update_task_execution_bootstrap(
            openclaw_dir,
            task_id,
            selected_execution_agent_id or router_agent_id,
            "pending_info",
            note=blocking_note,
            session_id=session_id,
            router_agent_id=router_agent_id,
            title=title,
        )
        execution = {
            "scheduled": False,
            "reason": "dispatch_preflight_blocked",
            "sessionId": session_id,
            "agentId": selected_execution_agent_id or router_agent_id,
            "agentLabel": agent_labels.get(selected_execution_agent_id or router_agent_id, selected_execution_agent_id or router_agent_id),
            "teamId": str((team or {}).get("id") or "").strip(),
            "teamName": str((team or {}).get("name") or "").strip(),
            "threadId": "",
            "bootstrap": {
                "status": "pending_info",
                "agentId": selected_execution_agent_id or router_agent_id,
                "agentLabel": agent_labels.get(selected_execution_agent_id or router_agent_id, selected_execution_agent_id or router_agent_id),
                "sessionId": session_id,
                "note": blocking_note,
                "attempts": 1,
            },
            "coordinationProtocol": task_coordination_protocol_snapshot(team) if isinstance(team, dict) and team.get("id") else {},
            "autoOperationMode": automation_mode,
            "validation": validation,
        }
    else:
        run = create_management_run_for_task(
            openclaw_dir,
            task_id=task_id,
            title=title,
            remark=remark,
            workflow_binding=workflow_resolution.get("binding", {}),
            team=team,
            target_agent_id=selected_execution_agent_id,
            owner=run_owner,
            risk_level=((decision.get("intelligence") or {}).get("riskLevel") if isinstance(decision.get("intelligence"), dict) else ""),
        )
        if automation_mode == MANAGEMENT_AUTOMATION_MODE_FULL_AUTO:
            execution = _start_task_execution_dispatch(
                openclaw_dir,
                task_id,
                title,
                remark=remark,
                target_agent_id=selected_execution_agent_id,
                workflow_binding=workflow_resolution.get("binding", {}),
                router_agent_id=router_agent_id,
                team=team,
                linked_run_id=str(run.get("id") or "").strip(),
                linked_run_title=str(run.get("title") or "").strip(),
                auto_operation_profile=auto_operation_profile,
            )
        else:
            session_id = task_execution_session_id(task_id)
            team_name = str((team or {}).get("name") or "").strip()
            operation_hint = (
                " 这是长期经营任务，先沉淀今日经营判断、推进动作和复盘，再决定何时启动。"
                if compact_auto_operation_profile(auto_operation_profile)
                else ""
            )
            if automation_mode == MANAGEMENT_AUTOMATION_MODE_ASSISTIVE:
                execution_status = "assistive_queue"
                note = (
                    f"任务已建好，当前为协作辅助模式，等待你确认后再由 {team_name or selected_execution_agent_id} 开始推进。{operation_hint}".strip()
                )
            else:
                execution_status = "manual_queue"
                note = (
                    f"任务已建好，当前为手动观察模式，先不自动开工；等你确认后再由 {team_name or selected_execution_agent_id} 接手。{operation_hint}".strip()
                )
            update_task_execution_bootstrap(
                openclaw_dir,
                task_id,
                selected_execution_agent_id,
                execution_status,
                note=note,
                session_id=session_id,
                router_agent_id=router_agent_id,
                title=title,
            )
            execution = {
                "scheduled": False,
                "reason": f"management_mode_{automation_mode}",
                "sessionId": session_id,
                "agentId": selected_execution_agent_id,
                "agentLabel": agent_labels.get(selected_execution_agent_id, selected_execution_agent_id),
                "teamId": str((team or {}).get("id") or "").strip(),
                "teamName": team_name,
                "threadId": "",
                "bootstrap": {
                    "status": execution_status,
                    "agentId": selected_execution_agent_id,
                    "agentLabel": agent_labels.get(selected_execution_agent_id, selected_execution_agent_id),
                    "sessionId": session_id,
                    "note": note,
                    "attempts": 0,
                },
                "coordinationProtocol": task_coordination_protocol_snapshot(team) if isinstance(team, dict) and team.get("id") else {},
                "autoOperationMode": automation_mode,
            }
    return {
        "taskId": task_id,
        "routingDecision": routing_decision,
        "workflowBinding": workflow_resolution.get("binding", {}),
        "planningBundle": planning_bundle,
        "execution": execution,
        "team": deepcopy(team) if team else None,
        "teamSelection": team_selection_payload,
        "run": run,
        "deduped": False,
        "duplicateOfTaskId": "",
        "validation": validation,
    }


def build_task_team_sync_message(task, team=None, note="", linked_run_id="", linked_run_title=""):
    task = task if isinstance(task, dict) else {}
    team = team if isinstance(team, dict) else {}
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if not route_meta and isinstance(task.get("meta"), dict):
        route_meta = (task.get("meta") or {}).get("routeDecision", {})
    last_dispatch = route_meta.get("teamDispatch") if isinstance(route_meta.get("teamDispatch"), dict) else {}
    task_id = str(task.get("id") or "").strip()
    title = str(task.get("title") or task_id).strip() or task_id
    team_name = str(team.get("name") or task.get("teamLabel") or "").strip()
    run_label = str(linked_run_title or linked_run_id or (task.get("linkedRun") or {}).get("title") or task.get("linkedRunId") or "").strip()
    lead_label = str(task.get("currentAgentLabel") or task.get("targetAgentLabel") or "").strip()
    current_update = str(task.get("currentUpdate") or task.get("now") or "").strip()
    task_long_term_memory = compact_task_long_term_memory(route_meta.get("taskLongTermMemory"))
    parts = [
        f"团队协作同步：我把 {task_id} 这一轮的讨论先收一下，大家继续接着往前聊：",
        f"任务标题：{title}",
    ]
    if team_name:
        parts.append(f"当前小组：{team_name}")
    if run_label:
        parts.append(f"协同 Run：{run_label}")
    if lead_label:
        parts.append(f"现在在牵头的人：{lead_label}")
    if current_update:
        parts.append(f"刚推进到：{current_update}")
    if note:
        parts.append(f"这一轮重点：{note}")
    response_entries = coordination_reply_entries(last_dispatch.get("responses"), limit=4)
    if response_entries:
        parts.append("群里刚刚已经有人提到：")
        parts.extend(f"- {item['agentDisplayName']}：{item['replyPreview']}" for item in response_entries)
    parts.extend(task_long_term_memory_prompt_lines(task_long_term_memory, audience="member"))
    protocol_lines = task_coordination_prompt_lines(team, audience="member")
    if protocol_lines:
        parts.extend(protocol_lines)
    parts.append("请先回应你理解到的团队当前判断，或者直接接住上一位同事提到的依赖，再补你的动作。")
    parts.append("这次请每位成员直接说清三件事：")
    parts.append("- 你现在接住的是哪一段")
    parts.append("- 你接下来 30-60 分钟先推进什么")
    parts.append("- 你需要谁配合，或者现在卡在哪")
    parts.append("如果上一位已经把你的判断说到了，就不要重复铺陈，先说你认同哪一点，再补你自己的新增动作。")
    parts.append("如果你暂时不介入，请明确回复 STANDBY，并说明什么条件触发你接手。")
    parts.append("如果你能接住别人的依赖，请直接说你来处理，不要等 lead 二次分配。")
    parts.append("请保持真实同事协作语气，避免只回收到、已阅，也不要像在写系统状态。")
    return "\n".join(parts)


def perform_task_team_sync(
    openclaw_dir,
    task,
    team=None,
    note="",
    requester_label="OpenClaw Team",
    router_agent_id="",
):
    task = task if isinstance(task, dict) else {}
    team = team if isinstance(team, dict) else {}
    task_id = str(task.get("id") or "").strip()
    if not task_id:
        raise RuntimeError("需要 taskId。")
    team_id = str(team.get("id") or task.get("teamId") or "").strip()
    if not team_id:
        raise RuntimeError("当前任务还没有绑定 Team。")
    if not team or not team.get("id"):
        team = resolve_agent_team_record(openclaw_dir, team_id) or {}
    if not team or not team.get("id"):
        raise RuntimeError("没有找到这条任务绑定的 Team。")
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    linked_run = latest_management_run_for_task(openclaw_dir, task_id)
    linked_run_id = str((linked_run or {}).get("id") or task.get("linkedRunId") or "").strip()
    linked_run_title = str((linked_run or {}).get("title") or ((task.get("linkedRun") or {}).get("title")) or "").strip()
    lead_agent_id = (
        str(task_execution_bootstrap_for_task(task).get("agentId") or "").strip()
        or str(task.get("currentAgent") or "").strip()
        or str(task.get("targetAgentId") or "").strip()
        or str(team.get("leadAgentId") or "").strip()
    )
    participants = task_team_participant_agent_ids(team, lead_agent_id=lead_agent_id)
    if not participants:
        raise RuntimeError("当前 Team 还没有可同步的成员。")
    thread = ensure_task_execution_team_thread(
        openclaw_dir,
        task_id,
        str(task.get("title") or task_id).strip() or task_id,
        team,
        lead_agent_id=lead_agent_id,
        linked_run_id=linked_run_id,
    )
    thread_meta = deepcopy(thread.get("meta", {})) if isinstance(thread.get("meta"), dict) else {}
    sessions_by_agent = thread_meta.get("sessionsByAgent") if isinstance(thread_meta.get("sessionsByAgent"), dict) else {}
    team_policy = merge_team_policy_state(team, thread_meta.get("teamPolicy"))
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if not route_meta and isinstance(task.get("meta"), dict):
        route_meta = (task.get("meta") or {}).get("routeDecision", {})
    last_dispatch = (
        thread_meta.get("lastDispatch")
        if isinstance(thread_meta.get("lastDispatch"), dict)
        else (route_meta.get("teamDispatch") if isinstance(route_meta.get("teamDispatch"), dict) else {})
    )
    memory_trace_meta = team_memory_trace_payload(team_policy)
    state_packet_meta = team_state_packet_payload(team_policy)
    sync_message = build_task_team_sync_message(
        task,
        team=team,
        note=note,
        linked_run_id=linked_run_id,
        linked_run_title=linked_run_title,
    )
    outbound_message = store_save_chat_message(
        openclaw_dir,
        {
            "threadId": thread.get("id", ""),
            "senderKind": "system",
            "senderId": "task-team-sync",
            "senderLabel": requester_label,
            "direction": "system",
            "body": sync_message,
            "meta": {
                "taskId": task_id,
                "teamId": team_id,
                "manualTeamSync": True,
                "note": note,
                "dispatchAgentIds": participants,
                **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
            },
        },
    )
    responses = []
    failures = []
    sync_target_ids = select_human_turn_targets(
        openclaw_dir,
        participants,
        purpose="sync",
        lead_agent_id=lead_agent_id,
        responses=(last_dispatch.get("responses") if isinstance(last_dispatch, dict) else []),
        config=config,
        metadata=metadata,
        agents=agents,
    )
    sync_turn_anchors = {
        agent_id: build_human_turn_anchor_payload(
            openclaw_dir,
            agent_id,
            turn_index=index,
            ordered_agent_ids=sync_target_ids,
            prior_responses=(last_dispatch.get("responses") if isinstance(last_dispatch, dict) else []),
            lead_agent_id=lead_agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        for index, agent_id in enumerate(sync_target_ids)
    }
    sync_turn_profiles = {
        agent_id: build_human_turn_profile_payload(
            openclaw_dir,
            agent_id,
            turn_index=index,
            participant_count=len(sync_target_ids),
            lead_agent_id=lead_agent_id,
            prior_responses=(last_dispatch.get("responses") if isinstance(last_dispatch, dict) else []),
            config=config,
            metadata=metadata,
            agents=agents,
        )
        for index, agent_id in enumerate(sync_target_ids)
    }
    sync_requests = apply_conversation_fanout_stagger(
        [
            {
                "agentId": agent_id,
                "sessionId": str(sessions_by_agent.get(agent_id, "") or "").strip(),
                "message": apply_turn_guidance_to_message(
                    sync_message,
                    turn_index=index,
                    participant_count=len(sync_target_ids),
                    anchor=sync_turn_anchors.get(agent_id),
                    turn_profile=sync_turn_profiles.get(agent_id),
                ),
                "thinking": "low",
            }
            for index, agent_id in enumerate(sync_target_ids)
        ],
        stagger_seconds=TEAM_CONVERSATION_STAGGER_SECONDS,
    )
    sync_results = perform_conversation_fanout(
        openclaw_dir,
        sync_requests,
        default_thinking="low",
        timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
        max_workers=TEAM_CONVERSATION_MAX_PARALLEL,
    )
    for failure in sync_results["failures"]:
        agent_id = str(failure.get("agentId") or "").strip()
        error_message = str(failure.get("error") or "团队协作同步失败。").strip()
        failures.append({"agentId": agent_id, "error": error_message})
        store_save_chat_message(
            openclaw_dir,
            {
                "threadId": thread.get("id", ""),
                "senderKind": "system",
                "senderId": "task-team-sync",
                "senderLabel": requester_label,
                "direction": "system",
                "body": f"{agent_id} 暂未同步本轮判断：{error_message}",
                "meta": {
                    "manualTeamSync": True,
                    "agentId": agent_id,
                    "replyToMessageId": outbound_message.get("id", ""),
                },
            },
        )
    for success in sync_results["successes"]:
        agent_id = str(success.get("agentId") or "").strip()
        actual_session_id = select_persistable_conversation_session_id(
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
        agent_identity = agent_runtime_identity_payload(
            openclaw_dir,
            agent_id,
            config=config,
            metadata=metadata,
            agents=agents,
        )
        response_payload = {
            "agentId": agent_id,
            "sessionId": actual_session_id,
            "replyPreview": reply_preview,
            "source": "manual_sync",
            "agentDisplayName": agent_identity.get("displayName", ""),
            "agentHumanName": agent_identity.get("humanName", ""),
            "agentJobTitle": agent_identity.get("jobTitle", ""),
        }
        responses.append(response_payload)
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
                        "replyToMessageId": outbound_message.get("id", ""),
                        "manualTeamSync": True,
                        "agentHumanName": agent_identity.get("humanName", ""),
                        "agentJobTitle": agent_identity.get("jobTitle", ""),
                        "turnFocus": (sync_turn_profiles.get(agent_id) or {}).get("turnFocus", ""),
                        "turnPace": (sync_turn_profiles.get(agent_id) or {}).get("turnPace", ""),
                        **(sync_turn_anchors.get(agent_id) or {}),
                        **({"coordinationState": state_packet_meta} if state_packet_meta else {}),
                        **memory_trace_meta,
                    },
                },
            )
            response_payload["messageId"] = saved_message.get("id", "")
    responded_agent_ids = [item.get("agentId") for item in responses if item.get("agentId")]
    sync_summary = {
        "at": now_iso(),
        "taskTitle": str(task.get("title") or task_id).strip() or task_id,
        "teamId": team_id,
        "teamName": str(team.get("name") or "").strip(),
        "threadId": thread.get("id", ""),
        "leadAgentId": lead_agent_id,
        "participantAgentIds": participants,
        "dispatchAgentIds": participants,
        "respondedAgentIds": responded_agent_ids,
        "responses": responses,
        "failedAgents": failures,
        "replyCount": len(responses),
        "summaryText": f"已发起一次团队协作同步；当前已回执 {len(responded_agent_ids)}/{len(participants)} 人。",
        "coordinationProtocol": task_coordination_protocol_snapshot(team),
        "syncType": "manual_sync",
        "requestedBy": requester_label,
        "note": note,
    }
    if isinstance(last_dispatch.get("internalDiscussion"), dict) and last_dispatch.get("internalDiscussion"):
        sync_summary["internalDiscussion"] = deepcopy(last_dispatch.get("internalDiscussion"))
    coordination_relay = _svc().relay_team_coordination_updates(
        openclaw_dir,
        thread,
        team=team,
        participant_agent_ids=participants,
        sessions_by_agent=sessions_by_agent,
        responses=responses,
        context_label=f"任务 {task_id} · {str(task.get('title') or task_id).strip() or task_id}",
        thinking="low",
        timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
        sender_label="Task Coordination",
        force_targets=True,
    )
    if coordination_relay.get("sent"):
        sync_summary["coordinationRelay"] = coordination_relay
    sync_summary["collaboration"] = build_team_collaboration_summary(sync_summary)
    next_team_policy = apply_team_working_memory(team_policy, sync_summary)
    sync_summary["taskLongTermMemory"] = compact_task_long_term_memory(next_team_policy.get("taskLongTermMemory"))
    updated_thread = store_save_chat_thread(
        openclaw_dir,
        {
            **thread,
            "status": "waiting_internal" if responses else ("blocked" if failures else thread.get("status", "open")),
            "updatedAt": sync_summary["at"],
            "meta": {
                **thread_meta,
                "teamId": team_id,
                "dispatchMode": "broadcast",
                "lastDispatch": sync_summary,
                "lastSync": sync_summary,
                "sessionsByAgent": sessions_by_agent,
                "teamPolicy": next_team_policy,
                "coordinationRelay": coordination_relay if coordination_relay.get("sent") else thread_meta.get("coordinationRelay", {}),
                "coordinationRelayPolicy": (
                    thread_meta.get("coordinationRelayPolicy")
                    if isinstance(thread_meta.get("coordinationRelayPolicy"), dict)
                    else {"enabled": True, "strategy": "broadcast_summary"}
                ),
            },
        },
    )
    sync_summary["threadId"] = updated_thread.get("id", "") or sync_summary["threadId"]
    update_task_team_dispatch_state(
        openclaw_dir,
        task_id,
        sync_summary,
        router_agent_id=router_agent_id,
    )
    return {
        "thread": updated_thread,
        "dispatch": sync_summary,
        "team": team,
    }
