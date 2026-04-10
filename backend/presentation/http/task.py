#!/usr/bin/env python3
"""Task command dispatcher extracted from collaboration_dashboard."""

from __future__ import annotations

from .aliases import task_action_kind


def handle_task_action_post(handler, path, payload, services):
    action = task_action_kind(path)
    if not action:
        return False

    if action == "create":
        if not handler._require_capability("taskWrite", "当前账号没有创建或推进任务的权限。"):
            return True
        title = str(payload.get("title", "")).strip()
        remark = str(payload.get("remark", "")).strip()
        team_id = str(payload.get("teamId", "")).strip()
        lead_agent_id = str(payload.get("leadAgentId", "")).strip()
        team_override_reason = str(payload.get("teamOverrideReason", "")).strip()
        prefer_fast_routing = bool(payload.get("preferFastRouting"))
        if not title:
            raise RuntimeError("任务标题不能为空。")
        task_result = services["perform_task_create"](
            handler.server.openclaw_dir,
            title,
            remark=remark,
            preferred_team_id=team_id,
            team_override_reason=team_override_reason,
            run_owner=services["session_for_client"](handler._session()).get("displayName", "OpenClaw Team"),
            prefer_fast_routing=prefer_fast_routing,
            requested_lead_agent_id=lead_agent_id,
        )
        task_id = task_result["taskId"]
        handler._audit("task_create", detail=f"创建任务 {task_id}", meta={"taskId": task_id, "title": title})
        services["invalidate_dashboard_bundle_cache"](handler.server.openclaw_dir, handler.server.output_dir)
        services["invalidate_management_payload_cache"](handler.server.openclaw_dir)
        services["invalidate_dashboard_bundle_cache"](handler.server.openclaw_dir, handler.server.output_dir, force_sync=False)
        run = task_result.get("run") if isinstance(task_result.get("run"), dict) else {}
        deduped = bool(task_result.get("deduped"))
        response_payload = {
            "ok": True,
            "message": (
                f"检测到相同任务仍在协同中，已复用现有任务 {task_id}。"
                if deduped
                else (
                    f"任务 {task_id} 已创建，但因缺少必要信息暂未进入自动派发。"
                    if (task_result.get("validation") or {}).get("blocking")
                    else (
                        f"任务 {task_id} 已创建，Run {run.get('id', '')} 已建立，并已派发给执行 Agent。"
                        if (task_result.get("execution") or {}).get("scheduled")
                        else f"任务 {task_id} 已创建，Run {run.get('id', '')} 已建立，并已进入当前协同链路。"
                    )
                )
            ),
            "taskId": task_id,
            "routingDecision": task_result.get("routingDecision", {}),
            "workflowBinding": task_result.get("workflowBinding", {}),
            "planningBundle": task_result.get("planningBundle", {}),
            "execution": task_result.get("execution", {}),
            "team": task_result.get("team"),
            "teamSelection": task_result.get("teamSelection", {}),
            "run": run,
            "deduped": deduped,
            "duplicateOfTaskId": str(task_result.get("duplicateOfTaskId") or "").strip(),
            "validation": task_result.get("validation", {}),
        }
        handler._send_json(response_payload)
        return True

    if action == "preview":
        if not handler._require_capability("taskWrite", "当前账号没有创建或推进任务的权限。"):
            return True
        title = str(payload.get("title", "")).strip()
        remark = str(payload.get("remark", "")).strip()
        team_id = str(payload.get("teamId", "")).strip()
        lead_agent_id = str(payload.get("leadAgentId", "")).strip()
        team_override_reason = str(payload.get("teamOverrideReason", "")).strip()
        if not title:
            raise RuntimeError("任务标题不能为空。")
        preview = services["perform_task_preview"](
            handler.server.openclaw_dir,
            title,
            remark=remark,
            preferred_team_id=team_id,
            team_override_reason=team_override_reason,
            requested_lead_agent_id=lead_agent_id,
        )
        handler._send_json({"ok": True, **preview})
        return True

    if action == "progress":
        if not handler._require_capability("taskWrite", "当前账号没有推进任务的权限。"):
            return True
        task_id = str(payload.get("taskId", "")).strip()
        message = str(payload.get("message", "")).strip()
        todos = str(payload.get("todos", "")).strip()
        mark_doing = bool(payload.get("markDoing"))
        if not task_id or not message:
            raise RuntimeError("任务编号和进展内容都不能为空。")
        services["perform_task_progress"](handler.server.openclaw_dir, task_id, message, todos=todos, mark_doing=mark_doing)
        handler._audit("task_progress", detail=f"同步任务 {task_id} 的进展", meta={"taskId": task_id, "markDoing": mark_doing})
        response_payload = {"ok": True, "message": f"任务 {task_id} 的最新进展已经同步。", "taskId": task_id}
        handler._send_json(response_payload)
        return True

    if action == "block":
        if not handler._require_capability("taskWrite", "当前账号没有标记阻塞的权限。"):
            return True
        task_id = str(payload.get("taskId", "")).strip()
        reason = str(payload.get("reason", "")).strip()
        if not task_id or not reason:
            raise RuntimeError("请提供任务编号和阻塞原因。")
        services["perform_task_block"](handler.server.openclaw_dir, task_id, reason)
        handler._audit("task_block", detail=f"标记任务 {task_id} 阻塞", meta={"taskId": task_id})
        response_payload = {"ok": True, "message": f"任务 {task_id} 已标记为阻塞。", "taskId": task_id}
        handler._send_json(response_payload)
        return True

    if action == "done":
        if not handler._require_capability("taskWrite", "当前账号没有完成任务的权限。"):
            return True
        task_id = str(payload.get("taskId", "")).strip()
        summary = str(payload.get("summary", "")).strip()
        output_path = str(payload.get("output", "")).strip()
        if not task_id:
            raise RuntimeError("请提供任务编号。")
        services["perform_task_done"](handler.server.openclaw_dir, task_id, output_path=output_path, summary=summary)
        handler._audit("task_done", detail=f"完成任务 {task_id}", meta={"taskId": task_id, "output": output_path})
        response_payload = {"ok": True, "message": f"任务 {task_id} 已完成并归档到交付列表。", "taskId": task_id}
        handler._send_json(response_payload)
        return True

    if action == "assign":
        if not handler._require_capability("taskWrite", "当前账号没有重新派发任务的权限。"):
            return True
        task_id = str(payload.get("taskId", "")).strip()
        target_agent_id = str(payload.get("targetAgentId", "")).strip()
        team_id = str(payload.get("teamId", "")).strip()
        note = str(payload.get("note", "")).strip()
        team = services["resolve_agent_team_record"](handler.server.openclaw_dir, team_id) if team_id else None
        if team and not target_agent_id:
            target_agent_id = str(team.get("leadAgentId") or "").strip()
        if not task_id or not target_agent_id:
            raise RuntimeError("请提供任务编号和目标 Agent。")
        current_tasks = services["merge_tasks"](handler.server.openclaw_dir, services["load_config"](handler.server.openclaw_dir))
        task = next((item for item in current_tasks if item.get("id") == task_id), None)
        if not task:
            raise RuntimeError(f"没有找到任务 {task_id}。")
        linked_run = services["latest_management_run_for_task"](handler.server.openclaw_dir, task_id)
        execution = services["start_task_execution_dispatch"](
            handler.server.openclaw_dir,
            task_id=task_id,
            title=str(task.get("title") or task_id),
            remark=note or str(task.get("currentUpdate") or task.get("now") or "").strip(),
            target_agent_id=target_agent_id,
            workflow_binding=task.get("workflowBinding", {}),
            router_agent_id=services["get_router_agent_id"](services["load_config"](handler.server.openclaw_dir)),
            team=team,
            linked_run_id=str((linked_run or {}).get("id") or "").strip(),
            linked_run_title=str((linked_run or {}).get("title") or "").strip(),
        )
        if team:
            services["patch_task_team_assignment_metadata"](
                handler.server.openclaw_dir,
                task_id,
                team,
                router_agent_id=services["get_router_agent_id"](services["load_config"](handler.server.openclaw_dir)),
            )
            team = services["link_task_to_agent_team"](handler.server.openclaw_dir, team, task_id)
        handler._audit(
            "task_assign",
            detail=f"将任务 {task_id} 派发给 {team.get('name') if team else target_agent_id}",
            meta={"taskId": task_id, "targetAgentId": target_agent_id, "teamId": team.get("id", "") if team else ""},
        )
        response_payload = {
            "ok": True,
            "message": (
                f"任务 {task_id} 已创建派发，但当前校验未通过，暂未进入自动执行。"
                if not (execution or {}).get("scheduled") and (execution or {}).get("reason") == "dispatch_preflight_blocked"
                else (
                    f"任务 {task_id} 已派发给 Team {team.get('name', '')}，由 {target_agent_id} 牵头。"
                    if team
                    else f"任务 {task_id} 已派发给 {target_agent_id}。"
                )
            ),
            "taskId": task_id,
            "targetAgentId": target_agent_id,
            "teamId": team.get("id", "") if team else "",
            "execution": execution,
            "validation": (execution or {}).get("validation", {}),
        }
        handler._send_json(response_payload)
        return True

    if action == "team-sync":
        if not handler._require_capability("taskWrite", "当前账号没有发起团队协作同步的权限。"):
            return True
        task_id = str(payload.get("taskId", "")).strip()
        note = str(payload.get("note", "")).strip()
        if not task_id:
            raise RuntimeError("请提供任务编号。")
        config = services["load_config"](handler.server.openclaw_dir)
        current_tasks = services["merge_tasks"](handler.server.openclaw_dir, config)
        task = next((item for item in current_tasks if item.get("id") == task_id), None)
        if not task:
            raise RuntimeError(f"没有找到任务 {task_id}。")
        route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
        if not route_meta and isinstance(task.get("meta"), dict):
            route_meta = (task.get("meta") or {}).get("routeDecision", {})
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = (
            str(team_assignment.get("teamId") or "").strip()
            or str(task.get("teamId") or "").strip()
            or str(route_meta.get("teamId") or "").strip()
        )
        team = services["resolve_agent_team_record"](handler.server.openclaw_dir, team_id) if team_id else None
        sync_result = services["perform_task_team_sync"](
            handler.server.openclaw_dir,
            task,
            team=team,
            note=note,
            requester_label=services["session_for_client"](handler._session()).get("displayName", "OpenClaw Team"),
            router_agent_id=services["get_router_agent_id"](config),
        )
        handler._audit(
            "task_team_sync",
            detail=f"发起任务 {task_id} 的团队协作同步",
            meta={
                "taskId": task_id,
                "teamId": (sync_result.get("team") or {}).get("id", ""),
                "threadId": (sync_result.get("dispatch") or {}).get("threadId", ""),
                "replyCount": (sync_result.get("dispatch") or {}).get("replyCount", 0),
            },
        )
        handler._send_json(
            {
                "ok": True,
                "message": f"任务 {task_id} 已重新发起团队协作同步。",
                "taskId": task_id,
                "team": sync_result.get("team"),
                "sync": sync_result.get("dispatch", {}),
            }
        )
        return True

    return False
