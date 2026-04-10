from __future__ import annotations

from . import orchestration_core as core
from . import orchestration_dispatch_core as dispatch_core


_SHARED_NAMES = [
    "TASK_EXECUTION_REPAIR_BATCH_SIZE",
    "TASK_EXECUTION_REPAIR_DISPATCHED_MINUTES",
    "TASK_EXECUTION_REPAIR_FAILED_MINUTES",
    "TASK_EXECUTION_REPAIR_MAX_ATTEMPTS",
    "TASK_EXECUTION_REPAIR_RESET_MINUTES",
    "TASK_EXECUTION_REPAIR_SCHEDULED_MINUTES",
    "TERMINAL_STATES",
    "_svc",
    "append_audit_event",
    "current_agent_for_task",
    "datetime",
    "get_router_agent_id",
    "latest_management_run_for_task",
    "load_config",
    "load_kanban_config",
    "logging",
    "merge_tasks",
    "now_utc",
    "parse_iso",
    "reset_task_execution_bootstrap",
    "resolve_agent_team_record",
    "task_execution_bootstrap_for_task",
    "task_execution_session_id",
    "task_execution_transcript_health",
    "task_has_meaningful_progress",
    "timezone",
]

globals().update({name: getattr(core, name) for name in _SHARED_NAMES})


def _start_task_execution_dispatch(*args, **kwargs):
    patched = getattr(_svc(), "start_task_execution_dispatch")
    if patched is dispatch_core.start_task_execution_dispatch:
        return dispatch_core.start_task_execution_dispatch(*args, **kwargs)
    return patched(*args, **kwargs)


def repair_task_execution_backlog(openclaw_dir, config=None, tasks=None, now=None):
    config = config or load_config(openclaw_dir)
    now = now or now_utc()
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    task_items = sorted(
        list(tasks if isinstance(tasks, list) else merge_tasks(openclaw_dir, config)),
        key=lambda item: parse_iso((item or {}).get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
    )
    repaired = []
    skipped = []

    for task in task_items:
        if not isinstance(task, dict):
            continue
        if len(repaired) >= TASK_EXECUTION_REPAIR_BATCH_SIZE:
            skipped.append({"taskId": str(task.get("id") or "").strip(), "reason": "batch_limit"})
            continue
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES or task.get("output"):
            continue
        target_agent_id = current_agent_for_task(task, kanban_cfg, router_agent_id)
        if not target_agent_id:
            skipped.append({"taskId": task_id, "reason": "missing_target_agent"})
            continue
        bootstrap = task_execution_bootstrap_for_task(task)
        team_assignment = (
            ((task.get("meta") or {}).get("routeDecision") or {}).get("teamAssignment")
            if isinstance((task.get("meta") or {}).get("routeDecision"), dict)
            else {}
        )
        if not isinstance(team_assignment, dict) or not team_assignment:
            team_assignment = task.get("routeDecision", {}).get("teamAssignment") if isinstance(task.get("routeDecision"), dict) else {}
        task_team = resolve_agent_team_record(
            openclaw_dir,
            str(team_assignment.get("teamId") or task.get("teamId") or "").strip(),
        ) if isinstance(team_assignment, dict) else None
        bootstrap = task_execution_bootstrap_for_task(task)
        attempts = int(bootstrap.get("attempts") or 0)
        session_id = str(bootstrap.get("sessionId") or task_execution_session_id(task_id)).strip() or task_execution_session_id(task_id)
        transcript = task_execution_transcript_health(openclaw_dir, target_agent_id, session_id)
        meaningful_progress = task_has_meaningful_progress(task, bootstrap.get("at"))
        if meaningful_progress or transcript["assistantMessages"] > 0:
            continue

        reason = ""
        last_at = parse_iso(bootstrap.get("at")) or parse_iso(task.get("updatedAt"))
        stale_minutes = max(0, int((now - last_at).total_seconds() / 60)) if last_at else TASK_EXECUTION_REPAIR_DISPATCHED_MINUTES + 1

        if attempts >= TASK_EXECUTION_REPAIR_MAX_ATTEMPTS:
            if stale_minutes < TASK_EXECUTION_REPAIR_RESET_MINUTES:
                skipped.append({"taskId": task_id, "reason": "retry_limit"})
                continue
            reset_task_execution_bootstrap(openclaw_dir, task_id, router_agent_id=router_agent_id)
            bootstrap = {"attempts": 0, "status": "scheduled"}
            attempts = 0
            append_audit_event(
                openclaw_dir,
                "task_execution_repair_reset",
                "system",
                detail=f"执行引导重试计数已重置：{task_id}",
                meta={"taskId": task_id, "targetAgentId": target_agent_id, "staleMinutes": stale_minutes},
            )

        if not bootstrap:
            reason = "任务已创建但还没有执行引导，自动补发执行指令。"
        else:
            status = str(bootstrap.get("status") or "").strip().lower()
            if status == "failed" and stale_minutes >= TASK_EXECUTION_REPAIR_FAILED_MINUTES:
                reason = "上次执行派发失败，按冷却时间自动重试。"
            elif status == "scheduled" and stale_minutes >= TASK_EXECUTION_REPAIR_SCHEDULED_MINUTES:
                reason = "任务仍停留在待派发，自动重新补发执行指令。"
            elif status == "dispatched" and stale_minutes >= TASK_EXECUTION_REPAIR_DISPATCHED_MINUTES:
                reason = "任务已派发但长时间没有新进展，自动重新提醒接手 Agent。"
            else:
                skipped.append({"taskId": task_id, "reason": "cooldown"})
                continue

        linked_run = latest_management_run_for_task(openclaw_dir, task_id)
        execution = _start_task_execution_dispatch(
            openclaw_dir,
            task_id,
            str(task.get("title") or task_id),
            remark=str(task.get("currentUpdate") or task.get("now") or "").strip(),
            target_agent_id=target_agent_id,
            workflow_binding=task.get("workflowBinding", {}),
            router_agent_id=router_agent_id,
            repair_reason=reason,
            team=task_team,
            promote_success_status=True,
            linked_run_id=str((linked_run or {}).get("id") or "").strip(),
            linked_run_title=str((linked_run or {}).get("title") or "").strip(),
        )
        if not execution or not execution.get("scheduled"):
            logging.warning("repair_task_execution_backlog: dispatch did not schedule for task %s", task_id)
            skipped.append({"taskId": task_id, "reason": "dispatch_not_scheduled"})
            continue
        append_audit_event(
            openclaw_dir,
            "task_execution_repair",
            "system",
            detail=f"自动修复执行链路：{task_id}",
            meta={
                "taskId": task_id,
                "targetAgentId": target_agent_id,
                "reason": reason,
                "attempts": int(((execution.get("bootstrap") or {}).get("attempts")) or attempts + 1),
            },
        )
        repaired.append(
            {
                "taskId": task_id,
                "title": str(task.get("title") or task_id),
                "targetAgentId": target_agent_id,
                "reason": reason,
                "attempts": int(((execution.get("bootstrap") or {}).get("attempts")) or attempts + 1),
                "status": "scheduled",
            }
        )

    return {
        "count": len(repaired),
        "items": repaired[:12],
        "skippedCount": len(skipped),
        "skipped": skipped[:12],
    }
