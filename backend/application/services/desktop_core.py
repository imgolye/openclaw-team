from __future__ import annotations

import sys
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone

from backend.application.services import chat_core as chat_context_core


TERMINAL_DESKTOP_TASK_STATES = {"done", "completed", "complete", "closed", "archived"}
TODO_DESKTOP_TASK_STATES = {"intake", "planning", "qualityreview", "pending", "taizi", "zhongshu", "menxia"}
DOING_DESKTOP_TASK_STATES = {"assigned", "doing", "next", "in_progress", "inprogress", "active"}
REVIEW_DESKTOP_TASK_STATES = {"review"}


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
def _desktop_task_team_id(task):
    task = task if isinstance(task, dict) else {}
    route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    team_selection = route.get("teamSelection") if isinstance(route.get("teamSelection"), dict) else {}
    for candidate in (
        team_selection.get("selectedTeamId"),
        task.get("teamId"),
        team_selection.get("recommendedTeamId"),
    ):
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    return ""


def _desktop_task_team_label(task):
    task = task if isinstance(task, dict) else {}
    route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    team_selection = route.get("teamSelection") if isinstance(route.get("teamSelection"), dict) else {}
    for candidate in (
        team_selection.get("selectedTeamName"),
        task.get("teamLabel"),
        team_selection.get("recommendedTeamName"),
    ):
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    return ""


def _desktop_task_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    now = now or svc.now_utc()

    def build():
        items = []
        column_counts = Counter()
        for task in svc.safe_list(svc.store_list_task_records(openclaw_dir, limit=240)):
            if not isinstance(task, dict):
                continue
            column, blocked = _task_column_for_desktop(task)
            column_counts[column] += 1
            route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
            team_selection = route.get("teamSelection") if isinstance(route.get("teamSelection"), dict) else {}
            execution_bootstrap = route.get("executionBootstrap") if isinstance(route.get("executionBootstrap"), dict) else {}
            team_dispatch = route.get("teamDispatch") if isinstance(route.get("teamDispatch"), dict) else {}
            updated_at = str(task.get("updatedAt") or task.get("createdAt") or "").strip()
            updated_dt = svc.parse_iso(updated_at)
            result_summary = str(task.get("resultSummary") or "").strip()
            if not result_summary:
                output = str(task.get("output") or "").strip()
                if output:
                    result_summary = svc.summarize_task_execution_text(output, limit=180)
            team_id = _desktop_task_team_id(task)
            team_label = _desktop_task_team_label(task)
            team_thread_id = str(team_dispatch.get("threadId") or task.get("teamThreadId") or "").strip()
            items.append(
                {
                    "id": str(task.get("id") or "").strip(),
                    "title": str(task.get("title") or "").strip(),
                    "state": str(task.get("state") or "").strip(),
                    "status": str(task.get("status") or "").strip(),
                    "column": column,
                    "blocked": blocked,
                    "priority": _task_priority_for_desktop(task),
                    "owner": str(task.get("owner") or "").strip(),
                    "org": str(task.get("org") or "").strip(),
                    "teamId": team_id,
                    "teamLabel": team_label,
                    "currentAgent": str(task.get("currentAgent") or "").strip(),
                    "currentAgentLabel": str(task.get("currentAgentLabel") or "").strip(),
                    "targetAgentId": str(task.get("targetAgentId") or "").strip(),
                    "updatedAt": updated_at,
                    "updatedAgo": svc.format_age(updated_dt, now),
                    "activityAt": updated_at,
                    "currentUpdate": str(task.get("currentUpdate") or "").strip(),
                    "resultSummary": result_summary,
                    "progressRatio": (task.get("todo") if isinstance(task.get("todo"), dict) else {}).get("ratio"),
                    "teamThreadId": team_thread_id,
                    "teamThreadTitle": (
                        str(team_dispatch.get("threadTitle") or "").strip()
                        or (f"{team_label} · {str(task.get('title') or '').strip()}" if team_thread_id and team_label else "")
                    ),
                    "collaboration": (
                        team_dispatch.get("collaboration")
                        if isinstance(team_dispatch.get("collaboration"), dict)
                        else {}
                    ),
                    "routeSummary": {
                        "recommendedTeamId": str(team_selection.get("recommendedTeamId") or "").strip(),
                        "recommendedTeamName": str(team_selection.get("recommendedTeamName") or "").strip(),
                        "recommendedConfidence": team_selection.get("recommendedConfidence"),
                        "selectedTeamId": str(team_selection.get("selectedTeamId") or "").strip(),
                        "selectedTeamName": str(team_selection.get("selectedTeamName") or "").strip(),
                        "selectedExecutionAgentLabel": (
                            str(team_selection.get("selectedExecutionAgentLabel") or "").strip()
                            or str(execution_bootstrap.get("agentLabel") or "").strip()
                        ),
                        "executionStatus": str(execution_bootstrap.get("status") or "").strip(),
                    },
                    "targetPath": "/tasks",
                }
            )
        items.sort(
            key=lambda item: svc.parse_iso(item.get("activityAt") or item.get("updatedAt"))
            or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        return {
            "summary": {
                "total": len(items),
                "todo": column_counts["todo"],
                "doing": column_counts["doing"],
                "review": column_counts["review"],
                "done": column_counts["done"],
                "blocked": len([item for item in items if item.get("blocked")]),
            },
            "items": items,
        }

    return svc.cached_payload(
        ("desktop-task-snapshot-v2", str(openclaw_dir)),
        10.0,
        build,
    )


def _desktop_team_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    svc = _svc()
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else _desktop_task_snapshot(openclaw_dir, config=config, now=now)

    def build():
        router_agent_id = svc.get_router_agent_id(config)
        kanban_cfg = svc.load_kanban_config(openclaw_dir, router_agent_id)
        agents = svc.safe_list(svc.load_agents(config))
        agent_labels, _label_to_ids = svc.build_label_maps(agents, kanban_cfg, router_agent_id)
        agent_map = {
            str(agent.get("id") or "").strip(): agent
            for agent in agents
            if str(agent.get("id") or "").strip()
        }
        tasks = svc.safe_list(task_snapshot.get("items"))
        chat_thread_presence = _desktop_chat_thread_engagement_map(openclaw_dir)
        active_tasks = Counter()
        blocked_tasks = Counter()
        completed_tasks = Counter()
        engaged_agents = defaultdict(set)
        for task in tasks:
            if not isinstance(task, dict):
                continue
            team_id = str(task.get("teamId") or "").strip()
            if not team_id:
                continue
            if str(task.get("column") or "").strip() == "done":
                completed_tasks[team_id] += 1
            else:
                active_tasks[team_id] += 1
            if task.get("blocked"):
                blocked_tasks[team_id] += 1
            for agent_id in (task.get("currentAgent"), task.get("targetAgentId")):
                normalized = str(agent_id or "").strip()
                if normalized:
                    engaged_agents[team_id].add(normalized)
        for team_id, agent_ids in chat_thread_presence.items():
            if not team_id:
                continue
            for agent_id in agent_ids:
                if agent_id:
                    engaged_agents[team_id].add(agent_id)

        items = []
        for team in svc.safe_list(svc.store_list_agent_teams(openclaw_dir)):
            if not isinstance(team, dict):
                continue
            team_id = str(team.get("id") or "").strip()
            if not team_id:
                continue
            member_ids = [
                str(agent_id or "").strip()
                for agent_id in svc.safe_list(team.get("memberAgentIds"))
                if str(agent_id or "").strip()
            ]
            engaged_member_ids = [agent_id for agent_id in member_ids if agent_id in engaged_agents[team_id]]
            lead_agent_id = str(team.get("leadAgentId") or "").strip()
            member_preview = []
            for agent_id in member_ids[:5]:
                agent = agent_map.get(agent_id, {})
                identity = agent.get("identity") if isinstance(agent.get("identity"), dict) else {}
                member_preview.append(
                    {
                        "id": agent_id,
                        "label": svc.agent_identity_display_name(
                            agent.get("humanName") or identity.get("name"),
                            agent.get("jobTitle") or agent.get("roleLabel"),
                            fallback=agent_labels.get(agent_id, agent_id),
                        ),
                        "status": "active" if agent_id in engaged_member_ids else "",
                    }
                )
            lead_agent = agent_map.get(lead_agent_id, {})
            lead_identity = lead_agent.get("identity") if isinstance(lead_agent.get("identity"), dict) else {}
            member_count = len(member_ids)
            online_count = len(engaged_member_ids)
            items.append(
                {
                    "id": team_id,
                    "name": str(team.get("name") or "").strip(),
                    "status": str(team.get("status") or "").strip(),
                    "leadAgentId": lead_agent_id,
                    "leadAgentLabel": svc.agent_identity_display_name(
                        lead_agent.get("humanName") or lead_identity.get("name"),
                        lead_agent.get("jobTitle") or lead_agent.get("roleLabel"),
                        fallback=agent_labels.get(lead_agent_id, lead_agent_id),
                    ),
                    "memberCount": member_count,
                    "onlineCount": online_count,
                    "onlineRatio": round((online_count / member_count), 2) if member_count else 0,
                    "activeTasks": int(active_tasks.get(team_id, 0)),
                    "blockedTasks": int(blocked_tasks.get(team_id, 0)),
                    "completedTasks": int(completed_tasks.get(team_id, 0)),
                    "memberPreview": member_preview,
                }
            )
        items.sort(
            key=lambda item: (
                -int(item.get("blockedTasks") or 0),
                -int(item.get("activeTasks") or 0),
                -int(item.get("onlineCount") or 0),
                str(item.get("name") or ""),
            )
        )
        return {
            "summary": {"total": len(items)},
            "items": items,
        }

    return svc.cached_payload(
        ("desktop-team-snapshot-v2", str(openclaw_dir)),
        10.0,
        build,
    )


def _desktop_activity_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    svc = _svc()
    config = config if isinstance(config, dict) else svc.load_config(openclaw_dir)
    now = now or svc.now_utc()
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else _desktop_task_snapshot(openclaw_dir, config=config, now=now)

    def build():
        items = []
        tasks = svc.safe_list(task_snapshot.get("items"))
        chat_threads = svc.safe_list(
            _desktop_chat_thread_items(
                openclaw_dir,
                include_task_ref=False,
                include_team_ref=False,
                include_collaboration=False,
            ).get("threads")
        )
        alerts = svc.safe_list((((_desktop_alert_snapshot(openclaw_dir, config=config) or {}).get("automation") or {}).get("alerts")))
        for task in tasks[:120]:
            if not isinstance(task, dict):
                continue
            updated_at = str(task.get("activityAt") or task.get("updatedAt") or "").strip()
            if not updated_at:
                continue
            items.append(
                {
                    "id": f"task:{str(task.get('id') or '').strip()}:{updated_at}",
                    "kind": "alert" if task.get("blocked") else "task",
                    "title": str(task.get("title") or "").strip(),
                    "detail": (
                        str(task.get("currentUpdate") or "").strip()
                        or str(task.get("resultSummary") or "").strip()
                        or str(task.get("teamLabel") or "").strip()
                    ),
                    "updatedAt": updated_at,
                    "updatedAgo": svc.format_age(svc.parse_iso(updated_at), now),
                    "taskId": str(task.get("id") or "").strip(),
                    "targetPath": "/tasks",
                }
            )
        for thread in chat_threads[:80]:
            if not isinstance(thread, dict):
                continue
            updated_at = str(thread.get("lastMessageAt") or thread.get("updatedAt") or "").strip()
            if not updated_at:
                continue
            items.append(
                {
                    "id": f"conversation:{str(thread.get('id') or '').strip()}:{updated_at}",
                    "kind": "conversation",
                    "title": str(thread.get("title") or "").strip(),
                    "detail": str(thread.get("lastMessagePreview") or thread.get("teamLabel") or "").strip(),
                    "updatedAt": updated_at,
                    "updatedAgo": svc.format_age(svc.parse_iso(updated_at), now),
                    "taskId": str(thread.get("linkedTaskId") or "").strip(),
                    "targetPath": _desktop_browser_path_for_thread(thread),
                }
            )
        for alert in alerts[:24]:
            if not isinstance(alert, dict):
                continue
            updated_at = str(alert.get("updatedAt") or alert.get("triggeredAt") or "").strip()
            if not updated_at:
                continue
            items.append(
                {
                    "id": f"system:{str(alert.get('id') or '').strip()}:{updated_at}",
                    "kind": "system",
                    "title": str(alert.get("title") or "").strip(),
                    "detail": str(alert.get("detail") or "").strip(),
                    "updatedAt": updated_at,
                    "updatedAgo": svc.format_age(svc.parse_iso(updated_at), now),
                    "taskId": "",
                    "targetPath": "/management",
                }
            )
        items.sort(
            key=lambda item: svc.parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        return {
            "summary": {"total": len(items[:32])},
            "items": items[:32],
        }

    return svc.cached_payload(
        ("desktop-activity-snapshot-v2", str(openclaw_dir)),
        10.0,
        build,
    )


def _desktop_alert_snapshot(openclaw_dir, config=None):
    svc = _svc()

    def build():
        alerts = []
        for alert in svc.safe_list(svc.store_list_automation_alerts(openclaw_dir, limit=24)):
            if not isinstance(alert, dict):
                continue
            updated_at = str(alert.get("updatedAt") or alert.get("triggeredAt") or "").strip()
            alerts.append(
                {
                    "id": str(alert.get("id") or "").strip(),
                    "title": str(alert.get("title") or "").strip(),
                    "detail": str(alert.get("detail") or "").strip(),
                    "severity": str(alert.get("severity") or "").strip() or "warning",
                    "status": str(alert.get("status") or "").strip(),
                    "updatedAt": updated_at,
                    "updatedAgo": svc.format_age(svc.parse_iso(updated_at), svc.now_utc()),
                }
            )
        return {
            "automation": {
                "summary": {"total": len(alerts)},
                "alerts": alerts,
            },
        }

    return svc.cached_payload(
        ("desktop-alert-snapshot", str(openclaw_dir)),
        10.0,
        build,
    )


def _task_column_for_desktop(task):
    task = task if isinstance(task, dict) else {}
    state = str(task.get("state") or task.get("status") or "").strip().lower()
    blocked = bool(task.get("blocked")) or state == "blocked"
    if state in REVIEW_DESKTOP_TASK_STATES:
        return "review", blocked
    if state in TERMINAL_DESKTOP_TASK_STATES:
        return "done", blocked
    if blocked:
        return "doing", True
    if state in DOING_DESKTOP_TASK_STATES:
        return "doing", False
    if state in TODO_DESKTOP_TASK_STATES:
        return "todo", False
    return "todo", blocked


def _task_priority_for_desktop(task):
    task = task if isinstance(task, dict) else {}
    route = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    for candidate in (
        route.get("priorityLevel"),
        (route.get("intelligence") if isinstance(route.get("intelligence"), dict) else {}).get("priorityLevel"),
        task.get("priorityLevel"),
        task.get("priority"),
    ):
        normalized = str(candidate or "").strip().lower()
        if normalized in {"low", "normal", "high", "critical"}:
            return normalized
    return "normal"


def _desktop_browser_path_for_thread(thread):
    thread = thread if isinstance(thread, dict) else {}
    channel = str(thread.get("channel") or "").strip().lower()
    if channel in {"customer_wechat", "openclaw_weixin"}:
        return "/communications"
    return "/battlefield"


def _desktop_notification_items(alerts, threads, activity_items, now):
    svc = _svc()
    alerts = svc.safe_list(alerts)
    threads = svc.safe_list(threads)
    activity_items = svc.safe_list(activity_items)
    items = []
    for alert in alerts[:8]:
        if not isinstance(alert, dict):
            continue
        items.append(
            {
                "id": f"alert:{str(alert.get('id') or '').strip()}",
                "kind": "system",
                "severity": str(alert.get("severity") or "warning").strip() or "warning",
                "title": str(alert.get("title") or "").strip(),
                "detail": str(alert.get("detail") or "").strip(),
                "status": str(alert.get("status") or "").strip(),
                "updatedAt": str(alert.get("updatedAt") or "").strip(),
                "updatedAgo": str(alert.get("updatedAgo") or "").strip(),
                "targetPath": "/management",
                "taskId": "",
                "threadId": "",
            }
        )
    for thread in threads[:12]:
        if not isinstance(thread, dict):
            continue
        last_at = str(thread.get("lastMessageAt") or thread.get("updatedAt") or thread.get("createdAt") or "").strip()
        if not last_at:
            continue
        items.append(
            {
                "id": f"thread:{str(thread.get('id') or '').strip()}",
                "kind": "conversation",
                "severity": "info",
                "title": str(thread.get("title") or "").strip(),
                "detail": str(thread.get("lastMessagePreview") or "").strip(),
                "status": str(thread.get("status") or "").strip(),
                "updatedAt": last_at,
                "updatedAgo": svc.format_age(svc.parse_iso(last_at), now),
                "targetPath": _desktop_browser_path_for_thread(thread),
                "taskId": str(thread.get("linkedTaskId") or "").strip(),
                "threadId": str(thread.get("id") or "").strip(),
            }
        )
    for item in activity_items[:18]:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or "").strip().lower()
        if kind not in {"task", "alert"}:
            continue
        items.append(
            {
                "id": f"activity:{str(item.get('id') or '').strip()}",
                "kind": "task",
                "severity": "warning" if kind == "alert" else "info",
                "title": str(item.get("title") or "").strip(),
                "detail": str(item.get("detail") or "").strip(),
                "status": kind,
                "updatedAt": str(item.get("updatedAt") or "").strip(),
                "updatedAgo": svc.format_age(svc.parse_iso(item.get("updatedAt")), now),
                "targetPath": str(item.get("targetPath") or "/tasks").strip() or "/tasks",
                "taskId": str(item.get("taskId") or "").strip(),
                "threadId": "",
            }
        )
    items.sort(
        key=lambda item: (
            svc.parse_iso(item.get("updatedAt"))
            or svc.parse_iso(item.get("triggeredAt"))
            or svc.parse_iso(item.get("createdAt"))
            or datetime.fromtimestamp(0, tz=timezone.utc)
        ),
        reverse=True,
    )
    return items[:32]


def _desktop_chat_thread_engagement_map(openclaw_dir, limit=80):
    svc = _svc()

    def build():
        presence = defaultdict(set)
        for thread in svc.safe_list(svc.store_list_chat_threads(openclaw_dir, limit=limit)):
            if not isinstance(thread, dict):
                continue
            status = str(thread.get("status") or "").strip().lower()
            if status in {"archived", "closed"}:
                continue
            meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
            linked_team_ids = [
                str(team_id or "").strip()
                for team_id in svc.safe_list(svc.linked_team_ids_for_chat_thread(meta))
                if str(team_id or "").strip()
            ]
            if not linked_team_ids:
                linked_team = thread.get("linkedTeam") if isinstance(thread.get("linkedTeam"), dict) else {}
                linked_team_id = str(linked_team.get("id") or "").strip()
                if linked_team_id:
                    linked_team_ids = [linked_team_id]
            participant_ids = [
                str(agent_id or "").strip()
                for agent_id in svc.safe_list(thread.get("participantAgentIds"))
                if str(agent_id or "").strip()
            ]
            for team_id in linked_team_ids:
                for agent_id in participant_ids:
                    presence[team_id].add(agent_id)
        return {
            team_id: sorted(agent_ids)
            for team_id, agent_ids in presence.items()
            if team_id
        }

    return svc.cached_payload(
        ("desktop-chat-thread-engagement-v2", str(openclaw_dir), str(limit)),
        5.0,
        build,
    )


def _desktop_chat_thread_items(openclaw_dir, limit=80, include_task_ref=True, include_team_ref=True, include_collaboration=True):
    svc = _svc()

    def build():
        threads = svc.safe_list(svc.store_list_chat_threads(openclaw_dir, limit=limit))
        thread_ids = [str(item.get("id") or "").strip() for item in threads if str(item.get("id") or "").strip()]
        summary_map = {
            str(item.get("threadId") or "").strip(): item
            for item in svc.safe_list(svc.store_list_chat_thread_message_summaries(openclaw_dir, thread_ids))
            if str(item.get("threadId") or "").strip()
        }
        team_map = {
            str(item.get("id") or "").strip(): item
            for item in svc.safe_list(svc.store_list_agent_teams(openclaw_dir))
            if str(item.get("id") or "").strip()
        }
        task_map = {}
        if include_task_ref:
            linked_task_ids = {
                str(thread.get("linkedTaskId") or "").strip()
                for thread in threads
                if str(thread.get("linkedTaskId") or "").strip()
            }
            for task_id in linked_task_ids:
                task = svc.store_get_task_record(openclaw_dir, task_id)
                if isinstance(task, dict):
                    normalized_task_id = str(task.get("id") or "").strip()
                    if normalized_task_id:
                        task_map[normalized_task_id] = task
        payload_threads = []
        for thread in threads:
            thread_id = str(thread.get("id") or "").strip()
            if not thread_id:
                continue
            meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
            team_id = str(meta.get("teamId") or "").strip()
            linked_team_ids = svc.linked_team_ids_for_chat_thread(meta) if include_team_ref else []
            dispatch_mode = svc.normalize_chat_dispatch_mode(meta.get("dispatchMode"), has_team=bool(linked_team_ids))
            last_dispatch = (
                meta.get("lastDispatch")
                if isinstance(meta.get("lastDispatch"), dict)
                else (meta.get("lastSync") if isinstance(meta.get("lastSync"), dict) else {})
            )
            message_summary = summary_map.get(thread_id, {})
            last_message_body = chat_context_core.preferred_chat_preview_text(
                message_summary.get("lastMessageBody") or "",
            )
            linked_task_id = str(thread.get("linkedTaskId") or "").strip()
            linked_team = team_map.get(team_id) if team_id else {}
            thread_item = {
                "id": thread_id,
                "title": str(thread.get("title") or "").strip(),
                "status": str(thread.get("status") or "").strip(),
                "channel": str(thread.get("channel") or "").strip(),
                "owner": str(thread.get("owner") or "").strip(),
                "linkedTaskId": linked_task_id,
                "createdAt": str(thread.get("createdAt") or "").strip(),
                "updatedAt": str(thread.get("updatedAt") or "").strip(),
                "lastMessageId": str(message_summary.get("lastMessageId") or "").strip(),
                "lastMessageAt": str(message_summary.get("lastMessageAt") or "").strip(),
                "lastMessagePreview": svc.summarize_task_execution_text(last_message_body, limit=160) if last_message_body else "",
                "messageCount": int(message_summary.get("messageCount") or 0),
                "teamLabel": str((linked_team or {}).get("name") or "").strip(),
                **chat_context_core._derive_chat_thread_conversation_binding(thread),
            }
            if include_team_ref:
                thread_item["linkedTeam"] = svc.compact_chat_thread_team_reference(linked_team)
                thread_item["dispatchMode"] = dispatch_mode
            if include_task_ref and linked_task_id:
                linked_task = task_map.get(linked_task_id, {})
                thread_item["linkedTask"] = {
                    "id": str(linked_task.get("id") or linked_task_id).strip(),
                    "title": str(linked_task.get("title") or "").strip(),
                    "status": str(linked_task.get("status") or "").strip(),
                    "currentUpdate": str(linked_task.get("currentUpdate") or "").strip(),
                    "updatedAt": str(linked_task.get("updatedAt") or "").strip(),
                } if linked_task else {"id": linked_task_id}
            if include_collaboration:
                thread_item["collaboration"] = svc.build_team_collaboration_summary(last_dispatch)
            payload_threads.append(thread_item)
        return {
            "summary": {
                "total": len(payload_threads),
                "active": len(
                    [
                        item
                        for item in payload_threads
                        if str(item.get("status") or "").strip().lower() not in {"archived", "closed"}
                    ]
                ),
            },
            "threads": payload_threads,
        }

    cache_key = (
        "desktop-chat-thread-items-v3",
        str(openclaw_dir),
        str(limit),
        "task" if include_task_ref else "no-task",
        "team" if include_team_ref else "no-team",
        "collab" if include_collaboration else "no-collab",
    )
    return svc.cached_payload(cache_key, 3.0, build)


def build_chat_thread_quick_snapshot(openclaw_dir, thread):
    svc = _svc()
    thread = thread if isinstance(thread, dict) else {}
    thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    workspace_path = str(thread.get("workspacePath") or thread_meta.get("workspacePath") or "").strip()
    workspace_authorized = bool(thread.get("workspaceAuthorized") or thread_meta.get("workspaceAuthorized")) and bool(workspace_path)
    team_id = str(thread_meta.get("teamId") or "").strip()
    linked_team_ids = svc.linked_team_ids_for_chat_thread(thread_meta)
    team_map = {
        item.get("id"): item
        for item in svc.safe_list(svc.store_list_agent_teams(openclaw_dir))
        if item.get("id")
    }
    pack_map = svc.workflow_pack_map_from_skills_payload(svc.load_skills_catalog(openclaw_dir))
    dispatch_mode = svc.normalize_chat_dispatch_mode(thread_meta.get("dispatchMode"), has_team=bool(linked_team_ids))
    participant_agents = [
        {
            "id": agent_id,
            "label": agent_id,
            "status": "",
            "focus": "",
        }
        for agent_id in svc.safe_list(thread.get("participantAgentIds"))
        if str(agent_id or "").strip()
    ]
    last_dispatch = thread_meta.get("lastDispatch") if isinstance(thread_meta.get("lastDispatch"), dict) else {}
    snapshot = svc.hydrate_chat_thread_pack_context(
        {
            **thread,
            "workspacePath": workspace_path,
            "workspaceAuthorized": workspace_authorized,
            "participantAgents": participant_agents,
            "messages": [],
            "messageCount": 0,
            "lastMessageId": "",
            "lastMessageAt": "",
            "lastMessagePreview": "",
            "updatedAgo": svc.format_age(svc.parse_iso(thread.get("updatedAt")), svc.now_utc()),
            "teamId": team_id,
            "linkedTeam": svc.compact_chat_thread_team_reference(team_map.get(team_id)),
            "linkedTeamIds": linked_team_ids,
            "linkedTeams": svc.compact_chat_thread_team_references(team_map, linked_team_ids),
            "dispatchMode": dispatch_mode,
            "teamPolicy": svc.hydrate_thread_memory_authority(
                openclaw_dir,
                thread,
                thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {},
            ),
            "lastDispatch": last_dispatch,
        },
        pack_map,
    )
    snapshot["collaboration"] = svc.build_team_collaboration_summary(last_dispatch)
    snapshot["teamOwnership"] = svc.build_team_ownership_payload(
        {},
        execution_team_id=team_id,
        recommended_team_id="",
        mode=str(snapshot.get("mode") or "").strip(),
        source="chat",
    )
    if workspace_path:
        snapshot["workspacePath"] = workspace_path
        snapshot["workspaceAuthorized"] = workspace_authorized
        snapshot["meta"] = {
            **(snapshot.get("meta") if isinstance(snapshot.get("meta"), dict) else {}),
            "workspacePath": workspace_path,
            "workspaceAuthorized": workspace_authorized,
        }
    return snapshot


def build_desktop_chat_threads_payload(openclaw_dir, limit=80):
    return _desktop_chat_thread_items(
        openclaw_dir,
        limit=limit,
        include_task_ref=True,
        include_team_ref=True,
        include_collaboration=True,
    )


def build_desktop_bootstrap_payload(openclaw_dir):
    svc = _svc()
    config = svc.load_config(openclaw_dir)
    now = svc.now_utc()
    teams = [
        {
            "id": str(item.get("id") or "").strip(),
            "name": str(item.get("name") or "").strip(),
            "leadAgentId": str(item.get("leadAgentId") or "").strip(),
            "status": str(item.get("status") or "").strip(),
        }
        for item in svc.safe_list(svc.store_list_agent_teams(openclaw_dir))
        if str(item.get("id") or "").strip()
    ]
    deliverables = svc.build_deliverables_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    payload = {
        "generatedAt": svc.now_iso(),
        "agentTeams": {
            "summary": {"total": len(teams)},
            "items": teams,
        },
        "chat": build_desktop_chat_threads_payload(openclaw_dir),
        "deliverables": deliverables[:24],
        "workspace": {
            "deliverables": deliverables[:24],
        },
    }
    payload["signature"] = svc.dashboard_signature(payload)
    return payload


def _build_desktop_summary_payload_from_snapshots(task_snapshot, team_snapshot):
    svc = _svc()
    team_items = svc.safe_list(((team_snapshot if isinstance(team_snapshot, dict) else {}).get("items")))
    tasks = svc.safe_list((task_snapshot if isinstance(task_snapshot, dict) else {}).get("items"))
    active_teams = 0
    for team in team_items:
        if not isinstance(team, dict):
            continue
        online_count = int(team.get("onlineCount") or 0)
        task_count = int(team.get("activeTasks") or 0)
        blocked_count = int(team.get("blockedTasks") or 0)
        if online_count > 0 or task_count > 0 or blocked_count > 0:
            active_teams += 1

    task_column_counter = Counter(
        str(task.get("column") or "").strip() or "todo"
        for task in tasks
        if isinstance(task, dict)
    )
    blocked_tasks = len([task for task in tasks if isinstance(task, dict) and task.get("blocked")])
    return {
        "generatedAt": svc.now_iso(),
        "summary": {
            "activeTeams": active_teams,
            "inProgressTasks": task_column_counter["doing"] + task_column_counter["review"],
            "blockedTasks": blocked_tasks,
        },
    }


def build_desktop_summary_payload(openclaw_dir):
    svc = _svc()
    now = svc.now_utc()
    config = svc.load_config(openclaw_dir)
    task_snapshot = _desktop_task_snapshot(openclaw_dir, config=config, now=now)
    team_snapshot = _desktop_team_snapshot(openclaw_dir, config=config, now=now, task_snapshot=task_snapshot)
    payload = _build_desktop_summary_payload_from_snapshots(task_snapshot, team_snapshot)
    payload["signature"] = svc.dashboard_signature(payload)
    return payload


def build_desktop_team_health_payload(openclaw_dir):
    svc = _svc()
    now = svc.now_utc()
    config = svc.load_config(openclaw_dir)
    task_snapshot = _desktop_task_snapshot(openclaw_dir, config=config, now=now)
    team_snapshot = _desktop_team_snapshot(openclaw_dir, config=config, now=now, task_snapshot=task_snapshot)
    team_items = svc.safe_list(((team_snapshot if isinstance(team_snapshot, dict) else {}).get("items")))
    payload = {
        "generatedAt": svc.now_iso(),
        "summary": {"total": len(team_items[:12])},
        "items": [
            {
                "id": str(team.get("id") or "").strip(),
                "name": str(team.get("name") or "").strip(),
                "leadAgentId": str(team.get("leadAgentId") or "").strip(),
                "leadAgentLabel": str(team.get("leadAgentLabel") or "").strip(),
                "memberCount": int(team.get("memberCount") or len(svc.safe_list(team.get("memberPreview")))),
                "onlineCount": int(team.get("onlineCount") or 0),
                "onlineRatio": round(
                    (
                        int(team.get("onlineCount") or 0)
                        / max(int(team.get("memberCount") or len(svc.safe_list(team.get("memberPreview"))) or 0), 1)
                    ),
                    2,
                ) if int(team.get("memberCount") or len(svc.safe_list(team.get("memberPreview"))) or 0) else 0,
                "activeTasks": int(team.get("activeTasks") or 0),
                "blockedTasks": int(team.get("blockedTasks") or 0),
                "memberPreview": [
                    {
                        "id": str(item.get("id") or "").strip(),
                        "label": str(item.get("label") or "").strip(),
                        "status": str(item.get("status") or "").strip(),
                    }
                    for item in svc.safe_list(team.get("memberPreview"))[:5]
                    if isinstance(item, dict)
                ],
            }
            for team in team_items[:12]
            if isinstance(team, dict)
        ],
    }
    payload["signature"] = svc.dashboard_signature(payload)
    return payload


def build_desktop_activity_payload(openclaw_dir):
    svc = _svc()
    now = svc.now_utc()
    config = svc.load_config(openclaw_dir)
    task_snapshot = _desktop_task_snapshot(openclaw_dir, config=config, now=now)
    activity_snapshot = _desktop_activity_snapshot(openclaw_dir, config=config, now=now, task_snapshot=task_snapshot)
    payload = {
        "generatedAt": svc.now_iso(),
        "summary": deepcopy((activity_snapshot if isinstance(activity_snapshot, dict) else {}).get("summary") if isinstance((activity_snapshot if isinstance(activity_snapshot, dict) else {}).get("summary"), dict) else {}),
        "items": deepcopy(svc.safe_list((activity_snapshot if isinstance(activity_snapshot, dict) else {}).get("items"))),
    }
    payload["signature"] = svc.dashboard_signature(payload)
    return payload


def build_desktop_notifications_payload(openclaw_dir):
    svc = _svc()
    now = svc.now_utc()
    config = svc.load_config(openclaw_dir)
    task_snapshot = _desktop_task_snapshot(openclaw_dir, config=config, now=now)
    activity_snapshot = _desktop_activity_snapshot(openclaw_dir, config=config, now=now, task_snapshot=task_snapshot)
    alert_snapshot = _desktop_alert_snapshot(openclaw_dir, config=config)
    threads = svc.safe_list(
        _desktop_chat_thread_items(
            openclaw_dir,
            include_task_ref=False,
            include_team_ref=False,
            include_collaboration=False,
        ).get("threads")
    )
    activity_items = svc.safe_list(((activity_snapshot if isinstance(activity_snapshot, dict) else {}).get("items")))
    alerts = svc.safe_list((((alert_snapshot.get("automation") if isinstance(alert_snapshot, dict) else {}) or {}).get("alerts")))
    items = _desktop_notification_items(alerts, threads, activity_items, now)
    payload = {
        "generatedAt": svc.now_iso(),
        "summary": {
            "total": len(items),
            "system": len([item for item in items if item.get("kind") == "system"]),
            "conversation": len([item for item in items if item.get("kind") == "conversation"]),
            "task": len([item for item in items if item.get("kind") == "task"]),
        },
        "items": items,
    }
    payload["signature"] = svc.dashboard_signature(payload)
    return payload


def build_desktop_tasks_payload(openclaw_dir):
    svc = _svc()
    now = svc.now_utc()
    config = svc.load_config(openclaw_dir)
    task_snapshot = _desktop_task_snapshot(openclaw_dir, config=config, now=now)
    items = svc.safe_list(task_snapshot.get("items"))[:160]
    column_counts = Counter(
        str(task.get("column") or "").strip() or "todo"
        for task in items
        if isinstance(task, dict)
    )
    payload = {
        "generatedAt": svc.now_iso(),
        "summary": {
            "total": len(items),
            "todo": column_counts["todo"],
            "doing": column_counts["doing"],
            "review": column_counts["review"],
            "done": column_counts["done"],
            "blocked": len([item for item in items if item.get("blocked")]),
        },
        "items": items,
    }
    payload["signature"] = svc.dashboard_signature(payload)
    return payload


def build_desktop_thread_detail(openclaw_dir, thread_id, message_limit=120):
    svc = _svc()
    thread_id = str(thread_id or "").strip()
    if not thread_id:
        raise RuntimeError("需要 threadId。")
    thread = svc.store_get_chat_thread(openclaw_dir, thread_id)
    if not thread:
        raise RuntimeError("聊天线程不存在。")
    config = svc.load_config(openclaw_dir)
    router_agent_id = svc.get_router_agent_id(config)
    kanban_cfg = svc.load_kanban_config(openclaw_dir, router_agent_id)
    agents = svc.load_agents(config)
    agent_labels, _label_to_ids = svc.build_label_maps(agents, kanban_cfg, router_agent_id)
    agent_map = {
        str(agent.get("id") or "").strip(): agent
        for agent in agents
        if str(agent.get("id") or "").strip()
    }
    team_map = {
        str(item.get("id") or "").strip(): item
        for item in svc.safe_list(svc.store_list_agent_teams(openclaw_dir))
        if str(item.get("id") or "").strip()
    }
    task = svc.store_get_task_record(openclaw_dir, thread.get("linkedTaskId"))
    messages = []
    for message in svc.store_list_recent_chat_messages(openclaw_dir, thread_id=thread_id, limit=message_limit):
        sender_id = str(message.get("senderId") or "").strip()
        agent = agent_map.get(sender_id, {})
        message_meta = message.get("meta") if isinstance(message.get("meta"), dict) else {}
        compact_attachments = [
            chat_context_core._compact_chat_thread_attachment_payload(
                item,
                thread_id=thread_id,
                message_id=str(message.get("id") or "").strip(),
            )
            for item in svc.safe_list(message_meta.get("attachments"))
        ]
        compact_attachments = [item for item in compact_attachments if item]
        normalized_meta = (
            {
                **message_meta,
                "attachments": compact_attachments,
            }
            if compact_attachments
            else message_meta
        )
        sender_display = svc.agent_identity_display_name(
            agent.get("humanName") or message_meta.get("agentHumanName"),
            agent.get("jobTitle") or message_meta.get("agentJobTitle"),
            fallback=message.get("senderLabel") or agent_labels.get(sender_id, sender_id or "unknown"),
        )
        messages.append(
            {
                **message,
                "meta": normalized_meta,
                "attachments": compact_attachments,
                "createdAgo": svc.format_age(svc.parse_iso(message.get("createdAt")), svc.now_utc()),
                "senderDisplay": sender_display,
            }
        )
    participant_agents = []
    for agent_id in svc.safe_list(thread.get("participantAgentIds")):
        normalized_agent_id = str(agent_id or "").strip()
        if not normalized_agent_id:
            continue
        agent = agent_map.get(normalized_agent_id, {})
        participant_agents.append(
            {
                "id": normalized_agent_id,
                "label": svc.agent_identity_display_name(
                    agent.get("humanName") or agent.get("name"),
                    agent.get("jobTitle") or agent.get("roleLabel"),
                    fallback=agent_labels.get(normalized_agent_id, normalized_agent_id),
                ),
                "status": str(agent.get("status") or "").strip(),
                "focus": str(agent.get("focus") or "").strip(),
            }
        )
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_id = str(meta.get("teamId") or "").strip()
    linked_team_ids = svc.linked_team_ids_for_chat_thread(meta)
    dispatch_mode = svc.normalize_chat_dispatch_mode(meta.get("dispatchMode"), has_team=bool(linked_team_ids))
    last_dispatch = (
        meta.get("lastDispatch")
        if isinstance(meta.get("lastDispatch"), dict)
        else (meta.get("lastSync") if isinstance(meta.get("lastSync"), dict) else {})
    )
    detail = {
        "id": str(thread.get("id") or "").strip(),
        "title": str(thread.get("title") or "").strip(),
        "status": str(thread.get("status") or "").strip(),
        "channel": str(thread.get("channel") or "").strip(),
        "owner": str(thread.get("owner") or "").strip(),
        "primaryAgentId": str(thread.get("primaryAgentId") or "").strip(),
        "currentTargetAgentId": str(thread.get("currentTargetAgentId") or "").strip(),
        "linkedTaskId": str(thread.get("linkedTaskId") or "").strip(),
        "linkedRunId": str(thread.get("linkedRunId") or "").strip(),
        "createdAt": str(thread.get("createdAt") or "").strip(),
        "updatedAt": str(thread.get("updatedAt") or "").strip(),
        "participantAgentIds": svc.safe_list(thread.get("participantAgentIds")),
        "participantAgents": participant_agents,
        "messages": messages,
        "messageCount": len(messages),
        "lastMessageId": str((messages[-1] if messages else {}).get("id") or "").strip(),
        "lastMessageAt": str((messages[-1] if messages else {}).get("createdAt") or "").strip(),
        "lastMessagePreview": (
            svc.summarize_task_execution_text(
                chat_context_core.preferred_chat_preview_text(
                    ((messages[-1] if messages else {}).get("body") or ""),
                    messages=messages,
                ),
                limit=220,
            )
            if messages
            else ""
        ),
        "teamId": team_id,
        "linkedTeamIds": linked_team_ids,
        "linkedTeam": svc.compact_chat_thread_team_reference(team_map.get(team_id)),
        "linkedTeams": svc.compact_chat_thread_team_references(team_map, linked_team_ids),
        "dispatchMode": dispatch_mode,
        "teamPolicy": svc.hydrate_thread_memory_authority(
            openclaw_dir,
            thread,
            meta.get("teamPolicy") if isinstance(meta.get("teamPolicy"), dict) else {},
        ),
        "collaboration": svc.build_team_collaboration_summary(last_dispatch),
        "linkedTask": svc.compact_task_reference(task) if isinstance(task, dict) else {},
        "workspacePath": str(thread.get("workspacePath") or meta.get("workspacePath") or "").strip(),
        "workspaceAuthorized": bool(thread.get("workspaceAuthorized") or meta.get("workspaceAuthorized")),
    }
    tiered_context_source = {
        **detail,
        "contextCompression": meta.get("contextCompression") if isinstance(meta.get("contextCompression"), dict) else {},
    }
    layered_context = chat_context_core.build_chat_thread_layered_context(tiered_context_source)
    payload = {"thread": detail, **layered_context}
    payload["signature"] = svc.dashboard_signature(payload)
    return payload
