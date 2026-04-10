from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from datetime import timedelta, timezone
from urllib.parse import urlsplit


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


MANAGEMENT_BOOTSTRAP_VERSION = _DelegatedSymbol("MANAGEMENT_BOOTSTRAP_VERSION")
TERMINAL_STATES = _DelegatedSymbol("TERMINAL_STATES")
build_automation_snapshot = _DelegatedSymbol("build_automation_snapshot")
build_dashboard_data = _DelegatedSymbol("build_dashboard_data")
dashboard_dir = _DelegatedSymbol("dashboard_dir")
guarded_http_request = _DelegatedSymbol("guarded_http_request")
now_iso = _DelegatedSymbol("now_iso")
parse_iso = _DelegatedSymbol("parse_iso")
parse_transcript_items = _DelegatedSymbol("parse_transcript_items")
safe_list = _DelegatedSymbol("safe_list")
send_notification_message = _DelegatedSymbol("send_notification_message")
session_transcript_path = _DelegatedSymbol("session_transcript_path")
store_list_automation_rules = _DelegatedSymbol("store_list_automation_rules")
store_list_notification_deliveries = _DelegatedSymbol("store_list_notification_deliveries")
store_save_automation_rule = _DelegatedSymbol("store_save_automation_rule")
store_save_notification_channel = _DelegatedSymbol("store_save_notification_channel")
store_save_notification_delivery = _DelegatedSymbol("store_save_notification_delivery")
store_upsert_automation_alert = _DelegatedSymbol("store_upsert_automation_alert")
transcript_quality_snapshot = _DelegatedSymbol("transcript_quality_snapshot")


def compute_conversation_quality_data(openclaw_dir, conversation_data, now):
    sessions = safe_list((conversation_data or {}).get("sessions"))
    agent_rows = {}
    session_rows = []
    total_response_samples = []
    total_tool_results = 0
    total_tool_failures = 0
    pressure_sessions = 0
    critical_sessions = 0

    for session in sessions:
        transcript_path = session.get("transcriptPath") or session_transcript_path(
            openclaw_dir, session.get("agentId", ""), session.get("sessionId", "")
        )
        transcript = parse_transcript_items(transcript_path, limit=180)
        quality = transcript_quality_snapshot(transcript)
        band = "stable"
        if session.get("abortedLastRun") or quality["toolFailureCount"] or quality["avgResponseSeconds"] >= 480:
            band = "critical"
        elif quality["pressureHits"] or quality["avgResponseSeconds"] >= 240:
            band = "watch"
        if band in {"watch", "critical"}:
            pressure_sessions += 1
        if band == "critical":
            critical_sessions += 1

        total_response_samples.extend(quality["responseSamples"])
        total_tool_results += quality["toolResultCount"]
        total_tool_failures += quality["toolFailureCount"]
        row = {
            "key": session.get("key", ""),
            "label": session.get("label") or session.get("sessionId") or session.get("key"),
            "agentId": session.get("agentId", ""),
            "agentLabel": session.get("agentLabel") or session.get("agentId", ""),
            "updatedAgo": session.get("updatedAgo", ""),
            "band": band,
            "avgResponseSeconds": quality["avgResponseSeconds"],
            "toolSuccessRate": quality["toolSuccessRate"],
            "pressureHits": quality["pressureHits"],
            "toolFailures": quality["toolFailureCount"],
            "abortedLastRun": bool(session.get("abortedLastRun")),
        }
        session_rows.append(row)

        agent_key = row["agentId"] or "unknown"
        agent_entry = agent_rows.setdefault(
            agent_key,
            {
                "id": agent_key,
                "title": row["agentLabel"],
                "sessionCount": 0,
                "pressureSessions": 0,
                "criticalSessions": 0,
                "abortedSessions": 0,
                "toolFailures": 0,
                "responseSamples": [],
                "toolSuccessRates": [],
            },
        )
        agent_entry["sessionCount"] += 1
        agent_entry["pressureSessions"] += 1 if band in {"watch", "critical"} else 0
        agent_entry["criticalSessions"] += 1 if band == "critical" else 0
        agent_entry["abortedSessions"] += 1 if row["abortedLastRun"] else 0
        agent_entry["toolFailures"] += row["toolFailures"]
        if quality["responseSamples"]:
            agent_entry["responseSamples"].extend(quality["responseSamples"])
        if quality["toolResultCount"]:
            agent_entry["toolSuccessRates"].append(quality["toolSuccessRate"])

    agent_cards = []
    for agent in agent_rows.values():
        avg_response_seconds = round(sum(agent["responseSamples"]) / len(agent["responseSamples"]), 1) if agent["responseSamples"] else 0.0
        tool_success_rate = round(sum(agent["toolSuccessRates"]) / len(agent["toolSuccessRates"])) if agent["toolSuccessRates"] else 100
        band = "stable"
        if agent["criticalSessions"] or agent["abortedSessions"] or tool_success_rate < 70:
            band = "critical"
        elif agent["pressureSessions"] or avg_response_seconds >= 240 or tool_success_rate < 90:
            band = "watch"
        agent_cards.append(
            {
                "id": agent["id"],
                "title": agent["title"],
                "sessionCount": agent["sessionCount"],
                "pressureSessions": agent["pressureSessions"],
                "criticalSessions": agent["criticalSessions"],
                "abortedSessions": agent["abortedSessions"],
                "avgResponseSeconds": avg_response_seconds,
                "toolSuccessRate": tool_success_rate,
                "band": band,
            }
        )
    agent_cards.sort(key=lambda item: (-item["criticalSessions"], -item["pressureSessions"], item["avgResponseSeconds"] or 0, item["title"]))

    avg_response_seconds = round(sum(total_response_samples) / len(total_response_samples), 1) if total_response_samples else 0.0
    tool_success_rate = round(((total_tool_results - total_tool_failures) / total_tool_results) * 100) if total_tool_results else 100
    return {
        "summary": {
            "sessionCount": len(sessions),
            "analyzedSessions": len(session_rows),
            "avgResponseSeconds": avg_response_seconds,
            "toolSuccessRate": tool_success_rate,
            "pressureSessions": pressure_sessions,
            "criticalSessions": critical_sessions,
        },
        "agents": agent_cards[:6],
        "sessions": sorted(
            session_rows,
            key=lambda item: (item["band"] != "critical", item["band"] != "watch", -item["pressureHits"], -(item["avgResponseSeconds"] or 0)),
        )[:8],
    }


def build_operational_reports(task_index, relays, events, management_runs, health_data, now):
    daily = []
    for offset in range(6, -1, -1):
        day_start = (now - timedelta(days=offset)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)
        completed = 0
        blocked = 0
        signals = 0
        for task in task_index:
            updated_dt = parse_iso(task.get("updatedAt"))
            if not updated_dt or not (day_start <= updated_dt < day_end):
                continue
            state = str(task.get("state", "")).lower()
            if state in TERMINAL_STATES:
                completed += 1
            if task.get("blocked"):
                blocked += 1
        for event in events:
            event_dt = parse_iso(event.get("at"))
            if event_dt and day_start <= event_dt < day_end:
                signals += 1
        daily.append(
            {
                "date": day_start.strftime("%m-%d"),
                "completed": completed,
                "blocked": blocked,
                "signals": signals,
            }
        )

    bottlenecks = []
    for agent in (health_data.get("agents") or [])[:6]:
        if agent.get("blockedTasks") or agent.get("band") in {"watch", "critical"}:
            bottlenecks.append(
                {
                    "title": agent["title"],
                    "detail": f"阻塞 {agent['blockedTasks']} · 完成率 {agent['completionRate']}%",
                    "type": "agent",
                }
            )
    stage_counter = Counter(run.get("stageKey", "unknown") for run in management_runs if run.get("status") == "blocked")
    for stage_key, count in stage_counter.most_common(3):
        bottlenecks.append(
            {
                "title": f"{stage_key} 阶段阻塞",
                "detail": f"{count} 条管理 Run 目前卡在这里",
                "type": "stage",
            }
        )
    relay_leaders = [
        {
            "route": f"{item.get('from')} -> {item.get('to')}",
            "count": item.get("count", 0),
            "lastAgo": item.get("lastAgo", ""),
        }
        for item in (relays or [])[:5]
    ]
    return {
        "daily": daily,
        "weekly": {
            "completed": sum(item["completed"] for item in daily),
            "blockedTouches": sum(item["blocked"] for item in daily),
            "signals": sum(item["signals"] for item in daily),
            "relayCount": sum(item.get("count", 0) for item in relays),
        },
        "bottlenecks": bottlenecks[:6],
        "relayLeaders": relay_leaders,
    }


def summarize_notification_target(channel):
    channel_type = str((channel or {}).get("type") or "").strip().lower()
    target = str((channel or {}).get("target") or "").strip()
    if channel_type == "telegram":
        return target or "Telegram chat"
    if channel_type == "feishu":
        return target or "Feishu webhook"
    return target or "Webhook"


def notification_channel_health(channel):
    meta = (channel or {}).get("meta")
    if isinstance(meta, dict):
        health = meta.get("health")
        if isinstance(health, dict):
            return health
    return {}


def notification_channel_available(channel):
    if str((channel or {}).get("status") or "").strip().lower() != "active":
        return False
    return str(notification_channel_health(channel).get("status") or "").strip().lower() not in {"error", "disabled"}


def probe_notification_channel(channel, allow_disabled_probe=False, openclaw_dir=""):
    channel_type = str((channel or {}).get("type") or "").strip().lower()
    status = str((channel or {}).get("status") or "").strip().lower()
    target = str((channel or {}).get("target") or "").strip()
    secret = str((channel or {}).get("secret") or "").strip()
    if status != "active" and not allow_disabled_probe:
        return {"status": "disabled", "ok": False, "detail": "通道已停用", "method": "status"}
    if channel_type == "telegram":
        if not secret or not target:
            return {"status": "error", "ok": False, "detail": "缺少 bot token 或 chat id", "method": "config"}
        if target.startswith("fixture://"):
            return {"status": "ok", "ok": True, "detail": f"Fixture 通道可用 · {summarize_notification_target(channel)}", "method": "fixture"}
        try:
            response = guarded_http_request(
                openclaw_dir,
                f"https://api.telegram.org/bot{secret}/getMe",
                method="GET",
                timeout=6,
                audit_context="notification-telegram-probe",
            )
            status_code = int(response.get("status") or 0)
            body = str(response.get("body") or "")
            return {
                "status": "ok" if 200 <= status_code < 300 else "error",
                "ok": 200 <= status_code < 300,
                "detail": f"Telegram API 可达 · {body[:120] or f'HTTP {status_code}'}" if 200 <= status_code < 300 else f"Telegram API 返回 HTTP {status_code}",
                "method": "api",
            }
        except RuntimeError as error:
            return {"status": "error", "ok": False, "detail": f"Telegram API 不可达: {error}", "method": "api"}
    if channel_type in {"feishu", "webhook"}:
        if not target:
            return {"status": "error", "ok": False, "detail": "缺少目标地址", "method": "config"}
        if target.startswith("fixture://"):
            return {"status": "ok", "ok": True, "detail": f"Fixture 通道可用 · {summarize_notification_target(channel)}", "method": "fixture"}
        parsed = urlsplit(target)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return {"status": "error", "ok": False, "detail": "目标地址不是合法的 HTTP/HTTPS URL", "method": "config"}
        try:
            response = guarded_http_request(
                openclaw_dir,
                target,
                method="GET",
                timeout=6,
                audit_context="notification-webhook-probe",
            )
            status_code = int(response.get("status") or 0)
            if 200 <= status_code < 500:
                return {"status": "ok", "ok": True, "detail": f"地址可达 · HTTP {status_code}", "method": "reachability"}
            return {"status": "error", "ok": False, "detail": f"地址返回 HTTP {status_code}", "method": "reachability"}
        except RuntimeError as error:
            return {"status": "error", "ok": False, "detail": f"地址不可达: {error}", "method": "reachability"}
    return {"status": "error", "ok": False, "detail": f"不支持的通道类型: {channel_type or 'unknown'}", "method": "config"}


def refresh_notification_channel_health(openclaw_dir, channels, now, force=False):
    refreshed = []
    probe_count = 0
    interval = timedelta(minutes=5)
    for channel in channels:
        health = notification_channel_health(channel)
        last_probe = parse_iso(health.get("lastProbeAt"))
        if not force and last_probe and now - last_probe < interval:
            refreshed.append(channel)
            continue
        remediation = (channel.get("meta") or {}).get("automationRemediation") if isinstance(channel.get("meta"), dict) else {}
        allow_disabled_probe = str(channel.get("status") or "").strip().lower() == "disabled" and bool(
            isinstance(remediation, dict) and remediation.get("disabledByAutomation")
        )
        result = probe_notification_channel(channel, allow_disabled_probe=allow_disabled_probe, openclaw_dir=openclaw_dir)
        probe_count += 1
        next_meta = deepcopy(channel.get("meta", {})) if isinstance(channel.get("meta"), dict) else {}
        previous_failures = int(health.get("consecutiveFailures") or 0)
        next_meta["health"] = {
            "status": result.get("status", "unknown"),
            "ok": bool(result.get("ok")),
            "detail": result.get("detail", ""),
            "method": result.get("method", "unknown"),
            "lastProbeAt": now.isoformat().replace("+00:00", "Z"),
            "lastError": "" if result.get("ok") else result.get("detail", ""),
            "consecutiveFailures": 0 if result.get("ok") else previous_failures + 1,
        }
        refreshed.append(store_save_notification_channel(openclaw_dir, {**channel, "meta": next_meta}))
    return refreshed, probe_count


def normalize_escalation_steps(raw_steps):
    steps = []
    if not isinstance(raw_steps, list):
        return steps
    for item in raw_steps:
        if not isinstance(item, dict):
            continue
        try:
            after_minutes = max(int(item.get("afterMinutes", 0) or 0), 0)
        except (TypeError, ValueError):
            after_minutes = 0
        channel_ids = [
            str(channel_id).strip()
            for channel_id in (item.get("channelIds") if isinstance(item.get("channelIds"), list) else [])
            if str(channel_id).strip()
        ]
        label = str(item.get("label") or "").strip()
        manual = bool(item.get("manual")) or (not channel_ids and bool(label))
        if not channel_ids and not manual:
            continue
        steps.append(
            {
                "afterMinutes": after_minutes,
                "channelIds": channel_ids,
                "label": label,
                "manual": manual,
            }
        )
    return sorted(steps, key=lambda item: item["afterMinutes"])


def normalize_weekly_report_schedule(raw_schedule):
    raw = raw_schedule if isinstance(raw_schedule, dict) else {}
    try:
        weekday = int(1 if raw.get("weekday") is None else raw.get("weekday"))
    except (TypeError, ValueError):
        weekday = 1
    try:
        hour = int(9 if raw.get("hour") is None else raw.get("hour"))
    except (TypeError, ValueError):
        hour = 9
    try:
        minute = int(0 if raw.get("minute") is None else raw.get("minute"))
    except (TypeError, ValueError):
        minute = 0
    return {
        "weekday": min(max(weekday, 1), 7),
        "hour": min(max(hour, 0), 23),
        "minute": min(max(minute, 0), 59),
    }


def normalize_daily_review_schedule(raw_schedule):
    raw = raw_schedule if isinstance(raw_schedule, dict) else {}
    try:
        hour = int(18 if raw.get("hour") is None else raw.get("hour"))
    except (TypeError, ValueError):
        hour = 18
    try:
        minute = int(30 if raw.get("minute") is None else raw.get("minute"))
    except (TypeError, ValueError):
        minute = 30
    return {
        "hour": min(max(hour, 0), 23),
        "minute": min(max(minute, 0), 59),
    }


def weekly_report_due(rule, now):
    schedule = normalize_weekly_report_schedule(((rule.get("meta") or {}).get("weeklyReport")))
    local_now = now.astimezone()
    iso = local_now.isocalendar()
    scheduled = local_now.replace(hour=schedule["hour"], minute=schedule["minute"], second=0, microsecond=0)
    scheduled = scheduled - timedelta(days=max(local_now.isoweekday() - schedule["weekday"], 0))
    due = local_now.isoweekday() == schedule["weekday"] and local_now >= scheduled
    week_key = f"{iso.year}-W{iso.week:02d}"
    return {"due": due, "weekKey": week_key, "scheduledAt": scheduled, "schedule": schedule}


def daily_review_due(rule, now):
    schedule = normalize_daily_review_schedule(((rule.get("meta") or {}).get("dailyReview")))
    local_now = now.astimezone()
    scheduled = local_now.replace(hour=schedule["hour"], minute=schedule["minute"], second=0, microsecond=0)
    due = local_now >= scheduled
    day_key = local_now.strftime("%Y-%m-%d")
    return {"due": due, "dayKey": day_key, "scheduledAt": scheduled, "schedule": schedule}


def resolve_alert_escalation(rule, alert, now, channels):
    channels = channels or {}
    base_channel_ids = [
        item
        for item in (rule.get("channelIds", []) or [])
        if notification_channel_available(channels.get(item, {}))
    ]
    steps = normalize_escalation_steps(((rule.get("meta") or {}).get("escalationSteps")))
    triggered_at = parse_iso(alert.get("triggeredAt")) or now
    age_minutes = max(int((now - triggered_at).total_seconds() // 60), 0)
    eligible_channel_ids = list(base_channel_ids)
    reached_steps = []
    current_step = None
    for index, step in enumerate(steps, start=1):
        if age_minutes < step["afterMinutes"]:
            continue
        reached = {
            "index": index,
            "afterMinutes": step["afterMinutes"],
            "label": step.get("label", ""),
            "manual": bool(step.get("manual")),
            "channelIds": [
                channel_id
                for channel_id in step.get("channelIds", [])
                if notification_channel_available(channels.get(channel_id, {}))
            ],
        }
        eligible_channel_ids.extend(reached["channelIds"])
        reached_steps.append(reached)
        current_step = reached
    deduped = []
    seen = set()
    for channel_id in eligible_channel_ids:
        if channel_id in seen:
            continue
        seen.add(channel_id)
        deduped.append(channel_id)
    return {
        "ageMinutes": age_minutes,
        "eligibleChannelIds": deduped,
        "reachedSteps": reached_steps,
        "currentStep": current_step,
    }


def task_started_reference_at(task):
    task = task if isinstance(task, dict) else {}
    return parse_iso(task.get("createdAt")) or parse_iso(task.get("updatedAt"))


def task_supervision_team_id(task):
    task = task if isinstance(task, dict) else {}
    route_meta = task.get("routeDecision") if isinstance(task.get("routeDecision"), dict) else {}
    if not route_meta and isinstance(task.get("meta"), dict):
        route_meta = (task.get("meta") or {}).get("routeDecision", {})
    team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
    return (
        str(task.get("teamId") or "").strip()
        or str(team_assignment.get("teamId") or "").strip()
        or str(route_meta.get("teamId") or "").strip()
    )


def build_incomplete_task_supervision_note(task, age_minutes, rule):
    task = task if isinstance(task, dict) else {}
    rule = rule if isinstance(rule, dict) else {}
    update_text = str(task.get("currentUpdate") or task.get("now") or "").strip()
    parts = [
        f"监督提醒：这条任务已经 {age_minutes} 分钟仍未完成，请马上重新对齐负责范围、下一步和依赖。",
    ]
    if update_text:
        parts.append(f"当前记录进展：{update_text}")
    if str(rule.get("name") or "").strip():
        parts.append(f"监督规则：{str(rule.get('name') or '').strip()}")
    parts.append("这是一轮督办同步：谁能直接补位就直接接住，不要只回收到。")
    return "\n".join(parts)


def recommended_management_rules():
    bootstrap_version = int(MANAGEMENT_BOOTSTRAP_VERSION)
    return [
        {
            "id": "default-blocked-task-timeout",
            "name": "阻塞超过 30 分钟自动升级",
            "description": "当任务阻塞超过 30 分钟时，自动生成运营告警，提醒负责人介入。",
            "triggerType": "blocked_task_timeout",
            "thresholdMinutes": 30,
            "cooldownMinutes": 90,
            "severity": "warning",
            "matchText": "",
            "status": "active",
            "channelIds": [],
            "meta": {
                "source": "product-default",
                "bootstrapVersion": bootstrap_version,
                "escalationSteps": [
                    {"afterMinutes": 30, "channelIds": [], "label": "升级到飞书值班群"},
                    {"afterMinutes": 60, "channelIds": [], "label": "升级到 Telegram 值班链路"},
                    {"afterMinutes": 90, "channelIds": [], "label": "升级到人工接管", "manual": True},
                ],
            },
        },
        {
            "id": "default-critical-task-done",
            "name": "S 级任务完成自动通知",
            "description": "关键任务完成后，自动向运营群同步结果。",
            "triggerType": "critical_task_done",
            "thresholdMinutes": 0,
            "cooldownMinutes": 240,
            "severity": "critical",
            "matchText": "S级",
            "status": "active",
            "channelIds": [],
            "meta": {
                "source": "product-default",
                "bootstrapVersion": bootstrap_version,
                "escalationSteps": [
                    {"afterMinutes": 0, "channelIds": [], "label": "先同步主通知通道"},
                    {"afterMinutes": 30, "channelIds": [], "label": "升级到人工接管", "manual": True},
                ],
            },
        },
        {
            "id": "default-task-incomplete-timeout",
            "name": "任务超时未完成自动督办",
            "description": "当活跃任务在阈值内仍未完成时，自动触发一轮团队督办同步，并生成运营告警。",
            "triggerType": "task_incomplete_timeout",
            "thresholdMinutes": 120,
            "cooldownMinutes": 60,
            "severity": "warning",
            "matchText": "",
            "status": "active",
            "channelIds": [],
            "meta": {
                "source": "product-default",
                "bootstrapVersion": bootstrap_version,
                "escalationSteps": [
                    {"afterMinutes": 0, "channelIds": [], "label": "立即发起团队督办同步"},
                    {"afterMinutes": 60, "channelIds": [], "label": "60 分钟后升级到人工接管", "manual": True},
                ],
            },
        },
        {
            "id": "default-agent-offline",
            "name": "Agent 离线超过 20 分钟提醒",
            "description": "当核心 Agent 长时间没有新信号时，自动发出巡检提醒。",
            "triggerType": "agent_offline",
            "thresholdMinutes": 20,
            "cooldownMinutes": 60,
            "severity": "warning",
            "matchText": "",
            "status": "active",
            "channelIds": [],
            "meta": {
                "source": "product-default",
                "bootstrapVersion": bootstrap_version,
                "escalationSteps": [
                    {"afterMinutes": 20, "channelIds": [], "label": "提醒运维值班"},
                    {"afterMinutes": 45, "channelIds": [], "label": "升级到人工接管", "manual": True},
                ],
            },
        },
        {
            "id": "default-daily-review-push",
            "name": "每日经营复盘自动推送",
            "description": "每天自动生成经营复盘，帮助团队回看完成情况、风险和第二天重点。",
            "triggerType": "daily_review_push",
            "thresholdMinutes": 0,
            "cooldownMinutes": 1440,
            "severity": "info",
            "matchText": "",
            "status": "active",
            "channelIds": [],
            "meta": {
                "source": "product-default",
                "bootstrapVersion": bootstrap_version,
                "dailyReview": {"hour": 18, "minute": 30},
                "escalationSteps": [
                    {"afterMinutes": 30, "channelIds": [], "label": "30 分钟后升级到人工接管", "manual": True},
                ],
            },
        },
        {
            "id": "default-weekly-report-push",
            "name": "每周运营周报自动推送",
            "description": "按周自动生成运营周报并推送到指定通知通道。",
            "triggerType": "weekly_report_push",
            "thresholdMinutes": 0,
            "cooldownMinutes": 10080,
            "severity": "info",
            "matchText": "",
            "status": "active",
            "channelIds": [],
            "meta": {
                "source": "product-default",
                "bootstrapVersion": bootstrap_version,
                "weeklyReport": {"weekday": 1, "hour": 9, "minute": 0},
                "escalationSteps": [
                    {"afterMinutes": 30, "channelIds": [], "label": "30 分钟后升级到人工接管", "manual": True},
                ],
            },
        },
    ]


def bootstrap_management_rules(openclaw_dir):
    existing = store_list_automation_rules(openclaw_dir)
    existing_ids = {str(item.get("id") or "").strip() for item in existing if str(item.get("id") or "").strip()}
    existing_names = {item.get("name", "") for item in existing}
    created = []
    for payload in recommended_management_rules():
        if str(payload.get("id") or "").strip() in existing_ids or payload["name"] in existing_names:
            continue
        created.append(store_save_automation_rule(openclaw_dir, payload))
    return {"created": created, "total": len(created)}


def render_management_weekly_report(openclaw_dir, management, theme, generated_at):
    report = management.get("reports", {}) if isinstance(management, dict) else {}
    health = management.get("agentHealth", {}) if isinstance(management, dict) else {}
    automation = management.get("automation", {}) if isinstance(management, dict) else {}
    weekly = report.get("weekly", {}) if isinstance(report, dict) else {}
    lines = [
        "# OpenClaw Team Weekly Ops Report",
        "",
        f"- Generated At: {generated_at}",
        f"- Theme: {theme.get('displayName', theme.get('name', 'unknown')) if isinstance(theme, dict) else 'unknown'}",
        "",
        "## Weekly Summary",
        "",
        f"- Completed Tasks: {weekly.get('completed', 0)}",
        f"- Blocked Touches: {weekly.get('blockedTouches', 0)}",
        f"- Activity Signals: {weekly.get('signals', 0)}",
        f"- Relay Count: {weekly.get('relayCount', 0)}",
        f"- Average Agent Health: {(health.get('summary') or {}).get('averageScore', 0)} / 100",
        f"- Active Rules: {(automation.get('summary') or {}).get('activeRules', 0)}",
        f"- Open Alerts: {(automation.get('summary') or {}).get('openAlerts', 0)}",
        "",
        "## Top Bottlenecks",
        "",
    ]
    bottlenecks = report.get("bottlenecks", []) if isinstance(report, dict) else []
    if bottlenecks:
        for item in bottlenecks:
            lines.append(f"- {item.get('title', 'Unknown')}: {item.get('detail', '')}")
    else:
        lines.append("- No major bottlenecks were detected this week.")
    lines.extend(["", "## Relay Leaders", ""])
    relay_leaders = report.get("relayLeaders", []) if isinstance(report, dict) else []
    if relay_leaders:
        for item in relay_leaders:
            lines.append(f"- {item.get('route', 'Unknown')}: {item.get('count', 0)} handoffs, last {item.get('lastAgo', 'unknown')}")
    else:
        lines.append("- No standout relay routes this week.")
    lines.extend(["", "## Agent Health", ""])
    for agent in (health.get("agents", []) if isinstance(health, dict) else [])[:8]:
        lines.append(
            f"- {agent.get('title', 'Agent')}: score {agent.get('score', 0)}, completion {agent.get('completionRate', 0)}%, blocked {agent.get('blockedTasks', 0)}, avg response {agent.get('avgResponseSeconds', 0)}s"
        )
    return "\n".join(lines).strip() + "\n"


def render_management_daily_review(openclaw_dir, management, theme, generated_at):
    report = management.get("reports", {}) if isinstance(management, dict) else {}
    health = management.get("agentHealth", {}) if isinstance(management, dict) else {}
    automation = management.get("automation", {}) if isinstance(management, dict) else {}
    daily_rows = safe_list(report.get("daily"))
    today = daily_rows[-1] if daily_rows else {}
    bottlenecks = safe_list(report.get("bottlenecks"))
    lines = [
        "# OpenClaw Team Daily Review",
        "",
        f"- Generated At: {generated_at}",
        f"- Theme: {theme.get('displayName', theme.get('name', 'unknown')) if isinstance(theme, dict) else 'unknown'}",
        f"- Day: {today.get('date', 'unknown')}",
        "",
        "## Today",
        "",
        f"- Completed Tasks: {today.get('completed', 0)}",
        f"- Blocked Touches: {today.get('blocked', 0)}",
        f"- Activity Signals: {today.get('signals', 0)}",
        f"- Open Alerts: {(automation.get('summary') or {}).get('openAlerts', 0)}",
        "",
        "## What Needs Attention",
        "",
    ]
    if bottlenecks:
        for item in bottlenecks[:5]:
            lines.append(f"- {item.get('title', 'Unknown')}: {item.get('detail', '')}")
    else:
        lines.append("- Today looks stable. No obvious bottleneck needs escalation.")
    lines.extend(["", "## Team Pulse", ""])
    for agent in (health.get("agents", []) if isinstance(health, dict) else [])[:6]:
        lines.append(
            f"- {agent.get('title', 'Agent')}: score {agent.get('score', 0)}, blocked {agent.get('blockedTasks', 0)}, recent completion {agent.get('completionRate', 0)}%"
        )
    lines.extend(
        [
            "",
            "## Next-Day Focus",
            "",
            "- Keep the lead owner clear for every active task.",
            "- Resolve the highest-friction blocker first, then update the team lane with a concrete next step.",
            "- End the day with one explicit conclusion and the first move for tomorrow.",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def export_management_weekly_report(openclaw_dir):
    data = build_dashboard_data(openclaw_dir, skip_automation_cycle=True)
    output_path = dashboard_dir(openclaw_dir) / "weekly-ops-report.md"
    output_path.write_text(
        render_management_weekly_report(openclaw_dir, data.get("management", {}), data.get("theme", {}), data.get("generatedAt", now_iso())),
        encoding="utf-8",
    )
    return {"path": str(output_path), "generatedAt": data.get("generatedAt", now_iso())}


def export_management_daily_review(openclaw_dir):
    data = build_dashboard_data(openclaw_dir, skip_automation_cycle=True)
    output_path = dashboard_dir(openclaw_dir) / "daily-ops-review.md"
    output_path.write_text(
        render_management_daily_review(openclaw_dir, data.get("management", {}), data.get("theme", {}), data.get("generatedAt", now_iso())),
        encoding="utf-8",
    )
    return {"path": str(output_path), "generatedAt": data.get("generatedAt", now_iso())}


def process_daily_review_push(openclaw_dir, automation, now):
    rules = [item for item in safe_list((automation or {}).get("rules")) if item.get("status") == "active"]
    channels = {item["id"]: item for item in safe_list((automation or {}).get("channels")) if item.get("id")}
    existing_deliveries = store_list_notification_deliveries(openclaw_dir, limit=240)
    delivery_map = {(item.get("alertId"), item.get("channelId")): item for item in existing_deliveries}
    report_count = 0
    delivery_attempt_count = 0
    delivery_success_count = 0
    manual_escalation_count = 0

    for rule in rules:
        if rule.get("triggerType") != "daily_review_push":
            continue
        due = daily_review_due(rule, now)
        if not due.get("due"):
            continue
        report = export_management_daily_review(openclaw_dir)
        event_key = f"daily-review:{rule.get('id')}:{due['dayKey']}"
        title = f"每日经营复盘已生成 · {due['dayKey']}"
        detail = f"今日经营复盘已生成，可在 {report['path']} 查看。"
        alert = store_upsert_automation_alert(
            openclaw_dir,
            {
                "ruleId": rule["id"],
                "eventKey": event_key,
                "title": title,
                "detail": detail,
                "severity": rule.get("severity", "info"),
                "status": "open",
                "sourceType": "report",
                "sourceId": report["path"],
                "triggeredAt": due["scheduledAt"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "meta": {
                    "triggerType": "daily_review_push",
                    "dayKey": due["dayKey"],
                    "report": report,
                    "dailyReview": due["schedule"],
                },
            },
        )
        report_count += 1
        escalation = resolve_alert_escalation(rule, alert, now, channels)
        alert_meta = deepcopy(alert.get("meta", {})) if isinstance(alert.get("meta"), dict) else {}
        alert_meta["escalation"] = {
            "ageMinutes": escalation.get("ageMinutes", 0),
            "reachedSteps": escalation.get("reachedSteps", []),
            "currentStep": escalation.get("currentStep"),
            "eligibleChannelIds": escalation.get("eligibleChannelIds", []),
        }
        any_success = False
        for channel_id in escalation.get("eligibleChannelIds", []):
            pair = (alert.get("id"), channel_id)
            prior_delivery = delivery_map.get(pair)
            cooldown_minutes = int(rule.get("cooldownMinutes") or 0)
            if prior_delivery:
                delivered_at = parse_iso(prior_delivery.get("deliveredAt"))
                within_cooldown = delivered_at and cooldown_minutes > 0 and now - delivered_at < timedelta(minutes=cooldown_minutes)
                if prior_delivery.get("outcome") == "success" and within_cooldown:
                    any_success = True
                    continue
            channel = channels.get(channel_id)
            if not channel:
                continue
            delivery_attempt_count += 1
            try:
                result = send_notification_message(channel, alert, openclaw_dir=openclaw_dir)
                outcome = "success" if result.get("ok") else "error"
                result_detail = result.get("detail", "")
            except Exception as error:
                outcome = "error"
                result_detail = str(error)
            delivery = store_save_notification_delivery(
                openclaw_dir,
                alert["id"],
                channel_id,
                outcome,
                detail=result_detail,
                meta={"channelType": channel.get("type", ""), "kind": "dailyReview"},
            )
            delivery_map[pair] = delivery
            if outcome == "success":
                delivery_success_count += 1
                any_success = True
        current_step = escalation.get("currentStep") or {}
        if current_step.get("manual"):
            manual_escalation_count += 1
        alert_status = "notified" if any_success else ("error" if escalation.get("eligibleChannelIds") or current_step.get("manual") else alert.get("status", "open"))
        store_upsert_automation_alert(
            openclaw_dir,
            {
                **alert,
                "status": alert_status,
                "meta": alert_meta,
            },
        )
    previous_engine = (automation or {}).get("engine") if isinstance((automation or {}).get("engine"), dict) else {}
    return {
        **(automation or {}),
        "engine": {
            **previous_engine,
            "dailyReviewCount": int(previous_engine.get("dailyReviewCount") or 0) + report_count,
        },
        "dailyReview": {
            "count": report_count,
            "deliveryAttemptCount": delivery_attempt_count,
            "deliverySuccessCount": delivery_success_count,
            "manualEscalationCount": manual_escalation_count,
        },
    }


def process_weekly_report_push(openclaw_dir, automation, now):
    rules = [item for item in safe_list((automation or {}).get("rules")) if item.get("status") == "active"]
    channels = {item["id"]: item for item in safe_list((automation or {}).get("channels")) if item.get("id")}
    existing_deliveries = store_list_notification_deliveries(openclaw_dir, limit=240)
    delivery_map = {(item.get("alertId"), item.get("channelId")): item for item in existing_deliveries}
    report_count = 0
    delivery_attempt_count = 0
    delivery_success_count = 0
    manual_escalation_count = 0

    for rule in rules:
        if rule.get("triggerType") != "weekly_report_push":
            continue
        due = weekly_report_due(rule, now)
        if not due.get("due"):
            continue
        report = export_management_weekly_report(openclaw_dir)
        event_key = f"weekly-report:{rule.get('id')}:{due['weekKey']}"
        title = f"运营周报已生成 · {due['weekKey']}"
        detail = f"本周运营周报已生成，可在 {report['path']} 查看。"
        alert = store_upsert_automation_alert(
            openclaw_dir,
            {
                "ruleId": rule["id"],
                "eventKey": event_key,
                "title": title,
                "detail": detail,
                "severity": rule.get("severity", "info"),
                "status": "open",
                "sourceType": "report",
                "sourceId": report["path"],
                "triggeredAt": due["scheduledAt"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "meta": {
                    "triggerType": "weekly_report_push",
                    "weekKey": due["weekKey"],
                    "report": report,
                    "weeklyReport": due["schedule"],
                },
            },
        )
        report_count += 1
        escalation = resolve_alert_escalation(rule, alert, now, channels)
        alert_meta = deepcopy(alert.get("meta", {})) if isinstance(alert.get("meta"), dict) else {}
        alert_meta["escalation"] = {
            "ageMinutes": escalation.get("ageMinutes", 0),
            "reachedSteps": escalation.get("reachedSteps", []),
            "currentStep": escalation.get("currentStep"),
            "eligibleChannelIds": escalation.get("eligibleChannelIds", []),
        }
        any_success = False
        for channel_id in escalation.get("eligibleChannelIds", []):
            pair = (alert.get("id"), channel_id)
            prior_delivery = delivery_map.get(pair)
            cooldown_minutes = int(rule.get("cooldownMinutes") or 0)
            if prior_delivery:
                delivered_at = parse_iso(prior_delivery.get("deliveredAt"))
                within_cooldown = delivered_at and cooldown_minutes > 0 and now - delivered_at < timedelta(minutes=cooldown_minutes)
                if prior_delivery.get("outcome") == "success" and within_cooldown:
                    any_success = True
                    continue
            channel = channels.get(channel_id)
            if not channel:
                continue
            try:
                result = send_notification_message(channel, alert, openclaw_dir=openclaw_dir)
                outcome = "success" if result.get("ok") else "error"
                result_detail = result.get("detail", "")
            except Exception as error:
                outcome = "error"
                result_detail = str(error)
            delivery_attempt_count += 1
            delivery = store_save_notification_delivery(
                openclaw_dir,
                alert["id"],
                channel_id,
                outcome,
                detail=result_detail,
                meta={"channelType": channel.get("type", ""), "kind": "weeklyReport"},
            )
            delivery_map[pair] = delivery
            if outcome == "success":
                any_success = True
                delivery_success_count += 1
        current_step = escalation.get("currentStep") or {}
        if current_step.get("manual"):
            manual_escalation_count += 1
        next_status = "notified" if any_success else ("error" if escalation.get("eligibleChannelIds") or current_step.get("manual") else "open")
        store_upsert_automation_alert(
            openclaw_dir,
            {
                "id": alert["id"],
                "ruleId": alert["ruleId"],
                "eventKey": alert["eventKey"],
                "title": alert["title"],
                "detail": alert["detail"],
                "severity": alert["severity"],
                "status": next_status,
                "sourceType": alert.get("sourceType", ""),
                "sourceId": alert.get("sourceId", ""),
                "triggeredAt": alert.get("triggeredAt", now_iso()),
                "meta": alert_meta,
            },
        )
        if any_success and alert.get("status") != "notified":
            store_upsert_automation_alert(
                openclaw_dir,
                {
                    "id": alert["id"],
                    "ruleId": alert["ruleId"],
                    "eventKey": alert["eventKey"],
                    "title": alert["title"],
                    "detail": alert["detail"],
                    "severity": alert["severity"],
                    "status": "notified",
                    "sourceType": alert.get("sourceType", ""),
                    "sourceId": alert.get("sourceId", ""),
                    "triggeredAt": alert.get("triggeredAt", now_iso()),
                    "meta": alert_meta,
                },
            )

    snapshot = build_automation_snapshot(openclaw_dir, rules, channels)
    previous_engine = (automation or {}).get("engine") if isinstance((automation or {}).get("engine"), dict) else {}
    snapshot_engine = snapshot.get("engine") if isinstance(snapshot.get("engine"), dict) else {}
    return {
        **(automation or {}),
        **snapshot,
        "engine": {
            **snapshot_engine,
            **previous_engine,
            "weeklyReportCount": int(previous_engine.get("weeklyReportCount") or 0) + report_count,
            "deliveryAttemptCount": int(previous_engine.get("deliveryAttemptCount") or 0) + delivery_attempt_count,
            "deliverySuccessCount": int(previous_engine.get("deliverySuccessCount") or 0) + delivery_success_count,
            "manualEscalationCount": int(previous_engine.get("manualEscalationCount") or 0) + manual_escalation_count,
        },
        "weeklyReport": {
            "count": report_count,
            "deliveryAttemptCount": delivery_attempt_count,
            "deliverySuccessCount": delivery_success_count,
            "manualEscalationCount": manual_escalation_count,
        },
    }
