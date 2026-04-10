from __future__ import annotations

import sys
from collections import Counter, defaultdict
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


CONVERSATION_SOURCE_LABELS = _DelegatedSymbol("CONVERSATION_SOURCE_LABELS")
MANAGEMENT_BOOTSTRAP_VERSION = _DelegatedSymbol("MANAGEMENT_BOOTSTRAP_VERSION")
PRODUCT_MODE_TEAM = _DelegatedSymbol("PRODUCT_MODE_TEAM")
TEAM_ALWAYS_ON_RUNTIME_EVERY = _DelegatedSymbol("TEAM_ALWAYS_ON_RUNTIME_EVERY")
TEAM_ALWAYS_ON_RUNTIME_MODE = _DelegatedSymbol("TEAM_ALWAYS_ON_RUNTIME_MODE")
TEAM_BOOTSTRAP_VERSION = _DelegatedSymbol("TEAM_BOOTSTRAP_VERSION")
THEME_CATALOG = _DelegatedSymbol("THEME_CATALOG")
artifact_deliverable_payload = _DelegatedSymbol("artifact_deliverable_payload")
agent_runtime_overrides = _DelegatedSymbol("agent_runtime_overrides")
bootstrap_management_rules = _DelegatedSymbol("bootstrap_management_rules")
build_customer_access_snapshot = _DelegatedSymbol("build_customer_access_snapshot")
build_decision_source_review = _DelegatedSymbol("build_decision_source_review")
build_management_automation_placeholder = _DelegatedSymbol("build_management_automation_placeholder")
build_operational_reports = _DelegatedSymbol("build_operational_reports")
build_recommendation_accuracy_review = _DelegatedSymbol("build_recommendation_accuracy_review")
build_routing_effectiveness_summary = _DelegatedSymbol("build_routing_effectiveness_summary")
build_task_intelligence_summary = _DelegatedSymbol("build_task_intelligence_summary")
build_team_ownership_payload = _DelegatedSymbol("build_team_ownership_payload")
cached_payload = _DelegatedSymbol("cached_payload")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_agent_team_payload = _DelegatedSymbol("compact_agent_team_payload")
compact_task_reference = _DelegatedSymbol("compact_task_reference")
compact_task_team_reference = _DelegatedSymbol("compact_task_team_reference")
compute_agent_health_data = _DelegatedSymbol("compute_agent_health_data")
compute_conversation_quality_data = _DelegatedSymbol("compute_conversation_quality_data")
current_management_automation_mode = _DelegatedSymbol("current_management_automation_mode")
datetime = _DelegatedSymbol("datetime")
format_age = _DelegatedSymbol("format_age")
heartbeat_config_enabled = _DelegatedSymbol("heartbeat_config_enabled")
hydrate_management_run_pack_context = _DelegatedSymbol("hydrate_management_run_pack_context")
infer_effective_team_runtime_mode = _DelegatedSymbol("infer_effective_team_runtime_mode")
infer_team_runtime_state = _DelegatedSymbol("infer_team_runtime_state")
is_online_reachable_status = _DelegatedSymbol("is_online_reachable_status")
load_audit_events = _DelegatedSymbol("load_audit_events")
load_automation_engine_status = _DelegatedSymbol("load_automation_engine_status")
load_config = _DelegatedSymbol("load_config")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
load_skills_catalog = _DelegatedSymbol("load_skills_catalog")
management_automation_mode_payload = _DelegatedSymbol("management_automation_mode_payload")
ensure_default_memory_bootstrap = _DelegatedSymbol("ensure_default_memory_bootstrap")
memory_system_status_payload = _DelegatedSymbol("memory_system_status_payload")
merged_agent_runtime_profile = _DelegatedSymbol("merged_agent_runtime_profile")
normalize_chat_dispatch_mode = _DelegatedSymbol("normalize_chat_dispatch_mode")
normalize_team_wake_scope = _DelegatedSymbol("normalize_team_wake_scope")
now_iso = _DelegatedSymbol("now_iso")
now_utc = _DelegatedSymbol("now_utc")
parse_iso = _DelegatedSymbol("parse_iso")
planning_binding_from_payload = _DelegatedSymbol("planning_binding_from_payload")
recommended_management_rules = _DelegatedSymbol("recommended_management_rules")
requested_team_runtime_every = _DelegatedSymbol("requested_team_runtime_every")
requested_team_runtime_mode = _DelegatedSymbol("requested_team_runtime_mode")
requested_team_wake_scope = _DelegatedSymbol("requested_team_wake_scope")
resolve_team_default_dispatch_mode = _DelegatedSymbol("resolve_team_default_dispatch_mode")
run_automation_engine_cycle = _DelegatedSymbol("run_automation_engine_cycle")
safe_list = _DelegatedSymbol("safe_list")
save_agent_team_preserving_meta = _DelegatedSymbol("save_agent_team_preserving_meta")
save_project_metadata = _DelegatedSymbol("save_project_metadata")
should_skip_management_automation_cycle = _DelegatedSymbol("should_skip_management_automation_cycle")
store_list_agent_teams = _DelegatedSymbol("store_list_agent_teams")
store_list_automation_rules = _DelegatedSymbol("store_list_automation_rules")
store_list_chat_threads = _DelegatedSymbol("store_list_chat_threads")
store_list_management_runs = _DelegatedSymbol("store_list_management_runs")
store_list_routing_decisions = _DelegatedSymbol("store_list_routing_decisions")
store_save_agent_team = _DelegatedSymbol("store_save_agent_team")
sync_requested_agent_team_runtime_policies = _DelegatedSymbol("sync_requested_agent_team_runtime_policies")
team_collaboration_protocol = _DelegatedSymbol("team_collaboration_protocol")
team_decision_log_text = _DelegatedSymbol("team_decision_log_text")
team_memory_text = _DelegatedSymbol("team_memory_text")
team_member_status_group = _DelegatedSymbol("team_member_status_group")
team_operating_brief = _DelegatedSymbol("team_operating_brief")
team_runtime_meta = _DelegatedSymbol("team_runtime_meta")
timedelta = _DelegatedSymbol("timedelta")
timezone = _DelegatedSymbol("timezone")
workflow_pack_map_from_skills_payload = _DelegatedSymbol("workflow_pack_map_from_skills_payload")


def build_management_runs_data(openclaw_dir, task_index, conversation_data, deliverables, now, skills_data=None, limit=48):
    task_map = {item.get("id"): item for item in task_index if item.get("id")}
    deliverable_map = {item.get("id"): item for item in deliverables if item.get("id")}
    deliverable_ids = {item.get("id") for item in deliverables if item.get("id")}
    team_map = {item.get("id"): item for item in store_list_agent_teams(openclaw_dir) if item.get("id")}
    pack_map = workflow_pack_map_from_skills_payload(skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir))
    session_map = {
        item.get("key"): item
        for item in (conversation_data.get("sessions", []) if isinstance(conversation_data, dict) else [])
        if item.get("key")
    }
    runs = []
    for run in store_list_management_runs(openclaw_dir, limit=limit):
        current_stage = next(
            (stage for stage in run.get("stages", []) if stage.get("key") == run.get("stageKey")),
            {},
        )
        planning_binding = planning_binding_from_payload(run)
        hydrated_run = hydrate_management_run_pack_context(
            {
                **run,
                "updatedAgo": format_age(parse_iso(run.get("updatedAt")), now),
                "createdAgo": format_age(parse_iso(run.get("createdAt")), now),
                "stageLabel": current_stage.get("title") or run.get("stageKey", ""),
                "stageStatus": current_stage.get("status") or ("done" if run.get("status") == "complete" else run.get("status")),
                "linkedTask": compact_task_reference(task_map.get(run.get("linkedTaskId"))),
                "linkedTeam": compact_task_team_reference(team_map.get(run.get("linkedTeamId"))),
                "linkedSession": session_map.get(run.get("linkedSessionKey")),
                "deliverable": deliverable_map.get(run.get("linkedTaskId")),
                "workflowBinding": run.get("workflowBinding", {}),
                "planningBinding": planning_binding,
            },
            pack_map,
        )
        hydrated_run.pop("meta", None)
        hydrated_run["teamOwnership"] = build_team_ownership_payload(
            team_map,
            execution_team_id=str(hydrated_run.get("linkedTeamId") or "").strip() or str((hydrated_run.get("linkedTeam") or {}).get("id") or "").strip(),
            recommended_team_id=str((hydrated_run.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
            mode=str((hydrated_run.get("linkedPack") or {}).get("mode") or "").strip(),
            source="run",
        )
        run_deliverables = []
        task_deliverable = deliverable_map.get(run.get("linkedTaskId"))
        if task_deliverable:
            run_deliverables.append(task_deliverable)
        for artifact in safe_list(hydrated_run.get("artifacts")):
            artifact_deliverable = artifact_deliverable_payload(artifact, hydrated_run, now=now)
            if artifact_deliverable.get("id"):
                run_deliverables.append(artifact_deliverable)
                if artifact_deliverable["id"] not in deliverable_ids:
                    deliverable_ids.add(artifact_deliverable["id"])
                    deliverables.append(artifact_deliverable)
        hydrated_run["deliverables"] = run_deliverables
        hydrated_run["deliverableCount"] = len(run_deliverables)
        alerts = safe_list(hydrated_run.get("alerts"))
        if hydrated_run.get("gateSummary", {}).get("blocked"):
            alerts.append({"severity": "warning", "title": f"{hydrated_run['gateSummary']['blocked']} review gates blocked"})
        if str((hydrated_run.get("releaseAutomation") or {}).get("status") or "").strip().lower() == "error":
            alerts.append({"severity": "critical", "title": "Release automation needs attention"})
        hydrated_run["alerts"] = alerts
        runs.append(hydrated_run)
    return runs


def build_management_data(openclaw_dir, task_index, conversation_data, deliverables, agents, events, relays, now, skills_data=None, skip_automation_cycle=False):
    management_metadata = load_project_metadata(openclaw_dir)
    team_map = {item.get("id"): item for item in store_list_agent_teams(openclaw_dir) if item.get("id")}
    runs = build_management_runs_data(
        openclaw_dir,
        task_index,
        conversation_data,
        deliverables,
        now,
        skills_data=skills_data,
        limit=48,
    )
    stage_counter = Counter()
    status_counter = Counter()
    risk_counter = Counter()
    for run in runs:
        stage_counter[run.get("stageKey", "unknown")] += 1
        status_counter[run.get("status", "active")] += 1
        risk_counter[run.get("riskLevel", "medium")] += 1
    health_data = cached_payload(
        ("management-health", str(openclaw_dir)),
        20,
        lambda: compute_agent_health_data(openclaw_dir, agents, task_index, deliverables, now),
    )
    conversation_quality = cached_payload(
        ("management-conversations", str(openclaw_dir)),
        20,
        lambda: compute_conversation_quality_data(openclaw_dir, conversation_data, now),
    )
    reports = cached_payload(
        ("management-reports", str(openclaw_dir)),
        20,
        lambda: build_operational_reports(task_index, relays, events, runs, health_data, now),
    )
    if skip_automation_cycle or should_skip_management_automation_cycle():
        automation = _svc().build_management_automation_placeholder(openclaw_dir, management_metadata)
    else:
        automation = cached_payload(
            ("management-automation", str(openclaw_dir)),
            15,
            lambda: run_automation_engine_cycle(openclaw_dir, source="dashboard", now=now),
        )
    if team_map:
        management_metadata = ensure_default_memory_bootstrap(
            openclaw_dir,
            load_project_metadata(openclaw_dir),
            agents=agents,
            teams=safe_list(team_map.values()),
        )
    else:
        management_metadata = load_project_metadata(openclaw_dir)
    automation["mode"] = automation.get("mode") if isinstance(automation.get("mode"), dict) else management_automation_mode_payload(
        current_management_automation_mode(management_metadata)
    )
    if not automation.get("engine"):
        automation["engine"] = load_automation_engine_status(openclaw_dir)
    automation["memorySystem"] = memory_system_status_payload(
        management_metadata,
        agents=agents,
        teams=safe_list(team_map.values()),
    )
    automation["customerChannels"] = _svc().build_customer_access_snapshot(openclaw_dir)
    intelligence = build_task_intelligence_summary(task_index)
    routing_decisions = store_list_routing_decisions(openclaw_dir, limit=180)
    decision_quality = build_routing_effectiveness_summary(task_index, routing_decisions)
    decision_source_review = build_decision_source_review(task_index, routing_decisions, now)
    recommendation_review = build_recommendation_accuracy_review(openclaw_dir, task_index, now)
    deliverables.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    return {
        "summary": {
            "total": len(runs),
            "active": sum(1 for item in runs if item.get("status") == "active"),
            "blocked": sum(1 for item in runs if item.get("status") == "blocked"),
            "readyForRelease": sum(1 for item in runs if item.get("stageKey") == "release" and item.get("status") != "complete"),
            "completed": sum(1 for item in runs if item.get("status") == "complete"),
            "plannedRuns": sum(1 for item in runs if item.get("planningBinding")),
            "teamRuns": sum(1 for item in runs if item.get("linkedTeam")),
            "statusBreakdown": dict(status_counter),
            "stageBreakdown": dict(stage_counter),
            "riskBreakdown": dict(risk_counter),
            "manualReviewCount": intelligence.get("manualReviewCount", 0),
            "lowConfidenceCount": intelligence.get("lowConfidenceCount", 0),
            "riskyFallbackCount": intelligence.get("riskyFallbackCount", 0),
            "decisionEvaluatedCount": decision_quality.get("evaluatedCount", 0),
            "decisionCompletionRate": decision_quality.get("completionRate", 0),
            "decisionBlockRate": decision_quality.get("blockRate", 0),
        },
        "runs": runs,
        "agentHealth": health_data,
        "conversationQuality": conversation_quality,
        "reports": reports,
        "automation": automation,
        "intelligence": intelligence,
        "decisionQuality": decision_quality,
        "decisionSourceReview": decision_source_review,
        "recommendationReview": recommendation_review,
    }


def communication_audit_category(action):
    normalized = str(action or "").strip().lower()
    if normalized in {"login", "logout"}:
        return "access"
    if normalized.startswith("conversation_"):
        return "conversation"
    if normalized.startswith("management_channel_"):
        return "channel"
    if normalized in {"task_create", "tenant_task_create"}:
        return "trigger"
    return "system"


def build_communications_data(openclaw_dir, conversation_data, management_data, now):
    sessions = safe_list((conversation_data or {}).get("sessions"))
    automation = (management_data or {}).get("automation") or {}
    channels = safe_list(automation.get("channels"))
    customer_channel_payload = automation.get("customerChannels") if isinstance(automation.get("customerChannels"), dict) else {}
    customer_channels = safe_list(customer_channel_payload.get("channels"))
    alerts = safe_list(automation.get("alerts"))
    audit_events = load_audit_events(openclaw_dir, limit=80)

    terminal_groups = defaultdict(list)
    for session in sessions:
        terminal_groups[session.get("source") or "main"].append(session)

    terminal_records = []
    for source, items in terminal_groups.items():
        ordered = sorted(
            items,
            key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        latest = ordered[0] if ordered else {}
        latest_dt = parse_iso(latest.get("updatedAt"))
        aborted_count = sum(1 for item in ordered if item.get("abortedLastRun"))
        active_24h = sum(
            1
            for item in ordered
            if (parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc)) >= now - timedelta(hours=24)
        )
        talkable = sum(1 for item in ordered if item.get("talkable"))
        status = "attention" if aborted_count else ("active" if active_24h else "idle")
        terminal_records.append(
            {
                "id": source,
                "label": CONVERSATION_SOURCE_LABELS.get(source, source),
                "source": source,
                "status": status,
                "sessionCount": len(ordered),
                "talkableCount": talkable,
                "agentCount": len({item.get("agentId") for item in ordered if item.get("agentId")}),
                "active24h": active_24h,
                "abortedCount": aborted_count,
                "latestAt": latest.get("updatedAt", ""),
                "latestAgo": format_age(latest_dt, now) if latest_dt else "未知时间",
                "preview": latest.get("preview", ""),
                "agents": sorted({item.get("agentLabel") or item.get("agentId") for item in ordered if item.get("agentId")}),
            }
        )
    terminal_records.sort(key=lambda item: (item.get("status") != "attention", -item.get("active24h", 0), -item.get("sessionCount", 0)))

    delivery_rows = []
    for alert in alerts:
        for delivery in safe_list(alert.get("deliveries")):
            delivery_rows.append(
                {
                    **delivery,
                    "alertId": alert.get("id", ""),
                    "alertTitle": alert.get("title", ""),
                    "severity": alert.get("severity", ""),
                    "status": alert.get("status", ""),
                }
            )

    channel_records = []
    for channel in channels:
        channel_deliveries = [item for item in delivery_rows if item.get("channelId") == channel.get("id")]
        success_count = sum(1 for item in channel_deliveries if item.get("outcome") == "success")
        error_count = sum(1 for item in channel_deliveries if item.get("outcome") != "success")
        latest_delivery = next(
            iter(
                sorted(
                    channel_deliveries,
                    key=lambda item: parse_iso(item.get("deliveredAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
                    reverse=True,
                )
            ),
            {},
        )
        latest_dt = parse_iso(latest_delivery.get("deliveredAt"))
        status = "error" if error_count else ("active" if channel.get("status") == "active" else "idle")
        channel_records.append(
            {
                **channel,
                "status": status,
                "successCount": success_count,
                "errorCount": error_count,
                "latestDeliveryAt": latest_delivery.get("deliveredAt", ""),
                "latestDeliveryAgo": format_age(latest_dt, now) if latest_dt else "尚未投递",
                "latestDetail": latest_delivery.get("detail", ""),
                "alertCount": len({item.get("alertId") for item in channel_deliveries if item.get("alertId")}),
            }
        )
    channel_records.sort(key=lambda item: (item.get("status") not in {"error", "active"}, -item.get("errorCount", 0), -item.get("successCount", 0)))

    audit_feed = []
    for event in audit_events:
        category = communication_audit_category(event.get("action"))
        if category == "system":
            continue
        at = parse_iso(event.get("at"))
        audit_feed.append(
            {
                "id": event.get("id"),
                "action": event.get("action", ""),
                "category": category,
                "headline": event.get("detail") or event.get("action", ""),
                "outcome": event.get("outcome", "success"),
                "actor": (event.get("actor") or {}).get("displayName") or (event.get("actor") or {}).get("username") or "system",
                "at": event.get("at", ""),
                "atAgo": format_age(at, now) if at else "未知时间",
                "meta": event.get("meta", {}),
            }
        )

    failures = sorted(
        [item for item in delivery_rows if item.get("outcome") != "success"],
        key=lambda item: parse_iso(item.get("deliveredAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )[:12]

    return {
        "summary": {
            "terminalCount": len(terminal_records),
            "sessionCount": len(sessions),
            "talkableSessions": sum(1 for item in sessions if item.get("talkable")),
            "activeChannels": sum(1 for item in channel_records if item.get("status") in {"active", "error"}),
            "customerChannelCount": len(customer_channels),
            "failedDeliveries": len(failures),
            "auditEvents": len(audit_feed[:24]),
        },
        "terminals": terminal_records,
        "channels": channel_records,
        "customerChannels": customer_channels,
        "failures": failures,
        "audit": audit_feed[:24],
        "commands": [
            {
                "label": "查看真实会话索引",
                "command": f'OPENCLAW_STATE_DIR="{openclaw_dir}" openclaw sessions --all-agents --json',
                "description": "检查当前全部通信会话和渠道来源。",
            },
            {
                "label": "检查 Gateway 健康",
                "command": f'OPENCLAW_STATE_DIR="{openclaw_dir}" openclaw gateway health --json',
                "description": "确认 Telegram、飞书等渠道是否仍然可用。",
            },
        ],
    }


def team_mode_language(metadata):
    theme_name = str(((metadata or {}) if isinstance(metadata, dict) else {}).get("theme") or "").strip()
    language = str(THEME_CATALOG.get(theme_name, {}).get("language", "zh-CN") or "zh-CN").strip().lower()
    return "en" if language.startswith("en") else "zh"


def team_mode_copy(metadata):
    if _svc().team_mode_language(metadata) == "en":
        return {
            "companyName": "All Hands",
            "companyFocus": "Handles company-wide alignment, cross-functional coordination, and issues that need broad support.",
            "companyDescription": "The default all-hands team for org-wide syncs, company notices, and cross-functional collaboration.",
            "companyMemory": "When a request needs broad alignment, name the lead, define the collaboration boundary, and split follow-up to the right teams.",
            "companyDecisions": "Default to a lead summary first, then hand work back to the relevant functional teams.",
            "coreName": "Product & Program",
            "coreFocus": "Owns requirement framing, prioritization, project coordination, and decision-making.",
            "coreDescription": "The default product and program team for scoping work, aligning stakeholders, and closing key decisions.",
            "coreMemory": "Clarify goals, scope, priority, and ownership before pushing work downstream.",
            "coreDecisions": "Default to reaching a product/program decision here before widening to all-hands coordination.",
            "deliveryName": "Engineering Delivery",
            "deliveryFocus": "Owns implementation, integration, bug fixing, and technical delivery.",
            "deliveryDescription": "The default engineering delivery team for build work, integration, and technical execution.",
            "deliveryMemory": "Progress updates should always say who is doing what, how far it is, and what is blocked.",
            "deliveryDecisions": "Default to assigning the responsible builder first, then pull in dependencies as needed.",
            "releaseName": "Quality & Release",
            "releaseFocus": "Owns test verification, release quality gates, launch timing, and rollback readiness.",
            "releaseDescription": "The default quality and release team for QA, launch checks, go-live confirmation, and rollback preparation.",
            "releaseMemory": "Every release conclusion needs evidence, impact scope, and a rollback path.",
            "releaseDecisions": "Keep implementation and release approval separate; require evidence before green-lighting.",
            "signalsName": "Marketing & Operations",
            "signalsFocus": "Owns market signals, customer feedback, operating updates, and external communications.",
            "signalsDescription": "The default marketing and operations team for brand, growth, customer feedback, and outward-facing updates.",
            "signalsMemory": "Before writing outward-facing content, confirm the audience, the core message, and the latest approved wording.",
            "signalsDecisions": "For any external-facing change, settle the message first and then sync the affected teams.",
            "fallbackName": "General Support",
            "fallbackFocus": "Provides broad support while responsibilities are still being split into clearer teams.",
            "fallbackDescription": "The default general support team for transitional or mixed-scope collaboration.",
            "fallbackMemory": "Clarify the lead, the support roles, and then quickly turn temporary collaboration into a clearer team split.",
            "fallbackDecisions": "Default to a lead-owned response first, then split work into clearer teams once the pattern stabilizes.",
        }
    return {
        "companyName": "全员协作组",
        "companyFocus": "负责公司级同步、跨部门协同和需要全员支援的事项。",
        "companyDescription": "默认的全员协作团队，适合公司通知、跨部门协同和需要多人联动的事项。",
        "companyMemory": "涉及公司级联动时，先明确主负责人、协作边界和对外口径，再分派到各职能组。",
        "companyDecisions": "默认先由主负责人汇总结论，再拆给各职能组同步推进。",
        "coreName": "产品与项目组",
        "coreFocus": "负责需求澄清、优先级判断、项目推进和跨团队协调。",
        "coreDescription": "默认的产品项目团队，承接需求判断、排期协调和关键决策收口。",
        "coreMemory": "先把目标、范围、优先级和负责人说清，再分配执行和验收责任。",
        "coreDecisions": "默认先在本组完成判断和优先级收口，再决定是否扩大到全员协作。",
        "deliveryName": "研发交付组",
        "deliveryFocus": "负责方案实现、联调集成、缺陷修复与技术交付。",
        "deliveryDescription": "默认的研发交付团队，承接开发实现、集成和落地推进。",
        "deliveryMemory": "同步进展时要说清谁在做、做到哪、卡点是什么。",
        "deliveryDecisions": "默认先点名负责成员执行，遇到依赖时再拉相关角色加入。",
        "releaseName": "质量发布组",
        "releaseFocus": "负责测试验证、上线把关、发布节奏和回滚预案。",
        "releaseDescription": "默认的质量发布团队，承接 QA、发布检查、上线确认和回滚准备。",
        "releaseMemory": "发布结论必须带验证证据、影响范围和回滚方案。",
        "releaseDecisions": "默认把实现和放行拆开，先补齐验证证据再决定是否上线。",
        "signalsName": "市场运营组",
        "signalsFocus": "负责市场信息、客户反馈、运营动作和对外内容。",
        "signalsDescription": "默认的市场运营团队，承接品牌、增长、客户反馈和内容同步。",
        "signalsMemory": "对外内容先确认受众、核心信息和最新口径，再形成统一输出。",
        "signalsDecisions": "涉及外部感知变化时，先收口口径，再同步到相关职能。",
        "fallbackName": "综合支持组",
        "fallbackFocus": "在职责还未细分时，提供通用协同与补位支持。",
        "fallbackDescription": "默认的综合支持团队，适合临时跨职能协作和过渡期编组。",
        "fallbackMemory": "先明确谁牵头、谁补位，再尽快沉淀成职责更清晰的小队。",
        "fallbackDecisions": "默认先由主负责人收口，再按稳定分工拆成更明确的团队。",
    }


def team_mode_agent_text(agent):
    parts = [
        str(agent.get("id") or "").strip(),
        str(agent.get("title") or "").strip(),
        str(agent.get("name") or "").strip(),
        str(agent.get("role") or "").strip(),
    ]
    parts.extend(str(item).strip() for item in safe_list(agent.get("skills")))
    return " ".join(part for part in parts if part).lower()


def pick_team_agents(agents, keywords, exclude=None):
    exclude_ids = {str(item).strip() for item in safe_list(exclude) if str(item).strip()}
    matched = []
    for agent in safe_list(agents):
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id or agent_id in exclude_ids:
            continue
        text = _svc().team_mode_agent_text(agent)
        if any(keyword in text for keyword in keywords):
            matched.append(agent_id)
    return matched


def build_default_agent_team_blueprints(agents, router_agent_id="", metadata=None):
    ordered_agents = []
    agent_map = {
        str(agent.get("id") or "").strip(): agent
        for agent in safe_list(agents)
        if str(agent.get("id") or "").strip()
    }
    if router_agent_id and router_agent_id in agent_map:
        ordered_agents.append(agent_map[router_agent_id])
    ordered_agents.extend(agent for agent in safe_list(agents) if str(agent.get("id") or "").strip() != router_agent_id)
    agent_ids = [str(agent.get("id") or "").strip() for agent in ordered_agents if str(agent.get("id") or "").strip()]
    copy = _svc().team_mode_copy(metadata)

    core_keywords = (
        "assistant",
        "strategy",
        "planner",
        "plan",
        "compliance",
        "review",
        "reviewer",
        "coo",
        "router",
        "dispatcher",
        "vp_strategy",
        "vp_compliance",
    )
    delivery_keywords = (
        "engineering",
        "devops",
        "data",
        "build",
        "implement",
        "integration",
        "platform",
        "operations",
    )
    release_keywords = (
        "qa",
        "quality",
        "verify",
        "release",
        "ship",
        "deploy",
        "rollback",
        "compliance",
        "devops",
        "quality",
        "qa",
        "vp_compliance",
    )
    signals_keywords = (
        "marketing",
        "hr",
        "briefing",
        "people",
        "growth",
        "brand",
        "news",
    )

    def dedupe(values):
        items = []
        for item in values:
            value = str(item or "").strip()
            if value and value not in items and value in agent_map:
                items.append(value)
        return items

    def available_ids(*values):
        return [value for value in values if str(value or "").strip() in agent_map]

    core_ids = dedupe(
        [router_agent_id, *available_ids("assistant", "vp_strategy", "vp_compliance", "coo")]
        + _svc().pick_team_agents(ordered_agents, core_keywords)
    )
    if not core_ids and agent_ids:
        core_ids = agent_ids[: min(3, len(agent_ids))]
    signals_ids = dedupe(
        [
            *available_ids("marketing", "briefing", "hr"),
            *_svc().pick_team_agents(ordered_agents, signals_keywords, exclude=core_ids),
        ]
    )
    release_ids = dedupe(
        [
            *available_ids("qa", "devops", "vp_compliance"),
            *_svc().pick_team_agents(ordered_agents, release_keywords),
        ]
    )
    delivery_ids = dedupe(
        [
            *available_ids("engineering", "devops", "data_team"),
            *_svc().pick_team_agents(ordered_agents, delivery_keywords, exclude=core_ids),
        ]
    )
    shared_release_delivery_ids = {"devops"}
    delivery_ids = [
        agent_id
        for agent_id in delivery_ids
        if agent_id not in {item for item in release_ids if item not in shared_release_delivery_ids}
    ]

    used = set(core_ids) | set(signals_ids) | set(delivery_ids) | set(release_ids)
    remaining = [agent_id for agent_id in agent_ids if agent_id not in used]
    if remaining:
        if delivery_ids:
            delivery_ids = dedupe(delivery_ids + remaining)
        elif release_ids:
            release_ids = dedupe(release_ids + remaining)
        elif core_ids:
            core_ids = dedupe(core_ids + remaining)
        else:
            core_ids = dedupe(remaining)

    blueprints = []

    def append_team(
        team_id,
        name,
        member_ids,
        focus,
        description,
        default_dispatch_mode="direct",
        default_wake_scope="all",
        team_memory="",
        decision_log="",
        runtime_mode=str(TEAM_ALWAYS_ON_RUNTIME_MODE),
        runtime_every=str(TEAM_ALWAYS_ON_RUNTIME_EVERY),
    ):
        member_ids = dedupe(member_ids)
        if not member_ids:
            return
        blueprints.append(
            {
                "id": team_id,
                "name": name,
                "status": "active",
                "leadAgentId": member_ids[0],
                "memberAgentIds": member_ids,
                "focus": focus,
                "description": description,
                "channel": "internal",
                "operatingBrief": description,
                "defaultDispatchMode": default_dispatch_mode,
                "defaultWakeScope": default_wake_scope,
                "teamMemory": team_memory,
                "decisionLog": decision_log,
                "meta": {
                    "source": "product-default",
                    "bootstrapVersion": int(TEAM_BOOTSTRAP_VERSION),
                    "defaultDispatchMode": default_dispatch_mode,
                    "defaultWakeScope": default_wake_scope,
                    "runtimeMode": runtime_mode,
                    "runtimeEvery": runtime_every,
                    "operatingBrief": description,
                    "teamMemory": team_memory,
                    "decisionLog": decision_log,
                },
            }
        )

    append_team(
        "team-company",
        copy["companyName"],
        agent_ids,
        copy["companyFocus"],
        copy["companyDescription"],
        default_dispatch_mode="direct",
        default_wake_scope="all",
        team_memory=copy["companyMemory"],
        decision_log=copy["companyDecisions"],
    )
    append_team(
        "team-core",
        copy["coreName"],
        core_ids,
        copy["coreFocus"],
        copy["coreDescription"],
        default_dispatch_mode="direct",
        default_wake_scope="all",
        team_memory=copy["coreMemory"],
        decision_log=copy["coreDecisions"],
    )
    if delivery_ids and set(delivery_ids) != set(core_ids):
        append_team(
            "team-delivery",
            copy["deliveryName"],
            delivery_ids,
            copy["deliveryFocus"],
            copy["deliveryDescription"],
            default_dispatch_mode="direct",
            default_wake_scope="all",
            team_memory=copy["deliveryMemory"],
            decision_log=copy["deliveryDecisions"],
        )
    if release_ids and set(release_ids) != set(core_ids):
        append_team(
            "team-release",
            copy["releaseName"],
            release_ids,
            copy["releaseFocus"],
            copy["releaseDescription"],
            default_dispatch_mode="direct",
            default_wake_scope="all",
            team_memory=copy["releaseMemory"],
            decision_log=copy["releaseDecisions"],
        )
    if signals_ids:
        append_team(
            "team-signals",
            copy["signalsName"],
            signals_ids,
            copy["signalsFocus"],
            copy["signalsDescription"],
            default_dispatch_mode="direct",
            default_wake_scope="all",
            team_memory=copy["signalsMemory"],
            decision_log=copy["signalsDecisions"],
        )
    if not blueprints and agent_ids:
        append_team(
            "team-default",
            copy["fallbackName"],
            agent_ids,
            copy["fallbackFocus"],
            copy["fallbackDescription"],
            default_dispatch_mode="direct",
            default_wake_scope="all",
            team_memory=copy["fallbackMemory"],
            decision_log=copy["fallbackDecisions"],
        )
    return blueprints


def backfill_agent_team_policy_defaults(openclaw_dir, teams, agents, router_agent_id="", metadata=None):
    blueprints = {
        item.get("id"): item
        for item in _svc().build_default_agent_team_blueprints(agents, router_agent_id=router_agent_id, metadata=metadata)
        if item.get("id")
    }
    existing_by_id = {
        str(team.get("id") or "").strip(): team
        for team in safe_list(teams)
        if isinstance(team, dict) and str(team.get("id") or "").strip()
    }
    updated = []
    for team_id, blueprint in blueprints.items():
        existing = existing_by_id.get(team_id)
        if not existing:
            store_save_agent_team(openclaw_dir, blueprint)
            updated.append(blueprint)
            continue
        team_meta = team_runtime_meta(existing)
        if str(team_meta.get("source") or "").strip() != "product-default":
            continue
        current_member_ids = [str(item or "").strip() for item in safe_list(existing.get("memberAgentIds")) if str(item or "").strip()]
        next_member_ids = [str(item or "").strip() for item in safe_list(blueprint.get("memberAgentIds")) if str(item or "").strip()]
        next_payload = {
            "id": existing.get("id", ""),
            "name": blueprint.get("name", existing.get("name", "")),
            "status": blueprint.get("status", existing.get("status", "active")),
            "leadAgentId": blueprint.get("leadAgentId", existing.get("leadAgentId", "")),
            "memberAgentIds": next_member_ids or current_member_ids,
            "description": blueprint.get("description", existing.get("description", "")),
            "focus": blueprint.get("focus", existing.get("focus", "")),
            "channel": blueprint.get("channel", existing.get("channel", "internal")),
            "defaultDispatchMode": blueprint.get("defaultDispatchMode", team_meta.get("defaultDispatchMode") or existing.get("defaultDispatchMode", "direct")),
            "defaultWakeScope": blueprint.get("defaultWakeScope", team_meta.get("defaultWakeScope") or existing.get("defaultWakeScope", "all")),
            "operatingBrief": blueprint.get("operatingBrief", team_meta.get("operatingBrief") or existing.get("operatingBrief", "")),
            "teamMemory": blueprint.get("teamMemory", team_meta.get("teamMemory") or existing.get("teamMemory", "")),
            "decisionLog": blueprint.get("decisionLog", team_meta.get("decisionLog") or existing.get("decisionLog", "")),
            "linkedTaskIds": safe_list(existing.get("linkedTaskIds")),
            "meta": blueprint.get("meta", {}),
        }
        if (
            str(existing.get("name") or "").strip() == str(next_payload["name"] or "").strip()
            and str(existing.get("leadAgentId") or "").strip() == str(next_payload["leadAgentId"] or "").strip()
            and current_member_ids == next_member_ids
            and str(existing.get("description") or "").strip() == str(next_payload["description"] or "").strip()
            and str(existing.get("focus") or "").strip() == str(next_payload["focus"] or "").strip()
            and str(team_meta.get("defaultDispatchMode") or "").strip().lower() == str(next_payload["defaultDispatchMode"] or "").strip().lower()
            and str(team_meta.get("defaultWakeScope") or "").strip().lower() == str(next_payload["defaultWakeScope"] or "").strip().lower()
            and str(team_meta.get("runtimeMode") or "").strip().lower() == str((next_payload.get("meta") or {}).get("runtimeMode") or "").strip().lower()
            and str(team_meta.get("runtimeEvery") or "").strip().lower() == str((next_payload.get("meta") or {}).get("runtimeEvery") or "").strip().lower()
            and str(team_meta.get("operatingBrief") or "").strip() == str(next_payload["operatingBrief"] or "").strip()
            and str(team_meta.get("teamMemory") or "").strip() == str(next_payload["teamMemory"] or "").strip()
            and str(team_meta.get("decisionLog") or "").strip() == str(next_payload["decisionLog"] or "").strip()
        ):
            continue
        saved = save_agent_team_preserving_meta(openclaw_dir, next_payload, existing=existing)
        updated.append(saved)

    for team in safe_list(teams):
        team_id = str(team.get("id") or "").strip()
        if not team_id:
            continue
        if team_id in blueprints:
            continue
        team_meta = team_runtime_meta(team)
        blueprint = blueprints.get(team_id, {})
        next_dispatch_mode = normalize_chat_dispatch_mode(
            team_meta.get("defaultDispatchMode") or blueprint.get("defaultDispatchMode"),
            has_team=True,
        )
        next_wake_scope = normalize_team_wake_scope(
            team_meta.get("defaultWakeScope") or blueprint.get("defaultWakeScope")
        ) or ("all" if requested_team_runtime_mode(team) == "all_standby" else "all")
        next_operating_brief = str(
            team_meta.get("operatingBrief")
            or blueprint.get("operatingBrief")
            or blueprint.get("description")
            or team.get("description")
            or team.get("focus")
            or ""
        ).strip()
        next_team_memory = str(
            team_meta.get("teamMemory")
            or blueprint.get("teamMemory")
            or ""
        ).strip()
        next_decision_log = str(
            team_meta.get("decisionLog")
            or blueprint.get("decisionLog")
            or ""
        ).strip()
        if (
            str(team_meta.get("defaultDispatchMode") or "").strip().lower() == next_dispatch_mode
            and str(team_meta.get("defaultWakeScope") or "").strip().lower() == next_wake_scope
            and str(team_meta.get("operatingBrief") or "").strip() == next_operating_brief
            and str(team_meta.get("teamMemory") or "").strip() == next_team_memory
            and str(team_meta.get("decisionLog") or "").strip() == next_decision_log
        ):
            continue
        saved = save_agent_team_preserving_meta(
            openclaw_dir,
            {
                "id": team.get("id", ""),
                "name": team.get("name", ""),
                "status": team.get("status", "active"),
                "leadAgentId": team.get("leadAgentId", ""),
                "memberAgentIds": safe_list(team.get("memberAgentIds")),
                "description": team.get("description", ""),
                "focus": team.get("focus", ""),
                "channel": team.get("channel", "internal"),
                "defaultDispatchMode": next_dispatch_mode,
                "defaultWakeScope": next_wake_scope,
                "operatingBrief": next_operating_brief,
                "teamMemory": next_team_memory,
                "decisionLog": next_decision_log,
                "linkedTaskIds": safe_list(team.get("linkedTaskIds")),
            },
            existing=team,
        )
        updated.append(saved)
    return updated


def ensure_default_team_mode(openclaw_dir, metadata, agents, router_agent_id):
    current_metadata = deepcopy(metadata if isinstance(metadata, dict) else {})
    changed = False
    product_mode = str(current_metadata.get("productMode") or "").strip().lower()
    if not product_mode:
        current_metadata["productMode"] = str(PRODUCT_MODE_TEAM)
        product_mode = str(PRODUCT_MODE_TEAM)
        changed = True
    if "teamModeDefault" not in current_metadata:
        current_metadata["teamModeDefault"] = True
        changed = True

    try:
        bootstrap_version = int(current_metadata.get("teamBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0

    teams = store_list_agent_teams(openclaw_dir)
    if product_mode == str(PRODUCT_MODE_TEAM) and not teams:
        for payload in _svc().build_default_agent_team_blueprints(agents, router_agent_id=router_agent_id, metadata=current_metadata):
            store_save_agent_team(openclaw_dir, payload)
        teams = store_list_agent_teams(openclaw_dir)
        sync_requested_agent_team_runtime_policies(openclaw_dir, teams=teams)
        teams = store_list_agent_teams(openclaw_dir)
        current_metadata["teamBootstrapVersion"] = int(TEAM_BOOTSTRAP_VERSION)
        current_metadata["teamBootstrapAt"] = now_iso()
        if bootstrap_version >= int(TEAM_BOOTSTRAP_VERSION):
            current_metadata["teamBootstrapSource"] = "product-default-self-heal"
        else:
            current_metadata["teamBootstrapSource"] = "product-default"
        changed = True
    elif product_mode == str(PRODUCT_MODE_TEAM) and bootstrap_version < int(TEAM_BOOTSTRAP_VERSION):
        updated = _svc().backfill_agent_team_policy_defaults(
            openclaw_dir,
            teams,
            agents,
            router_agent_id=router_agent_id,
            metadata=current_metadata,
        )
        if updated:
            teams = store_list_agent_teams(openclaw_dir)
            sync_requested_agent_team_runtime_policies(openclaw_dir, teams=teams)
            teams = store_list_agent_teams(openclaw_dir)
        current_metadata["teamBootstrapVersion"] = int(TEAM_BOOTSTRAP_VERSION)
        current_metadata["teamBootstrapSource"] = str(current_metadata.get("teamBootstrapSource") or "product-default").strip()
        changed = True

    if changed:
        save_project_metadata(openclaw_dir, current_metadata)
    return current_metadata, teams


def ensure_default_management_bootstrap(openclaw_dir, metadata):
    current_metadata = deepcopy(metadata if isinstance(metadata, dict) else {})
    changed = False
    try:
        bootstrap_version = int(current_metadata.get("managementBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0
    next_mode = current_management_automation_mode(current_metadata)
    if str(current_metadata.get("managementAutomationMode") or "").strip().lower() != next_mode:
        current_metadata["managementAutomationMode"] = next_mode
        changed = True
    expected_rules = recommended_management_rules()
    existing_rules = store_list_automation_rules(openclaw_dir)
    existing_ids = {
        str(item.get("id") or "").strip()
        for item in safe_list(existing_rules)
        if str(item.get("id") or "").strip()
    }
    existing_names = {
        str(item.get("name") or "").strip()
        for item in safe_list(existing_rules)
        if str(item.get("name") or "").strip()
    }
    missing_defaults = [
        payload
        for payload in expected_rules
        if str(payload.get("id") or "").strip() not in existing_ids
        and str(payload.get("name") or "").strip() not in existing_names
    ]
    if bootstrap_version >= int(MANAGEMENT_BOOTSTRAP_VERSION) and not missing_defaults:
        if changed:
            save_project_metadata(openclaw_dir, current_metadata)
        return current_metadata
    result = bootstrap_management_rules(openclaw_dir)
    current_metadata["managementBootstrapVersion"] = int(MANAGEMENT_BOOTSTRAP_VERSION)
    current_metadata["managementBootstrapAt"] = now_iso()
    current_metadata["managementBootstrapSource"] = (
        "product-default-self-heal"
        if bootstrap_version >= int(MANAGEMENT_BOOTSTRAP_VERSION) or missing_defaults
        else "product-default"
    )
    current_metadata["managementBootstrapCreated"] = int(result.get("total") or 0)
    save_project_metadata(openclaw_dir, current_metadata)
    return current_metadata


def build_agent_team_data(openclaw_dir, agents, task_index, now):
    agent_map = {item.get("id"): item for item in safe_list(agents) if item.get("id")}
    task_map = {item.get("id"): item for item in safe_list(task_index) if item.get("id")}
    local_config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=local_config)
    agent_overrides = agent_runtime_overrides(metadata)
    config_agent_map = {
        str(item.get("id") or "").strip(): item
        for item in safe_list(((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list"))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    threads = store_list_chat_threads(openclaw_dir, limit=240)
    items = []
    covered_agents = set()
    runtime_enabled_teams = 0
    for team in store_list_agent_teams(openclaw_dir):
        member_ids = [str(item or "").strip() for item in safe_list(team.get("memberAgentIds")) if str(item or "").strip()]
        members = []
        online_count = 0
        active_tasks = 0
        blocked_tasks = 0
        completed_tasks = 0
        member_status_counts = {"active": 0, "standby": 0, "idle": 0, "blocked": 0}
        heartbeat_enabled_ids = []
        heartbeat_every_values = []
        for agent_id in member_ids:
            covered_agents.add(agent_id)
            agent = agent_map.get(agent_id, {})
            agent_name = str(
                agent.get("name")
                or ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name")
                or ""
            ).strip()
            config_agent = config_agent_map.get(agent_id, {})
            override = agent_overrides.get(agent_id) if isinstance(agent_overrides.get(agent_id), dict) else {}
            runtime_profile = merged_agent_runtime_profile(agent_id, override=override)
            heartbeat = config_agent.get("heartbeat") if isinstance(config_agent.get("heartbeat"), dict) else {}
            if heartbeat_config_enabled(heartbeat):
                heartbeat_enabled_ids.append(agent_id)
                every = str(heartbeat.get("every") or "").strip()
                if every and every not in heartbeat_every_values:
                    heartbeat_every_values.append(every)
            status = str(agent.get("status") or "").strip().lower()
            if is_online_reachable_status(status):
                online_count += 1
            grouped_status = team_member_status_group(status)
            if grouped_status in member_status_counts:
                member_status_counts[grouped_status] += 1
            active_tasks += int(agent.get("activeTasks") or 0)
            blocked_tasks += int(agent.get("blockedTasks") or 0)
            completed_tasks += int(agent.get("completedTasks") or agent.get("completedCount") or 0)
            members.append(
                {
                    "id": agent_id,
                    "label": agent.get("title") or agent_name or agent_id,
                    "humanName": runtime_profile.get("humanName", "") or agent_name,
                    "name": agent_name,
                    "status": agent.get("status", ""),
                    "role": runtime_profile.get("role", ""),
                    "roleLabel": runtime_profile.get("roleLabel", ""),
                    "jobTitle": runtime_profile.get("jobTitle", ""),
                    "workingStyle": runtime_profile.get("workingStyle", ""),
                    "skills": safe_list(runtime_profile.get("skills")),
                    "topSkills": safe_list(runtime_profile.get("topSkills")),
                    "skillCount": int(runtime_profile.get("skillCount") or 0),
                    "focus": agent.get("focus", ""),
                    "activeTasks": int(agent.get("activeTasks") or 0),
                    "blockedTasks": int(agent.get("blockedTasks") or 0),
                }
            )
        lead = next((item for item in members if item.get("id") == team.get("leadAgentId")), members[0] if members else {})
        team_threads = [
            thread
            for thread in threads
            if str((((thread.get("meta") or {}) if isinstance(thread.get("meta"), dict) else {}).get("teamId")) or "").strip() == team.get("id")
        ]
        linked_tasks = [task_map[item] for item in safe_list(team.get("linkedTaskIds")) if item in task_map]
        runtime_requested_mode = requested_team_runtime_mode(team)
        runtime_effective_mode = infer_effective_team_runtime_mode(team, heartbeat_enabled_ids)
        runtime_mode = runtime_requested_mode or runtime_effective_mode
        runtime_state = infer_team_runtime_state(team, members, active_tasks, blocked_tasks, heartbeat_enabled_ids)
        coordination_protocol = team_collaboration_protocol(team)
        wake_fields = _svc().team_runtime_wake_fields(team, now)
        if runtime_effective_mode in {"lead_standby", "all_standby"}:
            runtime_enabled_teams += 1
        items.append(
            compact_agent_team_payload(
                {
                    **team,
                    "leadAgentLabel": lead.get("label") or team.get("leadAgentId", ""),
                    "members": members,
                    "memberCount": len(members),
                    "onlineCount": online_count,
                    "activeTasks": active_tasks,
                    "blockedTasks": blocked_tasks,
                    "completedTasks": completed_tasks,
                    "threadCount": len(team_threads),
                    "attentionThreadCount": sum(
                        1
                        for thread in team_threads
                        if thread.get("status") in {"waiting_internal", "waiting_external", "blocked"}
                    ),
                    "runtimeMode": runtime_mode,
                    "runtimeRequestedMode": runtime_requested_mode,
                    "runtimeEffectiveMode": runtime_effective_mode,
                    "runtimeState": runtime_state,
                    "defaultDispatchMode": resolve_team_default_dispatch_mode(team),
                    "defaultWakeScope": requested_team_wake_scope(team),
                    "operatingBrief": team_operating_brief(team),
                    "teamMemory": team_memory_text(team),
                    "decisionLog": team_decision_log_text(team),
                    "coordinationProtocol": coordination_protocol,
                    "humanToneGuide": str(coordination_protocol.get("humanToneGuide") or "").strip(),
                    "proactiveRules": clean_unique_strings(coordination_protocol.get("proactiveRules") or []),
                    "updateContract": str(coordination_protocol.get("updateContract") or "").strip(),
                    "escalationRule": str(coordination_protocol.get("escalationRule") or "").strip(),
                    "runtimeEvery": heartbeat_every_values[0] if len(heartbeat_every_values) == 1 else (requested_team_runtime_every(team) if heartbeat_every_values else ""),
                    "heartbeatEnabledCount": len(heartbeat_enabled_ids),
                    "heartbeatEnabledAgentIds": heartbeat_enabled_ids,
                    "activeMemberCount": member_status_counts["active"],
                    "standbyMemberCount": member_status_counts["standby"],
                    "idleMemberCount": member_status_counts["idle"],
                    "blockedMemberCount": member_status_counts["blocked"],
                    **wake_fields,
                    "linkedTasks": linked_tasks,
                    "updatedAgo": format_age(parse_iso(team.get("updatedAt")), now),
                }
            )
        )
    return {
        "mode": str(PRODUCT_MODE_TEAM),
        "summary": {
            "teamCount": len(items),
            "activeCount": sum(1 for item in items if item.get("status") == "active"),
            "threadedCount": sum(1 for item in items if item.get("threadCount", 0) > 0),
            "coveredAgentCount": len(covered_agents),
            "runtimeEnabledCount": runtime_enabled_teams,
            "wakeReadyCount": sum(1 for item in items if item.get("memberCount", 0) > 0),
        },
        "items": items,
    }


def team_runtime_wake_fields(team, now=None):
    current_now = now or now_utc()
    runtime_meta = team_runtime_meta(team)
    wake_meta = runtime_meta.get("runtimeWake") if isinstance(runtime_meta.get("runtimeWake"), dict) else {}
    wake_at = str(wake_meta.get("at") or runtime_meta.get("runtimeWakeAt") or "").strip()
    wake_target_agent_ids = [
        str(item or "").strip()
        for item in safe_list(wake_meta.get("targetAgentIds"))
        if str(item or "").strip()
    ]
    wake_responded_agent_ids = [
        str(item or "").strip()
        for item in safe_list(wake_meta.get("respondedAgentIds"))
        if str(item or "").strip()
    ]
    wake_failed_agents = [
        item
        for item in safe_list(wake_meta.get("failedAgents"))
        if isinstance(item, dict) and str(item.get("agentId") or "").strip()
    ]
    return {
        "wakeScope": str(wake_meta.get("scope") or "").strip(),
        "wakeStatus": str(wake_meta.get("status") or "").strip(),
        "wakeAt": wake_at,
        "wakeAgo": format_age(parse_iso(wake_at), current_now) if wake_at else "",
        "wakeTargetCount": len(wake_target_agent_ids),
        "wakeReplyCount": len(wake_responded_agent_ids),
        "wakeFailureCount": len(wake_failed_agents),
        "wakeTargetAgentIds": wake_target_agent_ids,
        "wakeRespondedAgentIds": wake_responded_agent_ids,
    }
