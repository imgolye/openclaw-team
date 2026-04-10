#!/usr/bin/env python3
"""Read/query route dispatcher extracted from collaboration_dashboard."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from .computer_use import handle_computer_use_read_route
from .workflow_api import handle_workflow_route
from .knowledge_base_api import handle_kb_route
from .automation_api import handle_automation_route
from .tool_bridge_api import handle_tool_bridge_route


DEFAULT_ACCOUNT_WORKSPACE_SEGMENT = "guest"


def _sanitize_account_workspace_segment(value, fallback=DEFAULT_ACCOUNT_WORKSPACE_SEGMENT):
    normalized = re.sub(r"[^a-z0-9._-]+", "-", str(value or "").strip().lower())
    normalized = re.sub(r"-+", "-", normalized).strip("-")
    fallback_value = str(fallback or DEFAULT_ACCOUNT_WORKSPACE_SEGMENT).strip() or DEFAULT_ACCOUNT_WORKSPACE_SEGMENT
    return normalized or fallback_value


def _normalize_account_workspace_root(value):
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    candidate = Path(normalized).expanduser()
    try:
        resolved = candidate.resolve()
    except OSError:
        resolved = candidate
    return str(resolved)


def _read_account_workspace_projection(handler):
    root_path = _normalize_account_workspace_root(
        handler.headers.get("x-mission-control-account-workspace-root", "")
    )
    account_key = _sanitize_account_workspace_segment(
        handler.headers.get("x-mission-control-account-workspace-key", ""),
        DEFAULT_ACCOUNT_WORKSPACE_SEGMENT,
    )
    return {
        "enabled": bool(root_path),
        "accountKey": account_key,
        "rootPath": root_path,
        "agentsDir": str(Path(root_path) / "agents") if root_path else "",
        "conversationsDir": str(Path(root_path) / "conversations") if root_path else "",
        "workflowDir": str(Path(root_path) / "workflow-studio") if root_path else "",
    }


def _account_workspace_cache_fragment(projection):
    projection = projection if isinstance(projection, dict) else {}
    return str(projection.get("rootPath") or "").strip() or "-"


def _resolve_account_agent_workspace(projection, agent_id):
    projection = projection if isinstance(projection, dict) else {}
    root_path = str(projection.get("rootPath") or "").strip()
    normalized_agent_id = _sanitize_account_workspace_segment(agent_id, "assistant")
    if not root_path:
        return ""
    return str(Path(root_path) / "agents" / normalized_agent_id)


def _project_agent_workspace_record(record, projection):
    if not isinstance(record, dict):
        return deepcopy(record)
    projected = deepcopy(record)
    if not projection.get("enabled"):
        return projected
    agent_id = str(projected.get("id") or "").strip()
    display_workspace = _resolve_account_agent_workspace(projection, agent_id)
    if not display_workspace:
        return projected
    runtime_workspace = str(projected.get("workspace") or "").strip()
    if runtime_workspace and runtime_workspace != display_workspace:
        projected["runtimeWorkspace"] = runtime_workspace
    projected["workspace"] = display_workspace
    return projected


def _project_agent_workspace_records(items, projection):
    return [
        _project_agent_workspace_record(item, projection)
        for item in (items if isinstance(items, list) else [])
    ]


def _project_openclaw_model_access_matrix(items, projection):
    projected_rows = []
    for row in items if isinstance(items, list) else []:
        if not isinstance(row, dict):
            projected_rows.append(deepcopy(row))
            continue
        projected_row = deepcopy(row)
        projected_models = []
        for model in row.get("models") if isinstance(row.get("models"), list) else []:
            if not isinstance(model, dict):
                projected_models.append(deepcopy(model))
                continue
            projected_model = deepcopy(model)
            projected_model["agents"] = _project_agent_workspace_records(model.get("agents"), projection)
            projected_models.append(projected_model)
        projected_row["models"] = projected_models
        projected_rows.append(projected_row)
    return projected_rows


def _project_agents_response(payload, projection, services):
    payload = payload if isinstance(payload, dict) else {}
    projected = {
        "agents": _project_agent_workspace_records(payload.get("agents"), projection),
    }
    if projection.get("enabled"):
        projected["accountWorkspace"] = deepcopy(projection)
    signature = str(services["dashboard_signature"](projected) or "").strip()
    if signature:
        projected["signature"] = signature
    return projected


def _project_desktop_bootstrap_payload(payload, projection, services):
    payload = deepcopy(payload if isinstance(payload, dict) else {})
    if projection.get("enabled"):
        workspace = payload.get("workspace") if isinstance(payload.get("workspace"), dict) else {}
        payload["workspace"] = {
            **workspace,
            "account": deepcopy(projection),
        }
    return _attach_root_signature(payload, services)


def _project_openclaw_payload(payload, projection, services):
    payload = deepcopy(payload if isinstance(payload, dict) else {})
    if not projection.get("enabled"):
        return _attach_root_signature(payload, services)
    runtime_overview = payload.get("runtimeOverview") if isinstance(payload.get("runtimeOverview"), dict) else {}
    if runtime_overview:
        payload["runtimeOverview"] = {
            **runtime_overview,
            "accountWorkspace": deepcopy(projection),
        }
    execution_architecture = payload.get("executionArchitecture") if isinstance(payload.get("executionArchitecture"), dict) else {}
    if execution_architecture:
        provider_path = (
            execution_architecture.get("providerPath")
            if isinstance(execution_architecture.get("providerPath"), dict)
            else {}
        )
        if provider_path:
            execution_architecture["providerPath"] = {
                **provider_path,
                "accountWorkspace": deepcopy(projection),
                "providerAssignments": _project_agent_workspace_records(provider_path.get("providerAssignments"), projection),
                "modelAccessMatrix": _project_openclaw_model_access_matrix(provider_path.get("modelAccessMatrix"), projection),
            }
            payload["executionArchitecture"] = execution_architecture
    models_payload = payload.get("models") if isinstance(payload.get("models"), dict) else {}
    if models_payload:
        payload["models"] = {
            **models_payload,
            "accountWorkspace": deepcopy(projection),
            "assignments": _project_agent_workspace_records(models_payload.get("assignments"), projection),
        }
    payload["accountWorkspace"] = deepcopy(projection)
    return _attach_root_signature(payload, services)


def _parse_bounded_int(value, default, minimum=1, maximum=100):
    try:
        normalized = int(value or default)
    except (TypeError, ValueError):
        normalized = int(default)
    return max(minimum, min(normalized, maximum))


def _compact_chat_catalog_collaboration(collaboration):
    collaboration = collaboration if isinstance(collaboration, dict) else {}
    compact = {
        "status": str(collaboration.get("status") or "").strip(),
        "headline": str(collaboration.get("headline") or "").strip(),
        "memberCount": int(collaboration.get("memberCount") or 0),
        "responseCount": int(collaboration.get("responseCount") or 0),
        "waitingCount": int(collaboration.get("waitingCount") or 0),
        "committedCount": int(collaboration.get("committedCount") or 0),
        "standbyCount": int(collaboration.get("standbyCount") or 0),
        "blockerCount": int(collaboration.get("blockerCount") or 0),
        "failureCount": int(collaboration.get("failureCount") or 0),
        "relaySent": bool(collaboration.get("relaySent")),
        "relayReplyCount": int(collaboration.get("relayReplyCount") or 0),
    }
    return {key: value for key, value in compact.items() if value not in ("", None)}


def _compact_chat_catalog_reference(item, fields):
    item = item if isinstance(item, dict) else {}
    compact = {}
    for field in fields:
        value = item.get(field)
        if isinstance(value, str):
            value = value.strip()
        if value in ("", None):
            continue
        compact[field] = value
    return compact


def _compact_chat_catalog_team_references(items):
    compact = []
    for item in items if isinstance(items, list) else []:
        entry = _compact_chat_catalog_reference(item, ("id", "name"))
        if entry:
            compact.append(entry)
    return compact


def _compact_orchestration_workflows(workflows):
    compact = []
    for workflow in workflows if isinstance(workflows, list) else []:
        if not isinstance(workflow, dict):
            continue
        latest_version = workflow.get("latestVersion") if isinstance(workflow.get("latestVersion"), dict) else {}
        compact.append(
            {
                "id": str(workflow.get("id") or "").strip(),
                "name": str(workflow.get("name") or workflow.get("id") or "").strip(),
                "status": str(workflow.get("status") or "").strip(),
                "mode": str(workflow.get("mode") or "").strip(),
                "latestVersion": {
                    "versionNumber": latest_version.get("versionNumber"),
                    "createdAt": str(latest_version.get("createdAt") or "").strip(),
                    "updatedAt": str(latest_version.get("updatedAt") or "").strip(),
                } if latest_version else {},
            }
        )
    return compact


def _select_context_summary_panel(context_hub):
    context_hub = context_hub if isinstance(context_hub, dict) else {}
    agent_memory = context_hub.get("agentMemory") if isinstance(context_hub.get("agentMemory"), dict) else {}
    shared_context = context_hub.get("sharedContext") if isinstance(context_hub.get("sharedContext"), dict) else {}
    return {
        "supported": bool(context_hub.get("supported", True)),
        "installed": bool(context_hub.get("installed")),
        "binary": str(context_hub.get("binary") or "").strip(),
        "version": str(context_hub.get("version") or "").strip(),
        "status": str(context_hub.get("status") or "").strip(),
        "summary": deepcopy(context_hub.get("summary") if isinstance(context_hub.get("summary"), dict) else {}),
        "annotations": deepcopy(context_hub.get("annotations") if isinstance(context_hub.get("annotations"), dict) else {}),
        "recommended": deepcopy(context_hub.get("recommended") if isinstance(context_hub.get("recommended"), list) else []),
        "cache": deepcopy(context_hub.get("cache") if isinstance(context_hub.get("cache"), dict) else {}),
        "config": deepcopy(context_hub.get("config") if isinstance(context_hub.get("config"), dict) else {}),
        "commands": deepcopy(context_hub.get("commands") if isinstance(context_hub.get("commands"), list) else []),
        "agentMemory": {
            "enabled": bool(agent_memory.get("enabled")),
            "provider": str(agent_memory.get("provider") or "").strip(),
            "summary": deepcopy(agent_memory.get("summary") if isinstance(agent_memory.get("summary"), dict) else {}),
            "agents": [],
        },
        "sharedContext": {
            "summary": deepcopy(shared_context.get("summary") if isinstance(shared_context.get("summary"), dict) else {}),
            "routerAgentId": str(shared_context.get("routerAgentId") or "").strip(),
            "routerAgentTitle": str(shared_context.get("routerAgentTitle") or "").strip(),
            "documents": [],
        },
    }


def _select_orchestration_summary_panel(orchestration):
    orchestration = orchestration if isinstance(orchestration, dict) else {}
    catalog_summary = orchestration.get("catalogSummary") if isinstance(orchestration.get("catalogSummary"), dict) else {}
    workflows = orchestration.get("workflows") if isinstance(orchestration.get("workflows"), list) else []
    policies = orchestration.get("routingPolicies") if isinstance(orchestration.get("routingPolicies"), list) else []
    replays = orchestration.get("replays") if isinstance(orchestration.get("replays"), list) else []
    routing_decisions = orchestration.get("routingDecisions") if isinstance(orchestration.get("routingDecisions"), list) else []
    workflow_versions = orchestration.get("workflowVersions") if isinstance(orchestration.get("workflowVersions"), list) else []
    return {
        "summary": deepcopy(orchestration.get("summary") if isinstance(orchestration.get("summary"), dict) else {}),
        "decisionQuality": deepcopy(orchestration.get("decisionQuality") if isinstance(orchestration.get("decisionQuality"), dict) else {}),
        "policyTrends": deepcopy(orchestration.get("policyTrends") if isinstance(orchestration.get("policyTrends"), list) else []),
        "workflowReview": deepcopy(orchestration.get("workflowReview") if isinstance(orchestration.get("workflowReview"), list) else []),
        "adjustmentReview": deepcopy(orchestration.get("adjustmentReview") if isinstance(orchestration.get("adjustmentReview"), dict) else {}),
        "reviewSuggestions": deepcopy(orchestration.get("reviewSuggestions") if isinstance(orchestration.get("reviewSuggestions"), list) else []),
        "nextStepSuggestions": deepcopy(orchestration.get("nextStepSuggestions") if isinstance(orchestration.get("nextStepSuggestions"), list) else []),
        "linkedSuggestions": deepcopy(orchestration.get("linkedSuggestions") if isinstance(orchestration.get("linkedSuggestions"), list) else []),
        "linkedReview": deepcopy(orchestration.get("linkedReview") if isinstance(orchestration.get("linkedReview"), dict) else {}),
        "catalogSummary": {
            "workflowCount": int(catalog_summary.get("workflowCount") or len(workflows)),
            "workflowVersionCount": int(catalog_summary.get("workflowVersionCount") or len(workflow_versions)),
            "policyCount": int(catalog_summary.get("policyCount") or len(policies)),
            "replayCount": int(catalog_summary.get("replayCount") or len(replays)),
            "routingDecisionCount": int(catalog_summary.get("routingDecisionCount") or len(routing_decisions)),
        },
    }


def _compact_chat_catalog_thread(thread):
    thread = thread if isinstance(thread, dict) else {}
    thread_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    workspace_path = str(thread.get("workspacePath") or thread_meta.get("workspacePath") or "").strip()
    workspace_authorized = bool(thread.get("workspaceAuthorized") or thread_meta.get("workspaceAuthorized")) and bool(workspace_path)
    preserve_empty_fields = {"lastMessageId", "lastMessageAt", "lastMessagePreview"}
    compact = {
        "id": str(thread.get("id") or "").strip(),
        "title": str(thread.get("title") or "").strip(),
        "owner": str(thread.get("owner") or "").strip(),
        "status": str(thread.get("status") or "").strip(),
        "createdAt": str(thread.get("createdAt") or "").strip(),
        "updatedAt": str(thread.get("updatedAt") or "").strip(),
        "lastMessageId": str(thread.get("lastMessageId") or "").strip(),
        "lastMessageAt": str(thread.get("lastMessageAt") or "").strip(),
        "lastMessagePreview": str(thread.get("lastMessagePreview") or "").strip(),
        "messageCount": int(thread.get("messageCount") or 0),
        "primaryAgentId": str(thread.get("primaryAgentId") or "").strip(),
        "primaryAgentLabel": str(thread.get("primaryAgentLabel") or "").strip(),
        "currentTargetAgentId": str(thread.get("currentTargetAgentId") or "").strip(),
        "teamId": str(thread.get("teamId") or "").strip(),
        "teamLabel": str(thread.get("teamLabel") or "").strip(),
        "dispatchMode": str(thread.get("dispatchMode") or "").strip(),
        "mode": str(thread.get("mode") or "").strip(),
        "managedTaskThread": bool(thread.get("managedTaskThread")),
        "participantAgentCount": int(thread.get("participantAgentCount") or 0),
        "participantHumanCount": int(thread.get("participantHumanCount") or 0),
        "linkedTaskId": str(thread.get("linkedTaskId") or "").strip(),
        "linkedRunId": str(thread.get("linkedRunId") or "").strip(),
        "linkedPackId": str(thread.get("linkedPackId") or "").strip(),
        "workspacePath": workspace_path,
        "workspaceAuthorized": workspace_authorized,
    }
    linked_task = _compact_chat_catalog_reference(thread.get("linkedTask"), ("id", "title", "currentUpdate"))
    linked_run = _compact_chat_catalog_reference(thread.get("linkedRun"), ("id", "title"))
    linked_pack = _compact_chat_catalog_reference(thread.get("linkedPack"), ("id", "name", "mode"))
    linked_team = _compact_chat_catalog_reference(thread.get("linkedTeam"), ("id", "name"))
    linked_teams = _compact_chat_catalog_team_references(thread.get("linkedTeams"))
    collaboration = _compact_chat_catalog_collaboration(thread.get("collaboration"))
    if linked_task:
        compact["linkedTask"] = linked_task
    if linked_run:
        compact["linkedRun"] = linked_run
    if linked_pack:
        compact["linkedPack"] = linked_pack
    if linked_team:
        compact["linkedTeam"] = linked_team
    if collaboration:
        compact["collaboration"] = collaboration
    return {
        key: value
        for key, value in compact.items()
        if key in preserve_empty_fields or value not in ("", None, [])
    }


def _truncate_compact_text(value, limit=160):
    normalized = " ".join(str(value or "").split()).strip()
    if not normalized:
        return ""
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit].rstrip()}…"


def _compact_conversation_session(session):
    session = session if isinstance(session, dict) else {}
    compact = {
        "key": str(session.get("key") or "").strip(),
        "sessionId": str(session.get("sessionId") or "").strip(),
        "agentId": str(session.get("agentId") or "").strip(),
        "agentLabel": str(session.get("agentLabel") or "").strip(),
        "agentHumanName": str(session.get("agentHumanName") or "").strip(),
        "agentJobTitle": str(session.get("agentJobTitle") or "").strip(),
        "label": str(session.get("label") or "").strip(),
        "sourceLabel": str(session.get("sourceLabel") or "").strip(),
        "talkable": bool(session.get("talkable")),
        "updatedAt": str(session.get("updatedAt") or "").strip(),
        "updatedAgo": str(session.get("updatedAgo") or "").strip(),
        "lastMessageId": str(session.get("lastMessageId") or "").strip(),
        "preview": _truncate_compact_text(session.get("preview"), limit=160),
        "model": str(session.get("model") or "").strip(),
        "provider": str(session.get("provider") or "").strip(),
    }
    return {key: value for key, value in compact.items() if value not in ("", None)}


def _filter_conversation_sessions(sessions, query_text=""):
    normalized_query = str(query_text or "").strip().lower()
    if not normalized_query:
        return [item for item in sessions if isinstance(item, dict)]
    filtered = []
    for session in sessions if isinstance(sessions, list) else []:
        if not isinstance(session, dict):
            continue
        haystack = " ".join(
            [
                str(session.get("key") or "").strip(),
                str(session.get("label") or "").strip(),
                str(session.get("agentId") or "").strip(),
                str(session.get("agentLabel") or "").strip(),
                str(session.get("agentHumanName") or "").strip(),
                str(session.get("agentJobTitle") or "").strip(),
                str(session.get("sourceLabel") or "").strip(),
                str(session.get("preview") or "").strip(),
                str(session.get("model") or "").strip(),
                str(session.get("provider") or "").strip(),
            ]
        ).lower()
        if normalized_query in haystack:
            filtered.append(session)
    return filtered


def _build_conversations_payload(conversations_data, page=1, page_size=36, query_text="", include_commands=False):
    conversations_data = conversations_data if isinstance(conversations_data, dict) else {}
    summary = conversations_data.get("summary") if isinstance(conversations_data.get("summary"), dict) else {}
    sessions = _filter_conversation_sessions(conversations_data.get("sessions"), query_text=query_text)
    total_count = len(sessions)
    page_size = _parse_bounded_int(page_size, 36, minimum=1, maximum=100)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    page = _parse_bounded_int(page, 1, minimum=1, maximum=total_pages)
    start = (page - 1) * page_size
    page_sessions = sessions[start : start + page_size]
    payload = {
        "conversations": {
            "summary": {
                "total": int(summary.get("total") or total_count),
                "talkable": int(summary.get("talkable") or 0),
                "active24h": int(summary.get("active24h") or 0),
                "pageSessionCount": len(page_sessions),
            },
            "sessions": [_compact_conversation_session(item) for item in page_sessions],
            "page": page,
            "pageSize": page_size,
            "totalCount": total_count,
            "totalPages": total_pages,
            "hasMore": page < total_pages,
            "query": str(query_text or "").strip(),
        }
    }
    supported = conversations_data.get("supported")
    error = str(conversations_data.get("error") or "").strip()
    if supported is False:
        payload["conversations"]["supported"] = False
    if error:
        payload["conversations"]["error"] = error
    if include_commands:
        payload["conversations"]["commands"] = conversations_data.get("commands") if isinstance(conversations_data.get("commands"), list) else []
    return payload


def _filter_chat_catalog_threads(threads, status="", query_text=""):
    normalized_status = str(status or "").strip().lower()
    normalized_query = str(query_text or "").strip().lower()
    filtered = []
    for thread in threads if isinstance(threads, list) else []:
        if not isinstance(thread, dict):
            continue
        thread_status = str(thread.get("status") or "").strip().lower()
        if normalized_status and thread_status != normalized_status:
            continue
        if normalized_query:
            linked_task = thread.get("linkedTask") if isinstance(thread.get("linkedTask"), dict) else {}
            linked_run = thread.get("linkedRun") if isinstance(thread.get("linkedRun"), dict) else {}
            linked_pack = thread.get("linkedPack") if isinstance(thread.get("linkedPack"), dict) else {}
            linked_team = thread.get("linkedTeam") if isinstance(thread.get("linkedTeam"), dict) else {}
            haystack = " ".join(
                [
                    str(thread.get("title") or "").strip(),
                    str(thread.get("owner") or "").strip(),
                    str(thread.get("teamLabel") or "").strip(),
                    str(thread.get("lastMessagePreview") or "").strip(),
                    str(linked_task.get("title") or "").strip(),
                    str(linked_run.get("title") or "").strip(),
                    str(linked_pack.get("name") or "").strip(),
                    str(linked_team.get("name") or "").strip(),
                ]
            ).lower()
            if normalized_query not in haystack:
                continue
        filtered.append(thread)
    return filtered


def _build_chat_catalog_payload(chat_data, page=1, page_size=24, status="", query_text=""):
    chat_data = chat_data if isinstance(chat_data, dict) else {}
    threads = _filter_chat_catalog_threads(chat_data.get("threads"), status=status, query_text=query_text)
    total_count = len(threads)
    page_size = _parse_bounded_int(page_size, 24, minimum=1, maximum=60)
    total_pages = max(1, (total_count + page_size - 1) // page_size)
    page = _parse_bounded_int(page, 1, minimum=1, maximum=total_pages)
    start = (page - 1) * page_size
    page_threads = threads[start : start + page_size]
    summary = chat_data.get("summary") if isinstance(chat_data.get("summary"), dict) else {}
    return {
        "chat": {
            "summary": {
                "threadCount": total_count,
                "pageThreadCount": len(page_threads),
                "waitingInternalCount": int(summary.get("waitingInternalCount") or 0),
                "waitingExternalCount": int(summary.get("waitingExternalCount") or 0),
            },
            "threads": [_compact_chat_catalog_thread(item) for item in page_threads],
            "page": page,
            "pageSize": page_size,
            "totalCount": total_count,
            "totalPages": total_pages,
            "hasMore": page < total_pages,
            "status": str(status or "").strip(),
            "query": str(query_text or "").strip(),
        }
    }


def _attach_payload_signature(payload, services, section_key):
    payload = payload if isinstance(payload, dict) else {}
    section = payload.get(section_key) if isinstance(payload.get(section_key), dict) else {}
    if not section:
        return payload
    signature = str(services["dashboard_signature"]({section_key: section}) or "").strip()
    if not signature:
        return payload
    return {
        **payload,
        section_key: {
            **section,
            "signature": signature,
        },
    }


def _attach_root_signature(payload, services):
    payload = payload if isinstance(payload, dict) else {}
    signature = str(services["dashboard_signature"](payload) or "").strip()
    if not signature:
        return payload
    return {
        **payload,
        "signature": signature,
    }


def _load_platform_bootstrap_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    now = services["now_utc"]()
    return services["cached_payload"](
        ("platform-bootstrap", str(openclaw_dir)),
        10.0,
        lambda: services["build_admin_bootstrap_snapshot"](
            openclaw_dir,
            config=config,
            now=now,
        ),
    )


def _load_platform_audit_logs_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    now = services["now_utc"]()
    return services["cached_payload"](
        ("platform-audit-logs", str(openclaw_dir)),
        10.0,
        lambda: services["build_admin_audit_logs_snapshot"](
            openclaw_dir,
            config=config,
            now=now,
        ),
    )


def _compact_platform_webhooks(channels):
    compact = []
    for channel in channels if isinstance(channels, list) else []:
        if not isinstance(channel, dict):
            continue
        meta = channel.get("meta") if isinstance(channel.get("meta"), dict) else {}
        health = meta.get("health") if isinstance(meta.get("health"), dict) else {}
        events = meta.get("events") if isinstance(meta.get("events"), list) else meta.get("eventTypes") if isinstance(meta.get("eventTypes"), list) else []
        compact.append(
            {
                "id": str(channel.get("id") or "").strip(),
                "name": str(channel.get("name") or channel.get("id") or "").strip(),
                "url": str(channel.get("target") or meta.get("url") or "").strip(),
                "events": [str(item).strip() for item in events if str(item).strip()],
                "status": str(channel.get("status") or health.get("status") or "active").strip() or "active",
                "signingEnabled": bool(meta.get("signingEnabled", bool(str(channel.get("secret") or "").strip()))),
                "lastDeliveryAgo": str(meta.get("lastDeliveryAgo") or health.get("lastDeliveryAgo") or "").strip(),
            }
        )
    return compact


def _build_platform_summary_payload(handler, services):
    bootstrap = _load_platform_bootstrap_snapshot(handler, services)
    audit = _load_platform_audit_logs_snapshot(handler, services)
    tenant_summary = bootstrap.get("tenantSummary") if isinstance(bootstrap.get("tenantSummary"), dict) else {}
    audit_summary = audit.get("auditSummary") if isinstance(audit.get("auditSummary"), dict) else {}
    platform = {
        "tenantSummary": tenant_summary,
        "counts": {
            "tenants": int(tenant_summary.get("total") or 0),
            "apiKeys": int(tenant_summary.get("apiKeys") or 0),
            "webhooks": len(services["store_list_notification_channels"](handler.server.openclaw_dir) or []),
            "auditLogs": int(audit_summary.get("total") or 0),
        },
    }
    return {"platform": platform}


def _load_platform_runtime_governance_summary_snapshot(handler, services):
    return services["cached_payload"](
        ("platform-runtime-governance-summary", str(handler.server.openclaw_dir)),
        10.0,
        lambda: services["build_platform_runtime_governance_summary"](
            services["load_skills_detail"](handler.server.openclaw_dir),
            services["store_list_management_runs"](handler.server.openclaw_dir, limit=96),
        ),
    )


def _load_platform_runtime_governance_runtimes_snapshot(handler, services):
    return services["cached_payload"](
        ("platform-runtime-governance-runtimes", str(handler.server.openclaw_dir)),
        10.0,
        lambda: services["build_platform_runtime_governance_runtimes"](
            services["load_skills_detail"](handler.server.openclaw_dir),
        ),
    )


def _load_platform_runtime_governance_packs_snapshot(handler, services):
    return services["cached_payload"](
        ("platform-runtime-governance-packs", str(handler.server.openclaw_dir)),
        10.0,
        lambda: services["build_platform_runtime_governance_packs"](
            services["load_skills_detail"](handler.server.openclaw_dir),
        ),
    )


def _load_platform_runtime_governance_browser_sessions_snapshot(handler, services):
    return services["cached_payload"](
        ("platform-runtime-governance-browser-sessions", str(handler.server.openclaw_dir)),
        10.0,
        lambda: services["build_platform_runtime_governance_browser_sessions"](
            {},
            services["store_list_management_runs"](handler.server.openclaw_dir, limit=96),
        ),
    )


def _build_platform_slice_payload(handler, services, slice_key):
    if slice_key == "tenants":
        openclaw_dir = handler.server.openclaw_dir
        config = services["load_config"](openclaw_dir)
        now = services["now_utc"]()
        tenants = services["build_admin_tenants_snapshot"](openclaw_dir, config=config, now=now)
        return {"platform": {"tenants": tenants.get("tenants", []), "tenantSummary": tenants.get("tenantSummary", {})}}
    if slice_key == "apiKeys":
        openclaw_dir = handler.server.openclaw_dir
        config = services["load_config"](openclaw_dir)
        now = services["now_utc"]()
        api_keys = services["build_admin_api_keys_snapshot"](openclaw_dir, config=config, now=now)
        tenant_summary = _load_platform_bootstrap_snapshot(handler, services).get("tenantSummary", {})
        return {"platform": {"apiKeys": api_keys.get("tenantApiKeys", []), "tenantSummary": tenant_summary}}
    if slice_key == "webhooks":
        channels = services["store_list_notification_channels"](handler.server.openclaw_dir)
        return {"platform": {"webhooks": _compact_platform_webhooks(channels)}}
    if slice_key == "auditLogs":
        audit = _load_platform_audit_logs_snapshot(handler, services)
        return {"platform": {"auditLogs": audit.get("auditLogs", [])}}
    if slice_key == "apiReference":
        return {"platform": {"apiReference": services["build_external_api_reference"]()}}
    if slice_key == "runtimeGovernanceSummary":
        runtime_governance = _load_platform_runtime_governance_summary_snapshot(handler, services)
        return {"platform": {"runtimeGovernance": {"summary": deepcopy(runtime_governance.get("summary") if isinstance(runtime_governance.get("summary"), dict) else {})}}}
    if slice_key == "runtimeGovernanceRuntimes":
        runtime_governance = _load_platform_runtime_governance_runtimes_snapshot(handler, services)
        return {"platform": {"runtimeGovernance": {"runtimes": deepcopy(runtime_governance.get("runtimes") if isinstance(runtime_governance.get("runtimes"), list) else [])}}}
    if slice_key == "runtimeGovernancePacks":
        runtime_governance = _load_platform_runtime_governance_packs_snapshot(handler, services)
        return {"platform": {"runtimeGovernance": {"packs": deepcopy(runtime_governance.get("packs") if isinstance(runtime_governance.get("packs"), list) else [])}}}
    if slice_key == "runtimeGovernanceBrowserSessions":
        runtime_governance = _load_platform_runtime_governance_browser_sessions_snapshot(handler, services)
        return {
            "platform": {
                "runtimeGovernance": {
                    "browserSessions": deepcopy(
                        runtime_governance.get("browserSessions") if isinstance(runtime_governance.get("browserSessions"), list) else []
                    )
                }
            }
        }
    return {"platform": {}}


def _build_orchestration_workflow_payload(data):
    data = data if isinstance(data, dict) else {}
    orchestration = data.get("orchestration") if isinstance(data.get("orchestration"), dict) else {}
    return {
        "orchestration": {
            "catalogSummary": {
                "workflowCount": len(orchestration.get("workflows", []) if isinstance(orchestration.get("workflows"), list) else []),
                "workflowVersionCount": len(
                    orchestration.get("workflowVersions", []) if isinstance(orchestration.get("workflowVersions"), list) else []
                ),
            },
            "workflows": deepcopy(orchestration.get("workflows") if isinstance(orchestration.get("workflows"), list) else []),
            "workflowVersions": deepcopy(
                orchestration.get("workflowVersions") if isinstance(orchestration.get("workflowVersions"), list) else []
            ),
        }
    }


def _build_context_summary_payload(data):
    data = data if isinstance(data, dict) else {}
    return {"contextHub": _select_context_summary_panel(data.get("contextHub", {}))}


def _build_context_slice_payload(data, slice_key):
    data = data if isinstance(data, dict) else {}
    context_hub = data.get("contextHub") if isinstance(data.get("contextHub"), dict) else {}
    if slice_key == "agentMemory":
        return {
            "contextHub": {
                "agentMemory": deepcopy(context_hub.get("agentMemory") if isinstance(context_hub.get("agentMemory"), dict) else {}),
            }
        }
    if slice_key == "sharedContext":
        return {
            "contextHub": {
                "sharedContext": deepcopy(context_hub.get("sharedContext") if isinstance(context_hub.get("sharedContext"), dict) else {}),
            }
        }
    return {"contextHub": {}}


def _build_orchestration_summary_payload(data):
    data = data if isinstance(data, dict) else {}
    orchestration = data.get("orchestration") if isinstance(data.get("orchestration"), dict) else {}
    return {"orchestration": _select_orchestration_summary_panel(orchestration)}


def _build_orchestration_slice_payload(data, slice_key):
    data = data if isinstance(data, dict) else {}
    orchestration = data.get("orchestration") if isinstance(data.get("orchestration"), dict) else {}
    if slice_key == "routing":
        return {
            "orchestration": {
                "catalogSummary": {
                    "policyCount": len(
                        orchestration.get("routingPolicies", []) if isinstance(orchestration.get("routingPolicies"), list) else []
                    ),
                    "routingDecisionCount": len(
                        orchestration.get("routingDecisions", []) if isinstance(orchestration.get("routingDecisions"), list) else []
                    ),
                },
                "routingPolicies": deepcopy(
                    orchestration.get("routingPolicies") if isinstance(orchestration.get("routingPolicies"), list) else []
                ),
                "routingDecisions": deepcopy(
                    orchestration.get("routingDecisions") if isinstance(orchestration.get("routingDecisions"), list) else []
                ),
                "routingHitLeaders": deepcopy(
                    orchestration.get("routingHitLeaders") if isinstance(orchestration.get("routingHitLeaders"), list) else []
                ),
                "routingTrend": deepcopy(
                    orchestration.get("routingTrend") if isinstance(orchestration.get("routingTrend"), list) else []
                ),
                "policyTrends": deepcopy(
                    orchestration.get("policyTrends") if isinstance(orchestration.get("policyTrends"), list) else []
                ),
                "intelligence": deepcopy(
                    orchestration.get("intelligence") if isinstance(orchestration.get("intelligence"), dict) else {}
                ),
                "decisionQuality": deepcopy(
                    orchestration.get("decisionQuality") if isinstance(orchestration.get("decisionQuality"), dict) else {}
                ),
            }
        }
    if slice_key == "replays":
        return {
            "orchestration": {
                "catalogSummary": {
                    "replayCount": len(orchestration.get("replays", []) if isinstance(orchestration.get("replays"), list) else []),
                },
                "replays": deepcopy(orchestration.get("replays") if isinstance(orchestration.get("replays"), list) else []),
                "contextHotspots": deepcopy(
                    orchestration.get("contextHotspots") if isinstance(orchestration.get("contextHotspots"), list) else []
                ),
            }
        }
    if slice_key == "planning":
        return {
            "orchestration": {
                "planning": deepcopy(orchestration.get("planning") if isinstance(orchestration.get("planning"), dict) else {}),
            }
        }
    return {"orchestration": {}}


def _load_theme_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    return services["cached_payload"](
        ("theme-snapshot", str(openclaw_dir)),
        10.0,
        lambda: services["build_theme_snapshot"](openclaw_dir, config=config),
    )


def _load_context_hub_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    router_agent_id = services["get_router_agent_id"](config)
    return services["load_context_hub_summary_data"](
        openclaw_dir,
        router_agent_id=router_agent_id,
    )


def _load_context_agent_memory_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    return services["cached_payload"](
        ("context-hub-agent-memory", str(openclaw_dir)),
        10.0,
        lambda: services["load_agent_memory_data"](openclaw_dir, config),
    )


def _load_context_shared_context_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    router_agent_id = services["get_router_agent_id"](config)
    return services["cached_payload"](
        ("context-hub-shared-context", str(openclaw_dir), str(router_agent_id)),
        10.0,
        lambda: services["load_shared_context_data"](
            openclaw_dir,
            config,
            router_agent_id=router_agent_id,
        ),
    )


def _load_management_summary_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    now = services["now_utc"]()
    return services["cached_payload"](
        ("management-summary", str(openclaw_dir)),
        10.0,
        lambda: services["build_management_summary_snapshot"](
            openclaw_dir,
            config=config,
            now=now,
        ),
    )


def _load_management_recommendations_preview_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    now = services["now_utc"]()
    task_snapshot = _load_task_index_snapshot(handler, services)
    return services["cached_payload"](
        ("management-recommendations-preview", str(openclaw_dir)),
        10.0,
        lambda: services["build_management_recommendations_preview_snapshot"](
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        ),
    )


def _load_communications_summary_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    return services["cached_payload"](
        ("communications-summary", str(openclaw_dir)),
        10.0,
        lambda: services["build_communications_summary_snapshot"](openclaw_dir, config=config, now=services["now_utc"]()),
    )


def _load_communications_signals_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    return services["cached_payload"](
        ("communications-terminals", str(openclaw_dir)),
        10.0,
        lambda: services["build_communications_terminals_snapshot"](openclaw_dir, config=config, now=services["now_utc"]()),
    )


def _load_communications_commands_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    return services["cached_payload"](
        ("communications-commands", str(openclaw_dir)),
        30.0,
        lambda: services["build_communications_commands_snapshot"](openclaw_dir),
    )


def _load_communications_delivery_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    return services["cached_payload"](
        ("communications-delivery", str(openclaw_dir)),
        10.0,
        lambda: services["build_communications_delivery_snapshot"](openclaw_dir, config=config, now=services["now_utc"]()),
    )


def _load_communications_failures_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    return services["cached_payload"](
        ("communications-failures", str(openclaw_dir)),
        10.0,
        lambda: services["build_communications_failures_snapshot"](openclaw_dir, config=config, now=services["now_utc"]()),
    )


def _load_communications_audit_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    return services["cached_payload"](
        ("communications-audit", str(openclaw_dir)),
        10.0,
        lambda: services["build_communications_audit_snapshot"](openclaw_dir, now=services["now_utc"]()),
    )


def _load_activity_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    now = services["now_utc"]()
    task_snapshot = services["cached_payload"](
        ("orchestration-task-index", str(openclaw_dir)),
        10.0,
        lambda: services["build_orchestration_task_index_snapshot"](openclaw_dir, config=config, now=now),
    )
    return services["cached_payload"](
        ("activity-snapshot", str(openclaw_dir)),
        10.0,
        lambda: services["build_activity_snapshot"](
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        ),
    )


def _split_chat_thread_layers(thread_detail):
    thread_detail = thread_detail if isinstance(thread_detail, dict) else {}
    layered_keys = ("hotContext", "warmSummary", "coldHistory")
    thread_payload = {key: value for key, value in thread_detail.items() if key not in layered_keys}
    layered_payload = {
        key: value
        for key, value in ((name, thread_detail.get(name)) for name in layered_keys)
        if isinstance(value, dict)
    }
    return thread_payload, layered_payload


def _load_task_index_snapshot(handler, services):
    openclaw_dir = handler.server.openclaw_dir
    config = services["load_config"](openclaw_dir)
    now = services["now_utc"]()
    return services["cached_payload"](
        ("orchestration-task-index", str(openclaw_dir)),
        10.0,
        lambda: services["build_orchestration_task_index_snapshot"](openclaw_dir, config=config, now=now),
    )


def _load_dashboard_bootstrap_snapshot(handler, services):
    permissions = handler._permissions()
    permissions_key = json.dumps(permissions, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    openclaw_dir = handler.server.openclaw_dir

    def build():
        config = services["load_config"](openclaw_dir)
        payload = {
            "generatedAt": services["now_utc"]().isoformat().replace("+00:00", "Z"),
            "generatedAgo": "刚刚",
            "routerAgentId": str(services["get_router_agent_id"](config) or "").strip(),
            "runtime": {
                "permissions": permissions,
            },
        }
        payload["signature"] = str(services["dashboard_signature"](payload) or "").strip()
        return payload

    return services["cached_payload"](
        ("dashboard-bootstrap", str(openclaw_dir), permissions_key),
        2.0,
        build,
    )


def handle_api_read_route(handler, path, services):
    if handle_computer_use_read_route(handler, path, services):
        return True
    if handle_tool_bridge_route(handler, services, path):
        return True
    # New API routes for desktop client
    if handle_workflow_route(handler, services, path):
        return True
    if handle_kb_route(handler, services, path):
        return True
    if handle_automation_route(handler, services, path):
        return True
    if path == "/api/dashboard":
        payload = _load_dashboard_bootstrap_snapshot(handler, services)
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/agents":
        openclaw_dir = handler.server.openclaw_dir
        config = services["load_config"](openclaw_dir)
        now = services["now_utc"]()
        account_workspace = _read_account_workspace_projection(handler)
        agents_payload = services["cached_payload"](
            ("agents-snapshot", str(openclaw_dir)),
            10.0,
            lambda: {
                "agents": services["build_agent_cards_snapshot"](
                    openclaw_dir,
                    config=config,
                    now=now,
                ),
            },
        )
        projected_payload = _project_agents_response(agents_payload, account_workspace, services)
        body = (json.dumps(projected_payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/tasks":
        snapshot = _load_task_index_snapshot(handler, services)
        tasks = [
            services["compact_task_dashboard_payload"](task)
            for task in (snapshot.get("taskIndex") if isinstance(snapshot.get("taskIndex"), list) else [])
            if isinstance(task, dict)
        ]
        body = (json.dumps({"tasks": tasks}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path.startswith("/api/tasks/") and path != "/api/tasks/":
        task_id = path[len("/api/tasks/"):].strip("/")
        if not task_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 taskId。"}, status=400)
            return True
        try:
            detail = services["load_task_detail"](handler.server.openclaw_dir, task_id)
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "task_detail_failed", "message": str(error)}, status=404)
            return True
        handler._send_json({"ok": True, "task": detail})
        return True
    if path == "/api/task":
        task_id = str(handler._query().get("taskId", [""])[0] or "").strip()
        if not task_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 taskId。"}, status=400)
            return True
        try:
            detail = services["load_task_detail"](handler.server.openclaw_dir, task_id)
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "task_detail_failed", "message": str(error)}, status=404)
            return True
        handler._send_json({"ok": True, "task": detail})
        return True
    if path == "/api/conversations":
        page = handler._query().get("page", ["1"])[0]
        page_size = handler._query().get("pageSize", ["36"])[0]
        query_text = handler._query().get("q", [""])[0]
        include_commands = str(handler._query().get("includeCommands", [""])[0] or "").strip().lower() in {"1", "true", "yes"}
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()

        def build_conversations():
            conversations = services["build_conversations_catalog_snapshot"](handler.server.openclaw_dir)
            return _attach_payload_signature(
                _build_conversations_payload(
                    conversations,
                    page=page,
                    page_size=page_size,
                    query_text=query_text,
                    include_commands=include_commands,
                ),
                services,
                "conversations",
            )

        payload = services["cached_payload"](
            ("conversations-catalog", str(handler.server.openclaw_dir), str(page), str(page_size), str(query_text), str(int(include_commands))),
            2.0,
            build_conversations,
        )
        payload_signature = str(((payload.get("conversations") if isinstance(payload.get("conversations"), dict) else {}).get("signature")) or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/agent-teams":
        openclaw_dir = handler.server.openclaw_dir
        config = services["load_config"](openclaw_dir)
        now = services["now_utc"]()
        agent_teams = services["cached_payload"](
            ("agent-teams-snapshot", str(openclaw_dir)),
            10.0,
            lambda: services["build_agent_teams_snapshot"](
                openclaw_dir,
                config=config,
                now=now,
            ),
        )
        body = (json.dumps({"agentTeams": agent_teams}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/chat":
        if not handler._require_capability("conversationWrite", "当前账号没有访问聊天数据的权限。"):
            return True
        page = handler._query().get("page", ["1"])[0]
        page_size = handler._query().get("pageSize", ["24"])[0]
        status = handler._query().get("status", [""])[0]
        query_text = handler._query().get("q", [""])[0]
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()

        def build_chat_catalog():
            openclaw_dir = handler.server.openclaw_dir
            config = services["load_config"](openclaw_dir)
            now = services["now_utc"]()
            chat_catalog = services["build_chat_catalog_page_snapshot"](
                openclaw_dir,
                page=page,
                page_size=page_size,
                status=status,
                query_text=query_text,
                config=config,
                now=now,
            )
            return _attach_payload_signature(
                {"chat": chat_catalog},
                services,
                "chat",
            )

        payload = services["cached_payload"](
            ("chat-catalog", str(handler.server.openclaw_dir), str(page), str(page_size), str(status), str(query_text)),
            2.0,
            build_chat_catalog,
        )
        payload_signature = str(((payload.get("chat") if isinstance(payload.get("chat"), dict) else {}).get("signature")) or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path.startswith("/api/chat/threads/") and path != "/api/chat/threads/":
        if not handler._require_capability("conversationWrite", "当前账号没有访问聊天线程详情的权限。"):
            return True
        thread_id = path[len("/api/chat/threads/"):].strip("/")
        before_message_id = str(handler._query().get("beforeMessageId", [""])[0] or "").strip()
        message_limit = _parse_bounded_int(handler._query().get("limit", ["120"])[0], 120, minimum=1, maximum=200)
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        if not thread_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 threadId。"}, status=400)
            return True

        def build_chat_thread_payload():
            detail = services["load_chat_thread_detail"](
                handler.server.openclaw_dir,
                thread_id,
                message_limit=message_limit,
                before_message_id=before_message_id,
            )
            compact_detail = services["compact_chat_thread_detail"](detail)
            thread_payload, layered_payload = _split_chat_thread_layers(compact_detail)
            return _attach_payload_signature({"thread": thread_payload, **layered_payload}, services, "thread")

        try:
            payload = services["cached_payload"](
                ("chat-thread", str(handler.server.openclaw_dir), thread_id, str(before_message_id), str(message_limit)),
                2.0,
                build_chat_thread_payload,
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "chat_thread_failed", "message": str(error)}, status=404)
            return True
        payload_signature = str(((payload.get("thread") if isinstance(payload.get("thread"), dict) else {}).get("signature")) or "").strip()
        if known_signature and not before_message_id and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/chat/thread":
        if not handler._require_capability("conversationWrite", "当前账号没有访问聊天线程详情的权限。"):
            return True
        thread_id = str(handler._query().get("threadId", [""])[0] or "").strip()
        before_message_id = str(handler._query().get("beforeMessageId", [""])[0] or "").strip()
        message_limit = _parse_bounded_int(handler._query().get("limit", ["120"])[0], 120, minimum=1, maximum=200)
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        if not thread_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 threadId。"}, status=400)
            return True

        def build_chat_thread_payload():
            detail = services["load_chat_thread_detail"](
                handler.server.openclaw_dir,
                thread_id,
                message_limit=message_limit,
                before_message_id=before_message_id,
            )
            compact_detail = services["compact_chat_thread_detail"](detail)
            thread_payload, layered_payload = _split_chat_thread_layers(compact_detail)
            return _attach_payload_signature({"thread": thread_payload, **layered_payload}, services, "thread")

        try:
            payload = services["cached_payload"](
                ("chat-thread", str(handler.server.openclaw_dir), thread_id, str(before_message_id), str(message_limit)),
                2.0,
                build_chat_thread_payload,
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "chat_thread_failed", "message": str(error)}, status=404)
            return True
        payload_signature = str(((payload.get("thread") if isinstance(payload.get("thread"), dict) else {}).get("signature")) or "").strip()
        if known_signature and not before_message_id and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/chat/attachment/content":
        if not handler._require_capability("conversationWrite", "当前账号没有访问聊天线程附件的权限。"):
            return True
        thread_id = str(handler._query().get("threadId", [""])[0] or "").strip()
        message_id = str(handler._query().get("messageId", [""])[0] or "").strip()
        attachment_id = str(handler._query().get("attachmentId", [""])[0] or "").strip()
        if not thread_id or not message_id or not attachment_id:
            handler._send_json(
                {"ok": False, "error": "missing_params", "message": "需要 threadId、messageId 和 attachmentId。"},
                status=400,
            )
            return True
        attachment = services["chat_thread_attachment_content"](
            handler.server.openclaw_dir,
            thread_id,
            message_id,
            attachment_id,
        )
        if not isinstance(attachment, dict) or not attachment.get("bytes"):
            handler._send_json(
                {"ok": False, "error": "attachment_not_found", "message": "未找到该线程附件。"},
                status=404,
            )
            return True
        filename = str(attachment.get("name") or "attachment.bin").replace('"', "")
        handler._send_bytes(
            attachment.get("bytes") or b"",
            content_type=str(attachment.get("mimeType") or "application/octet-stream"),
            status=200,
            extra_headers=[("Content-Disposition", f'inline; filename="{filename}"')],
        )
        return True
    if path == "/api/communications/summary":
        handler._send_json({"ok": True, "communications": _load_communications_summary_snapshot(handler, services)})
        return True
    if path == "/api/communications/terminals":
        handler._send_json({"ok": True, "communications": _load_communications_signals_snapshot(handler, services)})
        return True
    if path == "/api/communications/commands":
        handler._send_json({"ok": True, "communications": _load_communications_commands_snapshot(handler, services)})
        return True
    if path == "/api/communications/channels":
        handler._send_json({"ok": True, "communications": _load_communications_delivery_snapshot(handler, services)})
        return True
    if path == "/api/communications/failures":
        handler._send_json({"ok": True, "communications": _load_communications_failures_snapshot(handler, services)})
        return True
    if path == "/api/communications/audit":
        handler._send_json({"ok": True, "communications": _load_communications_audit_snapshot(handler, services)})
        return True
    if path == "/api/activity/events":
        payload = _load_activity_snapshot(handler, services)
        handler._send_json({"ok": True, "events": payload.get("events", []) if isinstance(payload.get("events"), list) else []})
        return True
    if path == "/api/activity/relays":
        payload = _load_activity_snapshot(handler, services)
        handler._send_json({"ok": True, "relays": payload.get("relays", []) if isinstance(payload.get("relays"), list) else []})
        return True
    if path == "/api/conversations/transcript":
        agent_id = str(handler._query().get("agentId", [""])[0] or "").strip()
        session_id = str(handler._query().get("sessionId", [""])[0] or "").strip()
        conversation_key = str(handler._query().get("conversationKey", [""])[0] or "").strip()
        if not agent_id or (not session_id and not conversation_key):
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 agentId，以及 sessionId 或 conversationKey。"}, status=400)
            return True
        conversation = services["load_conversation_transcript"](
            handler.server.openclaw_dir,
            agent_id,
            session_id,
            conversation_key,
        )
        handler._send_json({"ok": True, "conversation": conversation})
        return True
    if path == "/api/conversations/attachment/content":
        agent_id = str(handler._query().get("agentId", [""])[0] or "").strip()
        session_id = str(handler._query().get("sessionId", [""])[0] or "").strip()
        conversation_key = str(handler._query().get("conversationKey", [""])[0] or "").strip()
        attachment_id = str(handler._query().get("attachmentId", [""])[0] or "").strip()
        if not agent_id or not attachment_id or (not session_id and not conversation_key):
            handler._send_json(
                {"ok": False, "error": "missing_params", "message": "需要 agentId、attachmentId，以及 sessionId 或 conversationKey。"},
                status=400,
            )
            return True
        attachment = services["conversation_attachment_content"](
            handler.server.openclaw_dir,
            agent_id,
            attachment_id,
            session_id,
            conversation_key,
        )
        if not isinstance(attachment, dict) or not attachment.get("bytes"):
            handler._send_json(
                {"ok": False, "error": "attachment_not_found", "message": "未找到该会话附件。"},
                status=404,
            )
            return True
        filename = str(attachment.get("name") or "attachment.bin").replace('"', "")
        handler._send_bytes(
            attachment.get("bytes") or b"",
            str(attachment.get("mimeType") or attachment.get("type") or "application/octet-stream"),
            extra_headers=[("Content-Disposition", f'inline; filename="{filename}"')],
        )
        return True
    # /api/events is an alias for /api/activity/events
    if path == "/api/events":
        payload = _load_activity_snapshot(handler, services)
        handler._send_json({"ok": True, "events": payload.get("events", []) if isinstance(payload.get("events"), list) else []})
        return True
    if path == "/api/metrics":
        openclaw_dir = handler.server.openclaw_dir
        config = services["load_config"](openclaw_dir)
        now = services["now_utc"]()
        metrics = services["cached_payload"](
            ("metrics-snapshot", str(openclaw_dir)),
            10.0,
            lambda: services["build_metrics_snapshot"](
                openclaw_dir,
                config=config,
                now=now,
            ),
        )
        body = (json.dumps({"metrics": metrics}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/themes/current":
        snapshot = _load_theme_snapshot(handler, services)
        handler._send_json(
            {
                "ok": True,
                "routerAgentId": str(snapshot.get("routerAgentId") or "").strip(),
                "theme": snapshot.get("theme", {}) if isinstance(snapshot.get("theme"), dict) else {},
            }
        )
        return True
    if path == "/api/themes/catalog":
        snapshot = _load_theme_snapshot(handler, services)
        handler._send_json(
            {
                "ok": True,
                "themeCatalog": snapshot.get("themeCatalog", []) if isinstance(snapshot.get("themeCatalog"), list) else [],
            }
        )
        return True
    if path == "/api/themes/history":
        snapshot = _load_theme_snapshot(handler, services)
        handler._send_json(
            {
                "ok": True,
                "themeHistory": snapshot.get("themeHistory", []) if isinstance(snapshot.get("themeHistory"), list) else [],
            }
        )
        return True
    if path == "/api/themes/workforce":
        snapshot = _load_theme_snapshot(handler, services)
        handler._send_json(
            {
                "ok": True,
                "themeWorkforce": snapshot.get("themeWorkforce", {}) if isinstance(snapshot.get("themeWorkforce"), dict) else {},
            }
        )
        return True
    if path == "/api/themes/switch-status":
        handler._send_json({"ok": True, "job": services["load_theme_switch_status"](handler.server.openclaw_dir)})
        return True
    if path == "/api/skills":
        detail = services["load_skills_detail"](handler.server.openclaw_dir)
        body = (json.dumps({"skills": detail}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/skills/pack":
        pack_id = str(handler._query().get("id", [""])[0] or "").strip()
        if not pack_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 workflow pack id。"}, status=400)
            return True
        try:
            detail = services["load_skill_pack_detail"](handler.server.openclaw_dir, pack_id)
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "skill_pack_failed", "message": str(error)}, status=404)
            return True
        handler._send_json({"ok": True, "pack": detail})
        return True
    if path == "/api/management/run":
        run_id = str(handler._query().get("runId", [""])[0] or "").strip()
        if not run_id:
            handler._send_json({"ok": False, "error": "missing_params", "message": "需要 runId。"}, status=400)
            return True
        try:
            detail = services["build_management_run_snapshot"](handler.server.openclaw_dir, run_id)
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "run_failed", "message": str(error)}, status=404)
            return True
        handler._send_json({"ok": True, "run": detail})
        return True
    if path == "/api/management/bootstrap":
        handler._send_json({"ok": True, "management": _load_management_summary_snapshot(handler, services)})
        return True
    if path == "/api/management/recommendations/preview":
        handler._send_json({"ok": True, "management": _load_management_recommendations_preview_snapshot(handler, services)})
        return True
    if path == "/api/management/runs":
        detail = services["build_management_runs_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/decision/intelligence":
        detail = services["build_management_decision_intelligence_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/decision/quality":
        detail = services["build_management_decision_quality_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/decision/sources":
        detail = services["build_management_decision_sources_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/decision/recommendations":
        detail = services["build_management_decision_recommendations_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/automation/summary":
        detail = services["build_management_automation_summary_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/automation/rules":
        detail = services["build_management_automation_rules_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/automation/delivery":
        detail = services["build_management_automation_delivery_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/automation/alerts":
        detail = services["build_management_automation_alerts_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/insights/health/summary":
        detail = services["build_management_insights_health_summary_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/insights/health/agents":
        detail = services["build_management_insights_health_agents_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/insights/conversations":
        detail = services["build_management_insights_conversations_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/management/insights/reports/overview":
        detail = services["build_management_reports_overview_snapshot"](handler.server.openclaw_dir)
        handler._send_json({"ok": True, "management": detail})
        return True
    if path == "/api/context":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_context_summary_payload({"contextHub": _load_context_hub_snapshot(handler, services)}),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/context/agent-memory":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_context_slice_payload({"contextHub": {"agentMemory": _load_context_agent_memory_snapshot(handler, services)}}, "agentMemory"),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/context/shared-context":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_context_slice_payload({"contextHub": {"sharedContext": _load_context_shared_context_snapshot(handler, services)}}, "sharedContext"),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/overview":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            {"orchestration": services["build_orchestration_overview_snapshot"](handler.server.openclaw_dir)},
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/workflows":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_orchestration_workflow_payload(
                {"orchestration": services["build_orchestration_workflows_snapshot"](handler.server.openclaw_dir)}
            ),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/routing":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_orchestration_slice_payload(
                {"orchestration": services["build_orchestration_routing_snapshot"](handler.server.openclaw_dir)},
                "routing",
            ),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/replays":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_orchestration_slice_payload(
                {"orchestration": services["build_orchestration_replays_snapshot"](handler.server.openclaw_dir)},
                "replays",
            ),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/review":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            {"orchestration": services["build_orchestration_review_snapshot"](handler.server.openclaw_dir)},
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/suggestions":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            {"orchestration": services["build_orchestration_suggestions_snapshot"](handler.server.openclaw_dir)},
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/orchestration/planning":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(
            _build_orchestration_slice_payload(
                {"orchestration": services["build_orchestration_planning_snapshot"](handler.server.openclaw_dir)},
                "planning",
            ),
            services,
        )
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_summary_payload(handler, services), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/api-reference":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "apiReference"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/runtime-governance":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "runtimeGovernanceSummary"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/runtime-governance/runtimes":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "runtimeGovernanceRuntimes"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/runtime-governance/packs":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "runtimeGovernancePacks"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/runtime-governance/browser-sessions":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "runtimeGovernanceBrowserSessions"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/tenants":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "tenants"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/api-keys":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "apiKeys"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/webhooks":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "webhooks"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path == "/api/platform/audit-logs":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        payload = _attach_root_signature(_build_platform_slice_payload(handler, services, "auditLogs"), services)
        payload_signature = str(payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, **payload})
        return True
    if path in ("/api/openclaw", "/api/openclaw/overview"):
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        account_workspace = _read_account_workspace_projection(handler)
        openclaw_payload = services["cached_payload"](
            ("openclaw-overview-route", str(handler.server.openclaw_dir)),
            10.0,
            lambda: _attach_root_signature(
                services["load_openclaw_dashboard_summary"](handler.server.openclaw_dir),
                services,
            ),
        )
        openclaw_payload = _project_openclaw_payload(openclaw_payload, account_workspace, services)
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/models":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        account_workspace = _read_account_workspace_projection(handler)
        openclaw_payload = services["cached_payload"](
            ("openclaw-models-route", str(handler.server.openclaw_dir)),
            10.0,
            lambda: _attach_root_signature(
                services["load_openclaw_models_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        openclaw_payload = _project_openclaw_payload(openclaw_payload, account_workspace, services)
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/gateway":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = _attach_root_signature(
            services["load_openclaw_gateway_panel_data"](handler.server.openclaw_dir),
            services,
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/browser":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = _attach_root_signature(
            services["load_openclaw_browser_panel_data"](handler.server.openclaw_dir),
            services,
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/memory":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-memory-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_memory_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/memory-workflow":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-memory-workflow-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_memory_workflow_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/session-governance":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-session-governance-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_session_governance_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/orchestration":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-orchestration-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_orchestration_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/message-gateway":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-message-gateway-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_message_gateway_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/sandbox":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-sandbox-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_sandbox_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/voice-workflow":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-voice-workflow-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_voice_workflow_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/overview":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        account_workspace = _read_account_workspace_projection(handler)
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-overview-route", str(handler.server.openclaw_dir)),
            10.0,
            lambda: _attach_root_signature(
                services["load_openclaw_runtime_overview_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        openclaw_payload = _project_openclaw_payload(openclaw_payload, account_workspace, services)
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/local-runtime":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-local-runtime-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_local_runtime_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/runtime/skill-growth":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-runtime-skill-growth-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                services["load_openclaw_skill_growth_panel_data"](handler.server.openclaw_dir),
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/skills/check":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-skills-check-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                {"openclaw": services["load_openclaw_skills_check_panel_data"](handler.server.openclaw_dir)},
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/openclaw/skills/agent-params":
        known_signature = str(handler._query().get("signature", [""])[0] or "").strip()
        openclaw_payload = services["cached_payload"](
            ("openclaw-skills-agent-params-route", str(handler.server.openclaw_dir)),
            15.0,
            lambda: _attach_root_signature(
                {"openclaw": services["load_openclaw_agent_params_panel_data"](handler.server.openclaw_dir)},
                services,
            ),
        )
        payload_signature = str(openclaw_payload.get("signature") or "").strip()
        if known_signature and known_signature == payload_signature:
            handler._send_json({"ok": True, "unchanged": True, "signature": known_signature})
            return True
        handler._send_json({"ok": True, "openclaw": openclaw_payload})
        return True
    if path == "/api/deliverables":
        openclaw_dir = handler.server.openclaw_dir
        config = services["load_config"](openclaw_dir)
        now = services["now_utc"]()
        deliverables = services["cached_payload"](
            ("deliverables-snapshot", str(openclaw_dir)),
            10.0,
            lambda: services["build_deliverables_snapshot"](
                openclaw_dir,
                config=config,
                now=now,
            ),
        )
        body = (json.dumps({"deliverables": deliverables}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/deliverables/download":
        if not handler._require_capability("read", "当前账号没有下载交付产物的权限。"):
            return True
        deliverable_id = str(handler._query().get("id", [""])[0] or "").strip()
        try:
            openclaw_dir = handler.server.openclaw_dir
            config = services["load_config"](openclaw_dir)
            now = services["now_utc"]()
            deliverables = services["cached_payload"](
                ("deliverables-snapshot", str(openclaw_dir)),
                10.0,
                lambda: services["build_deliverables_snapshot"](
                    openclaw_dir,
                    config=config,
                    now=now,
                ),
            )
            archive = services["build_deliverable_zip_bytes"](
                openclaw_dir,
                {"deliverables": deliverables},
                deliverable_id,
            )
        except RuntimeError as error:
            handler._send_json({"ok": False, "error": "deliverable_download_failed", "message": str(error)}, status=400)
            return True
        handler._audit("deliverable_download", detail=f"下载交付产物 {deliverable_id}", meta={"deliverableId": deliverable_id, "path": str(archive.get("path", ""))})
        handler._send_bytes(
            archive["body"],
            "application/zip",
            extra_headers=[("Content-Disposition", f'attachment; filename="{archive["filename"]}"')],
        )
        return True
    if path == "/api/admin/bootstrap":
        if not handler._can("auditView"):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "当前账号没有查看后台治理数据的权限。"}, status=403)
            return True
        payload = services["build_admin_bootstrap_snapshot"](
            handler.server.openclaw_dir,
            services["load_config"](handler.server.openclaw_dir),
            services["now_utc"](),
        )
        body = (json.dumps({"admin": payload}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/admin/installations":
        if not handler._can("auditView"):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "当前账号没有查看后台治理数据的权限。"}, status=403)
            return True
        payload = services["build_admin_installations_snapshot"](
            handler.server.openclaw_dir,
            services["load_config"](handler.server.openclaw_dir),
            services["now_utc"](),
        )
        body = (json.dumps({"admin": payload}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/admin/tenants":
        if not handler._can("auditView"):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "当前账号没有查看后台治理数据的权限。"}, status=403)
            return True
        payload = services["build_admin_tenants_snapshot"](
            handler.server.openclaw_dir,
            services["load_config"](handler.server.openclaw_dir),
            services["now_utc"](),
        )
        body = (json.dumps({"admin": payload}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/admin/api-keys":
        if not handler._can("auditView"):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "当前账号没有查看后台治理数据的权限。"}, status=403)
            return True
        payload = services["build_admin_api_keys_snapshot"](
            handler.server.openclaw_dir,
            services["load_config"](handler.server.openclaw_dir),
            services["now_utc"](),
        )
        body = (json.dumps({"admin": payload}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/api/admin/users":
        if not handler._can("auditView"):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "当前账号没有查看后台治理数据的权限。"}, status=403)
            return True
        payload = services["build_admin_users_snapshot"](
            handler.server.openclaw_dir,
            services["load_config"](handler.server.openclaw_dir),
            services["now_utc"](),
        )
        body = (json.dumps({"admin": payload}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8")
        return True
    if path == "/events":
        handler._serve_events()
        return True
    return False


def handle_api_write_route(handler, path, services):
    if handle_tool_bridge_route(handler, services, path):
        return True
    if handle_workflow_route(handler, services, path):
        return True
    if handle_kb_route(handler, services, path):
        return True
    if handle_automation_route(handler, services, path):
        return True
    return False
