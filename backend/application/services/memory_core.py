from __future__ import annotations

import logging
import os
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


MEMORY_BOOTSTRAP_VERSION = _DelegatedSymbol("MEMORY_BOOTSTRAP_VERSION")
MEMORY_PROJECTION_POOL = _DelegatedSymbol("MEMORY_PROJECTION_POOL")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_task_long_term_memory = _DelegatedSymbol("compact_task_long_term_memory")
coordination_reply_entries = _DelegatedSymbol("coordination_reply_entries")
now_iso = _DelegatedSymbol("now_iso")
safe_list = _DelegatedSymbol("safe_list")
save_project_metadata = _DelegatedSymbol("save_project_metadata")
store_append_memory_event = _DelegatedSymbol("store_append_memory_event")
store_get_memory_snapshot = _DelegatedSymbol("store_get_memory_snapshot")
store_save_memory_snapshot = _DelegatedSymbol("store_save_memory_snapshot")
summarize_task_execution_text = _DelegatedSymbol("summarize_task_execution_text")
team_decision_log_text = _DelegatedSymbol("team_decision_log_text")
team_memory_text = _DelegatedSymbol("team_memory_text")
team_operating_brief = _DelegatedSymbol("team_operating_brief")


def memory_authority_query_matches(message_text):
    normalized = str(message_text or "").strip().lower()
    if not normalized:
        return False
    triggers = (
        "长期记忆",
        "记忆",
        "memory",
        "memory.md",
        "memory_search",
        "下次还记得",
        "怎么记住",
        "如何记住",
        "你怎么记得",
        "remember",
        "how do you remember",
        "how memory works",
    )
    return any(token in normalized for token in triggers)


def build_memory_authority_context_message(message_text, metadata, agents=None, teams=None):
    if not memory_authority_query_matches(message_text):
        return ""
    memory_system = memory_system_status_payload(metadata, agents=agents, teams=teams)
    if not memory_system.get("enabled"):
        return ""
    provider = str(memory_system.get("provider") or "local").strip() or "local"
    authority = str(memory_system.get("authority") or "project-metadata").strip() or "project-metadata"
    write_mode = str(memory_system.get("writeMode") or "async_projection").strip() or "async_projection"
    return (
        "Product memory system\n"
        f"- Long-term memory in this product is backed by the product memory system ({provider}, authority {authority}, write mode {write_mode}).\n"
        "- Do not claim that long-term memory here depends only on MEMORY.md or memory/*.md.\n"
        "- MEMORY.md and memory/*.md are editable memory documents, but the authoritative long-term memory comes from product memory snapshots/events and projected task/team context.\n"
        "- If the user asks how memory works here, explain it that way instead of guessing from generic OpenClaw habits."
    )


def default_memory_system_payload(agents=None, teams=None):
    normalized_agents = [
        str((item or {}).get("id") or "").strip()
        for item in safe_list(agents)
        if isinstance(item, dict) and str((item or {}).get("id") or "").strip()
    ]
    normalized_teams = [
        {
            "teamId": str((item or {}).get("id") or "").strip(),
            "name": str((item or {}).get("name") or "").strip(),
        }
        for item in safe_list(teams)
        if isinstance(item, dict) and str((item or {}).get("id") or "").strip()
    ]
    return {
        "enabled": True,
        "provider": "local",
        "authority": "project-metadata",
        "writeMode": "async_projection",
        "inspiredBy": "MemOS",
        "company": {
            "enabled": True,
            "label": "公司级长期记忆",
            "fields": [
                "长期经营判断",
                "复盘结论",
                "共享经营原则",
            ],
        },
        "team": {
            "enabled": True,
            "defaultMode": "shared",
            "fields": [
                "workingMemory",
                "decisionLog",
                "taskLongTermMemory",
            ],
            "teams": normalized_teams,
        },
        "agent": {
            "enabled": True,
            "defaultMode": "personal",
            "shareBackToTeam": True,
            "agentIds": normalized_agents,
        },
        "taskTemplates": {
            "default": {
                "label": "通用长期任务",
                "mode": "long_running",
                "summaryLabel": "长期任务记忆",
                "bootstrapNote": "这条任务会持续沉淀判断、最近学到和推进记录，不会每次都像第一次接手。",
                "reviewCadence": "milestone",
            },
            "company_auto_operation": {
                "label": "经营公司任务",
                "mode": "company_auto_operation",
                "summaryLabel": "长期经营记忆",
                "bootstrapNote": "这是一条长期经营任务，要持续积累经营判断、最近学到和复盘记录。",
                "reviewCadence": "daily",
            },
        },
    }


def normalize_memory_system_payload(payload, agents=None, teams=None):
    defaults = default_memory_system_payload(agents=agents, teams=teams)
    current = deepcopy(payload) if isinstance(payload, dict) else {}
    team_defaults = defaults.get("team", {})
    current_team = current.get("team") if isinstance(current.get("team"), dict) else {}
    default_team_ids = {
        str(item.get("teamId") or "").strip(): item
        for item in safe_list(team_defaults.get("teams"))
        if isinstance(item, dict) and str(item.get("teamId") or "").strip()
    }
    existing_team_ids = {
        str(item.get("teamId") or "").strip(): item
        for item in safe_list(current_team.get("teams"))
        if isinstance(item, dict) and str(item.get("teamId") or "").strip()
    }
    merged_teams = []
    for team_id, item in default_team_ids.items():
        existing = existing_team_ids.get(team_id, {})
        merged_teams.append(
            {
                "teamId": team_id,
                "name": str(existing.get("name") or item.get("name") or "").strip(),
            }
        )
    default_agent_ids = clean_unique_strings((defaults.get("agent", {}) or {}).get("agentIds") or [])
    existing_agent_ids = clean_unique_strings((current.get("agent", {}) or {}).get("agentIds") or [])
    merged_agent_ids = clean_unique_strings(existing_agent_ids + default_agent_ids)
    task_templates = defaults.get("taskTemplates", {})
    current_templates = current.get("taskTemplates") if isinstance(current.get("taskTemplates"), dict) else {}
    merged_templates = {}
    for template_id, default_template in task_templates.items():
        existing_template = current_templates.get(template_id) if isinstance(current_templates.get(template_id), dict) else {}
        merged_templates[template_id] = {
            **default_template,
            **existing_template,
            "label": str(existing_template.get("label") or default_template.get("label") or "").strip(),
            "mode": str(existing_template.get("mode") or default_template.get("mode") or "").strip(),
            "summaryLabel": str(existing_template.get("summaryLabel") or default_template.get("summaryLabel") or "").strip(),
            "bootstrapNote": str(existing_template.get("bootstrapNote") or default_template.get("bootstrapNote") or "").strip(),
            "reviewCadence": str(existing_template.get("reviewCadence") or default_template.get("reviewCadence") or "").strip(),
        }
    return {
        **defaults,
        **current,
        "enabled": True if current.get("enabled") is None else bool(current.get("enabled")),
        "provider": str(current.get("provider") or defaults.get("provider") or "local").strip() or "local",
        "authority": str(current.get("authority") or defaults.get("authority") or "project-metadata").strip() or "project-metadata",
        "writeMode": str(current.get("writeMode") or defaults.get("writeMode") or "async_projection").strip() or "async_projection",
        "inspiredBy": str(current.get("inspiredBy") or defaults.get("inspiredBy") or "MemOS").strip() or "MemOS",
        "company": {
            **defaults.get("company", {}),
            **(current.get("company") if isinstance(current.get("company"), dict) else {}),
            "enabled": True,
        },
        "team": {
            **team_defaults,
            **current_team,
            "enabled": True,
            "teams": merged_teams,
        },
        "agent": {
            **defaults.get("agent", {}),
            **(current.get("agent") if isinstance(current.get("agent"), dict) else {}),
            "enabled": True,
            "agentIds": merged_agent_ids,
        },
        "taskTemplates": merged_templates,
    }


def ensure_default_memory_bootstrap(openclaw_dir, metadata, agents=None, teams=None):
    current_metadata = deepcopy(metadata if isinstance(metadata, dict) else {})
    bootstrap_target_version = int(MEMORY_BOOTSTRAP_VERSION)
    try:
        bootstrap_version = int(current_metadata.get("memoryBootstrapVersion") or 0)
    except (TypeError, ValueError):
        bootstrap_version = 0
    normalized_memory = normalize_memory_system_payload(
        current_metadata.get("memorySystem"),
        agents=agents,
        teams=teams,
    )
    existing_memory = current_metadata.get("memorySystem") if isinstance(current_metadata.get("memorySystem"), dict) else {}
    if bootstrap_version >= bootstrap_target_version and existing_memory == normalized_memory:
        return current_metadata
    current_metadata["memorySystem"] = normalized_memory
    current_metadata["memoryBootstrapVersion"] = bootstrap_target_version
    current_metadata["memoryBootstrapAt"] = now_iso()
    current_metadata["memoryBootstrapSource"] = (
        "product-default-self-heal"
        if bootstrap_version >= bootstrap_target_version
        else "product-default"
    )
    save_project_metadata(openclaw_dir, current_metadata)
    return current_metadata


def current_memory_system(metadata, agents=None, teams=None):
    if isinstance(metadata, dict) and isinstance(metadata.get("memorySystem"), dict):
        return normalize_memory_system_payload(metadata.get("memorySystem"), agents=agents, teams=teams)
    return normalize_memory_system_payload({}, agents=agents, teams=teams)


def task_memory_template(memory_system, task_type=""):
    memory_system = memory_system if isinstance(memory_system, dict) else {}
    templates = memory_system.get("taskTemplates") if isinstance(memory_system.get("taskTemplates"), dict) else {}
    normalized_task_type = str(task_type or "").strip().lower()
    if normalized_task_type and isinstance(templates.get(normalized_task_type), dict):
        return templates.get(normalized_task_type)
    return templates.get("default") if isinstance(templates.get("default"), dict) else {}


def memory_system_status_payload(metadata, agents=None, teams=None):
    memory_system = current_memory_system(metadata, agents=agents, teams=teams)
    task_templates = memory_system.get("taskTemplates") if isinstance(memory_system.get("taskTemplates"), dict) else {}
    return {
        "enabled": bool(memory_system.get("enabled")),
        "provider": str(memory_system.get("provider") or "local").strip() or "local",
        "authority": str(memory_system.get("authority") or "project-metadata").strip() or "project-metadata",
        "writeMode": str(memory_system.get("writeMode") or "async_projection").strip() or "async_projection",
        "inspiredBy": str(memory_system.get("inspiredBy") or "MemOS").strip() or "MemOS",
        "bootstrapVersion": int(metadata.get("memoryBootstrapVersion") or 0) if isinstance(metadata, dict) else 0,
        "bootstrappedAt": str((metadata or {}).get("memoryBootstrapAt") or "").strip() if isinstance(metadata, dict) else "",
        "scopeSummary": {
            "companyEnabled": bool((memory_system.get("company") or {}).get("enabled")),
            "teamCount": len(safe_list((memory_system.get("team") or {}).get("teams"))),
            "agentCount": len(clean_unique_strings((memory_system.get("agent") or {}).get("agentIds") or [])),
            "taskTemplateCount": len(task_templates),
        },
        "taskTemplates": [
            {
                "id": template_id,
                "label": str(template.get("label") or "").strip(),
                "mode": str(template.get("mode") or "").strip(),
                "reviewCadence": str(template.get("reviewCadence") or "").strip(),
            }
            for template_id, template in task_templates.items()
            if isinstance(template, dict)
        ],
    }


def ensure_default_memory_authority_seed(openclaw_dir, agents=None, teams=None, metadata=None):
    bootstrap_target_version = int(MEMORY_BOOTSTRAP_VERSION)
    memory_system = current_memory_system(metadata, agents=agents, teams=teams)
    company_snapshot = store_get_memory_snapshot(openclaw_dir, "company", "company")
    if not company_snapshot:
        store_save_memory_snapshot(
            openclaw_dir,
            {
                "scope": "company",
                "ownerId": "company",
                "label": "公司级长期记忆",
                "summary": "这里沉淀长期经营判断、共享原则和复盘结论，供所有长期任务沿用。",
                "learningHighlights": [],
                "recentNotes": [],
                "meta": {
                    "source": "product-default",
                    "bootstrapVersion": bootstrap_target_version,
                    "inspiredBy": memory_system.get("inspiredBy") or "MemOS",
                },
            },
        )
    for team in safe_list((memory_system.get("team") or {}).get("teams")):
        if not isinstance(team, dict):
            continue
        team_id = str(team.get("teamId") or "").strip()
        if not team_id or store_get_memory_snapshot(openclaw_dir, "team", team_id):
            continue
        team_record = next(
            (item for item in safe_list(teams) if isinstance(item, dict) and str(item.get("id") or "").strip() == team_id),
            {},
        )
        team_name = str(team.get("name") or team_record.get("name") or team_id).strip()
        team_summary = clean_unique_strings(
            [
                summarize_task_execution_text(team_operating_brief(team_record) or team_record.get("description") or "", limit=120),
                summarize_task_execution_text(team_memory_text(team_record) or "", limit=120),
                summarize_task_execution_text(team_decision_log_text(team_record) or "", limit=120),
            ]
        )
        store_save_memory_snapshot(
            openclaw_dir,
            {
                "scope": "team",
                "ownerId": team_id,
                "label": team_name or team_id,
                "summary": " ".join(team_summary) if team_summary else f"{team_name or team_id} 的共享团队记忆。",
                "learningHighlights": [],
                "recentNotes": [],
                "meta": {
                    "source": "product-default",
                    "bootstrapVersion": bootstrap_target_version,
                },
            },
        )
    for agent_id in clean_unique_strings((memory_system.get("agent") or {}).get("agentIds") or []):
        if not agent_id or store_get_memory_snapshot(openclaw_dir, "agent", agent_id):
            continue
        agent_record = next(
            (item for item in safe_list(agents) if isinstance(item, dict) and str(item.get("id") or "").strip() == agent_id),
            {},
        )
        label = str(
            agent_record.get("title")
            or ((agent_record.get("identity", {}) if isinstance(agent_record.get("identity"), dict) else {}) or {}).get("name")
            or agent_id
        ).strip()
        store_save_memory_snapshot(
            openclaw_dir,
            {
                "scope": "agent",
                "ownerId": agent_id,
                "label": label or agent_id,
                "summary": f"这里沉淀 {label or agent_id} 的个人判断、偏好和可复用方法。",
                "learningHighlights": [],
                "recentNotes": [],
                "meta": {
                    "source": "product-default",
                    "bootstrapVersion": bootstrap_target_version,
                },
            },
        )


def should_project_memory_async():
    if os.environ.get("MISSION_CONTROL_FORCE_SYNC_MEMORY") == "1":
        return False
    return not bool(os.environ.get("PYTEST_CURRENT_TEST"))


def project_memory_records(openclaw_dir, records, events=None):
    for record in safe_list(records):
        if isinstance(record, dict):
            store_save_memory_snapshot(openclaw_dir, record)
    for event in safe_list(events):
        if isinstance(event, dict):
            store_append_memory_event(openclaw_dir, event)


def project_memory_records_async(openclaw_dir, records, events=None):
    if not safe_list(records) and not safe_list(events):
        return
    if not should_project_memory_async():
        project_memory_records(openclaw_dir, records, events)
        return

    def _worker():
        try:
            project_memory_records(openclaw_dir, records, events)
        except Exception as exc:  # pragma: no cover - background projection best effort
            logging.warning("memory projection failed: %s", exc)

    MEMORY_PROJECTION_POOL.submit(_worker)


def build_memory_projection_payloads(
    task_id="",
    thread_id="",
    task_title="",
    team_id="",
    team_name="",
    task_memory=None,
    team_policy=None,
    dispatch_state=None,
):
    records = []
    events = []
    normalized_task_id = str(task_id or "").strip()
    normalized_thread_id = str(thread_id or "").strip()
    normalized_team_id = str(team_id or "").strip()
    normalized_team_name = str(team_name or "").strip()
    task_memory_payload = compact_task_long_term_memory(task_memory)
    team_policy = team_policy if isinstance(team_policy, dict) else {}
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    updated_at = str(
        task_memory_payload.get("updatedAt")
        or team_policy.get("workingMemoryUpdatedAt")
        or dispatch_state.get("at")
        or now_iso()
    ).strip()

    if normalized_task_id and task_memory_payload.get("longTermMemory"):
        records.append(
            {
                "scope": "task",
                "ownerId": normalized_task_id,
                "label": str(task_title or normalized_task_id).strip() or normalized_task_id,
                "summary": str(task_memory_payload.get("longTermMemory") or "").strip(),
                "learningHighlights": clean_unique_strings(task_memory_payload.get("learningHighlights") or []),
                "recentNotes": safe_list(task_memory_payload.get("recentNotes"))[:4],
                "relatedTaskId": normalized_task_id,
                "relatedThreadId": normalized_thread_id,
                "updatedAt": updated_at,
                "meta": {"source": "task-long-term-memory"},
            }
        )
        events.append(
            {
                "scope": "task",
                "ownerId": normalized_task_id,
                "eventType": "snapshot_update",
                "summary": str(task_memory_payload.get("longTermMemory") or "").strip(),
                "relatedTaskId": normalized_task_id,
                "relatedThreadId": normalized_thread_id,
                "meta": {"source": "task-long-term-memory"},
            }
        )

    if normalized_team_id and (
        str(team_policy.get("workingMemory") or "").strip()
        or str(team_policy.get("currentFocus") or "").strip()
        or str(team_policy.get("teamMemory") or "").strip()
    ):
        team_summary = clean_unique_strings(
            [
                summarize_task_execution_text(team_policy.get("workingMemory") or "", limit=140),
                summarize_task_execution_text(team_policy.get("currentFocus") or "", limit=140),
                summarize_task_execution_text(team_policy.get("teamMemory") or "", limit=120),
            ]
        )
        records.append(
            {
                "scope": "team",
                "ownerId": normalized_team_id,
                "label": normalized_team_name or normalized_team_id,
                "summary": " ".join(team_summary),
                "learningHighlights": clean_unique_strings((task_memory_payload.get("learningHighlights") or [])[:2]),
                "recentNotes": safe_list(task_memory_payload.get("recentNotes"))[:2],
                "relatedTaskId": normalized_task_id,
                "relatedThreadId": normalized_thread_id,
                "updatedAt": updated_at,
                "meta": {"source": "team-policy"},
            }
        )
        events.append(
            {
                "scope": "team",
                "ownerId": normalized_team_id,
                "eventType": "shared_memory_update",
                "summary": " ".join(team_summary)[:320],
                "relatedTaskId": normalized_task_id,
                "relatedThreadId": normalized_thread_id,
                "meta": {"source": "team-policy"},
            }
        )

    for response in coordination_reply_entries(dispatch_state.get("responses"), limit=4):
        agent_id = str(response.get("agentId") or "").strip()
        if not agent_id:
            continue
        agent_label = str(response.get("agentDisplayName") or agent_id).strip()
        preview = summarize_task_execution_text(response.get("replyPreview") or "", limit=160)
        if not preview:
            continue
        records.append(
            {
                "scope": "agent",
                "ownerId": agent_id,
                "label": agent_label,
                "summary": preview,
                "learningHighlights": [],
                "recentNotes": [
                    {
                        "at": updated_at,
                        "summary": preview,
                        "focus": summarize_task_execution_text(task_title or "", limit=96),
                        "ownerLabel": agent_label,
                    }
                ],
                "relatedTaskId": normalized_task_id,
                "relatedThreadId": normalized_thread_id,
                "updatedAt": updated_at,
                "meta": {"source": "agent-response"},
            }
        )
        events.append(
            {
                "scope": "agent",
                "ownerId": agent_id,
                "eventType": "personal_memory_update",
                "summary": preview,
                "relatedTaskId": normalized_task_id,
                "relatedThreadId": normalized_thread_id,
                "meta": {"label": agent_label, "source": "agent-response"},
            }
        )
    return records, events


def hydrate_thread_memory_authority(openclaw_dir, thread, team_policy=None):
    thread = thread if isinstance(thread, dict) else {}
    next_policy = deepcopy(team_policy) if isinstance(team_policy, dict) else {}
    linked_task_id = str(thread.get("linkedTaskId") or "").strip()
    if linked_task_id:
        task_snapshot = store_get_memory_snapshot(openclaw_dir, "task", linked_task_id) or {}
        if task_snapshot:
            existing_task_memory = compact_task_long_term_memory(next_policy.get("taskLongTermMemory"))
            if not existing_task_memory.get("longTermMemory"):
                next_policy["taskLongTermMemory"] = compact_task_long_term_memory(
                    {
                        "longTermMemory": task_snapshot.get("summary"),
                        "learningHighlights": safe_list(task_snapshot.get("learning")),
                        "recentNotes": safe_list(task_snapshot.get("notes")),
                        "updatedAt": task_snapshot.get("updatedAt"),
                    }
                )
    team_meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_id = str(team_meta.get("teamId") or "").strip()
    if team_id:
        team_snapshot = store_get_memory_snapshot(openclaw_dir, "team", team_id) or {}
        if team_snapshot and not str(next_policy.get("workingMemory") or "").strip():
            next_policy["workingMemory"] = str(team_snapshot.get("summary") or "").strip()
            next_policy["workingMemoryUpdatedAt"] = str(team_snapshot.get("updatedAt") or "").strip()
    return next_policy
