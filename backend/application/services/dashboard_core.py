from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys


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


DEFAULT_THEME_NAME = _DelegatedSymbol("DEFAULT_THEME_NAME")
SELECTABLE_THEME_CATALOG = _DelegatedSymbol("SELECTABLE_THEME_CATALOG")
TERMINAL_STATES = _DelegatedSymbol("TERMINAL_STATES")
THEME_CATALOG = _DelegatedSymbol("THEME_CATALOG")
THEME_STYLES = _DelegatedSymbol("THEME_STYLES")
agent_runtime_overrides = _DelegatedSymbol("agent_runtime_overrides")
artifact_deliverable_payload = _DelegatedSymbol("artifact_deliverable_payload")
backfill_planning_bundles = _DelegatedSymbol("backfill_planning_bundles")
backfill_task_intelligence = _DelegatedSymbol("backfill_task_intelligence")
build_admin_bootstrap_snapshot = _DelegatedSymbol("build_admin_bootstrap_snapshot")
build_agent_team_data = _DelegatedSymbol("build_agent_team_data")
build_agent_team_options_snapshot = _DelegatedSymbol("build_agent_team_options_snapshot")
build_chat_data = _DelegatedSymbol("build_chat_data")
build_chat_catalog_page_snapshot = _DelegatedSymbol("build_chat_catalog_page_snapshot")
build_communications_data = _DelegatedSymbol("build_communications_data")
build_customer_access_snapshot = _DelegatedSymbol("build_customer_access_snapshot")
build_dashboard_state_cached = _DelegatedSymbol("build_dashboard_state_cached")
build_orchestration_adjustment_review = _DelegatedSymbol("build_orchestration_adjustment_review")
build_orchestration_linked_review = _DelegatedSymbol("build_orchestration_linked_review")
build_orchestration_linked_suggestions = _DelegatedSymbol("build_orchestration_linked_suggestions")
build_orchestration_next_step_suggestions = _DelegatedSymbol("build_orchestration_next_step_suggestions")
build_orchestration_policy_trends = _DelegatedSymbol("build_orchestration_policy_trends")
build_orchestration_replay = _DelegatedSymbol("build_orchestration_replay")
build_orchestration_review_suggestions = _DelegatedSymbol("build_orchestration_review_suggestions")
build_orchestration_workflow_review = _DelegatedSymbol("build_orchestration_workflow_review")
build_external_api_reference = _DelegatedSymbol("build_external_api_reference")
build_management_automation_placeholder = _DelegatedSymbol("build_management_automation_placeholder")
build_label_maps = _DelegatedSymbol("build_label_maps")
build_management_data = _DelegatedSymbol("build_management_data")
build_management_runs_data = _DelegatedSymbol("build_management_runs_data")
build_orchestration_data = _DelegatedSymbol("build_orchestration_data")
build_decision_source_review = _DelegatedSymbol("build_decision_source_review")
build_operational_reports = _DelegatedSymbol("build_operational_reports")
build_platform_runtime_governance_summary = _DelegatedSymbol("build_platform_runtime_governance_summary")
build_recommendation_accuracy_review = _DelegatedSymbol("build_recommendation_accuracy_review")
build_routing_effectiveness_summary = _DelegatedSymbol("build_routing_effectiveness_summary")
build_task_intelligence_summary = _DelegatedSymbol("build_task_intelligence_summary")
build_task_replay = _DelegatedSymbol("build_task_replay")
build_team_collaboration_summary = _DelegatedSymbol("build_team_collaboration_summary")
build_team_ownership_payload = _DelegatedSymbol("build_team_ownership_payload")
cached_payload = _DelegatedSymbol("cached_payload")
cached_payload_background = _DelegatedSymbol("cached_payload_background")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_orchestration_replay_payload = _DelegatedSymbol("compact_orchestration_replay_payload")
compact_orchestration_routing_decision = _DelegatedSymbol("compact_orchestration_routing_decision")
compact_task_dashboard_payload = _DelegatedSymbol("compact_task_dashboard_payload")
compact_task_reference = _DelegatedSymbol("compact_task_reference")
compact_task_team_reference = _DelegatedSymbol("compact_task_team_reference")
CONVERSATION_SOURCE_LABELS = _DelegatedSymbol("CONVERSATION_SOURCE_LABELS")
compute_agent_health_data = _DelegatedSymbol("compute_agent_health_data")
compute_conversation_quality_data = _DelegatedSymbol("compute_conversation_quality_data")
current_agent_for_task = _DelegatedSymbol("current_agent_for_task")
default_orchestration_workflow = _DelegatedSymbol("default_orchestration_workflow")
enrich_task_team_ownership = _DelegatedSymbol("enrich_task_team_ownership")
ensure_default_agent_profile_bootstrap = _DelegatedSymbol("ensure_default_agent_profile_bootstrap")
ensure_default_management_bootstrap = _DelegatedSymbol("ensure_default_management_bootstrap")
ensure_default_memory_authority_seed = _DelegatedSymbol("ensure_default_memory_authority_seed")
ensure_default_memory_bootstrap = _DelegatedSymbol("ensure_default_memory_bootstrap")
ensure_default_skill_library_bootstrap = _DelegatedSymbol("ensure_default_skill_library_bootstrap")
ensure_default_team_mode = _DelegatedSymbol("ensure_default_team_mode")
format_age = _DelegatedSymbol("format_age")
get_router_agent_id = _DelegatedSymbol("get_router_agent_id")
hydrate_workflow_pack_context = _DelegatedSymbol("hydrate_workflow_pack_context")
hydrate_management_run_pack_context = _DelegatedSymbol("hydrate_management_run_pack_context")
hydrate_task_run_links = _DelegatedSymbol("hydrate_task_run_links")
infer_deliverable_type = _DelegatedSymbol("infer_deliverable_type")
is_managed_task_execution_thread = _DelegatedSymbol("is_managed_task_execution_thread")
is_merged_duplicate_task = _DelegatedSymbol("is_merged_duplicate_task")
latest_progress_event = _DelegatedSymbol("latest_progress_event")
load_agents = _DelegatedSymbol("load_agents")
load_audit_events = _DelegatedSymbol("load_audit_events")
load_automation_engine_status = _DelegatedSymbol("load_automation_engine_status")
load_config = _DelegatedSymbol("load_config")
load_context_hub_data = _DelegatedSymbol("load_context_hub_data")
load_conversation_catalog = _DelegatedSymbol("load_conversation_catalog")
load_kanban_config = _DelegatedSymbol("load_kanban_config")
load_openclaw_dashboard_summary = _DelegatedSymbol("load_openclaw_dashboard_summary")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
resolve_project_dir = _DelegatedSymbol("resolve_project_dir")
load_skills_catalog = _DelegatedSymbol("load_skills_catalog")
memory_system_status_payload = _DelegatedSymbol("memory_system_status_payload")
merge_tasks = _DelegatedSymbol("merge_tasks")
merged_agent_runtime_profile = _DelegatedSymbol("merged_agent_runtime_profile")
now_utc = _DelegatedSymbol("now_utc")
parse_iso = _DelegatedSymbol("parse_iso")
planning_binding_from_payload = _DelegatedSymbol("planning_binding_from_payload")
resolve_workflow_pack_record = _DelegatedSymbol("resolve_workflow_pack_record")
safe_list = _DelegatedSymbol("safe_list")
session_last_activity = _DelegatedSymbol("session_last_activity")
should_skip_management_automation_cycle = _DelegatedSymbol("should_skip_management_automation_cycle")
status_for_agent = _DelegatedSymbol("status_for_agent")
store_list_chat_threads = _DelegatedSymbol("store_list_chat_threads")
store_get_task_record = _DelegatedSymbol("store_get_task_record")
get_management_run_record = _DelegatedSymbol("get_management_run_record")
list_chat_threads_page = _DelegatedSymbol("list_chat_threads_page")
summarize_chat_threads = _DelegatedSymbol("summarize_chat_threads")
list_notification_channels = _DelegatedSymbol("list_notification_channels")
list_customer_access_channels = _DelegatedSymbol("list_customer_access_channels")
list_notification_deliveries = _DelegatedSymbol("list_notification_deliveries")
list_automation_alerts = _DelegatedSymbol("list_automation_alerts")
store_list_agent_teams = _DelegatedSymbol("store_list_agent_teams")
store_list_management_runs = _DelegatedSymbol("store_list_management_runs")
store_list_orchestration_workflow_versions = _DelegatedSymbol("store_list_orchestration_workflow_versions")
store_list_orchestration_workflows = _DelegatedSymbol("store_list_orchestration_workflows")
store_list_routing_decisions = _DelegatedSymbol("store_list_routing_decisions")
store_list_routing_policies = _DelegatedSymbol("store_list_routing_policies")
store_list_theme_workforce_profiles = _DelegatedSymbol("store_list_theme_workforce_profiles")
task_display_update = _DelegatedSymbol("task_display_update")
task_execution_meta_for_thread = _DelegatedSymbol("task_execution_meta_for_thread")
task_result_summary = _DelegatedSymbol("task_result_summary")
task_route = _DelegatedSymbol("task_route")
todo_summary = _DelegatedSymbol("todo_summary")
workflow_pack_map_from_skills_payload = _DelegatedSymbol("workflow_pack_map_from_skills_payload")
work_guard_for_agent = _DelegatedSymbol("work_guard_for_agent")
workspace_last_activity = _DelegatedSymbol("workspace_last_activity")
run_automation_engine_cycle = _DelegatedSymbol("run_automation_engine_cycle")


def _compact_text_preview(value, limit=180):
    text = str(value or "").strip()
    if len(text) <= max(int(limit or 0), 0):
        return text
    return f"{text[: max(int(limit or 0) - 1, 0)].rstrip()}…"


def _deliverable_type_from_path(path):
    suffix = str(Path(path).suffix or "").strip().lower()
    if suffix in {".md", ".markdown"}:
        return "markdown"
    if suffix in {".pdf"}:
        return "pdf"
    if suffix in {".doc", ".docx"}:
        return "document"
    if suffix in {".ppt", ".pptx"}:
        return "presentation"
    if suffix in {".xls", ".xlsx", ".csv"}:
        return "spreadsheet"
    if suffix in {".json"}:
        return "json"
    if suffix in {".html", ".htm"}:
        return "html"
    if suffix in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}:
        return "image"
    if suffix in {".txt"}:
        return "text"
    return "document"


def _deliverable_summary_from_file(path, limit=220):
    file_path = Path(path)
    suffix = str(file_path.suffix or "").strip().lower()
    if suffix not in {".md", ".markdown", ".txt", ".json", ".html", ".htm"}:
        return ""
    try:
        content = file_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    normalized = " ".join(str(content or "").strip().split())
    return _compact_text_preview(normalized, limit=limit)


def _deliverables_directory_candidates(openclaw_dir):
    config = load_config(openclaw_dir)
    openclaw_root = Path(openclaw_dir).expanduser().resolve()
    candidates = [openclaw_root / "deliverables"]
    resolved_project_dir = str(resolve_project_dir(openclaw_dir) or "").strip()
    if resolved_project_dir:
        candidates.append(Path(resolved_project_dir).expanduser().resolve() / "platform" / "deliverables")
    repo_deliverables_dir = Path(__file__).resolve().parents[3] / "platform" / "deliverables"
    candidates.append(repo_deliverables_dir)
    for agent in safe_list(load_agents(config)):
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        workspace = Path(agent.get("workspace") or (openclaw_root / f"workspace-{agent_id}")).expanduser().resolve()
        candidates.append(workspace / "deliverables")
    unique = []
    seen = set()
    for candidate in candidates:
        normalized = str(candidate.expanduser().resolve())
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(Path(normalized))
    return unique


def _deliverable_relative_path(file_path, deliverables_dir, openclaw_dir):
    openclaw_root = Path(openclaw_dir).expanduser().resolve()
    project_root = None
    resolved_project_dir = str(resolve_project_dir(openclaw_dir) or "").strip()
    if resolved_project_dir:
        project_root = Path(resolved_project_dir).expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[3]
    roots = [root for root in (openclaw_root, project_root, repo_root) if root]
    for root in roots:
        try:
            relative = file_path.relative_to(root)
            parts = relative.parts
            if len(parts) >= 2 and parts[0] == "platform" and parts[1] == "deliverables":
                return str(Path("deliverables", *parts[2:]))
            return str(relative)
        except ValueError:
            continue
    try:
        return str(file_path.relative_to(deliverables_dir.parent))
    except ValueError:
        return file_path.name


def _build_filesystem_deliverables(openclaw_dir, now=None):
    now = now or now_utc()
    deliverables = []
    for deliverables_dir in _deliverables_directory_candidates(openclaw_dir):
        if not deliverables_dir.exists() or not deliverables_dir.is_dir():
            continue
        for file_path in sorted(
            (path for path in deliverables_dir.rglob("*") if path.is_file()),
            key=lambda item: item.stat().st_mtime if item.exists() else 0,
            reverse=True,
        ):
            try:
                updated_dt = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                updated_dt = None
            relative_path = _deliverable_relative_path(file_path, deliverables_dir, openclaw_dir)
            deliverables.append(
                {
                    "id": f"file:{relative_path}",
                    "title": file_path.stem.replace("-", " ").replace("_", " ").strip() or file_path.name,
                    "state": "ready",
                    "status": "ready",
                    "statusLabel": "ready",
                    "owner": "",
                    "updatedAt": updated_dt.isoformat().replace("+00:00", "Z") if updated_dt else "",
                    "updatedAgo": format_age(updated_dt, now) if updated_dt else "",
                    "summary": _deliverable_summary_from_file(file_path),
                    "output": str(file_path),
                    "path": str(file_path),
                    "pathLabel": relative_path,
                    "sourceTask": "",
                    "sourceRun": "",
                    "type": _deliverable_type_from_path(file_path),
                    "artifact": False,
                    "file": True,
                    "source": "filesystem_deliverable",
                }
            )
    return deliverables


def _workflow_storage_files(openclaw_dir):
    storage_dir = Path(openclaw_dir).expanduser().resolve() / ".mission-control" / "workflows"
    return storage_dir / "workflows.json", storage_dir / "workflow_runs.json"


def _load_json_map(path):
    file_path = Path(path)
    if not file_path.exists():
        return {}
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _normalize_deliverable_text(value, fallback=""):
    normalized = str(value or "").strip()
    return normalized or str(fallback or "").strip()


def _build_workflow_run_deliverables(openclaw_dir, now=None):
    now = now or now_utc()
    workflows_file, runs_file = _workflow_storage_files(openclaw_dir)
    workflow_map = _load_json_map(workflows_file)
    runs_map = _load_json_map(runs_file)
    deliverables = []
    for run in sorted(
        runs_map.values() if isinstance(runs_map, dict) else [],
        key=lambda item: str((item or {}).get("completedAt") or (item or {}).get("startedAt") or ""),
        reverse=True,
    ):
        if not isinstance(run, dict):
            continue
        if _normalize_deliverable_text(run.get("status")).lower() != "success":
            continue
        workflow_id = _normalize_deliverable_text(run.get("workflowId"))
        workflow = workflow_map.get(workflow_id) if isinstance(workflow_map, dict) else {}
        workflow = workflow if isinstance(workflow, dict) else {}
        workflow_name = _normalize_deliverable_text(workflow.get("name"), "工作流")
        artifacts = run.get("artifacts") if isinstance(run.get("artifacts"), dict) else {}
        final = artifacts.get("final") if isinstance(artifacts.get("final"), dict) else {}
        records = safe_list(final.get("deliverableRecords"))
        updated_at = _normalize_deliverable_text(run.get("completedAt") or run.get("startedAt"))
        updated_dt = parse_iso(updated_at)
        output_dir_text = _normalize_deliverable_text(run.get("outputDir"))
        output_dir = Path(output_dir_text).expanduser() if output_dir_text else None
        for record in records:
            if not isinstance(record, dict):
                continue
            target_path_text = _normalize_deliverable_text(record.get("path"))
            target_path = Path(target_path_text).expanduser() if target_path_text else Path()
            if not str(target_path).strip() or not target_path.exists():
                continue
            relative_path = (
                _deliverable_relative_path(target_path, output_dir, openclaw_dir)
                if output_dir is not None
                else target_path.name
            )
            title = target_path.stem.replace("-", " ").replace("_", " ").strip() or target_path.name
            summary = _deliverable_summary_from_file(target_path)
            if not summary:
                summary = f"{workflow_name} · {title}"
            deliverables.append(
                {
                    "id": f"workflow:{workflow_id}:{_normalize_deliverable_text(run.get('id'))}:{target_path.name}",
                    "title": title,
                    "state": "ready",
                    "status": "ready",
                    "statusLabel": "ready",
                    "owner": workflow_name,
                    "updatedAt": updated_at,
                    "updatedAgo": format_age(updated_dt, now) if updated_dt else "",
                    "summary": summary,
                    "output": str(target_path),
                    "path": str(target_path),
                    "pathLabel": relative_path,
                    "sourceTask": "",
                    "sourceRun": _normalize_deliverable_text(run.get("id")),
                    "sourceWorkflow": workflow_id,
                    "sourceWorkflowName": workflow_name,
                    "type": _deliverable_type_from_path(target_path),
                    "artifact": True,
                    "file": True,
                    "source": "workflow_run_deliverable",
                }
            )
    return deliverables


def _merge_unique_deliverables(*sources):
    merged = []
    seen_ids = set()
    seen_paths = set()
    for source in sources:
        for item in safe_list(source):
            if not isinstance(item, dict):
                continue
            deliverable = deepcopy(item)
            deliverable_id = str(deliverable.get("id") or "").strip()
            deliverable_path = str(deliverable.get("path") or deliverable.get("output") or "").strip()
            dedupe_key = deliverable_id or deliverable_path
            if not dedupe_key:
                continue
            if deliverable_id and deliverable_id in seen_ids:
                continue
            if deliverable_path and deliverable_path in seen_paths:
                continue
            if deliverable_id:
                seen_ids.add(deliverable_id)
            if deliverable_path:
                seen_paths.add(deliverable_path)
            merged.append(deliverable)
    return merged


def _append_management_run_artifact_deliverables(openclaw_dir, deliverables, now=None):
    now = now or now_utc()
    merged = _merge_unique_deliverables(deliverables)
    known_ids = {str(item.get("id") or "").strip() for item in merged if str(item.get("id") or "").strip()}
    for run in safe_list(store_list_management_runs(openclaw_dir, limit=128)):
        if not isinstance(run, dict):
            continue
        artifacts = safe_list(run.get("artifacts"))
        if not artifacts:
            meta = run.get("meta") if isinstance(run.get("meta"), dict) else {}
            artifacts = safe_list(meta.get("artifacts"))
        for artifact in artifacts:
            deliverable = artifact_deliverable_payload(artifact, run, now=now)
            deliverable_id = str(deliverable.get("id") or "").strip()
            if not deliverable_id or deliverable_id in known_ids:
                continue
            known_ids.add(deliverable_id)
            merged.append(deliverable)
    return merged


def compact_workflow_pack_dashboard_payload(pack):
    pack = pack if isinstance(pack, dict) else {}
    return {
        "id": str(pack.get("id") or "").strip(),
        "name": str(pack.get("name") or pack.get("id") or "").strip(),
        "description": _compact_text_preview(pack.get("description"), 180),
        "status": str(pack.get("status") or "").strip(),
        "mode": str(pack.get("mode") or "").strip(),
        "defaultEntry": str(pack.get("defaultEntry") or "").strip(),
        "recommendedTeamId": str(pack.get("recommendedTeamId") or "").strip(),
        "requiredRuntimes": clean_unique_strings(pack.get("requiredRuntimes") or []),
        "skillCount": int(pack.get("skillCount") or 0),
        "resolvedSkillCount": int(pack.get("resolvedSkillCount") or 0),
        "missingSkillSlugs": clean_unique_strings(pack.get("missingSkillSlugs") or []),
        "incomplete": bool(pack.get("incomplete")),
        "hydrationStatus": str(pack.get("hydrationStatus") or "").strip(),
        "starter": bool(pack.get("starter")),
        "modeAliases": clean_unique_strings(pack.get("modeAliases") or []),
        "stageCount": len(safe_list(pack.get("stages"))),
    }


def compact_skill_catalog_dashboard_payload(skill):
    skill = skill if isinstance(skill, dict) else {}
    return {
        "slug": str(skill.get("slug") or skill.get("name") or "").strip(),
        "name": str(skill.get("name") or skill.get("displayName") or skill.get("slug") or "").strip(),
        "displayName": str(skill.get("displayName") or skill.get("name") or skill.get("slug") or "").strip(),
        "description": _compact_text_preview(skill.get("description"), 200),
        "categoryLabel": str(skill.get("categoryLabel") or "").strip(),
        "relativePath": str(skill.get("relativePath") or skill.get("path") or "").strip(),
        "rootKind": str(skill.get("rootKind") or "").strip(),
        "status": str(skill.get("status") or "").strip(),
        "mode": str(skill.get("mode") or "").strip(),
        "stage": str(skill.get("stage") or "").strip(),
        "requiresRuntime": clean_unique_strings(skill.get("requiresRuntime") or []),
        "packIds": clean_unique_strings(skill.get("packIds") or []),
        "roleProfileSource": str(skill.get("roleProfileSource") or "").strip(),
        "installedInOpenClaw": bool(skill.get("installedInOpenClaw")),
        "publishedToOpenClaw": bool(skill.get("publishedToOpenClaw")),
        "eligible": skill.get("eligible"),
        "bundled": bool(skill.get("bundled")),
        "source": str(skill.get("source") or "").strip(),
        "missing": deepcopy(skill.get("missing") if isinstance(skill.get("missing"), dict) else {}),
    }


def compact_skills_payload_for_dashboard(skills_data):
    skills = skills_data if isinstance(skills_data, dict) else {}
    return {
        "supported": bool(skills.get("supported")),
        "error": str(skills.get("error") or "").strip(),
        "generatedAt": str(skills.get("generatedAt") or "").strip(),
        "summary": deepcopy(skills.get("summary") if isinstance(skills.get("summary"), dict) else {}),
        "packSummary": deepcopy(skills.get("packSummary") if isinstance(skills.get("packSummary"), dict) else {}),
        "roleSummary": deepcopy(skills.get("roleSummary") if isinstance(skills.get("roleSummary"), dict) else {}),
        "stageSummary": deepcopy(skills.get("stageSummary") if isinstance(skills.get("stageSummary"), dict) else {}),
        "runtimeSummary": deepcopy(skills.get("runtimeSummary") if isinstance(skills.get("runtimeSummary"), dict) else {}),
        "packs": [
            compact_workflow_pack_dashboard_payload(item)
            for item in safe_list(skills.get("packs"))
        ],
        "skills": [
            compact_skill_catalog_dashboard_payload(item)
            for item in safe_list(skills.get("skills"))
        ],
    }


def _skill_installability_payload(skill, project_dir, managed_skills_dir):
    skill = skill if isinstance(skill, dict) else {}
    slug = str(skill.get("slug") or "").strip()
    package = skill.get("package") if isinstance(skill.get("package"), dict) else {}
    package_path = str(package.get("path") or "").strip()
    project_skill_path = ""
    vendored_skill_path = ""
    managed_skill_path = ""
    if project_dir and slug:
        project_skill_path = str((project_dir / "platform" / "skills" / slug / "SKILL.md").resolve())
        vendored_skill_path = str((project_dir / "platform" / "vendor" / "openclaw-skills" / slug / "SKILL.md").resolve())
    if managed_skills_dir and slug:
        managed_skill_path = str((managed_skills_dir / slug / "SKILL.md").resolve())

    project_exists = bool(project_skill_path and Path(project_skill_path).exists())
    vendored_exists = bool(vendored_skill_path and Path(vendored_skill_path).exists())
    if project_exists:
        source_kind = "project"
        source_path = project_skill_path
    elif vendored_exists:
        source_kind = "vendored"
        source_path = vendored_skill_path
    else:
        source_kind = ""
        source_path = ""

    return {
        "packageReady": bool(package.get("exists")),
        "packagePath": package_path,
        "canInstall": bool(source_kind),
        "sourceKind": source_kind,
        "sourcePath": source_path,
        "managedPath": managed_skill_path,
    }


def _skill_diagnostics_payload(skill):
    skill = skill if isinstance(skill, dict) else {}
    issues = safe_list(skill.get("issues"))
    errors = [str(item.get("message") or "").strip() for item in issues if str(item.get("kind") or "").strip() == "error" and str(item.get("message") or "").strip()]
    warnings = [str(item.get("message") or "").strip() for item in issues if str(item.get("kind") or "").strip() == "warning" and str(item.get("message") or "").strip()]
    missing = skill.get("missing") if isinstance(skill.get("missing"), dict) else {}
    missing_bins = clean_unique_strings(missing.get("bins") or [])
    missing_env = clean_unique_strings(missing.get("env") or [])
    missing_config = clean_unique_strings(missing.get("config") or [])
    missing_count = len(missing_bins) + len(missing_env) + len(missing_config)
    return {
        "errorCount": len(errors),
        "warningCount": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "missingBins": missing_bins,
        "missingEnv": missing_env,
        "missingConfig": missing_config,
        "missingCount": missing_count,
        "eligible": skill.get("eligible"),
    }


def _skill_setup_checklist(skill, installability, diagnostics):
    skill = skill if isinstance(skill, dict) else {}
    installability = installability if isinstance(installability, dict) else {}
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    published = bool(skill.get("publishedToOpenClaw"))
    installed = bool(skill.get("installedInOpenClaw"))
    requirements_ready = (
        diagnostics.get("eligible") is not False
        and int(diagnostics.get("errorCount") or 0) == 0
        and int(diagnostics.get("missingCount") or 0) == 0
    )
    checklist = [
        {
            "key": "install",
            "done": installed,
            "blocking": True,
            "detail": installability.get("managedPath") or "",
        },
        {
            "key": "requirements",
            "done": requirements_ready,
            "blocking": True,
            "detail": {
                "errors": int(diagnostics.get("errorCount") or 0),
                "warnings": int(diagnostics.get("warningCount") or 0),
                "missing": int(diagnostics.get("missingCount") or 0),
            },
        },
        {
            "key": "package",
            "done": bool(installability.get("packageReady")),
            "blocking": False,
            "detail": installability.get("packagePath") or "",
        },
    ]
    if published != installed:
        checklist.insert(
            1,
            {
                "key": "publish",
                "done": published,
                "blocking": True,
                "detail": installability.get("managedPath") or "",
            },
        )
    return checklist


def _skill_next_actions(skill, installability, diagnostics):
    skill = skill if isinstance(skill, dict) else {}
    installability = installability if isinstance(installability, dict) else {}
    diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    actions = []
    if int(diagnostics.get("errorCount") or 0) > 0:
        actions.append("fix-validation")
    if int(diagnostics.get("missingCount") or 0) > 0 or diagnostics.get("eligible") is False:
        actions.append("configure-requirements")
    if not bool(skill.get("installedInOpenClaw")) and bool(installability.get("canInstall")):
        actions.append("install")
    if not bool(installability.get("packageReady")):
        actions.append("package")
    if bool(skill.get("installedInOpenClaw")) and not bool(skill.get("publishedToOpenClaw")):
        actions.append("publish")
    return clean_unique_strings(actions)


def enrich_skills_installation_state(skills_data, openclaw_dir, metadata=None):
    skills = deepcopy(skills_data if isinstance(skills_data, dict) else {})
    project_metadata = metadata if isinstance(metadata, dict) else {}
    project_dir_raw = str(skills.get("projectDir") or "").strip()
    project_dir = Path(project_dir_raw).expanduser().resolve() if project_dir_raw else None
    managed_skills_root = str(Path(openclaw_dir).expanduser().resolve() / "skills")
    managed_skills_dir = Path(managed_skills_root).expanduser() if managed_skills_root else None
    published_count = 0
    installed_count = 0
    for skill in safe_list(skills.get("skills")):
        managed_skill_path = managed_skills_dir / skill.get("slug", "") if managed_skills_dir else None
        installed_in_openclaw = bool(
            managed_skill_path and (managed_skill_path / "SKILL.md").exists()
        )
        published = installed_in_openclaw
        skill["installedInOpenClaw"] = installed_in_openclaw
        skill["publishedToOpenClaw"] = published
        installability = _skill_installability_payload(skill, project_dir, managed_skills_dir)
        diagnostics = _skill_diagnostics_payload(skill)
        skill["installability"] = installability
        skill["diagnostics"] = diagnostics
        skill["setupChecklist"] = _skill_setup_checklist(skill, installability, diagnostics)
        skill["nextActions"] = _skill_next_actions(skill, installability, diagnostics)
        if published:
            published_count += 1
        if installed_in_openclaw:
            installed_count += 1
    if isinstance(skills.get("summary"), dict):
        skills["summary"]["publishedToOpenClaw"] = published_count
        skills["summary"]["installedToOpenClaw"] = installed_count
        skills["summary"]["defaultInstalledSkills"] = clean_unique_strings((project_metadata.get("defaultInstalledSkills") or []))
    return skills


def compact_management_run_dashboard_payload(run):
    run = run if isinstance(run, dict) else {}
    linked_pack = run.get("linkedPack") if isinstance(run.get("linkedPack"), dict) else {}
    linked_task = run.get("linkedTask") if isinstance(run.get("linkedTask"), dict) else {}
    linked_session = run.get("linkedSession") if isinstance(run.get("linkedSession"), dict) else {}
    deliverable = run.get("deliverable") if isinstance(run.get("deliverable"), dict) else {}
    linked_team = run.get("linkedTeam") if isinstance(run.get("linkedTeam"), dict) else {}
    workflow_binding = run.get("workflowBinding") if isinstance(run.get("workflowBinding"), dict) else {}
    planning_binding = run.get("planningBinding") if isinstance(run.get("planningBinding"), dict) else {}
    team_ownership = run.get("teamOwnership") if isinstance(run.get("teamOwnership"), dict) else {}
    gate_summary = run.get("gateSummary") if isinstance(run.get("gateSummary"), dict) else {}
    progress = run.get("progress")
    if progress is None:
        stage_key = str(run.get("stageKey") or "").strip().lower()
        status = str(run.get("status") or "").strip().lower()
        if status in {"complete", "done", "resolved", "closed"} or stage_key == "release":
            progress = 100
        elif stage_key == "verification":
            progress = 84
        elif stage_key == "execution":
            progress = 58
        elif stage_key == "planning":
            progress = 28
        else:
            progress = 12
    return {
        "id": str(run.get("id") or "").strip(),
        "title": str(run.get("title") or run.get("id") or "").strip(),
        "status": str(run.get("status") or "").strip(),
        "stage": str(run.get("stageKey") or run.get("stageLabel") or "").strip(),
        "stageKey": str(run.get("stageKey") or "").strip(),
        "stageLabel": str(run.get("stageLabel") or run.get("stageKey") or "").strip(),
        "riskLevel": str(run.get("riskLevel") or "").strip(),
        "goal": str(run.get("goal") or "").strip(),
        "summary": str(run.get("summary") or run.get("note") or linked_task.get("currentUpdate") or "").strip(),
        "progress": int(progress or 0),
        "updatedAt": str(run.get("updatedAt") or "").strip(),
        "updatedAgo": str(run.get("updatedAgo") or "").strip(),
        "createdAt": str(run.get("createdAt") or "").strip(),
        "createdAgo": str(run.get("createdAgo") or "").strip(),
        "linkedTaskId": str(run.get("linkedTaskId") or linked_task.get("id") or "").strip(),
        "taskCount": int(run.get("taskCount") or len(safe_list(run.get("tasks"))) or (1 if linked_task.get("id") else 0)),
        "conversationCount": int(run.get("conversationCount") or len(safe_list(run.get("conversations"))) or (1 if linked_session.get("key") else 0)),
        "deliverableCount": int(run.get("deliverableCount") or len(safe_list(run.get("deliverables"))) or (1 if deliverable.get("id") else 0)),
        "linkedPackId": str(run.get("linkedPackId") or linked_pack.get("id") or "").strip(),
        "linkedPack": {
            "id": str(linked_pack.get("id") or "").strip(),
            "name": str(linked_pack.get("name") or linked_pack.get("id") or "").strip(),
            "description": _compact_text_preview(linked_pack.get("description"), 160),
            "mode": str(linked_pack.get("mode") or "").strip(),
            "defaultEntry": str(linked_pack.get("defaultEntry") or "").strip(),
            "recommendedTeamId": str(linked_pack.get("recommendedTeamId") or "").strip(),
            "requiredRuntimes": clean_unique_strings(linked_pack.get("requiredRuntimes") or []),
            "stageCount": int(linked_pack.get("stageCount") or len(safe_list(linked_pack.get("stages")))),
        } if linked_pack else {},
        "linkedTask": deepcopy(linked_task),
        "linkedSession": {
            "key": str(linked_session.get("key") or "").strip(),
            "label": str(linked_session.get("label") or linked_session.get("sessionId") or "").strip(),
            "agentId": str(linked_session.get("agentId") or "").strip(),
            "agentLabel": str(linked_session.get("agentLabel") or linked_session.get("agentId") or "").strip(),
            "preview": _compact_text_preview(linked_session.get("preview"), 160),
            "updatedAt": str(linked_session.get("updatedAt") or "").strip(),
            "updatedAgo": str(linked_session.get("updatedAgo") or "").strip(),
        } if linked_session else {},
        "deliverable": deepcopy(deliverable),
        "linkedTeam": deepcopy(linked_team),
        "teamOwnership": deepcopy(team_ownership),
        "workflowBinding": {
            "workflowId": str(workflow_binding.get("workflowId") or "").strip(),
            "workflowName": str(workflow_binding.get("workflowName") or "").strip(),
            "workflowVersionId": str(workflow_binding.get("workflowVersionId") or "").strip(),
            "workflowVersionNumber": workflow_binding.get("workflowVersionNumber"),
        } if workflow_binding else {},
        "planningBinding": {
            "bundleId": str(planning_binding.get("bundleId") or "").strip(),
            "relativeDir": str(planning_binding.get("relativeDir") or planning_binding.get("bundleDir") or "").strip(),
            "currentPhase": str(planning_binding.get("currentPhase") or "").strip(),
            "progressPercent": planning_binding.get("progressPercent"),
        } if planning_binding else {},
        "gateSummary": deepcopy(gate_summary),
        "stages": deepcopy(safe_list(run.get("stages"))),
        "alerts": [
            {
                "severity": str(item.get("severity") or "").strip(),
                "title": str(item.get("title") or item.get("message") or "").strip(),
            }
            for item in safe_list(run.get("alerts"))[:3]
            if isinstance(item, dict)
        ],
    }


def compact_management_run_overview_payload(run):
    run = run if isinstance(run, dict) else {}
    linked_task = run.get("linkedTask") if isinstance(run.get("linkedTask"), dict) else {}
    linked_team = run.get("linkedTeam") if isinstance(run.get("linkedTeam"), dict) else {}
    workflow_binding = run.get("workflowBinding") if isinstance(run.get("workflowBinding"), dict) else {}
    team_ownership = run.get("teamOwnership") if isinstance(run.get("teamOwnership"), dict) else {}
    progress = run.get("progress")
    if progress is None:
        stage_key = str(run.get("stageKey") or "").strip().lower()
        status = str(run.get("status") or "").strip().lower()
        if status in {"complete", "done", "resolved", "closed"} or stage_key == "release":
            progress = 100
        elif stage_key == "verification":
            progress = 84
        elif stage_key == "execution":
            progress = 58
        elif stage_key == "planning":
            progress = 28
        else:
            progress = 12
    return {
        "id": str(run.get("id") or "").strip(),
        "title": str(run.get("title") or run.get("id") or "").strip(),
        "status": str(run.get("status") or "").strip(),
        "stage": str(run.get("stageKey") or run.get("stageLabel") or "").strip(),
        "stageKey": str(run.get("stageKey") or "").strip(),
        "stageLabel": str(run.get("stageLabel") or run.get("stageKey") or "").strip(),
        "riskLevel": str(run.get("riskLevel") or "").strip(),
        "goal": str(run.get("goal") or "").strip(),
        "summary": str(run.get("summary") or run.get("note") or linked_task.get("currentUpdate") or "").strip(),
        "progress": int(progress or 0),
        "updatedAt": str(run.get("updatedAt") or "").strip(),
        "updatedAgo": str(run.get("updatedAgo") or "").strip(),
        "createdAt": str(run.get("createdAt") or "").strip(),
        "createdAgo": str(run.get("createdAgo") or "").strip(),
        "linkedTaskId": str(run.get("linkedTaskId") or linked_task.get("id") or "").strip(),
        "linkedTask": deepcopy(linked_task),
        "linkedTeam": deepcopy(linked_team),
        "teamOwnership": deepcopy(team_ownership),
        "workflowBinding": {
            "workflowId": str(workflow_binding.get("workflowId") or "").strip(),
            "workflowName": str(workflow_binding.get("workflowName") or "").strip(),
            "workflowVersionId": str(workflow_binding.get("workflowVersionId") or "").strip(),
            "workflowVersionNumber": workflow_binding.get("workflowVersionNumber"),
        } if workflow_binding else {},
    }


def compact_management_payload_for_dashboard(management_data):
    management = management_data if isinstance(management_data, dict) else {}
    agent_health = management.get("agentHealth") if isinstance(management.get("agentHealth"), dict) else {}
    conversation_quality = management.get("conversationQuality") if isinstance(management.get("conversationQuality"), dict) else {}
    reports = management.get("reports") if isinstance(management.get("reports"), dict) else {}
    automation = management.get("automation") if isinstance(management.get("automation"), dict) else {}
    recommendation_review = management.get("recommendationReview") if isinstance(management.get("recommendationReview"), dict) else {}
    return {
        "summary": deepcopy(management.get("summary") if isinstance(management.get("summary"), dict) else {}),
        "runs": [
            compact_management_run_overview_payload(item)
            for item in safe_list(management.get("runs"))
        ],
        "agentHealth": {
            "summary": deepcopy(agent_health.get("summary") if isinstance(agent_health.get("summary"), dict) else {}),
        },
        "conversationQuality": {
            "summary": deepcopy(conversation_quality.get("summary") if isinstance(conversation_quality.get("summary"), dict) else {}),
        },
        "reports": {
            "weekly": deepcopy(reports.get("weekly") if isinstance(reports.get("weekly"), dict) else {}),
        },
        "automation": {
            "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
            "mode": deepcopy(automation.get("mode") if isinstance(automation.get("mode"), dict) else {}),
        },
        "recommendationReview": {
            "score": recommendation_review.get("score", 0),
            "summary": deepcopy(recommendation_review.get("summary") if isinstance(recommendation_review.get("summary"), dict) else {}),
            "bundleOperatingSummary": deepcopy(
                recommendation_review.get("bundleOperatingSummary")
                if isinstance(recommendation_review.get("bundleOperatingSummary"), dict)
                else {}
            ),
            "bundlePriorityQueue": deepcopy(safe_list(recommendation_review.get("bundlePriorityQueue"))[:3]),
        },
    }


def _compact_management_alert_preview(alert):
    alert = alert if isinstance(alert, dict) else {}
    meta = alert.get("meta") if isinstance(alert.get("meta"), dict) else {}
    return {
        "id": str(alert.get("id") or "").strip(),
        "status": str(alert.get("status") or "").strip(),
        "severity": str(alert.get("severity") or "warning").strip(),
        "title": str(alert.get("title") or alert.get("message") or "").strip(),
        "detail": _compact_text_preview(alert.get("detail") or alert.get("ruleName") or "", 160),
        "sourceId": str(alert.get("sourceId") or "").strip(),
        "triggerType": str(alert.get("triggerType") or "").strip(),
        "updatedAt": str(alert.get("updatedAt") or alert.get("lastTriggeredAt") or "").strip(),
        "updatedAgo": str(alert.get("updatedAgo") or alert.get("lastTriggeredAgo") or "").strip(),
        "ruleName": str(alert.get("ruleName") or "").strip(),
        "meta": {
            "teamId": str(meta.get("teamId") or "").strip(),
            "ageMinutes": meta.get("ageMinutes"),
            "supervisionCount": meta.get("supervisionCount"),
        },
    }


def compact_openclaw_bootstrap_payload(openclaw_data):
    openclaw = openclaw_data if isinstance(openclaw_data, dict) else {}
    runtime_sync = openclaw.get("runtimeSync") if isinstance(openclaw.get("runtimeSync"), dict) else {}
    runtime_auth = runtime_sync.get("auth") if isinstance(runtime_sync.get("auth"), dict) else {}
    installation = openclaw.get("installation") if isinstance(openclaw.get("installation"), dict) else {}
    version = installation.get("version") if isinstance(installation.get("version"), dict) else {}
    execution_architecture = openclaw.get("executionArchitecture") if isinstance(openclaw.get("executionArchitecture"), dict) else {}
    provider_path = execution_architecture.get("providerPath") if isinstance(execution_architecture.get("providerPath"), dict) else {}
    local_runtime_path = execution_architecture.get("localRuntimePath") if isinstance(execution_architecture.get("localRuntimePath"), dict) else {}
    execution_policy = execution_architecture.get("executionPolicy") if isinstance(execution_architecture.get("executionPolicy"), dict) else {}
    decision = execution_architecture.get("decision") if isinstance(execution_architecture.get("decision"), dict) else {}
    provider_budget_profile = (
        provider_path.get("hostedProviderContextBudgetProfile")
        if isinstance(provider_path.get("hostedProviderContextBudgetProfile"), dict)
        else {}
    )
    local_reference = (
        local_runtime_path.get("referenceAlgorithm")
        if isinstance(local_runtime_path.get("referenceAlgorithm"), dict)
        else {}
    )
    return {
        "deferred": bool(openclaw.get("deferred")),
        "installation": {
            "installed": bool(installation.get("installed")),
            "managed": bool(installation.get("managed")),
            "label": str(installation.get("label") or "").strip(),
            "version": {
                "release": str(version.get("release") or "").strip(),
                "raw": str(version.get("raw") or "").strip(),
            },
        },
        "runtimeSync": {
            "lastSyncedAt": str(runtime_sync.get("lastSyncedAt") or "").strip(),
            "lastSyncedAgo": str(runtime_sync.get("lastSyncedAgo") or "").strip(),
            "auth": {
                "ok": bool(runtime_auth.get("ok")),
                "state": str(runtime_auth.get("state") or "").strip(),
                "readyCount": runtime_auth.get("readyCount") or 0,
                "targetCount": runtime_auth.get("targetCount") or 0,
                "error": str(runtime_auth.get("error") or "").strip(),
            },
        },
        "executionArchitecture": {
            "primaryPath": str(execution_architecture.get("primaryPath") or "").strip(),
            "fallbackPath": str(execution_architecture.get("fallbackPath") or "").strip(),
            "openclawRole": str(execution_architecture.get("openclawRole") or "").strip(),
            "decision": {
                "summary": str(decision.get("summary") or "").strip(),
                "reason": str(decision.get("reason") or "").strip(),
            },
            "executionPolicy": {
                "hostedProviderContextBudgetPolicy": str(
                    execution_policy.get("hostedProviderContextBudgetPolicy")
                    or provider_path.get("hostedProviderContextBudgetPolicy")
                    or ""
                ).strip(),
                "hostedProviderContextBudgetProfile": {
                    "policy": str(provider_budget_profile.get("policy") or "").strip(),
                    "label": str(provider_budget_profile.get("label") or "").strip(),
                    "summary": str(provider_budget_profile.get("summary") or "").strip(),
                    "appliesTo": deepcopy(provider_budget_profile.get("appliesTo") if isinstance(provider_budget_profile.get("appliesTo"), list) else []),
                },
            },
            "providerPath": {
                "ready": bool(provider_path.get("ready")),
                "readyProviderCount": provider_path.get("readyProviderCount") or 0,
                "routerProviderLabel": str(provider_path.get("routerProviderLabel") or "").strip(),
                "routerModel": str(provider_path.get("routerModel") or "").strip(),
                "preferredProviderLabel": str(provider_path.get("preferredProviderLabel") or "").strip(),
                "summary": str(provider_path.get("summary") or "").strip(),
                "providerFamilies": [
                    {
                        "id": str(item.get("id") or "").strip(),
                        "label": str(item.get("label") or item.get("id") or "").strip(),
                        "configured": bool(item.get("configured")),
                        "active": bool(item.get("active")),
                        "router": bool(item.get("router")),
                        "agentCount": item.get("agentCount") or 0,
                    }
                    for item in safe_list(provider_path.get("providerFamilies"))
                    if isinstance(item, dict) and str(item.get("label") or item.get("id") or "").strip()
                ],
                "activeModelFamilies": [
                    {
                        "id": str(item.get("id") or "").strip(),
                        "label": str(item.get("label") or item.get("id") or "").strip(),
                        "configured": bool(item.get("configured")),
                        "active": bool(item.get("active")),
                        "router": bool(item.get("router")),
                        "agentCount": item.get("agentCount") or 0,
                    }
                    for item in safe_list(provider_path.get("activeModelFamilies"))
                    if isinstance(item, dict) and str(item.get("label") or item.get("id") or "").strip()
                ],
                "hostedProviderContextBudgetPolicy": str(provider_path.get("hostedProviderContextBudgetPolicy") or "").strip(),
                "hostedProviderContextBudgetProfile": {
                    "policy": str(provider_budget_profile.get("policy") or "").strip(),
                    "label": str(provider_budget_profile.get("label") or "").strip(),
                    "summary": str(provider_budget_profile.get("summary") or "").strip(),
                },
                "modelAccessMatrix": [],
                "modelAccessMatrixSummary": deepcopy(
                    provider_path.get("modelAccessMatrixSummary")
                    if isinstance(provider_path.get("modelAccessMatrixSummary"), dict)
                    else {}
                ),
            },
            "localRuntimePath": {
                "configured": bool(local_runtime_path.get("configured")),
                "running": bool(local_runtime_path.get("running")),
                "backend": str(local_runtime_path.get("backend") or "").strip(),
                "summary": str(local_runtime_path.get("summary") or "").strip(),
                "healthOk": bool(local_runtime_path.get("healthOk")),
                "socketReachable": bool(local_runtime_path.get("socketReachable")),
                "httpResponding": bool(local_runtime_path.get("httpResponding")),
                "turboQuantEligible": bool(local_runtime_path.get("turboQuantEligible")),
                "turboQuantActive": bool(local_runtime_path.get("turboQuantActive")),
                "turboQuantVerified": bool(local_runtime_path.get("turboQuantVerified")),
                "referenceAlgorithmBundled": bool(local_runtime_path.get("referenceAlgorithmBundled")),
                "referenceAlgorithmAvailable": bool(local_runtime_path.get("referenceAlgorithmAvailable")),
                "referenceAlgorithmRoundTripVerified": bool(local_runtime_path.get("referenceAlgorithmRoundTripVerified")),
                "referenceAlgorithm": {
                    "library": str(local_reference.get("library") or "").strip(),
                    "license": str(local_reference.get("license") or "").strip(),
                    "variantIds": deepcopy(local_reference.get("variantIds") if isinstance(local_reference.get("variantIds"), list) else []),
                },
            },
        },
    }


def compact_management_bootstrap_payload(management_data):
    management = management_data if isinstance(management_data, dict) else {}
    return {
        "summary": deepcopy(management.get("summary") if isinstance(management.get("summary"), dict) else {}),
    }


def build_management_summary_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    runs = safe_list(store_list_management_runs(openclaw_dir, limit=48))
    stage_counter = Counter()
    status_counter = Counter()
    risk_counter = Counter()
    total = 0
    active = 0
    blocked = 0
    ready_for_release = 0
    completed = 0
    planned_runs = 0
    team_runs = 0
    for run in runs:
        if not isinstance(run, dict):
            continue
        total += 1
        stage_key = str(run.get("stageKey") or "unknown").strip() or "unknown"
        status = str(run.get("status") or "active").strip() or "active"
        risk_level = str(run.get("riskLevel") or "medium").strip() or "medium"
        stage_counter[stage_key] += 1
        status_counter[status] += 1
        risk_counter[risk_level] += 1
        if status == "active":
            active += 1
        if status == "blocked":
            blocked += 1
        if status == "complete":
            completed += 1
        if stage_key == "release" and status != "complete":
            ready_for_release += 1
        if run.get("planningBinding"):
            planned_runs += 1
        if str(run.get("linkedTeamId") or "").strip():
            team_runs += 1
    return compact_management_bootstrap_payload(
        {
            "summary": {
                "total": total,
                "active": active,
                "blocked": blocked,
                "readyForRelease": ready_for_release,
                "completed": completed,
                "plannedRuns": planned_runs,
                "teamRuns": team_runs,
                "statusBreakdown": dict(status_counter),
                "stageBreakdown": dict(stage_counter),
                "riskBreakdown": dict(risk_counter),
            },
        }
    )


def _build_management_run_reference_maps(openclaw_dir, limit=48):
    openclaw_dir = Path(openclaw_dir)

    def build():
        runs = [
            item
            for item in safe_list(store_list_management_runs(openclaw_dir, limit=limit))
            if isinstance(item, dict)
        ]
        linked_task_ids = {
            str(item.get("linkedTaskId") or "").strip()
            for item in runs
            if str(item.get("linkedTaskId") or "").strip()
        }
        linked_team_ids = {
            str(item.get("linkedTeamId") or "").strip()
            for item in runs
            if str(item.get("linkedTeamId") or "").strip()
        }
        task_map = {}
        task_limit = max(240, len(linked_task_ids) * 4)
        for item in _svc().safe_list(_svc().store_list_task_records(openclaw_dir, limit=task_limit)):
            if not isinstance(item, dict) or is_merged_duplicate_task(item):
                continue
            task_id = str(item.get("id") or "").strip()
            if not task_id or task_id not in linked_task_ids:
                continue
            task_map[task_id] = {
                "id": task_id,
                "title": str(item.get("title") or task_id).strip(),
                "currentUpdate": str(item.get("currentUpdate") or item.get("resultSummary") or "").strip(),
            }
        for task_id in linked_task_ids:
            if task_id in task_map:
                continue
            item = _svc().store_get_task_record(openclaw_dir, task_id)
            if not isinstance(item, dict):
                continue
            task_map[task_id] = {
                "id": task_id,
                "title": str(item.get("title") or task_id).strip(),
                "currentUpdate": str(item.get("currentUpdate") or item.get("resultSummary") or "").strip(),
            }
        team_map = {
            str(item.get("id") or "").strip(): {
                "id": str(item.get("id") or "").strip(),
                "name": str(item.get("name") or item.get("id") or "").strip(),
            }
            for item in safe_list(store_list_agent_teams(openclaw_dir))
            if str(item.get("id") or "").strip() and str(item.get("id") or "").strip() in linked_team_ids
        }
        return {
            "taskMap": task_map,
            "teamMap": team_map,
        }

    return cached_payload(
        ("management-direct-run-refs", str(openclaw_dir), str(limit)),
        5.0,
        build,
    )


def _build_management_run_rows_snapshot(openclaw_dir, config=None, now=None, limit=48):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        references = _build_management_run_reference_maps(openclaw_dir, limit=limit)
        task_map = references.get("taskMap") if isinstance(references.get("taskMap"), dict) else {}
        team_map = references.get("teamMap") if isinstance(references.get("teamMap"), dict) else {}
        rows = []
        for item in safe_list(store_list_management_runs(openclaw_dir, limit=limit)):
            if not isinstance(item, dict):
                continue
            linked_task_id = str(item.get("linkedTaskId") or "").strip()
            linked_team_id = str(item.get("linkedTeamId") or "").strip()
            linked_task = task_map.get(linked_task_id) if isinstance(task_map.get(linked_task_id), dict) else {}
            linked_team = team_map.get(linked_team_id) if isinstance(team_map.get(linked_team_id), dict) else {}
            rows.append(
                compact_management_run_overview_payload(
                    {
                        **item,
                        "updatedAgo": format_age(parse_iso(item.get("updatedAt")), now),
                        "createdAgo": format_age(parse_iso(item.get("createdAt")), now),
                        "stageLabel": (
                            next(
                                (
                                    stage.get("title")
                                    for stage in safe_list(item.get("stages"))
                                    if isinstance(stage, dict) and stage.get("key") == item.get("stageKey")
                                ),
                                "",
                            )
                            or item.get("stageKey", "")
                        ),
                        "linkedTask": linked_task,
                        "linkedTeam": linked_team,
                        "workflowBinding": item.get("workflowBinding") if isinstance(item.get("workflowBinding"), dict) else {},
                        "teamOwnership": build_team_ownership_payload(
                            team_map,
                            execution_team_id=linked_team_id,
                            recommended_team_id="",
                            mode="",
                            source="run",
                        ),
                    }
                )
            )
        return rows

    return cached_payload(
        ("management-direct-runs", str(openclaw_dir), str(limit)),
        5.0,
        build,
    )


def build_management_recommendations_preview_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    task_index = safe_list(task_snapshot.get("taskIndex"))
    recommendation_review = build_recommendation_accuracy_review(openclaw_dir, task_index, now)
    return {
        "recommendationReview": {
            "bundleOperatingSummary": deepcopy(
                recommendation_review.get("bundleOperatingSummary")
                if isinstance(recommendation_review.get("bundleOperatingSummary"), dict)
                else {}
            ),
            "bundlePriorityQueue": deepcopy(safe_list(recommendation_review.get("bundlePriorityQueue"))[:3]),
        },
    }


def _load_dashboard_conversation_catalog(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    agents = safe_list(load_agents(config))
    if not agents:
        agents = _fallback_runtime_agents_from_teams(openclaw_dir, config=config)
    agent_labels, _label_to_agent_ids = build_label_maps(agents, kanban_cfg, router_agent_id)
    return load_conversation_catalog(openclaw_dir, config, agent_labels)


def _fallback_runtime_agents_from_teams(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    overrides = agent_runtime_overrides(metadata)
    svc = _svc()
    try:
        env_builder = getattr(svc, "openclaw_command_env", None)
        command_runner = getattr(svc, "run_command", None)
        json_parser = getattr(svc, "parse_json_payload", None)
        if callable(env_builder) and callable(command_runner) and callable(json_parser):
            result = command_runner(["openclaw", "agents", "list", "--json"], env=env_builder(openclaw_dir))
            payload = json_parser(result.stdout, result.stderr, default=[])
            if isinstance(payload, list):
                runtime_agents = []
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    agent_id = str(item.get("id") or "").strip()
                    if not agent_id:
                        continue
                    identity_name = (
                        str(item.get("identityName") or "").strip()
                        or str(item.get("name") or "").strip()
                        or agent_id
                    )
                    runtime_agents.append(
                        {
                            "id": agent_id,
                            "model": "default",
                            "identity": {"name": identity_name},
                        }
                    )
                if runtime_agents:
                    return runtime_agents
    except Exception:
        pass
    seen = []
    for team in safe_list(store_list_agent_teams(openclaw_dir)):
        if not isinstance(team, dict):
            continue
        lead_agent_id = str(team.get("leadAgentId") or "").strip()
        if lead_agent_id and lead_agent_id not in seen:
            seen.append(lead_agent_id)
        for candidate in safe_list(team.get("memberAgentIds")):
            agent_id = str(candidate or "").strip()
            if agent_id and agent_id not in seen:
                seen.append(agent_id)
    fallback_agents = []
    for agent_id in seen:
        override = overrides.get(agent_id) if isinstance(overrides.get(agent_id), dict) else {}
        runtime_profile = merged_agent_runtime_profile(agent_id, override=override)
        identity_name = (
            str(runtime_profile.get("humanName") or "").strip()
            or str(runtime_profile.get("jobTitle") or "").strip()
            or str(runtime_profile.get("roleLabel") or "").strip()
            or agent_id
        )
        fallback_agents.append(
            {
                "id": agent_id,
                "model": "default",
                "identity": {"name": identity_name},
            }
        )
    return fallback_agents


def _is_reserved_communication_session_entry(entry):
    entry = entry if isinstance(entry, dict) else {}
    origin = entry.get("origin") if isinstance(entry.get("origin"), dict) else {}
    values = {
        str(origin.get("provider") or "").strip().lower(),
        str(origin.get("label") or "").strip().lower(),
        str(origin.get("from") or "").strip().lower(),
        str(origin.get("to") or "").strip().lower(),
    }
    return "heartbeat" in values


def _communication_source_from_session_key(session_key):
    parts = str(session_key or "").split(":")
    return parts[2] if len(parts) > 2 else "main"


def _load_communications_summary_snapshot(openclaw_dir, config=None, now=None):
    svc = _svc()
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        management_metadata = ensure_default_management_bootstrap(
            openclaw_dir,
            load_project_metadata(openclaw_dir, config=config),
        )
        automation = build_management_automation_placeholder(openclaw_dir, management_metadata=management_metadata)
        automation_summary = automation.get("summary") if isinstance(automation.get("summary"), dict) else {}
        session_count = 0
        talkable_sessions = 0
        active_24h = 0
        terminal_sources = set()
        recent_threshold = now - timedelta(hours=24)
        for agent in safe_list(load_agents(config)):
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            index = svc.load_json(openclaw_dir / "agents" / agent_id / "sessions" / "sessions.json", {})
            for session_key, entry in (index.items() if isinstance(index, dict) else []):
                if not isinstance(entry, dict):
                    continue
                session_count += 1
                source = "heartbeat" if _is_reserved_communication_session_entry(entry) else _communication_source_from_session_key(session_key)
                terminal_sources.add(source)
                if source not in {"heartbeat", "cron", "subagent"}:
                    talkable_sessions += 1
                updated_at = entry.get("updatedAt")
                updated_dt = None
                if isinstance(updated_at, (int, float)):
                    updated_dt = datetime.fromtimestamp(float(updated_at) / 1000.0, tz=timezone.utc)
                else:
                    updated_dt = parse_iso(updated_at)
                if updated_dt and updated_dt >= recent_threshold:
                    active_24h += 1
        audit_events = 0
        for event in safe_list(load_audit_events(openclaw_dir, limit=80)):
            action = str((event if isinstance(event, dict) else {}).get("action") or "").strip().lower()
            if action in {"login", "logout"}:
                category = "access"
            elif action.startswith("conversation_"):
                category = "conversation"
            elif action.startswith("management_channel_"):
                category = "channel"
            elif action in {"task_create", "tenant_task_create"}:
                category = "trigger"
            else:
                category = "system"
            if category != "system":
                audit_events += 1
        return {
            "summary": {
                "terminalCount": len(terminal_sources),
                "sessionCount": session_count,
                "talkableSessions": talkable_sessions,
                "active24h": active_24h,
                "activeChannels": int(automation_summary.get("activeChannels") or 0),
                "customerChannelCount": len(
                    safe_list(
                        (
                            automation.get("customerChannels")
                            if isinstance(automation.get("customerChannels"), dict)
                            else {}
                        ).get("channels")
                    )
                ),
                "failedDeliveries": int(automation_summary.get("deliveryFailures") or 0),
                "auditEvents": audit_events,
            }
        }

    return cached_payload(
        ("communications-summary-v2", str(openclaw_dir)),
        10.0,
        build,
    )


def _load_communications_snapshot(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        management_metadata = ensure_default_management_bootstrap(
            openclaw_dir,
            load_project_metadata(openclaw_dir, config=config),
        )
        return build_communications_data(
            openclaw_dir,
            _load_dashboard_conversation_catalog(openclaw_dir, config=config),
            {"automation": build_management_automation_placeholder(openclaw_dir, management_metadata=management_metadata)},
            now,
        )

    return cached_payload(
        ("communications-snapshot", str(openclaw_dir)),
        10.0,
        build,
    )


def build_communications_terminals_snapshot(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        terminal_groups = defaultdict(list)
        session_count = 0
        talkable_sessions = 0
        recent_threshold = now - timedelta(hours=24)
        for agent in safe_list(load_agents(config)):
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            agent_identity = agent.get("identity") if isinstance(agent.get("identity"), dict) else {}
            agent_label = str(agent_identity.get("name") or agent_id).strip() or agent_id
            index = _svc().load_json(openclaw_dir / "agents" / agent_id / "sessions" / "sessions.json", {})
            for session_key, entry in (index.items() if isinstance(index, dict) else []):
                if not isinstance(entry, dict):
                    continue
                session_count += 1
                source = "heartbeat" if _is_reserved_communication_session_entry(entry) else _communication_source_from_session_key(session_key)
                talkable = bool(entry.get("talkable")) if "talkable" in entry else source not in {"heartbeat", "cron", "subagent"}
                if talkable:
                    talkable_sessions += 1
                terminal_groups[source].append(
                    {
                        **entry,
                        "__agentId": agent_id,
                        "__agentLabel": str(entry.get("agentLabel") or entry.get("agentId") or agent_label).strip() or agent_id,
                    }
                )

        terminal_records = []
        for source, items in terminal_groups.items():
            ordered = sorted(
                items,
                key=lambda item: (
                    datetime.fromtimestamp(float(item.get("updatedAt")) / 1000.0, tz=timezone.utc)
                    if isinstance(item.get("updatedAt"), (int, float))
                    else parse_iso(item.get("updatedAt"))
                ) or datetime.fromtimestamp(0, tz=timezone.utc),
                reverse=True,
            )
            latest = ordered[0] if ordered else {}
            latest_updated_at = latest.get("updatedAt")
            if isinstance(latest_updated_at, (int, float)):
                latest_dt = datetime.fromtimestamp(float(latest_updated_at) / 1000.0, tz=timezone.utc)
            else:
                latest_dt = parse_iso(latest_updated_at)
            aborted_count = sum(1 for item in ordered if item.get("abortedLastRun"))
            active_24h = 0
            for item in ordered:
                updated_at = item.get("updatedAt")
                if isinstance(updated_at, (int, float)):
                    updated_dt = datetime.fromtimestamp(float(updated_at) / 1000.0, tz=timezone.utc)
                else:
                    updated_dt = parse_iso(updated_at)
                if updated_dt and updated_dt >= recent_threshold:
                    active_24h += 1
            talkable_count = sum(
                1 for item in ordered
                if (bool(item.get("talkable")) if "talkable" in item else source not in {"heartbeat", "cron", "subagent"})
            )
            status = "attention" if aborted_count else ("active" if active_24h else "idle")
            terminal_records.append(
                {
                    "id": source,
                    "label": CONVERSATION_SOURCE_LABELS.get(source, source),
                    "source": source,
                    "status": status,
                    "sessionCount": len(ordered),
                    "talkableCount": talkable_count,
                    "agentCount": len({item.get("__agentId") for item in ordered if item.get("__agentId")}),
                    "active24h": active_24h,
                    "abortedCount": aborted_count,
                    "latestAt": latest.get("updatedAt", ""),
                    "latestAgo": format_age(latest_dt, now) if latest_dt else "未知时间",
                    "preview": str(latest.get("preview") or latest.get("summary") or latest.get("headline") or "").strip(),
                    "agents": sorted({item.get("__agentLabel") for item in ordered if item.get("__agentLabel")}),
                }
            )

        terminal_records.sort(
            key=lambda item: (item.get("status") != "attention", -int(item.get("active24h") or 0), -int(item.get("sessionCount") or 0))
        )
        return {
            "summary": {
                "terminalCount": len(terminal_records),
                "sessionCount": session_count,
                "talkableSessions": talkable_sessions,
            },
            "terminals": terminal_records,
        }

    return cached_payload(
        ("communications-terminals-v1", str(openclaw_dir)),
        10.0,
        build,
    )


def build_communications_commands_snapshot(openclaw_dir):
    openclaw_dir = Path(openclaw_dir)
    return {
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


def _build_communications_delivery_rows(alerts):
    rows = []
    for alert in safe_list(alerts):
        if not isinstance(alert, dict):
            continue
        for delivery in safe_list(alert.get("deliveries")):
            if not isinstance(delivery, dict):
                continue
            rows.append(
                {
                    **delivery,
                    "alertId": alert.get("id", ""),
                    "alertTitle": alert.get("title", ""),
                    "severity": alert.get("severity", ""),
                    "status": alert.get("status", ""),
                }
            )
    return rows


def _build_communications_delivery_snapshot_from_storage(openclaw_dir, now):
    channels = safe_list(_svc().store_list_notification_channels(openclaw_dir))
    customer_channels = safe_list(_svc().store_list_customer_access_channels(openclaw_dir))
    deliveries = safe_list(_svc().store_list_notification_deliveries(openclaw_dir, limit=240))
    alerts = safe_list(_svc().store_list_automation_alerts(openclaw_dir, limit=120))
    alert_map = {str(item.get("id") or "").strip(): item for item in alerts if str(item.get("id") or "").strip()}
    channel_records = []
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        channel_id = str(channel.get("id") or "").strip()
        channel_deliveries = [item for item in deliveries if str(item.get("channelId") or "").strip() == channel_id]
        success_count = sum(1 for item in channel_deliveries if str(item.get("outcome") or "").strip().lower() == "success")
        error_count = sum(1 for item in channel_deliveries if str(item.get("outcome") or "").strip().lower() != "success")
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
        status = "error" if error_count else ("active" if str(channel.get("status") or "").strip().lower() == "active" else "idle")
        channel_records.append(
            {
                **channel,
                "status": status,
                "successCount": success_count,
                "errorCount": error_count,
                "latestDeliveryAt": latest_delivery.get("deliveredAt", ""),
                "latestDeliveryAgo": format_age(latest_dt, now) if latest_dt else "尚未投递",
                "latestDetail": latest_delivery.get("detail", ""),
                "alertCount": len({str(item.get("alertId") or "").strip() for item in channel_deliveries if str(item.get("alertId") or "").strip()}),
            }
        )
    channel_records.sort(
        key=lambda item: (
            item.get("status") not in {"error", "active"},
            -int(item.get("errorCount") or 0),
            -int(item.get("successCount") or 0),
        )
    )
    failures = []
    for delivery in sorted(
        [item for item in deliveries if str(item.get("outcome") or "").strip().lower() != "success"],
        key=lambda item: parse_iso(item.get("deliveredAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )[:24]:
        alert = alert_map.get(str(delivery.get("alertId") or "").strip(), {})
        failures.append(
            {
                **delivery,
                "alertTitle": str(alert.get("title") or delivery.get("alertId") or "").strip(),
                "severity": str(alert.get("severity") or "warning").strip(),
                "status": str(alert.get("status") or "").strip(),
            }
        )
    return {
        "summary": {
            "activeChannels": sum(1 for item in channel_records if item.get("status") in {"active", "error"}),
            "customerChannelCount": len(customer_channels),
            "failedDeliveries": len(failures),
        },
        "channels": channel_records,
        "customerChannels": customer_channels,
        "failures": failures,
    }


def _build_communications_delivery_snapshot_payload(automation, now):
    automation = automation if isinstance(automation, dict) else {}
    channels = safe_list(automation.get("channels"))
    customer_channels_payload = automation.get("customerChannels") if isinstance(automation.get("customerChannels"), dict) else {}
    customer_channels = safe_list(customer_channels_payload.get("channels"))
    alerts = safe_list(automation.get("alerts"))
    delivery_rows = _build_communications_delivery_rows(alerts)
    channel_records = []
    for channel in channels:
        if not isinstance(channel, dict):
            continue
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
    channel_records.sort(
        key=lambda item: (
            item.get("status") not in {"error", "active"},
            -int(item.get("errorCount") or 0),
            -int(item.get("successCount") or 0),
        )
    )
    failures = sorted(
        [item for item in delivery_rows if item.get("outcome") != "success"],
        key=lambda item: parse_iso(item.get("deliveredAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )[:12]
    return {
        "summary": {
            "activeChannels": sum(1 for item in channel_records if item.get("status") in {"active", "error"}),
            "customerChannelCount": len(customer_channels),
            "failedDeliveries": len(failures),
        },
        "channels": channel_records,
        "customerChannels": customer_channels,
        "failures": failures,
    }


def build_communications_delivery_snapshot(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    now = now or now_utc()
    delivery = _build_communications_delivery_snapshot_from_storage(openclaw_dir, now)
    summary = delivery.get("summary") if isinstance(delivery.get("summary"), dict) else {}
    return {
        "summary": {
            "activeChannels": int(summary.get("activeChannels") or 0),
            "customerChannelCount": int(summary.get("customerChannelCount") or 0),
            "failedDeliveries": int(summary.get("failedDeliveries") or 0),
        },
        "channels": deepcopy(safe_list(delivery.get("channels"))),
        "customerChannels": deepcopy(safe_list(delivery.get("customerChannels"))),
    }


def build_communications_failures_snapshot(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    now = now or now_utc()
    delivery = _build_communications_delivery_snapshot_from_storage(openclaw_dir, now)
    summary = delivery.get("summary") if isinstance(delivery.get("summary"), dict) else {}
    return {
        "summary": {
            "failedDeliveries": int(summary.get("failedDeliveries") or 0),
        },
        "failures": deepcopy(safe_list(delivery.get("failures"))),
    }


def build_communications_audit_snapshot(openclaw_dir, now=None):
    openclaw_dir = Path(openclaw_dir)
    now = now or now_utc()
    audit = safe_list(load_audit_events(openclaw_dir, limit=80))
    return {
        "summary": {
            "auditEvents": len(audit),
        },
        "audit": deepcopy(audit),
    }


def build_communications_summary_snapshot(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    return deepcopy(_load_communications_summary_snapshot(openclaw_dir, config=config, now=now))


def build_activity_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    task_index = safe_list(task_snapshot.get("taskIndex"))
    recent_threshold = now - timedelta(hours=24)
    global_events = []
    relay_counter = Counter()
    relay_last_at = {}
    for task in task_index:
        if not isinstance(task, dict):
            continue
        task_title = str(task.get("title") or task.get("id") or "").strip()
        task_id = str(task.get("id") or "").strip()
        for replay_event in safe_list(task.get("replay")):
            if not isinstance(replay_event, dict):
                continue
            global_events.append(
                {
                    "type": replay_event.get("kind"),
                    "at": replay_event.get("at", ""),
                    "title": task_title,
                    "taskId": task_id,
                    "headline": replay_event.get("headline", ""),
                    "detail": replay_event.get("detail", ""),
                }
            )
            if replay_event.get("kind") != "handoff":
                continue
            at = parse_iso(replay_event.get("at"))
            if not at or at < recent_threshold:
                continue
            edge = (replay_event.get("actorLabel"), replay_event.get("targetLabel"))
            relay_counter[edge] += 1
            relay_last_at[edge] = max(at, relay_last_at.get(edge, at))

    global_events.sort(
        key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    relays = [
        {
            "from": edge[0],
            "to": edge[1],
            "count": count,
            "lastAt": relay_last_at[edge].isoformat().replace("+00:00", "Z"),
            "lastAgo": format_age(relay_last_at[edge], now),
        }
        for edge, count in relay_counter.most_common(10)
    ]
    return {
        "events": global_events[:42],
        "relays": relays,
    }


def _build_management_task_deliverables(task_index):
    deliverables = []
    for task in safe_list(task_index):
        if not isinstance(task, dict):
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        output = str(task.get("output") or "").strip()
        if state not in TERMINAL_STATES and not output:
            continue
        deliverables.append(
            {
                "id": str(task.get("id") or "").strip(),
                "title": str(task.get("title") or task.get("id") or "Untitled Task").strip(),
                "state": str(task.get("state") or task.get("status") or "").strip(),
                "status": "completed" if state in TERMINAL_STATES else "review",
                "statusLabel": str(task.get("state") or task.get("status") or "").strip(),
                "owner": str(task.get("owner") or "").strip(),
                "updatedAt": str(task.get("updatedAt") or "").strip(),
                "updatedAgo": str(task.get("updatedAgo") or "").strip(),
                "summary": str(task.get("resultSummary") or task.get("currentUpdate") or "").strip(),
                "output": output,
                "sourceTask": str(task.get("id") or "").strip(),
                "type": infer_deliverable_type(output),
            }
        )
    return deliverables


def _load_agent_direct_task_state_snapshot(openclaw_dir):
    openclaw_dir = Path(openclaw_dir)

    def build():
        task_index = []
        active_tasks = []
        deliverables = []
        task_counts_by_agent = Counter()
        blocked_counts_by_agent = Counter()
        latest_focus_by_agent = {}
        latest_focus_dt_by_agent = {}
        agent_signals = defaultdict(list)
        recent_signal_count = 0
        signal_threshold = now_utc() - timedelta(hours=1)

        for task in _svc().safe_list(_svc().store_list_task_records(openclaw_dir, limit=240)):
            if not isinstance(task, dict) or is_merged_duplicate_task(task):
                continue
            compact = compact_task_dashboard_payload(task)
            task_index.append(compact)
            state = str(compact.get("state") or compact.get("status") or "").strip().lower()
            current_agent = str(compact.get("currentAgent") or "").strip()
            updated_dt = parse_iso(compact.get("updatedAt"))
            if updated_dt and updated_dt >= signal_threshold:
                recent_signal_count += 1
            if state not in TERMINAL_STATES:
                active_tasks.append(compact)
            if current_agent and state not in TERMINAL_STATES:
                task_counts_by_agent[current_agent] += 1
                if compact.get("blocked"):
                    blocked_counts_by_agent[current_agent] += 1
                if current_agent not in latest_focus_dt_by_agent or (
                    updated_dt and updated_dt >= latest_focus_dt_by_agent[current_agent]
                ):
                    latest_focus_dt_by_agent[current_agent] = updated_dt
                    latest_focus_by_agent[current_agent] = str(compact.get("currentUpdate") or compact.get("title") or "").strip()
                agent_signals[current_agent].append(
                    {
                        "title": str(compact.get("title") or "").strip(),
                        "taskId": str(compact.get("id") or "").strip(),
                        "meta": str(compact.get("updatedAgo") or "").strip(),
                        "detail": str(compact.get("currentUpdate") or compact.get("resultSummary") or "").strip(),
                    }
                )
            if state in TERMINAL_STATES or str(compact.get("output") or "").strip():
                deliverables.append(
                    {
                        "id": compact.get("id"),
                        "title": compact.get("title"),
                        "state": compact.get("state"),
                        "status": "completed" if state in TERMINAL_STATES else "review",
                        "statusLabel": compact.get("state"),
                        "owner": compact.get("owner"),
                        "updatedAt": compact.get("updatedAt"),
                        "updatedAgo": compact.get("updatedAgo"),
                        "summary": compact.get("resultSummary") or compact.get("currentUpdate"),
                        "output": compact.get("output"),
                        "sourceTask": compact.get("id"),
                        "type": infer_deliverable_type(compact.get("output")),
                    }
                )

        task_index.sort(
            key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        active_tasks.sort(
            key=lambda item: parse_iso(item.get("activityAt") or item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        deliverables.sort(
            key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        return {
            "taskIndex": task_index,
            "activeTasks": active_tasks,
            "deliverables": deliverables,
            "taskCountsByAgent": dict(task_counts_by_agent),
            "blockedCountsByAgent": dict(blocked_counts_by_agent),
            "latestFocusByAgent": latest_focus_by_agent,
            "latestFocusAtByAgent": {
                agent_id: timestamp.isoformat().replace("+00:00", "Z")
                for agent_id, timestamp in latest_focus_dt_by_agent.items()
                if timestamp
            },
            "handoffs24hByAgent": {},
            "agentSignals": dict(agent_signals),
            "signals1h": recent_signal_count,
            "relays": [],
            "events": [],
        }

    return cached_payload(
        ("agent-direct-task-state-v2", str(openclaw_dir)),
        10.0,
        build,
    )


def build_agent_cards_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    direct_task_state = task_snapshot if isinstance(task_snapshot, dict) else _load_agent_direct_task_state_snapshot(openclaw_dir)

    def build():
        raw_agents = safe_list(load_agents(config))
        if not raw_agents:
            raw_agents = _fallback_runtime_agents_from_teams(openclaw_dir, config=config)
        task_counts_by_agent = direct_task_state.get("taskCountsByAgent") if isinstance(direct_task_state.get("taskCountsByAgent"), dict) else {}
        blocked_counts_by_agent = direct_task_state.get("blockedCountsByAgent") if isinstance(direct_task_state.get("blockedCountsByAgent"), dict) else {}
        latest_focus_by_agent = direct_task_state.get("latestFocusByAgent") if isinstance(direct_task_state.get("latestFocusByAgent"), dict) else {}
        latest_focus_at_by_agent = direct_task_state.get("latestFocusAtByAgent") if isinstance(direct_task_state.get("latestFocusAtByAgent"), dict) else {}
        handoffs_24h_by_agent = direct_task_state.get("handoffs24hByAgent") if isinstance(direct_task_state.get("handoffs24hByAgent"), dict) else {}
        agent_signals = direct_task_state.get("agentSignals") if isinstance(direct_task_state.get("agentSignals"), dict) else {}
        active_tasks = safe_list(direct_task_state.get("activeTasks"))
        router_agent_id = str(get_router_agent_id(config)).strip()
        kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
        metadata = load_project_metadata(openclaw_dir, config=config)
        overrides = agent_runtime_overrides(metadata)
        agent_labels, _label_to_agent_ids = build_label_maps(raw_agents, kanban_cfg, router_agent_id)

        cards = []
        for agent in raw_agents:
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            workspace = Path(agent.get("workspace", "")) if agent.get("workspace") else openclaw_dir / f"workspace-{agent_id}"
            workspace_dt = workspace_last_activity(workspace)
            session_dt = session_last_activity(openclaw_dir, agent_id)
            signal_dt = parse_iso(latest_focus_at_by_agent.get(agent_id))
            last_seen = max([dt for dt in (workspace_dt, session_dt, signal_dt) if dt is not None], default=None)
            active_task_cards = [
                {
                    "id": task.get("id"),
                    "title": task.get("title"),
                    "state": task.get("state"),
                    "updatedAgo": task.get("updatedAgo"),
                }
                for task in active_tasks
                if isinstance(task, dict) and str(task.get("currentAgent") or "").strip() == agent_id
            ]
            status = status_for_agent(
                int(task_counts_by_agent.get(agent_id) or 0),
                int(blocked_counts_by_agent.get(agent_id) or 0),
                signal_dt,
                last_seen,
                now,
            )
            override = overrides.get(agent_id) if isinstance(overrides.get(agent_id), dict) else {}
            runtime_profile = merged_agent_runtime_profile(agent_id, override=override)
            is_paused = bool(override.get("paused"))
            if is_paused:
                status = "paused"
            work_guard = work_guard_for_agent(
                int(task_counts_by_agent.get(agent_id) or 0),
                int(blocked_counts_by_agent.get(agent_id) or 0),
                signal_dt,
                last_seen,
                workspace_dt,
                session_dt,
                now,
                paused=is_paused,
            )
            cards.append(
                {
                    "id": agent_id,
                    "name": ((agent.get("identity") if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", agent_id),
                    "humanName": runtime_profile.get("humanName", "") or ((agent.get("identity") if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", agent_id),
                    "title": agent_labels.get(agent_id, agent_id),
                    "model": agent.get("model", "default"),
                    "role": runtime_profile.get("role", ""),
                    "roleLabel": runtime_profile.get("roleLabel", ""),
                    "jobTitle": runtime_profile.get("jobTitle", ""),
                    "workingStyle": runtime_profile.get("workingStyle", ""),
                    "skills": safe_list(runtime_profile.get("skills")),
                    "skillCount": int(runtime_profile.get("skillCount") or 0),
                    "status": status,
                    "paused": is_paused,
                    "activeTasks": int(task_counts_by_agent.get(agent_id) or 0),
                    "blockedTasks": int(blocked_counts_by_agent.get(agent_id) or 0),
                    "focus": latest_focus_by_agent.get(agent_id, ""),
                    "currentTaskId": active_task_cards[0]["id"] if active_task_cards else "",
                    "currentTaskTitle": active_task_cards[0]["title"] if active_task_cards else "",
                    "lastSeenAgo": format_age(last_seen, now),
                    "lastSeenAt": last_seen.isoformat().replace("+00:00", "Z") if last_seen else "",
                    "workspaceLastSeenAgo": format_age(workspace_dt, now),
                    "sessionLastSeenAgo": format_age(session_dt, now),
                    "handoffs24h": int(handoffs_24h_by_agent.get(agent_id) or 0),
                    "activeTaskCards": active_task_cards[:6],
                    "recentSignals": safe_list(agent_signals.get(agent_id))[:8],
                    "workGuard": work_guard,
                    "runtimeOverride": override,
                }
            )
        return cards

    return deepcopy(
        cached_payload(
            ("agent-cards-direct-v2", str(openclaw_dir)),
            10.0,
            build,
        )
    )


def build_agent_teams_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    direct_task_state = task_snapshot if isinstance(task_snapshot, dict) else _load_agent_direct_task_state_snapshot(openclaw_dir)
    return deepcopy(
        cached_payload(
            ("agent-teams-direct-v2", str(openclaw_dir)),
            10.0,
            lambda: build_agent_team_data(
                openclaw_dir,
                build_agent_cards_snapshot(openclaw_dir, config=config, now=now, task_snapshot=direct_task_state),
                safe_list(direct_task_state.get("taskIndex")),
                now,
            ),
        )
    )


def build_agent_team_options_snapshot(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)

    def build():
        agent_cards_by_id = {
            str(agent.get("id") or "").strip(): agent
            for agent in safe_list(build_agent_cards_snapshot(openclaw_dir, config=config))
            if isinstance(agent, dict) and str(agent.get("id") or "").strip()
        }
        items = []
        active_count = 0
        for team in safe_list(store_list_agent_teams(openclaw_dir)):
            if not isinstance(team, dict):
                continue
            team_id = str(team.get("id") or "").strip()
            if not team_id:
                continue
            status = str(team.get("status") or "").strip()
            if status == "active":
                active_count += 1
            member_agent_ids = []
            for agent_id in [
                *safe_list(team.get("memberAgentIds")),
                *safe_list(team.get("participantAgentIds")),
                str(team.get("leadAgentId") or "").strip(),
            ]:
                normalized_id = str(agent_id or "").strip()
                if normalized_id and normalized_id not in member_agent_ids:
                    member_agent_ids.append(normalized_id)
            compact_members = []
            for agent_id in member_agent_ids:
                agent = agent_cards_by_id.get(agent_id, {})
                compact_members.append(
                    {
                        "id": agent_id,
                        "label": str(agent.get("title") or agent.get("humanName") or agent.get("name") or agent_id).strip(),
                        "humanName": str(agent.get("humanName") or agent.get("name") or "").strip(),
                        "name": str(agent.get("name") or "").strip(),
                        "status": str(agent.get("status") or "").strip(),
                        "roleLabel": str(agent.get("roleLabel") or "").strip(),
                        "jobTitle": str(agent.get("jobTitle") or "").strip(),
                        "focus": str(agent.get("focus") or "").strip(),
                    }
                )
            items.append(
                {
                    "id": team_id,
                    "name": str(team.get("name") or team_id).strip(),
                    "status": status,
                    "leadAgentId": str(team.get("leadAgentId") or "").strip(),
                    "memberCount": len(member_agent_ids),
                    "memberAgentIds": member_agent_ids,
                    "participantAgentIds": member_agent_ids,
                    "members": compact_members,
                }
            )
        items.sort(key=lambda item: (item.get("name") or item.get("id") or "").lower())
        return {
            "summary": {
                "teamCount": len(items),
                "activeTeamCount": active_count,
            },
            "items": items,
        }

    return cached_payload(
        ("agent-team-options-direct", str(openclaw_dir)),
        10.0,
        build,
    )


def build_deliverables_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    now = now or now_utc()
    direct_task_state = task_snapshot if isinstance(task_snapshot, dict) else _load_agent_direct_task_state_snapshot(openclaw_dir)
    deliverables = _merge_unique_deliverables(
        safe_list(direct_task_state.get("deliverables")),
        _build_filesystem_deliverables(openclaw_dir, now=now),
        _build_workflow_run_deliverables(openclaw_dir, now=now),
    )
    deliverables = _append_management_run_artifact_deliverables(openclaw_dir, deliverables, now=now)
    deliverables.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    return deepcopy(deliverables)


def build_metrics_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    direct_task_state = task_snapshot if isinstance(task_snapshot, dict) else _load_agent_direct_task_state_snapshot(openclaw_dir)
    return deepcopy(
        cached_payload(
            ("metrics-direct-v2", str(openclaw_dir)),
            10.0,
            lambda: (
                lambda agent_cards, task_index, active_tasks: {
                    "activeTasks": len(active_tasks),
                    "activeAgents": sum(
                        1
                        for agent in agent_cards
                        if str(agent.get("status") or "").strip().lower() in {"active", "waiting", "blocked"}
                    ),
                    "blockedTasks": sum(1 for task in active_tasks if isinstance(task, dict) and task.get("blocked")),
                    "completedToday": sum(
                        1
                        for task in task_index
                        if isinstance(task, dict)
                        and str(task.get("state") or "").strip().lower() == "done"
                        and (parse_iso(task.get("updatedAt")) or now) >= now - timedelta(days=1)
                    ),
                    "handoffs24h": 0,
                    "signals1h": int(direct_task_state.get("signals1h") or 0),
                    "needsAttentionAgents": sum(
                        1
                        for agent in agent_cards
                        if ((agent.get("workGuard") if isinstance(agent.get("workGuard"), dict) else {}).get("needsAttention"))
                    ),
                    "stalledAgents": sum(
                        1
                        for agent in agent_cards
                        if str(((agent.get("workGuard") if isinstance(agent.get("workGuard"), dict) else {}).get("reason")) or "").strip() == "stale_progress"
                    ),
                }
            )(
                build_agent_cards_snapshot(openclaw_dir, config=config, now=now, task_snapshot=direct_task_state),
                safe_list(direct_task_state.get("taskIndex")),
                safe_list(direct_task_state.get("activeTasks")),
            ),
        )
    )


def _load_chat_agent_presence_snapshot(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)

    def build():
        metadata = load_project_metadata(openclaw_dir, config=config)
        overrides = agent_runtime_overrides(metadata)
        direct_task_state = _load_agent_direct_task_state_snapshot(openclaw_dir)
        task_counts_by_agent = direct_task_state.get("taskCountsByAgent") if isinstance(direct_task_state.get("taskCountsByAgent"), dict) else {}
        blocked_counts_by_agent = direct_task_state.get("blockedCountsByAgent") if isinstance(direct_task_state.get("blockedCountsByAgent"), dict) else {}
        latest_focus_by_agent = direct_task_state.get("latestFocusByAgent") if isinstance(direct_task_state.get("latestFocusByAgent"), dict) else {}
        router_agent_id = str(get_router_agent_id(config)).strip()
        kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
        raw_agents = safe_list(load_agents(config))
        if not raw_agents:
            raw_agents = _fallback_runtime_agents_from_teams(openclaw_dir, config=config)
        agent_labels, _label_to_agent_ids = build_label_maps(raw_agents, kanban_cfg, router_agent_id)
        cards = []
        for agent in raw_agents:
            if not isinstance(agent, dict):
                continue
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            override = overrides.get(agent_id) if isinstance(overrides.get(agent_id), dict) else {}
            runtime_profile = merged_agent_runtime_profile(agent_id, override=override)
            if bool(override.get("paused")):
                status = "paused"
            elif int(blocked_counts_by_agent.get(agent_id) or 0) > 0:
                status = "blocked"
            elif int(task_counts_by_agent.get(agent_id) or 0) > 0:
                status = "active"
            else:
                status = "idle"
            identity = agent.get("identity") if isinstance(agent.get("identity"), dict) else {}
            cards.append(
                {
                    "id": agent_id,
                    "name": identity.get("name", agent_id),
                    "humanName": runtime_profile.get("humanName", "") or identity.get("name", agent_id),
                    "title": agent_labels.get(agent_id, agent_id),
                    "model": agent.get("model", "default"),
                    "role": runtime_profile.get("role", ""),
                    "roleLabel": runtime_profile.get("roleLabel", ""),
                    "jobTitle": runtime_profile.get("jobTitle", ""),
                    "status": status,
                    "focus": latest_focus_by_agent.get(agent_id, ""),
                }
            )
        return cards

    return deepcopy(
        cached_payload(
            ("chat-agent-presence-v1", str(openclaw_dir)),
            10.0,
            build,
        )
    )


def _load_chat_team_snapshot(openclaw_dir):
    openclaw_dir = Path(openclaw_dir)

    def build():
        items = []
        for team in safe_list(store_list_agent_teams(openclaw_dir)):
            if not isinstance(team, dict):
                continue
            team_id = str(team.get("id") or "").strip()
            if not team_id:
                continue
            items.append(
                {
                    "id": team_id,
                    "name": str(team.get("name") or "").strip(),
                    "status": str(team.get("status") or "").strip(),
                    "leadAgentId": str(team.get("leadAgentId") or "").strip(),
                    "memberAgentIds": [
                        str(agent_id or "").strip()
                        for agent_id in safe_list(team.get("memberAgentIds"))
                        if str(agent_id or "").strip()
                    ],
                }
            )
        return {"items": items}

    return deepcopy(
        cached_payload(
            ("chat-team-snapshot-v1", str(openclaw_dir)),
            10.0,
            build,
        )
    )


def build_conversations_catalog_snapshot(openclaw_dir, config=None):
    return deepcopy(_load_dashboard_conversation_catalog(openclaw_dir, config=config))


def _build_management_runtime_context(
    openclaw_dir,
    config=None,
    now=None,
    task_snapshot=None,
    include_conversation_data=False,
    include_activity_snapshot=False,
):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    task_index = safe_list(task_snapshot.get("taskIndex"))
    agents = safe_list(task_snapshot.get("agents"))
    if not agents:
        agents = safe_list(load_agents(config))
    conversation_data = (
        _load_dashboard_conversation_catalog(openclaw_dir, config=config)
        if include_conversation_data
        else {}
    )
    activity_snapshot = (
        build_activity_snapshot(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
        if include_activity_snapshot
        else {}
    )
    management_metadata = ensure_default_management_bootstrap(
        openclaw_dir,
        load_project_metadata(openclaw_dir, config=config),
    )
    return {
        "config": config,
        "now": now,
        "taskSnapshot": task_snapshot,
        "taskIndex": task_index,
        "agents": agents,
        "conversationData": conversation_data,
        "activitySnapshot": activity_snapshot,
        "managementMetadata": management_metadata,
    }


def build_management_runs_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None, limit=48):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        return {
            "runs": deepcopy(
                _build_management_run_rows_snapshot(
                    openclaw_dir,
                    config=config,
                    now=now,
                    limit=limit,
                )
            ),
        }

    return cached_payload(
        ("management-direct-runs", str(openclaw_dir), str(limit)),
        5.0,
        build,
    )


def build_management_decision_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
        task_index = context["taskIndex"]
        routing_decisions = store_list_routing_decisions(openclaw_dir, limit=180)
        return {
            "summary": deepcopy(
                build_management_summary_snapshot(
                    openclaw_dir,
                    config=context["config"],
                    now=context["now"],
                    task_snapshot=context["taskSnapshot"],
                ).get("summary")
                or {}
            ),
            "intelligence": build_task_intelligence_summary(task_index),
            "decisionQuality": build_routing_effectiveness_summary(task_index, routing_decisions),
            "decisionSourceReview": build_decision_source_review(task_index, routing_decisions, context["now"]),
            "recommendationReview": build_recommendation_accuracy_review(openclaw_dir, task_index, context["now"]),
        }

    return cached_payload(
        ("management-direct-decision", str(openclaw_dir)),
        5.0,
        build,
    )


def build_management_decision_intelligence_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
        return {
            "intelligence": build_task_intelligence_summary(context["taskIndex"]),
        }

    return cached_payload(
        ("management-direct-decision-intelligence", str(openclaw_dir)),
        5.0,
        build,
    )


def build_management_decision_quality_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        return {
            "decisionQuality": build_routing_effectiveness_summary(context["taskIndex"], routing_decisions),
        }

    return cached_payload(
        ("management-direct-decision-quality", str(openclaw_dir)),
        5.0,
        build,
    )


def build_management_decision_sources_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        return {
            "decisionSourceReview": build_decision_source_review(context["taskIndex"], routing_decisions, context["now"]),
        }

    return cached_payload(
        ("management-direct-decision-sources", str(openclaw_dir)),
        5.0,
        build,
    )


def build_management_decision_recommendations_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
        return {
            "recommendationReview": build_recommendation_accuracy_review(openclaw_dir, context["taskIndex"], context["now"]),
        }

    return cached_payload(
        ("management-direct-decision-recommendations", str(openclaw_dir)),
        5.0,
        build,
    )


def _build_management_automation_snapshot(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    teams = safe_list(store_list_agent_teams(openclaw_dir))

    def build():
        agents = safe_list(load_agents(config))
        management_metadata = ensure_default_management_bootstrap(
            openclaw_dir,
            load_project_metadata(openclaw_dir, config=config),
        )
        if should_skip_management_automation_cycle():
            automation = build_management_automation_placeholder(
                openclaw_dir,
                management_metadata=management_metadata,
            )
        else:
            automation = run_automation_engine_cycle(
                openclaw_dir,
                source="dashboard",
                now=now_utc(),
            )
            if not isinstance(automation.get("mode"), dict):
                automation["mode"] = build_management_automation_placeholder(
                    openclaw_dir,
                    management_metadata=management_metadata,
                ).get("mode", {})
            if not isinstance(automation.get("engine"), dict):
                automation["engine"] = load_automation_engine_status(openclaw_dir)
            if not isinstance(automation.get("customerChannels"), dict):
                automation["customerChannels"] = build_customer_access_snapshot(openclaw_dir)
        automation["memorySystem"] = memory_system_status_payload(
            management_metadata,
            agents=agents,
            teams=teams,
        )
        return automation

    return cached_payload(
        ("management-direct-automation", str(openclaw_dir)),
        5.0,
        build,
    )


def _build_management_automation_direct_payload(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    management_metadata = ensure_default_management_bootstrap(
        openclaw_dir,
        load_project_metadata(openclaw_dir, config=config),
    )
    agents = safe_list(load_agents(config))
    teams = safe_list(store_list_agent_teams(openclaw_dir))
    rules = safe_list(_svc().store_list_automation_rules(openclaw_dir))
    channels = safe_list(_svc().store_list_notification_channels(openclaw_dir))
    customer_channels = deepcopy(build_customer_access_snapshot(openclaw_dir))
    alerts = safe_list(_svc().store_list_automation_alerts(openclaw_dir, limit=120))
    deliveries = safe_list(_svc().store_list_notification_deliveries(openclaw_dir, limit=240))
    alert_map = {str(item.get("id") or "").strip(): item for item in alerts if str(item.get("id") or "").strip()}
    deliveries_by_alert = defaultdict(list)
    deliveries_by_channel = defaultdict(list)
    for delivery in deliveries:
        if not isinstance(delivery, dict):
            continue
        deliveries_by_alert[str(delivery.get("alertId") or "").strip()].append(delivery)
        deliveries_by_channel[str(delivery.get("channelId") or "").strip()].append(delivery)

    channel_records = []
    for channel in channels:
        if not isinstance(channel, dict):
            continue
        channel_id = str(channel.get("id") or "").strip()
        channel_deliveries = deliveries_by_channel.get(channel_id, [])
        success_count = sum(1 for item in channel_deliveries if str(item.get("outcome") or "").strip().lower() == "success")
        error_count = len(channel_deliveries) - success_count
        latest_failure = next(
            (
                item
                for item in sorted(
                    [item for item in channel_deliveries if str(item.get("outcome") or "").strip().lower() != "success"],
                    key=lambda item: parse_iso(item.get("deliveredAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
                    reverse=True,
                )
            ),
            {},
        )
        latest_failure_reason = str(latest_failure.get("detail") or latest_failure.get("outcome") or "").strip()
        latest_dt = parse_iso((latest_failure or {}).get("deliveredAt")) or parse_iso(channel.get("updatedAt"))
        channel_records.append(
            {
                **channel,
                "attempts": len(channel_deliveries),
                "success": success_count,
                "errors": error_count,
                "successRate": round((success_count / max(len(channel_deliveries), 1)) * 100, 1),
                "latestFailureReason": latest_failure_reason,
                "latestAgo": format_age(latest_dt, now) if latest_dt else "未知时间",
            }
        )
    channel_records.sort(key=lambda item: (-int(item.get("errors") or 0), -int(item.get("attempts") or 0), str(item.get("name") or "").lower()))

    failure_rows = []
    for delivery in sorted(
        [item for item in deliveries if str(item.get("outcome") or "").strip().lower() != "success"],
        key=lambda item: parse_iso(item.get("deliveredAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    ):
        alert = alert_map.get(str(delivery.get("alertId") or "").strip(), {})
        failure_rows.append(
            {
                **delivery,
                "alertTitle": str(alert.get("title") or delivery.get("alertId") or "").strip(),
                "severity": str(alert.get("severity") or "warning").strip(),
                "status": str(alert.get("status") or "").strip(),
            }
        )

    delivery_attempt_count = len(deliveries)
    delivery_success_count = sum(1 for item in deliveries if str(item.get("outcome") or "").strip().lower() == "success")
    delivery_error_count = delivery_attempt_count - delivery_success_count
    delivery_success_rate = round((delivery_success_count / max(delivery_attempt_count, 1)) * 100, 1)

    trend_by_day = defaultdict(lambda: {"attempts": 0, "success": 0, "errors": 0})
    for delivery in deliveries:
        delivered_at = parse_iso(delivery.get("deliveredAt"))
        if not delivered_at:
            continue
        day_key = delivered_at.strftime("%Y-%m-%d")
        bucket = trend_by_day[day_key]
        bucket["attempts"] += 1
        if str(delivery.get("outcome") or "").strip().lower() == "success":
            bucket["success"] += 1
        else:
            bucket["errors"] += 1
    delivery_trend = [
        {
            "date": day,
            "label": day[5:],
            **trend_by_day[day],
        }
        for day in sorted(trend_by_day.keys())[-7:]
    ]
    failure_reason_counter = Counter()
    failure_reason_samples = {}
    for failure in failure_rows:
        reason = str(failure.get("detail") or failure.get("alertTitle") or failure.get("outcome") or "unknown").strip()
        failure_reason_counter[reason] += 1
        failure_reason_samples.setdefault(reason, failure)

    delivery_failure_reasons = [
        {
            "label": reason,
            "count": count,
            "latestAgo": format_age(parse_iso(failure_reason_samples[reason].get("deliveredAt")), now) if parse_iso(failure_reason_samples[reason].get("deliveredAt")) else "未知时间",
            "sample": str(failure_reason_samples[reason].get("detail") or failure_reason_samples[reason].get("alertTitle") or "").strip(),
        }
        for reason, count in failure_reason_counter.most_common(6)
    ]

    active_rule_count = sum(1 for rule in rules if str(rule.get("status") or "").strip().lower() == "active")
    active_channel_count = sum(1 for channel in channel_records if str(channel.get("status") or "").strip().lower() == "active")
    healthy_channel_count = sum(1 for channel in channel_records if int(channel.get("errors") or 0) == 0)
    open_alert_count = sum(1 for alert in alerts if str(alert.get("status") or "").strip().lower() not in {"resolved", "closed"})
    notified_alert_count = sum(1 for alert in alerts if str(alert.get("status") or "").strip().lower() == "notified")

    rule_rows = []
    rule_suggestions = []
    tuning_rows = []
    remediation_actions = []
    for rule in rules:
        rule_id = str(rule.get("id") or "").strip()
        related_alerts = [item for item in alerts if str(item.get("ruleId") or "").strip() == rule_id]
        related_deliveries = [item for item in deliveries if str(item.get("alertId") or "").strip() in {str(alert.get("id") or "").strip() for alert in related_alerts}]
        triggered_count = len(related_alerts)
        notified_count = sum(1 for item in related_deliveries if str(item.get("outcome") or "").strip().lower() == "success")
        resolved_count = sum(1 for item in related_alerts if str(item.get("status") or "").strip().lower() in {"resolved", "closed"})
        delivery_error_count = sum(1 for item in related_deliveries if str(item.get("outcome") or "").strip().lower() != "success")
        unresolved_rate = int(round(((triggered_count - resolved_count) / max(triggered_count, 1)) * 100)) if triggered_count else 0
        rule_rows.append(
            {
                "ruleName": str(rule.get("name") or rule_id).strip(),
                "triggerType": str(rule.get("triggerType") or "").strip(),
                "severity": str(rule.get("severity") or "").strip(),
                "status": str(rule.get("status") or "").strip(),
                "thresholdMinutes": int(rule.get("thresholdMinutes") or 0),
                "cooldownMinutes": int(rule.get("cooldownMinutes") or 0),
                "triggeredCount": triggered_count,
                "notifiedCount": notified_count,
                "resolvedCount": resolved_count,
                "deliveryErrorCount": delivery_error_count,
                "unresolvedRate": unresolved_rate,
            }
        )
        verdict = "stabilizing" if unresolved_rate <= 20 and delivery_error_count == 0 else ("watch" if unresolved_rate <= 60 else "needs_attention")
        tuning_rows.append(
            {
                "ruleId": rule_id,
                "ruleName": str(rule.get("name") or rule_id).strip(),
                "source": "automation",
                "thresholdMinutes": int(rule.get("thresholdMinutes") or 0),
                "cooldownMinutes": int(rule.get("cooldownMinutes") or 0),
                "unresolvedRate": unresolved_rate,
                "atAgo": format_age(parse_iso(rule.get("updatedAt") or rule.get("createdAt")), now) if parse_iso(rule.get("updatedAt") or rule.get("createdAt")) else "未知时间",
                "verdict": verdict,
            }
        )
        if unresolved_rate >= 40 or delivery_error_count > 0:
            rule_suggestions.append(
                {
                    "title": str(rule.get("name") or rule_id).strip(),
                    "detail": f"命中 {triggered_count} 次，未关闭率 {unresolved_rate}% ，投递失败 {delivery_error_count} 次。",
                    "action": {
                        "type": "tune_rule",
                        "label": "调优规则",
                        "ruleId": rule_id,
                        "ruleName": str(rule.get("name") or rule_id).strip(),
                        "thresholdMinutes": int(rule.get("thresholdMinutes") or 0),
                        "cooldownMinutes": int(rule.get("cooldownMinutes") or 0),
                    },
                }
            )
        if delivery_error_count > 0:
            remediation_actions.append(
                {
                    "type": "tune_rule",
                    "ruleId": rule_id,
                    "ruleName": str(rule.get("name") or rule_id).strip(),
                    "thresholdMinutes": int(rule.get("thresholdMinutes") or 0),
                    "cooldownMinutes": int(rule.get("cooldownMinutes") or 0),
                }
            )

    for channel in channel_records:
        if int(channel.get("errors") or 0) <= 0:
            continue
        remediation_actions.append(
            {
                "type": "disable_channel",
                "channelId": str(channel.get("id") or "").strip(),
                "channelName": str(channel.get("name") or channel.get("id") or "").strip(),
            }
        )

    tuning_summary = {
        "total": len(tuning_rows),
        "autoCount": sum(1 for item in tuning_rows if item["verdict"] == "stabilizing"),
        "manualCount": sum(1 for item in tuning_rows if item["verdict"] == "needs_attention"),
        "watchCount": sum(1 for item in tuning_rows if item["verdict"] == "watch"),
    }

    recommendation_preview = build_management_recommendations_preview_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    recommendations = safe_list(
        (recommendation_preview.get("recommendationReview") if isinstance(recommendation_preview.get("recommendationReview"), dict) else {}).get("bundlePriorityQueue")
    )
    if not recommendations:
        recommendations = deepcopy(rule_suggestions[:3])
    placeholder = build_management_automation_placeholder(openclaw_dir, management_metadata=management_metadata)
    placeholder = placeholder if isinstance(placeholder, dict) else {}

    return {
        "summary": {
            "activeRules": active_rule_count,
            "activeChannels": active_channel_count,
            "healthyChannels": healthy_channel_count,
            "openAlerts": open_alert_count,
            "notifiedAlerts": notified_alert_count,
            "deliverySuccessRate": delivery_success_rate,
            "attemptCount": delivery_attempt_count,
            "successCount": delivery_success_count,
            "errorCount": delivery_error_count,
        },
        "mode": deepcopy(placeholder.get("mode") if isinstance(placeholder.get("mode"), dict) else {}),
        "engine": deepcopy(load_automation_engine_status(openclaw_dir)),
        "rules": deepcopy(rules),
        "channels": deepcopy(channel_records),
        "customerChannels": deepcopy(customer_channels),
        "alerts": deepcopy(alerts),
        "deliveryAnalytics": {
            "attemptCount": delivery_attempt_count,
            "successCount": delivery_success_count,
            "errorCount": delivery_error_count,
            "successRate": delivery_success_rate,
            "failingChannels": sum(1 for channel in channel_records if int(channel.get("errors") or 0) > 0),
            "latestFailures": deepcopy(failure_rows[:12]),
            "channels": deepcopy(channel_records[:5]),
            "failureReasons": deepcopy(delivery_failure_reasons),
            "trend": deepcopy(delivery_trend),
        },
        "recommendations": deepcopy(recommendations),
        "ruleEffectiveness": {
            "rows": deepcopy(rule_rows),
            "suggestions": deepcopy(rule_suggestions[:6]),
        },
        "tuningReview": {
            "summary": tuning_summary,
            "rows": deepcopy(tuning_rows[:8]),
        },
        "remediation": {
            "count": len(remediation_actions),
            "actions": deepcopy(remediation_actions[:6]),
        },
        "companyAutoOperation": deepcopy(placeholder.get("companyAutoOperation") if isinstance(placeholder.get("companyAutoOperation"), dict) else {}),
        "memorySystem": deepcopy(memory_system_status_payload(management_metadata, agents=agents, teams=teams)),
    }


def build_management_automation_summary_snapshot(openclaw_dir, config=None):
    payload = _build_management_automation_direct_payload(openclaw_dir, config=config)
    return {
        "automation": {
            "summary": deepcopy(payload.get("summary") if isinstance(payload.get("summary"), dict) else {}),
            "mode": deepcopy(payload.get("mode") if isinstance(payload.get("mode"), dict) else {}),
            "engine": deepcopy(payload.get("engine") if isinstance(payload.get("engine"), dict) else {}),
        },
    }


def build_management_automation_rules_snapshot(openclaw_dir, config=None):
    payload = _build_management_automation_direct_payload(openclaw_dir, config=config)
    return {
        "automation": {
            "summary": deepcopy(payload.get("summary") if isinstance(payload.get("summary"), dict) else {}),
            "mode": deepcopy(payload.get("mode") if isinstance(payload.get("mode"), dict) else {}),
            "engine": deepcopy(payload.get("engine") if isinstance(payload.get("engine"), dict) else {}),
            "rules": deepcopy(safe_list(payload.get("rules"))),
            "channels": deepcopy(safe_list(payload.get("channels"))),
            "companyAutoOperation": deepcopy(payload.get("companyAutoOperation") if isinstance(payload.get("companyAutoOperation"), dict) else {}),
            "memorySystem": deepcopy(payload.get("memorySystem") if isinstance(payload.get("memorySystem"), dict) else {}),
            "ruleEffectiveness": deepcopy(payload.get("ruleEffectiveness") if isinstance(payload.get("ruleEffectiveness"), dict) else {}),
        },
    }


def build_management_automation_delivery_snapshot(openclaw_dir, config=None):
    payload = _build_management_automation_direct_payload(openclaw_dir, config=config)
    return {
        "automation": {
            "summary": deepcopy(payload.get("summary") if isinstance(payload.get("summary"), dict) else {}),
            "channels": deepcopy(safe_list(payload.get("channels"))),
            "customerChannels": deepcopy(payload.get("customerChannels") if isinstance(payload.get("customerChannels"), dict) else {}),
            "deliveryAnalytics": deepcopy(payload.get("deliveryAnalytics") if isinstance(payload.get("deliveryAnalytics"), dict) else {}),
            "recommendations": deepcopy(safe_list(payload.get("recommendations"))),
            "ruleEffectiveness": deepcopy(payload.get("ruleEffectiveness") if isinstance(payload.get("ruleEffectiveness"), dict) else {}),
            "tuningReview": deepcopy(payload.get("tuningReview") if isinstance(payload.get("tuningReview"), dict) else {}),
            "remediation": deepcopy(payload.get("remediation") if isinstance(payload.get("remediation"), dict) else {}),
        },
    }


def build_management_automation_alerts_snapshot(openclaw_dir, config=None):
    payload = _build_management_automation_direct_payload(openclaw_dir, config=config)
    delivery_analytics = payload.get("deliveryAnalytics") if isinstance(payload.get("deliveryAnalytics"), dict) else {}
    return {
        "automation": {
            "summary": deepcopy(payload.get("summary") if isinstance(payload.get("summary"), dict) else {}),
            "alerts": deepcopy(safe_list(payload.get("alerts"))),
            "deliveryAnalytics": {
                "latestFailures": deepcopy(safe_list(delivery_analytics.get("latestFailures"))),
            },
        },
    }


def build_management_insights_health_summary_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        direct_task_state = (
            task_snapshot
            if isinstance(task_snapshot, dict) and isinstance(task_snapshot.get("taskIndex"), list)
            else _load_agent_direct_task_state_snapshot(openclaw_dir)
        )
        task_index = safe_list(direct_task_state.get("taskIndex"))
        task_counts_by_agent = direct_task_state.get("taskCountsByAgent") if isinstance(direct_task_state.get("taskCountsByAgent"), dict) else {}
        blocked_counts_by_agent = direct_task_state.get("blockedCountsByAgent") if isinstance(direct_task_state.get("blockedCountsByAgent"), dict) else {}
        recent_completed_by_agent = Counter()
        completed_by_agent = Counter()
        for task in task_index:
            if str(task.get("state") or "").lower() not in TERMINAL_STATES:
                continue
            agent_id = str(task.get("currentAgent") or "").strip()
            if not agent_id:
                continue
            completed_by_agent[agent_id] += 1
            updated_dt = parse_iso(task.get("updatedAt"))
            if updated_dt and updated_dt >= now - timedelta(days=7):
                recent_completed_by_agent[agent_id] += 1

        score_bands = {"excellent": 0, "stable": 0, "watch": 0, "critical": 0}
        score_total = 0
        score_count = 0
        for agent in safe_list(load_agents(config)):
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            active = int(task_counts_by_agent.get(agent_id) or 0)
            blocked = int(blocked_counts_by_agent.get(agent_id) or 0)
            completed = recent_completed_by_agent[agent_id] or completed_by_agent[agent_id]
            throughput_base = max(active + blocked + completed, 1)
            completion_rate = round((completed / throughput_base) * 100)
            block_rate = round((blocked / throughput_base) * 100)
            latency_score = 72
            completion_score = min(100, 50 + completion_rate)
            block_score = max(24, 100 - block_rate)
            score = round(latency_score * 0.25 + completion_score * 0.45 + block_score * 0.30)
            if score >= 85:
                band = "excellent"
            elif score >= 70:
                band = "stable"
            elif score >= 55:
                band = "watch"
            else:
                band = "critical"
            score_bands[band] += 1
            score_total += score
            score_count += 1

        return {
            "agentHealth": {
                "summary": {
                    "averageScore": round(score_total / score_count) if score_count else 0,
                    "excellent": score_bands["excellent"],
                    "stable": score_bands["stable"],
                    "watch": score_bands["watch"],
                    "critical": score_bands["critical"],
                },
            },
        }

    return cached_payload(
        ("management-direct-insights-health-summary", str(openclaw_dir)),
        10.0,
        build,
    )


def build_management_insights_health_agents_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        direct_task_state = _load_agent_direct_task_state_snapshot(openclaw_dir)
        task_index = safe_list(direct_task_state.get("taskIndex"))
        task_counts_by_agent = direct_task_state.get("taskCountsByAgent") if isinstance(direct_task_state.get("taskCountsByAgent"), dict) else {}
        blocked_counts_by_agent = direct_task_state.get("blockedCountsByAgent") if isinstance(direct_task_state.get("blockedCountsByAgent"), dict) else {}
        recent_completed_by_agent = Counter()
        completed_by_agent = Counter()
        for task in task_index:
            if str(task.get("state") or "").lower() not in TERMINAL_STATES:
                continue
            agent_id = str(task.get("currentAgent") or "").strip()
            if not agent_id:
                continue
            completed_by_agent[agent_id] += 1
            updated_dt = parse_iso(task.get("updatedAt"))
            if updated_dt and updated_dt >= now - timedelta(days=7):
                recent_completed_by_agent[agent_id] += 1
        cards = []
        for agent in safe_list(load_agents(config)):
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            latency_samples = _svc().agent_response_latency_samples(openclaw_dir, agent_id)
            avg_latency_seconds = round(sum(latency_samples) / len(latency_samples), 1) if latency_samples else 0.0
            active = int(task_counts_by_agent.get(agent_id) or 0)
            blocked = int(blocked_counts_by_agent.get(agent_id) or 0)
            completed = recent_completed_by_agent[agent_id] or completed_by_agent[agent_id]
            throughput_base = max(active + blocked + completed, 1)
            completion_rate = round((completed / throughput_base) * 100)
            block_rate = round((blocked / throughput_base) * 100)
            if not latency_samples:
                latency_score = 72
            elif avg_latency_seconds <= 90:
                latency_score = 96
            elif avg_latency_seconds <= 240:
                latency_score = 86
            elif avg_latency_seconds <= 480:
                latency_score = 70
            else:
                latency_score = 52
            completion_score = min(100, 50 + completion_rate)
            block_score = max(24, 100 - block_rate)
            score = round(latency_score * 0.25 + completion_score * 0.45 + block_score * 0.30)
            if score >= 85:
                band = "excellent"
            elif score >= 70:
                band = "stable"
            elif score >= 55:
                band = "watch"
            else:
                band = "critical"
            cards.append(
                {
                    "id": agent_id,
                    "title": str(agent.get("title") or agent_id).strip(),
                    "score": score,
                    "band": band,
                    "completionRate": completion_rate,
                    "blockRate": block_rate,
                    "avgResponseSeconds": avg_latency_seconds,
                }
            )
        cards.sort(key=lambda item: (-item["score"], item["title"]))
        return {
            "agentHealth": {
                "agents": cards,
            },
        }

    return cached_payload(
        ("management-direct-insights-health-agents", str(openclaw_dir)),
        10.0,
        build,
    )


def build_management_insights_conversations_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_conversation_data=True,
        )
        return {
            "conversationQuality": compute_conversation_quality_data(
                openclaw_dir,
                context["conversationData"],
                context["now"],
            ),
        }

    return cached_payload(
        ("management-direct-insights-conversations", str(openclaw_dir)),
        10.0,
        build,
    )


def build_management_reports_overview_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_activity_snapshot=True,
        )
        task_index = context["taskIndex"]
        deliverables = _build_management_task_deliverables(task_index)
        health_data = compute_agent_health_data(
            openclaw_dir,
            context["agents"],
            task_index,
            deliverables,
            context["now"],
        )
        reports = build_operational_reports(
            task_index,
            safe_list((context["activitySnapshot"] if isinstance(context["activitySnapshot"], dict) else {}).get("relays")),
            safe_list((context["activitySnapshot"] if isinstance(context["activitySnapshot"], dict) else {}).get("events")),
            safe_list(store_list_management_runs(openclaw_dir, limit=48)),
            health_data,
            context["now"],
        )
        return {
            "reports": deepcopy(reports if isinstance(reports, dict) else {}),
        }

    return cached_payload(
        ("management-direct-reports-overview", str(openclaw_dir)),
        5.0,
        build,
    )


def build_management_reports_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        context = _build_management_runtime_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_conversation_data=True,
            include_activity_snapshot=True,
        )
        task_index = context["taskIndex"]
        runs = build_management_runs_data(
            openclaw_dir,
            task_index,
            context["conversationData"],
            _build_management_task_deliverables(task_index),
            context["now"],
            skills_data=load_skills_catalog(openclaw_dir),
            limit=48,
        )
        deliverables = _build_management_task_deliverables(task_index)
        health_data = compute_agent_health_data(
            openclaw_dir,
            context["agents"],
            task_index,
            deliverables,
            context["now"],
        )
        conversation_quality = compute_conversation_quality_data(
            openclaw_dir,
            context["conversationData"],
            context["now"],
        )
        reports = build_operational_reports(
            task_index,
            safe_list((context["activitySnapshot"] if isinstance(context["activitySnapshot"], dict) else {}).get("relays")),
            safe_list((context["activitySnapshot"] if isinstance(context["activitySnapshot"], dict) else {}).get("events")),
            runs,
            health_data,
            context["now"],
        )
        summary_payload = build_management_summary_snapshot(
            openclaw_dir,
            config=context["config"],
            now=context["now"],
            task_snapshot=context["taskSnapshot"],
        )
        automation = _build_management_automation_snapshot(openclaw_dir, config=context["config"])
        return {
            "summary": deepcopy(summary_payload.get("summary") if isinstance(summary_payload.get("summary"), dict) else {}),
            "runs": [
                deepcopy({key: value for key, value in item.items() if key != "meta"})
                for item in runs
                if isinstance(item, dict)
            ],
            "reports": deepcopy(reports if isinstance(reports, dict) else {}),
            "agentHealth": deepcopy(health_data if isinstance(health_data, dict) else {}),
            "conversationQuality": deepcopy(conversation_quality if isinstance(conversation_quality, dict) else {}),
            "automation": {
                "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
            },
        }

    return cached_payload(
        ("management-direct-reports", str(openclaw_dir)),
        5.0,
        build,
    )


def build_management_run_snapshot(openclaw_dir, run_id, config=None, now=None, task_snapshot=None):
    from backend.adapters.storage.dashboard import get_management_run_record as store_get_management_run_record

    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        raise RuntimeError("需要 runId。")

    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    run = store_get_management_run_record(openclaw_dir, normalized_run_id)
    if not run:
        raise RuntimeError("Run 不存在。")
    skills_data = load_skills_catalog(openclaw_dir)
    pack_map = workflow_pack_map_from_skills_payload(skills_data if isinstance(skills_data, dict) else {})
    hydrated_run = hydrate_management_run_pack_context(run, pack_map)
    hydrated_run.pop("meta", None)
    task_record = _svc().store_get_task_record(openclaw_dir, hydrated_run.get("linkedTaskId"))
    team_record = next(
        (
            item
            for item in safe_list(store_list_agent_teams(openclaw_dir))
            if str(item.get("id") or "").strip() == str(hydrated_run.get("linkedTeamId") or "").strip()
        ),
        {},
    )
    conversation_data = _load_dashboard_conversation_catalog(openclaw_dir, config=config)
    session_record = next(
        (
            item
            for item in safe_list(conversation_data.get("sessions"))
            if str(item.get("key") or "").strip() == str(hydrated_run.get("linkedSessionKey") or "").strip()
        ),
        {},
    )
    linked_task = compact_task_reference(task_record) if isinstance(task_record, dict) else {}
    linked_team = compact_task_team_reference(team_record) if isinstance(team_record, dict) else {}
    linked_session = deepcopy(session_record) if isinstance(session_record, dict) else {}
    deliverables = []
    if isinstance(task_record, dict):
        task_state = str(task_record.get("state") or task_record.get("status") or "").strip().lower()
        output = str(task_record.get("output") or "").strip()
        if task_state in TERMINAL_STATES or output:
            deliverables.append(
                {
                    "id": str(task_record.get("id") or "").strip(),
                    "title": str(task_record.get("title") or task_record.get("id") or "Untitled Task").strip(),
                    "state": str(task_record.get("state") or task_record.get("status") or "").strip(),
                    "status": "completed" if task_state in TERMINAL_STATES else "review",
                    "statusLabel": str(task_record.get("state") or task_record.get("status") or "").strip(),
                    "owner": str(task_record.get("owner") or "").strip(),
                    "updatedAt": str(task_record.get("updatedAt") or "").strip(),
                    "updatedAgo": str(task_record.get("updatedAgo") or "").strip(),
                    "summary": str(task_record.get("resultSummary") or task_record.get("currentUpdate") or "").strip(),
                    "output": output,
                    "sourceTask": str(task_record.get("id") or "").strip(),
                    "type": infer_deliverable_type(output),
                }
            )
    for artifact in safe_list(hydrated_run.get("artifacts")):
        artifact_deliverable = artifact_deliverable_payload(artifact, hydrated_run, now=now)
        if artifact_deliverable.get("id"):
            deliverables.append(artifact_deliverable)
    hydrated_run["linkedTask"] = linked_task
    hydrated_run["linkedTeam"] = linked_team
    hydrated_run["linkedSession"] = linked_session
    hydrated_run["deliverables"] = deliverables
    hydrated_run["deliverableCount"] = len(deliverables)
    hydrated_run["deliverable"] = deliverables[0] if deliverables else {}
    hydrated_run["teamOwnership"] = build_team_ownership_payload(
        {
            str(item.get("id") or "").strip(): item
            for item in safe_list(store_list_agent_teams(openclaw_dir))
            if str(item.get("id") or "").strip()
        },
        execution_team_id=str(hydrated_run.get("linkedTeamId") or "").strip() or str((linked_team or {}).get("id") or "").strip(),
        recommended_team_id=str((hydrated_run.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
        mode=str((hydrated_run.get("linkedPack") or {}).get("mode") or "").strip(),
        source="run",
    )
    alerts = safe_list(hydrated_run.get("alerts"))
    if isinstance(hydrated_run.get("gateSummary"), dict) and hydrated_run["gateSummary"].get("blocked"):
        alerts.append({"severity": "warning", "title": f"{hydrated_run['gateSummary']['blocked']} review gates blocked"})
    if str((hydrated_run.get("releaseAutomation") or {}).get("status") or "").strip().lower() == "error":
        alerts.append({"severity": "critical", "title": "Release automation needs attention"})
    hydrated_run["alerts"] = alerts
    return deepcopy({key: value for key, value in hydrated_run.items() if key != "meta"})


def build_shell_workspace_context(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    metadata = load_project_metadata(openclaw_dir, config=config)
    agent_overrides = agent_runtime_overrides(metadata)
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    raw_agents = safe_list(task_snapshot.get("agents"))
    router_agent_id = str(task_snapshot.get("routerAgentId") or get_router_agent_id(config)).strip()
    task_index = safe_list(task_snapshot.get("taskIndex"))

    task_counts_by_agent = Counter()
    blocked_counts_by_agent = Counter()
    latest_focus_by_agent = {}
    latest_focus_dt_by_agent = {}
    relay_counter = Counter()
    relay_last_at = {}
    handoffs_24h_by_agent = Counter()
    agent_signals = defaultdict(list)
    global_events = []
    active_tasks = []
    recent_threshold = now - timedelta(hours=24)

    for task in task_index:
        if not isinstance(task, dict):
            continue
        current_agent = str(task.get("currentAgent") or "").strip()
        active = bool(task.get("active"))
        blocked = bool(task.get("blocked"))
        signal_dt = parse_iso(task.get("activityAt") or task.get("updatedAt"))
        if active:
            active_tasks.append(task)
        if current_agent and active:
            task_counts_by_agent[current_agent] += 1
            if blocked:
                blocked_counts_by_agent[current_agent] += 1
            if current_agent not in latest_focus_dt_by_agent or (
                signal_dt and signal_dt >= latest_focus_dt_by_agent[current_agent]
            ):
                latest_focus_dt_by_agent[current_agent] = signal_dt
                latest_focus_by_agent[current_agent] = (
                    str(task.get("currentUpdate") or "").strip()
                    or str(task.get("title") or "").strip()
                )

        for replay_event in safe_list(task.get("replay")):
            if not isinstance(replay_event, dict):
                continue
            global_events.append(
                {
                    "type": replay_event.get("kind", ""),
                    "at": replay_event.get("at", ""),
                    "title": task.get("title", ""),
                    "taskId": task.get("id", ""),
                    "headline": replay_event.get("headline", ""),
                    "detail": replay_event.get("detail", ""),
                }
            )

            if replay_event.get("kind") == "handoff":
                at = parse_iso(replay_event.get("at"))
                if at and at >= recent_threshold:
                    edge = (
                        str(replay_event.get("actorLabel") or "").strip(),
                        str(replay_event.get("targetLabel") or "").strip(),
                    )
                    relay_counter[edge] += 1
                    relay_last_at[edge] = max(at, relay_last_at.get(edge, at))
                    if replay_event.get("actorId"):
                        handoffs_24h_by_agent[replay_event["actorId"]] += 1
                    if replay_event.get("targetId"):
                        handoffs_24h_by_agent[replay_event["targetId"]] += 1
                if replay_event.get("actorId"):
                    agent_signals[replay_event["actorId"]].append(
                        {
                            "title": task.get("title", ""),
                            "taskId": task.get("id", ""),
                            "meta": replay_event.get("atAgo", ""),
                            "detail": f"移交给 {replay_event.get('targetLabel', '')} · {replay_event.get('detail', '')}".strip(" ·"),
                        }
                    )
                if replay_event.get("targetId"):
                    agent_signals[replay_event["targetId"]].append(
                        {
                            "title": task.get("title", ""),
                            "taskId": task.get("id", ""),
                            "meta": replay_event.get("atAgo", ""),
                            "detail": f"从 {replay_event.get('actorLabel', '')} 接到任务 · {replay_event.get('detail', '')}".strip(" ·"),
                        }
                    )
            elif replay_event.get("actorId"):
                agent_signals[replay_event["actorId"]].append(
                    {
                        "title": task.get("title", ""),
                        "taskId": task.get("id", ""),
                        "meta": replay_event.get("atAgo", ""),
                        "detail": replay_event.get("detail", ""),
                    }
                )

    active_tasks.sort(
        key=lambda item: parse_iso(item.get("activityAt") or item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    global_events.sort(
        key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )

    deliverables = build_deliverables_snapshot(
        openclaw_dir,
        config=config,
        now=now,
        task_snapshot={"deliverables": _build_management_task_deliverables(task_index)},
    )

    agent_cards = []
    active_agent_count = 0
    for agent in raw_agents:
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        workspace = Path(agent.get("workspace", "")) if agent.get("workspace") else openclaw_dir / f"workspace-{agent_id}"
        workspace_dt = workspace_last_activity(workspace)
        session_dt = session_last_activity(openclaw_dir, agent_id)
        signal_dt = latest_focus_dt_by_agent.get(agent_id)
        last_seen = max([dt for dt in (workspace_dt, session_dt, signal_dt) if dt is not None], default=None)
        active_task_cards = [
            {
                "id": task.get("id", ""),
                "title": task.get("title", ""),
                "state": task.get("state", ""),
                "updatedAgo": task.get("updatedAgo", ""),
            }
            for task in active_tasks
            if str(task.get("currentAgent") or "").strip() == agent_id
        ]
        status = status_for_agent(
            task_counts_by_agent[agent_id],
            blocked_counts_by_agent[agent_id],
            signal_dt,
            last_seen,
            now,
        )
        override = agent_overrides.get(agent_id) if isinstance(agent_overrides.get(agent_id), dict) else {}
        runtime_profile = merged_agent_runtime_profile(agent_id, override=override)
        is_paused = bool(override.get("paused"))
        if is_paused:
            status = "paused"
        work_guard = work_guard_for_agent(
            task_counts_by_agent[agent_id],
            blocked_counts_by_agent[agent_id],
            signal_dt,
            last_seen,
            workspace_dt,
            session_dt,
            now,
            paused=is_paused,
        )
        if status in {"active", "waiting", "blocked"}:
            active_agent_count += 1
        identity = agent.get("identity") if isinstance(agent.get("identity"), dict) else {}
        agent_cards.append(
            {
                "id": agent_id,
                "name": identity.get("name", agent_id),
                "humanName": runtime_profile.get("humanName", "") or identity.get("name", agent_id),
                "title": str(agent.get("title") or agent_id).strip(),
                "model": agent.get("model", "default"),
                "role": runtime_profile.get("role", ""),
                "roleLabel": runtime_profile.get("roleLabel", ""),
                "jobTitle": runtime_profile.get("jobTitle", ""),
                "workingStyle": runtime_profile.get("workingStyle", ""),
                "voiceReplyVoice": runtime_profile.get("voiceReplyVoice", ""),
                "voiceReplySpeed": runtime_profile.get("voiceReplySpeed", 1.0),
                "skills": safe_list(runtime_profile.get("skills")),
                "skillCount": int(runtime_profile.get("skillCount") or 0),
                "status": status,
                "paused": is_paused,
                "activeTasks": task_counts_by_agent[agent_id],
                "blockedTasks": blocked_counts_by_agent[agent_id],
                "focus": latest_focus_by_agent.get(agent_id, ""),
                "currentTaskId": active_task_cards[0]["id"] if active_task_cards else "",
                "currentTaskTitle": active_task_cards[0]["title"] if active_task_cards else "",
                "lastSeenAgo": format_age(last_seen, now),
                "lastSeenAt": last_seen.isoformat().replace("+00:00", "Z") if last_seen else "",
                "workspaceLastSeenAgo": format_age(workspace_dt, now),
                "sessionLastSeenAgo": format_age(session_dt, now),
                "handoffs24h": handoffs_24h_by_agent[agent_id],
                "activeTaskCards": active_task_cards[:6],
                "recentSignals": agent_signals[agent_id][:8],
                "workGuard": work_guard,
                "runtimeOverride": override,
            }
        )

    relays = []
    for edge, count in relay_counter.most_common(10):
        relays.append(
            {
                "from": edge[0],
                "to": edge[1],
                "count": count,
                "lastAt": relay_last_at[edge].isoformat().replace("+00:00", "Z"),
                "lastAgo": format_age(relay_last_at[edge], now),
            }
        )

    completed_today = sum(
        1
        for task in task_index
        if str(task.get("state", "")).lower() == "done"
        and (parse_iso(task.get("updatedAt")) or now) >= now - timedelta(days=1)
    )
    blocked_total = sum(1 for task in active_tasks if task.get("blocked"))
    needs_attention_agents = sum(1 for agent in agent_cards if (agent.get("workGuard") or {}).get("needsAttention"))
    stalled_agents = sum(1 for agent in agent_cards if (agent.get("workGuard") or {}).get("reason") == "stale_progress")
    signal_count = sum(
        1
        for event in global_events
        if parse_iso(event.get("at")) and parse_iso(event.get("at")) >= now - timedelta(hours=1)
    )
    agent_teams = build_agent_team_data(openclaw_dir, agent_cards, task_index, now)

    return {
        "generatedAt": now.isoformat().replace("+00:00", "Z"),
        "openclawDir": str(openclaw_dir),
        "routerAgentId": router_agent_id,
        "agents": agent_cards,
        "agentTeams": agent_teams,
        "taskIndex": [compact_task_dashboard_payload(task) for task in task_index][:72],
        "taskRecords": task_index,
        "deliverables": deliverables[:24],
        "events": global_events[:42],
        "relays": relays,
        "metrics": {
            "activeTasks": len(active_tasks),
            "activeAgents": active_agent_count,
            "blockedTasks": blocked_total,
            "completedToday": completed_today,
            "handoffs24h": sum(item["count"] for item in relays),
            "signals1h": signal_count,
            "needsAttentionAgents": needs_attention_agents,
            "stalledAgents": stalled_agents,
        },
    }


def build_chat_catalog_snapshot(openclaw_dir, context=None, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    context = context if isinstance(context, dict) else {}
    direct_task_state = _load_agent_direct_task_state_snapshot(openclaw_dir)
    skills_data = load_skills_catalog(openclaw_dir, config=config)
    management_runs = safe_list(
        context.get("managementRuns")
        if isinstance(context.get("managementRuns"), list)
        else store_list_management_runs(openclaw_dir, limit=128)
    )
    agent_cards = safe_list(context.get("agents")) or _load_chat_agent_presence_snapshot(
        openclaw_dir,
        config=config,
    )
    compact_task_index = safe_list(context.get("taskIndex")) or safe_list(direct_task_state.get("taskIndex"))
    deliverables = safe_list(context.get("deliverables")) or safe_list(direct_task_state.get("deliverables"))
    agent_teams_data = (
        context.get("agentTeams")
        if isinstance(context.get("agentTeams"), dict)
        else _load_chat_team_snapshot(openclaw_dir)
    )
    return build_chat_data(
        openclaw_dir,
        agent_cards,
        compact_task_index,
        deliverables,
        {"runs": management_runs},
        now,
        agent_teams_data=agent_teams_data,
        skills_data=skills_data,
    )


def build_chat_catalog_page_snapshot(openclaw_dir, page=1, page_size=24, status="", query_text="", config=None, now=None):
    from backend.application.services.chat_core import build_chat_catalog_page_snapshot as build_chat_catalog_page_snapshot_impl

    return deepcopy(
        build_chat_catalog_page_snapshot_impl(
            openclaw_dir,
            page=page,
            page_size=page_size,
            status=status,
            query_text=query_text,
            config=config,
            now=now,
        )
    )


def compact_dashboard_bootstrap_payload(data):
    payload = data if isinstance(data, dict) else {}
    return {
        "generatedAt": str(payload.get("generatedAt") or "").strip(),
        "generatedAgo": str(payload.get("generatedAgo") or "").strip(),
        "signature": str(payload.get("signature") or "").strip(),
        "routerAgentId": str(payload.get("routerAgentId") or "").strip(),
        "runtime": deepcopy(payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}),
    }


def build_action_dashboard_payload(data, sections=None):
    payload = compact_dashboard_bootstrap_payload(data)
    source = data if isinstance(data, dict) else {}
    for section in clean_unique_strings(sections or []):
        if section == "themes":
            payload["theme"] = deepcopy(source.get("theme") if isinstance(source.get("theme"), dict) else {})
            payload["themeCatalog"] = deepcopy(safe_list(source.get("themeCatalog")))
            payload["themeHistory"] = deepcopy(safe_list(source.get("themeHistory")))
            payload["themeWorkforce"] = deepcopy(source.get("themeWorkforce") if isinstance(source.get("themeWorkforce"), dict) else {})
            continue
        if section == "metrics":
            payload["metrics"] = deepcopy(source.get("metrics") if isinstance(source.get("metrics"), dict) else {})
            continue
        if section == "managementBootstrap":
            payload["management"] = compact_management_bootstrap_payload(source.get("management"))
            continue
        if section == "openclawBootstrap":
            payload["openclaw"] = compact_openclaw_bootstrap_payload(source.get("openclaw"))
            continue
        if section not in {
            "agents",
            "agentTeams",
            "chat",
            "contextHub",
            "deliverables",
            "management",
            "memorySystem",
            "openclaw",
            "orchestration",
            "platform",
            "skills",
            "taskIndex",
        }:
            continue
        value = source.get(section)
        if isinstance(value, dict):
            payload[section] = deepcopy(value)
        elif isinstance(value, list):
            payload[section] = deepcopy(safe_list(value))
        elif value is not None:
            payload[section] = deepcopy(value)
    return payload


def _select_management_detail_payload(management_data, section="", automation_tab="", insights_tab=""):
    management = management_data if isinstance(management_data, dict) else {}
    normalized_section = str(section or "overview").strip().lower() or "overview"
    normalized_automation_tab = str(automation_tab or "rules").strip().lower() or "rules"
    normalized_insights_tab = str(insights_tab or "health").strip().lower() or "health"
    automation = management.get("automation") if isinstance(management.get("automation"), dict) else {}

    if normalized_section == "decision":
        return {
            "summary": deepcopy(management.get("summary") if isinstance(management.get("summary"), dict) else {}),
            "intelligence": deepcopy(management.get("intelligence") if isinstance(management.get("intelligence"), dict) else {}),
            "decisionQuality": deepcopy(management.get("decisionQuality") if isinstance(management.get("decisionQuality"), dict) else {}),
            "decisionSourceReview": deepcopy(management.get("decisionSourceReview") if isinstance(management.get("decisionSourceReview"), dict) else {}),
            "recommendationReview": deepcopy(management.get("recommendationReview") if isinstance(management.get("recommendationReview"), dict) else {}),
        }

    if normalized_section == "automation":
        if normalized_automation_tab == "delivery":
            return {
                "automation": {
                    "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
                    "channels": deepcopy(safe_list(automation.get("channels"))),
                    "customerChannels": deepcopy(automation.get("customerChannels") if isinstance(automation.get("customerChannels"), dict) else {}),
                    "deliveryAnalytics": deepcopy(automation.get("deliveryAnalytics") if isinstance(automation.get("deliveryAnalytics"), dict) else {}),
                    "recommendations": deepcopy(safe_list(automation.get("recommendations"))),
                    "ruleEffectiveness": {
                        "suggestions": deepcopy(
                            safe_list(
                                (automation.get("ruleEffectiveness") if isinstance(automation.get("ruleEffectiveness"), dict) else {}).get("suggestions")
                            )
                        ),
                    },
                    "tuningReview": deepcopy(automation.get("tuningReview") if isinstance(automation.get("tuningReview"), dict) else {}),
                    "remediation": deepcopy(automation.get("remediation") if isinstance(automation.get("remediation"), dict) else {}),
                },
            }
        if normalized_automation_tab == "alerts":
            delivery_analytics = automation.get("deliveryAnalytics") if isinstance(automation.get("deliveryAnalytics"), dict) else {}
            return {
                "automation": {
                    "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
                    "alerts": deepcopy(safe_list(automation.get("alerts"))),
                    "deliveryAnalytics": {
                        "latestFailures": deepcopy(safe_list(delivery_analytics.get("latestFailures"))),
                    },
                },
            }
        return {
            "automation": {
                "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
                "mode": deepcopy(automation.get("mode") if isinstance(automation.get("mode"), dict) else {}),
                "engine": deepcopy(automation.get("engine") if isinstance(automation.get("engine"), dict) else {}),
                "rules": deepcopy(safe_list(automation.get("rules"))),
                "channels": deepcopy(safe_list(automation.get("channels"))),
                "companyAutoOperation": deepcopy(automation.get("companyAutoOperation") if isinstance(automation.get("companyAutoOperation"), dict) else {}),
                "memorySystem": deepcopy(automation.get("memorySystem") if isinstance(automation.get("memorySystem"), dict) else {}),
                "ruleEffectiveness": {
                    "rows": deepcopy(
                        safe_list(
                            (automation.get("ruleEffectiveness") if isinstance(automation.get("ruleEffectiveness"), dict) else {}).get("rows")
                        )
                    ),
                },
            },
        }

    if normalized_section == "insights":
        if normalized_insights_tab == "conversations":
            return {
                "conversationQuality": deepcopy(
                    management.get("conversationQuality") if isinstance(management.get("conversationQuality"), dict) else {}
                ),
            }
        if normalized_insights_tab == "reports":
            return {
                "reports": deepcopy(management.get("reports") if isinstance(management.get("reports"), dict) else {}),
            }
        return {
            "agentHealth": deepcopy(management.get("agentHealth") if isinstance(management.get("agentHealth"), dict) else {}),
        }

    if normalized_section == "reports":
        return {
            "summary": deepcopy(management.get("summary") if isinstance(management.get("summary"), dict) else {}),
            "runs": deepcopy(safe_list(management.get("runs"))),
            "reports": deepcopy(management.get("reports") if isinstance(management.get("reports"), dict) else {}),
            "agentHealth": deepcopy(management.get("agentHealth") if isinstance(management.get("agentHealth"), dict) else {}),
            "conversationQuality": deepcopy(
                management.get("conversationQuality") if isinstance(management.get("conversationQuality"), dict) else {}
            ),
            "automation": {
                "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
            },
        }

    if normalized_section == "alerts":
        delivery_analytics = automation.get("deliveryAnalytics") if isinstance(automation.get("deliveryAnalytics"), dict) else {}
        return {
            "automation": {
                "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
                "alerts": deepcopy(safe_list(automation.get("alerts"))),
                "deliveryAnalytics": {
                    "latestFailures": deepcopy(safe_list(delivery_analytics.get("latestFailures"))),
                },
            },
        }

    return {
        "summary": deepcopy(management.get("summary") if isinstance(management.get("summary"), dict) else {}),
        "runs": deepcopy(safe_list(management.get("runs"))),
        "agentHealth": {
            "summary": deepcopy(
                (management.get("agentHealth") if isinstance(management.get("agentHealth"), dict) else {}).get("summary")
                if isinstance((management.get("agentHealth") if isinstance(management.get("agentHealth"), dict) else {}).get("summary"), dict)
                else {}
            ),
        },
        "automation": {
            "summary": deepcopy(automation.get("summary") if isinstance(automation.get("summary"), dict) else {}),
        },
    }


def build_orchestration_task_index_snapshot(openclaw_dir, config=None, now=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    now = now or now_utc()
    tasks = merge_tasks(openclaw_dir, config)
    cached_payload(
        ("planning-backfill", str(openclaw_dir)),
        30,
        lambda: backfill_planning_bundles(openclaw_dir, config, tasks),
    )
    cached_payload(
        ("routing-intelligence-backfill", str(openclaw_dir)),
        60,
        lambda: backfill_task_intelligence(openclaw_dir, config, tasks),
    )
    agent_labels, label_to_agent_ids = build_label_maps(agents, kanban_cfg, router_agent_id)
    bootstrap_agents = [
        {
            "id": str(agent.get("id") or "").strip(),
            "title": agent_labels.get(str(agent.get("id") or "").strip(), str(agent.get("id") or "").strip()),
            "name": ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", ""),
            "role": "",
            "skills": [],
        }
        for agent in agents
        if str(agent.get("id") or "").strip()
    ]
    _metadata, teams = ensure_default_team_mode(openclaw_dir, metadata, bootstrap_agents, router_agent_id)
    team_map = {item.get("id"): item for item in safe_list(teams) if item.get("id")}
    task_thread_map = {}
    for thread in store_list_chat_threads(openclaw_dir, limit=256):
        if not is_managed_task_execution_thread(thread):
            continue
        task_execution = task_execution_meta_for_thread(thread)
        linked_task_id = str(task_execution.get("taskId") or thread.get("linkedTaskId") or "").strip()
        if not linked_task_id:
            continue
        existing_thread = task_thread_map.get(linked_task_id)
        existing_dt = parse_iso((existing_thread or {}).get("updatedAt")) if existing_thread else None
        thread_dt = parse_iso(thread.get("updatedAt"))
        if not existing_thread or (thread_dt and (not existing_dt or thread_dt >= existing_dt)):
            task_thread_map[linked_task_id] = thread

    task_counts_by_agent = Counter()
    blocked_counts_by_agent = Counter()
    latest_focus_by_agent = {}
    latest_focus_dt_by_agent = {}
    relay_counter = Counter()
    relay_last_at = {}
    handoffs_24h_by_agent = Counter()
    agent_signals = defaultdict(list)
    global_events = []
    task_index = []
    active_tasks = []
    deliverables = []
    recent_threshold = now - timedelta(hours=24)
    for task in tasks:
        if is_merged_duplicate_task(task):
            continue
        replay = build_task_replay(task, label_to_agent_ids, now)
        current_agent = current_agent_for_task(task, kanban_cfg, router_agent_id)
        progress_event = latest_progress_event(task, router_agent_id=router_agent_id)
        activity_at = str((progress_event or {}).get("at") or task.get("updatedAt") or "").strip()
        state = str(task.get("state", task.get("status", ""))).lower()
        todo = todo_summary(task)
        todo_items = [
            {
                "title": item.get("title", ""),
                "status": item.get("status", "not-started"),
            }
            for item in task.get("todos", [])
        ]

        task_record = {
            "id": task.get("id"),
            "title": task.get("title", task.get("id", "Untitled Task")),
            "state": task.get("state", task.get("status", "Unknown")),
            "owner": task.get("official", ""),
            "org": task.get("org", ""),
            "currentAgent": current_agent,
            "currentAgentLabel": (
                str(task.get("currentAgentLabel") or "").strip()
                or (
                    str(task.get("targetAgentLabel") or "").strip()
                    if current_agent and current_agent == str(task.get("targetAgentId") or "").strip()
                    else ""
                )
                or agent_labels.get(current_agent, current_agent or task.get("org", "?"))
            ),
            "currentUpdate": task_display_update(task),
            "updatedAt": task.get("updatedAt", ""),
            "updatedAgo": format_age(parse_iso(task.get("updatedAt")), now),
            "activityAt": activity_at,
            "output": task.get("output", ""),
            "outputLabel": Path(str(task.get("output") or "").strip()).name if str(task.get("output") or "").strip() else "",
            "todo": todo,
            "todoItems": todo_items,
            "route": task_route(task),
            "blocked": state == "blocked",
            "active": state not in TERMINAL_STATES,
            "replay": list(reversed(replay[-24:])),
            "targetAgentId": str(task.get("targetAgentId") or "").strip(),
            "targetAgentLabel": str(task.get("targetAgentLabel") or "").strip(),
            "workflowBinding": task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else (
                (task.get("meta") or {}).get("workflowBinding", {}) if isinstance(task.get("meta"), dict) else {}
            ),
            "planningBundle": task.get("planningBundle") if isinstance(task.get("planningBundle"), dict) else (
                (task.get("meta") or {}).get("planningBundle", {}) if isinstance(task.get("meta"), dict) else {}
            ),
            "routeDecision": (task.get("meta") or {}).get("routeDecision", {}) if isinstance(task.get("meta"), dict) else {},
            "resultSummary": task_result_summary(task),
        }
        route_meta = task_record["routeDecision"] if isinstance(task_record.get("routeDecision"), dict) else {}
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = str(team_assignment.get("teamId") or "").strip() or str(route_meta.get("teamId") or "").strip()
        linked_team = team_map.get(team_id) if team_id else None
        task_record["teamId"] = team_id
        task_record["teamLabel"] = (
            str((linked_team or {}).get("name") or "").strip()
            or str(team_assignment.get("teamName") or "").strip()
            or str(route_meta.get("teamName") or "").strip()
        )
        task_record["linkedTeam"] = linked_team
        task_thread = task_thread_map.get(str(task_record.get("id") or "").strip(), {})
        task_thread_meta = task_thread.get("meta") if isinstance(task_thread.get("meta"), dict) else {}
        task_thread_dispatch = (
            task_thread_meta.get("lastDispatch")
            if isinstance(task_thread_meta.get("lastDispatch"), dict)
            else (
                task_thread_meta.get("lastSync")
                if isinstance(task_thread_meta.get("lastSync"), dict)
                else {}
            )
        )
        task_record["teamThreadId"] = str(task_thread.get("id") or "").strip()
        task_record["teamThreadTitle"] = str(task_thread.get("title") or "").strip()
        task_record["teamThreadStatus"] = str(task_thread.get("status") or "").strip()
        task_record["collaboration"] = build_team_collaboration_summary(
            task_thread_dispatch
            if isinstance(task_thread_dispatch, dict) and task_thread_dispatch
            else (route_meta.get("teamDispatch") if isinstance(route_meta.get("teamDispatch"), dict) else {})
        )
        if state in TERMINAL_STATES and (task_record["teamThreadId"] or task_record["teamId"]):
            task_record["collaboration"] = {
                **task_record["collaboration"],
                "status": "healthy",
                "headline": "已完成收口",
                "waitingCount": 0,
                "failureCount": 0,
                "blockerCount": 0,
                "waitingAgentIds": [],
                "failedAgentIds": [],
                "blockerAgentIds": [],
            }
        task_index.append(task_record)
        if state in TERMINAL_STATES or task.get("output"):
            deliverables.append(
                {
                    "id": task_record["id"],
                    "title": task_record["title"],
                    "state": task_record["state"],
                    "status": "completed" if state in TERMINAL_STATES else "review",
                    "statusLabel": task_record["state"],
                    "owner": task_record["owner"],
                    "updatedAt": task_record["updatedAt"],
                    "updatedAgo": task_record["updatedAgo"],
                    "summary": task_record["resultSummary"] or task_record["currentUpdate"],
                    "output": task_record["output"],
                    "sourceTask": task_record["id"],
                    "type": infer_deliverable_type(task_record["output"]),
                }
            )
        if current_agent and task_record["active"]:
            task_counts_by_agent[current_agent] += 1
            if state == "blocked":
                blocked_counts_by_agent[current_agent] += 1
            signal_dt = parse_iso((progress_event or {}).get("at")) or parse_iso(task.get("updatedAt"))
            if current_agent not in latest_focus_dt_by_agent or (
                signal_dt and signal_dt >= latest_focus_dt_by_agent[current_agent]
            ):
                latest_focus_dt_by_agent[current_agent] = signal_dt
                latest_focus_by_agent[current_agent] = task_record["currentUpdate"] or task_record["title"]
        for replay_event in replay:
            global_events.append(
                {
                    "type": replay_event["kind"],
                    "at": replay_event.get("at", ""),
                    "title": task_record["title"],
                    "taskId": task_record["id"],
                    "headline": replay_event["headline"],
                    "detail": replay_event.get("detail", ""),
                }
            )
            if replay_event["kind"] == "handoff":
                at = parse_iso(replay_event.get("at"))
                if at and at >= recent_threshold:
                    edge = (replay_event["actorLabel"], replay_event["targetLabel"])
                    relay_counter[edge] += 1
                    relay_last_at[edge] = max(at, relay_last_at.get(edge, at))
                    if replay_event.get("actorId"):
                        handoffs_24h_by_agent[replay_event["actorId"]] += 1
                    if replay_event.get("targetId"):
                        handoffs_24h_by_agent[replay_event["targetId"]] += 1
                if replay_event.get("actorId"):
                    agent_signals[replay_event["actorId"]].append(
                        {
                            "title": task_record["title"],
                            "taskId": task_record["id"],
                            "meta": replay_event["atAgo"],
                            "detail": f"移交给 {replay_event['targetLabel']} · {replay_event.get('detail', '')}".strip(" ·"),
                        }
                    )
                if replay_event.get("targetId"):
                    agent_signals[replay_event["targetId"]].append(
                        {
                            "title": task_record["title"],
                            "taskId": task_record["id"],
                            "meta": replay_event["atAgo"],
                            "detail": f"从 {replay_event['actorLabel']} 接到任务 · {replay_event.get('detail', '')}".strip(" ·"),
                        }
                    )
            elif replay_event.get("actorId"):
                agent_signals[replay_event["actorId"]].append(
                    {
                        "title": task_record["title"],
                        "taskId": task_record["id"],
                        "meta": replay_event["atAgo"],
                        "detail": replay_event.get("detail", ""),
                    }
                )
        if task_record["active"]:
            active_tasks.append(task_record)
    active_tasks.sort(
        key=lambda item: parse_iso(item.get("activityAt") or item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    task_index.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    global_events.sort(
        key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    deliverables.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    relays = []
    for edge, count in relay_counter.most_common(10):
        relays.append(
            {
                "from": edge[0],
                "to": edge[1],
                "count": count,
                "lastAt": relay_last_at[edge].isoformat().replace("+00:00", "Z"),
                "lastAgo": format_age(relay_last_at[edge], now),
            }
        )
    return {
        "agents": agents,
        "routerAgentId": router_agent_id,
        "taskIndex": task_index,
        "activeTasks": active_tasks,
        "deliverables": deliverables,
        "events": global_events,
        "relays": relays,
        "taskCountsByAgent": dict(task_counts_by_agent),
        "blockedCountsByAgent": dict(blocked_counts_by_agent),
        "latestFocusByAgent": latest_focus_by_agent,
        "latestFocusAtByAgent": {
            agent_id: timestamp.isoformat().replace("+00:00", "Z")
            for agent_id, timestamp in latest_focus_dt_by_agent.items()
            if timestamp
        },
        "handoffs24hByAgent": dict(handoffs_24h_by_agent),
        "agentSignals": dict(agent_signals),
    }


def build_dashboard_operations_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    return deepcopy(
        build_shell_workspace_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
        )
    )


def _load_theme_template_roles(openclaw_dir, theme_name, config=None):
    project_dir = resolve_project_dir(openclaw_dir, config=config)
    if not project_dir:
        return []
    theme_file = Path(project_dir) / "platform" / "config" / "themes" / str(theme_name or "").strip() / "theme.json"
    if not theme_file.exists():
        return []
    try:
        payload = json.loads(theme_file.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return []
    roles = payload.get("roles") if isinstance(payload.get("roles"), dict) else {}
    items = []

    def append_role(key, record, kind="core", department=""):
        if not isinstance(record, dict):
            return
        agent_id = str(record.get("agent_id") or record.get("agentId") or "").strip()
        if not agent_id:
            return
        items.append(
            {
                "id": str(key or agent_id).strip(),
                "kind": kind,
                "department": str(department or "").strip(),
                "agentId": agent_id,
                "title": str(record.get("title") or agent_id).strip(),
                "identityName": str(record.get("identity_name") or record.get("identityName") or "").strip(),
                "description": str(record.get("description") or "").strip(),
                "modelTier": str(record.get("model_tier") or record.get("modelTier") or "").strip(),
            }
        )

    for key, record in roles.items():
        if key == "departments":
            continue
        append_role(key, record, kind="core")
    departments = roles.get("departments") if isinstance(roles.get("departments"), dict) else {}
    for key, record in departments.items():
        append_role(key, record, kind="department", department=key)
    return items


def build_theme_workforce_snapshot(openclaw_dir, config=None, agents=None, teams=None, skills_data=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    theme_name = metadata.get("theme", DEFAULT_THEME_NAME)
    agent_cards = safe_list(agents) if agents is not None else safe_list(build_agent_cards_snapshot(openclaw_dir, config=config))
    team_records = safe_list(teams) if teams is not None else safe_list(store_list_agent_teams(openclaw_dir))
    skills_payload = skills_data if isinstance(skills_data, dict) else load_skills_catalog(openclaw_dir)
    stored_profiles = {
        str(item.get("agentId") or "").strip(): item
        for item in safe_list(store_list_theme_workforce_profiles(openclaw_dir))
        if isinstance(item, dict) and str(item.get("agentId") or "").strip()
    }
    template_roles = _load_theme_template_roles(openclaw_dir, theme_name, config=config)
    team_membership_map = defaultdict(list)
    team_items = []
    for team in team_records:
        if not isinstance(team, dict):
            continue
        team_id = str(team.get("id") or "").strip()
        if not team_id:
            continue
        member_ids = []
        for agent_id in [str(team.get("leadAgentId") or "").strip(), *safe_list(team.get("memberAgentIds"))]:
            normalized_id = str(agent_id or "").strip()
            if normalized_id and normalized_id not in member_ids:
                member_ids.append(normalized_id)
                team_membership_map[normalized_id].append({"id": team_id, "name": str(team.get("name") or team_id).strip()})
        team_items.append(
            {
                "id": team_id,
                "name": str(team.get("name") or team_id).strip(),
                "status": str(team.get("status") or "").strip(),
                "leadAgentId": str(team.get("leadAgentId") or "").strip(),
                "memberCount": len(member_ids),
                "focus": str(team.get("focus") or "").strip(),
                "description": str(team.get("description") or "").strip(),
            }
        )
    skill_entries = safe_list((skills_payload.get("skills") if isinstance(skills_payload, dict) else []))
    skill_options = [
        {
            "value": str(skill.get("slug") or "").strip(),
            "label": str(skill.get("displayName") or skill.get("title") or skill.get("slug") or "").strip(),
            "mode": str(skill.get("mode") or "").strip(),
            "stage": str(skill.get("stage") or "").strip(),
            "status": str(skill.get("status") or "").strip(),
        }
        for skill in skill_entries
        if isinstance(skill, dict) and str(skill.get("slug") or "").strip()
    ]
    skill_labels = {item["value"]: item["label"] for item in skill_options}
    role_options = []
    for value in [
        *[str(item.get("role") or "").strip() for item in agent_cards if isinstance(item, dict)],
        *[str(item.get("role") or "").strip() for item in stored_profiles.values() if isinstance(item, dict)],
    ]:
        if value and value not in role_options:
            role_options.append(value)
    voice_runtime = {}
    voice_options = []
    qwen_voice_labels = {
        "serena": "Serena · 中文温和女声",
        "vivian": "Vivian · 中文明亮女声",
        "uncle_fu": "Uncle_Fu · 中文低沉男声",
        "dylan": "Dylan · 北京男声",
        "eric": "Eric · 成都男声",
        "sohee": "Sohee · 韩语女声",
        "ono_anna": "Ono_Anna · 日语女声",
        "aiden": "Aiden · 英文男声",
        "ryan": "Ryan · 英文动感男声",
    }
    sherpa_voice_labels = {
        "zf_001": "中文女 1 · 温和",
        "zf_002": "中文女 2 · 明亮",
        "zf_003": "中文女 3 · 柔和",
        "zf_004": "中文女 4 · 清晰",
        "zf_005": "中文女 5 · 沉稳",
        "zf_006": "中文女 6 · 轻快",
        "zm_009": "中文男 1 · 稳重",
        "zm_010": "中文男 2 · 自然",
        "zm_011": "中文男 3 · 清晰",
        "zm_012": "中文男 4 · 温厚",
        "zm_013": "中文男 5 · 年轻",
        "zf_017": "中文女 7 · 亲和",
        "zf_018": "中文女 8 · 活泼",
    }
    try:
        from .runtime_core import load_openclaw_voice_workflow_panel_data

        voice_workflow_payload = load_openclaw_voice_workflow_panel_data(openclaw_dir, config=config, metadata=metadata)
        voice_runtime = (
            voice_workflow_payload.get("voiceWorkflow", {}).get("speechRuntime", {})
            if isinstance(voice_workflow_payload, dict)
            else {}
        )
        runtime_voice_labels = voice_runtime.get("voiceLabels") if isinstance(voice_runtime.get("voiceLabels"), dict) else {}
        custom_voice_labels = {}
        for agent in agent_cards:
            if not isinstance(agent, dict):
                continue
            agent_id = str(agent.get("id") or "").strip()
            if not agent_id:
                continue
            label = str(agent.get("humanName") or agent.get("name") or agent_id).strip() or agent_id
            custom_voice_labels[f"custom:{agent_id}"] = f"{label} · 自定义"
        for agent_id, item in stored_profiles.items():
            if not isinstance(item, dict):
                continue
            label = str(item.get("humanName") or custom_voice_labels.get(f"custom:{agent_id}", "") or agent_id).strip()
            custom_voice_labels[f"custom:{agent_id}"] = f"{label} · 自定义"
        voice_options = []
        for item in clean_unique_strings(voice_runtime.get("availableVoices") or []):
            normalized_value = str(item or "").strip()
            if not normalized_value:
                continue
            voice_options.append(
                {
                    "value": normalized_value,
                    "label": custom_voice_labels.get(
                        normalized_value,
                        runtime_voice_labels.get(
                            normalized_value,
                            sherpa_voice_labels.get(normalized_value, qwen_voice_labels.get(normalized_value, normalized_value)),
                        ),
                    ),
                }
            )
    except Exception:
        voice_runtime = {}
        voice_options = []
    voice_runtime_provider = str(voice_runtime.get("provider") or "").strip().lower()
    if not voice_options:
        fallback_voice = str(voice_runtime.get("defaultVoice") or "serena").strip() or "serena"
        voice_options = [{"value": fallback_voice, "label": fallback_voice}]
    available_voice_values = [str(item.get("value") or "").strip() for item in voice_options if str(item.get("value") or "").strip()]
    builtin_voice_values = clean_unique_strings(voice_runtime.get("builtinVoices") or [item for item in available_voice_values if not str(item or "").strip().startswith("custom:")])
    preferred_builtin_voice_values = (
        [item for item in ["serena", "vivian", "uncle_fu", "dylan", "eric", "sohee", "ono_anna", "aiden", "ryan"] if item in builtin_voice_values]
        if voice_runtime_provider == "qwen3_tts"
        else (
            [item for item in ["zf_001", "zf_002", "zf_003", "zf_004", "zf_005", "zf_006", "zm_009", "zm_010", "zm_011", "zm_012", "zm_013"] if item in builtin_voice_values]
            if voice_runtime_provider == "sherpa_onnx"
            else [
                item
                for item in builtin_voice_values
                if any(token in str(item or "") for token in ("中文", "普通话", "国语", "粤语"))
            ]
        )
    ) or builtin_voice_values
    ordered_agent_ids = [
        str(item.get("id") or "").strip()
        for item in agent_cards
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    default_voice_value = str(
        voice_runtime.get("defaultVoice")
        or (preferred_builtin_voice_values[0] if preferred_builtin_voice_values else (builtin_voice_values[0] if builtin_voice_values else (available_voice_values[0] if available_voice_values else "")))
    ).strip()

    def resolve_effective_voice(agent_id, configured_voice, stored_profile=None):
        normalized_agent_id = str(agent_id or "").strip()
        voice = str(configured_voice or "").strip()
        stored_profile = stored_profile if isinstance(stored_profile, dict) else {}
        stored_meta = stored_profile.get("meta") if isinstance(stored_profile.get("meta"), dict) else {}
        source = str(stored_meta.get("source") or stored_profile.get("source") or "").strip().lower()
        auto_assign_default_voice = (
            len(preferred_builtin_voice_values) > 1
            and source in {"", "product-default", "agent-profile-bootstrap"}
            and (
                not voice
                or voice == default_voice_value
                or (voice in builtin_voice_values and voice not in preferred_builtin_voice_values)
            )
        )
        if preferred_builtin_voice_values and normalized_agent_id and normalized_agent_id in ordered_agent_ids and auto_assign_default_voice:
            return preferred_builtin_voice_values[ordered_agent_ids.index(normalized_agent_id) % len(preferred_builtin_voice_values)]
        if voice and voice in available_voice_values:
            return voice
        legacy_voice = {
            "alloy": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_001",
            "ash": "uncle_fu" if voice_runtime_provider == "qwen3_tts" else "zm_009",
            "ballad": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_002",
            "cedar": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_003",
            "coral": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_004",
            "echo": "uncle_fu" if voice_runtime_provider == "qwen3_tts" else "zm_010",
            "fable": "vivian" if voice_runtime_provider == "qwen3_tts" else "zf_005",
            "marin": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_006",
            "nova": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_017",
            "onyx": "uncle_fu" if voice_runtime_provider == "qwen3_tts" else "zm_011",
            "sage": "uncle_fu" if voice_runtime_provider == "qwen3_tts" else "zm_012",
            "shimmer": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_018",
            "verse": "vivian" if voice_runtime_provider == "qwen3_tts" else "zm_013",
            "zh": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_001",
            "zh-cn": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_001",
            "中文女": "serena" if voice_runtime_provider == "qwen3_tts" else "zf_001",
            "中文男": "uncle_fu" if voice_runtime_provider == "qwen3_tts" else "zm_009",
            "粤语女": "vivian" if voice_runtime_provider == "qwen3_tts" else "zf_018",
        }.get(voice.lower())
        if (
            legacy_voice
            and len(builtin_voice_values) > 1
            and source in {"", "product-default", "agent-profile-bootstrap"}
            and legacy_voice == default_voice_value
            and normalized_agent_id
            and normalized_agent_id in ordered_agent_ids
        ):
            return preferred_builtin_voice_values[ordered_agent_ids.index(normalized_agent_id) % len(preferred_builtin_voice_values)]
        if legacy_voice and legacy_voice in builtin_voice_values:
            return legacy_voice
        if preferred_builtin_voice_values and normalized_agent_id and normalized_agent_id in ordered_agent_ids:
            return preferred_builtin_voice_values[ordered_agent_ids.index(normalized_agent_id) % len(preferred_builtin_voice_values)]
        return str(voice_runtime.get("defaultVoice") or (available_voice_values[0] if available_voice_values else voice)).strip()
    employee_items = []
    customized_count = 0
    employee_ids = set()
    for agent in agent_cards:
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        employee_ids.add(agent_id)
        stored = stored_profiles.get(agent_id, {})
        stored_meta = stored.get("meta") if isinstance(stored.get("meta"), dict) else {}
        if stored:
            customized_count += 1
        skills = clean_unique_strings(stored.get("skills") or agent.get("skills") or [])
        employee_items.append(
            {
                "agentId": agent_id,
                "humanName": str(stored.get("humanName") or agent.get("humanName") or agent.get("name") or agent_id).strip(),
                "jobTitle": str(stored.get("jobTitle") or agent.get("jobTitle") or agent.get("roleLabel") or "").strip(),
                "role": str(stored.get("role") or agent.get("role") or "").strip(),
                "roleLabel": str(agent.get("roleLabel") or "").strip(),
                "workingStyle": str(stored.get("workingStyle") or agent.get("workingStyle") or "").strip(),
                "department": str(stored.get("department") or "").strip(),
                "voiceReplyVoice": resolve_effective_voice(
                    agent_id,
                    stored.get("voiceReplyVoice") or agent.get("voiceReplyVoice") or "",
                    stored,
                ),
                "voiceReplySpeed": stored.get("voiceReplySpeed")
                if stored.get("voiceReplySpeed") is not None
                else agent.get("voiceReplySpeed"),
                "voiceReplyInstructions": str(stored.get("voiceReplyInstructions") or agent.get("voiceReplyInstructions") or "").strip(),
                "voiceReplySampleConfigured": bool(str(stored_meta.get("voiceReplySamplePath") or agent.get("voiceReplySamplePath") or "").strip()),
                "voiceReplySampleName": str(stored_meta.get("voiceReplySampleName") or agent.get("voiceReplySampleName") or "").strip(),
                "voiceReplySamplePromptText": str(stored_meta.get("voiceReplySamplePromptText") or agent.get("voiceReplySamplePromptText") or "").strip(),
                "skills": skills,
                "skillLabels": [skill_labels.get(skill, skill) for skill in skills],
                "capabilityTags": clean_unique_strings(stored.get("capabilityTags") or []),
                "notes": str(stored.get("notes") or "").strip(),
                "teamNames": [item.get("name") for item in team_membership_map.get(agent_id, []) if item.get("name")],
                "teamIds": [item.get("id") for item in team_membership_map.get(agent_id, []) if item.get("id")],
                "status": str(agent.get("status") or "").strip(),
                "source": "database" if stored else "runtime",
            }
        )
    employee_items.sort(key=lambda item: (item.get("jobTitle") or item.get("humanName") or item.get("agentId")))
    return {
        "summary": {
            "employeeCount": len(employee_items),
            "customizedEmployeeCount": customized_count,
            "templateRoleCount": len(template_roles),
            "teamCount": len(team_items),
            "capabilitySkillCount": sum(len(item.get("skills") or []) for item in employee_items),
            "coveredTemplateRoleCount": len([item for item in template_roles if str(item.get("agentId") or "").strip() in employee_ids]),
        },
        "templateRoles": template_roles,
        "teams": team_items,
        "employees": employee_items,
        "roleOptions": role_options,
        "skillOptions": skill_options,
        "voiceOptions": voice_options,
        "voiceRuntime": {
            "provider": str(voice_runtime.get("provider") or "").strip(),
            "speakerCount": int(voice_runtime.get("speakerCount") or len(voice_options)),
            "builtinSpeakerCount": int(voice_runtime.get("builtinSpeakerCount") or len(builtin_voice_values)),
            "customSpeakerCount": int(voice_runtime.get("customSpeakerCount") or max(0, len(voice_options) - len(builtin_voice_values))),
            "defaultVoice": str(voice_runtime.get("defaultVoice") or voice_options[0]["value"]).strip(),
            "supportsDistinctMemberVoices": bool(voice_runtime.get("supportsDistinctMemberVoices")),
        },
    }


def build_theme_snapshot(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    router_agent_id = get_router_agent_id(config)
    now = now_utc()
    theme_name = metadata.get("theme", DEFAULT_THEME_NAME)
    theme_style = THEME_STYLES.get(theme_name, THEME_STYLES[DEFAULT_THEME_NAME])
    theme_catalog = []
    for theme_key, meta in SELECTABLE_THEME_CATALOG.items():
        theme_catalog.append(
            {
                "name": theme_key,
                "displayName": meta["displayName"],
                "language": meta.get("language", "zh-CN"),
                "tagline": meta["tagline"],
                "bestFor": meta["bestFor"],
                "summary": meta["summary"],
                "current": theme_key == theme_name,
            }
        )
    theme_history = []
    for event in load_audit_events(openclaw_dir, limit=36):
        if event.get("action") != "theme_switch":
            continue
        at = parse_iso(event.get("at"))
        actor = event.get("actor", {})
        meta = event.get("meta", {})
        target_theme = str(meta.get("theme", "")).strip()
        theme_history.append(
            {
                "id": event.get("id"),
                "theme": target_theme,
                "themeDisplayName": THEME_CATALOG.get(target_theme, {}).get("displayName", target_theme or "未知主题"),
                "headline": event.get("detail") or f"切换主题到 {target_theme or '未知主题'}",
                "actor": actor.get("displayName") or actor.get("username") or "system",
                "role": actor.get("role", ""),
                "at": event.get("at", ""),
                "atAgo": format_age(at, now) if at else "未知时间",
            }
        )
        if len(theme_history) >= 5:
            break
    return {
        "routerAgentId": router_agent_id,
        "theme": {
            "name": theme_name,
            "displayName": metadata.get("displayName", theme_name),
            "language": THEME_CATALOG.get(theme_name, {}).get("language", THEME_CATALOG[DEFAULT_THEME_NAME].get("language", "zh-CN")),
            "styles": theme_style,
        },
        "themeCatalog": theme_catalog,
        "themeHistory": theme_history,
        "themeWorkforce": build_theme_workforce_snapshot(openclaw_dir, config=config),
    }


def _build_orchestration_workflow_context(openclaw_dir, config=None, now=None, task_snapshot=None, include_versions=False):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    task_snapshot = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
        openclaw_dir,
        config=config,
        now=now,
    )
    agents = safe_list(task_snapshot.get("agents"))
    router_agent_id = str(task_snapshot.get("routerAgentId") or get_router_agent_id(config)).strip()
    skills_data = load_skills_catalog(openclaw_dir)
    pack_map = workflow_pack_map_from_skills_payload(skills_data if isinstance(skills_data, dict) else {})
    team_map = {
        item.get("id"): item
        for item in safe_list(store_list_agent_teams(openclaw_dir))
        if isinstance(item, dict) and item.get("id")
    }
    workflows = safe_list(store_list_orchestration_workflows(openclaw_dir))
    if not workflows:
        workflows = [default_orchestration_workflow(agents, router_agent_id)]
    hydrated_workflows = []
    workflow_versions = []
    for workflow in workflows:
        if not isinstance(workflow, dict):
            continue
        versions = (
            safe_list(store_list_orchestration_workflow_versions(openclaw_dir, workflow_id=workflow.get("id"), limit=12))
            if include_versions
            else []
        )
        hydrated = hydrate_workflow_pack_context(workflow, pack_map)
        workflow_record = {
            **workflow,
            **hydrated,
            "teamOwnership": build_team_ownership_payload(
                team_map,
                execution_team_id=str((hydrated.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
                recommended_team_id=str((hydrated.get("linkedPack") or {}).get("recommendedTeamId") or "").strip(),
                mode=str((hydrated.get("linkedPack") or {}).get("mode") or "").strip(),
                source="workflow",
            ),
        }
        if include_versions:
            workflow_record["versions"] = versions
            workflow_record["latestVersion"] = versions[0] if versions else None
            workflow_versions.extend(versions[:4])
        hydrated_workflows.append(workflow_record)
    return {
        "taskIndex": safe_list(task_snapshot.get("taskIndex")),
        "agents": agents,
        "routerAgentId": router_agent_id,
        "workflows": hydrated_workflows,
        "workflowVersions": workflow_versions[:24],
    }


def _build_orchestration_replay_slice(task_index):
    replays = []
    for task in safe_list(task_index)[:24]:
        if not isinstance(task, dict) or not task.get("id"):
            continue
        replay = build_orchestration_replay(task)
        replay.update(
            {
                "state": task.get("state", ""),
                "updatedAgo": task.get("updatedAgo", ""),
                "route": task.get("route", []),
                "blocked": bool(task.get("blocked")),
            }
        )
        replays.append(replay)
    replays.sort(key=lambda item: item.get("durationMinutes", 0), reverse=True)
    context_hotspots = sorted(
        [
            {
                "taskId": item.get("taskId", ""),
                "title": item.get("title", ""),
                "contextLossCount": item.get("contextLossCount", 0),
                "owner": item.get("owner", ""),
                "durationMinutes": item.get("durationMinutes", 0),
            }
            for item in replays
            if item.get("contextLossCount", 0) > 0
        ],
        key=lambda item: (-item["contextLossCount"], -item["durationMinutes"]),
    )[:8]
    return replays, context_hotspots


def _build_orchestration_planning_slice(task_index, management_runs):
    planned_tasks = [task for task in safe_list(task_index) if planning_binding_from_payload(task)]
    planned_runs = [run for run in safe_list(management_runs) if planning_binding_from_payload(run)]
    planning_recent = sorted(
        [
            {
                "id": item.get("id") or binding.get("bundleId", ""),
                "title": item.get("title") or item.get("taskTitle") or binding.get("title", ""),
                "kind": binding.get("kind", "task"),
                "relativeDir": binding.get("relativeDir", ""),
                "progressPercent": binding.get("progressPercent", 0),
                "currentPhase": binding.get("currentPhase", ""),
                "updatedAt": binding.get("updatedAt", ""),
            }
            for item in [*planned_tasks, *planned_runs]
            for binding in [planning_binding_from_payload(item)]
        ],
        key=lambda entry: parse_iso(entry.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    return {
        "taskCount": len(planned_tasks),
        "runCount": len(planned_runs),
        "coverage": int(round((len(planned_tasks) / max(len(safe_list(task_index)), 1)) * 100)) if safe_list(task_index) else 0,
        "recent": planning_recent[:8],
    }


def build_orchestration_summary_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        workflow_context = _build_orchestration_workflow_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_versions=True,
        )
        task_index = workflow_context["taskIndex"]
        workflows = workflow_context["workflows"]
        routing_policies = safe_list(store_list_routing_policies(openclaw_dir))
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        replays, context_hotspots = _build_orchestration_replay_slice(task_index)
        policy_trends = build_orchestration_policy_trends(routing_decisions, task_index)
        workflow_review = build_orchestration_workflow_review(workflows, task_index, replays)
        adjustment_review = build_orchestration_adjustment_review(workflows, workflow_review, now)
        review_suggestions = build_orchestration_review_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index)
        next_step_suggestions = build_orchestration_next_step_suggestions(workflows, workflow_review, adjustment_review)
        linked_suggestions = build_orchestration_linked_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index)
        linked_review = build_orchestration_linked_review(workflows, routing_policies, policy_trends, workflow_review, task_index)
        intelligence_summary = build_task_intelligence_summary(task_index)
        decision_quality = build_routing_effectiveness_summary(task_index, routing_decisions)
        management_runs = safe_list(store_list_management_runs(openclaw_dir, limit=96))
        planning = _build_orchestration_planning_slice(task_index, management_runs)
        return {
            "summary": {
                "workflowCount": len(workflows),
                "activePolicies": sum(1 for item in routing_policies if item.get("status") == "active"),
                "replayCount": len(replays),
                "contextLossHotspots": len(context_hotspots),
                "routingDecisionCount": len(routing_decisions),
                "plannedTaskCount": planning.get("taskCount", 0),
                "plannedRunCount": planning.get("runCount", 0),
                "manualReviewCount": intelligence_summary.get("manualReviewCount", 0),
                "lowConfidenceCount": intelligence_summary.get("lowConfidenceCount", 0),
                "riskyFallbackCount": intelligence_summary.get("riskyFallbackCount", 0),
                "decisionEvaluatedCount": decision_quality.get("evaluatedCount", 0),
                "decisionCompletionRate": decision_quality.get("completionRate", 0),
                "decisionBlockRate": decision_quality.get("blockRate", 0),
            },
            "catalogSummary": {
                "workflowCount": len(workflows),
                "workflowVersionCount": len(workflow_context["workflowVersions"]),
                "policyCount": len(routing_policies),
                "replayCount": len(replays),
                "routingDecisionCount": len(routing_decisions),
            },
            "decisionQuality": decision_quality,
            "policyTrends": policy_trends,
            "workflowReview": workflow_review,
            "adjustmentReview": adjustment_review,
            "reviewSuggestions": review_suggestions,
            "nextStepSuggestions": next_step_suggestions,
            "linkedSuggestions": linked_suggestions,
            "linkedReview": linked_review,
        }

    return cached_payload(
        ("orchestration-direct-summary", str(openclaw_dir)),
        10.0,
        build,
    )


def build_orchestration_overview_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        workflow_context = _build_orchestration_workflow_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_versions=True,
        )
        task_index = workflow_context["taskIndex"]
        workflows = workflow_context["workflows"]
        routing_policies = safe_list(store_list_routing_policies(openclaw_dir))
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        replays, context_hotspots = _build_orchestration_replay_slice(task_index)
        intelligence_summary = build_task_intelligence_summary(task_index)
        decision_quality = build_routing_effectiveness_summary(task_index, routing_decisions)
        management_runs = safe_list(store_list_management_runs(openclaw_dir, limit=96))
        planning = _build_orchestration_planning_slice(task_index, management_runs)
        summary = {
            "workflowCount": len(workflows),
            "activePolicies": sum(1 for item in routing_policies if item.get("status") == "active"),
            "replayCount": len(replays),
            "contextLossHotspots": len(context_hotspots),
            "routingDecisionCount": len(routing_decisions),
            "plannedTaskCount": planning.get("taskCount", 0),
            "plannedRunCount": planning.get("runCount", 0),
            "manualReviewCount": intelligence_summary.get("manualReviewCount", 0),
            "lowConfidenceCount": intelligence_summary.get("lowConfidenceCount", 0),
            "riskyFallbackCount": intelligence_summary.get("riskyFallbackCount", 0),
            "decisionEvaluatedCount": decision_quality.get("evaluatedCount", 0),
            "decisionCompletionRate": decision_quality.get("completionRate", 0),
            "decisionBlockRate": decision_quality.get("blockRate", 0),
        }
        catalog_summary = {
            "workflowCount": len(workflows),
            "workflowVersionCount": len(workflow_context["workflowVersions"]),
            "policyCount": len(routing_policies),
            "replayCount": len(replays),
            "routingDecisionCount": len(routing_decisions),
        }
        return {
            "summary": summary,
            "catalogSummary": catalog_summary,
        }

    return cached_payload(
        ("orchestration-direct-overview", str(openclaw_dir)),
        10.0,
        build,
    )


def build_orchestration_review_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        workflow_context = _build_orchestration_workflow_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_versions=True,
        )
        task_index = workflow_context["taskIndex"]
        workflows = workflow_context["workflows"]
        routing_policies = safe_list(store_list_routing_policies(openclaw_dir))
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        replays, _context_hotspots = _build_orchestration_replay_slice(task_index)
        policy_trends = build_orchestration_policy_trends(routing_decisions, task_index)
        workflow_review = build_orchestration_workflow_review(workflows, task_index, replays)
        adjustment_review = build_orchestration_adjustment_review(workflows, workflow_review, now)
        linked_review = build_orchestration_linked_review(workflows, routing_policies, policy_trends, workflow_review, task_index)
        return {
            "workflowReview": workflow_review,
            "adjustmentReview": adjustment_review,
            "linkedReview": linked_review,
        }

    return cached_payload(
        ("orchestration-direct-review", str(openclaw_dir)),
        10.0,
        build,
    )


def build_orchestration_suggestions_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        workflow_context = _build_orchestration_workflow_context(
            openclaw_dir,
            config=config,
            now=now,
            task_snapshot=task_snapshot,
            include_versions=True,
        )
        task_index = workflow_context["taskIndex"]
        workflows = workflow_context["workflows"]
        routing_policies = safe_list(store_list_routing_policies(openclaw_dir))
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        replays, _context_hotspots = _build_orchestration_replay_slice(task_index)
        policy_trends = build_orchestration_policy_trends(routing_decisions, task_index)
        workflow_review = build_orchestration_workflow_review(workflows, task_index, replays)
        adjustment_review = build_orchestration_adjustment_review(workflows, workflow_review, now)
        review_suggestions = build_orchestration_review_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index)
        next_step_suggestions = build_orchestration_next_step_suggestions(workflows, workflow_review, adjustment_review)
        linked_suggestions = build_orchestration_linked_suggestions(workflows, routing_policies, policy_trends, workflow_review, task_index)
        return {
            "reviewSuggestions": review_suggestions,
            "nextStepSuggestions": next_step_suggestions,
            "linkedSuggestions": linked_suggestions,
        }

    return cached_payload(
        ("orchestration-direct-suggestions", str(openclaw_dir)),
        10.0,
        build,
    )


def build_orchestration_workflows_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    return cached_payload(
        ("orchestration-direct-workflows", str(openclaw_dir)),
        10.0,
        lambda: (
            lambda workflow_context: {
                "workflows": workflow_context["workflows"],
                "workflowVersions": workflow_context["workflowVersions"],
            }
        )(
            _build_orchestration_workflow_context(
                openclaw_dir,
                config=config,
                now=now,
                task_snapshot=task_snapshot,
                include_versions=True,
            )
        ),
    )


def build_orchestration_routing_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        task_snapshot_local = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
            openclaw_dir,
            config=config,
            now=now,
        )
        task_index = safe_list(task_snapshot_local.get("taskIndex"))
        routing_policies = safe_list(store_list_routing_policies(openclaw_dir))
        routing_decisions = safe_list(store_list_routing_decisions(openclaw_dir, limit=180))
        policy_hits = Counter()
        trend_counter = Counter()
        for item in routing_decisions:
            policy_name = item.get("policyName") or item.get("policyId") or "Router fallback"
            policy_hits[policy_name] += 1
            decided_at = parse_iso(item.get("decidedAt"))
            if decided_at:
                trend_counter[decided_at.strftime("%Y-%m-%d")] += 1
        return {
            "routingPolicies": routing_policies,
            "routingDecisions": [compact_orchestration_routing_decision(item) for item in routing_decisions[:48]],
            "routingHitLeaders": [
                {"policyName": name, "count": count}
                for name, count in policy_hits.most_common(8)
            ],
            "policyTrends": build_orchestration_policy_trends(routing_decisions, task_index),
            "routingTrend": [
                {"date": date, "count": trend_counter[date]}
                for date in sorted(trend_counter.keys())[-7:]
            ],
            "intelligence": build_task_intelligence_summary(task_index),
            "decisionQuality": build_routing_effectiveness_summary(task_index, routing_decisions),
        }

    return cached_payload(
        ("orchestration-direct-routing", str(openclaw_dir)),
        10.0,
        build,
    )


def build_orchestration_replays_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()

    def build():
        task_snapshot_local = task_snapshot if isinstance(task_snapshot, dict) else build_orchestration_task_index_snapshot(
            openclaw_dir,
            config=config,
            now=now,
        )
        replays, context_hotspots = _build_orchestration_replay_slice(task_snapshot_local.get("taskIndex"))
        return {
            "replays": [compact_orchestration_replay_payload(item) for item in replays[:18]],
            "contextHotspots": context_hotspots,
        }

    return cached_payload(
        ("orchestration-direct-replays", str(openclaw_dir)),
        10.0,
        build,
    )


def build_orchestration_planning_snapshot(openclaw_dir, config=None, now=None, task_snapshot=None):
    openclaw_dir = Path(openclaw_dir)
    config = config if isinstance(config, dict) else load_config(openclaw_dir)
    now = now or now_utc()
    return cached_payload(
        ("orchestration-direct-planning", str(openclaw_dir)),
        10.0,
        lambda: {
            "planning": _build_orchestration_planning_slice(
                (
                    task_snapshot
                    if isinstance(task_snapshot, dict)
                    else build_orchestration_task_index_snapshot(openclaw_dir, config=config, now=now)
                ).get("taskIndex"),
                store_list_management_runs(openclaw_dir, limit=96),
            ),
        },
    )


def build_dashboard_data(openclaw_dir, skip_automation_cycle=False):
    openclaw_dir = Path(openclaw_dir)
    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    now = now_utc()
    tasks = merge_tasks(openclaw_dir, config)
    cached_payload(
        ("planning-backfill", str(openclaw_dir)),
        30,
        lambda: backfill_planning_bundles(openclaw_dir, config, tasks),
    )
    cached_payload(
        ("routing-intelligence-backfill", str(openclaw_dir)),
        60,
        lambda: backfill_task_intelligence(openclaw_dir, config, tasks),
    )
    theme_name = metadata.get("theme", DEFAULT_THEME_NAME)
    theme_style = THEME_STYLES.get(theme_name, THEME_STYLES[DEFAULT_THEME_NAME])
    agent_overrides = agent_runtime_overrides(metadata)

    agent_labels, label_to_agent_ids = build_label_maps(agents, kanban_cfg, router_agent_id)
    bootstrap_agents = [
        {
            "id": str(agent.get("id") or "").strip(),
            "title": agent_labels.get(str(agent.get("id") or "").strip(), str(agent.get("id") or "").strip()),
            "name": ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", ""),
            "role": "",
            "skills": [],
        }
        for agent in agents
        if str(agent.get("id") or "").strip()
    ]
    metadata, teams = ensure_default_team_mode(openclaw_dir, metadata, bootstrap_agents, router_agent_id)
    metadata = ensure_default_skill_library_bootstrap(openclaw_dir, config=config, metadata=metadata)
    metadata = ensure_default_agent_profile_bootstrap(openclaw_dir, metadata, bootstrap_agents)
    metadata = ensure_default_memory_bootstrap(openclaw_dir, metadata, bootstrap_agents, teams)
    ensure_default_memory_authority_seed(openclaw_dir, agents=bootstrap_agents, teams=teams, metadata=metadata)
    metadata = ensure_default_management_bootstrap(openclaw_dir, metadata)
    agent_overrides = agent_runtime_overrides(metadata)
    team_map = {item.get("id"): item for item in safe_list(teams) if item.get("id")}
    task_thread_map = {}
    for thread in store_list_chat_threads(openclaw_dir, limit=256):
        if not is_managed_task_execution_thread(thread):
            continue
        task_execution = task_execution_meta_for_thread(thread)
        linked_task_id = str(task_execution.get("taskId") or thread.get("linkedTaskId") or "").strip()
        if not linked_task_id:
            continue
        existing_thread = task_thread_map.get(linked_task_id)
        existing_dt = parse_iso((existing_thread or {}).get("updatedAt")) if existing_thread else None
        thread_dt = parse_iso(thread.get("updatedAt"))
        if not existing_thread or (thread_dt and (not existing_dt or thread_dt >= existing_dt)):
            task_thread_map[linked_task_id] = thread

    task_counts_by_agent = Counter()
    blocked_counts_by_agent = Counter()
    latest_focus_by_agent = {}
    latest_focus_dt_by_agent = {}
    relay_counter = Counter()
    relay_last_at = {}
    handoffs_24h_by_agent = Counter()
    agent_signals = defaultdict(list)
    global_events = []
    task_index = []
    active_tasks = []
    deliverables = []

    recent_threshold = now - timedelta(hours=24)

    for task in tasks:
        if is_merged_duplicate_task(task):
            continue
        replay = build_task_replay(task, label_to_agent_ids, now)
        current_agent = current_agent_for_task(task, kanban_cfg, router_agent_id)
        progress_event = latest_progress_event(task, router_agent_id=router_agent_id)
        activity_at = str((progress_event or {}).get("at") or task.get("updatedAt") or "").strip()
        state = str(task.get("state", task.get("status", ""))).lower()
        todo = todo_summary(task)
        todo_items = [
            {
                "title": item.get("title", ""),
                "status": item.get("status", "not-started"),
            }
            for item in task.get("todos", [])
        ]

        task_record = {
            "id": task.get("id"),
            "title": task.get("title", task.get("id", "Untitled Task")),
            "state": task.get("state", task.get("status", "Unknown")),
            "owner": task.get("official", ""),
            "org": task.get("org", ""),
            "currentAgent": current_agent,
            "currentAgentLabel": (
                str(task.get("currentAgentLabel") or "").strip()
                or (
                    str(task.get("targetAgentLabel") or "").strip()
                    if current_agent and current_agent == str(task.get("targetAgentId") or "").strip()
                    else ""
                )
                or agent_labels.get(current_agent, current_agent or task.get("org", "?"))
            ),
            "currentUpdate": task_display_update(task),
            "updatedAt": task.get("updatedAt", ""),
            "updatedAgo": format_age(parse_iso(task.get("updatedAt")), now),
            "activityAt": activity_at,
            "output": task.get("output", ""),
            "outputLabel": Path(str(task.get("output") or "").strip()).name if str(task.get("output") or "").strip() else "",
            "todo": todo,
            "todoItems": todo_items,
            "route": task_route(task),
            "blocked": state == "blocked",
            "active": state not in TERMINAL_STATES,
            "replay": list(reversed(replay[-24:])),
            "targetAgentId": str(task.get("targetAgentId") or "").strip(),
            "targetAgentLabel": str(task.get("targetAgentLabel") or "").strip(),
            "workflowBinding": task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else (
                (task.get("meta") or {}).get("workflowBinding", {}) if isinstance(task.get("meta"), dict) else {}
            ),
            "planningBundle": task.get("planningBundle") if isinstance(task.get("planningBundle"), dict) else (
                (task.get("meta") or {}).get("planningBundle", {}) if isinstance(task.get("meta"), dict) else {}
            ),
            "routeDecision": (task.get("meta") or {}).get("routeDecision", {}) if isinstance(task.get("meta"), dict) else {},
            "resultSummary": task_result_summary(task),
        }
        route_meta = task_record["routeDecision"] if isinstance(task_record.get("routeDecision"), dict) else {}
        team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
        team_id = (
            str(team_assignment.get("teamId") or "").strip()
            or str(route_meta.get("teamId") or "").strip()
        )
        linked_team = team_map.get(team_id) if team_id else None
        task_record["teamId"] = team_id
        task_record["teamLabel"] = (
            str((linked_team or {}).get("name") or "").strip()
            or str(team_assignment.get("teamName") or "").strip()
            or str(route_meta.get("teamName") or "").strip()
        )
        task_record["linkedTeam"] = linked_team
        task_thread = task_thread_map.get(str(task_record.get("id") or "").strip(), {})
        task_thread_meta = task_thread.get("meta") if isinstance(task_thread.get("meta"), dict) else {}
        task_thread_dispatch = (
            task_thread_meta.get("lastDispatch")
            if isinstance(task_thread_meta.get("lastDispatch"), dict)
            else (
                task_thread_meta.get("lastSync")
                if isinstance(task_thread_meta.get("lastSync"), dict)
                else {}
            )
        )
        task_record["teamThreadId"] = str(task_thread.get("id") or "").strip()
        task_record["teamThreadTitle"] = str(task_thread.get("title") or "").strip()
        task_record["teamThreadStatus"] = str(task_thread.get("status") or "").strip()
        task_record["collaboration"] = build_team_collaboration_summary(
            task_thread_dispatch
            if isinstance(task_thread_dispatch, dict) and task_thread_dispatch
            else (route_meta.get("teamDispatch") if isinstance(route_meta.get("teamDispatch"), dict) else {})
        )
        if state in TERMINAL_STATES and (task_record["teamThreadId"] or task_record["teamId"]):
            task_record["collaboration"] = {
                **task_record["collaboration"],
                "status": "healthy",
                "headline": "已完成收口",
                "waitingCount": 0,
                "failureCount": 0,
                "blockerCount": 0,
                "waitingAgentIds": [],
                "failedAgentIds": [],
                "blockerAgentIds": [],
            }
        task_index.append(task_record)
        if state in TERMINAL_STATES or task.get("output"):
            deliverables.append(
                {
                    "id": task_record["id"],
                    "title": task_record["title"],
                    "state": task_record["state"],
                    "status": "completed" if state in TERMINAL_STATES else "review",
                    "statusLabel": task_record["state"],
                    "owner": task_record["owner"],
                    "updatedAt": task_record["updatedAt"],
                    "updatedAgo": task_record["updatedAgo"],
                    "summary": task_record["resultSummary"] or task_record["currentUpdate"],
                    "output": task_record["output"],
                    "sourceTask": task_record["id"],
                    "type": infer_deliverable_type(task_record["output"]),
                }
            )

        if current_agent and task_record["active"]:
            task_counts_by_agent[current_agent] += 1
            if state == "blocked":
                blocked_counts_by_agent[current_agent] += 1
            signal_dt = parse_iso((progress_event or {}).get("at")) or parse_iso(task.get("updatedAt"))
            if current_agent not in latest_focus_dt_by_agent or (
                signal_dt and signal_dt >= latest_focus_dt_by_agent[current_agent]
            ):
                latest_focus_dt_by_agent[current_agent] = signal_dt
                latest_focus_by_agent[current_agent] = task_record["currentUpdate"] or task_record["title"]

        for replay_event in replay:
            global_events.append(
                {
                    "type": replay_event["kind"],
                    "at": replay_event.get("at", ""),
                    "title": task_record["title"],
                    "taskId": task_record["id"],
                    "headline": replay_event["headline"],
                    "detail": replay_event.get("detail", ""),
                }
            )

            if replay_event["kind"] == "handoff":
                at = parse_iso(replay_event.get("at"))
                if at and at >= recent_threshold:
                    edge = (replay_event["actorLabel"], replay_event["targetLabel"])
                    relay_counter[edge] += 1
                    relay_last_at[edge] = max(at, relay_last_at.get(edge, at))
                    if replay_event.get("actorId"):
                        handoffs_24h_by_agent[replay_event["actorId"]] += 1
                    if replay_event.get("targetId"):
                        handoffs_24h_by_agent[replay_event["targetId"]] += 1

                if replay_event.get("actorId"):
                    agent_signals[replay_event["actorId"]].append(
                        {
                            "title": task_record["title"],
                            "taskId": task_record["id"],
                            "meta": replay_event["atAgo"],
                            "detail": f"移交给 {replay_event['targetLabel']} · {replay_event.get('detail', '')}".strip(" ·"),
                        }
                    )
                if replay_event.get("targetId"):
                    agent_signals[replay_event["targetId"]].append(
                        {
                            "title": task_record["title"],
                            "taskId": task_record["id"],
                            "meta": replay_event["atAgo"],
                            "detail": f"从 {replay_event['actorLabel']} 接到任务 · {replay_event.get('detail', '')}".strip(" ·"),
                        }
                    )
            elif replay_event.get("actorId"):
                agent_signals[replay_event["actorId"]].append(
                    {
                        "title": task_record["title"],
                        "taskId": task_record["id"],
                        "meta": replay_event["atAgo"],
                        "detail": replay_event.get("detail", ""),
                    }
                )

        if task_record["active"]:
            active_tasks.append(task_record)

    active_tasks.sort(
        key=lambda item: parse_iso(item.get("activityAt") or item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    task_index.sort(
        key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    global_events.sort(
        key=lambda item: parse_iso(item.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    deliverables = build_deliverables_snapshot(
        openclaw_dir,
        config=config,
        now=now,
        task_snapshot={
            "deliverables": _merge_unique_deliverables(
                deliverables,
                _build_management_task_deliverables(task_index),
            )
        },
    )

    agent_cards = []
    active_agent_count = 0
    for agent in agents:
        agent_id = agent["id"]
        workspace = Path(agent.get("workspace", "")) if agent.get("workspace") else openclaw_dir / f"workspace-{agent_id}"
        workspace_dt = workspace_last_activity(workspace)
        session_dt = session_last_activity(openclaw_dir, agent_id)
        signal_dt = latest_focus_dt_by_agent.get(agent_id)
        last_seen = max([dt for dt in (workspace_dt, session_dt, signal_dt) if dt is not None], default=None)
        active_task_cards = [
            {
                "id": task["id"],
                "title": task["title"],
                "state": task["state"],
                "updatedAgo": task["updatedAgo"],
            }
            for task in active_tasks
            if task.get("currentAgent") == agent_id
        ]
        status = status_for_agent(
            task_counts_by_agent[agent_id],
            blocked_counts_by_agent[agent_id],
            signal_dt,
            last_seen,
            now,
        )
        override = agent_overrides.get(agent_id) if isinstance(agent_overrides.get(agent_id), dict) else {}
        runtime_profile = merged_agent_runtime_profile(agent_id, override=override)
        is_paused = bool(override.get("paused"))
        if is_paused:
            status = "paused"
        work_guard = work_guard_for_agent(
            task_counts_by_agent[agent_id],
            blocked_counts_by_agent[agent_id],
            signal_dt,
            last_seen,
            workspace_dt,
            session_dt,
            now,
            paused=is_paused,
        )
        if status in {"active", "waiting", "blocked"}:
            active_agent_count += 1

        agent_cards.append(
            {
                "id": agent_id,
                "name": agent.get("identity", {}).get("name", agent_id),
                "humanName": runtime_profile.get("humanName", "") or agent.get("identity", {}).get("name", agent_id),
                "title": agent_labels.get(agent_id, agent_id),
                "model": agent.get("model", "default"),
                "role": runtime_profile.get("role", ""),
                "roleLabel": runtime_profile.get("roleLabel", ""),
                "jobTitle": runtime_profile.get("jobTitle", ""),
                "workingStyle": runtime_profile.get("workingStyle", ""),
                "skills": safe_list(runtime_profile.get("skills")),
                "skillCount": int(runtime_profile.get("skillCount") or 0),
                "status": status,
                "paused": is_paused,
                "activeTasks": task_counts_by_agent[agent_id],
                "blockedTasks": blocked_counts_by_agent[agent_id],
                "focus": latest_focus_by_agent.get(agent_id, ""),
                "currentTaskId": active_task_cards[0]["id"] if active_task_cards else "",
                "currentTaskTitle": active_task_cards[0]["title"] if active_task_cards else "",
                "lastSeenAgo": format_age(last_seen, now),
                "lastSeenAt": last_seen.isoformat().replace("+00:00", "Z") if last_seen else "",
                "workspaceLastSeenAgo": format_age(workspace_dt, now),
                "sessionLastSeenAgo": format_age(session_dt, now),
                "handoffs24h": handoffs_24h_by_agent[agent_id],
                "activeTaskCards": active_task_cards[:6],
                "recentSignals": agent_signals[agent_id][:8],
                "workGuard": work_guard,
                "runtimeOverride": override,
            }
        )

    relays = []
    for edge, count in relay_counter.most_common(10):
        relays.append(
            {
                "from": edge[0],
                "to": edge[1],
                "count": count,
                "lastAt": relay_last_at[edge].isoformat().replace("+00:00", "Z"),
                "lastAgo": format_age(relay_last_at[edge], now),
            }
        )

    completed_today = sum(
        1
        for task in task_index
        if str(task.get("state", "")).lower() == "done"
        and (parse_iso(task.get("updatedAt")) or now) >= now - timedelta(days=1)
    )
    blocked_total = sum(1 for task in active_tasks if task.get("blocked"))
    needs_attention_agents = sum(1 for agent in agent_cards if (agent.get("workGuard") or {}).get("needsAttention"))
    stalled_agents = sum(1 for agent in agent_cards if (agent.get("workGuard") or {}).get("reason") == "stale_progress")
    signal_count = sum(
        1
        for event in global_events
        if parse_iso(event.get("at")) and parse_iso(event.get("at")) >= now - timedelta(hours=1)
    )
    theme_catalog = []
    for theme_key, meta in SELECTABLE_THEME_CATALOG.items():
        theme_catalog.append(
            {
                "name": theme_key,
                "displayName": meta["displayName"],
                "language": meta.get("language", "zh-CN"),
                "tagline": meta["tagline"],
                "bestFor": meta["bestFor"],
                "summary": meta["summary"],
                "current": theme_key == theme_name,
            }
        )
    theme_history = []
    for event in load_audit_events(openclaw_dir, limit=36):
        if event.get("action") != "theme_switch":
            continue
        at = parse_iso(event.get("at"))
        actor = event.get("actor", {})
        meta = event.get("meta", {})
        target_theme = str(meta.get("theme", "")).strip()
        theme_history.append(
            {
                "id": event.get("id"),
                "theme": target_theme,
                "themeDisplayName": THEME_CATALOG.get(target_theme, {}).get("displayName", target_theme or "未知主题"),
                "headline": event.get("detail") or f"切换主题到 {target_theme or '未知主题'}",
                "actor": actor.get("displayName") or actor.get("username") or "system",
                "role": actor.get("role", ""),
                "at": event.get("at", ""),
                "atAgo": format_age(at, now) if at else "未知时间",
            }
        )
        if len(theme_history) >= 5:
            break
    product_commands = [
        {
            "label": "打开实时面板",
            "command": f"python3 {openclaw_dir}/workspace-{router_agent_id}/scripts/collaboration_dashboard.py --serve --dir {openclaw_dir}",
            "description": "启动完整OpenClaw Team 本地应用。",
        },
        {
            "label": "查看健康状态",
            "command": f"python3 {openclaw_dir}/workspace-{router_agent_id}/scripts/health_dashboard.py --dir {openclaw_dir}",
            "description": "快速检查各 Agent 工作区和任务状态。",
        },
        {
            "label": "导出当前快照",
            "command": f"python3 {openclaw_dir}/workspace-{router_agent_id}/scripts/collaboration_dashboard.py --dir {openclaw_dir}",
            "description": "生成最新 HTML 和 JSON 快照。",
        },
    ]
    admin_data = build_admin_bootstrap_snapshot(openclaw_dir, config=config, now=now)
    conversation_data = load_conversation_catalog(openclaw_dir, config, agent_labels)
    skills_data = load_skills_catalog(openclaw_dir, config=config)
    openclaw_data = cached_payload_background(
        ("openclaw-summary", str(openclaw_dir)),
        30.0,
        lambda: load_openclaw_dashboard_summary(openclaw_dir, config=config, metadata=metadata),
        load_openclaw_dashboard_summary(openclaw_dir, config=config, metadata=metadata),
    )
    context_hub_data = load_context_hub_data(
        openclaw_dir,
        agent_cards=agent_cards,
        router_agent_id=router_agent_id,
    )
    agent_teams_data = build_agent_team_data(openclaw_dir, agent_cards, task_index, now)
    management_data = build_management_data(
        openclaw_dir,
        task_index,
        conversation_data,
        deliverables,
        agent_cards,
        global_events,
        relays,
        now,
        skills_data=skills_data,
        skip_automation_cycle=skip_automation_cycle,
    )
    hydrate_task_run_links(task_index, safe_list((management_data or {}).get("runs")))
    enrich_task_team_ownership(task_index, team_map)
    orchestration_data = build_orchestration_data(openclaw_dir, agent_cards, task_index, router_agent_id, now, skills_data=skills_data)
    compact_task_index = [compact_task_dashboard_payload(task) for task in safe_list(task_index)]
    communications_data = build_communications_data(openclaw_dir, conversation_data, management_data, now)
    chat_data = build_chat_data(openclaw_dir, agent_cards, compact_task_index, deliverables, management_data, now, agent_teams_data=agent_teams_data, skills_data=skills_data)
    platform_data = {
        "apiReference": build_external_api_reference(),
        "runtimeGovernance": build_platform_runtime_governance_summary(
            skills_data,
            safe_list((management_data or {}).get("runs")),
        ),
    }
    metadata = ensure_default_memory_bootstrap(
        openclaw_dir,
        load_project_metadata(openclaw_dir, config=config),
        bootstrap_agents,
        teams,
    )
    skills_data = enrich_skills_installation_state(skills_data, openclaw_dir, metadata=metadata)

    skills_dashboard_data = compact_skills_payload_for_dashboard(skills_data)
    management_dashboard_data = compact_management_payload_for_dashboard(management_data)
    theme_workforce = build_theme_workforce_snapshot(
        openclaw_dir,
        config=config,
        agents=agent_cards,
        teams=agent_teams_data.get("items") if isinstance(agent_teams_data, dict) else [],
        skills_data=skills_data,
    )

    return {
        "generatedAt": now.isoformat().replace("+00:00", "Z"),
        "generatedAgo": "刚刚",
        "openclawDir": str(openclaw_dir),
        "routerAgentId": router_agent_id,
        "theme": {
            "name": theme_name,
            "displayName": metadata.get("displayName", theme_name),
            "language": THEME_CATALOG.get(theme_name, {}).get("language", THEME_CATALOG[DEFAULT_THEME_NAME].get("language", "zh-CN")),
            "styles": theme_style,
        },
        "themeCatalog": theme_catalog,
        "themeHistory": theme_history,
        "themeWorkforce": theme_workforce,
        "ownerTitle": kanban_cfg.get("owner_title", "用户"),
        "agents": agent_cards,
        "agentTeams": agent_teams_data,
        "taskIndex": compact_task_index[:72],
        "deliverables": deliverables[:24],
        "events": global_events[:42],
        "relays": relays,
        "commands": product_commands,
        "admin": admin_data,
        "management": management_dashboard_data,
        "memorySystem": memory_system_status_payload(metadata, agents=bootstrap_agents, teams=teams),
        "orchestration": orchestration_data,
        "communications": communications_data,
        "chat": chat_data,
        "conversations": conversation_data,
        "platform": platform_data,
        "skills": skills_dashboard_data,
        "openclaw": openclaw_data,
        "contextHub": context_hub_data,
        "metrics": {
            "activeTasks": len(active_tasks),
            "activeAgents": active_agent_count,
            "blockedTasks": blocked_total,
            "completedToday": completed_today,
            "handoffs24h": sum(item["count"] for item in relays),
            "signals1h": signal_count,
            "needsAttentionAgents": needs_attention_agents,
            "stalledAgents": stalled_agents,
        },
    }


def load_skill_pack_detail(openclaw_dir, pack_id):
    return deepcopy(resolve_workflow_pack_record(openclaw_dir, pack_id))


def load_skills_detail(openclaw_dir):
    return cached_payload(
        ("skills-detail", str(Path(openclaw_dir).expanduser().resolve())),
        5.0,
        lambda: enrich_skills_installation_state(
            load_skills_catalog(openclaw_dir),
            openclaw_dir,
            metadata=load_project_metadata(openclaw_dir),
        ),
    )



def load_task_detail(openclaw_dir, task_id):
    openclaw_dir = Path(openclaw_dir)
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise RuntimeError("需要 taskId。")

    config = load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    router_agent_id = get_router_agent_id(config)
    kanban_cfg = load_kanban_config(openclaw_dir, router_agent_id)
    now = now_utc()
    tasks = merge_tasks(openclaw_dir, config)
    task = next(
        (
            item
            for item in tasks
            if isinstance(item, dict) and str(item.get("id") or "").strip() == normalized_task_id
        ),
        None,
    )
    if not task:
        raise RuntimeError("任务不存在。")

    agent_labels, label_to_agent_ids = build_label_maps(agents, kanban_cfg, router_agent_id)
    bootstrap_agents = [
        {
            "id": str(agent.get("id") or "").strip(),
            "title": agent_labels.get(str(agent.get("id") or "").strip(), str(agent.get("id") or "").strip()),
            "name": ((agent.get("identity", {}) if isinstance(agent.get("identity"), dict) else {}) or {}).get("name", ""),
            "role": "",
            "skills": [],
        }
        for agent in agents
        if str(agent.get("id") or "").strip()
    ]
    _metadata, teams = ensure_default_team_mode(openclaw_dir, metadata, bootstrap_agents, router_agent_id)
    team_map = {item.get("id"): item for item in safe_list(teams) if item.get("id")}

    task_thread = {}
    for thread in store_list_chat_threads(openclaw_dir, limit=256):
        if not is_managed_task_execution_thread(thread):
            continue
        task_execution = task_execution_meta_for_thread(thread)
        linked_task_id = str(task_execution.get("taskId") or thread.get("linkedTaskId") or "").strip()
        if linked_task_id != normalized_task_id:
            continue
        existing_dt = parse_iso(task_thread.get("updatedAt")) if task_thread else None
        thread_dt = parse_iso(thread.get("updatedAt"))
        if not task_thread or (thread_dt and (not existing_dt or thread_dt >= existing_dt)):
            task_thread = thread

    replay = build_task_replay(task, label_to_agent_ids, now)
    current_agent = current_agent_for_task(task, kanban_cfg, router_agent_id)
    progress_event = latest_progress_event(task, router_agent_id=router_agent_id)
    activity_at = str((progress_event or {}).get("at") or task.get("updatedAt") or "").strip()
    state = str(task.get("state", task.get("status", ""))).lower()
    todo = todo_summary(task)
    todo_items = [
        {
            "title": item.get("title", ""),
            "status": item.get("status", "not-started"),
        }
        for item in task.get("todos", [])
    ]

    detail = {
        "id": task.get("id"),
        "title": task.get("title", task.get("id", "Untitled Task")),
        "state": task.get("state", task.get("status", "Unknown")),
        "owner": task.get("official", ""),
        "org": task.get("org", ""),
        "currentAgent": current_agent,
        "currentAgentLabel": (
            str(task.get("currentAgentLabel") or "").strip()
            or (
                str(task.get("targetAgentLabel") or "").strip()
                if current_agent and current_agent == str(task.get("targetAgentId") or "").strip()
                else ""
            )
            or agent_labels.get(current_agent, current_agent or task.get("org", "?"))
        ),
        "currentUpdate": task_display_update(task),
        "updatedAt": task.get("updatedAt", ""),
        "updatedAgo": format_age(parse_iso(task.get("updatedAt")), now),
        "activityAt": activity_at,
        "output": task.get("output", ""),
        "outputLabel": Path(str(task.get("output") or "").strip()).name if str(task.get("output") or "").strip() else "",
        "todo": todo,
        "todoItems": todo_items,
        "route": task_route(task),
        "blocked": state == "blocked",
        "active": state not in TERMINAL_STATES,
        "replay": list(reversed(replay[-24:])),
        "replayCount": len(replay),
        "targetAgentId": str(task.get("targetAgentId") or "").strip(),
        "targetAgentLabel": str(task.get("targetAgentLabel") or "").strip(),
        "workflowBinding": task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else (
            (task.get("meta") or {}).get("workflowBinding", {}) if isinstance(task.get("meta"), dict) else {}
        ),
        "planningBundle": task.get("planningBundle") if isinstance(task.get("planningBundle"), dict) else (
            (task.get("meta") or {}).get("planningBundle", {}) if isinstance(task.get("meta"), dict) else {}
        ),
        "routeDecision": (task.get("meta") or {}).get("routeDecision", {}) if isinstance(task.get("meta"), dict) else {},
        "resultSummary": task_result_summary(task),
    }
    route_meta = detail["routeDecision"] if isinstance(detail.get("routeDecision"), dict) else {}
    team_assignment = route_meta.get("teamAssignment") if isinstance(route_meta.get("teamAssignment"), dict) else {}
    team_id = str(team_assignment.get("teamId") or "").strip() or str(route_meta.get("teamId") or "").strip()
    linked_team = team_map.get(team_id) if team_id else None
    detail["teamId"] = team_id
    detail["teamLabel"] = (
        str((linked_team or {}).get("name") or "").strip()
        or str(team_assignment.get("teamName") or "").strip()
        or str(route_meta.get("teamName") or "").strip()
    )
    detail["linkedTeam"] = linked_team
    task_thread_meta = task_thread.get("meta") if isinstance(task_thread.get("meta"), dict) else {}
    task_thread_dispatch = (
        task_thread_meta.get("lastDispatch")
        if isinstance(task_thread_meta.get("lastDispatch"), dict)
        else (
            task_thread_meta.get("lastSync")
            if isinstance(task_thread_meta.get("lastSync"), dict)
            else {}
        )
    )
    detail["teamThreadId"] = str(task_thread.get("id") or "").strip()
    detail["teamThreadTitle"] = str(task_thread.get("title") or "").strip()
    detail["teamThreadStatus"] = str(task_thread.get("status") or "").strip()
    detail["collaboration"] = build_team_collaboration_summary(
        task_thread_dispatch
        if isinstance(task_thread_dispatch, dict) and task_thread_dispatch
        else (route_meta.get("teamDispatch") if isinstance(route_meta.get("teamDispatch"), dict) else {})
    )
    if state in TERMINAL_STATES and (detail["teamThreadId"] or detail["teamId"]):
        detail["collaboration"] = {
            **detail["collaboration"],
            "status": "healthy",
            "headline": "已完成收口",
            "waitingCount": 0,
            "failureCount": 0,
            "blockerCount": 0,
            "waitingAgentIds": [],
            "failedAgentIds": [],
            "blockerAgentIds": [],
        }

    hydrate_task_run_links([detail], store_list_management_runs(openclaw_dir, limit=256))
    enrich_task_team_ownership([detail], team_map)
    return detail
