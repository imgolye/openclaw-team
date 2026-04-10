#!/usr/bin/env python3
"""Switch OpenClaw Team theme for an existing OpenClaw install."""

from __future__ import annotations

import argparse
import json
import shutil
import stat
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

SCRIPT_DIR = Path(__file__).resolve().parent
LIB_DIR = SCRIPT_DIR / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

from generate_config import load_existing_config, write_config
from project_metadata import load_project_metadata
from render_templates import render_theme
from theme_utils import (
    DEPARTMENT_KEYS,
    ROLE_KEYS,
    get_agent_id_map_by_semantic,
    infer_theme_name_from_config,
    load_theme,
    translate_text_references,
    translate_theme_value,
)


PROJECT_DIR = SCRIPT_DIR.parent.parent
THEMES_DIR = PROJECT_DIR / "platform" / "config" / "themes"
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

BACKEND_DIR = PROJECT_DIR / "backend"

from backend.adapters.storage.dashboard import list_task_records, replace_task_records_for_workspace

RUNTIME_SCRIPTS = (
    "kanban_update.py",
    "file_lock.py",
    "refresh_live_data.py",
    "health_dashboard.py",
    "env_utils.py",
    "monitoring.py",
    "openapi_spec.py",
    "collaboration_dashboard.py",
)
RUNTIME_SCRIPT_DIRS = ("application", "adapters", "domain", "presentation")
LEGACY_RUNTIME_ROOT_FILES = (
    "dashboard_store.py",
    "admin_service.py",
    "chat_data_service.py",
    "customer_access_service.py",
    "orchestration_service.py",
    "dashboard_data_service.py",
    "desktop_service.py",
    "management_service.py",
    "runtime_service.py",
    "route_aliases.py",
    "query_route_dispatcher.py",
    "task_command_dispatcher.py",
    "agent_command_dispatcher.py",
    "management_command_dispatcher.py",
    "chat_command_dispatcher.py",
    "platform_command_dispatcher.py",
    "command_route_dispatcher.py",
    "rest_route_dispatcher.py",
    "http_route_dispatcher.py",
)
GENERATED_ROOT_FILES = {
    "SOUL.md",
    "HEARTBEAT.md",
    "IDENTITY.md",
    "USER.md",
    "AGENTS.md",
    "MEMORY.md",
    "message-style.md",
    "task-templates.md",
    "daily-tasks.md",
    "examples.md",
}
GENERATED_DIRS = {"scripts", "shared-context"}
SKIP_MERGE_NAMES = {".git", ".hg", ".svn", "__pycache__"}


def semantic_keys():
    return list(ROLE_KEYS) + list(DEPARTMENT_KEYS)


def build_semantic_pairs(old_theme, new_theme):
    old_map = get_agent_id_map_by_semantic(old_theme)
    new_map = get_agent_id_map_by_semantic(new_theme)
    return [(key, old_map[key], new_map[key]) for key in semantic_keys()]


def timestamp():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def backup_installation(openclaw_dir, current_theme_name, target_theme_name):
    backup_dir = openclaw_dir / "backups" / f"theme-switch-{timestamp()}"
    backup_dir.mkdir(parents=True, exist_ok=False)

    for filename in ("openclaw.json", ".env", "mission-control.json"):
        source = openclaw_dir / filename
        if source.exists():
            shutil.copy2(source, backup_dir / filename)

    manifest = {
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "fromTheme": current_theme_name,
        "toTheme": target_theme_name,
    }
    (backup_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return backup_dir


def ensure_agent_layout(openclaw_dir, theme):
    for agent_id in get_agent_id_map_by_semantic(theme).values():
        workspace = openclaw_dir / f"workspace-{agent_id}"
        for dirname in ("scripts", "data", "memory", "shared-context", "shared-context/knowledge-base", "skills"):
            (workspace / dirname).mkdir(parents=True, exist_ok=True)
        (openclaw_dir / "agents" / agent_id / "agent").mkdir(parents=True, exist_ok=True)


def deploy_runtime_scripts(openclaw_dir, theme):
    for agent_id in get_agent_id_map_by_semantic(theme).values():
        scripts_dir = openclaw_dir / f"workspace-{agent_id}" / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        for dirname in ("backend", "services", "dispatchers", "stores", "integrations", *RUNTIME_SCRIPT_DIRS):
            target_dir = scripts_dir / dirname
            if target_dir.exists():
                shutil.rmtree(target_dir)
        for filename in LEGACY_RUNTIME_ROOT_FILES:
            legacy_file = scripts_dir / filename
            if legacy_file.exists():
                legacy_file.unlink()
        for script_name in RUNTIME_SCRIPTS:
            shutil.copy2(BACKEND_DIR / script_name, scripts_dir / script_name)
        target_dir = scripts_dir / "backend"
        if target_dir.exists():
            shutil.rmtree(target_dir)
        shutil.copytree(
            BACKEND_DIR,
            target_dir,
            ignore=shutil.ignore_patterns("__pycache__", ".DS_Store"),
        )


def merge_tree(source, target):
    if not source.exists():
        return
    if source.resolve() == target.resolve():
        return
    if source.is_symlink():
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() or target.is_symlink():
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()
        target.symlink_to(source.readlink())
        return
    if source.is_dir():
        if source.name in SKIP_MERGE_NAMES:
            return
        target.mkdir(parents=True, exist_ok=True)
        for entry in source.iterdir():
            merge_tree(entry, target / entry.name)
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    if target.is_symlink():
        target.unlink()
    elif target.exists():
        try:
            target.chmod(target.stat().st_mode | stat.S_IWUSR)
        except OSError:
            pass
    shutil.copy2(source, target)


def translate_path_references(value, old_to_new_agent_ids):
    if not isinstance(value, str) or not value:
        return value
    translated = value
    for old_id, new_id in old_to_new_agent_ids.items():
        translated = translated.replace(f"workspace-{old_id}", f"workspace-{new_id}")
        translated = translated.replace(f"/agents/{old_id}/", f"/agents/{new_id}/")
    return translated


def translate_task(task, old_theme, new_theme, old_to_new_agent_ids):
    translated = deepcopy(task)
    semantic_pairs = build_semantic_pairs(old_theme, new_theme)
    old_id_to_new_title = {}
    old_id_to_new_identity = {}
    for semantic_key, old_id, _new_id in semantic_pairs:
        if semantic_key in ROLE_KEYS:
            new_entry = new_theme["roles"][semantic_key]
        else:
            new_entry = new_theme["roles"]["departments"][semantic_key]
        old_id_to_new_title[old_id] = new_entry["title"]
        old_id_to_new_identity[old_id] = new_entry["identity_name"]

    def translate_agent_label(value):
        if not isinstance(value, str) or not value:
            return value
        if value in old_id_to_new_title:
            return old_id_to_new_title[value]
        translated_value = translate_theme_value(value, old_theme, new_theme)
        if translated_value != value:
            return translated_value
        if value in old_to_new_agent_ids:
            return old_id_to_new_identity.get(value, old_to_new_agent_ids[value])
        return value

    def translate_nested(value):
        if isinstance(value, str):
            translated_value = translate_text_references(value, old_theme, new_theme)
            translated_value = translate_path_references(translated_value, old_to_new_agent_ids)
            return translate_agent_label(translated_value)
        if isinstance(value, list):
            return [translate_nested(item) for item in value]
        if isinstance(value, dict):
            translated_map = {}
            for key, item in value.items():
                if key in {"agent", "targetAgentId", "currentAgent"} and isinstance(item, str) and item in old_to_new_agent_ids:
                    translated_map[key] = old_to_new_agent_ids[item]
                elif key in {"agentLabel", "targetAgentLabel", "currentAgentLabel"}:
                    translated_map[key] = translate_agent_label(item)
                else:
                    translated_map[key] = translate_nested(item)
            return translated_map
        return value

    for field in ("targetAgentId", "currentAgent"):
        value = translated.get(field)
        if isinstance(value, str) and value in old_to_new_agent_ids:
            translated[field] = old_to_new_agent_ids[value]
    if isinstance(translated.get("targetAgentLabel"), str):
        translated["targetAgentLabel"] = translate_agent_label(translated.get("targetAgentLabel"))
    if isinstance(translated.get("currentAgentLabel"), str):
        translated["currentAgentLabel"] = translate_agent_label(translated.get("currentAgentLabel"))
    for field in ("official", "org"):
        translated[field] = translate_theme_value(translated.get(field), old_theme, new_theme)
    for field in ("now", "currentUpdate", "block", "blockers", "output"):
        translated[field] = translate_text_references(translated.get(field), old_theme, new_theme)
        translated[field] = translate_path_references(translated.get(field), old_to_new_agent_ids)

    meta = translated.get("meta") if isinstance(translated.get("meta"), dict) else {}
    route_meta = meta.get("routeDecision") if isinstance(meta.get("routeDecision"), dict) else {}
    if route_meta:
        route_meta = translate_nested(route_meta)
        meta["routeDecision"] = route_meta
        translated["meta"] = meta
        translated["routeDecision"] = route_meta

    flow_log = []
    for entry in translated.get("flow_log", []):
        new_entry = deepcopy(entry)
        new_entry["from"] = translate_theme_value(new_entry.get("from"), old_theme, new_theme)
        new_entry["to"] = translate_theme_value(new_entry.get("to"), old_theme, new_theme)
        new_entry["remark"] = translate_text_references(new_entry.get("remark"), old_theme, new_theme)
        flow_log.append(new_entry)
    if flow_log:
        translated["flow_log"] = flow_log

    progress_log = []
    for entry in translated.get("progress_log", []):
        new_entry = deepcopy(entry)
        agent_id = new_entry.get("agent")
        if isinstance(agent_id, str) and agent_id in old_to_new_agent_ids:
            new_entry["agent"] = old_to_new_agent_ids[agent_id]
        new_entry["agentLabel"] = translate_theme_value(
            translate_agent_label(new_entry.get("agentLabel")),
            old_theme,
            new_theme,
        )
        new_entry["org"] = translate_theme_value(new_entry.get("org"), old_theme, new_theme)
        new_entry["text"] = translate_text_references(new_entry.get("text"), old_theme, new_theme)
        progress_log.append(new_entry)
    if progress_log:
        translated["progress_log"] = progress_log

    return translated


def workspace_id_for_path(workspace):
    name = Path(workspace).name
    return name[len("workspace-"):] if name.startswith("workspace-") else name


def load_tasks(openclaw_dir, workspace):
    workspace_path = Path(workspace).expanduser().resolve()
    return list_task_records(
        openclaw_dir,
        workspace_id=workspace_id_for_path(workspace_path),
        workspace_path=str(workspace_path),
    )


def save_tasks(openclaw_dir, workspace, tasks):
    workspace_path = Path(workspace).expanduser().resolve()
    replace_task_records_for_workspace(
        openclaw_dir,
        workspace_id_for_path(workspace_path),
        str(workspace_path),
        tasks,
    )


def merge_tasks(existing_tasks, incoming_tasks):
    merged = {task.get("id"): deepcopy(task) for task in existing_tasks if isinstance(task, dict)}
    for task in incoming_tasks:
        if not isinstance(task, dict):
            continue
        task_id = task.get("id")
        if not task_id:
            continue
        existing = merged.get(task_id)
        if not existing or str(task.get("updatedAt", "")) >= str(existing.get("updatedAt", "")):
            merged[task_id] = deepcopy(task)
    return sorted(
        merged.values(),
        key=lambda item: str(item.get("updatedAt", "")),
        reverse=True,
    )


def migrate_workspace(old_workspace, new_workspace, old_theme, new_theme, old_to_new_agent_ids):
    if not old_workspace.exists():
        return

    for entry in old_workspace.iterdir():
        if entry.name in GENERATED_ROOT_FILES or entry.name in GENERATED_DIRS:
            continue
        if entry.name == "data":
            continue
        merge_tree(entry, new_workspace / entry.name)

    old_data_dir = old_workspace / "data"
    new_data_dir = new_workspace / "data"
    new_data_dir.mkdir(parents=True, exist_ok=True)

    if old_data_dir.exists():
        for entry in old_data_dir.iterdir():
            if entry.name == "kanban_config.json":
                continue
            merge_tree(entry, new_data_dir / entry.name)

    openclaw_dir = new_workspace.parent
    translated_tasks = [
        translate_task(task, old_theme, new_theme, old_to_new_agent_ids)
        for task in load_tasks(openclaw_dir, old_workspace)
    ]
    merged_tasks = merge_tasks(load_tasks(openclaw_dir, new_workspace), translated_tasks)
    save_tasks(openclaw_dir, new_workspace, merged_tasks)


def migrate_agent_state(openclaw_dir, old_theme, new_theme):
    pairs = build_semantic_pairs(old_theme, new_theme)
    old_to_new_agent_ids = {old_id: new_id for _, old_id, new_id in pairs}

    for _semantic_key, old_id, new_id in pairs:
        old_agent_root = openclaw_dir / "agents" / old_id
        new_agent_root = openclaw_dir / "agents" / new_id
        if old_agent_root.exists():
            merge_tree(old_agent_root, new_agent_root)

        old_workspace = openclaw_dir / f"workspace-{old_id}"
        new_workspace = openclaw_dir / f"workspace-{new_id}"
        new_workspace.mkdir(parents=True, exist_ok=True)
        migrate_workspace(old_workspace, new_workspace, old_theme, new_theme, old_to_new_agent_ids)


def rotate_live_session_files(agent_root):
    sessions_dir = agent_root / "sessions"
    if not sessions_dir.exists():
        return 0

    rotated = 0
    timestamp_suffix = timestamp()
    for session_file in sorted(sessions_dir.glob("*.jsonl")):
        target = sessions_dir / f"{session_file.name}.reset.{timestamp_suffix}"
        index = 1
        while target.exists():
            target = sessions_dir / f"{session_file.name}.reset.{timestamp_suffix}.{index}"
            index += 1
        session_file.rename(target)
        rotated += 1

    registry_path = sessions_dir / "sessions.json"
    registry_path.write_text("{}\n", encoding="utf-8")
    return rotated


def reset_agent_sessions(openclaw_dir, theme):
    rotated = 0
    for agent_id in get_agent_id_map_by_semantic(theme).values():
        rotated += rotate_live_session_files(openclaw_dir / "agents" / agent_id)
    return rotated


def build_generate_args(openclaw_dir, theme_file, existing_config, metadata, task_prefix_override):
    return SimpleNamespace(
        theme=str(theme_file),
        openclaw_dir=str(openclaw_dir),
        primary_model=None,
        light_model=None,
        feishu_app_id=existing_config.get("channels", {}).get("feishu", {}).get("appId", ""),
        feishu_app_secret="",
        tg_bot_token="",
        tg_proxy=existing_config.get("channels", {}).get("telegram", {}).get("proxy"),
        qq_app_id=existing_config.get("channels", {}).get("qqbot", {}).get("appId", ""),
        qq_client_secret="",
        task_prefix=task_prefix_override or metadata.get("taskPrefix"),
        project_dir=metadata.get("projectDir") or str(PROJECT_DIR),
        base_config="",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--theme", required=True, help="Theme directory name, e.g. corporate")
    parser.add_argument("--dir", default=str(Path.home() / ".openclaw"))
    parser.add_argument("--task-prefix", default="")
    args = parser.parse_args()

    openclaw_dir = Path(args.dir).expanduser().resolve()
    config_path = openclaw_dir / "openclaw.json"
    if not config_path.exists():
        raise SystemExit(f"Missing OpenClaw config: {config_path}. Run setup.sh first.")

    theme_file = THEMES_DIR / args.theme / "theme.json"
    if not theme_file.exists():
        raise SystemExit(f"Unknown theme: {args.theme}")

    existing_config = load_existing_config(config_path)
    metadata = load_project_metadata(openclaw_dir, existing_config=existing_config)
    current_theme_name = metadata.get("theme") or infer_theme_name_from_config(existing_config, THEMES_DIR)
    new_theme = load_theme(theme_file)
    old_theme = load_theme(THEMES_DIR / current_theme_name / "theme.json") if current_theme_name else None

    backup_dir = backup_installation(openclaw_dir, current_theme_name, new_theme["name"])
    ensure_agent_layout(openclaw_dir, new_theme)

    if old_theme:
        migrate_agent_state(openclaw_dir, old_theme, new_theme)

    deploy_runtime_scripts(openclaw_dir, new_theme)
    render_theme(
        new_theme,
        openclaw_dir,
        args.task_prefix or metadata.get("taskPrefix") or new_theme.get("task_prefix", "TASK"),
        previous_theme=old_theme,
    )

    generate_args = build_generate_args(openclaw_dir, theme_file, existing_config, metadata, args.task_prefix)
    write_config(new_theme, generate_args, existing_config=existing_config)
    rotated_sessions = 0
    if old_theme and old_theme.get("name") != new_theme.get("name"):
        rotated_sessions = reset_agent_sessions(openclaw_dir, new_theme)

    print(
        f"Switched theme: {current_theme_name or 'unknown'} -> {new_theme['name']} "
        f"(backup: {backup_dir}, reset sessions: {rotated_sessions})"
    )


if __name__ == "__main__":
    main()
