#!/usr/bin/env python3
"""Theme helpers for OpenClaw Team."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path


ROLE_KEYS = ("router", "planner", "reviewer", "dispatcher", "briefing")
DEPARTMENT_KEYS = (
    "engineering",
    "operations",
    "analytics",
    "communications",
    "quality",
    "hr",
)
REQUIRED_THEME_FIELDS = (
    "name",
    "display_name",
    "description",
    "language",
    "owner_title",
    "task_prefix",
    "tone",
)
REQUIRED_ROLE_FIELDS = ("agent_id", "title", "identity_name", "description", "model_tier")
VALID_MODEL_TIERS = {"primary", "light"}


def _require_string(errors, value, path):
    if not isinstance(value, str) or not value.strip():
        errors.append(f"{path} must be a non-empty string")


def _validate_tools(errors, tools, path):
    if not isinstance(tools, dict):
        errors.append(f"{path} must be an object")
        return
    for key in ("allow", "deny"):
        if key in tools and (
            not isinstance(tools[key], list) or any(not isinstance(item, str) for item in tools[key])
        ):
            errors.append(f"{path}.{key} must be a list of strings")
    if "fs" in tools:
        fs_cfg = tools["fs"]
        if not isinstance(fs_cfg, dict):
            errors.append(f"{path}.fs must be an object")
        elif "workspaceOnly" in fs_cfg and not isinstance(fs_cfg["workspaceOnly"], bool):
            errors.append(f"{path}.fs.workspaceOnly must be a boolean")


def validate_theme(theme):
    errors = []
    if not isinstance(theme, dict):
        return ["theme root must be an object"]

    for field in REQUIRED_THEME_FIELDS:
        _require_string(errors, theme.get(field), field)

    roles = theme.get("roles")
    if not isinstance(roles, dict):
        errors.append("roles must be an object")
        return errors

    agent_ids = []

    for role_key in ROLE_KEYS:
        role = roles.get(role_key)
        if not isinstance(role, dict):
            errors.append(f"roles.{role_key} must be an object")
            continue
        for field in REQUIRED_ROLE_FIELDS:
            _require_string(errors, role.get(field), f"roles.{role_key}.{field}")
        if role.get("model_tier") not in VALID_MODEL_TIERS:
            errors.append(f"roles.{role_key}.model_tier must be one of {sorted(VALID_MODEL_TIERS)}")
        if role_key == "router" and "mentionPatterns" in role:
            patterns = role.get("mentionPatterns")
            if not isinstance(patterns, list) or any(not isinstance(item, str) for item in patterns):
                errors.append("roles.router.mentionPatterns must be a list of strings")
        agent_id = role.get("agent_id")
        if isinstance(agent_id, str):
            agent_ids.append(agent_id)

    departments = roles.get("departments")
    if not isinstance(departments, dict):
        errors.append("roles.departments must be an object")
        departments = {}

    missing_departments = [key for key in DEPARTMENT_KEYS if key not in departments]
    if missing_departments:
        errors.append(f"roles.departments is missing keys: {', '.join(missing_departments)}")

    for dep_key, dep in departments.items():
        if not isinstance(dep, dict):
            errors.append(f"roles.departments.{dep_key} must be an object")
            continue
        for field in REQUIRED_ROLE_FIELDS:
            _require_string(errors, dep.get(field), f"roles.departments.{dep_key}.{field}")
        if dep.get("model_tier") not in VALID_MODEL_TIERS:
            errors.append(
                f"roles.departments.{dep_key}.model_tier must be one of {sorted(VALID_MODEL_TIERS)}"
            )
        if "sandbox" in dep and dep["sandbox"] not in ("off", "all", "non-main"):
            errors.append(f"roles.departments.{dep_key}.sandbox must be one of off/all/non-main")
        if "elevated" in dep and not isinstance(dep["elevated"], bool):
            errors.append(f"roles.departments.{dep_key}.elevated must be a boolean")
        if "tools" in dep:
            _validate_tools(errors, dep["tools"], f"roles.departments.{dep_key}.tools")
        agent_id = dep.get("agent_id")
        if isinstance(agent_id, str):
            agent_ids.append(agent_id)

    duplicate_ids = sorted({agent_id for agent_id in agent_ids if agent_ids.count(agent_id) > 1})
    if duplicate_ids:
        errors.append(f"agent_id values must be unique; duplicates: {', '.join(duplicate_ids)}")

    return errors


def load_theme(path):
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        theme = json.load(f)
    errors = validate_theme(theme)
    if errors:
        joined = "\n- ".join(errors)
        raise ValueError(f"Invalid theme file {path}:\n- {joined}")
    return theme


def get_all_agent_ids(theme):
    roles = theme["roles"]
    ordered = [roles[key]["agent_id"] for key in ROLE_KEYS]
    ordered.extend(roles["departments"][key]["agent_id"] for key in DEPARTMENT_KEYS)
    return ordered


def _iter_semantic_units(theme):
    roles = theme["roles"]
    for key in ROLE_KEYS:
        yield key, roles[key]
    for key in DEPARTMENT_KEYS:
        yield key, roles["departments"][key]


def get_semantic_entries(theme):
    return {key: deepcopy(entry) for key, entry in _iter_semantic_units(theme)}


def get_agent_id_map_by_semantic(theme):
    return {key: entry["agent_id"] for key, entry in _iter_semantic_units(theme)}


def build_value_map(theme):
    mapping = {theme["owner_title"]: theme["owner_title"]}
    for semantic_key, entry in _iter_semantic_units(theme):
        mapping[entry["title"]] = (semantic_key, "title")
        mapping[entry["identity_name"]] = (semantic_key, "identity_name")
    return mapping


def translate_theme_value(value, old_theme, new_theme):
    if not isinstance(value, str) or not value:
        return value
    if value == old_theme["owner_title"]:
        return new_theme["owner_title"]
    value_map = build_value_map(old_theme)
    mapped = value_map.get(value)
    if not mapped:
        return value
    semantic_key, field_name = mapped
    if semantic_key in ROLE_KEYS:
        return new_theme["roles"][semantic_key][field_name]
    return new_theme["roles"]["departments"][semantic_key][field_name]


def translate_text_references(text, old_theme, new_theme):
    if not isinstance(text, str) or not text:
        return text

    replacements = {}
    for semantic_key, entry in _iter_semantic_units(old_theme):
        new_entry = (
            new_theme["roles"][semantic_key]
            if semantic_key in ROLE_KEYS
            else new_theme["roles"]["departments"][semantic_key]
        )
        replacements[entry["title"]] = new_entry["title"]
        replacements[entry["identity_name"]] = new_entry["identity_name"]
    replacements[old_theme["owner_title"]] = new_theme["owner_title"]

    translated = text
    for old_value in sorted(replacements, key=len, reverse=True):
        translated = translated.replace(old_value, replacements[old_value])
    return translated


def infer_theme_name_from_config(config, themes_dir):
    metadata = config.get("missionControl", {})
    stored_name = metadata.get("theme")
    if isinstance(stored_name, str) and stored_name:
        candidate = Path(themes_dir) / stored_name / "theme.json"
        if candidate.exists():
            return stored_name

    configured_ids = {
        agent.get("id")
        for agent in config.get("agents", {}).get("list", [])
        if isinstance(agent, dict)
    }
    if not configured_ids:
        return None

    for theme_file in sorted(Path(themes_dir).glob("*/theme.json")):
        theme = load_theme(theme_file)
        if configured_ids == set(get_all_agent_ids(theme)):
            return theme["name"]
    return None


def get_router_agent_id(config):
    for agent in config.get("agents", {}).get("list", []):
        if agent.get("default"):
            return agent["id"]
    agents = config.get("agents", {}).get("list", [])
    if agents:
        return agents[0]["id"]
    return None
