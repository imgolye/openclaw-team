#!/usr/bin/env python3
"""Generate and optionally merge OpenClaw Team OpenClaw config."""

from __future__ import annotations

import argparse
import json
import secrets
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from project_metadata import extract_legacy_metadata, load_project_metadata, sanitize_openclaw_config, write_project_metadata
from theme_utils import load_theme


PROJECT_VERSION = "1.8.0"
DEPLOYMENT_MODES = {"single_tenant", "shared_platform"}


def load_existing_config(path):
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


def deep_merge(base, overlay):
    if isinstance(base, dict) and isinstance(overlay, dict):
        merged = deepcopy(base)
        for key, value in overlay.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged
    return deepcopy(overlay)


def _existing_defaults(existing_config):
    return existing_config.get("agents", {}).get("defaults", {})


def resolve_primary_model(args, existing_config):
    if args.primary_model:
        return args.primary_model
    primary = _existing_defaults(existing_config).get("model", {}).get("primary")
    return primary or "gemma-4-e2b-edge"


def resolve_light_model(args, existing_config):
    if args.light_model:
        return args.light_model
    fallbacks = _existing_defaults(existing_config).get("model", {}).get("fallbacks", [])
    return fallbacks[0] if fallbacks else "gemma-4-e2b-edge"


def resolve_task_prefix(args, existing_config, theme):
    if args.task_prefix:
        return args.task_prefix
    metadata = extract_legacy_metadata(existing_config)
    existing_prefix = metadata.get("taskPrefix")
    if existing_prefix:
        return existing_prefix
    return theme.get("task_prefix", "TASK")


def resolve_project_dir(args, existing_config):
    if getattr(args, "project_dir", ""):
        return str(Path(args.project_dir).expanduser().resolve())
    metadata = extract_legacy_metadata(existing_config)
    project_dir = metadata.get("projectDir")
    if project_dir:
        return str(Path(project_dir).expanduser().resolve())
    return ""


def resolve_deployment_mode(args, existing_config):
    requested = str(getattr(args, "deployment_mode", "") or "").strip().lower()
    if requested in DEPLOYMENT_MODES:
        return requested
    metadata = extract_legacy_metadata(existing_config)
    existing_mode = str(metadata.get("deploymentMode") or "").strip().lower()
    if existing_mode in DEPLOYMENT_MODES:
        return existing_mode
    return "single_tenant"


def resolve_deployment_profile(args, existing_config):
    requested = str(getattr(args, "deployment_profile", "") or "").strip()
    if requested:
        return requested
    metadata = extract_legacy_metadata(existing_config)
    existing_profile = str(metadata.get("deploymentProfile") or "").strip()
    if existing_profile:
        return existing_profile
    return "standard"


def resolve_memory_search(existing_config):
    memory_search = deepcopy(_existing_defaults(existing_config).get("memorySearch"))
    if memory_search:
        return memory_search
    return {"enabled": True, "provider": "local"}


def resolve_elevated_default(existing_config):
    return _existing_defaults(existing_config).get("elevatedDefault", "full")


def resolve_timeout(existing_config):
    return _existing_defaults(existing_config).get("timeoutSeconds", 300)


def _existing_channel(existing_config, name):
    return deepcopy(existing_config.get("channels", {}).get(name, {}))


def build_channels(args, existing_config):
    channels = {}

    feishu_existing = _existing_channel(existing_config, "feishu")
    if args.feishu_app_id or feishu_existing:
        feishu = feishu_existing
        feishu["enabled"] = True
        feishu["appId"] = args.feishu_app_id or feishu_existing.get("appId", "")
        feishu["appSecret"] = "${FEISHU_APP_SECRET}"
        feishu.setdefault("domain", "feishu")
        feishu.setdefault("groupPolicy", "allowlist")
        feishu.setdefault("groupAllowFrom", [])
        feishu.setdefault("dmPolicy", "pairing")
        feishu.setdefault("allowFrom", ["*"])
        channels["feishu"] = feishu

    tg_existing = _existing_channel(existing_config, "telegram")
    if args.tg_bot_token or tg_existing:
        tg = tg_existing
        tg["enabled"] = True
        tg["botToken"] = "${TELEGRAM_BOT_TOKEN}"
        if args.tg_proxy is not None:
            if args.tg_proxy:
                tg["proxy"] = args.tg_proxy
            else:
                tg.pop("proxy", None)
        tg.setdefault("commands", {"native": True, "nativeSkills": False})
        tg.setdefault("dmPolicy", "pairing")
        tg.setdefault("allowFrom", ["*"])
        tg.setdefault("groupPolicy", "allowlist")
        tg.setdefault("groupAllowFrom", [])
        tg.setdefault("streaming", "off")
        channels["telegram"] = tg

    qq_existing = _existing_channel(existing_config, "qqbot")
    if args.qq_app_id or qq_existing:
        qq = qq_existing
        qq["enabled"] = True
        qq["appId"] = args.qq_app_id or qq_existing.get("appId", "")
        qq["clientSecret"] = "${QQBOT_CLIENT_SECRET}"
        qq.setdefault("allowFrom", ["*"])
        channels["qqbot"] = qq

    return channels


def _build_agent_tools(dep_info, default_elevated_allow_from):
    return {"elevated": {"enabled": True, "allowFrom": deepcopy(default_elevated_allow_from)}}


def build_default_elevated_allow_from(existing_config, channels):
    existing = existing_config.get("tools", {}).get("elevated", {}).get("allowFrom")
    if isinstance(existing, dict) and existing:
        return deepcopy(existing)

    derived = {}
    for channel_name, channel_cfg in channels.items():
        allow_from = deepcopy(channel_cfg.get("allowFrom", []))
        if allow_from == ["*"]:
            allow_from = []
        derived[channel_name] = allow_from
    return derived


def build_generated_config(theme, args, existing_config=None):
    existing_config = existing_config or {}
    roles = theme["roles"]
    router = roles["router"]
    planner = roles["planner"]
    reviewer = roles["reviewer"]
    dispatcher = roles["dispatcher"]
    briefing = roles["briefing"]
    departments = roles["departments"]
    oc_dir = args.openclaw_dir

    primary_model = resolve_primary_model(args, existing_config)
    light_model = resolve_light_model(args, existing_config)
    task_prefix = resolve_task_prefix(args, existing_config, theme)
    project_dir = resolve_project_dir(args, existing_config)

    all_ids = [
        router["agent_id"],
        planner["agent_id"],
        reviewer["agent_id"],
        dispatcher["agent_id"],
        briefing["agent_id"],
    ] + [department["agent_id"] for department in departments.values()]
    department_ids = [department["agent_id"] for department in departments.values()]
    all_other_agents = {
        agent_id: [candidate for candidate in all_ids if candidate and candidate != agent_id]
        for agent_id in all_ids
    }

    channels = build_channels(args, existing_config)
    default_elevated_allow_from = build_default_elevated_allow_from(existing_config, channels)

    def model_for(role_info):
        return primary_model if role_info.get("model_tier") == "primary" else light_model

    agents_list = [
        {
            "id": router["agent_id"],
            "default": True,
            "workspace": f"{oc_dir}/workspace-{router['agent_id']}",
            "agentDir": f"{oc_dir}/agents/{router['agent_id']}/agent",
            "model": model_for(router),
            "identity": {"name": router["identity_name"]},
            "heartbeat": {
                "every": "30m",
                "target": "none",
                "lightContext": True,
                "directPolicy": "allow",
                "prompt": "Check for active tasks. If none, reply HEARTBEAT_OK.",
            },
            "subagents": {"allowAgents": all_other_agents.get(router["agent_id"], [])},
            "groupChat": {"mentionPatterns": router.get("mentionPatterns", [])},
            "tools": {"elevated": {"enabled": True, "allowFrom": default_elevated_allow_from}},
        },
        {
            "id": planner["agent_id"],
            "workspace": f"{oc_dir}/workspace-{planner['agent_id']}",
            "agentDir": f"{oc_dir}/agents/{planner['agent_id']}/agent",
            "model": model_for(planner),
            "identity": {"name": planner["identity_name"]},
            "heartbeat": {
                "every": "30m",
                "target": "none",
                "lightContext": True,
                "directPolicy": "allow",
                "prompt": "Check for active tasks (Doing/Assigned/Blocked). If none, reply HEARTBEAT_OK.",
            },
            "subagents": {"allowAgents": all_other_agents.get(planner["agent_id"], [])},
            "tools": {"elevated": {"enabled": True, "allowFrom": deepcopy(default_elevated_allow_from)}},
        },
        {
            "id": reviewer["agent_id"],
            "workspace": f"{oc_dir}/workspace-{reviewer['agent_id']}",
            "agentDir": f"{oc_dir}/agents/{reviewer['agent_id']}/agent",
            "model": model_for(reviewer),
            "identity": {"name": reviewer["identity_name"]},
            "subagents": {"allowAgents": all_other_agents.get(reviewer["agent_id"], [])},
            "tools": {"elevated": {"enabled": True, "allowFrom": deepcopy(default_elevated_allow_from)}},
        },
        {
            "id": dispatcher["agent_id"],
            "workspace": f"{oc_dir}/workspace-{dispatcher['agent_id']}",
            "agentDir": f"{oc_dir}/agents/{dispatcher['agent_id']}/agent",
            "model": model_for(dispatcher),
            "identity": {"name": dispatcher["identity_name"]},
            "subagents": {"allowAgents": all_other_agents.get(dispatcher["agent_id"], [])},
            "tools": {"elevated": {"enabled": True, "allowFrom": default_elevated_allow_from}},
        },
    ]

    for department in departments.values():
        agent_cfg = {
            "id": department["agent_id"],
            "workspace": f"{oc_dir}/workspace-{department['agent_id']}",
            "agentDir": f"{oc_dir}/agents/{department['agent_id']}/agent",
            "model": model_for(department),
            "identity": {"name": department["identity_name"]},
            "subagents": {"allowAgents": all_other_agents.get(department["agent_id"], [])},
            "tools": {"elevated": {"enabled": True, "allowFrom": deepcopy(default_elevated_allow_from)}},
        }
        agents_list.append(agent_cfg)

    agents_list.append(
        {
            "id": briefing["agent_id"],
            "workspace": f"{oc_dir}/workspace-{briefing['agent_id']}",
            "agentDir": f"{oc_dir}/agents/{briefing['agent_id']}/agent",
            "model": model_for(briefing),
            "identity": {"name": briefing["identity_name"]},
            "subagents": {"allowAgents": all_other_agents.get(briefing["agent_id"], [])},
            "tools": {"elevated": {"enabled": True, "allowFrom": deepcopy(default_elevated_allow_from)}},
        }
    )

    gateway_auth_token = (
        existing_config.get("gateway", {}).get("auth", {}).get("token") or "${GATEWAY_AUTH_TOKEN}"
    )

    generated = {
        "agents": {
            "defaults": {
                "model": {
                    "primary": primary_model,
                    "fallbacks": [light_model],
                },
                "workspace": f"{oc_dir}/workspace",
                "memorySearch": resolve_memory_search(existing_config),
                "compaction": {"mode": "safeguard"},
                "contextPruning": {"mode": "cache-ttl", "ttl": "1h"},
                "elevatedDefault": resolve_elevated_default(existing_config),
                "sandbox": {"mode": "off", "scope": "agent"},
                "timeoutSeconds": resolve_timeout(existing_config),
                "heartbeat": {"every": "30m"},
                "maxConcurrent": 4,
                "subagents": {"maxConcurrent": 8},
            },
            "list": agents_list,
        },
        "tools": {
            "profile": "full",
            "sessions": {"visibility": "all"},
            "agentToAgent": {"enabled": True, "allow": all_ids},
            "elevated": {"enabled": True, "allowFrom": default_elevated_allow_from},
        },
        "session": {
            "dmScope": "per-channel-peer",
            "agentToAgent": {"maxPingPongTurns": 3},
            "maintenance": {
                "mode": "warn",
                "pruneAfter": "30d",
                "maxEntries": 500,
                "rotateBytes": "10mb",
            },
            "threadBindings": {"enabled": True, "idleHours": 4, "maxAgeHours": 24},
            "reset": {"mode": "daily", "atHour": 4, "idleMinutes": 120},
        },
        "cron": {
            "enabled": True,
            "maxConcurrentRuns": 2,
            "sessionRetention": "24h",
            "runLog": {"maxBytes": "2mb", "keepLines": 2000},
        },
        "commands": {
            "native": "auto",
            "nativeSkills": "auto",
            "text": True,
            "restart": True,
            "ownerDisplay": "raw",
        },
        "messages": {"ackReactionScope": "group-mentions"},
        "channels": channels,
        "browser": {
            "enabled": True,
            "defaultProfile": "openclaw",
        },
        "gateway": {
            "port": 18789,
            "mode": "local",
            "bind": "loopback",
            "auth": {"mode": "token", "token": gateway_auth_token},
            "http": {
                "endpoints": {
                    "chatCompletions": {"enabled": True},
                },
            },
            "trustedProxies": ["127.0.0.1"],
            "reload": {"mode": "hybrid", "debounceMs": 300},
        },
        "logging": {"redactSensitive": "tools"},
        "plugins": {
            "entries": {
                **{channel_name: {"enabled": True} for channel_name in channels},
                "browser": {"enabled": True},
            },
        },
    }

    if existing_config:
        return sanitize_openclaw_config(deep_merge(existing_config, generated))
    return generated


def write_config(theme, args, existing_config=None):
    config = build_generated_config(theme, args, existing_config=existing_config)
    config_path = Path(args.openclaw_dir) / "openclaw.json"
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    router_agent_id = next((agent.get("id", "") for agent in config.get("agents", {}).get("list", []) if agent.get("default")), "")
    existing_metadata = load_project_metadata(args.openclaw_dir, existing_config=existing_config or {})
    write_project_metadata(
        args.openclaw_dir,
        {
            **existing_metadata,
            "version": PROJECT_VERSION,
            "theme": theme["name"],
            "displayName": theme["display_name"],
            "taskPrefix": resolve_task_prefix(args, existing_config or {}, theme),
            "projectDir": resolve_project_dir(args, existing_config or {}),
            "routerAgentId": router_agent_id,
            "productMode": str(existing_metadata.get("productMode") or "team").strip() or "team",
            "teamModeDefault": bool(existing_metadata.get("teamModeDefault", True)),
            "deploymentMode": resolve_deployment_mode(args, existing_config or {}),
            "deploymentProfile": resolve_deployment_profile(args, existing_config or {}),
            "generatedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
    )
    print(f"Generated {config_path} with {len(config.get('agents', {}).get('list', []))} agents")
    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--theme", required=True)
    parser.add_argument("--openclaw-dir", required=True)
    parser.add_argument("--primary-model", default=None)
    parser.add_argument("--light-model", default=None)
    parser.add_argument("--feishu-app-id", default="")
    parser.add_argument("--feishu-app-secret", default="")
    parser.add_argument("--tg-bot-token", default="")
    parser.add_argument("--tg-proxy", default=None)
    parser.add_argument("--qq-app-id", default="")
    parser.add_argument("--qq-client-secret", default="")
    parser.add_argument("--task-prefix", default=None)
    parser.add_argument("--project-dir", default="")
    parser.add_argument("--base-config", default="")
    parser.add_argument("--deployment-mode", default="")
    parser.add_argument("--deployment-profile", default="")
    args = parser.parse_args()

    theme = load_theme(args.theme)
    existing_config = load_existing_config(args.base_config)
    write_config(theme, args, existing_config=existing_config)


if __name__ == "__main__":
    main()
