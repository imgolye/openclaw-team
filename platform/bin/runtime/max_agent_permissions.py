#!/usr/bin/env python3
"""Normalize an OpenClaw config to maximum agent permissions."""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path


def load_config(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def save_config(path: Path, payload: dict):
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def derive_allow_from(config: dict):
    existing = (((config.get("tools") or {}).get("elevated") or {}).get("allowFrom"))
    if isinstance(existing, dict) and existing:
        return deepcopy(existing)
    channels = config.get("channels") if isinstance(config.get("channels"), dict) else {}
    allow_from = {}
    for name, channel in channels.items():
        values = channel.get("allowFrom") if isinstance(channel, dict) else []
        allow_from[str(name)] = values if isinstance(values, list) and values else ["*"]
    return allow_from


def normalize_config(config: dict, preset: str = "max"):
    agents = config.get("agents") if isinstance(config.get("agents"), dict) else {}
    agent_list = agents.get("list") if isinstance(agents.get("list"), list) else []
    agent_ids = [str((agent or {}).get("id") or "").strip() for agent in agent_list if str((agent or {}).get("id") or "").strip()]
    allow_from = derive_allow_from(config)
    full_tools = {"elevated": {"enabled": True, "allowFrom": deepcopy(allow_from)}}
    channels = config.get("channels") if isinstance(config.get("channels"), dict) else {}

    defaults = agents.get("defaults") if isinstance(agents.get("defaults"), dict) else {}
    defaults["elevatedDefault"] = "full"
    defaults["sandbox"] = {"mode": "off", "scope": "agent"}
    defaults["subagents"] = {
        **(defaults.get("subagents") if isinstance(defaults.get("subagents"), dict) else {}),
        "maxConcurrent": int(((defaults.get("subagents") or {}).get("maxConcurrent") or 8)),
    }
    agents["defaults"] = defaults

    changed_agents = 0
    for agent in agent_list:
        if not isinstance(agent, dict):
            continue
        changed_agents += 1
        agent_id = str(agent.get("id") or "").strip()
        agent["sandbox"] = {"mode": "off", "scope": "agent"}
        agent["tools"] = deepcopy(full_tools)
        agent["subagents"] = {
            **(agent.get("subagents") if isinstance(agent.get("subagents"), dict) else {}),
            "allowAgents": [item for item in agent_ids if item and item != agent_id],
        }

    config["agents"] = agents
    tools = config.get("tools") if isinstance(config.get("tools"), dict) else {}
    tools["profile"] = "full"
    tools["agentToAgent"] = {"enabled": True, "allow": agent_ids}
    tools["elevated"] = {"enabled": True, "allowFrom": deepcopy(allow_from)}
    config["tools"] = tools

    commands = config.get("commands") if isinstance(config.get("commands"), dict) else {}
    commands["native"] = True
    commands["nativeSkills"] = True
    commands["text"] = True
    commands["restart"] = True
    commands["ownerDisplay"] = "raw"
    config["commands"] = commands

    for name, channel in list(channels.items()):
        if not isinstance(channel, dict):
            continue
        if preset == "single_tenant_prod":
            channel.setdefault("allowFrom", [])
            if "groupAllowFrom" in channel or "groupPolicy" in channel:
                channel.setdefault("groupAllowFrom", [])
                channel.setdefault("groupPolicy", "allowlist")
            if "dmPolicy" in channel:
                channel.setdefault("dmPolicy", "pairing")
        else:
            channel["allowFrom"] = ["*"]
            if "groupAllowFrom" in channel or "groupPolicy" in channel:
                channel["groupAllowFrom"] = ["*"]
                channel["groupPolicy"] = "open"
            if "dmPolicy" in channel:
                channel["dmPolicy"] = "open"
        if str(name) == "feishu":
            channel.pop("commands", None)
        else:
            channel_commands = channel.get("commands") if isinstance(channel.get("commands"), dict) else {}
            channel_commands["native"] = True
            channel_commands["nativeSkills"] = True
            channel["commands"] = channel_commands
        channels[str(name)] = channel
    config["channels"] = channels
    return config, changed_agents, agent_ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="OpenClaw state directory containing openclaw.json")
    parser.add_argument("--preset", choices=("max", "single_tenant_prod"), default="max")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    config_path = Path(args.dir).expanduser().resolve() / "openclaw.json"
    config = load_config(config_path)
    normalized, changed_agents, agent_ids = normalize_config(config, preset=args.preset)
    save_config(config_path, normalized)

    result = {
        "ok": True,
        "configPath": str(config_path),
        "agentCount": changed_agents,
        "agentIds": agent_ids,
        "preset": args.preset,
        "message": "Agent permissions normalized.",
    }
    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(result["message"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
