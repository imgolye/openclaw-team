#!/usr/bin/env python3
"""Seed OpenClaw agent auth profiles across all agent directories."""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
from pathlib import Path


PROVIDER_ENV_CATALOG = (
    ("zai", ("ZAI_API_KEY", "BIGMODEL_API_KEY", "ZHIPUAI_API_KEY")),
    ("openai", ("OPENAI_API_KEY",)),
    ("anthropic", ("ANTHROPIC_API_KEY",)),
    ("google", ("GEMINI_API_KEY", "GOOGLE_API_KEY")),
    ("deepseek", ("DEEPSEEK_API_KEY",)),
    ("qwen", ("QWEN_API_KEY", "DASHSCOPE_API_KEY")),
    ("openrouter", ("OPENROUTER_API_KEY",)),
    ("xai", ("XAI_API_KEY",)),
    ("minimax-cn", ("MINIMAX_CN_API_KEY", "MINIMAX_API_KEY")),
)


def load_json_text(raw: str):
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("auth payload must be a JSON object")
    profiles = payload.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise ValueError("auth payload must include non-empty profiles")
    payload.setdefault("version", 1)
    payload.setdefault("lastGood", {})
    payload.setdefault("usageStats", {})
    return payload


def load_source_payload(source_file: str):
    raw = ""
    if source_file == "-":
        raw = sys.stdin.read()
    else:
        raw = Path(source_file).expanduser().read_text(encoding="utf-8")
    return load_json_text(raw), "file"


def load_env_payload():
    raw = str(os.environ.get("OPENCLAW_AUTH_PROFILES_JSON") or "").strip()
    if raw:
        return load_json_text(raw), "env-json"
    raw_b64 = str(os.environ.get("OPENCLAW_AUTH_PROFILES_B64") or "").strip()
    if raw_b64:
        padded = raw_b64 + "=" * (-len(raw_b64) % 4)
        decoded = base64.b64decode(padded.encode("utf-8")).decode("utf-8")
        return load_json_text(decoded), "env-b64"
    return None, ""


def build_payload_from_provider_env():
    profiles = {}
    last_good = {}
    for provider, env_keys in PROVIDER_ENV_CATALOG:
        value = next((str(os.environ.get(key) or "").strip() for key in env_keys if str(os.environ.get(key) or "").strip()), "")
        if not value:
            continue
        profile_id = f"{provider}:default"
        profiles[profile_id] = {
            "type": "api_key",
            "provider": provider,
            "key": value,
        }
        last_good[provider] = profile_id
    if not profiles:
        return None
    return {
        "version": 1,
        "profiles": profiles,
        "lastGood": last_good,
        "usageStats": {},
    }


def load_config(openclaw_dir: Path):
    config_path = openclaw_dir / "openclaw.json"
    return json.loads(config_path.read_text(encoding="utf-8"))


def agent_auth_targets(openclaw_dir: Path, config: dict):
    targets = []
    for agent in config.get("agents", {}).get("list", []):
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        agent_dir_raw = str(agent.get("agentDir") or "").strip()
        agent_dir = Path(agent_dir_raw).expanduser() if agent_dir_raw else (openclaw_dir / "agents" / agent_id / "agent")
        if agent_dir.name != "agent":
            agent_dir = agent_dir / "agent"
        targets.append({"agentId": agent_id, "path": agent_dir / "auth-profiles.json"})
    return targets


def discover_existing_payload(targets):
    preferred_ids = ("assistant", "main")
    for preferred_id in preferred_ids:
        for item in targets:
            if item["agentId"] != preferred_id or not item["path"].exists():
                continue
            return load_json_text(item["path"].read_text(encoding="utf-8")), f"agent:{preferred_id}"
    for item in targets:
        if item["path"].exists():
            return load_json_text(item["path"].read_text(encoding="utf-8")), f"agent:{item['agentId']}"
    return None, ""


def write_targets(targets, payload, overwrite=False):
    written = 0
    skipped = 0
    for item in targets:
        target = item["path"]
        if target.exists() and not overwrite:
            skipped += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        target.chmod(0o600)
        written += 1
    return written, skipped


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True)
    parser.add_argument("--source-file", default="")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    openclaw_dir = Path(args.dir).expanduser().resolve()
    config = load_config(openclaw_dir)
    targets = agent_auth_targets(openclaw_dir, config)

    payload = None
    source = ""
    if args.source_file:
        payload, source = load_source_payload(args.source_file)
    else:
        payload, source = load_env_payload()
        if payload is None:
            payload, source = discover_existing_payload(targets)
        if payload is None:
            payload = build_payload_from_provider_env()
            source = "provider-env" if payload else "missing"

    if payload is None:
        result = {
            "ok": True,
            "source": source or "missing",
            "targetCount": len(targets),
            "writtenCount": 0,
            "skippedCount": len(targets),
            "providers": [],
            "message": "No auth payload source found.",
        }
    else:
        written, skipped = write_targets(targets, payload, overwrite=args.overwrite)
        providers = sorted(
            {
                str((entry or {}).get("provider") or "").strip()
                for entry in (payload.get("profiles") or {}).values()
                if isinstance(entry, dict) and str((entry or {}).get("provider") or "").strip()
            }
        )
        result = {
            "ok": True,
            "source": source,
            "targetCount": len(targets),
            "writtenCount": written,
            "skippedCount": skipped,
            "providers": providers,
            "message": "Agent auth profiles synced.",
        }

    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(result["message"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
