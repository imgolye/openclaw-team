#!/usr/bin/env python3
"""Resolve shared Mission Control runtime profile values."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


PATH_FIELDS = {
    "product.frontendDist",
    "openclaw.stateDir",
    "openclaw.pairingSourceDir",
}


def load_profiles(config_path: Path) -> dict:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(f"runtime profile config not found: {config_path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid runtime profile config: {config_path}: {exc}") from exc
    profiles = payload.get("profiles")
    if not isinstance(profiles, dict) or not profiles:
        raise SystemExit(f"runtime profile config has no profiles: {config_path}")
    return profiles


def dig(payload: object, field_path: str) -> object:
    current = payload
    for key in field_path.split("."):
        if not isinstance(current, dict) or key not in current:
            raise KeyError(field_path)
        current = current[key]
    return current


def resolve_path(value: str, project_dir: Path) -> str:
    expanded = Path(os.path.expanduser(value))
    if not expanded.is_absolute():
        expanded = (project_dir / expanded).resolve()
    else:
        expanded = expanded.resolve()
    return str(expanded)


def normalize_value(value: object, field_path: str, project_dir: Path) -> object:
    if isinstance(value, str) and field_path in PATH_FIELDS:
        return resolve_path(value, project_dir)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve Mission Control runtime profile values")
    parser.add_argument("--project-dir", default=".", help="Project root used for relative paths")
    parser.add_argument(
        "--config",
        default="platform/config/runtime-profiles.json",
        help="Runtime profile JSON relative to project root or absolute path",
    )
    parser.add_argument("--profile", default=os.environ.get("MISSION_CONTROL_RUNTIME_PROFILE", "host"))
    parser.add_argument("--field", default="", help="Dot path within the selected profile")
    parser.add_argument("--format", choices=("value", "json"), default="value")
    args = parser.parse_args()

    project_dir = Path(args.project_dir).expanduser().resolve()
    config_path = Path(args.config).expanduser()
    if not config_path.is_absolute():
        config_path = (project_dir / config_path).resolve()

    profiles = load_profiles(config_path)
    profile_key = str(args.profile or "").strip() or "host"
    try:
        profile = profiles[profile_key]
    except KeyError as exc:
        raise SystemExit(f"unknown runtime profile: {profile_key}") from exc

    try:
        value = dig(profile, args.field) if args.field else profile
    except KeyError as exc:
        raise SystemExit(f"missing field in runtime profile '{profile_key}': {args.field}") from exc

    normalized = normalize_value(value, args.field, project_dir) if args.field else profile
    if args.format == "json" or isinstance(normalized, (dict, list)):
        print(json.dumps(normalized, ensure_ascii=False))
    elif normalized is None:
        print("")
    else:
        print(normalized)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
