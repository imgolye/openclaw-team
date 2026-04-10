#!/usr/bin/env python3
"""Install and detect OpenClaw for OpenClaw Team."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
LIB_DIR = SCRIPT_DIR / "lib"
if str(LIB_DIR) not in sys.path:
    sys.path.insert(0, str(LIB_DIR))

from project_metadata import load_project_metadata, write_project_metadata  # noqa: E402


METHOD_ORDER = ("managed-local", "npm-global", "pnpm-global", "bun-global")
METHOD_LABELS = {
    "managed-local": "Managed local (recommended)",
    "npm-global": "npm global",
    "pnpm-global": "pnpm global",
    "bun-global": "bun global",
    "system-path": "System PATH",
    "missing": "Not installed",
}
METHOD_DESCRIPTIONS = {
    "managed-local": "Install OpenClaw into the OpenClaw Team state directory without relying on a global shell path.",
    "npm-global": "Use the global npm prefix on this machine.",
    "pnpm-global": "Use pnpm global installation on this machine.",
    "bun-global": "Use bun global installation on this machine.",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def run(args, env=None):
    process = subprocess.run(
        [str(arg) for arg in args],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    output = "\n".join(
        part.strip()
        for part in (process.stdout, process.stderr)
        if part and part.strip()
    ).strip()
    return process, output


def package_spec(version: str) -> str:
    normalized = str(version or "").strip()
    if not normalized or normalized == "latest":
        return "openclaw@latest"
    if normalized.startswith("openclaw@"):
        return normalized
    return f"openclaw@{normalized}"


def openclaw_dir_path(openclaw_dir: str) -> Path:
    return Path(openclaw_dir).expanduser().resolve()


def managed_root(openclaw_dir: str) -> Path:
    return openclaw_dir_path(openclaw_dir) / ".runtime" / "openclaw-managed"


def managed_bin_dir(openclaw_dir: str) -> Path:
    return managed_root(openclaw_dir) / "bin"


def wrapper_dir(openclaw_dir: str) -> Path:
    return openclaw_dir_path(openclaw_dir) / "bin"


def wrapper_path(openclaw_dir: str) -> Path:
    return wrapper_dir(openclaw_dir) / "openclaw"


def managed_cli_candidates(openclaw_dir: str) -> list[Path]:
    root = managed_root(openclaw_dir)
    return [
        root / "bin" / "openclaw",
        root / "node_modules" / ".bin" / "openclaw",
    ]


def existing_first(paths):
    for path in paths:
        candidate = Path(path)
        if candidate.exists():
            return candidate
    return None


def release_parts(raw_version: str):
    raw = str(raw_version or "").strip()
    if raw.startswith("OpenClaw "):
        suffix = raw.split("OpenClaw ", 1)[1]
        if " (" in suffix and suffix.endswith(")"):
            release, build = suffix[:-1].split(" (", 1)
            return release, build
        return suffix, ""
    return "", ""


def version_payload(cli_path: Path):
    candidate = Path(cli_path)
    if not candidate.exists():
        return {"raw": "", "release": "", "build": "", "ok": False}
    process, output = run([str(candidate), "--version"])
    release, build = release_parts(output)
    return {
        "raw": output,
        "release": release,
        "build": build,
        "ok": process.returncode == 0 and bool(output),
    }


def infer_method_from_path(cli_path: str, openclaw_dir: str) -> str:
    candidate = Path(cli_path).expanduser().resolve()
    base_dir = openclaw_dir_path(openclaw_dir)
    if candidate == wrapper_path(openclaw_dir).resolve() or str(candidate).startswith(str(managed_root(openclaw_dir))):
        return "managed-local"
    path_text = str(candidate)
    if ".bun" in path_text or "/bun/" in path_text:
        return "bun-global"
    if "pnpm" in path_text:
        return "pnpm-global"
    if "node_modules" in path_text:
        return "npm-global"
    if str(candidate).startswith(str(base_dir)):
        return "managed-local"
    return "system-path"


def npm_global_cli():
    if not shutil.which("npm"):
        return None
    process, output = run(["npm", "prefix", "-g"])
    if process.returncode != 0 or not output:
        return None
    candidate = Path(output.strip()) / "bin" / "openclaw"
    return candidate if candidate.exists() else None


def pnpm_global_cli():
    if not shutil.which("pnpm"):
        return None
    process, output = run(["pnpm", "bin", "-g"])
    if process.returncode != 0 or not output:
        return None
    candidate = Path(output.strip()) / "openclaw"
    return candidate if candidate.exists() else None


def bun_global_cli():
    if not shutil.which("bun"):
        return None
    candidates = []
    bun_install_root = str(os.environ.get("BUN_INSTALL") or "").strip()
    if bun_install_root:
        candidates.append(Path(bun_install_root) / "bin" / "openclaw")
    candidates.append(Path.home() / ".bun" / "bin" / "openclaw")
    return existing_first(candidates)


def available_methods(openclaw_dir: str, version: str = "latest"):
    spec = package_spec(version)
    root = managed_root(openclaw_dir)
    methods = []
    methods.append(
        {
            "id": "managed-local",
            "label": METHOD_LABELS["managed-local"],
            "description": METHOD_DESCRIPTIONS["managed-local"],
            "available": bool(shutil.which("npm")),
            "installerTool": "npm",
            "commandPreview": f"npm install --global --prefix {root} {spec}",
        }
    )
    methods.append(
        {
            "id": "npm-global",
            "label": METHOD_LABELS["npm-global"],
            "description": METHOD_DESCRIPTIONS["npm-global"],
            "available": bool(shutil.which("npm")),
            "installerTool": "npm",
            "commandPreview": f"npm install -g {spec}",
        }
    )
    methods.append(
        {
            "id": "pnpm-global",
            "label": METHOD_LABELS["pnpm-global"],
            "description": METHOD_DESCRIPTIONS["pnpm-global"],
            "available": bool(shutil.which("pnpm")),
            "installerTool": "pnpm",
            "commandPreview": f"pnpm add -g {spec}",
        }
    )
    methods.append(
        {
            "id": "bun-global",
            "label": METHOD_LABELS["bun-global"],
            "description": METHOD_DESCRIPTIONS["bun-global"],
            "available": bool(shutil.which("bun")),
            "installerTool": "bun",
            "commandPreview": f"bun install -g {spec}",
        }
    )
    return methods


def recommended_method(methods):
    for method_id in METHOD_ORDER:
        match = next((item for item in methods if item["id"] == method_id and item.get("available")), None)
        if match:
            return method_id
    return ""


def ensure_wrapper(openclaw_dir: str, target_cli: Path):
    destination = wrapper_path(openclaw_dir)
    destination.parent.mkdir(parents=True, exist_ok=True)
    script = f"""#!/usr/bin/env bash
set -euo pipefail
exec "{str(target_cli)}" "$@"
"""
    destination.write_text(script, encoding="utf-8")
    current_mode = destination.stat().st_mode
    destination.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return destination


def detect_installation(openclaw_dir: str, version: str = "latest"):
    metadata = load_project_metadata(openclaw_dir)
    install_meta = metadata.get("openclawInstall") if isinstance(metadata.get("openclawInstall"), dict) else {}
    methods = available_methods(openclaw_dir, version=version)
    recommended = recommended_method(methods)
    recorded_cli = Path(str(install_meta.get("cliPath") or "")).expanduser() if str(install_meta.get("cliPath") or "").strip() else None
    candidates = []
    if recorded_cli:
        candidates.append(recorded_cli)
    wrapper = wrapper_path(openclaw_dir)
    if wrapper.exists():
        candidates.append(wrapper)
    managed_cli = existing_first(managed_cli_candidates(openclaw_dir))
    if managed_cli:
        candidates.append(managed_cli)
    npm_cli = npm_global_cli()
    if npm_cli:
        candidates.append(npm_cli)
    pnpm_cli = pnpm_global_cli()
    if pnpm_cli:
        candidates.append(pnpm_cli)
    bun_cli = bun_global_cli()
    if bun_cli:
        candidates.append(bun_cli)
    path_cli = shutil.which("openclaw")
    if path_cli:
        candidates.append(Path(path_cli))

    chosen = None
    seen = set()
    for candidate in candidates:
        resolved = str(Path(candidate).expanduser())
        if resolved in seen:
            continue
        seen.add(resolved)
        if Path(resolved).exists():
            chosen = Path(resolved)
            break

    state = "ready" if chosen else "missing"
    if chosen:
        method = str(install_meta.get("method") or "").strip() or infer_method_from_path(chosen, openclaw_dir)
        cli_version = version_payload(chosen)
    else:
        method = "missing"
        cli_version = {"raw": "", "release": "", "build": "", "ok": False}

    return {
        "state": state,
        "installed": bool(chosen),
        "method": method,
        "label": METHOD_LABELS.get(method, method or METHOD_LABELS["missing"]),
        "cliPath": str(chosen) if chosen else "",
        "managed": method == "managed-local",
        "version": cli_version,
        "availableMethods": methods,
        "recommendedMethod": recommended,
        "managedRoot": str(managed_root(openclaw_dir)),
        "managedBinDir": str(managed_bin_dir(openclaw_dir)),
        "wrapperPath": str(wrapper_path(openclaw_dir)),
        "lastInstalledAt": str(install_meta.get("installedAt") or ""),
        "lastUpdatedAt": str(install_meta.get("updatedAt") or ""),
        "installerTool": str(install_meta.get("installerTool") or ""),
        "error": "" if chosen else "openclaw cli missing",
    }


def persist_installation_metadata(openclaw_dir: str, detection, method: str, installer_tool: str, package_name: str):
    metadata = load_project_metadata(openclaw_dir)
    current = metadata.get("openclawInstall") if isinstance(metadata.get("openclawInstall"), dict) else {}
    installed_at = str(current.get("installedAt") or "")
    timestamp = now_iso()
    metadata["openclawInstall"] = {
        **current,
        "method": method,
        "label": METHOD_LABELS.get(method, method),
        "cliPath": detection.get("cliPath", ""),
        "managedRoot": detection.get("managedRoot", ""),
        "managedBinDir": detection.get("managedBinDir", ""),
        "wrapperPath": detection.get("wrapperPath", ""),
        "installerTool": installer_tool,
        "package": package_name,
        "installedAt": installed_at or timestamp,
        "updatedAt": timestamp,
    }
    write_project_metadata(openclaw_dir, metadata)


def install_openclaw(openclaw_dir: str, method: str, version: str):
    methods = {item["id"]: item for item in available_methods(openclaw_dir, version=version)}
    selected_method = str(method or "").strip()
    if not selected_method or selected_method == "auto":
        selected_method = recommended_method(list(methods.values()))
    if not selected_method:
        raise RuntimeError("No available OpenClaw installation method was detected on this machine.")
    selected = methods.get(selected_method)
    if not selected or not selected.get("available"):
        raise RuntimeError(f"Requested installation method is not available: {selected_method}")

    spec = package_spec(version)
    cli_path = None
    process = None
    output = ""
    installer_tool = str(selected.get("installerTool") or "").strip()
    env = os.environ.copy()

    if selected_method == "managed-local":
        root = managed_root(openclaw_dir)
        root.mkdir(parents=True, exist_ok=True)
        process, output = run(["npm", "install", "--global", "--prefix", str(root), spec], env=env)
        if process.returncode != 0:
            raise RuntimeError(output or "OpenClaw managed-local install failed.")
        real_cli = existing_first(managed_cli_candidates(openclaw_dir))
        if not real_cli:
            raise RuntimeError("OpenClaw managed-local install completed but no executable was found.")
        cli_path = ensure_wrapper(openclaw_dir, real_cli)
    elif selected_method == "npm-global":
        process, output = run(["npm", "install", "-g", spec], env=env)
        if process.returncode != 0:
            raise RuntimeError(output or "OpenClaw npm global install failed.")
        cli_path = npm_global_cli() or (Path(shutil.which("openclaw")) if shutil.which("openclaw") else None)
    elif selected_method == "pnpm-global":
        process, output = run(["pnpm", "add", "-g", spec], env=env)
        if process.returncode != 0:
            raise RuntimeError(output or "OpenClaw pnpm global install failed.")
        cli_path = pnpm_global_cli() or (Path(shutil.which("openclaw")) if shutil.which("openclaw") else None)
    elif selected_method == "bun-global":
        process, output = run(["bun", "install", "-g", spec], env=env)
        if process.returncode != 0:
            raise RuntimeError(output or "OpenClaw bun global install failed.")
        cli_path = bun_global_cli() or (Path(shutil.which("openclaw")) if shutil.which("openclaw") else None)
    else:
        raise RuntimeError(f"Unsupported installation method: {selected_method}")

    detection = detect_installation(openclaw_dir, version=version)
    if cli_path and not detection.get("cliPath"):
        detection["cliPath"] = str(cli_path)
    if not detection.get("installed"):
        raise RuntimeError("OpenClaw install finished, but OpenClaw Team still could not detect the CLI.")
    persist_installation_metadata(openclaw_dir, detection, selected_method, installer_tool, spec)
    detection = detect_installation(openclaw_dir, version=version)
    return {
        "ok": True,
        "action": "install",
        "method": selected_method,
        "installerTool": installer_tool,
        "package": spec,
        "command": selected.get("commandPreview"),
        "output": output,
        "installation": detection,
    }


def update_openclaw(openclaw_dir: str, method: str, version: str):
    return {
        **install_openclaw(openclaw_dir, method=method, version=version),
        "action": "update",
    }


def emit(payload, as_json: bool):
    if as_json:
        print(json.dumps(payload, ensure_ascii=False))
        return
    if payload.get("ok") is False:
        print(payload.get("error") or "error")
        return
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(description="OpenClaw Team OpenClaw installer")
    parser.add_argument("action", choices=["detect", "install", "update"])
    parser.add_argument("--dir", dest="openclaw_dir", default=str(Path.home() / ".openclaw"))
    parser.add_argument("--method", default="auto")
    parser.add_argument("--version", default="latest")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    try:
        if args.action == "detect":
            payload = {"ok": True, **detect_installation(args.openclaw_dir, version=args.version)}
        elif args.action == "install":
            payload = install_openclaw(args.openclaw_dir, args.method, args.version)
        else:
            payload = update_openclaw(args.openclaw_dir, args.method, args.version)
        emit(payload, args.json)
        return 0
    except Exception as error:
        payload = {"ok": False, "error": str(error)}
        emit(payload, args.json)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
