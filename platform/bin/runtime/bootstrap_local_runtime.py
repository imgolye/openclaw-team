#!/usr/bin/env python3
"""Bootstrap a configured local model runtime if mission-control says it should run."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import subprocess
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _install_lib_dir() -> Path:
    return _repo_root() / "platform" / "bin" / "install" / "lib"


def _load_project_metadata_module():
    install_lib = str(_install_lib_dir())
    if install_lib not in sys.path:
        sys.path.insert(0, install_lib)
    from project_metadata import load_project_metadata, normalize_project_metadata, write_project_metadata  # noqa: WPS433

    return load_project_metadata, normalize_project_metadata, write_project_metadata


def _read_json(path: Path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _launcher_path(project_dir: Path) -> Path:
    candidates = [
        project_dir / "platform" / "bin" / "runtime" / "launch_local_model_runtime.py",
        Path("/app/platform/bin/runtime/launch_local_model_runtime.py"),
        _repo_root() / "platform" / "bin" / "runtime" / "launch_local_model_runtime.py",
    ]
    current_file = Path(__file__).resolve()
    for parent in current_file.parents:
        candidates.append(parent / "platform" / "bin" / "runtime" / "launch_local_model_runtime.py")
    seen = set()
    for candidate in candidates:
        normalized = candidate.resolve() if candidate.exists() else candidate
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Missing launch_local_model_runtime.py")


def _runtime_config(metadata):
    runtime = metadata.get("localRuntime") if isinstance(metadata, dict) else {}
    return runtime if isinstance(runtime, dict) else {}


def _runtime_enabled(runtime):
    return bool(runtime.get("enabled"))


def _runtime_configured(runtime):
    return bool(str(runtime.get("entrypoint") or "").strip() and str(runtime.get("modelPath") or "").strip())


def _runtime_running(pid_value):
    try:
        pid = int(str(pid_value or "").strip())
    except ValueError:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _build_launch_args(runtime, launcher: Path):
    args = [
        "python3",
        str(launcher),
        "--backend",
        str(runtime.get("backend") or "llama_cpp"),
        "--entrypoint",
        str(runtime.get("entrypoint") or ""),
        "--model",
        str(runtime.get("modelPath") or ""),
        "--host",
        str(runtime.get("host") or "127.0.0.1"),
        "--port",
        str(runtime.get("port") or 8080),
        "--ctx-size",
        str(runtime.get("contextLength") or 8192),
        "--gpu-layers",
        str(runtime.get("gpuLayers") if runtime.get("gpuLayers") is not None else 0),
        "--extra-args-json",
        json.dumps(runtime.get("extraArgs") if isinstance(runtime.get("extraArgs"), list) else [], ensure_ascii=False),
        "--json",
    ]
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    if kv_cache.get("enabled"):
        key_type = str(kv_cache.get("keyType") or "").strip()
        value_type = str(kv_cache.get("valueType") or "").strip()
        if key_type:
            args.extend(["--cache-type-k", key_type])
        if value_type:
            args.extend(["--cache-type-v", value_type])
    return args


def _runtime_env(runtime, openclaw_dir: Path):
    env = os.environ.copy()
    env["OPENCLAW_STATE_DIR"] = str(openclaw_dir)
    env["OPENCLAW_CONFIG_PATH"] = str(openclaw_dir / "openclaw.json")
    env["MISSION_CONTROL_LOCAL_RUNTIME_ENABLED"] = "1" if runtime.get("enabled") else "0"
    env["MISSION_CONTROL_LOCAL_RUNTIME_BACKEND"] = str(runtime.get("backend") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_ENTRYPOINT"] = str(runtime.get("entrypoint") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_MODEL"] = str(runtime.get("modelPath") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_HOST"] = str(runtime.get("host") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_PORT"] = str(runtime.get("port") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_CTX_SIZE"] = str(runtime.get("contextLength") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_GPU_LAYERS"] = str(runtime.get("gpuLayers") if runtime.get("gpuLayers") is not None else "")
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    env["MISSION_CONTROL_LOCAL_RUNTIME_KV_CACHE_ENABLED"] = "1" if kv_cache.get("enabled") else "0"
    env["MISSION_CONTROL_LOCAL_RUNTIME_KV_CACHE_MODE"] = str(kv_cache.get("mode") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_CACHE_TYPE_K"] = str(kv_cache.get("keyType") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_CACHE_TYPE_V"] = str(kv_cache.get("valueType") or "")
    env["MISSION_CONTROL_LOCAL_RUNTIME_EXTRA_ARGS_JSON"] = json.dumps(runtime.get("extraArgs") if isinstance(runtime.get("extraArgs"), list) else [], ensure_ascii=False)
    return env


def _print(result, as_json=False):
    if as_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(result.get("message") or "")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True)
    parser.add_argument("--project-dir", default="")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--print-command", action="store_true")
    parser.add_argument("--wait-seconds", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_project_metadata, normalize_project_metadata, write_project_metadata = _load_project_metadata_module()
    openclaw_dir = Path(args.dir).expanduser().resolve()
    project_dir = Path(args.project_dir).expanduser().resolve() if args.project_dir else _repo_root()
    metadata = load_project_metadata(openclaw_dir)
    metadata = normalize_project_metadata(metadata if isinstance(metadata, dict) else {})
    runtime = _runtime_config(metadata)
    launcher = _launcher_path(project_dir)

    result = {
        "ok": True,
        "started": False,
        "state": "disabled",
        "message": "Local runtime auto-start skipped.",
        "launcherPath": str(launcher),
        "pid": "",
        "logPath": "",
        "command": [],
        "commandString": "",
        "errors": [],
        "runtime": deepcopy(runtime),
    }

    if not _runtime_enabled(runtime):
        result["message"] = "Local runtime is disabled in mission-control.json."
        _print(result, args.json)
        return 0

    if not _runtime_configured(runtime):
        result["ok"] = False
        result["state"] = "incomplete"
        result["message"] = "Local runtime configuration is incomplete."
        result["errors"].append(
            {
                "field": "localRuntime",
                "code": "incomplete",
                "message": "entrypoint and modelPath are required before autostart can run.",
            }
        )
        _print(result, args.json)
        return 1

    if args.dry_run:
        launch_args = _build_launch_args(runtime, launcher)
        result.update(
            {
                "state": "dry-run",
                "message": "Local runtime autostart dry-run completed.",
                "command": launch_args,
                "commandString": shlex.join(launch_args) if hasattr(shlex, "join") else " ".join(shlex.quote(part) for part in launch_args),
            }
        )
        _print(result, args.json)
        return 0

    if _runtime_running(runtime.get("pid")):
        result["state"] = "running"
        result["started"] = False
        result["message"] = "Local runtime is already running."
        result["pid"] = str(runtime.get("pid") or "")
        result["logPath"] = str(runtime.get("logPath") or "")
        _print(result, args.json)
        return 0

    launch_args = _build_launch_args(runtime, launcher)
    launch_result = subprocess.run(
        launch_args,
        capture_output=True,
        text=True,
        check=False,
        env=_runtime_env(runtime, openclaw_dir),
    )
    try:
        prepared = json.loads((launch_result.stdout or "").strip() or "{}")
    except json.JSONDecodeError:
        prepared = {}
    if launch_result.returncode != 0 or not isinstance(prepared, dict) or not prepared.get("ok"):
        result["ok"] = False
        result["state"] = "error"
        result["message"] = "Local runtime launch preparation failed."
        result["errors"] = prepared.get("errors") if isinstance(prepared.get("errors"), list) else []
        stderr_text = (launch_result.stderr or "").strip()
        stdout_text = (launch_result.stdout or "").strip()
        if stderr_text:
            result["errors"].append({"field": "launcher", "code": "stderr", "message": stderr_text})
        elif stdout_text and not result["errors"]:
            result["errors"].append({"field": "launcher", "code": "stdout", "message": stdout_text})
        _print(result, args.json)
        return 1

    command = prepared.get("command") if isinstance(prepared.get("command"), list) else []
    command_string = str(prepared.get("commandString") or "").strip()
    if not command:
        result["ok"] = False
        result["state"] = "error"
        result["message"] = "Local runtime command is empty."
        result["errors"].append({"field": "command", "code": "empty", "message": "Launch command is empty."})
        _print(result, args.json)
        return 1

    logs_dir = openclaw_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "local-model-runtime.log"
    with log_path.open("a", encoding="utf-8") as log_file:
        child = subprocess.Popen(
            [str(item) for item in command],
            cwd=str(openclaw_dir),
            env=_runtime_env(runtime, openclaw_dir),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )

    runtime["enabled"] = True
    started_at = _utc_now_iso()
    runtime["lastStartedAt"] = started_at
    runtime["lastStartError"] = ""
    runtime["logPath"] = str(log_path)
    runtime["pid"] = str(child.pid)
    runtime["commandPreview"] = command_string
    metadata["localRuntime"] = runtime
    metadata["updatedAt"] = started_at
    try:
        # Persist the refreshed PID and launch details so dashboard status stays aligned.
        write_project_metadata(openclaw_dir, metadata)
    except Exception:
        pass

    result.update(
        {
            "started": True,
            "state": "running",
            "message": "Local runtime autostart launched.",
            "pid": child.pid,
            "logPath": str(log_path),
            "command": [str(item) for item in command],
            "commandString": command_string,
        }
    )
    if args.wait_seconds > 0:
        host = str(runtime.get("host") or "127.0.0.1")
        port = int(runtime.get("port") or 8080)
        for _ in range(max(0, args.wait_seconds)):
            try:
                with socket.create_connection((host, port), timeout=1):
                    result["message"] = "Local runtime autostart launched and is accepting connections."
                    break
            except OSError:
                pass
    _print(result, args.json)
    if args.print_command:
        print(command_string)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
