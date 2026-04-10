#!/usr/bin/env python3
"""Build a local TurboQuant / llama.cpp style runtime launch command.

This helper only assembles command arguments and validates required inputs.
It does not start the backend process by default.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import sys
from pathlib import Path


SUPPORTED_BACKENDS = {"llama_cpp"}
DEFAULT_BACKEND = "llama_cpp"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080
DEFAULT_CTX_SIZE = 8192
DEFAULT_GPU_LAYERS = -1


def _env(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip()


def _parse_int(raw: str, field: str, errors: list[dict], default: int | None = None) -> int | None:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        errors.append(
            {
                "field": field,
                "code": "invalid_integer",
                "message": f"{field} must be an integer.",
                "value": text,
            }
        )
        return default


def _parse_extra_args(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    return [item for item in shlex.split(text) if str(item).strip()]


def _parse_extra_args_json(raw: str, errors: list[dict]) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        errors.append(
            {
                "field": "extraArgsJson",
                "code": "invalid_json",
                "message": "extraArgsJson must be a JSON array of strings.",
                "detail": str(exc),
            }
        )
        return []
    if not isinstance(payload, list):
        errors.append(
            {
                "field": "extraArgsJson",
                "code": "invalid_shape",
                "message": "extraArgsJson must be a JSON array of strings.",
            }
        )
        return []
    result = []
    for item in payload:
        if item is None:
            continue
        result.append(str(item))
    return result


def _resolve_entrypoint(raw: str, errors: list[dict]) -> str:
    value = str(raw or "").strip()
    if not value:
        errors.append(
            {
                "field": "entrypoint",
                "code": "missing_required",
                "message": "entrypoint is required.",
            }
        )
        return ""
    candidate = Path(value).expanduser()
    if candidate.exists():
        return str(candidate.resolve())
    if os.sep in value or value.startswith("."):
        errors.append(
            {
                "field": "entrypoint",
                "code": "not_found",
                "message": "entrypoint path does not exist.",
                "value": value,
            }
        )
        return ""
    resolved = shutil.which(value)
    if resolved:
        return resolved
    errors.append(
        {
            "field": "entrypoint",
            "code": "not_found",
            "message": "entrypoint was not found on PATH.",
            "value": value,
        }
    )
    return ""


def _resolve_model(raw: str, errors: list[dict]) -> str:
    value = str(raw or "").strip()
    if not value:
        errors.append(
            {
                "field": "model",
                "code": "missing_required",
                "message": "model is required.",
            }
        )
        return ""
    candidate = Path(value).expanduser()
    if candidate.exists():
        return str(candidate.resolve())
    errors.append(
        {
            "field": "model",
            "code": "not_found",
            "message": "model path does not exist.",
            "value": value,
        }
    )
    return ""


def _quote_command(parts: list[str]) -> str:
    if hasattr(shlex, "join"):
        return shlex.join(parts)
    return " ".join(shlex.quote(part) for part in parts)


def _resolve_value(cli_value: str, env_name: str, default: str = "") -> str:
    value = str(cli_value or "").strip()
    if value:
        return value
    return _env(env_name, default)


def _build_command(args, errors: list[dict]):
    backend = _resolve_value(args.backend, "MISSION_CONTROL_LOCAL_RUNTIME_BACKEND", DEFAULT_BACKEND)
    if backend not in SUPPORTED_BACKENDS:
        errors.append(
            {
                "field": "backend",
                "code": "unsupported_backend",
                "message": f"Unsupported backend: {backend}.",
                "supported": sorted(SUPPORTED_BACKENDS),
            }
        )
        backend = DEFAULT_BACKEND

    entrypoint_raw = _resolve_value(args.entrypoint, "MISSION_CONTROL_LOCAL_RUNTIME_ENTRYPOINT")
    model_raw = _resolve_value(args.model, "MISSION_CONTROL_LOCAL_RUNTIME_MODEL")
    host = _resolve_value(args.host, "MISSION_CONTROL_LOCAL_RUNTIME_HOST", DEFAULT_HOST)
    port = _parse_int(_resolve_value(str(args.port), "MISSION_CONTROL_LOCAL_RUNTIME_PORT", str(DEFAULT_PORT)), "port", errors, default=DEFAULT_PORT)
    ctx_size = _parse_int(_resolve_value(str(args.ctx_size), "MISSION_CONTROL_LOCAL_RUNTIME_CTX_SIZE", str(DEFAULT_CTX_SIZE)), "ctxSize", errors, default=DEFAULT_CTX_SIZE)
    gpu_layers = _parse_int(_resolve_value(str(args.gpu_layers), "MISSION_CONTROL_LOCAL_RUNTIME_GPU_LAYERS", str(DEFAULT_GPU_LAYERS)), "gpuLayers", errors, default=DEFAULT_GPU_LAYERS)
    cache_type_k = _resolve_value(args.cache_type_k, "MISSION_CONTROL_LOCAL_RUNTIME_CACHE_TYPE_K")
    cache_type_v = _resolve_value(args.cache_type_v, "MISSION_CONTROL_LOCAL_RUNTIME_CACHE_TYPE_V")

    extra_args = []
    extra_args.extend(_parse_extra_args_json(_resolve_value(args.extra_args_json, "MISSION_CONTROL_LOCAL_RUNTIME_EXTRA_ARGS_JSON"), errors))
    extra_args.extend(_parse_extra_args(_resolve_value(args.extra_args, "MISSION_CONTROL_LOCAL_RUNTIME_EXTRA_ARGS")))
    extra_args.extend([str(item).strip() for item in (args.extra_arg or []) if str(item).strip()])
    extra_args = [item for item in extra_args if item]

    entrypoint = _resolve_entrypoint(entrypoint_raw, errors)
    model = _resolve_model(model_raw, errors)

    command = []
    if entrypoint:
        command.append(entrypoint)
    if backend == "llama_cpp":
        command.extend(["--host", host])
        if port is not None:
            command.extend(["--port", str(port)])
        if ctx_size is not None:
            command.extend(["--ctx-size", str(ctx_size)])
        if gpu_layers is not None:
            command.extend(["--n-gpu-layers", str(gpu_layers)])
        if cache_type_k:
            command.extend(["--cache-type-k", cache_type_k])
        if cache_type_v:
            command.extend(["--cache-type-v", cache_type_v])
        if model:
            command.extend(["--model", model])
        command.extend(extra_args)

    return {
        "backend": backend,
        "entrypoint": entrypoint,
        "host": host,
        "port": port,
        "ctxSize": ctx_size,
        "gpuLayers": gpu_layers,
        "cacheTypeK": cache_type_k,
        "cacheTypeV": cache_type_v,
        "model": model,
        "extraArgs": extra_args,
        "command": command,
    }


def _make_result(args, errors: list[dict]):
    built = _build_command(args, errors)
    ok = not errors
    return {
        "ok": ok,
        "backend": built["backend"],
        "entrypoint": built["entrypoint"],
        "model": built["model"],
        "host": built["host"],
        "port": built["port"],
        "ctxSize": built["ctxSize"],
        "gpuLayers": built["gpuLayers"],
        "cacheTypeK": built["cacheTypeK"],
        "cacheTypeV": built["cacheTypeV"],
        "extraArgs": built["extraArgs"],
        "command": built["command"],
        "commandString": _quote_command(built["command"]) if built["command"] else "",
        "errors": errors,
        "message": "Local runtime command ready." if ok else "Local runtime command is missing required inputs.",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", default="")
    parser.add_argument("--entrypoint", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--host", default="")
    parser.add_argument("--port", default="")
    parser.add_argument("--ctx-size", dest="ctx_size", default="")
    parser.add_argument("--gpu-layers", dest="gpu_layers", default="")
    parser.add_argument("--cache-type-k", dest="cache_type_k", default="")
    parser.add_argument("--cache-type-v", dest="cache_type_v", default="")
    parser.add_argument("--extra-args", dest="extra_args", default="")
    parser.add_argument("--extra-args-json", dest="extra_args_json", default="")
    parser.add_argument("--extra-arg", dest="extra_arg", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--print-command", action="store_true")
    args = parser.parse_args()

    errors: list[dict] = []
    result = _make_result(args, errors)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.print_command and result["commandString"]:
        print(result["commandString"])
    elif result["ok"]:
        print(result["message"])
    else:
        for error in result["errors"]:
            print(f"{error.get('field', 'runtime')}: {error.get('message', 'invalid runtime configuration')}", file=sys.stderr)

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
