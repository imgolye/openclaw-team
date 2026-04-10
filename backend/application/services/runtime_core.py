from __future__ import annotations

import base64
import json
import os
import re
import signal
import shlex
import shutil
import socket
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

VOICE_WORKFLOW_PANEL_CACHE_TTL_SECONDS = 15
VOICE_WORKFLOW_PANEL_CACHE = {}


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


def _override_or_local(name, local):
    try:
        module = _svc()
    except Exception:
        return local
    candidate = getattr(module, name, None)
    if candidate is None or candidate is local:
        return local
    return candidate


load_config = _DelegatedSymbol("load_config")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
get_router_agent_id = _DelegatedSymbol("get_router_agent_id")
read_env_value = _DelegatedSymbol("read_env_value")
load_skills_detail = _DelegatedSymbol("load_skills_detail")
merged_agent_runtime_profile = _DelegatedSymbol("merged_agent_runtime_profile")
build_deliverables_snapshot = _DelegatedSymbol("build_deliverables_snapshot")
build_computer_use_runs_snapshot = _DelegatedSymbol("build_computer_use_runs_snapshot")
store_list_model_provider_configs = _DelegatedSymbol("store_list_model_provider_configs")
store_list_customer_access_channels = _DelegatedSymbol("store_list_customer_access_channels")
store_save_model_provider_config = _DelegatedSymbol("store_save_model_provider_config")
customer_channel_voice_reply_config = _DelegatedSymbol("customer_channel_voice_reply_config")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
safe_list = _DelegatedSymbol("safe_list")
epoch_ms_to_iso = _DelegatedSymbol("epoch_ms_to_iso")
parse_json_payload = _DelegatedSymbol("parse_json_payload")
parse_iso = _DelegatedSymbol("parse_iso")
format_age = _DelegatedSymbol("format_age")
now_utc = _DelegatedSymbol("now_utc")
now_iso = _DelegatedSymbol("now_iso")
save_project_metadata = _DelegatedSymbol("save_project_metadata")
save_config = _DelegatedSymbol("save_config")
cached_payload = _DelegatedSymbol("cached_payload")
cached_payload_background = _DelegatedSymbol("cached_payload_background")
infer_model_provider = _DelegatedSymbol("infer_model_provider")
provider_key_status = _DelegatedSymbol("provider_key_status")
is_supported_openclaw_release = _DelegatedSymbol("is_supported_openclaw_release")
load_agents = _DelegatedSymbol("load_agents")
customer_voice_custom_voices = _DelegatedSymbol("customer_voice_custom_voices")
customer_voice_runtime_builtin_voices = _DelegatedSymbol("customer_voice_runtime_builtin_voices")


def _run_command(*args, **kwargs):
    return _override_or_local("run_command", run_command)(*args, **kwargs)


def _resolve_openclaw_fetch_guard_module(*args, **kwargs):
    return _override_or_local(
        "resolve_openclaw_fetch_guard_module",
        resolve_openclaw_fetch_guard_module,
    )(*args, **kwargs)


def _load_openclaw_installation_payload(*args, **kwargs):
    return _override_or_local(
        "load_openclaw_installation_payload",
        load_openclaw_installation_payload,
    )(*args, **kwargs)


def _model_provider_catalog():
    return list(getattr(_svc(), "MODEL_PROVIDER_CATALOG", []))


def _model_provider_capability_catalog():
    return deepcopy(getattr(_svc(), "MODEL_PROVIDER_CAPABILITY_CATALOG", {}))


def _provider_capability_profile(provider_id):
    normalized_provider_id = str(provider_id or "").strip().lower()
    catalog = _model_provider_capability_catalog()
    profile = catalog.get(normalized_provider_id) if isinstance(catalog, dict) else None
    if not isinstance(profile, dict):
        return {
            "summary": "",
            "bestFit": "",
            "contextWindow": "",
            "multimodalInputs": [],
            "capabilityTags": [],
            "supports": {},
        }
    return {
        "summary": str(profile.get("summary") or "").strip(),
        "bestFit": str(profile.get("bestFit") or "").strip(),
        "contextWindow": str(profile.get("contextWindow") or "").strip(),
        "multimodalInputs": clean_unique_strings(profile.get("multimodalInputs") or []),
        "starterModels": clean_unique_strings(profile.get("starterModels") or []),
        "localRuntimeProfiles": deepcopy(profile.get("localRuntimeProfiles") if isinstance(profile.get("localRuntimeProfiles"), list) else []),
        "capabilityTags": clean_unique_strings(profile.get("capabilityTags") or []),
        "supports": deepcopy(profile.get("supports") if isinstance(profile.get("supports"), dict) else {}),
    }


def _model_provider_starter_models():
    starters = []
    for provider in _model_provider_catalog():
        profile = _provider_capability_profile(provider.get("id"))
        starters.extend(clean_unique_strings(profile.get("starterModels") or []))
    return _ordered_unique_strings(starters)


def _provider_default_starter_model(provider_id):
    profile = _provider_capability_profile(provider_id)
    starters = clean_unique_strings(profile.get("starterModels") or [])
    return starters[0] if starters else ""


def _build_local_runtime_recommended_profiles():
    profiles = []
    seen = set()
    for provider in _model_provider_catalog():
        capability_profile = _provider_capability_profile(provider.get("id"))
        for raw_profile in capability_profile.get("localRuntimeProfiles") if isinstance(capability_profile.get("localRuntimeProfiles"), list) else []:
            if not isinstance(raw_profile, dict):
                continue
            profile_id = str(raw_profile.get("id") or "").strip()
            if not profile_id or profile_id in seen:
                continue
            seen.add(profile_id)
            provider_id = str(provider.get("id") or "").strip()
            entrypoint = _normalize_runtime_text(raw_profile.get("entrypoint"))
            model_path = _normalize_runtime_text(raw_profile.get("modelPath"))
            base_url = ""
            notes = str(raw_profile.get("notes") or "").strip()
            if provider_id == "gemma":
                entrypoint = ""
                model_path = recommended_gemma_local_runtime_model_path(profile_id, model_path)
                base_url = recommended_gemma_local_runtime_base_url()
                projector_path = ""
                models_dir = ""
                notes = "Run Gemma on the host machine and connect to it over an OpenAI-compatible base URL."
                download_sources = [
                    "https://blog.google/innovation-and-ai/technology/developers-tools/gemma-4/",
                    "https://ai.google.dev/gemma",
                ]
                extra_args = []
            else:
                projector_path = ""
                models_dir = ""
                download_sources = []
                extra_args = _normalize_runtime_args(raw_profile.get("extraArgs"))
            mode = DEFAULT_LOCAL_RUNTIME_MODE if provider_id == "gemma" else "managed_process"
            entrypoint_exists = bool(_resolve_local_runtime_entrypoint_path(entrypoint))
            required_files = [] if mode == DEFAULT_LOCAL_RUNTIME_MODE else _runtime_required_file_specs(model_path, extra_args, projector_path)
            missing_required_files = [item for item in required_files if not Path(str(item.get("path") or "")).expanduser().exists()]
            model_exists = True if mode == DEFAULT_LOCAL_RUNTIME_MODE else not missing_required_files
            profiles.append(
                {
                    "id": profile_id,
                    "mode": mode,
                    "providerId": provider_id,
                    "providerLabel": str(provider.get("label") or provider.get("id") or "").strip(),
                    "label": str(raw_profile.get("label") or profile_id).strip(),
                    "description": str(raw_profile.get("description") or "").strip(),
                    "backend": _normalize_runtime_text(raw_profile.get("backend"), DEFAULT_LOCAL_RUNTIME_BACKEND),
                    "baseUrl": base_url,
                    "entrypoint": entrypoint,
                    "modelPath": model_path,
                    "host": _normalize_runtime_text(raw_profile.get("host"), "host.docker.internal" if Path("/.dockerenv").exists() else DEFAULT_LOCAL_RUNTIME_HOST),
                    "port": _normalize_runtime_int(raw_profile.get("port"), DEFAULT_LOCAL_RUNTIME_PORT, minimum=1, maximum=65535),
                    "contextLength": _normalize_runtime_int(raw_profile.get("contextLength"), DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH, minimum=1),
                    "gpuLayers": _normalize_runtime_int(raw_profile.get("gpuLayers"), DEFAULT_LOCAL_RUNTIME_GPU_LAYERS, minimum=0),
                    "kvCacheEnabled": _normalize_runtime_bool(raw_profile.get("kvCacheEnabled"), False),
                    "cacheTypeK": _normalize_runtime_text(raw_profile.get("cacheTypeK"), DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE),
                    "cacheTypeV": _normalize_runtime_text(raw_profile.get("cacheTypeV"), DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE),
                    "extraArgs": extra_args,
                    "projectorPath": projector_path,
                    "notes": notes,
                    "modelsDir": models_dir,
                    "downloadSources": download_sources,
                    "entrypointExists": entrypoint_exists if mode != DEFAULT_LOCAL_RUNTIME_MODE else False,
                    "modelExists": model_exists,
                    "requiredFiles": required_files,
                    "missingFiles": [str(item.get("path") or "").strip() for item in missing_required_files],
                    "readyToStart": bool(base_url) if mode == DEFAULT_LOCAL_RUNTIME_MODE else bool(entrypoint_exists and model_exists),
                }
            )
    return profiles


def _local_runtime_recommended_profile_by_id(profile_id):
    normalized_profile_id = str(profile_id or "").strip()
    if not normalized_profile_id:
        return {}
    return next(
        (
            item
            for item in _build_local_runtime_recommended_profiles()
            if str(item.get("id") or "").strip() == normalized_profile_id
        ),
        {},
    )


def recommended_gemma_local_runtime_models_dir():
    configured = str(os.environ.get("MISSION_CONTROL_GEMMA4_MODELS_DIR") or "").strip()
    return configured or DEFAULT_GEMMA_LOCAL_RUNTIME_MODELS_DIR


def recommended_gemma_local_runtime_base_url():
    configured = str(os.environ.get("MISSION_CONTROL_LOCAL_RUNTIME_BASE_URL") or "").strip()
    if configured:
        return configured.rstrip("/")
    if Path("/.dockerenv").exists():
        return DEFAULT_LOCAL_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_LOCAL_RUNTIME_BASE_URL


def recommended_gemma_local_runtime_entrypoint(default_value=""):
    return str(default_value or "").strip()


def recommended_gemma_local_runtime_model_path(profile_id="", default_value=""):
    normalized_profile_id = str(profile_id or "").strip().lower()
    configured_base_url = recommended_gemma_local_runtime_base_url().lower()
    env_candidates = []
    if normalized_profile_id == "gemma-4-e2b-edge":
        env_candidates.append("MISSION_CONTROL_GEMMA4_E2B_MODEL_PATH")
    elif normalized_profile_id == "gemma-4-e4b-edge":
        env_candidates.append("MISSION_CONTROL_GEMMA4_E4B_MODEL_PATH")
    env_candidates.append("MISSION_CONTROL_GEMMA4_MODEL_PATH")
    for env_key in env_candidates:
        value = str(os.environ.get(env_key) or "").strip()
        if value:
            return Path(value).name if "/" in value else value
    fallback = str(default_value or "").strip()
    if (":11434" in configured_base_url or configured_base_url.endswith("/api")) and (
        not fallback or fallback.lower().endswith(".gguf")
    ):
        return "gemma4:latest"
    if fallback:
        return fallback
    if normalized_profile_id == "gemma-4-e2b-edge":
        return "gemma-4-e2b-it-q4_k_m.gguf"
    if normalized_profile_id == "gemma-4-e4b-edge":
        return "gemma-4-e4b-it-q4_k_m.gguf"
    return "gemma-4-it-q4_k_m.gguf"


def recommended_gemma_local_runtime_projector_path(profile_id="", default_value=""):
    normalized_profile_id = str(profile_id or "").strip().lower()
    env_candidates = []
    if normalized_profile_id == "gemma-4-e2b-edge":
        env_candidates.append("MISSION_CONTROL_GEMMA4_E2B_PROJECTOR_PATH")
    elif normalized_profile_id == "gemma-4-e4b-edge":
        env_candidates.append("MISSION_CONTROL_GEMMA4_E4B_PROJECTOR_PATH")
    env_candidates.append("MISSION_CONTROL_GEMMA4_PROJECTOR_PATH")
    for env_key in env_candidates:
        value = str(os.environ.get(env_key) or "").strip()
        if value:
            return value
    fallback = str(default_value or "").strip()
    if fallback:
        return fallback
    models_dir = recommended_gemma_local_runtime_models_dir()
    if normalized_profile_id == "gemma-4-e2b-edge":
        return f"{models_dir}/mmproj-e2b-f16.gguf"
    if normalized_profile_id == "gemma-4-e4b-edge":
        return f"{models_dir}/mmproj-e4b-f16.gguf"
    return f"{models_dir}/mmproj-f16.gguf"


def _runtime_extra_args_with_gemma_projector(extra_args, projector_path=""):
    normalized_args = _normalize_runtime_args(extra_args)
    normalized_projector = str(projector_path or "").strip()
    if not normalized_projector:
        return normalized_args
    updated = []
    index = 0
    replaced = False
    while index < len(normalized_args):
        item = str(normalized_args[index] or "").strip()
        if item == "--mmproj":
            updated.extend(["--mmproj", normalized_projector])
            index += 2
            replaced = True
            continue
        updated.append(item)
        index += 1
    if not replaced:
        updated.extend(["--mmproj", normalized_projector])
    return [item for item in updated if item]


def _runtime_required_file_specs(model_path="", extra_args=None, projector_path=""):
    specs = []
    normalized_model = str(model_path or "").strip()
    if normalized_model:
        specs.append({"kind": "model", "path": normalized_model})
    normalized_projector = str(projector_path or "").strip()
    args = _normalize_runtime_args(extra_args)
    for index, item in enumerate(args):
        if str(item).strip() != "--mmproj":
            continue
        if index + 1 < len(args):
            normalized_projector = str(args[index + 1] or "").strip() or normalized_projector
    if normalized_projector:
        specs.append({"kind": "projector", "path": normalized_projector})
    return specs


def _openclaw_baseline_release():
    return str(getattr(_svc(), "OPENCLAW_BASELINE_RELEASE", "") or "").strip()


def _repo_root():
    return Path(__file__).resolve().parents[3]


def resolve_project_dir(openclaw_dir, config=None):
    env_project_dir = os.environ.get("MISSION_CONTROL_PROJECT_DIR", "").strip()
    if env_project_dir:
        candidate = Path(env_project_dir).expanduser().resolve()
        if (candidate / "platform" / "bin" / "install" / "switch_theme.py").exists():
            return candidate

    config = config or load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    project_dir = str(metadata.get("projectDir", "")).strip()
    if project_dir:
        candidate = Path(project_dir).expanduser().resolve()
        if (candidate / "platform" / "bin" / "install" / "switch_theme.py").exists():
            return candidate

    for parent in Path(__file__).resolve().parents:
        if (parent / "platform" / "bin" / "install" / "switch_theme.py").exists() and (parent / "platform" / "config" / "themes").exists():
            return parent
    return None


def resolve_planning_project_dir(openclaw_dir, config=None):
    config = config or load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=config)
    project_dir = str(metadata.get("projectDir", "")).strip()
    if project_dir:
        candidate = Path(project_dir).expanduser().resolve()
        if candidate.exists():
            return candidate
    return resolve_project_dir(openclaw_dir, config=config)


def load_runtime_profile_catalog(project_dir=None):
    candidate_project_dir = Path(project_dir).expanduser().resolve() if project_dir else _repo_root()
    config_path = candidate_project_dir / "platform" / "config" / "runtime-profiles.json" if candidate_project_dir else None
    if not config_path or not config_path.exists():
        return {}
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    profiles = payload.get("profiles")
    return profiles if isinstance(profiles, dict) else {}


def resolve_runtime_profile_key(default="host"):
    normalized = str(os.environ.get("MISSION_CONTROL_RUNTIME_PROFILE") or default or "host").strip().lower()
    return normalized or "host"


def _normalize_runtime_profile_summary(profile_key, payload):
    profile = payload if isinstance(payload, dict) else {}
    product = profile.get("product") if isinstance(profile.get("product"), dict) else {}
    speech_runtime = profile.get("speechRuntime") if isinstance(profile.get("speechRuntime"), dict) else {}
    local_runtime = profile.get("localRuntime") if isinstance(profile.get("localRuntime"), dict) else {}
    deployment = profile.get("deployment") if isinstance(profile.get("deployment"), dict) else {}
    openclaw = profile.get("openclaw") if isinstance(profile.get("openclaw"), dict) else {}
    return {
        "key": str(profile_key or "").strip(),
        "label": str(profile.get("label") or profile_key or "").strip(),
        "product": {
            "host": str(product.get("host") or "").strip(),
            "bind": str(product.get("bind") or "").strip(),
            "port": int(product.get("port") or 0) if str(product.get("port") or "").strip() else None,
            "baseUrl": str(product.get("baseUrl") or "").strip(),
        },
        "speechRuntime": {
            "provider": str(speech_runtime.get("provider") or "").strip(),
            "baseUrl": str(speech_runtime.get("baseUrl") or "").strip(),
            "healthUrl": str(speech_runtime.get("healthUrl") or "").strip(),
            "port": int(speech_runtime.get("port") or 0) if str(speech_runtime.get("port") or "").strip() else None,
        },
        "localRuntime": {
            "baseUrl": str(local_runtime.get("baseUrl") or "").strip(),
        },
        "deployment": {
            "mode": str(deployment.get("mode") or "").strip(),
            "profile": str(deployment.get("profile") or "").strip(),
        },
        "openclaw": {
            "stateDir": str(openclaw.get("stateDir") or "").strip(),
            "pairingSourceDir": str(openclaw.get("pairingSourceDir") or "").strip(),
        },
    }


def _normalize_runtime_base_url(value):
    return str(value or "").strip().rstrip("/")


def runtime_script_path(openclaw_dir, script_name):
    config = load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    candidates = [
        Path(openclaw_dir) / f"workspace-{router_agent_id}" / "scripts" / script_name,
    ]
    project_dir = resolve_project_dir(openclaw_dir, config=config)
    if project_dir:
        backend_dir = Path(project_dir) / "backend"
        candidates.extend(
            [
                backend_dir / "tools" / script_name,
                backend_dir / script_name,
            ]
        )
    repo_backend_dir = _repo_root() / "backend"
    candidates.extend(
        [
            repo_backend_dir / "tools" / script_name,
            repo_backend_dir / script_name,
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Missing runtime script: {script_name}")


def openclaw_installer_script_path(openclaw_dir):
    project_dir = resolve_project_dir(openclaw_dir)
    if project_dir:
        candidate = Path(project_dir) / "platform" / "bin" / "install" / "openclaw_installer.py"
        if candidate.exists():
            return candidate
    fallback = _repo_root() / "platform" / "bin" / "install" / "openclaw_installer.py"
    if fallback.exists():
        return fallback
    raise FileNotFoundError("Missing OpenClaw installer script.")


def runtime_sync_script_path(openclaw_dir):
    project_dir = resolve_project_dir(openclaw_dir)
    if project_dir:
        candidate = Path(project_dir) / "platform" / "bin" / "runtime" / "sync_runtime_assets.sh"
        if candidate.exists():
            return candidate
    fallback = _repo_root() / "platform" / "bin" / "runtime" / "sync_runtime_assets.sh"
    if fallback.exists():
        return fallback
    raise FileNotFoundError("Missing runtime sync script.")


def openclaw_auth_sync_script_path(openclaw_dir):
    project_dir = resolve_project_dir(openclaw_dir)
    candidates = []
    if project_dir:
        candidates.append(Path(project_dir) / "platform" / "bin" / "runtime" / "sync_agent_auth.py")
    candidates.extend(
        [
            Path(openclaw_dir) / "bin" / "runtime" / "sync_agent_auth.py",
            Path("/app/platform/bin/runtime/sync_agent_auth.py"),
            _repo_root() / "platform" / "bin" / "runtime" / "sync_agent_auth.py",
        ]
    )
    current_file = Path(__file__).resolve()
    for parent in current_file.parents:
        candidates.append(parent / "platform" / "bin" / "runtime" / "sync_agent_auth.py")
    seen = set()
    for candidate in candidates:
        normalized = candidate.resolve() if candidate.exists() else candidate
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Missing agent auth sync script.")


def default_managed_openclaw_dirs(openclaw_dir):
    base_dir = Path(openclaw_dir).expanduser().resolve()
    return [
        base_dir / "bin",
        base_dir / ".runtime" / "openclaw-managed" / "bin",
        base_dir / ".runtime" / "openclaw-managed" / "node_modules" / ".bin",
    ]


def openclaw_command_bin_dirs(openclaw_dir, metadata=None):
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir)
    install_meta = metadata.get("openclawInstall") if isinstance(metadata.get("openclawInstall"), dict) else {}
    candidates = []
    cli_path = str(install_meta.get("cliPath") or "").strip()
    if cli_path:
        candidates.append(Path(cli_path).expanduser().resolve().parent)
    for candidate in default_managed_openclaw_dirs(openclaw_dir):
        candidates.append(candidate)
    seen = set()
    resolved = []
    for candidate in candidates:
        try:
            normalized = Path(candidate).expanduser().resolve()
        except (OSError, ValueError):
            continue
        key = str(normalized)
        if key in seen or not normalized.exists():
            continue
        seen.add(key)
        resolved.append(key)
    return resolved


def provider_config_env_map(openclaw_dir):
    env_values = {}
    try:
        configs = store_list_model_provider_configs(openclaw_dir)
    except Exception:
        return env_values
    for config in configs or []:
        if not isinstance(config, dict):
            continue
        status = str(config.get("status") or "active").strip().lower()
        if status == "disabled":
            continue
        key_value = str(config.get("keyValue") or "").strip()
        if not key_value:
            continue
        env_keys = config.get("envKeys") if isinstance(config.get("envKeys"), list) else []
        for key in env_keys:
            normalized_key = str(key or "").strip()
            if normalized_key and normalized_key not in env_values:
                env_values[normalized_key] = key_value
    return env_values


def _read_openclaw_env_file_values(openclaw_dir):
    env_path = Path(openclaw_dir) / ".env"
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    values = {}
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = str(key or "").strip()
        if normalized_key and normalized_key not in values:
            values[normalized_key] = value.strip()
    return values


def _read_host_openclaw_auth_fallback_values(openclaw_dir):
    try:
        current_dir = Path(openclaw_dir).expanduser().resolve()
        home_state_dir = (Path.home() / ".openclaw").resolve()
    except OSError:
        return {}
    if current_dir == home_state_dir:
        return {}
    fallback = {}
    config_path = home_state_dir / "openclaw.json"
    try:
        config_payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        config_payload = {}
    gateway = config_payload.get("gateway") if isinstance(config_payload.get("gateway"), dict) else {}
    auth = gateway.get("auth") if isinstance(gateway.get("auth"), dict) else {}
    gateway_token = str(auth.get("token") or "").strip()
    if gateway_token:
        fallback["GATEWAY_AUTH_TOKEN"] = gateway_token
    values = _read_openclaw_env_file_values(home_state_dir)
    for key in ("GATEWAY_AUTH_TOKEN", "DASHBOARD_AUTH_TOKEN"):
        if key in fallback:
            continue
        value = str(values.get(key) or "").strip()
        if value:
            fallback[key] = value
    return fallback


LOCAL_RUNTIME_CONFIG_KEY = "localRuntime"
SPEECH_RUNTIME_CONFIG_KEY = "speechRuntime"
EXECUTION_CONFIG_KEY = "execution"
DEFAULT_LOCAL_RUNTIME_MODE = "external_openai"
DEFAULT_LOCAL_RUNTIME_BACKEND = "llama_cpp"
DEFAULT_LOCAL_RUNTIME_HOST = "127.0.0.1"
DEFAULT_LOCAL_RUNTIME_PORT = 11434
DEFAULT_LOCAL_RUNTIME_BASE_URL = "http://127.0.0.1:11434/v1"
DEFAULT_LOCAL_RUNTIME_DOCKER_BASE_URL = "http://host.docker.internal:11434/v1"
DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH = 8192
DEFAULT_LOCAL_RUNTIME_GPU_LAYERS = 0
DEFAULT_LOCAL_RUNTIME_KV_CACHE_MODE = "turbo3"
DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE = "turbo3"
DEFAULT_GEMMA_LOCAL_RUNTIME_ENTRYPOINT = ""
DEFAULT_GEMMA_LOCAL_RUNTIME_MODELS_DIR = "/models/gemma4"
DEFAULT_EXECUTION_TRANSPORT = "openclaw"
DEFAULT_EXECUTION_PRIMARY_PATH = "local_runtime"
DEFAULT_EXECUTION_FALLBACK_PATH = "provider_api"
DEFAULT_EXECUTION_CONTEXT_MODE = "layered"
DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE = "primary_execution"
DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY = "balanced"
LEGACY_SPEECH_RUNTIME_PROVIDER_COSYVOICE = "cosyvoice_sft"
DEFAULT_SPEECH_RUNTIME_PROVIDER = "sherpa_onnx"
DEFAULT_SPEECH_RUNTIME_BASE_URL = ""
DEFAULT_SPEECH_RUNTIME_MODEL = "gpt-4o-mini-tts-2025-12-15"
DEFAULT_SPEECH_RUNTIME_API_KEY_ENV = "OPENAI_API_KEY"
DEFAULT_SHERPA_ONNX_RUNTIME_BASE_URL = "http://127.0.0.1:8090/v1"
DEFAULT_SHERPA_ONNX_RUNTIME_DOCKER_BASE_URL = "http://sherpa-onnx-tts:8080/v1"
DEFAULT_SHERPA_ONNX_RUNTIME_MODEL = "kokoro-multi-lang-v1_1"
DEFAULT_SHERPA_ONNX_RUNTIME_API_KEY_ENV = "SHERPA_ONNX_TTS_API_KEY"
DEFAULT_QWEN3_TTS_RUNTIME_BASE_URL = "http://127.0.0.1:8090/v1"
DEFAULT_QWEN3_TTS_RUNTIME_DOCKER_BASE_URL = "http://qwen3-tts:8080/v1"
DEFAULT_QWEN3_TTS_RUNTIME_MODEL = "qwen3-tts-12hz-0.6b-customvoice"
DEFAULT_QWEN3_TTS_RUNTIME_API_KEY_ENV = "QWEN3_TTS_API_KEY"
DEFAULT_ZHIPU_TTS_RUNTIME_BASE_URL = "https://open.bigmodel.cn/api/paas/v4"
DEFAULT_ZHIPU_TTS_RUNTIME_MODEL = "glm-tts"
DEFAULT_ZHIPU_TTS_RUNTIME_API_KEY_ENV = "ZAI_API_KEY"
SUPPORTED_SPEECH_RUNTIME_PROVIDERS = {"openai", "sherpa_onnx", "qwen3_tts", "zhipu"}
HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES = {"balanced", "aggressive", "full"}
HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_PROFILES = {
    "balanced": {
        "label": "Balanced",
        "summary": "Hold the default budget line for hosted providers.",
        "messageThresholdMultiplier": 1.0,
        "characterThresholdMultiplier": 1.0,
        "keepRecentMessagesDelta": 0,
    },
    "aggressive": {
        "label": "Aggressive",
        "summary": "Compress hosted-provider threads earlier and keep a tighter live window.",
        "messageThresholdMultiplier": 0.7,
        "characterThresholdMultiplier": 0.7,
        "keepRecentMessagesDelta": -2,
    },
    "full": {
        "label": "Full",
        "summary": "Preserve a wider live window before compression kicks in.",
        "messageThresholdMultiplier": 1.35,
        "characterThresholdMultiplier": 1.35,
        "keepRecentMessagesDelta": 2,
    },
}


def _speech_runtime_env_text(*keys):
    for key in keys:
        value = _normalize_runtime_text(os.environ.get(key))
        if value:
            return value
    return ""


def _speech_runtime_env_provider():
    provider = _speech_runtime_env_text("MISSION_CONTROL_SPEECH_RUNTIME_PROVIDER").lower()
    if provider == LEGACY_SPEECH_RUNTIME_PROVIDER_COSYVOICE:
        provider = DEFAULT_SPEECH_RUNTIME_PROVIDER
    return provider if provider in SUPPORTED_SPEECH_RUNTIME_PROVIDERS else ""


def _speech_runtime_base_url_override(provider=""):
    provider_key = {
        "openai": "MISSION_CONTROL_OPENAI_TTS_RUNTIME_BASE_URL",
        "sherpa_onnx": "MISSION_CONTROL_SHERPA_ONNX_RUNTIME_BASE_URL",
        "qwen3_tts": "MISSION_CONTROL_QWEN3_TTS_RUNTIME_BASE_URL",
        "zhipu": "MISSION_CONTROL_ZHIPU_TTS_RUNTIME_BASE_URL",
    }.get(str(provider or "").strip().lower(), "")
    return _speech_runtime_env_text(provider_key, "MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL")


def _speech_runtime_model_override(provider=""):
    provider_key = {
        "openai": "MISSION_CONTROL_OPENAI_TTS_RUNTIME_MODEL",
        "sherpa_onnx": "MISSION_CONTROL_SHERPA_ONNX_RUNTIME_MODEL",
        "qwen3_tts": "MISSION_CONTROL_QWEN3_TTS_RUNTIME_MODEL",
        "zhipu": "MISSION_CONTROL_ZHIPU_TTS_RUNTIME_MODEL",
    }.get(str(provider or "").strip().lower(), "")
    return _speech_runtime_env_text(provider_key, "MISSION_CONTROL_SPEECH_RUNTIME_MODEL")


def _speech_runtime_api_key_env_override(provider=""):
    provider_key = {
        "openai": "MISSION_CONTROL_OPENAI_TTS_RUNTIME_API_KEY_ENV",
        "sherpa_onnx": "MISSION_CONTROL_SHERPA_ONNX_RUNTIME_API_KEY_ENV",
        "qwen3_tts": "MISSION_CONTROL_QWEN3_TTS_RUNTIME_API_KEY_ENV",
        "zhipu": "MISSION_CONTROL_ZHIPU_TTS_RUNTIME_API_KEY_ENV",
    }.get(str(provider or "").strip().lower(), "")
    return _speech_runtime_env_text(provider_key, "MISSION_CONTROL_SPEECH_RUNTIME_API_KEY_ENV")


def _normalize_runtime_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off", ""}:
        return False
    return default


def _normalize_runtime_int(value, default, minimum=None, maximum=None):
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and normalized < minimum:
        return default
    if maximum is not None and normalized > maximum:
        return default
    return normalized


def _normalize_runtime_text(value, default=""):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _normalize_runtime_args(value):
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    return [item for item in shlex.split(text) if str(item).strip()]


def normalize_local_runtime_config(payload):
    runtime = deepcopy(payload) if isinstance(payload, dict) else {}
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    mode = _normalize_runtime_text(kv_cache.get("mode"), DEFAULT_LOCAL_RUNTIME_KV_CACHE_MODE)
    runtime_mode = _normalize_runtime_text(runtime.get("mode"), DEFAULT_LOCAL_RUNTIME_MODE)
    normalized = {
        "mode": runtime_mode,
        "enabled": _normalize_runtime_bool(runtime.get("enabled"), False),
        "backend": _normalize_runtime_text(runtime.get("backend"), DEFAULT_LOCAL_RUNTIME_BACKEND),
        "baseUrl": _normalize_runtime_text(
            runtime.get("baseUrl"),
            recommended_gemma_local_runtime_base_url() if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE else "",
        ),
        "entrypoint": _normalize_runtime_text(runtime.get("entrypoint")),
        "modelPath": _normalize_runtime_text(runtime.get("modelPath")),
        "host": _normalize_runtime_text(runtime.get("host"), DEFAULT_LOCAL_RUNTIME_HOST),
        "port": _normalize_runtime_int(runtime.get("port"), DEFAULT_LOCAL_RUNTIME_PORT, minimum=1, maximum=65535),
        "contextLength": _normalize_runtime_int(runtime.get("contextLength"), DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH, minimum=1),
        "gpuLayers": _normalize_runtime_int(runtime.get("gpuLayers"), DEFAULT_LOCAL_RUNTIME_GPU_LAYERS, minimum=0),
        "extraArgs": _normalize_runtime_args(runtime.get("extraArgs")),
        "kvCache": {
            "enabled": _normalize_runtime_bool(kv_cache.get("enabled"), False),
            "mode": mode,
            "keyType": _normalize_runtime_text(kv_cache.get("keyType"), mode or DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE),
            "valueType": _normalize_runtime_text(kv_cache.get("valueType"), mode or DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE),
        },
    }
    for key in ("lastConfiguredAt", "lastStartedAt", "lastStoppedAt", "lastStartError", "logPath", "pid", "commandPreview", "updatedAt"):
        text = _normalize_runtime_text(runtime.get(key))
        if text:
            normalized[key] = text
    return normalized


def normalize_execution_config(payload):
    execution = deepcopy(payload) if isinstance(payload, dict) else {}
    primary_path = _normalize_runtime_text(execution.get("primaryPath"), DEFAULT_EXECUTION_PRIMARY_PATH)
    if primary_path not in {"provider_api", "local_runtime", "auto"}:
        primary_path = DEFAULT_EXECUTION_PRIMARY_PATH
    fallback_path = _normalize_runtime_text(execution.get("fallbackPath"), DEFAULT_EXECUTION_FALLBACK_PATH)
    if fallback_path not in {"provider_api", "local_runtime", "none"}:
        fallback_path = DEFAULT_EXECUTION_FALLBACK_PATH
    local_runtime_role = _normalize_runtime_text(execution.get("localRuntimeRole"), DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE)
    if local_runtime_role not in {"optional_acceleration", "primary_execution"}:
        local_runtime_role = DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE
    hosted_provider_context_budget_policy = _normalize_runtime_text(
        execution.get("hostedProviderContextBudgetPolicy") or execution.get("contextBudgetPolicy"),
        DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY,
    ).lower()
    if hosted_provider_context_budget_policy not in HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES:
        hosted_provider_context_budget_policy = DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY
    return {
        "transport": _normalize_runtime_text(execution.get("transport"), DEFAULT_EXECUTION_TRANSPORT),
        "primaryPath": primary_path,
        "fallbackPath": fallback_path,
        "contextMode": _normalize_runtime_text(execution.get("contextMode"), DEFAULT_EXECUTION_CONTEXT_MODE),
        "localRuntimeRole": local_runtime_role,
        "preferredProviderId": _normalize_runtime_text(execution.get("preferredProviderId")),
        "hostedProviderContextBudgetPolicy": hosted_provider_context_budget_policy,
    }


def normalize_speech_runtime_config(payload):
    runtime = deepcopy(payload) if isinstance(payload, dict) else {}
    default_provider = DEFAULT_SPEECH_RUNTIME_PROVIDER
    provider = _normalize_runtime_text(runtime.get("provider"), default_provider).lower()
    provider = _speech_runtime_env_provider() or provider
    if provider not in SUPPORTED_SPEECH_RUNTIME_PROVIDERS:
        provider = default_provider
    if provider == LEGACY_SPEECH_RUNTIME_PROVIDER_COSYVOICE:
        provider = DEFAULT_SPEECH_RUNTIME_PROVIDER
    if provider == "sherpa_onnx":
        default_base_url = recommended_sherpa_onnx_runtime_base_url()
        default_model = DEFAULT_SHERPA_ONNX_RUNTIME_MODEL
        default_api_key_env = DEFAULT_SHERPA_ONNX_RUNTIME_API_KEY_ENV
    elif provider == "qwen3_tts":
        default_base_url = recommended_qwen3_tts_runtime_base_url()
        default_model = DEFAULT_QWEN3_TTS_RUNTIME_MODEL
        default_api_key_env = DEFAULT_QWEN3_TTS_RUNTIME_API_KEY_ENV
    elif provider == "zhipu":
        default_base_url = DEFAULT_ZHIPU_TTS_RUNTIME_BASE_URL
        default_model = DEFAULT_ZHIPU_TTS_RUNTIME_MODEL
        default_api_key_env = DEFAULT_ZHIPU_TTS_RUNTIME_API_KEY_ENV
    else:
        default_base_url = DEFAULT_SPEECH_RUNTIME_BASE_URL
        default_model = DEFAULT_SPEECH_RUNTIME_MODEL
        default_api_key_env = DEFAULT_SPEECH_RUNTIME_API_KEY_ENV
    resolved_base_url = _speech_runtime_base_url_override(provider) or _normalize_runtime_text(runtime.get("baseUrl"), default_base_url)
    resolved_model = _speech_runtime_model_override(provider) or _normalize_runtime_text(runtime.get("model"), default_model)
    resolved_api_key_env = _speech_runtime_api_key_env_override(provider) or _normalize_runtime_text(runtime.get("apiKeyEnv"), default_api_key_env)
    return {
        "provider": provider,
        "baseUrl": resolved_base_url,
        "model": resolved_model,
        "apiKeyEnv": resolved_api_key_env,
    }


def recommended_qwen3_tts_runtime_base_url():
    if Path("/.dockerenv").exists():
        return DEFAULT_QWEN3_TTS_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_QWEN3_TTS_RUNTIME_BASE_URL


def recommended_sherpa_onnx_runtime_base_url():
    if Path("/.dockerenv").exists():
        return DEFAULT_SHERPA_ONNX_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_SHERPA_ONNX_RUNTIME_BASE_URL


def load_speech_runtime_config(openclaw_dir, config=None, metadata=None):
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    runtime = local_metadata.get(SPEECH_RUNTIME_CONFIG_KEY) if isinstance(local_metadata.get(SPEECH_RUNTIME_CONFIG_KEY), dict) else {}
    return normalize_speech_runtime_config(runtime)


def load_execution_config(openclaw_dir, config=None, metadata=None):
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    execution = local_metadata.get(EXECUTION_CONFIG_KEY) if isinstance(local_metadata.get(EXECUTION_CONFIG_KEY), dict) else {}
    return normalize_execution_config(execution)


def _resolve_local_runtime_entrypoint_path(entrypoint):
    value = str(entrypoint or "").strip()
    if not value:
        return ""
    candidate = Path(value).expanduser()
    if candidate.exists():
        return str(candidate.resolve())
    if os.sep in value or value.startswith("."):
        return ""
    resolved = shutil.which(value)
    return str(resolved or "").strip()


def _extract_supported_cache_types(help_output):
    text = str(help_output or "")
    if not text:
        return []
    seen = set()
    supported = []
    for match in re.finditer(r"\b(?:turbo\d+|q\d+_\d+|iq\d+_[a-z0-9]+|f16|f32|bf16)\b", text, flags=re.IGNORECASE):
        candidate = str(match.group(0) or "").strip().lower()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        supported.append(candidate)
    return supported


def _probe_local_runtime_backend_flags(entrypoint_path, backend):
    resolved = str(entrypoint_path or "").strip()
    backend_name = str(backend or "").strip().lower()

    def build():
        result = {
            "probeOk": False,
            "probeError": "",
            "helpFlags": [],
            "cacheFlagsSupported": False,
            "ctxSizeFlagSupported": False,
            "gpuLayersFlagSupported": False,
            "supportedCacheTypes": [],
            "turboQuantVariantsSupported": [],
        }
        if not resolved or backend_name != "llama_cpp":
            return result
        commands = ([resolved, "--help"], [resolved, "-h"])
        combined = ""
        for command in commands:
            try:
                process = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=2,
                    check=False,
                )
            except FileNotFoundError:
                result["probeError"] = "entrypoint_not_found"
                return result
            except subprocess.TimeoutExpired:
                result["probeError"] = "help_timeout"
                return result
            output = "\n".join(part for part in ((process.stdout or "").strip(), (process.stderr or "").strip()) if part).strip()
            if output:
                combined = output
                break
        if not combined:
            result["probeError"] = "help_output_empty"
            return result
        result["probeOk"] = True
        result["helpFlags"] = [flag for flag in ("--cache-type-k", "--cache-type-v", "--ctx-size", "--n-gpu-layers") if flag in combined]
        result["cacheFlagsSupported"] = "--cache-type-k" in combined and "--cache-type-v" in combined
        result["ctxSizeFlagSupported"] = "--ctx-size" in combined
        result["gpuLayersFlagSupported"] = "--n-gpu-layers" in combined
        result["supportedCacheTypes"] = _extract_supported_cache_types(combined)
        result["turboQuantVariantsSupported"] = [item for item in result["supportedCacheTypes"] if item.startswith("turbo")]
        return result

    return cached_payload(("local-runtime-help-probe", resolved, backend_name), 60, build)


def build_local_runtime_capability_payload(runtime_payload):
    runtime = runtime_payload if isinstance(runtime_payload, dict) else {}
    runtime_mode = str(runtime.get("mode") or "").strip().lower()
    external_base_url = str(runtime.get("baseUrl") or "").strip().rstrip("/")
    resolved_entrypoint = _resolve_local_runtime_entrypoint_path(runtime.get("entrypoint"))
    model_path = str(runtime.get("modelPath") or "").strip()
    model_exists = True if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE else bool(model_path and Path(model_path).expanduser().exists())
    backend_name = str(runtime.get("backend") or "").strip().lower()
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    flag_probe = _probe_local_runtime_backend_flags(resolved_entrypoint, backend_name)
    supported_cache_types = safe_list(flag_probe.get("supportedCacheTypes"))
    turbo_variants_supported = safe_list(flag_probe.get("turboQuantVariantsSupported"))
    configured = bool(external_base_url and model_path) if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE else bool(str(runtime.get("entrypoint") or "").strip() and model_path)
    reachable = False
    reachability_error = ""
    health_socket_reachable = False
    health_http_responding = False
    health_ok = False
    health_state = "unconfigured" if not configured else "socket_unreachable"
    health_url = ""
    health_path = ""
    health_status_code = 0
    health_payload_status = ""
    health_probe_error = ""
    health_transport = ""
    health_body_snippet = ""
    health_candidates = ["/healthz", "/health", "/v1/health", "/v1/models", "/"]
    if configured:
        try:
            if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE and external_base_url:
                reachable = True
                health_socket_reachable = True
                host = ""
                port = 0
            else:
                host = str(runtime.get("host") or DEFAULT_LOCAL_RUNTIME_HOST)
                port = int(runtime.get("port") or DEFAULT_LOCAL_RUNTIME_PORT)
                with socket.create_connection((host, port), timeout=0.3):
                    reachable = True
                    health_socket_reachable = True
        except OSError as exc:
            reachability_error = str(exc)
        if health_socket_reachable:
            probe_targets = (
                _local_runtime_probe_candidates(external_base_url)
                if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE and external_base_url
                else [(f"http://{host}:{port}{candidate}", candidate) for candidate in health_candidates]
            )
            if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE and external_base_url:
                health_candidates = [candidate for _probe_url, candidate in probe_targets]
            for probe_url, candidate in probe_targets:
                try:
                    probe = _urllib_http_request(probe_url, timeout=2)
                except Exception as exc:
                    health_probe_error = str(exc)
                    continue
                health_http_responding = True
                health_url = probe.get("finalUrl") or probe_url
                health_path = candidate
                health_status_code = int(probe.get("status") or 0)
                health_transport = str(probe.get("transport") or "").strip()
                body_text = str(probe.get("body") or "").strip()
                health_body_snippet = body_text[:220]
                parsed_body = parse_json_payload(body_text, default=None)
                if isinstance(parsed_body, dict):
                    health_payload_status = str(parsed_body.get("status") or parsed_body.get("state") or "").strip()
                    health_ok = bool(parsed_body.get("ok")) or health_payload_status in {"ok", "healthy", "ready"}
                    if not health_ok and candidate.endswith("/models"):
                        health_ok = bool(safe_list(parsed_body.get("data")))
                    if not health_ok and candidate.endswith("/api/tags"):
                        health_ok = bool(safe_list(parsed_body.get("models")))
                else:
                    health_payload_status = body_text[:120]
                    health_ok = health_status_code in {200, 204} and bool(body_text)
                if health_ok:
                    health_probe_error = ""
                    health_state = "healthy"
                    break
                health_state = "http_responding"
            if not health_http_responding:
                health_state = "http_unreachable"
    requested_cache_types = []
    for item in (
        kv_cache.get("keyType"),
        kv_cache.get("valueType"),
    ):
        candidate = str(item or "").strip().lower()
        if candidate and candidate not in requested_cache_types:
            requested_cache_types.append(candidate)
    requested_variants = [item for item in requested_cache_types if item.startswith("turbo")]
    requested_cache_types_supported = bool(requested_cache_types) and all(item in supported_cache_types for item in requested_cache_types)
    requested_variants_supported = bool(requested_variants) and all(item in turbo_variants_supported for item in requested_variants)
    turbo_requested = bool(runtime.get("turboQuant"))
    turbo_eligible = backend_name == "llama_cpp" and runtime_mode != DEFAULT_LOCAL_RUNTIME_MODE and bool(resolved_entrypoint) and model_exists
    turbo_verified = turbo_eligible and bool(flag_probe.get("cacheFlagsSupported")) and bool(turbo_variants_supported)
    turbo_active = turbo_requested and requested_variants_supported
    return {
        "entrypointResolved": resolved_entrypoint,
        "entrypointExists": bool(resolved_entrypoint) if runtime_mode != DEFAULT_LOCAL_RUNTIME_MODE else False,
        "modelExists": model_exists,
        "launcherReady": bool(runtime.get("launcherPath")),
        "reachable": reachable,
        "reachabilityError": reachability_error,
        "socketReachable": health_socket_reachable,
        "socketError": reachability_error,
        "httpResponding": health_http_responding,
        "httpStatus": health_status_code or "",
        "httpError": health_probe_error,
        "healthUrl": health_url,
        "healthPath": health_path,
        "healthOk": health_ok,
        "healthBodySnippet": health_body_snippet,
        "healthTransport": health_transport,
        "healthState": health_state,
        "healthCandidates": health_candidates,
        "healthStatusCode": health_status_code,
        "healthPayloadStatus": health_payload_status,
        "healthProbeError": health_probe_error,
        "cacheFlagsSupported": bool(flag_probe.get("cacheFlagsSupported")),
        "ctxSizeFlagSupported": bool(flag_probe.get("ctxSizeFlagSupported")),
        "gpuLayersFlagSupported": bool(flag_probe.get("gpuLayersFlagSupported")),
        "probeOk": bool(flag_probe.get("probeOk")),
        "probeError": str(flag_probe.get("probeError") or "").strip(),
        "supportedCacheTypes": supported_cache_types,
        "turboQuantVariantsSupported": turbo_variants_supported,
        "requestedCacheTypes": requested_cache_types,
        "requestedCacheTypesSupported": requested_cache_types_supported,
        "turboQuantRequested": turbo_requested,
        "turboQuantRequestedVariants": requested_variants,
        "turboQuantRequestedVariantsSupported": requested_variants_supported,
        "turboQuantEligible": turbo_eligible,
        "turboQuantVerified": turbo_verified,
        "turboQuantActive": turbo_active,
        "helpFlags": safe_list(flag_probe.get("helpFlags")),
        "health": {
            "state": health_state,
            "configured": configured,
            "socketReachable": health_socket_reachable,
            "socketError": reachability_error,
            "httpResponding": health_http_responding,
            "httpStatus": health_status_code or "",
            "healthUrl": health_url,
            "healthPath": health_path,
            "healthOk": health_ok,
            "healthBodySnippet": health_body_snippet,
            "healthTransport": health_transport,
            "healthProbeError": health_probe_error,
            "healthPayloadStatus": health_payload_status,
            "checkedAt": now_iso(),
            "candidates": health_candidates,
        },
    }


def _speech_runtime_probe_candidates(base_url):
    normalized = _normalize_runtime_base_url(base_url)
    if not normalized:
        return []
    if normalized.endswith("/audio/speech"):
        root = normalized.rsplit("/audio/speech", 1)[0]
        return [f"{root}/healthz", f"{root}/health", f"{root}/models", normalized]
    if normalized.endswith("/v1"):
        root = normalized[: -len("/v1")]
        return [f"{root}/healthz", f"{root}/health", f"{normalized}/models", normalized]
    return [f"{normalized}/healthz", f"{normalized}/health", f"{normalized}/v1/models", normalized]


def _local_runtime_probe_candidates(base_url):
    normalized = _normalize_runtime_base_url(base_url)
    if not normalized:
        return []
    if normalized.endswith("/v1"):
        root = normalized[: -len("/v1")]
        return [
            (f"{normalized}/models", "/models"),
            (f"{root}/api/tags", "/api/tags"),
            (normalized, "/"),
        ]
    return [
        (f"{normalized}/healthz", "/healthz"),
        (f"{normalized}/health", "/health"),
        (f"{normalized}/v1/models", "/v1/models"),
        (f"{normalized}/api/tags", "/api/tags"),
        (normalized, "/"),
    ]


def build_speech_runtime_capability_payload(runtime_payload):
    runtime = runtime_payload if isinstance(runtime_payload, dict) else {}
    provider = str(runtime.get("provider") or DEFAULT_SPEECH_RUNTIME_PROVIDER).strip().lower()
    base_url = _normalize_runtime_base_url(runtime.get("baseUrl"))
    configured = bool(base_url)
    ready = False
    responding = False
    health_ok = False
    health_url = ""
    health_state = "unconfigured" if not configured else "unreachable"
    health_status_code = 0
    health_payload_status = ""
    health_probe_error = ""
    body_snippet = ""
    checked_at = now_iso()
    candidates = _speech_runtime_probe_candidates(base_url)
    if configured:
        for candidate in candidates:
            try:
                probe = _urllib_http_request(candidate, timeout=2)
            except Exception as exc:
                health_probe_error = str(exc)
                continue
            responding = True
            health_url = str(probe.get("finalUrl") or candidate).strip()
            health_status_code = int(probe.get("status") or 0)
            body_text = str(probe.get("body") or "").strip()
            body_snippet = body_text[:220]
            parsed_body = parse_json_payload(body_text, default=None)
            if isinstance(parsed_body, dict):
                health_payload_status = str(parsed_body.get("status") or parsed_body.get("state") or "").strip()
                health_ok = bool(parsed_body.get("ok")) or health_payload_status in {"ok", "healthy", "ready"}
                if not health_ok and candidate.endswith("/models"):
                    data = safe_list(parsed_body.get("data"))
                    health_ok = bool(data) or bool(parsed_body.get("voices"))
            else:
                health_payload_status = body_text[:120]
                health_ok = health_status_code in {200, 204}
            if health_ok:
                health_probe_error = ""
                health_state = "healthy"
                ready = True
                break
            health_state = "responding"
        if not responding and health_probe_error:
            health_state = "unreachable"
    return {
        "configured": configured,
        "ready": ready,
        "provider": provider,
        "baseUrl": base_url,
        "httpResponding": responding,
        "healthOk": health_ok,
        "healthState": health_state,
        "healthUrl": health_url,
        "healthStatusCode": health_status_code,
        "healthPayloadStatus": health_payload_status,
        "healthProbeError": health_probe_error,
        "healthBodySnippet": body_snippet,
        "checkedAt": checked_at,
        "candidates": candidates,
    }


def _build_turboquant_reference_payload():
    try:
        from .turboquant_vendor import build_turboquant_reference_payload
    except Exception as exc:  # pragma: no cover - surfaced in runtime diagnostics
        return {
            "bundled": False,
            "available": False,
            "roundTripVerified": False,
            "library": "turboquant_plus",
            "license": "Apache-2.0",
            "variantIds": ["turbo3", "turbo4"],
            "algorithms": [],
            "summary": "TurboQuant reference algorithm could not be loaded from the vendored path.",
            "error": f"{type(exc).__name__}: {exc}",
        }
    return build_turboquant_reference_payload()


def load_local_runtime_config(openclaw_dir, config=None, metadata=None):
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    runtime = local_metadata.get(LOCAL_RUNTIME_CONFIG_KEY) if isinstance(local_metadata.get(LOCAL_RUNTIME_CONFIG_KEY), dict) else {}
    return normalize_local_runtime_config(runtime)


def local_runtime_command_parts(local_runtime):
    runtime = normalize_local_runtime_config(local_runtime)
    if str(runtime.get("mode") or "").strip().lower() == DEFAULT_LOCAL_RUNTIME_MODE:
        return []
    entrypoint = str(runtime.get("entrypoint") or "").strip()
    if not entrypoint:
        return []
    args = [entrypoint]
    if str(runtime.get("backend") or "").strip().lower() == "llama_cpp":
        args.extend(["--host", str(runtime.get("host") or DEFAULT_LOCAL_RUNTIME_HOST)])
        args.extend(["--port", str(runtime.get("port") or DEFAULT_LOCAL_RUNTIME_PORT)])
        args.extend(["--ctx-size", str(runtime.get("contextLength") or DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH)])
        args.extend(["--n-gpu-layers", str(runtime.get("gpuLayers") if runtime.get("gpuLayers") is not None else DEFAULT_LOCAL_RUNTIME_GPU_LAYERS)])
        kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
        if kv_cache.get("enabled"):
            key_type = str(kv_cache.get("keyType") or "").strip()
            value_type = str(kv_cache.get("valueType") or "").strip()
            if key_type:
                args.extend(["--cache-type-k", key_type])
            if value_type:
                args.extend(["--cache-type-v", value_type])
        model_path = str(runtime.get("modelPath") or "").strip()
        if model_path:
            args.extend(["--model", model_path])
    args.extend(runtime.get("extraArgs") if isinstance(runtime.get("extraArgs"), list) else [])
    return [str(item) for item in args if str(item).strip()]


def local_runtime_launcher_script_path(openclaw_dir):
    project_dir = resolve_project_dir(openclaw_dir)
    candidates = []
    if project_dir:
        candidates.append(Path(project_dir) / "platform" / "bin" / "runtime" / "launch_local_model_runtime.py")
    candidates.extend(
        [
            Path(openclaw_dir) / "bin" / "runtime" / "launch_local_model_runtime.py",
            Path("/app/platform/bin/runtime/launch_local_model_runtime.py"),
            _repo_root() / "platform" / "bin" / "runtime" / "launch_local_model_runtime.py",
        ]
    )
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
    raise FileNotFoundError("Missing local runtime launcher script.")


def load_local_runtime_payload(openclaw_dir, config=None, metadata=None, include_capabilities=True):
    runtime = load_local_runtime_config(openclaw_dir, config=config, metadata=metadata)
    runtime_mode = str(runtime.get("mode") or "").strip().lower()
    launcher_path = ""
    launcher_error = ""
    if runtime_mode != DEFAULT_LOCAL_RUNTIME_MODE:
        try:
            launcher_path = str(local_runtime_launcher_script_path(openclaw_dir))
        except FileNotFoundError as error:
            launcher_error = str(error)
    command_parts = local_runtime_command_parts(runtime)
    command_preview = shlex.join(command_parts) if command_parts else str(runtime.get("commandPreview") or "").strip()
    configured = bool(str(runtime.get("baseUrl") or "").strip() and str(runtime.get("modelPath") or "").strip()) if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE else bool(str(runtime.get("entrypoint") or "").strip() and str(runtime.get("modelPath") or "").strip())
    enabled = bool(runtime.get("enabled"))
    pid_raw = str(runtime.get("pid") or "").strip()
    pid = int(pid_raw) if pid_raw.isdigit() else 0
    running = False
    if pid > 0:
        try:
            os.kill(pid, 0)
            running = True
        except OSError:
            running = False
    state = "disabled"
    if enabled and configured:
        state = "running" if running else "ready"
    elif enabled:
        state = "incomplete"
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    key_type = str(kv_cache.get("keyType") or "").strip()
    value_type = str(kv_cache.get("valueType") or "").strip()
    payload = {
        **runtime,
        "configured": configured,
        "running": running,
        "pid": pid if pid > 0 else "",
        "state": state,
        "ok": enabled and configured and not launcher_error,
        "label": {
            "disabled": "Disabled",
            "incomplete": "Incomplete",
            "ready": "Ready",
            "running": "Running",
        }.get(state, "Disabled"),
        "launcherPath": launcher_path,
        "launcherError": launcher_error,
        "commandPreview": command_preview,
        "recommendedProfiles": _build_local_runtime_recommended_profiles(),
        "cacheTypeK": key_type,
        "cacheTypeV": value_type,
        "turboQuant": bool(kv_cache.get("enabled")) and any(
            str(item).strip().lower().startswith("turbo")
            for item in (key_type, value_type, kv_cache.get("mode"))
            if str(item).strip()
        ),
    }
    payload["capabilities"] = build_local_runtime_capability_payload(payload) if include_capabilities else {}
    payload["health"] = payload["capabilities"].get("health", {}) if isinstance(payload.get("capabilities"), dict) else {}
    if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE:
        health_state = str((payload.get("capabilities") or {}).get("healthState") or "").strip()
        payload["running"] = enabled and configured and health_state in {"healthy", "http_responding"}
        payload["state"] = "running" if payload["running"] else ("ready" if enabled and configured else state)
        payload["ok"] = enabled and configured
    return payload


def _ordered_unique_strings(items):
    seen = set()
    ordered = []
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _build_provider_family_rows(provider_rows, provider_assignments):
    family_map = {}
    for row in provider_rows if isinstance(provider_rows, list) else []:
        provider_id = str(row.get("id") or "").strip()
        if not provider_id:
            continue
        capability_profile = _provider_capability_profile(provider_id)
        family_map[provider_id] = {
            "id": provider_id,
            "label": str(row.get("label") or provider_id).strip(),
            "configured": bool(row.get("configured")),
            "agentCount": int(row.get("agentCount") or 0),
            "active": False,
            "router": False,
            "agentIds": [],
            "models": [],
            "capabilityTags": capability_profile.get("capabilityTags", []),
            "summary": capability_profile.get("summary", ""),
            "bestFit": capability_profile.get("bestFit", ""),
            "contextWindow": capability_profile.get("contextWindow", ""),
            "multimodalInputs": capability_profile.get("multimodalInputs", []),
            "supports": capability_profile.get("supports", {}),
        }
    for assignment in provider_assignments if isinstance(provider_assignments, list) else []:
        provider_id = str(assignment.get("providerId") or "").strip() or "custom"
        capability_profile = _provider_capability_profile(provider_id)
        family = family_map.setdefault(
            provider_id,
            {
                "id": provider_id,
                "label": str(assignment.get("providerLabel") or provider_id).strip(),
                "configured": False,
                "agentCount": 0,
                "active": False,
                "router": False,
                "agentIds": [],
                "models": [],
                "capabilityTags": capability_profile.get("capabilityTags", []),
                "summary": capability_profile.get("summary", ""),
                "bestFit": capability_profile.get("bestFit", ""),
                "contextWindow": capability_profile.get("contextWindow", ""),
                "multimodalInputs": capability_profile.get("multimodalInputs", []),
                "supports": capability_profile.get("supports", {}),
            },
        )
        family["active"] = True
        family["router"] = family["router"] or bool(assignment.get("isRouter"))
        family["agentIds"] = _ordered_unique_strings([*family["agentIds"], assignment.get("id")])
        family["models"] = _ordered_unique_strings([*family["models"], assignment.get("model")])
        family["agentCount"] = max(int(family.get("agentCount") or 0), len(family["agentIds"]))
    ordered = sorted(
        family_map.values(),
        key=lambda item: (
            0 if item.get("router") else 1,
            0 if item.get("configured") else 1,
            str(item.get("label") or item.get("id") or ""),
        ),
    )
    return ordered


def _build_model_access_matrix_rows(provider_rows, provider_assignments):
    family_map = {}
    for row in provider_rows if isinstance(provider_rows, list) else []:
        provider_id = str(row.get("id") or "").strip()
        if not provider_id:
            continue
        capability_profile = _provider_capability_profile(provider_id)
        family_map[provider_id] = {
            "id": provider_id,
            "label": str(row.get("label") or provider_id).strip(),
            "configured": bool(row.get("configured")),
            "configuredKeys": safe_list(row.get("configuredKeys")),
            "active": False,
            "router": False,
            "agentIds": [],
            "routerAgentIds": [],
            "models": {},
            "capabilityTags": capability_profile.get("capabilityTags", []),
            "summary": capability_profile.get("summary", ""),
            "bestFit": capability_profile.get("bestFit", ""),
            "contextWindow": capability_profile.get("contextWindow", ""),
            "multimodalInputs": capability_profile.get("multimodalInputs", []),
            "supports": capability_profile.get("supports", {}),
        }
    for assignment in provider_assignments if isinstance(provider_assignments, list) else []:
        provider_id = str(assignment.get("providerId") or "").strip() or "custom"
        capability_profile = _provider_capability_profile(provider_id)
        family = family_map.setdefault(
            provider_id,
            {
                "id": provider_id,
                "label": str(assignment.get("providerLabel") or provider_id).strip(),
                "configured": False,
                "configuredKeys": [],
                "active": False,
                "router": False,
                "agentIds": [],
                "routerAgentIds": [],
                "models": {},
                "capabilityTags": capability_profile.get("capabilityTags", []),
                "summary": capability_profile.get("summary", ""),
                "bestFit": capability_profile.get("bestFit", ""),
                "contextWindow": capability_profile.get("contextWindow", ""),
                "multimodalInputs": capability_profile.get("multimodalInputs", []),
                "supports": capability_profile.get("supports", {}),
            },
        )
        model_name = str(assignment.get("model") or "").strip() or "unassigned"
        model_row = family["models"].setdefault(
            model_name,
            {
                "name": model_name,
                "providerId": provider_id,
                "providerLabel": str(family.get("label") or provider_id).strip(),
                "configured": bool(family.get("configured")),
                "active": False,
                "router": False,
                "agentIds": [],
                "routerAgentIds": [],
                "agents": [],
            },
        )
        agent_id = str(assignment.get("id") or "").strip()
        agent_title = str(assignment.get("title") or agent_id).strip()
        agent_workspace = str(assignment.get("workspace") or "").strip()
        agent_entry = {
            "id": agent_id,
            "title": agent_title,
            "workspace": agent_workspace,
            "isRouter": bool(assignment.get("isRouter")),
        }
        model_row["active"] = True
        model_row["router"] = model_row["router"] or bool(assignment.get("isRouter"))
        model_row["agentIds"] = _ordered_unique_strings([*model_row["agentIds"], agent_id])
        if assignment.get("isRouter"):
            model_row["routerAgentIds"] = _ordered_unique_strings([*model_row["routerAgentIds"], agent_id])
        existing_agents = {str(item.get("id") or "").strip(): item for item in model_row["agents"] if isinstance(item, dict)}
        if agent_id and agent_id not in existing_agents:
            model_row["agents"].append(agent_entry)
        elif agent_id and agent_id in existing_agents:
            existing_agents[agent_id].update(agent_entry)
        family["active"] = True
        family["router"] = family["router"] or bool(assignment.get("isRouter"))
        family["agentIds"] = _ordered_unique_strings([*family["agentIds"], agent_id])
        if assignment.get("isRouter"):
            family["routerAgentIds"] = _ordered_unique_strings([*family["routerAgentIds"], agent_id])
    ordered = []
    for family in sorted(
        family_map.values(),
        key=lambda item: (
            0 if item.get("router") else 1,
            0 if item.get("configured") else 1,
            str(item.get("label") or item.get("id") or ""),
        ),
    ):
        model_rows = []
        for model_row in sorted(
            (family.get("models") or {}).values(),
            key=lambda item: (
                0 if item.get("router") else 1,
                0 if item.get("active") else 1,
                -len(item.get("agentIds") or []),
                str(item.get("name") or ""),
            ),
        ):
            model_rows.append(
                {
                    "name": str(model_row.get("name") or "").strip(),
                    "providerId": str(model_row.get("providerId") or family.get("id") or "").strip(),
                    "providerLabel": str(model_row.get("providerLabel") or family.get("label") or "").strip(),
                    "configured": bool(model_row.get("configured")),
                    "active": bool(model_row.get("active")),
                    "router": bool(model_row.get("router")),
                    "agentCount": len(model_row.get("agentIds") or []),
                    "agentIds": _ordered_unique_strings(model_row.get("agentIds") or []),
                    "routerAgentIds": _ordered_unique_strings(model_row.get("routerAgentIds") or []),
                    "agents": sorted(
                        [
                            {
                                "id": str(agent.get("id") or "").strip(),
                                "title": str(agent.get("title") or agent.get("id") or "").strip(),
                                "workspace": str(agent.get("workspace") or "").strip(),
                                "isRouter": bool(agent.get("isRouter")),
                            }
                            for agent in model_row.get("agents", [])
                            if isinstance(agent, dict)
                        ],
                        key=lambda item: (
                            0 if item.get("isRouter") else 1,
                            str(item.get("title") or item.get("id") or ""),
                        ),
                    ),
                }
            )
        ordered.append(
            {
                "id": str(family.get("id") or "").strip(),
                "label": str(family.get("label") or family.get("id") or "").strip(),
                "configured": bool(family.get("configured")),
                "configuredKeys": safe_list(family.get("configuredKeys")),
                "active": bool(family.get("active")),
                "router": bool(family.get("router")),
                "agentCount": len(family.get("agentIds") or []),
                "agentIds": _ordered_unique_strings(family.get("agentIds") or []),
                "routerAgentIds": _ordered_unique_strings(family.get("routerAgentIds") or []),
                "models": model_rows,
                "capabilityTags": clean_unique_strings(family.get("capabilityTags") or []),
                "summary": str(family.get("summary") or "").strip(),
                "bestFit": str(family.get("bestFit") or "").strip(),
                "contextWindow": str(family.get("contextWindow") or "").strip(),
                "multimodalInputs": clean_unique_strings(family.get("multimodalInputs") or []),
                "supports": deepcopy(family.get("supports") if isinstance(family.get("supports"), dict) else {}),
            }
        )
    return ordered


def _normalize_hosted_provider_context_budget_policy(value):
    policy = _normalize_runtime_text(value, DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY).lower()
    if policy not in HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES:
        return DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY
    return policy


def _build_hosted_provider_context_budget_profile(policy):
    normalized_policy = _normalize_hosted_provider_context_budget_policy(policy)
    profile = deepcopy(HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_PROFILES.get(normalized_policy, HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_PROFILES["balanced"]))
    profile.update(
        {
            "policy": normalized_policy,
            "appliesTo": ["provider_api"],
        }
    )
    return profile


def _build_execution_policy_payload(execution, provider_families, primary_path, fallback_path):
    preferred_provider_id = str(execution.get("preferredProviderId") or "").strip()
    preferred_family = next((item for item in provider_families if item.get("id") == preferred_provider_id), None)
    active_families = [item for item in provider_families if item.get("active")]
    configured_families = [item for item in provider_families if item.get("configured")]
    multi_model = sum(len(item.get("models") or []) for item in active_families) > 1
    multi_provider = len(active_families) > 1
    hosted_provider_context_budget_policy = _normalize_hosted_provider_context_budget_policy(
        execution.get("hostedProviderContextBudgetPolicy")
    )
    hosted_provider_context_budget_profile = _build_hosted_provider_context_budget_profile(
        hosted_provider_context_budget_policy
    )
    summary_parts = [
        f"transport={str(execution.get('transport') or DEFAULT_EXECUTION_TRANSPORT).strip()}",
        f"primary={primary_path}",
        f"fallback={fallback_path}",
        f"hostedBudget={hosted_provider_context_budget_policy}",
        f"families={len(active_families)}",
    ]
    if preferred_family:
        summary_parts.append(f"preferred={preferred_family.get('label')}")
    elif preferred_provider_id:
        summary_parts.append(f"preferred={preferred_provider_id}")
    return {
        "transport": str(execution.get("transport") or DEFAULT_EXECUTION_TRANSPORT).strip(),
        "contextMode": str(execution.get("contextMode") or DEFAULT_EXECUTION_CONTEXT_MODE).strip(),
        "localRuntimeRole": str(execution.get("localRuntimeRole") or DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE).strip(),
        "primaryPath": primary_path,
        "fallbackPath": fallback_path,
        "preferredProviderId": preferred_provider_id,
        "hostedProviderContextBudgetPolicy": hosted_provider_context_budget_policy,
        "hostedProviderContextBudgetProfile": hosted_provider_context_budget_profile,
        "preferredProviderLabel": str((preferred_family or {}).get("label") or "").strip(),
        "activeFamilyCount": len(active_families),
        "configuredFamilyCount": len(configured_families),
        "multiModel": multi_model,
        "multiProvider": multi_provider,
        "summary": ", ".join(summary_parts),
    }


def _build_openclaw_model_topology(openclaw_dir, config=None):
    config = config or load_config(openclaw_dir)
    config_agents = ((config.get("agents", {}) if isinstance(config, dict) else {}) or {}).get("list", [])
    router_agent_id = get_router_agent_id(config)
    local_runtime_payload = load_local_runtime_payload(openclaw_dir, config=config, include_capabilities=True)
    local_runtime_active = bool(local_runtime_payload.get("configured")) and bool(
        local_runtime_payload.get("running") or local_runtime_payload.get("enabled")
    )

    provider_assignments = []
    provider_counter = Counter()
    model_counter = Counter()
    for agent in config_agents if isinstance(config_agents, list) else []:
        if not isinstance(agent, dict):
            continue
        model_name = str(agent.get("model") or "").strip()
        provider = infer_model_provider(model_name)
        provider_id = str(provider.get("id") or "custom").strip() or "custom"
        provider_label = str(provider.get("label") or "兼容 / 自定义").strip() or "兼容 / 自定义"
        agent_title = str(((agent.get("identity") or {}) if isinstance(agent.get("identity"), dict) else {}).get("name") or agent.get("id") or "").strip()
        agent_workspace = str(agent.get("workspace") or "").strip()
        if model_name:
            provider_counter[provider_id] += 1
            model_counter[model_name] += 1
        provider_assignments.append(
            {
                "id": str(agent.get("id") or "").strip(),
                "title": agent_title,
                "workspace": agent_workspace,
                "model": model_name,
                "providerId": provider_id,
                "providerLabel": provider_label,
                "isRouter": agent.get("id") == router_agent_id,
            }
        )

    router_assignment = next((item for item in provider_assignments if item.get("isRouter")), {})
    provider_rows = []
    for provider in _model_provider_catalog():
        readiness = provider_key_status(openclaw_dir, provider)
        provider_id = str(provider.get("id") or "").strip()
        if provider_id == "gemma" and local_runtime_active:
            configured_keys = safe_list(readiness.get("configuredKeys"))
            if "LOCAL_RUNTIME" not in configured_keys:
                configured_keys.append("LOCAL_RUNTIME")
            readiness = {
                **readiness,
                "configured": True,
                "configuredKeys": configured_keys,
            }
        capability_profile = _provider_capability_profile(provider.get("id"))
        provider_rows.append(
            {
                "id": provider_id,
                "label": str(provider.get("label") or provider.get("id") or "").strip(),
                "configured": bool(readiness.get("configured")),
                "configuredKeys": safe_list(readiness.get("configuredKeys")),
                "envKeys": list(provider.get("env", ())),
                "agentCount": provider_counter.get(provider_id, 0),
                "status": "ready" if readiness.get("configured") else "warning",
                "starterModels": capability_profile.get("starterModels", []),
                "defaultModel": _provider_default_starter_model(provider_id),
                "capabilityTags": capability_profile.get("capabilityTags", []),
                "summary": capability_profile.get("summary", ""),
                "bestFit": capability_profile.get("bestFit", ""),
                "contextWindow": capability_profile.get("contextWindow", ""),
                "multimodalInputs": capability_profile.get("multimodalInputs", []),
                "supports": capability_profile.get("supports", {}),
            }
        )
    ready_provider_rows = [item for item in provider_rows if item.get("configured")]
    provider_families = _build_provider_family_rows(provider_rows, provider_assignments)
    active_model_families = [item for item in provider_families if item.get("active")]
    model_access_matrix = _build_model_access_matrix_rows(provider_rows, provider_assignments)
    matrix_agent_ids = _ordered_unique_strings(
        agent_id
        for item in model_access_matrix
        for agent_id in safe_list(item.get("agentIds"))
    )
    matrix_router_agent_ids = _ordered_unique_strings(
        agent_id
        for item in model_access_matrix
        for agent_id in safe_list(item.get("routerAgentIds"))
    )
    matrix_model_rows = [model for item in model_access_matrix for model in safe_list(item.get("models"))]
    model_access_summary = {
        "familyCount": len(model_access_matrix),
        "configuredFamilyCount": sum(1 for item in model_access_matrix if item.get("configured")),
        "activeFamilyCount": sum(1 for item in model_access_matrix if item.get("active")),
        "modelCount": len(matrix_model_rows),
        "agentCount": len(matrix_agent_ids),
        "routerFamilyCount": sum(1 for item in model_access_matrix if item.get("router")),
        "routerModelCount": sum(1 for model in matrix_model_rows if model.get("router")),
        "routerAgentCount": len(matrix_router_agent_ids),
    }
    model_inventory = []
    for model_name, count in model_counter.most_common():
        provider = infer_model_provider(model_name)
        model_inventory.append(
            {
                "name": model_name,
                "provider": provider.get("label", "兼容 / 自定义"),
                "providerId": provider.get("id", "custom"),
                "agentCount": count,
                "agents": [
                    item.get("title") or item.get("id")
                    for item in provider_assignments
                    if item.get("model") == model_name
                ],
            }
        )
    model_summary = {
        "totalAgents": len(provider_assignments),
        "assignedAgents": sum(1 for item in provider_assignments if item.get("model")),
        "distinctModels": len(model_counter),
        "readyProviders": len(ready_provider_rows),
        "routerModel": str(router_assignment.get("model") or "").strip(),
        "routerProvider": str(router_assignment.get("providerLabel") or "").strip(),
    }
    return {
        "providerAssignments": provider_assignments,
        "routerAssignment": router_assignment,
        "providerRows": provider_rows,
        "readyProviderRows": ready_provider_rows,
        "providerFamilies": provider_families,
        "activeModelFamilies": active_model_families,
        "modelAccessMatrix": model_access_matrix,
        "modelAccessMatrixSummary": model_access_summary,
        "modelInventory": model_inventory,
        "modelSummary": model_summary,
    }


def load_model_execution_architecture_payload(
    openclaw_dir,
    config=None,
    metadata=None,
    include_local_runtime_capabilities=True,
    include_model_access_matrix=True,
    local_runtime_payload=None,
    topology=None,
):
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    execution = load_execution_config(openclaw_dir, config=config, metadata=metadata)
    local_runtime = (
        local_runtime_payload
        if isinstance(local_runtime_payload, dict)
        else load_local_runtime_payload(
            openclaw_dir,
            config=config,
            metadata=metadata,
            include_capabilities=include_local_runtime_capabilities,
        )
    )
    topology = topology if isinstance(topology, dict) else _build_openclaw_model_topology(openclaw_dir, config=config)
    provider_assignments = topology.get("providerAssignments") or []
    router_assignment = topology.get("routerAssignment") or {}
    provider_rows = topology.get("providerRows") or []
    ready_provider_rows = topology.get("readyProviderRows") or []
    provider_families = topology.get("providerFamilies") or []
    active_model_families = topology.get("activeModelFamilies") or []
    model_access_matrix = topology.get("modelAccessMatrix") or []
    model_access_summary = topology.get("modelAccessMatrixSummary") or {}

    local_runtime_enabled = bool(local_runtime.get("enabled"))
    local_runtime_configured = bool(local_runtime.get("configured"))
    local_runtime_running = bool(local_runtime.get("running"))
    local_runtime_backend = str(local_runtime.get("backend") or "").strip().lower()
    kv_cache = local_runtime.get("kvCache") if isinstance(local_runtime.get("kvCache"), dict) else {}
    local_runtime_capabilities = local_runtime.get("capabilities") if isinstance(local_runtime.get("capabilities"), dict) else {}
    turboquant_eligible = bool(local_runtime_capabilities.get("turboQuantEligible"))
    turboquant_active = bool(local_runtime_capabilities.get("turboQuantActive"))
    turboquant_reference = _build_turboquant_reference_payload()
    hosted_provider_context_budget_policy = _normalize_hosted_provider_context_budget_policy(execution.get("hostedProviderContextBudgetPolicy"))
    hosted_provider_context_budget_profile = _build_hosted_provider_context_budget_profile(hosted_provider_context_budget_policy)

    external_provider_ready = bool(ready_provider_rows or router_assignment.get("providerId"))
    preferred_primary = str(execution.get("primaryPath") or DEFAULT_EXECUTION_PRIMARY_PATH).strip()
    if preferred_primary == "local_runtime":
        if local_runtime_configured:
            primary_path = "local_runtime"
        elif external_provider_ready:
            primary_path = "provider_api"
        else:
            primary_path = "unconfigured"
    elif preferred_primary == "auto":
        if external_provider_ready:
            primary_path = "provider_api"
        elif local_runtime_configured:
            primary_path = "local_runtime"
        else:
            primary_path = "unconfigured"
    else:
        if external_provider_ready:
            primary_path = "provider_api"
        elif local_runtime_configured:
            primary_path = "local_runtime"
        else:
            primary_path = "unconfigured"

    preferred_fallback = str(execution.get("fallbackPath") or DEFAULT_EXECUTION_FALLBACK_PATH).strip()
    if preferred_fallback == "provider_api" and external_provider_ready and primary_path != "provider_api":
        fallback_path = "provider_api"
    elif preferred_fallback == "local_runtime" and local_runtime_configured and primary_path != "local_runtime":
        fallback_path = "local_runtime"
    else:
        fallback_path = "none"

    if primary_path == "provider_api":
        decision_summary = "OpenClaw should stay on the external provider path first."
        decision_reason = (
            "Your current stack routes prompts through OpenClaw to hosted provider APIs. "
            "Application-layer context layering applies immediately across multiple external model families, "
            "while TurboQuant-style KV compression only applies if you also run a local runtime."
        )
    elif primary_path == "local_runtime":
        decision_summary = "OpenClaw can run directly against the local runtime."
        decision_reason = (
            "No ready provider path was detected, so the local runtime becomes the primary execution path. "
            "This is the layer where TurboQuant-style KV-cache compression can actually take effect."
        )
    else:
        decision_summary = "Execution path still needs to be completed."
        decision_reason = (
            "OpenClaw is present, but neither a ready external provider path nor a complete local runtime path was detected."
        )
    execution_policy = _build_execution_policy_payload(execution, provider_families, primary_path, fallback_path)

    return {
        "topology": "openclaw_team -> openclaw -> provider_or_local_runtime",
        "openclawRole": "router_and_prompt_assembler",
        "preferences": execution,
        "primaryPath": primary_path,
        "fallbackPath": fallback_path,
        "contextStrategy": {
            "mode": "layered",
            "hotWarmCold": True,
            "appliesTo": ["provider_api", "local_runtime"],
            "summary": (
                "OpenClaw Team should always use hot context, warm summary, and cold history. "
                "That applies whether OpenClaw routes to a hosted provider API or to a local runtime."
            ),
        },
        "providerPath": {
            "kind": "external_provider",
            "ready": external_provider_ready,
            "readyProviderCount": len(ready_provider_rows),
            "configuredProviders": ready_provider_rows,
            "providerFamilies": provider_families,
            "activeModelFamilies": active_model_families,
            "modelAccessMatrix": model_access_matrix if include_model_access_matrix else [],
            "modelAccessMatrixSummary": model_access_summary,
            "preferredProviderId": execution_policy.get("preferredProviderId", ""),
            "preferredProviderLabel": execution_policy.get("preferredProviderLabel", ""),
            "multiProvider": bool(execution_policy.get("multiProvider")),
            "multiModel": bool(execution_policy.get("multiModel")),
            "routerProviderId": str(router_assignment.get("providerId") or "").strip(),
            "routerProviderLabel": str(router_assignment.get("providerLabel") or "").strip(),
            "routerModel": str(router_assignment.get("model") or "").strip(),
            "hostedProviderContextBudgetPolicy": hosted_provider_context_budget_policy,
            "hostedProviderContextBudgetProfile": hosted_provider_context_budget_profile,
            "turboQuantApplies": False,
            "summary": (
                "External providers are the primary path for OpenClaw when you use hosted APIs from different model families. "
                "TurboQuant does not run inside those provider services; OpenClaw Team can only optimize the context it sends. "
                f"Hosted-provider context budget policy is set to {hosted_provider_context_budget_profile.get('label') or hosted_provider_context_budget_policy}."
            ),
        },
        "localRuntimePath": {
            "kind": "local_runtime",
            "enabled": local_runtime_enabled,
            "configured": local_runtime_configured,
            "running": local_runtime_running,
            "backend": str(local_runtime.get("backend") or "").strip(),
            "entrypoint": str(local_runtime.get("entrypoint") or "").strip(),
            "modelPath": str(local_runtime.get("modelPath") or "").strip(),
            "kvCacheEnabled": bool(kv_cache.get("enabled")),
            "kvCacheMode": str(kv_cache.get("mode") or "").strip(),
            "cacheTypeK": str(kv_cache.get("keyType") or "").strip(),
            "cacheTypeV": str(kv_cache.get("valueType") or "").strip(),
            "reachable": bool(local_runtime_capabilities.get("reachable")),
            "socketReachable": bool(local_runtime_capabilities.get("socketReachable")),
            "socketError": str(local_runtime_capabilities.get("socketError") or "").strip(),
            "httpResponding": bool(local_runtime_capabilities.get("httpResponding")),
            "httpStatus": local_runtime_capabilities.get("httpStatus") or "",
            "httpError": str(local_runtime_capabilities.get("httpError") or "").strip(),
            "healthOk": bool(local_runtime_capabilities.get("healthOk")),
            "healthUrl": str(local_runtime_capabilities.get("healthUrl") or "").strip(),
            "healthPath": str(local_runtime_capabilities.get("healthPath") or "").strip(),
            "healthStatusCode": int(local_runtime_capabilities.get("healthStatusCode") or 0),
            "healthPayloadStatus": str(local_runtime_capabilities.get("healthPayloadStatus") or "").strip(),
            "healthProbeError": str(local_runtime_capabilities.get("healthProbeError") or "").strip(),
            "healthState": str(local_runtime_capabilities.get("healthState") or "").strip(),
            "healthBodySnippet": str(local_runtime_capabilities.get("healthBodySnippet") or "").strip(),
            "healthTransport": str(local_runtime_capabilities.get("healthTransport") or "").strip(),
            "health": local_runtime_capabilities.get("health") if isinstance(local_runtime_capabilities.get("health"), dict) else {},
            "probeOk": bool(local_runtime_capabilities.get("probeOk")),
            "probeError": str(local_runtime_capabilities.get("probeError") or "").strip(),
            "entrypointExists": bool(local_runtime_capabilities.get("entrypointExists")),
            "modelExists": bool(local_runtime_capabilities.get("modelExists")),
            "cacheFlagsSupported": bool(local_runtime_capabilities.get("cacheFlagsSupported")),
            "supportedCacheTypes": safe_list(local_runtime_capabilities.get("supportedCacheTypes")),
            "requestedCacheTypes": safe_list(local_runtime_capabilities.get("requestedCacheTypes")),
            "requestedCacheTypesSupported": bool(local_runtime_capabilities.get("requestedCacheTypesSupported")),
            "launcherReady": bool(local_runtime_capabilities.get("launcherReady")),
            "turboQuantEligible": turboquant_eligible,
            "turboQuantVariantsSupported": safe_list(local_runtime_capabilities.get("turboQuantVariantsSupported")),
            "turboQuantRequestedVariants": safe_list(local_runtime_capabilities.get("turboQuantRequestedVariants")),
            "turboQuantRequestedVariantsSupported": bool(local_runtime_capabilities.get("turboQuantRequestedVariantsSupported")),
            "turboQuantVerified": bool(local_runtime_capabilities.get("turboQuantVerified")),
            "turboQuantActive": turboquant_active,
            "referenceAlgorithm": turboquant_reference,
            "referenceAlgorithmBundled": bool(turboquant_reference.get("bundled")),
            "referenceAlgorithmAvailable": bool(turboquant_reference.get("available")),
            "referenceAlgorithmRoundTripVerified": bool(turboquant_reference.get("roundTripVerified")),
            "referenceAlgorithmProductRole": str(turboquant_reference.get("productRole") or "").strip(),
            "referenceAlgorithmUsedForLiveInference": bool(turboquant_reference.get("usedForLiveInference")),
            "referenceAlgorithmRequiresCompatibleBackend": bool(turboquant_reference.get("requiresCompatibleBackend")),
            "referenceAlgorithmLiveAccelerationStatus": str(turboquant_reference.get("liveAccelerationStatus") or "").strip(),
            "summary": (
                "Local runtime is the optional acceleration path. TurboQuant-style KV compression only applies here, "
                "when the backend actually supports local KV-cache control. "
                + (
                    "OpenClaw Team now vendors the reference TurboQuant algorithm for local verification, "
                    "but that vendored reference is not the live inference engine."
                    if turboquant_reference.get("bundled")
                    else "No vendored TurboQuant reference algorithm was detected."
                )
            ),
        },
        "executionPolicy": execution_policy,
        "decision": {
            "summary": decision_summary,
            "reason": decision_reason,
        },
    }


def local_runtime_env_map(openclaw_dir, config=None, metadata=None):
    runtime = load_local_runtime_config(openclaw_dir, config=config, metadata=metadata)
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    return {
        "MISSION_CONTROL_LOCAL_RUNTIME_ENABLED": "1" if runtime.get("enabled") else "0",
        "MISSION_CONTROL_LOCAL_RUNTIME_BACKEND": str(runtime.get("backend") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_ENTRYPOINT": str(runtime.get("entrypoint") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_MODEL": str(runtime.get("modelPath") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_HOST": str(runtime.get("host") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_PORT": str(runtime.get("port") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_CTX_SIZE": str(runtime.get("contextLength") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_GPU_LAYERS": str(runtime.get("gpuLayers") if runtime.get("gpuLayers") is not None else ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_KV_CACHE_ENABLED": "1" if kv_cache.get("enabled") else "0",
        "MISSION_CONTROL_LOCAL_RUNTIME_KV_CACHE_MODE": str(kv_cache.get("mode") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_CACHE_TYPE_K": str(kv_cache.get("keyType") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_CACHE_TYPE_V": str(kv_cache.get("valueType") or ""),
        "MISSION_CONTROL_LOCAL_RUNTIME_EXTRA_ARGS_JSON": json.dumps(runtime.get("extraArgs") if isinstance(runtime.get("extraArgs"), list) else [], ensure_ascii=False),
    }


OPENCLAW_COMMAND_ENV_KEYS = (
    "GATEWAY_AUTH_TOKEN",
    "DASHBOARD_AUTH_TOKEN",
    "MISSION_CONTROL_DATABASE_URL",
    "DATABASE_URL",
    "OPENCLAW_AUTH_SOURCE_FILE",
    "OPENCLAW_AUTH_PROFILES_JSON",
    "OPENCLAW_AUTH_PROFILES_B64",
    "ZAI_API_KEY",
    "BIGMODEL_API_KEY",
    "ZHIPUAI_API_KEY",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY",
    "GEMINI_API_KEY",
    "DEEPSEEK_API_KEY",
    "QWEN_API_KEY",
    "DASHSCOPE_API_KEY",
    "OPENROUTER_API_KEY",
    "XAI_API_KEY",
    "MINIMAX_API_KEY",
    "MINIMAX_CN_API_KEY",
)


def openclaw_command_env(openclaw_dir, include_local_runtime=True, metadata=None, env_file_values=None, provider_env_values=None):
    load_config(openclaw_dir)
    env = os.environ.copy()
    resolved_dir = str(Path(openclaw_dir).expanduser().resolve())
    env["OPENCLAW_STATE_DIR"] = resolved_dir
    env["OPENCLAW_CONFIG_PATH"] = str(Path(resolved_dir) / "openclaw.json")
    resolved_metadata = metadata if isinstance(metadata, dict) else None
    provider_env_values = provider_env_values if isinstance(provider_env_values, dict) else provider_config_env_map(openclaw_dir)
    for key, value in provider_env_values.items():
        if value and not env.get(key):
            env[key] = value
    env_file_values = env_file_values if isinstance(env_file_values, dict) else _read_openclaw_env_file_values(openclaw_dir)
    fallback_env_values = _read_host_openclaw_auth_fallback_values(openclaw_dir)
    for key in OPENCLAW_COMMAND_ENV_KEYS:
        if env.get(key):
            continue
        value = str(env_file_values.get(key) or fallback_env_values.get(key) or "").strip()
        if value:
            env[key] = value
    if not str(env.get("OPENCLAW_GATEWAY_TOKEN") or "").strip():
        gateway_token = str(env.get("GATEWAY_AUTH_TOKEN") or "").strip()
        if gateway_token:
            env["OPENCLAW_GATEWAY_TOKEN"] = gateway_token
    if include_local_runtime:
        if resolved_metadata is None:
            resolved_metadata = load_project_metadata(openclaw_dir)
        for key, value in local_runtime_env_map(openclaw_dir, metadata=resolved_metadata).items():
            if value or key.endswith("_ENABLED"):
                env[key] = value
    if resolved_metadata is None:
        resolved_metadata = load_project_metadata(openclaw_dir)
    path_prefixes = openclaw_command_bin_dirs(openclaw_dir, metadata=resolved_metadata)
    if path_prefixes:
        current_path = env.get("PATH", "")
        env["PATH"] = os.pathsep.join(path_prefixes + ([current_path] if current_path else []))
        cli_candidate = next((str(Path(item) / "openclaw") for item in path_prefixes if (Path(item) / "openclaw").exists()), "")
        if cli_candidate:
            env["OPENCLAW_BIN"] = cli_candidate
    return env


def openclaw_live_probe_env(openclaw_dir):
    return openclaw_command_env(openclaw_dir, include_local_runtime=False)


def merge_openclaw_panel_payload(base_payload, patch_payload):
    if not isinstance(base_payload, dict):
        return deepcopy(patch_payload) if isinstance(patch_payload, dict) else {}
    if not isinstance(patch_payload, dict):
        return deepcopy(base_payload)
    merged = deepcopy(base_payload)
    for key, value in patch_payload.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_openclaw_panel_payload(merged.get(key), value)
            continue
        merged[key] = deepcopy(value)
    return merged


def run_command(args, cwd=None, env=None, timeout=None, input_text=None):
    process = subprocess.run(
        [str(arg) for arg in args],
        capture_output=True,
        text=True,
        cwd=str(cwd) if cwd else None,
        env=env,
        input=input_text,
        check=False,
        timeout=timeout,
    )
    return process


def run_python_script(script_path, args, cwd=None, timeout=None):
    process = _run_command(["python3", str(script_path), *[str(arg) for arg in args]], cwd=cwd, timeout=timeout)
    output_parts = [part.strip() for part in (process.stdout, process.stderr) if part and part.strip()]
    return process, "\n".join(output_parts)


def resolve_openclaw_fetch_guard_module(openclaw_dir=""):
    candidate_strings = []
    env = openclaw_command_env(openclaw_dir) if openclaw_dir else os.environ.copy()
    explicit = str(env.get("OPENCLAW_FETCH_GUARD_MODULE") or "").strip()
    if explicit:
        candidate_strings.append(explicit)
    candidate_strings.extend(
        [
            "/usr/local/lib/node_modules/openclaw/dist/plugin-sdk/fetch-guard-RV5sCukz.js",
            "/opt/homebrew/lib/node_modules/openclaw/dist/plugin-sdk/fetch-guard-RV5sCukz.js",
        ]
    )
    npm_root_process = _run_command(["npm", "root", "-g"], env=env, timeout=5)
    if npm_root_process.returncode == 0:
        npm_root = str(npm_root_process.stdout or "").strip()
        if npm_root:
            candidate_strings.append(str(Path(npm_root) / "openclaw" / "dist" / "plugin-sdk" / "fetch-guard-RV5sCukz.js"))
    seen = set()
    for candidate_string in candidate_strings:
        if candidate_string in seen:
            continue
        seen.add(candidate_string)
        candidate = Path(candidate_string)
        if candidate.exists():
            return candidate.as_uri()
    return ""


def _urllib_http_request(url, method="GET", headers=None, data=None, timeout=8):
    request = Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return {
                "status": int(response.status),
                "body": response.read().decode("utf-8", "replace"),
                "headers": dict(response.headers.items()),
                "finalUrl": response.geturl(),
                "transport": "urllib",
            }
    except HTTPError as error:
        body = ""
        try:
            body = error.read().decode("utf-8", "replace")
        except Exception:
            body = ""
        return {
            "status": int(error.code),
            "body": body,
            "headers": dict(error.headers.items()) if error.headers else {},
            "finalUrl": getattr(error, "url", url),
            "transport": "urllib",
        }
    except URLError as error:
        raise RuntimeError(str(error.reason)) from error


def guarded_http_request(openclaw_dir, url, method="GET", headers=None, data=None, timeout=8, audit_context="dashboard-http"):
    env = openclaw_command_env(openclaw_dir) if openclaw_dir else os.environ.copy()
    module_url = _resolve_openclaw_fetch_guard_module(openclaw_dir)
    if not module_url:
        return _urllib_http_request(url, method=method, headers=headers, data=data, timeout=timeout)
    payload = {
        "url": str(url or "").strip(),
        "method": str(method or "GET").upper(),
        "headers": headers or {},
        "bodyB64": base64.b64encode(data).decode("ascii") if data else "",
        "timeoutMs": max(int(timeout or 0), 1) * 1000,
        "auditContext": str(audit_context or "dashboard-http").strip() or "dashboard-http",
        "useEnvProxy": any(
            str(env.get(key) or "").strip()
            for key in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy", "all_proxy")
        ),
    }
    script = """
const moduleUrl = process.env.OPENCLAW_FETCH_GUARD_MODULE_URL;
const raw = await new Promise((resolve, reject) => {
  let buffer = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => { buffer += chunk; });
  process.stdin.on("end", () => resolve(buffer));
  process.stdin.on("error", reject);
});
const payload = JSON.parse(raw || "{}");
try {
  const mod = await import(moduleUrl);
  const fetchWithSsrFGuard = mod.t;
  const withStrictGuardedFetchMode = mod.n;
  const withTrustedEnvProxyGuardedFetchMode = mod.r;
  const init = { method: payload.method || "GET", headers: payload.headers || {} };
  if (payload.bodyB64) {
    init.body = Buffer.from(payload.bodyB64, "base64");
  }
  const preset = {
    url: payload.url,
    init,
    timeoutMs: payload.timeoutMs || 8000,
    auditContext: payload.auditContext || "dashboard-http"
  };
  const params = payload.useEnvProxy
    ? withTrustedEnvProxyGuardedFetchMode(preset)
    : withStrictGuardedFetchMode(preset);
  const { response, finalUrl, release } = await fetchWithSsrFGuard(params);
  try {
    const body = await response.text();
    const headers = Object.fromEntries(response.headers.entries());
    process.stdout.write(JSON.stringify({
      status: response.status,
      body,
      headers,
      finalUrl,
      transport: "openclaw"
    }));
  } finally {
    await release();
  }
} catch (error) {
  process.stdout.write(JSON.stringify({
    transportError: true,
    name: error?.name || "Error",
    error: error?.message || String(error)
  }));
  process.exitCode = 1;
}
"""
    process = _run_command(
        ["node", "--input-type=module", "-e", script],
        env={**env, "OPENCLAW_FETCH_GUARD_MODULE_URL": module_url},
        timeout=max(int(timeout or 0) + 5, 10),
        input_text=json.dumps(payload, ensure_ascii=False),
    )
    stdout = str(process.stdout or "").strip()
    if process.returncode == 0 and stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict) and not parsed.get("transportError"):
            return parsed
    if stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict) and parsed.get("transportError"):
            raise RuntimeError(str(parsed.get("error") or "guarded fetch failed"))
    stderr_text = str(process.stderr or "").strip()
    if stderr_text:
        raise RuntimeError(stderr_text)
    return _urllib_http_request(url, method=method, headers=headers, data=data, timeout=timeout)


def fallback_openclaw_installation_payload(openclaw_dir, error=""):
    managed_root = Path(openclaw_dir).expanduser().resolve() / ".runtime" / "openclaw-managed"
    return {
        "ok": False,
        "state": "missing",
        "installed": False,
        "method": "missing",
        "label": "Not installed",
        "cliPath": "",
        "managed": False,
        "version": {"raw": "", "release": "", "build": "", "ok": False},
        "availableMethods": [],
        "recommendedMethod": "",
        "managedRoot": str(managed_root),
        "managedBinDir": str(managed_root / "bin"),
        "wrapperPath": str(Path(openclaw_dir).expanduser().resolve() / "bin" / "openclaw"),
        "lastInstalledAt": "",
        "lastUpdatedAt": "",
        "installerTool": "",
        "error": str(error or "").strip(),
    }


def openclaw_agent_auth_targets(openclaw_dir, config=None):
    resolved_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config or load_config(openclaw_dir)
    targets = []
    for agent in safe_list(((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list")):
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        agent_dir_raw = str(agent.get("agentDir") or "").strip()
        agent_dir = Path(agent_dir_raw).expanduser() if agent_dir_raw else (resolved_dir / "agents" / agent_id / "agent")
        if agent_dir.name != "agent":
            agent_dir = agent_dir / "agent"
        targets.append({"agentId": agent_id, "path": agent_dir / "auth-profiles.json"})
    return targets


def load_openclaw_agent_auth_payload(openclaw_dir, config=None):
    resolved_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config or load_config(openclaw_dir)
    targets = openclaw_agent_auth_targets(openclaw_dir, config=local_config)
    ready_targets = []
    providers = set()
    for item in targets:
        path = item.get("path")
        if not isinstance(path, Path) or not path.exists():
            continue
        ready_targets.append(item)
        if providers:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for profile in safe_list((payload or {}).get("profiles")):
            if isinstance(profile, dict):
                provider = str(profile.get("provider") or "").strip()
                if provider:
                    providers.add(provider)
        if not providers and isinstance((payload or {}).get("profiles"), dict):
            for profile in (payload.get("profiles") or {}).values():
                if isinstance(profile, dict):
                    provider = str(profile.get("provider") or "").strip()
                    if provider:
                        providers.add(provider)
    ready_count = len(ready_targets)
    total_count = len(targets)
    missing_count = max(total_count - ready_count, 0)
    state = "missing"
    if total_count > 0 and ready_count == total_count:
        state = "ready"
    elif ready_count > 0:
        state = "partial"
    try:
        script_path = str(openclaw_auth_sync_script_path(openclaw_dir))
    except FileNotFoundError:
        script_path = ""
    error = ""
    if state == "missing":
        error = "未检测到 agent auth-profiles.json；团队通信和任务执行会失败。"
    elif state == "partial":
        error = "部分 agent 缺少 auth-profiles.json，跨团队协作会出现选择性掉线。"
    return {
        "ok": state == "ready" and total_count > 0,
        "state": state,
        "label": {"ready": "Ready", "partial": "Partial", "missing": "Missing"}.get(state, "Missing"),
        "targetCount": total_count,
        "readyCount": ready_count,
        "missingCount": missing_count,
        "providers": sorted(providers),
        "scriptPath": script_path,
        "samplePath": str((ready_targets[0] or {}).get("path")) if ready_targets else "",
        "error": error,
        "managed": str(resolved_dir / "agents"),
    }


def load_runtime_sync_payload(openclaw_dir, config=None, metadata=None):
    resolved_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config or load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    runtime_meta = local_metadata.get("runtimeSync") if isinstance(local_metadata.get("runtimeSync"), dict) else {}
    project_dir = str(local_metadata.get("projectDir") or "").strip()
    if not project_dir:
        resolved_project_dir = resolve_project_dir(openclaw_dir, config=local_config)
        project_dir = str(resolved_project_dir or "").strip()
    router_agent_id = get_router_agent_id(local_config)
    workspace_total = 0
    workspace_ready = 0
    for agent in safe_list(((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list")):
        if not isinstance(agent, dict):
            continue
        workspace = str(agent.get("workspace") or "").strip()
        if not workspace:
            continue
        workspace_total += 1
        scripts_dir = Path(workspace).expanduser().resolve() / "scripts"
        if (
            (scripts_dir / "collaboration_dashboard.py").exists()
            and (scripts_dir / "env_utils.py").exists()
            and (scripts_dir / "backend" / "__init__.py").exists()
            and (scripts_dir / "backend" / "application" / "services" / "runtime_core.py").exists()
            and (scripts_dir / "backend" / "application" / "services" / "memory_core.py").exists()
            and (scripts_dir / "backend" / "application" / "services" / "dashboard_core.py").exists()
            and (scripts_dir / "backend" / "adapters" / "storage" / "dashboard.py").exists()
            and (scripts_dir / "backend" / "adapters" / "integrations" / "openclaw.py").exists()
            and (scripts_dir / "backend" / "adapters" / "integrations" / "wechat.py").exists()
            and (scripts_dir / "backend" / "presentation" / "http" / "http.py").exists()
            and (scripts_dir / "backend" / "presentation" / "http" / "task.py").exists()
        ):
            workspace_ready += 1
    skills_dir = resolved_dir / "skills"
    skills_count = len([item for item in skills_dir.iterdir() if item.is_dir()]) if skills_dir.exists() else 0
    frontend_dist_path = Path(project_dir) / "apps" / "frontend" / "dist" / "index.html" if project_dir else None
    frontend_built = bool(frontend_dist_path and frontend_dist_path.exists())
    auth_payload = load_openclaw_agent_auth_payload(openclaw_dir, config=local_config)
    local_runtime_payload = load_local_runtime_payload(openclaw_dir, config=local_config, metadata=local_metadata)
    last_synced_at = str(runtime_meta.get("lastSyncedAt") or "").strip()
    last_synced = parse_iso(last_synced_at)
    script_path = ""
    script_error = ""
    try:
        script_path = str(runtime_sync_script_path(openclaw_dir))
    except FileNotFoundError as error:
        script_error = str(error)
    coverage_ok = workspace_total > 0 and workspace_ready == workspace_total
    skills_ok = skills_count > 0
    state = "ready" if coverage_ok and skills_ok and auth_payload.get("ok") and project_dir and (frontend_built or not runtime_meta.get("buildFrontend")) else "pending"
    if script_error:
        state = "error"
    elif auth_payload.get("state") == "partial":
        state = "partial"
    elif auth_payload.get("state") == "missing" and last_synced_at:
        state = "partial"
    if last_synced_at and state == "pending":
        state = "partial"
    return {
        "ok": state == "ready" and not script_error,
        "state": state,
        "label": {"ready": "Ready", "partial": "Partial", "pending": "Pending", "error": "Error"}.get(state, "Pending"),
        "projectDir": project_dir,
        "scriptPath": script_path,
        "routerAgentId": router_agent_id,
        "lastSyncedAt": last_synced_at,
        "lastSyncedAgo": format_age(last_synced, now_utc()) if last_synced else "",
        "buildFrontend": bool(runtime_meta.get("buildFrontend", True)),
        "workspaceCoverage": {
            "ready": workspace_ready,
            "total": workspace_total,
            "ok": coverage_ok,
        },
        "skills": {
            "count": skills_count,
            "ok": skills_ok,
            "path": str(skills_dir),
        },
        "frontend": {
            "built": frontend_built,
            "path": str(frontend_dist_path) if frontend_dist_path else "",
        },
        "auth": auth_payload,
        "localRuntime": local_runtime_payload,
        "error": script_error or auth_payload.get("error") or str(runtime_meta.get("error") or "").strip(),
        "lastOutput": str(runtime_meta.get("lastOutput") or "").strip(),
    }


def load_openclaw_installation_payload(openclaw_dir):
    load_config(openclaw_dir)
    try:
        installer_script = openclaw_installer_script_path(openclaw_dir)
    except FileNotFoundError as error:
        return fallback_openclaw_installation_payload(openclaw_dir, error=str(error))
    try:
        process, output = run_python_script(
            installer_script,
            ["detect", "--dir", str(Path(openclaw_dir).expanduser().resolve()), "--json"],
            timeout=3,
        )
    except subprocess.TimeoutExpired:
        return fallback_openclaw_installation_payload(openclaw_dir, error="installer_detect_timeout")
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if isinstance(payload, dict):
        return payload
    return fallback_openclaw_installation_payload(openclaw_dir, error=output or "installer_detect_failed")


def load_openclaw_dashboard_summary(openclaw_dir, config=None, metadata=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    installation_payload = _load_openclaw_installation_payload(openclaw_dir)
    runtime_sync_payload = load_runtime_sync_payload(openclaw_dir, config=local_config, metadata=metadata)
    local_runtime_payload = load_local_runtime_payload(
        openclaw_dir,
        config=local_config,
        metadata=metadata,
        include_capabilities=False,
    )
    topology = _build_openclaw_model_topology(openclaw_dir, config=local_config)
    execution_architecture_payload = load_model_execution_architecture_payload(
        openclaw_dir,
        config=local_config,
        metadata=metadata,
        include_local_runtime_capabilities=False,
        include_model_access_matrix=False,
        local_runtime_payload=local_runtime_payload,
        topology=topology,
    )
    runtime_overview_payload = load_openclaw_runtime_overview_panel_data(
        openclaw_dir,
        config=local_config,
        metadata=metadata,
    )
    approvals_path = openclaw_dir / "exec-approvals.json"
    approvals_exists = approvals_path.exists()
    memory_defaults = (
        ((local_config.get("agents") or {}) if isinstance(local_config, dict) else {}).get("defaults")
        if isinstance((local_config.get("agents") or {}) if isinstance(local_config, dict) else {}, dict)
        else {}
    )
    memory_search = memory_defaults.get("memorySearch") if isinstance(memory_defaults, dict) else {}
    memory_provider = str((memory_search.get("provider") if isinstance(memory_search, dict) else "") or "").strip()
    memory_enabled = bool((memory_search.get("enabled") if isinstance(memory_search, dict) else False))
    configured_agents = safe_list(((local_config.get("agents") or {}) if isinstance(local_config, dict) else {}).get("list"))
    mcp_entries = (local_config.get("mcp") if isinstance(local_config, dict) and isinstance(local_config.get("mcp"), dict) else {}) or {}
    version_payload = installation_payload.get("version") if isinstance(installation_payload, dict) else {}
    if not isinstance(version_payload, dict):
        version_payload = {"raw": str(version_payload or "").strip(), "release": "", "build": ""}
    return {
        "supported": bool((installation_payload or {}).get("supported", True)),
        "deferred": True,
        "error": "",
        "version": {
            "raw": str(version_payload.get("raw") or "").strip(),
            "release": str(version_payload.get("release") or "").strip(),
            "build": str(version_payload.get("build") or "").strip(),
        },
        "installation": installation_payload,
        "runtimeSync": runtime_sync_payload,
        "localRuntime": local_runtime_payload,
        "executionArchitecture": execution_architecture_payload,
        "runtimeOverview": runtime_overview_payload.get("runtimeOverview") if isinstance(runtime_overview_payload.get("runtimeOverview"), dict) else {},
        "config": {"valid": True, "path": str(Path(openclaw_dir).expanduser().resolve() / "openclaw.json"), "deferred": True},
        "gateway": {
            "ok": False,
            "agentCount": 0,
            "defaultAgentId": "",
            "channels": [],
            "error": "",
            "rpc": {"ok": False, "url": "", "error": "", "serviceStatus": "", "bindMode": "", "port": None, "probeUrl": ""},
        },
        "browser": {
            "ok": False,
            "running": False,
            "profile": "",
            "targets": 0,
            "error": "",
            "profiles": [],
            "recommendedProfiles": [],
        },
        "nativeSkills": {
            "total": 0,
            "eligible": 0,
            "disabled": 0,
            "blocked": 0,
            "bundled": 0,
            "external": 0,
            "managedSkillsDir": str(Path(openclaw_dir).expanduser().resolve() / "skills"),
            "workspaceDir": str(Path(openclaw_dir).expanduser().resolve()),
            "sampleEligible": [],
            "sampleMissing": [],
            "missingBins": [],
            "missingEnv": [],
            "missingConfig": [],
            "sourceBreakdown": [],
            "warnings": [],
            "check": {"summary": {}, "missingRequirements": [], "warnings": []},
        },
        "approvals": {
            "ok": approvals_exists,
            "exists": approvals_exists,
            "path": str(approvals_path),
            "socketPath": str(openclaw_dir / "exec-approvals.sock"),
            "agentCount": len(configured_agents) if approvals_exists else 0,
            "ruleCount": 0,
            "defaultRuleCount": 0,
            "agents": [],
            "deferred": True,
            "error": "" if approvals_exists else "deferred",
        },
        "memory": {
            "ok": False,
            "agentCount": len(configured_agents) if memory_enabled else 0,
            "readyCount": 0,
            "providers": [{"name": memory_provider, "count": len(configured_agents)}] if memory_provider else [],
            "entries": [],
            "deferred": True,
            "error": "" if memory_enabled else "deferred",
        },
        "mcp": {
            "ok": False,
            "serverCount": len(mcp_entries),
            "enabledCount": sum(1 for item in mcp_entries.values() if not isinstance(item, dict) or item.get("enabled") is not False),
            "entries": [
                {
                    "name": str(name or "").strip(),
                    "enabled": not isinstance(item, dict) or item.get("enabled") is not False,
                    "transport": str((item.get("transport") if isinstance(item, dict) else "") or "").strip(),
                    "toolCount": 0,
                }
                for name, item in list(mcp_entries.items())[:12]
                if str(name or "").strip()
            ],
            "deferred": True,
            "error": "deferred",
        },
        "tools": {
            "ok": False,
            "totalCount": 0,
            "browserReady": False,
            "browserProfile": "",
            "browserSurfaceCount": 0,
            "skillCount": 0,
            "mcpToolCount": 0,
            "serverCount": 0,
            "entryCount": 0,
            "entries": [],
            "deferred": True,
            "error": "deferred",
        },
    }


def load_openclaw_models_panel_data(openclaw_dir, config=None, metadata=None):
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    topology = _build_openclaw_model_topology(openclaw_dir, config=config)
    execution_architecture_payload = load_model_execution_architecture_payload(
        openclaw_dir,
        config=config,
        metadata=metadata,
        include_local_runtime_capabilities=False,
        include_model_access_matrix=True,
        topology=topology,
    )
    return {
        "deferred": False,
        "error": "",
        "models": {
            "summary": deepcopy(topology.get("modelSummary") if isinstance(topology.get("modelSummary"), dict) else {}),
            "providers": deepcopy(topology.get("providerRows") if isinstance(topology.get("providerRows"), list) else []),
            "assignments": deepcopy(topology.get("providerAssignments") if isinstance(topology.get("providerAssignments"), list) else []),
            "catalog": [
                *deepcopy(topology.get("modelInventory") if isinstance(topology.get("modelInventory"), list) else []),
                *[
                    {"name": model_name, "provider": infer_model_provider(model_name).get("label", "兼容 / 自定义"), "providerId": infer_model_provider(model_name).get("id", "custom"), "agentCount": 0, "agents": []}
                    for model_name in _model_provider_starter_models()
                    if model_name not in {
                        str((item or {}).get("name") or "").strip()
                        for item in deepcopy(topology.get("modelInventory") if isinstance(topology.get("modelInventory"), list) else [])
                        if isinstance(item, dict)
                    }
                ],
            ],
        },
        "executionArchitecture": execution_architecture_payload,
    }

def _build_openclaw_agent_params(config):
    local_config = config if isinstance(config, dict) else {}
    config_agents = ((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list", [])
    agent_params = []
    for agent in config_agents if isinstance(config_agents, list) else []:
        if not isinstance(agent, dict):
            continue
        params = agent.get("params", {})
        if not params:
            params = {}
        agent_params.append(
            {
                "id": agent.get("id", ""),
                "workspace": agent.get("workspace", ""),
                "params": params,
                "summary": ", ".join(f"{key}={value}" for key, value in list(params.items())[:5]) if params else "",
            }
        )
    return agent_params


def _build_openclaw_native_skills_payload(native_skills_payload, skills_result, skills_check_payload, skills_check_result):
    native_skill_entries = native_skills_payload.get("skills", []) if isinstance(native_skills_payload, dict) else []
    managed_skills_dir = native_skills_payload.get("managedSkillsDir", "") if isinstance(native_skills_payload, dict) else ""
    workspace_dir = native_skills_payload.get("workspaceDir", "") if isinstance(native_skills_payload, dict) else ""

    source_counter = Counter(item.get("source", "unknown") for item in native_skill_entries)
    missing_bins = Counter()
    missing_env = Counter()
    missing_config = Counter()
    sample_eligible = []
    sample_missing = []
    bundled = 0
    external = 0
    disabled = 0
    blocked = 0
    eligible = 0
    for item in native_skill_entries:
        if item.get("bundled"):
            bundled += 1
        else:
            external += 1
        if item.get("disabled"):
            disabled += 1
        if item.get("blockedByAllowlist"):
            blocked += 1
        if item.get("eligible"):
            eligible += 1
            if len(sample_eligible) < 8:
                sample_eligible.append(
                    {
                        "title": item.get("name", "unknown"),
                        "meta": f"{item.get('source', 'unknown')} · {'bundled' if item.get('bundled') else 'external'}",
                        "detail": item.get("description", ""),
                    }
                )
        missing = item.get("missing", {}) if isinstance(item.get("missing"), dict) else {}
        for bin_name in missing.get("bins", []) or []:
            missing_bins[bin_name] += 1
        for env_name in missing.get("env", []) or []:
            missing_env[env_name] += 1
        for config_name in missing.get("config", []) or []:
            missing_config[config_name] += 1
        if not item.get("eligible") and len(sample_missing) < 8:
            reasons = []
            if missing.get("bins"):
                reasons.append(f"缺少命令: {', '.join(missing.get('bins', [])[:2])}")
            if missing.get("env"):
                reasons.append(f"缺少环境变量: {', '.join(missing.get('env', [])[:2])}")
            if missing.get("config"):
                reasons.append(f"缺少配置: {', '.join(missing.get('config', [])[:2])}")
            sample_missing.append(
                {
                    "title": item.get("name", "unknown"),
                    "meta": item.get("source", "unknown"),
                    "detail": " · ".join(reasons) or item.get("description", ""),
                }
            )

    return {
        "total": len(native_skill_entries),
        "eligible": eligible,
        "disabled": disabled,
        "blocked": blocked,
        "bundled": bundled,
        "external": external,
        "managedSkillsDir": managed_skills_dir,
        "workspaceDir": workspace_dir,
        "sampleEligible": sample_eligible,
        "sampleMissing": sample_missing,
        "missingBins": top_counter_items(missing_bins),
        "missingEnv": top_counter_items(missing_env),
        "missingConfig": top_counter_items(missing_config),
        "sourceBreakdown": top_counter_items(source_counter),
        "warnings": [line.strip() for line in (skills_result.stderr or "").splitlines() if line.strip()],
        "check": {
            "summary": skills_check_payload.get("summary", {}),
            "missingRequirements": skills_check_payload.get("missingRequirements", []),
            "warnings": [line.strip() for line in (skills_check_result.stderr or "").splitlines() if line.strip()],
        },
    }


def _fallback_openclaw_skills_check_panel_data(openclaw_dir, error_message="未检测到 openclaw CLI。"):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    return {
        "deferred": False,
        "error": error_message,
        "nativeSkills": {
            "total": 0,
            "eligible": 0,
            "disabled": 0,
            "blocked": 0,
            "bundled": 0,
            "external": 0,
            "managedSkillsDir": str(openclaw_dir / "skills"),
            "workspaceDir": str(openclaw_dir),
            "sampleEligible": [],
            "sampleMissing": [],
            "missingBins": [],
            "missingEnv": [],
            "missingConfig": [],
            "sourceBreakdown": [],
            "warnings": [],
            "check": {"summary": {}, "missingRequirements": [], "warnings": []},
        },
    }


def load_openclaw_skills_check_panel_data(openclaw_dir, config=None, metadata=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()

    def build():
        env = openclaw_command_env(openclaw_dir)
        skills_result = _run_command(["openclaw", "skills", "list", "--json"], env=env)
        native_skills_payload = parse_json_payload(skills_result.stdout, skills_result.stderr, default={"skills": []})
        if not isinstance(native_skills_payload, dict):
            native_skills_payload = {"skills": []}
        skills_check_result = _run_command(["openclaw", "skills", "check", "--json"], env=env)
        skills_check_payload = parse_json_payload(
            skills_check_result.stdout,
            skills_check_result.stderr,
            default={"summary": {}, "missingRequirements": []},
        )
        if not isinstance(skills_check_payload, dict):
            skills_check_payload = {"summary": {}, "missingRequirements": []}
        return {
            "nativeSkills": _build_openclaw_native_skills_payload(
                native_skills_payload,
                skills_result,
                skills_check_payload,
                skills_check_result,
            ),
        }

    try:
        payload = cached_payload(("openclaw-skills-check-panel", str(openclaw_dir)), 15.0, build)
    except FileNotFoundError:
        return _fallback_openclaw_skills_check_panel_data(openclaw_dir)
    return {
        "deferred": False,
        "error": "",
        "nativeSkills": deepcopy(payload.get("nativeSkills") if isinstance(payload.get("nativeSkills"), dict) else {}),
    }


def load_openclaw_agent_params_panel_data(openclaw_dir, config=None, metadata=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()

    def build():
        local_config = config or load_config(openclaw_dir)
        return {
            "agentParams": _build_openclaw_agent_params(local_config),
        }

    payload = cached_payload(("openclaw-agent-params-panel", str(openclaw_dir)), 15.0, build)
    return {
        "deferred": False,
        "error": "",
        "agentParams": deepcopy(payload.get("agentParams") if isinstance(payload.get("agentParams"), list) else []),
    }


OPENCLAW_CHANNEL_TITLES = {
    "telegram": "Telegram",
    "feishu": "Feishu",
    "qqbot": "QQBot",
}


def _build_openclaw_gateway_summary_payload(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    gateway_config = local_config.get("gateway") if isinstance(local_config.get("gateway"), dict) else {}
    channels_config = local_config.get("channels") if isinstance(local_config.get("channels"), dict) else {}
    port = gateway_config.get("port")
    bind_mode = str(gateway_config.get("bind") or gateway_config.get("mode") or "").strip()
    probe_url = f"http://127.0.0.1:{port}" if port else ""
    channel_entries = []
    configured_count = 0
    enabled_count = 0
    for channel_id, title in OPENCLAW_CHANNEL_TITLES.items():
        channel_config = channels_config.get(channel_id) if isinstance(channels_config.get(channel_id), dict) else {}
        enabled = bool(channel_config.get("enabled"))
        configured = bool(channel_config)
        detail = ""
        for candidate in (
            channel_config.get("appId"),
            channel_config.get("domain"),
            channel_config.get("groupPolicy"),
            channel_config.get("dmPolicy"),
        ):
            normalized = str(candidate or "").strip()
            if normalized:
                detail = normalized
                break
        if configured:
            configured_count += 1
        if enabled:
            enabled_count += 1
        channel_entries.append(
            {
                "id": channel_id,
                "title": title,
                "meta": "enabled" if enabled else ("configured" if configured else "not configured"),
                "detail": detail or ("Configured in openclaw.json" if configured else "No channel config"),
                "healthy": False,
                "running": False,
                "configured": configured,
                "enabled": enabled,
                "source": "config",
            }
        )
    return {
        "deferred": False,
        "error": "",
        "gateway": {
            "ok": False,
            "agentCount": 0,
            "defaultAgentId": "",
            "channels": channel_entries,
            "channelSummary": {
                "total": len(channel_entries),
                "configured": configured_count,
                "enabled": enabled_count,
            },
            "error": "",
            "liveDiagnosticsLoaded": False,
            "probeState": "summary",
            "probeUpdatedAt": "",
            "rpc": {
                "ok": False,
                "url": "",
                "error": "",
                "serviceStatus": "not_probed",
                "bindMode": bind_mode,
                "port": port,
                "probeUrl": probe_url,
            },
        },
    }


def _build_openclaw_browser_summary_payload(openclaw_dir, config=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    browser_config = local_config.get("browser") if isinstance(local_config.get("browser"), dict) else {}
    profiles_config = browser_config.get("profiles") if isinstance(browser_config.get("profiles"), dict) else {}
    normalized_profiles = []
    profile_names = set()
    for name, payload in profiles_config.items():
        normalized_name = str(name or "").strip()
        if not normalized_name:
            continue
        payload = payload if isinstance(payload, dict) else {}
        profile_names.add(normalized_name)
        detail = ""
        if payload.get("cdpUrl"):
            detail = str(payload.get("cdpUrl") or "").strip()
        elif payload.get("cdpPort"):
            detail = f"cdp:{payload.get('cdpPort')}"
        normalized_profiles.append(
            {
                "name": normalized_name,
                "running": False,
                "detail": detail,
                "source": "config",
            }
        )
    recommended_profiles = [
        {
            "name": "openclaw",
            "title": "openclaw",
            "detail": "产品内置的无头浏览器 profile，适合容器和默认安装场景，开箱即可直接打开网页。",
            "available": "openclaw" in profile_names,
        },
        {
            "name": "user",
            "title": "user",
            "detail": "复用用户已登录的本地 Chrome / Chromium 会话，适合真实业务站点联调。",
            "available": "user" in profile_names,
        },
    ]
    default_profile = str(browser_config.get("defaultProfile") or "").strip()
    return {
        "deferred": False,
        "error": "",
        "browser": {
            "ok": False,
            "running": False,
            "enabled": bool(browser_config.get("enabled", True)),
            "profile": default_profile,
            "targets": 0,
            "error": "",
            "profiles": normalized_profiles,
            "recommendedProfiles": recommended_profiles,
            "liveDiagnosticsLoaded": False,
            "probeState": "summary",
            "probeUpdatedAt": "",
        },
    }


def load_openclaw_gateway_panel_data(openclaw_dir, config=None, metadata=None):
    return cached_payload(
        ("openclaw-gateway-summary-v2", str(Path(openclaw_dir).expanduser().resolve())),
        30.0,
        lambda: _build_openclaw_gateway_summary_payload(openclaw_dir, config=config),
    )


def load_openclaw_gateway_health_panel_data(openclaw_dir, config=None, metadata=None, env=None):
    try:
        env = env if isinstance(env, dict) else openclaw_live_probe_env(openclaw_dir)
        health_result = _run_command(["openclaw", "gateway", "health", "--json"], env=env)
    except FileNotFoundError:
        return {
            "deferred": False,
            "error": "未检测到 openclaw CLI。",
            "gateway": {
                "error": "missing_cli",
                "probeState": "error",
                "probeUpdatedAt": now_iso(),
                "liveDiagnosticsLoaded": True,
            },
        }

    health_payload = parse_json_payload(health_result.stdout, health_result.stderr, default=None)
    if health_payload is None:
        health_payload = {
            "ok": False,
            "error": (health_result.stderr or health_result.stdout or "gateway_health_failed").strip(),
            "channels": {},
            "agents": [],
        }

    channel_entries = []
    health_channels = health_payload.get("channels", {}) if isinstance(health_payload, dict) else {}
    channel_order = health_payload.get("channelOrder", []) if isinstance(health_payload, dict) else []
    health_labels = health_payload.get("channelLabels", {}) if isinstance(health_payload, dict) else {}
    ordered_names = channel_order or list(health_channels.keys())
    for channel_name in ordered_names:
        channel = health_channels.get(channel_name, {})
        probe_payload = channel.get("probe", {}) if isinstance(channel.get("probe"), dict) else {}
        detail = ""
        if channel_name == "telegram" and isinstance(probe_payload.get("bot"), dict):
            detail = probe_payload["bot"].get("username", "")
        elif channel_name == "feishu":
            detail = probe_payload.get("appId", "")
        channel_entries.append(
            {
                "title": health_labels.get(channel_name, channel_name),
                "meta": "configured" if channel.get("configured") else "not configured",
                "detail": detail or channel.get("lastError") or "无额外信息",
                "healthy": bool(probe_payload.get("ok")) if probe_payload else bool(channel.get("configured")),
                "running": bool(channel.get("running")),
            }
        )

    error = str(health_payload.get("error") or "").strip()
    return {
        "deferred": False,
        "error": error,
        "gateway": {
            "ok": bool(health_payload.get("ok")),
            "durationMs": health_payload.get("durationMs"),
            "defaultAgentId": health_payload.get("defaultAgentId", ""),
            "agentCount": len(health_payload.get("agents", []) or []),
            "channels": channel_entries,
            "channelSummary": {
                "total": len(channel_entries),
                "configured": len([item for item in channel_entries if item.get("meta") != "not configured"]),
                "enabled": len([item for item in channel_entries if item.get("meta") == "configured"]),
            },
            "error": error,
            "liveDiagnosticsLoaded": True,
            "probeState": "ready" if not error else "error",
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_gateway_rpc_panel_data(openclaw_dir, config=None, metadata=None, env=None):
    try:
        env = env if isinstance(env, dict) else openclaw_live_probe_env(openclaw_dir)
        gateway_status_result = _run_command(["openclaw", "gateway", "status", "--require-rpc", "--json"], env=env)
    except FileNotFoundError:
        return {
            "deferred": False,
            "error": "未检测到 openclaw CLI。",
            "gateway": {
                "error": "missing_cli",
                "probeState": "error",
                "probeUpdatedAt": now_iso(),
                "liveDiagnosticsLoaded": True,
                "rpc": {"ok": False, "url": "", "error": "missing_cli", "serviceStatus": "", "bindMode": "", "port": None, "probeUrl": ""},
            },
        }

    gateway_status_payload = parse_json_payload(gateway_status_result.stdout, gateway_status_result.stderr, default=None)
    if gateway_status_payload is None:
        gateway_status_payload = {
            "service": {"runtime": {"status": "unknown"}},
            "gateway": {"bindMode": "", "port": None, "probeUrl": ""},
            "rpc": {
                "ok": False,
                "error": (gateway_status_result.stderr or gateway_status_result.stdout or "gateway_status_failed").strip(),
                "url": "",
            },
            "config": {},
        }
    rpc_payload = gateway_status_payload.get("rpc", {}) if isinstance(gateway_status_payload, dict) else {}
    service_payload = gateway_status_payload.get("service", {}) if isinstance(gateway_status_payload, dict) else {}
    gateway_runtime_payload = gateway_status_payload.get("gateway", {}) if isinstance(gateway_status_payload, dict) else {}
    error = str(rpc_payload.get("error") or "").strip()
    return {
        "deferred": False,
        "error": error,
        "gateway": {
            "error": error,
            "liveDiagnosticsLoaded": True,
            "probeState": "ready" if not error else "error",
            "probeUpdatedAt": now_iso(),
            "rpc": {
                "ok": bool(rpc_payload.get("ok")),
                "url": rpc_payload.get("url", ""),
                "error": error,
                "serviceStatus": (((service_payload.get("runtime", {}) if isinstance(service_payload.get("runtime"), dict) else {}) or {}).get("status", "")),
                "bindMode": gateway_runtime_payload.get("bindMode", ""),
                "port": gateway_runtime_payload.get("port"),
                "probeUrl": gateway_runtime_payload.get("probeUrl", ""),
            },
        },
    }


def load_openclaw_gateway_live_panel_data(openclaw_dir, config=None, metadata=None):
    env = openclaw_live_probe_env(openclaw_dir)
    with ThreadPoolExecutor(max_workers=2) as executor:
        health_future = executor.submit(load_openclaw_gateway_health_panel_data, openclaw_dir, config, metadata, env)
        rpc_future = executor.submit(load_openclaw_gateway_rpc_panel_data, openclaw_dir, config, metadata, env)
        health_payload = health_future.result()
        rpc_payload = rpc_future.result()
    return merge_openclaw_panel_payload(health_payload, rpc_payload)


def perform_openclaw_gateway_probe_refresh(openclaw_dir, config=None, metadata=None):
    return load_openclaw_gateway_live_panel_data(
        openclaw_dir,
        config=config,
        metadata=metadata,
    )


def load_openclaw_browser_panel_data(openclaw_dir, config=None, metadata=None):
    return cached_payload(
        ("openclaw-browser-summary-v2", str(Path(openclaw_dir).expanduser().resolve())),
        30.0,
        lambda: _build_openclaw_browser_summary_payload(openclaw_dir, config=config),
    )


def load_openclaw_browser_profiles_panel_data(openclaw_dir, config=None, metadata=None, env=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    try:
        env = env if isinstance(env, dict) else openclaw_live_probe_env(openclaw_dir)
        browser_profiles_payload, _browser_profiles_result = load_browser_profiles_payload(openclaw_dir, env=env)
    except FileNotFoundError:
        return {
            "deferred": False,
            "error": "未检测到 openclaw CLI。",
            "browser": {
                "error": "missing_cli",
                "profiles": [],
                "recommendedProfiles": [],
            },
        }

    browser_profiles = browser_profiles_payload.get("profiles", []) if isinstance(browser_profiles_payload, dict) else []
    normalized_browser_profiles = []
    browser_profile_names = set()
    for item in browser_profiles:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("profile") or item.get("id") or "").strip()
        if not name:
            continue
        browser_profile_names.add(name)
        normalized_browser_profiles.append(
            {
                "name": name,
                "running": bool(item.get("running")),
                "detail": item.get("description") or item.get("path") or item.get("label") or "",
            }
        )
    recommended_profiles = [
        {
            "name": "openclaw",
            "title": "openclaw",
            "detail": "产品内置的无头浏览器 profile，适合容器和默认安装场景，开箱即可直接打开网页。",
            "available": "openclaw" in browser_profile_names,
        },
        {
            "name": "user",
            "title": "user",
            "detail": "复用用户已登录的本地 Chrome / Chromium 会话，适合真实业务站点联调。",
            "available": "user" in browser_profile_names,
        },
    ]
    return {
        "deferred": False,
        "error": "",
        "browser": {
            "profiles": normalized_browser_profiles,
            "recommendedProfiles": recommended_profiles,
            "profile": configured_browser_default_profile(local_config),
        },
    }


def load_openclaw_browser_status_panel_data(openclaw_dir, config=None, metadata=None, env=None):
    try:
        env = env if isinstance(env, dict) else openclaw_live_probe_env(openclaw_dir)
        browser_status_payload = load_browser_status_payload(
            openclaw_dir,
            env=env,
            config=config,
        )
    except FileNotFoundError:
        return {
            "deferred": False,
            "error": "未检测到 openclaw CLI。",
            "browser": {
                "ok": False,
                "running": False,
                "profile": "",
                "targets": 0,
                "error": "missing_cli",
                "liveDiagnosticsLoaded": True,
                "probeState": "error",
                "probeUpdatedAt": now_iso(),
            },
        }
    error = str(browser_status_payload.get("error") or "").strip()
    return {
        "deferred": False,
        "error": error,
        "browser": {
            **browser_status_payload,
            "liveDiagnosticsLoaded": True,
            "probeState": "ready" if not error else "error",
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_browser_live_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    env = openclaw_live_probe_env(openclaw_dir)
    with ThreadPoolExecutor(max_workers=2) as executor:
        profiles_future = executor.submit(load_openclaw_browser_profiles_panel_data, openclaw_dir, local_config, metadata, env)
        status_future = executor.submit(load_openclaw_browser_status_panel_data, openclaw_dir, local_config, metadata, env)
        profiles_payload = profiles_future.result()
        status_payload = status_future.result()
    return merge_openclaw_panel_payload(profiles_payload, status_payload)


def perform_openclaw_browser_probe_refresh(openclaw_dir, config=None, metadata=None):
    return load_openclaw_browser_live_panel_data(
        openclaw_dir,
        config=config,
        metadata=metadata,
    )


def load_openclaw_local_runtime_panel_data(openclaw_dir, config=None, metadata=None):
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    local_runtime_payload = load_local_runtime_payload(
        openclaw_dir,
        config=config,
        metadata=metadata,
        include_capabilities=True,
    )
    execution_architecture_payload = load_model_execution_architecture_payload(
        openclaw_dir,
        config=config,
        metadata=metadata,
        include_local_runtime_capabilities=True,
        include_model_access_matrix=False,
        local_runtime_payload=local_runtime_payload,
    )
    return {
        "deferred": False,
        "error": str(local_runtime_payload.get("launcherError") or "").strip(),
        "localRuntime": local_runtime_payload,
        "executionArchitecture": execution_architecture_payload,
    }


def load_openclaw_runtime_overview_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    project_dir = resolve_project_dir(openclaw_dir, config=local_config)
    profiles = load_runtime_profile_catalog(project_dir=project_dir)
    active_profile_key = resolve_runtime_profile_key(default="host")
    active_profile = _normalize_runtime_profile_summary(active_profile_key, profiles.get(active_profile_key))
    profile_items = [
        _normalize_runtime_profile_summary(profile_key, payload)
        for profile_key, payload in profiles.items()
        if isinstance(payload, dict)
    ]
    speech_runtime = load_speech_runtime_config(openclaw_dir, config=local_config, metadata=local_metadata)
    speech_health = build_speech_runtime_capability_payload(speech_runtime)
    voice_workflow_payload = load_openclaw_voice_workflow_panel_data(openclaw_dir, config=local_config, metadata=local_metadata)
    voice_runtime = (
        voice_workflow_payload.get("voiceWorkflow", {}).get("speechRuntime", {})
        if isinstance(voice_workflow_payload, dict)
        else {}
    )
    local_runtime_payload = load_local_runtime_payload(
        openclaw_dir,
        config=local_config,
        metadata=local_metadata,
        include_capabilities=True,
    )
    local_runtime_capability = (
        local_runtime_payload.get("capabilities")
        if isinstance(local_runtime_payload.get("capabilities"), dict)
        else build_local_runtime_capability_payload(local_runtime_payload)
    )
    profile_speech_base_url = str(((active_profile.get("speechRuntime") or {}) if isinstance(active_profile.get("speechRuntime"), dict) else {}).get("baseUrl") or "").strip()
    profile_product_base_url = str(((active_profile.get("product") or {}) if isinstance(active_profile.get("product"), dict) else {}).get("baseUrl") or "").strip()
    runtime_overview = {
        "activeProfile": {
            **active_profile,
            "resolvedProjectDir": str(project_dir or "").strip(),
            "matchesCurrentSpeechRuntime": _normalize_runtime_base_url(profile_speech_base_url) == _normalize_runtime_base_url(speech_runtime.get("baseUrl")),
        },
        "profiles": profile_items,
        "speechRuntime": {
            **speech_runtime,
            **speech_health,
            "speakerCount": int(voice_runtime.get("speakerCount") or 0),
            "availableVoices": clean_unique_strings(voice_runtime.get("availableVoices") or [])[:12],
            "defaultVoice": str(voice_runtime.get("defaultVoice") or "").strip(),
            "supportsDistinctMemberVoices": bool(voice_runtime.get("supportsDistinctMemberVoices")),
            "profileBaseUrl": profile_speech_base_url,
        },
        "localRuntime": {
            "baseUrl": str(local_runtime_payload.get("baseUrl") or "").strip(),
            "profileBaseUrl": str(((active_profile.get("localRuntime") or {}) if isinstance(active_profile.get("localRuntime"), dict) else {}).get("baseUrl") or "").strip(),
            "health": local_runtime_capability.get("health") if isinstance(local_runtime_capability, dict) else {},
            "running": bool(local_runtime_payload.get("running")),
            "configured": bool(local_runtime_payload.get("configured")),
        },
        "product": {
            "baseUrl": profile_product_base_url,
            "port": ((active_profile.get("product") or {}) if isinstance(active_profile.get("product"), dict) else {}).get("port"),
            "stateDir": str(((active_profile.get("openclaw") or {}) if isinstance(active_profile.get("openclaw"), dict) else {}).get("stateDir") or "").strip(),
        },
        "checkedAt": now_iso(),
    }
    return {
        "deferred": False,
        "error": "",
        "runtimeOverview": runtime_overview,
    }


def load_openclaw_orchestration_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    runtime_sync_payload = load_runtime_sync_payload(openclaw_dir, config=local_config, metadata=local_metadata)
    execution_architecture_payload = load_model_execution_architecture_payload(
        openclaw_dir,
        config=local_config,
        metadata=local_metadata,
        include_local_runtime_capabilities=True,
        include_model_access_matrix=False,
        local_runtime_payload=runtime_sync_payload.get("localRuntime") if isinstance(runtime_sync_payload.get("localRuntime"), dict) else None,
    )
    agents_config = ((local_config.get("agents") or {}) if isinstance(local_config, dict) else {})
    agent_entries = [item for item in safe_list(agents_config.get("list")) if isinstance(item, dict)]
    workspace_coverage = runtime_sync_payload.get("workspaceCoverage") if isinstance(runtime_sync_payload.get("workspaceCoverage"), dict) else {}
    auth_payload = runtime_sync_payload.get("auth") if isinstance(runtime_sync_payload.get("auth"), dict) else {}
    sandbox_payload = {
        "mode": "per-agent workspace",
        "persistent": True,
        "projectDir": str(runtime_sync_payload.get("projectDir") or "").strip(),
        "workspaceRoot": str(Path(openclaw_dir).expanduser().resolve()),
        "frontendBuilt": bool(((runtime_sync_payload.get("frontend") or {}) if isinstance(runtime_sync_payload.get("frontend"), dict) else {}).get("built")),
        "skillsPath": str(((runtime_sync_payload.get("skills") or {}) if isinstance(runtime_sync_payload.get("skills"), dict) else {}).get("path") or "").strip(),
    }
    next_steps = []
    if not bool(workspace_coverage.get("ok")):
        next_steps.append("同步 agent workspace，让每位成员都有独立可持续的 sandbox。")
    if not bool(auth_payload.get("ok")):
        next_steps.append("补齐 agent 鉴权，让多成员编排能直接落到真实模型。")
    if not sandbox_payload["frontendBuilt"]:
        next_steps.append("构建前端产物并同步运行时，避免工作区只能停留在脚本层。")
    if not next_steps:
        next_steps.append("当前已经具备 DeerFlow 风格的多成员规划与持久 workspace 编排基础。")
    return {
        "deferred": False,
        "error": str(runtime_sync_payload.get("error") or "").strip(),
        "orchestration": {
            "ok": bool(workspace_coverage.get("ok")) and bool(auth_payload.get("ok")),
            "agentCount": len(agent_entries),
            "routerAgentId": str(runtime_sync_payload.get("routerAgentId") or get_router_agent_id(local_config) or "").strip(),
            "planning": {
                "primaryPath": str(execution_architecture_payload.get("primaryPath") or "").strip(),
                "fallbackPath": str(execution_architecture_payload.get("fallbackPath") or "").strip(),
                "preferredProviderId": str(execution_architecture_payload.get("preferredProviderId") or "").strip(),
                "summary": str(((execution_architecture_payload.get("decision") or {}) if isinstance(execution_architecture_payload.get("decision"), dict) else {}).get("summary") or "").strip(),
            },
            "subAgents": {
                "agentCount": len(agent_entries),
                "workspaceReady": max(0, int(workspace_coverage.get("ready") or 0)),
                "workspaceTotal": max(0, int(workspace_coverage.get("total") or 0)),
                "authReadyCount": max(0, int(auth_payload.get("readyCount") or 0)),
                "authTargetCount": max(0, int(auth_payload.get("targetCount") or 0)),
            },
            "sandbox": sandbox_payload,
            "nextSteps": next_steps[:4],
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_session_governance_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    agent_defaults = (
        ((local_config.get("agents") or {}) if isinstance(local_config, dict) else {}).get("defaults")
        if isinstance((local_config.get("agents") or {}) if isinstance(local_config, dict) else {}, dict)
        else {}
    )
    session_config = deepcopy(local_config.get("session") if isinstance(local_config.get("session"), dict) else {})
    tools_config = deepcopy(local_config.get("tools") if isinstance(local_config.get("tools"), dict) else {})
    compaction = deepcopy(agent_defaults.get("compaction") if isinstance(agent_defaults.get("compaction"), dict) else {})
    context_pruning = deepcopy(agent_defaults.get("contextPruning") if isinstance(agent_defaults.get("contextPruning"), dict) else {})
    if not str(compaction.get("mode") or "").strip():
        compaction["mode"] = "safeguard"
    if not str(context_pruning.get("mode") or "").strip():
        context_pruning["mode"] = "cache-ttl"
    if not str(context_pruning.get("ttl") or "").strip():
        context_pruning["ttl"] = "1h"
    session_config.pop("transcriptPath", None)
    if not str(session_config.get("dmScope") or "").strip():
        session_config["dmScope"] = "per-channel-peer"
    maintenance = session_config.get("maintenance") if isinstance(session_config.get("maintenance"), dict) else {}
    if not str(maintenance.get("mode") or "").strip():
        maintenance["mode"] = "warn"
    if not str(maintenance.get("pruneAfter") or "").strip():
        maintenance["pruneAfter"] = "30d"
    if not maintenance.get("maxEntries"):
        maintenance["maxEntries"] = 500
    if not str(maintenance.get("rotateBytes") or "").strip():
        maintenance["rotateBytes"] = "10mb"
    session_config["maintenance"] = maintenance
    tools_sessions = tools_config.get("sessions") if isinstance(tools_config.get("sessions"), dict) else {}
    tools_sessions.pop("transcript", None)
    tools_sessions["visibility"] = "all"
    tools_config["sessions"] = tools_sessions
    pressure = {
        "allAgents": True,
        "count": 0,
        "sessions": [],
    }
    error = ""
    try:
        env = openclaw_live_probe_env(openclaw_dir)
        result = _run_command(["openclaw", "sessions", "--all-agents", "--json"], env=env, timeout=8)
        payload = parse_json_payload(result.stdout, result.stderr, default=None)
        if isinstance(payload, dict):
            sessions = safe_list(payload.get("sessions"))
            pressure = {
                "allAgents": True,
                "count": len(sessions),
                "sessions": [
                    {
                        "key": str(item.get("key") or "").strip(),
                        "agentId": str(item.get("agentId") or "").strip(),
                        "updatedAt": epoch_ms_to_iso(item.get("updatedAt")),
                        "kind": str(item.get("kind") or "").strip(),
                        "contextTokens": item.get("contextTokens"),
                        "totalTokens": item.get("totalTokens"),
                        "abortedLastRun": bool(item.get("abortedLastRun")),
                    }
                    for item in sessions[:12]
                    if isinstance(item, dict) and str(item.get("key") or "").strip()
                ],
            }
        else:
            error = str(result.stderr or result.stdout or "session_governance_probe_failed").strip()
    except FileNotFoundError:
        error = "未检测到 openclaw CLI。"
    except Exception as exc:
        error = str(exc or "").strip()
    return {
        "deferred": False,
        "error": error,
        "sessionGovernance": {
            "compaction": compaction,
            "contextPruning": context_pruning,
            "session": session_config,
            "tools": tools_config,
            "pressure": pressure,
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_memory_workflow_panel_data(openclaw_dir, config=None, metadata=None):
    memory_payload = load_openclaw_memory_panel_data(openclaw_dir, config=config, metadata=metadata)
    governance_payload = load_openclaw_session_governance_panel_data(openclaw_dir, config=config, metadata=metadata)
    memory = memory_payload.get("memory") if isinstance(memory_payload.get("memory"), dict) else {}
    governance = governance_payload.get("sessionGovernance") if isinstance(governance_payload.get("sessionGovernance"), dict) else {}
    session_config = governance.get("session") if isinstance(governance.get("session"), dict) else {}
    maintenance_config = session_config.get("maintenance") if isinstance(session_config.get("maintenance"), dict) else {}
    tools_config = governance.get("tools") if isinstance(governance.get("tools"), dict) else {}
    tools_sessions = tools_config.get("sessions") if isinstance(tools_config.get("sessions"), dict) else {}
    short_term = {
        "mode": str(((governance.get("contextPruning") or {}) if isinstance(governance.get("contextPruning"), dict) else {}).get("mode") or "").strip(),
        "ttl": str(((governance.get("contextPruning") or {}) if isinstance(governance.get("contextPruning"), dict) else {}).get("ttl") or "").strip(),
        "dmScope": str(session_config.get("dmScope") or "").strip(),
        "pressureCount": max(0, int(((governance.get("pressure") or {}) if isinstance(governance.get("pressure"), dict) else {}).get("count") or 0)),
    }
    carry_forward = {
        "mode": str(((governance.get("compaction") or {}) if isinstance(governance.get("compaction"), dict) else {}).get("mode") or "").strip(),
        "maintenanceMode": str(maintenance_config.get("mode") or "").strip(),
        "pruneAfter": str(maintenance_config.get("pruneAfter") or "").strip(),
        "visibility": str(tools_sessions.get("visibility") or "").strip(),
    }
    long_term = {
        "readyCount": max(0, int(memory.get("readyCount") or 0)),
        "agentCount": max(0, int(memory.get("agentCount") or 0)),
        "providerCount": max(0, int(memory.get("providerCount") or len(safe_list(memory.get("providers"))))),
        "providers": safe_list(memory.get("providers"))[:6],
        "entries": safe_list(memory.get("entries"))[:6],
    }
    next_steps = []
    if not bool(memory.get("ok")):
        next_steps.append("先补 embeddings/provider，让长期记忆运行面进入 ready。")
    if not short_term["mode"]:
        next_steps.append("配置 context pruning，让短期会话不会继续无限膨胀。")
    if not carry_forward["mode"]:
        next_steps.append("补 compaction 策略，把长会话整理成可继续工作的 carry-forward。")
    if not next_steps:
        next_steps.append("当前已具备 DeerFlow 风格的短期上下文、carry-forward 与长期记忆协同链。")
    return {
        "deferred": False,
        "error": str(memory_payload.get("error") or governance_payload.get("error") or "").strip(),
        "memoryWorkflow": {
            "ok": bool(memory.get("ok")) and bool(short_term["mode"]) and bool(carry_forward["mode"]),
            "shortTerm": short_term,
            "carryForward": carry_forward,
            "longTerm": long_term,
            "nextSteps": next_steps[:4],
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_message_gateway_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    gateway_payload = load_openclaw_gateway_panel_data(openclaw_dir, config=local_config, metadata=local_metadata)
    runtime_sync_payload = load_runtime_sync_payload(openclaw_dir, config=local_config, metadata=local_metadata)
    gateway = gateway_payload.get("gateway") if isinstance(gateway_payload.get("gateway"), dict) else {}
    auth_payload = runtime_sync_payload.get("auth") if isinstance(runtime_sync_payload.get("auth"), dict) else {}
    channels = [item for item in safe_list(gateway.get("channels")) if isinstance(item, dict)]
    configured_channels = [item for item in channels if item.get("configured") or str(item.get("meta") or "").strip() != "not configured"]
    running_channels = [item for item in configured_channels if item.get("running") or item.get("healthy")]
    next_steps = []
    if not configured_channels:
        next_steps.append("至少接通一个 IM channel，把 agent 能力真正挂到消息入口。")
    if not bool(auth_payload.get("ok")):
        next_steps.append("同步 agent 鉴权，不然 message gateway 只能停在配置层。")
    if not running_channels and configured_channels:
        next_steps.append("执行一次 gateway live probe，确认配置过的通道已经真正联通。")
    if not next_steps:
        next_steps.append("当前消息通道已具备进入 super-agent 编排入口的基础。")
    return {
        "deferred": False,
        "error": str(gateway_payload.get("error") or runtime_sync_payload.get("error") or "").strip(),
        "messageGateway": {
            "ok": bool(configured_channels) and bool(auth_payload.get("ok")),
            "agentCount": max(0, int(gateway.get("agentCount") or 0)),
            "defaultAgentId": str(gateway.get("defaultAgentId") or "").strip(),
            "channelSummary": {
                "total": len(channels),
                "configured": len(configured_channels),
                "running": len(running_channels),
            },
            "channels": [
                {
                    "id": str(item.get("id") or "").strip(),
                    "title": str(item.get("title") or item.get("id") or "").strip(),
                    "meta": str(item.get("meta") or "").strip(),
                    "detail": str(item.get("detail") or "").strip(),
                    "healthy": bool(item.get("healthy")),
                    "running": bool(item.get("running")),
                }
                for item in channels[:8]
            ],
            "auth": {
                "readyCount": max(0, int(auth_payload.get("readyCount") or 0)),
                "targetCount": max(0, int(auth_payload.get("targetCount") or 0)),
                "state": str(auth_payload.get("state") or "").strip(),
            },
            "nextSteps": next_steps[:4],
            "probeUpdatedAt": now_iso(),
        },
    }


def _compact_runtime_preview(value, limit=220):
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: max(limit - 1, 0)].rstrip()}…"


def _skill_growth_slug(value):
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return slug[:64] or "draft-skill"


def _build_skill_growth_candidates(deliverables, runs, existing_skill_slugs):
    existing_skill_slugs = {
        str(item or "").strip().lower()
        for item in safe_list(list(existing_skill_slugs))
        if str(item or "").strip()
    }
    candidates = []
    seen_keys = set()

    def append_candidate(source_kind, title, summary, updated_at="", source_label="", category="workflow-automation", trigger_phrase="", objective=""):
        normalized_title = str(title or "").strip()
        if not normalized_title:
            return
        base_slug = _skill_growth_slug(normalized_title)
        suggested_slug = base_slug
        if suggested_slug in existing_skill_slugs:
            for suffix in ("-workflow", "-assistant", "-automation", "-playbook"):
                candidate_slug = f"{base_slug}{suffix}"[:64].strip("-")
                if candidate_slug and candidate_slug not in existing_skill_slugs:
                    suggested_slug = candidate_slug
                    break
        dedupe_key = f"{source_kind}:{suggested_slug}"
        if dedupe_key in seen_keys:
            return
        seen_keys.add(dedupe_key)
        candidates.append(
            {
                "id": dedupe_key,
                "sourceKind": source_kind,
                "title": normalized_title,
                "summary": _compact_runtime_preview(summary or normalized_title),
                "suggestedSlug": suggested_slug,
                "category": category,
                "triggerPhrase": trigger_phrase or (
                    f"请复用 {normalized_title} 这套方法继续处理相似任务"
                    if source_kind == "deliverable"
                    else f"请按这套自动化步骤继续完成：{normalized_title}"
                ),
                "objective": str(objective or "").strip(),
                "sourceLabel": str(source_label or normalized_title).strip(),
                "updatedAt": str(updated_at or "").strip(),
                "readyToScaffold": suggested_slug not in existing_skill_slugs,
            }
        )

    for item in safe_list(deliverables)[:12]:
        if not isinstance(item, dict):
            continue
        append_candidate(
            "deliverable",
            item.get("title") or item.get("pathLabel") or item.get("id"),
            item.get("summary") or item.get("pathLabel") or item.get("output"),
            updated_at=item.get("updatedAt"),
            source_label=item.get("pathLabel") or item.get("id"),
            category="workflow-automation",
        )

    for item in safe_list((runs or {}).get("items"))[:12]:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip().lower()
        if status not in {"succeeded", "completed", "done"}:
            continue
        append_candidate(
            "computer_use",
            item.get("objective") or item.get("sourceSummary") or item.get("id"),
            item.get("sourceSummary") or item.get("currentStepIntent") or item.get("deviceName"),
            updated_at=item.get("updatedAt"),
            source_label=item.get("sourceSummary") or item.get("deviceName") or item.get("id"),
            category="browser-automation",
            objective=item.get("objective"),
        )

    return candidates[:8]


def load_openclaw_skill_growth_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    skills_detail = load_skills_detail(openclaw_dir)
    skills = [item for item in safe_list((skills_detail or {}).get("skills")) if isinstance(item, dict)]
    skill_summary = (skills_detail.get("summary") if isinstance(skills_detail, dict) else {}) if isinstance((skills_detail or {}).get("summary"), dict) else {}
    existing_skill_slugs = [str(item.get("slug") or "").strip().lower() for item in skills if str(item.get("slug") or "").strip()]
    deliverables = build_deliverables_snapshot(openclaw_dir, config=local_config)
    runs = build_computer_use_runs_snapshot(openclaw_dir, page=1, page_size=12)
    memory_payload = load_openclaw_memory_panel_data(openclaw_dir, config=local_config, metadata=local_metadata)
    memory = memory_payload.get("memory") if isinstance(memory_payload.get("memory"), dict) else {}
    candidates = _build_skill_growth_candidates(deliverables, runs, existing_skill_slugs)
    next_steps = []
    if not candidates:
        next_steps.append("先通过交付物或成功的 browser / computer-use run 沉淀可复用做法。")
    if int(skill_summary.get("publishedToOpenClaw") or 0) == 0:
        next_steps.append("先把至少一项内部技能正式发布到 OpenClaw，建立可复用的增长闭环。")
    if int(memory.get("readyCount") or 0) <= 0:
        next_steps.append("先补长期记忆 provider，让技能候选能和记忆工作流联动。")
    if not next_steps:
        next_steps.append("当前已具备 Hermes 风格的技能自增长候选池，可直接从交付物和自动化 run 沉淀新技能。")
    return {
        "deferred": False,
        "error": "",
        "skillGrowth": {
            "ok": bool(candidates) or int(skill_summary.get("installedToOpenClaw") or 0) > 0,
            "candidateCount": len(candidates),
            "publishedCount": max(0, int(skill_summary.get("publishedToOpenClaw") or 0)),
            "installedCount": max(0, int(skill_summary.get("installedToOpenClaw") or 0)),
            "readySkillCount": sum(1 for item in skills if item.get("eligible") is not False),
            "sourceSummary": {
                "deliverables": len(safe_list(deliverables)),
                "computerUseRuns": max(0, int((runs.get("summary") or {}).get("total") or 0)),
                "memoryReady": max(0, int(memory.get("readyCount") or 0)),
            },
            "candidates": candidates,
            "nextSteps": next_steps[:4],
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_sandbox_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    runtime_sync_payload = load_runtime_sync_payload(openclaw_dir, config=local_config, metadata=local_metadata)
    local_runtime_payload = load_local_runtime_payload(openclaw_dir, config=local_config, metadata=local_metadata)
    execution_architecture_payload = load_model_execution_architecture_payload(
        openclaw_dir,
        config=local_config,
        metadata=local_metadata,
        include_local_runtime_capabilities=True,
        include_model_access_matrix=False,
        local_runtime_payload=local_runtime_payload,
    )
    workspace_coverage = runtime_sync_payload.get("workspaceCoverage") if isinstance(runtime_sync_payload.get("workspaceCoverage"), dict) else {}
    project_dir = str(runtime_sync_payload.get("projectDir") or "").strip()
    backends = [
        {
            "id": "provider_api",
            "title": "Hosted provider path",
            "kind": "hosted",
            "status": "ready" if str(execution_architecture_payload.get("primaryPath") or "").strip() == "provider_api" else "info",
            "detail": "OpenClaw Team -> OpenClaw -> provider API",
            "isPrimary": str(execution_architecture_payload.get("primaryPath") or "").strip() == "provider_api",
            "transport": str(execution_architecture_payload.get("transport") or "").strip(),
        },
        {
            "id": "local_runtime",
            "title": "Local runtime backend",
            "kind": "local",
            "status": "ready" if bool(local_runtime_payload.get("running")) else ("warning" if bool(local_runtime_payload.get("enabled")) else "idle"),
            "detail": str(local_runtime_payload.get("label") or local_runtime_payload.get("backend") or "local runtime").strip(),
            "isPrimary": str(execution_architecture_payload.get("primaryPath") or "").strip() == "local_runtime",
            "transport": str(execution_architecture_payload.get("transport") or "").strip(),
        },
        {
            "id": "workspace",
            "title": "Per-agent workspace",
            "kind": "workspace",
            "status": "ready" if bool(workspace_coverage.get("ok")) else "warning",
            "detail": f"workspace {int(workspace_coverage.get('ready') or 0)}/{int(workspace_coverage.get('total') or 0)}",
            "isPrimary": True,
            "transport": "filesystem",
        },
    ]
    next_steps = []
    if not bool(workspace_coverage.get("ok")):
        next_steps.append("先同步 workspace，让每位成员都有独立 sandbox。")
    if not bool(local_runtime_payload.get("running")) and str(execution_architecture_payload.get("fallbackPath") or "").strip() == "local_runtime":
        next_steps.append("本地 runtime 还是补充路径时，建议启动后再启用更强隔离执行。")
    if not project_dir:
        next_steps.append("补 projectDir，让 workspace sandbox 和 deliverables 能绑定到真实项目。")
    if not next_steps:
        next_steps.append("当前已具备 Hermes 风格的 hosted provider、local runtime 与 per-agent workspace 三层沙箱后端。")
    return {
        "deferred": False,
        "error": str(runtime_sync_payload.get("error") or local_runtime_payload.get("launcherError") or "").strip(),
        "sandbox": {
            "ok": bool(workspace_coverage.get("ok")) and bool(project_dir),
            "routing": {
                "transport": str(execution_architecture_payload.get("transport") or "").strip(),
                "primaryPath": str(execution_architecture_payload.get("primaryPath") or "").strip(),
                "fallbackPath": str(execution_architecture_payload.get("fallbackPath") or "").strip(),
                "contextMode": str(execution_architecture_payload.get("contextMode") or "").strip(),
                "localRuntimeRole": str(execution_architecture_payload.get("localRuntimeRole") or "").strip(),
            },
            "backends": backends,
            "projectDir": project_dir,
            "workspaceRoot": str(Path(openclaw_dir).expanduser().resolve()),
            "nextSteps": next_steps[:4],
            "probeUpdatedAt": now_iso(),
        },
    }


def load_openclaw_voice_workflow_panel_data(openclaw_dir, config=None, metadata=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    local_metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=local_config)
    runtime = load_speech_runtime_config(openclaw_dir, config=local_config, metadata=local_metadata)
    cache_key = (
        str(Path(openclaw_dir).expanduser().resolve()),
        str(runtime.get("provider") or "").strip(),
        str(runtime.get("baseUrl") or "").strip(),
        str(runtime.get("model") or "").strip(),
        str(runtime.get("apiKeyEnv") or "").strip(),
        str(((local_metadata.get(SPEECH_RUNTIME_CONFIG_KEY) or {}) if isinstance(local_metadata.get(SPEECH_RUNTIME_CONFIG_KEY), dict) else {}).get("updatedAt") or "").strip(),
    )
    now_ts = time.time()
    cached = VOICE_WORKFLOW_PANEL_CACHE.get(cache_key)
    if isinstance(cached, dict) and (now_ts - float(cached.get("at") or 0.0)) < VOICE_WORKFLOW_PANEL_CACHE_TTL_SECONDS:
        return deepcopy(cached.get("payload") or {})
    env = openclaw_command_env(openclaw_dir)
    api_key_env = str(runtime.get("apiKeyEnv") or DEFAULT_SPEECH_RUNTIME_API_KEY_ENV).strip()
    provider = str(runtime.get("provider") or DEFAULT_SPEECH_RUNTIME_PROVIDER).strip()
    runtime_base_url = str(runtime.get("baseUrl") or "").strip()
    builtin_voices = []
    custom_voices = []
    available_voices = []
    default_voice = ""
    voice_labels = {}
    if provider in {"qwen3_tts", "sherpa_onnx"}:
        runtime_ready = False
        try:
            from .customer_access_core import (
                _customer_voice_runtime_available_voices,
                _customer_voice_runtime_builtin_voices,
                _customer_voice_runtime_probe_payload,
            )

            builtin_voices = clean_unique_strings(_customer_voice_runtime_builtin_voices(openclaw_dir, runtime))
            available_voices = clean_unique_strings(_customer_voice_runtime_available_voices(openclaw_dir, runtime))
            custom_voices = [item for item in available_voices if item not in builtin_voices]
            probe_payload = _customer_voice_runtime_probe_payload(runtime, timeout=3)
            data = safe_list(probe_payload.get("data"))
            first_model = data[0] if data and isinstance(data[0], dict) else {}
            if not builtin_voices:
                builtin_voices = clean_unique_strings(first_model.get("voices") or probe_payload.get("voices") or [])
            voice_labels = first_model.get("voice_labels") if isinstance(first_model.get("voice_labels"), dict) else {}
            if not voice_labels:
                voice_labels = probe_payload.get("voice_labels") if isinstance(probe_payload.get("voice_labels"), dict) else {}
            if not available_voices:
                custom_voices = clean_unique_strings(customer_voice_custom_voices(openclaw_dir, config=local_config))
                available_voices = clean_unique_strings([*builtin_voices, *custom_voices])
            default_voice = str(first_model.get("default_voice") or probe_payload.get("default_voice") or "").strip()
            runtime_ready = bool(probe_payload or available_voices)
        except Exception:
            builtin_voices = []
            available_voices = []
            custom_voices = []
    else:
        runtime_ready = bool(
            str(env.get(api_key_env) or read_env_value(openclaw_dir, api_key_env) or os.environ.get(api_key_env) or "").strip()
        )
    if provider == "qwen3_tts" and not available_voices:
        builtin_voices = ["serena"]
        custom_voices = []
        available_voices = clean_unique_strings(builtin_voices)
    if provider == "sherpa_onnx" and not available_voices:
        builtin_voices = ["zf_001", "zf_002", "zf_003", "zf_004", "zf_005", "zf_006", "zm_009", "zm_010", "zm_011", "zm_012", "zm_013"]
        custom_voices = []
        available_voices = clean_unique_strings(builtin_voices)
    if not available_voices:
        configured_voice = str(runtime.get("defaultVoice") or "").strip()
        if configured_voice:
            available_voices = [configured_voice]
    if provider in {"qwen3_tts", "sherpa_onnx"} and not builtin_voices:
        builtin_voices = [item for item in available_voices if not str(item or "").strip().startswith("custom:")]
    if not default_voice:
        default_voice = (builtin_voices[0] if builtin_voices else (available_voices[0] if available_voices else ""))
    router_agent_id = str(get_router_agent_id(local_config) or "").strip()
    agents = [item for item in safe_list(load_agents(local_config)) if isinstance(item, dict) and str(item.get("id") or "").strip()]
    agent_label_map = {
        str(item.get("id") or "").strip(): str(
            ((item.get("identity") or {}) if isinstance(item.get("identity"), dict) else {}).get("name")
            or item.get("title")
            or item.get("name")
            or item.get("id")
            or ""
        ).strip()
        for item in agents
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    }
    speech_ready_count = 0
    transcribe_ready_count = 0
    sample_agents = []
    agent_overrides = local_metadata.get("agentOverrides") if isinstance(local_metadata.get("agentOverrides"), dict) else {}
    for agent in agents:
        agent_id = str(agent.get("id") or "").strip()
        override = agent_overrides.get(agent_id) if isinstance(agent_overrides.get(agent_id), dict) else {}
        profile = merged_agent_runtime_profile(agent_id, override=override) if agent_id else {}
        skills = {
            str(skill or "").strip().lower()
            for skill in [
                *safe_list(agent.get("skills")),
                *safe_list(profile.get("skills") if isinstance(profile, dict) else []),
            ]
            if str(skill or "").strip()
        }
        has_speech = "speech" in skills
        has_transcribe = "transcribe" in skills
        if has_speech:
            speech_ready_count += 1
        if has_transcribe:
            transcribe_ready_count += 1
        if len(sample_agents) < 6:
            sample_agents.append(
                {
                    "id": agent_id,
                    "label": str(((agent.get("identity") or {}) if isinstance(agent.get("identity"), dict) else {}).get("name") or profile.get("humanName") or agent.get("id") or "").strip(),
                    "speech": has_speech,
                    "transcribe": has_transcribe,
                }
            )
    channel_load_error = ""
    try:
        channels = [item for item in safe_list(store_list_customer_access_channels(openclaw_dir)) if isinstance(item, dict)]
    except Exception as exc:
        channels = []
        channel_load_error = str(exc).strip()
    voice_channels = []
    ready_voice_channels = 0
    for channel in channels[:8]:
        normalized_channel = channel if isinstance(channel, dict) else {}
        voice_config = customer_channel_voice_reply_config(channel)
        enabled = bool(voice_config.get("enabled"))
        channel_ready = enabled and runtime_ready
        if channel_ready:
            ready_voice_channels += 1
        enriched_channel = normalized_channel
        resolution = {}
        effective_voice = str(
            voice_config.get("voice")
            or ("follow_agent_profile" if str(voice_config.get("mode") or "").strip() == "agent_profile" else default_voice or DEFAULT_SPEECH_RUNTIME_PROVIDER)
        ).strip()
        if enabled:
            try:
                from .customer_access_core import (
                    enrich_customer_access_channel_payload,
                    resolve_agent_voice_reply_profile,
                    resolve_customer_channel_voice_test_target,
                )

                enriched_channel = enrich_customer_access_channel_payload(channel, openclaw_dir=openclaw_dir)
                resolution = resolve_customer_channel_voice_test_target(openclaw_dir, channel)
                if str(voice_config.get("mode") or "").strip() == "agent_profile":
                    agent_profile = resolve_agent_voice_reply_profile(openclaw_dir, resolution.get("agentId"))
                    effective_voice = str(agent_profile.get("voice") or effective_voice or "").strip()
            except Exception:
                resolution = {}
                enriched_channel = normalized_channel
        voice_channels.append(
            {
                "id": str(enriched_channel.get("id") or "").strip(),
                "name": str(enriched_channel.get("name") or enriched_channel.get("target") or enriched_channel.get("id") or "").strip(),
                "type": str(enriched_channel.get("type") or "").strip(),
                "sourceKind": "customer",
                "voiceEnabled": enabled,
                "ready": channel_ready,
                "voiceReplyMode": str(voice_config.get("mode") or "fixed").strip(),
                "voiceReplyVoice": effective_voice,
                "targetAgentId": str(resolution.get("agentId") or "").strip(),
                "targetAgentName": str(
                    resolution.get("agentName")
                    or agent_label_map.get(str(resolution.get("agentId") or "").strip())
                    or resolution.get("agentId")
                    or ""
                ).strip(),
                "targetTeamId": str(resolution.get("teamId") or "").strip(),
                "targetTeamName": str(resolution.get("teamName") or "").strip(),
                "entryUrl": str(enriched_channel.get("entryUrl") or "").strip(),
                "callbackPath": str(enriched_channel.get("callbackPath") or "").strip(),
                "verificationConfigured": bool(enriched_channel.get("verificationConfigured")),
                "outboundConfigured": bool(enriched_channel.get("outboundConfigured")),
                "pluginInstalled": bool(enriched_channel.get("pluginInstalled")),
                "pluginEnabled": bool(enriched_channel.get("pluginEnabled")),
                "accountConfigured": bool(enriched_channel.get("accountConfigured")),
                "accountLabel": str(enriched_channel.get("accountLabel") or "").strip(),
                "accountId": str(enriched_channel.get("accountId") or "").strip(),
            }
        )
    builtin_agent_profile = {}
    builtin_agent_name = agent_label_map.get(router_agent_id) or router_agent_id
    if router_agent_id:
        try:
            from .customer_access_core import resolve_agent_voice_reply_profile

            builtin_agent_profile = resolve_agent_voice_reply_profile(openclaw_dir, router_agent_id)
        except Exception:
            builtin_agent_profile = {}
    builtin_channel_ready = runtime_ready and speech_ready_count > 0
    builtin_input_ready = transcribe_ready_count > 0
    builtin_channels = [
        {
            "id": "web-conversations",
            "name": "Web conversations",
            "type": "builtin-web",
            "sourceKind": "builtin",
            "voiceEnabled": True,
            "ready": builtin_channel_ready,
            "detail": "Built-in web chat playback and upload lane.",
            "voiceReplyMode": "agent_profile",
            "voiceReplyVoice": str(builtin_agent_profile.get("voice") or "follow_agent_profile").strip(),
            "targetAgentId": router_agent_id,
            "targetAgentName": builtin_agent_name,
            "targetTeamId": "",
            "targetTeamName": "",
        },
        {
            "id": "desktop-client",
            "name": "Desktop client",
            "type": "builtin-desktop",
            "sourceKind": "builtin",
            "voiceEnabled": True,
            "ready": builtin_channel_ready,
            "detail": "Desktop chat and operator shell voice lane.",
            "voiceReplyMode": "agent_profile",
            "voiceReplyVoice": str(builtin_agent_profile.get("voice") or "follow_agent_profile").strip(),
            "targetAgentId": router_agent_id,
            "targetAgentName": builtin_agent_name,
            "targetTeamId": "",
            "targetTeamName": "",
        },
    ]
    voice_channels = builtin_channels + voice_channels
    ready_voice_channels += sum(1 for item in builtin_channels if item.get("ready"))
    customer_voice_channel_count = sum(1 for item in voice_channels if str(item.get("sourceKind") or "").strip() == "customer")
    builtin_voice_channel_count = sum(1 for item in voice_channels if str(item.get("sourceKind") or "").strip() == "builtin")
    customer_entry_blueprints = []
    try:
        from .customer_access_core import (
            customer_access_callback_path,
            enrich_customer_access_channel_payload,
            load_openclaw_weixin_runtime,
        )

        enriched_customer_channels = [
            enrich_customer_access_channel_payload(item, openclaw_dir=openclaw_dir)
            for item in channels
            if isinstance(item, dict)
        ]
        official_channels = [
            item for item in enriched_customer_channels
            if str(item.get("type") or "").strip().lower() == "wechat_official"
        ]
        openclaw_weixin_channels = [
            item for item in enriched_customer_channels
            if str(item.get("type") or "").strip().lower() == "openclaw_weixin"
        ]
        official_verification_ready = any(bool(item.get("verificationConfigured")) for item in official_channels)
        official_outbound_ready = any(bool(item.get("outboundConfigured")) for item in official_channels)
        official_callback_path = str(
            next(
                (
                    item.get("callbackPath")
                    for item in official_channels
                    if str(item.get("callbackPath") or "").strip()
                ),
                "",
            )
            or customer_access_callback_path("{channel_id}", provider="wechat")
            or ""
        ).strip()
        openclaw_weixin_runtime = load_openclaw_weixin_runtime(openclaw_dir)
        openclaw_weixin_account_ready = bool(
            any(bool(item.get("accountConfigured")) for item in openclaw_weixin_channels)
            or bool(openclaw_weixin_runtime.get("accountCount"))
        )
        customer_entry_blueprints = [
            {
                "key": "wechat-official",
                "type": "wechat_official",
                "title": "Official WeChat",
                "description": "Official account callback and outbound reply lane.",
                "recommendedAction": "wechat-official",
                "configuredCount": len(official_channels),
                "channelConfigured": bool(official_channels),
                "callbackPath": official_callback_path,
                "verificationConfigured": official_verification_ready,
                "outboundConfigured": official_outbound_ready,
                "ready": official_verification_ready and official_outbound_ready,
            },
            {
                "key": "openclaw-weixin",
                "type": "openclaw_weixin",
                "title": "OpenClaw Weixin",
                "description": "Scan-login based customer Weixin reception lane.",
                "recommendedAction": "openclaw-weixin",
                "configuredCount": len(openclaw_weixin_channels),
                "channelConfigured": bool(openclaw_weixin_channels),
                "pluginInstalled": bool(openclaw_weixin_runtime.get("installed")),
                "pluginEnabled": bool(openclaw_weixin_runtime.get("enabled")),
                "accountConfigured": openclaw_weixin_account_ready,
                "accountCount": int(openclaw_weixin_runtime.get("accountCount") or 0),
                "loginCommand": str(
                    openclaw_weixin_runtime.get("loginCommand")
                    or openclaw_weixin_runtime.get("containerLoginCommand")
                    or ""
                ).strip(),
                "ready": bool(
                    openclaw_weixin_runtime.get("installed")
                    and openclaw_weixin_runtime.get("enabled")
                    and openclaw_weixin_account_ready
                ),
            },
        ]
    except Exception:
        customer_entry_blueprints = []
    next_steps = []
    if not runtime_ready:
        next_steps.append(
            "先配置并启动语音 runtime，不然成员只能保留文本工作流。"
            if provider in {"qwen3_tts", "sherpa_onnx"}
            else "先配置语音 runtime，不然成员只能保留文本工作流。"
        )
    if speech_ready_count < len(agents):
        next_steps.append("补齐默认成员的 speech 技能，让语音输出进入主工作流。")
    if transcribe_ready_count < len(agents):
        next_steps.append("补齐 transcribe 技能，让语音输入和文本派发收成一条链。")
    if not any(item.get("voiceEnabled") for item in voice_channels):
        next_steps.append("至少开启一个支持 voice reply 的消息入口，把语音真正送到渠道。")
    elif customer_voice_channel_count == 0:
        next_steps.append("当前只有内置 Web / 桌面端语音链路，下一步可以补公众号或 OpenClaw 微信入口。")
    if channel_load_error:
        next_steps.append("渠道库暂时不可用，已退回内置语音通道；请检查 PostgreSQL 连接。")
    if not next_steps:
        next_steps.append("当前已具备 Hermes 风格的语音输入、语音输出和渠道回传基础。")
    payload = {
        "deferred": False,
        "error": "",
        "voiceWorkflow": {
            "ok": runtime_ready and speech_ready_count > 0,
            "speechRuntime": {
                **runtime,
                "ready": runtime_ready,
                "availableVoices": available_voices,
                "builtinVoices": builtin_voices,
                "voiceLabels": voice_labels,
                "defaultVoice": default_voice,
                "speakerCount": len(available_voices),
                "builtinSpeakerCount": len(builtin_voices),
                "customSpeakerCount": len(custom_voices),
                "supportsDistinctMemberVoices": len(available_voices) > 1,
            },
            "coverage": {
                "agentCount": len(agents),
                "speechReadyCount": speech_ready_count,
                "transcribeReadyCount": transcribe_ready_count,
                "agents": sample_agents,
            },
            "conversation": {
                "voiceReplyAttachmentReady": builtin_channel_ready,
                "voiceInputReady": builtin_input_ready,
                "webPlaybackReady": True,
            },
            "channels": {
                "total": len(voice_channels),
                "builtin": builtin_voice_channel_count,
                "customer": customer_voice_channel_count,
                "voiceEnabled": sum(1 for item in voice_channels if item.get("voiceEnabled")),
                "ready": ready_voice_channels,
                "items": voice_channels,
                "error": channel_load_error,
            },
            "customerEntryBlueprints": customer_entry_blueprints,
            "nextSteps": next_steps[:4],
            "probeUpdatedAt": now_iso(),
        },
    }
    VOICE_WORKFLOW_PANEL_CACHE[cache_key] = {"at": now_ts, "payload": deepcopy(payload)}
    return payload


def load_openclaw_control_data_subprocess(openclaw_dir, timeout=25):
    script_path = Path(getattr(_svc(), "__file__", Path(__file__))).resolve()
    try:
        process, output = run_python_script(
            script_path,
            ["--dir", str(Path(openclaw_dir).expanduser().resolve()), "--dump-openclaw-control", "--quiet"],
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        summary = load_openclaw_dashboard_summary(openclaw_dir)
        summary["error"] = "openclaw_control_timeout"
        summary["deferred"] = True
        return summary
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if process.returncode == 0 and isinstance(payload, dict):
        return payload
    summary = load_openclaw_dashboard_summary(openclaw_dir)
    summary["error"] = output or "openclaw_control_failed"
    summary["deferred"] = True
    return summary


def perform_openclaw_cli_install_action(openclaw_dir, action_name="install", method="", version="latest"):
    normalized_action = str(action_name or "install").strip().lower()
    if normalized_action not in {"install", "update"}:
        raise RuntimeError(f"Unsupported OpenClaw installer action: {action_name}")
    try:
        installer_script = openclaw_installer_script_path(openclaw_dir)
    except FileNotFoundError as error:
        raise RuntimeError(str(error)) from error
    args = [
        normalized_action,
        "--dir",
        str(Path(openclaw_dir).expanduser().resolve()),
        "--method",
        str(method or "auto").strip() or "auto",
        "--version",
        str(version or "latest").strip() or "latest",
        "--json",
    ]
    process, output = run_python_script(installer_script, args)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if process.returncode != 0:
        if isinstance(payload, dict) and payload.get("error"):
            raise RuntimeError(str(payload.get("error") or "").strip())
        raise RuntimeError(output or "OpenClaw install action failed.")
    if not isinstance(payload, dict):
        raise RuntimeError(output or "OpenClaw install action did not return JSON.")
    return payload


def perform_runtime_sync_action(openclaw_dir, build_frontend=True):
    local_config = load_config(openclaw_dir)
    project_dir = resolve_project_dir(openclaw_dir, config=local_config)
    if not project_dir:
        raise RuntimeError("当前安装没有关联OpenClaw Team 项目目录，无法同步运行时资产。")
    try:
        sync_script = runtime_sync_script_path(openclaw_dir)
    except FileNotFoundError as error:
        raise RuntimeError(str(error)) from error
    args = [
        "bash",
        str(sync_script),
        "--dir",
        str(Path(openclaw_dir).expanduser().resolve()),
        "--project-dir",
        str(project_dir),
    ]
    if build_frontend:
        args.append("--build-frontend")
    process = _run_command(args, cwd=project_dir)
    output = "\n".join(
        part.strip()
        for part in (process.stdout, process.stderr)
        if part and part.strip()
    ).strip()
    metadata = load_project_metadata(openclaw_dir, config=local_config)
    runtime_meta = metadata.get("runtimeSync") if isinstance(metadata.get("runtimeSync"), dict) else {}
    runtime_meta.update(
        {
            "lastAttemptAt": now_iso(),
            "buildFrontend": bool(build_frontend),
            "lastOutput": output,
        }
    )
    if process.returncode != 0:
        runtime_meta["error"] = output or "runtime_sync_failed"
        metadata["runtimeSync"] = runtime_meta
        save_project_metadata(openclaw_dir, metadata)
        raise RuntimeError(output or "OpenClaw Team runtime sync failed.")
    runtime_meta.update(
        {
            "lastSyncedAt": now_iso(),
            "error": "",
        }
    )
    metadata["projectDir"] = str(project_dir)
    metadata["runtimeSync"] = runtime_meta
    save_project_metadata(openclaw_dir, metadata)
    payload = load_runtime_sync_payload(openclaw_dir, config=load_config(openclaw_dir))
    payload["output"] = output
    return payload


def openclaw_browser_command(profile=""):
    args = ["openclaw", "browser"]
    normalized = str(profile or "").strip()
    if normalized:
        args.extend(["--browser-profile", normalized])
    return args


BROWSER_PROFILE_PREFERENCE_ORDER = ("openclaw", "user", "work", "remote")

def configured_browser_default_profile(config):
    browser = config.get("browser") if isinstance(config, dict) and isinstance(config.get("browser"), dict) else {}
    return str(browser.get("defaultProfile") or "").strip()


def normalize_browser_profiles_payload(payload):
    if isinstance(payload, dict):
        profiles = payload.get("profiles", [])
    elif isinstance(payload, list):
        profiles = payload
    else:
        profiles = []
    normalized_profiles = []
    for item in profiles if isinstance(profiles, list) else []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or item.get("profile") or item.get("id") or "").strip()
        if not name:
            continue
        normalized = deepcopy(item)
        normalized["name"] = name
        normalized_profiles.append(normalized)
    return {"profiles": normalized_profiles}


def browser_profile_candidate_names(config, browser_profiles_payload):
    profiles = normalize_browser_profiles_payload(browser_profiles_payload).get("profiles", [])
    available_names = [str(item.get("name") or "").strip() for item in profiles if str(item.get("name") or "").strip()]
    available_set = set(available_names)
    running_names = [name for name in available_names if next((item for item in profiles if item.get("name") == name and item.get("running")), None)]
    current_default = configured_browser_default_profile(config)
    candidates = []

    def add(name):
        normalized = str(name or "").strip()
        if normalized and normalized in available_set and normalized not in candidates:
            candidates.append(normalized)

    add(current_default)
    running_rank = {name: index for index, name in enumerate(BROWSER_PROFILE_PREFERENCE_ORDER)}
    for name in sorted(running_names, key=lambda item: (running_rank.get(item, len(BROWSER_PROFILE_PREFERENCE_ORDER)), item)):
        add(name)
    for name in BROWSER_PROFILE_PREFERENCE_ORDER:
        add(name)
    for name in available_names:
        add(name)
    return candidates


def resolve_available_browser_profile(openclaw_dir, config=None, browser_profiles_payload=None, fallback_profile=""):
    local_config = deepcopy(config) if isinstance(config, dict) else load_config(openclaw_dir)
    browser = local_config.get("browser") if isinstance(local_config.get("browser"), dict) else None
    if not isinstance(browser, dict):
        return configured_browser_default_profile(local_config)
    current_default = configured_browser_default_profile(local_config)
    profiles_payload = normalize_browser_profiles_payload(browser_profiles_payload)
    available_names = {
        str(item.get("name") or "").strip()
        for item in profiles_payload.get("profiles", [])
        if str(item.get("name") or "").strip()
    }
    if current_default and current_default in available_names:
        return current_default
    selected = str(fallback_profile or "").strip()
    if selected and selected not in available_names:
        selected = ""
    if not selected:
        candidates = browser_profile_candidate_names(local_config, profiles_payload)
        selected = candidates[0] if candidates else ""
    return selected or current_default


def load_browser_profiles_payload(openclaw_dir, env=None):
    process = _run_command(["openclaw", "browser", "--json", "profiles"], env=env or openclaw_command_env(openclaw_dir))
    parsed = parse_json_payload(process.stdout, process.stderr, default=None)
    return normalize_browser_profiles_payload(parsed), process


def normalize_browser_status_payload(payload, profile=""):
    if not isinstance(payload, dict):
        return None
    normalized = deepcopy(payload)
    if profile and not str(normalized.get("profile") or "").strip():
        normalized["profile"] = str(profile).strip()
    if "ok" not in normalized:
        normalized["ok"] = bool(normalized.get("enabled"))
    if "targets" not in normalized:
        normalized["targets"] = int(normalized.get("targets") or normalized.get("targetCount") or normalized.get("tabCount") or 0)
    normalized["error"] = str(normalized.get("error") or "").strip()
    return normalized


def load_browser_status_payload(openclaw_dir, env=None, config=None, browser_profiles_payload=None):
    env = env or openclaw_command_env(openclaw_dir)
    local_config = deepcopy(config) if isinstance(config, dict) else load_config(openclaw_dir)
    profiles_payload = normalize_browser_profiles_payload(browser_profiles_payload)
    errors = []
    for profile in browser_profile_candidate_names(local_config, profiles_payload):
        process = _run_command([*openclaw_browser_command(profile), "--json", "status"], env=env)
        parsed = normalize_browser_status_payload(parse_json_payload(process.stdout, process.stderr, default=None), profile=profile)
        if parsed is not None:
            return parsed
        output = join_command_output(process)
        if output:
            errors.append(output)
    process = _run_command(["openclaw", "browser", "--json", "status"], env=env)
    parsed = normalize_browser_status_payload(parse_json_payload(process.stdout, process.stderr, default=None))
    if parsed is not None:
        return parsed
    output = join_command_output(process)
    if output:
        errors.append(output)
    return {
        "ok": False,
        "running": False,
        "profile": "",
        "targets": 0,
        "error": errors[0] if errors else "browser_status_failed",
    }


def resolve_browser_command_profile(openclaw_dir, requested_profile="", config=None, browser_profiles_payload=None, env=None):
    normalized = str(requested_profile or "").strip()
    if normalized:
        return normalized
    env = env or openclaw_command_env(openclaw_dir)
    local_config = deepcopy(config) if isinstance(config, dict) else load_config(openclaw_dir)
    profiles_payload = normalize_browser_profiles_payload(browser_profiles_payload)
    if not profiles_payload.get("profiles"):
        profiles_payload, _process = load_browser_profiles_payload(openclaw_dir, env=env)
    selected_profile = resolve_available_browser_profile(
        openclaw_dir,
        config=local_config,
        browser_profiles_payload=profiles_payload,
    )
    if selected_profile:
        return selected_profile
    current_default = configured_browser_default_profile(local_config)
    if current_default:
        return current_default
    candidates = browser_profile_candidate_names(local_config, profiles_payload)
    return candidates[0] if candidates else ""


def join_command_output(process):
    return "\n".join(part.strip() for part in (process.stdout, process.stderr) if part and part.strip()).strip()


def top_counter_items(counter, limit=6):
    return [{"name": name, "count": count} for name, count in counter.most_common(limit)]


def normalize_openclaw_approvals_payload(payload, process):
    output = join_command_output(process)
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "exists": False,
            "path": "",
            "socketPath": "",
            "agentCount": 0,
            "ruleCount": 0,
            "agents": [],
            "error": output or "approvals_status_failed",
        }
    file_payload = payload.get("file", {}) if isinstance(payload.get("file"), dict) else {}
    agents_payload = file_payload.get("agents", {}) if isinstance(file_payload.get("agents"), dict) else {}
    agent_rows = []
    rule_count = 0
    for agent_id, item in agents_payload.items():
        if not isinstance(item, dict):
            continue
        allowlist = item.get("allowlist", []) if isinstance(item.get("allowlist"), list) else []
        count = len(allowlist)
        rule_count += count
        agent_rows.append(
            {
                "id": str(agent_id or "").strip(),
                "allowlistCount": count,
            }
        )
    defaults_payload = file_payload.get("defaults", {}) if isinstance(file_payload.get("defaults"), dict) else {}
    default_allowlist = defaults_payload.get("allowlist", []) if isinstance(defaults_payload.get("allowlist"), list) else []
    default_rule_count = len(default_allowlist)
    rule_count += default_rule_count
    return {
        "ok": bool(payload.get("exists")),
        "exists": bool(payload.get("exists")),
        "path": str(payload.get("path") or "").strip(),
        "socketPath": str(((file_payload.get("socket") or {}) if isinstance(file_payload.get("socket"), dict) else {}).get("path") or "").strip(),
        "agentCount": len(agent_rows),
        "ruleCount": rule_count,
        "defaultRuleCount": default_rule_count,
        "agents": sorted(agent_rows, key=lambda item: (-item.get("allowlistCount", 0), item.get("id", "")))[:8],
        "error": output if not payload.get("exists") else "",
    }


def normalize_openclaw_memory_payload(payload, process):
    output = join_command_output(process)
    def _coerce_int(value):
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0
    if isinstance(payload, dict):
        entries = payload.get("entries", [])
    elif isinstance(payload, list):
        entries = payload
    else:
        entries = []
    normalized_entries = []
    provider_counter = Counter()
    ready_count = 0
    for item in entries if isinstance(entries, list) else []:
        if not isinstance(item, dict):
            continue
        status_payload = item.get("status") if isinstance(item.get("status"), dict) else {}
        scan_payload = item.get("scan") if isinstance(item.get("scan"), dict) else {}
        agent_id = str(item.get("agentId") or item.get("id") or item.get("name") or "").strip()
        provider = str(
            item.get("provider")
            or item.get("engine")
            or item.get("embeddingProvider")
            or status_payload.get("provider")
            or status_payload.get("requestedProvider")
            or ""
        ).strip()
        indexed_files = max(0, _coerce_int(status_payload.get("files")))
        indexed_chunks = max(0, _coerce_int(status_payload.get("chunks")))
        discovered_files = max(0, _coerce_int(scan_payload.get("totalFiles")))
        cache_entries = max(0, _coerce_int(((status_payload.get("cache") or {}) if isinstance(status_payload.get("cache"), dict) else {}).get("entries")))
        vector_available = bool(((status_payload.get("vector") or {}) if isinstance(status_payload.get("vector"), dict) else {}).get("available"))
        fts_available = bool(((status_payload.get("fts") or {}) if isinstance(status_payload.get("fts"), dict) else {}).get("available"))
        explicit_available = item.get("available")
        if explicit_available is None:
            explicit_available = item.get("enabled")
        if explicit_available is None:
            explicit_available = item.get("ok")
        if explicit_available is None:
            explicit_available = item.get("healthy")
        if explicit_available is None:
            explicit_available = provider and (vector_available or fts_available or indexed_files > 0 or indexed_chunks > 0 or cache_entries > 0)
        available = bool(explicit_available)
        if provider:
            provider_counter[provider] += 1
        if available:
            ready_count += 1
        detail = str(item.get("detail") or item.get("reason") or "").strip()
        if not detail:
            detail_parts = []
            if indexed_files or indexed_chunks:
                detail_parts.append(f"{indexed_files} files")
                if indexed_chunks:
                    detail_parts.append(f"{indexed_chunks} chunks")
            elif discovered_files:
                detail_parts.append(f"{discovered_files} files discovered")
            if cache_entries:
                detail_parts.append(f"cache {cache_entries}")
            if vector_available:
                detail_parts.append("vector ready")
            if fts_available:
                detail_parts.append("fts ready")
            detail = " · ".join(detail_parts)
        normalized_entries.append(
            {
                "agentId": agent_id,
                "provider": provider,
                "available": available,
                "detail": detail,
            }
        )
    if process.returncode != 0:
        error = output or "memory_status_failed"
        return {
            "ok": False,
            "agentCount": len(normalized_entries),
            "readyCount": ready_count,
            "providers": top_counter_items(provider_counter),
            "entries": normalized_entries[:8],
            "error": error,
        }
    return {
        "ok": True,
        "agentCount": len(normalized_entries),
        "readyCount": ready_count,
        "providers": top_counter_items(provider_counter),
        "entries": normalized_entries[:8],
        "error": "",
    }


def _fallback_openclaw_memory_panel_data(openclaw_dir, error_message="未检测到 openclaw CLI。"):
    return {
        "deferred": False,
        "error": error_message,
        "memory": {
            "ok": False,
            "agentCount": 0,
            "readyCount": 0,
            "providers": [],
            "providerCount": 0,
            "entries": [],
            "liveDiagnosticsLoaded": True,
            "probeState": "error",
            "probeUpdatedAt": now_iso(),
            "error": error_message,
        },
    }


def load_openclaw_memory_panel_data(openclaw_dir, config=None, metadata=None):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()

    try:
        env = openclaw_command_env(openclaw_dir)
        memory_result = _run_command(["openclaw", "memory", "status", "--json"], env=env)
    except FileNotFoundError:
        return _fallback_openclaw_memory_panel_data(openclaw_dir)

    memory_payload = normalize_openclaw_memory_payload(
        parse_json_payload(memory_result.stdout, memory_result.stderr, default=None),
        memory_result,
    )
    memory_payload = memory_payload if isinstance(memory_payload, dict) else {}
    error = str(memory_payload.get("error") or "").strip()
    return {
        "deferred": False,
        "error": error,
        "memory": {
            **memory_payload,
            "providerCount": len(safe_list(memory_payload.get("providers"))),
            "liveDiagnosticsLoaded": True,
            "probeState": "ready" if not error else "error",
            "probeUpdatedAt": now_iso(),
        },
    }


def normalize_openclaw_mcp_payload(payload, process):
    output = join_command_output(process)
    if isinstance(payload, dict):
        raw_entries = payload.items()
    elif isinstance(payload, list):
        raw_entries = [(item.get("name") or item.get("id") or "", item) for item in payload if isinstance(item, dict)]
    else:
        raw_entries = []
    entries = []
    for name, item in raw_entries:
        if not isinstance(item, dict):
            item = {}
        normalized_name = str(name or item.get("name") or item.get("id") or "").strip()
        if not normalized_name:
            continue
        enabled = item.get("enabled")
        if enabled is None:
            enabled = item.get("disabled") is not True
        transport = str(item.get("transport") or item.get("mode") or item.get("type") or "").strip()
        tool_count = item.get("toolCount")
        if tool_count is None:
            tools = item.get("tools")
            tool_count = len(tools) if isinstance(tools, list) else 0
        entries.append(
            {
                "name": normalized_name,
                "enabled": bool(enabled),
                "transport": transport,
                "toolCount": int(tool_count or 0),
            }
        )
    enabled_count = sum(1 for item in entries if item.get("enabled"))
    return {
        "ok": process.returncode == 0,
        "serverCount": len(entries),
        "enabledCount": enabled_count,
        "entries": sorted(entries, key=lambda item: item.get("name", ""))[:12],
        "error": "" if process.returncode == 0 else (output or "mcp_list_failed"),
    }


def build_openclaw_tools_payload(browser_payload=None, native_skills_payload=None, mcp_payload=None):
    browser_payload = browser_payload if isinstance(browser_payload, dict) else {}
    native_skills_payload = native_skills_payload if isinstance(native_skills_payload, dict) else {}
    mcp_payload = mcp_payload if isinstance(mcp_payload, dict) else {}

    browser_ready = bool(browser_payload.get("ok") or browser_payload.get("running"))
    browser_profile = str(browser_payload.get("profile") or "").strip()
    browser_targets = max(0, int(browser_payload.get("targets") or 0))
    skill_count = max(0, int(native_skills_payload.get("eligible") or 0))
    sample_skills = [
        item
        for item in safe_list(native_skills_payload.get("sampleEligible"))
        if isinstance(item, dict) and str(item.get("title") or "").strip()
    ][:6]
    enabled_mcp_entries = [
        item
        for item in safe_list(mcp_payload.get("entries"))
        if isinstance(item, dict) and bool(item.get("enabled"))
    ]
    mcp_tool_count = sum(max(0, int(item.get("toolCount") or 0)) for item in enabled_mcp_entries)

    entries = []
    if browser_ready or browser_profile or browser_targets:
        browser_detail_parts = []
        if browser_profile:
            browser_detail_parts.append(f"profile {browser_profile}")
        if browser_targets:
            browser_detail_parts.append(f"{browser_targets} targets")
        entries.append(
            {
                "key": "browser-control",
                "name": "browser-control",
                "label": "Browser control",
                "kind": "browser",
                "ready": browser_ready,
                "count": 1 if browser_ready else 0,
                "detail": " · ".join(browser_detail_parts) or str(browser_payload.get("error") or "").strip(),
                "source": browser_profile or "browser",
            }
        )
    for item in sample_skills:
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        entries.append(
            {
                "key": f"skill:{title}",
                "name": title,
                "label": title,
                "kind": "skill",
                "ready": True,
                "count": 1,
                "detail": str(item.get("detail") or "").strip(),
                "source": str(item.get("meta") or "skill").strip(),
            }
        )
    for item in enabled_mcp_entries[:6]:
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        tool_count = max(0, int(item.get("toolCount") or 0))
        transport = str(item.get("transport") or "").strip()
        entries.append(
            {
                "key": f"mcp:{name}",
                "name": name,
                "label": name,
                "kind": "mcp",
                "ready": True,
                "count": tool_count,
                "detail": f"{tool_count} tools" if tool_count else (transport or "MCP"),
                "source": transport or "mcp",
            }
        )

    total_count = (1 if browser_ready else 0) + skill_count + mcp_tool_count
    return {
        "ok": total_count > 0,
        "totalCount": total_count,
        "browserReady": browser_ready,
        "browserProfile": browser_profile,
        "browserSurfaceCount": 1 if browser_ready else 0,
        "skillCount": skill_count,
        "mcpToolCount": mcp_tool_count,
        "serverCount": len(enabled_mcp_entries),
        "entryCount": len(entries),
        "entries": entries,
        "error": "",
    }


def load_openclaw_control_data(openclaw_dir):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    installation_payload = _load_openclaw_installation_payload(openclaw_dir)
    runtime_sync_payload = load_runtime_sync_payload(openclaw_dir)

    def build():
        env = openclaw_command_env(openclaw_dir)
        local_config = load_config(openclaw_dir)
        try:
            version_result = _run_command(["openclaw", "--version"], env=env)
        except FileNotFoundError:
            local_runtime_payload = load_local_runtime_payload(openclaw_dir, config=local_config)
            execution_architecture_payload = load_model_execution_architecture_payload(openclaw_dir, config=local_config)
            return {
                "supported": False,
                "error": "未检测到 openclaw CLI。",
                "version": {"raw": "unknown", "release": "", "build": ""},
                "config": {"valid": False, "path": str(openclaw_dir / "openclaw.json"), "error": "missing_cli"},
                "gateway": {"ok": False, "channels": [], "agentCount": 0, "defaultAgentId": "", "error": "missing_cli"},
                "nativeSkills": {
                    "total": 0,
                    "eligible": 0,
                    "disabled": 0,
                    "blocked": 0,
                    "bundled": 0,
                    "external": 0,
                    "sampleEligible": [],
                    "sampleMissing": [],
                    "missingBins": [],
                    "missingEnv": [],
                    "missingConfig": [],
                    "sourceBreakdown": [],
                    "warnings": [],
                },
                "approvals": {
                    "ok": False,
                    "exists": False,
                    "path": "",
                    "socketPath": "",
                    "agentCount": 0,
                    "ruleCount": 0,
                    "defaultRuleCount": 0,
                    "agents": [],
                    "error": "missing_cli",
                },
                "memory": {
                    "ok": False,
                    "agentCount": 0,
                    "readyCount": 0,
                    "providers": [],
                    "entries": [],
                    "error": "missing_cli",
                },
                "mcp": {
                    "ok": False,
                    "serverCount": 0,
                    "enabledCount": 0,
                    "entries": [],
                    "error": "missing_cli",
                },
                "tools": {
                    "ok": False,
                    "totalCount": 0,
                    "browserReady": False,
                    "browserProfile": "",
                    "browserSurfaceCount": 0,
                    "skillCount": 0,
                    "mcpToolCount": 0,
                    "serverCount": 0,
                    "entryCount": 0,
                    "entries": [],
                    "error": "missing_cli",
                },
                "installation": installation_payload,
                "runtimeSync": runtime_sync_payload,
                "localRuntime": local_runtime_payload,
                "executionArchitecture": execution_architecture_payload,
                "compatibility": [],
                "commands": [],
                "_nativeSkillNames": [],
            }

        version_raw = (version_result.stdout or version_result.stderr or "").strip()
        release_text = ""
        build_text = ""
        if version_raw.startswith("OpenClaw "):
            after_name = version_raw.split("OpenClaw ", 1)[1]
            if " (" in after_name and after_name.endswith(")"):
                release_text, build_text = after_name[:-1].split(" (", 1)
            else:
                release_text = after_name

        config_result = _run_command(["openclaw", "config", "validate", "--json"], env=env)
        config_payload = parse_json_payload(config_result.stdout, config_result.stderr, default=None)
        if config_payload is None:
            config_payload = {
                "valid": False,
                "path": str(openclaw_dir / "openclaw.json"),
                "error": (config_result.stderr or config_result.stdout or "config_validate_failed").strip(),
            }

        health_result = _run_command(["openclaw", "gateway", "health", "--json"], env=env)
        health_payload = parse_json_payload(health_result.stdout, health_result.stderr, default=None)
        if health_payload is None:
            health_payload = {
                "ok": False,
                "error": (health_result.stderr or health_result.stdout or "gateway_health_failed").strip(),
                "channels": {},
                "agents": [],
            }

        gateway_status_result = _run_command(["openclaw", "gateway", "status", "--require-rpc", "--json"], env=env)
        gateway_status_payload = parse_json_payload(gateway_status_result.stdout, gateway_status_result.stderr, default=None)
        if gateway_status_payload is None:
            gateway_status_payload = {
                "service": {"runtime": {"status": "unknown"}},
                "gateway": {"bindMode": "", "port": None, "probeUrl": ""},
                "rpc": {
                    "ok": False,
                    "error": (gateway_status_result.stderr or gateway_status_result.stdout or "gateway_status_failed").strip(),
                    "url": "",
                },
                "config": {},
            }
        rpc_payload = gateway_status_payload.get("rpc", {}) if isinstance(gateway_status_payload, dict) else {}
        service_payload = gateway_status_payload.get("service", {}) if isinstance(gateway_status_payload, dict) else {}
        gateway_runtime_payload = gateway_status_payload.get("gateway", {}) if isinstance(gateway_status_payload, dict) else {}
        rpc_ok = bool(rpc_payload.get("ok"))

        browser_status_payload = {
            "ok": False,
            "running": False,
            "profile": "",
            "targets": 0,
            "error": rpc_payload.get("error", "") if isinstance(rpc_payload, dict) else "",
        }
        browser_profiles_payload = {"profiles": []}
        if rpc_ok:
            browser_profiles_payload, _browser_profiles_result = load_browser_profiles_payload(openclaw_dir, env=env)
            selected_profile = resolve_available_browser_profile(
                openclaw_dir,
                config=local_config,
                browser_profiles_payload=browser_profiles_payload,
            )
            browser_status_payload = load_browser_status_payload(
                openclaw_dir,
                env=env,
                config=local_config,
                browser_profiles_payload=browser_profiles_payload,
            )
            if selected_profile and not str(browser_status_payload.get("profile") or "").strip():
                browser_status_payload["profile"] = selected_profile

        skills_result = _run_command(["openclaw", "skills", "list", "--json"], env=env)
        native_skills_payload = parse_json_payload(skills_result.stdout, skills_result.stderr, default={"skills": []})
        native_skill_entries = native_skills_payload.get("skills", []) if isinstance(native_skills_payload, dict) else []
        managed_skills_dir = native_skills_payload.get("managedSkillsDir", "") if isinstance(native_skills_payload, dict) else ""
        workspace_dir = native_skills_payload.get("workspaceDir", "") if isinstance(native_skills_payload, dict) else ""
        skills_check_result = _run_command(["openclaw", "skills", "check", "--json"], env=env)
        skills_check_payload = parse_json_payload(skills_check_result.stdout, skills_check_result.stderr, default={"summary": {}, "missingRequirements": []})
        if not isinstance(skills_check_payload, dict):
            skills_check_payload = {"summary": {}, "missingRequirements": []}
        approvals_result = _run_command(["openclaw", "approvals", "get", "--json"], env=env)
        approvals_payload = normalize_openclaw_approvals_payload(
            parse_json_payload(approvals_result.stdout, approvals_result.stderr, default=None),
            approvals_result,
        )
        memory_result = _run_command(["openclaw", "memory", "status", "--json"], env=env)
        memory_payload = normalize_openclaw_memory_payload(
            parse_json_payload(memory_result.stdout, memory_result.stderr, default=None),
            memory_result,
        )
        mcp_result = _run_command(["openclaw", "mcp", "list", "--json"], env=env)
        mcp_payload = normalize_openclaw_mcp_payload(
            parse_json_payload(mcp_result.stdout, mcp_result.stderr, default=None),
            mcp_result,
        )

        source_counter = Counter(item.get("source", "unknown") for item in native_skill_entries)
        missing_bins = Counter()
        missing_env = Counter()
        missing_config = Counter()
        sample_eligible = []
        sample_missing = []
        native_skill_names = []
        bundled = 0
        external = 0
        disabled = 0
        blocked = 0
        eligible = 0
        for item in native_skill_entries:
            name = item.get("name", "")
            if name:
                native_skill_names.append(name)
            if item.get("bundled"):
                bundled += 1
            else:
                external += 1
            if item.get("disabled"):
                disabled += 1
            if item.get("blockedByAllowlist"):
                blocked += 1
            if item.get("eligible"):
                eligible += 1
                if len(sample_eligible) < 8:
                    sample_eligible.append(
                        {
                            "title": item.get("name", "unknown"),
                            "meta": f"{item.get('source', 'unknown')} · {'bundled' if item.get('bundled') else 'external'}",
                            "detail": item.get("description", ""),
                        }
                    )
            missing = item.get("missing", {}) if isinstance(item.get("missing"), dict) else {}
            for bin_name in missing.get("bins", []) or []:
                missing_bins[bin_name] += 1
            for env_name in missing.get("env", []) or []:
                missing_env[env_name] += 1
            for config_name in missing.get("config", []) or []:
                missing_config[config_name] += 1
            if not item.get("eligible") and len(sample_missing) < 8:
                reasons = []
                if missing.get("bins"):
                    reasons.append(f"缺少命令: {', '.join(missing.get('bins', [])[:2])}")
                if missing.get("env"):
                    reasons.append(f"缺少环境变量: {', '.join(missing.get('env', [])[:2])}")
                if missing.get("config"):
                    reasons.append(f"缺少配置: {', '.join(missing.get('config', [])[:2])}")
                sample_missing.append(
                    {
                        "title": item.get("name", "unknown"),
                        "meta": item.get("source", "unknown"),
                        "detail": " · ".join(reasons) or item.get("description", ""),
                    }
                )

        browser_profiles = browser_profiles_payload.get("profiles", []) if isinstance(browser_profiles_payload, dict) else []
        normalized_browser_profiles = []
        preferred_profile_names = {"openclaw", "user"}
        browser_profile_names = set()
        for item in browser_profiles:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or item.get("profile") or item.get("id") or "").strip()
            if not name:
                continue
            browser_profile_names.add(name)
            normalized_browser_profiles.append(
                {
                    "name": name,
                    "running": bool(item.get("running")),
                    "detail": item.get("description") or item.get("path") or item.get("label") or "",
                }
            )
        recommended_profiles = [
            {
                "name": "openclaw",
                "title": "openclaw",
                "detail": "产品内置的无头浏览器 profile，适合容器和默认安装场景，开箱即可直接打开网页。",
                "available": "openclaw" in browser_profile_names,
            },
            {
                "name": "user",
                "title": "user",
                "detail": "复用用户已登录的本地 Chrome / Chromium 会话，适合真实业务站点联调。",
                "available": "user" in browser_profile_names,
            },
        ]
        local_runtime_payload = load_local_runtime_payload(openclaw_dir, config=local_config)

        channel_entries = []
        health_channels = health_payload.get("channels", {}) if isinstance(health_payload, dict) else {}
        channel_order = health_payload.get("channelOrder", []) if isinstance(health_payload, dict) else []
        health_labels = health_payload.get("channelLabels", {}) if isinstance(health_payload, dict) else {}
        ordered_names = channel_order or list(health_channels.keys())
        for channel_name in ordered_names:
            channel = health_channels.get(channel_name, {})
            probe = channel.get("probe", {}) if isinstance(channel.get("probe"), dict) else {}
            detail = ""
            if channel_name == "telegram" and isinstance(probe.get("bot"), dict):
                detail = probe["bot"].get("username", "")
            elif channel_name == "feishu":
                detail = probe.get("appId", "")
            channel_entries.append(
                {
                    "title": health_labels.get(channel_name, channel_name),
                    "meta": "configured" if channel.get("configured") else "not configured",
                    "detail": detail or channel.get("lastError") or "无额外信息",
                    "healthy": bool(probe.get("ok")) if probe else bool(channel.get("configured")),
                    "running": bool(channel.get("running")),
                }
            )

        agent_params = []
        config_agents = ((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {}).get("list", [])
        for agent in config_agents if isinstance(config_agents, list) else []:
            if not isinstance(agent, dict):
                continue
            params = agent.get("params", {})
            if not params:
                params = {}
            agent_params.append(
                {
                    "id": agent.get("id", ""),
                    "workspace": agent.get("workspace", ""),
                    "params": params,
                    "summary": ", ".join(f"{key}={value}" for key, value in list(params.items())[:5]) if params else "",
                }
            )

        topology = _build_openclaw_model_topology(openclaw_dir, config=local_config)
        model_assignments = deepcopy(topology.get("providerAssignments") if isinstance(topology.get("providerAssignments"), list) else [])
        model_inventory = deepcopy(topology.get("modelInventory") if isinstance(topology.get("modelInventory"), list) else [])
        provider_rows = deepcopy(topology.get("providerRows") if isinstance(topology.get("providerRows"), list) else [])
        router_assignment = deepcopy(topology.get("routerAssignment") if isinstance(topology.get("routerAssignment"), dict) else {})
        model_counter = Counter(
            str(item.get("model") or "").strip()
            for item in model_assignments
            if str(item.get("model") or "").strip()
        )

        execution_architecture_payload = load_model_execution_architecture_payload(
            openclaw_dir,
            config=local_config,
            include_local_runtime_capabilities=True,
            include_model_access_matrix=True,
            local_runtime_payload=local_runtime_payload,
            topology=topology,
        )

        tools_payload = build_openclaw_tools_payload(
            browser_status_payload,
            {"eligible": eligible, "sampleEligible": sample_eligible},
            mcp_payload,
        )

        compatibility = [
            {
                "title": "OpenClaw 版本",
                "status": "ready" if is_supported_openclaw_release(release_text) else "warning",
                "body": version_raw or "unknown",
                "meta": f"当前产品按 OpenClaw {_openclaw_baseline_release()}+ 适配。",
            },
            {
                "title": "配置校验",
                "status": "ready" if config_payload.get("valid") else "error",
                "body": "openclaw.json 已通过 schema 校验。" if config_payload.get("valid") else "当前配置未通过 schema 校验。",
                "meta": config_payload.get("path") or str(openclaw_dir / "openclaw.json"),
            },
            {
                "title": "Gateway 健康",
                "status": "ready" if health_payload.get("ok") else "warning",
                "body": "Gateway 健康检查通过。" if health_payload.get("ok") else "Gateway 健康检查失败或未返回结构化结果。",
                "meta": f"channel {len(channel_entries)} · agent {len(health_payload.get('agents', []) or [])}",
            },
            {
                "title": "Gateway RPC",
                "status": "ready" if rpc_ok else "warning",
                "body": "Gateway RPC 严格检查通过。" if rpc_ok else "新版 `gateway status --require-rpc` 未通过，浏览器和部分实时控制能力会受影响。",
                "meta": rpc_payload.get("url") or gateway_runtime_payload.get("probeUrl") or "no rpc url",
            },
            {
                "title": "原生 Skills",
                "status": "ready" if native_skill_entries else "warning",
                "body": f"OpenClaw 当前识别到 {len(native_skill_entries)} 个原生 skills，其中 {eligible} 个可直接使用。",
                "meta": f"bundled {bundled} · external {external}",
            },
            {
                "title": "Managed Skills 目录",
                "status": "ready" if managed_skills_dir else "warning",
                "body": managed_skills_dir or "当前没有返回 managed skills 目录。",
                "meta": workspace_dir or str(openclaw_dir),
            },
            {
                "title": "Browser Live Session",
                "status": "ready" if rpc_ok and preferred_profile_names.intersection(browser_profile_names) else "warning",
                "body": "浏览器 live session/profile 能力已可用。" if rpc_ok and preferred_profile_names.intersection(browser_profile_names) else "建议先补齐内置或本地 browser profile，并确认 RPC 在线，才能稳定接入浏览器能力。",
                "meta": ", ".join(sorted(browser_profile_names)) if browser_profile_names else "recommended: openclaw, user",
            },
            {
                "title": "Exec Approvals",
                "status": "ready" if approvals_payload.get("ok") else "warning",
                "body": (
                    f"审批规则文件已加载，覆盖 {approvals_payload.get('agentCount') or 0} 个 agent。"
                    if approvals_payload.get("ok")
                    else "当前没有拿到审批规则文件，命令级审批面可能还没准备好。"
                ),
                "meta": approvals_payload.get("path") or "no approvals file",
            },
            {
                "title": "Memory Runtime",
                "status": "ready" if memory_payload.get("ok") else "warning",
                "body": (
                    f"记忆状态已返回，{memory_payload.get('readyCount') or 0}/{memory_payload.get('agentCount') or 0} 个 agent 记忆可用。"
                    if memory_payload.get("ok")
                    else "记忆运行时没有返回结构化状态，通常意味着 embeddings/provider 还需要补齐。"
                ),
                "meta": ", ".join(item.get("name", "") for item in memory_payload.get("providers", [])[:3]) or (memory_payload.get("error") or "no providers"),
            },
            {
                "title": "MCP Servers",
                "status": "ready" if mcp_payload.get("serverCount") else "default",
                "body": (
                    f"当前注册了 {mcp_payload.get('serverCount') or 0} 个 MCP server。"
                    if mcp_payload.get("serverCount")
                    else "当前没有注册 MCP server。"
                ),
                "meta": f"enabled {mcp_payload.get('enabledCount') or 0}/{mcp_payload.get('serverCount') or 0}",
            },
            {
                "title": "Available Tools",
                "status": "ready" if tools_payload.get("ok") else "default",
                "body": (
                    f"当前可直接使用 {tools_payload.get('totalCount') or 0} 个工具面。"
                    if tools_payload.get("ok")
                    else "当前还没有稳定可见的工具面。"
                ),
                "meta": (
                    f"browser {tools_payload.get('browserSurfaceCount') or 0}"
                    f" · skills {tools_payload.get('skillCount') or 0}"
                    f" · mcp {tools_payload.get('mcpToolCount') or 0}"
                ),
            },
            {
                "title": "OpenClaw Team Runtime Sync",
                "status": "ready" if runtime_sync_payload.get("ok") else ("warning" if runtime_sync_payload.get("state") == "partial" else "error"),
                "body": "产品运行时脚本、skills 和前端产物已同步到当前 OpenClaw 安装。" if runtime_sync_payload.get("ok") else "建议执行一次运行时同步，确保安装、skills 和前端产物与当前仓库保持一致。",
                "meta": f"workspace {((runtime_sync_payload.get('workspaceCoverage') or {}).get('ready') or 0)}/{((runtime_sync_payload.get('workspaceCoverage') or {}).get('total') or 0)} · skills {((runtime_sync_payload.get('skills') or {}).get('count') or 0)}",
            },
            {
                "title": "Agent Auth",
                "status": (
                    "ready"
                    if ((runtime_sync_payload.get("auth") or {}).get("ok"))
                    else ("warning" if ((runtime_sync_payload.get("auth") or {}).get("state") == "partial") else "error")
                ),
                "body": "所有 agent 均已具备 auth-profiles.json，团队通信和任务执行可以直接落到真实模型。" if ((runtime_sync_payload.get("auth") or {}).get("ok")) else "缺少 agent 鉴权时，团队通信会退回 embedded 并在 provider auth 上失败。",
                "meta": f"auth {((runtime_sync_payload.get('auth') or {}).get('readyCount') or 0)}/{((runtime_sync_payload.get('auth') or {}).get('targetCount') or 0)}",
            },
            {
                "title": "Local Runtime / KV Cache",
                "status": "ready" if local_runtime_payload.get("running") else ("warning" if local_runtime_payload.get("enabled") else "default"),
                "body": "本地 llama.cpp / TurboQuant 风格运行时已经有完整启动接缝。" if local_runtime_payload.get("configured") else "可以在本地 runtime 配置里补 entrypoint、模型路径和 KV cache 类型。",
                "meta": local_runtime_payload.get("commandPreview") or local_runtime_payload.get("backend") or "disabled",
            },
        ]

        env_prefix = f'OPENCLAW_STATE_DIR="{openclaw_dir}" OPENCLAW_CONFIG_PATH="{openclaw_dir / "openclaw.json"}"'
        return {
            "supported": True,
            "error": "",
            "version": {"raw": version_raw, "release": release_text, "build": build_text},
            "installation": installation_payload,
            "runtimeSync": runtime_sync_payload,
            "localRuntime": local_runtime_payload,
            "executionArchitecture": execution_architecture_payload,
            "config": config_payload,
            "gateway": {
                "ok": bool(health_payload.get("ok")),
                "durationMs": health_payload.get("durationMs"),
                "defaultAgentId": health_payload.get("defaultAgentId", ""),
                "agentCount": len(health_payload.get("agents", []) or []),
                "channels": channel_entries,
                "error": health_payload.get("error", ""),
                "rpc": {
                    "ok": rpc_ok,
                    "url": rpc_payload.get("url", ""),
                    "error": rpc_payload.get("error", ""),
                    "serviceStatus": (((service_payload.get("runtime", {}) if isinstance(service_payload.get("runtime"), dict) else {}) or {}).get("status", "")),
                    "bindMode": gateway_runtime_payload.get("bindMode", ""),
                    "port": gateway_runtime_payload.get("port"),
                    "probeUrl": gateway_runtime_payload.get("probeUrl", ""),
                },
            },
            "browser": {
                "ok": bool(browser_status_payload.get("ok")) if isinstance(browser_status_payload, dict) else False,
                "running": bool(browser_status_payload.get("running")) if isinstance(browser_status_payload, dict) else False,
                "profile": browser_status_payload.get("profile", "") if isinstance(browser_status_payload, dict) else "",
                "targets": browser_status_payload.get("targets", 0) if isinstance(browser_status_payload, dict) else 0,
                "error": browser_status_payload.get("error", "") if isinstance(browser_status_payload, dict) else "",
                "profiles": normalized_browser_profiles,
                "recommendedProfiles": recommended_profiles,
            },
            "models": {
                "summary": {
                    "distinctModels": len(model_counter),
                    "assignedAgents": sum(1 for item in model_assignments if item.get("model")),
                    "totalAgents": len(model_assignments),
                    "readyProviders": sum(1 for item in provider_rows if item.get("configured")),
                    "routerModel": router_assignment.get("model", ""),
                    "routerProvider": router_assignment.get("provider", ""),
                },
                "assignments": model_assignments,
                "inventory": model_inventory,
                "providers": provider_rows,
                "catalog": [item["name"] for item in model_inventory],
            },
            "nativeSkills": {
                "total": len(native_skill_entries),
                "eligible": eligible,
                "disabled": disabled,
                "blocked": blocked,
                "bundled": bundled,
                "external": external,
                "managedSkillsDir": managed_skills_dir,
                "workspaceDir": workspace_dir,
                "sampleEligible": sample_eligible,
                "sampleMissing": sample_missing,
                "missingBins": top_counter_items(missing_bins),
                "missingEnv": top_counter_items(missing_env),
                "missingConfig": top_counter_items(missing_config),
                "sourceBreakdown": top_counter_items(source_counter),
                "warnings": [line.strip() for line in (skills_result.stderr or "").splitlines() if line.strip()],
                "check": {
                    "summary": skills_check_payload.get("summary", {}),
                    "missingRequirements": skills_check_payload.get("missingRequirements", []),
                    "warnings": [line.strip() for line in (skills_check_result.stderr or "").splitlines() if line.strip()],
                },
            },
            "approvals": approvals_payload,
            "memory": memory_payload,
            "mcp": mcp_payload,
            "tools": tools_payload,
            "agentParams": agent_params,
            "compatibility": compatibility,
            "commands": [
                {
                    "label": "OpenClaw Dashboard",
                    "command": f"{env_prefix} openclaw dashboard --no-open",
                    "description": "输出官方 Control UI 地址，不在浏览器里自动打开。",
                },
                {
                    "label": "Schema 校验",
                    "command": f"{env_prefix} openclaw config validate --json",
                    "description": "校验当前安装目录里的 openclaw.json 是否仍然有效。",
                },
                {
                    "label": "Gateway 健康",
                    "command": f"{env_prefix} openclaw gateway health --json",
                    "description": "获取当前 Gateway、channels、agents 的结构化健康数据。",
                },
                {
                    "label": "Gateway RPC 严格检查",
                    "command": f"{env_prefix} openclaw gateway status --require-rpc --json",
                    "description": "检查新版 Gateway RPC 是否真正在线，适合排查浏览器与实时控制链路。",
                },
                {
                    "label": "Browser Status",
                    "command": f"{env_prefix} openclaw browser --json status",
                    "description": "查看新版浏览器运行态、当前 profile 和实时 attach 状态。",
                },
                {
                    "label": "Browser Profiles",
                    "command": f"{env_prefix} openclaw browser --json profiles",
                    "description": "查看可用的浏览器 profiles，默认安装优先检查内置 `openclaw`，本机接管再看 `user`。",
                },
                {
                    "label": "Exec Approvals",
                    "command": f"{env_prefix} openclaw approvals get --json",
                    "description": "查看命令审批规则文件、socket 和 agent allowlist 状态。",
                },
                {
                    "label": "Memory Status",
                    "command": f"{env_prefix} openclaw memory status --json",
                    "description": "查看当前记忆 provider、agent 记忆可用性以及 embeddings 运行状态。",
                },
                {
                    "label": "MCP Servers",
                    "command": f"{env_prefix} openclaw mcp list --json",
                    "description": "查看当前注册的 MCP servers 和启用状态。",
                },
                {
                    "label": "原生 Skills",
                    "command": f"{env_prefix} openclaw skills list --json",
                    "description": "查看 OpenClaw 当前可识别的 skills 目录和可用性。",
                },
                {
                    "label": "Doctor",
                    "command": f"{env_prefix} openclaw doctor --non-interactive",
                    "description": "运行官方健康检查与修复建议流程。",
                },
                {
                    "label": "Onboard",
                    "command": f"{env_prefix} openclaw onboard",
                    "description": "进入官方 onboarding wizard，配置 gateway、workspace 和 skills。",
                },
                {
                    "label": "Sync OpenClaw Team Runtime",
                    "command": f'bash {runtime_sync_payload.get("scriptPath") or "platform/bin/runtime/sync_runtime_assets.sh"} --dir {openclaw_dir} --project-dir {runtime_sync_payload.get("projectDir") or "."} --build-frontend',
                    "description": "把当前仓库的脚本、skills 和前端产物同步进这套 OpenClaw 安装。",
                },
                {
                    "label": "Preview Local Runtime Command",
                    "command": f'python3 {local_runtime_payload.get("launcherPath") or "platform/bin/runtime/launch_local_model_runtime.py"} --json --print-command',
                    "description": "预览本地 llama.cpp / TurboQuant 风格 runtime 的完整启动命令。",
                },
            ],
            "_nativeSkillNames": native_skill_names,
        }

    return cached_payload(("openclaw-control", str(openclaw_dir)), 30, build)


def perform_openclaw_provider_setup(
    openclaw_dir,
    provider_id,
    api_key,
    sync_auth=True,
    rollout_model="",
    rollout_scope="none",
    rollout_agent_id="",
    set_preferred_provider=True,
):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    normalized_provider_id = str(provider_id or "").strip().lower()
    normalized_key = str(api_key or "").strip()
    normalized_rollout_model = str(rollout_model or "").strip()
    normalized_rollout_scope = str(rollout_scope or "none").strip().lower() or "none"
    normalized_rollout_agent_id = str(rollout_agent_id or "").strip()
    if not normalized_provider_id:
        raise RuntimeError("请选择要配置的模型供应商。")
    if not normalized_key:
        raise RuntimeError("请输入模型供应商的 API Key。")
    provider = next((item for item in _model_provider_catalog() if str(item.get("id") or "").strip().lower() == normalized_provider_id), None)
    if not isinstance(provider, dict):
        raise RuntimeError("当前不支持这个模型供应商。")
    env_keys = [str(item or "").strip() for item in provider.get("env", ()) if str(item or "").strip()]
    if not env_keys:
        raise RuntimeError("这个模型供应商当前没有可写入的环境变量。")
    saved_config = store_save_model_provider_config(
        openclaw_dir,
        {
            "providerId": normalized_provider_id,
            "providerLabel": str(provider.get("label") or normalized_provider_id).strip(),
            "keyValue": normalized_key,
            "envKeys": env_keys,
            "status": "active",
        },
    )
    auth_result = {}
    if sync_auth:
        auth_result = perform_openclaw_auth_sync(openclaw_dir, overwrite=True)
    execution_result = {}
    if set_preferred_provider:
        execution_result = perform_openclaw_execution_configure(
            openclaw_dir,
            preferred_provider_id=normalized_provider_id,
        )
    rollout_result = {}
    if normalized_rollout_scope not in {"", "none"}:
        if not normalized_rollout_model:
            raise RuntimeError("请选择要同步给团队的目标模型。")
        inferred_provider_id = str(infer_model_provider(normalized_rollout_model).get("id") or "").strip().lower()
        if inferred_provider_id and inferred_provider_id not in {"custom", "unassigned"} and inferred_provider_id != normalized_provider_id:
            raise RuntimeError("目标模型和当前供应商不匹配，请检查模型名称。")
        rollout_result = perform_model_rollout(
            openclaw_dir,
            model_name=normalized_rollout_model,
            scope=normalized_rollout_scope,
            agent_id=normalized_rollout_agent_id,
        )
    return {
        "providerId": normalized_provider_id,
        "providerLabel": str(provider.get("label") or normalized_provider_id).strip(),
        "envKeys": env_keys,
        "storage": "database",
        "saved": True,
        "syncAuth": bool(sync_auth),
        "authSync": auth_result,
        "setPreferredProvider": bool(set_preferred_provider),
        "execution": execution_result,
        "rollout": rollout_result,
        "updatedAt": str((saved_config or {}).get("updatedAt") or now_iso()).strip(),
    }


def perform_openclaw_execution_configure(
    openclaw_dir,
    primary_path="",
    fallback_path="",
    context_mode="",
    local_runtime_role="",
    preferred_provider_id="",
    hosted_provider_context_budget_policy="",
    transport="",
):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    metadata = load_project_metadata(openclaw_dir)
    current_execution = load_execution_config(openclaw_dir, metadata=metadata)
    preferred_provider_id = str(preferred_provider_id or "").strip()
    if preferred_provider_id:
        provider_ids = {
            str(item.get("id") or "").strip().lower()
            for item in _model_provider_catalog()
            if isinstance(item, dict)
            and str(item.get("id") or "").strip()
        }
        if preferred_provider_id.lower() not in provider_ids:
            raise RuntimeError("请选择已有的模型供应商。")
    execution = normalize_execution_config(
        {
            **current_execution,
            "transport": transport or current_execution.get("transport") or DEFAULT_EXECUTION_TRANSPORT,
            "primaryPath": primary_path or current_execution.get("primaryPath") or DEFAULT_EXECUTION_PRIMARY_PATH,
            "fallbackPath": fallback_path or current_execution.get("fallbackPath") or DEFAULT_EXECUTION_FALLBACK_PATH,
            "contextMode": context_mode or current_execution.get("contextMode") or DEFAULT_EXECUTION_CONTEXT_MODE,
            "localRuntimeRole": local_runtime_role or current_execution.get("localRuntimeRole") or DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE,
            "preferredProviderId": preferred_provider_id or current_execution.get("preferredProviderId") or "",
            "hostedProviderContextBudgetPolicy": hosted_provider_context_budget_policy
            or current_execution.get("hostedProviderContextBudgetPolicy")
            or DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY,
        }
    )
    execution["updatedAt"] = now_iso()
    metadata[EXECUTION_CONFIG_KEY] = execution
    save_project_metadata(openclaw_dir, metadata)
    result = load_model_execution_architecture_payload(openclaw_dir, metadata=metadata)
    result["saved"] = True
    result["storage"] = "mission-control.json"
    result["execution"] = execution
    result["updatedAt"] = execution["updatedAt"]
    return result


def perform_openclaw_speech_runtime_configure(
    openclaw_dir,
    provider="",
    base_url="",
    model="",
    api_key_env="",
):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    metadata = load_project_metadata(openclaw_dir)
    current_runtime = load_speech_runtime_config(openclaw_dir, metadata=metadata)
    next_runtime = normalize_speech_runtime_config(
        {
            **current_runtime,
            "provider": provider or current_runtime.get("provider") or DEFAULT_SPEECH_RUNTIME_PROVIDER,
            "baseUrl": base_url if base_url is not None else current_runtime.get("baseUrl"),
            "model": model or current_runtime.get("model") or DEFAULT_SPEECH_RUNTIME_MODEL,
            "apiKeyEnv": api_key_env or current_runtime.get("apiKeyEnv") or DEFAULT_SPEECH_RUNTIME_API_KEY_ENV,
        }
    )
    metadata[SPEECH_RUNTIME_CONFIG_KEY] = {
        **next_runtime,
        "updatedAt": now_iso(),
    }
    save_project_metadata(openclaw_dir, metadata)
    result = load_openclaw_voice_workflow_panel_data(openclaw_dir, metadata=metadata)
    result["saved"] = True
    result["storage"] = "mission-control.json"
    result["speechRuntime"] = next_runtime
    result["updatedAt"] = metadata[SPEECH_RUNTIME_CONFIG_KEY]["updatedAt"]
    return result


def perform_openclaw_speech_runtime_use_local_preset(openclaw_dir):
    return perform_openclaw_speech_runtime_configure(
        openclaw_dir,
        provider="sherpa_onnx",
        base_url=recommended_sherpa_onnx_runtime_base_url(),
        model=DEFAULT_SHERPA_ONNX_RUNTIME_MODEL,
        api_key_env=DEFAULT_SHERPA_ONNX_RUNTIME_API_KEY_ENV,
    )


def perform_openclaw_auth_sync(openclaw_dir, overwrite=True):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    script_path = openclaw_auth_sync_script_path(openclaw_dir)
    env = openclaw_command_env(openclaw_dir)
    args = ["python3", str(script_path), "--dir", str(openclaw_dir), "--json"]
    if overwrite:
        args.append("--overwrite")
    process = _run_command(args, env=env)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if not isinstance(payload, dict):
        raise RuntimeError(join_command_output(process) or "Agent auth 同步失败。")
    payload["command"] = " ".join(args)
    return payload


def backfill_model_provider_configs_from_auth(openclaw_dir):
    """Backfill model_provider_configs DB table from existing auth-profiles.json files.

    On startup, auth-profiles.json may contain valid API keys (set via
    ``sync_agent_auth.py`` copy-based sync or manual edits) that were never
    saved to the DB through the UI flow.  This causes
    ``provider_config_env_map()`` to return an empty dict, breaking model
    routing.

    This function reads the first available auth-profiles.json (preferring
    ``assistant`` → ``main`` → any agent), maps each profile entry to the
    MODEL_PROVIDER_CATALOG, and inserts missing entries into the DB using
    ``store_save_model_provider_config``.

    Returns a dict summarising what was backfilled.
    """
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()

    # ── 1. Determine which providers are already in the DB ──────────────
    try:
        existing_configs = store_list_model_provider_configs(openclaw_dir)
    except Exception:
        existing_configs = []
    existing_ids = {
        str(cfg.get("providerId") or "").strip().lower()
        for cfg in (existing_configs or [])
        if isinstance(cfg, dict)
    }

    # ── 2. Load the auth-profiles.json (discover from agents) ───────────
    agents_dir = openclaw_dir / "agents"
    auth_payload = None
    source_agent = ""
    preferred_ids = ("assistant", "main")
    try:
        # Check preferred agents first
        for agent_id in preferred_ids:
            candidate = agents_dir / agent_id / "agent" / "auth-profiles.json"
            if candidate.exists():
                data = json.loads(candidate.read_text(encoding="utf-8"))
                profiles = data.get("profiles")
                if isinstance(profiles, dict) and profiles:
                    auth_payload = data
                    source_agent = agent_id
                    break
        # Fallback: scan all agent directories
        if auth_payload is None and agents_dir.is_dir():
            for entry in sorted(agents_dir.iterdir()):
                candidate = entry / "agent" / "auth-profiles.json"
                if not candidate.exists():
                    continue
                data = json.loads(candidate.read_text(encoding="utf-8"))
                profiles = data.get("profiles")
                if isinstance(profiles, dict) and profiles:
                    auth_payload = data
                    source_agent = entry.name
                    break
    except (OSError, ValueError, TypeError):
        pass

    if auth_payload is None:
        return {"backfilled": 0, "skipped": 0, "source": "none", "providers": []}

    profiles = auth_payload.get("profiles") or {}

    # ── 3. Build a provider→catalog mapping ─────────────────────────────
    catalog = _model_provider_catalog()
    # Map auth-profile provider name → catalog entry
    # e.g. "zai" → {"id": "zhipu", "label": "Zhipu GLM", "env": (...)}
    provider_to_catalog = {}
    for entry in catalog:
        catalog_id = str(entry.get("id") or "").strip().lower()
        if not catalog_id:
            continue
        # Map by catalog id itself
        provider_to_catalog[catalog_id] = entry
        # Map by prefixes (e.g. "zai/" prefix maps to zhipu)
        for prefix in entry.get("prefixes", ()):
            prefix_base = str(prefix or "").strip().rstrip("/").lower()
            if prefix_base:
                provider_to_catalog[prefix_base] = entry

    # ── 4. Iterate profiles and backfill missing entries ────────────────
    backfilled = []
    skipped = 0
    for profile_id, profile in profiles.items():
        if not isinstance(profile, dict):
            continue
        provider_name = str(profile.get("provider") or "").strip().lower()
        api_key = str(profile.get("key") or "").strip()
        if not provider_name or not api_key:
            continue
        catalog_entry = provider_to_catalog.get(provider_name)
        if not isinstance(catalog_entry, dict):
            continue
        catalog_id = str(catalog_entry.get("id") or "").strip().lower()
        if not catalog_id:
            continue
        if catalog_id in existing_ids:
            skipped += 1
            continue
        env_keys = [str(k or "").strip() for k in catalog_entry.get("env", ()) if str(k or "").strip()]
        if not env_keys:
            continue
        try:
            store_save_model_provider_config(
                openclaw_dir,
                {
                    "providerId": catalog_id,
                    "providerLabel": str(catalog_entry.get("label") or catalog_id).strip(),
                    "keyValue": api_key,
                    "envKeys": env_keys,
                    "status": "active",
                },
            )
            backfilled.append(catalog_id)
            existing_ids.add(catalog_id)
        except Exception:
            pass

    return {
        "backfilled": len(backfilled),
        "skipped": skipped,
        "source": f"agent:{source_agent}" if source_agent else "none",
        "providers": backfilled,
    }


def _merge_local_runtime_payload(current_runtime, payload):
    current = normalize_local_runtime_config(current_runtime)
    merged = deepcopy(current)
    has_runtime_inputs = any(
        _normalize_runtime_text(payload.get(key))
        for key in ("mode", "backend", "baseUrl", "entrypoint", "modelPath", "host", "cacheTypeK", "cacheTypeV", "extraArgs")
    ) or any(payload.get(key) is not None for key in ("port", "contextLength", "gpuLayers", "kvCacheEnabled"))
    if payload.get("enabled") is not None:
        merged["enabled"] = _normalize_runtime_bool(payload.get("enabled"), current.get("enabled", False))
    elif has_runtime_inputs:
        merged["enabled"] = True
    merged["mode"] = _normalize_runtime_text(payload.get("mode"), current.get("mode") or DEFAULT_LOCAL_RUNTIME_MODE)
    merged["backend"] = _normalize_runtime_text(payload.get("backend"), current.get("backend") or DEFAULT_LOCAL_RUNTIME_BACKEND)
    merged["baseUrl"] = _normalize_runtime_text(payload.get("baseUrl"), current.get("baseUrl") or (recommended_gemma_local_runtime_base_url() if merged["mode"] == DEFAULT_LOCAL_RUNTIME_MODE else ""))
    merged["entrypoint"] = _normalize_runtime_text(payload.get("entrypoint"), current.get("entrypoint"))
    merged["modelPath"] = _normalize_runtime_text(payload.get("modelPath"), current.get("modelPath"))
    merged["host"] = _normalize_runtime_text(payload.get("host"), current.get("host") or DEFAULT_LOCAL_RUNTIME_HOST)
    merged["port"] = _normalize_runtime_int(payload.get("port"), current.get("port", DEFAULT_LOCAL_RUNTIME_PORT), minimum=1, maximum=65535)
    merged["contextLength"] = _normalize_runtime_int(payload.get("contextLength"), current.get("contextLength", DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH), minimum=1)
    merged["gpuLayers"] = _normalize_runtime_int(payload.get("gpuLayers"), current.get("gpuLayers", DEFAULT_LOCAL_RUNTIME_GPU_LAYERS), minimum=0)
    merged["extraArgs"] = _normalize_runtime_args(payload.get("extraArgs") if "extraArgs" in payload else current.get("extraArgs"))
    kv_current = current.get("kvCache") if isinstance(current.get("kvCache"), dict) else {}
    merged["kvCache"] = {
        "enabled": _normalize_runtime_bool(payload.get("kvCacheEnabled"), kv_current.get("enabled", False)),
        "mode": _normalize_runtime_text(payload.get("kvCacheMode"), kv_current.get("mode") or DEFAULT_LOCAL_RUNTIME_KV_CACHE_MODE),
        "keyType": _normalize_runtime_text(payload.get("cacheTypeK"), kv_current.get("keyType") or DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE),
        "valueType": _normalize_runtime_text(payload.get("cacheTypeV"), kv_current.get("valueType") or DEFAULT_LOCAL_RUNTIME_KV_CACHE_TYPE),
    }
    return normalize_local_runtime_config(merged)


def perform_local_model_runtime_configure(
    openclaw_dir,
    mode="",
    backend="",
    base_url="",
    entrypoint="",
    model_path="",
    host="",
    port=None,
    context_length=None,
    gpu_layers=None,
    kv_cache_enabled=None,
    cache_type_k="",
    cache_type_v="",
    extra_args="",
    enabled=None,
):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    metadata = load_project_metadata(openclaw_dir)
    payload = {
        "enabled": enabled,
        "mode": mode,
        "backend": backend,
        "baseUrl": base_url,
        "entrypoint": entrypoint,
        "modelPath": model_path,
        "host": host,
        "port": port,
        "contextLength": context_length,
        "gpuLayers": gpu_layers,
        "kvCacheEnabled": kv_cache_enabled,
        "cacheTypeK": cache_type_k,
        "cacheTypeV": cache_type_v,
        "extraArgs": extra_args,
    }
    runtime = _merge_local_runtime_payload(metadata.get(LOCAL_RUNTIME_CONFIG_KEY), payload)
    runtime["lastConfiguredAt"] = now_iso()
    runtime["updatedAt"] = runtime["lastConfiguredAt"]
    metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    save_project_metadata(openclaw_dir, metadata)
    result = load_local_runtime_payload(openclaw_dir, metadata=metadata)
    result["saved"] = True
    result["storage"] = "mission-control.json"
    return result


def perform_local_model_runtime_use_recommended_profile(openclaw_dir, profile_id):
    profile = _local_runtime_recommended_profile_by_id(profile_id)
    if not isinstance(profile, dict) or not str(profile.get("id") or "").strip():
        raise RuntimeError("当前不支持这个本地运行时预设。")
    result = perform_local_model_runtime_configure(
        openclaw_dir,
        mode=str(profile.get("mode") or "").strip(),
        backend=str(profile.get("backend") or "").strip(),
        base_url=str(profile.get("baseUrl") or "").strip(),
        entrypoint=str(profile.get("entrypoint") or "").strip(),
        model_path=str(profile.get("modelPath") or "").strip(),
        host=str(profile.get("host") or "").strip(),
        port=profile.get("port"),
        context_length=profile.get("contextLength"),
        gpu_layers=profile.get("gpuLayers"),
        kv_cache_enabled=profile.get("kvCacheEnabled"),
        cache_type_k=str(profile.get("cacheTypeK") or "").strip(),
        cache_type_v=str(profile.get("cacheTypeV") or "").strip(),
        extra_args=profile.get("extraArgs") or [],
        enabled=True,
    )
    result["recommendedProfile"] = profile
    return result


def inspect_local_runtime_model_dir(openclaw_dir, profile_id=""):
    profile = _local_runtime_recommended_profile_by_id(profile_id)
    if not isinstance(profile, dict) or not str(profile.get("id") or "").strip():
        raise RuntimeError("当前不支持这个本地运行时预设。")
    if str(profile.get("mode") or "").strip().lower() == DEFAULT_LOCAL_RUNTIME_MODE:
        return {
            "profileId": str(profile.get("id") or "").strip(),
            "profileLabel": str(profile.get("label") or profile.get("id") or "").strip(),
            "providerId": str(profile.get("providerId") or "").strip(),
            "modelsDir": "",
            "exists": False,
            "expectedFiles": [],
            "missingFiles": [],
            "requiredFiles": [],
            "files": [],
            "readyToStart": bool(str(profile.get("baseUrl") or "").strip()),
            "externalService": True,
            "baseUrl": str(profile.get("baseUrl") or "").strip(),
        }
    models_dir_value = str(profile.get("modelsDir") or "").strip()
    model_path_value = str(profile.get("modelPath") or "").strip()
    models_dir = Path(models_dir_value).expanduser() if models_dir_value else Path(model_path_value).expanduser().parent
    required_files = profile.get("requiredFiles") if isinstance(profile.get("requiredFiles"), list) else _runtime_required_file_specs(
        model_path_value,
        profile.get("extraArgs"),
        profile.get("projectorPath"),
    )
    expected_files = [Path(str(item.get("path") or "")).name for item in required_files if str(item.get("path") or "").strip()]
    try:
        file_entries = []
        if models_dir.exists():
            for item in sorted(models_dir.iterdir(), key=lambda entry: entry.name.lower()):
                stat = item.stat()
                file_entries.append(
                    {
                        "name": item.name,
                        "path": str(item),
                        "directory": item.is_dir(),
                        "size": stat.st_size if item.is_file() else 0,
                    }
                )
    except OSError as error:
        raise RuntimeError(f"无法读取模型目录：{error}") from error
    present_names = {entry["name"] for entry in file_entries}
    missing_files = [name for name in expected_files if name not in present_names]
    return {
        "profileId": str(profile.get("id") or "").strip(),
        "profileLabel": str(profile.get("label") or profile.get("id") or "").strip(),
        "providerId": str(profile.get("providerId") or "").strip(),
        "modelsDir": str(models_dir),
        "exists": models_dir.exists(),
        "expectedFiles": expected_files,
        "missingFiles": missing_files,
        "requiredFiles": required_files,
        "files": file_entries,
        "readyToStart": bool(profile.get("entrypointExists")) and not missing_files,
    }


def perform_local_model_runtime_stop(openclaw_dir):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    metadata = load_project_metadata(openclaw_dir)
    runtime = normalize_local_runtime_config(metadata.get(LOCAL_RUNTIME_CONFIG_KEY))
    if str(runtime.get("mode") or "").strip().lower() == DEFAULT_LOCAL_RUNTIME_MODE:
        runtime["enabled"] = False
        runtime["pid"] = ""
        runtime["lastStoppedAt"] = now_iso()
        runtime["updatedAt"] = runtime["lastStoppedAt"]
        metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
        save_project_metadata(openclaw_dir, metadata)
        result = load_local_runtime_payload(openclaw_dir, metadata=metadata)
        result.update({"stopped": True, "previousPid": "", "wasRunning": False, "hadPid": False, "forced": False, "externalService": True})
        return result
    pid_raw = str(runtime.get("pid") or "").strip()
    previous_pid = int(pid_raw) if pid_raw.isdigit() else 0
    had_pid = previous_pid > 0
    was_running = False
    forced = False
    if previous_pid > 0:
        try:
            os.kill(previous_pid, 0)
            was_running = True
        except OSError:
            was_running = False
    running_before_stop = was_running
    if was_running:
        try:
            os.kill(previous_pid, signal.SIGTERM)
        except OSError:
            was_running = False
        deadline = time.time() + 3
        while was_running and time.time() < deadline:
            try:
                os.kill(previous_pid, 0)
                time.sleep(0.1)
            except OSError:
                was_running = False
        if was_running:
            try:
                os.kill(previous_pid, signal.SIGKILL)
                forced = True
            except OSError:
                pass
            time.sleep(0.1)
    runtime["pid"] = ""
    runtime["lastStoppedAt"] = now_iso()
    runtime["updatedAt"] = runtime["lastStoppedAt"]
    metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    save_project_metadata(openclaw_dir, metadata)
    result = load_local_runtime_payload(openclaw_dir, metadata=metadata)
    result.update(
        {
            "stopped": True,
            "previousPid": previous_pid,
            "wasRunning": running_before_stop or forced,
            "hadPid": had_pid,
            "forced": forced,
        }
    )
    return result


def perform_local_model_runtime_start(
    openclaw_dir,
    mode="",
    backend="",
    base_url="",
    entrypoint="",
    model_path="",
    host="",
    port=None,
    context_length=None,
    gpu_layers=None,
    kv_cache_enabled=None,
    cache_type_k="",
    cache_type_v="",
    extra_args="",
    enabled=None,
):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    metadata = load_project_metadata(openclaw_dir)
    payload = {
        "enabled": True if enabled is None else enabled,
        "mode": mode,
        "backend": backend,
        "baseUrl": base_url,
        "entrypoint": entrypoint,
        "modelPath": model_path,
        "host": host,
        "port": port,
        "contextLength": context_length,
        "gpuLayers": gpu_layers,
        "kvCacheEnabled": kv_cache_enabled,
        "cacheTypeK": cache_type_k,
        "cacheTypeV": cache_type_v,
    }
    if extra_args not in (None, ""):
        payload["extraArgs"] = extra_args
    runtime = _merge_local_runtime_payload(metadata.get(LOCAL_RUNTIME_CONFIG_KEY), payload)
    metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    if str(runtime.get("mode") or "").strip().lower() == DEFAULT_LOCAL_RUNTIME_MODE:
        if not str(runtime.get("baseUrl") or "").strip():
            runtime["lastStartError"] = "本地 runtime 的外部服务地址不能为空。"
            metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
            save_project_metadata(openclaw_dir, metadata)
            raise RuntimeError(runtime["lastStartError"])
        runtime["enabled"] = True
        runtime["lastConfiguredAt"] = now_iso()
        runtime["lastStartedAt"] = runtime["lastConfiguredAt"]
        runtime["updatedAt"] = runtime["lastConfiguredAt"]
        runtime["lastStartError"] = ""
        runtime["pid"] = ""
        runtime["logPath"] = ""
        runtime["commandPreview"] = str(runtime.get("baseUrl") or "").strip()
        metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
        save_project_metadata(openclaw_dir, metadata)
        result = load_local_runtime_payload(openclaw_dir, metadata=metadata)
        result.update({"started": True, "pid": "", "logPath": "", "command": [], "commandString": "", "externalService": True})
        return result
    resolved_entrypoint = _resolve_local_runtime_entrypoint_path(runtime.get("entrypoint"))
    required_files = _runtime_required_file_specs(
        runtime.get("modelPath"),
        runtime.get("extraArgs"),
        runtime.get("projectorPath"),
    )
    missing_required_files = [
        str(item.get("path") or "").strip()
        for item in required_files
        if str(item.get("path") or "").strip() and not Path(str(item.get("path") or "")).expanduser().exists()
    ]
    start_errors = []
    if not resolved_entrypoint:
        start_errors.append("本地 runtime 的 entrypoint 不存在，请先确认 llama-server 已安装到容器或主机。")
    if missing_required_files:
        start_errors.append("本地 runtime 模型文件不存在：" + "，".join(missing_required_files))
    if start_errors:
        runtime["lastStartError"] = "；".join(start_errors)
        runtime["enabled"] = True if enabled is None else bool(enabled)
        runtime["updatedAt"] = now_iso()
        metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
        save_project_metadata(openclaw_dir, metadata)
        raise RuntimeError(runtime["lastStartError"])
    if load_local_runtime_payload(openclaw_dir, metadata=metadata).get("running"):
        perform_local_model_runtime_stop(openclaw_dir)
        metadata = load_project_metadata(openclaw_dir)
        runtime = _merge_local_runtime_payload(metadata.get(LOCAL_RUNTIME_CONFIG_KEY), payload)
        metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    launcher_path = local_runtime_launcher_script_path(openclaw_dir)
    launcher_args = [
        "python3",
        str(launcher_path),
        "--backend",
        runtime.get("backend") or DEFAULT_LOCAL_RUNTIME_BACKEND,
        "--entrypoint",
        runtime.get("entrypoint") or "",
        "--model",
        runtime.get("modelPath") or "",
        "--host",
        runtime.get("host") or DEFAULT_LOCAL_RUNTIME_HOST,
        "--port",
        str(runtime.get("port") or DEFAULT_LOCAL_RUNTIME_PORT),
        "--ctx-size",
        str(runtime.get("contextLength") or DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH),
        "--gpu-layers",
        str(runtime.get("gpuLayers") if runtime.get("gpuLayers") is not None else DEFAULT_LOCAL_RUNTIME_GPU_LAYERS),
        "--extra-args-json",
        json.dumps(runtime.get("extraArgs") if isinstance(runtime.get("extraArgs"), list) else [], ensure_ascii=False),
        "--json",
    ]
    kv_cache = runtime.get("kvCache") if isinstance(runtime.get("kvCache"), dict) else {}
    if kv_cache.get("enabled"):
        if str(kv_cache.get("keyType") or "").strip():
            launcher_args.extend(["--cache-type-k", str(kv_cache.get("keyType") or "").strip()])
        if str(kv_cache.get("valueType") or "").strip():
            launcher_args.extend(["--cache-type-v", str(kv_cache.get("valueType") or "").strip()])
    process = _run_command(launcher_args, env=openclaw_command_env(openclaw_dir))
    prepared = parse_json_payload(process.stdout, process.stderr, default=None)
    if not isinstance(prepared, dict) or not prepared.get("ok"):
        message = ""
        if isinstance(prepared, dict):
            message = "; ".join(str(item.get("message") or "").strip() for item in prepared.get("errors", []) if str(item.get("message") or "").strip())
            runtime["lastStartError"] = message or str(prepared.get("message") or "").strip()
        else:
            runtime["lastStartError"] = join_command_output(process) or "local_runtime_launch_prepare_failed"
        metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
        save_project_metadata(openclaw_dir, metadata)
        raise RuntimeError(runtime["lastStartError"] or "本地 runtime 启动预检查失败。")
    command = prepared.get("command") if isinstance(prepared.get("command"), list) else []
    if not command:
        raise RuntimeError("本地 runtime 启动命令为空。")
    logs_dir = openclaw_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "local-model-runtime.log"
    with log_path.open("a", encoding="utf-8") as log_file:
        child = subprocess.Popen(
            [str(item) for item in command],
            cwd=str(openclaw_dir),
            env=openclaw_command_env(openclaw_dir),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    runtime["enabled"] = True
    runtime["commandPreview"] = str(prepared.get("commandString") or "").strip()
    runtime["lastConfiguredAt"] = now_iso()
    runtime["lastStartedAt"] = runtime["lastConfiguredAt"]
    runtime["updatedAt"] = runtime["lastConfiguredAt"]
    runtime["lastStartError"] = ""
    runtime["logPath"] = str(log_path)
    runtime["pid"] = str(child.pid)
    metadata[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    save_project_metadata(openclaw_dir, metadata)
    result = load_local_runtime_payload(openclaw_dir, metadata=metadata)
    result.update(
        {
            "started": True,
            "pid": child.pid,
            "logPath": str(log_path),
            "command": command,
            "commandString": str(prepared.get("commandString") or "").strip(),
        }
    )
    return result


def perform_model_rollout(openclaw_dir, model_name, scope="all", agent_id=""):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    normalized_model = str(model_name or "").strip()
    if not normalized_model:
        raise RuntimeError("请输入要接入的模型名称。")

    config_path = openclaw_dir / "openclaw.json"
    original_text = config_path.read_text(encoding="utf-8")
    config = json.loads(original_text)
    agents = load_agents(config)
    if not agents:
        raise RuntimeError("当前配置里没有可更新的 Agent。")

    router_agent_id = get_router_agent_id(config)
    normalized_scope = str(scope or "all").strip().lower()
    if normalized_scope == "router":
        target_ids = {router_agent_id}
    elif normalized_scope == "agent":
        normalized_agent_id = str(agent_id or "").strip()
        if not normalized_agent_id:
            raise RuntimeError("请选择要更新的 Agent。")
        target_ids = {normalized_agent_id}
    else:
        normalized_scope = "all"
        target_ids = {agent.get("id", "") for agent in agents if agent.get("id")}

    matched_agents = []
    changed_agents = []
    for agent in agents:
        current_id = str(agent.get("id", "")).strip()
        if current_id not in target_ids:
            continue
        matched_agents.append(current_id)
        previous_model = str(agent.get("model", "")).strip()
        if previous_model == normalized_model:
            continue
        agent["model"] = normalized_model
        changed_agents.append(
            {
                "id": current_id,
                "previousModel": previous_model,
                "nextModel": normalized_model,
            }
        )

    if not matched_agents:
        raise RuntimeError("没有找到符合条件的 Agent。")

    if not changed_agents:
        return {
            "model": normalized_model,
            "scope": normalized_scope,
            "matchedAgents": matched_agents,
            "changedAgents": [],
            "updatedAgents": 0,
            "provider": infer_model_provider(normalized_model).get("label", "兼容 / 自定义"),
            "validated": True,
        }

    save_config(openclaw_dir, config)
    env = openclaw_command_env(openclaw_dir)
    validate_result = _run_command(["openclaw", "config", "validate", "--json"], env=env)
    validate_payload = parse_json_payload(validate_result.stdout, validate_result.stderr, default=None)
    if not (isinstance(validate_payload, dict) and validate_payload.get("valid")):
        config_path.write_text(original_text, encoding="utf-8")
        reason = ""
        if isinstance(validate_payload, dict):
            reason = str(validate_payload.get("error", "") or validate_payload.get("message", "")).strip()
        if not reason:
            reason = join_command_output(validate_result) or "openclaw config validate 未通过"
        raise RuntimeError(f"模型配置校验失败，已回滚：{reason}")

    return {
        "model": normalized_model,
        "scope": normalized_scope,
        "matchedAgents": matched_agents,
        "changedAgents": changed_agents,
        "updatedAgents": len(changed_agents),
        "provider": infer_model_provider(normalized_model).get("label", "兼容 / 自定义"),
        "validated": True,
    }
