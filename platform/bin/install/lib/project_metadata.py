#!/usr/bin/env python3
"""Helpers for OpenClaw Team runtime metadata stored outside openclaw.json."""

from __future__ import annotations

import json
import os
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path


METADATA_FILENAME = "mission-control.json"
CONFIG_KEY = "missionControl"
DEFAULT_THEME_NAME = "corporate"
DEFAULT_THEME_DISPLAY_NAME = "现代企业"
LOCAL_RUNTIME_CONFIG_KEY = "localRuntime"
SPEECH_RUNTIME_CONFIG_KEY = "speechRuntime"
EXECUTION_CONFIG_KEY = "execution"
DEFAULT_LOCAL_RUNTIME_MODE = "external_openai"
DEFAULT_LOCAL_RUNTIME_BACKEND = "llama_cpp"
DEFAULT_LOCAL_RUNTIME_ENTRYPOINT = ""
DEFAULT_LOCAL_RUNTIME_MODEL_PATH = ""
DEFAULT_LOCAL_RUNTIME_HOST = "127.0.0.1"
DEFAULT_LOCAL_RUNTIME_PORT = 8080
DEFAULT_LOCAL_RUNTIME_BASE_URL = "http://127.0.0.1:8080/v1"
DEFAULT_LOCAL_RUNTIME_DOCKER_BASE_URL = "http://host.docker.internal:8080/v1"
DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH = 8192
DEFAULT_LOCAL_RUNTIME_GPU_LAYERS = 0
DEFAULT_LOCAL_RUNTIME_KV_CACHE_MODE = "turbo3"
DEFAULT_LOCAL_RUNTIME_KV_CACHE_KEY_TYPE = "turbo3"
DEFAULT_LOCAL_RUNTIME_KV_CACHE_VALUE_TYPE = "turbo3"
DEFAULT_EXECUTION_TRANSPORT = "openclaw"
DEFAULT_EXECUTION_PRIMARY_PATH = "local_runtime"
DEFAULT_EXECUTION_FALLBACK_PATH = "provider_api"
DEFAULT_EXECUTION_CONTEXT_MODE = "layered"
DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE = "primary_execution"
DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY = "balanced"
HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES = {"balanced", "aggressive", "full"}
LEGACY_SPEECH_RUNTIME_PROVIDER_COSYVOICE = "cosyvoice_sft"
DEFAULT_SPEECH_RUNTIME_PROVIDER = "sherpa_onnx"
DEFAULT_SPEECH_RUNTIME_MODEL = "gpt-4o-mini-tts-2025-12-15"
DEFAULT_SPEECH_RUNTIME_BASE_URL = ""
DEFAULT_SPEECH_RUNTIME_API_KEY_ENV = "OPENAI_API_KEY"
DEFAULT_SHERPA_ONNX_RUNTIME_BASE_URL = "http://127.0.0.1:8080/v1"
DEFAULT_SHERPA_ONNX_RUNTIME_DOCKER_BASE_URL = "http://sherpa-onnx-tts:8080/v1"
DEFAULT_SHERPA_ONNX_RUNTIME_MODEL = "kokoro-multi-lang-v1_1"
DEFAULT_SHERPA_ONNX_RUNTIME_API_KEY_ENV = "SHERPA_ONNX_TTS_API_KEY"
DEFAULT_QWEN3_TTS_RUNTIME_BASE_URL = "http://127.0.0.1:8080/v1"
DEFAULT_QWEN3_TTS_RUNTIME_DOCKER_BASE_URL = "http://qwen3-tts:8080/v1"
DEFAULT_QWEN3_TTS_RUNTIME_MODEL = "qwen3-tts-12hz-0.6b-customvoice"
DEFAULT_QWEN3_TTS_RUNTIME_API_KEY_ENV = "QWEN3_TTS_API_KEY"
DEFAULT_ZHIPU_TTS_RUNTIME_BASE_URL = "https://open.bigmodel.cn/api/paas/v4"
DEFAULT_ZHIPU_TTS_RUNTIME_MODEL = "glm-tts"
DEFAULT_ZHIPU_TTS_RUNTIME_API_KEY_ENV = "ZAI_API_KEY"
SUPPORTED_SPEECH_RUNTIME_PROVIDERS = {"openai", "sherpa_onnx", "qwen3_tts", "zhipu"}
PLACEHOLDER_PREFIX = "${"
PLACEHOLDER_SUFFIX = "}"
OPENCLAW_BROWSER_PROFILE_FALLBACK_ORDER = ("openclaw", "user", "work", "remote")
OPENCLAW_BROWSER_PROFILE_ALIASES = {
    "chrome-relay": "user",
}
OPENCLAW_BROWSER_DRIVER_ALIASES = {
    "extension": "existing-session",
}
OPENCLAW_SUPPORTED_BROWSER_DRIVERS = {"openclaw", "existing-session"}
DEFAULT_OPENCLAW_SESSION_VISIBILITY = "all"
DEFAULT_GEMMA_LOCAL_RUNTIME_MODEL = "gemma-4-e2b-edge"
DEFAULT_GEMMA_LOCAL_RUNTIME_MODEL_PATH = "gemma-4-e2b-it-q4_k_m.gguf"
DEFAULT_GEMMA_LOCAL_RUNTIME_PROJECTOR_PATH = "mmproj-e2b-f16.gguf"
DEFAULT_GEMMA_LOCAL_RUNTIME_LIGHT_MODEL = "gemma-4-e4b-edge"
DEFAULT_GEMMA_LOCAL_RUNTIME_LIGHT_MODEL_PATH = "gemma-4-e4b-it-q4_k_m.gguf"
DEFAULT_GEMMA_LOCAL_RUNTIME_CONTEXT_LENGTH = 65536
DEFAULT_GEMMA_LOCAL_RUNTIME_CACHE_TYPE = "f16"
DEFAULT_GEMMA_LOCAL_RUNTIME_EXTRA_ARGS = [
    "--threads",
    "4",
    "--threads-batch",
    "4",
    "--jinja",
    "--mmproj",
    DEFAULT_GEMMA_LOCAL_RUNTIME_PROJECTOR_PATH,
]


def _running_in_container():
    return Path("/.dockerenv").exists()


def _default_local_runtime_base_url():
    if _running_in_container():
        return DEFAULT_LOCAL_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_LOCAL_RUNTIME_BASE_URL


def _default_speech_runtime_provider():
    return DEFAULT_SPEECH_RUNTIME_PROVIDER


def _qwen3_tts_runtime_base_url():
    if _running_in_container():
        return DEFAULT_QWEN3_TTS_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_QWEN3_TTS_RUNTIME_BASE_URL


def _sherpa_onnx_runtime_base_url():
    if _running_in_container():
        return DEFAULT_SHERPA_ONNX_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_SHERPA_ONNX_RUNTIME_BASE_URL


def _is_placeholder_secret(value, env_key):
    return str(value or "").strip() == f"{PLACEHOLDER_PREFIX}{env_key}{PLACEHOLDER_SUFFIX}"


def _resolved_secret(env_key):
    return str(os.environ.get(env_key) or "").strip()


def _sanitize_channel_secret(channel, secret_key, env_key, clear_keys=()):
    if not isinstance(channel, dict):
        return False
    changed = False
    current = str(channel.get(secret_key) or "").strip()
    resolved = _resolved_secret(env_key)
    if _is_placeholder_secret(current, env_key) or (not current and resolved):
        if resolved:
            if current != resolved:
                channel[secret_key] = resolved
                changed = True
            if channel.get("enabled") is not True:
                channel["enabled"] = True
                changed = True
        else:
            if secret_key in channel:
                channel.pop(secret_key, None)
                changed = True
            if channel.get("enabled") is not False:
                channel["enabled"] = False
                changed = True
            for extra_key in clear_keys:
                if extra_key in channel:
                    channel.pop(extra_key, None)
                    changed = True
    return changed


def _sanitize_gateway_auth(config):
    gateway = config.get("gateway") if isinstance(config.get("gateway"), dict) else {}
    auth = gateway.get("auth") if isinstance(gateway.get("auth"), dict) else {}
    current = str(auth.get("token") or "").strip()
    resolved = _resolved_secret("GATEWAY_AUTH_TOKEN")
    if not auth:
        return False
    changed = False
    if _is_placeholder_secret(current, "GATEWAY_AUTH_TOKEN") or (not current and resolved):
        if resolved:
            if current != resolved:
                auth["token"] = resolved
                changed = True
        elif "token" in auth:
            auth.pop("token", None)
            changed = True
        gateway["auth"] = auth
        config["gateway"] = gateway
    return changed


def _normalize_bool(value, default=False):
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


def _normalize_int(value, default, minimum=None, maximum=None):
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None and normalized < minimum:
        return default
    if maximum is not None and normalized > maximum:
        return default
    return normalized


def _normalize_str(value, default=""):
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _normalize_list(value):
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _migrate_legacy_browser_config(browser):
    normalized = deepcopy(browser) if isinstance(browser, dict) else {}
    profiles = normalized.get("profiles") if isinstance(normalized.get("profiles"), dict) else {}
    next_profiles = {}
    for raw_name, raw_payload in profiles.items():
        name = OPENCLAW_BROWSER_PROFILE_ALIASES.get(str(raw_name or "").strip(), str(raw_name or "").strip())
        if not name:
            continue
        payload = deepcopy(raw_payload) if isinstance(raw_payload, dict) else {}
        driver = str(payload.get("driver") or "").strip()
        if driver:
            payload["driver"] = OPENCLAW_BROWSER_DRIVER_ALIASES.get(driver, driver)
        payload.pop("relayBindHost", None)
        if name in next_profiles and not payload:
            continue
        next_profiles[name] = payload
    if next_profiles:
        normalized["profiles"] = next_profiles
    elif "profiles" in normalized:
        normalized.pop("profiles", None)
    normalized.pop("relayBindHost", None)
    default_profile = OPENCLAW_BROWSER_PROFILE_ALIASES.get(
        str(normalized.get("defaultProfile") or "").strip(),
        str(normalized.get("defaultProfile") or "").strip(),
    )
    available_profiles = set(next_profiles.keys()) | set(OPENCLAW_BROWSER_PROFILE_FALLBACK_ORDER)
    if not default_profile or default_profile not in available_profiles:
        fallback_candidates = list(OPENCLAW_BROWSER_PROFILE_FALLBACK_ORDER) + sorted(next_profiles.keys())
        default_profile = next(
            (candidate for candidate in fallback_candidates if candidate in available_profiles),
            "openclaw",
        )
    normalized["defaultProfile"] = default_profile or "openclaw"
    return normalized


def _sanitize_browser_config(browser):
    normalized = deepcopy(browser) if isinstance(browser, dict) else {}
    profiles = normalized.get("profiles") if isinstance(normalized.get("profiles"), dict) else {}
    next_profiles = {}
    for raw_name, raw_payload in profiles.items():
        name = str(raw_name or "").strip()
        if not name:
            continue
        payload = deepcopy(raw_payload) if isinstance(raw_payload, dict) else {}
        driver = str(payload.get("driver") or "").strip()
        if driver and driver not in OPENCLAW_SUPPORTED_BROWSER_DRIVERS:
            continue
        payload.pop("relayBindHost", None)
        if name in next_profiles and not payload:
            continue
        next_profiles[name] = payload
    if next_profiles:
        normalized["profiles"] = next_profiles
    elif "profiles" in normalized:
        normalized.pop("profiles", None)
    normalized.pop("relayBindHost", None)
    default_profile = str(normalized.get("defaultProfile") or "").strip()
    available_profiles = set(next_profiles.keys()) | set(OPENCLAW_BROWSER_PROFILE_FALLBACK_ORDER)
    if not default_profile or default_profile not in available_profiles:
        fallback_candidates = list(OPENCLAW_BROWSER_PROFILE_FALLBACK_ORDER) + sorted(next_profiles.keys())
        default_profile = next(
            (candidate for candidate in fallback_candidates if candidate in available_profiles),
            "openclaw",
        )
    normalized["defaultProfile"] = default_profile or "openclaw"
    return normalized


def _normalize_local_runtime(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    runtime = normalized.get(LOCAL_RUNTIME_CONFIG_KEY)
    runtime = deepcopy(runtime) if isinstance(runtime, dict) else {}
    kv_cache = runtime.get("kvCache")
    kv_cache = deepcopy(kv_cache) if isinstance(kv_cache, dict) else {}
    runtime_mode = _normalize_str(runtime.get("mode"), DEFAULT_LOCAL_RUNTIME_MODE)
    runtime["enabled"] = _normalize_bool(runtime.get("enabled"), False)
    runtime["mode"] = runtime_mode
    runtime["backend"] = _normalize_str(runtime.get("backend"), DEFAULT_LOCAL_RUNTIME_BACKEND)
    runtime["baseUrl"] = _normalize_str(runtime.get("baseUrl"), _default_local_runtime_base_url() if runtime_mode == DEFAULT_LOCAL_RUNTIME_MODE else "")
    runtime["entrypoint"] = _normalize_str(runtime.get("entrypoint"), DEFAULT_LOCAL_RUNTIME_ENTRYPOINT)
    runtime["modelPath"] = _normalize_str(runtime.get("modelPath"), DEFAULT_LOCAL_RUNTIME_MODEL_PATH)
    runtime["host"] = _normalize_str(runtime.get("host"), DEFAULT_LOCAL_RUNTIME_HOST)
    runtime["port"] = _normalize_int(runtime.get("port"), DEFAULT_LOCAL_RUNTIME_PORT, minimum=1, maximum=65535)
    runtime["contextLength"] = _normalize_int(runtime.get("contextLength"), DEFAULT_LOCAL_RUNTIME_CONTEXT_LENGTH, minimum=1)
    runtime["gpuLayers"] = _normalize_int(runtime.get("gpuLayers"), DEFAULT_LOCAL_RUNTIME_GPU_LAYERS, minimum=0)
    runtime["extraArgs"] = _normalize_list(runtime.get("extraArgs"))
    runtime["kvCache"] = {
        **kv_cache,
        "enabled": _normalize_bool(kv_cache.get("enabled"), False),
        "mode": _normalize_str(kv_cache.get("mode"), DEFAULT_LOCAL_RUNTIME_KV_CACHE_MODE),
        "keyType": _normalize_str(kv_cache.get("keyType"), DEFAULT_LOCAL_RUNTIME_KV_CACHE_KEY_TYPE),
        "valueType": _normalize_str(kv_cache.get("valueType"), DEFAULT_LOCAL_RUNTIME_KV_CACHE_VALUE_TYPE),
    }
    for key in ("lastConfiguredAt", "lastStartedAt", "lastStoppedAt", "lastStartError", "logPath", "pid", "commandPreview", "updatedAt"):
        text = _normalize_str(runtime.get(key), "")
        if text:
            runtime[key] = text
    normalized[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    return normalized


def _normalize_execution(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    execution = normalized.get(EXECUTION_CONFIG_KEY)
    execution = deepcopy(execution) if isinstance(execution, dict) else {}
    primary_path = _normalize_str(execution.get("primaryPath"), DEFAULT_EXECUTION_PRIMARY_PATH)
    if primary_path not in {"provider_api", "local_runtime", "auto"}:
        primary_path = DEFAULT_EXECUTION_PRIMARY_PATH
    fallback_path = _normalize_str(execution.get("fallbackPath"), DEFAULT_EXECUTION_FALLBACK_PATH)
    if fallback_path not in {"provider_api", "local_runtime", "none"}:
        fallback_path = DEFAULT_EXECUTION_FALLBACK_PATH
    local_runtime_role = _normalize_str(execution.get("localRuntimeRole"), DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE)
    if local_runtime_role not in {"optional_acceleration", "primary_execution"}:
        local_runtime_role = DEFAULT_EXECUTION_LOCAL_RUNTIME_ROLE
    hosted_provider_context_budget_policy = _normalize_str(
        execution.get("hostedProviderContextBudgetPolicy") or execution.get("contextBudgetPolicy"),
        DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY,
    ).lower()
    if hosted_provider_context_budget_policy not in HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY_CHOICES:
        hosted_provider_context_budget_policy = DEFAULT_EXECUTION_HOSTED_PROVIDER_CONTEXT_BUDGET_POLICY
    execution["transport"] = _normalize_str(execution.get("transport"), DEFAULT_EXECUTION_TRANSPORT)
    execution["primaryPath"] = primary_path
    execution["fallbackPath"] = fallback_path
    execution["contextMode"] = _normalize_str(execution.get("contextMode"), DEFAULT_EXECUTION_CONTEXT_MODE)
    execution["localRuntimeRole"] = local_runtime_role
    execution["preferredProviderId"] = _normalize_str(execution.get("preferredProviderId"), "")
    execution["hostedProviderContextBudgetPolicy"] = hosted_provider_context_budget_policy
    normalized[EXECUTION_CONFIG_KEY] = execution
    return normalized


def _self_heal_gemma_local_runtime(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    execution = normalized.get(EXECUTION_CONFIG_KEY) if isinstance(normalized.get(EXECUTION_CONFIG_KEY), dict) else {}
    runtime = normalized.get(LOCAL_RUNTIME_CONFIG_KEY) if isinstance(normalized.get(LOCAL_RUNTIME_CONFIG_KEY), dict) else {}
    primary_path = str(execution.get("primaryPath") or "").strip().lower()
    preferred_provider = str(execution.get("preferredProviderId") or "").strip().lower()
    if primary_path != "local_runtime":
        return normalized
    if preferred_provider not in {"", "gemma"}:
        return normalized
    runtime.update(
        {
            "enabled": True,
            "mode": DEFAULT_LOCAL_RUNTIME_MODE,
            "backend": DEFAULT_LOCAL_RUNTIME_BACKEND,
            "baseUrl": _default_local_runtime_base_url(),
            "entrypoint": "",
            "modelPath": DEFAULT_GEMMA_LOCAL_RUNTIME_MODEL_PATH,
            "host": "host.docker.internal" if _running_in_container() else DEFAULT_LOCAL_RUNTIME_HOST,
            "port": DEFAULT_LOCAL_RUNTIME_PORT,
            "contextLength": DEFAULT_GEMMA_LOCAL_RUNTIME_CONTEXT_LENGTH,
            "gpuLayers": DEFAULT_LOCAL_RUNTIME_GPU_LAYERS,
            "extraArgs": [],
            "kvCache": {
                "enabled": False,
                "mode": DEFAULT_GEMMA_LOCAL_RUNTIME_CACHE_TYPE,
                "keyType": DEFAULT_GEMMA_LOCAL_RUNTIME_CACHE_TYPE,
                "valueType": DEFAULT_GEMMA_LOCAL_RUNTIME_CACHE_TYPE,
            },
        }
    )
    normalized[LOCAL_RUNTIME_CONFIG_KEY] = runtime
    execution["preferredProviderId"] = "gemma"
    normalized[EXECUTION_CONFIG_KEY] = execution
    return normalized


def _normalize_openclaw_model_aliases(config):
    clean = deepcopy(config) if isinstance(config, dict) else {}
    agents = clean.get("agents") if isinstance(clean.get("agents"), dict) else {}
    defaults = agents.get("defaults") if isinstance(agents.get("defaults"), dict) else {}
    aliases = defaults.get("models") if isinstance(defaults.get("models"), dict) else {}
    return clean, agents, defaults, aliases


def apply_local_runtime_model_provider_config(config, metadata=None):
    clean, agents, defaults, aliases = _normalize_openclaw_model_aliases(config)
    normalized_metadata = normalize_project_metadata(metadata or {})
    runtime = normalized_metadata.get(LOCAL_RUNTIME_CONFIG_KEY) if isinstance(normalized_metadata.get(LOCAL_RUNTIME_CONFIG_KEY), dict) else {}
    execution = normalized_metadata.get(EXECUTION_CONFIG_KEY) if isinstance(normalized_metadata.get(EXECUTION_CONFIG_KEY), dict) else {}
    if str(execution.get("primaryPath") or "").strip().lower() != "local_runtime":
        return clean
    if str(execution.get("preferredProviderId") or "").strip().lower() not in {"", "gemma"}:
        return clean
    base_url = _normalize_str(runtime.get("baseUrl"), _default_local_runtime_base_url())
    model_path = _normalize_str(runtime.get("modelPath"), "")
    if not base_url or not model_path:
        return clean

    model_filename = Path(model_path).name
    if not model_filename:
        return clean
    if model_filename == Path(DEFAULT_GEMMA_LOCAL_RUNTIME_MODEL_PATH).name:
        alias = DEFAULT_GEMMA_LOCAL_RUNTIME_MODEL
        secondary_alias = DEFAULT_GEMMA_LOCAL_RUNTIME_LIGHT_MODEL
    elif model_filename == Path(DEFAULT_GEMMA_LOCAL_RUNTIME_LIGHT_MODEL_PATH).name:
        alias = DEFAULT_GEMMA_LOCAL_RUNTIME_LIGHT_MODEL
        secondary_alias = DEFAULT_GEMMA_LOCAL_RUNTIME_MODEL
    else:
        alias = Path(model_filename).stem
        secondary_alias = ""

    local_provider_ref = f"gemma/{model_filename}"
    legacy_provider_ref = f"gemma/{Path(DEFAULT_GEMMA_LOCAL_RUNTIME_LIGHT_MODEL_PATH).name}"
    models = clean.get("models") if isinstance(clean.get("models"), dict) else {}
    providers = models.get("providers") if isinstance(models.get("providers"), dict) else {}
    provider = providers.get("gemma") if isinstance(providers.get("gemma"), dict) else {}
    provider["api"] = "openai-completions"
    provider["baseUrl"] = base_url.rstrip("/")
    provider["apiKey"] = str(provider.get("apiKey") or "local-runtime").strip() or "local-runtime"
    provider_models = provider.get("models") if isinstance(provider.get("models"), list) else []
    provider_model_ids = {str(item.get("id") or "").strip(): item for item in provider_models if isinstance(item, dict)}
    provider_model_ids[model_filename] = {
        "id": model_filename,
        "name": alias,
    }
    provider["models"] = [provider_model_ids[key] for key in sorted(provider_model_ids.keys())]
    providers["gemma"] = provider
    models["providers"] = providers
    clean["models"] = models

    aliases[local_provider_ref] = {
        "alias": alias,
    }
    defaults["models"] = aliases
    model_defaults = defaults.get("model") if isinstance(defaults.get("model"), dict) else {}
    if str(model_defaults.get("primary") or "").strip() in {alias, secondary_alias, legacy_provider_ref}:
        model_defaults["primary"] = local_provider_ref
    fallbacks = model_defaults.get("fallbacks")
    if isinstance(fallbacks, list):
        model_defaults["fallbacks"] = [
            local_provider_ref if str(item).strip() in {alias, secondary_alias, legacy_provider_ref} else item
            for item in fallbacks
        ]
    if model_defaults:
        defaults["model"] = model_defaults
    agents["defaults"] = defaults
    for agent in agents.get("list", []):
        if not isinstance(agent, dict):
            continue
        if str(agent.get("model") or "").strip() in {alias, secondary_alias, legacy_provider_ref}:
            agent["model"] = local_provider_ref
    clean["agents"] = agents
    return clean


def _normalize_speech_runtime(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    runtime = normalized.get(SPEECH_RUNTIME_CONFIG_KEY)
    runtime = deepcopy(runtime) if isinstance(runtime, dict) else {}
    default_provider = _default_speech_runtime_provider()
    provider = _normalize_str(runtime.get("provider"), default_provider).lower()
    if provider not in SUPPORTED_SPEECH_RUNTIME_PROVIDERS:
        provider = default_provider
    if provider == LEGACY_SPEECH_RUNTIME_PROVIDER_COSYVOICE:
        provider = DEFAULT_SPEECH_RUNTIME_PROVIDER
    if provider == "sherpa_onnx":
        default_base_url = _sherpa_onnx_runtime_base_url()
        default_model = DEFAULT_SHERPA_ONNX_RUNTIME_MODEL
        default_api_key_env = DEFAULT_SHERPA_ONNX_RUNTIME_API_KEY_ENV
    elif provider == "qwen3_tts":
        default_base_url = _qwen3_tts_runtime_base_url()
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
    runtime["provider"] = provider
    runtime["baseUrl"] = _normalize_str(runtime.get("baseUrl"), default_base_url)
    normalized_model = _normalize_str(runtime.get("model"), default_model)
    if provider == "qwen3_tts" and normalized_model in {"cosyvoice-300m-sft", "cosyvoice-300m-instruct"}:
        normalized_model = default_model
    runtime["model"] = normalized_model
    runtime["apiKeyEnv"] = _normalize_str(runtime.get("apiKeyEnv"), default_api_key_env)
    normalized[SPEECH_RUNTIME_CONFIG_KEY] = runtime
    return normalized


def metadata_path(openclaw_dir):
    return Path(openclaw_dir).expanduser().resolve() / METADATA_FILENAME


def metadata_candidates(openclaw_dir):
    base = Path(openclaw_dir).expanduser().resolve()
    return (base / METADATA_FILENAME,)


def _load_json(path):
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def extract_legacy_metadata(config):
    if not isinstance(config, dict):
        return {}
    metadata = config.get(CONFIG_KEY, {})
    if isinstance(metadata, dict) and metadata:
        return deepcopy(metadata)
    return {}


def normalize_project_metadata(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    project_dir = _normalize_str(normalized.get("projectDir"))
    if project_dir:
        try:
            project_dir = str(Path(project_dir).expanduser().resolve())
        except Exception:
            project_dir = _normalize_str(project_dir)
        normalized["projectDir"] = project_dir
    else:
        normalized.pop("projectDir", None)
    theme_name = str(normalized.get("theme") or "").strip()
    if theme_name != DEFAULT_THEME_NAME:
        normalized["theme"] = DEFAULT_THEME_NAME
        normalized["displayName"] = DEFAULT_THEME_DISPLAY_NAME
    elif not str(normalized.get("displayName") or "").strip():
        normalized["displayName"] = DEFAULT_THEME_DISPLAY_NAME
    deployment_mode = str(normalized.get("deploymentMode") or "").strip().lower()
    if deployment_mode not in {"single_tenant", "shared_platform"}:
        normalized["deploymentMode"] = "single_tenant"
    deployment_profile = str(normalized.get("deploymentProfile") or "").strip().lower()
    if not deployment_profile:
        normalized["deploymentProfile"] = "standard"
    normalized = _normalize_local_runtime(normalized)
    normalized = _normalize_execution(normalized)
    normalized = _self_heal_gemma_local_runtime(normalized)
    return _normalize_speech_runtime(normalized)


def load_project_metadata(openclaw_dir, existing_config=None):
    sidecar = {}
    for candidate in metadata_candidates(openclaw_dir):
        sidecar = _load_json(candidate)
        if isinstance(sidecar, dict) and sidecar:
            break
    config = existing_config
    if config is None:
        config_path = Path(openclaw_dir).expanduser().resolve() / "openclaw.json"
        config = _load_json(config_path)
    legacy = extract_legacy_metadata(config)
    if isinstance(sidecar, dict) and sidecar:
        return normalize_project_metadata({**legacy, **sidecar})

    return normalize_project_metadata(legacy)


def sanitize_openclaw_config(config):
    clean = deepcopy(config)
    if isinstance(clean, dict):
        metadata = clean.get(CONFIG_KEY) if isinstance(clean.get(CONFIG_KEY), dict) else {}
        project_dir = _normalize_str(metadata.get("projectDir"))
        clean.pop(CONFIG_KEY, None)
        if project_dir:
            clean[CONFIG_KEY] = {"projectDir": project_dir}
        plugins = clean.get("plugins")
        if not isinstance(plugins, dict):
            plugins = {}
        plugins.pop("allow", None)
        entries = plugins.get("entries") if isinstance(plugins.get("entries"), dict) else {}
        browser_cfg = clean.get("browser") if isinstance(clean.get("browser"), dict) else {}
        if browser_cfg.get("enabled", True) is not False:
            browser_entry = entries.get("browser") if isinstance(entries.get("browser"), dict) else {}
            browser_entry.setdefault("enabled", True)
            entries["browser"] = browser_entry
        if entries:
            plugins["entries"] = entries
        if plugins:
            clean["plugins"] = plugins
        browser = clean.get("browser")
        if isinstance(browser, dict):
            clean["browser"] = _sanitize_browser_config(browser)
        session = clean.get("session") if isinstance(clean.get("session"), dict) else {}
        if session:
            session.pop("transcriptPath", None)
            clean["session"] = session
        tools = clean.get("tools") if isinstance(clean.get("tools"), dict) else {}
        sessions = tools.get("sessions") if isinstance(tools.get("sessions"), dict) else {}
        sessions.pop("transcript", None)
        sessions["visibility"] = DEFAULT_OPENCLAW_SESSION_VISIBILITY
        tools["sessions"] = sessions
        clean["tools"] = tools
        channels = clean.get("channels")
        if isinstance(channels, dict):
            feishu = channels.get("feishu")
            if isinstance(feishu, dict):
                feishu.pop("commands", None)
                channels["feishu"] = feishu
            if _sanitize_channel_secret(channels.get("feishu"), "appSecret", "FEISHU_APP_SECRET"):
                clean["channels"] = channels
            if _sanitize_channel_secret(channels.get("telegram"), "botToken", "TELEGRAM_BOT_TOKEN", clear_keys=("proxy",)):
                clean["channels"] = channels
            qqbot_changed = False
            for qq_channel_name in ("qqbot", "qq"):
                qqbot_changed = _sanitize_channel_secret(
                    channels.get(qq_channel_name),
                    "clientSecret",
                    "QQBOT_CLIENT_SECRET",
                ) or qqbot_changed
            if qqbot_changed:
                clean["channels"] = channels
        _sanitize_gateway_auth(clean)
        gateway = clean.get("gateway") if isinstance(clean.get("gateway"), dict) else {}
        gateway_http = gateway.get("http") if isinstance(gateway.get("http"), dict) else {}
        gateway_endpoints = gateway_http.get("endpoints") if isinstance(gateway_http.get("endpoints"), dict) else {}
        chat_completions = gateway_endpoints.get("chatCompletions") if isinstance(gateway_endpoints.get("chatCompletions"), dict) else {}
        chat_completions["enabled"] = True
        gateway_endpoints["chatCompletions"] = chat_completions
        gateway_http["endpoints"] = gateway_endpoints
        gateway["http"] = gateway_http
        clean["gateway"] = gateway
    return clean


def migrate_legacy_openclaw_config(config):
    clean = deepcopy(config)
    if isinstance(clean, dict):
        browser = clean.get("browser")
        if isinstance(browser, dict):
            clean["browser"] = _migrate_legacy_browser_config(browser)
    return sanitize_openclaw_config(clean)


def write_project_metadata(openclaw_dir, metadata):
    path = metadata_path(openclaw_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_project_metadata(metadata)
    payload = {
        key: value
        for key, value in deepcopy(normalized).items()
        if value not in (None, "")
    }
    payload["updatedAt"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
