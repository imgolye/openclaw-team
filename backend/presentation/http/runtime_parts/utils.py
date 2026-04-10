"""Runtime part: utils."""

LOCAL_RUNTIME_CONFIG_KEY = "localRuntime"
SPEECH_RUNTIME_CONFIG_KEY = "speechRuntime"
EXECUTION_CONFIG_KEY = "execution"
DEFAULT_LOCAL_RUNTIME_BACKEND = "llama_cpp"
DEFAULT_LOCAL_RUNTIME_ENTRYPOINT = ""
DEFAULT_LOCAL_RUNTIME_MODEL_PATH = ""
DEFAULT_LOCAL_RUNTIME_HOST = "127.0.0.1"
DEFAULT_LOCAL_RUNTIME_PORT = 8080
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
OPENCLAW_BROWSER_PROFILE_FALLBACK_ORDER = ("openclaw", "user", "work", "remote")
OPENCLAW_SUPPORTED_BROWSER_DRIVERS = {"openclaw", "existing-session"}
DEFAULT_OPENCLAW_SESSION_VISIBILITY = "all"


def _running_in_container():
    return Path("/.dockerenv").exists()


def _default_speech_runtime_provider():
    return DEFAULT_SPEECH_RUNTIME_PROVIDER


def _speech_runtime_env_text(*keys):
    for key in keys:
        value = str(os.environ.get(key) or "").strip()
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


def _qwen3_tts_runtime_base_url():
    override = _speech_runtime_base_url_override("qwen3_tts")
    if override:
        return override
    if _running_in_container():
        return DEFAULT_QWEN3_TTS_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_QWEN3_TTS_RUNTIME_BASE_URL


def _sherpa_onnx_runtime_base_url():
    override = _speech_runtime_base_url_override("sherpa_onnx")
    if override:
        return override
    if _running_in_container():
        return DEFAULT_SHERPA_ONNX_RUNTIME_DOCKER_BASE_URL
    return DEFAULT_SHERPA_ONNX_RUNTIME_BASE_URL

def infer_openclaw_dir(explicit_dir=None):
    if explicit_dir:
        return Path(explicit_dir).expanduser().resolve()

    env_dir = os.environ.get("OPENCLAW_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()

    script_path = Path(__file__).resolve()
    for parent in script_path.parents:
        if parent.name.startswith("workspace-"):
            return parent.parent

    return Path.home() / ".openclaw"


def parse_iso(value):
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def now_utc():
    return datetime.now(timezone.utc)


def now_iso():
    return now_utc().isoformat().replace("+00:00", "Z")


def epoch_ms_to_iso(value):
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        return ""


def parse_openclaw_release(value):
    if not value:
        return None
    parts = []
    for item in str(value).split("."):
        if not item.isdigit():
            return None
        parts.append(int(item))
    return tuple(parts)


def is_supported_openclaw_release(value):
    parsed = parse_openclaw_release(value)
    baseline = parse_openclaw_release(OPENCLAW_BASELINE_RELEASE)
    if not parsed or not baseline:
        return False
    return parsed >= baseline


def format_age(dt, now):
    if dt is None:
        return "无信号"
    delta = now - dt.astimezone(timezone.utc)
    total_seconds = int(delta.total_seconds())
    if total_seconds < 60:
        return "刚刚"
    if total_seconds < 3600:
        return f"{total_seconds // 60} 分钟前"
    if total_seconds < 86400:
        return f"{total_seconds // 3600} 小时前"
    return f"{delta.days} 天前"


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError, ValueError):
        return default


def safe_list(value):
    return value if isinstance(value, list) else []


def parse_json_payload(*candidates, default=None):
    decoder = json.JSONDecoder()
    for candidate in candidates:
        if not candidate:
            continue
        text = str(candidate).strip()
        if not text:
            continue
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        for index, char in enumerate(text):
            if char not in "[{":
                continue
            try:
                payload, _end = decoder.raw_decode(text[index:])
                return payload
            except json.JSONDecodeError:
                continue
    return deepcopy(default)


def _sanitize_browser_config_for_write(browser):
    normalized = deepcopy(browser) if isinstance(browser, dict) else {}
    profiles = normalized.get("profiles") if isinstance(normalized.get("profiles"), dict) else {}
    next_profiles = {}
    for raw_name, raw_payload in profiles.items():
        name = str(raw_name or "").strip()
        if not name:
            continue
        payload = deepcopy(raw_payload) if isinstance(raw_payload, dict) else {}
        driver = str(payload.get("driver") or "").strip()
        # Legacy profile translation happens during install/sync. Runtime only
        # keeps profiles that already match the current OpenClaw schema.
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


def load_config(openclaw_dir):
    config_path = Path(openclaw_dir) / "openclaw.json"
    config = load_json(config_path, {})
    sanitized = sanitize_runtime_secret_placeholders(openclaw_dir, sanitize_openclaw_config_for_write(config))
    if sanitized != config:
        config_path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return sanitized
    return config


def sanitize_openclaw_config_for_write(config):
    sanitized = deepcopy(config) if isinstance(config, dict) else {}
    mission_control = sanitized.get("missionControl") if isinstance(sanitized.get("missionControl"), dict) else {}
    project_dir = str(mission_control.get("projectDir") or "").strip()
    for key in ("sandbox", "missionControlDesktop", "missionControl"):
        sanitized.pop(key, None)
    if project_dir:
        sanitized["missionControl"] = {"projectDir": project_dir}
    plugins = sanitized.get("plugins") if isinstance(sanitized.get("plugins"), dict) else {}
    plugins.pop("allow", None)
    entries = plugins.get("entries") if isinstance(plugins.get("entries"), dict) else {}
    browser = sanitized.get("browser") if isinstance(sanitized.get("browser"), dict) else {}
    if browser.get("enabled", True) is not False:
        browser_entry = entries.get("browser") if isinstance(entries.get("browser"), dict) else {}
        browser_entry.setdefault("enabled", True)
        entries["browser"] = browser_entry
    if entries:
        plugins["entries"] = entries
    if plugins:
        sanitized["plugins"] = plugins
    browser = sanitized.get("browser")
    if isinstance(browser, dict):
        sanitized["browser"] = _sanitize_browser_config_for_write(browser)
    session = sanitized.get("session")
    if isinstance(session, dict):
        session.pop("transcriptPath", None)
        sanitized["session"] = session
    tools = sanitized.get("tools") if isinstance(sanitized.get("tools"), dict) else {}
    sessions = tools.get("sessions") if isinstance(tools.get("sessions"), dict) else {}
    sessions.pop("transcript", None)
    sessions["visibility"] = DEFAULT_OPENCLAW_SESSION_VISIBILITY
    tools["sessions"] = sessions
    sanitized["tools"] = tools
    channels = sanitized.get("channels")
    if isinstance(channels, dict):
        feishu = channels.get("feishu")
        if isinstance(feishu, dict):
            feishu.pop("commands", None)
            channels["feishu"] = feishu
        sanitized["channels"] = channels
    gateway = sanitized.get("gateway") if isinstance(sanitized.get("gateway"), dict) else {}
    gateway_http = gateway.get("http") if isinstance(gateway.get("http"), dict) else {}
    gateway_endpoints = gateway_http.get("endpoints") if isinstance(gateway_http.get("endpoints"), dict) else {}
    chat_completions = gateway_endpoints.get("chatCompletions") if isinstance(gateway_endpoints.get("chatCompletions"), dict) else {}
    chat_completions["enabled"] = True
    gateway_endpoints["chatCompletions"] = chat_completions
    gateway_http["endpoints"] = gateway_endpoints
    gateway["http"] = gateway_http
    sanitized["gateway"] = gateway
    return sanitized


def sanitize_runtime_secret_placeholders(openclaw_dir, config):
    sanitized = deepcopy(config) if isinstance(config, dict) else {}

    def resolved_secret(env_key):
        return str(os.environ.get(env_key) or read_env_value(openclaw_dir, env_key) or "").strip()

    def is_placeholder(value, env_key):
        return str(value or "").strip() == f"${{{env_key}}}"

    channels = sanitized.get("channels") if isinstance(sanitized.get("channels"), dict) else {}
    for channel_name, secret_key, env_key, clear_keys in (
        ("feishu", "appSecret", "FEISHU_APP_SECRET", ()),
        ("telegram", "botToken", "TELEGRAM_BOT_TOKEN", ("proxy",)),
        ("qqbot", "clientSecret", "QQBOT_CLIENT_SECRET", ()),
        ("qq", "clientSecret", "QQBOT_CLIENT_SECRET", ()),
    ):
        channel = channels.get(channel_name) if isinstance(channels.get(channel_name), dict) else {}
        if not channel:
            continue
        current = str(channel.get(secret_key) or "").strip()
        secret = resolved_secret(env_key)
        if is_placeholder(current, env_key) or (not current and secret):
            if secret:
                channel[secret_key] = secret
                channel["enabled"] = True
            else:
                channel.pop(secret_key, None)
                channel["enabled"] = False
                for extra_key in clear_keys:
                    channel.pop(extra_key, None)
            channels[channel_name] = channel
    if channels:
        sanitized["channels"] = channels

    gateway = sanitized.get("gateway") if isinstance(sanitized.get("gateway"), dict) else {}
    auth = gateway.get("auth") if isinstance(gateway.get("auth"), dict) else {}
    if auth:
        current = str(auth.get("token") or "").strip()
        secret = resolved_secret("GATEWAY_AUTH_TOKEN")
        if is_placeholder(current, "GATEWAY_AUTH_TOKEN") or (not current and secret):
            if secret:
                auth["token"] = secret
            else:
                auth.pop("token", None)
            gateway["auth"] = auth
            sanitized["gateway"] = gateway
    return sanitized


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


def _normalize_local_runtime(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    runtime = normalized.get(LOCAL_RUNTIME_CONFIG_KEY)
    runtime = deepcopy(runtime) if isinstance(runtime, dict) else {}
    kv_cache = runtime.get("kvCache")
    kv_cache = deepcopy(kv_cache) if isinstance(kv_cache, dict) else {}
    runtime["enabled"] = _normalize_bool(runtime.get("enabled"), False)
    runtime["backend"] = _normalize_str(runtime.get("backend"), DEFAULT_LOCAL_RUNTIME_BACKEND)
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


def _normalize_speech_runtime(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    runtime = normalized.get(SPEECH_RUNTIME_CONFIG_KEY)
    runtime = deepcopy(runtime) if isinstance(runtime, dict) else {}
    default_provider = _default_speech_runtime_provider()
    provider = _speech_runtime_env_provider() or _normalize_str(runtime.get("provider"), default_provider).lower()
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
    runtime["baseUrl"] = _speech_runtime_base_url_override(provider) or _normalize_str(runtime.get("baseUrl"), default_base_url)
    normalized_model = _speech_runtime_model_override(provider) or _normalize_str(runtime.get("model"), default_model)
    if provider == "qwen3_tts" and normalized_model in {"cosyvoice-300m-sft", "cosyvoice-300m-instruct"}:
        normalized_model = default_model
    runtime["model"] = normalized_model
    runtime["apiKeyEnv"] = _speech_runtime_api_key_env_override(provider) or _normalize_str(runtime.get("apiKeyEnv"), default_api_key_env)
    normalized[SPEECH_RUNTIME_CONFIG_KEY] = runtime
    return normalized


def save_config(openclaw_dir, config):
    config_path = Path(openclaw_dir) / "openclaw.json"
    sanitized = sanitize_runtime_secret_placeholders(openclaw_dir, sanitize_openclaw_config_for_write(config))
    config_path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def legacy_project_metadata(config):
    if not isinstance(config, dict):
        return {}
    metadata = config.get(PROJECT_METADATA_KEY, {})
    if isinstance(metadata, dict) and metadata:
        return deepcopy(metadata)
    return {}


def project_metadata_path(openclaw_dir):
    return Path(openclaw_dir) / PROJECT_METADATA_FILENAME


def project_metadata_candidate_paths(openclaw_dir):
    base = Path(openclaw_dir)
    return (base / PROJECT_METADATA_FILENAME,)


def normalize_project_metadata(metadata):
    normalized = deepcopy(metadata) if isinstance(metadata, dict) else {}
    theme_name = str(normalized.get("theme") or "").strip()
    if theme_name != DEFAULT_THEME_NAME:
        normalized["theme"] = DEFAULT_THEME_NAME
        normalized["displayName"] = THEME_CATALOG[DEFAULT_THEME_NAME]["displayName"]
    elif not str(normalized.get("displayName") or "").strip():
        normalized["displayName"] = THEME_CATALOG[DEFAULT_THEME_NAME]["displayName"]
    deployment_mode = str(normalized.get("deploymentMode") or "").strip().lower()
    if deployment_mode not in {"single_tenant", "shared_platform"}:
        normalized["deploymentMode"] = "single_tenant"
    deployment_profile = str(normalized.get("deploymentProfile") or "").strip().lower()
    if not deployment_profile:
        normalized["deploymentProfile"] = "standard"
    return _normalize_speech_runtime(_normalize_execution(_normalize_local_runtime(normalized)))


def load_project_metadata(openclaw_dir, config=None):
    data = {}
    for path in project_metadata_candidate_paths(openclaw_dir):
        data = load_json(path, {})
        if isinstance(data, dict) and data:
            break
    config = config or load_config(openclaw_dir)
    legacy = legacy_project_metadata(config)
    if isinstance(data, dict) and data:
        return normalize_project_metadata({**legacy, **data})
    if legacy:
        return normalize_project_metadata(legacy)
    inferred_theme = infer_theme_name_from_agents(config)
    return normalize_project_metadata({
        "theme": inferred_theme,
        "displayName": THEME_CATALOG.get(inferred_theme, {}).get("displayName", inferred_theme),
        "projectDir": "",
        "taskPrefix": "",
    })


def save_project_metadata(openclaw_dir, metadata):
    path = project_metadata_path(openclaw_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_project_metadata(metadata)
    payload = {
        key: value
        for key, value in deepcopy(normalized or {}).items()
        if value not in (None, "")
    }
    payload["updatedAt"] = now_iso()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def is_online_reachable_status(value):
    status = str(value or "").strip().lower()
    return status in {"active", "running", "online", "standby"}


def extract_current_phase(task_plan_text):
    match = re.search(r"## Current Phase\s+([^\n]+)", str(task_plan_text or ""), flags=re.IGNORECASE)
    return match.group(1).strip() if match else ""


def extract_latest_progress(progress_text):
    lines = [line.strip() for line in str(progress_text or "").splitlines() if line.strip()]
    for line in reversed(lines):
        if line.startswith("-") and len(line) > 1:
            return line[1:].strip()
    return lines[-1] if lines else ""


def extract_text_from_content(content):
    texts = []
    for item in content or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "text" and item.get("text"):
            texts.append(str(item.get("text")))
    return "\n\n".join(part.strip() for part in texts if part and str(part).strip()).strip()


def summarize_json(value, max_chars=180):
    try:
        rendered = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError):
        rendered = str(value)
    rendered = rendered.strip()
    if len(rendered) <= max_chars:
        return rendered
    return rendered[: max_chars - 1].rstrip() + "…"


def workspace_last_activity(workspace):
    latest = None
    workspace = Path(workspace)
    if not workspace.exists():
        return None
    for path in workspace.rglob("*"):
        if path.is_file() and ".git" not in str(path):
            dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if latest is None or dt > latest:
                latest = dt
    return latest


def latest_progress_event(task, router_agent_id=""):
    progress_log = [entry for entry in safe_list((task or {}).get("progress_log")) if isinstance(entry, dict)]
    if not progress_log:
        return None
    ordered = sorted(
        progress_log,
        key=lambda entry: parse_iso(entry.get("at")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    for entry in ordered:
        if not is_control_plane_progress_entry(task, entry, router_agent_id=router_agent_id):
            return entry
    return ordered[0]


def todo_summary(task):
    todos = task.get("todos", [])
    if not todos:
        return {"total": 0, "completed": 0, "ratio": 0}
    completed = sum(1 for item in todos if item.get("status") == "completed")
    return {
        "total": len(todos),
        "completed": completed,
        "ratio": int((completed / len(todos)) * 100),
    }


def build_label_maps(agents, kanban_cfg, router_agent_id):
    agent_labels = dict(kanban_cfg.get("agent_labels", {}))
    reverse = defaultdict(set)

    for agent in agents:
        agent_id = agent["id"]
        title_label = agent_labels.get(agent_id) or agent.get("identity", {}).get("name") or agent_id
        agent_labels.setdefault(agent_id, title_label)
        reverse[title_label].add(agent_id)
        reverse[agent_id].add(agent_id)

        identity_name = agent.get("identity", {}).get("name")
        if identity_name:
            reverse[identity_name].add(agent_id)

    if router_agent_id not in agent_labels:
        agent_labels[router_agent_id] = next(
            (agent.get("identity", {}).get("name", router_agent_id) for agent in agents if agent["id"] == router_agent_id),
            router_agent_id,
        )
    reverse[agent_labels[router_agent_id]].add(router_agent_id)

    return agent_labels, reverse


def detect_requested_priority(text):
    source = str(text or "").lower()
    if any(token in source for token in ("p0", "critical", "紧急", "s级", "sev0")):
        return "critical"
    if any(token in source for token in ("p1", "high", "高优", "优先", "release")):
        return "high"
    if any(token in source for token in ("p2", "low", "低优", "backlog")):
        return "low"
    return "normal"


def router_workspace_path(openclaw_dir, router_agent_id):
    return Path(openclaw_dir) / f"workspace-{router_agent_id}"


def build_management_approval_action(task, reason):
    workflow_binding = task.get("workflowBinding") if isinstance(task.get("workflowBinding"), dict) else {}
    selected_branch = workflow_binding.get("selectedBranch") if isinstance(workflow_binding.get("selectedBranch"), dict) else {}
    workflow_id = str(workflow_binding.get("workflowId") or "").strip()
    if not workflow_id:
        return {
            "type": "focus_task",
            "label": "查看任务",
            "taskId": task.get("id", ""),
        }
    return {
        "type": "insert_approval_node",
        "label": "前置人工复核",
        "payload": {
            "workflowId": workflow_id,
            "targetLaneId": str(selected_branch.get("targetLaneId") or "").strip(),
            "targetAgentId": str(task.get("targetAgentId") or "").strip(),
            "title": "人工复核",
            "approver": "运营负责人",
            "timeout": 30,
            "reason": reason,
        },
    }


def elapsed_minutes(dt, now):
    if not dt:
        return None
    return max(0, int((now - dt).total_seconds() // 60))


def normalize_for_signature(value):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if key in {"signature", "generatedAt", "generatedAgo"} or key.endswith("Ago"):
                continue
            cleaned[key] = normalize_for_signature(item)
        return cleaned
    if isinstance(value, list):
        return [normalize_for_signature(item) for item in value]
    return value


def read_env_value(openclaw_dir, key):
    env_path = Path(openclaw_dir) / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key == key:
            return value.strip()
    return ""


def users_store_path(openclaw_dir):
    return dashboard_dir(openclaw_dir) / "product_users.json"


def normalize_markdown_title(value, fallback):
    title = str(value or "").strip()
    title = re.sub(r"^#+\s*", "", title).strip()
    return title or fallback


def summarize_markdown_content(text, fallback=""):
    lines = []
    heading = ""
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            if not heading:
                heading = normalize_markdown_title(line, "")
            continue
        line = re.sub(r"\s+", " ", line)
        lines.append(line)
        if len(" ".join(lines)) >= 260:
            break
    summary = " ".join(lines).strip()
    if len(summary) > 260:
        summary = summary[:257].rstrip() + "..."
    return heading, summary or fallback


def build_markdown_document(path, *, root=None, now=None, kind="", agent_id="", agent_title="", label=""):
    document_path = Path(path)
    if not document_path.exists() or not document_path.is_file():
        return None
    try:
        content = document_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        content = ""
    stat = document_path.stat()
    updated_dt = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    heading, preview = summarize_markdown_content(content, fallback="暂无内容摘要。")
    relative = document_path.name
    if root:
        try:
            relative = str(document_path.resolve().relative_to(Path(root).resolve())).replace(os.sep, "/")
        except ValueError:
            relative = document_path.name
    title = normalize_markdown_title(label or heading, document_path.stem.replace("-", " "))
    return {
        "id": relative,
        "name": document_path.name,
        "title": title,
        "kind": kind or "document",
        "path": str(document_path),
        "relativePath": relative,
        "agentId": agent_id,
        "agentTitle": agent_title,
        "content": content[:12000],
        "truncated": len(content) > 12000,
        "preview": preview,
        "charCount": len(content),
        "lineCount": len(content.splitlines()),
        "bytes": stat.st_size,
        "updatedAt": updated_dt.isoformat().replace("+00:00", "Z"),
        "updatedAgo": format_age(updated_dt, now or datetime.now(timezone.utc)),
    }


def safe_download_name(value, fallback="deliverable"):
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip("-._")
    return normalized or fallback


def build_recommended_handoff_note(node, lane):
    stage_title = str(node.get("title") or lane.get("title") or "当前阶段").strip() or "当前阶段"
    lane_title = str(lane.get("title") or stage_title).strip() or stage_title
    return "\n".join(
        [
            f"{stage_title} 交接清单：",
            f"- 当前阶段目标：说明 {lane_title} 这一跳要完成的结果和交付边界。",
            "- 上一跳结论：补充已经确认的事实、决策和依赖状态。",
            "- 关键风险：写清还未解决的风险、阻塞点和需要继续观察的信号。",
            "- 输出物要求：说明这一跳必须产出的文档、回复、代码或验收结果。",
            "- 下一跳提醒：告诉接手 Agent 必须优先确认的事项和完成标准。",
        ]
    )


def stream_text_delta(previous_text, next_text):
    prior = str(previous_text or "")
    current = str(next_text or "")
    if not prior:
        return current
    if current.startswith(prior):
        return current[len(prior) :]
    return current


def schedule_runtime_restart(server, router_agent_id):
    if not getattr(server, "allow_runtime_restart", True):
        return {"scheduled": False, "reason": "disabled"}
    openclaw_dir = Path(getattr(server, "openclaw_dir", "")).expanduser().resolve()
    if not router_agent_id:
        return {"scheduled": False, "reason": "missing_router"}
    script_path = openclaw_dir / f"workspace-{router_agent_id}" / "scripts" / "collaboration_dashboard.py"
    if not script_path.exists():
        return {"scheduled": False, "reason": "missing_script", "script": str(script_path)}
    port = int((getattr(server, "server_address", ("127.0.0.1", 18890)) or ("127.0.0.1", 18890))[1])
    current_pid = os.getpid()
    log_dir = openclaw_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "mission-control.log"
    python_bin = shlex.quote(shutil.which("python3") or "python3")
    command = (
        f"sleep 1.5; "
        f"kill {current_pid} >/dev/null 2>&1 || true; "
        f"nohup {python_bin} {shlex.quote(str(script_path))} "
        f"--dir {shlex.quote(str(openclaw_dir))} --serve --port {port} "
        f"> {shlex.quote(str(log_path))} 2>&1 &"
    )
    subprocess.Popen(
        ["/bin/zsh", "-lc", command],
        cwd=str(openclaw_dir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    return {"scheduled": True, "routerAgentId": router_agent_id, "script": str(script_path), "port": port}


def _platform_runtime_governance_skill_pack_counters(skills_data):
    skills = safe_list((skills_data or {}).get("skills"))
    packs = safe_list((skills_data or {}).get("packs"))
    skill_runtime_counter = Counter()
    pack_runtime_counter = Counter()
    runtime_pack_count = 0
    text_only_pack_count = 0
    for skill in skills:
        for runtime_name in clean_unique_strings(skill.get("requiresRuntime") or []):
            skill_runtime_counter[runtime_name] += 1
    for pack in packs:
        required_runtimes = clean_unique_strings(pack.get("requiredRuntimes") or [])
        if required_runtimes:
            runtime_pack_count += 1
        else:
            text_only_pack_count += 1
        for runtime_name in required_runtimes:
            pack_runtime_counter[runtime_name] += 1
    return {
        "skills": skills,
        "packs": packs,
        "skillRuntimeCounter": skill_runtime_counter,
        "packRuntimeCounter": pack_runtime_counter,
        "runtimePackCount": runtime_pack_count,
        "textOnlyPackCount": text_only_pack_count,
    }


def _platform_runtime_governance_pack_rows(packs):
    pack_rows = []
    for pack in safe_list(packs):
        if not isinstance(pack, dict):
            continue
        required_runtimes = clean_unique_strings(pack.get("requiredRuntimes") or [])
        classification = "runtime-required" if required_runtimes else "text-only"
        pack_rows.append(
            {
                "id": str(pack.get("id") or "").strip(),
                "name": str(pack.get("name") or pack.get("id") or "").strip(),
                "mode": str(pack.get("mode") or "").strip(),
                "defaultEntry": str(pack.get("defaultEntry") or "").strip(),
                "requiredRuntimes": required_runtimes,
                "classification": classification,
                "hydrationStatus": str(pack.get("hydrationStatus") or "").strip() or ("incomplete" if pack.get("incomplete") else "ready"),
                "skillCount": int(pack.get("skillCount") or 0),
                "resolvedSkillCount": int(pack.get("resolvedSkillCount") or 0),
            }
        )
    pack_rows.sort(
        key=lambda item: (
            item.get("classification") != "runtime-required",
            -len(item.get("requiredRuntimes") or []),
            item.get("name") or item.get("id") or "",
        )
    )
    return pack_rows


def _platform_runtime_governance_browser_session_rows(management_runs):
    browser_session_rows = []
    for run in safe_list(management_runs):
        if not isinstance(run, dict):
            continue
        browser_session = run.get("browserSession") if isinstance(run.get("browserSession"), dict) else {}
        if not browser_session:
            continue
        browser_session_rows.append(
            {
                "runId": str(run.get("id") or "").strip(),
                "runTitle": str(run.get("title") or run.get("id") or "").strip(),
                "profile": str(browser_session.get("profile") or "").strip(),
                "status": str(browser_session.get("status") or "idle").strip(),
                "cookieBootstrapStatus": str(browser_session.get("cookieBootstrapStatus") or "").strip(),
                "requiresCookieBootstrap": bool(browser_session.get("requiresCookieBootstrap")),
                "lastSnapshotAt": str(browser_session.get("lastSnapshotAt") or "").strip(),
            }
        )
    return browser_session_rows


def build_platform_runtime_governance_summary(skills_data, management_runs=None):
    counters = _platform_runtime_governance_skill_pack_counters(skills_data)
    management_runs = safe_list(management_runs)
    browser_session_rows = _platform_runtime_governance_browser_session_rows(management_runs)
    pack_runtime_counter = counters["packRuntimeCounter"]
    return {
        "summary": {
            "skillCount": len(counters["skills"]),
            "packCount": len(counters["packs"]),
            "runtimeSkillCount": sum(1 for skill in counters["skills"] if clean_unique_strings(skill.get("requiresRuntime") or [])),
            "runtimePackCount": counters["runtimePackCount"],
            "textOnlyPackCount": counters["textOnlyPackCount"],
            "browserPackCount": pack_runtime_counter.get("browser", 0),
            "gitPackCount": pack_runtime_counter.get("git", 0),
            "ghPackCount": pack_runtime_counter.get("gh", 0),
            "cookiePackCount": pack_runtime_counter.get("cookies", 0),
            "activeBrowserSessionCount": sum(1 for item in browser_session_rows if item.get("status") not in {"", "idle"}),
            "cookieBootstrappedRunCount": sum(
                1
                for item in browser_session_rows
                if item.get("requiresCookieBootstrap") and item.get("cookieBootstrapStatus") == "ready"
            ),
            "releaseAutomatedRunCount": sum(1 for item in management_runs if str(((item.get("releaseAutomation") or {}).get("status") or "")).strip() not in {"", "idle"}),
            "qaAutomationRunCount": sum(1 for item in management_runs if bool((item.get("qaAutomation") or {}).get("enabled"))),
        },
    }


def build_platform_runtime_governance_runtimes(skills_data, management_runs=None):
    counters = _platform_runtime_governance_skill_pack_counters(skills_data)
    runtime_rows = [
        {
            "name": runtime_name,
            "skillCount": counters["skillRuntimeCounter"].get(runtime_name, 0),
            "packCount": counters["packRuntimeCounter"].get(runtime_name, 0),
        }
        for runtime_name in sorted(set(counters["skillRuntimeCounter"]) | set(counters["packRuntimeCounter"]))
    ]
    return {"runtimes": runtime_rows}


def build_platform_runtime_governance_packs(skills_data, management_runs=None):
    counters = _platform_runtime_governance_skill_pack_counters(skills_data)
    return {"packs": _platform_runtime_governance_pack_rows(counters["packs"])}


def build_platform_runtime_governance_browser_sessions(skills_data, management_runs=None):
    browser_session_rows = _platform_runtime_governance_browser_session_rows(safe_list(management_runs))
    return {"browserSessions": browser_session_rows}


def pack_seeded_management_stages(pack):
    stages = []
    for index, stage in enumerate(normalize_workflow_pack_stages(pack.get("stages"), fallback_name=pack.get("name"), fallback_mode=pack.get("mode")), start=1):
        stages.append(
            {
                "key": str(stage.get("key") or f"stage-{index}").strip() or f"stage-{index}",
                "title": str(stage.get("title") or f"Stage {index}").strip() or f"Stage {index}",
                "status": "active" if index == 1 else "pending",
                "note": str(stage.get("description") or "").strip(),
                "updatedAt": now_iso() if index == 1 else "",
            }
        )
    return stages


def render_release_template(template, context):
    rendered = str(template or "").strip()
    for key, value in (context or {}).items():
        rendered = rendered.replace(f"{{{key}}}", str(value or ""))
    return rendered


def apply_run_gate_stage_transition(run, gate, action):
    stages = deepcopy(run.get("stages") or [])
    if not stages:
        return stages, "", ""
    target_stage_key = str((gate or {}).get("stageKey") or "").strip()
    now = now_iso()
    current_stage_key = str(run.get("stageKey") or stages[0].get("key") or "").strip()
    if action == "pass":
        next_stage_key = current_stage_key
        for index, stage in enumerate(stages):
            stage_key = str(stage.get("key") or "").strip()
            if stage_key == target_stage_key:
                stage["status"] = "done"
                stage["updatedAt"] = now
                if index + 1 < len(stages):
                    next_stage = stages[index + 1]
                    if str(next_stage.get("status") or "").strip().lower() != "done":
                        next_stage["status"] = "active"
                        next_stage["updatedAt"] = now
                    next_stage_key = str(next_stage.get("key") or "").strip() or next_stage_key
                else:
                    next_stage_key = stage_key
                break
        return stages, next_stage_key, ""
    if action == "block":
        for stage in stages:
            if str(stage.get("key") or "").strip() == target_stage_key:
                stage["status"] = "blocked"
                stage["updatedAt"] = now
        return stages, target_stage_key or current_stage_key, "blocked"
    if action in {"reset", "activate"}:
        for stage in stages:
            stage_key = str(stage.get("key") or "").strip()
            if stage_key == target_stage_key:
                stage["status"] = "active"
                stage["updatedAt"] = now
            elif str(stage.get("status") or "").strip().lower() not in {"done"}:
                stage["status"] = "pending"
        return stages, target_stage_key or current_stage_key, "active"
    return stages, current_stage_key, ""


def summarize_context_packet(entry):
    detail = str(entry.get("detail") or "").strip()
    if entry.get("kind") == "handoff":
        risk = "high" if not detail else ("watch" if len(detail) < 12 else "good")
        summary = detail or "这次 handoff 没有写交接说明。"
    else:
        risk = "watch" if not detail else ("good" if len(detail) >= 18 else "watch")
        summary = detail or "当前进展没有附带明确上下文。"
    return {"summary": summary, "risk": risk}


def classify_delivery_failure(delivery, channel=None):
    outcome = str((delivery or {}).get("outcome") or "").strip().lower()
    if outcome == "success":
        return {"id": "success", "label": "成功"}
    detail = str((delivery or {}).get("detail") or "").strip()
    detail_lower = detail.lower()
    channel_health = (channel or {}).get("meta", {}).get("health", {}) if isinstance((channel or {}).get("meta"), dict) else {}
    health_status = str(channel_health.get("status") or "").strip().lower()
    health_detail = str(channel_health.get("detail") or "").strip().lower()
    combined = " ".join(part for part in [detail_lower, health_detail] if part)
    if health_status == "disabled":
        reason_id = "channel_disabled"
    elif any(token in combined for token in ("http/https url", "unknown url type", "invalid", "missing", "未填写", "需要", "not-a-url")):
        reason_id = "config"
    elif any(token in combined for token in ("401", "403", "unauthorized", "forbidden", "token", "鉴权", "认证")):
        reason_id = "auth"
    elif any(token in combined for token in ("429", "rate limit", "too many requests", "限流")):
        reason_id = "rate_limit"
    elif any(token in combined for token in ("timeout", "timed out", "connection", "network", "temporary", "502", "503", "504", "连接", "网络")):
        reason_id = "transport"
    else:
        reason_id = "unknown"
    return {"id": reason_id, "label": DELIVERY_FAILURE_REASON_LABELS.get(reason_id, "未知原因"), "detail": detail}


def build_delivery_analytics(deliveries, channels, now):
    now = now or now_utc()
    channel_map = {
        str(item.get("id") or ""): item
        for item in safe_list(channels)
        if isinstance(item, dict) and item.get("id")
    }
    normalized_deliveries = []
    for item in safe_list(deliveries):
        if not isinstance(item, dict):
            continue
        delivered_at = parse_iso(item.get("deliveredAt"))
        channel = channel_map.get(str(item.get("channelId") or ""))
        failure = classify_delivery_failure(item, channel=channel)
        normalized_deliveries.append(
            {
                **item,
                "deliveredAtDt": delivered_at,
                "failureReason": failure,
            }
        )

    attempts = len(normalized_deliveries)
    success_count = sum(1 for item in normalized_deliveries if str(item.get("outcome") or "") == "success")
    error_count = attempts - success_count
    success_rate = round((success_count / attempts) * 100, 1) if attempts else 0.0

    day_buckets = {}
    for offset in range(6, -1, -1):
        day = (now - timedelta(days=offset)).date()
        key = day.isoformat()
        day_buckets[key] = {
            "date": key,
            "label": day.strftime("%m-%d"),
            "attempts": 0,
            "success": 0,
            "errors": 0,
        }
    for item in normalized_deliveries:
        delivered_dt = item.get("deliveredAtDt")
        if not delivered_dt:
            continue
        key = delivered_dt.date().isoformat()
        if key not in day_buckets:
            continue
        bucket = day_buckets[key]
        bucket["attempts"] += 1
        if str(item.get("outcome") or "") == "success":
            bucket["success"] += 1
        else:
            bucket["errors"] += 1

    reason_counter = Counter()
    reason_latest = {}
    reason_samples = {}
    for item in normalized_deliveries:
        if str(item.get("outcome") or "") == "success":
            continue
        failure = item.get("failureReason") or {}
        reason_id = failure.get("id", "unknown")
        reason_counter[reason_id] += 1
        delivered_dt = item.get("deliveredAtDt")
        if delivered_dt and (reason_id not in reason_latest or delivered_dt > reason_latest[reason_id]):
            reason_latest[reason_id] = delivered_dt
        if reason_id not in reason_samples and failure.get("detail"):
            reason_samples[reason_id] = failure.get("detail")

    failure_reasons = []
    for reason_id, count in reason_counter.most_common(5):
        latest_dt = reason_latest.get(reason_id)
        failure_reasons.append(
            {
                "id": reason_id,
                "label": DELIVERY_FAILURE_REASON_LABELS.get(reason_id, "未知原因"),
                "count": count,
                "latestAgo": format_age(latest_dt, now) if latest_dt else "未知时间",
                "sample": reason_samples.get(reason_id, ""),
            }
        )

    channel_stats = []
    for channel_id, channel in channel_map.items():
        channel_deliveries = [item for item in normalized_deliveries if item.get("channelId") == channel_id]
        attempts_count = len(channel_deliveries)
        success = sum(1 for item in channel_deliveries if str(item.get("outcome") or "") == "success")
        errors = attempts_count - success
        latest = next(
            iter(
                sorted(
                    channel_deliveries,
                    key=lambda item: item.get("deliveredAtDt") or datetime.fromtimestamp(0, tz=timezone.utc),
                    reverse=True,
                )
            ),
            None,
        )
        latest_dt = latest.get("deliveredAtDt") if latest else None
        latest_reason = (latest.get("failureReason") or {}) if latest and str(latest.get("outcome") or "") != "success" else {}
        channel_stats.append(
            {
                "id": channel_id,
                "name": channel.get("name", channel_id),
                "type": channel.get("type", ""),
                "attempts": attempts_count,
                "success": success,
                "errors": errors,
                "successRate": round((success / attempts_count) * 100, 1) if attempts_count else 0.0,
                "latestAgo": format_age(latest_dt, now) if latest_dt else "尚未投递",
                "latestOutcome": latest.get("outcome", "") if latest else "",
                "latestFailureReason": latest_reason.get("label", ""),
                "healthStatus": str(channel.get("meta", {}).get("health", {}).get("status") or "").strip().lower(),
            }
        )
    channel_stats.sort(key=lambda item: (-item.get("errors", 0), item.get("successRate", 0.0), -item.get("attempts", 0)))

    return {
        "attemptCount": attempts,
        "successCount": success_count,
        "errorCount": error_count,
        "successRate": success_rate,
        "failingChannels": sum(1 for item in channel_stats if item.get("errors", 0) > 0),
        "trend": list(day_buckets.values()),
        "failureReasons": failure_reasons,
        "channels": channel_stats,
        "latestFailures": [
            {
                "id": item.get("id", ""),
                "channelId": item.get("channelId", ""),
                "channelName": channel_map.get(str(item.get("channelId") or ""), {}).get("name", item.get("channelId", "")),
                "detail": item.get("detail", ""),
                "reasonLabel": (item.get("failureReason") or {}).get("label", "未知原因"),
                "deliveredAt": item.get("deliveredAt", ""),
                "deliveredAgo": format_age(item.get("deliveredAtDt"), now) if item.get("deliveredAtDt") else "未知时间",
            }
            for item in normalized_deliveries
            if str(item.get("outcome") or "") != "success"
        ][:10],
    }


def rule_channel_ids(rule):
    channel_ids = [str(item).strip() for item in safe_list((rule or {}).get("channelIds")) if str(item).strip()]
    meta = (rule or {}).get("meta") if isinstance((rule or {}).get("meta"), dict) else {}
    for step in safe_list(meta.get("escalationSteps")):
        for channel_id in safe_list((step or {}).get("channelIds")):
            normalized = str(channel_id).strip()
            if normalized:
                channel_ids.append(normalized)
    deduped = []
    for channel_id in channel_ids:
        if channel_id not in deduped:
            deduped.append(channel_id)
    return deduped


def perform_disable_notification_channel(openclaw_dir, channel_id):
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        raise RuntimeError("请提供通知通道编号。")
    channels = store_list_notification_channels(openclaw_dir)
    channel = next((item for item in channels if item.get("id") == channel_id), None)
    if not channel:
        raise RuntimeError("通知通道不存在。")
    if str(channel.get("status") or "") == "disabled":
        return channel
    meta = deepcopy(channel.get("meta", {})) if isinstance(channel.get("meta"), dict) else {}
    remediation = deepcopy(meta.get("automationRemediation", {})) if isinstance(meta.get("automationRemediation"), dict) else {}
    remediation.update(
        {
            "disabledByAutomation": True,
            "disabledAt": now_iso(),
        }
    )
    meta["automationRemediation"] = remediation
    return store_save_notification_channel(
        openclaw_dir,
        {
            **channel,
            "status": "disabled",
            "meta": meta,
        },
    )


def perform_enable_notification_channel(openclaw_dir, channel_id):
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        raise RuntimeError("请提供通知通道编号。")
    channels = store_list_notification_channels(openclaw_dir)
    channel = next((item for item in channels if item.get("id") == channel_id), None)
    if not channel:
        raise RuntimeError("通知通道不存在。")
    if str(channel.get("status") or "") == "active":
        return channel
    meta = deepcopy(channel.get("meta", {})) if isinstance(channel.get("meta"), dict) else {}
    remediation = deepcopy(meta.get("automationRemediation", {})) if isinstance(meta.get("automationRemediation"), dict) else {}
    remediation.update(
        {
            "disabledByAutomation": False,
            "restoredAt": now_iso(),
        }
    )
    meta["automationRemediation"] = remediation
    return store_save_notification_channel(
        openclaw_dir,
        {
            **channel,
            "status": "active",
            "meta": meta,
        },
    )


def perform_append_rule_backup_channel(openclaw_dir, rule_id, channel_id, after_minutes=15, label=""):
    rule_id = str(rule_id or "").strip()
    channel_id = str(channel_id or "").strip()
    if not rule_id or not channel_id:
        raise RuntimeError("请提供规则编号和通道编号。")
    rules = store_list_automation_rules(openclaw_dir)
    rule = next((item for item in rules if item.get("id") == rule_id), None)
    if not rule:
        raise RuntimeError("自动化规则不存在。")
    channels = store_list_notification_channels(openclaw_dir)
    channel = next((item for item in channels if item.get("id") == channel_id), None)
    if not channel:
        raise RuntimeError("通知通道不存在。")
    meta = deepcopy(rule.get("meta", {})) if isinstance(rule.get("meta"), dict) else {}
    steps = [deepcopy(step) for step in safe_list(meta.get("escalationSteps")) if isinstance(step, dict)]
    if channel_id in rule_channel_ids(rule):
        return rule
    normalized_after = max(int(after_minutes or 0), 0)
    for step in steps:
        if int(step.get("afterMinutes") or 0) == normalized_after:
            channel_ids = [str(item).strip() for item in safe_list(step.get("channelIds")) if str(item).strip()]
            if channel_id not in channel_ids:
                channel_ids.append(channel_id)
            step["channelIds"] = channel_ids
            if label and not step.get("label"):
                step["label"] = label
            break
    else:
        steps.append(
            {
                "afterMinutes": normalized_after,
                "channelIds": [channel_id],
                "label": str(label or f"{normalized_after} 分钟后补发到 {channel.get('name', channel_id)}").strip(),
            }
        )
    steps.sort(key=lambda item: int(item.get("afterMinutes") or 0))
    meta["escalationSteps"] = steps
    updated_rule = store_save_automation_rule(
        openclaw_dir,
        {
            **rule,
            "meta": meta,
        },
    )
    return updated_rule


def perform_append_rule_manual_escalation(openclaw_dir, rule_id, after_minutes=30, label=""):
    rule_id = str(rule_id or "").strip()
    if not rule_id:
        raise RuntimeError("请提供规则编号。")
    rules = store_list_automation_rules(openclaw_dir)
    rule = next((item for item in rules if item.get("id") == rule_id), None)
    if not rule:
        raise RuntimeError("自动化规则不存在。")
    meta = deepcopy(rule.get("meta", {})) if isinstance(rule.get("meta"), dict) else {}
    steps = [deepcopy(step) for step in safe_list(meta.get("escalationSteps")) if isinstance(step, dict)]
    if any(step.get("manual") for step in steps):
        return rule
    normalized_after = max(int(after_minutes or 0), 0)
    steps.append(
        {
            "afterMinutes": normalized_after,
            "channelIds": [],
            "label": str(label or f"{normalized_after} 分钟后人工接管").strip(),
            "manual": True,
        }
    )
    steps.sort(key=lambda item: int(item.get("afterMinutes") or 0))
    meta["escalationSteps"] = steps
    updated_rule = store_save_automation_rule(
        openclaw_dir,
        {
            **rule,
            "meta": meta,
        },
    )
    return updated_rule


def parse_cors_origins(value):
    raw = str(value or "").strip()
    if not raw:
        return set(DEFAULT_FRONTEND_ORIGINS)
    return {item.strip() for item in raw.split(",") if item.strip()}


def guess_content_type(path):
    guessed, _encoding = mimetypes.guess_type(str(path))
    return guessed or "application/octet-stream"
