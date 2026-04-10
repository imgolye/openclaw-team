from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import mimetypes
import os
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

CUSTOMER_VOICE_RUNTIME_PROBE_CACHE_TTL_SECONDS = 30
CUSTOMER_VOICE_RUNTIME_PROBE_CACHE = {}
CUSTOMER_VOICE_BACKGROUND_JOBS = set()
CUSTOMER_VOICE_BACKGROUND_JOBS_LOCK = threading.Lock()
LOGGER = logging.getLogger("mission-control.customer-voice")


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


WECHAT_ACCESS_TOKEN_CACHE = _DelegatedSymbol("WECHAT_ACCESS_TOKEN_CACHE")
WECHAT_ACCESS_TOKEN_REFRESH_BUFFER_SECONDS = _DelegatedSymbol("WECHAT_ACCESS_TOKEN_REFRESH_BUFFER_SECONDS")
WECHAT_OFFICIAL_REPLY_FALLBACK = _DelegatedSymbol("WECHAT_OFFICIAL_REPLY_FALLBACK")
WECHAT_VOICE_REPLY_DEFAULT_VOICE = _DelegatedSymbol("WECHAT_VOICE_REPLY_DEFAULT_VOICE")
WECHAT_VOICE_REPLY_MAX_CHARS = _DelegatedSymbol("WECHAT_VOICE_REPLY_MAX_CHARS")
WECHAT_VOICE_REPLY_MAX_TEXT_SEND_CHARS = _DelegatedSymbol("WECHAT_VOICE_REPLY_MAX_TEXT_SEND_CHARS")
WECHAT_VOICE_REPLY_SUPPORTED_VOICES = _DelegatedSymbol("WECHAT_VOICE_REPLY_SUPPORTED_VOICES")
WECHAT_VOICE_REPLY_TIMEOUT_SECONDS = _DelegatedSymbol("WECHAT_VOICE_REPLY_TIMEOUT_SECONDS")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
get_router_agent_id = _DelegatedSymbol("get_router_agent_id")
invalidate_dashboard_bundle_cache = _DelegatedSymbol("invalidate_dashboard_bundle_cache")
load_agents = _DelegatedSymbol("load_agents")
load_config = _DelegatedSymbol("load_config")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
save_project_metadata = _DelegatedSymbol("save_project_metadata")
agent_runtime_profile_payload = _DelegatedSymbol("agent_runtime_profile_payload")
merge_team_policy_state = _DelegatedSymbol("merge_team_policy_state")
normalize_chat_thread_linked_team_ids = _DelegatedSymbol("normalize_chat_thread_linked_team_ids")
now_iso = _DelegatedSymbol("now_iso")
openclaw_command_env = _DelegatedSymbol("openclaw_command_env")
resolve_chat_thread_participant_agent_ids = _DelegatedSymbol("resolve_chat_thread_participant_agent_ids")
resolve_chat_thread_team_records = _DelegatedSymbol("resolve_chat_thread_team_records")
run_command = _DelegatedSymbol("run_command")
safe_list = _DelegatedSymbol("safe_list")
schedule_chat_thread_dispatch = _DelegatedSymbol("schedule_chat_thread_dispatch")
select_human_turn_targets = _DelegatedSymbol("select_human_turn_targets")
store_list_chat_threads = _DelegatedSymbol("store_list_chat_threads")
store_list_chat_messages = _DelegatedSymbol("store_list_chat_messages")
store_list_customer_access_channels = _DelegatedSymbol("store_list_customer_access_channels")
publish_chat_thread_stream_event = _DelegatedSymbol("publish_chat_thread_stream_event")
store_save_chat_message = _DelegatedSymbol("store_save_chat_message")
store_save_chat_thread = _DelegatedSymbol("store_save_chat_thread")
store_save_customer_access_channel = _DelegatedSymbol("store_save_customer_access_channel")

CHAT_THREAD_VOICE_REPLY_TRIGGER_PATTERNS = (
    r"(语音|音频).{0,6}(回复|回答|返回|发我|发给我|发送|附上|输出)",
    r"(回复|回答|返回|发送).{0,6}(语音|音频)",
    r"(读出来|念出来|朗读|播报)",
    r"(voice|audio)\s+(reply|response|message|note)",
    r"(reply|respond|send).{0,16}(voice|audio)",
    r"read\s+(it|this)?\s*aloud",
    r"spoken\s+response",
)
DEFAULT_AGENT_VOICE_REPLY_SPEED = 1.0


def parse_boolish(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    normalized = str(value).strip().lower()
    if not normalized:
        return bool(default)
    if normalized in {"1", "true", "yes", "on", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(value)


def normalize_voice_reply_speed(value, default=DEFAULT_AGENT_VOICE_REPLY_SPEED):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = float(default or DEFAULT_AGENT_VOICE_REPLY_SPEED)
    numeric = max(0.8, min(1.2, numeric))
    return round(numeric, 2)


def customer_channel_voice_reply_config(channel):
    channel = channel if isinstance(channel, dict) else {}
    meta = channel.get("meta") if isinstance(channel.get("meta"), dict) else {}
    raw_voice = str(meta.get("voiceReplyVoice") or "").strip()
    voice_mode = "agent_profile" if raw_voice in {"follow_agent_profile", "agent_profile"} else "fixed"
    voice = ""
    if voice_mode == "fixed":
        voice = raw_voice or WECHAT_VOICE_REPLY_DEFAULT_VOICE
    return {
        "enabled": parse_boolish(meta.get("voiceReplyEnabled"), False),
        "mode": voice_mode,
        "voice": voice,
        "instructions": str(meta.get("voiceReplyInstructions") or "").strip(),
    }


def chat_thread_message_requests_voice_reply(message_text):
    text = str(message_text or "").strip()
    if not text:
        return False
    return any(
        re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        for pattern in CHAT_THREAD_VOICE_REPLY_TRIGGER_PATTERNS
    )


def resolve_agent_voice_reply_profile(openclaw_dir, agent_id):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return {
            "voice": WECHAT_VOICE_REPLY_DEFAULT_VOICE,
            "speed": normalize_voice_reply_speed(DEFAULT_AGENT_VOICE_REPLY_SPEED),
        }
    profile = agent_runtime_profile_payload(openclaw_dir, normalized_agent_id)
    profile = profile if isinstance(profile, dict) else {}
    metadata = load_project_metadata(openclaw_dir)
    overrides = metadata.get("agentOverrides") if isinstance(metadata.get("agentOverrides"), dict) else {}
    override = overrides.get(normalized_agent_id) if isinstance(overrides.get(normalized_agent_id), dict) else {}
    runtime_config = customer_voice_runtime_config(openclaw_dir)
    runtime_provider = str(runtime_config.get("provider") or "").strip().lower()
    available_voices = _customer_voice_runtime_available_voices(openclaw_dir, runtime_config)
    builtin_voices = _customer_voice_runtime_builtin_voices(openclaw_dir, runtime_config)
    preferred_builtin_voices = (
        _preferred_qwen3_builtin_voices(builtin_voices or available_voices)
        if runtime_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3
        else _preferred_cosyvoice_builtin_voices(builtin_voices or available_voices)
    )
    configured_voice = str(profile.get("voiceReplyVoice") or WECHAT_VOICE_REPLY_DEFAULT_VOICE).strip() or WECHAT_VOICE_REPLY_DEFAULT_VOICE
    existing_sample_payload = customer_voice_effective_custom_sample_payload(
        openclaw_dir,
        normalized_agent_id,
        profile=profile,
    )
    existing_sample_voice_id = str(existing_sample_payload.get("voiceId") or "").strip()
    if existing_sample_voice_id:
        configured_voice = existing_sample_voice_id
    elif customer_voice_custom_voice_agent_id(configured_voice) == normalized_agent_id:
        configured_voice = ""
    default_voice = str(preferred_builtin_voices[0] if preferred_builtin_voices else (available_voices[0] if available_voices else WECHAT_VOICE_REPLY_DEFAULT_VOICE)).strip() or WECHAT_VOICE_REPLY_DEFAULT_VOICE
    if (
        runtime_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3
        and _should_auto_assign_distinct_qwen3_voice(
            configured_voice,
            override.get("source"),
            default_voice,
            preferred_builtin_voices or available_voices,
        )
    ):
        configured_voice = _distinct_qwen3_voice_for_agent(openclaw_dir, normalized_agent_id, preferred_builtin_voices or available_voices)
    if (
        runtime_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE
        and _should_auto_assign_distinct_cosyvoice_voice(
            configured_voice,
            override.get("source"),
            default_voice,
            preferred_builtin_voices or available_voices,
        )
    ):
        configured_voice = _distinct_cosyvoice_voice_for_agent(openclaw_dir, normalized_agent_id, preferred_builtin_voices or available_voices)
    voice = _customer_voice_provider_voice_name(
        configured_voice,
        provider=runtime_config.get("provider"),
        available_voices=available_voices,
        agent_id=normalized_agent_id,
        openclaw_dir=openclaw_dir,
    )
    speed = normalize_voice_reply_speed(profile.get("voiceReplySpeed"), default=DEFAULT_AGENT_VOICE_REPLY_SPEED)
    instructions = str(profile.get("voiceReplyInstructions") or "").strip()
    if not instructions:
        human_name = str(profile.get("humanName") or "").strip()
        job_title = str(profile.get("jobTitle") or "").strip()
        working_style = str(profile.get("workingStyle") or "").strip()
        guidance_parts = [
            "请像真人在微信里发语音一样自然说话。",
            "语气温和、口语化、有轻微情绪起伏，不要像播报器或机器朗读。",
            "停顿自然，表达简洁，像同事当面语音回复一样。",
        ]
        if human_name or job_title:
            identity = "，".join(item for item in [human_name, job_title] if item)
            guidance_parts.append(f"保持这个成员的身份感：{identity}。")
        if working_style:
            guidance_parts.append(f"协作与表达风格参考：{working_style}")
        instructions = " ".join(part for part in guidance_parts if part).strip()
    return {"voice": voice, "speed": speed, "instructions": instructions}


def maybe_attach_chat_thread_voice_reply(openclaw_dir, thread, message, request_text="", server=None):
    if not chat_thread_message_requests_voice_reply(request_text):
        return message
    message = message if isinstance(message, dict) else {}
    reply_text = str(message.get("body") or "").strip()
    if not reply_text:
        return message
    thread_id = str(message.get("threadId") or (thread or {}).get("id") or "").strip()
    message_id = str(message.get("id") or "").strip()
    if not thread_id or not message_id:
        return message
    existing_meta = message.get("meta") if isinstance(message.get("meta"), dict) else {}
    attachments = [dict(item) for item in safe_list(existing_meta.get("attachments")) if isinstance(item, dict)]
    for item in attachments:
        if str(item.get("source") or "").strip() == "member_voice_reply":
            if existing_meta.get("voiceReplyPending") or existing_meta.get("voiceReplyFailed"):
                normalized_meta = {
                    **existing_meta,
                    "voiceReplyRequested": True,
                    "voiceReplyPending": False,
                    "voiceReplyFailed": False,
                }
                saved = store_save_chat_message(
                    openclaw_dir,
                    {
                        "id": message_id,
                        "threadId": thread_id,
                        "senderKind": str(message.get("senderKind") or "agent").strip() or "agent",
                        "senderId": str(message.get("senderId") or "").strip(),
                        "senderLabel": str(message.get("senderLabel") or "").strip(),
                        "direction": str(message.get("direction") or "agent").strip() or "agent",
                        "body": reply_text,
                        "createdAt": str(message.get("createdAt") or "").strip(),
                        "meta": normalized_meta,
                    },
                )
                return saved or {**message, "meta": normalized_meta}
            return message
    pending_meta = {
        **existing_meta,
        "voiceReplyRequested": True,
        "voiceReplyPending": True,
        "voiceReplyFailed": False,
    }
    if (
        existing_meta.get("voiceReplyRequested") is not True
        or existing_meta.get("voiceReplyPending") is not True
        or existing_meta.get("voiceReplyFailed")
    ):
        saved_pending = store_save_chat_message(
            openclaw_dir,
            {
                "id": message_id,
                "threadId": thread_id,
                "senderKind": str(message.get("senderKind") or "agent").strip() or "agent",
                "senderId": str(message.get("senderId") or "").strip(),
                "senderLabel": str(message.get("senderLabel") or "").strip(),
                "direction": str(message.get("direction") or "agent").strip() or "agent",
                "body": reply_text,
                "createdAt": str(message.get("createdAt") or "").strip(),
                "meta": pending_meta,
            },
        )
        message = saved_pending or {**message, "meta": pending_meta}
        existing_meta = message.get("meta") if isinstance(message.get("meta"), dict) else pending_meta
    job_key = _customer_voice_background_job_key("thread-voice", thread_id, message_id)
    if _start_customer_voice_background_job(job_key):
        thread_worker = threading.Thread(
            target=_complete_chat_thread_voice_reply_attachment,
            args=(openclaw_dir, thread_id, message_id, server),
            daemon=True,
            name=f"voice-reply-{thread_id[:10]}",
        )
        thread_worker.start()
    return message


def customer_access_callback_path(channel_id, provider="wechat"):
    normalized_channel_id = str(channel_id or "").strip()
    normalized_provider = str(provider or "wechat").strip().lower()
    if not normalized_channel_id:
        return ""
    if normalized_provider == "wechat":
        return f"/api/customer-access/wechat/{normalized_channel_id}/callback"
    return ""


def load_openclaw_weixin_runtime(openclaw_dir):
    root_dir = Path(str(openclaw_dir or "")).expanduser().resolve()
    state_dir = root_dir / "openclaw-weixin"
    extension_candidates = [
        root_dir / "extensions" / "openclaw-weixin",
        state_dir / "extensions" / "openclaw-weixin",
        Path.home() / ".openclaw" / "extensions" / "openclaw-weixin",
    ]
    extension_dir = next((candidate for candidate in extension_candidates if candidate.exists()), extension_candidates[0])
    accounts_dir = state_dir / "accounts"
    accounts_index_path = state_dir / "accounts.json"
    config = load_config(openclaw_dir)
    plugins = config.get("plugins") if isinstance(config.get("plugins"), dict) else {}
    entries = plugins.get("entries") if isinstance(plugins.get("entries"), dict) else {}
    plugin_entry = entries.get("openclaw-weixin") if isinstance(entries.get("openclaw-weixin"), dict) else {}
    account_ids = []
    if accounts_index_path.exists():
        try:
            parsed = json.loads(accounts_index_path.read_text(encoding="utf-8"))
            if isinstance(parsed, list):
                account_ids.extend(str(item or "").strip() for item in parsed if str(item or "").strip())
        except Exception:
            pass
    if accounts_dir.exists():
        for candidate in sorted(accounts_dir.glob("*.json")):
            account_id = str(candidate.stem or "").strip()
            if account_id and account_id not in account_ids:
                account_ids.append(account_id)
    accounts = []
    for account_id in account_ids:
        payload = {}
        account_path = accounts_dir / f"{account_id}.json"
        if account_path.exists():
            try:
                payload = json.loads(account_path.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
        user_id = str(payload.get("userId") or "").strip()
        base_url = str(payload.get("baseUrl") or "").strip()
        token = str(payload.get("token") or "").strip()
        accounts.append(
            {
                "accountId": account_id,
                "userId": user_id,
                "baseUrl": base_url,
                "configured": bool(token),
                "label": f"{user_id} · {account_id}" if user_id else account_id,
            }
        )
    return {
        "installed": extension_dir.exists(),
        "enabled": bool(plugin_entry.get("enabled")) if isinstance(plugin_entry, dict) else False,
        "accountCount": len(accounts),
        "accounts": accounts,
        "loginCommand": "openclaw channels login --channel openclaw-weixin",
        "containerLoginCommand": "docker exec -it mission-control openclaw channels login --channel openclaw-weixin",
    }


def customer_channel_is_wechat(channel_type):
    normalized = str(channel_type or "").strip().lower()
    return normalized.startswith("wechat") or normalized == "openclaw_weixin"


def auto_selected_skill_slugs_for_message(message_text):
    text = str(message_text or "").strip().lower()
    if not text:
        return []
    selected = []
    if (
        "mp.weixin.qq.com" in text
        or "search.weixin.qq.com" in text
        or "公众号" in text
        or "微信文章" in text
        or ("http" in text and any(token in text for token in ("article", "网页", "页面", "正文", "链接")))
    ):
        selected.append("web-content-fetcher")
    return clean_unique_strings(selected)


def enrich_customer_access_channel_payload(channel, openclaw_dir=None):
    normalized = deepcopy(channel) if isinstance(channel, dict) else {}
    channel_id = str(normalized.get("id") or "").strip()
    channel_type = str(normalized.get("type") or "").strip().lower()
    meta = normalized.get("meta") if isinstance(normalized.get("meta"), dict) else {}
    voice_config = customer_channel_voice_reply_config(normalized)
    normalized["voiceReplyEnabled"] = bool(voice_config.get("enabled"))
    normalized["voiceReplyMode"] = str(voice_config.get("mode") or "fixed").strip()
    normalized["voiceReplyVoice"] = str(
        voice_config.get("voice")
        or ("follow_agent_profile" if normalized["voiceReplyMode"] == "agent_profile" else WECHAT_VOICE_REPLY_DEFAULT_VOICE)
    ).strip()
    if channel_type == "wechat_official":
        callback_path = customer_access_callback_path(channel_id, provider="wechat")
        normalized["callbackPath"] = callback_path
        normalized["verificationConfigured"] = bool(str(meta.get("verificationToken") or "").strip())
        normalized["outboundConfigured"] = bool(str(meta.get("appId") or "").strip() and str(meta.get("appSecret") or "").strip())
        normalized["welcomeReply"] = str(meta.get("welcomeReply") or "").strip()
    elif channel_type == "openclaw_weixin":
        runtime = load_openclaw_weixin_runtime(openclaw_dir or os.environ.get("OPENCLAW_STATE_DIR") or "")
        selected_account_id = str(meta.get("accountId") or "").strip()
        selected_account = next(
            (item for item in safe_list(runtime.get("accounts")) if str(item.get("accountId") or "").strip() == selected_account_id),
            None,
        )
        if not selected_account and len(safe_list(runtime.get("accounts"))) == 1:
            selected_account = safe_list(runtime.get("accounts"))[0]
        normalized["pluginInstalled"] = bool(runtime.get("installed"))
        normalized["pluginEnabled"] = bool(runtime.get("enabled"))
        normalized["loginCommand"] = str(runtime.get("containerLoginCommand") or runtime.get("loginCommand") or "").strip()
        if selected_account:
            normalized["accountId"] = str(selected_account.get("accountId") or "").strip()
            normalized["accountUserId"] = str(selected_account.get("userId") or "").strip()
            normalized["accountConfigured"] = bool(selected_account.get("configured"))
            normalized["accountLabel"] = str(selected_account.get("label") or "").strip()
    return normalized


def build_customer_access_snapshot(openclaw_dir):
    channels = [enrich_customer_access_channel_payload(item, openclaw_dir=openclaw_dir) for item in store_list_customer_access_channels(openclaw_dir)]
    active = [item for item in channels if str(item.get("status") or "").strip().lower() == "active"]
    openclaw_weixin = load_openclaw_weixin_runtime(openclaw_dir)
    return {
        "channels": channels,
        "openclawWeixin": openclaw_weixin,
        "summary": {
            "total": len(channels),
            "active": len(active),
            "wechatCount": sum(1 for item in channels if customer_channel_is_wechat(item.get("type"))),
            "webCount": sum(1 for item in channels if str(item.get("type") or "") in {"website", "landing_page"}),
            "openclawWeixinCount": sum(1 for item in channels if str(item.get("type") or "").strip().lower() == "openclaw_weixin"),
        },
    }


def resolve_customer_access_channel(openclaw_dir, channel_id):
    normalized_channel_id = str(channel_id or "").strip()
    if not normalized_channel_id:
        return None
    channels = store_list_customer_access_channels(openclaw_dir)
    channel = next((item for item in channels if str(item.get("id") or "").strip() == normalized_channel_id), None)
    return enrich_customer_access_channel_payload(channel, openclaw_dir=openclaw_dir) if channel else None


def resolve_customer_channel_voice_test_target(openclaw_dir, channel, preferred_agent_id=""):
    channel = channel if isinstance(channel, dict) else {}
    config = load_config(openclaw_dir)
    load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    router_agent_id = get_router_agent_id(config)
    normalized_preferred_agent_id = str(preferred_agent_id or "").strip()
    available_agents = [
        item for item in safe_list(agents)
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    channel_type = str(channel.get("type") or "").strip().lower()
    meta = channel.get("meta") if isinstance(channel.get("meta"), dict) else {}
    if channel_type == "wechat_official":
        requested_team_id = str(wechat_official_channel_config(channel).get("defaultTeamId") or "").strip()
    else:
        requested_team_id = str(meta.get("defaultTeamId") or "").strip()
    linked_team_ids = normalize_chat_thread_linked_team_ids(requested_team_id)
    team_records = resolve_chat_thread_team_records(openclaw_dir, requested_team_id, linked_team_ids)
    team = team_records[0] if team_records else None
    participant_agent_ids = (
        resolve_chat_thread_participant_agent_ids([], team_records)
        or default_customer_access_participant_agent_ids(available_agents, router_agent_id=router_agent_id)
        or ([router_agent_id] if router_agent_id else [])
    )
    resolved_agent_id = normalized_preferred_agent_id
    if resolved_agent_id and not any(str(item.get("id") or "").strip() == resolved_agent_id for item in available_agents):
        resolved_agent_id = ""
    if not resolved_agent_id:
        resolved_agent_id = (
            str((team or {}).get("leadAgentId") or "").strip()
            or str(router_agent_id or "").strip()
            or str((participant_agent_ids[0] if participant_agent_ids else "") or "").strip()
        )
    resolved_agent = next(
        (item for item in available_agents if str(item.get("id") or "").strip() == resolved_agent_id),
        None,
    )
    return {
        "teamId": str((team or {}).get("id") or requested_team_id or "").strip(),
        "teamName": str((team or {}).get("name") or "").strip(),
        "agentId": resolved_agent_id,
        "agentName": str((resolved_agent or {}).get("title") or (resolved_agent or {}).get("name") or resolved_agent_id).strip(),
        "participantAgentIds": participant_agent_ids,
        "routerAgentId": str(router_agent_id or "").strip(),
    }


def perform_customer_channel_voice_test(openclaw_dir, channel, sample_text="", preferred_agent_id=""):
    normalized_channel = enrich_customer_access_channel_payload(channel, openclaw_dir=openclaw_dir)
    channel_type = str(normalized_channel.get("type") or "").strip().lower()
    if channel_type not in {"wechat_official", "openclaw_weixin"}:
        raise RuntimeError("当前只有微信公众号和 OpenClaw 微信渠道支持语音回复预演。")
    voice_config = customer_channel_voice_reply_config(normalized_channel)
    runtime_config = customer_voice_runtime_config(openclaw_dir)
    available_voices = _customer_voice_runtime_available_voices(openclaw_dir, runtime_config)
    builtin_voices = _customer_voice_runtime_builtin_voices(openclaw_dir, runtime_config)
    probe_error = ""
    runtime_ready = False
    try:
        probe_payload = _customer_voice_runtime_probe_payload(runtime_config, timeout=3)
        runtime_ready = bool(probe_payload)
    except Exception as exc:
        probe_error = str(exc or "").strip()
    target = resolve_customer_channel_voice_test_target(
        openclaw_dir,
        normalized_channel,
        preferred_agent_id=preferred_agent_id,
    )
    normalized_sample_text = compact_customer_voice_reply_text(
        sample_text or "你好，我先接住你的问题，马上继续帮你跟进。",
        limit=96,
    )
    if not normalized_sample_text:
        normalized_sample_text = "你好，我先接住你的问题，马上继续帮你跟进。"
    effective_voice = str(voice_config.get("voice") or "").strip()
    effective_speed = None
    effective_instructions = str(voice_config.get("instructions") or "").strip()
    agent_profile = {}
    if str(voice_config.get("mode") or "").strip() == "agent_profile":
        agent_profile = resolve_agent_voice_reply_profile(openclaw_dir, target.get("agentId"))
        effective_voice = str(agent_profile.get("voice") or "").strip()
        effective_speed = agent_profile.get("speed")
        if not effective_instructions:
            effective_instructions = str(agent_profile.get("instructions") or "").strip()
    if effective_speed is None:
        effective_speed = normalize_voice_reply_speed(DEFAULT_AGENT_VOICE_REPLY_SPEED)
    if not effective_instructions:
        effective_instructions = default_customer_voice_runtime_instructions(runtime_config.get("provider"))
    synthesis = {
        "ok": False,
        "error": "",
        "bytes": 0,
        "extension": "",
    }
    audio_path = None
    try:
        audio_path = generate_customer_voice_reply_audio(
            openclaw_dir,
            normalized_sample_text,
            voice=(voice_config.get("voice") or "") if str(voice_config.get("mode") or "").strip() == "fixed" else "",
            instructions=str(voice_config.get("instructions") or "").strip(),
            agent_id=target.get("agentId") if str(voice_config.get("mode") or "").strip() == "agent_profile" else "",
            speed=effective_speed if str(voice_config.get("mode") or "").strip() == "agent_profile" else None,
        )
        if audio_path:
            path_obj = Path(audio_path)
            synthesis = {
                "ok": bool(path_obj.exists() and path_obj.stat().st_size > 0),
                "error": "",
                "bytes": int(path_obj.stat().st_size if path_obj.exists() else 0),
                "extension": path_obj.suffix.lstrip("."),
            }
    except Exception as exc:
        synthesis = {
            "ok": False,
            "error": str(exc or "").strip() or "语音合成失败。",
            "bytes": 0,
            "extension": "",
        }
    finally:
        if audio_path:
            Path(audio_path).unlink(missing_ok=True)
    mode = str(voice_config.get("mode") or "fixed").strip()
    preview = {
        "channelName": str(normalized_channel.get("name") or "").strip(),
        "channelType": channel_type,
        "voiceReplyEnabled": bool(voice_config.get("enabled")),
        "voiceReplyMode": mode,
        "requestedVoice": str(
            normalized_channel.get("voiceReplyVoice")
            or ("follow_agent_profile" if mode == "agent_profile" else WECHAT_VOICE_REPLY_DEFAULT_VOICE)
        ).strip(),
        "effectiveVoice": effective_voice or WECHAT_VOICE_REPLY_DEFAULT_VOICE,
        "effectiveInstructions": effective_instructions,
        "effectiveSpeed": effective_speed,
        "sampleText": normalized_sample_text,
        "runtime": {
            "provider": str(runtime_config.get("provider") or "").strip(),
            "baseUrl": str(runtime_config.get("baseUrl") or "").strip(),
            "model": str(runtime_config.get("model") or "").strip(),
            "ready": bool(runtime_ready or available_voices),
            "probeError": probe_error,
            "speakerCount": len(clean_unique_strings(available_voices or builtin_voices)),
        },
        "resolution": {
            "teamId": str(target.get("teamId") or "").strip(),
            "teamName": str(target.get("teamName") or "").strip(),
            "agentId": str(target.get("agentId") or "").strip(),
            "agentName": str(target.get("agentName") or "").strip(),
        },
        "synthesis": synthesis,
    }
    if agent_profile:
        preview["agentProfile"] = {
            "voice": str(agent_profile.get("voice") or "").strip(),
            "speed": agent_profile.get("speed"),
            "instructions": str(agent_profile.get("instructions") or "").strip(),
        }
    message = (
        f"已预演 {preview['channelName'] or '客户渠道'} 的语音回复。"
        if synthesis.get("ok")
        else f"{preview['channelName'] or '客户渠道'} 的语音策略已解析，但语音合成暂未成功。"
    )
    return {
        "ok": True,
        "message": message,
        "preview": preview,
    }


def wechat_official_channel_config(channel):
    channel = channel if isinstance(channel, dict) else {}
    meta = channel.get("meta") if isinstance(channel.get("meta"), dict) else {}
    voice_config = customer_channel_voice_reply_config(channel)
    return {
        "verificationToken": str(meta.get("verificationToken") or "").strip(),
        "appId": str(meta.get("appId") or "").strip(),
        "appSecret": str(meta.get("appSecret") or "").strip(),
        "welcomeReply": str(meta.get("welcomeReply") or "").strip() or WECHAT_OFFICIAL_REPLY_FALLBACK,
        "defaultTeamId": str(meta.get("defaultTeamId") or "").strip(),
        "autoReplyEnabled": parse_boolish(meta.get("autoReplyEnabled"), True),
        "voiceReplyEnabled": bool(voice_config.get("enabled")),
        "voiceReplyMode": str(voice_config.get("mode") or "fixed").strip(),
        "voiceReplyVoice": str(
            voice_config.get("voice")
            or ("follow_agent_profile" if str(voice_config.get("mode") or "").strip() == "agent_profile" else WECHAT_VOICE_REPLY_DEFAULT_VOICE)
        ).strip(),
        "voiceReplyInstructions": str(voice_config.get("instructions") or "").strip(),
    }


def verify_wechat_signature(token, signature, timestamp, nonce):
    normalized_token = str(token or "").strip()
    normalized_signature = str(signature or "").strip()
    normalized_timestamp = str(timestamp or "").strip()
    normalized_nonce = str(nonce or "").strip()
    if not normalized_token or not normalized_signature or not normalized_timestamp or not normalized_nonce:
        return False
    digest = hashlib.sha1("".join(sorted([normalized_token, normalized_timestamp, normalized_nonce])).encode("utf-8")).hexdigest()
    return hmac.compare_digest(digest, normalized_signature)


def parse_wechat_xml_message(raw_text):
    try:
        root = ET.fromstring(str(raw_text or "").strip())
    except ET.ParseError:
        return {}
    payload = {}
    for child in list(root):
        payload[child.tag] = str(child.text or "").strip()
    return payload


def build_wechat_text_reply(to_user, from_user, content):
    reply_text = str(content or "").strip()
    if not reply_text:
        return b"success"
    body = (
        "<xml>"
        f"<ToUserName><![CDATA[{to_user}]]></ToUserName>"
        f"<FromUserName><![CDATA[{from_user}]]></FromUserName>"
        f"<CreateTime>{int(time.time())}</CreateTime>"
        "<MsgType><![CDATA[text]]></MsgType>"
        f"<Content><![CDATA[{reply_text}]]></Content>"
        "</xml>"
    )
    return body.encode("utf-8")


def summarize_wechat_inbound_message(wechat_payload):
    wechat_payload = wechat_payload if isinstance(wechat_payload, dict) else {}
    msg_type = str(wechat_payload.get("MsgType") or "").strip().lower()
    event_type = str(wechat_payload.get("Event") or "").strip().lower()
    if msg_type == "text":
        return str(wechat_payload.get("Content") or "").strip()
    if msg_type == "image":
        image_url = str(wechat_payload.get("PicUrl") or "").strip()
        return f"客户发送了一张图片。{image_url}".strip()
    if msg_type == "voice":
        recognition = str(wechat_payload.get("Recognition") or "").strip()
        return recognition or "客户发送了一段语音消息。"
    if msg_type == "event" and event_type == "subscribe":
        return "客户通过微信关注并发起了咨询。"
    if msg_type == "event" and event_type:
        return f"客户触发了微信事件：{event_type}"
    return f"客户发送了一条微信 {msg_type or 'unknown'} 消息。"


def find_customer_access_thread(openclaw_dir, channel_id, external_user_id):
    normalized_channel_id = str(channel_id or "").strip()
    normalized_external_user_id = str(external_user_id or "").strip()
    if not normalized_channel_id or not normalized_external_user_id:
        return None
    for thread in store_list_chat_threads(openclaw_dir, limit=512):
        meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
        if (
            str(meta.get("customerChannelId") or "").strip() == normalized_channel_id
            and str(meta.get("customerExternalUserId") or "").strip() == normalized_external_user_id
        ):
            return thread
    return None


def default_customer_access_participant_agent_ids(agents, router_agent_id=""):
    ordered = []
    normalized_router_agent_id = str(router_agent_id or "").strip()
    if normalized_router_agent_id:
        ordered.append(normalized_router_agent_id)
    for agent in safe_list(agents):
        agent_id = str((agent or {}).get("id") or "").strip()
        if agent_id and agent_id not in ordered:
            ordered.append(agent_id)
    return ordered


def ensure_customer_access_thread(openclaw_dir, channel, external_user_id, external_display_name=""):
    channel = channel if isinstance(channel, dict) else {}
    existing = find_customer_access_thread(openclaw_dir, channel.get("id"), external_user_id)
    if existing:
        return existing, False
    config = load_config(openclaw_dir)
    load_project_metadata(openclaw_dir, config=config)
    agents = load_agents(config)
    router_agent_id = get_router_agent_id(config)
    channel_cfg = wechat_official_channel_config(channel)
    requested_team_id = channel_cfg.get("defaultTeamId")
    linked_team_ids = normalize_chat_thread_linked_team_ids(requested_team_id)
    team_records = resolve_chat_thread_team_records(openclaw_dir, requested_team_id, linked_team_ids)
    team = team_records[0] if team_records else None
    participant_agent_ids = (
        resolve_chat_thread_participant_agent_ids([], team_records)
        or default_customer_access_participant_agent_ids(agents, router_agent_id=router_agent_id)
        or ([router_agent_id] if router_agent_id else [])
    )
    primary_agent_id = str((team or {}).get("leadAgentId") or router_agent_id).strip()
    if not primary_agent_id and participant_agent_ids:
        primary_agent_id = str(participant_agent_ids[0] or "").strip()
    current_target_agent_id = primary_agent_id
    team_policy = merge_team_policy_state(team, {}) if team else {}
    channel_type = str(channel.get("type") or "").strip().lower()
    dispatch_mode = "broadcast" if len(participant_agent_ids) > 1 else "direct"
    thread_title = f"{str(channel.get('name') or '微信咨询').strip()} · {str(external_display_name or external_user_id)[-8:]}"
    thread_meta = {
        "customerChannelId": str(channel.get("id") or "").strip(),
        "customerChannelType": str(channel.get("type") or "").strip(),
        "customerExternalUserId": str(external_user_id or "").strip(),
        "customerDisplayName": str(external_display_name or "").strip(),
        "customerSource": channel_type or "customer_channel",
        "customerConversation": True,
        "dispatchMode": dispatch_mode,
        "teamPolicy": team_policy if isinstance(team_policy, dict) else {},
        "customerTeamRouting": "team" if team else ("all_agents" if len(participant_agent_ids) > 1 else "single_agent"),
    }
    if team:
        thread_meta["teamId"] = str(team.get("id") or "").strip()
        thread_meta["linkedTeamIds"] = linked_team_ids
    thread = store_save_chat_thread(
        openclaw_dir,
        {
            "title": thread_title,
            "status": "open",
            "channel": "customer_wechat",
            "owner": str(external_display_name or "微信客户").strip() or "微信客户",
            "primaryAgentId": primary_agent_id,
            "currentTargetAgentId": current_target_agent_id,
            "participantAgentIds": participant_agent_ids,
            "participantHumans": [
                {
                    "name": str(external_display_name or "微信客户").strip() or "微信客户",
                    "username": str(external_user_id or "").strip(),
                    "role": "customer",
                }
            ],
            "meta": thread_meta,
        },
    )
    return thread, True


def wechat_access_token_for_channel(channel):
    channel = channel if isinstance(channel, dict) else {}
    config = wechat_official_channel_config(channel)
    app_id = str(config.get("appId") or "").strip()
    app_secret = str(config.get("appSecret") or "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("微信公众号尚未配置 AppID / AppSecret。")
    cache_key = f"{str(channel.get('id') or '').strip()}:{app_id}"
    cached = WECHAT_ACCESS_TOKEN_CACHE.get(cache_key)
    now_ts = time.time()
    if isinstance(cached, dict) and str(cached.get("token") or "").strip() and float(cached.get("expiresAt") or 0) > now_ts:
        return str(cached.get("token") or "").strip()
    request_url = (
        "https://api.weixin.qq.com/cgi-bin/token"
        f"?grant_type=client_credential&appid={quote(app_id)}&secret={quote(app_secret)}"
    )
    payload = json.loads(urlopen(Request(request_url, method="GET"), timeout=10).read().decode("utf-8", "replace") or "{}")
    token = str(payload.get("access_token") or "").strip()
    expires_in = int(payload.get("expires_in") or 7200)
    if not token:
        raise RuntimeError(str(payload.get("errmsg") or "无法获取微信公众号 access_token。").strip() or "无法获取微信公众号 access_token。")
    WECHAT_ACCESS_TOKEN_CACHE[cache_key] = {
        "token": token,
        "expiresAt": now_ts + max(60, expires_in - WECHAT_ACCESS_TOKEN_REFRESH_BUFFER_SECONDS),
    }
    return token


def build_customer_voice_reply_text(content):
    text = str(content or "").strip()
    if not text:
        return ""
    text = re.sub(r"```[\s\S]*?```", " ", text)
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", " ", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"(^|\n)\s*#{1,6}\s*", r"\1", text)
    text = re.sub(r"(^|\n)\s*[-*•]\s*", r"\1", text)
    text = re.sub(r"(^|\n)\s*\d+\.\s*", r"\1", text)
    text = re.sub(r"[>\t]", " ", text)
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"\n+", "。", text)
    text = re.sub(r"[ \u3000]+", " ", text)
    text = re.sub(r"。{2,}", "。", text)
    text = text.strip(" ，,；;：:、。")
    if len(text) > WECHAT_VOICE_REPLY_MAX_CHARS:
        text = text[:WECHAT_VOICE_REPLY_MAX_CHARS].rstrip(" ，,；;：:、。")
    if text and text[-1] not in "。！？!?":
        text += "。"
    return text


def customer_voice_reply_contains_cjk(text):
    return bool(re.search(r"[\u3400-\u9fff]", str(text or "")))


CUSTOMER_VOICE_MELO_SAFE_CHARS = 72
CUSTOMER_VOICE_MELO_RETRY_SAFE_CHARS = 48
CUSTOMER_VOICE_QWEN3_FAST_CHARS = 56
CUSTOMER_VOICE_COSYVOICE_FAST_CHARS = 88


def compact_customer_voice_reply_text(text, limit=WECHAT_VOICE_REPLY_MAX_CHARS):
    normalized_limit = max(12, int(limit or WECHAT_VOICE_REPLY_MAX_CHARS))
    normalized_text = build_customer_voice_reply_text(text)
    if len(normalized_text) <= normalized_limit:
        return normalized_text
    sentence_candidates = [
        segment.strip(" ，,；;：:、。！？!?")
        for segment in re.split(r"[。！？!?]+", normalized_text)
        if str(segment or "").strip(" ，,；;：:、。！？!?")
    ]
    for sentence in sentence_candidates:
        candidate = sentence
        if len(candidate) <= normalized_limit:
            return candidate + "。"
        clause_parts = [
            part.strip(" ，,；;：:、。！？!?")
            for part in re.split(r"[，,；;：:、]+", sentence)
            if str(part or "").strip(" ，,；;：:、。！？!?")
        ]
        compact = ""
        for clause in clause_parts:
            proposal = f"{compact}，{clause}" if compact else clause
            if len(proposal) + 1 > normalized_limit:
                break
            compact = proposal
        if compact:
            return compact.rstrip(" ，,；;：:、。！？!?") + "。"
    fallback = normalized_text[:normalized_limit].rstrip(" ，,；;：:、。！？!?")
    return f"{fallback}。" if fallback else normalized_text[:normalized_limit]


def customer_voice_reply_retry_candidates(text):
    normalized_text = build_customer_voice_reply_text(text)
    if not normalized_text:
        return []
    candidates = []

    def add(candidate):
        normalized_candidate = build_customer_voice_reply_text(candidate)
        if normalized_candidate and normalized_candidate not in candidates:
            candidates.append(normalized_candidate)

    add(normalized_text)
    add(compact_customer_voice_reply_text(normalized_text, limit=CUSTOMER_VOICE_MELO_SAFE_CHARS))
    add(compact_customer_voice_reply_text(normalized_text, limit=CUSTOMER_VOICE_MELO_RETRY_SAFE_CHARS))

    sentence_candidates = [
        segment.strip(" ，,；;：:、。！？!?")
        for segment in re.split(r"[。！？!?]+", normalized_text)
        if str(segment or "").strip(" ，,；;：:、。！？!?")
    ]
    if sentence_candidates:
        first_sentence = sentence_candidates[0]
        add(first_sentence)
        clause_candidates = [
            clause.strip(" ，,；;：:、。！？!?")
            for clause in re.split(r"[，,；;：:、]+", first_sentence)
            if str(clause or "").strip(" ，,；;：:、。！？!?")
        ]
        if clause_candidates:
            add(clause_candidates[0])
            if len(clause_candidates) > 1:
                add("，".join(clause_candidates[:2]))

    add(compact_customer_voice_reply_text(normalized_text, limit=24))
    add(compact_customer_voice_reply_text(normalized_text, limit=16))
    return candidates


def optimize_customer_voice_reply_text(text, provider=""):
    normalized_text = build_customer_voice_reply_text(text)
    if not normalized_text:
        return ""
    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3:
        return compact_customer_voice_reply_text(normalized_text, limit=CUSTOMER_VOICE_QWEN3_FAST_CHARS)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        return compact_customer_voice_reply_text(normalized_text, limit=CUSTOMER_VOICE_COSYVOICE_FAST_CHARS)
    return normalized_text


def _customer_voice_background_job_key(kind, *parts):
    normalized_parts = [str(part or "").strip() for part in parts if str(part or "").strip()]
    return "::".join([str(kind or "").strip() or "voice", *normalized_parts])


def _start_customer_voice_background_job(job_key):
    normalized_key = str(job_key or "").strip()
    if not normalized_key:
        return False
    with CUSTOMER_VOICE_BACKGROUND_JOBS_LOCK:
        if normalized_key in CUSTOMER_VOICE_BACKGROUND_JOBS:
            return False
        CUSTOMER_VOICE_BACKGROUND_JOBS.add(normalized_key)
    return True


def _finish_customer_voice_background_job(job_key):
    normalized_key = str(job_key or "").strip()
    if not normalized_key:
        return
    with CUSTOMER_VOICE_BACKGROUND_JOBS_LOCK:
        CUSTOMER_VOICE_BACKGROUND_JOBS.discard(normalized_key)


def _latest_chat_thread_message(openclaw_dir, thread_id, message_id):
    normalized_thread_id = str(thread_id or "").strip()
    normalized_message_id = str(message_id or "").strip()
    if not normalized_thread_id or not normalized_message_id:
        return {}
    messages = safe_list(store_list_chat_messages(openclaw_dir, thread_id=normalized_thread_id, limit=240))
    for item in reversed(messages):
        if not isinstance(item, dict):
            continue
        if str(item.get("id") or "").strip() == normalized_message_id:
            return item
    return {}


def _save_chat_thread_voice_reply_message(openclaw_dir, message, meta):
    message = message if isinstance(message, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    return store_save_chat_message(
        openclaw_dir,
        {
            "id": str(message.get("id") or "").strip(),
            "threadId": str(message.get("threadId") or "").strip(),
            "senderKind": str(message.get("senderKind") or "agent").strip() or "agent",
            "senderId": str(message.get("senderId") or "").strip(),
            "senderLabel": str(message.get("senderLabel") or "").strip(),
            "direction": str(message.get("direction") or "agent").strip() or "agent",
            "body": str(message.get("body") or "").strip(),
            "createdAt": str(message.get("createdAt") or "").strip(),
            "meta": meta,
        },
    ) or {**message, "meta": meta}


def _complete_chat_thread_voice_reply_attachment(openclaw_dir, thread_id, message_id, server=None):
    job_key = _customer_voice_background_job_key("thread-voice", thread_id, message_id)
    try:
        latest_message = _latest_chat_thread_message(openclaw_dir, thread_id, message_id)
        if not latest_message:
            return
        existing_meta = latest_message.get("meta") if isinstance(latest_message.get("meta"), dict) else {}
        attachments = [dict(item) for item in safe_list(existing_meta.get("attachments")) if isinstance(item, dict)]
        for item in attachments:
            if str(item.get("source") or "").strip() == "member_voice_reply":
                if existing_meta.get("voiceReplyPending") or existing_meta.get("voiceReplyFailed"):
                    saved_message = _save_chat_thread_voice_reply_message(
                        openclaw_dir,
                        latest_message,
                        {
                            **existing_meta,
                            "voiceReplyRequested": True,
                            "voiceReplyPending": False,
                            "voiceReplyFailed": False,
                        },
                    )
                    _publish_chat_thread_voice_reply_event(server, thread_id, saved_message or latest_message, stage="attachment_ready")
                return
        reply_text = str(latest_message.get("body") or "").strip()
        if not reply_text:
            return
        try:
            audio_path = Path(
                generate_customer_voice_reply_audio(
                    openclaw_dir,
                    reply_text,
                    agent_id=str(latest_message.get("senderId") or "").strip(),
                )
            )
        except Exception:
            LOGGER.exception("chat thread voice reply generation failed for %s/%s", thread_id, message_id)
            saved_message = _save_chat_thread_voice_reply_message(
                openclaw_dir,
                latest_message,
                {
                    **existing_meta,
                    "voiceReplyRequested": True,
                    "voiceReplyPending": False,
                    "voiceReplyFailed": True,
                },
            )
            _publish_chat_thread_voice_reply_event(server, thread_id, saved_message or latest_message, stage="attachment_failed")
            return
        if not audio_path.exists():
            saved_message = _save_chat_thread_voice_reply_message(
                openclaw_dir,
                latest_message,
                {
                    **existing_meta,
                    "voiceReplyRequested": True,
                    "voiceReplyPending": False,
                    "voiceReplyFailed": True,
                },
            )
            _publish_chat_thread_voice_reply_event(server, thread_id, saved_message or latest_message, stage="attachment_failed")
            return
        mime_type = mimetypes.guess_type(str(audio_path))[0] or "audio/mpeg"
        attachment = {
            "id": f"catt_{secrets.token_hex(8)}",
            "kind": "audio",
            "name": f"{str(latest_message.get('senderId') or 'member').strip() or 'member'}-voice-reply{audio_path.suffix or '.mp3'}",
            "size": audio_path.stat().st_size,
            "type": mime_type,
            "mimeType": mime_type,
            "createdAt": now_iso(),
            "source": "member_voice_reply",
            "sourceLabel": "语音回复",
            "contentPath": str(audio_path),
        }
        saved_message = _save_chat_thread_voice_reply_message(
            openclaw_dir,
            latest_message,
            {
                **existing_meta,
                "voiceReplyRequested": True,
                "voiceReplyPending": False,
                "voiceReplyFailed": False,
                "attachments": attachments + [attachment],
            },
        )
        _publish_chat_thread_voice_reply_event(server, thread_id, saved_message or latest_message, stage="attachment_ready")
    finally:
        _finish_customer_voice_background_job(job_key)


def _publish_chat_thread_voice_reply_event(server, thread_id, message, stage="attachment_ready"):
    normalized_thread_id = str(thread_id or "").strip()
    if not normalized_thread_id or not server:
        return
    message = message if isinstance(message, dict) else {}
    publish_chat_thread_stream_event(
        server,
        normalized_thread_id,
        stage,
        messageId=str(message.get("id") or "").strip(),
        agentId=str(message.get("senderId") or "").strip(),
        sessionId="main",
        content="",
        delta="",
        voiceReplyPending=bool((message.get("meta") or {}).get("voiceReplyPending")),
        voiceReplyFailed=bool((message.get("meta") or {}).get("voiceReplyFailed")),
    )


def customer_voice_reply_temp_dir(openclaw_dir=""):
    root = Path(str(openclaw_dir or "/tmp")).expanduser()
    temp_dir = root / "runtime" / "customer-channel-voice"
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


def customer_voice_reply_timeout_seconds():
    try:
        resolved = float(WECHAT_VOICE_REPLY_TIMEOUT_SECONDS)
    except (TypeError, ValueError):
        resolved = 45.0
    return max(1.0, resolved)


def customer_voice_runtime_timeout_seconds(provider=""):
    resolved = customer_voice_reply_timeout_seconds()
    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3:
        return max(resolved, 180.0)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX:
        return max(resolved, 60.0)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        return max(resolved, 120.0)
    return resolved


CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI = "openai"
CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX = "sherpa_onnx"
CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3 = "qwen3_tts"
CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE = "cosyvoice_sft"
CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU = "zhipu"
CUSTOMER_VOICE_RUNTIME_CUSTOM_PREFIX = "custom:"
CUSTOMER_VOICE_RUNTIME_CUSTOM_SAMPLE_DIRNAME = "customer-voice-speakers"
CUSTOMER_VOICE_RUNTIME_CUSTOM_PROMPT_FILENAME = "prompt.txt"
DEFAULT_CUSTOMER_VOICE_OPENAI_MODEL = "gpt-4o-mini-tts-2025-12-15"
DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_MODEL = "kokoro-multi-lang-v1_1"
DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_BASE_URL = "http://127.0.0.1:8090/v1"
DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_DOCKER_BASE_URL = "http://sherpa-onnx-tts:8080/v1"
DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_API_KEY_ENV = "SHERPA_ONNX_TTS_API_KEY"
DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_API_KEY = "sherpa-onnx-local"
DEFAULT_CUSTOMER_VOICE_QWEN3_MODEL = "qwen3-tts-12hz-0.6b-customvoice"
DEFAULT_CUSTOMER_VOICE_QWEN3_BASE_URL = "http://127.0.0.1:8090/v1"
DEFAULT_CUSTOMER_VOICE_QWEN3_DOCKER_BASE_URL = "http://qwen3-tts:8080/v1"
DEFAULT_CUSTOMER_VOICE_QWEN3_API_KEY_ENV = "QWEN3_TTS_API_KEY"
DEFAULT_CUSTOMER_VOICE_QWEN3_API_KEY = "qwen3-local"
DEFAULT_CUSTOMER_VOICE_COSYVOICE_MODEL = "cosyvoice-300m-instruct"
DEFAULT_CUSTOMER_VOICE_COSYVOICE_BASE_URL = "http://127.0.0.1:8090/v1"
DEFAULT_CUSTOMER_VOICE_COSYVOICE_DOCKER_BASE_URL = "http://cosyvoice-tts:8080/v1"
DEFAULT_CUSTOMER_VOICE_COSYVOICE_API_KEY_ENV = "COSYVOICE_TTS_API_KEY"
DEFAULT_CUSTOMER_VOICE_COSYVOICE_API_KEY = "cosyvoice-local"
DEFAULT_CUSTOMER_VOICE_ZHIPU_MODEL = "glm-tts"
DEFAULT_CUSTOMER_VOICE_ZHIPU_BASE_URL = "https://open.bigmodel.cn/api/paas/v4"
DEFAULT_CUSTOMER_VOICE_API_KEY_ENV = "OPENAI_API_KEY"
DEFAULT_CUSTOMER_VOICE_ZHIPU_API_KEY_ENV = "ZAI_API_KEY"
CUSTOMER_VOICE_ZHIPU_VOICE_ALIASES = {
    "alloy": "tongtong",
    "ash": "xiaochen",
    "ballad": "chuichui",
    "cedar": "tongtong",
    "coral": "tongtong",
    "echo": "xiaochen",
    "fable": "chuichui",
    "marin": "tongtong",
    "nova": "tongtong",
    "onyx": "xiaochen",
    "sage": "xiaochen",
    "shimmer": "xiaochen",
    "verse": "chuichui",
}
CUSTOMER_VOICE_COSYVOICE_VOICE_ALIASES = {
    "alloy": "中文女",
    "ash": "中文男",
    "ballad": "中文女",
    "cedar": "中文女",
    "coral": "中文女",
    "echo": "中文男",
    "fable": "中文男",
    "marin": "中文女",
    "nova": "中文女",
    "onyx": "中文男",
    "sage": "中文男",
    "shimmer": "中文女",
    "verse": "中文男",
    "zh": "中文女",
    "zh-cn": "中文女",
}
CUSTOMER_VOICE_QWEN3_VOICE_ALIASES = {
    "alloy": "serena",
    "ash": "uncle_fu",
    "ballad": "serena",
    "cedar": "serena",
    "coral": "serena",
    "echo": "uncle_fu",
    "fable": "vivian",
    "marin": "serena",
    "nova": "serena",
    "onyx": "uncle_fu",
    "sage": "uncle_fu",
    "shimmer": "serena",
    "verse": "vivian",
    "zh": "serena",
    "zh-cn": "serena",
    "中文女": "serena",
    "中文男": "uncle_fu",
    "粤语女": "vivian",
}
CUSTOMER_VOICE_QWEN3_PREFERRED_ORDER = [
    "serena",
    "vivian",
    "uncle_fu",
    "dylan",
    "eric",
    "sohee",
    "ono_anna",
    "aiden",
    "ryan",
]
CUSTOMER_VOICE_SHERPA_ONNX_VOICE_ALIASES = {
    "alloy": "zf_001",
    "ash": "zm_009",
    "ballad": "zf_002",
    "cedar": "zf_003",
    "coral": "zf_004",
    "echo": "zm_010",
    "fable": "zf_005",
    "marin": "zf_006",
    "nova": "zf_017",
    "onyx": "zm_011",
    "sage": "zm_012",
    "shimmer": "zf_018",
    "verse": "zm_013",
    "zh": "zf_001",
    "zh-cn": "zf_001",
    "中文女": "zf_001",
    "中文男": "zm_009",
    "粤语女": "zf_018",
}
CUSTOMER_VOICE_SHERPA_ONNX_PREFERRED_ORDER = [
    "zf_001",
    "zf_002",
    "zf_003",
    "zf_004",
    "zf_005",
    "zf_006",
    "zf_017",
    "zf_018",
    "zm_009",
    "zm_010",
    "zm_011",
    "zm_012",
    "zm_013",
]


def customer_voice_custom_speakers_dir(openclaw_dir):
    root = Path(str(openclaw_dir or "/tmp")).expanduser()
    path = root / "runtime" / CUSTOMER_VOICE_RUNTIME_CUSTOM_SAMPLE_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def customer_voice_custom_voice_id(agent_id):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return ""
    return f"{CUSTOMER_VOICE_RUNTIME_CUSTOM_PREFIX}{normalized_agent_id}"


def customer_voice_custom_voice_agent_id(voice):
    normalized = str(voice or "").strip()
    if not normalized.startswith(CUSTOMER_VOICE_RUNTIME_CUSTOM_PREFIX):
        return ""
    return normalized[len(CUSTOMER_VOICE_RUNTIME_CUSTOM_PREFIX) :].strip()


def customer_voice_custom_sample_dir(openclaw_dir, agent_id):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        raise RuntimeError("agentId 不能为空。")
    path = customer_voice_custom_speakers_dir(openclaw_dir) / normalized_agent_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def customer_voice_custom_sample_payload(openclaw_dir, agent_id):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return {}
    sample_dir = customer_voice_custom_speakers_dir(openclaw_dir) / normalized_agent_id
    if not sample_dir.exists():
        return {}
    prompt_path = sample_dir / CUSTOMER_VOICE_RUNTIME_CUSTOM_PROMPT_FILENAME
    prompt_text = ""
    if prompt_path.exists():
        try:
            prompt_text = prompt_path.read_text(encoding="utf-8").strip()
        except Exception:
            prompt_text = ""
    sample_files = [
        item
        for item in sorted(sample_dir.iterdir())
        if item.is_file() and item.name != CUSTOMER_VOICE_RUNTIME_CUSTOM_PROMPT_FILENAME
    ]
    sample_path = sample_files[0] if sample_files else None
    if not sample_path or not prompt_text:
        return {}
    mime_type = mimetypes.guess_type(str(sample_path.name))[0] or "application/octet-stream"
    return {
        "voiceId": customer_voice_custom_voice_id(normalized_agent_id),
        "agentId": normalized_agent_id,
        "samplePath": str(sample_path),
        "sampleName": sample_path.name,
        "promptText": prompt_text,
        "mimeType": mime_type,
        "updatedAt": datetime.fromtimestamp(sample_path.stat().st_mtime, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def customer_voice_bootstrap_sample_payload(openclaw_dir, agent_id, sample_payload=None, profile=None):
    normalized_agent_id = str(agent_id or "").strip()
    payload = sample_payload if isinstance(sample_payload, dict) else customer_voice_custom_sample_payload(openclaw_dir, normalized_agent_id)
    if not normalized_agent_id or not payload:
        return False
    prompt_text = str(payload.get("promptText") or "").strip()
    if not prompt_text:
        return False
    runtime_profile = profile if isinstance(profile, dict) else agent_runtime_profile_payload(openclaw_dir, normalized_agent_id)
    reference_candidates = {
        build_agent_voice_reply_reference_text(normalized_agent_id, profile=runtime_profile).strip(),
        build_agent_voice_reply_reference_text(normalized_agent_id, profile={}).strip(),
        build_agent_voice_reply_reference_text(
            normalized_agent_id,
            profile={"humanName": normalized_agent_id, "jobTitle": "团队协作成员"},
        ).strip(),
    }
    reference_candidates = {item for item in reference_candidates if item}
    if not reference_candidates:
        return False
    return prompt_text in reference_candidates


def customer_voice_effective_custom_sample_payload(openclaw_dir, agent_id, sample_payload=None, profile=None):
    payload = sample_payload if isinstance(sample_payload, dict) else customer_voice_custom_sample_payload(openclaw_dir, agent_id)
    if customer_voice_bootstrap_sample_payload(openclaw_dir, agent_id, sample_payload=payload, profile=profile):
        return {}
    return payload if isinstance(payload, dict) else {}


def customer_voice_custom_voices(openclaw_dir, config=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    ordered_agent_ids = [
        str(item.get("id") or "").strip()
        for item in safe_list(load_agents(local_config))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    voices = []
    for agent_id in ordered_agent_ids:
        payload = customer_voice_effective_custom_sample_payload(openclaw_dir, agent_id)
        voice_id = str(payload.get("voiceId") or "").strip()
        if voice_id:
            voices.append(voice_id)
    return clean_unique_strings(voices)


def save_agent_voice_reply_sample(openclaw_dir, agent_id, filename="", content_b64="", prompt_text=""):
    normalized_agent_id = str(agent_id or "").strip()
    normalized_prompt_text = str(prompt_text or "").strip()
    if not normalized_agent_id:
        raise RuntimeError("agentId 不能为空。")
    if not normalized_prompt_text:
        raise RuntimeError("请填写声音样本对应的参考文本。")
    normalized_content = str(content_b64 or "").strip()
    if normalized_content.startswith("data:") and "," in normalized_content:
        normalized_content = normalized_content.split(",", 1)[1].strip()
    if not normalized_content:
        raise RuntimeError("请上传声音样本文件。")
    try:
        data = base64.b64decode(normalized_content, validate=True)
    except Exception as error:
        raise RuntimeError("声音样本文件内容无效。") from error
    if not data:
        raise RuntimeError("声音样本文件内容为空。")
    sample_dir = customer_voice_custom_sample_dir(openclaw_dir, normalized_agent_id)
    for existing in sample_dir.iterdir():
        if existing.is_file():
            existing.unlink(missing_ok=True)
    suffix = Path(str(filename or "sample.wav")).suffix.lower()
    if suffix not in {".wav", ".mp3", ".m4a", ".flac", ".ogg"}:
        suffix = ".wav"
    sample_path = sample_dir / f"sample{suffix}"
    sample_path.write_bytes(data)
    (sample_dir / CUSTOMER_VOICE_RUNTIME_CUSTOM_PROMPT_FILENAME).write_text(normalized_prompt_text, encoding="utf-8")
    return customer_voice_custom_sample_payload(openclaw_dir, normalized_agent_id)


def update_agent_voice_reply_sample_prompt_text(openclaw_dir, agent_id, prompt_text=""):
    normalized_agent_id = str(agent_id or "").strip()
    normalized_prompt_text = str(prompt_text or "").strip()
    if not normalized_agent_id:
        raise RuntimeError("agentId 不能为空。")
    if not normalized_prompt_text:
        raise RuntimeError("请填写声音样本对应的参考文本。")
    existing = customer_voice_custom_sample_payload(openclaw_dir, normalized_agent_id)
    if not existing:
        raise RuntimeError("当前成员还没有已保存的声音样本。")
    sample_dir = customer_voice_custom_speakers_dir(openclaw_dir) / normalized_agent_id
    (sample_dir / CUSTOMER_VOICE_RUNTIME_CUSTOM_PROMPT_FILENAME).write_text(normalized_prompt_text, encoding="utf-8")
    return customer_voice_custom_sample_payload(openclaw_dir, normalized_agent_id)


def remove_agent_voice_reply_sample(openclaw_dir, agent_id):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return False
    sample_dir = customer_voice_custom_speakers_dir(openclaw_dir) / normalized_agent_id
    if not sample_dir.exists():
        return False
    shutil.rmtree(sample_dir, ignore_errors=True)
    return True


def build_agent_voice_reply_reference_text(agent_id="", profile=None):
    profile = profile if isinstance(profile, dict) else {}
    normalized_agent_id = str(agent_id or "").strip() or "member"
    human_name = str(profile.get("humanName") or normalized_agent_id).strip() or normalized_agent_id
    job_title = str(profile.get("jobTitle") or profile.get("roleLabel") or "团队协作成员").strip() or "团队协作成员"
    lead_sentence = f"我是{human_name}，负责{job_title}。"
    followup_sentence = "有事直接找我。"
    return f"{lead_sentence}{followup_sentence}"


def persist_agent_voice_reply_sample_metadata(openclaw_dir, agent_id, sample_payload, instructions="", speed=None):
    normalized_agent_id = str(agent_id or "").strip()
    sample_payload = sample_payload if isinstance(sample_payload, dict) else {}
    if not normalized_agent_id or not sample_payload:
        return {}
    metadata = load_project_metadata(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else {}
    overrides = metadata.get("agentOverrides") if isinstance(metadata.get("agentOverrides"), dict) else {}
    entry = deepcopy(overrides.get(normalized_agent_id)) if isinstance(overrides.get(normalized_agent_id), dict) else {}
    entry["voiceReplyVoice"] = str(sample_payload.get("voiceId") or customer_voice_custom_voice_id(normalized_agent_id)).strip()
    entry["voiceReplySamplePath"] = str(sample_payload.get("samplePath") or "").strip()
    entry["voiceReplySampleName"] = str(sample_payload.get("sampleName") or "").strip()
    entry["voiceReplySamplePromptText"] = str(sample_payload.get("promptText") or "").strip()
    if str(instructions or "").strip():
        entry["voiceReplyInstructions"] = str(instructions or "").strip()
    if speed is not None:
        entry["voiceReplySpeed"] = normalize_voice_reply_speed(speed, default=DEFAULT_AGENT_VOICE_REPLY_SPEED)
    overrides[normalized_agent_id] = entry
    metadata["agentOverrides"] = overrides
    save_project_metadata(openclaw_dir, metadata)
    return entry


def ensure_agent_voice_reply_sample(openclaw_dir, agent_id, runtime_config=None, voice="", instructions="", speed=None, profile=None):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return {}
    existing_payload = customer_voice_custom_sample_payload(openclaw_dir, normalized_agent_id)
    if existing_payload:
        return existing_payload
    local_runtime_config = runtime_config if isinstance(runtime_config, dict) else customer_voice_runtime_config(openclaw_dir)
    provider = str(local_runtime_config.get("provider") or "").strip().lower()
    if provider != CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3:
        return {}
    runtime_profile = profile if isinstance(profile, dict) else agent_runtime_profile_payload(openclaw_dir, normalized_agent_id)
    builtin_voices = _customer_voice_runtime_builtin_voices(openclaw_dir, local_runtime_config)
    available_voices = builtin_voices or [
        item
        for item in _customer_voice_runtime_available_voices(openclaw_dir, local_runtime_config)
        if not customer_voice_custom_voice_agent_id(item)
    ]
    if not available_voices:
        return {}
    bootstrap_voice = str(voice or runtime_profile.get("voiceReplyVoice") or "").strip()
    if customer_voice_custom_voice_agent_id(bootstrap_voice):
        bootstrap_voice = ""
    bootstrap_voice = _customer_voice_provider_voice_name(
        bootstrap_voice,
        provider=provider,
        available_voices=available_voices,
        agent_id=normalized_agent_id,
        openclaw_dir="",
    )
    if not bootstrap_voice or customer_voice_custom_voice_agent_id(bootstrap_voice):
        return {}
    bootstrap_instructions = str(instructions or runtime_profile.get("voiceReplyInstructions") or "").strip()
    bootstrap_speed = speed if speed is not None else runtime_profile.get("voiceReplySpeed")
    reference_text = build_agent_voice_reply_reference_text(normalized_agent_id, profile=runtime_profile)
    reference_audio_path = None
    try:
        reference_audio_path = Path(
            _generate_customer_voice_reply_audio_via_openai_compatible(
                openclaw_dir,
                reference_text,
                local_runtime_config,
                voice=bootstrap_voice,
                instructions=bootstrap_instructions,
                speed=bootstrap_speed,
            )
        )
        sample_payload = save_agent_voice_reply_sample(
            openclaw_dir,
            normalized_agent_id,
            filename=reference_audio_path.name,
            content_b64=base64.b64encode(reference_audio_path.read_bytes()).decode("utf-8"),
            prompt_text=reference_text,
        )
        persist_agent_voice_reply_sample_metadata(
            openclaw_dir,
            normalized_agent_id,
            sample_payload,
            instructions=bootstrap_instructions,
            speed=bootstrap_speed,
        )
        return sample_payload
    finally:
        if reference_audio_path is not None:
            reference_audio_path.unlink(missing_ok=True)


def default_customer_voice_runtime_instructions(provider=""):
    normalized_provider = str(provider or "").strip().lower()
    if normalized_provider not in {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE,
    }:
        return ""
    return (
        "请像真人在微信里发语音一样自然说话。 "
        "语气温和、口语化、有轻微情绪起伏，不要像播报器或机器朗读。 "
        "停顿自然，表达简洁，像同事当面语音回复一样。"
    )


def _customer_voice_runtime_env_text(*keys):
    for key in keys:
        value = str(os.environ.get(key) or "").strip()
        if value:
            return value
    return ""


def _customer_voice_runtime_env_provider():
    provider = _customer_voice_runtime_env_text("MISSION_CONTROL_SPEECH_RUNTIME_PROVIDER").lower()
    if provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        provider = CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX
    return provider


def _customer_voice_runtime_base_url_override(provider=""):
    provider_key = {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI: "MISSION_CONTROL_OPENAI_TTS_RUNTIME_BASE_URL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX: "MISSION_CONTROL_SHERPA_ONNX_RUNTIME_BASE_URL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3: "MISSION_CONTROL_QWEN3_TTS_RUNTIME_BASE_URL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE: "MISSION_CONTROL_COSYVOICE_TTS_RUNTIME_BASE_URL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU: "MISSION_CONTROL_ZHIPU_TTS_RUNTIME_BASE_URL",
    }.get(str(provider or "").strip().lower(), "")
    return _customer_voice_runtime_env_text(provider_key, "MISSION_CONTROL_SPEECH_RUNTIME_BASE_URL")


def _customer_voice_runtime_model_override(provider=""):
    provider_key = {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI: "MISSION_CONTROL_OPENAI_TTS_RUNTIME_MODEL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX: "MISSION_CONTROL_SHERPA_ONNX_RUNTIME_MODEL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3: "MISSION_CONTROL_QWEN3_TTS_RUNTIME_MODEL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE: "MISSION_CONTROL_COSYVOICE_TTS_RUNTIME_MODEL",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU: "MISSION_CONTROL_ZHIPU_TTS_RUNTIME_MODEL",
    }.get(str(provider or "").strip().lower(), "")
    return _customer_voice_runtime_env_text(provider_key, "MISSION_CONTROL_SPEECH_RUNTIME_MODEL")


def _customer_voice_runtime_api_key_env_override(provider=""):
    provider_key = {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI: "MISSION_CONTROL_OPENAI_TTS_RUNTIME_API_KEY_ENV",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX: "MISSION_CONTROL_SHERPA_ONNX_RUNTIME_API_KEY_ENV",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3: "MISSION_CONTROL_QWEN3_TTS_RUNTIME_API_KEY_ENV",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE: "MISSION_CONTROL_COSYVOICE_TTS_RUNTIME_API_KEY_ENV",
        CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU: "MISSION_CONTROL_ZHIPU_TTS_RUNTIME_API_KEY_ENV",
    }.get(str(provider or "").strip().lower(), "")
    return _customer_voice_runtime_env_text(provider_key, "MISSION_CONTROL_SPEECH_RUNTIME_API_KEY_ENV")


def customer_voice_runtime_config(openclaw_dir, config=None):
    metadata = load_project_metadata(openclaw_dir, config=config)
    runtime = metadata.get("speechRuntime") if isinstance(metadata.get("speechRuntime"), dict) else {}
    default_provider = CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX
    provider = str(runtime.get("provider") or default_provider).strip().lower()
    provider = _customer_voice_runtime_env_provider() or provider
    if provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        provider = default_provider
    if provider not in {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU,
    }:
        provider = default_provider
    default_base_url = ""
    default_model = DEFAULT_CUSTOMER_VOICE_OPENAI_MODEL
    default_api_key_env = DEFAULT_CUSTOMER_VOICE_API_KEY_ENV
    if provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX:
        default_base_url = (
            DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_DOCKER_BASE_URL
            if Path("/.dockerenv").exists()
            else DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_BASE_URL
        )
        default_model = DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_MODEL
        default_api_key_env = DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_API_KEY_ENV
    elif provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3:
        default_base_url = (
            DEFAULT_CUSTOMER_VOICE_QWEN3_DOCKER_BASE_URL
            if Path("/.dockerenv").exists()
            else DEFAULT_CUSTOMER_VOICE_QWEN3_BASE_URL
        )
        default_model = DEFAULT_CUSTOMER_VOICE_QWEN3_MODEL
        default_api_key_env = DEFAULT_CUSTOMER_VOICE_QWEN3_API_KEY_ENV
    elif provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        default_base_url = (
            DEFAULT_CUSTOMER_VOICE_COSYVOICE_DOCKER_BASE_URL
            if Path("/.dockerenv").exists()
            else DEFAULT_CUSTOMER_VOICE_COSYVOICE_BASE_URL
        )
        default_model = DEFAULT_CUSTOMER_VOICE_COSYVOICE_MODEL
        default_api_key_env = DEFAULT_CUSTOMER_VOICE_COSYVOICE_API_KEY_ENV
    elif provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU:
        default_base_url = DEFAULT_CUSTOMER_VOICE_ZHIPU_BASE_URL
        default_model = DEFAULT_CUSTOMER_VOICE_ZHIPU_MODEL
        default_api_key_env = DEFAULT_CUSTOMER_VOICE_ZHIPU_API_KEY_ENV
    resolved_base_url = _customer_voice_runtime_base_url_override(provider) or str(runtime.get("baseUrl") or default_base_url).strip()
    resolved_model = _customer_voice_runtime_model_override(provider) or str(runtime.get("model") or default_model).strip()
    resolved_api_key_env = _customer_voice_runtime_api_key_env_override(provider) or str(runtime.get("apiKeyEnv") or default_api_key_env).strip()
    return {
        "provider": provider,
        "baseUrl": resolved_base_url,
        "model": resolved_model,
        "apiKeyEnv": resolved_api_key_env,
    }


def _customer_voice_audio_endpoint(base_url, provider=CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI):
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return ""
    if normalized.endswith("/audio/speech"):
        return normalized
    if str(provider or "").strip().lower() == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU:
        if normalized.endswith("/api/paas/v4") or normalized.endswith("/v4"):
            return f"{normalized}/audio/speech"
        if normalized.endswith("/api/paas"):
            return f"{normalized}/v4/audio/speech"
        return f"{normalized}/api/paas/v4/audio/speech"
    if normalized.endswith("/v1"):
        return f"{normalized}/audio/speech"
    return f"{normalized}/v1/audio/speech"


def _customer_voice_runtime_probe_url(base_url):
    normalized = str(base_url or "").strip().rstrip("/")
    if not normalized:
        return ""
    if normalized.endswith("/v1"):
        return f"{normalized}/models"
    if normalized.endswith("/audio/speech"):
        return normalized.rsplit("/", 2)[0] + "/models"
    return f"{normalized}/models"


def _customer_voice_runtime_probe_cache_key(runtime_config):
    runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
    return (
        str(runtime_config.get("provider") or "").strip().lower(),
        str(runtime_config.get("baseUrl") or "").strip().rstrip("/"),
        str(runtime_config.get("model") or "").strip(),
    )


def _customer_voice_runtime_probe_payload(runtime_config, timeout=3):
    runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
    provider = str(runtime_config.get("provider") or "").strip().lower()
    if provider not in {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX,
    }:
        return {}
    probe_url = _customer_voice_runtime_probe_url(runtime_config.get("baseUrl"))
    if not probe_url:
        return {}
    cache_key = _customer_voice_runtime_probe_cache_key(runtime_config)
    now = time.time()
    cached = CUSTOMER_VOICE_RUNTIME_PROBE_CACHE.get(cache_key)
    if isinstance(cached, dict) and (now - float(cached.get("at") or 0.0)) < CUSTOMER_VOICE_RUNTIME_PROBE_CACHE_TTL_SECONDS:
        return deepcopy(cached.get("payload") or {})
    request = Request(probe_url, headers={"Accept": "application/json"}, method="GET")
    with urlopen(request, timeout=max(1, int(timeout or 3))) as response:
        payload = json.loads(response.read().decode("utf-8", "replace") or "{}")
    CUSTOMER_VOICE_RUNTIME_PROBE_CACHE[cache_key] = {"at": now, "payload": payload}
    return deepcopy(payload)


def _customer_voice_runtime_builtin_voices(openclaw_dir, runtime_config):
    runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
    provider = str(runtime_config.get("provider") or "").strip().lower()
    if provider not in {
        CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3,
        CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX,
    }:
        return []
    try:
        payload = _customer_voice_runtime_probe_payload(runtime_config, timeout=3)
    except Exception:
        return []
    data = safe_list(payload.get("data"))
    first_model = data[0] if data and isinstance(data[0], dict) else {}
    return clean_unique_strings(first_model.get("voices") or payload.get("voices") or [])


def customer_voice_runtime_builtin_voices(openclaw_dir, runtime_config):
    return _customer_voice_runtime_builtin_voices(openclaw_dir, runtime_config)


def _customer_voice_runtime_available_voices(openclaw_dir, runtime_config):
    runtime_config = runtime_config if isinstance(runtime_config, dict) else {}
    builtin = _customer_voice_runtime_builtin_voices(openclaw_dir, runtime_config)
    provider = str(runtime_config.get("provider") or "").strip().lower()
    if provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3:
        return clean_unique_strings([*builtin, *customer_voice_custom_voices(openclaw_dir)])
    if provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX:
        return builtin
    if provider != CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        return builtin
    return clean_unique_strings([*builtin, *customer_voice_custom_voices(openclaw_dir)])


def _preferred_qwen3_voice(available_voices):
    voices = clean_unique_strings(available_voices or [])
    if not voices:
        return "serena"
    ordered = [item for item in CUSTOMER_VOICE_QWEN3_PREFERRED_ORDER if item in voices]
    return ordered[0] if ordered else voices[0]


def _preferred_qwen3_builtin_voices(available_voices):
    voices = clean_unique_strings(available_voices or [])
    if not voices:
        return []
    ordered = [item for item in CUSTOMER_VOICE_QWEN3_PREFERRED_ORDER if item in voices]
    return ordered or voices


def _distinct_qwen3_voice_for_agent(openclaw_dir, agent_id, available_voices):
    voices = _preferred_qwen3_builtin_voices(available_voices or [])
    if not voices:
        return _preferred_qwen3_voice(voices)
    config = load_config(openclaw_dir)
    ordered_agent_ids = [
        str(item.get("id") or "").strip()
        for item in safe_list(load_agents(config))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    normalized_agent_id = str(agent_id or "").strip()
    if normalized_agent_id and normalized_agent_id in ordered_agent_ids:
        return voices[ordered_agent_ids.index(normalized_agent_id) % len(voices)]
    return _preferred_qwen3_voice(voices)


def _should_auto_assign_distinct_qwen3_voice(configured_voice, source, default_voice, available_voices):
    voices = clean_unique_strings(available_voices or [])
    if len(voices) <= 1:
        return False
    normalized_source = str(source or "").strip().lower()
    if normalized_source not in {"", "product-default", "agent-profile-bootstrap"}:
        return False
    preferred_voices = _preferred_qwen3_builtin_voices(voices)
    requested = str(configured_voice or "").strip().lower()
    if not requested:
        return True
    if requested == str(default_voice or "").strip().lower():
        return True
    if preferred_voices and requested in voices and requested not in preferred_voices:
        return True
    legacy_voice = CUSTOMER_VOICE_QWEN3_VOICE_ALIASES.get(requested)
    return bool(legacy_voice and legacy_voice == str(default_voice or "").strip().lower())


def _preferred_cosyvoice_voice(available_voices):
    voices = clean_unique_strings(available_voices or [])
    voices = _preferred_cosyvoice_builtin_voices(voices)
    if not voices:
        return "中文女"
    exact = next((item for item in voices if item == "中文女"), "")
    if exact:
        return exact
    fuzzy = next((item for item in voices if "中文女" in item or item.endswith("女")), "")
    if fuzzy:
        return fuzzy
    return voices[0]


def _preferred_cosyvoice_builtin_voices(available_voices):
    voices = clean_unique_strings(available_voices or [])
    if not voices:
        return []
    chinese_like = [
        item
        for item in voices
        if any(token in str(item or "") for token in ("中文", "普通话", "国语", "粤语"))
    ]
    return chinese_like or voices


def _distinct_cosyvoice_voice_for_agent(openclaw_dir, agent_id, available_voices):
    voices = _preferred_cosyvoice_builtin_voices(available_voices or [])
    if not voices:
        return _preferred_cosyvoice_voice(voices)
    config = load_config(openclaw_dir)
    ordered_agent_ids = [
        str(item.get("id") or "").strip()
        for item in safe_list(load_agents(config))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    normalized_agent_id = str(agent_id or "").strip()
    if normalized_agent_id and normalized_agent_id in ordered_agent_ids:
        return voices[ordered_agent_ids.index(normalized_agent_id) % len(voices)]
    return _preferred_cosyvoice_voice(voices)


def _should_auto_assign_distinct_cosyvoice_voice(configured_voice, source, default_voice, available_voices):
    voices = clean_unique_strings(available_voices or [])
    if len(voices) <= 1:
        return False
    normalized_source = str(source or "").strip().lower()
    if normalized_source not in {"", "product-default", "agent-profile-bootstrap"}:
        return False
    preferred_voices = _preferred_cosyvoice_builtin_voices(voices)
    requested = str(configured_voice or "").strip()
    if not requested:
        return True
    if requested == str(default_voice or "").strip():
        return True
    if preferred_voices and requested in voices and requested not in preferred_voices:
        return True
    legacy_voice = CUSTOMER_VOICE_COSYVOICE_VOICE_ALIASES.get(requested.lower())
    return bool(legacy_voice and legacy_voice == str(default_voice or "").strip())


def _preferred_sherpa_onnx_voice(available_voices):
    voices = clean_unique_strings(available_voices or [])
    voices = _preferred_sherpa_onnx_builtin_voices(voices)
    if not voices:
        return "zf_001"
    exact = next((item for item in voices if item == "zf_001"), "")
    if exact:
        return exact
    return voices[0]


def _preferred_sherpa_onnx_builtin_voices(available_voices):
    voices = clean_unique_strings(available_voices or [])
    if not voices:
        return []
    ordered = [item for item in CUSTOMER_VOICE_SHERPA_ONNX_PREFERRED_ORDER if item in voices]
    return ordered or voices


def _distinct_sherpa_onnx_voice_for_agent(openclaw_dir, agent_id, available_voices):
    voices = _preferred_sherpa_onnx_builtin_voices(available_voices or [])
    if not voices:
        return _preferred_sherpa_onnx_voice(voices)
    config = load_config(openclaw_dir)
    ordered_agent_ids = [
        str(item.get("id") or "").strip()
        for item in safe_list(load_agents(config))
        if isinstance(item, dict) and str(item.get("id") or "").strip()
    ]
    normalized_agent_id = str(agent_id or "").strip()
    if normalized_agent_id and normalized_agent_id in ordered_agent_ids:
        return voices[ordered_agent_ids.index(normalized_agent_id) % len(voices)]
    return _preferred_sherpa_onnx_voice(voices)


def _should_auto_assign_distinct_sherpa_onnx_voice(configured_voice, source, default_voice, available_voices):
    voices = clean_unique_strings(available_voices or [])
    if len(voices) <= 1:
        return False
    normalized_source = str(source or "").strip().lower()
    if normalized_source not in {"", "product-default", "agent-profile-bootstrap"}:
        return False
    preferred_voices = _preferred_sherpa_onnx_builtin_voices(voices)
    requested = str(configured_voice or "").strip().lower()
    if not requested:
        return True
    if requested == str(default_voice or "").strip().lower():
        return True
    if preferred_voices and requested in voices and requested not in preferred_voices:
        return True
    legacy_voice = CUSTOMER_VOICE_SHERPA_ONNX_VOICE_ALIASES.get(requested)
    return bool(legacy_voice and legacy_voice == str(default_voice or "").strip().lower())


def _customer_voice_provider_voice_name(
    voice,
    provider=CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI,
    available_voices=None,
    agent_id="",
    openclaw_dir="",
):
    normalized = str(voice or WECHAT_VOICE_REPLY_DEFAULT_VOICE).strip().lower()
    normalized_provider = str(provider or CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI).strip().lower()
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3:
        speakers = clean_unique_strings(available_voices or [])
        requested = str(voice or "").strip().lower()
        custom_agent_id = customer_voice_custom_voice_agent_id(requested)
        if custom_agent_id and customer_voice_effective_custom_sample_payload(openclaw_dir, custom_agent_id):
            return customer_voice_custom_voice_id(custom_agent_id)
        if requested and requested in speakers:
            return requested
        alias = CUSTOMER_VOICE_QWEN3_VOICE_ALIASES.get(normalized)
        if alias and alias in speakers:
            return alias
        builtin_speakers = [item for item in speakers if not customer_voice_custom_voice_agent_id(item)]
        return _distinct_qwen3_voice_for_agent(openclaw_dir, agent_id, builtin_speakers or speakers)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX:
        speakers = clean_unique_strings(available_voices or [])
        requested = str(voice or "").strip().lower()
        if requested and requested in speakers:
            return requested
        alias = CUSTOMER_VOICE_SHERPA_ONNX_VOICE_ALIASES.get(normalized)
        if alias and alias in speakers:
            return alias
        return _distinct_sherpa_onnx_voice_for_agent(openclaw_dir, agent_id, speakers)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE:
        speakers = clean_unique_strings(available_voices or [])
        requested = str(voice or "").strip()
        custom_agent_id = customer_voice_custom_voice_agent_id(requested)
        if custom_agent_id and customer_voice_effective_custom_sample_payload(openclaw_dir, custom_agent_id):
            return customer_voice_custom_voice_id(custom_agent_id)
        if requested and requested in speakers:
            return requested
        alias = CUSTOMER_VOICE_COSYVOICE_VOICE_ALIASES.get(normalized)
        if alias and alias in speakers:
            return alias
        builtin_speakers = [item for item in speakers if not customer_voice_custom_voice_agent_id(item)]
        return _distinct_cosyvoice_voice_for_agent(openclaw_dir, agent_id, builtin_speakers or speakers)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU:
        if normalized in {"tongtong", "chuichui", "xiaochen"}:
            return normalized
        return CUSTOMER_VOICE_ZHIPU_VOICE_ALIASES.get(normalized, "tongtong")
    return normalized or "alloy"


def _generate_customer_voice_reply_audio_via_openai_compatible(
    openclaw_dir,
    normalized_text,
    runtime_config,
    voice=WECHAT_VOICE_REPLY_DEFAULT_VOICE,
    instructions="",
    speed=None,
):
    normalized_provider = str(runtime_config.get("provider") or CUSTOMER_VOICE_RUNTIME_PROVIDER_OPENAI).strip().lower()
    endpoint = _customer_voice_audio_endpoint(runtime_config.get("baseUrl"), provider=normalized_provider)
    if not endpoint:
        raise RuntimeError("未配置语音服务地址，无法生成语音回复。")
    env = openclaw_command_env(openclaw_dir)
    default_api_key_env = (
        DEFAULT_CUSTOMER_VOICE_ZHIPU_API_KEY_ENV
        if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU
        else (
            DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_API_KEY_ENV
            if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX
            else (
            DEFAULT_CUSTOMER_VOICE_QWEN3_API_KEY_ENV
            if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3
            else (
                DEFAULT_CUSTOMER_VOICE_COSYVOICE_API_KEY_ENV
                if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE
                else DEFAULT_CUSTOMER_VOICE_API_KEY_ENV
            )
            )
        )
    )
    api_key_env = str(runtime_config.get("apiKeyEnv") or default_api_key_env).strip()
    api_key_candidates = [env.get(api_key_env)]
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU:
        api_key_candidates.extend([env.get("ZAI_API_KEY"), env.get("BIGMODEL_API_KEY"), env.get("ZHIPUAI_API_KEY")])
    else:
        api_key_candidates.append(env.get("OPENAI_API_KEY"))
    api_key = str(next((value for value in api_key_candidates if str(value or "").strip()), "") or "").strip()
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3 and not api_key:
        api_key = DEFAULT_CUSTOMER_VOICE_QWEN3_API_KEY
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX and not api_key:
        api_key = DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_API_KEY
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE and not api_key:
        api_key = DEFAULT_CUSTOMER_VOICE_COSYVOICE_API_KEY
    response_format = "wav" if normalized_provider in {CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU, CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE, CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3, CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX} else "mp3"
    output_suffix = ".wav" if response_format == "wav" else ".mp3"
    available_voices = _customer_voice_runtime_available_voices(openclaw_dir, runtime_config)
    output_path = customer_voice_reply_temp_dir(openclaw_dir) / f"wechat-reply-{int(time.time() * 1000)}-{secrets.token_hex(4)}{output_suffix}"
    payload_base = {
        "model": str(
            runtime_config.get("model")
            or (
                DEFAULT_CUSTOMER_VOICE_ZHIPU_MODEL
                if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU
                else (
                    DEFAULT_CUSTOMER_VOICE_SHERPA_ONNX_MODEL
                    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_SHERPA_ONNX
                    else (
                    DEFAULT_CUSTOMER_VOICE_QWEN3_MODEL
                    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_QWEN3
                    else (
                        DEFAULT_CUSTOMER_VOICE_COSYVOICE_MODEL
                        if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_COSYVOICE
                        else DEFAULT_CUSTOMER_VOICE_OPENAI_MODEL
                    )
                    )
                )
            )
        ).strip(),
        "voice": _customer_voice_provider_voice_name(
            voice,
            provider=normalized_provider,
            available_voices=available_voices,
            openclaw_dir=openclaw_dir,
        ),
        "response_format": response_format,
    }
    normalized_instructions = str(instructions or "").strip()
    if normalized_instructions:
        payload_base["instructions"] = normalized_instructions
    normalized_speed = normalize_voice_reply_speed(speed, default=DEFAULT_AGENT_VOICE_REPLY_SPEED)
    if normalized_speed != DEFAULT_AGENT_VOICE_REPLY_SPEED:
        payload_base["speed"] = normalized_speed
    headers = {
        "Content-Type": "application/json",
        "Accept": "audio/mpeg, application/octet-stream;q=0.9, */*;q=0.1",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request_timeout = customer_voice_runtime_timeout_seconds(normalized_provider)
    candidate_inputs = [normalized_text]
    audio_bytes = b""
    last_error = None
    for candidate_text in candidate_inputs:
        payload = {**payload_base, "input": candidate_text}
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(endpoint, data=data, headers=headers, method="POST")
        try:
            with urlopen(request, timeout=request_timeout) as response:
                audio_bytes = response.read()
            if audio_bytes:
                break
        except HTTPError as error:
            detail = ""
            try:
                detail = error.read().decode("utf-8", "replace")
            except Exception:
                detail = ""
            last_error = RuntimeError(detail or f"语音服务返回错误状态 {error.code}。")
            raise last_error from error
        except URLError as error:
            raise RuntimeError(str(error.reason) or "语音服务连接失败。") from error
    if not audio_bytes and last_error:
        raise last_error
    if not audio_bytes:
        raise RuntimeError("语音服务没有返回音频数据。")
    output_path.write_bytes(audio_bytes)
    if normalized_provider == CUSTOMER_VOICE_RUNTIME_PROVIDER_ZHIPU and output_path.suffix == ".wav":
        ffmpeg = shutil.which("ffmpeg")
        if ffmpeg:
            converted_path = output_path.with_suffix(".mp3")
            process = subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-i",
                    str(output_path),
                    "-codec:a",
                    "libmp3lame",
                    "-q:a",
                    "3",
                    str(converted_path),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if process.returncode == 0 and converted_path.exists() and converted_path.stat().st_size > 0:
                output_path.unlink(missing_ok=True)
                return converted_path
    return output_path


def generate_customer_voice_reply_audio(
    openclaw_dir,
    content,
    voice=None,
    instructions="",
    agent_id="",
    speed=None,
):
    runtime_config = customer_voice_runtime_config(openclaw_dir)
    normalized_text = optimize_customer_voice_reply_text(
        content,
        provider=runtime_config.get("provider"),
    )
    if not normalized_text:
        raise RuntimeError("没有可用于语音回复的文本内容。")
    resolved_voice = str(voice or "").strip()
    resolved_speed = speed
    resolved_instructions = str(instructions or "").strip()
    normalized_agent_id = str(agent_id or "").strip()
    agent_voice_profile = {}
    if normalized_agent_id:
        agent_voice_profile = resolve_agent_voice_reply_profile(openclaw_dir, normalized_agent_id)
        if not resolved_voice:
            resolved_voice = str(agent_voice_profile.get("voice") or "").strip()
        if resolved_speed is None:
            resolved_speed = agent_voice_profile.get("speed")
        if not resolved_instructions:
            resolved_instructions = str(agent_voice_profile.get("instructions") or "").strip()
    if not resolved_instructions:
        resolved_instructions = default_customer_voice_runtime_instructions(runtime_config.get("provider"))
    return _generate_customer_voice_reply_audio_via_openai_compatible(
        openclaw_dir,
        normalized_text,
        runtime_config,
        voice=resolved_voice or WECHAT_VOICE_REPLY_DEFAULT_VOICE,
        instructions=resolved_instructions,
        speed=resolved_speed,
    )


def encode_multipart_formdata(fields=None, files=None):
    fields = fields if isinstance(fields, dict) else {}
    files = safe_list(files)
    boundary = f"----MissionControlBoundary{secrets.token_hex(12)}"
    chunks = []
    for name, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )
    for item in files:
        if not isinstance(item, dict):
            continue
        filename = str(item.get("filename") or "upload.bin").strip() or "upload.bin"
        content_type = str(item.get("contentType") or "application/octet-stream").strip() or "application/octet-stream"
        data = item.get("data")
        if isinstance(data, str):
            data = data.encode("utf-8")
        data = data if isinstance(data, (bytes, bytearray)) else b""
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                (
                    f'Content-Disposition: form-data; name="{str(item.get("name") or "file").strip() or "file"}"; '
                    f'filename="{filename}"\r\n'
                ).encode("utf-8"),
                f"Content-Type: {content_type}\r\n\r\n".encode("utf-8"),
                bytes(data),
                b"\r\n",
            ]
        )
    chunks.append(f"--{boundary}--\r\n".encode("utf-8"))
    return b"".join(chunks), f"multipart/form-data; boundary={boundary}"


def upload_wechat_temporary_media(channel, media_type, file_path):
    path = Path(str(file_path or "")).expanduser()
    if not path.exists():
        raise RuntimeError("待上传的微信语音文件不存在。")
    token = wechat_access_token_for_channel(channel)
    request_url = (
        "https://api.weixin.qq.com/cgi-bin/media/upload"
        f"?access_token={quote(token)}&type={quote(str(media_type or 'voice').strip() or 'voice')}"
    )
    content_type = mimetypes.guess_type(str(path.name))[0] or "application/octet-stream"
    body, multipart_content_type = encode_multipart_formdata(
        files=[
            {
                "name": "media",
                "filename": path.name,
                "contentType": content_type,
                "data": path.read_bytes(),
            }
        ]
    )
    request = Request(
        request_url,
        data=body,
        headers={"Content-Type": multipart_content_type},
        method="POST",
    )
    payload = json.loads(urlopen(request, timeout=20).read().decode("utf-8", "replace") or "{}")
    if str(payload.get("media_id") or "").strip():
        return payload
    raise RuntimeError(str(payload.get("errmsg") or "微信临时素材上传失败。").strip() or "微信临时素材上传失败。")


def send_wechat_customer_service_text(channel, external_user_id, content):
    normalized_user_id = str(external_user_id or "").strip()
    reply_text = str(content or "").strip()
    if not normalized_user_id or not reply_text:
        return {}
    token = wechat_access_token_for_channel(channel)
    request_url = f"https://api.weixin.qq.com/cgi-bin/message/custom/send?access_token={quote(token)}"
    payload = {
        "touser": normalized_user_id,
        "msgtype": "text",
        "text": {"content": reply_text[:WECHAT_VOICE_REPLY_MAX_TEXT_SEND_CHARS]},
    }
    request = Request(
        request_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    response = json.loads(urlopen(request, timeout=10).read().decode("utf-8", "replace") or "{}")
    errcode = int(response.get("errcode") or 0)
    if errcode != 0:
        raise RuntimeError(str(response.get("errmsg") or f"微信消息发送失败（{errcode}）").strip())
    return response


def send_wechat_customer_service_voice(channel, external_user_id, media_id):
    normalized_user_id = str(external_user_id or "").strip()
    normalized_media_id = str(media_id or "").strip()
    if not normalized_user_id or not normalized_media_id:
        return {}
    token = wechat_access_token_for_channel(channel)
    request_url = f"https://api.weixin.qq.com/cgi-bin/message/custom/send?access_token={quote(token)}"
    payload = {
        "touser": normalized_user_id,
        "msgtype": "voice",
        "voice": {"media_id": normalized_media_id},
    }
    request = Request(
        request_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    response = json.loads(urlopen(request, timeout=10).read().decode("utf-8", "replace") or "{}")
    errcode = int(response.get("errcode") or 0)
    if errcode != 0:
        raise RuntimeError(str(response.get("errmsg") or f"微信语音发送失败（{errcode}）").strip())
    return response


def try_send_wechat_customer_service_voice(openclaw_dir, channel, external_user_id, content):
    voice_config = _svc().customer_channel_voice_reply_config(channel)
    if not voice_config.get("enabled"):
        return {}
    audio_path = None
    try:
        audio_path = _svc().generate_customer_voice_reply_audio(
            openclaw_dir,
            content,
            voice=voice_config.get("voice") or WECHAT_VOICE_REPLY_DEFAULT_VOICE,
            instructions=voice_config.get("instructions") or "",
        )
        media = _svc().upload_wechat_temporary_media(channel, "voice", audio_path)
        media_id = str(media.get("media_id") or "").strip()
        if not media_id:
            raise RuntimeError("微信语音素材上传后没有返回 media_id。")
        return _svc().send_wechat_customer_service_voice(channel, external_user_id, media_id)
    finally:
        if audio_path:
            Path(audio_path).unlink(missing_ok=True)


def extract_json_object_from_output(output_text):
    text = str(output_text or "").strip()
    if not text:
        return {}
    for line in reversed(text.splitlines()):
        candidate = line.strip()
        if not candidate or not candidate.startswith("{"):
            continue
        try:
            return json.loads(candidate)
        except Exception:
            continue
    try:
        return json.loads(text)
    except Exception:
        return {}


def send_openclaw_weixin_customer_reply(openclaw_dir, channel, external_user_id, content, prefer_voice=False):
    channel = channel if isinstance(channel, dict) else {}
    meta = channel.get("meta") if isinstance(channel.get("meta"), dict) else {}
    account_id = str(meta.get("accountId") or channel.get("accountId") or "").strip()
    target = str(external_user_id or "").strip()
    reply_text = str(content or "").strip()
    if not account_id or not target:
        return {}
    env = _svc().openclaw_command_env(openclaw_dir)
    openclaw_bin = str(env.get("OPENCLAW_BIN") or "openclaw").strip() or "openclaw"
    audio_path = None
    if not prefer_voice and not reply_text:
        return {}
    args = [
        openclaw_bin,
        "message",
        "send",
        "--channel",
        "openclaw-weixin",
        "--account",
        account_id,
        "--target",
        target,
        "--json",
    ]
    try:
        if prefer_voice:
            voice_config = _svc().customer_channel_voice_reply_config(channel)
            audio_path = _svc().generate_customer_voice_reply_audio(
                openclaw_dir,
                reply_text,
                voice=voice_config.get("voice") or WECHAT_VOICE_REPLY_DEFAULT_VOICE,
                instructions=voice_config.get("instructions") or "",
            )
            args.extend(["--media", str(audio_path)])
        elif reply_text:
            args.extend(["--message", reply_text])
        process = _svc().run_command(args, env=env, timeout=customer_voice_reply_timeout_seconds())
        if process.returncode != 0:
            detail = "\n".join(part.strip() for part in [process.stdout, process.stderr] if part and part.strip())
            raise RuntimeError(detail or "OpenClaw 微信消息发送失败。")
        payload = _svc().extract_json_object_from_output(process.stdout)
        return payload if isinstance(payload, dict) else {}
    finally:
        if audio_path:
            Path(audio_path).unlink(missing_ok=True)


def maybe_forward_chat_thread_reply_to_wechat(openclaw_dir, thread, reply_text):
    thread = thread if isinstance(thread, dict) else {}
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    channel_id = str(meta.get("customerChannelId") or "").strip()
    external_user_id = str(meta.get("customerExternalUserId") or "").strip()
    if not channel_id or not external_user_id:
        return {}
    channel = _svc().resolve_customer_access_channel(openclaw_dir, channel_id)
    if not channel:
        raise RuntimeError("关联的微信入口不存在。")
    customer_source = str(meta.get("customerSource") or meta.get("customerChannelType") or channel.get("type") or "").strip().lower()
    if customer_source == "wechat_official":
        config = _svc().wechat_official_channel_config(channel)
        if not str(config.get("appId") or "").strip() or not str(config.get("appSecret") or "").strip():
            return {}
        if config.get("voiceReplyEnabled"):
            try:
                return _svc().try_send_wechat_customer_service_voice(openclaw_dir, channel, external_user_id, reply_text)
            except Exception:
                pass
        return _svc().send_wechat_customer_service_text(channel, external_user_id, reply_text)
    if customer_source == "openclaw_weixin":
        try:
            return _svc().send_openclaw_weixin_customer_reply(
                openclaw_dir,
                channel,
                external_user_id,
                reply_text,
                prefer_voice=bool(_svc().customer_channel_voice_reply_config(channel).get("enabled")),
            )
        except Exception:
            if not reply_text:
                raise
            return _svc().send_openclaw_weixin_customer_reply(openclaw_dir, channel, external_user_id, reply_text, prefer_voice=False)
    return {}


def dispatch_customer_wechat_message(openclaw_dir, channel, thread, inbound_message, merged_message, server=None):
    thread = thread if isinstance(thread, dict) else {}
    inbound_message = inbound_message if isinstance(inbound_message, dict) else {}
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_policy = meta.get("teamPolicy") if isinstance(meta.get("teamPolicy"), dict) else {}
    primary_agent_id = str(thread.get("currentTargetAgentId") or thread.get("primaryAgentId") or get_router_agent_id(load_config(openclaw_dir))).strip()
    participant_agent_ids = safe_list(thread.get("participantAgentIds")) or ([primary_agent_id] if primary_agent_id else [])
    dispatch_mode = "broadcast" if len(participant_agent_ids) > 1 else "direct"
    dispatch_agent_ids = (
        _svc().select_human_turn_targets(
            openclaw_dir,
            participant_agent_ids,
            purpose="broadcast",
            lead_agent_id=primary_agent_id,
        )
        if dispatch_mode == "broadcast"
        else ([primary_agent_id] if primary_agent_id else participant_agent_ids[:1])
    )
    dispatch = {
        "currentTargetAgentId": primary_agent_id,
        "dispatchMode": dispatch_mode,
        "dispatchAgentIds": dispatch_agent_ids,
        "participantAgentIds": participant_agent_ids,
    }
    return _svc().schedule_chat_thread_dispatch(
        openclaw_dir,
        thread,
        inbound_message,
        dispatch,
        merged_message,
        sessions_by_agent=(meta.get("sessionsByAgent") if isinstance(meta.get("sessionsByAgent"), dict) else {}),
        team_policy=team_policy,
        thinking="low",
        output_dir=getattr(server, "output_dir", None),
        defer_seconds=0.0,
        server=server,
        selected_skill_slugs=["web-content-fetcher"] if "http" in merged_message else [],
    )


def handle_wechat_customer_access_get(handler, channel_id):
    channel = _svc().resolve_customer_access_channel(handler.server.openclaw_dir, channel_id)
    if not channel:
        handler._send_bytes(b"channel not found", "text/plain; charset=utf-8", status=404)
        return
    if str(channel.get("status") or "").strip().lower() != "active":
        handler._send_bytes(b"channel disabled", "text/plain; charset=utf-8", status=403)
        return
    config = _svc().wechat_official_channel_config(channel)
    if not _svc().verify_wechat_signature(
        config.get("verificationToken"),
        handler._query().get("signature", [""])[0],
        handler._query().get("timestamp", [""])[0],
        handler._query().get("nonce", [""])[0],
    ):
        handler._send_bytes(b"invalid signature", "text/plain; charset=utf-8", status=403)
        return
    echo = str(handler._query().get("echostr", [""])[0] or "").strip()
    handler._send_bytes((echo or "ok").encode("utf-8"), "text/plain; charset=utf-8", status=200)


def handle_wechat_customer_access_post(handler, channel_id):
    channel = _svc().resolve_customer_access_channel(handler.server.openclaw_dir, channel_id)
    if not channel:
        handler._send_bytes(b"channel not found", "text/plain; charset=utf-8", status=404)
        return
    if str(channel.get("status") or "").strip().lower() != "active":
        handler._send_bytes(b"channel disabled", "text/plain; charset=utf-8", status=403)
        return
    config = _svc().wechat_official_channel_config(channel)
    if not _svc().verify_wechat_signature(
        config.get("verificationToken"),
        handler._query().get("signature", [""])[0],
        handler._query().get("timestamp", [""])[0],
        handler._query().get("nonce", [""])[0],
    ):
        handler._send_bytes(b"invalid signature", "text/plain; charset=utf-8", status=403)
        return
    length = int(handler.headers.get("Content-Length", "0") or "0")
    raw_body = handler.rfile.read(length).decode("utf-8", "replace") if length else ""
    inbound_payload = _svc().parse_wechat_xml_message(raw_body)
    external_user_id = str(inbound_payload.get("FromUserName") or "").strip()
    official_account_id = str(inbound_payload.get("ToUserName") or "").strip()
    if not external_user_id:
        handler._send_bytes(b"success", "text/plain; charset=utf-8", status=200)
        return
    customer_label = f"微信客户 {external_user_id[-6:]}"
    thread, created = _svc().ensure_customer_access_thread(
        handler.server.openclaw_dir,
        channel,
        external_user_id,
        external_display_name=customer_label,
    )
    message_text = _svc().summarize_wechat_inbound_message(inbound_payload)
    inbound_message = _svc().store_save_chat_message(
        handler.server.openclaw_dir,
        {
            "threadId": thread.get("id", ""),
            "senderKind": "customer",
            "senderId": external_user_id,
            "senderLabel": customer_label,
            "direction": "inbound",
            "body": message_text,
            "meta": {
                "channelId": str(channel.get("id") or "").strip(),
                "channelType": "wechat_official",
                "wechat": inbound_payload,
                "createdThread": bool(created),
            },
        },
    )
    merged_message = (
        f"来自微信客户 {customer_label} 的消息。\n"
        f"渠道：{str(channel.get('name') or '微信公众号').strip()}\n"
        f"内容：{message_text}"
    ).strip()
    _svc().dispatch_customer_wechat_message(
        handler.server.openclaw_dir,
        channel,
        thread,
        inbound_message,
        merged_message,
        server=handler.server,
    )
    _svc().invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
    auto_reply_enabled = bool(config.get("autoReplyEnabled", True))
    reply_body = _svc().build_wechat_text_reply(
        external_user_id,
        official_account_id,
        config.get("welcomeReply") if auto_reply_enabled else "",
    )
    handler._send_bytes(reply_body, "application/xml; charset=utf-8", status=200)


def perform_disable_customer_access_channel(openclaw_dir, channel_id):
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        raise RuntimeError("请提供客户入口编号。")
    channels = store_list_customer_access_channels(openclaw_dir)
    channel = next((item for item in channels if item.get("id") == channel_id), None)
    if not channel:
        raise RuntimeError("客户入口不存在。")
    if str(channel.get("status") or "") == "disabled":
        return channel
    return store_save_customer_access_channel(
        openclaw_dir,
        {
            **channel,
            "status": "disabled",
        },
    )


def perform_enable_customer_access_channel(openclaw_dir, channel_id):
    channel_id = str(channel_id or "").strip()
    if not channel_id:
        raise RuntimeError("请提供客户入口编号。")
    channels = store_list_customer_access_channels(openclaw_dir)
    channel = next((item for item in channels if item.get("id") == channel_id), None)
    if not channel:
        raise RuntimeError("客户入口不存在。")
    if str(channel.get("status") or "") == "active":
        return channel
    return store_save_customer_access_channel(
        openclaw_dir,
        {
            **channel,
            "status": "active",
        },
    )
