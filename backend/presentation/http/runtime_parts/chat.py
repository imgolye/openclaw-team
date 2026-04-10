"""Runtime part: chat."""

import json
import re
import secrets
import subprocess
import sys
import threading
import time

try:
    TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS
except NameError:
    TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS = 60


def _conversation_send_impl():
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        candidate = getattr(module, "perform_conversation_send", None)
        if callable(candidate):
            return candidate
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        candidate = getattr(module, "perform_conversation_send", None)
        if callable(candidate):
            return candidate
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        candidate = getattr(main, "perform_conversation_send", None)
        if callable(candidate):
            return candidate
    return perform_conversation_send


def _chat_runtime_callable(name, default):
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        candidate = getattr(module, name, None)
        if callable(candidate):
            return candidate
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        candidate = getattr(module, name, None)
        if callable(candidate):
            return candidate
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        candidate = getattr(main, name, None)
        if callable(candidate):
            return candidate
    return default

def safe_chat_attachments(value):
    attachments = []
    for item in safe_list(value):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        attachments.append(
            {
                "name": name[:240],
                "size": max(0, int(item.get("size") or 0)),
                "type": str(item.get("type") or "").strip()[:120],
                "preview": str(item.get("preview") or "").strip()[:2400],
            }
        )
    return attachments


def safe_chat_mentions(value):
    mentions = []
    for item in safe_list(value):
        mention = str(item or "").strip()
        if not mention:
            continue
        if mention not in mentions:
            mentions.append(mention[:120])
    return mentions


def safe_chat_reply_context(value):
    if not isinstance(value, dict):
        return {}
    reply_id = str(value.get("id") or "").strip()[:120]
    thread_id = str(value.get("threadId") or "").strip()[:120]
    sender = str(value.get("sender") or "").strip()[:160]
    text = str(value.get("text") or "").strip()[:280]
    if not reply_id and not sender and not text:
        return {}
    payload = {}
    if reply_id:
        payload["id"] = reply_id
    if thread_id:
        payload["threadId"] = thread_id
    if sender:
        payload["sender"] = sender
    if text:
        payload["text"] = text
    return payload


def prepare_chat_send_request(
    message_text,
    attachments=None,
    selected_skill_slugs=None,
    mention_agent_ids=None,
    reply_context=None,
    team_policy=None,
    thinking="low",
):
    normalized_message = str(message_text or "").strip()
    normalized_attachments = safe_chat_attachments(attachments)
    normalized_mentions = safe_chat_mentions(mention_agent_ids)
    normalized_reply_context = safe_chat_reply_context(reply_context)
    normalized_skills = clean_unique_strings(
        safe_list(selected_skill_slugs) + auto_selected_skill_slugs_for_message(normalized_message)
    )
    merged_message = merge_chat_message_with_attachments(
        normalized_message,
        normalized_attachments,
        normalized_mentions,
        team_policy=team_policy if isinstance(team_policy, dict) else {},
        reply_context=normalized_reply_context,
    ).strip()
    merged_message = append_conversation_voice_delivery_contract(merged_message)
    normalized_thinking = str(thinking or "").strip() or "low"
    if (
        normalized_thinking == "low"
        and normalized_message
        and len(normalized_message) <= 72
        and "\n" not in normalized_message
        and not normalized_attachments
        and not normalized_mentions
        and not normalized_reply_context
        and not normalized_skills
        and not conversation_message_requests_voice_reply(normalized_message)
    ):
        normalized_thinking = "minimal"
    return {
        "messageText": normalized_message,
        "attachments": normalized_attachments,
        "mentionAgentIds": normalized_mentions,
        "replyContext": normalized_reply_context,
        "selectedSkillSlugs": normalized_skills,
        "mergedMessage": merged_message,
        "thinking": normalized_thinking,
    }


CONVERSATION_CONTEXT_COMPRESSION_MESSAGE_THRESHOLD = 18
CONVERSATION_CONTEXT_COMPRESSION_CHARACTER_THRESHOLD = 4800
CONVERSATION_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES = 6
CONVERSATION_CONTEXT_COMPRESSION_MAX_HIGHLIGHTS = 8
CONVERSATION_VOICE_REPLY_TRIGGER_PATTERNS = (
    r"(语音|音频).{0,6}(回复|回答|返回|发我|发给我|发送|附上|输出)",
    r"(回复|回答|返回|发送).{0,6}(语音|音频)",
    r"(读出来|念出来|朗读|播报)",
    r"(voice|audio)\s+(reply|response|message|note)",
    r"(reply|respond|send).{0,16}(voice|audio)",
    r"read\s+(it|this)?\s*aloud",
    r"spoken\s+response",
)
CONVERSATION_VOICE_REPLY_GENERIC_PATTERNS = (
    r"^负责人好[，,。!！]?",
    r"^你好[，,。!！]?",
    r"有(什么|啥)需要我做的",
    r"有事直接说",
    r"直接说就行",
    r"我在呢",
)
CONVERSATION_REPLY_SENTINEL_TEXTS = {"NO_REPLY", "HEARTBEAT_OK"}
CONVERSATION_VOICE_REPLY_BACKGROUND_JOBS = set()
CONVERSATION_VOICE_REPLY_BACKGROUND_LOCK = threading.Lock()


def normalize_conversation_session_key(agent_id="", session_id="", conversation_key=""):
    normalized_agent_id = str(agent_id or "").strip()
    explicit_key = str(conversation_key or "").strip()
    candidate = explicit_key or str(session_id or "").strip() or "main"
    if not candidate:
        return ""
    if candidate.startswith("conversation:"):
        parts = candidate.split(":")
        if len(parts) >= 3:
            candidate = f"agent:{parts[1]}:{parts[2]}"
    if candidate == "main":
        return f"agent:{normalized_agent_id}:main" if normalized_agent_id else "main"
    if candidate.startswith(("agent:", "acp:", "cron:", "hook:", "node-")):
        return candidate
    if normalized_agent_id:
        return f"agent:{normalized_agent_id}:{candidate}"
    return candidate


def conversation_route_session_key(conversation_key):
    normalized_key = str(conversation_key or "").strip()
    if not normalized_key:
        return ""
    if normalized_key == "main" or normalized_key.endswith(":main"):
        return "main"
    return normalized_key


def conversation_display_session_id(agent_id="", conversation_key=""):
    normalized_key = str(conversation_key or "").strip()
    if not normalized_key:
        return "main"
    route_key = conversation_route_session_key(normalized_key)
    if route_key == "main":
        return "main"
    normalized_agent_id = str(agent_id or "").strip()
    prefix = f"agent:{normalized_agent_id}:"
    if normalized_agent_id and normalized_key.startswith(prefix):
        return normalized_key[len(prefix):] or "main"
    parts = normalized_key.split(":")
    if len(parts) >= 3 and parts[0] == "agent":
        return ":".join(parts[2:]).strip() or normalized_key
    return normalized_key


def build_direct_conversation_session_key(agent_id):
    normalized_agent_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-._") or "agent"
    return f"agent:{normalized_agent_id}:dm:{secrets.token_hex(6)}"


def build_primary_direct_conversation_session_key(agent_id):
    normalized_agent_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-._") or "agent"
    return f"agent:{normalized_agent_id}:dm:primary"


def normalize_product_conversation_session_key(agent_id="", session_id="", conversation_key=""):
    normalized_agent_id = str(agent_id or "").strip()
    explicit_key = str(conversation_key or "").strip()
    if explicit_key:
        normalized_key = normalize_conversation_session_key(normalized_agent_id, "", explicit_key)
        if conversation_route_session_key(normalized_key) == "main":
            return build_primary_direct_conversation_session_key(normalized_agent_id)
        return normalized_key
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id or normalized_session_id == "main":
        return build_primary_direct_conversation_session_key(normalized_agent_id)
    return normalize_conversation_session_key(normalized_agent_id, normalized_session_id, "")


def conversation_message_requests_voice_reply(message_text):
    normalized_text = str(message_text or "").strip()
    if not normalized_text:
        return False
    lowered = normalized_text.lower()
    return any(re.search(pattern, lowered, re.IGNORECASE) for pattern in CONVERSATION_VOICE_REPLY_TRIGGER_PATTERNS)


def _model_ref_looks_like_gemma(model_ref):
    normalized = str(model_ref or "").strip().lower()
    if not normalized:
        return False
    return normalized.startswith("gemma/") or normalized.startswith("gemma-") or "/gemma-" in normalized


def _model_ref_looks_like_zhipu(model_ref):
    normalized = str(model_ref or "").strip().lower()
    if not normalized:
        return False
    return (
        normalized.startswith("zai/")
        or normalized.startswith("zhipu/")
        or normalized.startswith("glm-")
        or "/glm-" in normalized
    )


def conversation_prefers_gemma_runtime(openclaw_dir, agent_id="", config=None, metadata=None):
    normalized_agent_id = str(agent_id or "").strip()
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    agents_payload = ((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {})
    agent_items = agents_payload.get("list") if isinstance(agents_payload.get("list"), list) else []
    for item in agent_items:
        if not isinstance(item, dict):
            continue
        if normalized_agent_id and str(item.get("id") or "").strip() != normalized_agent_id:
            continue
        if _model_ref_looks_like_gemma(item.get("model")):
            return True
        if normalized_agent_id:
            break
    local_metadata = metadata if isinstance(metadata, dict) else None
    if local_metadata is None:
        execution_config = local_config.get("execution") if isinstance(local_config.get("execution"), dict) else {}
        local_runtime_config = local_config.get("localRuntime") if isinstance(local_config.get("localRuntime"), dict) else {}
        has_runtime_hints = bool(execution_config) or bool(local_runtime_config)
        if not has_runtime_hints:
            return False
        try:
            local_metadata = load_project_metadata(openclaw_dir, config=local_config)
        except Exception:
            local_metadata = {}
    execution = local_metadata.get("execution") if isinstance(local_metadata.get("execution"), dict) else {}
    preferred_provider = str(execution.get("preferredProviderId") or "").strip().lower()
    if preferred_provider == "gemma":
        return True
    local_runtime = local_metadata.get("localRuntime") if isinstance(local_metadata.get("localRuntime"), dict) else {}
    if _model_ref_looks_like_gemma(local_runtime.get("modelPath")):
        return True
    return False


def conversation_prefers_explicit_direct_reply_contract(openclaw_dir, agent_id="", config=None, metadata=None):
    normalized_agent_id = str(agent_id or "").strip()
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    agents_payload = ((local_config.get("agents", {}) if isinstance(local_config, dict) else {}) or {})
    agent_items = agents_payload.get("list") if isinstance(agents_payload.get("list"), list) else []
    for item in agent_items:
        if not isinstance(item, dict):
            continue
        if normalized_agent_id and str(item.get("id") or "").strip() != normalized_agent_id:
            continue
        if _model_ref_looks_like_zhipu(item.get("model")):
            return True
        if normalized_agent_id:
            break
    local_metadata = metadata if isinstance(metadata, dict) else None
    if local_metadata is None:
        execution_config = local_config.get("execution") if isinstance(local_config.get("execution"), dict) else {}
        if not execution_config:
            return False
        try:
            local_metadata = load_project_metadata(openclaw_dir, config=local_config)
        except Exception:
            local_metadata = {}
    execution = local_metadata.get("execution") if isinstance(local_metadata.get("execution"), dict) else {}
    preferred_provider = str(execution.get("preferredProviderId") or "").strip().lower()
    return preferred_provider in {"zhipu", "zai"}


def normalize_conversation_thinking_for_model(openclaw_dir, agent_id="", thinking="low", config=None, metadata=None):
    normalized = str(thinking or "").strip() or "low"
    if normalized in {"low", "minimal", "none"} and conversation_prefers_gemma_runtime(
        openclaw_dir,
        agent_id=agent_id,
        config=config,
        metadata=metadata,
    ):
        return "none"
    return normalized


def conversation_voice_reply_followup_only_request(message_text):
    normalized_text = str(message_text or "").strip()
    if not conversation_message_requests_voice_reply(normalized_text):
        return False
    if re.search(r"[:：]\s*\S+", normalized_text):
        return False
    lowered = normalized_text.lower()
    if any(token in lowered for token in ("上一个", "上一条", "刚才", "刚刚", "前面", "继续按刚才", "延续刚才")):
        return True
    stripped = re.sub(r"[\s，,。！？!?:：、\"“”'‘’（）()【】\\[\\]<>《》-]+", "", lowered)
    stripped = re.sub(
        r"(请|只|就|直接|再|继续|给我|发我|发给我|回复我|回答我|返回给我|一句话|一段话|一段|一句|这条消息|这个消息|这句话|中文|语音|音频|voice|audio|reply|response|message|respond|send|read|aloud|spoken|回复|回答|返回|发送|发|读出来|念出来|朗读|播报|我|吧|呀|呢|啊)+",
        "",
        stripped,
        flags=re.IGNORECASE,
    )
    return len(stripped) <= 4


def _sanitize_conversation_explicit_candidate(value):
    candidate = str(value or "").strip()
    if not candidate:
        return ""
    candidate = re.split(
        r"\n\s*\n|\n(?:Current focus|Open loops|Recent team memory|Task learnings|Reply contract|Voice delivery contract|Product memory system|Direct reply contract)\b",
        candidate,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip()
    first_line = candidate.splitlines()[0].strip() if candidate.splitlines() else ""
    return first_line or candidate


def extract_explicit_conversation_voice_utterance(message_text):
    
    normalized_text = str(message_text or "").strip()
    if not conversation_message_requests_voice_reply(normalized_text):
        return ""
    colon_match = re.match(
        r"^[^:：]{0,40}(?:语音|音频|voice|audio)[^:：]{0,24}[:：]\s*(.+)$",
        normalized_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if colon_match:
        candidate = _sanitize_conversation_explicit_candidate(colon_match.group(1))
        if candidate:
            return candidate
    quoted_patterns = (
        r"(?:语音|音频|voice|audio)[^“\"'「『]{0,40}[“\"'「『](.+?)[”\"'」』]\s*$",
        r"(?:回复|回答|发送|朗读|念出来|读出来)[^“\"'「『]{0,24}[“\"'「『](.+?)[”\"'」』]\s*$",
    )
    for pattern in quoted_patterns:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            candidate = _sanitize_conversation_explicit_candidate(match.group(1))
            if candidate:
                return candidate
    return ""


def extract_explicit_conversation_reply_utterance(message_text):
    normalized_text = str(message_text or "").strip()
    if not normalized_text or conversation_message_requests_voice_reply(normalized_text):
        return ""
    colon_patterns = (
        r"^[^:：]{0,40}(?:回复|回答|返回|输出)[^:：]{0,24}[:：]\s*(.+)$",
        r"^[^:：]{0,20}(?:只用|就用)?(?:一句|一段话|一句话)?(?:中文)?(?:回复|回答)[^:：]{0,12}[:：]\s*(.+)$",
    )
    for pattern in colon_patterns:
        match = re.match(pattern, normalized_text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        candidate = _sanitize_conversation_explicit_candidate(match.group(1))
        if candidate:
            return candidate
    quoted_patterns = (
        r"(?:回复|回答|返回|输出)[^“\"'「『]{0,24}[“\"'「『](.+?)[”\"'」』]\s*$",
    )
    for pattern in quoted_patterns:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            candidate = _sanitize_conversation_explicit_candidate(match.group(1))
            if candidate:
                return candidate
    return ""


def rewrite_conversation_voice_request_text(message_text, followup_only=False):
    normalized_text = str(message_text or "").strip()
    if not conversation_message_requests_voice_reply(normalized_text):
        return normalized_text
    if followup_only:
        return "请延续上一条有效回答的内容，直接给出简体中文正文，不要提语音、音频、TTS、工具或配置。"
    explicit_utterance = extract_explicit_conversation_voice_utterance(normalized_text)
    if explicit_utterance:
        return explicit_utterance
    rewritten = normalized_text
    replacement_patterns = (
        (r"请?用中文语音回复我一句", "请用中文直接回复我一句"),
        (r"请?用语音回复我一句", "请直接回复我一句"),
        (r"请?用中文语音回复我", "请用中文直接回复我"),
        (r"请?用语音回复我", "请直接回复我"),
        (r"请?语音回复我", "请直接回复我"),
        (r"请?给我语音回复", "请直接回复我"),
        (r"请?发我语音", "请直接回复我"),
        (r"请?音频回复", "请直接回复"),
        (r"请?语音回答", "请直接回答"),
        (r"请?用语音回答", "请直接回答"),
        (r"请?用中文语音回答", "请用中文直接回答"),
        (r"请?用中文音频回答", "请用中文直接回答"),
        (r"请?用音频回复", "请直接回复"),
    )
    for pattern, replacement in replacement_patterns:
        rewritten = re.sub(pattern, replacement, rewritten, flags=re.IGNORECASE)
    return rewritten.strip() or normalized_text


def conversation_reply_contains_cjk(text):
    return bool(re.search(r"[\u3400-\u9fff]", str(text or "")))


def conversation_reply_looks_like_generic_voice_filler(text):
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return False
    return any(
        re.search(pattern, normalized_text, flags=re.IGNORECASE)
        for pattern in CONVERSATION_VOICE_REPLY_GENERIC_PATTERNS
    )


def should_replace_voice_followup_reply(reply_text, fallback_text):
    normalized_reply = str(reply_text or "").strip()
    normalized_fallback = str(fallback_text or "").strip()
    if not normalized_fallback:
        return False
    if not normalized_reply:
        return True
    if normalized_reply == normalized_fallback:
        return False
    if conversation_reply_looks_like_generic_voice_filler(normalized_reply):
        return True
    if conversation_reply_contains_cjk(normalized_fallback) and not conversation_reply_contains_cjk(normalized_reply):
        return True
    return False


def conversation_reply_is_placeholder(text):
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return True
    return normalized_text.upper() in CONVERSATION_REPLY_SENTINEL_TEXTS


def conversation_voice_delivery_contract(message_text=""):
    explicit_utterance = extract_explicit_conversation_voice_utterance(message_text)
    contract = (
        "\n\nVoice delivery contract\n"
        "- Voice is only the delivery format. First answer the user's actual request using the current conversation context, role, and any relevant facts."
        "\n- Preserve the original meaning, constraints, names, numbers, and follow-up intent from the user's message."
        "\n- If the user only asks for a voice reply, keep following the active topic and the most recent substantive request instead of switching to a generic greeting, filler, or repeated stock line."
        "\n- Reply in Simplified Chinese unless the user explicitly requests another language."
        "\n- Then write the final answer as a natural spoken script in plain text so the product can read it aloud."
        "\n- Keep it concise only when the underlying request is simple; if the user asks for explanation or summary, include the needed substance."
        "\n- The product will generate and attach the audio reply after your text is returned."
        "\n- Never call built-in speech/TTS/audio generation tools for this request."
        "\n- Do not mention provider configuration, TTS setup, or fallback limitations in the visible reply."
    )
    if explicit_utterance:
        contract = (
            f"{contract}\n- The user explicitly provided the exact sentence to speak. Output exactly that sentence in Simplified Chinese."
            "\n- Do not add acknowledgements, greetings, summaries, prefixes, suffixes, or extra explanation."
            f"\n- Exact spoken sentence\n{explicit_utterance}"
        )
    return contract


def append_conversation_voice_delivery_contract(message_text):
    text = str(message_text or "").strip()
    if not text or not conversation_message_requests_voice_reply(text):
        return text
    contract = conversation_voice_delivery_contract(text)
    return text if contract.strip() in text else f"{text}{contract}"


def append_conversation_voice_followup_context(message_text, latest_reply_text):
    text = str(message_text or "").strip()
    latest_reply_text = str(latest_reply_text or "").strip()
    if not text or not latest_reply_text:
        return text
    block = (
        "\n\nVoice follow-up context\n"
        "- The user is only changing the delivery format to voice and is not changing the topic."
        "\n- Reuse the latest substantive answer below as the spoken reply body unless the user explicitly asks to revise it."
        "\n- Keep the meaning aligned with that answer. Do not switch to a greeting, filler, or generic stock line."
        "\n- Reply in Simplified Chinese unless the user explicitly requests another language."
        f"\nLatest substantive answer\n{latest_reply_text}"
    )
    return text if block.strip() in text else f"{text}{block}"


def conversation_direct_reply_contract(explicit_utterance=""):
    contract = (
        "\n\nDirect reply contract\n"
        "- This is a user-visible direct conversation with the product owner."
        "\n- Always produce one visible assistant reply to the new user message."
        "\n- Never output NO_REPLY or HEARTBEAT_OK in a direct conversation."
        "\n- If context is insufficient, ask one concise clarifying question in Simplified Chinese instead of using a sentinel."
        "\n- Reply in Simplified Chinese unless the user explicitly requests another language."
    )
    explicit_utterance = str(explicit_utterance or "").strip()
    if explicit_utterance:
        contract = (
            f"{contract}\n- The user explicitly provided the exact visible sentence to return."
            "\n- Output exactly that sentence in Simplified Chinese."
            "\n- Do not add greetings, acknowledgements, prefixes, suffixes, or extra explanation."
            f"\n- Exact visible sentence\n{explicit_utterance}"
        )
    return contract


def append_conversation_direct_reply_contract(message_text, explicit_utterance=""):
    text = str(message_text or "").strip()
    if not text:
        return text
    contract = conversation_direct_reply_contract(explicit_utterance=explicit_utterance)
    return text if contract.strip() in text else f"{text}{contract}"


def _conversation_attachment_lane_dir(openclaw_dir, agent_id, session_id="", conversation_key=""):
    normalized_agent_id = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(agent_id or "").strip()).strip("-._") or "agent"
    normalized_key = normalize_conversation_session_key(agent_id, session_id, conversation_key)
    lane_hash = hashlib.sha1(normalized_key.encode("utf-8")).hexdigest()[:24]
    lane_dir = dashboard_dir(openclaw_dir) / "conversation-attachments" / normalized_agent_id / lane_hash
    lane_dir.mkdir(parents=True, exist_ok=True)
    return lane_dir


def _conversation_attachment_manifest_path(openclaw_dir, agent_id, session_id="", conversation_key=""):
    return _conversation_attachment_lane_dir(openclaw_dir, agent_id, session_id, conversation_key) / "manifest.json"


def _conversation_voice_reply_status_path(openclaw_dir, agent_id, session_id="", conversation_key=""):
    return _conversation_attachment_lane_dir(openclaw_dir, agent_id, session_id, conversation_key) / "voice-reply-status.json"


def _conversation_voice_reply_job_key(agent_id, session_id="", conversation_key="", item_id=""):
    normalized_key = normalize_conversation_session_key(agent_id, session_id, conversation_key)
    return "::".join(
        part for part in ["conversation-voice", str(agent_id or "").strip(), normalized_key, str(item_id or "").strip()] if part
    )


def _start_conversation_voice_reply_job(job_key):
    normalized_key = str(job_key or "").strip()
    if not normalized_key:
        return False
    with CONVERSATION_VOICE_REPLY_BACKGROUND_LOCK:
        if normalized_key in CONVERSATION_VOICE_REPLY_BACKGROUND_JOBS:
            return False
        CONVERSATION_VOICE_REPLY_BACKGROUND_JOBS.add(normalized_key)
    return True


def _finish_conversation_voice_reply_job(job_key):
    normalized_key = str(job_key or "").strip()
    if not normalized_key:
        return
    with CONVERSATION_VOICE_REPLY_BACKGROUND_LOCK:
        CONVERSATION_VOICE_REPLY_BACKGROUND_JOBS.discard(normalized_key)


def _normalize_conversation_attachment_record(record):
    record = record if isinstance(record, dict) else {}
    attachment_id = str(record.get("id") or "").strip()
    item_id = str(record.get("itemId") or "").strip()
    content_path = str(record.get("contentPath") or "").strip()
    if not attachment_id or not item_id or not content_path:
        return {}
    return {
        "id": attachment_id,
        "itemId": item_id,
        "kind": str(record.get("kind") or "file").strip() or "file",
        "name": str(record.get("name") or "attachment.bin").strip() or "attachment.bin",
        "mimeType": str(record.get("mimeType") or "application/octet-stream").strip() or "application/octet-stream",
        "size": max(0, int(record.get("size") or 0)),
        "createdAt": str(record.get("createdAt") or "").strip(),
        "source": str(record.get("source") or "").strip(),
        "sourceLabel": str(record.get("sourceLabel") or "").strip(),
        "contentPath": content_path,
    }


def load_conversation_attachment_records(openclaw_dir, agent_id, session_id="", conversation_key=""):
    manifest_path = _conversation_attachment_manifest_path(openclaw_dir, agent_id, session_id, conversation_key)
    payload = load_json(manifest_path, {})
    attachments = []
    for item in safe_list((payload or {}).get("attachments")):
        normalized = _normalize_conversation_attachment_record(item)
        if not normalized:
            continue
        if not Path(normalized["contentPath"]).exists():
            continue
        attachments.append(normalized)
    return attachments


def save_conversation_attachment_records(openclaw_dir, agent_id, session_id="", conversation_key="", attachments=None):
    manifest_path = _conversation_attachment_manifest_path(openclaw_dir, agent_id, session_id, conversation_key)
    normalized_key = normalize_conversation_session_key(agent_id, session_id, conversation_key)
    payload = {
        "agentId": str(agent_id or "").strip(),
        "conversationKey": normalized_key,
        "attachments": [item for item in (
            _normalize_conversation_attachment_record(entry) for entry in safe_list(attachments)
        ) if item],
        "updatedAt": now_iso(),
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id="", conversation_key=""):
    status_path = _conversation_voice_reply_status_path(openclaw_dir, agent_id, session_id, conversation_key)
    payload = load_json(status_path, {})
    statuses = {}
    for item_id, item in ((payload.get("items") or {}) if isinstance(payload, dict) else {}).items():
        normalized_item_id = str(item_id or "").strip()
        current = item if isinstance(item, dict) else {}
        if not normalized_item_id:
            continue
        statuses[normalized_item_id] = {
            "pending": bool(current.get("pending")),
            "failed": bool(current.get("failed")),
            "updatedAt": str(current.get("updatedAt") or "").strip(),
        }
    return statuses


def save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id="", conversation_key="", statuses=None):
    status_path = _conversation_voice_reply_status_path(openclaw_dir, agent_id, session_id, conversation_key)
    normalized_key = normalize_conversation_session_key(agent_id, session_id, conversation_key)
    items = {}
    for item_id, item in ((statuses or {}) if isinstance(statuses, dict) else {}).items():
        normalized_item_id = str(item_id or "").strip()
        current = item if isinstance(item, dict) else {}
        if not normalized_item_id:
            continue
        if not current.get("pending") and not current.get("failed"):
            continue
        items[normalized_item_id] = {
            "pending": bool(current.get("pending")),
            "failed": bool(current.get("failed")),
            "updatedAt": str(current.get("updatedAt") or now_iso()).strip(),
        }
    payload = {
        "agentId": str(agent_id or "").strip(),
        "conversationKey": normalized_key,
        "items": items,
        "updatedAt": now_iso(),
    }
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


def conversation_attachment_content(openclaw_dir, agent_id, attachment_id, session_id="", conversation_key=""):
    normalized_attachment_id = str(attachment_id or "").strip()
    if not normalized_attachment_id:
        return None
    for item in load_conversation_attachment_records(openclaw_dir, agent_id, session_id, conversation_key):
        if item.get("id") != normalized_attachment_id:
            continue
        content_path = Path(item.get("contentPath") or "")
        if not content_path.exists():
            return None
        return {
            **item,
            "path": str(content_path),
            "bytes": content_path.read_bytes(),
        }
    return None


def build_conversation_attachment_public_payload(agent_id, session_id="", conversation_key="", attachment=None):
    attachment = _normalize_conversation_attachment_record(attachment)
    if not attachment:
        return {}
    normalized_agent_id = str(agent_id or "").strip()
    normalized_key = normalize_conversation_session_key(agent_id, session_id, conversation_key)
    display_session_id = conversation_display_session_id(agent_id, normalized_key)
    query = (
        f"agentId={normalized_agent_id}"
        f"&sessionId={display_session_id}"
        f"&conversationKey={normalized_key}"
        f"&attachmentId={attachment['id']}"
    )
    download_url = f"/api/conversations/attachment/content?{query}"
    return {
        "id": attachment["id"],
        "kind": attachment["kind"],
        "name": attachment["name"],
        "size": attachment["size"],
        "type": attachment["mimeType"],
        "createdAt": attachment["createdAt"],
        "source": attachment["source"],
        "sourceLabel": attachment["sourceLabel"],
        "url": download_url,
        "downloadUrl": download_url,
    }


def merge_conversation_attachment_payloads(openclaw_dir, agent_id, session_id="", conversation_key="", transcript_items=None):
    attachment_records = load_conversation_attachment_records(openclaw_dir, agent_id, session_id, conversation_key)
    voice_reply_statuses = load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key)
    if not attachment_records and not voice_reply_statuses:
        return [dict(item) for item in safe_list(transcript_items)]
    attachments_by_item = defaultdict(list)
    for record in attachment_records:
        attachments_by_item[str(record.get("itemId") or "").strip()].append(
            build_conversation_attachment_public_payload(
                agent_id,
                session_id,
                conversation_key,
                attachment=record,
            )
        )
    merged = []
    for item in safe_list(transcript_items):
        current = dict(item) if isinstance(item, dict) else {}
        item_id = str(current.get("id") or "").strip()
        existing = [entry for entry in safe_list(current.get("attachments")) if isinstance(entry, dict)]
        extra = attachments_by_item.get(item_id, [])
        if extra:
            seen = {
                str(entry.get("id") or "").strip()
                for entry in existing
                if str(entry.get("id") or "").strip()
            }
            merged_attachments = list(existing)
            for entry in extra:
                entry_id = str(entry.get("id") or "").strip()
                if entry_id and entry_id in seen:
                    continue
                if entry_id:
                    seen.add(entry_id)
                merged_attachments.append(entry)
            current["attachments"] = merged_attachments
        merged_attachments = [entry for entry in safe_list(current.get("attachments")) if isinstance(entry, dict)]
        has_audio_attachment = any(
            str(entry.get("type") or entry.get("mimeType") or "").strip().lower().startswith("audio/")
            or str(entry.get("kind") or "").strip().lower() == "audio"
            for entry in merged_attachments
        )
        status = voice_reply_statuses.get(item_id) if item_id else None
        if status and not has_audio_attachment:
            current_meta = current.get("meta") if isinstance(current.get("meta"), dict) else {}
            current["meta"] = {
                **current_meta,
                "voiceReplyPending": bool(status.get("pending")),
                "voiceReplyFailed": bool(status.get("failed")),
            }
        merged.append(current)
    return merged


def latest_assistant_conversation_item(conversation):
    for item in reversed(safe_list((conversation or {}).get("items"))):
        if str((item or {}).get("kind") or "").strip() != "assistant":
            continue
        if str((item or {}).get("text") or "").strip():
            return item
    return None


def _complete_conversation_voice_reply_attachment(openclaw_dir, agent_id, session_id="", conversation_key="", item_id=""):
    job_key = _conversation_voice_reply_job_key(agent_id, session_id, conversation_key, item_id)
    try:
        current_conversation = load_conversation_transcript(openclaw_dir, agent_id, session_id, conversation_key)
        latest_item = latest_assistant_conversation_item(current_conversation)
        if not isinstance(latest_item, dict):
            return
        latest_item_id = str(latest_item.get("id") or "").strip()
        if item_id and latest_item_id != str(item_id or "").strip():
            target_item = next(
                (
                    dict(item)
                    for item in safe_list((current_conversation or {}).get("items"))
                    if isinstance(item, dict) and str(item.get("id") or "").strip() == str(item_id or "").strip()
                ),
                None,
            )
            if isinstance(target_item, dict):
                latest_item = target_item
                latest_item_id = str(target_item.get("id") or "").strip()
        if not latest_item_id:
            return
        status_map = load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key)
        existing_records = load_conversation_attachment_records(openclaw_dir, agent_id, session_id, conversation_key)
        if any(
            str(record.get("itemId") or "").strip() == latest_item_id
            and str(record.get("kind") or "").strip() == "audio"
            and str(record.get("source") or "").strip() == "member_voice_reply"
            for record in existing_records
        ):
            status_map.pop(latest_item_id, None)
            save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
            return
        reply_text = str(latest_item.get("text") or "").strip()
        if not reply_text:
            status_map[latest_item_id] = {"pending": False, "failed": True, "updatedAt": now_iso()}
            save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
            return
        try:
            audio_path = Path(generate_customer_voice_reply_audio(openclaw_dir, reply_text, agent_id=agent_id))
        except Exception:
            status_map[latest_item_id] = {"pending": False, "failed": True, "updatedAt": now_iso()}
            save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
            return
        lane_dir = _conversation_attachment_lane_dir(openclaw_dir, agent_id, session_id, conversation_key)
        files_dir = lane_dir / "files"
        files_dir.mkdir(parents=True, exist_ok=True)
        attachment_id = f"catt_{secrets.token_hex(8)}"
        suffix = audio_path.suffix or ".mp3"
        target_path = files_dir / f"{attachment_id}{suffix}"
        target_path.write_bytes(audio_path.read_bytes())
        audio_path.unlink(missing_ok=True)
        mime_type = mimetypes.guess_type(str(target_path))[0] or "audio/mpeg"
        existing_records.append(
            {
                "id": attachment_id,
                "itemId": latest_item_id,
                "kind": "audio",
                "name": f"{str(agent_id or 'agent').strip() or 'agent'}-voice-reply{suffix}",
                "mimeType": mime_type,
                "size": target_path.stat().st_size,
                "createdAt": now_iso(),
                "source": "member_voice_reply",
                "sourceLabel": "语音回复",
                "contentPath": str(target_path),
            }
        )
        save_conversation_attachment_records(
            openclaw_dir,
            agent_id,
            session_id,
            conversation_key,
            attachments=existing_records,
        )
        status_map.pop(latest_item_id, None)
        save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
    finally:
        _finish_conversation_voice_reply_job(job_key)


def _conversation_voice_reply_target_item(conversation, request_text="", item_id=""):
    current_conversation = conversation if isinstance(conversation, dict) else {}
    normalized_item_id = str(item_id or "").strip()
    items = [item for item in safe_list(current_conversation.get("items")) if isinstance(item, dict)]
    if normalized_item_id:
        explicit = next((dict(item) for item in items if str(item.get("id") or "").strip() == normalized_item_id), None)
        if isinstance(explicit, dict):
            return explicit
    normalized_request = str(request_text or "").strip()
    target_user_index = -1
    if normalized_request:
        for index in range(len(items) - 1, -1, -1):
            item = items[index]
            if str(item.get("kind") or "").strip() != "user":
                continue
            text = str(item.get("text") or "").strip()
            if normalized_request and normalized_request in text:
                target_user_index = index
                break
        if target_user_index < 0:
            for index in range(len(items) - 1, -1, -1):
                item = items[index]
                if str(item.get("kind") or "").strip() != "user":
                    continue
                if conversation_message_requests_voice_reply(item.get("text") or ""):
                    target_user_index = index
                    break
        if target_user_index < 0:
            for index in range(len(items) - 1, -1, -1):
                item = items[index]
                if str(item.get("kind") or "").strip() == "user":
                    target_user_index = index
                    break
    if target_user_index >= 0:
        for item in items[target_user_index + 1 :]:
            if str(item.get("kind") or "").strip() == "assistant" and str(item.get("text") or "").strip():
                return dict(item)
        return None
    return latest_assistant_conversation_item(current_conversation)


def _await_conversation_voice_reply_item(
    openclaw_dir,
    agent_id,
    session_id="",
    conversation_key="",
    request_text="",
    timeout_seconds=30.0,
    poll_interval=0.5,
):
    wait_job_key = _conversation_voice_reply_job_key(agent_id, session_id, conversation_key, "__await__")
    try:
        deadline = time.time() + max(1.0, float(timeout_seconds or 0.0))
        while time.time() < deadline:
            current_conversation = load_conversation_transcript(openclaw_dir, agent_id, session_id, conversation_key)
            latest_item = _conversation_voice_reply_target_item(current_conversation, request_text=request_text)
            if not isinstance(latest_item, dict):
                time.sleep(max(0.05, float(poll_interval or 0.0)))
                continue
            item_id = str(latest_item.get("id") or "").strip()
            if not item_id:
                return
            existing_records = load_conversation_attachment_records(openclaw_dir, agent_id, session_id, conversation_key)
            if any(
                str(record.get("itemId") or "").strip() == item_id
                and str(record.get("kind") or "").strip() == "audio"
                and str(record.get("source") or "").strip() == "member_voice_reply"
                for record in existing_records
            ):
                status_map = load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key)
                status_map.pop(item_id, None)
                save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
                return
            reply_text = str(latest_item.get("text") or "").strip()
            status_map = load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key)
            if not reply_text:
                status_map[item_id] = {"pending": False, "failed": True, "updatedAt": now_iso()}
                save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
                return
            status_map[item_id] = {"pending": True, "failed": False, "updatedAt": now_iso()}
            save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
            item_job_key = _conversation_voice_reply_job_key(agent_id, session_id, conversation_key, item_id)
            if _start_conversation_voice_reply_job(item_job_key):
                _complete_conversation_voice_reply_attachment(openclaw_dir, agent_id, session_id, conversation_key, item_id)
            return
    finally:
        _finish_conversation_voice_reply_job(wait_job_key)


def ensure_conversation_voice_reply_attachment(openclaw_dir, agent_id, session_id="", conversation_key="", request_text="", conversation=None):
    if not conversation_message_requests_voice_reply(request_text):
        return conversation
    runtime_profile = agent_runtime_profile_payload(openclaw_dir, agent_id)
    if "speech" not in clean_unique_strings(runtime_profile.get("skills") or []):
        return conversation
    current_conversation = conversation if isinstance(conversation, dict) else load_conversation_transcript(
        openclaw_dir,
        agent_id,
        session_id,
        conversation_key,
    )
    latest_item = _conversation_voice_reply_target_item(current_conversation, request_text=request_text)
    if not isinstance(latest_item, dict):
        # The response payload can arrive ahead of transcript persistence.
        # Retry briefly in-band, then fall back to a background watcher so the
        # send response stays fast while the voice attachment catches up.
        for attempt in range(5):
            reloaded_conversation = load_conversation_transcript(
                openclaw_dir,
                agent_id,
                session_id,
                conversation_key,
            )
            if isinstance(reloaded_conversation, dict):
                current_conversation = reloaded_conversation
                latest_item = _conversation_voice_reply_target_item(current_conversation, request_text=request_text)
                if isinstance(latest_item, dict):
                    break
            if attempt < 4:
                time.sleep(0.2)
    if not isinstance(latest_item, dict):
        wait_job_key = _conversation_voice_reply_job_key(agent_id, session_id, conversation_key, "__await__")
        if _start_conversation_voice_reply_job(wait_job_key):
            worker = threading.Thread(
                target=_await_conversation_voice_reply_item,
                args=(openclaw_dir, agent_id, session_id, conversation_key, request_text),
                daemon=True,
                name=f"conv-voice-await-{str(agent_id or 'agent')[:10]}",
            )
            worker.start()
        return current_conversation
    item_id = str(latest_item.get("id") or "").strip()
    if not item_id:
        return current_conversation
    existing_records = load_conversation_attachment_records(openclaw_dir, agent_id, session_id, conversation_key)
    if any(
        str(record.get("itemId") or "").strip() == item_id
        and str(record.get("kind") or "").strip() == "audio"
        and str(record.get("source") or "").strip() == "member_voice_reply"
        for record in existing_records
    ):
        status_map = load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key)
        if item_id in status_map:
            status_map.pop(item_id, None)
            save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
        if isinstance(current_conversation, dict):
            return {
                **current_conversation,
                "items": merge_conversation_attachment_payloads(
                    openclaw_dir,
                    agent_id,
                    session_id,
                    conversation_key,
                    current_conversation.get("items"),
                ),
            }
        return current_conversation
    reply_text = str(latest_item.get("text") or "").strip()
    if not reply_text:
        return current_conversation
    status_map = load_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key)
    status_map[item_id] = {"pending": True, "failed": False, "updatedAt": now_iso()}
    save_conversation_voice_reply_statuses(openclaw_dir, agent_id, session_id, conversation_key, status_map)
    job_key = _conversation_voice_reply_job_key(agent_id, session_id, conversation_key, item_id)
    if _start_conversation_voice_reply_job(job_key):
        worker = threading.Thread(
            target=_complete_conversation_voice_reply_attachment,
            args=(openclaw_dir, agent_id, session_id, conversation_key, item_id),
            daemon=True,
            name=f"conv-voice-{str(agent_id or 'agent')[:10]}",
        )
        worker.start()
    refreshed = load_conversation_transcript(openclaw_dir, agent_id, session_id, conversation_key)
    return refreshed if isinstance(refreshed, dict) else current_conversation


def build_conversation_context_highlights(messages, limit=CONVERSATION_CONTEXT_COMPRESSION_MAX_HIGHLIGHTS):
    relevant_messages = [
        item
        for item in safe_list(messages)
        if isinstance(item, dict)
        and str(item.get("kind") or "").strip() in {"user", "assistant"}
        and str(item.get("text") or "").strip()
    ]
    if not relevant_messages:
        return []
    highlights = []
    sampled = relevant_messages[-max(1, int(limit or 1)) :]
    for item in sampled:
        kind = str(item.get("kind") or "").strip()
        label = "User" if kind == "user" else "Assistant"
        text = summarize_task_execution_text(item.get("text") or "", limit=96)
        if not text:
            continue
        highlights.append(f"{label}: {text}")
    return highlights[: max(1, int(limit or 1))]


def render_conversation_context_summary(highlights):
    normalized = [str(item or "").strip() for item in safe_list(highlights) if str(item or "").strip()]
    if not normalized:
        return ""
    return "\n".join(f"- {item}" for item in normalized)


def build_conversation_recent_exchange_lines(messages, limit=CONVERSATION_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES):
    relevant_messages = [
        item
        for item in safe_list(messages)
        if isinstance(item, dict)
        and str(item.get("kind") or "").strip() in {"user", "assistant"}
        and str(item.get("text") or "").strip()
    ]
    recent_items = relevant_messages[-max(1, int(limit or 1)) :]
    lines = []
    for item in recent_items:
        kind = str(item.get("kind") or "").strip()
        label = "User" if kind == "user" else "Assistant"
        text = summarize_task_execution_text(item.get("text") or "", limit=120)
        if not text:
            continue
        lines.append(f"- {label}: {text}")
    return lines


def build_conversation_compressed_dispatch_message(summary_text, recent_lines, current_message):
    sections = [
        "Continue this direct conversation with the same tone and objective.",
    ]
    if summary_text:
        sections.append("Carry-forward summary\n" + summary_text)
    if recent_lines:
        sections.append("Recent exchange\n" + "\n".join(recent_lines))
    sections.append("New user message\n" + str(current_message or "").strip())
    return "\n\n".join(section for section in sections if str(section or "").strip())


def maybe_prepare_conversation_context_compression(openclaw_dir, agent_id, session_id, message_text, conversation_key=""):
    explicit_conversation_key = str(conversation_key or "").strip()
    normalized_conversation_key = normalize_conversation_session_key(agent_id, session_id, explicit_conversation_key)
    if explicit_conversation_key:
        route_session_key = conversation_route_session_key(normalized_conversation_key)
        transcript_session_ref = route_session_key if route_session_key == "main" else normalized_conversation_key
    else:
        raw_session_ref = str(session_id or "").strip() or "main"
        route_session_key = raw_session_ref
        transcript_session_ref = raw_session_ref
    if not agent_id or not transcript_session_ref:
        return {"applied": False}
    if route_session_key != "main" and is_reserved_conversation_session(openclaw_dir, agent_id, transcript_session_ref):
        return {"applied": False}
    transcript_path = session_transcript_path(openclaw_dir, agent_id, transcript_session_ref)
    if not transcript_path:
        return {"applied": False}
    transcript = parse_transcript_items(transcript_path, limit=320)
    relevant_messages = [
        item
        for item in safe_list((transcript or {}).get("items"))
        if isinstance(item, dict)
        and str(item.get("kind") or "").strip() in {"user", "assistant"}
        and str(item.get("text") or "").strip()
    ]
    if not relevant_messages:
        return {"applied": False}
    total_characters = sum(len(str(item.get("text") or "").strip()) for item in relevant_messages)
    if (
        len(relevant_messages) < CONVERSATION_CONTEXT_COMPRESSION_MESSAGE_THRESHOLD
        and total_characters < CONVERSATION_CONTEXT_COMPRESSION_CHARACTER_THRESHOLD
    ):
        return {"applied": False}
    if len(relevant_messages) <= CONVERSATION_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES:
        return {"applied": False}
    older_messages = relevant_messages[:-CONVERSATION_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES]
    recent_messages = relevant_messages[-CONVERSATION_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES :]
    highlights = build_conversation_context_highlights(older_messages)
    summary_text = render_conversation_context_summary(highlights)
    if not summary_text:
        return {"applied": False}
    recent_lines = build_conversation_recent_exchange_lines(recent_messages)
    return {
        "applied": True,
        "sessionId": conversation_display_session_id(agent_id, normalized_conversation_key if explicit_conversation_key else transcript_session_ref),
        "sessionKey": route_session_key,
        "conversationKey": normalized_conversation_key,
        "message": build_conversation_compressed_dispatch_message(summary_text, recent_lines, message_text),
        "meta": {
            "applied": True,
            "reason": "large_conversation_context",
            "previousSessionId": conversation_display_session_id(agent_id, normalized_conversation_key if explicit_conversation_key else transcript_session_ref),
            "sessionId": conversation_display_session_id(agent_id, normalized_conversation_key if explicit_conversation_key else transcript_session_ref),
            "sessionKey": route_session_key,
            "conversationKey": normalized_conversation_key,
            "summary": summary_text,
            "highlights": highlights,
            "recentExchange": recent_lines,
            "compressedMessageCount": max(0, len(older_messages)),
            "rotationCount": 0,
            "triggerMessageCount": len(relevant_messages),
            "triggerCharacterCount": total_characters,
            "keepRecentMessages": CONVERSATION_CONTEXT_COMPRESSION_KEEP_RECENT_MESSAGES,
            "lastCompressedAt": now_iso(),
        },
    }


def normalize_chat_dispatch_mode(value, has_team=False):
    mode = str(value or "").strip().lower()
    if mode in CHAT_THREAD_DISPATCH_MODES:
        return mode
    return "direct"


def detect_chat_broadcast_intent_tokens(text):
    normalized_text = str(text or "").strip().lower()
    if not normalized_text:
        return []
    hits = []
    for token in CHAT_BROADCAST_INTENT_TOKENS:
        normalized_token = str(token or "").strip().lower()
        if normalized_token and normalized_token in normalized_text and normalized_token not in hits:
            hits.append(normalized_token)
    return hits[:6]


def detect_chat_strict_all_hands_reply_tokens(text):
    normalized_text = str(text or "").strip().lower()
    if not normalized_text:
        return []
    tokens = (
        "全员语音报道",
        "全员报道",
        "全员语音回复",
        "全员逐个回复",
        "全员逐个报到",
        "所有人逐个报到",
        "所有人都回复",
        "每个人都报到",
        "每个人都回复",
        "逐个报到",
    )
    hits = []
    for token in tokens:
        normalized_token = str(token or "").strip().lower()
        if normalized_token and normalized_token in normalized_text and normalized_token not in hits:
            hits.append(normalized_token)
    return hits[:6]


def normalize_chat_thread_linked_team_ids(team_id="", linked_team_ids=None, collaborator_team_ids=None):
    normalized_primary = str(team_id or "").strip()
    collected = []
    for candidate in [normalized_primary, *safe_list(linked_team_ids), *safe_list(collaborator_team_ids)]:
        value = str(candidate or "").strip()
        if value and value not in collected:
            collected.append(value)
    return collected


def linked_team_ids_for_chat_thread(thread_or_meta):
    source = thread_or_meta if isinstance(thread_or_meta, dict) else {}
    meta = source.get("meta") if isinstance(source.get("meta"), dict) else source
    team_id = str(meta.get("teamId") or source.get("teamId") or "").strip()
    return normalize_chat_thread_linked_team_ids(team_id, meta.get("linkedTeamIds"))


def compact_chat_thread_team_references(team_map, linked_team_ids):
    refs = []
    for team_id in normalize_chat_thread_linked_team_ids(linked_team_ids=linked_team_ids):
        team_ref = compact_chat_thread_team_reference(team_map.get(team_id))
        if team_ref.get("id"):
            refs.append(team_ref)
    return refs


def resolve_chat_dispatch_targets(
    thread,
    target_agent_id="",
    mention_agent_ids=None,
    dispatch_mode="",
    dispatch_explicit=False,
    message_text="",
):
    meta = thread.get("meta") if isinstance(thread.get("meta"), dict) else {}
    team_id = str(meta.get("teamId") or "").strip()
    linked_team_ids = linked_team_ids_for_chat_thread(meta)
    team_policy = meta.get("teamPolicy") if isinstance(meta.get("teamPolicy"), dict) else {}
    participant_agent_ids = []
    for item in safe_list(thread.get("participantAgentIds")):
        agent_id = str(item or "").strip()
        if agent_id and agent_id not in participant_agent_ids:
            participant_agent_ids.append(agent_id)
    requested_target_agent_id = str(target_agent_id or "").strip()
    current_target_agent_id = (
        requested_target_agent_id
        or str(thread.get("currentTargetAgentId") or "").strip()
        or str(thread.get("primaryAgentId") or "").strip()
    )
    if current_target_agent_id and current_target_agent_id not in participant_agent_ids:
        participant_agent_ids.append(current_target_agent_id)
    mention_agent_ids = safe_chat_mentions(mention_agent_ids)
    for agent_id in mention_agent_ids:
        if agent_id not in participant_agent_ids:
            participant_agent_ids.append(agent_id)
    resolved_mode = normalize_chat_dispatch_mode(
        dispatch_mode or meta.get("dispatchMode") or team_policy.get("defaultDispatchMode"),
        has_team=bool(team_id or linked_team_ids),
    )
    requested_mode = str(dispatch_mode or "").strip().lower()
    dispatch_was_explicit = bool(dispatch_explicit or meta.get("dispatchModeExplicit"))
    broadcast_intent_hits = detect_chat_broadcast_intent_tokens(message_text)
    strict_all_hands_reply_hits = detect_chat_strict_all_hands_reply_tokens(message_text)
    if (
        resolved_mode != "broadcast"
        and not dispatch_was_explicit
        and not mention_agent_ids
        and (broadcast_intent_hits or strict_all_hands_reply_hits)
        and (team_id or linked_team_ids)
        and len(participant_agent_ids) > 1
    ):
        resolved_mode = "broadcast"
    if (
        resolved_mode == "broadcast"
        and not mention_agent_ids
        and not dispatch_was_explicit
        and not detect_all_hands_task_tokens(message_text)
        and not broadcast_intent_hits
        and not strict_all_hands_reply_hits
        and (team_id or linked_team_ids)
        and current_target_agent_id
    ):
        resolved_mode = "direct"
    if resolved_mode == "broadcast":
        dispatch_agent_ids = list(participant_agent_ids)
    elif resolved_mode == "mentions":
        dispatch_agent_ids = [agent_id for agent_id in mention_agent_ids if agent_id in participant_agent_ids]
        if not dispatch_agent_ids and current_target_agent_id:
            dispatch_agent_ids = [current_target_agent_id]
    else:
        dispatch_agent_ids = [current_target_agent_id] if current_target_agent_id else []
    if not dispatch_agent_ids and participant_agent_ids:
        dispatch_agent_ids = participant_agent_ids[:1]
    return {
        "dispatchMode": resolved_mode,
        "requestedDispatchMode": requested_mode or resolved_mode,
        "dispatchModeExplicit": dispatch_was_explicit,
        "broadcastIntentHits": broadcast_intent_hits,
        "strictAllHandsReplyHits": strict_all_hands_reply_hits,
        "strictAllHandsReplies": bool(strict_all_hands_reply_hits),
        "participantAgentIds": participant_agent_ids,
        "currentTargetAgentId": current_target_agent_id,
        "dispatchAgentIds": dispatch_agent_ids,
    }


def summarize_chat_dispatch_result(dispatch_mode, successes, failures):
    success_count = len(successes)
    failure_count = len(failures)
    if success_count == 1 and successes[0].get("replyPreview"):
        return successes[0]["replyPreview"][:160]
    if success_count > 1:
        if dispatch_mode == "broadcast":
            return f"团队广播已收到 {success_count} 位成员回复。"
        if dispatch_mode == "mentions":
            return f"被点名成员已收到 {success_count} 条回复。"
        return f"当前线程已收到 {success_count} 条回复。"
    if failure_count:
        return "消息已写入团队线程，但暂未收到成员回包。"
    return "消息已发送。"


def format_attachment_size(size):
    value = max(0, int(size or 0))
    if value >= 1024 * 1024:
        return f"{value / (1024 * 1024):.1f} MB"
    if value >= 1024:
        return f"{round(value / 1024)} KB"
    return f"{value} B"


def merge_chat_message_with_attachments(
    message_text,
    attachments,
    mention_agent_ids=None,
    team_policy=None,
    reply_context=None,
):
    message_text = str(message_text or "").strip()
    attachments = safe_chat_attachments(attachments)
    mention_agent_ids = safe_chat_mentions(mention_agent_ids)
    reply_context = safe_chat_reply_context(reply_context)
    team_policy = team_policy if isinstance(team_policy, dict) else {}
    blocks = []
    operating_brief = summarize_task_execution_text(team_policy.get("operatingBrief") or "", limit=140)
    team_memory = summarize_task_execution_text(team_policy.get("teamMemory") or "", limit=140)
    working_memory = summarize_task_execution_text(team_policy.get("workingMemory") or "", limit=120)
    decision_log = summarize_task_execution_text(team_policy.get("decisionLog") or "", limit=140)
    current_focus = summarize_task_execution_text(team_policy.get("currentFocus") or "", limit=120)
    open_loops = [
        summarize_task_execution_text(item, limit=88)
        for item in normalize_team_context_lines(team_policy.get("openLoops"), limit=2)
    ]
    open_loops = [item for item in open_loops if item]
    task_long_term_memory = compact_task_long_term_memory(team_policy.get("taskLongTermMemory"))
    protocol = team_policy.get("coordinationProtocol") if isinstance(team_policy.get("coordinationProtocol"), dict) else {}
    human_tone = summarize_task_execution_text(team_policy.get("humanToneGuide") or protocol.get("humanToneGuide") or "", limit=120)
    proactive_rules = clean_unique_strings(team_policy.get("proactiveRules") or protocol.get("proactiveRules") or [])
    update_contract = summarize_task_execution_text(protocol.get("updateContract") or "", limit=120)
    reply_contract_lines = [
        "Reply with the direct answer or judgment first.",
        "Keep the visible reply short and conversational unless the user clearly asks for depth.",
        "Default to 2-6 concise sentences or a very short list when it helps clarity.",
        "If there is a best path, say it directly instead of listing every possibility first.",
        "Use remembered context implicitly. Do not re-list stored memory, old decisions, or long background unless it changes the current answer.",
        "If you need to mention the past, fold it into one short natural sentence instead of announcing 'memory', 'history', or 'context'.",
        "If the user asks how memory works here, explain the product memory system and projected task/team context; do not say it only relies on MEMORY.md or memory/*.md.",
        "If another teammate should step in, you can pull them into the thread or say who should take over this part.",
        "Do not say you lack permission to involve teammates unless the system explicitly returned a real permission error.",
        "If browser, canvas, or runtime capability errors show up, first try a reasonable fallback path or explain the smallest workable workaround; do not dump raw environment errors into the visible reply.",
        "Do not end with 'not my problem', 'environment limitation', or tell the user to handle DevOps/browser setup themselves unless you already tried the available fallback path and clearly say what was tried.",
        "Avoid role labels, status labels, coordination tags, or system-style formatting in the visible reply.",
    ]
    if operating_brief:
        blocks.append(f"Team brief\n{operating_brief}")
    if team_memory:
        blocks.append(f"Team memory\n{team_memory}")
    if current_focus:
        blocks.append(f"Current focus\n{current_focus}")
    if open_loops:
        blocks.append("Open loops\n" + "\n".join(f"- {item}" for item in open_loops))
    if working_memory:
        blocks.append(f"Recent team memory\n{working_memory}")
    if task_long_term_memory.get("longTermMemory"):
        blocks.append(f"Task long-term memory\n{summarize_task_execution_text(task_long_term_memory.get('longTermMemory'), limit=140)}")
    if task_long_term_memory.get("learningHighlights"):
        learning_items = [
            summarize_task_execution_text(item, limit=88)
            for item in safe_list(task_long_term_memory.get("learningHighlights"))[:1]
        ]
        learning_items = [item for item in learning_items if item]
        if learning_items:
            blocks.append("Task learnings\n" + "\n".join(f"- {item}" for item in learning_items))
    if decision_log:
        blocks.append(f"Decision log\n{decision_log}")
    if human_tone:
        blocks.append(f"Communication style\n{human_tone}")
    if proactive_rules:
        blocks.append("Proactive coordination\n" + "\n".join(f"- {item}" for item in proactive_rules))
    if update_contract:
        blocks.append(f"Update contract\n{update_contract}")
    has_contextual_scaffolding = bool(
        operating_brief
        or team_memory
        or current_focus
        or open_loops
        or working_memory
        or task_long_term_memory.get("longTermMemory")
        or task_long_term_memory.get("learningHighlights")
        or decision_log
        or human_tone
        or proactive_rules
        or update_contract
        or attachments
        or mention_agent_ids
        or reply_context
    )
    if has_contextual_scaffolding:
        blocks.append("Reply contract\n" + "\n".join(f"- {item}" for item in reply_contract_lines))
    if mention_agent_ids:
        blocks.append("Mentioned members\n" + "\n".join(f"- @{item}" for item in mention_agent_ids))
    if reply_context:
        reply_sender = str(reply_context.get("sender") or "Earlier message").strip() or "Earlier message"
        reply_text = str(reply_context.get("text") or "").strip()
        reply_lines = [f"Replying to\n- {reply_sender}"]
        if reply_text:
            reply_lines.append(f"- Context: {reply_text}")
        blocks.append("\n".join(reply_lines))
    if attachments:
        lines = []
        for item in attachments:
            meta = item["name"]
            size_text = format_attachment_size(item.get("size", 0))
            if size_text:
                meta = f"{meta} ({size_text}"
                if item.get("type"):
                    meta = f"{meta}, {item['type']}"
                meta = f"{meta})"
            elif item.get("type"):
                meta = f"{meta} ({item['type']})"
            if item.get("preview"):
                lines.append(f"- {meta}\n  {item['preview']}")
            else:
                lines.append(f"- {meta}")
        blocks.append("Attachments\n" + "\n".join(lines))
    if not blocks:
        return message_text
    if message_text:
        return f"{message_text}\n\n" + "\n\n".join(blocks)
    return "\n\n".join(blocks)


def perform_chat_thread_send(
    openclaw_dir,
    thread_id,
    message_text="",
    actor=None,
    target_agent_id="",
    workspace_path="",
    workspace_authorized=False,
    attachments=None,
    mention_agent_ids=None,
    reply_context=None,
    selected_skill_slugs=None,
    dispatch_mode="",
    dispatch_mode_explicit=False,
    thinking="low",
    output_dir="",
    server=None,
):
    normalized_thread_id = str(thread_id or "").strip()
    if not normalized_thread_id:
        raise RuntimeError("需要 threadId，且至少要发送文字或附件。")
    thread = store_get_chat_thread(openclaw_dir, normalized_thread_id)
    if not thread:
        raise RuntimeError("聊天线程不存在。")
    meta = deepcopy(thread.get("meta", {})) if isinstance(thread.get("meta"), dict) else {}
    resolved_workspace_path = str(workspace_path or meta.get("workspacePath") or "").strip()
    resolved_workspace_authorized = bool(workspace_authorized or meta.get("workspaceAuthorized"))
    if resolved_workspace_path:
        meta["workspacePath"] = resolved_workspace_path
    else:
        meta.pop("workspacePath", None)
    if resolved_workspace_authorized and resolved_workspace_path:
        meta["workspaceAuthorized"] = True
    else:
        meta.pop("workspaceAuthorized", None)
    team_policy = meta.get("teamPolicy") if isinstance(meta.get("teamPolicy"), dict) else {}
    prepared = prepare_chat_send_request(
        message_text,
        attachments=attachments,
        selected_skill_slugs=selected_skill_slugs,
        mention_agent_ids=mention_agent_ids,
        reply_context=reply_context,
        team_policy=team_policy,
        thinking=thinking,
    )
    if not prepared["messageText"] and not prepared["attachments"]:
        raise RuntimeError("需要 threadId，且至少要发送文字或附件。")
    thread_status_val = str(thread.get("status") or "open").strip().lower()
    if thread_status_val in ("archived", "closed"):
        raise RuntimeError(f"聊天线程已{thread_status_val}，无法继续发送消息。")
    dispatch = resolve_chat_dispatch_targets(
        thread,
        target_agent_id=str(target_agent_id or "").strip(),
        mention_agent_ids=prepared["mentionAgentIds"],
        dispatch_mode=str(dispatch_mode or "").strip(),
        dispatch_explicit=bool(dispatch_mode_explicit),
        message_text=prepared["messageText"],
    )
    resolved_target_agent_id = dispatch.get("currentTargetAgentId")
    if not resolved_target_agent_id:
        raise RuntimeError("当前线程还没有主响应 Agent。")
    dispatch_agent_ids = safe_list(dispatch.get("dispatchAgentIds"))
    if not dispatch_agent_ids:
        raise RuntimeError("当前线程没有可分发的 Team 成员。")
    actor = actor if isinstance(actor, dict) else {}
    participant_agent_ids = safe_list(dispatch.get("participantAgentIds"))
    sessions_by_agent = meta.get("sessionsByAgent") if isinstance(meta.get("sessionsByAgent"), dict) else {}
    reply_to_message_id = str(prepared["replyContext"].get("id") or "").strip()
    reply_sender = str(prepared["replyContext"].get("sender") or "").strip()
    reply_preview = str(prepared["replyContext"].get("text") or "").strip()
    outbound_message = store_save_chat_message(
        openclaw_dir,
        {
            "threadId": normalized_thread_id,
            "senderKind": actor.get("kind", "user"),
            "senderId": actor.get("username", ""),
            "senderLabel": actor.get("displayName", ""),
            "direction": "outbound",
            "body": prepared["messageText"],
            "meta": {
                "targetAgentId": resolved_target_agent_id,
                "dispatchMode": dispatch.get("dispatchMode"),
                "dispatchModeExplicit": bool(dispatch.get("dispatchModeExplicit")),
                "dispatchAgentIds": dispatch_agent_ids,
                "attachments": prepared["attachments"],
                "mentionAgentIds": prepared["mentionAgentIds"],
                "replyToMessageId": reply_to_message_id,
                "acknowledgedAgentLabel": reply_sender,
                "acknowledgedPreview": reply_preview,
                "replyPreview": reply_preview,
                "replyContext": prepared["replyContext"],
                "selectedSkillSlugs": prepared["selectedSkillSlugs"],
                "teamContext": {
                    "hasOperatingBrief": bool(str(team_policy.get("operatingBrief") or "").strip()),
                    "hasTeamMemory": bool(str(team_policy.get("teamMemory") or "").strip()),
                    "hasDecisionLog": bool(str(team_policy.get("decisionLog") or "").strip()),
                },
                "workspacePath": resolved_workspace_path,
                "workspaceAuthorized": bool(resolved_workspace_authorized and resolved_workspace_path),
            },
        },
    )
    # Save thread with workspace path BEFORE dispatching so the worker
    # can read the up-to-date workspacePath from storage.
    updated_thread = store_save_chat_thread(
        openclaw_dir,
        {
            **thread,
            "currentTargetAgentId": resolved_target_agent_id,
            "participantAgentIds": participant_agent_ids,
            "meta": {
                **meta,
                "dispatchMode": dispatch.get("dispatchMode"),
                "dispatchModeExplicit": bool(dispatch.get("dispatchModeExplicit")),
                "sessionsByAgent": sessions_by_agent,
                "workspacePath": resolved_workspace_path,
                "workspaceAuthorized": bool(resolved_workspace_authorized and resolved_workspace_path),
            },
            "workspacePath": resolved_workspace_path,
            "workspaceAuthorized": bool(resolved_workspace_authorized and resolved_workspace_path),
            "status": "waiting_internal",
            "updatedAt": now_iso(),
        },
    )
    dispatch_summary = schedule_chat_thread_dispatch(
        openclaw_dir,
        updated_thread or thread,
        outbound_message,
        dispatch,
        prepared["mergedMessage"],
        sessions_by_agent=sessions_by_agent,
        team_policy=team_policy,
        attachments=prepared["attachments"],
        mention_agent_ids=prepared["mentionAgentIds"],
        thinking=prepared["thinking"],
        output_dir=output_dir,
        server=server,
        selected_skill_slugs=prepared["selectedSkillSlugs"],
    )
    store_save_chat_message(
        openclaw_dir,
        {
            "id": outbound_message.get("id", ""),
            "threadId": normalized_thread_id,
            "senderKind": actor.get("kind", "user"),
            "senderId": actor.get("username", ""),
            "senderLabel": actor.get("displayName", ""),
            "direction": "outbound",
            "body": prepared["messageText"],
            "createdAt": outbound_message.get("createdAt", ""),
            "meta": {
                "targetAgentId": resolved_target_agent_id,
                "dispatchMode": dispatch.get("dispatchMode"),
                "dispatchModeExplicit": bool(dispatch.get("dispatchModeExplicit")),
                "dispatchAgentIds": dispatch_agent_ids,
                "dispatchSummary": dispatch_summary,
                "attachments": prepared["attachments"],
                "mentionAgentIds": prepared["mentionAgentIds"],
                "replyToMessageId": reply_to_message_id,
                "acknowledgedAgentLabel": reply_sender,
                "acknowledgedPreview": reply_preview,
                "replyPreview": reply_preview,
                "replyContext": prepared["replyContext"],
                "selectedSkillSlugs": prepared["selectedSkillSlugs"],
                "workspacePath": resolved_workspace_path,
                "workspaceAuthorized": bool(resolved_workspace_authorized and resolved_workspace_path),
            },
        },
    )
    # Update thread again with dispatch summary
    updated_thread = store_save_chat_thread(
        openclaw_dir,
        {
            **(updated_thread or thread),
            "meta": {
                **meta,
                "dispatchMode": dispatch.get("dispatchMode"),
                "dispatchModeExplicit": bool(dispatch.get("dispatchModeExplicit")),
                "lastDispatch": dispatch_summary,
                "sessionsByAgent": sessions_by_agent,
                "workspacePath": resolved_workspace_path,
                "workspaceAuthorized": bool(resolved_workspace_authorized and resolved_workspace_path),
            },
        },
    )
    if output_dir:
        invalidate_dashboard_bundle_cache(openclaw_dir, output_dir)
    detail = load_chat_thread_detail(openclaw_dir, normalized_thread_id)
    return {
        "message": summarize_chat_dispatch_result(dispatch.get("dispatchMode"), [], []),
        "dispatch": dispatch_summary,
        "thread": detail,
        "threadRecord": updated_thread,
        "outboundMessage": outbound_message,
    }


def score_skill_relevance_for_message(skill, message_text):
    skill = skill if isinstance(skill, dict) else {}
    normalized_text = str(message_text or "").strip().lower()
    if not normalized_text:
        return 0
    slug = str(skill.get("slug") or "").strip().lower()
    hints = CHAT_SKILL_TRIGGER_HINTS.get(slug, ())
    score = 0
    if any(token in normalized_text for token in ("http://", "https://", "www.")):
        if slug == "web-content-fetcher":
            score += 6
        elif slug in {"browse", "playwright"}:
            score += 3
    for hint in hints:
        if hint and hint in normalized_text:
            score += 2
    description = str(skill.get("description") or "").strip().lower()
    display_name = str(skill.get("displayName") or skill.get("name") or "").strip().lower()
    for token in ("wechat", "微信", "公众号", "article", "网页", "文章", "browser", "浏览器"):
        if token in normalized_text and (token in description or token in display_name or token in slug):
            score += 1
    return score


def chat_message_may_need_skill_context(message_text, selected_skill_slugs=None):
    explicit = clean_unique_strings(selected_skill_slugs or [])
    if explicit:
        return True
    normalized_text = str(message_text or "").strip().lower()
    if not normalized_text:
        return False
    if conversation_message_requests_voice_reply(normalized_text):
        return True
    if any(token in normalized_text for token in ("http://", "https://", "www.", "mp.weixin.qq.com")):
        return True
    if len(normalized_text) <= 48:
        return False
    for hints in CHAT_SKILL_TRIGGER_HINTS.values():
        for hint in safe_list(hints):
            token = str(hint or "").strip().lower()
            if token and token in normalized_text:
                return True
    return False


def extract_chat_message_urls(message_text, limit=3):
    text = str(message_text or "").strip()
    if not text:
        return []
    urls = []
    for raw in text.replace("\n", " ").split():
        candidate = str(raw or "").strip().strip("()[]{}<>,，。；;！？!?\"'")
        lowered = candidate.lower()
        if not lowered.startswith(("http://", "https://", "www.")):
            continue
        normalized = candidate if not lowered.startswith("www.") else f"https://{candidate}"
        if normalized not in urls:
            urls.append(normalized)
        if len(urls) >= max(1, int(limit or 1)):
            break
    return urls


def chat_message_requires_forced_skill_usage(message_text, ordered_skill_slugs):
    slugs = {
        str(item or "").strip().lower()
        for item in safe_list(ordered_skill_slugs)
        if str(item or "").strip()
    }
    if "web-content-fetcher" not in slugs:
        return {}
    normalized_text = str(message_text or "").strip().lower()
    if not normalized_text:
        return {}
    if not any(token in normalized_text for token in ("http://", "https://", "www.", "mp.weixin.qq.com", "公众号", "微信文章")):
        return {}
    return {
        "slug": "web-content-fetcher",
        "urls": extract_chat_message_urls(message_text),
    }


def build_agent_skill_context_message(openclaw_dir, agent_id, message_text, config=None, metadata=None, selected_skill_slugs=None):
    text = str(message_text or "").strip()
    if not text or "Available local skills" in text:
        return text
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        return text
    config = config or load_config(openclaw_dir)
    metadata = metadata if isinstance(metadata, dict) else load_project_metadata(openclaw_dir, config=config)
    runtime_profile = _chat_runtime_callable(
        "agent_runtime_profile_payload",
        agent_runtime_profile_payload,
    )(
        openclaw_dir,
        normalized_agent_id,
        config=config,
        metadata=metadata,
    )
    skill_slugs = clean_unique_strings(runtime_profile.get("skills") or [])
    if not skill_slugs:
        return text
    try:
        skills_payload = _chat_runtime_callable(
            "load_skills_catalog",
            load_skills_catalog,
        )(openclaw_dir, config=config)
    except Exception:
        return text
    skill_map = {
        str(item.get("slug") or "").strip(): item
        for item in safe_list((skills_payload or {}).get("skills"))
        if isinstance(item, dict) and str(item.get("slug") or "").strip()
    }
    if not skill_map:
        return text
    explicitly_selected = [
        slug for slug in clean_unique_strings(selected_skill_slugs or [])
        if slug in skill_map and slug in skill_slugs
    ]
    voice_reply_requested = conversation_message_requests_voice_reply(text)
    voice_delivery_contract_text = conversation_voice_delivery_contract()
    voice_delivery_block = (
        voice_delivery_contract_text
        if voice_reply_requested and voice_delivery_contract_text.strip() not in text
        else ""
    )
    relevant_skills = []
    for slug in skill_slugs:
        if voice_reply_requested and slug == "speech":
            continue
        if slug in explicitly_selected:
            continue
        skill = skill_map.get(slug)
        if not skill:
            continue
        score = score_skill_relevance_for_message(skill, text)
        if score <= 0:
            continue
        relevant_skills.append((score, skill))
    if not relevant_skills and not explicitly_selected:
        return f"{text}{voice_delivery_block}" if voice_reply_requested else text
    relevant_skills.sort(
        key=lambda item: (
            -item[0],
            str(item[1].get("displayName") or item[1].get("slug") or "").strip().lower(),
        )
    )
    project_dir = _chat_runtime_callable(
        "resolve_project_dir",
        resolve_project_dir,
    )(openclaw_dir, config=config)
    skill_lines = []
    ordered_skills = [
        skill_map[slug]
        for slug in explicitly_selected
        if slug in skill_map and not (voice_reply_requested and slug == "speech")
    ]
    ordered_skills.extend(skill for _score, skill in relevant_skills[: max(0, 6 - len(ordered_skills))])
    for skill in ordered_skills[:6]:
        slug = str(skill.get("slug") or "").strip()
        display_name = str(skill.get("displayName") or skill.get("name") or slug).strip() or slug
        description = str(skill.get("description") or "").strip()
        note = skill_prompt_usage_note(skill)
        skill_path = resolve_skill_instruction_path(project_dir, skill)
        line = f"- {display_name} (`{slug}`)"
        if description:
            line += f": {description}"
        if note:
            line += f" {note}"
        if skill_path:
            line += f" Skill file: {skill_path}"
        skill_lines.append(line)
    if not skill_lines:
        return text
    ordered_skill_slugs = [
        str(skill.get("slug") or "").strip()
        for skill in ordered_skills[:6]
        if isinstance(skill, dict) and str(skill.get("slug") or "").strip()
    ]
    prefix = (
        "Use the explicitly enabled skills first when they fit this chat.\n"
        if explicitly_selected
        else ""
    )
    forced_skill_usage = chat_message_requires_forced_skill_usage(text, ordered_skill_slugs)
    forced_block = ""
    if forced_skill_usage.get("slug") == "web-content-fetcher":
        url_lines = [
            f"- Fetch this URL first with `web-content-fetcher`: {item}"
            for item in safe_list(forced_skill_usage.get("urls"))
            if str(item or "").strip()
        ]
        if not url_lines:
            url_lines = [
                "- Fetch the article/page URL from the user message first with `web-content-fetcher`.",
            ]
        forced_block = (
            "\n\nRequired skill execution\n"
            + "\n".join(url_lines)
            + "\n- Treat this as a direct webpage/article extraction task, especially for WeChat public account articles."
            + "\n- Do not answer from prior knowledge, generic search, or browser fallback before trying `web-content-fetcher`."
            + "\n- Only fall back to browser/search if `web-content-fetcher` returns blocked, empty, or materially incomplete content."
            + "\n- If fallback is required, say that `web-content-fetcher` was blocked or incomplete and then continue with the fallback result."
        )
    return (
        f"{text}\n\n"
        "Available local skills\n"
        + prefix
        + "\n".join(skill_lines)
        + voice_delivery_block
        + forced_block
        + "\n\n"
        "If one of these matches the request, use it directly instead of saying the skill only works from the product UI."
    )


def refresh_company_auto_operation_thread_policy(openclaw_dir, thread, runtime_item):
    thread = thread if isinstance(thread, dict) else {}
    runtime_item = runtime_item if isinstance(runtime_item, dict) else {}
    thread_id = str(thread.get("id") or "").strip()
    if not thread_id or not runtime_item.get("taskId"):
        return {}
    thread_meta = deepcopy(thread.get("meta", {})) if isinstance(thread.get("meta"), dict) else {}
    team_policy = thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {}
    updated_at = str(runtime_item.get("lastReviewedAt") or now_iso()).strip()
    current_focus = str(runtime_item.get("currentFocus") or "").strip()
    working_memory = str(runtime_item.get("summaryText") or "").strip()
    open_loop_items = [
        {
            "text": str(item.get("text") or "").strip(),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
        }
        for item in safe_list(runtime_item.get("openLoopItems"))
        if str((item or {}).get("text") or "").strip()
    ]
    next_team_policy = {
        **team_policy,
        **({"currentFocus": current_focus, "currentFocusUpdatedAt": updated_at} if current_focus else {}),
        **({"currentFocusItems": [{"text": current_focus, "ownerLabel": ""}]} if current_focus else {}),
        **({"workingMemory": working_memory, "workingMemoryUpdatedAt": updated_at} if working_memory else {}),
        "openLoopItems": open_loop_items,
        "openLoops": [item["text"] for item in open_loop_items],
        "activeOwners": clean_unique_strings(runtime_item.get("activeOwners") or []),
        "companyOperationMemory": {
            "longTermMemory": summarize_task_execution_text(runtime_item.get("longTermMemory") or "", limit=220),
            "learningHighlights": clean_unique_strings(runtime_item.get("learningHighlights") or [])[:4],
            "recentReviewNotes": safe_list(compact_company_auto_operation_runtime(runtime_item).get("recentReviewNotes"))[:3],
            "memoryUpdatedAt": str(runtime_item.get("memoryUpdatedAt") or updated_at).strip(),
        },
    }
    return store_save_chat_thread(
        openclaw_dir,
        {
            **thread,
            "updatedAt": updated_at,
            "meta": {
                **thread_meta,
                "teamPolicy": next_team_policy,
            },
        },
    )


def conversation_source_from_key(session_key):
    normalized_key = str(session_key or "").strip()
    if not normalized_key or normalized_key == "main":
        return "main"
    if normalized_key.startswith("conversation:"):
        parts = normalized_key.split(":")
        return parts[2] if len(parts) > 2 else "main"
    if normalized_key.startswith("agent:"):
        parts = normalized_key.split(":")
        return parts[2] if len(parts) > 2 else "main"
    if normalized_key.startswith("acp:"):
        return "dm"
    parts = normalized_key.split(":")
    return parts[0] if parts else "main"


def conversation_label(session):
    session_key = str(session.get("key", "") or "")
    parts = session_key.split(":")
    source = conversation_source_from_key(session_key)
    agent_id = session.get("agentId", "")
    if source == "main":
        return f"{agent_id} · 主会话"
    if source in {"dm", "direct"}:
        display_session_id = conversation_display_session_id(agent_id, session_key)
        session_label = summarize_task_execution_text(display_session_id, limit=20)
        return f"{agent_id} · 私聊 {session_label}".strip()
    if source in {"telegram", "qqbot", "feishu", "whatsapp", "discord", "slack"}:
        target = parts[-1] if len(parts) >= 5 else ""
        target_label = target[:18] + "..." if len(target) > 18 else target
        kind_label = "群组" if "group" in parts else "私聊"
        return f"{CONVERSATION_SOURCE_LABELS.get(source, source)} · {kind_label} {target_label}".strip()
    if source == "cron":
        return f"{agent_id} · 定时任务"
    if source == "subagent":
        return f"{agent_id} · 子代理会话"
    return session_key or agent_id or "未命名会话"


def load_conversation_catalog(openclaw_dir, config, agent_labels, limit=36):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()
    local_config = config or load_config(openclaw_dir)
    metadata = load_project_metadata(openclaw_dir, config=local_config)
    agent_overrides = agent_runtime_overrides(metadata)
    profile_map = {}
    for agent in load_agents(local_config):
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        override = agent_overrides.get(agent_id) if isinstance(agent_overrides.get(agent_id), dict) else {}
        profile_map[agent_id] = merged_agent_runtime_profile(agent_id, override=override)

    def fallback_session_items():
        items = []
        seen = []
        loaded_runtime_agents = False
        try:
            env = openclaw_command_env(openclaw_dir)
            result = run_command(["openclaw", "agents", "list", "--json"], env=env)
            payload = parse_json_payload(result.stdout, result.stderr, default=[])
            if isinstance(payload, list):
                for item in payload:
                    if not isinstance(item, dict):
                        continue
                    agent_id = str(item.get("id") or "").strip()
                    if agent_id and agent_id not in seen:
                        seen.append(agent_id)
                loaded_runtime_agents = bool(seen)
        except Exception:
            pass
        if not loaded_runtime_agents:
            for team in safe_list(store_list_agent_teams(openclaw_dir)):
                if not isinstance(team, dict):
                    continue
                lead_agent_id = str(team.get("leadAgentId") or "").strip()
                if lead_agent_id and lead_agent_id not in seen:
                    seen.append(lead_agent_id)
                for candidate in safe_list(team.get("memberAgentIds")):
                    agent_id = str(candidate or "").strip()
                    if agent_id and agent_id not in seen:
                        seen.append(agent_id)
        for agent_id in seen[:limit]:
            runtime_profile = profile_map.get(agent_id, {})
            items.append(
                {
                    "key": f"agent:{agent_id}:main",
                    "agentId": agent_id,
                    "agentLabel": agent_labels.get(agent_id, agent_id),
                    "agentHumanName": str(runtime_profile.get("humanName") or "").strip(),
                    "agentJobTitle": str(runtime_profile.get("jobTitle") or "").strip(),
                    "agentRoleLabel": str(runtime_profile.get("roleLabel") or "").strip(),
                    "sessionId": "main",
                    "kind": "direct",
                    "source": "main",
                    "sourceLabel": CONVERSATION_SOURCE_LABELS.get("main", "主会话"),
                    "talkable": True,
                    "label": f"{agent_id} · 主会话",
                    "updatedAt": "",
                    "updatedAgo": "",
                    "model": "default",
                    "provider": "",
                    "contextTokens": 0,
                    "totalTokens": 0,
                    "abortedLastRun": False,
                    "lastMessageId": "",
                    "preview": "暂时还没有可展示的文本消息。",
                    "transcriptPath": "",
                }
            )
        return items

    def build():
        env = openclaw_command_env(openclaw_dir)
        result = run_command(["openclaw", "sessions", "--all-agents", "--json"], env=env)
        payload = parse_json_payload(result.stdout, result.stderr, default=None)
        if payload is None:
            fallback_items = fallback_session_items()
            if fallback_items:
                return {
                    "supported": True,
                    "error": "",
                    "summary": {"total": len(fallback_items), "talkable": len(fallback_items), "active24h": 0},
                    "sessions": fallback_items,
                    "commands": [],
                }
            return {
                "supported": False,
                "error": (result.stderr or result.stdout or "读取会话目录失败。").strip(),
                "summary": {"total": 0, "talkable": 0, "active24h": 0},
                "sessions": [],
                "commands": [],
            }

        now = now_utc()
        items = []
        for session in payload.get("sessions", []) or []:
            updated_at = epoch_ms_to_iso(session.get("updatedAt"))
            updated_dt = parse_iso(updated_at)
            agent_id = str(session.get("agentId", "") or "").strip()
            session_id = str(session.get("sessionId", "") or "").strip()
            manifest_entry = conversation_session_manifest(openclaw_dir, agent_id, session_id)
            reserved_session = is_reserved_conversation_session_entry(manifest_entry)
            source = "heartbeat" if reserved_session else conversation_source_from_key(session.get("key"))
            runtime_profile = profile_map.get(agent_id, {})
            transcript_path = session_transcript_path(openclaw_dir, agent_id, session_id)
            transcript_preview = parse_transcript_items(transcript_path, limit=18)
            preview_items = transcript_preview.get("items") if isinstance(transcript_preview.get("items"), list) else []
            last_preview_item = preview_items[-1] if preview_items else {}
            items.append(
                {
                    "key": session.get("key", ""),
                    "agentId": agent_id,
                    "agentLabel": agent_labels.get(agent_id, agent_id),
                    "agentHumanName": str(runtime_profile.get("humanName") or "").strip(),
                    "agentJobTitle": str(runtime_profile.get("jobTitle") or "").strip(),
                    "agentRoleLabel": str(runtime_profile.get("roleLabel") or "").strip(),
                    "sessionId": session_id,
                    "kind": session.get("kind", "direct"),
                    "source": source,
                    "sourceLabel": "心跳会话" if reserved_session else CONVERSATION_SOURCE_LABELS.get(source, source),
                    "talkable": (source not in READ_ONLY_CONVERSATION_SOURCES) and not reserved_session,
                    "label": (f"{agent_id} · 心跳会话" if reserved_session else conversation_label(session)),
                    "updatedAt": updated_at,
                    "updatedAgo": format_age(updated_dt, now),
                    "model": session.get("model", ""),
                    "provider": session.get("modelProvider") or session.get("providerOverride") or "",
                    "contextTokens": session.get("contextTokens"),
                    "totalTokens": session.get("totalTokens"),
                    "abortedLastRun": bool(session.get("abortedLastRun")),
                    "lastMessageId": str(last_preview_item.get("id") or "").strip(),
                    "preview": transcript_preview.get("preview", "") or "暂时还没有可展示的文本消息。",
                    "transcriptPath": str(transcript_path) if transcript_path else "",
                }
            )

        items.sort(
            key=lambda item: parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
            reverse=True,
        )
        active24h = sum(
            1
            for item in items
            if (parse_iso(item.get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc)) >= now - timedelta(hours=24)
        )
        talkable = sum(1 for item in items if item.get("talkable"))
        if not items:
            items = fallback_session_items()
            talkable = sum(1 for item in items if item.get("talkable"))
        return {
            "supported": True,
            "error": "",
            "summary": {
                "total": len(items),
                "talkable": talkable,
                "active24h": active24h,
            },
            "sessions": items[:limit],
            "commands": [
                {
                    "label": "列出全部会话",
                    "command": f'OPENCLAW_STATE_DIR="{openclaw_dir}" OPENCLAW_CONFIG_PATH="{openclaw_dir / "openclaw.json"}" openclaw sessions --all-agents --json',
                    "description": "查看当前安装目录里的真实 OpenClaw 会话索引。",
                },
                {
                    "label": "与路由 Agent 对话",
                    "command": f'OPENCLAW_STATE_DIR="{openclaw_dir}" OPENCLAW_CONFIG_PATH="{openclaw_dir / "openclaw.json"}" openclaw agent --agent {get_router_agent_id(config)} --message "你好" --json',
                    "description": "从终端直接向当前路由 Agent 发起一轮真实对话。",
                },
            ],
        }

    return cached_payload(("conversation-catalog", str(openclaw_dir)), 10, build)


def load_conversation_transcript(openclaw_dir, agent_id, session_id="", conversation_key=""):
    normalized_key = normalize_conversation_session_key(agent_id, session_id, conversation_key)
    transcript_session_ref = (
        "main"
        if conversation_route_session_key(normalized_key) == "main"
        else conversation_display_session_id(agent_id, normalized_key)
    )
    path = session_transcript_path(openclaw_dir, agent_id, transcript_session_ref)
    transcript = parse_transcript_items(path, limit=140)
    transcript_items = merge_conversation_attachment_payloads(
        openclaw_dir,
        agent_id,
        session_id,
        normalized_key,
        transcript.get("items", []),
    )
    return {
        "agentId": agent_id,
        "sessionId": conversation_display_session_id(agent_id, normalized_key),
        "conversationKey": normalized_key,
        "key": normalized_key,
        "path": str(path) if path else "",
        "items": transcript_items,
        "stats": transcript.get("stats", {}),
        "meta": transcript.get("meta", {}),
    }


def open_conversation_session(openclaw_dir, agent_id, mode="main", conversation_key=""):
    normalized_agent_id = str(agent_id or "").strip()
    if not normalized_agent_id:
        raise RuntimeError("请先选择一个 Agent。")
    normalized_mode = str(mode or "main").strip().lower() or "main"
    if normalized_mode == "isolated":
        normalized_key = normalize_conversation_session_key(
            normalized_agent_id,
            "",
            conversation_key or build_direct_conversation_session_key(normalized_agent_id),
        )
    elif normalized_mode in {"raw-main", "legacy-main", "internal-main"}:
        normalized_key = normalize_conversation_session_key(normalized_agent_id, "main", conversation_key)
    else:
        normalized_key = normalize_product_conversation_session_key(
            normalized_agent_id,
            "main",
            conversation_key,
        )
    display_session_id = conversation_display_session_id(normalized_agent_id, normalized_key)
    return {
        "key": normalized_key,
        "conversationKey": normalized_key,
        "agentId": normalized_agent_id,
        "sessionId": display_session_id,
        "label": conversation_label({"key": normalized_key, "agentId": normalized_agent_id}) or f"{normalized_agent_id} · 主会话",
        "source": conversation_source_from_key(normalized_key),
        "talkable": True,
        "createdAt": now_iso(),
        "updatedAt": "",
        "updatedAgo": "",
    }


def openclaw_gateway_rpc_url(openclaw_dir, config=None):
    local_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    gateway = local_config.get("gateway") if isinstance(local_config.get("gateway"), dict) else {}
    port = int(gateway.get("port") or 18789)
    bind_mode = str(gateway.get("bind") or "loopback").strip().lower()
    host = "127.0.0.1" if bind_mode in {"", "loopback", "localhost", "local"} else "127.0.0.1"
    return f"ws://{host}:{port}"


def openclaw_gateway_auth_token(openclaw_dir, env=None, config=None):
    resolved_env = env if isinstance(env, dict) else openclaw_command_env(openclaw_dir)
    resolved_config = config if isinstance(config, dict) else load_config(openclaw_dir)
    gateway_auth = (
        ((resolved_config.get("gateway") or {}) if isinstance(resolved_config, dict) else {}).get("auth")
        if isinstance(((resolved_config.get("gateway") or {}) if isinstance(resolved_config, dict) else {}).get("auth"), dict)
        else {}
    )
    token = str(
        resolved_env.get("OPENCLAW_GATEWAY_TOKEN")
        or resolved_env.get("GATEWAY_AUTH_TOKEN")
        or gateway_auth.get("token")
        or gateway_auth.get("password")
        or ""
    ).strip()
    if not token:
        raise RuntimeError("当前 OpenClaw Gateway 没有可用的鉴权令牌。")
    return token


def openclaw_gateway_auth_header(openclaw_dir, env=None, config=None):
    return f"Bearer {openclaw_gateway_auth_token(openclaw_dir, env=env, config=config)}"


def register_conversation_session_alias(openclaw_dir, agent_id, conversation_key="", target_session_id=""):
    normalized_agent_id = str(agent_id or "").strip()
    normalized_key = normalize_conversation_session_key(normalized_agent_id, "", conversation_key)
    normalized_target_session_id = str(target_session_id or "").strip()
    if not normalized_agent_id or not normalized_key or not normalized_target_session_id:
        return None
    session_dirs = agent_session_dirs(openclaw_dir, normalized_agent_id)
    if not session_dirs:
        return None
    sessions_dir = session_dirs[0]
    try:
        sessions_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None
    sessions_path = sessions_dir / "sessions.json"
    index = load_json(sessions_path, {})
    if not isinstance(index, dict):
        index = {}
    existing_alias = index.get(normalized_key) if isinstance(index.get(normalized_key), dict) else {}
    target_entry = session_manifest_entry(openclaw_dir, normalized_agent_id, normalized_target_session_id)
    if not isinstance(target_entry, dict):
        target_entry = {}
    alias_entry = {
        **target_entry,
        **existing_alias,
        "sessionId": normalized_target_session_id,
        "updatedAt": int(time.time() * 1000),
    }
    session_file = str(alias_entry.get("sessionFile") or "").strip()
    if not session_file:
        candidate_file = sessions_dir / f"{normalized_target_session_id}.jsonl"
        if candidate_file.exists():
            session_file = str(candidate_file)
    if session_file:
        alias_entry["sessionFile"] = session_file
    alias_entry.setdefault("chatType", "direct")
    alias_entry.setdefault(
        "origin",
        {
            "label": "mission-control",
            "provider": "mission-control",
            "from": "mission-control",
            "to": "mission-control",
        },
    )
    index[normalized_key] = alias_entry
    sessions_path.write_text(json.dumps(index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return alias_entry


def extract_openclaw_chat_completion_text(payload):
    payload = payload if isinstance(payload, dict) else {}
    choices = payload.get("choices") if isinstance(payload.get("choices"), list) else []
    if not choices:
        return ""
    message = choices[0].get("message") if isinstance(choices[0], dict) else {}
    content = message.get("content") if isinstance(message, dict) else ""
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts = []
        for item in content:
            if not isinstance(item, dict):
                continue
            text_value = str(item.get("text") or item.get("content") or "").strip()
            if text_value:
                parts.append(text_value)
        return "\n".join(parts).strip()
    output_text = str(payload.get("output_text") or "").strip()
    return output_text


def conversation_transcript_reply_entries(payload):
    transcript_payload = payload if isinstance(payload, dict) else {}
    entries = []
    for index, item in enumerate(safe_list(transcript_payload.get("items"))):
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "").strip() != "assistant":
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        signature = (
            str(item.get("id") or "").strip()
            or str(item.get("timestamp") or "").strip()
            or f"{index}:{text[:160]}"
        )
        entries.append(
            {
                "signature": signature,
                "text": text,
                "item": item,
            }
        )
    return entries


def wait_for_conversation_gateway_reply(
    openclaw_dir,
    agent_id,
    conversation_key,
    baseline_signatures=None,
    timeout_seconds=45,
):
    deadline = time.time() + max(5.0, float(timeout_seconds or 45))
    seen_signatures = {
        str(item or "").strip()
        for item in safe_list(baseline_signatures)
        if str(item or "").strip()
    }
    placeholder_reply = None
    while time.time() < deadline:
        transcript_payload = _chat_runtime_callable(
            "load_conversation_transcript",
            load_conversation_transcript,
        )(
            openclaw_dir,
            agent_id,
            conversation_display_session_id(agent_id, conversation_key),
            conversation_key=conversation_key,
        )
        reply_entries = conversation_transcript_reply_entries(transcript_payload)
        for reply in reply_entries:
            if reply["signature"] not in seen_signatures:
                if conversation_reply_is_placeholder(reply.get("text")):
                    placeholder_reply = reply
                    seen_signatures.add(reply["signature"])
                    continue
                return reply
        time.sleep(0.35)
    if placeholder_reply is not None:
        return placeholder_reply
    raise RuntimeError("隔离私聊会话已提交，但等待回复超时。")


def perform_conversation_send_via_gateway_call(
    openclaw_dir,
    agent_id,
    message,
    conversation_key,
    thinking="low",
    timeout_seconds=45,
    fallback_reply_text="",
    prefer_fallback_reply=False,
    explicit_voice_utterance="",
    explicit_direct_reply_utterance="",
    defer_reply_until_sse=False,
):
    config = load_config(openclaw_dir)
    env = openclaw_command_env(openclaw_dir)
    route_session_key = conversation_route_session_key(conversation_key)
    baseline_transcript = _chat_runtime_callable(
        "load_conversation_transcript",
        load_conversation_transcript,
    )(
        openclaw_dir,
        agent_id,
        conversation_display_session_id(agent_id, conversation_key),
        conversation_key=conversation_key,
    )
    baseline_signatures = [
        item["signature"]
        for item in conversation_transcript_reply_entries(baseline_transcript)
    ]
    params = {
        "sessionKey": route_session_key,
        "message": str(message or "").strip(),
        "thinking": str(thinking or "low").strip() or "low",
        "idempotencyKey": f"conversation-send-{secrets.token_hex(8)}",
    }
    result = _chat_runtime_callable("run_command", run_command)(
        [
            "openclaw",
            "gateway",
            "call",
            "chat.send",
            "--url",
            openclaw_gateway_rpc_url(openclaw_dir, config=config),
            "--token",
            openclaw_gateway_auth_token(openclaw_dir, env=env, config=config),
            "--params",
            json.dumps(params, ensure_ascii=False),
            "--json",
            "--timeout",
            str(max(5000, int(timeout_seconds or 45) * 1000)),
        ]
        + ([] if defer_reply_until_sse else ["--expect-final"]),
        env=env,
        timeout=max(10, int(timeout_seconds or 45) + 5),
    )
    parsed = parse_json_payload(result.stdout, result.stderr, default=None)
    if result.returncode != 0:
        message_text = (
            str(parsed.get("error") or parsed.get("message") or "").strip()
            if isinstance(parsed, dict)
            else ""
        ) or str(result.stderr or result.stdout or "").strip() or "会话发送失败。"
        raise RuntimeError(message_text)
    parsed_result = parsed.get("result") if isinstance(parsed, dict) and isinstance(parsed.get("result"), dict) else {}
    parsed_payloads = parsed_result.get("payloads") if isinstance(parsed_result.get("payloads"), list) else []
    reply_text = parsed_payloads[0].get("text", "") if parsed_payloads and isinstance(parsed_payloads[0], dict) else ""
    if conversation_reply_is_placeholder(reply_text):
        reply_text = ""
    reply_source = "gateway_payload" if reply_text else ""
    fallback_text = str(fallback_reply_text or "").strip()
    exact_utterance = str(explicit_voice_utterance or "").strip()
    exact_direct_reply = str(explicit_direct_reply_utterance or "").strip()
    if prefer_fallback_reply and fallback_text:
        reply_text = fallback_text
        reply_source = "session_transcript_fallback"
    elif not reply_text and fallback_text:
        reply_text = str(fallback_reply_text or "").strip()
        reply_source = "session_transcript_fallback"
    elif defer_reply_until_sse and not reply_text and not exact_utterance:
        reply_source = "session_transcript_pending"
    if not reply_text:
        try:
            if defer_reply_until_sse:
                reply_entry = None
            else:
                reply_entry = wait_for_conversation_gateway_reply(
                    openclaw_dir,
                    agent_id,
                    conversation_key,
                    baseline_signatures=baseline_signatures,
                    timeout_seconds=timeout_seconds,
                )
        except RuntimeError as error:
            if "等待回复超时" in str(error or ""):
                reply_entry = None
                reply_source = "session_transcript_pending"
            else:
                raise
        if isinstance(reply_entry, dict):
            reply_text = str(reply_entry.get("text") or "").strip()
            if conversation_reply_is_placeholder(reply_text):
                reply_text = ""
            reply_source = str(reply_entry.get("source") or "").strip() or "session_transcript_fallback"
    if not reply_text and fallback_text:
        reply_text = fallback_text
        reply_source = "session_transcript_fallback"
    if exact_utterance:
        reply_text = exact_utterance
        reply_source = "voice_explicit_utterance"
    if exact_direct_reply:
        reply_text = exact_direct_reply
        reply_source = "text_explicit_utterance"
    return {
        "status": "ok",
        "result": {
            "meta": {
                "agentMeta": {
                    "agentId": str(agent_id or "").strip(),
                    "sessionId": conversation_display_session_id(agent_id, conversation_key),
                    "sessionKey": route_session_key,
                    "conversationKey": conversation_key,
                },
            },
            "payloads": [{"text": reply_text, "source": reply_source}],
            "transport": "gateway-rpc",
        },
        "raw": parsed,
    }


def resolve_chat_thread_team_records(openclaw_dir, team_id="", linked_team_ids=None, collaborator_team_ids=None):
    resolved_ids = normalize_chat_thread_linked_team_ids(team_id, linked_team_ids, collaborator_team_ids)
    if not resolved_ids:
        return []
    team_map = {
        str(item.get("id") or "").strip(): item
        for item in store_list_agent_teams(openclaw_dir)
        if str(item.get("id") or "").strip()
    }
    return [team_map[item] for item in resolved_ids if item in team_map]


def resolve_chat_thread_participant_agent_ids(existing_participants, team_records):
    participant_agent_ids = clean_unique_strings(existing_participants or [])
    if participant_agent_ids:
        return participant_agent_ids
    return clean_unique_strings([
        agent_id
        for team in safe_list(team_records)
        for agent_id in safe_list((team or {}).get("memberAgentIds"))
    ])


CONVERSATION_SESSION_RESET_ERROR_MARKERS = (
    "session inactive",
    "inactive session",
    "session not found",
    "unknown session",
    "invalid session",
    "missing session",
    "session missing",
    "session expired",
    "session ended",
    "stale session",
    "session stale",
    "no active session",
    "not an active session",
    "session file locked",
    "gateway closed",
    "abnormal closure",
    "gateway agent failed",
    "fallback to embedded",
)


def conversation_session_manifest(openclaw_dir, agent_id, session_id):
    entry = session_manifest_entry(openclaw_dir, agent_id, session_id)
    return entry if isinstance(entry, dict) else {}


def is_reserved_conversation_session_entry(entry):
    entry = entry if isinstance(entry, dict) else {}
    origin = entry.get("origin") if isinstance(entry.get("origin"), dict) else {}
    provider = str(origin.get("provider") or "").strip().lower()
    label = str(origin.get("label") or "").strip().lower()
    from_value = str(origin.get("from") or "").strip().lower()
    to_value = str(origin.get("to") or "").strip().lower()
    return "heartbeat" in {provider, label, from_value, to_value}


def is_reserved_conversation_session(openclaw_dir, agent_id, session_id):
    session_value = str(session_id or "").strip()
    if not session_value or not agent_id:
        return False
    return is_reserved_conversation_session_entry(
        conversation_session_manifest(openclaw_dir, agent_id, session_value)
    )


def conversation_session_exists(openclaw_dir, agent_id, session_id):
    session_value = str(session_id or "").strip()
    if not session_value or not agent_id:
        return False
    if session_transcript_path(openclaw_dir, agent_id, session_value):
        return True
    return bool(conversation_session_manifest(openclaw_dir, agent_id, session_value))


def sanitize_conversation_session_id_for_send(openclaw_dir, agent_id, session_id):
    session_value = str(session_id or "").strip()
    if not session_value:
        return ""
    if session_value == "main":
        return ""
    if is_reserved_conversation_session(openclaw_dir, agent_id, session_value):
        return ""
    if not conversation_session_exists(openclaw_dir, agent_id, session_value):
        return ""
    return session_value


def is_synthetic_conversation_session_id(session_id):
    session_value = str(session_id or "").strip().lower()
    if not session_value:
        return False
    return (
        session_value == "main"
        or session_value.startswith("chat-")
        or session_value.startswith("task-")
    )


def is_persistable_conversation_session_id(openclaw_dir, agent_id, session_id):
    session_value = str(session_id or "").strip()
    if not session_value:
        return False
    if is_synthetic_conversation_session_id(session_value):
        return False
    if is_reserved_conversation_session(openclaw_dir, agent_id, session_value):
        return False
    return True


def select_persistable_conversation_session_id(openclaw_dir, agent_id, primary_session_id="", fallback_session_id=""):
    primary_value = str(primary_session_id or "").strip()
    if is_persistable_conversation_session_id(openclaw_dir, agent_id, primary_value):
        return primary_value
    if primary_value:
        return ""
    fallback_value = str(fallback_session_id or "").strip()
    if is_persistable_conversation_session_id(openclaw_dir, agent_id, fallback_value):
        return fallback_value
    return ""


def should_retry_conversation_send_without_session_id(openclaw_dir, agent_id, session_id, error_text=""):
    session_value = str(session_id or "").strip()
    if not session_value:
        return False
    if session_value == "main":
        return True
    if is_reserved_conversation_session(openclaw_dir, agent_id, session_value):
        return True
    if not conversation_session_exists(openclaw_dir, agent_id, session_value):
        return False
    normalized_error = re.sub(r"\s+", " ", str(error_text or "").strip().lower())
    return any(marker in normalized_error for marker in CONVERSATION_SESSION_RESET_ERROR_MARKERS)


def perform_conversation_send(
    openclaw_dir,
    agent_id,
    message,
    session_id="",
    conversation_key="",
    thinking="low",
    agent_timeout_seconds=45,
    stream_observer=None,
    selected_skill_slugs=None,
    defer_reply_until_sse=False,
):
    if not agent_id:
        raise RuntimeError("请先选择一个 Agent。")
    text = str(message or "").strip()
    if not text:
        raise RuntimeError("消息不能为空。")
    voice_reply_requested = conversation_message_requests_voice_reply(text)
    voice_followup_only = conversation_voice_reply_followup_only_request(text)
    voice_explicit_utterance = extract_explicit_conversation_voice_utterance(text)
    explicit_voice_utterance_fast_path = voice_reply_requested and bool(voice_explicit_utterance)
    explicit_direct_reply_utterance = (
        extract_explicit_conversation_reply_utterance(text) if not voice_reply_requested else ""
    )
    explicit_direct_reply_fast_path = bool(explicit_direct_reply_utterance)
    rewritten_text = rewrite_conversation_voice_request_text(text, followup_only=voice_followup_only)
    selected_skill_slugs = clean_unique_strings((selected_skill_slugs or []) + auto_selected_skill_slugs_for_message(text))
    config = None
    metadata = None
    needs_memory_authority_context = bool(
        _chat_runtime_callable("memory_authority_query_matches", lambda _text: False)(rewritten_text)
    )
    needs_skill_context = chat_message_may_need_skill_context(rewritten_text, selected_skill_slugs=selected_skill_slugs)
    if explicit_voice_utterance_fast_path or explicit_direct_reply_fast_path:
        needs_memory_authority_context = False
        needs_skill_context = False
    if needs_memory_authority_context or needs_skill_context:
        config = load_config(openclaw_dir)
    if needs_memory_authority_context:
        metadata = load_project_metadata(openclaw_dir, config=config)
    text = rewritten_text
    memory_authority_context = build_memory_authority_context_message(text, metadata or {})
    if memory_authority_context and memory_authority_context not in text:
        text = f"{text}\n\n{memory_authority_context}"
    if needs_skill_context:
        if metadata is None:
            metadata = load_project_metadata(openclaw_dir, config=config)
        text = build_agent_skill_context_message(
            openclaw_dir,
            agent_id,
            text,
            config=config,
            metadata=metadata,
            selected_skill_slugs=selected_skill_slugs,
        )
    sanitized_session_value = sanitize_conversation_session_id_for_send(openclaw_dir, agent_id, session_id)
    explicit_conversation_key = str(conversation_key or "").strip()
    session_seed = sanitized_session_value if sanitized_session_value else ("main" if not explicit_conversation_key else session_id)
    requested_conversation_key = normalize_product_conversation_session_key(
        agent_id,
        session_seed,
        explicit_conversation_key,
    )
    requested_route_session_key = conversation_route_session_key(requested_conversation_key)
    use_isolated_conversation_route = requested_route_session_key != "main"
    voice_reply_fallback = str(voice_explicit_utterance or "").strip()
    if voice_followup_only:
        requested_session_ref = (
            requested_route_session_key
            if use_isolated_conversation_route
            else (str(sanitized_session_value or "").strip() or "main")
        )
        requested_conversation_ref = requested_conversation_key if use_isolated_conversation_route else ""
        transcript = _chat_runtime_callable(
            "load_conversation_transcript",
            load_conversation_transcript,
        )(
            openclaw_dir,
            agent_id,
            requested_session_ref,
            requested_conversation_ref,
        )
        voice_reply_fallback = latest_assistant_transcript_text(transcript)
        if conversation_reply_is_placeholder(voice_reply_fallback):
            voice_reply_fallback = ""
        if voice_reply_fallback:
            text = append_conversation_voice_followup_context(text, voice_reply_fallback)
    if voice_reply_requested and not explicit_voice_utterance_fast_path:
        contract = conversation_voice_delivery_contract()
        if contract.strip() not in text:
            text = f"{text}{contract}"
    env = openclaw_command_env(openclaw_dir)
    effective_thinking = str(thinking or "").strip() or "low"
    if explicit_voice_utterance_fast_path or explicit_direct_reply_fast_path:
        effective_thinking = "minimal"
    effective_thinking = normalize_conversation_thinking_for_model(
        openclaw_dir,
        agent_id=agent_id,
        thinking=effective_thinking,
        config=config,
        metadata=metadata,
    )
    agent_timeout_seconds = max(5, int(agent_timeout_seconds or 45))
    process_timeout_seconds = agent_timeout_seconds + 5
    prefer_explicit_direct_reply_contract = use_isolated_conversation_route and conversation_prefers_explicit_direct_reply_contract(
        openclaw_dir,
        agent_id=agent_id,
        config=config,
        metadata=metadata,
    )
    compression = {"applied": False}
    if not explicit_voice_utterance_fast_path and not explicit_direct_reply_fast_path and not prefer_explicit_direct_reply_contract:
        compression = maybe_prepare_conversation_context_compression(
            openclaw_dir,
            agent_id,
            requested_route_session_key if use_isolated_conversation_route else (sanitized_session_value or "main"),
            text,
            conversation_key=requested_conversation_key if use_isolated_conversation_route else "",
        )
    active_message = str(compression.get("message") or text).strip() if compression.get("applied") else text
    active_conversation_key = str(compression.get("conversationKey") or requested_conversation_key).strip() or requested_conversation_key
    active_route_session_key = str(compression.get("sessionKey") or requested_route_session_key).strip() or requested_route_session_key
    isolated_route_fallback_key = ""
    if prefer_explicit_direct_reply_contract:
        active_message = append_conversation_direct_reply_contract(
            active_message,
            explicit_utterance=explicit_direct_reply_utterance,
        )
    if use_isolated_conversation_route:
        gateway_attempts = [active_conversation_key]
        gateway_last_error = None
        gateway_attempt_index = 0
        while gateway_attempt_index < len(gateway_attempts):
            attempt_conversation_key = gateway_attempts[gateway_attempt_index]
            gateway_attempt_index += 1
            try:
                payload = perform_conversation_send_via_gateway_call(
                    openclaw_dir,
                    agent_id=agent_id,
                    message=active_message,
                    conversation_key=attempt_conversation_key,
                    thinking=effective_thinking,
                    timeout_seconds=agent_timeout_seconds,
                    fallback_reply_text=voice_reply_fallback,
                    prefer_fallback_reply=voice_followup_only,
                    explicit_voice_utterance=voice_explicit_utterance,
                    explicit_direct_reply_utterance=explicit_direct_reply_utterance,
                    defer_reply_until_sse=defer_reply_until_sse,
                )
            except subprocess.TimeoutExpired:
                gateway_last_error = RuntimeError(
                    "Agent 对话超时，请检查 OpenClaw Gateway、模型配置或该 Agent 的运行状态。"
                )
                break
            except Exception as error:
                gateway_last_error = error
                attempt_session_id = conversation_display_session_id(agent_id, attempt_conversation_key)
                if should_retry_conversation_send_without_session_id(
                    openclaw_dir,
                    agent_id,
                    attempt_session_id,
                    error_text=str(error or ""),
                ):
                    primary_conversation_key = build_primary_direct_conversation_session_key(agent_id)
                    if primary_conversation_key not in gateway_attempts and attempt_conversation_key != primary_conversation_key:
                        gateway_attempts.append(primary_conversation_key)
                        continue
                break
            if callable(stream_observer):
                payloads = (((payload.get("result", {}) or {}).get("payloads", [])) or [])
                reply_preview = payloads[0].get("text", "") if payloads and isinstance(payloads[0], dict) else ""
                stream_observer(
                    {
                        "stage": "delta" if reply_preview else "started",
                        "agentId": str(agent_id or "").strip(),
                        "sessionId": conversation_display_session_id(agent_id, attempt_conversation_key),
                        "conversationKey": attempt_conversation_key,
                        "content": reply_preview,
                        "delta": reply_preview,
                    }
                )
            if compression.get("applied"):
                result_payload = payload.get("result") if isinstance(payload.get("result"), dict) else {}
                result_meta = result_payload.get("meta") if isinstance(result_payload.get("meta"), dict) else {}
                payload["result"] = {
                    **result_payload,
                    "meta": {
                        **result_meta,
                        "conversationCompression": compression.get("meta"),
                    },
                }
            return payload
        isolated_route_fallback_key = active_conversation_key

    active_session_value = str(
        (active_route_session_key if isolated_route_fallback_key else sanitized_session_value) or ""
    ).strip()
    attempts = [active_session_value] if active_session_value else [""]
    last_error = None

    attempt_index = 0
    while attempt_index < len(attempts):
        attempt_session_id = attempts[attempt_index]
        attempt_index += 1
        args = ["openclaw", "agent", "--agent", agent_id, "--message", active_message, "--json", "--timeout", str(agent_timeout_seconds)]
        if attempt_session_id:
            args.extend(["--session-id", attempt_session_id])
        if effective_thinking:
            args.extend(["--thinking", effective_thinking])
        try:
            if callable(stream_observer):
                result, streamed_text = _chat_runtime_callable(
                    "run_streaming_agent_command",
                    run_streaming_agent_command,
                )(
                    args,
                    openclaw_dir=openclaw_dir,
                    agent_id=agent_id,
                    session_id=attempt_session_id,
                    env=env,
                    timeout=process_timeout_seconds,
                    stream_observer=stream_observer,
                )
            else:
                result = _chat_runtime_callable("run_command", run_command)(
                    args,
                    env=env,
                    timeout=process_timeout_seconds,
                )
                streamed_text = ""
        except subprocess.TimeoutExpired:
            last_error = RuntimeError(
                "Agent 对话超时，请检查 OpenClaw Gateway、模型配置或该 Agent 的运行状态。"
            )
            if (
                should_retry_conversation_send_without_session_id(
                    openclaw_dir,
                    agent_id,
                    attempt_session_id,
                    error_text=str(getattr(last_error, "args", [""])[0] or ""),
                )
                and "" not in attempts
            ):
                attempts.append("")
            continue

        payload = parse_json_payload(result.stdout, result.stderr, default=None)
        if result.returncode != 0 or payload is None:
            last_error = RuntimeError((result.stderr or result.stdout or "会话发送失败。").strip())
            if (
                should_retry_conversation_send_without_session_id(
                    openclaw_dir,
                    agent_id,
                    attempt_session_id,
                    error_text=str(getattr(last_error, "args", [""])[0] or ""),
                )
                and "" not in attempts
            ):
                attempts.append("")
            continue
        status = str(payload.get("status", "")).lower()
        if status not in {"ok", "completed", "success"} and payload.get("ok") is False:
            last_error = RuntimeError(summarize_json(payload))
            if (
                should_retry_conversation_send_without_session_id(
                    openclaw_dir,
                    agent_id,
                    attempt_session_id,
                    error_text=str(getattr(last_error, "args", [""])[0] or ""),
                )
                and "" not in attempts
            ):
                attempts.append("")
            continue
        result_payload = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        result_meta = result_payload.get("meta") if isinstance(result_payload.get("meta"), dict) else {}
        agent_meta = result_meta.get("agentMeta") if isinstance(result_meta.get("agentMeta"), dict) else {}
        actual_session_id_for_meta = str(agent_meta.get("sessionId") or attempt_session_id or "main").strip() or "main"
        payloads = result_payload.get("payloads") if isinstance(result_payload.get("payloads"), list) else []
        reply_preview = payloads[0].get("text", "") if payloads and isinstance(payloads[0], dict) else ""
        if conversation_reply_is_placeholder(reply_preview):
            reply_preview = ""
        if voice_reply_requested:
            fallback_reply_preview = voice_reply_fallback
            if not fallback_reply_preview:
                transcript_path = session_transcript_path(openclaw_dir, agent_id, actual_session_id_for_meta)
                transcript = parse_transcript_items(transcript_path, limit=220)
                fallback_reply_preview = latest_assistant_transcript_text(transcript)
                if conversation_reply_is_placeholder(fallback_reply_preview):
                    fallback_reply_preview = ""
            if voice_explicit_utterance:
                fallback_reply_preview = voice_explicit_utterance
            if fallback_reply_preview and (
                (not reply_preview)
                or conversation_reply_is_placeholder(reply_preview)
                or (voice_explicit_utterance and reply_preview != voice_explicit_utterance)
                or (voice_followup_only and should_replace_voice_followup_reply(reply_preview, fallback_reply_preview))
            ):
                result_payload = {
                    **result_payload,
                    "payloads": [
                        {
                            "text": fallback_reply_preview,
                            "source": "voice_explicit_utterance" if voice_explicit_utterance else "session_transcript_fallback",
                        }
                    ],
                }
                result_meta = result_payload.get("meta") if isinstance(result_payload.get("meta"), dict) else result_meta
                reply_preview = fallback_reply_preview
        actual_conversation_key = normalize_conversation_session_key(agent_id, actual_session_id_for_meta)
        response_session_id = actual_session_id_for_meta
        response_conversation_key = actual_conversation_key
        if isolated_route_fallback_key:
            register_conversation_session_alias(
                openclaw_dir,
                agent_id,
                conversation_key=isolated_route_fallback_key,
                target_session_id=actual_session_id_for_meta,
            )
            response_session_id = conversation_display_session_id(agent_id, isolated_route_fallback_key)
            response_conversation_key = isolated_route_fallback_key
        payload["result"] = {
            **result_payload,
            "meta": {
                **result_meta,
                "agentMeta": {
                    **agent_meta,
                    "agentId": str(agent_meta.get("agentId") or agent_id).strip(),
                    "sessionId": response_session_id,
                    "sessionKey": conversation_route_session_key(response_conversation_key),
                    "conversationKey": response_conversation_key,
                },
            },
        }
        if callable(stream_observer):
            payloads = (((payload.get("result", {}) or {}).get("payloads", [])) or [])
            reply_preview = payloads[0].get("text", "") if payloads and isinstance(payloads[0], dict) else ""
            result_meta = ((payload.get("result", {}) or {}).get("meta", {}) or {})
            agent_meta = (result_meta.get("agentMeta", {}) or {}) if isinstance(result_meta, dict) else {}
            actual_session_id = str(agent_meta.get("sessionId") or attempt_session_id).strip() or attempt_session_id
            actual_stream_conversation_key = str(
                agent_meta.get("conversationKey")
                or (response_conversation_key if response_conversation_key else "")
            ).strip() or normalize_conversation_session_key(agent_id, actual_session_id or "main")
            if reply_preview and reply_preview != streamed_text:
                stream_observer(
                    {
                        "stage": "delta",
                        "agentId": str(agent_id or "").strip(),
                        "sessionId": actual_session_id,
                        "conversationKey": actual_stream_conversation_key,
                        "content": reply_preview,
                        "delta": stream_text_delta(streamed_text, reply_preview),
                    }
                )
        if compression.get("applied"):
            result_payload = payload.get("result") if isinstance(payload.get("result"), dict) else {}
            result_meta = result_payload.get("meta") if isinstance(result_payload.get("meta"), dict) else {}
            payload["result"] = {
                **result_payload,
                "meta": {
                    **result_meta,
                    "conversationCompression": compression.get("meta"),
                },
            }
        return payload

    raise last_error or RuntimeError("会话发送失败。")


def perform_conversation_fanout(
    openclaw_dir,
    requests,
    default_thinking="low",
    timeout_seconds=TASK_EXECUTION_TEAM_FANOUT_TIMEOUT_SECONDS,
    stream_callback=None,
    max_workers=None,
    result_callback=None,
):
    normalized_requests = []
    for item in safe_list(requests):
        if not isinstance(item, dict):
            continue
        agent_id = str(item.get("agentId") or "").strip()
        message = str(item.get("message") or "").strip()
        if not agent_id or not message:
            continue
        try:
            delay_seconds = float(item.get("delaySeconds") or 0.0)
        except (TypeError, ValueError):
            delay_seconds = 0.0
        normalized_requests.append(
            {
                "index": len(normalized_requests),
                "agentId": agent_id,
                "sessionId": str(item.get("sessionId") or "").strip(),
                "message": message,
                "thinking": str(item.get("thinking") or default_thinking or "low").strip() or "low",
                "delaySeconds": max(0.0, delay_seconds),
                "selectedSkillSlugs": clean_unique_strings(item.get("selectedSkillSlugs") or []),
            }
        )
    if not normalized_requests:
        return {"successes": [], "failures": []}

    def send_request(request_item):
        def notify_stream(event):
            if not callable(stream_callback):
                return
            normalized_event = event if isinstance(event, dict) else {}
            payload = {
                **normalized_event,
                "agentId": request_item["agentId"],
                "sessionId": str(normalized_event.get("sessionId") or request_item["sessionId"] or "").strip(),
            }
            stream_callback(payload)

        try:
            if request_item["delaySeconds"] > 0:
                time.sleep(request_item["delaySeconds"])
            send_kwargs = {
                "agent_id": request_item["agentId"],
                "session_id": request_item["sessionId"],
                "message": request_item["message"],
                "thinking": request_item["thinking"],
                "agent_timeout_seconds": timeout_seconds,
            }
            selected_skill_slugs = clean_unique_strings(request_item.get("selectedSkillSlugs") or [])
            if selected_skill_slugs:
                send_kwargs["selected_skill_slugs"] = selected_skill_slugs
            if callable(stream_callback):
                send_kwargs["stream_observer"] = notify_stream
            result = _conversation_send_impl()(
                openclaw_dir,
                **send_kwargs,
            )
        except Exception as error:
            return (
                request_item["index"],
                False,
                {
                    "agentId": request_item["agentId"],
                    "error": str(error or "会话发送失败。").strip(),
                },
            )
        agent_meta = (((result.get("result", {}) or {}).get("meta", {}) or {}).get("agentMeta", {}) or {})
        actual_session_id = str(agent_meta.get("sessionId") or request_item["sessionId"] or "main").strip() or "main"
        return (
            request_item["index"],
            True,
            {
                "agentId": request_item["agentId"],
                "sessionId": actual_session_id,
                "result": result,
            },
        )

    ordered_results = []
    resolved_max_workers = max_workers if max_workers is not None else 4
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(int(resolved_max_workers or 1), len(normalized_requests)))
    ) as executor:
        futures = [executor.submit(send_request, item) for item in normalized_requests]
        for future in concurrent.futures.as_completed(futures):
            completed = future.result()
            ordered_results.append(completed)
            if callable(result_callback):
                index, ok, payload = completed
                try:
                    result_callback({"index": index, "ok": ok, **payload})
                except Exception:
                    pass
    ordered_results.sort(key=lambda item: item[0])
    return {
        "successes": [payload for _index, ok, payload in ordered_results if ok],
        "failures": [payload for _index, ok, payload in ordered_results if not ok],
    }


def perform_workflow_pack_launch_to_chat(openclaw_dir, pack, payload, actor):
    payload = payload if isinstance(payload, dict) else {}
    config = load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    team_id = str(payload.get("teamId") or pack.get("recommendedTeamId") or "").strip()
    thread_id = str(payload.get("threadId") or "").strip()
    existing_thread = (store_get_chat_thread(openclaw_dir, thread_id) or {}) if thread_id else {}
    existing_meta = existing_thread.get("meta") if isinstance(existing_thread.get("meta"), dict) else {}
    linked_team_ids = normalize_chat_thread_linked_team_ids(
        team_id,
        payload.get("linkedTeamIds") if isinstance(payload.get("linkedTeamIds"), list) else existing_meta.get("linkedTeamIds"),
        payload.get("collaboratorTeamIds") if isinstance(payload.get("collaboratorTeamIds"), list) else [],
    )
    team_id = team_id or (linked_team_ids[0] if linked_team_ids else "")
    team_records = resolve_chat_thread_team_records(openclaw_dir, team_id, linked_team_ids)
    team = team_records[0] if team_records else None
    participant_agent_ids = resolve_chat_thread_participant_agent_ids(
        payload.get("participantAgentIds") if isinstance(payload.get("participantAgentIds"), list) else existing_thread.get("participantAgentIds"),
        team_records,
    )
    participant_agent_ids = clean_unique_strings(
        participant_agent_ids
        or [str(payload.get("primaryAgentId") or existing_thread.get("primaryAgentId") or router_agent_id).strip()]
    )
    primary_agent_id = str(
        payload.get("primaryAgentId")
        or existing_thread.get("primaryAgentId")
        or ((team or {}).get("leadAgentId") if team else "")
        or (participant_agent_ids[0] if participant_agent_ids else router_agent_id)
    ).strip()
    current_target_agent_id = str(
        payload.get("currentTargetAgentId")
        or existing_thread.get("currentTargetAgentId")
        or primary_agent_id
    ).strip()
    thread_meta = deepcopy(existing_thread.get("meta", {})) if isinstance(existing_thread.get("meta"), dict) else {}
    pack_binding = workflow_pack_binding_payload(pack, source=str(payload.get("source") or "skills"), target="chat")
    pack_capabilities = workflow_pack_capabilities(pack)
    thread_meta = {
        **thread_meta,
        "mode": str(pack.get("mode") or "").strip(),
        "packBinding": pack_binding,
        "reviewGates": pack_capabilities.get("reviewGates", []),
        "artifactTemplates": pack_capabilities.get("artifactTemplates", []),
        "runtimePolicy": pack_capabilities.get("runtimePolicy", {}),
        "dispatchMode": normalize_chat_dispatch_mode(
            payload.get("dispatchMode") or thread_meta.get("dispatchMode") or resolve_team_default_dispatch_mode(team),
            has_team=bool(team_id),
        ),
    }
    if team_id:
        thread_meta["teamId"] = team_id
    if linked_team_ids:
        thread_meta["linkedTeamIds"] = linked_team_ids
    if team:
        thread_meta["teamPolicy"] = team_policy_payload(team)
    thread = store_save_chat_thread(
        openclaw_dir,
        {
            "id": existing_thread.get("id", ""),
            "title": str(payload.get("title") or existing_thread.get("title") or pack.get("name") or "Workflow Pack").strip(),
            "status": str(payload.get("status") or existing_thread.get("status") or "open").strip() or "open",
            "channel": str(payload.get("channel") or existing_thread.get("channel") or "internal").strip() or "internal",
            "owner": str(payload.get("owner") or existing_thread.get("owner") or actor.get("displayName", "")).strip(),
            "primaryAgentId": primary_agent_id,
            "currentTargetAgentId": current_target_agent_id,
            "linkedTaskId": str(payload.get("linkedTaskId") or existing_thread.get("linkedTaskId") or "").strip(),
            "linkedDeliverableId": str(payload.get("linkedDeliverableId") or existing_thread.get("linkedDeliverableId") or "").strip(),
            "linkedRunId": str(payload.get("linkedRunId") or existing_thread.get("linkedRunId") or "").strip(),
            "participantAgentIds": participant_agent_ids,
            "participantHumans": existing_thread.get("participantHumans") if isinstance(existing_thread.get("participantHumans"), list) else [
                {
                    "name": actor.get("displayName", ""),
                    "username": actor.get("username", ""),
                    "role": actor.get("role", ""),
                }
            ],
            "meta": thread_meta,
        },
    )
    brief = str(payload.get("brief") or "").strip()
    launch_note = brief or "\n".join(
        [
            f"已从 Workflow Pack 发起：{str(pack.get('name') or pack.get('id') or '').strip()}",
            f"模式：{str(pack.get('mode') or '').strip() or 'unknown'}",
            (
                "建议下一步：" + "、".join(str(item.get("displayName") or item.get("slug") or "").strip() for item in safe_list(pack.get("skillRefs"))[:3] if str(item.get("displayName") or item.get("slug") or "").strip())
                if safe_list(pack.get("skillRefs"))
                else "建议下一步：先补齐阶段目标和交接产物。"
            ),
        ]
    )
    store_save_chat_message(
        openclaw_dir,
        {
            "threadId": thread.get("id", ""),
            "senderKind": "system",
            "senderId": "workflow-pack",
            "senderLabel": "Workflow Pack",
            "direction": "system",
            "body": launch_note,
            "meta": {
                "packBinding": pack_binding,
                "mode": str(pack.get("mode") or "").strip(),
                "launchSource": str(payload.get("source") or "skills").strip() or "skills",
            },
        },
    )
    return thread


def send_notification_message(channel, alert, openclaw_dir=""):
    channel_type = str(channel.get("type") or "").strip().lower()
    target = str(channel.get("target") or "").strip()
    secret = str(channel.get("secret") or "").strip()
    title = str(alert.get("title") or "OpenClaw Team Alert").strip()
    detail = str(alert.get("detail") or "").strip()
    message = f"{title}\n{detail}".strip()

    if target.startswith("fixture://"):
        if target.startswith("fixture://error/"):
            return {"ok": False, "detail": f"fixture failure for {target}"}
        if target.startswith("fixture://auth/"):
            raise RuntimeError("通知推送失败: HTTP 401")
        if target.startswith("fixture://rate-limit/"):
            raise RuntimeError("通知推送失败: HTTP 429")
        return {"ok": True, "detail": f"fixture delivered to {target}"}

    if channel_type == "telegram":
        if not secret or not target:
            raise RuntimeError("Telegram 通知需要 bot token 和 chat id。")
        url = f"https://api.telegram.org/bot{secret}/sendMessage"
        data = json.dumps({"chat_id": target, "text": message}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
    elif channel_type == "feishu":
        if not target:
            raise RuntimeError("飞书通知需要 webhook 地址。")
        url = target
        data = json.dumps({"msg_type": "text", "content": {"text": message}}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
    elif channel_type == "webhook":
        if not target:
            raise RuntimeError("Webhook 通知需要 URL。")
        url = target
        data = json.dumps({"title": title, "text": detail, "alert": alert}, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
    else:
        raise RuntimeError(f"不支持的通知类型: {channel_type}")

    try:
        response = guarded_http_request(
            openclaw_dir,
            url,
            method="POST",
            headers=headers,
            data=data,
            timeout=8,
            audit_context=f"notification-send-{channel_type or 'unknown'}",
        )
        status_code = int(response.get("status") or 0)
        body = str(response.get("body") or "")
        if 200 <= status_code < 300:
            return {"ok": True, "detail": body[:320] or f"HTTP {status_code}"}
        raise RuntimeError(f"通知推送失败: HTTP {status_code}")
    except RuntimeError as error:
        message_text = str(error).strip()
        if message_text.startswith("通知推送失败:"):
            raise
        raise RuntimeError(f"通知推送失败: {message_text}") from error
