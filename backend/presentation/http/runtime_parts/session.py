"""Runtime part: session."""

def parse_transcript_items(transcript_path, limit=120):
    if not transcript_path or not Path(transcript_path).exists():
        return {
            "items": [],
            "preview": "",
            "meta": {"model": "", "provider": "", "thinkingLevel": ""},
            "stats": {"turns": 0, "userMessages": 0, "assistantMessages": 0, "toolMessages": 0},
        }

    items = []
    preview = ""
    meta = {"model": "", "provider": "", "thinkingLevel": ""}
    user_messages = 0
    assistant_messages = 0
    tool_messages = 0
    placeholder_texts = {"NO_REPLY", "HEARTBEAT_OK"}

    def sanitize_assistant_text(value):
        lines = []
        for raw_line in str(value or "").splitlines():
            normalized_line = re.sub(r"\s+", " ", raw_line.strip())
            if normalized_line.upper() in placeholder_texts:
                continue
            lines.append(raw_line.rstrip())
        return "\n".join(lines).strip()

    lines = Path(transcript_path).read_text(encoding="utf-8", errors="replace").splitlines()
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        entry_type = entry.get("type")
        if entry_type == "model_change":
            meta["model"] = entry.get("modelId", "") or meta["model"]
            meta["provider"] = entry.get("provider", "") or meta["provider"]
            continue
        if entry_type == "thinking_level_change":
            meta["thinkingLevel"] = entry.get("thinkingLevel", "") or meta["thinkingLevel"]
            continue
        if entry_type != "message":
            continue

        payload = entry.get("message", {}) if isinstance(entry.get("message"), dict) else {}
        role = payload.get("role", "")
        timestamp = entry.get("timestamp") or payload.get("timestamp") or ""
        content = payload.get("content", []) if isinstance(payload.get("content"), list) else []

        if role in {"user", "assistant"}:
            text = extract_text_from_content(content)
            if text:
                if role == "assistant":
                    text = sanitize_assistant_text(text)
                normalized_text = re.sub(r"\s+", " ", str(text or "").strip())
                if role == "assistant" and normalized_text.upper() in placeholder_texts:
                    continue
                if role == "assistant" and not normalized_text:
                    continue
                items.append(
                    {
                        "id": entry.get("id", ""),
                        "kind": role,
                        "title": "用户" if role == "user" else "Agent",
                        "text": text,
                        "at": timestamp,
                    }
                )
                preview = text
                if role == "user":
                    user_messages += 1
                else:
                    assistant_messages += 1
            for part in content:
                if not isinstance(part, dict) or part.get("type") != "toolCall":
                    continue
                tool_messages += 1
                items.append(
                    {
                        "id": part.get("id", entry.get("id", "")),
                        "kind": "tool_call",
                        "title": f"调用工具 · {part.get('name', 'unknown')}",
                        "text": summarize_json(part.get("arguments", {})),
                        "at": timestamp,
                    }
                )
            continue

        if role == "toolResult":
            tool_messages += 1
            text = extract_text_from_content(content) or summarize_json(payload.get("details", {}))
            items.append(
                {
                    "id": entry.get("id", ""),
                    "kind": "tool_result",
                    "title": f"工具结果 · {payload.get('toolName', 'unknown')}",
                    "text": text or "工具没有返回可展示文本。",
                    "at": timestamp,
                    "error": bool(payload.get("isError")),
                }
            )
            if text:
                preview = text

    if limit and len(items) > limit:
        items = items[-limit:]
    return {
        "items": items,
        "preview": preview,
        "meta": meta,
        "stats": {
            "turns": user_messages + assistant_messages,
            "userMessages": user_messages,
            "assistantMessages": assistant_messages,
            "toolMessages": tool_messages,
        },
    }


def latest_assistant_transcript_text(transcript):
    items = safe_list((transcript or {}).get("items"))
    for item in reversed(items):
        if str(item.get("kind") or "").strip() != "assistant":
            continue
        text = str(item.get("text") or "").strip()
        if text:
            return text
    return ""


def transcript_quality_snapshot(transcript):
    items = safe_list((transcript or {}).get("items"))
    response_samples = []
    pending_user_at = None
    tool_results = 0
    tool_failures = 0
    pressure_hits = 0
    for item in items:
        kind = str(item.get("kind") or "").strip().lower()
        text = str(item.get("text") or "")
        at = parse_iso(item.get("at"))
        if kind == "user":
            pending_user_at = at
            lowered = text.lower()
            if any(keyword in lowered for keyword in CONVERSATION_PRESSURE_KEYWORDS):
                pressure_hits += 1
        elif kind == "assistant" and pending_user_at and at:
            response_samples.append(max((at - pending_user_at).total_seconds(), 0))
            pending_user_at = None
        elif kind == "tool_result":
            tool_results += 1
            if item.get("error"):
                tool_failures += 1
    avg_response_seconds = round(sum(response_samples) / len(response_samples), 1) if response_samples else 0.0
    success_results = max(tool_results - tool_failures, 0)
    tool_success_rate = round((success_results / tool_results) * 100) if tool_results else 100
    return {
        "avgResponseSeconds": avg_response_seconds,
        "responseSamples": response_samples,
        "toolResultCount": tool_results,
        "toolFailureCount": tool_failures,
        "toolSuccessRate": tool_success_rate,
        "pressureHits": pressure_hits,
    }
