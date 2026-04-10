"""Runtime part: context_hub."""

def context_hub_bin():
    explicit = str(os.environ.get("CHUB_BIN", "")).strip()
    if explicit:
        return explicit
    return shutil.which("chub") or ""


def parse_context_hub_version(text):
    for line in str(text or "").splitlines():
        line = line.strip()
        if "Context Hub CLI v" in line:
            return line.rsplit("v", 1)[-1].strip()
    return ""


def context_hub_config_path(openclaw_dir=None):
    home_path = Path.home() / ".chub" / "config.yaml"
    if not openclaw_dir:
        return home_path
    managed_path = Path(openclaw_dir).expanduser().resolve() / ".chub" / "config.yaml"
    if managed_path.exists() or not home_path.exists():
        return managed_path
    return home_path


def context_hub_annotations_path():
    return Path.home() / ".chub" / "annotations"


def context_hub_sources_root(openclaw_dir=None):
    home_root = Path.home() / ".chub" / "sources"
    if not openclaw_dir:
        return home_root
    managed_root = Path(openclaw_dir).expanduser().resolve() / ".chub" / "sources"
    if managed_root.exists() or not home_root.exists():
        return managed_root
    return home_root


def summarize_context_hub_config(path):
    summary = {
        "path": str(path),
        "exists": path.exists(),
        "sourceCount": 0,
        "sourcePolicy": "",
        "refreshInterval": "",
        "telemetry": "",
        "feedback": "",
    }
    if not path.exists():
        return summary
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("- name:"):
                summary["sourceCount"] += 1
            elif line.startswith("source:"):
                summary["sourcePolicy"] = line.split(":", 1)[1].strip().strip('"')
            elif line.startswith("refresh_interval:"):
                summary["refreshInterval"] = line.split(":", 1)[1].strip()
            elif line.startswith("telemetry:"):
                summary["telemetry"] = line.split(":", 1)[1].strip()
            elif line.startswith("feedback:"):
                summary["feedback"] = line.split(":", 1)[1].strip()
    except OSError:
        return summary
    return summary


def run_context_hub_command(args, cwd=None):
    binary = context_hub_bin()
    if not binary:
        raise RuntimeError("当前环境未安装 Context Hub CLI（chub）。")
    return run_command([binary, *[str(arg) for arg in args]], cwd=cwd)


def normalize_context_hub_language(value):
    normalized = str(value or "").strip().lower()
    aliases = {
        "py": "python",
        "python": "python",
        "js": "javascript",
        "javascript": "javascript",
        "ts": "typescript",
        "typescript": "typescript",
        "rb": "ruby",
        "ruby": "ruby",
        "cs": "csharp",
        "csharp": "csharp",
        "go": "go",
    }
    return aliases.get(normalized, normalized)


def find_context_hub_registry_entry(entry_id, openclaw_dir=None):
    normalized_id = str(entry_id or "").strip()
    if not normalized_id:
        return None
    sources_root = context_hub_sources_root(openclaw_dir)
    if not sources_root.exists():
        return None
    for registry_path in sorted(sources_root.glob("*/registry.json")):
        try:
            payload = json.loads(registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        docs = safe_list(payload.get("docs"))
        skills = safe_list(payload.get("skills"))
        for item in [*docs, *skills]:
            if str(item.get("id") or "").strip() == normalized_id:
                return item
    return None


def iter_context_hub_registry_entries(openclaw_dir=None):
    sources_root = context_hub_sources_root(openclaw_dir)
    if not sources_root.exists():
        return []
    items = []
    for registry_path in sorted(sources_root.glob("*/registry.json")):
        source_name = registry_path.parent.name
        try:
            payload = json.loads(registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for doc in safe_list(payload.get("docs")):
            items.append({**doc, "_source": source_name, "_type": "doc"})
        for skill in safe_list(payload.get("skills")):
            items.append({**skill, "_source": source_name, "_type": "skill"})
    return items


def choose_context_hub_language(entry, requested=""):
    requested_normalized = normalize_context_hub_language(requested)
    languages = safe_list((entry or {}).get("languages"))
    if not languages:
        return requested_normalized
    available = []
    for item in languages:
        language = normalize_context_hub_language(item.get("language"))
        if language:
            available.append(language)
    if requested_normalized and requested_normalized in available:
        return requested_normalized
    for preferred in ("python", "javascript", "typescript", "go"):
        if preferred in available:
            return preferred
    return available[0] if available else requested_normalized


def search_context_hub_registry(query, lang="", tags="", limit=8, openclaw_dir=None):
    normalized_query = str(query or "").strip().lower()
    if not normalized_query:
        return []
    normalized_lang = normalize_context_hub_language(lang)
    tag_filters = [item.strip().lower() for item in str(tags or "").split(",") if item.strip()]
    words = [item for item in re.split(r"\s+", normalized_query) if item]
    results = []
    for entry in iter_context_hub_registry_entries(openclaw_dir=openclaw_dir):
        entry_tags = [str(item).strip().lower() for item in safe_list(entry.get("tags")) if str(item).strip()]
        if tag_filters and not all(tag in entry_tags for tag in tag_filters):
            continue
        if normalized_lang:
            available_languages = [
                normalize_context_hub_language(item.get("language"))
                for item in safe_list(entry.get("languages"))
                if normalize_context_hub_language(item.get("language"))
            ]
            if available_languages and normalized_lang not in available_languages:
                continue
            if entry.get("_type") != "doc" and not available_languages:
                continue
        entry_id = str(entry.get("id") or "").strip().lower()
        entry_name = str(entry.get("name") or "").strip().lower()
        entry_desc = str(entry.get("description") or "").strip().lower()
        score = 0
        if entry_id == normalized_query:
            score += 100
        elif normalized_query in entry_id:
            score += 50
        if entry_name == normalized_query:
            score += 80
        elif normalized_query in entry_name:
            score += 40
        for word in words:
            if word in entry_id:
                score += 10
            if word in entry_name:
                score += 10
            if word in entry_desc:
                score += 5
            if any(word in tag for tag in entry_tags):
                score += 15
        if score <= 0:
            continue
        results.append({**entry, "_score": score})
    results.sort(key=lambda item: (-int(item.get("_score") or 0), str(item.get("id") or "")))
    return results[: max(int(limit or 8), 1)]


def load_context_hub_data(openclaw_dir, agent_cards=None, router_agent_id=""):
    def build():
        config = load_config(openclaw_dir)
        binary = context_hub_bin()
        config_path = context_hub_config_path(openclaw_dir)
        annotations_path = context_hub_annotations_path()
        config_summary = summarize_context_hub_config(config_path)
        data = {
            "supported": True,
            "installed": bool(binary),
            "binary": binary,
            "version": "",
            "status": "warning",
            "config": config_summary,
            "cache": {"exists": False, "sources": []},
            "annotations": {
                "path": str(annotations_path),
                "total": 0,
                "items": [],
            },
            "recommended": [
                {"label": "OpenAI SDK", "query": "openai", "id": "openai/chat"},
                {"label": "Browser automation", "query": "browser automation", "id": "playwright"},
                {"label": "Stripe payments", "query": "stripe payments", "id": "stripe/api"},
                {"label": "Supabase", "query": "supabase", "id": "supabase"},
            ],
            "agentMemory": load_agent_memory_data(openclaw_dir, config, agent_cards=agent_cards),
            "sharedContext": load_shared_context_data(openclaw_dir, config, router_agent_id=router_agent_id, agent_cards=agent_cards),
            "commands": [
                {
                    "label": "Search docs",
                    "command": "chub search openai --json",
                    "description": "搜索最新可用文档和技能。",
                },
                {
                    "label": "Fetch a doc",
                    "command": "chub get openai/chat --lang py --json",
                    "description": "抓取指定文档正文，支持语言和增量文件。",
                },
                {
                    "label": "List annotations",
                    "command": "chub annotate --list --json",
                    "description": "查看本机已经积累的注释记忆。",
                },
                {
                    "label": "Refresh registry",
                    "command": "chub update --json",
                    "description": "更新 Context Hub 本地 registry 缓存。",
                },
            ],
        }
        if not binary:
            return data

        help_result = run_context_hub_command(["help"])
        data["version"] = parse_context_hub_version(help_result.stdout or help_result.stderr)

        cache_result = run_context_hub_command(["cache", "status", "--json"])
        cache_payload = parse_json_payload(cache_result.stdout, cache_result.stderr, default={"exists": False, "sources": []})
        if isinstance(cache_payload, dict):
            data["cache"] = cache_payload

        annotations_result = run_context_hub_command(["annotate", "--list", "--json"])
        annotation_items = parse_json_payload(annotations_result.stdout, annotations_result.stderr, default=[])
        if isinstance(annotation_items, list):
            data["annotations"] = {
                "path": str(annotations_path),
                "total": len(annotation_items),
                "items": annotation_items[:20],
            }

        data["status"] = "ready" if data["installed"] else "warning"
        return data

    return cached_payload(("context-hub", str(Path(openclaw_dir).expanduser().resolve())), 10, build)


def load_context_hub_summary_data(openclaw_dir, router_agent_id=""):
    openclaw_dir = Path(openclaw_dir).expanduser().resolve()

    def build():
        config = load_config(openclaw_dir)
        binary = context_hub_bin()
        config_path = context_hub_config_path(openclaw_dir)
        annotations_path = context_hub_annotations_path()
        config_summary = summarize_context_hub_config(config_path)
        data = {
            "supported": True,
            "installed": bool(binary),
            "binary": binary,
            "version": "",
            "status": "warning",
            "config": config_summary,
            "cache": {"exists": False, "sources": []},
            "annotations": {
                "path": str(annotations_path),
                "total": 0,
                "items": [],
            },
            "recommended": [
                {"label": "OpenAI SDK", "query": "openai", "id": "openai/chat"},
                {"label": "Browser automation", "query": "browser automation", "id": "playwright"},
                {"label": "Stripe payments", "query": "stripe payments", "id": "stripe/api"},
                {"label": "Supabase", "query": "supabase", "id": "supabase"},
            ],
            "agentMemory": load_agent_memory_summary_data(openclaw_dir, config),
            "sharedContext": load_shared_context_summary_data(openclaw_dir, config, router_agent_id=router_agent_id),
            "commands": [
                {
                    "label": "Search docs",
                    "command": "chub search openai --json",
                    "description": "搜索最新可用文档和技能。",
                },
                {
                    "label": "Fetch a doc",
                    "command": "chub get openai/chat --lang py --json",
                    "description": "抓取指定文档正文，支持语言和增量文件。",
                },
                {
                    "label": "List annotations",
                    "command": "chub annotate --list --json",
                    "description": "查看本机已经积累的注释记忆。",
                },
                {
                    "label": "Refresh registry",
                    "command": "chub update --json",
                    "description": "更新 Context Hub 本地 registry 缓存。",
                },
            ],
        }
        if not binary:
            return data

        help_result = run_context_hub_command(["help"])
        data["version"] = parse_context_hub_version(help_result.stdout or help_result.stderr)

        cache_result = run_context_hub_command(["cache", "status", "--json"])
        cache_payload = parse_json_payload(cache_result.stdout, cache_result.stderr, default={"exists": False, "sources": []})
        if isinstance(cache_payload, dict):
            data["cache"] = cache_payload

        annotations_result = run_context_hub_command(["annotate", "--list", "--json"])
        annotation_items = parse_json_payload(annotations_result.stdout, annotations_result.stderr, default=[])
        if isinstance(annotation_items, list):
            data["annotations"] = {
                "path": str(annotations_path),
                "total": len(annotation_items),
                "items": annotation_items[:20],
            }

        data["status"] = "ready" if data["installed"] else "warning"
        return data

    return cached_payload(("context-hub-summary", str(openclaw_dir)), 10, build)


def perform_context_hub_install():
    process = run_command(["npm", "install", "-g", "@aisuite/chub"])
    output = join_command_output(process)
    if process.returncode != 0:
        raise RuntimeError(output or "安装 Context Hub CLI 失败。")
    help_result = run_context_hub_command(["help"])
    return {
        "installed": True,
        "version": parse_context_hub_version(help_result.stdout or help_result.stderr),
        "output": output or join_command_output(help_result),
    }


def perform_context_hub_update():
    process = run_context_hub_command(["update", "--json"])
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    output = join_command_output(process)
    if process.returncode != 0 and payload is None:
        raise RuntimeError(output or "更新 Context Hub registry 失败。")
    return {"payload": payload, "output": output}


def perform_context_hub_search(query, lang="", tags="", limit=8, openclaw_dir=None):
    normalized_query = str(query or "").strip()
    if not normalized_query:
        raise RuntimeError("请先输入要检索的内容。")
    args = ["search", normalized_query, "--json", "--limit", str(limit or 8)]
    if lang:
        args.extend(["--lang", lang])
    if tags:
        args.extend(["--tags", tags])
    process = run_context_hub_command(args)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    fallback_results = search_context_hub_registry(
        normalized_query,
        lang=lang,
        tags=tags,
        limit=limit,
        openclaw_dir=openclaw_dir,
    )
    if isinstance(payload, dict) and safe_list(payload.get("results")):
        return payload
    if fallback_results:
        return {
            "results": fallback_results,
            "total": len(fallback_results),
            "query": normalized_query,
            "fallback": True,
        }
    if process.returncode != 0 or not isinstance(payload, dict):
        raise RuntimeError(join_command_output(process) or "Context Hub 搜索失败。")
    return payload


def perform_context_hub_get(entry_id, lang="", full=False, files="", openclaw_dir=None):
    normalized_id = str(entry_id or "").strip()
    if not normalized_id:
        raise RuntimeError("请先输入要获取的文档 ID。")
    entry = find_context_hub_registry_entry(normalized_id, openclaw_dir=openclaw_dir)
    selected_lang = choose_context_hub_language(entry, requested=lang)
    args = ["get", normalized_id, "--json"]
    if selected_lang:
        args.extend(["--lang", selected_lang])
    if files:
        args.extend(["--file", files])
    process = run_context_hub_command(args)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if process.returncode != 0 or not isinstance(payload, dict):
        raise RuntimeError(join_command_output(process) or f"获取 Context Hub 文档失败：{normalized_id}")
    payload["requestedFull"] = bool(full)
    if selected_lang and not payload.get("language"):
        payload["language"] = selected_lang
    return payload


def perform_context_hub_annotate(entry_id, note="", clear=False):
    normalized_id = str(entry_id or "").strip()
    if not normalized_id:
        raise RuntimeError("请先输入要标注的文档 ID。")
    args = ["annotate", normalized_id]
    if clear:
        args.append("--clear")
    else:
        normalized_note = str(note or "").strip()
        if not normalized_note:
            raise RuntimeError("请先输入要保存的 annotation。")
        args.append(normalized_note)
    args.append("--json")
    process = run_context_hub_command(args)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if process.returncode != 0 or not isinstance(payload, dict):
        raise RuntimeError(join_command_output(process) or f"保存 Context Hub annotation 失败：{normalized_id}")
    return payload


def perform_context_hub_feedback(entry_id, rating, comment="", labels=None, lang="", file_path="", agent="", model=""):
    normalized_id = str(entry_id or "").strip()
    normalized_rating = str(rating or "").strip().lower()
    if not normalized_id:
        raise RuntimeError("请先输入要反馈的文档 ID。")
    if normalized_rating not in {"up", "down"}:
        raise RuntimeError("反馈只能是 up 或 down。")
    args = ["feedback", normalized_id, normalized_rating]
    normalized_comment = str(comment or "").strip()
    if normalized_comment:
        args.append(normalized_comment)
    for label in labels or []:
        if str(label).strip():
            args.extend(["--label", str(label).strip()])
    if lang:
        args.extend(["--lang", lang])
    if file_path:
        args.extend(["--file", file_path])
    if agent:
        args.extend(["--agent", agent])
    if model:
        args.extend(["--model", model])
    args.append("--json")
    process = run_context_hub_command(args)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    if process.returncode != 0 or not isinstance(payload, dict):
        raise RuntimeError(join_command_output(process) or f"发送 Context Hub feedback 失败：{normalized_id}")
    if payload.get("status") == "error":
        raise RuntimeError(payload.get("reason") or "发送 Context Hub feedback 失败。")
    return payload


def invalidate_context_hub_payload_cache(openclaw_dir):
    raw_openclaw_dir = str(openclaw_dir)
    normalized_openclaw_dir = str(Path(openclaw_dir).expanduser().resolve())
    with BUNDLE_CACHE_LOCK:
        PAYLOAD_CACHE.pop(("context-hub", raw_openclaw_dir), None)
        PAYLOAD_CACHE.pop(("context-hub", normalized_openclaw_dir), None)
