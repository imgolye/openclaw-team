from __future__ import annotations

import sys


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


build_team_current_focus_items = _DelegatedSymbol("build_team_current_focus_items")
build_team_open_loop_items = _DelegatedSymbol("build_team_open_loop_items")
clean_unique_strings = _DelegatedSymbol("clean_unique_strings")
compact_task_long_term_memory = _DelegatedSymbol("compact_task_long_term_memory")
coordination_reply_entries = _DelegatedSymbol("coordination_reply_entries")
now_iso = _DelegatedSymbol("now_iso")
safe_list = _DelegatedSymbol("safe_list")
summarize_task_execution_text = _DelegatedSymbol("summarize_task_execution_text")
task_memory_template = _DelegatedSymbol("task_memory_template")
team_policy_payload = _DelegatedSymbol("team_policy_payload")


def seed_task_long_term_memory_payload(title="", remark="", team=None, memory_system=None, task_type=""):
    title_text = summarize_task_execution_text(title or "", limit=80)
    objective = summarize_task_execution_text(remark or title or "", limit=160)
    team_name = str((team or {}).get("name") or "").strip() if isinstance(team, dict) else ""
    template = task_memory_template(memory_system, task_type=task_type)
    summary_parts = clean_unique_strings(
        [
            f"这是一条需要持续推进的任务：{title_text}。" if title_text else "",
            f"当前目标：{objective}" if objective else "",
            f"默认由 {team_name} 持续跟进。" if team_name else "",
            str(template.get("bootstrapNote") or "").strip(),
        ]
    )
    return compact_task_long_term_memory(
        {
            "longTermMemory": " ".join(summary_parts),
            "learningHighlights": [],
            "recentNotes": (
                [
                    {
                        "at": now_iso(),
                        "summary": objective or title_text or "任务已建立，等待后续推进。",
                        "focus": objective or title_text,
                        "ownerLabel": team_name,
                    }
                ]
                if objective or title_text
                else []
            ),
            "updatedAt": now_iso(),
        }
    )


def build_task_long_term_memory_payload(existing_memory, dispatch_state, fallback_title="", fallback_note=""):
    existing = compact_task_long_term_memory(existing_memory)
    dispatch_state = dispatch_state if isinstance(dispatch_state, dict) else {}
    updated_at = str(dispatch_state.get("at") or now_iso()).strip()
    summary_text = summarize_task_execution_text(dispatch_state.get("summaryText") or fallback_note or "", limit=180)
    response_entries = coordination_reply_entries(dispatch_state.get("responses"), limit=4)
    focus_items = build_team_current_focus_items(dispatch_state, limit=3)
    focus_lines = [str(item.get("text") or "").strip() for item in focus_items if isinstance(item, dict) and str(item.get("text") or "").strip()]
    open_loop_items = build_team_open_loop_items(dispatch_state, limit=3)
    learning_candidates = clean_unique_strings(
        [
            *(existing.get("learningHighlights") or []),
            *focus_lines[:2],
            *[
                summarize_task_execution_text((item or {}).get("text") or "", limit=96)
                for item in open_loop_items
                if isinstance(item, dict)
            ][:2],
        ]
    )[:4]
    long_term_parts = clean_unique_strings(
        [
            existing.get("longTermMemory") or "",
            f"持续推进任务：{summarize_task_execution_text(fallback_title or '', limit=80)}。" if fallback_title else "",
            f"最近稳定判断：{summary_text}" if summary_text else "",
        ]
    )
    latest_note = {
        "at": updated_at,
        "summary": summary_text or summarize_task_execution_text(fallback_note or fallback_title or "", limit=140),
        "focus": focus_lines[0] if focus_lines else summarize_task_execution_text(fallback_note or "", limit=96),
        "ownerLabel": str((response_entries[0] or {}).get("agentDisplayName") or "").strip() if response_entries else "",
    }
    merged_recent = []
    seen_recent = set()
    for item in [latest_note, *safe_list(existing.get("recentNotes"))]:
        if not isinstance(item, dict):
            continue
        normalized = {
            "at": str(item.get("at") or "").strip(),
            "summary": summarize_task_execution_text(item.get("summary") or "", limit=140),
            "focus": summarize_task_execution_text(item.get("focus") or "", limit=96),
            "ownerLabel": str(item.get("ownerLabel") or "").strip(),
        }
        if not any(normalized.values()):
            continue
        note_key = (normalized.get("at"), normalized.get("summary") or normalized.get("focus"))
        if note_key in seen_recent:
            continue
        seen_recent.add(note_key)
        merged_recent.append(normalized)
    return {
        "longTermMemory": summarize_task_execution_text(" ".join(long_term_parts), limit=220),
        "learningHighlights": learning_candidates,
        "recentNotes": merged_recent[:4],
        "updatedAt": updated_at,
    }


def merge_team_policy_state(team, existing_policy=None):
    base_policy = team_policy_payload(team if isinstance(team, dict) else {})
    existing_policy = existing_policy if isinstance(existing_policy, dict) else {}
    next_policy = {**base_policy, **existing_policy}
    next_policy["proactiveRules"] = clean_unique_strings(
        existing_policy.get("proactiveRules") if isinstance(existing_policy.get("proactiveRules"), list) else base_policy.get("proactiveRules")
    )
    next_policy["coordinationProtocol"] = (
        existing_policy.get("coordinationProtocol")
        if isinstance(existing_policy.get("coordinationProtocol"), dict)
        else base_policy.get("coordinationProtocol")
    )
    next_policy["taskLongTermMemory"] = compact_task_long_term_memory(existing_policy.get("taskLongTermMemory"))
    return next_policy


def task_long_term_memory_prompt_lines(memory, audience="lead"):
    memory = compact_task_long_term_memory(memory)
    if not any([memory.get("longTermMemory"), memory.get("learningHighlights"), memory.get("recentNotes")]):
        return []
    normalized_audience = str(audience or "").strip().lower()
    highlight_limit = 2 if normalized_audience == "lead" else 1
    recent_limit = 2 if normalized_audience == "lead" else 1
    lines = []
    if memory.get("longTermMemory"):
        lines.append(f"长期记忆：{summarize_task_execution_text(memory.get('longTermMemory'), limit=120)}")
    learning_highlights = [
        summarize_task_execution_text(item, limit=72)
        for item in clean_unique_strings(memory.get("learningHighlights") or [])[:highlight_limit]
    ]
    learning_highlights = [item for item in learning_highlights if item]
    rendered_recent = []
    for item in safe_list(memory.get("recentNotes")):
        if not isinstance(item, dict):
            continue
        summary = summarize_task_execution_text(item.get("summary") or item.get("focus") or "", limit=80)
        if not summary:
            continue
        label = str(item.get("at") or "").strip()
        rendered_recent.append(f"{label + '：' if label else ''}{summary}")
        if len(rendered_recent) >= recent_limit:
            break
    if learning_highlights:
        lines.append(f"最近学到：{'；'.join(learning_highlights)}")
    if rendered_recent:
        lines.append(f"最近推进：{'；'.join(rendered_recent)}")
    if normalized_audience == "lead":
        lines.append("先沿用这份长期记忆，再往下推进，不要每次都像第一次接手；除非对方明确在问历史、复盘或判断依据，否则不要逐条复述。")
    else:
        lines.append("补位时先接住这里已有的判断，再补你的新增动作；默认把这层长期记忆当作你已经记得的背景，不要在可见回复里把它重新讲一遍。")
    return lines
