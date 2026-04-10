"""Agent Harness — Context compaction system.

Implements the "Compact" pattern from Claude Code: when an agent's
conversation history approaches the context window limit, the system
uses a separate LLM call to summarize older messages while preserving
critical context (active files, tasks, decisions).

This prevents context overflow and enables arbitrarily long agent sessions.

Usage::

    from backend.domain.core.context_compact import ContextCompactor

    compactor = ContextCompactor.default()

    # Check if compaction is needed
    if compactor.should_compact(messages, model="claude-sonnet-4-20250514"):
        result = compactor.compact(messages, agent_id="eng-01")
        messages = result.compacted_messages

    # Manual compaction
    result = compactor.compact(messages, agent_id="eng-01")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ── Model context limits ────────────────────────────────────────────────

MODEL_CONTEXT_WINDOWS: dict[str, int] = {
    "claude-sonnet-4-20250514": 200_000,
    "claude-3-5-sonnet": 200_000,
    "claude-3-5-haiku": 200_000,
    "claude-3-opus": 200_000,
    "claude-opus-4-6": 200_000,
    "gpt-4o": 128_000,
    "gpt-4o-mini": 128_000,
    "gpt-4-turbo": 128_000,
    "o1": 200_000,
    "o1-mini": 128_000,
    "_default": 128_000,
}

# Approximate chars per token (conservative estimate for mixed content)
CHARS_PER_TOKEN = 3.5


def estimate_tokens(text: str) -> int:
    """Rough token estimate from character count."""
    return int(len(text) / CHARS_PER_TOKEN)


def estimate_messages_tokens(messages: list[dict]) -> int:
    """Estimate total tokens across all messages."""
    total = 0
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            total += estimate_tokens(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    total += estimate_tokens(str(block.get("text", "")))
                else:
                    total += estimate_tokens(str(block))
        # Message metadata overhead
        total += 10
    return total


# ── Data structures ────────────────────────────────────────────────────


class CompactStrategy(str, Enum):
    """How to select which messages to summarize."""

    OLDEST_FIRST = "oldest_first"       # summarize oldest messages
    SLIDING_WINDOW = "sliding_window"   # keep a fixed window of recent messages
    IMPORTANCE_BASED = "importance_based"  # keep high-importance messages


@dataclass
class PreservedContext:
    """Critical context that must survive compaction."""

    active_files: list[str] = field(default_factory=list)
    active_tasks: list[dict] = field(default_factory=list)
    key_decisions: list[str] = field(default_factory=list)
    tool_results: list[dict] = field(default_factory=list)
    user_preferences: dict[str, Any] = field(default_factory=dict)

    def to_summary_text(self) -> str:
        parts = []
        if self.active_files:
            parts.append(f"Active files: {', '.join(self.active_files[:20])}")
        if self.active_tasks:
            task_lines = [
                f"- [{t.get('status', '?')}] {t.get('subject', '?')}"
                for t in self.active_tasks[:10]
            ]
            parts.append("Active tasks:\n" + "\n".join(task_lines))
        if self.key_decisions:
            parts.append("Key decisions:\n" + "\n".join(
                f"- {d}" for d in self.key_decisions[:10]
            ))
        return "\n\n".join(parts)


@dataclass
class CompactResult:
    """Result of a compaction operation."""

    compacted_messages: list[dict]
    summary_text: str
    preserved: PreservedContext
    original_count: int
    compacted_count: int
    tokens_before: int
    tokens_after: int
    duration_ms: float = 0.0

    @property
    def tokens_saved(self) -> int:
        return self.tokens_before - self.tokens_after

    @property
    def compression_ratio(self) -> float:
        if self.tokens_before == 0:
            return 0.0
        return round(self.tokens_after / self.tokens_before, 3)

    def to_dict(self) -> dict[str, Any]:
        return {
            "original_count": self.original_count,
            "compacted_count": self.compacted_count,
            "tokens_before": self.tokens_before,
            "tokens_after": self.tokens_after,
            "tokens_saved": self.tokens_saved,
            "compression_ratio": self.compression_ratio,
            "duration_ms": self.duration_ms,
            "active_files": self.preserved.active_files[:10],
            "active_tasks_count": len(self.preserved.active_tasks),
        }


# ── Context extractor ──────────────────────────────────────────────────


def extract_preserved_context(messages: list[dict]) -> PreservedContext:
    """Extract critical context from message history that must survive compaction."""
    ctx = PreservedContext()

    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, list):
            content = " ".join(
                str(b.get("text", "")) if isinstance(b, dict) else str(b)
                for b in content
            )

        role = msg.get("role", "")
        content_str = str(content)

        # Extract file references
        import re
        file_patterns = re.findall(
            r'(?:reading|wrote|edited|created|modified)\s+[`"]?([^\s`"]+\.[a-zA-Z]{1,5})[`"]?',
            content_str, re.IGNORECASE,
        )
        ctx.active_files.extend(file_patterns)

        # Extract task references
        task_match = re.findall(
            r'(?:task|任务)\s*[#:]?\s*(\d+)\s*[-:]\s*(.+?)(?:\n|$)',
            content_str, re.IGNORECASE,
        )
        for task_id, subject in task_match:
            ctx.active_tasks.append({"id": task_id, "subject": subject.strip()})

        # Extract key decisions (lines with decision-like language)
        decision_patterns = re.findall(
            r'(?:decided|decision|chose|选择|决定|确定)[:：]\s*(.+?)(?:\n|$)',
            content_str, re.IGNORECASE,
        )
        ctx.key_decisions.extend(decision_patterns)

        # Extract tool results (keep recent ones)
        if msg.get("role") == "tool" or msg.get("type") == "tool_result":
            ctx.tool_results.append({
                "tool": msg.get("name", msg.get("tool_name", "unknown")),
                "summary": content_str[:200],
            })

    # Deduplicate
    ctx.active_files = list(dict.fromkeys(ctx.active_files))[-20:]
    ctx.key_decisions = list(dict.fromkeys(ctx.key_decisions))[-10:]
    ctx.tool_results = ctx.tool_results[-10:]

    return ctx


# ── Compact prompt builder ──────────────────────────────────────────────


COMPACT_SYSTEM_PROMPT = """You are a conversation summarizer. Your job is to create a concise but comprehensive summary of a conversation between a user and an AI assistant.

Rules:
1. Preserve ALL factual information, decisions made, and action items
2. Preserve file paths, code snippets, and technical details that were discussed
3. Preserve task status and progress
4. Remove chitchat, repeated explanations, and verbose tool outputs
5. Format as a structured summary with sections
6. Write in the same language as the original conversation
7. Keep the summary under 2000 words

Output format:
## Conversation Summary

### Context
(What was the user working on)

### Actions Taken
(What was done, in order)

### Key Decisions
(Important choices made)

### Current State
(Where things stand now)

### Open Items
(What still needs to be done)
"""


def build_compact_prompt(messages: list[dict], preserved: PreservedContext) -> str:
    """Build the prompt for the compaction LLM call."""
    conversation_text = []
    for msg in messages:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        if isinstance(content, list):
            content = "\n".join(
                str(b.get("text", "")) if isinstance(b, dict) else str(b)
                for b in content
            )
        conversation_text.append(f"[{role}]: {content}")

    full_text = "\n\n".join(conversation_text)

    # Add preserved context hints
    preserved_hints = preserved.to_summary_text()
    if preserved_hints:
        full_text += f"\n\n---\nIMPORTANT CONTEXT TO PRESERVE:\n{preserved_hints}"

    return full_text


# ── Context Compactor ───────────────────────────────────────────────────


class ContextCompactor:
    """Manages context window compaction for long agent sessions.

    When conversation history approaches the model's context limit,
    the compactor summarizes older messages into a compact representation
    while preserving critical context.
    """

    _default: Optional["ContextCompactor"] = None

    def __init__(
        self,
        *,
        threshold_ratio: float = 0.75,
        keep_recent: int = 10,
        strategy: CompactStrategy = CompactStrategy.SLIDING_WINDOW,
    ) -> None:
        self._threshold_ratio = threshold_ratio  # compact when usage > this ratio
        self._keep_recent = keep_recent           # always keep this many recent messages
        self._strategy = strategy
        self._hook_registry = None
        self._compact_history: list[dict] = []
        self._max_history = 50

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "ContextCompactor":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    def attach_hooks(self, registry) -> None:
        self._hook_registry = registry

    # ── Should compact? ─────────────────────────────────────────────

    def should_compact(
        self,
        messages: list[dict],
        *,
        model: str = "claude-sonnet-4-20250514",
    ) -> bool:
        """Check whether the conversation needs compaction."""
        if len(messages) <= self._keep_recent + 2:
            return False

        window = MODEL_CONTEXT_WINDOWS.get(model, MODEL_CONTEXT_WINDOWS["_default"])
        tokens = estimate_messages_tokens(messages)
        usage_ratio = tokens / window

        return usage_ratio > self._threshold_ratio

    def usage_info(
        self,
        messages: list[dict],
        *,
        model: str = "claude-sonnet-4-20250514",
    ) -> dict[str, Any]:
        """Return context usage statistics."""
        window = MODEL_CONTEXT_WINDOWS.get(model, MODEL_CONTEXT_WINDOWS["_default"])
        tokens = estimate_messages_tokens(messages)
        return {
            "model": model,
            "context_window": window,
            "estimated_tokens": tokens,
            "usage_ratio": round(tokens / window, 3),
            "should_compact": tokens / window > self._threshold_ratio,
            "message_count": len(messages),
        }

    # ── Compact ─────────────────────────────────────────────────────

    def compact(
        self,
        messages: list[dict],
        *,
        agent_id: str = "",
        summarizer: Any = None,
    ) -> CompactResult:
        """Compact conversation history.

        Parameters
        ----------
        messages
            The full message list to compact.
        agent_id
            Agent that owns this conversation (for logging).
        summarizer
            Optional callable(system_prompt, user_prompt) -> summary_text.
            If not provided, uses a rule-based fallback.
        """
        start_time = time.time()
        tokens_before = estimate_messages_tokens(messages)

        # Split: old messages to summarize vs recent to keep
        split_point = max(0, len(messages) - self._keep_recent)

        # Always keep system messages at the start
        system_messages = []
        compactable = []
        for msg in messages[:split_point]:
            if msg.get("role") == "system":
                system_messages.append(msg)
            else:
                compactable.append(msg)

        recent_messages = messages[split_point:]

        # Extract preserved context from ALL messages
        preserved = extract_preserved_context(messages)

        # Generate summary
        if summarizer and compactable:
            compact_input = build_compact_prompt(compactable, preserved)
            try:
                summary_text = summarizer(COMPACT_SYSTEM_PROMPT, compact_input)
            except Exception:
                logger.exception("LLM summarizer failed, using fallback")
                summary_text = self._fallback_summary(compactable, preserved)
        elif compactable:
            summary_text = self._fallback_summary(compactable, preserved)
        else:
            summary_text = ""

        # Build compacted message list
        compacted: list[dict] = list(system_messages)

        if summary_text:
            compacted.append({
                "role": "assistant",
                "content": f"[Conversation Summary — compacted from {len(compactable)} messages]\n\n{summary_text}",
                "metadata": {
                    "compacted": True,
                    "original_count": len(compactable),
                    "compacted_at": time.time(),
                },
            })

        compacted.extend(recent_messages)

        tokens_after = estimate_messages_tokens(compacted)
        duration_ms = round((time.time() - start_time) * 1000, 1)

        result = CompactResult(
            compacted_messages=compacted,
            summary_text=summary_text,
            preserved=preserved,
            original_count=len(messages),
            compacted_count=len(compacted),
            tokens_before=tokens_before,
            tokens_after=tokens_after,
            duration_ms=duration_ms,
        )

        # Log
        self._compact_history.append({
            "timestamp": time.time(),
            "agent_id": agent_id,
            "tokens_saved": result.tokens_saved,
            "compression_ratio": result.compression_ratio,
            "original_count": result.original_count,
            "compacted_count": result.compacted_count,
        })
        if len(self._compact_history) > self._max_history:
            self._compact_history = self._compact_history[-self._max_history:]

        logger.info(
            "Compacted %d→%d messages, %d→%d tokens (%.1f%% saved) for agent=%s",
            result.original_count, result.compacted_count,
            tokens_before, tokens_after,
            (1 - result.compression_ratio) * 100,
            agent_id,
        )

        return result

    # ── Fallback summary ────────────────────────────────────────────

    def _fallback_summary(
        self,
        messages: list[dict],
        preserved: PreservedContext,
    ) -> str:
        """Rule-based summary when no LLM summarizer is available."""
        parts = ["## Conversation Summary (auto-compacted)\n"]

        # Count messages by role
        role_counts: dict[str, int] = {}
        for msg in messages:
            role = msg.get("role", "unknown")
            role_counts[role] = role_counts.get(role, 0) + 1

        parts.append(f"**Messages compacted:** {len(messages)} "
                      f"({', '.join(f'{v} {k}' for k, v in role_counts.items())})\n")

        # Include preserved context
        ctx_text = preserved.to_summary_text()
        if ctx_text:
            parts.append(f"### Preserved Context\n{ctx_text}\n")

        # Last few user messages as "recent topics"
        user_msgs = [m for m in messages if m.get("role") == "user"]
        if user_msgs:
            parts.append("### Recent Topics")
            for msg in user_msgs[-5:]:
                content = msg.get("content", "")
                if isinstance(content, str):
                    parts.append(f"- {content[:100]}")

        return "\n".join(parts)

    # ── Introspection ───────────────────────────────────────────────

    @property
    def compact_history(self) -> list[dict]:
        return list(self._compact_history)

    def __repr__(self) -> str:
        return f"<ContextCompactor compactions={len(self._compact_history)}>"
