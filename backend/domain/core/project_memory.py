"""Agent Harness — Project Memory system (CONTEXT.md discovery).

Implements the CLAUDE.md-style project memory pattern: each workspace
(and optionally each tenant / global scope) can have a CONTEXT.md file
that is automatically discovered and injected into the agent's system
prompt, providing project-specific instructions, conventions, and facts.

Three-layer inheritance::

    Global  (~/.openclaw/CONTEXT.md)
    └── Tenant  (<tenant_dir>/CONTEXT.md)
        └── Workspace  (<workspace>/CONTEXT.md, <workspace>/.context/*.md)

Lower layers override / extend higher layers.

Usage::

    from backend.domain.core.project_memory import ProjectMemory

    pm = ProjectMemory.default()
    context = pm.resolve("/path/to/workspace", tenant_id="tenant-01")
    # context.full_text  -> merged content
    # context.layers     -> per-layer detail
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Constants ───────────────────────────────────────────────────────────

CONTEXT_FILENAME = "CONTEXT.md"
CONTEXT_DIR_NAME = ".context"
DEFAULT_GLOBAL_DIR = os.path.expanduser("~/.openclaw")

LAYER_NAMES = ("global", "tenant", "workspace")


# ── Data structures ────────────────────────────────────────────────────


@dataclass
class MemoryLayer:
    """One layer of project memory."""

    name: str           # "global", "tenant", or "workspace"
    source_path: str    # absolute path to the file
    content: str        # raw file content
    loaded_at: float = field(default_factory=time.time)
    size_bytes: int = 0

    @property
    def token_estimate(self) -> int:
        return int(len(self.content) / 3.5)


@dataclass
class ResolvedContext:
    """Merged project memory across all layers."""

    layers: list[MemoryLayer]
    workspace_path: str
    tenant_id: str = ""

    @property
    def full_text(self) -> str:
        """Merge all layers with section headers."""
        parts = []
        for layer in self.layers:
            parts.append(f"<!-- [project-memory:{layer.name}] {layer.source_path} -->\n{layer.content}")
        return "\n\n---\n\n".join(parts)

    @property
    def total_tokens(self) -> int:
        return sum(l.token_estimate for l in self.layers)

    @property
    def layer_names(self) -> list[str]:
        return [l.name for l in self.layers]

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace": self.workspace_path,
            "tenant_id": self.tenant_id,
            "layer_count": len(self.layers),
            "total_tokens": self.total_tokens,
            "layers": [
                {
                    "name": l.name,
                    "source": l.source_path,
                    "size_bytes": l.size_bytes,
                    "token_estimate": l.token_estimate,
                }
                for l in self.layers
            ],
        }


# ── File discovery ─────────────────────────────────────────────────────


def discover_context_files(
    directory: str | Path,
    layer_name: str,
) -> list[MemoryLayer]:
    """Find and load CONTEXT.md and .context/*.md in a directory."""
    directory = Path(directory)
    layers: list[MemoryLayer] = []

    if not directory.is_dir():
        return layers

    # Primary: CONTEXT.md
    primary = directory / CONTEXT_FILENAME
    if primary.is_file():
        try:
            content = primary.read_text(encoding="utf-8")
            layers.append(MemoryLayer(
                name=layer_name,
                source_path=str(primary),
                content=content,
                size_bytes=len(content.encode("utf-8")),
            ))
        except Exception:
            logger.warning("Failed to read %s", primary, exc_info=True)

    # Secondary: .context/*.md (sorted alphabetically)
    context_dir = directory / CONTEXT_DIR_NAME
    if context_dir.is_dir():
        md_files = sorted(context_dir.glob("*.md"))
        for md_file in md_files:
            try:
                content = md_file.read_text(encoding="utf-8")
                layers.append(MemoryLayer(
                    name=f"{layer_name}/{md_file.name}",
                    source_path=str(md_file),
                    content=content,
                    size_bytes=len(content.encode("utf-8")),
                ))
            except Exception:
                logger.warning("Failed to read %s", md_file, exc_info=True)

    return layers


# ── Project Memory Manager ──────────────────────────────────────────────


class ProjectMemory:
    """Discovers and manages CONTEXT.md project memory files.

    Implements a three-layer inheritance:

    1. Global (``~/.openclaw/CONTEXT.md``) — user-wide defaults
    2. Tenant (``<tenant_dir>/CONTEXT.md``) — organization-level config
    3. Workspace (``<workspace>/CONTEXT.md``) — project-specific context

    Supports caching with TTL to avoid re-reading unchanged files.
    """

    _default: Optional["ProjectMemory"] = None

    def __init__(
        self,
        *,
        global_dir: str = DEFAULT_GLOBAL_DIR,
        tenant_base_dir: str | None = None,
        cache_ttl_seconds: float = 60.0,
        max_total_tokens: int = 8000,
    ) -> None:
        self._global_dir = global_dir
        self._tenant_base_dir = tenant_base_dir
        self._cache_ttl = cache_ttl_seconds
        self._max_total_tokens = max_total_tokens
        self._cache: dict[str, tuple[float, ResolvedContext]] = {}
        self._stats = {"hits": 0, "misses": 0, "resolves": 0}

    # ── Singleton ───────────────────────────────────────────────────

    @classmethod
    def default(cls) -> "ProjectMemory":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    @classmethod
    def reset_default(cls) -> None:
        cls._default = None

    # ── Resolve ─────────────────────────────────────────────────────

    def resolve(
        self,
        workspace_path: str,
        *,
        tenant_id: str = "",
    ) -> ResolvedContext:
        """Resolve project memory for a workspace, with caching."""
        cache_key = f"{workspace_path}|{tenant_id}"

        # Check cache
        if cache_key in self._cache:
            cached_time, cached_ctx = self._cache[cache_key]
            if time.time() - cached_time < self._cache_ttl:
                self._stats["hits"] += 1
                return cached_ctx

        self._stats["misses"] += 1
        self._stats["resolves"] += 1

        # Discover layers
        all_layers: list[MemoryLayer] = []

        # Layer 1: Global
        all_layers.extend(discover_context_files(self._global_dir, "global"))

        # Layer 2: Tenant
        if tenant_id and self._tenant_base_dir:
            tenant_dir = os.path.join(self._tenant_base_dir, tenant_id)
            all_layers.extend(discover_context_files(tenant_dir, "tenant"))

        # Layer 3: Workspace
        all_layers.extend(discover_context_files(workspace_path, "workspace"))

        # Trim if total tokens exceed budget
        all_layers = self._trim_layers(all_layers)

        ctx = ResolvedContext(
            layers=all_layers,
            workspace_path=workspace_path,
            tenant_id=tenant_id,
        )

        self._cache[cache_key] = (time.time(), ctx)

        logger.info(
            "Resolved project memory for workspace=%s, tenant=%s: %d layers, ~%d tokens",
            workspace_path, tenant_id or "(none)",
            len(ctx.layers), ctx.total_tokens,
        )

        return ctx

    def _trim_layers(self, layers: list[MemoryLayer]) -> list[MemoryLayer]:
        """Trim layers to fit within max_total_tokens budget.

        Strategy: keep the most specific layers (workspace > tenant > global)
        by reversing priority — drop global first if we're over budget.
        """
        total = sum(l.token_estimate for l in layers)
        if total <= self._max_total_tokens:
            return layers

        # Prioritize: workspace > tenant > global
        priority_order = {"workspace": 0, "tenant": 1, "global": 2}
        sorted_layers = sorted(
            layers,
            key=lambda l: priority_order.get(l.name.split("/")[0], 1),
        )

        result: list[MemoryLayer] = []
        remaining = self._max_total_tokens
        for layer in sorted_layers:
            if layer.token_estimate <= remaining:
                result.append(layer)
                remaining -= layer.token_estimate
            else:
                logger.info(
                    "Trimmed layer %s (%d tokens) — over budget",
                    layer.name, layer.token_estimate,
                )

        return result

    # ── Write ───────────────────────────────────────────────────────

    def write_context(
        self,
        directory: str | Path,
        content: str,
        *,
        filename: str = CONTEXT_FILENAME,
        subdirectory: bool = False,
    ) -> str:
        """Write a CONTEXT.md file.

        Parameters
        ----------
        directory
            The workspace or tenant directory.
        content
            Markdown content to write.
        filename
            Filename (default CONTEXT.md).
        subdirectory
            If True, write into .context/ subdirectory.

        Returns
        -------
        The absolute path of the written file.
        """
        directory = Path(directory)
        if subdirectory:
            target_dir = directory / CONTEXT_DIR_NAME
            target_dir.mkdir(parents=True, exist_ok=True)
            target = target_dir / filename
        else:
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / filename

        target.write_text(content, encoding="utf-8")

        # Invalidate cache for this directory
        self.invalidate_cache(str(directory))

        logger.info("Wrote project memory: %s (%d bytes)", target, len(content))
        return str(target)

    # ── Cache management ────────────────────────────────────────────

    def invalidate_cache(self, workspace_path: str = "") -> int:
        """Invalidate cached contexts. If workspace_path given, only that workspace."""
        if not workspace_path:
            count = len(self._cache)
            self._cache.clear()
            return count

        keys_to_remove = [k for k in self._cache if k.startswith(workspace_path)]
        for k in keys_to_remove:
            del self._cache[k]
        return len(keys_to_remove)

    # ── Introspection ───────────────────────────────────────────────

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def list_cached_workspaces(self) -> list[str]:
        return [k.split("|")[0] for k in self._cache]

    def __repr__(self) -> str:
        return (
            f"<ProjectMemory global={self._global_dir} "
            f"cached={len(self._cache)} resolves={self._stats['resolves']}>"
        )
