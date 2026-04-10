"""Concrete ToolExecutor — executes tools within a workspace scope.

Implements the ToolExecutor interface from QueryEngine, dispatching
to actual tool implementations (bash, file ops, search, etc.) with
workspace-scoped path validation.
"""

from __future__ import annotations

import asyncio
import glob as glob_mod
import logging
import os
import re
import subprocess
from pathlib import Path
from typing import Any

from backend.domain.core.file_path_validator import FilePathValidator
from backend.domain.core.query_engine import ToolExecutor

logger = logging.getLogger(__name__)

# Max output length for tool results to prevent context overflow
MAX_RESULT_LENGTH = 30_000
MAX_BASH_TIMEOUT = 120  # seconds


class HarnessToolExecutor(ToolExecutor):
    """Workspace-scoped tool executor.

    All file operations are constrained to the configured workspace roots.
    """

    def __init__(
        self,
        *,
        workspace_path: str = "",
        path_validator: FilePathValidator | None = None,
    ):
        self._workspace = workspace_path or os.getcwd()
        self._validator = path_validator or FilePathValidator(
            allowed_roots=[self._workspace] if self._workspace else []
        )

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def validator(self) -> FilePathValidator:
        return self._validator

    async def execute(
        self,
        tool_name: str,
        tool_args: dict,
        *,
        agent_id: str = "",
        context: dict | None = None,
    ) -> dict[str, Any]:
        """Execute a tool by name."""
        handler = getattr(self, f"_tool_{tool_name}", None)
        if handler is None:
            return {"error": f"Unknown tool: {tool_name}"}
        try:
            return await handler(tool_args)
        except Exception as exc:
            logger.warning("Tool %s failed: %s", tool_name, exc, exc_info=True)
            return {"error": str(exc)}

    # ── File tools ─────────────────────────────────────────────────────

    async def _tool_file_read(self, args: dict) -> dict:
        file_path = str(args.get("file_path") or args.get("path") or "").strip()
        if not file_path:
            return {"error": "file_path is required"}

        resolved = self._resolve_path(file_path)
        ok, reason = self._validator.validate(resolved, operation="read")
        if not ok:
            return {"error": reason}

        try:
            offset = int(args.get("offset", 0))
            limit = int(args.get("limit", 2000))
            with open(resolved, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            selected = lines[offset:offset + limit]
            numbered = [f"{offset + i + 1}\t{line}" for i, line in enumerate(selected)]
            content = "".join(numbered)
            return {"result": self._truncate(content)}
        except FileNotFoundError:
            return {"error": f"File not found: {file_path}"}
        except Exception as exc:
            return {"error": f"Read failed: {exc}"}

    async def _tool_file_write(self, args: dict) -> dict:
        file_path = str(args.get("file_path") or args.get("path") or "").strip()
        content = str(args.get("content") or "")
        if not file_path:
            return {"error": "file_path is required"}

        resolved = self._resolve_path(file_path)
        ok, reason = self._validator.validate(resolved, operation="write")
        if not ok:
            return {"error": reason}

        try:
            os.makedirs(os.path.dirname(resolved), exist_ok=True)
            with open(resolved, "w", encoding="utf-8") as f:
                f.write(content)
            return {"result": f"Written {len(content)} chars to {file_path}"}
        except Exception as exc:
            return {"error": f"Write failed: {exc}"}

    async def _tool_file_edit(self, args: dict) -> dict:
        file_path = str(args.get("file_path") or args.get("path") or "").strip()
        old_string = str(args.get("old_string") or "")
        new_string = str(args.get("new_string") or "")
        if not file_path:
            return {"error": "file_path is required"}
        if not old_string:
            return {"error": "old_string is required"}

        resolved = self._resolve_path(file_path)
        ok, reason = self._validator.validate(resolved, operation="write")
        if not ok:
            return {"error": reason}

        try:
            with open(resolved, "r", encoding="utf-8") as f:
                content = f.read()
            count = content.count(old_string)
            if count == 0:
                return {"error": f"old_string not found in {file_path}"}
            replace_all = bool(args.get("replace_all", False))
            if not replace_all and count > 1:
                return {"error": f"old_string found {count} times (not unique). Use replace_all=true or provide more context."}
            updated = content.replace(old_string, new_string) if replace_all else content.replace(old_string, new_string, 1)
            with open(resolved, "w", encoding="utf-8") as f:
                f.write(updated)
            return {"result": f"Replaced {count if replace_all else 1} occurrence(s) in {file_path}"}
        except FileNotFoundError:
            return {"error": f"File not found: {file_path}"}
        except Exception as exc:
            return {"error": f"Edit failed: {exc}"}

    async def _tool_file_delete(self, args: dict) -> dict:
        file_path = str(args.get("file_path") or args.get("path") or "").strip()
        if not file_path:
            return {"error": "file_path is required"}

        resolved = self._resolve_path(file_path)
        ok, reason = self._validator.validate(resolved, operation="delete")
        if not ok:
            return {"error": reason}

        try:
            os.remove(resolved)
            return {"result": f"Deleted {file_path}"}
        except FileNotFoundError:
            return {"error": f"File not found: {file_path}"}
        except Exception as exc:
            return {"error": f"Delete failed: {exc}"}

    # ── Search tools ────────────────────────────────────────────────────

    async def _tool_glob(self, args: dict) -> dict:
        pattern = str(args.get("pattern") or "").strip()
        search_path = str(args.get("path") or self._workspace).strip()
        if not pattern:
            return {"error": "pattern is required"}

        resolved_path = self._resolve_path(search_path)
        ok, reason = self._validator.validate(resolved_path, operation="read")
        if not ok:
            return {"error": reason}

        try:
            full_pattern = os.path.join(resolved_path, pattern)
            matches = sorted(glob_mod.glob(full_pattern, recursive=True))
            # Filter to workspace
            matches = [m for m in matches if self._validator.validate(m, operation="read")[0]]
            result = "\n".join(matches[:200])
            return {"result": result or "(no matches)"}
        except Exception as exc:
            return {"error": f"Glob failed: {exc}"}

    async def _tool_grep(self, args: dict) -> dict:
        pattern = str(args.get("pattern") or "").strip()
        search_path = str(args.get("path") or self._workspace).strip()
        if not pattern:
            return {"error": "pattern is required"}

        resolved_path = self._resolve_path(search_path)
        ok, reason = self._validator.validate(resolved_path, operation="read")
        if not ok:
            return {"error": reason}

        try:
            cmd = ["grep", "-rn", "--include=*", "-m", "50", pattern, resolved_path]
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._workspace,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
            output = stdout.decode("utf-8", errors="replace").strip()
            return {"result": self._truncate(output) or "(no matches)"}
        except asyncio.TimeoutError:
            return {"error": "Grep timed out"}
        except Exception as exc:
            return {"error": f"Grep failed: {exc}"}

    # ── Bash tool ────────────────────────────────────────────────────────

    async def _tool_bash(self, args: dict) -> dict:
        command = str(args.get("command") or "").strip()
        if not command:
            return {"error": "command is required"}

        ok, reason = self._validator.validate_command(command)
        if not ok:
            return {"error": reason}

        timeout = min(int(args.get("timeout", MAX_BASH_TIMEOUT)), MAX_BASH_TIMEOUT)

        try:
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self._workspace,
                env={**os.environ, "HOME": os.environ.get("HOME", "/tmp")},
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
            output = stdout.decode("utf-8", errors="replace")
            err_output = stderr.decode("utf-8", errors="replace")
            combined = output
            if err_output:
                combined += f"\n[stderr]\n{err_output}"
            if proc.returncode != 0:
                combined += f"\n[exit code: {proc.returncode}]"
            return {"result": self._truncate(combined)}
        except asyncio.TimeoutError:
            return {"error": f"Command timed out after {timeout}s"}
        except Exception as exc:
            return {"error": f"Bash failed: {exc}"}

    # ── Web tools ────────────────────────────────────────────────────────

    async def _tool_web_search(self, args: dict) -> dict:
        query = str(args.get("query") or "").strip()
        if not query:
            return {"error": "query is required"}
        return {"result": f"[Web search not yet implemented] Query: {query}"}

    async def _tool_web_fetch(self, args: dict) -> dict:
        url = str(args.get("url") or "").strip()
        if not url:
            return {"error": "url is required"}
        return {"result": f"[Web fetch not yet implemented] URL: {url}"}

    # ── Helpers ─────────────────────────────────────────────────────────

    def _resolve_path(self, file_path: str) -> str:
        """Resolve a path relative to the workspace."""
        p = Path(file_path)
        if not p.is_absolute():
            p = Path(self._workspace) / p
        return str(p.resolve())

    def _truncate(self, text: str) -> str:
        if len(text) <= MAX_RESULT_LENGTH:
            return text
        half = MAX_RESULT_LENGTH // 2
        return text[:half] + f"\n\n... ({len(text) - MAX_RESULT_LENGTH} chars truncated) ...\n\n" + text[-half:]
