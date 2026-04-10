"""File path validation and authorization for tool execution.

Enforces workspace-scoped file access: all file operations must target
paths within explicitly allowed workspace roots.  Prevents path traversal
attacks, symlink escapes, and access to sensitive files.

Usage::

    validator = FilePathValidator(allowed_roots=["/workspace/my-project"])
    ok, reason = validator.validate("/workspace/my-project/src/app.py")
    # ok=True

    ok, reason = validator.validate("/etc/passwd")
    # ok=False, reason="Path is outside all allowed workspace roots"
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Sensitive file patterns — always require explicit approval even inside workspace
SENSITIVE_PATTERNS = {
    ".env",
    ".env.local",
    ".env.production",
    "credentials.json",
    "credentials.yaml",
    "credentials.yml",
    "secrets.json",
    "secrets.yaml",
    "secrets.yml",
    ".npmrc",
    ".pypirc",
    "id_rsa",
    "id_ed25519",
    ".git/config",
}

# Directories never writable (read-only even inside workspace)
READONLY_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
}


class FilePathValidator:
    """Workspace-scoped file path authorization.

    Validates that file paths are within allowed workspace roots and
    not targeting sensitive files.
    """

    def __init__(self, allowed_roots: list[str] | None = None):
        self._roots: list[Path] = []
        for root in (allowed_roots or []):
            resolved = Path(root).resolve()
            if resolved.is_dir():
                self._roots.append(resolved)

    @property
    def allowed_roots(self) -> list[str]:
        return [str(r) for r in self._roots]

    def add_root(self, root: str) -> bool:
        """Add a workspace root.  Returns True if added."""
        resolved = Path(root).resolve()
        if resolved.is_dir() and resolved not in self._roots:
            self._roots.append(resolved)
            return True
        return False

    # ── Core validation ──────────────────────────────────────────────

    def validate(
        self,
        file_path: str,
        *,
        operation: str = "read",
    ) -> tuple[bool, str]:
        """Validate a file path for the given operation.

        Args:
            file_path: The path to validate (absolute or relative).
            operation: "read", "write", "delete", or "execute".

        Returns:
            (is_valid, reason) — reason is empty when valid.
        """
        if not file_path or not file_path.strip():
            return False, "Empty file path"

        if not self._roots:
            return False, "No workspace roots configured — file access disabled"

        # Resolve to absolute path
        raw = Path(file_path.strip())
        try:
            resolved = raw.resolve()
        except (OSError, ValueError) as exc:
            return False, f"Cannot resolve path: {exc}"

        # ── Check within allowed roots ───────────────────────────
        if not self._is_within_roots(resolved):
            return False, f"Path is outside all allowed workspace roots: {resolved}"

        # ── Check symlink escape ─────────────────────────────────
        if raw.is_symlink():
            link_target = raw.resolve()
            if not self._is_within_roots(link_target):
                return False, f"Symlink escapes workspace: {raw} -> {link_target}"

        # ── Check sensitive files ────────────────────────────────
        is_sensitive = self._is_sensitive(resolved)
        if is_sensitive and operation in ("write", "delete"):
            return False, f"Write/delete to sensitive file requires explicit approval: {resolved.name}"

        # ── Check readonly directories for writes ────────────────
        if operation in ("write", "delete") and self._is_in_readonly_dir(resolved):
            return False, f"Path is in a read-only directory: {self._readonly_dir_name(resolved)}"

        return True, ""

    def validate_command(self, command: str) -> tuple[bool, str]:
        """Validate a shell command for dangerous patterns.

        This is a basic safety check — not a sandbox.
        """
        if not command or not command.strip():
            return False, "Empty command"

        dangerous = [
            "rm -rf /", "rm -rf /*", "mkfs.", "dd if=",
            "> /dev/sd", "chmod 777 /", ":(){ :|:& };:",
        ]
        lower = command.strip().lower()
        for pattern in dangerous:
            if pattern in lower:
                return False, f"Command contains dangerous pattern: {pattern}"

        return True, ""

    # ── Helpers ───────────────────────────────────────────────────────

    def _is_within_roots(self, resolved: Path) -> bool:
        """Check if resolved path is under any allowed root."""
        for root in self._roots:
            try:
                resolved.relative_to(root)
                return True
            except ValueError:
                continue
        return False

    def _is_sensitive(self, resolved: Path) -> bool:
        """Check if path matches a sensitive file pattern."""
        name = resolved.name
        if name in SENSITIVE_PATTERNS:
            return True
        # Check parent/.git/config style
        parts = resolved.parts
        for i, part in enumerate(parts):
            suffix = "/".join(parts[i:])
            if suffix in SENSITIVE_PATTERNS:
                return True
        return False

    def _is_in_readonly_dir(self, resolved: Path) -> bool:
        """Check if path is inside a readonly directory."""
        for part in resolved.parts:
            if part in READONLY_DIRS:
                return True
        return False

    def _readonly_dir_name(self, resolved: Path) -> str:
        for part in resolved.parts:
            if part in READONLY_DIRS:
                return part
        return ""

    def __repr__(self) -> str:
        return f"<FilePathValidator roots={len(self._roots)}>"
