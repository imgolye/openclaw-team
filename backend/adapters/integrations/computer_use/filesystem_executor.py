from __future__ import annotations

"""Workspace filesystem adapter for Computer Use."""

import hashlib
import mimetypes
from pathlib import Path

from backend.application.services.computer_use_paths import (
    is_output_actual_path,
    mask_computer_use_local_paths,
    mask_paths_in_text_block,
    resolve_computer_use_read_path,
    resolve_computer_use_write_path,
)


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


class WorkspaceFileComputerUseExecutor:
    def __init__(self, workspace):
        self._workspace = workspace if isinstance(workspace, dict) else {}

    def execute(self, action):
        action = action if isinstance(action, dict) else {}
        action_name = _normalized_text(action.get("action"))
        if action_name == "write_file":
            resolved_path = resolve_computer_use_write_path(action.get("path"), self._workspace)
            payload = action.get("_decodedData")
            if not isinstance(payload, bytes):
                raise RuntimeError("write_file 缺少已解码数据。")
            Path(resolved_path).write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            return {
                "path": mask_computer_use_local_paths(resolved_path, self._workspace),
                "virtualPath": mask_computer_use_local_paths(resolved_path, self._workspace),
                "sizeBytes": len(payload),
                "hash": digest,
                "egressEligible": is_output_actual_path(resolved_path, self._workspace),
                "meta": {"path": resolved_path},
            }
        if action_name == "read_file":
            resolved_path = resolve_computer_use_read_path(action.get("path"), self._workspace)
            file_path = Path(resolved_path)
            payload = file_path.read_bytes()
            mime_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
            preview = ""
            if mime_type.startswith("text/") or mime_type in {"application/json", "application/xml"}:
                try:
                    preview = payload.decode("utf-8")[:500]
                except Exception:
                    preview = ""
            return {
                "path": mask_computer_use_local_paths(resolved_path, self._workspace),
                "virtualPath": mask_computer_use_local_paths(resolved_path, self._workspace),
                "name": file_path.name,
                "sizeBytes": len(payload),
                "mimeType": mime_type,
                "preview": mask_paths_in_text_block(preview, self._workspace),
                "egressEligible": is_output_actual_path(resolved_path, self._workspace),
                "payload": payload,
                "meta": {"path": resolved_path},
            }
        raise RuntimeError(f"当前 filesystem executor 暂不支持动作：{action_name}")
