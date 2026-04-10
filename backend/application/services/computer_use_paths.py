from __future__ import annotations

"""Computer Use workspace path helpers.

The virtual-path contract is adapted from Deer Flow's thread sandbox helpers:
https://github.com/bytedance/deer-flow
See upstream MIT license for details.
"""

import posixpath
import re
from pathlib import Path, PurePosixPath


COMPUTER_USE_VIRTUAL_PATH_PREFIX = "/mnt/user-data"
COMPUTER_USE_VIRTUAL_WORKSPACE = f"{COMPUTER_USE_VIRTUAL_PATH_PREFIX}/workspace"
COMPUTER_USE_VIRTUAL_UPLOADS = f"{COMPUTER_USE_VIRTUAL_PATH_PREFIX}/uploads"
COMPUTER_USE_VIRTUAL_OUTPUTS = f"{COMPUTER_USE_VIRTUAL_PATH_PREFIX}/outputs"
COMPUTER_USE_VIRTUAL_DOWNLOADS = f"{COMPUTER_USE_VIRTUAL_PATH_PREFIX}/downloads"
COMPUTER_USE_VIRTUAL_RESULT = f"{COMPUTER_USE_VIRTUAL_PATH_PREFIX}/result"
_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![:\w])/(?:[^\s\"'`;&|<>()]+)")


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def build_computer_use_workspace_paths(run_dir):
    root = Path(run_dir).expanduser().resolve()
    actual = {
        "root": str(root),
        "workspace": str(root / "workspace"),
        "uploads": str(root / "uploads"),
        "outputs": str(root / "outputs"),
        "downloads": str(root / "downloads"),
        "result": str(root / "result"),
    }
    virtual = {
        "root": COMPUTER_USE_VIRTUAL_PATH_PREFIX,
        "workspace": COMPUTER_USE_VIRTUAL_WORKSPACE,
        "uploads": COMPUTER_USE_VIRTUAL_UPLOADS,
        "outputs": COMPUTER_USE_VIRTUAL_OUTPUTS,
        "downloads": COMPUTER_USE_VIRTUAL_DOWNLOADS,
        "result": COMPUTER_USE_VIRTUAL_RESULT,
    }
    for path_value in actual.values():
        Path(path_value).mkdir(parents=True, exist_ok=True)
    return {
        "actual": actual,
        "virtual": virtual,
    }


def _reject_path_traversal(path):
    normalized = _normalized_text(path)
    if not normalized:
        raise RuntimeError("路径不能为空。")
    normalized_posix = normalized.replace("\\", "/")
    parts = PurePosixPath(normalized_posix).parts
    if ".." in parts:
        raise RuntimeError("检测到非法路径穿越。")
    return normalized_posix


def _workspace_sections(workspace):
    workspace = workspace if isinstance(workspace, dict) else {}
    actual = workspace.get("actual") if isinstance(workspace.get("actual"), dict) else {}
    virtual = workspace.get("virtual") if isinstance(workspace.get("virtual"), dict) else {}
    return actual, virtual


def _virtual_to_actual_mappings(workspace):
    actual, virtual = _workspace_sections(workspace)
    mappings = []
    for key in ("workspace", "uploads", "outputs", "downloads", "result", "root"):
        actual_path = _normalized_text(actual.get(key))
        virtual_path = _normalized_text(virtual.get(key))
        if actual_path and virtual_path:
            mappings.append((virtual_path, actual_path))
    mappings.sort(key=lambda item: len(item[0]), reverse=True)
    return mappings


def _actual_to_virtual_mappings(workspace):
    actual, virtual = _workspace_sections(workspace)
    mappings = []
    for key in ("workspace", "uploads", "outputs", "downloads", "result", "root"):
        actual_path = _normalized_text(actual.get(key))
        virtual_path = _normalized_text(virtual.get(key))
        if actual_path and virtual_path:
            mappings.append((actual_path, virtual_path))
    mappings.sort(key=lambda item: len(item[0]), reverse=True)
    return mappings


def replace_virtual_path(path, workspace):
    normalized = _reject_path_traversal(path)
    for virtual_path, actual_path in _virtual_to_actual_mappings(workspace):
        if normalized == virtual_path:
            return actual_path
        if normalized.startswith(f"{virtual_path}/"):
            suffix = normalized[len(virtual_path) :].lstrip("/")
            return str(Path(actual_path) / suffix)
    return path


def mask_computer_use_local_paths(text, workspace):
    message = str(text or "")
    if not message:
        return message
    masked = message
    for actual_path, virtual_path in _actual_to_virtual_mappings(workspace):
        masked = masked.replace(actual_path, virtual_path)
        masked = masked.replace(actual_path.replace("\\", "/"), virtual_path)
    return masked


def resolve_computer_use_read_path(path, workspace):
    normalized = replace_virtual_path(path, workspace)
    actual, _virtual = _workspace_sections(workspace)
    root = _normalized_text(actual.get("root"))
    if not root:
        raise RuntimeError("workspace 未初始化。")
    resolved = Path(normalized).expanduser().resolve()
    try:
        resolved.relative_to(Path(root).resolve())
    except ValueError as exc:
        raise RuntimeError("读取路径超出当前 run workspace。") from exc
    return str(resolved)


def resolve_computer_use_write_path(path, workspace, *, output_only=False):
    normalized = replace_virtual_path(path, workspace)
    actual, virtual = _workspace_sections(workspace)
    root = _normalized_text(actual.get("root"))
    if not root:
        raise RuntimeError("workspace 未初始化。")
    allowed_roots = []
    if output_only:
        for key in ("outputs", "downloads", "result"):
            root_value = _normalized_text(actual.get(key))
            if root_value:
                allowed_roots.append(Path(root_value).resolve())
    else:
        for key in ("workspace", "outputs", "downloads", "result"):
            root_value = _normalized_text(actual.get(key))
            if root_value:
                allowed_roots.append(Path(root_value).resolve())
    resolved = Path(normalized).expanduser().resolve()
    if not any(_is_relative_to(resolved, allowed_root) for allowed_root in allowed_roots):
        allowed_virtual = [
            _normalized_text(virtual.get(key))
            for key in ("workspace", "outputs", "downloads", "result")
            if _normalized_text(virtual.get(key))
        ]
        raise RuntimeError(f"写入路径必须位于当前 run workspace 内：{', '.join(allowed_virtual)}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return str(resolved)


def is_output_virtual_path(path, workspace):
    normalized = _reject_path_traversal(path)
    _actual, virtual = _workspace_sections(workspace)
    for key in ("outputs", "downloads", "result"):
        prefix = _normalized_text(virtual.get(key))
        if not prefix:
            continue
        if normalized == prefix or normalized.startswith(f"{prefix}/"):
            return True
    return False


def is_output_actual_path(path, workspace):
    actual, _virtual = _workspace_sections(workspace)
    normalized = Path(str(path or "")).expanduser().resolve()
    for key in ("outputs", "downloads", "result"):
        root_value = _normalized_text(actual.get(key))
        if not root_value:
            continue
        try:
            normalized.relative_to(Path(root_value).resolve())
            return True
        except ValueError:
            continue
    return False


def mask_paths_in_text_block(text, workspace):
    masked = mask_computer_use_local_paths(text, workspace)
    return _ABSOLUTE_PATH_PATTERN.sub(lambda match: _mask_absolute_path_match(match, workspace), masked)


def _mask_absolute_path_match(match, workspace):
    value = match.group(0)
    masked = mask_computer_use_local_paths(value, workspace)
    return masked


def _is_relative_to(path, base):
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def normalize_virtual_output_path(path):
    normalized = _reject_path_traversal(path)
    if normalized == COMPUTER_USE_VIRTUAL_OUTPUTS or normalized.startswith(f"{COMPUTER_USE_VIRTUAL_OUTPUTS}/"):
        return normalized
    if normalized == COMPUTER_USE_VIRTUAL_DOWNLOADS or normalized.startswith(f"{COMPUTER_USE_VIRTUAL_DOWNLOADS}/"):
        return normalized
    if normalized == COMPUTER_USE_VIRTUAL_RESULT or normalized.startswith(f"{COMPUTER_USE_VIRTUAL_RESULT}/"):
        return normalized
    raise RuntimeError(
        "外发产物路径必须位于 /mnt/user-data/outputs、/mnt/user-data/downloads 或 /mnt/user-data/result 下。"
    )
