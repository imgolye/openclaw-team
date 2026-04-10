"""Core utility functions — re-exported from collaboration_dashboard.

This is a lightweight compatibility layer created during the first-pass
modularization of backend/collaboration_dashboard.py.
"""

from backend.collaboration_dashboard import (  # noqa: F401
    cached_payload,
    cached_payload_background,
    clear_cached_payloads,
    dashboard_dirty_marker_path,
    dashboard_state_cache_entry,
    format_age,
    infer_openclaw_dir,
    load_json,
    mark_dashboard_dirty,
    now_iso,
    now_utc,
    parse_iso,
    safe_chat_attachments,
    safe_chat_mentions,
    safe_list,
)

__all__ = [
    "cached_payload",
    "cached_payload_background",
    "clear_cached_payloads",
    "dashboard_dirty_marker_path",
    "dashboard_state_cache_entry",
    "format_age",
    "infer_openclaw_dir",
    "load_json",
    "mark_dashboard_dirty",
    "now_iso",
    "now_utc",
    "parse_iso",
    "safe_chat_attachments",
    "safe_chat_mentions",
    "safe_list",
]
