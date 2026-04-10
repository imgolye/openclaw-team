"""Health, metrics, and API docs endpoints for OpenClaw Team."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

from backend.openapi_spec import OPENAPI_SPEC

# ── startup bookkeeping ────────────────────────────────────────────────

_START_TIME = time.monotonic()
_REQUEST_COUNTER = 0


def increment_request_counter() -> None:
    global _REQUEST_COUNTER
    _REQUEST_COUNTER += 1


# ── /health ────────────────────────────────────────────────────────────

def build_health_payload(version: str = "unknown") -> dict:
    return {
        "status": "ok",
        "version": version,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "uptime_seconds": round(time.monotonic() - _START_TIME, 1),
    }


# ── /metrics ───────────────────────────────────────────────────────────

def build_metrics_payload(version: str = "unknown") -> dict:
    import resource

    try:
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        mem_kb = rusage.ru_maxrss
    except Exception:
        mem_kb = 0

    process = None
    try:
        import psutil
        process = psutil.Process(os.getpid())
    except ImportError:
        pass

    return {
        "uptime_seconds": round(time.monotonic() - _START_TIME, 1),
        "version": version,
        "total_requests": _REQUEST_COUNTER,
        "process_rss_kb": mem_kb,
        "process_cpu_percent": process.cpu_percent() if process else None,
        "process_memory_mb": round(process.memory_info().rss / 1024 / 1024, 1) if process else None,
        "open_file_descriptors": process.num_fds() if process and hasattr(process, "num_fds") else None,
        "thread_count": process.num_threads() if process else None,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


# ── API versioning helper ─────────────────────────────────────────────

API_V1_PREFIX = "/api/v1"


def rewrite_path_to_legacy(path: str) -> str:
    """Strip /api/v1/ prefix so existing handlers see the legacy /api/ path."""
    if path.startswith(API_V1_PREFIX + "/"):
        return "/api/" + path[len(API_V1_PREFIX) + 1:]
    return path


def is_versioned_api_path(path: str) -> bool:
    return path.startswith(API_V1_PREFIX + "/")
