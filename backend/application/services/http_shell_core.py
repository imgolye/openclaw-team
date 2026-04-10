from __future__ import annotations

import gzip
import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


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


BUNDLE_CACHE_LOCK = _DelegatedSymbol("BUNDLE_CACHE_LOCK")
PRODUCT_VERSION = _DelegatedSymbol("PRODUCT_VERSION")
_payload_cache_set = _DelegatedSymbol("_payload_cache_set")
build_admin_bootstrap_snapshot = _DelegatedSymbol("build_admin_bootstrap_snapshot")
build_dashboard_state = _DelegatedSymbol("build_dashboard_state")
build_dashboard_state_cached = _DelegatedSymbol("build_dashboard_state_cached")
dashboard_state_cache_entry = _DelegatedSymbol("dashboard_state_cache_entry")
dashboard_state_cache_key = _DelegatedSymbol("dashboard_state_cache_key")
expected_action_value = _DelegatedSymbol("expected_action_value")
guess_content_type = _DelegatedSymbol("guess_content_type")
invalidate_dashboard_bundle_cache = _DelegatedSymbol("invalidate_dashboard_bundle_cache")
invalidate_management_payload_cache = _DelegatedSymbol("invalidate_management_payload_cache")
load_config = _DelegatedSymbol("load_config")
load_project_metadata = _DelegatedSymbol("load_project_metadata")
load_runtime_sync_payload = _DelegatedSymbol("load_runtime_sync_payload")
now_iso = _DelegatedSymbol("now_iso")
now_utc = _DelegatedSymbol("now_utc")
normalize_project_metadata = _DelegatedSymbol("normalize_project_metadata")
resolve_dashboard_auth_token = _DelegatedSymbol("resolve_dashboard_auth_token")
resolve_project_dir = _DelegatedSymbol("resolve_project_dir")
safe_next_path = _DelegatedSymbol("safe_next_path")
session_for_client = _DelegatedSymbol("session_for_client")
store_check_storage_readiness = _DelegatedSymbol("store_check_storage_readiness")
wait_for_dashboard_bundle_workers = _DelegatedSymbol("wait_for_dashboard_bundle_workers")

_TEXT_FRONTEND_EXTENSIONS = {".html", ".js", ".mjs", ".css", ".json", ".svg"}
_GZIP_COMPRESSIBLE_TYPES = {
    "application/javascript",
    "application/json",
    "application/xml",
    "image/svg+xml",
}
_GZIP_MIN_BYTES = 1024


def _bundle_cache_lock():
    return getattr(_svc(), "BUNDLE_CACHE_LOCK")


def frontend_dist(handler):
    return getattr(handler.server, "frontend_dist", None)


def cors_headers(handler):
    origin = handler.headers.get("Origin")
    allowed = getattr(handler.server, "cors_origins", ())
    if not origin or origin not in allowed:
        return []
    return [
        ("Access-Control-Allow-Origin", origin),
        ("Access-Control-Allow-Credentials", "true"),
        ("Vary", "Origin"),
    ]


def _accepts_gzip(handler):
    header = str(handler.headers.get("Accept-Encoding") or "").lower()
    return "gzip" in header


def _is_gzip_compressible(content_type):
    base_type = str(content_type or "").split(";", 1)[0].strip().lower()
    if not base_type or base_type == "text/event-stream":
        return False
    return base_type.startswith("text/") or base_type in _GZIP_COMPRESSIBLE_TYPES


def _has_header(headers, name):
    normalized = str(name or "").lower()
    return any(str(key).lower() == normalized for key, _value in headers)


def _maybe_gzip_body(handler, body, content_type, headers):
    if not _accepts_gzip(handler):
        return body, headers
    if len(body) < _GZIP_MIN_BYTES:
        return body, headers
    if not _is_gzip_compressible(content_type):
        return body, headers
    if _has_header(headers, "Content-Encoding"):
        return body, headers
    compressed = gzip.compress(body, compresslevel=6)
    if len(compressed) >= len(body):
        return body, headers
    next_headers = list(headers)
    next_headers.append(("Content-Encoding", "gzip"))
    next_headers.append(("Vary", "Accept-Encoding"))
    return compressed, next_headers


def send_bytes(handler, body, content_type, status=200, extra_headers=None):
    headers = list(cors_headers(handler))
    headers.extend(extra_headers or [])
    body, headers = _maybe_gzip_body(handler, body, content_type, headers)
    handler.send_response(status)
    handler.send_header("Content-Type", content_type)
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    handler.send_header("X-Content-Type-Options", "nosniff")
    handler.send_header("X-Frame-Options", "DENY")
    for key, value in headers:
        handler.send_header(key, value)
    handler.end_headers()
    if handler.command == "HEAD":
        return
    try:
        handler.wfile.write(body)
    except (BrokenPipeError, ConnectionResetError, TimeoutError):
        return


def send_json(handler, payload, status=200, extra_headers=None):
    if (
        isinstance(payload, dict)
        and "dashboard" in payload
        and getattr(handler, "_action_dashboard_requested", None) is not None
    ):
        payload = {key: value for key, value in payload.items() if key != "dashboard"}
    body = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    send_bytes(handler, body, "application/json; charset=utf-8", status=status, extra_headers=extra_headers)


def send_preflight(handler):
    headers = cors_headers(handler)
    if not headers:
        send_bytes(handler, b"Origin not allowed", "text/plain; charset=utf-8", status=403)
        return
    handler.send_response(204)
    for key, value in headers:
        handler.send_header(key, value)
    handler.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-API-Key")
    handler.send_header("Access-Control-Max-Age", "600")
    handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    handler.end_headers()


def serve_frontend_asset(handler, path):
    dist_dir = frontend_dist(handler)
    if not dist_dir:
        return False
    relative = path.lstrip("/")
    if not relative:
        return False
    candidate = (dist_dir / relative).resolve()
    if dist_dir not in candidate.parents or not candidate.is_file():
        return False
    content_type = guess_content_type(candidate)
    if candidate.suffix in _TEXT_FRONTEND_EXTENSIONS:
        content_type = f"{content_type}; charset=utf-8"
    send_bytes(handler, candidate.read_bytes(), content_type)
    return True


def serve_frontend_index(handler):
    dist_dir = frontend_dist(handler)
    if not dist_dir:
        return False
    index_path = dist_dir / "index.html"
    if not index_path.exists():
        return False
    send_bytes(handler, index_path.read_bytes(), "text/html; charset=utf-8")
    return True


def send_frontend_unavailable(handler):
    send_bytes(
        handler,
        b"Frontend dist not configured. Build apps/frontend/dist before opening UI routes.",
        "text/plain; charset=utf-8",
        status=503,
    )


def state_dir_readiness(handler):
    state_dir = Path(handler.server.openclaw_dir).expanduser().resolve()
    writable = False
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        writable = os.access(state_dir, os.W_OK)
    except Exception:
        writable = False
    return {
        "path": str(state_dir),
        "exists": state_dir.exists(),
        "writable": writable,
    }


def deployment_metadata(handler):
    try:
        return load_project_metadata(handler.server.openclaw_dir)
    except Exception:
        return normalize_project_metadata({})


def health_payload(handler, include_readiness=False):
    metadata = deployment_metadata(handler)
    payload = {
        "ok": True,
        "service": "mission-control",
        "version": str(PRODUCT_VERSION),
        "deploymentMode": str(metadata.get("deploymentMode") or "single_tenant"),
        "deploymentProfile": str(metadata.get("deploymentProfile") or "standard"),
        "time": now_iso(),
    }
    if not include_readiness:
        return payload
    state_dir = state_dir_readiness(handler)
    storage = store_check_storage_readiness(handler.server.openclaw_dir)
    dist_dir = frontend_dist(handler)
    frontend_ready = True
    frontend_path = ""
    if dist_dir:
        frontend_path = str(dist_dir)
        frontend_ready = (dist_dir / "index.html").exists()
    auth_token = resolve_dashboard_auth_token(handler.server.openclaw_dir)
    runtime_sync_payload = load_runtime_sync_payload(handler.server.openclaw_dir)
    runtime_auth = (runtime_sync_payload.get("auth") or {}) if isinstance(runtime_sync_payload, dict) else {}
    require_agent_auth = str(metadata.get("deploymentMode") or "single_tenant").strip().lower() == "single_tenant"
    auth_ready = bool(runtime_auth.get("ok")) if require_agent_auth else True
    ready = bool(storage.get("ok")) and bool(state_dir.get("writable")) and bool(frontend_ready) and auth_ready
    payload.update(
        {
            "ok": ready,
            "checks": {
                "storage": storage,
                "stateDir": state_dir,
                "frontend": {"ok": bool(frontend_ready), "path": frontend_path},
                "auth": {"ok": bool(auth_token), "mode": handler._auth_mode()},
                "agentAuth": {
                    "ok": bool(runtime_auth.get("ok")),
                    "state": str(runtime_auth.get("state") or ""),
                    "readyCount": runtime_auth.get("readyCount") or 0,
                    "targetCount": runtime_auth.get("targetCount") or 0,
                    "required": require_agent_auth,
                    "error": str(runtime_auth.get("error") or "").strip(),
                },
            },
        }
    )
    return payload


def runtime_data(handler, data):
    config = load_config(handler.server.openclaw_dir)
    project_dir = resolve_project_dir(handler.server.openclaw_dir, config)
    permissions = handler._permissions()
    data["admin"] = build_admin_bootstrap_snapshot(
        handler.server.openclaw_dir,
        config=config,
        now=now_utc(),
    )
    data["runtime"] = {
        "productVersion": str(PRODUCT_VERSION),
        "actionsEnabled": permissions.get("taskWrite") or permissions.get("themeWrite") or permissions.get("adminWrite"),
        "themeSwitchAvailable": bool(project_dir and (project_dir / "platform" / "bin" / "install" / "switch_theme.py").exists() and permissions.get("themeWrite")),
        "actionToken": expected_action_value(getattr(handler.server, "dashboard_auth_token", "")),
        "currentUser": session_for_client(handler._session()),
        "permissions": permissions,
        "authMode": "open" if not getattr(handler.server, "dashboard_auth_token", "") else "accounts",
    }
    return data


def bundle(handler):
    data = build_dashboard_state_cached(handler.server.openclaw_dir, ttl_seconds=5.0)
    return runtime_data(handler, data), {"json": handler.server.output_dir / "collaboration-dashboard.json"}


def refreshed_bundle(handler):
    invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
    invalidate_management_payload_cache(handler.server.openclaw_dir)
    wait_for_dashboard_bundle_workers(handler.server.openclaw_dir, handler.server.output_dir, timeout=2.0)
    data = build_dashboard_state(handler.server.openclaw_dir)
    with _bundle_cache_lock():
        _payload_cache_set(
            dashboard_state_cache_key(handler.server.openclaw_dir),
            dashboard_state_cache_entry(handler.server.openclaw_dir, data),
        )
    return runtime_data(handler, data), {"json": handler.server.output_dir / "collaboration-dashboard.json"}


def task_action_dashboard(handler, include_dashboard=False):
    invalidate_dashboard_bundle_cache(
        handler.server.openclaw_dir,
        handler.server.output_dir,
        force_sync=bool(include_dashboard),
    )
    invalidate_management_payload_cache(handler.server.openclaw_dir)
    if not include_dashboard:
        return None
    data, _paths = refreshed_bundle(handler)
    return handler._action_dashboard_payload(data, sections=["taskIndex"])


def path(handler):
    return urlsplit(handler.path).path


def query(handler):
    return parse_qs(urlsplit(handler.path).query)


def read_json_body(handler):
    length = int(handler.headers.get("Content-Length", "0") or "0")
    raw = handler.rfile.read(length).decode("utf-8", "replace") if length else "{}"
    return json.loads(raw or "{}")


def next_path(handler):
    return safe_next_path(query(handler).get("next", ["/"])[0])


def send_redirect(handler, location, extra_headers=None):
    headers = [("Location", location)]
    headers.extend(extra_headers or [])
    handler.send_response(302)
    for key, value in headers:
        handler.send_header(key, value)
    handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    handler.end_headers()
