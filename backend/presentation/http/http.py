from __future__ import annotations

import json


def _normalize_api_path(path: str) -> str:
    """Normalize versioned API paths to internal canonical form."""
    if path.startswith("/api/v1/"):
        return "/api/" + path[len("/api/v1/"):]
    return path


def _serve_monitoring_endpoints(handler, raw_path):
    """Handle /health, /metrics, /api/v1/docs. Returns True if handled."""
    if raw_path == "/health":
        from backend.monitoring import build_health_payload
        handler._send_json(build_health_payload())
        return True
    if raw_path == "/metrics":
        from backend.monitoring import build_metrics_payload
        handler._send_json(build_metrics_payload())
        return True
    if raw_path in ("/api/docs", "/api/v1/docs"):
        from backend.monitoring import OPENAPI_SPEC
        handler._send_json(OPENAPI_SPEC)
        return True
    return False


def handle_http_get(handler, services):
    raw_path = handler._path()
    legacy_path = _normalize_api_path(raw_path)

    # Monitoring endpoints (no auth required)
    if _serve_monitoring_endpoints(handler, raw_path):
        return

    if raw_path == "/healthz":
        handler._send_json(handler._health_payload(include_readiness=False))
        return
    if raw_path == "/readinessz":
        payload = handler._health_payload(include_readiness=True)
        handler._send_json(payload, status=200 if payload.get("ok") else 503)
        return
    wechat_match = services["WECHAT_CALLBACK_ROUTE"].match(raw_path)
    if wechat_match:
        services["handle_wechat_customer_access_get"](handler, wechat_match.group(1))
        return
    if handler._serve_frontend_asset(raw_path):
        return
    # v1 REST endpoints (native) first, then generic legacy fallback rewrite.
    if raw_path.startswith("/api/v1/") and handler._handle_rest_get(raw_path):
        return
    if raw_path == "/login":
        handler._handle_login_get()
        return
    path = handler._canonical_query_path(legacy_path)
    if path == "/api/auth/session":
        handler._handle_auth_session_get()
        return
    if handler._frontend_dist() and raw_path in handler.SPA_ROUTES:
        handler._serve_frontend_index()
        return
    if raw_path in handler.SPA_ROUTES:
        handler._send_frontend_unavailable()
        return
    if not handler._require_auth(api=raw_path.startswith("/api/") or raw_path == "/events"):
        return
    if handler._handle_api_read_route(path):
        return
    handler._send_bytes(b"Not found", "text/plain; charset=utf-8", status=404)


def handle_http_head(handler):
    if handler._path() == "/events":
        handler._send_bytes(b"", "text/plain; charset=utf-8", status=405)
        return
    handler.do_GET()


def handle_http_post(handler, services):
    raw_path = handler._path()
    legacy_path = _normalize_api_path(raw_path)
    wechat_match = services["WECHAT_CALLBACK_ROUTE"].match(raw_path)
    if wechat_match:
        services["handle_wechat_customer_access_post"](handler, wechat_match.group(1))
        return
    if raw_path.startswith("/api/v1/") and handler._handle_rest_post(raw_path):
        return
    if raw_path == "/login":
        handler._send_bytes(b"Method not allowed", "text/plain; charset=utf-8", status=405)
        return
    if raw_path in ("/api/auth/login", "/api/v1/auth/login"):
        handler._handle_auth_login_json()
        return
    if raw_path in ("/api/auth/logout", "/api/v1/auth/logout"):
        handler._handle_auth_logout_json()
        return
    if raw_path == "/logout":
        handler._handle_logout_post()
        return
    if not handler._require_auth(api=raw_path.startswith("/api/") or raw_path == "/events"):
        return
    action_path = handler._canonical_action_path(legacy_path)
    if handler._is_command_path(legacy_path):
        handler._handle_action_post(action_path)
        return
    query_path = handler._canonical_query_path(legacy_path)
    if handler._handle_api_write_route(query_path):
        return
    handler._send_bytes(b"Method not allowed", "text/plain; charset=utf-8", status=405)


def handle_http_put(handler):
    raw_path = handler._path()
    legacy_path = _normalize_api_path(raw_path)
    if raw_path.startswith("/api/v1/") and handler._handle_rest_post(raw_path):
        return
    if not handler._require_auth(api=raw_path.startswith("/api/") or raw_path == "/events"):
        return
    path = handler._canonical_query_path(legacy_path)
    if handler._handle_api_write_route(path):
        return
    handler._send_bytes(b"Method not allowed", "text/plain; charset=utf-8", status=405)


def handle_http_delete(handler):
    raw_path = handler._path()
    legacy_path = _normalize_api_path(raw_path)
    if not handler._require_auth(api=raw_path.startswith("/api/") or raw_path == "/events"):
        return
    path = handler._canonical_query_path(legacy_path)
    if handler._handle_api_write_route(path):
        return
    handler._send_bytes(b"Method not allowed", "text/plain; charset=utf-8", status=405)


def handle_http_options(handler):
    path = handler._path()
    if path.startswith("/api/") or path == "/events":
        handler._send_preflight()
        return
    handler._send_bytes(b"Method not allowed", "text/plain; charset=utf-8", status=405)
