"""HTTP request handler for OpenClaw Team."""

from __future__ import annotations

import importlib
import json
import logging
import sys
from http.server import BaseHTTPRequestHandler


def _dashboard_module():
    module = sys.modules.get("backend.collaboration_dashboard")
    if module is not None:
        return module
    module = sys.modules.get("collaboration_dashboard")
    if module is not None:
        return module
    main = sys.modules.get("__main__")
    if main is not None and str(getattr(main, "__file__", "")).endswith("collaboration_dashboard.py"):
        return main
    try:
        return importlib.import_module("backend.collaboration_dashboard")
    except ModuleNotFoundError:
        return importlib.import_module("collaboration_dashboard")


class CollaborationDashboardHandler(BaseHTTPRequestHandler):
    server_version = "OpenClawTeam"
    SPA_ROUTES = {
        "/",
        "/login",
        "/overview",
        "/workspace/home",
        "/workspace/runs",
        "/workspace/deliverables",
        "/management",
        "/ops/alerts",
        "/ops/agent-health",
        "/ops/reports",
        "/platform",
        "/orchestration",
        "/battlefield",
        "/context",
        "/agents",
        "/tasks",
        "/chat",
        "/conversations",
        "/communications",
        "/activity",
        "/desktop",
        "/themes",
        "/skills",
        "/openclaw",
        "/openclaw/runtime",
        "/openclaw/models",
        "/openclaw/skills",
        "/openclaw/governance",
        "/openclaw/gateway",
        "/admin",
    }

    def log_message(self, format, *args):
        return

    def handle(self):
        try:
            super().handle()
        except _dashboard_module().HANDLER_IGNORED_EXCEPTIONS:
            return

    def _is_api_request(self):
        path = str(getattr(self, "path", "") or "")
        return path.startswith("/api/") or path.startswith("/api/v1/") or path == "/events"

    def _handle_unexpected_error(self):
        path = str(getattr(self, "path", "") or "")
        logging.exception(
            "Unhandled request error while serving %s %s",
            getattr(self, "command", "?"),
            path,
        )
        try:
            if self._is_api_request():
                self._send_json(
                    {
                        "ok": False,
                        "error": "internal_error",
                        "message": "Internal server error. Please try again later.",
                    },
                    status=500,
                )
                return
            self._send_bytes(b"Internal server error", "text/plain; charset=utf-8", status=500)
        except _dashboard_module().HANDLER_IGNORED_EXCEPTIONS:
            return

    def _run_request(self, callback):
        try:
            return callback()
        except _dashboard_module().HANDLER_IGNORED_EXCEPTIONS:
            return
        except Exception:
            self._handle_unexpected_error()

    def _cors_headers(self):
        return _dashboard_module().resolve_handler_cors_headers(self)

    def _send_bytes(self, body, content_type, status=200, extra_headers=None):
        return _dashboard_module().send_handler_bytes(
            self,
            body,
            content_type,
            status=status,
            extra_headers=extra_headers,
        )

    def _send_json(self, payload, status=200, extra_headers=None):
        return _dashboard_module().send_handler_json(
            self,
            payload,
            status=status,
            extra_headers=extra_headers,
        )

    def _send_preflight(self):
        return _dashboard_module().send_handler_preflight(self)

    def _state_dir_readiness(self):
        return _dashboard_module().build_handler_state_dir_readiness(self)

    def _deployment_metadata(self):
        return _dashboard_module().load_handler_deployment_metadata(self)

    def _health_payload(self, include_readiness=False):
        return _dashboard_module().build_handler_health_payload(self, include_readiness=include_readiness)

    def _auth_mode(self):
        return _dashboard_module().resolve_auth_mode(self)

    def _auth_payload(self):
        return _dashboard_module().build_auth_payload(self)

    def _frontend_dist(self):
        return _dashboard_module().resolve_handler_frontend_dist(self)

    def _serve_frontend_asset(self, path):
        return _dashboard_module().serve_handler_frontend_asset(self, path)

    def _serve_frontend_index(self):
        return _dashboard_module().serve_handler_frontend_index(self)

    def _send_frontend_unavailable(self):
        return _dashboard_module().send_handler_frontend_unavailable(self)

    def _runtime_data(self, data):
        return _dashboard_module().build_handler_runtime_data(self, data)

    def _bundle(self):
        return _dashboard_module().build_handler_bundle(self)

    def _refreshed_bundle(self):
        return _dashboard_module().build_handler_refreshed_bundle(self)

    def _task_action_dashboard(self, include_dashboard=False):
        return None

    def _action_dashboard_payload(self, data, sections=None):
        return None

    def _compact_dashboard_payload(self, data):
        return _dashboard_module().compact_dashboard_bootstrap_payload(data)

    def _canonical_query_path(self, path):
        return _dashboard_module().resolve_canonical_query_path(path)

    def _canonical_action_path(self, path):
        return _dashboard_module().resolve_canonical_action_path(path)

    def _is_command_path(self, path):
        return _dashboard_module().is_command_api_path(path)

    def _task_action_kind(self, path):
        return _dashboard_module().resolve_task_action_kind(path)

    def _handle_task_action_post(self, path, payload):
        app = _dashboard_module()
        services = app.build_task_action_services(app.__dict__)
        return app.dispatch_task_action_post(self, path, payload, services)

    def _handle_command_action_post(self, path, payload):
        app = _dashboard_module()
        services = app.build_command_action_services(app.__dict__)
        return app.dispatch_action_post(self, path, payload, services)

    def _handle_api_read_route(self, path):
        app = _dashboard_module()
        services = app.build_api_read_services(app.__dict__)
        return app.dispatch_api_read_route(self, path, services)

    def _handle_api_write_route(self, path):
        app = _dashboard_module()
        services = app.build_api_read_services(app.__dict__)
        return app.dispatch_api_write_route(self, path, services)

    def _handle_http_get(self):
        app = _dashboard_module()
        services = app.build_http_get_services(app.__dict__)
        return app.dispatch_http_get(self, services)

    def _handle_http_head(self):
        return _dashboard_module().dispatch_http_head(self)

    def _handle_http_post(self):
        app = _dashboard_module()
        services = app.build_http_post_services(app.__dict__)
        return app.dispatch_http_post(self, services)

    def _handle_http_put(self):
        return _dashboard_module().dispatch_http_put(self)

    def _handle_http_delete(self):
        return _dashboard_module().dispatch_http_delete(self)

    def _handle_http_options(self):
        return _dashboard_module().dispatch_http_options(self)

    def _path(self):
        return _dashboard_module().resolve_handler_path(self)

    def _query(self):
        return _dashboard_module().resolve_handler_query(self)

    def _read_json_body(self):
        return _dashboard_module().read_handler_json_body(self)

    def _read_body(self):
        length_header = str(self.headers.get("Content-Length") or "").strip()
        try:
            length = int(length_header)
        except (TypeError, ValueError):
            length = 0
        if length <= 0:
            return ""
        body = self.rfile.read(length)
        if isinstance(body, bytes):
            return body.decode("utf-8", errors="replace")
        return str(body or "")

    def _next_path(self):
        return _dashboard_module().resolve_handler_next_path(self)

    def _session(self):
        return _dashboard_module().resolve_current_session(self)

    def _is_authenticated(self):
        return _dashboard_module().session_is_authenticated(self)

    def _permissions(self):
        return _dashboard_module().resolve_session_permissions(self)

    def _can(self, permission_key):
        return _dashboard_module().session_can(self, permission_key)

    def _api_key_value(self):
        return _dashboard_module().resolve_api_key_value(self)

    def _api_key_record(self):
        return _dashboard_module().resolve_api_key_record(self)

    def _current_actor(self):
        return _dashboard_module().resolve_current_actor(self)

    def _rest_auth_context(self, required_scope="", tenant_ref=""):
        return _dashboard_module().build_rest_auth_context(self, required_scope=required_scope, tenant_ref=tenant_ref)

    def _tenant_openclaw_dir(self, tenant):
        return _dashboard_module().resolve_tenant_openclaw_dir(self, tenant)

    def _login_cookie_header(self, session_data):
        return _dashboard_module().build_login_cookie_header(self, session_data)

    def _clear_cookie_header(self):
        return _dashboard_module().clear_auth_cookie_header()

    def _require_action_token(self, payload):
        return _dashboard_module().require_action_token_from_payload(self, payload)

    def _require_capability(self, permission_key, message, status=403):
        return _dashboard_module().require_permission_capability(self, permission_key, message, status=status)

    def _audit(self, action, outcome="success", detail="", meta=None):
        return _dashboard_module().write_auth_audit(
            self,
            action,
            outcome=outcome,
            detail=detail,
            meta=meta,
        )

    def _send_redirect(self, location, extra_headers=None):
        return _dashboard_module().send_handler_redirect(self, location, extra_headers=extra_headers)

    def _require_auth(self, api=False):
        return _dashboard_module().require_session_auth(self, api=api)

    def _build_session_data(self, kind, username, display_name, role):
        return _dashboard_module().build_session_data_for_auth(kind, username, display_name, role)

    def _authenticate_password(self, username, password):
        return _dashboard_module().perform_password_auth(self, username, password)

    def _handle_auth_session_get(self):
        return _dashboard_module().handle_auth_session_get_request(self)

    def _handle_auth_login_json(self):
        return _dashboard_module().handle_auth_login_json_request(self)

    def _handle_auth_logout_json(self):
        return _dashboard_module().handle_auth_logout_json_request(self)

    def _handle_login_get(self):
        return _dashboard_module().handle_login_get_request(self)

    def _handle_logout_post(self):
        return _dashboard_module().handle_logout_post_request(self)

    def _handle_rest_get(self, path):
        app = _dashboard_module()
        services = app.build_rest_get_services(app.__dict__)
        return app.dispatch_rest_get(self, path, services)

    def _handle_rest_post(self, path):
        app = _dashboard_module()
        services = app.build_rest_post_services(app.__dict__)
        return app.dispatch_rest_post(self, path, services)

    def _handle_action_post(self, path):
        app = _dashboard_module()
        path = self._canonical_action_path(path)
        try:
            payload = self._read_json_body()
        except json.JSONDecodeError:
            self._send_json({"ok": False, "error": "invalid_json", "message": "Request body is not valid JSON."}, status=400)
            return

        if not self._require_action_token(payload):
            return

        self._action_dashboard_requested = True

        try:
            if self._handle_task_action_post(path, payload):
                return

            if self._handle_command_action_post(path, payload):
                return

            self._send_json({"ok": False, "error": "not_found", "message": "Unknown action endpoint."}, status=404)
        except RuntimeError as error:
            self._audit("action_error", outcome="denied", detail=str(error), meta={"path": path})
            self._send_json({"ok": False, "error": "action_failed", "message": str(error)}, status=400)
        except Exception as error:
            logging.exception("Action request failed for %s", path)
            try:
                self._audit("action_error", outcome="error", detail=str(error), meta={"path": path})
            except Exception:
                logging.exception("Failed to audit action error for %s", path)
            self._send_json({"ok": False, "error": "internal_error", "message": "Action failed. Please try again later."}, status=500)
        finally:
            self._action_dashboard_requested = None

    def do_GET(self):
        self._run_request(self._handle_http_get)

    def do_HEAD(self):
        self._run_request(self._handle_http_head)

    def do_POST(self):
        self._run_request(self._handle_http_post)

    def do_PUT(self):
        self._run_request(self._handle_http_put)

    def do_DELETE(self):
        self._run_request(self._handle_http_delete)

    def do_OPTIONS(self):
        self._run_request(self._handle_http_options)

    def _serve_events(self):
        return _dashboard_module().serve_live_events(self)
