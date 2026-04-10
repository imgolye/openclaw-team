from __future__ import annotations

import hmac
import json
import logging
import sys
from datetime import timedelta
from http.cookies import SimpleCookie
from urllib.parse import parse_qs, quote


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


SESSION_COOKIE_MAX_AGE = _DelegatedSymbol("SESSION_COOKIE_MAX_AGE")
SESSION_COOKIE_NAME = _DelegatedSymbol("SESSION_COOKIE_NAME")
append_audit_event = _DelegatedSymbol("append_audit_event")
api_scope_allows = _DelegatedSymbol("api_scope_allows")
decode_session_cookie = _DelegatedSymbol("decode_session_cookie")
encode_session_cookie = _DelegatedSymbol("encode_session_cookie")
expected_action_value = _DelegatedSymbol("expected_action_value")
find_tenant_record = _DelegatedSymbol("find_tenant_record")
load_product_users = _DelegatedSymbol("load_product_users")
normalize_username = _DelegatedSymbol("normalize_username")
now_iso = _DelegatedSymbol("now_iso")
now_utc = _DelegatedSymbol("now_utc")
permissions_for_role = _DelegatedSymbol("permissions_for_role")
role_meta = _DelegatedSymbol("role_meta")
store_resolve_tenant_api_key = _DelegatedSymbol("store_resolve_tenant_api_key")
store_touch_tenant_api_key = _DelegatedSymbol("store_touch_tenant_api_key")
tenant_primary_openclaw_dir = _DelegatedSymbol("tenant_primary_openclaw_dir")
update_product_user_login = _DelegatedSymbol("update_product_user_login")
verify_password = _DelegatedSymbol("verify_password")


def parse_request_cookies(cookie_header):
    cookie = SimpleCookie()
    if cookie_header:
        cookie.load(cookie_header)
    return {name: morsel.value for name, morsel in cookie.items()}


def safe_next_path(path):
    if not path or not path.startswith("/"):
        return "/"
    if path.startswith("//") or path.startswith("/login"):
        return "/"
    return path


def find_product_user(openclaw_dir, username):
    normalized = normalize_username(username)
    return next((user for user in load_product_users(openclaw_dir) if user["username"] == normalized), None)


def session_for_client(session):
    if not session:
        return {"displayName": "Guest", "role": "viewer", "roleLabel": role_meta("viewer")["label"], "kind": "guest"}
    role = session.get("role", "viewer")
    return {
        "displayName": session.get("displayName") or session.get("username") or "User",
        "username": session.get("username", ""),
        "role": role,
        "roleLabel": role_meta(role)["label"],
        "kind": session.get("kind", "user"),
    }


def actor_from_session(session):
    client = session_for_client(session)
    return {
        "displayName": client["displayName"],
        "username": client.get("username", ""),
        "role": client["role"],
        "kind": client.get("kind", "user"),
    }


def auth_mode(handler):
    auth_token = getattr(handler.server, "dashboard_auth_token", "")
    if not auth_token:
        return "open"
    return "accounts"


def current_session(handler):
    cached = getattr(handler, "_cached_session", None)
    if cached is not None:
        return cached
    auth_token = getattr(handler.server, "dashboard_auth_token", "")
    if not auth_token:
        session = {
            "kind": "open",
            "username": "local-open",
            "displayName": "Local Access",
            "role": "owner",
            "issuedAt": now_iso(),
            "expiresAt": (now_utc() + timedelta(hours=12)).isoformat().replace("+00:00", "Z"),
        }
        handler._cached_session = session
        return session
    cookies = parse_request_cookies(handler.headers.get("Cookie", ""))
    current = cookies.get(SESSION_COOKIE_NAME, "")
    session = decode_session_cookie(auth_token, current)
    handler._cached_session = session
    return session


def is_authenticated(handler):
    return current_session(handler) is not None


def permissions(handler):
    session = current_session(handler)
    role = session.get("role", "viewer") if session else "viewer"
    return permissions_for_role(role)


def can(handler, permission_key):
    return bool(permissions(handler).get(permission_key))


def auth_payload(handler):
    session = current_session(handler)
    return {
        "ok": bool(session),
        "session": session_for_client(session),
        "permissions": permissions(handler),
        "authMode": auth_mode(handler),
        "actionToken": expected_action_value(getattr(handler.server, "dashboard_auth_token", "")) if session else "",
        "productVersion": _svc().PRODUCT_VERSION,
    }


def unavailable_auth_payload(handler, message="认证服务暂时不可用，请稍后重试。"):
    auth_token = getattr(handler.server, "dashboard_auth_token", "")
    fallback_mode = "open" if not auth_token else "accounts"
    return {
        "ok": False,
        "authenticated": False,
        "error": "auth_unavailable",
        "message": message,
        "session": session_for_client(None),
        "permissions": permissions_for_role("viewer"),
        "authMode": fallback_mode,
        "actionToken": "",
        "productVersion": _svc().PRODUCT_VERSION,
    }


def best_effort_append_audit_event(openclaw_dir, action, actor, outcome="success", detail="", meta=None):
    try:
        return append_audit_event(
            openclaw_dir,
            action,
            actor,
            outcome=outcome,
            detail=detail,
            meta=meta,
        )
    except Exception as error:
        logging.warning("audit event skipped for %s: %s", action, error)
        return None


def api_key_value(handler):
    auth_header = str(handler.headers.get("Authorization", "")).strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header.split(" ", 1)[1].strip()
    return str(handler.headers.get("X-API-Key", "")).strip()


def api_key_record(handler):
    cached = getattr(handler, "_cached_api_key_record", None)
    if cached is not None:
        return cached
    raw_key = api_key_value(handler)
    if not raw_key:
        handler._cached_api_key_record = None
        return None
    record = store_resolve_tenant_api_key(handler.server.openclaw_dir, raw_key)
    if record:
        store_touch_tenant_api_key(handler.server.openclaw_dir, record.get("id", ""))
    handler._cached_api_key_record = record
    return record


def current_actor(handler):
    session = current_session(handler)
    if session:
        return actor_from_session(session)
    api_key = api_key_record(handler)
    if api_key:
        return {
            "displayName": api_key.get("name") or api_key.get("prefix", "Tenant API Key"),
            "username": api_key.get("tenantId", "tenant-api"),
            "role": "owner",
            "kind": "api_key",
        }
    return {"displayName": "anonymous", "username": "anonymous", "role": "viewer", "kind": "anonymous"}


def rest_auth_context(handler, required_scope="", tenant_ref=""):
    api_key = api_key_record(handler)
    tenant = find_tenant_record(handler.server.openclaw_dir, tenant_ref) if tenant_ref else None
    if api_key:
        if required_scope and not api_scope_allows(api_key.get("scopes", []), required_scope):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "API Key 没有访问当前资源的 scope。"}, status=403)
            return None
        if tenant and api_key.get("tenantId") != tenant.get("id"):
            handler._send_json({"ok": False, "error": "permission_denied", "message": "API Key 不能访问其他租户的数据。"}, status=403)
            return None
        if tenant_ref and not tenant:
            handler._send_json({"ok": False, "error": "not_found", "message": "租户不存在。"}, status=404)
            return None
        return {"mode": "api_key", "apiKey": api_key, "tenant": tenant}
    if not is_authenticated(handler):
        handler._send_json({"ok": False, "error": "auth_required", "message": "请先登录或提供 API Key。"}, status=401)
        return None
    if not (can(handler, "adminWrite") or can(handler, "auditView")):
        handler._send_json({"ok": False, "error": "permission_denied", "message": "当前账号没有租户平台访问权限。"}, status=403)
        return None
    if tenant_ref and not tenant:
        handler._send_json({"ok": False, "error": "not_found", "message": "租户不存在。"}, status=404)
        return None
    return {"mode": "session", "tenant": tenant}


def tenant_openclaw_dir(handler, tenant):
    tenant_dir = tenant_primary_openclaw_dir(handler.server.openclaw_dir, tenant)
    if not tenant_dir:
        raise RuntimeError("租户还没有绑定可用的 OpenClaw 安装。")
    if not tenant_dir.exists():
        raise RuntimeError(f"租户安装目录不存在：{tenant_dir}")
    return tenant_dir


def login_cookie_header(handler, session_data):
    auth_token = getattr(handler.server, "dashboard_auth_token", "")
    return (
        "Set-Cookie",
        f"{SESSION_COOKIE_NAME}={encode_session_cookie(auth_token, session_data)}; Max-Age={SESSION_COOKIE_MAX_AGE}; Path=/; HttpOnly; SameSite=Lax",
    )


def clear_cookie_header():
    return (
        "Set-Cookie",
        f"{SESSION_COOKIE_NAME}=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax",
    )


def require_action_token(handler, payload):
    expected = expected_action_value(getattr(handler.server, "dashboard_auth_token", ""))
    if not expected:
        return True
    provided = str((payload or {}).get("actionToken", "")).strip()
    if provided and hmac.compare_digest(provided, expected):
        return True
    handler._send_json({"ok": False, "error": "invalid_action_token", "message": "操作令牌已失效，请刷新页面后重试。"}, status=403)
    return False


def require_capability(handler, permission_key, message, status=403):
    if can(handler, permission_key):
        return True
    handler._send_json({"ok": False, "error": "permission_denied", "message": message}, status=status)
    return False


def audit(handler, action, outcome="success", detail="", meta=None):
    return append_audit_event(
        handler.server.openclaw_dir,
        action,
        current_actor(handler),
        outcome=outcome,
        detail=detail,
        meta=meta,
    )


def require_auth(handler, api=False):
    if is_authenticated(handler):
        return True
    if api:
        body = json.dumps(
            {"error": "auth_required", "login": f"/login?next={quote(handler._path())}"},
            ensure_ascii=False,
        ).encode("utf-8")
        handler._send_bytes(body, "application/json; charset=utf-8", status=401)
    else:
        handler._send_redirect(f"/login?next={quote(handler._path())}")
    return False


def build_session_data(kind, username, display_name, role):
    return {
        "kind": kind,
        "username": username,
        "displayName": display_name,
        "role": role,
        "issuedAt": now_iso(),
        "expiresAt": (now_utc() + timedelta(hours=12)).isoformat().replace("+00:00", "Z"),
    }


def authenticate_password(handler, username, password):
    users = load_product_users(handler.server.openclaw_dir)
    if not users:
        return None, "当前实例还没有可用团队账号，请先完成数据库初始化。"
    username = normalize_username(username)
    password = str(password or "").strip()
    user = find_product_user(handler.server.openclaw_dir, username)
    if user and user.get("status") == "active" and verify_password(password, user.get("passwordHash", "")):
        session_data = build_session_data(
            "user",
            user["username"],
            user.get("displayName") or user["username"],
            user.get("role", "viewer"),
        )
        update_product_user_login(handler.server.openclaw_dir, username)
        best_effort_append_audit_event(
            handler.server.openclaw_dir,
            "login",
            actor_from_session(session_data),
            detail="团队账号登录成功。",
            meta={"mode": "password"},
        )
        return session_data, None
    best_effort_append_audit_event(
        handler.server.openclaw_dir,
        "login",
        {"displayName": username or "unknown", "username": username, "role": "viewer", "kind": "anonymous"},
        outcome="denied",
        detail="团队账号登录失败。",
        meta={"mode": "password"},
    )
    return None, "团队账号或密码不正确，请重新输入。"


def handle_auth_session_get(handler):
    try:
        payload = auth_payload(handler)
        payload["authenticated"] = payload["ok"]
        handler._send_json(payload)
    except Exception:
        logging.exception("auth session unavailable")
        handler._send_json(unavailable_auth_payload(handler), status=503)


def handle_auth_login_json(handler):
    try:
        payload = handler._read_json_body()
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "请求体不是合法 JSON。"}, status=400)
        return
    mode = str(payload.get("mode", "password") or "password").strip()
    if mode != "password":
        handler._send_json(
            {
                "ok": False,
                "error": "unsupported_auth_mode",
                "message": "当前实例只支持数据库团队账号登录。",
                "authMode": auth_mode(handler),
            },
            status=400,
        )
        return
    try:
        session_data, error_message = authenticate_password(handler, payload.get("username", ""), payload.get("password", ""))
    except Exception:
        logging.exception("auth login unavailable")
        handler._send_json(unavailable_auth_payload(handler), status=503)
        return
    if not session_data:
        handler._send_json(
            {
                "ok": False,
                "error": "invalid_credentials",
                "message": error_message,
                "authMode": auth_mode(handler),
            },
            status=401,
        )
        return
    handler._cached_session = session_data
    try:
        response = auth_payload(handler)
    except Exception:
        logging.exception("auth login response fallback")
        response = unavailable_auth_payload(handler, message="")
        response["session"] = session_for_client(session_data)
        response["permissions"] = permissions_for_role(session_data.get("role", "viewer"))
    response["ok"] = True
    response["authenticated"] = True
    response["error"] = ""
    response["message"] = ""
    handler._send_json(response, extra_headers=[login_cookie_header(handler, session_data)])


def handle_auth_logout_json(handler):
    session = current_session(handler)
    if session:
        best_effort_append_audit_event(
            handler.server.openclaw_dir,
            "logout",
            actor_from_session(session),
            detail="用户已退出OpenClaw Team。",
        )
    handler._cached_session = None
    handler._send_json({"ok": True}, extra_headers=[clear_cookie_header()])


def handle_login_get(handler):
    if handler._serve_frontend_index():
        return
    handler._send_frontend_unavailable()


def handle_logout_post(handler):
    session = current_session(handler)
    length = int(handler.headers.get("Content-Length", "0") or "0")
    payload = handler.rfile.read(length).decode("utf-8", "replace") if length else ""
    form = parse_qs(payload)
    next_path = safe_next_path((form.get("next", ["/login"])[0] or "/login"))
    best_effort_append_audit_event(
        handler.server.openclaw_dir,
        "logout",
        actor_from_session(session),
        detail="用户已退出OpenClaw Team。",
    )
    handler._cached_session = None
    handler._send_redirect(next_path, extra_headers=[clear_cookie_header()])
