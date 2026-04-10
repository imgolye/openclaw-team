from __future__ import annotations

"""Browser-backed Computer Use adapter.

This follows the Bytebot split between orchestration and the low-level executor
service, adapted to OpenClaw Team's OpenClaw browser runtime.
"""

import sys
import time


class _DelegatedSymbol:
    def __init__(self, name):
        self._name = name

    def _resolve(self):
        return getattr(_svc(), self._name)

    def __call__(self, *args, **kwargs):
        return self._resolve()(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._resolve(), attr)


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


perform_browser_open = _DelegatedSymbol("perform_browser_open")
perform_browser_plan = _DelegatedSymbol("perform_browser_plan")
perform_browser_snapshot = _DelegatedSymbol("perform_browser_snapshot")
perform_browser_start = _DelegatedSymbol("perform_browser_start")
resolve_browser_command_profile = _DelegatedSymbol("resolve_browser_command_profile")


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def resolve_executor_profile(openclaw_dir, requested_profile="", env_fingerprint=None):
    env_fingerprint = env_fingerprint if isinstance(env_fingerprint, dict) else {}
    return resolve_browser_command_profile(
        openclaw_dir,
        requested_profile=_normalized_text(
            requested_profile,
            _normalized_text(env_fingerprint.get("profile"), "openclaw"),
        ),
    )


class BrowserComputerUseExecutor:
    def __init__(self, openclaw_dir, profile):
        self._openclaw_dir = openclaw_dir
        self._profile = profile

    @property
    def profile(self):
        return self._profile

    def execute(self, action, *, target_url="", step_key=""):
        action = action if isinstance(action, dict) else {}
        action_name = _normalized_text(action.get("action"))
        if action_name == "click_mouse":
            ref = _normalized_text(action.get("ref"))
            if not ref:
                raise RuntimeError("browser click_mouse 需要 ref。")
            result = perform_browser_plan(
                self._openclaw_dir,
                [{
                    "action": "click",
                    "ref": ref,
                    "double": int(action.get("clickCount") or 1) >= 2,
                    "targetId": _normalized_text(action.get("targetId")),
                }],
                profile=self._profile,
            )
            return {
                "browserPlan": result if isinstance(result, dict) else {},
                "meta": {"profile": self._profile, "ref": ref},
            }
        if action_name in {"type_text", "paste_text"}:
            text = str(action.get("text") or "")
            if not text:
                raise RuntimeError(f"browser {action_name} 需要 text。")
            fields = action.get("fields") if isinstance(action.get("fields"), list) else []
            selector = _normalized_text(action.get("selector"))
            target_id = _normalized_text(action.get("targetId"))
            if not fields and selector:
                fields = [{"selector": selector, "value": text}]
            normalized_fields = []
            for item in fields:
                if not isinstance(item, dict):
                    continue
                next_field = dict(item)
                if "value" not in next_field:
                    next_field["value"] = text
                normalized_fields.append(next_field)
            if not normalized_fields:
                raise RuntimeError(f"browser {action_name} 需要 fields 或 selector。")
            result = perform_browser_plan(
                self._openclaw_dir,
                [{
                    "action": "fill",
                    "fields": normalized_fields,
                    "targetId": target_id,
                }],
                profile=self._profile,
            )
            return {
                "browserPlan": result if isinstance(result, dict) else {},
                "meta": {"profile": self._profile, "fieldCount": len(normalized_fields)},
            }
        if action_name == "application":
            application = _normalized_text(action.get("application"))
            if application != "firefox":
                raise RuntimeError(f"当前 browser executor 暂不支持 application={application}")
            browser_start_result = perform_browser_start(self._openclaw_dir, profile=self._profile)
            browser_open_result = None
            if target_url and step_key != "verify-result":
                browser_open_result = perform_browser_open(self._openclaw_dir, url=target_url, profile=self._profile)
            return {
                "browserStart": browser_start_result if isinstance(browser_start_result, dict) else {},
                "browserOpen": browser_open_result if isinstance(browser_open_result, dict) else {},
                "meta": {"profile": self._profile, "targetUrl": target_url},
            }
        if action_name == "wait":
            duration = int(action.get("duration") or 0)
            time.sleep(max(duration, 0) / 1000.0)
            return {
                "waitedMs": duration,
                "meta": {"duration": duration},
            }
        if action_name == "screenshot":
            snapshot = perform_browser_snapshot(
                self._openclaw_dir,
                profile=self._profile,
                selector=_normalized_text(action.get("selector")),
                target_id=_normalized_text(action.get("targetId")),
                limit=int(action.get("limit") or 120),
            )
            return {
                "snapshot": snapshot if isinstance(snapshot, dict) else {},
                "meta": {"profile": self._profile},
            }
        raise RuntimeError(f"当前 browser executor 暂不支持低阶动作：{action_name}")
