from __future__ import annotations

"""Computer Use action normalization.

The action contract is adapted from Bytebot's computer action model:
https://github.com/bytebot-ai/bytebot
See upstream Apache-2.0 license for details.
"""

import base64


COMPUTER_USE_BUTTONS = {"left", "right", "middle"}
COMPUTER_USE_PRESS_VALUES = {"up", "down"}
COMPUTER_USE_APPLICATIONS = {
    "firefox",
    "1password",
    "thunderbird",
    "vscode",
    "terminal",
    "desktop",
    "directory",
}
COMPUTER_USE_ACTIONS = {
    "move_mouse",
    "trace_mouse",
    "click_mouse",
    "press_mouse",
    "drag_mouse",
    "scroll",
    "type_keys",
    "paste_text",
    "press_keys",
    "type_text",
    "wait",
    "screenshot",
    "cursor_position",
    "application",
    "write_file",
    "read_file",
}
COMPUTER_USE_ACTION_SNAPSHOT_EXEMPT = {"screenshot", "cursor_position", "read_file"}


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def _bounded_int(value, default, minimum=0, maximum=100000):
    try:
        normalized = int(value if value is not None else default)
    except (TypeError, ValueError):
        normalized = int(default)
    return max(minimum, min(normalized, maximum))


def _normalized_coordinates(value):
    if not isinstance(value, dict):
        raise RuntimeError("coordinates 必须是对象。")
    try:
        x_value = int(value.get("x"))
        y_value = int(value.get("y"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("coordinates 需要有效的 x / y。") from exc
    return {"x": x_value, "y": y_value}


def _normalized_path(value):
    path = _normalized_text(value)
    if not path:
        raise RuntimeError("action path 不能为空。")
    return path


def _normalized_keys(value):
    if not isinstance(value, list) or not value:
        raise RuntimeError("keys 必须是非空数组。")
    cleaned = []
    for item in value:
        key = _normalized_text(item)
        if key:
            cleaned.append(key)
    if not cleaned:
        raise RuntimeError("keys 不能为空。")
    return cleaned


def _normalized_surface(value):
    if value is None:
        return ""
    surface = _normalized_text(value).lower()
    if not surface:
        return ""
    if surface not in {"browser", "desktop"}:
        raise RuntimeError(f"不支持的 surface：{surface}")
    return surface


def _normalized_browser_target(record):
    browser_target = {}
    ref = _normalized_text(record.get("ref"))
    selector = _normalized_text(record.get("selector"))
    target_id = _normalized_text(record.get("targetId") or record.get("target_id"))
    url = _normalized_text(record.get("url"))
    if ref:
        browser_target["ref"] = ref
    if selector:
        browser_target["selector"] = selector
    if target_id:
        browser_target["targetId"] = target_id
    if url:
        browser_target["url"] = url
    fields = record.get("fields")
    if isinstance(fields, list) and fields:
        browser_target["fields"] = [
            item
            for item in fields
            if isinstance(item, dict)
        ]
    return browser_target


def _normalized_button(value):
    button = _normalized_text(value, "left").lower()
    if button not in COMPUTER_USE_BUTTONS:
        raise RuntimeError(f"不支持的鼠标按键：{button}")
    return button


def _normalized_press(value):
    press = _normalized_text(value, "down").lower()
    if press not in COMPUTER_USE_PRESS_VALUES:
        raise RuntimeError(f"不支持的 press：{press}")
    return press


def _normalized_hold_keys(value):
    if value is None:
        return []
    if isinstance(value, list) and not value:
        return []
    return _normalized_keys(value)


def normalize_computer_action(record):
    if not isinstance(record, dict):
        raise RuntimeError("computer action 必须是对象。")
    action = _normalized_text(record.get("action")).lower()
    if action not in COMPUTER_USE_ACTIONS:
        raise RuntimeError(f"不支持的 computer action：{action or 'unknown'}")
    normalized = {"action": action}
    if action == "move_mouse":
        normalized["coordinates"] = _normalized_coordinates(record.get("coordinates"))
    elif action == "trace_mouse":
        path = record.get("path")
        if not isinstance(path, list) or not path:
            raise RuntimeError("trace_mouse.path 必须是非空坐标数组。")
        normalized["path"] = [_normalized_coordinates(item) for item in path]
        normalized["holdKeys"] = _normalized_hold_keys(record.get("holdKeys"))
    elif action == "click_mouse":
        coordinates = record.get("coordinates")
        if coordinates is not None:
            normalized["coordinates"] = _normalized_coordinates(coordinates)
        normalized["button"] = _normalized_button(record.get("button"))
        normalized["holdKeys"] = _normalized_hold_keys(record.get("holdKeys"))
        normalized["clickCount"] = _bounded_int(record.get("clickCount"), 1, minimum=1, maximum=5)
    elif action == "press_mouse":
        coordinates = record.get("coordinates")
        if coordinates is not None:
            normalized["coordinates"] = _normalized_coordinates(coordinates)
        normalized["button"] = _normalized_button(record.get("button"))
        normalized["press"] = _normalized_press(record.get("press"))
    elif action == "drag_mouse":
        path = record.get("path")
        if not isinstance(path, list) or len(path) < 2:
            raise RuntimeError("drag_mouse.path 至少需要两个坐标点。")
        normalized["path"] = [_normalized_coordinates(item) for item in path]
        normalized["button"] = _normalized_button(record.get("button"))
        normalized["holdKeys"] = _normalized_hold_keys(record.get("holdKeys"))
    elif action == "scroll":
        coordinates = record.get("coordinates")
        if coordinates is not None:
            normalized["coordinates"] = _normalized_coordinates(coordinates)
        direction = _normalized_text(record.get("direction"), "down").lower()
        if direction not in {"up", "down", "left", "right"}:
            raise RuntimeError(f"不支持的 scroll.direction：{direction}")
        normalized["direction"] = direction
        normalized["scrollCount"] = _bounded_int(record.get("scrollCount"), 1, minimum=1, maximum=200)
        normalized["holdKeys"] = _normalized_hold_keys(record.get("holdKeys"))
    elif action == "type_keys":
        normalized["keys"] = _normalized_keys(record.get("keys"))
        normalized["delay"] = _bounded_int(record.get("delay"), 40, minimum=0, maximum=5000)
    elif action == "paste_text":
        text = _normalized_text(record.get("text"))
        if not text:
            raise RuntimeError("paste_text.text 不能为空。")
        normalized["text"] = text
    elif action == "press_keys":
        normalized["keys"] = _normalized_keys(record.get("keys"))
        normalized["press"] = _normalized_press(record.get("press"))
    elif action == "type_text":
        text = _normalized_text(record.get("text"))
        if not text:
            raise RuntimeError("type_text.text 不能为空。")
        normalized["text"] = text
        normalized["delay"] = _bounded_int(record.get("delay"), 40, minimum=0, maximum=5000)
        normalized["sensitive"] = bool(record.get("sensitive"))
    elif action == "wait":
        normalized["duration"] = _bounded_int(record.get("duration"), 500, minimum=0, maximum=120000)
    elif action == "application":
        application = _normalized_text(record.get("application")).lower()
        if application not in COMPUTER_USE_APPLICATIONS:
            raise RuntimeError(f"不支持的 application：{application or 'unknown'}")
        normalized["application"] = application
    elif action == "write_file":
        normalized["path"] = _normalized_path(record.get("path"))
        data = _normalized_text(record.get("data"))
        if not data:
            raise RuntimeError("write_file.data 不能为空。")
        try:
            base64.b64decode(data.encode("utf-8"), validate=True)
        except Exception as exc:
            raise RuntimeError("write_file.data 需要是合法的 Base64。") from exc
        normalized["data"] = data
    elif action == "read_file":
        normalized["path"] = _normalized_path(record.get("path"))
    surface = _normalized_surface(record.get("surface"))
    if surface:
        normalized["surface"] = surface
    normalized.update(_normalized_browser_target(record))
    return normalized


def computer_action_side_effect_level(action):
    action_name = _normalized_text((action or {}).get("action")).lower()
    if action_name in {"screenshot", "cursor_position", "read_file", "wait"}:
        return "read_only"
    if action_name in {"move_mouse", "trace_mouse", "scroll", "application"}:
        return "idempotent_write"
    if action_name in {"click_mouse", "press_mouse", "drag_mouse", "type_keys", "paste_text", "press_keys", "type_text", "write_file"}:
        return "non_idempotent"
    return "read_only"


def computer_action_target(action):
    action = action if isinstance(action, dict) else {}
    action_name = _normalized_text(action.get("action")).lower()
    if _normalized_text(action.get("ref")):
        return {
            key: value
            for key, value in {
                "ref": action.get("ref"),
                "selector": action.get("selector"),
                "targetId": action.get("targetId"),
                "surface": action.get("surface"),
            }.items()
            if value not in ("", None)
        }
    if "coordinates" in action:
        return {"coordinates": action.get("coordinates")}
    if "path" in action and action_name not in {"write_file", "read_file"}:
        return {"path": action.get("path")}
    if action_name == "application":
        return {"application": action.get("application")}
    if action_name in {"write_file", "read_file"}:
        return {"path": action.get("path")}
    return {"action": action_name}


def should_capture_snapshot_after_action(action):
    action_name = _normalized_text((action or {}).get("action")).lower()
    return action_name not in COMPUTER_USE_ACTION_SNAPSHOT_EXEMPT
