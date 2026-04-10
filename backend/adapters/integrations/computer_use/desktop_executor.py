from __future__ import annotations

"""Native desktop adapter for Computer Use with graceful runtime probing."""

import os
import shlex
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

try:
    import Quartz
except Exception:  # pragma: no cover - optional at import time
    Quartz = None


APPLICATION_ALIASES = {
    "firefox": "Firefox",
    "browser": "Google Chrome",
    "chrome": "Google Chrome",
    "chromium": "Chromium",
    "safari": "Safari",
    "arc": "Arc",
    "1password": "1Password",
    "thunderbird": "Thunderbird",
    "vscode": "Visual Studio Code",
    "terminal": "Terminal",
    "directory": "Finder",
    "desktop": "Finder",
}
CLICLICK_KEY_ALIASES = {
    "enter": "return",
    "return": "return",
    "esc": "esc",
    "escape": "esc",
    "tab": "tab",
    "space": "space",
    "delete": "delete",
    "backspace": "delete",
    "arrowup": "arrow-up",
    "arrow-down": "arrow-down",
    "arrowleft": "arrow-left",
    "arrow-right": "arrow-right",
    "up": "arrow-up",
    "down": "arrow-down",
    "left": "arrow-left",
    "right": "arrow-right",
    "home": "home",
    "end": "end",
    "pageup": "page-up",
    "pagedown": "page-down",
}
CLICLICK_MODIFIER_KEYS = {"alt", "cmd", "ctrl", "fn", "shift"}

BROWSER_APPLICATION_CANDIDATES = {
    "firefox": ("Firefox", "Google Chrome", "Chromium", "Safari"),
    "browser": ("Google Chrome", "Safari", "Firefox", "Chromium"),
    "chrome": ("Google Chrome", "Chromium", "Safari"),
    "chromium": ("Chromium", "Google Chrome", "Safari"),
    "safari": ("Safari", "Google Chrome"),
    "arc": ("Arc", "Google Chrome", "Safari"),
}


def _normalized_text(value, default=""):
    text = str(value or "").strip()
    return text if text else default


def _safe_coordinates(value):
    if not isinstance(value, dict):
        raise RuntimeError("desktop action 需要有效坐标。")
    try:
        return int(value.get("x")), int(value.get("y"))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("desktop action 坐标无效。") from exc


def _command_output(process):
    stdout = str(process.stdout or "").strip()
    stderr = str(process.stderr or "").strip()
    return "\n".join(part for part in (stdout, stderr) if part)


def _mapped_key(key):
    normalized = _normalized_text(key).lower().replace("_", "").replace(" ", "")
    return CLICLICK_KEY_ALIASES.get(normalized, _normalized_text(key).lower())


def _application_exists(app_name):
    if not app_name:
        return False
    return any(
        Path(directory).expanduser().joinpath(f"{app_name}.app").exists()
        for directory in ("/Applications", "~/Applications")
    )


def _resolve_application_name(application):
    normalized = _normalized_text(application).lower()
    if normalized in BROWSER_APPLICATION_CANDIDATES:
        for candidate in BROWSER_APPLICATION_CANDIDATES[normalized]:
            if _application_exists(candidate):
                return candidate
    mapped = APPLICATION_ALIASES.get(normalized, _normalized_text(application))
    if mapped and _application_exists(mapped):
        return mapped
    return mapped


class DesktopComputerUseExecutor:
    def __init__(self):
        self._cliclick = shutil.which("cliclick")
        self._screencapture = shutil.which("screencapture")
        self._open = shutil.which("open") or shutil.which("xdg-open")
        self._pbcopy = shutil.which("pbcopy")

    def supports_screenshot(self):
        return bool(self._screencapture)

    def supports_pointer_actions(self):
        return bool(self._cliclick)

    def supports_keyboard_actions(self):
        return bool(self._cliclick)

    def supports_application_open(self):
        return bool(self._open)

    def supports_interaction(self):
        return bool(
            self.supports_pointer_actions()
            or self.supports_keyboard_actions()
            or self.supports_application_open()
        )

    def capability_summary(self):
        return {
            "screenshot": self.supports_screenshot(),
            "pointer": self.supports_pointer_actions(),
            "keyboard": self.supports_keyboard_actions(),
            "application": self.supports_application_open(),
        }

    def _run(self, args, *, input_text=None):
        process = subprocess.run(
            args,
            input=input_text,
            capture_output=True,
            text=True if input_text is not None else False,
            check=False,
        )
        output = _command_output(process)
        if process.returncode != 0:
            raise RuntimeError(output or f"命令执行失败：{' '.join(shlex.quote(str(item)) for item in args)}")
        return output

    def _run_cliclick(self, *commands, wait_ms=20, restore=False):
        if not self._cliclick:
            raise RuntimeError("当前系统未安装 cliclick，无法执行桌面鼠标键盘动作。")
        args = [self._cliclick]
        if restore:
            args.append("-r")
        args.extend(["-w", str(max(int(wait_ms or 0), 20))])
        args.extend(str(command) for command in commands if str(command or "").strip())
        return self._run(args)

    def _run_with_modifiers(self, commands, hold_keys=None, wait_ms=20):
        hold_keys = [
            _mapped_key(item)
            for item in (hold_keys if isinstance(hold_keys, list) else [])
            if _mapped_key(item) in CLICLICK_MODIFIER_KEYS
        ]
        command_list = []
        if hold_keys:
            command_list.append(f"kd:{','.join(hold_keys)}")
        command_list.extend(commands)
        if hold_keys:
            command_list.append(f"ku:{','.join(hold_keys)}")
        return self._run_cliclick(*command_list, wait_ms=wait_ms)

    def _current_mouse_position(self):
        if Quartz is None:
            return {"x": 0, "y": 0}
        point = Quartz.CGEventGetLocation(Quartz.CGEventCreate(None))
        return {"x": int(point.x), "y": int(point.y)}

    def _main_display_size(self):
        if Quartz is None:
            return {"width": 0, "height": 0}
        display_id = Quartz.CGMainDisplayID()
        return {
            "width": int(Quartz.CGDisplayPixelsWide(display_id)),
            "height": int(Quartz.CGDisplayPixelsHigh(display_id)),
        }

    def _capture_screenshot(self):
        if not self.supports_screenshot():
            raise RuntimeError("当前运行环境没有可用的原生桌面截图工具。")
        fd, temp_path = tempfile.mkstemp(prefix="mission-control-computer-use-", suffix=".png")
        os.close(fd)
        try:
            self._run([self._screencapture, "-x", temp_path])
            with open(temp_path, "rb") as handle:
                payload = handle.read()
        finally:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        position = self._current_mouse_position()
        size = self._main_display_size()
        summary = f"Desktop screenshot captured at cursor ({position['x']}, {position['y']}) on {size['width']}x{size['height']} screen."
        capture_meta = {
            "cursor": position,
            "display": size,
            "displaySize": size,
            "coordinateSpace": "desktop_pixels",
            "captureRect": {"x": 0, "y": 0, "width": size["width"], "height": size["height"]},
            "imageSize": size,
        }
        return {
            "imageBytes": payload,
            "mimeType": "image/png",
            "cursor": position,
            "display": size,
            "displaySize": size,
            "summaryText": summary,
            "meta": capture_meta,
        }

    def _scroll(self, action):
        if Quartz is None:
            raise RuntimeError("当前环境无法执行原生滚轮事件。")
        coordinates = action.get("coordinates")
        if isinstance(coordinates, dict):
            x_pos, y_pos = _safe_coordinates(coordinates)
            self._run_cliclick(f"m:{x_pos},{y_pos}")
        direction = _normalized_text(action.get("direction"), "down").lower()
        count = max(int(action.get("scrollCount") or 1), 1)
        delta_x = 0
        delta_y = 0
        if direction == "down":
            delta_y = -count
        elif direction == "up":
            delta_y = count
        elif direction == "left":
            delta_x = count
        elif direction == "right":
            delta_x = -count
        event = Quartz.CGEventCreateScrollWheelEvent(
            None,
            Quartz.kCGScrollEventUnitLine,
            2,
            delta_y,
            delta_x,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
        return {
            "direction": direction,
            "scrollCount": count,
            "coordinates": coordinates or {},
            "meta": {"direction": direction, "scrollCount": count},
        }

    def _type_text(self, text):
        return self._run_cliclick(f"t:{text}")

    def _press_special_key(self, key):
        mapped = _mapped_key(key)
        if mapped in CLICLICK_MODIFIER_KEYS:
            self._run_cliclick(f"kp:{mapped}")
            return
        self._run_cliclick(f"kp:{mapped}")

    def execute(self, action):
        action = action if isinstance(action, dict) else {}
        action_name = _normalized_text(action.get("action")).lower()

        if action_name == "application":
            if not self.supports_application_open():
                raise RuntimeError("当前运行环境没有可用的桌面应用启动命令。")
            application = _normalized_text(action.get("application")).lower()
            app_name = _resolve_application_name(application)
            args = [self._open, "-a", app_name]
            target_url = _normalized_text(action.get("url"))
            if target_url:
                args.append(target_url)
            output = self._run(args)
            return {
                "application": app_name,
                "targetUrl": target_url,
                "output": output,
                "meta": {"application": app_name, "targetUrl": target_url},
            }

        if action_name == "screenshot":
            return self._capture_screenshot()

        if action_name == "cursor_position":
            position = self._current_mouse_position()
            return {"coordinates": position, "meta": {"cursor": position}}

        if action_name == "move_mouse":
            if not self.supports_pointer_actions():
                raise RuntimeError("当前运行环境没有可用的原生桌面指针控制工具。")
            x_pos, y_pos = _safe_coordinates(action.get("coordinates"))
            self._run_cliclick(f"m:{x_pos},{y_pos}")
            return {"coordinates": {"x": x_pos, "y": y_pos}, "meta": {"cursor": {"x": x_pos, "y": y_pos}}}

        if action_name == "trace_mouse":
            commands = []
            for point in action.get("path") if isinstance(action.get("path"), list) else []:
                x_pos, y_pos = _safe_coordinates(point)
                commands.append(f"m:{x_pos},{y_pos}")
            self._run_with_modifiers(commands, hold_keys=action.get("holdKeys"))
            return {"path": action.get("path") or [], "meta": {"pathLength": len(action.get('path') or [])}}

        if action_name == "click_mouse":
            if not self.supports_pointer_actions():
                raise RuntimeError("当前运行环境没有可用的原生桌面点击工具。")
            coordinates = action.get("coordinates")
            target = "."
            if isinstance(coordinates, dict):
                x_pos, y_pos = _safe_coordinates(coordinates)
                target = f"{x_pos},{y_pos}"
            button = _normalized_text(action.get("button"), "left").lower()
            click_count = max(int(action.get("clickCount") or 1), 1)
            if button == "right":
                command = f"rc:{target}"
            elif click_count == 2:
                command = f"dc:{target}"
            elif click_count >= 3:
                command = f"tc:{target}"
            else:
                command = f"c:{target}"
            self._run_with_modifiers([command], hold_keys=action.get("holdKeys"))
            return {
                "coordinates": coordinates or self._current_mouse_position(),
                "button": button,
                "clickCount": click_count,
                "meta": {"button": button, "clickCount": click_count},
            }

        if action_name == "press_mouse":
            if not self.supports_pointer_actions():
                raise RuntimeError("当前运行环境没有可用的原生桌面点击工具。")
            button = _normalized_text(action.get("button"), "left").lower()
            if button != "left":
                raise RuntimeError("当前桌面执行器只支持 left press_mouse。")
            coordinates = action.get("coordinates")
            target = "."
            if isinstance(coordinates, dict):
                x_pos, y_pos = _safe_coordinates(coordinates)
                target = f"{x_pos},{y_pos}"
            press = _normalized_text(action.get("press"), "down").lower()
            command = f"{'dd' if press == 'down' else 'du'}:{target}"
            self._run_cliclick(command)
            return {"coordinates": coordinates or self._current_mouse_position(), "press": press, "meta": {"press": press}}

        if action_name == "drag_mouse":
            if not self.supports_pointer_actions():
                raise RuntimeError("当前运行环境没有可用的原生桌面拖拽工具。")
            path = action.get("path") if isinstance(action.get("path"), list) else []
            if len(path) < 2:
                raise RuntimeError("drag_mouse.path 至少需要两个坐标点。")
            start_x, start_y = _safe_coordinates(path[0])
            commands = [f"dd:{start_x},{start_y}"]
            for point in path[1:-1]:
                x_pos, y_pos = _safe_coordinates(point)
                commands.append(f"dm:{x_pos},{y_pos}")
            end_x, end_y = _safe_coordinates(path[-1])
            commands.append(f"du:{end_x},{end_y}")
            self._run_with_modifiers(commands, hold_keys=action.get("holdKeys"))
            return {"path": path, "meta": {"pathLength": len(path)}}

        if action_name == "scroll":
            if not self.supports_pointer_actions():
                raise RuntimeError("当前运行环境没有可用的原生桌面滚轮工具。")
            return self._scroll(action)

        if action_name == "type_text":
            if not self.supports_keyboard_actions():
                raise RuntimeError("当前运行环境没有可用的原生桌面键盘工具。")
            text = _normalized_text(action.get("text"))
            if not text:
                raise RuntimeError("type_text.text 不能为空。")
            self._type_text(text)
            delay_ms = int(action.get("delay") or 0)
            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)
            return {
                "typedLength": len(text),
                "sensitive": bool(action.get("sensitive")),
                "meta": {"typedLength": len(text), "sensitive": bool(action.get("sensitive"))},
            }

        if action_name == "paste_text":
            if not self._pbcopy or not self.supports_keyboard_actions():
                raise RuntimeError("当前运行环境没有可用的桌面粘贴能力。")
            text = _normalized_text(action.get("text"))
            if not text:
                raise RuntimeError("paste_text.text 不能为空。")
            self._run([self._pbcopy], input_text=text)
            self._run_cliclick("kd:cmd", "kp:v", "ku:cmd")
            return {"typedLength": len(text), "meta": {"typedLength": len(text), "paste": True}}

        if action_name == "type_keys":
            keys = action.get("keys") if isinstance(action.get("keys"), list) else []
            delay_ms = int(action.get("delay") or 0)
            for item in keys:
                key = _normalized_text(item)
                if len(key) == 1:
                    self._type_text(key)
                else:
                    self._press_special_key(key)
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
            return {"keys": keys, "meta": {"keyCount": len(keys)}}

        if action_name == "press_keys":
            keys = [_mapped_key(item) for item in (action.get("keys") if isinstance(action.get("keys"), list) else [])]
            press = _normalized_text(action.get("press"), "down").lower()
            if not keys:
                raise RuntimeError("press_keys.keys 不能为空。")
            if not all(item in CLICLICK_MODIFIER_KEYS for item in keys):
                raise RuntimeError("当前桌面执行器的 press_keys 仅支持 modifier keys。")
            command = f"{'kd' if press == 'down' else 'ku'}:{','.join(keys)}"
            self._run_cliclick(command)
            return {"keys": keys, "press": press, "meta": {"keys": keys, "press": press}}

        raise RuntimeError(f"当前桌面执行器暂不支持动作：{action_name}")
