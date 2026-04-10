"""
跨平台 JSON 文件锁工具。

使用 O_CREAT | O_EXCL 创建锁文件，兼容 macOS / Linux / Windows。
异常退出留下的陈旧锁会在超时后自动清理。
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
import time
from contextlib import contextmanager
from typing import Any, Callable


LOCK_TIMEOUT_SECONDS = float(os.environ.get("KANBAN_LOCK_TIMEOUT_SECONDS", "10"))
LOCK_STALE_SECONDS = float(os.environ.get("KANBAN_LOCK_STALE_SECONDS", "300"))
LOCK_RETRY_SECONDS = float(os.environ.get("KANBAN_LOCK_RETRY_SECONDS", "0.1"))


def _lock_path(path: pathlib.Path) -> pathlib.Path:
    return path.parent / (path.name + ".lock")


def _safe_unlink(path: pathlib.Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _lock_owner_pid(lock_file: pathlib.Path) -> int | None:
    try:
        payload = json.loads(lock_file.read_text(encoding="utf-8"))
    except Exception:
        return None
    try:
        pid = int(payload.get("pid"))
    except Exception:
        return None
    return pid if pid > 0 else None


def _pid_is_alive(pid: int | None) -> bool:
    if not pid:
        return False
    if pid == os.getpid():
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True
    return True


@contextmanager
def _exclusive_lock(path: pathlib.Path):
    lock_file = _lock_path(path)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + LOCK_TIMEOUT_SECONDS
    fd = None

    while fd is None:
        try:
            fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            payload = {"pid": os.getpid(), "createdAt": time.time()}
            os.write(fd, json.dumps(payload).encode("utf-8"))
            break
        except FileExistsError:
            try:
                age = time.time() - lock_file.stat().st_mtime
            except FileNotFoundError:
                continue
            owner_pid = _lock_owner_pid(lock_file)
            if owner_pid and not _pid_is_alive(owner_pid):
                _safe_unlink(lock_file)
                continue
            if age > LOCK_STALE_SECONDS:
                _safe_unlink(lock_file)
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting for lock: {lock_file}")
            time.sleep(LOCK_RETRY_SECONDS)

    try:
        yield
    finally:
        if fd is not None:
            os.close(fd)
        _safe_unlink(lock_file)


def atomic_json_read(path: pathlib.Path, default: Any = None) -> Any:
    """持锁读取 JSON 文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with _exclusive_lock(path):
        try:
            return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
        except Exception:
            return default


def atomic_json_update(
    path: pathlib.Path,
    modifier: Callable[[Any], Any],
    default: Any = None,
) -> Any:
    """
    原子地读取 → 修改 → 写回 JSON 文件。
    modifier(data) 应返回修改后的数据。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with _exclusive_lock(path):
        try:
            data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
        except Exception:
            data = default

        result = modifier(data)

        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=path.stem + "_"
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, str(path))
        except Exception:
            _safe_unlink(pathlib.Path(tmp_path))
            raise
        return result


def atomic_json_write(path: pathlib.Path, data: Any) -> None:
    """原子写入 JSON 文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with _exclusive_lock(path):
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=path.stem + "_"
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, str(path))
        except Exception:
            _safe_unlink(pathlib.Path(tmp_path))
            raise
