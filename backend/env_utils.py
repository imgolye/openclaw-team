"""公共工具：从 ~/.openclaw/.env 或指定目录的 .env 读取配置值。"""
from __future__ import annotations

import os
from pathlib import Path


def read_env_value(openclaw_dir: str | Path, key: str, *, env_keys: tuple[str, ...] | None = None) -> str:
    """读取环境变量，优先 OS 环境变量，其次 .env 文件。

    Args:
        openclaw_dir: openclaw 根目录路径
        key: 要查找的配置键名
        env_keys: 可选，额外的 OS 环境变量名列表（按顺序查找）
    """
    # 先查 OS 环境变量（key 本身 + 可选的 env_keys）
    for k in ([key] + list(env_keys or [])):
        v = os.environ.get(k, "").strip()
        if v:
            return v

    # 再查 .env 文件
    env_path = Path(openclaw_dir) / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        current_key, value = line.split("=", 1)
        if current_key == key:
            return value.strip()
    return ""
