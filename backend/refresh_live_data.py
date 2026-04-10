#!/usr/bin/env python3
"""兼容旧链路：任务更新后只标记 dashboard 脏状态，不再主动重建。"""
import datetime
import pathlib

_BASE = pathlib.Path(__file__).resolve().parent.parent
DASHBOARD_DIR = _BASE / 'dashboard'
DASHBOARD_DIRTY_STAMP = DASHBOARD_DIR / '.dashboard-dirty.stamp'


def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


try:
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIRTY_STAMP.write_text(now_iso(), encoding='utf-8')
except OSError:
    pass
