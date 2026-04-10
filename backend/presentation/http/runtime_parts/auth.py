"""Runtime part: auth."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError, ValueError):
        return default


def dashboard_dir(openclaw_dir):
    path = Path(openclaw_dir) / "dashboard"
    path.mkdir(parents=True, exist_ok=True)
    return path


def openclaw_state_dir_candidates(openclaw_dir):
    candidates = []
    primary = Path(openclaw_dir).expanduser() if openclaw_dir else None
    if primary:
        try:
            primary = primary.resolve()
        except OSError:
            pass
        candidates.append(primary)
    extra = str(os.environ.get("MISSION_CONTROL_HOST_PAIRING_SOURCE_DIR") or "").strip()
    if extra:
        extra_path = Path(extra).expanduser()
        try:
            extra_path = extra_path.resolve()
        except OSError:
            pass
        if extra_path not in candidates:
            candidates.append(extra_path)
    explicit_state_dir = str(os.environ.get("OPENCLAW_STATE_DIR") or "").strip()
    if explicit_state_dir:
        explicit_path = Path(explicit_state_dir).expanduser()
        try:
            explicit_path = explicit_path.resolve()
        except OSError:
            pass
        if explicit_path not in candidates:
            candidates.append(explicit_path)
    default_state_dir = Path.home() / ".openclaw"
    try:
        default_state_dir = default_state_dir.resolve()
    except OSError:
        pass
    if default_state_dir not in candidates:
        candidates.append(default_state_dir)
    return candidates


def agent_session_dirs(openclaw_dir, agent_id):
    if not agent_id:
        return []
    resolved = []
    for root in openclaw_state_dir_candidates(openclaw_dir):
        candidate = root / "agents" / agent_id / "sessions"
        if candidate.exists():
            resolved.append(candidate)
    if resolved:
        return resolved
    roots = openclaw_state_dir_candidates(openclaw_dir)
    if roots:
        return [roots[0] / "agents" / agent_id / "sessions"]
    return []

def agent_sessions_dir(openclaw_dir, agent_id):
    if not agent_id:
        return None
    dirs = agent_session_dirs(openclaw_dir, agent_id)
    return dirs[0] if dirs else None


def load_agent_session_index(openclaw_dir, agent_id):
    merged = {}
    for sessions_dir in reversed(agent_session_dirs(openclaw_dir, agent_id)):
        payload = load_json(sessions_dir / "sessions.json", {})
        if not isinstance(payload, dict):
            continue
        for key, value in payload.items():
            if isinstance(value, dict):
                merged[key] = value
    return merged


def session_manifest_entry(openclaw_dir, agent_id, session_id):
    session_id = str(session_id or "").strip()
    if not agent_id or not session_id:
        return None
    index = load_agent_session_index(openclaw_dir, agent_id)
    direct_key = f"agent:{agent_id}:{session_id}"
    direct_match = index.get(direct_key)
    if isinstance(direct_match, dict):
        return direct_match
    for key, value in index.items():
        if not isinstance(value, dict):
            continue
        if key == session_id or str(value.get("sessionId", "") or "").strip() == session_id:
            return value
    return None


def session_transcript_path(openclaw_dir, agent_id, session_id):
    if not agent_id or not session_id:
        return None
    session_dirs = agent_session_dirs(openclaw_dir, agent_id)
    if not session_dirs:
        return None

    for sessions_dir in session_dirs:
        direct_path = sessions_dir / f"{session_id}.jsonl"
        if direct_path.exists():
            return direct_path

    entry = session_manifest_entry(openclaw_dir, agent_id, session_id)
    if not isinstance(entry, dict):
        return None

    session_file = str(entry.get("sessionFile", "") or "").strip()
    if session_file:
        session_path = Path(session_file).expanduser()
        if session_path.exists():
            return session_path

    mapped_session_id = str(entry.get("sessionId", "") or "").strip()
    if mapped_session_id:
        for sessions_dir in session_dirs:
            mapped_path = sessions_dir / f"{mapped_session_id}.jsonl"
            if mapped_path.exists():
                return mapped_path
    return None


def session_id_for_transcript_path(openclaw_dir, agent_id, transcript_path, default_session_id=""):
    if not agent_id or not transcript_path:
        return str(default_session_id or "").strip()
    resolved_path = Path(transcript_path).expanduser().resolve()
    sessions_dirs = agent_session_dirs(openclaw_dir, agent_id)
    index = load_agent_session_index(openclaw_dir, agent_id)
    prefix = f"agent:{agent_id}:"
    for key, value in index.items():
        if not isinstance(value, dict):
            continue
        session_file = str(value.get("sessionFile") or "").strip()
        if session_file:
            try:
                candidate_path = Path(session_file).expanduser().resolve()
            except (OSError, ValueError):
                candidate_path = None
            if candidate_path and candidate_path == resolved_path:
                if key.startswith(prefix):
                    return key[len(prefix) :].strip() or str(default_session_id or "").strip() or resolved_path.stem
                return str(value.get("sessionId") or "").strip() or str(default_session_id or "").strip() or resolved_path.stem
        mapped_session_id = str(value.get("sessionId") or "").strip()
        if sessions_dirs and mapped_session_id:
            for sessions_dir in sessions_dirs:
                mapped_path = sessions_dir / f"{mapped_session_id}.jsonl"
                if mapped_path.exists() and mapped_path.resolve() == resolved_path:
                    if key.startswith(prefix):
                        return key[len(prefix) :].strip() or mapped_session_id
                    return mapped_session_id
    return str(default_session_id or "").strip() or resolved_path.stem


def find_conversation_session(conversations, agent_id, session_id):
    for session in conversations.get("sessions", []) or []:
        if session.get("agentId") == agent_id and session.get("sessionId") == session_id:
            return session
    return None


def session_last_activity(openclaw_dir, agent_id):
    sessions_dir = agent_sessions_dir(openclaw_dir, agent_id)
    latest = None
    if not sessions_dir or not sessions_dir.exists():
        return None
    for path in sessions_dir.rglob("*"):
        if path.is_file():
            dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if latest is None or dt > latest:
                latest = dt
    return latest


def audit_log_path(openclaw_dir):
    return dashboard_dir(openclaw_dir) / "audit-log.jsonl"


def automation_system_actor():
    return {
        "displayName": "Automation Engine",
        "username": "automation-engine",
        "role": "owner",
        "kind": "system",
    }
