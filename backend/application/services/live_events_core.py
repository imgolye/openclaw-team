from __future__ import annotations

import json
import logging
import os
import queue
import sys
import threading
import time
from copy import deepcopy
from http.server import ThreadingHTTPServer


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


EVENT_STREAM_STOP_EXCEPTIONS = _DelegatedSymbol("EVENT_STREAM_STOP_EXCEPTIONS")
LIVE_EVENT_PING_SECONDS = _DelegatedSymbol("LIVE_EVENT_PING_SECONDS")
LIVE_EVENT_QUEUE_SIZE = _DelegatedSymbol("LIVE_EVENT_QUEUE_SIZE")
LIVE_EVENT_REDIS_CHANNEL = _DelegatedSymbol("LIVE_EVENT_REDIS_CHANNEL")
LIVE_EVENT_WAIT_SECONDS = _DelegatedSymbol("LIVE_EVENT_WAIT_SECONDS")
now_iso = _DelegatedSymbol("now_iso")
read_env_value = _DelegatedSymbol("read_env_value")


def _live_event_ping_seconds():
    return float(getattr(_svc(), "LIVE_EVENT_PING_SECONDS"))


def _live_event_queue_size():
    return int(getattr(_svc(), "LIVE_EVENT_QUEUE_SIZE"))


def _live_event_redis_channel():
    return str(getattr(_svc(), "LIVE_EVENT_REDIS_CHANNEL"))


def _live_event_wait_seconds():
    return float(getattr(_svc(), "LIVE_EVENT_WAIT_SECONDS"))


def _event_stream_stop_exceptions():
    value = getattr(_svc(), "EVENT_STREAM_STOP_EXCEPTIONS")
    if isinstance(value, tuple):
        return value
    return (value,)


def publish_live_event(server, event_name, payload):
    if not server or not hasattr(server, "publish_live_event"):
        return
    try:
        server.publish_live_event(event_name, payload)
    except Exception:
        return


def publish_chat_thread_stream_event(server, thread_id, stage, **payload):
    normalized_thread_id = str(thread_id or "").strip()
    normalized_stage = str(stage or "").strip()
    if not normalized_thread_id or not normalized_stage:
        return
    publish_live_event(
        server,
        "chat_thread",
        {
            "threadId": normalized_thread_id,
            "stage": normalized_stage,
            "at": now_iso(),
            **payload,
        },
    )


def publish_conversation_stream_event(server, agent_id, session_id, stage, conversation_key="", **payload):
    normalized_agent_id = str(agent_id or "").strip()
    normalized_session_id = str(session_id or "").strip() or "main"
    normalized_stage = str(stage or "").strip()
    if not normalized_agent_id or not normalized_stage:
        return
    normalized_conversation_key = str(conversation_key or "").strip()
    if not normalized_conversation_key:
        normalized_conversation_key = (
            f"agent:{normalized_agent_id}:main"
            if normalized_session_id == "main"
            else (normalized_session_id if ":" in normalized_session_id else f"agent:{normalized_agent_id}:{normalized_session_id}")
        )
    publish_live_event(
        server,
        "conversation_stream",
        {
            "agentId": normalized_agent_id,
            "sessionId": normalized_session_id,
            "conversationKey": normalized_conversation_key,
            "stage": normalized_stage,
            "at": now_iso(),
            **payload,
        },
    )


def resolve_live_event_redis_url(openclaw_dir):
    for key in ("MISSION_CONTROL_REDIS_URL", "REDIS_URL"):
        value = str(os.environ.get(key) or read_env_value(openclaw_dir, key) or "").strip()
        if value:
            return value
    return ""


class RedisLiveEventBus:
    def __init__(self, redis_url, channel, server_id, on_event):
        self.redis_url = str(redis_url or "").strip()
        fallback_channel = _live_event_redis_channel()
        self.channel = str(channel or fallback_channel).strip() or fallback_channel
        self.server_id = str(server_id or "").strip()
        self.on_event = on_event
        self._client = None
        self._thread = None
        self._stop = threading.Event()
        self._available = False
        redis_module = getattr(_svc(), "redis", None)
        if not self.redis_url or redis_module is None:
            return
        try:
            self._client = redis_module.Redis.from_url(self.redis_url, decode_responses=True)
            self._client.ping()
            self._available = True
            self._thread = threading.Thread(target=self._listen_forever, name="mission-control-live-events", daemon=True)
            self._thread.start()
        except Exception as exc:
            logging.warning("Redis live event bus unavailable: %s", exc)
            self._client = None
            self._available = False

    @property
    def available(self):
        return self._available and self._client is not None

    def publish(self, event_name, payload):
        if not self.available:
            return False
        envelope = json.dumps(
            {
                "origin": self.server_id,
                "event": str(event_name or "").strip(),
                "payload": deepcopy(payload) if isinstance(payload, dict) else {},
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            self._client.publish(self.channel, envelope)
            return True
        except Exception as exc:
            logging.warning("Redis live event publish failed: %s", exc)
            return False

    def _listen_forever(self):
        while not self._stop.is_set() and self._client is not None:
            pubsub = None
            try:
                pubsub = self._client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                while not self._stop.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if not message:
                        continue
                    raw = message.get("data")
                    if not raw:
                        continue
                    try:
                        envelope = json.loads(raw)
                    except Exception:
                        continue
                    if str(envelope.get("origin") or "").strip() == self.server_id:
                        continue
                    event_name = str(envelope.get("event") or "").strip()
                    if not event_name:
                        continue
                    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
                    try:
                        self.on_event(event_name, payload)
                    except Exception:
                        logging.exception("Redis live event callback failed")
            except Exception as exc:
                logging.warning("Redis live event subscribe loop failed: %s", exc)
                time.sleep(1.0)
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass

    def close(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)


class MissionControlHTTPServer(ThreadingHTTPServer):
    request_queue_size = 128

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._live_event_lock = threading.RLock()
        self._live_event_subscribers = set()
        self.server_instance_id = _svc().secrets.token_hex(8)
        self.live_event_bus = None

    def subscribe_live_events(self):
        subscriber = queue.Queue(maxsize=_live_event_queue_size())
        with self._live_event_lock:
            self._live_event_subscribers.add(subscriber)
        return subscriber

    def unsubscribe_live_events(self, subscriber):
        with self._live_event_lock:
            self._live_event_subscribers.discard(subscriber)

    def _publish_live_event_local(self, event_name, payload):
        envelope = {
            "event": str(event_name or "").strip(),
            "payload": deepcopy(payload) if isinstance(payload, dict) else {},
        }
        with self._live_event_lock:
            subscribers = list(self._live_event_subscribers)
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(envelope)
                continue
            except queue.Full:
                pass
            try:
                subscriber.get_nowait()
            except queue.Empty:
                pass
            try:
                subscriber.put_nowait(envelope)
            except queue.Full:
                continue

    def publish_live_event(self, event_name, payload):
        self._publish_live_event_local(event_name, payload)
        if self.live_event_bus is not None:
            self.live_event_bus.publish(event_name, payload)


def serve_live_events(handler):
    if not handler._is_authenticated():
        handler._send_bytes(b"auth required", "text/plain; charset=utf-8", status=401)
        return
    handler.send_response(200)
    handler.send_header("Content-Type", "text/event-stream; charset=utf-8")
    handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
    handler.send_header("Connection", "keep-alive")
    for key, value in handler._cors_headers():
        handler.send_header(key, value)
    handler.end_headers()

    last_signature = None
    subscriber = handler.server.subscribe_live_events()
    next_dashboard_check = 0.0
    last_ping_at = time.monotonic()
    try:
        handler.wfile.write(b"retry: 3000\n\n")
        handler.wfile.flush()
        while True:
            now_monotonic = time.monotonic()
            if now_monotonic >= next_dashboard_check:
                try:
                    data, _paths = handler._bundle()
                except _event_stream_stop_exceptions():
                    return
                if data["signature"] != last_signature:
                    payload = json.dumps(
                        {"signature": data["signature"], "generatedAt": data["generatedAt"]},
                        ensure_ascii=False,
                    )
                    handler.wfile.write(f"event: dashboard\ndata: {payload}\n\n".encode("utf-8"))
                    handler.wfile.flush()
                    last_signature = data["signature"]
                    last_ping_at = time.monotonic()
                next_dashboard_check = time.monotonic() + max(float(handler.server.live_interval or 2.0), 0.5)
            try:
                envelope = subscriber.get(timeout=_live_event_wait_seconds())
            except queue.Empty:
                envelope = None
            if envelope:
                event_name = str(envelope.get("event") or "").strip()
                payload = json.dumps(envelope.get("payload") or {}, ensure_ascii=False)
                handler.wfile.write(f"event: {event_name}\ndata: {payload}\n\n".encode("utf-8"))
                handler.wfile.flush()
                last_ping_at = time.monotonic()
                continue
            if time.monotonic() - last_ping_at >= _live_event_ping_seconds():
                handler.wfile.write(b": ping\n\n")
                handler.wfile.flush()
                last_ping_at = time.monotonic()
    except (BrokenPipeError, ConnectionResetError, TimeoutError):
        return
    finally:
        handler.server.unsubscribe_live_events(subscriber)
