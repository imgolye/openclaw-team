#!/usr/bin/env python3
"""Automation API endpoints for desktop client."""

from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


# In-memory storage with file persistence
_automations_db = {}
_automation_runs_db = {}
_db_lock = threading.Lock()
_db_initialized = False


def _get_storage_dir(openclaw_dir):
    """Get the storage directory for automation data."""
    storage_dir = Path(openclaw_dir) / ".mission-control" / "automations"
    storage_dir.mkdir(parents=True, exist_ok=True)
    return storage_dir


def _get_automations_file(openclaw_dir):
    """Get the automations storage file path."""
    return _get_storage_dir(openclaw_dir) / "automations.json"


def _get_runs_file(openclaw_dir):
    """Get the automation runs storage file path."""
    return _get_storage_dir(openclaw_dir) / "automation_runs.json"


def _load_from_disk(openclaw_dir):
    """Load automations from disk storage."""
    global _automations_db, _automation_runs_db, _db_initialized
    
    if _db_initialized:
        return
    
    with _db_lock:
        if _db_initialized:
            return
        
        automations_file = _get_automations_file(openclaw_dir)
        runs_file = _get_runs_file(openclaw_dir)
        
        if automations_file.exists():
            try:
                with open(automations_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        _automations_db = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load automations: {e}")
        
        if runs_file.exists():
            try:
                with open(runs_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        _automation_runs_db = data
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Failed to load automation runs: {e}")
        
        _db_initialized = True


def _save_to_disk(openclaw_dir):
    """Save automations to disk storage."""
    with _db_lock:
        automations_file = _get_automations_file(openclaw_dir)
        runs_file = _get_runs_file(openclaw_dir)
        
        try:
            with open(automations_file, 'w', encoding='utf-8') as f:
                json.dump(_automations_db, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save automations: {e}")
        
        try:
            with open(runs_file, 'w', encoding='utf-8') as f:
                json.dump(_automation_runs_db, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save automation runs: {e}")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _generate_id():
    return str(uuid.uuid4())


def _get_openclaw_dir(handler):
    """Get openclaw directory from handler."""
    return getattr(handler.server, 'openclaw_dir', os.getcwd())


def _get_trigger_icon(trigger_type):
    """Get icon for trigger type."""
    icons = {
        "schedule": "⏰",
        "webhook": "🔗",
        "manual": "👆",
        "event": "⚡",
    }
    return icons.get(trigger_type, "⚙️")


def _get_trigger_label(trigger_type):
    """Get label for trigger type."""
    labels = {
        "schedule": "定时触发",
        "webhook": "Webhook",
        "manual": "手动触发",
        "event": "事件触发",
    }
    return labels.get(trigger_type, "其他")


def _normalize_workflow_binding(payload):
    source = payload if isinstance(payload, dict) else {}
    workflow_id = str(source.get("workflowId") or "").strip()
    workflow_name = str(source.get("workflowName") or "").strip()
    inputs = source.get("inputs") if isinstance(source.get("inputs"), dict) else {}
    if not workflow_id:
        return None
    return {
        "workflowId": workflow_id,
        "workflowName": workflow_name,
        "inputs": inputs,
    }


def _local_now():
    return datetime.now().astimezone()


def _normalize_time_text(value):
    text = str(value or "").strip()
    if not text:
        return ""
    parts = text.split(":")
    if len(parts) != 2:
        return ""
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return ""
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return ""
    return f"{hour:02d}:{minute:02d}"


def _coerce_weekday(value):
    try:
        weekday = int(value)
    except (TypeError, ValueError):
        return None
    if weekday < 0 or weekday > 6:
        return None
    return weekday


def _weekday_label(weekday):
    return {
        0: "周日",
        1: "周一",
        2: "周二",
        3: "周三",
        4: "周四",
        5: "周五",
        6: "周六",
    }.get(weekday, "周一")


def _build_schedule_trigger_config(mode, time_text, weekday=None):
    normalized_time = _normalize_time_text(time_text) or "09:00"
    normalized_mode = "weekly" if str(mode or "").strip().lower() == "weekly" else "daily"
    config = {
        "mode": normalized_mode,
        "time": normalized_time,
    }
    if normalized_mode == "weekly":
        normalized_weekday = _coerce_weekday(weekday)
        if normalized_weekday is None:
            normalized_weekday = 1
        config["weekday"] = normalized_weekday
        config["label"] = f"每{_weekday_label(normalized_weekday)} {normalized_time}"
    else:
        config["label"] = f"每日 {normalized_time}"
    return config


def _parse_schedule_label(label):
    text = str(label or "").strip()
    if not text:
        return None
    if text.startswith("每日 "):
        time_text = _normalize_time_text(text.replace("每日 ", "", 1).strip())
        if time_text:
            return _build_schedule_trigger_config("daily", time_text)
    if text.startswith("每周") and len(text) >= 7:
        weekday_label = text[2:4]
        time_text = _normalize_time_text(text[4:].strip())
        weekday = {
            "周日": 0,
            "周一": 1,
            "周二": 2,
            "周三": 3,
            "周四": 4,
            "周五": 5,
            "周六": 6,
        }.get(weekday_label)
        if weekday is not None and time_text:
            return _build_schedule_trigger_config("weekly", time_text, weekday)
    return None


def _normalize_trigger_config(trigger_type, payload):
    config = payload if isinstance(payload, dict) else {}
    normalized_trigger = str(trigger_type or "manual").strip().lower()
    if normalized_trigger != "schedule":
        return config
    mode = str(config.get("mode") or "").strip().lower()
    time_text = _normalize_time_text(config.get("time"))
    weekday = _coerce_weekday(config.get("weekday"))
    if not time_text:
        parsed = _parse_schedule_label(config.get("label"))
        if parsed:
            return parsed
    return _build_schedule_trigger_config(mode or "daily", time_text or "09:00", weekday)


def _extract_schedule_definition(automation):
    if str(automation.get("trigger") or "").strip().lower() != "schedule":
        return None
    config = _normalize_trigger_config("schedule", automation.get("triggerConfig"))
    mode = str(config.get("mode") or "daily").strip().lower()
    time_text = _normalize_time_text(config.get("time")) or "09:00"
    hour, minute = [int(part) for part in time_text.split(":")]
    definition = {
        "mode": mode,
        "time": time_text,
        "hour": hour,
        "minute": minute,
        "label": config.get("label") or "",
    }
    if mode == "weekly":
        definition["weekday"] = _coerce_weekday(config.get("weekday")) or 1
    return definition


def _compute_next_run_at(schedule, now=None):
    if not schedule:
        return None
    current = now or _local_now()
    candidate = current.replace(
        hour=int(schedule.get("hour") or 0),
        minute=int(schedule.get("minute") or 0),
        second=0,
        microsecond=0,
    )
    if schedule.get("mode") == "weekly":
        weekday = int(schedule.get("weekday") or 1)
        days_ahead = (weekday - candidate.weekday()) % 7
        candidate = candidate + timedelta(days=days_ahead)
        if candidate <= current:
            candidate = candidate + timedelta(days=7)
    else:
        if candidate <= current:
            candidate = candidate + timedelta(days=1)
    return candidate


def _current_schedule_slot(schedule, now=None):
    if not schedule:
        return None
    current = now or _local_now()
    slot = current.replace(
        hour=int(schedule.get("hour") or 0),
        minute=int(schedule.get("minute") or 0),
        second=0,
        microsecond=0,
    )
    if schedule.get("mode") == "weekly":
        if current.weekday() != int(schedule.get("weekday") or 1) or current < slot:
            return None
    else:
        if current < slot:
            return None
    return slot


def _schedule_slot_key(slot):
    if slot is None:
        return ""
    return slot.isoformat()


def _decorate_automation(automation):
    decorated = dict(automation or {})
    schedule = _extract_schedule_definition(decorated)
    if schedule:
        trigger_config = dict(decorated.get("triggerConfig") or {})
        trigger_config.update(
            {
                "mode": schedule["mode"],
                "time": schedule["time"],
                "label": schedule["label"],
            }
        )
        if schedule.get("mode") == "weekly":
            trigger_config["weekday"] = schedule.get("weekday")
        decorated["triggerConfig"] = trigger_config
        next_run_at = _compute_next_run_at(schedule)
        if next_run_at:
            decorated["nextRunAt"] = next_run_at.isoformat()
    return decorated


def _start_automation_run(openclaw_dir, automation_id, automation, trigger_source="manual", trigger_payload=None):
    run_id = _generate_id()
    now = _now_iso()
    run = {
        "id": run_id,
        "automationId": automation_id,
        "status": "running",
        "startedAt": now,
        "completedAt": None,
        "results": [],
        "source": str(trigger_source or "manual"),
    }
    if isinstance(trigger_payload, dict) and trigger_payload:
        run["trigger"] = trigger_payload

    _automation_runs_db[run_id] = run
    automation["lastRun"] = now
    automation["runCount"] = automation.get("runCount", 0) + 1
    automation["updatedAt"] = now
    _save_to_disk(openclaw_dir)

    workflow_binding = _normalize_workflow_binding(automation.get("workflowBinding"))
    if workflow_binding and workflow_binding.get("workflowId"):
        try:
            from backend.presentation.http import workflow_api as workflow_runtime

            workflow, workflow_run = workflow_runtime.start_workflow_run(
                openclaw_dir,
                workflow_binding["workflowId"],
                input_payload=workflow_binding.get("inputs"),
                trigger={
                    "type": "automation",
                    "source": str(trigger_source or "manual"),
                    "automationId": automation_id,
                    "automationName": automation.get("name"),
                    "automationTrigger": automation.get("trigger"),
                    **(trigger_payload if isinstance(trigger_payload, dict) else {}),
                },
            )

            run["status"] = "completed"
            run["completedAt"] = _now_iso()
            run["workflowId"] = workflow.get("id")
            run["workflowRunId"] = workflow_run.get("id")
            run["workflowName"] = workflow.get("name")
            run["workspacePath"] = workflow_run.get("workspacePath") or workflow.get("workspacePath") or ""
            run["results"] = [
                {
                    "step": 1,
                    "status": "success",
                    "message": f"已触发工作流 {workflow.get('name') or workflow.get('id')}",
                    "workflowId": workflow.get("id"),
                    "workflowRunId": workflow_run.get("id"),
                    "workspacePath": workflow_run.get("workspacePath") or workflow.get("workspacePath") or "",
                }
            ]
            automation["successCount"] = automation.get("successCount", 0) + 1
            _save_to_disk(openclaw_dir)
            return True, run, None
        except KeyError:
            run["status"] = "failed"
            run["completedAt"] = _now_iso()
            run["error"] = "关联的工作流不存在。"
            automation["failCount"] = automation.get("failCount", 0) + 1
            _save_to_disk(openclaw_dir)
            return False, run, "workflow_not_found"
        except Exception as exc:
            run["status"] = "failed"
            run["completedAt"] = _now_iso()
            run["error"] = str(exc)
            automation["failCount"] = automation.get("failCount", 0) + 1
            _save_to_disk(openclaw_dir)
            return False, run, "workflow_run_failed"

    def execute_automation():
        try:
            import time

            time.sleep(1.5)
            run["status"] = "completed"
            run["completedAt"] = datetime.now(timezone.utc).isoformat()
            run["results"] = [
                {
                    "step": i + 1,
                    "status": "success",
                    "message": f"动作 {action.get('name', i + 1)} 执行成功",
                }
                for i, action in enumerate(automation.get("actions", []))
            ]
            automation["successCount"] = automation.get("successCount", 0) + 1
            _save_to_disk(openclaw_dir)
        except Exception as exc:
            run["status"] = "failed"
            run["completedAt"] = datetime.now(timezone.utc).isoformat()
            run["error"] = str(exc)
            automation["failCount"] = automation.get("failCount", 0) + 1
            _save_to_disk(openclaw_dir)

    threading.Thread(target=execute_automation, daemon=True).start()
    return True, run, None


def run_custom_automation_cycle(openclaw_dir):
    _load_from_disk(openclaw_dir)
    now = _local_now()
    triggered = []
    updated = False

    for automation in _automations_db.values():
        if not isinstance(automation, dict):
            continue
        if not automation.get("enabled", True):
            continue
        schedule = _extract_schedule_definition(automation)
        if not schedule:
            continue

        next_run_at = _compute_next_run_at(schedule, now=now)
        if next_run_at and automation.get("nextRunAt") != next_run_at.isoformat():
            automation["nextRunAt"] = next_run_at.isoformat()
            updated = True

        slot = _current_schedule_slot(schedule, now=now)
        if slot is None:
            continue

        slot_key = _schedule_slot_key(slot)
        schedule_state = automation.get("scheduleState") if isinstance(automation.get("scheduleState"), dict) else {}
        if str(schedule_state.get("lastTriggeredSlot") or "").strip() == slot_key:
            continue

        ok, run, error_code = _start_automation_run(
            openclaw_dir,
            automation.get("id"),
            automation,
            trigger_source="schedule-daemon",
            trigger_payload={
                "scheduledFor": slot.isoformat(),
                "scheduleMode": schedule.get("mode"),
            },
        )
        automation["scheduleState"] = {
            **schedule_state,
            "lastTriggeredSlot": slot_key,
            "lastTriggeredAt": _now_iso(),
            "lastResult": "success" if ok else "failed",
        }
        updated = True
        triggered.append(
            {
                "automationId": automation.get("id"),
                "automationName": automation.get("name"),
                "scheduledFor": slot.isoformat(),
                "runId": run.get("id"),
                "status": run.get("status"),
                "workflowRunId": run.get("workflowRunId"),
                "error": run.get("error") or error_code,
            }
        )

    if updated:
        _save_to_disk(openclaw_dir)

    return {
        "ok": True,
        "checkedAt": now.isoformat(),
        "triggeredCount": len(triggered),
        "triggered": triggered,
    }


def _format_management_schedule_label(rule):
    trigger_type = str(rule.get("triggerType") or "").strip().lower()
    meta = rule.get("meta") if isinstance(rule.get("meta"), dict) else {}

    if trigger_type == "daily_review_push":
        daily = meta.get("dailyReview") if isinstance(meta.get("dailyReview"), dict) else {}
        hour = int(daily.get("hour") or 18)
        minute = int(daily.get("minute") or 30)
        return {
            "trigger": "schedule",
            "triggerIcon": _get_trigger_icon("schedule"),
            "triggerLabel": "定时触发",
            "triggerConfig": {"label": f"每日 {hour:02d}:{minute:02d}"},
        }

    if trigger_type == "weekly_report_push":
        weekly = meta.get("weeklyReport") if isinstance(meta.get("weeklyReport"), dict) else {}
        weekday = int(weekly.get("weekday") or 1)
        hour = int(weekly.get("hour") or 9)
        minute = int(weekly.get("minute") or 0)
        weekday_label = {
            0: "周日",
            1: "周一",
            2: "周二",
            3: "周三",
            4: "周四",
            5: "周五",
            6: "周六",
        }.get(weekday, "周一")
        return {
            "trigger": "schedule",
            "triggerIcon": _get_trigger_icon("schedule"),
            "triggerLabel": "定时触发",
            "triggerConfig": {"label": f"每{weekday_label} {hour:02d}:{minute:02d}"},
        }

    threshold_minutes = int(rule.get("thresholdMinutes") or 0)
    cooldown_minutes = int(rule.get("cooldownMinutes") or 0)
    pieces = []
    if threshold_minutes > 0:
        pieces.append(f"{threshold_minutes} 分钟阈值")
    if cooldown_minutes > 0:
        pieces.append(f"{cooldown_minutes} 分钟冷却")
    event_name = " / ".join(pieces) if pieces else "规则触发"
    return {
        "trigger": "event",
        "triggerIcon": _get_trigger_icon("event"),
        "triggerLabel": "事件触发",
        "triggerConfig": {"eventName": event_name},
    }


def _normalize_management_rule_automation(rule, engine=None):
    rule_id = str(rule.get("id") or "").strip()
    if not rule_id:
        return None

    trigger_meta = _format_management_schedule_label(rule)
    status = str(rule.get("status") or "").strip().lower()
    threshold_minutes = int(rule.get("thresholdMinutes") or 0)
    cooldown_minutes = int(rule.get("cooldownMinutes") or 0)
    severity = str(rule.get("severity") or "").strip()
    summary_bits = []
    if threshold_minutes > 0:
        summary_bits.append(f"阈值 {threshold_minutes} 分钟")
    if cooldown_minutes > 0:
        summary_bits.append(f"冷却 {cooldown_minutes} 分钟")
    if severity:
        summary_bits.append(f"级别 {severity}")

    description = str(rule.get("description") or "").strip()
    if summary_bits:
        description = f"{description} · {' · '.join(summary_bits)}" if description else " · ".join(summary_bits)

    return {
        "id": f"management:{rule_id}",
        "source": "management-rule",
        "sourceLabel": "运营规则",
        "readOnly": True,
        "supportsRun": False,
        "supportsToggle": False,
        "supportsEdit": False,
        "supportsDelete": False,
        "name": str(rule.get("name") or "未命名规则").strip() or "未命名规则",
        "description": description,
        "enabled": status == "active",
        "trigger": trigger_meta["trigger"],
        "triggerIcon": trigger_meta["triggerIcon"],
        "triggerLabel": trigger_meta["triggerLabel"],
        "triggerConfig": trigger_meta["triggerConfig"],
        "actions": [],
        "createdAt": rule.get("createdAt"),
        "updatedAt": rule.get("updatedAt"),
        "lastRun": None,
        "runCount": 0,
        "successCount": 0,
        "failCount": 0,
        "engine": engine if isinstance(engine, dict) else {},
        "managementRule": rule,
    }


def _build_management_rule_automations(openclaw_dir, services):
    builder = services.get("build_management_automation_rules_snapshot")
    if not callable(builder):
        return []

    try:
        detail = builder(openclaw_dir)
    except Exception:
        return []

    automation = detail.get("automation") if isinstance(detail, dict) else {}
    rules = automation.get("rules") if isinstance(automation, dict) else []
    engine = automation.get("engine") if isinstance(automation, dict) else {}
    normalized = []
    for item in rules if isinstance(rules, list) else []:
        if not isinstance(item, dict):
            continue
        mapped = _normalize_management_rule_automation(item, engine=engine)
        if mapped:
            normalized.append(mapped)
    return normalized


def _find_management_rule_automation(openclaw_dir, services, automation_id):
    target_id = str(automation_id or "").strip()
    if not target_id.startswith("management:"):
        return None
    for item in _build_management_rule_automations(openclaw_dir, services):
        if item.get("id") == target_id:
            return item
    return None


def handle_automation_get_list(handler, services):
    """GET /api/automations - List all automations."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    custom_automations = [_decorate_automation(item) for item in _automations_db.values()]
    management_automations = _build_management_rule_automations(openclaw_dir, services)
    automations = custom_automations + management_automations
    handler._send_json(
        {
            "ok": True,
            "automations": automations,
            "summary": {
                "customCount": len(custom_automations),
                "managementRuleCount": len(management_automations),
                "totalCount": len(automations),
            },
        }
    )
    return True


def handle_automation_get_detail(handler, services, automation_id):
    """GET /api/automations/{id} - Get automation detail."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    management_automation = _find_management_rule_automation(openclaw_dir, services, automation_id)
    if management_automation:
        handler._send_json({"ok": True, "automation": management_automation})
        return True
    automation = _automations_db.get(automation_id)
    if not automation:
        handler._send_json({"ok": False, "error": "not_found", "message": "自动化任务不存在"}, status=404)
        return True
    handler._send_json({"ok": True, "automation": _decorate_automation(automation)})
    return True


def handle_automation_create(handler, services):
    """POST /api/automations - Create new automation."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True

    automation_id = _generate_id()
    now = _now_iso()
    trigger_type = body.get("trigger", "manual")
    trigger_config = _normalize_trigger_config(trigger_type, body.get("triggerConfig", {}))
    
    automation = {
        "id": automation_id,
        "name": body.get("name", "未命名自动化"),
        "description": body.get("description", ""),
        "enabled": body.get("enabled", True),
        "trigger": trigger_type,
        "triggerIcon": _get_trigger_icon(trigger_type),
        "triggerLabel": _get_trigger_label(trigger_type),
        "triggerConfig": trigger_config,
        "actions": body.get("actions", []),
        "createdAt": now,
        "updatedAt": now,
        "lastRun": None,
        "runCount": 0,
        "successCount": 0,
        "failCount": 0,
        "workflowBinding": _normalize_workflow_binding(body.get("workflowBinding")),
    }
    if trigger_type == "schedule":
        next_run_at = _compute_next_run_at(_extract_schedule_definition(automation))
        if next_run_at:
            automation["nextRunAt"] = next_run_at.isoformat()
    
    _automations_db[automation_id] = automation
    _save_to_disk(openclaw_dir)
    
    handler._send_json({"ok": True, "automation": _decorate_automation(automation)}, status=201)
    return True


def handle_automation_update(handler, services, automation_id):
    """PUT /api/automations/{id} - Update automation."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    if _find_management_rule_automation(openclaw_dir, services, automation_id):
        handler._send_json({"ok": False, "error": "read_only", "message": "内置运营规则请前往管理页调整。"}, status=400)
        return True
    
    automation = _automations_db.get(automation_id)
    if not automation:
        handler._send_json({"ok": False, "error": "not_found", "message": "自动化任务不存在"}, status=404)
        return True
    
    try:
        body = json.loads(handler._read_body() or "{}")
    except json.JSONDecodeError:
        handler._send_json({"ok": False, "error": "invalid_json", "message": "无效的 JSON 数据"}, status=400)
        return True
    
    # Update allowed fields
    allowed_fields = ["name", "description", "enabled", "trigger", "triggerConfig", "actions"]
    for field in allowed_fields:
        if field in body:
            if field == "triggerConfig":
                automation[field] = _normalize_trigger_config(body.get("trigger", automation.get("trigger")), body[field])
            else:
                automation[field] = body[field]
    if "workflowBinding" in body:
        automation["workflowBinding"] = _normalize_workflow_binding(body.get("workflowBinding"))
    
    # Update trigger metadata if trigger changed
    if "trigger" in body:
        automation["triggerIcon"] = _get_trigger_icon(body["trigger"])
        automation["triggerLabel"] = _get_trigger_label(body["trigger"])
        automation["triggerConfig"] = _normalize_trigger_config(body["trigger"], automation.get("triggerConfig"))
    if str(automation.get("trigger") or "").strip().lower() == "schedule":
        next_run_at = _compute_next_run_at(_extract_schedule_definition(automation))
        automation["nextRunAt"] = next_run_at.isoformat() if next_run_at else None
    else:
        automation.pop("nextRunAt", None)
        automation.pop("scheduleState", None)
    
    automation["updatedAt"] = _now_iso()
    _save_to_disk(openclaw_dir)
    
    handler._send_json({"ok": True, "automation": _decorate_automation(automation)})
    return True


def handle_automation_delete(handler, services, automation_id):
    """DELETE /api/automations/{id} - Delete automation."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    if _find_management_rule_automation(openclaw_dir, services, automation_id):
        handler._send_json({"ok": False, "error": "read_only", "message": "内置运营规则不能在这里删除。"}, status=400)
        return True
    
    automation = _automations_db.pop(automation_id, None)
    if not automation:
        handler._send_json({"ok": False, "error": "not_found", "message": "自动化任务不存在"}, status=404)
        return True
    
    _save_to_disk(openclaw_dir)
    handler._send_json({"ok": True, "message": "自动化任务已删除"})
    return True


def handle_automation_toggle(handler, services, automation_id):
    """POST /api/automations/{id}/toggle - Enable/disable automation."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    if _find_management_rule_automation(openclaw_dir, services, automation_id):
        handler._send_json({"ok": False, "error": "read_only", "message": "内置运营规则请前往管理页启停。"}, status=400)
        return True
    
    automation = _automations_db.get(automation_id)
    if not automation:
        handler._send_json({"ok": False, "error": "not_found", "message": "自动化任务不存在"}, status=404)
        return True
    
    try:
        body = json.loads(handler._read_body() or "{}")
        enabled = body.get("enabled", not automation.get("enabled", True))
    except json.JSONDecodeError:
        enabled = not automation.get("enabled", True)
    
    automation["enabled"] = enabled
    automation["updatedAt"] = _now_iso()
    
    _save_to_disk(openclaw_dir)
    handler._send_json({"ok": True, "automation": automation})
    return True


def handle_automation_run(handler, services, automation_id):
    """POST /api/automations/{id}/run - Run automation immediately."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    if _find_management_rule_automation(openclaw_dir, services, automation_id):
        handler._send_json({"ok": False, "error": "read_only", "message": "这条系统规则由运营自动化引擎执行，不支持手动立即运行。"}, status=400)
        return True
    
    automation = _automations_db.get(automation_id)
    if not automation:
        handler._send_json({"ok": False, "error": "not_found", "message": "自动化任务不存在"}, status=404)
        return True
    
    ok, run, error_code = _start_automation_run(
        openclaw_dir,
        automation_id,
        automation,
        trigger_source="manual",
    )
    if ok:
        handler._send_json({"ok": True, "run": run})
        return True
    if error_code == "workflow_not_found":
        handler._send_json({"ok": False, "error": error_code, "message": "关联的工作流不存在。", "run": run}, status=404)
        return True
    handler._send_json({"ok": False, "error": error_code or "automation_run_failed", "message": run.get("error") or "自动化执行失败。", "run": run}, status=500)
    return True


def handle_automation_get_runs(handler, services, automation_id):
    """GET /api/automations/{id}/runs - Get automation run history."""
    openclaw_dir = _get_openclaw_dir(handler)
    _load_from_disk(openclaw_dir)
    management_automation = _find_management_rule_automation(openclaw_dir, services, automation_id)
    if management_automation:
        handler._send_json(
            {
                "ok": True,
                "runs": [],
                "source": "management-rule",
                "message": "系统运营规则由自动化引擎持续执行，这里暂不提供单规则运行记录。",
                "automation": management_automation,
            }
        )
        return True
    
    automation = _automations_db.get(automation_id)
    if not automation:
        handler._send_json({"ok": False, "error": "not_found", "message": "自动化任务不存在"}, status=404)
        return True
    
    runs = [run for run in _automation_runs_db.values() if run.get("automationId") == automation_id]
    runs.sort(key=lambda x: x.get("startedAt", ""), reverse=True)
    
    handler._send_json({"ok": True, "runs": runs})
    return True


def handle_automation_route(handler, services, path):
    """Route automation API requests."""
    method = handler.command
    
    # List automations
    if path == "/api/automations" and method == "GET":
        return handle_automation_get_list(handler, services)
    
    # Create automation
    if path == "/api/automations" and method == "POST":
        return handle_automation_create(handler, services)
    
    # Single automation operations
    if path.startswith("/api/automations/"):
        parts = path.split("/")
        if len(parts) >= 4:
            automation_id = parts[3]
            
            # Toggle automation
            if len(parts) == 5 and parts[4] == "toggle" and method == "POST":
                return handle_automation_toggle(handler, services, automation_id)
            
            # Run automation
            if len(parts) == 5 and parts[4] == "run" and method == "POST":
                return handle_automation_run(handler, services, automation_id)
            
            # Get automation runs
            if len(parts) == 5 and parts[4] == "runs" and method == "GET":
                return handle_automation_get_runs(handler, services, automation_id)
            
            # CRUD operations
            if len(parts) == 4:
                if method == "GET":
                    return handle_automation_get_detail(handler, services, automation_id)
                elif method == "PUT":
                    return handle_automation_update(handler, services, automation_id)
                elif method == "DELETE":
                    return handle_automation_delete(handler, services, automation_id)
    
    return False
