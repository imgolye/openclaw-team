"""Runtime part: task_execution."""

def _log_task_execution_dispatch_future_exception(future):
    try:
        error = future.exception()
    except Exception:
        logging.exception("task execution dispatch future inspection failed")
        return
    if error is not None:
        logging.exception("task execution dispatch worker failed", exc_info=error)


def _discard_task_execution_dispatch_future(future):
    with TASK_EXECUTION_DISPATCH_FUTURES_LOCK:
        TASK_EXECUTION_DISPATCH_FUTURES.discard(future)


def submit_task_execution_background(worker):
    dispatch_future = TASK_EXECUTION_DISPATCH_POOL.submit(worker)
    with TASK_EXECUTION_DISPATCH_FUTURES_LOCK:
        TASK_EXECUTION_DISPATCH_FUTURES.add(dispatch_future)
    if hasattr(dispatch_future, "add_done_callback"):
        dispatch_future.add_done_callback(_log_task_execution_dispatch_future_exception)
        dispatch_future.add_done_callback(_discard_task_execution_dispatch_future)
    else:
        _log_task_execution_dispatch_future_exception(dispatch_future)
        _discard_task_execution_dispatch_future(dispatch_future)
    return dispatch_future


def submit_task_execution_background_deferred(worker, delay_seconds=0.0):
    delay = max(float(delay_seconds or 0.0), 0.0)

    def launcher():
        if delay:
            time.sleep(delay)
        submit_task_execution_background(worker)

    thread = threading.Thread(target=launcher, daemon=True)
    thread.start()
    return thread


def wait_for_task_execution_dispatches(timeout=5.0):
    deadline = time.time() + max(float(timeout or 0.0), 0.0)
    while True:
        with TASK_EXECUTION_DISPATCH_FUTURES_LOCK:
            pending = [future for future in TASK_EXECUTION_DISPATCH_FUTURES if not future.done()]
            done = [future for future in TASK_EXECUTION_DISPATCH_FUTURES if future.done()]
            for future in done:
                TASK_EXECUTION_DISPATCH_FUTURES.discard(future)
        if not pending:
            return
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        concurrent.futures.wait(pending, timeout=min(remaining, 0.2), return_when=concurrent.futures.FIRST_COMPLETED)


def sync_task_execution_progress_backlog(openclaw_dir, config=None, tasks=None):
    config = config or load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    task_items = sorted(
        list(tasks if isinstance(tasks, list) else merge_tasks(openclaw_dir, config)),
        key=lambda item: parse_iso((item or {}).get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    synced = []
    skipped = []

    for task in task_items:
        if not isinstance(task, dict):
            continue
        if len(synced) >= TASK_EXECUTION_SYNC_BATCH_SIZE:
            skipped.append({"taskId": str(task.get("id") or "").strip(), "reason": "batch_limit"})
            continue
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES or task.get("output"):
            continue
        bootstrap = task_execution_bootstrap_for_task(task)
        if str(bootstrap.get("status") or "").strip().lower() not in {"dispatched", "scheduled", "failed"}:
            skipped.append({"taskId": task_id, "reason": "not_dispatched"})
            continue
        completion_candidate = latest_task_execution_completion_candidate(openclaw_dir, config, task)
        if completion_candidate:
            skipped.append({"taskId": task_id, "reason": "completion_pending"})
            continue
        candidate = latest_task_execution_sync_candidate(openclaw_dir, task)
        if not candidate:
            skipped.append({"taskId": task_id, "reason": "no_candidate"})
            continue
        sync_meta = task_execution_sync_for_task(task)
        if candidate.get("messageId") and candidate.get("messageId") == sync_meta.get("messageId"):
            skipped.append({"taskId": task_id, "reason": "already_synced"})
            continue
        if task_has_progress_after(task, candidate.get("at"), ignore_texts=[candidate.get("text", "")]):
            skipped.append({"taskId": task_id, "reason": "progress_newer"})
            continue
        text = str(candidate.get("text") or "").strip()
        if not text or text == str(task.get("currentUpdate") or "").strip():
            skipped.append({"taskId": task_id, "reason": "same_text"})
            continue
        perform_task_progress(openclaw_dir, task_id, text, mark_doing=False)
        sync_state = {
            "messageId": candidate.get("messageId", ""),
            "at": candidate.get("at", ""),
            "text": text,
            "agentId": candidate.get("agentId", ""),
            "sessionId": candidate.get("sessionId", ""),
        }
        update_task_execution_sync_state(openclaw_dir, task_id, sync_state, router_agent_id=router_agent_id)
        update_task_execution_bootstrap(
            openclaw_dir,
            task_id,
            str(candidate.get("agentId") or bootstrap.get("agentId") or task.get("targetAgentId") or "").strip(),
            "dispatched",
            note="已捕获目标 Agent 的后续进展，执行链路恢复为已派发。",
            session_id=str(candidate.get("sessionId") or bootstrap.get("sessionId") or task_execution_session_id(task_id)).strip(),
            router_agent_id=router_agent_id,
            title=str(task.get("title") or task_id),
        )
        append_audit_event(
            openclaw_dir,
            "task_execution_sync",
            "system",
            detail=f"自动回写任务进展：{task_id}",
            meta={"taskId": task_id, **sync_state},
        )
        synced.append({"taskId": task_id, "summary": text, "agentId": candidate.get("agentId", "")})

    return {
        "count": len(synced),
        "items": synced[:12],
        "skippedCount": len(skipped),
        "skipped": skipped[:12],
    }


def sync_task_execution_completion_backlog(openclaw_dir, config=None, tasks=None):
    config = config or load_config(openclaw_dir)
    router_agent_id = get_router_agent_id(config)
    task_items = sorted(
        list(tasks if isinstance(tasks, list) else merge_tasks(openclaw_dir, config)),
        key=lambda item: parse_iso((item or {}).get("updatedAt")) or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    completed = []
    skipped = []

    for task in task_items:
        if not isinstance(task, dict):
            continue
        if len(completed) >= TASK_EXECUTION_COMPLETION_SYNC_BATCH_SIZE:
            skipped.append({"taskId": str(task.get("id") or "").strip(), "reason": "batch_limit"})
            continue
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            continue
        state = str(task.get("state") or task.get("status") or "").strip().lower()
        if state in TERMINAL_STATES:
            continue
        candidate = latest_task_execution_completion_candidate(openclaw_dir, config, task)
        if not candidate:
            skipped.append({"taskId": task_id, "reason": "no_candidate"})
            continue
        summary = str(candidate.get("text") or "").strip() or "任务已完成。"
        output_path = str(candidate.get("outputPath") or "").strip()
        try:
            perform_task_done(openclaw_dir, task_id, output_path=output_path, summary=summary)
        except Exception as error:
            logging.error("sync_task_execution_completion_backlog: perform_task_done failed for task %s: %s", task_id, error)
            append_audit_event(
                openclaw_dir,
                "task_execution_complete_sync_error",
                "system",
                detail=f"自动收口任务失败：{task_id}",
                meta={"taskId": task_id, "error": str(error)},
            )
            skipped.append({"taskId": task_id, "reason": f"done_failed:{error}"})
            continue
        sync_state = {
            "messageId": candidate.get("messageId", ""),
            "at": candidate.get("at", ""),
            "text": summary,
            "agentId": candidate.get("agentId", ""),
            "sessionId": candidate.get("sessionId", ""),
            "outputPath": output_path,
            "completion": True,
        }
        update_task_execution_sync_state(openclaw_dir, task_id, sync_state, router_agent_id=router_agent_id)
        append_audit_event(
            openclaw_dir,
            "task_execution_complete_sync",
            "system",
            detail=f"自动收口任务完成：{task_id}",
            meta={"taskId": task_id, **sync_state},
        )
        completed.append(
            {
                "taskId": task_id,
                "summary": summary,
                "agentId": candidate.get("agentId", ""),
                "outputPath": output_path,
            }
        )
    return {
        "count": len(completed),
        "items": completed[:12],
        "skippedCount": len(skipped),
        "skipped": skipped[:12],
    }
