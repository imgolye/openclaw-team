"""Runtime part: gateway."""

def ensure_gateway_ready_for_team_wake(openclaw_dir):
    status_error = ""
    try:
        status = perform_gateway_service_action(openclaw_dir, "status")
        payload = status.get("payload") if isinstance(status.get("payload"), dict) else {}
        rpc = payload.get("rpc") if isinstance(payload.get("rpc"), dict) else {}
        service = payload.get("service") if isinstance(payload.get("service"), dict) else {}
        runtime = service.get("runtime") if isinstance(service.get("runtime"), dict) else {}
        runtime_status = str(runtime.get("status") or "").strip().lower()
        if rpc.get("ok") or runtime_status in {"running", "active"}:
            return {"action": "status", "result": status}
    except Exception as error:
        status_error = str(error or "").strip()
    try:
        started = perform_gateway_service_action(openclaw_dir, "start")
        return {"action": "start", "result": started, "statusError": status_error}
    except Exception as error:
        message = str(error or "").strip() or status_error or "无法拉起 OpenClaw Gateway。"
        raise RuntimeError(message)


def perform_gateway_service_action(openclaw_dir, action):
    action_name = str(action or "").strip().lower()
    if action_name not in {"start", "restart", "stop", "status"}:
        raise RuntimeError(f"不支持的 gateway 动作：{action_name}")
    env = openclaw_command_env(openclaw_dir)
    args = ["openclaw", "gateway", action_name]
    if action_name == "status":
        args.extend(["--require-rpc", "--json"])
    process = run_command(args, env=env)
    payload = parse_json_payload(process.stdout, process.stderr, default=None)
    output = join_command_output(process)
    if process.returncode != 0 and payload is None:
        raise RuntimeError(output or f"gateway {action_name} 执行失败。")
    return {"action": action_name, "payload": payload, "output": output}
