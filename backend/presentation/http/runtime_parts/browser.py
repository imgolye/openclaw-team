"""Runtime part: browser."""


def perform_browser_start(openclaw_dir, profile=""):
    env = openclaw_command_env(openclaw_dir)
    resolved_profile = resolve_browser_command_profile(openclaw_dir, requested_profile=profile, env=env)
    process = run_command([*openclaw_browser_command(resolved_profile), "start"], env=env)
    output = join_command_output(process)
    if process.returncode != 0:
        raise RuntimeError(output or "browser start 执行失败。")
    return {"output": output, "profile": resolved_profile}


def perform_browser_create_profile(openclaw_dir, name, driver="openclaw", color="", cdp_url=""):
    profile_name = str(name or "").strip()
    if not profile_name:
        raise RuntimeError("profile 名称不能为空。")
    env = openclaw_command_env(openclaw_dir)
    args = ["openclaw", "browser", "create-profile", "--name", profile_name]
    normalized_driver = str(driver or "").strip()
    if normalized_driver:
        args.extend(["--driver", normalized_driver])
    normalized_color = str(color or "").strip()
    if normalized_color:
        args.extend(["--color", normalized_color])
    normalized_cdp = str(cdp_url or "").strip()
    if normalized_cdp:
        args.extend(["--cdp-url", normalized_cdp])
    process = run_command(args, env=env)
    output = join_command_output(process)
    if process.returncode != 0:
        raise RuntimeError(output or "browser profile 创建失败。")
    return {"name": profile_name, "output": output}


def perform_browser_open(openclaw_dir, url, profile=""):
    normalized_url = str(url or "").strip()
    if not normalized_url:
        raise RuntimeError("URL 不能为空。")
    env = openclaw_command_env(openclaw_dir)
    resolved_profile = resolve_browser_command_profile(openclaw_dir, requested_profile=profile, env=env)
    process = run_command([*openclaw_browser_command(resolved_profile), "open", normalized_url], env=env)
    output = join_command_output(process)
    if process.returncode != 0:
        raise RuntimeError(output or "browser open 执行失败。")
    return {"url": normalized_url, "output": output, "profile": resolved_profile}


def perform_browser_snapshot(openclaw_dir, profile="", selector="", target_id="", limit=120):
    env = openclaw_command_env(openclaw_dir)
    resolved_profile = resolve_browser_command_profile(openclaw_dir, requested_profile=profile, env=env)
    args = [*openclaw_browser_command(resolved_profile), "snapshot", "--format", "ai", "--limit", str(limit)]
    normalized_selector = str(selector or "").strip()
    normalized_target = str(target_id or "").strip()
    if normalized_selector:
        args.extend(["--selector", normalized_selector])
    if normalized_target:
        args.extend(["--target-id", normalized_target])
    process = run_command(args, env=env)
    output = join_command_output(process)
    if process.returncode != 0:
        raise RuntimeError(output or "browser snapshot 执行失败。")
    return {"output": output, "profile": resolved_profile}


def perform_browser_plan(openclaw_dir, steps, profile=""):
    if not isinstance(steps, list) or not steps:
        raise RuntimeError("browser plan 不能为空，且必须是动作数组。")
    env = openclaw_command_env(openclaw_dir)
    resolved_profile = resolve_browser_command_profile(openclaw_dir, requested_profile=profile, env=env)
    results = []
    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise RuntimeError(f"第 {index} 步必须是对象。")
        action = str(step.get("action", "")).strip().lower()
        target_id = str(step.get("targetId", "")).strip()
        args = [*openclaw_browser_command(resolved_profile)]
        if action == "open":
            url = str(step.get("url", "")).strip()
            if not url:
                raise RuntimeError(f"第 {index} 步 open 缺少 url。")
            args.extend(["open", url])
        elif action == "snapshot":
            args.extend(["snapshot", "--format", str(step.get("format", "ai") or "ai"), "--limit", str(step.get("limit", 120) or 120)])
            selector = str(step.get("selector", "")).strip()
            if selector:
                args.extend(["--selector", selector])
        elif action == "click":
            ref = str(step.get("ref", "")).strip()
            if not ref:
                raise RuntimeError(f"第 {index} 步 click 缺少 ref。")
            args.extend(["click", ref])
            if step.get("double"):
                args.append("--double")
        elif action == "wait":
            args.append("wait")
            if step.get("time") is not None:
                args.extend(["--time", str(step.get("time"))])
            if step.get("selector"):
                args.append(str(step.get("selector")))
            if step.get("text"):
                args.extend(["--text", str(step.get("text"))])
            if step.get("url"):
                args.extend(["--url", str(step.get("url"))])
        elif action == "fill":
            fields = step.get("fields", [])
            if not isinstance(fields, list) or not fields:
                raise RuntimeError(f"第 {index} 步 fill 缺少 fields。")
            args.extend(["fill", "--fields", json.dumps(fields, ensure_ascii=False)])
        else:
            raise RuntimeError(f"第 {index} 步是不支持的 browser 动作：{action}")

        if target_id:
            args.extend(["--target-id", target_id])
        process = run_command(args, env=env)
        output = join_command_output(process)
        if process.returncode != 0:
            raise RuntimeError(output or f"第 {index} 步 {action} 执行失败。")
        results.append({"index": index, "action": action, "output": output})
    return {"results": results, "profile": resolved_profile}
