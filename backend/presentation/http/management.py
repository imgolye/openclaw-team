from __future__ import annotations

import secrets


def _action_dashboard(handler, svc, sections):
    svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
    svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
    data = svc.build_dashboard_state(handler.server.openclaw_dir)
    return handler._action_dashboard_payload(data, sections=sections)


def _handle_management_commands(handler, path, payload, svc):
    if path == "/api/actions/management/run/create":
        if not handler._require_capability("taskWrite", "当前账号没有创建端到端管理 Run 的权限。"):
            return True
        title = str(payload.get("title", "")).strip()
        if not title:
            raise RuntimeError("管理 Run 标题不能为空。")
        owner = str(payload.get("owner", "")).strip() or svc.session_for_client(handler._session()).get("displayName", "OpenClaw Team")
        linked_task_id = str(payload.get("linkedTaskId", "")).strip()
        linked_team_id = str(payload.get("linkedTeamId", "")).strip()
        linked_team = svc.resolve_agent_team_record(handler.server.openclaw_dir, linked_team_id) if linked_team_id else None
        current_tasks = svc.merge_tasks(handler.server.openclaw_dir, svc.load_config(handler.server.openclaw_dir))
        linked_task = next((item for item in current_tasks if item.get("id") == linked_task_id), None)
        run_id = secrets.token_hex(6)
        workflow_resolution = svc.resolve_run_workflow_binding(
            handler.server.openclaw_dir,
            workflow_id=str(payload.get("workflowId", "")).strip(),
            linked_task=linked_task,
        )
        project_dir = svc.resolve_planning_project_dir(handler.server.openclaw_dir)
        planning_binding = (
            svc.ensure_planning_bundle(
                handler.server.openclaw_dir,
                project_dir,
                "run",
                run_id,
                title=title,
                goal=str(payload.get("goal", "")).strip() or title,
                meta={
                    "runId": run_id,
                    "linkedTaskId": linked_task_id,
                    "linkedTeamId": linked_team_id,
                    "workflowBinding": workflow_resolution.get("binding", {}),
                },
            )
            if project_dir
            else {}
        )
        linked_agent_id = str(payload.get("linkedAgentId", "")).strip() or str((linked_team or {}).get("leadAgentId") or "").strip()
        run = svc.store_create_management_run(
            handler.server.openclaw_dir,
            {
                "id": run_id,
                "title": title,
                "goal": str(payload.get("goal", "")).strip(),
                "owner": owner,
                "linkedTaskId": linked_task_id,
                "linkedAgentId": linked_agent_id,
                "linkedTeamId": linked_team_id,
                "linkedSessionKey": str(payload.get("linkedSessionKey", "")).strip(),
                "releaseChannel": str(payload.get("releaseChannel", "")).strip() or "manual",
                "riskLevel": str(payload.get("riskLevel", "")).strip() or "medium",
                "stages": workflow_resolution.get("stages", []),
                "workflowBinding": workflow_resolution.get("binding", {}),
                "planningBinding": planning_binding,
            },
        )
        handler._audit(
            "management_run_create",
            detail=f"创建端到端管理 Run {run['title']}",
            meta={
                "runId": run["id"],
                "linkedTaskId": run.get("linkedTaskId", ""),
                "linkedTeamId": run.get("linkedTeamId", ""),
                "workflowId": (run.get("workflowBinding") or {}).get("workflowId", ""),
            },
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": (
                    f"端到端管理 Run {run['title']} 已建立，并绑定 Team {linked_team.get('name', '')}。"
                    if linked_team
                    else f"端到端管理 Run {run['title']} 已建立。"
                ),
                "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run["id"]),
            }
        )
        return True

    if path == "/api/actions/management/run/update":
        if not handler._require_capability("taskWrite", "当前账号没有推进端到端管理 Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        action = str(payload.get("action", "")).strip().lower()
        if not run_id or not action:
            raise RuntimeError("请提供 Run 编号和动作。")
        run = svc.store_update_management_run(
            handler.server.openclaw_dir,
            run_id,
            action,
            note=str(payload.get("note", "")).strip(),
            risk_level=str(payload.get("riskLevel", "")).strip(),
            linked_task_id=str(payload.get("linkedTaskId", "")).strip(),
        )
        handler._audit(
            "management_run_update",
            detail=f"更新端到端管理 Run {run_id}",
            meta={"runId": run_id, "action": action, "stageKey": run.get("stageKey", "")},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": f"端到端管理 Run {run.get('title', run_id)} 已更新。",
                "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", run_id)),
            }
        )
        return True

    if path == "/api/actions/management/run/link-pack":
        if not handler._require_capability("taskWrite", "当前账号没有绑定 workflow pack 到 Run 的权限。"):
            return True
        run_id = str(payload.get("runId", "")).strip()
        pack = svc.resolve_workflow_pack_or_mode_record(
            handler.server.openclaw_dir,
            str(payload.get("packId", "")).strip(),
            mode=str(payload.get("mode", "")).strip(),
            target="run",
        )
        run = svc.perform_workflow_pack_launch_to_run(
            handler.server.openclaw_dir,
            pack,
            {
                "runId": run_id,
                "source": str(payload.get("source") or "management").strip() or "management",
            },
            handler._current_actor(),
        )
        handler._audit(
            "management_run_link_pack",
            detail=f"为 Run {run.get('title', run_id)} 绑定 workflow pack {pack.get('name', pack.get('id', ''))}",
            meta={"runId": run.get("id", run_id), "packId": pack.get("id", "")},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": f"Run {run.get('title', run_id)} 已绑定到 {pack.get('name', pack.get('id', 'workflow pack'))}。",
                "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", run_id)),
            }
        )
        return True

    if path == "/api/actions/management/run/gate/update":
        if not handler._require_capability("taskWrite", "当前账号没有更新 review gates 的权限。"):
            return True
        run = svc.perform_management_run_gate_update(
            handler.server.openclaw_dir,
            str(payload.get("runId", "")).strip(),
            str(payload.get("gateId", "")).strip(),
            str(payload.get("action", "")).strip(),
            note=str(payload.get("note", "")).strip(),
        )
        handler._audit(
            "management_run_gate_update",
            detail=f"更新 Run {run.get('title', run.get('id', ''))} 的 review gate",
            meta={"runId": run.get("id", ""), "gateId": str(payload.get("gateId", "")).strip(), "action": str(payload.get("action", "")).strip()},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": "Review gate 已更新。", "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", ""))})
        return True

    if path == "/api/actions/management/run/artifact/save":
        if not handler._require_capability("taskWrite", "当前账号没有维护 Run artifacts 的权限。"):
            return True
        run = svc.perform_management_run_artifact_save(handler.server.openclaw_dir, str(payload.get("runId", "")).strip(), payload)
        handler._audit(
            "management_run_artifact_save",
            detail=f"为 Run {run.get('title', run.get('id', ''))} 保存 artifact",
            meta={"runId": run.get("id", ""), "artifactType": str(payload.get("type", "")).strip()},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": "Run artifact 已保存。", "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", ""))})
        return True

    if path == "/api/actions/management/run/browser":
        if not handler._require_capability("taskWrite", "当前账号没有操作 Run browser runtime 的权限。"):
            return True
        run, result = svc.perform_management_run_browser_action(handler.server.openclaw_dir, str(payload.get("runId", "")).strip(), payload)
        handler._audit(
            "management_run_browser_action",
            detail=f"执行 Run {run.get('title', run.get('id', ''))} 的 browser 动作",
            meta={"runId": run.get("id", ""), "action": str(payload.get("action", "")).strip()},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": "Browser runtime 动作已执行。",
                "result": result,
                "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", "")),
            }
        )
        return True

    if path == "/api/actions/management/run/release/ship":
        if not handler._require_capability("taskWrite", "当前账号没有执行 release automation 的权限。"):
            return True
        run, result = svc.perform_management_run_release_ship(handler.server.openclaw_dir, str(payload.get("runId", "")).strip(), payload)
        handler._audit(
            "management_run_release_ship",
            detail=f"执行 Run {run.get('title', run.get('id', ''))} 的 release automation",
            meta={"runId": run.get("id", ""), "provider": result.get("provider", "")},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": "Release automation 已执行。",
                "result": result,
                "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", "")),
            }
        )
        return True

    if path == "/api/actions/management/run/qa/self-heal":
        if not handler._require_capability("taskWrite", "当前账号没有执行 QA self-heal 的权限。"):
            return True
        run, followup = svc.perform_management_run_qa_self_heal(handler.server.openclaw_dir, str(payload.get("runId", "")).strip(), payload)
        handler._audit(
            "management_run_qa_self_heal",
            detail=f"执行 Run {run.get('title', run.get('id', ''))} 的 QA self-heal",
            meta={"runId": run.get("id", ""), "followupTaskId": (followup or {}).get("taskId", "")},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": "QA self-heal 已执行。",
                "followup": followup or {},
                "run": svc.build_management_run_snapshot(handler.server.openclaw_dir, run.get("id", "")),
            }
        )
        return True

    if path == "/api/actions/management/rule/save":
        if not handler._require_capability("taskWrite", "当前账号没有配置自动化规则的权限。"):
            return True
        name = str(payload.get("name", "")).strip()
        trigger_type = str(payload.get("triggerType", "")).strip()
        if not name or not trigger_type:
            raise RuntimeError("请填写规则名称和触发类型。")
        rule = svc.store_save_automation_rule(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": name,
                "description": str(payload.get("description", "")).strip(),
                "status": str(payload.get("status", "")).strip() or "active",
                "triggerType": trigger_type,
                "thresholdMinutes": int(payload.get("thresholdMinutes") or 0),
                "cooldownMinutes": int(payload.get("cooldownMinutes") or 60),
                "severity": str(payload.get("severity", "")).strip() or "warning",
                "matchText": str(payload.get("matchText", "")).strip(),
                "channelIds": payload.get("channelIds") if isinstance(payload.get("channelIds"), list) else [],
                "meta": {
                    "dailyReview": svc.normalize_daily_review_schedule(
                        payload.get("dailyReview") if isinstance(payload.get("dailyReview"), dict) else {}
                    ),
                    "weeklyReport": svc.normalize_weekly_report_schedule(
                        payload.get("weeklyReport") if isinstance(payload.get("weeklyReport"), dict) else {}
                    ),
                    "escalationSteps": svc.normalize_escalation_steps(
                        payload.get("escalationSteps") if isinstance(payload.get("escalationSteps"), list) else []
                    ),
                },
            },
        )
        handler._audit(
            "management_rule_save",
            detail=f"保存自动化规则 {rule['name']}",
            meta={"ruleId": rule["id"], "triggerType": rule["triggerType"]},
        )
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        svc.clear_cached_payloads()
        svc.run_automation_engine_cycle(handler.server.openclaw_dir, source="manual")
        data = handler._runtime_data(svc.build_dashboard_state(handler.server.openclaw_dir))
        handler._send_json({"ok": True, "message": f"自动化规则 {rule['name']} 已保存。", "rule": rule, "dashboard": handler._action_dashboard_payload(data, sections=["management"])})
        return True

    if path == "/api/actions/management/channel/save":
        if not handler._require_capability("adminWrite", "当前账号没有配置通知渠道的权限。"):
            return True
        name = str(payload.get("name", "")).strip()
        channel_type = str(payload.get("type", "")).strip()
        if not name or not channel_type:
            raise RuntimeError("请填写通知渠道名称和类型。")
        channel = svc.store_save_notification_channel(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": name,
                "type": channel_type,
                "status": str(payload.get("status", "")).strip() or "active",
                "target": str(payload.get("target", "")).strip(),
                "secret": str(payload.get("secret", "")).strip(),
                "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
            },
        )
        handler._audit(
            "management_channel_save",
            detail=f"保存通知渠道 {channel['name']}",
            meta={"channelId": channel["id"], "type": channel["type"]},
        )
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        svc.clear_cached_payloads()
        svc.run_automation_engine_cycle(handler.server.openclaw_dir, source="manual")
        data = handler._runtime_data(svc.build_dashboard_state(handler.server.openclaw_dir))
        handler._send_json({"ok": True, "message": f"通知渠道 {channel['name']} 已保存。", "channel": channel, "dashboard": handler._action_dashboard_payload(data, sections=["management"])})
        return True

    if path == "/api/actions/management/customer-channel/save":
        if not handler._require_capability("adminWrite", "当前账号没有配置客户接入渠道的权限。"):
            return True
        name = str(payload.get("name", "")).strip()
        channel_type = str(payload.get("type", "")).strip()
        if not name or not channel_type:
            raise RuntimeError("请填写客户入口名称和类型。")
        channel = svc.store_save_customer_access_channel(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": name,
                "type": channel_type,
                "status": str(payload.get("status", "")).strip() or "active",
                "target": str(payload.get("target", "")).strip(),
                "entryUrl": str(payload.get("entryUrl", "")).strip(),
                "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
            },
        )
        handler._audit(
            "management_customer_channel_save",
            detail=f"保存客户接入渠道 {channel['name']}",
            meta={"channelId": channel["id"], "type": channel["type"]},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"客户接入渠道 {channel['name']} 已保存。", "channel": channel, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/customer-channel/voice-test":
        if not handler._require_capability("adminWrite", "当前账号没有测试客户接入渠道语音回复的权限。"):
            return True
        name = str(payload.get("name", "")).strip() or "Customer Channel"
        channel_type = str(payload.get("type", "")).strip()
        if not channel_type:
            raise RuntimeError("请先选择客户入口类型。")
        result = svc.perform_customer_channel_voice_test(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip() or "ad-hoc",
                "name": name,
                "type": channel_type,
                "status": str(payload.get("status", "")).strip() or "active",
                "target": str(payload.get("target", "")).strip(),
                "entryUrl": str(payload.get("entryUrl", "")).strip(),
                "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
            },
            sample_text=str(payload.get("voiceTestText", "")).strip(),
            preferred_agent_id=str(payload.get("agentId", "")).strip(),
        )
        preview = result.get("preview") if isinstance(result.get("preview"), dict) else {}
        handler._audit(
            "management_customer_channel_voice_test",
            detail=f"预演客户接入渠道语音回复 {name}",
            meta={
                "channelType": channel_type,
                "voiceReplyMode": str(preview.get("voiceReplyMode") or "").strip(),
                "effectiveVoice": str(preview.get("effectiveVoice") or "").strip(),
            },
        )
        handler._send_json(result)
        return True

    if path == "/api/actions/management/channel/test":
        if not handler._require_capability("adminWrite", "当前账号没有测试通知渠道的权限。"):
            return True
        channel = {
            "id": str(payload.get("id", "")).strip() or "ad-hoc",
            "name": str(payload.get("name", "")).strip() or "Test Channel",
            "type": str(payload.get("type", "")).strip(),
            "target": str(payload.get("target", "")).strip(),
            "secret": str(payload.get("secret", "")).strip(),
        }
        if not channel["type"]:
            raise RuntimeError("请先选择通知渠道类型。")
        result = svc.send_notification_message(
            channel,
            {
                "title": "OpenClaw Team Test Ping",
                "detail": "这是一条来自闭环运营中心的测试通知，说明渠道配置已经可用。",
            },
            openclaw_dir=handler.server.openclaw_dir,
        )
        handler._audit(
            "management_channel_test",
            detail=f"测试通知渠道 {channel['name']}",
            meta={"channelType": channel["type"], "target": svc.summarize_notification_target(channel)},
        )
        handler._send_json({"ok": True, "message": f"测试通知已发送到 {svc.summarize_notification_target(channel)}。", "result": result})
        return True

    if path in {"/api/actions/management/channel/disable", "/api/actions/management/channel/enable"}:
        if not handler._require_capability("adminWrite", "当前账号没有调整通知渠道状态的权限。"):
            return True
        channel_id = str(payload.get("channelId", "")).strip()
        channel = (
            svc.perform_disable_notification_channel(handler.server.openclaw_dir, channel_id)
            if path.endswith("/disable")
            else svc.perform_enable_notification_channel(handler.server.openclaw_dir, channel_id)
        )
        handler._audit(
            "management_channel_disable" if path.endswith("/disable") else "management_channel_enable",
            detail=f"{'停用' if path.endswith('/disable') else '重新启用'}通知渠道 {channel['name']}",
            meta={"channelId": channel["id"], "type": channel["type"]},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": f"通知渠道 {channel['name']} 已{'停用' if path.endswith('/disable') else '重新启用'}。",
                "channel": channel,
                "dashboard": _action_dashboard(handler, svc, sections=["management"]),
            }
        )
        return True

    if path in {"/api/actions/management/customer-channel/disable", "/api/actions/management/customer-channel/enable"}:
        if not handler._require_capability("adminWrite", "当前账号没有调整客户接入渠道状态的权限。"):
            return True
        channel_id = str(payload.get("channelId", "")).strip()
        channel = (
            svc.perform_disable_customer_access_channel(handler.server.openclaw_dir, channel_id)
            if path.endswith("/disable")
            else svc.perform_enable_customer_access_channel(handler.server.openclaw_dir, channel_id)
        )
        handler._audit(
            "management_customer_channel_disable" if path.endswith("/disable") else "management_customer_channel_enable",
            detail=f"{'停用' if path.endswith('/disable') else '重新启用'}客户接入渠道 {channel['name']}",
            meta={"channelId": channel["id"], "type": channel["type"]},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json(
            {
                "ok": True,
                "message": f"客户接入渠道 {channel['name']} 已{'停用' if path.endswith('/disable') else '重新启用'}。",
                "channel": channel,
                "dashboard": _action_dashboard(handler, svc, sections=["management"]),
            }
        )
        return True

    if path == "/api/actions/management/rule/add-backup-channel":
        if not handler._require_capability("taskWrite", "当前账号没有调整自动化规则的权限。"):
            return True
        rule = svc.perform_append_rule_backup_channel(
            handler.server.openclaw_dir,
            str(payload.get("ruleId", "")).strip(),
            str(payload.get("channelId", "")).strip(),
            after_minutes=int(payload.get("afterMinutes") or 15),
            label=str(payload.get("label", "")).strip(),
        )
        handler._audit(
            "management_rule_add_backup_channel",
            detail=f"给规则 {rule['name']} 补兜底通道",
            meta={"ruleId": rule["id"], "channelIds": svc.rule_channel_ids(rule)},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"规则 {rule['name']} 已补入兜底通道。", "rule": rule, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/rule/add-manual-escalation":
        if not handler._require_capability("taskWrite", "当前账号没有调整自动化规则的权限。"):
            return True
        rule = svc.perform_append_rule_manual_escalation(
            handler.server.openclaw_dir,
            str(payload.get("ruleId", "")).strip(),
            after_minutes=int(payload.get("afterMinutes") or 30),
            label=str(payload.get("label", "")).strip(),
        )
        handler._audit("management_rule_add_manual_escalation", detail=f"给规则 {rule['name']} 补人工接管", meta={"ruleId": rule["id"]})
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"规则 {rule['name']} 已补人工接管节点。", "rule": rule, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/rule/pause":
        if not handler._require_capability("taskWrite", "当前账号没有调整自动化规则的权限。"):
            return True
        rule = svc.perform_pause_automation_rule(handler.server.openclaw_dir, str(payload.get("ruleId", "")).strip())
        handler._audit("management_rule_pause", detail=f"暂停规则 {rule['name']}", meta={"ruleId": rule["id"]})
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"规则 {rule['name']} 已暂停。", "rule": rule, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/rule/tune":
        if not handler._require_capability("taskWrite", "当前账号没有调整自动化规则的权限。"):
            return True
        rule = svc.perform_tune_automation_rule(
            handler.server.openclaw_dir,
            str(payload.get("ruleId", "")).strip(),
            threshold_minutes=payload.get("thresholdMinutes") if "thresholdMinutes" in payload else None,
            cooldown_minutes=payload.get("cooldownMinutes") if "cooldownMinutes" in payload else None,
            source="manual",
        )
        handler._audit(
            "management_rule_tune",
            detail=f"调整规则 {rule['name']}",
            meta={"ruleId": rule["id"], "thresholdMinutes": rule.get("thresholdMinutes", 0), "cooldownMinutes": rule.get("cooldownMinutes", 0)},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"规则 {rule['name']} 已更新阈值与冷却时间。", "rule": rule, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/alert/update":
        if not handler._require_capability("taskWrite", "当前账号没有处置运营告警的权限。"):
            return True
        alert = svc.perform_update_automation_alert_status(
            handler.server.openclaw_dir,
            str(payload.get("alertId", "")).strip(),
            str(payload.get("status", "")).strip(),
            actor=handler._current_actor(),
        )
        action_map = {"notified": "确认接手", "resolved": "关闭", "open": "重新打开"}
        handler._audit(
            "management_alert_update",
            detail=f"{action_map.get(alert['status'], '更新')}运营告警 {alert['title']}",
            meta={"alertId": alert["id"], "status": alert["status"], "ruleId": alert["ruleId"]},
        )
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"告警已{action_map.get(alert['status'], '更新')}。", "alert": alert, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/bootstrap":
        if not handler._require_capability("taskWrite", "当前账号没有初始化运营规则的权限。"):
            return True
        result = svc.bootstrap_management_rules(handler.server.openclaw_dir)
        svc.ensure_default_management_bootstrap(
            handler.server.openclaw_dir,
            svc.load_project_metadata(handler.server.openclaw_dir, config=svc.load_config(handler.server.openclaw_dir)),
        )
        handler._audit("management_bootstrap", detail="初始化默认闭环运营规则", meta={"created": result["total"]})
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"已补齐 {result['total']} 条默认运营规则。", "result": result, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/automation/mode":
        if not handler._require_capability("taskWrite", "当前账号没有调整运营模式的权限。"):
            return True
        mode = svc.perform_set_management_automation_mode(
            handler.server.openclaw_dir,
            str(payload.get("mode", "")).strip(),
            actor=handler._current_actor(),
        )
        handler._audit("management_automation_mode_update", detail=f"切换运营模式到 {mode['label']}", meta={"mode": mode["value"]})
        svc.invalidate_management_payload_cache(handler.server.openclaw_dir)
        handler._send_json({"ok": True, "message": f"运营模式已切换为 {mode['label']}。", "mode": mode, "dashboard": _action_dashboard(handler, svc, sections=["management"])})
        return True

    if path == "/api/actions/management/report/export":
        if not handler._require_capability("read", "当前账号没有导出运营周报的权限。"):
            return True
        report = svc.export_management_weekly_report(handler.server.openclaw_dir)
        handler._audit("management_report_export", detail="导出运营周报", meta={"path": report["path"]})
        handler._send_json({"ok": True, "message": f"运营周报已导出到 {report['path']}。", "report": report})
        return True

    if path == "/api/actions/orchestration/workflow/save":
        if not handler._require_capability("taskWrite", "当前账号没有编辑协作编排的权限。"):
            return True
        workflow = svc.store_save_orchestration_workflow(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": str(payload.get("name", "")).strip(),
                "description": str(payload.get("description", "")).strip(),
                "status": str(payload.get("status", "")).strip() or "active",
                "lanes": payload.get("lanes") if isinstance(payload.get("lanes"), list) else [],
                "nodes": payload.get("nodes") if isinstance(payload.get("nodes"), list) else [],
                "meta": payload.get("meta") if isinstance(payload.get("meta"), dict) else {},
            },
        )
        handler._audit("orchestration_workflow_save", detail=f"保存协作编排 {workflow['name']}", meta={"workflowId": workflow["id"], "nodeCount": len(workflow.get("nodes", []))})
        handler._send_json({"ok": True, "message": f"协作编排 {workflow['name']} 已保存。", "workflow": workflow, "dashboard": _action_dashboard(handler, svc, sections=["orchestration"])})
        return True

    if path == "/api/actions/orchestration/workflow/restore":
        if not handler._require_capability("taskWrite", "当前账号没有回退协作编排版本的权限。"):
            return True
        workflow_id = str(payload.get("workflowId", "")).strip()
        version_id = str(payload.get("versionId", "")).strip()
        workflow = svc.store_restore_orchestration_workflow_version(handler.server.openclaw_dir, workflow_id, version_id)
        handler._audit("orchestration_workflow_restore", detail=f"回退协作编排 {workflow['name']}", meta={"workflowId": workflow["id"], "versionId": version_id})
        handler._send_json({"ok": True, "message": f"协作编排 {workflow['name']} 已回退到指定版本。", "workflow": workflow, "dashboard": _action_dashboard(handler, svc, sections=["orchestration"])})
        return True

    if path == "/api/actions/orchestration/workflow/insert-approval":
        if not handler._require_capability("taskWrite", "当前账号没有调整协作编排的权限。"):
            return True
        adjustment = svc.perform_insert_workflow_approval_node(
            handler.server.openclaw_dir,
            workflow_id=str(payload.get("workflowId", "")).strip(),
            target_lane_id=str(payload.get("targetLaneId", "")).strip(),
            target_agent_id=str(payload.get("targetAgentId", "")).strip(),
            title=str(payload.get("title", "")).strip() or "人工复核",
            approver=str(payload.get("approver", "")).strip() or "运营负责人",
            timeout=int(payload.get("timeout") or 30),
            escalation_agent_id=str(payload.get("escalationAgentId", "")).strip(),
            reason=str(payload.get("reason", "")).strip(),
        )
        workflow = adjustment.get("workflow") or {}
        handler._audit(
            "orchestration_workflow_insert_approval",
            detail=f"为工作流 {workflow.get('name', adjustment.get('targetLaneTitle', ''))} 前置人工复核",
            meta={"workflowId": workflow.get("id", ""), "targetLaneId": adjustment.get("targetLaneId", ""), "inserted": adjustment.get("inserted", False)},
        )
        handler._send_json(
            {
                "ok": True,
                "message": (
                    f"已在 {adjustment.get('targetLaneTitle', '目标泳道')} 前置人工复核节点。"
                    if adjustment.get("inserted")
                    else f"{adjustment.get('targetLaneTitle', '目标泳道')} 已有人工复核节点。"
                ),
                "adjustment": adjustment,
                "workflow": workflow,
                "dashboard": _action_dashboard(handler, svc, sections=["orchestration"]),
            }
        )
        return True

    if path == "/api/actions/orchestration/workflow/strengthen-handoff":
        if not handler._require_capability("taskWrite", "当前账号没有调整协作编排的权限。"):
            return True
        adjustment = svc.perform_strengthen_workflow_handoff_note(
            handler.server.openclaw_dir,
            workflow_id=str(payload.get("workflowId", "")).strip(),
            node_id=str(payload.get("nodeId", "")).strip(),
            title=str(payload.get("title", "")).strip(),
            reason=str(payload.get("reason", "")).strip(),
        )
        workflow = adjustment.get("workflow") or {}
        handler._audit(
            "orchestration_workflow_strengthen_handoff",
            detail=f"强化工作流 {workflow.get('name', adjustment.get('nodeTitle', ''))} 的交接模板",
            meta={"workflowId": workflow.get("id", ""), "nodeId": adjustment.get("nodeId", ""), "updated": adjustment.get("updated", False)},
        )
        handler._send_json(
            {
                "ok": True,
                "message": (
                    f"已强化 {adjustment.get('nodeTitle', '目标节点')} 的交接模板。"
                    if adjustment.get("updated")
                    else f"{adjustment.get('nodeTitle', '目标节点')} 已是推荐交接模板。"
                ),
                "adjustment": adjustment,
                "workflow": workflow,
                "dashboard": _action_dashboard(handler, svc, sections=["orchestration"]),
            }
        )
        return True

    if path == "/api/actions/orchestration/policy/save":
        if not handler._require_capability("taskWrite", "当前账号没有编辑动态路由策略的权限。"):
            return True
        policy = svc.store_save_routing_policy(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": str(payload.get("name", "")).strip(),
                "status": str(payload.get("status", "")).strip() or "active",
                "strategyType": str(payload.get("strategyType", "")).strip(),
                "keyword": str(payload.get("keyword", "")).strip(),
                "targetAgentId": str(payload.get("targetAgentId", "")).strip(),
                "priorityLevel": str(payload.get("priorityLevel", "")).strip() or "normal",
                "queueName": str(payload.get("queueName", "")).strip(),
            },
        )
        handler._audit("orchestration_policy_save", detail=f"保存动态路由策略 {policy['name']}", meta={"policyId": policy["id"], "strategyType": policy["strategyType"]})
        handler._send_json({"ok": True, "message": f"动态路由策略 {policy['name']} 已保存。", "policy": policy, "dashboard": _action_dashboard(handler, svc, sections=["orchestration"])})
        return True

    return False
