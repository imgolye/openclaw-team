from __future__ import annotations

def _handle_agent_commands(handler, path, payload, svc):
    if path == "/api/actions/agents/pause":
        if not handler._require_capability("taskWrite", "当前账号没有调整 Agent 接单状态的权限。"):
            return True
        agent_id = str(payload.get("agentId", "")).strip()
        if not agent_id:
            raise RuntimeError("请提供 Agent 编号。")
        override = svc.set_agent_paused(handler.server.openclaw_dir, agent_id, True)
        handler._audit("agent_pause", detail=f"暂停 Agent {agent_id} 接单", meta={"agentId": agent_id})
        data, _paths = handler._refreshed_bundle()
        handler._send_json(
            {
                "ok": True,
                "message": f"Agent {agent_id} 已暂停接单。",
                "agentId": agent_id,
                "override": override,
                "dashboard": handler._action_dashboard_payload(data, sections=["agents"]),
            }
        )
        return True

    if path == "/api/actions/agents/resume":
        if not handler._require_capability("taskWrite", "当前账号没有调整 Agent 接单状态的权限。"):
            return True
        agent_id = str(payload.get("agentId", "")).strip()
        if not agent_id:
            raise RuntimeError("请提供 Agent 编号。")
        override = svc.set_agent_paused(handler.server.openclaw_dir, agent_id, False)
        handler._audit("agent_resume", detail=f"恢复 Agent {agent_id} 接单", meta={"agentId": agent_id})
        data, _paths = handler._refreshed_bundle()
        handler._send_json(
            {
                "ok": True,
                "message": f"Agent {agent_id} 已恢复接单。",
                "agentId": agent_id,
                "override": override,
                "dashboard": handler._action_dashboard_payload(data, sections=["agents"]),
            }
        )
        return True

    if path == "/api/actions/agents/transfer":
        if not handler._require_capability("taskWrite", "当前账号没有转移 Agent 任务的权限。"):
            return True
        source_agent_id = str(payload.get("sourceAgentId", "")).strip()
        target_agent_id = str(payload.get("targetAgentId", "")).strip()
        if not source_agent_id or not target_agent_id:
            raise RuntimeError("请提供源 Agent 和目标 Agent。")
        result = svc.perform_transfer_agent_tasks(
            handler.server.openclaw_dir,
            source_agent_id=source_agent_id,
            target_agent_id=target_agent_id,
            limit=int(payload.get("limit") or 0),
            reason=str(payload.get("reason", "")).strip(),
        )
        handler._audit(
            "agent_transfer_tasks",
            detail=f"转移 Agent {source_agent_id} 的任务到 {target_agent_id}",
            meta={"sourceAgentId": source_agent_id, "targetAgentId": target_agent_id, "count": result.get("count", 0)},
        )
        data, _paths = handler._refreshed_bundle()
        handler._send_json(
            {
                "ok": True,
                "message": (
                    f"已把 {result.get('count', 0)} 条任务转给 {result.get('targetAgentLabel', target_agent_id)}。"
                    if result.get("count", 0)
                    else "当前没有可转移的活跃任务。"
                ),
                "result": result,
                "dashboard": handler._action_dashboard_payload(data, sections=["taskIndex"]),
            }
        )
        return True

    if path == "/api/actions/agents/profile":
        if not handler._require_capability("taskWrite", "当前账号没有调整 Agent 角色与技能的权限。"):
            return True
        agent_id = str(payload.get("agentId", "")).strip()
        if not agent_id:
            raise RuntimeError("请提供 Agent 编号。")
        sample_payload = payload.get("voiceReplySample") if isinstance(payload.get("voiceReplySample"), dict) else {}
        clear_voice_reply_sample = bool(payload.get("clearVoiceReplySample"))
        sample_result = {}
        if clear_voice_reply_sample:
            svc.remove_agent_voice_reply_sample(handler.server.openclaw_dir, agent_id)
        elif sample_payload:
            sample_result = svc.save_agent_voice_reply_sample(
                handler.server.openclaw_dir,
                agent_id,
                filename=str(sample_payload.get("name") or "sample.wav").strip() or "sample.wav",
                content_b64=str(sample_payload.get("contentBase64") or "").strip(),
                prompt_text=str(payload.get("voiceReplySamplePromptText") or sample_payload.get("promptText") or "").strip(),
            )
        elif "voiceReplySamplePromptText" in payload:
            normalized_prompt = str(payload.get("voiceReplySamplePromptText") or "").strip()
            if normalized_prompt:
                try:
                    sample_result = svc.update_agent_voice_reply_sample_prompt_text(
                        handler.server.openclaw_dir,
                        agent_id,
                        normalized_prompt,
                    )
                except RuntimeError:
                    sample_result = {}
        override = svc.set_agent_profile(
            handler.server.openclaw_dir,
            agent_id=agent_id,
            role=str(payload.get("role", "")).strip(),
            skills=svc.safe_list(payload.get("skills")),
            human_name=(str(payload.get("humanName") or "").strip() if "humanName" in payload else None),
            job_title=(str(payload.get("jobTitle") or "").strip() if "jobTitle" in payload else None),
            working_style=(str(payload.get("workingStyle") or "").strip() if "workingStyle" in payload else None),
            department=(str(payload.get("department") or "").strip() if "department" in payload else None),
            capability_tags=(svc.safe_list(payload.get("capabilityTags")) if "capabilityTags" in payload else None),
            notes=(str(payload.get("notes") or "").strip() if "notes" in payload else None),
            voice_reply_voice=(str(payload.get("voiceReplyVoice") or "").strip() if "voiceReplyVoice" in payload else None),
            voice_reply_speed=(payload.get("voiceReplySpeed") if "voiceReplySpeed" in payload else None),
            voice_reply_instructions=(
                str(payload.get("voiceReplyInstructions") or "").strip() if "voiceReplyInstructions" in payload else None
            ),
            voice_reply_sample_path=(str(sample_result.get("samplePath") or "").strip() if sample_result else None),
            voice_reply_sample_name=(str(sample_result.get("sampleName") or "").strip() if sample_result else None),
            voice_reply_sample_prompt_text=(
                (
                    str(payload.get("voiceReplySamplePromptText") or sample_result.get("promptText") or "").strip()
                    if ("voiceReplySamplePromptText" in payload or sample_result)
                    else None
                )
            ),
            clear_voice_reply_sample=clear_voice_reply_sample,
        )
        if sample_result:
            override = svc.set_agent_profile(
                handler.server.openclaw_dir,
                agent_id=agent_id,
                role=override.get("role", ""),
                skills=svc.safe_list(override.get("skills")),
                human_name=override.get("humanName"),
                job_title=override.get("jobTitle"),
                working_style=override.get("workingStyle"),
                department=override.get("department"),
                capability_tags=svc.safe_list(override.get("capabilityTags")),
                notes=override.get("notes"),
                voice_reply_voice=svc.customer_voice_custom_voice_id(agent_id),
                voice_reply_speed=override.get("voiceReplySpeed"),
                voice_reply_instructions=override.get("voiceReplyInstructions"),
                voice_reply_sample_path=str(sample_result.get("samplePath") or "").strip(),
                voice_reply_sample_name=str(sample_result.get("sampleName") or "").strip(),
                voice_reply_sample_prompt_text=str(sample_result.get("promptText") or "").strip(),
            )
        if clear_voice_reply_sample and str(override.get("voiceReplyVoice") or "").strip() == svc.customer_voice_custom_voice_id(agent_id):
            override = svc.set_agent_profile(
                handler.server.openclaw_dir,
                agent_id=agent_id,
                role=override.get("role", ""),
                skills=svc.safe_list(override.get("skills")),
                human_name=override.get("humanName"),
                job_title=override.get("jobTitle"),
                working_style=override.get("workingStyle"),
                department=override.get("department"),
                capability_tags=svc.safe_list(override.get("capabilityTags")),
                notes=override.get("notes"),
                voice_reply_voice="",
                voice_reply_speed=override.get("voiceReplySpeed"),
                voice_reply_instructions=override.get("voiceReplyInstructions"),
                clear_voice_reply_sample=True,
            )
        handler._audit(
            "agent_profile_update",
            detail=f"更新 Agent {agent_id} 的角色与技能",
            meta={
                "agentId": agent_id,
                "role": override.get("role", ""),
                "jobTitle": override.get("jobTitle", ""),
                "skills": override.get("skills", []),
                "voiceSampleConfigured": bool(str(override.get("voiceReplySamplePath") or "").strip()),
            },
        )
        data, _paths = handler._refreshed_bundle()
        handler._send_json(
            {
                "ok": True,
                "message": f"Agent {agent_id} 的角色与技能已更新。",
                "agentId": agent_id,
                "override": override,
                "dashboard": handler._action_dashboard_payload(data, sections=["agents", "themes"]),
            }
        )
        return True

    if path == "/api/actions/agents/team/save":
        if not handler._require_capability("taskWrite", "当前账号没有维护 Agent Team 的权限。"):
            return True
        existing_team = svc.resolve_agent_team_record(handler.server.openclaw_dir, str(payload.get("id", "")).strip())
        requested_runtime_mode = svc.normalize_team_runtime_mode(str(payload.get("runtimeMode", "")).strip())
        requested_runtime_every = (
            svc.normalize_team_runtime_every(str(payload.get("runtimeEvery", "")).strip())
            if requested_runtime_mode
            else ""
        )
        coordination_protocol = {
            "humanToneGuide": str(payload.get("humanToneGuide", "")).strip(),
            "proactiveRules": svc.clean_unique_strings(payload.get("proactiveRules") if isinstance(payload.get("proactiveRules"), list) else []),
            "updateContract": str(payload.get("updateContract", "")).strip(),
            "escalationRule": str(payload.get("escalationRule", "")).strip(),
        }
        team = svc.save_agent_team_preserving_meta(
            handler.server.openclaw_dir,
            {
                "id": str(payload.get("id", "")).strip(),
                "name": str(payload.get("name", "")).strip(),
                "status": str(payload.get("status", "")).strip() or "active",
                "leadAgentId": str(payload.get("leadAgentId", "")).strip(),
                "memberAgentIds": payload.get("memberAgentIds") if isinstance(payload.get("memberAgentIds"), list) else [],
                "description": str(payload.get("description", "")).strip(),
                "focus": str(payload.get("focus", "")).strip(),
                "channel": str(payload.get("channel", "")).strip() or "internal",
                "defaultDispatchMode": str(payload.get("defaultDispatchMode", "")).strip(),
                "defaultWakeScope": str(payload.get("defaultWakeScope", "")).strip(),
                "operatingBrief": str(payload.get("operatingBrief", "")).strip(),
                "teamMemory": str(payload.get("teamMemory", "")).strip(),
                "decisionLog": str(payload.get("decisionLog", "")).strip(),
                "linkedTaskIds": payload.get("linkedTaskIds") if isinstance(payload.get("linkedTaskIds"), list) else [],
                "meta": {
                    "coordinationProtocol": coordination_protocol,
                    "humanToneGuide": coordination_protocol.get("humanToneGuide", ""),
                    "proactiveRules": coordination_protocol.get("proactiveRules", []),
                    "updateContract": coordination_protocol.get("updateContract", ""),
                    "escalationRule": coordination_protocol.get("escalationRule", ""),
                    "runtimeMode": requested_runtime_mode,
                    "runtimeEvery": requested_runtime_every,
                },
            },
            existing=existing_team,
        )
        runtime_result = None
        if requested_runtime_mode in {"quiet", "lead_standby", "all_standby"}:
            runtime_result = svc.apply_agent_team_runtime_policy(
                handler.server.openclaw_dir,
                team_id=team.get("id", ""),
                runtime_mode=requested_runtime_mode,
                runtime_every=requested_runtime_every,
                restart_gateway=bool(getattr(handler.server, "allow_gateway_restart", False)),
            )
        handler._audit(
            "agent_team_save",
            detail=f"保存 Agent Team {team.get('name', team.get('id', ''))}",
            meta={
                "teamId": team.get("id", ""),
                "leadAgentId": team.get("leadAgentId", ""),
                "runtimeMode": requested_runtime_mode,
                "runtimeEvery": requested_runtime_every,
            },
        )
        data, _paths = handler._refreshed_bundle()
        saved_team = next(
            (item for item in svc.safe_list((data.get("agentTeams", {}) or {}).get("items")) if item.get("id") == team.get("id")),
            runtime_result.get("team") if isinstance(runtime_result, dict) else team,
        )
        handler._send_json(
            {
                "ok": True,
                "message": f"Agent Team {saved_team.get('name', '')} 已保存。",
                "team": saved_team,
                "runtime": runtime_result,
                "dashboard": handler._action_dashboard_payload(data, sections=["agentTeams"]),
            }
        )
        return True

    if path == "/api/actions/agents/team/runtime":
        if not handler._require_capability("taskWrite", "当前账号没有调整 Team 值守模式的权限。"):
            return True
        team_id = str(payload.get("teamId", "")).strip()
        runtime_mode = str(payload.get("runtimeMode", "")).strip()
        if not team_id or not runtime_mode:
            raise RuntimeError("需要 teamId 和 runtimeMode。")
        result = svc.apply_agent_team_runtime_policy(
            handler.server.openclaw_dir,
            team_id=team_id,
            runtime_mode=runtime_mode,
            runtime_every=str(payload.get("runtimeEvery", "")).strip(),
            restart_gateway=bool(getattr(handler.server, "allow_gateway_restart", False)),
        )
        handler._audit(
            "agent_team_runtime",
            detail=f"更新 Team {result['team'].get('name', team_id)} 的值守模式",
            meta={
                "teamId": team_id,
                "runtimeMode": result.get("runtimeMode", ""),
                "runtimeEvery": result.get("runtimeEvery", ""),
                "appliedAgents": result.get("appliedAgents", []),
                "clearedAgents": result.get("clearedAgents", []),
            },
        )
        data, _paths = handler._refreshed_bundle()
        saved_team = next(
            (item for item in svc.safe_list((data.get("agentTeams", {}) or {}).get("items")) if item.get("id") == team_id),
            result.get("team"),
        )
        handler._send_json(
            {
                "ok": True,
                "message": f"Team {saved_team.get('name', team_id)} 已切到 {result.get('runtimeMode', runtime_mode)}。",
                "team": saved_team,
                "result": result,
                "dashboard": handler._action_dashboard_payload(data, sections=["agentTeams"]),
            }
        )
        return True

    if path == "/api/actions/agents/team/wake":
        if not handler._require_capability("taskWrite", "当前账号没有发送 Team 值守信号的权限。"):
            return True
        team_id = str(payload.get("teamId", "")).strip()
        if not team_id:
            raise RuntimeError("需要 teamId。")
        result = svc.perform_agent_team_wake(
            handler.server.openclaw_dir,
            team_id=team_id,
            scope=str(payload.get("scope", "")).strip(),
            message_text=str(payload.get("message", "")).strip(),
        )
        handler._audit(
            "agent_team_wake",
            detail=f"发送 Team {result['team'].get('name', team_id)} 的值守信号",
            meta={
                "teamId": team_id,
                "scope": result.get("scope", ""),
                "status": result.get("status", ""),
                "targetAgentIds": result.get("targetAgentIds", []),
                "replyCount": len(result.get("responses", [])),
                "failureCount": len(result.get("failures", [])),
            },
        )
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        team_records = svc.store_list_agent_teams(handler.server.openclaw_dir)
        raw_team = next((item for item in team_records if item.get("id") == team_id), result.get("team"))
        wake_fields = svc.team_runtime_wake_fields(raw_team, svc.now_utc())
        member_count = len([item for item in svc.safe_list(raw_team.get("memberAgentIds")) if str(item or "").strip()])
        runtime_mode = svc.requested_team_runtime_mode(raw_team)
        runtime_state = (
            "active"
            if wake_fields.get("wakeReplyCount", 0) > 0
            else ("standby" if runtime_mode in {"lead_standby", "all_standby"} else ("idle" if member_count else "quiet"))
        )
        saved_team = {
            **raw_team,
            "memberCount": member_count,
            "runtimeMode": runtime_mode,
            "runtimeState": runtime_state,
            **wake_fields,
        }
        dashboard = {
            "agentTeams": {
                "summary": {
                    "teamCount": len(team_records),
                    "wakeReadyCount": sum(
                        1
                        for item in team_records
                        if any(str(member or "").strip() for member in svc.safe_list(item.get("memberAgentIds")))
                    ),
                },
                "items": [saved_team],
            }
        }
        handler._send_json(
            {
                "ok": True,
                "message": f"Team {saved_team.get('name', team_id)} 值守信号已发送。",
                "team": saved_team,
                "result": result,
                "dashboard": dashboard,
            }
        )
        return True

    return False
