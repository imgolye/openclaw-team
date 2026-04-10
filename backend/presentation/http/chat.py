from __future__ import annotations

from copy import deepcopy
import secrets
import threading
import time


def _conversation_reply_entries_from_items(items):
    entries = []
    for index, item in enumerate(items or []):
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "").strip() != "assistant":
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        signature = (
            str(item.get("id") or "").strip()
            or str(item.get("timestamp") or "").strip()
            or f"{index}:{text[:160]}"
        )
        entries.append({"signature": signature, "text": text, "item": item})
    return entries


def _spawn_async_conversation_send(handler, svc, *, agent_id, session_id, conversation_key, stream_id, message, thinking, selected_skill_slugs, raw_message):
    openclaw_dir = handler.server.openclaw_dir
    normalized_agent_id = str(agent_id or "").strip()
    normalized_session_id = str(session_id or "").strip() or "main"
    normalized_conversation_key = str(conversation_key or "").strip()

    def notify_stream(event_payload):
        stream_stage = str((event_payload or {}).get("stage") or "").strip()
        stream_agent_id = str((event_payload or {}).get("agentId") or normalized_agent_id).strip()
        stream_session_id = str((event_payload or {}).get("sessionId") or normalized_session_id).strip() or normalized_session_id
        stream_conversation_key = str((event_payload or {}).get("conversationKey") or normalized_conversation_key).strip() or normalized_conversation_key
        if not stream_stage or not stream_agent_id:
            return
        try:
            svc.publish_conversation_stream_event(
                handler.server,
                stream_agent_id,
                stream_session_id,
                stream_stage,
                conversation_key=stream_conversation_key,
                streamId=stream_id,
                content=str((event_payload or {}).get("content") or ""),
                delta=str((event_payload or {}).get("delta") or ""),
            )
        except Exception:
            return

    def worker():
        try:
            result = svc.perform_conversation_send(
                openclaw_dir,
                agent_id=normalized_agent_id,
                session_id=normalized_session_id,
                conversation_key=normalized_conversation_key,
                message=message,
                thinking=thinking,
                stream_observer=notify_stream,
                selected_skill_slugs=selected_skill_slugs,
            )
        except Exception:
            svc.publish_conversation_stream_event(
                handler.server,
                normalized_agent_id,
                normalized_session_id,
                "failed",
                conversation_key=normalized_conversation_key,
                streamId=stream_id,
                content="",
                delta="",
            )
            return
        meta = ((result.get("result", {}) or {}).get("meta", {}) or {}).get("agentMeta", {}) or {}
        actual_agent_id = meta.get("agentId") or normalized_agent_id
        actual_session_id = meta.get("sessionId") or normalized_session_id
        actual_conversation_key = meta.get("conversationKey") or normalized_conversation_key
        payloads = (result.get("result", {}) or {}).get("payloads", []) or []
        reply_preview = payloads[0].get("text", "") if payloads and isinstance(payloads[0], dict) else ""
        try:
            svc.publish_conversation_stream_event(
                handler.server,
                actual_agent_id,
                actual_session_id,
                "completed",
                conversation_key=actual_conversation_key,
                streamId=stream_id,
                content=reply_preview,
                delta="",
            )
        except Exception:
            pass
        try:
            conversation = svc.load_conversation_transcript(
                openclaw_dir,
                actual_agent_id,
                actual_session_id,
                actual_conversation_key,
            )
            svc.ensure_conversation_voice_reply_attachment(
                openclaw_dir,
                actual_agent_id,
                actual_session_id,
                actual_conversation_key,
                request_text=raw_message,
                conversation=conversation,
            )
        except Exception:
            pass

    threading.Thread(
        target=worker,
        daemon=True,
        name=f"conv-send-{normalized_agent_id[:12] or 'agent'}",
    ).start()

def _handle_chat_commands(handler, path, payload, svc):
    if path == "/api/actions/chat/thread/save":
        if not handler._require_capability("conversationWrite", "当前账号没有维护聊天线程的权限。"):
            return True
        actor = handler._current_actor()
        thread_id = str(payload.get("id", "")).strip()
        existing_thread = (svc.store_get_chat_thread(handler.server.openclaw_dir, thread_id) or {}) if thread_id else {}
        existing_meta = deepcopy(existing_thread.get("meta", {})) if isinstance(existing_thread.get("meta"), dict) else {}
        team_id = str(payload.get("teamId", "")).strip() or str(existing_meta.get("teamId") or "").strip()
        linked_team_ids = svc.normalize_chat_thread_linked_team_ids(
            team_id,
            payload.get("linkedTeamIds") if isinstance(payload.get("linkedTeamIds"), list) else existing_meta.get("linkedTeamIds"),
            payload.get("collaboratorTeamIds") if isinstance(payload.get("collaboratorTeamIds"), list) else [],
        )
        team_id = team_id or (linked_team_ids[0] if linked_team_ids else "")
        team_records = svc.resolve_chat_thread_team_records(handler.server.openclaw_dir, team_id, linked_team_ids)
        team = team_records[0] if team_records else None
        participant_agent_ids = svc.resolve_chat_thread_participant_agent_ids(
            payload.get("participantAgentIds") if isinstance(payload.get("participantAgentIds"), list) else svc.safe_list(existing_thread.get("participantAgentIds")),
            team_records,
        )
        primary_agent_id = (
            str(payload.get("primaryAgentId", "")).strip()
            or str(existing_thread.get("primaryAgentId") or "").strip()
            or str((team or {}).get("leadAgentId") or "").strip()
        )
        current_target_agent_id = (
            str(payload.get("currentTargetAgentId", "")).strip()
            or str(existing_thread.get("currentTargetAgentId") or "").strip()
            or primary_agent_id
        )
        payload_meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        thread_meta = {**existing_meta, **payload_meta}
        workspace_path = str(payload.get("workspacePath") or thread_meta.get("workspacePath") or "").strip()
        workspace_authorized = bool(payload.get("workspaceAuthorized") or thread_meta.get("workspaceAuthorized"))
        if workspace_path:
            thread_meta = {**thread_meta, "workspacePath": workspace_path}
        else:
            thread_meta.pop("workspacePath", None)
        if workspace_authorized and workspace_path:
            thread_meta = {**thread_meta, "workspaceAuthorized": True}
        else:
            thread_meta.pop("workspaceAuthorized", None)
        if team_id:
            thread_meta = {**thread_meta, "teamId": team_id}
        if linked_team_ids:
            thread_meta = {**thread_meta, "linkedTeamIds": linked_team_ids}
        team_default_dispatch_mode = svc.resolve_team_default_dispatch_mode(team) if team else ""
        dispatch_mode_explicit = bool(payload.get("dispatchModeExplicit"))
        thread_meta = {
            **thread_meta,
            "dispatchMode": svc.normalize_chat_dispatch_mode(
                payload.get("dispatchMode") or thread_meta.get("dispatchMode") or team_default_dispatch_mode,
                has_team=bool(team_id),
            ),
            "dispatchModeExplicit": dispatch_mode_explicit if payload.get("dispatchMode") is not None else bool(thread_meta.get("dispatchModeExplicit")),
        }
        if team:
            existing_policy = thread_meta.get("teamPolicy") if isinstance(thread_meta.get("teamPolicy"), dict) else {}
            thread_meta = {**thread_meta, "teamPolicy": svc.merge_team_policy_state(team, existing_policy)}
        pack_binding = payload.get("packBinding") if isinstance(payload.get("packBinding"), dict) else {}
        linked_pack_id = str(payload.get("linkedPackId") or payload.get("packId") or "").strip()
        thread_mode_value = str(payload.get("mode") or thread_meta.get("mode") or existing_thread.get("mode") or "").strip()
        if linked_pack_id or thread_mode_value:
            pack = svc.resolve_workflow_pack_or_mode_record(handler.server.openclaw_dir, linked_pack_id, mode=thread_mode_value, target="chat")
            pack_binding = svc.workflow_pack_binding_payload(pack, source=str(payload.get("source") or "chat").strip() or "chat", target="chat")
            pack_capabilities = svc.workflow_pack_capabilities(pack)
            thread_meta = {
                **thread_meta,
                "reviewGates": pack_capabilities.get("reviewGates", []),
                "artifactTemplates": pack_capabilities.get("artifactTemplates", []),
                "runtimePolicy": pack_capabilities.get("runtimePolicy", {}),
            }
        elif not pack_binding:
            pack_binding = thread_meta.get("packBinding") if isinstance(thread_meta.get("packBinding"), dict) else {}
        if pack_binding:
            thread_meta = {**thread_meta, "packBinding": pack_binding}
        thread_mode = str(
            payload.get("mode")
            or (pack_binding.get("mode") if isinstance(pack_binding, dict) else "")
            or thread_meta.get("mode")
            or existing_thread.get("mode")
            or ""
        ).strip()
        if thread_mode:
            thread_meta = {**thread_meta, "mode": thread_mode}
        participant_humans = payload.get("participantHumans") if isinstance(payload.get("participantHumans"), list) else existing_thread.get("participantHumans")
        if not isinstance(participant_humans, list):
            participant_humans = [{"name": actor.get("displayName", ""), "username": actor.get("username", ""), "role": actor.get("role", "")}]
        thread = svc.store_save_chat_thread(
            handler.server.openclaw_dir,
            {
                "id": thread_id,
                "title": str(payload.get("title", "")).strip() or str(existing_thread.get("title") or "").strip(),
                "status": str(payload.get("status", "")).strip() or str(existing_thread.get("status") or "").strip() or "open",
                "channel": str(payload.get("channel", "")).strip() or str(existing_thread.get("channel") or "").strip() or "internal",
                "owner": str(payload.get("owner", "")).strip() or str(existing_thread.get("owner") or "").strip() or actor.get("displayName", ""),
                "primaryAgentId": primary_agent_id,
                "currentTargetAgentId": current_target_agent_id,
                "linkedTaskId": str(payload.get("linkedTaskId", "")).strip() or str(existing_thread.get("linkedTaskId") or "").strip(),
                "linkedDeliverableId": str(payload.get("linkedDeliverableId", "")).strip() or str(existing_thread.get("linkedDeliverableId") or "").strip(),
                "linkedRunId": str(payload.get("linkedRunId", "")).strip() or str(existing_thread.get("linkedRunId") or "").strip(),
                "participantAgentIds": participant_agent_ids,
                "participantHumans": participant_humans,
                "workspacePath": workspace_path,
                "workspaceAuthorized": workspace_authorized and bool(workspace_path),
                "meta": thread_meta,
            },
        )
        handler._audit("chat_thread_save", detail=f"保存聊天线程 {thread.get('title', thread.get('id', ''))}", meta={"threadId": thread.get("id", ""), "primaryAgentId": thread.get("primaryAgentId", "")})
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        detail = svc.build_chat_thread_quick_snapshot(handler.server.openclaw_dir, thread)
        handler._send_json({"ok": True, "message": f"聊天线程 {thread.get('title', '')} 已保存。", "thread": detail})
        return True

    if path == "/api/actions/chat/thread/delete":
        if not handler._require_capability("conversationWrite", "当前账号没有删除聊天线程的权限。"):
            return True
        thread_id = str(payload.get("threadId", "")).strip()
        if not thread_id:
            raise RuntimeError("需要 threadId。")
        thread = svc.store_get_chat_thread(handler.server.openclaw_dir, thread_id)
        if not thread:
            raise RuntimeError("聊天线程不存在。")
        deleted = svc.store_delete_chat_thread(handler.server.openclaw_dir, thread_id)
        handler._audit(
            "chat_thread_delete",
            detail=f"删除聊天线程 {thread.get('title', thread_id)}",
            meta={"threadId": thread_id, "messageDeleted": int(deleted.get("messageDeleted") or 0), "threadDeleted": int(deleted.get("threadDeleted") or 0)},
        )
        svc.invalidate_dashboard_bundle_cache(handler.server.openclaw_dir, handler.server.output_dir)
        remaining_threads = svc.store_list_chat_threads(handler.server.openclaw_dir, limit=1)
        next_thread_id = str((remaining_threads[0] or {}).get("id") or "").strip() if remaining_threads else ""
        next_thread = svc.load_chat_thread_detail(handler.server.openclaw_dir, next_thread_id) if next_thread_id else None
        handler._send_json(
            {
                "ok": True,
                "message": f"已删除聊天记录《{thread.get('title', thread_id)}》。",
                "deletedThreadId": thread_id,
                "nextThreadId": next_thread_id,
                "nextThread": next_thread,
                "result": deleted,
            }
        )
        return True

    if path == "/api/actions/chat/thread/send":
        if not handler._require_capability("conversationWrite", "当前账号没有发送聊天消息的权限。"):
            return True
        thread_id = str(payload.get("threadId", "")).strip()
        message_text = str(payload.get("message", "")).strip()
        target_agent_id = str(payload.get("targetAgentId", "")).strip()
        requested_dispatch_mode = str(payload.get("dispatchMode", "")).strip()
        dispatch_mode_explicit = bool(payload.get("dispatchModeExplicit"))
        prepared = svc.prepare_chat_send_request(
            message_text,
            attachments=payload.get("attachments"),
            selected_skill_slugs=payload.get("skillSlugs") if isinstance(payload.get("skillSlugs"), list) else [],
            mention_agent_ids=payload.get("mentionAgentIds"),
            reply_context=payload.get("replyContext"),
            thinking=payload.get("thinking", ""),
        )
        if not thread_id or (not prepared["messageText"] and not prepared["attachments"]):
            raise RuntimeError("需要 threadId，且至少要发送文字或附件。")
        result = svc.perform_chat_thread_send(
            handler.server.openclaw_dir,
            thread_id=thread_id,
            message_text=prepared["messageText"],
            actor=handler._current_actor(),
            target_agent_id=target_agent_id,
            workspace_path=str(payload.get("workspacePath") or "").strip(),
            workspace_authorized=bool(payload.get("workspaceAuthorized")),
            attachments=prepared["attachments"],
            mention_agent_ids=prepared["mentionAgentIds"],
            reply_context=prepared["replyContext"],
            selected_skill_slugs=prepared["selectedSkillSlugs"],
            dispatch_mode=requested_dispatch_mode,
            dispatch_mode_explicit=dispatch_mode_explicit,
            thinking=prepared["thinking"],
            output_dir=handler.server.output_dir,
            server=handler.server,
        )
        handler._audit(
            "chat_thread_send",
            detail=f"向线程 {((result.get('threadRecord') or {}).get('title') or thread_id)} 分发 Team 消息",
            meta={
                "threadId": thread_id,
                "targetAgentId": target_agent_id,
                "dispatchMode": str(((result.get("dispatch") or {}).get("dispatchMode")) or requested_dispatch_mode or "").strip(),
                "dispatchAgentIds": (result.get("dispatch") or {}).get("dispatchAgentIds") or [],
                "replyCount": int(((result.get("dispatch") or {}).get("replyCount")) or 0),
            },
        )
        handler._send_json(
            {
                "ok": True,
                "message": result.get("message") or "消息已发送。",
                "dispatch": result.get("dispatch") or {},
                "thread": result.get("thread") or {},
            }
        )
        return True

    if path == "/api/actions/chat/tool/approve":
        if not handler._require_capability("conversationWrite", "当前账号没有审批工具执行的权限。"):
            return True
        request_id = str(payload.get("requestId", "")).strip()
        approved = bool(payload.get("approved", False))
        if not request_id:
            raise RuntimeError("requestId is required")

        from backend.application.services.tool_enabled_dispatch import get_approval_registry
        import asyncio

        registry = get_approval_registry()
        try:
            loop = asyncio.new_event_loop()
            resolved = loop.run_until_complete(registry.resolve(request_id, approved))
            loop.close()
        except Exception:
            resolved = False

        handler._send_json({
            "ok": True,
            "resolved": resolved,
            "requestId": request_id,
            "approved": approved,
            "message": "工具执行已批准。" if approved else "工具执行已拒绝。",
        })
        return True

    if path == "/api/actions/conversations/send":
        if not handler._require_capability("conversationWrite", "当前账号没有发起或继续对话的权限。"):
            return True
        agent_id = str(payload.get("agentId", "")).strip()
        session_id = str(payload.get("sessionId", "")).strip()
        conversation_key = str(payload.get("conversationKey", "")).strip()
        requested_conversation_key = svc.normalize_product_conversation_session_key(
            agent_id,
            session_id or "main",
            conversation_key,
        )
        requested_session_id = svc.conversation_display_session_id(agent_id, requested_conversation_key)
        raw_message = str(payload.get("message", "")).strip()
        prepared = svc.prepare_chat_send_request(
            raw_message,
            attachments=payload.get("attachments"),
            selected_skill_slugs=payload.get("skillSlugs") if isinstance(payload.get("skillSlugs"), list) else [],
            thinking=payload.get("thinking", ""),
        )
        attachments = prepared["attachments"]
        selected_skill_slugs = prepared["selectedSkillSlugs"]
        message = prepared["mergedMessage"]
        thinking = prepared["thinking"]
        stream_id = secrets.token_hex(8)

        try:
            handler._audit(
                "conversation_send",
                detail=f"向 {agent_id} 发起对话",
                meta={"agentId": agent_id, "sessionId": requested_session_id, "conversationKey": requested_conversation_key, "attachmentCount": len(attachments)},
            )
        except Exception:
            pass
        try:
            svc.publish_conversation_stream_event(
                handler.server,
                agent_id,
                requested_session_id,
                "started",
                conversation_key=requested_conversation_key,
                streamId=stream_id,
                content="",
                delta="",
            )
        except Exception:
            pass
        _spawn_async_conversation_send(
            handler,
            svc,
            agent_id=agent_id,
            session_id=requested_session_id,
            conversation_key=requested_conversation_key,
            stream_id=stream_id,
            message=message,
            thinking=thinking,
            selected_skill_slugs=selected_skill_slugs,
            raw_message=raw_message,
        )
        conversation = svc.load_conversation_transcript(
            handler.server.openclaw_dir,
            agent_id,
            requested_session_id,
            requested_conversation_key,
        )
        handler._send_json(
            {
                "ok": True,
                "message": f"已向 {agent_id} 成功发送消息。",
                "conversation": conversation,
                "session": {"agentId": agent_id, "sessionId": requested_session_id, "conversationKey": requested_conversation_key, "key": requested_conversation_key},
            }
        )
        return True

    if path == "/api/actions/conversations/session/open":
        if not handler._require_capability("conversationWrite", "当前账号没有发起或继续对话的权限。"):
            return True
        agent_id = str(payload.get("agentId", "")).strip()
        mode = str(payload.get("mode", "direct") or "direct").strip() or "direct"
        conversation_key = str(payload.get("conversationKey", "")).strip()
        session = svc.open_conversation_session(
            handler.server.openclaw_dir,
            agent_id,
            mode=mode,
            conversation_key=conversation_key,
        )
        handler._send_json({"ok": True, "session": session})
        return True

    return False
