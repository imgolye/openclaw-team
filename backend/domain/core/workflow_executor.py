"""Real workflow node execution engine.

Replaces mock/simulation with actual LLM calls, HTTP requests,
code execution, and condition evaluation. Each node type has a
dedicated executor that leverages existing infrastructure:

- GatewayLLMProvider  → LLM / Agent / RAG nodes
- HarnessToolExecutor → Tool / Code nodes (bash, file ops)
- httpx               → HTTP Request nodes
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Template engine ───────────────────────────────────────────────────


def resolve_template(value: Any, context: dict) -> Any:
    """Recursively resolve ``{{path.to.key}}`` references in strings, dicts, lists.

    Supported paths:
        {{input.docUrl}}           → context["input"]["docUrl"]
        {{latest.summary}}         → context["latest"]["summary"]
        {{variables.publishTitle}} → context["variables"]["publishTitle"]
        {{nodes.node_id.output.x}} → context["nodes"]["node_id"]["output"]["x"]
    """
    if isinstance(value, str):
        def _replacer(m: re.Match) -> str:
            path = m.group(1).strip().split(".")
            obj: Any = context
            for key in path:
                if isinstance(obj, dict):
                    obj = obj.get(key, {})
                else:
                    return m.group(0)  # unresolvable — leave as-is
            if isinstance(obj, (dict, list)):
                return json.dumps(obj, ensure_ascii=False)
            return str(obj) if obj not in ({}, [], None) else m.group(0)
        return re.sub(r"\{\{(.+?)\}\}", _replacer, value)
    if isinstance(value, dict):
        return {k: resolve_template(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_template(v, context) for v in value]
    return value


def _safe_list(v: Any) -> list:
    return v if isinstance(v, list) else []


def _s(v: Any, default: str = "") -> str:
    return str(v).strip() if v else default


# ── Node executors ────────────────────────────────────────────────────


async def execute_start_node(node: dict, context: dict, config: dict) -> dict:
    """Parse user inputs from start / trigger nodes."""
    inputs = context.get("input", {})
    # Merge example values from config as defaults
    for inp in _safe_list(config.get("inputs")):
        if isinstance(inp, dict):
            name = _s(inp.get("name"))
            if name and name not in inputs:
                inputs[name] = inp.get("example", "")
    context["input"] = inputs
    return {
        "entryType": _s(node.get("type"), "start"),
        "inputs": inputs,
    }


async def execute_llm_node(node: dict, context: dict, config: dict) -> dict:
    """Call an LLM with the configured prompt."""
    from backend.adapters.llm.gateway_provider import GatewayLLMProvider

    prompt = resolve_template(_s(config.get("prompt"), "Summarize the context."), context)
    model = _s(config.get("model"), "claude-sonnet-4-20250514")
    temperature = float(config.get("temperature", 0.3))
    max_tokens = int(config.get("max_tokens") or config.get("maxTokens") or 4096)
    openclaw_dir = _s(context.get("openclaw_dir"))

    provider = GatewayLLMProvider(openclaw_dir=openclaw_dir)
    messages = [{"role": "user", "content": prompt}]
    system = _s(config.get("system"), "")

    # Inject latest context as system info if no explicit system prompt
    if not system and context.get("latest"):
        system = f"Previous step output:\n{json.dumps(context['latest'], ensure_ascii=False, indent=2)[:3000]}"

    result = await provider.complete(
        messages, system=system, model=model,
        max_tokens=max_tokens, temperature=temperature,
    )
    return {
        "text": result.text,
        "model": model,
        "tokens": {"input": result.input_tokens, "output": result.output_tokens},
    }


async def execute_agent_node(node: dict, context: dict, config: dict) -> dict:
    """Run an agent loop: LLM + tools until the agent finishes."""
    from backend.adapters.llm.gateway_provider import GatewayLLMProvider, get_tool_schemas
    from backend.adapters.tools.harness_executor import HarnessToolExecutor

    instructions = resolve_template(
        _s(config.get("instructions"), "Complete the task based on the provided context."), context
    )
    model = _s(config.get("model"), "claude-sonnet-4-20250514")
    max_turns = int(config.get("maxTurns") or config.get("max_turns") or 5)
    openclaw_dir = _s(context.get("openclaw_dir"))
    workspace = _s(context.get("workspacePath"), os.getcwd())

    provider = GatewayLLMProvider(openclaw_dir=openclaw_dir)
    executor = HarnessToolExecutor(workspace_path=workspace)
    tools = get_tool_schemas()

    system = instructions
    if context.get("latest"):
        system += f"\n\nContext from previous step:\n{json.dumps(context['latest'], ensure_ascii=False, indent=2)[:3000]}"

    messages = [{"role": "user", "content": "Execute the task described in the system instructions."}]
    collected_text = []

    for turn in range(max_turns):
        result = await provider.complete(
            messages, system=system, model=model,
            max_tokens=4096, temperature=0.2, tools=tools,
        )
        if result.text:
            collected_text.append(result.text)

        if not result.tool_calls:
            break  # agent finished

        # Execute tool calls
        for tc in result.tool_calls:
            tool_result = await executor.execute(tc["name"], tc.get("input", {}))
            messages.append({"role": "assistant", "content": result.text or "", "tool_calls": [tc]})
            messages.append({
                "role": "tool",
                "tool_use_id": tc.get("id", ""),
                "content": json.dumps(tool_result, ensure_ascii=False)[:8000],
            })

    return {
        "text": "\n".join(collected_text),
        "turns": min(turn + 1, max_turns),
        "model": model,
    }


async def execute_code_node(node: dict, context: dict, config: dict) -> dict:
    """Execute Python or JS code via subprocess."""
    language = _s(config.get("language"), "python").lower()
    code = _s(config.get("code"), "")
    timeout = min(int(config.get("timeout") or 120), 120)

    # If no explicit code, check for inline script in description
    if not code:
        code = _s(node.get("codeContent"), "")
    if not code:
        # Fallback: pass through latest data
        return {
            "result": context.get("latest", {}),
            "outputs": _safe_list(config.get("outputs")),
            "note": "No code provided, passing through latest context.",
        }

    # Write code to temp file and execute
    suffix = ".py" if language == "python" else ".js"
    cmd_prefix = "python3" if language == "python" else "node"

    input_json = json.dumps(context.get("latest", {}), ensure_ascii=False)

    with tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False) as f:
        f.write(code)
        f.flush()
        temp_path = f.name

    try:
        env = {**os.environ, "INPUT_JSON": input_json}
        proc = await asyncio.create_subprocess_exec(
            cmd_prefix, temp_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        stdout_str = stdout.decode("utf-8", errors="replace").strip()
        stderr_str = stderr.decode("utf-8", errors="replace").strip()

        if proc.returncode != 0:
            raise RuntimeError(f"Code exited with {proc.returncode}: {stderr_str[:1000]}")

        # Try parsing stdout as JSON
        try:
            result = json.loads(stdout_str)
        except (json.JSONDecodeError, ValueError):
            result = stdout_str

        return {
            "result": result,
            "stdout": stdout_str[:5000],
            "stderr": stderr_str[:1000] if stderr_str else None,
            "exitCode": proc.returncode,
        }
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


async def execute_http_node(node: dict, context: dict, config: dict) -> dict:
    """Make a real HTTP request."""
    import httpx

    method = _s(config.get("method"), "GET").upper()
    url = resolve_template(_s(config.get("url"), ""), context)
    headers = resolve_template(config.get("headers") or {}, context)
    body = resolve_template(config.get("body"), context)
    timeout = float(config.get("timeout") or 30)

    if not url:
        raise ValueError("HTTP node requires a URL")

    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        kwargs: dict[str, Any] = {"headers": headers}
        if body and method in ("POST", "PUT", "PATCH"):
            if isinstance(body, (dict, list)):
                kwargs["json"] = body
            else:
                kwargs["content"] = str(body)

        response = await client.request(method, url, **kwargs)

        # Try JSON response
        try:
            response_body = response.json()
        except Exception:
            response_body = response.text[:5000]

        return {
            "statusCode": response.status_code,
            "headers": dict(response.headers),
            "body": response_body,
            "url": url,
            "method": method,
        }


async def execute_tool_node(node: dict, context: dict, config: dict) -> dict:
    """Execute a tool via HarnessToolExecutor."""
    from backend.adapters.tools.harness_executor import HarnessToolExecutor

    tool_id = _s(config.get("toolId"), "")
    workspace = _s(context.get("workspacePath"), os.getcwd())
    executor = HarnessToolExecutor(workspace_path=workspace)

    # Map toolId to executor tool name
    # Format: "category.action" → try action first, then full toolId
    tool_name = tool_id.split(".")[-1] if "." in tool_id else tool_id

    # Build tool args from config (exclude meta keys)
    meta_keys = {"toolId", "onError", "retries", "timeout"}
    tool_args = resolve_template(
        {k: v for k, v in config.items() if k not in meta_keys},
        context,
    )

    result = await executor.execute(tool_name, tool_args)
    return {
        "toolId": tool_id,
        "toolName": tool_name,
        **result,
    }


async def execute_condition_node(node: dict, context: dict, config: dict) -> dict:
    """Evaluate conditions and return the matching branch."""
    rules = _safe_list(config.get("rules"))
    latest = context.get("latest", {})
    variables = context.get("variables", {})

    def _eval_condition(condition: str) -> bool:
        """Simple condition evaluator: 'field operator value'."""
        condition = condition.strip()
        if not condition:
            return True

        # Resolve template variables first
        condition = resolve_template(condition, context)

        # Simple patterns
        for op, fn in [
            ("===", lambda a, b: str(a).strip() == str(b).strip()),
            ("!==", lambda a, b: str(a).strip() != str(b).strip()),
            ("==", lambda a, b: str(a).strip() == str(b).strip()),
            ("!=", lambda a, b: str(a).strip() != str(b).strip()),
            (">=", lambda a, b: float(a) >= float(b)),
            ("<=", lambda a, b: float(a) <= float(b)),
            (">", lambda a, b: float(a) > float(b)),
            ("<", lambda a, b: float(a) < float(b)),
            (" contains ", lambda a, b: str(b).strip() in str(a)),
            (" not_empty", lambda a, _: bool(str(a).strip())),
            (" empty", lambda a, _: not bool(str(a).strip())),
        ]:
            if op in condition:
                parts = condition.split(op, 1)
                left_key = parts[0].strip()
                right_val = parts[1].strip() if len(parts) > 1 else ""
                # Resolve left side from context
                left_val = latest.get(left_key, variables.get(left_key, left_key))
                try:
                    return fn(left_val, right_val)
                except (ValueError, TypeError):
                    return False

        # Truthy check: just a variable name
        val = latest.get(condition, variables.get(condition))
        return bool(val)

    for rule in rules:
        if not isinstance(rule, dict):
            continue
        condition = _s(rule.get("condition"), "")
        label = _s(rule.get("label"), "default")
        if _eval_condition(condition):
            return {"branch": label, "matched": True, "condition": condition}

    # Default: first rule or "default"
    default_label = _s((rules[0] or {}).get("label"), "default") if rules else "default"
    return {"branch": default_label, "matched": False, "reason": "No condition matched, using default."}


async def execute_rag_node(node: dict, context: dict, config: dict) -> dict:
    """Knowledge retrieval — uses LLM summarization as fallback when no vector DB."""
    from backend.adapters.llm.gateway_provider import GatewayLLMProvider

    query = resolve_template(_s(config.get("query"), ""), context)
    if not query:
        query = _s(context.get("latest", {}).get("text", ""), "workflow context")
    sources = _safe_list(config.get("sources")) or ["knowledge-base"]
    openclaw_dir = _s(context.get("openclaw_dir"))

    provider = GatewayLLMProvider(openclaw_dir=openclaw_dir)
    result = await provider.complete(
        [{"role": "user", "content": f"Based on the query: '{query}', provide relevant knowledge and context. Sources: {', '.join(sources)}."}],
        system="You are a knowledge retrieval assistant. Provide concise, relevant information.",
        model="claude-sonnet-4-20250514", max_tokens=2048, temperature=0.1,
    )
    return {
        "query": query,
        "sources": sources,
        "highlights": [result.text[:500]] if result.text else [],
        "fullText": result.text,
    }


async def execute_variable_node(node: dict, context: dict, config: dict) -> dict:
    """Store variables in the execution context."""
    assignments = {}
    for item in _safe_list(config.get("assignments")):
        if not isinstance(item, dict):
            continue
        key = _s(item.get("name") or item.get("key") or item.get("variable"))
        if key:
            value = resolve_template(item.get("value"), context)
            assignments[key] = value
            context.setdefault("variables", {})[key] = value
    return {"assignments": assignments}


async def execute_iteration_node(node: dict, context: dict, config: dict) -> dict:
    """Iterate over a list, making each item available as a context variable.

    The actual child-node execution is handled by the orchestrator
    (workflow_api._execute_workflow_run) which reads the output and
    loops over ``items``, running the "loop-body" branch edges for
    each item and then continuing from "done".

    This executor resolves the items list and prepares the iteration
    metadata.  If the orchestrator does not support graph branching
    yet it still produces a useful summary.
    """
    items_raw = resolve_template(_s(config.get("itemsPath"), ""), context)
    # Parse items from JSON string if needed
    if isinstance(items_raw, str):
        items_raw = items_raw.strip()
        if items_raw.startswith("["):
            try:
                items_raw = json.loads(items_raw)
            except (json.JSONDecodeError, ValueError):
                items_raw = [line.strip() for line in items_raw.split("\n") if line.strip()]
        else:
            items_raw = [line.strip() for line in items_raw.split("\n") if line.strip()]
    items = _safe_list(items_raw)

    max_iter = int(config.get("maxIterations") or config.get("max_iterations") or 100)
    items = items[:max_iter]
    var_name = _s(config.get("iterationVariable") or config.get("iteration_variable"), "item")

    # Store iteration metadata in context for downstream use
    context.setdefault("variables", {})["_iteration_items"] = items
    context["variables"]["_iteration_variable"] = var_name
    context["variables"]["_iteration_count"] = len(items)

    return {
        "items": items,
        "count": len(items),
        "variable": var_name,
        "maxIterations": max_iter,
    }


async def execute_parallel_node(node: dict, context: dict, config: dict) -> dict:
    """Parallel branching — declares branches and passes context through.

    Real parallel execution is handled by the orchestrator which reads
    the ``branches`` output and fans out to the matching edge labels.
    This executor simply prepares the branch metadata.
    """
    branches = _safe_list(config.get("branches")) or ["branch-1", "branch-2"]
    return {
        "branches": branches,
        "count": len(branches),
        "input": context.get("latest", {}),
    }


async def execute_end_node(node: dict, context: dict, config: dict) -> dict:
    """Finalize workflow output."""
    deliverables = _safe_list(config.get("deliverables"))
    return {
        "summary": "Workflow finished successfully.",
        "deliverables": deliverables,
        "finalOutput": context.get("latest", {}),
    }


# ── Executor registry ────────────────────────────────────────────────

NODE_EXECUTORS: dict[str, Any] = {
    "start": execute_start_node,
    "trigger-schedule": execute_start_node,
    "trigger-webhook": execute_start_node,
    "llm": execute_llm_node,
    "agent": execute_agent_node,
    "code": execute_code_node,
    "http-request": execute_http_node,
    "tool": execute_tool_node,
    "if-else": execute_condition_node,
    "iteration": execute_iteration_node,
    "parallel": execute_parallel_node,
    "knowledge-retrieval": execute_rag_node,
    "variable-assigner": execute_variable_node,
    "end": execute_end_node,
}


# ── Error handling wrapper ────────────────────────────────────────────


async def execute_node(node: dict, context: dict) -> dict:
    """Execute a single workflow node with error handling.

    Supports three error strategies via node.config.onError:
      - "fail" (default): raise to caller, workflow stops
      - "skip": return skipped status, workflow continues
      - "retry": retry N times with exponential backoff
    """
    node_type = _s(node.get("type"), "agent")
    config = node.get("config") if isinstance(node.get("config"), dict) else {}

    executor_fn = NODE_EXECUTORS.get(node_type)
    if not executor_fn:
        raise ValueError(f"Unknown node type: {node_type}")

    strategy = _s(config.get("onError"), "fail")
    max_retries = int(config.get("retries") or config.get("maxRetries") or 2)

    last_error = None
    for attempt in range(max_retries + 1):
        try:
            output = await executor_fn(node, context, config)
            # Store in node-indexed context
            node_id = _s(node.get("id"), f"step_{id(node)}")
            context.setdefault("nodes", {})[node_id] = {"output": output, "status": "success"}
            context["latest"] = output
            return output
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Node %s (%s) failed attempt %d/%d: %s",
                _s(node.get("title")), node_type, attempt + 1, max_retries + 1, exc,
            )
            if strategy == "skip":
                skipped = {"status": "skipped", "error": str(exc)}
                node_id = _s(node.get("id"), f"step_{id(node)}")
                context.setdefault("nodes", {})[node_id] = {"output": skipped, "status": "skipped"}
                context["latest"] = skipped
                return skipped
            if strategy == "retry" and attempt < max_retries:
                await asyncio.sleep(2 ** attempt)  # 1s, 2s, 4s...
                continue
            raise

    raise last_error  # type: ignore[misc]
