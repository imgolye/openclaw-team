"""LLM Provider via OpenClaw Gateway.

Calls the gateway's OpenAI-compatible chat completions endpoint,
which handles model routing, API key management, and multi-provider
support (Claude, GPT, Gemma, DeepSeek, etc.).

The gateway token and URL are read from the openclaw config.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx

from backend.domain.core.query_engine import LLMProvider, TurnResult

logger = logging.getLogger(__name__)

DEFAULT_GATEWAY_URL = "http://127.0.0.1:18789"


def _normalize_provider_model_name(model: str = "") -> str:
    """Strip provider prefixes like ``zai/`` before calling provider APIs."""
    normalized = str(model or "").strip()
    if "/" not in normalized:
        return normalized
    provider_prefix, remainder = normalized.split("/", 1)
    if provider_prefix.strip().lower() in {
        "zai",
        "zhipu",
        "openai",
        "anthropic",
        "google",
        "gemini",
        "deepseek",
    } and remainder.strip():
        return remainder.strip()
    return normalized


def _load_gateway_config(openclaw_dir: str = "") -> dict:
    """Load gateway URL and auth token from openclaw config."""
    candidates = []
    if openclaw_dir:
        candidates.append(os.path.join(openclaw_dir, "openclaw.json"))
    candidates.extend([
        os.path.join(os.getcwd(), "openclaw.json"),
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "openclaw.json"),
    ])
    for path in candidates:
        try:
            with open(os.path.abspath(path), "r") as f:
                config = json.load(f)
                gw = config.get("gateway", {})
                return {
                    "url": f"http://127.0.0.1:{gw.get('port', 18789)}",
                    "token": gw.get("auth", {}).get("token", ""),
                }
        except (FileNotFoundError, json.JSONDecodeError):
            continue
    return {"url": DEFAULT_GATEWAY_URL, "token": ""}


def _load_provider_api_key(openclaw_dir: str, model: str) -> str:
    """Look up the provider API key for a given model from the database."""
    try:
        from backend.adapters.storage.dashboard import list_model_provider_configs
        configs = list_model_provider_configs(openclaw_dir)
        # Map model prefixes to provider IDs
        model_lower = model.lower()
        provider_id = ""
        if "claude" in model_lower or "anthropic" in model_lower:
            provider_id = "anthropic"
        elif "gpt" in model_lower or "o1" in model_lower or "o3" in model_lower or "o4" in model_lower:
            provider_id = "openai"
        elif "gemma" in model_lower or "gemini" in model_lower:
            provider_id = "google"
        elif "glm" in model_lower or "zhipu" in model_lower:
            provider_id = "zhipu"
        elif "deepseek" in model_lower:
            provider_id = "deepseek"

        for cfg in configs:
            if not isinstance(cfg, dict):
                continue
            if cfg.get("providerId") == provider_id and cfg.get("status") == "active":
                key = cfg.get("keyValue", "")
                if key:
                    return key
        # Fallback: return first active key
        for cfg in configs:
            if isinstance(cfg, dict) and cfg.get("status") == "active" and cfg.get("keyValue"):
                return cfg["keyValue"]
    except Exception:
        logger.debug("Could not load provider API key from database", exc_info=True)
    return ""


# ── Tool schema conversion ────────────────────────────────────────────

TOOL_SCHEMAS = [
    {
        "name": "bash",
        "description": "Execute a shell command. Use for system operations, running scripts, and terminal tasks.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The shell command to execute"},
                "timeout": {"type": "integer", "description": "Timeout in seconds (max 120)", "default": 60},
            },
            "required": ["command"],
        },
    },
    {
        "name": "file_read",
        "description": "Read the contents of a file. Returns numbered lines.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Absolute or relative path to the file"},
                "offset": {"type": "integer", "description": "Line offset to start from (0-based)", "default": 0},
                "limit": {"type": "integer", "description": "Number of lines to read", "default": 2000},
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "file_write",
        "description": "Write content to a file (creates or overwrites).",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Absolute or relative path"},
                "content": {"type": "string", "description": "The content to write"},
            },
            "required": ["file_path", "content"],
        },
    },
    {
        "name": "file_edit",
        "description": "Edit a file by replacing a specific string with a new string.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path to the file to edit"},
                "old_string": {"type": "string", "description": "The exact string to find and replace"},
                "new_string": {"type": "string", "description": "The replacement string"},
                "replace_all": {"type": "boolean", "description": "Replace all occurrences", "default": False},
            },
            "required": ["file_path", "old_string", "new_string"],
        },
    },
    {
        "name": "glob",
        "description": "Find files matching a glob pattern (e.g. '**/*.py').",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern"},
                "path": {"type": "string", "description": "Directory to search in"},
            },
            "required": ["pattern"],
        },
    },
    {
        "name": "grep",
        "description": "Search file contents with a regex pattern.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Regex pattern to search for"},
                "path": {"type": "string", "description": "File or directory to search in"},
            },
            "required": ["pattern"],
        },
    },
]


def get_tool_schemas() -> list[dict]:
    """Return tool definitions in Claude API format."""
    return TOOL_SCHEMAS


# ── Provider ────────────────────────────────────────────────────────────


class GatewayLLMProvider(LLMProvider):
    """LLM provider that calls the OpenClaw Gateway chat completions endpoint.

    The gateway handles model routing, API key management, and supports
    multiple providers (Anthropic, OpenAI, Google, etc.).
    """

    def __init__(
        self,
        *,
        gateway_url: str = "",
        gateway_token: str = "",
        openclaw_dir: str = "",
    ):
        if gateway_url and gateway_token:
            self._url = gateway_url.rstrip("/")
            self._token = gateway_token
        else:
            config = _load_gateway_config(openclaw_dir)
            self._url = config["url"]
            self._token = config["token"]
        self._openclaw_dir = openclaw_dir
        self._provider_key_cache: dict[str, str] = {}

    async def complete(
        self,
        messages: list[dict],
        *,
        system: str = "",
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 8192,
        temperature: float = 0.0,
        tools: list[dict] | None = None,
    ) -> TurnResult:
        """Call the LLM provider API.

        Determines the provider from the model name and calls the appropriate
        API directly (Anthropic Messages API or OpenAI Chat Completions).
        Falls back to the gateway's /v1/chat/completions endpoint.
        """
        provider_key = self._get_provider_key(model)
        model_lower = model.lower()

        # Route to the correct API based on model name
        if provider_key and ("claude" in model_lower or "anthropic" in model_lower):
            return await self._complete_anthropic(
                messages, system=system, model=model, max_tokens=max_tokens,
                temperature=temperature, tools=tools, api_key=provider_key,
            )
        elif provider_key and any(p in model_lower for p in ("gpt", "o1", "o3", "o4")):
            return await self._complete_openai(
                messages, system=system, model=model, max_tokens=max_tokens,
                temperature=temperature, tools=tools, api_key=provider_key,
            )
        elif provider_key and ("glm" in model_lower or "zhipu" in model_lower):
            logger.warning("[llm] zhipu call: model=%s key_prefix=%s", model, provider_key[:12])
            return await self._complete_openai_compatible(
                messages, system=system, model=model, max_tokens=max_tokens,
                temperature=temperature, tools=tools, api_key=provider_key,
                base_url="https://open.bigmodel.cn/api/paas/v4",
            )
        elif provider_key and "deepseek" in model_lower:
            return await self._complete_openai_compatible(
                messages, system=system, model=model, max_tokens=max_tokens,
                temperature=temperature, tools=tools, api_key=provider_key,
                base_url="https://api.deepseek.com",
            )
        else:
            # Fallback: try gateway
            return await self._complete_gateway(
                messages, system=system, model=model, max_tokens=max_tokens,
                temperature=temperature, tools=tools,
            )

    async def _complete_anthropic(
        self, messages, *, system, model, max_tokens, temperature, tools, api_key,
    ) -> TurnResult:
        """Call Anthropic Messages API directly."""
        api_messages = []
        for msg in messages:
            role = msg.get("role", "user")
            if role == "tool":
                api_messages.append({
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": msg.get("tool_use_id", ""),
                            "content": str(msg.get("content", "")),
                        }
                    ],
                })
            elif role == "system":
                continue  # system handled separately
            else:
                api_messages.append({"role": role, "content": msg.get("content", "")})

        api_model = _normalize_provider_model_name(model)
        payload: dict[str, Any] = {
            "model": api_model,
            "messages": api_messages,
            "max_tokens": max_tokens,
        }
        if system:
            payload["system"] = system
        if temperature > 0:
            payload["temperature"] = temperature
        if tools:
            payload["tools"] = tools

        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }

        async with httpx.AsyncClient(timeout=180) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                json=payload, headers=headers,
            )
            response.raise_for_status()
            data = response.json()

        return self._parse_anthropic_response(data, model)

    async def _complete_openai(
        self, messages, *, system, model, max_tokens, temperature, tools, api_key,
    ) -> TurnResult:
        """Call OpenAI Chat Completions API directly."""
        api_messages = []
        if system:
            api_messages.append({"role": "system", "content": system})
        for msg in messages:
            role = msg.get("role", "user")
            if role == "tool":
                api_messages.append({
                    "role": "tool",
                    "tool_call_id": msg.get("tool_use_id", ""),
                    "content": str(msg.get("content", "")),
                })
            else:
                api_messages.append({"role": role, "content": msg.get("content", "")})

        api_model = _normalize_provider_model_name(model)
        payload: dict[str, Any] = {
            "model": api_model,
            "messages": api_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if tools:
            # Convert Claude tool format to OpenAI function format
            openai_tools = []
            for t in tools:
                openai_tools.append({
                    "type": "function",
                    "function": {
                        "name": t.get("name", ""),
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", {}),
                    },
                })
            payload["tools"] = openai_tools

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        async with httpx.AsyncClient(timeout=180) as client:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                json=payload, headers=headers,
            )
            response.raise_for_status()
            data = response.json()

        return self._parse_response(data, model)

    async def _complete_openai_compatible(
        self, messages, *, system, model, max_tokens, temperature, tools, api_key,
        base_url: str,
    ) -> TurnResult:
        """Call an OpenAI-compatible API (Zhipu, DeepSeek, etc.)."""
        api_messages = []
        if system:
            api_messages.append({"role": "system", "content": system})
        for msg in messages:
            role = msg.get("role", "user")
            if role == "tool":
                api_messages.append({
                    "role": "tool",
                    "tool_call_id": msg.get("tool_use_id", ""),
                    "content": str(msg.get("content", "")),
                })
            elif role == "assistant" and msg.get("tool_calls"):
                # Preserve tool_calls on assistant messages so the API can
                # match subsequent tool-result messages to their origins.
                api_messages.append({
                    "role": "assistant",
                    "content": msg.get("content", "") or "",
                    "tool_calls": msg["tool_calls"],
                })
            else:
                api_messages.append({"role": role, "content": msg.get("content", "")})

        api_model = _normalize_provider_model_name(model)
        payload: dict[str, Any] = {
            "model": api_model,
            "messages": api_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if tools:
            openai_tools = []
            for t in tools:
                openai_tools.append({
                    "type": "function",
                    "function": {
                        "name": t.get("name", ""),
                        "description": t.get("description", ""),
                        "parameters": t.get("input_schema", {}),
                    },
                })
            payload["tools"] = openai_tools
            payload["tool_choice"] = "auto"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        endpoint = f"{base_url.rstrip('/')}/chat/completions"
        async with httpx.AsyncClient(timeout=180) as client:
            response = await client.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        return self._parse_response(data, model)

    async def _complete_gateway(
        self, messages, *, system, model, max_tokens, temperature, tools,
    ) -> TurnResult:
        """Fallback: call the gateway's chat completions endpoint."""
        api_messages = []
        if system:
            api_messages.append({"role": "system", "content": system})
        for msg in messages:
            role = msg.get("role", "user")
            if role == "tool":
                api_messages.append({
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": msg.get("tool_use_id", ""),
                            "content": str(msg.get("content", "")),
                        }
                    ],
                })
            else:
                api_messages.append({"role": role, "content": msg.get("content", "")})

        api_model = _normalize_provider_model_name(model)
        payload: dict[str, Any] = {
            "model": api_model,
            "messages": api_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if tools:
            payload["tools"] = tools

        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        endpoint = f"{self._url}/v1/chat/completions"
        async with httpx.AsyncClient(timeout=180) as client:
            response = await client.post(endpoint, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        return self._parse_response(data, model)

    def _get_provider_key(self, model: str) -> str:
        """Get the provider API key for a model, with caching."""
        model_lower = model.lower()
        # Determine cache key by provider
        if "claude" in model_lower or "anthropic" in model_lower:
            cache_key = "anthropic"
        elif "gpt" in model_lower or "o1" in model_lower or "o3" in model_lower or "o4" in model_lower:
            cache_key = "openai"
        elif "gemma" in model_lower or "gemini" in model_lower:
            cache_key = "google"
        else:
            cache_key = model_lower.split("-")[0] if model_lower else "default"

        if cache_key in self._provider_key_cache:
            return self._provider_key_cache[cache_key]

        key = _load_provider_api_key(self._openclaw_dir, model) if self._openclaw_dir else ""
        self._provider_key_cache[cache_key] = key
        return key

    def _parse_response(self, data: dict, model: str) -> TurnResult:
        """Parse the gateway response into a TurnResult."""
        # Handle Anthropic Messages API format
        if "content" in data:
            return self._parse_anthropic_response(data, model)

        # Handle OpenAI-compatible format
        choices = data.get("choices", [])
        if not choices:
            return TurnResult(text="[Empty response]", stop_reason="end_turn", model=model)

        choice = choices[0]
        message = choice.get("message", {})
        text = message.get("content", "") or ""
        stop_reason = choice.get("finish_reason", "stop")

        tool_calls = []
        for tc in message.get("tool_calls", []):
            func = tc.get("function", {})
            try:
                args = json.loads(func.get("arguments", "{}"))
            except json.JSONDecodeError:
                args = {}
            tool_calls.append({
                "id": tc.get("id", ""),
                "name": func.get("name", ""),
                "input": args,
            })

        if tool_calls:
            logger.info("[llm] parsed %d tool_calls: %s", len(tool_calls),
                        [tc["name"] for tc in tool_calls])

        usage = data.get("usage", {})

        return TurnResult(
            text=text,
            tool_calls=tool_calls,
            stop_reason="end_turn" if stop_reason in ("stop", "end_turn") else stop_reason,
            input_tokens=usage.get("prompt_tokens", 0),
            output_tokens=usage.get("completion_tokens", 0),
            model=data.get("model", model),
            raw_response=data,
        )

    def _parse_anthropic_response(self, data: dict, model: str) -> TurnResult:
        """Parse Anthropic Messages API response."""
        content_blocks = data.get("content", [])
        text_parts = []
        tool_calls = []

        for block in content_blocks:
            if block.get("type") == "text":
                text_parts.append(block.get("text", ""))
            elif block.get("type") == "tool_use":
                tool_calls.append({
                    "id": block.get("id", ""),
                    "name": block.get("name", ""),
                    "input": block.get("input", {}),
                })

        usage = data.get("usage", {})
        stop_reason = data.get("stop_reason", "end_turn")

        return TurnResult(
            text="\n".join(text_parts),
            tool_calls=tool_calls,
            stop_reason=stop_reason,
            input_tokens=usage.get("input_tokens", 0),
            output_tokens=usage.get("output_tokens", 0),
            model=data.get("model", model),
            raw_response=data,
        )
