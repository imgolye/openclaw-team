from __future__ import annotations

from types import SimpleNamespace

from .agent import _handle_agent_commands
from .chat import _handle_chat_commands
from .computer_use import _handle_computer_use_commands
from .management import _handle_management_commands
from .platform import _handle_platform_commands


def handle_action_post(handler, path, payload, services):
    svc = SimpleNamespace(**services)
    for dispatcher in (
        _handle_agent_commands,
        _handle_computer_use_commands,
        _handle_management_commands,
        _handle_chat_commands,
        _handle_platform_commands,
    ):
        if dispatcher(handler, path, payload, svc):
            return True
    return False
