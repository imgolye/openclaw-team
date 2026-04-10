#!/usr/bin/env python3
"""OpenClaw Team dashboard entrypoint."""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)

from backend.presentation.http import runtime as _runtime

globals().update(
    {
        name: value
        for name, value in vars(_runtime).items()
        if not (name.startswith("__") and name.endswith("__"))
    }
)
del _runtime

if __name__ == "__main__":
    main()
