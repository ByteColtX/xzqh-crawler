"""Command-line interface.

`pyproject.toml` exposes an entrypoint:

    xzqh = "xzqh_crawler.cli:main"

This file keeps the CLI stable while allowing `python -m xzqh_crawler` to work
(via `__main__.py`).
"""

from __future__ import annotations

from .__main__ import main

__all__ = ["main"]
