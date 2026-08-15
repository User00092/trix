from __future__ import annotations

import asyncio
import sys

from trix import cli


def test_event_loop_factory_ignores_uvicorn_subprocess_flag() -> None:
    loop = cli.event_loop_factory(use_subprocess=True)
    try:
        if sys.platform == "win32":
            assert isinstance(loop, asyncio.ProactorEventLoop)
        else:
            assert isinstance(loop, asyncio.SelectorEventLoop)
    finally:
        loop.close()
