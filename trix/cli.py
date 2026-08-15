from __future__ import annotations

import argparse
import asyncio
import sys


def event_loop_factory(*, use_subprocess: bool = False) -> asyncio.AbstractEventLoop:
    """Create an event loop that can launch Codex on Windows.

    Uvicorn normally selects a ``SelectorEventLoop`` on Windows when reload or
    multiple workers are enabled. That loop does not implement asyncio's
    subprocess APIs, which Trix needs for ``codex app-server``.
    """
    del use_subprocess
    return (
        asyncio.ProactorEventLoop()
        if sys.platform == "win32"
        else asyncio.SelectorEventLoop()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Trix Codex agent orchestration server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()
    import uvicorn

    uvicorn.run(
        "trix.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        loop="trix.cli:event_loop_factory",
    )


if __name__ == "__main__":
    main()
