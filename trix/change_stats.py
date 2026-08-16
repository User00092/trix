from __future__ import annotations

import subprocess
from pathlib import Path
from shutil import which
from typing import Any


def repository_change_stats(repository: str, since: str | None = None) -> dict[str, Any]:
    """Return session-era committed and working-tree totals without failing the API."""
    root = Path(repository)
    result: dict[str, Any] = {"files": 0, "additions": 0, "deletions": 0, "available": False}
    git = which("git")
    if not root.is_dir() or git is None:
        return result
    try:
        status = subprocess.run(  # noqa: S603 -- fixed executable, no shell
            [git, "-C", str(root), "status", "--porcelain=v1", "-z"],
            capture_output=True,
            check=True,
            timeout=5,
        )
        changed_files = {
            entry[3:].split(" -> ")[-1]
            for entry in status.stdout.decode(errors="replace").split("\0")
            if len(entry) >= 4
        }
        working_numstat = subprocess.run(  # noqa: S603 -- fixed executable, no shell
            [git, "-C", str(root), "diff", "HEAD", "--numstat", "--"],
            capture_output=True,
            check=True,
            text=True,
            timeout=5,
        )
        committed_numstat = None
        if since:
            committed_numstat = subprocess.run(  # noqa: S603 -- fixed executable, no shell
                [
                    git,
                    "-C",
                    str(root),
                    "log",
                    f"--since={since}",
                    "--format=",
                    "--numstat",
                    "--",
                ],
                capture_output=True,
                check=True,
                text=True,
                timeout=5,
            )
    except (OSError, subprocess.SubprocessError):
        return result

    additions = 0
    deletions = 0
    outputs = [working_numstat.stdout]
    if committed_numstat is not None:
        outputs.append(committed_numstat.stdout)
    for line in "\n".join(outputs).splitlines():
        parts = line.split("\t", 2)
        if len(parts) != 3:
            continue
        added, deleted, path = parts
        changed_files.add(path)
        if added.isdigit():
            additions += int(added)
        if deleted.isdigit():
            deletions += int(deleted)
    return {
        "files": len(changed_files),
        "additions": additions,
        "deletions": deletions,
        "available": True,
    }
