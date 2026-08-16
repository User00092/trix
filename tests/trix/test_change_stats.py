from __future__ import annotations

import subprocess
from shutil import which

from trix.change_stats import repository_change_stats


def test_repository_change_stats_counts_files_and_lines(tmp_path) -> None:
    git = which("git")
    assert git is not None

    def run(*args: str) -> None:
        subprocess.run([git, *args], check=True)  # noqa: S603 -- fixed test executable

    run("init", "-q", str(tmp_path))
    run("-C", str(tmp_path), "config", "user.email", "test@example.com")
    run("-C", str(tmp_path), "config", "user.name", "Test")
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("one\ntwo\n", encoding="utf-8")
    run("-C", str(tmp_path), "add", "tracked.txt")
    run("-C", str(tmp_path), "commit", "-qm", "initial")

    tracked.write_text("one\nchanged\nthree\n", encoding="utf-8")
    (tmp_path / "new.txt").write_text("untracked\n", encoding="utf-8")

    assert repository_change_stats(str(tmp_path)) == {
        "files": 2,
        "additions": 2,
        "deletions": 1,
        "available": True,
    }

    run("-C", str(tmp_path), "add", "tracked.txt")
    run("-C", str(tmp_path), "commit", "-qm", "agent change")
    assert repository_change_stats(str(tmp_path), "1970-01-01T00:00:00+00:00") == {
        "files": 2,
        "additions": 4,
        "deletions": 1,
        "available": True,
    }


def test_repository_change_stats_gracefully_handles_non_repository(tmp_path) -> None:
    assert repository_change_stats(str(tmp_path)) == {
        "files": 0,
        "additions": 0,
        "deletions": 0,
        "available": False,
    }
