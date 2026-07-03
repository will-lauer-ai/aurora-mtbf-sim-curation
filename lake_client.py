#!/usr/bin/env python3
"""Thin wrapper around frontier_query.py for data lake SQL queries.

Shells out to the frontier-data-lake skill's CLI tool, which handles
all JWT auth, token caching, and HTTP. No external dependencies beyond
Python stdlib.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

# Resolve the frontier_query.py path
_SKILL_DIR = Path.home() / ".claude" / "skills" / "frontier-data-lake"
_FALLBACK_DIR = Path.home() / "Downloads" / "frontier-data-lake-a223830" / "frontier-data-lake"


def _find_frontier_query() -> str:
    """Find the frontier_query.py executable."""
    # 1. On PATH?
    on_path = shutil.which("frontier_query.py")
    if on_path:
        return on_path
    # 2. In skill dir?
    skill_path = _SKILL_DIR / "frontier_query.py"
    if skill_path.exists():
        return str(skill_path)
    # 3. In fallback dir?
    fallback_path = _FALLBACK_DIR / "frontier_query.py"
    if fallback_path.exists():
        return str(fallback_path)
    raise FileNotFoundError(
        "frontier_query.py not found. Checked PATH, "
        f"{_SKILL_DIR}, and {_FALLBACK_DIR}."
    )


class LakeQueryError(Exception):
    """Raised when a data lake query fails."""

    def __init__(self, message: str, exit_code: int = 0, stderr: str = ""):
        super().__init__(message)
        self.exit_code = exit_code
        self.stderr = stderr


def _run(args: list[str], timeout: int = 300) -> str:
    """Run frontier_query.py with given args, return stdout (str)."""
    fq_path = _find_frontier_query()
    cmd = [fq_path] + args
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if proc.returncode == 3:
        raise LakeQueryError(
            "Auth error (exit code 3). Run `awslogin --sso` and retry.",
            exit_code=3,
            stderr=proc.stderr,
        )
    elif proc.returncode != 0:
        raise LakeQueryError(
            f"Query failed (exit code {proc.returncode}):\n{proc.stderr.strip()}",
            exit_code=proc.returncode,
            stderr=proc.stderr,
        )
    return proc.stdout


def check_auth() -> bool:
    """Verify that auth is working. Returns True if OK."""
    try:
        _run(["--check-auth"])
        return True
    except LakeQueryError as e:
        if e.exit_code == 3:
            print(f"Auth error: {e}", file=sys.stderr)
            return False
        raise


def query(sql: str, limit: int = 50000) -> list[dict]:
    """Run a SQL query and return rows as a list of dicts (JSON mode)."""
    stdout = _run(["--sql", sql, "--format", "json", "--limit", str(limit)])
    stdout = stdout.strip()
    if not stdout:
        return []
    result = json.loads(stdout)
    if isinstance(result, list):
        return result
    # Some queries might return a dict wrapper
    if isinstance(result, dict) and "resultJson" in result:
        inner = json.loads(result["resultJson"])
        return inner if isinstance(inner, list) else [inner]
    return [result] if result else []


def query_table(sql: str, limit: int = 50000) -> str:
    """Run a SQL query and return a pretty table string (for debugging)."""
    return _run(["--sql", sql, "--format", "table", "--limit", str(limit)])


def describe(table: str) -> list[dict]:
    """Describe a table's schema. Returns list of column dicts."""
    stdout = _run(["--describe", table])
    stdout = stdout.strip()
    if not stdout:
        return []
    result = json.loads(stdout)
    return result if isinstance(result, list) else [result]


def sample(table: str, n: int = 10) -> list[dict]:
    """Get a sample of rows from a table."""
    stdout = _run(["--sample", table, "--n", str(n)])
    stdout = stdout.strip()
    if not stdout:
        return []
    result = json.loads(stdout)
    return result if isinstance(result, list) else [result]


def vehicles() -> list[dict]:
    """List all vehicles (run this first to check naming conventions)."""
    stdout = _run(["--vehicles", "--format", "json"])
    stdout = stdout.strip()
    if not stdout:
        return []
    result = json.loads(stdout)
    return result if isinstance(result, list) else [result]


if __name__ == "__main__":
    # Quick smoke test
    print("Checking auth...", file=sys.stderr)
    if check_auth():
        print("Auth OK!", file=sys.stderr)
    else:
        print("Auth FAILED. Run: awslogin --sso", file=sys.stderr)
        sys.exit(1)
