#!/usr/bin/env python3
"""Run project scripts with local packages appended after the stdlib.

The checked-in virtualenv has useful third-party packages but also obsolete
backports. Do not expose that site-packages directory through PYTHONPATH,
because it can shadow Python 3 stdlib modules during interpreter startup.
"""

from __future__ import annotations

import os
import runpy
import sys


ROOT = os.path.dirname(os.path.abspath(__file__))
SITE_PACKAGES = os.path.abspath(os.path.join(ROOT, "..venv", "Lib", "site-packages"))


def _norm(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def _remove_from_sys_path(path: str) -> None:
    target = _norm(path)
    sys.path[:] = [
        item for item in sys.path if _norm(item or os.getcwd()) != target
    ]


def _sanitize_pythonpath() -> None:
    raw = os.environ.get("PYTHONPATH", "")
    if not raw:
        return
    kept = [
        item
        for item in raw.split(os.pathsep)
        if item and _norm(item) != _norm(SITE_PACKAGES)
    ]
    os.environ["PYTHONPATH"] = os.pathsep.join(kept)


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: research_python_runner.py SCRIPT [ARGS...]", file=sys.stderr)
        return 2
    _sanitize_pythonpath()
    _remove_from_sys_path(SITE_PACKAGES)
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    if os.path.isdir(SITE_PACKAGES) and SITE_PACKAGES not in sys.path:
        sys.path.append(SITE_PACKAGES)
    target = sys.argv[1]
    sys.argv = sys.argv[1:]
    runpy.run_path(target, run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
