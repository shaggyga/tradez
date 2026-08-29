#!/usr/bin/env python3
"""Launch the research-only OANDA trainer with local workspace paths.

This is a thin bootstrapper for Windows background launchers.  It avoids long
``python -c`` command lines, appends the project virtualenv site-packages, and
writes stdout/stderr plus a latest-launch JSON into ``data/runtime_logs``.
"""

from __future__ import annotations

import json
import os
import runpy
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SITE_PACKAGES = ROOT / "..venv" / "Lib" / "site-packages"
RUNTIME_LOGS = ROOT / "data" / "runtime_logs"
MANAGER = ROOT / "oanda_gpt_training_strategy_manager.py"
LATEST = RUNTIME_LOGS / "research_trainer_launcher_latest.json"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_latest(payload: dict[str, object]) -> None:
    LATEST.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    RUNTIME_LOGS.mkdir(parents=True, exist_ok=True)
    if "--smoke" in sys.argv:
        write_latest(
            {
                "pid": os.getpid(),
                "started_utc": utc_iso(),
                "script": str(MANAGER),
                "status": "smoke_ok",
            }
        )
        return 0
    stamp = utc_stamp()
    out_path = RUNTIME_LOGS / f"research_trainer_launcher_{stamp}.out.log"
    err_path = RUNTIME_LOGS / f"research_trainer_launcher_{stamp}.err.log"
    latest_payload = {
        "pid": os.getpid(),
        "started_utc": utc_iso(),
        "script": str(MANAGER),
        "stdout": str(out_path),
        "stderr": str(err_path),
        "status": "starting",
    }
    write_latest(latest_payload)
    sys.stdout = out_path.open("a", buffering=1, encoding="utf-8")
    sys.stderr = err_path.open("a", buffering=1, encoding="utf-8")
    sys.path.insert(0, str(ROOT))
    sys.path.append(str(SITE_PACKAGES))
    os.environ.setdefault("TRAD_PROJECT_ROOT", str(ROOT))
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("OANDA_SKIP_GPT_REVIEW", "1")
    existing_pythonpath = os.environ.get("PYTHONPATH", "")
    pythonpath_parts = [str(ROOT)]
    if existing_pythonpath:
        for item in existing_pythonpath.split(os.pathsep):
            if not item:
                continue
            try:
                if Path(item).resolve() == SITE_PACKAGES.resolve():
                    continue
            except Exception:
                pass
            pythonpath_parts.append(item)
    os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
    sys.argv = [str(MANAGER)]
    latest_payload.update({"status": "running", "running_utc": utc_iso()})
    write_latest(latest_payload)
    try:
        runpy.run_path(str(MANAGER), run_name="__main__")
        latest_payload.update({"status": "exited", "exited_utc": utc_iso(), "exit_code": 0})
        write_latest(latest_payload)
        return 0
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        latest_payload.update({"status": "exited", "exited_utc": utc_iso(), "exit_code": code})
        write_latest(latest_payload)
        raise
    except BaseException as exc:
        latest_payload.update(
            {
                "status": "crashed",
                "exited_utc": utc_iso(),
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
        write_latest(latest_payload)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
