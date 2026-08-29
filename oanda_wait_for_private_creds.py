#!/usr/bin/env python3
"""Wait for a private OANDA creds file, then start the practice supervisor.

The watcher records only credential key presence and paths. It never copies,
prints, or persists secret values.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = SCRIPT_DIR.parent
DEFAULT_STATE = (
    SCRIPT_DIR
    / "data"
    / "oanda_training_manager"
    / "state"
    / "private_creds_watcher_v1.json"
)
TOKEN_KEYS = (
    "OANDA_API_KEY",
    "OANDA_API_TOKEN",
    "OANDA_ACCESS_TOKEN",
)
DEFAULT_ACCOUNT_KEY = "OANDA_ACCOUNT_ID_DUM4"
PRACTICE_ACCOUNT_SUFFIX = "-007"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def cfg_value(text: str, key: str) -> str:
    escaped = re.escape(key)
    pattern = re.compile(
        rf"(?mi)^\s*(?:export\s+)?[\"']?{escaped}[\"']?\s*[:=]\s*"
        r"(?:\"([^\"]*)\"|'([^']*)'|([^\s,;#]+))"
    )
    match = pattern.search(text)
    if not match:
        return ""
    return next((value for value in match.groups() if value is not None), "").strip()


def inspect_creds(path: Path, account_key: str = DEFAULT_ACCOUNT_KEY) -> dict[str, object]:
    result: dict[str, object] = {
        "path": str(path),
        "exists": path.is_file(),
        "token_present": False,
        "account_present": False,
        "practice_007": False,
        "valid": False,
        "reason": "missing",
    }
    if not path.is_file():
        return result
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError as exc:
        result["reason"] = f"unreadable:{type(exc).__name__}"
        return result

    token = next((cfg_value(text, key) for key in TOKEN_KEYS if cfg_value(text, key)), "")
    account_id = cfg_value(text, account_key)
    result["token_present"] = len(token) >= 20
    result["account_present"] = bool(account_id)
    result["practice_007"] = account_id.endswith(PRACTICE_ACCOUNT_SUFFIX)
    result["valid"] = bool(
        result["token_present"] and result["account_present"] and result["practice_007"]
    )
    if not result["token_present"]:
        result["reason"] = "missing_practice_token"
    elif not result["account_present"]:
        result["reason"] = f"missing_{account_key}"
    elif not result["practice_007"]:
        result["reason"] = "account_key_does_not_resolve_to_007"
    else:
        result["reason"] = "ready"
    return result


def default_candidates(root: Path) -> list[Path]:
    home = Path.home()
    bases = [
        root / "trad",
        home / "Documents" / "forex" / "trad",
        home / "Downloads",
        home / "Desktop",
    ]
    paths: list[Path] = []
    for env_name in ("OANDA_CREDS_PATH", "TRAD_CREDS_PATH"):
        env_path = os.environ.get(env_name, "").strip()
        if env_path:
            paths.append(Path(env_path))
    for base in bases:
        paths.extend(base / name for name in ("creds", "creds.py", "creds.txt"))
    return dedupe_paths(paths)


def dedupe_paths(paths: Iterable[Path]) -> list[Path]:
    output: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        absolute = path.expanduser().absolute()
        key = os.path.normcase(str(absolute))
        if key not in seen:
            seen.add(key)
            output.append(absolute)
    return output


def write_state(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


def supervisor_command(args: argparse.Namespace, creds_path: Path) -> list[str]:
    command = [
        "powershell.exe",
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(args.supervisor),
        "-Root",
        str(args.root),
        "-AccountKey",
        args.account_key,
        "-RunLabel",
        args.run_label,
        "-ChildDurationSec",
        str(args.child_duration_sec),
        "-CredsPath",
        str(creds_path),
    ]
    if args.python:
        command.extend(("-PythonPath", str(args.python)))
    if args.safe_core_only:
        command.append("-SafeCoreOnly")
    if args.enable_crypto:
        command.append("-EnableCrypto")
    return command


def launch_supervisor(args: argparse.Namespace, creds_path: Path) -> tuple[int, Path, Path]:
    state_dir = args.state.parent
    state_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stdout_path = state_dir / f"practice_supervisor_launch_{stamp}.out.log"
    stderr_path = state_dir / f"practice_supervisor_launch_{stamp}.err.log"
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        process = subprocess.Popen(
            supervisor_command(args, creds_path),
            cwd=args.root,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            close_fds=True,
            creationflags=creationflags,
        )
    return process.pid, stdout_path, stderr_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--supervisor", type=Path, default=SCRIPT_DIR / "oanda_always_on_supervisor.ps1")
    parser.add_argument("--python", type=Path, default=None)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--account-key", default=DEFAULT_ACCOUNT_KEY)
    parser.add_argument(
        "--run-label",
        default="unified-signal-confidence-matrix-v10",
    )
    parser.add_argument("--candidate", type=Path, action="append", default=[])
    parser.add_argument("--duration-sec", type=int, default=4 * 60 * 60)
    parser.add_argument("--child-duration-sec", type=int, default=7 * 24 * 60 * 60)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--safe-core-only", action="store_true")
    parser.add_argument("--enable-crypto", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.root = args.root.expanduser().absolute()
    args.supervisor = args.supervisor.expanduser().absolute()
    args.state = args.state.expanduser().absolute()
    candidates = dedupe_paths([*args.candidate, *default_candidates(args.root)])
    started = time.monotonic()
    deadline = started + max(args.duration_sec, 0)

    while True:
        inspections = [inspect_creds(path, args.account_key) for path in candidates]
        ready = next((item for item in inspections if item["valid"]), None)
        public_inspections = [
            {
                "path": item["path"],
                "exists": item["exists"],
                "token_present": item["token_present"],
                "account_present": item["account_present"],
                "practice_007": item["practice_007"],
                "valid": item["valid"],
                "reason": item["reason"],
            }
            for item in inspections
        ]
        state: dict[str, object] = {
            "schema_version": 1,
            "updated_at": utc_now(),
            "status": "waiting_for_private_creds",
            "account_key": args.account_key,
            "elapsed_sec": round(time.monotonic() - started, 1),
            "duration_sec": args.duration_sec,
            "candidates": public_inspections,
            "secrets_persisted": False,
        }
        if ready is not None:
            creds_path = Path(str(ready["path"]))
            state["credential_path"] = str(creds_path)
            state["status"] = "ready_dry_run" if args.dry_run else "launching_practice_supervisor"
            if args.dry_run:
                state["supervisor_command"] = supervisor_command(args, creds_path)
                write_state(args.state, state)
                return 0
            try:
                pid, stdout_path, stderr_path = launch_supervisor(args, creds_path)
            except OSError as exc:
                state["status"] = "launch_error"
                state["error"] = f"{type(exc).__name__}: {exc}"
                write_state(args.state, state)
                return 2
            state.update(
                {
                    "status": "practice_supervisor_launched",
                    "supervisor_pid": pid,
                    "stdout": str(stdout_path),
                    "stderr": str(stderr_path),
                }
            )
            write_state(args.state, state)
            return 0

        write_state(args.state, state)
        if args.once or time.monotonic() >= deadline:
            state["status"] = "credential_wait_expired" if not args.once else "credential_not_found"
            state["updated_at"] = utc_now()
            write_state(args.state, state)
            return 1
        time.sleep(max(args.poll_sec, 0.25))


if __name__ == "__main__":
    sys.exit(main())
