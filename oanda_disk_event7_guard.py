#!/usr/bin/env python3
"""Run a command and stop its process tree if Windows logs a new disk Event 7."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def latest_event7_record() -> int:
    command = [
        "powershell.exe",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "(Get-WinEvent -FilterHashtable @{LogName='System';Id=7} "
        "-MaxEvents 1 -ErrorAction SilentlyContinue).RecordId",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"Event 7 query failed: {detail}")
    value = completed.stdout.strip()
    return int(value) if value else 0


def windows_process_tree(root_pid: int) -> list[int]:
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId | ConvertTo-Json -Compress",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0 or not completed.stdout.strip():
        return [root_pid]
    rows = json.loads(completed.stdout)
    if isinstance(rows, dict):
        rows = [rows]
    children: dict[int, list[int]] = {}
    for row in rows:
        parent = int(row.get("ParentProcessId") or 0)
        child = int(row.get("ProcessId") or 0)
        children.setdefault(parent, []).append(child)
    discovered: list[int] = []
    frontier = [root_pid]
    while frontier:
        parent = frontier.pop()
        for child in children.get(parent, []):
            if child not in discovered:
                discovered.append(child)
                frontier.append(child)
    return [*reversed(discovered), root_pid]


def live_process_ids() -> set[int]:
    completed = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "(Get-Process -ErrorAction SilentlyContinue).Id -join ','",
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        return set()
    return {
        int(value)
        for value in completed.stdout.strip().split(",")
        if value.strip().isdigit()
    }


def terminate_process_tree(process: subprocess.Popen[Any]) -> list[int]:
    tracked = windows_process_tree(process.pid)
    deadline = time.monotonic() + 120.0
    survivors = tracked
    while survivors and time.monotonic() < deadline:
        for pid in survivors:
            subprocess.run(
                ["taskkill.exe", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        time.sleep(2.0)
        live = live_process_ids()
        survivors = [pid for pid in tracked if pid in live]
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    return survivors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-record", type=int, required=True)
    parser.add_argument("--poll-sec", type=float, default=10.0)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")
    if args.poll_sec < 1.0:
        parser.error("--poll-sec must be at least 1 second")
    return args


def main() -> int:
    args = parse_args()
    observed_at_start = latest_event7_record()
    report: dict[str, Any] = {
        "schema_version": 1,
        "started_utc": utc_iso(),
        "status": "starting",
        "baseline_event7_record": args.baseline_record,
        "observed_event7_record_at_start": observed_at_start,
        "command": args.command,
        "cwd": str(Path.cwd().resolve()),
    }
    if observed_at_start > args.baseline_record:
        report.update(
            {
                "status": "refused_new_event7_before_start",
                "finished_utc": utc_iso(),
                "latest_event7_record": observed_at_start,
            }
        )
        write_json_atomic(args.report, report)
        print(json.dumps(report, indent=2), flush=True)
        return 3

    process = subprocess.Popen(args.command, cwd=Path.cwd())
    report.update({"status": "running", "child_pid": process.pid})
    write_json_atomic(args.report, report)
    newest = observed_at_start
    try:
        while process.poll() is None:
            time.sleep(args.poll_sec)
            newest = latest_event7_record()
            if newest > args.baseline_record:
                survivors = terminate_process_tree(process)
                report.update(
                    {
                        "status": (
                            "stopped_new_event7"
                            if not survivors
                            else "stop_timeout_new_event7"
                        ),
                        "latest_event7_record": newest,
                        "child_returncode": process.returncode,
                        "surviving_process_ids": survivors,
                        "finished_utc": utc_iso(),
                    }
                )
                write_json_atomic(args.report, report)
                print(json.dumps(report, indent=2), flush=True)
                return 3
        child_returncode = int(process.returncode or 0)
        newest = latest_event7_record()
        status = "completed" if child_returncode == 0 else "child_failed"
        exit_code = child_returncode
        if newest > args.baseline_record:
            status = "completed_with_new_event7"
            exit_code = 3
        report.update(
            {
                "status": status,
                "latest_event7_record": newest,
                "child_returncode": child_returncode,
                "finished_utc": utc_iso(),
            }
        )
        write_json_atomic(args.report, report)
        return exit_code
    except BaseException as exc:
        survivors = terminate_process_tree(process)
        report.update(
            {
                "status": "guard_failed",
                "error": f"{type(exc).__name__}: {exc}",
                "latest_event7_record": newest,
                "child_returncode": process.returncode,
                "surviving_process_ids": survivors,
                "finished_utc": utc_iso(),
            }
        )
        write_json_atomic(args.report, report)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
