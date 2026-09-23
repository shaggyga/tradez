#!/usr/bin/env python3
"""Run explicit foundation weight downloads in their isolated D-hosted runtimes."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import oanda_model_gap_runtime_profiles as runtime_profiles


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "foundation_model_weights.json"
DEFAULT_RUNTIME_CONFIG = ROOT / "config" / "model_gap_runtime_profiles.json"
DEFAULT_OUTPUT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "model_space"
    / "foundation_weight_sync_latest.json"
)


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


def output_tail(value: str | None, limit: int = 4000) -> str:
    return (value or "")[-limit:]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--runtime-config", type=Path, default=DEFAULT_RUNTIME_CONFIG)
    parser.add_argument("--models", default="")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8-sig"))
    runtimes = json.loads(args.runtime_config.read_text(encoding="utf-8-sig"))["profiles"]
    requested = {
        value.strip() for value in args.models.split(",") if value.strip()
    }
    models = [
        (name, value)
        for name, value in config["models"].items()
        if not requested or name in requested
    ]
    runtime_root = Path(
        os.environ.get(
            "FOREX_MODEL_RUNTIME_ROOT",
            str(ROOT.parent / "runtime" / "model_gap"),
        )
    ).resolve()
    environment = os.environ.copy()
    environment.update(
        {
            "FOREX_MODEL_RUNTIME_ROOT": str(runtime_root),
            "HF_HOME": str(runtime_root / "huggingface"),
            "HF_HUB_CACHE": str(runtime_root / "huggingface" / "hub"),
            "TEMP": str(runtime_root / "temp"),
            "TMP": str(runtime_root / "temp"),
            "PYTHONUTF8": "1",
        }
    )
    records: list[dict[str, Any]] = []
    for name, value in models:
        profile_name = str(value["profile"])
        profile = runtimes.get(profile_name)
        if profile is None:
            record = {
                "model": name,
                "profile": profile_name,
                "status": "runtime_profile_missing",
                "account_wired": False,
            }
            records.append(record)
            if args.stop_on_error:
                break
            continue
        python = runtime_profiles.expand_path(str(profile["python"])).resolve()
        destination = runtime_profiles.expand_path(str(value["destination"])).resolve()
        manifest = destination / "WEIGHT_MANIFEST.json"
        command = [
            str(python),
            str(ROOT / "oanda_foundation_weight_worker.py"),
            "--model",
            name,
            "--repo-id",
            str(value["repo_id"]),
            "--revision",
            str(value.get("revision") or "main"),
            "--destination",
            str(destination),
            "--output",
            str(manifest),
        ]
        print(f"[foundation-weights] {name} <- {value['repo_id']}", flush=True)
        if not python.is_file():
            completed = None
            status = "runtime_missing"
            stderr = f"runtime Python is absent: {python}"
        else:
            completed = subprocess.run(
                command,
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            status = (
                "snapshot_available"
                if completed.returncode == 0 and manifest.is_file()
                else "download_failed"
            )
            stderr = output_tail(completed.stderr)
        record = {
            "model": name,
            "profile": profile_name,
            "python": str(python),
            "repo_id": value["repo_id"],
            "requested_revision": value.get("revision") or "main",
            "destination": str(destination),
            "manifest": str(manifest),
            "status": status,
            "returncode": completed.returncode if completed else None,
            "stdout_tail": output_tail(completed.stdout) if completed else "",
            "stderr_tail": stderr,
            "account_wired": False,
        }
        if status == "snapshot_available":
            snapshot = json.loads(manifest.read_text(encoding="utf-8-sig"))
            record["resolved_revision"] = snapshot.get("resolved_revision")
            record["file_count"] = snapshot.get("file_count")
            record["total_bytes"] = snapshot.get("total_bytes")
        records.append(record)
        write_json_atomic(
            args.output,
            {
                "schema_version": 1,
                "generated_utc": utc_iso(),
                "runtime_root": str(runtime_root),
                "execution_policy": "explicit_download_shadow_only_no_account_wiring",
                "records": records,
                "account_wired": False,
            },
        )
        if status != "snapshot_available" and args.stop_on_error:
            break
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "runtime_root": str(runtime_root),
        "execution_policy": "explicit_download_shadow_only_no_account_wiring",
        "records": records,
        "summary": {
            "requested": len(models),
            "available": sum(row["status"] == "snapshot_available" for row in records),
            "failed": sum(row["status"] != "snapshot_available" for row in records),
            "account_wired": 0,
        },
        "account_wired": False,
    }
    write_json_atomic(args.output, report)
    print(json.dumps(report["summary"], indent=2))
    return 0 if report["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
