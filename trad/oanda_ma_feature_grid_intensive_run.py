#!/usr/bin/env python3
"""Reproduce the configured intensive MA-grid challengers and audit."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / "config" / "ma_feature_grid_intensive_v1.json"
DEFAULT_STATE = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "ma_feature_grid_intensive_audit"
    / "intensive_run_latest.json"
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve(root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def challenger_command(
    root: Path,
    config: dict[str, Any],
    challenger: dict[str, Any],
) -> list[str]:
    surface = config["surface"]
    command = [
        sys.executable,
        str(root / "oanda_ma_feature_grid_fit.py"),
        "--model-dir",
        str(resolve(root, challenger["model_dir"])),
        "--report-dir",
        str(resolve(root, challenger["report_dir"])),
        "--timeframes",
        ",".join(surface["timeframes"]),
        "--horizons-sec",
        ",".join(str(value) for value in surface["horizons_sec"]),
        "--samples-per-pair-timeframe",
        str(challenger["samples_per_pair_timeframe"]),
        "--minimum-split-rows",
        "500",
        "--estimator",
        challenger["estimator"],
        "--pair-context",
        challenger["pair_context"],
        "--target-space",
        challenger["target_space"],
        "--split-policy",
        config["source_contract"]["split_policy"],
        (
            "--executable-edge-head"
            if challenger.get("executable_edge_head")
            else "--no-executable-edge-head"
        ),
        "--no-stamped-copy",
    ]
    if challenger.get("dataset_cache"):
        command.extend(
            [
                "--dataset-cache",
                str(resolve(root, challenger["dataset_cache"])),
            ]
        )
    if challenger["name"].endswith("liquid_pairs"):
        command.extend(
            [
                "--instruments",
                ",".join(surface["liquid_pairs"]),
            ]
        )
    if challenger["estimator"] == "xgboost":
        command.extend(
            [
                "--xgb-device",
                str(challenger["xgb_device"]),
                "--xgb-estimators",
                str(challenger["xgb_estimators"]),
                "--xgb-max-depth",
                str(challenger["xgb_max_depth"]),
                "--xgb-learning-rate",
                str(challenger["xgb_learning_rate"]),
            ]
        )
    return command


def write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--challenger",
        action="append",
        default=[],
        help="challenger name; repeat to select several (default: all)",
    )
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    config = json.loads(args.config.read_text(encoding="utf-8"))
    selected = set(args.challenger)
    challengers = [
        row
        for row in config["challengers"]
        if not selected or row["name"] in selected
    ]
    missing = selected - {row["name"] for row in challengers}
    if missing:
        raise SystemExit(f"unknown challengers: {sorted(missing)}")

    state: dict[str, Any] = {
        "schema_version": 1,
        "experiment": config["experiment"],
        "started_at": utc_now(),
        "updated_at": utc_now(),
        "complete": False,
        "execution_policy": "shadow_only",
        "config": str(args.config.resolve()),
        "runs": [],
    }
    for challenger in challengers:
        report_path = (
            resolve(ROOT, challenger["report_dir"])
            / "ma_feature_grid_latest.json"
        )
        command = challenger_command(ROOT, config, challenger)
        run = {
            "name": challenger["name"],
            "started_at": utc_now(),
            "command": command,
            "report": str(report_path.resolve()),
            "status": "dry_run" if args.dry_run else "running",
        }
        state["runs"].append(run)
        state["updated_at"] = utc_now()
        write_state(args.state, state)
        if args.dry_run:
            continue
        if report_path.is_file() and not args.force:
            run["status"] = "skipped_existing"
            run["completed_at"] = utc_now()
            continue
        completed = subprocess.run(command, cwd=ROOT, check=False)
        run["returncode"] = completed.returncode
        run["status"] = "completed" if completed.returncode == 0 else "failed"
        run["completed_at"] = utc_now()
        state["updated_at"] = utc_now()
        write_state(args.state, state)
        if completed.returncode:
            return completed.returncode

    if not args.dry_run:
        audit = subprocess.run(
            [sys.executable, str(ROOT / "oanda_ma_feature_grid_intensive_audit.py")],
            cwd=ROOT,
            check=False,
        )
        state["audit_returncode"] = audit.returncode
        if audit.returncode:
            state["updated_at"] = utc_now()
            write_state(args.state, state)
            return audit.returncode
    state["complete"] = True
    state["completed_at"] = utc_now()
    state["updated_at"] = state["completed_at"]
    write_state(args.state, state)
    print(json.dumps(state, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
