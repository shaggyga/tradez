#!/usr/bin/env python3
"""Run the causal ARIMA grid across M1/M5/M15/M30/H1/H4 and varied horizons."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
WORKER = ROOT / "oanda_arima_h1_baseline_grid.py"
REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "arima_baselines"
DEFAULT_STATUS = ROOT / "data" / "oanda_training_manager" / "state" / "arima_multiframe_sweep_v1.json"

SWEEPS = [
    {"label": "M1", "bar": 1, "horizons": "1,5,15,30,60", "train": 20000, "cal": 3000, "test": 3000, "predict": 60},
    {"label": "M5", "bar": 5, "horizons": "5,15,30,60,120", "train": 10000, "cal": 2000, "test": 2000, "predict": 24},
    {"label": "M15", "bar": 15, "horizons": "15,30,60,120,240", "train": 8000, "cal": 1000, "test": 1000, "predict": 12},
    {"label": "M30", "bar": 30, "horizons": "30,60,120,240", "train": 6000, "cal": 800, "test": 800, "predict": 8},
    {"label": "H1", "bar": 60, "horizons": "60,120,240,480,720,1440", "train": 4000, "cal": 500, "test": 500, "predict": 5},
    {"label": "H4", "bar": 240, "horizons": "240,480,720,1440", "train": 2000, "cal": 250, "test": 250, "predict": 3},
]


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp.replace(path)


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def selected_sweeps(value: str) -> list[dict[str, Any]]:
    requested = {
        item.strip().upper() for item in value.split(",") if item.strip()
    }
    available = {str(row["label"]): row for row in SWEEPS}
    missing = sorted(requested - set(available))
    if missing:
        raise ValueError(f"unsupported ARIMA timeframes: {', '.join(missing)}")
    return [row for row in SWEEPS if not requested or row["label"] in requested]


def run_sweep(args: argparse.Namespace, status_path: Path) -> dict[str, Any]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    sweeps = selected_sweeps(args.timeframes)
    status: dict[str, Any] = {
        "schema_version": 1,
        "run_id": f"arima_multiframe_{stamp}",
        "started_utc": now(),
        "status": "running",
        "pairs": args.pairs,
        "windows": args.windows,
        "spec_limit": args.spec_limit,
        "timeframes": [row["label"] for row in sweeps],
        "runs": [],
    }
    atomic_json(status_path, status)
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)

    for sweep in sweeps:
        prefix = f"arima_multiframe_{str(sweep['label']).lower()}_{stamp}"
        summary_path = REPORT_ROOT / f"{prefix}_summary.json"
        stdout_path = REPORT_ROOT / f"{prefix}.out.log"
        stderr_path = REPORT_ROOT / f"{prefix}.err.log"
        command = [
            sys.executable, str(WORKER),
            "--source", "m1",
            "--bar-minutes", str(sweep["bar"]),
            "--pairs", args.pairs,
            "--horizons", str(sweep["horizons"]),
            "--windows", str(args.windows),
            "--train-rows", str(sweep["train"]),
            "--calibration-rows", str(sweep["cal"]),
            "--test-rows", str(sweep["test"]),
            "--predict-every", str(sweep["predict"]),
            "--min-calibration-trades", str(args.min_calibration_trades),
            "--maxiter", str(args.maxiter),
            "--spec-limit", str(args.spec_limit),
            "--output-prefix", prefix,
        ]
        record = {
            "timeframe": sweep["label"],
            "bar_minutes": sweep["bar"],
            "horizons_minutes": [int(value) for value in str(sweep["horizons"]).split(",")],
            "status": "running",
            "started_utc": now(),
            "summary": str(summary_path),
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
        }
        status["current_timeframe"] = sweep["label"]
        status["runs"].append(record)
        atomic_json(status_path, status)
        with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
            completed = subprocess.run(command, cwd=ROOT.parent, stdout=stdout, stderr=stderr, check=False)
        record["ended_utc"] = now()
        record["exit_code"] = completed.returncode
        record["status"] = "complete" if completed.returncode == 0 and summary_path.is_file() else "failed"
        if summary_path.is_file():
            summary = load_json(summary_path)
            record.update({
                "rows": summary.get("rows", 0),
                "successful_rows": summary.get("successful_rows", 0),
                "promotion_ready_count": summary.get("promotion_ready_count", 0),
                "error_count": summary.get("error_count", 0),
                "models": summary.get("declared_specs", args.spec_limit),
                "pairs_completed": len(summary.get("pairs") or []),
            })
        atomic_json(status_path, status)

    status["status"] = "complete" if all(row["status"] == "complete" for row in status["runs"]) else "partial_failure"
    status["current_timeframe"] = None
    status["ended_utc"] = now()
    atomic_json(status_path, status)
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", default="majors")
    parser.add_argument(
        "--timeframes",
        default="",
        help="Optional comma-separated subset of M1,M5,M15,M30,H1,H4",
    )
    parser.add_argument("--windows", type=int, default=2)
    parser.add_argument("--spec-limit", type=int, default=4)
    parser.add_argument("--min-calibration-trades", type=int, default=10)
    parser.add_argument("--maxiter", type=int, default=40)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    parser.add_argument("--repeat-hours", type=float, default=24.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    try:
        selected_sweeps(args.timeframes)
    except ValueError as error:
        parser.error(str(error))

    while True:
        previous = load_json(args.status)
        ended = previous.get("ended_utc")
        recent = False
        if ended and not args.force:
            try:
                age_hours = (datetime.now(timezone.utc) - datetime.fromisoformat(str(ended).replace("Z", "+00:00"))).total_seconds() / 3600
                recent = age_hours < max(1.0, args.repeat_hours)
            except ValueError:
                recent = False
        if not recent:
            result = run_sweep(args, args.status)
            print(json.dumps({"event": "arima_multiframe_complete", "status": result["status"], "run_id": result["run_id"]}), flush=True)
        if args.once:
            return 0
        time.sleep(max(300.0, args.repeat_hours * 3600.0))


if __name__ == "__main__":
    raise SystemExit(main())
