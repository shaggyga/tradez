#!/usr/bin/env python3
"""Run the strategy-family replay across every supported input/horizon cell.

The controller keeps one replay in memory at a time, covers every available
instrument, and writes an atomic progress record for the dashboard and audits.
Deep M1 history supplies M1-and-slower outcomes. Recent observed S5 data adds
the otherwise unavailable 30-second outcome for input frames that have enough
S5 history.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_s5_timeframe_strategy_replay import (
        parse_horizons,
        parse_timeframe,
        seconds_label,
    )
except ModuleNotFoundError:
    from trad.oanda_s5_timeframe_strategy_replay import (
        parse_horizons,
        parse_timeframe,
        seconds_label,
    )


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_M1_DIR = DATA_ROOT / "candles"
DEFAULT_M1_CACHE_DIR = DATA_ROOT / "candles_m1_parquet"
DEFAULT_S5_DIR = DATA_ROOT / "candles_s5_bam"
DEFAULT_REPORT_DIR = DATA_ROOT / "reports"
DEFAULT_STATE = DATA_ROOT / "state" / "timeframe_horizon_sweep_v1.json"
DEFAULT_LOCK = DATA_ROOT / "state" / "timeframe_horizon_sweep_v1.lock"
DEFAULT_TIMEFRAMES = (
    "H3",
    "H4",
    "H2",
    "H1",
    "M30",
    "M15",
    "M10",
    "S10",
    "S15",
    "S5",
    "S30",
    "M1",
    "M5",
)
DEFAULT_HORIZONS = (
    30,
    60,
    120,
    180,
    300,
    600,
    900,
    1800,
    3600,
    7200,
    10800,
    14400,
    21600,
    28800,
    43200,
    86400,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_labels(value: str) -> list[str]:
    labels: list[str] = []
    for item in value.split(","):
        if not item.strip():
            continue
        label = seconds_label(parse_timeframe(item.strip()))
        if label not in labels:
            labels.append(label)
    if not labels:
        raise argparse.ArgumentTypeError("at least one timeframe is required")
    return labels


def discover_instruments(m1_dir: Path, s5_dir: Path) -> list[str]:
    m1 = {path.name.removesuffix("_M1.csv") for path in m1_dir.glob("*_M1.csv")}
    s5 = {path.name.removesuffix("_S5.parquet") for path in s5_dir.glob("*_S5.parquet")}
    instruments = sorted(m1 & s5)
    if not instruments:
        raise RuntimeError("no instruments have both deep M1 and observed S5 history")
    return instruments


@dataclass(frozen=True)
class SweepRun:
    timeframe: str
    timeframe_seconds: int
    source: str
    horizons: tuple[int, ...]
    max_cycles: int
    step_seconds: int
    tail_rows: int
    supplemental: bool = False


def cycles_for(timeframe_seconds: int, args: argparse.Namespace) -> int:
    if timeframe_seconds >= 10800:
        return args.long_cycles
    if timeframe_seconds >= 3600:
        return args.hourly_cycles
    return args.cycles


def required_tail_rows(
    timeframe_seconds: int,
    step_seconds: int,
    cycles: int,
    horizons: tuple[int, ...],
    warmup_rows: int,
) -> int:
    decision_span = math.ceil(cycles * step_seconds / timeframe_seconds)
    outcome_span = math.ceil(max(horizons) / timeframe_seconds)
    return decision_span + outcome_span + warmup_rows


def build_runs(timeframes: list[str], args: argparse.Namespace) -> list[SweepRun]:
    runs: list[SweepRun] = []
    sampling = str(getattr(args, "sampling", "latest"))
    for label in timeframes:
        seconds = parse_timeframe(label)
        cycles = cycles_for(seconds, args)
        step = max(seconds, args.minimum_step_seconds)
        source = "s5" if seconds < 60 else "m1"
        horizons = tuple(
            horizon
            for horizon in args.horizons
            if source == "s5" or horizon >= 60
        )
        runs.append(
            SweepRun(
                timeframe=label,
                timeframe_seconds=seconds,
                source=source,
                horizons=horizons,
                max_cycles=cycles,
                step_seconds=step,
                tail_rows=(
                    0
                    if sampling == "uniform"
                    else required_tail_rows(
                        seconds, step, cycles, horizons, args.warmup_rows
                    )
                ),
            )
        )
        if 60 <= seconds <= args.s5_supplement_max_timeframe_sec and 30 in args.horizons:
            runs.append(
                SweepRun(
                    timeframe=label,
                    timeframe_seconds=seconds,
                    source="s5",
                    horizons=(30,),
                    max_cycles=cycles,
                    step_seconds=step,
                    tail_rows=(
                        0
                        if sampling == "uniform"
                        else required_tail_rows(
                            seconds, step, cycles, (30,), args.warmup_rows
                        )
                    ),
                    supplemental=True,
                )
            )
    return runs


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def report_path(report_dir: Path, run: SweepRun, instrument_count: int, stamp: str) -> Path:
    suffix = "s5_30s_supplement" if run.supplemental else (
        "s5_observed" if run.source == "s5" else "m1_deep"
    )
    return report_dir / (
        f"strategy_lab_timeframe_{run.timeframe.lower()}_all{instrument_count}_"
        f"{suffix}_{stamp}.json"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeframes", type=parse_labels, default=list(DEFAULT_TIMEFRAMES))
    parser.add_argument(
        "--horizons-sec", dest="horizons", type=parse_horizons,
        default=list(DEFAULT_HORIZONS),
    )
    parser.add_argument("--instruments", default="")
    parser.add_argument("--m1-dir", type=Path, default=DEFAULT_M1_DIR)
    parser.add_argument("--m1-cache-dir", type=Path, default=DEFAULT_M1_CACHE_DIR)
    parser.add_argument("--s5-dir", type=Path, default=DEFAULT_S5_DIR)
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--cycles", type=int, default=500)
    parser.add_argument("--hourly-cycles", type=int, default=400)
    parser.add_argument("--long-cycles", type=int, default=300)
    parser.add_argument(
        "--sampling",
        choices=("uniform", "latest"),
        default="uniform",
        help="uniform samples across all available history; latest is a tail-only diagnostic",
    )
    parser.add_argument("--minimum-step-seconds", type=int, default=300)
    parser.add_argument("--warmup-rows", type=int, default=380)
    parser.add_argument("--s5-supplement-max-timeframe-sec", type=int, default=14400)
    parser.add_argument("--profiles", default="strict,balanced,fast,loose")
    parser.add_argument("--families", default="")
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--stop-on-error", action="store_true")
    args = parser.parse_args()

    if min(args.cycles, args.hourly_cycles, args.long_cycles) <= 0:
        raise SystemExit("cycle counts must be positive")
    if args.minimum_step_seconds <= 0 or args.warmup_rows < 360:
        raise SystemExit("minimum step must be positive and warmup rows must be at least 360")

    instruments = (
        sorted({item.strip().upper().replace("/", "_") for item in args.instruments.split(",") if item.strip()})
        if args.instruments else discover_instruments(args.m1_dir, args.s5_dir)
    )
    runs = build_runs(args.timeframes, args)
    planned_cells = sorted(
        {
            (run.timeframe, int(horizon))
            for run in runs
            for horizon in run.horizons
        },
        key=lambda item: (parse_timeframe(item[0]), item[1]),
    )
    args.lock.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock_fd = os.open(args.lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise SystemExit(f"sweep lock already exists: {args.lock}") from error
    os.write(lock_fd, f"{os.getpid()}\n".encode("ascii"))
    os.close(lock_fd)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    state: dict[str, Any] = {
        "schema_version": 2,
        "status": "running",
        "pid": os.getpid(),
        "started_utc": utc_now(),
        "updated_utc": utc_now(),
        "instrument_count": len(instruments),
        "instruments": instruments,
        "requested_timeframes": args.timeframes,
        "requested_horizons_sec": args.horizons,
        "sampling_mode": args.sampling,
        "history_coverage": (
            "full_available_span" if args.sampling == "uniform" else "latest_tail"
        ),
        "run_count": len(runs),
        "planned_matrix_cell_count": len(planned_cells),
        "expected_matrix_cell_count": len(args.timeframes) * len(args.horizons),
        "planned_matrix_cells": [
            {"timeframe": timeframe, "horizon_sec": horizon}
            for timeframe, horizon in planned_cells
        ],
        "completed": [],
        "errors": [],
    }
    write_json_atomic(args.state, state)

    try:
        for index, run in enumerate(runs, start=1):
            output = report_path(args.report_dir, run, len(instruments), stamp)
            state["current"] = {"index": index, **asdict(run), "output": str(output)}
            state["updated_utc"] = utc_now()
            write_json_atomic(args.state, state)
            print(
                f"[sweep] {index}/{len(runs)} {run.timeframe} {run.source} "
                f"horizons={','.join(map(str, run.horizons))} pairs={len(instruments)}",
                flush=True,
            )
            command = [
                sys.executable,
                str(ROOT / "oanda_s5_timeframe_strategy_replay.py"),
                "--source", run.source,
                "--s5-dir", str(args.s5_dir),
                "--m1-dir", str(args.m1_dir),
                "--m1-cache-dir", str(args.m1_cache_dir),
                "--instruments", ",".join(instruments),
                "--timeframe", run.timeframe,
                "--profiles", args.profiles,
                "--horizons-sec", ",".join(map(str, run.horizons)),
                "--step-seconds", str(run.step_seconds),
                "--max-cycles", str(run.max_cycles),
                "--tail-rows", str(run.tail_rows),
                "--sampling", args.sampling,
                "--progress-every", str(args.progress_every),
                "--output", str(output),
            ]
            if args.families:
                command.extend(["--families", args.families])
            result = subprocess.run(command, cwd=ROOT, check=False)
            record = {"index": index, **asdict(run), "output": str(output), "returncode": result.returncode}
            if result.returncode == 0 and output.is_file():
                state["completed"].append(record)
            else:
                state["errors"].append(record)
                if args.stop_on_error:
                    break
            state["updated_utc"] = utc_now()
            write_json_atomic(args.state, state)
    finally:
        args.lock.unlink(missing_ok=True)

    state.pop("current", None)
    state["finished_utc"] = utc_now()
    state["updated_utc"] = state["finished_utc"]
    state["status"] = "complete" if not state["errors"] else "complete_with_errors"
    write_json_atomic(args.state, state)
    return 0 if not state["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
