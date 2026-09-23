#!/usr/bin/env python3
"""Build and audit the complete FX timeframe-by-horizon research panel."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import oanda_shared_timeframe_horizon_panel as panel
    from oanda_s5_timeframe_strategy_replay import parse_horizons, parse_instruments, parse_timeframe
    from oanda_timeframe_horizon_sweep import discover_instruments, parse_labels
except ModuleNotFoundError:
    from trad import oanda_shared_timeframe_horizon_panel as panel
    from trad.oanda_s5_timeframe_strategy_replay import parse_horizons, parse_instruments, parse_timeframe
    from trad.oanda_timeframe_horizon_sweep import discover_instruments, parse_labels


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
DEFAULT_OUTPUT_ROOT = DATA_ROOT / "training_sets" / "model_gap_full_matrix"


@dataclass(frozen=True)
class RunSpec:
    timeframe: str
    source: str
    horizons_sec: tuple[int, ...]
    sampling: str
    cycles: int
    tail_rows: int
    output: str
    role: str


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def build_specs(
    timeframes: list[str],
    horizons: list[int],
    cycles: int,
    supplement_cycles: int,
    include_subminute_supplement: bool,
) -> list[RunSpec]:
    specs: list[RunSpec] = []
    for timeframe in timeframes:
        seconds = parse_timeframe(timeframe)
        source = "s5" if seconds < 60 else "m1"
        primary_horizons = tuple(
            value for value in horizons if source == "s5" or value >= 60
        )
        specs.append(
            RunSpec(
                timeframe=timeframe,
                source=source,
                horizons_sec=primary_horizons,
                sampling="uniform",
                cycles=cycles,
                tail_rows=0,
                output=f"panel_{timeframe.lower()}_{source}_primary.parquet",
                role="full_span_primary",
            )
        )
        if include_subminute_supplement and seconds >= 60 and 30 in horizons:
            specs.append(
                RunSpec(
                    timeframe=timeframe,
                    source="hybrid",
                    horizons_sec=(30,),
                    sampling="latest",
                    cycles=supplement_cycles,
                    tail_rows=(
                        supplement_cycles * max(300, seconds) // seconds + 400
                    ),
                    output=f"panel_{timeframe.lower()}_hybrid_30s.parquet",
                    role="deep_features_observed_s5_execution",
                )
            )
    return specs


def reusable_output(path: Path, spec: RunSpec) -> bool:
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    if not path.is_file() or not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return False
    summary = manifest.get("panel", {})
    return (
        summary.get("path") == str(path.resolve())
        and summary.get("timeframes") == [spec.timeframe]
        and summary.get("horizons_sec") == list(spec.horizons_sec)
        and int(summary.get("rows", 0)) > 0
    )


def run_panel(
    spec: RunSpec,
    *,
    output_dir: Path,
    instruments: list[str],
    m1_dir: Path,
    m1_cache_dir: Path,
    s5_dir: Path,
    progress_every: int,
    resume: bool,
) -> dict[str, Any]:
    output = output_dir / spec.output
    if resume and reusable_output(output, spec):
        return {**asdict(spec), "status": "reused", "returncode": 0}
    command = [
        sys.executable,
        str(ROOT / "oanda_shared_timeframe_horizon_panel.py"),
        "--timeframe",
        spec.timeframe,
        "--source",
        spec.source,
        "--horizons-sec",
        ",".join(str(value) for value in spec.horizons_sec),
        "--instruments",
        ",".join(instruments),
        "--m1-dir",
        str(m1_dir),
        "--m1-cache-dir",
        str(m1_cache_dir),
        "--s5-dir",
        str(s5_dir),
        "--max-cycles",
        str(spec.cycles),
        "--sampling",
        spec.sampling,
        "--tail-rows",
        str(spec.tail_rows),
        "--progress-every",
        str(progress_every),
        "--output",
        str(output),
    ]
    started = utc_iso()
    completed = subprocess.run(command, cwd=ROOT, check=False)
    status = "completed" if completed.returncode == 0 and reusable_output(output, spec) else "failed"
    return {
        **asdict(spec),
        "status": status,
        "returncode": completed.returncode,
        "started_utc": started,
        "finished_utc": utc_iso(),
        "command": command,
    }


def audit_coverage(
    output_dir: Path,
    specs: list[RunSpec],
    instruments: list[str],
    timeframes: list[str],
    horizons: list[int],
) -> dict[str, Any]:
    observed: dict[tuple[str, str, int], int] = {}
    panels: list[dict[str, Any]] = []
    for spec in specs:
        path = output_dir / spec.output
        if not reusable_output(path, spec):
            continue
        frame = pd.read_parquet(
            path,
            columns=["instrument", "input_timeframe", "horizon_sec"],
        )
        counts = frame.groupby(
            ["instrument", "input_timeframe", "horizon_sec"], observed=True
        ).size()
        for (instrument, timeframe, horizon), rows in counts.items():
            key = (str(instrument), str(timeframe), int(horizon))
            observed[key] = observed.get(key, 0) + int(rows // 2)
        manifest_path = path.with_suffix(path.suffix + ".manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        panels.append(manifest["panel"])

    expected = {
        (instrument, timeframe, int(horizon))
        for instrument in instruments
        for timeframe in timeframes
        for horizon in horizons
    }
    missing = sorted(expected - set(observed))
    cell_rows: list[dict[str, Any]] = []
    for timeframe in timeframes:
        for horizon in horizons:
            values = [
                observed[(instrument, timeframe, int(horizon))]
                for instrument in instruments
                if (instrument, timeframe, int(horizon)) in observed
            ]
            missing_pairs = [
                instrument
                for instrument in instruments
                if (instrument, timeframe, int(horizon)) not in observed
            ]
            cell_rows.append(
                {
                    "input_timeframe": timeframe,
                    "horizon_sec": int(horizon),
                    "pairs_covered": len(values),
                    "pairs_expected": len(instruments),
                    "events": int(sum(values)),
                    "min_events_per_pair": int(min(values)) if values else 0,
                    "median_events_per_pair": float(pd.Series(values).median()) if values else 0.0,
                    "max_events_per_pair": int(max(values)) if values else 0,
                    "missing_pairs": missing_pairs,
                }
            )
    pd.DataFrame(
        [{key: value for key, value in row.items() if key != "missing_pairs"} for row in cell_rows]
    ).to_csv(output_dir / "coverage_cells.csv", index=False)
    return {
        "expected_timeframe_horizon_cells": len(timeframes) * len(horizons),
        "observed_timeframe_horizon_cells": sum(row["pairs_covered"] > 0 for row in cell_rows),
        "expected_pair_cells": len(expected),
        "observed_pair_cells": len(set(observed) & expected),
        "coverage_ratio": len(set(observed) & expected) / len(expected) if expected else 0.0,
        "fully_covered": not missing,
        "total_events": int(sum(observed.values())),
        "missing_pair_cells": [
            {"instrument": instrument, "input_timeframe": timeframe, "horizon_sec": horizon}
            for instrument, timeframe, horizon in missing
        ],
        "cells": cell_rows,
        "panels": panels,
    }


def completion_status(failed_runs: list[dict[str, Any]], coverage: dict[str, Any]) -> str:
    if failed_runs:
        return "complete_with_errors"
    if coverage.get("contract_complete", False) and not coverage.get(
        "fully_covered", False
    ):
        return "complete_with_documented_unavailable"
    if not coverage.get("fully_covered", False):
        return "incomplete_coverage"
    return "complete"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--timeframes", type=parse_labels, default=list(panel.CANONICAL_TIMEFRAMES))
    parser.add_argument("--horizons-sec", type=parse_horizons, default=list(panel.CANONICAL_HORIZONS_SEC))
    parser.add_argument("--instruments", default="")
    parser.add_argument("--m1-dir", type=Path, default=panel.DEFAULT_M1_DIR)
    parser.add_argument("--m1-cache-dir", type=Path, default=panel.DEFAULT_M1_CACHE_DIR)
    parser.add_argument("--s5-dir", type=Path, default=panel.DEFAULT_S5_DIR)
    parser.add_argument("--cycles", type=int, default=500)
    parser.add_argument("--supplement-cycles", type=int, default=500)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="number of isolated panel subprocesses to build concurrently",
    )
    parser.add_argument(
        "--s5-workers",
        type=int,
        default=1,
        help="maximum workers allowed to load S5 execution history",
    )
    parser.add_argument("--no-subminute-supplement", action="store_true")
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.cycles < 1 or args.supplement_cycles < 1:
        raise SystemExit("cycle counts must be positive")
    if args.workers < 1:
        raise SystemExit("worker count must be positive")
    if args.s5_workers < 1:
        raise SystemExit("S5 worker count must be positive")
    if args.workers > 1 and args.stop_on_error:
        raise SystemExit("--stop-on-error requires --workers 1")
    timeframes = list(dict.fromkeys(args.timeframes))
    horizons = sorted(set(args.horizons_sec))
    instruments = (
        parse_instruments(args.instruments)
        if args.instruments
        else discover_instruments(args.m1_dir, args.s5_dir)
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = (args.output_dir or DEFAULT_OUTPUT_ROOT / stamp).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / "full_matrix_state.json"
    specs = build_specs(
        timeframes,
        horizons,
        args.cycles,
        args.supplement_cycles,
        not args.no_subminute_supplement,
    )
    state: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "started_utc": utc_iso(),
        "updated_utc": utc_iso(),
        "execution_policy": "research_shadow_only_no_account_wiring",
        "account_wired": False,
        "instruments": instruments,
        "timeframes": timeframes,
        "horizons_sec": horizons,
        "workers": args.workers,
        "s5_workers": args.s5_workers,
        "runs": [],
    }
    write_json_atomic(state_path, state)

    def execute(index: int, spec: RunSpec) -> tuple[int, dict[str, Any]]:
        print(
            f"[full-matrix] {index}/{len(specs)} {spec.timeframe} {spec.source} "
            f"horizons={','.join(map(str, spec.horizons_sec))}",
            flush=True,
        )
        try:
            record = run_panel(
                spec,
                output_dir=output_dir,
                instruments=instruments,
                m1_dir=args.m1_dir,
                m1_cache_dir=args.m1_cache_dir,
                s5_dir=args.s5_dir,
                progress_every=args.progress_every,
                resume=not args.no_resume,
            )
        except Exception as error:
            record = {
                **asdict(spec),
                "status": "failed",
                "returncode": -1,
                "finished_utc": utc_iso(),
                "error_type": type(error).__name__,
                "error": str(error),
            }
        return index, record

    indexed_specs = list(enumerate(specs, start=1))
    records: dict[int, dict[str, Any]] = {}
    if args.workers == 1:
        for index, spec in indexed_specs:
            completed_index, record = execute(index, spec)
            records[completed_index] = record
            state["runs"] = [records[key] for key in sorted(records)]
            state["updated_utc"] = utc_iso()
            write_json_atomic(state_path, state)
            if record["status"] == "failed" and args.stop_on_error:
                break
    else:
        s5_specs = [row for row in indexed_specs if row[1].source in {"s5", "hybrid"}]
        m1_specs = [row for row in indexed_specs if row[1].source == "m1"]
        if s5_specs and m1_specs and args.s5_workers >= args.workers:
            raise SystemExit("--s5-workers must be lower than --workers for a mixed build")
        s5_workers = min(args.s5_workers, args.workers)
        m1_workers = args.workers - s5_workers if s5_specs else args.workers
        if not m1_specs:
            s5_workers = args.workers
        futures: dict[Any, int] = {}
        with ExitStack() as stack:
            if s5_specs:
                s5_executor = stack.enter_context(
                    ThreadPoolExecutor(max_workers=s5_workers)
                )
                futures.update(
                    {
                        s5_executor.submit(execute, index, spec): index
                        for index, spec in s5_specs
                    }
                )
            if m1_specs:
                m1_executor = stack.enter_context(
                    ThreadPoolExecutor(max_workers=m1_workers)
                )
                futures.update(
                    {
                        m1_executor.submit(execute, index, spec): index
                        for index, spec in m1_specs
                    }
                )
            for future in as_completed(futures):
                completed_index, record = future.result()
                records[completed_index] = record
                state["runs"] = [records[key] for key in sorted(records)]
                state["updated_utc"] = utc_iso()
                write_json_atomic(state_path, state)
                print(
                    f"[full-matrix] finished {completed_index}/{len(specs)} "
                    f"status={record['status']}",
                    flush=True,
                )
    coverage = audit_coverage(output_dir, specs, instruments, timeframes, horizons)
    failed = [row for row in state["runs"] if row["status"] == "failed"]
    state.update(
        {
            "status": completion_status(failed, coverage),
            "finished_utc": utc_iso(),
            "updated_utc": utc_iso(),
            "coverage": coverage,
        }
    )
    write_json_atomic(state_path, state)
    latest = DEFAULT_OUTPUT_ROOT / "full_matrix_latest.json"
    write_json_atomic(
        latest,
        {
            "schema_version": 1,
            "generated_utc": utc_iso(),
            "state": str(state_path),
            "status": state["status"],
            "coverage": coverage,
            "account_wired": False,
        },
    )
    print(
        json.dumps(
            {
                "state": str(state_path),
                "status": state["status"],
                "runs": len(state["runs"]),
                "failed_runs": len(failed),
                "coverage_ratio": coverage["coverage_ratio"],
                "observed_pair_cells": coverage["observed_pair_cells"],
                "expected_pair_cells": coverage["expected_pair_cells"],
            },
            indent=2,
        )
    )
    return 0 if state["status"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
