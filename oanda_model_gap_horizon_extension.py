#!/usr/bin/env python3
"""Extend an existing canonical model-gap panel with newly required horizons."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

try:
    import oanda_model_gap_full_matrix as matrix
    import oanda_shared_timeframe_horizon_panel as panel
    from oanda_model_gap_coverage_repair import latest_failed_runs, specs_from_state
    from oanda_s5_timeframe_strategy_replay import parse_horizons
    from oanda_timeframe_horizon_sweep import parse_labels
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad import oanda_model_gap_full_matrix as matrix
    from trad import oanda_shared_timeframe_horizon_panel as panel
    from trad.oanda_model_gap_coverage_repair import latest_failed_runs, specs_from_state
    from trad.oanda_s5_timeframe_strategy_replay import parse_horizons
    from trad.oanda_timeframe_horizon_sweep import parse_labels


DEFAULT_MATRIX_DIR = (
    matrix.DEFAULT_OUTPUT_ROOT / "full_68x13x12_recovered_20260719"
)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def build_extension_specs(
    timeframes: list[str],
    horizons: list[int],
    cycles: int,
) -> list[matrix.RunSpec]:
    suffix = f"{min(horizons)}_{max(horizons)}"
    return [
        matrix.RunSpec(
            timeframe=timeframe,
            source="s5" if panel.parse_timeframe(timeframe) < 60 else "m1",
            horizons_sec=tuple(horizons),
            sampling="uniform",
            cycles=cycles,
            tail_rows=0,
            output=(
                f"panel_{timeframe.lower()}_"
                f"{'s5' if panel.parse_timeframe(timeframe) < 60 else 'm1'}_"
                f"horizon_extension_{suffix}.parquet"
            ),
            role="canonical_horizon_extension",
        )
        for timeframe in timeframes
    ]


def missing_extension_specs(
    state: dict[str, Any],
    timeframes: list[str],
    target_horizons: list[int],
    cycles: int,
) -> list[matrix.RunSpec]:
    observed = {
        (str(run.get("timeframe")), int(horizon))
        for run in state.get("runs") or []
        if run.get("status") in {"completed", "reused"}
        for horizon in run.get("horizons_sec") or []
    }
    specs: list[matrix.RunSpec] = []
    for timeframe in timeframes:
        missing = [
            horizon
            for horizon in target_horizons
            if (timeframe, horizon) not in observed
        ]
        if missing:
            specs.extend(build_extension_specs([timeframe], missing, cycles))
    return specs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-dir", type=Path, default=DEFAULT_MATRIX_DIR)
    parser.add_argument("--timeframes", type=parse_labels, default=[])
    parser.add_argument(
        "--horizons-sec",
        type=parse_horizons,
        default=list(panel.CANONICAL_HORIZONS_SEC),
    )
    parser.add_argument("--cycles", type=int, default=500)
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument("--m1-dir", type=Path, default=panel.DEFAULT_M1_DIR)
    parser.add_argument("--m1-cache-dir", type=Path, default=panel.DEFAULT_M1_CACHE_DIR)
    parser.add_argument("--s5-dir", type=Path, default=panel.DEFAULT_S5_DIR)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    matrix_dir = args.matrix_dir.resolve()
    state_path = matrix_dir / "full_matrix_state.json"
    lock_path = matrix_dir / "horizon_extension.lock"
    if args.cycles < 1:
        raise SystemExit("cycles must be positive")
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as error:
        raise SystemExit(f"horizon extension lock already exists: {lock_path}") from error
    os.write(lock_fd, f"{os.getpid()}\n".encode("ascii"))
    os.close(lock_fd)
    try:
        state = read_json(state_path)
        all_timeframes = [str(value) for value in state.get("timeframes") or []]
        timeframes = [
            value for value in all_timeframes if not args.timeframes or value in args.timeframes
        ]
        instruments = [str(value) for value in state.get("instruments") or []]
        target_horizons = sorted({int(value) for value in args.horizons_sec})
        extension_specs = missing_extension_specs(
            state, timeframes, target_horizons, args.cycles
        )
        missing_horizons = sorted(
            {horizon for spec in extension_specs for horizon in spec.horizons_sec}
        )
        if not timeframes or not instruments:
            raise SystemExit("matrix state does not contain timeframes and instruments")
        state["status"] = "extending_horizons" if extension_specs else "auditing"
        state["horizons_sec"] = target_horizons
        state["updated_utc"] = matrix.utc_iso()
        matrix.write_json_atomic(state_path, state)

        failed: list[dict[str, Any]] = []
        for index, spec in enumerate(extension_specs, start=1):
            print(
                f"[horizon-extension] {index}/{len(extension_specs)} "
                f"{spec.timeframe} horizons={','.join(map(str, spec.horizons_sec))}",
                flush=True,
            )
            record = matrix.run_panel(
                spec,
                output_dir=matrix_dir,
                instruments=instruments,
                m1_dir=args.m1_dir.resolve(),
                m1_cache_dir=args.m1_cache_dir.resolve(),
                s5_dir=args.s5_dir.resolve(),
                progress_every=args.progress_every,
                resume=not args.no_resume,
            )
            state.setdefault("runs", []).append(record)
            state["updated_utc"] = matrix.utc_iso()
            matrix.write_json_atomic(state_path, state)
            if record["status"] == "failed":
                failed.append(record)
                if args.stop_on_error:
                    break

        all_specs = specs_from_state(state)
        coverage = matrix.audit_coverage(
            matrix_dir,
            all_specs,
            instruments,
            all_timeframes,
            target_horizons,
        )
        all_failed = latest_failed_runs(state)
        state.update(
            {
                "status": matrix.completion_status(all_failed, coverage),
                "finished_utc": matrix.utc_iso(),
                "updated_utc": matrix.utc_iso(),
                "coverage": coverage,
                "canonical_horizon_extension": {
                    "target_horizons_sec": target_horizons,
                    "new_horizons_sec": missing_horizons,
                    "runs": len(extension_specs),
                    "failed_runs": len(failed),
                },
            }
        )
        matrix.write_json_atomic(state_path, state)
        matrix.write_json_atomic(
            matrix.DEFAULT_OUTPUT_ROOT / "full_matrix_latest.json",
            {
                "schema_version": 1,
                "generated_utc": matrix.utc_iso(),
                "state": str(state_path),
                "status": state["status"],
                "coverage": coverage,
                "account_wired": False,
            },
        )
        print(
            json.dumps(
                {
                    "status": state["status"],
                    "new_horizons_sec": missing_horizons,
                    "expected_timeframe_horizon_cells": coverage[
                        "expected_timeframe_horizon_cells"
                    ],
                    "observed_timeframe_horizon_cells": coverage[
                        "observed_timeframe_horizon_cells"
                    ],
                    "coverage_ratio": coverage["coverage_ratio"],
                },
                indent=2,
            ),
            flush=True,
        )
        return 0 if state["status"] == "complete" else 1
    finally:
        lock_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
