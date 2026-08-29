#!/usr/bin/env python3
"""Merge targeted sparse-pair supplements into a model-gap matrix and re-audit it."""

from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import oanda_model_gap_full_matrix as matrix
    import oanda_shared_timeframe_horizon_panel as panel
except ModuleNotFoundError:
    from trad import oanda_model_gap_full_matrix as matrix
    from trad import oanda_shared_timeframe_horizon_panel as panel


def parse_supplement(value: str) -> tuple[str, Path]:
    target, separator, source = value.partition("=")
    if not separator or not target.strip() or not source.strip():
        raise argparse.ArgumentTypeError("supplement must be TARGET.parquet=SOURCE.parquet")
    target = target.strip()
    if Path(target).name != target:
        raise argparse.ArgumentTypeError("supplement target must be a matrix filename")
    return target, Path(source.strip()).resolve()


def select_missing_rows(
    frame: pd.DataFrame,
    missing_cells: set[tuple[str, str, int]],
) -> pd.DataFrame:
    required = {"instrument", "input_timeframe", "horizon_sec", "side_id"}
    absent = sorted(required - set(frame.columns))
    if absent:
        raise ValueError(f"supplement missing columns: {', '.join(absent)}")
    mask = [
        (str(row.instrument), str(row.input_timeframe), int(row.horizon_sec))
        in missing_cells
        for row in frame[["instrument", "input_timeframe", "horizon_sec"]].itertuples(
            index=False
        )
    ]
    return frame.loc[mask].copy()


def specs_from_state(state: dict[str, Any]) -> list[matrix.RunSpec]:
    fields = {
        "timeframe",
        "source",
        "horizons_sec",
        "sampling",
        "cycles",
        "tail_rows",
        "output",
        "role",
    }
    specs_by_output: dict[str, matrix.RunSpec] = {}
    for row in state.get("runs", []):
        payload = {key: row[key] for key in fields}
        payload["horizons_sec"] = tuple(int(value) for value in payload["horizons_sec"])
        spec = matrix.RunSpec(**payload)
        specs_by_output[spec.output] = spec
    return list(specs_by_output.values())


def latest_failed_runs(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Return failures that have not been superseded by a later retry."""
    latest_by_output: dict[str, dict[str, Any]] = {}
    for row in state.get("runs", []):
        latest_by_output[str(row.get("output", ""))] = row
    return [
        row
        for row in latest_by_output.values()
        if row.get("status") == "failed"
    ]


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def pair_cells(frame: pd.DataFrame) -> set[tuple[str, str, int]]:
    required = {"instrument", "input_timeframe", "horizon_sec"}
    if not required.issubset(frame.columns):
        return set()
    return {
        (str(row.instrument), str(row.input_timeframe), int(row.horizon_sec))
        for row in frame[["instrument", "input_timeframe", "horizon_sec"]]
        .drop_duplicates()
        .itertuples(index=False)
    }


def cell_records(cells: set[tuple[str, str, int]]) -> list[dict[str, Any]]:
    return [
        {
            "instrument": instrument,
            "input_timeframe": timeframe,
            "horizon_sec": horizon,
        }
        for instrument, timeframe, horizon in sorted(cells)
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix-dir", type=Path, required=True)
    parser.add_argument(
        "--supplement",
        type=parse_supplement,
        action="append",
        default=[],
        help="repeat TARGET.parquet=SOURCE.parquet for each repair source",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--accept-documented-unavailable", action="store_true")
    parser.add_argument("--maximum-documented-exit-delay-sec", type=int, default=300)
    parser.add_argument("--minimum-documentation-cycles", type=int, default=500)
    args = parser.parse_args()
    if args.maximum_documented_exit_delay_sec < 0:
        raise SystemExit("maximum documented exit delay must be non-negative")
    if args.minimum_documentation_cycles <= 0:
        raise SystemExit("minimum documentation cycles must be positive")

    matrix_dir = args.matrix_dir.resolve()
    state_path = matrix_dir / "full_matrix_state.json"
    state = read_json(state_path)
    before = state.get("coverage", {})
    missing = {
        (
            str(row["instrument"]),
            str(row["input_timeframe"]),
            int(row["horizon_sec"]),
        )
        for row in before.get("missing_pair_cells", [])
    }
    grouped: dict[str, list[Path]] = defaultdict(list)
    for target, source in args.supplement:
        grouped[target].append(source)

    backup_root = matrix_dir / "_repairs" / "pre_merge_backup"
    repairs: list[dict[str, Any]] = []
    documented_attempts: set[tuple[str, str, int]] = set()
    for target_name, sources in sorted(grouped.items()):
        target = matrix_dir / target_name
        target_manifest = target.with_suffix(target.suffix + ".manifest.json")
        if not target.is_file() or not target_manifest.is_file():
            raise FileNotFoundError(target)
        original_manifest = read_json(target_manifest)
        base = pd.read_parquet(target)
        additions: list[pd.DataFrame] = []
        source_records: list[dict[str, Any]] = []
        for source in sources:
            if not source.is_file():
                raise FileNotFoundError(source)
            source_manifest_path = source.with_suffix(source.suffix + ".manifest.json")
            source_manifest = read_json(source_manifest_path)
            source_frame = pd.read_parquet(source)
            selected = select_missing_rows(source_frame, missing)
            if not selected.empty:
                additions.append(selected)
            source_provenance = source_manifest.get("provenance", {})
            max_exit_delay = int(source_provenance.get("max_exit_delay_sec", 0))
            requested_instruments = [
                str(value)
                for value in source_provenance.get("requested_instruments", [])
            ]
            requested_horizons = [
                int(value)
                for value in source_provenance.get("requested_horizons_sec", [])
            ]
            requested_timeframes = [
                str(value)
                for value in source_manifest.get("panel", {}).get("timeframes", [])
            ]
            attempted = {
                (instrument, timeframe, horizon)
                for instrument in requested_instruments
                for timeframe in requested_timeframes
                for horizon in requested_horizons
            }
            observed_source_cells = pair_cells(source_frame)
            documentation_eligible = bool(
                args.accept_documented_unavailable
                and source_provenance.get("sampling_mode") == "uniform"
                and int(source_provenance.get("max_cycles", 0))
                >= args.minimum_documentation_cycles
                and max_exit_delay <= args.maximum_documented_exit_delay_sec
            )
            if documentation_eligible:
                documented_attempts.update(attempted - observed_source_cells)
            source_records.append(
                {
                    "path": str(source),
                    "sha256": panel.sha256_file(source),
                    "rows_selected": int(len(selected)),
                    "max_exit_delay_sec": max_exit_delay,
                    "sampling_mode": source_provenance.get("sampling_mode"),
                    "max_cycles": int(source_provenance.get("max_cycles", 0)),
                    "requested_pair_cells": len(attempted),
                    "observed_requested_pair_cells": len(
                        attempted & observed_source_cells
                    ),
                    "documentation_eligible": documentation_eligible,
                }
            )
        if not additions:
            repairs.append(
                {"target": target_name, "rows_added": 0, "sources": source_records}
            )
            continue

        backup_root.mkdir(parents=True, exist_ok=True)
        backup = backup_root / target.name
        backup_manifest = backup.with_suffix(backup.suffix + ".manifest.json")
        if not backup.exists():
            shutil.copy2(target, backup)
        if not backup_manifest.exists():
            shutil.copy2(target_manifest, backup_manifest)

        merged = pd.concat([base, *additions], ignore_index=True, sort=False)
        before_rows = len(merged)
        merged = merged.drop_duplicates("side_id", keep="first")
        added_rows = int(len(merged) - len(base))
        duplicate_rows = int(before_rows - len(merged))
        merged = merged.sort_values(
            ["prediction_time_utc", "instrument", "horizon_sec", "direction"]
        ).reset_index(drop=True)
        provenance = dict(original_manifest.get("provenance", {}))
        provenance["coverage_repair"] = {
            "policy": "targeted_missing_pair_cells_only",
            "sources": source_records,
            "pre_repair_sha256": original_manifest.get("panel", {}).get("sha256"),
            "backup": str(backup),
            "rows_added": added_rows,
            "duplicate_rows_ignored": duplicate_rows,
        }
        updated_manifest = panel.write_panel_atomic(merged, target, provenance)
        repairs.append(
            {
                "target": target_name,
                "rows_added": added_rows,
                "duplicate_rows_ignored": duplicate_rows,
                "new_sha256": updated_manifest["panel"]["sha256"],
                "sources": source_records,
            }
        )

    specs = specs_from_state(state)
    after = matrix.audit_coverage(
        matrix_dir,
        specs,
        [str(value) for value in state["instruments"]],
        [str(value) for value in state["timeframes"]],
        [int(value) for value in state["horizons_sec"]],
    )
    remaining = {
        (
            str(row["instrument"]),
            str(row["input_timeframe"]),
            int(row["horizon_sec"]),
        )
        for row in after.get("missing_pair_cells", [])
    }
    documented_unavailable = remaining & documented_attempts
    accounted = int(after["observed_pair_cells"]) + len(documented_unavailable)
    after["documented_unavailable_pair_cells"] = cell_records(
        documented_unavailable
    )
    after["documented_unavailable_count"] = len(documented_unavailable)
    after["unaccounted_pair_cells"] = cell_records(
        remaining - documented_unavailable
    )
    after["unaccounted_pair_cell_count"] = len(
        remaining - documented_unavailable
    )
    after["observed_or_documented_pair_cells"] = accounted
    after["accounted_coverage_ratio"] = float(
        accounted / int(after["expected_pair_cells"])
    )
    after["contract_complete"] = bool(
        accounted == int(after["expected_pair_cells"])
    )
    failed = latest_failed_runs(state)
    state["coverage"] = after
    state["status"] = matrix.completion_status(failed, after)
    state["updated_utc"] = matrix.utc_iso()
    state["coverage_repairs"] = [*state.get("coverage_repairs", []), *repairs]
    matrix.write_json_atomic(state_path, state)
    matrix.write_json_atomic(
        matrix.DEFAULT_OUTPUT_ROOT / "full_matrix_latest.json",
        {
            "schema_version": 1,
            "generated_utc": matrix.utc_iso(),
            "state": str(state_path),
            "status": state["status"],
            "coverage": after,
            "account_wired": False,
        },
    )
    report = {
        "schema_version": 1,
        "generated_utc": matrix.utc_iso(),
        "matrix_dir": str(matrix_dir),
        "status": state["status"],
        "before": {
            "observed_pair_cells": int(before.get("observed_pair_cells", 0)),
            "expected_pair_cells": int(before.get("expected_pair_cells", 0)),
            "coverage_ratio": float(before.get("coverage_ratio", 0.0)),
        },
        "after": {
            "observed_pair_cells": int(after["observed_pair_cells"]),
            "expected_pair_cells": int(after["expected_pair_cells"]),
            "coverage_ratio": float(after["coverage_ratio"]),
            "fully_covered": bool(after["fully_covered"]),
            "contract_complete": bool(after["contract_complete"]),
            "documented_unavailable_count": int(
                after["documented_unavailable_count"]
            ),
            "unaccounted_pair_cell_count": int(
                after["unaccounted_pair_cell_count"]
            ),
            "missing_pair_cells": after["missing_pair_cells"],
            "documented_unavailable_pair_cells": after[
                "documented_unavailable_pair_cells"
            ],
            "unaccounted_pair_cells": after["unaccounted_pair_cells"],
        },
        "repairs": repairs,
        "account_wired": False,
    }
    output = (args.output or matrix_dir / "coverage_repair_report.json").resolve()
    matrix.write_json_atomic(output, report)
    print(json.dumps({**report["after"], "status": state["status"], "report": str(output)}, indent=2))
    complete = bool(after["fully_covered"] or after["contract_complete"])
    return 0 if not args.require_complete or complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
