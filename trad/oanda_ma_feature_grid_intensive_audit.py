#!/usr/bin/env python3
"""Compare MA-grid experiments without promoting incomparable evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT_ROOT = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_OUTPUT_DIR = DEFAULT_REPORT_ROOT / "ma_feature_grid_intensive_audit"
REPORT_NAME = "ma_feature_grid_intensive_audit_latest.json"
CSV_NAME = "ma_feature_grid_intensive_cells_latest.csv"
SUMMARY_NAME = "MA_FEATURE_GRID_INTENSIVE_AUDIT_LATEST.md"


def finite(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def metric_rows(run_name: str, report: dict[str, Any]) -> list[dict[str, Any]]:
    fit = report.get("fit") or {}
    rows: list[dict[str, Any]] = []
    for timeframe_report in report.get("timeframe_reports") or ():
        if timeframe_report.get("status") != "fitted":
            continue
        timeframe = str(timeframe_report.get("timeframe") or "")
        replicated = {
            int(value)
            for value in (
                timeframe_report.get("replicated_movement_gate_horizons") or ()
            )
        }
        for split, horizons in (timeframe_report.get("metrics") or {}).items():
            for horizon, metrics in (horizons or {}).items():
                horizon_sec = int(horizon)
                row = {
                    "run": run_name,
                    "generated_at": report.get("generated_at"),
                    "estimator_type": fit.get("estimator_type") or "legacy_ridge",
                    "target_space": fit.get("target_space") or "raw_pips",
                    "pair_context": fit.get("pair_context") or "none",
                    "split_policy": fit.get("split_policy") or "pair_fraction",
                    "samples_per_pair_timeframe": fit.get(
                        "samples_per_pair_timeframe"
                    ),
                    "instrument_count": report.get("instrument_count"),
                    "timeframe": timeframe,
                    "horizon_sec": horizon_sec,
                    "split": split,
                    "replicated_movement_gate": horizon_sec in replicated,
                }
                row.update(
                    {
                        key: value
                        for key, value in metrics.items()
                        if key != "confidence_bins"
                    }
                )
                predicted_up = finite(metrics.get("predicted_up_fraction"))
                row["one_sided_prediction"] = bool(
                    predicted_up is not None
                    and (predicted_up <= 0.05 or predicted_up >= 0.95)
                )
                pooled_magnitude = finite(
                    metrics.get("magnitude_pip_correlation")
                )
                within_magnitude = finite(
                    metrics.get("within_pair_magnitude_pip_correlation")
                )
                row["magnitude_scale_inflation"] = (
                    pooled_magnitude - within_magnitude
                    if pooled_magnitude is not None
                    and within_magnitude is not None
                    else None
                )
                rows.append(row)
    return rows


def strict_pair_rows(cells: pd.DataFrame) -> pd.DataFrame:
    if cells.empty:
        return pd.DataFrame()
    identity = [
        "run",
        "timeframe",
        "horizon_sec",
        "estimator_type",
        "target_space",
        "pair_context",
        "split_policy",
        "samples_per_pair_timeframe",
        "instrument_count",
    ]
    fields = [
        "direction_accuracy",
        "direction_accuracy_lower_95",
        "macro_direction_accuracy",
        "macro_direction_accuracy_lower_95",
        "balanced_accuracy",
        "macro_balanced_accuracy",
        "macro_balanced_accuracy_lower_95",
        "predicted_up_fraction",
        "executable_average_net_pips",
        "pair_average_net_lower_95_pips",
        "executable_average_net_cost_units",
        "pair_average_net_cost_units_lower_95",
        "positive_pair_fraction",
        "within_pair_signed_pip_correlation",
        "within_pair_magnitude_pip_correlation",
        "movement_gate_entry_n",
        "movement_gate_pair_count",
        "movement_gate_average_net_pips",
        "movement_gate_pair_average_net_lower_95",
        "movement_gate_average_net_cost_units",
        "movement_gate_pair_average_net_cost_units_lower_95",
        "movement_gate_direction_accuracy_lower_95",
        "movement_gate_macro_direction_accuracy_lower_95",
        "movement_gate_positive_pair_fraction",
        "movement_gate_profit_factor",
    ]
    available = [field for field in fields if field in cells.columns]
    validation = cells[cells["split"] == "validation"][
        [*identity, *available]
    ].copy()
    holdout = cells[cells["split"] == "holdout"][
        [*identity, *available]
    ].copy()
    paired = validation.merge(
        holdout,
        on=identity,
        how="inner",
        suffixes=("_validation", "_holdout"),
    )
    if paired.empty:
        return paired

    def above(frame: pd.DataFrame, field: str, threshold: float) -> pd.Series:
        column = frame.get(field)
        if column is None:
            return pd.Series(False, index=frame.index)
        return pd.to_numeric(column, errors="coerce") > threshold

    def at_least(
        frame: pd.DataFrame,
        field: str,
        threshold: float,
    ) -> pd.Series:
        column = frame.get(field)
        if column is None:
            return pd.Series(False, index=frame.index)
        return pd.to_numeric(column, errors="coerce") >= threshold

    paired["strict_direction_cost_replication"] = (
        above(paired, "macro_direction_accuracy_lower_95_validation", 0.50)
        & above(paired, "macro_direction_accuracy_lower_95_holdout", 0.50)
        & above(paired, "macro_balanced_accuracy_validation", 0.50)
        & above(paired, "macro_balanced_accuracy_holdout", 0.50)
        & above(paired, "pair_average_net_lower_95_pips_validation", 0.0)
        & above(paired, "pair_average_net_lower_95_pips_holdout", 0.0)
        & above(
            paired,
            "pair_average_net_cost_units_lower_95_validation",
            0.0,
        )
        & above(
            paired,
            "pair_average_net_cost_units_lower_95_holdout",
            0.0,
        )
        & at_least(paired, "positive_pair_fraction_validation", 0.50)
        & at_least(paired, "positive_pair_fraction_holdout", 0.50)
    )
    paired["strict_movement_gate_replication"] = (
        at_least(paired, "movement_gate_entry_n_validation", 50)
        & at_least(paired, "movement_gate_entry_n_holdout", 50)
        & at_least(paired, "movement_gate_pair_count_validation", 8)
        & at_least(paired, "movement_gate_pair_count_holdout", 8)
        & above(
            paired,
            "movement_gate_pair_average_net_lower_95_validation",
            0.0,
        )
        & above(
            paired,
            "movement_gate_pair_average_net_lower_95_holdout",
            0.0,
        )
        & above(
            paired,
            "movement_gate_pair_average_net_cost_units_lower_95_validation",
            0.0,
        )
        & above(
            paired,
            "movement_gate_pair_average_net_cost_units_lower_95_holdout",
            0.0,
        )
        & above(
            paired,
            "movement_gate_direction_accuracy_lower_95_validation",
            0.50,
        )
        & above(
            paired,
            "movement_gate_direction_accuracy_lower_95_holdout",
            0.50,
        )
        & above(
            paired,
            "movement_gate_macro_direction_accuracy_lower_95_validation",
            0.50,
        )
        & above(
            paired,
            "movement_gate_macro_direction_accuracy_lower_95_holdout",
            0.50,
        )
        & at_least(
            paired,
            "movement_gate_positive_pair_fraction_validation",
            0.50,
        )
        & at_least(
            paired,
            "movement_gate_positive_pair_fraction_holdout",
            0.50,
        )
        & above(paired, "movement_gate_profit_factor_validation", 1.0)
        & above(paired, "movement_gate_profit_factor_holdout", 1.0)
    )
    paired["methodology_comparable"] = (
        paired["split_policy"] == "global_time_purged"
    )
    return paired


def run_summaries(cells: pd.DataFrame, paired: pd.DataFrame) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for run_name, rows in cells.groupby("run", sort=True):
        holdout = rows[rows["split"] == "holdout"]
        run_pairs = paired[paired["run"] == run_name] if not paired.empty else paired
        summaries.append(
            {
                "run": run_name,
                "generated_at": rows["generated_at"].iloc[0],
                "estimator_type": rows["estimator_type"].iloc[0],
                "target_space": rows["target_space"].iloc[0],
                "pair_context": rows["pair_context"].iloc[0],
                "split_policy": rows["split_policy"].iloc[0],
                "instrument_count": int(rows["instrument_count"].iloc[0] or 0),
                "holdout_cell_count": int(len(holdout)),
                "one_sided_holdout_cells": int(
                    holdout["one_sided_prediction"].fillna(False).sum()
                ),
                "strict_direction_cost_replications": int(
                    run_pairs.get(
                        "strict_direction_cost_replication",
                        pd.Series(dtype=bool),
                    ).fillna(False).sum()
                ),
                "strict_movement_gate_replications": int(
                    run_pairs.get(
                        "strict_movement_gate_replication",
                        pd.Series(dtype=bool),
                    ).fillna(False).sum()
                ),
                "diagnostics_complete": bool(
                    "within_pair_magnitude_pip_correlation" in holdout
                    and holdout[
                        "within_pair_magnitude_pip_correlation"
                    ].notna().any()
                ),
                "cost_normalized_diagnostics_complete": bool(
                    "pair_average_net_cost_units_lower_95" in holdout
                    and holdout[
                        "pair_average_net_cost_units_lower_95"
                    ].notna().any()
                ),
            }
        )
    return summaries


def top_diagnostic_rows(
    paired: pd.DataFrame,
    limit: int = 12,
) -> list[dict[str, Any]]:
    if paired.empty:
        return []
    required = (
        "pair_average_net_cost_units_lower_95_validation",
        "pair_average_net_cost_units_lower_95_holdout",
        "macro_direction_accuracy_lower_95_validation",
        "macro_direction_accuracy_lower_95_holdout",
    )
    if any(column not in paired for column in required):
        return []
    ranked = paired.copy()
    ranked["worst_pair_cost_lower"] = ranked[
        [
            "pair_average_net_cost_units_lower_95_validation",
            "pair_average_net_cost_units_lower_95_holdout",
        ]
    ].min(axis=1)
    ranked["worst_macro_direction_lower"] = ranked[
        [
            "macro_direction_accuracy_lower_95_validation",
            "macro_direction_accuracy_lower_95_holdout",
        ]
    ].min(axis=1)
    ranked = ranked[
        ranked["methodology_comparable"].fillna(False)
        & ranked["worst_pair_cost_lower"].notna()
    ].sort_values(
        ["worst_pair_cost_lower", "worst_macro_direction_lower"],
        ascending=False,
    )
    columns = [
        "run",
        "timeframe",
        "horizon_sec",
        "estimator_type",
        "pair_context",
        "instrument_count",
        "worst_pair_cost_lower",
        "worst_macro_direction_lower",
        "pair_average_net_cost_units_lower_95_validation",
        "pair_average_net_cost_units_lower_95_holdout",
        "macro_direction_accuracy_lower_95_validation",
        "macro_direction_accuracy_lower_95_holdout",
        "strict_direction_cost_replication",
        "strict_movement_gate_replication",
    ]
    records: list[dict[str, Any]] = []
    for raw in ranked.head(limit)[columns].to_dict(orient="records"):
        records.append(
            {
                key: (
                    None
                    if isinstance(value, float) and not math.isfinite(value)
                    else value
                )
                for key, value in raw.items()
            }
        )
    return records


def top_movement_gate_rows(
    paired: pd.DataFrame,
    limit: int = 12,
) -> list[dict[str, Any]]:
    if paired.empty:
        return []
    cost_columns = [
        "movement_gate_pair_average_net_cost_units_lower_95_validation",
        "movement_gate_pair_average_net_cost_units_lower_95_holdout",
    ]
    direction_columns = [
        "movement_gate_macro_direction_accuracy_lower_95_validation",
        "movement_gate_macro_direction_accuracy_lower_95_holdout",
    ]
    if any(column not in paired for column in (*cost_columns, *direction_columns)):
        return []
    ranked = paired.copy()
    ranked["worst_movement_pair_cost_lower"] = ranked[cost_columns].min(axis=1)
    ranked["worst_movement_macro_direction_lower"] = ranked[
        direction_columns
    ].min(axis=1)
    ranked = ranked[
        ranked["methodology_comparable"].fillna(False)
        & ranked["worst_movement_pair_cost_lower"].notna()
    ].sort_values(
        [
            "worst_movement_pair_cost_lower",
            "worst_movement_macro_direction_lower",
        ],
        ascending=False,
    )
    columns = [
        "run",
        "timeframe",
        "horizon_sec",
        "estimator_type",
        "pair_context",
        "instrument_count",
        "worst_movement_pair_cost_lower",
        "worst_movement_macro_direction_lower",
        *cost_columns,
        *direction_columns,
        "movement_gate_entry_n_validation",
        "movement_gate_entry_n_holdout",
        "strict_movement_gate_replication",
    ]
    records: list[dict[str, Any]] = []
    for raw in ranked.head(limit)[columns].to_dict(orient="records"):
        records.append(
            {
                key: (
                    None
                    if isinstance(value, float) and not math.isfinite(value)
                    else value
                )
                for key, value in raw.items()
            }
        )
    return records


def markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Intensive MA Feature Grid Audit",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        "This audit keeps older experiments visible but does not treat unpurged,",
        "row-pooled accuracy as equivalent to globally purged pair-aware evidence.",
        "All MA variants remain shadow-only unless validation and untouched holdout",
        "both clear the strict direction, pair-breadth, and after-spread gates.",
        "",
        "## Runs",
        "",
        "| Run | Estimator | Split | Pairs | Holdout cells | One-sided | Strict cost | Strict movement |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["runs"]:
        lines.append(
            "| {run} | {estimator_type} | {split_policy} | {instrument_count} | "
            "{holdout_cell_count} | {one_sided_holdout_cells} | "
            "{strict_direction_cost_replications} | "
            "{strict_movement_gate_replications} |".format(**row)
        )
    lines.extend(
        [
            "",
            "## Result",
            "",
            f"- Strict direction-and-cost replications: **{payload['strict_direction_cost_replications']}**",
            f"- Strict movement-gate replications: **{payload['strict_movement_gate_replications']}**",
            f"- Globally purged runs: **{payload['globally_purged_run_count']}**",
            f"- Legacy/incomparable runs retained for context: **{payload['legacy_run_count']}**",
            "",
            "A zero is a valid negative result. It means no MA cell is authorized for",
            "account use, not that the experiment failed to produce forecasts.",
            "",
            "## Highest Diagnostic Cells",
            "",
            "| Run | TF | Horizon | Worst pair net/spread lower 95% | Worst macro direction lower 95% |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in payload["top_diagnostic_cells"]:
        lines.append(
            "| {run} | {timeframe} | {horizon_sec}s | {cost:+.4f} | "
            "{direction:.2%} |".format(
                run=row["run"],
                timeframe=row["timeframe"],
                horizon_sec=int(row["horizon_sec"]),
                cost=float(row["worst_pair_cost_lower"]),
                direction=float(row["worst_macro_direction_lower"]),
            )
        )
    lines.extend(
        [
            "",
            "## Highest Movement-Gate Cells",
            "",
            "| Run | TF | Horizon | Worst gated pair net/spread lower 95% | Worst gated macro direction lower 95% |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in payload["top_movement_gate_cells"]:
        lines.append(
            "| {run} | {timeframe} | {horizon_sec}s | {cost:+.4f} | "
            "{direction:.2%} |".format(
                run=row["run"],
                timeframe=row["timeframe"],
                horizon_sec=int(row["horizon_sec"]),
                cost=float(row["worst_movement_pair_cost_lower"]),
                direction=float(row["worst_movement_macro_direction_lower"]),
            )
        )
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for path in sorted(args.report_root.glob("ma_feature_grid*/ma_feature_grid_latest.json")):
        if path.parent == args.output_dir:
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        run_name = path.parent.name
        rows.extend(metric_rows(run_name, report))
        sources.append(
            {
                "run": run_name,
                "path": str(path.resolve()),
                "sha256": sha256(path),
            }
        )
    cells = pd.DataFrame(rows)
    paired = strict_pair_rows(cells)
    runs = run_summaries(cells, paired) if not cells.empty else []
    top_cells = top_diagnostic_rows(paired)
    top_movement_cells = top_movement_gate_rows(paired)
    strict_direction = int(
        paired.get(
            "strict_direction_cost_replication",
            pd.Series(dtype=bool),
        ).fillna(False).sum()
    )
    strict_movement = int(
        paired.get(
            "strict_movement_gate_replication",
            pd.Series(dtype=bool),
        ).fillna(False).sum()
    )
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_count": len(sources),
        "sources": sources,
        "cell_row_count": int(len(cells)),
        "paired_cell_count": int(len(paired)),
        "strict_direction_cost_replications": strict_direction,
        "strict_movement_gate_replications": strict_movement,
        "globally_purged_run_count": sum(
            row["split_policy"] == "global_time_purged" for row in runs
        ),
        "legacy_run_count": sum(
            row["split_policy"] != "global_time_purged" for row in runs
        ),
        "execution_authorized": False,
        "runs": runs,
        "top_diagnostic_cells": top_cells,
        "top_movement_gate_cells": top_movement_cells,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.output_dir / REPORT_NAME
    csv_path = args.output_dir / CSV_NAME
    summary_path = args.output_dir / SUMMARY_NAME
    report_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    cells.to_csv(csv_path, index=False)
    summary_path.write_text(markdown(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "report": str(report_path.resolve()),
                "cells": str(csv_path.resolve()),
                "summary": str(summary_path.resolve()),
                "strict_direction_cost_replications": strict_direction,
                "strict_movement_gate_replications": strict_movement,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
