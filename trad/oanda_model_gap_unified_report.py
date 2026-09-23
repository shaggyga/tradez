#!/usr/bin/env python3
"""Unify all modern-model gap evidence under one shadow integration contract."""

from __future__ import annotations

import argparse
import json
import math
try:
    from oanda_profit_factor_contract_v2 import normalize_profit_factor, profit_factor_at_least, summary_metric_fields
except ModuleNotFoundError:
    from trad.oanda_profit_factor_contract_v2 import normalize_profit_factor, profit_factor_at_least, summary_metric_fields
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import oanda_model_gap_registry as registry
    import oanda_shared_timeframe_horizon_panel as panel
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad import oanda_model_gap_registry as registry
    from trad import oanda_shared_timeframe_horizon_panel as panel


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data" / "oanda_training_manager"
REPORT_ROOT = DATA_ROOT / "reports" / "modern_model_gap"
DEFAULT_OUTPUT = REPORT_ROOT / "unified_model_gap_market_latest.json"
DEFAULT_COMPLETION = DATA_ROOT / "model_space" / "model_gap_completion_latest.json"
DEFAULT_MATRIX = (
    DATA_ROOT / "training_sets" / "model_gap_full_matrix" / "full_matrix_latest.json"
)
DEFAULT_REPORTS = (
    REPORT_ROOT / "shared_panel_model_benchmark_latest.json",
    REPORT_ROOT / "shared_panel_neural_benchmark_latest.json",
    REPORT_ROOT / "shared_panel_neural_long_horizon_benchmark_latest.json",
    REPORT_ROOT / "foundation_market_benchmark_latest.json",
    REPORT_ROOT / "remaining_market_validation_latest.json",
)
REQUIRED_LONG_HORIZONS_SEC = panel.REQUIRED_LONG_HORIZONS_SEC
REQUIRED_LONG_CELLS = panel.REQUIRED_LONG_CELLS
MARKET_STATUSES = {"evaluated", "adapter_qualified", "bounded_market_scored"}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def preserve_generated_utc_if_unchanged(
    report: dict[str, Any],
    previous: dict[str, Any],
) -> dict[str, Any]:
    candidate = dict(report)
    candidate_without_time = dict(candidate)
    previous_without_time = dict(previous)
    candidate_without_time.pop("generated_utc", None)
    previous_without_time.pop("generated_utc", None)
    previous_time = str(previous.get("generated_utc") or "")
    if previous_time and candidate_without_time == previous_without_time:
        candidate["generated_utc"] = previous_time
    return candidate


def finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def result_cells(result: dict[str, Any]) -> list[dict[str, Any]]:
    holdout = result.get("holdout") or {}
    raw = (
        result.get("cells")
        or holdout.get("all_prediction_cell_metrics")
        or result.get("all_prediction_cell_metrics")
        or result.get("cell_metrics")
        or []
    )
    cells: list[dict[str, Any]] = []
    for row in raw:
        timeframe = str(row.get("input_timeframe") or "")
        horizon = int(finite(row.get("horizon_sec")))
        if not timeframe or horizon <= 0:
            continue
        events = int(finite(row.get("events"), finite(row.get("n"))))
        trades = int(finite(row.get("trades"), events))
        cells.append(
            {
                "input_timeframe": timeframe,
                "horizon_sec": horizon,
                "events": events,
                "trades": trades,
                "trade_rate": finite(row.get("trade_rate"), trades / events if events else 0.0),
                "direction_accuracy": finite(
                    row.get("direction_accuracy"), finite(row.get("best_side_rate"))
                ),
                "win_rate": finite(row.get("win_rate")),
                "mean_net_pips": finite(
                    row.get("mean_net_pips"), finite(row.get("avg_net_pips"))
                ),
                **summary_metric_fields(row, "median_net_pips"),
                "sum_net_pips": finite(row.get("sum_net_pips")),
                **normalize_profit_factor(row),
                **summary_metric_fields(row, "max_drawdown_pips"),
            }
        )
    return sorted(
        cells,
        key=lambda row: (
            panel.parse_timeframe(row["input_timeframe"]),
            row["horizon_sec"],
        ),
    )


def index_results(
    reports: Iterable[tuple[Path, dict[str, Any]]],
) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
    indexed: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    registered = {spec.model_id for spec in registry.MODEL_SPECS}
    for path, report in reports:
        for result in report.get("results") or []:
            model = str(result.get("model") or "")
            if model in registered:
                indexed.setdefault(model, []).append((path, result))
    return indexed


def merge_result_cells(
    sources: Iterable[tuple[Path, dict[str, Any]]],
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int], dict[str, Any]] = {}
    for _, result in sources:
        for row in result_cells(result):
            key = (row["input_timeframe"], int(row["horizon_sec"]))
            previous = merged.get(key)
            if previous is None or row["events"] > previous["events"]:
                merged[key] = row
    return sorted(
        merged.values(),
        key=lambda row: (
            panel.parse_timeframe(row["input_timeframe"]),
            row["horizon_sec"],
        ),
    )


def build_report(
    component_paths: Iterable[Path],
    completion_path: Path,
    matrix_path: Path,
) -> dict[str, Any]:
    components = [(path.resolve(), load_json(path.resolve())) for path in component_paths]
    completion = load_json(completion_path.resolve())
    matrix = load_json(matrix_path.resolve())
    indexed = index_results(components)
    completion_rows = {
        str(row.get("model")): row for row in completion.get("models") or []
    }
    remaining = next(
        (
            report
            for path, report in components
            if path.name == "remaining_market_validation_latest.json"
        ),
        {},
    )
    matrix_coverage = matrix.get("coverage") or {}
    expected_timeframe_horizon_cells = len(panel.CANONICAL_TIMEFRAMES) * len(
        panel.CANONICAL_HORIZONS_SEC
    )
    expected_pair_cells = 68 * expected_timeframe_horizon_cells
    observed_timeframe_horizon_cells = int(
        matrix_coverage.get("observed_timeframe_horizon_cells") or 0
    )
    observed_pair_cells = int(matrix_coverage.get("observed_pair_cells") or 0)
    documented_unavailable_count = int(
        matrix_coverage.get("documented_unavailable_count") or 0
    )
    accounted_pair_cells = int(
        matrix_coverage.get("observed_or_documented_pair_cells")
        or (observed_pair_cells + documented_unavailable_count)
    )
    unaccounted_pair_cell_count = int(
        matrix_coverage.get(
            "unaccounted_pair_cell_count",
            max(expected_pair_cells - accounted_pair_cells, 0),
        )
        or 0
    )
    fully_observed = (
        observed_timeframe_horizon_cells == expected_timeframe_horizon_cells
        and observed_pair_cells == expected_pair_cells
    )
    matrix_contract = matrix_coverage.get("contract_complete")
    matrix_complete = (
        bool(matrix_contract)
        if matrix_contract is not None
        else observed_timeframe_horizon_cells == expected_timeframe_horizon_cells
        and accounted_pair_cells == expected_pair_cells
        and unaccounted_pair_cell_count == 0
    )
    dataset = dict(remaining.get("dataset") or {})
    dataset.update(
        {
            "timeframes": list(panel.CANONICAL_TIMEFRAMES),
            "horizons_sec": list(panel.CANONICAL_HORIZONS_SEC),
            "expected_timeframe_horizon_cells": expected_timeframe_horizon_cells,
            "observed_timeframe_horizon_cells": observed_timeframe_horizon_cells,
            "expected_pair_cells": expected_pair_cells,
            "observed_pair_cells": observed_pair_cells,
            "documented_unavailable_count": documented_unavailable_count,
            "observed_or_documented_pair_cells": accounted_pair_cells,
            "unaccounted_pair_cell_count": unaccounted_pair_cell_count,
            "matrix_coverage_ratio": (
                observed_pair_cells / expected_pair_cells if expected_pair_cells else 0.0
            ),
            "matrix_accounted_coverage_ratio": (
                accounted_pair_cells / expected_pair_cells if expected_pair_cells else 0.0
            ),
            "matrix_fully_observed": fully_observed,
            "matrix_complete": matrix_complete,
        }
    )

    models: list[dict[str, Any]] = []
    for spec in registry.MODEL_SPECS:
        completion_row = completion_rows.get(spec.model_id, {})
        sources = indexed.get(spec.model_id, [])
        source_path, source = sources[0] if sources else (Path(), {})
        runtime_status = str(completion_row.get("runtime_status") or "not_reported")
        blocked = runtime_status == "blocked"
        cells = merge_result_cells(sources)
        observed = {
            (row["input_timeframe"], int(row["horizon_sec"])) for row in cells
        }
        missing_long = [
            {"input_timeframe": timeframe, "horizon_sec": horizon}
            for timeframe, horizon in REQUIRED_LONG_CELLS
            if (timeframe, horizon) not in observed
        ]
        market_statuses = [str(result.get("status") or "") for _, result in sources]
        market_status = next(
            (status for status in market_statuses if status in MARKET_STATUSES),
            "runtime_blocked" if blocked else "not_run",
        )
        backtest_complete = bool(
            completion_row.get("market_backtest_complete")
            and market_status in MARKET_STATUSES
        )
        models.append(
            {
                "model": spec.model_id,
                "family": spec.family,
                "provider": spec.provider,
                "runtime_status": runtime_status,
                "runtime_reason": completion_row.get("runtime_reason"),
                "status": market_status,
                "market_backtest_complete": backtest_complete,
                "long_horizon_coverage_complete": not missing_long and not blocked,
                "required_long_cells": len(REQUIRED_LONG_CELLS),
                "observed_cell_count": len(observed),
                "missing_required_long_cells": missing_long,
                "classification": source.get("classification")
                or (source.get("holdout") or {}).get("classification"),
                "selected": source.get("selected")
                or (source.get("holdout") or {}).get("selected")
                or source.get("summary"),
                "cells": cells,
                "validation": source.get("validation"),
                "production_gate": source.get("production_gate")
                or (source.get("holdout") or {}).get("production_gate"),
                "source_report": str(source_path) if source else "",
                "supporting_reports": [str(path) for path, _ in sources],
                "production_eligible": False,
                "account_wired": False,
            }
        )

    runnable = [row for row in models if row["runtime_status"] != "blocked"]
    blocked = [row for row in models if row["runtime_status"] == "blocked"]
    backtested = [row for row in runnable if row["market_backtest_complete"]]
    long_complete = [row for row in runnable if row["long_horizon_coverage_complete"]]
    summary = {
        "models": len(models),
        "families": len({row["family"] for row in models}),
        "runtime_available": len(runnable),
        "runtime_blocked": len(blocked),
        "runtime_blocked_models": [row["model"] for row in blocked],
        "market_backtested": len(backtested),
        "missing_market_backtests": [
            row["model"] for row in runnable if not row["market_backtest_complete"]
        ],
        "long_horizon_coverage_complete": len(long_complete),
        "missing_long_horizon_coverage": [
            row["model"] for row in runnable if not row["long_horizon_coverage_complete"]
        ],
        "all_requested_backtested": len(backtested) == len(runnable),
        "all_runnable_long_horizons_covered": len(long_complete) == len(runnable),
        "canonical_matrix_artifact_complete": matrix_complete,
        "inventory_complete": len(models) == len(registry.MODEL_SPECS) == 30,
        "production_eligible": 0,
        "account_wired": 0,
    }
    summary["integration_complete"] = bool(
        summary["inventory_complete"]
        and summary["all_requested_backtested"]
        and summary["all_runnable_long_horizons_covered"]
        and summary["canonical_matrix_artifact_complete"]
    )
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "scope": "all 30 modern model-gap entries under one shadow integration path",
        "execution_policy": "shadow_only_no_account_wiring",
        "required_long_horizon_contract": {
            "input_timeframes": ["H1", "H4"],
            "horizons_sec": list(REQUIRED_LONG_HORIZONS_SEC),
            "cells": [
                {"input_timeframe": timeframe, "horizon_sec": horizon}
                for timeframe, horizon in REQUIRED_LONG_CELLS
            ],
        },
        "dataset": dataset,
        "component_reports": [str(path) for path, _ in components],
        "models": models,
        "results": models,
        "summary": summary,
        "account_wired": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component-report", type=Path, action="append", default=[])
    parser.add_argument("--completion", type=Path, default=DEFAULT_COMPLETION)
    parser.add_argument("--matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paths = args.component_report or list(DEFAULT_REPORTS)
    while True:
        report = build_report(paths, args.completion, args.matrix)
        report = preserve_generated_utc_if_unchanged(
            report,
            load_json(args.output.resolve()),
        )
        atomic_json(args.output.resolve(), report)
        print(json.dumps(report["summary"], indent=2), flush=True)
        if args.require_complete and not report["summary"]["integration_complete"]:
            return 2
        if args.interval_sec <= 0.0:
            return 0
        time.sleep(max(60.0, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
