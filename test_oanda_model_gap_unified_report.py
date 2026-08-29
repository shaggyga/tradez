from __future__ import annotations

import json
from pathlib import Path

import oanda_model_gap_registry as registry
from oanda_model_gap_unified_report import (
    REQUIRED_LONG_CELLS,
    build_report,
    preserve_generated_utc_if_unchanged,
)


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def test_noop_unified_refresh_preserves_content_addressed_timestamp() -> None:
    previous = {
        "generated_utc": "2026-07-21T21:42:03+00:00",
        "summary": {"models": 30},
    }
    candidate = {
        "generated_utc": "2026-07-21T22:26:59+00:00",
        "summary": {"models": 30},
    }

    stable = preserve_generated_utc_if_unchanged(candidate, previous)

    assert stable["generated_utc"] == previous["generated_utc"]


def test_unified_report_keeps_all_30_and_requires_long_cells(tmp_path: Path) -> None:
    blocked = {"timesfm_icf", "mamba"}
    completion_rows = []
    results = []
    for spec in registry.MODEL_SPECS:
        is_blocked = spec.model_id in blocked
        completion_rows.append(
            {
                "model": spec.model_id,
                "runtime_status": "blocked" if is_blocked else "runtime_available",
                "market_backtest_complete": not is_blocked,
            }
        )
        if not is_blocked:
            results.append(
                {
                    "model": spec.model_id,
                    "status": "bounded_market_scored",
                    "cells": [
                        {
                            "input_timeframe": timeframe,
                            "horizon_sec": horizon,
                            "n": 40,
                            "direction_accuracy": 0.55,
                            "win_rate": 0.51,
                            "avg_net_pips": 0.1,
                        }
                        for timeframe, horizon in REQUIRED_LONG_CELLS
                    ],
                }
            )
    completion = tmp_path / "completion.json"
    component = tmp_path / "remaining_market_validation_latest.json"
    matrix = tmp_path / "matrix.json"
    write_json(completion, {"models": completion_rows})
    write_json(
        component,
        {
            "dataset": {"sources": [], "instruments": 68},
            "results": results,
        },
    )
    write_json(
        matrix,
        {
            "coverage": {
                "expected_timeframe_horizon_cells": 13 * 16,
                "observed_timeframe_horizon_cells": 13 * 16,
                "expected_pair_cells": 68 * 13 * 16,
                "observed_pair_cells": (68 * 13 * 16) - 42,
                "documented_unavailable_count": 42,
                "observed_or_documented_pair_cells": 68 * 13 * 16,
                "unaccounted_pair_cell_count": 0,
                "contract_complete": True,
            }
        },
    )

    report = build_report([component], completion, matrix)

    assert report["summary"]["models"] == 30
    assert report["summary"]["canonical_matrix_artifact_complete"] is True
    assert report["dataset"]["matrix_fully_observed"] is False
    assert report["dataset"]["documented_unavailable_count"] == 42
    assert report["summary"]["runtime_available"] == 28
    assert report["summary"]["runtime_blocked_models"] == ["timesfm_icf", "mamba"]
    assert report["summary"]["integration_complete"] is True
    assert len(report["dataset"]["horizons_sec"]) == 16
    assert report["dataset"]["expected_timeframe_horizon_cells"] == 208
    assert report["summary"]["canonical_matrix_artifact_complete"] is True


def test_unified_report_exposes_missing_long_horizon_cell(tmp_path: Path) -> None:
    model = registry.MODEL_SPECS[0].model_id
    completion = tmp_path / "completion.json"
    component = tmp_path / "remaining_market_validation_latest.json"
    matrix = tmp_path / "matrix.json"
    write_json(
        completion,
        {
            "models": [
                {
                    "model": spec.model_id,
                    "runtime_status": (
                        "blocked" if spec.model_id in {"timesfm_icf", "mamba"} else "runtime_available"
                    ),
                    "market_backtest_complete": spec.model_id == model,
                }
                for spec in registry.MODEL_SPECS
            ]
        },
    )
    write_json(
        component,
        {
            "dataset": {},
            "results": [
                {
                    "model": model,
                    "status": "evaluated",
                    "cells": [
                        {
                            "input_timeframe": "H1",
                            "horizon_sec": 3600,
                            "events": 20,
                            "best_side_rate": 0.6,
                        }
                    ],
                }
            ],
        },
    )
    write_json(matrix, {"coverage": {}})

    report = build_report([component], completion, matrix)
    row = next(value for value in report["models"] if value["model"] == model)

    assert row["long_horizon_coverage_complete"] is False
    assert {value["horizon_sec"] for value in row["missing_required_long_cells"]} >= {
        14400,
        43200,
        86400,
    }
    assert report["summary"]["integration_complete"] is False


def test_unified_report_unions_supplemental_cells_for_one_model(tmp_path: Path) -> None:
    model = registry.MODEL_SPECS[0].model_id
    blocked = {"timesfm_icf", "mamba"}
    completion = tmp_path / "completion.json"
    matrix = tmp_path / "matrix.json"
    primary = tmp_path / "primary.json"
    supplemental = tmp_path / "supplemental.json"
    write_json(
        completion,
        {
            "models": [
                {
                    "model": spec.model_id,
                    "runtime_status": (
                        "blocked" if spec.model_id in blocked else "runtime_available"
                    ),
                    "market_backtest_complete": spec.model_id == model,
                }
                for spec in registry.MODEL_SPECS
            ]
        },
    )
    midpoint = len(REQUIRED_LONG_CELLS) // 2

    def result(cells: tuple[tuple[str, int], ...]) -> dict:
        return {
            "model": model,
            "status": "adapter_qualified",
            "cells": [
                {"input_timeframe": timeframe, "horizon_sec": horizon, "events": 20}
                for timeframe, horizon in cells
            ],
        }

    write_json(primary, {"results": [result(REQUIRED_LONG_CELLS[:midpoint])]})
    write_json(supplemental, {"results": [result(REQUIRED_LONG_CELLS[midpoint:])]})
    write_json(matrix, {"coverage": {}})

    report = build_report([primary, supplemental], completion, matrix)
    row = next(value for value in report["models"] if value["model"] == model)

    assert row["long_horizon_coverage_complete"] is True
    assert row["observed_cell_count"] == len(REQUIRED_LONG_CELLS)
    assert row["supporting_reports"] == [str(primary.resolve()), str(supplemental.resolve())]
