from __future__ import annotations

import json
import sys

import oanda_model_gap_completion as completion


EXPECTED_PAIR_CELLS = 68 * len(completion.panel.CANONICAL_TIMEFRAMES) * len(
    completion.panel.CANONICAL_HORIZONS_SEC
)


def _write(path, payload) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def _market_row(model: str) -> dict:
    return {
        "model": model,
        "status": "bounded_market_scored",
        "cells": [{"input_timeframe": "M1", "horizon_sec": 60}],
        "validation": {"backtest_valid": True, "performance_passed": False},
        "production_gate": {"passed": False},
    }


def test_noop_completion_refresh_preserves_content_addressed_timestamp() -> None:
    previous = {
        "generated_utc": "2026-07-21T21:42:02+00:00",
        "summary": {"models": 30},
    }
    candidate = {
        "generated_utc": "2026-07-21T22:26:57+00:00",
        "summary": {"models": 30},
    }

    stable = completion.preserve_generated_utc_if_unchanged(candidate, previous)

    assert stable["generated_utc"] == previous["generated_utc"]
    assert candidate["generated_utc"] == "2026-07-21T22:26:57+00:00"


def test_completion_ledger_keeps_all_models_and_market_evidence(monkeypatch, tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    qualification = tmp_path / "qualification.json"
    matrix = tmp_path / "matrix.json"
    tabular = tmp_path / "tabular.json"
    neural = tmp_path / "neural.json"
    foundation = tmp_path / "foundation.json"
    remaining = tmp_path / "remaining.json"
    weights = tmp_path / "weights.json"
    output = tmp_path / "completion.json"
    blocked = {"timesfm_icf", "mamba"}
    _write(
        runtime,
        {
            "model_runtime": [
                {
                    "model": spec.model_id,
                    "status": "blocked" if spec.model_id in blocked else "runtime_available",
                    "reason": "audited runtime blocker" if spec.model_id in blocked else "",
                }
                for spec in completion.registry.MODEL_SPECS
            ]
        },
    )
    _write(qualification, {"results": []})
    _write(
        matrix,
        {
            "coverage": {
                "expected_pair_cells": EXPECTED_PAIR_CELLS,
                "observed_pair_cells": EXPECTED_PAIR_CELLS - 42,
                "documented_unavailable_count": 42,
                "observed_or_documented_pair_cells": EXPECTED_PAIR_CELLS,
                "unaccounted_pair_cell_count": 0,
                "fully_covered": False,
                "contract_complete": True,
            }
        },
    )
    tabular_models = {"catboost", "ngboost"}
    neural_models = {"tft", "deepar"}
    foundation_models = {
        "chronos_2",
        "timesfm_2_5",
        "moirai",
        "moirai_moe",
        "tiny_time_mixer",
        "toto_2",
        "lag_llama",
    }
    remaining_models = {
        spec.model_id
        for spec in completion.registry.MODEL_SPECS
        if spec.model_id not in blocked | tabular_models | neural_models | foundation_models
    }
    _write(tabular, {"results": [_market_row(model) for model in sorted(tabular_models)]})
    _write(neural, {"results": [_market_row(model) for model in sorted(neural_models)]})
    _write(
        foundation,
        {"results": [_market_row(model) for model in sorted(foundation_models)]},
    )
    _write(remaining, {"results": [_market_row(model) for model in sorted(remaining_models)]})
    _write(weights, {"records": []})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_model_gap_completion.py",
            "--runtime-report",
            str(runtime),
            "--qualification-report",
            str(qualification),
            "--matrix-report",
            str(matrix),
            "--tabular-report",
            str(tabular),
            "--neural-report",
            str(neural),
            "--foundation-report",
            str(foundation),
            "--remaining-market-report",
            str(remaining),
            "--weight-report",
            str(weights),
            "--output",
            str(output),
            "--require-complete",
        ],
    )

    assert completion.main() == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    catboost = next(row for row in report["models"] if row["model"] == "catboost")
    assert report["summary"]["models"] == 30
    assert report["summary"]["evidence_complete"] is True
    assert report["summary"]["market_backtest_required"] == 28
    assert report["summary"]["market_backtest_complete"] == 28
    assert report["summary"]["runtime_blocked_models"] == ["timesfm_icf", "mamba"]
    assert report["summary"]["matrix_complete"] is True
    assert report["matrix_coverage"]["fully_covered"] is False
    assert report["matrix_coverage"]["contract_complete"] is True
    assert report["matrix_coverage"]["documented_unavailable_count"] == 42
    assert catboost["evidence_level"] == "bounded_market_qualified"
    assert catboost["market"]["cell_count"] == 1
    assert report["summary"]["account_wired"] == 0


def test_completion_ledger_rejects_missing_or_partial_evidence(monkeypatch, tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    qualification = tmp_path / "qualification.json"
    matrix = tmp_path / "matrix.json"
    tabular = tmp_path / "tabular.json"
    neural = tmp_path / "neural.json"
    foundation = tmp_path / "foundation.json"
    remaining = tmp_path / "remaining.json"
    weights = tmp_path / "weights.json"
    output = tmp_path / "completion.json"
    _write(runtime, {"model_runtime": []})
    _write(qualification, {"results": []})
    _write(
        matrix,
        {
            "coverage": {
                "expected_pair_cells": EXPECTED_PAIR_CELLS,
                "observed_pair_cells": EXPECTED_PAIR_CELLS - 1,
            }
        },
    )
    _write(tabular, {"results": []})
    _write(neural, {"results": []})
    _write(foundation, {"results": []})
    _write(remaining, {"results": []})
    _write(weights, {})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_model_gap_completion.py",
            "--runtime-report",
            str(runtime),
            "--qualification-report",
            str(qualification),
            "--matrix-report",
            str(matrix),
            "--tabular-report",
            str(tabular),
            "--neural-report",
            str(neural),
            "--foundation-report",
            str(foundation),
            "--remaining-market-report",
            str(remaining),
            "--weight-report",
            str(weights),
            "--output",
            str(output),
            "--require-complete",
        ],
    )

    assert completion.main() == 2
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["summary"]["evidence_complete"] is False
    assert report["summary"]["matrix_complete"] is False
    assert report["summary"]["missing_required_reports"] == ["weights"]
