from __future__ import annotations

from pathlib import Path

import pandas as pd

import oanda_modern_tabular_gap_benchmark as benchmark


def synthetic_frame(rows: int = 40) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            name: [float(index % 7) for index in range(rows)]
            for name in benchmark.event_meta.NUMERIC_FEATURES
        }
    )
    frame["time_utc"] = pd.date_range(
        "2026-01-01", periods=rows, freq="5min", tz="UTC"
    )
    frame["instrument"] = ["EUR_USD", "GBP_USD"] * (rows // 2)
    frame["direction"] = ["LONG", "SHORT"] * (rows // 2)
    frame["theme"] = "event"
    frame["setup_family"] = "pressure"
    frame["trade_style"] = "scalp"
    frame[benchmark.event_meta.TARGET] = [index % 2 for index in range(rows)]
    frame["realized_pips"] = [1.0 if index % 2 else -1.0 for index in range(rows)]
    frame["realized_r"] = frame["realized_pips"]
    return frame


def test_load_dataset_enforces_complete_time_ordered_contract(tmp_path: Path) -> None:
    source = synthetic_frame()
    source.loc[0, benchmark.event_meta.NUMERIC_FEATURES[0]] = None
    path = tmp_path / "event_meta.parquet"
    source.to_parquet(path, index=False)

    loaded, summary = benchmark.load_dataset(path)

    assert len(loaded) == len(source) - 1
    assert loaded["time_utc"].is_monotonic_increasing
    assert summary["dropped_incomplete_rows"] == 1
    assert loaded[benchmark.event_meta.TARGET].nunique() == 2


def test_run_benchmark_persists_ranked_shadow_evidence(
    monkeypatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "event_meta.parquet"
    synthetic_frame().to_parquet(path, index=False)
    monkeypatch.setattr(benchmark, "REPORT_ROOT", tmp_path / "reports")
    monkeypatch.setattr(benchmark, "MODEL_ROOT", tmp_path / "models")

    def fake_evaluate(name: str, frame: pd.DataFrame):
        auc = 0.61 if name == "catboost" else 0.58
        return {"model": name}, {
            "status": "evaluated",
            "model": name,
            "walk_forward": {
                "mean_auc": auc,
                "mean_brier": 0.24,
            },
            "holdout": {"production_gate": {"passed": False}},
        }

    monkeypatch.setattr(benchmark, "evaluate_model", fake_evaluate)

    report = benchmark.run_benchmark(path, ["catboost", "ngboost"])

    assert report["execution_policy"] == "shadow_only"
    assert report["account_wired"] is False
    assert report["all_requested_evaluated"] is True
    assert report["ranking"] == ["catboost", "ngboost"]
    assert (benchmark.REPORT_ROOT / "modern_tabular_gap_benchmark_latest.json").is_file()
    assert len(report["artifacts"]) == 2
