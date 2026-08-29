from __future__ import annotations

import json

import pytest

import oanda_foundation_market_report_merge as merger


def _write(path, model: str, avg_net_pips: float) -> None:
    path.write_text(
        json.dumps(
            {
                "generated_utc": "2026-07-21T00:00:00+00:00",
                "fixtures": [f"{model}.npz"],
                "records": [{"model": model, "status": "scored"}],
                "results": [
                    {
                        "model": model,
                        "status": "bounded_market_scored",
                        "summary": {"avg_net_pips": avg_net_pips},
                        "production_eligible": False,
                        "account_wired": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_merge_reports_preserves_disjoint_results_and_ranks_net_pips(tmp_path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    _write(first, "chronos_2", -2.0)
    _write(second, "lag_llama", 0.5)

    report = merger.merge_reports([first, second])

    assert [row["model"] for row in report["results"]] == ["chronos_2", "lag_llama"]
    assert report["ranking"] == ["lag_llama", "chronos_2"]
    assert report["summary"] == {
        "requested": 2,
        "scored": 2,
        "failed_or_blocked": 0,
        "production_eligible": 0,
        "account_wired": 0,
    }
    assert len(report["source_reports"]) == 2


def test_merge_reports_rejects_duplicate_models(tmp_path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    _write(first, "chronos_2", -2.0)
    _write(second, "chronos_2", -1.0)

    with pytest.raises(ValueError, match="duplicate model"):
        merger.merge_reports([first, second])
