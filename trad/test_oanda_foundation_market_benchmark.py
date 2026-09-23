from __future__ import annotations

import json
from pathlib import Path

import oanda_foundation_market_benchmark as benchmark
import oanda_foundation_market_worker as worker


def test_worker_allows_explicit_cpu() -> None:
    assert worker.resolve_device("cpu") == "cpu"


def test_reusable_result_requires_matching_identity(tmp_path: Path) -> None:
    weights = tmp_path / "weights"
    fixture = tmp_path / "fixture.npz"
    path = tmp_path / "result.json"
    path.write_text(
        json.dumps(
            {
                "model": "chronos_2",
                "status": "bounded_market_scored",
                "weights": str(weights),
                "fixture": str(fixture),
                "summary": {"n": 12},
            }
        ),
        encoding="utf-8",
    )

    assert benchmark.reusable_result(path, "chronos_2", weights, fixture) is not None
    assert benchmark.reusable_result(path, "timesfm_2_5", weights, fixture) is None
    assert (
        benchmark.reusable_result(
            path, "chronos_2", weights, fixture, requested_device="auto"
        )
        is None
    )


def test_output_tail_handles_missing_reader_output() -> None:
    assert benchmark.output_tail(None) == ""
