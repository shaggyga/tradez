from __future__ import annotations

import json
import sys

import oanda_s5_backfill_only as backfill


def test_backfill_writes_only_to_explicit_output_root(monkeypatch, tmp_path) -> None:
    output = tmp_path / "recovered"
    summary = tmp_path / "summary.json"
    seen = []

    class DummyManager:
        client = object()

    monkeypatch.setattr(backfill.manager, "ensure_dirs", lambda: None)
    monkeypatch.setattr(backfill.manager, "TrainingStrategyManager", DummyManager)
    monkeypatch.setattr(backfill.manager, "iso_utc", lambda: "2026-07-19T00:00:00Z")
    monkeypatch.setattr(
        backfill.manager,
        "save_json",
        lambda path, payload: path.write_text(json.dumps(payload), encoding="utf-8"),
    )
    monkeypatch.setattr(
        backfill.s5_pipeline,
        "backfill_s5",
        lambda client, instrument, days: seen.append(
            (backfill.s5_pipeline.S5_ROOT, instrument, days)
        )
        or {"instrument": instrument, "rows": 1},
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "oanda_s5_backfill_only.py",
            "--days",
            "2",
            "--pairs",
            "EUR_USD,GBP_USD",
            "--output-dir",
            str(output),
            "--summary",
            str(summary),
        ],
    )

    assert backfill.main() == 0
    assert seen == [
        (output.resolve(), "EUR_USD", 2),
        (output.resolve(), "GBP_USD", 2),
    ]
    assert json.loads(summary.read_text(encoding="utf-8"))["output_dir"] == str(
        output.resolve()
    )
