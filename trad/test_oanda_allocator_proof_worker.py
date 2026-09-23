from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from trad.oanda_allocator_proof_worker import run_once


def test_worker_forwards_only_local_research_inputs(tmp_path):
    paths = {
        "database": tmp_path / "allocator.sqlite",
        "state": tmp_path / "allocator.json",
        "config": tmp_path / "config.json",
        "signal_database": tmp_path / "signals.sqlite",
        "quote_database": tmp_path / "quotes.sqlite",
        "account": tmp_path / "account.json",
        "executor": tmp_path / "executor.json",
    }
    expected = {
        "generated_utc": "2026-09-02T06:00:00+00:00",
        "research_only": True,
        "can_place_orders": False,
    }
    with patch(
        "trad.oanda_allocator_proof_worker.run_allocator_cycle",
        return_value=expected,
    ) as runner:
        observed = run_once(**paths)

    assert observed == expected
    runner.assert_called_once_with(
        database_path=Path(paths["database"]),
        state_path=Path(paths["state"]),
        config_path=Path(paths["config"]),
        signal_database=Path(paths["signal_database"]),
        quote_database=Path(paths["quote_database"]),
        account_path=Path(paths["account"]),
        executor_path=Path(paths["executor"]),
    )


def test_worker_source_has_no_order_submission_surface():
    source = Path(__file__).with_name("oanda_allocator_proof_worker.py").read_text(
        encoding="utf-8"
    )
    assert "requests." not in source
    assert "create_order" not in source
    assert "submit_order" not in source
    assert "close_trade" not in source
