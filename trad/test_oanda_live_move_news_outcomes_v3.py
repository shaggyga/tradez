from __future__ import annotations

import copy
import json
import sqlite3
from pathlib import Path

import pytest

from trad import oanda_live_move_news_outcomes as base
from trad import oanda_live_move_news_outcomes_v2 as outcomes_v2
from trad import oanda_live_move_news_outcomes_v3 as outcomes_v3
from trad import oanda_live_move_news_snapshot_v6 as v6
from trad import oanda_live_move_news_snapshot_v7 as v7r2


def _case() -> dict[str, object]:
    return {
        "instrument": "USD_PLN",
        "start_utc": "2026-08-27T08:08:00+00:00",
        "end_utc": "2026-08-27T08:17:00+00:00",
        "move_direction": "up",
        "contract_id": v7r2.CONTRACT_ID,
        "factor_primary_token": "PLN-",
        "factor_primary_method": (
            "causal_all68_currency_strength_fixed_onset_5m"
        ),
        "factor_primary_ambiguous": False,
        "move_bps": 8.443,
        "executable_net_pips": 17.0,
        "entry_quote_fresh": False,
        "forward_shadow_arms": {"technical_continuation": 1},
        "research_only": True,
        "execution_eligible": False,
    }


def _v7r2_database(path: Path, meter: Path) -> dict[str, object]:
    row = _case()
    v7r2.assign_overlap_factor_episodes([row])
    result = v7r2.record_cases(
        path,
        [row],
        recorded_utc="2026-08-27T08:42:00+00:00",
        meter_database=meter,
    )
    assert result["membership_conflict_total"] == 0
    return row


def _candle(
    epoch: int,
    bid_close: float,
    ask_close: float,
    bid_high: float,
    bid_low: float,
    ask_high: float,
    ask_low: float,
) -> dict[str, object]:
    return {
        "open_epoch": epoch,
        "close_epoch": epoch + 60,
        "bid_close": bid_close,
        "ask_close": ask_close,
        "bid_high": bid_high,
        "bid_low": bid_low,
        "ask_high": ask_high,
        "ask_low": ask_low,
    }


def test_v3r2_binds_only_v7r2_and_uses_separate_artifacts() -> None:
    assert outcomes_v3.CASE_CONTRACT_ID == v7r2.CONTRACT_ID
    assert outcomes_v3.FACTOR_EPISODE_CONTRACT_ID == (
        v7r2.FACTOR_EPISODE_CONTRACT_ID
    )
    assert outcomes_v3.CASE_CONTRACT_ID != outcomes_v2.CASE_CONTRACT_ID
    assert outcomes_v3.CONTRACT_ID != outcomes_v2.CONTRACT_ID
    assert outcomes_v3.DEFAULT_DATABASE != outcomes_v2.DEFAULT_DATABASE
    assert outcomes_v3.DEFAULT_OUTPUT != outcomes_v2.DEFAULT_OUTPUT
    assert outcomes_v3.DEFAULT_REPORT != outcomes_v2.DEFAULT_REPORT


def test_v3r2_validates_exact_case_and_factor_membership(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r2.sqlite"
    _v7r2_database(database, tmp_path / "meter.sqlite")
    validation = outcomes_v3.validate_case_database(database)
    assert validation["ok"] is True
    assert validation["case_registry_count"] == 1
    assert validation["factor_registry_count"] == 1
    assert validation["membership_mismatch_count"] == 0
    assert validation["membership_conflict_count"] == 0


def test_v3r2_rejects_v6r2_database(tmp_path: Path) -> None:
    database = tmp_path / "cases_v6r2.sqlite"
    connection = v6.connect_history(database)
    connection.close()
    validation = outcomes_v3.validate_case_database(database)
    assert validation["ok"] is False
    assert "required_tables_missing" in validation["reason"]
    with pytest.raises(outcomes_v3.UpstreamIntegrityError):
        outcomes_v3.run(
            database=database,
            candle_root=tmp_path / "candles",
            output=tmp_path / "out.json",
            report=tmp_path / "out.md",
        )


def test_v3r2_rejects_recorded_membership_conflict(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r2.sqlite"
    meter = tmp_path / "meter.sqlite"
    row = _v7r2_database(database, meter)
    changed = copy.deepcopy(row)
    changed["factor_episode_id"] = "deliberate_conflict"
    v7r2.record_cases(
        database,
        [changed],
        recorded_utc="2026-08-27T08:43:00+00:00",
        meter_database=meter,
    )
    validation = outcomes_v3.validate_case_database(database)
    assert validation["ok"] is False
    assert validation["membership_conflict_count"] == 1
    assert "membership_conflicts" in validation["failures"]


def test_v3r2_run_ignores_non_v7r2_case_rows(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r2.sqlite"
    _v7r2_database(database, tmp_path / "meter.sqlite")
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO mover_cases VALUES(?,?,?,?,?,?)",
            (
                "foreign-v6-case",
                "2026-08-27T08:42:00+00:00",
                "EUR_USD",
                "2026-08-27T08:00:00+00:00",
                "2026-08-27T08:05:00+00:00",
                json.dumps({"contract_id": v6.CONTRACT_ID}),
            ),
        )
    result = outcomes_v3.run(
        database=database,
        candle_root=tmp_path / "candles",
        output=tmp_path / "outcomes_v3r2.json",
        report=tmp_path / "outcomes_v3r2.md",
    )
    assert result["case_count"] == 1
    assert result["case_contract_id"] == v7r2.CONTRACT_ID
    assert result["contract_id"] == outcomes_v3.CONTRACT_ID
    assert result["upstream_integrity"]["ok"] is True
    assert result["retained_outcome_count"] == 0
    assert result["execution_eligible"] is False


def test_v7r2_and_v6r2_path_economics_are_equivalent() -> None:
    common = {
        "case_id": "same-economic-case",
        "instrument": "EUR_USD",
        "observation_utc": "1970-01-01T00:01:40Z",
        "entry_quote_fresh": True,
        "entry_bid": 1.1000,
        "entry_ask": 1.1002,
        "entry_pip": 0.0001,
        "factor_episode_id": "factor",
        "factor_representative": True,
    }
    candles = [
        _candle(100, 1.1001, 1.1003, 1.1004, 1.0999, 1.1006, 1.1001),
        _candle(400, 1.1007, 1.1009, 1.1009, 1.1005, 1.1011, 1.1007),
    ]
    old_case = dict(common, contract_id=v6.CONTRACT_ID)
    new_case = dict(common, contract_id=v7r2.CONTRACT_ID)
    old = base.mature_path(
        old_case,
        candles,
        arm="technical_continuation",
        side=1,
        horizon_min=5,
        outcome_contract_id=outcomes_v2.CONTRACT_ID,
    )
    new = base.mature_path(
        new_case,
        candles,
        arm="technical_continuation",
        side=1,
        horizon_min=5,
        outcome_contract_id=outcomes_v3.CONTRACT_ID,
    )
    assert old is not None and new is not None
    economic_fields = {
        "entry_price",
        "exit_price",
        "after_cost_pips",
        "mfe_pips",
        "mae_pips",
        "target_utc",
        "candle_utc",
        "maturation_clock_lag_sec",
    }
    assert {key: old[key] for key in economic_fields} == {
        key: new[key] for key in economic_fields
    }


def test_v3r2_has_no_broker_authorization_or_live_binding_surface() -> None:
    source = Path(outcomes_v3.__file__).read_text(encoding="utf-8").lower()
    assert "requests." not in source
    assert "oanda-api-v20" not in source
    assert "can_place_orders\"] = false" in source
    supervisor = (
        Path(outcomes_v3.__file__).resolve().parent
        / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert '"oanda_live_move_news_outcomes_v3.py"' not in supervisor
