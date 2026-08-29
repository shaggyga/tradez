from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from trad import oanda_live_move_news_outcomes as base
from trad import oanda_live_move_news_outcomes_v2 as outcomes_v2
from trad import oanda_live_move_news_outcomes_v4 as outcomes_v4
from trad import oanda_live_move_news_snapshot_v6 as v6
from trad import oanda_live_move_news_snapshot_v7 as v7r2
from trad import oanda_live_move_news_snapshot_v7r3 as v7r3


def _row(
    instrument: str, start: str, end: str, *, primary: str, move_bps: float
) -> dict[str, object]:
    row: dict[str, object] = {
        "instrument": instrument,
        "start_utc": start,
        "end_utc": end,
        "move_direction": "up",
        "contract_id": v7r3.CONTRACT_ID,
        "factor_primary_token": primary,
        "factor_primary_method": (
            "causal_all68_currency_strength_fixed_onset_5m"
        ),
        "factor_primary_ambiguous": False,
        "move_bps": move_bps,
        "executable_net_pips": move_bps,
        "entry_spread_pips": 2.0,
        "entry_quote_fresh": False,
        "forward_shadow_arms": {"technical_continuation": 1},
        "research_only": True,
        "execution_eligible": False,
    }
    row["case_id"] = v7r3.stable_case_id(row)
    return row


def _observe(
    database: Path, rows: list[dict[str, object]], observed: str
) -> None:
    registry = v7r3.load_union_registry(database)
    v7r3.freeze_observed_primaries(rows, registry)
    assignment = v7r3.assign_transitive_factor_episodes(
        rows, registry, detected_utc=observed
    )
    result = v7r3.record_cases(
        database,
        rows,
        recorded_utc=observed,
        meter_database=database.with_name("meter.sqlite"),
        planned_merges=assignment["planned_merges"],
    )
    assert result["membership_conflict_total"] == 0
    assert result["graph_cycle"] is False


def _fragmented_then_bridged_database(path: Path) -> tuple[str, str]:
    chf = _row(
        "CHF_HKD",
        "2026-08-27T08:35:00+00:00",
        "2026-08-27T08:48:00+00:00",
        primary="HKD-",
        move_bps=8.0,
    )
    nzd = _row(
        "NZD_HKD",
        "2026-08-27T08:36:00+00:00",
        "2026-08-27T08:43:00+00:00",
        primary="HKD-",
        move_bps=7.0,
    )
    _observe(path, [chf], "2026-08-27T08:42:48+00:00")
    _observe(path, [nzd], "2026-08-27T08:48:41+00:00")
    _observe(path, [dict(chf), dict(nzd)], "2026-08-27T08:50:21+00:00")
    return str(chf["case_id"]), str(nzd["case_id"])


def test_v4r3_binds_only_v7r3_and_separate_artifacts() -> None:
    assert outcomes_v4.CASE_CONTRACT_ID == v7r3.CONTRACT_ID
    assert outcomes_v4.FACTOR_EPISODE_CONTRACT_ID == (
        v7r3.FACTOR_EPISODE_CONTRACT_ID
    )
    assert outcomes_v4.CASE_CONTRACT_ID != v7r2.CONTRACT_ID
    assert outcomes_v4.CONTRACT_ID != outcomes_v2.CONTRACT_ID
    assert outcomes_v4.DEFAULT_DATABASE == v7r3.DEFAULT_HISTORY
    assert outcomes_v4.DEFAULT_OUTPUT.name == "live_move_news_outcomes_v4r3.json"


def test_v4r3_validates_transitive_root_database(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r3.sqlite"
    _fragmented_then_bridged_database(database)
    result = outcomes_v4.validate_case_database(database)
    assert result["ok"] is True
    assert result["case_count"] == 2
    assert result["root_merge_count"] == 1
    assert result["raw_root_count"] == 2
    assert result["canonical_root_count"] == 1
    assert result["root_union_cycle"] is False


def test_v4r3_canonical_summary_counts_bridged_roots_once(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r3.sqlite"
    chf_case, nzd_case = _fragmented_then_bridged_database(database)
    connection = base.connect(database)
    try:
        for case_id, first_seen, pips in (
            (chf_case, "2026-08-27T08:42:48+00:00", 5.0),
            (nzd_case, "2026-08-27T08:48:41+00:00", -1.0),
        ):
            connection.execute(
                "INSERT INTO mover_case_outcomes VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"outcome-{case_id}",
                    first_seen,
                    case_id,
                    v7r3.CONTRACT_ID,
                    "technical_continuation",
                    5,
                    1,
                    "target",
                    "candle",
                    pips,
                    json.dumps({"case_id": case_id}),
                ),
            )
        connection.commit()
        cell = outcomes_v4.summarize_canonical(connection)[0]
    finally:
        connection.close()
    assert cell["raw_n"] == 2
    assert cell["win_rate"] == 0.5
    assert cell["average_after_cost_pips"] == 2.0
    assert cell["factor_representative_n"] == 1
    assert cell["factor_representative_win_rate"] == 1.0
    assert cell["factor_representative_average_after_cost_pips"] == 5.0


def test_v4r3_rejects_rejected_v7r2_lineage(tmp_path: Path) -> None:
    database = tmp_path / "cases_v7r2.sqlite"
    row = {
        "instrument": "CHF_HKD",
        "start_utc": "2026-08-27T08:35:00+00:00",
        "end_utc": "2026-08-27T08:48:00+00:00",
        "move_direction": "up",
        "contract_id": v7r2.CONTRACT_ID,
        "factor_primary_token": "HKD-",
        "factor_primary_method": "causal_all68_currency_strength_fixed_onset_5m",
        "factor_primary_ambiguous": False,
        "move_bps": 8.0,
        "executable_net_pips": 8.0,
    }
    v7r2.assign_overlap_factor_episodes([row])
    v7r2.record_cases(
        database,
        [row],
        recorded_utc="2026-08-27T08:42:48+00:00",
        meter_database=tmp_path / "meter.sqlite",
    )
    validation = outcomes_v4.validate_case_database(database)
    assert validation["ok"] is False
    assert validation["reason"] == "required_tables_missing"
    with pytest.raises(outcomes_v4.UpstreamIntegrityError):
        outcomes_v4.run(
            database=database,
            candle_root=tmp_path / "candles",
            output=tmp_path / "out.json",
            report=tmp_path / "out.md",
        )


def test_v7r3_and_v6r2_maturation_economics_are_equal() -> None:
    case = {
        "case_id": "economic-case",
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
        {
            "open_epoch": 100,
            "close_epoch": 160,
            "bid_close": 1.1001,
            "ask_close": 1.1003,
            "bid_high": 1.1004,
            "bid_low": 1.0999,
            "ask_high": 1.1006,
            "ask_low": 1.1001,
        },
        {
            "open_epoch": 400,
            "close_epoch": 460,
            "bid_close": 1.1007,
            "ask_close": 1.1009,
            "bid_high": 1.1009,
            "bid_low": 1.1005,
            "ask_high": 1.1011,
            "ask_low": 1.1007,
        },
    ]
    old = base.mature_path(
        dict(case, contract_id=v6.CONTRACT_ID),
        candles,
        arm="technical_continuation",
        side=1,
        horizon_min=5,
        outcome_contract_id=outcomes_v2.CONTRACT_ID,
    )
    new = base.mature_path(
        dict(case, contract_id=v7r3.CONTRACT_ID),
        candles,
        arm="technical_continuation",
        side=1,
        horizon_min=5,
        outcome_contract_id=outcomes_v4.CONTRACT_ID,
    )
    assert old is not None and new is not None
    keys = (
        "entry_price",
        "exit_price",
        "after_cost_pips",
        "mfe_pips",
        "mae_pips",
        "target_utc",
        "candle_utc",
    )
    assert tuple(old[key] for key in keys) == tuple(new[key] for key in keys)


def test_v4r3_is_inert_and_live_diagnostic_bound() -> None:
    source = Path(outcomes_v4.__file__).read_text(encoding="utf-8").lower()
    assert "requests." not in source
    assert "oanda-api-v20" not in source
    assert "can_place_orders=false" in source
    supervisor = (
        Path(outcomes_v4.__file__).resolve().parent
        / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert '"oanda_live_move_news_outcomes_v4.py"' in supervisor
    assert '"live_move_news_outcomes_v4r3.json"' in supervisor
