import json
import sqlite3
from pathlib import Path

import oanda_live_move_news_outcomes as outcomes


def candle(epoch, bid_close, ask_close, bid_high, bid_low, ask_high, ask_low):
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


def test_long_outcome_uses_entry_ask_and_exit_bid():
    case = {
        "case_id": "case-1",
        "contract_id": outcomes.CASE_CONTRACT_ID,
        "instrument": "EUR_USD",
        "observation_utc": "1970-01-01T00:01:40Z",
        "entry_quote_fresh": True,
        "entry_bid": 1.1000,
        "entry_ask": 1.1002,
        "entry_pip": 0.0001,
        "factor_representative": True,
    }
    rows = [
        candle(100, 1.1001, 1.1003, 1.1004, 1.0999, 1.1006, 1.1001),
        candle(400, 1.1007, 1.1009, 1.1009, 1.1005, 1.1011, 1.1007),
    ]
    result = outcomes.mature_path(
        case, rows, arm="technical_continuation", side=1, horizon_min=5
    )
    assert result is not None
    assert result["after_cost_pips"] == 5.0
    assert result["mfe_pips"] == 7.0


def test_short_outcome_uses_entry_bid_and_exit_ask():
    case = {
        "case_id": "case-2",
        "contract_id": outcomes.CASE_CONTRACT_ID,
        "instrument": "USD_JPY",
        "observation_utc": "1970-01-01T00:01:40Z",
        "entry_quote_fresh": True,
        "entry_bid": 150.00,
        "entry_ask": 150.02,
        "entry_pip": 0.01,
    }
    rows = [candle(400, 149.92, 149.94, 149.95, 149.90, 149.97, 149.92)]
    result = outcomes.mature_path(case, rows, arm="broad", side=-1, horizon_min=5)
    assert result is not None
    assert result["after_cost_pips"] == 6.0


def test_outcome_table_is_append_only(tmp_path):
    path = tmp_path / "cases.sqlite"
    connection = outcomes.connect(path)
    try:
        connection.execute(
            "INSERT INTO mover_case_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ("o1", "now", "c1", outcomes.CASE_CONTRACT_ID, "arm", 5, 1,
             "target", "candle", 1.0, "{}"),
        )
        connection.commit()
        try:
            connection.execute("DELETE FROM mover_case_outcomes")
        except sqlite3.DatabaseError as exc:
            assert "append_only" in str(exc)
        else:
            raise AssertionError("outcome history allowed deletion")
    finally:
        connection.close()


def test_outcome_worker_is_supervised_without_execution_arguments():
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    assert '-Name "live_move_news_outcomes"' in supervisor
    block = supervisor.split('-Name "live_move_news_outcomes"', 1)[1].split(
        '$managed += Start-ManagedProcess', 1
    )[0]
    assert "order" not in block.lower()
    assert "authorization" not in block.lower()


def test_all68_m1_archive_refresh_matches_forward_outcome_horizons():
    supervisor = (
        Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1"
    ).read_text(encoding="utf-8")
    block = supervisor.split('-Name "all68_m1_forward_archive"', 1)[1].split(
        '$managed += Start-ManagedProcess', 1
    )[0]
    assert '"--interval-sec", "300"' in block
    assert "MaxAgeSec = 900" in block
    assert '"--backfill-requests-per-pair", "0"' in block


def test_summary_separates_raw_pairs_from_factor_representatives(tmp_path):
    path = tmp_path / "cases.sqlite"
    connection = outcomes.connect(path)
    try:
        for identifier, pips, representative in (
            ("o1", 4.0, True),
            ("o2", -2.0, False),
            ("o3", 8.0, True),
        ):
            case_payload = json.dumps(
                {
                    "contract_id": outcomes.CASE_CONTRACT_ID,
                    "entry_spread_pips": 2.0,
                    "factor_episode_id": (
                        "factor-o1" if identifier in {"o1", "o2"} else "factor-o3"
                    ),
                    "factor_representative": representative,
                    "move_bps": 2.0,
                }
            )
            connection.execute(
                "INSERT INTO mover_cases VALUES (?,?,?,?,?,?)",
                (identifier, "now", "EUR_USD", "start", "end", case_payload),
            )
            payload = json.dumps({"factor_representative": representative})
            connection.execute(
                "INSERT INTO mover_case_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (identifier, "now", identifier, outcomes.CASE_CONTRACT_ID, "arm", 5,
                 1, "target", "candle", pips, payload),
            )
        connection.commit()
        cell = outcomes.summarize(connection)[0]
        assert cell["raw_n"] == 3
        assert cell["cost_bucket"] == "liquid_le_2p"
        assert cell["win_rate"] == 0.666667
        assert cell["average_after_cost_pips"] == 3.333333
        assert cell["factor_representative_n"] == 2
        assert cell["factor_representative_win_rate"] == 1.0
        assert cell["factor_representative_average_after_cost_pips"] == 6.0
        assert cell["factor_representative_average_after_cost_spread_multiple"] == 3.0
    finally:
        connection.close()


def test_summary_counts_one_earliest_row_when_factor_representative_changes(tmp_path):
    connection = outcomes.connect(tmp_path / "cases.sqlite")
    try:
        for identifier, first_seen, pips in (
            ("early", "2026-08-25T01:00:00Z", -1.0),
            ("later", "2026-08-25T01:05:00Z", 8.0),
        ):
            case_payload = json.dumps(
                {
                    "contract_id": outcomes.CASE_CONTRACT_ID,
                    "entry_spread_pips": 2.0,
                    "factor_episode_id": "same-factor",
                    "factor_representative": True,
                    "move_bps": 5.0,
                }
            )
            connection.execute(
                "INSERT INTO mover_cases VALUES (?,?,?,?,?,?)",
                (identifier, first_seen, "EUR_USD", "start", "end", case_payload),
            )
            connection.execute(
                "INSERT INTO mover_case_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (identifier, "now", identifier, outcomes.CASE_CONTRACT_ID, "arm", 5,
                 1, "target", "candle", pips,
                 json.dumps({"factor_representative": True})),
            )
        connection.commit()
        cell = outcomes.summarize(connection)[0]
        assert cell["raw_n"] == 2
        assert cell["factor_representative_n"] == 1
        assert cell["factor_representative_win_rate"] == 0.0
        assert cell["factor_representative_average_after_cost_pips"] == -1.0
    finally:
        connection.close()


def test_archive_coverage_separates_future_path_latency_from_outcomes():
    cases = [
        {
            "instrument": "EUR_USD",
            "observation_utc": "1970-01-01T00:01:40Z",
            "entry_quote_fresh": True,
            "forward_shadow_arms": {"technical": 1, "strict": 0},
        }
    ]
    candles = {
        "EUR_USD": [candle(400, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)]
    }
    result = outcomes.archive_coverage(cases, candles, generated_epoch=500)
    assert result["instrument_count"] == 1
    assert result["potential_arm_horizon_count"] == 4
    assert result["pending_archive_arm_horizon_count"] == 3
    assert result["median_archive_lag_sec"] == 40.0


def test_run_skips_existing_case_arm_horizons_before_path_evaluation(
    monkeypatch, tmp_path: Path
):
    database = tmp_path / "cases.sqlite"
    connection = outcomes.connect(database)
    try:
        case = {
            "case_id": "case-existing",
            "contract_id": outcomes.CASE_CONTRACT_ID,
            "instrument": "EUR_USD",
            "observation_utc": "2026-08-25T00:00:00+00:00",
            "entry_quote_fresh": True,
            "entry_spread_pips": 1.5,
            "forward_shadow_arms": {"technical": 1},
        }
        connection.execute(
            "INSERT INTO mover_cases VALUES (?,?,?,?,?,?)",
            (
                case["case_id"],
                case["observation_utc"],
                case["instrument"],
                case["observation_utc"],
                case["observation_utc"],
                json.dumps(case),
            ),
        )
        for horizon in outcomes.HORIZONS_MIN:
            payload = {
                "factor_representative": True,
                "factor_episode_id": "factor-existing",
            }
            connection.execute(
                "INSERT INTO mover_case_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"existing-{horizon}",
                    "2026-08-25T01:00:00+00:00",
                    case["case_id"],
                    outcomes.CASE_CONTRACT_ID,
                    "technical",
                    horizon,
                    1,
                    "target",
                    "candle",
                    1.0,
                    json.dumps(payload),
                ),
            )
        connection.commit()
    finally:
        connection.close()

    monkeypatch.setattr(outcomes, "read_tail_candles", lambda path: [])

    def unexpected_maturation(*args, **kwargs):
        raise AssertionError("an already-matured path was evaluated again")

    monkeypatch.setattr(outcomes, "mature_path", unexpected_maturation)
    result = outcomes.run(
        database=database,
        candle_root=tmp_path / "candles",
        output=tmp_path / "outcomes.json",
        report=tmp_path / "outcomes.md",
    )
    assert result["inserted_outcome_count"] == 0
    assert result["retained_outcome_count"] == len(outcomes.HORIZONS_MIN)
    assert result["skipped_existing_outcome_count"] == len(outcomes.HORIZONS_MIN)
