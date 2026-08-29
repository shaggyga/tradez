from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path

import oanda_wtd_live_move_reconstruction as audit


def base_case(
    instrument: str,
    start: int,
    end: int,
    side: int,
    move_bps: float,
) -> dict:
    return {
        "instrument": instrument,
        "start_epoch": start,
        "end_epoch": end,
        "start_utc": audit.census.iso_epoch(start),
        "end_utc": audit.census.iso_epoch(end),
        "move_direction": "up" if side > 0 else "down",
        "actual_side": side,
        "duration_minutes": (end - start) / 60,
        "move_bps": move_bps,
        "gross_pips": move_bps,
        "executable_net_pips": move_bps - 1,
        "history_case_id": f"{instrument}-{start}",
        "history_contract_id": "test",
        "first_recorded_utc": audit.census.iso_epoch(end + 1),
        "physical_version_count": 1,
    }


def test_retained_history_deduplicates_contract_versions_without_rewrite(tmp_path: Path):
    database = tmp_path / "cases.sqlite"
    connection = sqlite3.connect(database)
    connection.execute(
        """CREATE TABLE mover_cases(
               case_id TEXT PRIMARY KEY, first_recorded_utc TEXT NOT NULL,
               instrument TEXT NOT NULL, start_utc TEXT NOT NULL,
               end_utc TEXT NOT NULL, case_json TEXT NOT NULL)"""
    )
    start = audit.census.parse_epoch("2026-08-25T01:00:00Z")
    end = start + 300
    for index, contract in enumerate(("v4", "v5")):
        payload = {
            "contract_id": contract,
            "instrument": "EUR_HUF",
            "start_utc": audit.census.iso_epoch(start),
            "end_utc": audit.census.iso_epoch(end + index * 60),
            "move_direction": "up",
            "move_bps": 10 + index,
            "executable_net_pips": 8 + index,
        }
        connection.execute(
            "INSERT INTO mover_cases VALUES(?,?,?,?,?,?)",
            (
                contract,
                audit.census.iso_epoch(end + 10 + index),
                "EUR_HUF",
                payload["start_utc"],
                payload["end_utc"],
                json.dumps(payload),
            ),
        )
    connection.commit()
    connection.close()

    rows, meta = audit.load_retained_cases(
        database, start_epoch=start - 60, as_of_epoch=end + 600
    )

    assert meta["physical_record_count"] == 2
    assert meta["logical_raw_case_count"] == 1
    assert meta["preserved_duplicate_contract_record_count"] == 1
    assert rows[0]["history_contract_id"] == "v4"
    assert rows[0]["physical_version_count"] == 2
    assert sqlite3.connect(database).execute(
        "SELECT COUNT(*) FROM mover_cases"
    ).fetchone()[0] == 2


def test_causal_huf_zar_grid_becomes_two_exact_episodes():
    start = audit.census.parse_epoch("2026-08-27T04:15:00Z")
    huf_end = audit.census.parse_epoch("2026-08-27T05:10:00Z")
    zar_end = audit.census.parse_epoch("2026-08-27T05:14:00Z")
    rows = [
        base_case(instrument, start, end, 1, move)
        for instrument, end, move in (
            ("EUR_HUF", huf_end, 15.44),
            ("USD_HUF", huf_end, 14.27),
            ("EUR_ZAR", zar_end, 9.81),
            ("USD_ZAR", zar_end, 8.98),
        )
    ]
    surfaces = {
        (huf_end, 60): {
            "valid": True,
            "status": "ready",
            "observation_count": 68,
            "coverage_pct": 100.0,
            "currency_strength_bps": {
                "EUR": 1.99,
                "USD": 1.35,
                "HUF": -7.40,
            },
        },
        (zar_end, 60): {
            "valid": True,
            "status": "ready",
            "observation_count": 68,
            "coverage_pct": 100.0,
            "currency_strength_bps": {
                "EUR": 2.58,
                "USD": 2.07,
                "ZAR": -6.64,
            },
        },
    }

    audit.assign_primary_factors(rows, surfaces)
    snapshot_rows = audit.snapshot_representatives(rows)
    episodes = audit.build_factor_episodes(snapshot_rows)

    assert [row["factor_primary_token"] for row in rows] == [
        "HUF-",
        "HUF-",
        "ZAR-",
        "ZAR-",
    ]
    assert len(episodes) == 2
    assert {row["factor_episode_category"] for row in episodes} == {"exact"}
    assert {row["factor_primary_token"] for row in episodes} == {"HUF-", "ZAR-"}


def test_snapshot_reduction_collapses_same_factor_and_start_bucket_only():
    start = audit.census.parse_epoch("2026-08-27T04:00:00Z")
    rows = [
        base_case("USD_HUF", start, start + 5 * 60, 1, 10.0),
        base_case("EUR_HUF", start + 60, start + 5 * 60, 1, 20.0),
    ]
    for row in rows:
        row.update(
            {
                "first_recorded_utc": audit.census.iso_epoch(start + 5 * 60 + 1),
                "factor_primary_token": "HUF-",
                "factor_primary_method": "causal_all68_currency_strength",
                "factor_primary_ambiguous": False,
                "factor_fallback_bucket": int((row["start_epoch"] + 450) // 900),
            }
        )

    representatives = audit.snapshot_representatives(rows)

    assert len(representatives) == 1
    assert representatives[0]["instrument"] == "EUR_HUF"
    assert representatives[0]["snapshot_factor_member_count"] == 2


def test_snapshot_member_fallback_provenance_propagates_to_episode_category():
    start = audit.census.parse_epoch("2026-08-27T04:00:00Z")
    exact = base_case("USD_HUF", start, start + 5 * 60, 1, 20.0)
    fallback = base_case("EUR_HUF", start + 60, start + 5 * 60, 1, 10.0)
    for row, method in (
        (exact, "causal_all68_currency_strength"),
        (fallback, "time_bucket_token_recurrence_fallback"),
    ):
        row.update(
            {
                "first_recorded_utc": audit.census.iso_epoch(start + 5 * 60 + 1),
                "factor_primary_token": "HUF-",
                "factor_primary_method": method,
                "factor_primary_ambiguous": False,
                "factor_fallback_bucket": int((row["start_epoch"] + 450) // 900),
            }
        )

    episodes = audit.build_factor_episodes(audit.snapshot_representatives([exact, fallback]))

    assert len(episodes) == 1
    assert episodes[0]["factor_episode_category"] == "fallback"
    assert set(episodes[0]["factor_episode_member_methods"]) == {
        "causal_all68_currency_strength",
        "time_bucket_token_recurrence_fallback",
    }


def test_ambiguous_and_missing_currency_are_separate_fail_closed_categories():
    start = audit.census.parse_epoch("2026-08-27T04:00:00Z")
    end = start + 15 * 60
    ambiguous = base_case("EUR_HUF", start, end, 1, 12)
    missing = base_case("EUR_TRY", start + 1_800, end + 1_800, 1, 11)
    surfaces = {
        (end, 15): {
            "valid": True,
            "status": "ready",
            "observation_count": 68,
            "coverage_pct": 100.0,
            "currency_strength_bps": {"EUR": 2.0, "HUF": -2.0},
        },
        (end + 1_800, 15): {
            "valid": True,
            "status": "ready_with_logged_pair_exclusions",
            "observation_count": 64,
            "coverage_pct": 94.118,
            "currency_strength_bps": {"EUR": 2.0},
            "missing_currencies": ["TRY"],
        },
    }

    audit.assign_primary_factors([ambiguous, missing], surfaces)
    episodes = audit.build_factor_episodes([ambiguous, missing])

    assert ambiguous["factor_primary_ambiguous"] is True
    assert missing["factor_primary_method"] == "time_bucket_token_recurrence_fallback"
    assert Counter(row["factor_episode_category"] for row in episodes) == Counter(
        {"ambiguous": 1, "fallback": 1}
    )


def test_completed_m1_technical_state_excludes_bar_open_at_decision():
    start = 2_000
    candles = []
    for index in range(62):
        epoch = start - (62 - index) * 60
        price = 1.0 + index * 0.0001
        candles.append(
            {
                "epoch": epoch,
                "bid_open": price - 0.00005,
                "ask_open": price + 0.00005,
                "bid_high": price,
                "ask_high": price + 0.0001,
                "bid_low": price - 0.0001,
                "ask_low": price,
                "bid_close": price - 0.00005,
                "ask_close": price + 0.00005,
            }
        )
    candles.append(
        {
            **candles[-1],
            "epoch": start,
            "bid_open": 9.0,
            "ask_open": 9.0001,
        }
    )

    state = audit.causal_technical_at_start(
        candles, start_epoch=start, pip_size=0.0001
    )

    assert state["state"] == "available"
    assert audit.census.parse_epoch(state["last_bar_open_utc"]) == start - 60
    assert abs(state["return_15m_pips"]) < 100


def test_factor_analysis_universe_retains_raw_long_segment_but_excludes_episode():
    start = audit.census.parse_epoch("2026-08-27T01:00:00Z")
    intrahour = base_case("USD_HUF", start, start + 60 * 60, 1, 16.0)
    long_segment = base_case("ZAR_JPY", start, start + 61 * 60, 1, 80.0)

    factor_rows, universe = audit.factor_analysis_universe(
        [intrahour, long_segment]
    )
    intrahour["factor_primary_token"] = "HUF-"
    intrahour["factor_primary_method"] = "causal_all68_currency_strength"
    intrahour["factor_primary_ambiguous"] = False
    intrahour["factor_fallback_bucket"] = start // 300
    episodes = audit.build_factor_episodes(factor_rows)

    assert len(factor_rows) == 1
    assert factor_rows[0]["instrument"] == "USD_HUF"
    assert universe["factor_eligible_logical_case_count"] == 1
    assert universe["over_60m_logical_case_count_excluded"] == 1
    assert len(episodes) == 1
    assert all(row["instrument"] != "ZAR_JPY" for row in episodes)


def test_report_contains_every_material_episode():
    episodes = []
    for index, magnitude in enumerate((16.0, 15.0, 14.99)):
        row = base_case("EUR_USD", 1_700_000_000 + index * 1_000, 1_700_000_300 + index * 1_000, 1, magnitude)
        row.update(
            {
                "factor_episode_id": f"e{index}",
                "factor_episode_category": "exact",
                "factor_primary_token": f"X{index}+",
                "factor_primary_method": "causal_all68_currency_strength",
                "factor_strength_observation_count": 68,
                "broad_news_state": "no_relevant_source",
                "strict_news_state": "no_relevant_source",
                "technical_trend_15m_state": "aligned",
                "technical_breakout_20m_state": "inside",
                "top_broad_story": {},
            }
        )
        episodes.append(row)
    payload = {
        "generated_utc": "2026-08-27T06:00:00+00:00",
        "contract_id": audit.CONTRACT_ID,
        "universe": {
            "week_start_utc": "2026-08-24T04:00:00+00:00",
            "as_of_utc": "2026-08-27T06:00:00+00:00",
            "physical_record_count": 3,
            "logical_raw_case_count": 3,
            "preserved_duplicate_contract_record_count": 0,
            "factor_eligible_logical_case_count": 3,
            "over_60m_logical_case_count_excluded": 0,
            "invalid_duration_logical_case_count_excluded": 0,
        },
        "factor_assignment": {
            "snapshot_factor_representative_count": 3,
            "factor_episode_count": 3,
            "episode_categories": {"exact": 3},
        },
        "magnitude_thresholds": audit.threshold_counts(episodes),
        "news_mapping": {"broad": {}, "strict": {}},
        "technical_state": {
            "trend_15m_vs_move": {},
            "breakout_20m_vs_move": {},
            "exhaustion_60m": {},
        },
        "episodes": episodes,
    }

    report = audit.render_markdown(payload)

    assert "X0+" in report
    assert "X1+" in report
    assert "X2+" not in report
    assert payload["magnitude_thresholds"]["gte_15_bps"] == 2
