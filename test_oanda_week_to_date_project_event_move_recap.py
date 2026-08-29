from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import oanda_week_to_date_project_event_move_recap as recap


UTC = dt.timezone.utc


def _snapshot(payload: dict) -> dict:
    return {
        "path": "fixture.json",
        "sha256": "0" * 64,
        "bytes": 2,
        "modified_utc": "2026-08-27T08:00:00+00:00",
        "state_clock_utc": payload.get("generated_utc") or payload.get("time"),
        "available": True,
        "cutoff_state": "included",
        "payload": payload,
    }


def _snapshots() -> dict:
    return {
        "account": _snapshot(
            {
                "time": "2026-08-27T08:00:00+00:00",
                "accounts": [
                    {
                        "account_id": "practice-007",
                        "env": "practice",
                        "balance": "41.0",
                        "NAV": "41.0",
                        "pl": "-9.0",
                        "unrealizedPL": "0",
                        "openTradeCount": 0,
                        "pendingOrderCount": 0,
                        "marginUsed": "0",
                        "ok": True,
                    }
                ],
            }
        ),
        "news_outcome": _snapshot(
            {
                "recorded_utc": "2026-08-27T08:00:00+00:00",
                "summary": {
                    "current_effective_theses": 10,
                    "wins": 1,
                    "misses_or_gaps": 9,
                    "reason_counts": {"strict_signal_absence": 5},
                },
            }
        ),
        "source_coverage": _snapshot(
            {
                "as_of_utc": "2026-08-27T08:00:00+00:00",
                "currency_count": 21,
                "pair_count": 68,
                "all_currencies_configured": True,
                "all_pairs_emitted": True,
                "pair_coverage_tier_counts": {"TWO_LEG_LIVE_DIRECT": 68},
            }
        ),
        "central_bank_coverage": _snapshot(
            {
                "currency_summary": {
                    "configured_complete": 21,
                    "release_operational": 21,
                    "release_healthy": 20,
                },
                "pair_summary": {"both_legs_operational": 68},
            }
        ),
        "economic_feed_completeness": _snapshot(
            {
                "currency_count": 21,
                "future_event_clock_count": 21,
                "structured_numeric_parser_count": 21,
                "actual_observation_currency_count": 21,
                "prospective_actual_observation_currency_count": 0,
                "causal_consensus_currency_count": 0,
                "prospective_surprise_ready_count": 0,
            }
        ),
        "macro_consensus_access": _snapshot(
            {"status": "blocked", "causal_consensus_provider_accessible": False}
        ),
        "daily_rate_context": _snapshot(
            {
                "totals": {"currencies": 8, "prospective_rows": 50},
                "intraday_rate_confirmation": False,
            }
        ),
        "narrative_meter": _snapshot(
            {
                "meter_contract_id": "v12",
                "generated_utc": "2026-08-27T08:00:00+00:00",
                "sealed_through_utc": "2026-08-27T07:55:00+00:00",
                "currency_count": 21,
                "instrument_count": 68,
                "integrity_status": "ok",
                "research_only": True,
                "execution_eligible": False,
            }
        ),
        "clock_integrity": _snapshot({"status": "ok"}),
        "project_integrity": _snapshot({"status": "ok", "failures": []}),
        "storage_headroom": _snapshot(
            {"status": "ok", "disk": {"free_gib": 100.0}}
        ),
    }


def _response(horizon: int, net: float | None) -> dict:
    if net is None:
        return {
            "horizon_minutes": horizon,
            "maturity_utc": "2026-08-27T09:05:00Z",
            "maturity_state": "not_matured_at_frozen_cutoff",
            "representative_path": None,
            "post_observation_path": None,
        }
    return {
        "horizon_minutes": horizon,
        "maturity_utc": "2026-08-27T08:35:00Z",
        "maturity_state": "matured_at_frozen_cutoff",
        "currency_factor_bps": 8.0,
        "currency_factor_rank_from_strongest": 1,
        "currency_factor_extreme_rank": 1,
        "usable_pair_count": 68,
        "cost_clearing_factor_consistent_pair_count": 2,
        "representative_path": {
            "instrument": "USD_JPY",
            "observed_side": "long",
            "start_utc": "2026-08-27T08:30:00Z",
            "end_utc": "2026-08-27T08:35:00Z",
            "midpoint_move_pips": 10.0,
            "observed_after_cost_pips": net,
            "entry_spread_pips": 1.5,
            "liquidity_cost_bucket": "liquid",
            "factor_consistent": True,
            "source_currency_dominant": True,
        },
        "post_observation_path": {"observed_after_cost_pips": net},
    }


def _official() -> dict:
    event = {
        "event_episode_id": "clock-1",
        "event_id": "event-1",
        "event_utc": "2026-08-27T08:30:00Z",
        "clock_basis": "scheduled_release_clock",
        "first_seen_utc": "2026-08-27T08:30:20Z",
        "source_latency_seconds": 20.0,
        "source_id": "official",
        "source_url": "https://example.invalid",
        "currency": "USD",
        "category": "growth_release",
        "headline": "Official release",
        "actual_value": 2.0,
        "consensus_value": 1.0,
        "consensus_observed_before_release": True,
        "previous_value": 0.5,
        "source_score": 1.0,
        "source_change_information": True,
        "supporting_statistical_artifact": False,
        "direction_state": "source_direction_aligned",
        "calendar_only": False,
        "responses": [_response(5, 8.5), _response(60, None)],
        "best_retrospective_response": _response(5, 8.5),
    }
    return {
        "contract_id": "official-v2",
        "deduplicated_official_source_item_count": 1,
        "cost_clearing_event_count": 1,
        "strict_attribution_candidate_count": 1,
        "direction_resolved_count": 1,
        "direction_aligned_count": 1,
        "direction_opposed_count": 0,
        "event_episode_representatives": [event],
    }


def _move(factor: str, magnitude: float, strict: str) -> dict:
    return {
        "factor_episode_id": factor,
        "factor_primary_token": factor,
        "factor_episode_category": "exact",
        "factor_episode_member_count": 1,
        "instrument": "USD_JPY",
        "move_direction": "up",
        "start_utc": "2026-08-27T08:00:00Z",
        "end_utc": "2026-08-27T08:05:00Z",
        "duration_minutes": 5,
        "move_bps": magnitude,
        "gross_pips": magnitude,
        "executable_net_pips": magnitude - 2,
        "broad_news_state": "aligned",
        "broad_news_side": "long",
        "strict_news_state": strict,
        "strict_news_side": "long" if strict == "aligned" else "neutral",
        "strict_news_story_count": 1 if strict == "aligned" else 0,
        "technical_trend_15m_state": "aligned",
        "technical_breakout_20m_state": "inside",
        "technical_exhaustion_60m_state": "none",
        "causal_m1_technical_state": {"spread_pips": 2.0},
        "top_broad_story": {"headline": "Official release"},
    }


def _moves() -> dict:
    episodes = [_move("USD+", 20.0, "aligned"), _move("JPY-", 8.0, "neutral_or_conflicted")]
    return {
        "contract_id": "moves-v3",
        "factor_contract_id": "factor-v3",
        "universe": {
            "physical_record_count": 3,
            "logical_raw_case_count": 2,
            "clear_move_definition": "fixture",
            "live_selection_scope": "fixture top movers",
        },
        "factor_assignment": {"factor_episode_count": 2},
        "magnitude_thresholds": {
            "gte_5_bps": 2,
            "gte_10_bps": 1,
            "gte_15_bps": 1,
            "gte_20_bps": 1,
            "gte_25_bps": 0,
        },
        "news_mapping": {
            "broad": {"aligned": 2},
            "strict": {"aligned": 1, "neutral_or_conflicted": 1},
        },
        "technical_state": {
            "trend_15m_vs_move": {"aligned": 2},
            "breakout_20m_vs_move": {"inside": 2},
        },
        "episodes": episodes,
    }


def test_week_start_uses_new_york_monday_not_utc_monday():
    cutoff = dt.datetime(2026, 8, 27, 9, tzinfo=UTC)
    assert recap.local_week_start(cutoff).isoformat() == "2026-08-24T00:00:00-04:00"


def test_snapshot_after_frozen_cutoff_is_excluded(tmp_path: Path):
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps({"generated_utc": "2026-08-27T09:01:00Z", "status": "ok"}),
        encoding="utf-8",
    )
    result = recap.capture_json_snapshot(
        path, dt.datetime(2026, 8, 27, 9, tzinfo=UTC)
    )
    assert result["cutoff_state"] == "excluded_after_cutoff"
    assert result["available"] is False
    assert result["payload"] == {}


def test_account_log_fallback_selects_latest_record_not_after_cutoff(tmp_path: Path):
    log = tmp_path / "account_snapshot_007_supervised_fixture.out.log"
    rows = [
        {
            "event": "account_snapshot",
            "time": "2026-08-27T12:59:00Z",
            "account_count": 1,
            "ok_count": 1,
            "nav": 41.5,
            "balance": 41.5,
            "pl": -8.4,
            "unrealizedPL": 0.0,
            "openTradeCount": 0,
            "pendingOrderCount": 0,
        },
        {
            "event": "account_snapshot",
            "time": "2026-08-27T13:01:00Z",
            "account_count": 1,
            "ok_count": 1,
            "nav": 99.0,
            "balance": 99.0,
            "pl": 50.0,
            "unrealizedPL": 0.0,
            "openTradeCount": 0,
            "pendingOrderCount": 0,
        },
    ]
    log.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    snapshot = recap.capture_account_log_snapshot(
        dt.datetime(2026, 8, 27, 13, tzinfo=UTC), tmp_path
    )
    assert snapshot is not None
    assert snapshot["cutoff_state"] == "included_historical_account_log_record"
    assert snapshot["state_clock_utc"] == "2026-08-27T12:59:00+00:00"
    account = snapshot["payload"]["accounts"][0]
    assert account["NAV"] == 41.5
    assert account["account_id"] == recap.PRACTICE_ACCOUNT_ID


def test_v12_fallback_uses_only_bucket_sealed_by_cutoff(tmp_path: Path):
    path = tmp_path / "meter.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE bucket_seals(
            meter_contract_id TEXT, clock_utc TEXT, sealed_at_utc TEXT,
            seal_grace_seconds INTEGER, currency_row_count INTEGER,
            input_story_count INTEGER
        );
        CREATE TABLE currency_meter(
            meter_contract_id TEXT, clock_utc TEXT, currency TEXT,
            created_utc TEXT
        );
        CREATE TABLE seal_integrity_events(
            meter_contract_id TEXT, detected_utc TEXT
        );
        CREATE TABLE meter_contract_registry(
            meter_contract_id TEXT, research_only INTEGER,
            execution_eligible INTEGER
        );
        """
    )
    contract = "v12-fixture"
    connection.execute(
        "INSERT INTO meter_contract_registry VALUES(?,?,?)", (contract, 1, 0)
    )
    connection.executemany(
        "INSERT INTO bucket_seals VALUES(?,?,?,?,?,?)",
        [
            (contract, "2026-08-27T12:55:00+00:00", "2026-08-27T12:57:00+00:00", 60, 21, 3),
            # Same market clock as the cutoff, but it was not known until later.
            (contract, "2026-08-27T13:00:00+00:00", "2026-08-27T13:01:00+00:00", 60, 21, 4),
        ],
    )
    currencies = [f"C{index:02d}" for index in range(21)]
    connection.executemany(
        "INSERT INTO currency_meter VALUES(?,?,?,?)",
        [
            (contract, "2026-08-27T12:55:00+00:00", currency, "2026-08-27T12:57:00+00:00")
            for currency in currencies
        ],
    )
    connection.commit()
    connection.close()
    snapshot = recap.capture_narrative_meter_sqlite_snapshot(
        dt.datetime(2026, 8, 27, 13, tzinfo=UTC), path
    )
    assert snapshot is not None
    assert snapshot["cutoff_state"] == "included_historical_sealed_meter_bucket"
    assert snapshot["payload"]["sealed_through_utc"] == "2026-08-27T12:55:00+00:00"
    assert snapshot["payload"]["currency_count"] == 21
    assert snapshot["payload"]["instrument_count"] == 68
    assert snapshot["payload"]["integrity_status"] == "ok"


def test_collector_log_fallback_recovers_both_coverage_summaries_before_cutoff(
    tmp_path: Path,
):
    log = tmp_path / "local_news_sentiment_supervised_fixture.out.log"

    def row(clock: str, currencies: int, pairs: int) -> dict:
        return {
            "generated_utc": clock,
            "heartbeat_utc": clock,
            "collector_contract_id": "collector-v1",
            "schema_version": "news-v3",
            "source_coverage": {
                "all_currencies_configured": currencies == 21,
                "currency_count": currencies,
                "pair_count": pairs,
                "pair_coverage_tier_counts": {"TWO_LEG_LIVE_DIRECT": pairs},
            },
            "official_central_bank_coverage": {
                "contract_complete": currencies == 21,
                "minimum_operational_complete": currencies == 21,
                "fully_healthy": False,
                "currency_summary": {
                    "expected": 21,
                    "configured_complete": currencies,
                    "release_operational": currencies,
                    "release_healthy": currencies - 1,
                },
                "pair_summary": {
                    "expected": 68,
                    "emitted": pairs,
                    "both_legs_operational": pairs,
                },
                "global_blockers": [],
            },
        }

    log.write_text(
        "\n".join(
            json.dumps(item)
            for item in (
                row("2026-08-27T12:59:00Z", 21, 68),
                row("2026-08-27T13:01:00Z", 2, 3),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    snapshots = recap.capture_collector_log_source_snapshots(
        dt.datetime(2026, 8, 27, 13, tzinfo=UTC), tmp_path
    )
    broad = snapshots["source_coverage"]
    central = snapshots["central_bank_coverage"]
    assert broad["state_clock_utc"] == "2026-08-27T12:59:00+00:00"
    assert broad["payload"]["currency_count"] == 21
    assert broad["payload"]["pair_count"] == 68
    assert broad["payload"]["all_pairs_emitted"] is True
    assert central["payload"]["currency_summary"]["release_operational"] == 21
    assert central["payload"]["pair_summary"]["both_legs_operational"] == 68
    assert central["payload"]["pre_cutoff_collector_history"] is True
    assert central["payload"]["non_event_structural_snapshot"] is False


def test_composition_is_dynamic_and_preserves_strict_distinctions():
    cutoff = dt.datetime(2026, 8, 27, 9, tzinfo=UTC)
    report = recap.compose_report(
        cutoff=cutoff,
        compiled_utc=cutoff,
        official=_official(),
        moves=_moves(),
        snapshots=_snapshots(),
        upstream_sources=[],
        live_cases=[],
    )
    counts = report["executive_counts"]
    assert counts["independent_official_event_clocks"] == 1
    assert counts["causal_pre_release_consensus_observations"] == 1
    assert counts["factor_deduplicated_move_episodes"] == 2
    assert counts["material_factor_episodes"] == 1
    assert counts["strict_publishable_directional_move_matches"] == 1
    assert len(report["movement_census"]["material_episodes"]) == 1
    fixed = report["official_event_clocks"][0]["fixed_horizon_responses"]
    assert fixed[1]["maturity_state"] == "not_matured_at_frozen_cutoff"
    assert report["supported_decision"] == "no_trade"
    assert report["can_place_orders"] is False


def test_write_report_emits_verifiable_md_json_sha256(tmp_path: Path):
    cutoff = dt.datetime(2026, 8, 27, 9, tzinfo=UTC)
    report = recap.compose_report(
        cutoff=cutoff,
        compiled_utc=cutoff,
        official=_official(),
        moves=_moves(),
        snapshots=_snapshots(),
        upstream_sources=[],
        live_cases=[],
    )
    artifacts = recap.write_report(report, tmp_path)
    json_path = Path(artifacts["json"])
    markdown_path = Path(artifacts["markdown"])
    hash_path = Path(artifacts["sha256"])
    assert json_path.exists() and markdown_path.exists() and hash_path.exists()
    assert "Official release" in markdown_path.read_text(encoding="utf-8")
    lines = hash_path.read_text(encoding="ascii").splitlines()
    expected_md = hashlib.sha256(markdown_path.read_bytes()).hexdigest()
    expected_json = hashlib.sha256(json_path.read_bytes()).hexdigest()
    assert lines == [
        f"{expected_md} *{markdown_path.name}",
        f"{expected_json} *{json_path.name}",
    ]


def test_compiler_source_has_no_execution_or_process_control_surface():
    source = (recap.ROOT / "oanda_week_to_date_project_event_move_recap.py").read_text(
        encoding="utf-8"
    )
    assert "import requests" not in source
    assert "import subprocess" not in source
    assert '"can_place_orders": False' in source
    assert '"can_promote": False' in source


def test_case_discovery_includes_release_case_roots_but_not_generated_recap(
    tmp_path: Path, monkeypatch
):
    live = tmp_path / "live"
    major = tmp_path / "major"
    output = tmp_path / "recap"
    nested = output / "us_release"
    for directory in (live, major, nested):
        directory.mkdir(parents=True, exist_ok=True)
    (live / "LIVE.md").write_text("# Live case\n", encoding="utf-8")
    (major / "STATS.md").write_text("# Stats case\n", encoding="utf-8")
    (nested / "US.md").write_text("# US case\n", encoding="utf-8")
    (output / "WEEK_TO_DATE_PROJECT_EVENT_MOVE_RECAP.md").write_text(
        "# Generated recap\n", encoding="utf-8"
    )
    monkeypatch.setattr(recap, "LIVE_CASE_DIRECTORY", live)
    monkeypatch.setattr(recap, "MAJOR_MOVE_CASE_DIRECTORY", major)
    monkeypatch.setattr(recap, "OUTPUT_DIRECTORY", output)
    start = dt.datetime.now(tz=UTC) - dt.timedelta(hours=1)
    cutoff = dt.datetime.now(tz=UTC) + dt.timedelta(hours=1)
    cases = recap.discover_live_cases(start, cutoff)
    assert {row["title"] for row in cases} == {
        "Live case",
        "Stats case",
        "US case",
    }


def test_case_discovery_uses_explicit_evidence_cutoff_for_late_case_artifact(
    tmp_path: Path, monkeypatch
):
    live = tmp_path / "live"
    major = tmp_path / "major"
    output = tmp_path / "recap"
    nested = output / "us_0830_release_live_case_v1"
    for directory in (live, major, nested):
        directory.mkdir(parents=True, exist_ok=True)
    markdown = nested / "US_0830_RELEASE_CASE_V1.md"
    companion = markdown.with_suffix(".json")
    markdown.write_text("# U.S. 08:30 case\n", encoding="utf-8")
    companion.write_text(
        json.dumps(
            {
                "cutoff_utc": "2026-08-27T13:00:00Z",
                "generated_utc": "2026-08-27T13:02:00Z",
                "research_only": True,
                "execution_eligible": False,
                "attribution_policy": {
                    "directional_forecast_claimed": False,
                    "causal_consensus_available": False,
                },
            }
        ),
        encoding="utf-8",
    )
    markdown.with_suffix(".sha256").write_text(
        "fixture  US_0830_RELEASE_CASE_V1.md\n", encoding="ascii"
    )
    future_mtime = dt.datetime(2026, 8, 27, 13, 2, tzinfo=UTC).timestamp()
    import os

    os.utime(markdown, (future_mtime, future_mtime))
    monkeypatch.setattr(recap, "LIVE_CASE_DIRECTORY", live)
    monkeypatch.setattr(recap, "MAJOR_MOVE_CASE_DIRECTORY", major)
    monkeypatch.setattr(recap, "OUTPUT_DIRECTORY", output)
    cases = recap.discover_live_cases(
        dt.datetime(2026, 8, 24, 4, tzinfo=UTC),
        dt.datetime(2026, 8, 27, 13, tzinfo=UTC),
    )
    assert len(cases) == 1
    case = cases[0]
    assert case["title"] == "U.S. 08:30 case"
    assert case["artifact_generated_after_report_cutoff"] is True
    assert case["directional_forecast_claimed"] is False
    assert case["retrospective_directional_win_claimed"] is False
    assert case["artifact_role"] == "diagnostic_case_inventory_only"
    assert case["companion_json"]["path"].endswith("US_0830_RELEASE_CASE_V1.json")
    assert case["hash_manifest"]["path"].endswith("US_0830_RELEASE_CASE_V1.sha256")
