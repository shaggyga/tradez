import datetime as dt
import json
import sqlite3

import pytest

from oanda_continuous_narrative_meter import build_contributions, build_meter
from oanda_continuous_narrative_meter_v12 import (
    CURRENCIES,
    METER_ACTIVATED_UTC,
    METER_CONTRACT_ID,
    SEAL_GRACE_SECONDS,
    build_cycle,
    floor_clock,
    open_bucket_end,
    persist,
    sealed_bucket_end,
)
from oanda_project_integrity_audit import continuous_narrative_meter_integrity


UTC = dt.timezone.utc


def article(*, story_id: str = "story-one", first_seen: str = "2026-08-27T07:25:01+00:00"):
    payload = {
        "event_lineage_id": story_id,
        "forward_signal_timely": True,
        "reports_prior_market_move": False,
        "observation_clock_trusted": True,
        "source_direct": True,
        "estimated_reaction_horizon_minutes": 60,
        "classification_version": "frozen-v12-test",
        "source_contract_id": "official-policy-v1",
        "source_cohort_id": "boj-rss-202608",
        "parser_version": "rss-parser-v2",
        "factor_terms": ["policy rate"],
        "topic_signature": "monetary_policy|rate_change",
        "directional_evidence": True,
        "directional_publish_eligible": True,
        "research_currency_scores": {"JPY": 0.6},
    }
    return {
        "event_id": story_id,
        "source_id": "boj",
        "source_kind": "rss",
        "source_verified": 1,
        "published_utc": "2026-08-27T07:20:00+00:00",
        "first_seen_utc": first_seen,
        "last_seen_utc": first_seen,
        "headline": "Bank changes policy",
        "domain": "boj.or.jp",
        "relevant": 1,
        "category": "central bank decision",
        "currencies_json": '["JPY"]',
        "currency_scores_json": '{"JPY":0.5}',
        "generic_sentiment_score": -0.2,
        "directional_confidence": 0.8,
        "payload_json": json.dumps(payload),
    }


def test_bucket_seals_only_after_close_plus_grace():
    before_grace = dt.datetime(2026, 8, 27, 7, 25, 59, tzinfo=UTC)
    after_grace = dt.datetime(2026, 8, 27, 7, 26, 0, tzinfo=UTC)
    assert floor_clock(before_grace) == dt.datetime(2026, 8, 27, 7, 25, tzinfo=UTC)
    assert sealed_bucket_end(before_grace) == dt.datetime(2026, 8, 27, 7, 20, tzinfo=UTC)
    assert sealed_bucket_end(after_grace) == dt.datetime(2026, 8, 27, 7, 25, tzinfo=UTC)
    assert open_bucket_end(before_grace) == dt.datetime(2026, 8, 27, 7, 30, tzinfo=UTC)
    assert SEAL_GRACE_SECONDS == 60


def test_current_bucket_is_provisional_not_part_of_sealed_rows():
    contributions, _ = build_contributions([article()])
    now = dt.datetime(2026, 8, 27, 7, 29, 0, tzinfo=UTC)
    sealed, all_rows, sealed_end, provisional_end = build_cycle(
        contributions, now=now
    )
    assert sealed_end == METER_ACTIVATED_UTC
    assert len(sealed) == len(CURRENCIES)
    assert provisional_end == dt.datetime(2026, 8, 27, 7, 30, tzinfo=UTC)
    jpy = next(
        row
        for row in all_rows
        if row["clock_utc"] == provisional_end and row["currency"] == "JPY"
    )
    assert jpy["active_story_count"] == 1


def test_completed_bucket_is_immutably_sealed_and_never_rewritten(tmp_path):
    clock = METER_ACTIVATED_UTC
    empty_rows = build_meter([], start=clock, end=clock)
    database = tmp_path / "v12.sqlite"
    first = persist(
        database,
        empty_rows,
        [],
        sealed_through=clock,
        now=clock + dt.timedelta(minutes=1),
    )
    assert first["inserted_currency_rows"] == len(CURRENCIES)
    assert first["inserted_bucket_seals"] == 1

    contributions, _ = build_contributions(
        [article(story_id="arrived-after-seal", first_seen="2026-08-27T07:24:59+00:00")]
    )
    replacement_rows = build_meter(contributions, start=clock, end=clock)
    second = persist(
        database,
        replacement_rows,
        contributions,
        sealed_through=clock,
        now=clock + dt.timedelta(minutes=2),
    )
    assert second["inserted_currency_rows"] == 0
    assert second["new_late_arrival_incidents"] == 1
    assert second["total_late_arrival_incidents"] == 1

    connection = sqlite3.connect(database)
    try:
        story_ids = json.loads(
            connection.execute(
                "SELECT story_ids_json FROM currency_meter "
                "WHERE meter_contract_id=? AND clock_utc=? AND currency='JPY'",
                (METER_CONTRACT_ID, clock.isoformat()),
            ).fetchone()[0]
        )
        assert story_ids == []
        assert connection.execute("SELECT COUNT(*) FROM bucket_seals").fetchone()[0] == 1
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute(
                "UPDATE currency_meter SET active_story_count=1 WHERE currency='JPY'"
            )
    finally:
        connection.close()


def test_worker_gap_is_recorded_and_not_backfilled(tmp_path):
    database = tmp_path / "v12.sqlite"
    first_clock = METER_ACTIVATED_UTC
    later_clock = first_clock + dt.timedelta(minutes=10)
    first_rows = build_meter([], start=first_clock, end=first_clock)
    later_rows = build_meter([], start=first_clock, end=later_clock)
    persist(
        database,
        first_rows,
        [],
        sealed_through=first_clock,
        now=first_clock + dt.timedelta(minutes=1),
    )
    result = persist(
        database,
        later_rows,
        [],
        sealed_through=later_clock,
        now=later_clock + dt.timedelta(minutes=1),
    )
    assert result["new_seal_gap_incidents"] == 1
    assert result["total_seal_gap_incidents"] == 1
    connection = sqlite3.connect(database)
    try:
        clocks = {
            row[0] for row in connection.execute("SELECT clock_utc FROM bucket_seals")
        }
        assert clocks == {first_clock.isoformat(), later_clock.isoformat()}
        gap = connection.execute(
            "SELECT missing_bucket_count,reason FROM seal_gap_events"
        ).fetchone()
        assert gap == (1, "worker_gap_preserved_no_retrospective_bucket_backfill")
    finally:
        connection.close()


def test_v12_contract_is_new_and_execution_ineligible(tmp_path):
    clock = METER_ACTIVATED_UTC
    rows = build_meter([], start=clock, end=clock)
    database = tmp_path / "v12.sqlite"
    persist(
        database,
        rows,
        [],
        sealed_through=clock,
        now=clock + dt.timedelta(minutes=1),
    )
    connection = sqlite3.connect(database)
    try:
        contract = connection.execute(
            "SELECT meter_contract_id,research_only,execution_eligible,"
            "bucket_semantics FROM meter_contract_registry"
        ).fetchone()
    finally:
        connection.close()
    assert contract[0] == METER_CONTRACT_ID
    assert contract[1:3] == (1, 0)
    assert "closed_bucket_end" in contract[3]


def test_integrity_binding_rejects_a_future_or_persisted_partial_state(tmp_path):
    clock = METER_ACTIVATED_UTC
    rows = build_meter([], start=clock, end=clock)
    database = tmp_path / "v12.sqlite"
    generated = clock + dt.timedelta(minutes=1)
    persistence = persist(
        database, rows, [], sealed_through=clock, now=generated
    )
    state = {
        "schema_version": "continuous_narrative_meter_latest_v12",
        "meter_contract_id": METER_CONTRACT_ID,
        "generated_utc": generated.isoformat(),
        "clock_utc": clock.isoformat(),
        "sealed_clock_utc": clock.isoformat(),
        "seal_grace_seconds": SEAL_GRACE_SECONDS,
        "bucket_minutes": 5,
        "research_only": True,
        "execution_eligible": False,
        "can_place_orders": False,
        "orders_placed": 0,
        "integrity_status": "ok",
        "persistence": persistence,
        "partial_live": {
            "state_kind": "unsealed_provisional_live_view",
            "bucket_end_utc": (clock + dt.timedelta(minutes=5)).isoformat(),
            "complete": False,
            "persisted": False,
            "proof_eligible": False,
            "research_only": True,
            "execution_eligible": False,
            "can_place_orders": False,
        },
    }
    check = continuous_narrative_meter_integrity(
        state, database_path=database, cutoff_epoch=generated.timestamp()
    )
    assert check["ok"] is True
    state["partial_live"]["persisted"] = True
    assert continuous_narrative_meter_integrity(
        state, database_path=database, cutoff_epoch=generated.timestamp()
    )["ok"] is False


def test_supervisor_and_consumers_bind_to_v12_paths():
    root = __import__("pathlib").Path(__file__).resolve().parent
    supervisor = (root / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    snapshot = (root / "oanda_live_move_news_snapshot.py").read_text(
        encoding="utf-8"
    )
    dashboard = (root / "oanda_practice_live_dashboard.py").read_text(
        encoding="utf-8"
    )
    assert '"oanda_continuous_narrative_meter_v12.py"' in supervisor
    assert '"continuous_narrative_meter_v12.json"' in supervisor
    assert '"continuous_narrative_meter_v11_frozen"' in supervisor
    assert '"v12_sealed_contract_cutover"' in supervisor
    assert '"continuous_narrative_meter_v12.json"' in snapshot
    assert '"continuous_narrative_meter_v12.json"' in dashboard
