import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_news_band_resolution_flow_h15 as timing
from oanda_level_band_contract_v2 import MarketBar


UTC = dt.timezone.utc
BASE = dt.datetime(2026, 8, 27, 12, 0, tzinfo=UTC)


def forecast(instrument="EUR_USD", approach=1):
    return {
        "forecast_id": f"band-{instrument}",
        "cohort_id": "level-cohort",
        "contract_id": "causal_level_band_prospective_v1_frozen_20260827a",
        "issued_at_utc": timing.iso(BASE + dt.timedelta(seconds=20)),
        "data_cutoff_utc": timing.iso(BASE),
        "instrument": instrument,
        "band_version_id": f"version-{instrument}",
        "approach_side": approach,
        "band_lower": 1.1000,
        "band_upper": 1.1005,
        "frozen_break_price": 1.1010 if approach == 1 else 1.0995,
        "frozen_reject_price": 1.0990 if approach == 1 else 1.1015,
        "pip": 0.0001,
    }


def minute(offset, mid, imbalance30=0.2, imbalance120=0.1, spread=1.0, instrument="EUR_USD"):
    epoch = int((BASE + dt.timedelta(minutes=offset)).timestamp())
    return {
        "minute_epoch": epoch,
        "instrument": instrument,
        "last_mid": mid,
        "average_spread_pips": spread,
        "imbalance_30s": imbalance30,
        "imbalance_120s": imbalance120,
        "last_broker_time": timing.iso(BASE + dt.timedelta(minutes=offset, seconds=59)),
    }


def news(instrument="EUR_USD", side="long", episode="episode-eur"):
    return {
        "episode_id": episode,
        "currency": "EUR",
        "instrument": instrument,
        "direction": side,
        "news_direction": side,
        "news_confidence": 0.75,
        "news_factor_source_ids": ["ecb_official"],
        "news_factor_source_cohort_ids": ["news-source-cohort"],
        "news_factor_source_contract_ids": ["news-source-contract"],
        "news_factor_first_known_utc": timing.iso(BASE - dt.timedelta(minutes=1)),
        "news_factor_expires_utc": timing.iso(BASE + dt.timedelta(minutes=30)),
        "_known": BASE - dt.timedelta(minutes=1),
        "_expires": BASE + dt.timedelta(minutes=30),
        "research_only": True,
        "execution_eligible": False,
    }


def bar(index, mid_close=1.1012):
    timestamp = BASE + dt.timedelta(minutes=4 + index)
    bid_open = 1.1010 + index * 0.00001
    ask_open = bid_open + 0.0002
    bid_close = mid_close - 0.0001
    ask_close = mid_close + 0.0001
    return MarketBar(
        timestamp=timestamp,
        minutes=1,
        mid_open=(bid_open + ask_open) / 2,
        mid_high=max((bid_open + ask_open) / 2, mid_close) + 0.0001,
        mid_low=min((bid_open + ask_open) / 2, mid_close) - 0.0001,
        mid_close=mid_close,
        bid_open=bid_open,
        bid_high=max(bid_open, bid_close) + 0.0001,
        bid_low=min(bid_open, bid_close) - 0.0001,
        bid_close=bid_close,
        ask_open=ask_open,
        ask_high=max(ask_open, ask_close) + 0.0001,
        ask_low=min(ask_open, ask_close) - 0.0001,
        ask_close=ask_close,
    )


def test_contact_and_resolution_require_distinct_completed_minutes():
    observed = BASE + dt.timedelta(minutes=3, seconds=30)
    # The first full minute is already beyond the break barrier. It establishes
    # contact only; a second completed minute must persist through the barrier.
    assert timing.detect_resolution(forecast(), [minute(1, 1.1012)], observed) is None
    result = timing.detect_resolution(
        forecast(), [minute(1, 1.1012), minute(2, 1.1011)], observed
    )
    assert result is not None
    assert result["resolution_kind"] == "break"
    assert result["direction"] == "long"
    assert result["contact_minute_utc"] == timing.iso(BASE + dt.timedelta(minutes=1))
    assert result["resolution_known_utc"] == timing.iso(BASE + dt.timedelta(minutes=3))


@pytest.mark.parametrize(
    ("approach", "second_mid", "kind", "side"),
    [
        (1, 1.1011, "break", "long"),
        (1, 1.0989, "reject", "short"),
        (-1, 1.0994, "break", "short"),
        (-1, 1.1016, "reject", "long"),
    ],
)
def test_frozen_band_resolution_maps_physical_side(approach, second_mid, kind, side):
    first_mid = 1.1002
    result = timing.detect_resolution(
        forecast(approach=approach),
        [minute(1, first_mid), minute(2, second_mid)],
        BASE + dt.timedelta(minutes=3, seconds=30),
    )
    assert result is not None
    assert (result["resolution_kind"], result["direction"]) == (kind, side)


def test_candidate_requires_news_known_before_contact_and_future_entry():
    config = timing.load_contract()
    observed = BASE + dt.timedelta(minutes=3, seconds=30)
    rows = {"EUR_USD": [minute(1, 1.1002), minute(2, 1.1011)]}
    candidates = timing.build_candidate_payloads(
        [news()], [forecast()], rows, observed, config
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    assert timing.parse_utc(candidate["planned_entry_utc"]) > observed
    assert candidate["flow_aligned"] is True
    assert candidate["news_source_cohort_ids"] == ["news-source-cohort"]
    assert candidate["news_source_contract_ids"] == ["news-source-contract"]
    assert candidate["research_only"] is True
    assert candidate["execution_eligible"] is False
    late = news()
    late["_known"] = BASE + dt.timedelta(minutes=2)
    late["news_factor_first_known_utc"] = timing.iso(late["_known"])
    assert timing.build_candidate_payloads([late], [forecast()], rows, observed, config) == []


def test_flow_uses_completed_resolution_minute_and_conflict_is_labeled():
    config = timing.load_contract()
    observed = BASE + dt.timedelta(minutes=3, seconds=30)
    rows = {
        "EUR_USD": [
            minute(1, 1.1002),
            minute(2, 1.1011, imbalance30=-0.3, imbalance120=0.1),
            # Current, incomplete minute must be ignored even though it agrees.
            minute(3, 1.1012, imbalance30=0.9, imbalance120=0.9),
        ]
    }
    candidate = timing.build_candidate_payloads(
        [news()], [forecast()], rows, observed, config
    )[0]
    assert candidate["signed_imbalance_30s"] == pytest.approx(-0.3)
    assert candidate["flow_aligned"] is False


def test_factor_episode_dedup_selects_lowest_cost_pair():
    config = timing.load_contract()
    observed = BASE + dt.timedelta(minutes=3, seconds=30)
    second = forecast("EUR_GBP")
    second_news = news("EUR_GBP")
    rows = {
        "EUR_USD": [minute(1, 1.1002, spread=2.0), minute(2, 1.1011, spread=2.0)],
        "EUR_GBP": [
            minute(1, 1.1002, spread=0.8, instrument="EUR_GBP"),
            minute(2, 1.1011, spread=0.8, instrument="EUR_GBP"),
        ],
    }
    result = timing.build_candidate_payloads(
        [news(), second_news], [forecast(), second], rows, observed, config
    )
    assert len(result) == 1
    assert result[0]["instrument"] == "EUR_GBP"


def test_stale_watchlist_fails_closed():
    config = timing.load_contract()
    observed = BASE + dt.timedelta(minutes=10)
    payload = {
        "status": "ok",
        "generated_utc": timing.iso(BASE),
        "watchlist": [{**news(), "arm": "news_only"}],
    }
    assert timing.load_active_news(payload, observed, config) == (
        [], "stale_or_invalid_watchlist"
    )


def test_live_watchlist_schema_binds_source_cohort_and_contract_lists():
    config = timing.load_contract()
    observed = BASE + dt.timedelta(minutes=3)
    payload = {
        "status": "ok",
        "generated_utc": timing.iso(observed),
        "watchlist": [{**news(), "arm": "news_only"}],
    }
    rows, state = timing.load_active_news(payload, observed, config)
    assert state == "fresh"
    assert len(rows) == 1
    assert rows[0]["_source_cohort_ids"] == ["news-source-cohort"]
    assert rows[0]["_source_contract_ids"] == ["news-source-contract"]
    assert "cohort_id" not in rows[0]


def test_append_only_ledger_creates_matched_no_flow_control(tmp_path):
    db = timing.open_ledger(tmp_path / "timing.sqlite")
    identity = timing.collection_identity()
    timing.register_cohort(db, identity, BASE)
    config = timing.load_contract()
    candidate = timing.build_candidate_payloads(
        [news()],
        [forecast()],
        {"EUR_USD": [minute(1, 1.1002), minute(2, 1.1011)]},
        BASE + dt.timedelta(minutes=3, seconds=30),
        config,
    )[0]
    candidate_id, inserted = timing.insert_candidate(db, identity, candidate)
    assert inserted is True
    lineage = json.loads(
        db.execute(
            "SELECT news_source_cohort_id FROM timing_candidates WHERE candidate_id=?",
            (candidate_id,),
        ).fetchone()[0]
    )
    assert lineage == {
        "cohort_ids": ["news-source-cohort"],
        "contract_ids": ["news-source-contract"],
    }
    arms = db.execute(
        "SELECT arm_name,matched_control_group_id,research_only,execution_eligible "
        "FROM timing_arms WHERE candidate_id=? ORDER BY arm_name",
        (candidate_id,),
    ).fetchall()
    assert [row[0] for row in arms] == [
        "news_band_resolution_flow_aligned",
        "news_band_resolution_no_flow_control",
    ]
    assert len({row[1] for row in arms}) == 1
    assert all(row[2:] == (1, 0) for row in arms)
    db.execute(
        "INSERT INTO timing_entries VALUES (?,?,?,?,?,?,?,1,0)",
        (candidate_id, timing.iso(BASE + dt.timedelta(minutes=4)), 1.1, 1.1002, "row", timing.iso(BASE), "entry"),
    )
    db.execute(
        "INSERT INTO timing_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,1,0)",
        ("outcome-test", candidate_id, 900, timing.iso(BASE), timing.iso(BASE), 1.0, 1.0, 0, None, "{}", "outcome"),
    )
    db.execute(
        "INSERT INTO timing_censors VALUES (?,?,?,?,?,1,0)",
        ("censor-test", candidate_id, timing.iso(BASE), "test_only", "{}"),
    )
    immutable_rows = (
        ("cohort_registry", "cohort_id", identity["cohort_id"]),
        ("timing_candidates", "candidate_id", candidate_id),
        ("timing_arms", "arm_id", db.execute("SELECT arm_id FROM timing_arms LIMIT 1").fetchone()[0]),
        ("timing_entries", "candidate_id", candidate_id),
        ("timing_outcomes", "outcome_id", "outcome-test"),
        ("timing_censors", "censor_id", "censor-test"),
    )
    for table, key, value in immutable_rows:
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            db.execute(f"UPDATE {table} SET {key}={key} WHERE {key}=?", (value,))
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            db.execute(f"DELETE FROM {table} WHERE {key}=?", (value,))
    db.close()


def test_exact_future_bam_entry_and_h15_invalidation_comparison(tmp_path):
    db = timing.open_ledger(tmp_path / "timing.sqlite")
    identity = timing.collection_identity()
    timing.register_cohort(db, identity, BASE)
    candidate = timing.build_candidate_payloads(
        [news()],
        [forecast()],
        {"EUR_USD": [minute(1, 1.1002), minute(2, 1.1011)]},
        BASE + dt.timedelta(minutes=3, seconds=30),
        timing.load_contract(),
    )[0]
    candidate_id, _ = timing.insert_candidate(db, identity, candidate)
    path = [bar(index, 1.1012 + index * 0.00002) for index in range(15)]
    path[1] = bar(1, 1.1004)  # Completed close back inside resistance band.
    before_entry = timing.append_entries_and_outcomes(
        db,
        BASE + dt.timedelta(minutes=4, seconds=30),
        {"EUR_USD": path},
    )
    assert before_entry == {"entries": 0, "outcomes": 0, "censors": 0}
    result = timing.append_entries_and_outcomes(
        db,
        BASE + dt.timedelta(minutes=20),
        {"EUR_USD": path},
    )
    assert result == {"entries": 1, "outcomes": 1, "censors": 0}
    entry = db.execute(
        "SELECT entry_utc,bid_open,ask_open FROM timing_entries WHERE candidate_id=?",
        (candidate_id,),
    ).fetchone()
    assert entry[0] == timing.iso(BASE + dt.timedelta(minutes=4))
    assert (entry[1], entry[2]) == pytest.approx((path[0].bid_open, path[0].ask_open))
    outcome = db.execute(
        "SELECT fixed_horizon_net_pips,invalidation_net_pips,invalidation_triggered "
        "FROM timing_outcomes WHERE candidate_id=?",
        (candidate_id,),
    ).fetchone()
    assert outcome[2] == 1
    assert outcome[1] != outcome[0]
    db.close()


def test_pending_instruments_include_unresolved_prior_cohorts(tmp_path):
    db = timing.open_ledger(tmp_path / "timing.sqlite")
    current = timing.collection_identity()
    prior = {
        **current,
        "cohort_id": "news_band_resolution_flow_h15_v1.prior",
        "definition_sha256": "1" * 64,
        "definition_json": '{"immutable":"prior"}',
    }
    timing.register_cohort(db, prior, BASE - dt.timedelta(days=1))
    timing.register_cohort(db, current, BASE)
    config = timing.load_contract()
    prior_candidate = timing.build_candidate_payloads(
        [news("GBP_USD", episode="episode-gbp")],
        [forecast("GBP_USD")],
        {
            "GBP_USD": [
                minute(1, 1.1002, instrument="GBP_USD"),
                minute(2, 1.1011, instrument="GBP_USD"),
            ]
        },
        BASE + dt.timedelta(minutes=3, seconds=30),
        config,
    )[0]
    current_candidate = timing.build_candidate_payloads(
        [news()],
        [forecast()],
        {"EUR_USD": [minute(1, 1.1002), minute(2, 1.1011)]},
        BASE + dt.timedelta(minutes=3, seconds=30),
        config,
    )[0]
    prior_id, _ = timing.insert_candidate(db, prior, prior_candidate)
    current_id, _ = timing.insert_candidate(db, current, current_candidate)
    assert timing.pending_instruments(db) == {"EUR_USD", "GBP_USD"}
    db.execute(
        "INSERT INTO timing_censors VALUES (?,?,?,?,?,1,0)",
        ("prior-censor", prior_id, timing.iso(BASE), "test_only", "{}"),
    )
    assert timing.pending_instruments(db) == {"EUR_USD"}
    assert current_id
    db.close()


def test_material_source_change_creates_new_cohort(tmp_path):
    first = tmp_path / "first.py"
    second = tmp_path / "second.py"
    first.write_text("one", encoding="utf-8")
    second.write_text("two", encoding="utf-8")
    assert timing.collection_identity(source_path=first)["cohort_id"] != timing.collection_identity(source_path=second)["cohort_id"]


def test_worker_has_no_order_or_control_surface():
    source = Path(timing.__file__).read_text(encoding="utf-8")
    forbidden = (
        "import requests",
        "OandaClient",
        "PracticeExecutor",
        "SignalContributionFeed",
        "read_credentials",
        "/orders",
        "/trades/",
    )
    assert all(token not in source for token in forbidden)
    contract = json.loads(timing.CONFIG.read_text(encoding="utf-8"))
    assert contract["policy"]["signal_feed_write"] is False
    assert contract["policy"]["lifecycle_write"] is False
    assert contract["policy"]["can_place_orders"] is False
