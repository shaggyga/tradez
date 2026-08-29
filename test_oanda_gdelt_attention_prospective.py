from __future__ import annotations

import datetime as dt
import sqlite3

from trad import oanda_gdelt_attention_prospective as prospective
from trad.oanda_gdelt_attention_prospective import completed_hour, contract, mature, open_ledger


def test_completed_hour_uses_broker_event_time() -> None:
    start, end = completed_hour(dt.datetime(2026, 8, 10, 1, 0, 30, tzinfo=dt.timezone.utc))
    assert start == dt.datetime(2026, 8, 10, 0, 0, tzinfo=dt.timezone.utc)
    assert end == dt.datetime(2026, 8, 10, 1, 0, tzinfo=dt.timezone.utc)


def test_material_collector_change_creates_new_cohort_id() -> None:
    first = contract(
        {}, config_sha256="a" * 64, collector_sha256="b" * 64,
        observation_helper_sha256="d" * 64,
    )
    second = contract(
        {}, config_sha256="a" * 64, collector_sha256="c" * 64,
        observation_helper_sha256="d" * 64,
    )
    assert first["cohort_id"] != second["cohort_id"]
    assert first["supersedes_cohort_id"].endswith("a" * 16)


def test_observation_clock_change_creates_new_child_cohort() -> None:
    first = contract(
        {}, config_sha256="a" * 64, collector_sha256="b" * 64,
        observation_helper_sha256="c" * 64,
        observation_time_contract_id="clock-v1",
    )
    second = contract(
        {}, config_sha256="a" * 64, collector_sha256="b" * 64,
        observation_helper_sha256="d" * 64,
        observation_time_contract_id="clock-v2",
    )
    assert first["cohort_id"] != second["cohort_id"]
    assert first["observation_time_contract_id"] == "clock-v1"
    assert second["observation_time_contract_id"] == "clock-v2"


def test_mapping_helper_change_creates_new_child_cohort() -> None:
    first = contract(
        {}, config_sha256="a" * 64, collector_sha256="b" * 64,
        observation_helper_sha256="c" * 64,
        mapping_helper_sha256="d" * 64,
    )
    second = contract(
        {}, config_sha256="a" * 64, collector_sha256="b" * 64,
        observation_helper_sha256="c" * 64,
        mapping_helper_sha256="e" * 64,
    )
    assert first["cohort_id"] != second["cohort_id"]
    assert first["mapping_helper_sha256"] == "d" * 64


def test_attention_hour_excludes_legacy_untrusted_articles(
    tmp_path, monkeypatch
) -> None:
    hour = dt.datetime(2026, 8, 17, 9, tzinfo=dt.timezone.utc)
    decision = hour + dt.timedelta(hours=1)

    def story(index: int, trusted: bool) -> dict:
        return {
            "lineage_id": f"{'current' if trusted else 'legacy'}-{index}",
            "currency": "JPY",
            "source_id": f"source-{index}",
            "first_seen_utc": (hour + dt.timedelta(minutes=index)).isoformat(),
            "published_utc": hour.isoformat(),
            "generic_tone": 0.0,
            "directional_confidence": 0.0,
            "relevant": True,
            "forward_signal_timely": True,
            "duplicate_observations": 0,
            "headline": f"story {index}",
            "collector_contract_id": (
                prospective.COLLECTOR_CONTRACT_ID if trusted else "legacy-v38"
            ),
            "collector_cohort_id": (
                prospective.COLLECTOR_COHORT_ID if trusted else "legacy-v38"
            ),
            "observation_time_contract_id": (
                prospective.OBSERVATION_TIME_CONTRACT_ID
                if trusted else "legacy-clock"
            ),
            "observation_clock_trusted": trusted,
        }

    stories = [story(i, False) for i in range(3)] + [
        story(i + 3, True) for i in range(3)
    ]
    monkeypatch.setattr(
        prospective, "load_mapped_stories", lambda path: (stories, {})
    )
    rows = prospective.eligible_attention_hours(
        tmp_path / "unused.sqlite", hour, decision, 3
    )
    assert len(rows) == 1
    assert rows[0]["story_count"] == 3
    assert all(value.startswith("current-") for value in rows[0]["lineage_ids"])

    monkeypatch.setattr(
        prospective,
        "load_mapped_stories",
        lambda path: ([story(i, False) for i in range(3)], {}),
    )
    assert prospective.eligible_attention_hours(
        tmp_path / "unused.sqlite", hour, decision, 3
    ) == []


def test_ledger_is_immutable(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    columns = db.execute("PRAGMA table_info(forecasts)").fetchall()
    values = []
    for _, name, kind, required, default, primary in columns:
        if name == "research_only": values.append(1)
        elif name == "execution_eligible": values.append(0)
        elif name == "direction_policy": values.append("abstain")
        elif "INT" in kind: values.append(1)
        elif "REAL" in kind: values.append(1.0)
        else: values.append(name)
    db.execute(f"INSERT INTO forecasts VALUES ({','.join('?' for _ in values)})", values)
    db.commit()
    try:
        db.execute("UPDATE forecasts SET instrument='USD_JPY'")
    except sqlite3.IntegrityError as exc:
        assert "immutable forecasts" in str(exc)
    else:
        raise AssertionError("forecast mutation was accepted")
    db.close()


def test_maturity_records_best_direction_but_does_not_choose_direction(tmp_path) -> None:
    db = open_ledger(tmp_path / "ledger.sqlite")
    db.execute(
        "INSERT INTO forecasts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("f", "c", "2026-08-10T01:00:01+00:00", "2026-08-10T00:00:00+00:00", "2026-08-10T01:00:00+00:00", 3, 2, "[]", "s", "EUR", "EUR_USD", 3600, "2026-08-10T01:00:01+00:00", 1.0999, 1.1001, 1.1, 1.8, "abstain", "cfg", "code", 1, 0),
    )
    quote_time = dt.datetime(2026, 8, 10, 2, 0, 30, tzinfo=dt.timezone.utc)
    inserted = mature(
        db,
        {"EUR_USD": {"time": quote_time, "bid": 1.1009, "ask": 1.1011, "mid": 1.101}},
        quote_time,
        120,
    )
    assert inserted == 1
    row = db.execute("SELECT movement_cleared_cost,best_direction_net_bps FROM outcomes").fetchone()
    assert row[0] == 1 and row[1] > 0
    db.close()
