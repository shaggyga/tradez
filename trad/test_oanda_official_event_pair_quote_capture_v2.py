import datetime as dt
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_official_event_pair_quote_capture_v2 as subject
import oanda_official_event_pair_quote_capture_v2_verifier as verifier


UTC = dt.timezone.utc


def _candidate(
    ledger: str,
    upstream_id: str,
    first_seen: dt.datetime,
    *,
    url: str = "https://www.rba.gov.au/media-releases/2026/mr-26-99.html",
) -> dict:
    payload = {
        "url": url,
        "title": "Policy decision fixture",
        "published_utc": "2026-09-04T15:31:00+00:00",
        "published_time_inferred": False,
    }
    row = {
        "ledger_name": ledger,
        "upstream_observation_id": upstream_id,
        "source_id": "rba_media",
        "source_contract_id": "source-contract",
        "source_cohort_id": "source-cohort",
        "source_first_seen_utc": subject.iso_utc(first_seen),
        "source_url": url,
        "headline": payload["title"],
        "publisher_time_eligible": True,
        "raw_payload_json": json.dumps(payload, sort_keys=True),
        "ledger_collector_contract_id": "collector-contract",
        "ledger_collector_cohort_id": "collector-cohort",
        "base_ineligibility_reasons": [],
    }
    identity_key, canonical_event_id = subject.canonical_event_identity(row)
    row.update(
        {
            "identity_key": identity_key,
            "canonical_event_id": canonical_event_id,
            "alias_id": subject._alias_id(ledger, upstream_id),
        }
    )
    return row


def _quotes(captured: dt.datetime) -> dict:
    rows = {}
    for index, instrument in enumerate(subject.EXPECTED_INSTRUMENTS):
        bid = 1.0 + index / 10000
        rows[instrument] = {
            "bid": bid,
            "ask": bid + 0.0002,
            "pip": 0.0001,
            "time": subject.iso_utc(captured - dt.timedelta(seconds=1)),
            "source": "stream",
            "tradeable": True,
        }
    return {
        "schema_version": subject.quote_v1.REQUIRED_QUOTE_SNAPSHOT_SCHEMA_VERSION,
        "producer": subject.quote_v1.REQUIRED_QUOTE_SNAPSHOT_PRODUCER,
        "generated_utc": subject.iso_utc(captured - dt.timedelta(milliseconds=10)),
        "quote_count": 68,
        "quotes": rows,
        "coverage": {
            "current_quote_count": 68,
            "last_known_quote_count": 68,
            "retained_last_known_count": 0,
        },
        "transport": {"source": "fixture", "sequence": 1},
    }


def test_config_pins_source_universe_and_unchanged_v1_quote_builder():
    config = subject.validate_config()
    assert config["expected_source_count"] == 42
    assert len(subject.source_ids_from_frozen_map()) == 42
    assert subject.POLICY["semantic_mapping_read_before_capture"] is False
    assert subject.POLICY["execution_eligible"] is False


def test_supervisor_and_vault_preserve_retired_dual_ledger_cohort():
    supervisor = (subject.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    assert 'official_event_pair_quote_capture_v2_retired' in supervisor
    assert 'official_event_pair_quote_capture_v2_verifier_retired' in supervisor
    assert 'v3_quote_snapshot_read_clock_cutover' in supervisor
    vault = (subject.ROOT / "forex_model_vault_sync.py").read_text(encoding="utf-8")
    for name in (
        "oanda_official_event_pair_quote_capture_v2.py",
        "oanda_official_event_pair_quote_capture_v2_verifier.py",
        "official_event_pair_quote_capture_v2.json",
        "test_oanda_official_event_pair_quote_capture_v2.py",
    ):
        assert name in vault


def test_http_and_https_aliases_share_canonical_publisher_identity():
    first = _candidate(
        "fast_lane",
        "fast-1",
        subject.ACTIVATED_UTC + dt.timedelta(seconds=1),
        url="http://www.rba.gov.au/media-releases/2026/mr-26-99.html",
    )
    second = _candidate(
        "main_news",
        "main-1",
        subject.ACTIVATED_UTC + dt.timedelta(seconds=2),
    )
    assert first["canonical_event_id"] == second["canonical_event_id"]
    assert first["alias_id"] != second["alias_id"]


def test_actionable_clock_cannot_predate_durable_row_observation():
    source_seen = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    worker_seen = source_seen + dt.timedelta(seconds=20)
    alias = subject.build_alias(
        _candidate("main_news", "main-1", source_seen), worker_seen, set()
    )
    assert alias["source_first_seen_utc"] == subject.iso_utc(source_seen)
    assert alias["row_observed_utc"] == subject.iso_utc(worker_seen)
    assert alias["actionable_event_utc"] == subject.iso_utc(worker_seen)


def test_preactivation_identity_is_retained_but_cannot_capture():
    row = _candidate(
        "fast_lane", "fast-1", subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    )
    alias = subject.build_alias(
        row,
        subject.ACTIVATED_UTC + dt.timedelta(seconds=2),
        {row["canonical_event_id"]},
    )
    assert alias["eligibility_state"] == "rejected_alias"
    assert alias["identity_preexisting_before_activation"] is True
    assert "canonical_identity_preexisting_before_activation" in alias["ineligibility_reasons"]


def test_alias_and_capture_ledgers_are_append_only(tmp_path: Path):
    observed = subject.ACTIVATED_UTC + dt.timedelta(seconds=2)
    alias = subject.build_alias(
        _candidate("fast_lane", "fast-1", observed - dt.timedelta(seconds=1)),
        observed,
        set(),
    )
    capture = subject.build_capture(alias, _quotes(observed), observed)
    database = tmp_path / "capture.sqlite"
    connection = subject.open_output_database(database)
    try:
        assert subject.insert_alias(connection, alias)
        assert subject.insert_capture(connection, capture)
        connection.commit()
        assert capture["canonical_event_id"] == alias["canonical_event_id"]
        assert capture["winner_alias_id"] == alias["alias_id"]
        assert capture["eligible_quote_count"] == 68
        assert capture["exact_all_68_available"] is True
        assert capture["semantic_mapping_read_before_capture"] is False
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("UPDATE official_event_alias SET source_id='x'")
        connection.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="append_only"):
            connection.execute("DELETE FROM official_event_pair_quote_capture")
    finally:
        connection.close()


def test_later_alias_cannot_create_second_canonical_capture(tmp_path: Path):
    observed = subject.ACTIVATED_UTC + dt.timedelta(seconds=2)
    first = subject.build_alias(
        _candidate("fast_lane", "fast-1", observed - dt.timedelta(seconds=1)),
        observed,
        set(),
    )
    later = subject.build_alias(
        _candidate("main_news", "main-1", observed + dt.timedelta(seconds=1)),
        observed + dt.timedelta(seconds=2),
        set(),
    )
    database = tmp_path / "capture.sqlite"
    connection = subject.open_output_database(database)
    try:
        subject.insert_alias(connection, first)
        subject.insert_alias(connection, later)
        assert subject.insert_capture(connection, subject.build_capture(first, _quotes(observed), observed))
        connection.commit()
        assert not subject.insert_capture(
            connection,
            subject.build_capture(
                later,
                _quotes(observed + dt.timedelta(seconds=2)),
                observed + dt.timedelta(seconds=2),
            ),
        )
    finally:
        connection.close()


def test_independent_verifier_rebuilds_dual_ledger_lineage(tmp_path: Path):
    first_seen = subject.ACTIVATED_UTC + dt.timedelta(seconds=1)
    observed = first_seen + dt.timedelta(seconds=1)
    raw_payload = json.dumps(
        {
            "url": "https://www.rba.gov.au/media-releases/2026/mr-26-99.html",
            "title": "Policy decision fixture",
            "published_utc": "2026-09-04T15:31:00+00:00",
            "source_direct": True,
        },
        sort_keys=True,
    )
    fast_database = tmp_path / "fast.sqlite"
    connection = sqlite3.connect(fast_database)
    connection.execute(
        """CREATE TABLE official_release_observation(
          observation_id TEXT PRIMARY KEY,source_id TEXT,source_contract_id TEXT,
          source_cohort_id TEXT,item_key TEXT,material_sha256 TEXT,first_seen_utc TEXT,
          prospective_observation INTEGER,listing_bootstrap INTEGER,
          identity_preexisting INTEGER,publisher_time_eligible INTEGER,
          observation_clock_trusted INTEGER,observation_clock_source TEXT,
          raw_payload_json TEXT,research_only INTEGER,execution_eligible INTEGER,
          can_authorize INTEGER,collector_contract_id TEXT,collector_cohort_id TEXT)"""
    )
    connection.execute(
        "INSERT INTO official_release_observation VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "fast-1", "rba_media", "source-contract", "source-cohort", "item", "material",
            subject.iso_utc(first_seen), 1, 0, 0, 1, 1, "fixture", raw_payload, 1, 0, 0,
            subject.REQUIRED_FAST_COLLECTOR_CONTRACT_ID,
            subject.REQUIRED_FAST_COLLECTOR_COHORT_ID,
        ),
    )
    connection.commit()
    connection.close()

    main_database = tmp_path / "main.sqlite"
    connection = sqlite3.connect(main_database)
    connection.execute(
        """CREATE TABLE articles(
          event_id TEXT PRIMARY KEY,source_id TEXT,source_quality REAL,source_verified INTEGER,
          published_utc TEXT,first_seen_utc TEXT,headline TEXT,source_url TEXT,payload_json TEXT)"""
    )
    connection.commit()
    connection.close()

    quote_path = tmp_path / "quotes.json"
    quote_path.write_text(json.dumps(_quotes(observed)), encoding="utf-8")
    output_database = tmp_path / "capture.sqlite"
    state_path = tmp_path / "state.json"
    heartbeat_path = tmp_path / "heartbeat.json"
    result = subject.run_cycle(
        fast_database=fast_database,
        main_database=main_database,
        quote_path=quote_path,
        output_database=output_database,
        snapshot_path=state_path,
        heartbeat_path=heartbeat_path,
        observed_utc=observed,
    )
    assert result["counts"]["capture_count"] == 1

    verifier._PREACTIVATION_CACHE.clear()
    checked = verifier.verify(
        fast_database=fast_database,
        main_database=main_database,
        capture_database=output_database,
        capture_state_path=state_path,
        now_utc=observed + dt.timedelta(seconds=1),
    )
    assert checked["verified"] is True, checked["failures"]
    assert checked["counts"]["alias_count"] == 1
    assert checked["counts"]["capture_count"] == 1
