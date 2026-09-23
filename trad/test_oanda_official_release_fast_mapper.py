import datetime as dt
import json
import os
from pathlib import Path
import sqlite3

import oanda_official_release_fast_lane as fast
import oanda_official_release_fast_mapper as mapper
from oanda_official_release_fast_lane_contract import (
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
    OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
)


UTC = dt.timezone.utc


def _all68_quote_payload(event_time: dt.datetime, captured: dt.datetime) -> dict:
    quotes = {
        instrument: {
            "bid": 150.0 if instrument.endswith("_JPY") else 1.0,
            "ask": 150.01 if instrument.endswith("_JPY") else 1.0001,
            "pip": 0.01 if instrument.endswith("_JPY") else 0.0001,
            "time": fast.iso_utc(event_time - dt.timedelta(seconds=5)),
            "source": "practice_007_fast_executor_price_stream",
        }
        for instrument in fast.EXPECTED_QUOTE_INSTRUMENTS
    }
    return {
        "schema_version": 2,
        "generated_utc": fast.iso_utc(captured - dt.timedelta(seconds=1)),
        "producer": "practice_007_fast_executor_price_stream",
        "connection_generation": 11,
        "quote_count": 68,
        "quotes": quotes,
        "coverage": {
            "current_quote_count": 68,
            "last_known_quote_count": 68,
            "retained_last_known_count": 0,
            "retained_last_known_instruments": [],
            "connection_generation": 11,
            "retained_quotes_execution_eligible": False,
        },
        "transport": {"source": "sqlite_wal", "sequence": 501},
        "research_only": True,
    }


def _seed_input(monkeypatch, path: Path) -> None:
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    connection = fast.open_database(path)
    try:
        source = {
            "source_id": "fed_monetary_policy",
            "source_contract_id": "fed-test-v1",
            "source_cohort_id": "fed-test-v1",
        }
        prospective_time = fast.QUOTE_CAPTURE_ACTIVATED_UTC + dt.timedelta(
            minutes=5
        )
        quote_captured = prospective_time + dt.timedelta(seconds=2)
        fast.append_observations(
            connection,
            source=source,
            rows=[
                {
                    "headline": "historical decision",
                    "source_url": "https://example.test/historical",
                    "published_utc": "2026-04-30T11:00:00+00:00",
                }
            ],
            first_seen=dt.datetime(2026, 8, 28, 15, 0, tzinfo=UTC),
            listing_bootstrap=True,
            observation_clock={"attested": True, "source": "test_clock"},
        )
        fast.append_observations(
            connection,
            source=source,
            rows=[
                {
                    "headline": "new policy decision",
                    "source_url": "https://example.test/new-decision",
                    "published_utc": fast.iso_utc(prospective_time),
                    "source_direct": True,
                    "source_verified": True,
                }
            ],
            first_seen=prospective_time,
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=lambda: _all68_quote_payload(
                prospective_time, quote_captured
            ),
            quote_captured_utc=quote_captured,
        )
    finally:
        connection.close()


def test_mapper_preserves_bootstrap_diagnostics_but_only_forwards_prospective(
    monkeypatch, tmp_path: Path
):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)

    def fake_classify(raw, *, first_seen):
        return {
            **raw,
            "classification_version": mapper.REQUIRED_CLASSIFICATION_VERSION,
            "currency_scores": {"USD": 0.75},
            "directional_publish_eligible": True,
            "forward_signal_timely": True,
            "source_direct": True,
            "source_verified": True,
            "execution_eligible": False,
            "can_place_orders": False,
        }

    monkeypatch.setattr(mapper.news, "classify_article", fake_classify)
    output_database = tmp_path / "mapping.sqlite"
    result = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )
    assert result["counts"] == {
        "mappings": 2,
        "prospective_inputs": 1,
        "semantic_direction_available": 2,
        "prospective_semantic_candidates": 1,
        "publish_eligible_forward_candidates": 1,
        "forward_shadow_candidates": 1,
        "mapped_sources": 1,
        "retained_mappings_all_classifier_versions": 2,
        "retained_classifier_versions": 1,
    }
    assert len(result["latest_forward_shadow_candidates"]) == 1
    candidate = result["latest_forward_shadow_candidates"][0]
    assert candidate["fast_lane_prospective_observation"] is True
    assert candidate["forward_shadow_candidate"] is True
    assert candidate["prospective_semantic_candidate"] is True
    assert candidate["publish_eligible_forward_candidate"] is True
    assert candidate["execution_eligible"] is False
    assert candidate["can_authorize"] is False

    with sqlite3.connect(output_database) as connection:
        bootstrap = json.loads(
            connection.execute(
                """
                SELECT mapping_payload_json FROM official_release_mapping
                WHERE input_prospective_observation = 0
                """
            ).fetchone()[0]
        )
    assert bootstrap["semantic_direction_available"] is True
    assert bootstrap["forward_shadow_candidate"] is False
    assert bootstrap["prospective_semantic_candidate"] is False
    assert bootstrap["directional_publish_eligible"] is False
    assert bootstrap["forward_signal_timely"] is False


def test_prospective_direct_semantic_is_retained_when_publish_gate_is_closed(
    monkeypatch, tmp_path: Path
):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)

    def fake_classify(raw, *, first_seen):
        return {
            **raw,
            "classification_version": mapper.REQUIRED_CLASSIFICATION_VERSION,
            "currency_scores": {"USD": -0.6},
            "directional_publish_eligible": False,
            "source_direct": True,
            "source_verified": True,
            "execution_eligible": False,
            "can_place_orders": False,
        }

    monkeypatch.setattr(mapper.news, "classify_article", fake_classify)
    output_database = tmp_path / "mapping.sqlite"
    result = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )
    assert result["counts"]["prospective_semantic_candidates"] == 1
    assert result["counts"]["publish_eligible_forward_candidates"] == 0
    assert result["counts"]["forward_shadow_candidates"] == 1
    candidate = result["latest_forward_shadow_candidates"][0]
    assert candidate["prospective_semantic_candidate"] is True
    assert candidate["publish_eligible_forward_candidate"] is False
    assert candidate["directional_publish_eligible"] is False
    assert candidate["execution_eligible"] is False


def test_mapper_rejects_wrong_fast_lane_contract(monkeypatch, tmp_path: Path):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)
    with sqlite3.connect(input_database) as connection:
        connection.execute("DROP TRIGGER trg_fast_lane_observation_no_update")
        connection.execute(
            "UPDATE official_release_observation SET collector_contract_id = 'forged'"
        )
        connection.commit()
    output = mapper.open_output_database(tmp_path / "mapping.sqlite")
    try:
        try:
            mapper.read_observations(input_database, output)
        except ValueError as exc:
            assert "contract mismatch" in str(exc)
        else:
            raise AssertionError("wrong input contract was accepted")
    finally:
        output.close()


def test_mapper_retains_prior_contract_as_nonprospective_diagnostic(
    monkeypatch, tmp_path: Path
):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)
    with sqlite3.connect(input_database) as connection:
        connection.execute("DROP TRIGGER trg_fast_lane_observation_no_update")
        connection.execute(
            """
            UPDATE official_release_observation
               SET collector_contract_id=?, collector_cohort_id=?
             WHERE prospective_observation=1
            """,
            (
                OFFICIAL_RELEASE_FAST_LANE_PRIOR_CONTRACT_ID,
                OFFICIAL_RELEASE_FAST_LANE_PRIOR_COHORT_ID,
            ),
        )
        connection.commit()
    output = mapper.open_output_database(tmp_path / "mapping.sqlite")
    try:
        observations = mapper.read_observations(input_database, output)
    finally:
        output.close()
    retained = next(
        row for row in observations if row["retained_prior_collector_contract"]
    )
    assert retained["upstream_prospective_observation"] is True
    assert retained["prospective_observation"] is False

    monkeypatch.setattr(
        mapper.news,
        "classify_article",
        lambda raw, *, first_seen: {
            **raw,
            "currency_scores": {"USD": 1.0},
            "directional_publish_eligible": True,
            "source_direct": True,
            "source_verified": True,
        },
    )
    classified = mapper.classify_observation(retained)
    assert classified["semantic_direction_available"] is True
    assert classified["fast_lane_upstream_prospective_observation"] is True
    assert classified["fast_lane_prospective_observation"] is False
    assert classified["fast_lane_retained_prior_collector_contract"] is True
    assert classified["prospective_semantic_candidate"] is False
    assert classified["forward_shadow_candidate"] is False


def test_new_classifier_version_creates_append_only_remap(monkeypatch, tmp_path: Path):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)

    def fake_classify(raw, *, first_seen):
        return {
            **raw,
            "classification_version": mapper.REQUIRED_CLASSIFICATION_VERSION,
            "currency_scores": {"USD": 0.5},
            "directional_publish_eligible": False,
            "source_direct": True,
            "source_verified": True,
            "execution_eligible": False,
            "can_place_orders": False,
        }

    monkeypatch.setattr(mapper.news, "classify_article", fake_classify)
    output_database = tmp_path / "mapping.sqlite"
    monkeypatch.setattr(mapper, "REQUIRED_CLASSIFICATION_VERSION", "rules_old")
    first = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )
    monkeypatch.setattr(mapper, "REQUIRED_CLASSIFICATION_VERSION", "rules_new")
    second = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )

    assert first["counts"]["mappings"] == 2
    assert second["counts"]["mappings"] == 2
    assert second["counts"]["retained_mappings_all_classifier_versions"] == 4
    assert second["counts"]["retained_classifier_versions"] == 2
    with sqlite3.connect(output_database) as connection:
        versions = connection.execute(
            """
            SELECT classification_version, COUNT(*)
            FROM official_release_mapping
            GROUP BY classification_version
            ORDER BY classification_version
            """
        ).fetchall()
    assert versions == [("rules_new", 2), ("rules_old", 2)]


def test_mapper_adapts_only_the_immutable_raw_all68_capture(monkeypatch):
    event_time = fast.QUOTE_CAPTURE_ACTIVATED_UTC + dt.timedelta(minutes=15)
    observation = {
        "observation_id": "obs_live",
        "source_id": "boj_updates",
        "source_contract_id": "boj_test",
        "first_seen_utc": mapper.iso_utc(event_time),
        "prospective_observation": True,
        "listing_bootstrap": False,
        "publisher_time_eligible": True,
        "raw": {"headline": "Policy release"},
    }
    captured = event_time + dt.timedelta(seconds=2)
    raw_capture = fast.build_raw_quote_capture(
        observation_id="obs_live",
        first_seen=event_time,
        input_prospective_observation=True,
        quote_payload=_all68_quote_payload(event_time, captured),
        captured_utc=captured,
    )
    attached = mapper.pre_map_quote_snapshot_from_raw_capture(
        observation, raw_capture
    )
    assert attached["timing_quality"] == "prospective_exact_live_quote"
    assert attached["quote_count"] == 68
    assert attached["capture_origin"] == "official_release_raw_append_boundary"
    assert attached["raw_capture_contract_id"] == fast.QUOTE_CAPTURE_CONTRACT_ID

    def fake_classify(raw, *, first_seen):
        # The semantic result can arrive after the entry gate.  The already
        # frozen quote bundle must remain attached unchanged.
        assert first_seen == event_time
        return {
            **raw,
            "currency_scores": {"JPY": 0.5},
            "source_direct": True,
            "source_verified": True,
            "directional_publish_eligible": False,
        }

    monkeypatch.setattr(mapper.news, "classify_article", fake_classify)
    classified = mapper.classify_observation(
        {**observation, "pre_map_quote_snapshot": attached}
    )
    assert classified["fast_lane_pre_map_quote_snapshot"] == attached

    late_raw = fast.build_raw_quote_capture(
        observation_id="obs_live_late",
        first_seen=event_time,
        input_prospective_observation=True,
        quote_payload=_all68_quote_payload(
            event_time, event_time + dt.timedelta(seconds=20)
        ),
        captured_utc=event_time + dt.timedelta(seconds=20),
    )
    late = mapper.pre_map_quote_snapshot_from_raw_capture(
        {**observation, "observation_id": "obs_live_late"}, late_raw
    )
    assert late["timing_quality"] == "prospective_clock_missed"
    assert late["quote_count"] == 0
    assert late["execution_eligible"] is False


def test_mapper_never_recaptures_a_failed_raw_boundary_attempt(
    monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(
        fast.news,
        "prospective_clock_attestation",
        lambda clock: bool(clock.get("attested")),
    )
    event_time = fast.QUOTE_CAPTURE_ACTIVATED_UTC + dt.timedelta(minutes=20)
    database = tmp_path / "input.sqlite"
    source = {
        "source_id": "fed_speeches",
        "source_contract_id": "fed-test-v2",
        "source_cohort_id": "fed-test-v2",
    }
    row = {
        "title": "Policy remarks with a failed quote read",
        "source_url": "https://example.test/failed-quote-read",
        "published_utc": fast.iso_utc(event_time),
        "source_direct": True,
        "source_verified": True,
    }
    connection = fast.open_database(database)
    try:
        fast.append_observations(
            connection,
            source=source,
            rows=[row],
            first_seen=event_time,
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=None,
            quote_captured_utc=event_time + dt.timedelta(seconds=1),
        )
        # A later duplicate with perfect quotes cannot improve the immutable
        # failure.  The loader must not be called because no raw row is new.
        fast.append_observations(
            connection,
            source=source,
            rows=[row],
            first_seen=event_time + dt.timedelta(seconds=10),
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
            quote_snapshot_loader=lambda: (_ for _ in ()).throw(
                AssertionError("mapper/backlog recapture was attempted")
            ),
            quote_captured_utc=event_time + dt.timedelta(seconds=10),
        )
    finally:
        connection.close()

    output = mapper.open_output_database(tmp_path / "mapping.sqlite")
    try:
        observations = mapper.read_observations(database, output)
    finally:
        output.close()
    assert len(observations) == 1
    observed = observations[0]
    assert observed["prospective_observation"] is True
    assert observed["raw_quote_capture_state"] == (
        "current_prospective_attachment"
    )
    attached = observed["pre_map_quote_snapshot"]
    assert attached["timing_quality"] == "prospective_quote_snapshot_unavailable"
    assert attached["quote_count"] == 0
    assert attached["quotes"] == {}


def test_continuous_cadence_does_not_add_five_seconds_after_a_slow_cycle():
    assert mapper.next_cycle_sleep_seconds(5.0, 11.0) == 0.1
    assert mapper.next_cycle_sleep_seconds(5.0, 2.0) == 3.0


def test_database_fingerprint_ignores_transient_sqlite_sidecar_clocks(
    tmp_path: Path,
):
    database = tmp_path / "mapping.sqlite"
    database.write_bytes(b"durable-database")
    wal = tmp_path / "mapping.sqlite-wal"
    shm = tmp_path / "mapping.sqlite-shm"
    wal.write_bytes(b"")
    shm.write_bytes(b"transient-reader-state")

    original = mapper._database_fingerprint(database)
    os.utime(wal, None)
    shm.write_bytes(b"different-transient-reader-state")
    assert mapper._database_fingerprint(database) == original
    wal.unlink()
    assert mapper._database_fingerprint(database) == original

    wal.write_bytes(b"uncheckpointed-frame")
    assert mapper._database_fingerprint(database) != original


def test_no_input_change_cycle_reuses_exact_diagnostics(
    monkeypatch,
    tmp_path: Path,
):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)

    monkeypatch.setattr(
        mapper.news,
        "classify_article",
        lambda raw, *, first_seen: {
            **raw,
            "classification_version": mapper.REQUIRED_CLASSIFICATION_VERSION,
            "currency_scores": {"USD": 0.5},
            "directional_publish_eligible": False,
            "source_direct": True,
            "source_verified": True,
            "execution_eligible": False,
            "can_place_orders": False,
        },
    )
    mapper._INTEGRITY_CACHE.clear()
    mapper._DIAGNOSTIC_CACHE.clear()
    mapper._CAUGHT_UP_CACHE.clear()
    calls = {"counts": 0, "candidates": 0, "integrity": 0, "reads": 0}
    original_counts = mapper.mapping_counts
    original_candidates = mapper.latest_candidates
    original_integrity = mapper.verify_database_integrity
    original_reads = mapper.read_observations

    def counted_counts(*args, **kwargs):
        calls["counts"] += 1
        return original_counts(*args, **kwargs)

    def counted_candidates(*args, **kwargs):
        calls["candidates"] += 1
        return original_candidates(*args, **kwargs)

    def counted_integrity(*args, **kwargs):
        calls["integrity"] += 1
        return original_integrity(*args, **kwargs)

    def counted_reads(*args, **kwargs):
        calls["reads"] += 1
        return original_reads(*args, **kwargs)

    monkeypatch.setattr(mapper, "mapping_counts", counted_counts)
    monkeypatch.setattr(mapper, "latest_candidates", counted_candidates)
    monkeypatch.setattr(mapper, "verify_database_integrity", counted_integrity)
    monkeypatch.setattr(mapper, "read_observations", counted_reads)
    output_database = tmp_path / "mapping.sqlite"
    first = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )
    after_first = dict(calls)
    second = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )

    assert first["counts"] == second["counts"]
    assert first["latest_forward_shadow_candidates"] == second[
        "latest_forward_shadow_candidates"
    ]
    assert first["sqlite_integrity"] == second["sqlite_integrity"] == "ok"
    assert second["inserted_mappings"] == 0
    assert calls["counts"] == after_first["counts"] == 1
    assert calls["candidates"] == after_first["candidates"] == 1
    assert calls["reads"] == after_first["reads"] == 1
    # The input gate is invoked on every cycle, but its unchanged durable
    # fingerprint returns the already verified result without another
    # SQLite quick_check. Output diagnostics are reused wholesale.
    assert calls["integrity"] == after_first["integrity"] + 1 == 4

    input_connection = fast.open_database(input_database)
    try:
        inserted = fast.append_observations(
            input_connection,
            source={
                "source_id": "fed_monetary_policy",
                "source_contract_id": "fed-test-v1",
                "source_cohort_id": "fed-test-v1",
            },
            rows=[
                {
                    "headline": "newly appended policy communication",
                    "source_url": "https://example.test/new-communication",
                    "published_utc": "2026-08-28T15:10:00+00:00",
                    "source_direct": True,
                    "source_verified": True,
                }
            ],
            first_seen=dt.datetime(2026, 8, 28, 15, 10, 5, tzinfo=UTC),
            listing_bootstrap=False,
            observation_clock={"attested": True, "source": "test_clock"},
        )
    finally:
        input_connection.close()
    assert inserted[0] == 1

    third = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
    )
    assert third["inserted_mappings"] == 1
    assert third["counts"]["mappings"] == 3
    assert calls["reads"] == 2


def test_partial_limit_is_not_marked_caught_up(monkeypatch, tmp_path: Path):
    input_database = tmp_path / "input.sqlite"
    _seed_input(monkeypatch, input_database)
    monkeypatch.setattr(
        mapper.news,
        "classify_article",
        lambda raw, *, first_seen: {
            **raw,
            "currency_scores": {"USD": 0.25},
            "source_direct": True,
            "source_verified": True,
            "directional_publish_eligible": False,
        },
    )
    mapper._INTEGRITY_CACHE.clear()
    mapper._DIAGNOSTIC_CACHE.clear()
    mapper._CAUGHT_UP_CACHE.clear()
    output_database = tmp_path / "mapping.sqlite"
    first = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
        limit=1,
    )
    second = mapper.run_cycle(
        input_database=input_database,
        output_database=output_database,
        snapshot_path=tmp_path / "snapshot.json",
        heartbeat_path=tmp_path / "heartbeat.json",
        limit=1,
    )
    assert first["inserted_mappings"] == 1
    assert first["counts"]["mappings"] == 1
    assert second["inserted_mappings"] == 1
    assert second["counts"]["mappings"] == 2


def test_mapper_is_hidden_supervised_vaulted_and_has_no_broker_surface():
    module_text = Path(mapper.__file__).read_text(encoding="utf-8")
    supervisor = (mapper.ROOT / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    vault_sync = (mapper.ROOT / "forex_model_vault_sync.py").read_text(
        encoding="utf-8"
    )
    for forbidden in (
        "create_order",
        "close_trade",
        "OANDA_CREDS",
        "practice_007_canary_authorization",
        'execution_eligible": True',
    ):
        assert forbidden not in module_text
    assert '-Name "official_release_fast_mapper"' in supervisor
    assert "official_release_fast_mapping_heartbeat_v3.json" in supervisor
    assert 'Path("trad/oanda_official_release_fast_mapper.py")' in vault_sync
    assert 'Path("trad/test_oanda_official_release_fast_mapper.py")' in vault_sync
