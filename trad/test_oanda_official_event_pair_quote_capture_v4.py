import copy
import datetime as dt
import json
import sqlite3

import pytest

import oanda_official_release_fast_lane as fast
import oanda_official_event_pair_quote_capture_v4 as subject

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 14, 1, tzinfo=UTC)


def snapshot(now=NOW, pairs=("EUR_USD", "USD_JPY", "USD_TRY")):
    quotes = {pair: dict(bid=1.2, ask=1.2002, pip=.0001,
                        time=now.isoformat(), tradeable=True, source="stream") for pair in pairs}
    return dict(schema_version=3, producer=subject.SNAPSHOT_PRODUCER, research_only=True,
                generated_utc=now.isoformat(), quote_count=len(quotes), quotes=quotes,
                connection_generation=1, coverage=dict(current_quote_count=len(quotes),
                  last_known_quote_count=len(quotes), retained_last_known_count=0,
                  connection_generation=1, retained_last_known_instruments=[]))


def observation(now=NOW):
    return dict(observation_id="evt", source_id="fed", source_contract_id="fed-contract",
                first_seen_utc=now.isoformat(), raw_payload_json='{"title":"Official release"}')


def binding():
    return {**subject._binding(), "activated_utc": (NOW-dt.timedelta(seconds=1)).isoformat()}


def build(value=None):
    return subject.build_capture(observation(), value or snapshot(), NOW,
                                 binding(), prospective=True)


def test_consistent_partial_universe_keeps_fresh_pairs_and_explicit_absences():
    q = snapshot()
    q["quotes"]["USD_TRY"]["tradeable"] = False
    capture = build(q)
    assert capture["eligible_quote_count"] == 2
    assert set(capture["eligible_quotes"]) == {"EUR_USD", "USD_JPY"}
    assert capture["exact_all_68_available"] is False
    assert len(capture["pair_rows"]) == 68
    assert capture["pair_rows"]["USD_TRY"]["invalid_reason"] == "not_explicitly_tradeable"
    assert capture["pair_rows"]["GBP_USD"]["invalid_reason"] == "missing_quote"
    assert capture["quote_snapshot_payload_sha256"] == subject.sha256(subject.canonical_json(q))


@pytest.mark.parametrize("mutation", [
    lambda q: q.update(producer="practice_007_fast_executor_price_stream"),
    lambda q: q.update(schema_version=True),
    lambda q: q.update(research_only=False),
    lambda q: q.update(quote_count=68),
    lambda q: q.update(quote_count="3"),
    lambda q: q["coverage"].update(current_quote_count=2),
    lambda q: q["coverage"].update(retained_last_known_count=1),
    lambda q: q["coverage"].update(connection_generation=2),
    lambda q: q.update(connection_generation=True),
    lambda q: q.update(generated_utc=(NOW+dt.timedelta(microseconds=1)).isoformat()),
    lambda q: q.update(generated_utc=(NOW-dt.timedelta(seconds=6)).isoformat()),
    lambda q: q.update(generated_utc=NOW.replace(tzinfo=None).isoformat()),
])
def test_bad_global_identity_or_clock_refuses_all(mutation):
    q = snapshot()
    mutation(q)
    result = build(q)
    assert result["eligible_quote_count"] == 0
    assert result["metadata_invalid_reasons"]


@pytest.mark.parametrize("patch", [dict(tradeable="true"), dict(bid=-1), dict(bid=True),
                                  dict(pip=".0001"), dict(ask=float("nan")),
                                  dict(time=NOW.replace(tzinfo=None).isoformat()),
                                  dict(time=(NOW-dt.timedelta(seconds=31)).isoformat())])
def test_bad_pair_does_not_hide_other_pair(patch):
    q = snapshot()
    q["quotes"]["USD_JPY"].update(patch)
    result = build(q)
    assert "USD_JPY" not in result["eligible_quotes"]
    assert "EUR_USD" in result["eligible_quotes"]


def test_legacy_contract_unchanged_and_binding_immutable(tmp_path):
    connection = fast.open_database(tmp_path/"fast.sqlite")
    b = subject.initialize(connection, NOW)
    assert subject.initialize(connection, NOW+dt.timedelta(hours=2)) == b
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE official_pair_capture_v4_binding SET payload_json='{}'")
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("DELETE FROM official_pair_capture_v4_binding")
    legacy = subject._legacy().build_capture(observation(), snapshot(), NOW)
    assert legacy["eligible_quote_count"] == 0
    assert "quote_snapshot_producer_mismatch" in legacy["metadata_invalid_reasons"]
    connection.close()


def test_append_and_restart_never_recapture_or_backfill(monkeypatch, tmp_path):
    monkeypatch.setattr(fast.news, "prospective_clock_attestation", lambda c: True)
    connection = fast.open_database(tmp_path/"fast.sqlite")
    b = subject.initialize(connection, NOW-dt.timedelta(seconds=1))
    args = dict(source=dict(source_id="fed", source_contract_id="fed-test", source_cohort_id="fed-test"),
                rows=[dict(title="Policy release", url="https://www.federalreserve.gov/new.htm",
                           published_utc=NOW.isoformat())], first_seen=NOW,
                listing_bootstrap=False, observation_clock={"attested": True},
                quote_captured_utc=NOW+dt.timedelta(seconds=1))
    calls = []
    def load():
        calls.append(1)
        return snapshot()
    fast.append_observations(connection, **args, quote_snapshot_loader=load, pair_capture_binding=b)
    first = connection.execute("SELECT capture_payload_json FROM official_event_pair_quote_capture").fetchone()[0]
    assert json.loads(first)["eligible_quote_count"] == 3
    assert len(calls) == 1
    fast.append_observations(connection, **args, quote_snapshot_loader=load, pair_capture_binding=b)
    assert connection.execute("SELECT COUNT(*) FROM official_event_pair_quote_capture").fetchone()[0] == 1
    assert len(calls) == 1
    assert connection.execute("SELECT capture_payload_json FROM official_event_pair_quote_capture").fetchone()[0] == first
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("DELETE FROM official_event_pair_quote_capture")
    connection.close()


def test_raw_append_survives_snapshot_failure_with_terminal_refusal(monkeypatch, tmp_path):
    monkeypatch.setattr(fast.news, "prospective_clock_attestation", lambda c: True)
    connection = fast.open_database(tmp_path/"fast.sqlite")
    b = subject.initialize(connection, NOW-dt.timedelta(seconds=1))
    def fail():
        raise OSError("fixture transport failed")
    fast.append_observations(connection,
        source=dict(source_id="fed", source_contract_id="fed-test"),
        rows=[dict(title="Release", url="https://www.federalreserve.gov/2.htm", published_utc=NOW.isoformat())],
        first_seen=NOW, listing_bootstrap=False, observation_clock={},
        quote_snapshot_loader=fail, quote_captured_utc=NOW, pair_capture_binding=b)
    payload = json.loads(connection.execute("SELECT capture_payload_json FROM official_event_pair_quote_capture").fetchone()[0])
    assert payload["eligible_quote_count"] == 0
    assert "quote_snapshot_read_failed:OSError" in payload["metadata_invalid_reasons"]
    assert connection.execute("SELECT COUNT(*) FROM official_release_observation").fetchone()[0] == 1
    connection.close()


def test_opted_in_capture_uses_actual_post_read_clock(monkeypatch, tmp_path):
    monkeypatch.setattr(fast.news, "prospective_clock_attestation", lambda c: True)
    connection = fast.open_database(tmp_path/"fast.sqlite")
    b = subject.initialize(connection, NOW-dt.timedelta(seconds=1))
    clocks = iter([NOW, NOW+dt.timedelta(seconds=2)])
    monkeypatch.setattr(fast, "utc_now", lambda: next(clocks))
    fast.append_observations(connection,
        source=dict(source_id="fed", source_contract_id="fed-test"),
        rows=[dict(title="Release", url="https://www.federalreserve.gov/3.htm", published_utc=NOW.isoformat())],
        first_seen=NOW, listing_bootstrap=False, observation_clock={},
        quote_snapshot_loader=lambda: snapshot(NOW+dt.timedelta(seconds=1)), pair_capture_binding=b)
    payload = json.loads(connection.execute("SELECT capture_payload_json FROM official_event_pair_quote_capture").fetchone()[0])
    assert payload["captured_utc"] == (NOW+dt.timedelta(seconds=2)).isoformat()
    assert payload["eligible_quote_count"] == 3
    connection.close()


def test_unsupported_fast_lane_source_is_explicit_even_without_transport_errors(monkeypatch, tmp_path):
    config = tmp_path/"config.json"
    mapping = tmp_path/"map.json"
    config.write_text(json.dumps({"policy": {}}), encoding="utf-8")
    mapping.write_text(json.dumps({"contract_id": "fixture", "currencies": [{"currency": "NZD"}]}), encoding="utf-8")
    monkeypatch.setattr(fast, "selected_sources", lambda c,m: [dict(source_id="rbnz", runtime_blocker="HTTP403")])
    monkeypatch.setattr(fast.news, "collection_order", lambda sources, now: sources)
    monkeypatch.setattr(fast.news, "migrate_derived_source_state", lambda source, state, now: state)
    monkeypatch.setattr(fast.news, "source_runtime_status", lambda source: "unsupported")
    def unexpected_fetch(*args, **kwargs):
        raise AssertionError("unsupported source must not be polled")
    monkeypatch.setattr(fast.news, "fetch_source", unexpected_fetch)
    result = fast.run_cycle(config_path=config, central_bank_map_path=mapping,
        database_path=tmp_path/"fast.sqlite", state_path=tmp_path/"state.json",
        snapshot_path=tmp_path/"snapshot.json", heartbeat_path=tmp_path/"heartbeat.json", now=NOW)
    assert result["errors"] == []
    assert result["coverage_status"] == "partial_source_support"
    assert result["supported_release_source_count"] == 0
    assert result["skipped_sources"] == [dict(source_id="rbnz", status="unsupported", reason="HTTP403")]
    heartbeat = json.loads((tmp_path/"heartbeat.json").read_text())
    assert heartbeat["details"]["skipped_sources"] == 1
    assert heartbeat["details"]["coverage_status"] == "partial_source_support"
