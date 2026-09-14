import datetime as dt
import json
import sqlite3

import pytest

import oanda_official_event_pair_horizon_capture_v2 as subject
import oanda_official_event_pair_quote_capture_v4 as capture
import oanda_official_release_fast_lane as fast
from oanda_feature_research_clock_v1 import validate_clock_state
from test_oanda_official_event_pair_quote_capture_v4 import NOW, snapshot, observation


def input_db(tmp_path):
    path = tmp_path/"fast.sqlite"
    conn = fast.open_database(path)
    binding = capture.initialize(conn, NOW-dt.timedelta(seconds=1))
    entry = capture.build_capture(observation(), snapshot(), NOW, binding, prospective=True)
    capture.insert_capture(conn, entry)
    conn.commit()
    conn.close()
    return path, binding, entry


def proof(now):
    return validate_clock_state(dict(schema_version=1, generated_utc=now.isoformat(), status="ok",
        timestamp_normalization_trusted=True, host_clock_synchronized=True,
        source_fresh=True, broker_clock_sample_count=64, broker_clock_lead_sec=0,
        source_age_sec=0), now_epoch=now.timestamp())


def run(tmp_path, path, when, payload=None, valid=True):
    return subject.run_cycle(input_database=path, output_directory=tmp_path/"out",
        clock=lambda: when, quote_loader=lambda p: payload or snapshot(when),
        clock_reader=lambda p: proof(when) if valid else dict(valid=False, reason="fixture_stale"))


def test_real_future_quotes_score_both_sides_and_restart_keeps_exact_result(tmp_path):
    path, _, entry = input_db(tmp_path)
    now = NOW+dt.timedelta(seconds=61)
    q = snapshot(now)
    q["quotes"]["EUR_USD"].update(bid=1.2010, ask=1.2012)
    q["quotes"]["USD_TRY"]["tradeable"] = False
    result = run(tmp_path, path, now, q)
    assert result["cycle"]["inserted_attempts"] == 1
    assert result["counts"]["valid_pair_outcomes"] == 2
    db = sqlite3.connect(tmp_path/"out"/"outcomes.sqlite")
    raw = db.execute("SELECT payload_json FROM official_event_pair_horizon_outcome WHERE instrument='EUR_USD'").fetchone()[0]
    item = json.loads(raw)
    assert item["buy_executable_pips"] == pytest.approx(8)
    assert item["sell_executable_pips"] == pytest.approx(-12)
    db.close()
    later = run(tmp_path, path, now+dt.timedelta(seconds=1), snapshot(now))
    assert later["cycle"]["inserted_attempts"] == 0
    db = sqlite3.connect(tmp_path/"out"/"outcomes.sqlite")
    assert db.execute("SELECT payload_json FROM official_event_pair_horizon_outcome WHERE instrument='EUR_USD'").fetchone()[0] == raw
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("DELETE FROM official_event_pair_horizon_outcome")
    db.close()


def test_missing_unrelated_exit_pair_does_not_erase_other_outcomes(tmp_path):
    path, _, _ = input_db(tmp_path)
    now = NOW+dt.timedelta(seconds=61)
    result = run(tmp_path, path, now, snapshot(now, ("EUR_USD",)))
    assert result["counts"]["valid_pair_outcomes"] == 1
    assert result["counts"]["invalid_pair_outcomes"] == 2


def test_clock_failure_persists_terminal_unknown_never_retries(tmp_path):
    path, _, _ = input_db(tmp_path)
    now = NOW+dt.timedelta(seconds=61)
    result = run(tmp_path, path, now, valid=False)
    assert result["status"] == "partial_unavailable"
    assert result["counts"]["valid_pair_outcomes"] == 0
    assert result["counts"]["invalid_pair_outcomes"] == 3
    resumed = run(tmp_path, path, now+dt.timedelta(seconds=1))
    assert resumed["cycle"]["inserted_attempts"] == 0
    assert resumed["counts"]["valid_pair_outcomes"] == 0


def test_late_worker_marks_missed_targets_without_hindsight_recovery(tmp_path):
    path, _, _ = input_db(tmp_path)
    now = NOW+dt.timedelta(minutes=6)
    result = run(tmp_path, path, now)
    assert result["cycle"]["inserted_attempts"] == 2
    assert result["counts"]["valid_pair_outcomes"] == 0
    assert result["counts"]["invalid_pair_outcomes"] == 6


def test_legacy_namespace_and_old_input_not_relabelled(tmp_path):
    import oanda_official_event_pair_horizon_capture_v1 as old
    old_contract = old.CONTRACT_ID
    _, binding, entry = input_db(tmp_path)
    module = subject.load_legacy(binding)
    assert old.CONTRACT_ID == old_contract
    assert module.CONTRACT_ID == subject.CONTRACT_ID
    bad = {**entry, "contract_id": "old-v1"}
    with pytest.raises(ValueError, match="entry_capture_binding"):
        module.build_attempt(bad, 1, snapshot(NOW+dt.timedelta(seconds=61)), NOW+dt.timedelta(seconds=61))


def test_changed_input_source_binding_refused(tmp_path, monkeypatch):
    path, _, _ = input_db(tmp_path)
    real_binding = capture._binding
    monkeypatch.setattr(capture, "_binding", lambda: {**real_binding(), "producer_source_sha256": "0"*64})
    with pytest.raises(ValueError, match="entry_cohort_source_binding_changed"):
        run(tmp_path, path, NOW+dt.timedelta(seconds=61))


def test_raw_official_append_through_terminal_outcome_preserves_legacy_zero(monkeypatch, tmp_path):
    monkeypatch.setattr(fast.news, "prospective_clock_attestation", lambda proof: True)
    path = tmp_path/"raw-to-outcome.sqlite"
    conn = fast.open_database(path)
    binding = capture.initialize(conn, NOW-dt.timedelta(seconds=1))
    fast.append_observations(conn,
        source=dict(source_id="fed_monetary_policy", source_contract_id="fixture-release"),
        rows=[dict(title="New official release", url="https://www.federalreserve.gov/fixture-new.htm",
                   published_utc=NOW.isoformat())], first_seen=NOW,
        listing_bootstrap=False, observation_clock={}, quote_snapshot_loader=lambda: snapshot(),
        quote_captured_utc=NOW+dt.timedelta(seconds=1), pair_capture_binding=binding)
    assert conn.execute("SELECT proof_quote_count FROM official_release_quote_capture").fetchone()[0] == 0
    assert conn.execute("SELECT eligible_quote_count FROM official_event_pair_quote_capture").fetchone()[0] == 3
    conn.close()
    result = run(tmp_path, path, NOW+dt.timedelta(seconds=61), snapshot(NOW+dt.timedelta(seconds=61), ("EUR_USD",)))
    assert result["cycle"]["inserted_attempts"] == 1
    assert result["counts"]["valid_pair_outcomes"] == 1
    assert result["counts"]["invalid_pair_outcomes"] == 2
