import copy
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_local_news_sentiment_repair_v2 as repair

NOW = 1788895200.0


def make_repaired_fixture(tmp_path, now=NOW):
    """Real disposable article DB/collector heartbeat/clock → actual RO capture.

    Return {data_root, snapshot, article, now}; no validation bypass or model fit.
    """
    now = repair.epoch(now)
    data = Path(tmp_path) / "data"
    news = data / "local_news_sentiment"
    state = data / "state"
    news.mkdir(parents=True, exist_ok=True); state.mkdir(parents=True, exist_ok=True)
    article = {
        "event_id": "fixture_original_event", "source_id": "fixture_feed", "source_name": "Fixture Publisher",
        "source_url": "https://fixture.example/original", "publisher_url": "https://fixture.example",
        "source_kind": "rss", "source_quality": 0.8, "source_verified": False, "source_direct": False,
        "headline": "Central bank maintains policy outlook following scheduled committee meeting",
        "summary": "Original retained synthetic context", "category": "monetary_policy", "scope": "currencies",
        "published_utc": repair.iso(now - 120), "first_seen_utc": repair.iso(now - 110),
        "last_seen_utc": repair.iso(now - 100), "causal_known_utc": repair.iso(now - 110),
        "classification_version": repair.guard.CLASSIFICATION_VERSION,
        "relevant": True, "context_only": True, "context_reason": "fixture_context",
        "currency_scores": {"USD": 0.4}, "research_currency_scores": {"USD": 0.4},
        "directional_bias": {}, "direct_currencies": ["USD"], "inferred_currencies": [], "currencies": ["USD"],
        "directional_confidence": 0.2, "forward_signal_timely": True, "forward_timeliness_limit_minutes": 30,
        "estimated_reaction_horizon_minutes": 60, "post_window_minutes": 360,
        "reports_prior_market_move": True, "structured_event": False, "scheduled_utc": None,
        "topic_signature": "policy_outlook|USD|context", "topic_action": "policy_outlook",
        "topic_entities": [], "topic_tags": [], "semantic_claims": [],
        "research_only": True, "execution_eligible": False, "can_place_orders": False,
    }
    path = news / "local_news_sentiment_v1.sqlite"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE articles (event_id TEXT PRIMARY KEY,source_id TEXT,source_kind TEXT,published_utc TEXT,first_seen_utc TEXT,last_seen_utc TEXT,relevant INTEGER,duplicate_count INTEGER,payload_json TEXT)")
        db.execute("INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?)", (article["event_id"], article["source_id"], article["source_kind"], article["published_utc"], article["first_seen_utc"], article["last_seen_utc"], 1, 0, json.dumps(article)))
    heartbeat = {"schema_version": repair.collector.SCHEMA_VERSION, "classification_version": repair.guard.CLASSIFICATION_VERSION,
        "status": "running_cycle", "phase": "collecting_sources", "generated_utc": repair.iso(now - 2),
        "last_progress_utc": repair.iso(now - 3), "cycle_started_utc": repair.iso(now - 30), "progress_sequence": 4}
    clock = {"schema_version": 1, "status": "ok", "generated_utc": repair.iso(now - 1),
        "timestamp_normalization_trusted": True, "host_clock_synchronized": True, "source_fresh": True,
        "broker_clock_lead_sec": 0.2, "broker_clock_sample_count": 128, "source_age_sec": 1,
        "external_https_clock": {"status": "ok", "offset_sec": 0.1, "round_trip_ms": 100, "precision_sec": 1,
                                 "server_date": repair.email.utils.format_datetime(dt.datetime.fromtimestamp(now - 2, dt.timezone.utc))}}
    (news / "collector_heartbeat_v1.json").write_text(json.dumps(heartbeat), encoding="utf-8")
    (state / "clock_integrity_v1.json").write_text(json.dumps(clock), encoding="utf-8")
    snapshot = repair.capture_repaired_snapshot(data, clock=lambda: now)
    return {"data_root": data, "snapshot": snapshot, "article": article, "now": now}


def reseal(snapshot):
    snapshot.pop("payload_sha256", None)
    snapshot["payload_sha256"] = repair.digest(snapshot)


def test_real_readonly_database_capture_replays_complete_context(tmp_path):
    f = make_repaired_fixture(tmp_path)
    core = repair.validate_repaired_snapshot(f["snapshot"])
    assert core["status"] == "current" and core["news_state"] == "context_only"
    evidence = f["snapshot"]["source_evidence"]
    assert evidence["complete"] and evidence["row_count"] == 1
    assert evidence["rows"][0]["first_seen_utc"] == f["article"]["first_seen_utc"]
    assert not (f["data_root"] / "local_news_sentiment_repair_v1").exists()


@pytest.mark.parametrize("field,value", [("complete", False), ("row_count", 0), ("rows_sha256", "0" * 64), ("payload_bytes", 0)])
def test_completeness_and_exact_rows_reject_tamper(tmp_path, field, value):
    s = make_repaired_fixture(tmp_path)["snapshot"]
    s["source_evidence"][field] = value; reseal(s)
    with pytest.raises(ValueError): repair.validate_repaired_snapshot(s)


@pytest.mark.parametrize("field", list(repair.INERT))
def test_inert_flags_not_replaced_with_safe_defaults(tmp_path, field):
    s = make_repaired_fixture(tmp_path)["snapshot"]
    s[field] = not repair.INERT[field]; reseal(s)
    with pytest.raises(ValueError, match="inert_flags"): repair.validate_repaired_snapshot(s)


@pytest.mark.parametrize("field,age", [("generated_utc", 91), ("last_progress_utc", 181)])
def test_current_clock_cannot_renew_stalled_collector(tmp_path, field, age):
    f = make_repaired_fixture(tmp_path)
    path = f["data_root"] / "local_news_sentiment/collector_heartbeat_v1.json"
    value = json.loads(path.read_text(encoding="utf-8")); value[field] = repair.iso(NOW - age)
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="upstream_collector_stale_or_future"):
        repair.capture_repaired_snapshot(f["data_root"], clock=lambda: NOW)


@pytest.mark.parametrize("mutate", [lambda s:s.update(host_clock_synchronized=False),
    lambda s:s.update(generated_utc=repair.iso(NOW + 1)),
    lambda s:s.update(broker_clock_lead_sec=5),
    lambda s:s.update(clock_discontinuity_active=True)])
def test_attested_clock_rejects_future_disagreement_and_discontinuity(tmp_path, mutate):
    s = make_repaired_fixture(tmp_path)["snapshot"]
    mutate(s["clock_evidence"]); reseal(s)
    with pytest.raises(ValueError): repair.validate_repaired_snapshot(s)


def test_cache_is_immutable_and_rechecks_bindings(tmp_path, monkeypatch):
    s = make_repaired_fixture(tmp_path)["snapshot"]
    first = repair.validate_repaired_snapshot(s)
    first["topics"].clear()
    assert repair.validate_repaired_snapshot(s)["topic_count"] == 1
    monkeypatch.setattr(repair, "source_bindings", lambda: {})
    with pytest.raises(ValueError, match="binding"): repair.validate_repaired_snapshot(s)


def test_snapshot_failure_does_not_publish_current_heartbeat(tmp_path, monkeypatch):
    f = make_repaired_fixture(tmp_path)
    original = repair.atomic_write_json
    def fail_current(path, value):
        if Path(path).name == "current_news_v2.json": raise OSError("synthetic_publish_failure")
        return original(path, value)
    monkeypatch.setattr(repair, "atomic_write_json", fail_current)
    hb = repair.run_cycle(f["data_root"], clock=lambda: NOW)
    assert hb["status"] == "unavailable" and hb["snapshot_sha256"] is None
    assert hb["publication_epoch"] is None and hb["errors"] == 1


def test_atomic_replace_failure_preserves_previous_bytes(tmp_path, monkeypatch):
    path = tmp_path / "current.json"; path.write_bytes(b"previous")
    monkeypatch.setattr(repair.os, "replace", lambda *args: (_ for _ in ()).throw(OSError("replace_failure")))
    with pytest.raises(OSError): repair.atomic_write_json(path, {"new": True})
    assert path.read_bytes() == b"previous"
    assert list(tmp_path.iterdir()) == [path]


def test_successful_cycle_commits_heartbeat_hash_after_snapshot(tmp_path):
    f = make_repaired_fixture(tmp_path)
    hb = repair.run_cycle(f["data_root"], clock=lambda: NOW)
    path = f["data_root"] / "local_news_sentiment_repair_v2/current_news_v2.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    assert hb["snapshot_sha256"] == repair.digest(value)
    assert hb["publication_epoch"] >= repair.epoch(value["generated_utc"])
    assert hb["status"] == "current" and not hb["can_place_orders"]


def test_complete_database_selection_is_not_the_published_500_topic_tail(tmp_path):
    f = make_repaired_fixture(tmp_path)
    database = f["data_root"] / "local_news_sentiment/local_news_sentiment_v1.sqlite"
    with sqlite3.connect(database) as db:
        for index in range(505):
            article = copy.deepcopy(f["article"])
            article["event_id"] = f"additional_retained_{index}"
            db.execute("INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?)", (article["event_id"], article["source_id"], article["source_kind"], article["published_utc"], article["first_seen_utc"], article["last_seen_utc"], 1, 0, json.dumps(article)))
    # An incomplete public tail is deliberately irrelevant to this source path.
    (database.parent / "topics_latest.json").write_text('{"topic_count":3197,"topics":[]}', encoding="utf-8")
    captured = repair.capture_repaired_snapshot(f["data_root"], clock=lambda: NOW)
    assert captured["source_evidence"]["row_count"] == 506
    assert len(captured["source_evidence"]["rows"]) == 506
    assert repair.validate_repaired_snapshot(captured)["status"] == "current"


def test_source_file_in_place_change_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "state.json"; path.write_text('{"ok":true}', encoding="utf-8")
    original = repair.os.fstat
    calls = 0
    def changed(fd):
        nonlocal calls
        calls += 1
        value = original(fd)
        if calls == 2:
            from types import SimpleNamespace
            return SimpleNamespace(st_ino=value.st_ino, st_size=value.st_size, st_mtime_ns=value.st_mtime_ns + 1)
        return value
    monkeypatch.setattr(repair.os, "fstat", changed)
    with pytest.raises(ValueError, match="changed_during_read"):
        repair._read_json(path, 1024)


def test_same_transaction_count_and_rows_exclude_concurrent_later_commit(tmp_path, monkeypatch):
    f = make_repaired_fixture(tmp_path)
    database = f["data_root"] / "local_news_sentiment/local_news_sentiment_v1.sqlite"
    connect = sqlite3.connect
    with connect(database) as db: db.execute("PRAGMA journal_mode=WAL")
    inserted = False
    class CountCursor:
        def __init__(self, cursor): self.cursor = cursor
        def fetchone(self):
            nonlocal inserted
            result = self.cursor.fetchone()
            if not inserted:
                article = copy.deepcopy(f["article"]); article["event_id"] = "concurrent_later_commit"
                with connect(database) as db:
                    db.execute("INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?)", (article["event_id"], article["source_id"], article["source_kind"], article["published_utc"], article["first_seen_utc"], article["last_seen_utc"], 1, 0, json.dumps(article)))
                inserted = True
            return result
    class Connection:
        def __init__(self, *args, **kwargs): self.db = connect(*args, **kwargs)
        def execute(self, sql, *args):
            result = self.db.execute(sql, *args)
            return CountCursor(result) if sql.startswith("SELECT count(*)") else result
        def set_progress_handler(self, *args): return self.db.set_progress_handler(*args)
        def close(self): self.db.close()
    monkeypatch.setattr(repair.sqlite3, "connect", Connection)
    first = repair.capture_repaired_snapshot(f["data_root"], clock=lambda: NOW)
    second = repair.capture_repaired_snapshot(f["data_root"], clock=lambda: NOW)
    assert first["source_evidence"]["row_count"] == 1
    assert second["source_evidence"]["row_count"] == 2


def test_unavailable_source_publishes_explicit_inert_rejection(tmp_path):
    f = make_repaired_fixture(tmp_path)
    hb = repair.run_cycle(f["data_root"], clock=lambda: NOW + 200)
    snapshot = json.loads((f["data_root"] / "local_news_sentiment_repair_v2/current_news_v2.json").read_text(encoding="utf-8"))
    assert snapshot["status"] == "unavailable" and snapshot["topics"] == []
    assert hb["source_status"] == "unavailable" and hb["last_error"]
    assert all(snapshot[key] is value for key, value in repair.INERT.items())


def test_fresh_wrapper_cannot_attest_cached_external_clock_probe(tmp_path):
    f = make_repaired_fixture(tmp_path)
    state = f["snapshot"]["clock_evidence"]
    state["source_fresh"] = False
    state["external_https_clock"]["server_date"] = repair.email.utils.format_datetime(dt.datetime.fromtimestamp(NOW - 1000, dt.timezone.utc))
    with pytest.raises(ValueError, match="alignment_unavailable"):
        repair.validate_clock_state(state, NOW)


def test_broker_probe_age_includes_age_of_integrity_wrapper(tmp_path):
    f = make_repaired_fixture(tmp_path)
    state = f["snapshot"]["clock_evidence"]
    state["external_https_clock"] = {}
    state["generated_utc"] = repair.iso(NOW - 80)
    state["source_age_sec"] = 20
    with pytest.raises(ValueError, match="alignment_unavailable"):
        repair.validate_clock_state(state, NOW)


def test_original_future_broker_probe_cannot_age_into_acceptance(tmp_path):
    f = make_repaired_fixture(tmp_path)
    state = f["snapshot"]["clock_evidence"]
    state["external_https_clock"] = {}
    state["generated_utc"] = repair.iso(NOW - 20)
    state["source_age_sec"] = -10
    with pytest.raises(ValueError, match="alignment_unavailable"):
        repair.validate_clock_state(state, NOW)
