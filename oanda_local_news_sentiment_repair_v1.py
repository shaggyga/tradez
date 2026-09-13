"""Separate research-only current-news projection from a complete RO DB snapshot.

No classification, governance/database writes, broker calls or old-output edits.
Current contexts are re-evaluated from original classified members, not given new
arrival/expiry clocks. Only main() publishes to the new dedicated output root.
"""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import email.utils
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from collections import OrderedDict
from contextlib import closing

import oanda_local_news_sentiment as collector
import oanda_news_causal_aggregation_guard_v1 as guard
import oanda_news_topic_identity_reconciliation_v1 as reconciliation

SCHEMA = "repaired_joint_news_current_v1_20260908"
HEARTBEAT_SCHEMA = "repaired_joint_news_heartbeat_v1_20260908"
EVIDENCE_SCHEMA = "repaired_news_complete_sqlite_input_v1_20260908"
CLOCK_CONTRACT = "repaired_news_synchronized_host_attestation_v1_20260908"
SOURCE_FILES = frozenset({
    "oanda_local_news_sentiment_repair_v1.py", "oanda_news_topic_identity_reconciliation_v1.py",
    "oanda_local_news_sentiment.py", "oanda_news_causal_aggregation_guard_v1.py",
    "oanda_news_classification_contract.py", "oanda_news_event_tagger.py", "oanda_news_collector_contract.py",
})
INERT = {"research_only": True, "execution_eligible": False, "can_place_orders": False,
         "can_promote": False, "can_authorize": False, "account_eligible": False, "proof_eligible": False}
MAX_ROWS = 4096
MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
MAX_STATE_BYTES = 128 * 1024
MAX_CAPTURE_SECONDS = 30
CORE_KEYS = ("as_of_utc", "generated_utc", "classification_version", "guard_version", "status", "news_state",
             "topics", "topic_count", "directional_topic_count", "context_topic_count", "errors", "limits")
_PREDICATE = "relevant = 1 AND (julianday(published_utc) >= julianday(?) OR julianday(published_utc) IS NULL)"
_REPLAY_CACHE = OrderedDict()
_REPLAY_LOCK = threading.Lock()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def epoch(value):
    if type(value) in (int, float) and math.isfinite(value):
        return float(value)
    if not isinstance(value, str):
        raise ValueError("news_clock_invalid")
    stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() != dt.timedelta(0):
        raise ValueError("news_clock_not_utc")
    return stamp.timestamp()


def iso(value):
    return dt.datetime.fromtimestamp(epoch(value), dt.timezone.utc).isoformat()


def source_bindings():
    root = Path(__file__).resolve().parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in sorted(SOURCE_FILES)}


def _flags(value):
    if any(value.get(key) is not expected for key, expected in INERT.items()):
        raise ValueError("repaired_news_inert_flags_invalid")


def _read_json(path, maximum):
    with Path(path).open("rb") as file:
        before = os.fstat(file.fileno())
        raw = file.read(maximum + 1)
        after = os.fstat(file.fileno())
    if len(raw) > maximum:
        raise ValueError("news_source_byte_bound")
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("news_source_changed_during_read")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("news_source_object_required")
    return value, hashlib.sha256(raw).hexdigest()


def validate_clock_state(state, observed_epoch):
    """Pure strict aligned-host subset of the frozen collector's clock contract.

    No cached executor offset or timestamp adjustment is accepted here. The
    retained integrity input independently replays the acceptance decision.
    """
    now = epoch(observed_epoch)
    age = now - epoch(state.get("generated_utc"))
    if not 0 <= age <= 90 or state.get("status") not in {"ok", "mitigated"}:
        raise ValueError("news_clock_state_stale_or_future")
    if state.get("timestamp_normalization_trusted") is not True or state.get("host_clock_synchronized") is not True:
        raise ValueError("news_clock_not_synchronized")
    if state.get("clock_discontinuity_active") is True:
        raise ValueError("news_clock_discontinuity_active")
    lead = state.get("broker_clock_lead_sec")
    samples = state.get("broker_clock_sample_count")
    source_age = state.get("source_age_sec")
    broker = (state.get("source_fresh") is True and type(samples) is int and samples >= 32
              and type(lead) in (int, float) and math.isfinite(lead) and abs(lead) <= 300
              and type(source_age) in (int, float) and math.isfinite(source_age)
              and source_age >= 0 and 0 <= source_age + age <= 90)
    ext = state.get("external_https_clock") or {}
    offset, rtt, precision = (ext.get(k) for k in ("offset_sec", "round_trip_ms", "precision_sec"))
    external = (ext.get("status") == "ok" and all(type(v) in (int, float) and math.isfinite(v) for v in (offset, rtt, precision))
                and abs(offset) <= 300 and 0 <= rtt <= 2000 and 0 <= precision <= 1)
    if external:
        try:
            server_date = email.utils.parsedate_to_datetime(ext.get("server_date"))
            probe_epoch = server_date.timestamp() - offset if server_date.tzinfo is not None else math.nan
            external = math.isfinite(probe_epoch) and 0 <= now - probe_epoch <= 90
        except (ValueError, TypeError, AttributeError, OverflowError):
            external = False
    if broker and external and abs(lead - offset) > 2:
        raise ValueError("news_clock_sources_disagree")
    if not ((broker and abs(lead) <= 2) or (external and abs(offset) <= 2)):
        raise ValueError("news_clock_alignment_unavailable")
    return {"contract_id": CLOCK_CONTRACT, "observed_epoch": now,
            "integrity_generated_epoch": epoch(state["generated_utc"]), "applied_offset_sec": 0,
            "trusted_for_prospective_evidence": True, "clock_state_sha256": digest(state)}


def validate_collector_observation(value, now):
    if value.get("classification_version") != guard.CLASSIFICATION_VERSION:
        raise ValueError("upstream_collector_classification_mismatch")
    if value.get("schema_version") != collector.SCHEMA_VERSION:
        raise ValueError("upstream_collector_schema_mismatch")
    generated = epoch(value.get("generated_utc"))
    progress = epoch(value.get("last_progress_utc"))
    started = epoch(value.get("cycle_started_utc"))
    if not started <= progress <= generated <= now or now - generated > 90 or now - progress > 180:
        raise ValueError("upstream_collector_stale_or_future")
    if value.get("status") not in {"running_cycle", "cycle_complete"}:
        raise ValueError("upstream_collector_not_active")


def _rows_to_articles(evidence, as_of):
    if evidence.get("schema_version") != EVIDENCE_SCHEMA or evidence.get("complete") is not True:
        raise ValueError("news_source_completeness_missing")
    cutoff = epoch(evidence.get("database_snapshot_epoch"))
    since = epoch(evidence.get("minimum_publication_utc"))
    if abs(cutoff - since - 86400) > 0.000001 or not cutoff <= as_of or as_of - cutoff > MAX_CAPTURE_SECONDS:
        raise ValueError("news_source_selection_clock_invalid")
    if evidence.get("predicate") != _PREDICATE or evidence.get("maximum_admissible_window_minutes") != 1440:
        raise ValueError("news_source_selection_contract_invalid")
    rows = evidence.get("rows")
    if not isinstance(rows, list) or len(rows) > MAX_ROWS or evidence.get("row_count") != len(rows):
        raise ValueError("news_source_row_count_mismatch")
    if evidence.get("rows_sha256") != digest(rows):
        raise ValueError("news_source_rows_hash_mismatch")
    total = 0
    articles = []
    seen = set()
    for row in rows:
        raw = row.get("payload_json")
        if not isinstance(raw, str):
            raise ValueError("news_source_payload_missing")
        total += len(raw.encode("utf-8"))
        if total > MAX_SOURCE_BYTES:
            raise ValueError("news_source_total_byte_bound")
        if hashlib.sha256(raw.encode("utf-8")).hexdigest() != row.get("payload_sha256"):
            raise ValueError("news_source_payload_hash_mismatch")
        payload = json.loads(raw)
        ident = row.get("event_id")
        if not isinstance(ident, str) or not ident or ident in seen or payload.get("event_id") != ident:
            raise ValueError("news_source_event_identity_conflict")
        seen.add(ident)
        if row.get("relevant") != 1 or payload.get("relevant") is not True:
            raise ValueError("news_source_relevance_mismatch")
        published = epoch(row.get("published_utc"))
        if published < since or epoch(payload.get("published_utc")) != published:
            raise ValueError("news_source_publication_mismatch")
        first = epoch(row.get("first_seen_utc"))
        last = epoch(row.get("last_seen_utc"))
        if not first <= last <= cutoff:
            raise ValueError("news_source_observation_clock_invalid")
        if payload.get("classification_version") != guard.CLASSIFICATION_VERSION:
            raise ValueError("news_source_classification_mismatch")
        if payload.get("research_only") is not True or payload.get("execution_eligible") is not False or payload.get("can_place_orders") is not False:
            raise ValueError("news_source_inert_flags_invalid")
        # Same canonical first-seen restoration as the frozen collector loader.
        # No classifier, migration, DB upsert or fallback observation is called.
        payload["first_seen_utc"] = row["first_seen_utc"]
        known = collector.causal_known_datetime(payload)
        if known is None:
            raise ValueError("news_source_causal_clock_missing")
        payload["causal_known_utc"] = known.isoformat()
        payload["last_seen_utc"] = row["last_seen_utc"]
        duplicates = row.get("duplicate_count")
        if type(duplicates) is not int or duplicates < 0:
            raise ValueError("news_source_duplicate_count_invalid")
        payload["duplicate_observation_count"] = duplicates
        payload["corroboration_count"] = int(payload.get("corroboration_count") or 0)
        articles.append(payload)
    if evidence.get("payload_bytes") != total:
        raise ValueError("news_source_byte_count_mismatch")
    return collector.collapse_exact_source_url_duplicates(articles)


def replay_source_evidence(evidence, *, as_of_epoch):
    """Pure full input → unchanged clustering → new reconciliation → old guard."""
    asof = dt.datetime.fromtimestamp(epoch(as_of_epoch), dt.timezone.utc)
    articles = _rows_to_articles(evidence, asof.timestamp())
    topics = collector.cluster_articles(articles, as_of=asof)
    reconciled = reconciliation.reconcile_topic_identities_with_provenance(topics, as_of=asof)
    core = guard.build_current_news_snapshot(reconciled["topics"], as_of=asof)
    if core["status"] != "current":
        raise ValueError("repaired_current_guard_unavailable:" + "|".join(core.get("errors") or []))
    return core, reconciled["reconciliations"]


def _assemble_snapshot(source_evidence, *, collector_observation, clock_evidence,
                       read_started_epoch, read_completed_epoch, as_of_epoch, generated_epoch, bindings, replayed):
    times = list(map(epoch, (read_started_epoch, read_completed_epoch, as_of_epoch, generated_epoch)))
    if times != sorted(times) or times[-1] - times[0] > MAX_CAPTURE_SECONDS:
        raise ValueError("repaired_news_observation_clock_invalid")
    if source_evidence.get("database_snapshot_epoch") != times[0]:
        raise ValueError("repaired_news_snapshot_clock_mismatch")
    validate_collector_observation(collector_observation, times[-1])
    attestation = validate_clock_state(clock_evidence, times[-1])
    core, provenance = replayed
    result = {**core, "schema_version": SCHEMA, "generated_utc": iso(times[-1]), **INERT,
              "source_bindings": bindings if bindings is not None else source_bindings(),
              "source_evidence": copy.deepcopy(source_evidence),
              "collector_observation": copy.deepcopy(collector_observation), "clock_evidence": copy.deepcopy(clock_evidence),
              "clock_attestation": attestation, "reconciliation_provenance": provenance,
              "observation": dict(zip(("read_started_epoch", "read_completed_epoch", "aggregation_epoch", "generated_epoch"), times)),
              "freshness_scope": "Current complete committed SQLite observation with active collector progress; original member arrival and expiry are unchanged.",
              "outer_limits": {"maximum_bytes": MAX_SNAPSHOT_BYTES, "maximum_rows": MAX_ROWS, "maximum_capture_seconds": MAX_CAPTURE_SECONDS}}
    result["payload_sha256"] = digest(result)
    if len(encoded(result)) > MAX_SNAPSHOT_BYTES:
        raise ValueError("repaired_news_snapshot_byte_bound")
    return result


def build_repaired_snapshot(source_evidence, *, collector_observation, clock_evidence,
                            read_started_epoch, read_completed_epoch, as_of_epoch, generated_epoch, bindings=None):
    replayed = replay_source_evidence(source_evidence, as_of_epoch=as_of_epoch)
    return _assemble_snapshot(source_evidence, collector_observation=collector_observation, clock_evidence=clock_evidence,
        read_started_epoch=read_started_epoch, read_completed_epoch=read_completed_epoch, as_of_epoch=as_of_epoch,
        generated_epoch=generated_epoch, bindings=bindings, replayed=replayed)


def validate_repaired_snapshot(snapshot):
    """Pure independent deterministic replay; returns the validated core fields.

    Source bytes are checked separately against the current exact seven-file
    closure. No live database or collector observation is substituted in replay.
    Caller additionally checks its own actual current-source freshness clock.
    """
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != SCHEMA:
        raise ValueError("repaired_news_schema_invalid")
    if len(encoded(snapshot)) > MAX_SNAPSHOT_BYTES:
        raise ValueError("repaired_news_snapshot_byte_bound")
    _flags(snapshot)
    if snapshot.get("status") != "current":
        raise ValueError("repaired_news_unavailable")
    sealed = dict(snapshot); expected = sealed.pop("payload_sha256", None)
    if expected != digest(sealed):
        raise ValueError("repaired_news_payload_hash_mismatch")
    bindings = snapshot.get("source_bindings")
    if not isinstance(bindings, dict) or set(bindings) != SOURCE_FILES or bindings != source_bindings():
        raise ValueError("repaired_news_source_binding_mismatch")
    key = (expected, tuple(sorted(bindings.items())))
    with _REPLAY_LOCK:
        cached = _REPLAY_CACHE.get(key)
    if cached is not None:
        return json.loads(cached)
    obs = snapshot.get("observation") or {}
    rebuilt = build_repaired_snapshot(snapshot["source_evidence"], collector_observation=snapshot["collector_observation"],
        clock_evidence=snapshot["clock_evidence"], read_started_epoch=obs.get("read_started_epoch"),
        read_completed_epoch=obs.get("read_completed_epoch"), as_of_epoch=obs.get("aggregation_epoch"),
        generated_epoch=obs.get("generated_epoch"), bindings=bindings)
    if rebuilt != snapshot:
        raise ValueError("repaired_news_replay_mismatch")
    core = {field: copy.deepcopy(snapshot[field]) for field in CORE_KEYS}
    immutable = encoded(core)
    if len(immutable) <= guard.MAX_CURRENT_SNAPSHOT_BYTES:
        with _REPLAY_LOCK:
            _REPLAY_CACHE[key] = immutable
            _REPLAY_CACHE.move_to_end(key)
            while len(_REPLAY_CACHE) > 4:
                _REPLAY_CACHE.popitem(last=False)
    return core


def capture_repaired_snapshot(data_root, *, clock=time.time):
    data = Path(data_root); news = data / "local_news_sentiment"
    clock_state, _ = _read_json(data / "state/clock_integrity_v1.json", MAX_STATE_BYTES)
    heartbeat, _ = _read_json(news / "collector_heartbeat_v1.json", MAX_STATE_BYTES)
    started = epoch(clock())
    validate_clock_state(clock_state, started); validate_collector_observation(heartbeat, started)
    database = news / "local_news_sentiment_v1.sqlite"
    since = iso(started - 86400)
    rows = []
    total = 0
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as db:
        db.execute("PRAGMA query_only=ON")
        deadline = time.monotonic() + 5
        db.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        db.execute("BEGIN")
        count = db.execute("SELECT count(*) FROM articles WHERE " + _PREDICATE, (since,)).fetchone()[0]
        if count > MAX_ROWS:
            raise ValueError("news_source_row_bound")
        cursor = db.execute("SELECT event_id,published_utc,first_seen_utc,last_seen_utc,relevant,duplicate_count,payload_json FROM articles WHERE " + _PREDICATE + " ORDER BY published_utc,event_id", (since,))
        for values in cursor:
            row = dict(zip(("event_id", "published_utc", "first_seen_utc", "last_seen_utc", "relevant", "duplicate_count", "payload_json"), values))
            raw = row["payload_json"].encode("utf-8")
            total += len(raw)
            if total > MAX_SOURCE_BYTES:
                raise ValueError("news_source_total_byte_bound")
            row["payload_sha256"] = hashlib.sha256(raw).hexdigest()
            rows.append(row)
        if len(rows) != count:
            raise ValueError("news_source_count_changed_in_transaction")
    completed = epoch(clock())
    evidence = {"schema_version": EVIDENCE_SCHEMA, "complete": True, "predicate": _PREDICATE,
                "database_snapshot_epoch": started, "minimum_publication_utc": since,
                "maximum_admissible_window_minutes": 1440, "row_count": count, "payload_bytes": total,
                "rows": rows, "rows_sha256": digest(rows)}
    asof = epoch(clock())
    # Capture an independent, fresh integrity/progress observation after the DB
    # read; do not renew stale upstream collection by merely rereading old rows.
    clock_state, _ = _read_json(data / "state/clock_integrity_v1.json", MAX_STATE_BYTES)
    heartbeat, _ = _read_json(news / "collector_heartbeat_v1.json", MAX_STATE_BYTES)
    bindings = source_bindings()
    # The first replay's computation time is part of the generation clock.
    replayed = replay_source_evidence(evidence, as_of_epoch=asof)
    generated = epoch(clock())
    if source_bindings() != bindings:
        raise ValueError("repaired_news_sources_changed_during_capture")
    return _assemble_snapshot(evidence, collector_observation=heartbeat, clock_evidence=clock_state,
        read_started_epoch=started, read_completed_epoch=completed, as_of_epoch=asof, generated_epoch=generated,
        bindings=bindings, replayed=replayed)


def atomic_write_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    raw = encoded(value)
    if len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError("repaired_news_publication_byte_bound")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", prefix=path.name + ".", suffix=".tmp", dir=path.parent, delete=False) as file:
            temporary = Path(file.name); file.write(raw); file.flush(); os.fsync(file.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def run_cycle(data_root, *, clock=time.time, previous_errors=0):
    output = Path(data_root) / "local_news_sentiment_repair_v1"
    snapshot = None
    error = ""
    published = None
    try:
        snapshot = capture_repaired_snapshot(data_root, clock=clock)
        atomic_write_json(output / "current_news_v1.json", snapshot)
        published = epoch(clock())
    except Exception as exc:
        error = type(exc).__name__ + ":" + str(exc)[:180]
        snapshot = {"schema_version": SCHEMA, "status": "unavailable", "news_state": "unavailable", "topics": [],
                    "topic_count": 0, "directional_topic_count": 0, "context_topic_count": 0,
                    "generated_utc": iso(clock()), "errors": [error], **INERT}
        snapshot["payload_sha256"] = digest(snapshot)
        try:
            atomic_write_json(output / "current_news_v1.json", snapshot)
            published = epoch(clock())
        except Exception as write_error:
            error += "|publication:" + type(write_error).__name__
            snapshot = None
    now = epoch(clock())
    heartbeat = {"schema_version": HEARTBEAT_SCHEMA, "generated_epoch": now, "generated_utc": iso(now),
        "status": "unavailable" if error else "current", "source_status": "unavailable" if error else "current",
        "phase": "research_collection", "errors": previous_errors + bool(error), "last_error": error,
        "publication_epoch": published, "snapshot_sha256": digest(snapshot) if snapshot is not None else None,
        "snapshot_generated_utc": snapshot.get("generated_utc") if snapshot else None, **INERT}
    atomic_write_json(output / "heartbeat.json", heartbeat)
    return heartbeat


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path(__file__).resolve().parent / "data/oanda_training_manager")
    parser.add_argument("--interval-sec", type=float, default=15)
    parser.add_argument("--duration-sec", type=float, default=604800)
    args = parser.parse_args(argv)
    if not 1 <= args.interval_sec <= 300 or not 0 < args.duration_sec <= 2592000:
        parser.error("bounded positive interval/duration required")
    stop = time.monotonic() + args.duration_sec
    errors = 0
    while time.monotonic() < stop:
        begun = time.monotonic()
        heartbeat = run_cycle(args.data_root, previous_errors=errors)
        errors = heartbeat["errors"]
        time.sleep(min(max(0, args.interval_sec - (time.monotonic() - begun)), max(0, stop - time.monotonic())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
