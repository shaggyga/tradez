"""Prospective 1/5/15/30/60 minute outcomes for append-boundary pair quotes.

Reuse the immutable V1 horizon storage and executable bid/ask arithmetic in a
private module namespace. V1 output, policies and callers are never mutated.
V2 binds the V4 dedicated-stream entry cohort, truthful partial quote coverage
and verified clocks. Both sides are probes, not chosen trades or forecasts.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import types

import oanda_official_event_pair_quote_capture_v4 as entry_v4
from oanda_feature_research_clock_v1 import DEFAULT_CLOCK_PATH, read_verified_clock, validate_clock_state
from oanda_quote_transport import load_quote_snapshot

ROOT = Path(__file__).resolve().parent
LOCAL = ROOT / "data" / "oanda_training_manager" / "local_news_sentiment"
DEFAULT_INPUT = LOCAL / "official_release_fast_lane_v4.sqlite"
DEFAULT_OUTPUT = LOCAL / "official_event_pair_horizon_v2"
DEFAULT_QUOTES = ROOT / "data" / "oanda_training_manager" / "state" / "practice_007_market_quotes_v1.json"
SCHEMA_VERSION = "official_event_pair_horizon_capture_v2"
CONTRACT_ID = "official_event_pair_horizon_capture_v2_dedicated_partial_read_clock_20260913"
COHORT_ID = "official_event_pair_horizon_capture_v2_20260913a"
V1_SHA256 = "d800ab181cb90e3f6d01c1ff6cc191b903601b080f77cd25d879333d49fcbcac"


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def _source_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_legacy(entry_binding):
    path = ROOT / "oanda_official_event_pair_horizon_capture_v1.py"
    source = path.read_bytes()
    if hashlib.sha256(source).hexdigest() != V1_SHA256:
        raise ValueError("pinned_horizon_v1_changed")
    module = types.ModuleType("official_horizon_v2_private_v1")
    module.__file__ = str(path)
    exec(compile(source, str(path), "exec"), module.__dict__)
    module.SCHEMA_VERSION = SCHEMA_VERSION
    module.CONTRACT_ID, module.COHORT_ID = CONTRACT_ID, COHORT_ID
    module.ENTRY_CONTRACT_ID, module.ENTRY_COHORT_ID = entry_v4.CONTRACT_ID, entry_v4.COHORT_ID
    module.ACTIVATED_UTC = entry_v4.parse_time(entry_binding["activated_utc"])
    module.POLICY = {**module.POLICY, "entry_cohort": entry_v4.COHORT_ID,
                     "quote_producer": entry_v4.SNAPSHOT_PRODUCER,
                     "partial_snapshot_with_consistent_counts_allowed": True,
                     "verified_host_clock_required": True}
    module._snapshot_metadata_reason = lambda snapshot, now: ";".join(entry_v4.snapshot_metadata_reasons(snapshot, now))
    original_build = module.build_attempt
    original_pair = module._pair_outcome

    def strict_pair(instrument, entry, exit_row, **kwargs):
        raw = exit_row if isinstance(exit_row, dict) else {}
        if raw and (any(type(raw.get(key)) not in (int, float) for key in ("bid", "ask", "pip"))
                    or entry_v4.parse_time(raw.get("time")) is None):
            kwargs["metadata_reason"] = "invalid_numeric_or_aware_exit_quote"
        return original_pair(instrument, entry, exit_row, **kwargs)
    module._pair_outcome = strict_pair

    def checked_build(entry, horizon, snapshot, now, *, read_started_utc=None):
        if (entry.get("schema_version") != entry_v4.SCHEMA_VERSION
                or entry.get("contract_id") != entry_v4.CONTRACT_ID
                or entry.get("cohort_id") != entry_v4.COHORT_ID
                or entry.get("entry_source_binding") != entry_binding):
            raise ValueError("entry_capture_binding_mismatch")
        expected_capture = entry_v4.sha256(f"{entry['observation_id']}|{entry_v4.CONTRACT_ID}|{entry_v4.COHORT_ID}")
        if entry.get("capture_id") != expected_capture:
            raise ValueError("entry_capture_identity_mismatch")
        if entry.get("eligible_quote_count") != len(entry.get("eligible_quotes") or {}):
            raise ValueError("entry_pair_count_mismatch")
        if horizon not in module.HORIZONS_MIN or type(horizon) is not int:
            raise ValueError("unregistered_horizon")
        if read_started_utc is not None and now < read_started_utc:
            raise ValueError("read_clock_rollback")
        header, outcomes = original_build(entry, horizon, snapshot, now, read_started_utc=read_started_utc)
        header["actual_quote_read_completed_utc"] = entry_v4.iso_utc(now)
        return header, outcomes
    module.build_attempt = checked_build
    return module


def read_entry_binding(connection):
    row = connection.execute("SELECT payload_json FROM official_pair_capture_v4_binding WHERE singleton=1").fetchone()
    if row is None:
        raise ValueError("entry_cohort_not_initialized")
    binding = json.loads(row[0])
    if any(binding.get(key) != value for key, value in entry_v4._binding().items()):
        raise ValueError("entry_cohort_source_binding_changed")
    if entry_v4.parse_time(binding.get("activated_utc")) is None:
        raise ValueError("entry_cohort_activation_missing")
    return binding


def bind_output(output, entry_binding, now):
    output.execute("CREATE TABLE IF NOT EXISTS official_horizon_v2_binding "
                   "(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload_json TEXT NOT NULL)")
    expected = dict(contract_id=CONTRACT_ID, cohort_id=COHORT_ID,
                    source_sha256=_source_hash(__file__), required_v1_sha256=V1_SHA256,
                    entry_source_binding=entry_binding,
                    quote_transport_sha256=_source_hash(ROOT/"oanda_quote_transport.py"),
                    clock_guard_sha256=_source_hash(ROOT/"oanda_feature_research_clock_v1.py"))
    row = output.execute("SELECT payload_json FROM official_horizon_v2_binding WHERE singleton=1").fetchone()
    if row:
        retained = json.loads(row[0])
        if any(retained.get(key) != value for key, value in expected.items()):
            raise ValueError("horizon_v2_binding_changed_new_cohort_required")
        return retained
    existing = output.execute("SELECT COUNT(*) FROM official_event_pair_horizon_attempt").fetchone()[0]
    if existing:
        raise ValueError("horizon_v2_refuses_existing_unbound_attempts")
    expected["initialized_utc"] = entry_v4.iso_utc(now)
    output.execute("INSERT INTO official_horizon_v2_binding VALUES (1,?)", (entry_v4.canonical_json(expected),))
    for action in ("UPDATE", "DELETE"):
        output.execute(f"CREATE TRIGGER IF NOT EXISTS horizon_v2_binding_no_{action.lower()} "
                       f"BEFORE {action} ON official_horizon_v2_binding BEGIN "
                       "SELECT RAISE(ABORT,'append_only:horizon_v2_binding'); END")
    output.commit()
    return expected


@contextmanager
def ownership(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory/"owner.lock").open("a+b") as handle:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            if handle.read(1) == b"":
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)


def run_cycle(*, input_database=DEFAULT_INPUT, output_directory=DEFAULT_OUTPUT,
              quote_path=DEFAULT_QUOTES, clock_path=DEFAULT_CLOCK_PATH,
              clock=utc_now, quote_loader=load_quote_snapshot, clock_reader=read_verified_clock):
    directory = Path(output_directory)
    if Path(input_database).resolve() == (directory/"outcomes.sqlite").resolve():
        raise ValueError("entry_and_outcome_database_must_differ")
    with ownership(directory):
        started = clock()
        proof = clock_reader(clock_path)
        snapshot = {}
        errors = []
        if proof.get("valid") is not True:
            errors.append("clock:"+str(proof.get("reason")))
        else:
            try:
                snapshot = quote_loader(Path(quote_path))
            except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
                errors.append("quote_read:"+type(exc).__name__)
        read_at = clock()
        state = (proof.get("evidence") or {}).get("clock_state")
        if proof.get("valid") is True and validate_clock_state(state, now_epoch=read_at.timestamp()).get("valid") is not True:
            snapshot = {}
            errors.append("clock:initial_proof_expired")
        if read_at < started:
            snapshot = {}
            errors.append("clock:rollback")
        if snapshot:
            errors.extend("quote_metadata:"+reason for reason in entry_v4.snapshot_metadata_reasons(snapshot, read_at))
        source = sqlite3.connect(f"file:{Path(input_database).resolve().as_posix()}?mode=ro", uri=True, timeout=5)
        try:
            source.execute("PRAGMA query_only=ON")
            binding = read_entry_binding(source)
            if source.execute("SELECT COUNT(*) FROM official_event_pair_quote_capture").fetchone()[0] > 10000:
                raise ValueError("entry_population_bound_requires_new_cohort")
            base = load_legacy(binding)
            output = base.open_database(directory/"outcomes.sqlite")
            try:
                manifest = bind_output(output, binding, started)
                original_insert = base._insert_attempt
                def guarded_insert(connection, header, outcomes, **kwargs):
                    now = clock()
                    valid = (proof.get("valid") is True and now >= read_at and
                             validate_clock_state(state, now_epoch=now.timestamp()).get("valid") is True)
                    if not valid and any(row.get("valid") for row in outcomes):
                        # Clock expiry cannot persist apparently valid outcomes.
                        raise ValueError("clock_proof_expired_before_outcome_commit")
                    header["clock_proof"] = proof
                    header["clock_or_quote_errors"] = list(errors)
                    return original_insert(connection, header, outcomes, **kwargs)
                base._insert_attempt = guarded_insert
                cycle = base.capture_due(output, source, snapshot, read_at, read_started_utc=started)
                counts = base.census(output)
            finally:
                output.close()
        finally:
            source.close()
        result = dict(schema_version=SCHEMA_VERSION, contract_id=CONTRACT_ID, cohort_id=COHORT_ID,
                      generated_utc=entry_v4.iso_utc(clock()),
                      status="partial_unavailable" if errors else "ok", errors=errors,
                      cycle=cycle, counts=counts, binding=manifest, policy=base.POLICY)
        base.write_json_atomic(directory/"latest.json", result)
        base.write_json_atomic(directory/"heartbeat.json", dict(schema_version="official_event_pair_horizon_capture_v2_heartbeat",
                               worker="oanda_official_event_pair_horizon_capture_v2",
                               updated_utc=result["generated_utc"], status=result["status"], counts=counts,
                               errors=errors, research_only=True, can_place_orders=False))
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-database", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-directory", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--quote-path", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--clock-path", type=Path, default=DEFAULT_CLOCK_PATH)
    parser.add_argument("--interval-sec", type=float, default=1)
    parser.add_argument("--duration-sec", type=float, default=0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    start = time.monotonic()
    while True:
        try:
            result = run_cycle(input_database=args.input_database, output_directory=args.output_directory,
                               quote_path=args.quote_path, clock_path=args.clock_path)
            if not args.quiet:
                print(json.dumps(result, separators=(",", ":")), flush=True)
        except Exception as exc:
            # Failed cycles stay visible, never reuse a previously successful heartbeat.
            args.output_directory.mkdir(parents=True, exist_ok=True)
            error = dict(schema_version="official_event_pair_horizon_capture_v2_heartbeat",
                         worker="oanda_official_event_pair_horizon_capture_v2", status="error",
                         updated_utc=entry_v4.iso_utc(utc_now()), error=f"{type(exc).__name__}:{exc}",
                         research_only=True, can_place_orders=False)
            temporary = args.output_directory/"heartbeat.error.tmp"
            temporary.write_text(json.dumps(error), encoding="utf-8")
            os.replace(temporary, args.output_directory/"heartbeat.json")
            if args.once:
                return 1
        if args.once or (args.duration_sec > 0 and time.monotonic()-start >= args.duration_sec):
            return 0
        time.sleep(max(.25, args.interval_sec))


if __name__ == "__main__":
    raise SystemExit(main())
