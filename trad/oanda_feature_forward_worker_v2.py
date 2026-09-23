"""Separate forward cohort with cached original-clock history and V1 scoring.

Frozen V1 ledgers stay with their existing owner and settle original targets.
This successor requires its own explicit configuration and source-bound output
directory; it cannot open an old ledger without the V2 identity metadata.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time

import oanda_feature_forward_ledger_v1 as ledger
import oanda_feature_forward_worker_v1 as base
import oanda_feature_forward_reader_v2 as reader

ROOT = Path(__file__).absolute().parent
SCHEMA = "feature_forward_operational_v2_20260916"
COHORT = "feature_forward_cached_m5_v2_20260916"
EXTENSION_KEY = "operational_extension_identity_v2"
SOURCES = (
    "oanda_feature_forward_worker_v2.py", "oanda_feature_forward_reader_v2.py",
    "oanda_feature_forward_ledger_v1.py", "oanda_feature_forward_worker_v1.py",
    "oanda_feature_forward_protocol_v1.py", "oanda_feature_move_mapping_v1.py",
    "oanda_feature_observations_v1.py", "oanda_feature_candle_inputs_v2.py",
    "oanda_feature_research_clock_v1.py", "oanda_exact_price_scoring.py")


def source_pins():
    return {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in SOURCES}


def read_config(path):
    path = ledger.safe_path(Path(path).absolute())
    raw = path.read_bytes()
    if len(raw) > 65536:
        raise ValueError("forward_v2_config_byte_bound")
    value = reader._json(raw)
    if (value.get("schema_version") != SCHEMA or value.get("cohort_id") != COHORT or
        value.get("can_place_orders") is not False or value.get("can_promote") is not False or
        value.get("research_only") is not True):
        raise ValueError("separate_research_forward_v2_config_required")
    if value.get("source_bindings") != source_pins():
        raise ValueError("forward_v2_source_closure_changed")
    if value.get("protocol_sha256") != ledger.p.PROTOCOL_SHA256:
        raise ValueError("unchanged_forward_protocol_required")
    for key in ("directory", "archive_root", "forward_frame_root", "quote_path", "clock_state"):
        value[key] = str(ledger.safe_path(value[key]))
    output = Path(value["directory"])
    parent = ROOT/"data"/"oanda_training_manager"/"operational_repair_20260916_forward_v2"
    if output.parent != parent or output.name != COHORT:
        raise ValueError("explicit_new_forward_v2_directory_required")
    if not 60 <= value.get("duration_sec", 0) <= 172800:
        raise ValueError("bounded_forward_v2_duration_required")
    if value.get("maximum_ledger_bytes") != 4096*1024*1024 or value.get("minimum_free_bytes") != 4096*1024*1024:
        raise ValueError("unchanged_forward_storage_limits_required")
    value["config_sha256"] = hashlib.sha256(raw).hexdigest()
    value["config_path"] = str(path)
    return value


class ForwardLedgerV2(ledger.ForwardLedger):
    """Additional identity seal; numerical/storage implementation stays V1."""
    def __init__(self, path, *, extension_identity, **kwargs):
        path = ledger.safe_path(Path(path).absolute())
        identity = ledger.p.canonical(extension_identity).decode()
        self.extension_identity = identity
        self.extension_pins = dict(extension_identity["source_bindings"])
        if path.exists():
            # Read-only refusal precedes the base owner's pragmas and lock file.
            with sqlite3.connect(path.as_uri()+"?mode=ro", uri=True, timeout=1) as check:
                check.execute("PRAGMA query_only=ON")
                row = check.execute("SELECT value FROM ff_meta WHERE key=?", (EXTENSION_KEY,)).fetchone()
            if row != (identity,):
                raise ValueError("old_or_foreign_forward_ledger_refused")
        existed = path.exists()
        super().__init__(path, **kwargs)
        try:
            if not existed:
                with self.transaction(65536, reserved=True):
                    self.db.execute("INSERT INTO ff_meta VALUES (?,?)", (EXTENSION_KEY, identity))
            self.verify_sources()
        except BaseException:
            self.close()
            raise

    def verify_sources(self):
        super().verify_sources()
        for name, expected in self.extension_pins.items():
            if name not in SOURCES or hashlib.sha256((ROOT/name).read_bytes()).hexdigest() != expected:
                raise ValueError("forward_v2_extension_source_changed")
        if self.db.execute("SELECT value FROM ff_meta WHERE key=?", (EXTENSION_KEY,)).fetchone() != (self.extension_identity,):
            raise ValueError("forward_v2_identity_metadata_changed")


class ForwardWorkerV2(base.ForwardWorker):
    def __init__(self, owner, *, frame_cache, **kwargs):
        self.frame_cache = frame_cache
        self.cache_error = None
        self.last_wait_refusal_bucket = None
        super().__init__(owner, mapping_reader=frame_cache, **kwargs)

    def tick(self):
        # Preparation does not authorize observations or settle without the same
        # verified clock gate. Refresh is amortized over the ordinary quote ticks.
        readiness = dict(ready=False, reason="verified_cache_unavailable")
        try:
            self.verified_clock()
        except Exception as exc:
            # Do not consult the owner clock or append refusals when the shared
            # clock gate failed. A diagnostic wall time has no ledger authority.
            diagnostic_epoch = time.time()
            report = dict(schema_version="feature_forward_worker_status_v1",
                worker="research_feature_forward_cached_v2", cohort_id=COHORT,
                can_place_orders=False, can_promote=False,
                status="verified_clock_unavailable", errors=[str(exc)[:300]],
                diagnostic_wall_epoch=diagnostic_epoch,
                generated_utc=datetime.fromtimestamp(diagnostic_epoch, timezone.utc).isoformat(),
                reader_version=reader.VERSION, frame_cache=self.frame_cache.last_report,
                decision_readiness=readiness, deferred_comparison_windows_sec=[900, 3600])
            self.last_report = report
            return report
        try:
            self.owner.verify_sources()
            self.frame_cache.refresh(as_of_epoch=self.owner.now())
            readiness = self.frame_cache.readiness(as_of_epoch=self.owner.now())
            self.cache_error = None
        except Exception as exc:
            self.cache_error = type(exc).__name__+":"+str(exc)[:240]
        previous_bucket = self.last_decision_bucket
        suppressed_bucket = None
        if not readiness["ready"]:
            # Do not consume the bucket merely because a cached endpoint is old
            # while a fresh producer publication can still arrive this bucket.
            # Quotes and all already-issued targets continue through base.tick.
            try:
                suppressed_bucket = int(self.owner.now()//ledger.p.PROTOCOL["decision_cadence_sec"])
                self.last_decision_bucket = suppressed_bucket
                if self.last_wait_refusal_bucket != suppressed_bucket and self.owner.due():
                    self.owner.refusal("forward_cache_waiting:"+readiness["reason"])
                    self.last_wait_refusal_bucket = suppressed_bucket
            except Exception as exc:
                self.cache_error = type(exc).__name__+":"+str(exc)[:240]
        try:
            report = super().tick()
        finally:
            if suppressed_bucket is not None:
                self.last_decision_bucket = previous_bucket
        report.update(worker="research_feature_forward_cached_v2", cohort_id=COHORT,
                      reader_version=reader.VERSION, frame_cache=self.frame_cache.last_report,
                      decision_readiness=readiness,
                      deferred_comparison_windows_sec=[900, 3600])
        if not readiness["ready"] and not report.get("errors"):
            report["status"] = "waiting_for_fresh_history"
        if self.cache_error:
            report.setdefault("errors", []).append("cache:"+self.cache_error)
            report["status"] = "partial_unavailable"
        return report


def run(config, *, once=False):
    from oanda_feature_research_clock_v1 import read_verified_clock
    proof = lambda: read_verified_clock(config["clock_state"])
    if proof().get("valid") is not True:
        raise ValueError("verified_forward_start_clock_required")
    directory = Path(config["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    extension = dict(schema=SCHEMA, cohort_id=COHORT, config_sha256=config["config_sha256"],
        source_bindings=config["source_bindings"], protocol_sha256=ledger.p.PROTOCOL_SHA256,
        history_contract="32 verified original-clock frames; unchanged prior-comparison math",
        old_ledger_imported=False, can_place_orders=False, can_promote=False)
    owner = ForwardLedgerV2(directory/"feature_forward_v1.sqlite", extension_identity=extension,
        max_bytes=config["maximum_ledger_bytes"], minimum_free_bytes=config["minimum_free_bytes"])
    cache = reader.VerifiedFrameCache(config["forward_frame_root"], config["archive_root"])
    worker = ForwardWorkerV2(owner, frame_cache=cache, clock_check=proof,
        archive_root=config["forward_frame_root"], forward_frame_root=config["forward_frame_root"],
        quote_path=config["quote_path"])
    started = time.monotonic()
    try:
        while True:
            report = worker.tick()
            report.update(config_path=config["config_path"], config_sha256=config["config_sha256"],
                          source_closure_sha256=ledger.p.digest(config["source_bindings"]))
            base.write_status(directory/"feature_forward_status_v1.json", report)
            if once or time.monotonic()-started >= config["duration_sec"]:
                return 0
            time.sleep(5)
    finally:
        owner.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    return run(read_config(args.config), once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
