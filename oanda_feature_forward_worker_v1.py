"""Standalone local-file forward observer. No network, models, orders or old DBs.

One expensive shared archive read per 300-second decision bucket; intervening
ticks only read the dedicated quote snapshot and settle pending pair outcomes.
The verified-clock adapter is mandatory, including for synthetic callers.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

try:
    import oanda_feature_forward_ledger_v1 as ledger
    import oanda_feature_move_mapping_v1 as mapper
    import oanda_feature_research_clock_v1 as clock_gate
except ModuleNotFoundError:
    from trad import oanda_feature_forward_ledger_v1 as ledger
    from trad import oanda_feature_move_mapping_v1 as mapper
    from trad import oanda_feature_research_clock_v1 as clock_gate

ROOT = Path(__file__).absolute().parent
DEFAULT_ARCHIVE = ROOT / "data/oanda_training_manager/feature_observations_v1"
DEFAULT_QUOTES = ROOT / "data/oanda_training_manager/state/practice_007_market_quotes_v1.json"
DEFAULT_DIRECTORY = ROOT / "data/oanda_training_manager/feature_forward_v1"
MAX_QUOTE_BYTES = 1024*1024
MAX_STATUS_BYTES = 2*1024*1024


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate_json_key")
        result[key] = value
    return result


def read_quotes(path, *, clock):
    path = ledger.safe_path(path)
    started = ledger.p.epoch(clock())
    with path.open("rb") as handle:
        before = os.fstat(handle.fileno())
        if before.st_size > MAX_QUOTE_BYTES:
            raise ValueError("quote_snapshot_byte_bound")
        raw = handle.read(MAX_QUOTE_BYTES+1)
        after = os.fstat(handle.fileno())
    stat = path.stat()
    identity = lambda item: (item.st_dev, item.st_ino, item.st_size, item.st_mtime_ns)
    if len(raw) > MAX_QUOTE_BYTES or identity(before) != identity(after) or identity(before) != identity(stat):
        raise ValueError("quote_snapshot_changed_or_oversize")
    payload = json.loads(raw, object_pairs_hook=_unique, parse_constant=lambda value: (_ for _ in ()).throw(ValueError("nonfinite_json")))
    # The dedicated stream's first research envelope was schema 1.  Its live
    # writer now publishes schema 3 with explicit coverage/tradeability; both
    # retain the same bounded quote contract used here.
    if (not isinstance(payload, dict) or type(payload.get("schema_version")) is not int
            or payload["schema_version"] not in (1, 3)):
        raise ValueError("dedicated_quote_snapshot_schema")
    if payload.get("producer") != "practice_007_dedicated_quote_stream" or payload.get("research_only") is not True:
        raise ValueError("dedicated_research_quote_producer_required")
    quotes = payload.get("quotes")
    if not isinstance(quotes, dict) or len(quotes) > 68 or type(payload.get("quote_count")) is not int or payload["quote_count"] != len(quotes):
        raise ValueError("quote_inventory_mismatch")
    completed = ledger.p.epoch(clock())
    generated = ledger.parse_time(payload.get("generated_utc"))
    if not generated <= started <= completed or completed-generated > 75:
        raise ValueError("quote_snapshot_clock_or_age")
    return quotes, started, completed, {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                                        "generated_utc": payload["generated_utc"], "producer": payload["producer"],
                                        "schema_version": payload["schema_version"]}


class ForwardWorker:
    def __init__(self, owner, *, clock_check, archive_root=DEFAULT_ARCHIVE, quote_path=DEFAULT_QUOTES,
                 mapping_reader=mapper.read_feature_move_maps, quote_reader=read_quotes):
        if not callable(clock_check):
            raise ValueError("verified_clock_check_required")
        self.owner, self.clock_check = owner, clock_check
        self.archive_root, self.quote_path = ledger.safe_path(archive_root), ledger.safe_path(quote_path)
        self.mapping_reader, self.quote_reader = mapping_reader, quote_reader
        self.last_decision_bucket = None
        self.last_report = None
        self.last_evaluation = None
        self.run_quote_counts = Counter()

    def verified_clock(self):
        proof = self.clock_check()
        if not isinstance(proof, dict) or proof.get("valid") is not True:
            raise ValueError("verified_clock_refused:"+str(proof.get("reason", "invalid_shape") if isinstance(proof, dict) else "invalid_shape")[:200])
        return proof

    def complete_clock(self, original):
        payload = (original.get("evidence") or {}).get("clock_state")
        proof = clock_gate.validate_clock_state(payload, now_epoch=self.owner.now())
        if proof.get("valid") is not True:
            raise ValueError("original_clock_proof_expired:"+proof["reason"])
        # A fresh good replacement cannot rescue a failed original above.
        return {"original": proof, "current": self.verified_clock()}

    def observe_quotes(self, *, force_reference=False):
        original = self.verified_clock()
        quotes, start, end, receipt = self.quote_reader(self.quote_path, clock=self.owner.now)
        evidence = self.complete_clock(original)
        receipt = {**receipt, "clock_gate": evidence}
        result = self.owner.observe_quotes(quotes, read_started_epoch=start, read_completed_epoch=end, source_receipt=receipt,
                                           force_reference=force_reference)
        self.run_quote_counts.update(result)
        return result

    def tick(self):
        report = {"schema_version": "feature_forward_worker_status_v1", "can_place_orders": False,
                  "worker": "research_feature_forward_v1", "can_promote": False, "status": "observing", "errors": []}
        try:
            self.verified_clock()
        except Exception as exc:
            report.update(status="verified_clock_unavailable", errors=[str(exc)[:300]])
            # Local diagnostic timestamp is not authority for any new label.
            report["diagnostic_wall_epoch"] = time.time()
            report["generated_utc"] = datetime.fromtimestamp(report["diagnostic_wall_epoch"], timezone.utc).isoformat()
            self.last_report = report
            return report
        try:
            reference_due = self.last_decision_bucket != int(self.owner.now()//300) and self.owner.due()
            report["quotes"] = self.observe_quotes(force_reference=reference_due)
            if report["quotes"].get("refused"):
                report["errors"].append("one_or_more_quotes_refused")
        except Exception as exc:
            report["errors"].append("quotes:"+str(exc)[:240])
        now = self.owner.now()
        bucket = int(now//ledger.p.PROTOCOL["decision_cadence_sec"])
        if bucket != self.last_decision_bucket and self.owner.due():
            # Do not repeatedly decode archives after a capacity/input refusal.
            self.last_decision_bucket = bucket
            try:
                original = self.verified_clock()
                maps = self.mapping_reader(self.archive_root, as_of_utc=datetime.fromtimestamp(now, timezone.utc).isoformat(),
                                           window_secs=(300, 900, 3600), include_all_comparisons=True,
                                           reference_only=True)
                self.complete_clock(original)
                # Mapping work may take time. Acquire an actually fresh reference
                # before publication instead of carrying a pre-read stale quote.
                try:
                    report["reference_quotes"] = self.observe_quotes(force_reference=True)
                except Exception as exc:
                    report["errors"].append("reference_quotes:"+str(exc)[:240])
                self.complete_clock(original)
                self.owner.publication_guard = lambda: self.complete_clock(original)
                try:
                    report["publication"] = self.owner.publish_comparisons(maps)
                    if any(item["status"] == "admission_refused" for item in report["publication"]):
                        report["errors"].append("one_or_more_event_windows_refused")
                finally:
                    self.owner.publication_guard = None
            except Exception as exc:
                report["errors"].append("publication:"+str(exc)[:240])
                self.owner.refusal("publication:"+str(exc))
        try:
            original = self.verified_clock()
            self.owner.settlement_guard = lambda: self.complete_clock(original)
            try:
                report["settlement"] = self.owner.settle()
            finally:
                self.owner.settlement_guard = None
            if (self.last_evaluation is None or "publication" in report
                    or report["settlement"].get("scored", 0) or report["settlement"].get("unknown", 0)):
                self.last_evaluation = self.owner.summary()
            report["evaluation"] = self.last_evaluation
        except Exception as exc:
            report["errors"].append("settlement:"+str(exc)[:240])
        if report["errors"]:
            report["status"] = "partial_unavailable"
        report["owner_observed_epoch"] = self.owner.now()
        report["worker_run_quote_counts"] = dict(self.run_quote_counts)
        report["run_quote_count_scope"] = "since_this_worker_start_not_full_quote_history"
        report["last_refusal"] = self.owner.last_refusal
        report["generated_utc"] = datetime.fromtimestamp(report["owner_observed_epoch"], timezone.utc).isoformat()
        self.last_report = report
        return report


def write_status(path, payload):
    path = ledger.safe_path(path)
    if path.name != "feature_forward_status_v1.json":
        raise ValueError("owned_status_filename_required")
    raw = ledger.p.canonical(payload)+b"\n"
    if len(raw) > MAX_STATUS_BYTES:
        raise ValueError("status_byte_bound")
    temporary = path.with_name(path.name+"."+uuid.uuid4().hex+".pending")
    ledger.safe_path(temporary)
    with temporary.open("xb") as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--quote-path", type=Path, default=DEFAULT_QUOTES)
    parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--clock-state", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--duration-sec", type=int, default=172800)
    parser.add_argument("--max-ledger-mib", type=int, default=512)
    parser.add_argument("--minimum-free-mib", type=int, default=64)
    args = parser.parse_args(argv)
    if not 1 <= args.duration_sec <= 172800:
        parser.error("duration must be 1..172800 seconds")
    if not 16 <= args.max_ledger_mib <= 4096 or not 0 <= args.minimum_free_mib <= 65536:
        parser.error("ledger cap must be16..4096MiB and free floor0..65536MiB")
    # The shared verified-clock adapter is source-bound by the integrated
    # operational generation. It never starts or repairs a clock monitor.
    try:
        from oanda_feature_research_clock_v1 import read_verified_clock
    except ModuleNotFoundError:
        from trad.oanda_feature_research_clock_v1 import read_verified_clock
    clock_check = lambda: read_verified_clock(args.clock_state)
    initial_clock = clock_check()
    directory = ledger.safe_path(args.directory)
    directory.mkdir(parents=True, exist_ok=True)
    if not isinstance(initial_clock, dict) or initial_clock.get("valid") is not True:
        report = {"schema_version": "feature_forward_worker_status_v1", "worker": "research_feature_forward_v1",
                  "status": "verified_clock_unavailable", "clock": initial_clock,
                  "generated_utc": datetime.now(timezone.utc).isoformat(), "can_place_orders": False,
                  "can_promote": False, "ledger_opened": False}
        write_status(directory/"feature_forward_status_v1.json", report)
        return 2
    owner = ledger.ForwardLedger(directory/"feature_forward_v1.sqlite", max_bytes=args.max_ledger_mib*1024*1024,
                                 minimum_free_bytes=args.minimum_free_mib*1024*1024)
    worker = ForwardWorker(owner, clock_check=clock_check, archive_root=args.archive_root, quote_path=args.quote_path)
    start = time.monotonic()
    try:
        while True:
            report = worker.tick()
            write_status(directory/"feature_forward_status_v1.json", report)
            if args.once or time.monotonic()-start >= args.duration_sec:
                return 0
            time.sleep(5)
    finally:
        owner.close()


if __name__ == "__main__":
    raise SystemExit(main())
