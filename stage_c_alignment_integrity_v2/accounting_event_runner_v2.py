"""Publish/resume an offline synthetic event-accounting run with state snapshots."""
from __future__ import annotations

import argparse
from decimal import Decimal, localcontext
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from accounting_events_v2 import EventLedger, SCHEMA
from accounting_fastpath_v2 import OptimizedEventLedger
from accounting_event_fixtures_v2 import lifecycle_fixture, hand_oracle
from accounting_event_audit_v2 import audit_accounting
from reference_accounting_adapter_v2 import DEFAULT_TRAD, load_reference, reference_dependency_identity
from publication import RunPublisher, effective_run_identity, sha256_file, verify_completed_run

SOURCE_NAMES = ("accounting_events_v2.py", "accounting_event_fixtures_v2.py", "accounting_event_audit_v2.py",
                "accounting_event_runner_v2.py", "accounting_fastpath_v2.py", "reference_accounting_adapter_v2.py", "publication.py", "contracts.py")


def encoded(value):
    def decimal(value):
        if isinstance(value, Decimal):
            return {"__decimal__": str(value)}
        raise TypeError("unsupported_checkpoint_type")
    return (json.dumps(value, default=decimal, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def decoded(raw):
    def restore(value):
        if set(value) == {"__decimal__"}:
            number = Decimal(value["__decimal__"])
            if not number.is_finite():
                raise ValueError("nonfinite_checkpoint_decimal")
            return number
        return value
    return json.loads(raw, object_hook=restore)


def plain_bytes(reference, value):
    return (json.dumps(reference.jsonable(value), sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def required_payloads(count):
    return {"run_inputs.json", "event_ledger.jsonl", "final_state.json", "accounting_audit.json", "run_report.json",
            *(f"state_{index:06d}.json" for index in range(1, count + 1))}


def identity_for(contract, events, trad_root, engine="reference"):
    reference = load_reference(trad_root)
    source = {name: sha256_file(ROOT / name) for name in SOURCE_NAMES}
    source.update(reference_engine=sha256_file(Path(reference.__file__)), legacy_dependency=sha256_file(Path(reference.legacy.__file__)))
    source["reference_import_closure"] = reference.digest(reference_dependency_identity(trad_root))
    source["run_inputs"] = hashlib.sha256(plain_bytes(reference, {"contract": contract, "events": events})).hexdigest()
    return effective_run_identity(contract={"schema_version": "forex_event_accounting_run.v2", "accounting_contract": contract,
            "event_count": len(events), "required_payloads": sorted(required_payloads(len(events))),
            "python": ".".join(map(str, sys.version_info[:3])), "engine": engine, "mode": "offline_synthetic_only"}, dependency_hashes=source)


def restore_state(book, snapshot, events, count, identity):
    if set(snapshot) != {"schema_version", "run_identity", "events_processed", "prefix_sha256", "state", "state_sha256", "ledger"}:
        raise ValueError("invalid_snapshot_fields")
    if (snapshot["schema_version"] != "forex_accounting_state_checkpoint.v2" or snapshot["run_identity"] != identity["fingerprint"]
            or snapshot["events_processed"] != count or snapshot["prefix_sha256"] != book.reference.digest(events[:count])
            or snapshot["state_sha256"] != book.reference.digest(snapshot["state"])):
        raise ValueError("checkpoint_identity_or_prefix_mismatch")
    state, rows = snapshot["state"], snapshot["ledger"]
    if (set(state) != {"schema_version", "contract_fingerprint", "cursor", "event_ids", "arms"}
            or state["schema_version"] != book.contract["schema_version"] or state["contract_fingerprint"] != book.reference.digest(book.contract)
            or set(state["arms"]) != set(book.contract["arms"]) or len(rows) != count
            or [row["event_id"] for row in rows] != [event["event_id"] for event in events[:count]]):
        raise ValueError("checkpoint_state_or_ledger_contract_mismatch")
    expected_ids = {}
    for event in events[:count]:
        expected_ids[event["event_id"]] = book.reference.digest(event)
    if state["event_ids"] != expected_ids:
        raise ValueError("checkpoint_event_deduplication_identity_mismatch")
    for arm in state["arms"].values():
        if set(arm) != set(book._flat()):
            raise ValueError("checkpoint_arm_schema_mismatch")
        book.reconcile(arm, events[count - 1]["quotes"], events[count - 1]["epoch"])
    book.state = state
    return rows


def scorecard(book, rows, events):
    final = {name: book.reconcile(arm, events[-1]["quotes"], events[-1]["epoch"])
             for name, arm in book.state["arms"].items()}
    output = {}
    with localcontext(book.reference.CTX):
        for name, state in final.items():
            closes = [leg for row in rows if row.get("arm") == name and row.get("kind") == "fill"
                      for leg in row.get("receipt", {}).get("legs", []) if leg.get("kind") == "close"]
            gains = sum((book.reference.number(leg["realized_usd"]) for leg in closes if book.reference.number(leg["realized_usd"]) > 0), Decimal(0))
            losses = -sum((book.reference.number(leg["realized_usd"]) for leg in closes if book.reference.number(leg["realized_usd"]) < 0), Decimal(0))
            output[name] = {**state, "starting_capital_usd": book.capital,
                "net_account_pnl_usd": None if state["equity_usd"] is None else state["equity_usd"] - book.capital,
                "return_fraction": None if state["equity_usd"] is None else (state["equity_usd"] - book.capital) / book.capital,
                "closed_lot_legs": len(closes), "gross_close_profit_factor": gains / losses if losses else None,
                "profit_factor_status": "defined" if losses else "undefined_no_closed_lots" if not closes else "undefined_no_losing_close_legs",
                "rejected_events": sum(row.get("arm") == name and row.get("receipt", {}).get("status") == "rejected" for row in rows)}
    return output


def run(contract, events, *, run_id, runs_dir, trad_root=DEFAULT_TRAD, resume=False, crash_after=None, engine="reference"):
    if not isinstance(events, list) or not 1 <= len(events) <= 256:
        raise ValueError("bounded_nonempty_event_tape_required")
    if engine not in {"reference", "optimized"}:
        raise ValueError("unsupported_accounting_engine")
    book = (EventLedger if engine == "reference" else OptimizedEventLedger)(contract, trad_root=trad_root)
    for event in events:
        book._validate_event(event)
    identity = identity_for(contract, events, trad_root, engine)
    publisher = RunPublisher(runs_dir, run_id, identity)
    if (publisher.root / "COMPLETION_MANIFEST.json").exists():
        verify_completed_run(publisher.root, identity)
        return {"status": "verified_completed", "run_id": run_id, "identity": identity["fingerprint"]}
    publisher.acquire(recover=resume)
    try:
        payloads = [publisher.write_or_validate_payload("run_inputs.json", plain_bytes(book.reference, {"contract": contract, "events": events}))]
        latest, rows = 0, []
        for count in range(1, len(events) + 1):
            name = f"state_{count:06d}.json"
            raw = publisher.read_verified_payload(name)
            if raw is None:
                break
            rows = restore_state(book, decoded(raw), events, count, identity)
            payloads.append(publisher.write_or_validate_payload(name, raw))
            latest = count
        for index in range(latest, len(events)):
            rows.append(book.apply(events[index]))
            count = index + 1
            snapshot = {"schema_version": "forex_accounting_state_checkpoint.v2", "run_identity": identity["fingerprint"],
                        "events_processed": count, "prefix_sha256": book.reference.digest(events[:count]),
                        "state": book.state, "state_sha256": book.reference.digest(book.state), "ledger": rows}
            payloads.append(publisher.write_or_validate_payload(f"state_{count:06d}.json", encoded(snapshot)))
            if crash_after == count:
                os._exit(91)
        arithmetic = audit_accounting(contract, events, rows)
        report = {"schema_version": "forex_event_accounting_report.v2", "status": "synthetic_accounting_only_not_market_evidence",
                  "run_identity": identity["fingerprint"], "engine": engine, "event_count": len(events), "arms": scorecard(book, rows, events),
                  "accounting_oracle": arithmetic,
                  "limitations": ["synthetic_explicit_fill_evidence_only", "fixed_decision_notional_margin_assumption_not_broker_schedule",
                                  "supplied_financing_scenario_not_historical_rates", "optimized_close_kernel_shares_lifecycle_and_risk; full_campaign_scaling_unverified",
                                  "resting_triggers_and_fills_require_explicit_synthetic_evidence_no_intrabar_inference",
                                  "no_real_order_routing", "no_fitted_forecast_or_policy_evidence"]}
        payloads.extend([
            publisher.write_or_validate_payload("event_ledger.jsonl", b"".join(plain_bytes(book.reference, row) for row in rows)),
            publisher.write_or_validate_payload("final_state.json", encoded(book.state)),
            publisher.write_or_validate_payload("accounting_audit.json", plain_bytes(book.reference, arithmetic)),
            publisher.write_or_validate_payload("run_report.json", plain_bytes(book.reference, report)),
        ])
        if crash_after == 0:
            os._exit(91)
        publisher.complete(payloads, required_payloads(len(events)))
    except BaseException:
        if publisher._owner_token is not None:
            publisher.release()
        raise
    return {"status": "completed", "run_id": run_id, "identity": identity["fingerprint"], "events": len(events),
            "independent_event_arm_rows": arithmetic.get("event_arm_rows_checked")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--runs-dir", type=Path, default=ROOT / "runs")
    parser.add_argument("--trad-root", type=Path, default=DEFAULT_TRAD)
    parser.add_argument("--input", type=Path, help="explicit synthetic contract/events JSON, otherwise use declared fixture")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--engine", choices=["reference", "optimized"], default="reference")
    parser.add_argument("--test-crash-after-event", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.input:
        supplied = json.loads(args.input.read_text(encoding="utf-8"))
        if set(supplied) != {"contract", "events"}:
            raise ValueError("exact_contract_and_events_input_required")
        contract, events = supplied["contract"], supplied["events"]
    else:
        # Resolve equivalent restored roots rather than importing original trad.
        contract, events = lifecycle_fixture(args.trad_root)
    result = run(contract, events, run_id=args.run_id, runs_dir=args.runs_dir, trad_root=args.trad_root,
                 resume=args.resume, crash_after=args.test_crash_after_event, engine=args.engine)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
