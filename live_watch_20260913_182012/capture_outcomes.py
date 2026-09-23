"""Bounded read-only Sunday forecast settlement audit; stdlib only.

Writes only beside this helper. Does not import project modules, open a writer
connection to production, score profitability, or make broker/network calls.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time

OUTPUT = Path(__file__).resolve().parent
STUDY = OUTPUT.parent / "trad/data/oanda_training_manager/pair_local_forecast_study_v2"
SUNDAY_START = datetime(2026, 9, 13, tzinfo=timezone.utc).timestamp()
SESSION_START = datetime(2026, 9, 13, 21, tzinfo=timezone.utc).timestamp()


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def compact_quote(raw):
    if raw is None:
        return None
    quote = json.loads(raw)
    return {k: quote.get(k) for k in (
        "quote_id", "instrument", "market_epoch", "available_epoch", "bid", "ask", "pip_size", "tradeable"
    )}


QUERY = """
SELECT f.id, f.reference, f.target, f.sha, f.payload,
 p.epoch AS published, p.forecast_sha AS published_sha,
 c.epoch AS consumed, c.forecast_sha AS consumed_sha, c.publication_sha,
 e.epoch AS entered, eq.payload AS entry_quote,
 o.epoch AS settled, oq.payload AS outcome_quote,
 x.epoch AS excluded, x.reason AS exclusion_reason,
 rq.payload AS reference_quote
FROM forecasts f
LEFT JOIN publication p ON p.id=f.id AND p.epoch<=:cutoff
LEFT JOIN consumption c ON c.id=f.id AND c.epoch<=:cutoff
LEFT JOIN entries e ON e.id=f.id AND e.epoch<=:cutoff
LEFT JOIN quotes eq ON eq.id=e.quote_id
LEFT JOIN outcomes o ON o.id=f.id AND o.epoch<=:cutoff
LEFT JOIN quotes oq ON oq.id=o.quote_id
LEFT JOIN exclusions x ON x.id=f.id AND x.epoch<=:cutoff
LEFT JOIN attempts a ON a.id=f.attempt_id
LEFT JOIN quotes rq ON rq.id=a.reference_id
WHERE f.reference>=:session AND f.reference<=:cutoff
ORDER BY f.reference, f.id
"""


def audit_ledger(path, cutoff):
    started = time.time()
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1.5)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        saved = connection.execute("SELECT sha,payload FROM contract WHERE id=1").fetchone()
        contract = json.loads(saved["payload"])
        rows = list(connection.execute(QUERY, {"cutoff": cutoff, "session": SESSION_START}))
        earlier = connection.execute(
            "SELECT COUNT(*) FROM forecasts WHERE reference>=? AND reference<?", (SUNDAY_START, SESSION_START)
        ).fetchone()[0]
        connection.rollback()
    finally:
        connection.close()
    family = contract["family"]
    grace = contract["maximum_target_quote_delay_sec"]
    entry_grace = contract["maximum_entry_delay_sec"]
    counts = Counter()
    reasons = Counter()
    details = []
    for row in rows:
        payload = json.loads(row["payload"])
        arms = payload.get("forecasts", [])
        arm = arms[0] if len(arms) == 1 else {}
        issues = []
        issued = arm.get("issued_epoch")
        if not isinstance(issued, (int, float)) or not SESSION_START <= issued <= cutoff:
            # A row from a concurrent commit after the global cutoff is visible
            # in some later-opened databases; do not leak it into this snapshot.
            if isinstance(issued, (int, float)) and issued > cutoff:
                counts["issued_after_cutoff_ignored"] += 1
                continue
            issues.append("missing_or_out_of_window_issue_clock")
        counts["forecast_records"] += 1
        if digest(payload) != row["sha"]:
            issues.append("forecast_payload_hash_mismatch")
        published = row["published"] is not None
        consumed = row["consumed"] is not None
        if published:
            counts["published"] += 1
            if row["published_sha"] != row["sha"]:
                issues.append("publication_hash_mismatch")
        if consumed:
            counts["consumed"] += 1
            expected = digest({"epoch": row["published"], "forecast_sha": row["sha"]})
            if not published or row["consumed_sha"] != row["sha"] or row["publication_sha"] != expected:
                issues.append("consumption_receipt_mismatch")
        reference_quote = compact_quote(row["reference_quote"])
        entry = compact_quote(row["entry_quote"])
        outcome = compact_quote(row["outcome_quote"])
        if (len(arms) != 1 or arm.get("family") != family or arm.get("instrument") != contract["instrument"]
                or arm.get("target_epoch") != row["target"] or arm.get("reference_epoch") != row["reference"]
                or row["target"] != row["reference"] + contract["horizon_sec"]):
            issues.append("forecast_family_or_target_mismatch")
        if published and consumed and isinstance(issued, (int, float)):
            if not row["reference"] <= issued <= row["published"] <= row["consumed"] < row["target"]:
                issues.append("publication_clock_order")
        if entry:
            counts["entries"] += 1
            if (not consumed or not entry["market_epoch"] > row["consumed"]
                    or not row["consumed"] < entry["available_epoch"] < row["target"]
                    or entry["available_epoch"] > row["consumed"] + entry_grace):
                issues.append("entry_clock_contract_violation")
        if outcome:
            counts["recorded_sunday_outcomes"] += 1
            if (not entry or outcome["market_epoch"] < row["target"]
                    or outcome["available_epoch"] > row["target"] + grace
                    or row["settled"] < outcome["available_epoch"]):
                issues.append("outcome_clock_contract_violation")
        excluded = row["excluded"] is not None
        if excluded:
            counts["explicit_exclusions"] += 1
            reasons[row["exclusion_reason"]] += 1
        due = row["target"] <= cutoff
        past_grace = row["target"] + grace < cutoff
        if due:
            counts["target_due"] += 1
            counts["published_target_due"] += int(published)
            counts["due_with_outcome"] += int(outcome is not None)
            counts["due_explicitly_excluded"] += int(excluded)
        else:
            counts["target_not_yet_due"] += 1
        unresolved = outcome is None and not excluded
        if due and unresolved:
            counts["due_unresolved"] += 1
        if past_grace:
            counts["target_due_plus_grace"] += 1
            if unresolved:
                counts["due_plus_grace_overdue_unresolved"] += 1
        if consumed and entry is None and unresolved and cutoff > min(row["target"], row["consumed"] + entry_grace):
            counts["entry_deadline_overdue_unresolved"] += 1
        if published and consumed and not issues:
            counts["publication_receipts_and_sample_clocks_valid"] += 1
        if issues:
            counts["records_with_validation_issues"] += 1
        details.append({
            "decision_id": row["id"], "forecast_sha256": row["sha"],
            "reference_epoch": row["reference"], "issued_epoch": issued,
            "publication_epoch": row["published"], "consumption_epoch": row["consumed"],
            "target_epoch": row["target"], "entry_recorded_epoch": row["entered"],
            "outcome_recorded_epoch": row["settled"], "exclusion_epoch": row["excluded"],
            "exclusion_reason": row["exclusion_reason"], "side": arm.get("side"),
            "probability_up": arm.get("probability_up"), "predicted_return_bps": arm.get("predicted_return_bps"),
            "reference_quote": reference_quote, "entry_quote": entry, "outcome_quote": outcome,
            "target_due": due, "due_plus_grace_overdue_unresolved": past_grace and unresolved,
            "validation_issues": issues,
        })
    return {
        "instrument": contract["instrument"], "family": family,
        "database": str(path), "contract_sha256": saved["sha"],
        "contract_hash_valid": digest(contract) == saved["sha"],
        "read_started_utc": utc(started), "read_completed_utc": utc(time.time()),
        "target_quote_grace_sec": grace, "entry_quote_grace_sec": entry_grace,
        "sunday_preopen_forecasts_excluded": earlier, "counts": dict(counts),
        "exclusion_reasons": dict(reasons), "forecasts": details,
    }


def main():
    cutoff = time.time()
    result = {
        "schema": "live_watch_pair_local_sunday_settlements_v1",
        "captured_as_of_epoch": cutoff, "captured_as_of_utc": utc(cutoff),
        "session_start_epoch": SESSION_START, "session_start_utc": utc(SESSION_START),
        "mode": "read_only_production_sqlite_no_worker_imports",
        "denominator": {
            "forecast_filter": "original reference >= Sunday 2026-09-13 21:00 UTC and <= audit cutoff; issue <= cutoff",
            "receipt_filter": "publication/consumption/entry/outcome/exclusion record clock <= common audit cutoff",
            "due": "original target <= common cutoff; never roll target forward",
            "overdue_unresolved": "original target + immutable contract maximum_target_quote_delay_sec < cutoff; no outcome and no explicit exclusion",
            "outcomes": "only joined to the session-filtered original forecast; Friday outcomes never included",
            "cross_database_consistency": "separate short read transactions, common clock cutoff; append-only evidence may be committed during capture",
            "independence": "overlapping H1 forecasts are not independent samples",
        },
        "performance": {"computed": False, "reason": "Raw settlement/receipt audit only; full frozen evaluator input validation and executable-cost scoring not duplicated."},
        "ledgers": [], "errors": [],
    }
    paths = sorted(STUDY.glob("pairs/*/*/study.sqlite"))
    for path in paths:
        try:
            result["ledgers"].append(audit_ledger(path, cutoff))
        except Exception as exc:
            result["errors"].append({"database": str(path), "error": type(exc).__name__ + ":" + str(exc)})
    total = Counter()
    families = defaultdict(Counter)
    reasons = Counter()
    for ledger in result["ledgers"]:
        total.update(ledger["counts"])
        families[ledger["family"]].update(ledger["counts"])
        reasons.update(ledger["exclusion_reasons"])
    for key in ["forecast_records", "published", "consumed", "entries", "target_due", "published_target_due",
                "target_due_plus_grace", "due_plus_grace_overdue_unresolved", "due_unresolved",
                "explicit_exclusions", "due_explicitly_excluded", "recorded_sunday_outcomes", "due_with_outcome",
                "records_with_validation_issues", "entry_deadline_overdue_unresolved"]:
        total.setdefault(key, 0)
    result.update(ledger_paths_found=len(paths), ledgers_read=len(result["ledgers"]),
                  counts=dict(total), family_counts={k: dict(v) for k, v in families.items()},
                  exclusion_reasons=dict(reasons), capture_completed_utc=utc(time.time()),
                  capture_duration_sec=round(time.time() - cutoff, 3))
    filename = OUTPUT / ("outcome_audit_" + datetime.fromtimestamp(cutoff, timezone.utc).strftime("%Y%m%d_%H%M%S_%f") + ".json")
    data = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False)
    filename.write_text(data, encoding="utf-8")
    temporary = OUTPUT / "latest_outcomes.json.tmp"
    temporary.write_text(data, encoding="utf-8")
    temporary.replace(OUTPUT / "latest_outcomes.json")
    print(json.dumps({"audit_file": str(filename), "as_of_utc": result["captured_as_of_utc"],
                      "ledgers_read": result["ledgers_read"], "errors": result["errors"],
                      "counts": result["counts"], "exclusion_reasons": result["exclusion_reasons"],
                      "duration_sec": result["capture_duration_sec"]}, sort_keys=True))


if __name__ == "__main__":
    main()
