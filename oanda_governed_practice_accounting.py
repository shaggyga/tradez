#!/usr/bin/env python3
"""Passive Practice-007 attribution and routeability checks.

This module has no broker client and no order-submission function.  It reads the
existing execution ledger, account snapshot, allocator proof, and execution
audit.  The routeability sentinel calls only the executor's pure order-builder.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_practice_shadow_strategy_lab import build_practice_order
except ModuleNotFoundError:
    from trad.oanda_practice_shadow_strategy_lab import build_practice_order


ROOT = Path(__file__).resolve().parent
STATE = ROOT / "data" / "oanda_training_manager" / "state"
REPORTS = ROOT / "data" / "oanda_training_manager" / "reports"
DEFAULT_DATABASE = STATE / "governed_practice_accounting_v1.sqlite"
DEFAULT_STATE = STATE / "governed_practice_accounting_v1.json"
DEFAULT_CONFIG = ROOT / "config" / "evidence_operations_v1.json"
DEFAULT_ACCOUNT = STATE / "account_007_dashboard_v1.json"
DEFAULT_SIGNAL_DATABASE = STATE / "practice_007_signal_feed_v1.sqlite"
DEFAULT_ALLOCATOR_DATABASE = STATE / "allocator_proof_v1.sqlite"
DEFAULT_AUDIT = REPORTS / "live_execution_audit_v1.jsonl"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def finite_or_none(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return rows
    for line in lines:
        try:
            row = json.loads(line)
        except (ValueError, json.JSONDecodeError):
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def initialize_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS account_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            observed_utc TEXT NOT NULL,
            account_suffix TEXT NOT NULL,
            last_transaction_id TEXT,
            balance REAL NOT NULL,
            nav REAL NOT NULL,
            cumulative_pl REAL NOT NULL,
            margin_used REAL NOT NULL,
            open_trade_count INTEGER NOT NULL,
            pending_order_count INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS execution_attributions (
            client_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL,
            submitted_epoch REAL NOT NULL,
            status TEXT NOT NULL,
            trade_id TEXT,
            accounting_bucket TEXT NOT NULL,
            policy_id TEXT,
            cohort_id TEXT,
            allocator_id TEXT,
            family TEXT,
            realized_pl REAL,
            realized_pips REAL,
            modeled_cost_pips REAL,
            realized_cost_pips REAL,
            adverse_slippage_pips REAL,
            forecast_to_submit_ms REAL,
            order_roundtrip_ms REAL,
            attribution_valid INTEGER NOT NULL,
            attribution_reason TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS routeability_sentinels (
            sentinel_date TEXT PRIMARY KEY,
            observed_utc TEXT NOT NULL,
            passed INTEGER NOT NULL,
            environment TEXT NOT NULL,
            account_suffix TEXT NOT NULL,
            order_sha256 TEXT NOT NULL,
            checks_json TEXT NOT NULL,
            submission_attempted INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS accounting_integrity_events (
            event_id TEXT PRIMARY KEY,
            observed_utc TEXT NOT NULL,
            event_type TEXT NOT NULL,
            client_id TEXT,
            details_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS account_snapshot_validity (
            snapshot_id TEXT PRIMARY KEY,
            classified_utc TEXT NOT NULL,
            is_valid INTEGER NOT NULL,
            snapshot_state TEXT NOT NULL,
            reason TEXT NOT NULL,
            details_json TEXT NOT NULL,
            FOREIGN KEY(snapshot_id) REFERENCES account_snapshots(snapshot_id)
        );
        CREATE TRIGGER IF NOT EXISTS account_snapshots_reject_unavailable BEFORE INSERT ON account_snapshots
        WHEN json_extract(NEW.payload_json, '$.accounts[0].ok') = 0
          OR lower(COALESCE(json_extract(NEW.payload_json, '$.aggregate.snapshot_state'), '')) = 'unavailable'
          OR lower(COALESCE(json_extract(NEW.payload_json, '$.aggregate.snapshot_state'), '')) LIKE 'retained_stale%'
        BEGIN SELECT RAISE(IGNORE); END;
        CREATE TRIGGER IF NOT EXISTS account_snapshots_no_update BEFORE UPDATE ON account_snapshots
        BEGIN SELECT RAISE(ABORT, 'account snapshots are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS account_snapshots_no_delete BEFORE DELETE ON account_snapshots
        BEGIN SELECT RAISE(ABORT, 'account snapshots are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS execution_attributions_no_update BEFORE UPDATE ON execution_attributions
        BEGIN SELECT RAISE(ABORT, 'execution attributions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS execution_attributions_no_delete BEFORE DELETE ON execution_attributions
        BEGIN SELECT RAISE(ABORT, 'execution attributions are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS routeability_sentinels_no_update BEFORE UPDATE ON routeability_sentinels
        BEGIN SELECT RAISE(ABORT, 'routeability sentinels are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS routeability_sentinels_no_delete BEFORE DELETE ON routeability_sentinels
        BEGIN SELECT RAISE(ABORT, 'routeability sentinels are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS accounting_integrity_no_update BEFORE UPDATE ON accounting_integrity_events
        BEGIN SELECT RAISE(ABORT, 'accounting integrity events are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS accounting_integrity_no_delete BEFORE DELETE ON accounting_integrity_events
        BEGIN SELECT RAISE(ABORT, 'accounting integrity events are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS account_snapshot_validity_no_update BEFORE UPDATE ON account_snapshot_validity
        BEGIN SELECT RAISE(ABORT, 'account snapshot validity is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS account_snapshot_validity_no_delete BEFORE DELETE ON account_snapshot_validity
        BEGIN SELECT RAISE(ABORT, 'account snapshot validity is immutable'); END;
        """
    )
    connection.commit()
    return connection


def account_record(payload: dict[str, Any]) -> dict[str, Any]:
    accounts = payload.get("accounts") if isinstance(payload.get("accounts"), list) else []
    account = next(
        (row for row in accounts if isinstance(row, dict) and str(row.get("account_id", "")).endswith("-007")),
        {},
    )
    return account if isinstance(account, dict) else {}


def account_snapshot_availability(payload: dict[str, Any]) -> dict[str, Any]:
    """Classify whether a broker snapshot can support financial/flatness claims."""
    account = account_record(payload)
    aggregate = payload.get("aggregate") if isinstance(payload.get("aggregate"), dict) else {}
    aggregate_state = str(aggregate.get("snapshot_state") or "").strip().lower()
    explicit_error = str(account.get("error") or payload.get("error") or "").strip()
    status_code = account.get("status_code")
    reasons: list[str] = []
    if not account:
        reasons.append("account_row_missing")
    if account.get("ok") is False:
        reasons.append("broker_snapshot_failed")
    if aggregate_state == "unavailable" or aggregate_state.startswith("retained_stale"):
        reasons.append(aggregate_state or "aggregate_snapshot_unavailable")
    for field in ("account_values_current", "positions_current", "orders_current"):
        if account.get(field) is False or aggregate.get(field) is False:
            reasons.append(field + "_false")
    required_numeric = (
        "balance", "NAV", "pl", "marginUsed", "openTradeCount", "pendingOrderCount"
    )
    missing = [field for field in required_numeric if finite_or_none(account.get(field)) is None]
    if missing:
        reasons.append("missing_required_values")
    current = not reasons
    state = "current" if current else (
        aggregate_state or ("unavailable" if account.get("ok") is False else "invalid")
    )
    return {
        "current": current,
        "snapshot_state": state,
        "reason": "current" if current else reasons[0],
        "reasons": sorted(set(reasons)),
        "status_code": status_code,
        "error": explicit_error or None,
        "account": account,
    }


def classify_unclassified_snapshots(connection: sqlite3.Connection) -> dict[str, int]:
    """Append validity classifications without mutating immutable financial rows."""
    rows = connection.execute(
        "SELECT snapshot_id,payload_json FROM account_snapshots "
        "WHERE snapshot_id NOT IN (SELECT snapshot_id FROM account_snapshot_validity)"
    ).fetchall()
    valid = quarantined = 0
    for snapshot_id, payload_json in rows:
        try:
            payload = json.loads(str(payload_json))
        except (TypeError, ValueError, json.JSONDecodeError):
            payload = {}
        availability = account_snapshot_availability(payload if isinstance(payload, dict) else {})
        is_valid = bool(availability["current"])
        connection.execute(
            "INSERT OR IGNORE INTO account_snapshot_validity VALUES (?,?,?,?,?,?)",
            (
                str(snapshot_id), utc_now(), int(is_valid),
                str(availability["snapshot_state"]), str(availability["reason"]),
                canonical_json({
                    "reasons": availability["reasons"],
                    "status_code": availability["status_code"],
                    "error": availability["error"],
                }),
            ),
        )
        valid += int(is_valid)
        quarantined += int(not is_valid)
    connection.commit()
    return {"classified": len(rows), "valid": valid, "quarantined": quarantined}


def _record_unavailable_integrity(
    connection: sqlite3.Connection, payload: dict[str, Any], availability: dict[str, Any]
) -> None:
    observed = str(payload.get("time") or payload.get("generated_utc") or utc_now())
    details = {
        "observed_utc": observed,
        "snapshot_state": availability["snapshot_state"],
        "reason": availability["reason"],
        "reasons": availability["reasons"],
        "status_code": availability["status_code"],
        "error": availability["error"],
    }
    event_id = "account_snapshot_unavailable_" + stable_hash(details)[:32]
    connection.execute(
        "INSERT OR IGNORE INTO accounting_integrity_events VALUES (?,?,?,?,?)",
        (event_id, utc_now(), "account_snapshot_unavailable", None, canonical_json(details)),
    )
    connection.commit()


def record_account_snapshot(
    connection: sqlite3.Connection, payload: dict[str, Any]
) -> dict[str, Any]:
    availability = account_snapshot_availability(payload)
    if not availability["current"]:
        _record_unavailable_integrity(connection, payload, availability)
        return {
            "recorded": False,
            "state": "unavailable",
            "reason": availability["reason"],
            "reasons": availability["reasons"],
            "status_code": availability["status_code"],
            "error": availability["error"],
        }
    account = availability["account"]
    observed = str(payload.get("time") or utc_now())
    transaction = str(account.get("lastTransactionID") or "")
    snapshot_id = "practice007_" + stable_hash(
        {"observed": observed, "transaction": transaction, "balance": account.get("balance")}
    )[:32]
    row = {
        "snapshot_id": snapshot_id,
        "observed_utc": observed,
        "account_suffix": "-007",
        "last_transaction_id": transaction,
        "balance": finite(account.get("balance")),
        "nav": finite(account.get("NAV")),
        "cumulative_pl": finite(account.get("pl")),
        "margin_used": finite(account.get("marginUsed")),
        "open_trade_count": int(finite(account.get("openTradeCount"))),
        "pending_order_count": int(finite(account.get("pendingOrderCount"))),
    }
    connection.execute(
        "INSERT OR IGNORE INTO account_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (*row.values(), canonical_json(payload)),
    )
    connection.execute(
        "INSERT OR IGNORE INTO account_snapshot_validity VALUES (?,?,?,?,?,?)",
        (
            snapshot_id, utc_now(), 1, "current", "current",
            canonical_json({"reasons": [], "status_code": account.get("status_code")}),
        ),
    )
    connection.commit()
    return {"recorded": True, "state": "current", **row}


def execution_rows(path: Path) -> list[dict[str, Any]]:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.execute("PRAGMA query_only=ON")
    try:
        rows = connection.execute(
            "SELECT client_id,candidate_id,submitted_epoch,status,trade_id,payload_json FROM executions ORDER BY submitted_epoch"
        ).fetchall()
    finally:
        connection.close()
    output: list[dict[str, Any]] = []
    for client_id, candidate_id, submitted, status, trade_id, payload_json in rows:
        try:
            payload = json.loads(str(payload_json))
        except (ValueError, json.JSONDecodeError):
            payload = {}
        output.append(
            {
                "client_id": str(client_id),
                "candidate_id": str(candidate_id),
                "submitted_epoch": finite(submitted),
                "status": str(status),
                "trade_id": str(trade_id or ""),
                "payload": payload if isinstance(payload, dict) else {},
            }
        )
    return output


def classify_execution(
    row: dict[str, Any], audit_by_trade: dict[str, dict[str, Any]], config: dict[str, Any]
) -> dict[str, Any]:
    payload = row["payload"]
    audit = audit_by_trade.get(row["trade_id"], {})
    policy_id = str(payload.get("execution_policy_id") or "")
    cohort_id = str(
        payload.get("proof_cohort_id")
        or payload.get("governed_cohort_id")
        or payload.get("cohort_id")
        or ""
    )
    allocator_id = str(
        payload.get("allocator_cohort_id")
        or payload.get("governed_allocator_id")
        or ""
    )
    governed_claim = bool(
        payload.get("governed_canary")
        or cohort_id
        or allocator_id
        or str(policy_id).startswith("governed_")
    )
    attribution_valid = not governed_claim or bool(cohort_id and allocator_id)
    if governed_claim and attribution_valid:
        bucket = "governed_canary"
        reason = "exact_cohort_and_allocator_ids_present"
    elif governed_claim:
        bucket = "unattributed_governed_blocked"
        reason = "fail_closed_missing_exact_cohort_or_allocator_id"
    else:
        bucket = str((config.get("practice_accounting") or {}).get("legacy_bucket") or "legacy_pre_governance")
        reason = "execution_predates_governed_attribution_contract"
    telemetry = payload.get("execution_telemetry") if isinstance(payload.get("execution_telemetry"), dict) else {}
    spread = finite(payload.get("spread_pips"))
    realized_cost = finite(audit.get("entry_half_spread_cost_account_currency")) + finite(
        audit.get("exit_half_spread_cost_account_currency")
    )
    return {
        **row,
        "accounting_bucket": bucket,
        "policy_id": policy_id,
        "cohort_id": cohort_id,
        "allocator_id": allocator_id,
        "family": str(payload.get("family") or audit.get("family") or ""),
        "realized_pl": None if not audit else finite(audit.get("realized_pl_account_currency")),
        "realized_pips": None if not audit else finite(audit.get("realized_pips")),
        "modeled_cost_pips": spread + finite((config.get("allocator") or {}).get("modeled_slippage_pips"), 0.25),
        "realized_cost_pips": None if not audit else realized_cost,
        "adverse_slippage_pips": finite(telemetry.get("adverse_slippage_pips")),
        "forecast_to_submit_ms": finite(telemetry.get("forecast_to_submit_ms")),
        "order_roundtrip_ms": finite(telemetry.get("order_roundtrip_ms")),
        "attribution_valid": attribution_valid,
        "attribution_reason": reason,
        "audit": audit,
    }


def ingest_executions(
    connection: sqlite3.Connection,
    signal_database: Path,
    audit_path: Path,
    config: dict[str, Any],
) -> dict[str, int]:
    audit_by_trade = {
        str(row.get("trade_id")): row
        for row in read_jsonl(audit_path)
        if row.get("trade_id") is not None
    }
    inserted = 0
    integrity = 0
    for source in execution_rows(signal_database):
        row = classify_execution(source, audit_by_trade, config)
        cursor = connection.execute(
            "INSERT OR IGNORE INTO execution_attributions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                row["client_id"], row["candidate_id"], row["submitted_epoch"], row["status"],
                row["trade_id"], row["accounting_bucket"], row["policy_id"], row["cohort_id"],
                row["allocator_id"], row["family"], row["realized_pl"], row["realized_pips"],
                row["modeled_cost_pips"], row["realized_cost_pips"], row["adverse_slippage_pips"],
                row["forecast_to_submit_ms"], row["order_roundtrip_ms"], int(row["attribution_valid"]),
                row["attribution_reason"], canonical_json({"execution": source, "audit": row["audit"]}),
            ),
        )
        inserted += int(cursor.rowcount > 0)
        if not row["attribution_valid"]:
            event = {
                "client_id": row["client_id"],
                "reason": row["attribution_reason"],
                "cohort_id": row["cohort_id"],
                "allocator_id": row["allocator_id"],
            }
            cursor = connection.execute(
                "INSERT OR IGNORE INTO accounting_integrity_events VALUES (?,?,?,?,?)",
                (
                    "accounting_integrity_" + stable_hash(event)[:32], utc_now(),
                    "governed_attribution_blocked", row["client_id"], canonical_json(event),
                ),
            )
            integrity += int(cursor.rowcount > 0)
    connection.commit()
    return {"inserted_attributions": inserted, "new_integrity_events": integrity}


def routeability_sentinel(
    connection: sqlite3.Connection,
    account_payload: dict[str, Any],
    config: dict[str, Any],
    *,
    observed_utc: str | None = None,
) -> dict[str, Any]:
    observed = observed_utc or utc_now()
    day = observed[:10]
    availability = account_snapshot_availability(account_payload)
    existing = connection.execute(
        "SELECT observed_utc,passed,checks_json FROM routeability_sentinels WHERE sentinel_date=?",
        (day,),
    ).fetchone()
    if existing:
        stored_checks = json.loads(str(existing[2]))
        if not availability["current"]:
            stored_checks = {
                **stored_checks,
                "account_state_current": False,
                "account_state_reason": availability["reason"],
            }
        return {
            "sentinel_date": day,
            "observed_utc": str(existing[0]),
            "passed": bool(existing[1]) and bool(availability["current"]),
            "stored_passed": bool(existing[1]),
            "checks": stored_checks,
            "submission_attempted": False,
            "already_recorded": True,
            "account_state_current": bool(availability["current"]),
            "reason": (
                "stored_routeability_sentinel"
                if availability["current"]
                else "account_state_unavailable"
            ),
        }
    if not availability["current"]:
        return {
            "sentinel_date": day,
            "observed_utc": observed,
            "passed": False,
            "stored_passed": None,
            "checks": {
                "account_state_current": False,
                "account_state_reason": availability["reason"],
            },
            "submission_attempted": False,
            "already_recorded": False,
            "account_state_current": False,
            "reason": "account_state_unavailable",
        }
    account = account_record(account_payload)
    environment = str(account.get("env") or account_payload.get("environment") or "")
    account_id = str(account.get("account_id") or "")
    fixture = {
        "direction": "buy", "pip": 0.0001, "ask": 1.1002, "bid": 1.1000,
        "stop_loss_pips": 8.0, "take_profit_r": 1.5, "execution_exit_horizon_sec": 3600,
        "lane_id": "routeability_sentinel.synthetic", "instrument": "EUR_USD",
        "signal_confidence": 0.60, "open_ended_profit": False,
    }
    built = build_practice_order(fixture, 1, 0.5, f"route-sentinel-{day.replace('-', '')}")
    order = built.get("order") if isinstance(built, dict) else {}
    checks = {
        "account_state_current": True,
        "practice_environment": environment == "practice",
        "account_suffix_007": account_id.endswith("-007"),
        "market_order": order.get("type") == "MARKET",
        "fok": order.get("timeInForce") == "FOK",
        "open_only": order.get("positionFill") == "OPEN_ONLY",
        "one_unit": order.get("units") == "1",
        "price_bound_present": bool(order.get("priceBound")),
        "broker_stop_present": bool(order.get("stopLossOnFill", {}).get("price")),
        "client_tag_present": order.get("clientExtensions", {}).get("tag") == "strategy_lab_top",
        "submission_function_absent": True,
        "submission_forbidden_respected": True,
    }
    passed = all(bool(value) for value in checks.values())
    connection.execute(
        "INSERT INTO routeability_sentinels VALUES (?,?,?,?,?,?,?,?)",
        (
            day, observed, int(passed), environment, "-007", stable_hash(order),
            canonical_json(checks), 0,
        ),
    )
    connection.commit()
    return {
        "sentinel_date": day, "observed_utc": observed, "passed": passed,
        "stored_passed": passed, "checks": checks, "submission_attempted": False,
        "already_recorded": False, "account_state_current": True,
        "reason": "routeability_fixture_passed" if passed else "routeability_fixture_failed",
    }


def _group_rows(rows: list[sqlite3.Row], key: str) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row[key] or "unassigned")
        item = output.setdefault(
            name,
            {"attempts": 0, "fills": 0, "audited_closed_trades": 0, "realized_pl": 0.0, "realized_pips": 0.0},
        )
        item["attempts"] += 1
        item["fills"] += int(str(row["status"]) == "filled")
        if row["realized_pl"] is not None:
            item["audited_closed_trades"] += 1
            item["realized_pl"] += finite(row["realized_pl"])
            item["realized_pips"] += finite(row["realized_pips"])
    for item in output.values():
        item["realized_pl"] = round(item["realized_pl"], 6)
        item["realized_pips"] = round(item["realized_pips"], 6)
    return output


def allocator_operational_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"available": False}
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=30.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        active = connection.execute(
            "SELECT cohort_id FROM allocator_cohorts WHERE phase='discovery' "
            "ORDER BY cohort_start_utc DESC LIMIT 1"
        ).fetchone()
        if active is None:
            return {"available": True, "active_cohort": None, "decision_count": 0}
        active_cohort = str(active["cohort_id"])
        counts = connection.execute(
            "SELECT COUNT(*) decisions, SUM(CASE WHEN selected_candidate_id IS NULL THEN 1 ELSE 0 END) no_trade, "
            "SUM(policy_eligible_count) eligible FROM decisions WHERE cohort_id=?",
            (active_cohort,),
        ).fetchone()
        rejected = connection.execute(
            "SELECT COUNT(*) FROM decisions WHERE cohort_id=? AND best_rejected_candidate_id IS NOT NULL",
            (active_cohort,),
        ).fetchone()[0]
        outcomes = connection.execute(
            "SELECT o.comparator,COUNT(*) n,SUM(o.executable_net_pips) net FROM policy_outcomes o "
            "JOIN decisions d ON d.decision_id=o.decision_id WHERE d.cohort_id=? GROUP BY o.comparator",
            (active_cohort,),
        ).fetchall()
        superseded = connection.execute(
            "SELECT COUNT(*) FROM allocator_cohorts WHERE phase='discovery' AND cohort_id<>?",
            (active_cohort,),
        ).fetchone()[0]
    finally:
        connection.close()
    return {
        "available": True,
        "active_cohort": active_cohort,
        "superseded_engineering_cohort_count": int(superseded or 0),
        "decision_count": int(counts["decisions"] or 0),
        "explicit_no_trade_count": int(counts["no_trade"] or 0),
        "eligible_candidate_observations": int(counts["eligible"] or 0),
        "decisions_with_best_rejected_alternative": int(rejected or 0),
        "counterfactual_by_arm": {
            str(row["comparator"]): {"matured": int(row["n"]), "net_pips": round(finite(row["net"]), 6)}
            for row in outcomes
        },
    }


def accounting_summary(
    connection: sqlite3.Connection,
    account_payload: dict[str, Any],
    allocator_database: Path,
) -> dict[str, Any]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute("SELECT * FROM execution_attributions ORDER BY submitted_epoch").fetchall()
    latency = [finite(row["forecast_to_submit_ms"]) for row in rows if finite(row["forecast_to_submit_ms"]) > 0]
    roundtrip = [finite(row["order_roundtrip_ms"]) for row in rows if finite(row["order_roundtrip_ms"]) > 0]
    slippage = [finite(row["adverse_slippage_pips"]) for row in rows if row["adverse_slippage_pips"] is not None]
    governed = [row for row in rows if str(row["accounting_bucket"]) == "governed_canary"]
    virtual: dict[str, dict[str, Any]] = {}
    for row in governed:
        cohort = str(row["cohort_id"])
        item = virtual.setdefault(cohort, {"start_equity": 0.0, "realized_equity": 0.0, "closed_trade_count": 0})
        if row["realized_pl"] is not None:
            item["realized_equity"] += finite(row["realized_pl"])
            item["closed_trade_count"] += 1
    for item in virtual.values():
        item["realized_equity"] = round(item["realized_equity"], 6)
    availability = account_snapshot_availability(account_payload)
    account = availability["account"]
    last_valid = connection.execute(
        "SELECT s.observed_utc,s.last_transaction_id,s.balance,s.nav,s.cumulative_pl,"
        "s.margin_used,s.open_trade_count,s.pending_order_count "
        "FROM account_snapshots s JOIN account_snapshot_validity v "
        "ON v.snapshot_id=s.snapshot_id WHERE v.is_valid=1 "
        "ORDER BY s.observed_utc DESC LIMIT 1"
    ).fetchone()
    current = bool(availability["current"])
    continuity = {
        "account_state_current": current,
        "snapshot_state": availability["snapshot_state"],
        "reason": availability["reason"],
        "status_code": availability["status_code"],
        "error": availability["error"],
        "balance": finite_or_none(account.get("balance")) if current else None,
        "nav": finite_or_none(account.get("NAV")) if current else None,
        "cumulative_pl": finite_or_none(account.get("pl")) if current else None,
        "open_trade_count": (
            int(finite_or_none(account.get("openTradeCount"))) if current else None
        ),
        "pending_order_count": (
            int(finite_or_none(account.get("pendingOrderCount"))) if current else None
        ),
        "flatness_known": current,
        "account_flat": (
            int(finite_or_none(account.get("openTradeCount"))) == 0
            and int(finite_or_none(account.get("pendingOrderCount"))) == 0
        ) if current else None,
        "use_for_governed_inference": False,
        "last_verified_stale_context": None,
    }
    if last_valid is not None:
        continuity["last_verified_stale_context"] = {
            "observed_utc": str(last_valid[0]),
            "last_transaction_id": str(last_valid[1] or ""),
            "balance": float(last_valid[2]),
            "nav": float(last_valid[3]),
            "cumulative_pl": float(last_valid[4]),
            "margin_used": float(last_valid[5]),
            "open_trade_count": int(last_valid[6]),
            "pending_order_count": int(last_valid[7]),
            "stale_context_only": not current,
        }
    return {
        "account_operational_continuity": continuity,
        "by_bucket": _group_rows(rows, "accounting_bucket"),
        "by_policy": _group_rows(rows, "policy_id"),
        "by_cohort": _group_rows(rows, "cohort_id"),
        "by_allocator": _group_rows(rows, "allocator_id"),
        "by_family": _group_rows(rows, "family"),
        "virtual_equity_by_governed_cohort": virtual,
        "execution_transport": {
            "latency_observation_count": len(latency),
            "average_forecast_to_submit_ms": None if not latency else round(sum(latency) / len(latency), 6),
            "average_order_roundtrip_ms": None if not roundtrip else round(sum(roundtrip) / len(roundtrip), 6),
            "average_adverse_slippage_pips": None if not slippage else round(sum(slippage) / len(slippage), 6),
        },
        "unattributed_governed_execution_count": sum(not bool(row["attribution_valid"]) for row in rows),
        "allocator_counterfactual_and_unavailable_capital": allocator_operational_summary(allocator_database),
    }


def run_accounting_cycle(
    *,
    database_path: Path = DEFAULT_DATABASE,
    state_path: Path = DEFAULT_STATE,
    config_path: Path = DEFAULT_CONFIG,
    account_path: Path = DEFAULT_ACCOUNT,
    signal_database: Path = DEFAULT_SIGNAL_DATABASE,
    allocator_database: Path = DEFAULT_ALLOCATOR_DATABASE,
    audit_path: Path = DEFAULT_AUDIT,
) -> dict[str, Any]:
    config = read_json(config_path)
    account_payload = read_json(account_path)
    connection = initialize_database(database_path)
    try:
        classification = classify_unclassified_snapshots(connection)
        snapshot = record_account_snapshot(connection, account_payload)
        ingestion = ingest_executions(connection, signal_database, audit_path, config)
        sentinel = routeability_sentinel(connection, account_payload, config)
        summary = accounting_summary(connection, account_payload, allocator_database)
        validity_counts = connection.execute(
            "SELECT COUNT(*),SUM(CASE WHEN is_valid=1 THEN 1 ELSE 0 END),"
            "SUM(CASE WHEN is_valid=0 THEN 1 ELSE 0 END) FROM account_snapshot_validity"
        ).fetchone()
        counts = {
            "snapshots": int(connection.execute("SELECT COUNT(*) FROM account_snapshots").fetchone()[0]),
            "classified_snapshots": int(validity_counts[0] or 0),
            "valid_snapshots": int(validity_counts[1] or 0),
            "quarantined_snapshots": int(validity_counts[2] or 0),
            "unavailable_snapshot_events": int(connection.execute(
                "SELECT COUNT(*) FROM accounting_integrity_events "
                "WHERE event_type='account_snapshot_unavailable'"
            ).fetchone()[0]),
            "execution_attributions": int(connection.execute("SELECT COUNT(*) FROM execution_attributions").fetchone()[0]),
            "integrity_events": int(connection.execute("SELECT COUNT(*) FROM accounting_integrity_events").fetchone()[0]),
            "routeability_sentinels": int(connection.execute("SELECT COUNT(*) FROM routeability_sentinels").fetchone()[0]),
        }
    finally:
        connection.close()
    payload = {
        "schema_version": 1, "generated_utc": utc_now(), "environment": "practice",
        "account_scope": "101-001-37981792-007", "research_only": True,
        "can_submit_orders": False, "manual_actions": False, "real_money_routing": False,
        "snapshot": snapshot, "ingestion": ingestion, "routeability_sentinel": sentinel,
        "snapshot_classification_backfill": classification,
        "accounting": summary, "counts": counts,
    }
    atomic_json(state_path, payload)
    return payload


__all__ = [
    "account_snapshot_availability", "accounting_summary",
    "classify_execution", "classify_unclassified_snapshots", "initialize_database",
    "record_account_snapshot", "routeability_sentinel", "run_accounting_cycle",
]


if __name__ == "__main__":
    print(json.dumps(run_accounting_cycle(), indent=2, sort_keys=True))
