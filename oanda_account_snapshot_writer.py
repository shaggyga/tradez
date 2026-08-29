#!/usr/bin/env python3
"""Write a lightweight OANDA account snapshot for the local dashboard."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_live_account_readonly_status import (
        DEFAULT_CREDS,
        PRACTICE_BASE_URL,
        account_config,
        cfg_value,
        list_practice_accounts,
        read_creds,
        summarize_account,
    )
except ImportError:
    from trad.oanda_live_account_readonly_status import (
        DEFAULT_CREDS,
        PRACTICE_BASE_URL,
        account_config,
        cfg_value,
        list_practice_accounts,
        read_creds,
        summarize_account,
    )

DEFAULT_STATE = Path(__file__).resolve().parent / "data" / "oanda_training_manager" / "state" / "account_dashboard_v1.json"

# These values are useful as stale operational context when OANDA's read-only
# account endpoint is temporarily unavailable.  Position/order collections and
# counts are deliberately absent: a prior flat (or open) account must never be
# presented as the current broker state after a failed read.
RETAINABLE_ACCOUNT_VALUE_FIELDS = ("NAV", "balance", "pl", "unrealizedPL")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _utc_age_seconds(source_time: str, observed_time: str) -> float | None:
    try:
        source = datetime.fromisoformat(str(source_time).replace("Z", "+00:00"))
        observed = datetime.fromisoformat(str(observed_time).replace("Z", "+00:00"))
        if source.tzinfo is None:
            source = source.replace(tzinfo=timezone.utc)
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, (observed - source).total_seconds()), 3)


def _account_identity(row: dict[str, Any]) -> str:
    return str(row.get("account_id") or row.get("role") or "")


def _last_verified_record(
    row: dict[str, Any],
    snapshot_time: str,
) -> dict[str, Any] | None:
    """Return safe financial context without carrying positions or orders."""
    if row.get("ok"):
        fields = {
            field: number
            for field in RETAINABLE_ACCOUNT_VALUE_FIELDS
            if (number := _finite_number(row.get(field))) is not None
        }
        if not fields:
            return None
        return {
            "source_time_utc": snapshot_time,
            "fields": fields,
            "positions_orders_retained": False,
        }
    retained = row.get("last_verified")
    if not isinstance(retained, dict):
        return None
    fields_raw = retained.get("fields")
    if not isinstance(fields_raw, dict):
        return None
    fields = {
        field: number
        for field in RETAINABLE_ACCOUNT_VALUE_FIELDS
        if (number := _finite_number(fields_raw.get(field))) is not None
    }
    source_time = str(retained.get("source_time_utc") or "")
    if not fields or not source_time:
        return None
    return {
        "source_time_utc": source_time,
        "fields": fields,
        "positions_orders_retained": False,
    }


def retain_failed_account_values(
    rows: list[dict[str, Any]],
    previous_snapshot: dict[str, Any] | None,
    observed_time: str,
) -> list[dict[str, Any]]:
    """Annotate failed reads with explicit, non-executable last-known values."""
    previous = previous_snapshot if isinstance(previous_snapshot, dict) else {}
    previous_time = str(previous.get("time") or "")
    prior_by_identity: dict[str, dict[str, Any]] = {}
    for prior in previous.get("accounts") or []:
        if not isinstance(prior, dict):
            continue
        identity = _account_identity(prior)
        record = _last_verified_record(prior, previous_time)
        if identity and record is not None:
            prior_by_identity[identity] = record

    output: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if row.get("ok"):
            row.update(
                {
                    "account_values_current": True,
                    "positions_current": True,
                    "orders_current": True,
                    "verified_at_utc": observed_time,
                }
            )
        else:
            # A failed /summary read conveys no current position or order state.
            row.update(
                {
                    "account_values_current": False,
                    "positions_current": False,
                    "orders_current": False,
                }
            )
            retained = prior_by_identity.get(_account_identity(row))
            if retained is not None:
                retained = dict(retained)
                retained["age_sec"] = _utc_age_seconds(
                    str(retained.get("source_time_utc") or ""), observed_time
                )
                row["last_verified"] = retained
        output.append(row)
    return output


def aggregate_account_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Build an aggregate whose failed reads can never look like zero equity."""
    ok_rows = [row for row in rows if row.get("ok")]
    all_current = bool(rows) and len(ok_rows) == len(rows)

    def complete_sum(field: str) -> float | None:
        values = [_finite_number(row.get(field)) for row in rows]
        if not all_current or any(value is None for value in values):
            return None
        return round(sum(value for value in values if value is not None), 5)

    def complete_count(field: str) -> int | None:
        value = complete_sum(field)
        return int(value) if value is not None else None

    errors = [
        {
            "account_id": row.get("account_id"),
            "role": row.get("role"),
            "status_code": row.get("status_code"),
            "error": row.get("error") or "account_read_failed",
        }
        for row in rows
        if not row.get("ok")
    ]

    last_verified_rows: list[dict[str, Any]] = []
    for row in rows:
        if row.get("ok"):
            continue
        retained = row.get("last_verified")
        if isinstance(retained, dict):
            last_verified_rows.append(
                {
                    "account_id": row.get("account_id"),
                    "role": row.get("role"),
                    **retained,
                }
            )

    retained_aggregate: dict[str, Any] | None = None
    if last_verified_rows and len(last_verified_rows) == len(rows):
        retained_fields: dict[str, float] = {}
        complete = True
        for field in RETAINABLE_ACCOUNT_VALUE_FIELDS:
            values = [
                _finite_number((item.get("fields") or {}).get(field))
                for item in last_verified_rows
            ]
            if any(value is None for value in values):
                complete = False
                break
            retained_fields[field] = round(
                sum(value for value in values if value is not None), 5
            )
        if complete:
            ages = [
                value
                for item in last_verified_rows
                if (value := _finite_number(item.get("age_sec"))) is not None
            ]
            retained_aggregate = {
                "state": "retained_stale",
                "fields": {
                    "nav": retained_fields["NAV"],
                    "balance": retained_fields["balance"],
                    "pl": retained_fields["pl"],
                    "unrealizedPL": retained_fields["unrealizedPL"],
                },
                "oldest_age_sec": max(ages) if ages else None,
                "accounts": last_verified_rows,
                "positions_orders_retained": False,
            }

    state = (
        "current"
        if all_current
        else "retained_stale_account_values"
        if retained_aggregate is not None
        else "unavailable"
    )
    return {
        "account_count": len(rows),
        "ok_count": len(ok_rows),
        "snapshot_state": state,
        "account_values_current": all_current,
        "positions_current": all_current,
        "orders_current": all_current,
        "nav": complete_sum("NAV"),
        "balance": complete_sum("balance"),
        "pl": complete_sum("pl"),
        "unrealizedPL": complete_sum("unrealizedPL"),
        "openTradeCount": complete_count("openTradeCount"),
        "pendingOrderCount": complete_count("pendingOrderCount"),
        "current_errors": errors,
        "last_verified": retained_aggregate,
    }


def practice_accounts_from_keys(creds_text: str, account_keys: set[str]) -> list[dict[str, str]]:
    api_key = cfg_value(creds_text, "OANDA_API_KEY", "OANDA_API_TOKEN")
    accounts: list[dict[str, str]] = []
    for account_key in sorted(account_keys):
        account_id = cfg_value(creds_text, account_key)
        suffix = account_id.rsplit("-", 1)[-1] if account_id else account_key
        accounts.append(
            {
                "role": f"practice_{suffix}",
                "env": "practice",
                "base_url": PRACTICE_BASE_URL,
                "api_key": api_key,
                "account_id": account_id,
            }
        )
    return accounts


def build_snapshot(
    creds: Path,
    roles: set[str],
    account_keys: set[str] | None = None,
    practice_accounts: list[dict[str, str]] | None = None,
    live_account_configs: list[dict[str, str]] | None = None,
    discovery_status: dict[str, Any] | None = None,
    previous_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    creds_text = read_creds(creds)
    selected_keys = account_keys or set()
    accounts = list(practice_accounts) if practice_accounts is not None else (
        practice_accounts_from_keys(creds_text, selected_keys)
        if selected_keys
        else list_practice_accounts(creds_text)
    )
    live_accounts = list(live_account_configs) if live_account_configs is not None else (
        [] if selected_keys else account_config(creds_text)
    )
    if roles:
        accounts = [account for account in accounts if account.get("role") in roles]
        live_accounts = [account for account in live_accounts if account.get("role") in roles]
    observed_time = utc_now()
    rows = [summarize_account(account, required_protection={"STOP_LOSS", "TAKE_PROFIT"}) for account in accounts]
    live_rows = [summarize_account(account, required_protection={"STOP_LOSS", "TAKE_PROFIT"}) for account in live_accounts]
    rows = retain_failed_account_values(rows, previous_snapshot, observed_time)
    aggregate = aggregate_account_rows(rows)
    payload = {
        "time": observed_time,
        "environment": "practice",
        "account_scope": (
            "managed_practice_only"
            if selected_keys
            else "discovered_all"
        ),
        "aggregate": aggregate,
        "accounts": rows,
        "live_accounts": live_rows,
    }
    if discovery_status is not None:
        payload["account_discovery"] = discovery_status
    return payload


def read_existing_snapshot(path: Path) -> dict[str, Any] | None:
    """Read restart context; malformed state is ignored and never guessed."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(
        f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    )
    try:
        temp.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        for attempt in range(8):
            try:
                os.replace(temp, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temp.unlink(missing_ok=True)


def write_snapshot_targets(
    output: Path,
    mirror_outputs: list[Path],
    payload: dict[str, Any],
) -> list[Path]:
    """Write one broker response to every compatibility snapshot path."""
    targets: list[Path] = []
    seen: set[Path] = set()
    for target in [Path(output), *[Path(path) for path in mirror_outputs]]:
        resolved = target.resolve()
        if resolved in seen:
            continue
        write_atomic(target, payload)
        targets.append(target)
        seen.add(resolved)
    return targets


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--output", type=Path, default=DEFAULT_STATE)
    parser.add_argument(
        "--mirror-output",
        type=Path,
        action="append",
        default=[],
        help="Atomically mirror the same fetched snapshot to a compatibility path.",
    )
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--discovery-interval-sec", type=float, default=300.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--role", action="append", default=[])
    parser.add_argument(
        "--account-key",
        action="append",
        default=[],
        help="Read only the named practice account id from the credentials file.",
    )
    args = parser.parse_args(argv)

    roles = {str(role).strip() for role in args.role if str(role).strip()}
    account_keys = {
        str(account_key).strip()
        for account_key in args.account_key
        if str(account_key).strip()
    }
    deadline = time.monotonic() + max(1.0, args.duration_sec)
    cached_practice_accounts: list[dict[str, str]] | None = None
    cached_live_accounts: list[dict[str, str]] | None = None
    next_discovery = 0.0
    last_discovery_success = ""
    last_discovery_error = ""
    previous_snapshot = read_existing_snapshot(args.output)
    while time.monotonic() < deadline:
        practice_override = None
        live_override = None
        discovery_status = None
        if not account_keys:
            now = time.monotonic()
            if now >= next_discovery:
                creds_text = read_creds(args.creds)
                discovered = list_practice_accounts(creds_text)
                discovery_error = next(
                    (
                        str(account.get("discovery_error") or "")
                        for account in discovered
                        if account.get("discovery_error")
                    ),
                    "",
                )
                valid = [
                    account
                    for account in discovered
                    if account.get("account_id") and not account.get("discovery_error")
                ]
                if valid:
                    cached_practice_accounts = discovered
                    cached_live_accounts = account_config(creds_text)
                    last_discovery_success = utc_now()
                    last_discovery_error = ""
                    next_discovery = now + max(30.0, args.discovery_interval_sec)
                else:
                    last_discovery_error = discovery_error or "no_practice_accounts_returned"
                    next_discovery = now + min(30.0, max(5.0, args.discovery_interval_sec))
                    if cached_practice_accounts is None:
                        cached_practice_accounts = discovered
                        cached_live_accounts = account_config(creds_text)
            practice_override = cached_practice_accounts
            live_override = cached_live_accounts
            discovery_status = {
                "cached_account_count": len(cached_practice_accounts or []),
                "last_success_utc": last_discovery_success,
                "last_error": last_discovery_error,
                "using_cached_ids": bool(last_discovery_error and last_discovery_success),
            }
        payload = build_snapshot(
            args.creds,
            roles,
            account_keys,
            practice_accounts=practice_override,
            live_account_configs=live_override,
            discovery_status=discovery_status,
            previous_snapshot=previous_snapshot,
        )
        write_snapshot_targets(args.output, args.mirror_output, payload)
        previous_snapshot = payload
        print(json.dumps({"event": "account_snapshot", "time": payload["time"], **payload["aggregate"]}), flush=True)
        minimum_interval = 1.0 if account_keys or roles else 5.0
        sleep_for = min(
            max(minimum_interval, args.interval_sec),
            max(0.0, deadline - time.monotonic()),
        )
        if sleep_for <= 0.0:
            break
        time.sleep(sleep_for)
    return 0


if __name__ == "__main__":
    sys.exit(main())
