#!/usr/bin/env python3
"""Flatten all known OANDA accounts from local registry/creds.

The script is intentionally execution-gated.  Without --execute it only reports
what it would close.  With --execute it:

1. Cancels standalone pending orders that are not attached to an open trade.
2. Closes every open trade with units=ALL.
3. Uses position closeout as a fallback for any remaining open positions.
4. Cancels remaining pending orders only once the account has no open trades.

No API keys are printed.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import requests
except Exception as exc:  # pragma: no cover - environment issue at runtime.
    requests = None  # type: ignore[assignment]
    REQUESTS_IMPORT_ERROR = exc
else:
    REQUESTS_IMPORT_ERROR = None


ROOT = Path(__file__).resolve().parent
CREDS_PATH = ROOT / "creds"
REGISTRY_PATH = ROOT / "config" / "accounts_registry.json"
RUNTIME_LOG_DIR = ROOT / "data" / "runtime_logs"

LIVE_BASE_URL = "https://api-fxtrade.oanda.com"
PRACTICE_BASE_URL = "https://api-fxpractice.oanda.com"

ACCOUNT_ID_RE = re.compile(r"^\d{3}-\d{3}-\d+-\d+$")

LIVE_TOKEN_ALIASES = (
    "OANDA_LIVE_API_KEY",
    "OANDA_API_KEY_LIVE",
    "OANDA_LIVE_API_TOKEN",
    "OANDA_API_TOKEN_LIVE",
)
PRACTICE_TOKEN_ALIASES = (
    "OANDA_API_KEY",
    "OANDA_API_TOKEN",
    "oandaapi",
)


def iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def read_creds(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = {}
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = ""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name):
            continue
        try:
            data[name] = ast.literal_eval(value)
        except Exception:
            data[name] = value.strip("\"'")
    for key, value in os.environ.items():
        if key.startswith("OANDA_") or key.startswith("FOREX_") or key == "oandaapi":
            data[key] = value
    return data


def first_setting(settings: dict[str, Any], names: tuple[str, ...]) -> str:
    for name in names:
        value = settings.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def env_for_account(account_id: str, role: dict[str, Any] | None = None) -> str:
    if account_id.startswith("001-"):
        return "live"
    if account_id.startswith("101-"):
        return "practice"
    execution = str((role or {}).get("execution") or "").lower()
    if "live" in execution:
        return "live"
    return "practice"


@dataclass
class AccountTarget:
    account_id: str
    env: str
    roles: set[str] = field(default_factory=set)
    aliases: set[str] = field(default_factory=set)

    @property
    def base_url(self) -> str:
        return LIVE_BASE_URL if self.env == "live" else PRACTICE_BASE_URL


def add_target(
    targets: dict[tuple[str, str], AccountTarget],
    *,
    account_id: str,
    env: str,
    role_name: str = "",
    aliases: list[str] | None = None,
) -> None:
    if not account_id or not ACCOUNT_ID_RE.match(account_id):
        return
    key = (env, account_id)
    target = targets.setdefault(key, AccountTarget(account_id=account_id, env=env))
    if role_name:
        target.roles.add(role_name)
    for alias in aliases or []:
        if alias:
            target.aliases.add(alias)


def collect_targets(creds: dict[str, Any], include_envs: set[str]) -> list[AccountTarget]:
    registry = read_json(REGISTRY_PATH, {})
    targets: dict[tuple[str, str], AccountTarget] = {}
    roles = registry.get("roles") if isinstance(registry, dict) else {}
    if isinstance(roles, dict):
        for role_name, role in roles.items():
            if not isinstance(role, dict):
                continue
            account_id = str(role.get("account_id") or "").strip()
            env = env_for_account(account_id, role)
            if env not in include_envs:
                continue
            aliases = [str(item) for item in role.get("account_aliases") or []]
            add_target(
                targets,
                account_id=account_id,
                env=env,
                role_name=str(role_name),
                aliases=aliases,
            )

    for key, value in creds.items():
        if not key.upper().startswith("OANDA_ACCOUNT"):
            continue
        account_id = str(value).strip()
        if not ACCOUNT_ID_RE.match(account_id):
            continue
        env = env_for_account(account_id)
        if env not in include_envs:
            continue
        add_target(targets, account_id=account_id, env=env, role_name=str(key), aliases=[str(key)])

    return sorted(targets.values(), key=lambda item: (item.env, item.account_id))


def token_for_target(creds: dict[str, Any], target: AccountTarget) -> str:
    aliases = LIVE_TOKEN_ALIASES if target.env == "live" else PRACTICE_TOKEN_ALIASES
    return first_setting(creds, aliases)


def build_session(token: str) -> requests.Session:
    if requests is None:
        raise RuntimeError(f"requests import failed: {REQUESTS_IMPORT_ERROR!r}")
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "RFC3339",
        }
    )
    return session


def api_request(
    session: requests.Session,
    method: str,
    target: AccountTarget,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> tuple[int, dict[str, Any]]:
    url = f"{target.base_url}/v3/accounts/{target.account_id}{path}"
    try:
        response = session.request(method, url, json=body, timeout=timeout)
    except Exception as exc:
        return 0, {"_error": True, "errorMessage": repr(exc)}
    try:
        payload = response.json() if response.text else {}
    except Exception:
        payload = {"raw": response.text[:500]}
    if not isinstance(payload, dict):
        payload = {"payload": payload}
    payload["_http_status"] = response.status_code
    if response.status_code >= 400:
        payload["_error"] = True
    return response.status_code, payload


def as_list(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    values = payload.get(key)
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


def numeric_units(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def position_is_open(position: dict[str, Any]) -> bool:
    long_units = numeric_units((position.get("long") or {}).get("units"))
    short_units = numeric_units((position.get("short") or {}).get("units"))
    return abs(long_units) > 0.0 or abs(short_units) > 0.0


def summarize_state(session: requests.Session, target: AccountTarget) -> dict[str, Any]:
    summary_code, summary_payload = api_request(session, "GET", target, "/summary")
    trades_code, trades_payload = api_request(session, "GET", target, "/openTrades")
    orders_code, orders_payload = api_request(session, "GET", target, "/pendingOrders")
    positions_code, positions_payload = api_request(session, "GET", target, "/openPositions")

    errors: list[dict[str, Any]] = []
    for label, code, payload in (
        ("summary", summary_code, summary_payload),
        ("openTrades", trades_code, trades_payload),
        ("pendingOrders", orders_code, orders_payload),
        ("openPositions", positions_code, positions_payload),
    ):
        if code != 200:
            errors.append(
                {
                    "endpoint": label,
                    "status": code,
                    "errorCode": payload.get("errorCode"),
                    "errorMessage": payload.get("errorMessage") or payload.get("raw"),
                }
            )

    account = summary_payload.get("account") if summary_code == 200 else {}
    if not isinstance(account, dict):
        account = {}
    trades = as_list(trades_payload, "trades") if trades_code == 200 else []
    orders = as_list(orders_payload, "orders") if orders_code == 200 else []
    positions = as_list(positions_payload, "positions") if positions_code == 200 else []
    open_positions = [position for position in positions if position_is_open(position)]
    standalone_orders = [order for order in orders if not str(order.get("tradeID") or "").strip()]

    return {
        "ok": not errors,
        "errors": errors,
        "NAV": account.get("NAV"),
        "balance": account.get("balance"),
        "marginUsed": account.get("marginUsed"),
        "marginAvailable": account.get("marginAvailable"),
        "marginCloseoutPercent": account.get("marginCloseoutPercent"),
        "unrealizedPL": account.get("unrealizedPL"),
        "pl": account.get("pl"),
        "lastTransactionID": account.get("lastTransactionID"),
        "open_trade_count": len(trades),
        "pending_order_count": len(orders),
        "standalone_pending_order_count": len(standalone_orders),
        "open_position_count": len(open_positions),
        "trades": [
            {
                "id": str(trade.get("id") or ""),
                "instrument": trade.get("instrument"),
                "units": trade.get("currentUnits"),
                "price": trade.get("price"),
                "unrealizedPL": trade.get("unrealizedPL"),
            }
            for trade in trades
        ],
        "pending_orders": [
            {
                "id": str(order.get("id") or ""),
                "type": order.get("type"),
                "instrument": order.get("instrument"),
                "units": order.get("units"),
                "tradeID": order.get("tradeID"),
            }
            for order in orders
        ],
        "open_positions": [
            {
                "instrument": position.get("instrument"),
                "long_units": (position.get("long") or {}).get("units"),
                "short_units": (position.get("short") or {}).get("units"),
                "unrealizedPL": position.get("unrealizedPL"),
            }
            for position in open_positions
        ],
    }


def action_result(action: str, item_id: str, code: int, payload: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "action": action,
        "id": item_id,
        "status": code,
        "ok": 200 <= code < 300,
        "lastTransactionID": payload.get("lastTransactionID"),
        "relatedTransactionIDs": payload.get("relatedTransactionIDs"),
    }
    if not result["ok"]:
        result["errorCode"] = payload.get("errorCode")
        result["errorMessage"] = payload.get("errorMessage") or payload.get("raw")
    return result


def cancel_order(session: requests.Session, target: AccountTarget, order_id: str) -> dict[str, Any]:
    code, payload = api_request(session, "PUT", target, f"/orders/{order_id}/cancel")
    return action_result("cancel_order", order_id, code, payload)


def close_trade(session: requests.Session, target: AccountTarget, trade_id: str) -> dict[str, Any]:
    code, payload = api_request(session, "PUT", target, f"/trades/{trade_id}/close", body={"units": "ALL"})
    return action_result("close_trade", trade_id, code, payload)


def close_position(session: requests.Session, target: AccountTarget, position: dict[str, Any]) -> dict[str, Any] | None:
    instrument = str(position.get("instrument") or "").strip()
    if not instrument:
        return None
    long_units = numeric_units((position.get("long") or {}).get("units"))
    short_units = numeric_units((position.get("short") or {}).get("units"))
    if abs(long_units) <= 0.0 and abs(short_units) <= 0.0:
        return None
    body = {
        "longUnits": "ALL" if abs(long_units) > 0.0 else "NONE",
        "shortUnits": "ALL" if abs(short_units) > 0.0 else "NONE",
    }
    code, payload = api_request(session, "PUT", target, f"/positions/{instrument}/close", body=body)
    return action_result("close_position", instrument, code, payload)


def flatten_account(
    session: requests.Session,
    target: AccountTarget,
    before: dict[str, Any],
    *,
    execute: bool,
    settle_seconds: float,
) -> dict[str, Any]:
    actions: dict[str, Any] = {
        "execute": execute,
        "standalone_orders_canceled_first": [],
        "trades_closed": [],
        "positions_closed_fallback": [],
        "orders_canceled_after_flat": [],
    }
    if not execute:
        return actions

    for order in before.get("pending_orders") or []:
        if str(order.get("tradeID") or "").strip():
            continue
        order_id = str(order.get("id") or "").strip()
        if order_id:
            actions["standalone_orders_canceled_first"].append(cancel_order(session, target, order_id))

    for trade in before.get("trades") or []:
        trade_id = str(trade.get("id") or "").strip()
        if trade_id:
            actions["trades_closed"].append(close_trade(session, target, trade_id))

    if settle_seconds > 0:
        time.sleep(settle_seconds)

    mid_state = summarize_state(session, target)
    if mid_state.get("open_position_count", 0) > 0:
        _, positions_payload = api_request(session, "GET", target, "/openPositions")
        for position in as_list(positions_payload, "positions"):
            result = close_position(session, target, position)
            if result is not None:
                actions["positions_closed_fallback"].append(result)

    if settle_seconds > 0:
        time.sleep(settle_seconds)

    final_pending_state = summarize_state(session, target)
    remaining_trades = int(final_pending_state.get("open_trade_count") or 0)
    for order in final_pending_state.get("pending_orders") or []:
        order_id = str(order.get("id") or "").strip()
        trade_id = str(order.get("tradeID") or "").strip()
        if not order_id:
            continue
        if remaining_trades > 0 and trade_id:
            continue
        actions["orders_canceled_after_flat"].append(cancel_order(session, target, order_id))

    return actions


def compact_target(target: AccountTarget) -> dict[str, Any]:
    return {
        "env": target.env,
        "account_id": target.account_id,
        "roles": sorted(target.roles),
        "aliases": sorted(target.aliases),
    }


def totals(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    before_trades = sum(int((row.get("before") or {}).get("open_trade_count") or 0) for row in accounts)
    after_trades = sum(int((row.get("after") or {}).get("open_trade_count") or 0) for row in accounts)
    before_positions = sum(int((row.get("before") or {}).get("open_position_count") or 0) for row in accounts)
    after_positions = sum(int((row.get("after") or {}).get("open_position_count") or 0) for row in accounts)
    before_orders = sum(int((row.get("before") or {}).get("pending_order_count") or 0) for row in accounts)
    after_orders = sum(int((row.get("after") or {}).get("pending_order_count") or 0) for row in accounts)
    failed_accounts = [
        row.get("account_id")
        for row in accounts
        if not (row.get("before") or {}).get("ok") or not (row.get("after") or {}).get("ok")
    ]
    action_errors: list[dict[str, Any]] = []
    for row in accounts:
        actions = row.get("actions") or {}
        for key in (
            "standalone_orders_canceled_first",
            "trades_closed",
            "positions_closed_fallback",
            "orders_canceled_after_flat",
        ):
            for action in actions.get(key) or []:
                if not action.get("ok"):
                    action_errors.append({"account_id": row.get("account_id"), **action})
    return {
        "accounts": len(accounts),
        "before_open_trades": before_trades,
        "after_open_trades": after_trades,
        "before_open_positions": before_positions,
        "after_open_positions": after_positions,
        "before_pending_orders": before_orders,
        "after_pending_orders": after_orders,
        "failed_accounts": failed_accounts,
        "action_error_count": len(action_errors),
        "action_errors": action_errors,
    }


def render_table(result: dict[str, Any]) -> str:
    rows = result.get("accounts") or []
    headers = ["env", "account", "roles", "trades", "positions", "pending", "status"]
    rendered: list[list[str]] = []
    for row in rows:
        before = row.get("before") or {}
        after = row.get("after") or {}
        status = "ok"
        if not before.get("ok"):
            status = "before_error"
        elif not after.get("ok"):
            status = "after_error"
        elif int(after.get("open_trade_count") or 0) or int(after.get("open_position_count") or 0):
            status = "not_flat"
        elif int((row.get("actions") or {}).get("execute") is True) and int(after.get("pending_order_count") or 0):
            status = "flat_pending_left"
        rendered.append(
            [
                str(row.get("env") or ""),
                str(row.get("account_id") or ""),
                ",".join(row.get("roles") or [])[:42],
                f"{before.get('open_trade_count', '?')} -> {after.get('open_trade_count', '?')}",
                f"{before.get('open_position_count', '?')} -> {after.get('open_position_count', '?')}",
                f"{before.get('pending_order_count', '?')} -> {after.get('pending_order_count', '?')}",
                status,
            ]
        )
    widths = [max(len(headers[i]), *(len(row[i]) for row in rendered)) for i in range(len(headers))]
    lines = ["  ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))]
    lines.append("  ".join("-" * widths[i] for i in range(len(headers))))
    for row in rendered:
        lines.append("  ".join(row[i].ljust(widths[i]) for i in range(len(headers))))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=CREDS_PATH)
    parser.add_argument("--execute", action="store_true", help="Actually flatten accounts.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table.")
    parser.add_argument("--live-only", action="store_true")
    parser.add_argument("--practice-only", action="store_true")
    parser.add_argument("--settle-seconds", type=float, default=0.75)
    parser.add_argument("--output", type=Path, help="Write full JSON report to this path.")
    args = parser.parse_args(argv)

    if args.live_only and args.practice_only:
        raise SystemExit("--live-only and --practice-only are mutually exclusive")
    include_envs = {"live", "practice"}
    if args.live_only:
        include_envs = {"live"}
    elif args.practice_only:
        include_envs = {"practice"}

    creds = read_creds(args.creds)
    targets = collect_targets(creds, include_envs)
    result: dict[str, Any] = {
        "generated_utc": iso_utc(),
        "execute": bool(args.execute),
        "accounts": [],
    }

    for target in targets:
        row = compact_target(target)
        token = token_for_target(creds, target)
        row["token_present"] = bool(token)
        if not token:
            row["before"] = {"ok": False, "errors": [{"endpoint": "credentials", "errorMessage": "missing_api_token"}]}
            row["actions"] = {"execute": bool(args.execute)}
            row["after"] = row["before"]
            result["accounts"].append(row)
            continue

        session = build_session(token)
        before = summarize_state(session, target)
        row["before"] = before
        row["actions"] = flatten_account(
            session,
            target,
            before,
            execute=bool(args.execute),
            settle_seconds=max(0.0, float(args.settle_seconds)),
        )
        row["after"] = summarize_state(session, target)
        result["accounts"].append(row)

    result["totals"] = totals(result["accounts"])
    output_path = args.output
    if output_path is None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        suffix = "execute" if args.execute else "status"
        output_path = RUNTIME_LOG_DIR / f"oanda_flatten_all_accounts_{suffix}_{stamp}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    result["report_path"] = str(output_path)

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(render_table(result))
        print(f"report_path={output_path}")

    totals_row = result.get("totals") or {}
    flat = (
        int(totals_row.get("after_open_trades") or 0) == 0
        and int(totals_row.get("after_open_positions") or 0) == 0
        and int(totals_row.get("action_error_count") or 0) == 0
        and not totals_row.get("failed_accounts")
    )
    return 0 if flat else 2


if __name__ == "__main__":
    raise SystemExit(main())
