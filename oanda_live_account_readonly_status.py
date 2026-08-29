#!/usr/bin/env python3
"""Read-only OANDA account status helper for the live forex stack.

The script only calls OANDA GET endpoints:

- /summary
- /openTrades
- /pendingOrders
- /pricing for instruments with open trades

It never prints API keys/tokens.  It exists to keep recurring live-week checks
consistent and to avoid ad hoc credential parsing in shell snippets.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parent
DEFAULT_CREDS = ROOT / "creds"

LIVE_BASE_URL = "https://api-fxtrade.oanda.com"
PRACTICE_BASE_URL = "https://api-fxpractice.oanda.com"


def read_creds(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def cfg_value(text: str, *names: str) -> str:
    for name in names:
        env_value = os.environ.get(name)
        if env_value:
            return env_value.strip()
        pattern = r'''(?m)^\s*{}\s*=\s*["']([^"']+)["']'''.format(re.escape(name))
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    return ""


def account_config(creds_text: str, include_practice_canary: bool = False) -> list[dict[str, str]]:
    accounts = [
        {
            "role": "gpt_live",
            "env": "live",
            "base_url": LIVE_BASE_URL,
            "api_key": cfg_value(creds_text, "OANDA_LIVE_API_KEY", "OANDA_API_KEY_LIVE", "OANDA_API_TOKEN_LIVE"),
            "account_id": cfg_value(creds_text, "OANDA_ACCOUNT_LIVE_MAIN", "OANDA_ACCOUNT_ID_LIVE_MAIN"),
        },
        {
            "role": "tech_live",
            "env": "live",
            "base_url": LIVE_BASE_URL,
            "api_key": cfg_value(creds_text, "OANDA_LIVE_API_KEY", "OANDA_API_KEY_LIVE", "OANDA_API_TOKEN_LIVE"),
            "account_id": cfg_value(creds_text, "OANDA_ACCOUNT_LIVE_TECH", "OANDA_ACCOUNT_ID_TECH_LIVE"),
        },
        {
            "role": "primary_live",
            "env": "live",
            "base_url": LIVE_BASE_URL,
            "api_key": cfg_value(creds_text, "OANDA_LIVE_API_KEY", "OANDA_API_KEY_LIVE", "OANDA_API_TOKEN_LIVE"),
            "account_id": cfg_value(creds_text, "OANDA_ACCOUNT_LIVE_PRIMARY", "OANDA_ACCOUNT_ID_PRIMARY_LIVE"),
        },
    ]
    if include_practice_canary:
        accounts.append(
            {
                "role": "canary_practice",
                "env": "practice",
                "base_url": PRACTICE_BASE_URL,
                "api_key": cfg_value(creds_text, "OANDA_API_KEY", "OANDA_API_TOKEN"),
                "account_id": cfg_value(creds_text, "OANDA_ACCOUNT_ID_DUM1"),
            }
        )
    return accounts


def get_json(
    session: requests.Session,
    base_url: str,
    account_id: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    response = session.get(
        f"{base_url}/v3/accounts/{account_id}{path}",
        params=params,
        timeout=20,
    )
    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text[:300]}
    return response.status_code, payload if isinstance(payload, dict) else {"payload": payload}


def executable_closeout_price(price: dict[str, Any], units: float) -> tuple[float, str]:
    """Return the top executable level for a trade, not OANDA's deepest tier.

    OANDA's pricing payload exposes ``closeoutBid``/``closeoutAsk`` at the
    outside of the full liquidity ladder.  Those values are useful for a
    maximum-size account closeout, but materially misstate the current price
    of the small practice-007 trades.  The first bid/ask level is the relevant
    executable price; retain the closeout field as a compatibility fallback.
    """

    if units > 0.0:
        levels = price.get("bids") or []
        fallback_name = "closeoutBid"
    elif units < 0.0:
        levels = price.get("asks") or []
        fallback_name = "closeoutAsk"
    else:
        return 0.0, "no_position"
    for level in levels if isinstance(levels, list) else []:
        if not isinstance(level, dict):
            continue
        try:
            value = float(level.get("price") or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0.0:
            return value, "top_of_book"
    try:
        fallback = float(price.get(fallback_name) or 0.0)
    except (TypeError, ValueError):
        fallback = 0.0
    return fallback, "deep_closeout_fallback" if fallback > 0.0 else "missing"


def list_practice_accounts(creds_text: str) -> list[dict[str, str]]:
    api_key = cfg_value(creds_text, "OANDA_API_KEY", "OANDA_API_TOKEN")
    if not api_key:
        return [
            {
                "role": "practice_discovery",
                "env": "practice",
                "base_url": PRACTICE_BASE_URL,
                "api_key": "",
                "account_id": "",
            }
        ]
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
    response = session.get(f"{PRACTICE_BASE_URL}/v3/accounts", timeout=20)
    try:
        payload = response.json()
    except Exception:
        payload = {"raw": response.text[:300]}
    if response.status_code != 200:
        return [
            {
                "role": "practice_discovery",
                "env": "practice",
                "base_url": PRACTICE_BASE_URL,
                "api_key": api_key,
                "account_id": "",
                "discovery_error": payload.get("errorMessage") or payload.get("raw") or f"HTTP {response.status_code}",
            }
        ]
    accounts = payload.get("accounts") if isinstance(payload, dict) else []
    out: list[dict[str, str]] = []
    for account in accounts if isinstance(accounts, list) else []:
        if not isinstance(account, dict):
            continue
        account_id = str(account.get("id") or "")
        if not account_id:
            continue
        suffix = account_id.rsplit("-", 1)[-1]
        out.append(
            {
                "role": f"practice_{suffix}",
                "env": "practice",
                "base_url": PRACTICE_BASE_URL,
                "api_key": api_key,
                "account_id": account_id,
            }
        )
    return sorted(out, key=lambda item: item.get("role", ""))


def summarize_account(account: dict[str, str], required_protection: set[str] | None = None) -> dict[str, Any]:
    role = account["role"]
    account_id = account.get("account_id") or ""
    api_key = account.get("api_key") or ""
    if account.get("discovery_error"):
        return {
            "role": role,
            "env": account.get("env", ""),
            "account_id": account_id,
            "ok": False,
            "error": account.get("discovery_error"),
        }
    if not account_id or not api_key:
        return {
            "role": role,
            "env": account.get("env", ""),
            "account_id": account_id,
            "ok": False,
            "error": "missing_account_or_api_key",
        }

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})
    summary_code, summary_payload = get_json(session, account["base_url"], account_id, "/summary")
    if summary_code != 200:
        return {
            "role": role,
            "env": account.get("env", ""),
            "account_id": account_id,
            "ok": False,
            "status_code": summary_code,
            "error": summary_payload.get("errorMessage") or summary_payload.get("raw") or "summary_request_failed",
        }

    trades_code, trades_payload = get_json(session, account["base_url"], account_id, "/openTrades")
    orders_code, orders_payload = get_json(session, account["base_url"], account_id, "/pendingOrders")

    summary = summary_payload.get("account") or {}
    open_trades = trades_payload.get("trades") if trades_code == 200 else []
    pending_orders = orders_payload.get("orders") if orders_code == 200 else []
    if not isinstance(open_trades, list):
        open_trades = []
    if not isinstance(pending_orders, list):
        pending_orders = []
    instruments = sorted(
        {
            str(trade.get("instrument") or "")
            for trade in open_trades
            if isinstance(trade, dict) and trade.get("instrument")
        }
    )
    pricing_by_instrument: dict[str, dict[str, Any]] = {}
    if instruments:
        pricing_code, pricing_payload = get_json(
            session,
            account["base_url"],
            account_id,
            "/pricing",
            params={
                "instruments": ",".join(instruments),
                "includeUnitsAvailable": "false",
            },
        )
        if pricing_code == 200:
            pricing_by_instrument = {
                str(price.get("instrument") or ""): price
                for price in pricing_payload.get("prices") or []
                if isinstance(price, dict) and price.get("instrument")
            }

    protection_by_trade: dict[str, set[str]] = {}
    protection_details_by_trade: dict[str, list[dict[str, Any]]] = {}
    for order in pending_orders:
        if not isinstance(order, dict):
            continue
        trade_id = str(order.get("tradeID") or "")
        order_type = str(order.get("type") or "")
        if trade_id and order_type:
            protection_by_trade.setdefault(trade_id, set()).add(order_type)
            protection_details_by_trade.setdefault(trade_id, []).append(
                {
                    "id": order.get("id"),
                    "type": order_type,
                    "price": order.get("price"),
                    "distance": order.get("distance"),
                    "timeInForce": order.get("timeInForce"),
                    "state": order.get("state"),
                    "triggerCondition": order.get("triggerCondition"),
                }
            )

    trades: list[dict[str, Any]] = []
    unprotected_trade_ids: list[str] = []
    required_protection = set(required_protection or {"TAKE_PROFIT", "STOP_LOSS", "TRAILING_STOP_LOSS"})
    for trade in open_trades:
        if not isinstance(trade, dict):
            continue
        trade_id = str(trade.get("id") or "")
        protection = sorted(protection_by_trade.get(trade_id, set()))
        client_extensions = trade.get("clientExtensions") or {}
        client_tag = str(client_extensions.get("tag") or "")
        client_comment = str(client_extensions.get("comment") or "")
        staged_open_ended = bool(
            client_tag == "strategy_lab_top"
            and re.search(r"(?:^|\|)ta\d", client_comment)
            and re.search(r"(?:^|\|)td\d", client_comment)
        )
        effective_required = {"STOP_LOSS"} if staged_open_ended else required_protection
        missing = sorted(effective_required.difference(protection))
        if trade_id and missing:
            unprotected_trade_ids.append(trade_id)
        instrument = str(trade.get("instrument") or "")
        units = float(trade.get("currentUnits") or 0.0)
        entry_price = float(trade.get("price") or 0.0)
        price = pricing_by_instrument.get(instrument) or {}
        closeout_price, closeout_price_source = executable_closeout_price(
            price,
            units,
        )
        pip_size = (
            0.01
            if instrument.endswith("_JPY")
            or instrument in {"EUR_HUF", "USD_HUF", "USD_THB"}
            else 0.0001
        )
        unrealized_pips = (
            (closeout_price - entry_price) / pip_size
            if units > 0.0 and closeout_price > 0.0
            else (entry_price - closeout_price) / pip_size
            if units < 0.0 and closeout_price > 0.0
            else None
        )
        trades.append(
            {
                "id": trade_id,
                "instrument": instrument,
                "units": trade.get("currentUnits"),
                "price": trade.get("price"),
                "closeoutPrice": closeout_price or None,
                "closeoutPriceSource": closeout_price_source,
                "pricingTime": price.get("time"),
                "unrealizedPips": (
                    round(unrealized_pips, 3)
                    if unrealized_pips is not None
                    else None
                ),
                "unrealizedPL": trade.get("unrealizedPL"),
                "openTime": trade.get("openTime"),
                "exitPolicy": "open_ended_staged_trailing" if staged_open_ended else "fixed_protection",
                "trailingStatus": (
                    "active"
                    if staged_open_ended and "TRAILING_STOP_LOSS" in protection
                    else "pending_activation"
                    if staged_open_ended
                    else None
                ),
                "protection": protection,
                "protectionDetails": sorted(
                    protection_details_by_trade.get(trade_id, []),
                    key=lambda item: str(item.get("type") or ""),
                ),
                "missingProtection": missing,
            }
        )

    return {
        "role": role,
        "env": account.get("env", ""),
        "account_id": account_id,
        "ok": True,
        "NAV": summary.get("NAV"),
        "balance": summary.get("balance"),
        "marginUsed": summary.get("marginUsed"),
        "marginAvailable": summary.get("marginAvailable"),
        "marginCloseoutPercent": summary.get("marginCloseoutPercent"),
        "unrealizedPL": summary.get("unrealizedPL"),
        "pl": summary.get("pl"),
        "openTradeCount": summary.get("openTradeCount"),
        "pendingOrderCount": len(pending_orders),
        "unprotectedTradeIds": unprotected_trade_ids,
        "trades": trades,
        "lastTransactionID": summary.get("lastTransactionID"),
    }


def table(rows: list[dict[str, Any]]) -> str:
    headers = ["role", "env", "NAV", "margin%", "open", "pending", "uPL", "protected", "trades"]
    rendered_rows = []
    for row in rows:
        if not row.get("ok"):
            rendered_rows.append(
                [
                    str(row.get("role", "")),
                    str(row.get("env", "")),
                    "ERR",
                    "",
                    "",
                    "",
                    "",
                    "no",
                    str(row.get("error", ""))[:80],
                ]
            )
            continue
        trades = row.get("trades") if isinstance(row.get("trades"), list) else []
        trade_text = "; ".join(
            f"{trade.get('instrument')} {trade.get('units')} uPL={trade.get('unrealizedPL')}"
            for trade in trades[:4]
            if isinstance(trade, dict)
        )
        protected = "yes" if not row.get("unprotectedTradeIds") else f"missing:{','.join(row.get('unprotectedTradeIds') or [])}"
        rendered_rows.append(
            [
                str(row.get("role", "")),
                str(row.get("env", "")),
                str(row.get("NAV", "")),
                str(row.get("marginCloseoutPercent", "")),
                str(row.get("openTradeCount", "")),
                str(row.get("pendingOrderCount", "")),
                str(row.get("unrealizedPL", "")),
                protected,
                trade_text,
            ]
        )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rendered_rows))
        for index in range(len(headers))
    ]
    lines = ["  ".join(headers[index].ljust(widths[index]) for index in range(len(headers)))]
    lines.append("  ".join("-" * widths[index] for index in range(len(headers))))
    for row in rendered_rows:
        lines.append("  ".join(row[index].ljust(widths[index]) for index in range(len(headers))))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--include-practice-canary", action="store_true")
    parser.add_argument("--all-practice", action="store_true", help="Discover and report every visible OANDA practice account.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a compact table.")
    parser.add_argument(
        "--role",
        action="append",
        help="Only report the selected account role. May be passed more than once.",
    )
    parser.add_argument(
        "--required-protection",
        choices=("all", "hard"),
        default="all",
        help="Protection set used for unprotectedTradeIds. 'hard' requires only STOP_LOSS and TAKE_PROFIT.",
    )
    args = parser.parse_args(argv)

    creds_text = read_creds(args.creds)
    accounts = list_practice_accounts(creds_text) if args.all_practice else account_config(creds_text, args.include_practice_canary)
    if args.role:
        wanted_roles = {role.strip() for role in args.role if role.strip()}
        accounts = [account for account in accounts if account.get("role") in wanted_roles]
    required_protection = (
        {"STOP_LOSS", "TAKE_PROFIT"}
        if args.required_protection == "hard"
        else {"TAKE_PROFIT", "STOP_LOSS", "TRAILING_STOP_LOSS"}
    )
    rows = [summarize_account(account, required_protection=required_protection) for account in accounts]
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        print(table(rows))
    return 1 if any(not row.get("ok") for row in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
