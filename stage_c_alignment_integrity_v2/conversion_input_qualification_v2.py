"""Qualify USD conversion route availability from an integrity-verified report.

The USD account is an explicit research assumption. This inspects retained
candle prices and side-rule expressions; it neither turns them into executable
Decimal quotes nor computes accounting results. No raw archive is read.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from math import isfinite
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from publication import sha256_file

PREDECESSOR = ROOT.parent / "trad" / "oanda_curve_management_replay_v1.py"
# Source inspected for quote_at, usd_rates and convert_pnl_to_usd. A different
# implementation must be inspected before changing this binding or its rules.
INSPECTED_PREDECESSOR_SHA256 = "52fd04c6b087df4be775e490002afd0744caeb245604b8f577ab0fcff41de8f3"


def encoded(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def load_verified_report(path: Path, expected_sha256: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise ValueError("externally_supplied_quote_report_sha256_required")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("quote_report_sha256_mismatch")
    report = json.loads(raw)
    if report.get("schema_version") != "forex_retained_quote_input_qualification.v2":
        raise ValueError("unsupported_quote_qualification_schema")
    rows = report.get("instruments")
    if (not isinstance(rows, list) or len(rows) != 68
            or len({row.get("instrument") for row in rows}) != 68
            or any(not isinstance(row.get("instrument"), str) or not re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", row["instrument"]) for row in rows)):
        raise ValueError("unique_all68_currency_pair_universe_required")
    integrity = report.get("source_integrity", {})
    if (integrity.get("member_sha256_and_size_verified_count") != 68
            or integrity.get("exact_archive_member_inventory_verified") is not True):
        raise ValueError("quote_report_lacks_all68_source_integrity")
    return report


def conversion_route(currency: str, point_name: str, expected_epoch: int,
                     instruments: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Mirror the inspected predecessor's route/side selection, not quote_at.

    Missing execution metadata deliberately remains unqualified. Rates are
    formulas applied to recorded candle prices, not fabricated Decimal quotes.
    """
    if currency == "USD":
        return {"currency": currency, "status": "usd_identity", "rate_available": True,
                "conversion_instrument": None, "direction": "identity",
                "positive_or_zero_amount_rate": {"expression": "1", "source_side": None},
                "negative_amount_rate": {"expression": "1", "source_side": None}}
    if not re.fullmatch(r"[A-Z]{3}", currency):
        raise ValueError("invalid_conversion_currency")
    direct, inverse = currency + "_USD", "USD_" + currency
    if direct in instruments:
        pair, direction, positive, negative = direct, "direct", "bid_close", "ask_close"
    elif inverse in instruments:
        pair, direction, positive, negative = inverse, "inverse", "1 / ask_close", "1 / bid_close"
    else:
        return {"currency": currency, "status": "missing_direct_or_inverse_usd_instrument", "rate_available": False,
                "conversion_instrument": None, "direction": None, "checked_instruments": [direct, inverse]}
    result: dict[str, Any] = {"currency": currency, "conversion_instrument": pair, "direction": direction,
                              "rate_available": False,
                              "direct_preferred_without_invalid_direct_fallback": True}
    point = instruments[pair].get(point_name, {})
    if point.get("status") != "valid_candle_close_pair":
        return {**result, "status": "conversion_candle_pair_unavailable", "source_status": point.get("status", "missing_point")}
    if point.get("raw_bar_start_epoch") != expected_epoch:
        return {**result, "status": "conversion_clock_mismatch"}
    try:
        bid, ask = float(point["bid_close"]), float(point["ask_close"])
    except (KeyError, TypeError, ValueError, OverflowError):
        return {**result, "status": "invalid_conversion_candle_prices"}
    if not (isfinite(bid) and isfinite(ask) and 0 < bid <= ask):
        return {**result, "status": "invalid_conversion_candle_prices"}
    return {**result, "status": "retained_candle_rate_available", "rate_available": True,
            "source_raw_bar_start_epoch": expected_epoch, "source_bid_close": bid, "source_ask_close": ask,
            "positive_or_zero_amount_rate": {"expression": positive, "source_side": "bid_close" if direction == "direct" else "ask_close"},
            "negative_amount_rate": {"expression": negative, "source_side": "ask_close" if direction == "direct" else "bid_close"}}


def qualify(report_path: Path, expected_sha256: str, predecessor_path: Path = PREDECESSOR) -> dict[str, Any]:
    report = load_verified_report(report_path, expected_sha256)
    predecessor_hash = sha256_file(predecessor_path)
    if predecessor_hash != INSPECTED_PREDECESSOR_SHA256:
        raise ValueError("accounting_predecessor_changed_requires_rule_reinspection")
    instruments = {row["instrument"]: row for row in report["instruments"]}
    clocks = {"origin": report["clock_contract"]["origin_epoch"], "target": report["clock_contract"]["target_epoch"]}
    currencies = sorted({name.split("_")[1] for name in instruments})
    by_currency = {currency: {point: conversion_route(currency, point, epoch, instruments) for point, epoch in clocks.items()}
                   for currency in currencies}
    rows = []
    for name in sorted(instruments):
        currency = name.split("_")[1]
        row: dict[str, Any] = {"instrument": name, "quote_currency": currency}
        for point in clocks:
            conversion = by_currency[currency][point]
            own_candle = instruments[name].get(point, {}).get("status") == "valid_candle_close_pair"
            row[point] = {"instrument_candle_close_available": own_candle, "quote_currency_conversion": conversion,
                          "candle_and_conversion_available": own_candle and conversion["rate_available"]}
        rows.append(row)
    counts: dict[str, Any] = {"instrument_count": len(rows), "distinct_quote_currency_count": len(currencies)}
    for point in clocks:
        counts[point] = {
            "quote_currency_conversion_available_instruments": sum(row[point]["quote_currency_conversion"]["rate_available"] for row in rows),
            "candle_and_conversion_available_instruments": sum(row[point]["candle_and_conversion_available"] for row in rows),
            "usd_identity_instruments": sum(row[point]["quote_currency_conversion"]["direction"] == "identity" for row in rows),
            "unavailable_instruments": [row["instrument"] for row in rows if not row[point]["quote_currency_conversion"]["rate_available"]],
        }
    return {
        "schema_version": "forex_retained_usd_conversion_qualification.v2",
        "status": "offline_research_candle_conversion_route_availability_only",
        "research_account_currency": "USD", "account_scope": "explicit_research_assumption_no_live_account_metadata",
        "input_tier": report["input_tier"], "source_quote_report_sha256": expected_sha256,
        "source_report_hash_validation": "recomputed_and_matched_external_expected_sha256",
        "archive_reread": False,
        "implementation_sha256": sha256_file(Path(__file__)),
        "accounting_rule_provenance": {"source": "oanda_curve_management_replay_v1.py",
            "source_sha256": predecessor_hash, "inspected_functions": ["quote_at", "usd_rates", "convert_pnl_to_usd"],
            "route_policy": "direct_preferred_else_inverse_no_triangulation_no_invalid_direct_fallback",
            "nonnegative_amount_rule": "sell_profit", "negative_amount_rule": "buy_loss"},
        "clock_contract": report["clock_contract"], "counts": counts,
        "by_quote_currency": by_currency, "instruments": rows,
        "execution_ready": False,
        "limitations": [
            "Rate availability refers only to co-stamped retained candle closes at the two explicit historical stamps.",
            "Source bid/ask prices are binary floating-point archive values; they are not exact Decimal quote endpoints.",
            "The predecessor additionally requires quote identity, explicit tradeability, market/arrival clocks and freshness; those are not supplied by this report.",
            "First-known quote availability and simultaneous executability remain unverified.",
            "Rate formulas are documented; no conversion PnL, size, slippage, financing or ledger is calculated.",
            "Quote-currency conversion coverage does not qualify base-currency sizing or full accounting inputs.",
            "USD identity is explicit; missing conversion routes never receive a rate-one fallback.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quote-report", type=Path, required=True)
    parser.add_argument("--quote-report-sha256", required=True)
    parser.add_argument("--accounting-source", type=Path, default=PREDECESSOR)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("conversion qualification output must use a new evidence path")
    report = qualify(args.quote_report, args.quote_report_sha256, args.accounting_source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as handle:
        handle.write(encoded(report))
    print(json.dumps({"output": str(args.output), "counts": report["counts"], "execution_ready": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
