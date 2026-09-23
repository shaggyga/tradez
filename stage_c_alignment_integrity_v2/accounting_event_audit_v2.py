"""Independent scalar arithmetic audit of accepted synthetic accounting events.

This is an accounting oracle, not an independently implemented policy/admission
engine. It recomputes economics from raw event inputs, never copied PnL receipts.
The optimized large-run engine gate remains separate.
"""
from copy import deepcopy
from decimal import Decimal, Context, ROUND_HALF_EVEN, localcontext

CTX = Context(prec=50, rounding=ROUND_HALF_EVEN)
TOLERANCE = Decimal("1e-40")


def D(value):
    return Decimal(str(value))


def rate(currency, amount, quotes):
    if currency == "USD":
        return Decimal(1)
    direct, inverse = currency + "_USD", "USD_" + currency
    if direct in quotes:
        return D(quotes[direct]["bid" if amount >= 0 else "ask"])
    q = quotes[inverse]
    return Decimal(1) / D(q["ask" if amount >= 0 else "bid"])


def price(pair, side, opening, quotes, slippage):
    q = quotes[pair]
    raw = D(q["ask"] if (side > 0) == opening else q["bid"])
    slip = (D(q["ask"]) + D(q["bid"])) / 2 * slippage / 10000
    return raw + (side if opening else -side) * slip


def audit_accounting(contract, events, rows):
    if len(events) != len(rows):
        raise ValueError("one_ledger_row_per_input_event_required")
    capital, margin = D(contract["initial_capital_usd"]), D(contract["margin_rate"])
    slip = D(contract["reference_config"]["slippage_bps_per_leg"])
    arms = {name: {"cash": capital, "realized": Decimal(0), "financing": Decimal(0), "fees": Decimal(0), "lots": {}, "orders": {}}
            for name in contract["arms"]}
    checked = 0
    max_residual = Decimal(0)
    with localcontext(CTX):
        for event, row in zip(events, rows):
            if row.get("status") == "duplicate_event_noop":
                continue
            if row["event_id"] != event["event_id"]:
                raise ValueError("event_ledger_identity_mismatch")
            status = row["receipt"]["status"]
            quotes = event["quotes"]
            if event["kind"] != "mark":
                arm = arms[event["arm"]]
                kind = event["kind"]
                if kind == "intent" and status == "accepted":
                    if event["action"] == "open":
                        base = event["instrument"][:3]
                        unit_value = rate(base, Decimal(-1), quotes)
                        units = int(D(event["notional_usd"]) / unit_value)
                        per_unit = unit_value * margin
                    else:
                        units, per_unit = event["units"], Decimal(0)
                    arm["orders"][event["order_id"]] = {**deepcopy(event), "units": units, "filled": 0,
                                                          "margin_unit": per_unit, "live": True}
                elif kind in {"cancel_ack", "expire"} and status in {"cancelled", "expired"}:
                    arm["orders"][event["order_id"]]["live"] = False
                elif kind == "fill" and status == "filled":
                    order = arm["orders"][event["order_id"]]
                    pair = order["instrument"]
                    if order["action"] == "open":
                        arm["lots"][event["fill_id"]] = {"pair": pair, "side": order["side"], "units": event["units"],
                                                        "entry": price(pair, order["side"], True, quotes, slip),
                                                        "margin_unit": order["margin_unit"], "filled_epoch": event["filled_epoch"]}
                    else:
                        remaining = event["units"]
                        keys = sorted(order["lot_ids"], key=lambda key: (arm["lots"].get(key, {}).get("filled_epoch", -1), key))
                        for key in keys:
                            if remaining == 0 or key not in arm["lots"]:
                                continue
                            lot = arm["lots"][key]
                            units = min(remaining, lot["units"])
                            quote_pnl = lot["side"] * units * (price(pair, lot["side"], False, quotes, slip) - lot["entry"])
                            pnl = quote_pnl * rate(pair[4:], quote_pnl, quotes)
                            arm["realized"] += pnl
                            arm["cash"] += pnl
                            lot["units"] -= units
                            remaining -= units
                            if not lot["units"]:
                                del arm["lots"][key]
                        if remaining:
                            raise AssertionError("oracle_close_quantity_unreconciled")
                    order["filled"] += event["units"]
                    if order["filled"] == order["units"]:
                        order["live"] = False
                    fee = D(event["fee_usd"])
                    arm["fees"] += fee
                    arm["cash"] -= fee
                elif kind == "financing" and status == "financing_applied":
                    for lot in arm["lots"].values():
                        signed = D(event["rates"][lot["pair"]]["long" if lot["side"] > 0 else "short"]) * lot["units"]
                        amount = signed * rate(lot["pair"][4:], signed, quotes)
                        arm["financing"] += amount
                        arm["cash"] += amount
            for name, arm in arms.items():
                used = sum((lot["units"] * lot["margin_unit"] for lot in arm["lots"].values()), Decimal(0))
                reserved = sum(((order["units"] - order["filled"]) * order["margin_unit"] for order in arm["orders"].values() if order["live"]), Decimal(0))
                expected = {"cash_usd": arm["cash"], "realized_usd": arm["realized"], "financing_usd": arm["financing"],
                            "fees_usd": arm["fees"], "used_margin_usd": used, "reserved_margin_usd": reserved}
                if row["arms"][name]["equity_usd"] is not None:
                    unrealized = Decimal(0)
                    for lot in arm["lots"].values():
                        quote_pnl = lot["side"] * lot["units"] * (price(lot["pair"], lot["side"], False, quotes, slip) - lot["entry"])
                        unrealized += quote_pnl * rate(lot["pair"][4:], quote_pnl, quotes)
                    expected.update(unrealized_usd=unrealized, equity_usd=arm["cash"] + unrealized,
                                    free_margin_usd=arm["cash"] + unrealized - used - reserved)
                for key, value in expected.items():
                    residual = abs(D(row["arms"][name][key]) - value)
                    max_residual = max(max_residual, residual)
                    if residual > TOLERANCE:
                        raise AssertionError(f"independent_accounting_mismatch:{event['event_id']}:{name}:{key}:{residual}")
                checked += 1
    return {"schema_version": "forex_independent_accounting_arithmetic.v2", "status": "verified",
            "event_arm_rows_checked": checked, "absolute_decimal_tolerance_usd": str(TOLERANCE),
            "maximum_residual_usd": str(max_residual),
            "independence": "raw_accepted_event_economics_recomputed_without_predecessor_pnl_functions",
            "not_verified": ["independent_policy_admission", "optimized_large_campaign_engine_parity", "historical_execution"]}
