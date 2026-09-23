"""Batch same-instrument FIFO closes; share lifecycle/risk, not close arithmetic.

Quote and conversion validation are cached once per close event. Decimal
settlement is accumulated in the reference lot order. This optimizes a bounded
kernel, not the complete large-campaign storage/replay architecture.
"""
from copy import deepcopy
from decimal import Decimal

from accounting_events_v2 import EventLedger


class OptimizedEventLedger(EventLedger):
    def _close_lots(self, arm, order, event, units, filled):
        selected = [key for key in order["lot_ids"] if key in arm["lots"]]
        if sum(arm["lots"][key]["position"]["base_units"] for key in selected) < units:
            raise ValueError("insufficient_remaining_lot_units")
        pair = order["instrument"]
        quote = self.reference.quote_at(event["quotes"], pair, filled, self.config["quote_max_age_sec"], observed_epoch=event["epoch"])
        if quote["market_epoch"] != filled:
            raise ValueError("missing_exact_execution_quote:" + pair)
        exit_price, slippage = self.reference._executed_price(quote, order["side"], False, self.config)
        rates = self.reference.usd_rates(pair[4:], event["quotes"], filled, self.config["quote_max_age_sec"], observed_epoch=event["epoch"])
        remaining, legs = units, []
        for key in selected:
            if remaining == 0:
                break
            lot = arm["lots"][key]
            original = deepcopy(lot["position"])
            closed = min(remaining, original["base_units"])
            original["base_units"] = closed
            quote_pnl = original["side"] * closed * (exit_price - self.decimal(original["entry_price"]))
            rate = rates["sell_currency_usd"] if quote_pnl >= 0 else rates["buy_currency_usd"]
            pnl = quote_pnl * rate
            conversion = {**rates, "applied_rate": rate, "conversion_side": "sell_profit" if quote_pnl >= 0 else "buy_loss"}
            legs.append({"kind": "close", "instrument": pair, "side": original["side"], "base_units": closed,
                         "quote_id": quote["quote_id"], "market_epoch": quote["market_epoch"],
                         "available_epoch": quote["available_epoch"], "execution_epoch": filled, "observed_epoch": event["epoch"],
                         "executed_price": exit_price, "slippage_price": slippage, "quote_currency_pnl": quote_pnl,
                         "realized_usd": pnl, "conversion": conversion, "original_entry": original, "closed_lot_id": key})
            arm["realized_usd"] += pnl
            arm["cash_usd"] += pnl
            lot["position"]["base_units"] -= closed
            if lot["position"]["base_units"] == 0:
                del arm["lots"][key]
            remaining -= closed
        return legs
