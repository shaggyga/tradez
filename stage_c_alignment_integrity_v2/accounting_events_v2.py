"""Research event ledger extending the reviewed curve-management Decimal core.

Only explicitly synthetic quote/fill/financing evidence is accepted. The
predecessor supplies quote validation, side prices, PnL, conversion and marks.
This module adds multi-lot order lifecycle, capacity and accounting records; it
does not turn candle touches into fills or assert historical broker rules.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Any

from reference_accounting_adapter_v2 import DEFAULT_TRAD, load_reference, synthetic_contract

SCHEMA = "forex_synthetic_accounting_events.v2"
TIER = "synthetic_executable_quotes.v2"
RETROSPECTIVE_TIER = "retained_candle_execution_scenario.v1"
KINDS = {"intent", "activate", "trigger", "fill", "cancel_request", "cancel_ack", "expire", "financing", "mark"}


def event_contract(trad_root: Path = DEFAULT_TRAD) -> dict[str, Any]:
    reference = load_reference(trad_root)
    contract = synthetic_contract(reference)
    config = contract["reference_config"]
    config["maximum_holding_sec"] = 5 * 86400
    config["metadata"]["USD_JPY"] = {"base_currency": "USD", "quote_currency": "JPY", "pip_size": ".01", "unit_increment": 1}
    return {"schema_version": SCHEMA, "input_tier": TIER, "mode": "offline_synthetic_only",
            "arms": ["hold", "rotate"], "initial_capital_usd": "10000", "reference_config": config,
            "margin_rate": "0.10", "margin_policy": "decision_notional_fixed_per_unit_capacity_only",
            "maximum_order_notional_usd": "5000", "maximum_gross_currency_usd": "20000",
            "financing_policy": "explicit_signed_per_unit_fixture_events_only",
            "fee_policy": "explicit_nonnegative_usd_per_fill_no_spread_fee",
            "cost_basis": "separate_fill_lots_fifo_close_no_intermediate_rounding",
            "exposure_policy": "signed_base_and_entry_quote_notional_not_broker_cash",
            "rounding": "decimal50_half_even_no_cash_quantization",
            "same_time_ordering": "explicit_unique_source_sequence",
            "resting_order_policy": "explicit_quote_trigger_and_separate_fill_evidence_oco_cancellation_not_atomic",
            "broker_access": False, "can_place_orders": False, "real_money": False}


def retrospective_event_contract(trad_root: Path = DEFAULT_TRAD) -> dict[str, Any]:
    result = event_contract(trad_root)
    result.update(schema_version="forex_retrospective_accounting_events.v1",
                  input_tier=RETROSPECTIVE_TIER, mode="offline_retrospective_scenario_only",
                  financing_policy="explicit_signed_per_unit_scenario_events_not_observed_rates")
    return result


def _integer(value: Any, name: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"invalid_{name}")
    return value


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 160:
        raise ValueError(f"invalid_{name}")
    return value


class EventLedger:
    """Incremental bookkeeping around one reused exact economic core."""

    def __init__(self, contract: dict[str, Any], *, trad_root: Path = DEFAULT_TRAD):
        self.reference = load_reference(trad_root)
        self.contract = deepcopy(contract)
        defaults = (retrospective_event_contract(trad_root) if contract.get("input_tier") == RETROSPECTIVE_TIER
                    else event_contract(trad_root))
        if set(contract) != set(defaults):
            raise ValueError("explicit_exact_event_contract_required")
        for key in ("schema_version", "input_tier", "mode", "margin_policy", "financing_policy", "fee_policy",
                    "cost_basis", "exposure_policy", "rounding", "same_time_ordering", "resting_order_policy", "broker_access", "can_place_orders", "real_money"):
            if contract[key] != defaults[key] or (isinstance(defaults[key], bool) and contract[key] is not defaults[key]):
                raise ValueError(f"unsupported_or_unsafe_contract:{key}")
        self.config = self.reference.validate_config(contract["reference_config"])
        arms = contract["arms"]
        if not isinstance(arms, list) or not 1 <= len(arms) <= 8 or len(set(arms)) != len(arms):
            raise ValueError("unique_bounded_arms_required")
        for arm in arms:
            _identifier(arm, "arm")
        self.capital = self.decimal(contract["initial_capital_usd"])
        self.margin_rate = self.decimal(contract["margin_rate"])
        self.max_notional = self.decimal(contract["maximum_order_notional_usd"])
        self.currency_limit = self.decimal(contract["maximum_gross_currency_usd"])
        if min(self.capital, self.margin_rate, self.max_notional, self.currency_limit) <= 0 or self.margin_rate > 1:
            raise ValueError("positive_bounded_risk_contract_required")
        self.state = {"schema_version": contract["schema_version"], "contract_fingerprint": self.reference.digest(contract),
                      "cursor": None, "event_ids": {}, "arms": {name: self._flat() for name in arms}}

    def decimal(self, value: Any) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            raise ValueError("exact_decimal_string_or_integer_required")
        return self.reference.number(value)

    def _flat(self) -> dict[str, Any]:
        return {"cash_usd": self.capital, "realized_usd": Decimal(0), "financing_usd": Decimal(0),
                "fees_usd": Decimal(0), "lots": {}, "orders": {}, "fill_ids": {}, "financing_ids": {}, "financing_periods": {}}

    def _remaining(self, order: dict[str, Any]) -> int:
        return order["units"] - order["filled_units"] if order["status"] in {"pending", "active", "triggered", "cancel_pending"} else 0

    def _exposures(self, lots: dict[str, Any], quotes: dict[str, Any], epoch: int) -> dict[str, Any]:
        native: dict[str, dict[str, Decimal]] = {}
        for lot in lots.values():
            position = lot["position"]
            base, quote = position["instrument"].split("_")
            quantity = Decimal(position["side"] * position["base_units"])
            for currency, amount in ((base, quantity), (quote, -quantity * self.decimal(position["entry_price"]))):
                item = native.setdefault(currency, {"net_native": Decimal(0), "gross_native": Decimal(0)})
                item["net_native"] += amount
                item["gross_native"] += abs(amount)
        for currency, item in native.items():
            rates = self.reference.usd_rates(currency, quotes, epoch, self.config["quote_max_age_sec"])
            item["gross_usd_conservative"] = item["gross_native"] * rates["buy_currency_usd"]
            rate = rates["sell_currency_usd"] if item["net_native"] >= 0 else rates["buy_currency_usd"]
            item["net_liquidation_usd"] = item["net_native"] * rate
            item["conversion"] = rates
        return native

    def reconcile(self, arm: dict[str, Any], quotes: dict[str, Any], epoch: int) -> dict[str, Any]:
        with localcontext(self.reference.CTX):
            return self._reconcile(arm, quotes, epoch)

    def _reconcile(self, arm: dict[str, Any], quotes: dict[str, Any], epoch: int) -> dict[str, Any]:
        expected = self.capital + arm["realized_usd"] + arm["financing_usd"] - arm["fees_usd"]
        if abs(arm["cash_usd"] - expected) > Decimal("1e-40"):
            raise AssertionError("cash_reconciliation_failed")
        used = sum((lot["margin_per_unit_usd"] * lot["position"]["base_units"] for lot in arm["lots"].values()), Decimal(0))
        reserved = sum((order["margin_per_unit_usd"] * self._remaining(order) for order in arm["orders"].values()), Decimal(0))
        unrealized = Decimal(0)
        unavailable = []
        for lot_id, lot in arm["lots"].items():
            mark = self._mark_position(lot["position"], quotes, epoch)
            if mark["equity_usd"] is None:
                unavailable.append({"lot_id": lot_id, "reason": mark["reason"]})
            else:
                unrealized += mark["equity_usd"]
        try:
            exposures = self._exposures(arm["lots"], quotes, epoch)
        except (ValueError, KeyError, TypeError) as exc:
            exposures = None
            unavailable.append({"exposure": str(exc)})
        equity = None if unavailable else arm["cash_usd"] + unrealized
        free = None if equity is None else equity - used - reserved
        concentration = None if exposures is None else any(item["gross_usd_conservative"] > self.currency_limit for item in exposures.values())
        overdue = [key for key, lot in arm["lots"].items() if epoch >= min(lot["position"]["original_target_epoch"],
                    lot["position"]["entry_epoch"] + int(self.config["maximum_holding_sec"]))]
        known_breach = bool(overdue) or concentration is True or (free is not None and free < 0)
        return {"cash_usd": arm["cash_usd"], "cash_reconciliation_residual_usd": arm["cash_usd"] - expected,
                "realized_usd": arm["realized_usd"], "financing_usd": arm["financing_usd"], "fees_usd": arm["fees_usd"],
                "unrealized_usd": None if unavailable else unrealized, "equity_usd": equity,
                "used_margin_usd": used, "reserved_margin_usd": reserved, "free_margin_usd": free,
                "currency_exposures": exposures, "concentration_breach": concentration,
                "risk_status": "breached" if known_breach else "unknown" if unavailable else "within_contract",
                "holding_deadline_breaches": overdue,
                "unavailable": unavailable, "open_lot_count": len(arm["lots"]),
                "pending_order_units": sum(self._remaining(order) for order in arm["orders"].values())}

    def _mark_position(self, position, quotes, epoch):
        """Value at the latest permitted quote; this is not an actual close fill.

        Actual fills retain their exact execution-quote requirement. Reusing the
        close-fill path for marks incorrectly rejected valid nonzero quote ages.
        """
        try:
            with localcontext(self.reference.CTX):
                pair=position['instrument']
                q=self.reference.quote_at(quotes,pair,epoch,self.config['quote_max_age_sec'])
                price,_=self.reference._executed_price(q,position['side'],False,self.config)
                pnl=position['side']*position['base_units']*(price-self.decimal(position['entry_price']))
                value,_=self.reference.convert_pnl_to_usd(pnl,self.config['metadata'][pair]['quote_currency'],
                    quotes,epoch,self.config['quote_max_age_sec'])
                return {'equity_usd':value,'reason':'latest_permitted_quote_liquidation_estimate'}
        except (ValueError,KeyError,TypeError) as exc:
            return {'equity_usd':None,'reason':str(exc)}

    def _admit(self, arm: dict[str, Any], quotes: dict[str, Any], epoch: int) -> None:
        report = self.reconcile(arm, quotes, epoch)
        if report["risk_status"] != "within_contract":
            raise ValueError("capacity_or_exposure_unavailable_or_breached")
        projected = deepcopy(arm["lots"])
        for key, order in arm["orders"].items():
            units = self._remaining(order)
            if not units or order["action"] != "open":
                continue
            quote = self.reference.quote_at(quotes, order["instrument"], epoch, self.config["quote_max_age_sec"])
            entry, _ = self.reference._executed_price(quote, order["side"], True, self.config)
            projected[f"reserved:{key}"] = {"position": {"instrument": order["instrument"], "side": order["side"],
                                                         "base_units": units, "entry_price": entry}}
        committed = self._exposures(projected, quotes, epoch)
        if any(item["gross_usd_conservative"] > self.currency_limit for item in committed.values()):
            raise ValueError("committed_currency_concentration_limit")

    def _intent(self, arm: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        order_id = _identifier(event["order_id"], "order_id")
        if order_id in arm["orders"]:
            raise ValueError("order_identity_already_exists")
        pair = self.reference.pair_name(event["instrument"])
        if pair not in self.config["metadata"]:
            raise ValueError("instrument_metadata_missing")
        action = event["action"]
        epoch = event["epoch"]
        order_type = event.get("order_type", "market")
        if order_type not in {"market", "stop", "limit"}:
            raise ValueError("unsupported_order_type")
        trigger_price = None if order_type == "market" else self.decimal(event["trigger_price"])
        if trigger_price is not None and trigger_price <= 0:
            raise ValueError("positive_trigger_price_required")
        if "oco_group" in event:
            _identifier(event["oco_group"], "oco_group")
        if action == "open":
            if type(event["side"]) is not int or event["side"] not in (-1, 1):
                raise ValueError("invalid_side")
            notional = self.decimal(event["notional_usd"])
            if not 0 < notional <= self.max_notional:
                raise ValueError("order_notional_outside_contract")
            sizing_config = {**self.config, "notional_usd": notional}
            units, sizing = self.reference.size_at_decision(pair, event["quotes"], epoch, sizing_config)
            if units < 1:
                raise ValueError("zero_sized_order")
            target = _integer(event["target_epoch"], "target_epoch")
            if not epoch + int(self.config["execution_delay_sec"]) < target <= epoch + int(self.config["maximum_holding_sec"]):
                raise ValueError("invalid_order_target")
            margin_per_unit = sizing["decision_value_usd"] / units * self.margin_rate
            lot_ids = []
            side = event["side"]
        elif action == "reduce":
            units = _integer(event["units"], "units", 1)
            lot_ids = event["lot_ids"]
            if not isinstance(lot_ids, list) or not lot_ids or len(set(lot_ids)) != len(lot_ids):
                raise ValueError("explicit_unique_close_lots_required")
            if any(key not in arm["lots"] or arm["lots"][key]["position"]["instrument"] != pair for key in lot_ids):
                raise ValueError("close_lot_identity_mismatch")
            side = arm["lots"][lot_ids[0]]["position"]["side"]
            if any(arm["lots"][key]["position"]["side"] != side for key in lot_ids):
                raise ValueError("one_side_close_order_required")
            if units > sum(arm["lots"][key]["position"]["base_units"] for key in lot_ids):
                raise ValueError("close_units_exceed_position")
            if any(self._remaining(order) and order["action"] == "reduce" and set(order["lot_ids"]).intersection(lot_ids) for order in arm["orders"].values()):
                raise ValueError("overlapping_close_reservation")
            margin_per_unit, sizing, target = Decimal(0), {}, event["epoch"] + int(self.config["maximum_holding_sec"])
            lot_ids = sorted(lot_ids, key=lambda key: (arm["lots"][key]["position"]["entry_epoch"], key))
        else:
            raise ValueError("unsupported_order_action")
        order = {"order_id": order_id, "instrument": pair, "action": action, "side": side, "units": units,
                 "filled_units": 0, "decision_epoch": epoch, "active_after_epoch": epoch + int(self.config["execution_delay_sec"]),
                 "status": "pending", "sizing": sizing, "target_epoch": target, "lot_ids": lot_ids,
                 "margin_per_unit_usd": margin_per_unit, "order_type": order_type,
                 "trigger_price": trigger_price, "oco_group": event.get("oco_group")}
        arm["orders"][order_id] = order
        if action == "open":
            self._admit(arm, event["quotes"], epoch)
        return {"status": "accepted", "order": deepcopy(order)}

    def _fill(self, arm: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        fill_id = _identifier(event["fill_id"], "fill_id")
        filled = _integer(event["filled_epoch"], "filled_epoch")
        fingerprint = self.reference.digest({key: event[key] for key in
            ("fill_id", "order_id", "units", "filled_epoch", "quotes", "fee_usd", "execution_evidence_id")})
        previous = arm["fill_ids"].get(fill_id)
        if previous is not None:
            if previous != fingerprint:
                raise ValueError("conflicting_duplicate_fill_identity")
            return {"status": "duplicate_fill_noop", "fill_id": fill_id}
        order = arm["orders"][event["order_id"]]
        units = _integer(event["units"], "fill_units", 1)
        if order["status"] not in {"active", "triggered", "cancel_pending"} or "active_epoch" not in order:
            raise ValueError("order_not_active")
        if filled < order["active_epoch"] or filled <= order["decision_epoch"] or filled > event["epoch"]:
            raise ValueError("fill_precedes_activation_or_is_future")
        if units > self._remaining(order):
            raise ValueError("fill_exceeds_remaining_order_units")
        if order["order_type"] != "market" and ("triggered_epoch" not in order or filled < order["triggered_epoch"]):
            raise ValueError("resting_fill_requires_prior_trigger_evidence")
        if order["order_type"] == "limit":
            q = self.reference.quote_at(event["quotes"], order["instrument"], filled, self.config["quote_max_age_sec"], observed_epoch=event["epoch"])
            fill_price, _ = self.reference._executed_price(q, order["side"], order["action"] == "open", self.config)
            transaction_side = order["side"] if order["action"] == "open" else -order["side"]
            if (transaction_side > 0 and fill_price > order["trigger_price"]) or (transaction_side < 0 and fill_price < order["trigger_price"]):
                raise ValueError("fill_price_violates_limit")
        fee = self.decimal(event["fee_usd"])
        if fee < 0:
            raise ValueError("nonnegative_explicit_fee_required")
        _identifier(event["execution_evidence_id"], "execution_evidence_id")
        receipt_legs = []
        if order["action"] == "open":
            if filled >= order["target_epoch"]:
                raise ValueError("opening_fill_after_target")
            decision = {"action": "enter", "instrument": order["instrument"], "side": order["side"],
                        "base_units": units, "decision_epoch": order["decision_epoch"], "sizing": order["sizing"],
                        "original_target_epoch": order["target_epoch"], "candidate_id": order["order_id"]}
            # Public predecessor apply_action fixes execution to one time. Its
            # reviewed economic core is reused after this adapter's separate
            # intent/activation/fill clock checks, allowing later partial fills
            # without inventing a different decision clock or resizing units.
            after, receipt = self.reference._apply_action(self.reference.flat_state(), decision, event["quotes"], filled,
                                                          self.config, observed_epoch=event["epoch"])
            if receipt["status"] != "applied":
                raise ValueError(receipt["reason"])
            arm["lots"][fill_id] = {"position": after["position"], "margin_per_unit_usd": order["margin_per_unit_usd"],
                                    "opening_fill_id": fill_id, "order_id": order["order_id"]}
            receipt_legs = receipt["legs"]
        else:
            receipt_legs = self._close_lots(arm, order, event, units, filled)
        arm["fees_usd"] += fee
        arm["cash_usd"] -= fee
        order["filled_units"] += units
        if order["filled_units"] == order["units"]:
            order["status"] = "filled"
        # Opening fills re-check equity/spread/fees and current currency risk;
        # reducing risk remains allowed while a pre-existing limit is breached.
        if order["action"] == "open":
            self._admit(arm, event["quotes"], event["epoch"])
        arm["fill_ids"][fill_id] = fingerprint
        return {"status": "filled", "fill_id": fill_id, "units": units, "fee_usd": fee,
                "execution_evidence_id": event["execution_evidence_id"], "legs": receipt_legs}

    def _close_lots(self, arm, order, event, units, filled):
        """Readable reference: reuse one independently applied core exit per lot."""
        remaining, legs = units, []
        for lot_id in order["lot_ids"]:
            if remaining == 0:
                break
            if lot_id not in arm["lots"]:
                continue
            lot = arm["lots"][lot_id]
            close_units = min(remaining, lot["position"]["base_units"])
            partial = deepcopy(lot["position"])
            partial["base_units"] = close_units
            after, receipt = self.reference._apply_action({"realized_usd": Decimal(0), "position": partial},
                {"action": "exit"}, event["quotes"], filled, self.config, observed_epoch=event["epoch"])
            if receipt["status"] != "applied":
                raise ValueError(receipt["reason"])
            arm["realized_usd"] += after["realized_usd"]
            arm["cash_usd"] += after["realized_usd"]
            legs.extend({**leg, "closed_lot_id": lot_id} for leg in receipt["legs"])
            lot["position"]["base_units"] -= close_units
            if lot["position"]["base_units"] == 0:
                del arm["lots"][lot_id]
            remaining -= close_units
        if remaining:
            raise ValueError("insufficient_remaining_lot_units")
        return legs

    def _financing(self, arm: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        identity = _identifier(event["financing_id"], "financing_id")
        _identifier(event["provenance_id"], "financing_provenance")
        _identifier(event["accrual_period_id"], "accrual_period_id")
        fingerprint = self.reference.digest({key: event[key] for key in ("rates", "quotes", "epoch", "provenance_id", "accrual_period_id")})
        if identity in arm["financing_ids"]:
            if arm["financing_ids"][identity] != fingerprint:
                raise ValueError("conflicting_financing_identity")
            return {"status": "duplicate_financing_noop", "financing_id": identity}
        if event["accrual_period_id"] in arm["financing_periods"]:
            raise ValueError("financing_period_already_applied_requires_explicit_correction_contract")
        legs = []
        total = Decimal(0)
        for lot_id, lot in arm["lots"].items():
            position = lot["position"]
            pair = position["instrument"]
            side = "long" if position["side"] > 0 else "short"
            if pair not in event["rates"] or side not in event["rates"][pair]:
                raise ValueError("explicit_financing_rate_missing")
            rate = self.decimal(event["rates"][pair][side])
            quote_amount = rate * position["base_units"]
            amount, conversion = self.reference.convert_pnl_to_usd(quote_amount, pair[4:], event["quotes"],
                                                                  event["epoch"], self.config["quote_max_age_sec"])
            total += amount
            legs.append({"lot_id": lot_id, "units": position["base_units"], "rate_quote_per_unit": rate,
                         "quote_currency": pair[4:], "quote_amount": quote_amount, "amount_usd": amount, "conversion": conversion})
        arm["financing_usd"] += total
        arm["cash_usd"] += total
        arm["financing_ids"][identity] = fingerprint
        arm["financing_periods"][event["accrual_period_id"]] = identity
        return {"status": "financing_applied", "financing_id": identity, "amount_usd": total, "legs": legs,
                "provenance_id": event["provenance_id"], "accrual_period_id": event["accrual_period_id"]}

    def _dispatch(self, arm: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
        kind = event["kind"]
        if kind == "intent":
            return self._intent(arm, event)
        if kind == "fill":
            return self._fill(arm, event)
        if kind == "financing":
            return self._financing(arm, event)
        order = arm["orders"][event["order_id"]]
        if kind == "activate":
            if (order["status"] not in {"pending", "cancel_pending"} or "active_epoch" in order
                    or event["epoch"] < order["active_after_epoch"]):
                raise ValueError("activation_before_latency_or_invalid_state")
            order["active_epoch"] = event["epoch"]
            if order["status"] != "cancel_pending":
                order["status"] = "active"
        elif kind == "trigger":
            if order["order_type"] == "market" or order["status"] not in {"active", "cancel_pending"} or "active_epoch" not in order:
                raise ValueError("trigger_requires_active_resting_order")
            _identifier(event["trigger_evidence_id"], "trigger_evidence_id")
            q = self.reference.quote_at(event["quotes"], order["instrument"], event["epoch"], self.config["quote_max_age_sec"])
            if q["market_epoch"] != event["epoch"] or q["market_epoch"] < order["active_epoch"]:
                raise ValueError("trigger_quote_precedes_activation_or_current_event")
            transaction_side = order["side"] if order["action"] == "open" else -order["side"]
            observed = q["ask"] if transaction_side > 0 else q["bid"]
            reached = observed >= order["trigger_price"] if transaction_side > 0 else observed <= order["trigger_price"]
            if order["order_type"] == "limit":
                reached = observed <= order["trigger_price"] if transaction_side > 0 else observed >= order["trigger_price"]
            if not reached:
                raise ValueError("quote_has_not_reached_trigger")
            order.update(triggered_epoch=event["epoch"], trigger_evidence_id=event["trigger_evidence_id"], trigger_quote_id=q["quote_id"])
            if order["status"] != "cancel_pending":
                order["status"] = "triggered"
        elif kind == "cancel_request":
            if not self._remaining(order):
                raise ValueError("cancel_request_requires_open_order")
            order.update(status="cancel_pending", cancel_requested_epoch=event["epoch"])
        elif kind == "cancel_ack":
            if order["status"] == "filled":
                return {"status": "late_cancel_ack_after_fill", "order_id": order["order_id"]}
            if order["status"] != "cancel_pending":
                raise ValueError("cancel_ack_without_request")
            order.update(status="cancelled", cancel_ack_epoch=event["epoch"])
        elif kind == "expire":
            if event["epoch"] < order["target_epoch"] or not self._remaining(order):
                raise ValueError("expiry_before_deadline_or_closed_order")
            order.update(status="expired", expiry_epoch=event["epoch"])
        return {"status": order["status"], "order_id": order["order_id"]}

    def _validate_event(self, event: dict[str, Any]) -> None:
        base = {"event_id", "kind", "epoch", "sequence", "quotes", "input_tier"}
        kind = event.get("kind")
        if kind not in KINDS or event.get("input_tier") != self.contract["input_tier"]:
            raise ValueError("unsupported_event_or_input_tier")
        specific = {"mark": set(), "activate": {"arm", "order_id"}, "cancel_request": {"arm", "order_id"},
                    "trigger": {"arm", "order_id", "trigger_evidence_id"},
                    "cancel_ack": {"arm", "order_id"}, "expire": {"arm", "order_id"},
                    "fill": {"arm", "order_id", "fill_id", "units", "filled_epoch", "fee_usd", "execution_evidence_id"},
                    "financing": {"arm", "financing_id", "rates", "provenance_id", "accrual_period_id"},
                    "intent": {"arm", "order_id", "instrument", "action"}}
        required = base | specific[kind]
        if kind == "intent":
            required |= {"side", "notional_usd", "target_epoch"} if event.get("action") == "open" else {"units", "lot_ids"}
            required |= {key for key in ("order_type", "oco_group") if key in event}
            if event.get("order_type", "market") in {"stop", "limit"}:
                required.add("trigger_price")
        if set(event) != required:
            raise ValueError("exact_event_schema_required_no_implicit_spread_fees_or_fill_assumptions")
        _identifier(event["event_id"], "event_id")
        _integer(event["epoch"], "epoch")
        _integer(event["sequence"], "sequence")
        if not isinstance(event["quotes"], dict) or (kind != "mark" and event["arm"] not in self.state["arms"]):
            raise ValueError("invalid_quote_panel_or_arm")

    def apply(self, event: dict[str, Any]) -> dict[str, Any]:
        """Consume one event atomically. Rejected proposals leave economics intact."""
        self._validate_event(event)
        fingerprint = self.reference.digest(event)
        event_id = event["event_id"]
        known = self.state["event_ids"].get(event_id)
        if known is not None:
            if known != fingerprint:
                raise ValueError("conflicting_event_id")
            return {"event_id": event_id, "status": "duplicate_event_noop"}
        cursor = (event["epoch"], event["sequence"])
        if self.state["cursor"] is not None and cursor <= tuple(self.state["cursor"]):
            raise ValueError("event_clock_must_increase_with_unique_source_sequence")
        with localcontext(self.reference.CTX):
            next_arms = dict(self.state["arms"])
            if event["kind"] == "mark":
                receipt = {"status": "marked"}
            else:
                arm_name = event["arm"]
                candidate = deepcopy(self.state["arms"][arm_name])
                try:
                    receipt = self._dispatch(candidate, event)
                except (ValueError, KeyError, TypeError) as exc:
                    receipt = {"status": "rejected", "reason": str(exc), "economics_unchanged": True}
                else:
                    next_arms[arm_name] = candidate
            reconciliation = {name: self.reconcile(arm, event["quotes"], event["epoch"])
                              for name, arm in next_arms.items()}
            self.state["arms"] = next_arms
            self.state["cursor"] = list(cursor)
            self.state["event_ids"][event_id] = fingerprint
            return {"event_id": event_id, "kind": event["kind"], "epoch": event["epoch"], "sequence": event["sequence"],
                    "arm": event.get("arm"), "receipt": receipt, "arms": reconciliation,
                    "state_fingerprint": self.reference.digest(self.state)}

    def replay(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.apply(event) for event in events]
