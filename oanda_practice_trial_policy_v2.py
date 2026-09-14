"""Pure conservative Practice007 trial arithmetic, not execution authority.

The caller authenticates account/market/source reads, enforces its exact practice
account allowlist and durable intent lock, persists the loss latch, and rechecks
before transport. This module performs no I/O and does not make a broker call.
Stops bound the modeled loss at their price, not gaps or guaranteed execution.
HomeConversions follows OANDA pricing-df: accountGain, accountLoss and
positionValue have distinct uses. Research signal flags are never rewritten.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, Context, localcontext, ROUND_FLOOR, ROUND_CEILING
import hashlib
import json
import math
import re

from src.forex_system.contracts.signed_currency_exposure import split_currency_pair

SCHEMA = "practice_trial_policy_v2_20260913"
CTX = Context(prec=192)
MAXIMUM_SESSION_SEC = 48 * 3600
POLICY = {
    "account_currency": "USD", "maximum_open_positions": 1,
    "maximum_margin_nav_fraction": "0.20", "risk_nav_fraction": "0.005",
    "margin_scope": "entry_sizing_cap_not_continuous_rebalance_or_forced_liquidation",
    "session_loss_start_nav_fraction": "0.10", "session_loss_usd_cap": "5",
    "minimum_side_probability": "0.55", "minimum_gross_cost_multiple": "2",
    "maximum_spread_bps": "4", "slippage_bps_per_leg": "0.1",
    "atr_true_ranges": 14, "atr_multiplier": "2", "atr_latest_close_max_age_sec": 90,
    "quote_max_age_sec": 15, "account_conversion_max_age_sec": 30,
    "maximum_original_signal_age_sec": 900,
    "maximum_entries_per_utc_day": 8, "same_pair_cooldown_sec": 1800,
    "minimum_remaining_sec": 60, "original_horizon_sec": 3600,
    "maximum_session_sec": MAXIMUM_SESSION_SEC, "entry_target_deadline_buffer_sec": 300,
    "policy_selection_scope": "explicit_conservative_trial_defaults_not_fitted_or_performance_validated",
}
_POLICY_JSON = json.dumps(POLICY, sort_keys=True, separators=(",", ":"))
MAX_ITEMS = 4096
MAX_BYTES = 128 * 1024


class PolicyError(ValueError):
    pass


def _need(ok, reason):
    if not ok:
        raise PolicyError(reason)


def _owned(value):
    count = 0
    def walk(x, depth):
        nonlocal count
        count += 1
        _need(count <= MAX_ITEMS and depth <= 10, "input_resource_bound")
        if type(x) is dict:
            _need(all(type(k) is str and len(k) <= 128 for k in x), "input_keys")
            for v in x.values(): walk(v, depth + 1)
        elif type(x) is list:
            for v in x: walk(v, depth + 1)
        elif type(x) is str:
            _need(len(x) <= 4096, "input_string_bound")
        elif type(x) is float:
            _need(math.isfinite(x), "input_nonfinite")
        else:
            _need(x is None or type(x) in (bool, int), "input_type")
    walk(value, 0)
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    _need(len(body) <= MAX_BYTES, "input_byte_bound")
    return json.loads(body), hashlib.sha256(body).hexdigest()


def _number(value, name, *, signed=False, zero=False):
    _need(type(value) is str and len(value) <= 128 and
          re.fullmatch(r"-?(?:0|[1-9][0-9]{0,14})(?:\.[0-9]{1,40})?", value), name + "_decimal_text")
    d = Decimal(value)
    _need(d.is_finite() and abs(d) <= Decimal("1e14"), name + "_bound")
    _need(signed or (d >= 0 if zero else d > 0), name + "_positive")
    return d


def _epoch(value, name="clock"):
    _need(type(value) in (int, float) and math.isfinite(value) and 0 < value < 10**11, name + "_invalid")
    return Decimal(str(value))


def _integer(value, name, minimum=0, maximum=1000000):
    _need(type(value) is int and minimum <= value <= maximum, name + "_invalid")
    return value


def _required(row, fields, name):
    _need(type(row) is dict and set(fields) <= set(row), name + "_fields")


def _pair(value):
    _need(type(value) is str and re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", value), "instrument_identity")
    base, quote = split_currency_pair(value)
    _need(base != quote, "instrument_same_currency")
    return base, quote


def _sha(value, name):
    _need(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value), name + "_sha256")


def _fresh(value, decision, max_age, name):
    observed = _epoch(value, name)
    _need(observed <= decision, name + "_future")
    _need(decision - observed <= max_age, name + "_stale")
    return observed


def _session(session, decision, *, entry_checks=False):
    _required(session, ("started_epoch", "start_nav_usd", "stop_epoch", "loss_stop_latched"), "session")
    started = _epoch(session["started_epoch"], "session_start")
    stop = _epoch(session["stop_epoch"], "session_stop")
    _need(started <= decision and started < stop and stop-started <= MAXIMUM_SESSION_SEC, "session_clock_order")
    nav = _number(session["start_nav_usd"], "session_start_nav")
    _need(type(session["loss_stop_latched"]) is bool, "session_latch_unknown")
    entries = None
    if entry_checks:
        _required(session, ("day_utc", "entries_today", "last_entry_epoch_by_instrument"), "session_entry")
        _need(session["day_utc"] == datetime.fromtimestamp(float(decision), timezone.utc).date().isoformat(), "session_daily_counters_stale")
        entries = _integer(session["entries_today"], "entries_today", 0, 100000)
        last = session["last_entry_epoch_by_instrument"]
        _need(type(last) is dict and len(last) <= 68, "cooldown_inventory")
        for pair, value in last.items():
            _pair(pair); e = _epoch(value, "last_entry")
            _need(started <= e <= decision, "last_entry_clock")
    return nav, stop, entries


def _account(account, decision):
    _required(account, ("currency", "NAV", "marginUsed", "marginAvailable", "marginRate",
                        "openTradeCount", "pendingOrderCount", "observed_epoch"), "account")
    _need(account["currency"] == "USD", "account_currency_not_usd")
    _fresh(account["observed_epoch"], decision, 30, "account_observation")
    nav = _number(account["NAV"], "account_nav", signed=True)
    used = _number(account["marginUsed"], "margin_used", zero=True)
    available = _number(account["marginAvailable"], "margin_available", signed=True)
    rate = _number(account["marginRate"], "account_margin_rate")
    _need(rate <= 1, "account_margin_rate_bound")
    trades = _integer(account["openTradeCount"], "open_trade_count", 0, 100000)
    pending = _integer(account["pendingOrderCount"], "pending_order_count", 0, 100000)
    return nav, used, available, rate, trades, pending


def _guard(account, session, decision):
    start_nav, stop, entries = _session(session, decision)
    nav, used, available, rate, trades, pending = _account(account, decision)
    loss_limit = min(start_nav * Decimal("0.10"), Decimal("5"))
    reached = start_nav - nav >= loss_limit or nav <= 0
    latched = session["loss_stop_latched"] or reached
    reason = "session_loss_stop" if latched else "session_ended" if decision >= stop else "within_session_limits"
    return dict(status="halt" if latched or decision >= stop else "within_limits", reason=reason,
                loss_stop_latched=latched, latch_must_be_persisted=latched,
                session_loss_limit_usd=str(loss_limit), drawdown_usd=str(start_nav-nav),
                current_nav_usd=str(nav), session_stop_epoch=session["stop_epoch"],
                current_margin_used_usd=str(used), entries_today=entries)


def _quote(quote, instrument, decision):
    _required(quote, ("instrument", "bid", "ask", "tradeable", "market_epoch", "observed_epoch"), "quote")
    _need(quote["instrument"] == instrument, "quote_instrument_mismatch")
    _need(quote["tradeable"] is True, "quote_not_explicitly_tradeable")
    observed = _fresh(quote["observed_epoch"], decision, 15, "quote_observation")
    market = _fresh(quote["market_epoch"], decision, 15, "quote_market")
    _need(market <= observed, "quote_market_after_observation")
    bid = _number(quote["bid"], "bid"); ask = _number(quote["ask"], "ask")
    _need(bid <= ask, "crossed_quote")
    return bid, ask, (bid+ask)/2


def _metadata(metadata, instrument):
    _required(metadata, ("name", "type", "marginRate", "minimumTradeSize", "tradeUnitsPrecision",
                         "pipLocation", "displayPrecision", "maximumOrderUnits", "maximumPositionSize"), "metadata")
    _need(metadata["name"] == instrument and metadata["type"] == "CURRENCY", "metadata_instrument_type")
    rate = _number(metadata["marginRate"], "instrument_margin_rate"); _need(rate <= 1, "instrument_margin_rate_bound")
    minimum = _number(metadata["minimumTradeSize"], "minimum_trade_size")
    order_max = _number(metadata["maximumOrderUnits"], "maximum_order_units")
    position_max = _number(metadata["maximumPositionSize"], "maximum_position_size", zero=True)
    units_precision = _integer(metadata["tradeUnitsPrecision"], "trade_units_precision", 0, 6)
    precision = _integer(metadata["displayPrecision"], "display_precision", 0, 10)
    location = _integer(metadata["pipLocation"], "pip_location", -10, 0)
    _need(precision >= -location, "pip_vs_display_precision")
    return rate, minimum, order_max, position_max, Decimal(10)**(-units_precision), Decimal(10)**(-precision), Decimal(10)**location


def _conversion(home_conversions, currency, decision):
    _required(home_conversions, ("observed_epoch", "rows"), "home_conversions")
    _fresh(home_conversions["observed_epoch"], decision, 30, "conversion_observation")
    rows = home_conversions["rows"]
    _need(type(rows) is list and 0 <= len(rows) <= 32, "conversion_row_count")
    parsed = {}
    for row in rows:
        _required(row, ("currency", "accountGain", "accountLoss", "positionValue"), "conversion")
        key = row["currency"]
        _need(type(key) is str and re.fullmatch(r"[A-Z]{3}", key) and key not in parsed, "conversion_currency_duplicate_or_invalid")
        parsed[key] = tuple(_number(row[k], "conversion_"+k) for k in ("accountGain", "accountLoss", "positionValue"))
    if currency == "USD" and currency not in parsed:
        return (Decimal(1),)*3  # Exact identity, actual account currency was checked.
    _need(currency in parsed, "quote_currency_conversion_missing")
    rates = parsed[currency]
    if currency == "USD": _need(rates == (Decimal(1),)*3, "home_currency_conversion_not_one")
    return rates


def _atr(candles, instrument, decision):
    _required(candles, ("instrument", "observed_epoch", "source_sha256", "rows"), "candles")
    _need(candles["instrument"] == instrument, "candle_instrument_mismatch")
    _sha(candles["source_sha256"], "candles")
    observed = _fresh(candles["observed_epoch"], decision, 90, "candle_observation")
    rows = candles["rows"]; _need(type(rows) is list and len(rows) == 15, "atr_requires_15_real_rows")
    previous_label = None; previous_close = None; ranges = []
    for row in rows:
        _required(row, ("label_epoch", "complete", "mid"), "candle")
        _need(row["complete"] is True, "candle_incomplete")
        label = _epoch(row["label_epoch"], "candle_label")
        _need(label % 60 == 0, "candle_not_native_m1")
        _need(label+60 <= observed, "candle_close_not_yet_observed")
        if previous_label is not None: _need(label-previous_label == 60, "candle_gap_or_duplicate")
        _required(row["mid"], ("h", "l", "c"), "candle_mid")
        high, low, close = (_number(row["mid"][k], "candle_"+k) for k in ("h", "l", "c"))
        _need(low <= close <= high, "candle_ohlc_invalid")
        if previous_close is not None: ranges.append(max(high-low, abs(high-previous_close), abs(low-previous_close)))
        previous_label, previous_close = label, close
    _need(decision-(previous_label+60) <= 90, "atr_latest_complete_bar_stale")
    atr = sum(ranges, Decimal(0))/14
    _need(atr > 0, "atr_zero_or_unavailable")
    return atr, float(previous_label+60)


def _entry(signal, quote, account, metadata, conversions, candles, session, decision):
    guard = _guard(account, session, decision)
    if guard["status"] != "within_limits":
        return dict(status="refused", action="none", reason=guard["reason"],
                    loss_stop_latched=guard["loss_stop_latched"], latch_must_be_persisted=guard["loss_stop_latched"],
                    session_loss_limit_usd=guard["session_loss_limit_usd"], drawdown_usd=guard["drawdown_usd"])
    _session(session, decision, entry_checks=True)
    nav, margin_used, margin_available, account_rate, trades, pending = _account(account, decision)
    _need(trades == 0, "existing_position_blocks_entry_or_adding")
    _need(pending == 0, "pending_order_blocks_entry")
    _need(session["entries_today"] < 8, "daily_entry_limit")
    _required(signal, ("instrument", "side", "reference_epoch", "reference_price", "original_target_epoch",
                       "expected_terminal_price", "probability_up", "issued_epoch", "available_epoch", "forecast_sha256"), "signal")
    instrument = signal["instrument"]; base, quote_currency = _pair(instrument)
    side = signal["side"]; _need(type(side) is int and side in (-1, 1), "signal_neutral_or_invalid_side")
    _sha(signal["forecast_sha256"], "forecast")
    reference = _epoch(signal["reference_epoch"], "reference")
    issued = _epoch(signal["issued_epoch"], "issued"); available = _epoch(signal["available_epoch"], "signal_available")
    target = _epoch(signal["original_target_epoch"], "original_target")
    _need(reference <= issued <= available <= decision < target, "signal_clock_order")
    _need(decision-issued <= 900, "original_signal_stale")
    _need(float(target)-float(reference) == 3600.0, "signal_not_original_h1")
    _need(target-decision >= 60, "insufficient_remaining_horizon")
    _need(target <= _epoch(session["stop_epoch"])-300,
          "target_beyond_entry_deadline")
    last = session["last_entry_epoch_by_instrument"].get(instrument)
    if last is not None: _need(decision-_epoch(last) >= 1800, "pair_cooldown")
    reference_price = _number(signal["reference_price"], "reference_price")
    terminal = _number(signal["expected_terminal_price"], "expected_terminal_price")
    _need((terminal-reference_price)*side > 0, "original_direction_terminal_mismatch")
    p = signal["probability_up"]
    _need(type(p) in (float, int, str) and type(p) is not bool, "probability_invalid")
    try: probability = Decimal(str(p))
    except Exception as exc: raise PolicyError("probability_invalid") from exc
    _need(probability.is_finite() and 0 <= probability <= 1, "probability_bound")
    side_probability = probability if side == 1 else 1-probability
    _need(side_probability >= Decimal("0.55"), "side_probability_below_minimum")
    bid, ask, mid = _quote(quote, instrument, decision)
    rate, minimum, order_max, position_max, unit_increment, tick, pip = _metadata(metadata, instrument)
    gain, loss, position_value = _conversion(conversions, quote_currency, decision)
    atr, atr_close = _atr(candles, instrument, decision)
    spread = ask-bid
    _need(spread/mid*10000 <= 4, "spread_bps_above_limit")
    slip = mid*Decimal("0.1")/10000
    raw_entry_bound = ask+slip if side == 1 else bid-slip
    entry_bound = (raw_entry_bound/tick).to_integral_value(rounding=ROUND_CEILING if side == 1 else ROUND_FLOOR)*tick
    rounding_slip = Decimal(side)*(entry_bound-raw_entry_bound)
    _need(rounding_slip >= 0 and entry_bound > 0, "entry_bound_invalid")
    gross_per_unit_quote = Decimal(side)*(terminal-mid)
    cost_per_unit_quote = spread+2*slip+rounding_slip
    _need(gross_per_unit_quote > 0, "remaining_edge_opposes_original_side")
    _need(gross_per_unit_quote >= 2*cost_per_unit_quote, "expected_gross_below_cost_hurdle")
    # An extra half of the current spread is retained for a terminal executable
    # quote. Future spread/FX conversion are assumptions, not observed outcomes.
    net_per_unit_quote = gross_per_unit_quote-cost_per_unit_quote
    net_per_unit_usd = net_per_unit_quote * (gain if net_per_unit_quote >= 0 else loss)
    stop_unrounded = bid-2*atr if side == 1 else ask+2*atr
    stop = (stop_unrounded/tick).to_integral_value(rounding=ROUND_FLOOR if side == 1 else ROUND_CEILING)*tick
    _need(stop > 0 and (bid-stop if side == 1 else stop-ask) > 0, "protective_stop_invalid")
    risk_per_unit = (abs(entry_bound-stop)+slip)*loss
    margin_per_unit = max(ask, entry_bound)*position_value*max(rate, account_rate)
    _need(risk_per_unit > 0 and margin_per_unit > 0, "unit_economics_invalid")
    risk_budget = nav*Decimal("0.005")
    # Remaining session drawdown budget also caps a new modeled stopped loss.
    session_remaining = Decimal(guard["session_loss_limit_usd"])-max(Decimal(0), Decimal(guard["drawdown_usd"]))
    risk_budget = min(risk_budget, session_remaining)
    margin_budget = min(nav*Decimal("0.20")-margin_used, margin_available)
    _need(risk_budget > 0 and margin_budget > 0, "account_risk_or_margin_capacity_unavailable")
    bounds = [risk_budget/risk_per_unit, margin_budget/margin_per_unit, order_max]
    if position_max > 0: bounds.append(position_max)  # OANDA0 denotes no instrument limit.
    units = (min(bounds)/unit_increment).to_integral_value(rounding=ROUND_FLOOR)*unit_increment
    _need(units >= minimum and units > 0, "risk_sized_units_below_broker_minimum")
    return dict(status="available", action="entry_candidate", reason="all_policy_gates_passed",
                instrument=instrument, side=side, units=format(units*side, "f"),
                stop_loss_price=format(stop, f".{metadata['displayPrecision']}f"),
                entry_price_bound=format(entry_bound, "f"), original_target_epoch=signal["original_target_epoch"],
                forecast_sha256=signal["forecast_sha256"], expected_net_usd=str(units*net_per_unit_usd),
                expected_gross_usd=str(units*gross_per_unit_quote*gain),
                modeled_round_trip_cost_usd=str(units*cost_per_unit_quote*gain),
                loss_side_cost_stress_usd=str(units*cost_per_unit_quote*loss),
                entry_tick_rounding_extra_price=str(rounding_slip),
                conversion_scope="Positive net quote PnL converts once with accountGain; gross/cost breakdown uses the same rate. Loss-side stress and stop budget use accountLoss; margin uses positionValue.",
                modeled_stop_loss_usd=str(units*risk_per_unit), risk_budget_usd=str(risk_budget),
                modeled_new_margin_usd=str(units*margin_per_unit), margin_budget_usd=str(margin_budget),
                effective_margin_rate=str(max(rate, account_rate)), atr_price=str(atr), atr_last_close_epoch=atr_close,
                spread_bps=str(spread/mid*10000), pip_size=str(pip), side_probability=str(side_probability),
                signed_currency_directions={base:side, quote_currency:-side}, loss_stop_latched=False,
                account_session_sha256=hashlib.sha256(json.dumps(dict(account=account, session=session), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
                economics_scope="Expected quote spread/slippage and current home conversions are assumptions; stop gaps, financing, impact and future FX changes are not bounded by this modeled loss.")


def _result(body, input_hash, decision_epoch):
    body = dict(body, schema_version=SCHEMA, policy=json.loads(_POLICY_JSON), inputs_sha256=input_hash,
                information_cutoff_epoch=decision_epoch, broker_action_performed=False,
                requires_execution_revalidation=True, performance_verified=False,
                computation_clock_scope="Pure calculation; caller records actual completion/publication separately.")
    body["assessment_sha256"] = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return body


def _public(fn, payload, decision_epoch):
    input_hash = None
    try:
        owned, input_hash = _owned(payload)
        with localcontext(CTX):
            decision = _epoch(decision_epoch, "decision")
            return _result(fn(owned, decision), input_hash, decision_epoch)
    except (PolicyError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
        reason = str(exc) if isinstance(exc, PolicyError) else "malformed_policy_input"
        # No amount, account identifier or raw exception payload is exposed.
        safe_decision = decision_epoch if type(decision_epoch) in (int, float) and math.isfinite(decision_epoch) else None
        return _result(dict(status="refused", action="none", reason=reason), input_hash, safe_decision)


def session_guard(account, session, decision_epoch):
    """Runner must persist loss_stop_latched with logical OR; never reset it."""
    return _public(lambda x, d: _guard(x["account"], x["session"], d),
                   dict(account=account, session=session), decision_epoch)


def evaluate_entry(signal, quote, account, metadata, home_conversions, candles, *, session, decision_epoch):
    """Return risk-sized entry candidate or explicit refusal; original signal is not mutated."""
    return _public(lambda x, d: _entry(x["signal"], x["quote"], x["account"], x["metadata"],
                                     x["home_conversions"], x["candles"], x["session"], d),
                   dict(signal=signal, quote=quote, account=account, metadata=metadata,
                        home_conversions=home_conversions, candles=candles, session=session), decision_epoch)


def choose_best(assessments):
    """Rank authenticated caller assessments by same-policy expected net USD.

    Seals detect accidental alteration, not origin/authenticity. The runner owns
    trusted source validation and single account-wide contemporaneous selection.
    """
    try:
        rows, _ = _owned(assessments)
        _need(type(rows) is list and len(rows) <= 68, "assessment_inventory_bound")
        eligible = []; seen = set(); cutoff = None; account_context = None
        with localcontext(CTX):
            for row in rows:
                _required(row, ("assessment_sha256", "status", "schema_version", "policy", "information_cutoff_epoch"), "assessment")
                seal = row.pop("assessment_sha256")
                _need(seal == hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest(), "assessment_seal_mismatch")
                row["assessment_sha256"] = seal
                _need(row["schema_version"] == SCHEMA and row["policy"] == json.loads(_POLICY_JSON), "assessment_policy_mismatch")
                d = _epoch(row["information_cutoff_epoch"], "assessment_cutoff")
                if cutoff is None: cutoff = d
                _need(cutoff == d, "mixed_assessment_clocks")
                if row["status"] != "available": continue
                _sha(row.get("account_session_sha256"), "account_session")
                if account_context is None: account_context = row["account_session_sha256"]
                _need(account_context == row["account_session_sha256"], "mixed_account_session_contexts")
                pair = row["instrument"]; _pair(pair); _need(pair not in seen, "duplicate_instrument_assessment"); seen.add(pair)
                value = row["expected_net_usd"]
                _need(type(value) is str and len(value) <= 192, "expected_net_usd_decimal")
                edge = Decimal(value)
                _need(edge.is_finite() and 0 < edge < Decimal("1e14"), "expected_net_usd_bound")
                eligible.append((edge, pair, row["forecast_sha256"], row))
            if not eligible: return dict(status="refused", action="none", reason="no_available_entry_candidate")
            eligible.sort(key=lambda x: (-x[0], x[1], x[2]))
            return deepcopy(eligible[0][3])
    except (PolicyError, KeyError, TypeError, ValueError, ArithmeticError) as exc:
        return dict(status="refused", action="none", reason=str(exc) if isinstance(exc, PolicyError) else "malformed_assessment")


def _manage(position, quote, account, session, decision):
    _required(position, ("trade_id", "instrument", "currentUnits", "price", "stop_loss_price",
                        "original_target_epoch", "opened_epoch", "observed_epoch", "forecast_sha256"), "position")
    _need(type(position["trade_id"]) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,100}", position["trade_id"]), "position_identity")
    instrument = position["instrument"]; _pair(instrument); _sha(position["forecast_sha256"], "position_forecast")
    observed = _fresh(position["observed_epoch"], decision, 30, "position_observation")
    opened = _epoch(position["opened_epoch"], "position_opened")
    target = _epoch(position["original_target_epoch"], "position_target")
    _need(opened <= observed and opened < target, "position_clock_order")
    units = _number(position["currentUnits"], "position_units", signed=True)
    _need(units != 0, "position_not_confirmed_open")
    entry = _number(position["price"], "position_entry")
    side = 1 if units > 0 else -1
    common = dict(status="available", trade_id=position["trade_id"], instrument=instrument,
                  original_target_epoch=position["original_target_epoch"], add_units="0")
    _, stop_epoch, _ = _session(session, decision)
    # Mandatory deadlines do not depend on a usable quote or account valuation.
    if decision >= min(target, stop_epoch):
        return dict(common, action="close", reason="original_target_or_session_deadline", loss_stop_latched=session["loss_stop_latched"])
    if session["loss_stop_latched"]:
        return dict(common, action="close", reason="session_loss_stop", loss_stop_latched=True)
    if position["stop_loss_price"] is None:
        return dict(common, action="close", reason="protective_stop_missing", loss_stop_latched=False)
    try:
        stop = _number(position["stop_loss_price"], "existing_stop")
    except PolicyError:
        return dict(common, action="close", reason="protective_stop_invalid", loss_stop_latched=False)
    guard = _guard(account, session, decision)
    if guard["status"] == "halt":
        return dict(common, action="close", reason=guard["reason"], loss_stop_latched=guard["loss_stop_latched"])
    try:
        bid, ask, _ = _quote(quote, instrument, decision)
    except PolicyError as exc:
        return dict(common, action="wait_unknown", reason=str(exc), loss_stop_latched=False,
                    stop_loss_price=position["stop_loss_price"], existing_position_preserved=True)
    exit_quote = bid if side == 1 else ask
    if (exit_quote-stop)*side <= 0:
        return dict(common, action="close", reason="protective_stop_crossed", loss_stop_latched=False)
    return dict(common, action="hold", reason="fixed_original_target_and_protective_stop", loss_stop_latched=False,
                stop_loss_price=position["stop_loss_price"], current_quote_pnl_per_base_unit=str((exit_quote-entry)*side))


def manage_position(position, quote, account, *, session, decision_epoch):
    """Exact observed position only; no add, target extension, flip or trailing."""
    return _public(lambda x, d: _manage(x["position"], x["quote"], x["account"], x["session"], d),
                   dict(position=position, quote=quote, account=account, session=session), decision_epoch)
