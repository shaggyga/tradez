"""Pure, untrained EURUSD reopening hypotheses; never a trading engine.

All inputs are caller-supplied snapshots. Observation and calendar provenance
are assertions, not independent proof; no historical forecast is reconstructed.
The first observed tradeable opening quote is the immutable reference, not a
claim to have captured the broker's first tick. One event per weekend must be
enforced by any future, separately reviewed collector.

Prices require strings, integers or Decimal (floats are rejected). Clocks accept
finite epoch numbers or decimal strings; float clocks preserve only str(float).
Returned Decimals serialize losslessly with the imported to_jsonable helper.
No files, databases, processes, network, models or runtime workers are opened.
"""
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
import hashlib
import json
import re
from types import MappingProxyType

from oanda_exact_price_scoring import decimal_value, quote_midpoint, to_jsonable


MODEL_VERSION = "eurusd_weekend_reopening_hypotheses_v1_20260907"
PARAMETERS = MappingProxyType({
    "instrument": "EUR_USD",
    "horizon_sec": 3600,
    "initial_decision_window_sec": 300,
    "maximum_quote_age_sec": 60,
    "maximum_anchor_age_before_close_sec": 300,
    "maximum_opening_spread_pips": "2",
    "minimum_absolute_gap_opening_spreads_exclusive": "2",
    "pip_size": "0.0001",
    "minimum_closure_sec": 86400,
    "maximum_closure_sec": 345600,
    "session_calendar_rule": "explicit_UTC_Friday_close_to_Sunday_or_Monday_reopen",
    "arms": ("gap_fade", "gap_continuation", "no_trade"),
})
_HASH = re.compile(r"^[0-9a-f]{64}$")


def _hash(value):
    return hashlib.sha256(json.dumps(to_jsonable(value), sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _text(value, name):
    if type(value) is not str or not value.strip() or len(value) > 256:
        raise ValueError(name + ": required bounded nonempty string")
    return value


def _clock(value, name):
    result = decimal_value(value, name)
    if not 0 < result < 32503680000:
        raise ValueError(name + ": invalid epoch")
    return result


def _price(value, name):
    if type(value) not in (str, int, Decimal):
        raise ValueError(name + ": exact decimal price required; floats rejected")
    return decimal_value(value, name)


def _record(value, name):
    if not isinstance(value, Mapping):
        raise ValueError(name + ": mapping required")
    if value.get("instrument") != "EUR_USD":
        raise ValueError(name + ": EUR_USD required")
    bid, ask = _price(value.get("bid"), name + ".bid"), _price(value.get("ask"), name + ".ask")
    if not 0 < bid <= ask:
        raise ValueError(name + ": require 0 < bid <= ask")
    source_hash = value.get("source_sha256")
    if type(source_hash) is not str or not _HASH.fullmatch(source_hash):
        raise ValueError(name + ": source_sha256 required")
    return {"instrument": "EUR_USD", "bid": bid, "ask": ask,
            "market_epoch": _clock(value.get("market_epoch"), name + ".market_epoch"),
            "observed_epoch": _clock(value.get("observed_epoch"), name + ".observed_epoch"),
            "source_id": _text(value.get("source_id"), name + ".source_id"),
            "source_sha256": source_hash}


def _session(value):
    if not isinstance(value, Mapping) or value.get("instrument") != "EUR_USD":
        raise ValueError("session: EUR_USD calendar mapping required")
    source_hash = value.get("calendar_source_sha256")
    if type(source_hash) is not str or not _HASH.fullmatch(source_hash):
        raise ValueError("session: calendar_source_sha256 required")
    result = {"session_id": _text(value.get("session_id"), "session.session_id"),
        "instrument": "EUR_USD",
        "friday_close_epoch": _clock(value.get("friday_close_epoch"), "session.friday_close_epoch"),
        "reopen_epoch": _clock(value.get("reopen_epoch"), "session.reopen_epoch"),
        "calendar_observed_epoch": _clock(value.get("calendar_observed_epoch"), "session.calendar_observed_epoch"),
        "calendar_source_id": _text(value.get("calendar_source_id"), "session.calendar_source_id"),
        "calendar_source_sha256": source_hash}
    close, reopen = result["friday_close_epoch"], result["reopen_epoch"]
    if (datetime.fromtimestamp(int(close), timezone.utc).weekday() != 4
        or datetime.fromtimestamp(int(reopen), timezone.utc).weekday() not in (6, 0)
        or not 86400 <= reopen - close <= 345600):
        raise ValueError("session: unsupported weekend calendar")
    return result


def forecast_weekend_reopening(friday_anchor, opening_quote, *, session, decision_epoch):
    """Return fixed fade/continuation/no-trade hypotheses or explicit abstention.

    session requires EUR_USD, session_id, friday_close_epoch, reopen_epoch,
    calendar_observed_epoch, calendar_source_id and calendar_source_sha256.
    The calendar must have been observed before reopening and before decision.

    Both price records require EUR_USD, market_epoch, observed_epoch, bid, ask,
    source_id and source_sha256. friday_anchor additionally requires
    is_final_preclose_anchor=True and anchor_kind='quote' or 'completed_m1'.
    A completed_m1 anchor needs complete=True and bar_start_epoch exactly 60
    seconds before market_epoch (the close boundary, not candle start).

    opening_quote additionally requires is_first_observed_tradeable_quote=True,
    tradeable=True and retained_last_known=False. Missing reference provenance
    abstains. Actual observation clocks of both prices must precede decision.
    The original target is opening market_epoch + 3600. No late target shifts,
    backdated issuance, history filling, fitted probabilities or tuning occur.
    Parameters are fixed research choices, not validated execution limits.
    """
    # Rebuild the public parameter mapping so callers cannot alter the policy
    # through a previous result. Arithmetic below uses frozen literals too.
    parameters = dict(PARAMETERS, arms=["gap_fade", "gap_continuation", "no_trade"])
    result = {"schema_version": MODEL_VERSION, "model_version": MODEL_VERSION,
        "status": "abstain", "reasons": [], "parameters": parameters,
        "parameters_sha256": _hash(parameters), "event_id": None,
        "forecast_batch_id": None, "input_capture_sha256": None,
        "decision_epoch": None, "reference_epoch": None, "target_epoch": None,
        "gap_price": None, "opening_spread_price": None, "friday_mid": None,
        "opening_mid": None, "input_snapshot": None,
        "arms": {name: {"direction": 0, "status": "abstain",
                        "probability_up": None, "expected_return_bps": None}
                 for name in ("gap_fade", "gap_continuation", "no_trade")},
        "research_only": True, "collection_enabled": False, "can_place_orders": False,
        "can_authorize": False, "can_promote": False, "account_eligible": False,
        "proof_eligible": False, "runtime_started": False,
        "historical_accuracy_established": False, "independent_sample_size": None,
        "availability_scope": "caller_asserted_actual_observations_not_independent_attestation",
        "limitations": [
            "Untrained fixed hypotheses; no demonstrated prediction accuracy or execution approval.",
            "The first observed opening quote need not be the broker's first quote.",
            "Caller must prove the session and final Friday anchor; current capture cannot prove past availability.",
            "Caller must enforce one event per weekend and publish before any independent executable entry quote.",
            "One-hour target stays tied to the original opening market timestamp, even after delays.",
            "Offline retrospective results are not prospective forecasts; no current registration is changed.",
        ]}
    try:
        with localcontext(Context(prec=4096, rounding=ROUND_HALF_EVEN)):
            decision = _clock(decision_epoch, "decision_epoch")
            result["decision_epoch"] = decision
            calendar = _session(session)
            if not (calendar["calendar_observed_epoch"] < calendar["reopen_epoch"]
                    and calendar["calendar_observed_epoch"] < decision):
                raise ValueError("calendar_not_known_before_reopening_and_decision")
            if not isinstance(opening_quote, Mapping) or opening_quote.get("is_first_observed_tradeable_quote") is not True:
                raise ValueError("unknown_opening_reference")
            opening = _record(opening_quote, "opening_quote")
            result.update(reference_epoch=opening["market_epoch"], target_epoch=opening["market_epoch"] + 3600)
            if opening_quote.get("tradeable") is not True or opening_quote.get("retained_last_known") is not False:
                raise ValueError("opening_quote_not_current_tradeable")
            anchor = _record(friday_anchor, "friday_anchor")
            if friday_anchor.get("is_final_preclose_anchor") is not True:
                raise ValueError("unknown_final_friday_anchor")
            kind = friday_anchor.get("anchor_kind")
            if kind not in ("quote", "completed_m1"):
                raise ValueError("unknown_friday_anchor_kind")
            anchor.update(anchor_kind=kind, is_final_preclose_anchor=True)
            if kind == "quote":
                if friday_anchor.get("tradeable") is not True or friday_anchor.get("retained_last_known") is not False:
                    raise ValueError("friday_anchor_not_current_tradeable_quote")
                anchor.update(tradeable=True, retained_last_known=False)
            if kind == "completed_m1":
                start = _clock(friday_anchor.get("bar_start_epoch"), "friday_anchor.bar_start_epoch")
                if friday_anchor.get("complete") is not True or start % 60 != 0 or start + 60 != anchor["market_epoch"]:
                    raise ValueError("friday_anchor_not_completed_m1_close")
                anchor.update(bar_start_epoch=start, complete=True)
            opening.update(tradeable=True, retained_last_known=False, is_first_observed_tradeable_quote=True)
            if any(not q["market_epoch"] <= q["observed_epoch"] < decision for q in (anchor, opening)):
                raise ValueError("price_observation_not_strictly_before_decision")
            if not 0 <= calendar["friday_close_epoch"] - anchor["market_epoch"] <= 300:
                raise ValueError("friday_anchor_outside_close_window")
            if not calendar["reopen_epoch"] <= opening["market_epoch"] <= calendar["reopen_epoch"] + 300:
                raise ValueError("opening_reference_outside_initial_window")
            if decision >= result["target_epoch"]:
                raise ValueError("original_target_elapsed")
            if decision > calendar["reopen_epoch"] + 300:
                raise ValueError("late_initial_decision")
            if decision - opening["market_epoch"] > 60:
                raise ValueError("opening_quote_stale_at_decision")
            mid = quote_midpoint(opening)
            friday_mid = quote_midpoint(anchor)
            gap, spread = mid - friday_mid, opening["ask"] - opening["bid"]
            snapshot = {"session": calendar, "friday_anchor": anchor, "opening_quote": opening,
                        "decision_epoch": decision}
            event = {"instrument": "EUR_USD", "friday_close_epoch": calendar["friday_close_epoch"],
                     "reopen_epoch": calendar["reopen_epoch"]}
            # Normalize numerically equal clock spellings in the event identity.
            event = {key: format(value.normalize(), "f") if isinstance(value, Decimal) else value
                     for key, value in event.items()}
            result.update(gap_price=gap, opening_spread_price=spread, friday_mid=friday_mid,
                opening_mid=mid, input_snapshot=snapshot, event_id=_hash(event),
                input_capture_sha256=_hash(snapshot))
            result["forecast_batch_id"] = _hash({"model_version": MODEL_VERSION,
                "parameters_sha256": result["parameters_sha256"], "input_capture_sha256": result["input_capture_sha256"]})
            if spread > Decimal("2") * Decimal("0.0001"):
                raise ValueError("opening_spread_exceeds_fixed_limit")
            if abs(gap) <= 2 * spread:
                raise ValueError("gap_not_above_two_opening_spreads")
            sign = 1 if gap > 0 else -1
            for name, side in (("gap_fade", -sign), ("gap_continuation", sign), ("no_trade", 0)):
                result["arms"][name].update(direction=side, status="hypothesis")
            result["status"] = "hypotheses"
    except (ValueError, TypeError, OverflowError, OSError) as exc:
        result["reasons"].append(str(exc))
    return result
