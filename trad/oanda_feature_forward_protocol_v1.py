"""Pure, preregistered exploratory feature-magnitude and subsequent quote scoring.

No feature sign is a forecast direction. Long/short are fixed symmetric probes.
The caller owns input and publication clocks; this module performs no I/O.
"""
from __future__ import annotations

from collections import Counter
from decimal import Context, Decimal, localcontext
from datetime import datetime
import hashlib
import json
import math
import re

try:
    import oanda_exact_price_scoring as exact
except ModuleNotFoundError:
    from trad import oanda_exact_price_scoring as exact

VERSION = "feature_forward_exploratory_v1_20260913"
MAX_ROWS = 100000
MAX_BATCH_BYTES = 64 * 1024 * 1024
PAIR = re.compile(r"^[A-Z]{3}_[A-Z]{3}$")
PROTOCOL = {
    "schema_version": VERSION, "decision_cadence_sec": 300,
    "comparison_windows_sec": [300, 900, 3600], "horizons_sec": [300, 900, 3600],
    "magnitude_alert_percentile": 95, "minimum_history_count": 5,
    "requires_nonzero_absolute_change": True, "price_move_threshold_bps": "5",
    "source_max_age_sec": 75, "quote_market_max_age_sec": 30,
    "entry_max_delay_sec": 60, "target_max_delay_sec": 60,
    "cost_stress_bps": ["1", "2"],
    "target_clock": "immutable_event_publication_plus_horizon",
    "outcome_move": "absolute_frozen_prepublication_reference_midpoint_to_target_midpoint",
    "directions": "fixed_long_and_short_probes_no_feature_sign_prediction",
    "quote_persistence": "forced_prepublication_references_and_first_eligible_unresolved_entry_or_target_only",
    "quote_observation_cadence_sec": 5,
    "other_valid_quote_reads": "count_observed_not_needed_without_persisting_quote_or_rewriting_past_evidence",
    "probability": None, "thresholds_are_exploratory": True,
    "can_place_orders": False, "can_promote": False,
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


PROTOCOL_SHA256 = digest(PROTOCOL)


def epoch(value):
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value < 8e9:
        raise ValueError("finite_positive_clock_required")
    return float(value)


def instrument(value):
    if type(value) is not str or not PAIR.fullmatch(value) or value[:3] == value[4:]:
        raise ValueError("instrument_required")
    return value


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def utc_epoch(value):
    if type(value) is not str:
        raise ValueError("explicit_timezone_clock_required")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("explicit_timezone_clock_required")
    return epoch(stamp.timestamp())


def classify(row):
    """All rows survive. Only observed, ranked numeric indicators are eligible."""
    if not isinstance(row, dict):
        raise ValueError("comparison_object_required")
    if row.get("feature_kind") not in ("indicator", "observed_feature", "microstructure"):
        return "excluded", "not_observed_numeric_feature"
    if row.get("status") != "available":
        return "excluded", str(row.get("reason") or row.get("status") or "unavailable")[:160]
    if not all(number(row.get(key)) for key in ("before", "after", "change")):
        return "excluded", "non_numeric_or_nonfinite"
    count = row.get("history_count")
    if type(count) is not int or count < PROTOCOL["minimum_history_count"]:
        return "excluded", "insufficient_history"
    percentile = row.get("unusual_percentile")
    if not number(percentile) or not 0 <= percentile <= 100:
        return "excluded", str(row.get("ranking_reason") or "unranked")[:160]
    alert = percentile >= PROTOCOL["magnitude_alert_percentile"] and abs(row["change"]) > 0
    return ("alert", None) if alert else ("control", None)


def prepare_comparisons(mapping, *, window_sec, observed_epoch):
    """Consume the explicit complete mapper lane, never its display selection."""
    now = epoch(observed_epoch)
    if type(window_sec) is not int or window_sec not in PROTOCOL["comparison_windows_sec"]:
        raise ValueError("comparison_window_required")
    if not isinstance(mapping, dict) or mapping.get("schema_version") != "feature_move_mapping_v1":
        raise ValueError("mapping_schema_required")
    if type(mapping.get("window_sec")) is not int or mapping["window_sec"] != window_sec:
        raise ValueError("mapping_window_mismatch")
    if mapping.get("instrument_filter") is not None:
        raise ValueError("filtered_population_refused")
    rows = mapping.get("all_feature_changes")
    total = (mapping.get("summary") or {}).get("total_feature_comparisons")
    # Empty waiting frames are still a durable cadence/window observation.
    if mapping.get("status") in ("waiting_for_observations", "stale_observations", "observation_validation_failed"):
        if (rows is None or rows == []) and total is None:
            rows, total = [], 0
    if not isinstance(rows, list) or len(rows) > MAX_ROWS or type(total) is not int or total != len(rows):
        raise ValueError("complete_untruncated_population_required")
    pairs = mapping.get("pairs")
    if not isinstance(pairs, list) or len(pairs) > 68:
        raise ValueError("bounded_pair_inventory_required")
    names = [instrument(row["instrument"]) for row in pairs]
    if len(names) != len(set(names)):
        raise ValueError("duplicate_pair")
    latest = mapping.get("latest_observed_utc")
    current = utc_epoch(latest) if latest is not None else None
    if current is not None and current > now:
        raise ValueError("future_mapping_source")
    stale = current is None or now-current > PROTOCOL["source_max_age_sec"]
    compact, seen, counts = [], set(), Counter()
    pair_counts = {pair: {"total": 0, "alert": 0, "control": 0, "excluded": 0} for pair in names}
    for row in rows:
        pair = instrument(row.get("instrument"))
        feature = row.get("feature_id")
        if pair not in names or type(feature) is not str or not 1 <= len(feature) <= 1024:
            raise ValueError("feature_identity_or_pair_inventory")
        key = (pair, feature)
        if key in seen:
            raise ValueError("duplicate_feature_identity")
        seen.add(key)
        category, reason = classify(row)
        if stale:
            category, reason = "excluded", "current_source_missing_or_stale"
        # Related-pair display links repeat O(pairs) per field and are not input
        # to this protocol. Every original comparison field is otherwise kept.
        retained = {key: value for key, value in row.items() if key != "related_pair_moves"}
        retained.update(forward_category=category, forward_exclusion_reason=reason)
        compact.append(retained)
        counts[category] += 1
        pair_counts[pair]["total"] += 1
        pair_counts[pair][category] += 1
    result = {
        "schema_version": VERSION, "protocol_sha256": PROTOCOL_SHA256,
        "window_sec": window_sec, "mapping_status": mapping.get("status"),
        "source_snapshots": mapping.get("source_snapshots", []),
        "latest_observed_utc": mapping.get("latest_observed_utc"),
        "mapping_generated_utc": mapping.get("generated_utc"),
        "archive": mapping.get("archive"), "coverage": mapping.get("summary"),
        "pairs": pairs, "comparisons": compact, "category_counts": dict(counts),
        "pair_category_counts": pair_counts,
        "decision_bucket": int(now // PROTOCOL["decision_cadence_sec"]),
    }
    if len(canonical(result)) > MAX_BATCH_BYTES:
        raise ValueError("comparison_batch_byte_bound")
    return result


def event_id(batch_id, pair, feature_id, horizon_sec):
    if type(horizon_sec) is not int or horizon_sec not in PROTOCOL["horizons_sec"]:
        raise ValueError("horizon_required")
    return digest([PROTOCOL_SHA256, batch_id, instrument(pair), feature_id, horizon_sec])


def price_quote(value):
    if not isinstance(value, dict) or value.get("tradeable") is not True:
        raise ValueError("explicit_tradeable_quote_required")
    result = {}
    for key in ("bid", "ask"):
        raw = value.get(key)
        if type(raw) not in (str, int, float) or len(str(raw)) > 128:
            raise ValueError("bounded_price_required")
        dec = exact.decimal_value(raw, key)
        if not Decimal("1e-12") <= dec <= Decimal("1e12") or abs(dec.as_tuple().exponent) > 64:
            raise ValueError("price_domain")
        result[key] = str(dec)
    exact.quote_midpoint(result)
    return result


def score_pair(reference, entry, target):
    """Bid/ask hypothetical probes, not fills, account P/L or fitted forecasts."""
    reference, entry, target = price_quote(reference), price_quote(entry), price_quote(target)
    keep = ("direction", "direction_correct", "actual_return_bps", "outcome_class",
            "net_price_move", "net_bps", "positive_after_spread", "stress_net_bps",
            "entry_spread_price", "target_spread_price")
    probes = {}
    for name, direction in (("fixed_long", 1), ("fixed_short", -1)):
        raw = exact.score_prediction(reference, target, direction=direction,
                                     probability_up="0.5", entry_quote=entry,
                                     extra_cost_stress_bps=PROTOCOL["cost_stress_bps"])
        # Exact scorer requires probability. Its placeholder and every derived
        # probability/Brier field are deliberately absent from this contract.
        probes[name] = exact.to_jsonable({key: raw[key] for key in keep})
    with localcontext(Context(prec=4096)):
        first, last = exact.quote_midpoint(reference), exact.quote_midpoint(target)
        large = abs(last - first) * 10000 >= Decimal(PROTOCOL["price_move_threshold_bps"]) * first
    return {"scoring_version": exact.SCORING_VERSION, "price_move_at_least_threshold": large,
            "price_move_threshold_bps": PROTOCOL["price_move_threshold_bps"], "probes": probes,
            "forecast_probability": None, "inferred_feature_direction": None,
            "account_return": None, "can_place_orders": False}


def confusion(category, score):
    if category not in ("alert", "control"):
        return "excluded"
    large = score["price_move_at_least_threshold"]
    if type(large) is not bool:
        raise ValueError("typed_outcome_required")
    return ("hit" if large else "false_alert") if category == "alert" else ("missed_move" if large else "quiet_control")
