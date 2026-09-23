"""Exact-clock currency peer context, separate from the pair-local kernel.

Conceptual lineage: D_unified_forecast.py:add_cross_sectional_features and
oanda_practice_shadow_strategy_lab.py:augment_cross_sectional_features.
This version averages signed *bps* returns from other pairs, excluding the
target pair. These describe observed peer movements, not currency forecasts.
There are no as-of substitutions, ATR normalizations or missing-horizon
fallbacks. A currency leg needs at least two distinct contributing peer pairs.
"""
from __future__ import annotations

import hashlib
import json
import math
from numbers import Real
import re
from typing import Mapping, Any

SCHEMA_VERSION = "rolling_m1_currency_peers_v1_20260915"
HORIZONS = (1, 5, 15, 60)
MIN_PEERS = 2
MAX_PAIRS = 256
PAIR = re.compile(r"[A-Z]{3}_[A-Z]{3}")
SOURCE_ORIGIN = "D_unified_forecast.py:add_cross_sectional_features; oanda_practice_shadow_strategy_lab.py:augment_cross_sectional_features (conceptual lineage; revised peer-only bps means)"


def _epoch(value) -> int | None:
    if isinstance(value,bool) or not isinstance(value,Real):
        return None
    if not math.isfinite(value) or value < 0 or value > 253402300799 or value % 60:
        return None
    return int(value)


def _number(value) -> float | None:
    if isinstance(value,bool) or not isinstance(value,Real) or not math.isfinite(value):
        return None
    return float(value)


def panel_registry() -> list[dict[str, Any]]:
    result = []
    for horizon in HORIZONS:
        for component in ("base_mean","quote_mean","diff"):
            result.append({"name":f"peer__{component}_{horizon}_bps","family":"currency_peer",
                "unit":"bps","horizon_minutes":horizon,"lookback_bars":horizon+1,
                "source_feature":f"m1__return_{horizon}_bps","source_origin":SOURCE_ORIGIN,
                "formula": "base signed peer mean minus quote signed peer mean" if component == "diff" else f"mean(other pairs' signed {horizon}m bps return for target {component.split('_')[0]} currency)",
                "clock":"exact target M1 bar START epoch equality; completed-bar inputs only",
                "minimum_distinct_peer_pairs_per_leg":MIN_PEERS,
                "self_pair_excluded":True,"missing_policy":"None; no imputation or horizon substitution",
                "model_input":True,"aliases":[]})
    return result


def _canonical(value) -> bytes:
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode("utf-8")


def panel_metadata() -> dict[str, Any]:
    return {"schema_version":SCHEMA_VERSION,"feature_count":len(panel_registry()),
        "registry_sha256":hashlib.sha256(_canonical(panel_registry())).hexdigest(),
        "minimum_peer_pairs_per_currency":MIN_PEERS,"max_input_pairs":MAX_PAIRS,
        "clock":"exact equal original M1 bar START; no as-of/future rows",
        "self_pair_excluded":True,"source_returns":"explicit matching horizon bps columns",
        "interpretation":"observed equal-weight signed peer movement, not a latent currency price or model forecast",
        "can_place_orders":False}


def compute_panel(rows: Mapping[str, Mapping[str, Any]], target_epoch: int) -> dict[str, Any]:
    """Compute peer context and deterministic provenance for every input pair.

    Input: ``{pair: {bar_start_epoch: original_start, values: {...}}}``.
    ``target_epoch`` is an explicit completed candle start, never wall time.
    Wrong-clock/malformed rows remain in the result with unavailable values.
    Nonfinite or missing returns exclude that source from that horizon only.
    The caller records the actual later publication time; this module does
    not pretend the panel was known at candle close or mutate pair features.
    """
    target = _epoch(target_epoch)
    if target is None:
        raise ValueError("explicit_aligned_target_minute_required")
    if not isinstance(rows,Mapping) or len(rows) > MAX_PAIRS:
        raise ValueError("bounded_pair_mapping_required")
    if any(not isinstance(pair,str) or not PAIR.fullmatch(pair) or pair[:3] == pair[4:] for pair in rows):
        raise ValueError("valid_distinct_currency_pair_keys_required")
    accepted, exclusions, rejection_by_pair = {}, [], {}
    for pair,row in sorted(rows.items()):
        reason = None
        if not isinstance(row,Mapping):
            reason = "malformed_source_row"
        else:
            clock = _epoch(row.get("bar_start_epoch"))
            if clock is None:
                reason = "missing_or_invalid_original_bar_start"
            elif clock > target:
                reason = "future_bar_start"
            elif clock < target:
                reason = "asynchronous_bar_start"
            elif not isinstance(row.get("values"),Mapping):
                reason = "missing_values_mapping"
        if reason:
            rejection_by_pair[pair] = reason
            exclusions.append({"pair":pair,"reason":reason})
        else:
            accepted[pair] = {h:_number(row["values"].get(f"m1__return_{h}_bps")) for h in HORIZONS}
    feature_names = [r["name"] for r in panel_registry()]
    by_pair = {}
    for pair in sorted(rows):
        base, quote = pair.split("_")
        values = {name:None for name in feature_names}
        support = {}
        for horizon in HORIZONS:
            legs = {}
            for side,currency in (("base",base),("quote",quote)):
                constituents = []
                missing_return_peers = []
                if pair in accepted:
                    for other,returns in accepted.items():
                        if other == pair:
                            continue
                        other_base,other_quote = other.split("_")
                        sign = 1 if other_base == currency else -1 if other_quote == currency else 0
                        if not sign:
                            continue
                        value = returns[horizon]
                        if value is None:
                            missing_return_peers.append(other)
                            continue
                        constituents.append({"pair":other,"sign":sign,"raw_return_bps":value,"signed_return_bps":sign*value})
                count = len(constituents)
                ready = count >= MIN_PEERS
                try:
                    mean = math.fsum(item["signed_return_bps"]/count for item in constituents) if ready else None
                except OverflowError:
                    mean,ready = None,False
                # Reject arithmetic overflow rather than emitting invalid JSON.
                if mean is not None and not math.isfinite(mean):
                    mean,ready = None,False
                values[f"peer__{side}_mean_{horizon}_bps"] = mean
                legs[side] = {"currency":currency,"count":count,"ready":ready,
                    "constituents":constituents,"missing_horizon_peers":missing_return_peers}
            left,right = values[f"peer__base_mean_{horizon}_bps"],values[f"peer__quote_mean_{horizon}_bps"]
            if left is not None and right is not None:
                difference = left-right
                values[f"peer__diff_{horizon}_bps"] = difference if math.isfinite(difference) else None
            support[str(horizon)] = legs
        finite_count = sum(v is not None for v in values.values())
        status = "available" if finite_count == len(values) else "partial" if finite_count else "unavailable"
        record = {"status":status,"reason":rejection_by_pair.get(pair),"values":values,"support":support,
            "finite_feature_count":finite_count,"source_clock_policy":"exact_equal_completed_bar_start",
            "bar_start_epoch":target,"pair":pair,"schema_version":SCHEMA_VERSION}
        record["provenance_sha256"] = hashlib.sha256(_canonical(record)).hexdigest()
        by_pair[pair] = record
    result = {"schema_version":SCHEMA_VERSION,"target_bar_start_epoch":target,"feature_names":feature_names,
        "by_pair":by_pair,"excluded_sources":exclusions,"accepted_clock_pair_count":len(accepted),
        "minimum_peer_pairs_per_currency":MIN_PEERS,"can_place_orders":False}
    result["provenance_sha256"] = hashlib.sha256(_canonical(result)).hexdigest()
    return result
