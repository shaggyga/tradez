"""Isolate existing rich feature calculations from trading and model runtimes.

Only the named, hash-verified calculator definitions are compiled. No source
module imports, initializers, account clients, model loads or fits run here.
The existing causal MA implementation retains its original feature names.
"""
from __future__ import annotations

import argparse
import ast
from functools import lru_cache
import hashlib
import math
from pathlib import Path
import re
import statistics
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Iterable

import oanda_ma_causal_features_v2 as ma

ROOT = Path(__file__).resolve().parent
SCHEMA = "research_existing_feature_calculators_v1_20260913"
LAB_SHA256 = "2529e1e1b4a45f8bd5838b2a32fd128cedb0c116c50f5ab10b0122cd27be4bb0"
LAB_FUNCTIONS = frozenset(("candle_values", "local_atr_pips", "resampled_series", "resampled_closes", "aggregate_complete_candles", "build_timeframe_feature_views", "candle_spreads_pips", "recent_ratio", "parsed_complete_closes", "aggregate_close_series", "build_ma_series_by_timeframe", "build_features", "augment_market_microstructure_features", "_normalized_pair_return", "_breadth", "_percentile_rank", "augment_cross_sectional_features"))
HELPER_FUNCTIONS = frozenset(("safe_float", "normalize_instrument", "quote_currency", "infer_pip_size", "parse_rfc3339", "candle_closes"))
TIMEFRAMES = ("M1", "M5", "M10", "M15", "M30", "H1", "H2", "H3", "H4")
STRUCTURAL_NAMES = ("volume_ratio_12", "volume_ratio_30", "current_volume", "current_candle_spread_pips", "median_spread_12_pips", "spread_ratio_12", "spread_drop_3_pips", "last", "r1_pips", "r3_pips", "r5_pips", "m5_r1_pips", "m5_r3_pips", "pos20", "m1_atr14_pips", "m5_atr14_pips")


def _slice(path, expected, names, constants=()):
    raw = path.read_bytes()
    if len(raw) > 2 * 1024 * 1024 or hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("reviewed_calculator_source_changed:" + path.name)
    nodes, found = [], set()
    for node in ast.parse(raw.decode("utf-8-sig")).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            if node.decorator_list or any(isinstance(child, (ast.Import, ast.ImportFrom, ast.With, ast.AsyncWith, ast.Global, ast.Nonlocal)) for child in ast.walk(node)):
                raise ValueError("calculator_slice_contains_noncalculator_construct")
            nodes.append(node)
            found.add(node.name)
        elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id in constants:
            ast.literal_eval(node.value)
            nodes.append(node)
            found.add(node.targets[0].id)
    if found != set(names) | set(constants):
        raise ValueError("reviewed_calculator_slice_missing")
    return nodes


@lru_cache(maxsize=1)
def calculators():
    helper_path = ROOT / "oanda_practice_eurusd_micro_scalper.py"
    helper_expected = "9600a35209d6ed6f177b6a535908dca4c8a97f211e6c1875ed3cdbc35329d79f"
    nodes = _slice(helper_path, helper_expected, HELPER_FUNCTIONS)
    nodes += _slice(ROOT / "oanda_practice_shadow_strategy_lab.py", LAB_SHA256, LAB_FUNCTIONS, ("MICROSTRUCTURE_SCALAR_KEYS",))
    namespace = dict(argparse=argparse, math=math, re=re, statistics=statistics, time=time, datetime=datetime, timezone=timezone, Any=Any, Iterable=Iterable)
    module = ast.Module(body=[ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<verified-existing-feature-calculators>", "exec"), namespace)
    return namespace


@lru_cache(maxsize=1)
def ma_names():
    return tuple(ma._compiled(ma._source())["ma_feature_names"]("M1"))


def source_identity():
    calculators()
    return {"calculator_schema": SCHEMA, "lab_source_sha256": LAB_SHA256, "scalar_helper_source_sha256": hashlib.sha256((ROOT / "oanda_practice_eurusd_micro_scalper.py").read_bytes()).hexdigest(), "calculator_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "ma_causal_source_sha256": hashlib.sha256(Path(ma.__file__).read_bytes()).hexdigest(), "ma_original_source_sha256": ma.ORIGINAL_SOURCE_SHA256, "ma_feature_version": ma.FEATURE_VERSION}


def calculate_pair(instrument, candle_sets, pip):
    """Reuse identical resamples only inside this one immutable input call."""
    ns = calculators()
    original = ns["resampled_series"]
    cache = {}
    def reused(candles, target_minutes, source_minutes=5):
        key = (id(candles), target_minutes, source_minutes)
        if key not in cache:
            cache[key] = original(candles, target_minutes, source_minutes=source_minutes)
        return cache[key]
    ns["resampled_series"] = reused
    try:
        return _calculate_pair(instrument, candle_sets, pip)
    finally:
        ns["resampled_series"] = original


def _calculate_pair(instrument, candle_sets, pip):
    """Run existing calculators on explicitly supplied, validated source rows."""
    ns = calculators()
    primary, reason = ns["build_features"](instrument, candle_sets, pip, include_ma_grid=False)
    if primary is None:
        m1 = candle_sets.get("M1") or []
        primary = {"instrument": instrument, "pip": pip, "candle_time": str(m1[-1].get("time") or "") if m1 else "", **{name: None for name in STRUCTURAL_NAMES}}
    views = ns["build_timeframe_feature_views"](instrument, candle_sets, pip, primary_features=primary if not reason else None)
    # M1's rich MA vector is materialized exactly once. The other timeframe
    # groups retain existing structural fields; no synthetic 643-wide copies.
    closes = [row["mid"]["c"] for row in candle_sets.get("M1", [])]
    rich_reason = ""
    try:
        rich = ma.build_feature_vector(closes, pip, "M1")
    except ValueError as exc:
        rich = {name: None for name in ma_names()}
        rich_reason = str(exc)
    primary.update(rich)
    coverage = {"structural_status": "available" if not reason else "unavailable", "structural_reason": reason or None, "rich_ma_status": "available" if not rich_reason else "unavailable", "rich_ma_reason": rich_reason or None, "rich_ma_feature_count": len(rich), "rich_ma_native_timeframes": ["M1"], "rich_ma_other_timeframes": "not_materialized_in_this_bounded_observer", "ma_initialization_rows": len(closes), "ma_history_policy": "all_explicitly_supplied_bounded_contiguous_source_rows", "retained_model_weight_parity_claimed": False}
    return primary, views, coverage


def augment_observed_caches(primary, views, quotes, metadata, *, eligible_cross_pairs=None):
    ns = calculators()
    quote_objects = {pair: SimpleNamespace(**row) for pair, row in quotes.items()}
    ns["augment_market_microstructure_features"](primary, quote_objects, metadata)
    selected = primary if eligible_cross_pairs is None else {pair: values for pair, values in primary.items() if pair in eligible_cross_pairs.get("M1", set())}
    ns["augment_cross_sectional_features"](selected)
    for timeframe in TIMEFRAMES:
        scoped = {pair: groups[timeframe] for pair, groups in views.items() if timeframe in groups}
        ns["augment_market_microstructure_features"](scoped, quote_objects, metadata)
        selected = scoped if eligible_cross_pairs is None else {pair: values for pair, values in scoped.items() if pair in eligible_cross_pairs.get(timeframe, set())}
        ns["augment_cross_sectional_features"](selected)
