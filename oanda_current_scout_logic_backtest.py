#!/usr/bin/env python3
"""Backtest the current OANDA scout-entry logic on historical research data.

This is read-only.  It replays the live scout's rule layer against the
historical technical_spike_research.parquet feature frame:

- recent observed move proxy = momentum_{5,15,30}_atr * atr240_pips
- current scout thresholds from oanda_spike_scout_account_manager.py
- production model segment/probability gate from technical_production.json
- scout model-disagreement override for drastic/large volatile moves
- path-aware exit metrics from the precomputed trailing/curve outcome columns

It is not a tick-level OANDA fill simulator.  It is a strategy-logic replay for
deciding whether the scout logic deserves live-tech allocation.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
DATASET = ROOT / "data" / "oanda_training_manager" / "continuous_research" / "technical_spike_research.parquet"
MANIFEST = ROOT / "data" / "oanda_training_manager" / "promotions" / "technical_production.json"
REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "latest_current_scout_logic_backtest.json"
CSV_REPORT = ROOT / "data" / "oanda_training_manager" / "reports" / "latest_current_scout_logic_backtest_trades.csv"

MAJOR_CURRENCIES = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
VOLATILE_SCOUT_PAIRS = {
    "CHF_ZAR", "EUR_ZAR", "GBP_ZAR", "USD_ZAR",
    "EUR_TRY", "USD_TRY",
    "USD_CZK", "EUR_CZK",
    "USD_NOK", "EUR_NOK",
    "USD_SEK", "EUR_SEK",
    "USD_MXN", "EUR_MXN",
    "HKD_JPY",
    "CHF_HKD", "GBP_HKD", "EUR_HKD", "CAD_HKD", "AUD_HKD", "USD_HKD",
}

# Dedicated spike_scout overrides from build_scout_config().
SCOUT_CFG = {
    "event_min_major_net_pips": 5.0,
    "event_min_cross_net_pips": 8.0,
    "event_min_exotic_net_pips": 35.0,
    "event_min_move_to_spread_ratio": 2.0,
    "event_max_spread_pips_major": 5.0,
    "event_max_spread_pips_cross": 10.0,
    "event_max_spread_pips_exotic": 260.0,
    "scout_volatile_min_net_pips": 40.0,
    "scout_volatile_max_spread_pips": 260.0,
    "scout_volatile_min_move_to_spread_ratio": 1.4,
    "scout_volatile_min_ev_score_to_trade": 52.0,
    "scout_volatile_base_risk_pct": 0.20,
    "scout_volatile_max_risk_pct": 0.60,
    "scout_min_ev_score_to_trade": 60.0,
    "event_scout_risk_pct": 0.25,
    "max_risk_pct_per_trade": 0.75,
    "drastic_move_threshold_multiplier": 2.0,
    "drastic_move_min_ratio": 0.75,
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        number = float(value)
        return number if math.isfinite(number) else default
    except Exception:
        return default


def normalize_instrument(value: Any) -> str:
    text = str(value or "").strip().upper().replace("/", "_").replace("-", "_")
    if "_" not in text and len(text) == 6:
        return text[:3] + "_" + text[3:]
    return text


def split_instrument(instrument: str) -> tuple[str, str]:
    inst = normalize_instrument(instrument)
    if "_" in inst:
        return tuple(inst.split("_", 1))  # type: ignore[return-value]
    return inst[:3], inst[3:]


def event_pair_kind(instrument: str) -> str:
    inst = normalize_instrument(instrument)
    if inst in VOLATILE_SCOUT_PAIRS:
        return "volatile"
    base, quote = split_instrument(inst)
    if base in MAJOR_CURRENCIES and quote in MAJOR_CURRENCIES:
        return "major"
    if base in MAJOR_CURRENCIES or quote in MAJOR_CURRENCIES:
        return "cross"
    return "exotic"


def threshold_pips(kind: str) -> float:
    if kind == "volatile":
        return SCOUT_CFG["scout_volatile_min_net_pips"]
    if kind == "major":
        return SCOUT_CFG["event_min_major_net_pips"]
    if kind == "cross":
        return SCOUT_CFG["event_min_cross_net_pips"]
    return SCOUT_CFG["event_min_exotic_net_pips"]


def max_spread_pips(kind: str) -> float:
    if kind == "volatile":
        return SCOUT_CFG["scout_volatile_max_spread_pips"]
    if kind == "major":
        return SCOUT_CFG["event_max_spread_pips_major"]
    if kind == "cross":
        return SCOUT_CFG["event_max_spread_pips_cross"]
    return SCOUT_CFG["event_max_spread_pips_exotic"]


def min_ratio(kind: str) -> float:
    if kind == "volatile":
        return SCOUT_CFG["scout_volatile_min_move_to_spread_ratio"]
    return SCOUT_CFG["event_min_move_to_spread_ratio"]


def clamp(value: Any, lo: float, hi: float) -> float:
    return max(lo, min(hi, safe_float(value, lo)))


def scout_ev_score(kind: str, net_pips: pd.Series, ratio: pd.Series, drastic: pd.Series) -> pd.Series:
    """Approximate live scout_ev_score_and_risk with age=0, basket=1, unknown profile."""
    if kind == "volatile":
        move_score = ((ratio - SCOUT_CFG["scout_volatile_min_move_to_spread_ratio"]) / 5.0 * 30.0).clip(0.0, 30.0)
        net_score = (net_pips / max(SCOUT_CFG["scout_volatile_min_net_pips"], 1.0) * 22.0).clip(0.0, 28.0)
        basket_score = 12.0
        early_score = 18.0
        profile_score = 2.0
        return (move_score + net_score + basket_score + early_score + profile_score + drastic.astype(float) * 8.0).clip(0.0, 100.0)
    move_score = ((ratio - SCOUT_CFG["event_min_move_to_spread_ratio"]) / 4.0 * 25.0).clip(0.0, 25.0)
    thresh = net_pips * 0.0 + 1.0
    net_score = (net_pips / thresh * 20.0).clip(0.0, 25.0)
    basket_score = 10.0
    early_score = 20.0
    profile_score = 3.0
    return (move_score + net_score + basket_score + early_score + profile_score + drastic.astype(float) * 8.0).clip(0.0, 100.0)


def session_name(frame: pd.DataFrame) -> pd.Series:
    out = pd.Series("off_session", index=frame.index, dtype="object")
    if "is_asia_session" in frame:
        out = out.mask(pd.to_numeric(frame["is_asia_session"], errors="coerce").fillna(0.0) >= 0.5, "asia")
    if "is_london_session" in frame:
        out = out.mask(pd.to_numeric(frame["is_london_session"], errors="coerce").fillna(0.0) >= 0.5, "london")
    if "is_new_york_session" in frame:
        out = out.mask(pd.to_numeric(frame["is_new_york_session"], errors="coerce").fillna(0.0) >= 0.5, "new_york")
    if "is_london_ny_overlap" in frame:
        out = out.mask(pd.to_numeric(frame["is_london_ny_overlap"], errors="coerce").fillna(0.0) >= 0.5, "london_ny_overlap")
    return out


def summarize(values: Iterable[float]) -> Dict[str, Any]:
    vals = np.asarray([float(v) for v in values if math.isfinite(float(v))], dtype=float)
    if vals.size == 0:
        return {"count": 0}
    wins = vals[vals > 0]
    losses = -vals[vals < 0]
    equity = vals.cumsum()
    running_peak = np.maximum.accumulate(equity)
    drawdown = running_peak - equity
    return {
        "count": int(vals.size),
        "sum": float(vals.sum()),
        "mean": float(vals.mean()),
        "median": float(np.median(vals)),
        "positive_count": int((vals > 0).sum()),
        "positive_rate": float((vals > 0).mean()),
        "min": float(vals.min()),
        "max": float(vals.max()),
        "profit_factor": float(wins.sum() / max(losses.sum(), 1e-9)),
        "max_drawdown_pips": float(drawdown.max()) if drawdown.size else 0.0,
    }


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    os.replace(tmp, path)


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def read_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


@dataclass
class ModelGate:
    manifest: Dict[str, Any]
    model: Any = None
    features: Sequence[str] = ()
    threshold: float = 0.75
    active: bool = False


def load_model_gate(manifest_path: Path) -> ModelGate:
    manifest = read_manifest(manifest_path)
    artifact = Path(str(manifest.get("model_artifact_path", "")))
    active = bool(
        manifest.get("activation_effective", False)
        or (
            manifest.get("technical_account_activation", False)
            and str(manifest.get("stage", "")).lower() in {"production", "technical_production"}
        )
    )
    model = None
    features = list(manifest.get("features") or [])
    if active and artifact.exists():
        bundle = joblib.load(artifact)
        if isinstance(bundle, dict):
            model = bundle.get("model")
            features = list(bundle.get("features") or features)
    threshold = safe_float(
        ((manifest.get("validation") or {}).get("selected_threshold") or {}).get("threshold"),
        0.75,
    )
    return ModelGate(manifest=manifest, model=model, features=features, threshold=threshold, active=bool(active and model is not None and features))


def parquet_columns(path: Path) -> List[str]:
    return list(pq.ParquetFile(path).schema.names)


def build_required_columns(model_features: Sequence[str]) -> List[str]:
    base = {
        "time_utc", "instrument", "atr240_pips", "spread_pips",
        "momentum_5_atr", "momentum_15_atr", "momentum_30_atr",
        "pair_taxonomy_primary", "regime_primary",
        "is_asia_session", "is_london_session", "is_new_york_session", "is_london_ny_overlap",
        "macro_pair_bias", "macro_rate_diff_bias", "macro_news_risk", "macro_event_risk",
    }
    for horizon in (30, 60, 120):
        for side in ("long", "short"):
            base.add(f"{side}_trailing_net_pips_{horizon}")
            base.add(f"{side}_fixed_net_pips_{horizon}")
            base.add(f"{side}_curve_net_pips_{horizon}")
            base.add(f"{side}_curve_best_net_pips_{horizon}")
            base.add(f"{side}_curve_mae_pips_{horizon}")
    base.update(model_features)
    return sorted(base)


def apply_signal_rules(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["instrument"] = out["instrument"].map(normalize_instrument)
    out["pair_kind"] = out["instrument"].map(event_pair_kind)
    out["threshold_pips"] = out["pair_kind"].map(threshold_pips).astype(float)
    out["max_spread_pips"] = out["pair_kind"].map(max_spread_pips).astype(float)
    out["min_ratio"] = out["pair_kind"].map(min_ratio).astype(float)
    out["atr240_pips"] = pd.to_numeric(out["atr240_pips"], errors="coerce").fillna(0.0)
    out["spread_pips"] = pd.to_numeric(out["spread_pips"], errors="coerce").fillna(999999.0)

    move_cols = []
    for minutes in (5, 15, 30):
        col = f"move_{minutes}_pips"
        out[col] = pd.to_numeric(out.get(f"momentum_{minutes}_atr", 0.0), errors="coerce").fillna(0.0) * out["atr240_pips"]
        move_cols.append(col)
    moves = out[move_cols].to_numpy(dtype=float)
    best_idx = np.nanargmax(np.abs(moves), axis=1)
    best_moves = moves[np.arange(len(out)), best_idx]
    windows = np.asarray([5, 15, 30], dtype=int)[best_idx]
    out["window_minutes"] = windows
    out["observed_move_pips"] = best_moves
    out["net_pips"] = np.abs(best_moves)
    out["direction"] = np.where(best_moves >= 0, "LONG", "SHORT")
    out["move_to_spread_ratio"] = out["net_pips"] / out["spread_pips"].clip(lower=0.1)
    out["cost_adjusted_net_pips"] = out["net_pips"] - out["spread_pips"]
    out["drastic_override"] = (
        (out["net_pips"] >= out["threshold_pips"] * SCOUT_CFG["drastic_move_threshold_multiplier"])
        & (out["move_to_spread_ratio"] >= SCOUT_CFG["drastic_move_min_ratio"])
    )
    out["rule_basic_pass"] = (
        (out["spread_pips"] <= out["max_spread_pips"])
        & (out["net_pips"] >= out["threshold_pips"])
        & ((out["move_to_spread_ratio"] >= out["min_ratio"]) | out["drastic_override"])
    )

    scores = []
    for kind, part in out.groupby("pair_kind", sort=False):
        scores.append(pd.Series(scout_ev_score(kind, part["net_pips"], part["move_to_spread_ratio"], part["drastic_override"]), index=part.index))
    out["scout_ev_score"] = pd.concat(scores).sort_index() if scores else pd.Series(dtype=float)
    out["rule_pass"] = out["rule_basic_pass"] & (
        ((out["pair_kind"] == "volatile") & (out["scout_ev_score"] >= SCOUT_CFG["scout_volatile_min_ev_score_to_trade"]))
        | ((out["pair_kind"] != "volatile") & (out["scout_ev_score"] >= SCOUT_CFG["scout_min_ev_score_to_trade"]))
    )
    out["session"] = session_name(out)
    return out


def apply_segment_and_macro(frame: pd.DataFrame, manifest: Dict[str, Any]) -> pd.DataFrame:
    out = frame.copy()
    allowed_segments = manifest.get("allowed_segments") if isinstance(manifest.get("allowed_segments"), dict) else {}

    def allowed_set(key: str) -> set[str]:
        raw = manifest.get(key) or (allowed_segments or {}).get(key) or []
        if isinstance(raw, str):
            raw = [raw]
        return {str(x) for x in raw if str(x)}

    allowed_families = allowed_set("allowed_pair_families")
    allowed_regimes = allowed_set("allowed_regimes")
    allowed_sessions = allowed_set("allowed_sessions")
    out["segment_pair_ok"] = True if not allowed_families else out["pair_taxonomy_primary"].astype(str).isin(allowed_families)
    out["segment_regime_ok"] = True if not allowed_regimes else out["regime_primary"].astype(str).isin(allowed_regimes)
    out["segment_session_ok"] = True if not allowed_sessions else out["session"].astype(str).isin(allowed_sessions)
    out["segment_ok"] = out["segment_pair_ok"] & out["segment_regime_ok"] & out["segment_session_ok"]

    news_risk = pd.to_numeric(out.get("macro_news_risk", 0.0), errors="coerce").fillna(0.0)
    event_risk = pd.to_numeric(out.get("macro_event_risk", 0.0), errors="coerce").fillna(0.0)
    macro_pair = pd.to_numeric(out.get("macro_pair_bias", 0.0), errors="coerce").fillna(0.0)
    rate_bias = pd.to_numeric(out.get("macro_rate_diff_bias", 0.0), errors="coerce").fillna(0.0)
    macro_bias = (macro_pair + 0.5 * rate_bias).clip(-1.0, 1.0)
    contra = 0.60
    out["macro_ok"] = (
        (news_risk < 0.85)
        & (event_risk < 0.85)
        & ~((out["direction"] == "LONG") & (macro_bias <= -contra))
        & ~((out["direction"] == "SHORT") & (macro_bias >= contra))
    )
    out["macro_combined_bias"] = macro_bias
    return out


def apply_model_gate(frame: pd.DataFrame, gate: ModelGate) -> pd.DataFrame:
    out = frame.copy()
    out["model_probability"] = np.nan
    out["model_ok"] = False
    if not gate.active:
        return out
    missing = [feature for feature in gate.features if feature not in out.columns]
    if missing:
        out["model_missing_features"] = ",".join(missing[:10])
        return out
    candidates = out["rule_pass"].fillna(False)
    if candidates.sum() <= 0:
        return out
    x = out.loc[candidates, list(gate.features)].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    try:
        probs = gate.model.predict_proba(x)[:, 1]
    except Exception:
        probs = gate.model.predict_proba(x.to_numpy(dtype=float))[:, 1]
    out.loc[candidates, "model_probability"] = probs
    out.loc[candidates, "model_ok"] = probs >= gate.threshold
    return out


def apply_outcomes(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for horizon in (30, 60, 120):
        for metric in ("trailing_net_pips", "fixed_net_pips", "curve_net_pips", "curve_best_net_pips", "curve_mae_pips"):
            long_col = f"long_{metric}_{horizon}"
            short_col = f"short_{metric}_{horizon}"
            dest = f"{metric}_{horizon}"
            long_vals = pd.to_numeric(out.get(long_col, np.nan), errors="coerce")
            short_vals = pd.to_numeric(out.get(short_col, np.nan), errors="coerce")
            out[dest] = np.where(out["direction"] == "LONG", long_vals, short_vals)
    return out


def apply_override(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    volatile = out["pair_kind"] == "volatile"
    threshold = out["threshold_pips"]
    net = out["net_pips"]
    ratio = out["move_to_spread_ratio"]
    cost = out["cost_adjusted_net_pips"]
    drastic = out["drastic_override"]
    out["scout_override_ok"] = (
        (volatile & drastic & (cost > 0.0) & (ratio >= 0.95) & (net >= threshold * 1.75))
        | (volatile & (cost >= 10.0) & (ratio >= 1.25) & (net >= np.maximum(threshold * 1.25, 50.0)))
        | ((~volatile) & drastic & (cost > 0.0) & (ratio >= 2.4))
    )
    return out


def dedupe_trades(frame: pd.DataFrame, mask: pd.Series, gap_minutes: int) -> pd.DataFrame:
    subset = frame.loc[mask].sort_values(["time_utc", "instrument", "direction"]).copy()
    if subset.empty:
        return subset
    kept = []
    last_seen: Dict[tuple[str, str], pd.Timestamp] = {}
    gap = pd.Timedelta(minutes=max(0, int(gap_minutes)))
    for idx, row in subset.iterrows():
        key = (str(row["instrument"]), str(row["direction"]))
        t = pd.Timestamp(row["time_utc"])
        last = last_seen.get(key)
        if last is not None and t - last < gap:
            continue
        kept.append(idx)
        last_seen[key] = t
    return subset.loc[kept].copy()


def summarize_variant(name: str, trades: pd.DataFrame, metric: str) -> Dict[str, Any]:
    if trades.empty:
        return {"variant": name, "trades": 0}
    trades = trades.sort_values("time_utc")
    out = {
        "variant": name,
        "trades": int(len(trades)),
        "metric": metric,
        "overall": summarize(trades[metric].dropna()),
        "unique_pairs": int(trades["instrument"].nunique()),
    }
    weeks = trades.assign(week=pd.to_datetime(trades["time_utc"], utc=True).dt.strftime("%G-W%V")).groupby("week")[metric].sum()
    out["weeks"] = int(len(weeks))
    out["positive_weeks"] = int((weeks > 0).sum())
    out["positive_week_rate"] = float((weeks > 0).mean()) if len(weeks) else 0.0
    out["worst_weeks"] = [{"week": str(k), "sum_pips": float(v)} for k, v in weeks.sort_values().head(5).items()]
    out["best_weeks"] = [{"week": str(k), "sum_pips": float(v)} for k, v in weeks.sort_values(ascending=False).head(5).items()]

    def grouped(field: str, limit: int = 15) -> List[Dict[str, Any]]:
        rows = []
        for key, part in trades.groupby(field):
            vals = pd.to_numeric(part[metric], errors="coerce").dropna()
            if len(vals) < 1:
                continue
            rows.append({
                field: str(key),
                "trades": int(len(vals)),
                "sum": float(vals.sum()),
                "mean": float(vals.mean()),
                "win_rate": float((vals > 0).mean()),
                "profit_factor": summarize(vals).get("profit_factor", 0.0),
            })
        rows.sort(key=lambda x: x["sum"], reverse=True)
        return rows[:limit]

    out["by_instrument_top"] = grouped("instrument", 20)
    out["by_instrument_bottom"] = sorted(grouped("instrument", 999), key=lambda x: x["sum"])[:20]
    out["by_pair_family"] = grouped("pair_taxonomy_primary", 20)
    out["by_regime"] = grouped("regime_primary", 20)
    out["by_session"] = grouped("session", 20)
    return out


def summarize_variant_multi(name: str, trades: pd.DataFrame, metrics: Sequence[str], primary_metric: str) -> Dict[str, Any]:
    if trades.empty:
        return {"variant": name, "trades": 0}
    out = summarize_variant(name, trades, primary_metric)
    out["metrics"] = {}
    for metric in metrics:
        if metric in trades.columns:
            out["metrics"][metric] = summarize(pd.to_numeric(trades[metric], errors="coerce").dropna())
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest current scout logic over historical technical data.")
    parser.add_argument("--dataset", type=Path, default=DATASET)
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--report", type=Path, default=REPORT)
    parser.add_argument("--csv-report", type=Path, default=CSV_REPORT)
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--cluster-gap-minutes", type=int, default=60)
    parser.add_argument("--metric", default="trailing_net_pips_120")
    parser.add_argument(
        "--extra-metrics",
        nargs="*",
        default=[
            "trailing_net_pips_30",
            "trailing_net_pips_60",
            "trailing_net_pips_120",
            "curve_net_pips_60",
            "curve_net_pips_120",
            "curve_best_net_pips_120",
            "fixed_net_pips_120",
        ],
    )
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument("--max-csv-rows", type=int, default=20000)
    args = parser.parse_args()

    gate = load_model_gate(args.manifest)
    available_cols = set(parquet_columns(args.dataset))
    required = [c for c in build_required_columns(gate.features) if c in available_cols]
    pf = pq.ParquetFile(args.dataset)
    since = pd.Timestamp(args.since, tz="UTC") if args.since else None
    until = pd.Timestamp(args.until, tz="UTC") if args.until else None
    frames = []
    rows_read = 0
    candidates_before_rules = 0
    for rg in range(pf.num_row_groups):
        table = pf.read_row_group(rg, columns=required)
        df = table.to_pandas()
        rows_read += len(df)
        if args.max_rows and rows_read > args.max_rows:
            extra = rows_read - args.max_rows
            if extra > 0:
                df = df.iloc[:-extra] if extra < len(df) else df.iloc[:0]
            rows_read = args.max_rows
        if df.empty:
            continue
        df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True, errors="coerce")
        if since is not None:
            df = df[df["time_utc"] >= since]
        if until is not None:
            df = df[df["time_utc"] <= until]
        if df.empty:
            continue
        df = apply_signal_rules(df)
        candidates_before_rules += int(df["rule_basic_pass"].sum())
        df = df[df["rule_pass"]].copy()
        if df.empty:
            if args.max_rows and rows_read >= args.max_rows:
                break
            continue
        df = apply_segment_and_macro(df, gate.manifest)
        df = apply_model_gate(df, gate)
        df = apply_override(df)
        df = apply_outcomes(df)
        frames.append(df)
        if args.max_rows and rows_read >= args.max_rows:
            break

    all_candidates = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    variants: Dict[str, pd.Series] = {}
    if not all_candidates.empty:
        variants = {
            "raw_rule": all_candidates["rule_pass"],
            "segment_gate": all_candidates["rule_pass"] & all_candidates["segment_ok"],
            "model_gate": all_candidates["rule_pass"] & all_candidates["segment_ok"] & all_candidates["macro_ok"] & all_candidates["model_ok"],
            "model_or_scout_override": all_candidates["rule_pass"] & all_candidates["segment_ok"] & all_candidates["macro_ok"] & (all_candidates["model_ok"] | all_candidates["scout_override_ok"]),
            "high_score_hybrid": all_candidates["rule_pass"] & all_candidates["segment_ok"] & all_candidates["macro_ok"] & (all_candidates["model_ok"] | all_candidates["scout_override_ok"]) & (all_candidates["scout_ev_score"] >= 70.0),
        }

    summaries = {}
    detail_rows: List[Dict[str, Any]] = []
    metrics = list(dict.fromkeys([args.metric, *args.extra_metrics]))
    for name, mask in variants.items():
        trades = dedupe_trades(all_candidates, mask.fillna(False), args.cluster_gap_minutes)
        summaries[name] = summarize_variant_multi(name, trades, metrics, args.metric)
        if len(detail_rows) < args.max_csv_rows and not trades.empty:
            keep = trades.sort_values("time_utc").head(max(0, args.max_csv_rows - len(detail_rows))).copy()
            keep["variant"] = name
            cols = [
                "variant", "time_utc", "instrument", "direction", "window_minutes", "net_pips",
                "spread_pips", "move_to_spread_ratio", "scout_ev_score", "drastic_override",
                "pair_kind", "pair_taxonomy_primary", "regime_primary", "session",
                "segment_ok", "macro_ok", "model_probability", "model_ok", "scout_override_ok",
                "trailing_net_pips_30", "trailing_net_pips_60", "trailing_net_pips_120",
                "curve_net_pips_120", "fixed_net_pips_120", "curve_best_net_pips_120",
            ]
            detail_rows.extend(keep[[c for c in cols if c in keep.columns]].to_dict("records"))

    payload = {
        "generated_utc": utc_iso(),
        "dataset": str(args.dataset),
        "manifest": str(args.manifest),
        "rows_read": int(rows_read),
        "raw_rule_basic_candidates_before_ev": int(candidates_before_rules),
        "rule_pass_candidates": int(len(all_candidates)),
        "cluster_gap_minutes": args.cluster_gap_minutes,
        "metric": args.metric,
        "model_gate": {
            "active": gate.active,
            "experiment_id": gate.manifest.get("experiment_id", ""),
            "candidate_id": gate.manifest.get("candidate_id", ""),
            "model_type": gate.manifest.get("model_type", ""),
            "target": gate.manifest.get("target", ""),
            "threshold": gate.threshold,
            "feature_count": len(gate.features),
            "allowed_pair_families": gate.manifest.get("allowed_pair_families", []),
            "allowed_regimes": gate.manifest.get("allowed_regimes", []),
            "allowed_sessions": gate.manifest.get("allowed_sessions", []),
        },
        "assumptions": {
            "observed_move": "max abs(momentum_5/15/30_atr * atr240_pips); 1m/3m/10m windows are not available in this parquet",
            "entry": "one de-duplicated entry per instrument/direction per cluster_gap_minutes",
            "exit_metric": args.metric,
            "not_included": "broker order-bound cancellation, financing, margin liquidation, exact tick-level stop/TP ordering",
            "purpose": "read-only live-tech scout logic replay, not financial advice",
        },
        "summaries": summaries,
    }
    atomic_write_json(args.report, payload)
    write_csv(args.csv_report, detail_rows)
    print(json.dumps({
        "report": str(args.report),
        "csv_report": str(args.csv_report),
        "rows_read": rows_read,
        "rule_pass_candidates": len(all_candidates),
        "variants": {k: v.get("overall", v) for k, v in summaries.items()},
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
