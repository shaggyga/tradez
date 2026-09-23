#!/usr/bin/env python3
"""Event study for missed volatile-pair scout moves.

This is a read-only diagnostic/report generator.  It uses the live technical
manager logs to answer: what moved, what the bot saw before/during the move,
which simple late-entry triggers would have fired, and how payoff scales on a
small live scout account.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


PROJECT_ROOT = Path(os.environ.get("TRAD_PROJECT_ROOT", str(Path(__file__).resolve().parent)))
TECH_ROOT = PROJECT_ROOT / "data" / "technical_scout_manager" / "account_live_tech_broad_regime_scout"
PRIMARY_ROOT = PROJECT_ROOT / "data" / "technical_scout_manager" / "account_live_primary_challenger_scout"
REPORT_ROOT = PROJECT_ROOT / "data" / "technical_scout_manager" / "reports"

PAIR_INFO = {
    # Current OANDA live instrument metadata read on 2026-06-24.  Used only for
    # approximate payoff/margin calculations in the report.
    "EUR_TRY": {"pip_size": 0.0001, "margin_rate": 0.25, "mid": 52.772205, "usd_per_quote": 1 / 46.497335},
    "USD_TRY": {"pip_size": 0.0001, "margin_rate": 0.25, "mid": 46.497335, "usd_per_quote": 1 / 46.497335},
    "USD_ZAR": {"pip_size": 0.0001, "margin_rate": 0.07, "mid": 16.570305, "usd_per_quote": 1 / 16.570305},
    "GBP_ZAR": {"pip_size": 0.0001, "margin_rate": 0.07, "mid": 21.81385, "usd_per_quote": 1 / 16.570305},
    "CHF_ZAR": {"pip_size": 0.0001, "margin_rate": 0.07, "mid": 20.39122, "usd_per_quote": 1 / 16.570305},
    "EUR_ZAR": {"pip_size": 0.0001, "margin_rate": 0.07, "mid": 18.94, "usd_per_quote": 1 / 16.570305},
    "USD_MXN": {"pip_size": 0.0001, "margin_rate": 0.10, "mid": 17.62475, "usd_per_quote": 1 / 17.62475},
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, engine="python", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame()


def parse_json(value: Any) -> Dict[str, Any]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        obj = json.loads(value)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def parse_time(value: Any) -> Optional[pd.Timestamp]:
    try:
        ts = pd.to_datetime(value, errors="coerce", utc=True)
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def accepted_keys_since(cutoff: pd.Timestamp) -> Tuple[set[Tuple[str, str]], List[Dict[str, Any]]]:
    roots = {"tech": TECH_ROOT, "primary": PRIMARY_ROOT}
    gpt_root = PROJECT_ROOT / "data" / "forex_gpt_manager" / "account_gpt_prod_live"
    roots["gpt"] = gpt_root
    accepted: List[Dict[str, Any]] = []
    for account, root in roots.items():
        for name in ["actions.csv", "order_result_ledger.csv", "trade_lifecycle_ledger.csv"]:
            df = read_csv(root / name)
            if df.empty or "time_utc" not in df:
                continue
            df["dt"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
            df = df[df["dt"] >= cutoff]
            for _, row in df.iterrows():
                status = str(row.get("status", "")).lower()
                action = str(row.get("action", row.get("action_type", ""))).lower()
                source = str(row.get("source", "")).lower()
                if "accepted" not in status:
                    continue
                if not (
                    action in {"open", "event_scout", "pressure_trade"}
                    or source == "open"
                    or action.startswith("event")
                ):
                    continue
                accepted.append({
                    "account": account,
                    "time_utc": row.get("time_utc", ""),
                    "instrument": row.get("instrument", ""),
                    "direction": row.get("direction", ""),
                    "units": row.get("units", ""),
                    "price": row.get("fill_price", row.get("price", "")),
                    "reason": str(row.get("reason", ""))[:240],
                })
    return {
        (str(item["instrument"]), str(item["direction"]))
        for item in accepted
        if item.get("instrument") and item.get("direction")
    }, accepted


def load_event_windows(cutoff: pd.Timestamp) -> pd.DataFrame:
    frames = []
    for source, root in [("tech", TECH_ROOT), ("primary", PRIMARY_ROOT)]:
        df = read_csv(root / "event_windows.csv")
        if df.empty or "time_utc" not in df:
            continue
        df["source"] = source
        df["dt"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        df = df[df["dt"] >= cutoff].copy()
        for col in ["net_pips", "mid_move_pips", "spread_avg_pips", "move_to_spread_ratio", "window_minutes"]:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def load_campaign_rows(cutoff: pd.Timestamp) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for source, root in [("tech", TECH_ROOT), ("primary", PRIMARY_ROOT)]:
        df = read_csv(root / "movement_campaigns.csv")
        if df.empty or "time_utc" not in df:
            continue
        df["dt"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        df = df[df["dt"] >= cutoff]
        for _, row in df.iterrows():
            raw = parse_json(row.get("raw_json", ""))
            signal = raw.get("signal") if isinstance(raw.get("signal"), dict) else {}
            if not signal and isinstance(raw.get("signals"), list) and raw["signals"]:
                signal = raw["signals"][0] if isinstance(raw["signals"][0], dict) else {}
            snapshot = signal.get("_technical_feature_snapshot", {}) if isinstance(signal, dict) else {}
            rows.append({
                "source": source,
                "time_utc": row.get("time_utc", ""),
                "instrument": row.get("instrument", signal.get("instrument", "")),
                "direction": row.get("direction", signal.get("direction", "")),
                "theme": row.get("theme", signal.get("theme", "")),
                "status": row.get("campaign_status", row.get("status", "")),
                "action": row.get("campaign_action", ""),
                "reason": row.get("reason", ""),
                "last_net_pips": safe_float(row.get("last_net_pips", signal.get("net_pips", 0.0))),
                "last_ratio": safe_float(row.get("last_ratio", signal.get("move_to_spread_ratio", 0.0))),
                "window_minutes": safe_int(signal.get("window_minutes", 0)),
                "start_utc": signal.get("start_utc", ""),
                "end_utc": signal.get("end_utc", ""),
                "event_age_minutes": safe_float(signal.get("event_age_minutes", 0.0)),
                "promoted_probability": safe_float(
                    (signal.get("_promoted_model_decision") or {}).get("probability", 0.0)
                    if isinstance(signal.get("_promoted_model_decision"), dict)
                    else 0.0
                ),
                "promoted_threshold": safe_float(
                    (signal.get("_promoted_model_decision") or {}).get("threshold", 0.0)
                    if isinstance(signal.get("_promoted_model_decision"), dict)
                    else 0.0
                ),
                "ensemble_probability": safe_float(
                    (signal.get("_ensemble_shadow_decision") or {}).get("probability", 0.0)
                    if isinstance(signal.get("_ensemble_shadow_decision"), dict)
                    else 0.0
                ),
                "snapshot": snapshot if isinstance(snapshot, dict) else {},
            })
    return rows


def classify_snapshot(snapshot: Dict[str, Any], direction: str) -> Dict[str, Any]:
    range60 = safe_float(snapshot.get("range_position_60_centered"))
    range240 = safe_float(snapshot.get("range_position_240_centered"))
    rsi = safe_float(snapshot.get("rsi14_centered"))
    m5 = safe_float(snapshot.get("momentum_5_atr"))
    m15 = safe_float(snapshot.get("momentum_15_atr"))
    m30 = safe_float(snapshot.get("momentum_30_atr"))
    m60 = safe_float(snapshot.get("momentum_60_atr"))
    spread_to_atr = safe_float(snapshot.get("spread_to_atr240"))
    atr_spread_eff = safe_float(snapshot.get("atr_spread_efficiency"))
    rule_long = safe_float(snapshot.get("rule_baseline_long_score"))
    rule_short = safe_float(snapshot.get("rule_baseline_short_score"))
    if range60 >= 0.8:
        range_label = "near_60m_high"
    elif range60 <= -0.8:
        range_label = "near_60m_low"
    else:
        range_label = "mid_60m_range"
    if range240 >= 0.8:
        range240_label = "near_240m_high"
    elif range240 <= -0.8:
        range240_label = "near_240m_low"
    else:
        range240_label = "mid_240m_range"
    dir_sign = 1 if direction.upper() == "LONG" else -1
    aligned_momentum = sum(1 for x in [m5, m15, m30, m60] if x * dir_sign > 0)
    return {
        "range60_label": range_label,
        "range240_label": range240_label,
        "rsi_centered": round(rsi, 4),
        "momentum_5_atr": round(m5, 4),
        "momentum_15_atr": round(m15, 4),
        "momentum_30_atr": round(m30, 4),
        "momentum_60_atr": round(m60, 4),
        "aligned_momentum_count": aligned_momentum,
        "spread_to_atr240": round(spread_to_atr, 4),
        "atr_spread_efficiency": round(atr_spread_eff, 4),
        "rule_baseline_long_score": round(rule_long, 4),
        "rule_baseline_short_score": round(rule_short, 4),
        "rule_direction_score": round(rule_long - rule_short, 4),
    }


def payoff_estimate(instrument: str, net_pips: float, account_nav: float = 10.0, margin_fraction: float = 0.70) -> Dict[str, Any]:
    info = PAIR_INFO.get(instrument)
    if not info:
        return {}
    margin_rate = safe_float(info["margin_rate"], 0.10)
    mid = safe_float(info["mid"], 1.0)
    # For non-USD base pairs this is approximate.  Use mid/base value in USD
    # where known; fallback to EURUSD-ish for EUR, GBPUSD-ish for GBP, CHFUSD-ish.
    base = instrument.split("_")[0]
    base_usd = {
        "USD": 1.0,
        "EUR": 1.135,
        "GBP": 1.316,
        "CHF": 1.22,
    }.get(base, 1.0)
    max_units = math.floor((account_nav * margin_fraction) / max(base_usd * margin_rate, 1e-9))
    pip_value_usd_per_unit = safe_float(info["pip_size"]) * safe_float(info["usd_per_quote"])
    gross_usd = max_units * net_pips * pip_value_usd_per_unit
    return {
        "account_nav": account_nav,
        "margin_fraction": margin_fraction,
        "margin_rate": margin_rate,
        "estimated_units": max_units,
        "pip_value_usd_per_unit": pip_value_usd_per_unit,
        "gross_usd_if_full_move_captured": gross_usd,
        "gross_return_pct_on_nav": (gross_usd / account_nav * 100.0) if account_nav else 0.0,
    }


def build_report(hours: int = 12, top_n: int = 20) -> Dict[str, Any]:
    cutoff = pd.Timestamp(utc_now() - timedelta(hours=hours))
    accepted_keys, accepted = accepted_keys_since(cutoff)
    windows = load_event_windows(cutoff)
    campaign_rows = load_campaign_rows(cutoff)
    if windows.empty:
        raise RuntimeError("No event windows available")
    rows = windows.copy()
    rows["entered_same_direction"] = [
        (str(i), str(d)) in accepted_keys
        for i, d in zip(rows["instrument"], rows["direction"])
    ]
    # cluster duplicate themes/logs by pair, direction, and 30m end bucket.
    rows["end_ts"] = pd.to_datetime(rows["end_utc"], errors="coerce", utc=True)
    rows["end_bucket"] = rows["end_ts"].dt.floor("30min").astype(str)
    rows["abs_net_pips"] = rows["net_pips"].abs()
    idx = rows.sort_values(["abs_net_pips", "move_to_spread_ratio"], ascending=False).groupby(
        ["instrument", "direction", "end_bucket"], dropna=False
    ).head(1).index
    clustered = rows.loc[idx].copy()
    missed = clustered[
        (~clustered["entered_same_direction"])
        & ((clustered["abs_net_pips"] >= 50) | (clustered["move_to_spread_ratio"] >= 1.8))
    ].sort_values(["abs_net_pips", "move_to_spread_ratio"], ascending=False).head(top_n)

    campaign_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in campaign_rows:
        campaign_by_key.setdefault((str(row["instrument"]), str(row["direction"])), []).append(row)
    for row_list in campaign_by_key.values():
        row_list.sort(key=lambda item: str(item.get("time_utc", "")))

    missed_rows: List[Dict[str, Any]] = []
    for _, row in missed.iterrows():
        inst = str(row["instrument"])
        direction = str(row["direction"])
        start = parse_time(row.get("start_utc"))
        end = parse_time(row.get("end_utc"))
        pair_campaigns = campaign_by_key.get((inst, direction), [])
        # Choose campaign nearest the end/start of the target event.
        chosen = None
        if end is not None:
            for item in pair_campaigns:
                item_time = parse_time(item.get("time_utc"))
                if item_time is None:
                    continue
                if item_time <= end + pd.Timedelta(minutes=5):
                    chosen = item
        # Find simple early/late windows inside the move or within 10m before.
        related = rows[(rows["instrument"].astype(str) == inst) & (rows["direction"].astype(str) == direction)].copy()
        if start is not None and end is not None:
            related["related_end"] = pd.to_datetime(related["end_utc"], errors="coerce", utc=True)
            related = related[
                (related["related_end"] >= start - pd.Timedelta(minutes=10))
                & (related["related_end"] <= end)
            ].sort_values("related_end")
        micro = related[
            (related["window_minutes"] <= 5)
            & (related["abs_net_pips"] >= 40)
            & (related["move_to_spread_ratio"] >= 1.0)
        ].head(1)
        late = related[
            (related["window_minutes"] <= 10)
            & (related["abs_net_pips"] >= 100)
            & (related["move_to_spread_ratio"] >= 1.4)
        ].head(1)
        soft = related[
            (related["move_to_spread_ratio"] >= 2.0)
            & (related["abs_net_pips"] >= 150)
        ].head(1)
        snapshot = chosen.get("snapshot", {}) if chosen else {}
        missed_rows.append({
            "instrument": inst,
            "direction": direction,
            "theme": row.get("theme", ""),
            "window_minutes": safe_int(row.get("window_minutes")),
            "net_pips": safe_float(row.get("net_pips")),
            "spread_avg_pips": safe_float(row.get("spread_avg_pips")),
            "move_to_spread_ratio": safe_float(row.get("move_to_spread_ratio")),
            "start_utc": row.get("start_utc", ""),
            "end_utc": row.get("end_utc", ""),
            "payoff_70pct_margin_10usd": payoff_estimate(inst, safe_float(row.get("net_pips"))),
            "nearest_campaign": {
                key: chosen.get(key)
                for key in [
                    "time_utc", "status", "action", "reason", "last_net_pips",
                    "last_ratio", "event_age_minutes", "promoted_probability",
                    "promoted_threshold", "ensemble_probability",
                ]
            } if chosen else {},
            "snapshot_classification": classify_snapshot(snapshot, direction) if snapshot else {},
            "micro_impulse_trigger": micro[[
                "time_utc", "window_minutes", "net_pips", "move_to_spread_ratio", "start_utc", "end_utc"
            ]].to_dict("records")[:1],
            "late_impulse_trigger": late[[
                "time_utc", "window_minutes", "net_pips", "move_to_spread_ratio", "start_utc", "end_utc"
            ]].to_dict("records")[:1],
            "soft_override_trigger": soft[[
                "time_utc", "window_minutes", "net_pips", "move_to_spread_ratio", "start_utc", "end_utc"
            ]].to_dict("records")[:1],
        })

    # Common themes/features among the top missed rows.
    feature_counters = {
        "themes": Counter(),
        "instruments": Counter(),
        "range60_labels": Counter(),
        "range240_labels": Counter(),
        "aligned_momentum_count": Counter(),
    }
    for item in missed_rows:
        feature_counters["themes"][str(item.get("theme", ""))] += 1
        feature_counters["instruments"][str(item.get("instrument", ""))] += 1
        cls = item.get("snapshot_classification") or {}
        if cls:
            feature_counters["range60_labels"][str(cls.get("range60_label", ""))] += 1
            feature_counters["range240_labels"][str(cls.get("range240_label", ""))] += 1
            feature_counters["aligned_momentum_count"][str(cls.get("aligned_momentum_count", ""))] += 1

    skip_reasons = Counter()
    for root in [TECH_ROOT, PRIMARY_ROOT]:
        df = read_csv(root / "actions.csv")
        if df.empty or "time_utc" not in df:
            continue
        df["dt"] = pd.to_datetime(df["time_utc"], errors="coerce", utc=True)
        df = df[(df["dt"] >= cutoff) & (df["status"].astype(str).str.lower() == "skipped")]
        for value in df.get("reject_reason", pd.Series(dtype=str)).fillna("").astype(str):
            if "too old" in value:
                skip_reasons["event too old"] += 1
            elif "score too low" in value or "EV score too low" in value:
                skip_reasons["score too low"] += 1
            elif "move/spread too low" in value:
                skip_reasons["move/spread too low"] += 1
            elif "counter-regime" in value:
                skip_reasons["counter-regime blocked"] += 1
            elif value:
                skip_reasons[value[:80]] += 1

    return {
        "generated_utc": utc_now().isoformat(),
        "lookback_hours": hours,
        "accepted_entries": accepted,
        "skip_reason_counts": skip_reasons.most_common(12),
        "top_missed_moves": missed_rows,
        "common_missed_move_patterns": {
            key: counter.most_common(10)
            for key, counter in feature_counters.items()
        },
        "candidate_logic": [
            {
                "name": "micro_impulse_scout",
                "entry": "same-direction 3m/5m event window >= 40 pips and move/spread >= 1.0",
                "exit": "very tight adaptive trail: max(0.35*ATR15, 0.20*event_window_pips), hard time stop 5-8m",
                "purpose": "enter late once movement is real; accepts missing first segment",
            },
            {
                "name": "late_breakout_scout",
                "entry": "same-direction <=10m window >=100 pips and move/spread >=1.4",
                "exit": "trail 30-45% of triggering window, cut if next 2 closes retrace >40%",
                "purpose": "capture middle of 15-30m violent spikes",
            },
            {
                "name": "soft_override_for_exotics",
                "entry": "ratio >=2.0 and absolute pips >=150, even if model score below 90, if spread_to_atr acceptable",
                "exit": "half-size only; pair-specific trail based on historical 1m/5m variance",
                "purpose": "avoid rejecting obvious huge windows because classifier score is conservative",
            },
        ],
    }


def write_outputs(report: Dict[str, Any]) -> Dict[str, str]:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = REPORT_ROOT / f"missed_spike_capture_study_{stamp}"
    outdir.mkdir(parents=True, exist_ok=True)
    json_path = outdir / "missed_spike_capture_study.json"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    rows = report.get("top_missed_moves", [])
    flat_rows = []
    for row in rows:
        flat = {
            key: value
            for key, value in row.items()
            if key not in {"payoff_70pct_margin_10usd", "nearest_campaign", "snapshot_classification", "micro_impulse_trigger", "late_impulse_trigger", "soft_override_trigger"}
        }
        payoff = row.get("payoff_70pct_margin_10usd") or {}
        for key, value in payoff.items():
            flat[f"payoff_{key}"] = value
        for key, value in (row.get("nearest_campaign") or {}).items():
            flat[f"campaign_{key}"] = value
        for key, value in (row.get("snapshot_classification") or {}).items():
            flat[f"feature_{key}"] = value
        for name in ["micro_impulse_trigger", "late_impulse_trigger", "soft_override_trigger"]:
            trigger = row.get(name) or []
            flat[name] = json.dumps(trigger, default=str)
        flat_rows.append(flat)
    csv_path = outdir / "top_missed_moves.csv"
    if flat_rows:
        pd.DataFrame(flat_rows).to_csv(csv_path, index=False)
    md_path = outdir / "summary.md"
    lines = [
        "# Missed spike capture study",
        "",
        f"Generated: {report.get('generated_utc')}",
        f"Lookback hours: {report.get('lookback_hours')}",
        "",
        "## Skip reasons",
        "",
    ]
    for reason, count in report.get("skip_reason_counts", [])[:8]:
        lines.append(f"- {reason}: {count}")
    lines.extend(["", "## Top missed moves", ""])
    for row in rows[:10]:
        payoff = row.get("payoff_70pct_margin_10usd") or {}
        lines.append(
            f"- {row['instrument']} {row['direction']} {row['net_pips']:.1f}p "
            f"ratio={row['move_to_spread_ratio']:.2f}; "
            f"$10@70% margin approx ${payoff.get('gross_usd_if_full_move_captured', 0.0):.3f}"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    latest_json = REPORT_ROOT / "latest_missed_spike_capture_study.json"
    latest_csv = REPORT_ROOT / "latest_missed_spike_capture_study_top_moves.csv"
    latest_md = REPORT_ROOT / "latest_missed_spike_capture_study.md"
    latest_json.write_text(json_path.read_text(encoding="utf-8"), encoding="utf-8")
    if csv_path.exists():
        latest_csv.write_text(csv_path.read_text(encoding="utf-8"), encoding="utf-8")
    latest_md.write_text(md_path.read_text(encoding="utf-8"), encoding="utf-8")
    return {
        "outdir": str(outdir),
        "json": str(json_path),
        "csv": str(csv_path),
        "markdown": str(md_path),
        "latest_json": str(latest_json),
        "latest_csv": str(latest_csv),
        "latest_markdown": str(latest_md),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build missed volatile spike capture study")
    parser.add_argument("--hours", type=int, default=12)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()
    report = build_report(hours=max(1, args.hours), top_n=max(1, args.top))
    outputs = write_outputs(report)
    print(json.dumps({
        "generated_utc": report["generated_utc"],
        "lookback_hours": report["lookback_hours"],
        "top_missed_moves": len(report["top_missed_moves"]),
        "skip_reason_counts": report["skip_reason_counts"][:5],
        "common_missed_move_patterns": report["common_missed_move_patterns"],
        "outputs": outputs,
    }, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
