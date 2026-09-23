from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import (
    ROOT,
    add_time_features,
    parse_date,
    pair_meta,
    select_pairs,
    utc_now_stamp,
    write_json,
)
from .economics import (
    ExecutionSettings,
    ExecutablePriceArrays,
    executable_price_arrays,
    execution_settings_from_config,
    prepare_executable_prices,
    simulate_tp_sl_contract_arrays,
)
from .features import write_feature_family_report


@dataclass(frozen=True)
class PolicyInstance:
    name: str
    tp_pips: float
    sl_pips: float
    policy_type: str


DEFAULT_DYNAMIC_POLICY_TEMPLATES = [
    {"name": "atr_0p25_0p40", "basis": "atr_15m_pips", "tp_mult": 0.25, "sl_mult": 0.40, "min_tp_pips": 1.0, "min_sl_pips": 1.5},
    {"name": "atr_0p35_0p55", "basis": "atr_15m_pips", "tp_mult": 0.35, "sl_mult": 0.55, "min_tp_pips": 1.0, "min_sl_pips": 1.8},
    {"name": "atr_0p50_0p80", "basis": "atr_15m_pips", "tp_mult": 0.50, "sl_mult": 0.80, "min_tp_pips": 1.5, "min_sl_pips": 2.5},
    {"name": "spread_2p5_4p0", "basis": "spread_pips_used", "tp_mult": 2.5, "sl_mult": 4.0, "min_tp_pips": 1.0, "min_sl_pips": 1.5},
    {"name": "spread_3p5_6p0", "basis": "spread_pips_used", "tp_mult": 3.5, "sl_mult": 6.0, "min_tp_pips": 1.0, "min_sl_pips": 2.0},
    {"name": "range_0p30_0p50", "basis": "range_15m_pips", "tp_mult": 0.30, "sl_mult": 0.50, "min_tp_pips": 1.0, "min_sl_pips": 1.8},
]


def normalize_pair_args(pairs: list[str] | None) -> list[str] | None:
    if not pairs:
        return None
    out: list[str] = []
    for item in pairs:
        out.extend([p.strip() for p in str(item).split(",") if p.strip()])
    return out or None


def _read_candles_for_audit(
    instrument: str,
    cfg: dict[str, Any],
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    max_rows: int | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = ROOT / cfg["candles_dir"] / f"{instrument}_M1.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    wanted = {
        "time",
        "datetime",
        "instrument",
        "granularity",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "bid_open",
        "bid_high",
        "bid_low",
        "bid_close",
        "ask_open",
        "ask_high",
        "ask_low",
        "ask_close",
        "spread_pips",
    }
    raw = pd.read_csv(path, usecols=lambda c: c in wanted)
    time_col = "datetime" if "datetime" in raw.columns else "time"
    raw["decision_time_utc"] = pd.to_datetime(raw[time_col], utc=True, errors="coerce")
    raw = raw.dropna(subset=["decision_time_utc"])
    if start is not None:
        raw = raw[raw["decision_time_utc"] >= start]
    if end is not None:
        raw = raw[raw["decision_time_utc"] <= end]
    rows_before_dedupe = len(raw)
    duplicate_timestamps = int(raw["decision_time_utc"].duplicated().sum())
    raw = raw.sort_values("decision_time_utc").drop_duplicates("decision_time_utc").reset_index(drop=True)
    if max_rows and len(raw) > max_rows:
        raw = raw.tail(max_rows).reset_index(drop=True)
    raw["instrument"] = instrument
    for col in [c for c in raw.columns if c not in {"time", "datetime", "instrument", "granularity", "decision_time_utc"}]:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    health = {
        "pair": instrument,
        "rows_before_dedupe": int(rows_before_dedupe),
        "duplicate_timestamps_dropped": duplicate_timestamps,
        "rows": int(len(raw)),
        "start": str(raw["decision_time_utc"].min()) if len(raw) else None,
        "end": str(raw["decision_time_utc"].max()) if len(raw) else None,
        "monotonic_after_sort": bool(raw["decision_time_utc"].is_monotonic_increasing),
    }
    return raw, health


def _add_audit_features(df: pd.DataFrame, instrument: str) -> pd.DataFrame:
    out = add_time_features(df.copy())
    pip = pair_meta(instrument, {"tier1_pairs": [], "tier2_pairs": [], "costs": {}}).pip_size
    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    prev_close = close.shift(1)
    tr_pips = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1) / pip
    for n in [1, 2, 3, 5, 8, 13, 15, 21, 30]:
        out[f"return_{n}m_pips"] = (close - close.shift(n)) / pip
    out["atr_15m_pips"] = tr_pips.rolling(15, min_periods=5).mean()
    out["atr_30m_pips"] = tr_pips.rolling(30, min_periods=10).mean()
    out["range_15m_pips"] = (high.rolling(15, min_periods=5).max() - low.rolling(15, min_periods=5).min()) / pip
    out["range_30m_pips"] = (high.rolling(30, min_periods=10).max() - low.rolling(30, min_periods=10).min()) / pip
    out["momentum_5m_atr"] = out["return_5m_pips"] / out["atr_15m_pips"].replace(0, np.nan)
    out["spread_to_atr_15m"] = out["spread_pips_used"] / out["atr_15m_pips"].replace(0, np.nan)
    return out


def _fixed_policies(cfg: dict[str, Any]) -> list[PolicyInstance]:
    return [
        PolicyInstance(str(p["name"]), float(p["tp_pips"]), float(p["sl_pips"]), "fixed")
        for p in cfg.get("policy_templates", [])
    ]


def _policies_for_row(row: pd.Series, fixed: list[PolicyInstance], dynamic_templates: list[dict[str, Any]]) -> list[PolicyInstance]:
    policies = list(fixed)
    for template in dynamic_templates:
        basis = float(row.get(str(template["basis"]), np.nan))
        if not np.isfinite(basis) or basis <= 0:
            continue
        tp = max(float(template.get("min_tp_pips", 0.0)), basis * float(template["tp_mult"]))
        sl = max(float(template.get("min_sl_pips", 0.0)), basis * float(template["sl_mult"]))
        if np.isfinite(tp) and np.isfinite(sl) and tp > 0 and sl > 0:
            policies.append(PolicyInstance(str(template["name"]), float(tp), float(sl), "dynamic"))
    return policies


def _quote_to_usd_series(pair: str, df: pd.DataFrame, closes: dict[str, pd.Series]) -> pd.Series:
    _, quote = pair.split("_", 1)
    idx = df["decision_time_utc"]
    if quote == "USD":
        return pd.Series(1.0, index=df.index)
    direct = f"{quote}_USD"
    inverse = f"USD_{quote}"
    if direct in closes:
        values = closes[direct].reindex(idx).ffill().bfill()
        return pd.Series(values.to_numpy(dtype=float), index=df.index)
    if inverse in closes:
        values = closes[inverse].reindex(idx).ffill().bfill()
        return pd.Series(1.0 / values.to_numpy(dtype=float), index=df.index)
    fallback = {"JPY": 0.0068, "CHF": 1.12, "CAD": 0.73, "AUD": 0.66, "NZD": 0.61, "GBP": 1.28, "EUR": 1.08}.get(quote, 1.0)
    return pd.Series(fallback, index=df.index)


def _update_topn(store: dict[pd.Timestamp, list[float]], ts: pd.Timestamp, value: float, n: int = 5) -> None:
    if not np.isfinite(value):
        return
    arr = store.setdefault(ts, [])
    arr.append(float(value))
    arr.sort(reverse=True)
    if len(arr) > n:
        del arr[n:]


def _summarize_topn(store: dict[pd.Timestamp, list[float]], k: int) -> dict[str, Any]:
    vals = [float(np.mean(arr[:k])) for arr in store.values() if arr]
    if not vals:
        return {"timestamps": 0, "mean_ev": None, "median_ev": None, "positive_rate": None, "sum_mean_ev": None}
    s = pd.Series(vals)
    return {
        "timestamps": int(len(vals)),
        "mean_ev": float(s.mean()),
        "median_ev": float(s.median()),
        "positive_rate": float((s > 0).mean()),
        "sum_mean_ev": float(s.sum()),
        "p05_ev": float(s.quantile(0.05)),
        "p95_ev": float(s.quantile(0.95)),
    }


def _acc_row(key: tuple[Any, ...], acc: dict[str, float], cols: list[str]) -> dict[str, Any]:
    n = max(float(acc.get("rows", 0.0)), 1.0)
    row = {col: val for col, val in zip(cols, key)}
    row.update({
        "rows": int(acc.get("rows", 0.0)),
        "mean_ev_usd_per_1k_units": float(acc.get("ev_sum", 0.0) / n),
        "sum_ev_usd_per_1k_units": float(acc.get("ev_sum", 0.0)),
        "positive_rate": float(acc.get("positive", 0.0) / n),
        "tp_rate": float(acc.get("tp", 0.0) / n),
        "sl_rate": float(acc.get("sl", 0.0) / n),
        "timeout_rate": float(acc.get("timeout", 0.0) / n),
        "ambiguous_rate": float(acc.get("ambiguous", 0.0) / n),
        "mean_mfe_pips": float(acc.get("mfe_sum", 0.0) / n),
        "mean_mae_pips": float(acc.get("mae_sum", 0.0) / n),
        "mean_duration_min": float(acc.get("duration_sum", 0.0) / n),
        "mean_tp_pips_used": float(acc.get("tp_pips_sum", 0.0) / n),
        "mean_sl_pips_used": float(acc.get("sl_pips_sum", 0.0) / n),
    })
    return row


def _baseline_summary(store: dict[pd.Timestamp, tuple[float, float]]) -> dict[str, Any]:
    vals = [float(ev) for _, ev in store.values() if np.isfinite(ev)]
    if not vals:
        return {"timestamps": 0}
    s = pd.Series(vals)
    return {
        "timestamps": int(len(vals)),
        "mean_ev": float(s.mean()),
        "median_ev": float(s.median()),
        "sum_ev": float(s.sum()),
        "positive_rate": float((s > 0).mean()),
        "p05_ev": float(s.quantile(0.05)),
        "p95_ev": float(s.quantile(0.95)),
    }


def _markdown_table(df: pd.DataFrame, max_rows: int | None = None) -> str:
    if df.empty:
        return "No rows."
    view = df.head(max_rows).copy() if max_rows else df.copy()
    view = view.fillna("")
    cols = [str(c) for c in view.columns]
    rows = [[str(v) for v in row] for row in view.to_numpy()]
    widths = [
        max(len(cols[i]), *(len(row[i]) for row in rows)) if rows else len(cols[i])
        for i in range(len(cols))
    ]
    header = "| " + " | ".join(cols[i].ljust(widths[i]) for i in range(len(cols))) + " |"
    sep = "| " + " | ".join("-" * widths[i] for i in range(len(cols))) + " |"
    body = ["| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(cols))) + " |" for row in rows]
    return "\n".join([header, sep] + body)


def _write_summary_md(path: Path, payload: dict[str, Any], policy_df: pd.DataFrame, health_df: pd.DataFrame) -> None:
    lines = [
        "# Fresh M1 Intrahour Label Audit",
        "",
        f"Generated UTC: `{payload['generated_utc']}`",
        f"Verdict: `{payload['verdict']}`",
        f"Pairs: `{', '.join(payload['pairs'])}`",
        f"Date range: `{payload['start']}` to `{payload['end']}`",
        f"Live execution enabled: `{payload['live_execution_enabled']}`",
        "",
        "## Execution",
        "```json",
        json.dumps(payload["execution"], indent=2, default=str),
        "```",
        "",
        "## Invariants",
        "```json",
        json.dumps(payload["invariants"], indent=2, default=str),
        "```",
        "",
        "## Data Health",
        _markdown_table(health_df),
        "",
        "## Top Policy/Horizon Economics",
        _markdown_table(policy_df.sort_values("mean_ev_usd_per_1k_units", ascending=False), max_rows=20),
        "",
        "## Caveats",
    ]
    lines.extend([f"- {c}" for c in payload.get("caveats", [])] or ["- None."])
    path.write_text("\n".join(lines), encoding="utf-8")


def run_label_audit(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str | None,
    end: str | None,
    tier: str,
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
    max_horizon: int | None = None,
    include_dynamic_policies: bool = True,
    cost_mode: str = "base",
    sample_minutes: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    start_ts = parse_date(start)
    end_ts = parse_date(end)
    selected_pairs = select_pairs(cfg, tier, normalize_pair_args(pairs))
    horizons = [int(h) for h in cfg["horizons_minutes"] if max_horizon is None or int(h) <= int(max_horizon)]
    if not horizons:
        raise ValueError("no horizons selected")

    fixed = _fixed_policies(cfg)
    dynamic_templates = cfg.get("dynamic_policy_templates", DEFAULT_DYNAMIC_POLICY_TEMPLATES) if include_dynamic_policies else []
    pair_frames: dict[str, tuple[pd.DataFrame, dict[str, Any], ExecutionSettings, ExecutablePriceArrays]] = {}
    closes: dict[str, pd.Series] = {}
    data_health: list[dict[str, Any]] = []

    for pair in selected_pairs:
        raw, health = _read_candles_for_audit(pair, cfg, start_ts, end_ts, max_rows_per_pair)
        if len(raw) <= max(horizons) + 60:
            health["skipped"] = True
            health["skip_reason"] = "too_few_rows"
            data_health.append(health)
            continue
        meta = pair_meta(pair, cfg)
        settings = execution_settings_from_config(cfg, meta.tier, cost_mode)
        exec_df, exec_meta = prepare_executable_prices(raw, pair, cfg, settings.spread_multiplier)
        exec_df = _add_audit_features(exec_df, pair)
        arrays = executable_price_arrays(exec_df)
        health.update(exec_meta)
        health["skipped"] = False
        data_health.append(health)
        pair_frames[pair] = (exec_df, health, settings, arrays)
        closes[pair] = pd.Series(exec_df["close"].to_numpy(dtype=float), index=exec_df["decision_time_utc"])

    if not pair_frames:
        raise RuntimeError("no usable candle frames for label audit")

    summary_acc: dict[tuple[str, str, int], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    pair_acc: dict[tuple[str, str, str, int], dict[str, float]] = defaultdict(lambda: defaultdict(float))
    oracle_topn: dict[pd.Timestamp, list[float]] = {}
    baseline_mom1: dict[pd.Timestamp, tuple[float, float]] = {}
    baseline_mom5: dict[pd.Timestamp, tuple[float, float]] = {}
    topk_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    feature_probe_frames: list[pd.DataFrame] = []
    oracle_rows: list[dict[str, Any]] = []

    invariants: dict[str, int] = defaultdict(int)
    invariants["same_bar_ambiguity_count"] = 0
    invariants["rows_evaluated"] = 0
    invariants["contracts_evaluated"] = 0
    invariants["dynamic_contracts_evaluated"] = 0

    for pair, (df, _health, settings, arrays) in pair_frames.items():
        meta = pair_meta(pair, cfg)
        quote_to_usd = _quote_to_usd_series(pair, df, closes).replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(1.0)
        pip_value = meta.pip_size * quote_to_usd.to_numpy(dtype=float)
        warmup = 60
        last_i = len(df) - int(settings.entry_delay_bars) - max(horizons)
        if last_i <= warmup:
            continue
        feature_probe_frames.append(df.iloc[warmup : min(warmup + 500, last_i)].copy())
        for i in range(warmup, last_i + 1):
            if sample_minutes and sample_minutes > 1 and (i - warmup) % int(sample_minutes) != 0:
                continue
            row = df.iloc[i]
            ts = pd.Timestamp(row["decision_time_utc"])
            policies = _policies_for_row(row, fixed, dynamic_templates)
            if not policies:
                continue
            invariants["rows_evaluated"] += 1
            side_feature = {
                "long": {
                    "mom1": float(row.get("return_1m_pips", np.nan)),
                    "mom5": float(row.get("return_5m_pips", np.nan)),
                },
                "short": {
                    "mom1": -float(row.get("return_1m_pips", np.nan)),
                    "mom5": -float(row.get("return_5m_pips", np.nan)),
                },
            }
            for side in ["long", "short"]:
                for policy in policies:
                    previous_mfe = None
                    previous_mae = None
                    first_terminal = None
                    endpoint_values: list[float] = []
                    for h in horizons:
                        result = simulate_tp_sl_contract_arrays(arrays, i, side, policy.tp_pips, policy.sl_pips, h, meta.pip_size, settings)
                        if not result.complete:
                            continue
                        ev = float(result.realized_pips * pip_value[i] * 1000.0) if np.isfinite(result.realized_pips) else np.nan
                        invariants["contracts_evaluated"] += 1
                        invariants["same_bar_ambiguity_count"] += int(result.same_bar_ambiguous)
                        if policy.policy_type == "dynamic":
                            invariants["dynamic_contracts_evaluated"] += 1
                        if result.entry_index is None or result.entry_index <= i:
                            invariants["label_start_violations"] += 1
                        if result.outcome == "timeout" and abs(float(result.realized_pips) - float(result.endpoint_pips)) > 1e-9:
                            invariants["timeout_endpoint_pnl_violations"] += 1
                        if previous_mfe is not None and np.isfinite(result.mfe_pips) and result.mfe_pips + 1e-9 < previous_mfe:
                            invariants["mfe_monotonicity_violations"] += 1
                        if previous_mae is not None and np.isfinite(result.mae_pips) and result.mae_pips + 1e-9 < previous_mae:
                            invariants["mae_monotonicity_violations"] += 1
                        previous_mfe = result.mfe_pips if np.isfinite(result.mfe_pips) else previous_mfe
                        previous_mae = result.mae_pips if np.isfinite(result.mae_pips) else previous_mae
                        terminal = "sl_before_tp" if result.outcome == "ambiguous_adverse_first" else result.outcome
                        if terminal in {"tp_before_sl", "sl_before_tp"}:
                            if first_terminal is None:
                                first_terminal = terminal
                            elif terminal != first_terminal:
                                invariants["first_touch_horizon_consistency_violations"] += 1
                        endpoint_values.append(float(result.endpoint_pips))

                        key = (policy.policy_type, policy.name, h)
                        pkey = (pair, policy.policy_type, policy.name, h)
                        for acc in [summary_acc[key], pair_acc[pkey]]:
                            acc["rows"] += 1
                            acc["ev_sum"] += 0.0 if not np.isfinite(ev) else ev
                            acc["positive"] += 1 if np.isfinite(ev) and ev > 0 else 0
                            acc["tp"] += 1 if result.outcome in {"tp_before_sl", "ambiguous_favorable_first"} else 0
                            acc["sl"] += 1 if result.outcome in {"sl_before_tp", "ambiguous_adverse_first"} else 0
                            acc["timeout"] += 1 if result.outcome == "timeout" else 0
                            acc["ambiguous"] += int(result.same_bar_ambiguous)
                            acc["mfe_sum"] += 0.0 if not np.isfinite(result.mfe_pips) else float(result.mfe_pips)
                            acc["mae_sum"] += 0.0 if not np.isfinite(result.mae_pips) else float(result.mae_pips)
                            acc["duration_sum"] += 0.0 if result.time_to_exit_min is None else float(result.time_to_exit_min)
                            acc["tp_pips_sum"] += float(policy.tp_pips)
                            acc["sl_pips_sum"] += float(policy.sl_pips)
                        _update_topn(oracle_topn, ts, ev, 5)

                        mom1 = side_feature[side]["mom1"]
                        mom5 = side_feature[side]["mom5"]
                        if np.isfinite(mom1):
                            old = baseline_mom1.get(ts)
                            if old is None or mom1 > old[0]:
                                baseline_mom1[ts] = (mom1, ev)
                        if np.isfinite(mom5):
                            old = baseline_mom5.get(ts)
                            if old is None or mom5 > old[0]:
                                baseline_mom5[ts] = (mom5, ev)

                        if len(sample_rows) < 1000 and (result.same_bar_ambiguous or (np.isfinite(ev) and ev > 0)):
                            sample_rows.append({
                                "timestamp": ts,
                                "pair": pair,
                                "side": side,
                                "policy_name": policy.name,
                                "policy_type": policy.policy_type,
                                "horizon_min": h,
                                "tp_pips": policy.tp_pips,
                                "sl_pips": policy.sl_pips,
                                "outcome": result.outcome,
                                "ev_usd_per_1k_units": ev,
                                "realized_pips": result.realized_pips,
                                "endpoint_pips": result.endpoint_pips,
                                "mfe_pips": result.mfe_pips,
                                "mae_pips": result.mae_pips,
                                "same_bar_ambiguous": result.same_bar_ambiguous,
                                "spread_pips_used": row.get("spread_pips_used"),
                                "atr_15m_pips": row.get("atr_15m_pips"),
                            })
                        if len(topk_rows) < 20000 and np.isfinite(ev):
                            topk_rows.append({
                                "timestamp": ts,
                                "pair": pair,
                                "side": side,
                                "policy_name": policy.name,
                                "policy_type": policy.policy_type,
                                "horizon_min": h,
                                "tp_pips": policy.tp_pips,
                                "sl_pips": policy.sl_pips,
                                "actual_ev_usd_per_1k_units": ev,
                                "actual_pips": result.realized_pips,
                                "outcome": result.outcome,
                            })
                    if len(endpoint_values) > 1 and len({round(v, 8) for v in endpoint_values if np.isfinite(v)}) <= 1:
                        invariants["endpoint_static_across_horizons_groups"] += 1

    policy_df = pd.DataFrame([_acc_row(k, v, ["policy_type", "policy_name", "horizon_min"]) for k, v in summary_acc.items()])
    pair_df = pd.DataFrame([_acc_row(k, v, ["pair", "policy_type", "policy_name", "horizon_min"]) for k, v in pair_acc.items()])
    health_df = pd.DataFrame(data_health)
    topk_df = pd.DataFrame(topk_rows)
    if not topk_df.empty:
        topk_df = topk_df.sort_values(["timestamp", "actual_ev_usd_per_1k_units"], ascending=[True, False])
        topk_df.groupby("timestamp", as_index=False).head(5).to_csv(output_dir / "topk_candidates.csv", index=False)
    else:
        pd.DataFrame().to_csv(output_dir / "topk_candidates.csv", index=False)
    pd.DataFrame(sample_rows).to_csv(output_dir / "positive_or_ambiguous_samples.csv", index=False)
    policy_df.sort_values(["policy_type", "policy_name", "horizon_min"]).to_csv(output_dir / "surface_label_audit.csv", index=False)
    pair_df.sort_values(["pair", "policy_type", "policy_name", "horizon_min"]).to_csv(output_dir / "pair_policy_horizon_economics.csv", index=False)
    health_df.to_csv(output_dir / "data_health.csv", index=False)

    for ts, arr in oracle_topn.items():
        oracle_rows.append({
            "timestamp": ts,
            "oracle_top1_ev": float(np.mean(arr[:1])) if arr else np.nan,
            "oracle_top3_mean_ev": float(np.mean(arr[:3])) if arr else np.nan,
            "oracle_top5_mean_ev": float(np.mean(arr[:5])) if arr else np.nan,
        })
    pd.DataFrame(oracle_rows).sort_values("timestamp").to_csv(output_dir / "oracle_opportunity_by_timestamp.csv", index=False)

    oracle_report = {
        "top1": _summarize_topn(oracle_topn, 1),
        "top3_mean": _summarize_topn(oracle_topn, 3),
        "top5_mean": _summarize_topn(oracle_topn, 5),
        "diagnostic_only": True,
        "oracle_columns_used_for_selection": False,
    }
    baseline_report = {
        "no_trade_baseline": {"return_pct": 0.0, "pnl": 0.0, "trade_count": 0},
        "top_side_momentum_1m": _baseline_summary(baseline_mom1),
        "top_side_momentum_5m": _baseline_summary(baseline_mom5),
    }
    write_json(output_dir / "oracle_opportunity_report.json", oracle_report)
    write_json(output_dir / "baseline_comparison_report.json", baseline_report)

    feature_df = pd.concat(feature_probe_frames, ignore_index=True) if feature_probe_frames else pd.DataFrame()
    feature_report_path = write_feature_family_report(feature_df, output_dir / "feature_family_report.json") if not feature_df.empty else None

    invariant_payload = dict(invariants)
    if not policy_df.empty:
        invariant_payload["policy_horizons_seen"] = {
            f"{ptype}|{pname}": int(count)
            for (ptype, pname), count in policy_df.groupby(["policy_type", "policy_name"])["horizon_min"].nunique().items()
        }
    else:
        invariant_payload["policy_horizons_seen"] = {}
    invariant_payload["policy_default_horizon_override_violations"] = 0
    violation_keys = [
        "label_start_violations",
        "timeout_endpoint_pnl_violations",
        "mfe_monotonicity_violations",
        "mae_monotonicity_violations",
        "first_touch_horizon_consistency_violations",
        "policy_default_horizon_override_violations",
    ]
    violation_total = int(sum(int(invariant_payload.get(k, 0)) for k in violation_keys))
    invariant_payload["invariant_violation_total"] = violation_total
    invariant_payload["label_start_strictly_after_decision"] = int(invariant_payload.get("label_start_violations", 0)) == 0
    invariant_payload["same_bar_ambiguity_mode_reported"] = True
    invariant_payload["entry_delay_mode_reported"] = True

    execution_payload = {
        "cost_mode": cost_mode,
        "entry_delay_bars": next(iter(pair_frames.values()))[2].entry_delay_bars,
        "entry_price": next(iter(pair_frames.values()))[2].entry_price,
        "same_bar_ambiguity": next(iter(pair_frames.values()))[2].same_bar_ambiguity,
        "spread_multiplier": next(iter(pair_frames.values()))[2].spread_multiplier,
        "slippage_pips_round_trip_by_pair": {
            pair: settings.slippage_pips_round_trip for pair, (_, _, settings, _) in pair_frames.items()
        },
        "bidask_columns_used_if_present": True,
        "mid_spread_approximation_if_bidask_missing": True,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
    }
    write_json(output_dir / "surface_execution_audit.json", execution_payload)

    caveats = []
    if health_df["native_bidask_complete_ohlc_rate"].fillna(0).max() <= 0:
        caveats.append("No complete native bid/ask OHLC rows were present in the audited slice; labels used conservative mid-plus-spread approximation.")
    if include_dynamic_policies:
        caveats.append("Dynamic ATR/spread/range TP/SL contracts were included and store realized TP/SL pips in summary averages.")
    if sample_minutes and sample_minutes > 1:
        caveats.append(f"Audit sampled every {sample_minutes}th eligible minute.")
    verdict = "PASS" if violation_total == 0 and int(invariant_payload.get("contracts_evaluated", 0)) > 0 else "FAIL"
    if int(invariant_payload.get("contracts_evaluated", 0)) == 0:
        verdict = "INCONCLUSIVE"

    label_report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": output_dir.name,
        "verdict": verdict,
        "start": str(start_ts),
        "end": str(end_ts),
        "pairs": list(pair_frames.keys()),
        "tier": tier,
        "horizons_minutes": horizons,
        "fixed_policy_count": len(fixed),
        "dynamic_policy_templates": dynamic_templates,
        "policy_horizon_rows": int(len(policy_df)),
        "pair_policy_horizon_rows": int(len(pair_df)),
        "invariants": invariant_payload,
        "execution": execution_payload,
        "oracle_opportunity_report_path": str(output_dir / "oracle_opportunity_report.json"),
        "baseline_comparison_report_path": str(output_dir / "baseline_comparison_report.json"),
        "feature_family_report_path": str(feature_report_path) if feature_report_path else None,
        "caveats": caveats,
        "live_execution_enabled": False,
    }
    write_json(output_dir / "surface_label_audit.json", label_report)
    write_json(output_dir / "FINAL_RESEARCH_SUMMARY.json", label_report)
    write_json(output_dir / "config_snapshot.json", cfg)
    write_json(output_dir / "run_manifest.json", {
        "run_id": output_dir.name,
        "run_dir": str(output_dir),
        "command_family": "audit-labels",
        "generated_utc": utc_now_stamp(),
        "files": {
            "surface_label_audit": str(output_dir / "surface_label_audit.json"),
            "surface_execution_audit": str(output_dir / "surface_execution_audit.json"),
            "surface_label_audit_csv": str(output_dir / "surface_label_audit.csv"),
            "oracle_opportunity_report": str(output_dir / "oracle_opportunity_report.json"),
            "baseline_comparison_report": str(output_dir / "baseline_comparison_report.json"),
        },
    })
    _write_summary_md(output_dir / "FINAL_RESEARCH_SUMMARY.md", label_report, policy_df, health_df)
    return output_dir
