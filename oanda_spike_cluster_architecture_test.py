from __future__ import annotations

import json
import math
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parent
RESEARCH_DIR = ROOT / "data" / "oanda_training_manager" / "continuous_research"
REPORT_ROOT = ROOT / "data" / "technical_scout_manager" / "reports"
SOURCE_PARQUET = RESEARCH_DIR / "technical_spike_research.parquet"
CACHE_PARQUET = RESEARCH_DIR / "spike_cluster_architecture_features_60m.parquet"


LOCAL_NUMERIC_FEATURES = [
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "acceleration_15_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "ema8_minus_ema21_atr",
    "ema21_slope_15_atr",
    "ema55_slope_30_atr",
    "macd_atr",
    "macd_hist_atr",
    "ma_stack_score",
    "rsi14_centered",
    "atr15_to_atr240",
    "compression_30",
    "range_position_60_centered",
    "range_position_240_centered",
    "donchian_60_position_centered",
    "donchian_60_breakout_atr",
    "donchian_240_position_centered",
    "donchian_240_breakout_atr",
    "trend_consistency_60",
    "realized_vol_ratio_30_240",
    "abs_momentum_5_atr",
    "abs_momentum_15_atr",
    "abs_momentum_30_atr",
    "abs_momentum_60_atr",
    "momentum_5_15_delta_atr",
    "momentum_15_30_delta_atr",
    "momentum_30_60_delta_atr",
    "momentum_alignment_long",
    "momentum_alignment_short",
    "momentum_alignment_edge",
    "momentum_acceleration_pressure",
    "momentum_exhaustion_pressure",
    "macd_hist_abs_atr",
    "trend_pressure_long",
    "trend_pressure_short",
    "trend_pressure_edge",
    "rsi_overbought_pressure",
    "rsi_oversold_pressure",
    "rsi_extreme_abs",
    "donchian_breakout_abs_atr",
    "range60_extreme_abs",
    "range240_extreme_abs",
    "near_upper_range_score",
    "near_lower_range_score",
    "range_breakout_pressure_long",
    "range_breakout_pressure_short",
    "strength_gap_abs_15",
    "strength_gap_abs_60",
    "strength_gap_15_60_delta",
    "atr_spread_efficiency",
    "spread_to_atr240",
    "spread_cost_pressure",
    "compression_breakout_pressure",
    "compression_then_expansion",
    "volatility_expansion_pressure",
    "liquidity_quality_score",
    "anti_chase_pressure",
    "london_volatility_pressure",
    "ny_volatility_pressure",
    "overlap_breakout_pressure",
    "rollover_spread_penalty",
    "volatile_spread_penalty",
    "rule_pre_breakout_long",
    "rule_pre_breakout_short",
    "rule_breakout_continuation_long",
    "rule_breakout_continuation_short",
    "rule_exhaustion_reversal_long",
    "rule_exhaustion_reversal_short",
    "rule_session_timing_long",
    "rule_session_timing_short",
    "rule_cross_pressure_long",
    "rule_cross_pressure_short",
    "rule_liquidity_ok",
    "rule_anti_chase_penalty",
    "rule_baseline_long_score",
    "rule_baseline_short_score",
    "rule_baseline_direction_score",
    "rule_baseline_strength_score",
    "spread_ratio_60",
    "volume_z_30",
    "strength_gap_15",
    "strength_gap_60",
    "strength_gap_rank_15",
    "strength_gap_rank_60",
    "spread_pips",
    "atr240_pips",
    "shadow_m30_strength",
    "shadow_m15_strength",
    "shadow_m5_strength",
    "shadow_m15_m30",
    "shadow_m5_m30",
    "shadow_rule_count",
    "hour_sin",
    "hour_cos",
    "weekday",
    "weekday_sin",
    "weekday_cos",
    "month_sin",
    "month_cos",
    "day_of_month_sin",
    "day_of_month_cos",
    "day_of_year_sin",
    "day_of_year_cos",
    "week_of_year_sin",
    "week_of_year_cos",
    "week_of_month",
    "is_month_start",
    "is_month_end",
    "is_quarter_end",
    "is_asia_session",
    "is_london_session",
    "is_new_york_session",
    "is_london_ny_overlap",
    "is_rollover_hour",
    "instrument_volatility_percentile",
    "instrument_median_abs_60_pips",
    "instrument_q995_abs_60_pips",
    "instrument_q995_move_to_spread",
    "is_major_pair",
    "is_usd_pair",
    "is_non_usd_pair",
    "is_jpy_cross",
    "is_exotic_pair",
    "is_volatile_pair",
    "is_volatile_or_exotic_pair",
    "is_non_usd_volatile_pair",
    "pair_class_usd_major",
    "pair_class_usd_other",
    "pair_class_jpy_risk",
    "pair_class_commodity",
    "pair_class_eur_gbp_cross",
    "pair_class_chf_safe_haven",
    "pair_class_exotic_high_spread",
    "pair_class_volatile_non_usd",
    "pair_class_other_cross",
    "base_is_usd",
    "quote_is_usd",
    "base_is_jpy",
    "quote_is_jpy",
    "base_is_chf",
    "quote_is_chf",
    "base_is_commodity",
    "quote_is_commodity",
    "base_is_risk",
    "quote_is_risk",
    "carry_proxy_base_score",
    "carry_proxy_quote_score",
    "carry_proxy_diff",
    "carry_proxy_abs_diff",
    "carry_proxy_positive",
    "carry_proxy_negative",
]


CLUSTER_NUMERIC_FEATURES = [
    "regime_trend_strength",
    "regime_volatility_expansion_score",
    "regime_compression_score",
    "regime_spread_cost_score",
    "regime_usd_strength_15",
    "regime_usd_strength_60",
    "regime_jpy_strength_15",
    "regime_chf_strength_15",
    "regime_risk_on_score",
    "regime_commodity_score",
    "regime_acceleration_score",
    "regime_is_usd_trend",
    "regime_is_risk_on",
    "regime_is_risk_off",
    "regime_is_jpy_unwind",
    "regime_is_chf_safe_haven",
    "regime_is_commodity_trend",
    "regime_is_low_vol_chop",
    "regime_is_high_vol_event",
    "regime_is_spread_impaired",
    "macro_pair_bias",
    "macro_usd_bias",
    "macro_jpy_bias",
    "macro_chf_bias",
    "macro_risk_bias",
    "macro_commodity_bias",
    "macro_rate_diff_bias",
    "macro_news_risk",
    "macro_event_risk",
    "macro_bias_age_hours",
]


TARGET_COLUMNS = [
    "major_event_60",
    "major_direction_up_60",
    "profitable_long_move_60",
    "profitable_short_move_60",
    "profitable_any_move_60",
    "best_curve_net_atr_60",
    "best_curve_direction_up_60",
    "long_curve_net_pips_60",
    "short_curve_net_pips_60",
    "long_trailing_stop075_net_pips_60",
    "short_trailing_stop075_net_pips_60",
]


META_COLUMNS = [
    "time_utc",
    "instrument",
    "pair_taxonomy_primary",
    "regime_primary",
]


@dataclass(frozen=True)
class Fold:
    name: str
    train_end: str
    test_start: str
    test_end: str


FOLDS = [
    Fold("2026_jan_feb", "2025-12-31 23:59:59+00:00", "2026-01-01 00:00:00+00:00", "2026-02-28 23:59:59+00:00"),
    Fold("2026_mar_apr", "2026-02-28 23:59:59+00:00", "2026-03-01 00:00:00+00:00", "2026-04-30 23:59:59+00:00"),
    Fold("2026_may_jun", "2026-04-30 23:59:59+00:00", "2026-05-01 00:00:00+00:00", "2026-06-19 23:59:59+00:00"),
]


def existing_columns(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).schema.names


def safe_columns(cols: Iterable[str], existing: set[str]) -> list[str]:
    return [c for c in cols if c in existing]


def read_source() -> pd.DataFrame:
    if not SOURCE_PARQUET.exists():
        raise FileNotFoundError(SOURCE_PARQUET)
    existing = set(existing_columns(SOURCE_PARQUET))
    columns = safe_columns(META_COLUMNS + LOCAL_NUMERIC_FEATURES + CLUSTER_NUMERIC_FEATURES + TARGET_COLUMNS, existing)
    df = pd.read_parquet(SOURCE_PARQUET, columns=columns)
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
    df = df.sort_values(["time_utc", "instrument"]).reset_index(drop=True)
    return df


def read_or_build_features() -> pd.DataFrame:
    if CACHE_PARQUET.exists() and CACHE_PARQUET.stat().st_mtime >= SOURCE_PARQUET.stat().st_mtime:
        print(f"Reading cached cluster features: {CACHE_PARQUET}")
        df = pd.read_parquet(CACHE_PARQUET)
        df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
        return df.sort_values(["time_utc", "instrument"]).reset_index(drop=True)

    print(f"Reading source: {SOURCE_PARQUET}")
    df = read_source()
    print("Engineering same-timestamp market/currency cluster features...")
    df = add_market_cluster_features(df)
    print(f"Writing cached cluster features: {CACHE_PARQUET}")
    df.to_parquet(CACHE_PARQUET, index=False)
    return df


def split_instrument_parts(df: pd.DataFrame) -> pd.DataFrame:
    parts = df["instrument"].astype(str).str.split("_", n=1, expand=True)
    df["base_ccy"] = parts[0]
    df["quote_ccy"] = parts[1]
    return df


def add_market_cluster_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds only same-timestamp cross-pair features. These are live-available if the
    account scans all pairs at a scan time, and do not use future target labels.
    """
    df = split_instrument_parts(df)

    agg_spec: dict[str, tuple[str, str]] = {
        "market_pair_count": ("instrument", "count"),
        "market_abs_momentum15_mean": ("abs_momentum_15_atr", "mean"),
        "market_abs_momentum15_max": ("abs_momentum_15_atr", "max"),
        "market_abs_momentum60_mean": ("abs_momentum_60_atr", "mean"),
        "market_abs_momentum60_max": ("abs_momentum_60_atr", "max"),
        "market_spread_to_atr_mean": ("spread_to_atr240", "mean"),
        "market_spread_cost_pressure_mean": ("spread_cost_pressure", "mean"),
        "market_vol_expansion_mean": ("volatility_expansion_pressure", "mean"),
        "market_range_extreme_mean": ("range60_extreme_abs", "mean"),
        "market_long_rule_sum": ("rule_baseline_long_candidate", "sum"),
        "market_short_rule_sum": ("rule_baseline_short_candidate", "sum"),
        "market_exhaustion_long_sum": ("rule_exhaustion_reversal_long", "sum"),
        "market_exhaustion_short_sum": ("rule_exhaustion_reversal_short", "sum"),
    }
    agg_spec = {k: v for k, v in agg_spec.items() if v[0] in df.columns}
    market = df.groupby("time_utc", observed=True).agg(**agg_spec).reset_index()
    df = df.merge(market, on="time_utc", how="left", copy=False)

    for momentum_col in ["momentum_15_atr", "momentum_60_atr"]:
        if momentum_col not in df.columns:
            continue
        suffix = momentum_col.replace("momentum_", "ccy_pressure_").replace("_atr", "")
        base = df[["time_utc", "base_ccy", momentum_col]].rename(columns={"base_ccy": "ccy", momentum_col: "pressure"})
        quote = df[["time_utc", "quote_ccy", momentum_col]].rename(columns={"quote_ccy": "ccy", momentum_col: "pressure"})
        quote["pressure"] = -quote["pressure"]
        pressure = pd.concat([base, quote], ignore_index=True)
        pressure["abs_pressure"] = pressure["pressure"].abs()
        pressure = (
            pressure.groupby(["time_utc", "ccy"], observed=True)
            .agg(ccy_pressure=("pressure", "mean"), ccy_pressure_abs=("abs_pressure", "mean"))
            .reset_index()
        )
        pressure_base = pressure.rename(
            columns={
                "ccy": "base_ccy",
                "ccy_pressure": f"base_{suffix}",
                "ccy_pressure_abs": f"base_{suffix}_abs",
            }
        )
        pressure_quote = pressure.rename(
            columns={
                "ccy": "quote_ccy",
                "ccy_pressure": f"quote_{suffix}",
                "ccy_pressure_abs": f"quote_{suffix}_abs",
            }
        )
        df = df.merge(pressure_base, on=["time_utc", "base_ccy"], how="left", copy=False)
        df = df.merge(pressure_quote, on=["time_utc", "quote_ccy"], how="left", copy=False)
        df[f"pair_{suffix}"] = df[f"base_{suffix}"] - df[f"quote_{suffix}"]
        df[f"pair_{suffix}_abs"] = df[f"pair_{suffix}"].abs()

    return df


def one_hot(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    use = [c for c in columns if c in df.columns]
    if not use:
        return pd.DataFrame(index=df.index)
    return pd.get_dummies(df[use].astype("category"), prefix=use, dtype=np.float32)


def make_matrix(df: pd.DataFrame, mode: str, fit_columns: list[str] | None = None) -> tuple[pd.DataFrame, list[str]]:
    local_cols = [c for c in LOCAL_NUMERIC_FEATURES if c in df.columns]
    x = df[local_cols].copy()
    x = x.apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)

    categorical = ["instrument", "pair_taxonomy_primary"]
    if mode == "cluster":
        cluster_cols = [c for c in CLUSTER_NUMERIC_FEATURES if c in df.columns]
        engineered = [
            c
            for c in df.columns
            if c.startswith("market_")
            or c.startswith("base_ccy_pressure_")
            or c.startswith("quote_ccy_pressure_")
            or c.startswith("pair_ccy_pressure_")
        ]
        add_cols = cluster_cols + engineered
        add = df[add_cols].copy()
        add = add.apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        x = pd.concat([x, add], axis=1)
        categorical = ["instrument", "pair_taxonomy_primary", "regime_primary"]

    dummies = one_hot(df, categorical)
    x = pd.concat([x.astype(np.float32), dummies], axis=1)
    x = x.loc[:, ~x.columns.duplicated()]

    if fit_columns is None:
        return x, list(x.columns)

    missing = [c for c in fit_columns if c not in x.columns]
    for c in missing:
        x[c] = np.float32(0.0)
    extra = [c for c in x.columns if c not in set(fit_columns)]
    if extra:
        x = x.drop(columns=extra)
    return x[fit_columns], fit_columns


def sample_training_indices(y: pd.Series, max_pos: int = 80_000, max_neg: int = 160_000, seed: int = 7) -> np.ndarray:
    pos = y[y > 0].index.to_numpy()
    neg = y[y <= 0].index.to_numpy()
    rng = np.random.default_rng(seed)
    if len(pos) > max_pos:
        pos = rng.choice(pos, size=max_pos, replace=False)
    if len(neg) > max_neg:
        neg = rng.choice(neg, size=max_neg, replace=False)
    idx = np.concatenate([pos, neg])
    rng.shuffle(idx)
    return idx


def fit_hgb(x: pd.DataFrame, y: pd.Series):
    # Fast architecture screen. The objective here is not final champion training;
    # it is an apples-to-apples comparison of local vs cluster-aware feature sets.
    model = make_pipeline(
        StandardScaler(with_mean=False),
        SGDClassifier(
            loss="log_loss",
            penalty="elasticnet",
            l1_ratio=0.10,
            alpha=1e-5,
            max_iter=1500,
            tol=1e-4,
            class_weight="balanced",
            random_state=42,
        ),
    )
    model.fit(x, y.astype(int))
    return model


def pair_prior_scores(train: pd.DataFrame, test: pd.DataFrame, target: str) -> np.ndarray:
    global_rate = float(train[target].fillna(0).mean())
    rates = train.groupby("instrument", observed=True)[target].mean().to_dict()
    mapped = test["instrument"].astype(str).map(rates)
    return pd.to_numeric(mapped, errors="coerce").fillna(global_rate).to_numpy(dtype=float)


def safe_auc(y: np.ndarray, p: np.ndarray) -> float | None:
    if len(np.unique(y)) < 2:
        return None
    return float(roc_auc_score(y, p))


def classification_metrics(y: np.ndarray, p: np.ndarray, prefix: str = "") -> dict:
    y = np.asarray(y).astype(int)
    p = np.asarray(p).astype(float)
    base = float(y.mean()) if len(y) else 0.0
    ap = float(average_precision_score(y, p)) if y.sum() else 0.0
    out = {
        f"{prefix}rows": int(len(y)),
        f"{prefix}positives": int(y.sum()),
        f"{prefix}base_rate": base,
        f"{prefix}auc": safe_auc(y, p),
        f"{prefix}ap": ap,
        f"{prefix}ap_lift": (ap / base) if base > 0 else None,
    }
    for pct in [0.001, 0.0025, 0.005, 0.01]:
        n = max(1, int(math.ceil(len(p) * pct)))
        top = np.argpartition(-p, n - 1)[:n]
        hits = int(y[top].sum())
        out[f"{prefix}top_{pct:.4f}_n"] = int(n)
        out[f"{prefix}top_{pct:.4f}_precision"] = float(hits / n)
        out[f"{prefix}top_{pct:.4f}_recall"] = float(hits / max(1, y.sum()))
    return out


def directional_trade_metrics(test: pd.DataFrame, long_score: np.ndarray, short_score: np.ndarray, label: str) -> list[dict]:
    score = np.maximum(long_score, short_score)
    is_long = long_score >= short_score
    rows = []
    for pct in [0.001, 0.0025, 0.005, 0.01]:
        n = max(1, int(math.ceil(len(score) * pct)))
        idx = np.argpartition(-score, n - 1)[:n]
        pick = test.iloc[idx].copy()
        direction_long = is_long[idx]
        net = np.where(
            direction_long,
            pd.to_numeric(pick["long_trailing_stop075_net_pips_60"], errors="coerce").fillna(pick["long_curve_net_pips_60"]).fillna(0.0),
            pd.to_numeric(pick["short_trailing_stop075_net_pips_60"], errors="coerce").fillna(pick["short_curve_net_pips_60"]).fillna(0.0),
        )
        raw_total = float(np.nansum(net))
        win_rate = float(np.nanmean(net > 0)) if len(net) else 0.0

        pick["_score"] = score[idx]
        pick["_is_long"] = direction_long
        pick["_net"] = net
        pick["_bucket_60m"] = pick["time_utc"].dt.floor("60min")
        clustered = pick.sort_values(["_bucket_60m", "instrument", "_score"], ascending=[True, True, False])
        clustered = clustered.drop_duplicates(["instrument", "_bucket_60m"], keep="first")
        cnet = clustered["_net"].to_numpy(dtype=float)

        rows.append(
            {
                "model": label,
                "top_pct": pct,
                "selected_rows": int(n),
                "raw_net_pips": raw_total,
                "raw_avg_pips": float(np.nanmean(net)) if len(net) else 0.0,
                "raw_win_rate": win_rate,
                "clustered_trades": int(len(clustered)),
                "clustered_net_pips": float(np.nansum(cnet)),
                "clustered_avg_pips": float(np.nanmean(cnet)) if len(cnet) else 0.0,
                "clustered_win_rate": float(np.nanmean(cnet > 0)) if len(cnet) else 0.0,
                "major_event_rows_selected": int(pd.to_numeric(pick["major_event_60"], errors="coerce").fillna(0).sum()),
            }
        )
    return rows


def fold_masks(df: pd.DataFrame, fold: Fold) -> tuple[pd.Series, pd.Series]:
    train_end = pd.Timestamp(fold.train_end)
    test_start = pd.Timestamp(fold.test_start)
    test_end = pd.Timestamp(fold.test_end)
    train = df["time_utc"] <= train_end
    test = (df["time_utc"] >= test_start) & (df["time_utc"] <= test_end)
    return train, test


def run_fold(df: pd.DataFrame, fold: Fold) -> tuple[list[dict], list[dict]]:
    train_mask, test_mask = fold_masks(df, fold)
    train = df.loc[train_mask].copy()
    test = df.loc[test_mask].copy()
    if train.empty or test.empty:
        raise RuntimeError(f"empty fold {fold.name}: train={len(train)} test={len(test)}")

    metric_rows: list[dict] = []
    trade_rows: list[dict] = []

    target = "major_event_60"
    y_train = train[target].fillna(0).astype(int)
    y_test = test[target].fillna(0).astype(int).to_numpy()

    prior = pair_prior_scores(train, test, target)
    metric_rows.append({"fold": fold.name, "model": "pair_history_prior", **classification_metrics(y_test, prior)})

    for mode, model_name in [("local", "pair_local_pooled"), ("cluster", "cluster_aware_pooled")]:
        train_idx = sample_training_indices(y_train, seed=17 + len(metric_rows))
        train_sample = train.loc[train_idx]
        x_train, columns = make_matrix(train_sample, mode=mode)
        y_sub = y_train.loc[train_idx]
        x_test, _ = make_matrix(test, mode=mode, fit_columns=columns)
        model = fit_hgb(x_train, y_sub)
        pred = model.predict_proba(x_test)[:, 1]
        metric_rows.append({"fold": fold.name, "model": model_name, **classification_metrics(y_test, pred)})

    for mode, model_name in [("local", "pair_local_directional"), ("cluster", "cluster_aware_directional")]:
        long_y = train["profitable_long_move_60"].fillna(0).astype(int)
        short_y = train["profitable_short_move_60"].fillna(0).astype(int)
        long_idx = sample_training_indices(long_y, max_pos=80_000, max_neg=200_000, seed=101 + len(trade_rows))
        short_idx = sample_training_indices(short_y, max_pos=80_000, max_neg=200_000, seed=201 + len(trade_rows))

        x_long_train, long_columns = make_matrix(train.loc[long_idx], mode=mode)
        x_long_test, _ = make_matrix(test, mode=mode, fit_columns=long_columns)
        long_model = fit_hgb(x_long_train, long_y.loc[long_idx])
        long_score = long_model.predict_proba(x_long_test)[:, 1]

        x_short_train, short_columns = make_matrix(train.loc[short_idx], mode=mode)
        x_short_test, _ = make_matrix(test, mode=mode, fit_columns=short_columns)
        short_model = fit_hgb(x_short_train, short_y.loc[short_idx])
        short_score = short_model.predict_proba(x_short_test)[:, 1]
        long_metrics = classification_metrics(test["profitable_long_move_60"].fillna(0).astype(int).to_numpy(), long_score, prefix="long_")
        short_metrics = classification_metrics(test["profitable_short_move_60"].fillna(0).astype(int).to_numpy(), short_score, prefix="short_")
        metric_rows.append({"fold": fold.name, "model": model_name, **long_metrics, **short_metrics})
        for row in directional_trade_metrics(test, long_score, short_score, model_name):
            row["fold"] = fold.name
            trade_rows.append(row)

    return metric_rows, trade_rows


def aggregate_report(metrics: pd.DataFrame, trades: pd.DataFrame, df: pd.DataFrame) -> dict:
    cls = metrics[~metrics["model"].str.contains("directional", na=False)].copy()
    trade = trades.copy()

    by_model = []
    for model, g in cls.groupby("model", observed=True):
        by_model.append(
            {
                "model": model,
                "folds": int(g["fold"].nunique()),
                "avg_auc": float(pd.to_numeric(g["auc"], errors="coerce").mean()),
                "avg_ap": float(pd.to_numeric(g["ap"], errors="coerce").mean()),
                "avg_ap_lift": float(pd.to_numeric(g["ap_lift"], errors="coerce").mean()),
                "avg_top_0p25pct_precision": float(pd.to_numeric(g["top_0.0025_precision"], errors="coerce").mean()),
                "avg_top_0p25pct_recall": float(pd.to_numeric(g["top_0.0025_recall"], errors="coerce").mean()),
            }
        )

    trade_summary = []
    if not trade.empty:
        for (model, pct), g in trade.groupby(["model", "top_pct"], observed=True):
            trade_summary.append(
                {
                    "model": model,
                    "top_pct": float(pct),
                    "folds": int(g["fold"].nunique()),
                    "clustered_net_pips": float(g["clustered_net_pips"].sum()),
                    "clustered_trades": int(g["clustered_trades"].sum()),
                    "clustered_avg_pips": float(g["clustered_net_pips"].sum() / max(1, g["clustered_trades"].sum())),
                    "avg_clustered_win_rate": float(g["clustered_win_rate"].mean()),
                    "raw_net_pips": float(g["raw_net_pips"].sum()),
                }
            )

    horizon = df[["time_utc", "instrument", "major_event_60"]].copy()
    horizon["_day"] = horizon["time_utc"].dt.date
    horizon["_hour"] = horizon["time_utc"].dt.floor("60min")
    events = horizon[horizon["major_event_60"].fillna(0).astype(int) > 0]
    day_counts = events.groupby("_day", observed=True)["instrument"].nunique()
    hour_counts = events.groupby("_hour", observed=True)["instrument"].nunique()
    cluster_stats = {
        "major_event_60_rows": int(len(events)),
        "major_event_60_instruments": int(events["instrument"].nunique()),
        "active_days": int(day_counts.shape[0]),
        "median_instruments_per_event_day": float(day_counts.median()) if len(day_counts) else 0.0,
        "p90_instruments_per_event_day": float(day_counts.quantile(0.90)) if len(day_counts) else 0.0,
        "max_instruments_per_event_day": int(day_counts.max()) if len(day_counts) else 0,
        "singleton_event_hours_share": float((hour_counts == 1).mean()) if len(hour_counts) else 0.0,
        "multi_pair_event_hours_share": float((hour_counts >= 2).mean()) if len(hour_counts) else 0.0,
        "five_plus_pair_event_hours_share": float((hour_counts >= 5).mean()) if len(hour_counts) else 0.0,
    }

    return {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(SOURCE_PARQUET),
        "rows": int(len(df)),
        "date_min": str(df["time_utc"].min()),
        "date_max": str(df["time_utc"].max()),
        "folds": [fold.__dict__ for fold in FOLDS],
        "cluster_stats": cluster_stats,
        "classification_summary": by_model,
        "trade_summary": sorted(trade_summary, key=lambda r: (r["model"], r["top_pct"])),
    }


def write_report(out_dir: Path, summary: dict, metrics: pd.DataFrame, trades: pd.DataFrame) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(out_dir / "cluster_architecture_metrics.csv", index=False)
    trades.to_csv(out_dir / "cluster_architecture_trade_sim.csv", index=False)
    (out_dir / "cluster_architecture_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Spike cluster architecture test\n")
    lines.append(f"Created UTC: {summary['created_utc']}\n")
    lines.append(f"Rows: {summary['rows']:,} from {summary['date_min']} to {summary['date_max']}\n")
    cs = summary["cluster_stats"]
    lines.append("## Cluster structure\n")
    lines.append(
        f"- Major 60m event rows: {cs['major_event_60_rows']:,} across {cs['major_event_60_instruments']} instruments.\n"
    )
    lines.append(
        f"- Event-day breadth: median {cs['median_instruments_per_event_day']:.1f}, p90 {cs['p90_instruments_per_event_day']:.1f}, max {cs['max_instruments_per_event_day']} instruments.\n"
    )
    lines.append(
        f"- Event-hour breadth: singleton {cs['singleton_event_hours_share']:.1%}, multi-pair {cs['multi_pair_event_hours_share']:.1%}, five-plus pair {cs['five_plus_pair_event_hours_share']:.1%}.\n"
    )

    lines.append("\n## Major-spike classifier summary\n")
    cls = pd.DataFrame(summary["classification_summary"])
    if not cls.empty:
        lines.append(cls.to_markdown(index=False, floatfmt=".4f"))
        lines.append("\n")

    lines.append("\n## Directional 60m trailing-stop trade simulation\n")
    tr = pd.DataFrame(summary["trade_summary"])
    if not tr.empty:
        lines.append(tr.to_markdown(index=False, floatfmt=".4f"))
        lines.append("\n")

    lines.append("\n## Interpretation\n")
    lines.append(
        "- If the cluster-aware model materially improves AP/top-bucket precision over pair-local, grouped/regime information is predictive and should be the scout trigger layer.\n"
    )
    lines.append(
        "- If pair-local remains close but trade simulation improves with cluster-aware, use global cluster detection for arming and pair-specific execution for sizing/exits.\n"
    )
    lines.append(
        "- If pair-history prior is competitive, spikes are mostly pair-idiosyncratic. If it is weak, pair identity alone is not enough.\n"
    )
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")

    latest_md = REPORT_ROOT / "latest_spike_cluster_architecture_test.md"
    latest_json = REPORT_ROOT / "latest_spike_cluster_architecture_test.json"
    latest_md.write_text((out_dir / "summary.md").read_text(encoding="utf-8"), encoding="utf-8")
    latest_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> int:
    warnings.filterwarnings("ignore", category=RuntimeWarning)
    df = read_or_build_features()

    metric_rows: list[dict] = []
    trade_rows: list[dict] = []
    for fold in FOLDS:
        print(f"Running fold {fold.name}...")
        m, t = run_fold(df, fold)
        metric_rows.extend(m)
        trade_rows.extend(t)

    metrics = pd.DataFrame(metric_rows)
    trades = pd.DataFrame(trade_rows)
    summary = aggregate_report(metrics, trades, df)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = REPORT_ROOT / f"spike_cluster_architecture_test_{stamp}"
    write_report(out_dir, summary, metrics, trades)
    print(f"Wrote {out_dir}")
    print(f"Wrote {REPORT_ROOT / 'latest_spike_cluster_architecture_test.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
