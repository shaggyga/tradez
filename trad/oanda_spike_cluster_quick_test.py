from __future__ import annotations

import json
import math
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parent
RESEARCH_DIR = ROOT / "data" / "oanda_training_manager" / "continuous_research"
REPORT_ROOT = ROOT / "data" / "technical_scout_manager" / "reports"
CACHE_PARQUET = RESEARCH_DIR / "spike_cluster_architecture_features_60m.parquet"
SOURCE_PARQUET = RESEARCH_DIR / "technical_spike_research.parquet"


META = [
    "time_utc",
    "instrument",
    "base_ccy",
    "quote_ccy",
    "pair_taxonomy_primary",
    "regime_primary",
    "is_volatile_or_exotic_pair",
]

TARGETS = [
    "major_event_60",
    "profitable_long_move_60",
    "profitable_short_move_60",
    "profitable_any_move_60",
    "long_trailing_stop075_net_pips_60",
    "short_trailing_stop075_net_pips_60",
    "long_curve_net_pips_60",
    "short_curve_net_pips_60",
]

LOCAL_FEATURES = [
    "momentum_5_atr",
    "momentum_15_atr",
    "momentum_30_atr",
    "momentum_60_atr",
    "acceleration_15_atr",
    "sma7_minus_8_atr",
    "sma30_slope_5_atr",
    "ema8_minus_ema21_atr",
    "ema21_slope_15_atr",
    "macd_atr",
    "macd_hist_atr",
    "ma_stack_score",
    "rsi14_centered",
    "atr15_to_atr240",
    "compression_30",
    "range_position_60_centered",
    "range_position_240_centered",
    "trend_consistency_60",
    "realized_vol_ratio_30_240",
    "abs_momentum_5_atr",
    "abs_momentum_15_atr",
    "abs_momentum_30_atr",
    "abs_momentum_60_atr",
    "momentum_alignment_edge",
    "momentum_acceleration_pressure",
    "momentum_exhaustion_pressure",
    "macd_hist_abs_atr",
    "trend_pressure_long",
    "trend_pressure_short",
    "trend_pressure_edge",
    "rsi_extreme_abs",
    "range60_extreme_abs",
    "range240_extreme_abs",
    "near_upper_range_score",
    "near_lower_range_score",
    "range_breakout_pressure_long",
    "range_breakout_pressure_short",
    "strength_gap_abs_15",
    "strength_gap_abs_60",
    "spread_to_atr240",
    "spread_cost_pressure",
    "compression_breakout_pressure",
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
    "rule_cross_pressure_long",
    "rule_cross_pressure_short",
    "rule_baseline_long_score",
    "rule_baseline_short_score",
    "rule_baseline_direction_score",
    "rule_baseline_strength_score",
    "spread_pips",
    "atr240_pips",
    "spread_ratio_60",
    "volume_z_30",
    "shadow_m30_strength",
    "shadow_m15_strength",
    "shadow_m5_strength",
    "shadow_m15_m30",
    "shadow_m5_m30",
    "shadow_rule_count",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
    "month_sin",
    "month_cos",
    "week_of_month",
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
    "is_jpy_cross",
    "is_exotic_pair",
    "is_volatile_pair",
    "is_non_usd_volatile_pair",
    "pair_class_usd_major",
    "pair_class_jpy_risk",
    "pair_class_commodity",
    "pair_class_chf_safe_haven",
    "pair_class_exotic_high_spread",
    "pair_class_volatile_non_usd",
    "carry_proxy_diff",
    "carry_proxy_abs_diff",
]

CLUSTER_FEATURES = [
    "market_pair_count",
    "market_abs_momentum15_mean",
    "market_abs_momentum15_max",
    "market_abs_momentum60_mean",
    "market_abs_momentum60_max",
    "market_spread_to_atr_mean",
    "market_spread_cost_pressure_mean",
    "market_vol_expansion_mean",
    "market_range_extreme_mean",
    "market_long_rule_sum",
    "market_short_rule_sum",
    "market_exhaustion_long_sum",
    "market_exhaustion_short_sum",
    "base_ccy_pressure_15",
    "base_ccy_pressure_15_abs",
    "quote_ccy_pressure_15",
    "quote_ccy_pressure_15_abs",
    "pair_ccy_pressure_15",
    "pair_ccy_pressure_15_abs",
    "base_ccy_pressure_60",
    "base_ccy_pressure_60_abs",
    "quote_ccy_pressure_60",
    "quote_ccy_pressure_60_abs",
    "pair_ccy_pressure_60",
    "pair_ccy_pressure_60_abs",
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
]

FOLDS = [
    ("2026_jan_feb", "2025-12-31 23:59:59+00:00", "2026-01-01 00:00:00+00:00", "2026-02-28 23:59:59+00:00"),
    ("2026_mar_apr", "2026-02-28 23:59:59+00:00", "2026-03-01 00:00:00+00:00", "2026-04-30 23:59:59+00:00"),
    ("2026_may_jun", "2026-04-30 23:59:59+00:00", "2026-05-01 00:00:00+00:00", "2026-06-19 23:59:59+00:00"),
]

ACCOUNT_NAV_USD = 10.0
MARGIN_FRACTION = 0.70

FALLBACK_USD_PER_CCY = {
    "USD": 1.0,
    "EUR": 1.135,
    "GBP": 1.316,
    "CHF": 1.22,
    "JPY": 1.0 / 145.0,
    "AUD": 0.65,
    "NZD": 0.60,
    "CAD": 0.73,
    "SGD": 0.78,
    "HKD": 1.0 / 7.85,
    "ZAR": 1.0 / 16.57,
    "MXN": 1.0 / 17.62,
    "TRY": 1.0 / 46.50,
    "CNH": 1.0 / 7.17,
    "CZK": 0.046,
    "PLN": 0.30,
    "NOK": 0.10,
    "SEK": 0.105,
    "DKK": 0.15,
    "HUF": 0.0029,
    "THB": 0.030,
}

FALLBACK_MARGIN_BY_CCY = {
    "TRY": 0.25,
    "CNH": 0.10,
    "MXN": 0.10,
    "ZAR": 0.07,
    "HKD": 0.05,
    "SGD": 0.05,
    "CZK": 0.05,
    "PLN": 0.05,
    "HUF": 0.05,
    "NOK": 0.05,
    "SEK": 0.05,
    "DKK": 0.05,
    "THB": 0.10,
}


def available_columns(path: Path) -> list[str]:
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).schema.names


def parse_creds() -> dict[str, str]:
    creds = ROOT / "creds"
    if not creds.exists():
        return {}
    text = creds.read_text(encoding="utf-8", errors="ignore")
    out: dict[str, str] = {}
    for key in [
        "OANDA_LIVE_API_KEY",
        "OANDA_ACCOUNT_LIVE_TECH",
        "OANDA_ACCOUNT_LIVE_MAIN",
        "OANDA_ACCOUNT_LIVE_PRIMARY",
    ]:
        m = re.search(rf"^\s*{re.escape(key)}\s*=\s*[\"']([^\"']+)[\"']", text, re.M)
        if m:
            out[key] = m.group(1).strip()
    return out


def oanda_get_json(path: str, token: str, timeout: int = 20) -> dict[str, Any]:
    url = "https://api-fxtrade.oanda.com/v3/" + path.lstrip("/")
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def chunked(items: list[str], n: int) -> list[list[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def fetch_oanda_value_metadata(instruments: list[str]) -> tuple[dict[str, dict[str, float]], dict[str, float], str]:
    creds = parse_creds()
    token = creds.get("OANDA_LIVE_API_KEY", "")
    account = creds.get("OANDA_ACCOUNT_LIVE_TECH") or creds.get("OANDA_ACCOUNT_LIVE_MAIN") or creds.get("OANDA_ACCOUNT_LIVE_PRIMARY")
    if not token or not account:
        return {}, FALLBACK_USD_PER_CCY.copy(), "fallback_no_creds"

    try:
        inst_payload = oanda_get_json(f"accounts/{account}/instruments", token)
        all_meta = {
            item.get("name"): item
            for item in inst_payload.get("instruments", [])
            if isinstance(item, dict) and item.get("name")
        }
        all_names = set(all_meta)
        currencies = set()
        for inst in instruments:
            if "_" in inst:
                a, b = inst.split("_", 1)
                currencies.add(a)
                currencies.add(b)
        conversion_pairs = set()
        for ccy in currencies:
            if ccy == "USD":
                continue
            if f"{ccy}_USD" in all_names:
                conversion_pairs.add(f"{ccy}_USD")
            if f"USD_{ccy}" in all_names:
                conversion_pairs.add(f"USD_{ccy}")

        price_names = sorted((set(instruments) | conversion_pairs) & all_names)
        prices: dict[str, float] = {}
        for group in chunked(price_names, 45):
            query = urllib.parse.urlencode({"instruments": ",".join(group)})
            payload = oanda_get_json(f"accounts/{account}/pricing?{query}", token)
            for px in payload.get("prices", []):
                try:
                    name = px.get("instrument")
                    bid = float(px["closeoutBid"])
                    ask = float(px["closeoutAsk"])
                    if name:
                        prices[name] = (bid + ask) / 2.0
                except Exception:
                    continue

        usd_per_ccy = FALLBACK_USD_PER_CCY.copy()
        for ccy in currencies:
            if ccy == "USD":
                usd_per_ccy[ccy] = 1.0
                continue
            direct = f"{ccy}_USD"
            inverse = f"USD_{ccy}"
            if direct in prices and prices[direct] > 0:
                usd_per_ccy[ccy] = prices[direct]
            elif inverse in prices and prices[inverse] > 0:
                usd_per_ccy[ccy] = 1.0 / prices[inverse]

        meta: dict[str, dict[str, float]] = {}
        for inst in instruments:
            item = all_meta.get(inst, {})
            if "_" not in inst:
                continue
            base, quote = inst.split("_", 1)
            pip_location = int(item.get("pipLocation", -2 if quote == "JPY" else -4))
            pip_size = float(10 ** pip_location)
            margin_rate = float(item.get("marginRate", fallback_margin_rate(inst)))
            meta[inst] = {
                "pip_size": pip_size,
                "margin_rate": margin_rate,
                "mid": prices.get(inst, fallback_pair_mid(inst, usd_per_ccy)),
                "base_usd": usd_per_ccy.get(base, FALLBACK_USD_PER_CCY.get(base, 1.0)),
                "quote_usd": usd_per_ccy.get(quote, FALLBACK_USD_PER_CCY.get(quote, 1.0)),
            }
        return meta, usd_per_ccy, "oanda_live_readonly"
    except Exception as exc:
        return {}, FALLBACK_USD_PER_CCY.copy(), f"fallback_oanda_error:{type(exc).__name__}"


def fallback_margin_rate(instrument: str) -> float:
    if "_" not in instrument:
        return 0.05
    base, quote = instrument.split("_", 1)
    if base in FALLBACK_MARGIN_BY_CCY:
        return FALLBACK_MARGIN_BY_CCY[base]
    if quote in FALLBACK_MARGIN_BY_CCY:
        return FALLBACK_MARGIN_BY_CCY[quote]
    if "JPY" in {base, quote} or "CHF" in {base, quote}:
        return 0.0333
    return 0.02


def fallback_pair_mid(instrument: str, usd_per_ccy: dict[str, float]) -> float:
    if "_" not in instrument:
        return 1.0
    base, quote = instrument.split("_", 1)
    base_usd = usd_per_ccy.get(base, FALLBACK_USD_PER_CCY.get(base, 1.0))
    quote_usd = usd_per_ccy.get(quote, FALLBACK_USD_PER_CCY.get(quote, 1.0))
    return base_usd / max(quote_usd, 1e-12)


def build_value_table(instruments: list[str]) -> tuple[pd.DataFrame, str]:
    meta, usd_per_ccy, source = fetch_oanda_value_metadata(instruments)
    rows = []
    for inst in instruments:
        if "_" not in inst:
            continue
        base, quote = inst.split("_", 1)
        info = meta.get(inst)
        if not info:
            info = {
                "pip_size": 0.01 if quote == "JPY" else 0.0001,
                "margin_rate": fallback_margin_rate(inst),
                "mid": fallback_pair_mid(inst, usd_per_ccy),
                "base_usd": usd_per_ccy.get(base, FALLBACK_USD_PER_CCY.get(base, 1.0)),
                "quote_usd": usd_per_ccy.get(quote, FALLBACK_USD_PER_CCY.get(quote, 1.0)),
            }
        margin_per_unit = max(float(info["base_usd"]) * float(info["margin_rate"]), 1e-12)
        units = math.floor((ACCOUNT_NAV_USD * MARGIN_FRACTION) / margin_per_unit)
        pip_value_usd_per_unit = float(info["pip_size"]) * float(info["quote_usd"])
        usd_per_pip_at_budget = units * pip_value_usd_per_unit
        rows.append(
            {
                "instrument": inst,
                "pip_size": float(info["pip_size"]),
                "margin_rate": float(info["margin_rate"]),
                "base_usd": float(info["base_usd"]),
                "quote_usd": float(info["quote_usd"]),
                "estimated_units_10usd_70pct_margin": int(units),
                "usd_per_pip_at_budget": float(usd_per_pip_at_budget),
            }
        )
    return pd.DataFrame(rows), source


def ensure_cache() -> Path:
    if CACHE_PARQUET.exists():
        return CACHE_PARQUET
    raise FileNotFoundError(
        f"{CACHE_PARQUET} does not exist. Run oanda_spike_cluster_architecture_test.py once to build same-timestamp cluster features."
    )


def load_frame() -> pd.DataFrame:
    path = ensure_cache()
    existing = set(available_columns(path))
    cols = [c for c in META + TARGETS + LOCAL_FEATURES + CLUSTER_FEATURES if c in existing]
    print(f"Reading {len(cols)} columns from {path}", flush=True)
    df = pd.read_parquet(path, columns=cols)
    df["time_utc"] = pd.to_datetime(df["time_utc"], utc=True)
    df = df[df["time_utc"] >= pd.Timestamp("2025-01-01", tz="UTC")].copy()
    if "is_volatile_or_exotic_pair" in df.columns:
        df = df[pd.to_numeric(df["is_volatile_or_exotic_pair"], errors="coerce").fillna(0) > 0].copy()
    if "base_ccy" not in df.columns or "quote_ccy" not in df.columns:
        parts = df["instrument"].astype(str).str.split("_", n=1, expand=True)
        df["base_ccy"] = parts[0]
        df["quote_ccy"] = parts[1]
    df = df.sort_values(["time_utc", "instrument"]).reset_index(drop=True)
    print(
        f"Loaded {len(df):,} volatile/exotic rows, {df['instrument'].nunique()} instruments, "
        f"{df['time_utc'].min()} to {df['time_utc'].max()}",
        flush=True,
    )
    return df


def attach_value_weights(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    value_table, source = build_value_table(sorted(df["instrument"].dropna().astype(str).unique().tolist()))
    if value_table.empty:
        df["usd_per_pip_at_budget"] = 0.0
        return df, value_table, source
    df = df.merge(
        value_table[["instrument", "usd_per_pip_at_budget", "estimated_units_10usd_70pct_margin", "margin_rate"]],
        on="instrument",
        how="left",
        copy=False,
    )
    df["usd_per_pip_at_budget"] = pd.to_numeric(df["usd_per_pip_at_budget"], errors="coerce").fillna(0.0)
    df["estimated_units_10usd_70pct_margin"] = pd.to_numeric(
        df["estimated_units_10usd_70pct_margin"], errors="coerce"
    ).fillna(0.0)
    df["margin_rate"] = pd.to_numeric(df["margin_rate"], errors="coerce").fillna(0.05)
    # Pre-trade, non-leaky ranking proxy: score can be multiplied by a pair's
    # historical 60m capacity and USD-per-pip. Realized USD still uses actual
    # selected future pips.
    q995_src = df["instrument_q995_abs_60_pips"] if "instrument_q995_abs_60_pips" in df.columns else pd.Series(0.0, index=df.index)
    spread_src = df["spread_pips"] if "spread_pips" in df.columns else pd.Series(0.0, index=df.index)
    q995 = pd.to_numeric(q995_src, errors="coerce").fillna(0.0)
    spread = pd.to_numeric(spread_src, errors="coerce").fillna(0.0)
    df["value_proxy_usd_60"] = (q995 - spread.clip(lower=0.0)).clip(lower=0.0) * df["usd_per_pip_at_budget"]
    return df, value_table, source


def make_x(df: pd.DataFrame, feature_cols: list[str], categorical_cols: list[str], fit_cols: list[str] | None = None):
    use = [c for c in feature_cols if c in df.columns]
    x = df[use].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0).astype(np.float32)
    cats = [c for c in categorical_cols if c in df.columns]
    if cats:
        d = pd.get_dummies(df[cats].astype("category"), prefix=cats, dtype=np.float32)
        x = pd.concat([x, d], axis=1)
    x = x.loc[:, ~x.columns.duplicated()]
    if fit_cols is None:
        return x, list(x.columns)
    missing = [c for c in fit_cols if c not in x.columns]
    for c in missing:
        x[c] = np.float32(0.0)
    extra = [c for c in x.columns if c not in set(fit_cols)]
    if extra:
        x = x.drop(columns=extra)
    return x[fit_cols], fit_cols


def sample_idx(y: pd.Series, max_neg: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    pos = y[y > 0].index.to_numpy()
    neg = y[y <= 0].index.to_numpy()
    if len(neg) > max_neg:
        neg = rng.choice(neg, max_neg, replace=False)
    idx = np.concatenate([pos, neg])
    rng.shuffle(idx)
    return idx


def fit_predict(train: pd.DataFrame, test: pd.DataFrame, target: str, mode: str, seed: int) -> np.ndarray:
    features = LOCAL_FEATURES if mode == "local" else LOCAL_FEATURES + CLUSTER_FEATURES
    cats = ["instrument", "pair_taxonomy_primary"] if mode == "local" else ["instrument", "pair_taxonomy_primary", "regime_primary"]
    y = train[target].fillna(0).astype(int)
    if y.sum() < 5 or y.nunique() < 2:
        return np.full(len(test), float(y.mean()) if len(y) else 0.0)
    idx = sample_idx(y, max_neg=60_000, seed=seed)
    x_train, cols = make_x(train.loc[idx], features, cats)
    x_test, _ = make_x(test, features, cats, fit_cols=cols)
    model = make_pipeline(
        StandardScaler(with_mean=False),
        SGDClassifier(
            loss="log_loss",
            penalty="elasticnet",
            l1_ratio=0.05,
            alpha=2e-5,
            max_iter=900,
            tol=1e-4,
            class_weight="balanced",
            random_state=seed,
        ),
    )
    model.fit(x_train, y.loc[idx])
    return model.predict_proba(x_test)[:, 1]


def prior_predict(train: pd.DataFrame, test: pd.DataFrame, target: str) -> np.ndarray:
    global_rate = float(train[target].fillna(0).mean())
    rates = train.groupby("instrument", observed=True)[target].mean().to_dict()
    return pd.to_numeric(test["instrument"].astype(str).map(rates), errors="coerce").fillna(global_rate).to_numpy(float)


def cls_metrics(y, p) -> dict:
    y = np.asarray(y).astype(int)
    p = np.asarray(p).astype(float)
    base = float(y.mean()) if len(y) else 0.0
    ap = float(average_precision_score(y, p)) if y.sum() else 0.0
    out = {
        "rows": int(len(y)),
        "positives": int(y.sum()),
        "base_rate": base,
        "auc": float(roc_auc_score(y, p)) if len(np.unique(y)) > 1 else None,
        "ap": ap,
        "ap_lift": ap / base if base else None,
    }
    for pct in [0.001, 0.0025, 0.005, 0.01]:
        n = max(1, int(math.ceil(len(p) * pct)))
        idx = np.argpartition(-p, n - 1)[:n]
        hits = int(y[idx].sum())
        out[f"top_{pct:.4f}_precision"] = float(hits / n)
        out[f"top_{pct:.4f}_recall"] = float(hits / max(1, y.sum()))
    return out


def trade_metrics(test: pd.DataFrame, long_score: np.ndarray, short_score: np.ndarray, model: str, rank_mode: str) -> list[dict]:
    base_score = np.maximum(long_score, short_score)
    value_proxy = pd.to_numeric(test.get("value_proxy_usd_60", 0.0), errors="coerce").fillna(0.0).to_numpy(float)
    if rank_mode == "value_proxy_rank":
        score = base_score * np.maximum(value_proxy, 0.0)
    else:
        score = base_score
    is_long = long_score >= short_score
    rows = []
    for pct in [0.001, 0.0025, 0.005, 0.01]:
        n = max(1, int(math.ceil(len(score) * pct)))
        idx = np.argpartition(-score, n - 1)[:n]
        pick = test.iloc[idx].copy()
        direction_long = is_long[idx]
        long_net = pd.to_numeric(pick.get("long_trailing_stop075_net_pips_60"), errors="coerce")
        short_net = pd.to_numeric(pick.get("short_trailing_stop075_net_pips_60"), errors="coerce")
        if long_net.isna().all():
            long_net = pd.to_numeric(pick.get("long_curve_net_pips_60"), errors="coerce")
        if short_net.isna().all():
            short_net = pd.to_numeric(pick.get("short_curve_net_pips_60"), errors="coerce")
        net = np.where(direction_long, long_net.fillna(0.0), short_net.fillna(0.0))
        usd_per_pip = pd.to_numeric(pick.get("usd_per_pip_at_budget", 0.0), errors="coerce").fillna(0.0).to_numpy(float)
        net_usd = net * usd_per_pip
        pick["_net"] = net
        pick["_net_usd"] = net_usd
        pick["_score"] = score[idx]
        pick["_bucket_60m"] = pick["time_utc"].dt.floor("60min")
        clustered = (
            pick.sort_values(["_bucket_60m", "instrument", "_score"], ascending=[True, True, False])
            .drop_duplicates(["instrument", "_bucket_60m"], keep="first")
        )
        cnet = clustered["_net"].to_numpy(float)
        cnet_usd = clustered["_net_usd"].to_numpy(float)
        rows.append(
            {
                "model": model,
                "rank_mode": rank_mode,
                "top_pct": pct,
                "rows_selected": int(n),
                "raw_net_pips": float(np.nansum(net)),
                "raw_net_usd_10usd_70pct_margin": float(np.nansum(net_usd)),
                "raw_avg_pips": float(np.nanmean(net)) if len(net) else 0.0,
                "raw_avg_usd": float(np.nanmean(net_usd)) if len(net_usd) else 0.0,
                "raw_win_rate": float(np.nanmean(net > 0)) if len(net) else 0.0,
                "clustered_trades": int(len(clustered)),
                "clustered_net_pips": float(np.nansum(cnet)),
                "clustered_net_usd_10usd_70pct_margin": float(np.nansum(cnet_usd)),
                "clustered_avg_pips": float(np.nanmean(cnet)) if len(cnet) else 0.0,
                "clustered_avg_usd": float(np.nanmean(cnet_usd)) if len(cnet_usd) else 0.0,
                "clustered_return_pct_on_10usd": float(np.nansum(cnet_usd) / ACCOUNT_NAV_USD * 100.0),
                "clustered_win_rate": float(np.nanmean(cnet > 0)) if len(cnet) else 0.0,
                "major_event_rows_selected": int(pd.to_numeric(pick["major_event_60"], errors="coerce").fillna(0).sum()),
            }
        )
    return rows


def cluster_structure(df: pd.DataFrame) -> dict:
    ev = df[df["major_event_60"].fillna(0).astype(int) > 0].copy()
    ev["day"] = ev["time_utc"].dt.date
    ev["hour"] = ev["time_utc"].dt.floor("60min")
    day_pair = ev.groupby("day", observed=True)["instrument"].nunique()
    hour_pair = ev.groupby("hour", observed=True)["instrument"].nunique()
    pair_counts = ev["instrument"].value_counts()
    return {
        "events": int(len(ev)),
        "instruments": int(ev["instrument"].nunique()),
        "active_days": int(day_pair.shape[0]),
        "top10_pair_event_share": float(pair_counts.head(10).sum() / max(1, len(ev))),
        "top20_pair_event_share": float(pair_counts.head(20).sum() / max(1, len(ev))),
        "median_pairs_per_event_day": float(day_pair.median()) if len(day_pair) else 0.0,
        "p90_pairs_per_event_day": float(day_pair.quantile(0.90)) if len(day_pair) else 0.0,
        "max_pairs_per_event_day": int(day_pair.max()) if len(day_pair) else 0,
        "singleton_event_hour_share": float((hour_pair == 1).mean()) if len(hour_pair) else 0.0,
        "multi_pair_event_hour_share": float((hour_pair >= 2).mean()) if len(hour_pair) else 0.0,
        "five_plus_pair_event_hour_share": float((hour_pair >= 5).mean()) if len(hour_pair) else 0.0,
    }


def markdown_table(df: pd.DataFrame, floatfmt: str = ".4f") -> str:
    if df.empty:
        return "(empty)"
    formatted = df.copy()
    for col in formatted.columns:
        if pd.api.types.is_float_dtype(formatted[col]):
            formatted[col] = formatted[col].map(lambda v: "" if pd.isna(v) else format(float(v), floatfmt))
    cols = list(formatted.columns)
    rows = [[str(v) for v in row] for row in formatted.astype(str).to_numpy().tolist()]
    widths = [
        max(len(str(col)), *(len(row[i]) for row in rows)) if rows else len(str(col))
        for i, col in enumerate(cols)
    ]
    header = "| " + " | ".join(str(col).ljust(widths[i]) for i, col in enumerate(cols)) + " |"
    sep = "| " + " | ".join("-" * widths[i] for i in range(len(cols))) + " |"
    body = ["| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(cols))) + " |" for row in rows]
    return "\n".join([header, sep] + body)


def main() -> int:
    df = load_frame()
    df, value_table, value_source = attach_value_weights(df)
    print(f"Value weights source: {value_source}", flush=True)
    metric_rows = []
    trade_rows = []

    for i, (name, train_end, test_start, test_end) in enumerate(FOLDS):
        train = df[df["time_utc"] <= pd.Timestamp(train_end)].copy()
        test = df[(df["time_utc"] >= pd.Timestamp(test_start)) & (df["time_utc"] <= pd.Timestamp(test_end))].copy()
        if train.empty or test.empty:
            continue
        print(f"Fold {name}: train={len(train):,} test={len(test):,}", flush=True)

        y_event = test["major_event_60"].fillna(0).astype(int).to_numpy()
        for model, pred in [
            ("pair_history_prior", prior_predict(train, test, "major_event_60")),
            ("pair_local_model", fit_predict(train, test, "major_event_60", "local", 100 + i)),
            ("cluster_aware_model", fit_predict(train, test, "major_event_60", "cluster", 200 + i)),
        ]:
            metric_rows.append({"fold": name, "target": "major_event_60", "model": model, **cls_metrics(y_event, pred)})

        for mode, model_name in [("local", "pair_local_directional"), ("cluster", "cluster_aware_directional")]:
            long_pred = fit_predict(train, test, "profitable_long_move_60", mode, 300 + i)
            short_pred = fit_predict(train, test, "profitable_short_move_60", mode, 400 + i)
            metric_rows.append(
                {
                    "fold": name,
                    "target": "profitable_long_move_60",
                    "model": model_name,
                    **cls_metrics(test["profitable_long_move_60"].fillna(0).astype(int).to_numpy(), long_pred),
                }
            )
            metric_rows.append(
                {
                    "fold": name,
                    "target": "profitable_short_move_60",
                    "model": model_name,
                    **cls_metrics(test["profitable_short_move_60"].fillna(0).astype(int).to_numpy(), short_pred),
                }
            )
            for rank_mode in ["score_rank", "value_proxy_rank"]:
                for r in trade_metrics(test, long_pred, short_pred, model_name, rank_mode):
                    r["fold"] = name
                    trade_rows.append(r)

    metrics = pd.DataFrame(metric_rows)
    trades = pd.DataFrame(trade_rows)

    cls_summary = (
        metrics.groupby(["target", "model"], observed=True)
        .agg(
            folds=("fold", "nunique"),
            avg_auc=("auc", "mean"),
            avg_ap=("ap", "mean"),
            avg_ap_lift=("ap_lift", "mean"),
            avg_top_0p25_precision=("top_0.0025_precision", "mean"),
            avg_top_0p25_recall=("top_0.0025_recall", "mean"),
        )
        .reset_index()
    )
    trade_summary = (
        trades.groupby(["model", "rank_mode", "top_pct"], observed=True)
        .agg(
            folds=("fold", "nunique"),
            clustered_trades=("clustered_trades", "sum"),
            clustered_net_pips=("clustered_net_pips", "sum"),
            clustered_net_usd_10usd_70pct_margin=("clustered_net_usd_10usd_70pct_margin", "sum"),
            raw_net_pips=("raw_net_pips", "sum"),
            raw_net_usd_10usd_70pct_margin=("raw_net_usd_10usd_70pct_margin", "sum"),
            avg_clustered_win_rate=("clustered_win_rate", "mean"),
        )
        .reset_index()
    )
    trade_summary["clustered_avg_pips"] = trade_summary["clustered_net_pips"] / trade_summary["clustered_trades"].clip(lower=1)
    trade_summary["clustered_avg_usd"] = trade_summary["clustered_net_usd_10usd_70pct_margin"] / trade_summary["clustered_trades"].clip(lower=1)
    trade_summary["clustered_return_pct_on_10usd"] = trade_summary["clustered_net_usd_10usd_70pct_margin"] / ACCOUNT_NAV_USD * 100.0

    summary = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "source": str(CACHE_PARQUET),
        "rows": int(len(df)),
        "date_min": str(df["time_utc"].min()),
        "date_max": str(df["time_utc"].max()),
        "filter": "time >= 2025-01-01 and is_volatile_or_exotic_pair == 1",
        "value_weighting": {
            "source": value_source,
            "account_nav_usd": ACCOUNT_NAV_USD,
            "margin_fraction": MARGIN_FRACTION,
            "note": "Realized USD uses selected future net pips * current/fallback USD-per-pip at a $10 account 70% margin budget. Value-proxy rank uses model probability * historical instrument q995/spread capacity * USD-per-pip; it does not use future net pips for ranking.",
        },
        "cluster_structure": cluster_structure(df),
        "classification_summary": cls_summary.to_dict(orient="records"),
        "trade_summary": trade_summary.to_dict(orient="records"),
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = REPORT_ROOT / f"spike_cluster_quick_test_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(out_dir / "metrics.csv", index=False)
    trades.to_csv(out_dir / "trade_sim.csv", index=False)
    cls_summary.to_csv(out_dir / "classification_summary.csv", index=False)
    trade_summary.to_csv(out_dir / "trade_summary.csv", index=False)
    value_table.to_csv(out_dir / "value_weights_by_instrument.csv", index=False)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md = []
    md.append("# Spike cluster quick test\n")
    md.append(f"Created UTC: {summary['created_utc']}\n")
    md.append(f"Rows: {summary['rows']:,}; {summary['date_min']} to {summary['date_max']}\n")
    md.append(f"Filter: {summary['filter']}\n")
    md.append(
        f"Value weighting: source={value_source}, nav=${ACCOUNT_NAV_USD:.2f}, margin_fraction={MARGIN_FRACTION:.2f}\n"
    )
    md.append("\n## Cluster structure\n")
    cs = summary["cluster_structure"]
    for k, v in cs.items():
        md.append(f"- {k}: {v}\n")
    md.append("\n## Classification summary\n")
    md.append(markdown_table(cls_summary, floatfmt=".4f"))
    md.append("\n\n## Directional trade simulation\n")
    md.append(markdown_table(trade_summary, floatfmt=".4f"))
    md.append("\n")
    (out_dir / "summary.md").write_text("\n".join(md), encoding="utf-8")
    (REPORT_ROOT / "latest_spike_cluster_quick_test.md").write_text((out_dir / "summary.md").read_text(encoding="utf-8"), encoding="utf-8")
    (REPORT_ROOT / "latest_spike_cluster_quick_test.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
