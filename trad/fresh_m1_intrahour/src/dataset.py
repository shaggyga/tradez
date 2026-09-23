from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import (
    ROOT,
    add_time_features,
    fallback_spread_pips,
    pair_meta,
    parse_date,
    pip_size_for,
    pip_value_usd_per_unit,
    quote_to_usd_from_prices,
    read_candles,
    select_pairs,
    slippage_pips,
    utc_now_stamp,
    write_json,
)
from .economics import (
    endpoint_after_cost_arrays,
    executable_price_arrays,
    execution_settings_from_config,
    prepare_executable_prices,
    simulate_tp_sl_contract_arrays,
)
from .features import write_feature_family_report


def _rolling_future_extreme(values: pd.Series, horizon: int, how: str) -> pd.Series:
    shifted = values.shift(-1)
    rolled = shifted.iloc[::-1].rolling(horizon, min_periods=1)
    out = rolled.max() if how == "max" else rolled.min()
    return out.iloc[::-1]


def add_pair_features(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    instrument = df["instrument"].iloc[0]
    pip = pip_size_for(instrument)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    tr_pips = tr / pip

    for n in [1, 2, 3, 5, 8, 10, 13, 15, 21, 30, 60]:
        df[f"return_{n}m_pips"] = (close - close.shift(n)) / pip
    for n in [5, 15, 30, 60]:
        df[f"atr_{n}m_pips"] = tr_pips.rolling(n, min_periods=max(2, n // 3)).mean()
        df[f"rv_{n}m_pips"] = df["return_1m_pips"].rolling(n, min_periods=max(2, n // 3)).std()
        df[f"range_{n}m_pips"] = (high.rolling(n, min_periods=max(2, n // 3)).max() - low.rolling(n, min_periods=max(2, n // 3)).min()) / pip

    df["momentum_3m_atr"] = df["return_3m_pips"] / df["atr_15m_pips"]
    df["momentum_5m_atr"] = df["return_5m_pips"] / df["atr_15m_pips"]
    df["momentum_10m_atr"] = ((close - close.shift(10)) / pip) / df["atr_30m_pips"]
    df["momentum_15m_atr"] = ((close - close.shift(15)) / pip) / df["atr_30m_pips"]
    df["acceleration_3_10"] = df["momentum_3m_atr"] - df["momentum_10m_atr"]
    df["acceleration_5_15"] = df["momentum_5m_atr"] - df["momentum_15m_atr"]

    for span in [5, 10, 20, 50]:
        ema = close.ewm(span=span, adjust=False, min_periods=max(2, span // 2)).mean()
        df[f"ema_{span}_gap_pips"] = (close - ema) / pip
        df[f"ema_{span}_slope_pips"] = (ema - ema.shift(3)) / pip
    recent_hi = high.rolling(30, min_periods=10).max()
    recent_lo = low.rolling(30, min_periods=10).min()
    range_width = (recent_hi - recent_lo).replace(0, np.nan)
    df["range_position_30m"] = (close - recent_lo) / range_width
    df["dist_recent_high_30m_pips"] = (recent_hi - close) / pip
    df["dist_recent_low_30m_pips"] = (close - recent_lo) / pip
    df["pullback_from_high_30m_pips"] = df["dist_recent_high_30m_pips"]
    df["bounce_from_low_30m_pips"] = df["dist_recent_low_30m_pips"]

    body = (close - df["open"].astype(float)).abs()
    wick = (high - low - body).clip(lower=0.0)
    df["wick_body_ratio"] = wick / body.replace(0, np.nan)
    direction = np.sign(close.diff())
    df["reversal_count_15m"] = (direction != direction.shift(1)).astype(int).rolling(15, min_periods=5).sum()
    net = (close - close.shift(15)).abs() / pip
    path = df["return_1m_pips"].abs().rolling(15, min_periods=5).sum()
    df["efficiency_ratio_15m"] = net / path.replace(0, np.nan)
    df["choppiness_15m"] = 1.0 - df["efficiency_ratio_15m"]
    df["volatility_expansion_15_60"] = df["atr_15m_pips"] / df["atr_60m_pips"]
    df["movement_quality"] = df["efficiency_ratio_15m"] * df["volatility_expansion_15_60"]

    spread = pd.to_numeric(df.get("spread_pips"), errors="coerce")
    meta = pair_meta(instrument, cfg)
    df["spread_pips"] = spread.fillna(spread.rolling(240, min_periods=10).median()).fillna(fallback_spread_pips(meta, cfg))
    df["slippage_pips"] = slippage_pips(meta, cfg)
    df["round_trip_cost_pips"] = df["spread_pips"] + (2.0 * df["slippage_pips"])
    df["spread_to_atr_15m"] = df["spread_pips"] / df["atr_15m_pips"]
    df["cost_pressure_score"] = df["round_trip_cost_pips"] / df["atr_30m_pips"]
    df["session_liquidity_score"] = (
        df["is_london"].fillna(0) + df["is_new_york"].fillna(0) + df["is_london_ny_overlap"].fillna(0)
        - df["is_rollover_risk"].fillna(0)
    )
    df["m5_momentum_pips"] = (close - close.shift(5)) / pip
    df["m15_momentum_pips"] = (close - close.shift(15)) / pip
    df["m30_momentum_pips"] = (close - close.shift(30)) / pip
    df["h1_momentum_pips"] = (close - close.shift(60)) / pip
    df["m1_m5_alignment"] = np.sign(df["return_1m_pips"]) * np.sign(df["m5_momentum_pips"])
    df["m1_m15_alignment"] = np.sign(df["return_1m_pips"]) * np.sign(df["m15_momentum_pips"])
    df["m1_h1_alignment"] = np.sign(df["return_1m_pips"]) * np.sign(df["h1_momentum_pips"])
    return df


def add_future_labels(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    instrument = df["instrument"].iloc[0]
    pip = pip_size_for(instrument)
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    best_net = pd.Series(-1e9, index=df.index, dtype=float)
    best_h = pd.Series(0, index=df.index, dtype=int)
    for h in cfg["horizons_minutes"]:
        future_close = close.shift(-h)
        future_high = _rolling_future_extreme(high, h, "max")
        future_low = _rolling_future_extreme(low, h, "min")
        up_endpoint = (future_close - close) / pip
        down_endpoint = (close - future_close) / pip
        up_mfe = (future_high - close) / pip
        up_mae = (close - future_low) / pip
        down_mfe = (close - future_low) / pip
        down_mae = (future_high - close) / pip
        df[f"long_endpoint_{h}m_pips"] = up_endpoint
        df[f"short_endpoint_{h}m_pips"] = down_endpoint
        df[f"long_mfe_{h}m_pips"] = up_mfe
        df[f"short_mfe_{h}m_pips"] = down_mfe
        df[f"long_mae_{h}m_pips"] = up_mae
        df[f"short_mae_{h}m_pips"] = down_mae
        net_abs = pd.concat([up_endpoint, down_endpoint], axis=1).max(axis=1)
        take = net_abs > best_net
        best_net.loc[take] = net_abs.loc[take]
        best_h.loc[take] = h
    df["raw_best_horizon_any_side"] = best_h
    return df


def _first_hit(close: np.ndarray, high: np.ndarray, low: np.ndarray, i: int, side_sign: int, tp_pips: float, sl_pips: float, hold: int, pip: float) -> tuple[str, int, float, float, float, float]:
    entry = close[i]
    end_i = min(len(close) - 1, i + hold)
    mfe = 0.0
    mae = 0.0
    for j in range(i + 1, end_i + 1):
        if side_sign > 0:
            fav = (high[j] - entry) / pip
            adv = (entry - low[j]) / pip
            tp = fav >= tp_pips
            sl = adv >= sl_pips
        else:
            fav = (entry - low[j]) / pip
            adv = (high[j] - entry) / pip
            tp = fav >= tp_pips
            sl = adv >= sl_pips
        mfe = max(mfe, fav)
        mae = max(mae, adv)
        if tp and sl:
            return "ambiguous_same_bar", j - i, tp_pips, mfe, mae, side_sign * (close[j] - entry) / pip
        if tp:
            return "tp_before_sl", j - i, tp_pips, mfe, mae, side_sign * (close[j] - entry) / pip
        if sl:
            return "sl_before_tp", j - i, -sl_pips, mfe, mae, side_sign * (close[j] - entry) / pip
    endpoint = side_sign * (close[end_i] - entry) / pip
    return "timeout", end_i - i, endpoint, mfe, mae, endpoint


def expand_side_rows(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    instrument = df["instrument"].iloc[0]
    meta = pair_meta(instrument, cfg)
    pip = meta.pip_size
    n = len(df)
    settings = execution_settings_from_config(cfg, meta.tier)
    exec_df, exec_meta = prepare_executable_prices(df, instrument, cfg, settings.spread_multiplier)
    exec_arrays = executable_price_arrays(exec_df)
    df = df.copy()
    df["spread_pips_used"] = exec_df["spread_pips_used"]
    df["round_trip_cost_pips_used"] = exec_df["round_trip_cost_pips_used"]
    df["execution_native_bidask_complete_ohlc_rate"] = exec_meta["native_bidask_complete_ohlc_rate"]
    close_by_pair = {instrument: float(df["close"].iloc[-1])}
    quote_to_usd = quote_to_usd_from_prices(instrument, float(df["close"].iloc[-1]), close_by_pair)
    pip_value = pip_value_usd_per_unit(instrument, quote_to_usd)

    base_cols = [c for c in df.columns if c not in {"datetime"}]
    long_df = df[base_cols].copy()
    short_df = df[base_cols].copy()
    long_df["side"] = "long"
    short_df["side"] = "short"
    long_df["side_sign"] = 1
    short_df["side_sign"] = -1
    side_df = pd.concat([long_df, short_df], ignore_index=True)
    sign = side_df["side_sign"].astype(float)

    for col in ["return_1m_pips", "return_2m_pips", "return_3m_pips", "return_5m_pips", "return_8m_pips", "return_13m_pips", "return_21m_pips"]:
        side_df[f"side_{col}"] = side_df[col] * sign
    side_df["side_momentum_3m_atr"] = side_df["momentum_3m_atr"] * sign
    side_df["side_momentum_5m_atr"] = side_df["momentum_5m_atr"] * sign
    side_df["side_momentum_15m_atr"] = side_df["momentum_15m_atr"] * sign
    side_df["side_acceleration"] = side_df["acceleration_5_15"] * sign
    side_df["side_trend_alignment"] = np.sign(side_df["m15_momentum_pips"] * sign) + np.sign(side_df["h1_momentum_pips"] * sign)
    side_df["side_range_position"] = np.where(side_df["side"] == "long", side_df["range_position_30m"], 1.0 - side_df["range_position_30m"])
    side_df["side_breakout_pressure"] = np.where(side_df["side"] == "long", -side_df["dist_recent_high_30m_pips"], -side_df["dist_recent_low_30m_pips"])
    side_df["side_pullback_pressure"] = np.where(side_df["side"] == "long", side_df["bounce_from_low_30m_pips"], side_df["pullback_from_high_30m_pips"])
    side_df["side_exhaustion_risk"] = side_df["side_momentum_15m_atr"].clip(lower=0) * side_df["choppiness_15m"].fillna(1.0)

    for h in cfg["horizons_minutes"]:
        payload: dict[tuple[str, str], list[float]] = {}
        for side_name in ["long", "short"]:
            endpoints: list[float] = []
            mfes: list[float] = []
            maes: list[float] = []
            for i in range(n):
                result = endpoint_after_cost_arrays(exec_arrays, i, side_name, int(h), pip, settings)
                endpoints.append(result.endpoint_pips)
                mfes.append(result.mfe_pips)
                maes.append(result.mae_pips)
            payload[(side_name, "endpoint")] = endpoints
            payload[(side_name, "mfe")] = mfes
            payload[(side_name, "mae")] = maes
        side_df[f"endpoint_{h}m_pips"] = np.concatenate([payload[("long", "endpoint")], payload[("short", "endpoint")]])
        side_df[f"mfe_{h}m_pips"] = np.concatenate([payload[("long", "mfe")], payload[("short", "mfe")]])
        side_df[f"mae_{h}m_pips"] = np.concatenate([payload[("long", "mae")], payload[("short", "mae")]])
        side_df[f"net_after_cost_{h}m_pips"] = side_df[f"endpoint_{h}m_pips"]
        side_df[f"net_account_pnl_{h}m_1k_units"] = side_df[f"net_after_cost_{h}m_pips"] * pip_value * 1000.0

    policy_cols = []
    for template in cfg["policy_templates"]:
        name = template["name"]
        tp = float(template["tp_pips"])
        sl = float(template["sl_pips"])
        for h in cfg["horizons_minutes"]:
            policy_payload: dict[tuple[str, str], list[Any]] = {}
            for side_name in ["long", "short"]:
                outcomes: list[str] = []
                times: list[int] = []
                pips: list[float] = []
                mfes: list[float] = []
                maes: list[float] = []
                endpoints: list[float] = []
                ambiguous: list[int] = []
                for i in range(n):
                    result = simulate_tp_sl_contract_arrays(exec_arrays, i, side_name, tp, sl, int(h), pip, settings)
                    outcomes.append(result.outcome)
                    times.append(result.time_to_exit_min if result.time_to_exit_min is not None else np.nan)
                    pips.append(result.realized_pips)
                    mfes.append(result.mfe_pips)
                    maes.append(result.mae_pips)
                    endpoints.append(result.endpoint_pips)
                    ambiguous.append(result.same_bar_ambiguous)
                policy_payload[(side_name, "outcome")] = outcomes
                policy_payload[(side_name, "time")] = times
                policy_payload[(side_name, "pips")] = pips
                policy_payload[(side_name, "mfe")] = mfes
                policy_payload[(side_name, "mae")] = maes
                policy_payload[(side_name, "endpoint")] = endpoints
                policy_payload[(side_name, "ambiguous")] = ambiguous
            prefix = f"policy_{name}_{h}m"
            side_df[f"{prefix}_outcome"] = np.concatenate([policy_payload[("long", "outcome")], policy_payload[("short", "outcome")]])
            side_df[f"{prefix}_time_to_exit"] = np.concatenate([policy_payload[("long", "time")], policy_payload[("short", "time")]])
            side_df[f"{prefix}_net_pips"] = np.concatenate([policy_payload[("long", "pips")], policy_payload[("short", "pips")]])
            side_df[f"{prefix}_net_account_pnl_1k_units"] = side_df[f"{prefix}_net_pips"] * pip_value * 1000.0
            side_df[f"{prefix}_tp_before_sl"] = side_df[f"{prefix}_outcome"].isin(["tp_before_sl", "ambiguous_favorable_first"]).astype(int)
            side_df[f"{prefix}_sl_before_tp"] = side_df[f"{prefix}_outcome"].isin(["sl_before_tp", "ambiguous_adverse_first"]).astype(int)
            side_df[f"{prefix}_timeout"] = (side_df[f"{prefix}_outcome"] == "timeout").astype(int)
            side_df[f"{prefix}_same_bar_ambiguous"] = np.concatenate([policy_payload[("long", "ambiguous")], policy_payload[("short", "ambiguous")]])
            side_df[f"{prefix}_endpoint_pips"] = np.concatenate([policy_payload[("long", "endpoint")], policy_payload[("short", "endpoint")]])
            side_df[f"{prefix}_mfe_pips"] = np.concatenate([policy_payload[("long", "mfe")], policy_payload[("short", "mfe")]])
            side_df[f"{prefix}_mae_pips"] = np.concatenate([policy_payload[("long", "mae")], policy_payload[("short", "mae")]])
            policy_cols.append(f"{prefix}_net_account_pnl_1k_units")

    side_df["best_policy_net_account_pnl_1k_units"] = side_df[policy_cols].max(axis=1)
    best_idx = side_df[policy_cols].fillna(-1e18).idxmax(axis=1)
    all_policy_na = side_df[policy_cols].isna().all(axis=1)
    side_df["best_exit_policy"] = best_idx.str.replace("policy_", "", regex=False).str.replace("_net_account_pnl_1k_units", "", regex=False)
    side_df.loc[all_policy_na, "best_exit_policy"] = np.nan
    horizon_cols = [f"net_account_pnl_{h}m_1k_units" for h in cfg["horizons_minutes"]]
    side_df["best_horizon_net_account_pnl_1k_units"] = side_df[horizon_cols].max(axis=1)
    horizon_idx = side_df[horizon_cols].fillna(-1e18).idxmax(axis=1)
    side_df["best_horizon"] = horizon_idx.str.extract(r"_(\d+)m_")[0].astype(float)
    target_policy = str(cfg["model"].get("target_policy", cfg["policy_templates"][0]["name"]))
    target_horizon = int(cfg["model"].get("target_horizon_minutes", cfg["horizons_minutes"][0]))
    target_policy_col = f"policy_{target_policy}_{target_horizon}m_net_account_pnl_1k_units"
    if target_policy_col not in side_df.columns:
        raise ValueError(f"target policy column not found: {target_policy_col}")
    side_df["target_policy"] = target_policy
    side_df["target_horizon_minutes"] = target_horizon
    side_df["target_ev_usd_1k_units"] = side_df[target_policy_col]
    max_hold = max(int(h) for h in cfg["horizons_minutes"])
    last_complete_count = max(0, n - int(settings.entry_delay_bars) - max_hold + 1)
    complete_mask = side_df.groupby(["instrument", "side"]).cumcount() < last_complete_count
    side_df.loc[~complete_mask, "target_ev_usd_1k_units"] = np.nan
    side_df["target_positive_ev"] = (side_df["target_ev_usd_1k_units"] > float(cfg["model"]["positive_ev_usd_threshold"])).astype(int)
    side_df["quote_to_usd"] = quote_to_usd
    side_df["pip_value_usd_per_unit"] = pip_value
    side_df["tier"] = meta.tier
    side_df["base_currency"] = meta.base
    side_df["quote_currency"] = meta.quote
    side_df["entry_delay_bars"] = settings.entry_delay_bars
    side_df["same_bar_ambiguity_mode"] = settings.same_bar_ambiguity
    side_df["execution_cost_mode"] = settings.cost_mode
    return side_df


def add_currency_strength(side_df: pd.DataFrame) -> pd.DataFrame:
    tmp = side_df[side_df["side"] == "long"][["decision_time_utc", "instrument", "base_currency", "quote_currency", "return_5m_pips", "return_15m_pips"]].copy()
    records = []
    for horizon_col in ["return_5m_pips", "return_15m_pips"]:
        base = tmp[["decision_time_utc", "base_currency", horizon_col]].rename(columns={"base_currency": "currency", horizon_col: "strength"})
        quote = tmp[["decision_time_utc", "quote_currency", horizon_col]].rename(columns={"quote_currency": "currency", horizon_col: "strength"})
        quote["strength"] = -quote["strength"]
        cur = pd.concat([base, quote], ignore_index=True).groupby(["decision_time_utc", "currency"], as_index=False)["strength"].mean()
        cur = cur.rename(columns={"strength": f"currency_strength_{horizon_col}"})
        records.append(cur)
    cur_strength = records[0].merge(records[1], on=["decision_time_utc", "currency"], how="outer")

    def merge_strength(df: pd.DataFrame, currency_col: str, prefix: str) -> pd.DataFrame:
        out = df.merge(cur_strength, left_on=["decision_time_utc", currency_col], right_on=["decision_time_utc", "currency"], how="left")
        out = out.rename(columns={
            "currency_strength_return_5m_pips": f"{prefix}_strength_5m",
            "currency_strength_return_15m_pips": f"{prefix}_strength_15m",
        }).drop(columns=["currency"], errors="ignore")
        return out

    side_df = merge_strength(side_df, "base_currency", "base")
    side_df = merge_strength(side_df, "quote_currency", "quote")
    side_df["base_minus_quote_strength_5m"] = side_df["base_strength_5m"] - side_df["quote_strength_5m"]
    side_df["base_minus_quote_strength_15m"] = side_df["base_strength_15m"] - side_df["quote_strength_15m"]
    sign = side_df["side_sign"].astype(float)
    side_df["side_currency_strength_5m"] = side_df["base_minus_quote_strength_5m"] * sign
    side_df["side_currency_strength_15m"] = side_df["base_minus_quote_strength_15m"] * sign
    return side_df


def add_cross_sectional_ranks(side_df: pd.DataFrame) -> pd.DataFrame:
    rank_cols = [
        "side_momentum_5m_atr",
        "side_currency_strength_5m",
        "atr_15m_pips",
        "spread_to_atr_15m",
        "cost_pressure_score",
        "movement_quality",
    ]
    for col in rank_cols:
        if col not in side_df.columns:
            continue
        ascending = col in {"spread_to_atr_15m", "cost_pressure_score"}
        side_df[f"xs_rank_{col}"] = side_df.groupby("decision_time_utc")[col].rank(pct=True, ascending=ascending)
    side_df["xs_opportunity_score"] = (
        side_df.get("xs_rank_side_momentum_5m_atr", 0)
        + side_df.get("xs_rank_side_currency_strength_5m", 0)
        + side_df.get("xs_rank_movement_quality", 0)
        + side_df.get("xs_rank_spread_to_atr_15m", 0)
    ) / 4.0
    return side_df


def build_dataset(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str | None,
    end: str | None,
    tier: str,
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    start_ts = parse_date(start)
    end_ts = parse_date(end)
    selected_pairs = select_pairs(cfg, tier, pairs)
    frames = []
    pair_summaries = []
    for instrument in selected_pairs:
        raw = read_candles(instrument, cfg, start_ts, end_ts, max_rows=max_rows_per_pair)
        if len(raw) < 120:
            continue
        raw = add_time_features(raw)
        raw = add_pair_features(raw, cfg)
        raw = add_future_labels(raw, cfg)
        side = expand_side_rows(raw, cfg)
        frames.append(side)
        pair_summaries.append({
            "instrument": instrument,
            "rows": int(len(raw)),
            "side_rows": int(len(side)),
            "start": str(raw["decision_time_utc"].min()),
            "end": str(raw["decision_time_utc"].max()),
        })
    if not frames:
        raise RuntimeError("no usable pair frames built")
    dataset = pd.concat(frames, ignore_index=True)
    dataset = add_currency_strength(dataset)
    dataset = add_cross_sectional_ranks(dataset)
    # Drop rows where future 30m labels or main features are unavailable.
    dataset = dataset.dropna(subset=["target_ev_usd_1k_units", "side_momentum_5m_atr", "atr_15m_pips", "spread_to_atr_15m"])
    dataset = dataset.sort_values(["decision_time_utc", "instrument", "side"]).reset_index(drop=True)
    dataset_path = output_dir / "side_dataset.parquet"
    dataset.to_parquet(dataset_path, index=False)
    feature_report_path = write_feature_family_report(dataset, output_dir / "feature_family_report.json")
    manifest = {
        "engine": cfg["engine_name"],
        "built_utc": utc_now_stamp(),
        "dataset_path": str(dataset_path),
        "feature_family_report_path": str(feature_report_path),
        "tier": tier,
        "pairs": selected_pairs,
        "pair_summaries": pair_summaries,
        "rows": int(len(dataset)),
        "start": str(dataset["decision_time_utc"].min()),
        "end": str(dataset["decision_time_utc"].max()),
        "horizons_minutes": cfg["horizons_minutes"],
        "policy_templates": cfg["policy_templates"],
        "target_policy": cfg["model"].get("target_policy"),
        "leakage_checks": {
            "features_use_current_or_past_candles": True,
            "labels_begin_after_decision_time": True,
            "old_reversal_target_used": False,
            "follow_momentum_policy_used": False,
            "label_derived_best_policy_used_for_training": False,
        },
        "execution_labeling": {
            "entry_delay_bars": int(cfg.get("execution", {}).get("entry_delay_bars", 1)),
            "entry_price": str(cfg.get("execution", {}).get("entry_price", "open")),
            "same_bar_ambiguity": str(cfg.get("execution", {}).get("same_bar_ambiguity", "adverse_first")),
            "spread_multiplier": float(cfg.get("execution", {}).get("spread_multiplier", 1.0)),
            "bidask_columns_preserved_when_available": True,
            "mid_spread_approximation_when_bidask_missing": True,
            "live_execution_enabled": False,
        },
        "columns": list(dataset.columns),
    }
    write_json(output_dir / "dataset_manifest.json", manifest)
    return dataset_path
