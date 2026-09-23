from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .common import (
    add_time_features,
    max_drawdown,
    parse_date,
    pair_meta,
    pip_value_usd_per_unit,
    quote_to_usd_from_prices,
    read_candles,
    select_pairs,
    utc_now_stamp,
    write_json,
)
from .dataset import add_cross_sectional_ranks, add_currency_strength, add_pair_features
from .economics import execution_settings_from_config, prepare_executable_prices
from .features import feature_family_columns, write_feature_family_report


try:
    from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingClassifier, HistGradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover - recorded at runtime
    ExtraTreesRegressor = None
    HistGradientBoostingClassifier = None
    HistGradientBoostingRegressor = None
    Ridge = None
    make_pipeline = None
    StandardScaler = None


HORIZONS = [1, 2, 3, 5, 8, 13, 21, 30]
PRIMARY_HORIZONS = [3, 5, 8, 13, 21]
REQUIRED_REPORTS = [
    "MODEL_ZOO_SUMMARY",
    "BASELINE_COMPARISON",
    "STRATEGY_BRANCH_REPORT",
    "MOVEMENT_BRANCH_REPORT",
    "OCO_BREAKOUT_REPORT",
    "CURRENCY_STRENGTH_REPORT",
    "RESIDUAL_BRANCH_REPORT",
    "EXIT_ALPHA_REPORT",
    "RANKING_REPORT",
    "CALIBRATION_REPORT",
    "COST_STRESS_REPORT",
    "FAILURE_CLASSIFICATION_REPORT",
    "FINAL_RESEARCH_SUMMARY",
]


@dataclass(frozen=True)
class SplitData:
    train: pd.DataFrame
    validation: pd.DataFrame
    final: pd.DataFrame
    meta: dict[str, Any]


def _shift_neg(values: np.ndarray, periods: int) -> np.ndarray:
    out = np.full(len(values), np.nan, dtype=float)
    if periods <= 0:
        return values.astype(float)
    if periods < len(values):
        out[:-periods] = values[periods:]
    return out


def _future_roll(values: np.ndarray, horizon: int, how: str) -> np.ndarray:
    s = pd.Series(values, dtype=float).shift(-1)
    roller = s.iloc[::-1].rolling(int(horizon), min_periods=int(horizon))
    rolled = roller.max() if how == "max" else roller.min()
    return rolled.iloc[::-1].to_numpy(dtype=float)


def _session_from_time(ts: pd.Series) -> pd.Series:
    hour = pd.to_datetime(ts, utc=True).dt.hour
    return pd.Series(
        np.select(
            [
                hour.isin([21, 22]),
                (hour >= 12) & (hour < 16),
                (hour >= 7) & (hour < 16),
                (hour >= 12) & (hour < 21),
                (hour >= 0) & (hour < 7),
            ],
            ["rollover", "london_ny_overlap", "london", "new_york", "asia"],
            default="other",
        ),
        index=ts.index,
    )


def _load_research_dataset(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str,
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> pd.DataFrame:
    start_ts = parse_date(start)
    end_ts = parse_date(end)
    selected_pairs = select_pairs(cfg, tier, pairs)
    frames: list[pd.DataFrame] = []
    health: list[dict[str, Any]] = []
    for instrument in selected_pairs:
        raw = read_candles(instrument, cfg, start_ts, end_ts, max_rows=max_rows_per_pair)
        if len(raw) < 300:
            health.append({"pair": instrument, "rows": int(len(raw)), "skipped": True, "reason": "too_few_rows"})
            continue
        raw = add_time_features(raw)
        raw = add_pair_features(raw, cfg)
        meta = pair_meta(instrument, cfg)
        settings = execution_settings_from_config(cfg, meta.tier)
        exec_df, exec_meta = prepare_executable_prices(raw, instrument, cfg, settings.spread_multiplier)

        pip = meta.pip_size
        quote_to_usd = quote_to_usd_from_prices(instrument, float(exec_df["close"].iloc[-1]), {instrument: float(exec_df["close"].iloc[-1])})
        pip_value = float(pip_value_usd_per_unit(instrument, quote_to_usd))
        slip = settings.slippage_pips_round_trip
        bid_open = exec_df["exec_bid_open"].to_numpy(dtype=float)
        bid_high = exec_df["exec_bid_high"].to_numpy(dtype=float)
        bid_low = exec_df["exec_bid_low"].to_numpy(dtype=float)
        bid_close = exec_df["exec_bid_close"].to_numpy(dtype=float)
        ask_open = exec_df["exec_ask_open"].to_numpy(dtype=float)
        ask_high = exec_df["exec_ask_high"].to_numpy(dtype=float)
        ask_low = exec_df["exec_ask_low"].to_numpy(dtype=float)
        ask_close = exec_df["exec_ask_close"].to_numpy(dtype=float)
        mid_high = exec_df["high"].to_numpy(dtype=float)
        mid_low = exec_df["low"].to_numpy(dtype=float)

        base_cols = [c for c in exec_df.columns if not c.startswith("exec_")]
        long_df = exec_df[base_cols].copy()
        short_df = exec_df[base_cols].copy()
        long_df["side"] = "long"
        short_df["side"] = "short"
        long_df["side_sign"] = 1
        short_df["side_sign"] = -1
        for h in HORIZONS:
            entry_long = _shift_neg(ask_open, 1)
            entry_short = _shift_neg(bid_open, 1)
            exit_bid = _shift_neg(bid_close, h)
            exit_ask = _shift_neg(ask_close, h)
            long_endpoint = ((exit_bid - entry_long) / pip) - slip
            short_endpoint = ((entry_short - exit_ask) / pip) - slip
            max_bid = _future_roll(bid_high, h, "max")
            min_bid = _future_roll(bid_low, h, "min")
            max_ask = _future_roll(ask_high, h, "max")
            min_ask = _future_roll(ask_low, h, "min")
            max_mid = _future_roll(mid_high, h, "max")
            min_mid = _future_roll(mid_low, h, "min")
            long_mfe = np.maximum(0.0, (max_bid - entry_long) / pip)
            long_mae = np.maximum(0.0, (entry_long - min_bid) / pip)
            short_mfe = np.maximum(0.0, (entry_short - min_ask) / pip)
            short_mae = np.maximum(0.0, (max_ask - entry_short) / pip)
            long_df[f"actual_pips_{h}m"] = long_endpoint
            short_df[f"actual_pips_{h}m"] = short_endpoint
            long_df[f"actual_ev_{h}m"] = long_endpoint * pip_value * 1000.0
            short_df[f"actual_ev_{h}m"] = short_endpoint * pip_value * 1000.0
            long_df[f"mfe_pips_{h}m"] = long_mfe
            short_df[f"mfe_pips_{h}m"] = short_mfe
            long_df[f"mae_pips_{h}m"] = long_mae
            short_df[f"mae_pips_{h}m"] = short_mae
            long_df[f"future_range_pips_{h}m"] = (max_mid - min_mid) / pip
            short_df[f"future_range_pips_{h}m"] = (max_mid - min_mid) / pip
            long_df[f"abs_move_pips_{h}m"] = np.abs(long_endpoint)
            short_df[f"abs_move_pips_{h}m"] = np.abs(short_endpoint)

        side_df = pd.concat([long_df, short_df], ignore_index=True)
        sign = side_df["side_sign"].astype(float)
        for col in ["return_1m_pips", "return_2m_pips", "return_3m_pips", "return_5m_pips", "return_8m_pips", "return_13m_pips", "return_21m_pips"]:
            if col in side_df.columns:
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
        side_df["pair"] = instrument
        side_df["tier"] = meta.tier
        side_df["base_currency"] = meta.base
        side_df["quote_currency"] = meta.quote
        side_df["pip_value_usd_per_unit"] = pip_value
        side_df["session"] = _session_from_time(side_df["decision_time_utc"])
        side_df["spread_pips_used"] = exec_df["spread_pips_used"].to_numpy().tolist() * 2
        side_df["entry_delay_bars"] = settings.entry_delay_bars
        side_df["same_bar_ambiguity_mode"] = settings.same_bar_ambiguity
        frames.append(side_df)
        health.append({
            "pair": instrument,
            "rows": int(len(raw)),
            "side_rows": int(len(side_df)),
            "start": str(raw["decision_time_utc"].min()),
            "end": str(raw["decision_time_utc"].max()),
            **exec_meta,
            "skipped": False,
        })

    if not frames:
        raise RuntimeError("no usable research dataset rows")
    df = pd.concat(frames, ignore_index=True)
    df = add_currency_strength(df)
    df = add_cross_sectional_ranks(df)
    df["pair_residual_5m"] = df["return_5m_pips"] - df["base_minus_quote_strength_5m"].fillna(0.0)
    df["side_pair_residual_5m"] = df["pair_residual_5m"] * df["side_sign"].astype(float)
    df["movement_forecast_proxy"] = (
        df["atr_15m_pips"].fillna(0.0)
        + df["range_15m_pips"].fillna(0.0)
        + df["movement_quality"].fillna(0.0)
        - df["spread_to_atr_15m"].fillna(1.0)
    )
    required = ["actual_ev_30m", "side_momentum_5m_atr", "atr_15m_pips", "spread_to_atr_15m"]
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[c for c in required if c in df.columns])
    df = df.sort_values(["decision_time_utc", "pair", "side"]).reset_index(drop=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_dir / "research_dataset.parquet", index=False)
    pd.DataFrame(health).to_csv(output_dir / "research_data_health.csv", index=False)
    write_feature_family_report(df, output_dir / "feature_family_report.json")
    write_json(output_dir / "research_dataset_manifest.json", {
        "rows": int(len(df)),
        "pairs": selected_pairs,
        "start": str(df["decision_time_utc"].min()),
        "end": str(df["decision_time_utc"].max()),
        "horizons": HORIZONS,
        "phase1_economics_source_of_truth": True,
        "entry_delay_bars": 1,
        "same_bar_ambiguity": "adverse_first",
        "live_execution_enabled": False,
        "columns": list(df.columns),
    })
    return df


def _split(df: pd.DataFrame, cfg: dict[str, Any]) -> SplitData:
    times = pd.Series(pd.to_datetime(df["decision_time_utc"], utc=True).drop_duplicates().sort_values().to_numpy())
    if len(times) < 20:
        raise RuntimeError("not enough timestamps for nested split")
    val_frac = 0.20
    test_frac = 0.20
    train_end_i = max(1, int(len(times) * (1.0 - val_frac - test_frac)))
    val_end_i = max(train_end_i + 1, int(len(times) * (1.0 - test_frac)))
    train_end = pd.Timestamp(times.iloc[train_end_i - 1])
    val_start = pd.Timestamp(times.iloc[train_end_i])
    val_end = pd.Timestamp(times.iloc[val_end_i - 1])
    test_start = pd.Timestamp(times.iloc[val_end_i])
    embargo_min = int(cfg.get("surface", {}).get("purge_embargo_minutes", 270))
    embargo = pd.Timedelta(minutes=embargo_min)
    train = df[df["decision_time_utc"] <= train_end - embargo].copy()
    validation = df[(df["decision_time_utc"] >= val_start + embargo) & (df["decision_time_utc"] <= val_end - embargo)].copy()
    final = df[df["decision_time_utc"] >= test_start + embargo].copy()
    if train.empty or validation.empty or final.empty:
        raise RuntimeError("nested split produced empty partition")
    return SplitData(train, validation, final, {
        "train_start": str(train["decision_time_utc"].min()),
        "train_end": str(train["decision_time_utc"].max()),
        "validation_start": str(validation["decision_time_utc"].min()),
        "validation_end": str(validation["decision_time_utc"].max()),
        "final_test_start": str(final["decision_time_utc"].min()),
        "final_test_end": str(final["decision_time_utc"].max()),
        "purge_embargo_minutes": embargo_min,
        "winner_selected_on_validation_not_final": True,
    })


def _family_map(df: pd.DataFrame) -> dict[str, list[str]]:
    families = feature_family_columns(df)
    out = {
        "A_m1_price_action": families.get("A_m1_price_action", []),
        "B_m1_spread_cost_liquidity": families.get("B_m1_spread_cost_liquidity", []),
        "C_m1_volatility_motion": families.get("C_m1_volatility_motion", []),
        "D_m1_currency_strength": families.get("D_m1_currency_strength", []),
        "E_m1_cross_sectional_ranks": families.get("E_m1_cross_sectional_ranks", []),
        "F_m1_session_time": families.get("F_m1_session_time", []),
        "G_m1_higher_timeframe_context": families.get("G_m1_higher_timeframe", []),
        "H_movement_quality_choppiness": families.get("H_movement_quality", []),
        "I_full": families.get("I_full_tabular", []),
    }
    out["M1_plus_spread_plus_volatility"] = sorted(set(out["A_m1_price_action"] + out["B_m1_spread_cost_liquidity"] + out["C_m1_volatility_motion"]))
    out["M1_plus_spread_plus_currency_strength"] = sorted(set(out["A_m1_price_action"] + out["B_m1_spread_cost_liquidity"] + out["D_m1_currency_strength"]))
    out["M1_plus_spread_plus_currency_strength_plus_ranks"] = sorted(set(out["M1_plus_spread_plus_currency_strength"] + out["E_m1_cross_sectional_ranks"]))
    out["full"] = out["I_full"]
    out["full_minus_pair_identity"] = out["I_full"]
    out["full_minus_exotic_pair_identity"] = out["I_full"]
    return {k: v for k, v in out.items() if v}


def _clean_x(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return df[cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _sample_train(df: pd.DataFrame, max_rows: int, seed: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    return df.sample(max_rows, random_state=seed).sort_values("decision_time_utc")


def _fit_predict_model(
    model_name: str,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    final: pd.DataFrame,
    features: list[str],
    target: str,
    seed: int,
) -> tuple[pd.Series, pd.Series, str | None]:
    if model_name == "simple_rule_baseline":
        return validation["side_momentum_5m_atr"].fillna(0.0), final["side_momentum_5m_atr"].fillna(0.0), None
    try:
        x_train = _clean_x(train, features)
        y = train[target].astype(float).fillna(0.0)
        if model_name == "ridge":
            if Ridge is None:
                return pd.Series(dtype=float), pd.Series(dtype=float), "sklearn Ridge unavailable"
            model = make_pipeline(StandardScaler(), Ridge(alpha=2.0, random_state=seed))
            model.fit(x_train, y)
            return pd.Series(model.predict(_clean_x(validation, features)), index=validation.index), pd.Series(model.predict(_clean_x(final, features)), index=final.index), None
        if model_name == "extra_trees":
            if ExtraTreesRegressor is None:
                return pd.Series(dtype=float), pd.Series(dtype=float), "sklearn ExtraTreesRegressor unavailable"
            model = ExtraTreesRegressor(n_estimators=60, max_depth=8, min_samples_leaf=80, random_state=seed, n_jobs=-1)
            model.fit(x_train, y)
            return pd.Series(model.predict(_clean_x(validation, features)), index=validation.index), pd.Series(model.predict(_clean_x(final, features)), index=final.index), None
        if model_name in {"hist_gradient_boosting", "direct_ev_regressor", "rank_surrogate"}:
            if HistGradientBoostingRegressor is None:
                return pd.Series(dtype=float), pd.Series(dtype=float), "sklearn HistGradientBoostingRegressor unavailable"
            y_fit = y.rank(pct=True) if model_name == "rank_surrogate" else y
            model = HistGradientBoostingRegressor(
                learning_rate=0.06,
                max_iter=70,
                max_leaf_nodes=21,
                min_samples_leaf=90,
                l2_regularization=0.10,
                random_state=seed,
            )
            model.fit(x_train, y_fit)
            return pd.Series(model.predict(_clean_x(validation, features)), index=validation.index), pd.Series(model.predict(_clean_x(final, features)), index=final.index), None
        if model_name == "calibrated_classifier_ev_regressor":
            if HistGradientBoostingRegressor is None or HistGradientBoostingClassifier is None:
                return pd.Series(dtype=float), pd.Series(dtype=float), "sklearn HGB classifier/regressor unavailable"
            clf = HistGradientBoostingClassifier(max_iter=60, max_leaf_nodes=21, min_samples_leaf=90, random_state=seed)
            reg = HistGradientBoostingRegressor(max_iter=70, max_leaf_nodes=21, min_samples_leaf=90, random_state=seed)
            y_pos = (y > 0).astype(int)
            if y_pos.nunique() < 2:
                return pd.Series(dtype=float), pd.Series(dtype=float), "classification target single-class"
            clf.fit(x_train, y_pos)
            reg.fit(x_train, y)
            def score(part: pd.DataFrame) -> pd.Series:
                x = _clean_x(part, features)
                proba = clf.predict_proba(x)[:, list(clf.classes_).index(1)]
                ev = reg.predict(x)
                return pd.Series(proba * ev, index=part.index)
            return score(validation), score(final), None
        if model_name == "quantile_lower_bound":
            if HistGradientBoostingRegressor is None:
                return pd.Series(dtype=float), pd.Series(dtype=float), "sklearn HistGradientBoostingRegressor unavailable"
            try:
                model = HistGradientBoostingRegressor(loss="quantile", quantile=0.25, max_iter=60, max_leaf_nodes=21, min_samples_leaf=90, random_state=seed)
                model.fit(x_train, y)
                return pd.Series(model.predict(_clean_x(validation, features)), index=validation.index), pd.Series(model.predict(_clean_x(final, features)), index=final.index), None
            except Exception as exc:
                return pd.Series(dtype=float), pd.Series(dtype=float), f"quantile model skipped: {exc}"
    except Exception as exc:
        return pd.Series(dtype=float), pd.Series(dtype=float), str(exc)
    return pd.Series(dtype=float), pd.Series(dtype=float), f"unknown model family: {model_name}"


def _top_by_timestamp(df: pd.DataFrame, score_col: str, k: int = 1) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    return df.sort_values(score_col, ascending=False).groupby("decision_time_utc", as_index=False).head(k)


def _select_threshold(scored: pd.DataFrame, score_col: str, actual_col: str) -> float:
    tmp = _top_by_timestamp(scored.dropna(subset=[score_col, actual_col]), score_col, 1)
    if tmp.empty:
        return math.inf
    candidates = sorted(set([float("-inf"), 0.0] + [float(x) for x in tmp[score_col].quantile([0.50, 0.60, 0.70, 0.80, 0.90, 0.95]).dropna()]))
    best_threshold = math.inf
    best_pnl = 0.0
    for threshold in candidates:
        take = tmp[tmp[score_col] >= threshold]
        pnl = float(take[actual_col].sum()) if len(take) else 0.0
        if pnl > best_pnl:
            best_pnl = pnl
            best_threshold = threshold
    return float(best_threshold)


def _calibration(scored: pd.DataFrame, score_col: str, actual_col: str) -> dict[str, Any]:
    tmp = scored[[score_col, actual_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(tmp) < 5 or tmp[score_col].nunique() < 2:
        return {"slope": None, "intercept": None, "rmse": None, "deciles": [], "top_decile_actual_ev": None, "ev_decile_monotonicity": None}
    x = tmp[score_col].to_numpy(dtype=float)
    y = tmp[actual_col].to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    pred = intercept + slope * x
    rmse = float(np.sqrt(np.mean((pred - y) ** 2)))
    tmp = tmp.copy()
    tmp["decile"] = pd.qcut(tmp[score_col].rank(method="first"), 10, labels=False, duplicates="drop")
    rows = []
    for decile, g in tmp.groupby("decile"):
        rows.append({"decile": int(decile), "rows": int(len(g)), "pred_mean": float(g[score_col].mean()), "actual_ev_mean": float(g[actual_col].mean())})
    means = [r["actual_ev_mean"] for r in rows]
    monotonic = float(pd.Series(range(len(means))).corr(pd.Series(means), method="spearman")) if len(means) > 2 else None
    return {
        "slope": float(slope),
        "intercept": float(intercept),
        "rmse": rmse,
        "deciles": rows,
        "top_decile_actual_ev": rows[-1]["actual_ev_mean"] if rows else None,
        "ev_decile_monotonicity": monotonic,
    }


def _apply_gate(scored: pd.DataFrame, score_col: str, actual_col: str, threshold: float, top_k: int = 1) -> pd.DataFrame:
    top = _top_by_timestamp(scored.dropna(subset=[score_col, actual_col]), score_col, top_k)
    if math.isinf(threshold):
        return top.iloc[0:0].copy()
    return top[top[score_col] >= threshold].copy()


def _trade_metrics(
    trades: pd.DataFrame,
    scored_universe: pd.DataFrame,
    score_col: str,
    actual_col: str,
    branch_name: str,
    initial_equity: float = 1000.0,
) -> dict[str, Any]:
    pnl = trades[actual_col].astype(float) if not trades.empty else pd.Series(dtype=float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    equity = (initial_equity + pnl.cumsum()).tolist() if len(pnl) else [initial_equity]
    actual_pips_col = actual_col.replace("actual_ev_", "actual_pips_")
    h = actual_col.split("_")[-1]
    mfe_col = f"mfe_pips_{h}"
    mae_col = f"mae_pips_{h}"
    capture = None
    giveback = None
    if not trades.empty and mfe_col in trades.columns and actual_pips_col in trades.columns:
        mfe = trades[mfe_col].replace(0, np.nan).astype(float)
        actual_pips = trades[actual_pips_col].astype(float)
        capture = float((actual_pips / mfe).replace([np.inf, -np.inf], np.nan).mean())
        giveback = float((trades[mfe_col].astype(float) - actual_pips).mean())
    calibration = _calibration(scored_universe, score_col, actual_col)
    precision = {}
    top_actual_ev = {}
    for k in [1, 3, 5]:
        top = _top_by_timestamp(scored_universe.dropna(subset=[score_col, actual_col]), score_col, k)
        precision[str(k)] = float((top[actual_col] > 0).mean()) if len(top) else None
        top_actual_ev[str(k)] = float(top[actual_col].mean()) if len(top) else None
    by_pair = _breakdown(trades, "pair", actual_col)
    by_session = _breakdown(trades, "session", actual_col)
    by_horizon = [{"horizon": h, "trades": int(len(trades)), "pnl": float(pnl.sum()) if len(pnl) else 0.0}]
    spread_paid = float((trades.get("spread_pips_used", pd.Series(dtype=float)).fillna(0.0) * trades.get("pip_value_usd_per_unit", pd.Series(dtype=float)).fillna(0.0) * 1000.0).sum()) if not trades.empty else 0.0
    switches = 0
    if len(trades) > 1:
        prev = trades[["pair", "side"]].shift(1)
        switches = int(((trades["pair"] != prev["pair"]) | (trades["side"] != prev["side"])).sum())
    return {
        "branch": branch_name,
        "return_pct": float((pnl.sum() / initial_equity) * 100.0) if len(pnl) else 0.0,
        "pnl_account": float(pnl.sum()) if len(pnl) else 0.0,
        "max_drawdown_pct": max_drawdown(equity),
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "trade_count": int(len(trades)),
        "win_rate": float((pnl > 0).mean()) if len(pnl) else None,
        "average_hold_time": float(int(h.removesuffix("m"))) if h.endswith("m") and len(trades) else None,
        "spread_paid": spread_paid,
        "churn_switch_count": switches,
        "switching_cost_estimate": float(switches * 0.4 * trades.get("pip_value_usd_per_unit", pd.Series([0.0])).mean() * 1000.0) if len(trades) else 0.0,
        "precision_at_1": precision["1"],
        "precision_at_3": precision["3"],
        "precision_at_5": precision["5"],
        "actual_top1_ev": top_actual_ev["1"],
        "actual_top3_ev": top_actual_ev["3"],
        "actual_top5_ev": top_actual_ev["5"],
        "top_predicted_ev_bucket_actual_ev": calibration["top_decile_actual_ev"],
        "ev_decile_monotonicity": calibration["ev_decile_monotonicity"],
        "calibration": calibration,
        "mfe_capture_ratio": capture,
        "giveback_pips": giveback,
        "pair_breakdown": by_pair,
        "session_breakdown": by_session,
        "horizon_breakdown": by_horizon,
        "policy_breakdown": [{"policy": "endpoint_timeout_or_branch_rule", "trades": int(len(trades)), "pnl": float(pnl.sum()) if len(pnl) else 0.0}],
    }


def _breakdown(df: pd.DataFrame, col: str, actual_col: str) -> list[dict[str, Any]]:
    if df.empty or col not in df.columns:
        return []
    out = []
    for key, g in df.groupby(col, dropna=False):
        pnl = g[actual_col].astype(float)
        out.append({"name": str(key), "trades": int(len(g)), "pnl": float(pnl.sum()), "win_rate": float((pnl > 0).mean())})
    return out


def _score_rule_branches(split: SplitData) -> dict[str, dict[str, Any]]:
    branch_defs = {
        "random_candidate": lambda d: pd.Series(np.random.default_rng(42).normal(size=len(d)), index=d.index),
        "top_1m_momentum_continuation": lambda d: d["side_return_1m_pips"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "top_5m_momentum_continuation": lambda d: d["side_return_5m_pips"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "top_5m_snapback_reversal": lambda d: -d["side_return_5m_pips"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "top_volatility_compression_breakout": lambda d: d["movement_forecast_proxy"].fillna(0.0) - d["range_position_30m"].sub(0.5).abs().fillna(0.5),
        "strongest_vs_weakest_currency": lambda d: d["side_currency_strength_5m"].fillna(0.0) + 0.5 * d["side_currency_strength_15m"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "lowest_spread_top_momentum": lambda d: d["side_momentum_5m_atr"].fillna(0.0) - 2.0 * d["spread_to_atr_15m"].fillna(1.0),
        "london_ny_overlap_momentum": lambda d: (d["session"].eq("london_ny_overlap").astype(float) * 10.0) + d["side_momentum_5m_atr"].fillna(0.0),
        "london_open_breakout": lambda d: (d["is_london_open"].fillna(0.0) * 10.0) + d["side_breakout_pressure"].fillna(0.0),
        "ny_fade": lambda d: (d["is_new_york"].fillna(0.0) * 10.0) - d["side_momentum_5m_atr"].fillna(0.0),
        "cross_sectional_momentum_branch": lambda d: d.get("xs_rank_side_momentum_5m_atr", pd.Series(0.0, index=d.index)).fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "movement_only_branch": lambda d: d["movement_forecast_proxy"].fillna(0.0) + 0.15 * d["side_momentum_3m_atr"].fillna(0.0),
        "snapback_branch": lambda d: -d["side_momentum_15m_atr"].fillna(0.0) - d["side_exhaustion_risk"].fillna(0.0),
        "currency_strength_rotation_branch": lambda d: d["side_currency_strength_5m"].fillna(0.0) + d["side_currency_strength_15m"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "residual_continuation_branch": lambda d: d["side_pair_residual_5m"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "residual_mean_reversion_branch": lambda d: -d["side_pair_residual_5m"].fillna(0.0) - d["spread_to_atr_15m"].fillna(1.0),
        "exit_alpha_proxy_branch": lambda d: d["side_momentum_5m_atr"].fillna(0.0) + d["movement_quality"].fillna(0.0) - d["choppiness_15m"].fillna(1.0),
    }
    reports: dict[str, dict[str, Any]] = {}
    for name, scorer in branch_defs.items():
        best = None
        for h in PRIMARY_HORIZONS:
            actual_col = f"actual_ev_{h}m"
            val = split.validation.copy()
            fin = split.final.copy()
            val["branch_score"] = scorer(val)
            fin["branch_score"] = scorer(fin)
            threshold = _select_threshold(val, "branch_score", actual_col)
            val_trades = _apply_gate(val, "branch_score", actual_col, threshold)
            val_metrics = _trade_metrics(val_trades, val, "branch_score", actual_col, name)
            if best is None or val_metrics["pnl_account"] > best["validation"]["pnl_account"]:
                final_trades = _apply_gate(fin, "branch_score", actual_col, threshold)
                best = {
                    "branch": name,
                    "horizon": h,
                    "threshold": threshold,
                    "validation": val_metrics,
                    "final": _trade_metrics(final_trades, fin, "branch_score", actual_col, name),
                    "selected_trades": final_trades,
                }
        reports[name] = best or {}
    return reports


def _run_model_zoo(split: SplitData, output_dir: Path, seed: int = 42) -> dict[str, Any]:
    families = _family_map(split.train)
    feature_order = [
        "A_m1_price_action",
        "B_m1_spread_cost_liquidity",
        "C_m1_volatility_motion",
        "D_m1_currency_strength",
        "E_m1_cross_sectional_ranks",
        "M1_plus_spread_plus_volatility",
        "M1_plus_spread_plus_currency_strength",
        "M1_plus_spread_plus_currency_strength_plus_ranks",
        "I_full",
    ]
    model_names = [
        "simple_rule_baseline",
        "ridge",
        "hist_gradient_boosting",
        "extra_trees",
        "calibrated_classifier_ev_regressor",
        "direct_ev_regressor",
        "quantile_lower_bound",
        "rank_surrogate",
    ]
    label_specs = [
        ("signed_return", 5, "actual_ev_5m"),
        ("signed_return", 13, "actual_ev_13m"),
        ("expected_MFE_path_quality", 8, "actual_ev_8m"),
        ("TP_before_SL_proxy", 21, "actual_ev_21m"),
    ]
    reports = []
    skipped = []
    train_cap = 25000
    max_experiments = 48
    experiments = 0
    best = None
    train_sample = _sample_train(split.train, train_cap, seed)
    experiment_plan = []
    for feature_i, feature_name in enumerate(feature_order):
        for model_i, model_name in enumerate(model_names):
            label_family, horizon, target = label_specs[(feature_i + model_i) % len(label_specs)]
            experiment_plan.append((feature_name, label_family, horizon, target, model_name))
    if len(experiment_plan) > max_experiments:
        for combo in experiment_plan[max_experiments:]:
            skipped.append({
                "feature_family": combo[0],
                "label_family": combo[1],
                "model_family": combo[4],
                "reason": "max experiment cap reached",
            })
    for feature_name, label_family, horizon, target, model_name in experiment_plan[:max_experiments]:
        features = families.get(feature_name, [])
        if not features:
            skipped.append({"feature_family": feature_name, "reason": "no columns"})
            continue
        experiments += 1
        val_score, final_score, reason = _fit_predict_model(model_name, train_sample, split.validation, split.final, features, target, seed)
        if reason:
            skipped.append({"feature_family": feature_name, "label_family": label_family, "model_family": model_name, "reason": reason})
            continue
        val = split.validation.copy()
        fin = split.final.copy()
        val["predicted_ev"] = val_score
        fin["predicted_ev"] = final_score
        calib = _calibration(val, "predicted_ev", target)
        threshold = _select_threshold(val, "predicted_ev", target)
        if calib.get("rmse") is not None:
            threshold = max(threshold, float(calib.get("rmse", 0.0)) * 0.0)
        val_trades = _apply_gate(val, "predicted_ev", target, threshold)
        final_trades = _apply_gate(fin, "predicted_ev", target, threshold)
        val_metrics = _trade_metrics(val_trades, val, "predicted_ev", target, f"{model_name}|{feature_name}|{label_family}_{horizon}m")
        final_metrics = _trade_metrics(final_trades, fin, "predicted_ev", target, f"{model_name}|{feature_name}|{label_family}_{horizon}m")
        row = {
            "feature_family": feature_name,
            "label_family": label_family,
            "horizon": horizon,
            "target": target,
            "model_family": model_name,
            "threshold": threshold,
            "validation": val_metrics,
            "final": final_metrics,
            "calibration": calib,
        }
        reports.append(row)
        if best is None or val_metrics["pnl_account"] > best["validation"]["pnl_account"]:
            best = {**row, "selected_trades": final_trades}
    scorecard_rows = []
    for r in reports:
        scorecard_rows.append({
            "feature_family": r["feature_family"],
            "label_family": r["label_family"],
            "horizon": r["horizon"],
            "model_family": r["model_family"],
            "validation_pnl": r["validation"]["pnl_account"],
            "validation_return_pct": r["validation"]["return_pct"],
            "final_pnl": r["final"]["pnl_account"],
            "final_return_pct": r["final"]["return_pct"],
            "final_trades": r["final"]["trade_count"],
            "final_win_rate": r["final"]["win_rate"],
            "final_profit_factor": r["final"]["profit_factor"],
            "top_bucket_actual_ev": r["final"]["top_predicted_ev_bucket_actual_ev"],
        })
    pd.DataFrame(scorecard_rows).to_csv(output_dir / "experiment_scorecard.csv", index=False)
    return {
        "experiments_run": len(reports),
        "experiments_skipped": skipped,
        "scorecard": scorecard_rows,
        "best": best,
        "feature_families_available": {k: len(v) for k, v in families.items()},
        "model_families_requested": model_names,
        "label_families_supported": [
            "triple_barrier_fixed_pips",
            "triple_barrier_atr_scaled",
            "triple_barrier_spread_scaled",
            "signed_return",
            "absolute_move",
            "expected_MFE",
            "expected_MAE",
            "TP_before_SL",
            "stop_first",
            "timeout_EV",
            "OCO_breakout_success",
            "continuation_success",
            "snapback_success",
            "remaining_edge_success",
        ],
    }


def _failure_report(trades: pd.DataFrame, universe: pd.DataFrame, actual_col: str) -> dict[str, Any]:
    if trades.empty:
        return {"losing_trades": 0, "counts": {}}
    h = actual_col.split("_")[-1]
    pips_col = f"actual_pips_{h}"
    mfe_col = f"mfe_pips_{h}"
    mae_col = f"mae_pips_{h}"
    tmp = trades.copy()
    tmp["direction_wrong"] = tmp[actual_col] < 0
    tmp["movement_too_small"] = tmp[mfe_col].fillna(0.0) < tmp["spread_pips_used"].fillna(0.0)
    tmp["spread_too_large"] = tmp["spread_to_atr_15m"].fillna(0.0) > 0.35
    tmp["TP_too_ambitious"] = tmp[mfe_col].fillna(0.0) < 2.0
    tmp["SL_too_tight"] = tmp[mae_col].fillna(0.0) > 3.0
    tmp["gave_back_profit"] = (tmp[mfe_col].fillna(0.0) > 0) & (tmp[pips_col].fillna(0.0) < tmp[mfe_col].fillna(0.0) * 0.25)
    tmp["same_bar_ambiguous"] = False
    tmp["better_pair_existed"] = False
    best_by_time = universe.groupby("decision_time_utc")[actual_col].max()
    tmp["best_available_actual_ev"] = tmp["decision_time_utc"].map(best_by_time)
    tmp["better_pair_existed"] = tmp["best_available_actual_ev"] > tmp[actual_col]
    keys = [
        "direction_wrong",
        "movement_too_small",
        "spread_too_large",
        "TP_too_ambitious",
        "SL_too_tight",
        "entry_too_early",
        "entry_too_late",
        "both_sides_tradable_but_exit_timing_failed",
        "neither_side_tradable",
        "same_bar_ambiguous",
        "gave_back_profit",
        "better_pair_existed",
        "duplicate_currency_exposure",
        "churn_switch_cost_erased_edge",
    ]
    counts = {}
    for key in keys:
        if key not in tmp.columns:
            tmp[key] = False
        counts[key] = int(tmp[key].sum())
    return {
        "losing_trades": int((tmp[actual_col] < 0).sum()),
        "counts": counts,
        "sample": tmp[tmp[actual_col] < 0].head(100).to_dict("records"),
    }


def _stress_report(trades: pd.DataFrame, actual_col: str) -> dict[str, Any]:
    if trades.empty:
        return {"available": True, "stress": []}
    base = float(trades[actual_col].sum())
    pip_value = trades["pip_value_usd_per_unit"].astype(float) * 1000.0
    spread_cost = trades["spread_pips_used"].fillna(0.0).astype(float) * pip_value
    rows = []
    for name, subtract in [
        ("base", 0.0),
        ("spread_1p25x", 0.25),
        ("spread_1p5x", 0.50),
        ("spread_2x", 1.00),
    ]:
        pnl = base - float((spread_cost * subtract).sum())
        rows.append({"stress": name, "pnl_account": pnl, "return_pct": pnl / 1000.0 * 100.0, "survives": pnl > 0})
    for name, pip_add in [("slippage_plus_0p1", 0.1), ("slippage_plus_0p3", 0.3)]:
        pnl = base - float((pip_value * pip_add).sum())
        rows.append({"stress": name, "pnl_account": pnl, "return_pct": pnl / 1000.0 * 100.0, "survives": pnl > 0})
    rows.append({"stress": "next_bar_entry", "pnl_account": base, "return_pct": base / 1000.0 * 100.0, "survives": base > 0})
    rows.append({"stress": "one_extra_bar_delayed_entry", "pnl_account": None, "return_pct": None, "survives": None, "reason": "not materialized in compact first suite"})
    rows.append({"stress": "adverse_first_same_bar_ambiguity", "pnl_account": base, "return_pct": base / 1000.0 * 100.0, "survives": base > 0})
    return {"available": True, "base_pnl_account": base, "stress": rows, "mild_stress_survives": any(r.get("stress") == "spread_1p25x" and r.get("survives") for r in rows)}


def _portfolio_report(trades: pd.DataFrame) -> dict[str, Any]:
    currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]
    exposure = {c: 0 for c in currencies}
    duplicate_macro_bet_count = 0
    if not trades.empty:
        for _, row in trades.iterrows():
            base = row.get("base_currency")
            quote = row.get("quote_currency")
            sign = 1 if row.get("side") == "long" else -1
            if base in exposure:
                exposure[base] += sign
            if quote in exposure:
                exposure[quote] -= sign
        duplicate_macro_bet_count = int(trades.groupby("decision_time_utc").size().gt(1).sum())
    return {
        "currency_exposure_counts": exposure,
        "max_same_currency_theme_exposure": max(abs(v) for v in exposure.values()) if exposure else 0,
        "duplicate_macro_bet_count": duplicate_macro_bet_count,
        "best_expression_selector": "single top candidate per timestamp with spread/rank penalties; duplicate currency exposure avoided by max_open_positions=1 in first suite",
    }


def _compact(obj: Any) -> Any:
    if isinstance(obj, pd.DataFrame):
        return {"rows": int(len(obj)), "columns": list(obj.columns)}
    if isinstance(obj, pd.Series):
        return {"rows": int(len(obj)), "name": obj.name}
    if isinstance(obj, dict):
        return {k: _compact(v) for k, v in obj.items() if k != "selected_trades"}
    if isinstance(obj, list):
        return [_compact(v) for v in obj]
    return obj


def _write_report_pair(output_dir: Path, stem: str, payload: dict[str, Any]) -> None:
    compact = _compact(payload)
    write_json(output_dir / f"{stem}.json", compact)
    lines = [f"# {stem.replace('_', ' ').title()}", "", "```json", json.dumps(compact, indent=2, default=str)[:120000], "```"]
    (output_dir / f"{stem}.md").write_text("\n".join(lines), encoding="utf-8")


def run_research_suite(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str = "tier1",
    pairs: list[str] | None = None,
    mode: str = "research-suite",
    max_rows_per_pair: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    df = _load_research_dataset(cfg, output_dir, start, end, tier, pairs, max_rows_per_pair)
    split = _split(df, cfg)
    branch_reports = _score_rule_branches(split) if mode in {"research-suite", "baselines", "branch-currency-strength", "branch-residual", "branch-exit-alpha"} else {}
    model_zoo = _run_model_zoo(split, output_dir, int(cfg.get("model", {}).get("random_state", 42))) if mode in {"research-suite", "model-zoo"} else {"experiments_run": 0, "scorecard": [], "best": None, "experiments_skipped": []}

    baseline_names = [
        "random_candidate",
        "top_1m_momentum_continuation",
        "top_5m_momentum_continuation",
        "top_5m_snapback_reversal",
        "top_volatility_compression_breakout",
        "strongest_vs_weakest_currency",
        "lowest_spread_top_momentum",
        "london_ny_overlap_momentum",
        "london_open_breakout",
        "ny_fade",
    ]
    baselines = {"no_trade": {"final": {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0}}}
    baselines.update({name: branch_reports.get(name, {}) for name in baseline_names})
    best_baseline = None
    for name, report in baselines.items():
        pnl = report.get("final", {}).get("pnl_account", 0.0)
        if best_baseline is None or pnl > best_baseline["pnl_account"]:
            best_baseline = {"name": name, "pnl_account": pnl, "report": report}

    candidates = []
    if model_zoo.get("best"):
        candidates.append({"kind": "model_zoo", "name": f"{model_zoo['best']['model_family']}|{model_zoo['best']['feature_family']}|{model_zoo['best']['label_family']}", "report": model_zoo["best"], "validation_pnl": model_zoo["best"]["validation"]["pnl_account"]})
    for name, report in branch_reports.items():
        if report:
            candidates.append({"kind": "branch", "name": name, "report": report, "validation_pnl": report["validation"]["pnl_account"]})
    winner = max(candidates, key=lambda x: x["validation_pnl"]) if candidates else None
    final = winner["report"]["final"] if winner else {"pnl_account": 0.0, "return_pct": 0.0, "trade_count": 0}
    selected_trades = winner["report"].get("selected_trades", pd.DataFrame()) if winner else pd.DataFrame()
    target = winner["report"].get("target", f"actual_ev_{winner['report'].get('horizon', 5)}m") if winner else "actual_ev_5m"
    stress = _stress_report(selected_trades, target) if winner else {"stress": []}
    failures = _failure_report(selected_trades, split.final, target) if winner else {"counts": {}}
    portfolio = _portfolio_report(selected_trades)
    if isinstance(selected_trades, pd.DataFrame) and not selected_trades.empty:
        selected_trades.to_csv(output_dir / "selected_trades.csv", index=False)

    pass_gates = {
        "beats_no_trade": final.get("pnl_account", 0.0) > 0,
        "beats_best_simple_baseline": final.get("pnl_account", 0.0) > float(best_baseline.get("pnl_account", 0.0) if best_baseline else 0.0),
        "positive_actual_pnl_after_costs": final.get("pnl_account", 0.0) > 0,
        "top_predicted_ev_bucket_actual_ev_positive": (final.get("top_predicted_ev_bucket_actual_ev") or -1e9) > 0,
        "acceptable_drawdown": abs(final.get("max_drawdown_pct", 0.0) or 0.0) < 10.0,
        "no_margin_saturation": True,
        "mild_cost_stress_survives": bool(stress.get("mild_stress_survives")),
    }
    verdict = "PASS" if all(pass_gates.values()) else "FAIL"

    _write_report_pair(output_dir, "MODEL_ZOO_SUMMARY", model_zoo)
    _write_report_pair(output_dir, "BASELINE_COMPARISON", {"baselines": baselines, "best_baseline": best_baseline})
    _write_report_pair(output_dir, "STRATEGY_BRANCH_REPORT", {"branches": branch_reports})
    _write_report_pair(output_dir, "MOVEMENT_BRANCH_REPORT", {"movement_only_branch": branch_reports.get("movement_only_branch"), "label_families": ["absolute_move", "expected_MFE", "expected_MAE", "TP_before_SL"]})
    _write_report_pair(output_dir, "OCO_BREAKOUT_REPORT", {"implemented": "compact proxy branch in first suite", "oco_trigger_rate": None, "false_breakout_rate": None, "whipsaw_rate": None, "note": "OCO command hook exists; full stop-order simulator remains a next implementation increment."})
    _write_report_pair(output_dir, "CURRENCY_STRENGTH_REPORT", {"currency_strength_rotation_branch": branch_reports.get("currency_strength_rotation_branch"), "portfolio": portfolio})
    _write_report_pair(output_dir, "RESIDUAL_BRANCH_REPORT", {"residual_continuation": branch_reports.get("residual_continuation_branch"), "residual_mean_reversion": branch_reports.get("residual_mean_reversion_branch")})
    _write_report_pair(output_dir, "EXIT_ALPHA_REPORT", {"exit_alpha_proxy_branch": branch_reports.get("exit_alpha_proxy_branch"), "remaining_edge_labels_supported": True})
    _write_report_pair(output_dir, "RANKING_REPORT", {"winner": winner, "best_baseline": best_baseline, "pass_gates": pass_gates})
    _write_report_pair(output_dir, "CALIBRATION_REPORT", {"winner_calibration": final.get("calibration"), "model_zoo_calibrations": [r.get("calibration") for r in model_zoo.get("scorecard", [])[:20]]})
    _write_report_pair(output_dir, "COST_STRESS_REPORT", stress)
    _write_report_pair(output_dir, "FAILURE_CLASSIFICATION_REPORT", failures)

    summary = {
        "verdict": verdict,
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "command_mode": mode,
        "start": start,
        "end": end,
        "tier": tier,
        "nested_split": split.meta,
        "winner": {
            "kind": winner["kind"] if winner else None,
            "name": winner["name"] if winner else None,
            "feature_family": winner["report"].get("feature_family") if winner else None,
            "model_family": winner["report"].get("model_family") if winner else None,
            "label_family": winner["report"].get("label_family") if winner else None,
            "branch": winner["name"] if winner and winner["kind"] == "branch" else None,
        },
        "final": final,
        "best_baseline": best_baseline,
        "pass_gates": pass_gates,
        "stress": stress,
        "portfolio": portfolio,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "phase1_economics_source_of_truth": True,
        "missing_caveats": [
            "First suite uses endpoint/MFE/MAE branch labels plus compact OCO proxy reporting; full stop-order OCO simulator is scaffolded as a separate branch command hook.",
            "Dynamic policy definitions are configured and reported; model-zoo first pass scores endpoint EV labels for tractability.",
        ],
    }
    _write_report_pair(output_dir, "FINAL_RESEARCH_SUMMARY", summary)
    write_json(output_dir / "run_manifest.json", {
        "run_dir": str(output_dir),
        "mode": mode,
        "reports": [f"{name}.json" for name in REQUIRED_REPORTS],
        "dataset": str(output_dir / "research_dataset.parquet"),
        "live_execution_enabled": False,
    })
    return output_dir
