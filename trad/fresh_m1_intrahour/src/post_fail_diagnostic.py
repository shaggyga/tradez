from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from .common import (
    add_time_features,
    max_drawdown,
    pair_meta,
    parse_date,
    pip_value_usd_per_unit,
    quote_to_usd_from_prices,
    read_candles,
    select_pairs,
    write_json,
)
from .dataset import add_pair_features
from .economics import (
    ExecutablePriceArrays,
    execution_settings_from_config,
    executable_price_arrays,
    prepare_executable_prices,
    simulate_tp_sl_contract_arrays,
)
from .features import feature_family_columns
from .research_suite import (
    HORIZONS,
    _clean_x,
    _load_research_dataset,
    _select_threshold,
    _session_from_time,
    _split,
    _top_by_timestamp,
)


try:
    from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
except Exception:  # pragma: no cover - reported in artifacts
    HistGradientBoostingClassifier = None
    HistGradientBoostingRegressor = None


POSITION_UNITS = 1000.0
PRIMARY_POLICY_HORIZONS = [3, 5, 8, 13, 21]
DIAGNOSTIC_HORIZONS = [1, 3, 5, 8, 13, 21, 30]


@dataclass
class MarketCache:
    pair: str
    frame: pd.DataFrame
    arrays: ExecutablePriceArrays
    pip_size: float
    pip_value_usd_per_unit: float
    settings: Any
    time_to_index: dict[pd.Timestamp, int]


def _json_safe(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if isinstance(value, pd.Series):
        return {str(k): _json_safe(v) for k, v in value.to_dict().items()}
    if isinstance(value, pd.DataFrame):
        return {
            "rows": int(len(value)),
            "columns": list(value.columns),
        }
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(v) for v in value.tolist()]
    return value


def _write_report(output_dir: Path, stem: str, payload: dict[str, Any]) -> None:
    clean = _json_safe(payload)
    write_json(output_dir / f"{stem}.json", clean)
    text = json.dumps(clean, indent=2, sort_keys=True, default=str)
    lines = [f"# {stem.replace('_', ' ').title()}", "", "```json", text[:180000], "```"]
    (output_dir / f"{stem}.md").write_text("\n".join(lines), encoding="utf-8")


def _series_or_empty(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def _safe_mean(values: pd.Series | np.ndarray) -> float | None:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    return float(arr.mean()) if len(arr) else None


def _safe_sum(values: pd.Series | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    return float(np.nansum(arr))


def _bucket_from_train(values: pd.Series, train_values: pd.Series, name: str) -> pd.Series:
    q = pd.to_numeric(train_values, errors="coerce").dropna().quantile([0.33, 0.67]).to_numpy(dtype=float)
    if len(q) != 2 or not np.isfinite(q).all() or q[0] == q[1]:
        return pd.Series("unknown", index=values.index)
    return pd.Series(
        np.select([values <= q[0], values <= q[1]], [f"low_{name}", f"mid_{name}"], default=f"high_{name}"),
        index=values.index,
    )


def _metrics(
    trades: pd.DataFrame,
    value_col: str = "actual_ev",
    pips_col: str = "actual_pips",
    initial_equity: float = 1000.0,
) -> dict[str, Any]:
    if trades.empty or value_col not in trades.columns:
        return {
            "pnl_account": 0.0,
            "return_pct": 0.0,
            "trade_count": 0,
            "win_rate": None,
            "profit_factor": None,
            "max_drawdown_pct": 0.0,
            "average_pips": None,
            "mfe_capture_ratio": None,
            "giveback_pips": None,
            "spread_paid": 0.0,
        }
    ordered = trades.sort_values("decision_time_utc") if "decision_time_utc" in trades.columns else trades
    pnl = pd.to_numeric(ordered[value_col], errors="coerce").fillna(0.0)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]
    equity = [initial_equity] + (initial_equity + pnl.cumsum()).tolist()
    pips = pd.to_numeric(ordered.get(pips_col, pd.Series(np.nan, index=ordered.index)), errors="coerce")
    mfe = pd.to_numeric(ordered.get("mfe_pips", pd.Series(np.nan, index=ordered.index)), errors="coerce")
    capture = (pips / mfe.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
    giveback = mfe - pips
    pip_value = pd.to_numeric(ordered.get("pip_value_usd_per_unit", pd.Series(0.0, index=ordered.index)), errors="coerce").fillna(0.0)
    spread = pd.to_numeric(ordered.get("spread_pips_used", pd.Series(0.0, index=ordered.index)), errors="coerce").fillna(0.0)
    weight = pd.to_numeric(ordered.get("allocation_weight", pd.Series(1.0, index=ordered.index)), errors="coerce").fillna(1.0)
    return {
        "pnl_account": float(pnl.sum()),
        "return_pct": float(pnl.sum() / initial_equity * 100.0),
        "trade_count": int(len(ordered)),
        "win_rate": float((pnl > 0).mean()) if len(ordered) else None,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and abs(losses.sum()) > 0 else None,
        "max_drawdown_pct": max_drawdown(equity),
        "average_pips": _safe_mean(pips),
        "mfe_capture_ratio": _safe_mean(capture),
        "giveback_pips": _safe_mean(giveback),
        "spread_paid": float((spread * pip_value * POSITION_UNITS * weight).sum()),
    }


def _breakdown(trades: pd.DataFrame, by: str, value_col: str = "actual_ev") -> list[dict[str, Any]]:
    if trades.empty or by not in trades.columns or value_col not in trades.columns:
        return []
    rows: list[dict[str, Any]] = []
    for key, group in trades.groupby(by, dropna=False):
        metric = _metrics(group, value_col=value_col)
        rows.append({"name": str(key), **metric})
    return sorted(rows, key=lambda row: row["pnl_account"], reverse=True)


def _select_top(df: pd.DataFrame, score_col: str, top_k: int = 1) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    return (
        df.dropna(subset=[score_col])
        .sort_values(["decision_time_utc", score_col], ascending=[True, False])
        .groupby("decision_time_utc", as_index=False)
        .head(top_k)
        .copy()
    )


def _evaluate_scored_strategy(
    validation: pd.DataFrame,
    final: pd.DataFrame,
    score_col: str,
    actual_col: str,
    name: str,
) -> dict[str, Any]:
    val = validation.copy()
    fin = final.copy()
    val["actual_ev"] = _series_or_empty(val, actual_col)
    fin["actual_ev"] = _series_or_empty(fin, actual_col)
    val["actual_pips"] = _series_or_empty(val, actual_col.replace("actual_ev_", "actual_pips_"))
    fin["actual_pips"] = _series_or_empty(fin, actual_col.replace("actual_ev_", "actual_pips_"))
    h = actual_col.removeprefix("actual_ev_")
    for frame in [val, fin]:
        frame["mfe_pips"] = _series_or_empty(frame, f"mfe_pips_{h}", np.nan)
        frame["mae_pips"] = _series_or_empty(frame, f"mae_pips_{h}", np.nan)
    threshold = _select_threshold(val, score_col, actual_col)
    val_raw = _select_top(val, score_col)
    fin_raw = _select_top(fin, score_col)
    val_gated = val_raw.iloc[0:0].copy() if math.isinf(threshold) else val_raw[val_raw[score_col] >= threshold].copy()
    fin_gated = fin_raw.iloc[0:0].copy() if math.isinf(threshold) else fin_raw[fin_raw[score_col] >= threshold].copy()
    return {
        "name": name,
        "actual_col": actual_col,
        "threshold": threshold,
        "validation_raw": _metrics(val_raw),
        "validation_gated": _metrics(val_gated),
        "final_raw": _metrics(fin_raw),
        "final_gated": _metrics(fin_gated),
        "validation_raw_trades": val_raw,
        "validation_gated_trades": val_gated,
        "final_raw_trades": fin_raw,
        "final_gated_trades": fin_gated,
    }


def _calibration(scored: pd.DataFrame, score_col: str, actual_col: str) -> dict[str, Any]:
    if score_col not in scored.columns or actual_col not in scored.columns:
        return {"slope": None, "intercept": None, "rmse": None, "top_decile_actual_ev": None, "deciles": []}
    tmp = scored[[score_col, actual_col]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(tmp) < 20 or tmp[score_col].nunique() < 2:
        return {"slope": None, "intercept": None, "rmse": None, "top_decile_actual_ev": None, "deciles": []}
    x = tmp[score_col].to_numpy(dtype=float)
    y = tmp[actual_col].to_numpy(dtype=float)
    slope, intercept = np.polyfit(x, y, 1)
    rmse = float(np.sqrt(np.mean((intercept + slope * x - y) ** 2)))
    tmp = tmp.copy()
    tmp["decile"] = pd.qcut(tmp[score_col].rank(method="first"), 10, labels=False, duplicates="drop")
    deciles = [
        {
            "decile": int(decile),
            "rows": int(len(group)),
            "predicted_mean": float(group[score_col].mean()),
            "actual_mean": float(group[actual_col].mean()),
        }
        for decile, group in tmp.groupby("decile")
    ]
    return {
        "slope": float(slope),
        "intercept": float(intercept),
        "rmse": rmse,
        "top_decile_actual_ev": float(deciles[-1]["actual_mean"]) if deciles else None,
        "deciles": deciles,
    }


def _binary_metrics(probability: pd.Series, target: pd.Series) -> dict[str, Any]:
    tmp = pd.DataFrame({"probability": probability, "target": target}).replace([np.inf, -np.inf], np.nan).dropna()
    if tmp.empty:
        return {"rows": 0, "accuracy": None, "brier": None, "positive_rate": None, "auc_proxy": None}
    y = tmp["target"].astype(int).to_numpy()
    p = tmp["probability"].astype(float).clip(0.0, 1.0).to_numpy()
    accuracy = float(((p >= 0.5).astype(int) == y).mean())
    brier = float(np.mean((p - y) ** 2))
    auc_proxy = None
    if len(np.unique(y)) == 2:
        ranks = pd.Series(p).rank(method="average").to_numpy()
        positive_rank_sum = ranks[y == 1].sum()
        n_pos = int((y == 1).sum())
        n_neg = int((y == 0).sum())
        auc_proxy = float((positive_rank_sum - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg))
    return {
        "rows": int(len(tmp)),
        "accuracy": accuracy,
        "brier": brier,
        "positive_rate": float(y.mean()),
        "auc_proxy": auc_proxy,
    }


def _add_mid_direction_columns(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    pip_map = {pair: pair_meta(pair, cfg).pip_size for pair in out["pair"].dropna().unique()}
    pip = out["pair"].map(pip_map).astype(float)
    grouped = out.groupby(["pair", "side"], sort=False)
    entry = grouped["open"].shift(-1)
    for horizon in HORIZONS:
        exit_close = grouped["close"].shift(-int(horizon))
        out[f"mid_direction_pips_{horizon}m"] = out["side_sign"].astype(float) * (exit_close - entry) / pip
    return out


def _load_market_caches(
    cfg: dict[str, Any],
    start: str,
    end: str,
    tier: str,
    pairs: list[str] | None,
    max_rows_per_pair: int | None,
) -> dict[str, MarketCache]:
    caches: dict[str, MarketCache] = {}
    start_ts = parse_date(start)
    end_ts = parse_date(end)
    for pair in select_pairs(cfg, tier, pairs):
        raw = read_candles(pair, cfg, start_ts, end_ts, max_rows=max_rows_per_pair)
        if len(raw) < 100:
            continue
        raw = add_time_features(raw)
        raw = add_pair_features(raw, cfg)
        meta = pair_meta(pair, cfg)
        settings = execution_settings_from_config(cfg, meta.tier)
        exec_df, _ = prepare_executable_prices(raw, pair, cfg, settings.spread_multiplier)
        quote_to_usd = quote_to_usd_from_prices(pair, float(exec_df["close"].iloc[-1]), {pair: float(exec_df["close"].iloc[-1])})
        pip_value = float(pip_value_usd_per_unit(pair, quote_to_usd))
        time_to_index = {pd.Timestamp(ts): int(i) for i, ts in enumerate(exec_df["decision_time_utc"])}
        caches[pair] = MarketCache(
            pair=pair,
            frame=exec_df,
            arrays=executable_price_arrays(exec_df),
            pip_size=meta.pip_size,
            pip_value_usd_per_unit=pip_value,
            settings=settings,
            time_to_index=time_to_index,
        )
    if not caches:
        raise RuntimeError("no market cache data available")
    return caches


def _endpoint_pips(cache: MarketCache, decision_index: int, side: str, horizon: int, entry_delay: int | None = None) -> float:
    delay = cache.settings.entry_delay_bars if entry_delay is None else int(entry_delay)
    entry_i = int(decision_index) + delay
    exit_i = entry_i + max(int(horizon), 1) - 1
    if entry_i < 0 or exit_i >= len(cache.frame):
        return float("nan")
    use_open = cache.settings.entry_price == "open" and delay > 0
    if side == "long":
        entry = float(cache.arrays.ask_open[entry_i] if use_open else cache.arrays.ask_close[entry_i])
        exit_price = float(cache.arrays.bid_close[exit_i])
        return float((exit_price - entry) / cache.pip_size - cache.settings.slippage_pips_round_trip)
    entry = float(cache.arrays.bid_open[entry_i] if use_open else cache.arrays.bid_close[entry_i])
    exit_price = float(cache.arrays.ask_close[exit_i])
    return float((entry - exit_price) / cache.pip_size - cache.settings.slippage_pips_round_trip)


def _endpoint_at_exit_index(cache: MarketCache, decision_index: int, side: str, exit_index: int) -> float:
    entry_i = int(decision_index) + cache.settings.entry_delay_bars
    if entry_i < 0 or exit_index < entry_i or exit_index >= len(cache.frame):
        return float("nan")
    if side == "long":
        entry = float(cache.arrays.ask_open[entry_i])
        return float((float(cache.arrays.bid_close[exit_index]) - entry) / cache.pip_size - cache.settings.slippage_pips_round_trip)
    entry = float(cache.arrays.bid_open[entry_i])
    return float((entry - float(cache.arrays.ask_close[exit_index])) / cache.pip_size - cache.settings.slippage_pips_round_trip)


def _apply_endpoint_variant(trades: pd.DataFrame, caches: dict[str, MarketCache], horizon: int, delayed: bool = False) -> pd.DataFrame:
    out = trades.copy()
    pips: list[float] = []
    for row in out.itertuples(index=False):
        cache = caches.get(getattr(row, "pair"))
        ts = pd.Timestamp(getattr(row, "decision_time_utc"))
        index = cache.time_to_index.get(ts) if cache else None
        pips.append(_endpoint_pips(cache, index, getattr(row, "side"), horizon, entry_delay=2 if delayed else None) if index is not None else np.nan)
    out["actual_pips"] = pips
    out["actual_ev"] = out["actual_pips"].astype(float) * out["pip_value_usd_per_unit"].astype(float) * POSITION_UNITS
    return out


def _portfolio_exposure(trades: pd.DataFrame) -> dict[str, Any]:
    currencies = ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]
    totals = {currency: 0 for currency in currencies}
    if not trades.empty:
        for row in trades.itertuples(index=False):
            sign = 1 if getattr(row, "side") == "long" else -1
            base = getattr(row, "base_currency", None)
            quote = getattr(row, "quote_currency", None)
            if base in totals:
                totals[base] += sign
            if quote in totals:
                totals[quote] -= sign
    return {
        "currency_exposure_counts": totals,
        "max_absolute_exposure": max(abs(v) for v in totals.values()) if totals else 0,
        "duplicate_currency_theme_events": int(trades.groupby("decision_time_utc").size().gt(1).sum()) if not trades.empty else 0,
    }


def _sample_train(df: pd.DataFrame, limit: int = 24000) -> pd.DataFrame:
    if len(df) <= limit:
        return df
    return df.sample(limit, random_state=42).sort_values("decision_time_utc")


def _fit_regressor(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    final: pd.DataFrame,
    features: list[str],
    target: pd.Series,
    fallback_column: str = "side_momentum_5m_atr",
) -> tuple[pd.Series, pd.Series, dict[str, Any]]:
    fallback_val = _series_or_empty(validation, fallback_column)
    fallback_final = _series_or_empty(final, fallback_column)
    if HistGradientBoostingRegressor is None or not features:
        return fallback_val, fallback_final, {"available": False, "reason": "HistGradientBoostingRegressor unavailable"}
    try:
        model = HistGradientBoostingRegressor(
            learning_rate=0.06,
            max_iter=55,
            max_leaf_nodes=17,
            min_samples_leaf=120,
            l2_regularization=0.15,
            random_state=42,
        )
        model.fit(_clean_x(train, features), target.astype(float).fillna(0.0))
        return (
            pd.Series(model.predict(_clean_x(validation, features)), index=validation.index),
            pd.Series(model.predict(_clean_x(final, features)), index=final.index),
            {"available": True, "model": "HistGradientBoostingRegressor", "feature_count": len(features)},
        )
    except Exception as exc:  # pragma: no cover - depends on optional sklearn runtime
        return fallback_val, fallback_final, {"available": False, "reason": str(exc)}


def _fit_classifier(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    final: pd.DataFrame,
    features: list[str],
    target: pd.Series,
    fallback_column: str = "side_momentum_5m_atr",
) -> tuple[pd.Series, pd.Series, dict[str, Any]]:
    fallback_val = (_series_or_empty(validation, fallback_column) > 0).astype(float)
    fallback_final = (_series_or_empty(final, fallback_column) > 0).astype(float)
    y = target.astype(int).fillna(0)
    if HistGradientBoostingClassifier is None or not features or y.nunique() < 2:
        return fallback_val, fallback_final, {"available": False, "reason": "classifier unavailable or target is single-class"}
    try:
        model = HistGradientBoostingClassifier(
            learning_rate=0.06,
            max_iter=55,
            max_leaf_nodes=17,
            min_samples_leaf=120,
            l2_regularization=0.15,
            random_state=42,
        )
        model.fit(_clean_x(train, features), y)
        positive_index = list(model.classes_).index(1)
        return (
            pd.Series(model.predict_proba(_clean_x(validation, features))[:, positive_index], index=validation.index),
            pd.Series(model.predict_proba(_clean_x(final, features))[:, positive_index], index=final.index),
            {"available": True, "model": "HistGradientBoostingClassifier", "feature_count": len(features)},
        )
    except Exception as exc:  # pragma: no cover - depends on optional sklearn runtime
        return fallback_val, fallback_final, {"available": False, "reason": str(exc)}


def _build_diagnostic_model_scores(split: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    train = _sample_train(split.train)
    validation = split.validation.copy()
    final = split.final.copy()
    families = feature_family_columns(train)
    directional_features = families.get("B_m1_spread_cost_liquidity", [])
    movement_features = sorted(set(
        families.get("C_m1_volatility_motion", [])
        + families.get("H_movement_quality", [])
        + families.get("B_m1_spread_cost_liquidity", [])
        + families.get("F_m1_session_time", [])
    ))
    movement_features = [column for column in movement_features if not column.startswith("side_")]
    if not movement_features:
        movement_features = directional_features

    ev_reg_val, ev_reg_final, ev_meta = _fit_regressor(
        train,
        validation,
        final,
        directional_features,
        train["actual_ev_13m"],
    )
    pos_val, pos_final, pos_meta = _fit_classifier(
        train,
        validation,
        final,
        directional_features,
        (train["actual_ev_13m"] > 0).astype(int),
    )
    no_edge_val, no_edge_final, no_edge_meta = _fit_classifier(
        train,
        validation,
        final,
        directional_features,
        (train["actual_ev_13m"] <= 0).astype(int),
    )
    validation["ev_regression"] = ev_reg_val
    final["ev_regression"] = ev_reg_final
    validation["positive_probability"] = pos_val
    final["positive_probability"] = pos_final
    validation["predicted_ev"] = ev_reg_val * pos_val
    final["predicted_ev"] = ev_reg_final * pos_final
    validation["no_edge_probability"] = no_edge_val
    final["no_edge_probability"] = no_edge_final

    horizon_reports: list[dict[str, Any]] = []
    for horizon in DIAGNOSTIC_HORIZONS:
        direction_target = (train[f"mid_direction_pips_{horizon}m"] > 0).astype(int)
        movement_threshold = _series_or_empty(train, "round_trip_cost_pips_used", 0.0)
        movement_target = (train[f"future_range_pips_{horizon}m"] > movement_threshold).astype(int)
        direction_val, direction_final, direction_meta = _fit_classifier(
            train,
            validation,
            final,
            directional_features,
            direction_target,
        )
        movement_val, movement_final, movement_meta = _fit_classifier(
            train,
            validation,
            final,
            movement_features,
            movement_target,
            fallback_column="movement_quality",
        )
        validation[f"direction_probability_{horizon}m"] = direction_val
        final[f"direction_probability_{horizon}m"] = direction_final
        validation[f"movement_probability_{horizon}m"] = movement_val
        final[f"movement_probability_{horizon}m"] = movement_final
        final_direction_target = (final[f"mid_direction_pips_{horizon}m"] > 0).astype(int)
        final_movement_target = (final[f"future_range_pips_{horizon}m"] > _series_or_empty(final, "round_trip_cost_pips_used", 0.0)).astype(int)
        signed_corr = pd.Series(direction_final, index=final.index).corr(final[f"actual_ev_{horizon}m"], method="spearman")
        horizon_reports.append({
            "horizon_min": horizon,
            "direction": _binary_metrics(direction_final, final_direction_target),
            "movement": _binary_metrics(movement_final, final_movement_target),
            "signed_return_spearman": float(signed_corr) if pd.notna(signed_corr) else None,
            "direction_model": direction_meta,
            "movement_model": movement_meta,
        })
    metadata = {
        "prior_winner_retested": "calibrated_classifier_ev_regressor | B_m1_spread_cost_liquidity | signed_return | 13m",
        "train_rows_used": int(len(train)),
        "directional_feature_count": int(len(directional_features)),
        "movement_feature_count": int(len(movement_features)),
        "ev_regressor": ev_meta,
        "positive_classifier": pos_meta,
        "no_edge_classifier": no_edge_meta,
        "horizon_models": horizon_reports,
    }
    return validation, final, metadata


def _simulate_market_contract_vector(
    cache: MarketCache,
    tp_pips: np.ndarray,
    sl_pips: np.ndarray,
    horizon: int,
    side: str,
) -> dict[str, np.ndarray]:
    n = len(cache.frame)
    indices = np.arange(n, dtype=int)
    delay = int(cache.settings.entry_delay_bars)
    entry_indices = indices + delay
    end_indices = entry_indices + max(int(horizon), 1) - 1
    valid = (entry_indices >= 0) & (end_indices < n)
    safe_entry = np.clip(entry_indices, 0, max(n - 1, 0))
    safe_end = np.clip(end_indices, 0, max(n - 1, 0))
    slip = float(cache.settings.slippage_pips_round_trip)
    tp = np.asarray(tp_pips, dtype=float)
    sl = np.asarray(sl_pips, dtype=float)
    if side == "long":
        entry = cache.arrays.ask_open[safe_entry]
        endpoint = (cache.arrays.bid_close[safe_end] - entry) / cache.pip_size - slip
    else:
        entry = cache.arrays.bid_open[safe_entry]
        endpoint = (entry - cache.arrays.ask_close[safe_end]) / cache.pip_size - slip
    realized = np.where(valid, endpoint, np.nan).astype(float)
    outcome = np.full(n, "incomplete", dtype=object)
    outcome[valid] = "timeout"
    exit_bars = np.where(valid, int(horizon), np.nan).astype(float)
    mfe = np.zeros(n, dtype=float)
    mae = np.zeros(n, dtype=float)
    active = valid.copy()
    for offset in range(max(int(horizon), 1)):
        current = entry_indices + offset
        safe_current = np.clip(current, 0, max(n - 1, 0))
        usable = active & (current < n)
        if side == "long":
            fav = (cache.arrays.bid_high[safe_current] - entry) / cache.pip_size
            adverse = (entry - cache.arrays.bid_low[safe_current]) / cache.pip_size
        else:
            fav = (entry - cache.arrays.ask_low[safe_current]) / cache.pip_size
            adverse = (cache.arrays.ask_high[safe_current] - entry) / cache.pip_size
        mfe[usable] = np.maximum(mfe[usable], fav[usable])
        mae[usable] = np.maximum(mae[usable], adverse[usable])
        hit_tp = usable & (fav >= tp)
        hit_sl = usable & (adverse >= sl)
        both = hit_tp & hit_sl
        take_tp = hit_tp & ~both
        take_sl = hit_sl | both
        realized[take_tp] = tp[take_tp] - slip
        realized[take_sl] = -sl[take_sl] - slip
        outcome[take_tp] = "tp_before_sl"
        outcome[hit_sl & ~both] = "sl_before_tp"
        outcome[both] = "ambiguous_adverse_first"
        exit_bars[take_tp | take_sl] = offset + 1
        active[take_tp | take_sl] = False
    return {
        "realized_pips": realized,
        "mfe_pips": np.where(valid, np.maximum(mfe, 0.0), np.nan),
        "mae_pips": np.where(valid, np.maximum(mae, 0.0), np.nan),
        "outcome": outcome,
        "exit_bars": exit_bars,
    }


def _policy_specs(cache: MarketCache, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    frame = cache.frame
    n = len(frame)
    specs: list[dict[str, Any]] = []
    for template in cfg.get("policy_templates", []):
        name = str(template["name"])
        if name not in {"tp2_sl3", "tp3_sl5", "tp5_sl8"}:
            continue
        specs.append({
            "name": f"fixed_{name}",
            "policy_type": "fixed",
            "tp": np.full(n, float(template["tp_pips"])),
            "sl": np.full(n, float(template["sl_pips"])),
        })
    atr = _series_or_empty(frame, "atr_15m_pips", 0.0).to_numpy(dtype=float)
    spread = _series_or_empty(frame, "spread_pips_used", 0.0).to_numpy(dtype=float)
    specs.extend([
        {
            "name": "dynamic_atr_0p35_0p55",
            "policy_type": "dynamic_atr",
            "tp": np.maximum(1.0, 0.35 * atr),
            "sl": np.maximum(1.8, 0.55 * atr),
        },
        {
            "name": "dynamic_spread_2p5_4p0",
            "policy_type": "dynamic_spread",
            "tp": np.maximum(1.0, 2.5 * spread),
            "sl": np.maximum(1.5, 4.0 * spread),
        },
        {
            "name": "hybrid_atr_spread_floor",
            "policy_type": "hybrid",
            "tp": np.maximum.reduce([np.full(n, 1.0), 0.35 * atr, 2.5 * spread]),
            "sl": np.maximum.reduce([np.full(n, 1.8), 0.55 * atr, 4.0 * spread]),
        },
    ])
    for spec in specs:
        spec["tp"] = np.nan_to_num(np.asarray(spec["tp"], dtype=float), nan=1.0, posinf=12.0, neginf=1.0)
        spec["sl"] = np.nan_to_num(np.asarray(spec["sl"], dtype=float), nan=1.5, posinf=18.0, neginf=1.5)
    return specs


def _policy_shape(
    caches: dict[str, MarketCache],
    partition_by_time: dict[pd.Timestamp, str],
    cfg: dict[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pair, cache in caches.items():
        partitions = cache.frame["decision_time_utc"].map(partition_by_time).fillna("excluded")
        for spec in _policy_specs(cache, cfg):
            for horizon in PRIMARY_POLICY_HORIZONS:
                for side in ["long", "short"]:
                    result = _simulate_market_contract_vector(cache, spec["tp"], spec["sl"], horizon, side)
                    pips = result["realized_pips"]
                    for partition in ["validation", "final"]:
                        mask = (partitions.to_numpy() == partition) & np.isfinite(pips)
                        if not mask.any():
                            continue
                        pnl = pips[mask] * cache.pip_value_usd_per_unit * POSITION_UNITS
                        outcomes = result["outcome"][mask]
                        rows.append({
                            "partition": partition,
                            "pair": pair,
                            "policy": spec["name"],
                            "policy_type": spec["policy_type"],
                            "horizon_min": int(horizon),
                            "side": side,
                            "rows": int(mask.sum()),
                            "pnl_account": float(np.nansum(pnl)),
                            "mean_pips": _safe_mean(pips[mask]),
                            "tp_before_sl_rate": float(np.mean(outcomes == "tp_before_sl")),
                            "sl_before_tp_rate": float(np.mean(np.isin(outcomes, ["sl_before_tp", "ambiguous_adverse_first"]))),
                            "timeout_rate": float(np.mean(outcomes == "timeout")),
                            "ambiguous_rate": float(np.mean(outcomes == "ambiguous_adverse_first")),
                            "mean_mfe_pips": _safe_mean(result["mfe_pips"][mask]),
                            "mean_mae_pips": _safe_mean(result["mae_pips"][mask]),
                        })
    table = pd.DataFrame(rows)
    if table.empty:
        return table, {"available": False, "reason": "no policy contracts"}
    aggregate = (
        table.groupby(["partition", "policy", "policy_type", "horizon_min"], as_index=False)
        .agg({
            "rows": "sum",
            "pnl_account": "sum",
            "mean_pips": "mean",
            "tp_before_sl_rate": "mean",
            "sl_before_tp_rate": "mean",
            "timeout_rate": "mean",
            "ambiguous_rate": "mean",
            "mean_mfe_pips": "mean",
            "mean_mae_pips": "mean",
        })
        .sort_values(["partition", "pnl_account"], ascending=[True, False])
    )
    return table, {
        "available": True,
        "contract_engine": "Phase 1 executable bid/ask, next-bar entry, adverse-first TP/SL ambiguity, round-trip slippage",
        "policy_variants": sorted(table["policy"].unique().tolist()),
        "aggregate": aggregate.to_dict("records"),
    }


def _simulate_oco(cache: MarketCache, lookback: int = 15, entry_window: int = 5, horizon: int = 13) -> pd.DataFrame:
    frame = cache.frame
    n = len(frame)
    idx = np.arange(n, dtype=int)
    a = cache.arrays
    buffer = 0.1 * cache.pip_size
    buy_stop = pd.Series(a.ask_high).rolling(lookback, min_periods=lookback).max().to_numpy(dtype=float) + buffer
    sell_stop = pd.Series(a.bid_low).rolling(lookback, min_periods=lookback).min().to_numpy(dtype=float) - buffer
    valid = (idx >= lookback - 1) & (idx + entry_window + horizon - 1 < n)
    entry_index = np.full(n, -1, dtype=int)
    side_sign = np.zeros(n, dtype=int)
    entry_price = np.full(n, np.nan, dtype=float)
    ambiguous_trigger = np.zeros(n, dtype=bool)
    unresolved = valid.copy()
    for offset in range(1, entry_window + 1):
        current = idx + offset
        safe_current = np.clip(current, 0, max(n - 1, 0))
        long_hit = unresolved & (a.ask_high[safe_current] >= buy_stop)
        short_hit = unresolved & (a.bid_low[safe_current] <= sell_stop)
        both = long_hit & short_hit
        only_long = long_hit & ~both
        only_short = short_hit & ~both
        long_entry = np.maximum(buy_stop, a.ask_open[safe_current])
        short_entry = np.minimum(sell_stop, a.bid_open[safe_current])
        end = np.clip(current + horizon - 1, 0, max(n - 1, 0))
        long_endpoint = (a.bid_close[end] - long_entry) / cache.pip_size - cache.settings.slippage_pips_round_trip
        short_endpoint = (short_entry - a.ask_close[end]) / cache.pip_size - cache.settings.slippage_pips_round_trip
        take_long_ambiguous = both & (long_endpoint <= short_endpoint)
        take_short_ambiguous = both & ~take_long_ambiguous
        take_long = only_long | take_long_ambiguous
        take_short = only_short | take_short_ambiguous
        entry_index[take_long | take_short] = current[take_long | take_short]
        entry_price[take_long] = long_entry[take_long]
        entry_price[take_short] = short_entry[take_short]
        side_sign[take_long] = 1
        side_sign[take_short] = -1
        ambiguous_trigger[both] = True
        unresolved[take_long | take_short] = False

    triggered = side_sign != 0
    realized = np.full(n, np.nan, dtype=float)
    mfe = np.full(n, np.nan, dtype=float)
    mae = np.full(n, np.nan, dtype=float)
    exit_bars = np.full(n, np.nan, dtype=float)
    outcome = np.full(n, "no_trigger", dtype=object)
    outcome[triggered] = "timeout"
    realized[triggered] = 0.0
    mfe[triggered] = 0.0
    mae[triggered] = 0.0
    exit_bars[triggered] = float(horizon)
    active = triggered.copy()
    tp = 3.0
    sl = 5.0
    for offset in range(horizon):
        current = entry_index + offset
        safe_current = np.clip(current, 0, max(n - 1, 0))
        usable = active & (current < n)
        long = side_sign == 1
        fav = np.where(long, (a.bid_high[safe_current] - entry_price) / cache.pip_size, (entry_price - a.ask_low[safe_current]) / cache.pip_size)
        adverse = np.where(long, (entry_price - a.bid_low[safe_current]) / cache.pip_size, (a.ask_high[safe_current] - entry_price) / cache.pip_size)
        mfe[usable] = np.maximum(mfe[usable], fav[usable])
        mae[usable] = np.maximum(mae[usable], adverse[usable])
        hit_tp = usable & (fav >= tp)
        hit_sl = usable & (adverse >= sl)
        both = hit_tp & hit_sl
        take_tp = hit_tp & ~both
        take_sl = hit_sl | both
        realized[take_tp] = tp - cache.settings.slippage_pips_round_trip
        realized[take_sl] = -sl - cache.settings.slippage_pips_round_trip
        outcome[take_tp] = "tp_before_sl"
        outcome[hit_sl & ~both] = "sl_before_tp"
        outcome[both] = "ambiguous_adverse_first"
        exit_bars[take_tp | take_sl] = offset + 1
        active[take_tp | take_sl] = False
    timeout = triggered & active
    timeout_index = np.clip(entry_index + horizon - 1, 0, max(n - 1, 0))
    long = side_sign == 1
    timeout_pips = np.where(long, (a.bid_close[timeout_index] - entry_price) / cache.pip_size, (entry_price - a.ask_close[timeout_index]) / cache.pip_size) - cache.settings.slippage_pips_round_trip
    realized[timeout] = timeout_pips[timeout]

    fields = [
        "decision_time_utc", "spread_pips_used", "round_trip_cost_pips_used", "atr_15m_pips",
        "range_15m_pips", "movement_quality", "volatility_expansion_15_60", "spread_to_atr_15m",
        "session_liquidity_score", "is_london", "is_new_york", "is_london_ny_overlap",
    ]
    out = frame[[field for field in fields if field in frame.columns]].copy()
    out["pair"] = cache.pair
    meta = pair_meta(cache.pair, {"tier1_pairs": [cache.pair], "tier2_pairs": []}) if False else None
    base, quote = cache.pair.split("_", 1)
    out["base_currency"] = base
    out["quote_currency"] = quote
    out["session"] = _session_from_time(out["decision_time_utc"])
    out["triggered"] = triggered
    out["no_trigger"] = valid & ~triggered
    out["side"] = np.where(side_sign == 1, "long", np.where(side_sign == -1, "short", None))
    out["actual_pips"] = realized
    out["actual_ev"] = realized * cache.pip_value_usd_per_unit * POSITION_UNITS
    out["pip_value_usd_per_unit"] = cache.pip_value_usd_per_unit
    out["mfe_pips"] = mfe
    out["mae_pips"] = mae
    out["outcome"] = outcome
    out["entry_bars_after_decision"] = np.where(triggered, entry_index - idx, np.nan)
    out["exit_bars"] = exit_bars
    out["ambiguous_trigger"] = ambiguous_trigger
    out["false_breakout"] = triggered & (realized < 0)
    out["whipsaw"] = triggered & ((outcome == "sl_before_tp") | (outcome == "ambiguous_adverse_first") | ((mfe < tp * 0.5) & (mae >= sl)))
    out["oco_signal_score"] = _series_or_empty(out, "movement_quality") + _series_or_empty(out, "volatility_expansion_15_60") - _series_or_empty(out, "spread_to_atr_15m")
    return out


@dataclass
class OracleSurface:
    times: pd.DatetimeIndex
    values: np.ndarray
    columns: list[tuple[int, str, str]]
    top_candidates: pd.DataFrame
    top1: pd.DataFrame
    time_index: dict[pd.Timestamp, int]
    column_index: dict[tuple[int, str, str], int]


def _build_oracle_surface(final: pd.DataFrame) -> OracleSurface:
    times = pd.DatetimeIndex(pd.to_datetime(final["decision_time_utc"], utc=True).drop_duplicates().sort_values())
    pair_sides = sorted({(str(pair), str(side)) for pair, side in final[["pair", "side"]].drop_duplicates().itertuples(index=False)})
    matrices: list[np.ndarray] = []
    columns: list[tuple[int, str, str]] = []
    multi_columns = pd.MultiIndex.from_tuples(pair_sides, names=["pair", "side"])
    for horizon in HORIZONS:
        pivot = final.pivot(index="decision_time_utc", columns=["pair", "side"], values=f"actual_ev_{horizon}m")
        pivot = pivot.reindex(index=times, columns=multi_columns)
        matrices.append(pivot.to_numpy(dtype=float))
        columns.extend((int(horizon), pair, side) for pair, side in pair_sides)
    values = np.concatenate(matrices, axis=1)
    safe = np.where(np.isfinite(values), values, -np.inf)
    top_k = min(5, safe.shape[1])
    partial = np.argpartition(safe, -top_k, axis=1)[:, -top_k:]
    partial_values = np.take_along_axis(safe, partial, axis=1)
    order = np.argsort(partial_values, axis=1)[:, ::-1]
    top_indices = np.take_along_axis(partial, order, axis=1)
    top_values = np.take_along_axis(safe, top_indices, axis=1)
    records: list[dict[str, Any]] = []
    for row_index, timestamp in enumerate(times):
        for rank in range(top_k):
            col_index = int(top_indices[row_index, rank])
            horizon, pair, side = columns[col_index]
            value = float(top_values[row_index, rank])
            if not np.isfinite(value):
                continue
            records.append({
                "decision_time_utc": timestamp,
                "oracle_rank": rank + 1,
                "oracle_horizon_min": horizon,
                "pair": pair,
                "side": side,
                "actual_ev": value,
            })
    top_candidates = pd.DataFrame(records)
    top1 = top_candidates[top_candidates["oracle_rank"] == 1].copy()
    metadata_columns = [
        "decision_time_utc", "pair", "side", "session", "spread_pips_used", "spread_to_atr_15m",
        "atr_15m_pips", "movement_quality", "side_currency_strength_5m", "side_pair_residual_5m",
        "base_currency", "quote_currency", "side_return_5m_pips", "side_momentum_5m_atr",
    ]
    metadata_columns = [column for column in metadata_columns if column in final.columns]
    metadata = final[metadata_columns].drop_duplicates(["decision_time_utc", "pair", "side"])
    top1 = top1.merge(metadata, on=["decision_time_utc", "pair", "side"], how="left")
    return OracleSurface(
        times=times,
        values=values,
        columns=columns,
        top_candidates=top_candidates,
        top1=top1,
        time_index={pd.Timestamp(ts): int(i) for i, ts in enumerate(times)},
        column_index={column: int(i) for i, column in enumerate(columns)},
    )


def _oracle_rank_gap(surface: OracleSurface, selected: pd.DataFrame, horizon: int = 13) -> tuple[pd.DataFrame, dict[str, Any]]:
    if selected.empty:
        return selected.copy(), {
            "selected_rows": 0,
            "mean_missed_opportunity": None,
            "wrong_side_rate": None,
            "wrong_horizon_rate": None,
            "selected_actual_ev_percentile": None,
            "selected_actual_ev_rank": None,
        }
    out = selected.copy()
    oracle_lookup = surface.top1.set_index("decision_time_utc")
    ranks: list[float] = []
    percentiles: list[float] = []
    oracle_values: list[float] = []
    oracle_pairs: list[Any] = []
    oracle_sides: list[Any] = []
    oracle_horizons: list[Any] = []
    for row in out.itertuples(index=False):
        ts = pd.Timestamp(getattr(row, "decision_time_utc"))
        time_index = surface.time_index.get(ts)
        if time_index is None:
            ranks.append(np.nan)
            percentiles.append(np.nan)
            oracle_values.append(np.nan)
            oracle_pairs.append(None)
            oracle_sides.append(None)
            oracle_horizons.append(None)
            continue
        candidate_index = surface.column_index.get((int(horizon), str(getattr(row, "pair")), str(getattr(row, "side"))))
        selected_value = float(getattr(row, "actual_ev", np.nan))
        candidates = surface.values[time_index]
        finite = candidates[np.isfinite(candidates)]
        if candidate_index is None or not len(finite):
            ranks.append(np.nan)
            percentiles.append(np.nan)
        else:
            ranks.append(float(1 + np.sum(finite > selected_value)))
            percentiles.append(float(np.mean(finite <= selected_value)))
        if ts in oracle_lookup.index:
            oracle_row = oracle_lookup.loc[ts]
            oracle_values.append(float(oracle_row["actual_ev"]))
            oracle_pairs.append(oracle_row["pair"])
            oracle_sides.append(oracle_row["side"])
            oracle_horizons.append(int(oracle_row["oracle_horizon_min"]))
        else:
            oracle_values.append(np.nan)
            oracle_pairs.append(None)
            oracle_sides.append(None)
            oracle_horizons.append(None)
    out["oracle_actual_ev"] = oracle_values
    out["oracle_pair"] = oracle_pairs
    out["oracle_side"] = oracle_sides
    out["oracle_horizon_min"] = oracle_horizons
    out["actual_ev_rank"] = ranks
    out["actual_ev_percentile"] = percentiles
    out["missed_opportunity_ev"] = out["oracle_actual_ev"] - out["actual_ev"]
    summary = {
        "selected_rows": int(len(out)),
        "mean_missed_opportunity": _safe_mean(out["missed_opportunity_ev"]),
        "wrong_side_rate": float((out["side"] != out["oracle_side"]).mean()),
        "wrong_horizon_rate": float((int(horizon) != out["oracle_horizon_min"]).mean()),
        "selected_actual_ev_percentile": _safe_mean(out["actual_ev_percentile"]),
        "selected_actual_ev_rank": _safe_mean(out["actual_ev_rank"]),
    }
    return out, summary


def _branch_masks(df: pd.DataFrame, train: pd.DataFrame) -> dict[str, pd.Series]:
    movement_cut = float(_series_or_empty(train, "movement_quality").quantile(0.65))
    strength_cut = float(_series_or_empty(train, "side_currency_strength_5m").abs().quantile(0.70))
    residual_cut = float(_series_or_empty(train, "side_pair_residual_5m").abs().quantile(0.70))
    overextension = float(_series_or_empty(train, "side_momentum_5m_atr").abs().quantile(0.70))
    return {
        "directional_ev_surface": pd.Series(True, index=df.index),
        "movement_only": _series_or_empty(df, "movement_quality") >= movement_cut,
        "cross_sectional_momentum": _series_or_empty(df, "xs_rank_side_momentum_5m_atr") >= 0.80,
        "continuation": _series_or_empty(df, "side_return_5m_pips") > 0,
        "snapback": _series_or_empty(df, "side_momentum_5m_atr") <= -overextension,
        "currency_strength": _series_or_empty(df, "side_currency_strength_5m").abs() >= strength_cut,
        "residual": _series_or_empty(df, "side_pair_residual_5m").abs() >= residual_cut,
        "exit_alpha_quality": _series_or_empty(df, "movement_quality") >= movement_cut,
    }


def _oracle_branch_summary(final: pd.DataFrame, train: pd.DataFrame) -> dict[str, Any]:
    ev_cols = [f"actual_ev_{horizon}m" for horizon in HORIZONS]
    work = final.copy()
    work["branch_best_ev"] = work[ev_cols].max(axis=1)
    work["branch_best_horizon"] = work[ev_cols].idxmax(axis=1).str.extract(r"(\d+)").astype(float)
    report: dict[str, Any] = {}
    for branch, mask in _branch_masks(work, train).items():
        eligible = work[mask].copy()
        top = _select_top(eligible, "branch_best_ev")
        report[branch] = {
            "eligible_candidates": int(len(eligible)),
            "eligible_timestamps": int(eligible["decision_time_utc"].nunique()) if not eligible.empty else 0,
            "oracle_top1_mean_ev": _safe_mean(top["branch_best_ev"]) if not top.empty else None,
            "oracle_positive_timestamp_rate": float((top["branch_best_ev"] > 0).mean()) if not top.empty else None,
        }
    return report


def _oracle_decomposition(final: pd.DataFrame, train: pd.DataFrame, surface: OracleSurface) -> dict[str, Any]:
    top = surface.top_candidates.copy()
    top_means = {
        f"top{k}": _safe_mean(top[top["oracle_rank"] <= k].groupby("decision_time_utc")["actual_ev"].mean())
        for k in [1, 3, 5]
    }
    top1 = surface.top1.copy()
    spread_bucket = _bucket_from_train(_series_or_empty(top1, "spread_to_atr_15m"), _series_or_empty(train, "spread_to_atr_15m"), "spread")
    volatility_bucket = _bucket_from_train(_series_or_empty(top1, "atr_15m_pips"), _series_or_empty(train, "atr_15m_pips"), "volatility")
    top1["spread_bucket"] = spread_bucket
    top1["volatility_bucket"] = volatility_bucket
    top1["policy"] = "endpoint_timeout"
    exit_upper = final.copy()
    upper_cols = []
    for horizon in HORIZONS:
        column = f"mfe_pips_{horizon}m"
        if column in exit_upper.columns:
            new_col = f"mfe_upper_ev_{horizon}m"
            exit_upper[new_col] = (
                _series_or_empty(exit_upper, column)
                - _series_or_empty(exit_upper, "round_trip_cost_pips_used", 0.0)
            ) * _series_or_empty(exit_upper, "pip_value_usd_per_unit", 0.0) * POSITION_UNITS
            upper_cols.append(new_col)
    if upper_cols:
        exit_upper["oracle_exit_upper_ev"] = exit_upper[upper_cols].max(axis=1)
        exit_top = _select_top(exit_upper, "oracle_exit_upper_ev")
        exit_upper_bound = _safe_mean(exit_top["oracle_exit_upper_ev"])
    else:
        exit_upper_bound = None
    return {
        "oracle_columns_used_for_training": False,
        "oracle_columns_used_for_allocation": False,
        "candidate_surface": {
            "timestamps": int(len(surface.times)),
            "candidates_per_timestamp": int(len(surface.columns)),
            "best_actual_ev_by_timestamp_file": "oracle_top_candidates.csv",
            "top_after_costs_mean_ev": top_means,
            "positive_oracle_top1_timestamp_rate": float((top1["actual_ev"] > 0).mean()) if not top1.empty else None,
        },
        "oracle_by_strategy_branch": _oracle_branch_summary(final, train),
        "oracle_by_pair": _breakdown(top1, "pair"),
        "oracle_by_session": _breakdown(top1, "session"),
        "oracle_by_horizon": _breakdown(top1, "oracle_horizon_min"),
        "oracle_by_policy": _breakdown(top1, "policy"),
        "oracle_by_spread_bucket": _breakdown(top1, "spread_bucket"),
        "oracle_by_volatility_bucket": _breakdown(top1, "volatility_bucket"),
        "oracle_by_direction": _breakdown(top1, "side"),
        "oracle_continuation_vs_snapback": {
            "continuation": _oracle_branch_summary(final, train).get("continuation"),
            "snapback": _oracle_branch_summary(final, train).get("snapback"),
        },
        "oracle_currency_strength_expression": _oracle_branch_summary(final, train).get("currency_strength"),
        "oracle_residual_strategy": _oracle_branch_summary(final, train).get("residual"),
        "oracle_exit_timing_upper_bound_mean_ev": exit_upper_bound,
    }


def _choose_no_edge_threshold(validation_top: pd.DataFrame) -> float:
    if validation_top.empty:
        return float("-inf")
    candidates = [-np.inf] + [float(value) for value in validation_top["no_edge_probability"].quantile([0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]).dropna()]
    best_threshold = -np.inf
    best_pnl = 0.0
    for threshold in sorted(set(candidates)):
        trades = validation_top[validation_top["no_edge_probability"] <= threshold]
        pnl = _safe_sum(trades["actual_ev"])
        if pnl > best_pnl:
            best_pnl = pnl
            best_threshold = threshold
    return float(best_threshold)


def _no_edge_report(validation: pd.DataFrame, final: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    val = validation.copy()
    fin = final.copy()
    for frame in [val, fin]:
        frame["actual_ev"] = _series_or_empty(frame, "actual_ev_13m")
        frame["actual_pips"] = _series_or_empty(frame, "actual_pips_13m")
        frame["mfe_pips"] = _series_or_empty(frame, "mfe_pips_13m", np.nan)
        frame["mae_pips"] = _series_or_empty(frame, "mae_pips_13m", np.nan)
    val_top = _select_top(val, "predicted_ev")
    fin_top = _select_top(fin, "predicted_ev")
    threshold = _choose_no_edge_threshold(val_top)
    val_taken = val_top[val_top["no_edge_probability"] <= threshold].copy()
    fin_taken = fin_top[fin_top["no_edge_probability"] <= threshold].copy()
    predicted_no_edge = fin_top["no_edge_probability"] > threshold
    actual_no_edge = fin_top["actual_ev"] <= 0
    true_positive = int((predicted_no_edge & actual_no_edge).sum())
    false_positive = int((predicted_no_edge & ~actual_no_edge).sum())
    false_negative = int((~predicted_no_edge & actual_no_edge).sum())
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else None
    recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else None
    report = {
        "definition": "No-edge means the top predicted 13m market candidate realizes non-positive executable after-cost EV.",
        "threshold_selected_on_validation": threshold,
        "validation_selected_stand_down": bool(np.isneginf(threshold)),
        "validation_allocator": _metrics(val_taken),
        "final_allocator": _metrics(fin_taken),
        "final_no_edge_precision": precision,
        "final_no_edge_recall": recall,
        "losing_trades_filtered": int((predicted_no_edge & actual_no_edge).sum()),
        "winning_trades_filtered": int((predicted_no_edge & ~actual_no_edge).sum()),
        "final_top_candidate_rows": int(len(fin_top)),
        "model": "HistGradientBoostingClassifier trained only on train partition; gate threshold selected only on validation.",
    }
    return report, val_taken, fin_taken


def _baseline_score_functions() -> dict[str, Callable[[pd.DataFrame], pd.Series]]:
    return {
        "random_candidate": lambda df: pd.Series(np.random.default_rng(42).normal(size=len(df)), index=df.index),
        "low_spread_momentum": lambda df: _series_or_empty(df, "side_momentum_5m_atr") - 2.0 * _series_or_empty(df, "spread_to_atr_15m"),
        "cross_sectional_momentum": lambda df: _series_or_empty(df, "xs_rank_side_momentum_5m_atr") + _series_or_empty(df, "xs_rank_movement_quality") - _series_or_empty(df, "spread_to_atr_15m"),
        "currency_strength_continuation": lambda df: _series_or_empty(df, "side_currency_strength_5m") + 0.5 * _series_or_empty(df, "side_currency_strength_15m") - _series_or_empty(df, "spread_to_atr_15m"),
        "best_expression_selector": lambda df: (
            _series_or_empty(df, "side_currency_strength_5m") + 0.5 * _series_or_empty(df, "side_currency_strength_15m")
        ) / (_series_or_empty(df, "spread_to_atr_15m", 1.0) + 0.15) + 0.20 * _series_or_empty(df, "session_liquidity_score"),
        "residual_continuation": lambda df: _series_or_empty(df, "side_pair_residual_5m") - _series_or_empty(df, "spread_to_atr_15m"),
        "residual_mean_reversion": lambda df: -_series_or_empty(df, "side_pair_residual_5m") - _series_or_empty(df, "spread_to_atr_15m"),
    }


def _build_baseline_suite(validation: pd.DataFrame, final: pd.DataFrame) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    reports: dict[str, dict[str, Any]] = {}
    for name, scorer in _baseline_score_functions().items():
        val = validation.copy()
        fin = final.copy()
        val["baseline_score"] = scorer(val)
        fin["baseline_score"] = scorer(fin)
        reports[name] = _evaluate_scored_strategy(val, fin, "baseline_score", "actual_ev_13m", name)
    compact = {
        "no_trade": {"final_gated": _metrics(pd.DataFrame())},
        "baselines": {
            name: {
                "threshold": report["threshold"],
                "validation_raw": report["validation_raw"],
                "validation_gated": report["validation_gated"],
                "final_raw": report["final_raw"],
                "final_gated": report["final_gated"],
            }
            for name, report in reports.items()
        },
    }
    return compact, reports


def _oco_variant_reports(
    oco: pd.DataFrame,
    train: pd.DataFrame,
    validation: pd.DataFrame,
    final: pd.DataFrame,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    time_partition: dict[pd.Timestamp, str] = {}
    for name, frame in [("train", train), ("validation", validation), ("final", final)]:
        for timestamp in pd.to_datetime(frame["decision_time_utc"], utc=True).drop_duplicates():
            time_partition[pd.Timestamp(timestamp)] = name
    out = oco.copy()
    out["partition"] = pd.to_datetime(out["decision_time_utc"], utc=True).map(time_partition).fillna("excluded")
    movement_lookup = pd.concat([
        validation[validation["side"] == "long"][["decision_time_utc", "pair", "movement_probability_13m"]],
        final[final["side"] == "long"][["decision_time_utc", "pair", "movement_probability_13m"]],
    ], ignore_index=True).drop_duplicates(["decision_time_utc", "pair"])
    out = out.merge(movement_lookup, on=["decision_time_utc", "pair"], how="left")
    range_to_atr = _series_or_empty(out, "range_15m_pips") / _series_or_empty(out, "atr_15m_pips", np.nan)
    train_reference = train[train["side"] == "long"]
    compression_cut = float((_series_or_empty(train_reference, "range_15m_pips") / _series_or_empty(train_reference, "atr_15m_pips", np.nan)).quantile(0.35))
    expansion_cut = float(_series_or_empty(train_reference, "volatility_expansion_15_60").quantile(0.65))
    spread_cut = float(_series_or_empty(train_reference, "spread_to_atr_15m").quantile(0.50))
    val_triggered = out[(out["partition"] == "validation") & out["triggered"]].copy()
    movement_candidates = [float(value) for value in val_triggered["movement_probability_13m"].quantile([0.40, 0.50, 0.60, 0.70, 0.80]).dropna()]
    movement_threshold = -np.inf
    best_movement_pnl = 0.0
    for candidate in movement_candidates:
        taken = _select_top(val_triggered[val_triggered["movement_probability_13m"] >= candidate], "oco_signal_score")
        pnl = _safe_sum(taken["actual_ev"])
        if pnl > best_movement_pnl:
            best_movement_pnl = pnl
            movement_threshold = candidate
    gates: dict[str, pd.Series] = {
        "recent_range_breakout_oco": out["triggered"],
        "compression_breakout_oco": out["triggered"] & (range_to_atr <= compression_cut),
        "volatility_expansion_oco": out["triggered"] & (_series_or_empty(out, "volatility_expansion_15_60") >= expansion_cut),
        "movement_model_gated_oco": out["triggered"] & (_series_or_empty(out, "movement_probability_13m", -np.inf) >= movement_threshold),
        "session_filtered_oco": out["triggered"] & out["session"].isin(["london", "london_ny_overlap"]),
        "spread_filtered_oco": out["triggered"] & (_series_or_empty(out, "spread_to_atr_15m") <= spread_cut),
        "hybrid_oco": out["triggered"] & (_series_or_empty(out, "movement_probability_13m", -np.inf) >= movement_threshold) & out["session"].isin(["london", "london_ny_overlap"]) & (_series_or_empty(out, "spread_to_atr_15m") <= spread_cut),
    }
    reports: dict[str, Any] = {}
    selected: dict[str, pd.DataFrame] = {}
    for name, gate in gates.items():
        val_candidates = out[(out["partition"] == "validation") & gate].copy()
        final_candidates = out[(out["partition"] == "final") & gate].copy()
        val_selected = _select_top(val_candidates, "oco_signal_score")
        final_selected = _select_top(final_candidates, "oco_signal_score")
        selected[name] = final_selected
        selected[f"{name}_validation"] = val_selected
        reports[name] = {
            "validation": _metrics(val_selected),
            "final": _metrics(final_selected),
            "final_trigger_rate": float(final_candidates["triggered"].mean()) if len(final_candidates) else 0.0,
            "final_no_trigger_rate": float(final_candidates["no_trigger"].mean()) if len(final_candidates) else 0.0,
            "final_false_breakout_rate": float(final_selected["false_breakout"].mean()) if len(final_selected) else None,
            "final_whipsaw_rate": float(final_selected["whipsaw"].mean()) if len(final_selected) else None,
            "final_tp_before_sl_rate": float((final_selected["outcome"] == "tp_before_sl").mean()) if len(final_selected) else None,
        }
    raw_best = max(reports, key=lambda name: reports[name]["validation"]["pnl_account"]) if reports else None
    final_oco = out[(out["partition"] == "final") & out["triggered"]].copy()
    oracle_oco = final_oco.sort_values("actual_ev", ascending=False).groupby("decision_time_utc", as_index=False).head(1)
    best_selected = selected.get(raw_best, pd.DataFrame()) if raw_best else pd.DataFrame()
    if not best_selected.empty:
        gap = best_selected.merge(
            oracle_oco[["decision_time_utc", "actual_ev"]].rename(columns={"actual_ev": "oracle_oco_actual_ev"}),
            on="decision_time_utc",
            how="left",
        )
        selected_oracle_gap = _safe_mean(gap["oracle_oco_actual_ev"] - gap["actual_ev"])
    else:
        selected_oracle_gap = None
    report = {
        "simulation_contract": {
            "entry": "Buy stop above a 15m executable ask range or sell stop below a 15m executable bid range; order becomes active next bar and expires after five bars.",
            "entry_resolution": "When both stops trigger in one bar, the lower realized executable outcome is selected as an adverse-first ambiguity resolution.",
            "exit": "Fixed 3 pip TP / 5 pip SL, 13m maximum hold, executable bid/ask and round-trip slippage.",
        },
        "thresholds_selected_without_final": {
            "compression_range_to_atr": compression_cut,
            "volatility_expansion": expansion_cut,
            "spread_to_atr": spread_cut,
            "movement_probability": movement_threshold,
        },
        "variants": reports,
        "best_oco_variant_selected_on_validation": raw_best,
        "best_oco_final": reports.get(raw_best, {}).get("final"),
        "oco_oracle_upper_bound": _metrics(oracle_oco),
        "oco_selected_vs_oracle_mean_gap": selected_oracle_gap,
        "all_final_trigger_rate": float(out[out["partition"] == "final"]["triggered"].mean()) if len(out[out["partition"] == "final"]) else 0.0,
        "all_final_no_trigger_rate": float((out[out["partition"] == "final"]["no_trigger"]).mean()) if len(out[out["partition"] == "final"]) else 0.0,
    }
    return report, selected


def _continuation_snapback_report(validation: pd.DataFrame, final: pd.DataFrame, train: pd.DataFrame) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    reports: dict[str, dict[str, Any]] = {}
    for horizon in [1, 3, 5, 8]:
        source = f"side_return_{horizon}m_pips"
        actual = f"actual_ev_{horizon}m"
        val = validation.copy()
        fin = final.copy()
        val["continuation_score"] = _series_or_empty(val, source) - _series_or_empty(val, "spread_to_atr_15m")
        fin["continuation_score"] = _series_or_empty(fin, source) - _series_or_empty(fin, "spread_to_atr_15m")
        reports[f"continuation_{horizon}m_simple"] = _evaluate_scored_strategy(val, fin, "continuation_score", actual, f"continuation_{horizon}m_simple")
        val["ml_continuation_score"] = val["continuation_score"] + 2.0 * (_series_or_empty(val, f"direction_probability_{horizon}m") - 0.5)
        fin["ml_continuation_score"] = fin["continuation_score"] + 2.0 * (_series_or_empty(fin, f"direction_probability_{horizon}m") - 0.5)
        reports[f"continuation_{horizon}m_ml_gated"] = _evaluate_scored_strategy(val, fin, "ml_continuation_score", actual, f"continuation_{horizon}m_ml_gated")
        val["snapback_score"] = -_series_or_empty(val, source) - _series_or_empty(val, "spread_to_atr_15m")
        fin["snapback_score"] = -_series_or_empty(fin, source) - _series_or_empty(fin, "spread_to_atr_15m")
        reports[f"snapback_{horizon}m_simple"] = _evaluate_scored_strategy(val, fin, "snapback_score", actual, f"snapback_{horizon}m_simple")
        val["ml_snapback_score"] = val["snapback_score"] + 2.0 * (_series_or_empty(val, f"direction_probability_{horizon}m") - 0.5)
        fin["ml_snapback_score"] = fin["snapback_score"] + 2.0 * (_series_or_empty(fin, f"direction_probability_{horizon}m") - 0.5)
        reports[f"snapback_{horizon}m_ml_gated"] = _evaluate_scored_strategy(val, fin, "ml_snapback_score", actual, f"snapback_{horizon}m_ml_gated")
    val = validation.copy()
    fin = final.copy()
    val["overextension_deceleration"] = -_series_or_empty(val, "side_momentum_5m_atr") - _series_or_empty(val, "side_acceleration") + _series_or_empty(val, "choppiness_15m")
    fin["overextension_deceleration"] = -_series_or_empty(fin, "side_momentum_5m_atr") - _series_or_empty(fin, "side_acceleration") + _series_or_empty(fin, "choppiness_15m")
    reports["overextension_deceleration_snapback"] = _evaluate_scored_strategy(val, fin, "overextension_deceleration", "actual_ev_13m", "overextension_deceleration_snapback")
    val["wick_giveback"] = _series_or_empty(val, "wick_body_ratio") - _series_or_empty(val, "side_momentum_5m_atr")
    fin["wick_giveback"] = _series_or_empty(fin, "wick_body_ratio") - _series_or_empty(fin, "side_momentum_5m_atr")
    reports["wick_giveback_snapback"] = _evaluate_scored_strategy(val, fin, "wick_giveback", "actual_ev_13m", "wick_giveback_snapback")
    val["range_position_snapback"] = (0.5 - _series_or_empty(val, "side_range_position")).abs() + -_series_or_empty(val, "side_momentum_5m_atr")
    fin["range_position_snapback"] = (0.5 - _series_or_empty(fin, "side_range_position")).abs() + -_series_or_empty(fin, "side_momentum_5m_atr")
    reports["range_position_snapback"] = _evaluate_scored_strategy(val, fin, "range_position_snapback", "actual_ev_13m", "range_position_snapback")

    continuation_names = [name for name in reports if name.startswith("continuation")]
    snapback_names = [name for name in reports if "snapback" in name]
    best_cont = max(continuation_names, key=lambda name: reports[name]["validation_gated"]["pnl_account"])
    best_snap = max(snapback_names, key=lambda name: reports[name]["validation_gated"]["pnl_account"])
    cont_trades = reports[best_cont]["final_raw_trades"].copy()
    snap_trades = reports[best_snap]["final_raw_trades"].copy()
    for frame in [cont_trades, snap_trades]:
        frame["spread_bucket"] = _bucket_from_train(_series_or_empty(frame, "spread_to_atr_15m"), _series_or_empty(train, "spread_to_atr_15m"), "spread")
        frame["volatility_bucket"] = _bucket_from_train(_series_or_empty(frame, "atr_15m_pips"), _series_or_empty(train, "atr_15m_pips"), "volatility")
    compact = {
        "best_continuation_selected_on_validation": best_cont,
        "best_snapback_selected_on_validation": best_snap,
        "best_continuation_final": reports[best_cont]["final_gated"],
        "best_snapback_final": reports[best_snap]["final_gated"],
        "continuation_by_session": _breakdown(cont_trades, "session"),
        "continuation_by_pair": _breakdown(cont_trades, "pair"),
        "continuation_by_volatility": _breakdown(cont_trades, "volatility_bucket"),
        "continuation_by_spread": _breakdown(cont_trades, "spread_bucket"),
        "snapback_by_session": _breakdown(snap_trades, "session"),
        "snapback_by_pair": _breakdown(snap_trades, "pair"),
        "snapback_by_volatility": _breakdown(snap_trades, "volatility_bucket"),
        "snapback_by_spread": _breakdown(snap_trades, "spread_bucket"),
        "inversion_signal": {
            "snapback_raw_pnl_minus_continuation_raw_pnl": snap_trades["actual_ev"].sum() - cont_trades["actual_ev"].sum(),
            "suggests_inversion": bool(snap_trades["actual_ev"].sum() > cont_trades["actual_ev"].sum() and snap_trades["actual_ev"].sum() > 0),
        },
        "strategies": {
            name: {
                "threshold": result["threshold"],
                "validation_gated": result["validation_gated"],
                "final_raw": result["final_raw"],
                "final_gated": result["final_gated"],
            }
            for name, result in reports.items()
        },
    }
    return compact, reports


def _strength_rank_decay_exit(
    trades: pd.DataFrame,
    all_rows: pd.DataFrame,
    caches: dict[str, MarketCache],
    horizon: int = 13,
) -> pd.DataFrame:
    if trades.empty:
        return trades.copy()
    columns = ["pair", "side", "decision_time_utc", "side_currency_strength_5m", "xs_rank_side_currency_strength_5m"]
    columns = [column for column in columns if column in all_rows.columns]
    lookup: dict[tuple[str, str, pd.Timestamp], tuple[float, float]] = {}
    for row in all_rows[columns].drop_duplicates(["pair", "side", "decision_time_utc"]).itertuples(index=False):
        values = row._asdict()
        lookup[(str(values["pair"]), str(values["side"]), pd.Timestamp(values["decision_time_utc"]))] = (
            float(values.get("side_currency_strength_5m", np.nan)),
            float(values.get("xs_rank_side_currency_strength_5m", np.nan)),
        )
    out = trades.copy()
    pips: list[float] = []
    exits: list[int] = []
    for row in out.itertuples(index=False):
        pair = str(getattr(row, "pair"))
        side = str(getattr(row, "side"))
        cache = caches.get(pair)
        timestamp = pd.Timestamp(getattr(row, "decision_time_utc"))
        start_index = cache.time_to_index.get(timestamp) if cache else None
        exit_horizon = horizon
        if cache is not None and start_index is not None:
            for step in range(1, horizon + 1):
                future_index = start_index + step
                if future_index >= len(cache.frame):
                    break
                future_time = pd.Timestamp(cache.frame["decision_time_utc"].iloc[future_index])
                strength, rank = lookup.get((pair, side, future_time), (np.nan, np.nan))
                if (np.isfinite(strength) and strength <= 0.0) or (np.isfinite(rank) and rank < 0.50):
                    exit_horizon = step
                    break
        pips.append(_endpoint_pips(cache, start_index, side, exit_horizon) if cache is not None and start_index is not None else np.nan)
        exits.append(exit_horizon)
    out["actual_pips"] = pips
    out["actual_ev"] = out["actual_pips"].astype(float) * out["pip_value_usd_per_unit"].astype(float) * POSITION_UNITS
    out["rank_decay_exit_horizon"] = exits
    return out


def _currency_residual_report(
    validation: pd.DataFrame,
    final: pd.DataFrame,
    all_rows: pd.DataFrame,
    caches: dict[str, MarketCache],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    strategies: dict[str, dict[str, Any]] = {}
    score_defs = {
        "strongest_vs_weakest_market_continuation": lambda df: _series_or_empty(df, "side_currency_strength_5m") + 0.5 * _series_or_empty(df, "side_currency_strength_15m") - _series_or_empty(df, "spread_to_atr_15m"),
        "best_expression_selector": lambda df: (
            _series_or_empty(df, "side_currency_strength_5m") + 0.5 * _series_or_empty(df, "side_currency_strength_15m")
        ) / (_series_or_empty(df, "spread_to_atr_15m", 1.0) + 0.15) + 0.2 * _series_or_empty(df, "session_liquidity_score"),
        "residual_continuation": lambda df: _series_or_empty(df, "side_pair_residual_5m") - _series_or_empty(df, "spread_to_atr_15m"),
        "residual_mean_reversion": lambda df: -_series_or_empty(df, "side_pair_residual_5m") - _series_or_empty(df, "spread_to_atr_15m"),
        "laggard_vs_leader": lambda df: -_series_or_empty(df, "side_pair_residual_5m") + _series_or_empty(df, "side_currency_strength_5m") - _series_or_empty(df, "spread_to_atr_15m"),
    }
    for name, scorer in score_defs.items():
        val = validation.copy()
        fin = final.copy()
        val["branch_score"] = scorer(val)
        fin["branch_score"] = scorer(fin)
        strategies[name] = _evaluate_scored_strategy(val, fin, "branch_score", "actual_ev_13m", name)
    strength = strategies["strongest_vs_weakest_market_continuation"]
    expression = strategies["best_expression_selector"]
    delayed_val = _apply_endpoint_variant(expression["validation_raw_trades"], caches, horizon=13, delayed=True)
    delayed_final = _apply_endpoint_variant(expression["final_raw_trades"], caches, horizon=13, delayed=True)
    rank_decay_val = _strength_rank_decay_exit(expression["validation_raw_trades"], all_rows, caches)
    rank_decay_final = _strength_rank_decay_exit(expression["final_raw_trades"], all_rows, caches)
    impulse = strength["final_raw_trades"]
    residual_component = _series_or_empty(final, "side_pair_residual_5m")
    factor_component = _series_or_empty(final, "base_minus_quote_strength_5m")
    report = {
        "strength_construction": "At each minute all Tier 1 base/quote currency returns are averaged into 5m/15m currency strength. Pair-side strength is base minus quote, adjusted for trade side.",
        "strategies": {
            name: {
                "threshold": result["threshold"],
                "validation_gated": result["validation_gated"],
                "final_raw": result["final_raw"],
                "final_gated": result["final_gated"],
            }
            for name, result in strategies.items()
        },
        "strongest_vs_weakest_final": strength["final_gated"],
        "best_expression_selector_final": expression["final_gated"],
        "delayed_entry_best_expression": {
            "validation": _metrics(delayed_val),
            "final": _metrics(delayed_final),
            "entry_assumption": "one extra executable bar beyond Phase 1 next-bar entry",
        },
        "exit_on_rank_decay": {
            "validation": _metrics(rank_decay_val),
            "final": _metrics(rank_decay_final),
            "rule": "exit at the first subsequent minute where side strength is non-positive or cross-sectional side-strength rank falls below 0.50; otherwise 13m time exit",
            "mean_exit_horizon_final": _safe_mean(rank_decay_final.get("rank_decay_exit_horizon", pd.Series(dtype=float))),
        },
        "currency_impulse_hit_rate": float((impulse["actual_ev"] > 0).mean()) if not impulse.empty else None,
        "factor_vs_residual": {
            "factor_residual_correlation": float(factor_component.corr(residual_component)) if len(final) else None,
            "factor_std": _safe_mean(abs(factor_component)),
            "residual_std": _safe_mean(abs(residual_component)),
        },
        "exposure_concentration": _portfolio_exposure(expression["final_raw_trades"]),
    }
    return report, strategies


def _direction_movement_report(
    final: pd.DataFrame,
    validation: pd.DataFrame,
    model_metadata: dict[str, Any],
    policy_table: pd.DataFrame,
    oco_selected: dict[str, pd.DataFrame],
) -> tuple[dict[str, Any], pd.DataFrame]:
    val = validation.copy()
    fin = final.copy()
    for frame in [val, fin]:
        frame["actual_ev"] = _series_or_empty(frame, "actual_ev_13m")
        frame["actual_pips"] = _series_or_empty(frame, "actual_pips_13m")
        frame["mfe_pips"] = _series_or_empty(frame, "mfe_pips_13m", np.nan)
        frame["mae_pips"] = _series_or_empty(frame, "mae_pips_13m", np.nan)
    selected = _select_top(fin, "predicted_ev")
    reverse = fin[["decision_time_utc", "pair", "side", "actual_ev_13m", "mfe_pips_13m"]].copy()
    reverse["side"] = np.where(reverse["side"] == "long", "short", "long")
    reverse = reverse.rename(columns={"actual_ev_13m": "opposite_side_actual_ev", "mfe_pips_13m": "opposite_side_mfe"})
    selected = selected.merge(reverse, on=["decision_time_utc", "pair", "side"], how="left")
    selected["mid_direction_correct"] = _series_or_empty(selected, "mid_direction_pips_13m") > 0
    selected["move_too_small"] = _series_or_empty(selected, "mid_direction_pips_13m").abs() < _series_or_empty(selected, "round_trip_cost_pips_used")
    selected["movement_existed_opposite_won"] = (selected["opposite_side_actual_ev"] > 0) & (selected["actual_ev"] <= 0)
    selected["movement_existed_both_ways_exit_failed"] = (_series_or_empty(selected, "mfe_pips") >= _series_or_empty(selected, "round_trip_cost_pips_used")) & (_series_or_empty(selected, "opposite_side_mfe") >= _series_or_empty(selected, "round_trip_cost_pips_used")) & (selected["actual_ev"] <= 0)
    selected["spread_slippage_erased"] = ((_series_or_empty(selected, "actual_pips") + _series_or_empty(selected, "round_trip_cost_pips_used")) > 0) & (selected["actual_pips"] <= 0)
    selected["no_meaningful_movement"] = _series_or_empty(selected, "future_range_pips_13m") <= _series_or_empty(selected, "round_trip_cost_pips_used")
    selected["too_adverse_before_favorable"] = _series_or_empty(selected, "mae_pips") > _series_or_empty(selected, "mfe_pips")
    direction_only = _select_top(fin, "direction_probability_13m")
    combined = _select_top(fin, "predicted_ev")
    movement_oco = oco_selected.get("movement_model_gated_oco", pd.DataFrame())
    tp_rates = []
    if not policy_table.empty:
        subset = policy_table[policy_table["partition"] == "final"]
        tp_rates = (
            subset.groupby(["policy", "horizon_min"], as_index=False)
            .agg({"tp_before_sl_rate": "mean", "sl_before_tp_rate": "mean", "pnl_account": "sum", "rows": "sum"})
            .to_dict("records")
        )
    failure_keys = [
        "mid_direction_correct", "move_too_small", "movement_existed_opposite_won",
        "movement_existed_both_ways_exit_failed", "spread_slippage_erased", "no_meaningful_movement",
        "too_adverse_before_favorable",
    ]
    failures = {key: int(selected[key].sum()) for key in failure_keys}
    report = {
        "horizon_predictability": model_metadata.get("horizon_models", []),
        "selected_trade_decomposition": {
            "selected_model_top1": _metrics(selected),
            "predicted_direction_correct_but_move_too_small": int((selected["mid_direction_correct"] & selected["move_too_small"]).sum()),
            "predicted_direction_wrong": int((~selected["mid_direction_correct"]).sum()),
            "movement_existed_but_opposite_side_won": int(selected["movement_existed_opposite_won"].sum()),
            "movement_existed_both_ways_but_exit_failed": int(selected["movement_existed_both_ways_exit_failed"].sum()),
            "movement_existed_but_spread_slippage_erased": int(selected["spread_slippage_erased"].sum()),
            "no_meaningful_movement": int(selected["no_meaningful_movement"].sum()),
            "too_much_adverse_path_before_favorable_move": int(selected["too_adverse_before_favorable"].sum()),
            "failure_counts": failures,
        },
        "top_k_actual_ev": {
            "direction_only": _metrics(direction_only),
            "movement_only_via_oco": _metrics(movement_oco),
            "direction_plus_movement": _metrics(combined),
        },
        "tp_before_sl_predictability_by_policy_horizon": tp_rates,
    }
    return report, selected


def _path_stats(cache: MarketCache, decision_index: int, side: str, horizon: int = 13) -> dict[str, Any]:
    delay = int(cache.settings.entry_delay_bars)
    entry_index = decision_index + delay
    end_index = entry_index + horizon - 1
    if entry_index < 0 or end_index >= len(cache.frame):
        return {"valid": False}
    if side == "long":
        entry = float(cache.arrays.ask_open[entry_index])
        fav = (cache.arrays.bid_high[entry_index:end_index + 1] - entry) / cache.pip_size
        adverse = (entry - cache.arrays.bid_low[entry_index:end_index + 1]) / cache.pip_size
    else:
        entry = float(cache.arrays.bid_open[entry_index])
        fav = (entry - cache.arrays.ask_low[entry_index:end_index + 1]) / cache.pip_size
        adverse = (cache.arrays.ask_high[entry_index:end_index + 1] - entry) / cache.pip_size
    endpoint = _endpoint_pips(cache, decision_index, side, horizon)
    return {
        "valid": True,
        "entry": entry,
        "favorable": np.asarray(fav, dtype=float),
        "adverse": np.asarray(adverse, dtype=float),
        "endpoint_pips": endpoint,
        "mfe_pips": float(np.nanmax(fav)) if len(fav) else np.nan,
        "mae_pips": float(np.nanmax(adverse)) if len(adverse) else np.nan,
        "time_to_mfe": int(np.nanargmax(fav) + 1) if len(fav) else None,
        "time_to_mae": int(np.nanargmax(adverse) + 1) if len(adverse) else None,
    }


def _fast_partial_pips(cache: MarketCache, decision_index: int, side: str, horizon: int = 13, tp: float = 1.5, sl: float = 5.0) -> float:
    path = _path_stats(cache, decision_index, side, horizon)
    if not path.get("valid"):
        return float("nan")
    for fav, adverse in zip(path["favorable"], path["adverse"]):
        hit_tp = fav >= tp
        hit_sl = adverse >= sl
        if hit_sl:
            return float(-sl - cache.settings.slippage_pips_round_trip)
        if hit_tp:
            endpoint_before_slip = float(path["endpoint_pips"]) + cache.settings.slippage_pips_round_trip
            return float(0.5 * tp + 0.5 * endpoint_before_slip - cache.settings.slippage_pips_round_trip)
    return float(path["endpoint_pips"])


def _trailing_pips(cache: MarketCache, decision_index: int, side: str, horizon: int = 13, activation: float = 2.0, trail: float = 1.0) -> float:
    path = _path_stats(cache, decision_index, side, horizon)
    if not path.get("valid"):
        return float("nan")
    high_water = 0.0
    for fav, adverse in zip(path["favorable"], path["adverse"]):
        high_water = max(high_water, float(fav))
        if high_water >= activation and adverse >= max(high_water - trail, 0.0):
            return float(high_water - trail - cache.settings.slippage_pips_round_trip)
    return float(path["endpoint_pips"])


def _score_maps(scored: pd.DataFrame) -> tuple[dict[tuple[str, str, pd.Timestamp], float], dict[pd.Timestamp, float]]:
    side_scores = {
        (str(pair), str(side), pd.Timestamp(ts)): float(score)
        for ts, pair, side, score in scored[["decision_time_utc", "pair", "side", "predicted_ev"]].itertuples(index=False)
        if np.isfinite(score)
    }
    max_scores = {
        pd.Timestamp(ts): float(score)
        for ts, score in scored.groupby("decision_time_utc")["predicted_ev"].max().items()
        if np.isfinite(score)
    }
    return side_scores, max_scores


def _signal_exit_horizon(
    row: Any,
    caches: dict[str, MarketCache],
    side_scores: dict[tuple[str, str, pd.Timestamp], float],
    max_scores: dict[pd.Timestamp, float],
    mode: str,
    horizon: int = 13,
    buffer_usd: float = 0.03,
) -> int:
    pair = str(getattr(row, "pair"))
    side = str(getattr(row, "side"))
    cache = caches.get(pair)
    timestamp = pd.Timestamp(getattr(row, "decision_time_utc"))
    start_index = cache.time_to_index.get(timestamp) if cache else None
    entry_score = float(getattr(row, "predicted_ev", 0.0))
    if cache is None or start_index is None:
        return horizon
    opposite = "short" if side == "long" else "long"
    for step in range(1, horizon + 1):
        future_index = start_index + step
        if future_index >= len(cache.frame):
            break
        future_time = pd.Timestamp(cache.frame["decision_time_utc"].iloc[future_index])
        same_score = side_scores.get((pair, side, future_time), -np.inf)
        opposite_score = side_scores.get((pair, opposite, future_time), -np.inf)
        max_score = max_scores.get(future_time, -np.inf)
        if mode == "signal_decay" and same_score <= max(entry_score * 0.25, 0.0):
            return step
        if mode == "opposite_side_ev" and opposite_score > same_score:
            return step
        if mode == "better_replacement" and max_score > same_score + buffer_usd:
            return step
    return horizon


def _exit_variant_frame(
    trades: pd.DataFrame,
    scored: pd.DataFrame,
    caches: dict[str, MarketCache],
    variant: str,
    horizon: int = 13,
) -> pd.DataFrame:
    out = trades.copy()
    if out.empty:
        return out
    side_scores, max_scores = _score_maps(scored)
    pips: list[float] = []
    exit_horizons: list[int] = []
    mfe: list[float] = []
    mae: list[float] = []
    time_mfe: list[Any] = []
    time_mae: list[Any] = []
    for row in out.itertuples(index=False):
        cache = caches.get(str(getattr(row, "pair")))
        timestamp = pd.Timestamp(getattr(row, "decision_time_utc"))
        index = cache.time_to_index.get(timestamp) if cache else None
        side = str(getattr(row, "side"))
        path = _path_stats(cache, index, side, horizon) if cache is not None and index is not None else {"valid": False}
        if not path.get("valid"):
            pips.append(np.nan)
            exit_horizons.append(horizon)
            mfe.append(np.nan)
            mae.append(np.nan)
            time_mfe.append(None)
            time_mae.append(None)
            continue
        if variant == "fixed_tp_sl":
            result = simulate_tp_sl_contract_arrays(cache.arrays, index, side, 3.0, 5.0, horizon, cache.pip_size, cache.settings)
            value = float(result.realized_pips)
            exit_horizon = int(result.time_to_exit_min or horizon)
        elif variant == "fast_partial_early_tp":
            value = _fast_partial_pips(cache, index, side, horizon)
            exit_horizon = horizon
        elif variant == "trailing_after_mfe":
            value = _trailing_pips(cache, index, side, horizon)
            exit_horizon = horizon
        elif variant == "time_stop_5m":
            value = _endpoint_pips(cache, index, side, 5)
            exit_horizon = 5
        elif variant in {"signal_decay", "opposite_side_ev", "better_replacement"}:
            exit_horizon = _signal_exit_horizon(row, caches, side_scores, max_scores, variant, horizon)
            value = _endpoint_pips(cache, index, side, exit_horizon)
        else:
            value = float(path["endpoint_pips"])
            exit_horizon = horizon
        pips.append(value)
        exit_horizons.append(exit_horizon)
        mfe.append(path["mfe_pips"])
        mae.append(path["mae_pips"])
        time_mfe.append(path["time_to_mfe"])
        time_mae.append(path["time_to_mae"])
    out["actual_pips"] = pips
    out["actual_ev"] = out["actual_pips"].astype(float) * out["pip_value_usd_per_unit"].astype(float) * POSITION_UNITS
    out["mfe_pips"] = mfe
    out["mae_pips"] = mae
    out["time_to_mfe"] = time_mfe
    out["time_to_mae"] = time_mae
    out["exit_horizon"] = exit_horizons
    return out


def _exit_alpha_report(
    validation: pd.DataFrame,
    final: pd.DataFrame,
    baseline_final: pd.DataFrame,
    caches: dict[str, MarketCache],
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    val_entries = _select_top(validation.assign(actual_ev=_series_or_empty(validation, "actual_ev_13m"), actual_pips=_series_or_empty(validation, "actual_pips_13m"), mfe_pips=_series_or_empty(validation, "mfe_pips_13m", np.nan), mae_pips=_series_or_empty(validation, "mae_pips_13m", np.nan)), "predicted_ev")
    final_entries = _select_top(final.assign(actual_ev=_series_or_empty(final, "actual_ev_13m"), actual_pips=_series_or_empty(final, "actual_pips_13m"), mfe_pips=_series_or_empty(final, "mfe_pips_13m", np.nan), mae_pips=_series_or_empty(final, "mae_pips_13m", np.nan)), "predicted_ev")
    variants = [
        "endpoint_timeout",
        "fixed_tp_sl",
        "fast_partial_early_tp",
        "trailing_after_mfe",
        "signal_decay",
        "opposite_side_ev",
        "better_replacement",
        "time_stop_5m",
    ]
    val_results: dict[str, pd.DataFrame] = {}
    final_results: dict[str, pd.DataFrame] = {}
    summary: dict[str, Any] = {}
    for variant in variants:
        if variant == "endpoint_timeout":
            val_variant = val_entries.copy()
            final_variant = final_entries.copy()
            val_variant["exit_horizon"] = 13
            final_variant["exit_horizon"] = 13
        else:
            val_variant = _exit_variant_frame(val_entries, validation, caches, variant)
            final_variant = _exit_variant_frame(final_entries, final, caches, variant)
        val_results[variant] = val_variant
        final_results[variant] = final_variant
        summary[variant] = {
            "validation": _metrics(val_variant),
            "final": _metrics(final_variant),
            "mean_time_to_mfe_final": _safe_mean(final_variant.get("time_to_mfe", pd.Series(dtype=float))),
            "mean_time_to_mae_final": _safe_mean(final_variant.get("time_to_mae", pd.Series(dtype=float))),
            "mean_exit_horizon_final": _safe_mean(final_variant.get("exit_horizon", pd.Series(dtype=float))),
        }
    best_variant = max(variants, key=lambda name: summary[name]["validation"]["pnl_account"])
    baseline_metrics = _metrics(baseline_final)
    report = {
        "entry_set": "Top one 13m calibrated-EV market candidate per timestamp; variant selection uses validation only.",
        "variants": summary,
        "best_exit_variant_selected_on_validation": best_variant,
        "best_exit_variant_final": summary[best_variant]["final"],
        "baseline_entry_set_final": baseline_metrics,
        "profit_capture_diagnosis": {
            "endpoint_capture_ratio": summary["endpoint_timeout"]["final"]["mfe_capture_ratio"],
            "endpoint_giveback_pips": summary["endpoint_timeout"]["final"]["giveback_pips"],
            "exit_improvement_over_timeout": summary[best_variant]["final"]["pnl_account"] - summary["endpoint_timeout"]["final"]["pnl_account"],
        },
        "oracle_exit_note": "The oracle MFE upper bound is reported separately in ORACLE_DECOMPOSITION and is never used to choose an entry or exit variant.",
    }
    return report, final_results


def _active_exposure(active: list[dict[str, Any]]) -> dict[str, int]:
    exposure: dict[str, int] = {}
    for position in active:
        sign = 1 if position["side"] == "long" else -1
        exposure[position.get("base_currency", "")] = exposure.get(position.get("base_currency", ""), 0) + sign
        exposure[position.get("quote_currency", "")] = exposure.get(position.get("quote_currency", ""), 0) - sign
    return exposure


def _can_add_currency(active: list[dict[str, Any]], row: dict[str, Any], cap: int) -> bool:
    exposure = _active_exposure(active)
    sign = 1 if row["side"] == "long" else -1
    base = row.get("base_currency", "")
    quote = row.get("quote_currency", "")
    return abs(exposure.get(base, 0) + sign) <= cap and abs(exposure.get(quote, 0) - sign) <= cap


def _run_allocator(
    candidates: pd.DataFrame,
    caches: dict[str, MarketCache],
    max_positions: int = 1,
    currency_cap: int = 1,
    anti_churn_minutes: int = 3,
    minimum_hold_minutes: int = 1,
    replacement: bool = False,
    replacement_buffer_usd: float = 0.03,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if candidates.empty:
        return candidates.copy(), {"replacement_count": 0, "skipped_currency_cap": 0, "skipped_churn": 0}
    required = ["decision_time_utc", "pair", "side", "allocator_score", "actual_ev", "actual_pips"]
    work = candidates.dropna(subset=[column for column in required if column in candidates.columns]).copy()
    work = work.sort_values(["decision_time_utc", "allocator_score"], ascending=[True, False])
    active: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []
    last_entry_global: pd.Timestamp | None = None
    skipped_currency_cap = 0
    skipped_churn = 0
    replacement_count = 0
    for timestamp, group in work.groupby("decision_time_utc", sort=True):
        timestamp = pd.Timestamp(timestamp)
        remaining: list[dict[str, Any]] = []
        for position in active:
            if position["planned_exit"] <= timestamp:
                closed.append(position)
            else:
                remaining.append(position)
        active = remaining
        for candidate in group.to_dict("records"):
            required_gap = max(int(anti_churn_minutes), int(minimum_hold_minutes))
            if last_entry_global is not None and (timestamp - last_entry_global).total_seconds() < required_gap * 60:
                skipped_churn += 1
                continue
            if not _can_add_currency(active, candidate, currency_cap):
                skipped_currency_cap += 1
                continue
            if len(active) >= max_positions:
                if not replacement:
                    continue
                incumbent = min(active, key=lambda row: float(row.get("allocator_score", -np.inf)))
                elapsed = (timestamp - incumbent["decision_time_utc"]).total_seconds() / 60.0
                if elapsed < minimum_hold_minutes or float(candidate["allocator_score"]) <= float(incumbent.get("allocator_score", -np.inf)) + replacement_buffer_usd:
                    continue
                if not incumbent.get("is_oco", False):
                    cache = caches.get(str(incumbent["pair"]))
                    index = cache.time_to_index.get(pd.Timestamp(incumbent["decision_time_utc"])) if cache else None
                    exit_index = cache.time_to_index.get(timestamp) if cache else None
                    if cache is not None and index is not None and exit_index is not None:
                        early_pips = _endpoint_at_exit_index(cache, index, str(incumbent["side"]), exit_index)
                        incumbent["actual_pips"] = early_pips
                        incumbent["actual_ev"] = early_pips * cache.pip_value_usd_per_unit * POSITION_UNITS * float(incumbent.get("allocation_weight", 1.0))
                        incumbent["early_replacement_exit"] = True
                active.remove(incumbent)
                closed.append(incumbent)
                replacement_count += 1
            horizon = int(candidate.get("allocation_horizon", 13))
            candidate["planned_exit"] = timestamp + pd.Timedelta(minutes=horizon)
            candidate["allocation_weight"] = 1.0 / max_positions
            candidate["actual_ev"] = float(candidate["actual_ev"]) * candidate["allocation_weight"]
            candidate["actual_pips"] = float(candidate["actual_pips"])
            candidate["early_replacement_exit"] = False
            active.append(candidate)
            last_entry_global = timestamp
            if len(active) >= max_positions:
                break
    closed.extend(active)
    trades = pd.DataFrame(closed)
    diagnostics = {
        "replacement_count": int(replacement_count),
        "skipped_currency_cap": int(skipped_currency_cap),
        "skipped_churn": int(skipped_churn),
        "max_positions": int(max_positions),
        "currency_cap": int(currency_cap),
        "anti_churn_minutes": int(anti_churn_minutes),
        "minimum_hold_minutes": int(minimum_hold_minutes),
        "replacement_buffer_usd": float(replacement_buffer_usd),
    }
    return trades, diagnostics


def _prepare_allocator_candidates(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["actual_ev"] = _series_or_empty(out, "actual_ev_13m")
    out["actual_pips"] = _series_or_empty(out, "actual_pips_13m")
    out["mfe_pips"] = _series_or_empty(out, "mfe_pips_13m", np.nan)
    out["mae_pips"] = _series_or_empty(out, "mae_pips_13m", np.nan)
    out["allocation_horizon"] = 13
    out["is_oco"] = False
    return out


def _allocator_experiments(
    validation: pd.DataFrame,
    final: pd.DataFrame,
    no_edge_threshold: float,
    oco_selected: dict[str, pd.DataFrame],
    caches: dict[str, MarketCache],
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    val = _prepare_allocator_candidates(validation)
    fin = _prepare_allocator_candidates(final)
    val["allocator_score"] = _series_or_empty(val, "predicted_ev")
    fin["allocator_score"] = _series_or_empty(fin, "predicted_ev")
    calibration = _calibration(validation, "predicted_ev", "actual_ev_13m")
    lower_bound = float(calibration.get("rmse") or 0.0)
    overextension = float(_series_or_empty(validation, "side_momentum_5m_atr").abs().quantile(0.70))
    candidate_sets: dict[str, tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]] = {
        "top1_only": (val, fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 0, "minimum_hold_minutes": 1}),
        "top2_currency_cap": (val, fin, {"max_positions": 2, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1}),
        "top3_currency_cap": (val, fin, {"max_positions": 3, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1}),
        "no_edge_gated": (val[val["no_edge_probability"] <= no_edge_threshold], fin[fin["no_edge_probability"] <= no_edge_threshold], {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1}),
        "cost_lower_bound_gated": (val[val["predicted_ev"] > 0], fin[fin["predicted_ev"] > 0], {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1}),
        "calibrated_ev_lower_bound_gated": (val[val["predicted_ev"] - lower_bound > 0], fin[fin["predicted_ev"] - lower_bound > 0], {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1}),
        "market_direction_movement_agree": (
            val[(val["direction_probability_13m"] >= 0.50) & (val["movement_probability_13m"] >= 0.50)],
            fin[(fin["direction_probability_13m"] >= 0.50) & (fin["movement_probability_13m"] >= 0.50)],
            {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1},
        ),
        "snapback_overextension": (
            val[val["side_momentum_5m_atr"] <= -overextension].assign(allocator_score=-val.loc[val["side_momentum_5m_atr"] <= -overextension, "side_momentum_5m_atr"]),
            fin[fin["side_momentum_5m_atr"] <= -overextension].assign(allocator_score=-fin.loc[fin["side_momentum_5m_atr"] <= -overextension, "side_momentum_5m_atr"]),
            {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1},
        ),
        "currency_strength_only": (
            val.assign(allocator_score=_series_or_empty(val, "side_currency_strength_5m") - _series_or_empty(val, "spread_to_atr_15m")),
            fin.assign(allocator_score=_series_or_empty(fin, "side_currency_strength_5m") - _series_or_empty(fin, "spread_to_atr_15m")),
            {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1},
        ),
        "anti_churn_strict": (val, fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 3, "minimum_hold_minutes": 3}),
        "anti_churn_loose": (val, fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1}),
        "minimum_hold_2m": (val, fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 2}),
        "minimum_hold_3m": (val, fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 3}),
        "replacement_buffered": (val, fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 2, "replacement": True, "replacement_buffer_usd": 0.03}),
    }
    oco_val = oco_selected.get("movement_model_gated_oco_validation", pd.DataFrame()).copy()
    oco_fin = oco_selected.get("movement_model_gated_oco", pd.DataFrame()).copy()
    if not oco_val.empty or not oco_fin.empty:
        for frame in [oco_val, oco_fin]:
            if not frame.empty:
                frame["allocator_score"] = _series_or_empty(frame, "movement_probability_13m")
                frame["allocation_horizon"] = 13
                frame["is_oco"] = True
                frame["mfe_pips"] = _series_or_empty(frame, "mfe_pips", np.nan)
                frame["mae_pips"] = _series_or_empty(frame, "mae_pips", np.nan)
        candidate_sets["oco_high_movement_low_direction"] = (oco_val, oco_fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 1, "minimum_hold_minutes": 1})
        mixed_val = pd.concat([val[(val["direction_probability_13m"] >= 0.50) & (val["movement_probability_13m"] >= 0.50)], val[val["side_momentum_5m_atr"] <= -overextension], val.assign(allocator_score=_series_or_empty(val, "side_currency_strength_5m")), oco_val], ignore_index=True)
        mixed_fin = pd.concat([fin[(fin["direction_probability_13m"] >= 0.50) & (fin["movement_probability_13m"] >= 0.50)], fin[fin["side_momentum_5m_atr"] <= -overextension], fin.assign(allocator_score=_series_or_empty(fin, "side_currency_strength_5m")), oco_fin], ignore_index=True)
        candidate_sets["mixed_branch_allocator"] = (mixed_val, mixed_fin, {"max_positions": 1, "currency_cap": 1, "anti_churn_minutes": 2, "minimum_hold_minutes": 2, "replacement": True, "replacement_buffer_usd": 0.03})
    reports: dict[str, Any] = {}
    final_trades: dict[str, pd.DataFrame] = {}
    for name, (val_candidates, fin_candidates, settings) in candidate_sets.items():
        val_trades, val_diag = _run_allocator(val_candidates, caches, **settings)
        fin_trades, fin_diag = _run_allocator(fin_candidates, caches, **settings)
        final_trades[name] = fin_trades
        reports[name] = {
            "settings": settings,
            "validation": _metrics(val_trades),
            "final": _metrics(fin_trades),
            "validation_diagnostics": val_diag,
            "final_diagnostics": fin_diag,
            "final_exposure": _portfolio_exposure(fin_trades),
        }
    best = max(reports, key=lambda name: reports[name]["validation"]["pnl_account"]) if reports else None
    return {
        "position_model": "All variants use 1,000 total notional units across open slots, executable endpoint costs, active currency caps, anti-churn timing, and early executable exits for replacement events.",
        "validation_selected_allocator": best,
        "best_allocator_final": reports.get(best, {}).get("final"),
        "experiments": reports,
        "no_edge_threshold": no_edge_threshold,
        "calibration_rmse_lower_bound": lower_bound,
    }, final_trades


def _cost_breakeven_for_trades(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {"available": False, "reason": "no trades"}
    work = trades.copy()
    weight = pd.to_numeric(work.get("allocation_weight", pd.Series(1.0, index=work.index)), errors="coerce").fillna(1.0)
    pip_value = pd.to_numeric(work.get("pip_value_usd_per_unit", pd.Series(0.0, index=work.index)), errors="coerce").fillna(0.0) * POSITION_UNITS * weight
    spread = _series_or_empty(work, "spread_pips_used") * pip_value
    total_cost_pips = _series_or_empty(work, "round_trip_cost_pips_used")
    slippage = (total_cost_pips - _series_or_empty(work, "spread_pips_used")).clip(lower=0.0) * pip_value
    base = _safe_sum(work["actual_ev"])
    scenarios = []
    for multiplier in [0.0, 0.5, 1.0, 1.25, 1.5, 2.0]:
        pnl = base + _safe_sum(spread) * (1.0 - multiplier)
        scenarios.append({"spread_multiplier": multiplier, "pnl_account": pnl, "survives": pnl > 0})
    for extra in [0.1, 0.3]:
        scenarios.append({"slippage_add_pips": extra, "pnl_account": base - _safe_sum(pip_value * extra), "survives": base - _safe_sum(pip_value * extra) > 0})
    zero_cost = base + _safe_sum(spread) + _safe_sum(slippage)
    spread_total = _safe_sum(spread)
    pip_value_total = _safe_sum(pip_value)
    return {
        "available": True,
        "base_pnl_account": base,
        "zero_spread_slippage_fantasy_upper_bound": zero_cost,
        "scenarios": scenarios,
        "breakeven_spread_multiplier": 1.0 + base / spread_total if spread_total > 0 else None,
        "breakeven_extra_slippage_pips": base / pip_value_total if pip_value_total > 0 else None,
        "required_average_gross_edge_pips": _safe_mean(total_cost_pips),
        "average_spread_pips": _safe_mean(_series_or_empty(work, "spread_pips_used")),
        "average_round_trip_slippage_pips": _safe_mean((total_cost_pips - _series_or_empty(work, "spread_pips_used")).clip(lower=0.0)),
    }


def _cost_breakeven_report(trade_sets: dict[str, pd.DataFrame]) -> dict[str, Any]:
    return {name: _cost_breakeven_for_trades(trades) for name, trades in trade_sets.items()}


def _horizon_policy_report(
    final: pd.DataFrame,
    model_metadata: dict[str, Any],
    policy_summary: dict[str, Any],
    surface: OracleSurface,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    model_rows: list[dict[str, Any]] = []
    for horizon in HORIZONS:
        net = _series_or_empty(final, f"actual_ev_{horizon}m")
        gross_pips = _series_or_empty(final, f"actual_pips_{horizon}m") + _series_or_empty(final, "round_trip_cost_pips_used")
        gross = gross_pips * _series_or_empty(final, "pip_value_usd_per_unit") * POSITION_UNITS
        rows.append({
            "horizon_min": int(horizon),
            "gross_ev_before_costs": _safe_mean(gross),
            "net_ev_after_costs": _safe_mean(net),
            "mean_mfe_pips": _safe_mean(_series_or_empty(final, f"mfe_pips_{horizon}m")),
            "mean_mae_pips": _safe_mean(_series_or_empty(final, f"mae_pips_{horizon}m")),
            "positive_rate": float((net > 0).mean()),
        })
        score_column = f"direction_probability_{horizon}m"
        if score_column in final.columns:
            scored = final.copy()
            scored["actual_ev"] = net
            scored["actual_pips"] = _series_or_empty(scored, f"actual_pips_{horizon}m")
            scored["mfe_pips"] = _series_or_empty(scored, f"mfe_pips_{horizon}m", np.nan)
            model_top = _select_top(scored, score_column)
            model_rows.append({"horizon_min": int(horizon), **_metrics(model_top), "available": True})
        else:
            model_rows.append({"horizon_min": int(horizon), "available": False, "reason": "No dedicated direction classifier was requested for this reporting-only horizon."})
    oracle_by_horizon = _breakdown(surface.top1, "oracle_horizon_min")
    top_endpoint = max(rows, key=lambda row: row["net_ev_after_costs"] if row["net_ev_after_costs"] is not None else -np.inf)
    available_model_rows = [row for row in model_rows if row.get("available")]
    top_model = max(available_model_rows, key=lambda row: row["pnl_account"]) if available_model_rows else None
    predictability = {int(row["horizon_min"]): row for row in model_metadata.get("horizon_models", [])}
    return {
        "endpoint_horizon_shape": rows,
        "model_selected_horizon_shape": model_rows,
        "policy_contract_shape": policy_summary.get("aggregate", []),
        "oracle_horizon_shape": oracle_by_horizon,
        "highest_endpoint_net_ev_horizon": top_endpoint,
        "highest_model_selected_edge_horizon": top_model,
        "short_horizon_cost_diagnosis": {
            "one_to_five_minute_net_mean_ev": _safe_mean([row["net_ev_after_costs"] for row in rows if row["horizon_min"] <= 5]),
            "one_to_five_minute_gross_mean_ev": _safe_mean([row["gross_ev_before_costs"] for row in rows if row["horizon_min"] <= 5]),
        },
        "longer_horizon_direction_diagnosis": {
            "horizons_13_to_30": [predictability.get(horizon) for horizon in [13, 21, 30] if horizon in predictability],
        },
    }


def _ex_post_branch_label(row: pd.Series | dict[str, Any]) -> str:
    value = row if isinstance(row, dict) else row.to_dict()
    strength = abs(float(value.get("side_currency_strength_5m", 0.0) or 0.0))
    residual = abs(float(value.get("side_pair_residual_5m", 0.0) or 0.0))
    momentum = float(value.get("side_return_5m_pips", 0.0) or 0.0)
    movement = float(value.get("movement_quality", 0.0) or 0.0)
    if strength >= max(residual, abs(momentum)) and strength > 0:
        return "currency_strength"
    if residual >= max(strength, abs(momentum)) and residual > 0:
        return "residual"
    if momentum < 0:
        return "snapback"
    if movement > 0:
        return "movement_continuation"
    return "directional_market"


def _model_oracle_gap_report(
    surface: OracleSurface,
    selections: dict[str, tuple[pd.DataFrame, int]],
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    oracle_branch = {
        pd.Timestamp(row.decision_time_utc): _ex_post_branch_label(row._asdict())
        for row in surface.top1.itertuples(index=False)
    }
    reports: dict[str, Any] = {}
    materialized: dict[str, pd.DataFrame] = {}
    for name, (trades, horizon) in selections.items():
        gap, summary = _oracle_rank_gap(surface, trades, horizon)
        if gap.empty:
            reports[name] = summary
            materialized[name] = gap
            continue
        gap["selected_branch"] = name
        gap["oracle_branch"] = gap["decision_time_utc"].map(oracle_branch)
        summary.update({
            "wrong_policy_rate": 0.0,
            "wrong_policy_definition": "The original selected directional surface contains endpoint-timeout candidates only; fixed/dynamic policy differences are tested separately in HORIZON_POLICY_SHAPE and EXIT_ALPHA_DEEP_DIVE.",
            "wrong_branch_rate": float((gap["selected_branch"] != gap["oracle_branch"]).mean()),
            "actual_ev_rank_summary": {
                "mean_rank": _safe_mean(gap["actual_ev_rank"]),
                "mean_percentile": _safe_mean(gap["actual_ev_percentile"]),
            },
        })
        reports[name] = summary
        materialized[name] = gap
    return {
        "definition": "Oracle uses realized after-cost values only after all validation selection is complete. It is never passed to a model, threshold, allocator, or policy selector.",
        "comparisons": reports,
    }, materialized


def _implementation_audit() -> dict[str, Any]:
    branches = [
        ("directional EV surface", "FULL", "Vectorized executable bid/ask endpoint labels existed in _load_research_dataset.", "FULL", "Retested as the non-oracle baseline surface."),
        ("movement-only branch", "PARTIAL", "Rule proxy used movement_quality but still selected a side with momentum.", "FULL", "Separate movement classifiers, movement decomposition, and movement-gated OCO are implemented."),
        ("OCO breakout branch", "STUB", "Report explicitly said compact proxy and full stop-order simulator remained next increment.", "FULL", "Executable 15m range stop OCO simulator with first-trigger cancellation and adverse ambiguity is implemented."),
        ("cross-sectional momentum branch", "PARTIAL", "A score existed but no targeted branch diagnosis or allocator comparison.", "FULL", "Cross-sectional score is tested against oracle and allocator variants."),
        ("snapback / mean-reversion branch", "PARTIAL", "Simple inverse score existed without deep 1m/3m/5m/8m or ML-gated tests.", "FULL", "Continuation/snapback deep tests and inversion diagnosis are implemented."),
        ("currency-strength rotation branch", "PARTIAL", "Strength score existed, but no delayed entry, rank-decay exit, or expression-quality test.", "FULL", "All requested strength expressions and exits are tested."),
        ("residual / relative-value branch", "PARTIAL", "Residual score was a direct proxy with no factor/residual decomposition.", "FULL", "Residual continuation, reversion, laggard/leader, and factor contribution tests are implemented."),
        ("exit-alpha / remaining-edge branch", "STUB", "Only an exit_alpha_proxy score and endpoint metrics existed.", "FULL", "Executable TP/SL, partial, trailing, signal-decay, opposite-EV, replacement, and time-stop tests are implemented."),
        ("no-edge classifier", "MISSING", "No classifier or standalone gate appeared in the suite.", "FULL", "Train-only classifier, validation-selected threshold, final precision/recall, and allocator gate are implemented."),
        ("baseline suite", "FULL", "No-trade and several simple branch baselines were present.", "FULL", "Expanded baseline comparisons remain part of the diagnostic."),
        ("model zoo", "FULL", "Requested model families were present in a bounded round-robin plan.", "FULL", "Prior winner is retested with separate EV, direction, movement, and no-edge diagnostics."),
        ("calibration", "PARTIAL", "Calibration was descriptive and not tied to an explicit stand-down decision.", "FULL", "Calibration lower bounds feed allocator and no-edge diagnostics."),
        ("cost stress", "PARTIAL", "One-extra-bar delay was explicitly not materialized.", "FULL", "Cost multipliers, zero-cost upper bound, breakeven spread/slippage, and delayed entry are materialized."),
        ("failure classification", "PARTIAL", "Several classes were placeholders or unpopulated fields.", "FULL", "Direction/movement/path/cost classifications are computed on selected final trades."),
        ("profit capture metrics", "PARTIAL", "MFE capture and giveback were endpoint summaries only.", "FULL", "Time-to-MFE/MAE and exit-variant capture are computed."),
        ("currency exposure controls", "PARTIAL", "Only cumulative counts were reported; allocation did not enforce caps.", "FULL", "Allocator enforces active currency caps and reports concentration."),
        ("anti-churn constraints", "PARTIAL", "Only a switch count and estimate were reported.", "FULL", "Allocator applies strict/loose churn, minimum hold, and replacement buffer variants."),
    ]
    rows = [
        {
            "branch": branch,
            "legacy_research_suite_status": old,
            "legacy_evidence": evidence,
            "post_fail_diagnostic_status": new,
            "remediation": remediation,
        }
        for branch, old, evidence, new, remediation in branches
    ]
    legacy_counts = pd.Series([row["legacy_research_suite_status"] for row in rows]).value_counts().to_dict()
    remediated_counts = pd.Series([row["post_fail_diagnostic_status"] for row in rows]).value_counts().to_dict()
    return {
        "scope": "Static audit of fresh_m1_intrahour/src/research_suite.py and related Phase 1 sources, followed by targeted diagnostic remediation in src/post_fail_diagnostic.py.",
        "branches": rows,
        "legacy_status_counts": legacy_counts,
        "post_fail_status_counts": remediated_counts,
        "conclusion": "The original research suite was not sufficient to diagnose the failed run. The post-fail command implements the missing executable diagnostics without changing live execution behavior.",
    }


def _candidate_record(name: str, kind: str, validation: dict[str, Any], final: dict[str, Any], trades: pd.DataFrame) -> dict[str, Any]:
    return {
        "name": name,
        "kind": kind,
        "validation": validation,
        "final": final,
        "trades": trades,
    }


def _decision_tree(
    oracle_exists: bool,
    winner: dict[str, Any] | None,
    best_baseline: dict[str, Any],
    oco_promising: bool,
    snapback_promising: bool,
    currency_promising: bool,
    residual_promising: bool,
    exit_promising: bool,
    no_edge_report: dict[str, Any],
    direction_predictable: bool,
    movement_predictable: bool,
) -> dict[str, Any]:
    winner_pnl = float(winner["final"].get("pnl_account", 0.0)) if winner else 0.0
    decisions: list[dict[str, Any]] = []
    if not oracle_exists:
        decisions.append({"condition": "Oracle top-1 after-cost EV is non-positive", "implication": "Stop short-horizon Tier 1 market-entry research under these assumptions and move to higher timeframe or different instruments."})
    elif winner_pnl <= 0:
        decisions.append({"condition": "Oracle is positive but every validation-selected non-oracle result fails final", "implication": "The immediate blocker is ranking/selection and policy robustness, not proof of an executable live edge."})
    else:
        decisions.append({"condition": "A validation-selected non-oracle branch has positive final after-cost P/L", "implication": "Continue only that branch with additional untouched periods; this is still not live-ready."})
    decisions.append({"condition": f"OCO promising = {oco_promising}", "implication": "Prioritize movement/OCO research only when the executable OCO final result is positive and survives costs."})
    decisions.append({"condition": f"Snapback promising = {snapback_promising}", "implication": "Prioritize reversal/snapback only if it beats continuation on validation-selected final results."})
    decisions.append({"condition": f"Currency strength promising = {currency_promising}; residual promising = {residual_promising}", "implication": "Promote factor or relative-value allocation only if its final selected P/L is positive after costs."})
    decisions.append({"condition": f"Exit alpha promising = {exit_promising}", "implication": "Prioritize exits only if a validation-selected exit variant improves final P/L materially without oracle timing."})
    decisions.append({"condition": f"No-edge gate final P/L = {no_edge_report.get('final_allocator', {}).get('pnl_account')}", "implication": "Use stand-down logic only if it filters more losses than winners and improves the untouched final allocator."})
    decisions.append({"condition": f"Direction predictable = {direction_predictable}; movement predictable = {movement_predictable}", "implication": "If movement is stronger than direction, retain OCO as the only short-horizon branch worth further research."})
    return {
        "root_verdict": "continue_research_only" if winner_pnl > max(float(best_baseline.get("pnl_account", 0.0)), 0.0) else "do_not_proceed_toward_live",
        "decisions": decisions,
        "live_execution_allowed": False,
    }


def run_post_fail_diagnostic(
    cfg: dict[str, Any],
    output_dir: Path,
    start: str,
    end: str,
    tier: str = "tier1",
    pairs: list[str] | None = None,
    max_rows_per_pair: int | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    audit = _implementation_audit()
    _write_report(output_dir, "IMPLEMENTATION_AUDIT", audit)

    df = _load_research_dataset(cfg, output_dir, start, end, tier, pairs, max_rows_per_pair)
    df = _add_mid_direction_columns(df, cfg)
    split = _split(df, cfg)
    partition_by_time: dict[pd.Timestamp, str] = {}
    for partition, frame in [("train", split.train), ("validation", split.validation), ("final", split.final)]:
        for timestamp in pd.to_datetime(frame["decision_time_utc"], utc=True).drop_duplicates():
            partition_by_time[pd.Timestamp(timestamp)] = partition
    caches = _load_market_caches(cfg, start, end, tier, pairs, max_rows_per_pair)
    policy_table, policy_summary = _policy_shape(caches, partition_by_time, cfg)
    if not policy_table.empty:
        policy_table.to_csv(output_dir / "policy_contract_surface.csv", index=False)

    validation, final, model_metadata = _build_diagnostic_model_scores(split)
    validation["actual_ev"] = _series_or_empty(validation, "actual_ev_13m")
    validation["actual_pips"] = _series_or_empty(validation, "actual_pips_13m")
    validation["mfe_pips"] = _series_or_empty(validation, "mfe_pips_13m", np.nan)
    validation["mae_pips"] = _series_or_empty(validation, "mae_pips_13m", np.nan)
    final["actual_ev"] = _series_or_empty(final, "actual_ev_13m")
    final["actual_pips"] = _series_or_empty(final, "actual_pips_13m")
    final["mfe_pips"] = _series_or_empty(final, "mfe_pips_13m", np.nan)
    final["mae_pips"] = _series_or_empty(final, "mae_pips_13m", np.nan)
    model_retest = _evaluate_scored_strategy(validation, final, "predicted_ev", "actual_ev_13m", "calibrated_classifier_ev_regressor_B_13m")
    model_calibration = {
        "validation": _calibration(validation, "predicted_ev", "actual_ev_13m"),
        "final": _calibration(final, "predicted_ev", "actual_ev_13m"),
        "metadata": model_metadata,
    }

    baseline_summary, baseline_reports = _build_baseline_suite(validation, final)
    no_edge, no_edge_val_trades, no_edge_final_trades = _no_edge_report(validation, final)

    oco_frames = [_simulate_oco(cache) for cache in caches.values()]
    oco = pd.concat(oco_frames, ignore_index=True) if oco_frames else pd.DataFrame()
    oco_report, oco_selected = _oco_variant_reports(oco, split.train, validation, final)
    if not oco.empty:
        oco[oco["decision_time_utc"].isin(final["decision_time_utc"].unique())].head(100000).to_csv(output_dir / "oco_final_candidates.csv", index=False)

    continuation_report, continuation_reports = _continuation_snapback_report(validation, final, split.train)
    currency_report, currency_reports = _currency_residual_report(validation, final, df, caches)
    best_baseline_name = max(
        ["no_trade"] + list(baseline_reports),
        key=lambda name: 0.0 if name == "no_trade" else baseline_reports[name]["final_gated"]["pnl_account"],
    )
    best_baseline_pnl = 0.0 if best_baseline_name == "no_trade" else float(baseline_reports[best_baseline_name]["final_gated"]["pnl_account"])
    baseline_entries = pd.DataFrame() if best_baseline_name == "no_trade" else baseline_reports[best_baseline_name]["final_raw_trades"]
    exit_report, exit_variants = _exit_alpha_report(validation, final, baseline_entries, caches)

    surface = _build_oracle_surface(final)
    surface.top_candidates.to_csv(output_dir / "oracle_top_candidates.csv", index=False)
    oracle_report = _oracle_decomposition(final, split.train, surface)
    oracle_report["oracle_oco_breakout"] = oco_report.get("oco_oracle_upper_bound")
    oracle_report["oracle_oco_selected_gap"] = oco_report.get("oco_selected_vs_oracle_mean_gap")
    _write_report(output_dir, "ORACLE_DECOMPOSITION", oracle_report)

    direction_movement, model_top_raw = _direction_movement_report(final, validation, model_metadata, policy_table, oco_selected)
    _write_report(output_dir, "DIRECTION_MOVEMENT_DECOMPOSITION", direction_movement)
    _write_report(output_dir, "FAILURE_CLASSIFICATION_REPORT", direction_movement.get("selected_trade_decomposition", {}))

    best_cont_name = continuation_report["best_continuation_selected_on_validation"]
    best_snap_name = continuation_report["best_snapback_selected_on_validation"]
    gap_inputs: dict[str, tuple[pd.DataFrame, int]] = {
        "model_retest": (model_retest["final_raw_trades"], 13),
        "best_baseline": (baseline_entries, 13),
        "best_continuation": (continuation_reports[best_cont_name]["final_raw_trades"], int(continuation_reports[best_cont_name]["actual_col"].split("_")[-1].removesuffix("m"))),
        "best_snapback": (continuation_reports[best_snap_name]["final_raw_trades"], int(continuation_reports[best_snap_name]["actual_col"].split("_")[-1].removesuffix("m"))),
        "currency_strength": (currency_reports["strongest_vs_weakest_market_continuation"]["final_raw_trades"], 13),
        "residual": (currency_reports["residual_continuation"]["final_raw_trades"], 13),
    }
    model_oracle_gap, gap_tables = _model_oracle_gap_report(surface, gap_inputs)
    if gap_tables:
        pd.concat([table.assign(comparison=name) for name, table in gap_tables.items() if not table.empty], ignore_index=True).head(200000).to_csv(output_dir / "model_selected_vs_oracle.csv", index=False)
    _write_report(output_dir, "MODEL_ORACLE_GAP", model_oracle_gap)

    allocator_report, allocator_trades = _allocator_experiments(
        validation,
        final,
        no_edge.get("threshold_selected_on_validation", -np.inf),
        oco_selected,
        caches,
    )
    best_allocator_name = allocator_report.get("validation_selected_allocator")
    _write_report(output_dir, "ALLOCATOR_EXPERIMENTS", allocator_report)
    if best_allocator_name and best_allocator_name in allocator_trades:
        allocator_trades[best_allocator_name].to_csv(output_dir / "best_allocator_selected_trades.csv", index=False)

    trade_sets: dict[str, pd.DataFrame] = {
        "model_retest": model_retest["final_gated_trades"],
        "best_baseline": baseline_entries,
        "best_continuation": continuation_reports[best_cont_name]["final_gated_trades"],
        "best_snapback": continuation_reports[best_snap_name]["final_gated_trades"],
        "currency_strength": currency_reports["strongest_vs_weakest_market_continuation"]["final_gated_trades"],
        "residual": currency_reports["residual_continuation"]["final_gated_trades"],
        "best_oco": oco_selected.get(oco_report.get("best_oco_variant_selected_on_validation", ""), pd.DataFrame()),
        "best_allocator": allocator_trades.get(best_allocator_name, pd.DataFrame()),
    }
    cost_report = _cost_breakeven_report(trade_sets)
    _write_report(output_dir, "COST_BREAKEVEN_ANALYSIS", cost_report)
    horizon_report = _horizon_policy_report(final, model_metadata, policy_summary, surface)
    _write_report(output_dir, "HORIZON_POLICY_SHAPE", horizon_report)

    _write_report(output_dir, "OCO_DEEP_DIVE", oco_report)
    _write_report(output_dir, "CONTINUATION_SNAPBACK_DEEP_DIVE", continuation_report)
    _write_report(output_dir, "CURRENCY_RESIDUAL_DEEP_DIVE", currency_report)
    _write_report(output_dir, "EXIT_ALPHA_DEEP_DIVE", exit_report)
    _write_report(output_dir, "NO_EDGE_GATE_REPORT", no_edge)
    _write_report(output_dir, "MODEL_CALIBRATION_REPORT", model_calibration)

    candidate_records: list[dict[str, Any]] = [
        _candidate_record("model_retest", "model", model_retest["validation_gated"], model_retest["final_gated"], model_retest["final_gated_trades"]),
        _candidate_record("no_edge_gated_model", "model_gate", no_edge["validation_allocator"], no_edge["final_allocator"], no_edge_final_trades),
    ]
    for name, result in continuation_reports.items():
        candidate_records.append(_candidate_record(name, "continuation_snapback", result["validation_gated"], result["final_gated"], result["final_gated_trades"]))
    for name, result in currency_reports.items():
        candidate_records.append(_candidate_record(name, "currency_residual", result["validation_gated"], result["final_gated"], result["final_gated_trades"]))
    for name, metrics in oco_report.get("variants", {}).items():
        candidate_records.append(_candidate_record(name, "oco", metrics["validation"], metrics["final"], oco_selected.get(name, pd.DataFrame())))
    for name, metrics in allocator_report.get("experiments", {}).items():
        candidate_records.append(_candidate_record(name, "allocator", metrics["validation"], metrics["final"], allocator_trades.get(name, pd.DataFrame())))
    winner = max(candidate_records, key=lambda row: row["validation"].get("pnl_account", 0.0)) if candidate_records else None
    oracle_top1 = oracle_report["candidate_surface"]["top_after_costs_mean_ev"].get("top1")
    oracle_exists = bool(oracle_top1 is not None and oracle_top1 > 0)
    best_baseline = {"name": best_baseline_name, "pnl_account": best_baseline_pnl}
    direction_aucs = [row.get("direction", {}).get("auc_proxy") for row in model_metadata.get("horizon_models", [])]
    movement_aucs = [row.get("movement", {}).get("auc_proxy") for row in model_metadata.get("horizon_models", [])]
    direction_predictable = bool(max([value for value in direction_aucs if value is not None] or [0.5]) > 0.55)
    movement_predictable = bool(max([value for value in movement_aucs if value is not None] or [0.5]) > 0.60)
    best_oco_name = oco_report.get("best_oco_variant_selected_on_validation")
    oco_promising = bool(oco_report.get("variants", {}).get(best_oco_name, {}).get("final", {}).get("pnl_account", 0.0) > 0)
    snapback_promising = bool(continuation_reports[best_snap_name]["final_gated"]["pnl_account"] > 0)
    currency_promising = bool(currency_reports["strongest_vs_weakest_market_continuation"]["final_gated"]["pnl_account"] > 0)
    residual_promising = bool(max(currency_reports["residual_continuation"]["final_gated"]["pnl_account"], currency_reports["residual_mean_reversion"]["final_gated"]["pnl_account"]) > 0)
    best_exit_name = exit_report.get("best_exit_variant_selected_on_validation")
    exit_promising = bool(
        exit_report.get("variants", {}).get(best_exit_name, {}).get("final", {}).get("pnl_account", 0.0) > 0
        and exit_report.get("variants", {}).get(best_exit_name, {}).get("final", {}).get("pnl_account", 0.0)
        > exit_report.get("variants", {}).get("endpoint_timeout", {}).get("final", {}).get("pnl_account", 0.0)
    )
    tree = _decision_tree(
        oracle_exists,
        winner,
        best_baseline,
        oco_promising,
        snapback_promising,
        currency_promising,
        residual_promising,
        exit_promising,
        no_edge,
        direction_predictable,
        movement_predictable,
    )
    _write_report(output_dir, "POST_FAIL_DECISION_TREE", tree)

    winner_final = winner["final"] if winner else _metrics(pd.DataFrame())
    pass_gates = {
        "non_oracle_final_positive_after_costs": winner_final.get("pnl_account", 0.0) > 0,
        "beats_no_trade": winner_final.get("pnl_account", 0.0) > 0,
        "beats_best_simple_baseline": winner_final.get("pnl_account", 0.0) > best_baseline_pnl,
        "selected_on_validation": bool(winner),
        "live_execution_disabled": True,
    }
    verdict = "PASS" if all(pass_gates.values()) else "FAIL"
    if not oracle_exists:
        top_blocker = "Oracle top-1 after-cost endpoint EV is not positive, so Tier 1 short-horizon opportunity is weak under the current contract."
        next_step = "Stop intrahour Tier 1 market-entry expansion and evaluate a higher timeframe or different instruments before any further model work."
    elif verdict == "FAIL":
        top_blocker = "Positive ex-post opportunity, if present, is not being ranked or converted into a validation-selected final edge after costs."
        next_step = "Keep live execution disabled; focus on the narrow branch with the smallest model-oracle gap and validate it on a new untouched period."
    else:
        top_blocker = "The result is research-only and has not survived additional periods, portfolio sizing, or operational validation."
        next_step = "Run another untouched walk-forward diagnostic for only the winning branch; do not enable live execution."
    final_summary = {
        "verdict": verdict,
        "run_id": output_dir.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "command_mode": "post-fail-diagnostic",
        "start": start,
        "end": end,
        "tier": tier,
        "nested_split": split.meta,
        "implementation_audit_status": audit["legacy_status_counts"],
        "post_fail_implementation_status": audit["post_fail_status_counts"],
        "winner_selected_on_validation": {
            "name": winner["name"] if winner else None,
            "kind": winner["kind"] if winner else None,
            "validation": winner["validation"] if winner else None,
            "final": winner_final,
        },
        "best_non_oracle_branch": winner["name"] if winner else None,
        "best_baseline": best_baseline,
        "best_allocator_variant": best_allocator_name,
        "oracle_opportunity_exists": oracle_exists,
        "oracle_top1_after_costs_mean_ev": oracle_top1,
        "direction_predictable": direction_predictable,
        "movement_predictable": movement_predictable,
        "oco_promising": oco_promising,
        "snapback_promising": snapback_promising,
        "currency_strength_promising": currency_promising,
        "residual_promising": residual_promising,
        "exit_alpha_promising": exit_promising,
        "pass_gates": pass_gates,
        "any_branch_beats_no_trade": any(record["final"].get("pnl_account", 0.0) > 0 for record in candidate_records),
        "any_branch_beats_best_baseline": any(record["final"].get("pnl_account", 0.0) > best_baseline_pnl for record in candidate_records),
        "biggest_blocker": top_blocker,
        "next_recommended_implementation_step": next_step,
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
        "phase1_economics_source_of_truth": True,
        "live_execution_confirmation": "No live trading, OANDA bot, credential, or account-file action was invoked by this diagnostic.",
    }
    _write_report(output_dir, "POST_FAIL_FINAL_SUMMARY", final_summary)
    write_json(output_dir / "run_manifest.json", _json_safe({
        "run_dir": str(output_dir),
        "mode": "post-fail-diagnostic",
        "reports": [
            "IMPLEMENTATION_AUDIT.json",
            "ORACLE_DECOMPOSITION.json",
            "MODEL_ORACLE_GAP.json",
            "DIRECTION_MOVEMENT_DECOMPOSITION.json",
            "OCO_DEEP_DIVE.json",
            "CONTINUATION_SNAPBACK_DEEP_DIVE.json",
            "CURRENCY_RESIDUAL_DEEP_DIVE.json",
            "EXIT_ALPHA_DEEP_DIVE.json",
            "COST_BREAKEVEN_ANALYSIS.json",
            "HORIZON_POLICY_SHAPE.json",
            "NO_EDGE_GATE_REPORT.json",
            "ALLOCATOR_EXPERIMENTS.json",
            "POST_FAIL_DECISION_TREE.json",
            "POST_FAIL_FINAL_SUMMARY.json",
        ],
        "live_execution_enabled": False,
        "oanda_execution_enabled": False,
    }))
    return output_dir
