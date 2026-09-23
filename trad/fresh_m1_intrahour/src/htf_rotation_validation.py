from __future__ import annotations

import gc
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from .common import ROOT
from .htf_validation_replay import (
    _profit_factor,
    _rich_metrics,
    allocate_fixed_exposure,
)
from .replay_metrics import write_report_pair


if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from oanda_broker_style_portfolio_replay import (  # noqa: E402
    DEFAULT_MARGIN_RATES,
    BrokerReplay,
    CandleStore,
    direction_int,
    max_spread_for,
    min_trailing_stop_for,
    pip_size,
    safe_float,
    utc_timestamp,
)


INITIAL_EQUITY = 1_000.0
BASE_RISK_PCT = 0.25
TARGET_MARGIN_PCT = 25.0
HARD_MARGIN_PCT = 40.0
TARGET_OPEN_RISK_PCT = 3.0
MAX_OPEN_RISK_PCT = 4.0
DEEP_ADVERSE_R = 0.50
FIXED_HORIZON_QUANTILES = (0.50, 0.75, 0.90, 0.95, 0.98, 0.99)
FIXED_HORIZON_QUOTAS = (1, 3, 8, 12, 16)
FIXED_HORIZON_CAPS = (3, 8, 15, 30)
PATH_THRESHOLD_SPECS = ("positive", "q90", "q95", "q98", "q99")
PATH_EXPOSURE_SPECS = (
    (1, 3),
    (3, 3),
    (8, 8),
    (8, 15),
    (8, 30),
    (12, 30),
    (16, 30),
)

DEFAULT_DEVELOPMENT_REPORT = (
    ROOT
    / "fresh_m1_intrahour"
    / "reports"
    / "htf_corrected_rebuild_tier1_2026q1_20260711"
)
DEFAULT_EVALUATION_REPORTS = (
    ROOT
    / "fresh_m1_intrahour"
    / "reports"
    / "htf_corrected_rebuild_tier1_2026q1_20260711",
    ROOT
    / "fresh_m1_intrahour"
    / "reports"
    / "htf_corrected_rebuild_tier1_20260710_v2",
)
LEGACY_REPLAY_REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "fresh_fullhist_m30_h1_h4_2025_validation_maxnew8_20260707"
    / "period_exact_oanda_replay"
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _load_surface(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path)
    required = {
        "instrument",
        "entry_time_utc",
        "label_end_time_utc",
        "selected_score",
        "selected_direction",
        "selected_pnl_usd",
        "selected_net_pips",
        "selected_gross_pips",
        "selected_spread_drag_pips",
        "atr240_pips",
        "spread_pips",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"prediction surface missing columns: {sorted(missing)}")
    for column in ("entry_time_utc", "label_end_time_utc", "exit_time_utc"):
        if column in frame:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    frame = frame.dropna(
        subset=[
            "instrument",
            "entry_time_utc",
            "label_end_time_utc",
            "selected_score",
            "selected_direction",
        ]
    ).copy()
    frame["instrument"] = frame["instrument"].astype(str)
    return frame.sort_values(["entry_time_utc", "selected_score"], ascending=[True, False])


def load_corrected_prediction_surfaces(
    development_report: Path,
    evaluation_reports: Iterable[Path],
    *,
    start: str,
    end: str,
    allowed_pairs: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    development = _load_surface(
        development_report / "corrected_h1_selected_candidate_oos_predictions.parquet"
    )
    evaluation_parts: list[pd.DataFrame] = []
    evaluation_manifest: list[dict[str, Any]] = []
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize("UTC")
    else:
        start_ts = start_ts.tz_convert("UTC")
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize("UTC")
    else:
        end_ts = end_ts.tz_convert("UTC")
    if len(str(end).strip()) == 10:
        end_ts += pd.Timedelta(days=1)
    for report in evaluation_reports:
        surface = _load_surface(report / "corrected_h1_frozen_evaluation_predictions.parquet")
        surface = surface[
            (surface["entry_time_utc"] >= start_ts)
            & (surface["entry_time_utc"] < end_ts)
        ].copy()
        evaluation_manifest.append({
            "report": str(report),
            "rows_in_requested_period": int(len(surface)),
            "start": surface["entry_time_utc"].min() if len(surface) else None,
            "end": surface["entry_time_utc"].max() if len(surface) else None,
        })
        if len(surface):
            evaluation_parts.append(surface)
    if not evaluation_parts:
        raise ValueError("no corrected evaluation predictions found for requested period")
    evaluation = pd.concat(evaluation_parts, ignore_index=True)
    evaluation = evaluation.sort_values(["entry_time_utc", "selected_score"])
    evaluation = evaluation.drop_duplicates(["instrument", "entry_time_utc"], keep="last")
    development = development[development["instrument"].isin(allowed_pairs)].copy()
    evaluation = evaluation[evaluation["instrument"].isin(allowed_pairs)].copy()
    if development.empty or evaluation.empty:
        raise ValueError("tier/pair filtering removed all corrected predictions")
    manifest = {
        "development_report": str(development_report),
        "development_rows": int(len(development)),
        "development_start": development["entry_time_utc"].min(),
        "development_end": development["entry_time_utc"].max(),
        "evaluation_reports": evaluation_manifest,
        "evaluation_rows": int(len(evaluation)),
        "evaluation_start": evaluation["entry_time_utc"].min(),
        "evaluation_end": evaluation["entry_time_utc"].max(),
        "pairs": sorted(development["instrument"].unique().tolist()),
        "pair_count": int(development["instrument"].nunique()),
        "candidate": str(development["candidate"].iloc[0]) if "candidate" in development else None,
        "feature_family": str(development["feature_family"].iloc[0]) if "feature_family" in development else None,
        "direction_family": str(development["direction_family"].iloc[0]) if "direction_family" in development else None,
        "objective": str(development["objective"].iloc[0]) if "objective" in development else None,
    }
    return development, evaluation, manifest


def _fold_metrics(trades: pd.DataFrame) -> list[dict[str, Any]]:
    if trades.empty:
        return []
    work = trades.copy()
    if "fold" not in work or work["fold"].isna().all():
        timestamps = pd.to_datetime(work["entry_time_utc"], utc=True)
        work["fold"] = timestamps.dt.year.astype(str) + "Q" + (((timestamps.dt.month - 1) // 3) + 1).astype(str)
    rows = []
    for fold, group in work.groupby("fold", sort=True):
        pnl = pd.to_numeric(group["selected_pnl_usd"], errors="coerce").fillna(0.0)
        rows.append({
            "fold": str(fold),
            "trades": int(len(group)),
            "pnl_usd": float(pnl.sum()),
            "win_rate": float((pnl > 0.0).mean()),
            "profit_factor": _profit_factor(pnl),
        })
    return rows


def fixed_horizon_viability(metrics: dict[str, Any], folds: list[dict[str, Any]]) -> dict[str, Any]:
    checks = {
        "minimum_100_trades": int(metrics.get("trades", 0)) >= 100,
        "positive_development_pnl": float(metrics.get("pnl_usd", 0.0)) > 0.0,
        "positive_mean_net_pips": float(metrics.get("mean_net_pips") or -math.inf) > 0.0,
        "profit_factor_above_one": float(metrics.get("profit_factor") or 0.0) > 1.0,
        "positive_quarter_majority": sum(float(row["pnl_usd"]) > 0.0 for row in folds) >= 3,
        "max_drawdown_at_most_35pct": float(metrics.get("max_drawdown_pct", math.inf)) <= 35.0,
    }
    return {
        **checks,
        "passed_check_count": int(sum(checks.values())),
        "total_check_count": int(len(checks)),
        "viable": bool(all(checks.values())),
    }


def fixed_horizon_grid(surface: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for quantile in FIXED_HORIZON_QUANTILES:
        threshold = max(0.0, float(surface["selected_score"].quantile(quantile)))
        eligible = surface[surface["selected_score"] >= threshold].copy()
        for quota in FIXED_HORIZON_QUOTAS:
            for cap in FIXED_HORIZON_CAPS:
                trades, attribution = allocate_fixed_exposure(
                    eligible,
                    max_new_per_timestamp=int(quota),
                    max_open_positions=int(cap),
                )
                metrics = _rich_metrics(trades, eligible)
                folds = _fold_metrics(trades)
                rows.append({
                    "score_quantile": float(quantile),
                    "frozen_score_threshold": float(threshold),
                    "max_new_per_timestamp": int(quota),
                    "max_open_positions": int(cap),
                    "metrics": metrics,
                    "folds": folds,
                    "viability": fixed_horizon_viability(metrics, folds),
                    "gate_attribution": attribution,
                })
    return rows


def _fixed_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    viability = row["viability"]
    metrics = row["metrics"]
    return (
        bool(viability["viable"]),
        int(viability["passed_check_count"]),
        sum(float(fold["pnl_usd"]) > 0.0 for fold in row["folds"]),
        float(metrics.get("pnl_usd", -math.inf)),
        float(metrics.get("mean_net_pips") or -math.inf),
        -float(metrics.get("max_drawdown_pct", math.inf)),
    )


def _threshold_map(surface: pd.DataFrame) -> dict[str, float]:
    return {
        "positive": 0.0,
        "q90": max(0.0, float(surface["selected_score"].quantile(0.90))),
        "q95": max(0.0, float(surface["selected_score"].quantile(0.95))),
        "q98": max(0.0, float(surface["selected_score"].quantile(0.98))),
        "q99": max(0.0, float(surface["selected_score"].quantile(0.99))),
    }


def _empirical_percentile(scores: np.ndarray, reference: np.ndarray) -> np.ndarray:
    ordered = np.sort(reference[np.isfinite(reference)])
    if not len(ordered):
        return np.full(len(scores), 0.5, dtype=float)
    positions = np.searchsorted(ordered, scores, side="right")
    return np.clip(positions / len(ordered), 0.0, 1.0)


def build_rotation_candidates(
    surface: pd.DataFrame,
    development_scores: np.ndarray,
) -> pd.DataFrame:
    frame = surface.copy()
    scores = pd.to_numeric(frame["selected_score"], errors="coerce").to_numpy(dtype=float)
    percentile = _empirical_percentile(scores, development_scores)
    atr = pd.to_numeric(frame["atr240_pips"], errors="coerce").fillna(0.1).clip(lower=0.1)
    frame["time_utc"] = pd.to_datetime(frame["entry_time_utc"], utc=True)
    frame["planned_exit_time"] = pd.to_datetime(frame["label_end_time_utc"], utc=True)
    frame["direction"] = frame["selected_direction"].astype(str).str.upper()
    frame["campaign"] = frame["instrument"].astype(str) + ":" + frame["direction"]
    frame["horizon_minutes"] = 120
    frame["probability"] = np.clip(percentile, 0.50, 0.999999)
    frame["confidence"] = frame["probability"]
    frame["expected_net_pips"] = scores
    frame["expected_move_atr"] = scores / atr.to_numpy(dtype=float)
    frame["rank_score"] = scores
    frame["score_percentile"] = percentile
    frame["model_type"] = "corrected_hgb"
    frame["feature_set"] = frame.get("feature_family", "safe_full_220")
    frame["target"] = frame.get("objective", "endpoint_regressor")
    frame["outcome"] = "corrected_120m_executable_endpoint"
    frame["experiment_id"] = "corrected_h1_rebuild"
    normalized = frame["time_utc"].dt.normalize()
    frame["week_start"] = (
        normalized - pd.to_timedelta(normalized.dt.weekday, unit="D")
    ).astype(str)
    frame["source_stream"] = "corrected_h1"
    frame["fold"] = pd.to_numeric(frame.get("fold", 0), errors="coerce").fillna(0).astype(int)
    keep = [
        "time_utc",
        "instrument",
        "direction",
        "campaign",
        "planned_exit_time",
        "horizon_minutes",
        "fold",
        "week_start",
        "experiment_id",
        "model_type",
        "feature_set",
        "target",
        "outcome",
        "probability",
        "confidence",
        "expected_move_atr",
        "expected_net_pips",
        "rank_score",
        "score_percentile",
        "spread_pips",
        "atr240_pips",
        "source_stream",
    ]
    return frame[keep].replace([np.inf, -np.inf], np.nan).dropna(
        subset=["time_utc", "planned_exit_time", "rank_score", "expected_move_atr"]
    ).sort_values(["time_utc", "rank_score"], ascending=[True, False])


class OpenPriceCandleStore(CandleStore):
    """Research candle store whose event snapshot is the executable M1 open."""

    def load_instrument(self, instrument: str) -> pd.DataFrame:
        instrument = str(instrument)
        cached = self.frames.get(instrument)
        if cached is not None:
            return cached
        path = self.root / f"{instrument}_M1.csv"
        if not path.exists():
            self.frames[instrument] = pd.DataFrame()
            return self.frames[instrument]
        columns = [
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "bid_open",
            "bid_high",
            "bid_low",
            "bid_close",
            "ask_open",
            "ask_high",
            "ask_low",
            "ask_close",
            "spread_pips",
        ]
        frame = pd.read_csv(path, usecols=lambda column: column in columns)
        frame["datetime"] = pd.to_datetime(frame["datetime"], utc=True, errors="coerce")
        frame = frame[(frame["datetime"] >= self.start) & (frame["datetime"] <= self.end)].copy()
        if frame.empty:
            self.frames[instrument] = frame
            return frame
        for column in columns:
            if column != "datetime" and column not in frame:
                frame[column] = np.nan
        for column in columns:
            if column != "datetime":
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        mid_open = frame["open"].where(np.isfinite(frame["open"]), frame["close"])
        mid_close = frame["close"].where(np.isfinite(frame["close"]), mid_open)
        spread = frame["spread_pips"].copy()
        derived = (frame["ask_open"] - frame["bid_open"]).abs() / pip_size(instrument)
        spread = spread.where(np.isfinite(spread), derived).fillna(0.0).clip(lower=0.0)
        half = spread * pip_size(instrument) / 2.0
        frame["open"] = mid_open
        frame["close"] = mid_close
        for side, sign in (("bid", -1.0), ("ask", 1.0)):
            frame[f"{side}_open"] = frame[f"{side}_open"].where(
                np.isfinite(frame[f"{side}_open"]), mid_open + sign * half
            )
            frame[f"{side}_close"] = frame[f"{side}_close"].where(
                np.isfinite(frame[f"{side}_close"]), mid_close + sign * half
            )
            frame[f"{side}_high"] = frame[f"{side}_high"].where(
                np.isfinite(frame[f"{side}_high"]), frame["high"] + sign * half
            )
            frame[f"{side}_low"] = frame[f"{side}_low"].where(
                np.isfinite(frame[f"{side}_low"]), frame["low"] + sign * half
            )
        frame["spread_pips"] = spread
        frame = frame.dropna(subset=["datetime", "open", "close"])
        frame = frame.sort_values("datetime").drop_duplicates("datetime", keep="last").set_index("datetime")
        self.frames[instrument] = frame
        hits = frame.reindex(self.timestamps.intersection(frame.index))
        for timestamp, row in hits.iterrows():
            self.price_lookup.setdefault(timestamp, {})[instrument] = safe_float(row.get("open"), 0.0)
        return frame

    def snapshot(self, instrument: str, time_utc: pd.Timestamp) -> dict[str, float] | None:
        frame = self.load_instrument(instrument)
        if frame.empty:
            return None
        timestamp = utc_timestamp(time_utc)
        location = frame.index.searchsorted(timestamp, side="right") - 1
        if location < 0:
            return None
        row = frame.iloc[location]
        return {
            "time_utc": frame.index[location],
            "mid": safe_float(row.get("open"), 0.0),
            "bid": safe_float(row.get("bid_open"), 0.0),
            "ask": safe_float(row.get("ask_open"), 0.0),
            "spread_pips": safe_float(row.get("spread_pips"), 0.0),
        }


class CorrectedRotationReplay(BrokerReplay):
    def __init__(
        self,
        *args: Any,
        sizing_mode: str,
        fixed_units: int,
        fixed_risk_pct: float,
        initial_equity: float,
        slippage_round_trip_pips: float,
        spread_multiplier: float = 1.0,
        retain_blocked_samples: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sizing_mode = str(sizing_mode)
        self.research_fixed_units = int(fixed_units)
        self.fixed_risk_pct = float(fixed_risk_pct)
        self.initial_equity_contract = float(initial_equity)
        self.slippage_round_trip_pips = float(slippage_round_trip_pips)
        self.spread_multiplier = float(spread_multiplier)
        self.retain_blocked_samples = int(retain_blocked_samples)
        self.block_reason_counts: Counter[str] = Counter()
        self.blocked_total = 0

    def block(self, candidate: pd.Series, reason: str, detail: str = "") -> None:
        self.blocked_total += 1
        self.block_reason_counts[str(reason)] += 1
        if len(self.blocked_rows) < self.retain_blocked_samples:
            super().block(candidate, reason, detail)

    def candidate_trade_terms(self, candidate: pd.Series, time_utc: pd.Timestamp) -> dict[str, Any] | None:
        instrument = str(candidate["instrument"])
        snap = self.candles.snapshot(instrument, time_utc)
        if not snap or min(snap["mid"], snap["bid"], snap["ask"]) <= 0.0:
            return None
        direction = direction_int(candidate.get("direction"))
        pip = pip_size(instrument)
        native_spread = max(safe_float(candidate.get("spread_pips"), snap["spread_pips"]), snap["spread_pips"], 0.0)
        stressed_spread = native_spread * self.spread_multiplier
        extra_half_spread = max(0.0, stressed_spread - native_spread) * pip / 2.0
        one_way_slippage = self.slippage_round_trip_pips * pip / 2.0
        atr_pips = max(0.1, safe_float(candidate.get("atr240_pips"), 0.1))
        edge_pips = max(0.0, safe_float(candidate.get("expected_net_pips"), 0.0))
        stop_pips = max(
            atr_pips * safe_float(self.live_cfg.get("atr_stop_multiplier"), 1.5),
            stressed_spread * safe_float(self.live_cfg.get("min_stop_spread_multiple"), 2.0),
            0.1,
        )
        take_profit_pips = max(
            edge_pips * safe_float(self.live_cfg.get("take_profit_edge_capture"), 0.6),
            stop_pips * safe_float(self.live_cfg.get("take_profit_min_r_multiple"), 0.5),
        )
        trailing_stop_pips = max(
            stop_pips * safe_float(self.live_cfg.get("trailing_stop_r_multiple"), 1.0),
            min_trailing_stop_for(self.live_cfg, instrument, stressed_spread),
        )
        if direction > 0:
            entry = snap["ask"] + extra_half_spread + one_way_slippage
            stop_loss = entry - stop_pips * pip
            take_profit = entry + take_profit_pips * pip
            trailing_stop = entry - trailing_stop_pips * pip
        else:
            entry = snap["bid"] - extra_half_spread - one_way_slippage
            stop_loss = entry + stop_pips * pip
            take_profit = entry - take_profit_pips * pip
            trailing_stop = entry + trailing_stop_pips * pip
        return {
            "direction": direction,
            "entry": entry,
            "mid": snap["mid"],
            "spread_pips": stressed_spread,
            "atr_pips": atr_pips,
            "edge_pips": edge_pips,
            "stop_pips": stop_pips,
            "take_profit_pips": take_profit_pips,
            "trailing_stop_pips": trailing_stop_pips,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "trailing_stop": trailing_stop,
        }

    def size_units(
        self,
        candidate: pd.Series,
        terms: dict[str, Any],
        time_utc: pd.Timestamp,
        released_margin: float = 0.0,
    ) -> tuple[int, dict[str, float]]:
        instrument = str(candidate["instrument"])
        nav = max(self.nav(time_utc), 0.0)
        if nav <= 0.0:
            return 0, {}
        pip_value = self.pip_value_per_unit_usd(instrument, terms["mid"], time_utc)
        risk_pips = terms["stop_pips"] + self.slippage_round_trip_pips / 2.0
        if self.sizing_mode == "fixed_units":
            units = int(self.research_fixed_units)
        elif self.sizing_mode == "fixed_initial_risk":
            risk_usd_budget = self.initial_equity_contract * self.fixed_risk_pct / 100.0
            units = int(risk_usd_budget / max(risk_pips * pip_value, 1e-9))
        elif self.sizing_mode == "legacy_compounding":
            return super().size_units(candidate, terms, time_utc, released_margin)
        else:
            raise ValueError(f"unknown sizing mode: {self.sizing_mode}")
        if units < 1:
            return 0, {}
        margin_per_unit = self.margin_mid_usd(instrument, terms["mid"], time_utc) * self.margin_rate(instrument)
        hard_margin_total = nav * safe_float(self.profile.get("hard_margin_pct"), HARD_MARGIN_PCT) / 100.0
        remaining_margin = max(0.0, hard_margin_total - self.margin_used(time_utc) + released_margin)
        if units * margin_per_unit > remaining_margin:
            return 0, {}
        risk_usd = risk_pips * pip_value * units
        signed_units = units if terms["direction"] > 0 else -units
        return signed_units, {
            "risk_pct": risk_usd / max(nav, 1e-9) * 100.0,
            "risk_usd_estimate": risk_usd,
            "margin_required": margin_per_unit * units,
            "pip_value": pip_value,
        }

    def open_candidate(self, candidate: pd.Series, time_utc: pd.Timestamp) -> bool:
        before_ids = {position.trade_id for position in self.open_positions}
        opened = super().open_candidate(candidate, time_utc)
        if opened:
            for position in self.open_positions:
                if position.trade_id not in before_ids:
                    position.last_check_time = utc_timestamp(time_utc) - pd.Timedelta(nanoseconds=1)
        return opened

    def close_position(
        self,
        position: Any,
        close_time: pd.Timestamp,
        reason: str,
        exit_price: float | None = None,
    ) -> None:
        snap = self.candles.snapshot(position.instrument, close_time)
        if exit_price is None:
            if snap:
                exit_price = snap["bid"] if position.direction > 0 else snap["ask"]
            else:
                exit_price = position.entry_price
        native_spread = safe_float(snap.get("spread_pips") if snap else position.spread_pips, position.spread_pips)
        extra_half_spread = max(0.0, native_spread * (self.spread_multiplier - 1.0)) * pip_size(position.instrument) / 2.0
        one_way_slippage = self.slippage_round_trip_pips * pip_size(position.instrument) / 2.0
        if position.direction > 0:
            exit_price = float(exit_price) - extra_half_spread - one_way_slippage
        else:
            exit_price = float(exit_price) + extra_half_spread + one_way_slippage
        super().close_position(position, close_time, reason, exit_price)

    def summary(self, candidates: pd.DataFrame) -> dict[str, Any]:
        summary = super().summary(candidates)
        summary["blocked_candidates"] = int(self.blocked_total)
        summary["blocked_reason_counts"] = dict(sorted(self.block_reason_counts.items()))
        return summary


def _legacy_contract(
    threshold: float,
    max_open_positions: int,
    *,
    realistic: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    live_cfg = _read_json(LEGACY_REPLAY_REPORT / "live_config_snapshot.json")
    profile = _read_json(LEGACY_REPLAY_REPORT / "profile_snapshot.json")
    live_cfg.update({
        "live_execution_enabled": False,
        "demo_execution_enabled": False,
        "oanda_execution_enabled": False,
        "broker_placement_enabled": False,
        "live_new_entries_enabled": False,
        "simulate_live_exit_checks": True,
        "conservative_intrabar_order": True,
        "deep_adverse_r_threshold": DEEP_ADVERSE_R,
        "atr_stop_multiplier": 1.5,
        "take_profit_edge_capture": 0.6,
        "take_profit_min_r_multiple": 0.5,
        "trailing_stop_r_multiple": 1.0,
        "slippage_pips_default": 0.0,
        "slippage_pips_exotic": 0.0,
    })
    profile.update({
        "start_equity": INITIAL_EQUITY,
        "max_open_positions": int(max_open_positions),
        "min_rank_score": float(threshold),
        "min_probability": 0.0,
        "max_same_pair_positions": 1,
    })
    if realistic:
        profile.update({
            "target_margin_pct": TARGET_MARGIN_PCT,
            "hard_margin_pct": HARD_MARGIN_PCT,
            "emergency_margin_pct": HARD_MARGIN_PCT,
            "target_open_risk_pct": TARGET_OPEN_RISK_PCT,
            "max_open_risk_pct": MAX_OPEN_RISK_PCT,
            "max_risk_pct": BASE_RISK_PCT,
            "min_risk_pct": BASE_RISK_PCT,
            "high_risk_pct": BASE_RISK_PCT,
            "medium_risk_pct": BASE_RISK_PCT,
            "low_risk_pct": BASE_RISK_PCT,
            "min_throttled_risk_pct": BASE_RISK_PCT,
        })
    return live_cfg, profile


def path_viability(summary: dict[str, Any], quarter_rows: list[dict[str, Any]]) -> dict[str, Any]:
    checks = {
        "minimum_100_trades": int(summary.get("opened_trades", 0)) >= 100,
        "positive_development_pnl": float(summary.get("pnl_usd", 0.0)) > 0.0,
        "positive_path_adjusted_pnl": float(summary.get("path_adjusted_pnl_usd", 0.0)) > 0.0,
        "profit_factor_at_least_1p05": float(summary.get("profit_factor", 0.0)) >= 1.05,
        "positive_quarter_majority": sum(float(row["pnl_usd"]) > 0.0 for row in quarter_rows) >= 3,
        "max_drawdown_at_most_25pct": float(summary.get("max_drawdown_pct", math.inf)) <= 25.0,
        "max_margin_at_most_40pct": float(summary.get("max_margin_used_pct", math.inf)) <= HARD_MARGIN_PCT + 1e-9,
        "no_margin_call_rows": int(summary.get("margin_call_rows", 1)) == 0,
        "ending_equity_positive": float(summary.get("ending_balance", 0.0)) > 0.0,
    }
    return {
        **checks,
        "passed_check_count": int(sum(checks.values())),
        "total_check_count": int(len(checks)),
        "viable": bool(all(checks.values())),
    }


def _path_quarters(closed_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not closed_rows:
        return []
    frame = pd.DataFrame(closed_rows)
    timestamps = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")
    frame["quarter"] = timestamps.dt.year.astype(str) + "Q" + (((timestamps.dt.month - 1) // 3) + 1).astype(str)
    rows = []
    for quarter, group in frame.groupby("quarter", sort=True):
        pnl = pd.to_numeric(group["pnl"], errors="coerce").fillna(0.0)
        adjusted = pd.to_numeric(group["path_adjusted_pnl"], errors="coerce").fillna(0.0)
        rows.append({
            "quarter": str(quarter),
            "trades": int(len(group)),
            "pnl_usd": float(pnl.sum()),
            "path_adjusted_pnl_usd": float(adjusted.sum()),
            "win_rate": float((pnl > 0.0).mean()),
            "profit_factor": _profit_factor(pnl),
        })
    return rows


def run_path_variant(
    candidates: pd.DataFrame,
    candles: OpenPriceCandleStore,
    *,
    threshold_name: str,
    threshold: float,
    quota: int,
    cap: int,
    sizing_mode: str = "fixed_initial_risk",
    fixed_units: int = 10_000,
    fixed_risk_pct: float = BASE_RISK_PCT,
    spread_multiplier: float = 1.0,
    slippage_round_trip_pips: float = 0.2,
    retain_outputs: bool = False,
) -> tuple[dict[str, Any], CorrectedRotationReplay]:
    realistic = sizing_mode != "legacy_compounding"
    live_cfg, profile = _legacy_contract(threshold, cap, realistic=realistic)
    replay = CorrectedRotationReplay(
        profile,
        candles,
        margin_rates=dict(DEFAULT_MARGIN_RATES),
        live_cfg=live_cfg,
        max_new_positions_per_timestamp=int(quota),
        sizing_mode=sizing_mode,
        fixed_units=fixed_units,
        fixed_risk_pct=fixed_risk_pct,
        initial_equity=INITIAL_EQUITY,
        slippage_round_trip_pips=slippage_round_trip_pips,
        spread_multiplier=spread_multiplier,
        retain_blocked_samples=200 if retain_outputs else 0,
    )
    started = time.perf_counter()
    summary = replay.run(candidates)
    elapsed = time.perf_counter() - started
    quarters = _path_quarters(replay.closed_rows)
    summary.update({
        "threshold_name": threshold_name,
        "frozen_score_threshold": float(threshold),
        "max_new_per_timestamp": int(quota),
        "max_open_positions": int(cap),
        "sizing_mode": sizing_mode,
        "fixed_units": int(fixed_units) if sizing_mode == "fixed_units" else None,
        "fixed_initial_risk_pct": float(fixed_risk_pct) if sizing_mode == "fixed_initial_risk" else None,
        "initial_equity": INITIAL_EQUITY,
        "compounding_position_risk": sizing_mode == "legacy_compounding",
        "spread_multiplier": float(spread_multiplier),
        "slippage_round_trip_pips": float(slippage_round_trip_pips),
        "pnl_usd": float(summary["ending_balance"] - INITIAL_EQUITY),
        "path_adjusted_pnl_usd": float(summary["path_adjusted_ending_balance"] - INITIAL_EQUITY),
        "quarter_breakdown": quarters,
        "elapsed_seconds": round(elapsed, 3),
    })
    summary["viability"] = path_viability(summary, quarters) if realistic else {
        "viable": False,
        "eligible_for_selection": False,
        "reason": "legacy compounding diagnostic is excluded from validation selection",
    }
    return summary, replay


def _path_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    viability = row["viability"]
    return (
        bool(viability.get("viable", False)),
        int(viability.get("passed_check_count", 0)),
        sum(float(item["pnl_usd"]) > 0.0 for item in row.get("quarter_breakdown", [])),
        float(row.get("path_adjusted_pnl_usd", -math.inf)),
        float(row.get("pnl_usd", -math.inf)),
        float(row.get("profit_factor", 0.0)),
        -float(row.get("max_drawdown_pct", math.inf)),
    )


def _selected_identity(row: dict[str, Any]) -> tuple[str, int, int]:
    return (
        str(row["threshold_name"]),
        int(row["max_new_per_timestamp"]),
        int(row["max_open_positions"]),
    )


def _surface_period_breakdown(surface: pd.DataFrame) -> list[dict[str, Any]]:
    timestamps = pd.to_datetime(surface["entry_time_utc"], utc=True)
    labels = timestamps.dt.year.astype(str) + "Q" + (((timestamps.dt.month - 1) // 3) + 1).astype(str)
    return [
        {
            "quarter": str(label),
            "rows": int(len(group)),
            "start": group["entry_time_utc"].min(),
            "end": group["entry_time_utc"].max(),
        }
        for label, group in surface.assign(_quarter=labels).groupby("_quarter", sort=True)
    ]


def _compact_fixed_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    flat = []
    for row in rows:
        flat.append({
            "score_quantile": row["score_quantile"],
            "frozen_score_threshold": row["frozen_score_threshold"],
            "max_new_per_timestamp": row["max_new_per_timestamp"],
            "max_open_positions": row["max_open_positions"],
            **{f"metric_{key}": value for key, value in row["metrics"].items() if not isinstance(value, (list, dict))},
            "positive_quarters": sum(float(fold["pnl_usd"]) > 0.0 for fold in row["folds"]),
            "viable": row["viability"]["viable"],
            "passed_checks": row["viability"]["passed_check_count"],
        })
    return pd.DataFrame(flat)


def _write_replay_outputs(output_dir: Path, replay: CorrectedRotationReplay) -> None:
    pd.DataFrame(replay.closed_rows).to_csv(output_dir / "selected_evaluation_trades.csv", index=False)
    pd.DataFrame(replay.curve_rows).to_csv(output_dir / "selected_evaluation_equity_curve.csv", index=False)
    pd.DataFrame(replay.blocked_rows).to_csv(output_dir / "selected_evaluation_blocked_samples.csv", index=False)


def run_htf_rotation_validation(
    cfg: dict[str, Any],
    output_dir: Path,
    *,
    start: str,
    end: str,
    tier: str,
    pairs: list[str] | None = None,
    development_report: Path | None = None,
    evaluation_reports: list[Path] | None = None,
    skip_path_grid: bool = False,
) -> Path:
    if tier != "tier1":
        raise ValueError("HTF rotation validation is Tier1-only unless a later task explicitly expands it")
    execution = cfg.get("execution", {})
    if bool(execution.get("live_execution_enabled")) or bool(execution.get("demo_execution_enabled")):
        raise RuntimeError("research safety contract requires live and demo execution disabled")
    output_dir.mkdir(parents=True, exist_ok=True)
    development_report = development_report or DEFAULT_DEVELOPMENT_REPORT
    evaluation_reports = evaluation_reports or list(DEFAULT_EVALUATION_REPORTS)
    allowed_pairs = set(pairs or cfg["tier1_pairs"])
    development, evaluation, data_manifest = load_corrected_prediction_surfaces(
        development_report,
        evaluation_reports,
        start=start,
        end=end,
        allowed_pairs=allowed_pairs,
    )
    development_scores = pd.to_numeric(development["selected_score"], errors="coerce").to_numpy(dtype=float)
    thresholds = _threshold_map(development)

    fixed_rows = fixed_horizon_grid(development)
    fixed_best = max(fixed_rows, key=_fixed_rank)
    _compact_fixed_rows(fixed_rows).to_csv(output_dir / "HTF_ROTATION_FIXED_HORIZON_GRID.csv", index=False)

    development_candidates = build_rotation_candidates(development, development_scores)
    evaluation_candidates = build_rotation_candidates(evaluation, development_scores)
    all_times = development_candidates["time_utc"].tolist()
    development_end = development_candidates["planned_exit_time"].max()
    candles = OpenPriceCandleStore(ROOT / cfg["candles_dir"], all_times, development_end)
    candles.load_all(development_candidates["instrument"].unique())

    path_rows: list[dict[str, Any]] = []
    path_grid_specs = [
        (threshold_name, quota, cap)
        for threshold_name in PATH_THRESHOLD_SPECS
        for quota, cap in PATH_EXPOSURE_SPECS
    ]
    if skip_path_grid:
        path_grid_specs = [("q99", 1, 3), ("q99", 8, 30), ("q99", 12, 30)]
    for threshold_name, quota, cap in path_grid_specs:
        row, _replay = run_path_variant(
            development_candidates,
            candles,
            threshold_name=threshold_name,
            threshold=thresholds[threshold_name],
            quota=quota,
            cap=cap,
        )
        path_rows.append(row)
    path_best = max(path_rows, key=_path_rank)
    path_viable_rows = [row for row in path_rows if row["viability"].get("viable", False)]

    exact_fixed_units_dev, _ = run_path_variant(
        development_candidates,
        candles,
        threshold_name="q99",
        threshold=thresholds["q99"],
        quota=8,
        cap=30,
        sizing_mode="fixed_units",
        fixed_units=10_000,
    )
    legacy_compounding_dev, _ = run_path_variant(
        development_candidates,
        candles,
        threshold_name="q99",
        threshold=thresholds["q99"],
        quota=8,
        cap=30,
        sizing_mode="legacy_compounding",
    )
    del candles
    gc.collect()

    evaluation_times = evaluation_candidates["time_utc"].tolist()
    evaluation_end = evaluation_candidates["planned_exit_time"].max()
    evaluation_candles = OpenPriceCandleStore(ROOT / cfg["candles_dir"], evaluation_times, evaluation_end)
    evaluation_candles.load_all(evaluation_candidates["instrument"].unique())

    identities = {
        _selected_identity(path_best),
        ("q99", 1, 3),
        ("q99", 8, 30),
        ("q99", 12, 30),
        ("q99", 16, 30),
    }
    evaluation_rows: list[dict[str, Any]] = []
    selected_replay: CorrectedRotationReplay | None = None
    for threshold_name, quota, cap in sorted(identities):
        row, replay = run_path_variant(
            evaluation_candidates,
            evaluation_candles,
            threshold_name=threshold_name,
            threshold=thresholds[threshold_name],
            quota=quota,
            cap=cap,
            retain_outputs=(threshold_name, quota, cap) == _selected_identity(path_best),
        )
        evaluation_rows.append(row)
        if (threshold_name, quota, cap) == _selected_identity(path_best):
            selected_replay = replay

    selected_evaluation = next(
        row for row in evaluation_rows if _selected_identity(row) == _selected_identity(path_best)
    )
    cost_stress = []
    for name, spread_multiplier, slippage in (
        ("base", 1.0, 0.2),
        ("spread_1p25x", 1.25, 0.2),
        ("spread_1p5x", 1.5, 0.2),
        ("slippage_plus_0p1", 1.0, 0.3),
        ("slippage_plus_0p3", 1.0, 0.5),
    ):
        if name == "base":
            stressed = selected_evaluation
        else:
            stressed, _ = run_path_variant(
                evaluation_candidates,
                evaluation_candles,
                threshold_name=path_best["threshold_name"],
                threshold=float(path_best["frozen_score_threshold"]),
                quota=int(path_best["max_new_per_timestamp"]),
                cap=int(path_best["max_open_positions"]),
                spread_multiplier=spread_multiplier,
                slippage_round_trip_pips=slippage,
            )
        cost_stress.append({
            "stress": name,
            "spread_multiplier": spread_multiplier,
            "slippage_round_trip_pips": slippage,
            "pnl_usd": stressed["pnl_usd"],
            "path_adjusted_pnl_usd": stressed["path_adjusted_pnl_usd"],
            "return_pct": stressed["return_pct"],
            "profit_factor": stressed["profit_factor"],
            "max_drawdown_pct": stressed["max_drawdown_pct"],
            "survives": float(stressed["pnl_usd"]) > 0.0 and float(stressed["path_adjusted_pnl_usd"]) > 0.0,
        })
    if selected_replay is not None:
        _write_replay_outputs(output_dir, selected_replay)

    exact_fixed_units_eval, _ = run_path_variant(
        evaluation_candidates,
        evaluation_candles,
        threshold_name="q99",
        threshold=thresholds["q99"],
        quota=8,
        cap=30,
        sizing_mode="fixed_units",
        fixed_units=10_000,
    )
    legacy_compounding_eval, _ = run_path_variant(
        evaluation_candidates,
        evaluation_candles,
        threshold_name="q99",
        threshold=thresholds["q99"],
        quota=8,
        cap=30,
        sizing_mode="legacy_compounding",
    )

    selected_quarters = {row["quarter"]: row for row in selected_evaluation["quarter_breakdown"]}
    q1_positive = float(selected_quarters.get("2026Q1", {}).get("pnl_usd", 0.0)) > 0.0
    later_positive = sum(
        float(row["pnl_usd"])
        for quarter, row in selected_quarters.items()
        if quarter != "2026Q1"
    ) > 0.0
    mild_cost_survival = all(
        row["survives"] for row in cost_stress if row["stress"] in {"base", "spread_1p25x", "slippage_plus_0p1"}
    )
    historical_checks = {
        "development_selected_path_config_viable": bool(path_best["viability"].get("viable", False)),
        "positive_2026_pnl": float(selected_evaluation["pnl_usd"]) > 0.0,
        "positive_2026_path_adjusted_pnl": float(selected_evaluation["path_adjusted_pnl_usd"]) > 0.0,
        "positive_2026_q1": q1_positive,
        "positive_2026_later_period": later_positive,
        "beats_no_trade": float(selected_evaluation["pnl_usd"]) > 0.0,
        "no_margin_calls": int(selected_evaluation["margin_call_rows"]) == 0,
        "mild_cost_stress_survives": mild_cost_survival,
        "selection_used_2025_only": True,
        "execution_disabled": True,
    }
    historical_verdict = "PASS" if all(historical_checks.values()) else "FAIL"
    canonical_checks = {
        **historical_checks,
        "fresh_untouched_post_selection_holdout": False,
    }
    canonical_verdict = "PASS" if all(canonical_checks.values()) else "FAIL"

    path_grid_frame = pd.DataFrame([
        {
            key: value
            for key, value in row.items()
            if key not in {"quarter_breakdown", "viability", "blocked_reason_counts"}
        }
        | {
            "positive_quarters": sum(float(item["pnl_usd"]) > 0.0 for item in row["quarter_breakdown"]),
            "viable": row["viability"].get("viable", False),
            "passed_checks": row["viability"].get("passed_check_count", 0),
        }
        for row in path_rows
    ])
    path_grid_frame.to_csv(output_dir / "HTF_ROTATION_PATH_REPLAY_GRID.csv", index=False)

    safety = {
        "research_only": True,
        "live_execution_enabled": False,
        "demo_execution_enabled": False,
        "oanda_execution_enabled": False,
        "broker_placement_enabled": False,
        "credentials_read": False,
        "account_files_read": False,
        "orders_placed": 0,
        "bots_started": 0,
        "margin_rate_source": "static research schedule",
    }
    report = {
        "verdict": canonical_verdict,
        "historical_validation_verdict": historical_verdict,
        "canonical_promotion_verdict": canonical_verdict,
        "historical_checks": historical_checks,
        "canonical_checks": canonical_checks,
        "interpretation": (
            "This validates the corrected H1 descendant under the archived rotation mechanics. "
            "It does not rehabilitate the original leaky M30/H1/H4 predictions."
        ),
        "data_manifest": data_manifest,
        "period_breakdown": {
            "development": _surface_period_breakdown(development),
            "evaluation": _surface_period_breakdown(evaluation),
            "evaluation_previously_inspected": True,
            "fresh_untouched_holdout": False,
        },
        "account_contract": {
            "initial_equity": INITIAL_EQUITY,
            "selection_sizing": "fixed initial-equity risk; no compounding position size",
            "risk_per_trade_pct": BASE_RISK_PCT,
            "target_margin_pct": TARGET_MARGIN_PCT,
            "hard_margin_pct": HARD_MARGIN_PCT,
            "target_open_risk_pct": TARGET_OPEN_RISK_PCT,
            "max_open_risk_pct": MAX_OPEN_RISK_PCT,
            "one_position_per_pair": True,
            "native_bid_ask": True,
            "entry_price": "first executable M1 open at corrected decision time",
            "same_bar_ambiguity": "adverse first",
            "base_round_trip_slippage_pips": 0.2,
        },
        "fixed_horizon_screen": {
            "declared_configs": int(len(fixed_rows)),
            "viable_configs": int(sum(row["viability"]["viable"] for row in fixed_rows)),
            "development_selected": fixed_best,
            "high_throughput_q99": [
                row for row in fixed_rows
                if row["score_quantile"] == 0.99
                and row["max_new_per_timestamp"] in {8, 12, 16}
                and row["max_open_positions"] in {8, 15, 30}
            ],
        },
        "path_rotation_selection": {
            "declared_configs": int(len(path_rows)),
            "viable_configs": int(len(path_viable_rows)),
            "development_selected_research_leader": path_best,
            "production_selection": path_best if path_viable_rows else {"strategy": "no_trade"},
            "selection_used_only_development": True,
            "evaluation_result": selected_evaluation,
            "evaluated_comparators": evaluation_rows,
        },
        "exact_archived_pacing_diagnostics": {
            "q99_8_new_30_cap_development": next(
                row for row in path_rows
                if row["threshold_name"] == "q99"
                and row["max_new_per_timestamp"] == 8
                and row["max_open_positions"] == 30
            ),
            "q99_8_new_30_cap_evaluation": next(
                row for row in evaluation_rows
                if row["threshold_name"] == "q99"
                and row["max_new_per_timestamp"] == 8
                and row["max_open_positions"] == 30
            ),
            "q99_12_new_30_cap_development": next(
                row for row in path_rows
                if row["threshold_name"] == "q99"
                and row["max_new_per_timestamp"] == 12
                and row["max_open_positions"] == 30
            ),
            "q99_12_new_30_cap_evaluation": next(
                row for row in evaluation_rows
                if row["threshold_name"] == "q99"
                and row["max_new_per_timestamp"] == 12
                and row["max_open_positions"] == 30
            ),
        },
        "account_sizing_diagnostics": {
            "fixed_10000_units_8_new_30_cap_development": exact_fixed_units_dev,
            "fixed_10000_units_8_new_30_cap_evaluation": exact_fixed_units_eval,
            "legacy_compounding_8_new_30_cap_development": legacy_compounding_dev,
            "legacy_compounding_8_new_30_cap_evaluation": legacy_compounding_eval,
            "legacy_compounding_eligible_for_selection": False,
        },
        "cost_stress": {
            "selected_identity": _selected_identity(path_best),
            "rows": cost_stress,
            "mild_stress_survives": mild_cost_survival,
        },
        "no_trade": {"pnl_usd": 0.0, "return_pct": 0.0, "trades": 0},
        "safety": safety,
        "limitations": [
            "2026 was already inspected before this allocator validation.",
            "Tier1 only; the original archive included a broader pair universe.",
            "The corrected rebuild selected H1; this is not a corrected M30/H1/H4 ensemble rebuild.",
            "Financing and weekend carry are not modeled.",
            "M1 bars use adverse-first ordering when both protective levels are reachable.",
        ],
    }
    final_summary = {
        "verdict": canonical_verdict,
        "historical_validation_verdict": historical_verdict,
        "canonical_promotion_verdict": canonical_verdict,
        "best_development_selected_rotation_config": {
            key: path_best[key]
            for key in (
                "threshold_name",
                "frozen_score_threshold",
                "max_new_per_timestamp",
                "max_open_positions",
                "sizing_mode",
            )
        },
        "development_result": {
            key: path_best.get(key)
            for key in (
                "pnl_usd",
                "return_pct",
                "opened_trades",
                "win_rate",
                "profit_factor",
                "max_drawdown_pct",
                "max_margin_used_pct",
                "max_open_positions_seen",
                "path_adjusted_pnl_usd",
            )
        },
        "evaluation_result": {
            key: selected_evaluation.get(key)
            for key in (
                "pnl_usd",
                "return_pct",
                "opened_trades",
                "win_rate",
                "profit_factor",
                "max_drawdown_pct",
                "max_margin_used_pct",
                "max_open_positions_seen",
                "path_adjusted_pnl_usd",
            )
        },
        "exact_8_new_30_cap_evaluation_pnl_usd": next(
            row["pnl_usd"] for row in evaluation_rows
            if row["threshold_name"] == "q99"
            and row["max_new_per_timestamp"] == 8
            and row["max_open_positions"] == 30
        ),
        "exact_12_new_30_cap_evaluation_pnl_usd": next(
            row["pnl_usd"] for row in evaluation_rows
            if row["threshold_name"] == "q99"
            and row["max_new_per_timestamp"] == 12
            and row["max_open_positions"] == 30
        ),
        "fixed_10000_units_feasible_on_1000_account": exact_fixed_units_eval["max_open_positions_seen"] >= 8,
        "mild_cost_stress_survives": mild_cost_survival,
        "fresh_untouched_holdout": False,
        "live_execution_disabled": True,
        "orders_placed": 0,
        "biggest_remaining_blocker": (
            "No untouched post-2026-07-07 data and no corrected multi-timeframe ensemble rebuild."
        ),
    }
    write_report_pair(output_dir, "HTF_ROTATION_VALIDATION_REPORT", report)
    write_report_pair(output_dir, "HTF_ROTATION_FINAL_SUMMARY", final_summary)
    write_report_pair(output_dir, "HTF_ROTATION_COST_STRESS", report["cost_stress"])
    write_report_pair(output_dir, "HTF_ROTATION_GATE_ATTRIBUTION", {
        "selected_development": path_best.get("blocked_reason_counts", {}),
        "selected_evaluation": selected_evaluation.get("blocked_reason_counts", {}),
        "exact_8_new_30_development": report["exact_archived_pacing_diagnostics"]["q99_8_new_30_cap_development"].get("blocked_reason_counts", {}),
        "exact_8_new_30_evaluation": report["exact_archived_pacing_diagnostics"]["q99_8_new_30_cap_evaluation"].get("blocked_reason_counts", {}),
    })
    (output_dir / "run_manifest.json").write_text(
        json.dumps({
            "mode": "htf-rotation-validation",
            "run_dir": str(output_dir),
            **safety,
            "reports": [
                "HTF_ROTATION_VALIDATION_REPORT.json",
                "HTF_ROTATION_FINAL_SUMMARY.json",
                "HTF_ROTATION_COST_STRESS.json",
                "HTF_ROTATION_GATE_ATTRIBUTION.json",
                "HTF_ROTATION_FIXED_HORIZON_GRID.csv",
                "HTF_ROTATION_PATH_REPLAY_GRID.csv",
            ],
        }, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_dir / "HTF_ROTATION_FINAL_SUMMARY.json"
