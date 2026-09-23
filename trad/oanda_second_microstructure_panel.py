#!/usr/bin/env python3
"""Build a causal shared panel from live second-forecast feature snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    import oanda_shared_timeframe_horizon_panel as shared_panel
except ModuleNotFoundError:
    from trad import oanda_shared_timeframe_horizon_panel as shared_panel


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_DATABASES = (
    DATA / "state" / "second_forecast_microstructure_v1.sqlite",
    DATA / "state" / "second_forecast_live_v1.sqlite",
)
DEFAULT_OUTPUT = (
    DATA
    / "training_sets"
    / "prospective_second_microstructure"
    / "second_microstructure_panel_latest.parquet"
)
DEFAULT_REPORT = (
    DATA
    / "reports"
    / "modern_model_gap"
    / "second_microstructure_panel_latest.json"
)
FEATURE_VERSION = "prospective_second_quote_flow_v1"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def finite_or_nan(value: Any) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return math.nan
    return output if math.isfinite(output) else math.nan


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_database(
    path: Path,
    cutoff_epoch: float,
    instruments: Iterable[str] = (),
    horizons: Iterable[int] = (),
    immutable_read: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path.is_file():
        return pd.DataFrame(), {
            "path": str(path.resolve()),
            "status": "missing",
            "rows": 0,
        }
    immutable_query = "&immutable=1" if immutable_read else ""
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro{immutable_query}",
        uri=True,
        timeout=10.0,
    )
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not {"forecasts", "feature_snapshots"}.issubset(tables):
            return pd.DataFrame(), {
                "path": str(path.resolve()),
                "status": "schema_missing",
                "rows": 0,
            }
        forecast_indexes = {
            str(row[1])
            for row in connection.execute("PRAGMA index_list(forecasts)")
        }
        origin_index_clause = (
            " INDEXED BY second_forecast_origin"
            if "second_forecast_origin" in forecast_indexes
            else ""
        )
        requested_instruments = tuple(sorted(set(str(value) for value in instruments)))
        requested_horizons = tuple(sorted(set(int(value) for value in horizons)))
        filters = [
            "f.status = 'matured'",
            "f.actual_signed_pips IS NOT NULL",
            "f.chosen_theoretical_pips IS NOT NULL",
            "f.origin_epoch >= ?",
        ]
        params: list[Any] = [int(cutoff_epoch)]
        if requested_instruments:
            filters.append(
                "f.instrument IN ("
                + ",".join("?" for _ in requested_instruments)
                + ")"
            )
            params.extend(requested_instruments)
        if requested_horizons:
            filters.append(
                "f.horizon_sec IN ("
                + ",".join("?" for _ in requested_horizons)
                + ")"
            )
            params.extend(requested_horizons)
        where_clause = " AND ".join(filters)
        frame = pd.read_sql_query(
            f"""
            SELECT f.origin_epoch, f.target_epoch, f.instrument, f.horizon_sec,
                   f.model_id, f.model_family, f.input_timeframe,
                   f.training_timeframe, f.predicted_signed_pips,
                   f.entry_mid, f.spread_pips, f.pip,
                   f.actual_signed_pips, f.chosen_theoretical_pips,
                   f.outcome_delay_sec, s.features_json
            FROM forecasts AS f{origin_index_clause}
            JOIN feature_snapshots s
              ON s.origin_epoch = f.origin_epoch
             AND s.instrument = f.instrument
            WHERE {where_clause}
            """,
            connection,
            params=params,
        )
    finally:
        connection.close()
    frame["source_database"] = str(path.resolve())
    return frame, {
        "path": str(path.resolve()),
        "status": "read",
        "rows": int(len(frame)),
        "query_index": (
            "second_forecast_origin" if origin_index_clause else "sqlite_planner"
        ),
        "immutable_read": bool(immutable_read),
    }


def read_databases(
    paths: Iterable[Path],
    cutoff_epoch: float,
    instruments: Iterable[str] = (),
    horizons: Iterable[int] = (),
    immutable_read: bool = False,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    parts: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    for path in paths:
        frame, summary = read_database(
            Path(path),
            cutoff_epoch,
            instruments=instruments,
            horizons=horizons,
            immutable_read=immutable_read,
        )
        summaries.append(summary)
        if not frame.empty:
            parts.append(frame)
    if not parts:
        return pd.DataFrame(), summaries
    return pd.concat(parts, ignore_index=True, sort=False), summaries


def _feature_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or ""))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def has_required_features(value: Any, names: Iterable[str]) -> bool:
    features = _feature_payload(value)
    return all(
        math.isfinite(finite_or_nan(features.get(str(name))))
        for name in names
    )


def _calendar(timestamp: pd.Timestamp) -> dict[str, float]:
    hour = timestamp.hour + timestamp.minute / 60.0 + timestamp.second / 3600.0
    weekday = timestamp.weekday()
    return {
        "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
        "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
        "weekday_sin": math.sin(2.0 * math.pi * weekday / 7.0),
        "weekday_cos": math.cos(2.0 * math.pi * weekday / 7.0),
    }


def build_panel(records: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if records.empty:
        return pd.DataFrame(), {
            "source_rows": 0,
            "events": 0,
            "duplicate_rows": 0,
            "conflicting_outcomes": 0,
            "invalid_cost_rows": 0,
        }
    keys = ["origin_epoch", "instrument", "horizon_sec"]
    records = records.copy()
    if {
        "model_rows_per_event",
        "actual_signed_pips_range",
    }.issubset(records.columns):
        model_rows = pd.to_numeric(
            records["model_rows_per_event"], errors="coerce"
        ).fillna(1).clip(lower=1)
        actual_range = pd.to_numeric(
            records["actual_signed_pips_range"], errors="coerce"
        ).fillna(0.0)
        source_rows = int(model_rows.sum())
        duplicate_rows = int((model_rows - 1).sum())
        conflicting = int((actual_range > 1e-6).sum())
        records = records.drop(
            columns=["model_rows_per_event", "actual_signed_pips_range"]
        )
    else:
        grouped = records.groupby(keys, observed=True)["actual_signed_pips"]
        conflicting = int(
            (
                grouped.agg(
                    lambda values: float(values.max() - values.min())
                )
                > 1e-6
            ).sum()
        )
        source_rows = len(records)
        if "outcome_delay_sec" in records:
            delay = pd.to_numeric(records["outcome_delay_sec"], errors="coerce")
            records["_outcome_delay_sort"] = delay.fillna(math.inf)
        else:
            records["_outcome_delay_sort"] = math.inf
        sort_columns = [*keys, "_outcome_delay_sort"]
        if "model_id" in records:
            sort_columns.append("model_id")
        records = (
            records.sort_values(sort_columns)
            .drop_duplicates(keys, keep="first")
            .drop(columns=["_outcome_delay_sort"])
            .reset_index(drop=True)
        )
        duplicate_rows = source_rows - len(records)
    rows: list[dict[str, Any]] = []
    invalid_cost_rows = 0
    for raw in records.to_dict(orient="records"):
        origin_epoch = int(finite(raw.get("origin_epoch")))
        horizon = int(finite(raw.get("horizon_sec")))
        if origin_epoch <= 0 or horizon <= 0:
            continue
        actual_signed = finite(raw.get("actual_signed_pips"))
        predicted_signed = finite(raw.get("predicted_signed_pips"))
        predicted_side = 1.0 if predicted_signed >= 0.0 else -1.0
        chosen_net = finite(raw.get("chosen_theoretical_pips"))
        inferred_round_trip_cost = predicted_side * actual_signed - chosen_net
        if inferred_round_trip_cost < -1e-6:
            invalid_cost_rows += 1
            continue
        inferred_round_trip_cost = max(0.0, inferred_round_trip_cost)
        long_net = actual_signed - inferred_round_trip_cost
        short_net = -actual_signed - inferred_round_trip_cost
        prediction_time = pd.Timestamp(origin_epoch, unit="s", tz="UTC")
        features = _feature_payload(raw.get("features_json"))
        spread = max(0.0, finite(raw.get("spread_pips")))
        spread_ratio = finite_or_nan(features.get("spread_ratio_60"))
        median_spread = (
            spread / spread_ratio
            if math.isfinite(spread_ratio) and spread_ratio > 0.0
            else math.nan
        )
        base_features = {
            "volume_ratio_12": finite_or_nan(features.get("activity_ratio_30_60")),
            "volume_ratio_30": finite_or_nan(features.get("activity_ratio_30_60")),
            "current_volume": finite_or_nan(features.get("activity_30")),
            "current_candle_spread_pips": finite_or_nan(
                features.get("spread_pips")
            ),
            "median_spread_12_pips": median_spread,
            "spread_ratio_12": spread_ratio,
            "spread_drop_3_pips": math.nan,
            "last": finite_or_nan(raw.get("entry_mid")),
            "r1_pips": finite_or_nan(features.get("return_5_pips")),
            "r3_pips": finite_or_nan(features.get("return_30_pips")),
            "r5_pips": finite_or_nan(features.get("return_60_pips")),
            "m5_r1_pips": math.nan,
            "m5_r3_pips": math.nan,
            "pos20": math.nan,
            "m1_atr14_pips": finite_or_nan(features.get("volatility_60_pips")),
            "m5_atr14_pips": math.nan,
        }
        cross_pair_features = {
            name: finite_or_nan(features.get(name))
            for name in shared_panel.CROSS_PAIR_FEATURES
        }
        microstructure = {
            name: finite_or_nan(features.get(name))
            for name in shared_panel.OPTIONAL_MICROSTRUCTURE_FEATURES
        }
        microstructure["live_spread_pips"] = finite_or_nan(
            features.get("spread_pips")
        )
        event_id = (
            f"{FEATURE_VERSION}|{origin_epoch}|{raw.get('instrument')}|{horizon}"
        )
        common = {
            "event_id": event_id,
            "decision_candle_utc": prediction_time,
            "prediction_time_utc": prediction_time,
            "maturity_time_utc": prediction_time + pd.to_timedelta(horizon, unit="s"),
            "max_feature_origin_utc": prediction_time,
            "instrument": str(raw.get("instrument") or ""),
            "input_timeframe": "S1",
            "input_timeframe_seconds": 1,
            "horizon_sec": horizon,
            "source": "prospective_second_microstructure",
            "spread_mode": "observed_oanda_stream_bid_ask",
            "feature_version": FEATURE_VERSION,
            "entry_spread_pips": spread,
            "inferred_round_trip_cost_pips": inferred_round_trip_cost,
            "market_mid_move_pips": actual_signed,
            "outcome_delay_sec": finite(raw.get("outcome_delay_sec")),
            **_calendar(prediction_time),
            **base_features,
            **cross_pair_features,
            **microstructure,
        }
        for direction, side_sign, own, opposite, best in (
            ("LONG", 1.0, long_net, short_net, long_net >= short_net),
            ("SHORT", -1.0, short_net, long_net, short_net > long_net),
        ):
            rows.append(
                {
                    **common,
                    "side_id": f"{event_id}|{direction}",
                    "direction": direction,
                    "side_sign": side_sign,
                    "realized_net_pips": own,
                    "opposite_net_pips": opposite,
                    "realized_edge_pips": own - opposite,
                    "realized_directional_move_pips": side_sign * actual_signed,
                    shared_panel.TARGET_COLUMN: int(own > 0.0),
                    "target_best_side": int(best),
                }
            )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.sort_values(
            ["prediction_time_utc", "instrument", "horizon_sec", "direction"]
        ).reset_index(drop=True)
        shared_panel.validate_panel(frame)
    return frame, {
        "source_rows": int(source_rows),
        "events": int(len(frame) // 2),
        "duplicate_rows": int(duplicate_rows),
        "conflicting_outcomes": conflicting,
        "invalid_cost_rows": invalid_cost_rows,
    }


def write_panel(frame: pd.DataFrame, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + f".{os.getpid()}.tmp")
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        action="append",
        dest="databases",
        default=None,
        help="Repeat for each second-forecast SQLite source.",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--window-days", type=int, default=2)
    parser.add_argument(
        "--minimum-origin-epoch",
        type=float,
        default=0.0,
        help="Optional lower origin bound applied inside each SQLite query.",
    )
    parser.add_argument("--min-events", type=int, default=5000)
    parser.add_argument(
        "--immutable-read",
        action="store_true",
        help="Read a known-static historical database without WAL coordination.",
    )
    parser.add_argument(
        "--instruments",
        default="",
        help="Optional comma-separated instrument allowlist.",
    )
    parser.add_argument(
        "--horizons",
        default="",
        help="Optional comma-separated horizon-second allowlist applied in SQLite.",
    )
    parser.add_argument(
        "--require-feature",
        action="append",
        dest="required_features",
        default=[],
        help="Repeat to keep only rows containing each named causal feature.",
    )
    args = parser.parse_args(argv)
    args.databases = args.databases or list(DEFAULT_DATABASES)
    if args.window_days <= 0 or args.min_events <= 0:
        raise SystemExit("window-days and min-events must be positive")
    return args


def run(args: argparse.Namespace) -> dict[str, Any]:
    cutoff = max(
        time.time() - args.window_days * 86400.0,
        float(args.minimum_origin_epoch or 0.0),
    )
    requested_instruments = {
        value.strip().upper()
        for value in str(args.instruments or "").split(",")
        if value.strip()
    }
    requested_horizons = {
        int(value.strip())
        for value in str(args.horizons or "").split(",")
        if value.strip()
    }
    records, database_summaries = read_databases(
        args.databases,
        cutoff,
        instruments=requested_instruments,
        horizons=requested_horizons,
        immutable_read=bool(args.immutable_read),
    )
    if requested_instruments and not records.empty:
        records = records[
            records["instrument"].astype(str).isin(requested_instruments)
        ].reset_index(drop=True)
    required_features = tuple(
        dict.fromkeys(
            str(value).strip()
            for value in args.required_features
            if str(value).strip()
        )
    )
    rows_before_required_feature_filter = int(len(records))
    if required_features and not records.empty:
        records = records[
            records["features_json"].map(
                lambda value: has_required_features(value, required_features)
            )
        ].reset_index(drop=True)
    required_feature_rows_skipped = (
        rows_before_required_feature_filter - len(records)
    )
    frame, panel_summary = build_panel(records)
    events = int(len(frame) // 2)
    artifact: dict[str, Any] = {}
    status = "waiting_for_data"
    if events:
        write_panel(frame, args.output)
        artifact = {
            "path": str(args.output.resolve()),
            "bytes": args.output.stat().st_size,
            "sha256": sha256_file(args.output),
            **shared_panel.validate_panel(frame),
        }
        status = "ready" if events >= args.min_events else "insufficient_events"
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "status": status,
        "minimum_events": args.min_events,
        "minimum_origin_epoch": float(cutoff),
        "instrument_filter": sorted(requested_instruments),
        "horizon_filter": sorted(requested_horizons),
        "immutable_read": bool(args.immutable_read),
        "required_features": list(required_features),
        "required_feature_rows_skipped": int(required_feature_rows_skipped),
        "databases": database_summaries,
        "panel": panel_summary,
        "artifact": artifact,
        "contract": {
            "features_are_strictly_available_at_origin": True,
            "outcomes_are_later_stream_quotes": True,
            "long_and_short_targets_include_inferred_round_trip_cost": True,
            "duplicate_model_predictions_are_one_market_event": True,
            "chronological_validation_required_before_model_activation": True,
            "account_execution_authorized": False,
        },
    }
    atomic_json(args.report, report)
    return report


def main(argv: list[str] | None = None) -> int:
    print(json.dumps(run(parse_args(argv)), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
