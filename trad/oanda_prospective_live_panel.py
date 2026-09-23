#!/usr/bin/env python3
"""Build a leakage-audited training panel from prospective live observations."""

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
from typing import Any

import pandas as pd

try:
    import oanda_shared_timeframe_horizon_panel as shared_panel
except ModuleNotFoundError:
    from trad import oanda_shared_timeframe_horizon_panel as shared_panel


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
DEFAULT_ARCHIVE = DATA / "prospective_live_model_features"
DEFAULT_LEDGER = DATA / "state" / "model_gap_live_forecasts_v1.sqlite"
DEFAULT_OUTPUT = (
    DATA
    / "training_sets"
    / "prospective_live"
    / "prospective_live_shared_panel_latest.parquet"
)
DEFAULT_REPORT = (
    DATA
    / "reports"
    / "modern_model_gap"
    / "prospective_live_panel_latest.json"
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


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
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_matured_events(ledger: Path, cutoff_epoch: float) -> tuple[pd.DataFrame, int]:
    if not ledger.is_file():
        return pd.DataFrame(), 0
    connection = sqlite3.connect(ledger)
    try:
        frame = pd.read_sql_query(
            """
            SELECT o.snapshot_id, o.instrument, o.input_timeframe,
                   o.horizon_sec, o.generated_epoch, o.target_epoch,
                   o.entry_bid, o.entry_ask, o.entry_spread_pips, o.pip,
                   o.observed_epoch, o.outcome_delay_sec,
                   o.signed_mid_move_pips, o.exit_spread_pips
            FROM outcomes o
            WHERE o.status = 'matured' AND o.generated_epoch >= ?
            ORDER BY o.generated_epoch, o.snapshot_id, o.instrument,
                     o.input_timeframe, o.horizon_sec
            """,
            connection,
            params=(cutoff_epoch,),
        )
    finally:
        connection.close()
    if frame.empty:
        return frame, 0
    keys = ["snapshot_id", "instrument", "input_timeframe", "horizon_sec"]
    spread = frame.groupby(keys, observed=True)["signed_mid_move_pips"].agg(
        lambda values: float(values.max() - values.min())
    )
    conflicting = int((spread > 1e-6).sum())
    frame = frame.drop_duplicates(keys, keep="first").reset_index(drop=True)
    return frame, conflicting


def read_archived_features(
    archive: Path,
    snapshot_ids: set[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    import pyarrow.parquet as pq

    parts: list[pd.DataFrame] = []
    files = sorted(archive.rglob("features_*.parquet")) if archive.is_dir() else []
    failures: list[str] = []
    for path in files:
        try:
            frame = pq.ParquetFile(path).read().to_pandas()
        except Exception as exc:
            failures.append(f"{path.name}:{type(exc).__name__}:{exc}")
            continue
        if "snapshot_id" not in frame:
            continue
        frame = frame[frame["snapshot_id"].astype(str).isin(snapshot_ids)]
        if not frame.empty:
            parts.append(frame)
    output = pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame()
    return output, {
        "files_scanned": len(files),
        "files_matched": len(parts),
        "read_failures": failures[:100],
    }


def _timestamp(value: Any) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed)


def build_panel(
    outcomes: pd.DataFrame,
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if outcomes.empty or features.empty:
        return pd.DataFrame(), {"joined_events": 0, "missing_feature_events": len(outcomes)}
    keys = ["snapshot_id", "instrument", "input_timeframe"]
    features = features.drop_duplicates(keys, keep="last")
    duplicate_columns = [
        column
        for column in ("generated_epoch", "bid", "ask", "pip")
        if column in features
    ]
    feature_values = features.drop(columns=duplicate_columns, errors="ignore")
    merged = outcomes.merge(feature_values, on=keys, how="left", indicator=True)
    missing = int((merged["_merge"] != "both").sum())
    merged = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
    rows: list[dict[str, Any]] = []
    rejected_origin = 0
    scalar_names = tuple(
        dict.fromkeys(
            (
                *shared_panel.BASE_SCALAR_FEATURES,
                *shared_panel.CROSS_PAIR_FEATURES,
                *shared_panel.OPTIONAL_MICROSTRUCTURE_FEATURES,
            )
        )
    )
    for raw in merged.to_dict(orient="records"):
        generated_epoch = finite(raw.get("generated_epoch"))
        prediction_time = pd.Timestamp(generated_epoch, unit="s", tz="UTC")
        feature_origin = _timestamp(raw.get("feature_origin_utc"))
        if feature_origin is None:
            feature_origin = prediction_time
        if feature_origin > prediction_time:
            rejected_origin += 1
            continue
        horizon = int(finite(raw.get("horizon_sec")))
        pip = max(1e-12, finite(raw.get("pip"), 0.0001))
        entry_spread = max(0.0, finite(raw.get("entry_spread_pips")))
        exit_spread = max(0.0, finite(raw.get("exit_spread_pips")))
        mid_move = finite(raw.get("signed_mid_move_pips"))
        round_trip_cost = 0.5 * (entry_spread + exit_spread)
        long_net = mid_move - round_trip_cost
        short_net = -mid_move - round_trip_cost
        timeframe = str(raw.get("input_timeframe") or "").upper()
        timeframe_seconds = int(finite(raw.get("input_timeframe_seconds")))
        if timeframe_seconds <= 0:
            timeframe_seconds = {
                "S5": 5,
                "S10": 10,
                "S15": 15,
                "S30": 30,
                "M1": 60,
                "M5": 300,
                "M10": 600,
                "M15": 900,
                "M30": 1800,
                "H1": 3600,
                "H2": 7200,
                "H3": 10800,
                "H4": 14400,
            }.get(timeframe, 0)
        if horizon <= 0 or timeframe_seconds <= 0 or pip <= 0.0:
            continue
        hour = (
            prediction_time.hour
            + prediction_time.minute / 60.0
            + prediction_time.second / 3600.0
        )
        weekday = prediction_time.weekday()
        event_id = (
            f"prospective_live_v1|{raw['snapshot_id']}|{raw['instrument']}|"
            f"{timeframe}|{horizon}"
        )
        common = {
            "event_id": event_id,
            "decision_candle_utc": feature_origin,
            "prediction_time_utc": prediction_time,
            "maturity_time_utc": prediction_time + pd.to_timedelta(horizon, unit="s"),
            "max_feature_origin_utc": feature_origin,
            "instrument": str(raw.get("instrument") or ""),
            "input_timeframe": timeframe,
            "input_timeframe_seconds": timeframe_seconds,
            "horizon_sec": horizon,
            "source": "prospective_live",
            "spread_mode": "observed_oanda_stream_bid_ask",
            "feature_version": shared_panel.FEATURE_VERSION,
            "entry_spread_pips": entry_spread,
            "market_mid_move_pips": mid_move,
            "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
            "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
            "weekday_sin": math.sin(2.0 * math.pi * weekday / 7.0),
            "weekday_cos": math.cos(2.0 * math.pi * weekday / 7.0),
            **{name: finite(raw.get(name)) for name in scalar_names},
        }
        for direction, sign, own, opposite, best in (
            ("LONG", 1.0, long_net, short_net, long_net >= short_net),
            ("SHORT", -1.0, short_net, long_net, short_net > long_net),
        ):
            rows.append(
                {
                    **common,
                    "side_id": f"{event_id}|{direction}",
                    "direction": direction,
                    "side_sign": sign,
                    "realized_net_pips": own,
                    "opposite_net_pips": opposite,
                    "realized_edge_pips": own - opposite,
                    "realized_directional_move_pips": sign * mid_move,
                    shared_panel.TARGET_COLUMN: int(own > 0.0),
                    "target_best_side": int(best),
                }
            )
    frame = pd.DataFrame(rows)
    if not frame.empty:
        shared_panel.validate_panel(frame)
    return frame, {
        "joined_events": int(len(frame) // 2),
        "missing_feature_events": missing,
        "rejected_feature_origin_events": rejected_origin,
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
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--min-events", type=int, default=5000)
    parser.add_argument(
        "--validate-existing",
        action="store_true",
        help=(
            "Validate the existing report/parquet contract without rebuilding "
            "the live panel or scanning the feature archive."
        ),
    )
    args = parser.parse_args(argv)
    if args.window_days <= 0 or args.min_events <= 0:
        raise SystemExit("window-days and min-events must be positive")
    return args


def validate_existing(args: argparse.Namespace) -> dict[str, Any]:
    """Perform a bounded integrity audit suitable for the live control loop."""

    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "schema_version": 1,
            "generated_utc": utc_iso(),
            "status": "invalid",
            "reason": f"report_unreadable:{type(exc).__name__}",
        }
    artifact = report.get("artifact") or {}
    contract = report.get("contract") or {}
    expected_rows = int(artifact.get("rows") or 0)
    expected_bytes = int(artifact.get("bytes") or 0)
    output = Path(artifact.get("path") or args.output)
    reasons: list[str] = []
    if report.get("status") != "ready":
        reasons.append("report_not_ready")
    if int(artifact.get("events") or 0) < args.min_events:
        reasons.append("insufficient_events")
    if not output.is_file():
        reasons.append("artifact_missing")
        actual_bytes = 0
        actual_rows = 0
    else:
        actual_bytes = output.stat().st_size
        try:
            import pyarrow.parquet as pq

            actual_rows = int(pq.ParquetFile(output).metadata.num_rows)
        except Exception as exc:
            actual_rows = 0
            reasons.append(f"parquet_unreadable:{type(exc).__name__}")
    if expected_bytes <= 0 or actual_bytes != expected_bytes:
        reasons.append("artifact_size_mismatch")
    if expected_rows <= 0 or actual_rows != expected_rows:
        reasons.append("artifact_row_mismatch")
    for name in (
        "features_are_point_in_time_archives",
        "outcomes_are_later_executable_bid_ask_observations",
        "duplicate_model_predictions_are_one_market_event",
        "long_and_short_targets_include_entry_and_exit_spread",
    ):
        if contract.get(name) is not True:
            reasons.append(f"contract_missing:{name}")
    if contract.get("account_execution_authorized") is not False:
        reasons.append("account_execution_contract_changed")
    try:
        report_epoch = datetime.fromisoformat(
            str(report.get("generated_utc") or "").replace("Z", "+00:00")
        ).timestamp()
    except ValueError:
        report_epoch = 0.0
    report_age_hours = (
        None if report_epoch <= 0.0 else max(0.0, (time.time() - report_epoch) / 3600.0)
    )
    return {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "status": "valid" if not reasons else "invalid",
        "reasons": reasons,
        "mode": "existing_artifact_integrity",
        "artifact": {
            "path": str(output.resolve()),
            "bytes": actual_bytes,
            "rows": actual_rows,
            "events": int(artifact.get("events") or 0),
            "sha256": artifact.get("sha256"),
        },
        "report_generated_utc": report.get("generated_utc"),
        "report_age_hours": (
            None if report_age_hours is None else round(report_age_hours, 3)
        ),
        "full_rebuild_mode": "manual_or_off_market",
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    cutoff = time.time() - args.window_days * 86400.0
    outcomes, conflicts = read_matured_events(args.ledger, cutoff)
    features, archive_summary = read_archived_features(
        args.archive,
        set(outcomes.get("snapshot_id", pd.Series(dtype=str)).astype(str)),
    )
    frame, join_summary = build_panel(outcomes, features)
    events = int(len(frame) // 2)
    status = "waiting_for_data"
    artifact: dict[str, Any] = {}
    if events:
        write_panel(frame, args.output)
        artifact = {
            "path": str(args.output.resolve()),
            "bytes": args.output.stat().st_size,
            "sha256": sha256_file(args.output),
            "rows": len(frame),
            "events": events,
            "timeframes": sorted(frame["input_timeframe"].unique().tolist()),
            "horizons_sec": sorted(int(value) for value in frame["horizon_sec"].unique()),
            "instruments": sorted(frame["instrument"].unique().tolist()),
            "start_utc": frame["prediction_time_utc"].min().isoformat(),
            "end_utc": frame["prediction_time_utc"].max().isoformat(),
        }
        status = "ready" if events >= args.min_events else "insufficient_events"
    report = {
        "schema_version": 1,
        "generated_utc": utc_iso(),
        "status": status,
        "minimum_events": args.min_events,
        "matured_event_rows": int(len(outcomes)),
        "conflicting_duplicate_outcomes": conflicts,
        "feature_archive": archive_summary,
        "join": join_summary,
        "artifact": artifact,
        "contract": {
            "features_are_point_in_time_archives": True,
            "outcomes_are_later_executable_bid_ask_observations": True,
            "duplicate_model_predictions_are_one_market_event": True,
            "long_and_short_targets_include_entry_and_exit_spread": True,
            "account_execution_authorized": False,
        },
    }
    atomic_json(args.report, report)
    return report


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = validate_existing(args) if args.validate_existing else run(args)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"ready", "valid"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
