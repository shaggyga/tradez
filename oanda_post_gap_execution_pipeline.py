#!/usr/bin/env python3
"""Audit economics, score signal cells, and freeze the practice-007 policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

try:
    from oanda_statistical_validation import benjamini_hochberg, normal_cdf
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_statistical_validation import benjamini_hochberg, normal_cdf

try:
    from oanda_shared_timeframe_horizon_panel import (
        CANONICAL_HORIZONS_SEC,
        CANONICAL_TIMEFRAMES,
    )
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_shared_timeframe_horizon_panel import (
        CANONICAL_HORIZONS_SEC,
        CANONICAL_TIMEFRAMES,
    )


EXPECTED_MATRIX_CELLS = len(CANONICAL_TIMEFRAMES) * len(CANONICAL_HORIZONS_SEC)


DEFAULT_ROOT = Path(__file__).resolve().parents[1]
MANAGER_RELATIVE = Path("trad/data/oanda_training_manager")
REPORT_DIR_RELATIVE = MANAGER_RELATIVE / "reports/modern_model_gap"
STATE_DIR_RELATIVE = MANAGER_RELATIVE / "state"
MARKET_REPORT_NAME = "remaining_market_validation_latest.json"
ECONOMICS_NAME = "post_gap_economics_audit_latest.json"
LEADERBOARD_NAME = "post_gap_signal_cell_leaderboard_latest.json"
FEED_NAME = "post_gap_feed_realism_latest.json"
EVALUATION_NAME = "post_gap_frozen_evaluation_latest.json"
PIPELINE_NAME = "post_gap_execution_pipeline_latest.json"
INCUMBENT_NAME = "practice_007_execution_policy_v1.json"
CHALLENGER_NAME = "practice_007_execution_policy_challenger_v1.json"

PANEL_COLUMNS = (
    "instrument",
    "direction",
    "input_timeframe",
    "horizon_sec",
    "entry_spread_pips",
    "market_mid_move_pips",
    "realized_net_pips",
    "opposite_net_pips",
    "current_volume",
    "volume_ratio_12",
    "volume_ratio_30",
)
IDENTITY_TOLERANCE_PIPS = 0.001


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


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


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _empty_aggregate() -> dict[str, float]:
    return defaultdict(float)


def _merge_group_aggregates(
    target: dict[tuple[str, str, int], dict[str, float]],
    grouped: pd.DataFrame,
) -> None:
    for key, row in grouped.iterrows():
        normalized = (str(key[0]), str(key[1]), int(key[2]))
        bucket = target.setdefault(normalized, _empty_aggregate())
        for column, value in row.items():
            bucket[str(column)] += finite(value)


def _derived_panel_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame[frame["direction"].astype(str).str.upper() == "LONG"].copy()
    numeric = PANEL_COLUMNS[4:]
    for column in numeric:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(
        subset=[
            "entry_spread_pips",
            "market_mid_move_pips",
            "realized_net_pips",
            "opposite_net_pips",
        ]
    )
    long_net = frame["realized_net_pips"]
    short_net = frame["opposite_net_pips"]
    mid_move = frame["market_mid_move_pips"]
    frame["inferred_exit_spread_pips"] = -(
        long_net + short_net
    ) - frame["entry_spread_pips"]
    frame["identity_error_pips"] = ((long_net - short_net) / 2.0 - mid_move).abs()
    frame["identity_violation"] = (
        frame["identity_error_pips"] > IDENTITY_TOLERANCE_PIPS
    ).astype(float)
    frame["invalid_exit_spread"] = (
        frame["inferred_exit_spread_pips"] < -1e-5
    ).astype(float)
    frame["abs_mid_move_pips"] = mid_move.abs()
    frame["correct_side_net_pips"] = np.where(mid_move >= 0.0, long_net, short_net)
    frame["wrong_side_net_pips"] = np.where(mid_move >= 0.0, short_net, long_net)
    frame["oracle_net_pips"] = np.maximum(long_net, short_net)
    frame["volume_present"] = frame["current_volume"].notna().astype(float)
    frame["volume_positive"] = (frame["current_volume"].fillna(0.0) > 0.0).astype(float)
    frame["volume_ratio_present"] = (
        frame[["volume_ratio_12", "volume_ratio_30"]].notna().all(axis=1)
    ).astype(float)
    frame["n"] = 1.0
    return frame


AGGREGATE_COLUMNS = (
    "n",
    "entry_spread_pips",
    "inferred_exit_spread_pips",
    "identity_error_pips",
    "identity_violation",
    "invalid_exit_spread",
    "abs_mid_move_pips",
    "correct_side_net_pips",
    "wrong_side_net_pips",
    "oracle_net_pips",
    "realized_net_pips",
    "opposite_net_pips",
    "volume_present",
    "volume_positive",
    "volume_ratio_present",
)


def _finalize_economic_row(
    key: tuple[str, str, int],
    aggregate: dict[str, float],
) -> dict[str, Any]:
    count = max(1.0, aggregate["n"])
    correct = aggregate["correct_side_net_pips"] / count
    wrong = aggregate["wrong_side_net_pips"] / count
    denominator = correct - wrong
    break_even = -wrong / denominator if denominator > 1e-12 else math.inf
    return {
        "instrument": key[0],
        "input_timeframe": key[1],
        "horizon_sec": key[2],
        "n": int(aggregate["n"]),
        "mean_entry_spread_pips": round(aggregate["entry_spread_pips"] / count, 6),
        "mean_inferred_exit_spread_pips": round(
            aggregate["inferred_exit_spread_pips"] / count,
            6,
        ),
        "mean_round_trip_cost_pips": round(
            (
                aggregate["entry_spread_pips"]
                + aggregate["inferred_exit_spread_pips"]
            )
            / count,
            6,
        ),
        "mean_absolute_mid_move_pips": round(
            aggregate["abs_mid_move_pips"] / count,
            6,
        ),
        "mean_correct_direction_net_pips": round(correct, 6),
        "mean_wrong_direction_net_pips": round(wrong, 6),
        "mean_oracle_net_pips": round(aggregate["oracle_net_pips"] / count, 6),
        "mean_unconditional_long_net_pips": round(
            aggregate["realized_net_pips"] / count,
            6,
        ),
        "mean_unconditional_short_net_pips": round(
            aggregate["opposite_net_pips"] / count,
            6,
        ),
        "minimum_direction_accuracy_to_break_even": (
            None if not math.isfinite(break_even) else round(break_even, 8)
        ),
        "mean_pricing_identity_error_pips": round(
            aggregate["identity_error_pips"] / count,
            6,
        ),
        "pricing_identity_violation_count": int(aggregate["identity_violation"]),
        "invalid_inferred_exit_spread_count": int(aggregate["invalid_exit_spread"]),
        "volume_present_pct": round(100.0 * aggregate["volume_present"] / count, 4),
        "volume_positive_pct": round(100.0 * aggregate["volume_positive"] / count, 4),
        "volume_ratio_present_pct": round(
            100.0 * aggregate["volume_ratio_present"] / count,
            4,
        ),
    }


def audit_panel_economics(
    market_report_path: Path,
    *,
    batch_size: int = 131072,
    max_source_rows: int = 0,
) -> dict[str, Any]:
    market_report = read_json(market_report_path)
    sources = (market_report.get("dataset") or {}).get("sources") or []
    aggregates: dict[tuple[str, str, int], dict[str, float]] = {}
    source_rows: list[dict[str, Any]] = []
    total_long_rows = 0
    for source_row in sources:
        path = Path(str(source_row.get("path") or ""))
        if not path.is_file():
            source_rows.append({"path": str(path), "status": "missing"})
            continue
        parquet = pq.ParquetFile(path)
        read_rows = 0
        for batch in parquet.iter_batches(
            columns=list(PANEL_COLUMNS),
            batch_size=max(1024, int(batch_size)),
        ):
            frame = _derived_panel_frame(batch.to_pandas())
            if max_source_rows > 0:
                remaining = max_source_rows - read_rows
                if remaining <= 0:
                    break
                frame = frame.head(remaining)
            if frame.empty:
                continue
            read_rows += len(frame)
            total_long_rows += len(frame)
            grouped = frame.groupby(
                ["instrument", "input_timeframe", "horizon_sec"],
                observed=True,
            )[list(AGGREGATE_COLUMNS)].sum()
            _merge_group_aggregates(aggregates, grouped)
            if max_source_rows > 0 and read_rows >= max_source_rows:
                break
        source_rows.append(
            {
                "path": str(path.resolve()),
                "status": "audited",
                "source_rows": parquet.metadata.num_rows,
                "long_rows_audited": read_rows,
                "bytes": path.stat().st_size,
                "sha256": str(source_row.get("sha256") or ""),
            }
        )
    cells = [
        _finalize_economic_row(key, aggregates[key])
        for key in sorted(aggregates)
    ]
    total_identity_violations = sum(
        row["pricing_identity_violation_count"] for row in cells
    )
    invalid_exit_spreads = sum(
        row["invalid_inferred_exit_spread_count"] for row in cells
    )
    pair_count = len({row["instrument"] for row in cells})
    matrix_cells = len(
        {(row["input_timeframe"], row["horizon_sec"]) for row in cells}
    )
    validation_passed = bool(
        total_long_rows > 0
        and pair_count == 68
        and matrix_cells == EXPECTED_MATRIX_CELLS
        and total_identity_violations == 0
        and invalid_exit_spreads == 0
    )
    return {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "market_report": str(market_report_path.resolve()),
        "market_report_sha256": sha256_file(market_report_path),
        "status": "passed" if validation_passed else "failed",
        "validation_passed": validation_passed,
        "scope": {
            "long_rows_audited": total_long_rows,
            "instruments": pair_count,
            "timeframe_horizon_cells": matrix_cells,
            "pair_timeframe_horizon_cells": len(cells),
            "source_files": len(source_rows),
            "max_source_rows_per_file": max_source_rows or None,
        },
        "unit_contract": {
            "pip_size": "0.01 for JPY quote pairs; 0.0001 otherwise",
            "long_net": "exit_bid - entry_ask, divided by pip size",
            "short_net": "entry_bid - exit_ask, divided by pip size",
            "mid_move_identity": "(long_net - short_net) / 2",
            "identity_tolerance_pips": IDENTITY_TOLERANCE_PIPS,
            "identity_tolerance_reason": (
                "source executable-return fields are persisted to 0.001 pip"
            ),
            "inferred_exit_spread": (
                "-(long_net + short_net) - entry_spread; must be non-negative"
            ),
            "costs": "observed entry and exit bid/ask spread included",
        },
        "volume_contract": {
            "field": "current_volume plus 12/30-bar ratios",
            "meaning": "OANDA candle tick/update count, not centralized traded size",
            "available_across_matrix": True,
        },
        "violations": {
            "pricing_identity": total_identity_violations,
            "negative_inferred_exit_spread": invalid_exit_spreads,
        },
        "sources": source_rows,
        "cells": cells,
    }


def _aggregate_economics_by_matrix_cell(
    economics: dict[str, Any],
) -> dict[tuple[str, int], dict[str, float]]:
    buckets: dict[tuple[str, int], dict[str, float]] = {}
    for row in economics.get("cells") or []:
        key = (str(row.get("input_timeframe") or ""), int(row.get("horizon_sec") or 0))
        count = max(0, int(row.get("n") or 0))
        if not count:
            continue
        bucket = buckets.setdefault(key, defaultdict(float))
        bucket["n"] += count
        for field in (
            "minimum_direction_accuracy_to_break_even",
            "mean_round_trip_cost_pips",
            "mean_absolute_mid_move_pips",
        ):
            value = row.get(field)
            if value is not None:
                bucket[field] += count * finite(value)
    for bucket in buckets.values():
        count = max(1.0, bucket["n"])
        for field in tuple(bucket):
            if field != "n":
                bucket[field] /= count
    return buckets


def build_model_cell_leaderboard(
    market_report: dict[str, Any],
    economics: dict[str, Any],
) -> list[dict[str, Any]]:
    economic_cells = _aggregate_economics_by_matrix_cell(economics)
    rows: list[dict[str, Any]] = []
    for result in market_report.get("results") or []:
        model = str(result.get("model") or result.get("model_id") or "unknown")
        for cell in result.get("cells") or []:
            timeframe = str(cell.get("input_timeframe") or "")
            horizon = int(cell.get("horizon_sec") or 0)
            economics_cell = economic_cells.get((timeframe, horizon)) or {}
            trades = int(cell.get("trades") or 0)
            accuracy = finite(cell.get("direction_accuracy"))
            break_even = finite(
                economics_cell.get("minimum_direction_accuracy_to_break_even"),
                0.5,
            )
            standard_error = (
                math.sqrt(max(1e-12, break_even * (1.0 - break_even) / trades))
                if trades > 0 and 0.0 < break_even < 1.0
                else math.inf
            )
            p_value = (
                1.0 - normal_cdf((accuracy - break_even) / standard_error)
                if math.isfinite(standard_error)
                else 1.0
            )
            rows.append(
                {
                    "model": model,
                    "input_timeframe": timeframe,
                    "horizon_sec": horizon,
                    "events": int(cell.get("events") or 0),
                    "trades": trades,
                    "trade_rate": finite(cell.get("trade_rate")),
                    "direction_accuracy": accuracy,
                    "cost_break_even_direction_accuracy": round(break_even, 8),
                    "direction_edge_one_sided_p_value": round(p_value, 10),
                    "win_rate": finite(cell.get("win_rate")),
                    "mean_net_pips": finite(cell.get("mean_net_pips")),
                    "median_net_pips": finite(cell.get("median_net_pips")),
                    "sum_net_pips": finite(cell.get("sum_net_pips")),
                    "profit_factor": finite(cell.get("profit_factor")),
                    "mean_round_trip_cost_pips": round(
                        finite(economics_cell.get("mean_round_trip_cost_pips")),
                        6,
                    ),
                    "mean_absolute_mid_move_pips": round(
                        finite(economics_cell.get("mean_absolute_mid_move_pips")),
                        6,
                    ),
                }
            )
    q_values = benjamini_hochberg(
        [row["direction_edge_one_sided_p_value"] for row in rows]
    )
    trial_count = len(rows)
    for row, q_value in zip(rows, q_values):
        row["trial_count"] = trial_count
        row["direction_edge_fdr_q_value"] = round(q_value, 10)
        blockers = []
        if row["trades"] < 30:
            blockers.append("minimum_trades")
        if row["mean_net_pips"] <= 0.0:
            blockers.append("executable_net_edge")
        if row["profit_factor"] < 1.05:
            blockers.append("profit_factor")
        if q_value > 0.10:
            blockers.append("multiple_testing_direction_edge")
        row["eligible"] = not blockers
        row["blocked_by"] = blockers
    return sorted(
        rows,
        key=lambda row: (
            bool(row["eligible"]),
            row["trades"] >= 30,
            row["direction_edge_fdr_q_value"] <= 0.10,
            row["mean_net_pips"],
            row["trades"],
            row["direction_accuracy"],
        ),
        reverse=True,
    )


def build_live_cell_policy(exit_state: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    cells: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    for horizon_text, evidence in (
        exit_state.get("prediction_quality_by_horizon") or {}
    ).items():
        horizon = int(horizon_text)
        for bucket, key_shape in (
            ("pair_model_timeframes", "exact"),
            ("pair_timeframes", "pair_timeframe"),
            ("model_timeframes", "model_timeframe"),
        ):
            for scope_key, value in (evidence.get(bucket) or {}).items():
                if not isinstance(value, dict):
                    continue
                parts = str(scope_key).split("::")
                if key_shape == "exact" and len(parts) == 3:
                    policy_key = f"{parts[0]}|{parts[1]}|{parts[2]}|{horizon}"
                elif key_shape == "pair_timeframe" and len(parts) == 2:
                    policy_key = f"{parts[0]}|*|{parts[1]}|{horizon}"
                elif key_shape == "model_timeframe" and len(parts) == 2:
                    policy_key = f"*|{parts[0]}|{parts[1]}|{horizon}"
                else:
                    continue
                compact = {
                    key: value.get(key)
                    for key in (
                        "eligible",
                        "negative_evidence",
                        "blocked_by",
                        "sample_count",
                        "minimum_total_samples",
                        "evidence_strength",
                        "calibrated_expected_net_pips",
                        "calibrated_lower_net_pips",
                        "calibrated_positive_probability",
                        "selection_adjusted_lower_net_pips",
                        "deflated_sharpe_probability",
                        "trial_count",
                        "independent_holdout_blocks",
                        "minimum_independent_blocks",
                        "holdout",
                        "overall",
                    )
                    if key in value
                }
                compact["scope"] = key_shape
                cells[policy_key] = compact
                rows.append({"cell_key": policy_key, "horizon_sec": horizon, **compact})
    rows.sort(
        key=lambda row: (
            bool(row.get("eligible")),
            finite(row.get("selection_adjusted_lower_net_pips"), -999.0),
            finite(row.get("deflated_sharpe_probability")),
            int(row.get("sample_count") or 0),
        ),
        reverse=True,
    )
    return cells, rows


def build_exit_policies(exit_sweep: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for horizon, payload in (exit_sweep.get("horizons") or {}).items():
        overall_scope = payload.get("overall") or {}
        selected = (
            overall_scope.get("selected_on_inner_walkforward")
            or overall_scope.get("selected_on_training")
            or {}
        )
        overall = {**selected, "scope": "overall"} if selected else {}
        families = {}
        for family, family_scope in (payload.get("families") or {}).items():
            family_selected = (
                family_scope.get("selected_on_inner_walkforward")
                or family_scope.get("selected_on_training")
                or {}
            )
            if family_selected:
                families[str(family)] = {
                    **family_selected,
                    "scope": "family",
                }
        output[str(int(horizon))] = {"overall": overall, "families": families}
    return output


def summarize_execution_ledger(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"status": "missing", "database": str(path), "executions": 0}
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5.0)
    try:
        rows = connection.execute(
            "SELECT submitted_epoch, status, trade_id, payload_json "
            "FROM executions ORDER BY submitted_epoch"
        ).fetchall()
    finally:
        connection.close()
    statuses = Counter(str(row[1]) for row in rows)
    policy_counts: Counter[str] = Counter()
    for _, _, _, payload_json in rows:
        try:
            payload = json.loads(payload_json or "{}")
        except json.JSONDecodeError:
            continue
        policy_id = str(payload.get("execution_policy_id") or "pre_policy")
        policy_counts[policy_id] += 1
    return {
        "status": "ready",
        "database": str(path.resolve()),
        "executions": len(rows),
        "status_counts": dict(sorted(statuses.items())),
        "policy_counts": dict(sorted(policy_counts.items())),
        "first_submitted_epoch": rows[0][0] if rows else None,
        "last_submitted_epoch": rows[-1][0] if rows else None,
        "filled_trade_ids": [str(row[2]) for row in rows if row[1] == "filled" and row[2]],
    }


def feed_realism_report(root: Path) -> dict[str, Any]:
    state_root = root / STATE_DIR_RELATIVE
    state_files = {}
    for name in (
        "second_forecast_hot_heartbeat_v1.json",
        "second_forecast_tracker_heartbeat_v1.json",
        "strategy_lab_heartbeat_v1.json",
        "depth_live_v1.json",
    ):
        path = state_root / name
        payload = read_json(path)
        state_files[name] = {
            "exists": path.is_file(),
            "modified_utc": (
                datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
                if path.is_file()
                else None
            ),
            "status": payload.get("status"),
            "generated_at": payload.get("generated_at") or payload.get("generated_utc"),
        }
    return {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "provider": "OANDA v20 practice",
        "pricing_contract": {
            "tick_complete": False,
            "maximum_documented_stream_updates_per_instrument_per_second": 4,
            "documented_behavior": "pricing stream omits some prices and caps updates",
            "source": "https://developer.oanda.com/rest-live-v20/pricing-ep/",
        },
        "runtime": {
            "strategy_scan_pause_sec": 0.25,
            "strategy_trade_manage_interval_sec": 0.25,
            "hot_forecast_cadence_sec": 1,
            "hot_quote_sample_sec": 5,
            "tracker_quote_sample_sec": 30,
            "forecast_claim": "subsecond decisions are not claimed; S5 is the fastest fitted bar input",
        },
        "volume": {
            "live_and_historical": True,
            "meaning": "OANDA candle tick/update count",
            "centralized_traded_volume": False,
        },
        "order_book": {
            "collected_when_depth_worker_is_healthy": state_files[
                "depth_live_v1.json"
            ]["exists"],
            "meaning": "OANDA position/order-book snapshots, not exchange L2 depth",
        },
        "state_files": state_files,
    }


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_candidate_policy(
    *,
    economics: dict[str, Any],
    market_report: dict[str, Any],
    market_report_path: Path,
    exit_state: dict[str, Any],
    exit_state_path: Path,
    exit_sweep: dict[str, Any],
    exit_sweep_path: Path,
    cells: dict[str, Any],
    freeze_hours: float,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    frozen_until = now + timedelta(hours=max(1.0, float(freeze_hours)))
    exit_policies = build_exit_policies(exit_sweep)
    identity = {
        "market_report_sha256": sha256_file(market_report_path),
        "economics_validation_passed": bool(economics.get("validation_passed")),
        "pricing_identity_tolerance_pips": (economics.get("unit_contract") or {}).get(
            "identity_tolerance_pips"
        ),
        "exit_state_sha256": sha256_file(exit_state_path) if exit_state_path.is_file() else "",
        "exit_sweep_sha256": sha256_file(exit_sweep_path) if exit_sweep_path.is_file() else "",
        "cell_count": len(cells),
        "exit_horizon_count": len(exit_policies),
        "gates": {
            "minimum_gross_to_spread": 1.15,
            "negative_veto_min_samples": 30,
            "maximum_fdr_q_value": 0.10,
            "minimum_deflated_sharpe_probability": 0.90,
        },
    }
    policy_id = "p007-" + hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    market_summary = market_report.get("summary") or {}
    validation_passed = bool(
        economics.get("validation_passed")
        and market_summary.get("all_requested_backtested")
    )
    return {
        "schema_version": 1,
        "generated_utc": now.isoformat(),
        "status": "active" if validation_passed else "shadow_only",
        "policy_id": policy_id,
        "account_scope": "practice_007_only",
        "execution_mode": "frozen_guarded_all_signal_consensus",
        "effective_utc": now.isoformat(),
        "frozen_until_utc": frozen_until.isoformat(),
        "validation_passed": validation_passed,
        "gates": identity["gates"],
        "cells": cells,
        "exit_policies": exit_policies,
        "provenance": {
            **identity,
            "market_report": str(market_report_path.resolve()),
            "exit_state": str(exit_state_path.resolve()),
            "exit_sweep": str(exit_sweep_path.resolve()),
            "costs": "observed bid/ask executable pips",
            "selection": "chronological holdout plus trial-adjusted evidence",
        },
    }


def promote_or_hold(
    challenger: dict[str, Any],
    incumbent_path: Path,
    *,
    enabled: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    incumbent = read_json(incumbent_path)
    if not enabled:
        return incumbent, {
            "action": "hold_incumbent",
            "reason": "account_auto_promotion_disabled",
        }
    now = datetime.now(timezone.utc)
    frozen_until = _parse_utc(incumbent.get("frozen_until_utc"))
    if not incumbent:
        atomic_json(incumbent_path, challenger)
        return challenger, {"action": "bootstrap_incumbent", "reason": "no_prior_policy"}
    if (
        incumbent.get("status") == "active"
        and frozen_until is not None
        and now < frozen_until
    ):
        return incumbent, {
            "action": "hold_incumbent",
            "reason": "frozen_forward_window_active",
            "remaining_sec": round((frozen_until - now).total_seconds(), 3),
        }
    incumbent_validated = sum(
        bool(value.get("eligible")) for value in (incumbent.get("cells") or {}).values()
    )
    challenger_validated = sum(
        bool(value.get("eligible")) for value in (challenger.get("cells") or {}).values()
    )
    incumbent_negative = sum(
        bool(value.get("negative_evidence"))
        for value in (incumbent.get("cells") or {}).values()
    )
    challenger_negative = sum(
        bool(value.get("negative_evidence"))
        for value in (challenger.get("cells") or {}).values()
    )
    passes = bool(
        challenger.get("status") == "active"
        and challenger_validated >= max(0, int(0.80 * incumbent_validated))
        and challenger_negative <= incumbent_negative
    )
    if passes:
        atomic_json(incumbent_path, challenger)
        return challenger, {
            "action": "promote_challenger",
            "reason": "freeze_elapsed_and_evidence_not_worse",
            "incumbent_validated_cells": incumbent_validated,
            "challenger_validated_cells": challenger_validated,
            "incumbent_negative_cells": incumbent_negative,
            "challenger_negative_cells": challenger_negative,
        }
    return incumbent, {
        "action": "hold_incumbent",
        "reason": "challenger_evidence_gate_failed",
        "incumbent_validated_cells": incumbent_validated,
        "challenger_validated_cells": challenger_validated,
        "incumbent_negative_cells": incumbent_negative,
        "challenger_negative_cells": challenger_negative,
    }


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    root = args.root.resolve()
    report_dir = args.report_dir.resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    market_report_path = args.market_report.resolve()
    market_report = read_json(market_report_path)
    economics_path = report_dir / ECONOMICS_NAME
    prior_economics = read_json(economics_path)
    market_hash = sha256_file(market_report_path)
    if (
        not args.force_economics
        and prior_economics.get("market_report_sha256") == market_hash
        and prior_economics.get("validation_passed")
    ):
        economics = prior_economics
        economics_reused = True
    else:
        economics = audit_panel_economics(
            market_report_path,
            batch_size=args.batch_size,
            max_source_rows=args.max_source_rows,
        )
        atomic_json(economics_path, economics)
        economics_reused = False

    exit_state_path = args.exit_state.resolve()
    exit_sweep_path = args.exit_sweep.resolve()
    exit_state = read_json(exit_state_path)
    exit_sweep = read_json(exit_sweep_path)
    cells, live_cell_rows = build_live_cell_policy(exit_state)
    historical_model_rows = build_model_cell_leaderboard(market_report, economics)
    leaderboard = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "status": "ready",
        "historical_model_cell_count": len(historical_model_rows),
        "live_pair_model_timeframe_cell_count": len(live_cell_rows),
        "historical_model_cells": historical_model_rows,
        "live_signal_cells": live_cell_rows,
        "definitions": {
            "cell": "model or all-signal evidence at one input timeframe and outcome horizon",
            "cost_break_even_direction_accuracy": (
                "accuracy required for mean correct/wrong executable payoffs to net zero"
            ),
            "fdr_q_value": "Benjamini-Hochberg false-discovery-rate adjusted p-value",
            "deflated_sharpe_probability": (
                "probability that block Sharpe exceeds the expected best null result "
                "after the recorded number of trials"
            ),
        },
    }
    leaderboard_path = report_dir / LEADERBOARD_NAME
    atomic_json(leaderboard_path, leaderboard)

    challenger = build_candidate_policy(
        economics=economics,
        market_report=market_report,
        market_report_path=market_report_path,
        exit_state=exit_state,
        exit_state_path=exit_state_path,
        exit_sweep=exit_sweep,
        exit_sweep_path=exit_sweep_path,
        cells=cells,
        freeze_hours=args.freeze_hours,
    )
    atomic_json(args.challenger_state.resolve(), challenger)
    incumbent, promotion = promote_or_hold(
        challenger,
        args.incumbent_state.resolve(),
        enabled=bool(args.account_auto_promotion),
    )

    feed = feed_realism_report(root)
    feed_path = report_dir / FEED_NAME
    atomic_json(feed_path, feed)
    ledger = summarize_execution_ledger(args.signal_feed.resolve())
    evaluation = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "status": "collecting_frozen_forward_evidence",
        "incumbent_policy_id": incumbent.get("policy_id"),
        "challenger_policy_id": challenger.get("policy_id"),
        "promotion": promotion,
        "execution_ledger": ledger,
        "limitations": (
            "Account P/L is not attributed to the new policy until executions carry "
            "its policy_id and those trades close."
        ),
    }
    evaluation_path = report_dir / EVALUATION_NAME
    atomic_json(evaluation_path, evaluation)

    pipeline = {
        "schema_version": 1,
        "generated_utc": utc_now(),
        "status": "ready" if economics.get("validation_passed") else "blocked",
        "economics_reused": economics_reused,
        "account_scope": "practice_007_only",
        "incumbent": {
            "policy_id": incumbent.get("policy_id"),
            "status": incumbent.get("status"),
            "effective_utc": incumbent.get("effective_utc"),
            "frozen_until_utc": incumbent.get("frozen_until_utc"),
            "cell_count": len(incumbent.get("cells") or {}),
            "validated_cells": sum(
                bool(value.get("eligible"))
                for value in (incumbent.get("cells") or {}).values()
            ),
            "negative_cells": sum(
                bool(value.get("negative_evidence"))
                for value in (incumbent.get("cells") or {}).values()
            ),
            "exit_horizon_count": len(incumbent.get("exit_policies") or {}),
        },
        "challenger": {
            "policy_id": challenger.get("policy_id"),
            "status": challenger.get("status"),
            "cell_count": len(challenger.get("cells") or {}),
            "validated_cells": sum(
                bool(value.get("eligible"))
                for value in (challenger.get("cells") or {}).values()
            ),
            "negative_cells": sum(
                bool(value.get("negative_evidence"))
                for value in (challenger.get("cells") or {}).values()
            ),
        },
        "promotion": promotion,
        "economics": {
            "status": economics.get("status"),
            "validation_passed": economics.get("validation_passed"),
            "scope": economics.get("scope") or {},
            "violations": economics.get("violations") or {},
            "unit_contract": economics.get("unit_contract") or {},
            "volume_contract": economics.get("volume_contract") or {},
        },
        "cell_leaderboard": {
            "historical_model_cell_count": len(historical_model_rows),
            "historical_eligible_count": sum(
                bool(row.get("eligible")) for row in historical_model_rows
            ),
            "live_signal_cell_count": len(live_cell_rows),
            "live_eligible_count": sum(
                bool(row.get("eligible")) for row in live_cell_rows
            ),
            "live_negative_count": sum(
                bool(row.get("negative_evidence")) for row in live_cell_rows
            ),
            "top_historical_cells": historical_model_rows[:12],
            "top_live_cells": live_cell_rows[:12],
        },
        "feed_realism": {
            "provider": feed.get("provider"),
            "pricing_contract": feed.get("pricing_contract") or {},
            "runtime": feed.get("runtime") or {},
            "volume": feed.get("volume") or {},
            "order_book": feed.get("order_book") or {},
        },
        "frozen_evaluation": {
            "status": evaluation.get("status"),
            "execution_ledger": ledger,
        },
        "artifacts": {
            "economics": str(economics_path),
            "leaderboard": str(leaderboard_path),
            "feed_realism": str(feed_path),
            "frozen_evaluation": str(evaluation_path),
            "incumbent_state": str(args.incumbent_state.resolve()),
            "challenger_state": str(args.challenger_state.resolve()),
        },
        "automation": {
            "incremental_calibration_interval_sec": 300,
            "bounded_exit_refit_interval_sec": 900,
            "lane_promotion_interval_sec": 300,
            "policy_challenger_interval_sec": int(args.interval_sec or 900),
            "frozen_forward_hours": args.freeze_hours,
            "account_auto_promotion": bool(args.account_auto_promotion),
            "policy_auto_promotion": (
                "only after freeze expiration and non-degrading evidence gates"
            ),
        },
    }
    atomic_json(report_dir / PIPELINE_NAME, pipeline)
    return pipeline


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=DEFAULT_ROOT / REPORT_DIR_RELATIVE,
    )
    parser.add_argument(
        "--market-report",
        type=Path,
        default=DEFAULT_ROOT / REPORT_DIR_RELATIVE / MARKET_REPORT_NAME,
    )
    parser.add_argument(
        "--exit-state",
        type=Path,
        default=DEFAULT_ROOT / STATE_DIR_RELATIVE / "strategy_exit_fit_v1.json",
    )
    parser.add_argument(
        "--exit-sweep",
        type=Path,
        default=(
            DEFAULT_ROOT
            / MANAGER_RELATIVE
            / "reports/exit_policy_s5_walkforward_v1.json"
        ),
    )
    parser.add_argument(
        "--signal-feed",
        type=Path,
        default=DEFAULT_ROOT / STATE_DIR_RELATIVE / "practice_007_signal_feed_v1.sqlite",
    )
    parser.add_argument(
        "--incumbent-state",
        type=Path,
        default=DEFAULT_ROOT / STATE_DIR_RELATIVE / INCUMBENT_NAME,
    )
    parser.add_argument(
        "--challenger-state",
        type=Path,
        default=DEFAULT_ROOT / STATE_DIR_RELATIVE / CHALLENGER_NAME,
    )
    parser.add_argument("--batch-size", type=int, default=131072)
    parser.add_argument("--max-source-rows", type=int, default=0)
    parser.add_argument("--freeze-hours", type=float, default=6.0)
    parser.add_argument("--force-economics", action="store_true")
    parser.add_argument("--account-auto-promotion", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=0.0)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if (
        args.batch_size <= 0
        or args.max_source_rows < 0
        or args.freeze_hours <= 0.0
        or args.interval_sec < 0.0
    ):
        raise SystemExit("batch, row cap, freeze, and interval values are invalid")
    return args


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    while True:
        started = time.monotonic()
        report = run_pipeline(args)
        print(
            json.dumps(
                {
                    "status": report.get("status"),
                    "incumbent": report.get("incumbent"),
                    "challenger": report.get("challenger"),
                    "promotion": report.get("promotion"),
                    "elapsed_sec": round(time.monotonic() - started, 3),
                },
                indent=2,
            ),
            flush=True,
        )
        if args.interval_sec <= 0.0:
            return 0
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
