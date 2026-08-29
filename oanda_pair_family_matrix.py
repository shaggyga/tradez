#!/usr/bin/env python3
"""Persist the complete observed pair/family/timeframe/horizon signal matrix."""

from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_DATA_ROOT = (
    Path(__file__).resolve().parent / "data" / "oanda_training_manager"
)
DEFAULT_STATE_ROOT = DEFAULT_DATA_ROOT / "state"
DEFAULT_EXIT_FIT_STATE = DEFAULT_STATE_ROOT / "strategy_exit_fit_v1.json"
DEFAULT_TIMEFRAME_STATE = DEFAULT_STATE_ROOT / "timeframe_matrix_calibration_v1.json"
DEFAULT_OUTPUT = DEFAULT_STATE_ROOT / "pair_family_timeframe_horizon_v1.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float | None = None) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def integer(value: Any, default: int = 0) -> int:
    number = finite(value)
    return default if number is None else int(number)


def load_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _scope_parts(scope_key: str) -> tuple[str, str, str] | None:
    parts = str(scope_key).split("::", 2)
    if len(parts) != 3 or not all(parts):
        return None
    return parts[0], parts[1], parts[2]


def _cell_key(
    instrument: str,
    family: str,
    input_timeframe: str,
    horizon_sec: int,
) -> str:
    return "|".join((instrument, family, input_timeframe, str(horizon_sec)))


def _strategy_row(
    instrument: str,
    family: str,
    input_timeframe: str,
    horizon_sec: int,
    fit: dict[str, Any],
) -> dict[str, Any]:
    holdout = fit.get("holdout") or {}
    overall = fit.get("overall") or {}
    blocked_by = list(fit.get("blocked_by") or [])
    eligible = bool(fit.get("eligible"))
    negative = bool(fit.get("negative_evidence"))
    status = "eligible" if eligible else "negative" if negative else "collecting"
    return {
        "cell_key": _cell_key(instrument, family, input_timeframe, horizon_sec),
        "instrument": instrument,
        "family": family,
        "input_timeframe": input_timeframe,
        "horizon_sec": int(horizon_sec),
        "source_kind": "live_strategy_exit_fit",
        "sample_count": integer(fit.get("sample_count", overall.get("n"))),
        "holdout_n": integer(holdout.get("n")),
        "independent_blocks": integer(fit.get("independent_holdout_blocks")),
        "minimum_samples": integer(fit.get("minimum_total_samples"), 30),
        "direction_accuracy_pct": None,
        "win_rate_pct": finite(holdout.get("win_rate")),
        "average_net_pips": finite(holdout.get("avg_pips")),
        "lower_confidence_net_pips": finite(
            holdout.get("lower_confidence_pips")
        ),
        "calibrated_positive_probability": finite(
            fit.get("calibrated_positive_probability")
        ),
        "ever_positive_rate_pct": finite(holdout.get("ever_positive_rate")),
        "median_mfe_pips": finite(holdout.get("median_mfe_pips")),
        "median_mae_pips": finite(holdout.get("median_mae_pips")),
        "median_entry_spread_pips": finite(
            holdout.get("median_entry_spread_pips")
        ),
        "evidence_strength": finite(fit.get("evidence_strength"), 0.0),
        "eligible": eligible,
        "negative_evidence": negative,
        "status": status,
        "blocked_by": blocked_by,
        "split": str(fit.get("split") or "chronological holdout"),
    }


def _equation_row(surface: dict[str, Any]) -> dict[str, Any] | None:
    instrument = str(surface.get("instrument") or "")
    lane_id = str(surface.get("lane_id") or "")
    horizon_sec = integer(surface.get("horizon_sec"))
    if (
        not instrument
        or not lane_id.startswith("timeframe_equation_matrix.")
        or horizon_sec <= 0
    ):
        return None
    input_timeframe = lane_id.rsplit(".", 1)[-1].upper()
    validation_ready = bool(surface.get("validation_ready"))
    average_net = finite(surface.get("calibrated_average_net_pips"))
    lower_net = finite(surface.get("lower_confidence_net_pips"))
    blocked_by: list[str] = []
    if not validation_ready:
        blocked_by.append("minimum_surface_evidence")
    if average_net is None or average_net <= 0.0:
        blocked_by.append("average_executable_net_pips")
    if lower_net is None or lower_net <= 0.0:
        blocked_by.append("lower_confidence_net_pips")
    return {
        "cell_key": _cell_key(
            instrument,
            "timeframe_equation_matrix",
            input_timeframe,
            horizon_sec,
        ),
        "instrument": instrument,
        "family": "timeframe_equation_matrix",
        "input_timeframe": input_timeframe,
        "horizon_sec": horizon_sec,
        "source_kind": "timeframe_equation_calibration",
        "sample_count": integer(surface.get("n")),
        "holdout_n": integer(surface.get("oos_n")),
        "independent_blocks": integer(surface.get("independent_blocks")),
        "minimum_samples": 120,
        "direction_accuracy_pct": (
            None
            if finite(surface.get("calibrated_accuracy")) is None
            else 100.0 * float(surface["calibrated_accuracy"])
        ),
        "win_rate_pct": (
            None
            if finite(surface.get("calibrated_win_rate")) is None
            else 100.0 * float(surface["calibrated_win_rate"])
        ),
        "average_net_pips": average_net,
        "lower_confidence_net_pips": lower_net,
        "calibrated_positive_probability": None,
        "ever_positive_rate_pct": None,
        "median_mfe_pips": None,
        "median_mae_pips": None,
        "median_entry_spread_pips": None,
        "evidence_strength": min(1.0, integer(surface.get("oos_n")) / 80.0),
        "eligible": False,
        "negative_evidence": bool(validation_ready and average_net is not None and average_net < 0.0),
        "status": "validated_shadow" if validation_ready else "collecting",
        "blocked_by": blocked_by,
        "split": "rolling causal out-of-sample calibration",
    }


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        bool(row.get("eligible")),
        finite(row.get("lower_confidence_net_pips"), -math.inf),
        finite(row.get("average_net_pips"), -math.inf),
        integer(row.get("holdout_n")),
        integer(row.get("sample_count")),
        str(row.get("instrument") or ""),
        str(row.get("family") or ""),
        str(row.get("input_timeframe") or ""),
        integer(row.get("horizon_sec")),
    )


def build_pair_family_matrix(
    exit_fit_state: dict[str, Any],
    timeframe_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return every observed interaction cell from the persisted evidence states."""

    cells: dict[str, dict[str, Any]] = {}
    for horizon_text, evidence in (
        exit_fit_state.get("prediction_quality_by_horizon") or {}
    ).items():
        horizon_sec = integer(horizon_text)
        if horizon_sec <= 0 or not isinstance(evidence, dict):
            continue
        scopes = evidence.get("pair_family_timeframes") or {}
        if not isinstance(scopes, dict):
            continue
        for scope_key, fit in scopes.items():
            parts = _scope_parts(scope_key)
            if parts is None or not isinstance(fit, dict):
                continue
            row = _strategy_row(*parts, horizon_sec, fit)
            cells[row["cell_key"]] = row

    timeframe_state = timeframe_state or {}
    pair_surfaces = timeframe_state.get("pair_surfaces") or {}
    surfaces = pair_surfaces.values() if isinstance(pair_surfaces, dict) else pair_surfaces
    for surface in surfaces:
        if not isinstance(surface, dict):
            continue
        row = _equation_row(surface)
        if row is not None:
            cells[row["cell_key"]] = row

    rows = sorted(cells.values(), key=_sort_key, reverse=True)
    pairs = sorted({str(row["instrument"]) for row in rows})
    families = sorted({str(row["family"]) for row in rows})
    timeframes = sorted({str(row["input_timeframe"]) for row in rows})
    horizons = sorted({int(row["horizon_sec"]) for row in rows})
    possible = len(pairs) * len(families) * len(timeframes) * len(horizons)
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "status": "ready" if rows else "collecting",
        "scope": (
            "complete sparse export of every observed pair x strategy-family x "
            "input-timeframe x outcome-horizon cell; absent Cartesian cells have "
            "no recorded evidence"
        ),
        "definitions": {
            "direction_accuracy_pct": "Correct exit-mid direction percentage; spread is not part of this metric.",
            "win_rate_pct": "Percentage of chronological holdout or causal OOS outcomes positive after bid/ask costs.",
            "average_net_pips": "Mean executable directional pips after bid/ask costs in holdout or causal OOS data.",
            "lower_confidence_net_pips": "One-sided lower confidence bound for executable average net pips.",
            "holdout_n": "Newest chronological holdout observations, or causal OOS observations for equation surfaces.",
            "independent_blocks": "Non-overlapping maturity-sized evidence blocks in the holdout/OOS sample.",
            "eligible": "Existing account gate result; collecting cells never become executable from this leaderboard alone.",
        },
        "counts": {
            "observed_cells": len(rows),
            "possible_cartesian_cells": possible,
            "coverage_pct": 0.0 if not possible else round(100.0 * len(rows) / possible, 6),
            "pairs": len(pairs),
            "families": len(families),
            "timeframes": len(timeframes),
            "horizons": len(horizons),
            "eligible_cells": sum(bool(row.get("eligible")) for row in rows),
            "positive_average_cells": sum(
                finite(row.get("average_net_pips"), -math.inf) > 0.0 for row in rows
            ),
            "positive_lower_bound_cells": sum(
                finite(row.get("lower_confidence_net_pips"), -math.inf) > 0.0
                for row in rows
            ),
            "strategy_exit_fit_cells": sum(
                row.get("source_kind") == "live_strategy_exit_fit" for row in rows
            ),
            "timeframe_equation_cells": sum(
                row.get("source_kind") == "timeframe_equation_calibration" for row in rows
            ),
        },
        "dimensions": {
            "pairs": pairs,
            "families": families,
            "timeframes": timeframes,
            "horizons_sec": horizons,
        },
        "sources": {
            "strategy_exit_fit_generated_at": exit_fit_state.get("generated_at"),
            "timeframe_matrix_generated_at": timeframe_state.get("generated_utc"),
        },
        "rows": rows,
    }


def write_pair_family_matrix(
    exit_fit_state: dict[str, Any],
    *,
    timeframe_state_path: Path = DEFAULT_TIMEFRAME_STATE,
    output_path: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    payload = build_pair_family_matrix(
        exit_fit_state,
        load_json_dict(timeframe_state_path),
    )
    write_json_atomic(output_path, payload)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exit-fit-state", type=Path, default=DEFAULT_EXIT_FIT_STATE)
    parser.add_argument("--timeframe-state", type=Path, default=DEFAULT_TIMEFRAME_STATE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = build_pair_family_matrix(
        load_json_dict(args.exit_fit_state),
        load_json_dict(args.timeframe_state),
    )
    write_json_atomic(args.output, payload)
    print(json.dumps({"output": str(args.output), **payload["counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
