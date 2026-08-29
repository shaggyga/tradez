#!/usr/bin/env python3
"""Audit primary live bot wiring against the selected OANDA-style backtest row."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "primary_forecast_rotation_bot.json"
REPORT = (
    ROOT
    / "data"
    / "oanda_training_manager"
    / "reports"
    / "primary_live_exit_rotation_sweep_selected_live_nav_20260703"
    / "sweep_summary.csv"
)
OUT = ROOT / "data" / "technical_scout_manager" / "account_live_primary_forecast_rotation" / "parity_audit_latest.json"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def check(name: str, live: Any, expected: Any, *, status: str = "pass", note: str = "") -> Dict[str, Any]:
    ok = live == expected
    return {
        "name": name,
        "status": status if ok else "fail",
        "live": live,
        "expected": expected,
        "note": note,
    }


def main() -> int:
    cfg = json.loads(CONFIG.read_text(encoding="utf-8-sig"))
    sweep = pd.read_csv(REPORT)
    best = sweep.sort_values("return_pct", ascending=False).iloc[0].to_dict()
    candidate_path = Path(cfg["model_stream"]["candidate_rows_csv"])
    checks: List[Dict[str, Any]] = []

    expected_candidate = (
        ROOT
        / "data"
        / "oanda_training_manager"
        / "reports"
        / "expanded_m30_h1_h4_oanda_broker_sweep_inverted"
        / "ensemble_m30_h1_h4_all_h_tf_growth_wider_stop"
        / "candidate_rows.csv"
    ).resolve()
    checks.append(check("forecast_source", cfg.get("forecast_source"), "model_stream"))
    checks.append(check("model_stream_enabled", cfg.get("model_stream", {}).get("enabled"), True))
    checks.append(check("candidate_rows_path", str(candidate_path.resolve()), str(expected_candidate)))
    checks.append(check("candidate_rows_exists", candidate_path.exists(), True))
    if candidate_path.exists():
        checks.append(check("candidate_rows_count", csv_rows(candidate_path), 40750))

    model_streams = [
        (item.get("source_stream"), item.get("feature_set"), item.get("feature_timeframe"))
        for item in cfg.get("model_stream", {}).get("models", [])
    ]
    checks.append(
        check(
            "model_members",
            model_streams,
            [("m30", "technical_full", "30min"), ("h1", "technical_full", "1h"), ("h4", "technical_full", "4h")],
        )
    )

    exact_keys = {
        "signal_gone_exit_minutes": float(best["signal_gone_exit_minutes"]),
        "signal_gone_min_horizon_fraction": float(best["signal_gone_min_horizon_fraction"]),
        "profit_lock_min_age_minutes": float(best["profit_lock_min_age_minutes"]),
        "profit_lock_min_pips": float(best["profit_lock_min_pips"]),
        "profit_lock_r_multiple": float(best["profit_lock_r_multiple"]),
        "atr_stop_multiplier": 1.25,
        "take_profit_edge_capture": 0.50,
        "take_profit_min_r_multiple": 0.40,
        "trailing_stop_r_multiple": 0.80,
        "min_edge_pips": 0.50,
        "max_spread_to_edge_ratio": 0.80,
        "min_profit_per_margin": 0.0035,
        "target_margin_used_pct": 68.0,
        "max_margin_used_pct": 82.0,
        "target_open_risk_pct": 7.2,
        "max_open_risk_pct": 9.5,
    }
    for key, expected in exact_keys.items():
        live = cfg.get(key)
        if isinstance(expected, float):
            live = float(live)
        checks.append(check(key, live, expected))

    mapped = [
        (
            "replacement_min_age_minutes",
            float(cfg.get("replacement_min_age_minutes")),
            float(best["min_rotation_age_minutes"]),
            "live replacement age maps to replay min_rotation_age_minutes",
        ),
        (
            "replacement_min_score_multiplier",
            float(cfg.get("replacement_min_score_multiplier")),
            float(best["rotation_score_multiplier"]),
            "live replacement score multiplier maps to replay rotation_score_multiplier",
        ),
        (
            "replacement_on_pressure",
            bool(cfg.get("replacement_on_pressure")),
            True,
            "enables pressure-based replacement like replay margin/risk rotations",
        ),
    ]
    for name, live, expected, note in mapped:
        checks.append(check(name, live, expected, note=note))

    checks.append(
        {
            "name": "max_rotations_cap",
            "status": "non_binding_difference",
            "live": {
                "max_replacements_per_hour": cfg.get("max_replacements_per_hour"),
                "max_new_positions_per_cycle": cfg.get("max_new_positions_per_cycle"),
            },
            "expected": {"max_rotations_per_timestamp": int(best["max_rotations_per_timestamp"])},
            "note": "Replay cap is per historical timestamp; selected winner used max 1 stale rotation per timestamp and max 2 per rolling hour.",
        }
    )
    checks.append(
        {
            "name": "multi_close_replacement",
            "status": "non_binding_difference",
            "live": "one replacement close per replacement open",
            "expected": "replay can close multiple eligible positions before one open",
            "note": "Selected winner used max 1 stale rotation per timestamp, so multi-close replacement did not bind.",
        }
    )
    checks.append(
        {
            "name": "execution_prices",
            "status": "known_difference",
            "live": "broker quotes, order reject rules, latency, actual fills",
            "expected": "historical OANDA M1 bid/ask candle replay",
            "note": "This can only be confirmed forward by shadow/live trade-vs-replay reconciliation.",
        }
    )

    result = {
        "selected_backtest": {
            key: best[key]
            for key in [
                "variant_name",
                "return_pct",
                "profit_factor",
                "win_rate",
                "max_drawdown_pct",
                "max_margin_used_pct",
                "margin_call_rows",
            ]
        },
        "candidate_rows_sha256": file_sha256(candidate_path) if candidate_path.exists() else "",
        "checks": checks,
        "failures": [item for item in checks if item["status"] == "fail"],
        "known_differences": [
            item
            for item in checks
            if item["status"] in {"known_difference", "mapped_not_identical", "non_binding_difference"}
        ],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if not result["failures"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
