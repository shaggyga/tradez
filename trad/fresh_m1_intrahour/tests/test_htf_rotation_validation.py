from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from fresh_m1_intrahour.src.htf_rotation_validation import (
    HARD_MARGIN_PCT,
    _legacy_contract,
    _threshold_map,
    build_rotation_candidates,
    fixed_horizon_viability,
    path_viability,
)


class HTFRotationValidationTests(unittest.TestCase):
    def test_thresholds_are_derived_from_development_surface(self) -> None:
        development = pd.DataFrame({"selected_score": np.arange(1.0, 101.0)})
        thresholds = _threshold_map(development)
        self.assertEqual(thresholds["positive"], 0.0)
        self.assertAlmostEqual(thresholds["q99"], 99.01)

    def test_candidate_percentiles_use_supplied_development_scores(self) -> None:
        surface = pd.DataFrame({
            "instrument": ["EUR_USD", "GBP_USD"],
            "entry_time_utc": pd.to_datetime([
                "2026-01-02T10:00:00Z",
                "2026-01-02T10:00:00Z",
            ]),
            "label_end_time_utc": pd.to_datetime([
                "2026-01-02T12:00:00Z",
                "2026-01-02T12:00:00Z",
            ]),
            "selected_score": [2.0, 20.0],
            "selected_direction": ["LONG", "SHORT"],
            "atr240_pips": [10.0, 20.0],
            "spread_pips": [1.0, 1.2],
            "feature_family": ["safe_full_220", "safe_full_220"],
            "objective": ["endpoint_regressor", "endpoint_regressor"],
            "fold": ["evaluation", "evaluation"],
        })
        candidates = build_rotation_candidates(surface, np.array([1.0, 2.0, 3.0, 4.0]))
        self.assertAlmostEqual(float(candidates.iloc[0]["score_percentile"]), 1.0)
        self.assertAlmostEqual(float(candidates.iloc[1]["score_percentile"]), 0.5)
        self.assertEqual(set(candidates["source_stream"]), {"corrected_h1"})

    def test_legacy_snapshot_is_forced_to_research_only(self) -> None:
        live_cfg, profile = _legacy_contract(5.0, 30, realistic=True)
        self.assertFalse(live_cfg["live_execution_enabled"])
        self.assertFalse(live_cfg["demo_execution_enabled"])
        self.assertFalse(live_cfg["oanda_execution_enabled"])
        self.assertFalse(live_cfg["broker_placement_enabled"])
        self.assertFalse(live_cfg["live_new_entries_enabled"])
        self.assertEqual(profile["max_open_positions"], 30)
        self.assertEqual(profile["min_rank_score"], 5.0)
        self.assertEqual(profile["hard_margin_pct"], HARD_MARGIN_PCT)

    def test_fixed_horizon_viability_rejects_large_drawdown(self) -> None:
        metrics = {
            "trades": 500,
            "pnl_usd": 250.0,
            "mean_net_pips": 1.0,
            "profit_factor": 1.2,
            "max_drawdown_pct": 50.0,
        }
        folds = [
            {"pnl_usd": 100.0},
            {"pnl_usd": 80.0},
            {"pnl_usd": 70.0},
            {"pnl_usd": 0.0},
        ]
        result = fixed_horizon_viability(metrics, folds)
        self.assertFalse(result["viable"])
        self.assertFalse(result["max_drawdown_at_most_35pct"])

    def test_path_viability_rejects_margin_breach(self) -> None:
        summary = {
            "opened_trades": 500,
            "pnl_usd": 250.0,
            "path_adjusted_pnl_usd": 100.0,
            "profit_factor": 1.2,
            "max_drawdown_pct": 10.0,
            "max_margin_used_pct": 55.0,
            "margin_call_rows": 0,
            "ending_balance": 1250.0,
        }
        quarters = [
            {"pnl_usd": 100.0},
            {"pnl_usd": 80.0},
            {"pnl_usd": 70.0},
            {"pnl_usd": 0.0},
        ]
        result = path_viability(summary, quarters)
        self.assertFalse(result["viable"])
        self.assertFalse(result["max_margin_at_most_40pct"])


if __name__ == "__main__":
    unittest.main()
