"""Focused regression tests for the significant-move research pipeline."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


TRAD_DIR = Path(__file__).resolve().parent
if str(TRAD_DIR) not in sys.path:
    sys.path.insert(0, str(TRAD_DIR))

import significant_moves_pipeline as pipeline


class SignificantMovesPipelineTests(unittest.TestCase):
    @staticmethod
    def _config(**overrides: object) -> dict[str, object]:
        config: dict[str, object] = {
            "pipeline_version": "unit-test",
            "bar_minutes": 5,
            "horizons_minutes": [120],
            "scan_stride_minutes": 5,
            "primary_horizon_minutes": 120,
            "broad_quantile": 0.0,
            "strict_quantile": 0.75,
            "fixed_minimum_pips": 0.0,
            "minimum_threshold_history_windows": 1,
            "minimum_window_bar_coverage": 1.0,
            "atr_lookback_minutes": 15,
            "atr_minimum_observations": 2,
            "strict_minimum_atr_units": 0.0,
            "strict_minimum_move_to_cost": 0.0,
            "strict_minimum_path_efficiency": 0.0,
            "slippage_pips_per_side": 0.0,
            "reversal_noise_pips": 0.1,
            "quality_warning_bar_coverage": 0.9,
        }
        config.update(overrides)
        return config

    @staticmethod
    def _regular_five_minute_bars() -> pd.DataFrame:
        index = pd.date_range(
            "2024-01-02T00:00:00Z",
            periods=30,
            freq="5min",
        )
        # A convex path gives the six two-hour windows distinct, positive moves.
        mids = [1.1000 + (position**2) / 10_000.0 for position in range(len(index))]
        half_spread = 0.5 / 10_000.0
        wick = 0.2 / 10_000.0
        return pd.DataFrame(
            {
                "close": mids,
                "high": [mid + wick for mid in mids],
                "low": [mid - wick for mid in mids],
                "bid_close": [mid - half_spread for mid in mids],
                "ask_close": [mid + half_spread for mid in mids],
                "spread_pips": [1.0] * len(index),
                "source_m1_observations": [5] * len(index),
                "spread_estimated_fraction": [0.0] * len(index),
            },
            index=index,
        )

    def test_validated_pip_multiplier_exceptions(self) -> None:
        for instrument in ("USD_JPY", "EUR_HUF", "USD_THB"):
            with self.subTest(instrument=instrument):
                self.assertEqual(pipeline.pip_multiplier(instrument), 100.0)

        for instrument in ("HKD_JPY", "EUR_USD"):
            with self.subTest(instrument=instrument):
                self.assertEqual(pipeline.pip_multiplier(instrument), 10_000.0)

    def test_detect_candidates_uses_exact_wall_clock_horizon(self) -> None:
        bars = self._regular_five_minute_bars()

        candidates, threshold_rows = pipeline.detect_move_candidates(
            bars,
            "EUR_USD",
            self._config(),
        )

        self.assertEqual(len(threshold_rows), 1)
        threshold = threshold_rows[0]
        self.assertEqual(threshold["status"], "ok")
        self.assertEqual(threshold["horizon_minutes"], 120)
        self.assertEqual(threshold["valid_windows"], 6)
        self.assertGreater(threshold["broad_threshold_pips"], 0.0)
        self.assertGreaterEqual(
            threshold["strict_threshold_pips"],
            threshold["broad_threshold_pips"],
        )

        self.assertFalse(candidates.empty)
        self.assertEqual(len(candidates), 6)
        elapsed = candidates["end_timestamp"] - candidates["start_timestamp"]
        self.assertTrue(elapsed.eq(pd.Timedelta(hours=2)).all())
        self.assertTrue(candidates["duration_minutes"].eq(120).all())
        self.assertTrue(candidates["duration_hours"].eq(2.0).all())
        self.assertTrue(
            candidates["broad_threshold_pips"]
            .eq(threshold["broad_threshold_pips"])
            .all()
        )

    def test_sparse_window_fails_full_coverage_requirement(self) -> None:
        bars = self._regular_five_minute_bars()
        regular_candidates, regular_thresholds = pipeline.detect_move_candidates(
            bars,
            "EUR_USD",
            self._config(),
        )
        excluded_start = bars.index[0]
        self.assertIn(excluded_start, set(regular_candidates["start_timestamp"]))

        sparse_bars = bars.copy()
        missing_timestamp = sparse_bars.index[1]
        sparse_bars.loc[
            missing_timestamp,
            ["close", "high", "low", "bid_close", "ask_close"],
        ] = float("nan")
        sparse_bars.loc[missing_timestamp, "source_m1_observations"] = 0

        sparse_candidates, sparse_thresholds = pipeline.detect_move_candidates(
            sparse_bars,
            "EUR_USD",
            self._config(minimum_window_bar_coverage=1.0),
        )

        self.assertEqual(regular_thresholds[0]["valid_windows"], 6)
        self.assertEqual(sparse_thresholds[0]["status"], "ok")
        self.assertEqual(sparse_thresholds[0]["valid_windows"], 4)
        self.assertNotIn(excluded_start, set(sparse_candidates["start_timestamp"]))

    def test_non_overlap_selection_keeps_highest_score_and_audits_rejections(
        self,
    ) -> None:
        origin = pd.Timestamp("2024-01-02T00:00:00Z")
        candidates = pd.DataFrame(
            [
                {
                    "move_id": "highest-up",
                    "instrument": "EUR_USD",
                    "start_timestamp": origin,
                    "end_timestamp": origin + pd.Timedelta(minutes=120),
                    "direction": "up",
                    "significance_score": 100.0,
                    "absolute_pip_change": 300.0,
                    "is_primary_horizon": True,
                    "strict_eligible": True,
                },
                {
                    "move_id": "lower-up",
                    "instrument": "EUR_USD",
                    "start_timestamp": origin + pd.Timedelta(minutes=15),
                    "end_timestamp": origin + pd.Timedelta(minutes=135),
                    "direction": "up",
                    "significance_score": 80.0,
                    "absolute_pip_change": 250.0,
                    "is_primary_horizon": True,
                    "strict_eligible": True,
                },
                {
                    "move_id": "lower-down",
                    "instrument": "EUR_USD",
                    "start_timestamp": origin + pd.Timedelta(minutes=30),
                    "end_timestamp": origin + pd.Timedelta(minutes=150),
                    "direction": "down",
                    "significance_score": 90.0,
                    "absolute_pip_change": 275.0,
                    "is_primary_horizon": True,
                    "strict_eligible": True,
                },
            ]
        )

        overlap_groups, deduplicated, final = (
            pipeline.select_non_overlapping_candidates(
                candidates,
                self._config(
                    overlap_group_minimum_fraction=0.3,
                    overlap_start_tolerance_minutes=45.0,
                    parent_trend_gap_minutes=15.0,
                    final_maximum_overlap_fraction=0.0,
                ),
            )
        )

        indexed = overlap_groups.set_index("move_id")
        self.assertEqual(
            indexed.at["highest-up", "overlap_group_id"],
            indexed.at["lower-up", "overlap_group_id"],
        )
        self.assertEqual(indexed.at["highest-up", "rank_within_overlap_group"], 1)
        self.assertEqual(indexed.at["lower-up", "rank_within_overlap_group"], 2)
        self.assertEqual(
            indexed.at["lower-up", "selection_reason"],
            "rejected_overlap_group_lower_score",
        )

        self.assertEqual(set(deduplicated["move_id"]), {"highest-up", "lower-down"})
        self.assertEqual(final["move_id"].tolist(), ["highest-up"])
        self.assertEqual(
            indexed.at["highest-up", "selection_reason"],
            "selected_strict_non_overlapping",
        )
        self.assertEqual(
            indexed.at["lower-down", "selection_reason"],
            "rejected_strict_interval_overlap",
        )
        self.assertEqual(indexed.at["lower-down", "rejected_by_move_id"], "highest-up")


if __name__ == "__main__":
    unittest.main()
