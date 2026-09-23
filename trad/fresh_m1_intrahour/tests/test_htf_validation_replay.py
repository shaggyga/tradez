from __future__ import annotations

import unittest

import pandas as pd

from fresh_m1_intrahour.src.common import load_config
from fresh_m1_intrahour.src.htf_corrected_rebuild import (
    MAX_ALIGNMENT_DELAY_MINUTES,
    UNSAFE_GLOBAL_FEATURES,
    _corrected_viability,
    _prediction_surface,
    _selection_rank_key,
    build_corrected_h1_labels,
    corrected_candidate_specs,
    corrected_feature_families,
)
from fresh_m1_intrahour.src.htf_validation_replay import (
    _cost_stress,
    _feature_timing_audit,
    _timestamp_ns,
    allocate_fixed_exposure,
    curve_path_window_minutes,
    decision_time_from_bar_start,
)


class HTFTimingTests(unittest.TestCase):
    def test_decision_time_is_after_completed_bar(self) -> None:
        raw = pd.Timestamp("2026-07-01T12:00:00Z")
        self.assertEqual(
            decision_time_from_bar_start(raw, "m30"),
            pd.Timestamp("2026-07-01T12:30:00Z"),
        )
        self.assertEqual(
            decision_time_from_bar_start(raw, "h4"),
            pd.Timestamp("2026-07-01T16:00:00Z"),
        )

    def test_archived_curve_path_window_bug_scales_with_timeframe(self) -> None:
        self.assertEqual(curve_path_window_minutes(120, "m30"), 720)
        self.assertEqual(curve_path_window_minutes(120, "h1"), 1440)
        self.assertEqual(curve_path_window_minutes(120, "h4"), 5760)

    def test_feature_audit_fails_original_timing_contract(self) -> None:
        audit = _feature_timing_audit()
        self.assertFalse(audit["full_timing_audit_passed"])
        self.assertTrue(audit["bar_timestamp_contract"]["original_replay_bar_completion_lookahead"])
        self.assertEqual(
            audit["path_label_contract"]["windows"]["h4"]["actual_path_window_hours"],
            96.0,
        )

    def test_timestamp_alignment_normalizes_microseconds_to_nanoseconds(self) -> None:
        values = pd.Series(pd.to_datetime(["2026-07-01T12:00:00Z"]))
        microseconds = values.astype("datetime64[us, UTC]")
        self.assertEqual(_timestamp_ns(values)[0], _timestamp_ns(microseconds)[0])
        self.assertGreater(_timestamp_ns(microseconds)[0], 10**18)


class CorrectedH1RebuildTests(unittest.TestCase):
    def test_feature_families_exclude_global_holdout_metadata(self) -> None:
        families = corrected_feature_families()
        self.assertEqual(len(families["safe_full_220"]), 220)
        self.assertEqual(len(families["motion_cost_117"]), 117)
        self.assertEqual(len(families["arima_causal"]), 50)
        self.assertEqual(len(families["safe_full_220_plus_arima"]), 270)
        self.assertEqual(
            families["safe_full_220_plus_arima"][:220],
            families["safe_full_220"],
        )
        self.assertFalse(UNSAFE_GLOBAL_FEATURES.intersection(families["safe_full_220"]))

    def test_candidate_grid_is_fixed_before_evaluation(self) -> None:
        specs = corrected_candidate_specs()
        self.assertEqual(len(specs), 24)
        self.assertEqual(len({spec["name"] for spec in specs}), 24)

    def test_viability_rejects_drawdown_and_pair_concentration(self) -> None:
        metrics = {
            "pnl_usd": 100.0,
            "trades": 200,
            "mean_net_pips": 0.5,
            "max_drawdown_pct": 40.0,
            "top_pair_trade_share": 0.55,
        }
        folds = [
            {"trades": 100, "pnl_usd": 60.0},
            {"trades": 100, "pnl_usd": 40.0},
        ]
        result = _corrected_viability(metrics, folds)
        self.assertTrue(result["viable_before_risk_concentration_checks"])
        self.assertFalse(result["viable"])
        self.assertEqual(result["passed_check_count"], 4)

    def test_nonviable_fallback_prefers_most_viability_checks_passed(self) -> None:
        mostly_sound = {
            "development": {"pnl_usd": 100.0, "mean_net_pips": 0.5},
            "viability": {
                "viable": False,
                "passed_check_count": 5,
                "positive_folds": 2,
            },
        }
        unstable = {
            "development": {"pnl_usd": -500.0, "mean_net_pips": -1.0},
            "viability": {
                "viable": False,
                "passed_check_count": 3,
                "positive_folds": 3,
            },
        }
        self.assertIs(max([unstable, mostly_sound], key=_selection_rank_key), mostly_sound)

    def test_corrected_label_path_is_exactly_120_minutes(self) -> None:
        cfg = load_config()
        raw_times = pd.to_datetime([
            "2025-06-02T08:00:00Z",
            "2025-06-02T09:00:00Z",
            "2025-06-02T10:00:00Z",
        ])
        rows = pd.DataFrame({
            "instrument": "EUR_USD",
            "raw_bar_time_utc": raw_times,
            "decision_time_utc": raw_times + pd.Timedelta(hours=1),
            "spread_pips": 1.2,
            "atr240_pips": 8.0,
            "momentum_30_atr": [0.5, -0.5, 0.2],
        })
        labeled, audit = build_corrected_h1_labels(rows, cfg, fixed_units=10_000)
        self.assertEqual(len(labeled), 3)
        durations = labeled["label_end_time_utc"] - labeled["entry_time_utc"]
        self.assertTrue((durations == pd.Timedelta(minutes=120)).all())
        self.assertTrue(audit["label_end_strictly_after_decision"])
        self.assertTrue(audit["all_alignment_delays_within_limit"])
        self.assertLessEqual(
            audit["endpoint_alignment_delay_max_minutes"],
            MAX_ALIGNMENT_DELAY_MINUTES,
        )
        self.assertFalse(audit["old_curve_targets_used"])

        surface = _prediction_surface(
            labeled,
            prediction=[0.7, 0.8, 0.9],
            spec={
                "name": "test",
                "feature_family": "test",
                "direction": "reversal",
                "objective": "curve_classifier",
            },
            fold="test",
        )
        self.assertTrue((surface["exit_time_utc"] == surface["label_end_time_utc"]).all())


class HTFAllocatorTests(unittest.TestCase):
    def test_allocator_enforces_quota_pair_and_exposure(self) -> None:
        rows = pd.DataFrame({
            "entry_time_utc": pd.to_datetime([
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:00:00Z",
                "2026-01-01T00:30:00Z",
                "2026-01-01T02:00:00Z",
            ]),
            "exit_time_utc": pd.to_datetime([
                "2026-01-01T02:00:00Z",
                "2026-01-01T02:00:00Z",
                "2026-01-01T02:00:00Z",
                "2026-01-01T02:30:00Z",
                "2026-01-01T04:00:00Z",
            ]),
            "instrument": ["EUR_USD", "EUR_USD", "GBP_USD", "USD_JPY", "USD_JPY"],
            "timeframe": ["m30", "h1", "m30", "m30", "m30"],
            "selected_score": [0.9, 0.8, 0.7, 0.95, 0.6],
        })
        selected, attribution = allocate_fixed_exposure(
            rows,
            max_new_per_timestamp=3,
            max_open_positions=2,
        )
        self.assertEqual(len(selected), 3)
        self.assertEqual(attribution["rejected_by_pair_overlap"], 1)
        self.assertEqual(attribution["rejected_by_exposure_cap"], 1)

    def test_cost_stress_uses_round_trip_cost_components(self) -> None:
        trades = pd.DataFrame({
            "selected_gross_pips": [4.0, 2.0],
            "selected_spread_drag_pips": [1.0, 1.0],
            "slippage_pips_round_trip": [0.2, 0.2],
            "pip_value_usd_per_unit": [0.0001, 0.0001],
            "fixed_units": [10000, 10000],
        })
        stress = _cost_stress(trades)
        self.assertAlmostEqual(stress["base_pnl_usd"], 3.6)
        by_name = {row["stress"]: row for row in stress["stress"]}
        self.assertAlmostEqual(by_name["zero_cost_fantasy"]["pnl_usd"], 6.0)
        self.assertAlmostEqual(by_name["spread_2x"]["pnl_usd"], 1.6)


if __name__ == "__main__":
    unittest.main()
