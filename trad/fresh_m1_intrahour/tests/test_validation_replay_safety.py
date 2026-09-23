from __future__ import annotations

import unittest

import pandas as pd

from fresh_m1_intrahour.src.features import (
    allowed_numeric_feature_columns,
    assert_decision_time_feature_columns,
    build_feature_family_report,
)
from fresh_m1_intrahour.src.movement_first import add_movement_targets
from fresh_m1_intrahour.src.research_suite import SplitData
from fresh_m1_intrahour.src.validation_replay import (
    _candidate_specs,
    _development_walk_forward_splits,
    _signal_metric,
)


class FeatureLeakageTests(unittest.TestCase):
    def test_future_and_generated_economics_columns_are_excluded(self) -> None:
        frame = pd.DataFrame({
            "return_5m_pips": [1.0, 2.0],
            "atr_15m_pips": [2.0, 3.0],
            "abs_move_pips_21m": [8.0, 9.0],
            "future_range_pips_21m": [10.0, 11.0],
            "actual_ev_21m": [0.2, -0.3],
            "target_signed_return_21m": [0.2, -0.3],
            "gross_mid_ev_21m": [0.5, 0.1],
            "cost_drag_ev_21m": [0.3, 0.4],
        })
        allowed = allowed_numeric_feature_columns(frame)
        self.assertEqual(allowed, ["atr_15m_pips", "return_5m_pips"])
        report = build_feature_family_report(frame)
        self.assertTrue(report["no_feature_leakage"])
        self.assertIn("abs_move_pips_21m", report["excluded_label_like_columns"])
        self.assertNotIn("abs_move_pips_21m", report["approved_numeric_feature_columns"])

    def test_feature_assertion_fails_closed(self) -> None:
        assert_decision_time_feature_columns(["return_5m_pips", "spread_pips"])
        with self.assertRaises(ValueError):
            assert_decision_time_feature_columns(["return_5m_pips", "abs_move_pips_21m"])

    def test_generated_movement_target_cannot_become_a_feature(self) -> None:
        frame = pd.DataFrame({
            "future_range_pips_13m": [2.0, 8.0],
            "round_trip_cost_pips_used": [1.0, 1.0],
            "return_5m_pips": [0.1, 0.2],
        })
        enriched = add_movement_targets(frame, [13], [1.0])
        target = "target_move_exceeds_cost_plus_1p0_13m"
        self.assertEqual(enriched[target].tolist(), [0.0, 1.0])
        self.assertNotIn(target, allowed_numeric_feature_columns(enriched))


class ReplayProtocolTests(unittest.TestCase):
    @staticmethod
    def _rows(start: str, periods: int) -> pd.DataFrame:
        timestamps = pd.date_range(start, periods=periods, freq="min", tz="UTC")
        return pd.DataFrame({
            "decision_time_utc": timestamps,
            "pair": "EUR_USD",
            "side": "long",
            "actual_ev_13m": 0.0,
        })

    def test_walk_forward_development_never_uses_outer_final(self) -> None:
        train = self._rows("2026-06-01", 180)
        validation = self._rows("2026-06-02", 120)
        final = self._rows("2026-06-03", 60)
        outer = SplitData(train, validation, final, {})
        _, folds, _ = _development_walk_forward_splits(
            outer,
            {"surface": {"purge_embargo_minutes": 0}},
            folds=3,
        )
        final_start = final["decision_time_utc"].min()
        self.assertGreaterEqual(len(folds), 2)
        self.assertTrue(all(fold.validation["decision_time_utc"].max() < final_start for fold in folds))

    def test_movement_summary_uses_value_units_not_pnl_labels(self) -> None:
        frame = pd.DataFrame({
            "decision_time_utc": pd.to_datetime([
                "2026-06-01T00:00:00Z",
                "2026-06-01T00:00:00Z",
                "2026-06-01T00:01:00Z",
                "2026-06-01T00:01:00Z",
            ]),
            "score": [0.9, 0.1, 0.8, 0.2],
            "future_range_pips_13m": [4.0, 2.0, 6.0, 1.0],
        })
        result = _signal_metric(frame, "score", "future_range_pips_13m", "movement", "pips")
        self.assertEqual(result["top1"]["unit"], "pips")
        self.assertEqual(result["top1"]["actual_value_sum"], 10.0)
        self.assertNotIn("pnl_account", result["top1"])

    def test_candidate_grid_contains_old_and_movement_first_families(self) -> None:
        names = {spec["candidate"] for spec in _candidate_specs()}
        self.assertIn("Extra Trees|I_full|TP_before_SL_proxy|21m", names)
        self.assertTrue(any(name.startswith("movement_first|") for name in names))


if __name__ == "__main__":
    unittest.main()
