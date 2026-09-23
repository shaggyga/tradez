import unittest

from trad.oanda_strategy_representation_audit import build_audit


class StrategyRepresentationAuditTests(unittest.TestCase):
    def test_exit_fit_blockers_distinguish_negative_evidence_from_missing_fit(self):
        scope = {
            "family": "momentum",
            "sample_count": 120,
            "eligible": False,
            "blocked_by": ["holdout_edge", "holdout_confidence"],
            "holdout": {
                "avg_pips": -0.4,
                "lower_confidence_pips": -0.9,
            },
        }
        audit = build_audit(
            families=("momentum",),
            default_families=("momentum",),
            profiles=("fast",),
            promotion={"signal_evidence": []},
            exit_fit={
                "counts": {"fit_signal_rows": 120},
                "eligible": {},
                "horizon_states": {
                    "300": {
                        "global": {
                            "sample_count": 120,
                            "eligible": False,
                            "blocked_by": ["holdout_edge"],
                            "holdout": {
                                "avg_pips": -0.2,
                                "lower_confidence_pips": -0.5,
                                "profit_factor": 0.9,
                            },
                        },
                        "top_families": [scope],
                    }
                },
            },
            calibration={},
            pattern_summary={},
        )
        self.assertEqual(audit["counts"]["exit_global_horizons"], 1)
        self.assertEqual(
            audit["counts"]["exit_global_positive_holdout_horizons"], 0
        )
        self.assertEqual(
            audit["exit_fit_blockers"]["counts"]["holdout_confidence"],
            1,
        )
        self.assertEqual(
            len(audit["exit_fit_blockers"]["closest_sampled_scopes"][0]["blocked_by"]),
            2,
        )

    def test_combination_rule_depth_is_separated_from_feature_domain_breadth(self):
        audit = build_audit(
            families=("momentum",),
            default_families=("momentum",),
            profiles=("fast",),
            promotion={"eligible_lane_count": 0, "signal_evidence": []},
            exit_fit={"counts": {}, "eligible": {}},
            calibration={"surface_count": 0, "ready_surface_count": 0},
            pattern_summary={},
            combination_artifacts={
                "standard": {
                    "rules": [
                        {
                            "condition_count": 3,
                            "conditions": [
                                {"feature": "atr_m1_pips"},
                                {"feature": "live_spread_atr"},
                                {"feature": "ema_gap_m15_5_13"},
                            ],
                        }
                    ]
                }
            },
        )
        row = audit["combination_feature_domains"]["artifacts"][0]
        self.assertEqual(row["raw_condition_count"], 3)
        self.assertEqual(row["independent_feature_domain_count"], 2)
        self.assertEqual(row["rules_with_same_domain_redundancy"], 1)
        self.assertEqual(
            audit["counts"]["combination_rules_with_same_domain_redundancy"],
            1,
        )

    def test_nominal_lanes_are_separated_from_observed_and_validated_coverage(self):
        audit = build_audit(
            families=("momentum", "bollinger_reversion", "ahl_multihorizon_trend"),
            default_families=("momentum", "bollinger_reversion"),
            profiles=("strict", "fast"),
            promotion={
                "eligible_lane_count": 0,
                "signal_evidence": [
                    {
                        "family": "momentum",
                        "lane_id": "momentum.fast",
                        "horizon_sec": 300,
                        "sample_count": 120,
                        "pair_count": 8,
                        "eligible": False,
                        "blocked_by": ["time_block_stability"],
                    },
                    {
                        "family": "bollinger_reversion",
                        "lane_id": "bollinger_reversion.fast",
                        "horizon_sec": 300,
                        "sample_count": 80,
                        "pair_count": 5,
                        "eligible": False,
                        "blocked_by": ["minimum_samples", "holdout_confidence"],
                    },
                ],
            },
            exit_fit={"counts": {"fit_signal_rows": 500}, "eligible": {"families": 0}},
            calibration={"surface_count": 100, "ready_surface_count": 1},
            pattern_summary={"combinations_tested": 80, "replicated_profitable_count": 0},
        )

        self.assertEqual(audit["counts"]["catalogue_families"], 3)
        self.assertEqual(audit["counts"]["independent_archetypes"], 2)
        self.assertEqual(audit["counts"]["expected_lanes"], 5)
        self.assertEqual(audit["counts"]["observed_expected_lanes"], 2)
        self.assertEqual(audit["counts"]["promotion_eligible_rows"], 0)
        self.assertEqual(audit["counts"]["exit_eligible_scopes"], 0)
        self.assertEqual(
            audit["promotion_blockers"]["counts"]["time_block_stability"],
            1,
        )
        self.assertEqual(audit["counts"]["promotion_rows_one_blocker"], 1)
        self.assertEqual(audit["counts"]["promotion_rows_two_blockers"], 1)
        self.assertEqual(
            audit["promotion_blockers"]["near_eligible_evidence"][0]["family"],
            "momentum",
        )
        archetypes = {
            row["archetype"]: row for row in audit["archetypes"]
        }
        trend = archetypes["trend_momentum"]
        self.assertEqual(trend["expected_lanes"], 3)
        self.assertEqual(trend["observed_lanes"], 1)
        self.assertEqual(trend["observed_lane_pct"], 33.333)
        reversion = archetypes["mean_reversion"]
        self.assertEqual(reversion["expected_lanes"], 2)
        self.assertEqual(reversion["observed_lanes"], 1)
        self.assertIn(
            "event_fundamental_strategy",
            {row["gap"] for row in audit["gaps"]},
        )


if __name__ == "__main__":
    unittest.main()
