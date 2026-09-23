import unittest

from trad.oanda_signal_engine_refresh_audit import (
    build_audit,
    preserve_validated_audit_if_contract_unchanged,
)


class SignalEngineRefreshAuditTests(unittest.TestCase):
    def test_live_state_and_random_probe_ids_do_not_churn_frozen_audit(self):
        previous = {
            "generated_utc": "2026-07-21T21:41:55+00:00",
            "counts": {"model_gap_models": 30},
            "contract_probe": {"accepted": 30, "candidate_ids": ["old"]},
            "live_model_gap_worker": {"source": "worker.py", "state": {}},
        }
        current = {
            "generated_utc": "2026-07-21T22:25:46+00:00",
            "counts": {"model_gap_models": 30},
            "contract_probe": {"accepted": 30, "candidate_ids": ["new"]},
            "live_model_gap_worker": {
                "source": "worker.py",
                "state": {"cycles": 900, "published_forecasts": 1200},
            },
        }

        stable = preserve_validated_audit_if_contract_unchanged(current, previous)

        self.assertIs(stable, previous)

    def test_contract_change_replaces_frozen_audit(self):
        previous = {"counts": {"model_gap_models": 30}}
        current = {"counts": {"model_gap_models": 31}}

        self.assertIs(
            preserve_validated_audit_if_contract_unchanged(current, previous),
            current,
        )

    def test_full_signal_surface_and_model_routes_validate(self):
        report = build_audit()

        self.assertTrue(report["validation_passed"])
        self.assertEqual(report["counts"]["strategy_parameter_lanes"], 209)
        self.assertEqual(report["counts"]["canonical_horizons"], 15)
        self.assertEqual(report["counts"]["model_gap_models"], 30)
        self.assertEqual(report["counts"]["live_artifact_models_supported"], 4)
        self.assertEqual(report["counts"]["live_artifact_matched_baselines"], 2)
        self.assertEqual(report["counts"]["model_gap_live_artifact_producers"], 2)
        self.assertEqual(
            report["counts"]["model_gap_adapter_only_pending_live_producer"],
            28,
        )
        self.assertEqual(report["counts"]["model_gap_implementation_pending"], 0)
        self.assertEqual(
            report["counts"]["model_gap_shadow_only_without_verified_live_artifact"],
            28,
        )
        self.assertEqual(report["counts"]["canonical_timeframe_horizon_cells"], 195)
        self.assertEqual(report["contract_probe"]["accepted"], 30)
        self.assertEqual(report["contract_probe"]["account_eligible"], 0)
        self.assertEqual(
            {row["model_id"] for row in report["live_artifact_routes"]},
            {
                "logistic_baseline",
                "hist_gradient_boosting",
                "catboost",
                "ngboost",
            },
        )
        self.assertTrue(
            all(not row["account_eligible"] for row in report["live_artifact_routes"])
        )
        self.assertEqual(
            report["model_report"]["path"],
            "trad/data/oanda_training_manager/reports/modern_model_gap/"
            "unified_model_gap_market_latest.json",
        )
        self.assertTrue(
            all(
                str(row["adapter_source"]).startswith("trad/")
                for row in report["model_gap_routes"]
            )
        )


if __name__ == "__main__":
    unittest.main()
