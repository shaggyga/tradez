import tempfile
import time
import unittest
from pathlib import Path

try:
    from oanda_signal_contribution_feed import SignalContributionFeed
except ModuleNotFoundError:
    from trad.oanda_signal_contribution_feed import SignalContributionFeed


class SignalContributionFeedTests(unittest.TestCase):
    def test_registers_complete_model_gap_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            coverage = feed.coverage()
            auto_checkpoint = feed.connection.execute(
                "PRAGMA wal_autocheckpoint"
            ).fetchone()[0]
            feed.close()

        self.assertEqual(auto_checkpoint, 0)
        self.assertEqual(coverage["model_gap"]["registered"], 30)
        self.assertEqual(coverage["model_gap"]["adapter_implemented"], 30)
        self.assertEqual(coverage["model_gap"]["fresh_live_contributors"], 0)
        self.assertTrue(
            coverage["contract"][
                "all_fresh_finite_predictions_enter_research_consensus"
            ]
        )

    def test_model_forecast_is_live_research_evidence_not_historical_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            result = feed.publish_forecasts(
                [
                    {
                        "model_id": "ngboost",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M5",
                        "generated_epoch": time.time(),
                        "account_eligible": True,
                        "forecast_curve": {
                            "300": {
                                "probability_up": 0.61,
                                "predicted_signed_pips": 1.4,
                                "predicted_magnitude_pips": 2.3,
                            },
                            "3600": {
                                "probability_up": 0.57,
                                "predicted_signed_pips": 3.2,
                            },
                        },
                    }
                ],
                "ngboost-live",
                30.0,
            )
            rows = feed.recent()
            coverage = feed.coverage()
            feed.close()

        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["rejected"], 0)
        publish_latency = result["maintenance"]["latency_ms"]
        self.assertEqual(
            set(publish_latency),
            {
                "normalize",
                "register",
                "candidate_upsert",
                "registry_update",
                "maintenance",
                "commit",
                "total",
            },
        )
        self.assertGreaterEqual(publish_latency["total"], 0.0)
        self.assertEqual(len(rows), 1)
        candidate = rows[0]
        self.assertEqual(set(candidate["forecast_curve"]), {"300", "3600"})
        self.assertEqual(
            candidate["forecast_curve"]["300"]["predicted_magnitude_pips"],
            2.3,
        )
        self.assertFalse(candidate["account_eligible"])
        self.assertTrue(candidate["research_only"])
        policy = candidate["signal_provenance"]["policy"]
        self.assertFalse(policy["historical_report_used_as_live_value"])
        self.assertEqual(coverage["model_gap"]["fresh_live_contributors"], 1)

    def test_forecast_curve_list_uses_the_same_validated_contract(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            result = feed.publish_forecasts(
                [
                    {
                        "model_id": "move_alert.causal_continuation_state",
                        "family": "move_alert_causal_state",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M1",
                        "generated_epoch": time.time(),
                        "account_eligible": False,
                        "forecast_curve": [
                            {
                                "horizon_sec": 300,
                                "direction": "buy",
                                "probability_up": 0.56,
                                "predicted_signed_pips": 1.2,
                                "account_eligible": False,
                            },
                            {
                                "horizon_sec": 900,
                                "direction": "buy",
                                "probability_up": 0.54,
                                "predicted_signed_pips": 1.8,
                                "account_eligible": False,
                            },
                        ],
                    }
                ],
                "move-alert-live",
                30.0,
            )
            recent = feed.recent()
            feed.close()

        self.assertEqual(result["accepted"], 1)
        self.assertEqual(result["rejected"], 0)
        self.assertEqual(set(recent[0]["forecast_curve"]), {"300", "900"})
        self.assertTrue(recent[0]["research_only"])
        self.assertFalse(recent[0]["account_eligible"])

    def test_stale_and_invalid_forecasts_are_audited_but_not_ranked(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            result = feed.publish_forecasts(
                [
                    {
                        "model_id": "catboost",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M1",
                        "generated_epoch": time.time() - 600.0,
                        "horizon_sec": 300,
                        "probability_up": 0.60,
                    },
                    {
                        "model_id": "catboost",
                        "instrument": "EURUSD",
                        "input_timeframe": "M1",
                        "horizon_sec": 300,
                        "probability_up": 0.60,
                    },
                ],
                "catboost-live",
                30.0,
                max_age_sec=60.0,
            )
            coverage = feed.coverage()
            recent = feed.recent()
            feed.close()

        self.assertEqual(result["accepted"], 0)
        self.assertEqual(result["rejected"], 2)
        self.assertEqual(recent, [])
        reasons = {
            row["reason"]: row["count"]
            for row in coverage["audit_24h"]
            if row["status"] == "rejected"
        }
        self.assertEqual(reasons["stale_forecast"], 1)
        self.assertEqual(reasons["invalid_instrument"], 1)

    def test_unregistered_model_cannot_self_authorize_account_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            result = feed.publish_forecasts(
                [
                    {
                        "model_id": "custom_gpt_checkpoint",
                        "family": "gpt_forecast",
                        "instrument": "GBP_JPY",
                        "input_timeframe": "H1",
                        "account_eligible": True,
                        "horizon_sec": 14400,
                        "probability_up": 0.40,
                        "predicted_signed_pips": -12.0,
                    }
                ],
                "gpt-checkpoint",
                30.0,
            )
            candidate = feed.recent()[0]
            feed.close()

        self.assertEqual(result["accepted"], 1)
        self.assertFalse(candidate["account_eligible"])
        self.assertEqual(candidate["research_blocked_reason"], "model_production_policy")

    def test_unregistered_forecast_batch_uses_one_registry_transaction(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            register_calls = 0
            original_register = feed.register_contributors

            def counted_register(rows):
                nonlocal register_calls
                register_calls += 1
                return original_register(rows)

            feed.register_contributors = counted_register
            result = feed.publish_forecasts(
                [
                    {
                        "model_id": f"new_model_{index}",
                        "family": "batch_test",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M1",
                        "generated_epoch": time.time(),
                        "horizon_sec": 60,
                        "probability_up": 0.55,
                    }
                    for index in range(3)
                ],
                "batch-test",
                30.0,
            )
            feed.close()

        self.assertEqual(result["accepted"], 3)
        self.assertEqual(register_calls, 1)

    def test_practice_promotion_is_enforced_per_curve_point(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "signals.sqlite"
            feed = SignalContributionFeed(path)
            feed.set_contributor_account_eligibility(
                {"catboost": True},
                account_scope="practice_007_only",
                policy_status="active",
            )
            candidate = feed.normalize_forecast(
                {
                    "model_id": "catboost",
                    "instrument": "EUR_USD",
                    "input_timeframe": "M1",
                    "generated_epoch": time.time(),
                    "account_eligible": True,
                    "forecast_curve": {
                        "300": {
                            "probability_up": 0.60,
                            "account_eligible": True,
                            "predictor_promotion_evidence": {"eligible": True},
                        },
                        "3600": {
                            "probability_up": 0.58,
                            "account_eligible": False,
                        },
                    },
                },
                "promotion-unit",
            )
            feed.close()

            reopened = SignalContributionFeed(path)
            preserved = reopened._policy("catboost")
            reopened.close()

        self.assertTrue(candidate["account_eligible"])
        self.assertTrue(candidate["forecast_curve"]["300"]["account_eligible"])
        self.assertFalse(candidate["forecast_curve"]["3600"]["account_eligible"])
        self.assertTrue(
            candidate["forecast_curve"]["300"]["predictor_promotion_evidence"][
                "eligible"
            ]
        )
        self.assertTrue(preserved["account_eligible"])

    def test_latest_model_snapshot_replaces_feed_row_and_audit_is_rolled_up(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            generated = time.time()
            first = feed.publish_forecasts(
                [
                    {
                        "model_id": "catboost",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M1",
                        "generated_epoch": generated,
                        "horizon_sec": 300,
                        "probability_up": 0.60,
                    }
                ],
                "model-live",
                30.0,
            )
            second = feed.publish_forecasts(
                [
                    {
                        "model_id": "catboost",
                        "instrument": "EUR_USD",
                        "input_timeframe": "M1",
                        "generated_epoch": generated + 0.1,
                        "horizon_sec": 300,
                        "probability_up": 0.62,
                    }
                ],
                "model-live",
                30.0,
            )
            rows = feed.recent()
            coverage = feed.coverage()
            feed.close()

        self.assertNotEqual(first["candidate_ids"], second["candidate_ids"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["probability_up"], 0.62)
        accepted = next(
            row
            for row in coverage["audit_24h"]
            if row["status"] == "accepted"
        )
        self.assertEqual(accepted["count"], 2)
        self.assertTrue(coverage["contract"]["audit_payloads_are_minute_rollups"])

    def test_expired_candidate_maintenance_is_bounded(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(
                Path(temporary) / "signals.sqlite",
                maintenance_batch_size=2,
            )
            now = time.time()
            feed.connection.executemany(
                """
                INSERT INTO candidates(
                    candidate_id, published_epoch, expires_epoch, source, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (f"expired-{index}", now - 1000.0, now - 500.0, "test", "{}")
                    for index in range(5)
                ],
            )
            feed.connection.commit()
            maintenance = feed.publish([], "test", 30.0)
            remaining = feed.connection.execute(
                "SELECT COUNT(*) FROM candidates WHERE expires_epoch < ?",
                (now - 60.0,),
            ).fetchone()[0]
            auto_vacuum = feed.connection.execute("PRAGMA auto_vacuum").fetchone()[0]
            feed.close()

        self.assertEqual(maintenance["candidate_rows_deleted"], 2)
        self.assertEqual(remaining, 3)
        self.assertEqual(auto_vacuum, 2)

    def test_publish_compacts_only_reconstructable_curve_fields(self):
        with tempfile.TemporaryDirectory() as temporary:
            feed = SignalContributionFeed(Path(temporary) / "signals.sqlite")
            feed.publish(
                [
                    {
                        "id": "curve",
                        "instrument": "EUR_USD",
                        "account_eligible": True,
                        "forecast_curve": {
                            "300": {
                                "horizon_sec": 300,
                                "direction": "buy",
                                "probability_up": 0.61,
                                "calibrated_probability_up": 0.61,
                                "direction_threshold": 0.5,
                                "predicted_signed_pips": 1.4,
                                "predicted_magnitude_pips": 2.3,
                                "projected_net_pips": 1.1,
                                "account_eligible": True,
                                "research_only": False,
                                "filter_reasons": [],
                            },
                            "3600": {
                                "horizon_sec": 3600,
                                "direction": "sell",
                                "probability_up": 0.43,
                                "account_eligible": False,
                                "filter_reasons": ["validation_pending"],
                            },
                        },
                        "signal_provenance": {
                            "producer": "unit",
                            "valid_horizons_sec": [300, 3600],
                        },
                    }
                ],
                "unit",
                30.0,
            )
            candidate = feed.recent()[0]
            feed.close()

        first = candidate["forecast_curve"]["300"]
        self.assertNotIn("horizon_sec", first)
        self.assertNotIn("direction", first)
        self.assertNotIn("projected_net_pips", first)
        self.assertNotIn("research_only", first)
        self.assertNotIn("filter_reasons", first)
        self.assertEqual(first["predicted_magnitude_pips"], 2.3)
        self.assertNotIn("account_eligible", first)
        self.assertNotIn("calibrated_probability_up", first)
        self.assertNotIn("direction_threshold", first)
        self.assertTrue(candidate["account_eligible"])
        second = candidate["forecast_curve"]["3600"]
        self.assertFalse(second["account_eligible"])
        self.assertEqual(second["filter_reasons"], ["validation_pending"])
        self.assertNotIn(
            "valid_horizons_sec",
            candidate["signal_provenance"],
        )


if __name__ == "__main__":
    unittest.main()
