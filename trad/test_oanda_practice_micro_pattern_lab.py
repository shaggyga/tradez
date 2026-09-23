import json
import math
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trad.oanda_practice_micro_pattern_lab import (
    CROSS_EQUATION_FEATURES,
    EQUATION_FEATURES,
    MICRO_SPECS,
    PRUNED_MICRO_SPECS,
    MicroPatternModel,
    default_equation_state,
    equation_definition,
    update_equation,
)


class MicroPatternModelTests(unittest.TestCase):
    def test_every_quote_and_causal_prediction_is_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "micro.sqlite3"
            state = root / "state.json.gz"
            snapshot = root / "snapshot.json"
            model = MicroPatternModel(
                database,
                state,
                snapshot,
                {"EUR_USD": 0.0001},
                prior_count=5.0,
                min_pattern_count=2,
                min_baseline_count=2,
            )
            start = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)
            mid = 1.1000
            steps = (1.0, -1.0, 1.0, 2.0, -2.0, 1.0, -1.0, 2.0)
            for index in range(320):
                mid += steps[index % len(steps)] * 0.0001
                timestamp = (start + timedelta(milliseconds=250 * index)).isoformat().replace("+00:00", "Z")
                self.assertTrue(model.observe("EUR_USD", timestamp, mid - 0.00001, mid + 0.00001))
            summaries = model.summaries()
            self.assertEqual({row["model_id"] for row in summaries}, {spec.model_id for spec in MICRO_SPECS})
            self.assertTrue(all(row["matured"] > 0 for row in summaries))
            self.assertTrue(any(row["ready_matured"] > 0 for row in summaries))
            self.assertTrue(
                all(math.isfinite(row["movement_coefficient"]) for row in model.latest.values())
            )
            model.save_snapshot(active=True)
            payload = json.loads(snapshot.read_text(encoding="utf-8"))
            model.close()
            self.assertEqual(payload["received_quote_updates"], 320)
            self.assertEqual(len(payload["models"]), len(MICRO_SPECS))
            self.assertTrue(payload["latest_forecasts"])
            self.assertIn("intraminute.equation.boundary", {row["model_id"] for row in payload["models"]})
            self.assertTrue(payload["signal_correlation_matrix"]["cells"])
            self.assertEqual(len(payload["ruleset_leaderboard"]), 0)
            self.assertEqual(
                {row["model_id"] for row in payload["retired_models"]},
                {spec.model_id for spec in PRUNED_MICRO_SPECS},
            )

            connection = sqlite3.connect(database)
            quote_count = connection.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
            prediction_count = connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
            first_pattern_count = connection.execute(
                "SELECT pattern_count FROM predictions WHERE model_id='micro.sign7.1000ms' ORDER BY id LIMIT 1"
            ).fetchone()[0]
            matured_count = connection.execute(
                "SELECT COUNT(*) FROM predictions WHERE status='matured'"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(quote_count, 320)
            self.assertGreater(prediction_count, 2000)
            self.assertEqual(first_pattern_count, 0)
            self.assertGreater(matured_count, 500)

    def test_intraminute_forecast_targets_next_minute_boundary(self):
        boundary_spec = next(spec for spec in MICRO_SPECS if spec.model_id == "intraminute.sign3.boundary")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = MicroPatternModel(
                root / "micro.sqlite3",
                root / "state.json.gz",
                root / "snapshot.json",
                {"EUR_USD": 0.0001},
            )
            start = datetime(2026, 7, 14, 12, 0, 58, tzinfo=timezone.utc)
            mid = 1.1000
            for index in range(6):
                mid += 0.0001
                timestamp = (start + timedelta(milliseconds=250 * index)).isoformat().replace("+00:00", "Z")
                self.assertTrue(model.observe("EUR_USD", timestamp, mid - 0.00001, mid + 0.00001))
            forecast = model.latest["EUR_USD:intraminute.sign3.boundary"]
            self.assertEqual(forecast["target_kind"], boundary_spec.target)
            self.assertEqual(forecast["target_time"], "2026-07-14T12:01:00+00:00")
            self.assertLessEqual(forecast["target_horizon_ms"], 2000)
            model.close()

    def test_raw_history_retention_keeps_new_rows_and_cumulative_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "micro.sqlite3"
            model = MicroPatternModel(
                database,
                root / "state.json.gz",
                root / "snapshot.json",
                {"EUR_USD": 0.0001},
                retention_hours=1.0,
                prune_interval_sec=60.0,
            )
            start = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)
            mid = 1.1000
            for index in range(24):
                offset = timedelta(seconds=index) if index < 12 else timedelta(hours=2, seconds=index)
                mid += (1 if index % 2 else -1) * 0.0001
                timestamp = (start + offset).isoformat().replace("+00:00", "Z")
                self.assertTrue(model.observe("EUR_USD", timestamp, mid - 0.00001, mid + 0.00001))
            matured_before = sum(row["matured"] for row in model.summaries())
            now_ns = int((start + timedelta(hours=2, minutes=1)).timestamp() * 1_000_000_000)

            result = model.prune_history(force=True, now_ns=now_ns)
            model.database.commit()
            quote_count = model.database.execute("SELECT COUNT(*) FROM quotes").fetchone()[0]
            prediction_count = model.database.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]
            minimum_quote_ns = model.database.execute("SELECT MIN(time_ns) FROM quotes").fetchone()[0]
            cutoff_ns = result["cutoff_time_ns"]

            self.assertGreater(result["quotes_deleted"], 0)
            self.assertGreater(result["predictions_deleted"], 0)
            self.assertGreater(quote_count, 0)
            self.assertGreater(prediction_count, 0)
            self.assertGreaterEqual(minimum_quote_ns, cutoff_ns)
            self.assertEqual(sum(row["matured"] for row in model.summaries()), matured_before)
            model.close()

    def test_equation_forecast_logs_continuous_features(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = MicroPatternModel(
                root / "micro.sqlite3",
                root / "state.json.gz",
                root / "snapshot.json",
                {"EUR_USD": 0.0001},
                min_pattern_count=1,
                min_baseline_count=1,
            )
            start = datetime(2026, 7, 14, 12, 0, tzinfo=timezone.utc)
            mid = 1.1000
            for index in range(12):
                mid += (1 if index % 2 else -1) * 0.0001
                timestamp = (start + timedelta(milliseconds=250 * index)).isoformat().replace("+00:00", "Z")
                self.assertTrue(model.observe("EUR_USD", timestamp, mid - 0.00001, mid + 0.00001))
            forecast = model.latest["EUR_USD:equation.ewls.1000ms"]
            model.close()
            self.assertEqual(forecast["model"], "live_equation_forecast")
            self.assertEqual(set(forecast["equation_features"]), set(EQUATION_FEATURES))
            self.assertIn("legacy model id says ewls", forecast["equation_definition"]["notes"][0])
            self.assertTrue(math.isfinite(forecast["expected_signed_move_pips"]))
            retired_ids = {spec.model_id for spec in PRUNED_MICRO_SPECS}
            self.assertNotIn("equation.kmeans4.2000ms", {spec.model_id for spec in MICRO_SPECS})
            self.assertIn("equation.kmeans4.2000ms", retired_ids)
            cross_spec = next(spec for spec in PRUNED_MICRO_SPECS if spec.model_id == "equation.cross_relative.2000ms")
            cross_definition = equation_definition(cross_spec)
            self.assertTrue(set(CROSS_EQUATION_FEATURES).issubset(cross_definition["features"]))

    def test_huber_equation_clips_large_residual_update(self):
        features = {feature: 0.0 for feature in EQUATION_FEATURES}
        features["bias"] = 1.0
        normal = default_equation_state()
        robust = default_equation_state()

        update_equation(normal, features, 10.0, EQUATION_FEATURES, "nlms")
        update_equation(robust, features, 10.0, EQUATION_FEATURES, "huber")

        self.assertGreater(normal["weights"]["bias"], robust["weights"]["bias"])

    def test_duplicate_or_out_of_order_quote_time_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = MicroPatternModel(
                root / "micro.sqlite3",
                root / "state.json.gz",
                root / "snapshot.json",
                {"EUR_USD": 0.0001},
            )
            timestamp = "2026-07-14T12:00:00.000000000Z"
            self.assertTrue(model.observe("EUR_USD", timestamp, 1.1, 1.1001))
            self.assertFalse(model.observe("EUR_USD", timestamp, 1.1001, 1.1002))
            model.close()


if __name__ == "__main__":
    unittest.main()
