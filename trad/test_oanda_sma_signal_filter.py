import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier

from trad.oanda_practice_shadow_strategy_lab import PracticeExecutor
from trad.oanda_sma_signal_filter import (
    SMA_FEATURE_NAMES,
    SmaSignalFilterModel,
    SmaSignalFilterStore,
    build_sma_signal_vector,
    snapshot_identity,
    validated_filter_weight,
)
from trad.oanda_sma_signal_filter_fit import (
    chronological_split,
    completed_close_series,
    snapshot_feature_matrix,
)


def feature_fixture() -> dict[str, object]:
    rising = [1.1000 + index * 0.0001 for index in range(400)]
    return {
        "pip": 0.0001,
        "closes": rising,
        "m5_closes": rising,
        "m10_closes": rising,
        "m15_closes": rising,
        "m30_closes": rising,
        "h1_closes": rising,
        "h2_closes": rising,
        "h3_closes": rising,
        "h4_closes": rising,
    }


class SmaFeatureTests(unittest.TestCase):
    def test_feature_net_is_wide_and_direction_conditioned(self):
        buy = build_sma_signal_vector(feature_fixture(), "buy", spread_pips=1.2)
        sell = build_sma_signal_vector(feature_fixture(), "sell", spread_pips=1.2)

        self.assertGreaterEqual(len(SMA_FEATURE_NAMES), 600)
        self.assertEqual(set(buy), set(sell))
        self.assertAlmostEqual(
            buy["sma__M1__p20__distance_atr"],
            -sell["sma__M1__p20__distance_atr"],
        )
        self.assertAlmostEqual(
            buy["sma__H4__p8_21__gap_atr"],
            -sell["sma__H4__p8_21__gap_atr"],
        )
        self.assertEqual(
            buy["sma__M15__p5_8__cross_age_log"],
            sell["sma__M15__p5_8__cross_age_log"],
        )

    def test_resampled_bar_is_unavailable_until_it_closes(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        frame = pd.DataFrame(
            {
                "time": [start + timedelta(minutes=index) for index in range(10)],
                "close": np.arange(10, dtype=float),
            }
        )
        values, available_ns = completed_close_series(frame, 5)

        self.assertEqual(values.tolist(), [4.0, 9.0])
        self.assertEqual(
            pd.Timestamp(available_ns[0], tz="UTC"),
            pd.Timestamp(start + timedelta(minutes=5)),
        )
        before_close = pd.Timestamp(start + timedelta(minutes=4, seconds=59)).value
        at_close = pd.Timestamp(start + timedelta(minutes=5)).value
        self.assertEqual(np.searchsorted(available_ns, before_close, side="right") - 1, -1)
        self.assertEqual(np.searchsorted(available_ns, at_close, side="right") - 1, 0)

    def test_chronological_split_purges_training_and_validation_edges(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        times = pd.Series(
            [start + timedelta(minutes=index) for index in range(100)]
        )
        split = chronological_split(times, 300)
        train_times = times[split.train]
        validation_times = times[split.validation]

        self.assertTrue((train_times <= split.train_end - timedelta(seconds=300)).all())
        self.assertTrue(
            (
                validation_times
                <= split.validation_end - timedelta(seconds=300)
            ).all()
        )
        self.assertFalse(np.any(split.train & split.validation))
        self.assertFalse(np.any(split.validation & split.holdout))

    def test_historical_extractor_populates_fresh_and_rejects_stale_rows(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start = datetime(2026, 1, 1, tzinfo=timezone.utc)
            candles = pd.DataFrame(
                {
                    "time": [
                        (start + timedelta(minutes=index)).isoformat().replace(
                            "+00:00",
                            "Z",
                        )
                        for index in range(400)
                    ],
                    "close": [
                        1.1000 + index * 0.00001 for index in range(400)
                    ],
                }
            )
            candles.to_parquet(root / "EUR_USD_M1.parquet", index=False)
            fresh_time = start + timedelta(minutes=399)
            stale_time = start + timedelta(days=1)
            outcomes = pd.DataFrame(
                {
                    "snapshot_key": ["fresh", "stale"],
                    "instrument": ["EUR_USD", "EUR_USD"],
                    "entry_minute": [fresh_time, stale_time],
                    "direction": ["buy", "buy"],
                    "pip": [0.0001, 0.0001],
                    "entry_spread_pips": [1.0, 1.0],
                }
            )

            snapshots, matrix, summary = snapshot_feature_matrix(
                outcomes,
                root,
                lookback_days=7,
            )

        rows = dict(
            zip(
                snapshots["snapshot_key"],
                snapshots["feature_row"],
            )
        )
        self.assertTrue(np.any(np.isfinite(matrix[int(rows["fresh"])])))
        self.assertFalse(np.any(np.isfinite(matrix[int(rows["stale"])])))
        self.assertEqual(summary["EUR_USD"]["populated"], 1)


class SmaStoreAndModelTests(unittest.TestCase):
    def test_store_normalizes_shared_market_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "sma.sqlite"
            store = SmaSignalFilterStore(path)
            vector = build_sma_signal_vector(
                feature_fixture(),
                "buy",
                spread_pips=1.0,
            )
            common = {
                "instrument": "EUR_USD",
                "origin_time": "2026-01-01T00:00:00Z",
                "direction": "buy",
                "family": "momentum",
                "profile": "fast",
                "kind": "signal",
                "entry_time": "2026-01-01T00:01:00Z",
                "entry_spread_pips": 1.0,
                "features": vector,
            }
            first = store.register_candidate(
                event_id="one",
                lane_id="momentum.fast",
                **common,
            )
            second = store.register_candidate(
                event_id="two",
                lane_id="momentum.loose",
                **common,
            )
            store.flush(force=True)
            store.observe("one", 300, "2026-01-01T00:06:00Z", 1.2, 2.0, 0.5)
            store.flush(force=True)
            store.close()

            connection = sqlite3.connect(path)
            counts = (
                connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0],
                connection.execute("SELECT COUNT(*) FROM candidates").fetchone()[0],
                connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0],
            )
            connection.close()

        self.assertEqual(first, second)
        self.assertEqual(
            first,
            snapshot_identity(
                "EUR_USD",
                "2026-01-01T00:00:00Z|2026-01-01T00:01:00Z",
                "buy",
            ),
        )
        self.assertEqual(counts, (1, 2, 1))

    def test_locked_flush_retains_buffer_for_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "sma.sqlite"
            store = SmaSignalFilterStore(path, busy_timeout_ms=100)
            vector = build_sma_signal_vector(
                feature_fixture(),
                "buy",
                spread_pips=1.0,
            )
            store.register_candidate(
                event_id="locked",
                instrument="EUR_USD",
                origin_time="2026-01-01T00:00:00Z",
                direction="buy",
                lane_id="momentum.fast",
                family="momentum",
                profile="fast",
                kind="signal",
                entry_time="2026-01-01T00:01:00Z",
                entry_spread_pips=1.0,
                features=vector,
            )
            blocker = sqlite3.connect(path)
            blocker.execute("BEGIN IMMEDIATE")
            try:
                self.assertFalse(store.flush(force=True))
                self.assertEqual(len(store.pending_candidates), 1)
                self.assertEqual(store.flush_busy_count, 1)
            finally:
                blocker.rollback()
                blocker.close()

            self.assertTrue(store.flush(force=True))
            self.assertEqual(len(store.pending_candidates), 0)
            store.close()

            connection = sqlite3.connect(path)
            count = connection.execute(
                "SELECT COUNT(*) FROM candidates"
            ).fetchone()[0]
            connection.close()

        self.assertEqual(count, 1)

    def test_model_hot_loads_and_emits_horizon_curve(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "filter.joblib"
            estimator = DummyClassifier(strategy="constant", constant=1)
            estimator.fit([[0.0], [1.0]], [0, 1])
            joblib.dump(
                {
                    "generated_at": "2026-01-01T00:00:00Z",
                    "models": {
                        "300": {
                            "estimator": estimator,
                            "feature_names": ["sma__context__spread_atr"],
                            "model_name": "dummy",
                            "score_kind": "probability",
                            "threshold": 0.8,
                            "account_eligible": True,
                            "holdout": {"average_net_pips": 1.0},
                        }
                    },
                },
                path,
            )
            forecast = SmaSignalFilterModel(path).predict(
                {"sma__context__spread_atr": 0.5}
            )

        self.assertTrue(forecast["ready"])
        self.assertTrue(forecast["curve"]["300"]["accepted"])
        self.assertEqual(forecast["account_eligible_horizons"], [300])

    def test_model_batches_multiple_market_vectors(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "filter.joblib"
            estimator = DummyClassifier(strategy="prior")
            estimator.fit([[0.0], [1.0], [2.0]], [0, 1, 1])
            joblib.dump(
                {
                    "generated_at": "2026-01-01T00:00:00Z",
                    "models": {
                        "300": {
                            "estimator": estimator,
                            "feature_names": ["sma__context__spread_atr"],
                            "model_name": "dummy",
                            "score_kind": "probability",
                            "threshold": 0.5,
                            "account_eligible": False,
                        }
                    },
                },
                path,
            )
            model = SmaSignalFilterModel(path)
            forecasts = model.predict_many(
                {
                    ("EUR_USD", "buy"): {
                        "sma__context__spread_atr": 0.5
                    },
                    ("GBP_USD", "sell"): {
                        "sma__context__spread_atr": 1.5
                    },
                }
            )

        self.assertEqual(
            set(forecasts),
            {("EUR_USD", "buy"), ("GBP_USD", "sell")},
        )
        self.assertTrue(
            all(forecast["ready"] for forecast in forecasts.values())
        )
        self.assertEqual(
            forecasts[("EUR_USD", "buy")]["curve"]["300"],
            forecasts[("GBP_USD", "sell")]["curve"]["300"],
        )

    def test_unvalidated_filter_is_neutral_and_validated_reject_is_not(self):
        shadow = {
            "curve": {
                "300": {
                    "score": 0.1,
                    "threshold": 0.7,
                    "account_eligible": False,
                }
            }
        }
        reject = {
            "curve": {
                "300": {
                    "score": 0.1,
                    "threshold": 0.7,
                    "accepted": False,
                    "account_eligible": True,
                }
            }
        }
        self.assertEqual(validated_filter_weight(shadow, 300), 1.0)
        self.assertLess(validated_filter_weight(reject, 300), 0.1)

    def test_validated_reject_is_removed_from_consensus_support(self):
        rejected_filter = {
            "curve": {
                "300": {
                    "horizon_sec": 300,
                    "score": 0.1,
                    "threshold": 0.7,
                    "accepted": False,
                    "account_eligible": True,
                }
            }
        }
        contributor = {
            "instrument": "EUR_USD",
            "direction": "buy",
            "family": "momentum",
            "lane_id": "momentum.fast",
            "model_id": "momentum.fast",
            "input_timeframe": "M1",
            "signal_role": "structural",
            "signal_eligible": True,
            "account_eligible": True,
            "signal_confidence": 0.7,
            "projected_net_pips": 1.0,
            "matrix_input_weight": 1.0,
            "historical_reliability": 0.5,
            "sma_filter": rejected_filter,
        }

        aggregate = PracticeExecutor._aggregate_horizon_contributors(
            300,
            [contributor],
        )

        self.assertEqual(aggregate["eligible_component_count"], 0)
        self.assertIn("ensemble_no_eligible_structural_support", aggregate["signal_blocked_by"])


if __name__ == "__main__":
    unittest.main()
