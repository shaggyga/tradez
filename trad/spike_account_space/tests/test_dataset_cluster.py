from __future__ import annotations

import tempfile
from pathlib import Path
import unittest

import numpy as np
import pandas as pd

from trad.spike_account_space.dataset import (
    build_forward_labels,
    load_normalized_bars,
    normalize_cached_bars,
    pip_size,
)
from trad.spike_account_space.events import assign_event_clusters
from trad.spike_account_space.splits import make_nested_purged_splits
from trad.spike_account_space.thresholds import (
    apply_train_only_thresholds,
    fit_train_only_thresholds,
)


def cache_fixture(periods: int = 30) -> pd.DataFrame:
    timestamp = pd.date_range("2026-01-05 09:00", periods=periods, freq="5min", tz="UTC")
    close = pd.Series(1.1000 + np.arange(periods) * 0.0001)
    spread = pd.Series(np.full(periods, 2.0))
    return pd.DataFrame(
        {
            "time_utc": timestamp,
            "open": close - 0.00002,
            "high": close + 0.00005,
            "low": close - 0.00005,
            "close": close,
            "bid_close": close - spread * 0.0001 / 2,
            "ask_close": close + spread * 0.0001 / 2,
            "spread_pips": spread,
            "source_m1_observations": 5,
            "spread_estimated_fraction": 0.0,
        }
    )


class BarAndLabelTests(unittest.TestCase):
    def test_normalized_schema_and_project_pip_exceptions(self) -> None:
        bars = normalize_cached_bars(cache_fixture(), "EUR_USD")
        self.assertEqual(str(bars["timestamp"].dt.tz), "UTC")
        self.assertTrue((bars["timestamp"].dt.minute % 5 == 0).all())
        self.assertAlmostEqual(float(bars.loc[0, "pip_size"]), 0.0001)
        self.assertAlmostEqual(
            float(bars.loc[0, "ask_close"] - bars.loc[0, "bid_close"]), 0.0002
        )
        self.assertTrue(bars["bid_ask_open_high_low_is_derived"].all())
        self.assertEqual(pip_size("USD_JPY"), 0.01)
        self.assertEqual(pip_size("HKD_JPY"), 0.0001)

    def test_grid_rejects_missing_timestamp_and_naive_time(self) -> None:
        missing = cache_fixture().drop(index=4).reset_index(drop=True)
        with self.assertRaisesRegex(ValueError, "UTC grid gap"):
            normalize_cached_bars(missing, "EUR_USD")
        naive = cache_fixture()
        naive["time_utc"] = naive["time_utc"].dt.tz_localize(None)
        with self.assertRaisesRegex(ValueError, "naive"):
            normalize_cached_bars(naive, "EUR_USD")

    def test_loader_finds_stable_cache_layout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            cache = Path(temporary) / "cache" / "bars"
            cache.mkdir(parents=True)
            cache_fixture().to_parquet(cache / "EUR_USD.parquet", index=False)
            bars = load_normalized_bars(temporary, instruments=["EUR/USD"])
        self.assertEqual(len(bars), 30)
        self.assertEqual(set(bars["instrument"]), {"EUR_USD"})

    def test_forward_label_uses_exact_timestamp_not_observation_offset(self) -> None:
        raw = cache_fixture(periods=27)
        # Keep the 5-minute lattice but make one intervening market observation
        # explicitly absent.  Dropping this row then shifting 24 observations
        # would select 11:05 instead of the exact 11:00 endpoint.
        missing_row = 10
        price_columns = ["open", "high", "low", "close", "bid_close", "ask_close"]
        raw.loc[missing_row, price_columns] = np.nan
        bars = normalize_cached_bars(raw, "EUR_USD")
        labels = build_forward_labels(
            bars,
            horizon_minutes=120,
            minimum_observation_fraction=0.0,
            drop_invalid=False,
        )
        first = labels.loc[labels["timestamp"] == pd.Timestamp("2026-01-05 09:00", tz="UTC")].iloc[0]
        self.assertEqual(first["outcome_timestamp"], pd.Timestamp("2026-01-05 11:00", tz="UTC"))
        self.assertAlmostEqual(float(first["forward_signed_pips"]), 24.0, places=8)
        self.assertEqual(int(first["forward_observation_count"]), 23)
        self.assertEqual(int(first["forward_expected_observation_count"]), 24)

        strict = build_forward_labels(
            bars,
            horizon_minutes=120,
            minimum_observation_fraction=1.0,
            drop_invalid=False,
        )
        strict_first = strict.loc[strict["timestamp"] == first["timestamp"]].iloc[0]
        self.assertFalse(bool(strict_first["label_is_valid"]))
        self.assertIn("sparse_forward_window", strict_first["label_quality_flags"])
        tail = labels.iloc[-1]
        self.assertIn("missing_exact_120m_endpoint", tail["label_quality_flags"])


class EventThresholdAndSplitTests(unittest.TestCase):
    def _event_frame(self) -> pd.DataFrame:
        timestamp = pd.to_datetime(
            [
                "2026-06-17 10:00Z",
                "2026-06-17 10:05Z",
                "2026-06-17 10:15Z",
                "2026-06-17 13:00Z",
                "2026-06-17 10:20Z",
            ],
            utc=True,
        )
        return pd.DataFrame(
            {
                "decision_id": ["eur0", "eur1", "gbp0", "jpy0", "aud0"],
                "timestamp": timestamp,
                "outcome_timestamp": timestamp + pd.Timedelta(minutes=120),
                "instrument": ["EUR_USD", "EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD"],
                "forward_signed_pips": [-80.0, -75.0, -90.0, 100.0, -10.0],
                "forward_abs_pips": [80.0, 75.0, 90.0, 100.0, 10.0],
                "is_significant": [True, True, True, True, False],
                "label_is_valid": True,
            }
        )

    def test_pair_overlap_and_cross_pair_event_clusters_are_stable(self) -> None:
        original = assign_event_clusters(self._event_frame())
        by_id = original.set_index("decision_id")
        self.assertEqual(by_id.loc["eur0", "overlap_group_id"], by_id.loc["eur1", "overlap_group_id"])
        self.assertEqual(by_id.loc["eur0", "event_cluster_id"], by_id.loc["gbp0", "event_cluster_id"])
        self.assertNotEqual(by_id.loc["eur0", "event_cluster_id"], by_id.loc["jpy0", "event_cluster_id"])
        self.assertEqual(int(by_id.loc["eur0", "event_pair_count"]), 2)
        self.assertTrue(bool(by_id.loc["eur0", "is_cross_pair_shock"]))
        self.assertTrue(pd.isna(by_id.loc["aud0", "event_cluster_id"]))
        self.assertTrue(by_id["event_cluster_is_hindsight_metadata"].all())

        reversed_result = assign_event_clusters(
            self._event_frame().iloc[::-1].reset_index(drop=True)
        ).set_index("decision_id")
        pd.testing.assert_series_equal(
            by_id["event_cluster_id"].sort_index(),
            reversed_result["event_cluster_id"].sort_index(),
            check_names=False,
        )

    def test_thresholds_are_fit_only_on_supplied_train_rows(self) -> None:
        train = pd.DataFrame(
            {
                "decision_id": [f"train{i}" for i in range(12)],
                "timestamp": pd.date_range("2026-01-01", periods=12, freq="h", tz="UTC"),
                "instrument": ["EUR_USD"] * 10 + ["GBP_USD"] * 2,
                "forward_abs_pips": list(range(1, 11)) + [2, 3],
                "label_is_valid": True,
            }
        )
        model = fit_train_only_thresholds(
            train, quantile=0.8, minimum_group_samples=5, fixed_floor=0.0
        )
        test = pd.DataFrame(
            {
                "instrument": ["EUR_USD", "GBP_USD", "NEW_PAIR"],
                "forward_abs_pips": [1_000_000.0, 1_000_000.0, 1_000_000.0],
                "label_is_valid": True,
            }
        )
        applied = apply_train_only_thresholds(test, model)
        self.assertEqual(model.fit_scope, "train_only")
        self.assertLess(model.group_thresholds["EUR_USD"], 10.0)
        self.assertNotIn("GBP_USD", model.group_thresholds)
        self.assertEqual(applied.loc[1, "threshold_source"], "global_train_fallback")
        self.assertEqual(applied.loc[2, "significance_threshold"], model.global_threshold)
        self.assertTrue(applied["is_significant"].all())
        self.assertEqual(applied["threshold_train_fingerprint"].nunique(), 1)

    def test_nested_splits_purge_embargo_and_keep_events_indivisible(self) -> None:
        periods = 240
        timestamp = pd.date_range("2026-01-01", periods=periods, freq="h", tz="UTC")
        clusters = pd.Series(pd.NA, index=range(periods), dtype="string")
        clusters.iloc[[94, 95, 96]] = "boundary_event"
        frame = pd.DataFrame(
            {
                "decision_id": [f"d{i}" for i in range(periods)],
                "timestamp": timestamp,
                "outcome_timestamp": timestamp + pd.Timedelta(hours=2),
                "event_cluster_id": clusters,
            }
        )
        nested = make_nested_purged_splits(
            frame,
            n_outer_splits=2,
            n_inner_splits=2,
            outer_initial_train_fraction=0.4,
            inner_initial_train_fraction=0.5,
            purge="2h",
            embargo="1h",
        )
        self.assertEqual(len(nested), 2)
        first = nested[0].outer
        self.assertTrue({94, 95, 96}.issubset(set(first.omitted_positions)))
        for nested_fold in nested:
            outer = nested_fold.outer
            train = frame.iloc[outer.train_positions]
            test = frame.iloc[outer.test_positions]
            self.assertLess(train["timestamp"].max(), test["timestamp"].min())
            self.assertLessEqual(
                train["outcome_timestamp"].max(), test["timestamp"].min() - pd.Timedelta("2h")
            )
            self.assertLessEqual(
                train["timestamp"].max(), test["timestamp"].min() - pd.Timedelta("1h")
            )
            train_groups = set(train["event_cluster_id"].dropna())
            test_groups = set(test["event_cluster_id"].dropna())
            self.assertFalse(train_groups.intersection(test_groups))
            for inner in nested_fold.inner:
                self.assertTrue(set(inner.train_positions).issubset(set(outer.train_positions)))
                self.assertTrue(set(inner.test_positions).issubset(set(outer.train_positions)))


if __name__ == "__main__":
    unittest.main()
