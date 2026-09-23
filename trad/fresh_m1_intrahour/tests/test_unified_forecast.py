from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

try:
    from fresh_m1_intrahour.src.unified_forecast import (
        add_cross_sectional_features,
        add_targets,
        chronological_split,
        infer_native_pip_size,
        live_matrix_from_snapshot,
        resample_completed,
        timeframe_features,
    )
except ModuleNotFoundError:
    from src.unified_forecast import (
        add_cross_sectional_features,
        add_targets,
        chronological_split,
        infer_native_pip_size,
        live_matrix_from_snapshot,
        resample_completed,
        timeframe_features,
    )


def synthetic_m1(rows: int = 20_000) -> pd.DataFrame:
    start = pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC")
    decision = start + pd.Timedelta(minutes=1)
    movement = np.sin(np.arange(rows) / 17.0) * 0.00001 + 0.000002
    close = 1.1 + np.cumsum(movement)
    open_ = np.r_[close[0], close[:-1]]
    spread = 0.0001
    return pd.DataFrame(
        {
            "bar_start_utc": start,
            "decision_time_utc": decision,
            "instrument": "EUR_USD",
            "open": open_,
            "high": np.maximum(open_, close) + 0.00003,
            "low": np.minimum(open_, close) - 0.00003,
            "close": close,
            "volume": 80 + np.arange(rows) % 20,
            "spread_pips": 1.0,
            "bid_open": open_ - spread / 2,
            "bid_high": np.maximum(open_, close) + 0.00003 - spread / 2,
            "bid_low": np.minimum(open_, close) - 0.00003 - spread / 2,
            "bid_close": close - spread / 2,
            "ask_open": open_ + spread / 2,
            "ask_high": np.maximum(open_, close) + 0.00003 + spread / 2,
            "ask_low": np.minimum(open_, close) - 0.00003 + spread / 2,
            "ask_close": close + spread / 2,
        }
    )


class UnifiedForecastTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = {
            "tier1_pairs": ["EUR_USD"],
            "tier2_pairs": [],
            "costs": {
                "fallback_spread_pips_tier1": 1.0,
                "slippage_pips_tier1": 0.2,
            },
        }

    def test_completed_resample_never_uses_partial_higher_timeframe_bar(self):
        m1 = synthetic_m1(61)
        output = resample_completed(m1, "M30")
        self.assertEqual(len(output), 2)
        self.assertTrue((output["source_bar_count"] == 30).all())
        self.assertEqual(
            output["decision_time_utc"].iloc[-1],
            pd.Timestamp("2026-01-01T01:00:00Z"),
        )

    def test_targets_are_midpoint_path_and_tradability_is_separate(self):
        m1 = synthetic_m1(200)
        base = timeframe_features(m1, "EUR_USD", "M1", 5)
        output = add_targets(base, m1, "EUR_USD", self.cfg, [1, 5])
        current = m1["close"].iloc[100]
        expected = (m1["close"].iloc[105] - current) / 0.0001
        row = output.iloc[100]
        self.assertAlmostEqual(row["target_return_pips_5"], expected)
        self.assertIn("diag_long_net_pips_5", output)
        self.assertNotEqual(
            row["target_return_pips_5"], row["diag_long_net_pips_5"]
        )

    def test_barrier_targets_preserve_order_and_mark_same_bar_ambiguity(self):
        m1 = synthetic_m1(120)
        for column in ("open", "high", "low", "close"):
            m1[column] = 1.1
        m1["high"] = 1.10001
        m1["low"] = 1.09999
        m1.loc[51, "high"] = 1.10011
        m1.loc[52, "low"] = 1.09989
        m1.loc[71, "high"] = 1.10031
        m1.loc[71, "low"] = 1.09969
        base = timeframe_features(m1, "EUR_USD", "M1", 5)
        output = add_targets(base, m1, "EUR_USD", self.cfg, [5])

        ordered = output.iloc[50]
        self.assertEqual(ordered["target_up_before_down_1pips_5"], 1.0)
        self.assertEqual(ordered["target_down_before_up_1pips_5"], 0.0)
        self.assertEqual(ordered["target_first_barrier_minutes_1pips_5"], 1.0)

        ambiguous = output.iloc[70]
        self.assertEqual(ambiguous["target_barrier_ambiguous_3pips_5"], 1.0)
        self.assertEqual(ambiguous["target_up_before_down_3pips_5"], 0.0)
        self.assertEqual(ambiguous["target_down_before_up_3pips_5"], 0.0)

    def test_continuation_and_spread_normalized_targets_are_explicit(self):
        m1 = synthetic_m1(120)
        m1.loc[:, "close"] = 1.1
        m1.loc[50, "close"] = 1.1001
        m1.loc[51, "close"] = 1.1002
        m1.loc[52, "close"] = 1.10015
        m1.loc[53:55, "close"] = [1.1002, 1.10025, 1.1003]
        m1["open"] = m1["close"].shift(1).fillna(m1["close"])
        m1["high"] = m1[["open", "close"]].max(axis=1) + 0.00001
        m1["low"] = m1[["open", "close"]].min(axis=1) - 0.00001
        base = timeframe_features(m1, "EUR_USD", "M1", 5)
        output = add_targets(base, m1, "EUR_USD", self.cfg, [5])
        row = output.iloc[50]
        self.assertEqual(row["target_continuation_minutes_5"], 1.0)
        self.assertEqual(row["target_time_to_reversal_minutes_5"], 2.0)
        self.assertEqual(row["target_reversal_observed_5"], 1.0)
        self.assertAlmostEqual(row["target_abs_move_to_spread_5"], 2.0)
        self.assertGreater(row["target_up_mfe_pips_5"], 2.0)

    def test_cross_pair_breadth_excludes_the_current_pair(self):
        stamp = pd.Timestamp("2026-01-01T12:00:00Z")
        frame = pd.DataFrame(
            {
                "decision_time_utc": [stamp] * 3,
                "instrument": ["EUR_USD", "GBP_USD", "USD_JPY"],
                "base_currency": ["EUR", "GBP", "USD"],
                "quote_currency": ["USD", "USD", "JPY"],
                "m1__return_lag_00_pips": [2.0, 0.5, -2.0],
                "m1__atr_14_pips": [1.0, 1.0, 1.0],
            }
        )
        output = add_cross_sectional_features(frame)
        self.assertEqual(output.loc[0, "cross__available_pair_count_1"], 3.0)
        self.assertEqual(
            output.loc[0, "cross__same_direction_breadth_ex_self_1"], 0.5
        )
        self.assertEqual(
            output.loc[2, "cross__same_direction_breadth_ex_self_1"], 0.0
        )
        self.assertEqual(
            output.loc[0, "cross__volatility_breadth_ex_self_1"], 0.5
        )

    def test_native_bid_ask_spread_recovers_nonstandard_pip_size(self):
        frame = pd.DataFrame(
            {
                "bid_close": np.full(20, 396.825),
                "ask_close": np.full(20, 397.053),
                "spread_pips": np.full(20, 22.8),
            }
        )
        self.assertEqual(infer_native_pip_size(frame, "EUR_HUF"), 0.01)

    def test_targets_reject_paths_crossing_a_missing_m1_bar(self):
        m1 = synthetic_m1(200).drop(index=102).reset_index(drop=True)
        base = timeframe_features(m1, "EUR_USD", "M1", 5)
        output = add_targets(base, m1, "EUR_USD", self.cfg, [5])
        row = output.iloc[100]
        self.assertTrue(np.isnan(row["target_return_pips_5"]))
        self.assertTrue(np.isnan(row["target_path_range_pips_5"]))
        self.assertTrue(np.isnan(row["diag_long_net_pips_5"]))
        self.assertTrue(np.isnan(row["diag_short_net_pips_5"]))

    def test_volume_baseline_is_shifted_before_current_observation(self):
        m1 = synthetic_m1(300)
        features = timeframe_features(m1, "EUR_USD", "M1", 5)
        changed = m1.copy()
        changed.loc[250, "volume"] = 1_000_000
        changed_features = timeframe_features(changed, "EUR_USD", "M1", 5)
        original_baseline = (
            m1.loc[250, "volume"]
            / features.loc[250, "m1__tick_activity_ratio_session"]
        )
        changed_baseline = (
            changed.loc[250, "volume"]
            / changed_features.loc[250, "m1__tick_activity_ratio_session"]
        )
        self.assertAlmostEqual(original_baseline, changed_baseline, places=5)

    def test_purge_keeps_all_labels_before_next_partition(self):
        frame = pd.DataFrame(
            {
                "decision_time_utc": pd.date_range(
                    "2026-01-01", periods=1000, freq="min", tz="UTC"
                ),
                "instrument": "EUR_USD",
            }
        )
        train, validation, final, split = chronological_split(
            frame, 0.2, 0.2, 60
        )
        self.assertTrue(split["train_purge_passed"])
        self.assertTrue(split["validation_purge_passed"])
        self.assertLess(
            train["decision_time_utc"].max() + pd.Timedelta(minutes=60),
            validation["decision_time_utc"].min(),
        )
        self.assertLess(
            validation["decision_time_utc"].max() + pd.Timedelta(minutes=60),
            final["decision_time_utc"].min(),
        )

    def test_live_snapshot_reconstructs_one_structural_row_per_pair(self):
        values = synthetic_m1(260)

        def series_payload(frame):
            return {
                "bar_start_utc": frame["bar_start_utc"].iloc[-1].isoformat(),
                "open": frame["open"].tolist(),
                "high": frame["high"].tolist(),
                "low": frame["low"].tolist(),
                "close": frame["close"].tolist(),
                "tick_activity": frame["volume"].tolist(),
                "historical_spread_pips": frame["spread_pips"].tolist(),
            }

        snapshot = {
            "generated_utc": values["decision_time_utc"].iloc[-1].isoformat(),
            "instruments": {
                pair: {
                    "structural_series": {
                        timeframe: series_payload(values)
                        for timeframe in ("M1", "M30", "H1", "H4")
                    }
                }
                for pair in ("EUR_USD", "GBP_USD")
            },
        }
        matrix = live_matrix_from_snapshot(
            snapshot,
            [
                "m1__return_lag_00_pips",
                "h4__arima_proxy_forecast_pips",
                "cross__base_minus_quote_strength_1",
                "cross__same_direction_breadth_ex_self_1",
            ],
        )
        self.assertEqual(set(matrix["instrument"]), {"EUR_USD", "GBP_USD"})
        self.assertTrue(
            matrix[
                [
                    "m1__return_lag_00_pips",
                    "h4__arima_proxy_forecast_pips",
                ]
            ]
            .notna()
            .all()
            .all()
        )
        self.assertTrue(
            matrix["cross__same_direction_breadth_ex_self_1"].notna().all()
        )


if __name__ == "__main__":
    unittest.main()
