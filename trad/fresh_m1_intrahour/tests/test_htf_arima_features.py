from __future__ import annotations

import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from fresh_m1_intrahour.src.htf_arima_features import (
    ARIMA_FEATURE_COLUMNS,
    _align_completed_closes,
    build_pair_causal_arima_features,
)


class HTFArimaFeatureTests(unittest.TestCase):
    def test_feature_contract_is_unique_and_label_free(self) -> None:
        self.assertEqual(len(ARIMA_FEATURE_COLUMNS), 50)
        self.assertEqual(len(ARIMA_FEATURE_COLUMNS), len(set(ARIMA_FEATURE_COLUMNS)))
        forbidden = ("future", "target", "outcome", "profit", "mfe", "mae")
        self.assertFalse([
            column
            for column in ARIMA_FEATURE_COLUMNS
            if any(token in column.lower() for token in forbidden)
        ])

    def test_appending_future_prices_does_not_change_prior_features(self) -> None:
        rng = np.random.default_rng(42)
        closes = 1.1 * np.exp(np.cumsum(rng.normal(0.0, 0.0002, 420)))
        kwargs = {
            "refit_interval": 24,
            "train_window": 240,
            "min_train_samples": 48,
        }
        prefix = build_pair_causal_arima_features(closes[:320], 0.0001, **kwargs)
        full = build_pair_causal_arima_features(closes, 0.0001, **kwargs).iloc[:320]
        assert_frame_equal(prefix, full, check_exact=False, rtol=1e-12, atol=1e-12)

    def test_open_m1_candle_at_decision_time_is_not_used(self) -> None:
        decisions = pd.DataFrame({
            "_arima_row_id": [0, 1],
            "decision_time_utc": pd.to_datetime([
                "2026-07-01T01:00:00Z",
                "2026-07-01T02:00:00Z",
            ]),
        })
        candles = pd.DataFrame({
            "datetime": pd.to_datetime([
                "2026-07-01T00:59:00Z",
                "2026-07-01T01:00:00Z",
                "2026-07-01T01:59:00Z",
                "2026-07-01T02:00:00Z",
            ]),
            "close": [1.01, 9.99, 1.02, 9.99],
        })
        aligned = _align_completed_closes(decisions, candles)
        self.assertEqual(aligned["_arima_close"].tolist(), [1.01, 1.02])
        self.assertTrue(
            (
                aligned["_arima_close_available_time"]
                <= aligned["decision_time_utc"]
            ).all()
        )


if __name__ == "__main__":
    unittest.main()
