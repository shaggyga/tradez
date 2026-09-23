from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from trad.spike_account_space.assumptions import (
    attach_timestamped_economics,
    build_project_metadata,
)
from trad.spike_account_space.gate import (
    VAULT_CAUSAL_FEATURES,
    audit_vault_columns,
    fit_movement_classifier,
    iter_vault_features,
    reject_hindsight_feature_names,
    score_movement_classifier,
)
from trad.spike_account_space.pipeline import (
    _decision_schema,
    _write_optional_parquet,
    make_account_configs,
)


class VaultGateTests(unittest.TestCase):
    def test_hindsight_names_are_rejected_and_audited(self) -> None:
        with self.assertRaises(ValueError):
            reject_hindsight_feature_names(["momentum_15_atr", "future_move_pips_120"])
        audit = audit_vault_columns(
            ["momentum_15_atr", "future_move_pips_120", "mystery_feature", "spread_pips"]
        ).set_index("column")
        self.assertEqual(audit.loc["momentum_15_atr", "status"], "included")
        self.assertEqual(audit.loc["future_move_pips_120", "reason"], "hindsight_or_label_name")
        self.assertEqual(audit.loc["mystery_feature", "reason"], "not_explicitly_causality_approved")

    def test_left_label_is_shifted_before_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index = pd.date_range("2026-01-01T00:00:00Z", periods=60, freq="5min", name="time_utc")
            source = pd.DataFrame(index=index)
            for position, column in enumerate(VAULT_CAUSAL_FEATURES):
                source[column] = 0.1 + position / 100.0
            source["atr240_pips"] = 2.0
            source["spread_pips"] = 0.8
            source["future_move_pips_120"] = 9999.0
            source.to_parquet(root / "EUR_USD.parquet")
            _, loaded = next(
                iter_vault_features(
                    root,
                    instruments=["EUR_USD"],
                    decision_offset_minutes=5,
                    decision_stride_minutes=5,
                )
            )
            self.assertEqual(loaded.loc[0, "timestamp"], index[0] + pd.Timedelta(minutes=5))
            self.assertNotIn("future_move_pips_120", loaded.columns)
            self.assertTrue(loaded["features_are_causal"].all())

    def test_validation_labels_cannot_change_fitted_gate(self) -> None:
        rng = np.random.default_rng(42)
        train = pd.DataFrame(
            rng.normal(size=(240, len(VAULT_CAUSAL_FEATURES))),
            columns=VAULT_CAUSAL_FEATURES,
        )
        train["decision_id"] = [f"train-{index}" for index in range(len(train))]
        train["is_significant"] = np.arange(len(train)) % 11 == 0
        validation = pd.DataFrame(
            rng.normal(size=(30, len(VAULT_CAUSAL_FEATURES))),
            columns=VAULT_CAUSAL_FEATURES,
        )
        validation["decision_id"] = [f"valid-{index}" for index in range(len(validation))]
        validation["is_significant"] = False
        model = fit_movement_classifier(train, maximum_train_rows=500, max_iter=20)
        before = score_movement_classifier(validation, model)
        validation["is_significant"] = True
        validation["future_move_pips_120"] = 1e9
        after = score_movement_classifier(validation, model)
        np.testing.assert_allclose(before, after)


class EconomicsAdapterTests(unittest.TestCase):
    def test_hkd_jpy_metadata_uses_project_pip_exception(self) -> None:
        metadata, ledger = build_project_metadata(["HKD_JPY", "USD_JPY"])
        self.assertEqual(metadata["HKD_JPY"].pip_location, -4)
        self.assertEqual(metadata["USD_JPY"].pip_location, -2)
        self.assertFalse(ledger["is_account_specific_snapshot"].any())

    def test_exact_timestamp_conversion_and_missing_path_fail_closed(self) -> None:
        metadata, _ = build_project_metadata(["EUR_GBP"])
        entry = pd.Timestamp("2026-01-01T12:00:00Z")
        exit_time = entry + pd.Timedelta(minutes=30)
        outcomes = pd.DataFrame(
            [
                {
                    "instrument": "EUR_GBP",
                    "triggered": True,
                    "entry_timestamp": entry,
                    "exit_timestamp": exit_time,
                    "realized_pips": 10.0,
                    "primary_realized_pips": 10.0,
                    "risk_pips_per_unit": 5.0,
                    "filled_legs": 1,
                }
            ]
        )
        bars = pd.DataFrame(
            [
                {"timestamp": entry, "instrument": "EUR_GBP", "bid_close": 0.8499, "ask_close": 0.8501},
                {"timestamp": entry, "instrument": "EUR_USD", "bid_close": 1.0999, "ask_close": 1.1001},
                {"timestamp": entry, "instrument": "GBP_USD", "bid_close": 1.2939, "ask_close": 1.2941},
                {"timestamp": exit_time, "instrument": "GBP_USD", "bid_close": 1.2949, "ask_close": 1.2951},
            ]
        )
        attached = attach_timestamped_economics(outcomes, bars, metadata)
        self.assertTrue(bool(attached.loc[0, "economics_valid"]))
        self.assertAlmostEqual(attached.loc[0, "realized_pnl_account_per_unit"], 0.0012949, places=8)
        missing_exit = attach_timestamped_economics(outcomes, bars.iloc[:3], metadata)
        self.assertFalse(bool(missing_exit.loc[0, "economics_valid"]))


class PipelineUtilityTests(unittest.TestCase):
    def test_empty_optional_output_removes_stale_trade_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "outer_trades.parquet"
            pd.DataFrame({"trade_id": ["stale"]}).to_parquet(path, index=False)
            self.assertTrue(path.exists())
            _write_optional_parquet(pd.DataFrame(), path)
            self.assertFalse(path.exists())

    def test_execution_delay_preserves_research_timestamp(self) -> None:
        timestamp = pd.Timestamp("2026-01-01T12:00:00Z")
        frame = pd.DataFrame(
            [
                {
                    "decision_id": "d1",
                    "timestamp": timestamp,
                    "instrument": "EUR_USD",
                    "movement_score": 2.0,
                    "movement_threshold": 1.0,
                    "movement_gate_passed": True,
                    "atr_pips": 10.0,
                    "spread_pips": 1.0,
                    "expected_net_edge_pips": 8.0,
                    "currency_theme_cluster_id": "instrument_composition:USD",
                    "event_cluster_id": pd.NA,
                    "is_significant": False,
                    "forward_abs_pips": 4.0,
                }
            ]
        )
        decision = _decision_schema(frame, execution_delay_minutes=1)
        self.assertEqual(decision.loc[0, "research_label_timestamp"], timestamp)
        self.assertEqual(decision.loc[0, "timestamp"], timestamp + pd.Timedelta(minutes=1))

    def test_account_sampling_covers_every_requested_balance(self) -> None:
        config = {
            "movement_gate": {"random_seed": 7},
            "account_economics": {"starting_balances": [50, 100, 1000, 10000], "account_currency": "USD"},
            "account_search": {
                "maximum_trials": 8,
                "risk_per_trade_fraction": [0.001, 0.002],
                "max_total_open_risk_fraction": [0.01],
                "max_theme_open_risk_fraction": [0.005],
                "max_margin_used_fraction": [0.1],
                "max_concurrent_positions": [2],
                "max_positions_per_theme": [1],
                "max_new_positions_per_timestamp": [1],
                "daily_loss_stop_fraction": [0.01],
                "drawdown_halt_fraction": [0.05],
                "margin_closeout_buffer": [1.25],
            },
        }
        configs = make_account_configs(config)
        self.assertEqual({item.starting_balance for item in configs}, {50.0, 100.0, 1000.0, 10000.0})


if __name__ == "__main__":
    unittest.main()
