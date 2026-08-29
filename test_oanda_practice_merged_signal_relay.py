from __future__ import annotations

import os
import unittest
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

try:
    from oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger import ForexManager
    import oanda_practice_merged_006_gpt as merged_gpt
    from oanda_practice_merged_signal_relay import (
        build_execution_candidate,
        candidate_rejection_reason,
        execution_exit_horizon,
        parse_args,
        ranked_snapshot_signals,
    )
except ModuleNotFoundError:  # Package imports used by the complete test suite.
    from trad.oanda_forex_gpt_advisor_all_pairs_v5_24_movement_ledger import ForexManager
    from trad import oanda_practice_merged_006_gpt as merged_gpt
    from trad.oanda_practice_merged_signal_relay import (
        build_execution_candidate,
        candidate_rejection_reason,
        execution_exit_horizon,
        parse_args,
        ranked_snapshot_signals,
    )


class MergedSignalRelayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.args = parse_args([])
        self.signal = {
            "id": "EUR_USD:multi:900",
            "lane_id": "pattern_count_forecast.fast",
            "family": "pattern_count_forecast",
            "instrument": "EUR_USD",
            "direction": "buy",
            "preferred_horizon_sec": 14400,
            "execution_horizon_sec": 14400,
            "signal_confidence": 0.54,
            "projected_net_pips": 0.35,
            "historical_reliability": 0.25,
            "agreement_family_count": 9,
            "opposing_family_count": 2,
            "execution_validation": {
                "validated": False,
                "prediction_quality_negative": False,
                "negative_historical_warmup": False,
            },
        }

    def test_unvalidated_consensus_is_accepted_for_aggressive_paper_lane(self):
        self.assertEqual(candidate_rejection_reason(self.signal, self.args), "")
        ranked, rejected = ranked_snapshot_signals(
            {"top_signals": [self.signal]},
            self.args,
        )
        self.assertEqual(len(ranked), 1)
        self.assertEqual(dict(rejected), {})

    def test_persistently_negative_history_is_always_rejected(self):
        signal = dict(self.signal)
        signal["execution_validation"] = {
            "prediction_quality_negative": True,
            "negative_historical_warmup": True,
            "validated": False,
        }
        self.assertEqual(
            candidate_rejection_reason(signal, self.args),
            "negative_historical_warmup",
        )

    def test_negative_quality_exploration_requires_strong_current_consensus(self):
        signal = dict(self.signal)
        signal["signal_confidence"] = 0.51
        signal["projected_net_pips"] = 0.2
        signal["execution_validation"] = {
            "prediction_quality_negative": True,
            "negative_historical_warmup": False,
            "validated": False,
        }
        self.assertEqual(candidate_rejection_reason(signal, self.args), "")
        signal["opposing_family_count"] = 8
        self.assertEqual(
            candidate_rejection_reason(signal, self.args),
            "negative_quality_consensus_floor",
        )

    def test_negative_quality_exploration_can_be_disabled(self):
        signal = dict(self.signal)
        signal["execution_validation"] = {
            "prediction_quality_negative": True,
            "negative_historical_warmup": False,
            "validated": False,
        }
        strict_args = parse_args(["--no-allow-negative-quality-exploration"])
        self.assertEqual(
            candidate_rejection_reason(signal, strict_args),
            "prediction_quality_negative",
        )

    def test_positive_family_consensus_is_required(self):
        signal = dict(self.signal)
        signal["agreement_family_count"] = 3
        signal["opposing_family_count"] = 4
        self.assertEqual(
            candidate_rejection_reason(signal, self.args),
            "family_consensus_not_positive",
        )

    def test_live_candidate_uses_current_quote_and_caps_hold_at_15_minutes(self):
        quote = SimpleNamespace(
            bid=1.1000,
            ask=1.1002,
            time="2026-07-21T04:00:00Z",
        )
        candidate, reason = build_execution_candidate(
            self.signal,
            quote,
            {"pip_location": -4.0},
            self.args,
        )
        self.assertEqual(reason, "")
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate["bid"], 1.1000)
        self.assertEqual(candidate["ask"], 1.1002)
        self.assertEqual(candidate["execution_horizon_sec"], 14400)
        self.assertEqual(candidate["execution_exit_horizon_sec"], 900)
        self.assertGreaterEqual(candidate["stop_loss_pips"], 7.0)
        self.assertTrue(candidate["open_ended_profit"])

    def test_exit_horizon_maps_to_supported_values(self):
        self.assertEqual(execution_exit_horizon(14400, 900), 900)
        self.assertEqual(execution_exit_horizon(420, 900), 300)
        self.assertEqual(execution_exit_horizon(30, 900), 60)

    def test_gpt_wrapper_pins_dum3_to_practice_006_and_shared_lock(self):
        with patch.object(
            merged_gpt,
            "read_credentials",
            return_value=("redacted", "101-001-00000000-006"),
        ), patch.dict(os.environ, {}, clear=False):
            account_id = merged_gpt.configure_environment()
            self.assertTrue(account_id.endswith("-006"))
            self.assertEqual(os.environ["OANDA_ENV"], "practice")
            self.assertEqual(os.environ["FOREX_ALLOW_LIVE"], "false")
            self.assertEqual(os.environ["FOREX_TARGET_MARGIN_USED_PCT"], "68")
            self.assertEqual(os.environ["FOREX_MAX_MARGIN_USED_PCT"], "82")
            self.assertTrue(
                os.environ["FOREX_ACCOUNT_EXECUTION_LOCK_PATH"].endswith(
                    "practice_006_merged_order.lock"
                )
            )

    def test_gpt_cross_process_lock_is_exclusive(self):
        test_dir = Path(__file__).resolve().parent / "data" / "test_runtime"
        test_dir.mkdir(parents=True, exist_ok=True)
        lock_path = test_dir / f"merged_lock_{os.getpid()}.lock"
        lock_path.unlink(missing_ok=True)
        manager = ForexManager.__new__(ForexManager)
        environment = {
            "FOREX_ACCOUNT_EXECUTION_LOCK_PATH": str(lock_path),
            "FOREX_ACCOUNT_EXECUTION_LOCK_TIMEOUT_SEC": "0.1",
            "FOREX_ACCOUNT_EXECUTION_LOCK_STALE_SEC": "30",
        }
        with patch.dict(os.environ, environment, clear=False):
            first, acquired = manager.acquire_account_execution_lock()
            self.assertTrue(acquired)
            second, acquired_second = manager.acquire_account_execution_lock()
            self.assertFalse(acquired_second)
            self.assertIsNone(second)
            manager.release_account_execution_lock(first)
            third, acquired_third = manager.acquire_account_execution_lock()
            self.assertTrue(acquired_third)
            manager.release_account_execution_lock(third)
        self.assertFalse(lock_path.exists())


if __name__ == "__main__":
    unittest.main()
