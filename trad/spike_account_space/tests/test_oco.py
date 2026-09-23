from __future__ import annotations

import unittest

import pandas as pd

from trad.spike_account_space.oco import evaluate_oco_candidates
from trad.spike_account_space.schemas import BufferConfig, ExitConfig, OCOConfig


PIP = 0.0001
SPREAD = 0.0002


def _bar(ts: str, *, open_: float, high: float, low: float, close: float) -> dict:
    return {
        "timestamp": pd.Timestamp(ts),
        "instrument": "EUR_USD",
        "bid_open": open_,
        "bid_high": high,
        "bid_low": low,
        "bid_close": close,
        "ask_open": open_ + SPREAD,
        "ask_high": high + SPREAD,
        "ask_low": low + SPREAD,
        "ask_close": close + SPREAD,
        "pip_size": PIP,
    }


def _decision(score: float = 0.9, **extra: object) -> pd.DataFrame:
    row = {
        "decision_id": "d1",
        "timestamp": pd.Timestamp("2026-01-01T00:01:00Z"),
        "instrument": "EUR_USD",
        "movement_score": score,
        "movement_threshold": 0.8,
        "atr_pips": 10.0,
        "spread_pips": 2.0,
        "expected_net_edge_pips": 4.0,
        "currency_theme_cluster_id": "usd_move",
        "pip_value_account_per_unit": 0.0001,
        "margin_per_unit_account": 0.03,
    }
    row.update(extra)
    return pd.DataFrame([row])


def _base_bars(*future: dict, wide_history_low: bool = True) -> pd.DataFrame:
    history_low = 0.9980 if wide_history_low else 0.9999
    rows = [
        _bar("2026-01-01T00:00:00Z", open_=1.0000, high=1.0001, low=history_low, close=1.0000),
        _bar("2026-01-01T00:01:00Z", open_=1.0000, high=1.0001, low=history_low, close=1.0000),
    ]
    rows.extend(future)
    return pd.DataFrame(rows)


def _config(**kwargs: object) -> OCOConfig:
    defaults = dict(
        range_lookback_minutes=5,
        minimum_history_bars=2,
        trigger_timeout_minutes=2,
        cancel_latency_minutes=0,
        buffer=BufferConfig(fixed_pips=0, spread_multiple=0, atr_multiple=0),
        exit=ExitConfig(stop_loss_pips=5, take_profit_pips=5, max_hold_minutes=1),
    )
    defaults.update(kwargs)
    return OCOConfig(**defaults)


class OCOEvaluationTests(unittest.TestCase):
    def test_movement_gate_rejects_before_path_evaluation(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0001, high=1.0010, low=1.0000, close=1.0005),
        )
        result = evaluate_oco_candidates(_decision(score=0.7), bars, _config())
        self.assertEqual(result.loc[0, "trigger_outcome"], "movement_gate_rejected")
        self.assertFalse(bool(result.loc[0, "triggered"]))

    def test_trigger_bar_never_credits_pre_entry_favorable_extreme(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0001, high=1.0015, low=1.0000, close=1.0003),
            _bar("2026-01-01T00:03:00Z", open_=1.0002, high=1.00025, low=1.00015, close=1.0002),
        )
        result = evaluate_oco_candidates(_decision(), bars, _config())
        self.assertEqual(result.loc[0, "exit_reason"], "time_exit")
        self.assertLess(float(result.loc[0, "realized_pips"]), 0.0)
        self.assertEqual(float(result.loc[0, "mfe_pips"]), 0.0)

    def test_entry_bar_stop_is_adverse_first_and_ambiguity_is_flagged(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0001, high=1.0015, low=0.9997, close=1.0003),
            _bar("2026-01-01T00:03:00Z", open_=1.0002, high=1.0003, low=1.0000, close=1.0001),
        )
        result = evaluate_oco_candidates(_decision(), bars, _config())
        self.assertEqual(result.loc[0, "primary_exit_reason"], "stop_loss")
        self.assertTrue(bool(result.loc[0, "same_bar_exit_ambiguous"]))
        self.assertAlmostEqual(float(result.loc[0, "realized_pips"]), -5.0, places=6)

    def test_cancel_latency_can_fill_sibling(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0001, high=1.0006, low=1.0000, close=1.0003),
            _bar("2026-01-01T00:03:00Z", open_=1.0000, high=1.0002, low=0.9995, close=0.9998),
            _bar("2026-01-01T00:04:00Z", open_=0.9998, high=1.0000, low=0.9995, close=0.9997),
            wide_history_low=False,
        )
        cfg = _config(
            cancel_latency_minutes=1,
            exit=ExitConfig(stop_loss_pips=20, take_profit_pips=20, max_hold_minutes=1),
        )
        result = evaluate_oco_candidates(_decision(), bars, cfg)
        self.assertTrue(bool(result.loc[0, "double_trigger"]))
        self.assertEqual(int(result.loc[0, "filled_legs"]), 2)
        self.assertEqual(result.loc[0, "ambiguity_resolution"], "cancel_latency_double_fill")
        self.assertEqual(pd.Timestamp(result.loc[0, "sibling_entry_timestamp"]), pd.Timestamp("2026-01-01T00:03:00Z"))
        self.assertEqual(pd.Timestamp(result.loc[0, "primary_exit_timestamp"]), pd.Timestamp("2026-01-01T00:03:00Z"))
        self.assertEqual(pd.Timestamp(result.loc[0, "sibling_exit_timestamp"]), pd.Timestamp("2026-01-01T00:04:00Z"))

    def test_same_bar_two_stop_ambiguity_uses_worst_single_fill(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0000, high=1.0008, low=0.9994, close=1.0000),
            _bar("2026-01-01T00:03:00Z", open_=1.0000, high=1.0001, low=0.9999, close=1.0000),
            wide_history_low=False,
        )
        result = evaluate_oco_candidates(_decision(), bars, _config())
        self.assertTrue(bool(result.loc[0, "same_bar_entry_ambiguous"]))
        self.assertEqual(result.loc[0, "ambiguity_resolution"], "adverse_first_worst_single_fill")
        self.assertEqual(int(result.loc[0, "filled_legs"]), 1)
        self.assertLessEqual(float(result.loc[0, "realized_pips"]), 0.0)

    def test_atr_scaled_exit_resolves_before_sizing(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0001, high=1.0006, low=1.0000, close=1.0003),
            _bar("2026-01-01T00:03:00Z", open_=1.0003, high=1.0004, low=1.0001, close=1.0002),
        )
        cfg = _config(exit=ExitConfig(
            stop_loss_pips=None,
            take_profit_pips=None,
            stop_loss_atr_multiple=2.0,
            take_profit_atr_multiple=3.0,
            max_hold_minutes=1,
        ))
        result = evaluate_oco_candidates(_decision(), bars, cfg)
        self.assertAlmostEqual(float(result.loc[0, "resolved_stop_loss_pips"]), 20.0)
        self.assertAlmostEqual(float(result.loc[0, "resolved_take_profit_pips"]), 30.0)
        self.assertAlmostEqual(float(result.loc[0, "risk_pips_per_unit"]), 20.0)

    def test_trailing_exit_uses_only_completed_bar_extreme(self) -> None:
        bars = _base_bars(
            _bar("2026-01-01T00:02:00Z", open_=1.0001, high=1.0005, low=1.0000, close=1.0003),
            _bar("2026-01-01T00:03:00Z", open_=1.0005, high=1.0013, low=1.0004, close=1.0010),
            _bar("2026-01-01T00:04:00Z", open_=1.0010, high=1.0011, low=1.0008, close=1.0009),
        )
        cfg = _config(exit=ExitConfig(
            stop_loss_pips=20,
            take_profit_pips=100,
            trailing_start_pips=5,
            trailing_distance_pips=3,
            max_hold_minutes=2,
        ))
        result = evaluate_oco_candidates(_decision(), bars, cfg)
        self.assertEqual(result.loc[0, "primary_exit_reason"], "trailing_stop")
        self.assertGreater(float(result.loc[0, "realized_pips"]), 0.0)


if __name__ == "__main__":
    unittest.main()
