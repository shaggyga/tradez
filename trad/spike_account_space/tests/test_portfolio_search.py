from __future__ import annotations

import unittest

import pandas as pd

from trad.spike_account_space.portfolio import replay_account
from trad.spike_account_space.schemas import AccountConfig, ObjectiveConfig
from trad.spike_account_space.search import search_account_space


def _outcomes(rows: list[dict]) -> pd.DataFrame:
    defaults = {
        "instrument": "EUR_USD",
        "entry_timestamp": pd.Timestamp("2026-01-01T00:00:00Z"),
        "exit_timestamp": pd.Timestamp("2026-01-01T01:00:00Z"),
        "realized_pips": 5.0,
        "risk_pips_per_unit": 10.0,
        "mae_pips": 5.0,
        "triggered": True,
        "is_executable_candidate": True,
        "expected_net_edge_pips": 5.0,
        "movement_score": 0.9,
        "currency_theme_cluster_id": "usd_strength",
        "risk_per_pip_account_per_unit": 0.01,
        "pnl_per_pip_account_per_unit": 0.01,
        "margin_per_unit_account": 0.10,
        "reserved_legs": 1,
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def _account(**kwargs: object) -> AccountConfig:
    defaults = dict(
        starting_balance=10_000.0,
        risk_per_trade_fraction=0.01,
        max_total_open_risk_fraction=0.05,
        max_theme_open_risk_fraction=0.05,
        max_margin_used_fraction=0.50,
        max_concurrent_positions=5,
        max_positions_per_theme=5,
        max_positions_per_instrument=1,
        max_new_positions_per_timestamp=5,
        daily_loss_stop_fraction=0.50,
        drawdown_halt_fraction=0.50,
        max_units=100,
    )
    defaults.update(kwargs)
    return AccountConfig(**defaults)


class PortfolioReplayTests(unittest.TestCase):
    def test_theme_cap_selects_highest_causal_edge(self) -> None:
        data = _outcomes([
            {"decision_id": "low", "instrument": "EUR_USD", "expected_net_edge_pips": 2.0},
            {"decision_id": "high", "instrument": "USD_JPY", "expected_net_edge_pips": 8.0},
        ])
        result = replay_account(data, _account(max_positions_per_theme=1))
        self.assertEqual(result.trades["decision_id"].tolist(), ["high"])
        self.assertIn("currency_theme_position_cap", result.blocked["reason"].tolist())

    def test_concurrent_cap_blocks_distinct_themes(self) -> None:
        data = _outcomes([
            {"decision_id": "a", "instrument": "EUR_USD", "currency_theme_cluster_id": "a"},
            {"decision_id": "b", "instrument": "USD_JPY", "currency_theme_cluster_id": "b"},
        ])
        result = replay_account(data, _account(max_concurrent_positions=1))
        self.assertEqual(len(result.trades), 1)
        self.assertIn("concurrent_position_cap", result.blocked["reason"].tolist())

    def test_daily_loss_stop_blocks_later_entry(self) -> None:
        data = _outcomes([
            {
                "decision_id": "loss", "realized_pips": -30.0,
                "entry_timestamp": pd.Timestamp("2026-01-01T00:00:00Z"),
                "exit_timestamp": pd.Timestamp("2026-01-01T00:30:00Z"),
            },
            {
                "decision_id": "later", "instrument": "USD_JPY", "currency_theme_cluster_id": "jpy",
                "entry_timestamp": pd.Timestamp("2026-01-01T00:31:00Z"),
                "exit_timestamp": pd.Timestamp("2026-01-01T01:00:00Z"),
            },
        ])
        result = replay_account(data, _account(daily_loss_stop_fraction=0.002))
        self.assertEqual(result.trades["decision_id"].tolist(), ["loss"])
        self.assertIn("daily_loss_stop", result.blocked["reason"].tolist())

    def test_drawdown_halt_is_separate_from_daily_stop(self) -> None:
        data = _outcomes([
            {
                "decision_id": "loss", "realized_pips": -30.0,
                "entry_timestamp": pd.Timestamp("2026-01-01T00:00:00Z"),
                "exit_timestamp": pd.Timestamp("2026-01-01T00:30:00Z"),
            },
            {
                "decision_id": "later", "instrument": "USD_JPY", "currency_theme_cluster_id": "jpy",
                "entry_timestamp": pd.Timestamp("2026-01-01T00:31:00Z"),
                "exit_timestamp": pd.Timestamp("2026-01-01T01:00:00Z"),
            },
        ])
        result = replay_account(data, _account(daily_loss_stop_fraction=0.5, drawdown_halt_fraction=0.002))
        self.assertIn("drawdown_halt", result.blocked["reason"].tolist())

    def test_missing_economics_fails_closed(self) -> None:
        data = _outcomes([{"decision_id": "x"}]).drop(
            columns=["risk_per_pip_account_per_unit", "pnl_per_pip_account_per_unit", "margin_per_unit_account"]
        )
        result = replay_account(data, _account())
        self.assertTrue(result.trades.empty)
        self.assertTrue(result.blocked.iloc[0]["reason"].startswith("missing_economics:"))

    def test_exact_per_unit_pnl_supports_double_leg_conversion(self) -> None:
        data = _outcomes([{
            "decision_id": "double",
            "realized_pips": 0.0,
            "realized_pnl_account_per_unit": -0.20,
            "reserved_legs": 2,
        }])
        result = replay_account(data, _account())
        self.assertEqual(len(result.trades), 1)
        self.assertAlmostEqual(float(result.trades.loc[0, "pnl_account"]), -20.0)

    def test_margin_and_stress_guards_limit_units(self) -> None:
        data = _outcomes([{
            "decision_id": "wide",
            "margin_per_unit_account": 100.0,
            "mae_pips": 1000.0,
        }])
        result = replay_account(data, _account(
            starting_balance=1000.0,
            max_margin_used_fraction=0.10,
            min_units=2,
            max_units=100,
        ))
        self.assertTrue(result.trades.empty)
        self.assertIn(result.blocked.loc[0, "reason"], {"risk_or_margin_capacity", "stressed_margin_closeout_buffer"})

    def test_future_mae_does_not_selectively_block_but_invalidates_objective(self) -> None:
        data = _outcomes([{
            "decision_id": "future_stress",
            "mae_pips": 1_000_000.0,
        }])
        account = _account(max_units=1)
        replay = replay_account(data, account)
        self.assertEqual(len(replay.trades), 1)
        self.assertGreater(replay.summary["mae_stress_closeout_events"], 0)
        search = search_account_space(
            data,
            [account],
            ObjectiveConfig(minimum_trades_per_fold=0),
        )
        strategy = search.leaderboard[search.leaderboard["strategy"] != "no_trade"].iloc[0]
        self.assertEqual(float(strategy["stable_objective"]), -1_000_000.0)


class SearchTests(unittest.TestCase):
    def test_no_trade_baseline_beats_a_losing_configuration(self) -> None:
        data = _outcomes([{"decision_id": "loss", "realized_pips": -20.0}])
        result = search_account_space(
            data,
            [_account()],
            ObjectiveConfig(minimum_trades_per_fold=0),
        )
        self.assertEqual(result.leaderboard.loc[0, "strategy"], "no_trade")
        self.assertEqual(float(result.leaderboard.loc[0, "stable_objective"]), 0.0)
        self.assertFalse(bool(result.leaderboard.loc[1, "beats_no_trade"]))

    def test_hindsight_selection_column_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _account(selection_score_column="realized_pips")


if __name__ == "__main__":
    unittest.main()
