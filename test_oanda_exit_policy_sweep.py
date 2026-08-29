import unittest

import numpy as np

from trad.oanda_exit_policy_sweep import metrics, simulate_policy


def paths(
    favorable: list[list[float]],
    adverse: list[list[float]],
    close: list[list[float]],
    elapsed: list[list[float]],
    spread: list[float] | None = None,
) -> dict[str, np.ndarray]:
    return {
        "favorable": np.asarray(favorable, dtype=float),
        "adverse": np.asarray(adverse, dtype=float),
        "close": np.asarray(close, dtype=float),
        "elapsed": np.asarray(elapsed, dtype=float),
        "spread": np.asarray(spread or [1.0] * len(close), dtype=float),
    }


class ExitPolicySweepTests(unittest.TestCase):
    def test_stop_wins_same_bar_ambiguity(self):
        sample = paths([[3.0]], [[-3.0]], [[1.0]], [[5.0]])
        policy = {
            "id": "fixed",
            "kind": "fixed",
            "stop_pips": 2.0,
            "target_pips": 2.0,
        }
        values, holds = simulate_policy(sample, policy, 60)
        self.assertEqual(values.tolist(), [-2.0])
        self.assertEqual(holds.tolist(), [5.0])

    def test_trailing_stop_only_applies_after_observed_close(self):
        sample = paths(
            [[3.5, 2.0]],
            [[0.0, -0.5]],
            [[3.0, 0.0]],
            [[5.0, 10.0]],
            [0.2],
        )
        policy = {
            "id": "trail",
            "kind": "trailing",
            "stop_pips": 4.0,
            "activation_pips": 2.0,
            "trail_distance_pips": 2.0,
            "floor_pips": 0.2,
            "spread_multiple": 1.5,
            "step_pips": 0.25,
        }
        values, holds = simulate_policy(sample, policy, 60)
        self.assertAlmostEqual(values[0], 1.0)
        self.assertEqual(holds.tolist(), [10.0])

    def test_initial_stop_is_widened_beyond_entry_spread(self):
        sample = paths(
            [[0.0]],
            [[-2.1]],
            [[-2.1]],
            [[5.0]],
            [2.0],
        )
        policy = {
            "id": "fixed",
            "kind": "fixed",
            "stop_pips": 1.5,
            "target_pips": 4.0,
        }
        values, _ = simulate_policy(sample, policy, 60)
        self.assertAlmostEqual(values[0], -2.1)

    def test_endpoint_respects_requested_horizon(self):
        sample = paths(
            [[0.0, 0.0]],
            [[0.0, 0.0]],
            [[1.0, 5.0]],
            [[30.0, 90.0]],
        )
        values, holds = simulate_policy(sample, {"id": "endpoint", "kind": "endpoint"}, 60)
        self.assertEqual(values.tolist(), [1.0])
        self.assertEqual(holds.tolist(), [30.0])

    def test_metrics_report_exposure_rate_and_drawdown(self):
        values = np.asarray([2.0, -1.0, -2.0, 3.0])
        holds = np.asarray([60.0, 60.0, 60.0, 60.0])
        result = metrics(values, holds, range(4))
        self.assertEqual(result["n"], 4)
        self.assertAlmostEqual(result["average_net_pips"], 0.5)
        self.assertAlmostEqual(result["pips_per_exposure_hour"], 30.0)
        self.assertAlmostEqual(result["max_drawdown_pips"], 3.0)


if __name__ == "__main__":
    unittest.main()
