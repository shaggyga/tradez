import tempfile
import unittest
from pathlib import Path

import pandas as pd

import oanda_cost_clearance_cross_sectional_research as research


class CostClearanceResearchTests(unittest.TestCase):
    def test_pip_inference_uses_recorded_contract(self):
        frame = pd.DataFrame({"spread_pips": [2.0] * 10, "ask_close": [10.04] * 10, "bid_close": [10.02] * 10})
        self.assertEqual(research.infer_pip_size("USD_HUF", frame), 0.01)

    def test_exact_executable_outcome(self):
        frame = pd.DataFrame({
            "epoch": [0, 60, 120, 180, 240, 300],
            "bid_close": [1.0000, 1.0, 1.0, 1.0, 1.0, 1.0010],
            "ask_close": [1.0002, 1.0002, 1.0002, 1.0002, 1.0002, 1.0012],
            "mid": [1.0001, 1.0001, 1.0001, 1.0001, 1.0001, 1.0011],
            "spread_pips": [2.0] * 6,
            "pip_size": [0.0001] * 6,
        })
        result = research.attach_outcomes(frame, 5, 0.25)
        self.assertAlmostEqual(result.loc[0, "long_net_pips"], 7.75)
        self.assertAlmostEqual(result.loc[0, "short_net_pips"], -12.25)
        self.assertEqual(result.loc[0, "cost_clear"], 1)

    def test_currency_disjoint_selection(self):
        rows = pd.DataFrame([
            {"instrument": "EUR_USD", "base": "EUR", "quote": "USD", "predicted_ev_pips": 4.0},
            {"instrument": "GBP_USD", "base": "GBP", "quote": "USD", "predicted_ev_pips": 3.0},
            {"instrument": "AUD_JPY", "base": "AUD", "quote": "JPY", "predicted_ev_pips": 2.0},
            {"instrument": "CAD_CHF", "base": "CAD", "quote": "CHF", "predicted_ev_pips": 1.0},
        ])
        selected = research.select_currency_disjoint(rows)
        self.assertEqual(list(selected["instrument"]), ["EUR_USD", "AUD_JPY", "CAD_CHF"])

    def test_atomic_text(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "value.txt"
            research.atomic_text(target, "ok")
            self.assertEqual(target.read_text(), "ok")


if __name__ == "__main__":
    unittest.main()
