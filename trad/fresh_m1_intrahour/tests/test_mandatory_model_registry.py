from __future__ import annotations

import unittest
from pathlib import Path

from fresh_m1_intrahour.src.mandatory_model_launcher import build_mandatory_validation_command
from fresh_m1_intrahour.src.mandatory_model_registry import (
    ARIMA_ORDERS,
    MANDATORY_MODEL_NAMES,
    build_mandatory_registry,
)


class MandatoryModelRegistryTests(unittest.TestCase):
    def test_every_required_name_has_exactly_one_implementation(self) -> None:
        registry = build_mandatory_registry()
        self.assertEqual(len(MANDATORY_MODEL_NAMES), 86)
        self.assertEqual(len(MANDATORY_MODEL_NAMES), len(set(MANDATORY_MODEL_NAMES)))
        self.assertEqual(set(registry), set(MANDATORY_MODEL_NAMES))

    def test_all_requested_arima_orders_are_present(self) -> None:
        registry = build_mandatory_registry()
        expected = {f"ARIMA({p},{d},{q})" for p, d, q in ARIMA_ORDERS}
        self.assertTrue(expected.issubset(registry))
        self.assertEqual(len(expected), 8)

    def test_execution_named_models_are_offline_simulations(self) -> None:
        registry = build_mandatory_registry()
        for name in ("OANDA Live Mirror Model",):
            self.assertEqual(registry[name].execution_mode, "offline_research")
            self.assertEqual(registry[name].category, "execution_simulation")
        self.assertNotIn("MT5 Execution Model", registry)

    def test_registry_covers_forecast_policy_controller_and_diagnostic_layers(self) -> None:
        categories = {spec.category for spec in build_mandatory_registry().values()}
        self.assertTrue(
            {
                "forecast",
                "forecast_gate",
                "state_space",
                "regime",
                "trade_policy",
                "controller",
                "meta_controller",
                "execution_simulation",
                "diagnostic",
            }.issubset(categories)
        )

    def test_launcher_is_tier1_and_research_only(self) -> None:
        command = build_mandatory_validation_command(
            Path("reports/test"),
            start="2024-07-01",
            end="2026-07-07",
            tier="tier1",
            pairs=["EUR_USD", "USD_JPY"],
            workers=2,
            maxiter=5,
            quick=True,
        )
        joined = " ".join(command).lower()
        self.assertIn("mandatory_model_validation_worker.py", joined)
        self.assertIn("--tier tier1", joined)
        self.assertIn("--quick", joined)
        self.assertNotIn("live", joined)
        with self.assertRaises(ValueError):
            build_mandatory_validation_command(
                Path("reports/test"),
                start="2024-07-01",
                end="2026-07-07",
                tier="all",
                pairs=["EUR_USD"],
                workers=1,
                maxiter=5,
                quick=True,
            )

    def test_atr_variants_keep_raw_forecast_until_policy_gate(self) -> None:
        worker = Path(__file__).resolve().parents[1] / "mandatory_model_validation_worker.py"
        source = worker.read_text(encoding="utf-8")
        self.assertIn('h1["signal_atr_032"] = h1["signal_arima_201"]', source)
        self.assertIn('tf["signal_m15_atr_032"] = atr_signal', source)


if __name__ == "__main__":
    unittest.main()
