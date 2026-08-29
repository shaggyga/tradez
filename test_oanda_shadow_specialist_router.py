import unittest

from trad.oanda_shadow_specialist_router import route_specialists


def candidate(name: str, archetype: str, lower: float) -> dict:
    return {
        "specialist": name,
        "archetype": archetype,
        "selection_average_net": 2.0,
        "selection_ci_lower": 1.0,
        "holdout_average_net": 2.0,
        "holdout_ci_lower": lower,
        "independent_blocks": 30,
        "observations": 300,
        "source_decision": "forward_observe_only",
    }


class ShadowSpecialistRouterTests(unittest.TestCase):
    def test_no_trade_when_every_specialist_fails_evidence(self):
        weak = candidate("weak", "price_panel", -0.1)
        result = route_specialists("mixed", [weak])
        self.assertEqual(result["decision"], "no_trade_no_validated_specialist")
        self.assertEqual(result["selected"], [])
        self.assertFalse(result["account_eligible"])
        self.assertFalse(result["execution_adapter"])

    def test_router_keeps_only_best_model_per_independent_archetype(self):
        result = route_specialists(
            "mixed",
            [
                candidate("price-a", "price_panel", 0.2),
                candidate("price-b", "price_panel", 0.8),
                candidate("news", "news_event", 0.4),
            ],
        )
        self.assertEqual(result["decision"], "observe_specialists")
        self.assertEqual(
            {row["specialist"] for row in result["selected"]},
            {"price-b", "news"},
        )

    def test_regime_excludes_otherwise_valid_unrelated_specialist(self):
        result = route_specialists(
            "news_event",
            [
                candidate("price", "price_panel", 1.0),
                candidate("news", "news_event", 0.5),
            ],
        )
        self.assertEqual(
            [row["specialist"] for row in result["selected"]],
            ["news"],
        )


if __name__ == "__main__":
    unittest.main()
