import unittest

from trad.oanda_practice_shadow_strategy_lab import FAMILIES
from trad.oanda_strategy_archetypes import (
    FAMILY_ARCHETYPE,
    archetype_vote_summary,
    strategy_archetype,
)


class StrategyArchetypeTests(unittest.TestCase):
    def test_every_strategy_lab_family_has_an_audited_assignment(self):
        missing = sorted(family for family in FAMILIES if family not in FAMILY_ARCHETYPE)
        self.assertEqual(missing, [])

    def test_profiles_and_correlated_families_do_not_inflate_independence(self):
        summary = archetype_vote_summary(
            [
                {"family": "momentum", "direction": "buy", "matrix_weight": 1.0},
                {"family": "momentum", "direction": "buy", "matrix_weight": 0.8},
                {"family": "ema_trend_cross", "direction": "buy", "matrix_weight": 0.9},
                {"family": "donchian_breakout", "direction": "buy", "matrix_weight": 0.7},
            ],
            target_direction="buy",
        )
        self.assertEqual(summary["raw_family_count"], 3)
        self.assertEqual(summary["archetype_count"], 2)
        self.assertEqual(summary["agreeing_archetype_count"], 2)
        self.assertEqual(summary["opposing_archetype_count"], 0)
        self.assertEqual(
            summary["agreeing_archetypes"],
            ["breakout_expansion", "trend_momentum"],
        )

    def test_conflicted_archetype_is_not_counted_on_both_sides(self):
        summary = archetype_vote_summary(
            [
                {"family": "momentum", "direction": "buy", "matrix_weight": 1.0},
                {"family": "ema_trend_cross", "direction": "sell", "matrix_weight": 1.0},
                {"family": "bollinger_reversion", "direction": "buy", "matrix_weight": 0.6},
            ],
            target_direction="buy",
        )
        self.assertEqual(summary["archetype_count"], 2)
        self.assertEqual(summary["agreeing_archetype_count"], 1)
        self.assertEqual(summary["opposing_archetype_count"], 0)
        self.assertEqual(summary["conflicted_archetypes"], ["trend_momentum"])

    def test_unknown_families_are_grouped_conservatively_and_flagged(self):
        summary = archetype_vote_summary(
            [
                {"family": "future_alpha_a", "direction": "buy"},
                {"family": "future_alpha_b", "direction": "buy"},
            ],
            target_direction="buy",
        )
        self.assertEqual(strategy_archetype("future_alpha_a"), "unclassified")
        self.assertEqual(summary["raw_family_count"], 2)
        self.assertEqual(summary["archetype_count"], 1)
        self.assertEqual(summary["unclassified_families"], ["future_alpha_a", "future_alpha_b"])


if __name__ == "__main__":
    unittest.main()
