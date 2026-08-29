import unittest

from trad.oanda_signal_combination_fit import (
    apply_forward_refit_confirmation,
    carry_forward_unselected_rules,
    retained_shadow_rule,
)


class SignalCombinationFitTests(unittest.TestCase):
    def test_unselected_legacy_horizon_is_demoted_during_schema_migration(self):
        carried = carry_forward_unselected_rules(
            [
                {
                    "horizon_sec": 900,
                    "account_eligible": True,
                    "holdout": {"expected_net_pips": 1.0},
                }
            ],
            {300},
            chronological_validation_blocks=2,
        )
        self.assertEqual(len(carried), 1)
        self.assertFalse(carried[0]["account_eligible"])
        self.assertEqual(
            carried[0]["retained_reason"],
            "no_new_chronologically_replicated_rules",
        )

    def test_unselected_current_rule_preserves_forward_confirmation(self):
        current = {
            "horizon_sec": 900,
            "account_eligible": True,
            "selection": {},
            "final_holdout": {},
            "replication_signature": "stable",
            "first_replicated_utc": "2026-01-01T00:00:00+00:00",
        }
        carried = carry_forward_unselected_rules(
            [current],
            {300},
            chronological_validation_blocks=2,
        )
        self.assertEqual(carried, [current])

    def test_new_rule_waits_one_horizon_for_forward_refit_confirmation(self):
        rule = {
            "horizon_sec": 300,
            "predicted_direction": "buy",
            "account_eligible": True,
            "conditions": [
                {"feature": "rsi", "operator": ">=", "label": "high"},
                {"feature": "trend", "operator": ">=", "label": "high"},
            ],
        }
        first = apply_forward_refit_confirmation(
            [rule],
            [],
            validated_utc="2026-01-01T00:00:00+00:00",
        )[0]
        early = apply_forward_refit_confirmation(
            [rule],
            [first],
            validated_utc="2026-01-01T00:04:59+00:00",
        )[0]
        confirmed = apply_forward_refit_confirmation(
            [rule],
            [early],
            validated_utc="2026-01-01T00:05:00+00:00",
        )[0]
        self.assertFalse(first["account_eligible"])
        self.assertFalse(early["account_eligible"])
        self.assertTrue(confirmed["account_eligible"])
        self.assertTrue(confirmed["forward_refit_confirmed"])

    def test_direction_change_restarts_forward_confirmation(self):
        buy = {
            "horizon_sec": 300,
            "predicted_direction": "buy",
            "account_eligible": True,
            "conditions": [
                {"feature": "trend", "operator": ">=", "label": "high"},
                {"feature": "rsi", "operator": ">=", "label": "high"},
            ],
        }
        first = apply_forward_refit_confirmation(
            [buy],
            [],
            validated_utc="2026-01-01T00:00:00+00:00",
        )[0]
        sell = {**buy, "predicted_direction": "sell"}
        changed = apply_forward_refit_confirmation(
            [sell],
            [first],
            validated_utc="2026-01-01T01:00:00+00:00",
        )[0]
        self.assertFalse(changed["account_eligible"])
        self.assertEqual(changed["forward_confirmation_age_sec"], 0.0)

    def test_retained_rule_loses_account_eligibility_after_failed_refit(self):
        retained = retained_shadow_rule(
            {
                "rule_id": "h300-r1",
                "account_eligible": True,
                "selection": {"expected_net_pips": 0.5},
                "final_holdout": {"expected_net_pips": 0.4},
            },
            chronological_validation_blocks=2,
        )
        self.assertFalse(retained["account_eligible"])
        self.assertTrue(retained["previous_account_eligible"])
        self.assertTrue(retained["validation_schema_compatible"])
        self.assertEqual(
            retained["shadow_reason"],
            "stale_no_new_chronological_replication",
        )

    def test_legacy_rule_is_marked_incompatible_with_two_block_validation(self):
        retained = retained_shadow_rule(
            {"rule_id": "h300-r1", "account_eligible": True, "holdout": {}},
            chronological_validation_blocks=2,
        )
        self.assertFalse(retained["validation_schema_compatible"])
        self.assertFalse(retained["account_eligible"])


if __name__ == "__main__":
    unittest.main()
