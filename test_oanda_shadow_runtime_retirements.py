import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
POLICY = ROOT / "config" / "shadow_runtime_retirements_v1.json"
SUPERVISOR = ROOT / "oanda_always_on_supervisor.ps1"


class ShadowRuntimeRetirementTests(unittest.TestCase):
    def test_retirement_contract_is_preservative_and_fail_closed(self) -> None:
        payload = json.loads(POLICY.read_text(encoding="utf-8"))
        policy = payload["policy"]
        self.assertTrue(policy["retirement_stops_future_collection_only"])
        self.assertTrue(policy["historical_code_and_evidence_are_preserved"])
        self.assertTrue(policy["retired_collectors_cannot_authorize_or_trade"])
        self.assertTrue(policy["reopening_requires_a_materially_new_contract_and_cohort"])

        names = {row["name"] for row in payload["retired_collectors"]}
        self.assertEqual(
            names,
            {
                "causal_source_factor_response_map_v1",
                "causal_source_factor_response_map_v2",
                "causal_source_factor_response_map_v3",
                "causal_source_factor_response_map_v4",
                "causal_source_factor_response_map_v5",
                "source_conditioned_currency_rank_v1",
                "source_conditioned_currency_rank_v2",
                "source_conditioned_currency_rank_v3",
                "source_conditioned_currency_rank_v4",
                "practice_019_hgb_rotation",
                "hgb_live_outcomes",
                "hgb_adaptive_fit",
                "manager_decision_outcome_ledger",
                "one_hour_shadow_signal",
                "executable_opportunity_proof",
            },
        )
        for row in payload["retired_collectors"]:
            self.assertTrue(
                row["state"].startswith(("retired_", "retiring_", "superseded_"))
            )
            self.assertIn(row["runtime_action"], {"disabled", "mature_only"})
            self.assertTrue(row["reason"])
            self.assertTrue(row["preserved_artifacts"])

    def test_supervisor_disables_every_retired_collector(self) -> None:
        payload = json.loads(POLICY.read_text(encoding="utf-8"))
        supervisor = SUPERVISOR.read_text(encoding="utf-8")
        disabled_block = supervisor.split("$DisabledNames = @(", 1)[1].split(")", 1)[0]
        for row in payload["retired_collectors"]:
            if row["runtime_action"] == "disabled":
                if row["state"].startswith("superseded_"):
                    # A sealed collector with an adopted successor is stopped
                    # explicitly by script needle at the cutover boundary; it
                    # has no Start-ManagedProcess entry to disable by name.
                    self.assertIn("Stop-MatchingPython", supervisor)
                    self.assertIn(row["script"], supervisor)
                    self.assertIn(row["successor"]["script"], supervisor)
                else:
                    self.assertIn(f'"{row["name"]}"', disabled_block)
            else:
                self.assertNotIn(f'"{row["name"]}"', disabled_block)
                self.assertIn('"--mature-only"', supervisor)
            if row["name"] in {
                "practice_019_hgb_rotation",
                "hgb_live_outcomes",
                "hgb_adaptive_fit",
            }:
                self.assertNotIn(f'-Name "{row["name"]}"', supervisor)
            else:
                self.assertIn(row["script"], supervisor)

    def test_inactive_feature_groups_cannot_claim_model_eligibility(self) -> None:
        payload = json.loads(POLICY.read_text(encoding="utf-8"))
        groups = payload["inactive_feature_groups"]
        self.assertGreaterEqual(len(groups), 2)
        for row in groups:
            self.assertFalse(row["active_model_eligibility"])
            self.assertTrue(row["historical_contract_retained"])

    def test_practice_019_stack_is_retired_and_has_no_spawn_surface(self) -> None:
        payload = json.loads(POLICY.read_text(encoding="utf-8"))
        supervisor = SUPERVISOR.read_text(encoding="utf-8")
        rows = {row["name"]: row for row in payload["retired_collectors"]}
        retired_names = {
            "practice_019_hgb_rotation",
            "hgb_live_outcomes",
            "hgb_adaptive_fit",
        }
        self.assertTrue(retired_names.issubset(rows))
        for name in retired_names:
            self.assertEqual(rows[name]["runtime_action"], "disabled")
            self.assertIn(f'"{name}"', supervisor)
            self.assertNotIn(f'-Name "{name}"', supervisor)

        historical_config = ROOT / "config" / "primary_forecast_rotation_demo_019.json"
        config_text = historical_config.read_text(encoding="utf-8")
        self.assertIn(r'D:\\forex\\trad', config_text)
        self.assertIn(
            "config/primary_forecast_rotation_demo_019.json",
            rows["practice_019_hgb_rotation"]["preserved_artifacts"],
        )


if __name__ == "__main__":
    unittest.main()
