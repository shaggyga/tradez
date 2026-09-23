import json
import tempfile
import unittest
from pathlib import Path

from trad.oanda_execution_policy import FrozenExecutionPolicy


class FrozenExecutionPolicyTests(unittest.TestCase):
    def policy(self, root: Path, *, status: str = "active") -> FrozenExecutionPolicy:
        path = root / "policy.json"
        path.write_text(
            json.dumps(
                {
                    "status": status,
                    "account_scope": "practice_007_only",
                    "policy_id": "p007-test",
                    "gates": {
                        "negative_veto_min_samples": 30,
                        "minimum_gross_to_spread": 1.15,
                    },
                    "cells": {
                        "EUR_USD|ridge.s5|S5|300": {
                            "eligible": False,
                            "negative_evidence": True,
                            "sample_count": 50,
                            "evidence_strength": 1.0,
                        },
                        "GBP_USD|*|M1|300": {
                            "eligible": True,
                            "negative_evidence": False,
                            "sample_count": 100,
                            "evidence_strength": 0.5,
                        },
                    },
                    "exit_policies": {
                        "300": {
                            "overall": {
                                "eligible": True,
                                "policy": {"kind": "time_stop", "horizon_fraction": 0.5},
                            },
                            "families": {},
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        return FrozenExecutionPolicy(path)

    def test_negative_exact_cell_vetoes_candidate(self):
        with tempfile.TemporaryDirectory() as folder:
            policy = self.policy(Path(folder))
            row = {
                "instrument": "EUR_USD",
                "model_id": "ridge.s5",
                "input_timeframe": "S5",
                "execution_horizon_sec": 300,
                "signal_eligible": True,
                "gross_to_spread": 2.0,
            }
            policy.apply_ranked_candidate(row)
            self.assertFalse(row["signal_eligible"])
            self.assertIn("frozen_policy_negative_cell", row["signal_blocked_by"])
            self.assertEqual(row["execution_policy_id"], "p007-test")

    def test_fallback_cell_downweights_without_promoting(self):
        with tempfile.TemporaryDirectory() as folder:
            policy = self.policy(Path(folder))
            row = {
                "instrument": "GBP_USD",
                "model_id": "other",
                "input_timeframe": "M1",
                "execution_horizon_sec": 300,
                "signal_eligible": False,
                "matrix_input_weight": 1.0,
                "gross_to_spread": 2.0,
            }
            policy.apply_ranked_candidate(row)
            self.assertFalse(row["signal_eligible"])
            self.assertAlmostEqual(row["matrix_input_weight"], 0.8)

    def test_inactive_policy_does_not_modify_candidate(self):
        with tempfile.TemporaryDirectory() as folder:
            policy = self.policy(Path(folder), status="shadow_only")
            row = {"signal_eligible": True, "execution_horizon_sec": 300}
            policy.apply_ranked_candidate(row)
            self.assertEqual(row, {"signal_eligible": True, "execution_horizon_sec": 300})

    def test_validated_exit_policy_is_returned(self):
        with tempfile.TemporaryDirectory() as folder:
            policy = self.policy(Path(folder))
            selected = policy.exit_policy(family="ridge", horizon_sec=300)
            self.assertEqual(selected["policy"]["kind"], "time_stop")


if __name__ == "__main__":
    unittest.main()
