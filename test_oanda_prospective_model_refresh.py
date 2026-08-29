import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from trad import oanda_prospective_model_refresh as refresh


class ProspectiveModelRefreshTests(unittest.TestCase):
    def args(self, root: Path, **overrides):
        values = {
            "panel": root / "panel.parquet",
            "panel_report": root / "panel_report.json",
            "historical_panel_dir": root / "historical",
            "candidate_root": root / "candidates",
            "state": root / "state.json",
            "active_report": root / "active_report.json",
            "active_model_root": root / "active_models",
            "models": "catboost,ngboost",
            "min_events": 10,
            "min_new_events": 5,
            "max_events": 1000,
            "min_train_events": 10,
            "min_test_events": 5,
            "no_activate": True,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_waits_without_ready_prospective_panel(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = self.args(root)
            args.panel_report.write_text(
                json.dumps({"status": "waiting_for_data", "artifact": {}}),
                encoding="utf-8",
            )

            state = refresh.run(args)

        self.assertEqual(state["status"], "waiting_for_matured_prospective_data")
        self.assertFalse(state["account_execution_authorized"])

    def test_new_panel_runs_candidate_benchmark_without_forcing_activation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = self.args(root)
            args.panel.write_bytes(b"panel")
            args.panel_report.write_text(
                json.dumps(
                    {
                        "status": "ready",
                        "artifact": {
                            "events": 20,
                            "sha256": "abc",
                        },
                    }
                ),
                encoding="utf-8",
            )
            report = {
                "results": [
                    {
                        "model": model,
                        "status": "evaluated",
                        "holdout": {"production_gate": {"passed": True}},
                    }
                    for model in ("catboost", "ngboost")
                ],
                "artifacts": [],
            }
            with patch.object(
                refresh.benchmark,
                "run_benchmark",
                return_value=report,
            ) as mocked:
                state = refresh.run(args)

        self.assertTrue(mocked.called)
        self.assertEqual(state["status"], "candidate_retained_not_activated")
        self.assertTrue(state["all_requested_robust"])
        self.assertFalse(state["activated"])

    def test_research_shadow_activation_is_hash_verified_and_not_account_eligible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = root / "candidate"
            active = root / "active"
            candidate.mkdir()
            active.mkdir()
            models = ["catboost", "ngboost"]
            artifacts = []
            results = []
            for model in models:
                path = candidate / f"{model}_shared_panel_latest.joblib"
                path.write_bytes(f"{model}-artifact".encode("ascii"))
                artifacts.append(
                    {
                        "model": model,
                        "sha256": refresh.benchmark.sha256_file(path),
                    }
                )
                results.append(
                    {
                        "model": model,
                        "status": "evaluated",
                        "holdout": {"production_gate": {"passed": False}},
                    }
                )
            report = {"artifacts": artifacts, "results": results}

            activation = refresh.activate_shadow_candidate(
                report,
                candidate,
                active,
                root / "active_report.json",
                models,
                "research-test",
                activation_policy="full-matrix research shadow",
                performance_gate_required=False,
            )

            self.assertFalse(activation["account_eligible"])
            self.assertFalse(activation["performance_gate_passed"])
            self.assertFalse(activation["performance_gate_required"])
            self.assertEqual(activation["policy"], "full-matrix research shadow")
            self.assertTrue(all((active / f"{model}_shared_panel_latest.joblib").is_file() for model in models))


if __name__ == "__main__":
    unittest.main()
