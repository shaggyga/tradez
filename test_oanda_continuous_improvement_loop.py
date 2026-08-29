import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from trad.oanda_continuous_improvement_loop import (
    atomic_json,
    parse_args,
    runtime_health,
    validation_commands,
)


class ContinuousImprovementLoopTests(unittest.TestCase):
    def test_continuous_runs_defer_the_initial_vault_checkpoint_by_default(self):
        args = parse_args([])

        self.assertTrue(args.defer_initial_vault_sync)

    def test_manual_runs_can_request_an_immediate_vault_checkpoint(self):
        args = parse_args(["--immediate-initial-vault-sync"])

        self.assertFalse(args.defer_initial_vault_sync)

    def test_atomic_json_tolerates_a_transient_reader_lock(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "state.json"
            attempts = 0

            def transient_denial(source, target):
                nonlocal attempts
                attempts += 1
                if attempts <= 6:
                    raise PermissionError("destination remains briefly open")
                Path(target).write_bytes(Path(source).read_bytes())
                Path(source).unlink()

            with patch(
                "trad.oanda_continuous_improvement_loop.os.replace",
                side_effect=transient_denial,
            ), patch("trad.oanda_continuous_improvement_loop.time.sleep"):
                atomic_json(path, {"ready": True})

            self.assertEqual(attempts, 7)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"ready": True})

    def test_news_outcome_record_is_not_a_runtime_worker(self):
        self.assertNotIn("news_outcome_audit", runtime_health(0.0)["workers"])

    def test_news_outcome_record_is_only_validated_not_regenerated(self):
        matches = [
            command
            for name, command, _timeout in validation_commands()
            if "news_outcome_improvement" in name
        ]
        self.assertEqual(len(matches), 1)
        self.assertIn("--validate-existing", matches[0])


if __name__ == "__main__":
    unittest.main()
