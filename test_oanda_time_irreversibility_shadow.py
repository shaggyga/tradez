import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from trad.oanda_directional_change_shadow import directional_change_states
from trad.oanda_time_irreversibility_shadow import (
    gated_directions,
    ordinal_irreversibility_states,
    ordinal_pattern,
    run_audit,
)
from trad.test_oanda_directional_change_shadow import build_database, synthetic_dataset


class TimeIrreversibilityShadowTests(unittest.TestCase):
    def test_ordinal_pattern_reverses_monotonic_path(self):
        self.assertEqual(ordinal_pattern((1.0, 2.0, 3.0)), (0, 1, 2))
        self.assertEqual(ordinal_pattern((3.0, 2.0, 1.0)), (2, 1, 0))

    def test_current_forward_move_cannot_change_current_score(self):
        base = ordinal_irreversibility_states(synthetic_dataset(1.0))
        shocked = ordinal_irreversibility_states(synthetic_dataset(-1000.0))
        self.assertEqual(
            base["irreversibility_score"][-1],
            shocked["irreversibility_score"][-1],
        )
        self.assertTrue(base["warm"][-1])

    def test_gates_emit_only_predeclared_continuation_or_reversal(self):
        dataset = synthetic_dataset()
        dc = directional_change_states(dataset)
        ordinal = ordinal_irreversibility_states(dataset)
        arms = gated_directions(dc, ordinal)
        for name, values in arms.items():
            self.assertTrue(set(np.unique(values)).issubset({-1, 0, 1}), name)

    def test_audit_remains_shadow_only(self):
        with tempfile.TemporaryDirectory() as folder:
            database = Path(folder) / "audit.sqlite"
            output = Path(folder) / "irreversibility.json"
            build_database(database)
            payload = run_audit(database, output, horizons=("M5",), max_rows=10_000)
            saved = json.loads(output.read_text(encoding="utf-8"))
        self.assertIn("M5", payload["horizons"])
        self.assertFalse(saved["account_eligible"])
        self.assertFalse(saved["execution_adapter"])
        self.assertFalse(saved["holdout_used_for_selection"])


if __name__ == "__main__":
    unittest.main()
