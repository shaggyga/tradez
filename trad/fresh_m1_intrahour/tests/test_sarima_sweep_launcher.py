from __future__ import annotations

import unittest
from pathlib import Path

from fresh_m1_intrahour.src.sarima_sweep_launcher import (
    LEGACY_SITE_PACKAGES,
    WORKER,
    build_sarima_command,
    find_sarima_runtime,
)


class SarimaSweepLauncherTests(unittest.TestCase):
    def test_existing_runtime_and_worker_are_available(self) -> None:
        self.assertTrue(find_sarima_runtime().exists())
        self.assertTrue((LEGACY_SITE_PACKAGES / "statsmodels").exists())
        self.assertTrue(WORKER.exists())

    def test_command_is_research_only_and_bounded(self) -> None:
        command = build_sarima_command(
            Path("reports/test"),
            start="2025-01-01",
            end="2026-07-07",
            tier="tier1",
            pairs=None,
            labels_path=None,
            workers=2,
            maxiter_screen=5,
            maxiter_final=10,
            refine_per_category=3,
            max_specs=4,
            quick=True,
            force_data=False,
            force_screen=False,
            force_refinement=False,
        )
        joined = " ".join(command)
        self.assertIn("sarima_sweep_worker.py", joined)
        self.assertIn("--max-specs 4", joined)
        self.assertIn("--refine-per-category 3", joined)
        self.assertIn("--quick", joined)
        self.assertNotIn("live", joined.lower())
        self.assertNotIn("order", joined.lower())


if __name__ == "__main__":
    unittest.main()
