import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
REGISTER = ROOT / "config" / "forex_source_gap_register_v1.json"
PENDING = ROOT / "FOREX_PENDING_IMPROVEMENTS.md"
PROJECT_LOG = ROOT / "FOREX_PROJECT_LOG.md"


class SourceGapPendingReconciliationTests(unittest.TestCase):
    def test_structured_source_inventory_and_pending_queue_are_reconciled(self) -> None:
        register = json.loads(REGISTER.read_text(encoding="utf-8"))
        pending = PENDING.read_text(encoding="utf-8")
        sources = register.get("sources") or []
        family_ids = [row["source_family"] for row in sources]

        self.assertEqual(len(family_ids), len(set(family_ids)))
        self.assertEqual(register["repository_controlled_pending"], 0)
        self.assertEqual(
            register["queue_authority"]["canonical_pending_path"],
            PENDING.name,
        )
        self.assertFalse(
            register["queue_authority"][
                "all_source_families_must_appear_in_canonical_pending"
            ]
        )
        self.assertIn("no unimplemented repository-controlled changes", pending.lower())
        self.assertIn("active research direction is", pending.lower())
        self.assertIn("source-first", pending.lower())
        self.assertTrue(PROJECT_LOG.exists())
        project_log = PROJECT_LOG.read_text(encoding="utf-8")
        self.assertIn("2026-08-19 21:32 America/New_York", project_log)
        self.assertIn("restore the full spike/blurb entry objective", project_log.lower())
        for family_id in family_ids:
            self.assertNotIn(f"`{family_id}`", pending)

    def test_free_only_procurement_policy_is_consistent(self) -> None:
        register = json.loads(REGISTER.read_text(encoding="utf-8"))
        policy = register["procurement_policy"]
        pending = PENDING.read_text(encoding="utf-8")

        self.assertIs(policy["paid_sources_allowed"], False)
        self.assertIs(policy["public_or_existing_credential_sources_only"], True)
        self.assertIn("Paid procurement remains out of scope", pending)


if __name__ == "__main__":
    unittest.main()
