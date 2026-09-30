"""Operational refusal/recovery tests; retain fixtures via --evidence."""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("forex_preflight", ROOT / "tools" / "forex_preflight.py")
preflight = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(preflight)
EVIDENCE = None


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def record(path, data):
    return {"path": path, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.base = EVIDENCE / (self._testMethodName + "_" + uuid.uuid4().hex[:8])
        self.root, self.vault = self.base / "project", self.base / "vault"
        self.root.mkdir(parents=True)
        self.vault.mkdir()
        self.write(self.root / "stage_c_alignment_integrity_v2" / "source.py", b"# retained source\n")
        self.write(self.root / "FOREX_HANDOFF.md", b"current handoff\n")
        self.write(self.root / "requirements-engineering.lock.txt", b"# Observed Python 3.12.10 engineering environment\nnumpy==2.5.1\n")
        self.write(self.root / "artifacts" / "registry.json", encoded({"artifacts": {"missing": {"archive_vault_relative_path": "missing.zip"}}}))
        self.run_git("init", "-b", "forex")
        self.run_git("config", "user.email", "test@example.invalid")
        self.run_git("config", "user.name", "Offline Fixture")
        self.commit()
        self.head = self.run_git("rev-parse", "HEAD")
        self.write(self.vault / "VAULT_FIRST_REUSE.md", b"Reuse exact identities before any run.\n")
        self.queue = {"active_step_id": "previous", "exact_next_item": "next_science",
                      "steps": [{"step_id": "previous", "review_status": "accepted_within_scope"}],
                      "pre_research_gate": {"required": True, "pointer": "OPERATIONAL_READINESS_LATEST.json",
                                            "active_operational_step": "ops-step"},
                      "operational_steps": [{"step_id": "ops-step", "implementation_status": "complete",
                                              "review_status": "accepted_within_scope"}]}
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.board([])
        self.write(self.vault / "SHARED_GIT_REMOTE_LATEST.json", encoded({"branch": "forex", "handoff_document_commit": self.head}))
        self.seal("engine", {"source_snapshot/source.py": (self.root / "stage_c_alignment_integrity_v2" / "source.py").read_bytes()},
                  "DESIGN_ALIGNMENT_LATEST.json")
        self.seal_docs()
        self.seal_gate()

    @staticmethod
    def write(path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def run_git(self, *args):
        result = subprocess.run(["git", "-C", str(self.root), *args], capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", "replace"))
        return result.stdout.decode().strip()

    def commit(self):
        self.run_git("add", ".")
        self.run_git("commit", "--no-gpg-sign", "-m", "retained fixture")

    def seal(self, package, files, pointer_name, extra_manifest=None, extra_pointer=None):
        records = []
        for name, data in files.items():
            self.write(self.vault / package / name, data)
            records.append(record(name, data))
        manifest = {"files": records, **(extra_manifest or {})}
        raw = encoded(manifest)
        self.write(self.vault / package / "MANIFEST.json", raw)
        pointer = {"package": package, "manifest": package + "/MANIFEST.json",
                   "manifest_sha256": hashlib.sha256(raw).hexdigest(), **(extra_pointer or {})}
        self.write(self.vault / pointer_name, encoded(pointer))

    def seal_docs(self):
        external = [record(name, (self.vault / name).read_bytes()) for name in ("VAULT_FIRST_REUSE.md", "REVIEW_QUEUE.json")]
        project = [record("FOREX_HANDOFF.md", (self.root / "FOREX_HANDOFF.md").read_bytes())]
        self.seal("docs", {"REVIEW.md": b"Accepted scope.\n"}, "CHECKPOINT_REVIEW_LATEST.json",
                  {"external_documents": external, "project_documents": project}, {"review": "docs/REVIEW.md"})

    def seal_gate(self, status="complete", step_id="ops-step", package="ops"):
        data = encoded({"status": status, "research_authorization": False, "step_id": step_id, "package": package})
        self.seal(package, {"OPS_STATUS.json": data},
                  "OPERATIONAL_READINESS_LATEST.json", extra_pointer={"ops_status": package + "/OPS_STATUS.json",
                  "status": status, "research_authorization": False, "step_id": step_id})
        for entry in self.queue["operational_steps"]:
            if entry["step_id"] == step_id:
                entry.update({"packet": package + "/OPS_STATUS.json", "packet_sha256": hashlib.sha256(data).hexdigest()})
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()

    def board(self, rows):
        header = "| Task ID | Chat / owner | Started UTC | Description | Reserved scope / files | Status | Last update UTC | Handoff / next action |\n|---|---|---|---|---|---|---|---|\n"
        text = header + "".join(f"| `{name}` | owner | now | offline work | engineering | `{status}` | now | reconcile |\n" for name, status in rows)
        self.write(self.vault / "CHAT_COORDINATION_BOARD.md", text.encode())

    def inspect(self, **kwargs):
        return preflight.preflight(self.root, self.vault, profile="stdlib", **kwargs)

    def assert_block(self, report, name):
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(next(c for c in report["checks"] if c["check"] == name)["status"], "blocked")

    def test_complete_setup_passes_without_granting_research(self):
        result = self.inspect()
        self.assertEqual(result["status"], "pass", result)
        self.assertFalse(result["research_authorization"])
        self.assertEqual(result["models_loaded"], 0)
        self.assertEqual(result["models_fitted"], 0)
        self.assertEqual(result["network_calls"], 0)

    def test_corrupt_pointer_then_restore(self):
        path = self.vault / "DESIGN_ALIGNMENT_LATEST.json"
        original = path.read_bytes()
        pointer = json.loads(original)
        pointer["manifest_sha256"] = "0" * 64
        self.write(path, encoded(pointer))
        self.assert_block(self.inspect(), "vault_packages")
        self.write(path, original)
        self.assertEqual(self.inspect()["status"], "pass")

    def test_mutated_sealed_member_refused(self):
        self.write(self.vault / "docs" / "REVIEW.md", b"changed after sealing")
        self.assert_block(self.inspect(), "vault_packages")

    def test_uncovered_pointer_target_refused(self):
        path = self.vault / "CHECKPOINT_REVIEW_LATEST.json"
        pointer = json.loads(path.read_bytes())
        pointer["review"] = "other/REVIEW.md"
        self.write(path, encoded(pointer))
        self.assert_block(self.inspect(), "vault_packages")

    def test_current_doc_drift_refused_then_restored(self):
        path = self.vault / "VAULT_FIRST_REUSE.md"
        original = path.read_bytes()
        self.write(path, b"unsealed edits")
        self.assert_block(self.inspect(), "current_documents")
        self.write(path, original)
        self.assertEqual(self.inspect()["status"], "pass")

    def test_dirty_source_and_untracked_file_preserved(self):
        path = self.root / "stage_c_alignment_integrity_v2" / "source.py"
        self.write(path, b"# unfinished numerical edit\n")
        self.write(self.root / "unknown.py", b"# user change\n")
        result = self.inspect()
        self.assert_block(result, "repository")
        self.assert_block(result, "engineering_source")
        self.assertEqual(path.read_bytes(), b"# unfinished numerical edit\n")
        self.assertTrue((self.root / "unknown.py").is_file())

    def test_committed_source_drift_still_refused(self):
        self.write(self.root / "stage_c_alignment_integrity_v2" / "source.py", b"# changed numerical source\n")
        self.commit()
        self.assert_block(self.inspect(expected_revision=self.run_git("rev-parse", "HEAD")), "engineering_source")

    def test_wrong_revision_then_exact_recovery(self):
        self.assert_block(self.inspect(expected_revision="0" * 40), "revision")
        self.assertEqual(self.inspect(expected_revision=self.head)["status"], "pass")

    def test_stale_final_receipt_refused(self):
        self.write(self.vault / "SHARED_GIT_REMOTE_LATEST.json", encoded({"branch": "forex", "handoff_document_commit": "0" * 40}))
        self.assert_block(self.inspect(), "revision")

    def test_wrong_branch_refused(self):
        self.run_git("checkout", "-b", "old-main")
        self.assert_block(self.inspect(), "revision")

    def test_pending_operational_review_refused_then_accepted(self):
        self.seal_gate("ready_for_review")
        self.assert_block(self.inspect(), "operational_gate")
        self.seal_gate("accepted_within_scope")
        self.assertEqual(self.inspect()["status"], "pass")

    def test_old_complete_gate_cannot_satisfy_new_pending_operational_step(self):
        self.queue["pre_research_gate"]["active_operational_step"] = "new-required-step"
        self.queue["operational_steps"].append({"step_id": "new-required-step",
            "implementation_status": "in_progress", "review_status": "pending"})
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assert_block(self.inspect(), "operational_gate")
        # Merely marking the newer queue complete still cannot reuse the old gate.
        self.queue["operational_steps"][-1].update({"implementation_status": "complete",
            "review_status": "accepted_within_scope"})
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assert_block(self.inspect(), "operational_gate")
        self.seal_gate(step_id="new-required-step", package="new-ops")
        self.assertEqual(self.inspect()["status"], "pass")

    def test_ambiguous_or_unbound_operational_queue_refused(self):
        entry = self.queue["operational_steps"][0]
        self.queue["operational_steps"].append(dict(entry))
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assert_block(self.inspect(), "operational_gate")
        self.queue["operational_steps"].pop()
        entry["packet_sha256"] = "0" * 64
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assert_block(self.inspect(), "operational_gate")

    def test_missing_operational_step_identity_refused(self):
        del self.queue["pre_research_gate"]["active_operational_step"]
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assert_block(self.inspect(), "operational_gate")

    def test_selected_item_cannot_skip_queue(self):
        self.assert_block(self.inspect(work_item="unrelated_old_experiment"), "operational_gate")

    def test_immediate_prerequisite_review_refused(self):
        self.queue["steps"][0]["review_status"] = "changes_requested"
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assert_block(self.inspect(), "operational_gate")

    def test_unrelated_historical_approval_does_not_block(self):
        self.queue["steps"].append({"step_id": "old_archived_missing_approval", "review_status": "blocked"})
        self.write(self.vault / "REVIEW_QUEUE.json", encoded(self.queue))
        self.seal_docs()
        self.assertEqual(self.inspect()["status"], "pass")

    def test_unresolved_owner_refused_and_terminal_history_accepted(self):
        self.board([("friend", "HANDOFF"), ("me", "IN_PROGRESS")])
        self.assert_block(self.inspect(claim_id="me"), "coordination")
        self.board([("friend", "DONE"), ("me", "IN_PROGRESS")])
        self.assertEqual(self.inspect(claim_id="me")["status"], "pass")

    def test_unknown_own_claim_refused(self):
        self.assert_block(self.inspect(claim_id="absent"), "coordination")

    def test_duplicate_owner_row_refused(self):
        self.board([("same", "DONE"), ("same", "IN_PROGRESS")])
        self.assert_block(self.inspect(claim_id="same"), "coordination")

    def test_requested_missing_artifact_never_fits_or_overwrites(self):
        self.assert_block(self.inspect(artifacts=["missing"]), "artifact:missing")
        self.assertFalse((self.vault / "missing.zip").exists())

    def test_corrupt_requested_artifact_refused(self):
        self.write(self.root / "artifacts" / "registry.json", encoded({"artifacts": {"bad": {
            "archive_vault_relative_path": "bad.zip", "archive_sha256": "0" * 64, "archive_bytes": 3}}}))
        self.commit()
        self.write(self.vault / "bad.zip", b"bad")
        result = self.inspect(expected_revision=self.run_git("rev-parse", "HEAD"), artifacts=["bad"])
        self.assert_block(result, "artifact:bad")
        self.assertEqual((self.vault / "bad.zip").read_bytes(), b"bad")

    def test_environment_mismatch_then_exact_metadata_match(self):
        kwargs = {"profile": "engineering"}
        with mock.patch.object(preflight, "environment_versions", return_value={"python": "3.12.10", "numpy": "0.0"}):
            self.assert_block(preflight.preflight(self.root, self.vault, **kwargs), "environment")
        with mock.patch.object(preflight, "environment_versions", return_value={"python": "3.12.10", "numpy": "2.5.1"}):
            self.assertEqual(preflight.preflight(self.root, self.vault, **kwargs)["status"], "pass")

    def test_recipe_requires_pin_and_matching_source(self):
        path = self.base / "recipe.json"
        recipe = {"environment": {"python": preflight.platform.python_version()},
                  "sources": {"source.py": hashlib.sha256((self.root / "stage_c_alignment_integrity_v2" / "source.py").read_bytes()).hexdigest()}}
        self.write(path, encoded(recipe))
        self.assert_block(self.inspect(recipe=path), "environment")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        self.assertEqual(self.inspect(recipe=path, recipe_sha256=digest)["status"], "pass")
        recipe["sources"]["source.py"] = "0" * 64
        self.write(path, encoded(recipe))
        self.assert_block(self.inspect(recipe=path, recipe_sha256=hashlib.sha256(path.read_bytes()).hexdigest()), "environment")

    def test_board_change_during_read_refused(self):
        original = preflight.Snapshot.unchanged
        def changed(snapshot):
            self.board([("new-owner", "IN_PROGRESS")])
            return original(snapshot)
        with mock.patch.object(preflight.Snapshot, "unchanged", changed):
            self.assert_block(self.inspect(), "snapshot_stability")

    def scope_review(self):
        self.board([("capture", "IN_PROGRESS"), ("me", "IN_PROGRESS")])
        other = preflight.active_claims((self.vault / "CHAT_COORDINATION_BOARD.md").read_text())[0]
        manifest = json.loads((self.vault / "docs/MANIFEST.json").read_text())
        manifest["coordination_reconciliations"] = [{"work_item": "next_science", "claim_id": "me",
            "decision": "nonoverlapping", "other_claim": other,
            "reason": "Fixture scope review: capture owns recorder; selected research owns isolated saved inputs."}]
        self.seal("docs", {"REVIEW.md": b"Accepted scope.\n"}, "CHECKPOINT_REVIEW_LATEST.json",
                  manifest, {"review": "docs/REVIEW.md"})

    def test_sealed_exact_scope_review_preserves_other_owner(self):
        self.scope_review()
        report = self.inspect(claim_id="me")
        self.assertEqual(report["status"], "pass", report)
        self.assertEqual(len(report["unresolved_claims"]), 2)

    def test_sealed_unclaimed_next_scope_does_not_take_other_ownership(self):
        self.scope_review()
        self.board([("capture", "IN_PROGRESS")])
        m = json.loads((self.vault / "docs/MANIFEST.json").read_text())
        m["coordination_reconciliations"][0]["claim_id"] = None
        self.seal("docs", {"REVIEW.md": b"Accepted scope.\n"}, "CHECKPOINT_REVIEW_LATEST.json",
                  m, {"review": "docs/REVIEW.md"})
        self.assertEqual(self.inspect()["status"], "pass")
        self.assert_block(self.inspect(claim_id="new-owner"), "coordination")

    def test_scope_review_cannot_cover_changed_or_additional_owner(self):
        self.scope_review()
        self.board([("capture", "HANDOFF"), ("me", "IN_PROGRESS")])
        self.assert_block(self.inspect(claim_id="me"), "coordination")
        self.board([("capture", "IN_PROGRESS"), ("me", "IN_PROGRESS"), ("unknown", "IN_PROGRESS")])
        self.assert_block(self.inspect(claim_id="me"), "coordination")

    def test_scope_review_cannot_be_reused_for_other_work_or_claim(self):
        self.scope_review()
        self.assert_block(self.inspect(), "coordination")
        self.assert_block(self.inspect(claim_id="me", work_item="unreviewed"), "coordination")

    def test_preflight_does_not_modify_project_or_vault(self):
        def inventory():
            return {str(p.relative_to(self.base)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in self.base.rglob("*") if p.is_file()}
        before = inventory()
        result = self.inspect()
        self.assertEqual(result["status"], "pass")
        self.assertEqual(before, inventory())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    args, rest = parser.parse_known_args()
    EVIDENCE = args.evidence.resolve()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[sys.argv[0], *rest], verbosity=2)
