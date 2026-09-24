"""Offline session guard tests; every fixture is confined to a temporary directory."""
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / "tools" / "forex_timed_session.py"
SPEC = importlib.util.spec_from_file_location("forex_timed_session", SOURCE)
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)


class SessionGuardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "session.json"
        self.zero = datetime(2026, 9, 24, 12, tzinfo=timezone.utc)
        self.counter = 0
        guard.create(self.path, owner="mabel", session_id="test-session",
                     started_utc=self.zero, deadline_utc=self.zero + timedelta(hours=4),
                     step_id="partial-step", resume_command="resume exact-recipe.json",
                     evidence_path=self.proof({"request": "four hours"}), now=self.zero)

    def proof(self, value=None):
        self.counter += 1
        path = self.root / f"evidence-{self.counter}.json"
        path.write_text(json.dumps(value or {"observation": self.counter}), encoding="utf-8")
        return path

    def move(self, action, seconds, **kwargs):
        kwargs.setdefault("evidence_path", self.proof())
        return guard.transition(self.path, owner="mabel", action=action,
                                now=self.zero + timedelta(seconds=seconds), **kwargs)

    def start(self, seconds=1, turn="turn-a"):
        return self.move("start", seconds, turn_id=turn)

    def register_backend(self, seconds=0.5):
        return self.move("register-backend", seconds, evidence_path=self.backend_receipt())

    def backend_receipt(self, **overrides):
        return self.proof({"id": "synthetic-fixture-only", "kind": "heartbeat", "status": "ACTIVE",
                           "session_id": "test-session", "deadline_utc": "2026-09-24T16:00:00Z",
                           "tool_result_path": str(self.proof({"fixture": "synthetic tool result"})),
                           "original_request_path": str(self.proof({"fixture": "synthetic original request"})),
                           **overrides})

    def state(self):
        return guard.load(self.path, "mabel")

    def assert_refused_unchanged(self, operation, pattern):
        before = self.path.read_bytes()
        with self.assertRaisesRegex(guard.Refusal, pattern):
            operation()
        self.assertEqual(before, self.path.read_bytes())
        self.assertFalse(self.path.with_name(self.path.name + ".lock").exists())

    def test_checkpoint_does_not_authorize_early_finish(self):
        self.start()
        self.move("checkpoint", 20, turn_id="turn-a", next_step="next-step",
                  resume_command="resume next.json")
        detail = self.proof({"reason": "normal", "detail": "One package finished",
                             "authorized_unfinished_work": True,
                             "authorized_unfinished_eligible_work": True})
        self.assert_refused_unchanged(lambda: self.move("finish", 21, turn_id="turn-a",
                                                       reason="normal", evidence_path=detail), "continue")
        self.assertEqual(self.state()["current_step_id"], "next-step")

    def test_yield_and_resume_exclude_idle_gap_keep_original_deadline(self):
        original = self.state()["contract"].copy()
        self.register_backend()
        self.start()
        result = self.move("yield", 21, turn_id="turn-a")
        self.assertEqual(result["status"], "awaiting_continuation")
        self.assertEqual(result["observed_active_seconds"], 20)
        self.assertTrue(result["backend_receipt_registered"])
        resumed = self.start(1000, "turn-b")
        self.assertEqual(resumed["current_step_id"], "partial-step")
        self.assertEqual(resumed["resume_command"], "resume exact-recipe.json")
        result = self.move("yield", 1010, turn_id="turn-b")
        self.assertEqual(result["observed_active_seconds"], 30)
        self.assertEqual(self.state()["contract"], original)

    def test_unverified_long_active_gap_is_not_credited(self):
        self.register_backend()
        self.start()
        self.move("touch", 21, turn_id="turn-a")
        stale = guard.report(self.state(), self.zero + timedelta(seconds=500))
        self.assertEqual(stale["status"], "active_observation_stale")
        self.move("touch", 1000, turn_id="turn-a")
        result = self.move("yield", 1010, turn_id="turn-a")
        self.assertEqual(result["observed_active_seconds"], 30)
        self.assertEqual(len(self.state()["segments"]), 2)

    def test_owner_clock_and_replayed_evidence_refuse_without_mutating(self):
        proof = self.proof()
        self.assert_refused_unchanged(lambda: guard.transition(self.path, owner="g", action="start",
                                                               evidence_path=proof, turn_id="g-turn",
                                                               now=self.zero + timedelta(seconds=1)), "owner")
        self.assert_refused_unchanged(lambda: self.start(0), "clock")
        self.move("start", 1, turn_id="turn-a", evidence_path=proof)
        self.assert_refused_unchanged(lambda: self.move("touch", 2, turn_id="turn-a", evidence_path=proof), "replay")
        self.assert_refused_unchanged(lambda: self.move("touch", 1, turn_id="turn-a"), "clock")
        self.assert_refused_unchanged(lambda: self.move("touch", 2, turn_id="wrong"), "turn mismatch")

    def test_missing_and_empty_evidence_fail_closed(self):
        self.assert_refused_unchanged(lambda: self.move("start", 1, turn_id="turn-a",
                                                       evidence_path=self.root / "missing"), "existing")
        empty = self.root / "empty"
        empty.touch()
        self.assert_refused_unchanged(lambda: self.move("start", 1, turn_id="turn-a", evidence_path=empty), "nonempty")
        self.assert_refused_unchanged(lambda: self.move("start", 1, turn_id="turn-a", evidence_path=self.path), "own work evidence")

    def test_no_overlapping_turns_and_explicit_recovery(self):
        self.start()
        self.move("touch", 21, turn_id="turn-a")
        self.assert_refused_unchanged(lambda: self.start(1000, "turn-b"), "already active")
        detail = self.proof({"previous_turn_id": "turn-a", "previous_turn_inactive": True,
                             "detail": "Task final reply observed; no worker was launched"})
        result = self.move("abandon-turn", 1001, evidence_path=detail)
        self.assertEqual(result["observed_active_seconds"], 20)
        self.assertEqual(result["status"], "awaiting_continuation")
        self.start(1002, "turn-b")

    def test_backend_receipt_never_starts_worker_or_extends_clock(self):
        original = self.state()["contract"].copy()
        receipt = self.backend_receipt()
        result = self.move("register-backend", 1, evidence_path=receipt)
        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["backend_receipt_registered"])
        self.assertFalse(result["worker_liveness_verified"])
        self.start(2)
        result = self.move("yield", 10, turn_id="turn-a")
        self.assertEqual(result["status"], "awaiting_continuation")
        self.assertFalse(result["worker_liveness_verified"])
        self.assertEqual(self.state()["contract"], original)

    def test_backend_metadata_cannot_refresh_stale_work_observation(self):
        self.start()
        receipt = self.backend_receipt()
        result = self.move("register-backend", 1000, evidence_path=receipt)
        self.assertEqual(result["status"], "active_observation_stale")
        self.assertEqual(result["observed_active_seconds"], 0)

    def test_original_timestamp_text_is_preserved_at_input_precision(self):
        other = self.root / "other.json"
        start, deadline = "2026-09-24T11:00:00.0000003Z", "2026-09-24T15:00:00.0000003Z"
        result = guard.create(other, owner="mabel", session_id="precise", started_utc=start,
                              deadline_utc=deadline, step_id="exact", resume_command="resume exact",
                              evidence_path=self.proof(), now=self.zero)
        self.assertEqual(result["original_started_utc"], start)
        self.assertEqual(result["original_deadline_utc"], deadline)

    def test_invalid_backend_receipt_and_incomplete_blocker_record_refuse(self):
        bad = self.proof({"id": "not-active", "kind": "heartbeat", "status": "PAUSED"})
        self.assert_refused_unchanged(lambda: self.move("register-backend", 1, evidence_path=bad), "ACTIVE")
        bad = self.proof({"reason": "genuine_all_paths_blocked", "detail": "One item blocked",
                          "authorized_unfinished_eligible_work": False, "all_paths_reviewed": True,
                          "blocked_paths": [{"step_id": "one", "reason": "No evidence"}]})
        self.assert_refused_unchanged(lambda: self.move("finish", 1, reason="genuine_all_paths_blocked",
                                                       evidence_path=bad), "Evidence path")

    def test_deadline_finish_and_refusal_to_restart(self):
        self.start()
        detail = self.proof({"reason": "deadline", "detail": "Time expired; checkpoint safe",
                             "authorized_unfinished_eligible_work": True})
        self.assert_refused_unchanged(lambda: self.move("finish", 2, turn_id="turn-a",
                                                       reason="deadline", evidence_path=detail), "not arrived")
        result = self.move("finish", 14400, turn_id="turn-a", reason="deadline", evidence_path=detail)
        self.assertEqual(result["finish_reason"], "deadline")
        self.assertEqual(result["observed_active_seconds"], 0)
        self.assert_refused_unchanged(lambda: self.start(14401, "turn-b"), "Finished")

    def test_early_completion_requires_no_authorized_unfinished_work(self):
        self.start()
        proof = self.proof({"reason": "all_work_complete", "detail": "Reviewed full authorized queue",
                            "authorized_unfinished_work": False, "authorized_unfinished_eligible_work": False})
        result = self.move("finish", 21, turn_id="turn-a", reason="all_work_complete", evidence_path=proof)
        self.assertEqual(result["finish_reason"], "all_work_complete")
        self.assertEqual(result["observed_active_seconds"], 20)

    def test_single_blocker_cannot_finish_with_eligible_siblings(self):
        self.start()
        blocker = self.proof({"reason": "no_data"})
        detail = {"reason": "genuine_all_paths_blocked", "detail": "Branch review",
                  "authorized_unfinished_eligible_work": True, "all_paths_reviewed": True,
                  "blocked_paths": [{"step_id": "one", "reason": "Missing cohort", "evidence_path": str(blocker)}]}
        self.assert_refused_unchanged(lambda: self.move("finish", 10, turn_id="turn-a",
                                                       reason="genuine_all_paths_blocked", evidence_path=self.proof(detail)), "branch scan")
        detail["authorized_unfinished_eligible_work"] = False
        result = self.move("finish", 11, turn_id="turn-a", reason="genuine_all_paths_blocked",
                           evidence_path=self.proof(detail))
        self.assertEqual(result["finish_reason"], "genuine_all_paths_blocked")

    def test_platform_and_user_stop_need_actual_supporting_file(self):
        for reason in ("platform_limit", "user_stop"):
            detail = {"reason": reason, "detail": "External stopping condition",
                      "authorized_unfinished_eligible_work": True}
            self.assert_refused_unchanged(lambda: self.move("finish", 1, reason=reason,
                                                           evidence_path=self.proof(detail)), "Evidence path")
        detail["supporting_evidence_path"] = str(self.proof({"user": "stop now"}))
        result = self.move("finish", 2, reason="user_stop", evidence_path=self.proof(detail))
        self.assertEqual(result["finish_reason"], "user_stop")

    def test_contract_tamper_overlap_and_existing_lock_fail_closed(self):
        original = self.path.read_bytes()
        state = json.loads(original)
        state["contract"]["original_deadline_utc"] = "2026-09-24T17:00:00Z"
        self.path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaisesRegex(guard.Refusal, "modified"):
            self.state()
        self.path.write_bytes(original)
        self.register_backend()
        self.start()
        self.move("yield", 21, turn_id="turn-a")
        self.start(30, "turn-b")
        state = self.state()
        state["segments"][-1]["started_utc"] = guard.stamp(self.zero + timedelta(seconds=10))
        self.path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaisesRegex(guard.Refusal, "Overlapping"):
            self.state()
        self.path.write_bytes(original)
        lock = self.path.with_name(self.path.name + ".lock")
        lock.write_text("some other writer", encoding="utf-8")
        with self.assertRaisesRegex(guard.Refusal, "lock already exists"):
            self.start()
        self.assertTrue(lock.exists())
        self.assertEqual(self.path.read_bytes(), original)

    def test_no_backend_yield_refuses_unchanged_before_deadline(self):
        self.start()
        self.assert_refused_unchanged(lambda: self.move("yield", 21, turn_id="turn-a"),
                                     "Cannot yield before deadline")
        self.assertEqual(self.state()["status"], "running")

    def test_unrelated_session_or_deadline_receipt_cannot_enable_yield(self):
        for overrides in ({"session_id": "unrelated"}, {"deadline_utc": "2026-09-24T17:00:00Z"}):
            receipt = self.backend_receipt(**overrides)
            self.assert_refused_unchanged(lambda: self.move("register-backend", 1, evidence_path=receipt),
                                         "exact session and original deadline")

    def test_expired_or_unsubstantiated_backend_receipt_is_refused(self):
        self.assert_refused_unchanged(lambda: self.move("register-backend", 14401,
                                                       evidence_path=self.backend_receipt()), "expired")
        receipt = self.backend_receipt(tool_result_path=str(self.root / "missing"))
        self.assert_refused_unchanged(lambda: self.move("register-backend", 1, evidence_path=receipt),
                                     "existing regular file")

    def test_changed_or_missing_continuation_receipt_refuses_yield(self):
        self.register_backend()
        self.start()
        receipt = Path(self.state()["backend_receipt"]["receipt"]["path"])
        receipt.write_text('{"status":"PAUSED"}', encoding="utf-8")
        self.assert_refused_unchanged(lambda: self.move("yield", 21, turn_id="turn-a"), "receipt changed")
        receipt.unlink()
        self.assert_refused_unchanged(lambda: self.move("yield", 21, turn_id="turn-a"), "existing regular file")

    def test_changed_tool_result_cannot_enable_yield(self):
        self.register_backend()
        self.start()
        tool_result = Path(self.state()["backend_receipt"]["tool_result"]["path"])
        tool_result.write_text('{"status":"revoked"}', encoding="utf-8")
        self.assert_refused_unchanged(lambda: self.move("yield", 21, turn_id="turn-a"),
                                     "supporting evidence changed")

    def test_user_stop_prevents_all_automatic_resume_paths(self):
        self.register_backend()
        self.start()
        detail = self.proof({"reason": "user_stop", "detail": "User explicitly said do not resume",
                             "authorized_unfinished_eligible_work": True,
                             "supporting_evidence_path": str(self.proof({"user": "Do not resume"}))})
        self.move("finish", 21, turn_id="turn-a", reason="user_stop", evidence_path=detail)
        for action in ("start", "register-backend", "checkpoint", "touch", "yield", "abandon-turn"):
            self.assert_refused_unchanged(lambda: self.move(action, 22, turn_id="turn-b",
                                                           next_step="next", resume_command="resume next"), "Finished")

    def test_cli_status_is_read_only_and_refusal_has_exit_two(self):
        before = self.path.read_bytes()
        result = subprocess.run([sys.executable, "-I", "-B", str(SOURCE), "status", "--state", str(self.path),
                                 "--owner", "mabel"], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse(json.loads(result.stdout)["worker_liveness_verified"])
        result = subprocess.run([sys.executable, "-I", "-B", str(SOURCE), "start", "--state", str(self.path),
                                 "--owner", "g", "--turn-id", "bad", "--evidence", str(self.proof())],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)["status"], "refused")
        self.assertEqual(self.path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
