#!/usr/bin/env python3
"""Local timed-turn accounting and finish guard, not a worker or scheduler.

All mutations require the recorded owner and strictly increasing real UTC (the CLI
does not accept a clock override). Evidence-linked observations credit at most 120
seconds between touches; longer gaps earn no active time. The original wall-clock
deadline never moves. Evidence and scheduler receipts are operator declarations,
not independent proof of worker liveness. This helper cannot prevent an agent from
ignoring it or guarantee that a registered scheduler will actually resume work.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile

SCHEMA = "forex_timed_session_guard.v1"
MAX_GAP_SECONDS = 120
MAX_BYTES = 16 * 1024 * 1024
FINISH_REASONS = {"normal", "user_stop", "deadline", "all_work_complete",
                  "genuine_all_paths_blocked", "platform_limit"}


class Refusal(ValueError):
    """An unsafe or insufficiently evidenced state transition."""


def require(condition, message):
    if not condition:
        raise Refusal(message)


def utc(value=None):
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        result = value
    else:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(result.tzinfo is not None and result.utcoffset().total_seconds() == 0,
            "UTC timestamp with explicit offset required")
    return result.astimezone(timezone.utc)


def stamp(value):
    return utc(value).isoformat().replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def object_json(raw):
    value = json.loads(raw)
    require(isinstance(value, dict), "JSON object required")
    return value


def plain_path(path):
    result = Path(os.path.abspath(path))
    for part in (result, *result.parents):
        require(not part.is_symlink(), "Symlink paths are not permitted")
        require(not getattr(part, "is_junction", lambda: False)(), "Junction paths are not permitted")
    return result


def evidence(path):
    require(path is not None, "Evidence path required")
    path = plain_path(path)
    require(path.is_file(), "Evidence must be an existing regular file")
    before = path.stat()
    require(0 < before.st_size <= MAX_BYTES, "Evidence must be nonempty and bounded")
    raw = path.read_bytes()
    after = path.stat()
    require((before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns),
            "Evidence changed during read")
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw)}, raw


@contextmanager
def locked(path):
    path = plain_path(path)
    require(path.parent.is_dir(), "State parent directory must already exist")
    lock = path.with_name(path.name + ".lock")
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise Refusal("Session lock already exists; inspect its owner before recovery") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(str(os.getpid()))
        yield path
    finally:
        lock.unlink()


def save(path, state):
    validate(state)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(state, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def validate(state):
    require(state.get("schema") == SCHEMA, "Unsupported session schema")
    contract = state["contract"]
    require(state["contract_sha256"] == digest(contract), "Original contract was modified")
    start, deadline = utc(contract["original_started_utc"]), utc(contract["original_deadline_utc"])
    require(deadline > start and contract["duration_seconds"] == (deadline - start).total_seconds(),
            "Invalid original start/deadline")
    require(contract["max_unverified_gap_seconds"] == MAX_GAP_SECONDS, "Gap policy changed")
    observed = utc(state["last_observed_utc"])
    require(observed >= start, "Clock precedes original start")
    previous = None
    for event in state["events"]:
        current = utc(event["utc"])
        require(previous is None or current > previous, "Replayed or stale event clock")
        previous = current
    require(previous == observed, "Last observation does not match event log")
    previous_end, open_count = start, 0
    for segment in state["segments"]:
        begin, verified = utc(segment["started_utc"]), utc(segment["verified_until_utc"])
        require(previous_end <= begin <= verified <= observed, "Overlapping or invalid segments")
        if segment["ended_utc"] is None:
            open_count += 1
            require(segment is state["segments"][-1], "Only last segment may remain open")
        else:
            require(utc(segment["ended_utc"]) == verified, "Unverified segment end")
        previous_end = verified
    require(open_count == (1 if state["status"] == "running" else 0), "Active segment/status mismatch")
    require(bool(state["active_turn_id"]) == (open_count == 1), "Active turn/status mismatch")
    if open_count:
        require(state["segments"][-1]["turn_id"] == state["active_turn_id"], "Active segment owner mismatch")
    require(state["status"] in {"ready", "running", "awaiting_continuation", "finished"}, "Invalid status")
    require(bool(state["current_step_id"]) and bool(state["resume_command"]), "Exact resume required")


def load(path, owner):
    record, raw = evidence(path)
    state = object_json(raw)
    validate(state)
    require(state["contract"]["owner"] == owner, "Session owner mismatch")
    return state


def create(path, *, owner, session_id, started_utc, deadline_utc, step_id,
           resume_command, evidence_path, now=None):
    now = utc(now)
    start, deadline = utc(started_utc), utc(deadline_utc)
    require(owner and session_id and step_id and resume_command, "Identity and exact resume required")
    require(start <= now and deadline > start, "Invalid original clock")
    proof, _ = evidence(evidence_path)
    contract = {"owner": owner, "session_id": session_id,
                "original_started_utc": started_utc if isinstance(started_utc, str) else stamp(start),
                "original_deadline_utc": deadline_utc if isinstance(deadline_utc, str) else stamp(deadline),
                "duration_seconds": (deadline - start).total_seconds(),
                "max_unverified_gap_seconds": MAX_GAP_SECONDS}
    state = {"schema": SCHEMA, "contract": contract, "contract_sha256": digest(contract),
             "status": "ready", "last_observed_utc": stamp(now), "current_step_id": step_id,
             "resume_command": resume_command, "active_turn_id": None, "segments": [],
             "backend_receipt": None, "finish_reason": None,
             "events": [{"action": "create", "utc": stamp(now), "evidence": proof}]}
    with locked(path) as target:
        require(not target.exists(), "Session already exists; resume it without resetting its clock")
        save(target, state)
    return report(state, now)


def report(state, now=None):
    now = utc(now)
    require(now >= utc(state["last_observed_utc"]), "Status clock is stale")
    active = sum((utc(x["verified_until_utc"]) - utc(x["started_utc"])).total_seconds()
                 for x in state["segments"])
    observed_status = state["status"]
    if observed_status == "running" and (now - utc(state["segments"][-1]["verified_until_utc"])).total_seconds() > MAX_GAP_SECONDS:
        observed_status = "active_observation_stale"
    return {"session_id": state["contract"]["session_id"], "status": observed_status,
            "recorded_status": state["status"], "observed_active_seconds": active,
            "wall_elapsed_seconds": (now - utc(state["contract"]["original_started_utc"])).total_seconds(),
            "seconds_until_original_deadline": max(0, (utc(state["contract"]["original_deadline_utc"]) - now).total_seconds()),
            "original_started_utc": state["contract"]["original_started_utc"],
            "original_deadline_utc": state["contract"]["original_deadline_utc"],
            "current_step_id": state["current_step_id"], "resume_command": state["resume_command"],
            "backend_receipt_registered": state["backend_receipt"] is not None,
            "worker_liveness_verified": False, "finish_reason": state["finish_reason"]}


def advance_active(state, now, turn_id):
    require(state["status"] == "running" and turn_id == state["active_turn_id"], "Active turn mismatch")
    segment = state["segments"][-1]
    prior = utc(segment["verified_until_utc"])
    if (now - prior).total_seconds() > MAX_GAP_SECONDS:
        segment["ended_utc"] = segment["verified_until_utc"]
        state["segments"].append({"turn_id": turn_id, "started_utc": stamp(now),
                                  "verified_until_utc": stamp(now), "ended_utc": None})
    else:
        segment["verified_until_utc"] = stamp(now)


def close_active(state):
    if state["status"] == "running":
        segment = state["segments"][-1]
        segment["ended_utc"] = segment["verified_until_utc"]
    state["active_turn_id"] = None


def require_registered_continuation(state):
    backend = state["backend_receipt"]
    require(isinstance(backend, dict) and backend.get("status") == "ACTIVE",
            "Cannot yield before deadline without an ACTIVE registered continuation receipt; "
            "keep working or record an evidenced explicit stop")
    require(backend.get("session_id") == state["contract"]["session_id"]
            and backend.get("deadline_utc") == state["contract"]["original_deadline_utc"],
            "Continuation receipt does not bind this session")
    current, _ = evidence(backend["receipt"]["path"])
    require(current == backend["receipt"], "Continuation receipt changed after registration")
    for name in ("tool_result", "original_request"):
        current, _ = evidence(backend[name]["path"])
        require(current == backend[name], "Continuation supporting evidence changed after registration")


def transition(path, *, owner, action, evidence_path, turn_id=None, reason=None,
               next_step=None, resume_command=None, now=None):
    now = utc(now)
    proof, raw = evidence(evidence_path)
    with locked(path) as target:
        require(Path(proof["path"]) != target, "Session state cannot serve as its own work evidence")
        state = load(target, owner)
        require(now > utc(state["last_observed_utc"]), "Replayed or stale clock")
        require(state["status"] != "finished", "Finished session cannot be restarted")
        require(proof["sha256"] not in {x["evidence"]["sha256"] for x in state["events"]},
                "Evidence replay; record a fresh observation")
        if action == "start":
            require(state["status"] in {"ready", "awaiting_continuation"}, "A turn is already active")
            require(now < utc(state["contract"]["original_deadline_utc"]), "Original deadline has passed")
            require(turn_id and turn_id not in {x["turn_id"] for x in state["segments"]}, "Fresh turn ID required")
            state["segments"].append({"turn_id": turn_id, "started_utc": stamp(now),
                                      "verified_until_utc": stamp(now), "ended_utc": None})
            state["active_turn_id"], state["status"] = turn_id, "running"
        elif action in {"touch", "yield", "checkpoint"}:
            if action == "yield" and now < utc(state["contract"]["original_deadline_utc"]):
                require_registered_continuation(state)
            advance_active(state, now, turn_id)
            if action == "yield":
                close_active(state)
                state["status"] = "awaiting_continuation"
            elif action == "checkpoint":
                require(next_step and resume_command, "Checkpoint needs exact next step and resume command")
                state["current_step_id"], state["resume_command"] = next_step, resume_command
        elif action == "register-backend":
            receipt = object_json(raw)
            require(receipt.get("id") and receipt.get("kind") == "heartbeat" and receipt.get("status") == "ACTIVE",
                    "Receipt must identify an ACTIVE heartbeat; registration does not prove execution")
            require(receipt.get("session_id") == state["contract"]["session_id"]
                    and receipt.get("deadline_utc") == state["contract"]["original_deadline_utc"],
                    "Continuation receipt must bind this exact session and original deadline")
            require(now < utc(receipt["deadline_utc"]), "Cannot register an expired continuation receipt")
            tool_result, _ = evidence(receipt.get("tool_result_path"))
            original_request, _ = evidence(receipt.get("original_request_path"))
            state["backend_receipt"] = {"receipt": proof, "id": receipt["id"], "kind": "heartbeat",
                                        "status": "ACTIVE", "session_id": receipt["session_id"],
                                        "deadline_utc": receipt["deadline_utc"], "tool_result": tool_result,
                                        "original_request": original_request,
                                        "registered_utc": stamp(now), "liveness_verified": False}
        elif action == "abandon-turn":
            detail = object_json(raw)
            require(state["status"] == "running" and detail.get("previous_turn_id") == state["active_turn_id"]
                    and detail.get("previous_turn_inactive") is True and bool(detail.get("detail")),
                    "Recovery requires evidence identifying the inactive prior turn")
            close_active(state)
            state["status"] = "awaiting_continuation"
        elif action == "finish":
            require(reason in FINISH_REASONS, "Unknown finish reason")
            detail = object_json(raw)
            require(detail.get("reason") == reason and bool(detail.get("detail")), "Finish reason evidence required")
            require(type(detail.get("authorized_unfinished_eligible_work")) is bool,
                    "Fresh eligible-work assessment required")
            if reason in {"normal", "deadline"} and now >= utc(state["contract"]["original_deadline_utc"]):
                reason = "deadline"
            elif reason in {"normal", "all_work_complete"}:
                require(detail.get("authorized_unfinished_work") is False
                        and detail["authorized_unfinished_eligible_work"] is False,
                        "Useful authorized work remains; continue instead of finishing")
                reason = "all_work_complete"
            elif reason == "deadline":
                raise Refusal("Original deadline has not arrived")
            elif reason == "genuine_all_paths_blocked":
                require(detail["authorized_unfinished_eligible_work"] is False
                        and detail.get("all_paths_reviewed") is True
                        and isinstance(detail.get("blocked_paths"), list) and detail["blocked_paths"],
                        "All-path blocker requires a branch scan and no eligible sibling work")
                for blocked in detail["blocked_paths"]:
                    require(isinstance(blocked, dict) and blocked.get("step_id") and blocked.get("reason"),
                            "Each blocked path needs a reason")
                    evidence(blocked.get("evidence_path"))
            elif reason in {"user_stop", "platform_limit"}:
                evidence(detail.get("supporting_evidence_path"))
            if state["status"] == "running":
                advance_active(state, now, turn_id)
                close_active(state)
            state["status"], state["finish_reason"] = "finished", reason
        else:
            raise Refusal("Unknown action")
        state["last_observed_utc"] = stamp(now)
        state["events"].append({"action": action, "utc": stamp(now), "evidence": proof})
        save(target, state)
    return report(state, now)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["create", "start", "touch", "checkpoint", "yield",
                                           "register-backend", "abandon-turn", "finish", "status"])
    parser.add_argument("--state", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--evidence")
    parser.add_argument("--session-id")
    parser.add_argument("--started-utc")
    parser.add_argument("--deadline-utc")
    parser.add_argument("--step-id")
    parser.add_argument("--next-step")
    parser.add_argument("--resume-command")
    parser.add_argument("--turn-id")
    parser.add_argument("--reason", choices=sorted(FINISH_REASONS))
    args = parser.parse_args()
    try:
        if args.action == "create":
            require(args.started_utc and args.deadline_utc, "Original start/deadline required")
            result = create(args.state, owner=args.owner, session_id=args.session_id,
                            started_utc=args.started_utc, deadline_utc=args.deadline_utc,
                            step_id=args.step_id, resume_command=args.resume_command, evidence_path=args.evidence)
        elif args.action == "status":
            result = report(load(args.state, args.owner))
        else:
            result = transition(args.state, owner=args.owner, action=args.action, evidence_path=args.evidence,
                                turn_id=args.turn_id, reason=args.reason, next_step=args.next_step,
                                resume_command=args.resume_command)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError, IndexError) as exc:
        print(json.dumps({"status": "refused", "reason": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
