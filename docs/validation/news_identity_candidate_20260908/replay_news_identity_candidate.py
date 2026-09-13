"""Replay the retained September 8 failure; never publish to the live project."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "workspace" / "trad"
sys.dont_write_bytecode = True
sys.path[:0] = [str(HERE), str(SOURCE)]

import oanda_news_causal_aggregation_guard_v1 as guard
from news_topic_identity_reconcile_candidate_v1 import reconcile_topic_identities_with_provenance

case_path = HERE.parent / "runtime" / "NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json"
case_bytes = case_path.read_bytes()
assert hashlib.sha256(case_bytes).hexdigest() == "7f76c161ae89eb72470c61210874372fd4c102131e76137fc761ebb46fb2284d"
case = json.loads(case_bytes)
inputs = case["conflicts"][0]["topics"]
at = dt.datetime.fromisoformat(case["guarded_snapshot"]["as_of_utc"])
before = guard.build_current_news_snapshot(inputs, as_of=at)
reconciliation = reconcile_topic_identities_with_provenance(inputs, as_of=at)
after = guard.build_current_news_snapshot(reconciliation["topics"], as_of=at)
member_map = lambda topics: {m["event_id"]: m for topic in topics
                            for m in topic["causal_aggregation_guard"]["members"]}
assert before["errors"] == ["conflicting_current_topic_identity"]
assert after["status"] == "current" and after["news_state"] == "context_only"
assert after["topic_count"] == 1 and after["directional_topic_count"] == 0
assert member_map(inputs) == member_map(after["topics"])
assert len(member_map(after["topics"])) == 17
bindings = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (
    case_path, Path(guard.__file__), HERE / "news_topic_identity_reconcile_candidate_v1.py", Path(__file__)
)}
result = {
    "schema": "retained_news_identity_replay_offline_v1_20260908",
    "status": "passed",
    "replay_recorded_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    "original_case_as_of_utc": at.isoformat(),
    "scope": "Pure offline replay of two retained topics at their original cutoff. The before/after current status refers only to that fixed historical cutoff. This file is not a current producer publication or forecast input.",
    "source_bindings": bindings,
    "before": before,
    "reconciliation": reconciliation,
    "after": after,
    "all_17_original_member_payloads_preserved": True,
    "all_members_same_claim_asserted": False,
    "deployment": {"deployed": False, "registry_changed": False,
                   "old_guard_changed": False, "old_forecasts_rescored": False,
                   "prospective_result": False, "can_place_orders": False,
                   "can_promote": False},
}
target = HERE / "CANDIDATE_RETAINED_CASE_REPLAY_20260908.json"
target.write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
print(json.dumps({"status": "passed", "input_topics": len(inputs), "output_topics": after["topic_count"],
                  "preserved_members": 17, "news_state_at_original_cutoff": after["news_state"],
                  "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}))
