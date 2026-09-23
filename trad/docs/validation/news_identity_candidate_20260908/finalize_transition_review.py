"""Bind the completed static transition review; evidence writes only."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json

OUT = Path(__file__).resolve().parent

def binding(name):
    path = OUT / name
    raw = path.read_bytes()
    return {"path": str(path), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}

capture = json.loads((OUT / "JOINT_NEWS_VERSION_TRANSITION_20260908.json").read_bytes())
assert all(item["matches"] for item in capture["registered_source_checks"])
assert all(item["contract_hash_matches"] and item["source_bindings_match_registry"] for item in capture["registered_contracts"])
report = {
    "schema": "joint_news_transition_assessment_v1_20260908",
    "status": "completed_readonly_offline_plan",
    "assessed_utc": datetime.now(timezone.utc).isoformat(),
    "source_observed_utc": capture["observed_utc"],
    "bindings": [binding("JOINT_NEWS_VERSION_TRANSITION_20260908.json"), binding("JOINT_NEWS_VERSION_TRANSITION_20260908.md"), binding("capture_transition_bindings.py"), binding("finalize_transition_review.py")],
    "registered_sources_verified": 16,
    "per_pair_contracts_verified": 68,
    "current_phase": "offline candidate only; no deployment, registration or runtime changes",
    "publication_only_transition": {
        "condition": "Original classified members and governed content remain unchanged; only reconciliation/publication implementation changes.",
        "may_preserve_classifier_semantic_version": True,
        "may_read_existing_governed_history_unchanged": True,
        "new_news_database_required": False,
        "new_governance_worker_required": False,
        "new_source_bindings_and_forecast_cohort_required_if_deployed": True,
        "original_clocks_hashes_visibility_and_expiry_preserved": True,
    },
    "changed_member_transition": {
        "condition": "Classification version or governed member content changes.",
        "requirement": "Separate or explicitly partitioned new ingestion; do not contaminate the old reader-selected mapping rows. Preserve each original member and its availability receipts.",
    },
    "clarification_of_raw_capture": "Its new_history path and new governance module are conditional options for changed classified/governed content, not required for publication-only repair.",
    "old_study_continuation": "Permissible under original unchanged source/output/history semantics; this static audit makes no runtime-health assertion.",
    "deployment_actions": 0,
    "runtime_or_database_actions": 0,
    "canonical_source_or_registration_edits": 0,
    "all_order_promotion_authorization_account_proof_flags_to_remain_false": True,
}
target = OUT / "JOINT_NEWS_VERSION_TRANSITION_ASSESSMENT_20260908.json"
with target.open("x", encoding="utf-8", newline="\n") as handle:
    json.dump(report, handle, indent=2, sort_keys=True)
    handle.write("\n")
print(json.dumps({"assessment": binding(target.name), "report": binding("JOINT_NEWS_VERSION_TRANSITION_20260908.md")}))
