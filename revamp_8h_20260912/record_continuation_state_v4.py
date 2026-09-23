"""Record the completed, inactive continuation without rewriting older states."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(r"C:\Users\zmoor\Documents\forex\revamp_8h_20260912")
PUB = ROOT / "publication_checkpoint_006"
DEST = ROOT / "CONTINUATION_STATE_004.json"

def digest(path):
    data = path.read_bytes()
    return {"path": str(path), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}

def pinned(relative, expected):
    result = digest(ROOT / relative)
    assert result["sha256"] == expected, (relative, result["sha256"])
    return result

assert not DEST.exists(), "Existing state must remain immutable"
artifacts = {
    "richer_completion": pinned("direction_richer_archive_003/evaluation_004/COMPLETION_RECEIPT.json", "ece3966b18c0e79636e7fdc9cf1d11f9ae378c0539f2ec84e9b7c7ff263470fb"),
    "richer_replay": pinned("direction_richer_archive_003/SAVED_ARTIFACT_RECREATION_004.json", "d9caa31c277de0c8f2de366223694b9d75a0e2e204671aa67626f1ed79937bcf"),
    "richer_metrics": pinned("direction_richer_archive_003/results_recomputed_004/INDEPENDENT_RESULTS.json", "090ed1c3ccbc3ba9d721a55c14013d973ac002e5f7c3327c1461785bb2155292"),
    "execution_provenance": pinned("direction_richer_archive_003/EXECUTION_PROVENANCE_ACCEPTANCE_004.json", "ecfa76476b41c1cb9b81b6f51349dd35d6a416ad286d2f3d9658befa4e963182"),
    "concentration": pinned("direction_richer_archive_003/results_concentration_004/CONCENTRATION_001.json", "701688acd8f8ec17ceb6dd8302058e1bb1f88a62f1e4864fccf8cac9b1bfd7fa"),
    "typed_package_publication": pinned("runtime/typed_outcome_package_001/PUBLICATION_RECEIPT_001.json", "d2de2178140958949c5ef14f60ee07493ad3dbfba4a160d8a69aad7ad175bb11"),
    "final_process_observation": pinned("runtime/market_open_readiness_001/PROCESS_OBSERVATION_003.json", "56081d19ab63ad22f4864af61b20450374e056c856601227df76ac516d0d74a1"),
    "publication_plan": pinned("publication_checkpoint_006/FINAL_PLAN_002.json", "414cfc879b8e593f16696b84f3eb76a2bc288e2c6748a0abc3748529f92969d8"),
    "publisher": pinned("publication_checkpoint_006/publish_checkpoint_v1.py", "c82553c4dcb4c33c4fa9d3d65a3cbf307ed26b15ad2ec62476d8b0e94536c702"),
    "publication_receipt": digest(PUB / "execution_001/PUBLICATION_RECEIPT.json"),
}
overview = digest(ROOT.parent / "trad/docs/FOREX_CURRENT_STATE_20260913.md")
assert overview["sha256"] == "058b81e6a94753329635a4b264e937f2a87f38e591f12ce47aca025a6db0cbb8"
receipt = json.loads(Path(artifacts["publication_receipt"]["path"]).read_bytes())
assert len(receipt["copied_files"]) == 33
assert len(receipt["guide_hash_chain"]) == 6
assert receipt["services_or_orders_activated"] is False
now = datetime.now(timezone.utc).isoformat()
state = {
    "schema": "forex_continuation_state_004",
    "observed_utc": now,
    "supersedes_status_only": str(ROOT / "CONTINUATION_STATE_003.json"),
    "artifacts": artifacts,
    "canonical_overview": overview,
    "richer_study": {
        "preparation_complete": True, "primary_pairs": 68,
        "primary_M1_rows": 53512475, "original_hourly_origins": 875146,
        "completed_H60_cells": 3, "completed_H240_cells": 3,
        "richer_predictions_replayed_exactly": 841178,
        "reports_independently_recomputed": 12,
        "paired_differences_independently_recomputed": 6,
        "negative_richer_cells_after_declared_costs": 5,
        "positive_richer_cells_after_declared_costs": 1,
        "positive_cell": "H60_2026_ytd; July31 contributes more than the entire cell net gain; descriptive development evidence only",
        "selected_profitable_setup": None,
        "previous_failures_and_frozen_sources_preserved": True,
    },
    "typed_outcomes": {
        "installed_inactive": True, "canonical_and_relocated_cases_passed": 210,
        "same_related_cases_not_independent_trading_trials": True,
        "source_package_published_to_local_vault": True,
        "old_callers_repointed": False,
    },
    "publication": {
        "checkpoint": "CHECKPOINT_006", "copied_members": 33,
        "guides_updated": 6, "original_guide_bytes_preserved": True,
        "local_publication_only_cloud_sync_not_proved": True,
    },
    "project_operational": False, "overall_revamp_complete": False,
    "services_started": False, "trading_activated": False,
    "old_trial_deadline_changed": False, "scheduled_monitoring_created": False,
    "next_work": [
        "Resolve recorded clock-start rejection and establish fresh project clock evidence.",
        "Create a current passive operating/recovery profile without resetting the completed Friday trial.",
        "Verify fresh quotes/archive, successful news collection and acknowledged forecast publication together.",
        "Restore exact selected model/input/runtime dependencies and validate producer-to-outcome integration.",
        "Prepare a separate bounded practice experiment with tested position management; no profitable model has qualified.",
        "Reuse existing weekend baseline and history contract for a separately measured session-aware study; preserve original-known news chronology.",
    ],
    "clock_start_blocker": "Retained continuation record reports earlier automatic approval review rejection as blocked by policy; not retried or bypassed.",
}
with DEST.open("x", encoding="utf-8", newline="\n") as handle:
    json.dump(state, handle, indent=2, ensure_ascii=False)
    handle.write("\n")
with (ROOT / "WORK_LOG.md").open("a", encoding="utf-8", newline="\n") as handle:
    handle.write(f"\n\n### {now} — richer comparison complete, inactive recorder packaged, current checkpoint published\n\n")
    handle.write("All six richer H60/H240 fits are complete. The final global 68-slot identity contract retains the actual 67-pair H240 population and original eligible rows. Three completed H60 cells were copied with recursive provenance; three missing H240 cells were fitted. Saved-model replay reproduces 841,178 richer estimates exactly, as well as six reused baseline arms. Independent endpoint/prediction recomputation reconciles all 12 reports and six paired differences. Execution provenance verifies all 144 final members and the retained copy/failure chain. Earlier receipt-size, memory and population failures remain preserved.\n\n")
    handle.write("Five of six richer cells are negative after declared executable costs. Direction is approximately 50.2–51.2%. The sole positive H60 2026 cell averages +1.1129 bps per selected observation (+0.8330 bps with the fixed entry delay), but July31 contributes more than the whole-period net sum and all other dates combined lose. No profitable setup is selected; these remain dependent development-period endpoint proxies, not account returns. Existing calendar-aware carry and weekend helper designs were found and linked to avoid duplicate work.\n\n")
    handle.write("The inactive typed outcome revision is installed and its source package is published to the local vault. The same 210 synthetic cases pass installed and relocated; historical callers remain separate. CHECKPOINT_006 publishes 30 evidence files plus overview/summary/README, with six current-guide notices preserving every original guide byte as a suffix. Canonical overview and final plan are pinned in CONTINUATION_STATE_004.json. Local publication does not establish OneDrive cloud synchronization.\n\n")
    handle.write("Final process evidence at01:23:14–15UTC found no Python/project PowerShell process and no dashboard listener. No service, order, new trial, clock-start retry or monitoring schedule was activated. The clock-start rejection, expired recovery profile, stale collection/publication, exact runtime/model recovery and new practice/position-management integration remain operational blockers. Overall revamp and market-open readiness remain incomplete.\n")
assert json.loads(DEST.read_bytes()) == state
print(json.dumps({"state": digest(DEST), "work_log": digest(ROOT / "WORK_LOG.md"), "services_started": False}))
