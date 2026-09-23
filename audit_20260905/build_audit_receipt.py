"""Record audit findings and hash-bound source/evidence without changing project files."""
from __future__ import annotations
import datetime as dt
import hashlib
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / "trad"

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def row(identity, priority, title, status, sources, evidence):
    return dict(id=identity, priority=priority, title=title, status=status,
                source_references=[dict(path=p, line=n) for p,n in sources],
                reproduction_evidence=evidence, repaired=False,
                historical_production_impact="not_established")

findings = [
    row("A01", "P1", "Signed revocation or tighter canary ignored at final submission", "new_code_defect_reproduced",
        [("oanda_practice_top_signal_executor.py",710)], ["execution/execution_gate_reproduction.json"]),
    row("A02", "P1", "Unavailable currency conversion falls back to 1.0 for exposure caps", "new_code_defect_reproduced",
        [("oanda_practice_shadow_strategy_lab.py",3583),("oanda_practice_top_signal_executor.py",675)], ["execution/execution_gate_reproduction.json"]),
    row("A03", "P1", "Rank entry quote can precede semantic mapping availability", "new_code_defect_reproduced",
        [("oanda_causal_source_factor_response_map_v1.py",848),("oanda_causal_source_factor_response_map_v1.py",2239),("oanda_source_conditioned_currency_rank_v1.py",346)], ["evidence/evidence_clock_reproduction.json"]),
    row("A04", "P1", "Receipt availability timestamp can precede durable commit", "known_code_defect_reconfirmed",
        [("oanda_source_governance_news_fast_lane.py",340),("oanda_source_governance_news_fast_lane.py",376)], ["known_fastlane_faults.json"]),
    row("A05", "P2", "Late collector commit omitted after scan cursor advances", "new_code_defect_reproduced",
        [("oanda_source_governance_news_fast_lane.py",255),("oanda_source_governance.py",561),("oanda_local_news_sentiment.py",19714)], ["late_commit_reproduction.json"]),
    row("A06", "P2", "Building progress overwrites completed integrity snapshot", "known_code_defect_reconfirmed",
        [("oanda_source_governance_news_fast_lane.py",289)], ["known_fastlane_faults.json"]),
    row("A07", "P2", "Error or interrupted state resets committed scan cursor", "known_code_defect_reconfirmed",
        [("oanda_source_governance_news_fast_lane.py",255)], ["known_fastlane_faults.json"]),
    row("A08", "P2", "Quoted alphabetic secrets excluded as code references", "new_code_defect_reproduced",
        [("tools/credential_audit.py",80),("tools/vault_worktree_snapshot.py",134),("tools/vault_worktree_snapshot.py",155)], ["recovery/credential_bypass_reproduction.json"]),
    row("A09", "P3", "Source-only restore instructions include historical-evidence validator", "new_documentation_defect_reproduced",
        [("docs/VAULT_RECREATION_CURRENT.md",45),("oanda_issue_register_validator.py",79)], ["recovery/source_only_register_validation.json"]),
]
source_names = sorted({ref["path"] for item in findings for ref in item["source_references"]} | {
    "README.md", "FOREX_AUDIT_START_HERE.md", "FOREX_AUDIT_STATE_CURRENT.json",
    "FOREX_FAULT_AUDIT_CURRENT.json", "FOREX_ISSUE_REGISTER_CURRENT.json",
    "oanda_causal_source_factor_response_map_v8.py", "oanda_source_conditioned_currency_rank_v7.py",
    "oanda_prospective_governance.py", "oanda_project_integrity_audit.py",
})
evidence_names = sorted({name for item in findings for name in item["reproduction_evidence"]} | {
    "FOREX_INDEPENDENT_AUDIT_20260905.md", "execution/EXECUTION_AUDIT.md",
    "evidence/EVIDENCE_AUDIT.md", "recovery/RECOVERY_AUDIT.md",
    "focused_tests.txt", "issue_register_validation.json", "runtime_observation.json", "final_verification.json",
    "recovery/verification.json", "recovery/environment_verification.json",
    "recovery/legacy_verification.json", "recovery/maintenance_tests.xml",
    "evidence/saved_metrics_and_hashes.json", "late_commit_reproduction.py",
    "execution/reproduce_execution_gates.py", "evidence/evidence_clock_reproduction.py",
    "recovery/reproduce_credential_bypass.py", "run_focused_tests.py",
})
report = {
    "schema_version":"independent_forex_review_v1",
    "generated_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
    "canonical_project":str(ROOT), "vault_guide":r"C:\Users\zmoor\OneDrive\thevault\projects\forex\README.md",
    "supported_decision":"no_trade", "runtime_restarted":False, "broker_requests":0,
    "production_database_writes":0, "source_or_vault_edits":False,
    "scope":"Independent risk-based source, frozen-record, recovery and isolated-fixture audit; not exhaustive module or production-ledger verification",
    "summary":{"new_code_defects":5,"known_code_defects_reconfirmed":3,"new_documentation_defects":1,"unwired_confirmation_api_advisory":True},
    "findings":findings,
    "test_runs":[{"scope":"fastlane_and_register","passed":11,"skipped":0},
                 {"scope":"source_v8_rank_v7_governance","passed":18,"skipped":0},
                 {"scope":"vault_maintenance","passed":90,"skipped":1}],
    "test_count_caveat":"Some tests overlap between runs; these are not unique test counts or an exhaustive suite",
    "source_sha256":{name:sha(ROOT/name) for name in source_names},
    "evidence_sha256":{name:sha(OUT/name) for name in evidence_names},
    "original_issue_register_unchanged":True,
}
(OUT/"AUDIT_FINDINGS.json").write_text(json.dumps(report,indent=2)+"\n",encoding="utf-8")
print(json.dumps({"findings":len(findings),"source_files_bound":len(source_names),"evidence_files_bound":len(evidence_names),"output":str(OUT/"AUDIT_FINDINGS.json")}))
