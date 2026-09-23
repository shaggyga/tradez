"""Validate and hash this audit's artifacts, without reading project payloads."""
import ast
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
status = {
    "schema": "forex_stage_a_audit_status_v1",
    "completed_utc": datetime.now(timezone.utc).isoformat(),
    "status": "initial_bounded_stage_a_audit_complete",
    "entrypoint": "AUDIT_REPORT.md",
    "authorized_scope": "First audit stage only; source/evidence inspection and separate audit reports",
    "stage_b_started": False,
    "project_source_changes": False,
    "runtime_or_scheduled_task_changes": False,
    "project_tests_run": False,
    "fits_backtests_or_model_deserialization": False,
    "paid_data_cloud_or_broker_api_calls": 0,
    "credentials_read": False,
    "background_audit_jobs_left_running": False,
    "d_drive_reads": "Limited metadata/small manifests before fresh disk events were discovered; then stopped",
    "d_drive_payload_integrity": "deferred_fresh_bad_blocks",
    "deeper_audit_items_open": [
        "D-only preservation and payload verification",
        "Complete current modified/untracked source checkpoint and runtime reconstruction",
        "Source-history full hashes, row-level integrity, availability/vintage and native/reacquired reconciliation",
        "Complete six-model-family/variant lineage, damaged neural and moments replication recovery",
        "Macro population, historical expectations and exact BOJ incident clocks",
        "Integrated all-68 >=24h adaptive replay, accounting, rotation and matched advisor readiness mapping",
        "Current live-output freshness and complete worker/account readiness"
    ],
    "fresh_checks": {
        "vault_source_members_matching": 3486,
        "shared_record_hashes_matching": 207,
        "shared_record_hashes_mismatching": 1,
        "mismatched_shared_record": "README.md changed after index seal",
        "cold_receipt_semantic_hash": "match",
        "chat_pages_hashed": 190,
        "unique_saved_turn_ids": 1077,
        "undated_turns": 206,
        "c_m1_files_readable_footers": 68,
        "c_m1_footer_rows": 50321016,
        "c_m1_config_instrument_sets_equal": True,
        "legacy_experiment_json_members": 3124,
        "named_hgb_record_feature_names": 227
    },
    "limits": "Fresh identity/metadata checks do not reproduce scientific results, establish a profitable edge, verify all historical payloads or authorize execution.",
    "continuation": "RESUME_INSTRUCTIONS.md"
}
(ROOT / "RUN_STATUS.json").write_text(json.dumps(status,indent=2),encoding="utf-8")

checks = {"json_readability": [], "audit_helper_syntax": [], "markdown_local_links": []}
for path in sorted(ROOT.glob("*.json")):
    if path.name in {"AUDIT_ARTIFACT_MANIFEST.json", "AUDIT_VALIDATION.json"}:
        continue
    json.loads(path.read_text(encoding="utf-8-sig"))
    checks["json_readability"].append(path.name)
for path in sorted(ROOT.glob("*.py")):
    ast.parse(path.read_text(encoding="utf-8"),filename=str(path))
    checks["audit_helper_syntax"].append(path.name)
missing=[]
for path in sorted(ROOT.glob("*.md")):
    for target in re.findall(r"\]\(([^)]+)\)",path.read_text(encoding="utf-8-sig")):
        if "://" in target or target.startswith("#"):
            continue
        clean=target.strip("<>").split("#")[0]
        linked=path.parent / clean
        if not linked.exists():
            missing.append({"source":path.name,"target":target})
        checks["markdown_local_links"].append({"source":path.name,"target":target,"exists":linked.exists()})
with (ROOT/"SOURCE_HISTORY_COVERAGE.csv").open(encoding="utf-8-sig",newline="") as f:
    rows=list(csv.DictReader(f))
assert len(rows)==len({r["instrument"] for r in rows})==68
assert sum(int(r["rows"]) for r in rows)==50321016
assert not missing, missing
h=json.loads((ROOT/"SOURCE_HISTORY_METADATA.json").read_text())
assert h["current_config_pair_reconciliation"]["matched"]
assert not h["record_mismatches"]
assert all(not r["changed_during_read"] for r in h["records"])
assert all(all(n in r["columns"] for n in ["bid_open","bid_high","bid_low","bid_close","ask_open","ask_high","ask_low","ask_close","spread_pips","volume"]) for r in h["records"])
checks["history_csv_unique_pairs"]=68
checks["history_csv_rows_match_footer_total"]=True
checks["history_bid_ask_ohlc_spread_volume_schema_all68"]=True
checks["status"]="passed_for_audit_artifacts_only"
checks["limitation"]="No project tests or economic validation performed"
(ROOT/"AUDIT_VALIDATION.json").write_text(json.dumps(checks,indent=2),encoding="utf-8")
files=[]
for path in sorted(ROOT.iterdir()):
    if path.is_file() and path.name!="AUDIT_ARTIFACT_MANIFEST.json":
        data=path.read_bytes()
        files.append({"path":path.name,"bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()})
manifest={"sealed_utc":datetime.now(timezone.utc).isoformat(),"scope":"audit artifacts only; excludes this manifest to avoid self-reference","files":files}
(ROOT/"AUDIT_ARTIFACT_MANIFEST.json").write_text(json.dumps(manifest,indent=2),encoding="utf-8")
print(json.dumps({"status":checks["status"],"files_sealed":len(files),"bytes":sum(f["bytes"] for f in files),"missing_links":missing},indent=2))
