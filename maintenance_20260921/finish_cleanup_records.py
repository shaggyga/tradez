from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import os
import shutil

BASE = Path(__file__).resolve().parent
PROJECT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex")
MAINT = PROJECT / "maintenance/vault_refresh_20260921"
readback = json.loads((BASE / "CLEANUP_INDEPENDENT_READBACK.json").read_bytes())
source = json.loads((BASE / "CURRENT_SOURCE_PRESERVATION.json").read_bytes())
assert readback["status"] == "passed" and not readback["failures"]
assert source["all_source_bytes_unchanged"] and source["git_status_unchanged"]
retained = []
for path in (BASE / "restore_validation", Path(r"C:\ForexRestore_20260921")):
    count = total = 0
    for directory, dirs, files in os.walk(path, followlinks=False):
        dirs[:] = [d for d in dirs if not (Path(directory)/d).is_symlink() and not (Path(directory)/d).is_junction()]
        for name in files:
            p = Path(directory) / name
            if p.is_file() and not p.is_symlink():
                count += 1
                total += p.stat().st_size
    retained.append({"path": str(path), "files": count, "logical_bytes": total, "retained": True})
result = {"schema": "forex_c_cleanup_and_recovery_summary_v1", "completed_utc": datetime.now(timezone.utc).isoformat(), "status": "reviewed_cleanup_complete_with_policy_blocked_restore_scratch_retained", "cold_logs_archived_and_removed": 5027, "generated_cache_files_archived_and_removed": 164, "independent_archive_items_verified": 5191, "independent_verification_failures": 0, "original_bytes": 329452350, "archive_payload_bytes": 14426082, "archive_per_file_receipt_bytes": 6517046, "logical_savings_after_per_file_receipts": 308509222, "physical_space_gain_measured": False, "all4950_source_files_unchanged": True, "git_status_unchanged": True, "live_workers_or_accounts_changed": False, "D_access": False, "cold_archive_root": readback["archive_root"], "cold_archive_cloud_copy": False, "cleanup_readback_receipt": "CLEANUP_INDEPENDENT_READBACK.json", "portable_checkpoint_payload_bytes": 1888197100, "retained_restore_outputs": retained, "restore_scratch_removal": {"executed": False, "blocked_by": "automatic approval review", "reason_returned": "blocked by policy; no more specific reason supplied", "retry_performed": False}, "free_C_bytes_at_completion": shutil.disk_usage("C:\\").free, "space_note": "New checkpoint, current source checkpoint copy, local private source backup and retained restore outputs consume more than cleanup saved; no net C-drive gain is claimed."}
raw = (json.dumps(result, indent=2) + "\n").encode()
(BASE / "CLEANUP_RESULTS.json").write_bytes(raw)
(MAINT / "CLEANUP_RESULTS.json").write_bytes(raw)
for name in ("CLEANUP_INDEPENDENT_READBACK.json", "CLEANUP_SYNTHETIC_VALIDATION.json", "CURRENT_SOURCE_PRESERVATION.json", "RESTORE_SCRATCH_CLEANUP.json", "cleanup_archive_engine.py", "run_reviewed_cleanup.py"):
    shutil.copyfile(BASE / name, MAINT / name)
with (MAINT / "README.md").open("a", encoding="utf-8") as f:
    f.write("\n## Completed cleanup and retained copies\n\n[Cleanup results](CLEANUP_RESULTS.json) and [independent archive readback](CLEANUP_INDEPENDENT_READBACK.json) verify all5,191 removed originals against their lossless archives:5,027 old logs and164 generated caches. Logical savings after per-file receipts are308,509,222bytes; this is not a measured physical free-space increase. Raw logs stay in the local C cold archive and are not copied into OneDrive.\n\n[Source preservation](CURRENT_SOURCE_PRESERVATION.json) confirms every one of the4,950 original eligible source files and Git status are unchanged. The new portable checkpoint has4,881 shared-source members plus9 explicit supplements and136 data files.69 privacy-classified source paths and2 private evidence receipts are withheld and listed by identity.\n\nAutomatic approval review blocked removal of the task-created restore folders, reporting only ‘blocked by policy’. No retry was made. The verified inactive copy remains at C:\\ForexRestore_20260921; the failed first-attempt copy remains under C:\\Users\\zmoor\\Documents\\forex\\maintenance_20260921\\restore_validation. [Retained-folder record](RESTORE_SCRATCH_CLEANUP.json) and cleanup results give the exact scope. The checkpoint plus these retained copies consume more disk space than the cleanup saved.\n\nTo recover a removed diagnostic, use its per-file receipt in the local cold archive and the retained cleanup_archive_engine.py restore_copy helper with a fresh destination. It verifies compressed and original hashes and never overwrites an existing file. The archived bytecode is regenerable; current Python source remains intact. Do not rerun the cleanup wrapper from the Vault: its hash-pinned local plan and completed receipts belong to the original maintenance task.\n")
recreation = PROJECT / "RECREATION.md"
text = recreation.read_text(encoding="utf-8").replace("below216characters", "at most 216 characters")
recreation.write_text(text, encoding="utf-8")
print(json.dumps({"status": result["status"], "net_logical_cleanup_savings": result["logical_savings_after_per_file_receipts"], "retained_restore_bytes": sum(r["logical_bytes"] for r in retained), "free_C_bytes": result["free_C_bytes_at_completion"]}))
