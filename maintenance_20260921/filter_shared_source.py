"""Derive a shareable worktree snapshot, preserving private-export originals locally."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import shutil
import stat
import sys
import zipfile

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / "trad"
DEST = BASE / "checkpoint/source"
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from tools.vault_worktree_snapshot import encoded, digest, verify_snapshot

privacy = json.loads((BASE / "SOURCE_PRIVACY_REVIEW.json").read_text(encoding="utf-8-sig"))
pointer_path = DEST / "WORKTREE_SOURCE_LATEST.json"
original_pointer = json.loads(pointer_path.read_bytes())
original_manifest_path = DEST / original_pointer["manifest"]
original = json.loads(original_manifest_path.read_bytes())
private_names = {"oanda_arima_canary_executor.py", "oanda_gpt_training_strategy_manager.py", "oanda_model_space_agenda.py", "oanda_trainer_reporting_extensions.py"}
for item in privacy["findings"]:
    if item.get("account_id_count", 0) or item.get("raw_oanda_token_count", 0):
        private_names.add(item["path"])
rows = [r for r in original["files"] if r["path"] not in private_names]
removed = [r for r in original["files"] if r["path"] in private_names]
assert len(rows) + len(removed) == original["file_count"] and len(removed) >= 4
excluded = list(original["excluded"]) + [{"path": r["path"], "reason": "private_legacy_source_or_account_identifier_review"} for r in removed]
state = {key: original[key] for key in ("kind", "base_git_commit", "worktree_dirty", "is_clean_commit_archive", "index_inventory_sha256", "porcelain_status_sha256", "porcelain_status_entries")}
snapshot_id = digest(encoded({"state": state, "files": rows, "excluded": excluded}))
stem = "forex_worktree_source_" + snapshot_id[:24]
archive_path = DEST / (stem + ".zip")
manifest_path = DEST / (stem + ".manifest.json")
with zipfile.ZipFile(DEST / original["archive"]) as source, zipfile.ZipFile(archive_path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as out:
    for row in rows:
        raw = source.read(row["path"])
        assert len(raw) == row["size"] and digest(raw) == row["sha256"]
        info = zipfile.ZipInfo(row["path"], date_time=(1980, 1, 1, 0, 0, 0))
        info.create_system = 3
        info.external_attr = (stat.S_IFREG | 0o644) << 16
        info.compress_type = zipfile.ZIP_DEFLATED
        out.writestr(info, raw)
manifest = {**original, "created_utc": datetime.now(timezone.utc).isoformat(), "snapshot_id": snapshot_id, "archive": archive_path.name, "archive_sha256": digest(archive_path.read_bytes()), "content_sha256": digest(encoded(rows)), "file_count": len(rows), "files": rows, "excluded": excluded,
    "scope": "Current eligible tracked/untracked worktree source with explicit privacy exclusions; not every legacy worker dependency or Git history",
    "derived_from_snapshot_id": original["snapshot_id"], "privacy_review": {"private_source_exclusions": 4, "total_withheld_paths": len(removed), "account_identifier_flagged_files_withheld": len(privacy["findings"]), "source_bytes_redacted": False, "private_credential_files_read": False}}
manifest["credential_audit"] = {**original["credential_audit"], "files_scanned": len(rows), "additional_review": "All flagged account-identifier/raw-token members withheld, including fixtures; four private legacy dependencies excluded"}
manifest_path.write_bytes(encoded(manifest))
verification = verify_snapshot(manifest_path)
pointer = {**original_pointer, "snapshot_id": snapshot_id, "published_utc": datetime.now(timezone.utc).isoformat(), "archive": archive_path.name, "archive_sha256": manifest["archive_sha256"], "manifest": manifest_path.name, "manifest_sha256": digest(manifest_path.read_bytes()), "content_sha256": manifest["content_sha256"], "verification": verification, "privacy_exclusions": "PRIVATE_DEPENDENCIES.json", "reused": False}
local_private = BASE / "private_local_export"
local_private.mkdir(exist_ok=False)
for path in (DEST / original["archive"], original_manifest_path, pointer_path):
    path.rename(local_private / path.name)
pointer_path.write_bytes(encoded(pointer))
(DEST / "PRIVATE_DEPENDENCIES.json").write_bytes(encoded({"schema": "forex_explicit_private_source_dependencies_v1", "reason": "Existing private module classifications and account/token pattern findings; exact unredacted payloads remain only in original C worktree/local private source export", "source_root_provenance": str(ROOT), "private_local_export_provenance": str(local_private), "withheld_files": removed, "not_standalone_legacy_runtime": True, "restoration_rule": "For a legacy component requiring these, obtain an authorized secure local copy and verify its hash; never use fake stubs or silently substitute source."}))
(BASE / "SHARED_SOURCE_EXPORT_RECEIPT.json").write_bytes(encoded(pointer))
print(json.dumps({"status": "passed", "files": len(rows), "withheld": len(removed), "archive_bytes": archive_path.stat().st_size, "verification": verification}))
