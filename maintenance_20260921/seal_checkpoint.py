"""Seal the completed portable folder using relative identities."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import shutil
import sys

BASE = Path(__file__).resolve().parent
OUT = BASE / "checkpoint"


def read(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


assert not (OUT / "MANIFEST.json").exists(), "Never silently reseal an existing checkpoint"
pointer = read(OUT / "source/WORKTREE_SOURCE_LATEST.json")
assert pointer.get("privacy_exclusions") == "PRIVATE_DEPENDENCIES.json"
source = read(OUT / "source" / pointer["manifest"])
assert pointer["verification"]["status"] == "passed"
assert (OUT / "verification/LOOSE_RECORD_PRIVACY_REVIEW.json").exists()
privacy = read(OUT / "verification/LOOSE_RECORD_PRIVACY_REVIEW.json")
assert privacy["passed"] and not privacy["findings"]
shutil.copyfile(BASE / "restore_checkpoint.py", OUT / "restore_checkpoint.py")
shutil.copyfile(BASE / "test_restore_checkpoint.py", OUT / "test_restore_checkpoint.py")
shutil.copyfile(BASE / "SOURCE_PRIVACY_REVIEW.json", OUT / "verification/SOURCE_PRIVACY_REVIEW.json")
shutil.copyfile(BASE / "SHARED_SOURCE_EXPORT_RECEIPT.json", OUT / "verification/SHARED_SOURCE_EXPORT_RECEIPT.json")
if (BASE / "RESTORE_TOOL_TESTS.json").exists():
    shutil.copyfile(BASE / "RESTORE_TOOL_TESTS.json", OUT / "verification/RESTORE_TOOL_TESTS.json")
# Refresh the loose-text privacy receipt after all final helper/receipt copies.
sys.dont_write_bytecode = True
sys.path.insert(0, str(BASE.parent / "trad"))
from tools.vault_worktree_snapshot import audit_payload, credentials
scanned = []
review_path = OUT / "verification/LOOSE_RECORD_PRIVACY_REVIEW.json"
for path in sorted(OUT.rglob("*")):
    if not path.is_file() or path == review_path or path.suffix.lower() not in {".json", ".md", ".txt", ".py", ".csv", ".xml", ".log", ".ini"}:
        continue
    rel = path.relative_to(OUT).as_posix()
    raw = path.read_bytes()
    audit_payload(rel, raw, set())
    assert not credentials.ACCOUNT_ID.search(raw), f"Account-identifier pattern in {rel}"
    scanned.append({"path": rel, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
review_path.write_text(json.dumps({"generated_utc": datetime.now(timezone.utc).isoformat(), "passed": True, "findings": [], "files_scanned": len(scanned), "bytes_scanned": sum(r["bytes"] for r in scanned), "scope": "Final loose text after helper/receipt copies; excludes this report, not-yet-created top manifest, ZIP payloads and binary/PDF data. No private credential files read. Source ZIP filtered and separately verified.", "files": scanned}, indent=2) + "\n", encoding="utf-8")
archives = [{"path": "source/" + source["archive"], "target_prefix": "trad", "members": [{"path": r["path"], "bytes": r["size"], "sha256": r["sha256"]} for r in source["files"]]}]
for item in read(OUT / "inputs/INPUT_ARCHIVES.json")["archives"]:
    archives.append({"path": item["path"], "target_prefix": item["target_prefix"], "members": [{k: r[k] for k in ("path", "bytes", "sha256")} for r in item["members"]]})
supp = read(OUT / "supplements/SUPPLEMENT_ARCHIVE.json")
archives.append({"path": supp["path"], "target_prefix": supp["target_prefix"], "members": [{k: r[k] for k in ("path", "bytes", "sha256")} for r in supp["members"]]})
files = []
for path in sorted(OUT.rglob("*")):
    if path.is_file():
        assert not path.is_symlink() and not path.is_junction()
        files.append({"path": path.relative_to(OUT).as_posix(), "bytes": path.stat().st_size, "sha256": sha(path)})
manifest = {"schema": "forex_portable_checkpoint_v1", "sealed_utc": datetime.now(timezone.utc).isoformat(), "scope": "Inactive offline research recovery with explicit private-source/evidence and runtime exclusions", "base_git_commit": source["base_git_commit"], "source_snapshot_id": source["snapshot_id"], "source_members": len(source["files"]), "raw_data_files": 136, "data_vintages_merged": False, "cloud_sync_verified": False, "files": files, "archives": archives}
(OUT / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"status": "sealed", "files": len(files), "source_members": len(source["files"]), "archive_members": sum(len(a["members"]) for a in archives), "payload_bytes": sum(r["bytes"] for r in files), "manifest_sha256": sha(OUT / "MANIFEST.json")}))
