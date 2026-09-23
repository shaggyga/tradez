"""Read-only provenance checks; writes results only beside this audit helper.

Does not import project code, extract archives, deserialize models, or open DBs.
"""
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import zipfile

OUT = Path(__file__).resolve().parent
VAULT = Path(r"C:\Users\zmoor\OneDrive\thevault")
PROJECT = VAULT / "projects" / "forex"

def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))

def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def verify_file(path, expected_sha, expected_size=None):
    row = {"path": str(path), "expected_sha256": expected_sha,
           "expected_bytes": expected_size}
    try:
        before = path.stat()
        row.update(actual_bytes=before.st_size, actual_sha256=sha(path))
        after = path.stat()
        row["changed_during_read"] = (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
        row["status"] = "match" if (row["actual_sha256"] == expected_sha and
            (expected_size is None or before.st_size == expected_size) and
            not row["changed_during_read"]) else "mismatch"
    except Exception as exc:
        row.update(status="unavailable", error=f"{type(exc).__name__}: {exc}")
    return row

result = {"observed_utc": datetime.now(timezone.utc).isoformat(),
          "scope": "Current local Vault record hashes, source archive/member hashes, old checkpoint and consolidation receipt; no source execution or cloud verification."}
manifest_path = PROJECT / "SHARED_PROJECT_STATE_CURRENT.json"
manifest = read_json(manifest_path)
records = []
for row in manifest["records"]:
    target = (PROJECT / row["name"]).resolve()
    if not target.is_relative_to(PROJECT.resolve()):
        records.append({"name": row["name"], "status": "unsafe_path_not_read"})
        continue
    check = verify_file(target, row["sha256"], row.get("size"))
    check.update(name=row["name"], declared_source=row.get("source"), provenance=row.get("provenance"))
    records.append(check)
result["shared_record_manifest"] = {"path": str(manifest_path), "sha256": sha(manifest_path),
    "generated_utc": manifest.get("generated_utc"), "declared_count": manifest.get("record_count"),
    "summary": dict(Counter(r["status"] for r in records)), "records": records}

pointer_path = PROJECT / "source" / "WORKTREE_SOURCE_LATEST.json"
pointer = read_json(pointer_path)
source_dir = pointer_path.parent
archive = source_dir / pointer["archive"]
source_manifest_path = source_dir / pointer["manifest"]
source_manifest = read_json(source_manifest_path)
source_result = {"pointer_path": str(pointer_path), "pointer_sha256": sha(pointer_path),
    "snapshot_id": pointer["snapshot_id"], "published_utc": pointer.get("published_utc"),
    "base_git_commit": pointer.get("base_git_commit"),
    "archive": verify_file(archive, pointer["archive_sha256"]),
    "manifest": verify_file(source_manifest_path, pointer["manifest_sha256"]),
    "manifest_exclusions": source_manifest.get("excluded"),
    "ignored_scope": source_manifest.get("ignored_scope")}
member_checks = []
with zipfile.ZipFile(archive) as z:
    names = z.namelist()
    source_result["duplicate_members"] = [k for k,v in Counter(names).items() if v > 1]
    expected_names = set()
    for row in source_manifest["files"]:
        name = row["path"].replace("\\", "/")
        expected_names.add(name)
        check = {"path": name}
        try:
            info = z.getinfo(name)
            if info.file_size > 64 * 1024 * 1024:
                check.update(status="deferred_large_member", bytes=info.file_size)
            else:
                h = hashlib.sha256()
                with z.open(info) as f:
                    for block in iter(lambda: f.read(1024 * 1024), b""):
                        h.update(block)
                check.update(bytes=info.file_size, sha256=h.hexdigest(),
                    status="match" if info.file_size == row["size"] and h.hexdigest() == row["sha256"] else "mismatch")
        except Exception as exc:
            check.update(status="unavailable", error=f"{type(exc).__name__}: {exc}")
        member_checks.append(check)
    source_result["unlisted_members"] = sorted(set(names) - expected_names)
source_result["member_summary"] = dict(Counter(r["status"] for r in member_checks))
source_result["members"] = member_checks
result["source_snapshot"] = source_result

old_pointer = read_json(VAULT / "CHECKPOINT_LATEST.json")
old_base = old_pointer["base_checkpoint"]
result["old_root_checkpoint"] = verify_file(VAULT / old_base["archive"], old_base["archive_sha256"], old_base["archive_bytes"])
receipt = Path(r"C:\Users\zmoor\Documents\vault_cold_archive\20260901_record_consolidation\VAULT_RECORD_CONSOLIDATION_RECEIPT.json")
receipt_data = read_json(receipt)
receipt_declared_digest = receipt_data.pop("receipt_sha256")
receipt_digest = hashlib.sha256(json.dumps(receipt_data, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
result["cold_receipt"] = {"path": str(receipt), "raw_file_sha256": sha(receipt),
    "hash_contract": "canonical JSON sort_keys=True separators=(',',':') before adding receipt_sha256",
    "expected_semantic_sha256": "f1d164639412f0176920d7fc6459e5d21b73620a5f5a665b9d9dfdc49c4d18d7",
    "declared_semantic_sha256": receipt_declared_digest, "actual_semantic_sha256": receipt_digest,
    "status": "match" if receipt_digest == receipt_declared_digest == "f1d164639412f0176920d7fc6459e5d21b73620a5f5a665b9d9dfdc49c4d18d7" else "mismatch"}
result["cold_tree_verification"] = "not rerun; roots observed, receipt bytes verified only"
output = OUT / "VAULT_PROVENANCE_VERIFICATION.json"
output.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps({"output": str(output), "shared_record_summary": result["shared_record_manifest"]["summary"],
    "shared_mismatches": [r["name"] for r in records if r["status"] != "match"],
    "source_archive": source_result["archive"]["status"], "source_manifest": source_result["manifest"]["status"],
    "source_member_summary": source_result["member_summary"], "extra_members": source_result["unlisted_members"],
    "old_root_checkpoint": result["old_root_checkpoint"]["status"], "cold_receipt": result["cold_receipt"]["status"]}, indent=2))
