from pathlib import Path
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / "trad"
private = BASE / "private_local_export"
pointer = json.loads((private / "WORKTREE_SOURCE_LATEST.json").read_bytes())
manifest = json.loads((private / pointer["manifest"]).read_bytes())
changed = []
for row in manifest["files"]:
    path = ROOT / row["path"]
    if not path.is_file():
        changed.append({"path": row["path"], "state": "missing"})
    else:
        h = hashlib.sha256(path.read_bytes()).hexdigest()
        if h != row["sha256"]:
            changed.append({"path": row["path"], "state": "different_from_pre_cleanup_snapshot"})
status = subprocess.check_output(["git", "--no-optional-locks", "-C", str(ROOT), "status", "--porcelain=v1", "-z", "--untracked-files=all"], env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"})
result = {"observed_utc": datetime.now(timezone.utc).isoformat(), "source_files_checked": len(manifest["files"]), "all_source_bytes_unchanged": not changed, "changed": changed, "git_porcelain_sha256": hashlib.sha256(status).hexdigest(), "git_status_unchanged": hashlib.sha256(status).hexdigest() == manifest["porcelain_status_sha256"], "scope": "Full4950eligible pre-cleanup source/config/docs/test bytes, including private source files retained only on C; no contents exported. Cache removals excluded by original exporter. Runtime data not asserted immutable."}
(BASE / "CURRENT_SOURCE_PRESERVATION.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
print(json.dumps(result))
