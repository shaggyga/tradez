from pathlib import Path
import hashlib
import json
from datetime import datetime, timezone

VAULT = Path(r"C:\Users\zmoor\OneDrive\thevault")
OUT = VAULT / "projects/forex/maintenance/vault_refresh_20260921"
names = ["README.md", "INDEX.md", "index.json", "projects/forex/README.md", "projects/forex/AUDIT_START_HERE.md", "projects/forex/RECREATION.md", "projects/forex/PROJECT_HISTORY.md", "projects/forex/RESEARCH_INDEX.md", "projects/forex/PENDING_IMPROVEMENTS_CURRENT.md", "projects/forex/PROJECT_LOG_CURRENT.md", "projects/forex/KNOWLEDGE_INDEX.md", "projects/forex/SHARED_PROJECT_STATE_CURRENT.json", "projects/forex/VAULT_FILE_INVENTORY.json", "projects/forex/VAULT_READABILITY_REPORT.json", "projects/forex/maintenance/README.md", "projects/forex/source/WORKTREE_SOURCE_LATEST.json"]
if (OUT / "PREIMAGES.json").exists():
    raise RuntimeError("Preimage record already exists; never overwrite")
rows = []
for name in names:
    source = VAULT / name
    if not source.exists():
        rows.append({"path": name, "existed": False})
        continue
    raw = source.read_bytes()
    target = OUT / "before" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("xb") as f:
        f.write(raw)
    assert target.read_bytes() == raw
    rows.append({"path": name, "existed": True, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "preserved_path": target.relative_to(VAULT).as_posix()})
(OUT / "PREIMAGES.json").write_text(json.dumps({"created_utc": datetime.now(timezone.utc).isoformat(), "files": rows, "scope": "Original mutable navigation and indexes before September21 refresh; dated historical payloads untouched"}, indent=2) + "\n", encoding="utf-8")
print(json.dumps({"preimages_preserved": sum(r["existed"] for r in rows)}))
