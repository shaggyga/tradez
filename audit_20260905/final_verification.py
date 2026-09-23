"""Final read-only project identity and audit-link verification."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import sys

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / "trad"
VAULT = Path(r"C:\Users\zmoor\OneDrive\thevault\projects\forex")
sys.path.insert(0, str(ROOT))
from tools import vault_worktree_snapshot as snapshot

pointer = json.loads((VAULT/"source/WORKTREE_SOURCE_LATEST.json").read_bytes())
manifest = json.loads((VAULT/"source"/pointer["manifest"]).read_bytes())
current, candidates, _ = snapshot.source_state(ROOT)
source_errors = []
for row in manifest["files"]:
    p = ROOT / row["path"]
    if not p.is_file() or p.stat().st_size != row["size"] or hashlib.sha256(p.read_bytes()).hexdigest() != row["sha256"]:
        source_errors.append(row["path"])
current_inventory = {p for p in candidates if not snapshot.excluded_reason(p)}
recorded_inventory = {row["path"] for row in manifest["files"]}
record_errors = []
shared = json.loads((VAULT/"SHARED_PROJECT_STATE_CURRENT.json").read_bytes())
for row in shared["records"]:
    for p in (VAULT/row["name"], ROOT.parent/row["source"]):
        if not p.is_file() or p.stat().st_size != row["size"] or hashlib.sha256(p.read_bytes()).hexdigest() != row["sha256"]:
            record_errors.append(row["name"])
report = OUT / "FOREX_INDEPENDENT_AUDIT_20260905.md"
links = re.findall(r"\]\(([^)\n]+)\)", report.read_text(encoding="utf-8"))
bad_links = []
for link in links:
    value = re.sub(r":\d+$", "", link)
    p = Path(value)
    if not p.is_absolute():
        p = OUT/p
    if not p.exists():
        bad_links.append(link)
result = {
    "checked_utc":dt.datetime.now(dt.timezone.utc).isoformat(),
    "source_members":len(manifest["files"]),"source_mismatches":source_errors,
    "source_inventory_equal":current_inventory == recorded_inventory,
    "git_identity_equal":all(current[k] == manifest[k] for k in current),
    "shared_records":len(shared["records"]),"record_mismatches":record_errors,
    "audit_report_links_checked":len(links),"invalid_links":bad_links,
}
assert not source_errors and not record_errors and not bad_links
assert result["source_inventory_equal"] and result["git_identity_equal"]
(OUT/"final_verification.json").write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
print(json.dumps(result,indent=2))
