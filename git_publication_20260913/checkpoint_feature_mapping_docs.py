"""Audit and checkpoint four reviewed documentation files; preserve the earlier tag."""
import hashlib
import json
import runpy
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).with_name("FEATURE_MAPPING_DOC_CHECKPOINT.json")
EXPECTED_PARENT = "695f20cb93bbf31d2b53a19134f27cdd52838686"
PATHS = ["FOREX_PENDING_IMPROVEMENTS.md",
         "docs/FOREX_SIGNAL_RESEARCH_ROADMAP_20260913.md",
         "docs/FOREX_FEATURE_MOVE_MAPPING_REVIEW_20260913.md",
         "docs/FOREX_OFFICIAL_SOURCE_COVERAGE_REVIEW_20260913.md"]

def git(*args, input=None):
    return subprocess.run(["git", "-C", str(ROOT), *args], input=input,
                          capture_output=True, check=True).stdout

recovery = "--recover-receipt" in sys.argv[1:]
current_head = git("rev-parse", "HEAD").decode().strip()
if recovery:
    assert git("rev-parse", "HEAD^").decode().strip() == EXPECTED_PARENT
    assert git("log", "-1", "--format=%s").decode().strip() == "Record feature-change mapping objective and source audit gaps"
else:
    assert current_head == EXPECTED_PARENT
assert not git("diff", "--cached", "--name-only")
rules = runpy.run_path(str(ROOT / "tools" / "credential_audit.py"), run_name="bounded_doc_audit_rules")
files, findings, originals = [], [], {}
for relative in PATHS:
    path = ROOT / relative
    assert path.is_file() and not path.is_symlink() and path.stat().st_size < 2_000_000
    data = path.read_bytes()
    assert not rules["path_is_banned"](relative)
    originals[relative] = data
    files.append({"path": relative, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    for name, pattern in rules["PATTERNS"].items():
        for match in pattern.finditer(data):
            findings.append({"path": relative, "rule": name,
                             "line": data.count(b"\n", 0, match.start()) + 1})
report = {"schema_version": "feature_mapping_doc_checkpoint_v1",
          "generated_utc": datetime.now(timezone.utc).isoformat(),
          "parent": EXPECTED_PARENT, "files": files,
          "credential_pattern_findings": findings,
          "scope": "four documentation files; no market data, runtime state or remote action",
          "checks": {"independent_source_review": True,
                     "synthetic_legacy_helper_check": True}}
if findings:
    OUT.write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
    raise SystemExit("Documentation credential-pattern findings require review")
if not recovery:
    git("add", "--", *PATHS)
    assert set(git("diff", "--cached", "--name-only", "-z").decode().split("\0")) - {""} == set(PATHS)
else:
    assert set(git("diff", "HEAD^", "HEAD", "--name-only", "-z").decode().split("\0")) - {""} == set(PATHS)
for relative, data in originals.items():
    assert git("show", ":" + relative) == data
    assert (ROOT / relative).read_bytes() == data
if not recovery:
    git("diff", "--cached", "--check")
    git("commit", "-m", "Record feature-change mapping objective and source audit gaps")
else:
    git("diff", "HEAD^", "HEAD", "--check")
    for relative, data in originals.items():
        assert git("show", "HEAD:" + relative) == data
    report["receipt_recovery"] = "Commit succeeded before original post-commit receipt check compared an annotated tag object to the commit. Verified peeled tag, exact parent, changed paths and bytes; no new commit or tag mutation."
report["commit"] = git("rev-parse", "HEAD").decode().strip()
report["preserved_checkpoint_tag"] = git("rev-parse", "checkpoint-2026-09-13-signals^{commit}").decode().strip()
assert report["preserved_checkpoint_tag"] == EXPECTED_PARENT
report["working_tree_clean"] = not bool(git("status", "--porcelain"))
report["status"] = "documentation_committed_locally_no_push"
OUT.write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
print(json.dumps(report, indent=2))
