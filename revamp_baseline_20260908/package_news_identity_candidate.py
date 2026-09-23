"""Retain the accepted offline candidate and update the existing work queue."""
import ast
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sys

BASE = Path(__file__).resolve().parent
WORK = BASE / "news_repair_review"
PROJECT = BASE.parent / "trad"
EVIDENCE = PROJECT / "docs/validation/news_identity_candidate_20260908"
RECOVERY = Path("D:/ForexRecovery/revamp_20260908T1353Z/news_identity_candidate")
REPORT = PROJECT / "docs/FOREX_NEWS_IDENTITY_CANDIDATE_20260908.md"
VALIDATION = PROJECT / "FOREX_NEWS_IDENTITY_CANDIDATE_VALIDATION_20260908.json"
sys.dont_write_bytecode = True


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as out:
        json.dump(value, out, indent=2, sort_keys=True, allow_nan=False)
        out.write("\n")


def replace_once(path, old, new):
    value = path.read_text(encoding="utf-8-sig")
    assert value.count(old) == 1, (path.name, old)
    path.write_text(value.replace(old, new), encoding="utf-8")


test = json.loads((WORK / "CANDIDATE_TEST_RECEIPT_20260908.json").read_text())
assert test["status"] == "passed" and test["counts"] == {"tests": 89, "errors": 0, "failures": 0, "skipped": 0}
assert test["frozen_sources_unchanged"]
for name, expected in test["artifacts"].items():
    path = WORK / name
    if not path.exists():
        path = BASE / "workspace/trad" / name
    assert digest(path) == expected, name
for name, expected in test["frozen_source_hashes"].items():
    assert digest(Path(name)) == digest(PROJECT / Path(name).name) == expected

registry_path = PROJECT / "config/joint_price_news_study_v2_20260907.json"
registry = json.loads(registry_path.read_text())
assert len(registry["source_bindings"]) == 16 and len(registry["pairs"]) == 68
for name, expected in registry["source_bindings"].items():
    assert digest(PROJECT / name) == expected, name
for name in ("can_place_orders", "can_promote", "can_authorize", "account_eligible", "proof_eligible"):
    assert registry[name] is False
assert digest(PROJECT / "FOREX_REVAMP_BASELINE_RECOVERY_VALIDATION_20260908.json") == "697140a991a30f11acd8d2e74c1fcc3309b8d0257762326186281651484b5c16"

# Recover the exact previous test inputs by reversing only the subsequent narrow
# addition. Hash equality with its retained receipt is required before saving.
prior = json.loads((WORK / "CANDIDATE_TEST_RECEIPT_20260908T143254354740Z.json").read_text())
candidate_raw = (WORK / "news_topic_identity_reconcile_candidate_v1.py").read_bytes()
prior_candidate = candidate_raw.replace(b"        topic_member_ids = set()\n", b"").replace(
    b"            if event_id in topic_member_ids:\n"
    b"                # The original guard deliberately withholds this malformed\n"
    b"                # within-topic identity. Do not turn it into new support by\n"
    b"                # treating it as an ordinary duplicate across valid variants.\n"
    b"                raise ValueError(\"reconcile_duplicate_member_within_input_topic\")\n"
    b"            topic_member_ids.add(event_id)\n", b"")
prior_tests = (WORK / "test_news_topic_identity_reconcile_candidate_v1.py").read_bytes().split(
    b"\n\ndef test_internal_duplicate_member_rejection_cannot_be_erased_by_union():", 1)[0]
for name, raw in [("news_topic_identity_reconcile_candidate_v1.py", prior_candidate),
                  ("test_news_topic_identity_reconcile_candidate_v1.py", prior_tests)]:
    assert sha256(raw).hexdigest() == prior["artifacts"][name], ("prior source recovery", name)
    target = WORK / "prior_test_run" / (name + ".txt")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        assert target.read_bytes() == raw
    else:
        with target.open("xb") as out:
            out.write(raw)

sys.path.insert(0, str(PROJECT))
from tools.vault_worktree_snapshot import audit_payload

selected = [p for p in sorted(WORK.rglob("*")) if p.is_file() and p.suffix in {".md", ".json", ".xml", ".py", ".txt"}]
case = BASE / "runtime/NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json"
selected.extend([case, Path(__file__)])
assert not EVIDENCE.exists() and not RECOVERY.exists()
copied = []
for source in selected:
    relative = source.relative_to(WORK) if source.is_relative_to(WORK) else Path(source.name)
    if relative.name.startswith("test_") and relative.suffix == ".py":
        relative = relative.with_name(relative.name + ".txt")
    raw = source.read_bytes()
    assert len(raw) < 2 * 1024 * 1024
    audit_payload("docs/validation/news_identity_candidate_20260908/" + relative.as_posix(), raw, set())
    for root in (EVIDENCE, RECOVERY):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as out:
            out.write(raw)
        assert digest(source) == digest(target)
    copied.append({"source": str(source), "copy": str(EVIDENCE / relative),
                   "recovery_copy": str(RECOVERY / relative), "bytes": len(raw), "sha256": digest(source)})

manifest = EVIDENCE / "EVIDENCE_MANIFEST_20260908.json"
save(manifest, {"schema": "offline_news_identity_candidate_evidence_v1", "files": copied,
                "test_source_extension_note": "Test source copies use .py.txt to avoid automatic test discovery. Restore original names and documented sibling source/runtime layout for replay.",
                "prior_run_source_note": "Prior 88-test source reconstructed from the final source by reversing the subsequent addition; both exact hashes match the retained prior test receipt."})
shutil.copyfile(manifest, RECOVERY / manifest.name)

validation = {
    "schema": "offline_news_identity_candidate_validation_v1_20260908",
    "recorded_utc": datetime.now(timezone.utc).isoformat(),
    "status": "passed_offline_candidate_only",
    "report": {"path": str(REPORT), "sha256": digest(REPORT)},
    "evidence_manifest": {"path": str(manifest), "sha256": digest(manifest), "file_count": len(copied)},
    "test_receipt": {"path": str(WORK / "CANDIDATE_TEST_RECEIPT_20260908.json"), "sha256": digest(WORK / "CANDIDATE_TEST_RECEIPT_20260908.json"), "tests_passed": 89},
    "replay": {"path": str(WORK / "CANDIDATE_RETAINED_CASE_REPLAY_20260908.json"), "sha256": digest(WORK / "CANDIDATE_RETAINED_CASE_REPLAY_20260908.json"), "input_topics": 2, "output_topics": 1, "original_members_preserved": 17, "directional_topics": 0},
    "current_registry_unchanged": {"path": str(registry_path), "sha256": digest(registry_path), "source_bindings": registry["source_bindings"], "pairs": 68},
    "recovery_directory": str(RECOVERY),
    "runtime_or_registration_changed": False,
    "old_guard_changed": False, "original_ledger_writes": False,
    "deployed": False, "live_publication_recovery_verified": False, "predictive_improvement_claimed": False,
    "can_place_orders": False, "can_promote": False,
    "remaining": ["new version and cohort integration", "live publication and coverage validation", "prospective predictive acceptance", "broader horizon recovery and comparisons"],
}
save(VALIDATION, validation)
for source in (REPORT, VALIDATION):
    shutil.copyfile(source, RECOVERY / source.name)
    assert digest(source) == digest(RECOVERY / source.name)

pending = PROJECT / "FOREX_PENDING_IMPROVEMENTS.md"
replace_once(pending,
    "Repair upstream identity/group reconciliation in a new version using the retained case; preserve original member clocks, deduplication and the guard.",
    "An offline candidate now reconciles that case as one context-only topic with all 17 members preserved; 89 tests passed. Versioned integration, live publication recovery and prospective acceptance remain open. See `docs/FOREX_NEWS_IDENTITY_CANDIDATE_20260908.md`; original member clocks, deduplication and the guard remain preserved.")
for relative, target in [("docs/RESEARCH_INDEX.md", "FOREX_NEWS_IDENTITY_CANDIDATE_20260908.md"),
                         ("docs/RESEARCH_INDEX_VAULT.md", "NEWS_IDENTITY_CANDIDATE_CURRENT.md")]:
    replace_once(PROJECT / relative, "## Active work\n", "## Active work\n\n- [September 8 offline news identity repair candidate — 89 tests; integration pending](" + target + ")\n")
with (PROJECT / "FOREX_PROJECT_LOG.md").open("a", encoding="utf-8") as out:
    out.write("\n\n## 2026-09-08 — offline news identity repair candidate accepted\n\n"
              "Prepared a pure producer-side reconciliation candidate outside live modules. The retained two-topic collision replays as one context-only topic with all 17 original member payloads preserved. The final 89-test run passed, including the unchanged admission suite and checks for identity ambiguity, original member clocks, stale singleton directions, window intersection, syndication, internal duplicate rejection and bounds. Earlier 88-test artifacts remain retained. No live source, registration, worker or original ledger changed. The baseline/recovery records remain unchanged. New version/cohort integration and fresh operational/predictive acceptance remain pending. See docs/FOREX_NEWS_IDENTITY_CANDIDATE_20260908.md.\n")
replace_once(PROJECT / "forex_model_vault_sync.py", "CANONICAL_PROJECT_RECORDS = (\n",
    'CANONICAL_PROJECT_RECORDS = (\n'
    '    (Path("trad/docs/FOREX_NEWS_IDENTITY_CANDIDATE_20260908.md"), "NEWS_IDENTITY_CANDIDATE_CURRENT.md"),\n'
    '    (Path("trad/FOREX_NEWS_IDENTITY_CANDIDATE_VALIDATION_20260908.json"), "NEWS_IDENTITY_CANDIDATE_VALIDATION_CURRENT.json"),\n')
ast.parse((PROJECT / "forex_model_vault_sync.py").read_text(encoding="utf-8-sig"))
save(WORK / "CANDIDATE_PACKAGING_RECEIPT_20260908.json", {
    "status": "passed", "validation_sha256": digest(VALIDATION),
    "evidence_files": len(copied), "recovery_directory": str(RECOVERY),
    "registry_source_bindings_still_match": all(digest(PROJECT / name) == expected for name, expected in registry["source_bindings"].items()),
    "baseline_validation_unchanged": digest(PROJECT / "FOREX_REVAMP_BASELINE_RECOVERY_VALIDATION_20260908.json") == "697140a991a30f11acd8d2e74c1fcc3309b8d0257762326186281651484b5c16",
})
print(json.dumps({"status": "passed", "evidence_files": len(copied), "validation_sha256": digest(VALIDATION)}))
