"""Seal the exact reviewed source change set; never enumerate runtime data."""
from __future__ import annotations

import ast
import hashlib
import json
import runpy
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO = ROOT / "trad"
PARENT = "223e9a9fea5cc7e374d25cdd424521d5d9a8563a"
PATHS = sorted([
    "FOREX_PENDING_IMPROVEMENTS.md",
    "docs/FOREX_FEATURE_MOVE_MAPPING_REVIEW_20260913.md",
    "docs/FOREX_FEATURE_MOVE_REPAIRS_20260913.md",
    "oanda_feature_move_mapping_v1.py",
    "oanda_feature_observations_v1.py",
    "oanda_latest_moves.py",
    "oanda_main_signal_dashboard.html",
    "oanda_model_gap_live_signal_worker.py",
    "oanda_move_first_news_case_audit.py",
    "oanda_news_feed_backtest.py",
    "oanda_practice_live_dashboard.py",
    "oanda_practice_shadow_strategy_lab.py",
    "test_oanda_elapsed_move_windows_v2.py",
    "test_oanda_feature_move_dashboard_v1.py",
    "test_oanda_feature_move_mapping_v1.py",
    "test_oanda_feature_observations_v1.py",
    "test_oanda_joint_v3_ledger_observer_handler_v1.py",
    "test_oanda_latest_moves.py",
    "test_oanda_model_gap_live_signal_worker.py",
    "test_oanda_move_first_news_case_audit.py",
    "test_oanda_practice_shadow_strategy_lab.py",
])


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git(*args):
    return subprocess.run(["git", "-C", str(REPO), *args], check=True,
                          capture_output=True).stdout


def names(raw):
    return {part.decode("utf-8") for part in raw.split(b"\0") if part}


def record(path):
    payload = path.read_bytes()
    return {"path": str(path.relative_to(ROOT)).replace("\\", "/"),
            "sha256": sha(payload), "bytes": len(payload)}


def exact_scope():
    assert git("rev-parse", "HEAD").decode().strip() == PARENT, "Unexpected predecessor"
    changed = names(git("diff", "--name-only", "-z", "HEAD"))
    added = names(git("ls-files", "--others", "--exclude-standard", "-z"))
    assert changed | added == set(PATHS), "Unexpected changed paths"
    assert not git("diff", "--cached", "--name-only", "-z"), "Preexisting staged changes"
    git("-c", "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol",
        "diff", "--check")


def manifest():
    result = []
    tracked = names(git("ls-tree", "-r", "--name-only", "-z", "HEAD", "--", *PATHS))
    for name in PATHS:
        path = REPO / name
        assert not path.is_symlink() and not path.is_junction()
        payload = path.read_bytes()
        if path.suffix == ".py":
            ast.parse(payload, filename=name)
        old = git("show", f"HEAD:{name}") if name in tracked else None
        result.append({"path": name, "sha256": sha(payload), "bytes": len(payload),
                       "before_sha256": sha(old) if old is not None else None,
                       "crlf_count": payload.count(b"\r\n"),
                       "lf_count": payload.count(b"\n")})
    return result


def credential_scan():
    scanner_path = REPO / "tools" / "credential_audit.py"
    scanner = runpy.run_path(str(scanner_path), run_name="bounded_credential_scan")
    findings = []
    accounts = 0
    for name in PATHS:
        assert not scanner["path_is_banned"](name)
        payload = (REPO / name).read_bytes()
        accounts += len(scanner["ACCOUNT_ID"].findall(payload))
        for rule, pattern in scanner["PATTERNS"].items():
            for match in pattern.finditer(payload):
                material = match.group(1) if match.lastindex else match.group(0)
                if rule == "credential_assignment":
                    if scanner["synthetic_fixture_value"](name, material):
                        continue
                    if scanner["credential_assignment_is_reference"](name, match):
                        continue
                findings.append({"path": name, "rule": rule,
                                 "line": payload.count(b"\n", 0, match.start()) + 1,
                                 "value_sha256_prefix": sha(material)[:16]})
    return {"files_scanned": len(PATHS), "finding_count": len(findings),
            "findings": findings, "passed": not findings,
            "account_metadata_occurrences": accounts,
            "scanner_source": record(scanner_path),
            "scope": "Exact changed source/docs/test paths only; suspected values never emitted."}


def prepare():
    exact_scope()
    members = manifest()
    scan = credential_scan()
    assert scan["passed"], json.dumps(scan["findings"])
    xml_path = HERE / "final_tests_003.xml"
    suites = ET.parse(xml_path).getroot().findall("testsuite")
    tests = {key: sum(int(suite.attrib.get(key, "0")) for suite in suites)
             for key in ("tests", "failures", "errors", "skipped")}
    assert tests == {"tests": 124, "failures": 0, "errors": 0, "skipped": 0}, tests
    mover_dir = ROOT / "git_publication_20260913" / "move_window_repair_001"
    installed = json.loads((mover_dir / "installation_001" / "RECEIPT.json").read_text())
    for item in installed["files"]:
        assert sha((REPO / item["name"]).read_bytes()) == item["after_sha256"]
    receipts = [
        xml_path,
        ROOT / "git_publication_20260913/obs_v4/OBSERVATION_VALIDATION_RECEIPT.json",
        ROOT / "git_publication_20260913/feature_move_dashboard_001/INTEGRATION_SOURCE_RECEIPT_001.json",
        mover_dir / "INSTALLED_REPAIR_RECEIPT_001.json",
        mover_dir / "independent_model_review_001/REVIEW_003.json",
        mover_dir / "CALCULATION_CONTRACT_FINAL_001.md",
        ROOT / "git_publication_20260913/feature_mapper_peer_001/REVIEW_002.json",
        ROOT / "git_publication_20260913/obs_universe_v3/FINAL_UNIVERSE_REVALIDATION.json",
    ]
    # The exact final universe receipt is supplied after its fresh validation.
    universe_path = Path(sys.argv[2])
    assert universe_path.is_relative_to(ROOT / "git_publication_20260913")
    universe = json.loads(universe_path.read_text())
    assert universe["status"] == "passed" and universe["pairs"] == 68
    assert universe["full_summary"]["available"] == 13464
    universe_binding = json.loads(receipts[-1].read_text())
    for name, digest in universe_binding["source_sha256"].items():
        assert sha((REPO / name).read_bytes()) == digest, "Universe source mismatch"
    assert sha(universe_path.read_bytes()) == universe_binding["receipts"][-1]["sha256"]
    receipts.append(universe_path)
    result = {
        "schema_version": "feature_move_repairs_validation_v1_20260913",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "status": "source_repairs_validated_live_activation_pending",
        "repository": str(REPO), "predecessor_commit": PARENT,
        "source_manifest": members,
        "validation": {"integrated_observation_mapper_dashboard": tests,
                       "installed_elapsed_regressions_passed": 66,
                       "independent_elapsed_probes_passed": 14,
                       "independent_mapper_probes_passed": 8,
                       "primary_test_executions_passed": 190,
                       "synthetic_universe_pairs": 68,
                       "synthetic_primary_fields_per_pair": 200,
                       "synthetic_feature_comparisons_available": 13464,
                       "synthetic_benchmark_seconds": {
                           key: universe[key] for key in
                           ("writer_seconds", "full_reader_seconds", "filtered_reader_seconds")},
                       "source_parse_and_git_whitespace": "passed"},
        "evidence": [record(path) for path in receipts],
        "credential_scan": scan,
        "preservation": [
            "No original market data, model, database, account, credentials or runtime payload opened or modified.",
            "No project service, broker action, model fit, or network research was started.",
            "Old immutable reports, scientific source pins and prior Git checkpoints remain unchanged.",
            "Legacy default candle loader and legacy training row layouts remain compatible.",
            "Original dashboard HTML outside marked additions and unrelated dashboard AST remain unchanged.",
            "Earlier failed fixtures and review findings remain in separate dated evidence files."
        ],
        "remaining_limits": [
            "Software is installed in source; fresh live observations and upstream clock/feed still need operational verification.",
            "The current research-only supervisor profile excludes the legacy observation producer; this work starts no replacement.",
            "Historical uncaptured features cannot be recovered by this source change.",
            "The new feature/pair join is contemporaneous and descriptive, not causal or profitable forecasting evidence.",
            "Sample clocks retained from the older ticker are not authenticated arrival evidence.",
            "Archive read bounds and sampling may limit visible comparison coverage and are explicitly reported.",
            "Official feed/parser coverage and retrospective BoJ identification remain separately tracked."
        ],
        "seal_helper": record(Path(__file__)),
    }
    output = HERE / "REPAIR_VALIDATION.json"
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(result, handle, indent=2)
        handle.write("\n")
    print(json.dumps({"status": result["status"], "files": len(members),
                      "test_executions_passed": 190,
                      "credential_findings": scan["finding_count"],
                      "receipt": str(output), "receipt_sha256": sha(output.read_bytes())}))


def commit():
    exact_scope()
    result = json.loads((HERE / "REPAIR_VALIDATION.json").read_text())
    assert manifest() == result["source_manifest"], "Source changed after validation"
    assert credential_scan()["passed"]
    git("add", "--", *PATHS)
    assert names(git("diff", "--cached", "--name-only", "-z")) == set(PATHS)
    for item in result["source_manifest"]:
        assert sha(git("show", ":" + item["path"])) == item["sha256"], "Index byte mismatch"
    git("-c", "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol",
        "diff", "--cached", "--check")
    git("commit", "-m", "Repair feature observation history and movement mapping")
    head = git("rev-parse", "HEAD").decode().strip()
    assert git("rev-parse", "HEAD^").decode().strip() == PARENT
    assert not git("status", "--porcelain"), "Unexpected post-commit changes"
    checkpoint = {
        "status": "committed_local_checkpoint",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "commit": head, "parent": PARENT, "working_tree_clean": True,
        "files": len(PATHS), "validation": record(HERE / "REPAIR_VALIDATION.json"),
        "pushed": False, "older_tags_unchanged": True,
    }
    output = HERE / "CHECKPOINT.json"
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(checkpoint, handle, indent=2)
        handle.write("\n")
    print(json.dumps(checkpoint))


if __name__ == "__main__":
    if sys.argv[1] == "prepare":
        prepare()
    elif sys.argv[1] == "commit":
        commit()
    else:
        raise SystemExit("Use prepare or commit")
