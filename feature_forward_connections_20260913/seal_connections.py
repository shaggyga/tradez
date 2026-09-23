"""Seal exactly the reviewed source changes; never read original runtime data."""
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(r"C:\Users\zmoor\Documents\forex\trad")
OUT = Path(__file__).parent
PARENT = "cc4e7321c6a7b5129c247664ee13b7c620208d63"
FILES = [
    "FOREX_PENDING_IMPROVEMENTS.md", "FOREX_PROJECT_LOG.md",
    "docs/FOREX_FEATURE_MOVE_REPAIRS_20260913.md",
    "docs/FOREX_CURRENT_SETUP_AND_SIGNAL_SOURCES_20260913.md",
    "docs/FOREX_FEATURE_FORWARD_CONNECTIONS_20260913.md",
    "docs/FOREX_FEATURE_FORWARD_PROTOCOL_20260913.md",
    "oanda_always_on_supervisor.ps1", "oanda_feature_move_mapping_v1.py",
    "oanda_feature_observations_v1.py", "test_oanda_feature_move_mapping_v1.py",
    "oanda_feature_forward_ledger_v1.py", "oanda_feature_forward_protocol_v1.py",
    "oanda_feature_forward_worker_v1.py", "oanda_feature_research_clock_v1.py",
    "oanda_research_feature_calculator_v1.py", "oanda_research_feature_observation_worker_v1.py",
    "test_oanda_feature_forward_v1.py", "test_oanda_feature_research_clock_v1.py",
    "test_oanda_feature_research_connections_v1.py", "test_oanda_research_feature_observation_worker_v1.py",
]


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True).stdout


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    mode = sys.argv[1]
    assert mode in ("validate", "commit")
    manifest, scan_hits = {}, []
    patterns = {
        "private_key": rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        "github_token": rb"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,})\b",
        "aws_access": rb"\bAKIA[0-9A-Z]{16}\b",
        "oanda_literal": rb"(?i)(?:api[_-]?key|access[_-]?token|password|secret)\s*[:=]\s*[\"'][A-Za-z0-9_-]{32,}[\"']",
    }
    for name in FILES:
        path = ROOT / name
        assert path.is_file() and not path.is_symlink()
        raw = path.read_bytes()
        manifest[name] = {"bytes": len(raw), "sha256": sha(raw)}
        if name.endswith(".py"):
            ast.parse(raw.decode("utf-8-sig"), filename=name)
        for rule, pattern in patterns.items():
            if re.search(pattern, raw):
                scan_hits.append({"file": name, "rule": rule})
    assert not scan_hits, scan_hits
    xml_path = OUT / "integrated_tests_final.xml"
    document = ET.parse(xml_path).getroot()
    suites = [document] if document.tag == "testsuite" else list(document.iter("testsuite"))
    counts = {key: sum(int(node.attrib.get(key, 0)) for node in suites)
              for key in ("tests", "failures", "errors", "skipped")}
    assert counts["tests"] >= 300 and counts["failures"] == counts["errors"] == counts["skipped"] == 0
    git("-c", "core.whitespace=blank-at-eol,blank-at-eof,space-before-tab,cr-at-eol", "diff", "--check")
    assert git("rev-parse", "HEAD").decode().strip() == PARENT
    if mode == "validate":
        extras = json.loads((OUT / "validation_context.json").read_text(encoding="utf-8"))
        proof = {
            "schema": "feature_forward_connections_validation_v1",
            "recorded_utc": datetime.now(timezone.utc).isoformat(),
            "parent_commit": PARENT, "files": manifest,
            "tests": {**counts, "junit": str(xml_path), "sha256": sha(xml_path.read_bytes())},
            "source_ast_parse": "passed", "targeted_secret_scan": "no_matches",
            "whitespace_check": "passed", "original_market_data_accessed": False,
            "runtime_services_started": False, "broker_actions": False,
            "model_fit_or_historical_rescore": False, **extras,
        }
        destination = OUT / "CONNECTION_VALIDATION.json"
        assert not destination.exists()
        destination.write_text(json.dumps(proof, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"validation": str(destination), "sha256": sha(destination.read_bytes()),
                          "tests": counts, "files": len(FILES)}))
        return
    receipt = json.loads((OUT / "CONNECTION_VALIDATION.json").read_text(encoding="utf-8"))
    assert receipt["files"] == manifest, "Source changed after validation"
    assert not git("diff", "--cached", "--name-only").strip(), "Preexisting staged changes"
    git("add", "--", *FILES)
    staged = set(git("diff", "--cached", "--name-only").decode().splitlines())
    assert staged == set(FILES), (staged ^ set(FILES))
    for name in FILES:
        assert git("show", ":" + name) == (ROOT / name).read_bytes(), name
    git("commit", "-m", "Connect rich research observations to forward outcome evaluation")
    commit = git("rev-parse", "HEAD").decode().strip()
    assert git("rev-parse", "HEAD^").decode().strip() == PARENT
    for name in FILES:
        assert git("show", "HEAD:" + name) == (ROOT / name).read_bytes(), name
    checkpoint = {
        "schema": "feature_forward_connections_checkpoint_v1", "commit": commit,
        "parent_commit": PARENT, "files_verified": len(FILES),
        "validation_sha256": sha((OUT / "CONNECTION_VALIDATION.json").read_bytes()),
        "recorded_utc": datetime.now(timezone.utc).isoformat(),
        "remaining_worktree_status": git("status", "--short").decode(),
        "pushed": False, "services_started": False,
    }
    destination = OUT / "CHECKPOINT.json"
    assert not destination.exists()
    destination.write_text(json.dumps(checkpoint, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(checkpoint))


if __name__ == "__main__":
    main()
