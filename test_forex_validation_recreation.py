from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

import forex_validation_recreation as recreation


def test_cli_default_profile_exists_in_packaged_contract(monkeypatch) -> None:
    monkeypatch.setattr(sys, "argv", ["forex_validation_recreation.py"])
    args = recreation.parse_args()
    config = json.loads(recreation.DEFAULT_CONFIG.read_text(encoding="utf-8-sig"))

    assert args.profile == recreation.DEFAULT_PROFILE
    assert args.profile in config["profiles"]


def test_resolve_json_path_supports_lists_and_lengths() -> None:
    payload = {"summary": {"failed": 0}, "rows": [{"status": "ok"}, {"status": "blocked"}]}
    assert recreation.resolve_json_path(payload, "summary.failed") == 0
    assert recreation.resolve_json_path(payload, "rows.$len") == 2
    assert recreation.resolve_json_path(payload, "rows.1.status") == "blocked"


def test_verify_assertions_reports_exact_observations() -> None:
    rows = recreation.verify_assertions(
        {"summary": {"passed": 4, "wired": False}},
        {"summary.passed": 4, "summary.wired": False},
    )
    assert all(row["passed"] for row in rows)


def test_evidence_mode_verifies_hash_and_expected_results(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence.json"
    evidence.write_text(
        json.dumps({"summary": {"failed": 0, "passed": 3}}),
        encoding="utf-8",
    )
    digest = hashlib.sha256(evidence.read_bytes()).hexdigest()
    config = {
        "profiles": {
            "fixture": {
                "evidence": [
                    {
                        "path": "evidence.json",
                        "sha256": digest,
                        "assertions": {
                            "summary.failed": 0,
                            "summary.passed": 3,
                        },
                    }
                ]
            }
        }
    }

    report = recreation.run_recreation(
        tmp_path,
        config,
        "fixture",
        "evidence",
        tmp_path / "output",
    )

    assert report["status"] == "passed"
    assert report["evidence"][0]["status"] == "verified"
    assert report["account_processes_started"] == 0


def test_evidence_hash_drift_fails_closed(tmp_path: Path) -> None:
    evidence = tmp_path / "evidence.json"
    evidence.write_text("{}", encoding="utf-8")
    profile = {
        "evidence": [
            {"path": "evidence.json", "sha256": "0" * 64}
        ]
    }
    with pytest.raises(recreation.ValidationRecreationError, match="hash mismatch"):
        recreation.verify_evidence(tmp_path, profile)


def test_configured_path_cannot_escape_import_root(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    profile = {"evidence": [{"path": "../outside.json"}]}
    with pytest.raises(recreation.ValidationRecreationError, match="escapes"):
        recreation.verify_evidence(tmp_path, profile)


def test_pytest_file_isolation_aggregates_independent_receipts(
    monkeypatch, tmp_path: Path
) -> None:
    for name in ("test_a.py", "test_b.py"):
        (tmp_path / name).write_text("", encoding="utf-8")
    calls = []

    def fake_run(command, *, root, timeout_seconds, label):
        calls.append((command, label))
        passed = 1 if label.endswith("test_a.py") else 2
        return {
            "label": label,
            "command": command,
            "returncode": 0,
            "stdout_tail": f"{passed} passed in 0.01s",
            "stderr_tail": "",
        }

    monkeypatch.setattr(recreation, "_run_command", fake_run)
    report = recreation.run_pytest(
        tmp_path,
        {
            "pytest_files": ["test_a.py", "test_b.py"],
            "pytest_minimum_passed": 3,
            "pytest_isolate_files": True,
        },
        tmp_path / "output",
    )

    assert report["status"] == "passed"
    assert report["passed"] == 3
    assert report["files"] == 2
    assert len(report["subprocesses"]) == 2
    assert len(calls) == 2


def test_compile_file_isolation_records_each_source(monkeypatch, tmp_path: Path) -> None:
    for name in ("a.py", "b.py"):
        (tmp_path / name).write_text("pass\n", encoding="utf-8")
    calls = []

    def fake_run(command, *, root, timeout_seconds, label):
        calls.append((command, label))
        return {
            "label": label,
            "command": command,
            "returncode": 0,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    monkeypatch.setattr(recreation, "_run_command", fake_run)
    report = recreation.run_compile(
        tmp_path,
        {
            "compile_files": ["a.py", "b.py"],
            "compile_isolate_files": True,
        },
    )

    assert report["status"] == "passed"
    assert report["files"] == 2
    assert len(report["subprocesses"]) == 2
    assert len(calls) == 2
