import json
import hashlib
from pathlib import Path

import oanda_issue_register_validator as subject


def issue(
    status: str,
    *,
    artifact: str = "receipt.json",
    artifact_sha256: str = "",
) -> dict:
    return {
        "issue_id": "case",
        "title": "Case",
        "priority": "P0",
        "owner_component": "collector",
        "status": status,
        "acceptance_tests": ["exact regression"],
        "evidence_paths": ["evidence.json"],
        "validation_artifacts": (
            [{"path": artifact, "sha256": artifact_sha256}]
            if artifact else []
        ),
        "status_history": [{"at_utc": "2026-09-01T00:00:00Z", "status": status}],
    }


def write_register(tmp_path: Path, row: dict) -> Path:
    path = tmp_path / "register.json"
    path.write_text(json.dumps({
        "schema_version": "forex_issue_register_v1",
        "issues": [row],
    }), encoding="utf-8")
    return path


def test_terminal_state_requires_existing_validation_artifact(tmp_path: Path) -> None:
    (tmp_path / "evidence.json").write_text("{}", encoding="utf-8")
    path = write_register(tmp_path, issue("complete", artifact="missing.json"))
    result = subject.validate_register(path, root=tmp_path)
    assert result["valid"] is False
    assert result["errors"] == ["case:missing_validation_artifact:missing.json"]


def test_validated_terminal_state_passes(tmp_path: Path) -> None:
    (tmp_path / "evidence.json").write_text("{}", encoding="utf-8")
    receipt = tmp_path / "receipt.json"
    receipt.write_text("{}", encoding="utf-8")
    path = write_register(tmp_path, issue(
        "complete",
        artifact_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),
    ))
    result = subject.validate_register(path, root=tmp_path)
    assert result["valid"] is True
    assert result["status_counts"]["complete"] == 1


def test_refresh_cannot_hide_status_history_or_external_blocker(tmp_path: Path) -> None:
    (tmp_path / "evidence.json").write_text("{}", encoding="utf-8")
    row = issue("blocked_external", artifact="")
    row["status_history"][-1]["status"] = "open"
    path = write_register(tmp_path, row)
    result = subject.validate_register(path, root=tmp_path)
    assert "case:status_history_mismatch" in result["errors"]
    assert "case:blocked_without_blocker" in result["errors"]
