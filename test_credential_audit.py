from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from trad.tools.credential_audit import audit


def git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


def test_audit_redacts_real_secret_and_allows_explicit_synthetic_fixture(tmp_path: Path) -> None:
    git(tmp_path, "init")
    candidate = "sk-" + "realisticlookingsecret123456789"
    (tmp_path / "safe.py").write_text(f'API_KEY = "{candidate}"\n', encoding="utf-8")
    result = audit(tmp_path, "candidate")
    assert result["passed"] is False
    assert result["findings"][0]["value_sha256_prefix"]
    assert "realisticlooking" not in str(result)

    (tmp_path / "safe.py").unlink()
    fixture = tmp_path / "test_oanda_latest_moves.py"
    synthetic = "practice-token-" + "value-that-must-not-be-persisted"
    fixture.write_text(f'API_KEY = "{synthetic}"\n', encoding="utf-8")
    result = audit(tmp_path, "candidate")
    assert result["passed"] is True


def test_commit_audit_is_pinned_to_requested_revision(tmp_path: Path) -> None:
    git(tmp_path, "init")
    git(tmp_path, "config", "user.name", "Test")
    git(tmp_path, "config", "user.email", "test@example.invalid")
    source = tmp_path / "source.py"
    source.write_text("VALUE = 'safe'\n", encoding="utf-8")
    git(tmp_path, "add", "source.py")
    git(tmp_path, "commit", "-m", "safe")
    safe_commit = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    unsafe_value = "sk-" + "realisticlookingsecret123456789"
    source.write_text(f'API_KEY = "{unsafe_value}"\n', encoding="utf-8")
    git(tmp_path, "add", "source.py")
    git(tmp_path, "commit", "-m", "unsafe")

    result = audit(tmp_path, "commit", revision=safe_commit)
    assert result["passed"] is True
    assert result["audited_revision"] == safe_commit


def test_fixture_marker_does_not_hide_a_second_real_secret(tmp_path: Path) -> None:
    git(tmp_path, "init")
    fixture = tmp_path / "test_oanda_latest_moves.py"
    dummy_value = "practice-token-" + "value-that-must-not-be-persisted"
    faux_real_value = "realsecretmaterial" + "1234567890"
    fixture.write_text(
        f'API_KEY = "{dummy_value}"\n'
        f'ACCESS_TOKEN = "{faux_real_value}"\n',
        encoding="utf-8",
    )
    result = audit(tmp_path, "candidate")
    assert result["passed"] is False
    assert result["finding_count"] == 1


@pytest.mark.parametrize(
    "relative",
    (".env.local", "creds.prod", "credentials.yaml", ".netrc", "private/secrets/token.txt"),
)
def test_private_path_variants_are_rejected(tmp_path: Path, relative: str) -> None:
    git(tmp_path, "init")
    target = tmp_path / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("not echoed\n", encoding="utf-8")
    result = audit(tmp_path, "candidate")
    assert result["passed"] is False
    assert result["findings"][0]["rule"] == "banned_private_path"
