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


@pytest.mark.parametrize("quote", ['"', "'"])
def test_quoted_identifier_like_password_is_rejected(tmp_path: Path, quote: str) -> None:
    git(tmp_path, "init")
    secret = "Alphabetic" + "PrivatePassword"
    (tmp_path / "source.py").write_text(f"password = {quote}{secret}{quote}\n", encoding="utf-8")
    result = audit(tmp_path, "candidate")
    assert result["passed"] is False
    assert result["findings"][0]["rule"] == "credential_assignment"
    assert secret not in str(result)


@pytest.mark.parametrize("filename", ["config.yaml", "config.ini", "config.json", "README.md"])
def test_bare_config_password_is_not_a_source_reference(tmp_path: Path, filename: str) -> None:
    git(tmp_path, "init")
    value = "Alphabetic" + "PrivatePassword"
    (tmp_path / filename).write_text(f"password: {value}\n", encoding="utf-8")
    assert audit(tmp_path, "candidate")["passed"] is False


def test_unquoted_python_reference_remains_allowed(tmp_path: Path) -> None:
    git(tmp_path, "init")
    (tmp_path / "source.py").write_text(
        "api_key = " + "configured_token_name\npassword = " + "self.configured_password\n", encoding="utf-8"
    )
    assert audit(tmp_path, "candidate")["passed"] is True


@pytest.mark.parametrize("style", ["embedded", "triple_direct", "triple_multiline", "triple_embedded", "comment", "raw", "bytes", "fstring"])
def test_literal_assignment_context_cannot_be_exempted_as_python_reference(tmp_path: Path, style: str) -> None:
    git(tmp_path, "init")
    value = "Alphabetic" + "PrivatePassword"
    payloads = {
        "embedded": f'message = "password = {value}"\n',
        "triple_direct": f'password = """{value}"""\n',
        "triple_multiline": f'password = """\n{value}\n"""\n',
        "triple_embedded": f'settings = """\npassword={value}\n"""\n',
        "comment": f'# password = {value}\n',
        "raw": f'password = r"{value}"\n',
        "bytes": f"password = b'''{value}'''\n",
        "fstring": f'message = f"password = {value}"\n',
    }
    (tmp_path / "source.py").write_text(payloads[style], encoding="utf-8")
    result = audit(tmp_path, "candidate")
    assert result["passed"] is False
    assert result["findings"][0]["rule"] == "credential_assignment"
    assert value not in str(result)


@pytest.mark.parametrize("filename", ["source.js", "source.ts", "source.jsx", "source.mjs"])
def test_javascript_strings_and_comments_have_no_reference_exemption(tmp_path: Path, filename: str) -> None:
    git(tmp_path, "init")
    value = "Alphabetic" + "PrivatePassword"
    (tmp_path / filename).write_text(f'const data = "password={value}";\n// password={value}\nconst password = `{value}`;\n', encoding="utf-8")
    result = audit(tmp_path, "candidate")
    assert result["passed"] is False
    assert result["finding_count"] == 3


@pytest.mark.parametrize("prefix,encoding", [("# Unicode café £\n", "utf-8"), ("# Unicode café £\n", "utf-8-sig"), ("# coding: latin-1\n# café\n", "latin-1")])
def test_token_proven_reference_uses_correct_byte_offsets(tmp_path: Path, prefix: str, encoding: str) -> None:
    git(tmp_path, "init")
    expression = "self." + "configured_password"
    (tmp_path / "source.py").write_bytes((prefix + "password = " + expression + "\r\n").encode(encoding))
    assert audit(tmp_path, "candidate")["passed"] is True


def test_uncertain_python_tokenization_does_not_exempt_references(tmp_path: Path) -> None:
    git(tmp_path, "init")
    expression = "self." + "configured_password"
    (tmp_path / "source.py").write_text("password = " + expression + "\nunclosed = (\n", encoding="utf-8")
    assert audit(tmp_path, "candidate")["passed"] is False
