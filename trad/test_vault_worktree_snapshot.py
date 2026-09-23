from __future__ import annotations

import json
import subprocess
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    from tools import vault_worktree_snapshot as snapshot
except ModuleNotFoundError:
    from trad.tools import vault_worktree_snapshot as snapshot


def git(root: Path, *args: str, stdin: bytes | None = None) -> bytes:
    return subprocess.run(["git", "-C", str(root), *args], input=stdin,
                          capture_output=True, check=True).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init")
    git(root, "config", "user.name", "Snapshot Test")
    git(root, "config", "user.email", "test@example.invalid")
    (root / "source.py").write_text("VALUE = 1\n")
    (root / ".gitignore").write_text("creds\ndata/\n")
    git(root, "add", ".")
    git(root, "commit", "-m", "baseline")
    return root


def test_dirty_untracked_snapshot_is_not_commit_and_preserves_git(repo: Path, tmp_path: Path) -> None:
    (repo / "source.py").write_text("VALUE = 2\n")
    (repo / "test_new.py").write_text("def test_new():\n    assert True\n")
    before = (repo / ".git/index").read_bytes()
    head = git(repo, "rev-parse", "HEAD")
    result = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert result["kind"] == "working_tree_snapshot"
    assert result["worktree_dirty"] is True
    assert result["is_clean_commit_archive"] is False
    assert result["verification"]["python_files_compiled"] == 2
    assert (repo / ".git/index").read_bytes() == before
    assert git(repo, "rev-parse", "HEAD") == head
    manifest = json.loads((tmp_path / "vault/source" / result["manifest"]).read_bytes())
    assert {row["path"] for row in manifest["files"]} == {".gitignore", "source.py", "test_new.py"}
    assert not (tmp_path / "vault/source/SOURCE_BASELINE_LATEST.json").exists()


def test_missing_tracked_member_fails(repo: Path, tmp_path: Path) -> None:
    (repo / "source.py").unlink()
    with pytest.raises(RuntimeError, match="missing or unreadable"):
        snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert not (tmp_path / "vault/source" / snapshot.POINTER).exists()


def test_staged_symlink_rejected_without_platform_symlink_privilege(repo: Path, tmp_path: Path) -> None:
    blob = git(repo, "hash-object", "-w", "--stdin", stdin=b"source.py").decode().strip()
    git(repo, "update-index", "--add", "--cacheinfo", f"120000,{blob},linked.py")
    (repo / "linked.py").write_text("source.py")
    with pytest.raises(RuntimeError, match="symlink"):
        snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")


@pytest.mark.parametrize("name", ["../escape.py", "/absolute.py", "a/../escape.py", "C:/escape.py", "a\\escape.py", "a//b.py",
                                 "NUL.py", "COM1.json", "a./b.py", "a /b.py", "bad?.json", "LPT\u00b2.txt"])
def test_unsafe_path_rejected(name: str) -> None:
    with pytest.raises(RuntimeError, match="unsafe"):
        snapshot.safe_name(name)


def test_unexpected_credential_rejected_without_value(repo: Path, tmp_path: Path) -> None:
    value = "sk-" + "Ab23Cd45" * 4
    (repo / "unsafe.md").write_text(value)
    with pytest.raises(RuntimeError, match="credential audit") as error:
        snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert value not in str(error.value)


def test_known_private_value_rejected_in_unlabelled_source(repo: Path, tmp_path: Path) -> None:
    value = "0123456789abcdef" * 2 + "-" + "fedcba9876543210" * 2
    (repo / "creds").write_text(value)
    (repo / "unsafe.md").write_text("accidental copy " + value)
    with pytest.raises(RuntimeError, match="known private value") as error:
        snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert value not in str(error.value)


def test_private_and_runtime_members_omitted_with_reasons(repo: Path, tmp_path: Path) -> None:
    (repo / "data").mkdir()
    (repo / "data/evidence.json").write_text("{}")
    git(repo, "add", "-f", "data/evidence.json")
    (repo / "credentials.json").write_text("{}")
    result = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    manifest = json.loads((tmp_path / "vault/source" / result["manifest"]).read_bytes())
    assert {row["reason"] for row in manifest["excluded"]} == {"runtime_or_binary_payload", "private_path"}
    assert "not recursively inventoried" in manifest["ignored_scope"]


def test_mutation_during_verification_never_advances_pointer(repo: Path, tmp_path: Path) -> None:
    original = snapshot.verify_snapshot
    def mutate(path: Path, **kwargs):
        result = original(path, **kwargs)
        (repo / "source.py").write_text("VALUE = 3\n")
        return result
    with patch.object(snapshot, "verify_snapshot", side_effect=mutate):
        with pytest.raises(RuntimeError, match="changed during snapshot"):
            snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert not (tmp_path / "vault/source" / snapshot.POINTER).exists()


def test_repeated_content_preserves_immutable_pair(repo: Path, tmp_path: Path) -> None:
    first = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    source = tmp_path / "vault/source"
    originals = {name: ((source / first[name]).read_bytes(), (source / first[name]).stat().st_mtime_ns)
                 for name in ("archive", "manifest")}
    second = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert second["reused"] is True
    for name, (payload, modified) in originals.items():
        assert second[name] == first[name]
        assert (source / second[name]).read_bytes() == payload
        assert (source / second[name]).stat().st_mtime_ns == modified


def test_verifier_rejects_archive_hash_mismatch(repo: Path, tmp_path: Path) -> None:
    result = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    source = tmp_path / "vault/source"
    with (source / result["archive"]).open("ab") as handle:
        handle.write(b"corruption")
    with pytest.raises(RuntimeError, match="archive hash mismatch"):
        snapshot.verify_snapshot(source / result["manifest"])


def test_verifier_rejects_hash_bound_traversal(repo: Path, tmp_path: Path) -> None:
    result = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    source = tmp_path / "vault/source"
    manifest_path = source / result["manifest"]
    manifest = json.loads(manifest_path.read_bytes())
    archive = source / result["archive"]
    with zipfile.ZipFile(archive, "a") as handle:
        handle.writestr("../escape.py", "raise RuntimeError('not run')")
    manifest["archive_sha256"] = snapshot.digest(archive.read_bytes())
    manifest_path.write_bytes(snapshot.encoded(manifest))
    with pytest.raises(RuntimeError, match="unsafe"):
        snapshot.verify_snapshot(manifest_path)


def test_compile_does_not_execute_code(repo: Path, tmp_path: Path) -> None:
    (repo / "source.py").write_text("raise RuntimeError('must never execute')\n")
    result = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert result["verification"]["code_executed"] is False


def test_late_mutation_after_immutable_publication_never_advances_pointer(repo: Path, tmp_path: Path) -> None:
    original = snapshot.assert_unchanged
    calls = 0
    def mutate(root, state, paths, rows):
        nonlocal calls
        calls += 1
        if calls == 2:
            (repo / "source.py").write_text("VALUE = 4\n")
        original(root, state, paths, rows)
    with patch.object(snapshot, "assert_unchanged", side_effect=mutate):
        with pytest.raises(RuntimeError, match="changed during snapshot"):
            snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert not (tmp_path / "vault/source" / snapshot.POINTER).exists()
    assert len(list((tmp_path / "vault/source").glob("*.zip"))) == 1


def test_unknown_binary_extension_is_explicitly_excluded(repo: Path, tmp_path: Path) -> None:
    (repo / "unexpected.exe").write_bytes(b"MZ binary")
    result = snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    manifest = json.loads((tmp_path / "vault/source" / result["manifest"]).read_bytes())
    assert {"path": "unexpected.exe", "reason": "non_source_extension"} in manifest["excluded"]


def test_persisted_archive_corruption_cannot_publish_passed_pointer(repo: Path, tmp_path: Path) -> None:
    original = snapshot.verify_snapshot
    def corrupt(path: Path, **kwargs):
        if path.parent.name == "source":
            manifest = json.loads(path.read_bytes())
            with (path.parent / manifest["archive"]).open("ab") as handle:
                handle.write(b"corruption after write")
        return original(path, **kwargs)
    with patch.object(snapshot, "verify_snapshot", side_effect=corrupt):
        with pytest.raises(RuntimeError, match="archive hash mismatch"):
            snapshot.sync_worktree_snapshot(repo, tmp_path / "vault")
    assert not (tmp_path / "vault/source" / snapshot.POINTER).exists()


@pytest.mark.parametrize("value", [b"Alphabetic" + b"PrivatePassword", b"Identifier_" + b"Like_Private_Value"])
def test_private_literal_is_retained_even_when_it_looks_like_identifier(tmp_path: Path, value: bytes) -> None:
    private = tmp_path / "private_fixture.txt"
    private.write_bytes(value + b"\n")
    known = snapshot.known_private_values([private])
    assert value in known
    with pytest.raises(RuntimeError, match="known private value"):
        snapshot.audit_payload("source.py", b'VALUE = "' + value + b'"\n', known)


def test_quoted_alphabetic_assignment_rejected_without_private_file() -> None:
    value = b"Alphabetic" + b"PrivatePassword"
    with pytest.raises(RuntimeError, match="credential_assignment"):
        snapshot.audit_payload("config/synthetic_example.py", b'password = "' + value + b'"\n', set())


def test_private_assignment_literal_is_not_exempted_as_expression(tmp_path: Path) -> None:
    value = b"Alphabetic" + b"PrivatePassword"
    private = tmp_path / "private_fixture.txt"
    private.write_bytes(b'password = "' + value + b'"\n')
    assert value in snapshot.known_private_values([private])


def test_unquoted_source_expression_still_allowed_but_not_config_literal() -> None:
    payload = b"password = " + b"self.configured_password\n"
    snapshot.audit_payload("source.py", payload, set())
    with pytest.raises(RuntimeError, match="credential_assignment"):
        snapshot.audit_payload("config.yaml", payload, set())


@pytest.mark.parametrize("wrapper", [b'message = "password=%s"\n', b'password = """%s"""\n', b'settings = """\npassword=%s\n"""\n', b'# password=%s\n'])
def test_snapshot_rejects_literal_assignments_inside_python_contexts(wrapper: bytes) -> None:
    value = b"Alphabetic" + b"PrivatePassword"
    with pytest.raises(RuntimeError, match="credential_assignment"):
        snapshot.audit_payload("source.py", wrapper % value, set())


def test_known_private_literal_precedes_even_valid_python_reference_exemption() -> None:
    value = b"configured_" + b"private_password"
    payload = b"password = " + value + b"\n"
    snapshot.audit_payload("source.py", payload, set())
    with pytest.raises(RuntimeError, match="known private value"):
        snapshot.audit_payload("source.py", payload, {value})


def test_private_triple_quoted_literal_is_retained(tmp_path: Path) -> None:
    value = b"Alphabetic" + b"PrivatePassword"
    private = tmp_path / "synthetic_private.txt"
    private.write_bytes(b'password = """' + value + b'"""\n')
    assert value in snapshot.known_private_values([private])
