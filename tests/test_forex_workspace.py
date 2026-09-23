"""Offline safety tests. Supply --evidence to retain each test's synthetic files."""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import subprocess
import sys
import unittest
from unittest import mock
from types import SimpleNamespace
import uuid
import warnings
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("forex_workspace", ROOT / "tools" / "forex_workspace.py")
workspace = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(workspace)
EVIDENCE = None


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def record(name, data):
    return {"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def snapshot(path):
    return {item.relative_to(path).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
            for item in path.rglob("*") if item.is_file()}


class RetrievalTests(unittest.TestCase):
    def setUp(self):
        self.root = EVIDENCE / (self._testMethodName + "_" + uuid.uuid4().hex[:8])
        self.root.mkdir()
        self.vault = self.root / "vault"
        self.vault.mkdir()
        self.destination = self.root / "retrieved"
        unsigned = {"schema_version": "all68_run_identity.v2", "contract": {}, "dependency_hashes": {}}
        self.identity = {**unsigned, "fingerprint": hashlib.sha256(encoded(unsigned)).hexdigest()}
        # Deliberately invalid model bytes: successful byte retrieval must never deserialize them.
        self.files = {"nested/fit.joblib": b"not a model: deserialization is forbidden", "result.json": b"{}"}
        payloads = [record(name, data) for name, data in sorted(self.files.items())]
        self.files["RUN_IDENTITY.json"] = encoded(self.identity)
        self.files["COMPLETION_MANIFEST.json"] = encoded({"payloads": payloads,
            "required_payloads": sorted(item["path"] for item in payloads), "run_identity": self.identity})
        self.manifest = {"schema_version": "forex_saved_artifacts_manifest.v1",
            "original_members": [record(name, data) for name, data in sorted(self.files.items())],
            "original_run_identity_fingerprint": self.identity["fingerprint"],
            "original_run_identity_sha256": hashlib.sha256(self.files["RUN_IDENTITY.json"]).hexdigest(),
            "original_completion_manifest_sha256": hashlib.sha256(self.files["COMPLETION_MANIFEST.json"]).hexdigest(),
            "payload_count": 2, "model_count": 1, "model_payloads": ["nested/fit.joblib"],
            "recipe_reference": {}, "required_source_references": [], "original_environment": {"python": "3.12.10"}}
        self.files[workspace.MANIFEST_NAME] = encoded(self.manifest)
        self.artifact = {key: self.manifest[key] for key in (
            "original_run_identity_fingerprint", "original_run_identity_sha256", "original_completion_manifest_sha256",
            "payload_count", "model_count", "recipe_reference", "required_source_references", "original_environment")}
        self.artifact.update({"archive_vault_relative_path": "saved.zip",
            "saved_artifacts_manifest_sha256": hashlib.sha256(self.files[workspace.MANIFEST_NAME]).hexdigest()})
        self.pack()
        self.registry = {"artifacts": {"fixture": self.artifact}}

    def pack(self, extra=()):
        stream = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
                for name, data in self.files.items():
                    zipped.writestr(name, data)
                for name, data in extra:
                    if isinstance(name, str):
                        raw_name = name
                        name = zipfile.ZipInfo(raw_name)
                        # The Windows constructor normalizes backslashes. Restore raw
                        # ZIP names so the unsafe-path test actually reaches the reader.
                        name.filename = name.orig_filename = raw_name
                    zipped.writestr(name, data)
        self.archive = stream.getvalue()
        (self.vault / "saved.zip").write_bytes(self.archive)
        self.artifact.update({"archive_sha256": hashlib.sha256(self.archive).hexdigest(),
            "archive_bytes": len(self.archive), "archive_member_count": len(self.files) + len(extra),
            "archive_expanded_bytes": sum(map(len, self.files.values())) + sum(len(data) for _, data in extra)})

    def retrieve(self):
        return workspace.retrieve(self.vault, "fixture", self.destination, self.registry)

    def reparse_metadata(self, paths, tag, *, extra_attributes=0, symlink=False):
        """Simulate only lstat metadata; never change real hydration/link state."""
        original = Path.lstat
        selected = set(paths)
        def synthetic(path, *args, **kwargs):
            actual = original(path, *args, **kwargs)
            if path not in selected:
                return actual
            return SimpleNamespace(
                st_mode=stat.S_IFLNK if symlink else actual.st_mode,
                st_file_attributes=getattr(actual, "st_file_attributes", 0) | 0x400 | extra_attributes,
                st_reparse_tag=tag)
        return mock.patch.object(Path, "lstat", synthetic)

    def test_all_cloud_tag_variants_allow_source_only_byte_retrieval(self):
        before = snapshot(self.vault)
        for variant in range(16):
            with self.subTest(variant=variant), self.reparse_metadata(
                    (self.vault, self.vault / "saved.zip"), 0x9000001A | (variant << 12)):
                result = self.retrieve()
                self.assertEqual(result["status"], "retrieved_verified" if variant == 0 else "reused_verified")
                self.assertEqual(result["models_loaded"], 0)
                self.assertEqual(result["models_fitted"], 0)
        self.assertEqual(snapshot(self.vault), before)

    def test_cloud_status_reads_vault_ancestors_and_files(self):
        self.status_fixture()
        selected = [self.vault, *(p for p in self.vault.rglob("*") if p.is_file())]
        with self.reparse_metadata(selected, 0x9000F01A):
            self.assertEqual(workspace.status(self.vault)["exact_next_item"], "next_scientific_step")

    def test_cloud_registry_source_read_is_allowed(self):
        path = self.vault / "registry.json"
        path.write_bytes(encoded(self.registry))
        with mock.patch.object(workspace, "REGISTRY", path), self.reparse_metadata((path,), 0x9000001A):
            self.assertEqual(workspace.load_registry(), self.registry)

    def test_cloud_destination_or_parent_remains_rejected(self):
        self.destination.mkdir()
        for path in (self.destination, self.root):
            with self.subTest(path=path), self.reparse_metadata((path,), 0x9000001A):
                with self.assertRaisesRegex(workspace.ReviewRequired, "reparse point"):
                    self.retrieve()
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_source_rejects_name_surrogates_unknown_tags_and_symlink_mode(self):
        for tag, symlink in ((0xA0000003, False), (0xA000000C, False),
                             (0xB000001A, False), (0x9000001C, False),
                             (0x00000000, False), (0x9000001A, True)):
            with self.subTest(tag=tag, symlink=symlink), self.reparse_metadata(
                    (self.vault / "saved.zip",), tag, symlink=symlink):
                with self.assertRaisesRegex(workspace.ReviewRequired, "reparse point"):
                    self.retrieve()
                self.assertFalse(self.destination.exists())

    def test_offline_or_recall_cloud_source_is_rejected_before_open(self):
        for attribute in (0x1000, 0x40000, 0x400000):
            with self.subTest(attribute=attribute), self.reparse_metadata(
                    (self.vault / "saved.zip",), 0x9000001A, extra_attributes=attribute):
                with mock.patch.object(Path, "open", side_effect=AssertionError("must not hydrate")):
                    with self.assertRaisesRegex(workspace.ReviewRequired, "not locally available"):
                        self.retrieve()
                self.assertFalse(self.destination.exists())

    def test_success_and_exact_read_only_reuse(self):
        before = snapshot(self.vault)
        result = self.retrieve()
        self.assertEqual(result["status"], "retrieved_verified")
        self.assertEqual(result["models_loaded"], 0)
        self.assertEqual(result["models_fitted"], 0)
        self.assertEqual(snapshot(self.vault), before)
        after = snapshot(self.destination)
        times = {p: p.stat().st_mtime_ns for p in self.destination.rglob("*") if p.is_file()}
        self.assertEqual(self.retrieve()["status"], "reused_verified")
        self.assertEqual(snapshot(self.destination), after)
        self.assertEqual(times, {p: p.stat().st_mtime_ns for p in times})

    def test_partial_destination_is_preserved(self):
        self.destination.mkdir()
        (self.destination / "result.json").write_bytes(b"my existing data")
        before = snapshot(self.destination)
        with self.assertRaisesRegex(workspace.ReviewRequired, "Partial"):
            self.retrieve()
        self.assertEqual(snapshot(self.destination), before)

    def test_corrupt_complete_destination_is_preserved(self):
        self.retrieve()
        (self.destination / "result.json").write_bytes(b"corrupt")
        before = snapshot(self.destination)
        with self.assertRaisesRegex(workspace.ReviewRequired, "mismatch"):
            self.retrieve()
        self.assertEqual(snapshot(self.destination), before)

    def test_unexpected_destination_directory_is_preserved(self):
        self.retrieve()
        (self.destination / "unexpected").mkdir()
        with self.assertRaisesRegex(workspace.ReviewRequired, "Partial"):
            self.retrieve()
        self.assertTrue((self.destination / "unexpected").is_dir())

    def test_bad_archive_hash_makes_no_destination(self):
        self.artifact["archive_sha256"] = "0" * 64
        with self.assertRaisesRegex(workspace.ReviewRequired, "Archive SHA256"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_payload_hash_mismatch_makes_no_destination(self):
        self.files["result.json"] = b"bad"
        self.pack()
        with self.assertRaisesRegex(workspace.ReviewRequired, "byte count|hash mismatch"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_saved_manifest_hash_mismatch(self):
        self.artifact["saved_artifacts_manifest_sha256"] = "0" * 64
        with self.assertRaisesRegex(workspace.ReviewRequired, "manifest SHA256"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_unsafe_paths_must_fail_before_writes(self):
        for name in ("../escape", "/absolute", "C:/drive", "a\\b", "CON.txt", "trailing.", "a//b"):
            with self.subTest(name=name):
                self.pack(((name, b"x"),))
                with self.assertRaisesRegex(workspace.ReviewRequired, "Unsafe"):
                    self.retrieve()
                self.assertFalse(self.destination.exists())

    def test_duplicate_and_case_collision(self):
        for name in ("result.json", "RESULT.json", "NESTED/other.json"):
            with self.subTest(name=name):
                self.pack(((name, b"{}"),))
                with self.assertRaisesRegex(workspace.ReviewRequired, "collid"):
                    self.retrieve()
                self.assertFalse(self.destination.exists())

    def test_symlink_zip_entry(self):
        info = zipfile.ZipInfo("link")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        self.pack(((info, b"target"),))
        with self.assertRaisesRegex(workspace.ReviewRequired, "Non-regular"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_directory_and_file_collision(self):
        self.pack((("nested", b"x"),))
        with self.assertRaisesRegex(workspace.ReviewRequired, "collision"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_oversized_expansion(self):
        with mock.patch.object(workspace, "MAX_BYTES", 100):
            with self.assertRaisesRegex(workspace.ReviewRequired, "expansion"):
                workspace.inspect_archive(self.archive, self.artifact)
        self.assertFalse(self.destination.exists())

    def test_extra_zip_member(self):
        self.pack((("extra.txt", b"surprise"),))
        with self.assertRaisesRegex(workspace.ReviewRequired, "inventory"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_pinned_run_identity_mismatch(self):
        self.artifact["original_run_identity_fingerprint"] = "0" * 64
        with self.assertRaisesRegex(workspace.ReviewRequired, "identity mismatch"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_destination_inside_vault_rejected(self):
        self.destination = self.vault / "new"
        before = snapshot(self.vault)
        with self.assertRaisesRegex(workspace.ReviewRequired, "outside"):
            self.retrieve()
        self.assertEqual(snapshot(self.vault), before)

    def test_missing_archive_does_not_fit_or_create_destination(self):
        self.artifact["archive_vault_relative_path"] = "unavailable.zip"
        with self.assertRaisesRegex(workspace.ReviewRequired, "unavailable"):
            self.retrieve()
        self.assertFalse(self.destination.exists())

    def test_duplicate_json_key_rejected(self):
        with self.assertRaisesRegex(workspace.ReviewRequired, "Duplicate JSON"):
            workspace.json_bytes(b'{"a": 1, "a": 2}')

    def status_fixture(self):
        manifest_data = b'{"files": []}'
        for name in ("engine", "review"):
            (self.vault / name).mkdir()
            (self.vault / name / "MANIFEST.json").write_bytes(manifest_data)
        for name, folder in (("DESIGN_ALIGNMENT_LATEST.json", "engine"), ("CHECKPOINT_REVIEW_LATEST.json", "review")):
            (self.vault / name).write_bytes(encoded({"package": folder, "manifest": folder + "/MANIFEST.json",
                "manifest_sha256": hashlib.sha256(manifest_data).hexdigest()}))
        (self.vault / "REVIEW_QUEUE.json").write_bytes(encoded({"steps": [{"step_id": "current", "review_status": "accepted_within_scope"}],
            "active_step_id": "current", "exact_next_item": "next_scientific_step", "updated_utc": "2026-09-23T00:00:00Z"}))
        (self.vault / "CHAT_COORDINATION_BOARD.md").write_text("owner | IN_PROGRESS\n", encoding="utf-8")
        (self.vault / "VAULT_FIRST_REUSE.md").write_text("Reuse before fitting.\n", encoding="utf-8")

    def test_status_reads_current_queue_and_board_without_mutation(self):
        self.status_fixture()
        before = snapshot(self.vault)
        result = workspace.status(self.vault)
        self.assertEqual(result["exact_next_item"], "next_scientific_step")
        self.assertIn("IN_PROGRESS", result["coordination_board"])
        self.assertEqual(result["active_steps"][0]["review_status"], "accepted_within_scope")
        self.assertEqual(snapshot(self.vault), before)

    def test_status_rejects_pointer_manifest_corruption(self):
        self.status_fixture()
        (self.vault / "engine" / "MANIFEST.json").write_bytes(b"changed")
        with self.assertRaisesRegex(workspace.ReviewRequired, "manifest SHA256"):
            workspace.status(self.vault)

    def test_status_detects_queue_changed_during_read(self):
        self.status_fixture()
        original = workspace.read_bytes
        reads = 0
        def changing_read(path, limit=workspace.MAX_JSON_BYTES, **kwargs):
            nonlocal reads
            value = original(path, limit, **kwargs)
            if Path(path).name == "REVIEW_QUEUE.json":
                reads += 1
                if reads > 1:
                    return value + b" "
            return value
        with mock.patch.object(workspace, "read_bytes", changing_read):
            with self.assertRaisesRegex(workspace.ReviewRequired, "changed during"):
                workspace.status(self.vault)

    def test_cli_unicode_board_is_portable_ascii_json(self):
        self.status_fixture()
        (self.vault / "CHAT_COORDINATION_BOARD.md").write_text("Owner \u2014 review \U0001f50e\n", encoding="utf-8")
        result = subprocess.run([sys.executable, "-I", "-B", str(ROOT / "tools/forex_workspace.py"),
                                 "status", "--vault", str(self.vault)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout.decode("ascii"))
        self.assertIn("\U0001f50e", report["coordination_board"])

    def test_cli_failure_has_review_required_and_exit_2(self):
        result = subprocess.run([sys.executable, "-I", "-B", str(ROOT / "tools/forex_workspace.py"),
                                 "retrieve", "--vault", str(self.vault), "--artifact", "unknown",
                                 "--destination", str(self.destination)], capture_output=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout.decode("ascii"))["status"], "review_required")
        self.assertFalse(self.destination.exists())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    options, remaining = parser.parse_known_args()
    EVIDENCE = options.evidence.resolve()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[sys.argv[0], *remaining], verbosity=2)
