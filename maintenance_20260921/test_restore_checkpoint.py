"""Focused safety tests; fixtures contain synthetic bytes only."""
import hashlib
import importlib.util
import json
from pathlib import Path
import stat
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location("checkpoint_restore_under_test", Path(__file__).with_name("restore_checkpoint.py"))
restore = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(restore)


def record(name, payload):
    return {"path": name, "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}


class RestoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.checkpoint = self.root / "checkpoint"
        self.checkpoint.mkdir()
        self.destination = self.root / "restored"

    def package(self, members=None, prefix="trad", extra_archive=None):
        members = members if members is not None else {"main.py": b"raise RuntimeError('must never run')\n"}
        manifest = {"schema": restore.SCHEMA, "files": [], "archives": []}
        for filename, entries, target in [("source.zip", members, prefix)] + (extra_archive or []):
            path = self.checkpoint / filename
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
                for name, payload in entries.items():
                    archive.writestr(name, payload)
            manifest["files"].append(record(filename, path.read_bytes()))
            manifest["archives"].append({"path": filename, "target_prefix": target,
                                           "members": [record(name, data) for name, data in entries.items()]})
        note = b"This is source-only metadata.\n"
        (self.checkpoint / "HANDOFF.md").write_bytes(note)
        manifest["files"].append(record("HANDOFF.md", note))
        self.save_manifest(manifest)
        return manifest

    def save_manifest(self, manifest):
        (self.checkpoint / "MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")

    def test_default_full_verify_then_restore_never_executes_source(self):
        self.package()
        report = restore.process_checkpoint(self.checkpoint)
        self.assertEqual(report["archive_member_count"], 1)
        self.assertFalse(self.destination.exists())
        restored = restore.process_checkpoint(self.checkpoint, self.destination)
        self.assertFalse(restored["code_executed"])
        self.assertTrue(restored["restored_files_sha256_verified"])
        self.assertTrue((self.destination / "trad/main.py").is_file())
        self.assertTrue((self.destination / "checkpoint_records/HANDOFF.md").is_file())
        self.assertFalse((self.destination / "checkpoint_records/source.zip").exists())
        self.assertTrue((self.destination / "checkpoint_records/RESTORE_VERIFICATION.json").is_file())

    def test_empty_target_prefix_preserves_sibling_structure(self):
        self.package({"direction_decision/src/helper.py": b"# helper\n", "trad/subdir/file.py": b"# source\n"}, prefix="")
        restore.process_checkpoint(self.checkpoint, self.destination)
        self.assertTrue((self.destination / "direction_decision/src/helper.py").is_file())
        self.assertTrue((self.destination / "trad/subdir/file.py").is_file())

    def test_archive_requires_whole_file_hash_record(self):
        manifest = self.package()
        manifest["files"] = [item for item in manifest["files"] if item["path"] != "source.zip"]
        self.save_manifest(manifest)
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_traversal_and_windows_aliases_rejected(self):
        for path in ("../escape.py", "/absolute.py", "a/../../x", "C:/x", "a\\x", "a//x", "a./x", "NUL.txt", "COM¹.txt", "a:x"):
            with self.subTest(path=path):
                with self.assertRaises(restore.CheckpointError):
                    restore.safe_relative(path)
        self.package({"../escape.py": b"x"})
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint, self.destination)
        self.assertFalse(self.destination.exists())

    def test_merged_archive_case_collision_rejected(self):
        self.package({"File.py": b"one"}, extra_archive=[("other.zip", {"file.py": b"two"}, "trad")])
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_merged_file_directory_collision_rejected(self):
        self.package({"module": b"one"}, extra_archive=[("other.zip", {"module/file.py": b"two"}, "trad")])
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_member_payload_hash_verified_even_when_zip_hash_is_valid(self):
        manifest = self.package()
        manifest["archives"][0]["members"][0]["sha256"] = "0" * 64
        self.save_manifest(manifest)
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint, self.destination)
        self.assertFalse(self.destination.exists())

    def test_whole_archive_hash_mismatch_rejected(self):
        manifest = self.package()
        manifest["files"][0]["sha256"] = "0" * 64
        self.save_manifest(manifest)
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_preexisting_destination_rejected(self):
        self.package()
        self.destination.mkdir()
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint, self.destination)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_unlisted_zip_member_rejected(self):
        manifest = self.package()
        with zipfile.ZipFile(self.checkpoint / "source.zip", "a") as archive:
            archive.writestr("extra.py", b"x")
        manifest["files"][0] = record("source.zip", (self.checkpoint / "source.zip").read_bytes())
        self.save_manifest(manifest)
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_zip_symlink_rejected(self):
        manifest = self.package({"link.py": b"target"})
        with zipfile.ZipFile(self.checkpoint / "source.zip", "w") as archive:
            info = zipfile.ZipInfo("link.py")
            info.create_system = 3
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(info, b"target")
        manifest["files"][0] = record("source.zip", (self.checkpoint / "source.zip").read_bytes())
        self.save_manifest(manifest)
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_records_collision_rejected(self):
        self.package({"HANDOFF.md": b"overwrite"}, prefix="checkpoint_records")
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)

    def test_duplicate_json_key_rejected(self):
        self.package()
        (self.checkpoint / "MANIFEST.json").write_text('{"schema":"x","schema":"y"}', encoding="utf-8")
        with self.assertRaises(restore.CheckpointError):
            restore.process_checkpoint(self.checkpoint)


if __name__ == "__main__":
    unittest.main()
