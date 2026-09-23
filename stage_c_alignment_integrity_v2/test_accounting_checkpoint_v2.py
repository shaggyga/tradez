import json
from pathlib import Path
import sys
import zipfile

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from accounting_checkpoint_v2 import export_checkpoint, restore_checkpoint, SOURCE_ALLOWLIST, SCHEMA
from portable_checkpoint_v2 import inspect_package
from publication import sha256_file


@pytest.fixture(scope="module")
def package(tmp_path_factory):
    root = tmp_path_factory.mktemp("accounting-export")
    path = root / "accounting.zip"
    return path, export_checkpoint(path)


def test_relocated_accounting_source_predecessors_and_all_states_replay(package, tmp_path):
    path, receipt = package
    restored = restore_checkpoint(path, tmp_path / "restored", receipt["sha256"])
    assert restored["subprocess_replay_verified"]
    assert restored["replay"] == receipt["expected_replay"]
    assert restored["replay"]["independent_event_arm_rows"] == 40
    assert len(restored["replay"]["payloads"]) == 25
    assert {file.name for file in (tmp_path / "restored" / "source").iterdir()} == set(SOURCE_ALLOWLIST)
    assert restored['replay']['packaged_native_operator']['runs']['reference']['payloads']
    assert receipt['validation_runs_included_in_zip'] is False
    assert Path(receipt['local_validation_evidence']).is_dir()
    with zipfile.ZipFile(path) as z:
        assert not any(n.startswith('expected') for n in z.namelist())


def test_wrong_external_hash_never_creates_destination(package, tmp_path):
    path, receipt = package
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        restore_checkpoint(path, tmp_path / "untouched", "0" * 64)
    assert not (tmp_path / "untouched").exists()


def test_old_neutral_checkpoint_contract_cannot_be_confused_with_accounting(package):
    path, receipt = package
    with pytest.raises(ValueError, match="unsupported checkpoint contract"):
        inspect_package(path, receipt["sha256"])
    assert inspect_package(path, receipt["sha256"], expected_source_allowlist=SOURCE_ALLOWLIST, expected_schema=SCHEMA)


def test_source_tampering_is_rejected_even_with_recomputed_external_hash(package, tmp_path):
    source, receipt = package
    altered = tmp_path / "changed.zip"
    with zipfile.ZipFile(source) as original, zipfile.ZipFile(altered,"x") as archive:
        for item in original.infolist():
            raw = original.read(item)
            if item.filename == "source/accounting_events_v2.py":
                raw += b"\n# changed dependency\n"
            archive.writestr(item, raw)
    with pytest.raises(ValueError, match="member hash"):
        restore_checkpoint(altered, tmp_path / "refused", sha256_file(altered))
    assert not (tmp_path / "refused").exists()


def test_completed_package_cannot_overwrite_prior_checkpoint(package):
    path, receipt = package
    with pytest.raises(FileExistsError, match="immutable"):
        export_checkpoint(path)
    assert sha256_file(path) == receipt["sha256"]
