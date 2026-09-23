"""Offline closure/portability checks for compact integrity checkpoints."""
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import zipfile

import pytest

import forex_model_vault_sync as vault
import oanda_four_hour_best_improvement_pass as watch
import oanda_integrity_publication as details


SNAPSHOT=vault.INTEGRITY_SNAPSHOT_ARTIFACTS[0]
SECTION="move_first_operational_mapping_alignment_v4"


@pytest.fixture
def tmp_path(tmp_path_factory):
    # Keep the real restored project layout within Windows MAX_PATH.
    return tmp_path_factory.mktemp("i")


def fixture_snapshot(root,episode="first"):
    full={"schema_version":1,"status":"degraded","failures":["clock_explicitly_classified"],
          "generated_utc":"2026-09-06T00:00:00+00:00","checks":{"example":False},
          SECTION:{"contract_id":"frozen-fixture","research_only":True,"execution_eligible":False,
                   "episode_rows":[{"episode_id":episode,"mapping_hash":"fixture","nested":{"value":17}}]}}
    path=root/SNAPSHOT
    path.parent.mkdir(parents=True,exist_ok=True)
    compact=details.compact_integrity_payload(full,snapshot_dir=path.parent)
    path.write_text(json.dumps(compact),encoding="utf-8")
    return full,compact,path


def test_collect_only_verified_current_detail_closure(tmp_path):
    full,compact,path=fixture_snapshot(tmp_path)
    original=path.read_bytes()
    references=details.verified_detail_artifact_bytes(compact,snapshot_dir=path.parent)
    orphan=path.parent/details.DETAIL_DIRECTORY/"unreferenced.json"
    orphan.write_text("unreferenced",encoding="utf-8")
    files=vault.collect_files(tmp_path)
    assert path in files and set(references)<=set(files)
    assert orphan not in files
    assert path.read_bytes()==original
    assert details.restore_integrity_details(compact,snapshot_dir=path.parent)==full


def test_legacy_full_snapshot_does_not_add_or_read_detail_files(tmp_path):
    path=tmp_path/SNAPSHOT
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({SECTION:{"episode_rows":[{"original":True}]}}))
    assert vault.collect_files(tmp_path)==[path]


@pytest.mark.parametrize("damage",["missing","corrupt","path","hash","size","count","schema"])
def test_bad_detail_aborts_before_existing_checkpoint_changes(tmp_path,damage):
    _,compact,path=fixture_snapshot(tmp_path)
    reference=compact[SECTION]["_detail_reference"]
    artifact=path.parent/reference["path"]
    if damage=="missing": artifact.unlink()
    elif damage=="corrupt": artifact.write_bytes(b"corrupt")
    elif damage=="path": reference["path"]="../outside.json"
    elif damage=="hash": reference["sha256"]="f"*64
    elif damage=="size": reference["bytes"]+=1
    elif damage=="count": reference["item_count"]+=1
    else: compact["_detail_publication"]["contract_id"]="wrong"
    path.write_text(json.dumps(compact))
    destination=tmp_path/"existing.zip"
    destination.write_bytes(b"prior checkpoint")
    with pytest.raises((ValueError,OSError)):
        vault.collect_files(tmp_path)
    with pytest.raises((ValueError,OSError)):
        vault.write_zip(tmp_path,[path],{},destination)
    assert destination.read_bytes()==b"prior checkpoint"


def test_write_zip_captures_new_generation_closure_after_collect(tmp_path):
    _,old,path=fixture_snapshot(tmp_path)
    files=vault.collect_files(tmp_path)
    manifest=vault.build_manifest(tmp_path,files)
    full,new,_=fixture_snapshot(tmp_path,episode="next")
    old_name=(SNAPSHOT.parent/old[SECTION]["_detail_reference"]["path"]).as_posix()
    new_name=(SNAPSHOT.parent/new[SECTION]["_detail_reference"]["path"]).as_posix()
    destination=tmp_path/"checkpoint.zip"
    written=vault.write_zip(tmp_path,files,manifest,destination)
    with zipfile.ZipFile(destination) as archive:
        assert json.loads(archive.read(SNAPSHOT.as_posix()))==new
        assert new_name in archive.namelist() and old_name not in archive.namelist()
        restored=tmp_path/"restored"
        archive.extractall(restored)
    assert details.restore_integrity_details(new,snapshot_dir=(restored/SNAPSHOT).parent)==full
    assert set(row["path"] for row in written["files"])=={SNAPSHOT.as_posix(),new_name}


def test_streaming_zip_uses_same_verified_bytes_if_live_files_change(tmp_path,monkeypatch):
    full,current,path=fixture_snapshot(tmp_path)
    trigger=tmp_path/"trad/00trigger.py"
    trigger.write_text("# fixture\n")
    files=vault.collect_files(tmp_path)
    manifest=vault.build_manifest(tmp_path,files)
    snapshot_bytes=path.read_bytes()
    artifact=path.parent/current[SECTION]["_detail_reference"]["path"]
    artifact_bytes=artifact.read_bytes()
    original_open=vault.open_binary_read
    changed=[]
    def change_during_stream(candidate):
        if candidate==trigger:
            path.write_text(json.dumps({"status":"newer","failures":[]}))
            artifact.write_bytes(b"damaged after verified capture")
            changed.append(True)
        return original_open(candidate)
    monkeypatch.setattr(vault,"open_binary_read",change_during_stream)
    destination=tmp_path/"checkpoint.zip"
    vault.write_zip(tmp_path,files,manifest,destination)
    assert changed
    with zipfile.ZipFile(destination) as archive:
        assert archive.read(SNAPSHOT.as_posix())==snapshot_bytes
        assert archive.read(artifact.relative_to(tmp_path).as_posix())==artifact_bytes
        restored=tmp_path/"restored"
        archive.extractall(restored)
    assert details.restore_integrity_details(current,snapshot_dir=(restored/SNAPSHOT).parent)==full


def test_four_hour_snapshot_records_portable_locator_and_restores_saved_bytes(tmp_path,monkeypatch):
    original=tmp_path
    full,compact,path=fixture_snapshot(original)
    trad=original/"trad"
    monkeypatch.setattr(watch,"ROOT",trad)
    monkeypatch.setattr(watch,"STATE",path.parent)
    for name in ("refresh_control","account_snapshot","quote_highwater","important_files","process_snapshot"):
        monkeypatch.setattr(watch,name,lambda *args,**kwargs:{})
    saved=watch.snapshot({},1,datetime.now(timezone.utc))
    assert saved["integrity_snapshot_source"]==watch.INTEGRITY_SNAPSHOT_SOURCE
    assert saved["selected_action"]==watch.select_action({"integrity":compact})
    moved=tmp_path/"restored_project"
    moved_state=moved/watch.INTEGRITY_PROJECT_RELATIVE_PATH.parent
    for artifact,encoded in details.verified_detail_artifact_bytes(compact,snapshot_dir=path.parent).items():
        target=moved_state/artifact.relative_to(path.parent)
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(encoded)
    # No current snapshot exists in the restored project. Its historical
    # embedded bytes, explicit locator and immutable details are sufficient.
    assert watch.restore_snapshot_integrity(saved,project_root=moved)==full
    bad=copy.deepcopy(saved)
    bad["integrity_snapshot_source"]["project_relative_path"]="../outside.json"
    with pytest.raises(ValueError,match="locator"):
        watch.restore_snapshot_integrity(bad,project_root=moved)
    bad.pop("integrity_snapshot_source")
    with pytest.raises(ValueError,match="locator"):
        watch.restore_snapshot_integrity(bad,project_root=moved)
    assert watch.restore_snapshot_integrity({"integrity":full},project_root=moved)==full
