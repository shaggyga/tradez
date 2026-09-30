import importlib.util
import json
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location('snapshot', Path(__file__).parents[1]/'tools/forex_vault_snapshot.py')
snapshot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(snapshot)


def test_snapshot_copies_exact_docs_records_exclusions_and_detects_tampering(tmp_path):
    source=tmp_path/'source';source.mkdir()
    (source/'README.md').write_bytes(b'# Example\r\n')
    (source/'REVIEW.json').write_text('{"status":"pending"}')
    (source/'model.joblib').write_bytes(b'not loaded')
    (source/'credentials.json').write_text('{"password":"do-not-publish"}')
    (source/'report.md').write_text('access_token="'+ 'z'*40 +'"')
    (source/'source').mkdir();(source/'source'/'old.md').write_text('duplicate source')
    destination=tmp_path/'copy'
    result=snapshot.export(source,destination)
    assert result['files']==2
    assert (destination/'README.md').read_bytes()==b'# Example\r\n'
    manifest=json.loads((destination/'SNAPSHOT_MANIFEST.json').read_text())
    assert len(manifest['excluded'])==4
    assert not (destination/'credentials.json').exists()
    assert not (destination/'report.md').exists()
    (destination/'README.md').write_text('changed')
    with pytest.raises(ValueError,match='hash mismatch'):snapshot.verify(destination)


def test_refuses_existing_destination_and_nested_copy(tmp_path):
    source=tmp_path/'source';source.mkdir();destination=tmp_path/'copy';destination.mkdir()
    with pytest.raises(ValueError,match='new'):snapshot.export(source,destination)
    with pytest.raises(ValueError,match='disjoint'):snapshot.export(source,source/'copy')


def test_manifest_cannot_escape_copy(tmp_path):
    (tmp_path/'SNAPSHOT_MANIFEST.json').write_text(json.dumps({'files':[{'path':'../outside','bytes':1,'sha256':'0'*64}]}))
    with pytest.raises(ValueError,match='Unsafe'):snapshot.verify(tmp_path)


def test_unexpected_snapshot_files_are_rejected(tmp_path):
    source=tmp_path/'source';source.mkdir();destination=tmp_path/'copy'
    snapshot.export(source,destination)
    (destination/'unlisted.md').write_text('unexpected')
    with pytest.raises(ValueError,match='Unexpected'):snapshot.verify(destination)


def test_private_key_and_github_credential_are_withheld():
    assert snapshot.contains_secret(b'-----BEGIN PRIVATE KEY-----')
    assert snapshot.contains_secret(b'ghp_'+b'z'*36)
    assert not snapshot.contains_secret(b'access_token="<your_token_here>"')


def test_nested_design_is_kept_but_deep_history_is_referenced(tmp_path):
    source=tmp_path/'source'
    specification=source/'PACKET'/'specification';specification.mkdir(parents=True)
    history=source/'PACKET'/'nested_history';history.mkdir()
    (specification/'DESIGN.md').write_text('# Governing design')
    (history/'REPEAT.md').write_text('# Duplicate deep history')
    destination=tmp_path/'copy'
    result=snapshot.export(source,destination)
    assert result['files']==1
    assert (destination/'PACKET/specification/DESIGN.md').is_file()
    manifest=json.loads((destination/'SNAPSHOT_MANIFEST.json').read_text())
    assert manifest['excluded'][0]['reason']=='nested_historical_material_retrieve_from_shared_vault'
