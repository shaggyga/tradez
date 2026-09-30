import json
import hashlib
import pytest
import inspect_operational_runtime_v1 as runtime
from inspect_operational_runtime_v1 import capture


@pytest.fixture(autouse=True)
def isolated_root(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime,'ROOT',tmp_path)


def test_status_uses_profile_identity_and_actual_clock(tmp_path):
    heartbeat=tmp_path/'heartbeat.json';profile=tmp_path/'profile.json'
    profile.write_text(json.dumps({'services':[{'name':'quotes','heartbeat':str(heartbeat),
        'heartbeat_field':'worker','heartbeat_schema':'quotes_v1','max_age_sec':90}]}))
    heartbeat.write_text(json.dumps({'worker':'quotes_v1','updated_at':'2026-01-01T00:00:00Z','status':'running'}))
    row=capture(profile,now=1767225630)['services'][0]
    assert row['fresh'] and row['age_sec']==30 and row['schema_matches']
    assert not capture(profile,now=1767225800)['services'][0]['fresh']
    assert not capture(profile,now=1767225500)['services'][0]['fresh']
    heartbeat.write_text(json.dumps({'worker':'wrong','generated_epoch':1767225630}))
    assert not capture(profile,now=1767225630)['services'][0]['fresh']


def test_new_file_mtime_cannot_replace_missing_embedded_clock(tmp_path):
    heartbeat=tmp_path/'heartbeat.json';heartbeat.write_text('{"schema_version":"v1"}')
    profile=tmp_path/'profile.json'
    profile.write_text(json.dumps({'services':[{'name':'test','heartbeat':str(heartbeat),
        'heartbeat_schema':'v1','max_age_sec':90}]}))
    row=capture(profile)['services'][0]
    assert not row['fresh'] and 'clock unavailable' in row['read_error']


def test_fresh_controller_requires_the_current_profile_identity(tmp_path):
    profile=tmp_path/'profile.json';profile.write_text('{"services":[]}')
    state=tmp_path/'data/oanda_training_manager/state';state.mkdir(parents=True)
    path=state/'operational_supervisor_v2.json'
    value={'schema_version':'operational_supervisor_v6_20260916',
        'generated_epoch':1000,'operational_profile_sha256':hashlib.sha256(profile.read_bytes()).hexdigest()}
    path.write_text(json.dumps(value))
    row=capture(profile,now=1010)['controllers'][0]
    assert row['fresh'] and row['profile_matches']
    assert not capture(profile,now=1200)['controllers'][0]['fresh']
    value['operational_profile_sha256']='0'*64;path.write_text(json.dumps(value))
    row=capture(profile,now=1010)['controllers'][0]
    assert not row['fresh'] and not row['profile_matches']
