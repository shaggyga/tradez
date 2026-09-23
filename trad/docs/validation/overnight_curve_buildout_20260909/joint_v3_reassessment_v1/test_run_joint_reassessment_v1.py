import json
from pathlib import Path
from types import SimpleNamespace
import pytest
import run_joint_reassessment_v1 as w


def fixture(tmp_path,monkeypatch,fail=False):
    monkeypatch.setattr(w,'BASE',tmp_path)
    pin={'source':'fixed'};monkeypatch.setattr(w,'bindings',lambda:dict(pin))
    historical=tmp_path/'historical';historical.mkdir();(historical/'prior').write_text('unchanged')
    module=SimpleNamespace(HERE=historical,ProcessPoolExecutor=object)
    def main():
        assert module.HERE!=historical
        with module.ProcessPoolExecutor(max_workers=4) as pool:
            values=list(pool.map(lambda value:value+1,[1,2],chunksize=1))
        assert values==[2,3]
        module.HERE.joinpath('inputs').mkdir();module.HERE.joinpath('inputs/AUD_CAD.json.gz').write_bytes(b'fixture')
        if fail:raise ValueError('fixture_partial_failure')
        module.HERE.joinpath('V3_COMPLETED_PERFORMANCE_20260909.json').write_text(json.dumps(
            dict(status='passed',registry_sha256=w.REGISTRY_SHA,ledgers=68)))
    module.main=main;monkeypatch.setattr(w,'load_original',lambda:module)
    return module,historical,pin


def test_original_output_untouched_and_same_process_globals_restored(tmp_path,monkeypatch):
    module,old,_=fixture(tmp_path,monkeypatch)
    result=w.run(tmp_path/'fresh')
    assert result['status']=='passed' and result['actual_parallel_workers']==1
    assert module.HERE==old and module.ProcessPoolExecutor is object
    assert [p.name for p in old.iterdir()]==['prior'] and (old/'prior').read_text()=='unchanged'
    assert {r['relative_path'] for r in result['output_files']}=={
        'WRAPPER_STARTED.json','inputs/AUD_CAD.json.gz','V3_COMPLETED_PERFORMANCE_20260909.json'}


def test_partial_failure_retained_without_false_completion(tmp_path,monkeypatch):
    module,old,_=fixture(tmp_path,monkeypatch,fail=True)
    with pytest.raises(ValueError,match='fixture_partial_failure'):w.run(tmp_path/'fresh')
    assert (tmp_path/'fresh/WRAPPER_FAILED.json').exists()
    assert not (tmp_path/'fresh/WRAPPER_COMPLETED.json').exists()
    assert (tmp_path/'fresh/inputs/AUD_CAD.json.gz').exists() and module.HERE==old


def test_changed_source_on_import_refused_before_output(tmp_path,monkeypatch):
    module,_,pin=fixture(tmp_path,monkeypatch)
    def changed():pin['source']='changed';return module
    monkeypatch.setattr(w,'load_original',changed)
    with pytest.raises(ValueError,match='sources_changed_on_import'):w.run(tmp_path/'fresh')
    assert not (tmp_path/'fresh').exists()


def test_bad_and_existing_output_roots_refused(tmp_path,monkeypatch):
    fixture(tmp_path,monkeypatch);existing=tmp_path/'existing';existing.mkdir()
    for path in (existing,tmp_path.parent/'outside',tmp_path/'nested'/'deeper'):
        with pytest.raises(ValueError,match='fresh_external_output_required'):w.run(path)


def test_serial_map_does_not_schedule_after_failure():
    called=[]
    def action(value):
        called.append(value)
        if value==2:raise ValueError('fail')
        return value
    with pytest.raises(ValueError):list(w.SerialExecutor(4).map(action,[1,2,3]))
    assert called==[1,2]


def test_serial_pair_cap():
    with pytest.raises(ValueError,match='assessment_pair_bound'):list(w.SerialExecutor(4).map(lambda x:x,range(69)))
