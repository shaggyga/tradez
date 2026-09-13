import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import pytest
import oanda_pair_local_forecast_study_v3 as worker
import prepare_pair_local_registry_v3 as prep


def test_prepare_full_frozen_metadata_source_bound_without_database(tmp_path):
    out=tmp_path/'controls.json'
    before=set(tmp_path.rglob('*'))
    result=prep.prepare_registry(out)
    registry=worker.load_registry(out)
    assert result['status']=='prepared_not_activated' and result['pairs']==68
    assert result['database_opened'] is False and result['worker_started'] is False
    assert set(tmp_path.rglob('*'))-before=={out}
    assert len(registry['source_bindings'])==9
    assert len({slot['contract']['contract_id'] for item in registry['pairs'].values() for slot in item['families'].values()})==136
    for pair,item in registry['pairs'].items():
        assert set(item['families'])==worker.FAMILIES
        for family,slot in item['families'].items():
            c=slot['contract'];assert c['horizon_sec']==3600 and c['cadence_sec']==900
            assert c['cohorts'][family].startswith(f'causal_pair_family_v2_20260907.{pair}.{family}.{worker.COHORT_SCOPE}.')
            assert c['source_bindings']==registry['source_bindings']
            assert all(c[f] is False for f in worker.INERT_FLAGS)


def test_preparer_preserves_existing_registry(tmp_path):
    out=tmp_path/'registry.json';out.write_bytes(b'original')
    with pytest.raises(ValueError,match='refuse_existing'):prep.prepare_registry(out)
    assert out.read_bytes()==b'original'


def test_old_registry_rejected_before_any_activation(tmp_path):
    registry=prep.make_registry(selected_pairs=['EUR_USD'])
    slot=registry['pairs']['EUR_USD']['families']['probabilistic_state_space']
    identity='causal_pair_family_v2_20260907.EUR_USD.probabilistic_state_space.fixture'
    slot['contract'].update(contract_id=identity,cohorts={'probabilistic_state_space':identity})
    slot['contract']['evaluation_protocol'].update(contract_id=identity+'.evaluation',cohorts={'probabilistic_state_space':identity})
    slot['contract_sha256']=worker.digest(slot['contract'])
    out=tmp_path/'registry.json';out.write_bytes(worker.encoded(registry))
    with pytest.raises(ValueError,match='distinct_opening_control_cohort'):worker.load_registry(out)


@pytest.mark.parametrize('name',['study','candle_root','quote_path'])
def test_explicit_routes_reject_non_c_before_lstat(monkeypatch,name,tmp_path):
    attempted=[]
    original=worker.os.lstat
    def probe(path,*args,**kwargs):
        assert not str(path).lower().startswith('d:')
        attempted.append(str(path));return original(path,*args,**kwargs)
    monkeypatch.setattr(worker.os,'lstat',probe)
    kwargs={'study':tmp_path/'pair_local_forecast_study_v3',name:Path('D:/unread/fixture')}
    with pytest.raises(ValueError,match='absolute_c'):worker.resolve_runtime_paths(**kwargs)
    assert all(not p.lower().startswith('d:') for p in attempted)


def test_explicit_and_default_routes_do_not_fall_back(tmp_path):
    study=tmp_path/'pair_local_forecast_study_v3'
    result=worker.resolve_runtime_paths(study=study,candle_root=tmp_path/'shared/candles',quote_path=tmp_path/'quotes/current.json')
    assert result=={'study':study,'candle_root':tmp_path/'shared/candles','quote_path':tmp_path/'quotes/current.json'}
    default=worker.resolve_runtime_paths(study=study)
    assert default['candle_root']==tmp_path/'candles'
    assert default['quote_path']==tmp_path/'state/practice_007_market_quotes_v1.json'


def test_main_plumbs_new_arguments_without_activation(monkeypatch,tmp_path):
    import sys
    got=[]
    monkeypatch.setattr(worker,'run',lambda *a,**k:got.append((a,k)))
    monkeypatch.setattr(sys,'argv',['worker','--config',str(tmp_path/'registry.json'),'--study',str(tmp_path/'pair_local_forecast_study_v3'),'--candle-root',str(tmp_path/'candles'),'--quote-path',str(tmp_path/'q.json'),'--once'])
    worker.main()
    assert got==[((tmp_path/'registry.json',),{'duration_sec':0,'once':True,'study':tmp_path/'pair_local_forecast_study_v3','candle_root':tmp_path/'candles','quote_path':tmp_path/'q.json'})]


def test_run_refuses_unactivated_study_before_lock_or_runner(tmp_path,monkeypatch):
    config=tmp_path/'registry.json';prep.prepare_registry(config,selected_pairs=['EUR_USD'])
    study=tmp_path/'pair_local_forecast_study_v3'
    def forbidden(*a,**k):raise AssertionError('runner must not initialize')
    monkeypatch.setattr(worker,'PairRunner',forbidden)
    with pytest.raises(ValueError,match='complete_prior_v3_activation_required'):
        worker.run(config,study=study,once=True)
    assert not study.exists()


def test_new_activation_refuses_existing_root_without_touching_its_bytes(tmp_path):
    study=tmp_path/'pair_local_forecast_study_v3';study.mkdir();marker=study/'old';marker.write_bytes(b'unchanged')
    with pytest.raises(ValueError,match='wholly_absent_v3'):
        worker.activate_fresh_study(tmp_path/'missing_registry.json',study=study)
    assert marker.read_bytes()==b'unchanged' and list(study.iterdir())==[marker]


