"""Source-bound preparation is separate from later actual-clock activation."""
import hashlib
import json
from pathlib import Path
import pytest

import prepare_joint_price_news_registry_v3 as prep
import oanda_joint_price_news_forecast_study_v3 as worker
from test_oanda_joint_price_news_forecast_worker_v3 import registry_fixture


def metadata_fixture(tmp_path,monkeypatch):
    registry,_=registry_fixture(tmp_path,monkeypatch)
    monkeypatch.setattr(prep,'ROOT',tmp_path)
    value={'schema_version':'pair_local_forecast_registry_v2_20260907',
        'registry_id':'pair_local_forecast_study_v2_20260907','pairs':registry['pairs']}
    path=tmp_path/'frozen_metadata.json';path.write_bytes(worker.encoded(value))
    return path,hashlib.sha256(path.read_bytes()).hexdigest()


def test_prepare_has_real_preparation_time_but_no_activation_or_database(tmp_path,monkeypatch):
    path,digest=metadata_fixture(tmp_path,monkeypatch)
    output=tmp_path/'prepared'/'registry.json';at=1788872400.125
    result=prep.prepare_registry(output,metadata_path=path,metadata_sha256=digest,clock=lambda:at)
    registry=worker.load_registry(output)
    assert result['status']=='prepared_not_activated' and result['created_epoch']==at
    assert registry['created_epoch']==at and registry['preparation_clock_is_activation'] is False
    assert result['database_opened'] is result['worker_started'] is result['historical_rows_imported'] is False
    assert not list(tmp_path.rglob('study.sqlite')) and not list(tmp_path.rglob('activation_receipt.json'))
    for pair,item in registry['pairs'].items():
        c=item['families']['ridge_price_news_v1']['contract']
        assert c['contract_id'].endswith('.news_repair_v3_20260908.prospective')
        assert c['feature_version']=='sha256:'+registry['source_bindings']['oanda_causal_forecast_inputs_joint_news_v2.py']
        assert 'activated_epoch' not in c and 'activated_epoch' not in c['evaluation_protocol']
        assert c['evaluation_protocol']['historical_start_utc']==worker.utc(at)


def test_preparation_cannot_overwrite_existing_registration(tmp_path,monkeypatch):
    path,digest=metadata_fixture(tmp_path,monkeypatch)
    output=tmp_path/'registry_original.json';output.write_bytes(b'preserved registration')
    with pytest.raises(ValueError,match='refuse_existing'):
        prep.prepare_registry(output,metadata_path=path,metadata_sha256=digest)
    assert output.read_bytes()==b'preserved registration'


def test_frozen_pip_metadata_is_used_without_currency_suffix_heuristic(tmp_path,monkeypatch):
    path,digest=metadata_fixture(tmp_path,monkeypatch)
    result=prep.make_registry(selected_pairs=['HKD_JPY'],scope='preflight',metadata_path=path,metadata_sha256=digest)
    assert set(result['pairs'])=={'HKD_JPY'}
    assert result['pairs']['HKD_JPY']['pip_size']==.0001
    assert result['pairs']['HKD_JPY']['families']['ridge_price_news_v1']['contract']['pip_size']==.0001


@pytest.mark.parametrize('case',['unknown_pair','empty_pairs','source_metadata_changed','scope_path','scope_missing'])
def test_ambiguous_preparation_is_rejected_before_output(tmp_path,monkeypatch,case):
    path,digest=metadata_fixture(tmp_path,monkeypatch)
    kwargs={'metadata_path':path,'metadata_sha256':digest}
    if case=='unknown_pair':kwargs['selected_pairs']=['ABC_DEF']
    if case=='empty_pairs':kwargs['selected_pairs']=[]
    if case=='source_metadata_changed':path.write_text('{}')
    if case=='scope_path':kwargs['scope']='../old'
    if case=='scope_missing':kwargs['scope']=''
    output=tmp_path/'must_not_exist.json'
    with pytest.raises(ValueError):prep.prepare_registry(output,**kwargs)
    assert not output.exists() and not list(tmp_path.rglob('study.sqlite'))


def test_actual_activation_after_preparation_never_reuses_preparation_clock(tmp_path,monkeypatch):
    path,digest=metadata_fixture(tmp_path,monkeypatch);prepared=1788872400.0
    output=tmp_path/'prepared.json'
    prep.prepare_registry(output,metadata_path=path,metadata_sha256=digest,clock=lambda:prepared)
    values=iter([prepared+600,prepared+601,prepared+602,prepared+603,prepared+604,prepared+605,prepared+606,prepared+607,prepared+608,prepared+609])
    receipt=worker.activate_fresh_study(output,study=tmp_path/'joint_price_news_study_v3',clock=lambda:next(values))
    assert receipt['activation_started_epoch']>=prepared+600
    assert all(r['activated_epoch']>=prepared+600 for r in receipt['ledgers'])
    assert all(not any(r['counts'].values()) for r in receipt['ledgers'])
