import copy
import json
from pathlib import Path

import pytest

import oanda_observed_management_io_v1 as io
import oanda_forecast_curve_contract_v1 as contract
import oanda_forecast_curve_file_store_v1 as store
import oanda_s5_mba_research_capture_v1 as candles
import test_oanda_curve_momentum_observation_v1 as momentum_fixture

CYCLE='cycle_1788934000000000000'
SOURCES={'fixture.py':'a'*64}


def clocks(*values):
    iterator=iter(values)
    return lambda:next(iterator)


def anchor(root,*,convention='official_midpoint',pair='GBP_USD',status='issued_and_consumed'):
    policy=contract.make_policy(native_horizons_sec=[3600],maximum_reference_age_sec=30,
        maximum_build_sec=10,maximum_issue_delay_sec=10,maximum_publication_delay_sec=10,
        maximum_decision_age_sec=14500,minimum_remaining_sec=1)
    prepared=contract.prepare_curve(instrument=pair,pip_size='0.0001',
        forecast_cohort='pilot_fixture/'+pair+'/'+convention,model_sha256='b'*64,
        feature_version='fixture',source_bindings=SOURCES,input_capture_sha256='c'*64,
        input_available_epoch=1201,reference_epoch=1200,reference_label_epoch=1195,
        reference_price='1.2500',reference_price_kind='official_midpoint_S5_close',
        bar_duration_sec=5,model_fitted_epoch=1000,computation_started_epoch=1201,computed_epoch=1202,
        points=[dict(horizon_sec=3600,target_epoch=4800,target_label_epoch=4795,
            model_id='fixture',predicted_signed_pips='3',probability_up='0.6',
            probability_scope='original_unconditional_uncalibrated')],
        policy=policy,computation_sha256='d'*64,input_context={'price_convention':convention})
    curve=contract.issue_curve(prepared,expected_source_bindings=SOURCES,clock=lambda:1203)
    published=store.publish_curve(root/'published',curve,expected_source_bindings=SOURCES,clock=clocks(1204,1205,1206))
    observation=dict(instrument=pair,price_convention=convention,status=status,
        registry_issue_admitted=True,publication_completed=True,consumption_completed=True,
        publication=published,curve_sha256=curve['curve_sha256'])
    io.persist_record(root,('cycles',CYCLE,'gbp_usd','official_midpoint'),'issued_observation.json',observation,clock=lambda:1207)
    return curve,published


def test_publication_has_real_clock_and_exact_bytes(tmp_path):
    result=io.persist_record(tmp_path,('steps','s000'),'plan.json',{'one':1},clock=clocks(1200,1202))
    assert result['publication_started_epoch']==1200 and result['publication_completed_epoch']==1202
    assert io.read_file(tmp_path,('steps','s000'),'plan.json')==b'{"one":1}'
    assert result['broker_access'] is False and result['can_place_orders'] is False


def test_existing_evidence_never_overwritten(tmp_path):
    io.persist_record(tmp_path,(),'plan.json',{'a':1},clock=lambda:1200)
    with pytest.raises(ValueError,match='already_exists'):
        io.persist_record(tmp_path,(),'plan.json',{'a':1},clock=lambda:1201)
    assert io.read_file(tmp_path,(),'plan.json')==b'{"a":1}'


@pytest.mark.parametrize('name',['../escape.json','token.txt','a/b.json','A.json','x.json/else'])
def test_unsafe_record_names_rejected(tmp_path,name):
    with pytest.raises(ValueError,match='record_name'):
        io.persist_record(tmp_path,(),name,{},clock=lambda:1200)


def test_reversed_publication_clock_keeps_real_evidence(tmp_path):
    with pytest.raises(ValueError,match='clock_reversed'):
        io.persist_record(tmp_path,(),'plan.json',{},clock=clocks(1202,1200))
    assert (tmp_path/'plan.json').read_bytes()==b'{}'


def test_readback_failure_not_a_successful_publication(tmp_path,monkeypatch):
    monkeypatch.setattr(store,'_read',lambda _:b'other')
    with pytest.raises(ValueError,match='readback_mismatch'):
        io.persist_record(tmp_path,(),'plan.json',{},clock=lambda:1200)


@pytest.mark.parametrize('raw',[b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":Infinity}'])
def test_json_identity_errors_are_not_normalized(raw):
    with pytest.raises(ValueError):
        io.decode(raw)


def test_latest_cycles_is_bounded_and_validated(tmp_path):
    root=tmp_path/'cycles';root.mkdir()
    for name in (CYCLE,'cycle_1788934100000000000','ignore','cycle_injected'):
        (root/name).mkdir()
    assert io.recent_cycles(tmp_path,limit=1)==['cycle_1788934100000000000']
    for invalid in (0,21,True):
        with pytest.raises(ValueError,match='scan_bound'):
            io.recent_cycles(tmp_path,invalid)


def test_actual_momentum_source_read_clock_is_separate(tmp_path,monkeypatch):
    raw,receipt,_,args=momentum_fixture.make(monkeypatch)
    parts=('cycles',CYCLE,'gbp_usd')
    io.persist_bytes(tmp_path,parts,'source_raw.json',raw,clock=lambda:momentum_fixture.NOW+.15)
    io.persist_record(tmp_path,parts,'capture_receipt.json',receipt,clock=lambda:momentum_fixture.NOW+.15)
    result=io.read_momentum_source(tmp_path,CYCLE,args['metadata'],clock=clocks(momentum_fixture.NOW+.2,momentum_fixture.NOW+.3))
    assert result['raw_bytes']==raw and result['receipt']==receipt
    assert result['source_consumption']['read_completed_epoch']==momentum_fixture.NOW+.3
    assert result['receipt']['read_completed_epoch']==momentum_fixture.NOW


def test_source_cannot_be_consumed_before_capture_completed(tmp_path,monkeypatch):
    raw,receipt,_,args=momentum_fixture.make(monkeypatch)
    parts=('cycles',CYCLE,'gbp_usd')
    io.persist_bytes(tmp_path,parts,'source_raw.json',raw,clock=lambda:momentum_fixture.NOW+.15)
    io.persist_record(tmp_path,parts,'capture_receipt.json',receipt,clock=lambda:momentum_fixture.NOW+.15)
    with pytest.raises(ValueError,match='read_clock_order'):
        io.read_momentum_source(tmp_path,CYCLE,args['metadata'],clock=lambda:momentum_fixture.NOW)


def test_consumed_anchor_keeps_original_publication_clocks(tmp_path):
    curve,published=anchor(tmp_path)
    before={str(p):p.read_bytes() for p in tmp_path.rglob('*.json')}
    result=io.read_curve_anchor(tmp_path,CYCLE,SOURCES,'pilot_fixture',clock=lambda:1210)
    assert result['curve']==curve and result['publication']==published['publication']
    assert result['consumption']['available_epoch']==1210
    assert result['original_target_epoch']==4800
    assert before=={str(p):p.read_bytes() for p in tmp_path.rglob('*.json')}


@pytest.mark.parametrize('kwargs',[
    {'convention':'ba_derived_midpoint'},{'pair':'EUR_USD'}, {'status':'issued_not_published'}])
def test_nonmatching_original_issues_refused(tmp_path,kwargs):
    anchor(tmp_path,**kwargs)
    with pytest.raises(ValueError,match='original_issue_not_complete'):
        io.read_curve_anchor(tmp_path,CYCLE,SOURCES,'pilot_fixture',clock=lambda:1210)


def test_old_initial_anchor_refused_even_if_curve_unexpired(tmp_path):
    anchor(tmp_path)
    with pytest.raises(ValueError,match='initial_curve_age_limit'):
        io.read_curve_anchor(tmp_path,CYCLE,SOURCES,'pilot_fixture',clock=lambda:1400)


def test_wrong_cohort_refused(tmp_path):
    anchor(tmp_path)
    with pytest.raises(ValueError,match='anchor_identity_mismatch'):
        io.read_curve_anchor(tmp_path,CYCLE,SOURCES,'wrong',clock=lambda:1210)


@pytest.mark.parametrize('parts',[iter(['steps']),['../outside'],'steps',['.'],['..'],['a']*9,[3]])
def test_bad_parts_rejected_before_any_write(tmp_path,parts):
    with pytest.raises(ValueError,match='bounded_path_components'):
        io.persist_record(tmp_path,parts,'plan.json',{},clock=lambda:1200)
    assert list(tmp_path.iterdir())==[]


def test_relative_root_is_normalized_before_write(tmp_path,monkeypatch):
    monkeypatch.chdir(tmp_path)
    result=io.persist_record(Path('newroot'),('steps',),'plan.json',{},clock=lambda:1200)
    assert result['relative_path']=='steps/plan.json'
    assert (tmp_path/'newroot/steps/plan.json').read_bytes()==b'{}'


def test_directory_scan_has_actual_inventory_cap(tmp_path,monkeypatch):
    directory=tmp_path/'cycles';directory.mkdir()
    for i in range(4):
        (directory/('ignored_'+str(i))).mkdir()
    monkeypatch.setattr(io,'MAX_CYCLE_ENTRIES',3)
    with pytest.raises(ValueError,match='directory_scan_limit'):
        io.recent_cycles(tmp_path,limit=1)
