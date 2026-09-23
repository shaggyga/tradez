"""Research worker retry, family isolation and publication-clock boundaries."""
from copy import deepcopy
from concurrent.futures import Future
from pathlib import Path
import hashlib
import json
import socket
import pytest
import oanda_pair_local_forecast_study_v3 as worker
from test_oanda_causal_forecast_ledger_pair_v2 import (
    Clock, contract_fixture, capture_validation_seam, opened, issued, completed,
)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args,**kwargs):raise AssertionError('fixture must not access network')
    monkeypatch.setattr(socket.socket,'connect',forbidden)


def registry_fixture(tmp_path,monkeypatch):
    bindings={}
    for name in worker.REQUIRED_SOURCE_BINDINGS:
        raw=('# fixture '+name).encode();(tmp_path/name).write_bytes(raw);bindings[name]=hashlib.sha256(raw).hexdigest()
    monkeypatch.setattr(worker,'ROOT',tmp_path)
    deps={'python':worker.platform.python_version(),**{p:worker.importlib.metadata.version(p) for p in ('numpy','scikit-learn')}}
    pairs={}
    for pair,pip in (('EUR_USD',.0001),('HKD_JPY',.0001)):
        families={}
        for family in worker.FAMILIES:
            contract=contract_fixture(pair,pip,family)
            identity=f'causal_pair_family_v2_20260907.{pair}.{family}.{worker.COHORT_SCOPE}.fixture'
            contract.update(contract_id=identity,cohorts={family:identity})
            contract['evaluation_protocol'].update(contract_id=identity+'.evaluation',cohorts={family:identity})
            contract.update(source_bindings=bindings,dependency_versions=deps,numeric_model_source_sha256=bindings['oanda_pair_local_models_v2.py'])
            families[family]={'contract':contract,'contract_sha256':worker.digest(contract)}
        pairs[pair]={'pip_size':pip,'families':families}
    registry={'schema_version':worker.REGISTRY_SCHEMA,'registry_id':'pair_local_forecast_study_v3_20260913',
        'collection_enabled':True,'research_only':True,**{f:False for f in worker.INERT_FLAGS},
        'source_bindings':bindings,'dependency_versions':deps,'pairs':pairs}
    path=tmp_path/'registry.json';path.write_text(json.dumps(registry));return registry,path


def test_registry_binds_two_independent_families(tmp_path,monkeypatch):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    assert worker.load_registry(path)==registry


@pytest.mark.parametrize('case',['family_missing','contract_hash','pair_pip','wrong_family','source_changed','orders','dependencies'])
def test_registry_fails_before_activation(tmp_path,monkeypatch,case):
    registry,path=registry_fixture(tmp_path,monkeypatch);pair=registry['pairs']['EUR_USD'];slot=pair['families']['probabilistic_state_space']
    if case=='family_missing':pair['families'].pop('ridge_return_repaired')
    if case=='contract_hash':slot['contract_sha256']='0'*64
    if case=='pair_pip':pair['pip_size']=.01
    if case=='wrong_family':slot['contract']['family']='ridge_return_repaired'
    if case=='source_changed':(tmp_path/'oanda_pair_local_models_v2.py').write_text('changed')
    if case=='orders':registry['can_place_orders']=True
    if case=='dependencies':registry['dependency_versions']['numpy']='wrong'
    path.write_text(json.dumps(registry))
    with pytest.raises(ValueError):worker.load_registry(path)


def test_verified_publication_requires_consumption_and_exact_family(opened):
    ledger,clock,contract=opened
    identity,reference,capture,result=issued(ledger,clock)
    publication=worker.verified_publication(ledger)
    assert publication['decision_id']==identity
    assert len(publication['forecasts'])==1
    assert publication['forecasts'][0]['family']==contract['family']
    assert publication['issued_epoch']<=publication['publication_epoch']<=publication['consumption_epoch']
    assert publication['target_epoch']==reference['market_epoch']+3600


@pytest.mark.parametrize('field',['sha','publication_sha','consumed_sha','consumed','published'])
def test_summary_withholds_corrupt_receipt(opened,monkeypatch,field):
    ledger,clock,_=opened;issued(ledger,clock);read=ledger._read
    def changed(query,params=()):
        rows=[dict(row) for row in read(query,params)]
        rows[0][field]='0'*64 if 'sha' in field else clock.value+100
        return rows
    monkeypatch.setattr(ledger,'_read',changed)
    with pytest.raises(ValueError):worker.verified_publication(ledger)


def test_scorecard_uses_selected_family_denominator(opened,tmp_path):
    ledger,clock,contract=opened;completed(ledger,clock)
    report=worker.write_scorecard(ledger,tmp_path/'score.json')
    assert report['family']==contract['family']
    assert report['coverage']['scored_decisions']==1
    assert report['collection_counts']['outcomes']==1
    assert 'paired_scored_decisions' not in report['coverage']


def test_cached_readiness_keeps_real_observation_and_expires():
    capture={'status':'ready','first_observed_epoch':100,'max_bar_close_epoch':90,
        'family_readiness':{'probabilistic_state_space':{'ready':True,'reasons':[]}}}
    ready=worker.input_readiness(capture,'probabilistic_state_space',150)
    assert ready['observed_epoch']==100 and ready['status']=='ready'
    assert worker.input_readiness(capture,'probabilistic_state_space',991)['status']=='unavailable'
    assert worker.input_readiness(capture,'ridge_return_repaired',150)['status']=='blocked'


def test_early_read_failure_preserves_reason_without_inventing_input_clock():
    capture={'status':'abstain','reasons':['EUR_USD:OSError:source_unreadable']}
    readiness=worker.input_readiness(capture,'probabilistic_state_space',150)
    assert readiness=={'observed_epoch':None,'status':'unavailable','reason':capture['reasons'][0],'diagnostics':{}}


class DummyLedger:
    def __init__(self):self.calls=[]
    def begin_attempt(self,bucket,quote):self.calls.append((bucket,quote));return 'attempt-'+str(len(self.calls))


class DummyPool:
    def __init__(self):self.calls=[]
    def submit(self,fn,*args):self.calls.append((fn,args));return Future()


def scheduler_fixture(tmp_path):
    runner=worker.PairRunner.__new__(worker.PairRunner);runner.clock=Clock();now=runner.clock.value
    runner.future=None;runner.active=None;runner.queue=['EUR_USD'];runner.cursor=0;runner.candle_root=tmp_path
    runner.pool=DummyPool();runner.registry={'pairs':{'EUR_USD':{'pip_size':.0001}}}
    families={f:{'ledger':DummyLedger(),'last_success_bucket':-1,'last_try':0,'failed_basis':None} for f in sorted(worker.FAMILIES)}
    capture={'status':'ready','first_observed_epoch':now-10,'max_bar_close_epoch':now-60,'source_capture_sha256':'a'*64,
        'family_readiness':{f:{'ready':f=='probabilistic_state_space','reasons':[]} for f in families}}
    runner.states={'EUR_USD':{'families':families,'capture':capture,'last_scan':now,'source_signature':None}}
    runner.current={'EUR_USD':{f:{'quote_id':'quote1','market_epoch':now,'available_epoch':now} for f in families}}
    return runner


def test_state_publishes_independently_when_ridge_blocked(tmp_path):
    runner=scheduler_fixture(tmp_path);runner.schedule_work()
    assert runner.active[2][0]=='probabilistic_state_space'
    assert len(runner.states['EUR_USD']['families']['ridge_return_repaired']['ledger'].calls)==0
    assert runner.pool.calls[0][1][0]['first_observed_epoch']==runner.clock.value-10


def test_failed_slot_retries_same_bucket_after_new_quote(tmp_path):
    runner=scheduler_fixture(tmp_path);runner.schedule_work();family='probabilistic_state_space'
    slot=runner.states['EUR_USD']['families'][family];bucket=slot['last_attempt']['bucket']
    slot['failed_basis']=(runner.states['EUR_USD']['capture']['source_capture_sha256'],'quote1',bucket)
    runner.future=None;runner.clock.advance(31);runner.states['EUR_USD']['last_scan']=runner.clock.value
    runner.schedule_work();assert runner.future is None
    runner.current['EUR_USD'][family]={'quote_id':'quote2','market_epoch':runner.clock.value,'available_epoch':runner.clock.value}
    runner.schedule_work()
    assert slot['last_attempt']['bucket']==bucket and len(slot['ledger'].calls)==2


def test_successful_family_does_not_reissue_same_bucket(tmp_path):
    runner=scheduler_fixture(tmp_path)
    runner.states['EUR_USD']['families']['probabilistic_state_space']['last_success_bucket']=int(runner.clock.value//900)
    runner.schedule_work();assert runner.future is None


def test_frequent_candle_changes_cannot_starve_any_ready_pair_or_family(tmp_path):
    runner=scheduler_fixture(tmp_path)
    runner.queue=['EUR_USD','USD_JPY']
    runner.registry['pairs']['USD_JPY']={'pip_size':.01}
    runner.states['USD_JPY']=deepcopy(runner.states['EUR_USD'])
    runner.current['USD_JPY']=deepcopy(runner.current['EUR_USD'])
    for pair in runner.queue:
        (tmp_path/f'{pair}_M1.csv').write_text('a')
        for ready in runner.states[pair]['capture']['family_readiness'].values():ready['ready']=True
    fits=[];kinds=[]
    for step in range(8):
        runner.clock.advance(31)
        for pair,state in runner.states.items():
            # Sources change before every scheduler visit and both families
            # retain a causally observed, still usable capture.
            (tmp_path/f'{pair}_M1.csv').write_text('a'*(step+2))
            for quote in runner.current[pair].values():quote['market_epoch']=runner.clock.value
        runner.schedule_work()
        if runner.active is None:continue
        kind,pair,detail=runner.active;kinds.append(kind)
        if kind=='fit':
            family=detail[0];fits.append((pair,family))
            runner.states[pair]['families'][family]['last_success_bucket']=int(runner.clock.value//900)
        runner.future=None;runner.active=None
    assert len(fits)==4 and len(set(fits))==4
    assert kinds[:7]==['fit','capture','fit','capture','fit','capture','fit']
    # All families have durable successes for this cadence: further candle
    # changes can trigger captures, but cannot generate duplicate issuance.
    for _ in range(4):
        runner.schedule_work()
        assert runner.active is None or runner.active[0]=='capture'
        runner.future=None;runner.active=None


def test_retained_ledger_cadence_claim_prevents_duplicate_after_restart(tmp_path):
    runner=scheduler_fixture(tmp_path);slot=runner.states['EUR_USD']['families']['probabilistic_state_space']
    slot['ledger'].begin_attempt=lambda bucket,quote:None
    runner.schedule_work()
    assert runner.future is None
    assert slot['last_success_bucket']==int(runner.clock.value//900)
    assert not runner.pool.calls


def test_pending_forecast_survives_current_input_gap(opened):
    ledger,clock,contract=opened;issued(ledger,clock);publication=worker.verified_publication(ledger)
    family=contract['family'];other=next(f for f in worker.FAMILIES if f!=family)
    other_contract=contract_fixture(family=other)
    class Other:
        contract=other_contract;contract_hash=worker.digest(other_contract);activated_epoch=ledger.activated_epoch
        def counts(self):return {}
    states={contract['instrument']:{'capture':None,'quote_reason':'no_fresh_quote','families':{
        family:{'ledger':ledger,'publication':publication},other:{'ledger':Other(),'publication':None}}}}
    registry={'pairs':{contract['instrument']:{'pip_size':contract['pip_size']}}}
    summary=worker.build_summary(registry,states,clock.value)
    assert summary['rows'][0]['families'][family]['status']=='forecast'
    assert summary['rows'][0]['families'][other]['status']=='unavailable'
    clock.value=publication['target_epoch']
    assert worker.build_summary(registry,states,clock.value)['rows'][0]['families'][family]['latest_forecast'] is None
