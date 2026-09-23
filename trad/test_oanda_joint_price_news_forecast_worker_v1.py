"""Research worker retry, family isolation and publication-clock boundaries."""
from copy import deepcopy
from concurrent.futures import Future
from pathlib import Path
import hashlib
import json
import socket
import pytest
import oanda_joint_price_news_forecast_study_v1 as worker
from test_oanda_causal_forecast_ledger_joint_news_v1 import (
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
            contract.update(source_bindings=bindings,dependency_versions=deps,numeric_model_source_sha256=bindings['oanda_joint_price_news_models_v1.py'])
            families[family]={'contract':contract,'contract_sha256':worker.digest(contract)}
        pairs[pair]={'pip_size':pip,'families':families}
    registry={'schema_version':worker.REGISTRY_SCHEMA,'registry_id':'joint_price_news_study_v1_20260907',
        'collection_enabled':True,'research_only':True,**{f:False for f in worker.INERT_FLAGS},
        'source_bindings':bindings,'dependency_versions':deps,'pairs':pairs}
    path=tmp_path/'registry.json';path.write_text(json.dumps(registry));return registry,path


def test_registry_binds_single_joint_family(tmp_path,monkeypatch):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    assert worker.load_registry(path)==registry


@pytest.mark.parametrize('case',['family_missing','contract_hash','pair_pip','wrong_family','source_changed','orders','dependencies'])
def test_registry_fails_before_activation(tmp_path,monkeypatch,case):
    registry,path=registry_fixture(tmp_path,monkeypatch);pair=registry['pairs']['EUR_USD'];slot=pair['families']['ridge_price_news_v1']
    if case=='family_missing':pair['families'].pop('ridge_price_news_v1')
    if case=='contract_hash':slot['contract_sha256']='0'*64
    if case=='pair_pip':pair['pip_size']=.01
    if case=='wrong_family':slot['contract']['family']='ridge_return_repaired'
    if case=='source_changed':(tmp_path/'oanda_joint_price_news_models_v1.py').write_text('changed')
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
    capture={'status':'ready','first_observed_epoch':100,'max_bar_close_epoch':90,'news_evidence_epoch':90,'news_expires_epoch':390,
        'family_readiness':{'ridge_price_news_v1':{'ready':True,'reasons':[]}}}
    ready=worker.input_readiness(capture,'ridge_price_news_v1',150)
    assert ready['observed_epoch']==100 and ready['status']=='ready'
    assert worker.input_readiness(capture,'ridge_price_news_v1',991)['status']=='unavailable'
    assert worker.input_readiness(capture,'ridge_return_repaired',150)['status']=='blocked'


def test_early_read_failure_preserves_reason_without_inventing_input_clock():
    capture={'status':'abstain','reasons':['EUR_USD:OSError:source_unreadable']}
    readiness=worker.input_readiness(capture,'ridge_price_news_v1',150)
    assert readiness=={'observed_epoch':None,'status':'unavailable','reason':capture['reasons'][0],'diagnostics':{},
                       'price_bar_close_epoch':None,'news_evidence_epoch':None,'news_expires_epoch':None}


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
    capture={'status':'ready','first_observed_epoch':now-10,'max_bar_close_epoch':now-60,'news_evidence_epoch':now-20,'news_expires_epoch':now+280,'source_capture_sha256':'a'*64,
        'family_readiness':{f:{'ready':f=='ridge_price_news_v1','reasons':[]} for f in families}}
    runner.states={'EUR_USD':{'families':families,'capture':capture,'last_scan':now,'source_signature':None}}
    runner.current={'EUR_USD':{f:{'quote_id':'quote1','market_epoch':now,'available_epoch':now} for f in families}}
    return runner


def test_state_publishes_independently_when_ridge_blocked(tmp_path):
    runner=scheduler_fixture(tmp_path);runner.schedule_work()
    assert runner.active[2][0]=='ridge_price_news_v1'
    assert runner.pool.calls[0][1][0]['first_observed_epoch']==runner.clock.value-10


def test_failed_slot_retries_same_bucket_after_new_quote(tmp_path):
    runner=scheduler_fixture(tmp_path);runner.schedule_work();family='ridge_price_news_v1'
    slot=runner.states['EUR_USD']['families'][family];bucket=slot['last_attempt']['bucket']
    slot['failed_basis']=(runner.states['EUR_USD']['capture']['source_capture_sha256'],'quote1',bucket)
    runner.future=None;runner.clock.advance(31);runner.states['EUR_USD']['last_scan']=runner.clock.value
    runner.schedule_work();assert runner.future is None
    runner.current['EUR_USD'][family]={'quote_id':'quote2','market_epoch':runner.clock.value,'available_epoch':runner.clock.value}
    runner.schedule_work()
    assert slot['last_attempt']['bucket']==bucket and len(slot['ledger'].calls)==2


def test_successful_family_does_not_reissue_same_bucket(tmp_path):
    runner=scheduler_fixture(tmp_path)
    runner.states['EUR_USD']['families']['ridge_price_news_v1']['last_success_bucket']=int(runner.clock.value//900)
    runner.schedule_work();assert runner.future is None


def test_pending_forecast_survives_current_input_gap(opened):
    ledger,clock,contract=opened;issued(ledger,clock);publication=worker.verified_publication(ledger)
    family=contract['family']
    states={contract['instrument']:{'capture':None,'quote_reason':'no_fresh_quote','families':{family:{'ledger':ledger,'publication':publication}}}}
    registry={'pairs':{contract['instrument']:{'pip_size':contract['pip_size']}}}
    summary=worker.build_summary(registry,states,clock.value)
    assert summary['rows'][0]['families'][family]['status']=='forecast'
    clock.value=publication['target_epoch']
    assert worker.build_summary(registry,states,clock.value)['rows'][0]['families'][family]['latest_forecast'] is None


@pytest.mark.parametrize('now,expected',[(300,'ready'),(300.0001,'unavailable'),(251,'ready')])
def test_news_age_limit_has_exact_inclusive_boundary(now,expected):
    capture={'status':'ready','first_observed_epoch':100,'max_bar_close_epoch':90,
        'news_evidence_epoch':0,'news_expires_epoch':400,
        'family_readiness':{'ridge_price_news_v1':{'ready':True,'reasons':[]}}}
    assert worker.input_readiness(capture,'ridge_price_news_v1',now)['status']==expected
    assert worker.input_readiness(capture,'ridge_price_news_v1',now)['news_expires_epoch']==400


@pytest.mark.parametrize('now,expected',[(250,'ready'),(250.0001,'unavailable')])
def test_original_news_window_can_expire_before_snapshot_age_limit(now,expected):
    capture={'status':'ready','first_observed_epoch':100,'max_bar_close_epoch':90,
        'news_evidence_epoch':90,'news_expires_epoch':250,
        'family_readiness':{'ridge_price_news_v1':{'ready':True,'reasons':[]}}}
    assert worker.input_readiness(capture,'ridge_price_news_v1',now)['status']==expected


def test_changed_news_without_new_price_schedules_capture(tmp_path,monkeypatch):
    runner=scheduler_fixture(tmp_path);state=runner.states['EUR_USD']
    state['last_scan']=0;state['source_signature']=('same-price','old-news')
    monkeypatch.setattr(worker,'source_signature',lambda *args:('same-price','new-news'))
    runner.schedule_work()
    assert runner.active==('capture','EUR_USD',('same-price','new-news'))
    assert runner.pool.calls[0][0] is worker.capture_once


def test_publication_preserves_original_news_clocks(opened):
    ledger,clock,_=opened;_,_,capture,_=issued(ledger,clock)
    arm=worker.verified_publication(ledger)['forecasts'][0]
    for key in ('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch',
                'news_available_epoch','news_expires_epoch'):
        assert arm[key]==capture[key]
