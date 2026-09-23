"""Research worker retry, family isolation and publication-clock boundaries."""
from copy import deepcopy
from concurrent.futures import Future
from pathlib import Path
import hashlib
import json
import socket
import pytest
import oanda_joint_price_news_forecast_study_v3 as worker
from test_oanda_causal_forecast_ledger_joint_news_v2 import (
    Clock, contract_fixture, capture_validation_seam, opened, issued, completed, quote,
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
            identity=f'joint_price_news_v1_20260907.{pair}.{family}.{worker.COHORT_SCOPE}.unit'
            contract.update(contract_id=identity,cohorts={family:identity},
                model_version='sha256:'+bindings['oanda_joint_price_news_models_v1.py'],
                feature_version='sha256:'+bindings['oanda_causal_forecast_inputs_joint_news_v2.py'])
            contract['evaluation_protocol'].update(contract_id=identity+'.evaluation',cohorts={family:identity},
                model_version=contract['model_version'],feature_version=contract['feature_version'])
            families[family]={'contract':contract,'contract_sha256':worker.digest(contract)}
        pairs[pair]={'pip_size':pip,'families':families}
    registry={'schema_version':worker.REGISTRY_SCHEMA,'registry_id':'joint_price_news_study_v3_20260908',
        'collection_enabled':True,'research_only':True,**{f:False for f in worker.INERT_FLAGS},
        'source_bindings':bindings,'dependency_versions':deps,'pairs':pairs}
    path=tmp_path/'registry.json';path.write_text(json.dumps(registry));return registry,path


def test_registry_binds_single_joint_family(tmp_path,monkeypatch):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    assert worker.load_registry(path)==registry


@pytest.mark.parametrize('flag',list(worker.INERT_FLAGS))
@pytest.mark.parametrize('value',[True,0,'false',None])
def test_every_registry_authority_flag_is_exact_false(tmp_path,monkeypatch,flag,value):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    registry[flag]=value;path.write_text(json.dumps(registry))
    with pytest.raises(ValueError,match='inert_registry_required'):worker.load_registry(path)


@pytest.mark.parametrize('case',['prior_cohort','prior_ledger_schema','feature_source','model_source','source_missing'])
def test_new_registry_cannot_relabel_previous_registration(tmp_path,monkeypatch,case):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    slot=registry['pairs']['EUR_USD']['families']['ridge_price_news_v1'];c=slot['contract']
    if case=='prior_cohort':
        old='joint_price_news_v1_20260907.EUR_USD.ridge_price_news_v1.prospective_scheduler_v2'
        c.update(contract_id=old,cohorts={'ridge_price_news_v1':old})
        c['evaluation_protocol'].update(contract_id=old+'.evaluation',cohorts=c['cohorts'])
    if case=='prior_ledger_schema':c['schema_version']='causal_joint_price_news_ledger_v1_20260907'
    if case in ('feature_source','model_source'):
        key='feature_version' if case=='feature_source' else 'model_version'
        c[key]='sha256:'+'b'*64;c['evaluation_protocol'][key]=c[key]
    if case=='source_missing':registry['source_bindings'].pop('oanda_news_collector_contract.py')
    slot['contract_sha256']=worker.digest(c);path.write_text(json.dumps(registry))
    with pytest.raises(ValueError):worker.load_registry(path)


def test_deployment_activation_creates_only_empty_new_ledgers_at_actual_clock(tmp_path,monkeypatch):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    clock=Clock();clock.advance(500)
    study=tmp_path/'new_parent'/'joint_price_news_study_v3'
    receipt=worker.activate_fresh_study(path,study=study,clock=clock)
    assert receipt['status']=='activated_empty' and receipt['worker_started'] is False
    assert len(receipt['ledgers'])==2 and receipt['registry_sha256']==worker.digest(registry)
    assert receipt['activation_started_epoch']<=min(r['activated_epoch'] for r in receipt['ledgers'])
    assert max(r['activated_epoch'] for r in receipt['ledgers'])<=receipt['activation_completed_epoch']
    assert all(not any(r['counts'].values()) for r in receipt['ledgers'])
    assert not (study/'summary.json').exists() and not (study/'heartbeat.json').exists()
    before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in study.rglob('*') if p.is_file()}
    with pytest.raises(ValueError,match='wholly_absent'):worker.activate_fresh_study(path,study=study,clock=clock)
    assert {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in before}==before


def test_activation_never_imports_prior_forecasts_or_opens_an_old_study(tmp_path,monkeypatch):
    registry,path=registry_fixture(tmp_path,monkeypatch)
    old=tmp_path/'joint_price_news_study_v2';old.mkdir();sentinel=old/'study.sqlite';sentinel.write_bytes(b'original retained records')
    with pytest.raises(ValueError,match='wholly_absent'):worker.activate_fresh_study(path,study=old,clock=Clock())
    new=tmp_path/'joint_price_news_study_v3'
    worker.activate_fresh_study(path,study=new,clock=Clock())
    assert sentinel.read_bytes()==b'original retained records'
    assert not any(sum(json.loads((new/'activation_receipt.json').read_text())['ledgers'][i]['counts'].values()) for i in range(2))


def test_source_failure_prevents_any_activation_directory(tmp_path,monkeypatch):
    _,path=registry_fixture(tmp_path,monkeypatch)
    (tmp_path/'oanda_news_topic_identity_reconciliation_v1.py').write_text('changed after preparation')
    study=tmp_path/'joint_price_news_study_v3'
    with pytest.raises(ValueError,match='source_binding_mismatch'):worker.activate_fresh_study(path,study=study,clock=Clock())
    assert not study.exists()


def test_partial_activation_is_retained_and_not_automatically_retried(tmp_path,monkeypatch):
    _,path=registry_fixture(tmp_path,monkeypatch);original=worker.CausalForecastLedger;calls=[]
    def fail_second(*args,**kwargs):
        calls.append(args)
        if len(calls)==2:raise OSError('synthetic disk failure')
        return original(*args,**kwargs)
    monkeypatch.setattr(worker,'CausalForecastLedger',fail_second)
    study=tmp_path/'joint_price_news_study_v3'
    with pytest.raises(OSError,match='synthetic'):worker.activate_fresh_study(path,study=study,clock=Clock())
    failure=json.loads((study/'activation_failed.json').read_text())
    assert failure['status']=='partial_activation_requires_explicit_review' and len(failure['completed_ledgers'])==1
    assert not (study/'activation_receipt.json').exists()
    with pytest.raises(ValueError,match='wholly_absent'):worker.activate_fresh_study(path,study=study,clock=Clock())


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


@pytest.mark.parametrize('missing',['entry','target'])
def test_scorecard_retains_missing_exact_endpoint_exclusions(opened,tmp_path,missing):
    ledger,clock,_=opened
    identity,reference,_,_=issued(ledger,clock)
    original_target=reference['market_epoch']+3600
    if missing=='entry':
        clock.advance(61);ledger.settle()
        clock.value=original_target;quote(ledger,clock);ledger.settle()
        reason='missing_later_entry_before_deadline'
    else:
        clock.advance();quote(ledger,clock);ledger.settle()
        clock.value=original_target+61;ledger.settle()
        reason='missing_quote_at_original_target'
    report=worker.write_scorecard(ledger,tmp_path/'excluded.json')
    assert report['coverage']['scored_decisions']==0
    assert report['collection_counts']['outcomes']==0
    assert report['collection_counts']['exclusions']==1
    row=ledger._read('SELECT id,reason FROM exclusions')[0]
    assert row['id']==identity and row['reason']==reason
    assert ledger._read('SELECT target FROM forecasts')[0]['target']==original_target


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
    runner.future=None;runner.active=None;runner.queue=['EUR_USD'];runner.capture_cursor=0;runner.fit_cursor=0;runner.candle_root=tmp_path;runner.study=tmp_path/'study'
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
    runner.prefer_fit=False
    state['last_scan']=0;state['source_signature']=('same-price','old-news')
    monkeypatch.setattr(worker,'source_signature',lambda *args:('same-price','new-news'))
    runner.schedule_work()
    assert runner.active==('capture','EUR_USD',('same-price','new-news'))
    assert runner.pool.calls[0][0] is worker.capture_once


def test_continuous_68_pair_source_updates_cannot_starve_fitting(tmp_path,monkeypatch):
    runner=scheduler_fixture(tmp_path);original=runner.states['EUR_USD'];quote=runner.current['EUR_USD']
    # Use actual registered instruments, but no network or live files.
    registry=json.loads(Path(__file__).with_name('config').joinpath('pair_local_forecast_study_v2_20260907.json').read_bytes())
    runner.queue=sorted(registry['pairs']);runner.registry={'pairs':registry['pairs']};runner.prefer_fit=False
    runner.states={pair:deepcopy(original) for pair in runner.queue}
    runner.current={pair:deepcopy(quote) for pair in runner.queue}
    for state in runner.states.values():state['last_scan']=0
    monkeypatch.setattr(worker,'source_signature',lambda *args:('always-changing',runner.clock.value))
    selected=[]
    for _ in range(136):
        runner.schedule_work();assert runner.active is not None
        kind,pair,detail=runner.active;selected.append((kind,pair))
        if kind=='capture':runner.states[pair]['source_signature']=detail
        else:runner.states[pair]['families']['ridge_price_news_v1']['last_success_bucket']=int(runner.clock.value//900)
        runner.future=None;runner.active=None
        # Every full68pair sweep now far exceeds the30second rescan period.
        runner.clock.advance(1)
        for quotes in runner.current.values():
            for row in quotes.values():row['market_epoch']=runner.clock.value;row['available_epoch']=runner.clock.value
    assert len({pair for kind,pair in selected if kind=='fit'})==68
    assert all(selected[i][0]!=selected[i+1][0] for i in range(135))


def test_ready_cached_input_fits_before_repeated_refresh(tmp_path,monkeypatch):
    runner=scheduler_fixture(tmp_path);runner.states['EUR_USD']['last_scan']=0
    monkeypatch.setattr(worker,'source_signature',lambda *args:('changed',runner.clock.value))
    runner.schedule_work()
    assert runner.active[0]=='fit'


def test_repeated_failed_fits_cannot_lock_work_classes_to_even_odd_pairs(tmp_path,monkeypatch):
    from collections import Counter
    runner=scheduler_fixture(tmp_path);original=runner.states['EUR_USD'];quote=runner.current['EUR_USD']
    registry=json.loads(Path(__file__).with_name('config').joinpath('pair_local_forecast_study_v2_20260907.json').read_bytes())
    runner.queue=sorted(registry['pairs']);runner.registry={'pairs':registry['pairs']};runner.prefer_fit=False
    runner.states={pair:deepcopy(original) for pair in runner.queue}
    runner.current={pair:deepcopy(quote) for pair in runner.queue}
    monkeypatch.setattr(worker,'source_signature',lambda *args:('always-changing',runner.clock.value))
    fitted=Counter();captured=Counter()
    for index in range(408):
        runner.clock.advance(31)
        for state in runner.states.values():
            state['capture'].update(first_observed_epoch=runner.clock.value-10,max_bar_close_epoch=runner.clock.value-60,
                news_evidence_epoch=runner.clock.value-20,news_expires_epoch=runner.clock.value+280)
        for quotes in runner.current.values():
            for row in quotes.values():row.update(market_epoch=runner.clock.value,available_epoch=runner.clock.value,quote_id='quote'+str(index))
        runner.schedule_work();kind,pair,detail=runner.active
        if kind=='capture':captured[pair]+=1;runner.states[pair]['source_signature']=detail
        else:
            fitted[pair]+=1
            runner.states[pair]['families']['ridge_price_news_v1']['failed_basis']=detail[3]
        runner.future=None;runner.active=None
    assert set(fitted)==set(captured)==set(runner.queue)
    assert set(fitted.values())==set(captured.values())=={3}


def test_capture_failure_keeps_source_reason_without_fake_availability(tmp_path,monkeypatch):
    import oanda_causal_forecast_inputs_joint_news_v2 as adapter
    def unavailable(*args,**kwargs):raise ValueError('current_news_stale_or_future')
    monkeypatch.setattr(adapter,'capture_news_inputs',unavailable)
    capture=worker.capture_once(tmp_path,'EUR_USD',.0001,tmp_path/'own')
    assert capture['status']=='abstain' and 'first_observed_epoch' not in capture
    assert capture['reasons']==['ValueError:current_news_stale_or_future']


def test_new_worker_rejects_news_capture_from_different_store(tmp_path,monkeypatch):
    import oanda_causal_forecast_inputs_joint_news_v2 as adapter
    monkeypatch.setattr(adapter,'capture_news_inputs',lambda *args,**kwargs:{'news_capture_path':str(tmp_path/'old'/'a.json.gz')})
    capture=worker.capture_once(tmp_path,'EUR_USD',.0001,tmp_path/'own')
    assert capture['reasons']==['ValueError:shared_news_storage_root_mismatch']


def test_publication_preserves_original_news_clocks(opened):
    ledger,clock,_=opened;_,_,capture,_=issued(ledger,clock)
    arm=worker.verified_publication(ledger)['forecasts'][0]
    for key in ('news_capture_sha256','news_evidence_epoch','news_generated_epoch','news_first_observed_epoch',
                'news_available_epoch','news_expires_epoch'):
        assert arm[key]==capture[key]


def test_worker_cannot_create_lock_or_directory_before_complete_activation(tmp_path,monkeypatch):
    _,path=registry_fixture(tmp_path,monkeypatch)
    study=tmp_path/'joint_price_news_study_v3';monkeypatch.setattr(worker,'STUDY',study)
    with pytest.raises(ValueError,match='complete_prior_v3_activation_required'):worker.run(path,once=True)
    assert not study.exists()


def test_read_only_activation_preflight_accepts_actual_empty_contracts(tmp_path,monkeypatch):
    registry,path=registry_fixture(tmp_path,monkeypatch);clock=Clock()
    study=tmp_path/'joint_price_news_study_v3'
    worker.activate_fresh_study(path,study=study,clock=clock)
    before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in study.rglob('*') if p.is_file()}
    assert worker.verify_activated_study(registry,study,clock=clock)
    assert {p:hashlib.sha256(p.read_bytes()).hexdigest() for p in before}==before


def test_missing_source_signature_keeps_explicit_blocker_and_no_old_capture(tmp_path,monkeypatch):
    runner=scheduler_fixture(tmp_path);runner.prefer_fit=False
    state=runner.states['EUR_USD'];state['last_scan']=0
    def missing(*args):raise FileNotFoundError('repaired news snapshot missing')
    monkeypatch.setattr(worker,'source_signature',missing)
    runner.schedule_work()
    assert runner.future is None and state['capture'] is None
    assert 'repaired news snapshot missing' in state['capture_error']
    assert state['source_signature'] is None
