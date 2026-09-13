from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

import pytest

import oanda_joint_v3_ledger_status_observer_v2 as observer

ROOT=Path(__file__).resolve().parent
EVIDENCE=ROOT.parent/'overnight_curve_buildout_20260909/coherent_reader_design/ledger_observer_v1'
FIXTURE=EVIDENCE/'RETAINED_EURUSD_LEDGER_ROWS_PRIVATE_20260909.json'
REGISTRY_PATH=ROOT/'config/joint_price_news_study_v3_20260908.json'
FAMILY=observer.FAMILY


def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);raw=observer.encoded(value);path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def create_db(path,rows):
    path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path)
    db.executescript('''
        CREATE TABLE contract(id INTEGER PRIMARY KEY,sha TEXT,payload TEXT);
        CREATE TABLE activation(id INTEGER PRIMARY KEY,epoch REAL,contract_sha TEXT);
        CREATE TABLE forecasts(id TEXT PRIMARY KEY,bucket INTEGER UNIQUE,attempt_id TEXT,reference REAL,target REAL,sha TEXT,payload TEXT);
        CREATE TABLE attempts(id TEXT PRIMARY KEY,bucket INTEGER,sequence INTEGER,epoch REAL,reference_id TEXT);
        CREATE TABLE quotes(id TEXT PRIMARY KEY,market REAL,available REAL,payload TEXT);
        CREATE TABLE publication(id TEXT PRIMARY KEY,epoch REAL,forecast_sha TEXT);
        CREATE TABLE consumption(id TEXT PRIMARY KEY,epoch REAL,forecast_sha TEXT,publication_sha TEXT);
        CREATE TABLE diagnostics(attempt_id TEXT PRIMARY KEY,bucket INTEGER,epoch REAL,payload TEXT);
    ''')
    for table,items in rows.items():
        for item in items:
            keys=list(item)
            db.execute('INSERT INTO '+table+'('+','.join('"'+key+'"' for key in keys)+') VALUES('+','.join('?' for _ in keys)+')',tuple(item[key] for key in keys))
    db.commit();db.close()


@pytest.fixture
def world(tmp_path):
    raw=FIXTURE.read_bytes();assert hashlib.sha256(raw).hexdigest()=='f3c3aa748c2c4b2b98dd9014eff9a8a282a62a309e0e00d13dcd35a520eac525'
    fixture=json.loads(raw);registry=json.loads(REGISTRY_PATH.read_bytes())
    assert hashlib.sha256(REGISTRY_PATH.read_bytes()).hexdigest()==observer.ORIGIN_REGISTRY_SHA256
    now=fixture['read_completed_epoch']+1
    study=tmp_path/'joint_price_news_study_v3';study.mkdir()
    path=study/'pairs/EUR_USD'/FAMILY/'study.sqlite';create_db(path,fixture['tables'])
    active=fixture['tables']['activation'][0]['epoch']
    activations=[]
    for pair,item in registry['pairs'].items():
        activations.append(dict(instrument=pair,family=FAMILY,path=str(study/'pairs'/pair/FAMILY/'study.sqlite'),
            contract_sha256=item['families'][FAMILY]['contract_sha256'],activated_epoch=active,counts={}))
    activation=dict(schema_version='joint_price_news_v3_activation_receipt_20260908',status='activated_empty',
        registry_sha256=observer.ORIGIN_REGISTRY_SHA256,registry_file_sha256=observer.ORIGIN_REGISTRY_SHA256,
        source_bindings=registry['source_bindings'],study_root=str(study),activation_started_epoch=active-1,
        activation_completed_epoch=active+1,ledgers=activations,worker_started=False,**observer.AUTHORITY)
    activation_sha=save(study/'activation_receipt.json',activation)
    old=dict(schema_version='joint_price_news_forecast_summary_v3_20260908',registry_sha256=observer.ORIGIN_REGISTRY_SHA256,
        generated_epoch=now-1,rows=[],**observer.AUTHORITY)
    old['payload_sha256']=observer.digest(old);save(study/'summary.json',old)
    hb=dict(schema_version='joint_price_news_forecast_heartbeat_v3_20260908',registry_sha256=observer.ORIGIN_REGISTRY_SHA256,
        generated_epoch=now,summary_sha256='f'*64,errors=7,heartbeat_publication_errors=0,**observer.AUTHORITY)
    save(study/'heartbeat.json',hb)
    sources,_=observer._closure(registry,observer.own_source_bindings(),lambda:now)
    rules=observer._frozen_rules(sources)
    return dict(study=study,path=path,registry=registry,fixture=fixture,now=now,active=active,
        activation=activation,activation_sha=activation_sha,rules=rules)


def run(world,**kwargs):
    return observer.observe_joint_v3(REGISTRY_PATH,world['study'],
        expected_observer_source_bindings=observer.own_source_bindings(),expected_activation_sha256=world['activation_sha'],
        clock=kwargs.pop('clock',lambda:world['now']),**kwargs)


def pair(world,**kwargs):
    return observer._pair(world['path'],world['registry']['pairs']['EUR_USD']['families'][FAMILY],
        {'activated_epoch':world['active']},world['rules'],kwargs.get('clock',lambda:world['now']),
        time.monotonic,time.monotonic()+2)


def slot(report,pair='EUR_USD'):
    return next(row['families'][FAMILY] for row in report['summary']['rows'] if row['instrument']==pair)


def update(world,sql,params=()):
    db=sqlite3.connect(world['path']);db.execute(sql,params);db.commit();db.close()


def test_real_retained_forecast_survives_upstream_mismatch_with_original_identity_and_clocks(world):
    before=world['path'].read_bytes()
    result=run(world)
    selected=slot(result);original=json.loads(world['fixture']['tables']['forecasts'][0]['payload'])
    forecast=selected['latest_forecast']
    assert result['registered_pairs']==68 and result['attempted_pairs']==68 and result['verified_ledger_pairs']==1
    assert result['current_forecast_pairs']==1 and len(result['failures'])==67
    assert set(result['failures'].values())=={'observer_missing_ledger'}
    assert result['original_producer_envelope']['status']=='generation_mismatch'
    assert result['original_producer_envelope']['producer_reported_errors']==7
    assert result['old_heartbeat_validated_for_forecasts'] is False
    assert result['current_input_readiness_observed'] is False
    assert forecast['decision_id']==original['decision_id']
    for key in ('reference_epoch','target_epoch'):
        assert forecast[key]==original[key]
    assert forecast['issued_epoch']==original['forecasts'][0]['issued_epoch']
    assert forecast['consumption_epoch']==world['fixture']['tables']['consumption'][0]['epoch']
    assert forecast['publication_epoch']==world['fixture']['tables']['publication'][0]['epoch']
    assert forecast['forecasts'][0]['news_expires_epoch']==original['forecasts'][0]['news_expires_epoch']
    assert selected['current_readiness']['reason']=='current_inputs_not_observed'
    assert forecast['publication_verified_epoch']==world['now']
    assert world['path'].read_bytes()==before
    assert observer.digest({k:v for k,v in result.items() if k!='payload_sha256'})==result['payload_sha256']
    assert result['summary']['registry_sha256']==result['observer_spec_sha256']!=observer.ORIGIN_REGISTRY_SHA256
    assert selected['origin_registry_sha256']==observer.ORIGIN_REGISTRY_SHA256


def test_complete_empty_ledgers_are_not_forecasts_and_all_pair_inventory_is_explicit(world):
    for instrument,item in world['registry']['pairs'].items():
        if instrument=='EUR_USD':continue
        spec=item['families'][FAMILY]
        rows={'contract':[dict(id=1,sha=spec['contract_sha256'],payload=observer.encoded(spec['contract']).decode())],
            'activation':[dict(id=1,epoch=world['active'],contract_sha=spec['contract_sha256'])]}
        create_db(world['study']/'pairs'/instrument/FAMILY/'study.sqlite',rows)
    result=run(world,time_budget_sec=8)
    assert result['verified_ledger_pairs']==68 and result['failures']=={}
    assert result['current_forecast_pairs']==1
    assert slot(result,'GBP_USD')['reason']=='no_verified_published_forecast'


@pytest.mark.parametrize('table',['publication','consumption'])
def test_missing_receipt_never_becomes_verified_forecast(world,table):
    update(world,'DELETE FROM '+table)
    result=run(world)
    assert slot(result)['latest_forecast'] is None
    assert slot(result)['latest_forecast_pending_verified_publication'] is True
    assert result['current_forecast_pairs']==0


@pytest.mark.parametrize('case',['contract','activation','forecast_hash','publication_hash','consumer_hash','reference_quote','future_publication','future_consumption'])
def test_changed_original_rows_are_refused_without_fallback_to_summary(world,case):
    if case=='contract':update(world,"UPDATE contract SET sha=?",('b'*64,))
    elif case=='activation':update(world,'UPDATE activation SET epoch=epoch+1')
    elif case=='forecast_hash':update(world,'UPDATE forecasts SET sha=?',('b'*64,))
    elif case=='publication_hash':update(world,'UPDATE publication SET forecast_sha=?',('b'*64,))
    elif case=='consumer_hash':update(world,'UPDATE consumption SET publication_sha=?',('b'*64,))
    elif case=='reference_quote':
        quote=json.loads(world['fixture']['tables']['quotes'][0]['payload']);quote['instrument']='GBP_USD'
        update(world,'UPDATE quotes SET payload=?',(observer.encoded(quote).decode(),))
    elif case=='future_publication':update(world,'UPDATE publication SET epoch=?',(world['now']+1,))
    else:update(world,'UPDATE consumption SET epoch=?',(world['now']+1,))
    result=run(world)
    assert slot(result)['latest_forecast'] is None and 'EUR_USD' in result['failures']
    assert result['current_forecast_pairs']==0


def test_original_news_expiry_does_not_reset_original_h1_but_elapsed_h1_is_unavailable(world):
    original=json.loads(world['fixture']['tables']['forecasts'][0]['payload'])
    assert world['now']>original['forecasts'][0]['news_expires_epoch']
    assert pair(world)['status']=='forecast'
    end=original['target_epoch']
    result=pair(world,clock=lambda:end)
    assert result['status']=='unavailable' and result['reason']=='original_h1_target_elapsed'
    assert result['ledger_observation']['latest_original_target_epoch']==end


def test_pending_latest_forecast_does_not_relabel_earlier_verified_current_forecast(world):
    original=world['fixture']['tables']['forecasts'][0]
    update(world,'INSERT INTO forecasts(id,bucket,attempt_id,reference,target,sha,payload) VALUES(?,?,?,?,?,?,?)',
        ('pending_fixture',original['bucket']+1,'pending_attempt',original['reference']+1,original['target']+1,'b'*64,'{}'))
    result=pair(world)
    assert result['latest_forecast']['decision_id']==original['id']
    assert result['latest_forecast_pending_verified_publication'] is True


@pytest.mark.parametrize('case',['bucket','sequence','attempt_epoch','quote_tradeable','quote_id','quote_market','quote_available','source_read_future'])
def test_original_ledger_generated_identities_and_reference_clocks_are_reverified(world,case):
    if case=='bucket':update(world,'UPDATE forecasts SET bucket=bucket+1')
    elif case=='sequence':update(world,'UPDATE attempts SET sequence=sequence+1')
    elif case=='attempt_epoch':update(world,'UPDATE attempts SET epoch=epoch+1')
    elif case=='quote_market':update(world,'UPDATE quotes SET market=market+1')
    elif case=='quote_available':update(world,'UPDATE quotes SET available=available+1')
    else:
        quote=json.loads(world['fixture']['tables']['quotes'][0]['payload'])
        if case=='quote_tradeable':quote['tradeable']=False
        elif case=='quote_id':quote['quote_id']='f'*64
        else:quote['source_read_epoch']=world['now']+100
        update(world,'UPDATE quotes SET payload=?',(observer.encoded(quote).decode(),))
    result=run(world)
    assert slot(result)['latest_forecast'] is None
    assert result['current_forecast_pairs']==0 and 'EUR_USD' in result['failures']


def test_budget_exhaustion_lists_all_68_pairs_without_claiming_unvisited_rows_checked(world):
    value=[0.0]
    def monotonic():value[0]+=.2;return value[0]
    result=run(world,monotonic=monotonic,time_budget_sec=8)
    assert result['status']=='partial_observation'
    assert len(result['summary']['rows'])==68 and result['unvisited_pairs']>0
    assert 'observer_budget_exhausted' in result['failures'].values()
    assert result['attempted_pairs']+result['unvisited_pairs']==68


def test_oversized_sqlite_payload_is_bounded_before_python_materialization(world):
    update(world,'UPDATE forecasts SET payload=?',('x'*(observer.MAX_RECORD_BYTES+1),))
    result=run(world)
    assert result['failures']['EUR_USD']=='observer_sqlite_time_budget_or_unavailable'
    assert slot(result)['latest_forecast'] is None


def test_malformed_diagnostic_is_isolated_as_named_pair_failure(world):
    original=world['fixture']['tables']['attempts'][0]
    update(world,'INSERT INTO diagnostics VALUES(?,?,?,?)',
        (original['id'],original['bucket'],world['now']-1,'[]'))
    result=run(world)
    assert result['failures']['EUR_USD']=='observer_diagnostic_shape'
    assert len(result['summary']['rows'])==68


def test_immutable_source_activation_and_registry_bindings_fail_closed(world,tmp_path):
    with pytest.raises(observer.ObserverError,match='activation_receipt_hash'):
        observer.observe_joint_v3(REGISTRY_PATH,world['study'],expected_observer_source_bindings=observer.own_source_bindings(),
            expected_activation_sha256='b'*64,clock=lambda:world['now'])
    wrong=observer.own_source_bindings();wrong[observer.OWN_FILES[0]]='b'*64
    with pytest.raises(observer.ObserverError,match='loaded_source_binding'):
        observer.observe_joint_v3(REGISTRY_PATH,world['study'],expected_observer_source_bindings=wrong,
            expected_activation_sha256=world['activation_sha'],clock=lambda:world['now'])
    changed=tmp_path/'registry.json';changed.write_bytes(REGISTRY_PATH.read_bytes()+b'\n')
    with pytest.raises(observer.ObserverError,match='original_registry_hash'):
        observer.observe_joint_v3(changed,world['study'],expected_observer_source_bindings=observer.own_source_bindings(),
            expected_activation_sha256=world['activation_sha'],clock=lambda:world['now'])
    registry=deepcopy(world['registry']);registry['source_bindings']['oanda_joint_price_news_models_v1.py']='b'*64
    with pytest.raises(observer.ObserverError,match='source_binding_mismatch'):
        observer._closure(registry,observer.own_source_bindings(),lambda:world['now'])


def test_duplicate_activation_identity_rejected_even_with_caller_retained_hash(world):
    value=deepcopy(world['activation']);value['ledgers'][1]=deepcopy(value['ledgers'][0])
    world['activation_sha']=save(world['study']/'activation_receipt.json',value)
    with pytest.raises(observer.ObserverError,match='activation_pair'):run(world)


def test_authorizer_rejects_mutations_and_frozen_rule_namespace_has_no_writer_class():
    for action in (sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE,sqlite3.SQLITE_ATTACH,sqlite3.SQLITE_PRAGMA,sqlite3.SQLITE_CREATE_TABLE):
        assert observer._deny_mutation(action,None,None,None,None)==sqlite3.SQLITE_DENY
    assert observer._deny_mutation(sqlite3.SQLITE_SELECT,None,None,None,None)==sqlite3.SQLITE_OK
    registry=json.loads(REGISTRY_PATH.read_bytes())
    sources,_=observer._closure(registry,observer.own_source_bindings(),lambda:1788940732.0)
    namespace=observer._frozen_rules(sources)
    assert 'CausalForecastLedger' not in namespace and 'PairRunner' not in namespace
    assert 'verified_publication' in namespace and 'validate_contract' in namespace


def test_readonly_proxy_rejects_nonselect_before_any_database_action():
    class Never:
        def execute(self,*args):raise AssertionError('database_should_not_be_called')
    proxy=observer._ReadOnlyProxy(Never(),{},1,lambda:2)
    with pytest.raises(observer.ObserverError,match='select_only'):proxy._read('UPDATE anything SET x=1')


def test_upstream_future_at_its_own_read_cannot_age_into_coherence(world):
    path=world['study']/'summary.json';value=json.loads(path.read_bytes())
    value['generated_epoch']=world['now'];value['payload_sha256']=observer.digest({k:v for k,v in value.items() if k!='payload_sha256'})
    save(path,value)
    hb_path=world['study']/'heartbeat.json';hb=json.loads(hb_path.read_bytes())
    hb['summary_sha256']=observer.digest(value);save(hb_path,hb)
    samples=iter([world['now']-2,world['now']-1,world['now'],world['now']+1,world['now']+2])
    result=observer._upstream(world['study'],observer.ORIGIN_REGISTRY_SHA256,lambda:next(samples))
    assert result['status']=='unavailable' and result['reason']=='origin_envelope_future_at_own_read'
    assert result['forecast_acceptance_from_this_envelope'] is False


def test_reparse_path_is_refused_if_supported(world,tmp_path):
    link=tmp_path/'db_link.sqlite'
    try:os.symlink(world['path'],link)
    except OSError:pytest.skip('symbolic links require an unavailable Windows privilege')
    with pytest.raises(observer.ObserverError,match='reparse'):observer.safe_path(link,file=True)


@pytest.mark.parametrize('clock',[True,None,'now',float('nan'),float('inf'),-1,10**400])
def test_bad_observation_clocks_never_start_database_reads(world,clock):
    with pytest.raises(observer.ObserverError,match='wall_clock_integrity'):run(world,clock=lambda:clock)
