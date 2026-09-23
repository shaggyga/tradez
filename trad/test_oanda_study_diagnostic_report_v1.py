"""Offline read-only adapter, outcome-blind selection and cache boundaries."""
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import time

import pytest

import oanda_study_diagnostic_report_v1 as diagnostic
import oanda_fixed_forecast_evaluation_eurusd_v1 as scorer
from test_oanda_fixed_forecast_evaluation import add_decision


@pytest.fixture(autouse=True)
def clear_cache():
    diagnostic._CACHE.clear()
    yield
    diagnostic._CACHE.clear()


@pytest.fixture
def fixture_data():
    contract=json.loads((diagnostic.ROOT/diagnostic.CONFIG).read_bytes())
    protocol=deepcopy(contract['evaluation_protocol'])
    start=int(time.time())-15000
    protocol.update(historical_start_utc=diagnostic._utc(start),historical_end_utc=diagnostic._utc(start+14000))
    data={'schema_version':'fixed_forecast_evaluation_input_v1','observed_cutoff_epoch':0,'decisions':[],'quotes':[],
          'collection_counts':{k:0 for k in diagnostic.TABLES}}
    for offset in (0,900,1800):
        add_decision(data,protocol,offset=offset)
    for q in data['quotes']:
        q['bid']=str(q['bid']);q['ask']=str(q['ask'])
    duplicate=deepcopy(data['decisions'][0]);duplicate['decision_id']='duplicate'
    for f in duplicate['forecasts']:
        f['forecast_id']='duplicate:'+f['family'];f['issued_epoch']+=1
    data['decisions'].insert(1,duplicate)
    return data,protocol,contract


def test_all_duplicate_members_excluded_input_and_quotes_unchanged(fixture_data):
    data,_,_=fixture_data;before=deepcopy(data)
    filtered,manifest=diagnostic._exclude_duplicate_references(data)
    assert data==before
    assert [d['decision_id'] for d in filtered['decisions']]==['d900','d1800']
    assert manifest[0]['decision_ids']==['d0','duplicate']
    assert filtered['quotes']==data['quotes']
    assert filtered['collection_counts']==data['collection_counts']
    assert filtered['decisions']==data['decisions'][2:]


@pytest.mark.parametrize('target_bid', ['0.00001','1.0','99.0'])
@pytest.mark.parametrize('side',[-1,0,1])
def test_exclusion_cannot_depend_on_prices_or_forecast_direction(fixture_data,target_bid,side):
    data,_,_=fixture_data
    expected=diagnostic._exclude_duplicate_references(data)[1]
    for q in data['quotes']:q['bid']=target_bid
    for d in data['decisions']:
        for f in d['forecasts']:f['side']=side;f['probability_up']=0 if side<0 else 1
    assert diagnostic._exclude_duplicate_references(data)[1]==expected


@pytest.mark.parametrize('bad',[True,None,'123',float('nan'),float('inf')])
def test_malformed_reference_is_not_silently_dropped(fixture_data,bad):
    data,_,_=fixture_data;data['decisions'][0]['reference_epoch']=bad
    with pytest.raises(ValueError,match='invalid_finite_clock'):
        diagnostic._exclude_duplicate_references(data)


def test_duplicate_decision_id_fails_even_if_reference_would_be_excluded(fixture_data):
    data,_,_=fixture_data;data['decisions'][1]['decision_id']='d0'
    with pytest.raises(ValueError,match='missing_or_duplicate_decision_id'):
        diagnostic._exclude_duplicate_references(data)


def test_equal_targets_with_unique_references_are_not_selected_away(fixture_data):
    data,_,_=fixture_data;data['decisions']=data['decisions'][2:]
    data['decisions'][1]['target_epoch']=data['decisions'][0]['target_epoch']
    filtered,groups=diagnostic._exclude_duplicate_references(data)
    assert not groups and filtered==data


def test_build_reproduces_frozen_scorer_and_keeps_original_failure(fixture_data,tmp_path,monkeypatch):
    data,protocol,contract=fixture_data
    monkeypatch.setattr(diagnostic,'_snapshot',lambda *_:(deepcopy(data),deepcopy(protocol),tmp_path))
    report=diagnostic._build(tmp_path,include_inputs=True)
    filtered,_=diagnostic._exclude_duplicate_references(data)
    expected=scorer.jsonable(scorer.evaluate(filtered,protocol))
    assert report['report']['paired_summaries']==expected['paired_summaries']
    assert report['report']['coverage']['paired_scored_decisions']==2
    assert report['original_scorer']=={'status':'failed','error':'duplicate_market_reference_epoch'}
    assert report['original_input']==data and report['filtered_input']==filtered
    assert report['original_input_sha256']==diagnostic._sha(diagnostic._encoded(data))
    assert report['filtered_input_sha256']==expected['input_sha256']
    assert report['excluded_decision_count']==2 and report['retained_decision_count']==2
    assert report['registered_scorecard'] is report['can_place_orders'] is report['proof_eligible'] is False


def test_unique_input_keeps_original_scorer_success(fixture_data,tmp_path,monkeypatch):
    data,protocol,_=fixture_data;data['decisions']=data['decisions'][2:]
    monkeypatch.setattr(diagnostic,'_snapshot',lambda *_:(data,protocol,tmp_path))
    result=diagnostic._build(tmp_path)
    assert result['original_scorer']['status']=='passed'
    assert result['original_input_sha256']==result['filtered_input_sha256']
    assert result['excluded_decision_count']==0


def test_lifetime_capacity_disclosed_without_truncating(fixture_data,tmp_path,monkeypatch):
    data,protocol,_=fixture_data
    data['collection_counts']['quotes']=int(.9*diagnostic.MAX_QUOTES)
    monkeypatch.setattr(diagnostic,'_snapshot',lambda *_:(data,protocol,tmp_path))
    result=diagnostic._build(tmp_path)
    assert result['capacity_state']=='near_limit'
    assert result['bounds']['maximum_lifetime_quotes']==8192
    assert result['original_decision_count']==4
    assert result['remaining_capacity']['quotes']==diagnostic.MAX_QUOTES-data['collection_counts']['quotes']


def test_api_report_byte_limit_fails_closed(fixture_data,tmp_path,monkeypatch):
    data,protocol,_=fixture_data
    monkeypatch.setattr(diagnostic,'_snapshot',lambda *_:(data,protocol,tmp_path))
    monkeypatch.setattr(diagnostic,'MAX_REPORT_BYTES',100)
    result=diagnostic.get_eurusd_diagnostic_report(tmp_path)
    assert result['status']=='unavailable' and result['error']=='diagnostic_report_size_limit'


def test_other_integrity_errors_are_not_repaired(fixture_data,tmp_path,monkeypatch):
    data,protocol,_=fixture_data
    q=deepcopy(data['quotes'][0]);q['bid']='0.5';data['quotes'].append(q)
    monkeypatch.setattr(diagnostic,'_snapshot',lambda *_:(data,protocol,tmp_path))
    with pytest.raises(ValueError,match='original_scorer_other_validation_failure'):
        diagnostic._build(tmp_path)


def make_database(tmp_path,fixture_data):
    data,protocol,contract=fixture_data
    study=tmp_path/'causal_forecast_study_eurusd_v1';study.mkdir()
    path=study/'study.sqlite'
    with sqlite3.connect(path) as db:
        db.executescript('''CREATE TABLE contract(id INTEGER,sha TEXT,payload TEXT);
        CREATE TABLE activation(id INTEGER,epoch REAL,contract_sha TEXT);
        CREATE TABLE clocks(epoch REAL);
        CREATE TABLE forecasts(id TEXT,bucket INTEGER,sha TEXT,payload TEXT);
        CREATE TABLE quotes(id TEXT,available REAL,market REAL,payload TEXT);
        CREATE TABLE consumption(id TEXT,epoch REAL);''')
        for table in diagnostic.TABLES:
            if table not in ('forecasts','quotes','consumption'):db.execute(f'CREATE TABLE {table}(id TEXT)')
        db.execute('INSERT INTO contract VALUES(1,?,?)',(diagnostic.CONTRACT_SHA256,diagnostic._encoded(contract).decode()))
        start=scorer.epoch(protocol['historical_start_utc'])
        db.execute('INSERT INTO activation VALUES(1,?,?)',(start,diagnostic.CONTRACT_SHA256))
        db.execute('INSERT INTO clocks VALUES(?)',(data['observed_cutoff_epoch'],))
        for i,d in enumerate(data['decisions']):
            value=deepcopy(d)
            committed=max(f.pop('committed_available_epoch') for f in value['forecasts'])
            db.execute('INSERT INTO forecasts VALUES(?,?,?,?)',(value['decision_id'],i,diagnostic._sha(diagnostic._encoded(value)),diagnostic._encoded(value).decode()))
            db.execute('INSERT INTO consumption VALUES(?,?)',(value['decision_id'],committed))
        for q in data['quotes']:
            db.execute('INSERT INTO quotes VALUES(?,?,?,?)',(q['quote_id'],q['available_epoch'],q['market_epoch'],diagnostic._encoded(q).decode()))
    return path,contract


def test_snapshot_reads_database_without_modifying_any_bytes(tmp_path,fixture_data):
    path,contract=make_database(tmp_path,fixture_data)
    before=path.read_bytes()
    data,protocol,_=diagnostic._snapshot(tmp_path,contract)
    assert path.read_bytes()==before
    assert data['collection_counts']['forecasts']==4
    assert len(data['quotes'])==9
    assert protocol['historical_start_utc']==fixture_data[1]['historical_start_utc']
    assert not list(path.parent.glob('*-wal'))


def test_bounded_snapshot_matches_frozen_export_exactly(tmp_path,fixture_data,monkeypatch):
    from oanda_causal_forecast_ledger_eurusd_v1 import CausalForecastLedger
    path,contract=make_database(tmp_path,fixture_data)
    cutoff=time.time()
    monkeypatch.setattr(diagnostic.time,'time',lambda:cutoff)
    facade=object.__new__(CausalForecastLedger)
    facade.path=path.resolve()
    facade.clock=lambda:cutoff
    facade._contract_encoded=diagnostic._encoded(contract)
    facade.activated_epoch=scorer.epoch(fixture_data[1]['historical_start_utc'])
    expected_data,expected_protocol=facade.export_evaluation()
    actual_data,actual_protocol,_=diagnostic._snapshot(tmp_path,contract)
    assert actual_data==expected_data
    assert actual_protocol==expected_protocol


@pytest.mark.parametrize('kind',['contract','activation','future_clock','forecast_hash'])
def test_snapshot_rejects_integrity_failures(tmp_path,fixture_data,kind):
    path,contract=make_database(tmp_path,fixture_data)
    with sqlite3.connect(path) as db:
        if kind=='contract':db.execute("UPDATE contract SET sha='wrong'")
        elif kind=='activation':db.execute("UPDATE activation SET contract_sha='wrong'")
        elif kind=='future_clock':db.execute('UPDATE clocks SET epoch=?',(time.time()+10000,))
        else:db.execute("UPDATE forecasts SET sha='wrong'")
    with pytest.raises(ValueError):diagnostic._snapshot(tmp_path,contract)


@pytest.mark.parametrize('bound',['MAX_QUOTES','MAX_DECISIONS','MAX_PAYLOAD_BYTES','MAX_DB_BYTES'])
def test_snapshot_bounds_fail_closed(tmp_path,fixture_data,monkeypatch,bound):
    _,contract=make_database(tmp_path,fixture_data)
    monkeypatch.setattr(diagnostic,bound,1)
    with pytest.raises(ValueError):diagnostic._snapshot(tmp_path,contract)


def test_snapshot_sql_deadline_is_enforced(tmp_path,fixture_data,monkeypatch):
    _,contract=make_database(tmp_path,fixture_data)
    monkeypatch.setattr(diagnostic,'SQL_DEADLINE_SECONDS',-1)
    # Add enough safe fixture rows to invoke SQLite's progress handler.
    with sqlite3.connect(tmp_path/'causal_forecast_study_eurusd_v1/study.sqlite') as db:
        db.executemany('INSERT INTO quotes VALUES(?,?,?,?)',[(f'x{i}',0,0,'{}') for i in range(500)])
    with pytest.raises(sqlite3.OperationalError,match='interrupted'):
        diagnostic._snapshot(tmp_path,contract)


def test_registered_source_or_config_mutation_fails_before_database(monkeypatch):
    original=diagnostic._read_bounded
    monkeypatch.setattr(diagnostic,'_read_bounded',lambda p,*a:original(p,*a)+b' ' if p.name=='oanda_exact_price_scoring.py' else original(p,*a))
    with pytest.raises(ValueError,match='registered_source_hash_mismatch'):diagnostic._registered_contract()
    monkeypatch.setattr(diagnostic,'_read_bounded',lambda p,*a:original(p,*a)+b' ')
    with pytest.raises(ValueError,match='registered_config_hash_mismatch'):diagnostic._registered_contract()


def cached_value():
    value=diagnostic._base();now=time.time()
    value.update(status='available',generated_epoch=now,generated_utc=diagnostic._utc(now),report={'sentinel':1})
    return value


@pytest.mark.parametrize('fails',[False,True])
def test_cache_both_success_and_errors_avoids_repeated_database_calls(tmp_path,monkeypatch,fails):
    calls=[]
    def build(_):
        calls.append(1)
        if fails:raise OSError('private URL or credential must never appear')
        return cached_value()
    monkeypatch.setattr(diagnostic,'_build',build)
    first=diagnostic.get_eurusd_diagnostic_report(tmp_path);second=diagnostic.get_eurusd_diagnostic_report(tmp_path)
    assert len(calls)==1
    if fails:assert second['error']=='OSError'
    else:
        first['report']['sentinel']=99
        assert second['report']['sentinel']==1


def test_expired_cache_is_rebuilt(tmp_path,monkeypatch):
    key=str(tmp_path.resolve());diagnostic._CACHE[key]=(time.monotonic()-61,cached_value())
    calls=[]
    monkeypatch.setattr(diagnostic,'_build',lambda _:(calls.append(1) or cached_value()))
    diagnostic.get_eurusd_diagnostic_report(tmp_path)
    assert calls==[1]


@pytest.mark.parametrize('delta',[-1,121])
def test_actual_return_clock_recheck_hides_future_or_stale_metrics(delta):
    value=cached_value();reported=diagnostic._present(value,value['generated_epoch']+delta)
    assert reported['status']=='unavailable' and reported['report'] is None


def test_locked_build_returns_bounded_unavailable(tmp_path):
    diagnostic._LOCK.acquire()
    try:result=diagnostic.get_eurusd_diagnostic_report(tmp_path)
    finally:diagnostic._LOCK.release()
    assert result['error']=='diagnostic_build_in_progress'


def test_original_scorecard_staleness_is_not_replaced(tmp_path):
    p=tmp_path/'scorecard.json';raw=json.dumps({'generated_utc':'2026-01-01T00:00:00+00:00','status':'old'})
    p.write_text(raw)
    value=diagnostic._scorecard_state(tmp_path,time.time())
    assert value['stale'] is True and value['status']=='old' and p.read_text()==raw


def test_review_output_exclusive_and_runtime_paths_forbidden(tmp_path,monkeypatch):
    monkeypatch.setattr(diagnostic,'_build',lambda *a,**k:cached_value())
    output=tmp_path/'review.json';data_root=tmp_path/'runtime'
    diagnostic.write_review_evidence(data_root,output)
    with pytest.raises(FileExistsError):diagnostic.write_review_evidence(data_root,output)
    with pytest.raises(ValueError,match='runtime_or_config'):
        diagnostic.write_review_evidence(data_root,data_root/'bad.json')
    with pytest.raises(ValueError,match='runtime_or_config'):
        diagnostic.write_review_evidence(data_root,diagnostic.ROOT/'config/bad.json')


def test_review_size_limit_does_not_create_partial_file(tmp_path,monkeypatch):
    monkeypatch.setattr(diagnostic,'_build',lambda *a,**k:cached_value())
    monkeypatch.setattr(diagnostic,'MAX_REVIEW_BYTES',1)
    output=tmp_path/'review.json'
    with pytest.raises(ValueError,match='review_size_limit'):
        diagnostic.write_review_evidence(tmp_path/'runtime',output)
    assert not output.exists()
