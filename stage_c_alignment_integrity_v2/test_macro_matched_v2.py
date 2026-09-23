import copy
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import pandas as pd
import pytest

ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import macro_matched_population_v2 as core
import macro_matched_operator_v2 as op
from publication import RunPublisher
INPUTS=Path(os.environ.get('FOREX_MACRO_MATCHED_INPUTS',str(ROOT/'evidence/timed_20260922_154551/macro_matched_population/inputs')))
RECIPE=ROOT/'MACRO_MATCHED_OPERATOR_RECIPE.json'


@pytest.fixture(scope='module')
def completed(tmp_path_factory):
    supplied=os.environ.get('FOREX_MACRO_MATCHED_RUNS');runs=Path(supplied) if supplied else tmp_path_factory.mktemp('matched')
    r=op.operate('verify' if supplied else 'run',RECIPE,op.sha(RECIPE),INPUTS,runs)
    assert r['status']=='completed_verified',r
    return Path(r['run_path'])


def plan():return op.read(INPUTS/'MATCHED_POPULATION_PLAN.json')
def event(**kw):return dict(event_id='fixture',currencies=['EUR'],publication_epoch=3601,available_epoch=3601,release_candidate_within_retained_metadata=False,**kw)
def raw(last=100.05):return pd.DataFrame({'epoch':[3600,7200,90000],'mid':[100.,last,101.],'bid':[99.99,last-.01,100.99],'ask':[100.01,last+.01,101.01]})
def row_for(frame):return core.pair_rows('EUR_USD',frame,[event()],plan(),'synthetic')['rows'][0]
def doc(vid='a',pub='2026-09-14T12:00:00Z',kind='retained_parsed_detail',bootstrap=False):
    return {'version_id':vid,'canonical_event_id':'e','publication_clocks':[pub],'available_epoch':1789390800,
        'kind':kind,'listing_bootstrap':bootstrap,'published_time_inferred':False,'source_id':'source','source_url':'https://example.invalid/doc','currencies':['EUR']}


def test_exact_endpoint_hand_oracle_and_rounding():
    r=row_for(raw())
    assert (r['decision_epoch'],r['reference_bar_start_epoch'],r['target_bar_start_epoch'],r['assumed_outcome_available_epoch'])==(3660,3600,7200,7260)
    assert r['midpoint_return_bps']==pytest.approx(5) and r['movement_label']=='no_move'
    assert r['long_endpoint_proxy_bps']==pytest.approx(3) and r['short_endpoint_proxy_bps']==pytest.approx(-7)
    assert r['state']=='observed_midpoint_endpoints' and not r['forecast_admission']


@pytest.mark.parametrize('last,label',[(101,'positive_move'),(99,'negative_move'),(100,'no_move')])
def test_all_observed_move_signs_retained(last,label):assert row_for(raw(last))['movement_label']==label


@pytest.mark.parametrize('mutation,status',[
    ('missing_reference','no_exact_reference'),('missing_target','no_exact_target'),
    ('duplicate_reference','invalid_or_duplicate_reference'),('invalid_mid','invalid_or_duplicate_target')])
def test_absence_or_invalidity_is_never_no_move(mutation,status):
    frame=raw()
    if mutation=='missing_reference':frame=frame.iloc[1:]
    if mutation=='missing_target':frame=frame.drop(1)
    if mutation=='duplicate_reference':frame=pd.concat([frame,frame.iloc[[0]]])
    if mutation=='invalid_mid':frame.loc[1,'mid']=0
    r=row_for(frame)
    assert r['state']==status and r['midpoint_return_bps'] is None and r['movement_label'] is None


def test_invalid_bidask_retains_only_midpoint_research():
    frame=raw();frame.loc[1,'bid']=frame.loc[1,'ask']+1;r=row_for(frame)
    assert r['movement_label']=='no_move' and not r['bidask_endpoint_valid']
    assert r['long_endpoint_proxy_bps'] is None and r['short_endpoint_proxy_bps'] is None


def test_future_outcome_hidden_until_assumed_maturity():
    r=row_for(raw())
    a=core.reveal(r,7260-1);b=core.reveal(r,7260)
    assert a['status']=='pending_assumed_maturity' and a['midpoint_return_bps'] is None
    assert a['long_endpoint_proxy_bps'] is None and a['movement_label'] is None
    assert b['movement_label']=='no_move'
    changed={**r,'midpoint_return_bps':999,'movement_label':'positive_move'}
    assert core.reveal(changed,7259)==a


def test_version_dedup_and_ambiguous_publication_not_hash_order():
    a=doc();b=doc('b','2026-09-15T12:00:00Z')
    x=core.resolve_events([a,b]);y=core.resolve_events([b,a]);assert x==y
    assert len(x)==1 and x[0]['publication_epoch'] is None
    assert x[0]['version_ids']==['a','b'] and not x[0]['release_candidate_within_retained_metadata']
    assert x[0]['resolution_uses_full_cohort_retrospectively'] and not x[0]['forecast_admission']
    with pytest.raises(ValueError):core.resolve_events([a,a])


def test_timezone_equivalent_publication_is_one_instant():
    x=core.resolve_events([doc(),doc('b','2026-09-14T08:00:00-04:00')])[0]
    assert x['publication_epoch']==core.epoch('2026-09-14T12:00:00Z')


@pytest.mark.parametrize('kind,bootstrap,reason',[
    ('calendar_or_schedule_listing',False,'calendar_or_schedule_listing'),
    ('headline_or_listing_only',False,'not_exclusively_parsed_detail'),
    ('retained_parsed_detail',True,'retained_listing_bootstrap')])
def test_nonrelease_records_preserved_but_not_promoted(kind,bootstrap,reason):
    x=core.resolve_events([doc(kind=kind,bootstrap=bootstrap)])[0]
    assert reason in x['release_limitations'] and not x['release_candidate_within_retained_metadata']


def test_all68_and_fixed_actual_support(completed):
    report=op.read(completed/'matched_report.json');events=op.read(completed/'events.json')
    coverage=op.read(completed/'pair_coverage.json')
    assert [x['pair'] for x in coverage]==op.read(INPUTS/'universe.json')
    assert report['events']==2207 and report['versions']==2696 and report['pair_rows']==60220
    assert report['release_candidates_within_retained_metadata']==8
    assert [g['movement_labels'].get('no_move',0) for g in report['groups']]==[1563,396,9740,2513]
    assert [g['support_states']['observed_midpoint_endpoints'] for g in report['groups']]==[1895,1326,11463,10616]
    assert all(x['mapped_events']+x['unmapped_events']==len(events) for x in coverage)
    assert all(not x['forecast_admission'] and x['expectations'] is None for x in events)


def test_all_pair_rows_reconcile_shared_events_without_independence(completed):
    total=0
    for pair in op.read(INPUTS/'universe.json'):
        p=op.read(completed/(pair+'.json'));total+=len(p['rows'])
        assert len(p['rows'])==p['mapped_events']*4
        assert len({(r['event_id'],r['anchor'],r['horizon_minutes']) for r in p['rows']})==len(p['rows'])
        assert all(not r['independent_observation'] and not r['forecast_admission'] for r in p['rows'])
    assert total==60220


def test_original_endpoint_formula_on_real_raw_source(completed):
    capture=op.read(INPUTS/'CANDLE_CAPTURE.json');descriptor=next(x for x in capture['pairs'] if x['pair']=='EUR_USD')
    frame=core.decode_candles((INPUTS/'EUR_USD.csv.gz').read_bytes(),descriptor,'EUR_USD',plan())
    lookup=frame.set_index('epoch');rows=op.read(completed/'EUR_USD.json')['rows'];checked=0
    for r in rows:
        if r['state']=='observed_midpoint_endpoints':
            a=lookup.loc[r['reference_bar_start_epoch']];b=lookup.loc[r['target_bar_start_epoch']]
            assert r['midpoint_return_bps']==(b.mid/a.mid-1)*10000
            checked+=1
    assert checked>0


def test_candle_hash_and_pair_mutations_fail():
    capture=op.read(INPUTS/'CANDLE_CAPTURE.json');d=next(x for x in capture['pairs'] if x['pair']=='EUR_USD');b=(INPUTS/'EUR_USD.csv.gz').read_bytes()
    with pytest.raises(ValueError,match='compressed_candle_hash_mismatch'):core.decode_candles(b+b'bad',d,'EUR_USD',plan())
    with pytest.raises(ValueError,match='candle_pair_or_granularity_mismatch'):core.decode_candles(b,d,'GBP_USD',plan())


def test_candle_clock_mutation_rejected_after_valid_hashes():
    d=next(x for x in op.read(INPUTS/'CANDLE_CAPTURE.json')['pairs'] if x['pair']=='EUR_USD')
    raw=gzip.decompress((INPUTS/'EUR_USD.csv.gz').read_bytes());lines=raw.splitlines(keepends=True)
    fields=lines[1].split(b',');fields[1]=b'2020-01-01T00:00:00+00:00';lines[1]=b','.join(fields)
    expanded=b''.join(lines);compressed=gzip.compress(expanded)
    mutated={**d,'compressed_sha256':hashlib.sha256(compressed).hexdigest(),'expanded_sha256':hashlib.sha256(expanded).hexdigest(),'expanded_bytes':len(expanded)}
    with pytest.raises(ValueError,match='candle_clock_mismatch'):core.decode_candles(compressed,mutated,'EUR_USD',plan())


def test_external_recipe_pin_failure_is_inert(tmp_path):
    r=op.operate('run',RECIPE,'0'*64,INPUTS,tmp_path/'runs')
    assert r['status']=='review_required' and not (tmp_path/'runs').exists()


def test_live_writer_refused(tmp_path):
    r=op.read(RECIPE);owner=RunPublisher(tmp_path,r['run_id'],op.identity_for(r));owner.acquire()
    try:assert op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path)['status']=='review_required'
    finally:owner.release()


@pytest.mark.parametrize('boundary',[3,38])
def test_process_death_resumes_all71_exact_payloads(tmp_path,completed,boundary):
    p=subprocess.run([sys.executable,'-I','-B',str(ROOT/'macro_matched_operator_v2.py'),'run','--recipe',str(RECIPE),'--recipe-sha256',op.sha(RECIPE),'--inputs',str(INPUTS),'--runs-dir',str(tmp_path),'--test-crash-after',str(boundary)],capture_output=True,text=True,timeout=120)
    assert p.returncode==91,(p.stdout,p.stderr)
    root=tmp_path/op.read(RECIPE)['run_id'];before={n:op.sha(root/n) for n in op.read(RECIPE)['required_payloads'] if (root/n).exists()}
    assert not (root/'COMPLETION_MANIFEST.json').exists()
    r=op.operate('resume',RECIPE,op.sha(RECIPE),INPUTS,tmp_path);assert r['status']=='completed_verified',r
    assert all(op.sha(root/n)==h for n,h in before.items())
    assert all(op.sha(root/n)==op.sha(completed/n) for n in op.read(RECIPE)['required_payloads'])
