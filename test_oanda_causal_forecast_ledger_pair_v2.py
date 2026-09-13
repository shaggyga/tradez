"""Disposable single-family ledgers: retry/evidence and exact causal boundaries."""
from copy import deepcopy
from hashlib import sha256
import json
import sqlite3
import zlib

import pytest

from oanda_causal_forecast_ledger_pair_v2 import CausalForecastLedger, FAMILIES, SCHEMA, digest
from oanda_fixed_forecast_evaluation_pair_v2 import evaluate, PROTOCOL_SCHEMA
import oanda_causal_forecast_inputs_pair_v2 as input_module


class Clock:
    def __init__(self,value=1_800_000_000.0):self.value=value
    def __call__(self):return self.value
    def advance(self,seconds=1):self.value+=seconds;return self.value


def contract_fixture(instrument='EUR_USD',pip_size=.0001,family='probabilistic_state_space'):
    cohorts={family:f'causal_pair_family_v2_20260907.{instrument}.{family}.fixture'}
    protocol={'schema_version':PROTOCOL_SCHEMA,'contract_id':'fixture-family-evaluation-v2',
        'family':family,'instrument':instrument,'pip_size':pip_size,'input_timeframe':'M1','horizon_sec':3600,
        'proof_eligible':False,'account_eligible':False,'collection_enabled':False,
        'historical_start_utc':'2026-01-01T00:00:00+00:00','historical_end_utc':'2028-01-01T00:00:00+00:00',
        'cohorts':cohorts,'model_version':'sha256:'+'a'*64,'feature_version':'sha256:'+'b'*64,
        'baselines':['fair_coin','zero_move','no_trade','rolling_class_rate'],
        'rolling_lookback':100,'rolling_min_labels':20,'quote_max_age_sec':60,
        'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60,'maximum_input_age_sec':900,'extra_cost_stress_bps':[0.0,.5,1.0]}
    return {'schema_version':SCHEMA,'contract_id':'fixture-family-collection-v2','family':family,
        'research_only':True,'account_eligible':False,'proof_eligible':False,'can_place_orders':False,
        'can_authorize':False,'can_promote':False,'historical_rows_imported':False,
        'instrument':instrument,'pip_size':pip_size,'input_timeframe':'M1','horizon_sec':3600,
        'cohorts':deepcopy(cohorts),'evaluation_protocol':protocol,'model_version':protocol['model_version'],
        'feature_version':protocol['feature_version'],'numeric_model_source_sha256':'c'*64,
        'dependency_versions':{'python':'fixture-only'},'cadence_sec':900,'maximum_build_sec':120,'maximum_input_age_sec':900,
        'quote_max_age_sec':60,'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60}


@pytest.fixture(autouse=True)
def capture_validation_seam(monkeypatch):
    real=input_module.validate_capture
    def check(capture,**kwargs):
        if capture.get('fixture_only') is True:return
        return real(capture,**kwargs)
    monkeypatch.setattr(input_module,'validate_capture',check)


@pytest.fixture(params=[(pair,pip,family) for pair,pip in
    [('EUR_USD',.0001),('AUD_JPY',.01),('HKD_JPY',.0001),('USD_HUF',.01)] for family in FAMILIES])
def opened(tmp_path,request):
    clock=Clock();contract=contract_fixture(*request.param)
    ledger=CausalForecastLedger(tmp_path/'fresh.sqlite',contract,clock=clock,activate=True)
    try:yield ledger,clock,contract
    finally:ledger.close()


def quote(ledger,clock,**changes):
    return ledger.observe_quote({'instrument':ledger.contract['instrument'],'pip_size':ledger.contract['pip_size'],
        'market_epoch':clock.value,'available_epoch':clock.value,'bid':'1.1000','ask':'1.1002',
        'tradeable':True,**changes})


def ready_attempt(ledger,clock,reference=None):
    clock.advance();reference=reference or quote(ledger,clock)
    attempt=ledger.begin_attempt(int(clock.value//900),reference)
    assert attempt
    clock.advance();family=ledger.contract['family']
    capture={'fixture_only':True,'status':'ready','first_observed_epoch':clock.value,
        'max_bar_close_epoch':clock.value-1,'pairs':[ledger.contract['instrument']],
        'output_instrument':ledger.contract['instrument'],'pip_size':ledger.contract['pip_size'],
        'model_source_sha256':ledger.contract['numeric_model_source_sha256'],
        'dependency_versions':deepcopy(ledger.contract['dependency_versions']),
        'family_readiness':{family:{'status':'ready','ready':True,'reasons':[]}}}
    capture['source_capture_sha256']=digest(capture)
    result={'status':'ready','output_instrument':capture['output_instrument'],'pip_size':capture['pip_size'],
        'model_source_sha256':capture['model_source_sha256'],'source_capture_sha256':capture['source_capture_sha256'],
        'dependency_versions':deepcopy(capture['dependency_versions']),
        'computation_started_epoch':clock.value,'computed_epoch':clock.value+.5,
        'predictions':{family:{'probability_up':.75,'expected_signed_pips':2.0,'side':1,'diagnostics':{'fixture_only':True}}}}
    clock.advance()
    return attempt,reference,capture,result


def issued(ledger,clock):
    attempt,reference,capture,result=ready_attempt(ledger,clock)
    identity=ledger.issue(attempt,capture,result,after_commit=clock.advance)
    clock.advance();ledger.consume()
    return identity,reference,capture,result


def completed(ledger,clock,side=1):
    attempt,reference,capture,result=ready_attempt(ledger,clock)
    result['predictions'][ledger.contract['family']]['side']=side
    identity=ledger.issue(attempt,capture,result,after_commit=clock.advance)
    clock.advance();ledger.consume();clock.advance()
    entry=quote(ledger,clock,bid='1.1004',ask='1.1006');ledger.settle()
    clock.value=reference['market_epoch']+3600
    target=quote(ledger,clock,bid='1.1010',ask='1.1012');ledger.settle()
    return identity,reference,entry,target


def test_no_unrequested_activation_or_file_creation(tmp_path):
    path=tmp_path/'not_created'/'study.sqlite'
    with pytest.raises(ValueError,match='activation'):
        CausalForecastLedger(path,contract_fixture())
    assert not path.parent.exists()


def test_v1_database_refused_before_writer_or_schema_mutation(tmp_path):
    from oanda_causal_forecast_ledger_pair_v1 import CausalForecastLedger as OldLedger
    from test_oanda_causal_forecast_ledger_pair_v1 import contract_fixture as old_contract
    path=tmp_path/'old.sqlite';old=OldLedger(path,old_contract(),clock=Clock(),activate=True);old.close()
    before=path.read_bytes()
    with pytest.raises(ValueError,match='immutable_contract'):
        CausalForecastLedger(path,contract_fixture(),clock=Clock(),activate=True)
    assert path.read_bytes()==before


def test_actual_activation_and_immutable_contract(opened):
    ledger,clock,contract=opened
    assert ledger.activated_epoch==clock.value
    contract['family']='modified';view=ledger.contract;view['pip_size']=.01
    assert ledger.contract_hash==digest(ledger.contract)
    with pytest.raises(sqlite3.IntegrityError,match='immutable_evidence'):
        ledger.db.execute('DELETE FROM contract')


def test_partial_result_publishes_selected_family_without_sibling(opened):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock);r['status']='partial'
    identity=ledger.issue(a,c,r,after_commit=clock.advance)
    saved=json.loads(ledger.db.execute('SELECT payload FROM forecasts').fetchone()[0])
    assert saved['decision_id']==identity and saved['attempt_id']==a
    assert [f['family'] for f in saved['forecasts']]==[ledger.contract['family']]
    assert ledger.counts()['forecasts']==1


def test_abstention_capture_retained_and_same_bucket_retry_publishes(opened):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock)
    ledger.record_abstention(a,{'status':'abstain','reasons':['training_rows:10<24']},capture=c,result={'status':'abstain'})
    b,q2,c2,r2=ready_attempt(ledger,clock)
    assert a!=b
    ledger.issue(b,c2,r2)
    assert ledger.counts()['attempts']==2 and ledger.counts()['diagnostics']==1
    assert ledger.begin_attempt(int(clock.value//900),q2) is None
    diag=json.loads(ledger.db.execute('SELECT payload FROM diagnostics').fetchone()[0])
    raw=ledger.db.execute('SELECT payload FROM inputs WHERE id=?',(diag['input_capture_sha256'],)).fetchone()[0]
    assert json.loads(zlib.decompress(raw))==c
    assert diag['retained_result']=={'status':'abstain'}
    assert diag['capture_is_forecast_authority'] is False


def test_closed_attempt_cannot_later_issue_or_change_abstention(opened):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock)
    ledger.record_abstention(a,{'status':'abstain'})
    with pytest.raises(ValueError,match='closed'):ledger.issue(a,c,r)
    with pytest.raises(ValueError,match='closed'):ledger.record_abstention(a,{'status':'different'})
    assert ledger.counts()['forecasts']==0


def test_cached_capture_before_attempt_keeps_original_availability(opened):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock)
    ledger.record_abstention(a,{'status':'deferred'},capture=c)
    original_observed=c['first_observed_epoch'];clock.advance()
    b=ledger.begin_attempt(int(clock.value//900),quote(ledger,clock))
    r['computation_started_epoch']=clock.advance();r['computed_epoch']=clock.advance();clock.advance()
    ledger.issue(b,c,r)
    arm=json.loads(ledger.db.execute('SELECT payload FROM forecasts').fetchone()[0])['forecasts'][0]
    assert arm['input_source_observed_epoch']==original_observed
    assert ledger.counts()['inputs']==1


def test_duplicate_reference_across_bucket_rejected_before_publication(opened):
    ledger,clock,_=opened;clock.advance(895)
    a,reference,c,r=ready_attempt(ledger,clock)
    ledger.issue(a,c,r,after_commit=clock.advance);clock.advance(2)
    b,_,c2,r2=ready_attempt(ledger,clock,reference=reference)
    with pytest.raises(ValueError,match='market_reference_already_issued'):ledger.issue(b,c2,r2)
    ledger.record_abstention(b,{'status':'abstain','reason':'market_reference_already_issued'},capture=c2)
    assert ledger.counts()['forecasts']==ledger.counts()['publication']==1
    assert ledger.counts()['attempts']==2


def test_commit_receipt_consumption_are_independent_boundaries(opened):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock);seen=[]
    def hook():
        with sqlite3.connect(ledger.path.as_uri()+'?mode=ro',uri=True) as db:
            seen.append(tuple(db.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ('forecasts','publication','consumption')))
        clock.advance()
    ledger.issue(a,c,r,after_commit=hook)
    assert seen==[(1,0,0)] and ledger.counts()['consumption']==0
    clock.advance();ledger.consume();assert ledger.counts()['consumption']==1


def test_crash_after_commit_can_recover_only_postcommit_receipt(opened):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock)
    def crash():raise RuntimeError('fixture crash after commit')
    with pytest.raises(RuntimeError):ledger.issue(a,c,r,after_commit=crash)
    assert ledger.counts()['forecasts']==1 and ledger.counts()['publication']==0
    clock.advance(30);ledger.recover_publications()
    assert ledger.db.execute('SELECT epoch FROM publication').fetchone()[0]==clock.value
    assert ledger.counts()['consumption']==0


@pytest.mark.parametrize('field,value',[
    ('input_source_observed_epoch',-1),('computed_epoch',float('nan')),('computation_started_epoch',0),
    ('output_instrument','GBP_USD'),('pip_size',.001),('source_capture_sha256','wrong'),
    ('model_source_sha256','wrong'),('dependency_versions',{})])
def test_bad_result_identity_or_clocks_cannot_issue(opened,field,value):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock)
    if field=='input_source_observed_epoch':c['first_observed_epoch']=value
    else:r[field]=value
    with pytest.raises(ValueError):ledger.issue(a,c,r)
    assert ledger.counts()['forecasts']==ledger.counts()['publication']==0


@pytest.mark.parametrize('kind',['missing','unknown','blocked','computed_before_attempt','future_capture','preactivation','late_build'])
def test_selected_family_and_causal_conditions_required(opened,kind):
    ledger,clock,_=opened;a,q,c,r=ready_attempt(ledger,clock);family=ledger.contract['family']
    if kind=='missing':r['predictions']={}
    elif kind=='unknown':r['predictions']['invented']={}
    elif kind=='blocked':c['family_readiness'][family]={'status':'blocked','ready':False}
    elif kind=='computed_before_attempt':r['computation_started_epoch']=q['market_epoch']-1
    elif kind=='future_capture':c['max_bar_close_epoch']=clock.value+1
    elif kind=='preactivation':c['first_observed_epoch']=ledger.activated_epoch
    elif kind=='late_build':clock.advance(121)
    with pytest.raises(ValueError):ledger.issue(a,c,r)
    assert ledger.counts()['forecasts']==0


def test_entry_must_have_market_and_availability_after_consumption(opened):
    ledger,clock,_=opened;identity,ref,_,_=issued(ledger,clock)
    consumed=clock.value;clock.advance();quote(ledger,clock,market_epoch=consumed);ledger.settle()
    assert ledger.counts()['entries']==0
    clock.advance();later=quote(ledger,clock);ledger.settle()
    assert ledger.db.execute('SELECT quote_id FROM entries').fetchone()[0]==later['quote_id']


def test_original_target_and_independent_family_scoring(opened):
    ledger,clock,_=opened;identity,ref,entry,target=completed(ledger,clock)
    data,protocol=ledger.export_evaluation();report=evaluate(data,protocol)
    assert data['decisions'][0]['target_epoch']==ref['market_epoch']+3600
    assert report['coverage']['scored_decisions']==1
    assert report['coverage']['total_forecasts']==1
    assert report['decisions'][0]['entry']['quote_id']==entry['quote_id']
    assert report['decisions'][0]['target']['quote_id']==target['quote_id']
    assert set(report['summaries'])=={ledger.contract['family'],'fair_coin','zero_move','no_trade','rolling_class_rate'}


@pytest.mark.parametrize('missing',['entry','target'])
def test_missing_executable_quote_stays_visible(opened,missing):
    ledger,clock,_=opened;_,ref,_,_=issued(ledger,clock)
    if missing=='target':clock.advance();quote(ledger,clock);ledger.settle()
    clock.value=ref['market_epoch']+3661;ledger.settle()
    assert ledger.counts()['exclusions']==1 and ledger.counts()['outcomes']==0
    data,protocol=ledger.export_evaluation();report=evaluate(data,protocol)
    assert report['coverage']['scored_decisions']==0


def test_two_pending_attempts_still_allow_only_one_success(opened):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    b,_,c2,r2=ready_attempt(ledger,clock)
    ledger.issue(a,c,r)
    with pytest.raises(ValueError,match='bucket_already_issued'):ledger.issue(b,c2,r2)
    ledger.record_abstention(b,{'reason':'bucket_already_issued'})
    assert ledger.counts()['forecasts']==1 and ledger.counts()['attempts']==2


def test_same_market_clock_with_changed_quote_prices_is_still_duplicate(opened):
    ledger,clock,_=opened;clock.advance(895)
    a,ref,c,r=ready_attempt(ledger,clock);ledger.issue(a,c,r);clock.advance(4)
    changed=quote(ledger,clock,market_epoch=ref['market_epoch'],bid='1.1001',ask='1.1003')
    assert changed['quote_id']!=ref['quote_id']
    b,_,c2,r2=ready_attempt(ledger,clock,reference=changed)
    with pytest.raises(ValueError,match='market_reference_already_issued'):ledger.issue(b,c2,r2)
    assert ledger.counts()['forecasts']==1


def test_neutral_arm_keeps_own_decision_but_no_directional_denominator(opened):
    ledger,clock,_=opened;completed(ledger,clock,side=0)
    data,protocol=ledger.export_evaluation();report=evaluate(data,protocol)
    summary=report['summaries'][ledger.contract['family']]
    assert summary['decisions']==1 and summary['directional_decisions']==0 and summary['abstentions']==1
    assert summary['direction_hit_rate_when_directional'] is None
    assert summary['mean_net_bps_per_decision']==0


def test_read_only_export_can_run_off_writer_thread(opened):
    from concurrent.futures import ThreadPoolExecutor
    ledger,clock,_=opened;completed(ledger,clock)
    with ThreadPoolExecutor(max_workers=1) as pool:data,protocol=pool.submit(ledger.export_evaluation).result()
    assert evaluate(data,protocol)['coverage']['scored_decisions']==1


@pytest.mark.parametrize('age',[900,900.001])
def test_cached_input_freshness_checked_at_actual_issue(opened,age):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    c['max_bar_close_epoch']=clock.value-age
    if age==900:ledger.issue(a,c,r)
    else:
        with pytest.raises(ValueError,match='cached_input_expired'):ledger.issue(a,c,r)
        assert ledger.counts()['forecasts']==0


@pytest.mark.parametrize('family,count',[(FAMILIES[0],25),(FAMILIES[0],480),(FAMILIES[1],480)])
def test_real_new_capture_computation_and_later_issue_integration(tmp_path,family,count):
    from test_oanda_causal_forecast_inputs_pair_v2 import archive
    _,observed=archive(tmp_path,count)
    clock=Clock(observed-2);contract=contract_fixture(family=family)
    contract['numeric_model_source_sha256']=input_module.NUMERICAL_SOURCE_SHA256
    contract['dependency_versions']=input_module.dependency_versions()
    ledger=CausalForecastLedger(tmp_path/'actual.sqlite',contract,clock=clock,activate=True)
    try:
        clock.value=observed
        capture=input_module.capture_inputs(tmp_path,'EUR_USD',pip_size=.0001,clock=clock)
        input_module.validate_capture(capture,instrument='EUR_USD',pip_size=.0001)
        clock.advance();reference=quote(ledger,clock)
        attempt=ledger.begin_attempt(int(clock.value//900),reference)
        result=input_module.compute_predictions(capture,families=(family,),clock=lambda:clock.advance(.01))
        assert result['status']=='ready' and set(result['predictions'])=={family}
        clock.advance();ledger.issue(attempt,capture,result)
        saved=json.loads(ledger.db.execute('SELECT payload FROM forecasts').fetchone()[0])
        assert saved['forecasts'][0]['input_source_observed_epoch']==observed
        assert saved['forecasts'][0]['issued_epoch']>result['computed_epoch']
        assert saved['target_epoch']==reference['market_epoch']+3600
    finally:ledger.close()
