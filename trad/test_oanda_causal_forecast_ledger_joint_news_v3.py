"""Disposable single-family ledgers: retry/evidence and exact causal boundaries."""
from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
import zlib

import pytest

from oanda_causal_forecast_ledger_joint_news_v3 import CausalForecastLedger, FAMILIES, SCHEMA, digest
from oanda_fixed_forecast_evaluation_joint_news_v1 import evaluate, PROTOCOL_SCHEMA
import oanda_causal_forecast_ledger_joint_news_v3 as ledger_module


class Clock:
    def __init__(self,value=1_800_000_000.0):self.value=value
    def __call__(self):return self.value
    def advance(self,seconds=1):self.value+=seconds;return self.value


def contract_fixture(instrument='EUR_USD',pip_size=.0001,family='ridge_price_news_v1'):
    cohorts={family:f'joint_price_news_v1_20260907.{instrument}.{family}.news_repair_v2_fixture'}
    protocol={'schema_version':PROTOCOL_SCHEMA,'contract_id':'fixture-family-evaluation-v2',
        'family':family,'instrument':instrument,'pip_size':pip_size,'input_timeframe':'M1','horizon_sec':3600,
        'proof_eligible':False,'account_eligible':False,'collection_enabled':False,
        'historical_start_utc':'2026-01-01T00:00:00+00:00','historical_end_utc':'2028-01-01T00:00:00+00:00',
        'cohorts':cohorts,'model_version':'sha256:'+'a'*64,'feature_version':'sha256:'+'b'*64,
        'baselines':['fair_coin','zero_move','no_trade','rolling_class_rate'],
        'rolling_lookback':100,'rolling_min_labels':20,'quote_max_age_sec':60,
        'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60,'maximum_input_age_sec':900,'maximum_news_age_sec':300,'extra_cost_stress_bps':[0.0,.5,1.0]}
    return {'schema_version':SCHEMA,'contract_id':'fixture-family-collection-v2','family':family,
        'research_only':True,'account_eligible':False,'proof_eligible':False,'can_place_orders':False,
        'can_authorize':False,'can_promote':False,'historical_rows_imported':False,
        'instrument':instrument,'pip_size':pip_size,'input_timeframe':'M1','horizon_sec':3600,
        'cohorts':deepcopy(cohorts),'evaluation_protocol':protocol,'model_version':protocol['model_version'],
        'feature_version':protocol['feature_version'],'numeric_model_source_sha256':'c'*64,
        'dependency_versions':{'python':'fixture-only'},'cadence_sec':900,'maximum_build_sec':120,'maximum_input_age_sec':900,'maximum_news_age_sec':300,
        'quote_max_age_sec':60,'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60}


@pytest.fixture(autouse=True)
def capture_validation_seam(monkeypatch):
    real=ledger_module.validate_input_capture
    def check(capture,**kwargs):
        if capture.get('fixture_only') is True:return
        return real(capture,**kwargs)
    monkeypatch.setattr(ledger_module,'validate_input_capture',check)


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
        'news_evidence_epoch':clock.value-20,'news_generated_epoch':clock.value-10,
        'news_first_observed_epoch':clock.value-1,'news_available_epoch':clock.value-1,
        'news_expires_epoch':clock.value+280,'news_capture_sha256':'e'*64,
        'max_bar_close_epoch':clock.value-1,'pairs':[ledger.contract['instrument']],
        'output_instrument':ledger.contract['instrument'],'pip_size':ledger.contract['pip_size'],
        'model_source_sha256':ledger.contract['numeric_model_source_sha256'],
        'dependency_versions':deepcopy(ledger.contract['dependency_versions']),
        'family_readiness':{family:{'status':'ready','ready':True,'reasons':[]}}}
    capture.update(research_only=True,can_place_orders=False,can_promote=False,can_authorize=False,account_eligible=False,proof_eligible=False)
    capture['source_capture_sha256']=digest(capture)
    result={'status':'ready','output_instrument':capture['output_instrument'],'pip_size':capture['pip_size'],
        'model_source_sha256':capture['model_source_sha256'],'source_capture_sha256':capture['source_capture_sha256'],
        'dependency_versions':deepcopy(capture['dependency_versions']),
        'computation_started_epoch':clock.value,'computed_epoch':clock.value+.5,
        'predictions':{family:{'probability_up':.75,'expected_signed_pips':2.0,'side':1,'diagnostics':{
            'fixture_only':True,'training_news_available_max_epoch':clock.value-4000,
            'training_news_feature_cutoff_max_epoch':clock.value-3601,
            'training_label_maturity_max_epoch':clock.value-1,
            'matched_price_only_expected_pips':1.3,'neutral_news_ablation_expected_pips':1.2}}}}
    result.update(research_only=True,can_place_orders=False,can_promote=False,can_authorize=False,account_eligible=False,proof_eligible=False)
    result.update({key:capture[key] for key in (*ledger_module.NEWS_CLOCK_FIELDS,'news_capture_sha256')})
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


def test_actual_activation_and_immutable_contract(opened):
    ledger,clock,contract=opened
    assert ledger.activated_epoch==clock.value
    contract['family']='modified';view=ledger.contract;view['pip_size']=.01
    assert ledger.contract_hash==digest(ledger.contract)
    with pytest.raises(sqlite3.IntegrityError,match='immutable_evidence'):
        ledger.db.execute('DELETE FROM contract')


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


@pytest.mark.parametrize('age',[900,900.001])
def test_cached_input_freshness_checked_at_actual_issue(opened,age):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    c['max_bar_close_epoch']=clock.value-age
    r['predictions'][ledger.contract['family']]['diagnostics']['training_label_maturity_max_epoch']=c['max_bar_close_epoch']
    if age==900:ledger.issue(a,c,r)
    else:
        with pytest.raises(ValueError,match='cached_input_expired'):ledger.issue(a,c,r)
        assert ledger.counts()['forecasts']==0


@pytest.mark.parametrize('field', [*ledger_module.NEWS_CLOCK_FIELDS,'news_capture_sha256'])
def test_result_must_repeat_exact_current_news_capture_binding(opened,field):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    r[field]='f'*64 if field=='news_capture_sha256' else r[field]+.001
    with pytest.raises(ValueError,match='result_news_'):ledger.issue(a,c,r)
    assert ledger.counts()['forecasts']==0


@pytest.mark.parametrize('age',[300,300.001])
def test_news_freshness_uses_actual_issue_boundary(opened,age):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    c['news_evidence_epoch']=clock.value-age
    c['news_expires_epoch']=clock.value+1
    r.update({key:c[key] for key in ledger_module.NEWS_CLOCK_FIELDS})
    if age==300:ledger.issue(a,c,r)
    else:
        with pytest.raises(ValueError,match='news_stale'):ledger.issue(a,c,r)
        assert ledger.counts()['forecasts']==0


@pytest.mark.parametrize('remaining',[0,-.001])
def test_original_member_expiry_is_inclusive_and_cannot_be_renewed(opened,remaining):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    c['news_expires_epoch']=r['news_expires_epoch']=clock.value+remaining
    if remaining==0:ledger.issue(a,c,r)
    else:
        with pytest.raises(ValueError,match='news_original_expiry'):ledger.issue(a,c,r)
        assert ledger.counts()['forecasts']==0


@pytest.mark.parametrize('kind',['generated_before_evidence','consumer_before_generated','ack_retimestamped',
    'consumer_after_pair','future_source','missing_clock','boolean_clock','missing_hash','invalid_hash'])
def test_news_source_consumer_order_and_identity_fail_closed(opened,kind):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    if kind=='generated_before_evidence':c['news_generated_epoch']=c['news_evidence_epoch']-1
    elif kind=='consumer_before_generated':c['news_first_observed_epoch']=c['news_available_epoch']=c['news_generated_epoch']-1
    elif kind=='ack_retimestamped':c['news_available_epoch']+=.1
    elif kind=='consumer_after_pair':c['news_first_observed_epoch']=c['news_available_epoch']=c['first_observed_epoch']+.1
    elif kind=='future_source':c['news_evidence_epoch']=clock.value+1
    elif kind=='missing_clock':c.pop('news_generated_epoch')
    elif kind=='boolean_clock':c['news_generated_epoch']=True
    elif kind=='missing_hash':c.pop('news_capture_sha256')
    elif kind=='invalid_hash':c['news_capture_sha256']='unsealed'
    r.update({key:c[key] for key in (*ledger_module.NEWS_CLOCK_FIELDS,'news_capture_sha256') if key in c})
    with pytest.raises(ValueError):ledger.issue(a,c,r)
    assert ledger.counts()['forecasts']==0


@pytest.mark.parametrize('kind',['missing','future_availability','feature_after_label','unmature_label','boolean'])
def test_training_news_is_point_in_time_and_labels_are_mature(opened,kind):
    ledger,clock,_=opened;a,_,c,r=ready_attempt(ledger,clock)
    d=r['predictions'][ledger.contract['family']]['diagnostics']
    if kind=='missing':d.pop('training_news_available_max_epoch')
    elif kind=='future_availability':d['training_news_available_max_epoch']=d['training_news_feature_cutoff_max_epoch']+1
    elif kind=='feature_after_label':d['training_news_feature_cutoff_max_epoch']=d['training_label_maturity_max_epoch']+1
    elif kind=='unmature_label':d['training_label_maturity_max_epoch']=c['max_bar_close_epoch']+1
    elif kind=='boolean':d['training_news_available_max_epoch']=True
    with pytest.raises(ValueError):ledger.issue(a,c,r)
    assert ledger.counts()['forecasts']==0


def test_shared_news_clocks_and_model_ablation_survive_evaluation(opened):
    ledger,clock,_=opened;a,ref,c,r=ready_attempt(ledger,clock)
    diag=r['predictions'][ledger.contract['family']]['diagnostics']
    diag.update(fitted_joint={'coefficients':[1,2]},matched_price_only_expected_pips=1.3,
                neutral_news_ablation_expected_pips=1.2,news_ablation_difference_pips=.8)
    identity=ledger.issue(a,c,r,after_commit=clock.advance)
    clock.advance();ledger.consume();clock.advance();quote(ledger,clock);ledger.settle()
    clock.value=ref['market_epoch']+3600;quote(ledger,clock,bid='1.1010',ask='1.1012');ledger.settle()
    data,protocol=ledger.export_evaluation();decision=data['decisions'][0]
    assert decision['news_capture_sha256']==decision['forecasts'][0]['news_capture_sha256']==c['news_capture_sha256']
    score=evaluate(data,protocol)['decisions'][0]['scores'][ledger.contract['family']]
    assert score['diagnostics']==diag
    assert score['news_provenance']['news_evidence_epoch']==c['news_evidence_epoch']


@pytest.mark.parametrize('instrument,pip_size',[('EUR_USD',.0001),('EUR_JPY',.01),('EUR_HUF',.01)])
def test_real_joint_capture_fit_publication_and_exact_h1_score(tmp_path,instrument,pip_size):
    from test_oanda_causal_forecast_inputs_joint_news_v3 import make_joint_capture
    import oanda_causal_forecast_inputs_joint_news_v3 as inputs
    capture,descriptor,now=make_joint_capture(tmp_path/'sources',instrument=instrument,pip_size=pip_size)
    contract=contract_fixture(instrument,pip_size)
    contract['numeric_model_source_sha256']=capture['model_source_sha256']
    contract['dependency_versions']=capture['dependency_versions']
    for target in (contract,contract['evaluation_protocol']):
        target['model_version']='sha256:'+capture['model_source_sha256']
        target['feature_version']='sha256:'+sha256(Path(inputs.__file__).read_bytes()).hexdigest()
    clock=Clock(now-1)
    ledger=CausalForecastLedger(tmp_path/'new_joint.sqlite',contract,clock=clock,activate=True)
    try:
        clock.value=now+1;reference=quote(ledger,clock)
        attempt=ledger.begin_attempt(int(clock.value//900),reference)
        clock.advance();result=inputs.compute_predictions(capture,news_capture=descriptor,clock=clock)
        assert result['status']=='ready',result['reasons']
        clock.advance();identity=ledger.issue(attempt,capture,result,after_commit=clock.advance)
        clock.advance();ledger.consume();clock.advance();entry=quote(ledger,clock);ledger.settle()
        clock.value=reference['market_epoch']+3600;target=quote(ledger,clock,bid='1.1008',ask='1.1010');ledger.settle()
        data,protocol=ledger.export_evaluation();report=evaluate(data,protocol)
        assert ledger.counts()['forecasts']==ledger.counts()['publication']==ledger.counts()['consumption']==ledger.counts()['outcomes']==1
        assert report['coverage']['scored_decisions']==1
        scored=report['decisions'][0]
        assert scored['decision_id']==identity and scored['target_epoch']==reference['market_epoch']+3600
        assert scored['entry']['quote_id']==entry['quote_id'] and scored['target']['quote_id']==target['quote_id']
        arm=data['decisions'][0]['forecasts'][0];score=scored['scores'][ledger.contract['family']]
        assert arm['news_capture_sha256']==descriptor['news_capture_sha256']
        assert arm['input_source_observed_epoch']==capture['first_observed_epoch']
        assert score['diagnostics']['fitted_joint']==result['predictions'][ledger.contract['family']]['diagnostics']['fitted_joint']
        assert score['news_provenance']['training_label_maturity_max_epoch']<=capture['max_bar_close_epoch']
    finally:ledger.close()


def test_real_shared_evidence_tamper_between_fit_and_issue_cannot_publish(tmp_path):
    from test_oanda_causal_forecast_inputs_joint_news_v3 import make_joint_capture
    import oanda_causal_forecast_inputs_joint_news_v3 as inputs
    capture,descriptor,now=make_joint_capture(tmp_path/'sources')
    contract=contract_fixture();contract['numeric_model_source_sha256']=capture['model_source_sha256']
    contract['dependency_versions']=capture['dependency_versions'];clock=Clock(now-1)
    ledger=CausalForecastLedger(tmp_path/'new_joint.sqlite',contract,clock=clock,activate=True)
    try:
        clock.value=now+1;attempt=ledger.begin_attempt(int(clock.value//900),quote(ledger,clock))
        clock.advance();result=inputs.compute_predictions(capture,clock=clock)
        assert result['status']=='ready'
        path=Path(descriptor['news_capture_path']);path.write_bytes(b'corrupted retained news evidence')
        clock.advance()
        with pytest.raises((ValueError,OSError)):ledger.issue(attempt,capture,result)
        assert ledger.counts()['forecasts']==ledger.counts()['publication']==0
    finally:ledger.close()


def test_original_joint_ledger_refused_before_any_writer(tmp_path):
    from oanda_causal_forecast_ledger_joint_news_v1 import CausalForecastLedger as OldLedger
    from test_oanda_causal_forecast_ledger_joint_news_v1 import contract_fixture as old_contract
    path=tmp_path/'old_joint.sqlite'
    old=OldLedger(path,old_contract(),clock=Clock(),activate=True);old.close()
    before=path.read_bytes()
    with pytest.raises(ValueError,match='immutable_contract_mismatch'):
        CausalForecastLedger(path,contract_fixture(),clock=Clock(),activate=True)
    assert path.read_bytes()==before


@pytest.mark.parametrize('scope',['capture','result'])
@pytest.mark.parametrize('field',['can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible'])
@pytest.mark.parametrize('bad',[True,'false',None])
def test_new_capture_and_result_require_explicit_false_authority(tmp_path,scope,field,bad):
    clock=Clock();ledger=CausalForecastLedger(tmp_path/'fresh.sqlite',contract_fixture(),clock=clock,activate=True)
    try:
        attempt,_,capture,result=ready_attempt(ledger,clock)
        (capture if scope=='capture' else result)[field]=bad
        with pytest.raises(ValueError,match='inert_flags'):ledger.issue(attempt,capture,result)
        assert ledger.counts()['forecasts']==ledger.counts()['publication']==0
    finally:ledger.close()


def test_dispatch_uses_new_adapter_without_mutating_old(monkeypatch):
    import oanda_causal_forecast_inputs_joint_news_v1 as old
    import oanda_causal_forecast_inputs_joint_news_v3 as new
    seen=[];original=old.validate_capture
    monkeypatch.setattr(new,'validate_capture',lambda *args,**kwargs:seen.append((args,kwargs)))
    ledger_module.validate_input_capture({'sentinel':True},instrument='EUR_USD',pip_size=.0001)
    assert seen==[(({'sentinel':True},),{'instrument':'EUR_USD','pip_size':.0001})]
    assert old.validate_capture is original
