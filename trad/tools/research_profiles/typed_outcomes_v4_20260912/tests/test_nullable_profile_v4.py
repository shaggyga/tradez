"""Actual successor loop and SQLite, restricted to owned fictional fixtures."""
import ast,copy,json,sqlite3
from pathlib import Path
import pytest
from oanda_signal_probability_semantics_v2 import CONTRACT,COHORT,legacy_directional_point,side_classifier_point,POSITIVE_HELPER_SHA256
from oanda_typed_signal_feed_adapter_v2 import normalize_forecast_v2
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4,LEDGER_CONTRACT
from oanda_outcome_clock_v2 import iso
from outcome_shadow_profile_v4 import run_profile,run_shared_intake,read_profile_forecasts,normalize_candidate,prepare_paths,owned_role_lock
T=1800000000.

class Clock:
    def __init__(self,t=T):self.now=t
    def __call__(self):return self.now

def inputs(t=T,pair='EUR_USD'):
    return {'snapshot_id':'fictional-input','generated_epoch':t,'generated_utc':iso(t),
        'instruments':{pair:{'quote':{'bid':1.1,'ask':1.1001,'time':iso(t)},
            'timeframe_features':{'M1':{'pip':.0001,'candle_time':iso(t-60)}}}}}

def candidate(t=T,pair='EUR_USD',identifier='source-original-id',direction='buy',signed=None):
    point=legacy_directional_point({'horizon_sec':60,'direction':direction,'predicted_signed_pips':signed})
    return {'id':identifier,'generated_epoch':t,'generated_utc':iso(t),'model_id':'fictional','family':'fixture',
        'input_timeframe':'M1','instrument':pair,'bid':1.1,'ask':1.1001,'pip':.0001,
        'signal_semantics_contract':CONTRACT,'cohort_id':COHORT,'research_only':True,'account_eligible':False,
        'direction':point['direction'],'forecast_curve':{'60':point}}

def side_candidate(long=.7,short=.3,target='target_profitable'):
    raw=candidate();mapping={'estimator_classes':[0,1],'calibrator_classes':None,'positive_class':1,
        'helper_sha256':POSITIVE_HELPER_SHA256,'historical_mapping_verified':False}
    point=side_classifier_point(horizon_sec=60,target=target,long_probability=long,short_probability=short,
        metrics={'brier':.1,'events':20,'side_rows':40},artifact_sha256='a'*64,report_sha256='b'*64,
        calibrator_applied=False,class_mapping=mapping)
    raw.update(direction=point['direction'],forecast_curve={'60':point},producer_metadata={
        'artifact_sha256':'a'*64,'source_report_sha256':'b'*64,'source_target':target})
    return raw

def quotes(clock,event=None,receipt=None,pair='EUR_USD',bid=1.1003):
    return {'generated_utc':iso(clock() if receipt is None else receipt),'quotes':{pair:{
        'bid':bid,'ask':bid+.0001,'time':iso(clock() if event is None else event)}}}

def run(root,clock,producer=None,input_reader=None,quote_reader=None,**kw):
    return run_profile(root,'typed_signals',producer=producer or (lambda _:[candidate()]),
        input_reader=input_reader or (lambda:inputs()),quote_reader=quote_reader or (lambda:quotes(clock)),
        clock=clock,sleep=lambda _:None,max_delay_sec=10,max_quote_age_sec=15,**kw)

def rows(root,table,role='typed_signals'):
    connection=sqlite3.connect((root/role/'forecasts_v4.sqlite').as_uri()+'?mode=ro',uri=True)
    try:
        cursor=connection.execute('SELECT * FROM '+table);return [dict(zip([x[0] for x in cursor.description],r)) for r in cursor.fetchall()]
    finally:connection.close()

def admitted(raw=None,observed=T):
    value,_=normalize_candidate(raw or candidate(),inputs(),observed,15)
    return value

def test_actual_loop_null_roundtrip_and_zero_brier_denominator(tmp_path):
    root=tmp_path/'profile';clock=Clock();raw=side_candidate()
    assert run(root,clock,producer=lambda _:[raw])['recorded_prediction_points']==1
    pending=rows(root,'predictions')[0]
    assert pending['candidate_id']==raw['id'] and pending['probability_up'] is None
    assert json.loads(pending['source_point_json'])==raw['forecast_curve']['60']
    clock.now=T+60;assert run(root,clock,producer=lambda _:[])['pending_prediction_points']==0
    result=rows(root,'outcomes')[0]
    assert result['target_epoch']==T+60 and result['quote_epoch']==T+60 and result['brier'] is None
    assert result['probability_up'] is None and result['executable_net_pips']==pytest.approx(2)
    ledger=LiveForecastLedgerV4(root/'typed_signals/forecasts_v4.sqlite')
    try:summary=ledger.summary(now=T+60)
    finally:ledger.close()
    cell=summary['cells'][0]
    assert cell['n']==cell['direction_n']==cell['executable_n']==1
    assert cell['midpoint_brier_n']==0 and cell['midpoint_brier'] is None
    assert cell['signed_error_n']==cell['magnitude_error_n']==0

@pytest.mark.parametrize('which',['flat','unavailable','side_tie'])
def test_no_direction_is_not_scored_as_sell_or_trade_success(tmp_path,which):
    root=tmp_path/'profile';clock=Clock()
    raw=side_candidate(.5,.5) if which=='side_tie' else candidate(direction=which if which=='flat' else '',signed=None)
    run(root,clock,producer=lambda _:[raw]);clock.now=T+60;run(root,clock,producer=lambda _:[])
    row=rows(root,'outcomes')[0]
    for field in ('direction_correct','executable_profitable','executable_net_pips','brier'):assert row[field] is None
    assert row['actual_up']==1 and row['signed_mid_move_pips']==pytest.approx(3)

@pytest.mark.parametrize('bad',[.5,True,0.,float('nan')])
def test_nonnull_midpoint_p_rejected_by_intake_and_direct_register(tmp_path,bad):
    raw=candidate();raw['forecast_curve']['60']['probability_up']=bad
    with pytest.raises(ValueError):normalize_candidate(raw,inputs(),T,15)
    value=admitted();value['forecast_curve']['60']['probability_up']=bad
    ledger=LiveForecastLedgerV4(tmp_path/'owned_v4.sqlite')
    try:
        with pytest.raises(ValueError):ledger.register([value],'fixture')
        assert ledger.pending_predictions==0
    finally:ledger.close()

def test_changed_target_or_source_point_under_id_is_immutable(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'owned_v4.sqlite')
    try:
        first=admitted(side_candidate());assert ledger.register([first],'source')==1
        assert ledger.register([first],'source')==0
        with pytest.raises(ValueError,match='immutable_ledger'):ledger.register([admitted(side_candidate(target='target_best_side'))],'source')
        assert ledger.pending_predictions==1
    finally:ledger.close()

def test_mixed_valid_corrupt_batch_never_partially_registers(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'owned_v4.sqlite')
    try:
        good=admitted();bad=copy.deepcopy(good);bad['id']='bad';bad['forecast_curve']['60']['account_eligible']=True
        with pytest.raises(ValueError):ledger.register([good,bad],'fixture')
        assert ledger.connection.execute('SELECT COUNT(*) FROM predictions').fetchone()==(0,)
    finally:ledger.close()

def test_maturity_outcome_and_prediction_delete_are_one_transaction(tmp_path,monkeypatch):
    from oanda_outcome_clock_v2 import build_market_quote_snapshot
    ledger=LiveForecastLedgerV4(tmp_path/'owned_v4.sqlite')
    try:
        ledger.register([admitted()],'fixture');clock=Clock(T+60)
        snap=build_market_quote_snapshot(quotes(clock),now=clock(),max_quote_age_sec=15)
        def fail(_):raise RuntimeError('owned injected post-outcome failure')
        with monkeypatch.context() as patch:
            patch.setattr(ledger,'_delete_prediction_rowids',fail)
            with pytest.raises(RuntimeError):ledger.mature(snap,10,now=clock())
        assert ledger.connection.execute('SELECT COUNT(*) FROM outcomes').fetchone()==(0,)
        assert ledger.connection.execute('SELECT COUNT(*) FROM predictions').fetchone()==(1,)
        assert ledger.mature(snap,10,now=clock())['matured']==1
    finally:ledger.close()

@pytest.mark.parametrize('which',['missing','stale','future','empty','error'])
def test_no_forecast_or_blocked_cycle_still_censors(tmp_path,which):
    root=tmp_path/'profile';clock=Clock();run(root,clock);clock.now=T+71
    def produce(_):
        if which=='error':raise RuntimeError('fictional')
        return []
    source={} if which=='missing' else inputs(T-1000 if which=='stale' else T+100 if which=='future' else T)
    result=run(root,clock,producer=produce,input_reader=lambda:source,quote_reader=lambda:{})
    row=rows(root,'outcomes')[0]
    assert result['pending_prediction_points']==0 and row['status'].startswith('censored')
    for field in ('actual_up','direction_correct','executable_profitable','executable_net_pips','signed_mid_move_pips','brier','signed_pip_error'):assert row[field] is None

def test_post_work_capture_matures_pending_even_on_model_error(tmp_path):
    root=tmp_path/'profile';clock=Clock();run(root,clock);clock.now=T+59;seen=[]
    def quote_reader():seen.append(clock());return quotes(clock)
    def producer(_):clock.now=T+62;raise RuntimeError('fictional expensive model')
    result=run(root,clock,producer=producer,input_reader=lambda:inputs(T+59),quote_reader=quote_reader)
    assert seen==[T+59,T+62] and result['after_work']['maturity']['matured']==1

def test_pretarget_quote_pending_then_timely_pair_quote_scores(tmp_path):
    root=tmp_path/'profile';clock=Clock();run(root,clock);clock.now=T+61
    assert run(root,clock,producer=lambda _:[],quote_reader=lambda:quotes(clock,event=T+59))['pending_prediction_points']==1
    run(root,clock,producer=lambda _:[],quote_reader=lambda:quotes(clock,event=T+60))
    assert rows(root,'outcomes')[0]['quote_epoch']==T+60

@pytest.mark.parametrize('kind',['stale','future_quote','future_receipt','missing'])
def test_invalid_postwork_capture_refuses_new_forecast(tmp_path,kind):
    root=tmp_path/'profile';clock=Clock();reads=[]
    def read():
        reads.append(clock())
        if len(reads)==1:return quotes(clock)
        if kind=='missing':return {}
        return quotes(clock,event=T+100 if kind=='future_quote' else T-20 if kind=='stale' else T,
                      receipt=T+100 if kind=='future_receipt' else T)
    result=run(root,clock,quote_reader=read)
    assert result['recorded_prediction_points']==0 and rows(root,'predictions')==[]

def test_original_entry_and_clock_survive_market_change_and_restart(tmp_path):
    root=tmp_path/'profile';clock=Clock();reads=[]
    def read():reads.append(1);return quotes(clock,bid=1.2 if len(reads)>1 else 1.1003)
    def producer(_):clock.now=T+5;return [candidate()]
    run(root,clock,producer=producer,quote_reader=read)
    row=rows(root,'predictions')[0]
    assert row['entry_bid']==1.1 and row['generated_epoch']==T and row['target_epoch']==T+60
    path=next((root/'typed_signals/forecast_receipts').glob('*.json'));original=path.read_bytes()
    clock.now=T+60;run(root,clock,producer=lambda _:[]);clock.now=T+61;run(root,clock,producer=lambda _:[])
    assert path.read_bytes()==original and rows(root,'predictions')==[] and len(rows(root,'outcomes'))==1

def test_shared_late_intake_preserves_original_observation_then_censors(tmp_path):
    source=tmp_path/'source';destination=tmp_path/'destination';clock=Clock();run(source,clock)
    clock.now=T+71
    result=run_shared_intake(destination,receipt_reader=lambda:read_profile_forecasts(source),quote_reader=lambda:{},
        clock=clock,sleep=lambda _:None,max_delay_sec=10)
    row=rows(destination,'outcomes','shared_intake')[0]
    assert row['forecast_observed_epoch']==T and row['target_epoch']==T+60
    assert row['candidate_id']=='source-original-id' and row['brier'] is None
    assert result['pending_prediction_points']==0

def test_wrong_schema_refused_without_mutating_retained_v3_db(tmp_path):
    source=Path(__file__).resolve().parents[1]/'original_v3/oanda_forecast_ledger_v3.py'
    tree=ast.parse(source.read_text());schema=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SCHEMA_SQL' for t in n.targets))
    path=tmp_path/'old_v4.sqlite';connection=sqlite3.connect(path);connection.executescript(schema)
    connection.execute('INSERT INTO outcome_clock_contract VALUES(1,?,0)',('per_pair_nullable_forecast_outputs_v3_20260912',));connection.commit();connection.close()
    before=path.read_bytes()
    with pytest.raises(ValueError,match='legacy_or_unknown'):LiveForecastLedgerV4(path)
    assert path.read_bytes()==before and not Path(str(path)+'-wal').exists()

def test_role_lock_still_prevents_competing_loop(tmp_path):
    root=tmp_path/'profile';paths=prepare_paths(root,'typed_signals')
    with owned_role_lock(paths['lock']):
        with pytest.raises(ValueError,match='already_owned'):run(root,Clock())
    assert not paths['ledger'].exists()

def test_summary_keeps_native_pips_within_pair(tmp_path):
    ledger=LiveForecastLedgerV4(tmp_path/'owned_v4.sqlite')
    from oanda_outcome_clock_v2 import build_market_quote_snapshot
    try:
        a=admitted();raw=candidate(pair='GBP_USD',identifier='second');b,_=normalize_candidate(raw,inputs(pair='GBP_USD'),T,15)
        ledger.register([a,b],'fixture');clock=Clock(T+60);payload=quotes(clock)
        payload['quotes']['GBP_USD']=quotes(clock,pair='GBP_USD')['quotes']['GBP_USD']
        ledger.mature(build_market_quote_snapshot(payload,now=clock(),max_quote_age_sec=15),10,now=clock())
        summary=ledger.summary(now=clock());assert {row['instrument'] for row in summary['cells']}=={'EUR_USD','GBP_USD'}
    finally:ledger.close()

def test_top_numeric_alias_is_rejected_and_legacy_magnitude_remains_optional(tmp_path):
    raw=candidate(signed=1);raw['predicted_signed_pips']=True
    with pytest.raises(ValueError):normalize_forecast_v2(raw,source='fixture')
    clock=Clock();root=tmp_path/'profile';run(root,clock,producer=lambda _:[candidate(signed=1)])
    clock.now=T+60;run(root,clock,producer=lambda _:[])
    row=rows(root,'outcomes')[0]
    assert row['signed_pip_error']==pytest.approx(-2) and row['magnitude_pip_error'] is None
