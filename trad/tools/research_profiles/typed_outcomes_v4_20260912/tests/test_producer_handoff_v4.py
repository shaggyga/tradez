"""Real typed producer → existing-loop successor, fake estimators only."""
import copy,json
import pytest
from oanda_signal_probability_semantics_v2 import legacy_directional_point
from oanda_typed_signal_feed_adapter_v2 import normalize_forecast_v2
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4
from oanda_outcome_clock_v2 import iso,build_market_quote_snapshot
from outcome_shadow_profile_v4 import normalize_candidate,run_typed_source_profile
from test_candidate_semantics_v2 import make_worker,snapshot
from test_nullable_profile_v4 import T,Clock,candidate,inputs,quotes,run,rows,admitted

def run_wrapped(root,clock,producer,input_reader,source='fictional-model-gap-source'):
    return run_typed_source_profile(root,source=source,producer=producer,input_reader=input_reader,
        quote_reader=lambda:quotes(clock),clock=clock,sleep=lambda _:None,max_delay_sec=10,max_quote_age_sec=15)

def test_real_typed_producer_and_nullable_loop_preserve_source_and_target(tmp_path):
    worker,_=make_worker();source=snapshot();source['instruments']['EUR_USD']['quote']['time']=iso(T)
    root=tmp_path/'p';clock=Clock();original=[]
    def produce(data):
        result=worker.forecast(data);original.extend(copy.deepcopy(result));clock.now=T+2;return result
    state=run_wrapped(root,clock,produce,lambda:source)
    assert state['recorded_prediction_points']==1 and len(original)==1
    prediction=rows(root,'predictions')[0]
    assert prediction['candidate_id'].startswith('typed-v2-') and prediction['candidate_id']!=original[0]['id']
    assert prediction['generated_epoch']==T and prediction['target_epoch']==T+300 and prediction['forecast_observed_epoch']==T+2
    assert json.loads(prediction['source_point_json'])==original[0]['forecast_curve']['300']
    metadata=original[0]['producer_metadata']
    assert metadata['input_feature_contract']=='inherited_feature_row_training_live_parity_unverified'
    assert metadata['input_feature_training_live_parity_verified'] is False
    assert metadata['execution_microstructure_used_by_structural_model'] is None
    assert metadata['structural_microstructure_use_status']=='unknown_unverified_artifact_feature_selection'
    clock.now=T+300;run_wrapped(root,clock,lambda _:[],lambda:source)
    terminal=rows(root,'outcomes')[0]
    assert terminal['candidate_id']==prediction['candidate_id'] and terminal['brier'] is None
    receipt=json.loads(next((root/'typed_signals/forecast_receipts').glob('*.json')).read_text())
    assert receipt['identity']['source_forecast']['producer_metadata']==metadata
    assert receipt['identity']['source_forecast']['original_producer_id']==original[0]['id']

def test_delayed_producer_output_after_target_is_refused_without_retiming(tmp_path):
    root=tmp_path/'p';clock=Clock()
    def producer(_):clock.now=T+61;return [candidate()]
    # Longer quote age isolates target observation ordering from quote staleness.
    from outcome_shadow_profile_v4 import run_profile
    state=run_profile(root,'typed_signals',producer=producer,input_reader=lambda:inputs(),quote_reader=lambda:quotes(clock),
        clock=clock,sleep=lambda _:None,max_delay_sec=10,max_quote_age_sec=120)
    assert state['recorded_prediction_points']==0 and state['errors']==['forecast_observed_after_first_target']
    assert rows(root,'predictions')==[]

def test_duplicate_intake_retains_first_observed_clock(tmp_path):
    root=tmp_path/'p';clock=Clock(T+2);assert run(root,clock)['recorded_prediction_points']==1
    path=next((root/'typed_signals/forecast_receipts').glob('*.json'));original=path.read_bytes()
    clock.now=T+3;state=run(root,clock)
    assert state['recorded_prediction_points']==0 and path.read_bytes()==original
    assert rows(root,'predictions')[0]['forecast_observed_epoch']==T+2

def test_missing_pair_quote_is_not_censored_before_deadline(tmp_path):
    root=tmp_path/'p';clock=Clock();run(root,clock)
    for now in (T+59,T+60,T+65,T+70):
        clock.now=now;state=run(root,clock,producer=lambda _:[],quote_reader=lambda:{})
        assert state['pending_prediction_points']==1 and rows(root,'outcomes')==[]
    clock.now=T+71;run(root,clock,producer=lambda _:[],quote_reader=lambda:{})
    assert rows(root,'outcomes')[0]['status'].startswith('censored')

def test_declared_forecast_precision_preserves_one_millisecond_tolerance():
    raw=candidate();raw['generated_utc']=iso(T+.0005)
    normalized,_=normalize_candidate(raw,inputs(),T+.01,15)
    assert normalized['generated_epoch']==T and normalized['generated_utc']==raw['generated_utc']

def test_receipt_precision_remains_independent_and_explicit():
    source=inputs();source['generated_utc']=iso(T+.0005)
    result=build_market_quote_snapshot(source,now=T+.01,max_quote_age_sec=15)
    assert result['receipt_rejection']=='source_receipt_clocks_disagree'
    with pytest.raises(ValueError,match='forecast_entry_clock_unavailable_or_stale'):
        normalize_candidate(candidate(),source,T+.01,15)

def test_direct_registration_cannot_change_generated_origin_field(tmp_path):
    value=admitted();value['forecast_generated_epoch']=T+1
    ledger=LiveForecastLedgerV4(tmp_path/'direct_v4.sqlite')
    try:
        with pytest.raises(ValueError,match='forecast_origin_fields_disagree'):ledger.register([value],'fixture')
        assert ledger.pending_predictions==0
    finally:ledger.close()

def test_immutable_json_distinguishes_original_numeric_source_types(tmp_path):
    first=candidate(signed=1);second=candidate(signed=1.)
    assert first['forecast_curve']['60']==second['forecast_curve']['60']
    assert json.dumps(first['forecast_curve']['60'])!=json.dumps(second['forecast_curve']['60'])
    ledger=LiveForecastLedgerV4(tmp_path/'direct_v4.sqlite')
    try:
        ledger.register([admitted(first)],'fixture')
        with pytest.raises(ValueError,match='immutable_ledger'):ledger.register([admitted(second)],'fixture')
    finally:ledger.close()

@pytest.mark.parametrize('field',['predicted_signed_pips','predicted_magnitude_pips','projected_net_pips'])
def test_top_level_boolean_cannot_alias_numeric_anchor(field):
    raw=candidate();raw['forecast_curve']['60']=legacy_directional_point({'horizon_sec':60,'direction':'buy',field:1})
    raw[field]=True
    with pytest.raises(ValueError):normalize_forecast_v2(raw,source='fixture')

def test_wall_highwater_survives_restart_and_refuses_rollback(tmp_path):
    root=tmp_path/'p';clock=Clock();run(root,clock);clock.now=T+5;run(root,clock,producer=lambda _:[])
    clock.now=T+4
    with pytest.raises(ValueError,match='rollback'):run(root,clock,producer=lambda _:[])

def test_input_snapshot_is_frozen_before_producer_mutation(tmp_path):
    root=tmp_path/'p';clock=Clock()
    def producer(data):data['instruments']['EUR_USD']['quote']['bid']=9;return [candidate()]
    run(root,clock,producer=producer)
    path=next((root/'typed_signals/input_snapshots').glob('*.json'))
    assert json.loads(path.read_text())['instruments']['EUR_USD']['quote']['bid']==1.1

def test_same_candle_new_generation_gets_distinct_typed_identity_and_replay_is_duplicate(tmp_path):
    worker,_=make_worker();source=snapshot();source['instruments']['EUR_USD']['quote']['time']=iso(T)
    clock=Clock();root=tmp_path/'p';raw=[]
    def producer(data):
        result=worker.forecast(data);raw.extend(copy.deepcopy(result));return result
    assert run_wrapped(root,clock,producer,lambda:source)['recorded_prediction_points']==1
    clock.now=T+1;source=copy.deepcopy(source);source.update(generated_epoch=T+1,generated_utc=iso(T+1))
    source['instruments']['EUR_USD']['quote'].update(time=iso(T+1),bid=1.1002,ask=1.1003)
    assert run_wrapped(root,clock,producer,lambda:source)['recorded_prediction_points']==1
    assert raw[0]['id']==raw[1]['id'],'raw source candle identity is deliberately unchanged'
    predictions=rows(root,'predictions');assert len({row['candidate_id'] for row in predictions})==2
    assert {row['target_epoch'] for row in predictions}=={T+300,T+301}
    assert {row['entry_bid'] for row in predictions}=={1.1,1.1002}
    receipts={p.name:p.read_bytes() for p in (root/'typed_signals/forecast_receipts').glob('*.json')}
    clock.now=T+2
    assert run_wrapped(root,clock,lambda _:[raw[1]],lambda:source)['recorded_prediction_points']==0
    assert {p.name:p.read_bytes() for p in (root/'typed_signals/forecast_receipts').glob('*.json')}==receipts
    assert {row['forecast_observed_epoch'] for row in rows(root,'predictions')}=={T,T+1}

def test_source_name_is_pinned_in_new_cohort_policy(tmp_path):
    root=tmp_path/'p';clock=Clock()
    run_wrapped(root,clock,lambda _:[candidate()],lambda:inputs(),source='source-A')
    with pytest.raises(ValueError,match='new_cohort_required_for_changed_clock_policy'):
        run_wrapped(root,clock,lambda _:(_ for _ in ()).throw(AssertionError('must not call producer')),lambda:inputs(),source='source-B')

def test_already_normalized_typed_output_is_not_reidentified(tmp_path):
    value=normalize_forecast_v2(candidate(),source='source-A');root=tmp_path/'p';clock=Clock()
    run_wrapped(root,clock,lambda _:[value],lambda:inputs(),source='source-A')
    assert rows(root,'predictions')[0]['candidate_id']==value['id']

def test_low_level_raw_model_gap_candle_id_is_explicitly_refused():
    raw=candidate();raw['id']='model-gap-side-v2-fictional'
    with pytest.raises(ValueError,match='requires_typed_source_wrapper'):normalize_candidate(raw,inputs(),T,15)

@pytest.mark.parametrize('value',['not-a-clock',iso(T-1),iso(T+1),None])
def test_direct_registration_validates_original_entry_clock_pair(tmp_path,value):
    raw=admitted();raw['research_entry_clock']['entry_quote_time']=value
    ledger=LiveForecastLedgerV4(tmp_path/'direct_v4.sqlite')
    try:
        with pytest.raises(ValueError):ledger.register([raw],'fixture')
        assert ledger.pending_predictions==0
    finally:ledger.close()

def test_hold_is_non_directional_with_unavailable_action_outcomes(tmp_path):
    root=tmp_path/'p';clock=Clock();run(root,clock,producer=lambda _:[candidate(direction='hold')])
    clock.now=T+60;run(root,clock,producer=lambda _:[])
    row=rows(root,'outcomes')[0]
    assert row['direction']=='hold'
    for field in ('direction_correct','executable_profitable','executable_net_pips','brier'):assert row[field] is None
