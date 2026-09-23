"""Exact source cell identity and source-reported clock refusal regressions."""
import copy,json
import pytest
import oanda_typed_signal_feed_adapter_v2 as feed
from test_candidate_semantics_v2 import emitted,make_worker,snapshot
from test_candidate_contract_v3 import reload_fixture

@pytest.mark.parametrize('value',[300.9,'300',300.,True,None,0,-300])
def test_source_cell_horizon_requires_exact_integer(value):
    worker,path,_,_,raw,_=reload_fixture();report=json.loads(raw)
    report['results'][0]['holdout']['all_prediction_cell_metrics'][0]['horizon_sec']=value
    path.versions=[json.dumps(report).encode()];worker.reload()
    assert worker.runtimes=={}
    assert 'exact integer horizon' in worker.load_errors['fictional_model']

def test_duplicate_source_cell_cannot_replace_other_validation_evidence():
    worker,path,_,_,raw,_=reload_fixture();report=json.loads(raw)
    rows=report['results'][0]['holdout']['all_prediction_cell_metrics'];row=copy.deepcopy(rows[0]);row['brier']=.9;rows.append(row)
    path.versions=[json.dumps(report).encode()];worker.reload()
    assert worker.runtimes=={}
    assert 'duplicate source validation cell' in worker.load_errors['fictional_model']

@pytest.mark.parametrize('value',[None,'not-a-clock','2027-01-15T07:59:00','2027-01-15T08:00:01+00:00',True,1700000000.,'1969-12-31T00:00:00+00:00'])
def test_source_clock_rejected_before_dependency_and_model_call(value):
    worker,_=make_worker();state=snapshot();state['instruments']['EUR_USD']['timeframe_features']['M1']['candle_time']=value
    worker._dependencies=lambda:(_ for _ in ()).throw(AssertionError('must refuse before dependency/model'))
    with pytest.raises(ValueError):worker.forecast(state)
    assert worker.last_generated_epoch==0

def test_missing_source_clock_has_no_generation_or_snapshot_fallback():
    worker,_=make_worker();state=snapshot();del state['instruments']['EUR_USD']['timeframe_features']['M1']['candle_time']
    state['snapshot_id']='fallback';state['instruments']['EUR_USD']['feature_origin_utc']='2027-01-15T07:59:00+00:00'
    worker._dependencies=lambda:(_ for _ in ()).throw(AssertionError('must refuse before dependency/model'))
    with pytest.raises(ValueError,match='explicit source candle_time'):worker.forecast(state)

def test_valid_source_clock_is_bound_without_completed_feature_attestation():
    worker,_=make_worker();state=snapshot();state['instruments']['EUR_USD']['timeframe_features']['M1']['candle_time']='2027-01-15T02:59:00-05:00'
    raw=worker.forecast(state)[0];metadata=raw['producer_metadata']
    assert metadata['input_feature_origin_utc']=='2027-01-15T07:59:00+00:00'
    assert metadata['source_candle_time_supplied']=='2027-01-15T02:59:00-05:00'
    assert metadata['input_feature_completion_verified'] is False
    assert metadata['input_feature_original_availability_verified'] is False
    assert metadata['decision_identity']=='artifact_pair_timeframe_source_reported_candle_clock'

@pytest.mark.parametrize('value',[None,'not-a-clock','2027-01-15T08:00:00','2027-01-15T08:00:01+00:00'])
def test_feed_requires_exact_original_clock_pair(value):
    raw=emitted();raw['generated_utc']=value
    with pytest.raises(ValueError):feed.normalize_forecast_v2(raw,source='synthetic')

def test_feed_accepts_equivalent_aware_clock_without_changing_source_text():
    raw=emitted();raw['generated_utc']='2027-01-15T03:00:00-05:00'
    actual=feed.normalize_forecast_v2(raw,source='synthetic')
    assert actual['generated_utc']==raw['generated_utc']
    assert actual['generated_epoch']==raw['generated_epoch']
