"""Source wrapper and exact shared-observation proof tests; owned fixtures only."""
import json
import pytest
import outcome_shadow_profile_v4 as profile
from oanda_forecast_ledger_v4 import LiveForecastLedgerV4
from oanda_outcome_clock_v2 import iso
from test_nullable_profile_v4 import T,Clock,inputs,candidate,run,quotes,rows

@pytest.fixture
def tmp_path(tmp_path_factory):return tmp_path_factory.mktemp('t')

def wrapped(root,clock,raw,source_inputs):
    return profile.run_typed_source_profile(root,source='fixed-source',producer=lambda _:[raw],
        input_reader=lambda:source_inputs,quote_reader=lambda:quotes(clock),
        clock=clock,sleep=lambda _:None,max_delay_sec=10,max_quote_age_sec=15)

def test_same_original_candle_id_has_distinct_generations_and_exact_duplicate_is_stable(tmp_path):
    root=tmp_path/'p';clock=Clock();raw1=candidate(identifier='model-gap-side-v2-fixture');source1=inputs()
    source1['instruments']['EUR_USD']['timeframe_features']['M1']['candle_time']=iso(T-60)
    assert wrapped(root,clock,raw1,source1)['recorded_prediction_points']==1
    clock.now=T+1;raw2=candidate(t=T+1,identifier=raw1['id']);source2=inputs(t=T+1)
    source2['instruments']['EUR_USD']['timeframe_features']['M1']['candle_time']=iso(T-60)
    assert wrapped(root,clock,raw2,source2)['recorded_prediction_points']==1
    clock.now=T+2;assert wrapped(root,clock,raw2,source2)['recorded_prediction_points']==0
    pending=rows(root,'predictions')
    assert len(pending)==2 and len({r['candidate_id'] for r in pending})==2
    assert {r['target_epoch'] for r in pending}=={T+60,T+61}
    receipts=[json.loads(p.read_text()) for p in (root/'typed_signals/forecast_receipts').glob('*.json')]
    assert {r['identity']['source_forecast']['original_producer_id'] for r in receipts}=={raw1['id']}

def test_hold_retains_market_outcome_without_selected_action_or_brier(tmp_path):
    root=tmp_path/'p';clock=Clock();run(root,clock,producer=lambda _:[candidate(direction='hold')])
    clock.now=T+60;run(root,clock,producer=lambda _:[])
    actual=rows(root,'outcomes')[0]
    assert actual['direction']=='hold' and actual['actual_up']==1
    assert all(actual[k] is None for k in ['probability_up','brier','direction_correct','executable_net_pips','executable_profitable'])

def test_shared_observation_proof_refuses_equal_python_but_distinct_json_source(tmp_path):
    source=tmp_path/'s';clock=Clock();run(source,clock,producer=lambda _:[candidate(signed=1)])
    envelope=next(profile.read_profile_forecasts(source))
    changed=candidate(signed=1.0)
    assert changed==envelope['receipt']['identity']['source_forecast']
    assert profile.canonical(changed)!=profile.canonical(envelope['receipt']['identity']['source_forecast'])
    paths=profile.prepare_paths(tmp_path/'d','shared_intake')
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10)
    ledger=LiveForecastLedgerV4(paths['ledger'])
    try:
        with pytest.raises(ValueError,match='observation_proof'):
            profile.record_forecast(paths,ledger,changed,envelope['inputs'],observed_epoch=T,
                max_quote_age_sec=15,observation_receipt=envelope['receipt'],intake_epoch=T+1)
        assert not list(paths['receipts'].glob('*.json'))
    finally:ledger.close()

def test_pinned_producer_identity_cannot_change_only_json_numeric_type(tmp_path):
    paths=profile.prepare_paths(tmp_path/'p','typed_signals')
    profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10,producer_identity={'release':1})
    with pytest.raises(ValueError,match='new_cohort'):
        profile.pin_run_policy(paths,max_quote_age_sec=15,max_delay_sec=10,producer_identity={'release':1.0})
