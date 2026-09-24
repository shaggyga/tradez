import copy
from types import SimpleNamespace
import pytest
from contracts import fingerprint
from test_chronological_layer_v2 import setup as layer_setup
from chronological_layer_v2 import frame
from chronological_native_v2 import build_frame


def setup():
    rows,labels,obs,frozen,c=layer_setup();t=500000;cohort={'name':'example','target':t+900,'policy_origins':[t]}
    saved=frame(15,t,rows,obs,labels,frozen,c)
    values={r['record_id']:{'signed':{'ridge':r['ridge_prediction_bps'],'recovered_hgb':r['recovered_hgb_prediction_bps']},'absolute':dict(r['magnitude_prediction_bps'])} for r in rows if r['decision_epoch']==t}
    predictor=SimpleNamespace(available_epoch=0,predict=lambda *a:copy.deepcopy(values))
    native=SimpleNamespace(prepare=lambda pred,*a:{'prediction':pred},verify=lambda:None)
    market={'universe':c['universe'],'rows':[{'instrument':p,'price_epoch':t,'status':'valid_candle_close_pair'} for p in c['universe']],'metadata':{p:{} for p in c['universe']}}
    c['resources']={'fresh_inference_and_fit_seconds':1,'native_frame_seconds':2}
    return [cohort,t,predictor,native,obs,rows,labels,saved,frozen,market,c]


def test_fresh_qualification_retains_values_but_has_new_identity_and_clock():
    args=setup();before=copy.deepcopy(args[7]);result,timing=build_frame(*args)
    assert args[7]==before and len(result['predictions'])==280 and len(result['coverage'])==280
    old={p['forecast_id']:p for p in before['predictions']}
    for pred in result['predictions']:
        original=old[pred['diagnostic_forecast_id']]
        assert pred['forecast_id']!=original['forecast_id'] and pred['prediction_bps']==original['prediction_bps']
        assert pred['available_epoch']==500002 and pred['production_available_epoch'] is None
        if pred['variant']!='raw_unrestricted':assert original['available_epoch']==500031
    assert timing['qualification_regressions']==4 and not timing['scientific_parameters_changed']


def test_changed_fresh_base_prediction_refuses():
    args=setup();original=args[2].predict
    def wrong(*a):
        result=original(*a);next(iter(result.values()))['signed']['ridge']+=1;return result
    args[2].predict=wrong
    with pytest.raises(ValueError,match='base_values'):build_frame(*args)


def test_rehashed_saved_expanding_coefficients_refuse():
    args=setup();s=args[7]['expanding_snapshot'];s['parameters']['ridge']['signed_only']['coefficient'][0]+=1;s['layer_id']=fingerprint({k:v for k,v in s.items() if k!='layer_id'})
    with pytest.raises(ValueError,match='expanding_reconstruction'):build_frame(*args)


@pytest.mark.parametrize('field,value',[('model_id','x'*64),('instrument','WRONG'),('available_epoch',999999),('target_epoch',500901)])
def test_rehashed_diagnostic_authority_changes_refuse(field,value):
    args=setup();p=args[7]['predictions'][0];p[field]=value;p['forecast_id']=fingerprint({k:v for k,v in p.items() if k!='forecast_id'})
    with pytest.raises(ValueError,match='diagnostic_binding'):build_frame(*args)


def test_missing_reference_keeps_all_coverage_and_drops_only_native_admission():
    args=setup();args[9]['rows'][0]['status']='missing_exact_bar';result,_=build_frame(*args)
    assert len(result['coverage'])==280 and len(result['predictions'])==266
    blocked=[r for r in result['coverage'] if r['reason']=='reference_missing_exact_bar'];assert len(blocked)==14 and all(r['source_reason']=='eligible' for r in blocked)


def test_future_current_outcomes_cannot_change_native_values():
    args=setup();before,_=build_frame(*args)
    for row in args[5]:
        if row['decision_epoch']>=args[1]:args[6][row['record_id'],row['target_id']]['value']=float('nan')
    after,_=build_frame(*args);assert after==before


@pytest.mark.parametrize('clock,reason',[([0.,1.01],'fresh_fit_slot'),([0.,.5,2.01],'complete_slot')])
def test_timing_failure_refuses_native_qualification(monkeypatch,clock,reason):
    args=setup();ticks=iter(clock);monkeypatch.setattr('chronological_native_v2.time',SimpleNamespace(monotonic=lambda:next(ticks)))
    with pytest.raises(ValueError,match=reason):build_frame(*args)


def test_duplicate_reference_refuses():
    args=setup();args[9]['rows'].append(dict(args[9]['rows'][0]))
    with pytest.raises(ValueError,match='reference_slots'):build_frame(*args)


@pytest.mark.parametrize('mode',['frozen_prefix','expanding_prefix'])
def test_batch_application_is_exact_original_row_arithmetic(mode):
    from magnitude_layer_v2 import apply_snapshot,apply_snapshot_batch
    rows,labels,obs,snapshot,c=layer_setup();current=[r for r in rows if r['decision_epoch']==500000]
    assert apply_snapshot_batch(current,snapshot,mode)==[p for r in current for p in apply_snapshot(r,snapshot,mode)]


def test_batch_retains_training_membership_and_duplicate_refusals():
    from magnitude_layer_v2 import apply_snapshot_batch
    rows,labels,obs,snapshot,c=layer_setup();current=[r for r in rows if r['decision_epoch']==500000]
    with pytest.raises(ValueError,match='duplicate_magnitude_application'):apply_snapshot_batch([current[0],current[0]],snapshot,'frozen_prefix')
    with pytest.raises(ValueError,match='future_or_current'):apply_snapshot_batch([rows[0]],snapshot,'frozen_prefix')


def test_batch_refuses_snapshot_mutation_at_end(monkeypatch):
    import magnitude_layer_v2 as m
    rows,labels,obs,snapshot,c=layer_setup();current=[r for r in rows if r['decision_epoch']==500000];original=m._apply_parameters
    def changed(row,s,mode):
        value=original(row,s,mode);s['parameters']['ridge']['signed_only']['coefficient'][0]+=1;return value
    monkeypatch.setattr(m,'_apply_parameters',changed)
    with pytest.raises(ValueError,match='changed_during_batch'):m.apply_snapshot_batch(current,snapshot,'frozen_prefix')
