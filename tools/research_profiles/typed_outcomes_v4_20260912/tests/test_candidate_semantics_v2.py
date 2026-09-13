"""Synthetic full candidate module plumbing. No initialization or real artifacts."""
import copy,json,math
from pathlib import Path
from types import SimpleNamespace
import numpy as np
import pandas as pd
import pytest
import oanda_signal_probability_semantics_v2 as semantics
import oanda_typed_signal_feed_adapter_v2 as feed
import oanda_model_gap_live_signal_producer_v2 as producer

class Estimator:
    def __init__(self,values=((.3,.7),(.7,.3)),classes=(0,1),mutate=False):
        self.values=np.asarray(values,dtype=float);self.classes_=np.asarray(classes);self.mutate=mutate
    def predict_proba(self,frame):
        if self.mutate:self.classes_=self.classes_[::-1]
        return self.values.copy()

class PromotionTripwire:
    def evidence(self,*args):raise AssertionError('legacy promotion must never be consulted')
    def summary(self,*args):return {'status':'synthetic_disabled'}

def make_worker(*,target='target_profitable',estimator=None,calibrator=None):
    run=producer.ArtifactRuntime(model_id='fictional_model',path=Path('never_read.joblib'),sha256='a'*64,
        artifact={'schema_version':1,'model':'fictional_model','target':target,'numeric_features':['side_sign'],
            'categorical_features':[],'estimator':estimator or Estimator(),'calibrator':calibrator,
            'execution_policy':'shadow_only'},cells=(('M1',300),),
        cell_metrics={('M1',300):{'brier':.04,'events':50,'side_rows':100}})
    worker=producer.ModelGapArtifactProducerV2.__new__(producer.ModelGapArtifactProducerV2)
    worker.runtimes={'fictional_model':run};worker.last_generated_epoch=0;worker.load_errors={}
    worker.source_report_sha256='b'*64;worker.promotion=PromotionTripwire()
    worker._dependencies=lambda:(None,np,pd)
    return worker,run

def snapshot():
    return {'generated_epoch':1800000000.,'generated_utc':'2027-01-15T08:00:00+00:00',
        'instruments':{'EUR_USD':{'quote':{'bid':1.1,'ask':1.1001},
            'timeframe_features':{'M1':{'pip':.0001,'candle_time':'2027-01-15T07:59:00+00:00'}}}}}

def emitted(**kwargs):
    worker,run=make_worker(**kwargs)
    result=worker.forecast(snapshot());assert len(result)==1
    return result[0]

@pytest.mark.parametrize('target',['target_profitable','target_best_side'])
def test_real_producer_feed_handoff_retains_target_marginals_counts_and_null_midpoint(target):
    original=emitted(target=target)
    actual=feed.normalize_forecast_v2(original,source='synthetic_fixture',policy_account_eligible=True)
    point=actual['forecast_curve']['300']
    assert point['side_probabilities']=={'long':.7,'short':.3}
    assert point['relative_side_logit_score']==pytest.approx(.8448275862068966)
    assert point['probability_target']['source_target']==target
    assert point['side_validation']['brier_rows']==100
    assert point['side_validation']['distinct_events']==50
    assert point['side_validation']['source_reported_brier']==.04
    assert point['side_validation']['midpoint_calibration_evidence'] is False
    assert point['source_input']['class_mapping']['estimator_classes']==[0,1]
    assert point['source_input']['class_mapping']['helper_sha256']==semantics.POSITIVE_HELPER_SHA256
    assert point['source_input']['class_mapping']['historical_mapping_verified'] is False
    for field in ('probability_up','calibrated_probability_up','signal_confidence','calibrated_brier','calibration_n','predicted_signed_pips','predicted_magnitude_pips'):
        assert point[field] is None
    assert actual['account_eligible'] is False and actual['research_only'] is True
    assert actual['probability_up'] is None and actual['signal_confidence'] is None
    assert actual['direction']=='buy'
    assert actual['original_producer_id']==original['id']
    assert actual['id'].startswith('typed-v2-') and original['id'].startswith('model-gap-side-v2-')
    assert json.loads(json.dumps(actual,allow_nan=False))==actual
    assert feed.entry_qualification(actual)['qualified'] is False

@pytest.mark.parametrize('estimator,calibrator,expected',[
    (Estimator(((.8,.2),(.3,.7)),classes=(1,0)),None,[.8,.3]),
    (Estimator(),Estimator(((.9,.1),(.4,.6)),classes=(1,0)),[.9,.4]),
])
def test_actual_estimator_and_calibrator_positive_class_mapping(estimator,calibrator,expected):
    point=emitted(estimator=estimator,calibrator=calibrator)['forecast_curve']['300']
    assert list(point['side_probabilities'].values())==pytest.approx(expected)
    assert point['source_input']['class_mapping']['estimator_classes']==estimator.classes_.tolist()
    assert point['source_input']['class_mapping']['calibrator_classes']==(None if calibrator is None else calibrator.classes_.tolist())

@pytest.mark.parametrize('values',[
    ((.3,.9),(.7,.3)),((.2,float('nan')),(.7,.3)),((.2,1.5),(.7,.3)),
    ((.3,.7),),((.3,.7),(.7,.3),(.4,.6)),((.3,.7,0),(.7,.3,0)),
])
def test_invalid_prediction_matrix_fails_before_pairing(values):
    with pytest.raises(ValueError):emitted(estimator=Estimator(values))

@pytest.mark.parametrize('classes',[(1,1),(1,2),('0','1'),(False,True),(0.,1.)])
def test_class_mapping_requires_binary_integer_class_labels(classes):
    with pytest.raises(ValueError):emitted(estimator=Estimator(classes=classes))

@pytest.mark.parametrize('which',['estimator','calibrator'])
def test_class_order_cannot_change_during_inference(which):
    kwargs={which:Estimator(mutate=True)}
    with pytest.raises(ValueError,match='changed during inference'):emitted(**kwargs)

@pytest.mark.parametrize('target',['arbitrary_label','midpoint_up',None,True])
def test_unreviewed_targets_cannot_emit_a_relabelled_forecast(target):
    with pytest.raises(ValueError):emitted(target=target)

@pytest.mark.parametrize('field,value',[('schema_version',2),('schema_version',True),('model','another_model')])
def test_artifact_schema_and_model_identity_are_checked(field,value):
    worker,run=make_worker();run.artifact[field]=value
    with pytest.raises(ValueError):worker.forecast(snapshot())

def test_legacy_promotion_state_refuses_before_any_loading():
    with pytest.raises(ValueError,match='refuses legacy promotion'):
        producer.ModelGapArtifactProducerV2(promotion_state_path=Path('must_not_be_read.json'))

def test_old_probability_convenience_api_refuses():
    with pytest.raises(ValueError,match='midpoint'):producer.relative_up_probability(.7,.3)

@pytest.mark.parametrize('left,right',[(0.,1.),(1.,0.),(0.,0.),(1.,1.),(.5,.5)])
def test_source_extremes_preserved_and_tied_side_preference_is_flat(left,right):
    point=emitted(estimator=Estimator(((1-left,left),(1-right,right))))['forecast_curve']['300']
    assert point['side_probabilities']=={'long':left,'short':right}
    assert point['direction']==('buy' if left>right else 'sell' if left<right else 'flat')
    assert point['probability_up'] is None

@pytest.mark.parametrize('move,direction',[(8.,'buy'),(-8.,'sell'),(0.,'flat')])
def test_signed_move_keeps_units_but_never_invents_probability(move,direction):
    point=semantics.legacy_directional_point({'horizon_sec':300,'predicted_signed_pips':move})
    assert point['direction']==direction and point['predicted_signed_pips']==move
    assert point['probability_up'] is None and point['signal_confidence'] is None
    assert semantics.validate_point(point)==point

@pytest.mark.parametrize('confidence',[.9,.2])
def test_direction_confidence_preserved_without_probability_or_side_reversal(confidence):
    point=semantics.legacy_directional_point({'horizon_sec':300,'direction':'buy','confidence':confidence})
    assert point['direction']=='buy' and point['source_confidence']==confidence
    assert point['probability_up'] is None and point['account_eligible'] is False

def test_unknown_legacy_probability_and_calibration_are_retained_but_unverified():
    raw={'horizon_sec':300,'probability_up':.9,'probability_target':'arbitrary_legacy_target',
        'calibrated_brier':.01,'calibration_n':700}
    point=semantics.legacy_directional_point(raw)
    assert point['source_input']['raw']==raw
    assert point['source_probability_reported_as_up']==.9
    assert point['probability_up'] is None and point['direction']=='unavailable'
    assert point['calibrated_brier'] is None and point['calibration_n'] is None
    assert point['legacy_reported_validation']['calibration_n']==700

@pytest.mark.parametrize('bad',[float('nan'),float('inf'),'garbage',True])
def test_malformed_explicit_probability_is_not_repaired_with_confidence(bad):
    with pytest.raises(ValueError):semantics.legacy_directional_point({'horizon_sec':300,'probability_up':bad,'direction':'buy','confidence':.9})

@pytest.mark.parametrize('field,value',[('probability_up',.9),('account_eligible',True),('direction','sell'),
    ('calibrated_brier',.01),('calibration_n',100),('relative_side_logit_score',.9)])
def test_derived_point_fields_cannot_be_modified_after_source_mapping(field,value):
    point=emitted()['forecast_curve']['300'];point[field]=value
    with pytest.raises(ValueError):semantics.validate_point(point)

@pytest.mark.parametrize('outer,value',[('artifact_sha256','c'*64),('source_report_sha256','c'*64),('source_target','target_best_side')])
def test_point_and_producer_source_identity_must_match(outer,value):
    raw=emitted();raw['producer_metadata'][outer]=value
    with pytest.raises(ValueError):feed.normalize_forecast_v2(raw,source='synthetic')

@pytest.mark.parametrize('consumer',[feed.to_legacy_feed,feed.to_legacy_outcome_ledger,feed.to_probability_consensus])
def test_unsupported_legacy_consumers_refuse_instead_of_substituting_half(consumer):
    typed=feed.normalize_forecast_v2(emitted(),source='synthetic')
    with pytest.raises(ValueError,match='legacy transport refused'):consumer(typed)

@pytest.mark.parametrize('key,value',[('account_eligible',True),('research_only',False),('probability_up',.5),
    ('signal_confidence',.9),('signal_semantics_contract','legacy'),('cohort_id','old-cohort'),('direction','sell')])
def test_outer_envelope_cannot_request_unsupported_semantics_or_qualification(key,value):
    raw=emitted();raw[key]=value
    with pytest.raises(ValueError):feed.normalize_forecast_v2(raw,source='synthetic')

def test_duplicate_numeric_horizon_alias_is_refused():
    raw=emitted();raw['forecast_curve']['300.0']=copy.deepcopy(raw['forecast_curve']['300'])
    with pytest.raises(ValueError):feed.normalize_forecast_v2(raw,source='synthetic')

def test_original_source_identity_determinism_and_quote_binding():
    raw=emitted();before=copy.deepcopy(raw)
    first=feed.normalize_forecast_v2(raw,source='synthetic');second=feed.normalize_forecast_v2(copy.deepcopy(raw),source='synthetic')
    assert first==second and raw==before
    changed=copy.deepcopy(raw);changed['bid']=1.09
    assert feed.normalize_forecast_v2(changed,source='synthetic')['id']!=first['id']

def test_zero_forecast_duplicate_snapshot_does_not_republish():
    worker,_=make_worker();first=worker.forecast(snapshot());second=worker.forecast(snapshot())
    assert len(first)==1 and second==[] and worker.last_summary['status']=='duplicate_snapshot'

@pytest.mark.parametrize('metrics',[{'brier':.1,'events':50},{'brier':.1,'side_rows':0},
    {'brier':.1,'side_rows':50,'events':51},{'brier':.1,'side_rows':100,'events':50,'target':'target_best_side'}])
def test_source_brier_requires_true_denominator_and_consistent_declared_target(metrics):
    worker,run=make_worker();run.cell_metrics[('M1',300)]=metrics
    with pytest.raises(ValueError):worker.forecast(snapshot())

def test_missing_brier_remains_missing_not_perfect_zero():
    worker,run=make_worker();run.cell_metrics[('M1',300)]={'events':50,'side_rows':100}
    point=worker.forecast(snapshot())[0]['forecast_curve']['300']
    assert point['side_validation']['source_reported_brier'] is None
    assert point['calibrated_brier'] is None

def test_legacy_point_retains_null_explicit_probability():
    point=semantics.legacy_directional_point({'horizon_sec':300,'direction':'buy','confidence':.9,'probability_up':None})
    assert point['probability_up'] is None

def test_pinned_positive_helper_identity_refuses_changed_bytes(monkeypatch):
    monkeypatch.setattr(semantics,'_positive',None)
    monkeypatch.setattr(semantics,'POSITIVE_HELPER_SHA256','f'*64)
    with pytest.raises(ValueError,match='dependency identity'):semantics.positive_probability(Estimator(),pd.DataFrame({'x':[0,0]}))
