"""Permanent pure-contract regression tests; no real manager or artifact load."""
import copy
from datetime import datetime,timedelta,timezone
import numpy as np
import pandas as pd
import pytest
import oanda_ensemble_probability_contract_v2 as contract

class FixedModel:
    def __init__(self,p=.9,classes=(1,0)):
        self.p=p;self.classes_=np.array(classes);self.fitted_rows=None
    def fit(self,x,y):self.fitted_rows=len(x);return self
    def predict_proba(self,x):
        return np.tile([self.p if label==1 else 1-self.p for label in self.classes_],(len(x),1))

class Engine:
    def __init__(self):self.models=[];self.calibrators=[]
    def estimator(self,spec):
        result=FixedModel();self.models.append(result);return result
    def fit_probability_calibrator(self,p,y):
        assert np.allclose(p,.9)
        result=FixedModel(.2);self.calibrators.append(result);return result

def fitted():
    start=datetime(2026,1,1,tzinfo=timezone.utc)
    def frame(prefix,minutes):
        clocks=[start+timedelta(minutes=minutes+i) for i in range(8)]
        return pd.DataFrame({'row_id':[prefix+str(i) for i in range(8)],'time_utc':clocks,'feature_available_utc':clocks,
            'target_end_utc':[t+timedelta(minutes=60) for t in clocks],
            'label_available_utc':[t+timedelta(minutes=60,seconds=1) for t in clocks],
            'label_matured':[True]*8,'availability_source_sha256':['a'*64]*8,'x':np.arange(8.),'long_profit_60':np.arange(8)%2})
    engine=Engine();schema=contract.feature_contract(['x'],{'x':'ATR_multiple'},'b'*64,'c'*64)
    member=contract.fit_member_v2(engine=engine,spec={'target':'long_profit_60','instrument_subset':'all'},
        train=frame('train',0),calibration=frame('cal',180),schema=schema,fit_asof=start+timedelta(days=1),
        weight=1.,min_train_rows=4,min_calibration_rows=4)
    bundle=contract.bundle_v2([member])
    score=contract.score_bundle_v2(bundle,pd.DataFrame({'x':[1.]}),feature_contract_ids=bundle['schema_ids'])
    policy={'contract':contract.CONTRACT,'combination':contract.COMBINATION,'bundle_fit_id':bundle['bundle_fit_id'],
        'evidence_role':'development_thresholds_only','mode':'directional_research','event_score_threshold':.1,'agreement_threshold':.6,'min_members':1}
    return engine,bundle,score,policy

def test_calibrator_and_class_mapping_survive_research_intake():
    engine,bundle,score,policy=fitted()
    assert len(engine.models)==1 and engine.models[0].fitted_rows==8
    assert bundle['members'][0]['event_head']['calibrator'] is engine.calibrators[0]
    assert score['event_score']==pytest.approx([.2]) and score['required_direction']==['LONG']
    result=contract.research_decision_v2(bundle,score,policy)
    assert result['research_threshold_pass']==[True] and result['entry_eligible'] is False
    intake=contract.manager_research_score_v2(bundle,{'direction':'LONG','_ensemble_feature_contract_ids_v2':bundle['schema_ids']},{'x':1.},policy)
    assert intake['available'] and intake['probability'] is None
    assert intake['shadow_approved'] is False and intake['signed_entry_qualified'] is False

@pytest.mark.parametrize('field,value',[('schema_ids',['d'*64]),('weights',[99.]),('horizon_minutes',240),('qualification_status','qualified'),('selected_threshold',.1)])
def test_outer_bundle_metadata_cannot_override_members(field,value):
    _,bundle,_,_=fitted();bundle[field]=value
    with pytest.raises(ValueError):contract.score_bundle_v2(bundle,pd.DataFrame({'x':[1.]}),feature_contract_ids=bundle['schema_ids'])

@pytest.mark.parametrize('field,value',[('event_score',[]),('event_score',[.2,.2]),('event_score',[float('inf')]),
    ('agreement_score',[1.1]),('required_direction',['SHORT']),('required_direction',['BUY']),('member_count',99),('entry_eligible',True)])
def test_thresholds_refuse_misaligned_or_inconsistent_score(field,value):
    _,bundle,score,policy=fitted();score[field]=value
    with pytest.raises(ValueError):contract.research_decision_v2(bundle,score,policy)

def test_member_detail_cannot_disagree_with_aggregate():
    _,bundle,score,policy=fitted();score['members'][0]['event_probability']=[.99]
    with pytest.raises(ValueError):contract.research_decision_v2(bundle,score,policy)

def test_legacy_full_window_fit_is_not_silently_recalibrated():
    with pytest.raises(ValueError,match='maturity'):contract.refuse_legacy_final_fit()

def test_side_target_does_not_claim_midpoint_up_probability():
    item=contract.target_contract('profitable_short_move_60')
    assert item['direction_rule']=='short' and item['event_semantics']=='profitable_move_onset'
    assert item['signed_midpoint_up_probability'] is False
