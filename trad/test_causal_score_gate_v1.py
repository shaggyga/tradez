from copy import deepcopy
from decimal import Decimal, localcontext
import pytest
import causal_score_gate_v1 as gate

def policy(**changes):
    value={'schema':gate.SCHEMA,'session_id':'synthetic-only','scopes':[{
        'scope_id':'EUR_USD.origin-v1','instrument':'EUR_USD','model_id':'model','model_sha256':'a'*64,
        'forecast_cohort':'fixture','source_bindings':{'model.py':'a'*64},'horizon_sec':3600,
        'score_recipe':'known_origin_atr_edge_times_original_confidence'}],
        'lookback_sec':1000,'minimum_history':2,'maximum_history':10,'maximum_seen':100,'entry_percentile':'0.6'}
    value.update(changes);return value

def fixture(index,decision,score='1',*,side=1,origin=None):
    reference=decision-10 if origin is None else origin
    sample={**deepcopy(policy()['scopes'][0]),'curve_id':'curve.'+str(index),'curve_sha256':'b'*64,
        'node_id':'node.'+str(index),'node_sha256':'c'*64,'reference_epoch':reference,
        'original_target_epoch':reference+3600,'source_available_epoch':reference+1,'static_score':score}
    sample['sample_id']=gate.sample_identity(sample)
    raw={key:deepcopy(sample[key]) for key in ('instrument','model_id','model_sha256','forecast_cohort','source_bindings',
        'curve_id','curve_sha256','node_id','node_sha256','reference_epoch','original_target_epoch')}
    raw['issued_epoch']=reference+1
    row={'instrument':'EUR_USD','candidate_id':sample['node_id'],'kind':'curve','side':side,
        'decision_epoch':decision,'available_epoch':decision,'original_target_epoch':sample['original_target_epoch'],
        'source_payload':raw,'unchanged_economic_fixture':Decimal('12.34')}
    return row,sample

def step(state,p,index,decision,score='1',**kwargs):
    row,sample=fixture(index,decision,score,**kwargs)
    return gate.build_score_channels([row],[sample],state,decision_epoch=decision,policy=p)

def primed(p=None):
    p=p or policy();state=gate.initial_state(p)
    for index,(when,score) in enumerate(((100,'0'),(200,'2'))):
        state=step(state,p,index,when,score)['next_score_state']
    return p,state

def test_average_rank_reuses_prior_convention_and_exact_entry_subset():
    p,state=primed();row,sample=fixture(2,300)
    frozen=deepcopy((row,sample,state))
    result=gate.build_score_channels([row],[sample],state,decision_epoch=300,policy=p)
    assert result['entry_rows']==[row] and result['continuation_rows']==[row]
    receipt=result['percentile_receipts'][0]
    assert receipt['rank_numerator_twice']==4 and receipt['rank_denominator_twice']==6
    assert len(result['next_score_state']['seen'])==3
    assert (row,sample,state)==frozen

def test_empty_history_withholds_entry_but_keeps_continuation():
    p=policy();result=step(gate.initial_state(p),p,0,100)
    assert not result['entry_rows'] and len(result['continuation_rows'])==1
    assert result['percentile_receipts'][0]['reason']=='calibration_history_insufficient'

def test_repricing_and_retry_cannot_resample_forecast_or_calibrate_itself():
    p,state=primed();row,sample=fixture(2,300)
    result=gate.build_score_channels([row],[sample,sample],state,decision_epoch=300,policy=p)
    assert len(result['next_score_state']['seen'])==3
    row['decision_epoch']=400;row['available_epoch']=400;row['unchanged_economic_fixture']=Decimal('0.05')
    repeated=gate.build_score_channels([row],[sample],result['next_score_state'],decision_epoch=400,policy=p)
    assert len(repeated['next_score_state']['seen'])==3
    assert repeated['percentile_receipts'][0]['history_count']==2
    assert repeated['next_score_state']['seen'][sample['sample_id']]['first_seen_decision_epoch']==300
    assert repeated['entry_rows']==[row]

def test_window_expiry_does_not_reset_session_identity_inventory():
    p,state=primed(policy(lookback_sec=100));row,sample=fixture(0,1200,'0',origin=90)
    result=gate.build_score_channels([row],[sample],state,decision_epoch=1200,policy=p)
    assert len(result['next_score_state']['seen'])==2
    assert result['percentile_receipts'][0]['history_count']==0
    assert result['next_score_state']['seen'][sample['sample_id']]['first_seen_decision_epoch']==100

def test_count_window_is_selected_by_time_and_identity_not_score():
    p,state=primed(policy(maximum_history=2))
    state=step(state,p,2,300,'100000')['next_score_state']
    result=step(state,p,3,400,'1')
    ids=result['percentile_receipts'][0]['history_sample_ids']
    assert ids==[fixture(1,200,'2')[1]['sample_id'],fixture(2,300,'100000')[1]['sample_id']]

@pytest.mark.parametrize('kind',['static_score','node_sha256','source_available_epoch','curve_sha256'])
def test_static_identity_conflict_refuses_entry_and_preserves_seen(kind):
    p,state=primed();row,sample=fixture(2,300)
    first=gate.build_score_channels([row],[sample],state,decision_epoch=300,policy=p)
    row['decision_epoch']=400;row['available_epoch']=400
    if kind=='static_score':sample[kind]='2'
    elif kind=='source_available_epoch':sample[kind]+=1
    else:sample[kind]='d'*64;row['source_payload'][kind]=sample[kind]
    out=gate.build_score_channels([row],[sample],first['next_score_state'],decision_epoch=400,policy=p)
    assert not out['entry_rows'] and out['continuation_rows']==[row]
    assert any(x['reason']=='conflicting_static_forecast_sample' for x in out['refusals'])
    assert out['next_score_state']['seen']==first['next_score_state']['seen']

def test_conflicting_duplicate_in_one_group_poisoned_independent_of_order():
    p,state=primed();row,sample=fixture(2,300);bad=deepcopy(sample);bad['static_score']='2'
    first=gate.build_score_channels([row],[sample,bad],state,decision_epoch=300,policy=p)
    second=gate.build_score_channels([row],[bad,sample],state,decision_epoch=300,policy=p)
    assert first==second and not first['entry_rows'] and first['next_score_state']['seen']==state['seen']

def test_invalid_duplicate_cannot_leave_good_copy_admitted():
    p,state=primed();row,sample=fixture(2,300);bad=deepcopy(sample);bad['source_bindings']['model.py']='d'*64
    result=gate.build_score_channels([row],[bad,sample],state,decision_epoch=300,policy=p)
    assert not result['entry_rows'] and result['next_score_state']['seen']==state['seen']

def test_neutral_continuation_retained_when_entry_withheld():
    p,state=primed();row,sample=fixture(2,300,side=0)
    result=gate.build_score_channels([row],[sample],state,decision_epoch=300,policy=p)
    assert not result['entry_rows'] and result['continuation_rows']==[row]
    assert result['percentile_receipts'][0]['reason']=='neutral_continuation'

@pytest.mark.parametrize('field,value',[('source_available_epoch',True),('reference_epoch',float('nan')),
    ('static_score',True),('static_score','NaN'),('horizon_sec',3600.0),('model_sha256','wrong')])
def test_malformed_scores_refused_without_becoming_history(field,value):
    p,state=primed();row,sample=fixture(2,300);sample[field]=value
    result=gate.build_score_channels([row],[sample],state,decision_epoch=300,policy=p)
    assert not result['entry_rows'] and result['next_score_state']['seen']==state['seen']

def test_future_available_score_cannot_be_backdated_into_history():
    p,state=primed();row,sample=fixture(2,300);sample['source_available_epoch']=301
    result=gate.build_score_channels([row],[sample],state,decision_epoch=300,policy=p)
    assert not result['entry_rows'] and result['next_score_state']['seen']==state['seen']

def test_session_inventory_overflow_refuses_whole_frame_atomically():
    p,state=primed(policy(maximum_history=2,maximum_seen=2));before=deepcopy(state)
    with pytest.raises(ValueError,match='session_seen_inventory_full'):step(state,p,2,300)
    assert state==before

@pytest.mark.parametrize('clock',[True,199,200,float('inf')])
def test_decision_clock_contract(clock):
    p,state=primed()
    with pytest.raises(ValueError):step(state,p,2,clock)

def test_state_and_policy_binding_cannot_be_changed_silently():
    p,state=primed();state['seen'][next(iter(state['seen']))]['first_seen_decision_epoch']=1
    with pytest.raises(ValueError,match='gate_state_binding'):step(state,p,2,300)

def test_outcome_or_future_map_in_continuation_is_refused():
    p,state=primed();row,sample=fixture(2,300);row['source_payload']['realized_outcome_atr']=999
    with pytest.raises(ValueError,match='future_or_outcome'):gate.build_score_channels([row],[sample],state,decision_epoch=300,policy=p)

def test_gate_threshold_and_receipts_independent_of_global_decimal_precision():
    p,state=primed();normal=step(state,p,2,300)
    with localcontext() as ctx:
        ctx.prec=3;low=step(state,p,2,300)
    assert normal==low

def test_whole_group_first_seen_freeze_order_and_scope_isolation():
    p=policy(minimum_history=1)
    eur,eur_score=fixture(0,100,'1')
    gbp,gbp_score=deepcopy(eur),deepcopy(eur_score)
    second=deepcopy(p['scopes'][0]);second.update(scope_id='GBP_USD.origin-v1',instrument='GBP_USD')
    p['scopes'].append(second)
    gbp_score.update(second);gbp_score['sample_id']=gate.sample_identity(gbp_score)
    gbp['instrument']='GBP_USD';gbp['source_payload']['instrument']='GBP_USD'
    first=gate.build_score_channels([eur,gbp],[eur_score,gbp_score],gate.initial_state(p),decision_epoch=100,policy=p)
    other=gate.build_score_channels([gbp,eur],[gbp_score,eur_score],gate.initial_state(p),decision_epoch=100,policy=p)
    assert not first['entry_rows'] and all(x['history_count']==0 for x in first['percentile_receipts'])
    assert first['next_score_state']==other['next_score_state']
    assert first['percentile_receipts']==other['percentile_receipts']
    assert first['continuation_rows']==[eur,gbp] and other['continuation_rows']==[gbp,eur]
    current,current_score=fixture(1,200,'2')
    later=gate.build_score_channels([current],[current_score],first['next_score_state'],decision_epoch=200,policy=p)
    assert later['percentile_receipts'][0]['history_count']==1
    assert later['percentile_receipts'][0]['history_sample_ids']==[eur_score['sample_id']]

def test_late_arrival_records_actual_first_seen_not_earlier_source_availability():
    p=policy(minimum_history=1,lookback_sec=100)
    row,sample=fixture(0,300,'1',origin=90)
    first=gate.build_score_channels([row],[sample],gate.initial_state(p),decision_epoch=300,policy=p)
    assert first['next_score_state']['seen'][sample['sample_id']]['first_seen_decision_epoch']==300
    later=step(first['next_score_state'],p,1,350,'2')
    assert later['percentile_receipts'][0]['history_sample_ids']==[sample['sample_id']]

@pytest.mark.parametrize('value',['1e-1000000000','1e1000000000','0e-1000000000','1e-129',
    '9'*129,'0.'+'1'*97,Decimal('1e-1000000000'),Decimal('0e1000000000'),1<<10000])
def test_unbounded_numeric_expansion_refused_before_fraction(value,monkeypatch):
    p=policy(entry_percentile=value)
    monkeypatch.setattr(gate,'Fraction',lambda *args:pytest.fail('Fraction must not see unbounded input'))
    with pytest.raises(ValueError,match='bounded_'):gate.initial_state(p)

def test_unbounded_decimal_is_rejected_before_fixed_format_serialization():
    with pytest.raises(ValueError,match='bounded_decimal'):
        gate.canonical({'value':Decimal('1e-1000000000')})

def test_supported_numeric_domain_retains_extremes_and_existing_precision():
    assert gate.number('1e-128')==Decimal('1e-128')
    assert gate.number('1e128')==Decimal('1e128')
    assert gate.number('0.'+'1'*96)==Decimal('0.'+'1'*96)
    assert gate.canonical({'value':Decimal('1e-128')}).startswith(b'{"value":"0.')
