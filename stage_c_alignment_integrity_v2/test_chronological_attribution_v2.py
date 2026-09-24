from copy import deepcopy
from decimal import Decimal
import pytest
from layer_policy_attribution_v2 import unique,outcome_value,bind_candidate,bind_original_candidate,account_reconcile,metrics,forecast_table
from chronological_attribution_operator_v2 import checked_bytes


def pred():
    return {'record_id':'EUR_USD:100','target_id':'endpoint','target_epoch':200,'decision_epoch':100,'available_epoch':102,
      'base_method':'ridge','variant':'raw_unrestricted','instrument':'EUR_USD','model_id':'m','signed_fit_id':'s',
      'absolute_fit_id':'a','observation_sha256':'o','eligibility_snapshot_id':None,'prediction_bps':2}


def outcome():return {'record_id':'EUR_USD:100','target_id':'endpoint','label_end_epoch':200,'available_epoch':201,'value':3}


def candidate():
    return {'instrument':'EUR_USD','decision_epoch':102,'original_target_epoch':200,'side':1,
      'source_payload':{'issued_epoch':102,'model_id':'m','source_bindings':{'prediction':'p','model':'m','signed_fit':'s',
       'absolute_fit':'a','feature_observation':'o','eligibility_snapshot':None}}}


def test_duplicate_outcome_refuses_even_identical():
    with pytest.raises(ValueError,match='duplicate'):unique([outcome(),outcome()],lambda o:(o['record_id'],o['target_id']),'outcome')


@pytest.mark.parametrize('key,value',[('record_id','GBP_USD:100'),('target_id','other'),('label_end_epoch',199),('available_epoch',199)])
def test_wrong_original_outcome_or_backdated_maturity_refuses(key,value):
    o=outcome();o[key]=value
    with pytest.raises(ValueError,match='original_outcome'):outcome_value(pred(),o,300)


def test_future_and_null_outcomes_stay_unavailable_not_zero():
    assert outcome_value(pred(),outcome(),200)==(None,'not_mature_at_assessment')
    o=outcome();o['value']=None
    assert outcome_value(pred(),o,300)==(None,'original_label_unavailable')
    o['value']=0;assert outcome_value(pred(),o,300)==(Decimal(0),'mature')


def test_nonfinite_label_refuses():
    o=outcome();o['value']=float('nan')
    with pytest.raises(ValueError,match='nonfinite'):outcome_value(pred(),o,300)


@pytest.mark.parametrize('change',['model','fit','origin','side','method','target'])
def test_selected_binding_requires_all_original_dimensions(change):
    p=pred();c=candidate();d={'epoch':102};method='ridge__raw_unrestricted'
    assert bind_candidate(c,d,{'p':p},method,200)=='p'
    if change=='model':c['source_payload']['source_bindings']['model']='wrong'
    if change=='fit':c['source_payload']['source_bindings']['signed_fit']='wrong'
    if change=='origin':d['epoch']=103
    if change=='side':c['side']=-1
    if change=='method':method='recovered_hgb__raw_unrestricted'
    if change=='target':c['original_target_epoch']=201
    with pytest.raises(ValueError,match='binding'):bind_candidate(c,d,{'p':p},method,200)


def ledger_fixture():
    events=[{'arm':'a','kind':'fill','epoch':100,'receipt':{'status':'filled','fee_usd':'.1','legs':[
       {'kind':'close','instrument':'EUR_USD','realized_usd':'10'}]}},
       {'arm':'a','kind':'financing','epoch':101,'receipt':{'status':'financing_applied','amount_usd':'-.2'}}]
    report={'arms':{'a':{'open_lot_count':0,'pending_order_units':0,'unrealized_usd':'0',
       'realized_usd':'10','fees_usd':'.1','financing_usd':'-.2','net_account_pnl_usd':'9.7'}}}
    return events,report


def test_account_financing_and_fees_reconcile_once_without_pair_fee_allocation():
    e,r=ledger_fixture();a,opens=account_reconcile('a',e,r,'1e-40')
    assert a['net_account_pnl_usd']=='9.7' and a['realized_usd_by_instrument']=={'EUR_USD':'10'}
    r['arms']['a']['net_account_pnl_usd']='9.8'
    with pytest.raises(ValueError,match='reconciliation'):account_reconcile('a',e,r,'1e-40')


def test_unpriced_or_open_parent_is_not_misreported_closed():
    e,r=ledger_fixture();r['arms']['a']['unrealized_usd']=None
    with pytest.raises(ValueError,match='closed_parent'):account_reconcile('a',e,r,'1e-40')


def test_rejected_financing_is_visible_and_not_called_applied_zero_cost():
    e,r=ledger_fixture();e[1]['receipt']={'status':'rejected','reason':'explicit_financing_rate_missing'}
    r['arms']['a'].update(financing_usd='0',net_account_pnl_usd='9.9')
    a,_=account_reconcile('a',e,r,'1e-40')
    assert a['financing_usd']=='0' and not a['financing_application_complete']
    assert a['rejected_receipts']==[{'epoch':101,'kind':'financing','reason':'explicit_financing_rate_missing'}]


def test_metrics_distinguish_zero_direction_and_unavailable_rows():
    rows=[{'forecast_id':'a','origin_epoch':100,'instrument':'EUR_USD','outcome_status':'mature','prediction_bps':'2','outcome_bps':'3','error_bps':'-1'},
      {'forecast_id':'b','origin_epoch':100,'instrument':'GBP_USD','outcome_status':'mature','prediction_bps':'0','outcome_bps':'0','error_bps':'0'},
      {'forecast_id':'c','origin_epoch':200,'instrument':'EUR_USD','outcome_status':'original_label_unavailable','prediction_bps':'2','outcome_bps':None,'error_bps':None}]
    m=metrics(rows);assert m['mature_rows']==2 and m['mae_bps']=='0.5' and m['direction_nonzero_rows']==1 and m['direction_matches']==1
    assert m['distinct_origins']==2 and m['distinct_pairs']==2


def test_missing_all68_slot_refuses():
    with pytest.raises(ValueError,match='all68'):forecast_table({'100':{'coverage':[]}},[],{'origins':[100],'universe':['EUR_USD'],'methods':['ridge__raw_unrestricted']})


def test_native_node_id_maps_to_normalized_decision_candidate_and_exact_payload():
    raw={'node_id':'node','instrument':'EUR_USD','side':1,'expected_terminal_price':'1.2','source_bindings':{'prediction':'p'}}
    normalized={'candidate_id':'node','instrument':'EUR_USD','side':1,'expected_terminal_price':'1.2','base_units':1000,'source_payload':deepcopy(raw)}
    assert bind_original_candidate([raw],normalized)==1
    normalized['source_payload']['source_bindings']['prediction']='forged'
    with pytest.raises(ValueError,match='original_frame'):bind_original_candidate([raw],normalized)


def test_changed_parent_bytes_refuse_even_if_valid_json(tmp_path):
    import hashlib
    p=tmp_path/'x.json';p.write_bytes(b'{"x":1}')
    recipe={'inputs':{'a':{'files':{'x.json':{'bytes':7,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}}}},'contract':{'resources':{'max_member_bytes':16}}}
    assert checked_bytes({'a':str(tmp_path)},recipe,'a','x.json')==p.read_bytes()
    p.write_bytes(b'{"x":2}')
    with pytest.raises(ValueError,match='input_hash'):checked_bytes({'a':str(tmp_path)},recipe,'a','x.json')


def rank_fixture():
    ps={};rows={};raw=[]
    for fid,instrument,predicted,actual in [('a','EUR_USD',2,3),('b','GBP_USD',-4,-3),('c','AUD_USD',1,5)]:
        ps[fid]={'instrument':instrument,'available_epoch':102,'prediction_bps':predicted}
        rows[fid]={'outcome_status':'mature','outcome_bps':str(actual)}
        raw.append({'node_id':fid,'instrument':instrument,'side':1 if predicted>0 else -1,'issued_epoch':102,
            'expected_terminal_price':'1.2','decision_quote':{'bid':'1.0' if predicted>0 else '1.3','ask':'1.0' if predicted>0 else '1.3'},'source_bindings':{'prediction':fid}})
    cand={'candidate_id':'a','instrument':'EUR_USD','side':1,'expected_terminal_price':'1.2','source_payload':deepcopy(raw[0])}
    return raw,cand,ps,rows


def test_endpoint_rank_ties_and_direction_are_explicit():
    from chronological_attribution_v2 import endpoint_rank
    result=endpoint_rank(*rank_fixture())
    assert result['rank_best_1']==2 and result['strictly_better']==1 and result['tied_including_selected']==2
    assert result['mature_candidates']==3 and result['selected_directional_endpoint_bps']=='3'


def test_quote_relative_side_can_differ_from_forecast_reference_sign():
    from chronological_attribution_v2 import endpoint_rank
    raw,cand,ps,rows=rank_fixture();ps['a']['prediction_bps']=-0.003658861857168627
    result=endpoint_rank(raw,cand,ps,rows)
    assert result['selected_directional_endpoint_bps']=='3' and result['rank_best_1']==2


def test_unavailable_selected_endpoint_has_no_rank():
    from chronological_attribution_v2 import endpoint_rank
    raw,cand,ps,rows=rank_fixture();rows['a'].update(outcome_status='not_mature_at_assessment',outcome_bps=None)
    r=endpoint_rank(raw,cand,ps,rows)
    assert r['rank_best_1'] is None and r['mature_candidates']==2 and r['unavailable_candidates']==1


@pytest.mark.parametrize('fault',['unknown','wrong_side','duplicate'])
def test_rank_requires_unique_original_bound_candidates(fault):
    from chronological_attribution_v2 import endpoint_rank
    raw,cand,ps,rows=rank_fixture()
    if fault=='unknown':raw[1]['source_bindings']['prediction']='unknown'
    if fault=='wrong_side':raw[1]['side']=1
    if fault=='duplicate':raw.append(deepcopy(raw[1]));raw[-1]['node_id']='other'
    with pytest.raises(ValueError):endpoint_rank(raw,cand,ps,rows)


@pytest.mark.parametrize('fault',['cohort','target','origin','prediction_cohort'])
def test_cross_cohort_native_frame_refuses_before_metrics(fault):
    from chronological_attribution_v2 import forecast_table
    f={'cohort':'monday','target_epoch':200,'origin_epoch':100,'predictions':[{'cohort':'monday'}]}
    if fault=='cohort':f['cohort']='tuesday'
    if fault=='target':f['target_epoch']=201
    if fault=='origin':f['origin_epoch']=99
    if fault=='prediction_cohort':f['predictions'][0]['cohort']='tuesday'
    with pytest.raises(ValueError,match='cohort'):
        forecast_table({'100':f},[],{'cohort':'monday','target_epoch':200})


def test_wrong_capsule_pin_refuses_before_destination(tmp_path):
    from chronological_attribution_checkpoint_v2 import restore
    p=tmp_path/'x.zip';p.write_bytes(b'not zip');dest=tmp_path/'restored'
    with pytest.raises(ValueError,match='pin'):restore(p,'0'*64,dest)
    assert not dest.exists()


@pytest.mark.parametrize('fault',['traversal','case_collision','oversized_member','too_many_members'])
def test_streaming_capsule_rejects_unsafe_inventory_before_extract(tmp_path,fault):
    import hashlib,zipfile
    from chronological_attribution_checkpoint_v2 import restore
    p=tmp_path/'bad.zip';dest=tmp_path/'output'
    with zipfile.ZipFile(p,'w',compression=zipfile.ZIP_DEFLATED) as z:
        if fault=='traversal':z.writestr('../escape',b'x')
        elif fault=='case_collision':z.writestr('x',b'x');z.writestr('X',b'y')
        elif fault=='oversized_member':z.writestr('big',b'x'*(16777216+1))
        else:
            for i in range(641):z.writestr(str(i),b'')
    with pytest.raises(ValueError):restore(p,hashlib.sha256(p.read_bytes()).hexdigest(),dest)
    assert not dest.exists() and not (tmp_path/'escape').exists()


def test_recipe_source_drift_refuses_before_analysis_import(tmp_path):
    import hashlib,json
    from chronological_attribution_operator_v2 import preflight
    p=tmp_path/'recipe.json';p.write_text(json.dumps({'sources':{}}))
    with pytest.raises(ValueError,match='source_drift'):preflight(p,hashlib.sha256(p.read_bytes()).hexdigest(),{})
