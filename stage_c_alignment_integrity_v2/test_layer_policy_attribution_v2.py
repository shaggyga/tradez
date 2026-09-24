from copy import deepcopy
from decimal import Decimal
import pytest
from layer_policy_attribution_v2 import unique,outcome_value,bind_candidate,bind_original_candidate,account_reconcile,metrics,forecast_table
from layer_policy_attribution_operator_v2 import checked_bytes


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
