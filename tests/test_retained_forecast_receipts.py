from pathlib import Path
from copy import deepcopy
import json,sys
import pytest
R=Path(__file__).resolve().parents[1];sys.path[:0]=[str(R/'tools'),str(R/'tests')]
import forex_retained_forecast_receipts as m
import test_retained_management_readiness as old
from oanda_exact_quote_receipts_v1 import parse_price


def fixture(tmp_path):
    meta,data=old.fixture(tmp_path)
    for value in data.values():
        if isinstance(value,dict) and 'forecasts' in value:
            for f in value['forecasts']:f['target_semantics']='completed_M1_midpoint_elapsed_return'
    issues={}
    remap={}
    for name,v in list(data.items()):
        if name.startswith('issue_'):
            pin=m.prior.sha(m.prior.encoded(v));remap[name[6:-5]]=pin;issues['issue_'+pin+'.json']=v;del data[name]
    data.update(issues)
    data['current.json']['payload_sha256']=remap[data['current.json']['payload_sha256']]
    for a in data['anchors.json']:
        b=json.loads(a['body']);b['forecast']['target_semantics']='completed_M1_midpoint_elapsed_return';b['publication_sha256']=remap[b['publication_sha256']]
        a['body']=json.dumps(b)
    return meta,data


def context(tmp_path):
    meta,data=fixture(tmp_path);path=tmp_path/'inputs';pin=old.save(path,meta,data)
    return m.VerifiedCapture.load(path,pin),path,pin


def receipt(c):return next(r for r in c.receipts.values() if r['observation_kind']=='capture_observed')


def test_actual_consumer_complete_receipts_and_outcome_separation(tmp_path):
    c,_,_=context(tmp_path);rows,report=m.inspect(c)
    assert report['receipts']==136 and report['population']==68
    assert report['candidate_statuses']=={'forecast_observation_available':68}
    assert report['native_gate_unchanged']=='native_synthetic_qualification_tier_required'
    r=receipt(c);consumed=c.consume(r,observed_epoch=10002)
    x=c.candidate(r,consumed,decision_epoch=10002,target_epoch=10300)
    assert x['original_terminal_price_estimate']=='1.10022'
    assert x['pricing_status']=='quote_missing' and not x['conditional_value_qualified']
    assert not x['action_eligible'] and 'outcome' not in json.dumps(r)
    assert r['publication_completion_epoch'] is None


@pytest.mark.parametrize('field,value',[
    ('target_epoch',10301),('reference_epoch',9999),('expected_return_bps',20),
    ('original_model_id','wrong'),('feature_hash','0'*64),('input_hash','0'*64),
    ('reference_mid',2),('issued_epoch',9999),('target_semantics','synthetic'),
])
def test_resealed_receipt_cannot_override_original_issuance(tmp_path,field,value):
    c,_,_=context(tmp_path);r=deepcopy(receipt(c));r['forecast'][field]=value
    r=m.seal({k:v for k,v in r.items() if k!='receipt_sha256'},'receipt_sha256')
    with pytest.raises(ValueError,match='authenticated_source'):c.validate(r)


@pytest.mark.parametrize('field,value',[
    ('original_observed_epoch',9999),('original_publication_sha256','0'*64),
    ('model_definition_sha256','0'*64),('observation_kind','first_observed'),
    ('input_tier','synthetic_fresh_conditional_native_curve.v1'),('can_place_orders',True)
])
def test_resealed_envelope_identity_and_authority_refused(tmp_path,field,value):
    c,_,_=context(tmp_path);r=deepcopy(receipt(c));r[field]=value
    r=m.seal({k:v for k,v in r.items() if k!='receipt_sha256'},'receipt_sha256')
    with pytest.raises(ValueError):c.validate(r)


def test_actual_new_observation_is_not_backdated_or_refreshed(tmp_path):
    c,_,_=context(tmp_path);r=receipt(c)
    with pytest.raises(ValueError,match='before_retained'):c.consume(r,observed_epoch=10001)
    late=c.consume(r,observed_epoch=10200)
    with pytest.raises(ValueError,match='precedes'):c.candidate(r,late,decision_epoch=10002,target_epoch=10300)
    assert c.candidate(r,late,decision_epoch=10200,target_epoch=10300)['reason']=='retained_reference_stale'
    bad={**late,'available_epoch':10001};bad=m.seal({k:v for k,v in bad.items() if k!='consumption_sha256'},'consumption_sha256')
    with pytest.raises(ValueError,match='consumption_semantic'):c.candidate(r,bad,decision_epoch=10200,target_epoch=10300)


def test_exact_target_elapsed_and_quote_refusal(tmp_path):
    c,_,_=context(tmp_path);r=receipt(c);con=c.consume(r,observed_epoch=10002)
    assert c.candidate(r,con,decision_epoch=10002,target_epoch=10301)['reason']=='exact_original_target_required'
    assert c.candidate(r,con,decision_epoch=10300,target_epoch=10300)['reason']=='target_elapsed'
    pair=r['forecast']['instrument']
    raw=json.dumps(dict(type='PRICE',instrument=pair,time='1970-01-01T02:46:41Z',
        bids=[dict(price='1.1000')],asks=[dict(price='1.1002')],tradeable=True,status='tradeable')).encode()
    q=parse_price(raw,received_epoch=10001.5,generation=1,session_id='fixture')
    q['available_epoch']=10002
    result=c.candidate(r,con,decision_epoch=10002,target_epoch=10300,quote=q)
    assert result['pricing_status']=='quote_validated' and result['bid']=='1.1000'
    assert 'expected_remaining' not in json.dumps(result)
    q['bid']=1.1
    assert c.candidate(r,con,decision_epoch=10002,target_epoch=10300,quote=q)['pricing_status']=='quote_refused'


def test_anchor_requires_new_input_same_exact_terminal_and_family(tmp_path):
    # Two verified captured horizons with identical target and distinct original input.
    meta,d=fixture(tmp_path);reg=d['registry.json'];reg['connections'][0].update(kind='legacy26_matched',arm='ridge',feature_names=['x'])
    oldentry={**reg['connections'][0],'id':'old_saved','horizon_minutes':7}
    reg['connections'].append(oldentry);rid=m.prior.sha(m.prior.encoded(reg));meta['registry_sha256']=rid
    current=d['current.json'];current.pop('payload_sha256');current['registry_sha256']=rid;current['connections']=reg['connections']
    for f in current['forecasts']:f['registry_sha256']=rid
    original=[]
    for a in d['anchors.json']:
        b=json.loads(a['body']);f=b['forecast'];f.update(connection='old_saved',horizon_minutes=7,target_epoch=10300,registry_sha256=rid)
        original.append(f)
    pub={**deepcopy(current),'generated_epoch':9882.,'forecasts':original}
    before=m.prior.sha(m.prior.encoded(pub))
    for a,f in zip(d['anchors.json'],original):
        a.update(id=m.prior.tracking.prediction_key(f),registry=rid,connection='old_saved',horizon=7,target=10300,
                 body=json.dumps(dict(forecast=f,publication_epoch=9882.,publication_sha256=before,first_observed_epoch=9883.)))
    current['coverage'] += [dict(connection='old_saved',instrument=p,status='unavailable') for p in reg['pairs']]
    after=m.prior.sha(m.prior.encoded(current))
    d={k:v for k,v in d.items() if not k.startswith('issue_')}
    d.update({'issue_'+before+'.json':pub,'issue_'+after+'.json':deepcopy(current)})
    current['payload_sha256']=after
    c=m.VerifiedCapture(meta,d,'a'*64);r=receipt(c);a=c.receipts['first_observed','old_saved',r['forecast']['instrument']]
    con=c.consume(r,observed_epoch=10002)
    result=c.candidate(r,con,decision_epoch=10002,target_epoch=10300,anchor=a)
    assert result['relation']=='same_terminal_updated_forecast_observation'
    assert not result['conditional_value_qualified']
    assert c.candidate(r,con,decision_epoch=10002,target_epoch=10300,anchor=r)['reason']=='fresh_same_terminal_input_required'
    other=next(x for x in c.receipts.values() if x['forecast']['instrument']!=r['forecast']['instrument'])
    assert c.candidate(r,con,decision_epoch=10002,target_epoch=10300,anchor=other)['reason']=='incumbent_terminal_mismatch'


def test_outcome_changes_never_enter_decision_consumer(tmp_path):
    c,_,_=context(tmp_path);r=receipt(c);con=c.consume(r,observed_epoch=10002)
    before=c.candidate(r,con,decision_epoch=10002,target_epoch=10300)
    c.outcomes={key:{'state':'settled','outcome_sha256':'f'*64} for key in c.outcomes}
    assert c.candidate(r,con,decision_epoch=10002,target_epoch=10300)==before


def test_whole_population_restart_corruption_and_input_identity(tmp_path):
    _,inp,pin=context(tmp_path)
    assert m.run(inp,pin,tmp_path/'runs','one')['status']=='completed'
    assert m.run(inp,pin,tmp_path/'runs','two',max_new=1)['status']=='checkpointed'
    assert m.run(inp,pin,tmp_path/'runs','two',resume=True)['status']=='completed'
    for path in (tmp_path/'runs/one').glob('rows_*.json'):assert path.read_bytes()==(tmp_path/'runs/two'/path.name).read_bytes()
    assert m.run(inp,pin,tmp_path/'runs','two')['status']=='verified_completed'
    (tmp_path/'runs/two/rows_0000.json').write_bytes(b'{}')
    with pytest.raises(Exception):m.run(inp,pin,tmp_path/'runs','two',resume=True)
    with pytest.raises(ValueError,match='trusted_manifest'):m.VerifiedCapture.load(inp,'0'*64)
