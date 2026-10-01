from pathlib import Path
from copy import deepcopy
import json
import sys
import pytest

R = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(R / 'tools'), str(R / 'tests')]
import forex_retained_combined_observation as m
import test_retained_forecast_receipts as old


def fixture(tmp_path):
    context, path, pin = old.context(tmp_path)
    pairs = sorted(context.registry['pairs'])
    quotes = {}
    for pair in pairs:
        raw = json.dumps(dict(type='PRICE', instrument=pair, time='1970-01-01T02:46:41Z',
            bids=[dict(price='1.1000')], asks=[dict(price='1.1002')], tradeable=True)).encode()
        quotes[pair] = m.quotes.parse_price(raw, received_epoch=10001.5, generation=2, session_id='fixture')
    snapshot = dict(schema=m.quotes.SCHEMA, producer='exact_raw_price_sidecar', session_id='fixture',
        connection_generation=2, snapshot_created_epoch=10002, instruments=pairs,
        quotes=quotes, refusals={}, quote_count=len(quotes), research_only=True, can_place_orders=False)
    snapshot['snapshot_sha256'] = m.quotes.digest(snapshot)
    request = dict(schema=m.SCHEMA, mode='frozen_replay', source_bindings=m.sources(),
        capture_manifest_sha256=pin, forecast_observed_epoch=10002, quote_observed_epoch=10002,
        decision_epoch=10002, quote_snapshot=snapshot, expected_session_id='fixture', expected_generation=2,
        requested_targets={b['connection'] + '/' + b['instrument']:10300 for b in context.population})
    return context, path, request


def seal_snapshot(request):
    s=request['quote_snapshot']
    s['snapshot_sha256']=m.quotes.digest({k:v for k,v in s.items() if k not in ('snapshot_sha256','transport')})


def test_complete_join_preserves_target_prices_and_no_authority(tmp_path):
    context, _, request = fixture(tmp_path)
    rows, report = m.observe(context, request)
    assert report['statuses'] == {'priced_observation':68}
    assert report['quote_pairs_validated'] == 68
    assert not report['current_live_observation'] and not report['manager_activation']
    assert report['native_gate_unchanged'] == 'native_synthetic_qualification_tier_required'
    for row in rows:
        c=row['candidate']
        assert c['bid']=='1.1000' and c['original_terminal_price_estimate']=='1.10022'
        assert not row['action_eligible'] and not row['conditional_value_qualified']
        assert c['original_target_epoch']==c['requested_target_epoch']==10300


@pytest.mark.parametrize('field,value,reason',[
    ('expected_session_id','other','expected_quote_session'),
    ('expected_generation',3,'expected_quote_generation'),
    ('expected_generation',True,'expected_quote_generation'),
    ('capture_manifest_sha256','0'*64,'capture_binding'),
    ('source_bindings',{},'combined_source_bindings'),
    ('mode','current','replay_scope_required'),
    ('forecast_observed_epoch',10001,'combined_clock_order'),
    ('quote_observed_epoch',10003,'combined_clock_order'),
    ('decision_epoch',10001,'combined_clock_order'),
    ('quote_observed_epoch',10001,'snapshot_future'),
    ('requested_targets',{},'exact_requested_population'),
])
def test_drift_refused_through_join(tmp_path,field,value,reason):
    c,_,r=fixture(tmp_path);r[field]=value
    with pytest.raises(ValueError,match=reason):m.observe(c,r)


def test_exact_target_mismatch_is_complete_explicit_population(tmp_path):
    c,_,r=fixture(tmp_path);key=next(iter(r['requested_targets']));r['requested_targets'][key]+=1
    rows,report=m.observe(c,r)
    assert report['statuses']=={'forecast_refused':1,'priced_observation':67}
    assert next(x for x in rows if x['status']=='forecast_refused')['candidate']['reason']=='exact_original_target_required'


def test_missing_and_malformed_pair_never_retains_price(tmp_path):
    c,_,r=fixture(tmp_path);s=r['quote_snapshot'];pairs=s['instruments']
    del s['quotes'][pairs[0]];s['quote_count']-=1
    s['quotes'][pairs[1]]['bid']='1.0000';seal_snapshot(r)
    rows,report=m.observe(c,r)
    assert report['statuses']=={'quote_missing':1,'quote_refused':1,'priced_observation':66}
    assert rows[0]['candidate']['pricing_status']=='quote_missing'
    assert rows[1]['quote_reason']=='raw_receipt_identity'


@pytest.mark.parametrize('decision,status',[(10032,'quote_refused'),(10200,'forecast_refused'),(10300,'forecast_refused')])
def test_later_observation_never_renews_old_prices_or_forecasts(tmp_path,decision,status):
    c,_,r=fixture(tmp_path)
    r.update(decision_epoch=decision,forecast_observed_epoch=decision,quote_observed_epoch=decision)
    rows,report=m.observe(c,r)
    assert report['statuses']=={status:68}
    assert not any(x['action_eligible'] for x in rows)


def test_transport_publication_clock_and_inventory_bound(tmp_path):
    c,_,r=fixture(tmp_path)
    r['quote_snapshot']['transport']={'publication_started_epoch':10003}
    with pytest.raises(ValueError,match='publication_clock_order'):m.observe(c,r)
    r['quote_snapshot'].pop('transport');r['quote_snapshot']['instruments'].pop();seal_snapshot(r)
    with pytest.raises(ValueError,match='exact_instrument_inventory'):m.observe(c,r)


def test_cli_function_external_pin_restart_and_tamper(tmp_path):
    c,path,r=fixture(tmp_path);request=tmp_path/'REQUEST.json';raw=m.prior.encoded(r);request.write_bytes(raw)
    pin=m.prior.sha(raw);out=tmp_path/'runs'
    assert m.run(path,request,pin,out,'one')['status']=='completed'
    assert m.run(path,request,pin,out,'resume',max_new=1)['status']=='checkpointed'
    assert m.run(path,request,pin,out,'resume',resume=True)['status']=='completed'
    for p in (out/'one').glob('rows_*.json'):assert p.read_bytes()==(out/'resume'/p.name).read_bytes()
    assert m.run(path,request,pin,out,'one')['status']=='verified_completed'
    with pytest.raises(ValueError,match='request_external_hash'):m.run(path,request,'0'*64,out,'bad')
    p=out/'one'/'rows_0000.json';p.write_bytes(b'[]')
    with pytest.raises(Exception):m.run(path,request,pin,out,'one')
