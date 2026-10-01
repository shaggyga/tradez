from copy import deepcopy
from pathlib import Path
import sys
import pytest
sys.path[:0] = [str(Path(__file__).resolve().parents[1] / 'tools'), str(Path(__file__).parent)]
import forex_management_state_binding as m
import test_retained_management_contract as old


def fixture(tmp_path, lifecycle='FLAT'):
    context, capture, base, key = old.fixture(tmp_path)
    records = deepcopy(base['evidence_records'])
    def add(body):
        pin = m.prior.sha(m.prior.encoded(body)); records[pin] = body; return pin
    receipt = dict(schema='paper_state_observation.v1', slot=key, evidence_tier='synthetic_fixture',
        account_kind='paper', book_id='fixture', producer_id='fixture', episode_id='fixture',
        producer_source_sha256=add({'source': 'synthetic_fixture'}),
        event_head_sha256=add({'event': 'synthetic_fixture'}), asof_epoch=10002,
        lifecycle=lifecycle, pending_orders=[], position_state_sha256=None)
    if lifecycle not in ('FLAT', 'CLOSED', 'ENTRY_PENDING'):
        receipt['position_state_sha256'] = base['contracts'][key]['state_sha256']
    if lifecycle.endswith('_PENDING'):
        receipt['pending_orders'] = [dict(order_id='pending', status='accepted',
            kind=lifecycle.split('_')[0], remaining_units=10,
            event_sha256=add({'event': 'synthetic_pending'}))]
    request = dict(schema=m.SCHEMA, source_bindings=m.sources(), management_request=base,
        state_records=records, state_bindings={key: add(receipt)})
    return context, capture, request, key, deepcopy(receipt)


def replace(request, key, receipt):
    pin = m.prior.sha(m.prior.encoded(receipt))
    request['state_records'][pin] = receipt; request['state_bindings'][key] = pin


@pytest.mark.parametrize('state,status', [('FLAT','declared_flat_wait'), ('CLOSED','declared_flat_wait'),
    ('ENTRY_PENDING','pending_settlement_wait'), ('REDUCE_PENDING','pending_settlement_wait'),
    ('EXIT_PENDING','pending_settlement_wait'), ('OPEN','declared_open_contract_consistent')])
def test_real_consumer_lifecycles(tmp_path, state, status):
    c, _, q, key, _ = fixture(tmp_path, state)
    rows, report = m.inspect(c, q); row = next(r for r in rows if r['slot'] == key)
    assert row['status'] == status, row
    assert report['population'] == 136 and report['statuses']['state_missing'] == 135
    assert not row['action_eligible'] and not row['state_producer_qualified']
    if state.endswith('_PENDING'):
        assert row['action'] == 'WAIT' and row['capacity_released'] is False


@pytest.mark.parametrize('fault', ['stale','future','wrong_slot','fake_flat','pending_flat','unknown',
    'partial_fill','missing_event','wrong_tier','wrong_position'])
def test_refusals(tmp_path, fault):
    c, _, q, key, r = fixture(tmp_path, 'ENTRY_PENDING' if fault == 'partial_fill' else 'FLAT')
    if fault == 'stale': r['asof_epoch'] -= 1
    elif fault == 'future': r['asof_epoch'] += 1
    elif fault == 'wrong_slot': r['slot'] = 'other'
    elif fault == 'fake_flat': r['position_state_sha256'] = 'position'
    elif fault == 'pending_flat': r['pending_orders'] = [{}]
    elif fault == 'unknown': r['lifecycle'] = 'UNKNOWN'
    elif fault == 'partial_fill': r['pending_orders'][0]['status'] = 'partially_filled'
    elif fault == 'missing_event': r['event_head_sha256'] = 'missing'
    elif fault == 'wrong_tier': r['evidence_tier'] = 'preserved_observation'
    elif fault == 'wrong_position': r.update(lifecycle='OPEN', position_state_sha256='missing')
    replace(q, key, r)
    rows, _ = m.inspect(c,q)
    assert next(x for x in rows if x['slot'] == key)['status'] == 'state_refused'


def test_missing_is_not_flat(tmp_path):
    c, _, q, _, _ = fixture(tmp_path); q['state_bindings'] = {}
    rows, report = m.inspect(c,q)
    assert report['statuses'] == {'state_missing':136}
    assert all('action' not in r for r in rows)


def test_tamper_and_population(tmp_path):
    c, _, q, key, r = fixture(tmp_path)
    changed = deepcopy(q); changed['state_records'][changed['state_bindings'][key]]['asof_epoch'] = 0
    with pytest.raises(ValueError, match='evidence_record_hash'): m.inspect(c,changed)
    q['state_bindings']['unknown/slot'] = q['state_bindings'][key]
    with pytest.raises(ValueError, match='state_binding_population'): m.inspect(c,q)


def test_resume_exact_and_hash_refusal(tmp_path):
    _, capture, q, _, _ = fixture(tmp_path)
    request = tmp_path/'request.json'; request.write_bytes(m.prior.encoded(q)); pin=m.prior.sha(request.read_bytes())
    output=tmp_path/'runs'
    assert m.run(capture,request,pin,output,'partial',max_new=1)['status']=='checkpointed'
    assert m.run(capture,request,pin,output,'partial',resume=True)['status']=='completed'
    assert m.run(capture,request,pin,output,'whole')['status']=='completed'
    a={p.relative_to(output/'partial').as_posix():p.read_bytes() for p in (output/'partial').rglob('rows_*.json')}
    b={p.relative_to(output/'whole').as_posix():p.read_bytes() for p in (output/'whole').rglob('rows_*.json')}
    assert a and a==b
    with pytest.raises(ValueError, match='request_external_hash'):
        m.run(capture,request,'0'*64,output,'bad')
