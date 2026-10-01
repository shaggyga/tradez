"""Offline lifecycle binding over retained management observations; never an order engine.

State receipts are supplied observations, not inferred positions. Content hashes
prove identity, not that a supplied state producer is independently qualified.
"""
from collections import Counter
from copy import deepcopy
from pathlib import Path
import argparse
import json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import forex_retained_management_contract as contract

SCHEMA = 'retained_management_state_binding.v1'
prior = contract.prior
FLAGS = dict(contract.FLAGS, state_producer_qualified=False)


def sources():
    return {**contract.sources(), 'tools/forex_management_state_binding.py':
            prior.sha(prior.read(contract.ROOT / 'tools/forex_management_state_binding.py'))}


def lifecycle(receipt, records, decision, tier):
    """Check explicit state without allocating capital or choosing an action."""
    need = prior.need
    need(receipt['schema'] == 'paper_state_observation.v1', 'state_schema')
    need(receipt['evidence_tier'] == tier, 'state_evidence_tier')
    need(receipt['asof_epoch'] == decision, 'state_not_current_at_decision')
    need(receipt['account_kind'] == 'paper', 'explicit_paper_state_required')
    for name in ('book_id', 'producer_id', 'episode_id'):
        need(type(receipt[name]) is str and bool(receipt[name]), 'state_identity')
    for name in ('producer_source_sha256', 'event_head_sha256'):
        contract.authenticate(receipt[name], records)
    state = receipt['lifecycle']
    need(state in ('FLAT', 'ENTRY_PENDING', 'OPEN', 'REDUCE_PENDING', 'EXIT_PENDING', 'CLOSED'),
         'unknown_lifecycle')
    pending = receipt['pending_orders']
    need(type(pending) is list and len(pending) <= 68, 'bounded_pending_orders')
    ids = set()
    for order in pending:
        need(type(order['order_id']) is str and order['order_id'] and order['order_id'] not in ids,
             'unique_pending_order')
        ids.add(order['order_id'])
        need(order['status'] in ('submitted', 'accepted', 'partially_filled'), 'pending_order_status')
        need(order['kind'] in ('ENTRY', 'REDUCE', 'EXIT'), 'pending_order_kind')
        need(type(order['remaining_units']) is int and order['remaining_units'] > 0, 'pending_units')
        contract.authenticate(order['event_sha256'], records)
    position = receipt['position_state_sha256']
    if state in ('FLAT', 'CLOSED'):
        need(position is None and not pending, 'flat_or_closed_has_exposure')
        return dict(status='declared_flat_wait', lifecycle=state, action='WAIT',
                    reason='no_entry_valuation_or_authority', **FLAGS)
    if state == 'ENTRY_PENDING':
        need(pending and all(p['kind'] == 'ENTRY' for p in pending), 'entry_pending_orders')
        # Partial fills must have an explicit position; no inference from order size.
        need(position is not None or all(p['status'] != 'partially_filled' for p in pending),
             'partial_fill_requires_position')
    else:
        need(position is not None, 'incumbent_position_required')
        expected = {'REDUCE_PENDING': 'REDUCE', 'EXIT_PENDING': 'EXIT'}.get(state)
        need((not pending) if state == 'OPEN' else
             bool(pending) and all(p['kind'] == expected for p in pending), 'lifecycle_pending_mismatch')
    if position is not None:
        body = contract.authenticate(position, records)
        need(body['state_asof_epoch'] == decision, 'incumbent_state_clock')
        need(type(body['base_units']) is int and body['base_units'] > 0 and
             type(body['side']) is int and body['side'] in (-1, 1), 'incumbent_size_side')
    if pending:
        return dict(status='pending_settlement_wait', lifecycle=state, action='WAIT',
                    capacity_released=False, reason='settlement_not_observed', **FLAGS)
    return dict(status='open_requires_management_contract', lifecycle=state, **FLAGS)


def inspect(context, request):
    prior.need(request['schema'] == SCHEMA and request['source_bindings'] == sources(), 'state_binding_source')
    base = request['management_request']
    rows, report = contract.inspect(context, base)
    slots = {r['slot'] for r in rows}
    bindings = request['state_bindings']
    records = request['state_records']
    prior.need(type(bindings) is dict and set(bindings) <= slots, 'state_binding_population')
    prior.need(type(records) is dict and len(records) <= 50000, 'bounded_state_records')
    for pin in records:
        contract.authenticate(pin, records)
    decision = base['combined_request']['decision_epoch']
    result = []
    for original in rows:
        row = deepcopy(original)
        row['management_contract_status'] = row.pop('status')
        outcome = dict(status='state_missing', reason='no_observed_paper_state', **FLAGS)
        if row['slot'] in bindings:
            try:
                receipt = contract.authenticate(bindings[row['slot']], records)
                prior.need(receipt['slot'] == row['slot'], 'state_slot_mismatch')
                outcome = lifecycle(receipt, records, decision, base['evidence_tier'])
                if outcome['status'] == 'open_requires_management_contract':
                    supplied = base['contracts'].get(row['slot'])
                    prior.need(supplied is not None and supplied['state_sha256'] == receipt['position_state_sha256'],
                               'management_position_binding')
                    prior.need(row['management_contract_status'] == 'declared_contract_consistent',
                               'management_prerequisites_not_satisfied')
                    outcome['status'] = 'declared_open_contract_consistent'
                outcome['state_receipt_sha256'] = bindings[row['slot']]
            except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
                outcome = dict(status='state_refused', reason=type(exc).__name__ + ':' + str(exc), **FLAGS)
        if outcome['status'] != 'declared_open_contract_consistent':
            for key in ('values', 'switch_advantage_usd', 'optional_rotation_permitted_by_risk', 'hard_exit_required'):
                row.pop(key, None)
        result.append({**row, **outcome})
    return result, dict(schema=SCHEMA, population=len(result),
        statuses=dict(Counter(r['status'] for r in result)), management_report=report,
        scope='Declared lifecycle consistency only; no independently qualified current state or policy', **FLAGS)


def run(capture, request_path, expected_sha256, output, run_id, *, resume=False, max_new=None):
    raw = prior.read(Path(request_path))
    prior.need(prior.sha(raw) == expected_sha256, 'request_external_hash')
    prior.need(max_new is None or type(max_new) is int and max_new > 0, 'positive_chunk_limit')
    request = json.loads(raw)
    context = contract.combined.forecasts.VerifiedCapture.load(capture,
        request['management_request']['combined_request']['capture_manifest_sha256'])
    rows, report = inspect(context, request)
    payloads = {f'rows_{i//32:04d}.json': prior.encoded(rows[i:i+32]) for i in range(0, len(rows), 32)}
    payloads['REPORT.json'] = prior.encoded(report)
    identity = prior.effective_run_identity(contract={'schema': SCHEMA, 'request_sha256': expected_sha256,
        'required_payloads': sorted(payloads)}, dependency_hashes=sources())
    pub = prior.RunPublisher(output, run_id, identity)
    if (pub.root / 'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root, identity)
        return {'status': 'verified_completed'}
    pub.acquire(recover=resume)
    completed = False
    try:
        receipts = []; added = 0
        for name, raw in payloads.items():
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added >= max_new:
                    return {'status': 'checkpointed', 'next_payload': name}
                added += 1
            receipts.append(pub.write_or_validate_payload(name, raw))
        pub.complete(receipts, set(payloads)); completed = True
    finally:
        if not completed:
            pub.release()
    return {'status': 'completed', **report}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--capture', type=Path, required=True)
    p.add_argument('--request', type=Path, required=True)
    p.add_argument('--sha256', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--run-id', default='state-binding')
    p.add_argument('--resume', action='store_true')
    p.add_argument('--max-new', type=int)
    a = p.parse_args()
    print(json.dumps(run(a.capture, a.request, a.sha256, a.output, a.run_id,
                         resume=a.resume, max_new=a.max_new)))
