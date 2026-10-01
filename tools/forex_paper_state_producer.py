"""Explicit prospective empty paper episode, with immutable WAIT observations.

This producer has no fill/entry or position-import interface. Its state proof is
limited to its own newly initialized book, not an existing or broker portfolio.
"""
from pathlib import Path
from collections import Counter
import argparse
import json
import os
import re
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
import forex_management_state_binding as binding

prior = binding.prior
forecasts = binding.contract.combined.forecasts
ROOT = binding.contract.ROOT
SCHEMA = 'prospective_empty_paper_episode.v1'


def sources():
    return {**binding.sources(), 'tools/forex_paper_state_producer.py': prior.sha(prior.read(Path(__file__)))}


def digest(body):
    return prior.sha(prior.encoded(body))


def exclusive(path, body):
    raw = prior.encoded(body)
    prior.need(not path.parent.is_symlink(), 'book_symlink')
    with path.open('xb') as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    prior.need(prior.read(path) == raw, 'durable_readback')


def initialize(root, episode_id, registry_sha256, *, clock=time.time):
    root = Path(root)
    prior.need(re.fullmatch(r'[a-z0-9_]{1,80}', episode_id) is not None, 'episode_id')
    prior.need(re.fullmatch(r'[0-9a-f]{64}', registry_sha256) is not None, 'registry_identity')
    now = forecasts.native.epoch(clock())
    manager = prior.load_reference(ROOT / 'trad')
    # Reuse exact reference-accounting initialization, never a historical final state.
    initial = manager.jsonable(manager.flat_state())
    config = dict(schema=SCHEMA, episode_id=episode_id, registry_sha256=registry_sha256,
        created_epoch=now, source_bindings=sources(), policy='empty_book_wait_only',
        initial_state=initial, no_imported_positions=True, **binding.contract.FLAGS)
    root.mkdir(parents=True, exist_ok=False)
    exclusive(root/'genesis.json', config)
    return {'genesis_sha256': digest(config), 'created_epoch': now}


def journal(root, expected_head=None):
    root = Path(root)
    prior.need(root.is_dir() and not root.is_symlink(), 'regular_book_directory')
    config = json.loads(prior.read(root/'genesis.json'))
    prior.need(config['schema'] == SCHEMA and config['source_bindings'] == sources(), 'producer_source_identity')
    prior.need(config['policy'] == 'empty_book_wait_only' and config['no_imported_positions'] is True,
               'explicit_new_empty_book')
    prior.need(config['initial_state'] == {'realized_usd':'0', 'position':None}, 'reference_flat_initialization')
    prior.need(all(config.get(k) == v for k,v in binding.contract.FLAGS.items()), 'non_authorizing_book')
    files = list(root.iterdir())
    prior.need(len(files) <= 1001 and all(p.name == 'genesis.json' or
        re.fullmatch(r'event_[0-9]{6}\.json', p.name) for p in files), 'bounded_book_inventory')
    head = digest(config); epoch = forecasts.native.epoch(config['created_epoch']); events=[]
    for index, path in enumerate(sorted(p for p in files if p.name != 'genesis.json')):
        prior.need(path.name == f'event_{index:06d}.json', 'event_sequence_gap')
        event = json.loads(prior.read(path))
        prior.need(event['schema'] == SCHEMA and event['sequence'] == index and
            event['parent_sha256'] == head and event['episode_id'] == config['episode_id'], 'event_chain')
        prior.need(event['action'] == 'WAIT' and event['position'] is None and event['pending_orders'] == [],
                   'wait_only_event')
        now = forecasts.native.epoch(event['decision_epoch'])
        prior.need(now >= epoch and event['registry_sha256'] == config['registry_sha256'], 'event_clock_or_registry')
        forecasts.native._hash_text(event['capture_sha256'])
        epoch = now; head = digest(event); events.append(event)
    prior.need(expected_head is None or head == expected_head, 'externally_pinned_event_head')
    return config, events, head


def observe(root, capture, capture_sha256, *, clock=time.time):
    config, events, head = journal(root)
    prior.need(len(events) < 1000, 'episode_event_limit')
    context = forecasts.VerifiedCapture.load(capture, capture_sha256)
    decision = forecasts.native.epoch(clock())
    prior.need(config['registry_sha256'] == context.manifest['registry_sha256'], 'episode_registry_changed')
    prior.need(config['created_epoch'] <= context.manifest['capture_started_epoch'] <= context.now <= decision,
               'prospective_capture_order')
    prior.need(not events or decision >= events[-1]['decision_epoch'], 'clock_reversed')
    event = dict(schema=SCHEMA, sequence=len(events), parent_sha256=head, episode_id=config['episode_id'],
        registry_sha256=config['registry_sha256'], capture_sha256=capture_sha256,
        decision_epoch=decision, action='WAIT', position=None, pending_orders=[])
    # Concurrent writers contend on the same immutable sequence, not a mutable head.
    exclusive(Path(root)/f'event_{len(events):06d}.json', event)
    return {'event_head_sha256':digest(event), 'sequence':len(events), 'decision_epoch':decision}


def inspect(root, expected_head, capture, capture_sha256):
    config, events, head = journal(root, expected_head)
    prior.need(bool(events), 'observation_event_required')
    event=events[-1]
    prior.need(event['capture_sha256'] == capture_sha256, 'event_capture_binding')
    context=forecasts.VerifiedCapture.load(capture, capture_sha256)
    # Captured legacy quotes are observable market inputs, not exact-price fill receipts.
    legacy_quotes=json.loads(prior.read(Path(capture)/'quotes.json'))
    legacy_map=legacy_quotes.get('quotes', {})
    prior.need(isinstance(legacy_map,dict), 'legacy_quote_inventory')
    prior.need(config['registry_sha256'] == context.manifest['registry_sha256'] and
        config['created_epoch'] <= context.manifest['capture_started_epoch'] <= context.now <= event['decision_epoch'],
        'observation_lineage')
    current={(r['forecast']['connection'],r['forecast']['instrument']):r
        for r in context.receipts.values() if r['observation_kind']=='capture_observed'}
    records={digest(config):config, head:event}
    rows=[]
    for base in context.population:
        key=base['connection'],base['instrument']; slot='/'.join(key)
        state=dict(schema='paper_state_observation.v1', slot=slot, evidence_tier='preserved_observation',
            account_kind='paper',book_id=config['episode_id'],episode_id=config['episode_id'],
            producer_id=SCHEMA,producer_source_sha256=digest(config),event_head_sha256=head,
            asof_epoch=event['decision_epoch'],lifecycle='FLAT',pending_orders=[],position_state_sha256=None)
        life=binding.lifecycle(state,records,event['decision_epoch'],'preserved_observation')
        receipt=current.get(key);candidate=None
        if receipt:
            consumption=context.consume(receipt,observed_epoch=event['decision_epoch'])
            candidate=context.candidate(receipt,consumption,decision_epoch=event['decision_epoch'],
                target_epoch=receipt['forecast']['target_epoch'])
        rows.append(dict(slot=slot,**life,state_receipt=state,state_receipt_sha256=digest(state),
            paper_book_proof='verified_new_empty_wait_only_episode', forecast_candidate=candidate,
            forecast_receipt_sha256=receipt['receipt_sha256'] if receipt else None,
            legacy_quote_observed=key[1] in legacy_map,
            quote_status='exact_execution_receipt_not_bound',
            economic_status='entry_and_conditional_economics_not_bound'))
    report=dict(schema=SCHEMA,episode_id=config['episode_id'],event_head_sha256=head,
        registry_sha256=config['registry_sha256'],capture_sha256=capture_sha256,
        decision_epoch=event['decision_epoch'],population=len(rows),paper_books=1,
        positions=0,pending_orders=0,actions={'WAIT':len(rows)},
        legacy_quote_input_sha256=context.manifest['files']['quotes.json']['sha256'],
        legacy_quote_pairs_observed=len(set(legacy_map)&set(context.registry['pairs'])),
        forecast_statuses=dict(Counter(r['forecast_candidate']['status'] if r['forecast_candidate'] else 'missing' for r in rows)),
        paper_book_proof='verified_new_empty_wait_only_episode',current_live_observation=False,
        scope='Replay of actually recorded prospective empty-book observation; not trading or managed-position qualification',
        **binding.FLAGS)
    return rows,report


def replay(root, head, capture, capture_sha256, output, run_id, *, resume=False,max_new=None):
    prior.need(max_new is None or type(max_new) is int and max_new>0, 'positive_chunk_limit')
    rows,report=inspect(root,head,capture,capture_sha256)
    payloads={f'rows_{i//32:04d}.json':prior.encoded(rows[i:i+32]) for i in range(0,len(rows),32)}
    payloads['REPORT.json']=prior.encoded(report)
    identity=prior.effective_run_identity(contract={'schema':SCHEMA,'event_head':head,
        'capture':capture_sha256,'required_payloads':sorted(payloads)},dependency_hashes=sources())
    pub=prior.RunPublisher(output,run_id,identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root,identity);return {'status':'verified_completed'}
    pub.acquire(recover=resume);completed=False
    try:
        receipts=[];added=0
        for name,raw in payloads.items():
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added>=max_new:return {'status':'checkpointed'}
                added+=1
            receipts.append(pub.write_or_validate_payload(name,raw))
        pub.complete(receipts,set(payloads));completed=True
    finally:
        if not completed:pub.release()
    return {'status':'completed',**report}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('initialize');a.add_argument('--book',type=Path,required=True);a.add_argument('--episode',required=True);a.add_argument('--registry-sha256',required=True)
    for name in ['observe','replay']:
        a=sub.add_parser(name);a.add_argument('--book',type=Path,required=True);a.add_argument('--capture',type=Path,required=True);a.add_argument('--capture-sha256',required=True)
        if name=='replay':
            a.add_argument('--head',required=True);a.add_argument('--output',type=Path,required=True);a.add_argument('--run-id',default='paper-state');a.add_argument('--resume',action='store_true');a.add_argument('--max-new',type=int)
    a=p.parse_args()
    if a.command=='initialize':result=initialize(a.book,a.episode,a.registry_sha256)
    elif a.command=='observe':result=observe(a.book,a.capture,a.capture_sha256)
    else:result=replay(a.book,a.head,a.capture,a.capture_sha256,a.output,a.run_id,resume=a.resume,max_new=a.max_new)
    print(json.dumps(result))
