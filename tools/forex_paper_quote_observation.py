"""Replay an authenticated prospective paper observation with worker-bound quotes.

This joins existing consumers; it neither enters positions nor qualifies economics.
The outer request pins the actual heartbeat read and immutable paper event.
"""
from pathlib import Path
import argparse
import datetime
import json
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parent))
import forex_paper_state_producer as paper

combined = paper.binding.contract.combined
prior = paper.prior
SCHEMA = 'paper_quote_observation.v1'


def sources():
    return {**paper.sources(), 'tools/forex_paper_quote_observation.py': prior.sha(prior.read(Path(__file__)))}


def worker_binding(heartbeat, snapshot, observed_epoch, decision_epoch):
    """Validate the independently captured worker generation and freshness."""
    epoch = paper.forecasts.native.epoch
    observed, decision = epoch(observed_epoch), epoch(decision_epoch)
    stamp = datetime.datetime.fromisoformat(heartbeat['updated_at'])
    prior.need(stamp.tzinfo is not None, 'worker_timestamp_timezone')
    updated = stamp.timestamp()
    prior.need(updated <= observed <= decision and 0 <= decision-updated <= 15, 'worker_heartbeat_stale_or_future')
    prior.need(heartbeat['status'] == 'running' and heartbeat['phase'] == 'streaming' and
               heartbeat['role'] == 'practice_007_quote_stream', 'worker_not_streaming')
    detail = heartbeat['details']; stream = detail['stream']; exact = stream['exact_receipts']
    prior.need(detail['can_place_orders'] is False and detail['instrument_count'] == len(snapshot['instruments']), 'worker_scope')
    prior.need(stream['connected'] is True and stream['raw_price_observer_errors'] == 0, 'worker_stream_error')
    prior.need(type(exact['connection_generation']) is int and
               exact['connection_generation'] == stream['connection_generation'] == snapshot['connection_generation'] and
               exact['session_id'] == snapshot['session_id'], 'worker_generation_mismatch')
    transport = exact['transport']
    prior.need(transport['enabled'] is True and transport['thread_alive'] is True and
               transport['last_error'] == '' and transport['mirror_error'] == '', 'worker_transport_error')
    age = epoch(transport['last_success_age_sec'])
    prior.need(age >= 0 and age + decision-updated <= 15, 'worker_publication_stale')
    return dict(session_id=exact['session_id'], connection_generation=exact['connection_generation'],
                updated_epoch=updated, observed_epoch=observed, decision_epoch=decision,
                heartbeat_sha256=paper.digest(heartbeat))


def inspect(book, capture, request):
    prior.need(request['schema'] == SCHEMA and request['source_bindings'] == sources(), 'paper_quote_source_identity')
    join = request['combined_request']; decision = join['decision_epoch']
    proof = worker_binding(request['heartbeat'], join['quote_snapshot'], request['heartbeat_observed_epoch'], decision)
    prior.need(join['expected_session_id'] == proof['session_id'] and
               join['expected_generation'] == proof['connection_generation'], 'joined_worker_identity')
    context = paper.forecasts.VerifiedCapture.load(capture, join['capture_manifest_sha256'])
    paper_rows, paper_report = paper.inspect(book, request['event_head_sha256'], capture, context.capture_sha256)
    prior.need(paper_report['decision_epoch'] == decision, 'paper_quote_decision_mismatch')
    rows, report = combined.observe(context, join)
    state = {row['slot']: row for row in paper_rows}
    prior.need(set(state) == {r['connection']+'/'+r['instrument'] for r in rows}, 'paper_quote_population')
    result = []
    for row in rows:
        slot = row['connection']+'/'+row['instrument']; p = state[slot]
        result.append(dict(slot=slot, observation=row, state_receipt=p['state_receipt'],
            state_receipt_sha256=p['state_receipt_sha256'], paper_book_proof=p['paper_book_proof'],
            action='WAIT', economic_status='entry_and_conditional_economics_not_bound',
            **paper.binding.FLAGS))
    return result, dict(schema=SCHEMA, population=len(result), joined_report=report,
        worker_binding=proof, paper_report=paper_report, paper_books=1, positions=0,
        pending_orders=0, recorded_book_events=1, scope='Recorded prospective observation replay; not current availability or trading qualification',
        **paper.binding.FLAGS)


def run(book, capture, request_path, expected, output, run_id, *, resume=False, max_new=None):
    prior.need(max_new is None or type(max_new) is int and max_new > 0, 'positive_chunk_limit')
    raw = prior.read(Path(request_path)); prior.need(prior.sha(raw) == expected, 'request_external_hash')
    rows, report = inspect(book, capture, json.loads(raw))
    payloads = {f'rows_{i//32:04d}.json': prior.encoded(rows[i:i+32]) for i in range(0,len(rows),32)}
    payloads['REPORT.json'] = prior.encoded(report)
    identity = prior.effective_run_identity(contract=dict(schema=SCHEMA, request_sha256=expected,
        required_payloads=sorted(payloads)), dependency_hashes=sources())
    pub = prior.RunPublisher(output,run_id,identity)
    if (pub.root/'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root,identity);return {'status':'verified_completed'}
    pub.acquire(recover=resume);completed=False;added=0;receipts=[]
    try:
        for name, body in payloads.items():
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added >= max_new:return {'status':'checkpointed'}
                added += 1
            receipts.append(pub.write_or_validate_payload(name,body))
        pub.complete(receipts,set(payloads));completed=True
    finally:
        if not completed:pub.release()
    return {'status':'completed', **report}


def record(book, output, quote_path, heartbeat_path, *, clock=time.time):
    """One prospective read-only market observation; append only a paper WAIT event.

    Output must be new. A failure is preserved there and never silently retried.
    This is an observation command, not an unattended scheduler or trade executor.
    """
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    def save(name,value):
        paper.exclusive(output/name,value)
    try:
        captured=prior.capture(output/'capture');save('CAPTURE_RESULT.json',captured)
        pin=captured['manifest_sha256'];context=paper.forecasts.VerifiedCapture.load(output/'capture',pin)
        snapshot=combined.quotes.load_quote_snapshot(Path(quote_path));quote_clock=clock()
        heartbeat=json.loads(prior.read(Path(heartbeat_path)));heartbeat_clock=clock()
        # Refuse an unhealthy generation before mutating even the paper journal.
        worker_binding(heartbeat,snapshot,heartbeat_clock,clock())
        event=paper.observe(book,output/'capture',pin,clock=clock);save('OBSERVATION.json',event)
        current={(r['forecast']['connection'],r['forecast']['instrument']):r
                 for r in context.receipts.values() if r['observation_kind']=='capture_observed'}
        targets={}
        for base in context.population:
            key=base['connection'],base['instrument'];receipt=current.get(key)
            # Missing slots retain a requested horizon, never a fabricated forecast.
            targets['/'.join(key)]=(receipt['forecast']['target_epoch'] if receipt else
                event['decision_epoch']+context.entries[key[0]]['horizon_minutes']*60)
        join=dict(schema=combined.SCHEMA,mode='frozen_replay',source_bindings=combined.sources(),
            capture_manifest_sha256=pin,forecast_observed_epoch=context.now,quote_observed_epoch=quote_clock,
            decision_epoch=event['decision_epoch'],quote_snapshot=snapshot,
            expected_session_id=snapshot['session_id'],expected_generation=snapshot['connection_generation'],
            requested_targets=targets)
        request=dict(schema=SCHEMA,source_bindings=sources(),event_head_sha256=event['event_head_sha256'],
            heartbeat=heartbeat,heartbeat_observed_epoch=heartbeat_clock,combined_request=join)
        save('REQUEST.json',request);request_hash=paper.digest(request)
        rows,report=inspect(book,output/'capture',request)
        save('REPORT.json',report)
        receipt=dict(request_sha256=request_hash,event_head_sha256=event['event_head_sha256'],
            recorded_epoch=clock(),source_bindings=sources(),report_sha256=paper.digest(report),
            book_mutation='one_immutable_WAIT_event', market_reads_only=True, **paper.binding.FLAGS)
        save('RECEIPT.json',receipt)
        return receipt
    except Exception as exc:
        save('FAILURE.json',dict(error=type(exc).__name__,detail=str(exc),observed_epoch=clock()))
        raise


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    a=sub.add_parser('record')
    for name in ('book','output','quote-path','heartbeat-path'):a.add_argument('--'+name,type=Path,required=True)
    a=sub.add_parser('replay')
    for name in ('book','capture','request','output'):a.add_argument('--'+name,type=Path,required=True)
    a.add_argument('--sha256',required=True);a.add_argument('--run-id',default='paper-quotes')
    a.add_argument('--resume',action='store_true');a.add_argument('--max-new',type=int)
    a=p.parse_args()
    result=(record(a.book,a.output,a.quote_path,a.heartbeat_path) if a.command=='record' else
        run(a.book,a.capture,a.request,a.sha256,a.output,a.run_id,resume=a.resume,max_new=a.max_new))
    print(json.dumps(result))
