"""Bounded, offline joined quote/forecast observations; no action authority.

The externally pinned request binds both inputs and all replay clocks. This
entry point deliberately supports frozen replay only: supplying a recent clock
does not make old source data live. Missing quotes never become indicative fills.
"""
from pathlib import Path
import argparse
from collections import Counter
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'tools'), str(ROOT / 'trad')]
import forex_retained_forecast_receipts as forecasts
import oanda_exact_quote_receipts_v1 as quotes

prior = forecasts.prior
SCHEMA = 'retained_combined_observation.v1'
SOURCES = list(dict.fromkeys(forecasts.SOURCES + [
    'trad/oanda_exact_quote_receipts_v1.py', 'trad/oanda_research_quote_receipt_v1.py',
    'trad/oanda_quote_transport.py', 'tools/forex_retained_combined_observation.py']))


def sources():
    return {n: prior.sha(prior.read(ROOT / n)) for n in SOURCES}


def load_request(path, expected):
    raw = prior.read(Path(path))
    prior.need(prior.sha(raw) == expected, 'request_external_hash')
    request = json.loads(raw)
    prior.need(request['schema'] == SCHEMA and request['mode'] == 'frozen_replay', 'replay_scope_required')
    prior.need(request['source_bindings'] == sources(), 'combined_source_bindings')
    return request


def observe(context, request):
    """Actual joined consumer. Callers must first authenticate the request bytes."""
    prior.need(request['schema'] == SCHEMA and request['mode'] == 'frozen_replay', 'replay_scope_required')
    prior.need(request['source_bindings'] == sources(), 'combined_source_bindings')
    prior.need(context.capture_sha256 == request['capture_manifest_sha256'], 'capture_binding')
    forecast_clock, quote_clock, decision = [forecasts.native.epoch(request[k]) for k in
        ('forecast_observed_epoch', 'quote_observed_epoch', 'decision_epoch')]
    prior.need(context.now <= forecast_clock <= decision and quote_clock <= decision, 'combined_clock_order')
    snapshot = request['quote_snapshot']
    # External request hash authenticates the transport fields too, including
    # declared write-start time. The internal snapshot seal alone excludes them.
    prior.need(snapshot['session_id'] == request['expected_session_id'], 'expected_quote_session')
    prior.need(type(request['expected_generation']) is int and
               snapshot['connection_generation'] == request['expected_generation'], 'expected_quote_generation')
    mapped = quotes.map_receipts(snapshot, observed_epoch=quote_clock, decision_epoch=decision,
        instruments=sorted(context.registry['pairs']))
    current = {(r['forecast']['connection'], r['forecast']['instrument']): r
               for r in context.receipts.values() if r['observation_kind'] == 'capture_observed'}
    targets = request['requested_targets']
    expected = {b['connection'] + '/' + b['instrument'] for b in context.population}
    prior.need(isinstance(targets, dict) and set(targets) == expected, 'exact_requested_population')
    rows = []
    counts = Counter()
    for base in context.population:
        connection, pair = base['connection'], base['instrument']
        receipt = current.get((connection, pair))
        requested = forecasts.native.epoch(targets[connection + '/' + pair])
        quote = mapped['quotes'].get(pair)
        reason = mapped['refusals'].get(pair)
        consumption = context.consume(receipt, observed_epoch=forecast_clock) if receipt else None
        candidate = context.candidate(receipt, consumption, decision_epoch=decision,
            target_epoch=requested, quote=quote) if receipt else None
        if candidate is None:
            status = 'forecast_missing'
        elif candidate['status'] != 'forecast_observation_available':
            status = 'forecast_refused'
        elif candidate['pricing_status'] == 'quote_validated':
            status = 'priced_observation'
        else:
            status = 'quote_missing' if reason == 'quote_missing' else 'quote_refused'
        counts[status] += 1
        rows.append(dict(connection=connection, instrument=pair, status=status,
            coverage_status=base['coverage_status'], requested_target_epoch=requested,
            receipt_sha256=receipt['receipt_sha256'] if receipt else None,
            consumption=consumption, candidate=candidate, quote_reason=reason,
            quote_id=quote['quote_id'] if quote else None,
            quote_status='validated' if quote else ('missing' if reason == 'quote_missing' else 'refused'),
            action_eligible=False, conditional_value_qualified=False, **forecasts.FLAGS))
    report = dict(schema=SCHEMA, mode='frozen_replay', population=len(rows),
        pairs=len(context.registry['pairs']), connections=len(context.entries), statuses=dict(counts),
        quote_pairs_validated=len(mapped['quotes']), quote_refusals=mapped['refusals'],
        capture_manifest_sha256=context.capture_sha256,
        quote_snapshot_sha256=snapshot['snapshot_sha256'], quote_session=request['expected_session_id'],
        quote_generation=request['expected_generation'], forecast_observed_epoch=forecast_clock,
        quote_observed_epoch=quote_clock, decision_epoch=decision,
        native_gate_unchanged=context.previous_report['existing_native_policy_gate'],
        current_live_observation=False, conditional_value_qualified=False, action_eligible=False,
        scope='Joined historical observations, not fresh market availability, conditional value or position decisions',
        **forecasts.FLAGS)
    return rows, report


def run(path, request_path, expected, output, run_id, *, resume=False, max_new=None):
    prior.need(max_new is None or type(max_new) is int and max_new > 0, 'positive_chunk_limit')
    request = load_request(request_path, expected)
    context = forecasts.VerifiedCapture.load(path, request['capture_manifest_sha256'])
    rows, report = observe(context, request)
    chunks = [rows[i:i + 32] for i in range(0, len(rows), 32)]
    required = {f'rows_{i:04d}.json' for i in range(len(chunks))} | {'REPORT.json'}
    identity = prior.effective_run_identity(contract={'schema': SCHEMA, 'request_sha256': expected,
        'population': len(rows), 'required_payloads': sorted(required), 'rows_per_payload': 32},
        dependency_hashes=sources())
    pub = prior.RunPublisher(output, run_id, identity)
    if (pub.root / 'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root, identity)
        return {'status': 'verified_completed'}
    pub.acquire(recover=resume)
    completed = False
    payloads = []
    added = 0
    try:
        for index, chunk in enumerate(chunks):
            name = f'rows_{index:04d}.json'
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added >= max_new:
                    return {'status': 'checkpointed', 'next_row': index * 32}
                added += 1
            payloads.append(pub.write_or_validate_payload(name, prior.encoded(chunk)))
        payloads.append(pub.write_or_validate_payload('REPORT.json', prior.encoded(report)))
        pub.complete(payloads, required)
        completed = True
    finally:
        if not completed:
            pub.release()
    return {'status': 'completed', **report}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--request', type=Path, required=True)
    p.add_argument('--sha256', required=True, help='Externally verified SHA-256 of exact request bytes')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--run-id', default='combined')
    p.add_argument('--resume', action='store_true')
    p.add_argument('--max-new', type=int)
    a = p.parse_args()
    print(json.dumps(run(a.input, a.request, a.sha256, a.output, a.run_id,
        resume=a.resume, max_new=a.max_new)))
