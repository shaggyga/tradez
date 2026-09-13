"""Read retained envelope bytes and replay extracted frozen methods offline."""
import ast
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
from types import SimpleNamespace
import time

BASE = Path(__file__).resolve().parent
RETAINED = BASE.parent/'actual_forward_001'
ROOT = BASE.parents[2]/'trad'
WORKER = ROOT/'oanda_joint_price_news_forecast_study_v3.py'
LEDGER = ROOT/'oanda_causal_forecast_ledger_joint_news_v2.py'
EXPECTED_WORKER = '5dfc29c29fbb61daef4f9f9d5c50f8c029a4d9ed57438288839176e6b55a7f80'
EXPECTED_LEDGER = '17782392f8e3994ff83620109141770b1f1f8182d863444ed4aaa6067bf4ef72'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return sha(encoded(value))


def ref(path):
    raw = path.read_bytes()
    return dict(path=str(path), sha256=sha(raw), bytes=len(raw))


def save(path, value):
    raw = json.dumps(value, indent=2, sort_keys=True, allow_nan=False).encode()+b'\n'
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    assert path.read_bytes() == raw
    return ref(path)


def source_closure(registry):
    found = {name: sha((ROOT/name).read_bytes()) for name in registry['source_bindings']}
    assert found == registry['source_bindings']
    assert found[WORKER.name] == EXPECTED_WORKER and found[LEDGER.name] == EXPECTED_LEDGER
    return found


def extracted_namespace(worker_raw, ledger_raw):
    worker_tree = ast.parse(worker_raw)
    ledger_tree = ast.parse(ledger_raw)
    selected = [n for n in ledger_tree.body if isinstance(n, ast.FunctionDef) and n.name in ('encoded', 'digest', 'number')]
    selected += [n for n in worker_tree.body if isinstance(n, ast.FunctionDef) and n.name in ('input_readiness', 'build_summary')]
    for node in worker_tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in ('FAMILIES', 'SUMMARY_SCHEMA', 'HEARTBEAT_SCHEMA', 'INERT_FLAGS') for t in node.targets):
            selected.append(node)
    original_class = next(n for n in worker_tree.body if isinstance(n, ast.ClassDef) and n.name == 'PairRunner')
    methods = [n for n in original_class.body if isinstance(n, ast.FunctionDef) and n.name in ('finish_work', 'publish_status')]
    selected.append(ast.ClassDef(name='ExtractedRunner', bases=[], keywords=[], body=methods, decorator_list=[]))
    tree = ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[]))
    # No import of the live worker, ledger class, database or network modules.
    namespace = dict(json=json, hashlib=hashlib, math=math)
    exec(compile(tree, str(WORKER)+'#offline_ast_extract', 'exec'), namespace)
    extract = ast.unparse(tree).encode()+b'\n'
    with (BASE/'EXTRACTED_FROZEN_PUBLICATION_METHODS.py.txt').open('xb') as stream:
        stream.write(extract)
    return namespace


def synthetic_cases(namespace):
    family = 'ridge_price_news_v1'
    findings = []
    for result_status in ('ready', 'unavailable', 'exception'):
        stored = {}
        publications = []
        clock = [1000.0]

        def atomic_sink(path, value):
            # Retain immutable serialized bytes, exactly as the real writer
            # does before fsync/replace; no test files or runtime writes.
            raw = namespace['encoded'](value)
            stored[Path(path).name] = raw
            publications.append(dict(name=Path(path).name, sha256=sha(raw)))

        namespace['atomic_json'] = atomic_sink
        namespace['verified_publication'] = lambda ledger: None
        ledger = SimpleNamespace(activated_epoch=900.0, contract_hash='a'*64,
            contract={'cohorts': {family: 'fixture_only'}}, counts=lambda: {},
            issue=lambda *args: None, consume_publications=lambda: None,
            record_abstention=lambda *args, **kwargs: None)
        attempt = dict(epoch=999.0, reason='building', attempt_id='fixture_attempt', bucket=1)
        capture = dict(first_observed_epoch=998.0, max_bar_close_epoch=990.0,
            news_evidence_epoch=998.0, news_expires_epoch=1298.0, status='ready',
            family_readiness={family: dict(ready=True, reasons=[])})
        slot = dict(ledger=ledger, publication=None, building=True, last_attempt=attempt, reason='building')
        state = dict(capture=capture, families={family: slot})
        registry = {'pairs': {'EUR_USD': {'pip_size': .0001}}}
        runner = namespace['ExtractedRunner']()
        runner.states = {'EUR_USD': state}
        runner.registry = registry
        runner.clock = lambda: clock[0]
        runner.study = Path('offline_synthetic_only')
        runner.last_summary = runner.last_heartbeat = 0.0
        runner.summary = None
        runner.errors = runner.heartbeat_errors = 0
        runner.last_error = ''
        runner.active = ('fit', 'EUR_USD', (family, 'fixture_attempt', capture, 'fixture_basis'))
        runner.error = lambda pair, exc: 'synthetic_fit_error'
        runner.publish_status(force=True)
        original_summary_bytes = stored['summary.json']
        original_summary_sha = sha(original_summary_bytes)
        original_payload = json.loads(original_summary_bytes)['payload_sha256']
        assert runner.summary['rows'][0]['families'][family]['last_attempt'] is attempt
        assert runner.summary['rows'][0]['families'][family]['current_readiness']['diagnostics'] is capture['family_readiness'][family]

        def result():
            if result_status == 'exception':
                raise ValueError('synthetic_only')
            return dict(status=result_status, predictions={family: {}}, reasons=['fixture_abstention'])

        runner.future = SimpleNamespace(done=lambda: True, result=result)
        clock[0] = 1004.0
        runner.finish_work()  # The original frozen method performs mutation.
        changed = runner.summary['rows'][0]['families'][family]['last_attempt']['reason']
        assert changed != 'building'
        clock[0] = 1006.0
        runner.publish_status()  # HB due, new summary not due.
        heartbeat = json.loads(stored['heartbeat.json'])
        memory_hash = namespace['digest'](runner.summary)
        payload_replay = namespace['digest']({k:v for k,v in runner.summary.items() if k != 'payload_sha256'})
        assert stored['summary.json'] == original_summary_bytes
        assert heartbeat['summary_sha256'] == memory_hash != original_summary_sha
        assert runner.summary['payload_sha256'] == original_payload != payload_replay
        assert heartbeat['summary_sha256'] not in {x['sha256'] for x in publications if x['name']=='summary.json'}
        # A detached exact-byte snapshot is the sufficient boundary: later
        # mutable live state cannot change its cached hash, rows or old clocks.
        detached = json.loads(original_summary_bytes)
        assert namespace['digest'](detached) == original_summary_sha
        assert detached['rows'][0]['families'][family]['last_attempt']['reason'] == 'building'
        findings.append(dict(fit_outcome=result_status, changed_reason=changed,
            published_summary_sha256=original_summary_sha, heartbeat_named_sha256=heartbeat['summary_sha256'],
            actual_summary_file_unchanged=True, heartbeat_names_unpublished_memory_bytes=True,
            in_memory_payload_seal_invalid=True, last_attempt_alias=True, readiness_diagnostics_alias=True,
            immutable_byte_snapshot_remains_coherent=True, simulated_clock_only=True))
    return findings


def actual_trace():
    result_raw = (RETAINED/'RESULT.json').read_bytes()
    result = json.loads(result_raw)
    blobs = {}
    for path in sorted((RETAINED/'private_envelope_bytes').iterdir()):
        assert path.is_file() and re.fullmatch('[0-9a-f]{64}\\.json', path.name)
        raw = path.read_bytes()
        assert len(raw) <= 1024*1024 and sha(raw) == path.stem
        value = json.loads(raw)
        assert encoded(value) == raw
        blobs[path.stem] = value
    summaries = {key:value for key,value in blobs.items() if value.get('schema_version') == 'joint_price_news_forecast_summary_v3_20260908'}
    assert summaries and all(digest({k:v for k,v in s.items() if k!='payload_sha256'}) == s['payload_sha256'] for s in summaries.values())
    failed = []
    for item in result['observations']:
        obs_path = RETAINED/'observations'/item['observation_persistence']['name']
        obs_raw = obs_path.read_bytes()
        assert sha(obs_raw) == item['observation_persistence']['bytes_sha256']
        observed = json.loads(obs_raw)
        if observed['status'] != 'unavailable':
            continue
        assert observed['reason'] == 'heartbeat_generation_not_observed'
        records = observed['retained_byte_files']
        heartbeat_sha = next(r['bytes_sha256'] for r in records if r['kind']=='heartbeat')
        current_summary_sha = next(r['bytes_sha256'] for r in records if r['kind']=='summary')
        hb = blobs[heartbeat_sha]
        named = hb['summary_sha256']
        assert named not in summaries
        matches = []
        for summary_sha, source in summaries.items():
            if not 0 <= hb['generated_epoch']-source['generated_epoch'] <= 90:
                continue
            changed = deepcopy(source)
            for row in changed['rows']:
                for family, slot in row['families'].items():
                    attempt = slot.get('last_attempt')
                    if not attempt or attempt.get('reason') != 'building':
                        continue
                    attempt['reason'] = 'published'
                    if digest(changed) == named:
                        matches.append(dict(instrument=row['instrument'], family=family,
                            original_summary_sha256=summary_sha, original_summary_generated_epoch=source['generated_epoch'],
                            attempt_id=attempt['attempt_id'], only_field_changed='last_attempt.reason',
                            from_value='building', to_value='published', old_payload_sha256_preserved=True,
                            mutated_payload_seal_valid=digest({k:v for k,v in changed.items() if k!='payload_sha256'})==changed['payload_sha256']))
                    attempt['reason'] = 'building'
        assert len(matches) == 1, 'unexplained_or_ambiguous_heartbeat_hash'
        assert matches[0]['mutated_payload_seal_valid'] is False
        failed.append(dict(index=item['index'], actual_observed_epoch=observed['observed_epoch'],
            heartbeat_file_sha256=heartbeat_sha, heartbeat_generated_epoch=hb['generated_epoch'],
            heartbeat_named_summary_sha256=named, concurrently_read_summary_sha256=current_summary_sha,
            hash_equivalence=matches[0]))
    assert len(failed) == result['status_counts']['unavailable'] == 45
    groups = []
    for item in failed:
        if not groups or item['index'] != groups[-1]['last_index']+1:
            groups.append(dict(first_index=item['index'], last_index=item['index'], samples=0,
                first_observed_epoch=item['actual_observed_epoch'], last_observed_epoch=item['actual_observed_epoch'],
                instruments=set(), named_hashes=set()))
        group=groups[-1]
        group['last_index']=item['index'];group['samples']+=1;group['last_observed_epoch']=item['actual_observed_epoch']
        group['instruments'].add(item['hash_equivalence']['instrument'])
        group['named_hashes'].add(item['heartbeat_named_summary_sha256'])
    for group in groups:
        group['instruments']=sorted(group['instruments']);group['named_hashes']=sorted(group['named_hashes'])
        group['first_to_last_observation_sec']=group['last_observed_epoch']-group['first_observed_epoch']
    return dict(observer_result=ref(RETAINED/'RESULT.json'), observation_count=result['observation_count'],
        observation_started_epoch=result['started_epoch'], observation_completed_epoch=result['completed_epoch'],
        observed_coherent_samples=result['status_counts']['coherent_envelope_observed'],
        unavailable_samples=len(failed), exact_single_field_hash_equivalences=len(failed),
        observed_summary_generations=len(summaries), all_observed_summary_payload_seals_valid=True,
        heartbeat_named_mutated_bytes_absent_from_all_retained_summaries=True,
        retained_generation_selections=result['retained_generation_selection_count'],
        first_acceptance_from_pending_bytes=result['first_pending_acceptance_count'],
        groups=groups, failed_sample_proofs=failed)


def main():
    began=time.time()
    registry=json.loads((RETAINED/'registry.json').read_bytes())
    before=source_closure(registry)
    worker_raw=WORKER.read_bytes();ledger_raw=LEDGER.read_bytes()
    namespace=extracted_namespace(worker_raw, ledger_raw)
    synthetic=synthetic_cases(namespace)
    actual=actual_trace()
    assert source_closure(registry)==before
    evidence=save(BASE/'PRODUCER_ALIAS_DIAGNOSIS_20260909.json', dict(
        schema_version='joint_summary_mutable_alias_diagnosis_v1_20260909', status='confirmed',
        analysis_started_epoch=began, analysis_completed_epoch=time.time(),
        original_worker=ref(WORKER), original_ledger_encoding=ref(LEDGER), exact_current_source_bindings=before,
        extracted_methods=ref(BASE/'EXTRACTED_FROZEN_PUBLICATION_METHODS.py.txt'),
        actual_retained_evidence=actual, offline_extracted_method_cases=synthetic,
        conclusion='All 45 unavailable observations name exact unpublished in-memory summary bytes produced by changing one aliased last_attempt.reason from building to published without renewing the original payload seal.',
        limitations=['The original files remain unchanged; no worker, database, broker, dashboard or registry was mutated.',
            'This diagnoses transport integrity, not forecast quality, row eligibility or trading readiness.',
            'Only the retained 600 observations are claimed; unobserved intervals are not reconstructed.',
            'Readiness diagnostics and forecast objects also alias mutable state; only last_attempt mutation was proved as the cause of these actual failures.'],
        research_only=True, can_place_orders=False, can_promote=False, can_authorize=False,
        account_eligible=False, proof_eligible=False))
    print(json.dumps(dict(status='confirmed', unavailable_samples=45, exact_matches=45,
        groups=actual['groups'], synthetic_cases=len(synthetic), receipt=evidence)))


if __name__=='__main__':
    main()
