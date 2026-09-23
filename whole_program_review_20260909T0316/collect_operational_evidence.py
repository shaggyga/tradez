"""Retain bounded read-only observations for the whole-program assessment."""
from pathlib import Path
from datetime import datetime, timezone
from hashlib import sha256
import json

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent / 'trad'
OBSERVATION = HERE.parent / 'news_identity_integration_20260908/runtime/RUNTIME_WHOLE_PROGRAM_20260909_0316_20260908.json'

def digest(p):
    return sha256(p.read_bytes()).hexdigest()

def main():
    observed = json.loads(OBSERVATION.read_bytes())
    registry_path = PROJECT / 'config/joint_price_news_study_v3_20260908.json'
    registry = json.loads(registry_path.read_bytes())
    bindings = [{'path': k, 'expected_sha256': v, 'actual_sha256': digest(PROJECT/k)}
                for k, v in registry['source_bindings'].items()]
    record = {
        'schema_version': 'whole_program_readonly_observation_20260909',
        'assembled_utc': datetime.now(timezone.utc).isoformat(),
        'runtime_observation': {'path': str(OBSERVATION), 'sha256': digest(OBSERVATION),
                                'finished_utc': observed['finished_utc']},
        'supervisor': {k: observed['supervisor'].get(k) for k in
                       ['status', 'reasons', 'running_worker_count', 'expected_worker_count']},
        'selected_study': observed['api'].get('selected_joint_study', {}).get('counts'),
        'selected_study_reasons': observed['api'].get('selected_joint_study', {}).get('reason_counts'),
        'current_price_observation': observed['api'].get('market'),
        'account_positions_only': observed['api'].get('account_positions_only'),
        'new_study_heartbeat': observed['heartbeats']['joint_price_news_study_v3']['value'],
        'repaired_news_heartbeat': observed['heartbeats']['repaired_news_producer']['value'],
        'registry': {'path': str(registry_path), 'sha256': digest(registry_path),
                     'source_bindings': bindings, 'all_source_hashes_match': all(
                         r['expected_sha256'] == r['actual_sha256'] for r in bindings),
                     **{k: registry[k] for k in ['research_only', 'can_place_orders',
                         'can_promote', 'can_authorize']}},
        'short_process_sample': {
            'source': 'Direct PowerShell Get-Process observations retained in this conversation',
            'end_utc': '2026-09-09T03:17:58.8695246Z',
            'elapsed_seconds': 10.0231199, 'process_count_start': 35, 'process_count_end': 35,
            'aggregate_cpu_core_equivalents': 4.9526121103270455,
            'summed_working_sets_GiB': 4.5377197265625,
            'limitations': 'Short sample, not a controlled benchmark. Working sets may share memory; no utilization ceiling or long-run load inferred.'},
        'scope': 'Read-only assessment. No source/model changes, restarts, broker actions or trading authorization. Existing dated validation is not rewritten.',
    }
    target = HERE / 'OPERATIONAL_OBSERVATION.json'
    with target.open('x', encoding='utf-8') as f:
        json.dump(record, f, indent=2, allow_nan=False)
        f.write('\n')
    print(json.dumps({'path': str(target), 'sha256': digest(target),
                      'all_source_hashes_match': record['registry']['all_source_hashes_match']}))

if __name__ == '__main__':
    main()
