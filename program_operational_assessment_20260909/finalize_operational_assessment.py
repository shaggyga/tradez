"""One final bounded observation and concise report; no runtime/project writes."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import time

OUT = Path(__file__).resolve().parent
PROJECT = Path('C:/Users/zmoor/Documents/forex/trad')
DATA = PROJECT / 'data/oanda_training_manager'
INITIAL = OUT / 'OPERATIONAL_CURRENT_ASSESSMENT_20260909.json'
initial_raw = INITIAL.read_bytes()
initial = json.loads(initial_raw)
bindings = {}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def load(relative):
    path = DATA / relative
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    assert len(raw) < 2*1024*1024 and (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
    observed = time.time()
    bindings[relative] = {'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw), 'observed_epoch': observed}
    return json.loads(raw), observed


def epoch(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


audit, audit_observed = load('state/project_integrity_audit_v1.json')
summary, summary_observed = load('joint_price_news_study_v3/summary.json')
worker, worker_observed = load('joint_price_news_study_v3/heartbeat.json')
news, news_observed = load('local_news_sentiment_repair_v1/heartbeat.json')
integrity = {
    'generated_utc': audit['generated_utc'], 'observed_epoch': audit_observed,
    'age_sec': audit_observed - epoch(audit['generated_utc']),
    'advanced_since_initial': audit['generated_utc'] != initial['integrity']['generated_utc'],
    'status': audit['status'], 'inactive_failure_count': len(audit['inactive_component_failures']),
    'active_shared_or_unknown_failures': audit['active_shared_or_unknown_failures'],
    'live_runtime_assertions': audit['live_runtime_assertions'],
    'new_joint_study_named_checks': [key for key in audit['checks'] if any(term in key for term in ('joint_price_news', 'news_repair', 'pair_local_forecast'))],
    'snapshot_fresh_at_publication': audit['snapshot_fresh_at_publication'],
    'publication_latency_sec': audit['publication_latency_sec'],
    'configured_interval_sec': 300, 'configured_supervisor_max_artifact_age_sec': 1200,
    'publication_freshness_limit_sec': 180,
    'clock_meaning': '180 seconds limits original snapshot-to-publication latency, not time since publication. The 300-second loop and 1200-second liveness bound are separate.',
}
latest = []
counts = {}
missing = []
for row in summary['rows']:
    active = []
    for family, slot in row['families'].items():
        for key, value in slot.get('counts', {}).items():
            if type(value) is int:
                counts[key] = counts.get(key, 0) + value
        forecast = slot.get('latest_forecast')
        if forecast:
            latest.append(forecast['publication_epoch'])
            if forecast['target_epoch'] > summary_observed:
                active.append(family)
        if not forecast or forecast['target_epoch'] <= summary_observed:
            readiness = slot.get('current_readiness') or {}
            missing.append({'instrument': row['instrument'], 'family': family, 'status': slot.get('status'),
                            'reason': slot.get('reason'), 'observed_epoch': slot.get('observed_epoch'),
                            'readiness_status': readiness.get('status'), 'readiness_reason': readiness.get('reason')})
last_error = str(worker.get('last_error') or '')
assert 'http' not in last_error and len(last_error) < 200
news_last_error = str(news.get('last_error') or '')
assert 'http' not in news_last_error and len(news_last_error) < 200
joint = {'summary_generated_epoch': summary['generated_epoch'], 'summary_observed_epoch': summary_observed,
         'heartbeat_generated_epoch': worker['generated_epoch'], 'heartbeat_observed_epoch': worker_observed,
         'summary_payload_hash_valid': summary['payload_sha256'] == digest({k: v for k, v in summary.items() if k != 'payload_sha256'}),
         'heartbeat_summary_binding_matches': worker['summary_sha256'] == digest(summary),
         'pairs_with_forecast_reported': worker['pairs_with_forecast'], 'counts_reported': worker['counts'],
         'errors_cumulative': worker['errors'], 'errors_added_since_initial': worker['errors']-initial['joint_v3_worker']['errors'],
         'heartbeat_publication_errors': worker['heartbeat_publication_errors'], 'retained_last_error': last_error,
         'heartbeat_advanced': worker['generated_epoch'] > initial['joint_v3_worker']['generated_epoch'],
         'latest_publication_epoch': max(latest), 'latest_publication_after_initial_observation': max(latest) > initial['capture_finished_epoch'],
         'ledger_counts_from_worker_verified_summary': counts, 'missing_forecast_rows': missing,
         'scope': 'Source/hash-checked worker summary projection only; no independent ledger rescoring in this task.'}
news_final = {'status': news['status'], 'source_status': news['source_status'],
              'generated_epoch': news['generated_epoch'], 'observed_epoch': news_observed,
              'age_sec': news_observed-news['generated_epoch'], 'errors_cumulative': news['errors'],
              'errors_added_since_initial': news['errors']-initial['repaired_news_producer']['errors'],
              'retained_last_error': news_last_error,
              'heartbeat_advanced': news['generated_epoch'] > initial['repaired_news_producer']['generated_epoch']}
storage = initial['storage']
storage_compact = {key: storage[key] for key in ('generated_utc', 'observed_epoch', 'age_sec', 'status', 'disk')}
storage_compact['growth_projection'] = {key: value for key, value in storage['growth_projection'].items() if key != 'databases'}
storage_compact['projection_caveat'] = 'Short allocation/WAL-growth sample, advisory only; not a forecast of stable daily consumption and may omit separate shared-news capture storage.'
value = {
    'schema_version': 'program_operational_assessment_final_v1_20260909',
    'generated_utc': utc(time.time()), 'initial_evidence_path': str(INITIAL),
    'initial_evidence_sha256': hashlib.sha256(initial_raw).hexdigest(),
    'source_bindings': initial['source_bindings'], 'followup_observed_artifacts': bindings,
    'supervisor_initial_observation': {k: v for k, v in initial['supervision'].items() if k != 'workers'},
    'os_process_checks': initial['os_process_identity_checks'],
    'no_observed_execution_promotion_authorization_processes': not initial['observed_execution_promotion_authorization_processes'],
    'registered_research_controls': initial['registered_research_controls'],
    'account_observation': initial['account'], 'integrity_followup': integrity,
    'joint_followup': joint, 'repaired_news_followup': news_final,
    'clock_initial_observation': initial['clock'], 'storage_initial_observation': storage_compact,
    'disposition': 'Research collection operational with material limitations; trading remains disabled and readiness/profitability is not established.',
    'runtime_actions': False, 'source_changes': False, 'database_queries': 0, 'broker_requests': 0,
}
raw = (json.dumps(value, indent=2) + '\n').encode()
target = OUT / 'OPERATIONAL_FINAL_ASSESSMENT_20260909.json'
with target.open('xb') as handle:
    handle.write(raw)
markdown = f'''# Current operational assessment — 9 September 2026

Research collection is operational; this is not evidence that the whole program is correct or ready to trade. Observations span {utc(initial['capture_started_epoch'])} to {value['generated_utc']}.

- The fresh supervisor reported 17 running workers. An independent OS inventory found every reported PID, including Python wrapper/child pairs. No executor, promotion or authorization worker was observed. Dashboard output was explicitly **not checked** by the supervisor; process liveness does not establish HTTP/UI correctness.
- The selected research registry contains 68 inert contracts; all 20 source bindings matched. Orders, authorization, promotion, account eligibility and proof eligibility remain false. The research supervisor rejects workers outside its collection allowlist.
- The local practice-account snapshot at {initial['account']['snapshot_time_utc']} was {initial['account']['age_sec']:.2f} seconds old: one successful/current account observation, **0 open trades/positions and 0 pending orders**. No account identifiers, monetary values, transaction IDs or credentials are included.
- Integrity audit `{integrity['status']}` at {integrity['generated_utc']} has **{integrity['inactive_failure_count']} failed inactive-component checks and {len(integrity['active_shared_or_unknown_failures'])} active/shared/unknown failures**. Disabled historical evidence remains invalid. Retained executor/proof “healthy/running” booleans are historical claims; their separate live assertions correctly say stopped. There are no explicitly named joint-v3 study checks, so an empty active-failure list does not independently validate the new model pipeline.
- The audit publication advanced: {integrity['advanced_since_initial']}. Its 300-second interval and 1,200-second supervisor liveness allowance are distinct from the 180-second snapshot-to-publication limit. The earlier audit age of {initial['integrity']['age_sec']:.1f} seconds was not itself a violated 180-second publication rule.
- Joint v3 currently reports **{joint['pairs_with_forecast_reported']}/68** forecasts. Summary seal valid: {joint['summary_payload_hash_valid']}; matching heartbeat: {joint['heartbeat_summary_binding_matches']}. Latest publication {utc(joint['latest_publication_epoch'])} is after the initial inspection, showing actual publication progress. The worker retains {joint['errors_cumulative']} cumulative errors ({joint['errors_added_since_initial']} added in this interval); latest retained error is `{joint['retained_last_error']}`. This rejection is preserved rather than treated as a successful forecast.
- The repaired-news producer is {news_final['status']}/{news_final['source_status']}, {news_final['age_sec']:.1f} seconds old; {news_final['errors_cumulative']} cumulative errors and {news_final['errors_added_since_initial']} added during this interval. Current clock observations were synchronized/consistent with no active discontinuity. Neither freshness nor unchanged counters proves content or numerical accuracy.
- Storage says **{storage['status']}** with **{storage['disk']['free_gib']} GiB free**, but its separate growth projection says **{storage['growth_projection']['status']}**: approximately {storage['growth_projection']['estimated_positive_growth_bytes_per_day']/1e9:.2f} GB/day and {storage['growth_projection']['projected_days_to_minimum_free']:.2f} days to the 50-GiB minimum. This is a {storage['growth_projection']['observation_seconds']:.0f}-second allocation/WAL sample, advisory only, and not a proven long-run consumption forecast.

Current missing forecast rows: {', '.join(row['instrument'] + ': ' + str(row['reason']) for row in missing)}. Their original observation clocks are retained in the JSON.

Remaining limitations are the three withheld pairs, retained input-expiry failures, disabled historical analysis/proof components, storage growth, and unestablished out-of-sample trading value. No broker action, database query, heavy audit, test, source edit, worker start/stop or dashboard change was performed.

Evidence: `OPERATIONAL_FINAL_ASSESSMENT_20260909.json` (SHA-256 `{hashlib.sha256(raw).hexdigest()}`), with original source/artifact hashes and the retained initial observation.
'''
report = OUT / 'OPERATIONAL_READINESS_REVIEW_20260909.md'
with report.open('x', encoding='utf-8', newline='\n') as handle:
    handle.write(markdown)
print(json.dumps({'json': str(target), 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw),
                  'report': str(report), 'report_sha256': hashlib.sha256(report.read_bytes()).hexdigest(),
                  'integrity': integrity, 'joint': joint, 'news': news_final}, indent=2))
