"""Package completed joint research evidence without modifying studies or runtime."""
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json
import xml.etree.ElementTree as ET

BASE = Path(__file__).resolve().parent
PROJECT = BASE.parent / 'trad'
PACKAGE = PROJECT / 'docs/validation/joint_price_news_20260907'
FINAL = PACKAGE / 'final_runtime'

def digest(path):
    return sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))

def ref(path):
    return {'path': path.relative_to(PROJECT).as_posix(), 'sha256': digest(path), 'bytes': path.stat().st_size}

def write_new(path, data):
    raw = (json.dumps(data, indent=2, allow_nan=False) + '\n').encode('utf-8')
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('xb') as handle:
        handle.write(raw)

FILES = '''
scheduler_v2/ACTIVATION_RECEIPT_20260907.json
scheduler_v2/JOINT_START_DISPATCH_20260907.json
scheduler_v2/RUNTIME_BEFORE_JOINT_START_20260907.json
scheduler_v2/UI_MONITOR_RELOAD_DISPATCH_20260907.json
scheduler_v2/canonical_runtime/JOINT_V2_CANONICAL_RUNTIME_AUDIT_20260907.json
scheduler_v2/canonical_runtime/WORKER15_STORAGE_RUNTIME_VERIFICATION_20260907.json
scheduler_v2/canonical_runtime/OS_PROCESS_INVENTORY_20260907.json
scheduler_v2/canonical_runtime/STORAGE_HEADROOM_SNAPSHOT_20260907.json
dashboard/LIVE_JOINT_DASHBOARD_VERIFICATION_20260907.json
dashboard/FRESH_NEWS_JOINT_GROWTH_CASE_20260907.json
dashboard/FRESH_V165_JOINT_SOURCE_20260907.json
'''.split()

staged = []
for name in FILES:
    source, target = BASE / name, FINAL / name
    raw = source.read_bytes()
    json.loads(raw)
    assert len(raw) < 3 * 1024 * 1024
    assert target.resolve().is_relative_to(FINAL.resolve())
    if target.exists() and target.read_bytes() != raw:
        raise ValueError('Different existing evidence: ' + name)
    staged.append((source, target, raw))

registry_path = PROJECT / 'config/joint_price_news_study_v2_20260907.json'
registry = read(registry_path)
assert digest(registry_path) == '20fea86e661efd758536b81fa5c474a440feee2e6af48b28a1016b1538d3691e'
assert len(registry['pairs']) == 68
bindings = {}
for pair in registry['pairs'].values():
    assert set(pair['families']) == {'ridge_price_news_v1'}
    contract = pair['families']['ridge_price_news_v1']['contract']
    assert all(contract[k] is False for k in ('can_place_orders', 'can_promote', 'account_eligible', 'proof_eligible'))
    for name, expected in contract['source_bindings'].items():
        assert bindings.get(name, expected) == expected
        bindings[name] = expected
assert len(bindings) == 16
assert all(digest(PROJECT / name) == expected for name, expected in bindings.items())

runtime = read(BASE / FILES[5])
assert runtime['status'] == 'passed' and all(runtime['checks'].values())
source_bindings = dict(runtime['source_hashes'])
source_bindings.update({
    'oanda_practice_live_dashboard.py': 'a5dd6945cc448a6ab86fa5fab8d55aea6040bf197e5a50117fb1bbc218d2f9fe',
    'oanda_main_signal_dashboard.html': '9e279285b2a4e00ad8f52c450f5af6533cb72df445cd55bb50f940c0413a618c',
})
assert all(digest(PROJECT / name) == expected for name, expected in source_bindings.items())

audit = read(BASE / FILES[4])
assert audit['integrity_status'] == 'passed'
assert all(audit['old_registered_sources_unchanged'].values())
for name, sources in audit['old_registered_source_bindings'].items():
    assert all(digest(PROJECT / file) == expected for file, expected in sources.items()), name
price_registry_path = PROJECT / 'config/pair_local_forecast_study_v2_20260907.json'
assert digest(price_registry_path) == 'f7dc675925a765c0bb369282895f2839326476a05eadb7f5df6e0f27a10ed505'
price_registry = read(price_registry_path)
price_sources = {}
for pair in price_registry['pairs'].values():
    for family in pair['families'].values():
        price_sources.update(family['contract']['source_bindings'])
assert len(price_sources) == 9
assert all(digest(PROJECT / name) == expected for name, expected in price_sources.items())

fresh = read(BASE / FILES[9])
assert fresh['status'] == 'passed' and fresh['active_pair_count'] == 61
assert fresh['directional_topics'] == 0 and fresh['guard_replayed_current_topic_count'] == 12
collection = fresh['api_primary_collection']
assert collection['can_place_orders'] is False and collection['can_promote'] is False
assert collection['model_attempts']['counts']['outcomes'] == 0

test_names = {
    'model_inputs/joint_model_inputs_tests.xml': 63,
    'ledger_evaluation/final_tests.xml': 440,
    'scheduler_v2/worker_final_tests.xml': 60,
    'scheduler_v2_operational/operational_gate_tests.xml': 157,
    'dashboard/joint_dashboard_tests.xml': 227,
    'pair_forecast_repair_v2_20260907/news_audit/news_guard_collector_final.xml': 573,
}
test_results = []
for name, expected in test_names.items():
    path = PACKAGE / name
    cases = list(ET.fromstring(path.read_bytes()).iter('testcase'))
    assert len(cases) == expected
    assert not any(c.find(tag) is not None for c in cases for tag in ('failure', 'error', 'skipped'))
    test_results.append({**ref(path), 'passed': expected, 'failed': 0, 'errors': 0, 'skipped': 0})

records = []
for source, target, raw in staged:
    assert source.read_bytes() == raw
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with target.open('xb') as handle:
            handle.write(raw)
    assert target.read_bytes() == raw
    records.append({**ref(target), 'source': str(source), 'source_sha256': digest(source), 'source_destination_equal': True})
final_manifest = FINAL / 'FINAL_RUNTIME_COPY_MANIFEST_20260907.json'
write_new(final_manifest, {
    'schema_version': 'joint_final_runtime_copy_manifest_v1_20260907',
    'generated_utc': datetime.now(timezone.utc).isoformat(), 'status': 'passed',
    'file_count': len(records), 'records': records,
    'scope': 'Exact-byte completed runtime and browser evidence; each retains its original observation clock.',
    'runtime_mutations': False, 'broker_actions': False,
})

report = PROJECT / 'docs/FOREX_JOINT_PRICE_NEWS_20260907.md'
receipt = {
    'schema_version': 'forex_joint_price_news_validation_v1_20260907',
    'recorded_utc': datetime.now(timezone.utc).isoformat(),
    'engineering_validation_status': 'passed',
    'predictive_improvement_status': 'not_established_pending_prospective_joint_outcomes',
    'report': ref(report),
    'active_registry': ref(registry_path),
    'active_pair_count_registered': 68,
    'registered_source_bindings_verified_at_receipt_creation': bindings,
    'dashboard_and_operations_sources_verified_at_receipt_creation': source_bindings,
    'earlier_registered_source_preservation': {
        'pair_v1_eurusd_v1_joint_v1': audit['old_registered_sources_unchanged'],
        'pair_v2_registry': ref(price_registry_path),
        'pair_v2_all_nine_source_bindings_unchanged': True,
        'joint_v1_runtime_role': 'Still running separately to retain original H1 outcomes and scheduler comparison.',
    },
    'separate_component_test_suites': test_results,
    'test_count_scope': 'Suites can overlap and are not summed into an independent-test total. Earlier failures remain in retained evidence.',
    'curated_evidence_manifest': ref(PACKAGE / 'CURATED_EVIDENCE_COPY_MANIFEST_20260907.json'),
    'final_runtime_copy_manifest': ref(final_manifest),
    'activation_receipt': ref(FINAL / FILES[0]),
    'canonical_ledger_audit': ref(FINAL / FILES[4]),
    'runtime_verification': {
        'evidence': ref(FINAL / FILES[5]),
        'observed_utc': runtime['observed_utc'],
        'checks': runtime['checks'],
        'expected_research_workers': 15, 'registered_core_readable_databases': 352,
    },
    'live_dashboard_verification': ref(FINAL / FILES[8]),
    'fresh_news_and_forecast_observation': {
        'evidence': ref(FINAL / FILES[9]),
        'observed_utc': datetime.fromtimestamp(fresh['finished_epoch'], timezone.utc).isoformat(),
        'pairs_with_published_forecasts': fresh['active_pair_count'],
        'news_evidence_as_of_utc': fresh['api_pair_news']['as_of_utc'],
        'news_source_age_sec': fresh['source_age_sec'],
        'replayed_current_context_topics': 12, 'vetted_directional_topics': 0,
        'primary_collection': collection,
        'interpretation': 'News context enters the learned joint model even with no vetted directional topic. Original issued forecasts retain their H1 targets when current news later becomes stale.',
    },
    'price_only_performance_sanity_check': {
        'evidence': ref(PACKAGE / 'price_v2_first_outcomes/PRICE_V2_FIRST_OUTCOMES_ASSESSMENT_20260907.json'),
        'publication_chains_checked': 758, 'retained_outcomes_and_scores': 188,
        'actual_integrity_mismatches': 0,
        'state_space': {'direction_correct': 16, 'denominator': 95, 'positive_after_spread': 0, 'mean_net_bps': -13.821},
        'ridge': {'direction_correct': 58, 'denominator': 93, 'positive_after_spread': 1, 'mean_net_bps': -10.308},
        'scope': 'Separate price-only families and denominators in a narrow correlated initial period. Not joint accuracy, causal news impact, or account P/L.',
    },
    'reorganization': [
        'Combined signals versus actual live prices are primary; positions remain account observations.',
        'Same-joint-model neutral-news prediction plus learned news adjustment equals the joint forecast.',
        'Separately fitted price-only comparison and older model families remain named details.',
        'README and current pipeline/research guides lead to evidence; prior guides retained verbatim in history.',
    ],
    'known_limits_and_pending_acceptance': [
        'No canonical joint H1 outcome was received at the retained 22:51:41 UTC observation; profitability and accuracy improvement are unproven.',
        'Training covers roughly one trading day; overlapping outcomes and correlated pairs require broader sessions and calibration.',
        'First price-only outcomes show spread costs overwhelming small expected moves.',
        'News cycles can exceed the 300-second maximum age; new joint forecasts then withhold instead of renewing old timestamps.',
        'News reconstruction is bounded to 48 hours and 5000 rows, with explicit decoded/compressed byte limits; bound failures remain visible.',
        'Shared compressed snapshots still imply roughly 2 GB/day at one capture per minute; no evidence deletion introduced.',
        'Broader immutable training history, currency co-movement models, and prior joint scheduler retirement remain separately logged pending work.',
        'Curated vault evidence excludes databases and full raw replay archives, which remain in the actual local project/workspace.',
    ],
    'orders_enabled': False, 'promotion_enabled': False, 'proof_eligible': False,
    'scheduled_health_automation': 'paused',
    'requested_in_chat_one_hour_watch': 'completed earlier; preserved separate watch report',
    'receipt_creation_runtime_mutations': False,
}
target = PROJECT / 'FOREX_JOINT_PRICE_NEWS_VALIDATION_20260907.json'
write_new(target, receipt)
print(json.dumps({'receipt': ref(target), 'final_runtime_files': len(records), 'source_bindings_verified': 16,
                  'separate_passed_case_counts': list(test_names.values()), 'status': 'passed'}))
