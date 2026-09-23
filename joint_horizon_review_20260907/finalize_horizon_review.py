"""Record the completed coverage audit and HTML-only visibility change."""
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json
import xml.etree.ElementTree as ET

BASE = Path(__file__).resolve().parent
PROJECT = BASE.parent / 'trad'
PACKAGE = PROJECT / 'docs/validation/horizon_coverage_20260907'

def digest(path):
    return sha256(path.read_bytes()).hexdigest()

def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))

def ref(path):
    return {'path': path.relative_to(PROJECT).as_posix(), 'sha256': digest(path), 'bytes': path.stat().st_size}

def write_new(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as handle:
        json.dump(data, handle, indent=2, allow_nan=False)
        handle.write('\n')

names = '''
MISSING_JOINT_FORECASTS_20260907T232846Z.json
MISSING_JOINT_FORECASTS_20260907T232846Z.md
ANCHOR_ELIGIBILITY_AUDIT_20260907.json
CURRENT_JOINT_H1_OUTCOME_OBSERVATION_20260907.json
EURUSD_RETAINED_CONJUNCTION_EXAMPLE_20260907.json
EURUSD_JOINT_CONTRIBUTION_20260907T233125Z.json
PUBLICATION_GENERATION_FINDINGS_20260907.md
publication_generation_20260907T233403Z/PUBLICATION_GENERATION_DIAGNOSIS.json
HORIZON_SOURCE_API_INVENTORY_20260907.json
HORIZON_AND_FORECAST_VISIBILITY_REVIEW_20260907.md
FORECAST_VISIBILITY_VALIDATION_RECEIPT_20260907.json
forecast_visibility_tests.xml
LIVE_FORECAST_VISIBILITY_VERIFICATION_20260907.json
JOINT_HORIZON_AND_GATE_REVIEW_20260907.md
JOINT_HORIZON_REVIEW_VALIDATION_20260907.json
'''.split()
# Include the completed independent statistical/diff reviews, excluding raw data.
names += sorted(p.name for p in BASE.iterdir() if p.is_file()
                and p.suffix in {'.md', '.json'} and p.name not in names
                and ('INDEPENDENT' in p.name or 'STATISTICAL' in p.name or 'STRICTNESS' in p.name))
assert len(names) == len(set(names))
owner = read(BASE / 'FORECAST_VISIBILITY_VALIDATION_RECEIPT_20260907.json')
assert owner['validation_status'] == 'passed'
for name, expected in owner['source_sha256'].items():
    assert digest(PROJECT / name) == expected
assert digest(PROJECT / 'oanda_practice_live_dashboard.py') == owner['unchanged_backend_sha256']
assert owner['live_coverage'] == {'combined': 61, 'price_only': 4, 'unavailable': 3, 'total': 68}
cases = list(ET.fromstring((BASE / 'forecast_visibility_tests.xml').read_bytes()).iter('testcase'))
assert len(cases) == 240
assert not any(case.find(tag) is not None for case in cases for tag in ('failure', 'error', 'skipped'))
source_sets = {}
for name in ('joint_price_news_study_v2_20260907.json', 'joint_price_news_study_v1_20260907.json',
             'pair_local_forecast_study_v2_20260907.json'):
    path = PROJECT / 'config' / name
    registry = read(path)
    bindings = {}
    for pair in registry['pairs'].values():
        for family in pair['families'].values():
            bindings.update(family['contract']['source_bindings'])
    assert all(digest(PROJECT / source) == expected for source, expected in bindings.items()), name
    source_sets[name] = {'registry': ref(path), 'all_bound_sources_unchanged': True, 'source_bindings': bindings}

staged = []
for name in names:
    source, target = BASE / name, PACKAGE / name
    raw = source.read_bytes()
    if source.suffix == '.json': json.loads(raw)
    assert target.resolve().is_relative_to(PACKAGE.resolve()) and len(raw) < 1024 * 1024
    assert not target.exists() or target.read_bytes() == raw
    staged.append((source, target, raw))
records = []
for source, target, raw in staged:
    assert source.read_bytes() == raw
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with target.open('xb') as handle: handle.write(raw)
    assert target.read_bytes() == raw
    records.append({**ref(target), 'source': str(source), 'source_sha256': digest(source), 'exact_copy': True})
manifest = PACKAGE / 'CURATED_EVIDENCE_MANIFEST_20260907.json'
write_new(manifest, {'schema_version': 'horizon_coverage_evidence_manifest_v1_20260907',
                     'recorded_utc': datetime.now(timezone.utc).isoformat(), 'file_count': len(records),
                     'records': records, 'scope': 'Curated completed evidence. Raw price/news captures, runtime databases and screenshots remain at their original workspace paths.'})
report = PROJECT / 'docs/FOREX_HORIZON_COVERAGE_REVIEW_20260907.md'
receipt = {
    'schema_version': 'forex_horizon_coverage_review_validation_v1_20260907',
    'recorded_utc': datetime.now(timezone.utc).isoformat(), 'ui_validation_status': 'passed',
    'audit_scope': 'Read-only readiness, anchor, conjunction and horizon audit; HTML-only visibility fix.',
    'report': ref(report), 'evidence_manifest': ref(manifest),
    'dashboard_owner_validation': ref(PACKAGE / 'FORECAST_VISIBILITY_VALIDATION_RECEIPT_20260907.json'),
    'changed_source_sha256': owner['source_sha256'],
    'unchanged_backend_sha256': owner['unchanged_backend_sha256'],
    'focused_tests': owner['test_result'], 'live_coverage': owner['live_coverage'],
    'live_observed_utc': owner['live_observed_utc'],
    'registered_source_sets_verified_unchanged': source_sets,
    'independent_html_diff_review': {
        'reviewer': 'pair_ledger_evaluation', 'result': 'no blocker',
        'html_sha256': owner['source_sha256']['oanda_main_signal_dashboard.html'],
        'scope': 'Source/family freshness, disjoint counts, explicit price-only provenance, original H1 clocks and retained joint blocker.',
    },
    'findings': {
        'four_price_only_pairs': ['EUR_DKK', 'GBP_HKD', 'GBP_PLN', 'GBP_SGD'],
        'three_no_accepted_current_quote_pairs': ['EUR_TRY', 'TRY_JPY', 'USD_TRY'],
        'current_joint_horizon_sec': 3600,
        'joint_technical_context_interactions_implemented': True,
        'vetted_directional_training_examples_in_four_audited_pairs': 0,
        'anchor_audit': 'All four meet unchanged 48/12/8 counts on 3-minute causal anchors; no model fit or new publication was performed.',
        'independence_caveat': 'Dense H1 anchors overlap; current n/4 shrinkage assumes 15-minute spacing. Age-derived context patterns are not independent stories.',
        'next_model_experiment': 'Genuine per-horizon joint forecasts with trained missing-news handling, overlap-aware weighting, shared-currency factors and held-out uncertainty/comparator evaluation.',
        'next_model_implemented_in_this_change': False,
        'prediction_improvement_established': False,
        'publication_generation_mismatch': 'Intermittent API mismatches retained; later exact producer pairs coherent. Failure phase/caching cause not established and guards were not weakened.',
    },
    'models_changed': False, 'registrations_changed': False, 'gates_changed': False,
    'ledger_or_outcome_mutations': False, 'runtime_restarts': 0,
    'orders_enabled': False, 'promotion_enabled': False,
}
target = PROJECT / 'FOREX_HORIZON_COVERAGE_REVIEW_VALIDATION_20260907.json'
write_new(target, receipt)
with (PROJECT / 'FOREX_PROJECT_LOG.md').open('a', encoding='utf-8') as handle:
    handle.write('\n\n## ' + datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC') + ' — forecast visibility and horizon strictness review\n\n'
                 'The live UI now separates combined, price-only and unavailable forecasts, exposing existing technical predictions inline when the joint model is blocked. Dated desktop/mobile verification showed 61 combined, 4 price-only and 3 unavailable; 240 focused tests passed. No model, registration, gate, runtime worker or outcome was changed.\n\n'
                 'The seven-pair audit distinguished four training/context blockers from three TRY pairs without accepted current quotes. A causal historical replay showed that denser anchors supply enough rows to meet the unchanged counts for all four, but overlapping outcomes and age-derived patterns are not independent evidence. The model still publishes H1 only and has no vetted directional-news training examples in the audited pairs. A broader joint horizon curve with partial-input handling and honest uncertainty is explicitly pending, not reported as implemented.\n\n'
                 'Intermittent joint summary/heartbeat mismatch API observations and subsequent coherent producer checks are both retained; their exact failure phase remains unresolved. Source binding guards were preserved. See docs/FOREX_HORIZON_COVERAGE_REVIEW_20260907.md and FOREX_HORIZON_COVERAGE_REVIEW_VALIDATION_20260907.json for evidence and limitations.\n')
template = (BASE.parent / 'joint_price_news_20260907/export_and_verify_final_vault.py').read_text(encoding='utf-8')
exporter = BASE / 'export_and_verify_final_vault.py'
with exporter.open('x', encoding='utf-8') as handle:
    handle.write(template.replace('== 158', '== 160').replace('joint_final_vault_export_verification_v1_20260907',
                                                          'horizon_final_vault_export_verification_v1_20260907'))
print(json.dumps({'receipt': ref(target), 'curated_files': len(records), 'tests_passed': len(cases),
                  'registered_sources_unchanged': True, 'status': 'passed'}))
