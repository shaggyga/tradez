"""Freeze only new model/input and independent review evidence outside trad."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
TRAD = OUT.parent/'trad'
sys.path.insert(0, str(TRAD))
import oanda_pair_local_models_v2 as models
import oanda_causal_forecast_inputs_pair_v2 as inputs


def binding(path):
    value = path.read_bytes()
    return {'path':str(path.resolve()), 'sha256':hashlib.sha256(value).hexdigest(), 'bytes':len(value)}


def write_new(name, value):
    with (OUT/name).open('x', encoding='utf-8') as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write('\n')
    return binding(OUT/name)


def xml_counts(path):
    suites = list(ET.parse(path).getroot().iter('testsuite'))
    return {key:sum(int(s.get(key, 0)) for s in suites) for key in ('tests','failures','errors','skipped')}


source_names = ('oanda_pair_local_models_v2.py','oanda_causal_forecast_inputs_pair_v2.py',
                'test_oanda_pair_local_models_v2.py','test_oanda_causal_forecast_inputs_pair_v2.py')
sources = [binding(TRAD/name) for name in source_names]
assert sources[0]['sha256'] == inputs.NUMERICAL_SOURCE_SHA256
assert sources[0]['sha256'] == '294c63bf0ed873395c4850a8722a2622eaa2bc563ceb7ecf9886764c63d8bf24'
counts = xml_counts(OUT/'models_inputs_deadline_tests.xml')
assert counts == {'tests':73,'failures':0,'errors':0,'skipped':0}, counts
registry_path = TRAD/'config/pair_local_forecast_study_v1_20260907.json'
registry = json.loads(registry_path.read_bytes())
old_checks = [dict(binding(TRAD/name), expected_sha256=sha) for name,sha in registry['source_bindings'].items()]
assert len(old_checks) == 9 and all(item['sha256'] == item['expected_sha256'] for item in old_checks)
now = datetime.now(timezone.utc).isoformat()
result = {
    'schema_version':'pair_v2_models_inputs_validation_20260907', 'status':'passed', 'generated_utc':now,
    'source_bindings':sources, 'model_version':models.MODEL_VERSION, 'parameters':dict(models.PARAMETERS),
    'primary_test_evidence':dict(binding(OUT/'models_inputs_deadline_tests.xml'), counts=counts),
    'previous_test_evidence':dict(binding(OUT/'models_inputs_initial_tests.xml'),
        counts=xml_counts(OUT/'models_inputs_initial_tests.xml'),
        note='67 passing cases before current-clock cache expiry hardening; pytest cache warning only.'),
    'independent_review':{'reviewer':'pair_ledger_evaluation', 'status':'closed_no_remaining_blocker',
        'scope':['historical feature cutoff and exact mature H1 endpoints','session and elapsed-time gap semantics',
                 'pip/variance units and uncalibrated shrunk estimates','independent family readiness',
                 'actual computation start/completion 900-second freshness checks'],
        'early_failure_observation_note':'An early source I/O failure may have no source-observation clock; root worker handles it as unavailable and records a separate actual event clock.'},
    'apis':{
        'models.family_readiness':'(rows_by_epoch, cutoff_epoch, *, instrument, pip_size) -> readiness by family',
        'models.predict_all':'(rows_by_epoch, cutoff_epoch, *, instrument, pip_size, families=None) -> available family tuples(expected_signed_pips, probability_up, diagnostics)',
        'inputs.capture_inputs':'(candle_root, instrument, *, pip_size, clock=time.time) -> sealed observed capture',
        'inputs.validate_capture':'(capture, *, instrument=None, pip_size=None) -> validates original observed bytes, identity, geometry, clocks and derived readiness',
        'inputs.compute_predictions':'(capture, *, families=None, clock=time.time) -> ready/partial/abstain; selected available predictions with actual computation_started_epoch/computed_epoch',
    },
    'capture_status_semantics':'ready means source/availability validation succeeded; readiness_status and family_readiness separately expose zero, one or two ready models.',
    'freshness_policy':'Original captured close must be no more than 900 seconds old at capture and at actual computation start/end; ledger independently checks actual issue. No clock is refreshed when cached source bytes are reused.',
    'input_bounds':{'maximum_real_rows':4096,'maximum_tail_bytes':4*1024*1024,'no_price_imputation':True,
                    'session_boundary_gap_sec_exclusive':1800,'historical_label_exact_endpoint_sec':3600},
    'probability_scope':'Uncalibrated, deliberately shrunk engineering estimates; probabilities are not measured accuracy. Neutral expected movement emits side 0 and probability 0.5.',
    'outcome_independent_constants':'Constants were fixed from engineering design before any v2 forecast outcome; no observed outcome tuning or profitability claim.',
    'draft_readiness_evidence':dict(binding(OUT/'MODELS_INPUTS_V2_READONLY_PREFLIGHT_20260907.json'),
        note='Read-only 68-archive preflight at 21:03:42–21:03:56 UTC: 65 ready captures with both family input minima; 3 stale TRY captures. No fitting or issuance. Artifact binds earlier adapter 294771c0 before current-computation cache deadline checks; unchanged input-readiness criteria. Not a final-source live forecast count.'),
    'frozen_v1_registry':binding(registry_path),'frozen_v1_source_checks':old_checks,
    'runtime_started_by_this_validation':False,'registered_config_changed':False,'broker_requests':0,
    'can_place_orders':False,'can_promote':False,
}
review_names = ('oanda_causal_forecast_ledger_pair_v2.py','oanda_fixed_forecast_evaluation_pair_v2.py')
review_sources = [binding(TRAD/name) for name in review_names]
assert [s['sha256'] for s in review_sources] == [
    '7bdfbe1fb7076982e6192db39da5d24ae28d70811496115da31cc42ddf39dc8f',
    '16b78433bf70bf46910effd60068ef7a43c690c3f7ad2c729a567e0620b205c2']
review = {'schema_version':'pair_v2_ledger_evaluator_independent_review_20260907',
    'status':'passed','generated_utc':now,'reviewer':'pair_model_inputs','source_bindings':review_sources,
    'method':'Read-only source review; no extra tests or runtime database actions performed by this reviewer.',
    'blocking_findings':[],
    'verified_design':['Single selected family contract and cohort; absent sibling never censors eligible output',
        'Original cached capture availability preserved, fitting starts after reserved attempt, issue happens after actual computation',
        'Actual issue separately enforces the registered 900-second input age boundary',
        'Unique family cadence bucket and original quote reference prevent repeat forecast publication',
        'Failed-readiness raw inputs/results are retained as diagnostic evidence without forecast authority',
        'Commit receipt and separate consumer observation precede eligible entry; original target remains reference plus 3600',
        'Read-only consistent export retains actual attempt and computation clocks; exact decimal scoring includes directional denominator and neutral abstentions'],
    'limits':['Review is not an independent proof of historical availability or prediction effectiveness.',
              'Root and ledger-owner test receipts supply executable validation separately.']}
print(json.dumps({'models_inputs':write_new('MODELS_INPUTS_V2_VALIDATION_20260907.json',result),
                  'ledger_review':write_new('LEDGER_EVALUATOR_V2_INDEPENDENT_REVIEW_20260907.json',review)}, indent=2))
