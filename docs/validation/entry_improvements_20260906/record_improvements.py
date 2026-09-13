"""Freeze completed offline improvements and update current navigation, never runtime."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
EVIDENCE = ROOT / 'docs/validation/entry_improvements_20260906'
NOW = datetime.now(timezone.utc).isoformat()
sys.dont_write_bytecode = True
sys.path[:0] = [str(ROOT), str(ROOT.parent)]

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path): return json.loads(path.read_text(encoding='utf-8-sig'))
def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')

suite = ET.parse(OUT/'integration_tests.xml').getroot().find('testsuite')
assert suite is not None and int(suite.get('failures')) == int(suite.get('errors')) == 0
assert int(suite.get('tests')) >= 367
observed = load(OUT/'runtime_verification.json')
assert observed['remaining_project_processes'] == 0
assert all(task['state'] == 'Disabled' for task in observed['scheduled_tasks'])
assert not EVIDENCE.exists()
EVIDENCE.mkdir(parents=True)

for folder in ('entry_audit', 'exact_scoring', 'joint_baselines', 'dashboard_diagnostics', 'before_source', 'before_current_records'):
    source = OUT/folder
    if source.exists():
        # Only small evidence/source files; exclude test fixture trees and caches.
        for path in source.iterdir():
            if path.is_file() and path.suffix.lower() in {'.py', '.json', '.md', '.txt', '.xml'}:
                target = EVIDENCE/folder/path.name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, target)
for name in ('integration_tests.xml', 'guard_blocked_publisher_fixtures.xml', 'run_tests.py', 'runtime_verification.json', 'verify_stopped.ps1', 'record_improvements.py'):
    shutil.copyfile(OUT/name, EVIDENCE/name)

contract_path = ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json'
contract = load(contract_path)
for name, digest in contract['source_bindings'].items():
    assert sha(ROOT/name) == digest, name
prior_names = ['FOREX_PREDICTION_SANITY_VALIDATION_20260906.json',
    'FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json', 'FOREX_CAUSAL_TIMING_REPAIR_VALIDATION_20260906.json',
    'FOREX_PREDICTION_QUALITY_20260906.json', 'FOREX_FIXED_EVALUATION_RESULTS_20260906.json',
    'FOREX_FIXED_EVALUATION_VALIDATION_20260906.json', 'FOREX_OPTIMIZATION_VALIDATION_20260906.json',
    'FOREX_REPAIR_VALIDATION_20260905.json']
prior_expected = {
    'FOREX_PREDICTION_SANITY_VALIDATION_20260906.json':'be23bf8a32f6ad622bfd4b517f18dc9a3d748224c2936320659665c0eb2b30ae',
    'FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json':'79623133ed757680b46f8e5f4c724b6ca5d93a85ad05888cf899d99989fff8da',
    'FOREX_CAUSAL_TIMING_REPAIR_VALIDATION_20260906.json':'c6fb0c4c7ccb3a2650f2eb8169bc462d62df88130283d33442f0fd6edc6d52dc',
    'FOREX_PREDICTION_QUALITY_20260906.json':'205ca0f6a5fbd40af7b45c0994ffcf93424da675e79b4637d030cd84560d489e',
    'FOREX_FIXED_EVALUATION_RESULTS_20260906.json':'7acecda9075ba197f50eb4f9b671246916e8535d0f10d90a8d125bfcb4491cd3',
    'FOREX_FIXED_EVALUATION_VALIDATION_20260906.json':'3b6559a1e38c1f95eb4a4e1597fcc702b862c665a375bcf037a74d6cf417a803',
    'FOREX_OPTIMIZATION_VALIDATION_20260906.json':'1b7ff7f4c567e9c7ac092adc3e31f57bab02f67d06cb2ce1eae923531be33a0a',
    'FOREX_REPAIR_VALIDATION_20260905.json':'a38cf7e7e152aac9108d33b909f1d3ac17a5e1f9228068fc7afa510b9c5e2926'}
assert {name:sha(ROOT/name) for name in prior_names} == prior_expected

new_sources = ['oanda_entry_diagnostics.py', 'oanda_practice_shadow_strategy_lab.py',
    'oanda_practice_live_dashboard.py', 'test_oanda_entry_diagnostics.py',
    'test_oanda_executor_dashboard_diagnostics.py', 'oanda_exact_price_scoring.py',
    'test_oanda_exact_price_scoring.py', 'oanda_fixed_forecast_evaluation_exact.py',
    'test_oanda_fixed_forecast_evaluation_exact.py', 'oanda_joint_return_baselines.py',
    'test_oanda_joint_return_baselines.py', 'forex_model_vault_sync.py',
    'docs/FOREX_ENTRY_IMPROVEMENTS_20260906.md', 'FOREX_ENTRY_RESEARCH_FOLLOWUP_20260906.json',
    'docs/FOREX_WEEK_ENTRY_AUDIT_20260906.md', 'FOREX_WEEK_ENTRY_AUDIT_20260906.json',
    'FOREX_RUNTIME_STOP_20260906.json']
sources = [ROOT/name for name in new_sources] + sorted(path for path in EVIDENCE.rglob('*') if path.is_file())
receipt = {
    'schema_version':'forex_entry_improvements_validation_v1', 'generated_utc':NOW,
    'scope':'Stopped canonical C project; saved week-entry audit, additive entry diagnostics, exact scoring and joint-return offline baselines',
    'status':'offline_validated_runtime_stopped',
    'source_bindings':[{'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p),'bytes':p.stat().st_size} for p in sources],
    'prior_receipts_unchanged':prior_expected,
    'registered_study':{'contract_id':contract['contract_id'],'contract_sha256':sha(contract_path),
        'source_bindings_unchanged':contract['source_bindings'],'running':False,'changed':False},
    'test_result':{'junit':(EVIDENCE/'integration_tests.xml').relative_to(ROOT).as_posix(),
        'junit_sha256':sha(EVIDENCE/'integration_tests.xml'),'checks_including_subtests':int(suite.get('tests')),
        'passed_test_functions':int(suite.get('tests'))-112,'passed_subtests':112,
        'failures':0,'errors':0,'skipped':int(suite.get('skipped')),
        'seconds':float(suite.get('time')),
        'excluded_existing_quote_publisher_thread_fixtures':5,
        'initial_guard_result':'254 tests and 112 subtests passed; five unrelated publisher fixtures were blocked before starting threads. The initial XML is preserved.'},
    'exact_history_replay':{'rows':8414,'stored_direction_hits':4156,'exact_direction_hits':4148,
        'exact_flats':296,'positive_after_spread':716,'historical_availability_reconstructed':False},
    'joint_baseline':{'arms':3,'status':'implemented_offline_not_activated','tests':68,
        'synthetic_fit_median_seconds':0.04351500002667308,'historical_market_accuracy_established':False},
    'entry_audit':load(ROOT/'FOREX_WEEK_ENTRY_AUDIT_20260906.json')['funnel'],
    'independent_review':{'joint_baseline':'Root reviewed aligned features, training-only scalers and mature labels.',
        'exact_scorer':'Calibration reviewer checked exact signs, anchors, baseline clocks, JSON serialization and same-read file hashing.',
        'entry_diagnostics':'Ledger reviewer found and repaired the initially missing fast-executor dashboard source; routing rules preserved.'},
    'runtime_observation':observed,
    'runtime_started':False,'broker_requests':0,'orders_submitted':0,'execution_gates_changed':False,
    'prediction_improvement_demonstrated':False,'proof_eligible':False,'account_eligible':False,
    'limitations':['Local saved records only; no new broker history retrieval.',
        'Sampled candidate observations and truncated historical details do not reconstruct exact unique cycle totals.',
        'The new scorer and numerical baselines are offline and require separate registered integration and genuine future outcomes.',
        'Source snapshot and local vault copy do not include every runtime database, private credential or historical model weight.']}
receipt_path = ROOT/'FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json'
assert not receipt_path.exists()
write(receipt_path, receipt)

register_path = ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json'
register = load(register_path)
issue = next(row for row in register['issues'] if row['issue_id']=='FX-20260906-EXACT-MIDPOINT-SCORING')
issue['status'] = 'implemented_collecting'
issue['status_history'].append({'at_utc':NOW,'status':'implemented_collecting',
    'scope':'Offline implementation validated; runtime stopped; separate registered integration and future evidence remain pending.'})
issue['collection_state'] = 'paused_user_requested_stop'
issue['current_mitigation'] = 'Exact scorer and separate v2 evaluator implemented and validated; 4148/8414 hits,296 flats,716 net positives reproduced. Frozen registered worker untouched. Predictive acceptance remains disabled pending separate integration with exact reference anchors and fresh evidence.'
issue['evidence_paths'].append('docs/FOREX_ENTRY_IMPROVEMENTS_20260906.md')
issue['validation_artifacts'].append({'path':receipt_path.name,'sha256':sha(receipt_path)})
register['issues'].insert(0,{
    'issue_id':'FX-20260906-ENTRY-DIAGNOSTIC-SCOPE','title':'Consumer entry telemetry conflates local counts and final rejection reasons',
    'priority':'P2','owner_component':'practice_entry_diagnostics','status':'complete',
    'acceptance_tests':['Distinguish empty local candidate input from a populated shared feed',
        'Retain full final block reason counts before limiting detailed examples',
        'Keep selection, authorization and stale/unknown dashboard observations distinct',
        'Read bounded actual fast-executor logs and preserve unchanged trading gates'],
    'completion_scope':'Additive sampled diagnostics and dashboard path validated offline; durable lifetime counters and runtime validation remain separate follow-up work.',
    'evidence_paths':['docs/FOREX_ENTRY_IMPROVEMENTS_20260906.md'],
    'validation_artifacts':[{'path':receipt_path.name,'sha256':sha(receipt_path)}],
    'status_history':[{'at_utc':NOW,'status':'complete'}]})
register['generated_utc'] = NOW
write(register_path, register)
from oanda_issue_register_validator import validate_register
validation = validate_register(register_path, root=ROOT)
assert validation['valid'], validation
write(OUT/'issue_register_validation.json', validation)

def prepend_after_title(path, prose):
    text = path.read_text(encoding='utf-8')
    first, rest = text.split('\n', 1)
    path.write_text(first+'\n\n'+prose+'\n\nEarlier dated observations follow; their runtime states and backlog counts are historical.\n'+rest, encoding='utf-8')

for name, prefix in [('README.md','docs/'),('docs/AUDIT_STATE_CURRENT.md','')]:
    prepend_after_title(ROOT/name,
        f'Current update ({NOW}): **all Forex workers remain stopped**, and both Forex scheduled tasks are disabled. This supersedes every earlier collection/running note below.\n\n'
        f'[The entry audit and improvements]({prefix}FOREX_ENTRY_IMPROVEMENTS_20260906.md) confirm zero entries in the last completed week: candidates reached the executor but failed final conflict/cost rules. Exact-price scoring and joint-predictor baselines are implemented offline; predictive improvement remains unproven. Follow-up work is recorded in `FOREX_ENTRY_RESEARCH_FOLLOWUP_20260906.json` and validation in `FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json`.')
prepend_after_title(ROOT/'FOREX_AUDIT_START_HERE.md',
    f'Current update ({NOW}): **the actual C-drive project is stopped**, with both Forex scheduled tasks disabled. This supersedes every earlier running/collection note below. Start with `ENTRY_IMPROVEMENTS_CURRENT.md`, `ENTRY_IMPROVEMENTS_VALIDATION_CURRENT.json`, `WEEK_ENTRY_AUDIT_CURRENT.json`, `ENTRY_RESEARCH_FOLLOWUP_CURRENT.json` and `RUNTIME_STOP_CURRENT.json`.\n\n'
    'Canonical source is `C:\\Users\\zmoor\\Documents\\forex\\trad`. The equivalent source report is `docs/FOREX_ENTRY_IMPROVEMENTS_20260906.md`. Last week had no entries despite an operating executor; final conflict/cost gates were the immediate bottleneck. The missing exact scorer and compact joint-predictor baselines now exist offline. No new model has demonstrated improved real-market accuracy or been activated.')
prepend_after_title(ROOT/'docs/VAULT_RECREATION_CURRENT.md',
    f'Current disposition ({NOW}): runtime stopped at user request. The latest source includes offline exact scoring, joint-return baselines and entry diagnostics; these do not authorize a restart. Begin with the vault entry-improvement records and its latest source manifest.')
for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
    value = load(ROOT/name)
    value['latest_review'] = {'observed_utc':NOW,'runtime_state':'stopped_user_requested',
        'supersedes_prior_runtime_and_backlog_status':True,'runtime_verification':receipt_path.name,
        'entry_improvements':'docs/FOREX_ENTRY_IMPROVEMENTS_20260906.md',
        'followup':'FOREX_ENTRY_RESEARCH_FOLLOWUP_20260906.json',
        'issue_count':validation['issue_count'],'issue_status_counts':validation['status_counts'],
        'all_other_embedded_snapshots_retain_their_original_timestamps':True}
    write(ROOT/name, value)
print(json.dumps({'receipt':receipt_path.name,'sha256':sha(receipt_path),'test_result':receipt['test_result'],
    'issue_register':validation,'evidence_files':len(sources)}))
