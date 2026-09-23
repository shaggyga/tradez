"""Bind final reviewed source, preserve supporting receipts, and register work."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent/'trad'
DEST = ROOT/'docs/validation/optimization_20260906'
sys.dont_write_bytecode = True
sys.path.insert(0,str(ROOT))
from oanda_issue_register_validator import validate_register

def read(path): return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def bind(path): return {'path':path.relative_to(ROOT).as_posix(),'bytes':path.stat().st_size,'sha256':sha(path)}
def write(path,value): path.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n',encoding='utf-8')

now = datetime.now(timezone.utc).isoformat()
assert sha(ROOT/'FOREX_REPAIR_VALIDATION_20260905.json') == 'a38cf7e7e152aac9108d33b909f1d3ac17a5e1f9228068fc7afa510b9c5e2926'
assert sha(ROOT/'FOREX_PERFORMANCE_AUDIT_20260905.json') == '31243fe61c45cebae0eced4b448da8cbf9db053548531a5a309cb5e2ec58a4e5'
benchmark = read(OUT/'integrity_publication_benchmark.json')
assert sha(Path(benchmark['input']['path'])) == benchmark['input']['sha256']
assert all(benchmark['equivalence'].values())
for name,digest in benchmark['source_sha256'].items(): assert sha(ROOT/name)==digest
independent = read(OUT/'review/COMPACT_INTEGRITY_REVIEW_RECEIPT.json')
assert not independent['findings']
for row in independent['sources']: assert sha(Path(row['path']))==row['sha256']
predictions = read(ROOT/'FOREX_PREDICTION_QUALITY_20260906.json')
for row in predictions['sources']+predictions['code_sources']: assert sha(Path(row['path']))==row['sha256']

evidence = []
for relative in ('integration_tests.xml','pytest_publication.xml','integrity_consumer_tests.xml',
                 'stopped_before_optimization.json','stopped_after_validation.json',
                 'review/COMPACT_INTEGRITY_REVIEW.md','review/COMPACT_INTEGRITY_REVIEW_RECEIPT.json',
                 'review/review_tests.xml','review/run_review.py'):
    source = OUT/relative
    target = DEST/relative
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes(source.read_bytes())
    evidence.append(bind(target))
suite = ET.parse(OUT/'integration_tests.xml').find('testsuite').attrib
assert suite['tests']=='132' and suite['skipped']=='1' and suite['failures']=='0' and suite['errors']=='0'
stopped = read(OUT/'stopped_after_validation.json')
assert not stopped['runtime_processes']
assert all(row['State']=='Disabled' for row in stopped['forex_tasks'])
for name in ('source_factor_response_v9.json','source_conditioned_currency_rank_v8.json'):
    assert read(ROOT/'config'/name)['collection_enabled'] is False

source_names = [
    'oanda_integrity_publication.py','oanda_project_integrity_audit.py','forex_model_vault_sync.py',
    'oanda_four_hour_best_improvement_pass.py','test_oanda_integrity_publication.py',
    'test_forex_integrity_detail_export.py','test_oanda_four_hour_best_improvement_pass.py',
    'test_forex_model_vault_sync.py','test_oanda_project_integrity_audit.py','test_oanda_issue_register_validator.py',
    'oanda_issue_register_validator.py','README.md','FOREX_AUDIT_START_HERE.md','docs/AUDIT_STATE_CURRENT.md',
    'FOREX_PROJECT_LOG.md','FOREX_PENDING_IMPROVEMENTS.md','FOREX_OPTIMIZATION_BACKLOG_20260906.json',
    'FOREX_PREDICTION_QUALITY_20260906.json','docs/FOREX_PREDICTION_QUALITY_20260906.md',
    'docs/FOREX_OPTIMIZATION_REVIEW_20260906.md','docs/validation/assess_saved_predictions_20260906.py',
    'config/source_factor_response_v9.json','config/source_conditioned_currency_rank_v8.json',
]
source_files = sorted(set([ROOT/name for name in source_names]+[p for p in DEST.rglob('*') if p.is_file()]))
receipt = {
    'schema_version':'forex_optimization_validation_v1', 'generated_utc':now,
    'canonical_project':str(ROOT), 'status':'reporting_optimization_offline_validated_runtime_stopped',
    'supported_decision':'no_trade', 'runtime_restarted':False, 'collection_activated':False,
    'production_database_connections':0, 'production_database_writes':0,'broker_requests':0,
    'source_change_scope':['Compact five episode_rows arrays only after unchanged audit checks',
                           'Immutable hash-verified detail publication and exact restoration',
                           'Coherent current snapshot plus detail checkpoint export',
                           'Portable saved-observer locator','Dated logs, prediction assessment and vault record mappings'],
    'combined_validation':{'passed':131,'skipped':1,'failures':0,'errors':0,
                           'junit_seconds':float(suite['time']),
                           'skip_reason':'Windows symlink-creation privilege unavailable',
                           'guard':'No subprocesses, network, workers or SQLite outside disposable fixtures',
                           'modules':['test_oanda_integrity_publication.py','test_forex_integrity_detail_export.py',
                                      'test_oanda_four_hour_best_improvement_pass.py','test_forex_model_vault_sync.py',
                                      'test_oanda_issue_register_validator.py','test_oanda_project_integrity_audit.py']},
    'benchmark':benchmark,
    'prediction_headline':predictions['current_top_signal_shadow']['overall'],
    'prediction_assessment_sha256':sha(ROOT/'FOREX_PREDICTION_QUALITY_20260906.json'),
    'prediction_verdict':predictions['verdict'],
    'new_research_limitation':{'issue_id':'FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK','status':'open',
        'description':'Completed-outcome recalibration lacks original-forecast-issue-time mature-label filtering; saved replay scores are not forward proof.',
        'existing_guard':'account_eligible remains false; no model or threshold changed',
        'resolution_gate':'Separate frozen, issue-time-causal calibration and same-decision baselines on new independent evidence'},
    'independent_reviews':{'publisher_and_exporter':independent,'consumer_inventory':read(OUT/'integrity_consumer_review.json')},
    'stopped_state':stopped,
    'unchanged_prior_receipts':[bind(ROOT/'FOREX_REPAIR_VALIDATION_20260905.json'),
                              bind(ROOT/'FOREX_PERFORMANCE_AUDIT_20260905.json'),
                              bind(ROOT/'FOREX_INDEPENDENT_AUDIT_20260905.json')],
    'source_bindings':[bind(path) for path in source_files],
    'evidence':evidence,
    'limitations':['No live integrity cycle, broker performance or production query benchmark.',
        'Five timing samples with warm filesystem cache are not latency percentiles.',
        'Full detail is retained; compact-summary size reduction is not total project storage reduction.',
        'Current JSON, Markdown and JSONL remain separate filesystem writes.',
        'Current-checkpoint detail closure does not automatically export every historical JSONL dependency.',
        'New detail files are never automatically deleted; storage-growth and retention work remains pending.',
        'No useful predictive edge established; prospective evidence cannot be generated while stopped.',
        'Export verification is separate under vault maintenance and does not alter this dated receipt.']
}
receipt_path = ROOT/'FOREX_OPTIMIZATION_VALIDATION_20260906.json'
assert not receipt_path.exists(),'Dated receipt must not be overwritten'
write(receipt_path,receipt)

register_path = ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json'
register = read(register_path)
new_issues = [
    {'issue_id':'FX-20260906-INTEGRITY-PUBLICATION-OVERHEAD',
     'title':'Repeated embedded episode arrays inflate integrity current and history publications',
     'priority':'P2','owner_component':'project_integrity_publication','status':'complete',
     'acceptance_tests':['Preserve all non-row values and exactly reconstruct original JSON',
                        'Reject corrupt or missing detail and retain generation ownership',
                        'Export coherent verified detail with current checkpoints and restore copied records',
                        'Measure first and repeated publication on actual saved payload'],
     'acceptance_scope':'Source implementation and offline saved-payload benchmark complete; full live cycle pending while stopped',
     'evidence_paths':['docs/FOREX_OPTIMIZATION_REVIEW_20260906.md'],
     'validation_artifacts':[{'path':receipt_path.name,'sha256':sha(receipt_path)}],
     'status_history':[{'at_utc':now,'status':'complete'}]},
    {'issue_id':'FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK',
     'title':'Completed-outcome calibration replay does not establish probabilities available at original forecast issue',
     'priority':'P1','owner_component':'timeframe_matrix_calibration','status':'open',
     'acceptance_tests':['Preserve legacy replay as diagnostic without relabeling historical evidence',
                        'Use only training labels matured before each forecast issue in a separate frozen evaluation',
                        'Compare frozen same-decision baselines with independent untouched after-cost evidence'],
     'evidence_paths':['docs/FOREX_PREDICTION_QUALITY_20260906.md','FOREX_PREDICTION_QUALITY_20260906.json'],
     'current_mitigation':'Account eligibility remains false; prediction assessment explicitly excludes replay scores from forward proof.',
     'validation_artifacts':[], 'status_history':[{'at_utc':now,'status':'open'}]},
]
assert not any(row['issue_id'] in {new['issue_id'] for new in new_issues} for row in register['issues'])
register['issues'] = new_issues + register['issues']
register['generated_utc'] = now
write(register_path,register)
result = validate_register(register_path,root=ROOT)
assert result['valid'],result
write(OUT/'issue_register_validation.json',result)
print(json.dumps({'receipt_sha256':sha(receipt_path),'source_bindings':len(source_files),'register_validation':result},indent=2))
