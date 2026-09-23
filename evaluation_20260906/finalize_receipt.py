"""Freeze source/evidence validation without changing historical receipts."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
EVIDENCE=ROOT/'docs/validation/fixed_evaluation_20260906'
sys.dont_write_bytecode=True
sys.path.insert(0,str(ROOT))
from oanda_issue_register_validator import validate_register
from oanda_fixed_forecast_evaluation import content_hash

def read(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def bind(path):return {'path':path.relative_to(ROOT).as_posix(),'bytes':path.stat().st_size,'sha256':sha(path)}
def write(path,value):path.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n',encoding='utf-8')

prior={'FOREX_OPTIMIZATION_VALIDATION_20260906.json':'1b7ff7f4c567e9c7ac092adc3e31f57bab02f67d06cb2ce1eae923531be33a0a',
       'FOREX_PREDICTION_QUALITY_20260906.json':'205ca0f6a5fbd40af7b45c0994ffcf93424da675e79b4637d030cd84560d489e',
       'FOREX_REPAIR_VALIDATION_20260905.json':'a38cf7e7e152aac9108d33b909f1d3ac17a5e1f9228068fc7afa510b9c5e2926'}
for name,digest in prior.items():assert sha(ROOT/name)==digest
availability=read(OUT/'data/availability_diagnostic.json')
for row in availability['local_artifacts']:assert sha(OUT/'data'/row['path'])==row['sha256']
for row in availability['source_code_and_registry']:assert sha(ROOT/row['path'])==row['sha256']
for row in availability['database_file_bindings']:
    assert row['sha256']==row['second_sha256'] and row['stat_before']==row['stat_after']
strict=read(OUT/'strict_evaluation_report.json');protocol=read(ROOT/'config/fixed_forecast_evaluation_v1_20260906.json')
assert strict['protocol_sha256']==content_hash(protocol)
assert strict['input_sha256']==content_hash(read(OUT/'strict_input_final.json'))
assert strict['coverage']['total_forecasts']==1628 and strict['coverage']['paired_scored_decisions']==0
assert sha(ROOT/'oanda_fixed_forecast_evaluation.py')=='7ffbb85a29ca414239d720de33a7d413fdc2447af2c58e2bd70e151abb8e0b0c'
assert sha(ROOT/'oanda_causal_prediction_baselines.py')=='7260b9b5a0f6dec033deeacf37e76ea58b67ad21071b52ee915b8b0113dcf99d'
suite=ET.parse(OUT/'integration_tests.xml').find('testsuite').attrib
assert suite['tests']=='158' and all(suite[k]=='0' for k in ('errors','failures','skipped'))
stopped=read(OUT/'stopped_after_validation.json')
assert not stopped['runtime_processes'] and all(r['State']=='Disabled' for r in stopped['forex_tasks'])
(EVIDENCE/'stopped_after_validation.json').write_bytes((OUT/'stopped_after_validation.json').read_bytes())

names=['oanda_fixed_forecast_evaluation.py','oanda_causal_prediction_baselines.py',
       'tools/build_fixed_forecast_input.py','test_oanda_fixed_forecast_evaluation.py',
       'test_oanda_causal_prediction_baselines.py','test_build_fixed_forecast_input.py',
       'test_forex_model_vault_sync.py','test_oanda_issue_register_validator.py','forex_model_vault_sync.py',
       'config/fixed_forecast_evaluation_v1_20260906.json','docs/FOREX_FIXED_EVALUATION_20260906.md',
       'FOREX_FIXED_EVALUATION_RESULTS_20260906.json','FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json',
       'README.md','FOREX_AUDIT_START_HERE.md','FOREX_PROJECT_LOG.md','FOREX_PENDING_IMPROVEMENTS.md','docs/AUDIT_STATE_CURRENT.md']
files=sorted(set([ROOT/n for n in names]+[p for p in EVIDENCE.rglob('*') if p.is_file()]))
stamp=datetime.now(timezone.utc).isoformat()
receipt={'schema_version':'forex_fixed_evaluation_validation_v1','generated_utc':stamp,
    'status':'fixed_offline_comparison_complete_fresh_prospective_evidence_unavailable',
    'canonical_project':str(ROOT),'runtime_started':False,'broker_requests':0,'production_logical_writes':0,
    'database_access':availability['boundary'],'database_main_wal_bindings':availability['database_file_bindings'],
    'protocol':protocol,'protocol_file_sha256':sha(ROOT/'config/fixed_forecast_evaluation_v1_20260906.json'),
    'validation':{'passed':158,'failed':0,'errors':0,'skipped':0,'seconds':float(suite['time']),
        'guard':'Network, threads, subprocesses, production SQLite and non-fixture writes prohibited for tests',
        'independent_scorer_tests':76,'independent_baseline_tests':55,'suite_counts_overlap_do_not_add':True},
    'actual_data_result':{'preserved_forecasts':1628,'reference_epochs':407,'missing_outcomes':85,
        'identical_archival_endpoint_epochs':368,'strict_clock_valid_paired_decisions':0,
        'all_original_payload_hashes_verified':True,'all_archival_metrics_independently_recomputed':True,
        'all_four_models_worse_brier_than_constant_half':True,'all_four_models_worse_mae_than_zero_move':True,
        'all_four_stored_quote_net_means_negative':True,'old_data_reclassified_as_prospective':False},
    'separate_scope':['Archival comparisons retain original emitted predictions, prices and missingness.',
        'Strict scorer preserves original target/reference/side and rejects unavailable clocks.',
        'Causal rolling baseline behavior verified on fixtures; historical rolling metrics remain unavailable.',
        'No old producer/cohort/evidence was rewritten, activated or promoted.'],
    'stopped_state':stopped,'source_bindings':[bind(path) for path in files],
    'preserved_prior_receipts':[bind(ROOT/name) for name in prior],
    'remaining_issues':['FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK','FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY'],
    'limitations':['No strict causal historical or fresh prospective performance is established.',
        'Input assertions passing clock checks do not independently attest historical availability.',
        '368 overlapping archived endpoints over6UTCdays are not independent trials.',
        'Source visibility, feature/label availability and correct entry/target production still require a separately versioned producer.',
        'Ephemeral SQLite SHM reader metadata may change; logical tables and main/WAL bytes were not changed.',
        'Vault export verification is recorded separately; no live runtime readiness claim.']}
path=ROOT/'FOREX_FIXED_EVALUATION_VALIDATION_20260906.json'
assert not path.exists(),'Dated validation must not be overwritten'
write(path,receipt)

register_path=ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json';register=read(register_path)
new_ids={'FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY','FX-20260906-FIXED-FORECAST-EVALUATION'}
assert not any(r['issue_id'] in new_ids for r in register['issues'])
artifact={'path':path.name,'sha256':sha(path)}
register['issues'][:0]=[
    {'issue_id':'FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY','title':'Four-family archived entries precede publication and outcome producers use inconsistent target clocks',
     'priority':'P1','owner_component':'proof_shadow_predictors_and_canonical_outcome_worker','status':'open',
     'acceptance_tests':['Preserve original cohorts and missing clocks as diagnostics without backfilling proof',
        'Separately bind genuine forecast issue/publication, feature and training-label availability',
        'Obtain a postpublication executable entry quote and retain a single original forecast target',
        'Validate new independent evidence under a separately registered successor before any promotion'],
     'evidence_paths':['docs/FOREX_FIXED_EVALUATION_20260906.md','FOREX_FIXED_EVALUATION_RESULTS_20260906.json'],
     'validation_artifacts':[artifact],'status_history':[{'at_utc':stamp,'status':'open'}],
     'current_mitigation':'Project stopped; new strict evaluator rejects every missing-clock archived forecast; no account/proof eligibility.'},
    {'issue_id':'FX-20260906-FIXED-FORECAST-EVALUATION','title':'Implement fixed four-family EUR/USD baseline comparison with strict clock and evidence rejection',
     'priority':'P2','owner_component':'offline_prediction_evaluation','status':'complete',
     'acceptance_tests':['Compare identical archived endpoints against nonfitted baselines with all missingness retained',
        'Reject future/missing feature, forecast and training-label clocks in a separate evaluator',
        'Preserve original target, reference, side, abstentions and paired denominators',
        'Validate causal rolling baseline against unique strictly prior labels and preserve reproducible source/data'],
     'acceptance_scope':'Offline tooling, comparison and evidence preservation complete; no live or predictive confirmation claimed',
     'evidence_paths':['docs/FOREX_FIXED_EVALUATION_20260906.md'],'validation_artifacts':[artifact],
     'status_history':[{'at_utc':stamp,'status':'complete'}]}]
for row in register['issues']:
    if row['issue_id']=='FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK':
        row.setdefault('validation_artifacts',[]).append(artifact)
        row['followup']='Separate strict evaluator and causal baseline helper implemented; legacy producer unchanged, prospective repair remains pending.'
register['generated_utc']=stamp
write(register_path,register)
checked=validate_register(register_path,root=ROOT)
assert checked['valid'],checked
write(OUT/'issue_register_validation.json',checked)
print(json.dumps({'receipt_sha256':sha(path),'source_bindings':len(files),'issue_register':checked},indent=2))
