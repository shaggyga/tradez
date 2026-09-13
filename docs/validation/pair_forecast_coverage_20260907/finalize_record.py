"""Record verified pair coverage and update current local audit navigation."""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,shutil,sys,xml.etree.ElementTree as ET
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
from oanda_pair_local_forecast_study_v1 import load_registry,digest
from oanda_issue_register_validator import validate_register
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def write(path,value):path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf8')
registry=load_registry(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
live=load(sorted(OUT.glob('LIVE_PAIR_VERIFICATION_*.json'))[-1])
ui=load(OUT/'LIVE_PAIR_DASHBOARD_VALIDATION.json')
recovery=load(OUT/'PAIR_RUNTIME_RECOVERY_20260907.json')
assert live['published_pairs']==20 and live['published_sets']>=20
assert recovery['status']=='passed'
report=ROOT/'docs/FOREX_PAIR_FORECAST_COVERAGE_20260907.md'
assert report.is_file() and 'pending at this report' not in report.read_text()
nav='''September 7 pair-coverage update: **20 pairs now have published forecasts (40 model predictions)**,
verified at 17:19 UTC. The new registry covers all 68 observed instruments, each
with independent ridge/state-space inputs, cadence and immutable evidence.
Other rows show their own last-attempt warm-up or missing-quote reason. The
dashboard provides pair selection, all-pairs display and explicit uncalibrated
probability labels. The research gate admits 14 workers; trading remains off.

Read the pair-coverage report and validation receipt linked below. The original
EUR/USD and shared four-family studies remain separate and unchanged. Greater
coverage does not demonstrate better predictions; new H1 outcomes were still
pending at this observation. Earlier dated runtime descriptions retain their
original scope.

'''
for relative in ('README.md','FOREX_AUDIT_START_HERE.md','docs/AUDIT_STATE_CURRENT.md','docs/VAULT_RECREATION_CURRENT.md'):
    path=ROOT/relative;text=path.read_text(encoding='utf8')
    assert 'September 7 pair-coverage update:' not in text
    first,rest=text.split('\n',1)
    if relative=='FOREX_AUDIT_START_HERE.md':
        links='Vault records: `PAIR_FORECAST_COVERAGE_CURRENT.md` and `PAIR_FORECAST_COVERAGE_VALIDATION_CURRENT.json`. The canonical vault mapping is now **145 records**.\n\n'
    else:
        prefix='' if relative.startswith('docs/') else 'docs/'
        receipt_prefix='../' if relative.startswith('docs/') else ''
        links=f'[Pair-coverage report]({prefix}FOREX_PAIR_FORECAST_COVERAGE_20260907.md) · [Validation receipt]({receipt_prefix}FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json).\n\n'
    if relative.endswith('VAULT_RECREATION_CURRENT.md'):
        links+='Recreating source does not restore per-pair SQLite ledgers, WAL files, activation clocks or private credentials. Preserve each registered runtime directory separately. The EUR/USD primary pointer remains unchanged.\n\n'
    path.write_text(first+'\n\n'+nav+links+rest.lstrip('\n'),encoding='utf8')
evidence=ROOT/'docs/validation/pair_forecast_coverage_20260907'
evidence.mkdir(parents=True,exist_ok=False)
for path in OUT.iterdir():
    if path.is_file() and path.suffix.lower() in ('.json','.xml','.py','.ps1','.cjs','.png'):
        shutil.copy2(path,evidence/path.name)
for relative in ('ledger_tests/PAIR_LEDGER_EVALUATION_VALIDATION_20260907.json','ledger_tests/pair_ledger_evaluation_final.xml'):
    path=OUT/relative
    (evidence/path.parent.name).mkdir(exist_ok=True)
    shutil.copy2(path,evidence/relative)
shutil.copytree(OUT/'before_source',evidence/'before_source')
test_files=['pair_models_inputs_tests.xml','ledger_tests/pair_ledger_evaluation_final.xml',
    'dashboard_worker_tests.xml','pair_reload_preflight_tests.xml']
tests=[]
for relative in test_files:
    path=OUT/relative;root=ET.parse(path).getroot();suites=list(root.iter('testsuite'))
    row={name:sum(int(suite.attrib.get(name,0)) for suite in suites) for name in ('tests','errors','failures','skipped')}
    assert not row['errors'] and not row['failures'];row['evidence']=relative;tests.append(row)
old={name:sha(ROOT/name) for name in ('FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json',
    'FOREX_MARKET_OPEN_VALIDATION_20260906.json','FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json')}
assert old['FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json']=='3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792'
sources=set(registry['source_bindings'])|{
    'config/pair_local_forecast_study_v1_20260907.json','config/causal_forecast_study_current.json',
    'oanda_always_on_supervisor.ps1','start_oanda_research_collection.ps1','oanda_practice_live_dashboard.py',
    'oanda_main_signal_dashboard.html','forex_model_vault_sync.py','README.md','FOREX_AUDIT_START_HERE.md',
    'docs/AUDIT_STATE_CURRENT.md','docs/VAULT_RECREATION_CURRENT.md','FOREX_PROJECT_LOG.md',
    'FOREX_PENDING_IMPROVEMENTS.md','docs/FOREX_PAIR_FORECAST_COVERAGE_20260907.md',
    'test_oanda_collection_dashboard_status.py','test_oanda_pair_forecast_dashboard.py',
    'test_oanda_pair_local_forecast_worker_v1.py','test_oanda_pair_local_models_v1.py',
    'test_oanda_causal_forecast_inputs_pair_v1.py','test_oanda_causal_forecast_ledger_pair_v1.py',
    'test_oanda_fixed_forecast_evaluation_pair_v1.py'}
sources|={path.relative_to(ROOT).as_posix() for path in evidence.rglob('*') if path.is_file()}
register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT);assert register['valid']
receipt={'schema_version':'forex_pair_forecast_coverage_validation_v1_20260907',
    'observed_utc':datetime.now(timezone.utc).isoformat(),'status':'independent_pair_forecasts_publishing_accuracy_pending',
    'registry_sha256':digest(registry),'registered_pairs':68,'published_pairs_at_live_check':live['published_pairs'],
    'published_sets_at_live_check':live['published_sets'],'model_predictions_at_live_check':live['published_sets']*2,
    'live_check_epoch':live['observed_epoch'],'new_outcomes_at_live_check':live['outcomes'],
    'tests':tests,'test_cases_passed':sum(row['tests']-row['skipped'] for row in tests),
    'supervisor_gate':load(OUT/'PAIR_SUPERVISOR_GATE_REVIEW_20260907.json'),
    'runtime_recovery_evidence':'docs/validation/pair_forecast_coverage_20260907/PAIR_RUNTIME_RECOVERY_20260907.json',
    'dashboard_evidence':'docs/validation/pair_forecast_coverage_20260907/LIVE_PAIR_DASHBOARD_VALIDATION.json',
    'preserved_studies':live['preserved_studies'],'prior_receipts_unchanged':old,
    'issue_register_validation':register,'account_trading_enabled':False,'can_place_orders':False,
    'prediction_improvement_demonstrated':False,'historical_forecasts_imported':False,
    'limitations':['Each remaining pair needs its own valid inputs and fresh quotes; coverage can change.',
        'Raw model probabilities are uncalibrated estimates, not measured accuracy.',
        'New original-H1 outcomes and sufficient prospective samples are required before evaluating predictive improvement.',
        'Pair-local models do not complete cross-pair or news-predictor research.',
        'No full database recovery, broad database integrity pass or cloud sync completion claimed.'],
    'source_bindings':[{'path':relative,'bytes':(ROOT/relative).stat().st_size,'sha256':sha(ROOT/relative)} for relative in sorted(sources)]}
receipt_path=ROOT/'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json'
assert not receipt_path.exists();write(receipt_path,receipt)
latest={'observed_utc':receipt['observed_utc'],'runtime_state':receipt['status'],'supersedes_prior_runtime_status':True,
    'report':'docs/FOREX_PAIR_FORECAST_COVERAGE_20260907.md','validation':receipt_path.name,
    'validation_sha256':sha(receipt_path),'managed_worker_count':14,'pair_registry':registry['registry_id'],
    'registered_pairs':68,'published_pairs_at_live_check':20,'primary_study':'eurusd_v1','cross_pair_study':'gap_v2',
    'can_place_orders':False,'practice_trading_ready':False,'prediction_improvement_demonstrated':False,
    'all_other_embedded_snapshots_retain_their_original_timestamps':True}
for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
    path=ROOT/name;value=load(path);value['latest_review']=latest;write(path,value)
print(json.dumps({'receipt':str(receipt_path),'sha256':sha(receipt_path),'source_bindings':len(sources),'tests':receipt['test_cases_passed']}))
