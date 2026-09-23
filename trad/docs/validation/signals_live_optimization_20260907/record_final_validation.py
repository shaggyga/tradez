"""Freeze review evidence and current operational findings for the local vault."""
from collections import Counter
from datetime import datetime,timezone
import hashlib,json,shutil,sys
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
EVIDENCE=ROOT/'docs/validation/signals_live_optimization_20260907'
sys.path.insert(0,str(ROOT))
from oanda_causal_forecast_study_eurusd_v1 import load_contract
from oanda_causal_forecast_study_gap_v2 import load_contract as load_paired
from oanda_issue_register_validator import validate_register
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def write(path,value):path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf8')
local=load_contract(ROOT/'config/causal_forecast_study_eurusd_v1_20260907.json')
paired=load_paired(ROOT/'config/causal_forecast_study_gap_v2_20260907.json')
runtime=load(OUT/'final_runtime_observation.json')
expected={'oanda_account_snapshot_writer.py','oanda_practice_live_dashboard.py','oanda_local_news_sentiment.py',
 'oanda_official_release_fast_lane.py','oanda_official_release_fast_mapper.py','oanda_source_governance_news_fast_lane.py',
 'oanda_practice_quote_stream.py','oanda_clock_integrity_monitor.py','oanda_all68_m1_forward_updater.py',
 'oanda_project_integrity_audit.py','oanda_storage_headroom_guard.py','oanda_causal_forecast_study_gap_v2.py',
 'oanda_causal_forecast_study_eurusd_v1.py'}
supervisors=[r for r in runtime['processes'] if r['script']=='oanda_always_on_supervisor.ps1']
assert len(supervisors)==1 and supervisors[0]['research_collection_only']
workers=[r for r in runtime['processes'] if r['script'].endswith('.py')]
assert Counter(r['script'] for r in workers)==Counter({name:2 for name in expected})
assert len(runtime['managed_running'])==13 and all(r['freshness']['fresh'] for r in runtime['managed_running'])
assert runtime['supervisor_error_count']==0 and runtime['dashboard_http_status']==200
assert all(task['state']=='Disabled' for task in runtime['forex_tasks'])
account=runtime['account']['aggregate']
assert account['positions_current'] and account['orders_current']
assert account['openTradeCount']==account['pendingOrderCount']==0
live_path=max(OUT.glob('LIVE_COMPANION_VERIFICATION_*.json'),key=lambda p:p.stat().st_mtime_ns)
live=load(live_path)
assert live['actual_publication_and_clock_checks']=='passed' and live['counts']['forecasts']>=1
assert live['counts']['publication']==live['counts']['consumption']==live['counts']['forecasts']
assert live['counts']['entries']>=1
register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT)
assert register['valid'],register
EVIDENCE.mkdir(parents=True,exist_ok=False)
for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
    target=EVIDENCE/'before_current_records'/name;target.parent.mkdir(exist_ok=True)
    target.write_bytes((ROOT/name).read_bytes())
for source in OUT.rglob('*'):
    if not source.is_file():continue
    relative=source.relative_to(OUT)
    if any('fixture' in part.lower() or part in ('__pycache__','retired_study') for part in relative.parts[:-1]):continue
    if source.suffix.lower() not in ('.json','.xml','.md','.py','.ps1','.cjs','.png'):continue
    if relative.as_posix()=='live_input_probe/observed_inputs.json':continue
    target=EVIDENCE/relative;target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(source,target)
code=set(local['source_bindings'])|set(paired['source_bindings'])|{
 'oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html','oanda_all68_m1_forward_updater.py',
 'oanda_market_overview.py','oanda_weekend_reopening_baseline.py','oanda_always_on_supervisor.ps1',
 'start_oanda_research_collection.ps1','forex_model_vault_sync.py',
 'config/causal_forecast_study_current.json','config/causal_forecast_study_eurusd_v1_20260907.json',
 'config/causal_forecast_study_gap_v2_20260907.json',
 'test_oanda_causal_forecast_inputs_eurusd_v1.py','test_oanda_eurusd_local_models_v1.py',
 'test_oanda_causal_forecast_ledger_eurusd_v1.py','test_oanda_causal_forecast_study_eurusd_v1.py',
 'test_oanda_causal_forecast_study_eurusd_v1_io.py','test_oanda_causal_forecast_inputs_gap_v2.py',
 'test_oanda_gap_aware_four_family_models.py','test_oanda_causal_forecast_ledger_gap_v2.py',
 'test_oanda_causal_forecast_study_gap_v2.py','test_oanda_causal_forecast_study_gap_v2_io.py',
 'test_oanda_all68_m1_forward_updater.py','test_oanda_market_overview.py',
 'test_oanda_collection_dashboard_status.py','test_oanda_weekend_reopening_baseline.py',
 'README.md','FOREX_AUDIT_START_HERE.md','docs/AUDIT_STATE_CURRENT.md','docs/VAULT_RECREATION_CURRENT.md',
 'docs/FOREX_SIGNALS_LIVE_OPTIMIZATION_20260907.md','FOREX_PROJECT_LOG.md','FOREX_PENDING_IMPROVEMENTS.md'}
bound=[ROOT/name for name in sorted(code)]+sorted(p for p in EVIDENCE.rglob('*') if p.is_file())
prior={'FOREX_MARKET_OPEN_VALIDATION_20260906.json':'70fd1bb5769ccd53c15e1dd4d20d6d25ad0cbf0ae9e17b85239d461c0b43ed90',
 'FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json':'d2b733b0ade4c0c1a53aa8e544f34cbf578fe7dc44d76864bb3ad25d659922be'}
for name,expected_sha in prior.items():assert sha(ROOT/name)==expected_sha
receipt={'schema_version':'forex_signals_live_optimization_validation_v1_20260907',
 'generated_utc':datetime.now(timezone.utc).isoformat(),'status':'research_forecasts_publishing_accuracy_pending',
 'runtime_mode':'ResearchCollectionOnly','managed_worker_count':13,'runtime_observation':runtime,
 'live_forecast_verification':live,'eurusd_activation':load(OUT/'EURUSD_ACTIVATION_RECEIPT.json'),
 'four_family_activation_and_predecessor':load(OUT/'ACTIVATION_RECEIPT.json'),
 'gate_review':load(OUT/'COMPANION_GATE_REVIEW.json'),
 'performance':load(OUT/'EURUSD_REGISTRATION_PREPARED.json')['readonly_live_probe'],
 'prior_receipts_unchanged':prior,'issue_register_validation':register,
 'can_place_orders':False,'practice_trading_ready':False,'prediction_improvement_demonstrated':False,
 'independent_sample_size':None,'offline_reopening_baseline_activated':False,
 'limitations':['Published sets overlap and can share the same original reference/target; record counts are not independent outcomes.',
 'No completed new H1 outcomes at the verification clock; model probabilities are not validated accuracy.',
 'The separate four-family study still needs shared complete input minutes; EURUSD-only publication does not repair missing peer candles.',
 'Opening-gap baseline remains offline and cannot retrospectively issue an opening forecast.',
 'Exact scoring preserves the supplied decimal prices; it cannot recover vendor precision already lost upstream.',
 'Only local vault/source bytes are verified separately; private credentials and full runtime databases are not source-archive contents.'],
 'source_bindings':[{'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p),'bytes':p.stat().st_size} for p in bound]}
destination=ROOT/'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json'
assert not destination.exists();write(destination,receipt)
for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
    current=load(ROOT/name)
    current['latest_review']={'observed_utc':receipt['generated_utc'],'runtime_state':receipt['status'],
        'supersedes_prior_runtime_status':True,'report':'docs/FOREX_SIGNALS_LIVE_OPTIMIZATION_20260907.md',
        'validation':destination.name,'validation_sha256':sha(destination),'managed_worker_count':13,
        'primary_study':'eurusd_v1','cross_pair_study':'gap_v2','can_place_orders':False,
        'practice_trading_ready':False,'prediction_improvement_demonstrated':False,
        'all_other_embedded_snapshots_retain_their_original_timestamps':True}
    write(ROOT/name,current)
print(json.dumps({'status':receipt['status'],'source_binding_count':len(bound),'receipt_sha256':sha(destination),'records_expected':143}))
