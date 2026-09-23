"""Record a separately dated dashboard read-consistency follow-up."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,shutil,sys,xml.etree.ElementTree as ET
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
OLD=OUT.parent/'pair_coverage_20260907'
sys.path.insert(0,str(ROOT))
from oanda_pair_local_forecast_study_v1 import load_registry,digest
from oanda_issue_register_validator import validate_register
def load(path):return json.loads(path.read_text(encoding='utf-8-sig'))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def write(path,value):path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf8')
registry=load_registry(ROOT/'config/pair_local_forecast_study_v1_20260907.json')
runtime=load(OUT/'DASHBOARD_CONSISTENCY_RUNTIME_VERIFICATION_RETAINED_20260907.json')
probe=load(OUT/'DASHBOARD_API_CONSISTENCY_PROBE_RETAINED2_20260907.json')
assessment=load(OUT/'DASHBOARD_API_CONSISTENCY_ASSESSMENT_20260907.json')
assert runtime['status']==assessment['status']=='passed'
assert assessment['raw_probe_sha256']==sha(OUT/'DASHBOARD_API_CONSISTENCY_PROBE_RETAINED2_20260907.json')
assert assessment['steady_window']['all_reads_current'] and assessment['steady_window']['total_read_count']==248
assert assessment['steady_window']['reader_source_sha256']==sha(ROOT/'oanda_practice_live_dashboard.py')
report=ROOT/'docs/FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md'
assert report.is_file() and all(token not in report.read_text(encoding='utf8') for token in ("pending at this report","LIVE_PROBE_PENDING"))
now=datetime.now(timezone.utc).isoformat()
nav='''September 7 dashboard follow-up: the reader now checks the actual post-read
clock and retains only previously verified summaries bound by the current fresh
heartbeat during crossed publication generations, preventing valid pair
forecasts from briefly disappearing during normal publication. Persistent stale,
future, altered or invalid evidence still stays unavailable. The 68-pair study
registry, models, targets and order restrictions are unchanged. The original
coverage receipt and source archive remain dated history; the new receipt binds
the updated dashboard. The canonical vault mapping now contains 147 records.

'''
for relative in ('README.md','FOREX_AUDIT_START_HERE.md','docs/AUDIT_STATE_CURRENT.md','docs/VAULT_RECREATION_CURRENT.md'):
    path=ROOT/relative;text=path.read_text(encoding='utf8')
    assert 'September 7 dashboard follow-up:' not in text
    first,rest=text.split('\n',1)
    if relative=='FOREX_AUDIT_START_HERE.md':
        links='Vault records: `PAIR_DASHBOARD_CONSISTENCY_CURRENT.md` and `PAIR_DASHBOARD_CONSISTENCY_VALIDATION_CURRENT.json`.\n\n'
    else:
        prefix='' if relative.startswith('docs/') else 'docs/'
        receipt_prefix='../' if relative.startswith('docs/') else ''
        links=f'[Dashboard consistency report]({prefix}FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md) · [Validation]({receipt_prefix}FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json).\n\n'
    path.write_text(first+'\n\n'+nav+links+rest.lstrip('\n'),encoding='utf8')
entry=f'''\n\n## 2026-09-07 — dashboard publication read consistency\n\nRecorded {now}. Fixed two display races found after the pair-coverage export:
sampling the consumer clock before reading a newer heartbeat, and reading
summary/heartbeat files from different atomic publication generations. The
reader now observes time after reading and hashes, with bounded retries and
retained verified generations only while the fresh heartbeat binds their exact
bytes. Original source and forecast clocks remain unchanged. Future or stale clocks,
tampered seals, identities and persistent mismatches remain fail-closed.
Focused tests and sustained live observation across multiple summary generations
passed. Only the dashboard restarted; the supervisor and thirteen other workers
were preserved. The registered studies and earlier receipt bytes are unchanged.
See [the follow-up report](docs/FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md).
This fixes display continuity; predictive accuracy still needs prospective
baseline, calibration and after-cost evaluation.\n'''
with (ROOT/'FOREX_PROJECT_LOG.md').open('a',encoding='utf8') as handle:handle.write(entry)
path=ROOT/'FOREX_PENDING_IMPROVEMENTS.md';first,rest=path.read_text(encoding='utf8').split('\n',1)
path.write_text(first+'\n\n## September 7 — dashboard consistency verified; performance acceptance pending\n\n'
    'The dashboard generation/clock read races are fixed and live-verified. All\n'
    'registered model and scoring sources remain unchanged. Pair coverage still\n'
    'depends on each pair\'s own inputs and fresh quotes. Remaining acceptance is\n'
    'prospective H1 outcomes, calibration, direction/magnitude and after-cost\n'
    'baseline comparisons; raw probabilities remain uncalibrated. See\n'
    '[the consistency record](docs/FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md).\n\n'+rest.lstrip('\n'),encoding='utf8')
evidence=ROOT/'docs/validation/pair_dashboard_consistency_20260907'
evidence.mkdir(parents=True,exist_ok=False)
for path in OUT.iterdir():
    if path.is_file() and path.suffix.lower() in ('.json','.py','.ps1','.xml','.png','.cjs'):
        shutil.copy2(path,evidence/path.name)
shutil.copytree(OUT/'before_source',evidence/'before_source')
retry=OLD/'dashboard_snapshot_retry'
shutil.copytree(retry/'before_source',evidence/'dashboard_before_source')
shutil.copytree(retry/'before_retained_generation',evidence/'dashboard_before_retained_generation')
for path in retry.iterdir():
    if path.is_file() and path.suffix.lower() in ('.json','.xml'):
        shutil.copy2(path,evidence/path.name)
suite=ET.parse(retry/'tests.xml').getroot()
tests={key:sum(int(row.attrib.get(key,0)) for row in suite.iter('testsuite')) for key in ('tests','errors','failures','skipped')}
assert tests['errors']==tests['failures']==0
old={name:sha(ROOT/name) for name in ('FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json',
    'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json')}
assert old['FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json']=='6953f2d7259be748847ec0dd6a34abd1b2f482dd8be7aa6913bc7d906ff4cfb5'
assert old['FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json']=='3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792'
sources=set(registry['source_bindings'])|{'oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html',
    'test_oanda_pair_forecast_dashboard.py','test_oanda_collection_dashboard_status.py',
    'test_oanda_pair_local_forecast_worker_v1.py','forex_model_vault_sync.py','oanda_always_on_supervisor.ps1',
    'start_oanda_research_collection.ps1','config/pair_local_forecast_study_v1_20260907.json',
    'config/causal_forecast_study_current.json','README.md','FOREX_AUDIT_START_HERE.md',
    'docs/AUDIT_STATE_CURRENT.md','docs/VAULT_RECREATION_CURRENT.md','FOREX_PROJECT_LOG.md',
    'FOREX_PENDING_IMPROVEMENTS.md','docs/FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md'}
sources|={path.relative_to(ROOT).as_posix() for path in evidence.rglob('*') if path.is_file()}
register=validate_register(ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json',root=ROOT);assert register['valid']
receipt={'schema_version':'forex_pair_dashboard_consistency_validation_v1_20260907','observed_utc':now,
    'status':'dashboard_publication_read_consistency_verified','tests':tests,'registry_sha256':digest(registry),
    'registered_pairs':68,'registered_model_ledger_worker_sources_unchanged':registry['source_bindings'],
    'runtime_verification':'docs/validation/pair_dashboard_consistency_20260907/DASHBOARD_CONSISTENCY_RUNTIME_VERIFICATION_RETAINED_20260907.json',
    'repeated_api_verification':'docs/validation/pair_dashboard_consistency_20260907/DASHBOARD_API_CONSISTENCY_PROBE_RETAINED2_20260907.json',
    'live_probe_assessment':'docs/validation/pair_dashboard_consistency_20260907/DASHBOARD_API_CONSISTENCY_ASSESSMENT_20260907.json',
    'live_probe_scope':'All248 steady reads current across3summary transitions; raw detector remains incomplete for its extra independently-reconstructed-retention coverage criterion. Zero such live retained cases;226 adversarial tests cover retained cache provenance. Cold priming observations retained separately.',
    'first_retry_only_live_probe':'docs/validation/pair_dashboard_consistency_20260907/DASHBOARD_API_CONSISTENCY_PROBE_20260907.json',
    'first_retry_only_result':'Failed:38 unavailable results in184 reads; observed3.2-3.5second worker publication phase gaps exceeded short reader retries. Failed observations preserved.',
    'prior_receipts_unchanged':old,'prior_receipt_semantics':'Preserved dated receipts bind their own earlier source versions; this receipt binds the updated dashboard and current navigation.',
    'issue_register_validation':register,'can_place_orders':False,'prediction_improvement_demonstrated':False,
    'limitations':['A bounded multi-generation live check is not proof of uninterrupted future uptime.',
        'Persistent invalid or inconsistent publications remain unavailable.',
        'More coverage and display continuity do not establish prediction accuracy.',
        'No registered study sources, forecasts, outcomes or clocks were rewritten.'],
    'source_bindings':[{'path':relative,'bytes':(ROOT/relative).stat().st_size,'sha256':sha(ROOT/relative)} for relative in sorted(sources)]}
receipt_path=ROOT/'FOREX_PAIR_DASHBOARD_CONSISTENCY_VALIDATION_20260907.json'
assert not receipt_path.exists();write(receipt_path,receipt)
for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
    path=ROOT/name;value=load(path);latest=dict(value['latest_review'])
    latest.update(observed_utc=now,runtime_state='pair_forecasts_publishing_dashboard_consistency_verified',
        report='docs/FOREX_PAIR_DASHBOARD_CONSISTENCY_20260907.md',validation=receipt_path.name,
        validation_sha256=sha(receipt_path),prior_pair_coverage_validation='FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json')
    value['latest_review']=latest;write(path,value)
print(json.dumps({'receipt':str(receipt_path),'sha256':sha(receipt_path),'source_bindings':len(sources),'tests':tests}))
