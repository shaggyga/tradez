"""Record verified collection restart separately from historical stopped receipts."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import xml.etree.ElementTree as ET

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent/'trad'
EVIDENCE = ROOT/'docs/validation/market_open_20260906'
NOW = datetime.now(timezone.utc).isoformat()
sys.dont_write_bytecode = True
sys.path[:0] = [str(ROOT),str(ROOT.parent)]
def load(p): return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def write(p, value):
    p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')

runtime = load(OUT/'runtime_verified.json')
assert runtime['research_supervisor_count'] == 1
assert runtime['managed_worker_count'] == 12
assert runtime['unexpected_project_workers'] == []
assert runtime['quote_stream']['connected'] is True
assert runtime['account']['current'] is True
assert runtime['dashboard']['http_status'] == 200
assert runtime['dashboard']['collection_status']['status'] == 'running'
assert runtime['study']['errors'] == 0
assert runtime['study']['can_place_orders'] is False
preflight = load(OUT/'preflight.json')
assert preflight['status'] == 'passed'
contract = load(ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json')
for name,digest in contract['source_bindings'].items(): assert sha(ROOT/name) == digest,name
gate = load(OUT/'gate_review_result.json')
assert gate['status'] == 'passed' and gate['allowed_worker_count'] == 12
dashboard_validation = load(OUT/'review/COLLECTION_DASHBOARD_CHANGE_RECEIPT_R2_20260906.json')
for row in dashboard_validation['sources']: assert sha(ROOT/row['name']) == row['sha256'],row['name']
assert sha(OUT/'review'/dashboard_validation['tests']['xml']) == dashboard_validation['tests']['xml_sha256']
suite = ET.parse(OUT/'review'/dashboard_validation['tests']['xml']).getroot().find('testsuite')
assert int(suite.attrib['tests']) == 57 and int(suite.attrib['failures']) == int(suite.attrib['errors']) == 0
assert sha(OUT/'review/COLLECTION_DASHBOARD_CHANGE_RECEIPT_20260906.json') == dashboard_validation['previous_receipt_sha256']

assert not EVIDENCE.exists()
EVIDENCE.mkdir(parents=True)
for folder in ('review','dashboard_status','before_source','before_current_records'):
    source = OUT/folder
    if source.exists():
        for p in source.rglob('*'):
            if p.is_file() and p.suffix.lower() in {'.py','.ps1','.json','.xml','.md','.html','.txt'}:
                destination = EVIDENCE/folder/p.relative_to(source)
                destination.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(p,destination)
for name in ('preflight.json','preflight.py','gate_review_result.json','review_gate.ps1','launch_result.txt',
             'sleep_policy.txt','observe.ps1','observation_startup.json','observation_verified.json',
             'observation_all_workers.json','observation_stagger_complete.json','observation_before_dashboard_reload.json',
             'observation_final.json','observation_final_confirmed.json',
             'runtime_verified.json','verify_ready.py','dashboard_restart.json','dashboard_restart_final.json',
             'reload_dashboard.ps1','record_readiness.py'):
    shutil.copyfile(OUT/name,EVIDENCE/name)

report = f'''# Forex market-open preparation — September 6, 2026

Recorded {NOW}. Following the request to be ready for market open, the existing **collection-only runtime is back online** on `C:\\Users\\zmoor\\Documents\\forex\\trad`. This supersedes the earlier stopped disposition for the collection session. It is not practice-entry readiness.

OANDA US lists ordinary FX reopening at **17:05 New York / 21:05 UTC on September 6**. Its holiday notice retains regular FX hours on September 6–7. TRY instruments have separate hours. Sources: [hours of operation](https://www.oanda.com/us-en/trading/hours-of-operation/) and [holiday hours](https://www.oanda.com/us-en/trading/holiday-trading-hours/), checked September 6.

One research-only supervisor, PID {runtime['research_supervisor_pid']}, manages twelve admitted workers. The practice account, quote stream, news capture/mapping, M1 archive, clock monitoring, storage monitoring, integrity audit, dashboard and registered causal study are running. The quote stream is connected for {runtime['quote_stream']['quoted_instruments']} pairs and the account observation is current, with {runtime['account']['open_trades']} open trades and {runtime['account']['pending_orders']} pending orders. Both the dashboard page and main API respond successfully. The final observations and source bindings are in `FOREX_MARKET_OPEN_VALIDATION_20260906.json`.

Current prices remain nontradeable before the opening. Stream connectivity and fresh process heartbeats do not establish fresh executable prices. The study is correctly waiting for a tradeable quote with zero forecasts/outcomes so far and no errors. Real opening-price arrival has not yet been observed.

The frozen study needs **335 consecutive common completed M1 bars across seven pairs**, approximately 5 hours 35 minutes after the weekend gap, plus archive/cadence delay. An original one-hour target must then mature before an outcome is available. This expected warm-up must not be bypassed with fabricated historical availability. Exact-price scoring and the new joint-return baselines remain offline additions pending separate registered integration; this restart preserves the current study's six source bindings, contract payload and original activation.

The dashboard now displays collection status separately from the trading-signal active flag. Its status is based on timestamped study, quote-stream and account observations with explicit identity and freshness checks; missing/stale observations remain unknown. The stream check accounts for time elapsed since its published observation, and the banner scopes its disabled-trading claim to the registered study. This does not set strategy activity or trading readiness to true. Only the dashboard worker was restarted to load this presentation change; both controlled reloads and the intervening independent review are preserved.

The broad integrity audit currently reports **{runtime['integrity']['status']}**, with `{runtime['integrity']['supported_decision']}` as its supported decision. Its unrelaxed failures include intentionally absent trading/evidence inputs. This is not a full-project passing integrity certificate. The storage guard reports **{runtime['storage']['status']}**. Broker clock offset cannot be measured from live executable updates while the market remains closed; host time checks and the clock monitor remain active.

Practice trading remains disabled: the closed allow-list excludes the executor, strategy lab, lifecycle/authorization and promotion workers. The saved lifecycle still has zero confirmed candidates. No gates were relaxed, no orders were submitted by this preparation and no real-market predictive improvement is asserted. The supervisor restarts admitted child processes during the running session. Both legacy Forex scheduled tasks remain disabled because the old watchdog targets the broader trading launcher. A machine reboot still requires the collection launcher; idle sleep is already disabled in the current power plan, without changing settings.

Validation includes the independent readiness review, exact registered source/dependency checks, a preserved stopped-study copy with a passing SQLite quick check, the isolated twelve-name/107-blocked-name launcher review, 57 passing focused dashboard tests, independent repair verification and post-restart API/process observations. The first post-reload observation caught the new dashboard child before the supervisor's next PID-list refresh; the final confirmed observation reconciles every current worker and child PID. The preflight records the canonical contract payload digest separately from the literal configuration-file digest. The prior entry-improvement and stop receipts are retained unchanged as dated history.

The canonical restart command is `trad\\start_oanda_research_collection.ps1` from the parent Forex directory. The local vault receives the new readiness records and a verified source snapshot; its databases and private credentials are not part of that source archive.
'''
report_path=ROOT/'docs/FOREX_MARKET_OPEN_20260906.md'
assert not report_path.exists()
report_path.write_text(report,encoding='utf-8')
bound = [ROOT/p for p in ('oanda_practice_live_dashboard.py','oanda_main_signal_dashboard.html',
    'test_oanda_collection_dashboard_status.py','oanda_always_on_supervisor.ps1','start_oanda_research_collection.ps1',
    'forex_model_vault_sync.py','docs/FOREX_MARKET_OPEN_20260906.md')] + sorted(p for p in EVIDENCE.rglob('*') if p.is_file())
prior = {'FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json':'d2b733b0ade4c0c1a53aa8e544f34cbf578fe7dc44d76864bb3ad25d659922be'}
for name,digest in prior.items(): assert sha(ROOT/name)==digest
receipt={'schema_version':'forex_market_open_validation_v1','generated_utc':NOW,
    'runtime_mode':'ResearchCollectionOnly','status':'collection_online_waiting_for_market_open',
    'user_authorized_collection_resume':True,'runtime_observation':runtime,'preflight':preflight,
    'gate_review':gate,'dashboard_validation':dashboard_validation,'registered_study_unchanged':True,'can_place_orders':False,
    'practice_trading_ready':False,'new_offline_models_activated':False,'prediction_improvement_demonstrated':False,
    'prior_receipts_unchanged':prior,
    'source_bindings':[{'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p),'bytes':p.stat().st_size} for p in bound],
    'limitations':['Opening-price receipt remains unobserved before 17:05 New York.',
        'Study warm-up, original target maturity and independent acceptance remain required.',
        'Broad degraded integrity is not converted to a pass for collection mode.',
        'Both legacy tasks stay disabled; no autonomous reboot recovery is claimed.']}
receipt_path=ROOT/'FOREX_MARKET_OPEN_VALIDATION_20260906.json'
assert not receipt_path.exists()
write(receipt_path,receipt)

def prepend(path,prose):
    original=path.read_text(encoding='utf-8')
    first,rest=original.split('\n',1)
    path.write_text(first+'\n\n'+prose+'\n\nEarlier runtime observations below describe their recorded times.\n'+rest,encoding='utf-8')
for name,prefix in [('README.md','docs/'),('docs/AUDIT_STATE_CURRENT.md','')]:
    prepend(ROOT/name,f'Current market-open update ({NOW}): **collection is running again** at user request, under one research supervisor with twelve admitted workers. This supersedes the earlier stopped runtime notes. **Practice trading remains disabled.**\n\n[The readiness record]({prefix}FOREX_MARKET_OPEN_20260906.md) distinguishes healthy account/data connectivity from market-open price availability, the 335-minute research warm-up and pending predictive acceptance. Validation is `FOREX_MARKET_OPEN_VALIDATION_20260906.json`.')
prepend(ROOT/'FOREX_AUDIT_START_HERE.md',f'Current market-open update ({NOW}): **the actual C-drive project is collecting again**, with one research supervisor and twelve admitted workers. Practice trading remains disabled. This supersedes the stopped runtime notes below. Start with `MARKET_OPEN_CURRENT.md` and `MARKET_OPEN_VALIDATION_CURRENT.json`; source counterparts are `docs/FOREX_MARKET_OPEN_20260906.md` and `FOREX_MARKET_OPEN_VALIDATION_20260906.json`. Opening prices and research warm-up remain pending. The earlier entry improvements and missing-model follow-up are retained unchanged.')
prepend(ROOT/'docs/VAULT_RECREATION_CURRENT.md',f'Current runtime update ({NOW}): the canonical C project has resumed collection only. Source recreation remains an offline operation and does not recreate its databases or authorize trading. See the market-open readiness record for current observations.')
for name in ('FOREX_AUDIT_STATE_CURRENT.json','FOREX_COMMONS_CURRENT.json'):
    value=load(ROOT/name)
    value['latest_review']={'observed_utc':NOW,'runtime_state':'research_collection_running',
        'supersedes_prior_runtime_status':True,'market_open_readiness':'docs/FOREX_MARKET_OPEN_20260906.md',
        'validation':receipt_path.name,'can_place_orders':False,'practice_trading_ready':False,
        'all_other_embedded_snapshots_retain_their_original_timestamps':True}
    write(ROOT/name,value)
register_path=ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json'
register=load(register_path)
exact=next(row for row in register['issues'] if row['issue_id']=='FX-20260906-EXACT-MIDPOINT-SCORING')
exact['collection_state']='existing_study_collecting_exact_successor_not_registered'
exact['current_runtime_observation']={'observed_utc':NOW,'validation':receipt_path.name,
    'note':'Existing collection resumed; exact successor integration remains pending. This does not close the scoring issue.'}
write(register_path,register)
from oanda_issue_register_validator import validate_register
validated=validate_register(register_path,root=ROOT)
assert validated['valid'],validated
write(OUT/'issue_register_validation.json',validated)
print(json.dumps({'receipt':receipt_path.name,'sha256':sha(receipt_path),'bound_files':len(bound),'issue_register_valid':True}))
