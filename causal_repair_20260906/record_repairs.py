"""Publish dated repair records without rewriting historical audit receipts."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

OUT=Path(__file__).resolve().parent;ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True;sys.path[:0]=[str(ROOT),str(ROOT.parent)]
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write_new(p,value):
    with p.open('x',encoding='utf-8') as f:json.dump(value,f,indent=2);f.write('\n')
now=datetime.now(timezone.utc).isoformat()
runtime=load(OUT/'runtime_verification.json')
observation=load(OUT/'observation_after_activation.json')
assert len(observation['managed_running'])==12 and observation['supervisor_error_count']==0
assert all(row['state']=='Disabled' for row in observation['forex_tasks'])
assert observation['account']['aggregate']['openTradeCount']==observation['account']['aggregate']['pendingOrderCount']==0
evidence=ROOT/'docs/validation/causal_timing_repair_20260906'
evidence.mkdir(parents=True,exist_ok=False)
paths=[OUT/n for n in ('combined_421_tests.xml','worker_final_55_tests.xml','run_tests.py',
    'LEDGER_INDEPENDENT_REVIEW_20260906.json','LEDGER_INDEPENDENT_REVIEW_20260906.md',
    'ledger_review_v4.xml','run_ledger_tests.py','review_gate.ps1','gate_review_result.json',
    'runtime_verification.json','study_heartbeat.json','study_scorecard.json',
    'observation_after_activation.json','legacy_strict_recheck.json','frozen_contract_sha256.json')]
paths += [OUT.parent/'calibration_repair_20260906'/n for n in
    ('integration_tests.xml','randomized_oracle.py','randomized_oracle_result.json')]
for p in paths:
    name=('calibration_'+p.name) if p.parent.name=='calibration_repair_20260906' else p.name
    with (evidence/name).open('xb') as f:f.write(p.read_bytes())
report=f'''# Forex prediction timing repair — September 6, 2026

Recorded: {now}. Collection is running with twelve admitted workers. The
separate four-family study activated at {runtime['activation_utc']} and is
waiting for tradable EUR/USD quotes. It has zero forecasts and zero outcomes
at this observation. Trading, promotion and old model/outcome/calibration
producers remain disabled. The practice account has zero trades and orders.

## Repairs completed

The old four-family archive used pre-computation entry quotes, lacked
availability evidence and mixed original-entry and recording-relative targets.
The successor reads exact bounded M1 candle bytes for seven fixed major pairs,
retains hashes and observed-now clocks, aligns real timestamps, and requires
335–512 consecutive common minute bars. It reuses the four numerical algorithms
with frozen parameters; the seven-pair input universe is a new contract, not a
continuation of old cohorts. No model was selected by these new outcomes.

After all model computations complete, it records conservative feature and
learning-data availability, then samples actual issuance. All four forecasts
commit together. A separate postcommit receipt and an independent consumer
read precede the first later executable entry quote. Targets always remain the
original reference quote time plus one hour. Crashes cannot backdate visibility
or restart a horizon. Missing/late entry or target quotes become exclusions.
Captured training history is allowed as newly observed input; old forecasts,
outcomes and missing historical clocks are never imported or manufactured.

The old calibration replay still preserves its numerical diagnostics, but new
publications cannot advertise validation/proof/account readiness. A separate
V2 offline evaluator admits only unique, correctly scoped labels whose target,
quote maturity and committed availability precede each original forecast issue.
It handles unordered inputs, duplicated market events, ties and flat outcomes.
The strict comparison scorer now requires the same strictly later quote boundary
as the collector, including at equal timestamps.

## Operation and resource use

Only the separate study ledger is written. It has no broker or order interface
and does not feed the old signal, outcome, lifecycle or promotion databases.
Input capture is bounded to one MiB per pair and at most 1,024 retained source
rows; exact captured evidence is compressed in SQLite. A single model build
uses seven pairs and at most 512 common rows; native numerical thread counts
are limited to one. Quote/outcome collection continues while a model fits.
Scorecards run on a separate read-only database snapshot every fifteen minutes,
outside the quote polling loop. No automated evidence deletion was introduced.

Source, dependency, model and feature bindings are frozen in the registered
contract. Changed code or parameters require a separately registered study;
they are not silently accepted into this one. Windows process locking prevents
duplicate study workers. Both existing Forex scheduled tasks remain disabled.
The research supervisor was reloaded while existing collection children stayed
running; its old eleven-worker runtime had first been restored after it was
found stopped at the start of this resumed session.

Protocol: `config/causal_forecast_study_v1_20260906.json`.
Live heartbeat, scorecard and ledger: `data/oanda_training_manager/causal_forecast_study_v1/`.
These runtime files change; the validation evidence includes frozen observations.
The vault source export is not a backup of the active study's SQLite ledger.
Recover that ledger with its database/WAL and registered contract; a source-only
restore must use a fresh contract rather than silently recreating this study.
The scorecard uses the conservative offline evaluator's diagnostic schema.
The new ledger is prospective; independently confirmed performance remains pending.

## Verification and limits

The combined offline suite passed 421 tests and 29 subtests. A final focused
worker run passed 55 tests after retaining the original provider quote fields.
Independent ledger review passed 106 adversarial tests, including actual
synthetic candle capture and all four numerical models. Independent calibration
review matched a full-scan oracle for 5,000 randomized forecasts. Isolated
PowerShell checks admitted exactly twelve workers and rejected 107 other names,
including an unknown future worker. No test placed an order or touched a
production database. The initial combined test harness misread Windows file
URIs, producing 47 fixture-boundary errors; correcting URI decoding retained
the same write/network restrictions, and the complete rerun passed.

The actual newly registered ledger passes SQLite integrity checking and exactly
matches the frozen contract. Twelve admitted workers are running, the dashboard
responds, the new heartbeat is fresh, and no supervisor errors were observed.
The broader project audit remains degraded/no_trade; deliberately disabled
components and unresolved evidence checks were not disguised as healthy.

Both P1 repairs are implemented and awaiting evidence, rather than declared
fully accepted. The study needs at least 335 uninterrupted common minute bars
(about five and a half hours after a gap) and fresh tradeable quotes. Data
refresh cadence can add delay. After that, one-hour outcomes must mature.
Accuracy and net returns cannot be judged from zero new outcomes. Overlapping
fifteen-minute forecasts are not independent trials. Source/clock checks are
engineering evidence, not an independent audit of predictive edge or an order
authorization. Fitted weight artifacts are not retained; reproducibility binds
source, fixed parameters, exact captured inputs and dependency versions.

Rechecking all 407 old decisions still yields zero valid causal pairs, with
all 1,628 archived forecasts retained. Prior findings—49.39% shadow direction
and 8.51% positive after spread; all four fixed archived models below simple
baselines—are unchanged historical diagnostics. This repair improves measurement
integrity. It does not claim that predictions have already improved.
'''
report_path=ROOT/'docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md'
with report_path.open('x',encoding='utf-8') as f:f.write(report)
followup={'schema_version':'forex_causal_timing_followup_v1','updated_utc':now,
    'runtime':'research_collection_plus_separate_causal_study_running',
    'completed':['Separate timestamped input and forecast/receipt/entry/target ledger',
        'Fail-closed legacy readiness and issue-clock-aware calibration V2',
        '421 tests plus29 subtests; independent clock/crash/real-model fixture review',
        'Actual future-study registration and observed closed-market waiting'],
    'pending':['Obtain first genuine four-family forecast batch, later entry and original-target outcome',
        'Accumulate an untouched sample and compare fixed baselines after spread/cost stress',
        'Independent validation of availability/provenance and effective sample size before any eligibility change',
        'Full-project degraded checks, source V9/rank V8 activation and external data gaps remain separate'],
    'prediction_improvement_proven':False,'can_place_orders':False,
    'source_v9_collection_enabled':False,'rank_v8_collection_enabled':False}
write_new(ROOT/'FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json',followup)
bindings=['oanda_causal_forecast_ledger.py','oanda_causal_forecast_study.py','oanda_causal_forecast_inputs.py',
    'oanda_timeframe_matrix_calibration.py','oanda_timeframe_matrix_calibration_v2.py',
    'oanda_fixed_forecast_evaluation.py','oanda_always_on_supervisor.ps1',
    'test_oanda_causal_forecast_inputs.py','test_oanda_causal_forecast_study.py','test_oanda_causal_forecast_ledger.py',
    'test_oanda_timeframe_matrix_calibration.py','test_oanda_timeframe_matrix_calibration_v2.py',
    'config/causal_forecast_study_v1_20260906.json','docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md',
    'FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json']
bindings += [p.relative_to(ROOT).as_posix() for p in evidence.iterdir() if p.is_file()]
receipt={'schema_version':'forex_causal_timing_repair_validation_v1','generated_utc':now,
    'status':'implemented_collecting_future_evidence','runtime_observation':runtime,
    'supervision_observation':observation,'combined_tests_passed':421,'subtests_passed':29,
    'final_worker_tests_passed':55,'independent_ledger_tests_passed':106,
    'calibration_randomized_oracle_forecasts':5000,
    'source_bindings':[{'path':p,'sha256':sha(ROOT/p)} for p in bindings],
    'historical_forecast_imports':0,'prediction_improvement_demonstrated':False,
    'orders_placed':0,'promotion_or_execution_enabled':False,
    'historical_strict_recheck':load(OUT/'legacy_strict_recheck.json')['coverage'],
    'prior_receipts_unchanged':{p.name:sha(p) for p in [ROOT/'FOREX_FIXED_EVALUATION_VALIDATION_20260906.json',
        ROOT/'FOREX_FIXED_EVALUATION_RESULTS_20260906.json',ROOT/'FOREX_RESEARCH_RESTART_VALIDATION_20260906.json',
        ROOT/'FOREX_OPTIMIZATION_VALIDATION_20260906.json',ROOT/'FOREX_REPAIR_VALIDATION_20260905.json']}}
receipt_path=ROOT/'FOREX_CAUSAL_TIMING_REPAIR_VALIDATION_20260906.json'
write_new(receipt_path,receipt)
register_path=ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json';register=load(register_path)
register['generated_utc']=now
for issue in register['issues']:
    if issue['issue_id'] in ('FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY','FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK'):
        issue['status']='implemented_collecting'
        issue['status_history'].append({'at_utc':now,'status':'implemented_collecting'})
        issue['current_mitigation']='Legacy producers disabled; separate actual-clock study is registered and waiting for future observations. No prediction or execution acceptance claimed.'
        issue['implementation_ref']='causal_four_family_future_collection_v1_20260906;timeframe_matrix_calibration_v2;421tests;future_acceptance_pending'
        issue['evidence_paths'] += ['docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md']
        issue['validation_artifacts'].append({'path':receipt_path.name,'sha256':sha(receipt_path)})
register_path.write_text(json.dumps(register,indent=2)+'\n',encoding='utf-8')
note='''Latest September 6 timing repair: collection now runs twelve workers, including
a separately registered four-family study. It is waiting for fresh market data;
trading remains disabled. Both prediction-clock fixes are implemented, with
future evidence still pending. See [the repair record](docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md)
and `FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json`. Earlier entries below are dated history.

'''
for name in ('README.md','FOREX_PENDING_IMPROVEMENTS.md'):
    p=ROOT/name;text=p.read_text(encoding='utf-8');head,rest=text.split('\n',1)
    p.write_text(head+'\n\n'+note+rest.lstrip('\n'),encoding='utf-8')
p=ROOT/'FOREX_AUDIT_START_HERE.md';text=p.read_text(encoding='utf-8');head,rest=text.split('\n',1)
guide='''Latest September 6 update: read `CAUSAL_TIMING_REPAIR_CURRENT.md`,
`CAUSAL_TIMING_REPAIR_VALIDATION_CURRENT.json`, `CAUSAL_TIMING_FOLLOWUP_CURRENT.json`
and `CAUSAL_FORECAST_STUDY_PROTOCOL_CURRENT.json` in this vault. Source equivalents
are `docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md`, the corresponding dated JSONs,
and `config/causal_forecast_study_v1_20260906.json`. Twelve research workers now
run, including a separate future-only study. Trading stays disabled. The clock
fixes are implemented; fresh prediction/outcome validation remains pending.
Earlier stopped and eleven-worker observations below are dated history.

'''
p.write_text(head+'\n\n'+guide+rest.lstrip('\n'),encoding='utf-8')
p=ROOT/'docs/AUDIT_STATE_CURRENT.md';text=p.read_text(encoding='utf-8');head,rest=text.split('\n',1)
p.write_text(head+'\n\n'+note.replace('(docs/','(')+rest.lstrip('\n'),encoding='utf-8')
p=ROOT/'FOREX_PROJECT_LOG.md';text=p.read_text(encoding='utf-8');pos=text.index('\n## ')
entry=f'''\n## 2026-09-06 — causal timing repair and new future-only study\n\nRecorded {now}. Resumed collection after finding the previous runtime stopped.\nImplemented a separate timestamped seven-pair input/four-family EURUSD study,\npostcommit publication plus independent consumption, later executable entry,\nand immutable original target. Retired old proof/outcome/calibration workers.\nLegacy calibration readiness now fails closed; separate V2 filters original\nissue-time labels. Bounded input/model work and asynchronous scorecards protect\nquote polling. Combined421tests/29subtests; independent106ledger tests and5000\ncalibration oracle forecasts pass. New study registered at {runtime['activation_utc']},\nwith zero forecasts/outcomes and closed-market waiting; twelve research workers\nare healthy and trading stays off. Both P1 statuses are implemented_collecting,\nnot complete. Historical407decisions still yield zero valid causal pairs.\nSee docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md and its validation/followup JSONs.\n\n'''
p.write_text(text[:pos]+'\n'+entry+text[pos:],encoding='utf-8')
from oanda_issue_register_validator import validate_register
validation=validate_register(register_path,root=ROOT)
assert validation['valid'],validation
write_new(OUT/'issue_register_validation.json',validation)
print(json.dumps({'status':receipt['status'],'receipt_sha256':sha(receipt_path),'register':validation}))
