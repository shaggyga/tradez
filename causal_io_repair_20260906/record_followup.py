"""Append the Windows publication repair without rewriting initial receipts."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import sys
import xml.etree.ElementTree as ET

OUT=Path(__file__).resolve().parent; ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True;sys.path.insert(0,str(ROOT))
def load(p):return json.loads(p.read_text(encoding='utf-8-sig'))
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def write_new(p,value):
    with p.open('x',encoding='utf-8') as f:json.dump(value,f,indent=2);f.write('\n')
now=datetime.now(timezone.utc).isoformat()
runtime=load(OUT/'runtime_verification.json')
observation=load(OUT/'observation_after_activation.json')
preservation=load(OUT/'INITIAL_STUDY_PRESERVATION.json')
initial_export=load(OUT.parent/'causal_repair_20260906/VAULT_EXPORT_VERIFICATION.json')
assert len(observation['managed_running'])==12 and observation['supervisor_error_count']==0
assert all(row['state']=='Disabled' for row in observation['forex_tasks'])
assert observation['account']['aggregate']['openTradeCount']==observation['account']['aggregate']['pendingOrderCount']==0
assert all(value==0 for value in runtime['counts'].values())
assert runtime['errors']==0
test_xml=OUT/'io_tests.xml'
suites=ET.parse(test_xml).getroot()
test_count=sum(int(s.get('tests',0)) for s in suites.iter('testsuite'))
assert test_count>0 and sum(int(s.get('failures',0))+int(s.get('errors',0)) for s in suites.iter('testsuite'))==0
evidence=ROOT/'docs/validation/causal_io_repair_20260906'; evidence.mkdir(parents=True,exist_ok=False)
paths=[OUT/name for name in ('INITIAL_STUDY_PRESERVATION.json','regression_63_tests.xml','io_tests.xml',
    'run_tests.py','independent_io_harness.py','review_gate.ps1','gate_review_result.json','frozen_contract_sha256.json',
    'runtime_verification.json','study_heartbeat.json','study_scorecard.json','observation_after_activation.json')]
paths.append(OUT.parent/'causal_repair_20260906/VAULT_EXPORT_VERIFICATION.json')
for source in paths:
    with (evidence/source.name).open('xb') as f:f.write(source.read_bytes())
for source in (OUT/'before/data/oanda_training_manager/logs').glob('*.err.log'):
    with (evidence/source.name).open('xb') as f:f.write(source.read_bytes())
report=f'''# Forex causal study Windows publication repair — September 6, 2026

Recorded {now}. This addendum supersedes the runtime registration described in
[the initial timing repair](FOREX_CAUSAL_TIMING_REPAIR_20260906.md).

## What the final runtime check found

The initial study exited twice at 15:11:26 and 15:12:03 UTC because Windows
temporarily denied replacement of heartbeat.json. The supervisor restarted it;
its own error counter and the replacement process's heartbeat counter were
zero, so those fields alone did not reveal the earlier crashes. The preserved
stderr logs and supervisor process-start events establish the failures.

The initial worker was stopped before repair. Its contract, runtime files and
logs were copied with hashes, and its database integrity check passed. It had
zero quotes, attempts, forecasts, entries and outcomes. No evidence was moved
into the replacement ledger. The original source is preserved in vault archive
{initial_export['source']['archive']} (SHA256
{initial_export['source']['archive_sha256']}); the initial dated validation
receipt describes that source snapshot rather than the subsequently changed worker.

## Repair and independent checks

Atomic publication now serializes and fsyncs once, then retries transient
Windows replacement failures up to eight times with at most 1.13 seconds of
total backoff. All attempts use identical bytes and timestamps. Temporary-file
cleanup only touches files created by that call and cannot mask its result.
Heartbeat publication failure retains the last good file, increments an error
counter for the next successful heartbeat and continues quote collection.
Persistently stale heartbeat output remains visible to the supervisor.
Scorecards use the same helper in their existing background worker.

The numerical models, inputs, calibration, ledger and scoring rules are
unchanged. Retry delays do not shift issuance, publication, entry or original
target clocks; any quote gaps remain subject to the existing exclusions.

All 63 worker/retirement/register regression tests and {test_count} independent
publication tests passed. The supervisor gate again admitted exactly twelve
workers and blocked 107 names, including an unknown future worker, with no
real process actions during testing. The initial timing repair's 421 tests
and 29 subtests remain historical evidence for that repair.

## Current collection and pending evidence

The separately frozen contract
`causal_four_family_future_collection_v1_io_r2_20260906` activated at
{runtime['activation_utc']}. Its actual contract SHA256 is
{runtime['contract_sha256']}. It uses new cohort identities and the separate
`data/oanda_training_manager/causal_forecast_study_v1_io_r2/study.sqlite` ledger.
The old contract file and database remain unchanged and are no longer selected
by the supervisor. The current worker rejects their obsolete source binding.

At this observation twelve collection workers are running, the new heartbeat
has zero errors and is waiting for tradable quotes, and the practice account
has zero open trades or pending orders. Trading, promotion, proof eligibility,
source V9, rank V8 and both Forex scheduled tasks remain disabled.

There are still zero new forecasts or outcomes and no demonstrated prediction
improvement. The old strict recheck remains 407 decisions, 1,628 forecasts and
zero valid causal comparisons. Fresh collection requires 335 consecutive
common minute bars after a market gap (about 5.5 hours), plus collector lag,
cadence and a one-hour target before any new outcome can be scored.
An untouched after-cost sample and independent acceptance remain pending.

The vault source archive excludes runtime databases and WAL files. Continuing
this exact registration after recovery requires its database/WAL and contract;
a source-only recovery requires a separately registered study. Only local
vault bytes are verified; cloud synchronization completion is not observed.
'''
report_path=ROOT/'docs/FOREX_CAUSAL_IO_REPAIR_20260906.md'
with report_path.open('x',encoding='utf-8') as f:f.write(report)
followup=load(ROOT/'FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json')
followup.update(updated_utc=now,current_contract_id='causal_four_family_future_collection_v1_io_r2_20260906',
    current_contract_sha256=runtime['contract_sha256'],actual_activation_utc=runtime['activation_utc'],
    operational_revision='Windows atomic publication retry and heartbeat failure isolation; initial zero-forecast study preserved and retired.',
    supersedes_runtime_registration_in='FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json',
    prediction_improvement_demonstrated=False)
followup['completed'].append('Separate io_r2 registration after Windows publication fix; 63 regression and 16 independent IO tests, including real Windows reader lock, pass')
followup_path=ROOT/'FOREX_CAUSAL_IO_FOLLOWUP_20260906.json';write_new(followup_path,followup)
binding_paths=[ROOT/name for name in ('oanda_causal_forecast_study.py','oanda_always_on_supervisor.ps1',
    'test_oanda_causal_forecast_study_io.py','test_oanda_causal_forecast_study.py',
    'config/causal_forecast_study_v1_io_r2_20260906.json')]+[report_path,followup_path]+list(evidence.iterdir())
receipt={'schema_version':'forex_causal_io_repair_validation_v1','generated_utc':now,
    'status':'implemented_collecting_future_evidence','runtime_observation':runtime,
    'supervision_observation':observation,'initial_study_preservation':preservation,
    'initial_source_snapshot':initial_export['source'],
    'tests':{'regression_passed':63,'independent_io_passed':test_count,'supervisor_gate':load(OUT/'gate_review_result.json')},
    'source_bindings':[{'path':p.relative_to(ROOT).as_posix(),'sha256':sha(p)} for p in binding_paths],
    'prior_receipts_unchanged':{'FOREX_CAUSAL_TIMING_REPAIR_VALIDATION_20260906.json':sha(ROOT/'FOREX_CAUSAL_TIMING_REPAIR_VALIDATION_20260906.json')},
    'prediction_improvement_demonstrated':False,'orders_placed':0,'promotion_or_execution_enabled':False}
receipt_path=ROOT/'FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json';write_new(receipt_path,receipt)
register_path=ROOT/'FOREX_ISSUE_REGISTER_CURRENT.json';register=load(register_path);register['generated_utc']=now
for issue in register['issues']:
    if issue['issue_id'] in ('FX-20260906-FOUR-FAMILY-ENTRY-TARGET-AVAILABILITY','FX-20260906-CALIBRATION-REPLAY-ISSUE-CLOCK'):
        issue['evidence_paths'].append(report_path.relative_to(ROOT).as_posix())
        issue['validation_artifacts'].append({'path':receipt_path.name,'sha256':sha(receipt_path)})
        issue['implementation_ref']='causal_four_family_future_collection_v1_io_r2_20260906;timeframe_matrix_calibration_v2;future_acceptance_pending'
        issue['current_mitigation']='Legacy producers disabled; separate actual-clock io_r2 study registered after preserving the zero-forecast initial study and repairing Windows heartbeat replacement failures. Fresh observations and independent prediction acceptance remain pending.'
register_path.write_text(json.dumps(register,indent=2)+'\n',encoding='utf-8')
note=f'''Current update ({now}): the initial causal study had two Windows heartbeat
replacement failures. The fixed `io_r2` worker is separately registered and
running in collection mode; the original zero-forecast registration is preserved.
Trading stays off, and improved predictions still require fresh outcomes.
See [the current operational addendum](docs/FOREX_CAUSAL_IO_REPAIR_20260906.md)
and `FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json`.

Earlier dated observations follow; their registration details are historical.

'''
for name in ('README.md','FOREX_AUDIT_START_HERE.md','FOREX_PENDING_IMPROVEMENTS.md','docs/AUDIT_STATE_CURRENT.md'):
    p=ROOT/name;head,rest=p.read_text(encoding='utf-8').split('\n',1)
    local_note=note.replace('(docs/','(') if name.startswith('docs/') else note
    p.write_text(head+'\n\n'+local_note+rest.lstrip('\n'),encoding='utf-8')
p=ROOT/'FOREX_PROJECT_LOG.md';text=p.read_text(encoding='utf-8');pos=text.index('\n## ')
entry=f'''\n## 2026-09-06 — Windows study heartbeat repair and separate io_r2 registration\n\nRecorded {now}. Final process-log review found two transient heartbeat replace\nfailures despite the recovered worker's zero error counter. Added bounded atomic\nretry and heartbeat failure isolation; preserved the initial zero-forecast\nstudy and activated a new source-bound io_r2 contract at {runtime['activation_utc']}.\nNo numerical or timing-rule changes. 63 regression tests, {test_count} independent\npublication tests and the 12-worker/107-blocked-name gate pass. Collection is\nrunning, trading disabled, and fresh predictive acceptance remains pending.\nSee docs/FOREX_CAUSAL_IO_REPAIR_20260906.md and its hash-bound validation receipt.\n\n'''
p.write_text(text[:pos]+entry+text[pos:],encoding='utf-8')
from oanda_issue_register_validator import validate_register
validation=validate_register(register_path,root=ROOT);assert validation['valid'],validation
write_new(OUT/'issue_register_validation.json',validation)
print(json.dumps({'receipt_sha256':sha(receipt_path),'tests':receipt['tests'],'register_valid':validation['valid']}))
