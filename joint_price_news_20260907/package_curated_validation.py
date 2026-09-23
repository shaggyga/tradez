"""Copy explicit completed audit evidence with immutable source/destination hashes."""
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import xml.etree.ElementTree as ET

SOURCE=Path(__file__).resolve().parent
WORKSPACE=SOURCE.parent
PROJECT=WORKSPACE/'trad'
DESTINATION=PROJECT/'docs/validation/joint_price_news_20260907'
MANIFEST=DESTINATION/'CURATED_EVIDENCE_COPY_MANIFEST_20260907.json'

JOINT_FILES='''
worker_runtime_first_tests.xml
worker_runtime_final_tests.xml
worker_runtime_context_tests.xml
worker_first_tests.xml
worker_final_tests.xml
storage_news_gate_verified_tests.xml
storage_news_gate_final_tests.xml
STORAGE_RELOAD_DISPATCH_20260907.json
RUNTIME_BEFORE_JOINT_START_20260907.json
JOINT_START_DISPATCH_20260907.json
ACTIVATION_RECEIPT_20260907.json
JOINT_WORKER_SOURCE_FREEZE_20260907.json
canonical_runtime/WORKER14_RUNTIME_VERIFICATION_20260907.json
canonical_runtime/OS_PROCESS_INVENTORY_20260907.json
canonical_runtime/JOINT_CANONICAL_INITIAL_RUNTIME_AUDIT_20260907.json
canonical_runtime/INITIAL_SCHEDULER_DELAY_REVIEW_20260907.json
live_preflight/summary.json
live_preflight/result.json
live_preflight/heartbeat.json
live_preflight/INDEPENDENT_PREFLIGHT_AUDIT_20260907.json
model_inputs/MODEL_INPUT_IMPLEMENTATION_RECEIPT_20260907.json
model_inputs/JOINT_RETROSPECTIVE_TRAINING_AUDIT_20260907.json
model_inputs/joint_model_inputs_tests.xml
ledger_evaluation/real_integration_first_tests.xml
ledger_evaluation/NEWS_GUARD_INDEPENDENT_BOUNDARY_REVIEW_20260907.json
ledger_evaluation/JOINT_LEDGER_WORKER_READONLY_REVIEW_20260907.json
ledger_evaluation/JOINT_LEDGER_EVALUATION_VALIDATION_20260907.json
ledger_evaluation/JOINT_INPUT_WORKER_INDEPENDENT_REVIEW_20260907.json
ledger_evaluation/initial_tests.xml
ledger_evaluation/final_tests.xml
ledger_evaluation/causal_news_tests.xml
ledger_evaluation/ablation_comparison_first_tests.xml
price_v2_first_outcomes/PRICE_V2_FIRST_OUTCOMES_ASSESSMENT_20260907.json
price_v2_first_outcomes/PRICE_V2_FIRST_OUTCOMES_SANITY_20260907.json
price_v2_first_outcomes/PRICE_V2_FIRST_OUTCOMES_20260907.md
price_v2_first_outcomes/PROJECT_REPORT_PUBLICATION_20260907.json
fairness_independent/SCHEDULER_PARITY_PROBE_a27fe6e6ee3b.json
fairness_independent/SCHEDULER_PARITY_PROBE_33978088db94.json
fairness_independent/JOINT_V2_OPERATIONAL_ADDITIONS_INDEPENDENT_REVIEW_20260907.json
fairness_independent/JOINT_SCHEDULER_V2_INDEPENDENT_REVIEW_20260907.json
fairness_independent/JOINT_DASHBOARD_V2_INDEPENDENT_REVIEW_20260907.json
dashboard/joint_dashboard_tests216.xml
dashboard/joint_dashboard_tests.xml
scheduler_v2/worker_first_tests.xml
scheduler_v2/worker_final_tests.xml
scheduler_v2/live_preflight68/summary.json
scheduler_v2/live_preflight68/result.json
scheduler_v2/live_preflight68/heartbeat.json
scheduler_v2/live_preflight68/INDEPENDENT_PREFLIGHT68_AUDIT_20260907.json
scheduler_v2_operational/OPERATIONAL_V2_GATE_VALIDATION_20260907.json
scheduler_v2_operational/operational_gate_tests.xml
'''.split()

NEWS_FILES='''
CURRENT_NEWS_AUDIT_AND_REPAIR_20260907.md
CURRENT_NEWS_CONTENT_AUDIT_20260907.json
CORROBORATION_CLOCK_REPRODUCTION_20260907.json
SCB_PUBLIC_SOURCE_PROBE_20260907.json
NEWS_CAUSAL_AGGREGATION_VALIDATION_20260907.json
NEWS_RELOAD_AND_LIVE_REPAIR_RECORD_20260907.json
news_guard_tests.xml
news_guard_collector_final.xml
news_collector_regression_initial.xml
news_collector_regression.xml
runtime_reload_20260907/NEWS_PROCESS_RELOAD_VERIFICATION_20260907.json
runtime_reload_20260907/PROCESS_BEFORE.json
runtime_reload_20260907/CONTROLLED_STOPS.json
runtime_reload_20260907/before/manifest.json
supervisor_gate_reload_20260907/NEWS_SUPERVISOR_GATE_RELOAD_VERIFICATION_20260907.json
supervisor_gate_reload_20260907/PROCESS_BEFORE.json
supervisor_gate_reload_20260907/NEWS_PIDS_POST_COMPLETED_CYCLE.json
live_guarded_snapshot_20260907T221957/LIVE_NEWS_GUARD_VERIFICATION_20260907.json
'''.split()

FOCUSED_XML_COUNTS={
 'model_inputs/joint_model_inputs_tests.xml':63,
 'ledger_evaluation/final_tests.xml':440,
 'scheduler_v2/worker_final_tests.xml':60,
 'scheduler_v2_operational/operational_gate_tests.xml':157,
 'dashboard/joint_dashboard_tests.xml':227,
}

def digest(raw):return sha256(raw).hexdigest()
def test_counts(raw):
    cases=list(ET.fromstring(raw).iter('testcase'))
    return {'cases':len(cases),'failures':sum(len(c.findall('failure')) for c in cases),
            'errors':sum(len(c.findall('error')) for c in cases),'skipped':sum(len(c.findall('skipped')) for c in cases)}

if MANIFEST.exists():raise FileExistsError('Completed manifest already exists; no replacement permitted.')
selection=[(SOURCE,Path(relative),Path(relative)) for relative in JOINT_FILES]
news_root=WORKSPACE/'pair_forecast_repair_v2_20260907/news_audit'
selection.extend((news_root,Path(relative),Path('pair_forecast_repair_v2_20260907/news_audit')/relative) for relative in NEWS_FILES)
assert len(selection)==len({target for _,_,target in selection})

# Validate the entire explicit manifest before the first canonical write.
staged=[]
for base,relative,target_relative in selection:
    source=(base/relative).resolve();target=(DESTINATION/target_relative).resolve()
    assert source.is_relative_to(base.resolve()) and target.is_relative_to(DESTINATION.resolve())
    assert source.suffix in ('.json','.xml','.md')
    raw=source.read_bytes()
    if len(raw)>2*1024*1024:raise ValueError('Unexpectedly large curated evidence: '+str(relative))
    if source.suffix=='.json':json.loads(raw)
    counts=test_counts(raw) if source.suffix=='.xml' else None
    if target_relative.as_posix() in FOCUSED_XML_COUNTS:
        assert counts=={'cases':FOCUSED_XML_COUNTS[target_relative.as_posix()],'failures':0,'errors':0,'skipped':0}
    existed=target.exists()
    if existed and target.read_bytes()!=raw:raise ValueError('Refusing to replace different canonical bytes: '+str(target))
    staged.append((source,target,raw,counts,existed))

records=[]
for source,target,raw,counts,existed in staged:
    if source.read_bytes()!=raw:raise ValueError('Source changed after packaging preflight: '+str(source))
    if not existed:
        target.parent.mkdir(parents=True,exist_ok=True)
        with target.open('xb') as handle:handle.write(raw)
    assert target.read_bytes()==source.read_bytes()==raw
    record={'source':str(source),'destination':str(target),
            'source_workspace_relative':source.relative_to(WORKSPACE).as_posix(),
            'destination_package_relative':target.relative_to(DESTINATION).as_posix(),
            'source_sha256':digest(raw),'destination_sha256':digest(raw),'bytes':len(raw),
            'action':'preserved_existing_identical_bytes' if existed else 'copied_new_exact_bytes'}
    if counts is not None:record['retained_test_result']=counts
    records.append(record)

for source,target,raw,_,_ in staged:
    assert source.read_bytes()==target.read_bytes()==raw
manifest={'schema_version':'curated_joint_evidence_copy_manifest_v1_20260907',
          'generated_utc':datetime.now(timezone.utc).isoformat(),'status':'passed',
          'scope':'Exact-byte packaging of explicitly curated completed audit receipts, notes and test XML; each underlying artifact retains its own status and observation time.',
          'destination_root':str(DESTINATION.resolve()),'file_count':len(records),
          'total_bytes':sum(row['bytes'] for row in records),
          'copied_count':sum(row['action']=='copied_new_exact_bytes' for row in records),
          'preserved_existing_identical_count':sum(row['action']=='preserved_existing_identical_bytes' for row in records),
          'records':records,'all_source_destination_hashes_equal':True,
          'focused_final_xml_case_counts':FOCUSED_XML_COUNTS,
          'count_scope':'Separate suites can overlap. They are not summed into an independent-test total; prior failures are preserved and are not relabeled as passing.',
          'omitted_categories':['SQLite databases and raw ledger snapshots','Full raw news/member/shared-capture archives',
             'Duplicate model, worker and other source code copies','Full preflight registry copies',
             'Final canonical v2 runtime/reload/UI artifacts still owned and packaged separately by root'],
          'evidence_limits':['Receipts may bind excluded raw/source evidence that remains in the original workspace; this curated package is not a complete replay archive.',
             'Disposable 4-pair and 68-pair preflight results are not activated primary performance.',
             'Earlier v1 and 14-worker runtime receipts are dated observations, not final v2/15-worker verification.',
             'Price-v2 outcome assessment keeps its original narrow period, denominators and initial audit-only failure record.'],
          'packaging_helper':{'path':str(Path(__file__).resolve()),'sha256':digest(Path(__file__).read_bytes())},
          'source_config_ledger_changes':False,'runtime_or_broker_actions':False}
with MANIFEST.open('x',encoding='utf-8') as handle:
    json.dump(manifest,handle,indent=2,allow_nan=False);handle.write('\n')
print(json.dumps({'manifest':str(MANIFEST.resolve()),'sha256':digest(MANIFEST.read_bytes()),
                  'file_count':len(records),'bytes':manifest['total_bytes'],
                  'copied':manifest['copied_count'],'preserved':manifest['preserved_existing_identical_count']}))
