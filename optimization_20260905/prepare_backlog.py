"""Record bounded optimization work without changing trading contracts."""
from datetime import datetime, timezone
from pathlib import Path
import json

ROOT=Path(__file__).resolve().parent.parent/'trad'
payload={
    'schema_version':'forex_optimization_backlog_v1',
    'generated_utc':datetime.now(timezone.utc).isoformat(),
    'runtime_disposition':'intentionally_stopped',
    'source_audit':'docs/FOREX_PERFORMANCE_AUDIT_20260905.md',
    'items':[
        {'id':'OPT-20260906-INTEGRITY-PUBLICATION','priority':'P2','status':'in_progress',
         'improvement':'Publish compact integrity summaries and retain immutable content-addressed episode detail.',
         'acceptance':['All checks, failures, authority flags, counts and unomitted fields equal the full payload',
                       'Exact full detail reconstruction; corrupt/missing references fail',
                       'Changing summary timestamps reuses unchanged detail bytes',
                       'Checkpoint exports and relocated history preserve coherent resolvable detail',
                       'Measure first publication and repeat cost, serialization, decode, bytes and memory on the saved payload'],
         'runtime_validation':'pending a separately requested restart'},
        {'id':'OPT-20260906-INTEGRITY-QUERY-PROFILE','priority':'P2','status':'pending',
         'improvement':'Profile expensive integrity queries on coherent realistic read-only fixtures before changing queries or indexes.',
         'acceptance':['Compare verdicts and snapshot cutoffs with full reconciliation','Measure execution plans, row counts and query timing'],
         'boundary':'No production database tuning or weaker evidence checks in this reporting optimization'},
        {'id':'OPT-20260906-STORAGE-RETENTION','priority':'P2','status':'pending',
         'improvement':'Measure sustained growth and apply verified retention only to eligible archival/log material.',
         'acceptance':['Inventory separately by runtime ledger, source observation, log and archive',
                       'Archive recovery/hash checks precede any approved retirement of redundant copies'],
         'boundary':'No deletions, live VACUUM or historical evidence changes in this pass'},
        {'id':'OPT-20260906-COLLECTOR-YIELD','priority':'P2','status':'pending',
         'improvement':'Profile duplicate/reclassification work and per-source incremental yield before altering poll cadence.',
         'acceptance':['Compare canonical outputs and first-known clocks','Measure useful fresh items per request and official-event delay'],
         'boundary':'Configured source count is not a performance or accuracy metric'},
        {'id':'OPT-20260906-PREDICTION-QUALITY','priority':'P1','status':'assessment_in_progress',
         'improvement':'Publish an honest prediction scorecard with exact cohorts, denominators, cost effects, baselines and causality limits.',
         'acceptance':['Distinguish completed-outcome replay diagnostics from prospective predictions',
                       'Preserve effective sample size and current no-trade gates','Do not tune frozen models against observed outcomes'],
         'next_research_gate':'New untouched prospective after-cost evidence and appropriate baselines; existing successors remain disabled'}
    ]}
(ROOT/'FOREX_OPTIMIZATION_BACKLOG_20260906.json').write_text(json.dumps(payload,indent=2)+'\n',encoding='utf-8')
print('Recorded five optimization priorities and their acceptance boundaries.')
