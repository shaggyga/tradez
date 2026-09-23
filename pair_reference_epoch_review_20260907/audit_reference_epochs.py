"""Read-only structural diagnosis; never reads outcome price payloads or scores."""
from __future__ import annotations
import collections
from contextlib import closing
import datetime as dt
import hashlib
import json
from pathlib import Path
import sqlite3

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
DATA=ROOT/'data/oanda_training_manager'
OUT=Path(__file__).resolve().parent/'REFERENCE_EPOCH_AUDIT.json'
def stamp():return dt.datetime.now(dt.timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
paths=[DATA/'causal_forecast_study_eurusd_v1/study.sqlite',*sorted((DATA/'pair_local_forecast_study_v1/pairs').glob('*/study.sqlite'))]
result={'schema':'decision_reference_epoch_read_only_audit_v1_20260907','started_utc':stamp(),
        'project_root':str(ROOT),'selection_is_outcome_blind':True,
        'database_access':'mode=ro + query_only + BEGIN; only outcome row counts are read, not outcome identities or prices',
        'snapshots':[]}
for path in paths:
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=2)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        clock=stamp()
        contractrow=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        contract=json.loads(contractrow['payload'])
        activation=dict(db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone())
        forecasts=[dict(r) for r in db.execute('SELECT id,bucket,target,sha,payload FROM forecasts ORDER BY bucket')]
        quotes={r['id']:dict(r) for r in db.execute('SELECT id,market,available,payload FROM quotes')}
        counts={t:db.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ('quotes','attempts','forecasts','publication','consumption','entries','outcomes','exclusions')}
        highwater=db.execute('SELECT max(epoch) FROM clocks').fetchone()[0]
        db.rollback()
    groups=collections.defaultdict(list)
    badhash=[]
    for row in forecasts:
        payload=json.loads(row['payload'])
        if sha(encoded(payload))!=row['sha']:badhash.append(row['id'])
        ref=payload.get('reference_epoch')
        groups[float(ref)].append((row,payload))
    duplicates=[]
    for ref,group in sorted(groups.items()):
        if len(group)<2:continue
        ids={p.get('reference_quote_id') for _,p in group}
        originalquotes=[json.loads(quotes[q]['payload']) for q in sorted(ids) if q in quotes]
        quotesame=len({encoded({k:q.get(k) for k in ('instrument','pip_size','market_epoch','bid','ask','tradeable')}) for q in originalquotes})==1
        duplicate={'reference_epoch':ref,'member_count':len(group),
            'reference_quote_id_count':len(ids),'same_canonical_reference_quote_identity':quotesame,
            'all_original_targets_unchanged':all(p['target_epoch']==ref+3600 for _,p in group),
            'reference_quotes':originalquotes,
            'decisions':[{'decision_id':row['id'],'bucket':row['bucket'],'target_epoch':p['target_epoch'],
                'reference_quote_id':p.get('reference_quote_id'),'input_capture_sha256':p.get('input_capture_sha256'),
                'payload_sha256':row['sha'],'issued_epochs':sorted({f['issued_epoch'] for f in p['forecasts']}),
                'cohort_ids':sorted({f['cohort_id'] for f in p['forecasts']}),
                'forecast_ids':sorted(f['forecast_id'] for f in p['forecasts'])} for row,p in group]}
        duplicates.append(duplicate)
    qgroups=collections.defaultdict(list)
    for q in quotes.values():qgroups[float(q['market'])].append(json.loads(q['payload']))
    qcollisions=[{'market_epoch':m,'quote_count':len(qs),
                 'identities':[{k:q.get(k) for k in ('quote_id','instrument','pip_size','market_epoch','available_epoch','bid','ask','tradeable')} for q in qs]}
                for m,qs in qgroups.items() if len(qs)>1]
    excluded=sum(g['member_count'] for g in duplicates)
    result['snapshots'].append({'database_path':str(path),'read_started_utc':clock,'read_finished_utc':stamp(),
        'instrument':contract['instrument'],'contract_sha256':contractrow['sha'],'activation':activation,
        'cadence_sec':contract.get('cadence_sec'),'quote_max_age_sec':contract.get('quote_max_age_sec'),
        'counts':counts,'clock_highwater':highwater,'forecast_payload_hash_mismatches':badhash,
        'ordered_forecast_raw_payloads_sha256':sha(('\n'.join(r['payload'] for r in forecasts)).encode()),
        'duplicate_reference_groups':duplicates,'duplicate_reference_group_count':len(duplicates),
        'outcome_blind_excluded_decisions':excluded,'remaining_unique_reference_decisions':len(forecasts)-excluded,
        'duplicate_market_quote_epoch_groups':qcollisions})
result['summary']={'databases':len(result['snapshots']),
 'pair_databases':len(paths)-1,
 'decision_reference_duplicate_databases':sum(bool(s['duplicate_reference_groups']) for s in result['snapshots']),
 'pair_decision_reference_duplicate_databases':sum(bool(s['duplicate_reference_groups']) for s in result['snapshots'][1:]),
 'duplicate_decision_reference_groups':sum(s['duplicate_reference_group_count'] for s in result['snapshots']),
 'outcome_blind_excluded_decisions':sum(s['outcome_blind_excluded_decisions'] for s in result['snapshots']),
 'duplicate_quote_market_epoch_groups':sum(len(s['duplicate_market_quote_epoch_groups']) for s in result['snapshots']),
 'hash_mismatches':sum(len(s['forecast_payload_hash_mismatches']) for s in result['snapshots'])}
result['diagnosis']={'root_cause':'Cadence buckets are unique but decision market reference epochs are not. A fresh retained quote can be reused across a bucket boundary; evaluator enforces one decision per reference epoch.',
 'not_a_quote_deduplication_problem':True,
 'adapter_recommendation':'Preserve exact complete original exported dataset and protocol with hashes. Find repeated finite decision reference epochs before any scoring and exclude ALL members of each repeated epoch group. Retain original quote array, availability clocks, IDs, cohort IDs, protocol, targets and remaining decision order. Invoke unchanged frozen evaluator on a separate filtered copy. Include exclusion IDs/references and before/after counts. Fail closed on other validation errors.',
 'reporting_scope':'Separately versioned engineering diagnostic; not the original registered scorecard, model promotion, execution authority, or evidence that excluded decisions were wrong.',
 'future_collection_remedy':'A separately registered successor can prevent duplicate decision reference identities across cadence buckets. Do not edit current frozen collection or evaluator sources.'}
result['source_bindings']=[]
for name in ('oanda_causal_forecast_ledger_eurusd_v1.py','oanda_fixed_forecast_evaluation_eurusd_v1.py',
             'oanda_causal_forecast_ledger_pair_v1.py','oanda_fixed_forecast_evaluation_pair_v1.py'):
    p=ROOT/name;raw=p.read_bytes();result['source_bindings'].append({'path':str(p),'sha256':sha(raw),'bytes':len(raw)})
result['finished_utc']=stamp()
OUT.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'output':str(OUT),'summary':result['summary'],'affected':[{'path':s['database_path'],'counts':s['counts'],
 'excluded':s['outcome_blind_excluded_decisions'],'remaining':s['remaining_unique_reference_decisions'],
 'references':[g['reference_epoch'] for g in s['duplicate_reference_groups']]} for s in result['snapshots'] if s['duplicate_reference_groups']]},indent=2))
