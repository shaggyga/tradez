"""One-shot read-only ledger/performance snapshot; writes only new review evidence.

No schedule, loop, ledger constructor, broker client or runtime write is used.
Each pair is its own SQLite read transaction, so the capture is not globally atomic.
"""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Context, localcontext, ROUND_HALF_EVEN
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import time

ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad')
WORK = Path(__file__).resolve().parent
DATA = ROOT/'data/oanda_training_manager'
STUDY = DATA/'pair_local_forecast_study_v1'
REGISTRY = ROOT/'config/pair_local_forecast_study_v1_20260907.json'
REGISTRY_SHA = 'e0aa74fa5d20b7be983050625773834ea68d74ffc15b566ab5e03df9b3380d65'
TABLES = ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')
FAMILIES = ('probabilistic_state_space','ridge_return_repaired')
MAX_QUOTES, MAX_DECISIONS = 20000, 512

def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',',':'), allow_nan=False).encode()

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()

def bounded(path, maximum):
    with path.open('rb') as handle:
        raw = handle.read(maximum+1)
    if len(raw)>maximum:
        raise ValueError('file_limit')
    return raw

def verify_registry():
    raw=bounded(REGISTRY,1024*1024)
    if sha(raw)!=REGISTRY_SHA:
        raise ValueError('registry_hash_mismatch')
    registry=json.loads(raw)
    for name, expected in registry['source_bindings'].items():
        path=(ROOT/name).resolve()
        if path.parent!=ROOT or sha(bounded(path,2*1024*1024))!=expected:
            raise ValueError('registered_source_hash_mismatch')
    if len(registry['pairs'])!=68:
        raise ValueError('registry_pair_count')
    return registry

def read_ledger(pair,item):
    contract=item['contract']; expected=item['contract_sha256']
    if sha(encoded(contract))!=expected:
        raise ValueError('contract_hash_mismatch')
    path=(STUDY/'pairs'/pair/'study.sqlite').resolve()
    if not path.is_relative_to(STUDY.resolve()) or not path.is_file() or path.stat().st_size>128*1024*1024:
        raise ValueError('database_path_or_size')
    for suffix,limit in (('-wal',256*1024*1024),('-shm',2*1024*1024)):
        side=Path(str(path)+suffix)
        if side.exists() and (side.resolve().parent!=path.parent or side.stat().st_size>limit):
            raise ValueError('database_sidecar_limit')
    started=time.time(); deadline=time.monotonic()+3
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.2)) as db:
        db.row_factory=sqlite3.Row
        db.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        saved=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        if saved is None or saved['sha']!=expected or saved['payload'].encode()!=encoded(contract):
            raise ValueError('ledger_contract_mismatch')
        active=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        if active is None or active['contract_sha']!=expected:
            raise ValueError('activation_contract_mismatch')
        counts={table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in TABLES}
        if counts['quotes']>MAX_QUOTES or counts['forecasts']>MAX_DECISIONS:
            raise ValueError('snapshot_row_limit')
        size=sum(db.execute(f'SELECT coalesce(sum(length(payload)),0) FROM {table}').fetchone()[0] for table in ('quotes','forecasts'))
        if size>48*1024*1024:
            raise ValueError('snapshot_payload_limit')
        rows=db.execute('SELECT f.payload,f.sha,c.epoch FROM forecasts f LEFT JOIN consumption c ON c.id=f.id ORDER BY f.bucket').fetchall()
        quotes=[json.loads(row[0]) for row in db.execute('SELECT payload FROM quotes ORDER BY available,market,id')]
        outcomes=[dict(r) for r in db.execute('SELECT id,quote_id,epoch FROM outcomes ORDER BY id')]
        entries=[dict(r) for r in db.execute('SELECT id,quote_id,epoch FROM entries ORDER BY id')]
        exclusions=[dict(r) for r in db.execute('SELECT id,reason,epoch FROM exclusions ORDER BY id')]
        highwater=db.execute('SELECT max(epoch) FROM clocks').fetchone()[0]
        cutoff=time.time()
        if not 0<active['epoch']<=highwater<=cutoff:
            raise ValueError('snapshot_clock_order')
        db.rollback()
    decisions=[]
    for row in rows:
        decision=json.loads(row['payload'])
        if sha(encoded(decision))!=row['sha']:
            raise ValueError('forecast_hash_mismatch')
        for arm in decision['forecasts']:
            arm['committed_available_epoch']=row['epoch']
        decisions.append(decision)
    protocol=deepcopy(contract['evaluation_protocol'])
    protocol.update(historical_start_utc=utc(active['epoch']),historical_end_utc=utc(max(cutoff,active['epoch']+.000001)))
    dataset={'schema_version':'fixed_forecast_evaluation_input_v1','observed_cutoff_epoch':cutoff,
             'decisions':decisions,'quotes':quotes,'collection_counts':counts}
    metadata={'read_started_utc':utc(started),'observed_cutoff_utc':utc(cutoff),'observed_cutoff_epoch':cutoff,
              'activation_utc':utc(active['epoch']),'contract_sha256':expected,'counts':counts,
              'input_sha256':sha(encoded(dataset)),'protocol_sha256':sha(encoded(protocol)),
              'retained_outcome_rows':outcomes,'retained_entry_rows':entries,'ledger_exclusions':exclusions,
              'original_target_due_decisions':sum(d['target_epoch']<=cutoff for d in decisions),
              'original_target_not_yet_due_decisions':sum(d['target_epoch']>cutoff for d in decisions)}
    return dataset,protocol,metadata

def duplicate_groups(dataset):
    groups=defaultdict(list)
    for row in dataset['decisions']:
        groups[row['reference_epoch']].append(row['decision_id'])
    return [{'reference_epoch':epoch,'decision_ids':ids} for epoch,ids in sorted(groups.items()) if len(ids)>1]

def compact_report(report):
    keys=('status','generated_utc','evaluator_version','input_sha256','protocol_sha256','coverage',
          'collection_counts','outcome_counts','decision_exclusion_counts','quote_exclusions',
          'paired_summaries','paired_deltas','observed_utc_day_blocks','independent_sample_size')
    return {key:report.get(key) for key in keys}

def stored_scorecard(pair,item,now):
    path=STUDY/'pairs'/pair/'scorecard.json'
    if not path.is_file():
        return {'status':'missing','stored_scored_rows':0},[]
    raw=bounded(path,8*1024*1024); report=json.loads(raw)
    if report.get('study_contract_sha256')!=item['contract_sha256'] or report.get('instrument')!=pair:
        raise ValueError('stored_scorecard_identity')
    stamp=datetime.fromisoformat(report['generated_utc'].replace('Z','+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('stored_scorecard_naive_clock')
    age=now-stamp.timestamp()
    rows=report.get('decisions',[])
    if len(rows)!=report['coverage']['paired_scored_decisions']:
        raise ValueError('stored_scorecard_count_mismatch')
    result=compact_report(report)
    result.update(sha256=sha(raw),age_sec=round(age,3),future=age<0,
                  age_over_300_sec=age>300,stored_scored_rows=len(rows),
                  decision_ids=[r['decision_id'] for r in rows])
    return result,rows

def summarize_samples(samples, scorer):
    with localcontext(Context(prec=80,rounding=ROUND_HALF_EVEN)):
        return {family:scorer.summarize(rows)|{
            'direction_hits':sum(r['direction_correct'] for r in rows if r['side']),
            'positive_after_spread':sum(r['positive_after_spread'] for r in rows if r['side'])}
            for family,rows in samples.items()}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args(); destination=args.output.resolve()
    if destination.parent!=WORK or destination.suffix!='.json' or destination.exists():
        raise SystemExit('Choose a NEW JSON directly in this review workspace.')
    started=time.time(); registry=verify_registry()
    sys.path.insert(0,str(ROOT))
    import oanda_pair_local_forecast_study_v1 as worker
    import oanda_fixed_forecast_evaluation_pair_v1 as scorer
    from oanda_study_diagnostic_report_v1 import get_eurusd_diagnostic_report
    worker.load_registry(REGISTRY)
    pairs={}; samples={family:[] for family in FAMILIES}; totals=Counter()
    for pair,item in sorted(registry['pairs'].items()):
        value={'instrument':pair,'status':'unavailable'}; pairs[pair]=value
        try:
            dataset,protocol,meta=read_ledger(pair,item); value.update(meta)
            totals.update(meta['counts'])
            value['duplicate_reference_groups']=duplicate_groups(dataset)
            try:
                saved,savedrows=stored_scorecard(pair,item,time.time())
                value['stored_scorecard']=saved
                totals['stored_scored_decisions']+=len(savedrows)
            except Exception as exc:
                savedrows=[]; value['stored_scorecard']={'status':'unavailable','error':str(exc)}
            report=scorer.evaluate(dataset,protocol)
            value.update(status='evaluated',fresh_evaluation=compact_report(report))
            scored={r['decision_id']:r for r in report['decisions']}
            retained={r['id']:r for r in meta['retained_outcome_rows']}
            entries={r['id']:r for r in meta['retained_entry_rows']}
            value['retained_outcomes_not_fresh_scored']=sorted(set(retained)-set(scored))
            value['fresh_scored_without_retained_outcome']=sorted(set(scored)-set(retained))
            value['selected_quote_mismatches']=[identity for identity,row in scored.items()
                if (identity in retained and retained[identity]['quote_id']!=row['target']['quote_id'])
                or (identity in entries and entries[identity]['quote_id']!=row['entry']['quote_id'])]
            savedids={r['decision_id'] for r in savedrows}
            value['fresh_scored_not_in_stored_scorecard']=sorted(set(scored)-savedids)
            value['stored_scored_not_in_fresh_evaluation']=sorted(savedids-set(scored))
            value['saved_vs_fresh_scoring_mismatches']=[r['decision_id'] for r in savedrows if r['decision_id'] in scored
                and any(r['scores'][f][key]!=scorer.jsonable(scored[r['decision_id']]['scores'][f][key])
                        for f in FAMILIES for key in ('side','direction_correct','positive_after_spread','net_bps'))]
            value['fresh_scored_rows']=[{
                'decision_id':row['decision_id'],'reference_epoch':row['reference_epoch'],'target_epoch':row['target_epoch'],
                'entry_quote_id':row['entry']['quote_id'],'target_quote_id':row['target']['quote_id'],
                'actual_return_bps':row['actual_return_bps'],'outcome_class':row['outcome_class'],
                'scores':{family:{key:row['scores'][family][key] for key in
                          ('side','probability_up','direction_correct','positive_after_spread','net_bps','stress_net_bps')}
                          for family in FAMILIES}} for row in scored.values()]
            value['fresh_evaluation']['family_counts_and_metrics']=summarize_samples(
                {family:[r['scores'][family] for r in scored.values()] for family in FAMILIES},scorer)
            if value['selected_quote_mismatches'] or value['saved_vs_fresh_scoring_mismatches']:
                value['status']='reconciliation_failed'
            else:
                for family in FAMILIES:
                    samples[family].extend(r['scores'][family] for r in scored.values())
                totals['fresh_scored_decisions']+=len(scored)
        except Exception as exc:
            value.update(status='unavailable',error=str(exc) if isinstance(exc,ValueError) else type(exc).__name__)
    eur=get_eurusd_diagnostic_report(DATA)
    if eur.get('report'):
        eur['report']=compact_report(eur['report'])|{
            'family_counts_and_metrics':{f:{
                'direction_hits':sum(r['scores'][f]['direction_correct'] for r in get_eurusd_diagnostic_report(DATA)['report']['decisions'] if r['scores'][f]['side']),
                'positive_after_spread':sum(r['scores'][f]['positive_after_spread'] for r in get_eurusd_diagnostic_report(DATA)['report']['decisions'] if r['scores'][f]['side'])}
                for f in FAMILIES}}
    verify_registry()
    finished=time.time()
    output={'schema_version':'read_only_pair_performance_snapshot_v1_20260907',
        'status':'completed','started_utc':utc(started),'finished_utc':utc(finished),
        'duration_sec':round(finished-started,3),'helper_sha256':sha(Path(__file__).read_bytes()),
        'registry_sha256':REGISTRY_SHA,'registered_source_bindings':registry['source_bindings'],
        'pair_status_counts':dict(Counter(v['status'] for v in pairs.values())),
        'aggregate_counts':dict(totals),'fresh_pair_family_metrics':summarize_samples(samples,scorer),
        'pairs':pairs,'original_eurusd_supplemental_diagnostic':eur,
        'orders_authorized':False,'ledger_writes':False,'registered_scorecard_writes':False,
        'limitations':[
            'One-shot read-only snapshot. Pair transactions have individual cutoffs; they are not globally atomic.',
            'Fresh scores use the unchanged registered exact evaluator on each complete available ledger snapshot, without filtering pairs or decisions by outcomes.',
            'Stored scorecard rows and retained ledger outcomes are separate counts. Neither alone establishes all current valid scores.',
            'Pooled rates weight each scored pair-decision once; currencies and overlapping H1 horizons are correlated. Independent sample size is unknown.',
            'Net basis points use executable bid/ask entry and original target quotes, plus stated stress costs; no account-currency realized dollars, financing or guaranteed fills.',
            'A high reported probability is an uncalibrated model estimate, not measured accuracy.',
            'Original EUR/USD companion remains separate. Its supplemental outcome-blind duplicate exclusions do not replace its failing registered scorecard.',
            'This is observational engineering evidence and provides no model promotion, trading or capital authorization.']}
    raw=json.dumps(scorer.jsonable(output),indent=2,allow_nan=False).encode()+b'\n'
    if len(raw)>16*1024*1024:
        raise ValueError('output_size_limit')
    with destination.open('xb') as handle:
        handle.write(raw)
    print(json.dumps({'output':str(destination),'sha256':sha(raw),'bytes':len(raw),
        'started_utc':output['started_utc'],'finished_utc':output['finished_utc'],
        'pair_status_counts':output['pair_status_counts'],'aggregate_counts':output['aggregate_counts'],
        'fresh_pair_family_metrics':scorer.jsonable(output['fresh_pair_family_metrics']),
        'original_eurusd_status':eur['status']}))

if __name__=='__main__':
    main()
