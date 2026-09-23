"""Read-only retirement-obligation evidence for old joint v1/v2 only.

No stop, recovery, checkpoint, quote backfill, fit or scoring. Snapshot identity
excludes growing quotes, attempts and clock logs; exact committed forecast and
settlement row hashes remain comparable immediately before any later retirement.
"""
from collections import Counter
from contextlib import closing
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import time

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
ROOT=Path(r'C:/Users/zmoor/Documents/forex/trad');DATA=ROOT/'data/oanda_training_manager'
REGISTRIES={'v1':('joint_price_news_study_v1_20260907.json','4f4b23454a17de805c6e3f775f458ddec0cb0c820b2c6e30a9593495d3e88d94'),
 'v2':('joint_price_news_study_v2_20260907.json','20fea86e661efd758536b81fa5c474a440feee2e6af48b28a1016b1538d3691e')}
FAMILY='ridge_price_news_v1'
COLUMNS={'forecasts':('id','bucket','attempt_id','reference','target','sha','payload'),
 'publication':('id','epoch','forecast_sha'),'consumption':('id','epoch','forecast_sha','publication_sha'),
 'entries':('id','quote_id','epoch'),'outcomes':('id','quote_id','epoch'),'exclusions':('id','reason','epoch')}
MAX_ROWS=4096;MAX_TABLE_BYTES=64*1024*1024


def need(ok,reason):
    if not ok:raise ValueError(reason)


def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(v):return hashlib.sha256(encoded(v)).hexdigest()
def sha(raw):return hashlib.sha256(raw).hexdigest()


def safe(path):
    path=Path(path).absolute();need('..' not in path.parts,'path_escape')
    for p in reversed([path,*path.parents]):
        s=p.lstat();need(not stat.S_ISLNK(s.st_mode) and not getattr(s,'st_file_attributes',0)&1024,'reparse_path')
    return path


def read(path,cap):
    path=safe(path)
    with path.open('rb') as f:raw=f.read(cap+1)
    need(len(raw)<=cap,'source_byte_bound');return raw


def registries():
    values={};bindings={}
    for study,(name,want) in REGISTRIES.items():
        raw=read(ROOT/'config'/name,1024*1024);need(sha(raw)==want,'registry_hash')
        r=json.loads(raw);need(r['schema_version']=='joint_price_news_registry_'+study+'_20260907','registry_schema')
        need(r['research_only'] is True and all(r.get(k) is False for k in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported')),'registry_authority')
        need(len(r['pairs'])==68 and len(r['source_bindings'])==16,'registry_inventory')
        for source,expected in r['source_bindings'].items():
            need(re.fullmatch(r'(?:[a-z0-9_]+/)*[a-z0-9_]+\.py',source),'source_path')
            need(sha(read(ROOT/source,8*1024*1024))==expected,'source_hash')
        values[study]=r;bindings[study]=dict(registry_sha256=want,source_bindings=r['source_bindings'])
    return values,bindings


def obligations(forecasts,tables,now):
    """Disposition is about original persisted obligations, not performance."""
    by={name:unique_rows(rows,name) for name,rows in tables.items()}
    counts=Counter();issues=[]
    for name,rows in by.items():
        for ident in rows:
            if ident not in forecasts:counts['orphan_'+name]+=1;issues.append(dict(id=ident,reason='orphan_'+name))
    for ident,f in forecasts.items():
        published=ident in by['publication'];consumed=ident in by['consumption'];entry=ident in by['entries'];outcome=ident in by['outcomes'];excluded=ident in by['exclusions']
        for key,condition in [('unpublished_forecast',not published),('unconsumed_forecast',not consumed),('future_original_target',f['target']>now),('outcome_and_exclusion',outcome and excluded),('outcome_without_entry',outcome and not entry)]:
            if condition:counts[key]+=1;issues.append(dict(id=ident,reason=key))
        if not outcome and not excluded:
            counts['unresolved_forecasts']+=1
            if consumed:
                deadline=f['target']+60 if entry else min(f['target'],by['consumption'][ident]['epoch']+60)
                reason='unresolved_deadline_elapsed' if now>deadline else 'unresolved_deadline_not_elapsed'
            else:reason='unresolved_without_consumption'
            counts[reason]+=1;issues.append(dict(id=ident,reason=reason))
    counts['terminal_outcomes']=len(by['outcomes']);counts['terminal_exclusions']=len(by['exclusions'])
    return dict(counts),issues


def unique_rows(rows,name):
    ids=[row['id'] for row in rows]
    need(all(type(ident) is str and ident for ident in ids) and len(ids)==len(set(ids)),'duplicate_or_invalid_id:'+name)
    return {row['id']:row for row in rows}


def selected_quote_identity(rows):
    need(len(rows)==len({row['id'] for row in rows}),'duplicate_selected_quote')
    return digest(sorted(rows,key=lambda row:row['id']))


def audit_one(path,contract,contract_sha,registry,ledger):
    began=time.time();safe(path);need(path.stat().st_size<=1024**3,'database_size_bound')
    ledger.validate_contract(contract);need(digest(contract)==contract_sha and contract['source_bindings']==registry['source_bindings'],'registered_contract_hash')
    raw_tables={};records={};quote_refs={};issues=[];total_bytes=0
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,4*1024*1024)
        allowed={sqlite3.SQLITE_SELECT,sqlite3.SQLITE_READ,sqlite3.SQLITE_FUNCTION,sqlite3.SQLITE_TRANSACTION,sqlite3.SQLITE_PRAGMA}
        db.set_authorizer(lambda action,a,b,c,d:sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
        stop=time.monotonic()+5;db.set_progress_handler(lambda:int(time.monotonic()>stop),1000);db.execute('BEGIN')
        saved=db.execute('SELECT id,sha,payload FROM contract').fetchall()
        need(len(saved)==1 and saved[0]['id']==1 and saved[0]['sha']==contract_sha and saved[0]['payload']==encoded(contract).decode(),'stored_contract_identity')
        pinned=time.time();activation=db.execute('SELECT id,epoch,contract_sha FROM activation').fetchall()
        need(len(activation)==1 and activation[0]['id']==1 and activation[0]['contract_sha']==contract_sha,'activation_identity')
        activation=dict(activation[0]);need(registry['created_epoch']<=activation['epoch']<=pinned,'activation_clock')
        schema=[dict(r) for r in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]
        need(len(schema)<=256,'schema_bound')
        for table,columns in COLUMNS.items():
            need(tuple(r[1] for r in db.execute('PRAGMA table_info('+table+')'))==columns,'table_schema:'+table)
            count=db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0];need(count<=MAX_ROWS,'row_count_bound:'+table)
            raw_tables[table]=[];records[table]=[];table_bytes=0
            for row in db.execute('SELECT '+','.join(columns)+' FROM '+table+' ORDER BY id'):
                record=dict(row);raw=encoded(record);table_bytes+=len(raw);need(table_bytes<=MAX_TABLE_BYTES,'table_payload_bound')
                raw_tables[table].append(record)
                records[table].append({**{k:v for k,v in record.items() if k!='payload'},'row_sha256':sha(raw),
                    **({'payload_raw_sha256':sha(record['payload'].encode())} if 'payload' in record else {})})
            total_bytes+=table_bytes
        maps={t:unique_rows(v,t) for t,v in raw_tables.items()};forecasts=maps['forecasts']
        def quote(ident):
            if ident not in quote_refs:
                row=db.execute('SELECT id,market,available,payload FROM quotes WHERE id=?',(ident,)).fetchone();need(row is not None,'referenced_quote_missing')
                row=dict(row);payload=json.loads(row['payload']);need(payload['market_epoch']==row['market'] and payload['available_epoch']==row['available'],'quote_column_clock_mismatch')
                need(payload['instrument']==contract['instrument'] and payload['tradeable'] is True and 0<row['market']<=row['available']<=pinned,'quote_identity_or_clock')
                quote_refs[ident]=dict(id=ident,market_epoch=row['market'],available_epoch=row['available'],row_sha256=digest(row))
            return quote_refs[ident]
        for ident,f in forecasts.items():
            try:
                payload=json.loads(f['payload']);need(digest(payload)==f['sha'] and payload['decision_id']==ident,'forecast_payload_hash_or_id')
                need(payload['instrument']==contract['instrument'] and payload['reference_epoch']==f['reference'] and payload['target_epoch']==f['target'],'forecast_pair_or_clock')
                need(f['target']==f['reference']+3600 and len(payload['forecasts'])==1,'original_h1_target')
                value=payload['forecasts'][0];issued=value['issued_epoch']
                need(value['cohort_id']==contract['cohorts'][FAMILY] and value['family']==FAMILY,'forecast_cohort')
                need(activation['epoch']<payload['attempt_epoch']<=issued<=pinned and value['reference_epoch']==f['reference'] and value['target_epoch']==f['target'],'forecast_issue_clock')
                attempt=db.execute('SELECT id,epoch,reference_id FROM attempts WHERE id=?',(f['attempt_id'],)).fetchone()
                need(attempt is not None and attempt['id']==payload['attempt_id'] and attempt['epoch']==payload['attempt_epoch'] and attempt['reference_id']==payload['reference_quote_id'],'forecast_attempt_binding')
                reference=quote(payload['reference_quote_id']);need(reference['market_epoch']==f['reference'] and reference['available_epoch']<=payload['attempt_epoch'],'reference_clock')
                p=maps['publication'].get(ident);c=maps['consumption'].get(ident);e=maps['entries'].get(ident);o=maps['outcomes'].get(ident);x=maps['exclusions'].get(ident)
                if p:need(p['forecast_sha']==f['sha'] and issued<=p['epoch']<=pinned,'publication_hash_or_clock')
                if c:need(p is not None and c['forecast_sha']==f['sha'] and c['publication_sha']==digest({'epoch':p['epoch'],'forecast_sha':f['sha']}) and p['epoch']<=c['epoch']<=pinned,'consumption_hash_or_clock')
                if e:
                    q=quote(e['quote_id']);need(c is not None and c['epoch']<q['market_epoch']<=q['available_epoch']<=c['epoch']+60 and q['available_epoch']<f['target'] and q['available_epoch']<=e['epoch']<=pinned,'entry_clock_or_consumption')
                if o:
                    q=quote(o['quote_id']);need(e is not None and f['target']<=q['market_epoch']<=q['available_epoch']<=f['target']+60 and q['available_epoch']<=o['epoch']<=pinned,'outcome_clock_or_entry')
                if x:
                    need(c is not None and x['epoch']<=pinned,'exclusion_clock_or_consumption')
                    if x['reason']=='missing_later_entry_before_deadline':need(e is None and x['epoch']>min(f['target'],c['epoch']+60),'entry_exclusion_deadline')
                    elif x['reason']=='missing_quote_at_original_target':need(e is not None and x['epoch']>f['target']+60,'target_exclusion_deadline')
                    else:raise ValueError('unknown_exclusion_reason')
            except (ValueError,KeyError,TypeError) as error:issues.append(dict(id=ident,reason=str(error) if re.fullmatch('[a-z0-9_]+',str(error)) else type(error).__name__))
        counts={table:len(rows) for table,rows in records.items()}
        auxiliary={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('attempts','diagnostics','inputs')}
        observed=time.time();disposition,unsettled=obligations(forecasts,{k:v for k,v in raw_tables.items() if k!='forecasts'},observed);issues.extend(unsettled)
        immutable=digest(dict(contract_sha256=contract_sha,activation=activation,tables=records))
    return dict(status='no_retirement_obligation_blocker_observed' if not issues else 'blocked',database_path=str(path),instrument=contract['instrument'],
        contract_sha256=contract_sha,cohort=contract['cohorts'][FAMILY],activation=activation,read_started_epoch=began,snapshot_pinned_epoch=pinned,read_completed_epoch=observed,
        validation_completed_epoch=time.time(),schema_sha256=digest(schema),row_counts=counts,auxiliary_counts_excluded_from_identity=auxiliary,original_target_max_epoch=max((r['target'] for r in forecasts.values()),default=None),
        original_target_min_epoch=min((r['target'] for r in forecasts.values()),default=None),disposition_counts=disposition,exclusion_reason_counts=dict(Counter(r['reason'] for r in raw_tables['exclusions'])),
        issues=issues,immutable_obligation_records_sha256=immutable,immutable_records=records,selected_quote_evidence=list(quote_refs.values()),
        selected_quote_evidence_sha256=selected_quote_identity(list(quote_refs.values())),bytes_processed=total_bytes,
        scope='Original persisted obligations and selected-quote clock checks. No price scoring, full input replay, earliest-quote replay or retrospective proof of quote absence; no live quote-table hash.')


def run(output):
    need(output.parent==BASE and not output.exists(),'fresh_external_directory');safe(BASE)
    own=Path(__file__).read_bytes();r,bindings=registries();sys.path.insert(0,str(ROOT));ledger=__import__('oanda_causal_forecast_ledger_joint_news_v1')
    output.mkdir();(output/'ledgers').mkdir();began=time.time();rows=[];all_cohorts=set()
    for study,registry in r.items():
        for pair,entry in sorted(registry['pairs'].items()):
            need(re.fullmatch('[A-Z]{3}_[A-Z]{3}',pair) and set(entry['families'])=={FAMILY},'pair_registry_slot')
            slot=entry['families'][FAMILY];contract=slot['contract'];need(contract['instrument']==pair,'pair_contract')
            need(contract['cohorts'][FAMILY] not in all_cohorts,'cross_study_cohort_reuse');all_cohorts.add(contract['cohorts'][FAMILY])
            path=DATA/('joint_price_news_study_'+study)/'pairs'/pair/FAMILY/'study.sqlite'
            try:value=audit_one(path,contract,slot['contract_sha256'],registry,ledger)
            except Exception as error:value=dict(status='blocked',database_path=str(path),instrument=pair,observed_epoch=time.time(),issues=[dict(reason=str(error) if re.fullmatch('[A-Za-z0-9_:]+',str(error)) else type(error).__name__)])
            value['study']=study;dest=output/'ledgers'/(study+'_'+pair+'.json');raw=encoded(value)+b'\n'
            with dest.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
            rows.append(dict(study=study,instrument=pair,path=str(dest),sha256=sha(raw),status=value['status'],row_counts=value.get('row_counts'),disposition_counts=value.get('disposition_counts'),
                issues=value['issues'],original_target_max_epoch=value.get('original_target_max_epoch'),read_started_epoch=value.get('read_started_epoch'),read_completed_epoch=value.get('read_completed_epoch'),immutable_obligation_records_sha256=value.get('immutable_obligation_records_sha256'),
                selected_quote_evidence_sha256=value.get('selected_quote_evidence_sha256')))
    after,after_bindings=registries();unchanged=bindings==after_bindings and Path(__file__).read_bytes()==own;need(unchanged,'source_changed_during_audit')
    totals={}
    for study in r:
        selected=[v for v in rows if v['study']==study];counts=Counter();dispositions=Counter()
        for v in selected:counts.update(v.get('row_counts') or {});dispositions.update(v.get('disposition_counts') or {})
        totals[study]=dict(ledgers=len(selected),blocked_ledgers=sum(v['status']=='blocked' for v in selected),row_counts=dict(counts),disposition_counts=dict(dispositions),
            maximum_original_target_epoch=max((v['original_target_max_epoch'] for v in selected if v['original_target_max_epoch'] is not None),default=None))
    report=dict(schema_version='old_joint_retirement_obligation_snapshot_v1_20260909',status='no_retirement_obligation_blocker_observed' if all(v['status']!='blocked' for v in rows) else 'blocked',
        started_epoch=began,completed_epoch=time.time(),source_bindings=bindings,all_registered_sources_unchanged=unchanged,helper_sha256=sha(own),registered_ledgers=len(rows),studies=totals,ledgers=rows,
        original_sources_or_records_changed=False,process_actions=False,GET=False,score_replay=False,
        limits=['Workers remain running. This is a per-ledger coherent snapshot, not globally atomic; repeat exact obligation identity checks immediately before any later controlled retirement.',
            'Growing quotes, attempts, inputs, diagnostics and clock logs do not enter the immutable obligation identity. Only original contracts/activation and f/publication/consumption/entry/outcome/exclusion rows do.',
            'Stored exclusion reason and original60-second deadline are checked, but no new settlement or missing-quote proof is manufactured.',
            'This establishes a bounded retirement-obligation snapshot, not a full numerical forecast-performance validation or authorization to stop processes.'])
    dest=output/'OLD_JOINT_RETIREMENT_OBLIGATIONS_20260909.json';raw=encoded(report)+b'\n'
    with dest.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    print(json.dumps(dict(status=report['status'],path=str(dest),sha256=sha(raw),studies=totals)),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output-directory',type=Path,required=True);a=p.parse_args();run(a.output_directory.absolute())
