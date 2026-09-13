"""One-shot selected SQLite online backups. Source handles are read-only/query-only.

No live checkpoint, vacuum, replay, ledger constructor or runtime mutation.
Each BEGIN-pinned database has its own observation clock; the set is not globally atomic.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time

sys.dont_write_bytecode = True
ROOT = Path(r'C:\Users\zmoor\Documents\forex\trad').resolve()
WORK = Path(__file__).resolve().parent
DEST = Path(r'D:\ForexRecovery\revamp_20260908T1353Z\databases').resolve()
STUDIES = ('joint_price_news_study_v2', 'joint_price_news_study_v1', 'pair_local_forecast_study_v2')
TABLES = ('contract','activation','clocks','quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')

def encoded(v):
    return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()

def utc(t=None):
    return datetime.fromtimestamp(time.time() if t is None else t,timezone.utc).isoformat()

def exclusive_json(path,value):
    with path.open('x',encoding='utf-8',newline='\n') as f:
        json.dump(value,f,indent=2,allow_nan=False); f.write('\n')

def plan():
    entries=[]; registries=[]; bindings={}
    for study in STUDIES:
        path=ROOT/'config'/f'{study}_20260907.json'
        registry=json.loads(path.read_bytes())
        assert registry['registry_id']==f'{study}_20260907'
        assert len(registry['pairs'])==68
        assert registry['research_only'] is True
        for k in ('can_authorize','can_place_orders','can_promote','proof_eligible','account_eligible','historical_rows_imported'):
            assert registry[k] is False, (study,k)
        for name,digest in registry['source_bindings'].items():
            source=(ROOT/name).resolve(); assert source.parent==ROOT and sha(source)==digest,(study,name)
            assert name not in bindings or bindings[name]==digest,(study,name)
            bindings[name]=digest
        registries.append({'study':study,'path':str(path),'sha256':sha(path),'bytes':path.stat().st_size})
        for pair,item in sorted(registry['pairs'].items()):
            assert re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',pair)
            for family,spec in sorted(item['families'].items()):
                assert family in ('ridge_price_news_v1','ridge_return_repaired','probabilistic_state_space')
                contract=spec['contract']; digest=spec['contract_sha256']
                assert hashlib.sha256(encoded(contract)).hexdigest()==digest
                assert contract['instrument']==pair and contract['family']==family
                source=(ROOT/'data/oanda_training_manager'/study/'pairs'/pair/family/'study.sqlite').resolve()
                intended=(ROOT/'data/oanda_training_manager'/study).resolve()
                assert source.is_relative_to(intended) and source.is_file()
                size=source.stat().st_size; wal=Path(str(source)+'-wal')
                wal_size=wal.stat().st_size if wal.exists() else 0
                assert size<256*1024*1024 and wal_size<256*1024*1024
                target=(DEST/study/pair/family/'study.sqlite').resolve()
                assert target.is_relative_to(DEST) and not target.exists()
                entries.append({'study':study,'instrument':pair,'family':family,'source':str(source),'destination':str(target),'observed_main_bytes':size,'observed_wal_bytes':wal_size,'contract_sha256':digest,'contract':contract})
    free=shutil.disk_usage(DEST.anchor).free
    expected=sum(e['observed_main_bytes']+e['observed_wal_bytes'] for e in entries)
    assert free>2*expected+1024**3
    assert len(entries)==272
    return {'schema_version':'selected_model_ledger_recovery_plan_v1','generated_utc':utc(),'destination_root':str(DEST),'free_bytes_before':free,'estimated_main_plus_wal_bytes':expected,'database_count':len(entries),'registries':registries,'source_bindings':bindings,'entries':entries}

def backup_one(item):
    source=Path(item['source']); target=Path(item['destination'])
    started=time.time(); deadline=time.monotonic()+90
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('xb'): pass
    result={k:v for k,v in item.items() if k!='contract'}
    result['backup_started_utc']=utc(started)
    def guard(status,remaining,total):
        if time.monotonic()>deadline: raise TimeoutError('bounded_backup_deadline')
    with closing(sqlite3.connect(source.as_uri()+'?mode=ro',uri=True,timeout=2)) as src:
        src.execute('PRAGMA query_only=ON')
        src.set_progress_handler(lambda:int(time.monotonic()>deadline),10000)
        src.execute('BEGIN')
        saved=src.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
        assert saved and saved[0]==item['contract_sha256'] and saved[1].encode()==encoded(item['contract'])
        snapshot_observed=time.time()
        active=src.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
        assert active and active[1]==saved[0] and math.isfinite(active[0]) and 0<active[0]<=snapshot_observed
        schemas=dict(src.execute("SELECT name,sql FROM sqlite_master WHERE type='table'"))
        assert set(schemas)==set(TABLES)
        counts={table:src.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in TABLES}
        highwater=src.execute('SELECT max(epoch) FROM clocks').fetchone()[0]
        assert highwater is not None and active[0]<=highwater<=snapshot_observed
        with closing(sqlite3.connect(target,timeout=2)) as dst:
            src.backup(dst,pages=1024,progress=guard,sleep=.01)
        src.rollback()
    assert all(not Path(str(target)+suffix).exists() for suffix in ('-wal','-shm','-journal'))
    with closing(sqlite3.connect(target.as_uri()+'?mode=ro&immutable=1',uri=True)) as copied:
        copied.execute('PRAGMA query_only=ON')
        integrity=[r[0] for r in copied.execute('PRAGMA integrity_check')]
        assert integrity==['ok'],integrity
        copied_counts={table:copied.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in TABLES}
        assert copied_counts==counts
        assert dict(copied.execute("SELECT name,sql FROM sqlite_master WHERE type='table'"))==schemas
        assert copied.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()==saved
        assert copied.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()==active
    result.update(snapshot_observed_epoch=snapshot_observed,snapshot_observed_utc=utc(snapshot_observed),activation_epoch=active[0],highwater_epoch=highwater,counts=counts,schema_sha256=hashlib.sha256(encoded(schemas)).hexdigest(),integrity_check=integrity,backup_completed_utc=utc(),snapshot_bytes=target.stat().st_size,snapshot_sha256=sha(target))
    card=source.with_name('scorecard.json')
    result['stored_scorecard']={'status':'missing'}
    if card.is_file():
        assert card.stat().st_size<64*1024*1024
        before=card.stat(); payload=card.read_bytes(); after=card.stat()
        if (before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns):
            value=json.loads(payload); dest=target.with_name('scorecard_original.json')
            with dest.open('xb') as f:f.write(payload)
            result['stored_scorecard']={'status':'captured_separately','path':str(dest),'sha256':hashlib.sha256(payload).hexdigest(),'bytes':len(payload),'observed_utc':utc(),'generated_utc':value.get('generated_utc'),'study_contract_sha256':value.get('study_contract_sha256'),'note':'Atomic producer file captured after DB backup; not asserted to share the database transaction.'}
        else: result['stored_scorecard']={'status':'changed_during_read_not_retained'}
    return result

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--copy',action='store_true'); args=ap.parse_args()
    info=plan(); WORK.mkdir(parents=True,exist_ok=True)
    if not args.copy:
        exclusive_json(WORK/'SELECTED_LEDGER_RECOVERY_PLAN_20260908.json',info)
        print(json.dumps({k:info[k] for k in ('database_count','estimated_main_plus_wal_bytes','free_bytes_before')})); return
    manifest=WORK/'SELECTED_LEDGER_BACKUP_MANIFEST_20260908.json'
    journal=WORK/'SELECTED_LEDGER_BACKUP_PROGRESS_20260908.jsonl'
    assert not manifest.exists() and not journal.exists()
    DEST.mkdir(parents=True,exist_ok=True)
    for reg in info['registries']:
        dest=DEST/'registries'/Path(reg['path']).name; dest.parent.mkdir(exist_ok=True)
        payload=Path(reg['path']).read_bytes(); assert hashlib.sha256(payload).hexdigest()==reg['sha256']
        with dest.open('xb') as f:f.write(payload)
        reg['backup_path']=str(dest)
    results=[]
    with journal.open('x',encoding='utf-8',newline='\n') as log:
        for index,item in enumerate(info.pop('entries'),1):
            result=backup_one(item); results.append(result)
            log.write(json.dumps(result,sort_keys=True)+'\n');log.flush()
            if index%16==0 or index==272: print(json.dumps({'completed':index,'total':272,'last_study':item['study']}),flush=True)
    assert all(sha(ROOT/name)==expected for name,expected in info['source_bindings'].items())
    assert all(sha(Path(row['path']))==row['sha256'] for row in info['registries'])
    info.update(schema_version='selected_model_ledger_backup_manifest_v1',status='passed',completed_utc=utc(),entries=results,total_snapshot_bytes=sum(r['snapshot_bytes'] for r in results),free_bytes_after=shutil.disk_usage(DEST.anchor).free,helper_sha256=sha(__file__),scope='272 selected registered model ledgers; original snapshots only; no raw quote archive, source governance database or legacy 80-ledger set copied; per-database coherent observations, not globally atomic.')
    exclusive_json(manifest,info)
    print(json.dumps({'manifest':str(manifest),'sha256':sha(manifest),'status':'passed','total_snapshot_bytes':info['total_snapshot_bytes']}),flush=True)

if __name__=='__main__':main()
