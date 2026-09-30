"""Index-only repair of technical-store read latency. No observation/label writes."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time

INDEXES={
 'idx_observations_pair_time_publication_v1':'CREATE INDEX idx_observations_pair_time_publication_v1 ON observations(pair,t,published)',
 'idx_outcomes_state_count_v1':'CREATE INDEX idx_outcomes_state_count_v1 ON outcomes(state)',
}
PENDING='''SELECT o.t FROM observations o LEFT JOIN outcomes y
 ON y.pair=o.pair AND y.t=o.t AND y.horizon=?
 WHERE o.pair=? AND o.published>0 AND o.published<=? AND o.t+?<=? AND y.t IS NULL ORDER BY o.t'''
STATES='SELECT state,COUNT(*) FROM outcomes GROUP BY state'

def measure(db,query,args):
    start=time.monotonic();rows=db.execute(query,args).fetchall()
    return {'elapsed_seconds':time.monotonic()-start,'rows':len(rows),
            'result_sha256':hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest(),
            'plan':list(db.execute('EXPLAIN QUERY PLAN '+query,args))}

def maintain(path,*,apply=False,maximum_seconds=90):
    path=Path(path).absolute()
    if not path.is_file() or any(p.is_symlink() or getattr(p,'is_junction',lambda:False)() for p in (path,*path.parents)):
        raise ValueError('existing_plain_database_required')
    started=time.monotonic();now=time.time()
    db=sqlite3.connect(path.as_uri()+('?mode=rw' if apply else '?mode=ro'),uri=True,timeout=2)
    try:
        db.set_progress_handler(lambda:int(time.monotonic()-started>maximum_seconds),1000)
        db.execute('BEGIN IMMEDIATE' if apply else 'BEGIN')
        existing=dict(db.execute("SELECT name,sql FROM sqlite_master WHERE type='index'"))
        for name,sql in INDEXES.items():
            if name in existing and existing[name]!=sql:raise ValueError('index_definition_mismatch:'+name)
        # Identical snapshot, parameters, original consumer SQL and populations.
        queries={'states':(STATES,())}
        for pair in ('EUR_USD','AUD_CAD','USD_JPY'):
            for h in (5,15,30,60):queries[pair+'_'+str(h)]=(PENDING,(h,pair,now,h*60,int(now)-60))
        before={k:measure(db,*v) for k,v in queries.items()}
        counts={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('observations','outcomes')}
        created=[]
        if apply:
            for name,sql in INDEXES.items():
                if name not in existing:db.execute(sql);created.append(name)
        after={k:measure(db,*v) for k,v in queries.items()}
        if any(before[k]['rows']!=after[k]['rows'] or before[k]['result_sha256']!=after[k]['result_sha256'] for k in queries):
            raise ValueError('original_query_population_changed')
        if any(db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0]!=n for t,n in counts.items()):raise ValueError('row_count_changed')
        if apply and (not any('COVERING INDEX idx_observations' in r[-1] for r in after['EUR_USD_60']['plan'])
                      or any('TEMP B-TREE' in r[-1] for r in after['states']['plan'])):raise ValueError('query_plan_not_repaired')
        if time.monotonic()-started>maximum_seconds:raise sqlite3.OperationalError('maintenance_time_bound')
        db.commit()
        return {'schema':'forex.technical_store_index_maintenance.v1','utc':datetime.now(timezone.utc).isoformat(),
                'database':str(path),'mode':'apply_indexes' if apply else 'read_only','status':'pass',
                'indexes_created':created,'counts':counts,'content_updates':0,'models_fitted':0,
                'before':before,'after':after,'elapsed_seconds':time.monotonic()-started,'maximum_seconds':maximum_seconds}
    except BaseException:db.rollback();raise
    finally:db.close()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',type=Path,required=True)
    p.add_argument('--apply',action='store_true');p.add_argument('--receipt',type=Path,required=True);a=p.parse_args()
    if a.receipt.exists():raise ValueError('new_receipt_required')
    try:result=maintain(a.database,apply=a.apply)
    except Exception as exc:
        result={'status':'failed','utc':datetime.now(timezone.utc).isoformat(),'error':type(exc).__name__+':'+str(exc)}
        a.receipt.write_text(json.dumps(result,indent=2)+'\n');raise
    a.receipt.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'status':result['status'],'counts':result['counts'],'indexes_created':result['indexes_created'],
      'elapsed_seconds':result['elapsed_seconds'],'states_seconds_before':result['before']['states']['elapsed_seconds'],
      'states_seconds_after':result['after']['states']['elapsed_seconds']}))
