"""Bounded read-only comparison of explicit archived sources and runtime status."""
from pathlib import Path
import json,hashlib,zipfile,time,sqlite3,subprocess
from datetime import datetime,timezone
from contextlib import closing

OUT=Path(__file__).parent;C=OUT.parent/'trad';D=Path('D:/forex/trad')
def sha_bytes(raw):return hashlib.sha256(raw).hexdigest()
def sha(p):return sha_bytes(p.read_bytes())
inventory=json.loads((OUT/'CURVE_SOURCE_ARCHIVE_INVENTORY_20260908.json').read_text(encoding='utf-8'))
archive=Path(sorted([a['path'] for a in inventory['local_archives'] if a.get('source_package_members')==36])[-1])
result={'schema':'feature_curve_recovery_runtime_verification_v1_20260908','observed_utc':datetime.now(timezone.utc).isoformat(),
        'archive':str(archive),'archive_bytes':archive.stat().st_size,'archive_sha256':sha(archive),'source_comparisons':[]}
with zipfile.ZipFile(archive) as z:
    for entry in z.infolist():
        name=entry.filename.replace('\\','/')
        if 'fresh_m1_intrahour/src/' not in name or not name.endswith('.py'):continue
        assert entry.file_size<=256*1024
        relative=name[name.index('fresh_m1_intrahour/'):];raw=z.read(entry);target=D/relative
        result['source_comparisons'].append({'member':name,'bytes':len(raw),'member_sha256':sha_bytes(raw),'d_path':str(target),
          'd_sha256':sha(target) if target.is_file() else None,'matches_d':target.is_file() and sha_bytes(raw)==sha(target)})
    result['relevant_payload_members']=[{'path':e.filename,'bytes':e.file_size} for e in z.infolist() if any(t in e.filename.lower() for t in ['unified_intrahour','ma_feature_grid','unified_forecast','latest_validation'])]
    result['private_path_members_not_read']=[e.filename for e in z.infolist() if any(part.lower() in ('creds','creds.py','.env','credentials.json','secrets.json','secrets') for part in e.filename.replace('\\','/').split('/'))]
for command in [['git','ls-files','--','fresh_m1_intrahour'],['git','check-ignore','-v','--no-index','--','fresh_m1_intrahour/src/unified_forecast.py','fresh_m1_intrahour/README.md']]:
    cp=subprocess.run(command,cwd=C,capture_output=True,text=True,encoding='utf-8',timeout=15)
    result.setdefault('git_evidence',[]).append({'command':command,'exit_code':cp.returncode,'stdout':cp.stdout,'stderr':cp.stderr})
result['omission_source_bindings']={p:sha(C/p) for p in ['.gitignore','tools/vault_worktree_snapshot.py']}
logs=sorted((C/'data/oanda_training_manager/logs').glob('always_on_supervisor_*.jsonl'),key=lambda p:p.stat().st_mtime)
log=logs[-1]
with log.open('rb') as handle:
    handle.seek(max(0,log.stat().st_size-1024*1024));tail=handle.read(1024*1024)
rows=[]
for line in tail.splitlines()[1:]:
    try:row=json.loads(line)
    except json.JSONDecodeError:continue
    if row.get('event')=='heartbeat':rows.append(row)
assert rows
heartbeat=rows[-1];names={'strategy_lab','practice_007_fast_executor','practice_007_signal_feed_availability','signal_feed_wal_maintenance',
 'second_forecast_hot','second_forecast_tracker','second_forecast_microstructure','second_forecast_fit','model_gap_live_signal','proof_shadow_predictors',
 'practice_007_quote_stream','pair_local_forecast_study_v1','pair_local_forecast_study_v2','joint_price_news_study_v1','joint_price_news_study_v2'}
result['supervisor_observation']={'path':str(log),'time':heartbeat.get('time'),'observed_epoch':time.time(),
  'managed':[{k:v for k,v in item.items() if k in ('name','running','started','pids','freshness')} for item in heartbeat.get('managed',[]) if item.get('name') in names]}
db=C/'data/oanda_training_manager/state/practice_007_signal_feed_v1.sqlite';result['feed']={'path':str(db),'exists':db.is_file()}
if db.is_file():
    deadline=time.monotonic()+5
    try:
        with closing(sqlite3.connect(db.as_uri()+'?mode=ro',uri=True,timeout=1)) as conn:
            conn.execute('PRAGMA query_only=ON');conn.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
            result['feed']['tables']=[r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            result['feed']['current_count']=conn.execute('SELECT COUNT(*) FROM candidates WHERE expires_epoch >= ?',(time.time(),)).fetchone()[0]
            result['feed']['latest_sources']=[dict(zip(('source','count','latest_published_epoch','latest_expiry_epoch'),r)) for r in conn.execute('SELECT source,COUNT(*),MAX(published_epoch),MAX(expires_epoch) FROM candidates GROUP BY source ORDER BY MAX(published_epoch) DESC LIMIT 20')]
    except sqlite3.Error as e:result['feed']['error']=str(e)
result['scope']='No source restoration, extraction, model loading, fitting, activation, orders or runtime modifications. Only explicitly selected .py archive members were read in memory; listed private-path archive members were not read. An independent credential audit is still required before any future selective recovery; complete archives must not be treated as safe source-only exports.'
dest=OUT/'RECOVERY_MEMBERS_RUNTIME_20260908.json';dest.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'path':str(dest),'sha256':sha(dest),'archive':str(archive),'source_members':len(result['source_comparisons']),
 'source_matches_d':sum(r['matches_d'] for r in result['source_comparisons']),'private_path_members_not_read':result['private_path_members_not_read'],
 'managed':[(r['name'],r['running'],r.get('freshness',{}).get('reason')) for r in result['supervisor_observation']['managed']], 'feed':result['feed'],
 'payload_artifacts':[r for r in result['relevant_payload_members'] if r['path'].endswith(('.joblib','.parquet'))]},indent=2))
