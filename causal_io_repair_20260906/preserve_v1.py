"""Preserve the stopped initial registration before a separate operational revision."""
from pathlib import Path
import hashlib
import json
import sqlite3
from datetime import datetime, timezone

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
paths=[ROOT/name for name in (
    'oanda_causal_forecast_study.py','oanda_always_on_supervisor.ps1',
    'forex_model_vault_sync.py','README.md','FOREX_AUDIT_START_HERE.md',
    'FOREX_PENDING_IMPROVEMENTS.md','FOREX_PROJECT_LOG.md','docs/AUDIT_STATE_CURRENT.md',
    'FOREX_ISSUE_REGISTER_CURRENT.json','config/causal_forecast_study_v1_20260906.json')]
study=ROOT/'data/oanda_training_manager/causal_forecast_study_v1'
paths.extend(p for p in study.iterdir() if p.is_file())
logs=ROOT/'data/oanda_training_manager/logs'
paths.extend(logs.glob('causal_forecast_study_v1_supervised_20260906_*.err.log'))
paths.append(logs/'always_on_supervisor_20260906_110820.jsonl')
backup=OUT/'before'; backup.mkdir(exist_ok=False)
rows=[]
for source in paths:
    relative=source.relative_to(ROOT)
    target=backup/relative
    target.parent.mkdir(parents=True,exist_ok=True)
    raw=source.read_bytes()
    with target.open('xb') as f: f.write(raw)
    assert sha(source)==sha(target)
    rows.append({'path':relative.as_posix(),'sha256':sha(target),'bytes':len(raw)})
with sqlite3.connect((study/'study.sqlite').as_uri()+'?mode=ro',uri=True) as con:
    counts={name:con.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in
            ('quotes','attempts','forecasts','publication','consumption','entries','outcomes','exclusions')}
    assert all(n==0 for n in counts.values()),counts
    integrity=con.execute('PRAGMA integrity_check').fetchone()[0]
assert integrity=='ok'
result={'preserved_utc':datetime.now(timezone.utc).isoformat(),'stopped_initial_study_counts':counts,
        'database_integrity':integrity,'files':rows,'historical_forecast_imports':0,
        'reason':'Two Windows heartbeat replace sharing failures caused worker exits; new operational revision will use a distinct registration and ledger.'}
with (OUT/'INITIAL_STUDY_PRESERVATION.json').open('x',encoding='utf-8') as f:json.dump(result,f,indent=2)
print(json.dumps({'preserved_files':len(rows),'counts':counts,'integrity':integrity}))
