"""Read-only verification and frozen observation of the new research ledger."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sqlite3
import sys

OUT=Path(__file__).resolve().parent; ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True;sys.path[:0]=[str(ROOT),str(ROOT.parent)]
from oanda_causal_forecast_study import load_contract
from oanda_causal_forecast_ledger import digest

def sha(raw):return hashlib.sha256(raw).hexdigest()
study=ROOT/'data/oanda_training_manager/causal_forecast_study_v1_io_r2'
config=load_contract(ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json')
copies={}
for name in ('heartbeat.json','scorecard.json'):
    raw=(study/name).read_bytes()
    with (OUT/('study_'+name)).open('xb') as f:f.write(raw)
    copies[name]={'sha256':sha(raw),'bytes':len(raw)}
heartbeat=json.loads((OUT/'study_heartbeat.json').read_bytes())
with sqlite3.connect((study/'study.sqlite').as_uri()+'?mode=ro',uri=True) as db:
    db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
    integrity=db.execute('PRAGMA integrity_check').fetchone()[0]
    registration=db.execute('SELECT sha,payload FROM contract WHERE id=1').fetchone()
    activation=db.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
    counts={t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in
        ('quotes','attempts','forecasts','publication','consumption','entries','outcomes','exclusions')}
assert integrity=='ok' and registration[0]==digest(config)==activation[1]==heartbeat['contract_sha256']
assert json.loads(registration[1])==config
assert heartbeat['errors']==0 and heartbeat['can_place_orders'] is False
receipt={'schema_version':'causal_study_runtime_observation_v1',
    'observed_utc':datetime.now(timezone.utc).isoformat(),'database_integrity':integrity,
    'contract_sha256':registration[0],'activation_epoch':activation[0],
    'activation_utc':datetime.fromtimestamp(activation[0],timezone.utc).isoformat(),
    'counts':counts,'phase':heartbeat['phase'],'errors':heartbeat['errors'],
    'research_only':True,'can_place_orders':False,'can_promote':False,
    'artifacts':copies,'historical_forecasts_imported':0,
    'clock_valid_outcomes_observed':0,'predictive_improvement_demonstrated':False}
with (OUT/'runtime_verification.json').open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(json.dumps(receipt))
