"""Validate the registered study and preserve its stopped files before resuming."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import sys

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent/'trad'
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT))
from oanda_causal_forecast_study import load_contract
from oanda_causal_forecast_ledger import digest

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
config = ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json'
contract = load_contract(config)
study = ROOT/'data/oanda_training_manager/causal_forecast_study_v1_io_r2'
before = OUT/'before_study_verified'
before.mkdir(exist_ok=False)
preserved = []
for source in sorted(study.iterdir()):
    if not source.is_file(): continue
    assert source.stat().st_size < 10_000_000, source.name
    original = sha(source)
    shutil.copyfile(source, before/source.name)
    assert sha(source) == sha(before/source.name) == original
    preserved.append({'name':source.name,'sha256':original,'bytes':source.stat().st_size})
connection = sqlite3.connect((before/'study.sqlite').as_uri()+'?mode=ro', uri=True)
connection.execute('PRAGMA query_only=ON')
integrity = connection.execute('PRAGMA quick_check').fetchall()
assert integrity == [('ok',)], integrity
tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
activation = connection.execute('SELECT epoch,contract_sha FROM activation WHERE id=1').fetchone()
assert activation and activation[1] == digest(contract)
connection.close()
heartbeat = json.loads((before/'heartbeat.json').read_bytes())
assert heartbeat['contract_id'] == contract['contract_id']
assert heartbeat['contract_sha256'] == digest(contract)
assert heartbeat['activated_epoch'] == activation[0]
result = {'schema_version':'forex_market_open_preflight_v1',
    'observed_utc':datetime.now(timezone.utc).isoformat(), 'status':'passed',
    'authorization':'User requested readiness for market open after the stopped audit; resume the existing collection-only session.',
    'scope':'Collection and the existing registered causal study, with execution still disabled.',
    'contract_id':contract['contract_id'],'registered_contract_payload_sha256':digest(contract),
    'contract_file_sha256':sha(config),
    'source_bindings_unchanged':contract['source_bindings'],'dependency_versions_verified':contract['dependency_versions'],
    'preserved_stopped_study_files':preserved,'copied_study_quick_check':'ok','copied_study_tables':tables,
    'saved_study_counts':heartbeat['counts'],'registered_activation_epoch_unchanged':heartbeat['activated_epoch'],
    'config_changed':False,'study_reregistered':False,'runtime_started_by_preflight':False,
    'market_open_utc':'2026-09-06T21:05:00+00:00','market_open_new_york':'2026-09-06T17:05:00-04:00',
    'market_hours_sources':['https://www.oanda.com/us-en/trading/hours-of-operation/',
        'https://www.oanda.com/us-en/trading/holiday-trading-hours/'],
    'market_hours_scope':'OANDA US ordinary FX hours on September 6; TRY pairs have separate hours.',
    'can_place_orders':False,'new_offline_models_activated':False}
(OUT/'preflight.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({key:result[key] for key in ('status','contract_id','registered_contract_payload_sha256','saved_study_counts','can_place_orders')}))
