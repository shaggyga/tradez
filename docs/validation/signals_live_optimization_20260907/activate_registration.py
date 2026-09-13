"""Retain predecessor evidence, then activate the separately frozen inert study."""
import hashlib,json,sqlite3,sys,time
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
from oanda_causal_forecast_study_gap_v2 import load_contract,atomic_json
from oanda_causal_forecast_ledger_gap_v2 import CausalForecastLedger,digest
old_config=ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json'
old=json.loads(old_config.read_bytes())
unchanged={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in old['source_bindings']}
assert unchanged==old['source_bindings']
new=load_contract(ROOT/'config/causal_forecast_study_gap_v2_20260907.json')
assert new['predecessor_contract_sha256']==digest(old)
prior=ROOT/'data/oanda_training_manager/causal_forecast_study_v1_io_r2'
backup=OUT/'retired_study'
backup.mkdir(exist_ok=False)
with sqlite3.connect((prior/'study.sqlite').as_uri()+'?mode=ro',uri=True) as source:
    source.execute('PRAGMA query_only=ON')
    with sqlite3.connect(backup/'study.sqlite') as target: source.backup(target)
with sqlite3.connect((backup/'study.sqlite').as_uri()+'?mode=ro',uri=True) as database:
    assert database.execute('PRAGMA integrity_check').fetchone()[0]=='ok'
    counts={table:database.execute('SELECT COUNT(*) FROM '+table).fetchone()[0] for table in
        ('quotes','attempts','diagnostics','inputs','forecasts','publication','consumption','entries','outcomes','exclusions')}
for name,path in [('contract.json',old_config),('heartbeat.json',prior/'heartbeat.json'),('scorecard.json',prior/'scorecard.json')]:
    if path.exists():
        with (backup/name).open('xb') as handle: handle.write(path.read_bytes())
directory=ROOT/'data/oanda_training_manager/causal_forecast_study_gap_v2'
directory.mkdir(exist_ok=False)
ledger=CausalForecastLedger(directory/'study.sqlite',new,activate=True)
try:
    assert not any(ledger.counts().values())
    activation={'activated_epoch':ledger.activated_epoch,'contract_sha256':ledger.contract_hash,
        'counts_at_activation':ledger.counts(),'can_place_orders':False,'can_promote':False}
finally: ledger.close()
pointer={'schema_version':'causal_study_current_pointer_v1','selected_study':'gap_v2','contract_sha256':digest(new)}
atomic_json(ROOT/'config/causal_forecast_study_current.json',pointer)
receipt={'observed_epoch':time.time(),'new_activation':activation,
    'predecessor_contract_sha256':digest(old),'predecessor_counts':counts,
    'predecessor_snapshot_sha256':hashlib.sha256((backup/'study.sqlite').read_bytes()).hexdigest(),
    'predecessor_config_file_sha256':hashlib.sha256(old_config.read_bytes()).hexdigest(),
    'predecessor_sources_unchanged':unchanged,'pointer':pointer,
    'backup_scope':'Consistent SQLite backup retained locally, outside source archive. Original DB, config and registered sources retained.'}
with (OUT/'ACTIVATION_RECEIPT.json').open('x',encoding='utf8') as handle: json.dump(receipt,handle,indent=2)
print(json.dumps(receipt))
