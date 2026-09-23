"""Activate the source-frozen, order-incapable local-model companion."""
import hashlib,json,sys,time
from pathlib import Path
OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.path.insert(0,str(ROOT))
from oanda_causal_forecast_study_eurusd_v1 import load_contract,atomic_json
from oanda_causal_forecast_ledger_eurusd_v1 import CausalForecastLedger,digest
from oanda_causal_forecast_study_gap_v2 import load_contract as load_paired
paired=load_paired(ROOT/'config/causal_forecast_study_gap_v2_20260907.json')
new=load_contract(ROOT/'config/causal_forecast_study_eurusd_v1_20260907.json')
assert new['companion_contract_sha256']==digest(paired)
folder=ROOT/'data/oanda_training_manager/causal_forecast_study_eurusd_v1'
folder.mkdir(exist_ok=False)
ledger=CausalForecastLedger(folder/'study.sqlite',new,activate=True)
try:
    assert not any(ledger.counts().values())
    receipt={'activated_epoch':ledger.activated_epoch,'contract_sha256':ledger.contract_hash,
        'counts_at_activation':ledger.counts(),'can_place_orders':False,'can_promote':False,
        'paired_study_continues_unchanged':True,'paired_contract_sha256':digest(paired)}
finally:ledger.close()
pointer_path=ROOT/'config/causal_forecast_study_current.json'
with (OUT/'POINTER_BEFORE_EURUSD.json').open('xb') as f:f.write(pointer_path.read_bytes())
atomic_json(pointer_path,{'schema_version':'causal_study_current_pointer_v1','selected_study':'eurusd_v1','contract_sha256':digest(new)})
with (OUT/'EURUSD_ACTIVATION_RECEIPT.json').open('x',encoding='utf8') as f:json.dump(receipt,f,indent=2)
print(json.dumps(receipt))
