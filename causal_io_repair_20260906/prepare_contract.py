"""Prepare a distinct operational revision; never rewrite an activated contract."""
from pathlib import Path
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sys

OUT=Path(__file__).resolve().parent; ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True; sys.path.insert(0,str(ROOT))
from oanda_causal_forecast_ledger import validate_contract, digest, encoded
from oanda_causal_forecast_study import REQUIRED_SOURCE_BINDINGS, load_contract
prior_path=ROOT/'config/causal_forecast_study_v1_20260906.json'
prior=json.loads(prior_path.read_bytes())
contract=json.loads(prior_path.read_bytes())
now=datetime.now(timezone.utc)
bindings={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in sorted(REQUIRED_SOURCE_BINDINGS)}
assert [name for name in bindings if bindings[name]!=prior['source_bindings'][name]]==['oanda_causal_forecast_study.py']
model_version='sha256:'+digest({'sources':bindings,'dependencies':prior['dependency_versions'],'training_inputs':prior['input_pairs']})
cohorts={family:'causal_four_family_v1_20260906.'+family+'.'+model_version[-16:] for family in prior['cohorts']}
assert set(cohorts.values()).isdisjoint(prior['cohorts'].values())
contract.update(contract_id='causal_four_family_future_collection_v1_io_r2_20260906',
                prepared_utc=now.isoformat(), source_bindings=bindings,
                model_version=model_version, cohorts=cohorts,
                predecessor_contract_sha256=digest(prior),
                operational_revision='Bounded Windows atomic replacement retry; heartbeat publication errors do not terminate quote collection. Initial zero-forecast registration retired intact; no numerical model or timing-rule changes.')
contract['evaluation_protocol'].update(
    contract_id='causal_four_family_future_evaluation_v1_io_r2_20260906',
    model_version=model_version, cohorts=cohorts,
    historical_start_utc=now.isoformat(),historical_end_utc=(now+timedelta(days=365)).isoformat())
validate_contract(contract)
target=ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json'
with target.open('xb') as f:f.write(encoded(contract)+b'\n')
assert load_contract(target)==contract
receipt={'prepared_utc':now.isoformat(),'path':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
         'registered_contract_payload_sha256':digest(contract),'predecessor_contract_payload_sha256':digest(prior),
         'activation_executed':False,'numeric_input_ledger_scorer_source_changed':False}
with (OUT/'frozen_contract_sha256.json').open('x',encoding='utf-8') as f:json.dump(receipt,f,indent=2)
print(json.dumps(receipt))
