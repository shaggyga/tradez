"""Prepare a new source-bound research registration; never activate or start it."""
import copy,hashlib,json,sys
from datetime import datetime,timezone,timedelta
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent/'trad'
sys.path.insert(0,str(ROOT))
import oanda_causal_forecast_study_gap_v2 as worker
import oanda_causal_forecast_inputs_gap_v2 as inputs
import oanda_gap_aware_four_family_models as models
from oanda_causal_forecast_ledger_gap_v2 import digest,encoded,validate_contract
old=json.loads((ROOT/'config/causal_forecast_study_v1_io_r2_20260906.json').read_bytes())
new=copy.deepcopy(old)
sources={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in sorted(worker.REQUIRED_SOURCE_BINDINGS)}
identity=digest({'numerical_source':inputs.NUMERICAL_SOURCE_SHA256,'parameters':dict(models.PARAMETERS),
    'input_source':sources['oanda_causal_forecast_inputs_gap_v2.py'],'dependency_versions':inputs.dependency_versions()})
cohorts={family:'causal_four_family_v1_20260906.'+family+'.gap_v2.'+identity[:16] for family in inputs.FAMILIES}
now=datetime.now(timezone.utc)
new.update(contract_id='causal_four_family_future_collection_gap_v2_20260907',
    prepared_utc=now.isoformat(),predecessor_contract_sha256=digest(old),source_bindings=sources,
    dependency_versions=inputs.dependency_versions(),cohorts=cohorts,
    numeric_model_source_sha256=inputs.NUMERICAL_SOURCE_SHA256,
    model_version='sha256:'+identity,feature_version='sha256:'+sources['oanda_causal_forecast_inputs_gap_v2.py'],
    operational_revision='Separately registered timestamp-addressed training across valid retained segments, with 61 current common closes. No missing prices synthesized and no elapsed-time labels compressed. Exact decimal observed-price scoring; original H1 target and all publication/entry clocks preserved.',
    input_policy={'alignment':'actual shared UTC minute starts','minimum_current_common_bars':61,
        'maximum_source_rows_per_pair':1024,'maximum_common_bar_age_sec':900,
        'gap_policy':'Current features require 61 consecutive common closes. Supervised samples require 121 real consecutive prices with target exactly 3600 seconds after sample. Retain valid older training segments, including pre-weekend; no imputation.',
        'availability':'after stable local capture; never an inferred historical first-known time',
        'learning_availability':'conservative upper bound sampled after all four model computations'},
    scoring_version='exact_decimal_bidask_scoring_v1_20260906',
    historical_rows_imported=False,
    historical_training_scope='Retained past candles may train a newly issued future forecast. No historical forecasts or historical first-known clocks are imported.')
new['evaluation_protocol'].update(contract_id='causal_four_family_future_evaluation_gap_v2_20260907',
    historical_start_utc=now.isoformat(),historical_end_utc=(now+timedelta(days=365)).isoformat(),
    cohorts=cohorts,model_version=new['model_version'],feature_version=new['feature_version'])
validate_contract(new)
destination=ROOT/'config/causal_forecast_study_gap_v2_20260907.json'
with destination.open('xb') as handle: handle.write(encoded(new)+b'\n')
assert worker.load_contract(destination)==new
receipt={'prepared_epoch':now.timestamp(),'activated':False,'contract_sha256':digest(new),
    'config_file_sha256':hashlib.sha256(destination.read_bytes()).hexdigest(),
    'source_bindings':sources,'cohorts':cohorts,'orders':0}
out=Path(__file__).resolve().parent/'REGISTRATION_PREPARED.json'
out.write_text(json.dumps(receipt,indent=2),encoding='utf8')
print(json.dumps(receipt))
