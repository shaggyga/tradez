"""Freeze a new future-only research contract before separate activation."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import sys

OUT=Path(__file__).resolve().parent
ROOT=OUT.parent/'trad'
sys.dont_write_bytecode=True
sys.path[:0]=[str(ROOT),str(ROOT.parent)]
from oanda_causal_forecast_inputs import dependency_versions, NUMERICAL_SOURCE_SHA256, PAIRS
from oanda_causal_forecast_ledger import validate_contract, digest, encoded, FAMILIES, SCHEMA
from oanda_causal_forecast_study import REQUIRED_SOURCE_BINDINGS

bindings={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in sorted(REQUIRED_SOURCE_BINDINGS)}
assert bindings['oanda_proof_shadow_predictors.py']==NUMERICAL_SOURCE_SHA256
deps=dependency_versions()
model_version='sha256:'+digest({'sources':bindings,'dependencies':deps,'training_inputs':list(PAIRS)})
feature_version='sha256:'+bindings['oanda_causal_forecast_inputs.py']
cohorts={family:'causal_four_family_v1_20260906.'+family+'.'+model_version[-16:] for family in FAMILIES}
now=datetime.now(timezone.utc)
protocol={
    'schema_version':'fixed_forecast_evaluation_protocol_v1',
    'contract_id':'causal_four_family_future_evaluation_v1_20260906',
    'collection_enabled':False,'account_eligible':False,'proof_eligible':False,
    'instrument':'EUR_USD','input_timeframe':'M1','horizon_sec':3600,
    'historical_start_utc':now.isoformat(),'historical_end_utc':(now+timedelta(days=365)).isoformat(),
    'window_semantics':'Export replaces these placeholders with actual registered activation and consistent snapshot cutoff.',
    'cohorts':cohorts,'model_version':model_version,'feature_version':feature_version,
    'baselines':['fair_coin','zero_move','no_trade','rolling_class_rate'],
    'rolling_lookback':100,'rolling_min_labels':20,
    'quote_max_age_sec':60,'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60,
    'extra_cost_stress_bps':[0.0,0.5,1.0],
    'target_rule':'Original reference market epoch plus3600, never shifted by fitting/publication/entry.',
    'entry_rule':'First observed valid quote with BOTH market and successor availability strictly after independent forecast/receipt consumption.',
    'missingness':'Retain attempts, abstentions, missing publication/entries/targets and all four families; no return-based selection.',
    'graduation':'None. Overlapping research outcomes and caller-observed clocks are not independent evidence or order authorization.'}
contract={
    'schema_version':SCHEMA,'contract_id':'causal_four_family_future_collection_v1_20260906',
    'prepared_utc':now.isoformat(),'collection_enabled':True,'research_only':True,
    'can_place_orders':False,'can_authorize':False,'can_promote':False,
    'account_eligible':False,'proof_eligible':False,'historical_rows_imported':False,
    'instrument':'EUR_USD','input_timeframe':'M1','horizon_sec':3600,
    'cohorts':cohorts,'model_version':model_version,'feature_version':feature_version,
    'source_bindings':bindings,'numeric_model_source_sha256':NUMERICAL_SOURCE_SHA256,
    'dependency_versions':deps,'input_pairs':list(PAIRS),'cadence_sec':900,
    'maximum_build_sec':120,'quote_max_age_sec':60,'maximum_entry_delay_sec':60,
    'maximum_target_quote_delay_sec':60,'evaluation_protocol':protocol,
    'input_policy':{'maximum_common_bars':512,'minimum_common_contiguous_bars':335,
        'maximum_common_bar_age_sec':900,'alignment':'actual shared UTC minute starts',
        'availability':'after stable local capture; never an inferred historical first-known time',
        'learning_availability':'conservative upper bound sampled after all four model computations',
        'gap_policy':'Use the contiguous common suffix; wait for sufficient bars after a weekend/daily interruption.'},
    'activation_policy':'Actual post-contract-commit clock in a separate append-only activation receipt; prepared_utc does not activate.',
    'model_identity_scope':'Source, fixed parameters, exact captured inputs and dependency versions; no claim that fitted weights are stored.',
    'execution_scope':'No broker API, signal-feed write, lifecycle/promotion or order capability; no activation of sourceV9/rankV8.'}
validate_contract(contract)
target=ROOT/'config/causal_forecast_study_v1_20260906.json'
with target.open('xb') as f:f.write(encoded(contract)+b'\n')
with (OUT/'frozen_contract_sha256.json').open('x',encoding='utf-8') as f:
    json.dump({'path':str(target),'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
        'registered_contract_payload_sha256':digest(contract),'activation_executed':False},f,indent=2)
print(json.dumps({'status':'prepared_not_activated','contract_sha256':digest(contract),'dependencies':deps}))
