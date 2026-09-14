"""Prepare an exclusive source-bound opening price-control registration; never activate or open DBs.

Preparation time is not activation. At deployment, call the new worker's
activate_fresh_study() only after source freeze; each empty ledger samples its
actual activation clock then. Existing study files are never imported.
"""
from copy import deepcopy
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import re
import time

import oanda_pair_local_forecast_study_v3 as worker
from oanda_causal_forecast_ledger_pair_v2 import SCHEMA
from oanda_fixed_forecast_evaluation_pair_v2 import PROTOCOL_SCHEMA

ROOT=Path(__file__).resolve().parent
METADATA_REGISTRY=ROOT/'config/pair_local_forecast_study_v2_20260907.json'
METADATA_REGISTRY_SHA256='f7dc675925a765c0bb369282895f2839326476a05eadb7f5df6e0f27a10ed505'


def make_registry(*,scope='prospective',selected_pairs=None,metadata_path=METADATA_REGISTRY,
                  metadata_sha256=METADATA_REGISTRY_SHA256,clock=time.time):
    """Build new contracts from explicit frozen pair/pip metadata, never prices."""
    if not isinstance(scope,str) or re.fullmatch(r'[a-z0-9][a-z0-9_]{0,63}',scope) is None:
        raise ValueError('bounded_registration_scope_required')
    metadata_path=worker.plain_c_path(metadata_path)
    raw=worker.bounded_bytes(metadata_path,1024*1024)
    if hashlib.sha256(raw).hexdigest()!=metadata_sha256:raise ValueError('pair_metadata_registry_hash_mismatch')
    metadata=json.loads(raw)
    if metadata.get('schema_version')!='pair_local_forecast_registry_v2_20260907' or metadata.get('registry_id')!='pair_local_forecast_study_v2_20260907':
        raise ValueError('frozen_pair_metadata_registry_required')
    pairs=metadata.get('pairs')
    if not isinstance(pairs,dict) or not 1<=len(pairs)<=68:raise ValueError('bounded_pair_metadata_required')
    selected=set(pairs) if selected_pairs is None else set(selected_pairs)
    if not selected or not selected<=set(pairs):raise ValueError('selected_pairs_missing_from_frozen_metadata')
    bindings={name:hashlib.sha256(worker.bounded_bytes(ROOT/name,2*1024*1024)).hexdigest()
              for name in sorted(worker.REQUIRED_SOURCE_BINDINGS)}
    dependencies={'python':platform.python_version(),**{name:importlib.metadata.version(name) for name in ('numpy','scikit-learn')}}
    model='sha256:'+bindings['oanda_pair_local_models_v2.py']
    features='sha256:'+bindings['oanda_causal_forecast_inputs_pair_v2.py']
    prepared=worker.number(clock());registered={}
    if prepared<=0:raise ValueError('positive_preparation_clock_required')
    for pair in sorted(selected):
        if not isinstance(pair,str) or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',pair):raise ValueError('pair_identity')
        pip=pairs[pair]['pip_size'];families={}
        for family in sorted(worker.FAMILIES):
            identity=f'causal_pair_family_v2_20260907.{pair}.{family}.{worker.COHORT_SCOPE}.{scope}'
            common={'family':family,'instrument':pair,'pip_size':pip,'input_timeframe':'M1','horizon_sec':3600,
                'cohorts':{family:identity},'model_version':model,'feature_version':features,'quote_max_age_sec':60,
                'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60,'maximum_input_age_sec':900}
            protocol={**deepcopy(common),'schema_version':PROTOCOL_SCHEMA,'contract_id':identity+'.evaluation',
                'proof_eligible':False,'account_eligible':False,'collection_enabled':False,
                'historical_start_utc':worker.utc(prepared),'historical_end_utc':worker.utc(prepared+366*86400),
                'baselines':['fair_coin','zero_move','no_trade','rolling_class_rate'],
                'rolling_lookback':100,'rolling_min_labels':20,'extra_cost_stress_bps':[0.0,.5,1.0]}
            contract={**deepcopy(common),'schema_version':SCHEMA,'contract_id':identity,
                'research_only':True,**{flag:False for flag in worker.INERT_FLAGS},'evaluation_protocol':protocol,
                'source_bindings':bindings,'dependency_versions':dependencies,
                'numeric_model_source_sha256':bindings['oanda_pair_local_models_v2.py'],
                'cadence_sec':900,'maximum_build_sec':120,'retry_minimum_sec':30,
                'capture_policy':'Actual per-pair price timestamps and exact H1 endpoints in the same session; unchanged v2 price-family readiness. No synthetic bars, missing-row padding or prior forecast imports.',
                'prediction_scope':'Separate opening price-only control cohort reusing unchanged v2 state-space and Ridge families. Uncalibrated prospective research; no authorization, order, promotion or account authority.'}
            worker.validate_contract(contract)
            families[family]={'contract':contract,'contract_sha256':worker.digest(contract)}
        registered[pair]={'pip_size':pip,'families':families}
    return {'schema_version':worker.REGISTRY_SCHEMA,'registry_id':'pair_local_forecast_study_v3_20260913',
        'collection_enabled':True,'research_only':True,**{flag:False for flag in worker.INERT_FLAGS},
        'source_bindings':bindings,'dependency_versions':dependencies,'pairs':registered,
        'created_epoch':prepared,'preparation_clock_is_activation':False,
        'pair_metadata_source':{'path':str(metadata_path),'sha256':metadata_sha256},
        'activation_policy':'Preparation creates no database. Deployment must explicitly activate a wholly new study root with the then-current ledger clocks. The evaluator export uses actual ledger activation, never this preparation clock.',
        'purpose':'Separately registered price-only controls using fair capture/fit scheduling and unchanged v2 numerical formulas. Old study contracts, forecasts, outcomes and exclusions are retained unchanged.'}


def prepare_registry(output,**kwargs):
    output=worker.plain_c_path(output)
    if output.exists():raise ValueError('refuse_existing_registration')
    registry=make_registry(**kwargs);raw=worker.encoded(registry)
    if len(raw)>1024*1024:raise ValueError('registry_size_limit')
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('xb') as handle:handle.write(raw)
    verified=worker.load_registry(output)
    if worker.digest(verified)!=worker.digest(registry):raise ValueError('prepared_registration_changed')
    return {'status':'prepared_not_activated','path':str(output),'sha256':hashlib.sha256(raw).hexdigest(),
            'bytes':len(raw),'pairs':len(registry['pairs']),'created_epoch':registry['created_epoch'],
            'database_opened':False,'worker_started':False,'historical_rows_imported':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--scope',default='prospective')
    parser.add_argument('--pairs',nargs='+')
    args=parser.parse_args()
    print(json.dumps(prepare_registry(args.output,scope=args.scope,selected_pairs=args.pairs),indent=2))


if __name__=='__main__':main()
