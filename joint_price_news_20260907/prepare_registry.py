"""Build a source-bound, explicitly inert v2 registry; never activate anything."""
from copy import deepcopy
from pathlib import Path
import argparse
import importlib.metadata
import json
import platform
import sys
import time

ROOT=Path(__file__).resolve().parents[1]/'trad'
sys.path.insert(0,str(ROOT))
import oanda_joint_price_news_forecast_study_v1 as worker
from oanda_causal_forecast_ledger_joint_news_v1 import SCHEMA
from oanda_fixed_forecast_evaluation_joint_news_v1 import PROTOCOL_SCHEMA


def make_registry(suffix,selected=None):
    old=json.loads((ROOT/'config/pair_local_forecast_study_v1_20260907.json').read_bytes())
    bindings={name:worker.hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in sorted(worker.REQUIRED_SOURCE_BINDINGS)}
    deps={'python':platform.python_version(),**{p:importlib.metadata.version(p) for p in ('numpy','scikit-learn')}}
    model='sha256:'+bindings['oanda_joint_price_news_models_v1.py'];features='sha256:'+bindings['oanda_causal_forecast_inputs_joint_news_v1.py']
    pairs={}
    for pair,item in sorted(old['pairs'].items()):
        if selected and pair not in selected:continue
        pip=item['pip_size'];families={}
        for family in sorted(worker.FAMILIES):
            identity=f'joint_price_news_v1_20260907.{pair}.{family}.{suffix}'
            cohorts={family:identity}
            common={'family':family,'instrument':pair,'pip_size':pip,'input_timeframe':'M1','horizon_sec':3600,
                'cohorts':cohorts,'model_version':model,'feature_version':features,'quote_max_age_sec':60,
                'maximum_entry_delay_sec':60,'maximum_target_quote_delay_sec':60,'maximum_input_age_sec':900,'maximum_news_age_sec':300}
            protocol={**deepcopy(common),'schema_version':PROTOCOL_SCHEMA,'contract_id':identity+'.evaluation',
                'proof_eligible':False,'account_eligible':False,'collection_enabled':False,
                'historical_start_utc':'2026-09-07T00:00:00+00:00','historical_end_utc':'2027-09-07T00:00:00+00:00',
                'baselines':['fair_coin','zero_move','no_trade','rolling_class_rate'],
                'rolling_lookback':100,'rolling_min_labels':20,'extra_cost_stress_bps':[0.0,.5,1.0]}
            contract={**deepcopy(common),'schema_version':SCHEMA,'contract_id':identity,
                'research_only':True,**{flag:False for flag in worker.INERT_FLAGS},'evaluation_protocol':protocol,
                'source_bindings':bindings,'dependency_versions':deps,'numeric_model_source_sha256':bindings['oanda_joint_price_news_models_v1.py'],
                'cadence_sec':900,'maximum_build_sec':120,'retry_minimum_sec':30,
                'capture_policy':'Actual UTC price timestamps and exact H1 labels with committed point-in-time news visibility. Current guarded news is independently consumed; no synthetic bars or renewed reaction windows.',
                'prediction_scope':'Joint price and news Ridge with matched price-only comparator and neutral-news ablation. Limited historical training; uncalibrated prospective research.'}
            worker.validate_contract(contract)
            families[family]={'contract':contract,'contract_sha256':worker.digest(contract)}
        pairs[pair]={'pip_size':pip,'families':families}
    return {'schema_version':worker.REGISTRY_SCHEMA,'registry_id':'joint_price_news_study_v1_20260907',
        'collection_enabled':True,'research_only':True,**{flag:False for flag in worker.INERT_FLAGS},
        'source_bindings':bindings,'dependency_versions':deps,'pairs':pairs,
        'created_epoch':time.time(),'purpose':'Prospective joint price/news successor. Historical records train the model only; new publications and outcomes are independently retained.'}


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--suffix',required=True);parser.add_argument('--pairs',nargs='*');args=parser.parse_args()
    if args.output.exists():raise SystemExit('Refuse to overwrite prepared registration')
    registry=make_registry(args.suffix,args.pairs);worker.atomic_json(args.output,registry)
    verified=worker.load_registry(args.output)
    print(json.dumps({'path':str(args.output),'sha256':worker.digest(verified),'bytes':args.output.stat().st_size,
        'pairs':len(verified['pairs']),'families':sum(len(p['families']) for p in verified['pairs'].values())}))
