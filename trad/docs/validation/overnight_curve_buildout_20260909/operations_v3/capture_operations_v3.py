"""Source-bound operational wrapper for the explicitly retired two joint workers.

Reuses accepted V2 reads and limits unchanged. No orders, GET, runtime writes,
forecast scoring, or claim that historical 17-worker observations were different.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
ROOT=Path(r'C:/Users/zmoor/Documents/forex/trad')
V2=BASE.parent/'operations_v2/capture_operations_v2.py'
V2_SHA='2e378d5a83d6b8f68446c2fb762ca959b95b48430de1b59d01c651b81e3ba128'
HEALTH_SHA='e4c6349dc32a45abc7c692e17611bde3a1871e2be5b153a9d8a5693f4c4fa32f'
SUPERVISOR_SHA='28c226f56105331cb7e828bc54a02359f814f98b55838ca12f36cf19e733473e'
MANIFEST_SHA='38e7c9be1e8c6d8a2b14bb36f54526751bdf9cd8ddd762fed8840bd927a74faf'
EXPECTED=frozenset({'account_snapshot','live_dashboard','local_news_sentiment','official_release_fast_lane',
    'official_release_fast_mapper','all68_m1_forward_archive','practice_007_quote_stream','clock_integrity_monitor',
    'pair_local_forecast_study_v1','pair_local_forecast_study_v2','source_governance_news_fast_lane',
    'project_integrity_audit','storage_headroom_guard','local_news_sentiment_repair_v1','joint_price_news_study_v3'})
RETIRED=frozenset({'joint_price_news_study_v1','joint_price_news_study_v2'})


def need(ok,reason):
    if not ok:raise ValueError(reason)


def sha(raw):return hashlib.sha256(raw).hexdigest()


def load_base():
    need(sha(V2.read_bytes())==V2_SHA,'accepted_v2_source_changed')
    spec=importlib.util.spec_from_file_location('private_accepted_operations_v2',V2)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def own_bindings(base):
    paths={V2:V2_SHA,ROOT/'oanda_project_runtime_health.py':HEALTH_SHA,
        ROOT/'oanda_always_on_supervisor.ps1':SUPERVISOR_SHA,
        ROOT/'config/joint_study_runtime_retirement_v1_20260909.json':MANIFEST_SHA}
    for path,want in paths.items():need(base.raw_read(path)[1]['sha256']==want,'retirement_operational_source_changed')
    paths[Path(__file__).resolve()]=base.raw_read(Path(__file__))[1]['sha256']
    return {str(path):value for path,value in paths.items()}


def supervised_workers(supervisor,os_status):
    expected=supervisor.get('expected_workers')
    need(type(expected) is list and len(expected)==15 and set(expected)==EXPECTED,'main15_expected_inventory')
    pids={r['pid'] for r in os_status['processes']};rows={}
    for name in expected:
        original=supervisor.get('workers',{}).get(name)
        if original is None:
            rows[name]=dict(status='not_reported',running=None,pids=[],all_reported_pids_present_in_os=False)
        else:
            rows[name]={k:original[k] for k in ('running','pids','supervisor_check_ok','explicitly_inactive') if k in original}
            rows[name].update(status='supervisor_reported',all_reported_pids_present_in_os=bool(original['pids']) and all(pid in pids for pid in original['pids']))
    return rows


def collect_with_base(base):
    # A fresh private module instance is used per invocation. No shared live
    # module or accepted source bytes are patched; restore even on failure.
    originals=(base.HEALTH_SHA,base.supervised_workers,base.supervisor_observation)
    observed={}
    def supervision(ops):
        value=originals[2](ops);observed['supervisor']=value;return value
    base.HEALTH_SHA=HEALTH_SHA;base.supervised_workers=supervised_workers;base.supervisor_observation=supervision
    try:result=base.collect()
    finally:base.HEALTH_SHA,base.supervised_workers,base.supervisor_observation=originals
    workers=result.pop('main17_workers');count=result.pop('main17_os_present_count')
    result['main15_workers']=workers;result['main15_os_present_count']=count
    result['schema_version']='overnight_operations_observation_v3_20260909'
    original=observed.get('supervisor',{})
    result['retired_joint_workers']={name:original.get('workers',{}).get(name,{'status':'not_reported','explicitly_inactive':None}) for name in sorted(RETIRED)}
    result['retirement_scope']='Only joint study V1/V2 removed from expected active set. Retained rows/failures and old17-worker reports are preserved; missing does not establish inactive status.'
    return result


def collect():
    base=load_base();before=own_bindings(base);result=collect_with_base(base);after=own_bindings(base)
    need(before==after,'operations_v3_identity_changed')
    result['operations_v3_source_bindings']=before
    result['accepted_operations_v2_sha256']=V2_SHA
    return result


def write_output(path,result,base):
    path=Path(path).absolute();need(path.parent==BASE and path.suffix=='.json' and not path.exists(),'fresh_external_output_required');base.safe(BASE)
    raw=json.dumps(result,sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
    with path.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    need(path.read_bytes()==raw,'output_readback');return {'path':str(path),'sha256':sha(raw)}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    need(a.output.absolute().parent==BASE and not a.output.exists(),'fresh_external_output_required')
    value=collect();receipt=write_output(a.output,value,load_base())
    print(json.dumps({**receipt,'main15_os_present_count':value['main15_os_present_count'],
        'joint_current_pairs':value['independent_joint_ledger_observer']['current_forecast_pairs']}))
