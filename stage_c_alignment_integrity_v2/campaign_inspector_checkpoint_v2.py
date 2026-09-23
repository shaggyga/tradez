"""Compact report checkpoint with hash-pinned model and policy reconstruction chains."""
import hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import matched_remaining_checkpoint_v2 as model
import matched_policy_checkpoint_v2 as policy
import campaign_inspector_operator_v2 as op
from accounting_checkpoint_v2 import dependency_lock
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_campaign_inspector_checkpoint.v1'
ALLOWLIST=tuple(sorted(set(model.ALLOWLIST)|set(policy.ALLOWLIST)|set(op.SOURCES)|{'campaign_inspector_checkpoint_v2.py','CAMPAIGN_INSPECTOR_CONTRACT_V2.md','CAMPAIGN_INSPECTOR_OPERATOR_RECIPE.json','test_campaign_inspector_v2.py'}))
EXTERNAL={
 'model':{'sha256':'2fca12f0c7d6ab5085718203c1c4b8e1598db748479a749d7deca51f2a13cbb8','vault_path':'MATCHED_REMAINING_20260922_041223/checkpoint/forex_matched_remaining.zip'},
 'policy':{'sha256':'daed749436fb6794fbfbf3c932f64aacc8c009a915a045adb37fcc7f64363f37','vault_path':'MATCHED_POLICY_20260922_044505/checkpoint/forex_matched_policy.zip'}}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def export(package,paths,runs):
    recipe=ROOT/'CAMPAIGN_INSPECTOR_OPERATOR_RECIPE.json'
    r=op.operate('verify',recipe,op.sha(recipe),paths,runs)
    if r['status']!='completed_verified':raise ValueError('inspector_export_requires_verified_run')
    expected={n:op.sha(Path(r['run_path'])/n) for n in op.REQUIRED}
    payloads={**{'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST},'EXTERNAL_CHECKPOINTS.json':encoded(EXTERNAL),'EXPECTED_REPORT.json':encoded(expected),'DEPENDENCIES.json':encoded(dependency_lock())}
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(payloads.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(payloads.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(manifest))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'report_payloads':len(expected),'required_external_checkpoints':EXTERNAL}
def restore(package,digest,destination,model_package,policy_package,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    if json.loads(data['EXTERNAL_CHECKPOINTS.json'])!=EXTERNAL:raise ValueError('external_checkpoint_contract_mismatch')
    if json.loads(data['DEPENDENCIES.json'])!=dependency_lock():raise ValueError('inspector_restore_environment_mismatch')
    for name,p in [('model',model_package),('policy',policy_package)]:
        if op.sha(p)!=EXTERNAL[name]['sha256']:raise ValueError('external_checkpoint_hash_mismatch:'+name)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    mr=model.restore(model_package,EXTERNAL['model']['sha256'],destination/'model',False)
    pr=policy.restore(policy_package,EXTERNAL['policy']['sha256'],destination/'policy',False)
    paths={k:str(destination/'model'/v) for k,v in {'technical':'technical_runs/causal-technical-inputs','matched':'matched_runs/matched-development-campaign','remaining':'runs/matched-remaining-native-inputs'}.items()}
    for method in ('ridge','recovered_hgb'):
        for scenario in ('candle_zero_slippage_financing','candle_one_bp_slippage_rollover'):
            for engine in ('reference','optimized'):
                paths['policy/'+method+'/'+scenario+'/'+engine]=str(destination/'policy/runs'/f'{method}-{scenario}-{engine}')
    paths_file=destination/'PATHS.json';paths_file.write_text(json.dumps(paths,indent=2),encoding='utf-8')
    source=destination/'source';recipe=source/'CAMPAIGN_INSPECTOR_OPERATOR_RECIPE.json';runs=destination/'runs'
    config={'python':sys.executable,'recipe_sha256':op.sha(recipe),'paths':str(paths_file),'runs_dir':str(runs)}
    (destination/'FOREX_INSPECTOR_LOCAL.json').write_text(json.dumps(config,indent=2),encoding='utf-8')
    cmd=[sys.executable,'-I','-B',str(source/'campaign_inspector_operator_v2.py'),'run','--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths_file),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
    (destination/'operator.stdout').write_text(p.stdout,encoding='utf-8');(destination/'operator.stderr').write_text(p.stderr,encoding='utf-8')
    if p.returncode:raise ValueError('relocated_inspector_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified':raise ValueError('relocated_inspector_incomplete')
    actual={n:op.sha(Path(receipt['run_path'])/n) for n in op.REQUIRED}
    if actual!=read(destination/'EXPECTED_REPORT.json'):raise ValueError('relocated_report_payload_mismatch')
    if run_tests:
        env=dict(os.environ,FOREX_INSPECTOR_PATHS=str(paths_file),FOREX_INSPECTOR_RUNS=str(runs))
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(source/'test_campaign_inspector_v2.py')],capture_output=True,text=True,timeout=240,env=env)
        (destination/'tests.stdout').write_text(p.stdout,encoding='utf-8');(destination/'tests.stderr').write_text(p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('relocated_inspector_tests_failed:'+p.stdout+p.stderr)
    r={'status':'VERIFIED','checkpoint_sha256':digest,'report_payloads':len(actual),'relocated_tests_passed':run_tests,'model_reconstruction':mr,'policy_reconstruction':pr,'inspector':receipt}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(r,indent=2),encoding='utf-8');return r
