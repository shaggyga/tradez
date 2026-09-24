"""Portable source checkpoint plus exact saved-input companion; never refits."""
import argparse,hashlib,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import warm_curve_operator_v2 as op
import warm_original_inputs_capsule_v2 as capsule
from portable_checkpoint_v2 import inspect_package,_plain_destination,safe_member,MANIFEST,encoded
SCHEMA='forex_warm_curve_checkpoint.v2'
INPUT_CAPSULE_SHA256='25061ff42b9dac4027cfcfb3042a3778b5db448e449428d8415c951de2b7676f'
ALLOWLIST=tuple(sorted(set(op.SOURCES)|{'warm_curve_checkpoint_v2.py','warm_original_inputs_capsule_v2.py','portable_checkpoint_v2.py','test_warm_curve_operator_v2.py','WARM_CURVE_OPERATOR_RECIPE.json','WARM_CURVE_CONTRACT_V2.md'}))
def inventory(root):
    return {r['path']:r['sha256'] for r in op.read(root/'COMPLETION_MANIFEST.json')['payloads'] if not r['path'].startswith('timing_') and r['path'] not in {'startup_resources.json','resource_receipts.json'}}
def invoke(source,paths,trad,runs,action):
    recipe=source/'WARM_CURVE_OPERATOR_RECIPE.json'
    cmd=[sys.executable,'-X','utf8','-I','-B',str(source/'warm_curve_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths),'--trad-root',str(trad),'--runs-dir',str(runs)]
    r=subprocess.run(cmd,capture_output=True,text=True,timeout=1000)
    if r.returncode:raise ValueError('warm_portable_operator_failed:'+r.stdout+r.stderr)
    result=json.loads(r.stdout)
    if result['status']!='completed_verified':raise ValueError('warm_portable_completion_required')
    return result
def export(package,paths,trad,runs):
    result=invoke(ROOT,paths,trad,runs,'verify')
    values={'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST}
    recipe=op.read(ROOT/'WARM_CURVE_OPERATOR_RECIPE.json')
    for n,digest in recipe['predecessors'].items():
        if op.sha(trad/n)!=digest:raise ValueError('warm_export_predecessor_drift')
        values['trad/'+n]=(trad/n).read_bytes()
    values['EXPECTED_SCIENTIFIC.json']=encoded(inventory(Path(result['run_path'])))
    values['COMPANION.json']=encoded({'sha256':INPUT_CAPSULE_SHA256,'scope':'exact_saved_original_inputs_no_fitting'})
    manifest={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(v),'sha256':hashlib.sha256(v).hexdigest()} for n,v in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,v in sorted(values.items()):z.writestr(n,v)
        z.writestr(MANIFEST,encoded(manifest))
    digest=op.sha(package);inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'scientific_payloads':len(inventory(Path(result['run_path']))),'input_companion_sha256':INPUT_CAPSULE_SHA256,'models_fitted':0}
def restore(package,digest,destination,original_inputs,run_tests=False):
    data=inspect_package(package,digest,expected_source_allowlist=ALLOWLIST,expected_schema=SCHEMA)
    recipe=json.loads(data['source/WARM_CURVE_OPERATOR_RECIPE.json'])
    expected={'source/'+n for n in ALLOWLIST}|{'trad/'+n for n in recipe['predecessors']}|{'COMPANION.json','EXPECTED_SCIENTIFIC.json',MANIFEST}
    if set(data)!=expected:raise ValueError('warm_checkpoint_exact_inventory_required')
    if json.loads(data['COMPANION.json'])['sha256']!=INPUT_CAPSULE_SHA256:raise ValueError('warm_companion_contract_mismatch')
    capsule.inspect(original_inputs,INPUT_CAPSULE_SHA256)
    _plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,v in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(v)
    capsule.restore(original_inputs,INPUT_CAPSULE_SHA256,destination/'original_inputs')
    result=invoke(destination/'source',destination/'original_inputs/PATHS.json',destination/'trad',destination/'runs','run')
    actual=inventory(Path(result['run_path']))
    if actual!=json.loads(data['EXPECTED_SCIENTIFIC.json']):raise ValueError('warm_portable_scientific_mismatch')
    if run_tests:
        p=subprocess.run([sys.executable,'-X','utf8','-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_warm_curve_operator_v2.py')],capture_output=True,text=True,timeout=180)
        (destination/'tests.log').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('warm_portable_tests_failed:'+p.stdout+p.stderr)
    receipt={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'timings':'separately_authenticated_bounded_not_byte_equal','relocated_tests_passed':run_tests,'operator':result,'models_fitted':0,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_bytes(encoded(receipt));return receipt
if __name__=='__main__':
    p=argparse.ArgumentParser();sub=p.add_subparsers(dest='action',required=True);e=sub.add_parser('export');r=sub.add_parser('restore')
    for n in ('package','paths','trad-root','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    for n in ('package','destination','original-inputs'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.trad_root,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.original_inputs,a.run_tests),sort_keys=True))
