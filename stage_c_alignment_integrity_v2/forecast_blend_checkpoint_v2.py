"""Exact retained-record blend replay from an authenticated capsule."""
import argparse,hashlib,json,os,stat,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import forecast_blend_operator_v2 as op
from portable_checkpoint_v2 import _plain_destination,safe_member,MANIFEST,encoded
from publication import sha256_file
SCHEMA='forex_forecast_blend_checkpoint.v1'
RECIPE='FORECAST_BLEND_OPERATOR_RECIPE_REVIEWED_V2.json'
ALLOWLIST=tuple(sorted(set(op.SOURCES)|{'forecast_blend_checkpoint_v2.py','freeze_forecast_blend_recipe_v2.py','portable_checkpoint_v2.py',RECIPE,'RESIDUAL_CALIBRATION_OPERATOR_RECIPE.json','FORECAST_BLEND_CONTRACT_V2.md','test_forecast_blend_v2.py'}))
LINEAGE={'joint':{'run_identity':'bd7b33a2a66090418de21f3ea7317d8aa07edf1ae6b0ccc391a6c64c7be304bf'},'technical':{'run_identity':'3c51adb6cb8467ed15b1bf604c03c917bc0cab7d91180a3674336ccf6765f9d6'}}
def read(p):return json.loads(p.read_text(encoding='utf-8'))
def inspect_capsule(package,digest):
    # This explicit larger schema contains259MB of original JSON records. The
    # unchanged32MiB fixture checkpoint reader remains appropriate to its scope.
    if package.stat().st_size>128*1024*1024 or sha256_file(package)!=digest:raise ValueError('bounded_pinned_record_capsule_required')
    with zipfile.ZipFile(package) as z:
        infos=z.infolist();names=[x.filename for x in infos]
        if len(infos)>256 or len(set(names))!=len(names) or sum(x.file_size for x in infos)>384*1024*1024:raise ValueError('bounded_unique_capsule_inventory_required')
        for info in infos:
            safe_member(info.filename)
            if info.file_size>8*1024*1024 or info.is_dir() or stat.S_ISLNK(info.external_attr>>16) or info.flag_bits&1:raise ValueError('plain_bounded_capsule_file_required')
        if MANIFEST not in names or z.getinfo(MANIFEST).file_size>1048576:raise ValueError('bounded_capsule_manifest_required')
        manifest=json.loads(z.read(MANIFEST))
        if manifest['schema_version']!=SCHEMA or manifest['source_allowlist']!=list(ALLOWLIST):raise ValueError('capsule_schema_source_allowlist_mismatch')
        rows=manifest['members'];expected={x['path']:x for x in rows}
        if len(expected)!=len(rows) or set(names)!=set(expected)|{MANIFEST}:raise ValueError('exact_capsule_members_required')
        data={}
        for n,row in expected.items():
            raw=z.read(n)
            if len(raw)!=row['bytes'] or hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('capsule_member_bytes_changed')
            data[n]=raw
        recipe=json.loads(data['source/'+RECIPE])
        allowed={'source/'+n for n in ALLOWLIST}|{'EXPECTED_REPLAY.json','ORIGINAL_OPERATOR_RECEIPT.json','LINEAGE_CHECKPOINTS.json','CAPSULE_SCOPE.json'}
        for alias in ('joint','technical'):
            allowed.update('capsule/'+alias+'/'+n for n in set(recipe['dependencies'][alias]['payloads'])|{'RUN_IDENTITY.json','COMPLETION_MANIFEST.json'})
        if set(data)!=allowed:raise ValueError('declared_original_capsule_paths_required')
        data[MANIFEST]=z.read(MANIFEST);return data
def invoke(source,paths,runs,action):
    recipe=source/RECIPE;cmd=[sys.executable,'-I','-B',str(source/'forecast_blend_operator_v2.py'),action,'--recipe',str(recipe),'--recipe-sha256',op.sha(recipe),'--paths',str(paths),'--runs-dir',str(runs)]
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=600)
    if p.returncode:raise ValueError('blend_checkpoint_operator_failed:'+p.stdout+p.stderr)
    receipt=json.loads(p.stdout)
    if receipt['status']!='completed_verified':raise ValueError('blend_checkpoint_incomplete')
    return receipt,{x['path']:x['sha256'] for x in read(Path(receipt['run_path'])/'COMPLETION_MANIFEST.json')['payloads'] if x['path']!='resource_receipts.json'}
def export(package,paths,runs):
    receipt,expected=invoke(ROOT,paths,runs,'verify');r=read(ROOT/RECIPE);values={'source/'+n:(ROOT/n).read_bytes() for n in ALLOWLIST};inputs=read(paths);input_count=0
    for alias,value in inputs.items():
        root=Path(value)
        for n in sorted(set(r['dependencies'][alias]['payloads'])|{'RUN_IDENTITY.json','COMPLETION_MANIFEST.json'}):values['capsule/'+alias+'/'+n]=(root/n).read_bytes();input_count+=1
    values.update({'EXPECTED_REPLAY.json':encoded(expected),'ORIGINAL_OPERATOR_RECEIPT.json':encoded(receipt),'LINEAGE_CHECKPOINTS.json':encoded({'required_for':'preserved original forecast and label lineage; no base model reconstruction occurs','predecessors':LINEAGE}),'CAPSULE_SCOPE.json':encoded({'input_files':input_count,'joint_scientific_and_timing_payloads':117,'technical_payloads':70,'model_weights_included':False,'raw_data_archive_included':False,'original_records_not_refitted':True,'blend_weights_fixed':'1/2'})})
    m={'schema_version':SCHEMA,'source_allowlist':list(ALLOWLIST),'members':[{'path':n,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()} for n,b in sorted(values.items())]}
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for n,b in sorted(values.items()):z.writestr(n,b)
        z.writestr(MANIFEST,encoded(m))
    digest=op.sha(package);inspect_capsule(package,digest)
    return {'status':'VERIFIED_EXPORT','sha256':digest,'scientific_payloads':len(expected),'capsule_files':input_count,'input_tier':'preserved_original_forecast_and_label_capsule','base_models_loaded':0,'original_base_suites_rerun':False,'required_companions':{}}
def restore(package,digest,destination,run_tests=False):
    data=inspect_capsule(package,digest);_plain_destination(destination);destination.mkdir(parents=True,exist_ok=True)
    for n,b in data.items():
        p=destination.joinpath(*safe_member(n).parts);p.parent.mkdir(parents=True,exist_ok=True)
        with p.open('xb') as f:f.write(b)
    paths={k:str(destination/'capsule'/k) for k in ('joint','technical')};pf=destination/'PATHS.json';pf.write_text(json.dumps(paths,indent=2)+'\n',encoding='utf-8')
    receipt,actual=invoke(destination/'source',pf,destination/'runs','run')
    if actual!=read(destination/'EXPECTED_REPLAY.json'):raise ValueError('blend_relocated_payload_mismatch')
    if run_tests:
        p=subprocess.run([sys.executable,'-I','-B','-m','pytest','-q','-p','no:cacheprovider','--noconftest',str(destination/'source/test_forecast_blend_v2.py')],capture_output=True,text=True,timeout=900)
        (destination/'tests.stdout').write_text(p.stdout+p.stderr,encoding='utf-8')
        if p.returncode:raise ValueError('blend_relocated_tests_failed:'+p.stdout+p.stderr)
    result={'status':'VERIFIED','checkpoint_sha256':digest,'scientific_payloads_identical':len(actual),'relocated_tests_passed':run_tests,'input_tier':'preserved_original_forecast_and_label_capsule','original_capsule_files':191,'original_base_suites_rerun':False,'base_models_fitted':0,'base_models_loaded':0,'operator':receipt,'independent_review':False}
    (destination/'RESTORE_RECEIPT.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');return result
if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='action',required=True);e=s.add_parser('export')
    for n in ('package','paths','runs-dir'):e.add_argument('--'+n,type=Path,required=True)
    r=s.add_parser('restore')
    for n in ('package','destination'):r.add_argument('--'+n,type=Path,required=True)
    r.add_argument('--sha256',required=True);r.add_argument('--run-tests',action='store_true');a=p.parse_args()
    print(json.dumps(export(a.package,a.paths,a.runs_dir) if a.action=='export' else restore(a.package,a.sha256,a.destination,a.run_tests),sort_keys=True))
