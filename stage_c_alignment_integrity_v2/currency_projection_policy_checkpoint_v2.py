"""Compact portable input capsule for the 48-path projection-policy replay."""
import argparse, hashlib, json, shutil, stat, subprocess, sys, zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parent
MANIFEST='MANIFEST.json'; SCHEMA='forex_currency_projection_policy_checkpoint.v1'
enc=lambda x:(json.dumps(x,sort_keys=True,separators=(',',':'))+'\n').encode()
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()

def safe(name):
    p=Path(name)
    if p.is_absolute() or '..' in p.parts or not p.parts:raise ValueError('checkpoint_unsafe_path')
    return p

def run_manifest(root):
    data={}
    for item in Path(root).glob('*/COMPLETION_MANIFEST.json'):
        manifest=json.loads(item.read_text()); data[item.parent.name]={x['path']:x['sha256'] for x in manifest['payloads']}
    if len(data)!=48:raise ValueError('checkpoint_exact_48_completed_runs')
    return data

def export(package,runs,native,extension,source):
    c=json.loads((ROOT/'CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json').read_text()); expected=run_manifest(runs); records=[]
    names=['RUN_IDENTITY.json','COMPLETION_MANIFEST.json']+[f'frame_{cohort}_{origin}.json' for cohort,v in c['cohorts'].items() for origin in v['origins']]
    values={}
    for name in names:values['native/'+name]=(Path(native)/name).read_bytes()
    for name in ('RUN_IDENTITY.json','COMPLETION_MANIFEST.json','market_later_monday.json','market_later_tuesday.json'):values['extension/'+name]=(Path(extension)/name).read_bytes()
    source_files=['CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json','currency_projection_policy_operator_v2.py','currency_projection_policy_fixture_v2.py','currency_projection_policy_input_v2.py','policy_runner_v2.py','historical_native_input_v2.py','currency_projection_policy_review_v2.py','currency_projection_policy_checkpoint_v2.py']
    for name in source_files:values['source/'+name]=(Path(source)/name).read_bytes()
    values['EXPECTED.json']=enc(expected); values['SCOPE.json']=enc({'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip(),'parent_native_identity':c['parent_native_identity'],'parent_extension_identity':c['parent_extension_identity'],'output_scope':'48 replay outputs, verified against manifest hashes','restore':'requires a clean Git checkout at source_commit and same locked Python environment'})
    with zipfile.ZipFile(package,'x',compression=zipfile.ZIP_DEFLATED) as z:
        for name,data in sorted(values.items()):z.writestr(name,data);records.append({'path':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()})
        z.writestr(MANIFEST,enc({'schema_version':SCHEMA,'members':records}))
    inspect(package,sha(package));return {'status':'VERIFIED_EXPORT','sha256':sha(package),'members':len(records)+1,'expected_runs':len(expected)}

def inspect(package,digest):
    if sha(package)!=digest:raise ValueError('checkpoint_pin_mismatch')
    with zipfile.ZipFile(package) as z:
        names=z.namelist(); manifest=json.loads(z.read(MANIFEST)); rows={x['path']:x for x in manifest['members']}
        if manifest['schema_version']!=SCHEMA or len(rows)!=len(manifest['members']) or set(names)!=set(rows)|{MANIFEST}:raise ValueError('checkpoint_inventory')
        for name,row in rows.items():
            safe(name); info=z.getinfo(name)
            if info.file_size!=row['bytes'] or stat.S_ISLNK(info.external_attr>>16) or hashlib.sha256(z.read(name)).hexdigest()!=row['sha256']:raise ValueError('checkpoint_member_changed')
        return manifest

def restore(package,digest,destination,source):
    manifest=inspect(package,digest); destination=Path(destination); source=Path(source)
    if destination.exists() and any(destination.iterdir()):raise ValueError('checkpoint_empty_destination_required')
    destination.mkdir(parents=True,exist_ok=True)
    with zipfile.ZipFile(package) as z:
        for row in manifest['members']:
            target=destination/safe(row['path']);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(z.read(row['path']))
    paths={'native':str(destination/'native'),'extension':str(destination/'extension'),'trad':str(source.parent/'trad')}
    (destination/'PATHS.json').write_bytes(enc(paths)); contract=source/'CURRENCY_PROJECTION_POLICY_CONTRACT_V2.json'
    command=[sys.executable,'-X','utf8','-I','-B',str(source/'currency_projection_policy_operator_v2.py'),'run','--contract',str(contract),'--contract-sha256',sha(contract),'--paths',str(destination/'PATHS.json'),'--runs-dir',str(destination/'runs')]
    subprocess.run(command,capture_output=True,text=True,timeout=7200)
    expected=read_expected(destination/'EXPECTED.json'); actual=run_manifest(destination/'runs')
    if actual!=expected:raise ValueError('checkpoint_replay_payload_mismatch')
    review=[sys.executable,'-X','utf8','-I','-B',str(source/'currency_projection_policy_review_v2.py'),'--contract',str(contract),'--runs-dir',str(destination/'runs')]
    result=subprocess.run(review,capture_output=True,text=True,timeout=600)
    if result.returncode:raise ValueError('checkpoint_replay_review_failed:'+result.stdout+result.stderr)
    return {'status':'VERIFIED','runs':len(actual),'checkpoint_sha256':digest,'review':json.loads(result.stdout)}

def read_expected(path):return json.loads(Path(path).read_bytes())

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['export','inspect','restore']);p.add_argument('--package',type=Path,required=True);p.add_argument('--sha256')
    for n in ('runs-dir','native','extension','source','destination'):p.add_argument('--'+n,type=Path)
    a=p.parse_args();print(json.dumps(inspect(a.package,a.sha256) if a.action=='inspect' else restore(a.package,a.sha256,a.destination,a.source) if a.action=='restore' else export(a.package,a.runs_dir,a.native,a.extension,a.source),sort_keys=True))
