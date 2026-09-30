"""Pack/restore authenticated retained inference bytes; never loads or fits models."""
import argparse
import hashlib
import json
from pathlib import Path,PurePosixPath
import zipfile

REGISTRY='trad/config/retained_forecast_connection_20260930.json'
PREFIX='trad/data/retained_connection_20260930/capsule/'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def records(registry):
    result=list(registry['normalizers'].values())
    for entry in registry['connections']:
        result.extend(entry['models'])
        if entry.get('fit_metadata'):result.append(entry['fit_metadata'])
    return {r['path']:r['sha256'] for r in result}
def contained(root,name):
    p=PurePosixPath(name)
    if (p.is_absolute() or '..' in p.parts or '\\' in name or ':' in name
        or not (name==REGISTRY or name.startswith(PREFIX))):raise ValueError('capsule_path_not_allowed')
    path=root.joinpath(*p.parts)
    if not path.resolve().is_relative_to(root.resolve()):raise ValueError('capsule_path_escape')
    return path
def pack(root,output):
    root=Path(root).resolve();output=Path(output)
    if output.exists():raise ValueError('new_capsule_required')
    raw=(root/REGISTRY).read_bytes();reg=json.loads(raw);payloads={REGISTRY:sha(raw),**records(reg)}
    data={}
    for name,expected in payloads.items():
        data[name]=contained(root,name).read_bytes()
        if sha(data[name])!=expected:raise ValueError('source_artifact_changed')
    manifest={'schema':'forex.retained_inference_capsule.v1','models_fitted':0,'payloads':payloads}
    with zipfile.ZipFile(output,'x',compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr('MANIFEST.json',json.dumps(manifest,sort_keys=True))
        for name,content in data.items():z.writestr(name,content)
    return {'capsule':str(output),'sha256':sha(output.read_bytes()),'bytes':output.stat().st_size,'payloads':len(payloads),'models_fitted':0}
def restore(root,capsule,expected):
    root=Path(root).resolve();capsule=Path(capsule)
    if capsule.stat().st_size>128*1024**2 or sha(capsule.read_bytes())!=expected:raise ValueError('capsule_identity')
    with zipfile.ZipFile(capsule) as z:
        infos=z.infolist()
        if len(infos)>100 or sum(i.file_size for i in infos)>256*1024**2:raise ValueError('capsule_size')
        names=[i.filename for i in infos]
        if len(names)!=len(set(names)):raise ValueError('duplicate_member')
        manifest=json.loads(z.read('MANIFEST.json'))
        if manifest.get('schema')!='forex.retained_inference_capsule.v1' or manifest.get('models_fitted')!=0:raise ValueError('capsule_schema')
        if set(names)!=set(manifest['payloads'])|{'MANIFEST.json'}:raise ValueError('capsule_inventory')
        data={name:z.read(name) for name in manifest['payloads']}
    for name,raw in data.items():
        path=contained(root,name)
        if sha(raw)!=manifest['payloads'][name]:raise ValueError('payload_identity')
        if path.exists() and path.read_bytes()!=raw:raise ValueError('existing_different_bytes:'+name)
    reg=json.loads(data[REGISTRY]);expected_records={REGISTRY:sha(data[REGISTRY]),**records(reg)}
    if expected_records!=manifest['payloads']:raise ValueError('registry_inventory')
    for name,raw in data.items():
        path=contained(root,name);path.parent.mkdir(parents=True,exist_ok=True)
        if not path.exists():
            with path.open('xb') as f:f.write(raw)
    return {'status':'restored_exact_bytes','payloads':len(data),'models_fitted':0,'models_loaded':0,
            'scope':'Artifact retrieval only; verify matching Git source, runtime and live inputs before inference.'}
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='action',required=True)
    a=sub.add_parser('pack');a.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);a.add_argument('--output',type=Path,required=True)
    a=sub.add_parser('restore');a.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);a.add_argument('--capsule',type=Path,required=True);a.add_argument('--sha256',required=True)
    a=p.parse_args();print(json.dumps(pack(a.root,a.output) if a.action=='pack' else restore(a.root,a.capsule,a.sha256),indent=2))
