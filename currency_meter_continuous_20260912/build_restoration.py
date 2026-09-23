"""Create an inactive source/test restoration, without copying live runtime state."""
from datetime import datetime,timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import shutil
import zipfile

ROOT=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()


def main():
    output=ROOT/'restoration_001';output.mkdir(exist_ok=False)
    manifest=json.loads((ROOT/'RELEASE_MANIFEST.json').read_text(encoding='utf-8'))
    for name,expected in manifest['files'].items():
        assert sha(ROOT/name)==expected
        target=output/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(ROOT/name,target)
    for name in ('README.md','RELEASE_MANIFEST.json','STORAGE_DESIGN_RECEIPT.json','build_restoration.py'):
        shutil.copy2(ROOT/name,output/name)
    for folder in ('review','feed_audit'):
        for path in (ROOT/folder).rglob('*'):
            if path.is_file() and path.suffix in {'.md','.json','.xml'}:
                target=output/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
    (output/'runtime').mkdir();(output/'runtime/STOP').write_text('Inactive restoration. No upstream or broker services are started.\n',encoding='utf-8')
    (output/'ENVIRONMENT.json').write_text(json.dumps({'python':'3.12.10','platform':'Windows','runtime_dependencies':'Python standard library','pytest':importlib.metadata.version('pytest')},indent=2),encoding='utf-8')
    archive=ROOT/'currency_meter_continuous_20260912.zip'
    with zipfile.ZipFile(archive,'x',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for path in sorted(output.rglob('*')):
            if path.is_file():z.write(path,path.relative_to(output).as_posix())
    full={p.relative_to(output).as_posix():sha(p) for p in sorted(output.rglob('*')) if p.is_file()}
    receipt={'created_utc':datetime.now(timezone.utc).isoformat(),'archive':str(archive),'archive_sha256':sha(archive),'archive_bytes':archive.stat().st_size,
             'scope':'inactive_source_tests_and_evidence_metadata','release_manifest_sha256':sha(ROOT/'RELEASE_MANIFEST.json'),
             'files':full,'live_database_copied':False,'stop_marker_present':True}
    with (ROOT/'review/RESTORATION_BUILD.json').open('x',encoding='utf-8') as out:json.dump(receipt,out,indent=2,sort_keys=True)
    print(json.dumps({k:receipt[k] for k in ('archive','archive_sha256','archive_bytes','stop_marker_present')}))


if __name__=='__main__':main()
