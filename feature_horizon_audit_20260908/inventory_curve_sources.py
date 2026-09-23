"""Read-only archive membership and actual C/D source inventory; no extraction/import."""
from pathlib import Path
import json,hashlib,zipfile,time,subprocess
from datetime import datetime,timezone

OUT=Path(__file__).parent;C=OUT.parent/'trad';D=Path('D:/forex/trad');V=Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()
def identity(p,hash_small=True):
    if not p.is_file():return {'path':str(p),'exists':False}
    s=p.stat();return {'path':str(p),'exists':True,'bytes':s.st_size,'mtime_epoch':s.st_mtime,
      'sha256':sha(p) if hash_small and s.st_size<=20*1024*1024 else None}
pointer_path=V/'source/WORKTREE_SOURCE_LATEST.json';pointer=json.loads(pointer_path.read_text(encoding='utf-8'))
manifest_path=pointer_path.parent/pointer['manifest'];manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
keys={'created_utc','kind','base_git_commit','archive','archive_sha256','manifest','manifest_sha256','content_sha256','source_file_count','files_scanned','included_file_count'}
record={'schema':'feature_horizon_source_archive_inventory_v1_20260908','observed_utc':datetime.now(timezone.utc).isoformat(),
 'latest_pointer':{k:v for k,v in pointer.items() if k in keys},'latest_pointer_file':identity(pointer_path),
 'latest_manifest_keys':sorted(manifest),'latest_manifest_file':identity(manifest_path),
 'latest_manifest_matches_pointer':sha(manifest_path)==pointer['manifest_sha256'],
 'latest_archive_matches_pointer':sha(pointer_path.parent/pointer['archive'])==pointer['archive_sha256']}
source_names=[p.relative_to(D).as_posix() for p in (D/'fresh_m1_intrahour/src').glob('*.py')]
record['missing_c_package']={name:{'c':identity(C/name),'d':identity(D/name)} for name in sorted(source_names)}
record['d_package_python_count']=len(source_names)
patterns=['fresh_m1_intrahour/src/','unified_forecast','ma_feature_grid','multihorizon_currency_panel_v3','live_model_feature_snapshot']
def archive_inventory(p):
    item=identity(p,False)
    try:
        with zipfile.ZipFile(p) as z:
            names=z.infolist();matches=[{'path':i.filename,'bytes':i.file_size,'crc32':f'{i.CRC:08x}'} for i in names if any(s in i.filename.replace('\\','/').lower() for s in patterns)]
            item.update(member_count=len(names),matches=matches,source_package_members=sum('fresh_m1_intrahour/src/' in i.filename.replace('\\','/') for i in names))
    except (OSError,zipfile.BadZipFile) as exc:item['error']=type(exc).__name__
    return item
vault_archives=sorted((V/'source').glob('*.zip'))
local_archives=sorted((C/'artifacts').rglob('*.zip'))
quarantine=sorted((OUT.parent/'vault_quarantine_20260809').rglob('*.zip'))
record['vault_source_archives']=[archive_inventory(p) for p in vault_archives]
record['local_archives']=[archive_inventory(p) for p in local_archives+quarantine]
record['vault_other_archives']=[archive_inventory(p) for p in V.glob('*.zip')]
record['manifest_relevant_entries']={}
for key,value in manifest.items():
    if isinstance(value,list):
        selected=[row for row in value if any(s in json.dumps(row).lower() for s in patterns)]
        if selected:record['manifest_relevant_entries'][key]=selected
record['targeted_current_and_d_artifacts']=[]
for parent in (C,D):
    for folder in ('fresh_m1_intrahour/models','data/oanda_training_manager/models/ma_feature_grid','data/oanda_training_manager/reports/ma_feature_grid'):
        q=parent/folder
        record['targeted_current_and_d_artifacts'].append({'directory':str(q),'exists':q.is_dir(),
            'files':[identity(p) for p in sorted(q.glob('*')) if p.is_file()]})
for path in (C/'data/oanda_training_manager/state/live_model_feature_snapshot_v1.json',D/'data/oanda_training_manager/state/live_model_feature_snapshot_v1.json'):
    meta=identity(path)
    if path.is_file():
        v=json.loads(path.read_text(encoding='utf-8'));meta['snapshot']={k:v.get(k) for k in ('schema_version','generated_epoch','generated_utc','snapshot_id','feature_count','instrument_count')}
        meta['snapshot']['instruments']=len(v.get('instruments') or {})
    record.setdefault('feature_snapshots',[]).append(meta)
record['scope']='File identity and ZIP central-directory membership only; no extracted or restored files, no project imports, no joblib/pickle deserialization, no model execution. Archive membership alone does not establish complete runtime recreation or validation.'
dest=OUT/'CURVE_SOURCE_ARCHIVE_INVENTORY_20260908.json';dest.write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'path':str(dest),'sha256':sha(dest),'d_python_count':len(source_names),
 'latest_archive_verified':record['latest_archive_matches_pointer'],'latest_manifest_verified':record['latest_manifest_matches_pointer'],
 'vault_source':[(a['path'],a['source_package_members'],len(a['matches'])) for a in record['vault_source_archives']],
 'local_matching_archives':[(a['path'],a.get('source_package_members'),len(a.get('matches',[]))) for a in record['local_archives'] if a.get('matches')],
 'feature_snapshots':record['feature_snapshots']},indent=2))
