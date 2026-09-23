"""C-only bounded cleanup inventory; does not delete, move, hydrate, or execute project code."""
import hashlib,json,os,re,stat,time
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path

OUT=Path(__file__).resolve().parent
ROOTS=[Path(r'C:\Users\zmoor\Documents\forex'),Path(r'C:\Users\zmoor\OneDrive\thevault')]
START=time.monotonic();LIMIT=180;ENTRY_LIMIT=500000
entries=0;errors=[];skipped=[];dirs=[];archives=[];caches=[]
KNOWN_JUNCTION=Path(r'C:\Users\zmoor\Documents\forex\cleanup_audit_20260915\junction_capability_test')

def protected_link(path,s):
    return path==KNOWN_JUNCTION or bool(getattr(s,'st_file_attributes',0)&0x400) or stat.S_ISLNK(s.st_mode) or path.is_junction()

def walk(path,depth=0):
    global entries
    if time.monotonic()-START>LIMIT or entries>=ENTRY_LIMIT:raise RuntimeError('metadata_budget_reached')
    summary={'path':str(path),'depth':depth,'bytes':0,'files':0,'directories':0,'newest_mtime_ns':0,'extensions':Counter(),'skipped_descendants':0}
    files=[]
    try:
        with os.scandir(path) as stream:children=list(stream)
    except OSError as exc:
        errors.append({'path':str(path),'error':f'{type(exc).__name__}: {exc}'});return summary
    for child in children:
        entries+=1;p=Path(child.path)
        if p==OUT:continue
        try:s=child.stat(follow_symlinks=False)
        except OSError as exc:errors.append({'path':str(p),'error':str(exc)});continue
        if protected_link(p,s):
            skipped.append({'path':str(p),'reason':'reparse_symlink_or_junction','tag':getattr(s,'st_reparse_tag',0),'attributes':getattr(s,'st_file_attributes',0)});summary['skipped_descendants']+=1;continue
        if stat.S_ISDIR(s.st_mode):
            if child.name in {'.git','.venv','venv','node_modules'}:
                skipped.append({'path':str(p),'reason':'protected_runtime_or_repository_directory'});summary['skipped_descendants']+=1;continue
            sub=walk(p,depth+1)
            summary['bytes']+=sub['bytes'];summary['files']+=sub['files'];summary['directories']+=sub['directories']+1
            summary['newest_mtime_ns']=max(summary['newest_mtime_ns'],sub['newest_mtime_ns'])
            summary['extensions'].update(sub['extensions']);summary['skipped_descendants']+=sub['skipped_descendants']
        elif stat.S_ISREG(s.st_mode):
            item={'path':str(p),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'attributes':getattr(s,'st_file_attributes',0),'links':s.st_nlink,'file_id':s.st_ino}
            files.append(item);summary['bytes']+=s.st_size;summary['files']+=1;summary['newest_mtime_ns']=max(summary['newest_mtime_ns'],s.st_mtime_ns)
            summary['extensions'][p.suffix.lower() or '<none>']+=1
            if p.suffix.lower() in {'.zip','.7z','.tgz','.gz'}:archives.append(item)
    summary['extensions']=dict(summary['extensions'])
    dirs.append(summary)
    if path.name=='__pycache__':
        unmatched=[]
        for f in files:
            name=Path(f['path']).name
            match=re.match(r'^(.*?)\.(?:cpython-\d+|pypy\d*)(?:\.opt-\d+)?\.pyc$',name)
            original=path.parent/(match.group(1)+'.py') if match else None
            try:
                source_stat=original.stat(follow_symlinks=False) if original else None
                source_present=bool(source_stat and stat.S_ISREG(source_stat.st_mode) and not protected_link(original,source_stat))
            except OSError:source_present=False
            if not source_present:unmatched.append(f['path'])
        caches.append(dict(summary,kind='python_bytecode',qualification='source_matched_generated_bytecode' if files and not unmatched and not summary['directories'] and not summary['skipped_descendants'] else 'review_required',unmatched_files=unmatched[:10]))
    elif path.name=='.pytest_cache':
        marker=path/'CACHEDIR.TAG';signature=False
        try:
            if any(f['path']==str(marker) for f in files):
                with marker.open('rb') as f:signature=f.read(128).startswith(b'Signature: 8a477f597d28d172789f06886806bc55')
        except OSError:pass
        caches.append(dict(summary,kind='pytest_cache',qualification='generated_cache_marker_verified' if signature else 'review_required',cache_signature_verified=signature))
    return summary

roots=[];status='completed'
try:
    for root in ROOTS:
        assert root.drive.upper()=='C:'
        if protected_link(root,root.stat(follow_symlinks=False)):
            skipped.append({'path':str(root),'reason':'root_reparse_skipped'});continue
        roots.append(walk(root))
except RuntimeError as exc:status=str(exc)
result={'schema':'forex_cleanup_inventory_v1','observed_utc':datetime.now(timezone.utc).isoformat(),'status':status,'elapsed_seconds':time.monotonic()-START,'metadata_entries':entries,'roots':roots,'directories':dirs,'archives':archives,'cache_candidates':caches,'skipped':skipped,'errors':errors,'limitations':['Logical file sizes, not allocated disk bytes; skipped links/environments excluded.','Candidate classification is not deletion authorization or proof that audit fixtures are disposable.','No content read except bounded standard pytest CACHEDIR.TAG marker.','No D access or junction traversal.']}
(OUT/'CLEANUP_METADATA_INVENTORY.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
print(json.dumps({'status':status,'elapsed_seconds':result['elapsed_seconds'],'entries':entries,'roots':roots,'archives':len(archives),'caches':len(caches),'skipped':len(skipped),'errors':len(errors)},indent=2))
