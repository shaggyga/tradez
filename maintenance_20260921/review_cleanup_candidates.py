"""Review exact C-side cleanup candidates, without removal or moves."""
import ctypes,hashlib,json,os,re,stat,time
from collections import defaultdict
from datetime import datetime,timezone
from pathlib import Path

OUT=Path(__file__).resolve().parent
inventory=json.loads((OUT/'CLEANUP_METADATA_INVENTORY.json').read_text())
TRAD=Path(r'C:\Users\zmoor\Documents\forex\trad')
LOGROOT=TRAD/'data'/'oanda_training_manager'/'logs'
NOW=time.time();CUTOFF=NOW-72*3600

def regular(path):
    try:s=path.stat(follow_symlinks=False)
    except OSError:return None
    return s if stat.S_ISREG(s.st_mode) and not(getattr(s,'st_file_attributes',0)&0x400) else None

def stamp(s):return {'bytes':s.st_size,'mtime_ns':s.st_mtime_ns}
def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        while chunk:=f.read(4*1024*1024):h.update(chunk)
    return h.hexdigest()

bytecode=[];cache_rejections=[]
for cached in inventory['cache_candidates']:
    path=Path(cached['path'])
    try:relative=path.relative_to(TRAD)
    except ValueError:continue
    if not (relative.parts==('__pycache__',) or relative.parts==('tools','__pycache__') or (relative.parts[0]=='src' and relative.parts[-1]=='__pycache__')):continue
    for entry in os.scandir(path):
        p=Path(entry.path);s=regular(p)
        match=re.fullmatch(r'(.*?)\.(?:cpython-\d+|pypy\d*)(?:-pytest-[0-9.]+)?(?:\.opt-\d+)?\.pyc',p.name)
        source=path.parent/(match.group(1)+'.py') if match else None
        ss=regular(source) if source else None
        if s is None or ss is None:
            cache_rejections.append({'path':str(p),'reason':'not_regular_bytecode_with_regular_paired_source'});continue
        with p.open('rb') as f:header=f.read(16)
        valid=len(header)==16 and header[2:4]==b'\r\n' and int.from_bytes(header[4:8],'little') in (0,1,2,3)
        if not valid:
            cache_rejections.append({'path':str(p),'reason':'unexpected_bytecode_header'});continue
        bytecode.append({'path':str(p),**stamp(s),'modified_utc':datetime.fromtimestamp(s.st_mtime,timezone.utc).isoformat(),'source':str(source),'source_metadata':stamp(ss),'bytecode_header_hex':header.hex(),'status':'generated_cache_candidate_after_source_checkpoint','evidence':'regular bytecode header, recognized CPython/pytest cache naming, exact paired regular .py source exists; normal current source cache roots only','preconditions':['verified checkpoint includes paired source','recheck unchanged size/mtime and no reparse ancestor before removal','do not delete parent source tree or alter running processes']})

logs=[];recent=[]
for entry in os.scandir(LOGROOT):
    p=Path(entry.path)
    if not re.search(r'\.(out|err|stdout|stderr)\.log$',p.name,re.I):continue
    s=regular(p)
    if s is None:continue
    item={'path':str(p),**stamp(s),'modified_utc':datetime.fromtimestamp(s.st_mtime,timezone.utc).isoformat()}
    if s.st_mtime>CUTOFF:recent.append(item)
    elif s.st_size:logs.append(item)

# Probe only sharing/identity, never log contents. Handles are closed immediately.
kernel=ctypes.WinDLL('kernel32',use_last_error=True)
kernel.CreateFileW.argtypes=[ctypes.c_wchar_p,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_void_p,ctypes.c_ulong,ctypes.c_ulong,ctypes.c_void_p]
kernel.CreateFileW.restype=ctypes.c_void_p
kernel.GetFinalPathNameByHandleW.argtypes=[ctypes.c_void_p,ctypes.c_wchar_p,ctypes.c_ulong,ctypes.c_ulong]
kernel.CloseHandle.argtypes=[ctypes.c_void_p]
invalid=ctypes.c_void_p(-1).value
for row in logs:
    path=Path(row['path']);s=regular(path)
    row['metadata_stable_on_recheck']=bool(s and stamp(s)=={'bytes':row['bytes'],'mtime_ns':row['mtime_ns']})
    if not row['metadata_stable_on_recheck']:
        row['status']='skip_changed';continue
    handle=kernel.CreateFileW(str(path),0x80000000,0,None,3,0x00200000,None)
    if handle==invalid:
        row.update(status='skip_exclusive_open_failed',windows_error=ctypes.get_last_error());continue
    try:
        buffer=ctypes.create_unicode_buffer(32768)
        n=kernel.GetFinalPathNameByHandleW(handle,buffer,len(buffer),0)
        final=buffer.value if n else ''
        normalized=final.removeprefix('\\\\?\\')
        row['resolved_path']=normalized
        if normalized.casefold()!=str(path).casefold():row['status']='skip_resolved_path_mismatch'
        else:row['status']='lossless_archive_candidate_currently_exclusive_readable'
    finally:kernel.CloseHandle(handle)
    row['preconditions']=['checkpoint and preserve diagnostic provenance','archive with full original SHA256 and decompressed-byte SHA256 verification','repeat held-handle no-writer, stable metadata and final path checks immediately before any deletion','retain original on any failure','keep private stdout payloads local; no automatic Vault upload']

groups=defaultdict(list)
for file in inventory['archives']:
    if not file['path'].lower().endswith('.zip') or file['bytes']<1_000_000:continue
    if '\\BIGTRIAD\\' in file['path']:continue
    groups[file['bytes']].append(file)
duplicates=[];hash_bytes=0
for size,items in sorted(groups.items(),reverse=True):
    if len(items)<2:continue
    if hash_bytes+size*len(items)>500_000_000:continue
    byhash=defaultdict(list)
    for item in items:
        path=Path(item['path']);s=regular(path)
        if s is None or s.st_size!=size or getattr(s,'st_file_attributes',0)&(0x1000|0x400000):continue
        digest=sha(path);hash_bytes+=size;after=path.stat(follow_symlinks=False)
        if stamp(s)!=stamp(after):continue
        byhash[digest].append({'path':str(path),'bytes':size,'sha256':digest,'metadata_stable':True,'file_id':s.st_ino,'links':s.st_nlink})
    for digest,members in byhash.items():
        if len(members)>1:duplicates.append({'sha256':digest,'bytes_each':size,'members':members,'redundant_logical_bytes_upper_bound':size*(len(members)-1),'status':'exact_duplicate_content_reference_review_required','preservation_condition':'Keep at least one independently verified retained copy; aliases, immutable archive names, sealed tree hashes and restore pointers must remain valid. No deletion authorized by this inventory.'})

qualified=[x for x in logs if x['status']=='lossless_archive_candidate_currently_exclusive_readable']
result={'schema':'forex_cleanup_reviewed_candidates_v1','observed_utc':datetime.now(timezone.utc).isoformat(),'status':'read_only_inventory_complete','roots':[str(r) for r in [TRAD,LOGROOT]],
 'generated_bytecode_candidates':bytecode,'generated_bytecode_bytes':sum(x['bytes'] for x in bytecode),'bytecode_rejections':cache_rejections,
 'cold_stdout_age_cutoff_utc':datetime.fromtimestamp(CUTOFF,timezone.utc).isoformat(),'cold_stdout_candidates':logs,'cold_stdout_qualified_files':len(qualified),'cold_stdout_qualified_original_bytes':sum(x['bytes'] for x in qualified),'recent_stdout_excluded':recent,
 'exact_duplicate_archives':duplicates,'duplicate_hash_read_bytes':hash_bytes,
 'excluded_categories':['All databases and SQLite WAL/SHM sets','Raw candles, news, features, models, normalizers and research results','Pytest temporary fixture trees without verified preservation/recreation evidence','Sealed archive payloads including embedded caches','Current/recent or changed logs and exclusive-open failures','All junction/symlink/reparse targets including the known D junction','Codex/venv runtime installations and .git','Compressed cold-log originals and restore receipts already created on Sept15'],
 'limitations':['Logical bytes are not promised physical free-space gains.','Exclusive-read probe is point-in-time only; it is not permanent proof of no future writer.','No cache/log removal, archive creation, source deletion, process action or D read was performed.']}
(OUT/'REVIEWED_CLEANUP_CANDIDATES.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
print(json.dumps({'generated_bytecode_files':len(bytecode),'generated_bytecode_bytes':result['generated_bytecode_bytes'],'bytecode_rejections':len(cache_rejections),'cold_stdout_qualified_files':len(qualified),'cold_stdout_qualified_original_bytes':result['cold_stdout_qualified_original_bytes'],'recent_stdout_excluded':len(recent),'duplicate_groups':len(duplicates),'duplicate_read_bytes':hash_bytes,'duplicate_redundant_upper_bound':sum(x['redundant_logical_bytes_upper_bound'] for x in duplicates)},indent=2))
