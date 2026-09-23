"""Lossless cold-stdout cleanup with a Windows writer-excluding source handle.

Archives stay local; scripts/metadata can be published to the vault. No SQLite,
model, feature, forecast, JSONL or live configuration files are candidates.
"""
from __future__ import annotations
import argparse, ctypes, gzip, hashlib, json, os, shutil, sys, time
from ctypes import wintypes as W
from datetime import datetime, timezone
from pathlib import Path
import msvcrt

ROOT=Path(r'C:\Users\zmoor\Documents\forex\trad')
LOG_ROOT=ROOT/'data/oanda_training_manager/logs'
AUDIT_ROOT=ROOT.parent/'maintenance_20260921'
ARCHIVE_ROOT=ROOT.parent/'cold_log_archive_20260921'
SELECTION_SHA='d5af6bdb1b0426c4f18b2fec65424bbad1df1ebcedc1a6e976698c12e2d79d77'
CUTOFF_NS=1789709471912360960
BLOCK=4*1024**2
MIN_FREE=32*1024**3
K=ctypes.WinDLL('kernel32',use_last_error=True)
K.CreateFileW.argtypes=[W.LPCWSTR,W.DWORD,W.DWORD,W.LPVOID,W.DWORD,W.DWORD,W.HANDLE]
K.CreateFileW.restype=W.HANDLE
K.GetFinalPathNameByHandleW.argtypes=[W.HANDLE,W.LPWSTR,W.DWORD,W.DWORD]
K.GetFinalPathNameByHandleW.restype=W.DWORD
K.GetFileInformationByHandle.argtypes=[W.HANDLE,W.LPVOID];K.GetFileInformationByHandle.restype=W.BOOL
K.SetFileInformationByHandle.argtypes=[W.HANDLE,ctypes.c_int,W.LPVOID,W.DWORD];K.SetFileInformationByHandle.restype=W.BOOL
K.CloseHandle.argtypes=[W.HANDLE];K.CloseHandle.restype=W.BOOL

class Info(ctypes.Structure):
    _fields_=[('attrs',W.DWORD),('creation',W.FILETIME),('access',W.FILETIME),('write',W.FILETIME),
              ('volume',W.DWORD),('size_hi',W.DWORD),('size_lo',W.DWORD),('links',W.DWORD),('id_hi',W.DWORD),('id_lo',W.DWORD)]
class Disposition(ctypes.Structure):
    _fields_=[('DeleteFile',ctypes.c_ubyte)]

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(BLOCK),b''):h.update(b)
    return h.hexdigest()

def save(path,obj):
    path=Path(path);temporary=path.with_name(path.name+'.writing')
    with temporary.open('wb') as f:
        f.write((json.dumps(obj,indent=2,sort_keys=True,allow_nan=False)+'\n').encode());f.flush();os.fsync(f.fileno())
    os.replace(temporary,path)

def bounded_file(path,root):
    path,root=Path(path).absolute(),Path(root).resolve(strict=True)
    if not path.is_relative_to(root) or path==root:raise ValueError('exact_log_root_boundary')
    for item in (path,*path.parents):
        if item==root.parent:break
        if item.lstat().st_file_attributes&0x400:raise ValueError('reparse_points_forbidden')
    result=path.resolve(strict=True)
    if not result.is_relative_to(root) or not result.is_file():raise ValueError('regular_file_inside_log_root_required')
    if not result.name.endswith(('.out.log','.err.log','.stdout.log','.stderr.log','.pyc')):raise ValueError('reviewed_stdout_stderr_or_bytecode_only')
    return result

def handle_info(handle):
    info=Info()
    if not K.GetFileInformationByHandle(handle,ctypes.byref(info)):raise ctypes.WinError(ctypes.get_last_error())
    buf=ctypes.create_unicode_buffer(32768)
    n=K.GetFinalPathNameByHandleW(handle,buf,len(buf),0)
    if not n or n>=len(buf):raise ValueError('final_handle_path_unavailable')
    final=buf.value
    if final.startswith('\\\\?\\'):final=final[4:]
    return {'path':str(Path(final).absolute()),'bytes':(info.size_hi<<32)|info.size_lo,
            'mtime_ns':(((info.write.dwHighDateTime<<32)|info.write.dwLowDateTime)-116444736000000000)*100,
            'volume':info.volume,'file_id':(info.id_hi<<32)|info.id_lo,'links':info.links,'attributes':info.attrs}

def verify_gzip(path,expected_sha,expected_bytes):
    h=hashlib.sha256();n=0
    with gzip.open(path,'rb') as f:
        for b in iter(lambda:f.read(BLOCK),b''):h.update(b);n+=len(b)
    if n!=expected_bytes or h.hexdigest()!=expected_sha:raise ValueError('complete_decompressed_hash_and_size_must_match')
    return {'restored_bytes':n,'restored_sha256':h.hexdigest(),'gzip_crc_and_stream_end_verified':True}

def archive_file(source,archive,receipt,expected,*,source_root=LOG_ROOT,minimum_free=MIN_FREE,verify=verify_gzip):
    source=bounded_file(source,source_root);archive,receipt=Path(archive),Path(receipt)
    if expected['mtime_ns']>=CUTOFF_NS:raise ValueError('cold_date_cutoff_required')
    if archive.exists() or receipt.exists():raise ValueError('new_archive_and_receipt_required')
    archive.parent.mkdir(parents=True,exist_ok=True);receipt.parent.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(archive.parent).free<minimum_free:raise ValueError('drive_reserve')
    # Request DELETE now, but share only READ. Any existing/new writer or
    # deleter blocks this operation; never retry with weaker sharing flags.
    handle=K.CreateFileW('\\\\?\\'+str(source),0x80000000|0x00010000,1,None,3,0x08000000|0x00200000,None)
    if handle==ctypes.c_void_p(-1).value:raise ctypes.WinError(ctypes.get_last_error())
    stream=None;marked=False
    try:
        before=handle_info(handle)
        if Path(before['path'])!=source or before['bytes']!=expected['bytes'] or before['mtime_ns']!=expected['mtime_ns']:
            raise ValueError('held_original_identity_mismatch')
        if before['links']!=1 or before['attributes']&(0x400|0x10|0x1):raise ValueError('single_link_regular_nonreadonly_source_required')
        fd=msvcrt.open_osfhandle(handle,os.O_RDONLY|os.O_BINARY)
        stream=os.fdopen(fd,'rb',buffering=BLOCK) # owns handle until finally
        h=hashlib.sha256();n=0
        with archive.open('xb') as out:
            with gzip.GzipFile(filename='',mode='wb',fileobj=out,compresslevel=6,mtime=0) as packed:
                for b in iter(lambda:stream.read(BLOCK),b''):
                    h.update(b);n+=len(b);packed.write(b)
                    if shutil.disk_usage(archive.parent).free<minimum_free:raise ValueError('drive_reserve_during_archive')
            out.flush();os.fsync(out.fileno())
        if n!=before['bytes'] or handle_info(handle)!=before:raise ValueError('held_source_changed_during_read')
        verified=verify(archive,h.hexdigest(),n)
        result={'schema':'forex_reviewed_cleanup_archive_item_v1','kind':expected['kind'],'status':'verified_pending_original_removal',
                'source':before,'original_sha256':h.hexdigest(),'archive_path':str(archive.resolve()),
                'archive_bytes':archive.stat().st_size,'archive_sha256':sha(archive),
                'verification':verified,'writer_exclusion':'GENERIC_READ|DELETE; FILE_SHARE_READ only; same handle retained through verified deletion',
                'verified_utc':datetime.now(timezone.utc).isoformat(),'original_removed':False}
        save(receipt,result) # durable restoration proof before deletion
        if handle_info(handle)!=before:raise ValueError('held_source_changed_before_disposition')
        flag=Disposition(1)
        if not K.SetFileInformationByHandle(handle,4,ctypes.byref(flag),ctypes.sizeof(flag)):
            raise ctypes.WinError(ctypes.get_last_error())
        marked=True
    finally:
        if stream is not None:stream.close()
        else:K.CloseHandle(handle)
    # Other shared readers may delay deletion. Do not claim absence early.
    try:
        source.stat();absent=False
    except FileNotFoundError:absent=True
    except OSError:absent=False
    if marked and absent:
        result.update(status='archived_original_removed',original_removed=True,
                      logical_bytes_recovered=n-result['archive_bytes'],completed_utc=datetime.now(timezone.utc).isoformat())
    else:result.update(status='verified_delete_pending_other_readers',logical_bytes_recovered=0)
    save(receipt,result);return result

def restore_copy(receipt_path,destination):
    r=json.loads(Path(receipt_path).read_bytes());archive=Path(r['archive_path']);destination=Path(destination)
    if destination.exists():raise ValueError('restore_never_overwrites_existing_file')
    if sha(archive)!=r['archive_sha256']:raise ValueError('archive_changed')
    verify_gzip(archive,r['original_sha256'],r['source']['bytes'])
    destination.parent.mkdir(parents=True,exist_ok=True)
    with destination.open('xb') as out,gzip.open(archive,'rb') as src:
        shutil.copyfileobj(src,out,BLOCK);out.flush();os.fsync(out.fileno())
    if sha(destination)!=r['original_sha256']:raise ValueError('restored_copy_hash_mismatch')
    return {'path':str(destination),'bytes':destination.stat().st_size,'sha256':sha(destination)}

if __name__=='__main__':
    raise SystemExit('Use run_reviewed_cleanup.py with its exact plan and checkpoint gate.')
