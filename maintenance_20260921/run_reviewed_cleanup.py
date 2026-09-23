"""Pinned, checkpoint-gated cleanup. Default is read-only; --apply removes only verified originals."""
import argparse,hashlib,importlib.util,json,os,stat,time
from collections import Counter
from datetime import datetime,timezone
from pathlib import Path

HERE=Path(__file__).resolve().parent
WORKSPACE=Path(r'C:\Users\zmoor\Documents\forex')
TRAD=WORKSPACE/'trad'
LOGROOT=TRAD/'data/oanda_training_manager/logs'
ARCHIVE=WORKSPACE/'cold_log_archive_20260921'
PLAN=HERE/'EXACT_CLEANUP_EXECUTION_PLAN.json'
ENGINE=HERE/'cleanup_archive_engine.py'
PLAN_SHA='207653f85bf74e8fe927c68d0760ee74b1ab97609703e9f4467da321d8ad9f8b'
ENGINE_SHA='4eb43e788e10b513e196e6133d607fdde5ef182166d8af02e922e2e55ec6b648'


def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as f:
        while block:=f.read(4*1024*1024):value.update(block)
    return value.hexdigest()


def plain_c_path(path,*,must_exist=True):
    path=Path(path).absolute()
    if path.drive.upper()!='C:' or '..' in path.parts:raise ValueError('C_absolute_plain_path_required')
    for current in (path,*path.parents):
        try:s=current.stat(follow_symlinks=False)
        except FileNotFoundError:
            if current==path and must_exist:raise
            continue
        if getattr(s,'st_file_attributes',0)&0x400 or stat.S_ISLNK(s.st_mode) or current.is_junction():raise ValueError('reparse_symlink_junction_refused')
    if must_exist and path.resolve(strict=True)!=path:raise ValueError('resolved_path_mismatch')
    return path


def validate_record(record):
    path=Path(record['source_path'])
    if record['kind']=='cold_stdout_stderr':
        if path.parent!=LOGROOT or not path.name.endswith(('.out.log','.err.log','.stdout.log','.stderr.log')):raise ValueError('stdout_scope_mismatch')
        if Path(record['source_root'])!=LOGROOT:raise ValueError('stdout_root_mismatch')
    elif record['kind']=='generated_bytecode':
        if Path(record['source_root'])!=TRAD or not path.is_relative_to(TRAD) or path.suffix!='.pyc':raise ValueError('bytecode_scope_mismatch')
        parent=path.parent.relative_to(TRAD)
        if not (parent.parts==('__pycache__',) or parent.parts==('tools','__pycache__') or (parent.parts[0]=='src' and parent.parts[-1]=='__pycache__')):raise ValueError('bytecode_cache_root_mismatch')
    else:raise ValueError('unreviewed_kind')


def existing_receipt(engine,receipt,record):
    plain_c_path(receipt)
    saved=json.loads(receipt.read_bytes())
    if saved['source']['path']!=record['source_path'] or saved['source']['bytes']!=record['bytes'] or saved['source']['mtime_ns']!=record['mtime_ns']:raise ValueError('existing_receipt_identity_mismatch')
    archive=plain_c_path(saved['archive_path'])
    if archive.parent!=ARCHIVE/'payloads':raise ValueError('existing_archive_boundary_mismatch')
    if digest(archive)!=saved['archive_sha256']:raise ValueError('existing_archive_hash_mismatch')
    engine.verify_gzip(archive,saved['original_sha256'],saved['source']['bytes'])
    return saved


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--checkpoint-receipt',type=Path)
    parser.add_argument('--checkpoint-sha256')
    parser.add_argument('--checkpoint-status',default='passed',choices=['passed','verified','completed_verified','portable_restore_verified'])
    parser.add_argument('--offset',type=int,default=0)
    parser.add_argument('--limit',type=int,default=5191)
    args=parser.parse_args()
    plain_c_path(PLAN);plain_c_path(ENGINE)
    if digest(PLAN)!=PLAN_SHA or digest(ENGINE)!=ENGINE_SHA:raise ValueError('pinned_plan_or_engine_changed')
    plan=json.loads(PLAN.read_bytes());records=plan['records']
    if len(records)!=5191 or Counter(r['kind'] for r in records)!=Counter({'cold_stdout_stderr':5027,'generated_bytecode':164}):raise ValueError('exact_reviewed_counts_required')
    if len({r['source_path'].casefold() for r in records})!=5191 or Path(plan['archive_root'])!=ARCHIVE:raise ValueError('unique_exact_plan_paths_required')
    for record in records:validate_record(record)
    if not(0<=args.offset<5191 and 1<=args.limit<=5191):raise ValueError('bounded_slice_required')
    selected=records[args.offset:args.offset+args.limit]
    if not args.apply:
        print(json.dumps({'mode':'read_only_plan_validation','plan_sha256':PLAN_SHA,'engine_sha256':ENGINE_SHA,'selected_records':len(selected),'all_reviewed_records':5191,'all_reviewed_source_bytes':plan['source_bytes'],'archive_root':str(ARCHIVE),'checkpoint_required_before_apply':True,'source_or_archive_files_modified':False},indent=2));return
    if not args.checkpoint_receipt or not args.checkpoint_sha256:raise ValueError('successful_hash_pinned_checkpoint_receipt_required')
    checkpoint=plain_c_path(args.checkpoint_receipt)
    if digest(checkpoint)!=args.checkpoint_sha256.lower():raise ValueError('checkpoint_receipt_hash_mismatch')
    checkpoint_data=json.loads(checkpoint.read_bytes())
    if checkpoint_data.get('status')!=args.checkpoint_status:raise ValueError('checkpoint_status_not_verified')
    # This gate records the root audit's already verified restoration receipt. It
    # does not invent archive semantics from arbitrary "complete" strings.
    for path in (WORKSPACE,TRAD,LOGROOT,HERE):plain_c_path(path)
    plain_c_path(ARCHIVE,must_exist=False)
    ARCHIVE.mkdir(exist_ok=True)
    for child in ('payloads','receipts'):
        path=ARCHIVE/child;plain_c_path(path,must_exist=False);path.mkdir(exist_ok=True);plain_c_path(path)
    spec=importlib.util.spec_from_file_location('pinned_cleanup_archive_engine',ENGINE)
    engine=importlib.util.module_from_spec(spec);spec.loader.exec_module(engine)
    run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    run_path=HERE/f'CLEANUP_EXECUTION_{run_id}.json'
    run={'schema':'forex_exact_cleanup_execution_receipt_v1','status':'running','started_utc':datetime.now(timezone.utc).isoformat(),'plan_sha256':PLAN_SHA,'engine_sha256':ENGINE_SHA,'checkpoint_receipt':str(checkpoint),'checkpoint_receipt_sha256':args.checkpoint_sha256.lower(),'checkpoint_status':args.checkpoint_status,'offset':args.offset,'selected_records':len(selected),'archive_root':str(ARCHIVE),'items':[],'skips':[],'live_processes_changed':False,'D_access':False}
    engine.save(run_path,run)
    for number,record in enumerate(selected,start=1):
        source=Path(record['source_path'])
        key=hashlib.sha256(str(source).casefold().encode('utf-8')).hexdigest()[:24]
        archive=ARCHIVE/'payloads'/(key+'.gz');receipt=ARCHIVE/'receipts'/(key+'.json')
        try:
            # Never retry a receipt of uncertain deletion by guessing. Retain
            # any original still present and require explicit reconciliation.
            if receipt.exists():
                saved=existing_receipt(engine,receipt,record)
                status='already_archived_original_absent' if saved.get('original_removed') and not source.exists() else 'existing_archive_preserved_original_not_retried'
                run['items'].append({'source_path':str(source),'status':status,'receipt_path':str(receipt),'original_removed_this_run':False});continue
            plain_c_path(source);plain_c_path(archive,must_exist=False);plain_c_path(receipt,must_exist=False)
            if record['kind']=='generated_bytecode':
                paired=plain_c_path(record['paired_source']);info=paired.stat()
                if {'bytes':info.st_size,'mtime_ns':info.st_mtime_ns}!=record['paired_source_metadata'] or digest(paired)!=record['paired_source_sha256']:raise ValueError('paired_source_changed_since_review')
                with source.open('rb') as f:
                    if f.read(16).hex()!=record['bytecode_header_hex']:raise ValueError('bytecode_header_changed_since_review')
            result=engine.archive_file(source,archive,receipt,record,source_root=Path(record['source_root']),minimum_free=plan['minimum_free_bytes'])
            run['items'].append({'source_path':str(source),'kind':record['kind'],'status':result['status'],'receipt_path':str(receipt),'original_bytes':record['bytes'],'archive_bytes':result['archive_bytes'],'original_removed_this_run':result['original_removed'],'logical_bytes_recovered':result.get('logical_bytes_recovered',0)})
        except Exception as exc:
            run['skips'].append({'source_path':str(source),'type':type(exc).__name__,'reason':str(exc),'original_removal_not_claimed':True})
            if 'drive_reserve' in str(exc):break
        finally:
            if number%50==0:
                engine.save(run_path,run)
                print(json.dumps({'processed':number,'selected':len(selected),'removed_this_run':sum(x.get('original_removed_this_run',False) for x in run['items']),'skipped':len(run['skips'])}),flush=True)
    run.update(status='completed' if not run['skips'] else 'completed_with_skips',completed_utc=datetime.now(timezone.utc).isoformat(),files_removed_this_run=sum(x.get('original_removed_this_run',False) for x in run['items']),logical_bytes_recovered=sum(x.get('logical_bytes_recovered',0) for x in run['items']))
    engine.save(run_path,run)
    print(json.dumps({'receipt':str(run_path),'status':run['status'],'files_removed_this_run':run['files_removed_this_run'],'logical_bytes_recovered':run['logical_bytes_recovered'],'skipped':len(run['skips'])},indent=2))


if __name__=='__main__':main()
