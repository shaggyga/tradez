"""Verify explicit reviewed evidence selections, optionally copy exact source members.

No vault writes, runtime data discovery, source replacement, Git mutation or broker IO.
All selected content passes the existing credential-aware source-export scanner.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import time

ROOT=Path(r'C:/Users/zmoor/Documents/forex/trad')
BASE=Path(__file__).resolve().parent
PREFIX='docs/validation/overnight_curve_buildout_20260909/'
MAX_FILES=1200
MAX_FILE_BYTES=4*1024*1024
MAX_TOTAL_BYTES=64*1024*1024
# Exact original scorers referenced by the already accepted wrappers. No other
# prior workspace, runtime file, dataset or model artifact is admitted here.
ORIGINAL_HELPERS={
    Path(r'C:/Users/zmoor/Documents/forex/program_assessment_20260909T0316Z/performance/assess_v3.py'):
        'bad4ea610f01a19e82ae1e38709775ab306c609e2cdb799e35a5da75c44ea5a2',
    Path(r'C:/Users/zmoor/Documents/forex/revamp_baseline_20260908/performance/evaluate_frozen_baseline.py'):
        '186e9000e2af2360e23bbdb752fb83bd26df61ca7c387899a880adae2739b7f1',
}
sys.dont_write_bytecode=True
sys.path.insert(0,str(ROOT))
from tools import vault_worktree_snapshot as safety


def need(value,reason):
    if not value:raise ValueError(reason)


def digest(raw):return hashlib.sha256(raw).hexdigest()


def read_bound(path,expected,size=None,*,base=BASE):
    path=Path(path).absolute()
    if not path.is_relative_to(base):
        need(base==BASE and ORIGINAL_HELPERS.get(path)==expected,'source_outside_declared_root')
        base=path.parent
    path=safety.regular_file(base,path.relative_to(base).as_posix())
    need(re.fullmatch('[0-9a-f]{64}',expected) is not None,'source_digest_shape')
    need(path.stat().st_size<=MAX_FILE_BYTES,'source_file_byte_bound')
    raw=path.read_bytes()
    need(len(raw)<=MAX_FILE_BYTES and digest(raw)==expected,'source_identity_changed')
    if size is not None:need(type(size) is int and len(raw)==size,'source_size_changed')
    return raw


def member_name(name):
    need(type(name) is str and name.startswith(PREFIX),'unexpected_export_member')
    safety.safe_name(name)
    need(name!=PREFIX.rstrip('/') and safety.excluded_reason(name) is None,'excluded_export_member')
    return name


def source_record(value):
    need(type(value) is dict and set(('path','sha256','bytes'))<=set(value),'source_record_shape')
    return {k:value[k] for k in ('path','sha256','bytes')}


def validate_entries(entries,private_values,*,base=BASE,root=ROOT):
    need(type(entries) is list and 0<len(entries)<=MAX_FILES,'selection_file_count_bound')
    admitted={};payloads={};total=0
    for entry in entries:
        member=member_name(entry['intended_member']);source=source_record(entry['original_source'])
        raw=read_bound(source['path'],source['sha256'],source['bytes'],base=base)
        safety.audit_payload(member,raw,private_values)
        if member.endswith('.py'):compile(raw,member,'exec',dont_inherit=True)
        folded=member.casefold()
        if folded in admitted:
            prior=admitted[folded]
            need(prior['intended_member']==member and prior['original_source']==source,'duplicate_export_member_conflict')
            continue
        total+=len(raw);need(total<=MAX_TOTAL_BYTES,'selection_total_byte_bound')
        destination=root/member
        if destination.exists() or destination.is_symlink():
            existing=safety.regular_file(root,member).read_bytes()
            need(existing==raw,'existing_export_member_differs')
        # Validate existing ancestors without following a redirected directory.
        for directory in destination.parents:
            if directory==root.parent:break
            try:
                info=directory.lstat()
            except FileNotFoundError:continue
            need(not stat.S_ISLNK(info.st_mode) and not getattr(info,'st_reparse_tag',0)&0x20000000,'export_parent_redirect')
            need(stat.S_ISDIR(info.st_mode),'export_parent_not_directory')
        admitted[folded]=dict(intended_member=member,original_source=source,
            kind=entry.get('kind','curated_evidence'),group=entry.get('group','unclassified'),
            artifact_status=entry.get('artifact_status','see_original_record'),
            machine_specific_dependencies_not_implied_included=True)
        payloads[member]=raw
    return sorted(admitted.values(),key=lambda r:r['intended_member']),payloads,total


def execute(selections,output,*,apply=False):
    need(type(apply) is bool and 1<=len(selections)<=8,'selection_input_bound')
    output=Path(output).absolute()
    need(output.parent==BASE and output.suffix=='.json' and not output.exists() and not output.is_symlink(),'fresh_external_receipt_required')
    started=time.time();bindings=[];entries=[];dependencies={}
    for path,expected in selections:
        raw=read_bound(path,expected);selection=json.loads(raw)
        need(type(selection.get('entries')) is list,'explicit_selection_entries_required')
        entries.extend(selection['entries'])
        bindings.append(dict(path=str(Path(path).absolute()),sha256=expected,bytes=len(raw)))
        for item in selection.get('canonical_source_dependencies',[]):
            source=source_record(item.get('original_source',item))
            read_bound(source['path'],source['sha256'],source['bytes'],base=ROOT)
            key=source['path'].casefold()
            need(key not in dependencies or dependencies[key]==source,'canonical_dependency_conflict')
            dependencies[key]=source
    private_values=safety.known_private_values([ROOT/'creds'])
    accepted,payloads,total=validate_entries(entries,private_values)
    # Finish all source and destination preflight before the first source-tree write.
    for row in accepted:read_bound(**dict(path=row['original_source']['path'],expected=row['original_source']['sha256'],size=row['original_source']['bytes']))
    if apply:
        for member,raw in payloads.items():
            target=ROOT/member
            target.parent.mkdir(parents=True,exist_ok=True)
            if not target.exists():
                with target.open('xb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())
            need(safety.regular_file(ROOT,member).read_bytes()==raw,'copied_member_readback_changed')
    for binding in bindings:read_bound(binding['path'],binding['sha256'],binding['bytes'])
    for source in dependencies.values():read_bound(source['path'],source['sha256'],source['bytes'],base=ROOT)
    for row in accepted:read_bound(row['original_source']['path'],row['original_source']['sha256'],row['original_source']['bytes'])
    report=dict(schema_version='portable_evidence_preparation_v1_20260909',status='copied_exact_members' if apply else 'preflight_passed_no_copy',
        started_epoch=started,completed_epoch=time.time(),helper_sha256=digest(Path(__file__).read_bytes()),
        source_scanner_sha256=digest((ROOT/'tools/vault_worktree_snapshot.py').read_bytes()),
        selections=bindings,entries=accepted,file_count=len(accepted),total_bytes=total,
        canonical_source_dependencies=sorted(dependencies.values(),key=lambda r:r['path']),
        credential_pattern_and_known_value_scan_passed=True,known_private_values_checked=bool(private_values),
        selected_python_compile_passed=True,all_source_hashes_unchanged=True,project_exact_copy_performed=apply,
        vault_writes=False,broker_requests=False,git_mutations=False,
        scope='Explicit selected evidence only. Copying accepted helpers does not transport their private inputs, models, original runtime or machine-specific path dependencies. Source ZIP publication and navigation audit are separate later checks.')
    raw=safety.encoded(report)
    with output.open('xb') as handle:handle.write(raw);handle.flush();os.fsync(handle.fileno())
    print(json.dumps(dict(status=report['status'],path=str(output),sha256=digest(raw),file_count=len(accepted),bytes=total)))
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--selection',nargs=2,action='append',required=True,metavar=('PATH','SHA256'))
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();execute(args.selection,args.output,apply=args.apply)
