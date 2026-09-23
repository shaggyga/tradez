"""Publish the verified source snapshot and canonical records, then check navigation.

Calls only the existing source-snapshot/canonical-record/readability interfaces.
No model checkpoints, model imports, runtime controls, pruning or broker access.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

BASE=Path(__file__).resolve().parent
WORKSPACE=BASE.parent
ROOT=WORKSPACE/'trad'
VAULT=Path(r'C:/Users/zmoor/OneDrive/thevault/projects/forex')
SOURCES={
    'forex_model_vault_sync.py':'0931bd4c2f0c83af838be13cb7577cf24b9af6c518bf332bcc1a2105325a1bb8',
    'tools/vault_worktree_snapshot.py':'5868f3794de8e98b75f459be72d08971184002a4a7da1f1bdd78ef6cbd144515',
    'tools/audit_forex_vault_readability.py':'1480d9a930a622c8b4136cbe7817208459c5c813bbee6306ce575d5d9c343062',
}
REPORT='docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md'
VALIDATION='FOREX_OVERNIGHT_CURVE_BUILDOUT_VALIDATION_20260909.json'


def sha(raw):return hashlib.sha256(raw).hexdigest()


def need(value,reason):
    if not value:raise ValueError(reason)


def bound(path,expected):
    raw=Path(path).read_bytes();need(sha(raw)==expected,'publication_input_changed');return raw


def save(path,value):
    raw=(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode()
    with path.open('xb') as h:h.write(raw);h.flush();os.fsync(h.fileno())
    return dict(path=str(path),sha256=sha(raw),bytes=len(raw))


def require_exported_members(manifest,required):
    rows=manifest.get('files',[])
    need(isinstance(rows,list),'source_manifest_inventory')
    by_name={row['path']:row for row in rows}
    need(len(by_name)==len(rows),'source_manifest_duplicate_member')
    for name,record in required.items():
        actual=by_name.get(name)
        need(actual is not None and actual.get('sha256')==record['sha256'] and
             actual.get('size')==record['bytes'],'required_evidence_missing_or_changed_in_source_archive')


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--expected-report-sha256',required=True)
    parser.add_argument('--expected-validation-sha256',required=True)
    parser.add_argument('--copy-receipt',type=Path,required=True)
    parser.add_argument('--expected-copy-receipt-sha256',required=True)
    parser.add_argument('--output-directory',type=Path,required=True)
    args=parser.parse_args();out=args.output_directory.absolute()
    need(out.parent==BASE and not out.exists() and not out.is_symlink(),'fresh_publication_output_required')
    started=time.time();helper_raw=Path(__file__).read_bytes()
    for name,expected in SOURCES.items():bound(ROOT/name,expected)
    report_raw=bound(ROOT/REPORT,args.expected_report_sha256)
    validation_raw=bound(ROOT/VALIDATION,args.expected_validation_sha256)
    validation=json.loads(validation_raw)
    need(validation.get('schema_version')=='forex_overnight_curve_buildout_validation_v1_20260909','validation_schema')
    need(validation.get('status')=='verification_complete_publication_separate','validation_not_complete')
    need(validation.get('orders_enabled') is False,'authority_mismatch')
    copy_path=args.copy_receipt.absolute()
    need(copy_path.parent==BASE and not copy_path.is_symlink(),'unexpected_copy_receipt')
    declared_copy=validation.get('portable_evidence',{}).get('project_copy_receipt',{})
    need(declared_copy.get('path')==str(copy_path) and
         declared_copy.get('sha256')==args.expected_copy_receipt_sha256,'validation_copy_receipt_mismatch')
    copied=json.loads(bound(copy_path,args.expected_copy_receipt_sha256))
    need(copied.get('status')=='copied_exact_members' and copied.get('all_source_hashes_unchanged') is True,
         'verified_project_copy_required')
    need(copied.get('credential_pattern_and_known_value_scan_passed') is True and
         copied.get('selected_python_compile_passed') is True,'copy_scan_or_compile_missing')
    sys.dont_write_bytecode=True;sys.path.insert(0,str(ROOT))
    from tools import vault_worktree_snapshot as snapshot
    from tools import audit_forex_vault_readability as readability
    import forex_model_vault_sync as records
    for directory in (BASE,WORKSPACE,ROOT,VAULT,VAULT/'source'):readability.checked_root(directory)
    for directory,name in ((VAULT,'SHARED_PROJECT_STATE_CURRENT.json'),(VAULT/'source',snapshot.POINTER),
                           (VAULT,readability.REPORT),(VAULT,readability.INDEX)):
        readability.checked_output(directory,name)
    _,source_names,source_modes=snapshot.source_state(ROOT);source_names=set(source_names)
    private_values=snapshot.known_private_values([ROOT/'creds'])
    need(0<len(copied['entries'])<=1200,'copy_inventory_bound')
    required_members={REPORT:dict(sha256=sha(report_raw),bytes=len(report_raw)),
                      VALIDATION:dict(sha256=sha(validation_raw),bytes=len(validation_raw))}
    for entry in copied['entries']:
        name=entry['intended_member']
        need(name.startswith('docs/validation/overnight_curve_buildout_20260909/'),'copy_member_scope')
        member=snapshot.regular_file(ROOT,name);source=entry['original_source']
        raw=bound(member,source['sha256']);need(len(raw)==source['bytes'],'copy_member_size')
        snapshot.audit_payload(name,raw,private_values)
        need(name not in required_members,'duplicate_required_member')
        required_members[name]=dict(sha256=source['sha256'],bytes=source['bytes'])
    for name in required_members:
        need(name in source_names and snapshot.excluded_reason(name) is None and
             source_modes.get(name) not in {'120000','160000'},'required_evidence_not_export_eligible')
    # Preflight every canonical record before the first vault write.
    destinations=set();canonical=[]
    for relative,name in records.CANONICAL_PROJECT_RECORDS:
        snapshot.safe_name(relative.as_posix());snapshot.safe_name(name)
        need('/' not in name and name.casefold() not in destinations,'canonical_destination_collision')
        destinations.add(name.casefold())
        source=snapshot.regular_file(WORKSPACE,relative.as_posix());raw=source.read_bytes()
        snapshot.audit_payload(relative.as_posix().removeprefix('trad/'),raw,private_values)
        readability.checked_output(VAULT,name)
        if (VAULT/name).exists() or (VAULT/name).is_symlink():snapshot.regular_file(VAULT,name)
        canonical.append(dict(source=relative.as_posix(),name=name,sha256=sha(raw),bytes=len(raw)))
    need(len(canonical)==181,'canonical_record_inventory_changed')
    out.mkdir()
    save(out/'PUBLICATION_PREFLIGHT_20260909.json',dict(started_epoch=started,completed_epoch=time.time(),
        sources=SOURCES,report_sha256=sha(report_raw),validation_sha256=sha(validation_raw),
        copy_receipt_sha256=args.expected_copy_receipt_sha256,canonical_records=canonical,
        status='preflight_passed_vault_not_yet_written',orders_enabled=False))
    try:
        pointer=snapshot.sync_worktree_snapshot(ROOT,VAULT,private_files=[ROOT/'creds'])
        save(out/'SOURCE_PUBLICATION_RECEIPT_20260909.json',pointer)
        manifest_path=snapshot.regular_file(VAULT/'source',pointer['manifest'])
        manifest=json.loads(bound(manifest_path,pointer['manifest_sha256']))
        require_exported_members(manifest,required_members)
        # Require the exact preflighted canonical bytes still present.
        for item in canonical:bound(WORKSPACE/item['source'],item['sha256'])
        canonical_result=records.sync_canonical_project_records(WORKSPACE,VAULT)
        save(out/'CANONICAL_RECORD_PUBLICATION_RECEIPT_20260909.json',canonical_result)
        checks=[]
        for mode in ('--build','--check'):
            result=subprocess.run([sys.executable,'-B',str(ROOT/'tools/audit_forex_vault_readability.py'),
                '--vault-project',str(VAULT),mode],capture_output=True,text=True,timeout=240,check=False)
            # Do not retain potentially arbitrary exception/source payloads on failure.
            need(result.returncode==0,'vault_readability_command_failed')
            value=json.loads(result.stdout)
            need(value['status']=='pass_with_declared_external_dependencies' and value['navigation_errors']==0,
                 'vault_readability_not_passed')
            checks.append(value)
        for name,expected in SOURCES.items():bound(ROOT/name,expected)
        bound(ROOT/REPORT,args.expected_report_sha256);bound(ROOT/VALIDATION,args.expected_validation_sha256)
        for item in canonical:
            bound(WORKSPACE/item['source'],item['sha256']);bound(VAULT/item['name'],item['sha256'])
        current=json.loads((VAULT/'source/WORKTREE_SOURCE_LATEST.json').read_bytes())
        need(current['archive_sha256']==pointer['archive_sha256'] and current['manifest_sha256']==pointer['manifest_sha256'],
             'source_pointer_changed')
        verified=snapshot.verify_snapshot(manifest_path)
        require_exported_members(json.loads(bound(manifest_path,pointer['manifest_sha256'])),required_members)
        final_files={name:dict(path=str(VAULT/name),sha256=sha((VAULT/name).read_bytes()),bytes=(VAULT/name).stat().st_size)
            for name in ('SHARED_PROJECT_STATE_CURRENT.json','source/WORKTREE_SOURCE_LATEST.json',
                         'source/'+pointer['manifest'],readability.REPORT,readability.INDEX)}
        need(Path(__file__).read_bytes()==helper_raw,'publication_helper_changed')
        result=dict(schema_version='overnight_vault_publication_v1_20260909',status='published_and_verified',
            started_epoch=started,completed_epoch=time.time(),helper_sha256=sha(helper_raw),
            source_bindings=SOURCES,source_pointer=pointer,source_reverification=verified,
            canonical_record_count=len(canonical),canonical_result=canonical_result,readability_checks=checks,
            required_exported_member_count=len(required_members),all_required_members_verified_in_archive=True,
            final_persisted_files=final_files,
            report_sha256=sha(report_raw),validation_sha256=sha(validation_raw),
            copy_receipt_sha256=args.expected_copy_receipt_sha256,source_only=True,raw_runtime_or_models_exported=False,
            credentials_exported=False,history_pruned=False,orders_enabled=False,broker_requests=False)
        receipt=save(out/'OVERNIGHT_VAULT_PUBLICATION_20260909.json',result)
        print(json.dumps(dict(status=result['status'],receipt=receipt,archive_sha256=pointer['archive_sha256'],
            canonical_records=len(canonical),readability_status=checks[-1]['status'])))
    except Exception as exc:
        save(out/'PUBLICATION_FAILURE_20260909.json',dict(status='failed',failed_epoch=time.time(),
            exception_type=type(exc).__name__,reason='publication_or_postpublication_verification_failed',
            partial_publication_possible=True,automatic_rollback=False,history_pruned=False,orders_enabled=False))
        raise RuntimeError('overnight_publication_failed_see_bounded_receipt') from None


if __name__=='__main__':main()
