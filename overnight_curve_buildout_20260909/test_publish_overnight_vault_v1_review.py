"""Isolated orchestration tests: every publishing interface uses temporary fixtures.

No real vault/project writes, subprocess, credential read, source export or Git call.
The existing pure path guards are AST-loaded read-only; their full tools are not run.
"""
import ast
import hashlib
import importlib.util
import json
import stat
import sys
import types
from pathlib import Path, PurePosixPath

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent/'trad'
HELPER = HERE/'publish_overnight_vault_v1.py'


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value,sort_keys=True).encode()


def pure_functions(path, names):
    tree=ast.parse(path.read_bytes(),filename=str(path))
    selected=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name in names]
    assert {node.name for node in selected}==set(names)
    namespace=dict(Path=Path,PurePosixPath=PurePosixPath,stat=stat)
    exec(compile(ast.Module(body=selected,type_ignores=[]),str(path),'exec'),namespace)
    return namespace


@pytest.fixture
def env(tmp_path,monkeypatch):
    spec=importlib.util.spec_from_file_location('publication_review_subject',HELPER)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    workspace=tmp_path/'workspace';base=workspace/'review';root=workspace/'trad';vault=tmp_path/'mock_vault'
    for path in (base,root,vault/'source'):path.mkdir(parents=True,exist_ok=True)
    for key,value in dict(BASE=base,WORKSPACE=workspace,ROOT=root,VAULT=vault).items():monkeypatch.setattr(module,key,value)
    bindings={}
    for name in module.SOURCES:
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);raw=b'isolated mock interface\n';path.write_bytes(raw)
        bindings[name]=digest(raw)
    monkeypatch.setattr(module,'SOURCES',bindings)
    member='docs/validation/overnight_curve_buildout_20260909/fixture.json'
    evidence=root/member;evidence.parent.mkdir(parents=True,exist_ok=True);evidence.write_bytes(b'{"fixture":true}')
    copy=base/'copy.json'
    copied=dict(status='copied_exact_members',all_source_hashes_unchanged=True,
        credential_pattern_and_known_value_scan_passed=True,selected_python_compile_passed=True,
        entries=[dict(intended_member=member,original_source=dict(sha256=digest(evidence.read_bytes()),bytes=evidence.stat().st_size))])
    copy.write_bytes(encode(copied))
    report=root/module.REPORT;report.parent.mkdir(parents=True,exist_ok=True);report.write_bytes(b'# isolated report\n')
    validation=root/module.VALIDATION
    validation.write_bytes(encode(dict(schema_version='forex_overnight_curve_buildout_validation_v1_20260909',
        status='verification_complete_publication_separate',orders_enabled=False,
        portable_evidence=dict(project_copy_receipt=dict(path=str(copy),sha256=digest(copy.read_bytes()))))))
    records=[(Path('trad')/module.REPORT,'REPORT.md'),(Path('trad')/module.VALIDATION,'VALIDATION.json')]
    for index in range(179):
        path=root/f'fixture_{index:03}.md';path.write_bytes(f'fixture {index}\n'.encode())
        records.append((Path('trad')/path.name,path.name))
    state=types.SimpleNamespace(module=module,root=root,vault=vault,base=base,workspace=workspace,
        member=member,copy=copy,records=records,events=[],audited=[],mode=None)
    snapshots=pure_functions(ROOT/'tools/vault_worktree_snapshot.py',('safe_name','regular_file'))
    reads=pure_functions(ROOT/'tools/audit_forex_vault_readability.py',('safe_relative','checked_root','checked_output'))
    snapshot=types.ModuleType('tools.vault_worktree_snapshot')
    snapshot.POINTER='WORKTREE_SOURCE_LATEST.json'
    snapshot.safe_name=snapshots['safe_name'];snapshot.regular_file=snapshots['regular_file']
    snapshot.source_state=lambda _:({},[member,module.REPORT,module.VALIDATION],{})
    snapshot.excluded_reason=lambda name:None
    snapshot.known_private_values=lambda _:set()
    def audit(name,raw,values):
        state.audited.append(name)
        if state.mode=='last_canonical_rejected' and name=='fixture_178.md':raise ValueError('fixture_scan_rejected')
    snapshot.audit_payload=audit
    def sync(root_arg,vault_arg,**kwargs):
        assert root_arg==root and vault_arg==vault
        assert all(relative.as_posix().removeprefix('trad/') in state.audited for relative,_ in records)
        assert (state.out/'PUBLICATION_PREFLIGHT_20260909.json').exists()
        state.events.append('source')
        if state.mode=='source_failure':raise ValueError('arbitrary failure contents should not be exported')
        required=[member,module.REPORT,module.VALIDATION]
        rows=[dict(path=name,sha256=digest((root/name).read_bytes()),size=(root/name).stat().st_size) for name in required]
        if state.mode=='omit_member':rows=rows[1:]
        if state.mode=='change_member_hash':rows[0]['sha256']='0'*64
        if state.mode=='change_member_size':rows[0]['size']+=1
        manifest=vault/'source/mock.manifest.json';manifest.write_bytes(encode(dict(files=rows)))
        pointer=dict(archive='mock.zip',archive_sha256='a'*64,manifest=manifest.name,manifest_sha256=digest(manifest.read_bytes()))
        (vault/'source'/snapshot.POINTER).write_bytes(encode(pointer))
        return pointer
    snapshot.sync_worktree_snapshot=sync
    def verify(path):
        state.events.append('verify')
        if state.mode=='manifest_changes_during_verify':path.write_bytes(encode(dict(files=[])))
        return dict(status='passed',roundtrip_verified=True)
    snapshot.verify_snapshot=verify
    readability=types.ModuleType('tools.audit_forex_vault_readability')
    readability.REPORT='VAULT_READABILITY_REPORT.json';readability.INDEX='KNOWLEDGE_INDEX.md'
    readability.checked_root=reads['checked_root'];readability.checked_output=reads['checked_output']
    canonical=types.ModuleType('forex_model_vault_sync');canonical.CANONICAL_PROJECT_RECORDS=records
    def canonical_sync(workspace_arg,vault_arg):
        assert workspace_arg==workspace and vault_arg==vault
        state.events.append('canonical')
        if state.mode=='canonical_failure':raise ValueError('fixture_canonical_failure')
        for relative,name in records:(vault/name).write_bytes((workspace/relative).read_bytes())
        raw=encode(dict(record_count=len(records)));(vault/'SHARED_PROJECT_STATE_CURRENT.json').write_bytes(raw)
        return dict(manifest_sha256=digest(raw),records=[])
    canonical.sync_canonical_project_records=canonical_sync
    package=types.ModuleType('tools');package.vault_worktree_snapshot=snapshot;package.audit_forex_vault_readability=readability
    for name,value in {'tools':package,'tools.vault_worktree_snapshot':snapshot,
        'tools.audit_forex_vault_readability':readability,'forex_model_vault_sync':canonical}.items():monkeypatch.setitem(sys.modules,name,value)
    def run(args,**kwargs):
        assert args[2]==str(root/'tools/audit_forex_vault_readability.py')
        mode=args[-1];state.events.append(mode)
        if state.mode=='readability_failure':return types.SimpleNamespace(returncode=1,stdout='sensitive arbitrary diagnostic')
        for name in (readability.REPORT,readability.INDEX):(vault/name).write_bytes(b'isolated fixture\n')
        return types.SimpleNamespace(returncode=0,stdout=json.dumps(dict(status='pass_with_declared_external_dependencies',navigation_errors=0)))
    monkeypatch.setattr(module.subprocess,'run',run)
    monkeypatch.setattr(sys,'path',list(sys.path));monkeypatch.setattr(sys,'dont_write_bytecode',True)
    state.out=base/'fresh_output'
    monkeypatch.setattr(sys,'argv',[str(HELPER),'--expected-report-sha256',digest(report.read_bytes()),
        '--expected-validation-sha256',digest(validation.read_bytes()),'--copy-receipt',str(copy),
        '--expected-copy-receipt-sha256',digest(copy.read_bytes()),'--output-directory',str(state.out)])
    state.snapshot=snapshot;state.readability=readability
    return state


def test_all_181_preflight_before_source_then_narrow_publication(env):
    env.module.main()
    assert env.events==['source','canonical','--build','--check','verify']
    value=json.loads((env.out/'OVERNIGHT_VAULT_PUBLICATION_20260909.json').read_bytes())
    assert value['canonical_record_count']==181 and value['required_exported_member_count']==3
    assert len(value['final_persisted_files'])==5
    assert value['all_required_members_verified_in_archive'] is True
    assert value['broker_requests'] is False and value['history_pruned'] is False


def test_last_canonical_scan_failure_has_no_vault_call(env):
    env.mode='last_canonical_rejected'
    with pytest.raises(ValueError,match='fixture_scan_rejected'):env.module.main()
    assert env.events==[] and not env.out.exists()


def test_missing_181st_canonical_source_has_no_vault_call(env):
    (env.workspace/env.records[-1][0]).unlink()
    with pytest.raises(FileNotFoundError):env.module.main()
    assert env.events==[] and not env.out.exists()


@pytest.mark.parametrize('mode',['omit_member','change_member_hash','change_member_size'])
def test_persisted_required_member_refused_before_canonical(env,mode):
    env.mode=mode
    with pytest.raises(RuntimeError,match='overnight_publication_failed'):env.module.main()
    assert env.events==['source']
    failure=json.loads((env.out/'PUBLICATION_FAILURE_20260909.json').read_bytes())
    assert failure['partial_publication_possible'] is True and failure['automatic_rollback'] is False
    assert not (env.out/'OVERNIGHT_VAULT_PUBLICATION_20260909.json').exists()


def test_required_member_not_in_git_inventory_refuses_before_write(env):
    env.snapshot.source_state=lambda _:({},[env.module.REPORT,env.module.VALIDATION],{})
    with pytest.raises(ValueError,match='not_export_eligible'):env.module.main()
    assert env.events==[]


@pytest.mark.parametrize('relative',['SHARED_PROJECT_STATE_CURRENT.json','source/WORKTREE_SOURCE_LATEST.json','VAULT_READABILITY_REPORT.json','KNOWLEDGE_INDEX.md'])
def test_nonregular_fixed_destination_refuses_before_write(env,relative):
    (env.vault/relative).mkdir()
    with pytest.raises(ValueError,match='redirected or nonregular'):env.module.main()
    assert env.events==[] and not env.out.exists()


def test_redirected_fixed_destination_refuses_before_write(env,monkeypatch):
    target=env.vault/'SHARED_PROJECT_STATE_CURRENT.json';target.write_bytes(b'fixture')
    original=Path.is_symlink
    monkeypatch.setattr(Path,'is_symlink',lambda self:self==target or original(self))
    with pytest.raises(ValueError,match='redirected or nonregular'):env.module.main()
    assert env.events==[]


@pytest.mark.parametrize('mode,events',[
    ('source_failure',['source']),('canonical_failure',['source','canonical']),
    ('readability_failure',['source','canonical','--build']),
    ('manifest_changes_during_verify',['source','canonical','--build','--check','verify'])])
def test_later_failure_is_honest_partial_not_success_or_raw_exception(env,mode,events):
    env.mode=mode
    with pytest.raises(RuntimeError,match='overnight_publication_failed'):env.module.main()
    assert env.events==events
    raw=(env.out/'PUBLICATION_FAILURE_20260909.json').read_bytes();value=json.loads(raw)
    assert value['status']=='failed' and value['partial_publication_possible'] is True
    assert value['automatic_rollback'] is False and value['orders_enabled'] is False
    assert b'sensitive' not in raw and b'arbitrary' not in raw
    assert not (env.out/'OVERNIGHT_VAULT_PUBLICATION_20260909.json').exists()
