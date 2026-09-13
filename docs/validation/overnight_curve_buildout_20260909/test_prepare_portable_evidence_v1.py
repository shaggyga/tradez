"""Disposable packaging fixtures; no real credentials, project writes or vault IO."""
import hashlib
import importlib.util
import json
from pathlib import Path
import stat
from types import SimpleNamespace

import pytest

SOURCE=Path(__file__).with_name('prepare_portable_evidence_v1.py')
PREFIX='docs/validation/overnight_curve_buildout_20260909/'


@pytest.fixture
def env(tmp_path,monkeypatch):
    spec=importlib.util.spec_from_file_location('portable_evidence_fixture',SOURCE)
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
    base=tmp_path/'external';root=tmp_path/'project';base.mkdir();root.mkdir()
    (root/'tools').mkdir()
    # Report provenance points to an exact copy of the actual imported scanner.
    (root/'tools/vault_worktree_snapshot.py').write_bytes(Path(mod.safety.__file__).read_bytes())
    old_read,old_validate=mod.read_bound,mod.validate_entries
    monkeypatch.setattr(mod,'BASE',base);monkeypatch.setattr(mod,'ROOT',root)
    monkeypatch.setattr(mod,'read_bound',lambda *args,**kwargs:old_read(*args,**{'base':base,**kwargs}))
    monkeypatch.setattr(mod,'validate_entries',lambda *args,**kwargs:old_validate(*args,**{'base':base,'root':root,**kwargs}))
    monkeypatch.setattr(mod.safety,'known_private_values',lambda paths:set())
    return mod,base,root


def entry(base,name='note.txt',raw=b'Plain retained engineering evidence.\n',member=None):
    path=base/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
    return {'intended_member':member or PREFIX+name,'original_source':{'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)},'group':'fixture','kind':'summary'}


def selection(env,entries,dependencies=None,name='selection.json'):
    mod,base,_=env;path=base/name
    raw=json.dumps({'entries':entries,'canonical_source_dependencies':dependencies or []}).encode()
    path.write_bytes(raw)
    return [(path,mod.digest(raw))]


def tree(root):return {str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}


def refused_without_writes(env,selections):
    mod,base,root=env;before=tree(root);output=base/'result.json'
    with pytest.raises((ValueError,RuntimeError,KeyError,TypeError,SyntaxError,OSError)):
        mod.execute(selections,output,apply=True)
    assert tree(root)==before
    assert not output.exists()


@pytest.mark.parametrize('member',[
    PREFIX+'../outside.txt',PREFIX+'folder/../../outside.txt',PREFIX+'folder//note.txt',
    PREFIX+'folder/./note.txt',PREFIX+'C:/outside.txt',PREFIX+'folder\\note.txt',
    PREFIX+'CON.txt',PREFIX+'trail. /note.txt',PREFIX+'note.txt:alternate',
    'C:/outside.txt','/absolute/note.txt','docs/validation/other/note.txt',
])
def test_member_traversal_windows_alias_or_wrong_prefix_rejects_before_writes(env,member):
    _,base,_=env
    refused_without_writes(env,selection(env,[entry(base,'valid.txt'),entry(base,'bad.txt',member=member)]))


def test_case_colliding_targets_reject_before_any_copy(env):
    _,base,_=env
    records=[entry(base,'one.txt',member=PREFIX+'Same.txt'),entry(base,'two.txt',member=PREFIX+'same.txt')]
    refused_without_writes(env,selection(env,records))


def test_identical_repeated_entry_is_deduplicated_without_duplicate_byte_count(env):
    mod,base,root=env;record=entry(base)
    report=mod.execute(selection(env,[record,record]),base/'result.json',apply=True)
    assert report['file_count']==1 and report['total_bytes']==record['original_source']['bytes']
    assert (root/record['intended_member']).read_bytes()==(base/'note.txt').read_bytes()


def test_changed_source_hash_rejects_even_after_a_valid_earlier_entry(env):
    _,base,_=env;good=entry(base,'good.txt');bad=entry(base,'bad.txt');selections=selection(env,[good,bad])
    (base/'bad.txt').write_bytes(b'Changed after selection.\n')
    refused_without_writes(env,selections)


def test_changed_selection_bytes_reject_before_any_copy(env):
    _,base,_=env;selections=selection(env,[entry(base)])
    selections[0][0].write_bytes(b'{}')
    refused_without_writes(env,selections)


def test_known_private_bytes_are_rejected_without_printing_the_value(env,monkeypatch,capsys):
    mod,base,_=env;private=b'nonpublic_'+b'fixture_material_90210'
    monkeypatch.setattr(mod.safety,'known_private_values',lambda paths:{private})
    records=[entry(base,'good.txt'),entry(base,'bad.txt',raw=b'prefix '+private+b' suffix')]
    refused_without_writes(env,selection(env,records))
    captured=capsys.readouterr();assert private.decode() not in captured.out+captured.err


def test_existing_credential_pattern_is_rejected_without_scanner_exemptions(env):
    _,base,_=env
    # Construct the exact material at runtime; this test source itself contains no
    # contiguous credential pattern and uses no new scanner exemption.
    material=(b'a'*32)+b'-'+(b'b'*32)
    header=b'Bea'+b'rer '+material
    refused_without_writes(env,selection(env,[entry(base,raw=b'opaque '+header+b' evidence')]))


def test_invalid_python_source_fails_compile_before_any_copy(env):
    _,base,_=env
    refused_without_writes(env,selection(env,[entry(base,'good.txt'),entry(base,'broken.py',raw=b'def broken(:\n')]))


def test_valid_python_is_compiled_without_execution(env):
    mod,base,root=env
    record=entry(base,'source.py',raw=b'raise RuntimeError("Must never execute source during validation")\n')
    result=mod.execute(selection(env,[record]),base/'result.json',apply=False)
    assert result['selected_python_compile_passed'] is True
    assert not (root/record['intended_member']).exists()


def test_existing_destination_mismatch_rejects_before_any_new_write(env):
    _,base,root=env;one=entry(base,'one.txt');two=entry(base,'two.txt')
    destination=root/two['intended_member'];destination.parent.mkdir(parents=True);destination.write_bytes(b'Different retained evidence')
    refused_without_writes(env,selection(env,[one,two]))


def test_existing_exact_destination_is_preserved_and_idempotent(env):
    mod,base,root=env;record=entry(base)
    destination=root/record['intended_member'];destination.parent.mkdir(parents=True);destination.write_bytes((base/'note.txt').read_bytes())
    before=destination.stat().st_mtime_ns
    result=mod.execute(selection(env,[record]),base/'result.json',apply=True)
    assert result['status']=='copied_exact_members'
    assert destination.stat().st_mtime_ns==before


def test_source_outside_declared_evidence_root_is_rejected(env,tmp_path):
    _,base,_=env;outside=entry(tmp_path,'outside.txt')
    refused_without_writes(env,selection(env,[entry(base),outside]))


@pytest.mark.parametrize('nested',[False,True])
def test_direct_and_nested_canonical_dependency_manifest_shapes(env,nested):
    mod,base,root=env;native=root/'native.py';native.write_bytes(b'VALUE = 1\n')
    record={'path':str(native),'sha256':mod.digest(native.read_bytes()),'bytes':native.stat().st_size}
    dependency={'original_source':record,'scope':'nested_fixture'} if nested else {**record,'source_snapshot_member':'native.py'}
    result=mod.execute(selection(env,[entry(base)],dependencies=[dependency]),base/'result.json',apply=False)
    assert result['canonical_source_dependencies']==[record]
    assert not (root/PREFIX).exists()


@pytest.mark.parametrize('nested',[False,True])
def test_changed_canonical_dependency_rejects_before_any_copy(env,nested):
    mod,base,root=env;native=root/'native.py';native.write_bytes(b'VALUE = 1\n')
    record={'path':str(native),'sha256':mod.digest(native.read_bytes()),'bytes':native.stat().st_size}
    selections=selection(env,[entry(base)],dependencies=[{'original_source':record} if nested else record])
    native.write_bytes(b'VALUE = 2\n');refused_without_writes(env,selections)


@pytest.mark.parametrize('field,value',[('sha256','bad'),('bytes',True),('bytes',-1)])
def test_invalid_source_identity_fields_reject_before_copy(env,field,value):
    _,base,_=env;record=entry(base);record['original_source'][field]=value
    refused_without_writes(env,selection(env,[record]))


def test_unknown_patch_extension_remains_rejected_while_exact_txt_mapping_passes(env):
    mod,base,root=env;record=entry(base,'changes.patch')
    refused_without_writes(env,selection(env,[record]))
    record['intended_member']+='.txt'
    result=mod.execute(selection(env,[record],name='selection2.json'),base/'accepted.json',apply=True)
    assert result['file_count']==1
    assert (root/record['intended_member']).read_bytes()==(base/'changes.patch').read_bytes()


def test_existing_receipt_is_never_overwritten(env):
    mod,base,root=env;selections=selection(env,[entry(base)]);output=base/'result.json';output.write_bytes(b'prior receipt')
    before=tree(root)
    with pytest.raises(ValueError,match='fresh_external_receipt_required'):mod.execute(selections,output,apply=True)
    assert output.read_bytes()==b'prior receipt' and tree(root)==before


def test_dangling_receipt_symlink_rejected_before_any_copy(env,monkeypatch):
    mod,base,root=env;selections=selection(env,[entry(base)]);output=base/'result.json'
    original=Path.is_symlink
    monkeypatch.setattr(Path,'is_symlink',lambda path:True if path==output else original(path))
    # Simulate the observable broken-link predicate without Windows link privilege.
    # No filesystem redirection is made and every fixture path stays disposable.
    refused_without_writes(env,selections)


def test_test_source_itself_passes_unchanged_export_scanner(env):
    mod,_,_=env
    mod.safety.audit_payload(PREFIX+'test_prepare_portable_evidence_v1.py.txt',Path(__file__).read_bytes(),set())


def test_file_and_aggregate_limits_reject_before_writes(env,monkeypatch):
    mod,base,_=env
    monkeypatch.setattr(mod,'MAX_TOTAL_BYTES',3)
    refused_without_writes(env,selection(env,[entry(base,raw=b'four')]))


def test_redirected_source_is_rejected(env,tmp_path):
    _,base,_=env;outside=tmp_path/'outside.txt';outside.write_bytes(b'outside');link=base/'link.txt'
    try:link.symlink_to(outside)
    except OSError:pytest.skip('Symlink privilege unavailable on this host')
    record={'intended_member':PREFIX+'link.txt','original_source':{'path':str(link),'sha256':hashlib.sha256(outside.read_bytes()).hexdigest(),'bytes':outside.stat().st_size}}
    refused_without_writes(env,selection(env,[record]))


def test_redirected_destination_parent_is_rejected_before_copy(env,tmp_path):
    _,base,root=env;outside=tmp_path/'outside';outside.mkdir();redirect=root/'docs'
    try:redirect.symlink_to(outside,target_is_directory=True)
    except OSError:pytest.skip('Symlink privilege unavailable on this host')
    record=entry(base)
    with pytest.raises((ValueError,RuntimeError,OSError)):
        env[0].execute(selection(env,[record]),base/'result.json',apply=True)
    assert list(outside.iterdir())==[]


@pytest.mark.parametrize('mode,tag',[(stat.S_IFLNK,0),(stat.S_IFDIR,0xA0000003)])
def test_dangling_or_reparse_destination_ancestor_preflight_uses_lstat(env,monkeypatch,mode,tag):
    mod,base,root=env;selections=selection(env,[entry(base)]);redirect=root/'docs'
    original=Path.lstat
    def observed(path,*args,**kwargs):
        if path==redirect:return SimpleNamespace(st_mode=mode,st_reparse_tag=tag)
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'lstat',observed)
    refused_without_writes(env,selections)


def test_exact_two_original_scorer_allowlist_reads_and_scans_without_execution(env):
    mod,base,_=env
    assert len(mod.ORIGINAL_HELPERS)==2
    assert {path.name for path in mod.ORIGINAL_HELPERS}=={'assess_v3.py','evaluate_frozen_baseline.py'}
    for path,expected in mod.ORIGINAL_HELPERS.items():
        raw=mod.read_bound(path,expected,base=base)
        assert mod.digest(raw)==expected
        mod.safety.audit_payload(PREFIX+'original_helpers/'+path.name,raw,set())
        compile(raw,path.name,'exec',dont_inherit=True)


def test_original_scorer_allowlist_does_not_accept_changed_expected_hash(env):
    mod,base,_=env
    for path in mod.ORIGINAL_HELPERS:
        with pytest.raises(ValueError,match='source_outside_declared_root'):
            mod.read_bound(path,'0'*64,base=base)


def test_allowlisted_basename_at_any_other_path_is_not_authorized(env,tmp_path):
    mod,base,_=env;outside=tmp_path/'assess_v3.py';outside.write_bytes(b'VALUE = 1\n')
    with pytest.raises(ValueError,match='source_outside_declared_root'):
        mod.read_bound(outside,mod.digest(outside.read_bytes()),base=base)
