"""Isolated final-assembler boundaries; no project, vault, or broker actions."""
from copy import deepcopy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest


SOURCE = Path(__file__).with_name('assemble_final_validation_v1.py')
spec = importlib.util.spec_from_file_location('isolated_final_assembler', SOURCE)
assembler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assembler)


def encoded(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


@pytest.fixture
def case(tmp_path, monkeypatch):
    base = tmp_path / 'evidence'
    root = tmp_path / 'project'
    base.mkdir(); root.mkdir()
    monkeypatch.setattr(assembler, 'BASE', base)
    monkeypatch.setattr(assembler, 'ROOT', root)
    report = root / 'docs' / 'report.md'
    report.parent.mkdir()
    report.write_bytes(b'Dated research evidence; remaining work retained.\n')
    output = root / 'validation.json'
    monkeypatch.setattr(assembler, 'REPORT', report)
    monkeypatch.setattr(assembler, 'OUTPUT', output)
    (root / 'forex_model_vault_sync.py').write_bytes(b'# existence anchor only\n')
    member = assembler.PREFIX + 'synthetic/receipt.json'
    copied_member = root / member
    copied_member.parent.mkdir(parents=True)
    copied_member.write_bytes(b'{"status":"synthetic"}\n')
    dependency = root / 'pure_dependency.py'
    dependency.write_bytes(b'RESEARCH_ONLY = True\n')
    bindings = {}
    registry_paths = []
    source_paths = []
    for index, label in enumerate(('joint_v3', 'second_curve_pilot', 'paper_management', 'm1_risk_distributions')):
        source = root / ('source_%s.py' % index)
        source.write_bytes(('VALUE = %s\n' % index).encode())
        registry = dict(source_bindings={source.name: digest(source.read_bytes())}, research_only=True,
                        orders_enabled=False, controls={'can_place_orders': False, 'can_promote': False})
        path = root / 'config' / (label + '.json')
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(encoded(registry))
        bindings[label] = (path.relative_to(root).as_posix(), digest(path.read_bytes()), 1)
        registry_paths.append(path); source_paths.append(source)
    monkeypatch.setattr(assembler, 'REGISTRIES', bindings)
    copied = dict(status='copied_exact_members', all_source_hashes_unchanged=True,
                  credential_pattern_and_known_value_scan_passed=True, selected_python_compile_passed=True,
                  entries=[dict(intended_member=member, original_source=dict(path=str(base / 'original.json'),
                      sha256=digest(copied_member.read_bytes()), bytes=copied_member.stat().st_size), kind='receipt')],
                  canonical_source_dependencies=[dict(path=str(dependency), sha256=digest(dependency.read_bytes()),
                      bytes=dependency.stat().st_size)])
    specification = dict(evidence=[dict(label='dated observation', archive_member=member,
                         sha256=digest(copied_member.read_bytes()))], remaining_gaps=['No profitability established.'])
    copy_path = base / 'copy.json'
    spec_path = base / 'spec.json'

    def bind():
        copy_path.write_bytes(encoded(copied)); spec_path.write_bytes(encoded(specification))
        argv = ['assembler', '--copy-receipt', str(copy_path), '--expected-copy-sha256', digest(copy_path.read_bytes()),
                '--specification', str(spec_path), '--expected-specification-sha256', digest(spec_path.read_bytes()),
                '--expected-report-sha256', digest(report.read_bytes())]
        monkeypatch.setattr(sys, 'argv', argv)
        return argv

    bind()
    return dict(base=base, root=root, report=report, output=output, copied=copied, specification=specification,
                copy_path=copy_path, spec_path=spec_path, member=member, copied_member=copied_member,
                dependency=dependency, registry_paths=registry_paths, source_paths=source_paths, bind=bind)


def reject_without_output(case, reason=None):
    with pytest.raises((ValueError, FileNotFoundError), match=reason):
        assembler.main()
    assert not case['output'].exists()


def test_complete_isolated_inventory_embeds_original_exact_bindings(case, capsys):
    original_copy = deepcopy(case['copied'])
    assembler.main()
    value = json.loads(case['output'].read_bytes())
    assert value['status'] == 'verification_complete_publication_separate'
    assert len(value['source_closures']) == 4
    assert value['portable_evidence']['entries'] == original_copy['entries']
    assert value['portable_evidence']['canonical_source_dependencies'] == original_copy['canonical_source_dependencies']
    assert value['selected_result_evidence'][0]['sha256'] == original_copy['entries'][0]['original_source']['sha256']
    assert value['report']['sha256'] == digest(case['report'].read_bytes())
    assert value['started_epoch'] <= value['completed_epoch']
    assert value['orders_enabled'] is value['manager_activation'] is value['publication_performed'] is False
    printed = json.loads(capsys.readouterr().out)
    assert printed['sha256'] == digest(case['output'].read_bytes())


@pytest.mark.parametrize('flag', ['all_source_hashes_unchanged', 'credential_pattern_and_known_value_scan_passed', 'selected_python_compile_passed'])
def test_copy_verification_flags_required_before_output(case, flag):
    case['copied'][flag] = False; case['bind']()
    reject_without_output(case, 'copy_verification_required')


def test_dry_copy_receipt_cannot_claim_completed_copy(case):
    case['copied']['status'] = 'preflight_passed_no_copy'; case['bind']()
    reject_without_output(case, 'actual_copy_required')


@pytest.mark.parametrize('key', ['copy_path', 'spec_path', 'report', 'copied_member', 'dependency'])
def test_bound_inputs_changed_before_read_reject(case, key):
    case[key].write_bytes(b'changed retained bytes\n')
    reject_without_output(case, 'bound_input_changed')


@pytest.mark.parametrize('group', ['registry_paths', 'source_paths'])
def test_any_frozen_closure_change_rejects(case, group):
    case[group][2].write_bytes(b'changed frozen bytes\n')
    reject_without_output(case, 'bound_input_changed')


def test_missing_copy_member_rejects(case):
    # The declared path never existed; no deletion is needed for this fixture.
    name = assembler.PREFIX + 'synthetic/missing.json'
    case['copied']['entries'][0]['intended_member'] = name
    case['specification']['evidence'][0]['archive_member'] = name
    case['bind']()
    reject_without_output(case)


def test_copied_size_mismatch_rejects(case):
    case['copied']['entries'][0]['original_source']['bytes'] += 1; case['bind']()
    reject_without_output(case, 'copied_member_size_changed')


@pytest.mark.parametrize('bad', ['../outside.json', assembler.PREFIX + '../outside.json', assembler.PREFIX + 'x//y.json',
                                 assembler.PREFIX + 'x\\y.json', assembler.PREFIX + 'C:stream'])
def test_member_paths_reject_traversal_or_ambiguous_separator(case, bad):
    case['copied']['entries'][0]['intended_member'] = bad; case['bind']()
    reject_without_output(case, 'evidence_member_scope')


def test_case_colliding_members_reject_before_reads(case):
    row = deepcopy(case['copied']['entries'][0]); row['intended_member'] = row['intended_member'].upper()
    # Keep the fixed prefix while colliding in the filesystem-relative suffix.
    row['intended_member'] = assembler.PREFIX + 'SYNTHETIC/RECEIPT.JSON'
    case['copied']['entries'].append(row); case['bind']()
    reject_without_output(case, 'duplicate_member')


@pytest.mark.parametrize('change', ['hash', 'missing', 'case'])
def test_selected_reference_must_match_exact_verified_member(case, change):
    row = case['specification']['evidence'][0]
    if change == 'hash': row['sha256'] = 'a' * 64
    elif change == 'missing': row['archive_member'] = assembler.PREFIX + 'missing.json'
    else: row['archive_member'] = assembler.PREFIX + 'SYNTHETIC/RECEIPT.JSON'
    case['bind']()
    reject_without_output(case, 'reference_not_in_verified_copy')


def test_canonical_dependency_traversal_rejects_before_outside_read(case, monkeypatch):
    outside = case['root'].parent / 'outside.py'
    outside.write_bytes(b'OUTSIDE = True\n')
    malicious = case['root'] / '..' / outside.name
    case['copied']['canonical_source_dependencies'][0] = dict(path=str(malicious), sha256=digest(outside.read_bytes()), bytes=outside.stat().st_size)
    case['bind']()
    original = assembler.read_bound
    def watched(path, *args, **kwargs):
        assert Path(path).resolve() != outside.resolve(), 'outside_dependency_read'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(assembler, 'read_bound', watched)
    reject_without_output(case, 'canonical_dependency_scope')


@pytest.mark.parametrize('key', assembler.AUTHORITY_FIELDS)
def test_nested_positive_authority_rejects(key):
    with pytest.raises(ValueError, match='research_authority_changed'):
        assembler.check_authority({'nested': [{'nested': {key: True}}]})


@pytest.mark.parametrize('bad', [0, None, 'false'])
def test_authority_requires_exact_false(bad):
    with pytest.raises(ValueError, match='research_authority_changed'):
        assembler.check_authority({'orders_enabled': bad})


@pytest.mark.parametrize('key', ['copy_path', 'spec_path', 'report', 'copied_member', 'dependency', 'registry', 'closure_source'])
def test_mutation_after_initial_read_is_detected_before_output(case, monkeypatch, key):
    target = case['registry_paths'][0] if key == 'registry' else case['source_paths'][0] if key == 'closure_source' else case[key]
    original = assembler.read_bound
    changed = False
    def mutate_after_read(path, *args, **kwargs):
        nonlocal changed
        raw = original(path, *args, **kwargs)
        if Path(path) == target and not changed:
            target.write_bytes(raw + b' '); changed = True
        return raw
    monkeypatch.setattr(assembler, 'read_bound', mutate_after_read)
    reject_without_output(case, 'bound_input_changed')
    assert changed


def test_existing_output_is_retained_and_never_overwritten(case):
    original = b'prior validation evidence\n'; case['output'].write_bytes(original)
    with pytest.raises(ValueError, match='fresh_validation_required'): assembler.main()
    assert case['output'].read_bytes() == original


def test_concurrent_output_creation_cannot_be_overwritten(case, monkeypatch, capsys):
    original_open = Path.open
    competing = b'concurrent retained evidence\n'
    def create_before_exclusive(path, mode='r', *args, **kwargs):
        if path == case['output'] and mode == 'xb':
            with original_open(path, 'xb') as stream: stream.write(competing)
        return original_open(path, mode, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', create_before_exclusive)
    with pytest.raises(FileExistsError): assembler.main()
    assert case['output'].read_bytes() == competing
    assert not capsys.readouterr().out


def test_sync_failure_retains_unaccepted_orphan_and_emits_no_success(case, monkeypatch, capsys):
    def fail_sync(_): raise OSError('synthetic_sync_failure')
    monkeypatch.setattr(assembler.os, 'fsync', fail_sync)
    with pytest.raises(OSError, match='synthetic_sync_failure'): assembler.main()
    assert case['output'].exists()
    assert not capsys.readouterr().out
    with pytest.raises(ValueError, match='fresh_validation_required'): assembler.main()


def test_bounded_read_never_requests_unlimited_bytes(tmp_path, monkeypatch):
    path = tmp_path / 'bounded.json'; path.write_bytes(b'{}')
    original_open = Path.open
    requests = []
    class Tracking(io.BytesIO):
        def read(self, size=-1):
            requests.append(size)
            assert 0 <= size <= 9, 'unbounded_read_request'
            return super().read(size)
    def open_bounded(target, mode='r', *args, **kwargs):
        if target == path and mode == 'rb': return Tracking(b'0123456789abcdef')
        return original_open(target, mode, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', open_bounded)
    with pytest.raises(ValueError, match='bound_input_changed|input_size_bound'):
        assembler.read_bound(path, digest(b'{}'), maximum=8)
    assert requests


def test_reparse_point_is_rejected_by_lstat_even_without_symlink_privilege(tmp_path, monkeypatch):
    path = tmp_path / 'redirect.json'; path.write_bytes(b'{}')
    original = Path.lstat
    class Info:
        st_mode = 0o100644
        st_file_attributes = 0x400
    monkeypatch.setattr(Path, 'lstat', lambda target: Info() if target == path else original(target))
    with pytest.raises(ValueError, match='redirected_path'): assembler.regular(path)


@pytest.mark.parametrize('clock_values', [(20.0, 19.0), (float('nan'), 20.0), (10.0, float('inf')), (0.0, 20.0), (-1.0, 20.0)])
def test_invalid_or_reversed_verification_clock_withholds_output(case, monkeypatch, clock_values):
    samples = iter(clock_values)
    monkeypatch.setattr(assembler, 'time', SimpleNamespace(time=lambda: next(samples)))
    reject_without_output(case, 'validation_clock_integrity')


def test_equal_actual_clock_samples_are_preserved(case, monkeypatch):
    monkeypatch.setattr(assembler, 'time', SimpleNamespace(time=lambda: 20.0))
    assembler.main()
    value = json.loads(case['output'].read_bytes())
    assert value['started_epoch'] == value['completed_epoch'] == 20.0


def test_corrupted_readback_withholds_success_and_preserves_evidence(case, monkeypatch, capsys):
    original_sync = assembler.os.fsync
    def corrupt_after_sync(handle):
        original_sync(handle)
        # A separate local writer changes only this disposable output after sync.
        case['output'].write_bytes(b'synthetic readback corruption\n')
    monkeypatch.setattr(assembler.os, 'fsync', corrupt_after_sync)
    with pytest.raises(ValueError, match='bound_input_changed|validation_persisted_bytes_changed'):
        assembler.main()
    assert case['output'].read_bytes() == b'synthetic readback corruption\n'
    assert not capsys.readouterr().out


def test_nested_registry_authority_change_blocks_full_assembly(case):
    path = case['registry_paths'][0]
    value = json.loads(path.read_bytes()); value['controls']['can_promote'] = True
    path.write_bytes(encoded(value))
    name, _, count = assembler.REGISTRIES['joint_v3']
    assembler.REGISTRIES['joint_v3'] = (name, digest(path.read_bytes()), count)
    reject_without_output(case, 'research_authority_changed')
