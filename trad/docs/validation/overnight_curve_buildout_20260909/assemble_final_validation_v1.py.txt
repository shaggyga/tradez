"""Bind the final report to copied evidence and unchanged research source closures.

This verifies existing local artifacts. It does not rescore results, authorize
orders, control processes, or publish anything to the vault.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import time

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
REPORT = ROOT / 'docs/FOREX_OVERNIGHT_CURVE_BUILDOUT_20260909.md'
OUTPUT = ROOT / 'FOREX_OVERNIGHT_CURVE_BUILDOUT_VALIDATION_20260909.json'
PREFIX = 'docs/validation/overnight_curve_buildout_20260909/'
REGISTRIES = {
    'joint_v3': ('config/joint_price_news_study_v3_20260908.json',
        'ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771', 20),
    'second_curve_pilot': ('config/recovered_second_curve_pilot_v1_20260909.json',
        'ae64f2cae6df44dad81b46deb16e95e4d5f67dabb7fc1e65289b96ba6b0fe2b2', 14),
    'paper_management': ('config/observed_curve_management_v1_20260909.json',
        '60689101fb63a8bcce2465d19f627b9b2ce357d1ee271c65298f255663d991e1', 20),
    'm1_risk_distributions': ('config/m1_risk_distributions_v1_20260909.json',
        '855c3bb96105c0cf0943ebbb5d28e6d10b6b9bf718573883f897161d08453674', 8),
}
AUTHORITY_FIELDS = ('orders_enabled', 'can_place_orders', 'can_promote',
    'can_authorize', 'account_eligible', 'execution_eligible', 'proof_eligible',
    'manager_activation', 'positions_managed', 'broker_access', 'account_access')


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def regular(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        info = part.lstat()
        need(not stat.S_ISLNK(info.st_mode) and
             not (getattr(info, 'st_file_attributes', 0) & 0x400), 'redirected_path')
    need(path.is_file(), 'regular_file_required')
    return path


def read_bound(path, expected, maximum=4 * 1024 * 1024):
    path = regular(path)
    need(path.stat().st_size <= maximum, 'input_size_bound')
    with path.open('rb') as handle:
        raw = handle.read(maximum + 1)
    need(len(raw) <= maximum and sha(raw) == expected, 'bound_input_changed')
    return raw


def safe_member(name):
    need(isinstance(name, str) and name.startswith(PREFIX) and '\\' not in name
         and ':' not in name and all(x not in ('', '.', '..') for x in name.split('/')),
         'evidence_member_scope')
    return name


def check_authority(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in AUTHORITY_FIELDS:
                need(item is False, 'research_authority_changed')
            check_authority(item)
    elif isinstance(value, list):
        for item in value:
            check_authority(item)


def checked_inventory(copied):
    need(copied.get('status') == 'copied_exact_members', 'actual_copy_required')
    for flag in ('all_source_hashes_unchanged', 'credential_pattern_and_known_value_scan_passed',
                 'selected_python_compile_passed'):
        need(copied.get(flag) is True, 'copy_verification_required')
    entries = copied.get('entries')
    need(isinstance(entries, list) and 0 < len(entries) <= 1200, 'inventory_bound')
    by_name = {}
    for row in entries:
        name = safe_member(row['intended_member'])
        need(name.casefold() not in by_name, 'duplicate_member')
        source = row['original_source']
        need(isinstance(source['sha256'], str) and len(source['sha256']) == 64 and
             isinstance(source['bytes'], int) and not isinstance(source['bytes'], bool) and
             0 <= source['bytes'] <= 4 * 1024 * 1024, 'member_binding')
        by_name[name.casefold()] = row
    return by_name


def checked_references(spec, by_name):
    references = spec.get('evidence')
    need(isinstance(references, list) and 1 <= len(references) <= 100, 'evidence_reference_bound')
    need(isinstance(spec.get('remaining_gaps'), list) and len(spec['remaining_gaps']) > 0,
         'remaining_gaps_required')
    labels = set()
    result = []
    for row in references:
        need(isinstance(row.get('label'), str) and row['label'] and row['label'] not in labels,
             'evidence_label')
        labels.add(row['label'])
        name = safe_member(row['archive_member'])
        actual = by_name.get(name.casefold())
        need(actual is not None and actual['intended_member'] == name and
             actual['original_source']['sha256'] == row['sha256'], 'reference_not_in_verified_copy')
        result.append(dict(row, bytes=actual['original_source']['bytes']))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--copy-receipt', type=Path, required=True)
    parser.add_argument('--expected-copy-sha256', required=True)
    parser.add_argument('--specification', type=Path, required=True)
    parser.add_argument('--expected-specification-sha256', required=True)
    parser.add_argument('--expected-report-sha256', required=True)
    args = parser.parse_args()
    started = time.time()
    copy_path, spec_path = args.copy_receipt.absolute(), args.specification.absolute()
    need(copy_path.parent == BASE and spec_path.parent == BASE, 'input_scope')
    need(not OUTPUT.exists() and not OUTPUT.is_symlink(), 'fresh_validation_required')
    helper = Path(__file__).read_bytes()
    copy_raw = read_bound(copy_path, args.expected_copy_sha256)
    spec_raw = read_bound(spec_path, args.expected_specification_sha256)
    report_raw = read_bound(REPORT, args.expected_report_sha256)
    copied, spec = json.loads(copy_raw), json.loads(spec_raw)
    by_name = checked_inventory(copied)
    references = checked_references(spec, by_name)
    for row in copied['entries']:
        source = row['original_source']
        raw = read_bound(ROOT / row['intended_member'], source['sha256'])
        need(len(raw) == source['bytes'], 'copied_member_size_changed')
    dependencies = copied.get('canonical_source_dependencies', [])
    for row in dependencies:
        path = Path(row['path']).absolute()
        need(path.is_relative_to(ROOT) and '..' not in path.parts,
             'canonical_dependency_scope')
        need(len(read_bound(path, row['sha256'])) == row['bytes'], 'canonical_dependency_size')
    closures = {}
    for label, (name, expected, count) in REGISTRIES.items():
        registry_raw = read_bound(ROOT / name, expected)
        registry = json.loads(registry_raw)
        check_authority(registry)
        bindings = registry['source_bindings']
        need(isinstance(bindings, dict) and len(bindings) == count, 'source_closure_count')
        verified = []
        for source_name, source_sha in bindings.items():
            path = (ROOT / source_name).absolute()
            need(path.is_relative_to(ROOT) and '..' not in Path(source_name).parts,
                 'source_closure_scope')
            raw = read_bound(path, source_sha)
            verified.append(dict(path=source_name, sha256=source_sha, bytes=len(raw)))
        closures[label] = dict(registry=name, registry_sha256=expected,
            source_count=count, source_bindings=verified, all_match=True,
            research_authority_flags_verified_disabled=True)
    # Check every bound input again at the end, so concurrent edits are not hidden.
    read_bound(copy_path, args.expected_copy_sha256)
    read_bound(spec_path, args.expected_specification_sha256)
    read_bound(REPORT, args.expected_report_sha256)
    for row in copied['entries']:
        read_bound(ROOT / row['intended_member'], row['original_source']['sha256'])
    for row in dependencies:
        read_bound(row['path'], row['sha256'])
    for closure in closures.values():
        read_bound(ROOT / closure['registry'], closure['registry_sha256'])
        for row in closure['source_bindings']:
            read_bound(ROOT / row['path'], row['sha256'])
    need(Path(__file__).read_bytes() == helper, 'validation_helper_changed')
    completed = time.time()
    need(math.isfinite(started) and math.isfinite(completed) and 0 < started <= completed,
         'validation_clock_integrity')
    value = dict(schema_version='forex_overnight_curve_buildout_validation_v1_20260909',
        status='verification_complete_publication_separate', started_epoch=started,
        completed_epoch=completed, helper_sha256=sha(helper),
        report=dict(path=str(REPORT), sha256=sha(report_raw), bytes=len(report_raw)),
        specification=dict(path=str(spec_path), sha256=sha(spec_raw)),
        source_closures=closures, selected_result_evidence=references,
        portable_evidence=dict(project_copy_receipt=dict(path=str(copy_path), sha256=sha(copy_raw)),
            entries=copied['entries'], canonical_source_dependencies=dependencies,
            exact_copy_inventory_embedded=True, all_copied_members_reverified=True,
            total_files=len(copied['entries']),
            total_bytes=sum(r['original_source']['bytes'] for r in copied['entries'])),
        remaining_gaps=spec['remaining_gaps'], orders_enabled=False,
        broker_requests=False, manager_activation=False, publication_performed=False,
        limitations=['This receipt verifies the recorded artifacts, not predictive profitability.',
            'Dated evidence has its original observation cutoff; it is not a synchronized live snapshot.',
            'The separately verified vault source pointer and publication receipt identify the final archive.',
            'Retained runtime databases, private news inputs and model weights remain external dependencies.'])
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    need(len(raw) <= 4 * 1024 * 1024, 'validation_output_bound')
    regular(OUTPUT.parent / 'forex_model_vault_sync.py')
    with OUTPUT.open('xb') as handle:
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    need(read_bound(OUTPUT, sha(raw)) == raw, 'validation_persisted_bytes_changed')
    print(json.dumps(dict(path=str(OUTPUT), sha256=sha(raw), bytes=len(raw),
        status=value['status'], copied_files=len(copied['entries']),
        closures_verified=len(closures), selected_evidence=len(references))))


if __name__ == '__main__':
    main()
