"""Copy the exact selected 46-source registry into a fresh synthetic pytest kit.

The project is read-only. All fixtures, SQLite writes and proofs are under the
new owned output directory. No trading process or network action is permitted
inside pytest. Existing source cohorts and ledgers are never opened by a fixture.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

PACKAGE = Path(__file__).resolve().parent
WORKER = 'oanda_joint_price_news_forecast_study_v7.py'
TESTS = ('test_core_operational_v2.py', 'test_full_operational_v2.py',
         'test_async_bootstrap.py', 'test_news_completion_handoff.py',
         'test_packed_archive_v5.py', 'test_compact_canonical_encoding_v6.py',
         'test_archive_layout_reuse_v7.py')
EXPECTED_TEST_COUNT = 48


def need(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def plain(path):
    path = Path(path).absolute()
    for part in (path, *path.parents):
        need(not part.is_symlink() and not part.is_junction(), 'reparse_path_refused')
    return path


def read(path, cap):
    path = plain(path)
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        raw = stream.read(cap + 1)
        after = os.fstat(stream.fileno())
    need(0 < len(raw) <= cap and (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
         == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), 'bounded_unchanged_read_required')
    return raw


def save(path, value):
    raw = json.dumps(value, indent=2, sort_keys=True, allow_nan=False).encode()
    with path.open('xb') as stream:
        stream.write(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project-root', type=Path, default=PACKAGE.parents[2])
    parser.add_argument('--registry', type=Path, help='Optional explicit registry beneath project/config; default is the current joint dashboard selection.')
    parser.add_argument('--output-parent', type=Path, default=PACKAGE / 'runs')
    parser.add_argument('--prepare-only', action='store_true', help='Copy verified sources/fixtures without invoking pytest.')
    args = parser.parse_args()
    project = plain(args.project_root).resolve()
    need(project.is_dir() and (project / WORKER).is_file(), 'actual_project_root_required')
    package_manifest = json.loads(read(PACKAGE / 'REPRODUCTION_PACKAGE.json', 64 * 1024))
    need(package_manifest.get('test_count_expected') == EXPECTED_TEST_COUNT,
         'portable_test_inventory_count_changed')
    fixture_raw = {}
    for name, expected in package_manifest['fixture_sha256'].items():
        need(type(name) is str and re.fullmatch('[A-Za-z0-9_]+\\.py', name), 'fixture_filename_required')
        raw = read(PACKAGE / 'fixtures' / name, 2 * 1024**2)
        need(sha(raw) == expected, 'reproduction_fixture_changed:' + name)
        fixture_raw[name] = raw
    need(set(TESTS).issubset(fixture_raw) and {'sitecustomize.py', 'conftest.py', 'baseline_worker_stage006.py'}.issubset(fixture_raw),
         'complete_portable_fixture_set_required')
    selected_sha = None
    pointer_raw = None
    if args.registry is None:
        pointer_raw = read(project / 'config/operational_dashboard_current.json', 16 * 1024)
        joint = json.loads(pointer_raw)['joint']
        selected = Path(joint['registry_path'])
        need(not selected.is_absolute() and '..' not in selected.parts, 'relative_current_registry_required')
        registry = plain(project / selected).resolve()
        selected_sha = joint['registry_sha256']
    else:
        registry = plain(args.registry if args.registry.is_absolute() else project / args.registry).resolve()
    need(registry.is_relative_to(project / 'config'), 'registry_must_be_beneath_project_config')
    registry_raw = read(registry, 2 * 1024**2)
    need(selected_sha is None or sha(registry_raw) == selected_sha, 'selected_registry_digest_changed')
    registered = json.loads(registry_raw)
    need(registered.get('schema_version') == 'joint_price_news_registry_v7_20260913', 'native_v7_registry_required')
    bindings = registered.get('source_bindings')
    need(type(bindings) is dict and len(bindings) == 46 and WORKER in bindings
         and 'compact_projection_store_v1.py' in bindings, 'compact_native_46_source_registry_required')
    sources = {}
    for name, expected in bindings.items():
        need(type(name) is str and re.fullmatch('[A-Za-z0-9_]+\\.py', name)
             and type(expected) is str and re.fullmatch('[a-f0-9]{64}', expected), 'source_binding_shape')
        raw = read(project / name, 2 * 1024**2)
        need(sha(raw) == expected, 'actual_registered_source_changed:' + name)
        sources[name] = raw
    tree = ast.parse(sources[WORKER])
    inventories = [ast.literal_eval(node.value.args[0]) for node in tree.body
                   if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'REQUIRED_SOURCE_BINDINGS' for t in node.targets)
                   and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
                   and node.value.func.id == 'frozenset' and len(node.value.args) == 1]
    need(len(inventories) == 1 and set(inventories[0]) == set(bindings), 'exact_worker_required_inventory')
    output_parent = plain(args.output_parent)
    output_parent.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix='native-news-compact-', dir=output_parent))
    kit = output / 'kit'
    kit.mkdir()
    for name, raw in {**sources, **fixture_raw}.items():
        with (kit / name).open('xb') as stream:
            stream.write(raw)
        need(read(kit / name, 2 * 1024**2) == raw, 'isolated_source_copy_mismatch')
    # A second complete read closes a concurrent edit window before pytest starts.
    for name, raw in sources.items():
        need(read(project / name, 2 * 1024**2) == raw, 'project_source_changed_during_copy:' + name)
    need(read(registry, 2 * 1024**2) == registry_raw, 'registry_changed_during_copy')
    save(kit / 'SOURCE_KIT_INVENTORY_003.json', {'scope': 'Fresh synthetic integration kit; no live runtime or market authority',
        'files': [{'name': name, 'sha256': digest} for name, digest in sorted(bindings.items())]})
    (output / 'selected_registry.json').write_bytes(registry_raw)
    if pointer_raw is not None:
        (output / 'selected_pointer.json').write_bytes(pointer_raw)
    preparation = {'status': 'prepared_not_executed', 'prepared_utc': datetime.now(timezone.utc).isoformat(),
                   'project_root': str(project), 'selected_registry': str(registry),
                   'registry_file_sha256': sha(registry_raw), 'source_bindings': bindings,
                   'fixture_sha256': package_manifest['fixture_sha256'], 'python_executable': sys.executable,
                   'network_and_child_processes_blocked_inside_pytest': True,
                   'real_model_fits': False, 'actual_runtime_activation': False,
                   'live_database_read_or_write_requested': False, 'output': str(output)}
    save(output / 'PREPARATION.json', preparation)
    print(json.dumps({'prepared': str(output), 'source_count': len(sources), 'test_count_expected': EXPECTED_TEST_COUNT}), flush=True)
    if args.prepare_only:
        return 0
    env = os.environ.copy()
    env['PYTHONPATH'] = str(kit)
    env['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '1'
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    command = [sys.executable, '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
               *TESTS, '--basetemp=' + str(output / 'synthetic'), '--junitxml=' + str(output / 'INTEGRATION.xml')]
    completed = subprocess.run(command, cwd=kit, env=env, check=False)
    xml_error = None
    cases = []
    try:
        cases = list(ET.parse(output / 'INTEGRATION.xml').iter('testcase'))
    except (OSError, ET.ParseError) as exc:
        xml_error = type(exc).__name__ + ':' + str(exc)
    incomplete = sum(any(case.find(tag) is not None for tag in ('failure', 'error', 'skipped')) for case in cases)
    passed = completed.returncode == 0 and xml_error is None and len(cases) == EXPECTED_TEST_COUNT and incomplete == 0
    save(output / 'RESULT.json', {'status': 'passed' if passed else 'failed',
         'pytest_exit_code': completed.returncode, 'finished_utc': datetime.now(timezone.utc).isoformat(),
         'test_count_expected': EXPECTED_TEST_COUNT, 'test_count_observed': len(cases),
         'failed_error_or_skipped_cases': incomplete, 'xml_error': xml_error,
         'scope': '48 synthetic integration, scheduling, storage, canonical-byte and layout tests; no forecast profitability or live-market qualification',
         'source_registry_file_sha256': sha(registry_raw), 'preparation': 'PREPARATION.json'})
    return 0 if passed else (completed.returncode or 1)


if __name__ == '__main__':
    raise SystemExit(main())
