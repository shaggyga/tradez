"""Offline release verification. No credentials, services, fitting or orders.

Run from any directory. The manifest covers portable source/artifacts/configs;
private historical data, broker state and an operational launch are separate.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.metadata
import json
from pathlib import Path, PurePosixPath
import platform
import sys

ROOT = Path(__file__).resolve().parent


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def verified_path(root, relative):
    part = PurePosixPath(relative)
    if part.is_absolute() or '..' in part.parts or '\\' in relative or ':' in relative:
        raise ValueError('unsafe_manifest_path')
    path = root.joinpath(*part.parts)
    if not path.resolve().is_relative_to(root.resolve()) or path.is_symlink():
        raise ValueError('path_outside_release')
    return path


def verify(root=ROOT):
    manifest_raw = (root/'RELEASE_MANIFEST.json').read_bytes()
    manifest = json.loads(manifest_raw)
    if manifest['schema_version'] != 'forex_weekend_setup_release_v1_20260911':
        raise ValueError('unsupported_release_schema')
    names = [row['path'] for row in manifest['files']]
    if len(names) != len(set(names)):
        raise ValueError('duplicate_manifest_path')
    failures, syntax_count, file_count, checked_objects = [], 0, 0, {}
    for item in manifest['files']:
        try:
            path = verified_path(root, item['path'])
            raw = path.read_bytes()
            if len(raw) != item['bytes'] or sha(raw) != item['sha256']:
                raise ValueError('file_content_changed')
            # Parse changed/new Python, not every unrelated historic entry point.
            if item['path'] in manifest['implementation_files'] and path.suffix == '.py':
                ast.parse(raw, filename=item['path'])
                syntax_count += 1
            file_count += 1
            if item['path'] in ('SETUP_PROFILES.json', 'src/config/practice007_joint_v3_20260909_v1.json'):
                checked_objects[item['path']] = raw
        except Exception as error:
            failures.append({'path': item['path'], 'error_type': type(error).__name__,
                             'reason': str(error)[:160]})
    profile = json.loads(checked_objects['SETUP_PROFILES.json'])
    if profile['orders_enabled'] is not False or profile['automatic_start'] is not False:
        failures.append({'path': 'SETUP_PROFILES.json', 'reason': 'inactive_setup_required'})
    practice = json.loads(checked_objects['src/config/practice007_joint_v3_20260909_v1.json'])
    seal = practice.pop('config_sha256')
    encoded = json.dumps(practice, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    if practice['enabled'] is not False or sha(encoded) != seal:
        failures.append({'path': 'practice config', 'reason': 'disabled_config_or_seal_invalid'})
    expected_practice_sources = {'oanda_practice_trial_runner_v1.py', 'oanda_practice_trial_broker_v1.py',
        'oanda_practice_forecast_adapter_v1.py', 'oanda_practice_trial_policy_v1.py',
        'oanda_live_account_readonly_status.py', 'oanda_trade_reconciliation_v1.py',
        'src/forex_system/contracts/signed_currency_exposure.py', 'oanda_practice_trial_runtime_v2.py',
        'practice_trial_runtime_vendor/immutable_order_preflight_v1.py'}
    if set(practice['source_bindings']) != expected_practice_sources:
        failures.append({'path': 'practice config', 'reason': 'complete_nine_source_inventory_required'})
    for name, expected in practice['source_bindings'].items():
        if sha(verified_path(root/'src', name).read_bytes()) != expected:
            failures.append({'path': name, 'reason': 'practice_source_binding_changed'})
    runtime = {}
    for name, expected in manifest['required_runtime_versions'].items():
        try:
            actual = sys.version.split()[0] if name == 'python' else importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            actual = None
        runtime[name] = {'expected': expected, 'actual': actual, 'matched': actual == expected}
    if (root/'RELEASE_MANIFEST.json').read_bytes() != manifest_raw:
        failures.append({'path': 'RELEASE_MANIFEST.json', 'reason': 'manifest_changed_during_verification'})
    return {'schema_version': 'forex_weekend_setup_verification_v1_20260911',
            'status': 'verified' if not failures and all(v['matched'] for v in runtime.values()) else 'failed',
            'manifest_sha256': sha(manifest_raw),
            'files_verified': file_count, 'files_expected': len(names),
            'implementation_python_parsed': syntax_count, 'failures': failures,
            'runtime': runtime, 'platform': platform.platform(),
            'verification_scope': 'file/source/config/runtime integrity; not market performance or live operational readiness',
            'services_started': False, 'broker_access': False, 'orders_enabled': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, help='Optional new verification receipt; existing files are never overwritten.')
    args = parser.parse_args(argv)
    try:
        result = verify()
    except Exception as error:
        result = {'status': 'failed', 'error_type': type(error).__name__, 'reason': str(error)[:200]}
    payload = json.dumps(result, indent=2)+'\n'
    if args.output:
        with args.output.open('x', encoding='utf-8') as stream:
            stream.write(payload)
    print(payload)
    return 0 if result['status'] == 'verified' else 1


if __name__ == '__main__':
    raise SystemExit(main())
