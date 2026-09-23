"""Build a scoped operational source/evidence checkpoint; never copy runtime DBs.

Inventory is read-only by default. Publishing requires its exact inventory hash.
Existing checkpoints are never overwritten. Private runtime data is explicitly
an external dependency; this package is not a complete service backup.
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.vault_worktree_snapshot import (
    audit_payload, credentials, excluded_reason, known_private_values, regular_file, safe_name,
)

EVIDENCE_ROOTS = ('docs/validation/rolling_operations_20260916',
                  'docs/validation/news_incremental_20260916')
EXTRA_FILES = {
    'README.md', 'FOREX_PENDING_IMPROVEMENTS.md', 'docs/FOREX_OPERATIONAL_REPAIR_20260916.md',
    'oanda_operational_recovery_contract_v2.ps1', 'oanda_operational_supervisor_v2.ps1',
    'oanda_supervisor_watchdog_v2.ps1', 'start_oanda_operational_research_v2.ps1',
    'start_oanda_supervisor_watchdog_v2.ps1', 'oanda_operational_log_relay_v1.py',
    'tools/publish_operational_checkpoint_v1.py',
    'tools/check_rolling_alignment_live_v2.py', 'tools/check_rolling_operations_resume_v2.py',
    'tools/check_rolling_operations_live_v2.py',
}
# Selected regression tests read these PowerShell sources as data, which Python
# AST import closure cannot discover. Preserve failed-generation recreation too.
EXTRA_FILES.update(f'{stem}_v{version}.ps1' for version in (3, 4) for stem in (
    'oanda_operational_recovery_contract', 'oanda_operational_supervisor',
    'oanda_supervisor_watchdog', 'start_oanda_operational_research', 'start_oanda_supervisor_watchdog'))
TEST_PATTERNS = ('test_oanda_rolling_technical_*v*.py', 'test_oanda_feature_forward_*v2*.py',
                 'test_*incremental*v2*.py', 'test_*recovery*v2*.py', 'test_*recovery*v3*.py',
                 'test_*recovery*v4*.py',
                 'test_*operational*supervisor*.py', 'test_*log_relay*.py',
                 'test_*revision*transport*v6*.py', 'test_*joint*news*v8*.py',
                 'test_revision_news_descendants_v3.py', 'test_revision_news_resumable_v3.py',
                 'test_publish_operational_checkpoint_v1.py')
EXCLUDED_EVIDENCE_DIRS = {'archive', 'archives', 'capture_archive', 'cache', 'validation_cache',
                         'semantic_cache', '__pycache__', '.pytest_cache', 'raw'}
MAX_FILE_BYTES = 16 * 1024**2
MAX_TOTAL_BYTES = 256 * 1024**2
MAX_FILES = 8192
PRIVATE_EXTERNAL_SOURCES = frozenset({'oanda_arima_canary_executor.py',
    'oanda_gpt_training_strategy_manager.py', 'oanda_model_space_agenda.py',
    'oanda_trainer_reporting_extensions.py'})


def is_hash(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def is_link(info):
    return stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_reparse_tag', 0) & 0x20000000)


def reject_link_chain(path):
    # Do not resolve first: resolving would hide a symlink/junction alias.
    for candidate in (path, *path.parents):
        if candidate.exists() or candidate.is_symlink():
            if is_link(candidate.lstat()):
                raise ValueError('link/reparse alias is prohibited')


def is_evidence(name):
    return any(name.startswith(folder + '/') for folder in EVIDENCE_ROOTS)


def admitted_name(name):
    safe_name(name)
    if is_evidence(name):
        parts = Path(name).parts
        lowered = Path(name).name.lower()
        if (any(p.lower() in EXCLUDED_EVIDENCE_DIRS for p in parts)
                or any(s in lowered for s in ('stdout', 'stderr'))
                or lowered.endswith(('.out.log', '.err.log'))):
            raise ValueError('raw/cache/redirected evidence is excluded: ' + name)
        # Only bounded textual audit evidence, including completed test/run logs.
        probe = name[:-4] + '.txt' if name.lower().endswith('.log') else name
        if Path(name).suffix.lower() not in {'.json', '.md', '.log', '.py', '.txt', '.xml'} or excluded_reason(probe):
            raise ValueError('non-text or private evidence is excluded: ' + name)
    elif excluded_reason(name):
        raise ValueError('private/runtime/non-source member is excluded: ' + name)
    return name


def local_reference(value, *, required=False):
    path = Path(value)
    if not path.is_absolute():
        try:
            safe_name(value.replace('\\', '/'))
        except RuntimeError:
            if required:
                raise
            # Human prose, glob-like process needles and external ../ references
            # are not local payload declarations. Pins may never use this escape.
            return None
        path = ROOT / path
    try:
        name = path.relative_to(ROOT).as_posix()
    except ValueError:
        if required:
            raise ValueError('pinned dependency is outside the scoped project') from None
        return None
    # Runtime JSON references describe external dependencies, not payloads.
    if name.split('/')[0].lower() == 'data' and not required:
        return None
    admitted_name(name)
    reject_link_chain(path)
    if not path.is_file():
        if required:
            raise ValueError('missing source/config dependency: ' + name)
        return None
    return name


def read_stable(name):
    path = regular_file(ROOT, admitted_name(name))
    before = path.stat()
    if before.st_size > MAX_FILE_BYTES:
        raise ValueError('oversized member requires explicit review: ' + name)
    with path.open('rb') as stream:
        raw = stream.read(MAX_FILE_BYTES + 1)
    after = regular_file(ROOT, name).stat()
    identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
    if len(raw) > MAX_FILE_BYTES or identity(before) != identity(after) or len(raw) != after.st_size:
        raise ValueError('member changed while reading: ' + name)
    return raw


def evidence_files(folder):
    base = ROOT / folder
    reject_link_chain(base)
    if not base.exists():
        return
    stack = [base]
    visited_entries = 0
    while stack:
        directory = stack.pop()
        reject_link_chain(directory)
        with os.scandir(directory) as entries:
            for entry in entries:
                visited_entries += 1
                if visited_entries > 32768:
                    raise ValueError('evidence traversal exceeds bounded inventory')
                info = entry.stat(follow_symlinks=False)
                if is_link(info):
                    continue
                path = Path(entry.path)
                if stat.S_ISDIR(info.st_mode):
                    if entry.name.lower() not in EXCLUDED_EVIDENCE_DIRS:
                        stack.append(path)
                elif stat.S_ISREG(info.st_mode):
                    name = path.relative_to(ROOT).as_posix()
                    try:
                        admitted_name(name)
                    except ValueError:
                        continue
                    yield name


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return (json.dumps(value, sort_keys=True, indent=2) + '\n').encode()


def build(profile):
    profile = local_reference(profile, required=True)
    selected = set()
    pins = {}
    observed = {}
    configs = set()
    pending = [profile]
    def remember(name):
        raw = read_stable(name)
        digest = sha(raw)
        if name in observed and observed[name] != digest:
            raise ValueError('member changed during source closure: ' + name)
        observed[name] = digest
        return raw
    def pin(value, expected):
        if not is_hash(expected):
            raise ValueError('malformed source/config pin')
        name = local_reference(value, required=True)
        if name in pins and pins[name] != expected:
            raise ValueError('conflicting pin: ' + name)
        pins[name] = expected
        selected.add(name)
        if name.endswith('.json'):
            pending.append(name)
    while pending:
        name = pending.pop()
        if name in configs:
            continue
        configs.add(name)
        selected.add(name)
        doc = json.loads(remember(name))
        def walk(value):
            if isinstance(value, dict):
                # Operational profiles use script/source_sha256; other configs
                # use path/sha256 or foo[_path]/foo_sha256 sibling fields.
                for key, item in value.items():
                    if not isinstance(item, str) or not item.endswith(('.py', '.ps1', '.json')):
                        continue
                    hash_key = ('source_sha256' if key == 'script' else 'sha256' if key == 'path'
                                else (key[:-5] if key.endswith('_path') else key) + '_sha256')
                    expected = value.get(hash_key)
                    candidate = Path(item)
                    if not candidate.is_absolute():
                        candidate = ROOT / candidate
                    if (expected is not None and candidate.is_relative_to(ROOT)
                            and candidate.relative_to(ROOT).parts[0].lower() != 'data'):
                        pin(item, expected)
                for key, item in value.items():
                    if (isinstance(key, str) and key.endswith(('.py', '.ps1', '.json'))
                            and is_hash(item)):
                        pin(key.replace('\\', '/'), item)
                    walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)
            elif isinstance(value, str) and value.endswith(('.py', '.ps1', '.json')):
                rel = local_reference(value)
                if rel is not None:
                    selected.add(rel)
                    if rel.endswith('.json'):
                        pending.append(rel)
        walk(doc)
    for name, expected in pins.items():
        if sha(remember(name)) != expected:
            raise ValueError('changed source/config pin: ' + name)
    selected.update(EXTRA_FILES)
    for pattern in TEST_PATTERNS:
        selected.update(p.relative_to(ROOT).as_posix() for p in ROOT.glob(pattern))
    for folder in EVIDENCE_ROOTS:
        selected.update(evidence_files(folder))
    # Discover local Python imports without importing or executing the services.
    visited = set()
    pending = sorted(name for name in selected if name.endswith('.py'))
    while pending:
        name = pending.pop()
        if name in visited:
            continue
        visited.add(name)
        path = regular_file(ROOT, name)
        tree = ast.parse(remember(name), filename=name)
        candidates = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [a.name for a in node.names]
                base = ROOT
            elif isinstance(node, ast.ImportFrom):
                base = ROOT if node.level == 0 else path.parent
                for _ in range(max(0, node.level - 1)):
                    base = base.parent
                modules = [node.module or '']
                modules += [((node.module + '.') if node.module else '') + a.name
                            for a in node.names if a.name != '*']
            else:
                continue
            for module in modules:
                prefix = base / module.replace('.', '/')
                candidates.extend((prefix.with_suffix('.py'), prefix / '__init__.py'))
                # Importing pkg.child executes each regular parent package too.
                parent = prefix.parent
                while parent.is_relative_to(ROOT) and parent != ROOT:
                    candidates.append(parent / '__init__.py')
                    parent = parent.parent
        for p in candidates:
            if p.is_relative_to(ROOT) and p.is_file():
                rel = local_reference(str(p), required=True)
                selected.add(rel)
                if rel not in visited:
                    pending.append(rel)
    private = known_private_values([ROOT / 'creds', ROOT / '.env'])
    # Known account identifiers are private too, even without token syntax.
    for private_path in (ROOT / 'creds', ROOT / '.env'):
        if private_path.exists():
            private.update(credentials.ACCOUNT_ID.findall(regular_file(ROOT, private_path.name).read_bytes()))
    if len(selected) > MAX_FILES:
        raise ValueError('scoped checkpoint exceeds file count budget')
    rows = []
    private_dependencies = []
    seen_case = set()
    total = 0
    for name in sorted(selected):
        if name.casefold() in seen_case:
            raise ValueError('case-aliased duplicate member')
        seen_case.add(name.casefold())
        raw = remember(name)
        if name in pins and sha(raw) != pins[name]:
            raise ValueError('changed source/config pin during final read: ' + name)
        if name in PRIVATE_EXTERNAL_SOURCES:
            private_dependencies.append({'path': name, 'bytes': len(raw), 'sha256': sha(raw),
                'binding': 'config_pinned' if name in pins else 'working_tree_snapshot',
                'reason': 'Explicit private legacy source dependency; payload excluded, never redacted.',
                'restore_requirement': 'Retain this exact original private file locally. Restore it securely '
                    'and verify this SHA256 before resolving imports or considering a service restart.'})
            continue
        audit_payload(name, raw, private)
        total += len(raw)
        if total > MAX_TOTAL_BYTES:
            raise ValueError('scoped checkpoint exceeds size budget')
        rows.append({'path': name, 'bytes': len(raw), 'sha256': sha(raw),
                     'binding': 'config_pinned' if name in pins else 'working_tree_snapshot'})
    return {'schema': 'forex_scoped_operational_checkpoint_v1', 'profile': profile,
            'files': rows, 'config_pins_verified': len(pins),
            'private_external_sources': private_dependencies,
            'database_backup': False, 'credentials_included': False,
            'runtime_data_included': False, 'clean_git_commit': False,
            'closure_scope': 'Declared profile/config paths and pins, static local Python imports '
                'including parent packages, explicit recovery scripts and bounded textual evidence. '
                'Dynamic imports, external modules and undeclared PowerShell dependencies are not '
                'claimed complete; validate the inactive restored tree before restart.',
            'evidence_exclusions': ['name-surrogate links/reparse points', 'raw archives and caches',
                                    'stdout/stderr and .out.log/.err.log redirects', 'database/binary payloads'],
            'external_dependencies': ['Original live SQLite databases and coherent WAL state',
                'Original news source file identities and immutable receipt/archive history',
                'Broker credentials, installed Python runtime and third-party dependencies',
                'Local rolling smoke copy when verifying the pre-cutover fingerprint'],
            'restore_warning': 'Do not launch writers from a restored source tree. Restore private '
                'state coherently and validate source identity/ownership before any restart. '
                'This snapshot does not authorize orders, a new trial, or a recovery cutoff extension.'}


def publish(inventory, accepted, accepted_sha256, destination):
    raw = encode(inventory)
    if not is_hash(accepted_sha256) or sha(accepted) != accepted_sha256 or accepted != raw:
        raise ValueError('inventory changed or not explicitly accepted')
    destination = Path(destination).absolute()
    reject_link_chain(destination)
    if destination.is_relative_to(ROOT) or ROOT.is_relative_to(destination):
        raise ValueError('publication destination must be outside source project')
    destination.mkdir(parents=False, exist_ok=False)
    for row in inventory['files']:
        source = read_stable(row['path'])
        if sha(source) != row['sha256']:
            raise ValueError('source changed during copy')
        target = destination / 'source' / row['path']
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source)
    (destination / 'MANIFEST.json').write_bytes(raw)
    readme = ('# Operational repair checkpoint — September 16, 2026\n\n'
              'Read the [operational repair and remaining limits](source/docs/FOREX_OPERATIONAL_REPAIR_20260916.md) '
              'and [pending queue](source/FOREX_PENDING_IMPROVEMENTS.md).\n\n'
              'This is a scoped source and evidence snapshot, not a complete runtime backup. '
              'Original databases, credentials, news object archives and local file identities '
              'remain external dependencies. Any private_external_sources in MANIFEST.json are '
              'hash-bound local files deliberately omitted without redaction (four approved private legacy '
              'dependencies in the production closure); they must be retained '
              'securely for recreation. Restore inactive; do not start duplicate writers. '
              'The package changes no trading authority or trial cutoff.\n')
    readme_bytes = readme.encode('utf-8')
    (destination / 'README.md').write_bytes(readme_bytes)
    zip_path = destination / 'operational_source_20260916.zip'
    with zipfile.ZipFile(zip_path, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        archive.write(destination / 'MANIFEST.json', 'MANIFEST.json')
        archive.write(destination / 'README.md', 'README.md')
        for row in inventory['files']:
            archive.write(destination / 'source' / row['path'], 'source/' + row['path'])
    with zipfile.ZipFile(zip_path) as archive:
        expected_names = {'MANIFEST.json', 'README.md'} | {'source/' + row['path'] for row in inventory['files']}
        infos = archive.infolist()
        if (len(infos) != len(expected_names) or {info.filename for info in infos} != expected_names
                or any(info.is_dir() or stat.S_IFMT(info.external_attr >> 16) not in {0, stat.S_IFREG}
                       for info in infos) or archive.testzip() is not None):
            raise ValueError('zip inventory/type/CRC failed')
        if archive.read('MANIFEST.json') != raw or archive.read('README.md') != readme_bytes:
            raise ValueError('zip metadata readback mismatch')
        if (destination / 'MANIFEST.json').read_bytes() != raw or (destination / 'README.md').read_bytes() != readme_bytes:
            raise ValueError('copied metadata readback mismatch')
        for row in inventory['files']:
            if sha(archive.read('source/' + row['path'])) != row['sha256']:
                raise ValueError('zip readback mismatch')
            if sha((destination / 'source' / row['path']).read_bytes()) != row['sha256']:
                raise ValueError('copied source readback mismatch')
    receipt = {'created_utc': datetime.now(timezone.utc).isoformat(),
               'manifest_sha256': sha(raw), 'zip_sha256': sha(zip_path.read_bytes()),
               'zip_bytes': zip_path.stat().st_size, 'files': len(inventory['files']),
               'private_legacy_source_dependencies_local': len(inventory.get('private_external_sources', [])),
               'standalone_runtime_recreation': False,
               'local_readback_passed': True, 'cloud_sync_verified': False}
    (destination / 'PUBLISH_RECEIPT.json').write_bytes(encode(receipt))
    return receipt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile', required=True)
    ap.add_argument('--inventory', required=True)
    ap.add_argument('--publish', type=Path)
    ap.add_argument('--inventory-sha256')
    args = ap.parse_args()
    inventory_path = Path(args.inventory).absolute()
    reject_link_chain(inventory_path)
    if inventory_path.is_relative_to(ROOT) and is_evidence(inventory_path.relative_to(ROOT).as_posix()):
        raise ValueError('inventory must be outside recursively collected evidence')
    inventory = build(args.profile)
    raw = encode(inventory)
    if not args.publish:
        with inventory_path.open('xb') as stream:
            stream.write(raw)
        print(json.dumps({'files': len(inventory['files']), 'bytes': sum(x['bytes'] for x in inventory['files']),
                          'inventory_sha256': sha(raw), 'inventory': str(inventory_path)}))
        return
    accepted = inventory_path.read_bytes()
    receipt = publish(inventory, accepted, args.inventory_sha256, args.publish)
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
