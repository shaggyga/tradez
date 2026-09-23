"""Publish the reviewed canonical records and credential-audited source archive."""
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import json
import subprocess
import sys

BASE = Path(__file__).resolve().parent
WORKSPACE = BASE.parent
PROJECT = WORKSPACE / 'trad'
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')

def digest(path):
    return sha256(path.read_bytes()).hexdigest()

def save(name, data):
    path = BASE / name
    with path.open('x', encoding='utf-8') as handle:
        json.dump(data, handle, indent=2, allow_nan=False)
        handle.write('\n')
    return path

def run(label, args):
    result = subprocess.run([sys.executable, *map(str, args)], cwd=PROJECT,
                            capture_output=True, text=True, encoding='utf-8',
                            creationflags=subprocess.CREATE_NO_WINDOW)
    save(label + '_COMMAND_RESULT_20260907.json', {
        'finished_utc': datetime.now(timezone.utc).isoformat(),
        'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr,
    })
    if result.returncode:
        raise RuntimeError(label + ' failed; retained command result contains diagnostic.')
    return json.loads(result.stdout)

canonical = run('CANONICAL_EXPORT', [PROJECT / 'forex_model_vault_sync.py', '--root', WORKSPACE,
                                  '--destination', VAULT, '--canonical-only'])
assert canonical['file_count'] == 160
records = []
for destination in canonical['destinations']:
    for row in destination['canonical_project_records']['records']:
        source, target = WORKSPACE / row['source'], VAULT / row['name']
        source_hash, target_hash = digest(source), digest(target)
        assert source_hash == target_hash == row['sha256']
        records.append({'source': row['source'], 'destination': row['name'], 'sha256': source_hash,
                        'source_destination_equal': True})
assert len(records) == 160
print(json.dumps({'stage': 'canonical_export_verified', 'records': len(records)}), flush=True)

snapshot = run('WORKTREE_EXPORT', [PROJECT / 'tools/vault_worktree_snapshot.py',
                                 '--root', PROJECT, '--vault-project', VAULT])
manifest_path = VAULT / 'source' / snapshot['manifest']
archive_path = VAULT / 'source' / snapshot['archive']
assert digest(manifest_path) == snapshot['manifest_sha256']
assert digest(archive_path) == snapshot['archive_sha256']
verification = run('WORKTREE_OFFLINE_VERIFY', [PROJECT / 'tools/vault_worktree_snapshot.py', '--verify', manifest_path])
manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
assert manifest['credential_audit']['passed'] is True
latest = json.loads((VAULT / 'source/WORKTREE_SOURCE_LATEST.json').read_text(encoding='utf-8-sig'))
assert latest['snapshot_id'] == snapshot['snapshot_id']
assert latest['archive_sha256'] == digest(archive_path)
for row in records:
    assert digest(WORKSPACE / row['source']) == digest(VAULT / row['destination']) == row['sha256']

receipt = save('VAULT_EXPORT_VERIFICATION_20260907.json', {
    'schema_version': 'horizon_final_vault_export_verification_v1_20260907',
    'recorded_utc': datetime.now(timezone.utc).isoformat(), 'status': 'passed',
    'canonical_record_count': len(records), 'canonical_records': records,
    'source_snapshot': snapshot, 'offline_archive_verification': verification,
    'manifest': str(manifest_path), 'archive': str(archive_path),
    'source_file_count': manifest['file_count'], 'credential_audit': manifest['credential_audit'],
    'pointer_matches_verified_archive': True, 'runtime_actions': False, 'broker_actions': False,
    'scope': 'Canonical audit records plus source/config/tests/docs. Databases, raw runtime history and private credentials are excluded from source archive.',
})
print(json.dumps({'status': 'passed', 'receipt': str(receipt), 'receipt_sha256': digest(receipt),
                  'canonical_records': len(records), 'source_file_count': manifest['file_count'],
                  'archive': str(archive_path), 'archive_sha256': digest(archive_path),
                  'verification': verification}), flush=True)
