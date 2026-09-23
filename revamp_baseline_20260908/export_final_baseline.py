"""Publish canonical audit records/source and refresh the vault knowledge index."""
from pathlib import Path
from hashlib import sha256
from datetime import datetime, timezone
import json
import subprocess
import sys

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent
PROJECT = ROOT / 'trad'
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')

def digest(path):
    return sha256(path.read_bytes()).hexdigest()

def save(name, value):
    with (BASE / name).open('x', encoding='utf-8') as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write('\n')

def run(label, args):
    p = subprocess.run([sys.executable, *map(str, args)], cwd=PROJECT,
                       capture_output=True, text=True, encoding='utf-8',
                       creationflags=subprocess.CREATE_NO_WINDOW)
    save(label + '_RESULT_20260908.json', {'returncode': p.returncode, 'stdout': p.stdout,
         'stderr': p.stderr, 'finished_utc': datetime.now(timezone.utc).isoformat()})
    assert p.returncode == 0, label
    return json.loads(p.stdout)

assert (PROJECT / 'FOREX_REVAMP_BASELINE_RECOVERY_VALIDATION_20260908.json').is_file()
canonical = run('CANONICAL_EXPORT', [PROJECT / 'forex_model_vault_sync.py', '--root', ROOT,
                                   '--destination', VAULT, '--canonical-only'])
assert canonical['file_count'] == 175
records = []
for destination in canonical['destinations']:
    for row in destination['canonical_project_records']['records']:
        assert digest(ROOT / row['source']) == digest(VAULT / row['name']) == row['sha256']
        records.append({'source': row['source'], 'destination': row['name'], 'sha256': row['sha256']})
assert len(records) == 175
print(json.dumps({'stage': 'canonical_records_verified', 'count': len(records)}), flush=True)
snapshot = run('SOURCE_EXPORT', [PROJECT / 'tools/vault_worktree_snapshot.py', '--root', PROJECT,
                                '--vault-project', VAULT])
assert snapshot['verification']['status'] == 'passed'
manifest_path, archive_path = VAULT / 'source' / snapshot['manifest'], VAULT / 'source' / snapshot['archive']
assert digest(manifest_path) == snapshot['manifest_sha256']
assert digest(archive_path) == snapshot['archive_sha256']
manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
assert manifest['credential_audit']['passed']
print(json.dumps({'stage': 'source_export_verified', 'files': manifest['file_count']}), flush=True)
built = run('READABILITY_BUILD', [PROJECT / 'tools/audit_forex_vault_readability.py', '--vault-project', VAULT, '--build'])
checked = run('READABILITY_CHECK', [PROJECT / 'tools/audit_forex_vault_readability.py', '--vault-project', VAULT, '--check'])
readability = json.loads((VAULT / 'VAULT_READABILITY_REPORT.json').read_text(encoding='utf-8-sig'))
assert readability['status'] == 'pass_with_declared_external_dependencies'
assert readability['record_count'] == len(records)
assert readability['source_archive_sha256'] == snapshot['archive_sha256']
for row in records:
    assert digest(ROOT / row['source']) == digest(VAULT / row['destination']) == row['sha256']
receipt_name = 'FINAL_VAULT_VERIFICATION_20260908.json'
save(receipt_name, {'status': 'passed', 'recorded_utc': datetime.now(timezone.utc).isoformat(),
     'canonical_records': records, 'canonical_record_count': len(records),
     'source_snapshot': snapshot, 'credential_audit': manifest['credential_audit'],
     'readability_build': built, 'readability_check': checked,
     'readability_report_sha256': digest(VAULT / 'VAULT_READABILITY_REPORT.json'),
     'knowledge_index_sha256': digest(VAULT / 'KNOWLEDGE_INDEX.md'),
     'scope': 'Current canonical documentation/evidence plus C source snapshot. D recovery databases, fitted weights, full matrix and legacy intrahour source remain separate local recovery assets; no full model/runtime recovery from the vault alone is claimed.',
     'runtime_or_broker_actions': False})
print(json.dumps({'status': 'passed', 'receipt': str(BASE / receipt_name),
                  'receipt_sha256': digest(BASE / receipt_name), 'canonical_records': len(records),
                  'source_files': manifest['file_count'], 'source_archive': str(archive_path),
                  'readability': readability['status']}), flush=True)
