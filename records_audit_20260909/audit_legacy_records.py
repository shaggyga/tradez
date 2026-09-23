"""Verify the preserved old-vault payload without restoring or executing it."""
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
import hashlib
import json

ROOT = Path('C:/Users/zmoor/Documents/forex/trad/artifacts/vault_cleanup/review_reset_20260905')
OUT = Path(__file__).resolve().parent / 'inventory'
OUT.mkdir(exist_ok=True)

def io_path(path):
    value = str(path)
    return path if value.startswith('\\\\?\\') else Path('\\\\?\\' + value)

def digest(path):
    h = hashlib.sha256()
    with io_path(path).open('rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()

started = datetime.now(timezone.utc).isoformat()
receipt_path = ROOT / 'receipt.json'
receipt_hash = digest(receipt_path)
receipt = json.loads(receipt_path.read_bytes())
plan_hash = digest(ROOT / 'plan.json')
if receipt['plan_sha256'] != plan_hash:
    raise ValueError('receipt_plan_hash_mismatch')
payload = (ROOT / 'payload').resolve()
records = []
seen = set()
for target in receipt['targets']:
    for row in target['files']:
        rel = row['path']
        if rel in seen:
            raise ValueError('duplicate_receipt_path')
        seen.add(rel)
        path = (payload / rel).resolve()
        if not path.is_relative_to(payload):
            raise ValueError('payload_path_escape')
        actual_path = io_path(path)
        result = {'relative_path': rel, 'target': target['name'], 'bytes': row['bytes'],
                  'expected_sha256': row['sha256'], 'exists': actual_path.is_file()}
        if actual_path.is_file():
            result['actual_bytes'] = actual_path.stat().st_size
            result['actual_sha256'] = digest(path)
            result['matches_receipt'] = result['actual_bytes'] == row['bytes'] and result['actual_sha256'] == row['sha256']
        records.append(result)
payload_io = io_path(payload)
actual_paths = {p.relative_to(payload_io).as_posix() for p in payload_io.rglob('*') if p.is_file()}
extra_paths = sorted(actual_paths - seen)
expected_bytes = sum(r['bytes'] for r in records)
if len(records) != receipt['moved_files'] or expected_bytes != receipt['moved_bytes']:
    raise ValueError('receipt_total_mismatch')
hash_counts = Counter(r['expected_sha256'] for r in records)
errors = [r['relative_path'] for r in records if not r.get('matches_receipt')]
changed = digest(receipt_path) != receipt_hash
summary = {'target_count': len(receipt['targets']), 'files_checked': len(records),
    'files_verified': sum(bool(r.get('matches_receipt')) for r in records),
    'bytes_checked': expected_bytes,
    'bytes_verified': sum(r['bytes'] for r in records if r.get('matches_receipt')), 'missing_or_changed_files': errors,
    'extra_payload_paths': extra_paths, 'unique_file_hashes': len(hash_counts),
    'hashes_repeated_across_paths': sum(n > 1 for n in hash_counts.values()),
    'receipt_changed_during_read': changed,
    'markdown_files': sum(Path(r['relative_path']).suffix.lower() == '.md' for r in records)}
result = {'schema_version': 'forex_legacy_vault_payload_audit_v2',
    'started_utc': started, 'completed_utc': datetime.now(timezone.utc).isoformat(),
    'receipt_path': str(receipt_path), 'receipt_sha256': receipt_hash, 'plan_sha256': plan_hash,
    'summary': summary, 'files': records,
    'initial_attempt': {'path': str(OUT / 'LEGACY_VAULT_PAYLOAD_AUDIT.json'),
        'limitation': 'The initial ordinary Windows path check reported 4727 files absent because their paths exceeded the ordinary path limit. Four extended-path probes confirmed presence. That initial missing count is superseded by this extended-path audit.'},
    'limits': ['File hashes verify preservation, not semantic correctness or independent experiments.',
              'Nested ZIPs and fitted model files were hashed as files, not loaded or executed.',
              'No raw account identifiers or source document contents copied into this inventory.',
              'Semantic review covers the named branch audit findings, not every historical code line.']}
raw = (json.dumps(result, indent=2, sort_keys=True) + '\n').encode()
path = OUT / 'LEGACY_VAULT_PAYLOAD_AUDIT_V2.json'
with path.open('xb') as f:
    f.write(raw)
assert path.read_bytes() == raw
compact = {k: v for k, v in summary.items() if k not in {'missing_or_changed_files', 'extra_payload_paths'}}
compact['missing_or_changed_count'] = len(errors)
compact['extra_payload_count'] = len(extra_paths)
print(json.dumps({'summary': compact, 'receipt': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}))
