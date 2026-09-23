"""Read-only documentary inventory. Writes only this new audit directory.

Reuses the inspected prior verifier instead of implementing another archive
validator. Does not import project code, load models, open DBs or call brokers.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import re

BASE = Path(__file__).resolve().parent
OUT = BASE / 'inventory'
OUT.mkdir(exist_ok=True)
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
PROJECT_PARENT = Path('C:/Users/zmoor/Documents/forex')
PRIOR = PROJECT_PARENT / 'prior_work_reconstruction_20260909/inspect_vault_inventory.py'

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def safe_text(value):
    return re.sub(r'\b\d{3}-\d{3}-\d+-\d{3}\b', '[account identity omitted]', str(value))

def write_new(name, value):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    with (OUT / name).open('xb') as f:
        f.write(raw)
    assert (OUT / name).read_bytes() == raw
    return {'path': str(OUT / name), 'sha256': sha(raw), 'bytes': len(raw)}

started = datetime.now(timezone.utc).isoformat()
spec = importlib.util.spec_from_file_location('prior_vault_inventory_verifier', PRIOR)
prior = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prior)
prior.OUT = OUT
prior.main()

shared_raw = (VAULT / 'SHARED_PROJECT_STATE_CURRENT.json').read_bytes()
pointer_raw = (VAULT / 'source/WORKTREE_SOURCE_LATEST.json').read_bytes()
shared = json.loads(shared_raw)
pointer = json.loads(pointer_raw)
manifest = json.loads((VAULT / 'source' / pointer['manifest']).read_bytes())
archive_members = {r['path']: r for r in manifest['files']}
records = []
syntax_errors = []
for row in shared['records']:
    path = VAULT / row['name']
    raw = path.read_bytes()
    result = dict(vault_name=row['name'], canonical_source=row['source'],
        bytes=len(raw), sha256=sha(raw), mapped_hash_matches=sha(raw) == row['sha256'])
    text = raw.decode('utf-8-sig')
    result['line_count'] = len(text.splitlines())
    if path.suffix.lower() == '.json':
        try:
            parsed = json.loads(text)
            result['json_parsed'] = True
            result['description'] = safe_text(parsed.get('schema_version', 'JSON record')) if isinstance(parsed, dict) else 'JSON list/scalar'
        except (ValueError, TypeError) as exc:
            result['json_parsed'] = False
            syntax_errors.append({'file': row['name'], 'error': type(exc).__name__})
    else:
        headings = [line for line in text.splitlines() if re.match(r'^#{1,3}\s', line)]
        result['description'] = safe_text(headings[0] if headings else 'Text record')
        result['heading_count'] = len(headings)
    canonical = (PROJECT_PARENT / row['source']).resolve()
    if not canonical.is_relative_to(PROJECT_PARENT):
        raise ValueError('canonical_source_outside_project')
    result['canonical_exists'] = canonical.is_file()
    if canonical.is_file():
        actual = canonical.read_bytes()
        result['canonical_sha256'] = sha(actual)
        result['canonical_matches_vault'] = sha(actual) == result['sha256']
    member_name = row['source'].removeprefix('trad/')
    member = archive_members.get(member_name)
    result['archive_member'] = member_name if member else None
    result['archive_matches_vault'] = member['sha256'] == result['sha256'] if member else None
    records.append(result)

# Compare the published source scope with corresponding current project files.
# Differences are dated worktree drift, not automatically archival corruption.
source_comparison = []
for name, row in sorted(archive_members.items()):
    path = (PROJECT_PARENT / 'trad' / name).resolve()
    if not path.is_relative_to(PROJECT_PARENT / 'trad'):
        raise ValueError('source_member_outside_project')
    result = {'path': name, 'archived_sha256': row['sha256'], 'exists': path.is_file()}
    if path.is_file():
        result['current_sha256'] = sha(path.read_bytes())
        result['matches_archive'] = result['current_sha256'] == row['sha256']
    source_comparison.append(result)

# Inventory additional vault text records, including dated maintenance history.
# No raw credentials, databases, model blobs or archive-member contents exported.
mapped_names = {r['name'] for r in shared['records']}
extras = []
for path in sorted(VAULT.rglob('*')):
    if not path.is_file() or path.suffix.lower() not in {'.md', '.json', '.txt'}:
        continue
    rel = path.relative_to(VAULT).as_posix()
    if rel in mapped_names:
        continue
    size = path.stat().st_size
    result = {'vault_name': rel, 'bytes': size, 'mapped_canonical_record': False}
    if size > 16 << 20:
        result['inspection'] = 'size_inventory_only_over_16MiB'
    else:
        raw = path.read_bytes()
        result['sha256'] = sha(raw)
        try:
            text = raw.decode('utf-8-sig')
            result['text_decoded'] = True
            if path.suffix.lower() == '.json':
                parsed = json.loads(text)
                result['json_parsed'] = True
            else:
                headings = [line for line in text.splitlines() if re.match(r'^#{1,3}\s', line)]
                result['description'] = safe_text(headings[0] if headings else 'Text record')
        except (UnicodeDecodeError, ValueError):
            result['inspection'] = 'decode_or_json_error'
    extras.append(result)

# Detect edits to inputs during this run. A fixed receipt must not hide writers.
changed_during_read = []
for record in records:
    if sha((VAULT / record['vault_name']).read_bytes()) != record['sha256']:
        changed_during_read.append('vault/' + record['vault_name'])
    source = PROJECT_PARENT / record['canonical_source']
    if record.get('canonical_exists') and (not source.is_file() or sha(source.read_bytes()) != record['canonical_sha256']):
        changed_during_read.append(record['canonical_source'])
if (VAULT / 'SHARED_PROJECT_STATE_CURRENT.json').read_bytes() != shared_raw:
    changed_during_read.append('SHARED_PROJECT_STATE_CURRENT.json')
if (VAULT / 'source/WORKTREE_SOURCE_LATEST.json').read_bytes() != pointer_raw:
    changed_during_read.append('source/WORKTREE_SOURCE_LATEST.json')

drift = [r for r in source_comparison if not r.get('matches_archive')]
summary = dict(canonical_records=len(records), mapped_vault_hash_failures=sum(not r['mapped_hash_matches'] for r in records),
    json_syntax_errors=syntax_errors, canonical_source_missing=sum(not r['canonical_exists'] for r in records),
    canonical_source_different=sum(r.get('canonical_matches_vault') is False for r in records),
    source_archive_members=len(source_comparison), current_source_drift=len(drift),
    mapped_records_not_in_archive=sum(r['archive_member'] is None for r in records),
    mapped_archive_hash_mismatches=sum(r['archive_matches_vault'] is False for r in records),
    extra_vault_text_records=len(extras), changed_during_read=changed_during_read)
receipt = write_new('RECORD_CORRESPONDENCE_AUDIT.json', dict(
    schema_version='forex_record_correspondence_audit_v1', started_utc=started,
    completed_utc=datetime.now(timezone.utc).isoformat(),
    reused_verifier={'path': str(PRIOR), 'sha256': sha(PRIOR.read_bytes())},
    summary=summary, canonical_records=records, current_source_comparison=source_comparison,
    additional_vault_text_records=extras,
    limits=['Parsing, hashes and inventories do not establish economic correctness.',
            'Semantic findings are in the separate model, account and evaluation audits.',
            'Current source was sampled file by file, not an atomic filesystem snapshot.',
            'Private credentials, raw databases and model internals were not inspected.',
            'Source drift may be authorized concurrent work; preserve the old snapshot.']))
print(json.dumps({'receipt': receipt, 'summary': summary, 'drift_paths': [r['path'] for r in drift]}))
