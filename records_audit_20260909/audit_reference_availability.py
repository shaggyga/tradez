"""Check exact local paths referenced by records; never infer source identity."""
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
from urllib.parse import unquote
import hashlib
import json
import re

BASE = Path(__file__).resolve().parent
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
PROJECT = Path('C:/Users/zmoor/Documents/forex/trad')
source = VAULT / 'VAULT_READABILITY_REPORT.json'
raw = source.read_bytes()
data = json.loads(raw)
pointer = json.loads((VAULT / 'source/WORKTREE_SOURCE_LATEST.json').read_bytes())
manifest = json.loads((VAULT / 'source' / pointer['manifest']).read_bytes())
def io_path(path):
    value = str(path)
    return path if value.startswith('\\\\?\\') else Path('\\\\?\\' + value)

started = datetime.now(timezone.utc).isoformat()
records = []
for row in data['references']:
    if row['classification'] not in {'machine_local_external_not_read', 'not_in_vault_or_current_source_archive'}:
        continue
    target = unquote(str(row.get('resolved') or row['target'])).strip('<> ').replace('\\', '/')
    target = re.sub(r':\d+(?:-\d+)?$', '', target.split('#')[0])
    result = {'origin': row['origin'], 'target': row['target'], 'classification': row['classification']}
    paths = []
    if re.match(r'^[A-Za-z]:/', target) and not re.search(r'[<>*?{}]', target):
        paths = [Path(target)]
    elif row['classification'] == 'not_in_vault_or_current_source_archive' and not re.search(r'[:<>*?{}]', target):
        for base in (VAULT / Path(row['origin']).parent, PROJECT):
            path = (base / target).resolve()
            if path.is_relative_to(VAULT.parents[1]) or path.is_relative_to(PROJECT.parent):
                paths.append(path)
    if not paths:
        result['availability'] = 'not_a_supported_literal_local_path'
    else:
        result['checked_paths'] = [str(p) for p in paths]
        result['existing_paths'] = [str(p) for p in paths if io_path(p).exists()]
        result['availability'] = 'exists_identity_unverified' if result['existing_paths'] else 'absent_at_exact_checked_paths'
    if result['availability'] != 'exists_identity_unverified':
        matches = [r['path'] for r in manifest['files'] if Path(r['path']).name.lower() == Path(target).name.lower()]
        result['same_basename_archive_candidates'] = matches[:30]
        result['same_basename_candidate_count'] = len(matches)
        result['candidates_establish_reference_identity'] = False
    records.append(result)
counts = dict(Counter(r['availability'] for r in records))
result = {'schema_version': 'forex_record_external_reference_availability_v2',
    'started_utc': started, 'completed_utc': datetime.now(timezone.utc).isoformat(),
    'source': str(source), 'source_sha256': hashlib.sha256(raw).hexdigest(),
    'reference_occurrences_checked': len(records), 'availability_counts': counts, 'references': records,
    'supersedes': 'EXTERNAL_REFERENCE_AVAILABILITY.json: extended Windows paths and documented sibling vault/project paths are supported in v2; basename matches are suggestions only.',
    'limits': ['Repeated references are occurrences, not unique files or independent experiments.',
              'Existence does not verify historical contents, model compatibility or original availability clocks.',
              'No databases, model payloads, credential files or external URLs were opened.',
              'Missing exact paths do not establish that no archive or differently located copy exists.']}
encoded = (json.dumps(result, indent=2, sort_keys=True) + '\n')
encoded = re.sub(r'\b\d{3}-\d{3}-\d+-\d{3}\b', '[account identity omitted]', encoded).encode()
path = BASE / 'inventory/EXTERNAL_REFERENCE_AVAILABILITY_V2.json'
with path.open('xb') as f:
    f.write(encoded)
assert path.read_bytes() == encoded
print(json.dumps({'path': str(path), 'occurrences': len(records), 'counts': counts,
                  'sha256': hashlib.sha256(encoded).hexdigest()}))
