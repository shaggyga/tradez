"""Bind the final documentary audit and check its links and source bracket."""
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import unquote
import hashlib
import json
import re

BASE = Path(__file__).resolve().parent
VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
PROJECT_PARENT = Path('C:/Users/zmoor/Documents/forex')

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def io_path(path):
    value = str(path)
    return path if value.startswith('\\\\?\\') else Path('\\\\?\\' + value)

pins = {
    'model_records/MODEL_RECORDS_AUDIT_20260909.md': '9a3602896e38a281899f8e8b53922a464326926953076ab0126e13d0f2d752f4',
    'model_records/MODEL_RECORDS_AUDIT_20260909.json': 'acbee196348d8b8866dd8fa9a819258b8346b62854b741c27f29931017296bf0',
    'account_records/ACCOUNT_CAPITAL_RECORDS_AUDIT_20260909.md': '58b591a10de2c7d51c8d73c4dcf1e595b4ef5db24eddd33677440d566c772f39',
    'account_records/ACCOUNT_CAPITAL_RECORDS_AUDIT_20260909.json': 'abcc91982b2c2d1cf351a18406bfed9ffd596746189fc8e426290b5a386a6f5e',
    'evaluation_records/EVALUATION_PENDING_RECORDS_AUDIT_20260909.md': '0f84352e6264d5199bb8c331522afc4bd1aa3e9aaeb03dd5883680eb661ad7f7',
    'evaluation_records/EVALUATION_PENDING_RECORDS_AUDIT_20260909.json': '43058e91790e6ad544c4dd858a0b56beaeba22d482f8178fb2eaf750ab18e355',
}
final_names = list(pins) + [
    'FOREX_RECORDS_AUDIT_20260909.md',
    'inventory/RECORD_CORRESPONDENCE_AUDIT.json',
    'inventory/VAULT_INVENTORY_VERIFICATION.json',
    'inventory/VAULT_HISTORY_DISCOVERY_INDEX.json',
    'inventory/LEGACY_VAULT_PAYLOAD_AUDIT_V2.json',
    'inventory/EXTERNAL_REFERENCE_AVAILABILITY_V2.json',
    'audit_record_inventory.py', 'audit_legacy_records.py',
    'audit_reference_availability.py', 'finalize_records_audit.py',
]
bindings = []
links = []
for name in final_names:
    path = BASE / name
    raw = path.read_bytes()
    digest = sha(raw)
    if name in pins and digest != pins[name]:
        raise ValueError('branch_changed_after_finalization:' + name)
    if path.suffix in {'.json', '.md'}:
        if re.search(rb'\b\d{3}-\d{3}-\d+-\d{3}\b', raw):
            raise ValueError('unmasked_account_identity_in_output:' + name)
    if path.suffix == '.json':
        json.loads(raw)
    bindings.append({'path': str(path), 'relative_path': name, 'bytes': len(raw), 'sha256': digest})
    if path.suffix == '.md':
        for match in re.finditer(r'\]\(([^)]+)\)', raw.decode('utf-8')):
            target = unquote(match.group(1)).strip('<>')
            if target.startswith(('http:', 'https:', '#')):
                continue
            target = re.sub(r':\d+(?:-\d+)?$', '', target.split('#')[0])
            destination = Path(target) if re.match(r'^[A-Za-z]:[/\\]', target) else (path.parent / target).resolve()
            exists = io_path(destination).exists()
            planned_index = destination == BASE / 'RECORDS_AUDIT_FINAL_INDEX.json'
            links.append({'origin': name, 'target': match.group(1), 'exists': exists or planned_index})

correspondence = json.loads((BASE / 'inventory/RECORD_CORRESPONDENCE_AUDIT.json').read_bytes())
legacy = json.loads((BASE / 'inventory/LEGACY_VAULT_PAYLOAD_AUDIT_V2.json').read_bytes())
if legacy['summary']['files_verified'] != 5637 or legacy['summary']['missing_or_changed_files']:
    raise ValueError('legacy_verification_not_closed')
source_changes = []
json_ambiguities = []
for row in correspondence['canonical_records']:
    for path, expected in ((VAULT / row['vault_name'], row['sha256']),
                           (PROJECT_PARENT / row['canonical_source'], row['canonical_sha256'])):
        if sha(io_path(path).read_bytes()) != expected:
            source_changes.append(str(path))
    if row['vault_name'].endswith('.json'):
        duplicates = []
        nonfinite = []
        def object_hook(pairs):
            seen = set()
            for key, value in pairs:
                if key in seen:
                    duplicates.append(key)
                seen.add(key)
            return dict(pairs)
        json.loads((VAULT / row['vault_name']).read_bytes(), object_pairs_hook=object_hook,
                   parse_constant=lambda value: nonfinite.append(value))
        if duplicates or nonfinite:
            json_ambiguities.append({'record': row['vault_name'], 'duplicate_key_count': len(duplicates),
                                     'nonfinite_constant_count': len(nonfinite)})
broken = [r for r in links if not r['exists']]
main_broken = [r for r in broken if r['origin'] == 'FOREX_RECORDS_AUDIT_20260909.md']
if main_broken or source_changes:
    raise ValueError('main_links_or_canonical_source_bracket_failed:' + json.dumps({'links': main_broken, 'sources': source_changes}))
result = {
    'schema_version': 'forex_records_audit_final_index_v1',
    'completed_utc': datetime.now(timezone.utc).isoformat(),
    'status': 'enumerated_record_reconciliation_completed_with_declared_scope_limits',
    'artifacts': bindings, 'local_markdown_links_checked': len(links),
    'unresolved_output_links': broken, 'canonical_source_changes_at_final_read': source_changes,
    'canonical_json_structural_ambiguities': json_ambiguities,
    'superseded_development_receipts': {
        'inventory/LEGACY_VAULT_PAYLOAD_AUDIT.json': 'Ordinary Windows paths falsely reported absent long-path files; use V2.',
        'inventory/EXTERNAL_REFERENCE_AVAILABILITY.json': 'Use V2 with extended paths and documented sibling roots.',
    },
    'scope_note': 'File verification and named semantic record review do not certify every code path, raw observation or economic claim. No model refit/rescore, broker request or project/vault/runtime change.',
}
raw = (json.dumps(result, indent=2, sort_keys=True) + '\n').encode()
path = BASE / 'RECORDS_AUDIT_FINAL_INDEX.json'
with path.open('xb') as f:
    f.write(raw)
assert path.read_bytes() == raw
print(json.dumps({'path': str(path), 'sha256': sha(raw), 'artifact_count': len(bindings),
    'local_links_checked': len(links), 'unresolved_links': broken,
    'canonical_json_structural_ambiguities': json_ambiguities,
    'canonical_source_changes': source_changes}))
