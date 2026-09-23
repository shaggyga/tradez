"""Bind the completed documentary reconstruction and verify its local links."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import re

ROOT = Path(__file__).resolve().parent
FILES = [
    'FOREX_PRIOR_WORK_RECONSTRUCTION_20260909.md',
    'VAULT_INVENTORY_VERIFICATION.json', 'VAULT_HISTORY_DISCOVERY_INDEX.json',
    'inspect_vault_inventory.py', 'finalize_prior_work_reconstruction.py',
    'features_models/FEATURE_MODEL_PRIOR_WORK_RECONSTRUCTION_20260909.md',
    'features_models/FEATURE_MODEL_PRIOR_WORK_RECONSTRUCTION_20260909_READABLE.md',
    'features_models/FEATURE_MODEL_PRIOR_WORK_RECONSTRUCTION_20260909.json',
    'features_models/READABLE_COPY_FORMATTING_REVIEW_20260909.json',
    'official_currency/OFFICIAL_CURRENCY_PRIOR_WORK_RECONSTRUCTION_20260909.md',
    'official_currency/OFFICIAL_CURRENCY_PRIOR_WORK_RECONSTRUCTION_20260909.json',
    'official_currency/SCHEDULED_REACTION_RETAINED_AGGREGATE_20260909.json',
    'pair_management/PAIR_MANAGEMENT_VAULT_RECONSTRUCTION_20260909.md',
    'pair_management/PAIR_MANAGEMENT_VAULT_RECONSTRUCTION_20260909.json',
]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def main():
    members = []
    for name in FILES:
        path = ROOT / name
        raw = path.read_bytes()
        if len(raw) > 4 << 20:
            raise ValueError('unexpected_document_size')
        if name.endswith('.json'):
            json.loads(raw)
        members.append(dict(path=str(path), bytes=len(raw), sha256=sha(raw)))
    inventory = json.loads((ROOT / 'VAULT_INVENTORY_VERIFICATION.json').read_bytes())
    for key in ('pointer', 'shared_index'):
        original = inventory[key]
        if sha(Path(original['path']).read_bytes()) != original['sha256']:
            raise ValueError('vault_index_changed_since_inspection')
    report = (ROOT / FILES[0]).read_text(encoding='utf-8')
    report.encode('ascii')
    links = []
    for match in re.finditer(r'\[[^\]]+\]\(([^)]+)\)', report):
        target = match.group(1).strip('<>')
        if target.startswith(('https://', 'http://')):
            raise ValueError('unexpected_network_reference_in_local_reconstruction')
        path = Path(re.sub(r':\d+$', '', target))
        if not path.is_absolute() or not path.is_file():
            raise ValueError('missing_local_report_link: ' + target)
        links.append(target)
    result = dict(schema_version='forex_prior_work_reconstruction_index_v1',
        completed_utc=datetime.now(timezone.utc).isoformat(), status='documentary_reconstruction_complete',
        report=members[0], members=members, checked_local_links=links,
        checked_local_link_count=len(links), link_errors=0,
        archive_members_verified=inventory['archive_file_count'],
        canonical_records_verified=inventory['canonical_record_count'],
        source_manifest_sha256=inventory['manifest']['sha256'],
        source_archive_sha256=inventory['archive']['sha256'],
        reviewed_areas=['wide and joint features', 'explicit conjunctions and interactions',
            'currency and model co-movement', 'historical model catalogue',
            'official-release currency responses and ranks', 'source and technical comparisons',
            'pair selection, entry and management', 'current narrower study and unresolved integration'],
        peer_review=dict(official_currency='No factual, unit or stage blocker in owned scope.',
            pair_management='Owned claims passed; zero-forecast wording corrected to zero eligible decisions.'),
        newly_scored_outcomes=False, newly_fitted_models=False,
        new_database_access='One bounded read-only aggregate query of a small retained scheduled-event ledger; no bulk scan or upstream replay.',
        project_writes=False, vault_writes=False, broker_requests=False, runtime_changes=False,
        limits=['Family-level reconstruction of selected retained evidence, not every historical database row.',
            'Branch group counts overlap and are not summed.',
            'Historical metrics preserve their original units, samples, dates and validation limitations.',
            'No profitable strategy, best model or activation authority is established by this documentary audit.'])
    raw = (json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    output = ROOT / 'PRIOR_WORK_RECONSTRUCTION_INDEX_FINAL.json'
    with output.open('xb') as handle:
        handle.write(raw)
    if output.read_bytes() != raw:
        raise ValueError('index_readback')
    print(json.dumps(dict(path=str(output), sha256=sha(raw), report_sha256=members[0]['sha256'],
        bound_documents=len(members), local_links=len(links), status=result['status'])))


if __name__ == '__main__':
    main()
