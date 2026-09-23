"""Read-only inventory verification and history discovery for the Forex vault.

Writes only this investigation's new output directory. It executes no exported
source, imports no model, opens no runtime database, and changes no vault file.
"""
from pathlib import Path, PurePosixPath
from datetime import datetime, timezone
import hashlib
import json
import re
import zipfile

VAULT = Path('C:/Users/zmoor/OneDrive/thevault/projects/forex')
OUT = Path(__file__).resolve().parent


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def bound(path, maximum):
    if not path.is_file() or path.stat().st_size > maximum:
        raise ValueError('missing_or_oversized_input: ' + str(path))
    raw = path.read_bytes()
    return raw, dict(path=str(path), bytes=len(raw), sha256=digest(raw))


def safe_name(value):
    if not isinstance(value, str) or '\\' in value or ':' in value:
        raise ValueError('invalid_archive_name')
    name = PurePosixPath(value)
    if name.is_absolute() or '..' in name.parts or not name.parts:
        raise ValueError('invalid_relative_member')
    return value


def write_new(name, value):
    raw = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()
    with (OUT / name).open('xb') as handle:
        handle.write(raw)
    if (OUT / name).read_bytes() != raw:
        raise ValueError('output_readback')
    return dict(path=str(OUT / name), bytes=len(raw), sha256=digest(raw))


def main():
    started = datetime.now(timezone.utc).isoformat()
    pointer_raw, pointer_id = bound(VAULT / 'source/WORKTREE_SOURCE_LATEST.json', 4 << 20)
    pointer = json.loads(pointer_raw)
    archive_name, manifest_name = safe_name(pointer['archive']), safe_name(pointer['manifest'])
    if '/' in archive_name or '/' in manifest_name:
        raise ValueError('pointer_must_name_direct_source_child')
    manifest_raw, manifest_id = bound(VAULT / 'source' / manifest_name, 8 << 20)
    archive_raw, archive_id = bound(VAULT / 'source' / archive_name, 64 << 20)
    manifest = json.loads(manifest_raw)
    if manifest_id['sha256'] != pointer['manifest_sha256']:
        raise ValueError('manifest_pointer_hash')
    if archive_id['sha256'] != pointer['archive_sha256'] or archive_id['sha256'] != manifest['archive_sha256']:
        raise ValueError('archive_pointer_hash')
    entries = {safe_name(row['path']): row for row in manifest['files']}
    if len(entries) != len(manifest['files']) or len(entries) != manifest['file_count']:
        raise ValueError('manifest_member_identity')
    if len({name.casefold() for name in entries}) != len(entries):
        raise ValueError('case_colliding_members')
    if sum(row['size'] for row in entries.values()) > 256 << 20:
        raise ValueError('archive_uncompressed_bound')
    with zipfile.ZipFile(VAULT / 'source' / archive_name) as archive:
        members = archive.infolist()
        if len(members) != len(entries) or set(x.filename for x in members) != set(entries):
            raise ValueError('archive_manifest_inventory')
        for member in members:
            original = entries[safe_name(member.filename)]
            if member.file_size != original['size'] or member.file_size > 32 << 20:
                raise ValueError('member_size')
            if digest(archive.read(member)) != original['sha256']:
                raise ValueError('member_hash')

    shared_raw, shared_id = bound(VAULT / 'SHARED_PROJECT_STATE_CURRENT.json', 4 << 20)
    shared = json.loads(shared_raw)
    records = []
    if shared['record_count'] != len(shared['records']):
        raise ValueError('canonical_record_count')
    for row in shared['records']:
        name = safe_name(row['name'])
        raw, identity = bound(VAULT / name, 64 << 20)
        if len(raw) != row['size'] or identity['sha256'] != row['sha256']:
            raise ValueError('canonical_record_changed: ' + name)
        records.append(dict(identity, vault_name=name, original_source=row['source']))

    categories = {
        'official_currency_response_rank': r'official|source.condition|source.factor|currency.rank|currency.state',
        'feature_joint_models': r'feature|horizon|second.ridge|second.forecast|model.gap|modern.model|intrahour|forecast',
        'combination_dependence': r'combin|interaction|ensemble|lead.lag|cross.pair|graph|covarian|cointegr',
        'news_blurb_discovery': r'blurb|narrative|move.first|spike|news',
        'entry_position_management': r'entry|exit|rotation|portfolio|counterfactual|management|manager|paper',
    }
    compiled = {name: re.compile(pattern, re.I) for name, pattern in categories.items()}
    matched_members = [dict(path=name, bytes=row['size'], sha256=row['sha256'],
        categories=[key for key, pattern in compiled.items() if pattern.search(name)])
        for name, row in sorted(entries.items()) if any(p.search(name) for p in compiled.values())]
    headings = []
    read_docs = []
    for path in sorted(VAULT.glob('*.md')):
        if path.name == 'FEATURE_DICTIONARY_CURRENT.md':
            continue
        raw, identity = bound(path, 8 << 20)
        read_docs.append(identity)
        for number, line in enumerate(raw.decode('utf-8-sig').splitlines(), 1):
            if not re.match(r'^#{1,5}\s', line):
                continue
            tags = [name for name, pattern in compiled.items() if pattern.search(line)]
            if tags:
                headings.append(dict(path=str(path), line=number, heading=line, categories=tags))
    if (VAULT / 'source/WORKTREE_SOURCE_LATEST.json').read_bytes() != pointer_raw:
        raise ValueError('source_pointer_changed_during_inspection')
    if (VAULT / 'SHARED_PROJECT_STATE_CURRENT.json').read_bytes() != shared_raw:
        raise ValueError('shared_index_changed_during_inspection')
    result = dict(schema_version='forex_prior_work_vault_inventory_v1', started_utc=started,
        completed_utc=datetime.now(timezone.utc).isoformat(), status='inventory_and_all_member_hashes_verified',
        pointer=pointer_id, manifest=manifest_id, archive=archive_id, shared_index=shared_id,
        archive_file_count=len(entries), archive_uncompressed_bytes=sum(r['size'] for r in entries.values()),
        archive_recorded_creation=manifest.get('created_utc'), archive_scope=manifest.get('scope'),
        canonical_record_count=len(records), canonical_records=records,
        markdown_discovery_sources=read_docs, relevant_heading_count=len(headings),
        candidate_member_count=len(matched_members),
        limits=['Hash verification establishes byte identity, not correctness of historical performance.',
               'Filename and heading matches are discovery candidates, not counts of experiments or models.',
               'Runtime databases, raw news, credentials and serialized model contents were not inspected.',
               'Dated CURRENT aliases remain dated; original source inventory and assessment cutoffs are separate.',
               'This inspection does not prove OneDrive cloud synchronization.'],
        project_writes=False, vault_writes=False, database_access=False, broker_requests=False)
    index_id = write_new('VAULT_HISTORY_DISCOVERY_INDEX.json', dict(categories=categories,
        headings=headings, candidate_archive_members=matched_members))
    result['discovery_index'] = index_id
    report_id = write_new('VAULT_INVENTORY_VERIFICATION.json', result)
    print(json.dumps(dict(receipt=report_id, archive_files=len(entries), canonical_records=len(records),
        matched_headings=len(headings), candidate_members=len(matched_members))))


if __name__ == '__main__':
    main()
