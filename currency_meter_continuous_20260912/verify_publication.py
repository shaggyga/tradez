"""Verify the published documentation, vault bytes and immutable release."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def main():
    publication = read(ROOT / 'PUBLICATION_RECEIPT.json')
    final_documents = {}
    for item in publication['changes']:
        assert sha(Path(item['before_copy'])) == item['before_sha256'], item['before_copy']
        final_documents[item['path']] = item['after_sha256']
    for name, expected in final_documents.items():
        assert sha(Path(name)) == expected, name
    assert sha(Path(publication['new_canonical_report'])) == publication['new_canonical_report_sha256']
    drop = Path(publication['vault_addendum'])
    assert sha(drop / 'MANIFEST.json') == publication['vault_manifest_sha256']
    vault_files = read(drop / 'MANIFEST.json')
    assert len(vault_files) == publication['vault_files']
    for name, expected in vault_files.items():
        assert sha(drop / name) == expected, name
    release = read(ROOT / 'RELEASE_MANIFEST.json')
    completion = read(ROOT / 'COMPLETION_RECEIPT.json')
    assert sha(ROOT / 'COMPLETION_RECEIPT.json') == publication['completion_sha256']
    assert sha(ROOT / 'RELEASE_MANIFEST.json') == completion['release_sha256']
    for name, expected in release['files'].items():
        assert sha(ROOT / name) == expected, name
    build = read(ROOT / 'review/RESTORATION_BUILD.json')
    assert sha(Path(build['archive'])) == build['archive_sha256']
    assert sha(drop / Path(build['archive']).name) == build['archive_sha256']
    for name, expected in build['files'].items():
        assert sha(ROOT / 'restore_check_001' / name) == expected, name
    prior = ROOT.parent / 'direction_decision_20260911/local_restoration_001.zip'
    assert sha(prior) == completion['previous_signed_cost_archive_preserved']
    result = {
        'verified_utc': datetime.now(timezone.utc).isoformat(),
        'passed': True,
        'before_document_versions_verified': len(publication['changes']),
        'final_modified_documents_verified': len(final_documents),
        'new_canonical_report_verified': True,
        'vault_files_verified': len(vault_files),
        'frozen_release_files_verified': len(release['files']),
        'restored_files_verified': len(build['files']),
        'vault_zip_matches_release_zip': True,
        'previous_signed_cost_archive_unchanged': True,
        'publication_receipt_sha256': sha(ROOT / 'PUBLICATION_RECEIPT.json'),
    }
    with (ROOT / 'POST_PUBLICATION_VERIFICATION.json').open('x', encoding='utf-8') as out:
        json.dump(result, out, indent=2, sort_keys=True)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
