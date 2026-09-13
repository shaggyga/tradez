"""Publish verified pair forecast coverage records and source-only snapshot.

Only local OneDrive bytes are verified. Cloud synchronization is not observed.
No publication occurs on import; run only after canonical evidence is final.
"""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

OUT = Path(__file__).resolve().parent
ROOT = OUT.parent / 'trad'
VAULT = Path(r'C:\Users\zmoor\OneDrive\thevault\projects\forex')
PRIOR_RECEIPT = 'FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json'
PRIOR_SHA256 = '3671297affd5addd4c2e4080c66d4d74382c24678a3430d7e4694814e5993792'
RECEIPT = 'FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json'
REPORT = 'docs/FOREX_PAIR_FORECAST_COVERAGE_20260907.md'
sys.dont_write_bytecode = True
sys.path[:0] = [str(ROOT), str(ROOT.parent)]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def encoded(value):
    return (json.dumps(value, indent=2) + '\n').encode()


def main():
    import forex_model_vault_sync as records
    from tools import vault_worktree_snapshot as snapshot
    from oanda_issue_register_validator import validate_register

    receipt_path = ROOT / RECEIPT
    receipt = load(receipt_path)
    receipt_sha256 = sha(receipt_path)
    assert sha(ROOT / PRIOR_RECEIPT) == PRIOR_SHA256, 'prior_signals_live_receipt_changed'
    assert receipt['prior_receipts_unchanged'].get(PRIOR_RECEIPT) == PRIOR_SHA256
    assert (ROOT / REPORT).is_file(), 'pair_coverage_report_missing'
    report_sha256 = sha(ROOT / REPORT)
    for row in receipt['source_bindings']:
        assert sha(ROOT / row['path']) == row['sha256'], row['path']
    for name, expected in receipt['prior_receipts_unchanged'].items():
        assert sha(ROOT / name) == expected, name
    assert len(records.CANONICAL_PROJECT_RECORDS) == 145, 'final_canonical_record_count_required'
    mapped_sources = [source.as_posix() for source, _ in records.CANONICAL_PROJECT_RECORDS]
    for name in (RECEIPT, REPORT):
        assert mapped_sources.count('trad/' + name) == 1, 'new_canonical_record_binding_required:' + name
    register = validate_register(ROOT / 'FOREX_ISSUE_REGISTER_CURRENT.json', root=ROOT)
    assert register['valid'], register

    backup = VAULT / 'maintenance/before_pair_forecast_coverage_20260907'
    backup.mkdir(parents=True, exist_ok=False)
    names = ['source/WORKTREE_SOURCE_LATEST.json', 'SHARED_PROJECT_STATE_CURRENT.json',
             'maintenance/LOCAL_VERIFICATION_CURRENT.json',
             'SIGNALS_LIVE_OPTIMIZATION_VALIDATION_CURRENT.json',
             'maintenance/SIGNALS_LIVE_EXPORT_VERIFICATION_20260907.json']
    for source, name in records.CANONICAL_PROJECT_RECORDS:
        target = VAULT / name
        if target.is_file() and target.read_bytes() != (ROOT.parent / source).read_bytes():
            names.append(name)
    preserved = []
    for name in sorted(set(names)):
        source = VAULT / name
        if not source.exists():
            continue
        target = backup / name
        target.parent.mkdir(parents=True, exist_ok=True)
        raw = source.read_bytes()
        with target.open('xb') as handle:
            handle.write(raw)
        preserved.append({'path': name, 'backup': target.relative_to(VAULT).as_posix(),
                          'sha256': sha(target), 'bytes': len(raw)})
    with (backup / 'PRESERVED_FILES.json').open('xb') as handle:
        handle.write(encoded({'files': preserved, 'deletions': 0}))

    print('Previous current records preserved; publishing canonical records.', flush=True)
    records.sync_canonical_project_records(ROOT.parent, VAULT)
    print('Publishing and verifying source-only archive.', flush=True)
    pointer = snapshot.sync_worktree_snapshot(ROOT, VAULT)
    manifest = load(VAULT / 'SHARED_PROJECT_STATE_CURRENT.json')
    assert manifest['record_count'] == len(records.CANONICAL_PROJECT_RECORDS) == 145
    for row in manifest['records']:
        assert sha(VAULT / row['name']) == row['sha256'] == sha(ROOT.parent / row['source'])
    source_manifest = load(VAULT / 'source' / pointer['manifest'])
    for row in source_manifest['files']:
        assert sha(ROOT / row['path']) == row['sha256'], row['path']
    for row in receipt['source_bindings']:
        assert sha(ROOT / row['path']) == row['sha256'], row['path']
    for name, expected in receipt['prior_receipts_unchanged'].items():
        assert sha(ROOT / name) == expected, name
    assert sha(ROOT / PRIOR_RECEIPT) == PRIOR_SHA256
    assert sha(VAULT / 'SIGNALS_LIVE_OPTIMIZATION_VALIDATION_CURRENT.json') == PRIOR_SHA256
    assert sha(ROOT / REPORT) == report_sha256, 'pair_coverage_report_changed_during_export'
    assert sha(receipt_path) == receipt_sha256, 'pair_coverage_receipt_changed_during_export'

    verified = {
        'schema_version': 'forex_pair_forecast_coverage_export_verification_v1',
        'verified_utc': datetime.now(timezone.utc).isoformat(),
        'status': 'local_source_and_pair_forecast_coverage_records_verified',
        'source': pointer, 'record_count': manifest['record_count'],
        'current_source_or_target_mismatches': 0,
        'pair_forecast_coverage_receipt_sha256': receipt_sha256,
        'pair_forecast_coverage_report_sha256': report_sha256,
        'prior_signals_live_receipt_unchanged_sha256': PRIOR_SHA256,
        'previous_records_preserved': (backup / 'PRESERVED_FILES.json').relative_to(VAULT).as_posix(),
        'credential_audit': manifest['credential_audit'], 'issue_register_validation': register,
        'deletions': 0, 'git_commit_created': False, 'runtime_mode': 'ResearchCollectionOnly',
        'research_only': True, 'can_place_orders': False, 'prediction_improvement_demonstrated': False,
        'limitations': [
            'Only local OneDrive bytes verified; cloud sync completion not observed.',
            'No full runtime database recovery or broad database integrity pass claimed.',
            'The source-only archive excludes study databases and WAL files; restoring an existing study requires those plus its registered contract.',
            'More covered pairs and forecast publications do not establish predictive improvement. Original-H1 outcomes and sufficient prospective samples are still required.',
        ],
    }
    with (VAULT / 'maintenance/PAIR_FORECAST_COVERAGE_EXPORT_VERIFICATION_20260907.json').open('xb') as handle:
        handle.write(encoded(verified))
    records.write_bytes_atomic(VAULT / 'maintenance/LOCAL_VERIFICATION_CURRENT.json', encoded(verified))
    with (OUT / 'VAULT_EXPORT_VERIFICATION.json').open('xb') as handle:
        handle.write(encoded(verified))
    print(json.dumps({'status': verified['status'], 'record_count': manifest['record_count'],
        'archive': pointer['archive'], 'archive_sha256': pointer['archive_sha256'],
        'source_files': source_manifest['file_count'], 'verification': pointer['verification']}))


if __name__ == '__main__':
    main()
