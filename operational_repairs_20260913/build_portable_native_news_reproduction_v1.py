"""Package only new permanent test/docs files; never alter the runtime."""
from pathlib import Path
from datetime import datetime, timezone
import ast
import hashlib
import json
import shutil

OPS = Path(__file__).resolve().parent
PACKAGE = OPS / 'portable_native_news_reproduction_v1'
DEST = OPS.parent / 'trad/docs/validation/native_news_compact_20260914'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def copy_file(source, target):
    raw = source.read_bytes()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open('xb') as stream:
        stream.write(raw)
    assert target.read_bytes() == raw
    return {'source_relative_to_ops': source.relative_to(OPS).as_posix(),
            'package_path': target.relative_to(PACKAGE).as_posix(),
            'sha256': sha(raw), 'bytes': len(raw)}


def main():
    assert not DEST.exists(), 'New permanent package only; existing package is never overwritten'
    supplemental = []
    for generation, names in (
        ('compact_news_candidate_v4', ('test_compact_projection_candidate.py', 'test_independent_compact_acceptance.py')),
        ('compact_news_candidate_v6', ('test_compact_news_io_adapter.py', 'test_compact_transport_bootstrap.py',
                                      'test_packed_archive_v5.py', 'test_compact_canonical_encoding_v6.py')),
    ):
        for name in names:
            supplemental.append(copy_file(OPS / generation / name, PACKAGE / 'supplemental_test_sources' / (name + '.txt')))
    receipts = []
    for generation, name in (
        ('compact_news_candidate_v4', 'candidate_v4_contract_tests.xml'),
        ('compact_news_candidate_v4', 'empty_observation_test.xml'),
        ('compact_news_candidate_v6', 'INHERITED_IO_ADAPTER_TRANSPORT_TESTS.xml'),
        ('compact_news_candidate_v6', 'PACKED_CANONICAL_TESTS.xml'),
        ('compact_native_stage_007', 'ROOT_INTEGRATION_TESTS.xml'),
        ('compact_native_stage_007', 'SOURCE_STAGE.json'),
    ):
        receipts.append(copy_file(OPS / generation / name, PACKAGE / 'historical_receipts' / name))
    fixtures = {}
    for p in sorted((PACKAGE / 'fixtures').glob('*.py')):
        ast.parse(p.read_bytes(), filename=str(p))
        fixtures[p.name] = sha(p.read_bytes())
    runner_raw = (PACKAGE / 'run_reproduction.py').read_bytes()
    ast.parse(runner_raw)
    manifest = {'schema_version': 'native_news_portable_reproduction_v1_20260914',
        'created_utc': datetime.now(timezone.utc).isoformat(), 'fixture_sha256': fixtures,
        'runner_sha256': sha(runner_raw), 'test_count_expected': 14,
        'historical_receipts': receipts, 'supplemental_test_sources': supplemental,
        'runtime_implementations_copied_into_package': False,
        'ast_only_baseline_worker_fixture': 'fixtures/baseline_worker_stage006.py',
        'integration_run_against_installed_successor': 'pending root handover',
        'ordinary_live_capture_capacity': 'separate unresolved gate; not implied by synthetic test pass'}
    (PACKAGE / 'REPRODUCTION_PACKAGE.json').write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')
    contents = {p.relative_to(PACKAGE).as_posix(): sha(p.read_bytes()) for p in PACKAGE.rglob('*') if p.is_file()}
    assert all(not p.is_symlink() and not p.is_junction() for p in (PACKAGE, *PACKAGE.parents))
    DEST.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(PACKAGE, DEST)
    actual = {p.relative_to(DEST).as_posix(): sha(p.read_bytes()) for p in DEST.rglob('*') if p.is_file()}
    assert actual == contents
    receipt = {'installed_utc': datetime.now(timezone.utc).isoformat(), 'destination': str(DEST),
               'files': contents, 'file_count': len(contents),
               'total_bytes': sum(p.stat().st_size for p in DEST.rglob('*') if p.is_file()),
               'verification': 'all Python fixtures and runner parsed; byte-exact installation',
               'runtime_source_config_ledger_changes': False, 'test_invocation': 'not run before actual native handover'}
    path = OPS / 'PORTABLE_NATIVE_NEWS_REPRODUCTION_INSTALL.json'
    assert not path.exists()
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding='utf-8')
    print(json.dumps({key: receipt[key] for key in ('destination', 'file_count', 'total_bytes', 'verification', 'test_invocation')}))


if __name__ == '__main__':
    main()
