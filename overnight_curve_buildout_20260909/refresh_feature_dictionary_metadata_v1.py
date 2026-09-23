"""Reviewed metadata-only dictionary refresh; no worker/model/runtime imports."""
import ast
from copy import deepcopy
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent / 'trad'
EVIDENCE = BASE / 'feature_dictionary_source_refresh_v1'
MANAGER = 'oanda_advisor_account_manager_auto.py'
OLD_MANAGER = 'af211315d359e07a5b41b38ca0c4d30f75a7851ea5ce521a84b08d2af9983e27'
NEW_MANAGER = 'f07fb5dc4c1eb460b40bd1108636e1213b082c7c6ca6aa592998e8dcb8389faa'
EXPECTED = {
    'docs/feature_dictionary/catalog_251.json': '918ee5daa52ba9c2464cc11e5dae726f5813ab1cefcc51db1d7e02010e369e49',
    'docs/FOREX_FEATURE_DICTIONARY_CURRENT.json': 'cf813370d8a00e0e33fdb599a6c933fd9d9f9de29cd2de95a6b3b5a4a79600bc',
    'docs/FOREX_FEATURE_DICTIONARY_CURRENT.md': '340349f4c55628234fb354e97ab7d251c1e9182c0ed190cfa65e87a369315374',
    'tools/build_forex_feature_dictionary.py': '955b60e211ff071d7ab8d9729ef74142b7f204880c1988c2bfb18f0683f8aa67',
    'test_forex_feature_dictionary.py': '54b08b1cad5b55e34fb907887b969b3ed0501e94b872c754893e0349ea228123',
}
CATALOG = next(iter(EXPECTED))


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def record(path):
    raw = path.read_bytes()
    return dict(path=str(path.resolve()), sha256=sha(raw), bytes=len(raw))


def save(path, value):
    raw = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    with path.open('xb') as stream:
        stream.write(raw)
    return record(path)


def main():
    assert not EVIDENCE.exists(), 'fresh_evidence_directory_required'
    before = {name: (ROOT / name).read_bytes() for name in EXPECTED}
    for name, expected in EXPECTED.items():
        assert sha(before[name]) == expected, name
    old_source = BASE / 'management_correctness_audit_v1/before_source' / MANAGER
    accepted_source = BASE / 'management_correctness_audit_v1/accepted_source' / MANAGER
    assert record(old_source)['sha256'] == OLD_MANAGER
    assert record(accepted_source)['sha256'] == NEW_MANAGER
    assert (ROOT / MANAGER).read_bytes() == accepted_source.read_bytes()
    trees = [ast.parse(path.read_text(encoding='utf-8-sig')) for path in (old_source, accepted_source)]
    proofs = []
    for name, old_line, new_line in [('atr_pips', 1942, 1950), ('candle_snapshot', 1957, 1965)]:
        nodes = [next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name) for tree in trees]
        dumps = [ast.dump(node, include_attributes=False) for node in nodes]
        assert dumps[0] == dumps[1] and nodes[0].lineno == old_line and nodes[1].lineno == new_line
        proofs.append(dict(symbol=name, old_start=old_line, new_start=new_line, ast_sha256=sha(dumps[0].encode()), identical=True))
    prior_manifests = []
    for path in sorted(BASE.rglob('CURATED*.json')):
        payload = path.read_bytes()
        if b'FEATURE_DICTIONARY' in payload or b'catalog_251' in payload:
            prior_manifests.append(record(path))
    assert len(prior_manifests) == 2
    prior_validation = ROOT / 'FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json'
    retained = dict(before)
    retained['FOREX_FEATURE_DICTIONARY_VALIDATION_20260908.json'] = prior_validation.read_bytes()
    EVIDENCE.mkdir()
    preserved = []
    for name, raw in retained.items():
        target = EVIDENCE / 'before_source' / name
        if target.suffix == '.py':
            target = target.with_name(target.name + '.txt')
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(raw)
        assert target.read_bytes() == raw
        preserved.append(dict(original_path=str(ROOT / name), retained=record(target)))
    save(EVIDENCE / 'BEFORE_DICTIONARY_SOURCE_REFRESH_20260909.json', dict(
        created_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), retained=preserved,
        manager_before=record(old_source), manager_after=record(accepted_source), unchanged_symbol_proofs=proofs,
        historical_only_prior_manifest_bindings=prior_manifests, source_or_runtime_change=False))
    builder = ROOT / 'tools/build_forex_feature_dictionary.py'
    def invoke(args):
        result = subprocess.run([sys.executable, '-B', str(builder), '--root', str(ROOT), *args],
                                cwd=ROOT, capture_output=True, text=True, timeout=90)
        return dict(command=[sys.executable, '-B', str(builder), '--root', str(ROOT), *args],
                    exit_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
    failed_check = invoke(['--check'])
    assert failed_check['exit_code'] != 0 and 'inspected source changed; review definition before rebuilding: ' + MANAGER in failed_check['stderr']
    save(EVIDENCE / 'BEFORE_EXPECTED_SOURCE_DRIFT_CHECK_20260909.json', failed_check)
    old_catalog = json.loads(before[CATALOG]); expected_catalog = deepcopy(old_catalog)
    expected_catalog['source_hashes'][MANAGER] = NEW_MANAGER
    feature = expected_catalog['features'][13]
    assert feature['feature_id'] == 'atr_14_pips'
    for index, old_line, new_line in [(4, 1942, 1950), (5, 1972, 1980)]:
        ref = feature['source_refs'][index]
        assert ref['path'] == MANAGER and ref['line'] == old_line
        ref['line'] = new_line
    replacements = [(OLD_MANAGER.encode(), NEW_MANAGER.encode())]
    for old_line, new_line in [(1942, 1950), (1972, 1980)]:
        old = ('"path": "' + MANAGER + '",\n          "line": ' + str(old_line)).encode()
        new = old.replace(str(old_line).encode(), str(new_line).encode())
        if old not in before[CATALOG]:
            old = old.replace(b'\n', b'\r\n'); new = new.replace(b'\n', b'\r\n')
        replacements.append((old, new))
    updated_catalog = before[CATALOG]
    for old, new in replacements:
        assert updated_catalog.count(old) == 1, old
        updated_catalog = updated_catalog.replace(old, new)
    assert json.loads(updated_catalog) == expected_catalog
    assert (ROOT / CATALOG).read_bytes() == before[CATALOG]
    (ROOT / CATALOG).write_bytes(updated_catalog)
    built = invoke(['--build'])
    save(EVIDENCE / 'DICTIONARY_BUILD_20260909.json', built)
    assert built['exit_code'] == 0, built
    checked = invoke(['--check'])
    save(EVIDENCE / 'DICTIONARY_CHECK_20260909.json', checked)
    assert checked['exit_code'] == 0, checked
    expected_document = json.loads(before['docs/FOREX_FEATURE_DICTIONARY_CURRENT.json'])
    expected_document['parts']['catalog'] = expected_catalog
    expected_document['authored_part_sha256'][CATALOG] = sha(updated_catalog)
    assert json.loads((ROOT / 'docs/FOREX_FEATURE_DICTIONARY_CURRENT.json').read_bytes()) == expected_document
    expected_markdown = before['docs/FOREX_FEATURE_DICTIONARY_CURRENT.md']
    for old_line, new_line in [(1942, 1950), (1972, 1980)]:
        old = (MANAGER + ':' + str(old_line)).encode(); new = (MANAGER + ':' + str(new_line)).encode()
        assert expected_markdown.count(old) == 1
        expected_markdown = expected_markdown.replace(old, new)
    assert (ROOT / 'docs/FOREX_FEATURE_DICTIONARY_CURRENT.md').read_bytes() == expected_markdown
    assert prior_validation.read_bytes() == retained[prior_validation.name]
    assert record(ROOT / MANAGER)['sha256'] == NEW_MANAGER
    for name in ('tools/build_forex_feature_dictionary.py', 'test_forex_feature_dictionary.py'):
        assert (ROOT / name).read_bytes() == before[name]
    for binding in prior_manifests:
        assert record(Path(binding['path'])) == binding
    result = dict(status='metadata_only_rebuilt_and_checked', completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        helper=record(Path(__file__)), preserved=preserved, unchanged_symbol_proofs=proofs,
        changes=[dict(path=CATALOG, fields=['source_hashes.' + MANAGER, 'features[13].source_refs[4].line', 'features[13].source_refs[5].line']),
                 dict(path='docs/FOREX_FEATURE_DICTIONARY_CURRENT.json', meaning='Deterministic generated counterpart plus authored catalog SHA; all other parsed values unchanged.'),
                 dict(path='docs/FOREX_FEATURE_DICTIONARY_CURRENT.md', meaning='Only the two advisor source-line references changed.')],
        current_files=[record(ROOT / name) for name in EXPECTED], old_validation_unchanged=record(prior_validation),
        manager_sources_unchanged=True, all_feature_definitions_unchanged=True, old_manifests_unchanged=prior_manifests,
        runtime_changes=False, model_fits=False, broker_requests=0)
    print(json.dumps(save(EVIDENCE / 'DICTIONARY_METADATA_APPLICATION_20260909.json', result)))


if __name__ == '__main__':
    main()
