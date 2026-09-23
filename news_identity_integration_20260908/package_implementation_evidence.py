"""Copy an explicit evidence allowlist only; never import project/runtime code."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET

BASE = Path('C:/Users/zmoor/Documents/forex')
PROJECT = BASE / 'trad'
WORK = BASE / 'news_identity_integration_20260908'
REVIEW = BASE / 'revamp_baseline_20260908/news_repair_review'
ADAPTER = REVIEW / 'adapter_ledger_v2'
DEST = PROJECT / 'docs/validation/news_identity_integration_20260908'
ITEMS = {}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def add(path, relative, expected=None, binding=None):
    path = Path(path).resolve(strict=True)
    relative = Path(relative)
    if path.suffix.lower() in {'.py', '.ps1', '.html'}:
        relative = relative.with_name(relative.name + '.txt')
    key = str(path).casefold()
    item = ITEMS.setdefault(key, {'source': path, 'relative': relative,
                                 'expected_hashes': set(), 'bindings': set()})
    if expected:
        item['expected_hashes'].add(expected.lower())
    if binding:
        item['bindings'].add(str(binding))


def inventory():
    producer_receipts = [
        ('RESEARCH_PRODUCER_REPAIR_TEST_RECEIPT_20260908.json',
         'RESEARCH_PRODUCER_REPAIR_TESTS_20260908.xml',
         '16c383eca837b6a59b7071245f47b75b289cc2a6bba152b6f6bb0afa80b1a350'),
        ('RESEARCH_PRODUCER_REPAIR_FINAL_TEST_RECEIPT_20260908.json',
         'RESEARCH_PRODUCER_REPAIR_FINAL_TESTS_20260908.xml',
         '4dd06e1d06416e7dc7b6660797da285df6cee04ba89478df891f1f9521217e1b'),
        ('RESEARCH_PRODUCER_REPAIR_CLOCK_FINAL_TEST_RECEIPT_20260908.json',
         'RESEARCH_PRODUCER_REPAIR_CLOCK_FINAL_TESTS_20260908.xml',
         '664b44ae0b5c6d08bbe83042a07ebfd8578e345360d58feff72268c7dceb56db'),
    ]
    for receipt_name, xml_name, digest in producer_receipts:
        path = REVIEW / receipt_name
        receipt = read_json(path)
        add(path, 'producer/' + receipt_name, digest, 'owner final evidence hash')
        add(REVIEW / xml_name, 'producer/' + xml_name, receipt['xml_sha256'], path)
    receipt = read_json(REVIEW / producer_receipts[-1][0])
    for name, digest in receipt['new_sources_and_tests'].items():
        add(PROJECT / name, 'sources/final/' + name, digest, producer_receipts[-1][0])

    path = ADAPTER / 'ADAPTER_LEDGER_IMPLEMENTATION_VALIDATION_20260908.json'
    receipt = read_json(path)
    add(path, 'adapter_ledger/' + path.name,
        'ddfbe74462f1bb85e44935149aa8df7aa5c97c4df311a179c7315daf7478e89b',
        'adapter owner final evidence hash')
    for item in receipt['evidence']:
        original = Path(item['path'])
        area = 'reviews' if original.parent == WORK else 'adapter_ledger'
        add(original, area + '/' + original.name, item['sha256'], path)
    for item in receipt['sources']:
        original = Path(item['path'])
        add(original, 'sources/final/' + original.name, item['sha256'], path)

    path = WORK / 'WORKER_PREPARATION_VALIDATION_20260908.json'
    receipt = read_json(path)
    add(path, 'worker/' + path.name)
    for name, info in receipt['sources'].items():
        add(PROJECT / name, 'sources/final/' + name, info['sha256'], path)
    test = receipt['test_result']
    add(Path(test['xml_path']), 'worker/' + Path(test['xml_path']).name,
        test['xml_sha256'], path)

    for name in ['PRODUCER_INDEPENDENT_REVIEW_20260908.json',
                 'OPERATIONAL_INDEPENDENT_REVIEW_20260908.json',
                 'INPUTS_LEDGER_INDEPENDENT_REVIEW_20260908.json']:
        path = WORK / name
        receipt = read_json(path)
        add(path, 'reviews/' + name)
        for filename, digest in receipt.get('source_bindings', {}).items():
            recovered = WORK / 'reconstructed_review_sources' / (filename + '.txt')
            if recovered.exists():
                add(recovered, 'sources/reviewed_pre_followup/' + recovered.name, digest, path)
            else:
                add(PROJECT / filename, 'sources/final/' + filename, digest, path)

    reconstruction = WORK / 'reconstructed_review_sources/REVIEW_SOURCE_RECONSTRUCTION_RECEIPT_20260908.json'
    add(reconstruction, 'source_manifests/' + reconstruction.name,
        '1767154a31853279e6120a4d7305a467663fa1fe3b7e4edab1643feab34fcf75',
        'parent-authorized exact prior review hash reconstruction')

    path = WORK / 'DEPLOYMENT_SOURCE_FREEZE_20260908.json'
    receipt = read_json(path)
    add(path, 'source_manifests/' + path.name)
    for name, digest in receipt['source_bindings'].items():
        add(PROJECT / name, 'sources/final/' + name, digest, path)

    for name in ['BEFORE_SOURCE_MANIFEST_20260908.json',
                 'BEFORE_STORAGE_MANIFEST_20260908.json']:
        path = WORK / name
        manifest = read_json(path)
        add(path, 'source_manifests/' + name)
        for item in manifest['files']:
            add(WORK / 'before_source' / item['path'], 'sources/before/' + item['path'],
                item['sha256'], path)

    for name in ['UI_HEALTH_STORAGE_FIRST_20260908.xml',
                 'SUPERVISOR_GATES_20260908.xml', 'STORAGE_V3_FINAL_20260908.xml']:
        add(WORK / name, 'operational/' + name, binding='parent requested retained XML')
    # Retain test sources named by the requested XML. These copies are the current
    # packaging observation, not invented historical test-time hash attestations.
    tests = ['test_oanda_storage_headroom_guard.py',
             'test_oanda_joint_news_v2_operational_gate.py',
             'test_oanda_joint_price_news_dashboard_v1.py',
             'test_oanda_news_causal_aggregation_guard_v1.py']
    for name in tests:
        add(PROJECT / name, 'sources/packaging_observed/' + name, binding='retained regression source; packaging-time hash, not a historical test-time attestation')


DANGER_KEYS = {'raw_payload', 'payload_json', 'articles', 'topics', 'members',
               'source_evidence', 'csv_text', 'price_csv', 'csv_content',
               'account_id', 'accountid', 'access_token', 'api_key', 'password',
               'private_key', 'authorization'}
SECRET_PATTERN = re.compile(
    r'\b(?:sk-[A-Za-z0-9]{24,}|AKIA[A-Z0-9]{16}|[a-fA-F0-9]{64}-[a-fA-F0-9]{64})\b'
    r'|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----'
    r'|\b\d{3}-\d{3}-\d{7}-\d{3}\b')


def privacy_findings(value, path='$'):
    findings = []
    if isinstance(value, dict):
        for key, child in value.items():
            new_path = path + '.' + str(key)
            if key.lower() in DANGER_KEYS and child not in (None, False, '', [], {}):
                findings.append({'path': new_path, 'type': type(child).__name__,
                                 'length': len(child) if isinstance(child, (str, list, dict)) else None})
            findings += privacy_findings(child, new_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            findings += privacy_findings(child, path + '[' + str(index) + ']')
    return findings


def inspect():
    rows, findings, xml_counts = [], [], {}
    destinations = set()
    for item in sorted(ITEMS.values(), key=lambda x: str(x['relative']).casefold()):
        path, relative = item['source'], item['relative']
        if not path.is_relative_to(BASE) or path.suffix.lower() not in {'.json', '.xml', '.md', '.py', '.ps1', '.html', '.txt'}:
            raise ValueError('Source outside explicit safe type/root: ' + str(path))
        destination = (DEST / relative).resolve()
        if not destination.is_relative_to(DEST) or str(destination).casefold() in destinations:
            raise ValueError('Invalid or duplicate destination')
        destinations.add(str(destination).casefold())
        before = path.stat()
        data = path.read_bytes()
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError('Source changed during read: ' + str(path))
        if len(data) > 2 * 1024 * 1024:
            raise ValueError('Unexpected evidence size: ' + str(path))
        digest = sha(data)
        if item['expected_hashes'] and item['expected_hashes'] != {digest}:
            findings.append({'source': str(path), 'kind': 'receipt_hash_mismatch',
                             'actual_sha256': digest, 'expected': sorted(item['expected_hashes'])})
        decoded = data.decode('utf-8-sig')
        if SECRET_PATTERN.search(decoded):
            findings.append({'source': str(path), 'kind': 'secret_or_account_pattern'})
        if path.suffix == '.json':
            for finding in privacy_findings(json.loads(decoded)):
                findings.append({'source': str(path), 'kind': 'structured_payload', **finding})
        if path.suffix == '.xml':
            tree = ET.fromstring(data)
            suites = [tree] if tree.tag == 'testsuite' else tree.findall('testsuite')
            xml_counts[str(relative)] = {
                k: sum(int(s.attrib.get(k, '0')) for s in suites)
                for k in ('tests', 'failures', 'errors', 'skipped')}
        rows.append({
            'original_path': str(path), 'canonical_path': str(destination),
            'canonical_relative_path': relative.as_posix(), 'bytes': len(data),
            'original_sha256': digest, 'copy_sha256': digest,
            'original_mtime_ns': after.st_mtime_ns,
            'verified_receipt_sha256': sorted(item['expected_hashes']),
            'receipt_or_selection_references': sorted(item['bindings']),
            'exact_byte_copy': True,
            'source_kind': ('reconstructed_exact_hash_match' if 'reconstructed_review_sources' in path.parts
                            and path.suffix == '.txt' else 'inert_source_copy'
                            if path.suffix in {'.py', '.ps1', '.html'} else 'retained_evidence'),
        })
    return rows, findings, xml_counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--copy', action='store_true')
    args = parser.parse_args()
    inventory()
    rows, findings, xml_counts = inspect()
    if findings:
        print(json.dumps({'status': 'privacy_review_required', 'findings': findings}, indent=2))
        raise SystemExit(2)
    manifest = {
        'schema_version': 'news_identity_integration_evidence_copy_manifest_v1_20260908',
        'generated_utc': datetime.now(timezone.utc).isoformat(),
        'status': 'verified_exact_byte_copies' if args.copy else 'preflight_verified',
        'canonical_package_root': str(DEST), 'copied_artifact_count': len(rows),
        'copied_artifact_bytes': sum(r['bytes'] for r in rows),
        'count_scope': 'Copied artifacts only; this generated manifest is one additional file.',
        'privacy_review': {
            'method': 'Explicit allowlist; bounded UTF-8 reads; structured JSON raw-payload/account/secret key checks; high-specificity token/private-key/account-number text checks; owner provenance clarification.',
            'findings': [],
            'retained': 'Compact benchmark counts, durations, source hashes and numerical engineering fit diagnostics; no raw article/current-topic/price input capture.',
            'limitation': 'Bounded artifact privacy review, not a claim that a complete repository credential audit was rerun.',
        },
        'exclusions': [
            {'path': str(ADAPTER / 'live_capacity_probe_final2/EURUSD_COMPUTED_NOT_ISSUED_20260908.json'),
             'sha256': '4d56b1dab2c877581f976c3fbd0000cf1c6dd2142cb2a58f0268e00371dc1e42',
             'reason': 'Contains original captured price CSV; exact-byte FINAL2 benchmark and adapter receipt retain the compact fit and original full-artifact hash.'},
            {'scope': 'All news_captures, compressed shared captures, raw repaired_current_observed snapshots, DBs, articles and source payloads',
             'reason': 'Live data/full input captures excluded by scope; receipt path references remain original but are not copied targets.'},
            {'scope': 'Runtime observations, activation/selection/reload records and process logs',
             'reason': 'Parent owns separate later evidence packaging.'},
            {'scope': 'New post-review cadence/scope changes and updated runtime-health tests',
             'reason': 'Parent owns subsequent source-bound validation; three prior reviewed source versions are reconstructed offline to their exact original review hashes and explicitly identified.'},
        ],
        'validation_scope': 'Historical receipts and XML are retained as produced, including prior failed/concurrent-source-change and slow attempts. No tests or runtime code executed during packaging. Current source copies do not substitute for prior unavailable source versions.',
        'xml_counts': xml_counts, 'artifacts': rows,
    }
    if args.copy:
        if DEST.exists():
            raise ValueError('New package already exists; refusing every overwrite')
        # Verify all sources once more before the first destination mutation.
        for row in rows:
            if sha(Path(row['original_path']).read_bytes()) != row['original_sha256']:
                raise ValueError('Source changed after preflight: ' + row['original_path'])
        DEST.mkdir(parents=True, exist_ok=False)
        for row in rows:
            data = Path(row['original_path']).read_bytes()
            if sha(data) != row['original_sha256']:
                raise ValueError('Source changed during copy: ' + row['original_path'])
            destination = Path(row['canonical_path'])
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open('xb') as stream:
                stream.write(data)
            if sha(destination.read_bytes()) != row['original_sha256']:
                raise ValueError('Destination verification failed: ' + str(destination))
        target = DEST / 'EVIDENCE_COPY_MANIFEST_20260908.json'
        data = (json.dumps(manifest, indent=2, ensure_ascii=False) + '\n').encode('utf-8')
        with target.open('xb') as stream:
            stream.write(data)
        print(json.dumps({'status': 'packaged', 'artifacts': len(rows),
                          'total_files_with_manifest': len(rows) + 1,
                          'artifact_bytes': manifest['copied_artifact_bytes'],
                          'manifest': str(target), 'manifest_sha256': sha(data),
                          'xml_counts': xml_counts}, indent=2))
    else:
        print(json.dumps({'status': 'preflight_verified', 'artifact_count': len(rows),
                          'bytes': manifest['copied_artifact_bytes'], 'xml_counts': xml_counts}, indent=2))


if __name__ == '__main__':
    main()
