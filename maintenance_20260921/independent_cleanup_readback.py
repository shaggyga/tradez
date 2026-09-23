"""Independent post-cleanup verification. Standard library; no payload text output."""
import gzip
import hashlib
import json
import os
import stat
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(r'C:\Users\zmoor\Documents\forex')
HERE = BASE / 'maintenance_20260921'
COLD = BASE / 'cold_log_archive_20260921'
TRAD = BASE / 'trad'
RUN = HERE / 'CLEANUP_EXECUTION_20260921T054839255651Z.json'
PLAN = HERE / 'EXACT_CLEANUP_EXECUTION_PLAN.json'
OUT = HERE / 'CLEANUP_INDEPENDENT_READBACK.json'
BLOCK = 1024 * 1024
START = time.monotonic()

def fingerprint(info):
    return (info.st_size, info.st_mtime_ns, info.st_ino, info.st_dev)

def plain(path, root, *, absent_ok=False):
    path = Path(path).absolute()
    if path.drive.upper() != 'C:' or '..' in path.parts or not path.is_relative_to(root):
        raise ValueError('C_path_boundary_failure')
    for ancestor in (path, *path.parents):
        try:
            info = ancestor.lstat()
        except FileNotFoundError:
            if ancestor == path and not absent_ok:
                raise
            continue
        if getattr(info, 'st_file_attributes', 0) & 0x400 or stat.S_ISLNK(info.st_mode):
            raise ValueError('reparse_or_symbolic_link_forbidden')
    return path

def hash_file(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        while data := stream.read(BLOCK):
            h.update(data)
    return h.hexdigest()

plain(RUN, HERE); plain(PLAN, HERE); plain(COLD, BASE)
run = json.loads(RUN.read_bytes())
plan = json.loads(PLAN.read_bytes())
assert run['status'] == 'completed' and run['files_removed_this_run'] == 5191
assert hash_file(PLAN) == run['plan_sha256']
assert len(run['items']) == len(plan['records']) == 5191 and not run['skips']
planned = {r['source_path'].casefold(): r for r in plan['records']}
assert len(planned) == 5191
checkpoint = plain(run['checkpoint_receipt'], HERE)
assert hash_file(checkpoint) == run['checkpoint_receipt_sha256']

totals = Counter()
kinds = {}
failures = []
seen = set()
expected_archives = set()
expected_receipts = set()
receipt_hash_manifest = hashlib.sha256()

for index, item in enumerate(run['items'], start=1):
    source_path = item['source_path']
    try:
        folded = source_path.casefold()
        if folded in seen:
            raise ValueError('duplicate_source_in_execution_receipt')
        seen.add(folded)
        expected = planned[folded]
        source = plain(source_path, TRAD, absent_ok=True)
        key = hashlib.sha256(folded.encode('utf-8')).hexdigest()[:24]
        receipt = plain(item['receipt_path'], COLD)
        if receipt != COLD / 'receipts' / (key + '.json'):
            raise ValueError('receipt_path_mapping_mismatch')
        expected_receipts.add(receipt.name)
        receipt_before = receipt.stat()
        receipt_payload = receipt.read_bytes()
        saved = json.loads(receipt_payload)
        if fingerprint(receipt.stat()) != fingerprint(receipt_before):
            raise ValueError('receipt_changed_during_read')
        assert saved['source']['path'] == source_path
        assert saved['source']['bytes'] == expected['bytes'] == item['original_bytes']
        assert saved['source']['mtime_ns'] == expected['mtime_ns']
        assert saved['kind'] == expected['kind'] == item['kind']
        assert saved['original_removed'] is True and item['original_removed_this_run'] is True
        assert saved['status'] == item['status'] == 'archived_original_removed'
        receipt_hash = hashlib.sha256(receipt_payload).hexdigest()
        receipt_hash_manifest.update((receipt.name + ':' + receipt_hash + '\n').encode())
        archive = plain(saved['archive_path'], COLD)
        if archive != COLD / 'payloads' / (key + '.gz'):
            raise ValueError('archive_path_mapping_mismatch')
        expected_archives.add(archive.name)
        archive_before = archive.stat()
        if archive_before.st_size != saved['archive_bytes'] or archive_before.st_size != item['archive_bytes']:
            raise ValueError('compressed_size_mismatch')
        if hash_file(archive) != saved['archive_sha256']:
            raise ValueError('compressed_sha256_mismatch')
        decompressed_hash = hashlib.sha256()
        decompressed_size = 0
        with gzip.open(archive, 'rb') as stream:
            while data := stream.read(BLOCK):
                decompressed_hash.update(data)
                decompressed_size += len(data)
        if decompressed_size != expected['bytes'] or decompressed_hash.hexdigest() != saved['original_sha256']:
            raise ValueError('complete_decompressed_sha256_or_size_mismatch')
        if decompressed_size != saved['verification']['restored_bytes'] or decompressed_hash.hexdigest() != saved['verification']['restored_sha256']:
            raise ValueError('original_receipt_verification_mismatch')
        if fingerprint(archive.stat()) != fingerprint(archive_before):
            raise ValueError('archive_changed_during_readback')
        try:
            source.lstat()
        except FileNotFoundError:
            totals['originals_absent'] += 1
        else:
            raise ValueError('source_path_present_at_readback_requires_identity_review')
        totals['verified_items'] += 1
        totals['original_bytes'] += decompressed_size
        totals['archive_bytes'] += archive_before.st_size
        totals['receipt_bytes'] += len(receipt_payload)
        kind = kinds.setdefault(item['kind'], Counter())
        kind.update(items=1, original_bytes=decompressed_size,
                    archive_bytes=archive_before.st_size, receipt_bytes=len(receipt_payload))
    except Exception as exc:
        failures.append({'index': index, 'source_path': source_path,
                         'error_type': type(exc).__name__, 'reason': str(exc)})
    if index % 1000 == 0:
        print(json.dumps({'checked': index, 'verified': totals['verified_items'],
                          'failures': len(failures)}), flush=True)

assert seen == set(planned)
actual_archive_names = {p.name for p in (COLD / 'payloads').iterdir()}
actual_receipt_names = {p.name for p in (COLD / 'receipts').iterdir()}
for kind, actual, expected in [('payload', actual_archive_names, expected_archives),
                               ('receipt', actual_receipt_names, expected_receipts)]:
    if actual != expected:
        failures.append({'type': kind + '_directory_membership_mismatch',
                         'unexpected_count': len(actual - expected),
                         'missing_count': len(expected - actual)})
logical_recovered = totals['original_bytes'] - totals['archive_bytes']
if logical_recovered != run['logical_bytes_recovered']:
    failures.append({'type': 'logical_recovered_total_mismatch',
                     'calculated': logical_recovered, 'execution_claim': run['logical_bytes_recovered']})
result = {
    'schema': 'forex_cleanup_independent_readback_v1',
    'status': 'passed' if not failures and totals['verified_items'] == 5191 else 'failed',
    'completed_utc': datetime.now(timezone.utc).isoformat(),
    'duration_seconds': round(time.monotonic() - START, 3),
    'execution_receipt': str(RUN), 'execution_receipt_sha256': hash_file(RUN),
    'plan_sha256': hash_file(PLAN), 'checkpoint_receipt_sha256': hash_file(checkpoint),
    'verifier_script_sha256': hash_file(Path(__file__)),
    'method': 'Independent standard-library verification; every receipt, compressed SHA-256, gzip stream to EOF/CRC, decompressed SHA-256 and length, stable archive metadata, exact plan mapping, C path containment and original absence.',
    'archive_root': str(COLD), 'expected_items': 5191,
    'counts_and_bytes': dict(totals),
    'by_kind': {k: dict(v) for k, v in kinds.items()},
    'archive_payload_file_count': len(actual_archive_names),
    'archive_receipt_file_count': len(actual_receipt_names),
    'ordered_receipt_name_sha256_manifest_sha256': receipt_hash_manifest.hexdigest(),
    'logical_bytes_recovered_excluding_receipt_metadata': logical_recovered,
    'logical_bytes_recovered_after_per_file_receipt_metadata': logical_recovered - totals['receipt_bytes'],
    'physical_space_reclaimed_measured': False,
    'failures': failures, 'payload_text_printed_or_published': False,
    'files_deleted_by_verifier': 0, 'source_or_archive_files_modified': False,
    'D_access': False,
    'limitations': ['Verification is a point-in-time check; it does not prevent future changes.',
                    'Logical byte savings exclude NTFS allocation effects and maintenance/checkpoint files.',
                    'No unrelated scratch restoration folders were removed or retried.'],
}
temporary = OUT.with_suffix('.json.writing')
with temporary.open('w', encoding='utf-8', newline='\n') as stream:
    json.dump(result, stream, indent=2, sort_keys=True)
    stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
os.replace(temporary, OUT)
print(json.dumps({'status': result['status'], 'counts_and_bytes': dict(totals),
                  'failures': len(failures), 'result_path': str(OUT),
                  'result_sha256': hash_file(OUT)}, indent=2))
if result['status'] != 'passed':
    raise SystemExit(1)
