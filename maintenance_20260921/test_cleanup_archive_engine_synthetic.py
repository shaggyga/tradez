"""Bounded Windows engine verification; touches only its fresh synthetic directory."""
import ctypes
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENGINE = HERE / 'cleanup_archive_engine.py'
EXPECTED_ENGINE_SHA = '4eb43e788e10b513e196e6133d607fdde5ef182166d8af02e922e2e55ec6b648'
WORK = HERE / ('synthetic_cleanup_check_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def assert_plain_c(path):
    path = Path(path).absolute()
    assert path.drive.upper() == 'C:' and path.is_relative_to(HERE)
    for item in (path, *path.parents):
        if item.exists():
            assert not item.is_junction() and not item.is_symlink()
            assert not item.stat(follow_symlinks=False).st_file_attributes & 0x400
    return path

assert sha(ENGINE) == EXPECTED_ENGINE_SHA
assert_plain_c(WORK)
WORK.mkdir(exist_ok=False)
SOURCES = WORK / 'fresh_synthetic_sources'
ARCHIVES = WORK / 'fresh_synthetic_archives'
SOURCES.mkdir(); ARCHIVES.mkdir()
spec = importlib.util.spec_from_file_location('synthetic_checked_engine', ENGINE)
engine = importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine)

results = []

def source_case(name):
    source = assert_plain_c(SOURCES / (name + '.stdout.log'))
    payload = (('Synthetic engine test only: ' + name + '\n') * 127).encode() + bytes(range(256))
    source.write_bytes(payload)
    old_ns = engine.CUTOFF_NS - 86400 * 10**9
    os.utime(source, ns=(old_ns, old_ns))
    s = source.stat()
    expected = {'source_path': str(source), 'bytes': s.st_size,
                'mtime_ns': s.st_mtime_ns, 'kind': 'synthetic_test_only'}
    archive = assert_plain_c(ARCHIVES / (name + '.gz'))
    receipt = assert_plain_c(ARCHIVES / (name + '.json'))
    return source, payload, expected, archive, receipt

def invoke(source, archive, receipt, expected, **kwargs):
    assert source.is_relative_to(SOURCES) and archive.is_relative_to(ARCHIVES) and receipt.is_relative_to(ARCHIVES)
    return engine.archive_file(source, archive, receipt, expected,
                               source_root=SOURCES, **kwargs)

def capture(name, function):
    began = time.monotonic()
    try:
        detail = function()
        results.append({'test': name, 'status': 'passed',
                        'seconds': round(time.monotonic()-began, 4), **detail})
    except Exception as exc:
        results.append({'test': name, 'status': 'failed',
                        'seconds': round(time.monotonic()-began, 4),
                        'error_type': type(exc).__name__, 'error': str(exc)})

def successful_archive():
    source, payload, expected, archive, receipt = source_case('successful_archive')
    expected_sha = hashlib.sha256(payload).hexdigest()
    result = invoke(source, archive, receipt, expected)
    assert result['status'] == 'archived_original_removed'
    assert result['original_removed'] is True and not source.exists()
    assert result['original_sha256'] == expected_sha
    assert result['verification']['restored_sha256'] == expected_sha
    assert result['verification']['restored_bytes'] == len(payload)
    assert result['archive_sha256'] == sha(archive)
    restored = assert_plain_c(WORK / 'restored_successful_archive.stdout.log')
    restored_result = engine.restore_copy(receipt, restored)
    assert restored.read_bytes() == payload and restored_result['sha256'] == expected_sha
    return {'source': str(source), 'original_removed': True,
            'original_bytes': len(payload), 'original_sha256': expected_sha,
            'archive': str(archive), 'archive_sha256': sha(archive),
            'receipt': str(receipt), 'receipt_sha256': sha(receipt),
            'restored_copy': str(restored), 'restored_sha256': sha(restored)}

def corrupted_archive_preserved():
    source, payload, expected, archive, receipt = source_case('corrupted_archive')
    def corrupt_then_verify(path, expected_sha, expected_bytes):
        # Truncate the completed gzip CRC/trailer; the real verifier must fail.
        with Path(path).open('r+b') as stream:
            stream.truncate(Path(path).stat().st_size - 8)
            stream.flush(); os.fsync(stream.fileno())
        return engine.verify_gzip(path, expected_sha, expected_bytes)
    caught = None
    try:
        invoke(source, archive, receipt, expected, verify=corrupt_then_verify)
    except (EOFError, OSError, ValueError) as exc:
        caught = {'type': type(exc).__name__, 'reason': str(exc)}
    assert caught is not None
    assert source.read_bytes() == payload and not receipt.exists()
    return {'source': str(source), 'original_preserved': True,
            'source_sha256': sha(source), 'unverified_archive': str(archive),
            'unverified_archive_sha256': sha(archive),
            'receipt_created': False, 'expected_exception': caught}

def concurrent_writer_preserved():
    source, payload, expected, archive, receipt = source_case('concurrent_writer')
    # An existing write-capable handle excludes the archiver's READ-only share mode.
    handle = engine.K.CreateFileW('\\\\?\\' + str(source), 0x40000000, 1 | 2 | 4,
                                  None, 3, 0, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    caught = None
    try:
        try:
            invoke(source, archive, receipt, expected)
        except OSError as exc:
            caught = {'type': type(exc).__name__, 'winerror': exc.winerror,
                      'reason': str(exc)}
    finally:
        assert engine.K.CloseHandle(handle)
    assert caught and caught['winerror'] == 32
    assert source.read_bytes() == payload and not archive.exists() and not receipt.exists()
    return {'source': str(source), 'original_preserved': True,
            'source_sha256': sha(source), 'archive_created': False,
            'receipt_created': False, 'expected_exception': caught}

def changed_metadata_preserved():
    source, payload, expected, archive, receipt = source_case('changed_metadata')
    changed = payload + b'CHANGED_AFTER_PLAN\n'
    source.write_bytes(changed)
    caught = None
    try:
        invoke(source, archive, receipt, expected)
    except ValueError as exc:
        caught = {'type': type(exc).__name__, 'reason': str(exc)}
    assert caught and caught['reason'] == 'held_original_identity_mismatch'
    assert source.read_bytes() == changed and not archive.exists() and not receipt.exists()
    return {'source': str(source), 'original_preserved': True,
            'source_sha256': sha(source), 'archive_created': False,
            'receipt_created': False, 'expected_exception': caught}

capture('archive_verify_remove_and_restore_exact_bytes', successful_archive)
capture('corrupted_gzip_verification_preserves_original', corrupted_archive_preserved)
capture('concurrent_writer_lock_preserves_original', concurrent_writer_preserved)
capture('source_metadata_changed_since_plan_preserves_original', changed_metadata_preserved)

dry = subprocess.run([sys.executable, '-I', '-S', '-B', str(HERE / 'run_reviewed_cleanup.py')],
                     capture_output=True, text=True, timeout=90, check=False)
try:
    dry_detail = json.loads(dry.stdout)
except Exception:
    dry_detail = {'stdout': dry.stdout, 'stderr': dry.stderr}
dry_pass = (dry.returncode == 0 and dry_detail.get('mode') == 'read_only_plan_validation'
            and dry_detail.get('all_reviewed_records') == 5191
            and dry_detail.get('source_or_archive_files_modified') is False)
results.append({'test': 'wrapper_exact_plan_default_dry_run',
                'status': 'passed' if dry_pass else 'failed',
                'returncode': dry.returncode, 'output': dry_detail})

output = {'schema': 'forex_cleanup_synthetic_validation_v1',
          'status': 'passed' if all(r['status'] == 'passed' for r in results) else 'failed',
          'completed_utc': datetime.now(timezone.utc).isoformat(),
          'test_scope': str(WORK), 'engine_sha256': sha(ENGINE),
          'wrapper_sha256': sha(HERE / 'run_reviewed_cleanup.py'),
          'test_script_sha256': sha(Path(__file__)),
          'project_original_files_deleted': 0, 'D_access': False,
          'production_cleanup_executed': False, 'tests': results}
destination = HERE / 'CLEANUP_SYNTHETIC_VALIDATION.json'
engine.save(destination, output)
print(json.dumps({'status': output['status'], 'tests': [{k:r[k] for k in ('test', 'status')} for r in results],
                  'result_path': str(destination), 'result_sha256': sha(destination),
                  'synthetic_directory': str(WORK),
                  'project_original_files_deleted': 0}, indent=2))
if output['status'] != 'passed':
    sys.exit(1)
