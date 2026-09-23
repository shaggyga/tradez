"""Lossless offline storage, no producer/model/database/broker execution."""
import copy
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading

import pytest

import oanda_news_capture_storage_v1 as storage


def seal(value):
    value['news_capture_sha256'] = hashlib.sha256(storage.canonical_bytes(
        {key: item for key,item in value.items() if key != 'news_capture_sha256'})).hexdigest()
    return value


def capture(rows=200):
    return seal({**storage.FLAGS, 'schema_version': 'synthetic_complete_capture',
        'news_evidence_epoch': 1788920000.125, 'news_generated_epoch': 1788920001.125,
        'first_observed_epoch': 1788920002.125, 'news_available_epoch': 1788920002.125,
        'news_expires_epoch': 1788920300.125,
        'current_snapshot': {'source_evidence': {'rows': [{'payload_json': '{"text":"synthetic"}',
                                                        'first_seen_utc': '2026-09-09T00:00:00+00:00'}]},
                             'as_of_utc': '2026-09-09T00:00:01+00:00'},
        'current_members': [{'event_id': 'synthetic-current', 'observed_available_utc': '2026-09-09T00:00:02+00:00'}],
        'history': [{'source_event_id': f'event_{index}', 'mapping_visible_epoch': 1788800000+index,
                     'member': {'first_seen_utc': '2026-09-08T00:00:00+00:00', 'text': 'Fixture café 海外\n"quoted"',
                                'ordinal': index}} for index in range(rows)]})


@pytest.fixture(autouse=True)
def no_external_io(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('Storage tests cannot access network or databases')
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(sqlite3, 'connect', forbidden)


def manifest_path(root, descriptor):
    return root/'manifests'/(descriptor['manifest_sha256']+'.json')


def manifest(root, descriptor):
    return json.loads(manifest_path(root, descriptor).read_bytes())


def changed_manifest(root, descriptor, alter):
    value = manifest(root, descriptor)
    alter(value)
    raw = storage.canonical_bytes(value)
    result = {**descriptor, 'manifest_sha256': hashlib.sha256(raw).hexdigest()}
    manifest_path(root, result).write_bytes(raw)
    return result


def test_exact_roundtrip_preserves_all_bytes_clocks_and_original_seal(tmp_path):
    original = capture()
    raw = storage.canonical_bytes(original)
    descriptor, stats = storage.store_capture(tmp_path, original)
    restored = storage.load_capture(tmp_path, descriptor)
    assert storage.canonical_bytes(restored) == raw
    assert descriptor['capture_sha256'] == original['news_capture_sha256']
    assert restored['first_observed_epoch'] == original['first_observed_epoch']
    assert restored['news_expires_epoch'] == original['news_expires_epoch']
    assert restored['history'] == original['history']
    assert stats['new_blob_count'] >= 4
    assert descriptor['storage_integrity_only'] is True


def test_repeated_store_is_deterministic_and_never_rewrites(tmp_path):
    original = capture()
    descriptor, _ = storage.store_capture(tmp_path, original)
    times = {str(p): p.stat().st_mtime_ns for p in tmp_path.rglob('*') if p.is_file()}
    again, stats = storage.store_capture(tmp_path, copy.deepcopy(original))
    assert again == descriptor and stats['new_blob_count'] == 0
    assert stats['new_blob_stored_bytes'] == 0
    assert times == {str(p): p.stat().st_mtime_ns for p in tmp_path.rglob('*') if p.is_file()}


def test_rolling_window_reuses_middle_chunks_without_dropping_rows(tmp_path):
    original = capture(1800)
    first, _ = storage.store_capture(tmp_path, original)
    changed = copy.deepcopy(original)
    changed['history'] = changed['history'][3:] + [{'source_event_id': 'new_event', 'mapping_visible_epoch': 1788900000,
                                                 'member': {'ordinal': 9999}}]
    changed['first_observed_epoch'] += 60
    changed['news_available_epoch'] += 60
    seal(changed)
    second, stats = storage.store_capture(tmp_path, changed)
    assert second != first
    assert stats['reused_blob_count'] > stats['new_blob_count']
    assert storage.load_capture(tmp_path, second) == changed
    assert storage.load_capture(tmp_path, first) == original


def test_empty_history_remains_complete_empty_array(tmp_path):
    value = capture(0)
    descriptor, stats = storage.store_capture(tmp_path, value)
    assert stats['history_chunks'] == 0
    assert storage.load_capture(tmp_path, descriptor) == value


def test_material_capture_above_old_16mib_roundtrips_under_new_explicit_bounds(tmp_path):
    value = capture(96)
    for row in value['history']:
        row['member']['bounded_synthetic_payload'] = 'x' * (192*1024)
    seal(value)
    raw = storage.canonical_bytes(value)
    assert 16*1024*1024 < len(raw) < storage.MAX_HISTORY_BYTES
    descriptor, _ = storage.store_capture(tmp_path, value)
    restored = storage.load_capture(tmp_path, descriptor)
    assert storage.canonical_bytes(restored) == raw
    assert manifest(tmp_path, descriptor)['limits'] == storage.LIMITS


@pytest.mark.parametrize('field', list(storage.FLAGS))
def test_unsafe_outer_flag_is_rejected(tmp_path, field):
    value = capture()
    value[field] = not storage.FLAGS[field]
    seal(value)
    with pytest.raises(ValueError, match='inert_flags'):
        storage.store_capture(tmp_path, value)
    assert not list(tmp_path.rglob('*.json'))


def test_altered_original_capture_seal_rejected_before_any_write(tmp_path):
    value = capture()
    value['news_evidence_epoch'] += 1
    with pytest.raises(ValueError, match='seal_mismatch'):
        storage.store_capture(tmp_path, value)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('change, reason', [
    (lambda v: v['history'].append(copy.deepcopy(v['history'][0])), 'event_identity'),
    (lambda v: v['history'][0].update(source_event_id=''), 'event_identity'),
    (lambda v: v['history'][0]['member'].update(text='x'*(storage.MAX_ROW_BYTES+1)), 'row_bound'),
    (lambda v: v.update(extra='x'*(storage.MAX_METADATA_BYTES+1)), 'metadata_byte_bound'),
    (lambda v: v.update(history=[{'source_event_id':str(i)} for i in range(storage.MAX_HISTORY_ROWS+1)]), 'row_count'),
])
def test_bounds_and_duplicate_identities_withhold_complete_capture(tmp_path, change, reason):
    value = capture()
    change(value)
    seal(value)
    with pytest.raises(ValueError, match=reason):
        storage.store_capture(tmp_path, value)


@pytest.mark.parametrize('bad', [float('nan'), float('inf'), object(), {1:'non-string key'}])
def test_noncanonical_values_never_enter_storage(tmp_path, bad):
    value = capture()
    value['bad'] = bad
    with pytest.raises(ValueError):
        storage.store_capture(tmp_path, value)


def test_mutating_returned_capture_cannot_modify_future_load(tmp_path):
    original = capture()
    descriptor, _ = storage.store_capture(tmp_path, original)
    value = storage.load_capture(tmp_path, descriptor)
    value['history'].clear()
    value['current_snapshot']['as_of_utc'] = 'modified'
    assert storage.load_capture(tmp_path, descriptor) == original


def test_caller_mutation_during_blob_write_does_not_mix_generations(tmp_path, monkeypatch):
    original = capture()
    expected = copy.deepcopy(original)
    put = storage._put_blob
    def mutate(root, raw):
        original['history'].clear()
        original['news_evidence_epoch'] += 100
        return put(root, raw)
    monkeypatch.setattr(storage, '_put_blob', mutate)
    descriptor, _ = storage.store_capture(tmp_path, original)
    assert storage.load_capture(tmp_path, descriptor) == expected


@pytest.mark.parametrize('change', [
    lambda d: d.update(manifest_sha256='../escape'),
    lambda d: d.update(manifest_path='/tmp/elsewhere'),
    lambda d: d.update(canonical_bytes=True),
    lambda d: d.update(can_place_orders=True),
    lambda d: d.update(storage_version='unknown'),
])
def test_descriptor_identity_traversal_and_type_rejected(tmp_path, change):
    descriptor, _ = storage.store_capture(tmp_path, capture())
    change(descriptor)
    with pytest.raises(ValueError):
        storage.load_capture(tmp_path, descriptor)


def test_missing_blob_withholds_instead_of_partial_history(tmp_path):
    descriptor, _ = storage.store_capture(tmp_path, capture())
    ref = manifest(tmp_path, descriptor)['history'][0]['blob']
    (tmp_path/'blobs'/(ref['sha256']+'.json.gz')).unlink()
    with pytest.raises(FileNotFoundError):
        storage.load_capture(tmp_path, descriptor)


def test_corrupt_blob_rejected_and_never_overwritten(tmp_path):
    value = capture()
    descriptor, _ = storage.store_capture(tmp_path, value)
    ref = manifest(tmp_path, descriptor)['components']['snapshot']
    path = tmp_path/'blobs'/(ref['sha256']+'.json.gz')
    path.write_bytes(b'corrupt')
    with pytest.raises(ValueError):
        storage.load_capture(tmp_path, descriptor)
    with pytest.raises(ValueError):
        storage.store_capture(tmp_path, value)
    assert path.read_bytes() == b'corrupt'


@pytest.mark.parametrize('alter', [
    lambda m: m['limits'].update(history_bytes=10**12),
    lambda m: m['history'].append(copy.deepcopy(m['history'][0])),
    lambda m: m['history'].reverse(),
    lambda m: m.update(history_rows=m['history_rows']-1),
    lambda m: m['components']['snapshot'].update(sha256='../elsewhere'),
    lambda m: m.update(can_promote=True),
    lambda m: m['history'][0]['blob'].update(canonical_bytes=10**12),
    lambda m: [x['blob'].update(stored_bytes=storage.MAX_STORED_BLOB_BYTES) for x in m['history']],
])
def test_tampered_resealed_manifest_is_rejected_by_contract(tmp_path, alter):
    descriptor, _ = storage.store_capture(tmp_path, capture(1600))
    changed = changed_manifest(tmp_path, descriptor, alter)
    with pytest.raises(ValueError):
        storage.load_capture(tmp_path, changed)


@pytest.mark.parametrize('kind', ['bomb', 'concatenated', 'trailing', 'truncated'])
def test_gzip_abuse_rejected_even_with_updated_compressed_hash(tmp_path, kind):
    descriptor, _ = storage.store_capture(tmp_path, capture())
    value = manifest(tmp_path, descriptor)
    ref = value['components']['metadata']
    path = tmp_path/'blobs'/(ref['sha256']+'.json.gz')
    raw = path.read_bytes()
    if kind == 'bomb': raw = gzip.compress(b'x'*(ref['canonical_bytes']+10000), mtime=0)
    elif kind == 'concatenated': raw += gzip.compress(b'{}', mtime=0)
    elif kind == 'trailing': raw += b'extra'
    else: raw = raw[:-1]
    path.write_bytes(raw)
    def alter(m):
        m['components']['metadata'].update(stored_bytes=len(raw), stored_sha256=hashlib.sha256(raw).hexdigest())
    changed = changed_manifest(tmp_path, descriptor, alter)
    with pytest.raises(ValueError, match='decompression|gzip'):
        storage.load_capture(tmp_path, changed)


def test_symlink_directory_rejected_when_host_supports_links(tmp_path):
    outside = tmp_path/'outside'
    outside.mkdir()
    root = tmp_path/'store'
    root.mkdir()
    try:
        (root/'blobs').symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip('Host does not permit test symlink creation: '+str(exc.winerror if hasattr(exc,'winerror') else exc.errno))
    with pytest.raises(ValueError, match='directory_escape'):
        storage.store_capture(root, capture())
    assert not list(outside.iterdir())


def test_detected_link_is_rejected_without_needing_host_symlink_privilege(tmp_path, monkeypatch):
    original = Path.is_symlink
    monkeypatch.setattr(Path, 'is_symlink', lambda path: path.name == 'blobs' or original(path))
    with pytest.raises(ValueError, match='directory_escape'):
        storage.store_capture(tmp_path, capture())


@pytest.mark.skipif(sys.platform != 'win32', reason='Windows junction semantics')
def test_real_windows_directory_junction_cannot_redirect_capture_blobs(tmp_path):
    outside = tmp_path/'outside'
    outside.mkdir()
    root = tmp_path/'store'
    root.mkdir()
    script = tmp_path/'make_test_junction.ps1'
    script.write_text('param([string]$LinkPath,[string]$TargetPath)\n'
                      'New-Item -ItemType Junction -Path $LinkPath -Target $TargetPath -ErrorAction Stop | Out-Null\n',
                      encoding='utf-8')
    subprocess.run(['powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                    '-File', str(script), '-LinkPath', str(root/'blobs'), '-TargetPath', str(outside)],
                   check=True, capture_output=True, text=True, timeout=15)
    assert (root/'blobs').is_junction()
    with pytest.raises(ValueError, match='directory_escape'):
        storage.store_capture(root, capture())
    assert not list(outside.iterdir())


def test_concurrent_identical_writers_publish_one_immutable_generation(tmp_path):
    value = capture(600)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: storage.store_capture(tmp_path, value), range(4)))
    assert all(result[0] == results[0][0] for result in results)
    assert len(list((tmp_path/'manifests').glob('*.json'))) == 1
    assert not list(tmp_path.rglob('*.tmp'))
    assert storage.load_capture(tmp_path, results[0][0]) == value


def test_failed_manifest_publish_leaves_reusable_orphans_but_no_claim(tmp_path, monkeypatch):
    publish = storage._publish_new
    def fail_manifest(path, raw):
        if path.parent.name == 'manifests':
            raise OSError('synthetic publication failure')
        return publish(path, raw)
    monkeypatch.setattr(storage, '_publish_new', fail_manifest)
    with pytest.raises(OSError):
        storage.store_capture(tmp_path, capture())
    assert list((tmp_path/'blobs').glob('*.gz'))
    assert not list((tmp_path/'manifests').glob('*.json'))
    monkeypatch.setattr(storage, '_publish_new', publish)
    descriptor, stats = storage.store_capture(tmp_path, capture())
    assert stats['new_blob_count'] == 0
    assert storage.load_capture(tmp_path, descriptor) == capture()


def test_atomic_publish_failure_never_exposes_partial_file(tmp_path, monkeypatch):
    monkeypatch.setattr(storage.os, 'fsync', lambda _: (_ for _ in ()).throw(OSError('synthetic sync failure')))
    with pytest.raises(OSError):
        storage.store_capture(tmp_path, capture())
    assert not [p for p in tmp_path.rglob('*') if p.is_file()]


def test_reader_keeps_prior_generation_during_later_publish(tmp_path, monkeypatch):
    original = capture()
    first, _ = storage.store_capture(tmp_path, original)
    later = copy.deepcopy(original)
    later['first_observed_epoch'] += 10
    seal(later)
    waiting, release = threading.Event(), threading.Event()
    publish = storage._publish_new
    def pause(path, raw):
        if path.parent.name == 'manifests':
            waiting.set()
            assert release.wait(10), 'Bounded synthetic writer did not resume'
        return publish(path, raw)
    monkeypatch.setattr(storage, '_publish_new', pause)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(storage.store_capture, tmp_path, later)
        assert waiting.wait(10)
        try:
            assert storage.load_capture(tmp_path, first) == original
            assert len(list((tmp_path/'manifests').glob('*.json'))) == 1
        finally:
            release.set()
        second, _ = future.result(timeout=10)
    assert storage.load_capture(tmp_path, second) == later


def diagnostic(root, **kwargs):
    arguments = {'producer':'oanda_news_fixture_v1.py', 'event_code':'capture_failed',
                 'source_bindings':{'oanda_news_fixture_v1.py':'a'*64},
                 'counters':{'cumulative_errors':3}, 'clock':lambda:1788920000.25}
    arguments.update(kwargs)
    return storage.append_diagnostic(root, **arguments)


def test_error_and_recovery_are_both_durable_with_original_clocks(tmp_path):
    first = diagnostic(tmp_path, occurred_epoch=1788919999.75)
    second = diagnostic(tmp_path, event_code='recovered', clock=lambda:1788920100.0)
    error = storage.read_diagnostic(tmp_path, first)
    recovery = storage.read_diagnostic(tmp_path, second)
    assert error['event_code'] == 'capture_failed' and recovery['event_code'] == 'recovered'
    assert error['occurred_epoch'] == 1788919999.75
    assert error['recorded_epoch'] == 1788920000.25
    assert error['counters']['cumulative_errors'] == 3
    assert len(list((tmp_path/'diagnostics').glob('*.json'))) == 2
    assert not {'message','exception','articles','account_id'} & set(error)


@pytest.mark.parametrize('kwargs', [
    {'event_code':'raw article or credential text'}, {'producer':'../escape.py'},
    {'occurred_epoch':1788920001}, {'clock':lambda:float('nan')},
    {'counters':{'message':'not allowed'}}, {'counters':{'cumulative_errors':True}},
    {'source_bindings':{'oanda_news_fixture_v1.py':'not-a-hash'}},
    {'source_bindings':{'oanda_another_fixture_v1.py':'a'*64}},
    {'capture_sha256':'http://private.invalid'},
])
def test_diagnostic_rejects_raw_text_future_clocks_and_invalid_bindings(tmp_path, kwargs):
    with pytest.raises(ValueError):
        diagnostic(tmp_path, **kwargs)
    assert not list(tmp_path.rglob('*.json'))


def test_journal_failure_is_visible_and_does_not_destroy_previous_event(tmp_path, monkeypatch):
    first = diagnostic(tmp_path)
    monkeypatch.setattr(storage.os, 'link', lambda *args: (_ for _ in ()).throw(OSError('synthetic link failure')))
    with pytest.raises(OSError):
        diagnostic(tmp_path, event_code='recovered')
    assert storage.read_diagnostic(tmp_path, first)['event_code'] == 'capture_failed'
    assert len(list((tmp_path/'diagnostics').glob('*.json'))) == 1
    assert not list(tmp_path.rglob('*.tmp'))


def test_concurrent_diagnostics_do_not_overwrite_last_error(tmp_path):
    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(lambda _: diagnostic(tmp_path), range(20)))
    assert len({r['sha256'] for r in receipts}) == 20
    assert all(storage.read_diagnostic(tmp_path, r)['counters']['cumulative_errors'] == 3 for r in receipts)


def test_diagnostic_corruption_rejected(tmp_path):
    receipt = diagnostic(tmp_path)
    path = tmp_path/'diagnostics'/(receipt['sha256']+'.json')
    path.write_bytes(path.read_bytes().replace(b'capture_failed', b'source_stale'))
    with pytest.raises(ValueError, match='receipt_hash'):
        storage.read_diagnostic(tmp_path, receipt)


@pytest.mark.parametrize('alter', [
    lambda e: e.update(message='raw text must not be an accepted event field'),
    lambda e: e.update(occurred_epoch=e['recorded_epoch']+1),
    lambda e: e.update(can_place_orders=True),
    lambda e: e.update(event_code=['capture_failed']),
    lambda e: e['counters'].update(cumulative_errors=True),
    lambda e: e.update(source_bindings={'oanda_another_fixture_v1.py':'a'*64}),
])
def test_diagnostic_replays_field_contract_after_matching_hash(tmp_path, alter):
    receipt = diagnostic(tmp_path)
    value = storage.read_diagnostic(tmp_path, receipt)
    alter(value)
    raw = storage.canonical_bytes(value)
    changed = {**receipt, 'sha256':hashlib.sha256(raw).hexdigest(), 'bytes':len(raw)}
    (tmp_path/'diagnostics'/(changed['sha256']+'.json')).write_bytes(raw)
    with pytest.raises(ValueError):
        storage.read_diagnostic(tmp_path, changed)
