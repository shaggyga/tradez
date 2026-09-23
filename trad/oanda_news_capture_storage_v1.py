"""Lossless research-capture storage and immutable sanitized diagnostics.

This module has no producer, broker, model, registry or runtime entrypoint. A new
caller/registration must explicitly adopt this descriptor. It verifies storage
integrity only; the caller must still replay all news/causal/source semantics.
Original capture contents, ordering, hash, availability and expiry are preserved.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
import uuid
import zlib

VERSION = 'news_capture_storage_v1_20260909'
DESCRIPTOR_SCHEMA = 'lossless_news_capture_descriptor_v1_20260909'
MANIFEST_SCHEMA = 'lossless_news_capture_manifest_v1_20260909'
DIAGNOSTIC_SCHEMA = 'research_news_diagnostic_event_v1_20260909'
CHUNK_METHOD = 'ordered_event_sha256_anchor_low6bits_zero_or_512k_v1'
MAX_LOGICAL_BYTES = 64 * 1024 * 1024
MAX_HISTORY_BYTES = 32 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024
MAX_MEMBERS_BYTES = 4 * 1024 * 1024
MAX_METADATA_BYTES = 128 * 1024
MAX_HISTORY_ROWS = 10000
MAX_MEMBER_ROWS = 8192
TARGET_CHUNK_BYTES = 512 * 1024
MAX_ROW_BYTES = 1024 * 1024
MAX_HISTORY_CHUNKS = 1024
MAX_STORED_BLOB_BYTES = 17 * 1024 * 1024
MAX_TOTAL_STORED_BYTES = 64 * 1024 * 1024
MAX_MANIFEST_BYTES = 512 * 1024
MAX_DIAGNOSTIC_BYTES = 16384
FLAGS = {'research_only': True, 'can_place_orders': False, 'can_promote': False,
         'can_authorize': False, 'account_eligible': False, 'proof_eligible': False}
LIMITS = {'logical_bytes': MAX_LOGICAL_BYTES, 'history_bytes': MAX_HISTORY_BYTES,
          'snapshot_bytes': MAX_SNAPSHOT_BYTES, 'members_bytes': MAX_MEMBERS_BYTES,
          'metadata_bytes': MAX_METADATA_BYTES, 'history_rows': MAX_HISTORY_ROWS,
          'member_rows': MAX_MEMBER_ROWS, 'row_bytes': MAX_ROW_BYTES,
          'history_chunks': MAX_HISTORY_CHUNKS, 'stored_blob_bytes': MAX_STORED_BLOB_BYTES,
          'total_stored_bytes': MAX_TOTAL_STORED_BYTES, 'manifest_bytes': MAX_MANIFEST_BYTES}
EVENT_CODES = frozenset({'capture_failed', 'source_stale', 'clock_rejected', 'identity_conflict',
                         'capacity_exceeded', 'history_incomplete', 'publication_failed',
                         'heartbeat_failed', 'source_binding_changed', 'recovered'})
COUNTERS = frozenset({'cumulative_errors', 'history_rows', 'source_rows', 'canonical_bytes',
                      'stored_bytes', 'limit_bytes', 'publication_errors'})


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _valid_hash(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def _integer(value, maximum, *, minimum=0):
    _require(type(value) is int and minimum <= value <= maximum, 'storage_invalid_integer_bound')
    return value


def _json_types(value, depth=0):
    _require(depth <= 64, 'storage_json_depth_bound')
    if isinstance(value, dict):
        _require(all(isinstance(k, str) for k in value), 'storage_json_string_keys_required')
        for child in value.values():
            _json_types(child, depth+1)
    elif isinstance(value, list):
        for child in value:
            _json_types(child, depth+1)
    else:
        _require(value is None or type(value) in (bool, int, float, str), 'storage_json_type')
        if type(value) is float:
            _require(math.isfinite(value), 'storage_nonfinite_number')


def canonical_bytes(value):
    _json_types(value)
    return _encoded_validated(value)


def _encoded_validated(value):
    """Encode already validated JSON-owned values without repeating tree walks.

    Only private paths after canonical_bytes or _decode may use this function.
    It does not change serialization, finite-number checks or capture hash scope.
    """
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'storage_duplicate_json_key')
            result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError('storage_nonfinite_number')))
    except (UnicodeError, RecursionError, json.JSONDecodeError) as exc:
        raise ValueError('storage_invalid_json') from exc
    _json_types(value)
    _require(_encoded_validated(value) == raw, 'storage_noncanonical_json')
    return value


def _safe_dir(root, name, *, create=False):
    root = Path(root).resolve()
    child = root / name
    if create:
        root.mkdir(parents=True, exist_ok=True)
        child.mkdir(exist_ok=True)
    _require(not child.is_symlink() and child.resolve() == child, 'storage_directory_escape')
    return child


def _read(path, maximum):
    before = path.stat()
    _require(before.st_size <= maximum and not path.is_symlink(), 'storage_read_bound_or_symlink')
    with path.open('rb') as handle:
        opened = os.fstat(handle.fileno())
        raw = handle.read(maximum+1)
        closed = os.fstat(handle.fileno())
    after = path.stat()
    identity = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns)
    _require(len(raw) <= maximum and len({identity(x) for x in (before, opened, closed, after)}) == 1,
             'storage_read_changed_or_oversized')
    return raw


def _unpack(raw, maximum):
    try:
        decoder = zlib.decompressobj(16+zlib.MAX_WBITS)
        value = decoder.decompress(raw, maximum+1)
        _require(len(value) <= maximum and not decoder.unconsumed_tail, 'storage_decompression_bound')
        _require(decoder.eof and not decoder.unused_data, 'storage_invalid_or_multimember_gzip')
        return value
    except zlib.error as exc:
        raise ValueError('storage_invalid_gzip') from exc


def _publish_new(path, raw):
    """Publish complete bytes without ever replacing an existing destination.

    A same-directory temporary file is flushed before atomic hard-link creation.
    Link failure propagates; no unsafe replace fallback is attempted. An orphaned
    complete blob is harmless if the later manifest fails; nothing is deleted.
    """
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.news-storage-', suffix='.tmp', delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
            return True
        except FileExistsError:
            return False
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)  # Only the newly created private temporary file.


def _put_blob(root, raw):
    path = _safe_dir(root, 'blobs', create=True) / (_hash(raw)+'.json.gz')
    created = False
    if not path.exists():
        compressed = gzip.compress(raw, mtime=0)
        _require(len(compressed) <= MAX_STORED_BLOB_BYTES, 'storage_compressed_blob_bound')
        created = _publish_new(path, compressed)
    stored = _read(path, MAX_STORED_BLOB_BYTES)
    _require(_unpack(stored, len(raw)) == raw, 'storage_immutable_blob_collision')
    return {'sha256': _hash(raw), 'canonical_bytes': len(raw), 'stored_bytes': len(stored),
            'stored_sha256': _hash(stored)}, created


def _get_blob(root, reference, maximum):
    _require(isinstance(reference, dict) and set(reference) == {'sha256', 'canonical_bytes', 'stored_bytes', 'stored_sha256'},
             'storage_blob_reference_schema')
    _require(_valid_hash(reference['sha256']) and _valid_hash(reference['stored_sha256']), 'storage_blob_reference_hash')
    length = _integer(reference['canonical_bytes'], maximum, minimum=1)
    stored_length = _integer(reference['stored_bytes'], MAX_STORED_BLOB_BYTES, minimum=1)
    path = _safe_dir(root, 'blobs') / (reference['sha256']+'.json.gz')
    stored = _read(path, stored_length)
    _require(len(stored) == stored_length and _hash(stored) == reference['stored_sha256'], 'storage_compressed_blob_hash')
    raw = _unpack(stored, length)
    _require(len(raw) == length and _hash(raw) == reference['sha256'], 'storage_canonical_blob_hash')
    return _decode(raw)


def _history_chunks(rows):
    chunk, size = [], 2
    seen = set()
    for row in rows:
        _require(isinstance(row, dict), 'storage_history_row_object')
        identity = row.get('source_event_id')
        _require(isinstance(identity, str) and 1 <= len(identity) <= 512 and identity not in seen,
                 'storage_history_event_identity')
        seen.add(identity)
        row_raw = _encoded_validated(row)
        _require(len(row_raw) <= MAX_ROW_BYTES, 'storage_history_row_bound')
        if chunk and size+len(row_raw)+1 > TARGET_CHUNK_BYTES:
            yield _encoded_validated(chunk), len(chunk)
            chunk, size = [], 2
        chunk.append(row)
        size += len(row_raw)+1
        if int.from_bytes(hashlib.sha256(identity.encode('utf-8')).digest()[:8], 'big') & 63 == 0:
            yield _encoded_validated(chunk), len(chunk)
            chunk, size = [], 2
    if chunk:
        yield _encoded_validated(chunk), len(chunk)


def _capture_parts(capture, *, raw=None):
    _require(isinstance(capture, dict) and all(capture.get(k) is v for k,v in FLAGS.items()), 'storage_capture_inert_flags')
    _require(_valid_hash(capture.get('news_capture_sha256')), 'storage_capture_seal_required')
    _require(isinstance(capture.get('history'), list) and len(capture['history']) <= MAX_HISTORY_ROWS, 'storage_history_row_count')
    _require(isinstance(capture.get('current_snapshot'), dict), 'storage_snapshot_object_required')
    _require(isinstance(capture.get('current_members'), list) and len(capture['current_members']) <= MAX_MEMBER_ROWS,
             'storage_member_row_count')
    raw = _encoded_validated(capture) if raw is None else raw
    _require(len(raw) <= MAX_LOGICAL_BYTES, 'storage_logical_byte_bound')
    _require(_hash(_encoded_validated({k:v for k,v in capture.items() if k != 'news_capture_sha256'})) == capture['news_capture_sha256'],
             'storage_capture_seal_mismatch')
    values = {'history': capture['history'], 'snapshot': capture['current_snapshot'], 'members': capture['current_members'],
              'metadata': {k:v for k,v in capture.items() if k not in {'history', 'current_snapshot', 'current_members'}}}
    bounds = {'history': MAX_HISTORY_BYTES, 'snapshot': MAX_SNAPSHOT_BYTES, 'members': MAX_MEMBERS_BYTES, 'metadata': MAX_METADATA_BYTES}
    parts = {key: _encoded_validated(value) for key,value in values.items()}
    for key, part in parts.items():
        _require(len(part) <= bounds[key], 'storage_'+key+'_byte_bound')
    return raw, parts


def store_capture(root, capture):
    """Store all captured evidence. Return a new, explicitly versioned descriptor.

    Storage does not renew clocks or certify currentness. No original capture or
    registered source is modified, even when the same original seal is reused.
    """
    # Freeze one caller view before planning/reusing any parts; caller mutation
    # cannot splice different generations into an otherwise valid descriptor.
    snapshot = canonical_bytes(capture)
    _require(len(snapshot) <= MAX_LOGICAL_BYTES, 'storage_logical_byte_bound')
    # This is our own canonical finite JSON, already type/depth checked above.
    # Decode into private ownership; reserializing it here would be redundant.
    capture = json.loads(snapshot)
    raw, parts = _capture_parts(capture, raw=snapshot)
    chunks = list(_history_chunks(capture['history']))
    _require(len(chunks) <= MAX_HISTORY_CHUNKS, 'storage_history_chunk_bound')
    references, history = {}, []
    created_count, created_bytes, reused = 0, 0, 0
    for name in ('metadata', 'snapshot', 'members'):
        ref, created = _put_blob(root, parts[name])
        references[name] = ref
        created_count += created
        created_bytes += ref['stored_bytes'] if created else 0
        reused += not created
    for chunk, row_count in chunks:
        ref, created = _put_blob(root, chunk)
        history.append({'blob': ref, 'rows': row_count})
        created_count += created
        created_bytes += ref['stored_bytes'] if created else 0
        reused += not created
    total_stored = sum(r['stored_bytes'] for r in references.values()) + sum(r['blob']['stored_bytes'] for r in history)
    _require(total_stored <= MAX_TOTAL_STORED_BYTES, 'storage_total_compressed_bound')
    manifest = {'schema_version': MANIFEST_SCHEMA, 'storage_version': VERSION, 'chunk_method': CHUNK_METHOD,
                'capture_sha256': capture['news_capture_sha256'], 'canonical_sha256': _hash(raw), 'canonical_bytes': len(raw),
                'history_rows': len(capture['history']), 'history_bytes': len(parts['history']), 'history': history,
                'components': references, 'limits': LIMITS, 'storage_integrity_only': True, **FLAGS}
    manifest_raw = canonical_bytes(manifest)
    _require(len(manifest_raw) <= MAX_MANIFEST_BYTES, 'storage_manifest_byte_bound')
    manifest_sha = _hash(manifest_raw)
    path = _safe_dir(root, 'manifests', create=True) / (manifest_sha+'.json')
    if not path.exists():
        _publish_new(path, manifest_raw)
    _require(_read(path, MAX_MANIFEST_BYTES) == manifest_raw, 'storage_immutable_manifest_collision')
    return {'schema_version': DESCRIPTOR_SCHEMA, 'storage_version': VERSION, 'manifest_sha256': manifest_sha,
            'capture_sha256': capture['news_capture_sha256'], 'canonical_sha256': _hash(raw), 'canonical_bytes': len(raw),
            'storage_integrity_only': True, **FLAGS}, {
                'new_blob_count': created_count, 'reused_blob_count': reused, 'new_blob_stored_bytes': created_bytes,
                'referenced_compressed_bytes': total_stored, 'history_chunks': len(history), 'manifest_bytes': len(manifest_raw)}


def load_capture(root, descriptor):
    """Reconstruct and hash-check complete original content; never validate freshness."""
    keys = {'schema_version', 'storage_version', 'manifest_sha256', 'capture_sha256', 'canonical_sha256',
            'canonical_bytes', 'storage_integrity_only', *FLAGS}
    _require(isinstance(descriptor, dict) and set(descriptor) == keys, 'storage_descriptor_schema')
    _require(descriptor['schema_version'] == DESCRIPTOR_SCHEMA and descriptor['storage_version'] == VERSION
             and descriptor['storage_integrity_only'] is True and all(descriptor.get(k) is v for k,v in FLAGS.items()),
             'storage_descriptor_identity_or_flags')
    for name in ('manifest_sha256', 'capture_sha256', 'canonical_sha256'):
        _require(_valid_hash(descriptor[name]), 'storage_descriptor_hash')
    _integer(descriptor['canonical_bytes'], MAX_LOGICAL_BYTES, minimum=1)
    path = _safe_dir(root, 'manifests') / (descriptor['manifest_sha256']+'.json')
    raw = _read(path, MAX_MANIFEST_BYTES)
    _require(_hash(raw) == descriptor['manifest_sha256'], 'storage_manifest_hash')
    manifest = _decode(raw)
    expected_keys = {'schema_version','storage_version','chunk_method','capture_sha256','canonical_sha256','canonical_bytes',
                     'history_rows','history_bytes','history','components','limits','storage_integrity_only',*FLAGS}
    _require(isinstance(manifest, dict) and set(manifest) == expected_keys, 'storage_manifest_schema')
    _require(manifest['schema_version'] == MANIFEST_SCHEMA and manifest['storage_version'] == VERSION
             and manifest['chunk_method'] == CHUNK_METHOD and manifest['limits'] == LIMITS
             and manifest['storage_integrity_only'] is True and all(manifest.get(k) is v for k,v in FLAGS.items()),
             'storage_manifest_identity_or_limits')
    _require(all(manifest[k] == descriptor[k] for k in ('capture_sha256', 'canonical_sha256', 'canonical_bytes')),
             'storage_manifest_capture_binding')
    _integer(manifest['history_rows'], MAX_HISTORY_ROWS)
    _integer(manifest['history_bytes'], MAX_HISTORY_BYTES, minimum=2)
    refs = manifest['components']
    _require(isinstance(refs, dict) and set(refs) == {'metadata', 'snapshot', 'members'}, 'storage_components_schema')
    history_refs = manifest['history']
    _require(isinstance(history_refs, list) and len(history_refs) <= MAX_HISTORY_CHUNKS, 'storage_history_chunk_bound')
    rows_declared, total_stored = 0, 0
    for item in history_refs:
        _require(isinstance(item, dict) and set(item) == {'blob', 'rows'}, 'storage_history_reference_schema')
        rows_declared += _integer(item['rows'], MAX_HISTORY_ROWS, minimum=1)
    _require(rows_declared == manifest['history_rows'], 'storage_history_row_count_binding')
    for ref in list(refs.values()) + [x['blob'] for x in history_refs]:
        _require(isinstance(ref, dict), 'storage_blob_reference_schema')
        total_stored += _integer(ref.get('stored_bytes'), MAX_STORED_BLOB_BYTES, minimum=1)
    _require(total_stored <= MAX_TOTAL_STORED_BYTES, 'storage_total_compressed_bound')
    metadata = _get_blob(root, refs['metadata'], MAX_METADATA_BYTES)
    snapshot = _get_blob(root, refs['snapshot'], MAX_SNAPSHOT_BYTES)
    members = _get_blob(root, refs['members'], MAX_MEMBERS_BYTES)
    history, history_bytes = [], 2
    for item in history_refs:
        rows = _get_blob(root, item['blob'], MAX_ROW_BYTES+2)
        _require(isinstance(rows, list) and len(rows) == item['rows'], 'storage_history_chunk_row_binding')
        history_bytes += len(_encoded_validated(rows))-2 + (1 if history else 0)
        _require(history_bytes <= MAX_HISTORY_BYTES, 'storage_history_byte_bound')
        history.extend(rows)
    _require(history_bytes == manifest['history_bytes'], 'storage_history_byte_binding')
    _require(isinstance(metadata, dict) and not {'history', 'current_snapshot', 'current_members'} & set(metadata),
             'storage_metadata_reserved_keys')
    capture = {**metadata, 'history': history, 'current_snapshot': snapshot, 'current_members': members}
    rebuilt, _ = _capture_parts(capture)
    _require(len(rebuilt) == descriptor['canonical_bytes'] and _hash(rebuilt) == descriptor['canonical_sha256'], 'storage_reconstructed_capture_hash')
    _require(capture['news_capture_sha256'] == descriptor['capture_sha256'], 'storage_original_capture_hash')
    # The declared deterministic partition itself is verified, not merely its rows.
    expected = [(_hash(part), count) for part,count in _history_chunks(history)]
    actual = [(item['blob']['sha256'], item['rows']) for item in history_refs]
    _require(expected == actual, 'storage_history_partition_binding')
    return capture


def append_diagnostic(root, *, producer, event_code, source_bindings, counters=None,
                      capture_sha256=None, occurred_epoch=None, clock=time.time):
    """Append one durable bounded error/recovery event, without raw exception text.

    The caller supplies a code from a fixed taxonomy; raw articles, messages,
    URLs, account IDs and credentials are not accepted fields. Write failure
    propagates, so a future producer must expose journal failure explicitly.
    """
    recorded = clock()
    _require(type(recorded) in (int, float) and math.isfinite(recorded) and recorded > 0, 'diagnostic_record_clock')
    occurred = recorded if occurred_epoch is None else occurred_epoch
    _require(type(occurred) in (int, float) and math.isfinite(occurred) and 0 < occurred <= recorded, 'diagnostic_event_clock')
    _require(isinstance(producer, str) and re.fullmatch(r'oanda_[a-z0-9_]{1,120}\.py', producer), 'diagnostic_producer_name')
    _require(isinstance(event_code, str) and event_code in EVENT_CODES, 'diagnostic_unknown_event_code')
    _require(isinstance(source_bindings, dict) and 1 <= len(source_bindings) <= 32, 'diagnostic_source_bindings')
    for name, value in source_bindings.items():
        _require(isinstance(name, str) and re.fullmatch(r'oanda_[a-z0-9_]{1,120}\.py', name) and _valid_hash(value), 'diagnostic_source_binding')
    _require(producer in source_bindings, 'diagnostic_producer_binding_missing')
    counters = {} if counters is None else counters
    _require(isinstance(counters, dict) and set(counters) <= COUNTERS, 'diagnostic_counter_names')
    for number in counters.values():
        _integer(number, 10**15)
    _require(capture_sha256 is None or _valid_hash(capture_sha256), 'diagnostic_capture_hash')
    event = {'schema_version': DIAGNOSTIC_SCHEMA, 'storage_version': VERSION, 'event_id': uuid.uuid4().hex,
             'producer': producer, 'event_code': event_code, 'occurred_epoch': occurred, 'recorded_epoch': recorded,
             'source_bindings': dict(source_bindings), 'counters': dict(counters), 'capture_sha256': capture_sha256,
             'scope': 'durable_diagnostic_only_not_forecast_evidence', **FLAGS}
    raw = canonical_bytes(event)
    _require(len(raw) <= MAX_DIAGNOSTIC_BYTES, 'diagnostic_event_byte_bound')
    sha = _hash(raw)
    path = _safe_dir(root, 'diagnostics', create=True) / (sha+'.json')
    _publish_new(path, raw)
    _require(_read(path, MAX_DIAGNOSTIC_BYTES) == raw, 'diagnostic_immutable_collision')
    return {'schema_version': DIAGNOSTIC_SCHEMA, 'sha256': sha, 'bytes': len(raw),
            'event_id': event['event_id'], 'recorded_epoch': recorded, **FLAGS}


def read_diagnostic(root, receipt):
    receipt_keys = {'schema_version', 'sha256', 'bytes', 'event_id', 'recorded_epoch', *FLAGS}
    _require(isinstance(receipt, dict) and set(receipt) == receipt_keys and receipt.get('schema_version') == DIAGNOSTIC_SCHEMA
             and _valid_hash(receipt.get('sha256')), 'diagnostic_receipt_schema')
    _integer(receipt['bytes'], MAX_DIAGNOSTIC_BYTES, minimum=1)
    _require(all(receipt.get(k) is v for k,v in FLAGS.items()), 'diagnostic_receipt_flags')
    raw = _read(_safe_dir(root, 'diagnostics') / (receipt['sha256']+'.json'), MAX_DIAGNOSTIC_BYTES)
    _require(len(raw) == receipt.get('bytes') and _hash(raw) == receipt['sha256'], 'diagnostic_receipt_hash')
    event = _decode(raw)
    _require(isinstance(event, dict), 'diagnostic_event_schema')
    _require(event.get('event_id') == receipt.get('event_id') and event.get('recorded_epoch') == receipt.get('recorded_epoch'),
             'diagnostic_receipt_identity')
    _require(event.get('schema_version') == DIAGNOSTIC_SCHEMA and event.get('storage_version') == VERSION
             and isinstance(event.get('event_code'), str) and event.get('event_code') in EVENT_CODES
             and all(event.get(k) is v for k,v in FLAGS.items()),
             'diagnostic_event_identity_or_flags')
    event_keys = {'schema_version', 'storage_version', 'event_id', 'producer', 'event_code',
                  'occurred_epoch', 'recorded_epoch', 'source_bindings', 'counters', 'capture_sha256', 'scope', *FLAGS}
    _require(set(event) == event_keys and event['scope'] == 'durable_diagnostic_only_not_forecast_evidence'
             and isinstance(event['event_id'], str) and re.fullmatch('[a-f0-9]{32}', event['event_id']),
             'diagnostic_event_schema')
    occurred, recorded = event['occurred_epoch'], event['recorded_epoch']
    _require(type(occurred) in (int,float) and type(recorded) in (int,float)
             and math.isfinite(occurred) and math.isfinite(recorded) and 0 < occurred <= recorded,
             'diagnostic_event_clock')
    producer, bindings, counters = event['producer'], event['source_bindings'], event['counters']
    _require(isinstance(producer, str) and re.fullmatch(r'oanda_[a-z0-9_]{1,120}\.py', producer), 'diagnostic_producer_name')
    _require(isinstance(bindings, dict) and 1 <= len(bindings) <= 32 and producer in bindings, 'diagnostic_source_bindings')
    for name, value in bindings.items():
        _require(isinstance(name, str) and re.fullmatch(r'oanda_[a-z0-9_]{1,120}\.py', name) and _valid_hash(value),
                 'diagnostic_source_binding')
    _require(isinstance(counters, dict) and set(counters) <= COUNTERS, 'diagnostic_counter_names')
    for number in counters.values():
        _integer(number, 10**15)
    _require(event['capture_sha256'] is None or _valid_hash(event['capture_sha256']), 'diagnostic_capture_hash')
    return event
