"""Bounded, append-only, lossless storage for passive current meter captures.

Adopts the canonical content-addressing, bounded decompression and immutable
evidence principles in trad/oanda_news_capture_storage_v1.py. This descriptor
uses SQLite row-version deduplication and ordered references; it does not alter
the source or feature contracts, certify their meaning, or renew their clocks.
No broker, network, producer, historical backfill or executable entrypoint.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import sqlite3
import zlib

VERSION = 'current_meter_content_addressed_store_v1_20260912'
APPLICATION_ID = 1296389169
SCHEMA_VERSION = 1
PAGE_SIZE = 4096
TABLES = {'store_metadata', 'blobs', 'captures', 'row_refs', 'publication_receipts'}


class StorageError(ValueError):
    pass


class CapacityError(StorageError):
    pass


class DuplicateBucketError(StorageError):
    def __init__(self, capture_id):
        self.capture_id = capture_id
        super().__init__('context_bucket_already_captured:' + capture_id)


@dataclass(frozen=True)
class Limits:
    max_database_bytes: int = 512 * 1024 * 1024
    max_unique_logical_bytes: int = 2 * 1024 * 1024 * 1024
    min_free_disk_bytes: int = 256 * 1024 * 1024
    max_capture_bytes: int = 136 * 1024 * 1024
    max_row_bytes: int = 1024 * 1024
    max_rows: int = 10000
    max_feature_bytes: int = 8 * 1024 * 1024
    max_envelope_bytes: int = 2 * 1024 * 1024


def utcnow():
    return datetime.now(timezone.utc)


def _require(condition, reason):
    if not condition:
        raise StorageError(reason)


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _types(value, depth=0):
    _require(depth <= 64, 'json_depth_bound')
    if isinstance(value, dict):
        _require(all(isinstance(k, str) for k in value), 'json_string_keys_required')
        for item in value.values():
            _types(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            _types(item, depth + 1)
    elif isinstance(value, datetime):
        _require(value.tzinfo is not None and value.utcoffset() is not None, 'naive_datetime')
    else:
        _require(value is None or type(value) in (str, bool, int, float), 'json_value_type')
        if type(value) is float:
            _require(math.isfinite(value), 'nonfinite_json')


def canonical_bytes(value):
    """Exactly the prior collector's encoding, including escaped Unicode."""
    _types(value)
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True,
                      allow_nan=False, default=lambda x: x.isoformat()).encode('utf-8')


def _decode(raw):
    def unique_pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'duplicate_json_key')
            result[key] = value
        return result
    try:
        result = json.loads(raw, object_pairs_hook=unique_pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(StorageError('nonfinite_json')))
        _require(canonical_bytes(result) == raw, 'noncanonical_json')
        return result
    except (UnicodeError, RecursionError, json.JSONDecodeError) as exc:
        raise StorageError('invalid_json') from exc


def _epoch(value):
    try:
        dt = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace('Z', '+00:00'))
        _require(dt.tzinfo is not None and dt.utcoffset() is not None, 'naive_clock')
        return dt.timestamp()
    except (TypeError, AttributeError, ValueError, OverflowError) as exc:
        raise StorageError('invalid_clock') from exc


def _bucket(value):
    if type(value) in (int, float):
        _require(math.isfinite(value), 'invalid_context_bucket')
        seconds = value
    else:
        seconds = _epoch(value)
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def _limits(limits):
    _require(isinstance(limits, Limits), 'limits_type')
    for key, value in asdict(limits).items():
        _require(type(value) is int and value >= (0 if key == 'min_free_disk_bytes' else 1), 'invalid_limit:' + key)
    _require(limits.max_database_bytes <= (2**31 - 1) * PAGE_SIZE, 'database_limit_too_large')


def _capacity(condition, reason):
    if not condition:
        raise CapacityError(reason)


def _prepare(raw, feature, capture_id, limits, *, metadata_only=False):
    _require(isinstance(capture_id, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,160}', capture_id), 'invalid_capture_id')
    # Only _read may skip freezing: it exclusively owns the just-decoded blobs,
    # already checked every byte/hash/size and reconstructed the complete seal.
    if not metadata_only:
        raw_bytes, feature_bytes = canonical_bytes(raw), canonical_bytes(feature)
        _capacity(len(raw_bytes) + len(feature_bytes) <= limits.max_capture_bytes, 'capture_byte_limit')
        _capacity(len(feature_bytes) <= limits.max_feature_bytes, 'feature_byte_limit')
        raw, feature = _decode(raw_bytes), _decode(feature_bytes)
    _require(isinstance(raw, dict) and isinstance(feature, dict), 'capture_objects_required')
    rows = raw.get('original_rows')
    _require(isinstance(rows, list) and all(isinstance(r, dict) for r in rows), 'original_row_objects_required')
    _capacity(len(rows) <= limits.max_rows, 'row_count_limit')
    _require(feature.get('capture_id') == capture_id, 'feature_capture_identity')
    _require(feature.get('research_only') is True and feature.get('execution_eligible') is False, 'inert_feature_required')
    _require(feature.get('forecast') is False, 'feature_is_not_forecast')
    contract, cohort = feature.get('schema_version'), feature.get('cohort_id')
    _require(all(isinstance(x, str) and 1 <= len(x) <= 256 for x in (contract, cohort)), 'source_contract_identity')
    bindings = raw.get('source_bindings')
    _require(isinstance(bindings, dict) and bindings and bindings == feature.get('source_bindings'), 'source_bindings_mismatch')
    _require(all(isinstance(k, str) and isinstance(v, str) and re.fullmatch('[a-f0-9]{64}', v) for k, v in bindings.items()), 'invalid_source_hashes')
    states = feature.get('current_states')
    _require(isinstance(states, list) and states and all(isinstance(s, dict) for s in states), 'states_required')
    clocks = {s.get('clock_utc') for s in states}
    _require(len(clocks) == 1, 'mixed_context_clocks')
    bucket = _bucket(next(iter(clocks)))
    times = [_epoch(raw['read_receipt'][key]) for key in ('read_started_utc', 'read_completed_utc')]
    times += [_epoch(feature[key]) for key in ('computation_started_utc', 'computation_completed_utc')]
    _require(times == sorted(times) and _epoch(bucket) <= times[2], 'capture_clock_order')
    metadata = {'contract': contract, 'cohort': cohort, 'bucket': bucket,
                'source_sha': _hash(canonical_bytes(bindings)),
                'computation_completed_utc': feature['computation_completed_utc']}
    if metadata_only:
        return metadata
    # Deduplication uses entire original row versions, not event IDs or headlines.
    blobs = {}
    def add(value, maximum):
        encoded = canonical_bytes(value)
        _capacity(len(encoded) <= maximum, 'component_byte_limit')
        digest = _hash(encoded)
        blobs[digest] = (encoded, zlib.compress(encoded))
        return digest
    refs = [add(row, limits.max_row_bytes) for row in rows]
    envelope = add({key: value for key, value in raw.items() if key != 'original_rows'}, limits.max_envelope_bytes)
    feature_sha = add(feature, limits.max_feature_bytes)
    # The ordered manifest itself is content-addressed. An unchanged selection
    # reuses one compact blob, instead of inserting thousands of row references.
    manifest_sha = add(refs, limits.max_rows * 68 + 2)
    return {'raw_bytes': raw_bytes, 'feature_bytes': feature_bytes, 'raw_sha': _hash(raw_bytes),
            'feature_sha': feature_sha, 'envelope_sha': envelope, 'refs': refs, 'manifest_sha': manifest_sha, 'blobs': blobs,
            **metadata}


def _identity(c, *, allow_empty=False):
    tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    if not tables and allow_empty:
        return False
    _require(tables == TABLES and c.execute('PRAGMA application_id').fetchone()[0] == APPLICATION_ID
             and c.execute('PRAGMA user_version').fetchone()[0] == SCHEMA_VERSION, 'foreign_or_changed_storage_schema')
    _require(c.execute('SELECT version FROM store_metadata').fetchall() == [(VERSION,)], 'storage_version_mismatch')
    return True


def _initialize(c):
    statements = [
        'CREATE TABLE store_metadata(version TEXT PRIMARY KEY)',
        'CREATE TABLE blobs(sha256 TEXT PRIMARY KEY, logical_bytes INTEGER NOT NULL, stored_sha256 TEXT NOT NULL, compressed BLOB NOT NULL)',
        'CREATE TABLE captures(capture_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, cohort_id TEXT NOT NULL, context_bucket TEXT NOT NULL, raw_sha256 TEXT NOT NULL, raw_bytes INTEGER NOT NULL, envelope_sha256 TEXT NOT NULL REFERENCES blobs(sha256), feature_sha256 TEXT NOT NULL REFERENCES blobs(sha256), feature_bytes INTEGER NOT NULL, source_bindings_sha256 TEXT NOT NULL, row_count INTEGER NOT NULL, computation_completed_utc TEXT NOT NULL, UNIQUE(cohort_id,context_bucket))',
        'CREATE TABLE row_refs(capture_id TEXT PRIMARY KEY REFERENCES captures(capture_id), manifest_sha256 TEXT NOT NULL REFERENCES blobs(sha256))',
        'CREATE TABLE publication_receipts(capture_id TEXT PRIMARY KEY REFERENCES captures(capture_id), receipt_sha256 TEXT NOT NULL, receipt_json BLOB NOT NULL)',
    ]
    for statement in statements:
        c.execute(statement)
    c.execute('INSERT INTO store_metadata VALUES(?)', (VERSION,))
    for table in sorted(TABLES):
        for action in ('UPDATE', 'DELETE'):
            c.execute(f"CREATE TRIGGER immutable_{table}_{action.lower()} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'append_only_evidence'); END")
    c.execute(f'PRAGMA application_id={APPLICATION_ID}')
    c.execute(f'PRAGMA user_version={SCHEMA_VERSION}')


@contextmanager
def _connect_read(database, *, allow_empty=False):
    path = Path(database)
    _require(not path.is_symlink(), 'database_symlink')
    c = sqlite3.connect(path.resolve(strict=True).as_uri() + '?mode=ro', uri=True, timeout=10)
    try:
        c.execute('PRAGMA query_only=ON')
        c.execute('BEGIN')
        _identity(c, allow_empty=allow_empty)
        yield c
    finally:
        c.close()


def _blob(c, digest, maximum):
    row = c.execute('SELECT logical_bytes, stored_sha256, compressed FROM blobs WHERE sha256=?', (digest,)).fetchone()
    _require(row is not None, 'missing_blob_reference')
    size, stored_sha, compressed = row
    _require(type(size) is int and 0 < size <= maximum and len(compressed) <= maximum + 65536, 'blob_size_bound')
    _require(_hash(compressed) == stored_sha, 'compressed_blob_hash_mismatch')
    try:
        decoder = zlib.decompressobj()
        encoded = decoder.decompress(compressed, size + 1)
        _require(len(encoded) == size and decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
                 'invalid_or_oversized_compressed_blob')
    except zlib.error as exc:
        raise StorageError('invalid_compression') from exc
    _require(_hash(encoded) == digest, 'blob_content_hash_mismatch')
    return _decode(encoded)


def _read(c, capture_id, *, require_receipt=True):
    row = c.execute('SELECT * FROM captures WHERE capture_id=?', (capture_id,)).fetchone()
    _require(row is not None, 'capture_missing')
    columns = [d[0] for d in c.execute('SELECT * FROM captures LIMIT 0').description]
    record = dict(zip(columns, row))
    hard = Limits()
    _require(type(record['raw_bytes']) is int and 0 < record['raw_bytes'] <= hard.max_capture_bytes
             and type(record['feature_bytes']) is int and 0 < record['feature_bytes'] <= hard.max_feature_bytes,
             'declared_capture_size_bound')
    raw = _blob(c, record['envelope_sha256'], hard.max_envelope_bytes)
    _require(isinstance(raw, dict) and 'original_rows' not in raw, 'invalid_raw_envelope')
    manifest = c.execute('SELECT manifest_sha256 FROM row_refs WHERE capture_id=?', (capture_id,)).fetchone()
    _require(manifest is not None, 'missing_ordered_row_manifest')
    refs = _blob(c, manifest[0], hard.max_rows * 68 + 2)
    _require(isinstance(refs, list) and len(refs) == record['row_count'] and len(refs) <= hard.max_rows
             and all(isinstance(r, str) and re.fullmatch('[a-f0-9]{64}', r) for r in refs), 'invalid_ordered_row_manifest')
    original_rows, row_bytes = [], 0
    for digest in refs:
        original = _blob(c, digest, hard.max_row_bytes)
        row_bytes += len(canonical_bytes(original))
        _require(row_bytes <= record['raw_bytes'], 'reconstructed_row_bytes_bound')
        original_rows.append(original)
    raw['original_rows'] = original_rows
    feature = _blob(c, record['feature_sha256'], hard.max_feature_bytes)
    raw_bytes, feature_bytes = canonical_bytes(raw), canonical_bytes(feature)
    _require(len(raw_bytes) == record['raw_bytes'] and _hash(raw_bytes) == record['raw_sha256']
             and len(feature_bytes) == record['feature_bytes']
             and len(raw_bytes) + len(feature_bytes) <= hard.max_capture_bytes, 'reconstructed_capture_hash_mismatch')
    prepared = _prepare(raw, feature, capture_id, hard, metadata_only=True)
    for key, expected in [('contract_id', prepared['contract']), ('cohort_id', prepared['cohort']),
                          ('context_bucket', prepared['bucket']), ('source_bindings_sha256', prepared['source_sha']),
                          ('computation_completed_utc', prepared['computation_completed_utc'])]:
        _require(record[key] == expected, 'capture_metadata_mismatch:' + key)
    sealed = c.execute('SELECT receipt_sha256,receipt_json FROM publication_receipts WHERE capture_id=?', (capture_id,)).fetchone()
    if sealed is None:
        _require(not require_receipt, 'capture_not_published')
        return {'raw': raw, 'feature': feature, 'receipt': None}
    digest, encoded = sealed
    _require(_hash(encoded) == digest, 'receipt_hash_mismatch')
    receipt = _decode(encoded)
    expected = {'capture_id': capture_id, 'storage_version': VERSION, 'contract_id': prepared['contract'],
                'cohort_id': prepared['cohort'], 'context_bucket': prepared['bucket'], 'raw_sha256': record['raw_sha256'],
                'feature_sha256': record['feature_sha256'], 'source_bindings_sha256': prepared['source_sha'],
                'research_only': True, 'execution_eligible': False, 'can_place_orders': False, 'can_promote': False,
                'availability_rule': 'post_FULL_commit_independent_exact_payload_readback'}
    _require(all(receipt.get(k) == v and type(receipt.get(k)) is type(v) for k, v in expected.items()), 'receipt_metadata_mismatch')
    _require(_epoch(receipt.get('observed_ready_utc')) >= _epoch(prepared['computation_completed_utc']), 'publication_precedes_computation')
    return {'raw': raw, 'feature': feature, 'receipt': receipt | {'receipt_sha256': digest}}


def read_capture(database, capture_id):
    """Return exact JSON-owned raw, feature and verified receipt; no freshness claim."""
    with _connect_read(database) as c:
        return _read(c, capture_id)


def publication_header(database, capture_id):
    """Bounded publication metadata for startup clock seeding/recovery routing.

    This verifies the receipt seal against capture-table identities and clocks,
    without opening any row, feature or envelope blob. It is NOT full payload
    integrity, feature authority, availability or freshness verification. A
    consumer must still use read_capture in its bounded execution environment.
    """
    _require(isinstance(capture_id, str) and re.fullmatch(r'[A-Za-z0-9_.-]{1,160}', capture_id), 'invalid_capture_id')
    fields = {'capture_id': 160, 'contract_id': 256, 'cohort_id': 256,
              'context_bucket': 64, 'raw_sha256': 64, 'feature_sha256': 64,
              'source_bindings_sha256': 64, 'computation_completed_utc': 64}
    # A damaged database cannot cause an unbounded metadata/receipt allocation.
    selected = [f"CASE WHEN typeof(c.{name})='text' AND length(c.{name}) BETWEEN 1 AND {maximum} THEN c.{name} END"
                for name, maximum in fields.items()]
    selected += ["r.capture_id IS NOT NULL", "CASE WHEN length(r.receipt_sha256)=64 THEN r.receipt_sha256 END",
                 "CASE WHEN length(r.receipt_json) BETWEEN 1 AND 16384 THEN r.receipt_json END"]
    with _connect_read(database) as c:
        row = c.execute('SELECT ' + ','.join(selected) + ' FROM captures c LEFT JOIN publication_receipts r USING(capture_id) WHERE c.capture_id=?', (capture_id,)).fetchone()
    _require(row is not None, 'capture_missing')
    record = dict(zip(fields, row[:len(fields)]))
    _require(all(isinstance(v, str) for v in record.values()), 'publication_header_metadata_bound')
    for name in ('raw_sha256', 'feature_sha256', 'source_bindings_sha256'):
        _require(re.fullmatch('[a-f0-9]{64}', record[name]) is not None, 'publication_header_invalid_hash')
    context, complete = _epoch(record['context_bucket']), _epoch(record['computation_completed_utc'])
    _require(context <= complete and _bucket(record['context_bucket']) == record['context_bucket'], 'publication_header_clock_order')
    present, digest, encoded = row[len(fields):]
    result = {'capture_id': capture_id, 'published': bool(present),
              'computation_completed_utc': record['computation_completed_utc'], 'observed_ready_utc': None}
    if not present:
        return result
    _require(isinstance(digest, str) and isinstance(encoded, bytes), 'publication_header_receipt_bound')
    _require(_hash(encoded) == digest, 'receipt_hash_mismatch')
    receipt = _decode(encoded)
    expected = {name: record[name] for name in fields if name != 'computation_completed_utc'}
    expected |= {'storage_version': VERSION, 'research_only': True, 'execution_eligible': False,
                 'can_place_orders': False, 'can_promote': False,
                 'availability_rule': 'post_FULL_commit_independent_exact_payload_readback'}
    _require(isinstance(receipt, dict) and all(receipt.get(k) == v and type(receipt.get(k)) is type(v) for k, v in expected.items()),
             'receipt_metadata_mismatch')
    ready = receipt.get('observed_ready_utc')
    _require(isinstance(ready, str) and len(ready) <= 64 and _epoch(ready) >= complete, 'publication_precedes_computation')
    return result | {'observed_ready_utc': ready}


def find_bucket(database, context_bucket, *, cohort_id):
    """Return an existing capture ID, including a crash-left unpublished payload."""
    if not Path(database).exists():
        return None
    with _connect_read(database, allow_empty=True) as c:
        if not _identity(c, allow_empty=True):
            return None
        row = c.execute('SELECT capture_id FROM captures WHERE cohort_id=? AND context_bucket=?', (cohort_id, _bucket(context_bucket))).fetchone()
        return row[0] if row else None


def find_bucket_status(database, context_bucket, *, cohort_id):
    """Distinguish a durable publication from a payload awaiting recovery."""
    if not Path(database).exists():
        return None
    with _connect_read(database, allow_empty=True) as c:
        if not _identity(c, allow_empty=True):
            return None
        row = c.execute('SELECT c.capture_id,r.capture_id FROM captures c LEFT JOIN publication_receipts r USING(capture_id) WHERE c.cohort_id=? AND c.context_bucket=?', (cohort_id, _bucket(context_bucket))).fetchone()
        return {'capture_id': row[0], 'published': row[1] is not None} if row else None


def recover_pending(database, capture_id, *, limits=Limits(), clock=utcnow):
    """Finish a crash-left payload with a newly observed publication clock."""
    with _connect_read(database) as c:
        retained = _read(c, capture_id, require_receipt=False)
    if retained['receipt'] is not None:
        return retained['receipt']
    return publish(database, retained['raw'], retained['feature'], capture_id, limits=limits, clock=clock)


def _check_capacity(c, database, limits, additional_stored_bytes=0):
    pages = c.execute('PRAGMA page_count').fetchone()[0]
    page_size = c.execute('PRAGMA page_size').fetchone()[0]
    logical = c.execute('SELECT COALESCE(SUM(logical_bytes),0) FROM blobs').fetchone()[0]
    _capacity(pages * page_size <= limits.max_database_bytes, 'database_page_limit')
    _capacity(logical <= limits.max_unique_logical_bytes, 'unique_logical_byte_limit')
    # Reserve enough for a worst-case rollback journal as well as new pages.
    free = shutil.disk_usage(Path(database).parent).free
    _capacity(free >= limits.min_free_disk_bytes + pages * page_size + additional_stored_bytes, 'minimum_free_disk_reserve')


def publish(database, raw, feature, capture_id, *, limits=Limits(), clock=utcnow):
    """Commit evidence, read it independently, then append a real ready receipt.

    A duplicate identical ID returns its original receipt without renewing it.
    A different ID for an existing cohort/bucket raises DuplicateBucketError.
    Payloads left by a crash before the ready receipt remain unreadable to public
    consumers; retrying the *same* exact ID/raw/feature safely completes them.
    """
    _limits(limits)
    # Readers use these fixed maxima, so configurable limits may only tighten.
    for name in ('max_capture_bytes', 'max_rows', 'max_row_bytes', 'max_feature_bytes', 'max_envelope_bytes'):
        _require(getattr(limits, name) <= getattr(Limits(), name), 'limit_exceeds_reader_bound:' + name)
    plan = _prepare(raw, feature, capture_id, limits)
    writer_started = _epoch(clock())
    _require(writer_started >= _epoch(plan['computation_completed_utc']), 'writer_clock_precedes_computation')
    database = Path(database)
    _require(not database.is_symlink(), 'database_symlink')
    database.parent.mkdir(parents=True, exist_ok=True)
    _capacity(shutil.disk_usage(database.parent).free >= limits.min_free_disk_bytes, 'minimum_free_disk_reserve')
    try:
        with closing(sqlite3.connect(database, timeout=10, isolation_level=None)) as c:
            exists = _identity(c, allow_empty=True)
            c.execute('PRAGMA foreign_keys=ON')
            if not exists:
                c.execute(f'PRAGMA page_size={PAGE_SIZE}')
            _require(c.execute('PRAGMA page_size').fetchone()[0] == PAGE_SIZE, 'unexpected_page_size')
            c.execute('PRAGMA journal_mode=DELETE')
            c.execute('PRAGMA synchronous=FULL')
            max_pages = limits.max_database_bytes // PAGE_SIZE
            _capacity(max_pages >= 1, 'database_page_limit')
            _capacity(c.execute('PRAGMA page_count').fetchone()[0] <= max_pages, 'database_page_limit')
            c.execute(f'PRAGMA max_page_count={max_pages}')
            c.execute('BEGIN IMMEDIATE')
            try:
                if not exists:
                    _initialize(c)
                prior = c.execute('SELECT raw_sha256,feature_sha256 FROM captures WHERE capture_id=?', (capture_id,)).fetchone()
                if prior is not None:
                    _require(prior == (plan['raw_sha'], plan['feature_sha']), 'capture_id_content_conflict')
                    previous = _read(c, capture_id, require_receipt=False)
                    c.commit()
                    if previous['receipt'] is not None:
                        return previous['receipt']
                else:
                    bucket = c.execute('SELECT capture_id FROM captures WHERE cohort_id=? AND context_bucket=?', (plan['cohort'], plan['bucket'])).fetchone()
                    if bucket:
                        raise DuplicateBucketError(bucket[0])
                    cohorts = c.execute('SELECT DISTINCT contract_id,cohort_id,source_bindings_sha256 FROM captures').fetchall()
                    _require(not cohorts or cohorts == [(plan['contract'], plan['cohort'], plan['source_sha'])], 'foreign_capture_cohort_or_sources')
                    additional = sum(len(compressed) for _, compressed in plan['blobs'].values()) + 65536
                    _check_capacity(c, database, limits, additional)
                    for digest, (encoded, compressed) in plan['blobs'].items():
                        existing = c.execute('SELECT logical_bytes,stored_sha256,compressed FROM blobs WHERE sha256=?', (digest,)).fetchone()
                        if existing is None:
                            c.execute('INSERT INTO blobs VALUES(?,?,?,?)', (digest, len(encoded), _hash(compressed), compressed))
                        else:
                            _require(canonical_bytes(_blob(c, digest, limits.max_capture_bytes)) == encoded, 'existing_content_address_collision')
                    c.execute('INSERT INTO captures VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                              (capture_id, plan['contract'], plan['cohort'], plan['bucket'], plan['raw_sha'], len(plan['raw_bytes']),
                               plan['envelope_sha'], plan['feature_sha'], len(plan['feature_bytes']), plan['source_sha'],
                               len(plan['refs']), plan['computation_completed_utc']))
                    c.execute('INSERT INTO row_refs VALUES(?,?)', (capture_id, plan['manifest_sha']))
                    _check_capacity(c, database, limits)
                    # Reserve pages for the following tiny immutable receipt.
                    _capacity((c.execute('PRAGMA page_count').fetchone()[0] + 4) * PAGE_SIZE <= limits.max_database_bytes, 'receipt_page_reserve')
                    c.commit()
            except BaseException:
                c.rollback()
                raise
            # This opens a distinct read-only connection after the FULL commit.
            with _connect_read(database) as verify:
                payload = _read(verify, capture_id, require_receipt=False)
                _require(canonical_bytes(payload['raw']) == plan['raw_bytes']
                         and canonical_bytes(payload['feature']) == plan['feature_bytes'], 'postcommit_exact_readback_mismatch')
            ready = clock()
            _require(_epoch(ready) >= writer_started, 'publication_clock_regressed')
            ready = ready.isoformat() if isinstance(ready, datetime) else ready
            receipt = {'capture_id': capture_id, 'storage_version': VERSION, 'contract_id': plan['contract'],
                       'cohort_id': plan['cohort'], 'context_bucket': plan['bucket'], 'observed_ready_utc': ready,
                       'raw_sha256': plan['raw_sha'], 'feature_sha256': plan['feature_sha'],
                       'source_bindings_sha256': plan['source_sha'],
                       'availability_rule': 'post_FULL_commit_independent_exact_payload_readback',
                       'research_only': True, 'execution_eligible': False, 'can_place_orders': False, 'can_promote': False}
            encoded = canonical_bytes(receipt)
            c.execute('BEGIN IMMEDIATE')
            try:
                c.execute('INSERT INTO publication_receipts VALUES(?,?,?)', (capture_id, _hash(encoded), encoded))
                _check_capacity(c, database, limits)
                c.commit()
            except BaseException:
                c.rollback()
                raise
        return read_capture(database, capture_id)['receipt']
    except sqlite3.OperationalError as exc:
        if 'full' in str(exc).lower():
            raise CapacityError('sqlite_database_or_disk_full') from exc
        raise


def stats(database):
    """Read-only accounting. Logical bytes count unique canonical blobs once."""
    path = Path(database)
    empty = {'storage_version': VERSION, 'exists': False, 'captures': 0, 'published_captures': 0,
                'unpublished_captures': 0, 'unique_blobs': 0, 'unique_logical_bytes': 0,
                'stored_blob_bytes': 0, 'row_references': 0, 'database_bytes': 0, 'page_bytes': 0}
    if not path.exists():
        return empty
    with _connect_read(path, allow_empty=True) as c:
        if not _identity(c, allow_empty=True):
            return empty | {'exists': True, 'database_bytes': path.stat().st_size}
        captures = c.execute('SELECT COUNT(*) FROM captures').fetchone()[0]
        published = c.execute('SELECT COUNT(*) FROM publication_receipts').fetchone()[0]
        blobs, logical, stored = c.execute('SELECT COUNT(*),COALESCE(SUM(logical_bytes),0),COALESCE(SUM(length(compressed)),0) FROM blobs').fetchone()
        return {'storage_version': VERSION, 'exists': True, 'captures': captures, 'published_captures': published,
                'unpublished_captures': captures - published, 'unique_blobs': blobs, 'unique_logical_bytes': logical,
                'stored_blob_bytes': stored, 'row_references': c.execute('SELECT COALESCE(SUM(row_count),0) FROM captures').fetchone()[0],
                'database_bytes': path.stat().st_size,
                'page_bytes': c.execute('PRAGMA page_count').fetchone()[0] * c.execute('PRAGMA page_size').fetchone()[0]}
