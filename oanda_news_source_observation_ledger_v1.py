"""Additive prospective article/source evidence; no fetches or implicit clocks."""
from __future__ import annotations
from contextlib import contextmanager
import datetime as dt
import hashlib
import json
import math
import sqlite3
from typing import Any, Mapping

CONTRACT = 'article_source_observation_version_ledger_v1_20260912'
SOURCE_FIELDS = ('source_id', 'source_kind', 'source_contract_id', 'source_cohort_id',
                 'source_config_sha256', 'source_lineage_version', 'source_contract_derived')
PROVENANCE_FIELDS = ('collector_contract_id', 'collector_cohort_id', 'observation_time_contract_id',
                     'observation_clock_trusted', 'observation_clock_source')
ENVELOPE_FIELDS = frozenset(('event_id', 'first_seen_utc', 'last_seen_utc', 'published_utc',
    'causal_known_utc', 'numeric_causal_known_utc', 'detail_available_utc',
    'detail_attachment_available_utc', 'detail_publisher_resolution_known_utc', 'publication_clock_known_utc',
    'duplicate_observation_count', 'availability_lag_minutes', 'event_lineage_id',
    'material_update_id', 'publication_hold_seconds', *PROVENANCE_FIELDS))
ACTIVE_FIELDS = ('source_observation_ledger_contract', 'source_observation_version_id',
    'source_observation_first_valid_id', 'source_version_first_known_utc',
    'source_version_available_utc', 'source_version_observed_utc', 'source_version_clock_provenance',
    'source_version_clock_status', 'source_lineage_origin_status', 'source_version_known_time_basis',
    'article_first_source_observation_id', 'article_first_source_identity')
TABLES = ('article_source_versions_v1', 'article_source_observations_v1',
          'article_source_origins_v1', 'article_source_projections_v1')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)


def digest(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def aware(value):
    try:
        parsed = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset() is None or not math.isfinite(parsed.timestamp()):
            return None
        return parsed.astimezone(dt.timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def raw_clock(value):
    return value.isoformat() if isinstance(value, dt.datetime) else value


@contextmanager
def ledger_transaction(connection):
    """A rollback boundary without committing an existing caller transaction."""
    connection.execute('SAVEPOINT news_source_ledger_batch_v1')
    try:
        yield
    except BaseException:
        connection.execute('ROLLBACK TO news_source_ledger_batch_v1')
        connection.execute('RELEASE news_source_ledger_batch_v1')
        raise
    else:
        connection.execute('RELEASE news_source_ledger_batch_v1')


def initialize(connection):
    statements = [
      '''CREATE TABLE IF NOT EXISTS article_source_versions_v1 (
        version_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, canonical_event_id TEXT NOT NULL,
        source_identity_json TEXT NOT NULL, source_identity_sha256 TEXT NOT NULL,
        content_json TEXT NOT NULL, content_sha256 TEXT NOT NULL)''',
      '''CREATE TABLE IF NOT EXISTS article_source_observations_v1 (
        observation_seq INTEGER PRIMARY KEY AUTOINCREMENT, observation_id TEXT NOT NULL UNIQUE,
        contract_id TEXT NOT NULL, canonical_event_id TEXT NOT NULL, incoming_event_id TEXT NOT NULL,
        version_id TEXT NOT NULL, envelope_json TEXT NOT NULL, incoming_payload_sha256 TEXT NOT NULL,
        incoming_payload_bytes INTEGER NOT NULL, supplied_now_json TEXT NOT NULL,
        source_observed_json TEXT NOT NULL, known_epoch REAL, known_utc TEXT,
        clock_status TEXT NOT NULL, clock_reasons_json TEXT NOT NULL)''',
      '''CREATE TABLE IF NOT EXISTS article_source_origins_v1 (
        canonical_event_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, origin_status TEXT NOT NULL,
        original_row_json TEXT NOT NULL, original_row_sha256 TEXT NOT NULL)''',
      '''CREATE TABLE IF NOT EXISTS article_source_projections_v1 (
        projection_id TEXT PRIMARY KEY, contract_id TEXT NOT NULL, canonical_event_id TEXT NOT NULL,
        version_id TEXT NOT NULL, first_valid_observation_id TEXT NOT NULL,
        operation TEXT NOT NULL, row_json TEXT NOT NULL, row_sha256 TEXT NOT NULL,
        payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL)''',
      "CREATE INDEX IF NOT EXISTS idx_source_version_url_v1 ON article_source_versions_v1(json_extract(source_identity_json,'$.source_id'),json_extract(content_json,'$.source_url'),canonical_event_id)",
      'CREATE INDEX IF NOT EXISTS idx_source_obs_version_seq_v1 ON article_source_observations_v1(version_id,observation_seq)',
      'CREATE INDEX IF NOT EXISTS idx_source_obs_event_v1 ON article_source_observations_v1(canonical_event_id,observation_seq)',
      'CREATE INDEX IF NOT EXISTS idx_source_projection_event_v1 ON article_source_projections_v1(canonical_event_id)',
    ]
    with ledger_transaction(connection):
        for statement in statements:
            connection.execute(statement)
        for table in TABLES:
            install_insert_conflict_guard(connection, table)
            for action in ('UPDATE', 'DELETE'):
                connection.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_{table}_{action.lower()}
                  BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'append_only_source_evidence'); END''')


def table_row(connection, event_id):
    cursor = connection.execute('SELECT * FROM articles WHERE event_id=?', (event_id,))
    row = cursor.fetchone()
    return dict(zip([column[0] for column in cursor.description], row)) if row is not None else None


def capture_origin(connection, row, status):
    if connection.execute('SELECT 1 FROM article_source_origins_v1 WHERE canonical_event_id=?', (row['event_id'],)).fetchone():
        return
    text = encoded(row)
    connection.execute('''INSERT OR IGNORE INTO article_source_origins_v1
      VALUES(?,?,?,?,?)''', (row['event_id'], CONTRACT, status, text, digest(text)))


def _clock_evidence(article, now, expected_provenance):
    reasons = []
    current = aware(now)
    observed = aware(article.get('first_seen_utc'))
    if current is None:
        reasons.append('supplied_now_missing_invalid_or_naive')
    if observed is None:
        reasons.append('source_observation_missing_invalid_or_naive')
    for name in ('first_seen_utc', 'last_seen_utc', 'detail_available_utc',
                 'detail_attachment_available_utc', 'detail_publisher_resolution_known_utc', 'publication_clock_known_utc'):
        if name == 'first_seen_utc' or article.get(name) not in (None, ''):
            value = aware(article.get(name))
            if value is None:
                reasons.append('invalid_or_naive_' + name)
            elif current is not None and value > current:
                reasons.append('future_observation_' + name)
    last_seen = aware(article.get('last_seen_utc'))
    if observed is not None and last_seen is not None and last_seen < observed:
        reasons.append('source_first_seen_after_last_seen')
    # Publication/eligibility can intentionally be in the future (embargo).
    # They are not proof of an observation now. Invalid/naive values are never
    # normalized into UTC; the original values stay in the observation ledger.
    for name in ('published_utc', 'causal_known_utc', 'numeric_causal_known_utc'):
        if name == 'published_utc' or article.get(name) not in (None, ''):
            if aware(article.get(name)) is None:
                reasons.append('invalid_or_naive_eligibility_' + name)
    if article.get('observation_clock_trusted') is not True:
        reasons.append('incoming_observation_not_attested')
    for name, expected in expected_provenance.items():
        if not isinstance(article.get(name), str) or article[name] != expected:
            reasons.append('incoming_provenance_mismatch_' + name)
    for name in ('source_id', 'source_kind', 'source_contract_id', 'source_cohort_id'):
        if not isinstance(article.get(name), str) or not article[name].strip():
            reasons.append('missing_source_identity_' + name)
    # Caller-supplied trusted observation time is explicit. No host utcnow or
    # fallback replaces an invalid/missing supplied/source clock.
    return ('valid_attested_observation' if not reasons else 'unproven_observation', reasons,
            current.timestamp() if not reasons else None, current.isoformat() if not reasons else None)


def record_observation(connection, article, canonical_event_id, now, *, expected_provenance):
    incoming = dict(article)
    full_text = encoded(incoming)
    envelope = {key: value for key, value in incoming.items() if key in ENVELOPE_FIELDS}
    content = {key: value for key, value in incoming.items() if key not in ENVELOPE_FIELDS}
    content_text = encoded(content)
    source_identity = {key: incoming.get(key) for key in SOURCE_FIELDS}
    identity_text = encoded(source_identity)
    content_sha = digest(content_text)
    version_id = digest(encoded([CONTRACT, canonical_event_id, digest(identity_text), content_sha]))
    now_text = encoded(raw_clock(now))
    observation_id = digest(encoded([CONTRACT, canonical_event_id, digest(full_text), now_text]))
    status, reasons, known_epoch, known_utc = _clock_evidence(incoming, now, expected_provenance)
    existing = table_row(connection, canonical_event_id)
    if existing is not None:
        capture_origin(connection, existing, 'legacy_origin_query_history_unresolved')
    connection.execute('INSERT OR IGNORE INTO article_source_versions_v1 VALUES(?,?,?,?,?,?,?)',
        (version_id, CONTRACT, canonical_event_id, identity_text, digest(identity_text), content_text, content_sha))
    connection.execute('''INSERT OR IGNORE INTO article_source_observations_v1
      (observation_id,contract_id,canonical_event_id,incoming_event_id,version_id,envelope_json,
       incoming_payload_sha256,incoming_payload_bytes,supplied_now_json,source_observed_json,
       known_epoch,known_utc,clock_status,clock_reasons_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (observation_id, CONTRACT, canonical_event_id, str(incoming['event_id']), version_id, encoded(envelope),
         digest(full_text), len(full_text.encode('utf-8')), now_text, encoded(incoming.get('first_seen_utc')),
         known_epoch, known_utc, status, encoded(reasons)))
    first = connection.execute('''SELECT observation_id,known_epoch,known_utc,envelope_json
      FROM article_source_observations_v1 WHERE version_id=? AND clock_status='valid_attested_observation'
      ORDER BY observation_seq LIMIT 1''', (version_id,)).fetchone()
    canonical_first = connection.execute('''SELECT o.observation_id,o.envelope_json,v.source_identity_json
      FROM article_source_observations_v1 o JOIN article_source_versions_v1 v ON v.version_id=o.version_id
      WHERE o.canonical_event_id=? AND o.clock_status='valid_attested_observation'
      ORDER BY o.observation_seq LIMIT 1''', (canonical_event_id,)).fetchone()
    origin = connection.execute('SELECT origin_status FROM article_source_origins_v1 WHERE canonical_event_id=?', (canonical_event_id,)).fetchone()
    return {'observation_id': observation_id, 'version_id': version_id, 'status': status, 'reasons': reasons,
      'eligible': status == 'valid_attested_observation' and first is not None,
      'first_valid_id': first[0] if first else None, 'first_known_epoch': first[1] if first else None,
      'first_known_utc': first[2] if first else None,
      'first_observed_utc': aware(json.loads(first[3]).get('first_seen_utc')).isoformat() if first else None,
      'first_provenance': {key: json.loads(first[3]).get(key) for key in PROVENANCE_FIELDS} if first else None,
      'source_identity': source_identity, 'origin_status': origin[0] if origin else 'new_lineage_cohort_origin',
      'canonical_first_id': canonical_first[0] if canonical_first else None,
      'canonical_first_envelope': json.loads(canonical_first[1]) if canonical_first else None,
      'canonical_first_source_identity': json.loads(canonical_first[2]) if canonical_first else None}


def reconstruct_observation(connection, observation_id):
    row = connection.execute('''SELECT v.content_json,v.content_sha256,o.envelope_json,
      o.incoming_payload_sha256,o.incoming_payload_bytes FROM article_source_observations_v1 o
      JOIN article_source_versions_v1 v ON v.version_id=o.version_id WHERE o.observation_id=?''', (observation_id,)).fetchone()
    if row is None:
        raise ValueError('source_observation_missing')
    if digest(row[0]) != row[1]:
        raise ValueError('source_content_hash_mismatch')
    content, envelope = json.loads(row[0]), json.loads(row[2])
    if set(content) & set(envelope):
        raise ValueError('source_envelope_content_overlap')
    text = encoded({**content, **envelope})
    if digest(text) != row[3] or len(text.encode('utf-8')) != row[4]:
        raise ValueError('source_observation_reconstruction_mismatch')
    return text


def select_observation(observation, previous, incoming):
    if not observation['eligible']:
        return False
    if not previous:
        return True
    if bool(previous.get('detail_enriched')) and not bool(incoming.get('detail_enriched')):
        return False
    if previous.get('source_observation_ledger_contract') != CONTRACT:
        return True
    previous_id = previous.get('source_observation_version_id')
    if previous_id == observation['version_id']:
        return False
    previous_observed = aware(previous.get('source_version_observed_utc'))
    incoming_observed = aware(observation.get('first_observed_utc'))
    if previous_observed is None or incoming_observed is None:
        raise ValueError('active_source_version_missing_observed_boundary')
    return (incoming_observed.timestamp(), observation['version_id']) > (previous_observed.timestamp(), str(previous_id))


def bind_active_version(payload, observation, previous=None):
    result = dict(payload)
    if not observation['eligible']:
        raise ValueError('unproven_source_observation_cannot_be_active')
    for key in ('source_id', 'source_kind', 'source_contract_id', 'source_cohort_id'):
        if result.get(key) != observation['source_identity'][key]:
            raise ValueError('active_payload_source_identity_mismatch:' + key)
    if not previous:
        first_envelope = observation['canonical_first_envelope']
        result['first_seen_utc'] = first_envelope['first_seen_utc']
        result['published_utc'] = first_envelope['published_utc']
        for key in PROVENANCE_FIELDS:
            if key in first_envelope:
                result[key] = first_envelope[key]
    boundary = aware(observation['first_known_utc'])
    prior = source_version_floor(previous or {})
    if prior is not None:
        boundary = max(boundary, prior)
    result.update(article_first_source_observation_id=observation['canonical_first_id'],
        article_first_source_identity=observation['canonical_first_source_identity'],
        source_observation_ledger_contract=CONTRACT,
        source_observation_version_id=observation['version_id'],
        source_observation_first_valid_id=observation['first_valid_id'],
        source_version_first_known_utc=observation['first_known_utc'],
        source_version_available_utc=boundary.isoformat(),
        source_version_observed_utc=observation['first_observed_utc'],
        source_version_clock_provenance=observation['first_provenance'],
        source_version_clock_status='valid_attested_observation',
        source_lineage_origin_status=observation['origin_status'],
        source_version_known_time_basis='first_valid_appended_supplied_collector_observation_boundary;not_postcommit_readback_proof')
    causal = aware(result.get('causal_known_utc'))
    result['causal_known_utc'] = max(boundary, causal).isoformat() if causal else boundary.isoformat()
    numeric = aware(result.get('numeric_causal_known_utc'))
    if numeric is not None:
        result['numeric_causal_known_utc'] = max(boundary, numeric).isoformat()
    return result


def source_version_floor(article):
    marker = article.get('source_observation_ledger_contract')
    if marker is None:
        return None
    if marker != CONTRACT or article.get('source_version_clock_status') != 'valid_attested_observation':
        raise ValueError('unsupported_or_unproven_source_version')
    floor = aware(article.get('source_version_available_utc'))
    first = aware(article.get('source_version_first_known_utc'))
    observed = aware(article.get('source_version_observed_utc'))
    if floor is None or first is None or observed is None or floor < first or first < observed:
        raise ValueError('source_version_known_boundary_invalid')
    return floor


def source_version_provenance_valid(article, expected_provenance):
    if article.get('source_observation_ledger_contract') is None:
        return True
    try:
        source_version_floor(article)
    except ValueError:
        return False
    provenance = article.get('source_version_clock_provenance')
    return bool(isinstance(provenance, Mapping) and provenance.get('observation_clock_trusted') is True
                and all(provenance.get(key) == value for key, value in expected_provenance.items()))


def preserve_active_version(updated, previous):
    """Reclassification is not a new source observation or a license to lose its floor."""
    result = dict(updated)
    if previous.get('source_observation_ledger_contract') is None:
        return result
    source_version_floor(previous)
    for key in ACTIVE_FIELDS:
        result[key] = previous[key]
    for key in PROVENANCE_FIELDS:
        if key in previous:
            result[key] = previous[key]
        else:
            result.pop(key, None)
    for key in ('source_id', 'source_kind', 'source_contract_id', 'source_cohort_id'):
        if result.get(key) != previous.get(key):
            raise ValueError('reclassification_cannot_reassign_source_version_identity')
    for key in SOURCE_FIELDS:
        if key in previous:
            result[key] = previous[key]
        else:
            result.pop(key, None)
    prior = source_version_floor(previous)
    current = aware(result.get('causal_known_utc'))
    result['causal_known_utc'] = max(prior, current).isoformat() if current else prior.isoformat()
    numeric = aware(result.get('numeric_causal_known_utc'))
    if numeric is not None:
        result['numeric_causal_known_utc'] = max(prior, numeric).isoformat()
    return result


def record_projection(connection, event_id, operation):
    row = table_row(connection, event_id)
    if row is None:
        raise ValueError('canonical_projection_missing')
    payload = json.loads(row['payload_json'])
    if payload.get('source_observation_ledger_contract') is None:
        if payload.get('classification_observation_contract') != 'article_classification_observation_v1_20260912':
            return  # An unversioned old projection is not prospective evidence.
        if payload.get('classification_clock_status') != 'valid_attested_classification' or aware(payload.get('classification_available_utc')) is None:
            raise ValueError('unproven_legacy_derived_projection')
    else:
        source_version_floor(payload)
    for field in ('source_id', 'source_kind', 'source_name', 'source_url', 'first_seen_utc', 'published_utc'):
        if row[field] != payload[field]:
            raise ValueError('canonical_table_payload_mismatch:' + field)
    if not connection.execute('SELECT 1 FROM article_source_origins_v1 WHERE canonical_event_id=?', (event_id,)).fetchone():
        capture_origin(connection, row, 'new_lineage_cohort_origin')
    text = encoded(row)
    projection_id = digest(encoded([CONTRACT, event_id, text]))
    if connection.execute('SELECT 1 FROM article_source_projections_v1 WHERE projection_id=?', (projection_id,)).fetchone():
        return
    connection.execute('INSERT OR IGNORE INTO article_source_projections_v1 VALUES(?,?,?,?,?,?,?,?,?,?)',
      (projection_id, CONTRACT, event_id, str(payload.get('source_observation_version_id') or ''),
       str(payload.get('source_observation_first_valid_id') or ''), operation, text, digest(text),
       row['payload_json'], digest(row['payload_json'])))


def install_insert_conflict_guard(connection, table):
    """Block REPLACE bypasses while preserving identical INSERT OR IGNORE retries."""
    if table not in (*TABLES, 'article_classification_observations_v1'):
        raise ValueError('unexpected_immutable_table')
    info = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    columns = [row[1] for row in info]
    primary = [row[1] for row in sorted(info, key=lambda row:row[5]) if row[5]]
    unique = [primary]
    for index in connection.execute(f'PRAGMA index_list("{table}")').fetchall():
        if index[2]:
            keys = [row[2] for row in connection.execute('PRAGMA index_info("'+index[1].replace('"','""')+'")')]
            if all(isinstance(key, str) for key in keys) and keys not in unique:
                unique.append(keys)
    def quote(name):
        return '"'+name.replace('"','""')+'"'
    collisions = ' OR '.join('('+' AND '.join(f'old.{quote(key)} IS NEW.{quote(key)}' for key in keys)+')' for keys in unique)
    matches = []
    for column in columns:
        expression = f'old.{quote(column)} IS NEW.{quote(column)}'
        if column == 'observation_seq':
            expression = '(NEW.observation_seq = -1 OR '+expression+')'
        matches.append(expression)
    all_equal = ' AND '.join(matches)
    connection.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_{table}_insert_conflict
        BEFORE INSERT ON "{table}"
        WHEN EXISTS(SELECT 1 FROM "{table}" AS old WHERE {collisions})
        BEGIN
          SELECT CASE WHEN EXISTS(SELECT 1 FROM "{table}" AS old
               WHERE ({collisions}) AND NOT ({all_equal}))
            THEN RAISE(ABORT,'conflicting_immutable_evidence_identity')
            ELSE RAISE(IGNORE) END;
        END''')
