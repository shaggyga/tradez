"""Bounded read-only collector projection evidence and pure supplied-admission selection.

No writer, cursor persistence, admission publisher, history adapter or CLI exists.
An admission envelope is an explicit external trust boundary: this pure component
checks its exact binding and clocks, but cannot prove the supplier persisted it.
"""
from __future__ import annotations
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from contextlib import closing

import oanda_news_source_observation_ledger_v1 as source
import oanda_news_classification_observation_v1 as classification
import oanda_local_news_sentiment_repair_v2 as repair
from oanda_news_classification_contract import NEWS_CLASSIFICATION_VERSION

SCHEMA = 'collector_projection_revision_capture_v1_20260913'
ADMISSION = 'supplied_projection_revision_admission_v1_20260913'
MAX_ROWS = 256
MAX_BYTES = 16 * 1024 * 1024
MAX_SECONDS = 30
MAX_BATCHES = 4096
PROJECTION_COLUMNS = ('projection_id', 'contract_id', 'canonical_event_id', 'version_id',
    'first_valid_observation_id', 'operation', 'row_json', 'row_sha256', 'payload_json', 'payload_sha256')
INERT = {'research_only': True, 'can_place_orders': False, 'can_authorize': False,
         'can_promote': False, 'proof_eligible': False}


def encoded(value): return source.encoded(value).encode('utf-8')
def sha(value): return hashlib.sha256(encoded(value)).hexdigest()
def require(condition, reason):
    if not condition: raise ValueError(reason)


def plain_c(path):
    path = Path(path)
    require(path.is_absolute() and path.drive.upper() == 'C:' and '..' not in path.parts and not any(':' in part for part in path.parts[1:]),
            'plain_absolute_c_path_required')
    for part in (*reversed(path.parents), path):
        try: info = part.lstat()
        except FileNotFoundError: raise ValueError('required_path_missing')
        require(not stat.S_ISLNK(info.st_mode) and not getattr(info, 'st_file_attributes', 0) & 1024,
                'reparse_path_refused')
    return path


def _identity(path):
    path = plain_c(path); info = path.lstat()
    require(stat.S_ISREG(info.st_mode), 'regular_database_required')
    return {'path': str(path), 'device': info.st_dev, 'inode': info.st_ino}


def _number(value):
    require(type(value) in (int, float) and math.isfinite(value), 'finite_clock_required')
    return float(value)


def _integer(value, reason, minimum=0, maximum=(1 << 63)-1):
    require(type(value) is int and minimum <= value <= maximum, reason)
    return value


def _time(value):
    parsed = source.aware(value)
    require(parsed is not None, 'aware_clock_required')
    return parsed.timestamp()


def source_identities(configured_sources):
    """Use the collector's actual explicit/derived source contract semantics."""
    result = {}
    for configured in configured_sources:
        value = repair.collector.source_config_lineage(configured)
        ident = {key: value.get('kind') if key == 'source_kind' else value.get(key)
                 for key in source.SOURCE_FIELDS}
        name = ident['source_id']
        require(isinstance(name, str) and name and name not in result, 'unique_source_identity_required')
        result[name] = ident
    return result


def policy_for(expected_sources, expected_provenance):
    require(isinstance(expected_sources, dict) and 1 <= len(expected_sources) <= 512, 'bounded_sources_required')
    for name, ident in expected_sources.items():
        require(set(ident) == set(source.SOURCE_FIELDS) and ident['source_id'] == name, 'exact_source_identity_fields_required')
        require(all(isinstance(ident.get(k), str) and ident[k] for k in
                    ('source_id', 'source_kind', 'source_contract_id', 'source_cohort_id')), 'source_identity_required')
    expected = {'collector_contract_id': repair.collector.COLLECTOR_CONTRACT_ID,
        'collector_cohort_id': repair.collector.COLLECTOR_COHORT_ID,
        'observation_time_contract_id': repair.collector.OBSERVATION_TIME_CONTRACT_ID}
    require(expected_provenance == expected, 'current_collector_provenance_required')
    source_root = plain_c(repair.__file__).parent
    bindings = {name: hashlib.sha256(plain_c(source_root / name).read_bytes()).hexdigest() for name in sorted(repair.SOURCE_FILES)}
    bindings[Path(__file__).name] = hashlib.sha256(plain_c(__file__).read_bytes()).hexdigest()
    return {'schema_version': SCHEMA, 'sources': copy.deepcopy(expected_sources),
            'provenance': copy.deepcopy(expected), 'classification_version': NEWS_CLASSIFICATION_VERSION,
            'source_bindings': bindings}


def _json(text):
    require(isinstance(text, str), 'retained_json_text_required')
    value = json.loads(text)
    require(isinstance(value, dict), 'retained_json_object_required')
    encoded(value)  # Refuse NaN/Infinity retained by permissive JSON decoders.
    return value


def _observation(version, observation, expected):
    _integer(observation['observation_seq'], 'source_sequence_integer_required', 1)
    _integer(observation['incoming_payload_bytes'], 'source_payload_byte_count_integer_required')
    require(version['contract_id'] == observation['contract_id'] == source.CONTRACT, 'source_contract_mismatch')
    identity = _json(version['source_identity_json']); content = _json(version['content_json'])
    require(source.digest(version['source_identity_json']) == version['source_identity_sha256']
        and source.digest(version['content_json']) == version['content_sha256'], 'source_evidence_hash_mismatch')
    require(version['version_id'] == source.digest(source.encoded([source.CONTRACT,
        version['canonical_event_id'], version['source_identity_sha256'], version['content_sha256']])), 'source_version_id_mismatch')
    require({k: content.get(k) for k in source.SOURCE_FIELDS} == identity == expected, 'current_source_identity_mismatch')
    envelope = _json(observation['envelope_json'])
    require(not set(content) & set(envelope), 'source_envelope_overlap')
    incoming = {**content, **envelope}; raw = source.encoded(incoming)
    require(source.digest(raw) == observation['incoming_payload_sha256'] and
        len(raw.encode()) == observation['incoming_payload_bytes'], 'source_reconstruction_mismatch')
    require(observation['canonical_event_id'] == version['canonical_event_id'] and
        observation['version_id'] == version['version_id'] and
        observation['incoming_event_id'] == incoming['event_id'], 'source_reference_mismatch')
    require(observation['observation_id'] == source.digest(source.encoded([source.CONTRACT,
        observation['canonical_event_id'], source.digest(raw), observation['supplied_now_json']])), 'source_observation_id_mismatch')
    return incoming


def _derived(row, event, version, payload, policy):
    _integer(row['observation_seq'], 'classification_sequence_integer_required', 1)
    require(row['contract_id'] == classification.CONTRACT and row['canonical_event_id'] == event and
        row['source_version_id'] == version, 'classification_reference_mismatch')
    content = _json(row['content_json']); derived = _json(row['derived_payload_json'])
    require(source.digest(row['content_json']) == row['content_sha256'] and
        source.digest(row['derived_payload_json']) == row['derived_payload_sha256'], 'classification_hash_mismatch')
    require(classification.content(derived) == content == classification.content(payload), 'classification_content_mismatch')
    ident = source.digest(source.encoded([classification.CONTRACT, event, version, row['content_sha256']]))
    require(ident == row['classification_version_id'] == payload['classification_observation_version_id'], 'classification_version_id_mismatch')
    clock = json.loads(row['supplied_clock_json']); provenance = json.loads(row['supplied_provenance_json'])
    require(row['observation_id'] == source.digest(source.encoded([classification.CONTRACT, ident,
        row['derived_payload_sha256'], clock, provenance, row['operation']])), 'classification_observation_id_mismatch')
    require(row['clock_status'] == 'valid_attested_classification' and json.loads(row['clock_reasons_json']) == []
        and isinstance(provenance, dict) and provenance.get('observation_clock_trusted') is True and
        all(provenance.get(k) == v for k, v in policy['provenance'].items()), 'classification_clock_unproven')
    require(_time(clock) == _time(row['known_utc']) == _number(row['known_epoch']), 'classification_clock_binding_mismatch')
    require(derived['event_id'] == event and derived.get('source_observation_version_id') == version,
            'classification_derived_reference_mismatch')
    return provenance


def validate_projection(evidence, policy, observed_epoch):
    """Pure verification of the exact retained projection plus referenced ledger rows."""
    p = evidence['projection']; row = _json(p['row_json']); payload = _json(p['payload_json'])
    require(set(p) == {'projection_seq', *PROJECTION_COLUMNS} and type(p['projection_seq']) is int and p['projection_seq'] > 0,
            'projection_schema_mismatch')
    event = p['canonical_event_id']
    require(p['contract_id'] == source.CONTRACT and row['event_id'] == payload['event_id'] == event,
            'projection_identity_mismatch')
    require(source.digest(p['row_json']) == p['row_sha256'] and source.digest(p['payload_json']) == p['payload_sha256']
        and row['payload_json'] == p['payload_json'], 'projection_hash_mismatch')
    require(p['projection_id'] == source.digest(source.encoded([source.CONTRACT, event, p['row_json']])), 'projection_id_mismatch')
    for field in ('source_id', 'source_kind', 'source_name', 'source_url', 'first_seen_utc', 'published_utc', 'headline', 'summary'):
        require(row[field] == payload[field], 'table_payload_mismatch:' + field)
    _integer(row['duplicate_count'], 'article_duplicate_count_integer_required')
    require(type(payload.get('relevant')) is bool and type(row['relevant']) is int and row['relevant'] == int(payload['relevant']), 'table_relevance_mismatch')
    for col, field in (('currencies_json', 'currencies'), ('currency_scores_json', 'currency_scores'), ('directional_bias_json', 'directional_bias')):
        require(json.loads(row[col]) == payload[field], 'table_derived_mismatch:' + field)
    require(payload.get('classification_version') == policy['classification_version'], 'current_classification_required')
    require(payload.get('research_only') is True and payload.get('execution_eligible') is False and
        payload.get('can_place_orders') is False, 'inert_payload_required')
    expected = policy['sources'].get(payload.get('source_id'))
    require(expected is not None and {k: payload.get(k) for k in source.SOURCE_FIELDS} == expected, 'current_source_identity_mismatch')
    require(p['version_id'] and p['version_id'] == payload.get('source_observation_version_id') == evidence['source_version']['version_id']
        and p['first_valid_observation_id'] == payload.get('source_observation_first_valid_id') == evidence['source_observation']['observation_id'],
        'projection_source_reference_missing')
    incoming = _observation(evidence['source_version'], evidence['source_observation'], expected)
    obs = evidence['source_observation']
    supplied = json.loads(obs['supplied_now_json'])
    status, reasons, known, known_utc = source._clock_evidence(incoming, supplied, policy['provenance'])
    require((status, reasons, known, known_utc) == (obs['clock_status'], json.loads(obs['clock_reasons_json']), obs['known_epoch'], obs['known_utc'])
        and status == 'valid_attested_observation', 'source_clock_unproven')
    require(json.loads(obs['source_observed_json']) == incoming['first_seen_utc'], 'source_observed_clock_mismatch')
    require(payload['source_version_first_known_utc'] == known_utc and
        _time(payload['source_version_observed_utc']) == _time(incoming['first_seen_utc']), 'source_floor_binding_mismatch')
    original_version = evidence['canonical_first_version']
    original_observation = evidence['canonical_first_observation']
    original_identity = _json(original_version['source_identity_json'])
    original = _observation(original_version, original_observation, original_identity)
    original_status = source._clock_evidence(original, json.loads(original_observation['supplied_now_json']), policy['provenance'])
    require(original_status == ('valid_attested_observation', [], original_observation['known_epoch'], original_observation['known_utc']), 'canonical_first_clock_unproven')
    require(payload.get('source_lineage_origin_status') == 'new_lineage_cohort_origin', 'legacy_origin_not_reconstructable')
    require(original_version['canonical_event_id'] == event and
        payload.get('article_first_source_observation_id') == original_observation['observation_id'] and
        payload.get('article_first_source_identity') == original_identity and
        payload['first_seen_utc'] == original['first_seen_utc'], 'canonical_first_source_binding_mismatch')
    source_floor = source.source_version_floor(payload)
    require(source.source_version_provenance_valid(payload, policy['provenance']) and
        payload['source_version_clock_provenance'] == {k: incoming.get(k) for k in source.PROVENANCE_FIELDS}, 'source_provenance_mismatch')
    first = evidence['classification_first']; available = evidence['classification_available']
    _derived(first, event, p['version_id'], payload, policy)
    provenance = _derived(available, event, p['version_id'], payload, policy)
    require(payload['classification_first_known_utc'] == first['known_utc'] and
        _time(payload['classification_available_utc']) == _time(available['known_utc']) and
        payload['classification_clock_provenance'] == provenance and
        payload['classification_content_sha256'] == available['content_sha256'], 'classification_availability_binding_mismatch')
    derived_floor = classification.classification_floor(payload)
    require(classification.classification_provenance_valid(payload, policy['provenance']), 'classification_provenance_mismatch')
    require(source_floor.timestamp() <= derived_floor.timestamp() <= observed_epoch and
        _time(row['first_seen_utc']) <= _time(row['last_seen_utc']) <= observed_epoch, 'future_or_regressed_projection_clock')
    # Publication/causal eligibility may be future embargo clocks. Retain them;
    # no eligibility or score filtering precedes canonical revision selection.
    require(_time(payload['causal_known_utc']) >= derived_floor.timestamp(), 'causal_floor_before_classification')
    return payload


def _row(connection, sql, args=(), text_fields=()):
    if text_fields:
        expression = '+'.join('COALESCE(length(CAST(' + field + ' AS BLOB)),0)' for field in text_fields)
        size = connection.execute('SELECT ' + expression + ' FROM (' + sql + ')', args).fetchone()
        require(size is not None, 'referenced_immutable_evidence_missing')
        require(type(size[0]) is int and size[0] <= MAX_BYTES, 'oversized_referenced_evidence_refused')
    row = connection.execute(sql, args).fetchone()
    require(row is not None, 'referenced_immutable_evidence_missing')
    return dict(row)


def _capture(connection, projection):
    payload = _json(projection['payload_json']); version = projection['version_id']
    require(version, 'legacy_unversioned_projection_not_reconstructable')
    source_version = _row(connection, 'SELECT * FROM article_source_versions_v1 WHERE version_id=?', (version,), ('source_identity_json', 'content_json'))
    observation = _row(connection, "SELECT * FROM article_source_observations_v1 WHERE version_id=? AND clock_status='valid_attested_observation' ORDER BY observation_seq LIMIT 1", (version,), ('envelope_json', 'supplied_now_json', 'source_observed_json', 'clock_reasons_json'))
    require(observation['observation_id'] == projection['first_valid_observation_id'], 'not_first_valid_source_reference')
    source.reconstruct_observation(connection, observation['observation_id'])
    original = _row(connection, "SELECT * FROM article_source_observations_v1 WHERE canonical_event_id=? AND clock_status='valid_attested_observation' ORDER BY observation_seq LIMIT 1", (projection['canonical_event_id'],), ('envelope_json', 'supplied_now_json', 'source_observed_json', 'clock_reasons_json'))
    original_version = _row(connection, 'SELECT * FROM article_source_versions_v1 WHERE version_id=?', (original['version_id'],), ('source_identity_json', 'content_json'))
    source.reconstruct_observation(connection, original['observation_id'])
    class_version = payload.get('classification_observation_version_id')
    first = _row(connection, "SELECT * FROM article_classification_observations_v1 WHERE classification_version_id=? AND clock_status='valid_attested_classification' ORDER BY observation_seq LIMIT 1", (class_version,), ('content_json', 'derived_payload_json', 'supplied_clock_json', 'supplied_provenance_json', 'clock_reasons_json'))
    available = _row(connection, "SELECT * FROM article_classification_observations_v1 WHERE classification_version_id=? AND clock_status='valid_attested_classification' AND known_utc=? ORDER BY observation_seq LIMIT 1", (class_version, payload.get('classification_available_utc')), ('content_json', 'derived_payload_json', 'supplied_clock_json', 'supplied_provenance_json', 'clock_reasons_json'))
    return {'projection': projection, 'source_version': source_version, 'source_observation': observation,
            'canonical_first_version': original_version, 'canonical_first_observation': original,
            'classification_first': first, 'classification_available': available}


def _verify_immutable_schema(connection):
    # Build only an ephemeral in-memory expected schema using the producer's
    # actual initializers. No schema call ever targets the read-only input.
    with closing(sqlite3.connect(':memory:')) as expected:
        source.initialize(expected); classification.initialize(expected)
        expected_rows = expected.execute("SELECT type,name,sql FROM sqlite_master WHERE type IN ('table','trigger') AND name NOT LIKE 'sqlite_%'").fetchall()
    def normal(sql): return re.sub(r'\s+', ' ', str(sql)).strip().casefold()
    for kind, name, sql in expected_rows:
        actual = connection.execute('SELECT type,sql FROM sqlite_master WHERE name=?', (name,)).fetchone()
        require(actual is not None and actual[0] == kind and normal(actual[1]) == normal(sql),
                'immutable_ledger_schema_or_guard_mismatch:' + name)


def read_projection_snapshot(database_path, *, checkpoint=None, expected_sources, expected_provenance,
                             clock_provider, max_rows=MAX_ROWS, max_bytes=MAX_BYTES):
    """Complete declared contiguous prefix; no cursor advances or output writes.

    Only a future durable admission publisher may persist next_checkpoint.
    The rowid is paired with exact projection evidence and file identity, never
    treated as a portable sequence across rebuilt/replaced input databases.
    """
    require(type(max_rows) is int and 1 <= max_rows <= MAX_ROWS and type(max_bytes) is int and 1 <= max_bytes <= MAX_BYTES,
            'capture_bounds_invalid')
    checkpoint = copy.deepcopy(checkpoint)
    policy = policy_for(expected_sources, expected_provenance)
    started, initial_clock = clock_provider(); started = _number(started); initial_clock = copy.deepcopy(initial_clock)
    repair.validate_clock_state(initial_clock, started)
    path = plain_c(database_path)
    # SQLite may discover sidecars implicitly; refuse reparse sidecars before open.
    for suffix in ('-wal', '-shm', '-journal'):
        sibling = path.with_name(path.name + suffix)
        try: sibling.lstat()
        except FileNotFoundError: continue
        plain_c(sibling)
    identity = _identity(path); identity_sha = sha(identity)
    cursor = 0
    if checkpoint is not None:
        require(set(checkpoint) == {'input_identity_sha256', 'policy_sha256', 'through_seq', 'anchor_sha256'}, 'checkpoint_schema_mismatch')
        cursor = checkpoint['through_seq']
        require(type(cursor) is int and cursor >= 0 and checkpoint['input_identity_sha256'] == identity_sha and
            checkpoint['policy_sha256'] == sha(policy), 'checkpoint_identity_or_policy_changed')
    deadline = time.monotonic() + MAX_SECONDS
    entries = []; total = 0; anchor_sha = checkpoint['anchor_sha256'] if checkpoint else None
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as connection:
        connection.row_factory = sqlite3.Row
        connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, MAX_BYTES)
        connection.execute('PRAGMA query_only=ON')
        connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        connection.execute('BEGIN')
        _verify_immutable_schema(connection)
        columns = tuple(r[1] for r in connection.execute('PRAGMA table_info(article_source_projections_v1)'))
        require(columns == PROJECTION_COLUMNS, 'projection_table_schema_mismatch')
        if cursor:
            anchor = _row(connection, 'SELECT rowid AS projection_seq,* FROM article_source_projections_v1 WHERE rowid=?', (cursor,))
            require(sha(anchor) == anchor_sha, 'checkpoint_anchor_changed_or_lost')
        else: require(anchor_sha is None, 'zero_checkpoint_anchor_invalid')
        high = int(connection.execute('SELECT COALESCE(MAX(rowid),0) FROM article_source_projections_v1').fetchone()[0])
        require(high >= cursor, 'checkpoint_ahead_of_input')
        pending = connection.execute('SELECT rowid,length(CAST(row_json AS BLOB))+length(CAST(payload_json AS BLOB)) AS bytes FROM article_source_projections_v1 WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?', (cursor, high, max_rows + 1)).fetchall()
        for item in pending[:max_rows]:
            require(type(item[1]) is int, 'projection_payload_missing')
            if item[1] > max_bytes:
                if entries: break
                raise ValueError('oversized_head_projection_refused')
            p = _row(connection, 'SELECT rowid AS projection_seq,* FROM article_source_projections_v1 WHERE rowid=?', (item[0],))
            evidence = _capture(connection, p)
            size = len(encoded(evidence))
            if total + size > max_bytes:
                if entries: break
                raise ValueError('oversized_head_evidence_refused')
            validate_projection(evidence, policy, started)
            entries.append(evidence); total += size
            require(time.monotonic() <= deadline, 'capture_time_bound')
        through = entries[-1]['projection']['projection_seq'] if entries else cursor
        if entries: anchor_sha = sha(entries[-1]['projection'])
        # An invalid/oversized head is never skipped; no result is produced on refusal.
        require(not pending or entries, 'pending_head_not_consumed')
    completed, final_clock = clock_provider(); completed = _number(completed); final_clock = copy.deepcopy(final_clock)
    require(started <= completed <= started + MAX_SECONDS and time.monotonic() <= deadline, 'capture_clock_or_duration_bound')
    repair.validate_clock_state(initial_clock, completed); repair.validate_clock_state(final_clock, completed)
    require(_identity(path) == identity, 'database_changed_during_capture')
    require(policy_for(expected_sources, expected_provenance) == policy, 'source_or_policy_changed_during_capture')
    core = {'schema_version': SCHEMA, 'policy': policy, 'input_identity': identity,
        'previous_checkpoint': checkpoint, 'after_seq': cursor, 'through_seq': through,
        'committed_high_watermark': high, 'more_pending': through < high, 'complete_declared_prefix': True,
        'complete_historical_window': False, 'row_count': len(entries), 'evidence_bytes': total,
        'entries': entries, 'read_started_epoch': started, 'read_completed_epoch': completed,
        'initial_clock_state': initial_clock, 'completion_clock_state': final_clock,
        'next_checkpoint': {'input_identity_sha256': identity_sha, 'policy_sha256': sha(policy),
            'through_seq': through, 'anchor_sha256': anchor_sha}, 'admitted': False, **INERT}
    return {**core, 'snapshot_sha256': sha(core)}


def _validate_snapshot_shape(snapshot):
    for key in ('after_seq', 'through_seq', 'committed_high_watermark', 'row_count', 'evidence_bytes'):
        _integer(snapshot[key], 'snapshot_integer_required:' + key)
    require(snapshot['after_seq'] <= snapshot['through_seq'] <= snapshot['committed_high_watermark'], 'snapshot_sequence_range_invalid')
    require(snapshot['row_count'] <= MAX_ROWS and snapshot['evidence_bytes'] <= MAX_BYTES, 'snapshot_size_range_invalid')
    require(type(snapshot['more_pending']) is bool and snapshot['more_pending'] == (snapshot['through_seq'] < snapshot['committed_high_watermark']), 'snapshot_pending_binding_invalid')
    require(snapshot['complete_historical_window'] is False, 'no_historical_completeness_claim')
    for key in ('device', 'inode'):
        _integer(snapshot['input_identity'][key], 'input_identity_integer_required:' + key)
    identity_sha = sha(snapshot['input_identity']); policy_sha = sha(snapshot['policy'])
    def checkpoint(value, through):
        require(type(value) is dict and set(value) == {'input_identity_sha256', 'policy_sha256', 'through_seq', 'anchor_sha256'}, 'snapshot_checkpoint_schema_invalid')
        _integer(value['through_seq'], 'checkpoint_through_integer_required')
        require(value['through_seq'] == through and value['input_identity_sha256'] == identity_sha and value['policy_sha256'] == policy_sha, 'snapshot_checkpoint_binding_invalid')
        require((through == 0 and value['anchor_sha256'] is None) or
            (through > 0 and type(value['anchor_sha256']) is str and re.fullmatch('[0-9a-f]{64}', value['anchor_sha256']) is not None), 'snapshot_checkpoint_anchor_invalid')
    previous = snapshot['previous_checkpoint']
    if previous is None: require(snapshot['after_seq'] == 0, 'initial_snapshot_sequence_invalid')
    else: checkpoint(previous, snapshot['after_seq'])
    checkpoint(snapshot['next_checkpoint'], snapshot['through_seq'])
    expected_anchor = sha(snapshot['entries'][-1]['projection']) if snapshot['entries'] else (previous['anchor_sha256'] if previous else None)
    require(snapshot['next_checkpoint']['anchor_sha256'] == expected_anchor, 'next_checkpoint_exact_anchor_invalid')


def latest_admitted_as_of(batches, decision_epoch):
    """Select canonical latest version first, including neutral/retracted members.

    Accept only explicitly supplied immutable admission/readback envelopes.
    This does not claim the supplied envelope was persisted by a real writer.
    Missing batches are not silently described as a complete historical window.
    """
    decision = _number(decision_epoch)
    require(isinstance(batches, list) and len(batches) <= MAX_BATCHES, 'bounded_admitted_batches_required')
    selected = {}; seen_batch = {}; seen_seq = {}; stream = None; policy_sha = None
    last_batch = 0; last_through = 0; last_available = -math.inf
    for envelope in sorted(batches, key=lambda v: v['admission']['batch_sequence']):
        snapshot = envelope['snapshot']; admission = envelope['admission']
        core = {k: v for k, v in snapshot.items() if k != 'snapshot_sha256'}
        _validate_snapshot_shape(snapshot)
        require(snapshot.get('snapshot_sha256') == sha(core) and snapshot.get('schema_version') == SCHEMA
            and snapshot.get('complete_declared_prefix') is True and snapshot.get('admitted') is False, 'snapshot_binding_invalid')
        require(admission.get('schema_version') == ADMISSION and admission.get('status') == 'admitted'
            and admission.get('availability_basis') == 'externally_supplied_durable_admission_readback', 'admission_required')
        body = {k: v for k, v in admission.items() if k != 'admission_sha256'}
        require(admission.get('admission_sha256') == sha(body) and admission.get('snapshot_sha256') == snapshot['snapshot_sha256'], 'admission_snapshot_mismatch')
        _integer(admission['through_seq'], 'admission_through_integer_required')
        if admission['first_projection_seq'] is not None:
            _integer(admission['first_projection_seq'], 'admission_first_integer_required', 1)
        seq = admission['batch_sequence']; require(type(seq) is int and seq > 0, 'admission_sequence_invalid')
        batchid = sha([ADMISSION, sha(snapshot['input_identity']), seq, snapshot['snapshot_sha256']])
        require(admission.get('batch_id') == batchid and admission.get('first_projection_seq') ==
            (snapshot['entries'][0]['projection']['projection_seq'] if snapshot['entries'] else None)
            and admission.get('through_seq') == snapshot['through_seq'], 'admission_sequence_binding_mismatch')
        if seq in seen_batch:
            require(seen_batch[seq] == admission['admission_sha256'], 'duplicate_batch_conflict')
            continue  # Identical duplicate delivery is not another observation.
        seen_batch[seq] = admission['admission_sha256']
        current_stream = sha(snapshot['input_identity']); current_policy = sha(snapshot['policy'])
        if stream is None: stream, policy_sha = current_stream, current_policy
        require((stream, policy_sha) == (current_stream, current_policy), 'cross_stream_or_policy_mix_refused')
        require(seq == last_batch + 1 and snapshot['after_seq'] == last_through, 'complete_admission_prefix_required')
        admitted = _number(admission['admitted_epoch']); available = _number(admission['admitted_available_epoch'])
        require(snapshot['read_started_epoch'] <= snapshot['read_completed_epoch'] <= admitted <= available
            and available >= last_available, 'admission_clock_order_invalid')
        repair.validate_clock_state(snapshot['initial_clock_state'], snapshot['read_started_epoch'])
        repair.validate_clock_state(snapshot['initial_clock_state'], snapshot['read_completed_epoch'])
        repair.validate_clock_state(snapshot['completion_clock_state'], snapshot['read_completed_epoch'])
        repair.validate_clock_state(snapshot['initial_clock_state'], admitted)
        repair.validate_clock_state(snapshot['initial_clock_state'], available)
        repair.validate_clock_state(admission['clock_state'], admitted)
        repair.validate_clock_state(admission['clock_state'], available)
        require(snapshot['row_count'] == len(snapshot['entries']) <= MAX_ROWS and
            snapshot['evidence_bytes'] == sum(len(encoded(e)) for e in snapshot['entries']) <= MAX_BYTES, 'snapshot_size_mismatch')
        prior_seq = snapshot['after_seq']
        for evidence in snapshot['entries']:
            p = evidence['projection']; s = p['projection_seq']
            require(type(s) is int and prior_seq < s <= snapshot['through_seq'] and s not in seen_seq, 'duplicate_or_unordered_projection_sequence')
            seen_seq[s] = p['projection_id']; prior_seq = s
            payload = validate_projection(evidence, snapshot['policy'], snapshot['read_started_epoch'])
            if available <= decision:
                selected[p['canonical_event_id']] = {'canonical_event_id': p['canonical_event_id'],
                    'projection_seq': s, 'projection_id': p['projection_id'], 'payload': copy.deepcopy(payload),
                    'evidence': copy.deepcopy(evidence), 'batch_id': batchid,
                    'snapshot_sha256': snapshot['snapshot_sha256'], 'admission_sha256': admission['admission_sha256'],
                    'admitted_available_epoch': available}
        require(prior_seq == snapshot['through_seq'], 'through_sequence_mismatch')
        last_batch, last_through, last_available = seq, snapshot['through_seq'], available
    return {'schema_version': ADMISSION, 'decision_epoch': decision,
        'members': [selected[k] for k in sorted(selected)], 'canonical_event_count': len(selected),
        'selection_before_relevance_score_and_headline_filters': True,
        'full_history_window_proven': False, 'durable_storage_proven_by_this_pure_function': False, **INERT}
