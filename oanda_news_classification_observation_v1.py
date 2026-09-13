"""Append-only derived classification availability, separate from story arrival."""
from __future__ import annotations
import json
from typing import Mapping
from oanda_news_source_observation_ledger_v1 import (
    ACTIVE_FIELDS, ENVELOPE_FIELDS, PROVENANCE_FIELDS, CONTRACT as SOURCE_CONTRACT,
    aware, encoded, digest, ledger_transaction, source_version_floor, table_row, capture_origin, install_insert_conflict_guard,
)

CONTRACT = 'article_classification_observation_v1_20260912'
TABLE = 'article_classification_observations_v1'
FIELDS = ('classification_observation_contract', 'classification_observation_version_id',
    'classification_first_known_utc', 'classification_available_utc', 'classification_clock_provenance',
    'classification_clock_status', 'classification_source_lineage_status',
    'classification_content_sha256', 'classification_known_time_basis',
    'source_evidence_contract', 'source_evidence_content_sha256', 'source_evidence_available_utc')


def initialize(connection):
    with ledger_transaction(connection):
        connection.execute('''CREATE TABLE IF NOT EXISTS article_classification_observations_v1 (
            observation_seq INTEGER PRIMARY KEY AUTOINCREMENT, observation_id TEXT NOT NULL UNIQUE,
            contract_id TEXT NOT NULL, canonical_event_id TEXT NOT NULL, source_version_id TEXT NOT NULL,
            classification_version_id TEXT NOT NULL, content_json TEXT NOT NULL, content_sha256 TEXT NOT NULL,
            derived_payload_json TEXT NOT NULL, derived_payload_sha256 TEXT NOT NULL,
            supplied_clock_json TEXT NOT NULL, supplied_provenance_json TEXT NOT NULL,
            operation TEXT NOT NULL, clock_status TEXT NOT NULL, clock_reasons_json TEXT NOT NULL,
            known_utc TEXT, known_epoch REAL)''')
        connection.execute('CREATE INDEX IF NOT EXISTS idx_classification_version_seq_v1 ON article_classification_observations_v1(classification_version_id,observation_seq)')
        install_insert_conflict_guard(connection, TABLE)
        for action in ('UPDATE', 'DELETE'):
            connection.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_{TABLE}_{action.lower()}
                BEFORE {action} ON {TABLE} BEGIN SELECT RAISE(ABORT,'append_only_classification_evidence'); END''')


def content(payload):
    excluded = set(ENVELOPE_FIELDS) | set(ACTIVE_FIELDS) | set(FIELDS)
    return {key:value for key,value in payload.items() if key not in excluded}


def classification_floor(payload):
    marker = payload.get('classification_observation_contract')
    if marker is None:
        return None
    if marker != CONTRACT or payload.get('classification_clock_status') != 'valid_attested_classification':
        raise ValueError('unsupported_or_unproven_classification_clock')
    first = aware(payload.get('classification_first_known_utc'))
    available = aware(payload.get('classification_available_utc'))
    if first is None or available is None or available < first:
        raise ValueError('classification_known_boundary_invalid')
    return available


def classification_provenance_valid(payload, expected_provenance):
    if payload.get('classification_observation_contract') is None:
        return True
    try:
        classification_floor(payload)
    except ValueError:
        return False
    provenance = payload.get('classification_clock_provenance')
    # A newly derived legacy story is not retroactively proven to have the
    # current source/query lineage, even when its new computation has a clock.
    return bool(payload.get('source_observation_ledger_contract') == SOURCE_CONTRACT
        and payload.get('classification_source_lineage_status') == 'version_bound'
        and isinstance(provenance, Mapping) and provenance.get('observation_clock_trusted') is True
        and all(provenance.get(key) == value for key,value in expected_provenance.items()))


def record_and_bind(connection, payload, previous, clock_provider, *, expected_provenance, operation, expected_stored_payload_json=None):
    """Invoke the supplied clock only after the caller computed this payload.

    This is an attested post-computation/pre-commit boundary, not proof that a
    downstream observer had read the committed row at that instant.
    """
    result = dict(payload)
    previous = previous or {}
    for key in PROVENANCE_FIELDS:
        if previous:
            if key in previous:
                result[key] = previous[key]
            else:
                result[key] = False if key == 'observation_clock_trusted' else 'legacy_unattested' if key == 'observation_clock_source' else ''
    content_text = encoded(content(result))
    content_sha = digest(content_text)
    source_id = str(result.get('source_observation_version_id') or '')
    version_id = digest(encoded([CONTRACT, str(result['event_id']), source_id, content_sha]))
    raw_clock, provenance, reasons = None, None, []
    try:
        if not callable(clock_provider):
            reasons.append('missing_postcomputation_clock_provider')
        else:
            raw_clock, provenance = clock_provider()
    except Exception as error:
        reasons.append('postcomputation_clock_provider_failed:' + type(error).__name__)
    supplied_clock = raw_clock.isoformat() if hasattr(raw_clock, 'isoformat') else raw_clock
    clock = aware(raw_clock)
    if clock is None:
        reasons.append('postcomputation_clock_missing_invalid_or_naive')
    if not isinstance(provenance, Mapping) or provenance.get('observation_clock_trusted') is not True:
        reasons.append('postcomputation_clock_unattested')
    for key, expected in expected_provenance.items():
        if not isinstance(provenance, Mapping) or provenance.get(key) != expected:
            reasons.append('postcomputation_provenance_mismatch_' + key)
    source_floor = source_version_floor(result)
    prior_floor = classification_floor(previous)
    first_seen = aware(result.get('first_seen_utc'))
    for name, floor in (('source_version', source_floor), ('prior_classification', prior_floor), ('story_first_seen', first_seen)):
        if clock is not None and floor is not None and clock < floor:
            reasons.append('postcomputation_clock_before_' + name)
    existing = table_row(connection, str(result['event_id']))
    if expected_stored_payload_json is not None and (existing is None or existing['payload_json'] != expected_stored_payload_json):
        reasons.append('source_projection_changed_during_classification')
    status = 'valid_attested_classification' if not reasons else 'unproven_classification' 
    raw_payload = encoded(result)
    observation_id = digest(encoded([CONTRACT, version_id, digest(raw_payload), supplied_clock, provenance, operation]))
    if existing is not None:
        capture_origin(connection, existing, 'legacy_origin_query_history_unresolved')
    connection.execute('''INSERT OR IGNORE INTO article_classification_observations_v1
        (observation_id,contract_id,canonical_event_id,source_version_id,classification_version_id,
         content_json,content_sha256,derived_payload_json,derived_payload_sha256,supplied_clock_json,
         supplied_provenance_json,operation,clock_status,clock_reasons_json,known_utc,known_epoch)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (observation_id, CONTRACT, str(result['event_id']), source_id, version_id, content_text, content_sha,
         raw_payload, digest(raw_payload), encoded(supplied_clock), encoded(provenance), operation, status,
         encoded(reasons), clock.isoformat() if not reasons else None, clock.timestamp() if not reasons else None))
    if reasons:
        return None
    first = connection.execute('''SELECT known_utc FROM article_classification_observations_v1
        WHERE classification_version_id=? AND clock_status='valid_attested_classification'
        ORDER BY observation_seq LIMIT 1''', (version_id,)).fetchone()[0]
    same_active_version = previous.get('classification_observation_version_id') == version_id
    available = prior_floor if same_active_version and prior_floor is not None else clock
    if same_active_version:
        provenance = previous.get('classification_clock_provenance', provenance)
    result.update(classification_observation_contract=CONTRACT,
        classification_observation_version_id=version_id, classification_first_known_utc=first,
        classification_available_utc=available.isoformat(), classification_clock_provenance=provenance,
        classification_clock_status=status,
        classification_source_lineage_status='version_bound' if source_floor is not None else 'legacy_origin_query_history_unresolved',
        classification_content_sha256=content_sha,
        classification_known_time_basis='explicit_attested_postcomputation_precommit;not_downstream_readback_proof')
    body_fields = ('headline', 'summary', 'source_url', 'material_content_sha256',
                   'detail_content_sha256', 'detail_attachment_content_sha256')
    # Reclassification operates on an already retained body. Source/query or
    # classification metadata changes alone are not another physical story.
    body = previous if operation == 'retained_reclassification' and previous else result
    body_sha = digest(encoded({key:body.get(key) for key in body_fields}))
    previous_body_sha = previous.get('source_evidence_content_sha256') or digest(encoded({key:previous.get(key) for key in body_fields}))
    if previous and (body_sha == previous_body_sha or operation == 'retained_reclassification'):
        evidence_clock = aware(previous.get('source_evidence_available_utc'))
        if evidence_clock is None:
            evidence_clock = max(value for key in ('first_seen_utc','published_utc','detail_available_utc')
                                 if (value := aware(previous.get(key))) is not None)
    else:
        evidence_clock = max([value for key in ('first_seen_utc','published_utc','detail_available_utc')
                              if (value := aware(result.get(key))) is not None] + ([source_floor] if source_floor is not None else []))
    result.update(source_evidence_contract='retained_story_body_age_v1_20260912',
                  source_evidence_content_sha256=body_sha, source_evidence_available_utc=evidence_clock.isoformat())
    current = aware(result.get('causal_known_utc'))
    result['causal_known_utc'] = max(available, current).isoformat() if current else available.isoformat()
    numeric = aware(result.get('numeric_causal_known_utc'))
    if numeric is not None:
        result['numeric_causal_known_utc'] = max(available, numeric).isoformat()
    return result


def publication_as_of(clock_provider, minimum, *, expected_provenance):
    if not callable(clock_provider):
        raise ValueError('missing_postcomputation_publication_clock')
    raw, provenance = clock_provider()
    clock = aware(raw)
    floor = aware(minimum)
    if clock is None or floor is None or clock < floor:
        raise ValueError('publication_clock_invalid_or_regressed')
    if not isinstance(provenance, Mapping) or provenance.get('observation_clock_trusted') is not True or not all(provenance.get(key)==value for key,value in expected_provenance.items()):
        raise ValueError('publication_clock_unattested')
    return clock
