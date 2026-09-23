"""Pure immutable boundary for a separately registered family-summary producer.

This module performs no I/O, samples no clocks and grants no execution authority.
The caller must bind this source in its new registration, pass actual readback
bytes/completion, and publish exactly FrozenSummary.raw. Only after successful
readback should it replace its last PublishedSummary. Old source/cohorts remain
unchanged. These checks attest an envelope, not ledger or forecast eligibility.
"""
from dataclasses import dataclass
import hashlib
import json
import math
import re

VERSION = 'immutable_family_summary_publication_v1_20260909'
MAX_BYTES = 1024 * 1024
MAX_NODES = 100000
MAX_DEPTH = 24
MAX_STRING = 8192
MAX_PAIRS = 68
MAX_FAMILIES_PER_PAIR = 2
INERT_FLAGS = ('can_place_orders', 'can_promote', 'can_authorize', 'account_eligible',
               'proof_eligible', 'historical_rows_imported')
STATUSES = ('forecast', 'warming', 'building', 'unavailable', 'ready')
SUMMARY_KEYS = frozenset(('schema_version', 'registry_sha256', 'generated_epoch',
    'research_only', 'rows', 'payload_sha256', *INERT_FLAGS))


class SummaryBoundaryError(ValueError):
    """Bounded codes only; no source payload or raw exception text."""


def _require(ok, code):
    if not ok:
        raise SummaryBoundaryError(code)


def _epoch(value):
    _require(type(value) in (int, float), 'summary_boundary_clock_type')
    _require(0 < value < 10**12 and math.isfinite(value), 'summary_boundary_clock_range')
    return value


def _hash(value):
    _require(type(value) is str and re.fullmatch('[a-f0-9]{64}', value) is not None,
             'summary_boundary_hash_format')
    return value


def _shape(value):
    remaining = [MAX_NODES]

    def walk(item, depth):
        remaining[0] -= 1
        _require(remaining[0] >= 0 and depth <= MAX_DEPTH, 'summary_boundary_structure_limit')
        if item is None or type(item) is bool:
            return
        if type(item) is str:
            _require(len(item) <= MAX_STRING, 'summary_boundary_string_limit')
        elif type(item) is int:
            _require(-10**30 < item < 10**30, 'summary_boundary_integer_limit')
        elif type(item) is float:
            _require(math.isfinite(item), 'summary_boundary_nonfinite_number')
        elif type(item) is list:
            _require(len(item) <= MAX_NODES, 'summary_boundary_structure_limit')
            for child in item:
                walk(child, depth+1)
        elif type(item) is dict:
            _require(len(item) <= MAX_NODES, 'summary_boundary_structure_limit')
            for key, child in item.items():
                _require(type(key) is str and 1 <= len(key) <= 128, 'summary_boundary_key')
                walk(child, depth+1)
        else:
            raise SummaryBoundaryError('summary_boundary_json_type')
    walk(value, 0)


def _encoded(value):
    try:
        _shape(value)
        raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    except SummaryBoundaryError:
        raise
    except (ValueError, TypeError, RuntimeError, RecursionError):
        raise SummaryBoundaryError('summary_boundary_canonical_encoding') from None
    _require(len(raw) <= MAX_BYTES, 'summary_boundary_byte_limit')
    return raw


def _digest(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _decode(raw):
    _require(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'summary_boundary_raw_bytes')
    try:
        value = json.loads(raw)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise SummaryBoundaryError('summary_boundary_json_decode') from None
    _require(_encoded(value) == raw, 'summary_boundary_noncanonical_bytes')
    return value


def _validate(value, expected_schema, expected_registry_sha256):
    _require(type(expected_schema) is str and re.fullmatch('[a-z][a-z0-9_]{1,127}', expected_schema) is not None,
             'summary_boundary_expected_schema')
    _hash(expected_registry_sha256)
    _require(type(value) is dict and set(value) == SUMMARY_KEYS, 'summary_boundary_envelope_schema')
    _require(value['schema_version'] == expected_schema, 'summary_boundary_schema_identity')
    _require(value['registry_sha256'] == expected_registry_sha256, 'summary_boundary_registry_identity')
    _require(value['research_only'] is True and all(value[flag] is False for flag in INERT_FLAGS),
             'summary_boundary_inert_flags')
    generated = _epoch(value['generated_epoch'])
    _require(type(value['rows']) is list and 1 <= len(value['rows']) <= MAX_PAIRS, 'summary_boundary_pair_limit')
    seen = set()
    for row in value['rows']:
        _require(type(row) is dict and set(row) == {'instrument', 'pip_size', 'families'}, 'summary_boundary_row_shape')
        pair = row['instrument']
        _require(type(pair) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', pair) is not None and pair not in seen,
                 'summary_boundary_pair_identity')
        seen.add(pair)
        families = row['families']
        _require(type(families) is dict and 1 <= len(families) <= MAX_FAMILIES_PER_PAIR, 'summary_boundary_family_limit')
        for family, slot in families.items():
            _require(type(family) is str and re.fullmatch('[a-z][a-z0-9_]{1,63}', family) is not None,
                     'summary_boundary_family_identity')
            _require(type(slot) is dict and slot.get('status') in STATUSES, 'summary_boundary_family_status')
            _require(_epoch(slot.get('observed_epoch')) <= generated, 'summary_boundary_future_row')
    _hash(value['payload_sha256'])
    _require(_digest({k: v for k, v in value.items() if k != 'payload_sha256'}) == value['payload_sha256'],
             'summary_boundary_payload_seal')
    return value


@dataclass(frozen=True, slots=True)
class FrozenSummary:
    """Only immutable byte/scalar values; no alias to a caller's nested state."""
    raw: bytes
    summary_sha256: str
    payload_sha256: str
    schema_version: str
    registry_sha256: str
    generated_epoch: float


@dataclass(frozen=True, slots=True)
class PublishedSummary:
    """Caller-supplied actual readback, not an internally observed file event."""
    snapshot: FrozenSummary
    read_completed_epoch: float
    readback_sha256: str


def freeze_summary(summary, *, expected_schema, expected_registry_sha256):
    """Validate the sealed, detached canonical bytes; never renew any clock/seal."""
    raw = _encoded(summary)
    value = _validate(_decode(raw), expected_schema, expected_registry_sha256)
    return FrozenSummary(raw=raw, summary_sha256=hashlib.sha256(raw).hexdigest(),
        payload_sha256=value['payload_sha256'], schema_version=value['schema_version'],
        registry_sha256=value['registry_sha256'], generated_epoch=value['generated_epoch'])


def _validated_frozen(snapshot):
    _require(type(snapshot) is FrozenSummary, 'summary_boundary_snapshot_type')
    value = _validate(_decode(snapshot.raw), snapshot.schema_version, snapshot.registry_sha256)
    _epoch(snapshot.generated_epoch)
    _require(snapshot.summary_sha256 == hashlib.sha256(snapshot.raw).hexdigest()
        and snapshot.payload_sha256 == value['payload_sha256']
        and snapshot.generated_epoch == value['generated_epoch'], 'summary_boundary_snapshot_identity')
    return value


def verify_written_summary(snapshot, observed_raw, *, read_completed_epoch):
    """Accept only exact written/readback bytes and a non-backdated read clock."""
    _validated_frozen(snapshot)
    _require(type(observed_raw) is bytes and observed_raw == snapshot.raw, 'summary_boundary_readback_mismatch')
    completed = _epoch(read_completed_epoch)
    _require(completed >= snapshot.generated_epoch, 'summary_boundary_readback_before_summary')
    return PublishedSummary(snapshot=snapshot, read_completed_epoch=completed,
        readback_sha256=hashlib.sha256(observed_raw).hexdigest())


def heartbeat_fields(published, *, generated_epoch):
    """Derive all summary-dependent header fields from the admitted bytes only.

    Other live worker metadata may be added by the new producer. These counts
    describe the snapshot's original observations and are not refreshed row or
    target eligibility. The consumer must still validate original row/H1 clocks.
    """
    _require(type(published) is PublishedSummary, 'summary_boundary_publication_type')
    value = _validated_frozen(published.snapshot)
    _require(published.readback_sha256 == published.snapshot.summary_sha256, 'summary_boundary_readback_identity')
    completed = _epoch(published.read_completed_epoch)
    generated = _epoch(generated_epoch)
    _require(value['generated_epoch'] <= completed <= generated, 'summary_boundary_heartbeat_clock_order')
    slots = [slot for row in value['rows'] for slot in row['families'].values()]
    return dict(summary_sha256=published.snapshot.summary_sha256,
        registry_sha256=value['registry_sha256'], generated_epoch=generated,
        summary_generated_epoch=value['generated_epoch'], summary_read_completed_epoch=completed,
        summary_boundary_version=VERSION, pair_count=len(value['rows']), family_count=len(slots),
        counts={status: sum(slot['status'] == status for slot in slots) for status in STATUSES},
        pairs_with_forecast=sum(any(slot['status'] == 'forecast' for slot in row['families'].values()) for row in value['rows']))
