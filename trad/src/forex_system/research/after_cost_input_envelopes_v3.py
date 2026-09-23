"""Canonical immutable envelope hashes for the after-cost v3 research branch."""

from __future__ import annotations

import copy
import hashlib
import json
import math
from typing import Any, Mapping


RECORD_HASH_FIELD = "canonical_record_sha256"
ENVELOPE_HASH_FIELD = "canonical_envelope_sha256"


class CanonicalEnvelopeError(ValueError):
    pass


def _reject_nonfinite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise CanonicalEnvelopeError("canonical JSON cannot contain NaN or infinity")
    if isinstance(value, Mapping):
        for item in value.values(): _reject_nonfinite(item)
    elif isinstance(value, (list, tuple)):
        for item in value: _reject_nonfinite(item)


def canonical_bytes(value: Any) -> bytes:
    _reject_nonfinite(value)
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise CanonicalEnvelopeError("value is not canonical-JSON serializable") from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def record_sha256(record: Mapping[str, Any]) -> str:
    payload = copy.deepcopy(dict(record)); payload.pop(RECORD_HASH_FIELD, None)
    return canonical_sha256(payload)


def envelope_sha256(envelope: Mapping[str, Any]) -> str:
    payload = copy.deepcopy(dict(envelope)); payload.pop(ENVELOPE_HASH_FIELD, None)
    return canonical_sha256(payload)


def seal_record(record: Mapping[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(dict(record)); payload[RECORD_HASH_FIELD] = record_sha256(payload); return payload


def seal_envelope(envelope: Mapping[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(dict(envelope))
    records = payload.get("records") or []
    if not isinstance(records, list) or any(not isinstance(record, Mapping) for record in records):
        raise CanonicalEnvelopeError("envelope records must be a list of mappings")
    for record in records:
        verify_record(record)
    payload["record_count"] = len(records)
    payload[ENVELOPE_HASH_FIELD] = envelope_sha256(payload)
    return payload


def verify_record(record: Mapping[str, Any]) -> str:
    claimed = str(record.get(RECORD_HASH_FIELD) or "").lower(); actual = record_sha256(record)
    if claimed != actual: raise CanonicalEnvelopeError("canonical record hash mismatch")
    return actual


def verify_envelope(envelope: Mapping[str, Any]) -> str:
    claimed = str(envelope.get(ENVELOPE_HASH_FIELD) or "").lower(); actual = envelope_sha256(envelope)
    if claimed != actual: raise CanonicalEnvelopeError("canonical envelope hash mismatch")
    return actual


__all__ = ["CanonicalEnvelopeError", "canonical_bytes", "canonical_sha256", "record_sha256", "envelope_sha256", "seal_record", "seal_envelope", "verify_record", "verify_envelope"]
