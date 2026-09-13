"""Immutable local research-curve I/O with independently observed receipts.

No model, account, supervisor, feed or broker imports. The registered caller owns
expected model/cohort/policy/source identity and actual filesystem access. Files
are exclusive, flushed, fsynced and read back; no file is deleted or overwritten.
Partial crash files remain explicit refusals. File fsync does not establish
Windows directory durability or survival of every power-loss scenario.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

import oanda_forecast_curve_contract_v1 as contract

DESCRIPTOR_SCHEMA = 'forecast_curve_file_descriptor_v1_20260909'
_SHA = re.compile(r'^[0-9a-f]{64}$')
_DESCRIPTOR_KEYS = {'schema_version', 'curve_sha256', 'persisted_curve_bytes_sha256',
                    'publication_sha256', 'publication_bytes_sha256', 'descriptor_sha256',
                    *contract.AUTHORITY}


def _require(condition, code):
    if not condition:
        raise contract.CurveContractError(code)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _safe_components(path, *, require_file=False):
    """Reject reparse points in every retained absolute path component.

    The directory must be controlled by the caller. Checks bracket I/O but are
    not a sandbox against an administrator concurrently replacing directories.
    """
    path = Path(path)
    _require(path.is_absolute(), 'curve_store_absolute_path_required')
    current = Path(path.anchor)
    try:
        for part in path.parts[1:]:
            current /= part
            info = current.lstat()
            _require(not stat.S_ISLNK(info.st_mode)
                     and not (getattr(info, 'st_file_attributes', 0) & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 1024)),
                     'curve_store_reparse_point')
            last = current == path
            _require(stat.S_ISREG(info.st_mode) if last and require_file else stat.S_ISDIR(info.st_mode),
                     'curve_store_path_type')
    except OSError:
        raise contract.CurveContractError('curve_store_path_unavailable') from None
    return path


def _directory(root, parts=(), *, create=False):
    try:
        root = Path(os.path.abspath(os.fspath(root)))
    except (TypeError, ValueError, OSError):
        raise contract.CurveContractError('curve_store_root_invalid') from None
    _require(all(isinstance(part, str) and re.fullmatch(r'[a-z0-9_]{1,64}', part) for part in parts),
             'curve_store_internal_path_invalid')
    desired = root.joinpath(*parts)
    current = Path(desired.anchor)
    for part in desired.parts[1:]:
        current /= part
        try:
            if create:
                current.mkdir(exist_ok=True)
        except OSError:
            raise contract.CurveContractError('curve_store_directory_creation_failed') from None
        _safe_components(current)
    _require(desired == root or root in desired.parents, 'curve_store_path_escape')
    return desired


def _identity(value):
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns


def _read(path):
    _safe_components(path, require_file=True)
    try:
        before = path.lstat()
        _require(0 < before.st_size <= contract.MAX_BYTES, 'curve_store_read_byte_limit')
        with path.open('rb') as handle:
            opened = os.fstat(handle.fileno())
            raw = handle.read(contract.MAX_BYTES+1)
            finished = os.fstat(handle.fileno())
        after = path.lstat()
    except OSError:
        raise contract.CurveContractError('curve_store_read_failed') from None
    _safe_components(path, require_file=True)
    _require(len(raw) <= contract.MAX_BYTES and len({_identity(v) for v in (before, opened, finished, after)}) == 1,
             'curve_store_read_changed_or_oversized')
    return raw


def _decode(raw):
    def pairs(items):
        value = {}
        for key, item in items:
            _require(key not in value, 'curve_store_duplicate_json_key')
            value[key] = item
        return value
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(contract.CurveContractError('curve_store_nonfinite_json')))
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        if isinstance(exc, contract.CurveContractError):
            raise
        raise contract.CurveContractError('curve_store_invalid_json') from None
    _require(contract.canonical_bytes(value) == raw, 'curve_store_noncanonical_json')
    return value


def _write_exclusive(path, raw):
    _require(0 < len(raw) <= contract.MAX_BYTES, 'curve_store_write_byte_limit')
    _safe_components(path.parent)
    try:
        with path.open('xb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        return False
    except OSError:
        # A partial orphan is intentionally retained. Never recover by replacing it.
        raise contract.CurveContractError('curve_store_write_or_sync_failed') from None
    _require(_read(path) == raw, 'curve_store_write_readback_mismatch')
    return True


def _descriptor(curve, publication):
    body = dict(schema_version=DESCRIPTOR_SCHEMA, curve_sha256=curve['curve_sha256'],
        persisted_curve_bytes_sha256=_sha(contract.canonical_bytes(curve)),
        publication_sha256=publication['publication_sha256'],
        publication_bytes_sha256=_sha(contract.canonical_bytes(publication)), **contract.AUTHORITY)
    return {**body, 'descriptor_sha256':contract.content_hash(body)}


def _validate_descriptor(value):
    _require(isinstance(value, dict) and set(value) == _DESCRIPTOR_KEYS, 'curve_store_descriptor_shape')
    _require(value.get('schema_version') == DESCRIPTOR_SCHEMA
             and all(value.get(k) is v for k,v in contract.AUTHORITY.items()), 'curve_store_descriptor_authority_or_schema')
    for key in ('curve_sha256','persisted_curve_bytes_sha256','publication_sha256','publication_bytes_sha256','descriptor_sha256'):
        _require(isinstance(value[key], str) and _SHA.fullmatch(value[key]) is not None, 'curve_store_descriptor_hash')
    _require(contract.content_hash({k:v for k,v in value.items() if k!='descriptor_sha256'}) == value['descriptor_sha256'],
             'curve_store_descriptor_seal')
    # Do not retain a mutable caller descriptor across the independently timed read.
    return _decode(contract.canonical_bytes(value))


def _validate_publication(curve, publication, expected_source_bindings):
    _require(isinstance(publication, dict), 'curve_store_publication_object')
    required = ('persisted_bytes_sha256','publication_started_epoch','publication_completed_epoch')
    _require(all(key in publication for key in required), 'curve_store_publication_fields')
    rebuilt = contract.publication_receipt(curve,
        persisted_bytes_sha256=publication['persisted_bytes_sha256'],
        publication_started_epoch=publication['publication_started_epoch'],
        expected_source_bindings=expected_source_bindings, clock=lambda:publication['publication_completed_epoch'])
    _require(publication == rebuilt, 'curve_store_publication_semantic_mismatch')
    return publication


def publish_curve(root, curve, *, expected_source_bindings, clock=time.time):
    """Exclusively persist issued bytes, then their factual publication receipt.

    Existing complete publications retain their original clocks. A curve with no
    complete publication receipt is an orphan/in-progress refusal; retry never
    changes the issued curve or invents successful historical publication.
    """
    curve = contract.validate_curve(curve, expected_source_bindings=expected_source_bindings)
    raw = contract.canonical_bytes(curve)
    begun = contract.epoch(clock())
    _require(begun >= curve['issued_epoch'], 'curve_store_publication_before_issue')
    directory = _directory(root, ('curves',curve['curve_sha256']), create=True)
    path, pub_path = directory/'curve.json', directory/'publication.json'
    created = _write_exclusive(path, raw)
    _require(_read(path) == raw, 'curve_store_existing_curve_conflict')
    if not created:
        try:
            publication = _decode(_read(pub_path))
        except contract.CurveContractError:
            raise contract.CurveContractError('curve_store_orphan_or_in_progress_publication') from None
        _validate_publication(curve, publication, expected_source_bindings)
        observed = contract.epoch(clock())
        _require(observed >= publication['publication_completed_epoch'], 'curve_store_existing_publication_future')
        return dict(descriptor=_descriptor(curve,publication),publication=publication,
            curve_created=False,publication_created=False,existing_record=True,
            receipt_persisted_observed_epoch=observed)
    publication = contract.publication_receipt(curve,persisted_bytes_sha256=_sha(_read(path)),
        publication_started_epoch=begun,expected_source_bindings=expected_source_bindings,clock=clock)
    pub_raw = contract.canonical_bytes(publication)
    published = _write_exclusive(pub_path,pub_raw)
    _require(published, 'curve_store_unexpected_publication_collision')
    _require(_read(pub_path) == pub_raw, 'curve_store_publication_readback_mismatch')
    observed = contract.epoch(clock())
    _require(observed >= publication['publication_completed_epoch'], 'curve_store_receipt_persistence_clock')
    return dict(descriptor=_descriptor(curve,publication),publication=publication,
        curve_created=True,publication_created=True,existing_record=False,
        receipt_persisted_observed_epoch=observed)


def consume_published_curve(root, descriptor, *, expected_source_bindings,
                            persist_consumption=False, clock=time.time):
    """Independently read/hash both files before sampling actual availability.

    Consumption is an observation receipt; eligibility remains target-specific
    in the decision adapter. Optional consumption persistence never rebases the
    observation epoch and returns its later persistence clock separately.
    """
    _require(type(persist_consumption) is bool, 'curve_store_persistence_option')
    descriptor = _validate_descriptor(descriptor)
    directory = _directory(root, ('curves',descriptor['curve_sha256']))
    raw = _read(directory/'curve.json')
    _require(_sha(raw) == descriptor['persisted_curve_bytes_sha256'], 'curve_store_curve_bytes_hash')
    curve = _decode(raw)
    contract.validate_curve(curve,expected_source_bindings=expected_source_bindings)
    _require(curve['curve_sha256'] == descriptor['curve_sha256'], 'curve_store_curve_identity')
    pub_raw = _read(directory/'publication.json')
    _require(_sha(pub_raw) == descriptor['publication_bytes_sha256'], 'curve_store_publication_bytes_hash')
    publication = _decode(pub_raw)
    _validate_publication(curve,publication,expected_source_bindings)
    _require(publication['publication_sha256'] == descriptor['publication_sha256'], 'curve_store_publication_identity')
    consumption = contract.consume_curve(curve,publication,expected_source_bindings=expected_source_bindings,clock=clock)
    persisted = None
    created = False
    if persist_consumption:
        # The receipt binds its curve internally. Do not repeat the 64-character
        # curve hash in this path: real Windows workspace roots otherwise push
        # optional observation receipts beyond the legacy path-length boundary.
        consumption_dir = _directory(root,('consumptions',),create=True)
        consumption_path = consumption_dir/(consumption['consumption_sha256']+'.json')
        consumption_raw = contract.canonical_bytes(consumption)
        created = _write_exclusive(consumption_path,consumption_raw)
        _require(_read(consumption_path) == consumption_raw, 'curve_store_existing_consumption_conflict')
        persisted = contract.epoch(clock())
        _require(persisted >= consumption['observed_epoch'], 'curve_store_consumption_persistence_clock')
    return dict(curve=curve,publication=publication,consumption=consumption,
        consumption_record_created=created,consumption_persisted_observed_epoch=persisted)
