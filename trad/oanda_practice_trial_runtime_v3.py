"""Staged runtime reliability; no broker I/O or policy/cap changes.

The exact reviewed immutable-order candidate is executed against privately
compiled, hash-pinned original policy bytes. No global base_policy alias is
installed. Status retries are a bounded attempt/sleep budget, not an absolute
filesystem timeout. Failed status publication never establishes broker failure.
"""
from __future__ import annotations

import builtins
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import threading
import time
from types import ModuleType
import uuid

SCHEMA = 'practice_trial_runtime_v3_20260913'
ROOT = Path(__file__).resolve().parent
CANDIDATE_NAME = 'practice_trial_runtime_vendor/immutable_order_preflight_v2.py'
PINNED_SOURCES = {
    CANDIDATE_NAME: 'ee4396854446864d50afb294c764ef2c116f6bb2b45c576dff291ee4e2c1595b',
    'oanda_practice_trial_policy_v2.py': '3156c910db6bd3fe3bd0ac9ffe4b232a8173eec82a3da8f81f2d339f3ccfecd1',
    'src/forex_system/contracts/signed_currency_exposure.py': 'aeb0301a68ba5e2bb8e10ac1771f1799e962122f8cad8ef35c947f6971a69fa9',
}
MAX_SOURCE_BYTES = 256 * 1024
MAX_STATUS_BYTES = 512 * 1024
RETRY_DELAYS = (.05, .10, .20)
_WRITE_LOCK = threading.Lock()


def _raw(path, cap):
    with Path(path).open('rb') as stream:
        raw = stream.read(cap + 1)
    if len(raw) > cap:
        raise ValueError('runtime_source_or_status_size')
    return raw


def _sources():
    rows = {name: _raw(ROOT / name, MAX_SOURCE_BYTES) for name in PINNED_SOURCES}
    if any(hashlib.sha256(raw).hexdigest() != PINNED_SOURCES[name] for name, raw in rows.items()):
        raise ValueError('immutable_preflight_source_changed')
    return rows


def _compile(raw, path, overrides):
    name = '_practice_pinned_' + uuid.uuid4().hex
    module = ModuleType(name)
    module.__file__ = str(path)
    original_import = builtins.__import__

    def importer(import_name, *args, **kwargs):
        return overrides[import_name] if import_name in overrides else original_import(import_name, *args, **kwargs)

    module.__dict__['__builtins__'] = dict(vars(builtins), __import__=importer)
    # dataclasses with postponed annotations consult this during compilation.
    # The private module is never published as an ordinary application import.
    sys.modules[name] = module
    try:
        exec(compile(raw, str(path), 'exec', dont_inherit=True), module.__dict__)
    finally:
        del sys.modules[name]
    return module


def validate_immutable_order(original_context, revalidation, *, expected_account_sha256,
                             expected_trial_id, transport_state='unknown'):
    rows = _sources()
    signed_name = 'src/forex_system/contracts/signed_currency_exposure.py'
    signed = _compile(rows[signed_name], ROOT / signed_name, {})
    policy_name = 'oanda_practice_trial_policy_v2.py'
    policy = _compile(rows[policy_name], ROOT / policy_name,
                      {'src.forex_system.contracts.signed_currency_exposure': signed})
    candidate = _compile(rows[CANDIDATE_NAME], ROOT / CANDIDATE_NAME, {'base_policy': policy})
    result = candidate.validate_immutable_order(original_context, revalidation,
        expected_account_sha256=expected_account_sha256, expected_trial_id=expected_trial_id,
        transport_state=transport_state)
    _sources()
    # Keep the candidate's original result/seal unchanged; this wrapper binds
    # the exact bytes actually compiled, without inventing an I/O receipt.
    return {'schema_version': SCHEMA, 'candidate_result': result,
            'compiled_source_bindings': dict(PINNED_SOURCES),
            'scope': 'Same current invocation only; broker retains claim/transport authority.'}


def sanitized_diagnostic(exc, *, operation):
    """No exception message, local variables, source lines or arbitrary paths."""
    allowed = {'status_open', 'status_write', 'status_flush', 'status_fsync',
               'status_replace', 'cycle', 'status_publication'}
    frames = []
    tb = exc.__traceback__
    while tb is not None:
        code = tb.tb_frame.f_code
        filename = Path(code.co_filename).name
        function = code.co_name
        frames.append({'source_file': filename if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_.-]{0,95}', filename) else 'redacted',
                       'function': function if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,95}', function) else 'redacted',
                       'line': tb.tb_lineno})
        frames = frames[-12:]
        tb = tb.tb_next
    return dict(schema_version=SCHEMA, type=type(exc).__name__,
                operation=operation if operation in allowed else 'unspecified',
                errno=exc.errno if isinstance(exc, OSError) and type(exc.errno) is int else None,
                winerror=getattr(exc, 'winerror', None) if type(getattr(exc, 'winerror', None)) is int else None,
                traceback_frames=frames, exception_text_retained=False,
                cause_or_lock_owner_established=False)


class StatusPublicationError(OSError):
    def __init__(self, diagnostic):
        super().__init__('status_publication_failed')
        self.diagnostic = diagnostic


def atomic_status(path, value, *, sleeper=time.sleep, clock=time.time):
    """Four attempts, at most .35 seconds of retry sleep; exact payload reused.

    One per-PID temp path is retained on failure, limiting temp accumulation
    within a worker. Durable ledger diagnostics retain each failure identity.
    Non-permission I/O errors are reported immediately, without blind retries.
    """
    last_clock = None
    def sample_clock():
        nonlocal last_clock
        now = clock()
        if type(now) not in (int, float) or not math.isfinite(now) or not 0 < now < 1e11:
            raise ValueError('status_publication_clock_invalid')
        if last_clock is not None and now < last_clock:
            raise ValueError('status_publication_clock_rollback')
        last_clock = now
        return now
    started = sample_clock()
    if type(value) is dict and 'observed_epoch' in value:
        observed = value['observed_epoch']
        if type(observed) not in (int, float) or not math.isfinite(observed) or not 0 < observed <= started:
            raise ValueError('status_payload_clock_after_publication_started')
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    if len(raw) > MAX_STATUS_BYTES:
        raise ValueError('status_byte_bound')
    path = Path(path)
    tmp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    failures = []
    with _WRITE_LOCK:
        for attempt in range(len(RETRY_DELAYS) + 1):
            operation = 'status_open'
            try:
                with tmp.open('wb') as stream:
                    operation = 'status_write'; stream.write(raw)
                    operation = 'status_flush'; stream.flush()
                    operation = 'status_fsync'; os.fsync(stream.fileno())
                operation = 'status_replace'; os.replace(tmp, path)
                return dict(status='published', attempts=attempt + 1, started_epoch=started, completed_epoch=sample_clock(),
                            payload_sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw),
                            recovered_failures=failures)
            except OSError as exc:
                failure = sanitized_diagnostic(exc, operation=operation)
                failure.update(attempt=attempt + 1, observed_epoch=sample_clock())
                failures.append(failure)
                retryable = isinstance(exc, PermissionError) or exc.errno in {errno.EACCES, errno.EPERM} or getattr(exc, 'winerror', None) in {5, 32, 33}
                if not retryable or attempt == len(RETRY_DELAYS):
                    raise StatusPublicationError(dict(status='unavailable', attempts=attempt + 1,
                        payload_sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw),
                        failures=failures, started_epoch=started, completed_epoch=sample_clock(),
                        temp_scope='one_per_pid_overwritten_on_next_publication')) from exc
                sleeper(RETRY_DELAYS[attempt])


def accounting_snapshot(rows, now, *, utc_day):
    """Read-only derived counters. Daily means claim day, not fill/close day.

    Definite no-POST and fill classifications use original broker result states.
    Missing transport evidence stays unknown; a claim alone is not a POST.
    """
    def counts(selected):
        claimed = [r for r in selected if r['attempted'] == 1]
        no_post = [r for r in selected if r['status'] == 'not_submitted']
        claimed_no_post = sum(r['attempted'] == 1 for r in no_post)
        transport_evidence = 0
        for row in claimed:
            result = row.get('result') or {}
            receipt = result.get('submission_receipt')
            reconciled = result.get('order_id') is not None and row['status'] in {'not_filled', 'filled_open', 'filled_closed'}
            if receipt is not None or reconciled:
                transport_evidence += 1
        return dict(prepared_records=len(selected), durable_claims=len(claimed),
                    definitely_not_submitted_records=len(no_post), claimed_definitely_not_submitted=claimed_no_post,
                    transport_evidenced_claims=transport_evidence,
                    possibly_transmitted_or_confirmed_claims=len(claimed) - claimed_no_post,
                    filled_records=sum(r['status'] in {'filled_open', 'filled_closed'} for r in selected),
                    unresolved_records=sum(r['status'] not in {'not_filled', 'not_submitted', 'filled_closed'} for r in selected))
    day = utc_day(now)
    today = [r for r in rows if r['attempted'] == 1 and utc_day(r['attempt_epoch']) == day]
    return dict(schema_version=SCHEMA, day_utc=day, total=counts(rows), claim_day=counts(today),
                cap_semantics='all_durable_claims_unchanged', cap_value=8,
                daily_fill_scope='fills_of_intents_claimed_today_not_execution_calendar_day',
                evidence_scope='derived_original_retained_states_and_receipts_no_historical_rewrite')
