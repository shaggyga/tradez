"""Bounded, read-only observation of exact registered joint-v3 envelopes.

This is transport evidence, not a forecast/ledger validator. A coherent envelope
never authorizes a position, establishes accuracy or declares its rows usable.
The producer's files, clocks and seals are never changed. Pending sealed bytes
may be selected only after an actually observed current heartbeat names them.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time

SCHEMA = 'joint_v3_envelope_observation_v1_20260909'
REGISTRY_ID = 'joint_price_news_study_v3_20260908'
SUMMARY_SCHEMA = 'joint_price_news_forecast_summary_v3_20260908'
HEARTBEAT_SCHEMA = 'joint_price_news_forecast_heartbeat_v3_20260908'
AUTHORITY = {'research_only': True, 'can_place_orders': False,
             'can_promote': False, 'can_authorize': False,
             'account_eligible': False, 'proof_eligible': False,
             'historical_rows_imported': False}
MAX_SUMMARY_BYTES = 1024 * 1024
MAX_HEARTBEAT_BYTES = 65536
MAX_AGE_SEC = 90
MAX_KEYS = 4
MAX_GENERATIONS = 2
SOURCE_NAMES = frozenset({
    'oanda_joint_price_news_forecast_study_v3.py',
    'oanda_causal_forecast_ledger_joint_news_v2.py',
    'oanda_fixed_forecast_evaluation_joint_news_v1.py',
    'oanda_joint_price_news_models_v1.py',
    'oanda_causal_forecast_inputs_joint_news_v2.py',
    'oanda_causal_forecast_inputs.py', 'oanda_causal_forecast_inputs_gap_v2.py',
    'oanda_exact_price_scoring.py', 'oanda_causal_prediction_baselines.py',
    'oanda_pair_local_models_v2.py', 'oanda_causal_forecast_inputs_pair_v2.py',
    'oanda_news_causal_aggregation_guard_v1.py',
    'oanda_news_classification_contract.py', 'oanda_local_news_sentiment.py',
    'oanda_source_governance.py', 'oanda_source_governance_news_fast_lane.py',
    'oanda_local_news_sentiment_repair_v1.py',
    'oanda_news_topic_identity_reconciliation_v1.py',
    'oanda_news_event_tagger.py', 'oanda_news_collector_contract.py',
})


class ObservationError(ValueError):
    pass


def _require(condition, reason):
    if not condition:
        raise ObservationError(reason)


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'),
                          allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, RecursionError):
        raise ObservationError('invalid_json_value') from None


def _decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            _require(key not in result, 'duplicate_json_key')
            result[key] = value
        return result
    try:
        value = json.loads(raw, object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(
                               ObservationError('nonfinite_json')))
        _require(type(value) is dict, 'json_object_required')
        _canonical(value)
        return value
    except (UnicodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        if isinstance(exc, ObservationError):
            raise
        raise ObservationError('invalid_json') from None


def _epoch(value):
    _require(type(value) in (int, float) and math.isfinite(value) and value > 0,
             'invalid_clock')
    return value


def _inert(value):
    return all(value.get(key) is expected for key, expected in AUTHORITY.items())


def _hash(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def _safe_path(path, *, file=True):
    _require(path.is_absolute(), 'absolute_path_required')
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current /= part
        info = current.lstat()
        _require(not stat.S_ISLNK(info.st_mode) and not (
            getattr(info, 'st_file_attributes', 0) &
            getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 1024)), 'reparse_path')
        final = current == path
        _require(stat.S_ISREG(info.st_mode) if final and file else
                 stat.S_ISDIR(info.st_mode), 'path_type')


def _identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)


def _ordered_reads(receipts, earliest, latest):
    previous = earliest
    for receipt in receipts:
        begun, completed = (_epoch(receipt[k]) for k in
                            ('read_started_epoch', 'read_completed_epoch'))
        _require(previous <= begun <= completed <= latest,
                 'consumer_clock_regression')
        previous = completed


def read_stable(path, limit, *, clock=time.time):
    """Actual read clocks and file identity accompany retained exact bytes."""
    _require(type(limit) is int and 0 < limit <= MAX_SUMMARY_BYTES, 'read_limit')
    path = Path(path)
    started = _epoch(clock())
    try:
        _safe_path(path)
        before = path.lstat()
        _require(0 < before.st_size <= limit, 'file_byte_limit')
        with path.open('rb') as stream:
            opened = os.fstat(stream.fileno())
            raw = stream.read(limit + 1)
            closed = os.fstat(stream.fileno())
        after = path.lstat()
        _safe_path(path)
    except OSError:
        raise ObservationError('file_read_unavailable') from None
    _require(len(raw) <= limit, 'file_byte_limit')
    _require(len({_identity(v) for v in (before, opened, closed, after)}) == 1,
             'file_replaced_during_read')
    completed = _epoch(clock())
    _require(completed >= started, 'consumer_clock_regression')
    return raw, {'read_started_epoch': started, 'read_completed_epoch': completed,
                 'bytes_sha256': _sha(raw), 'byte_count': len(raw),
                 'file_identity': list(_identity(after))}


@dataclass(frozen=True)
class _Generation:
    raw: bytes
    canonical_sha256: str
    generated_epoch: float
    first_observed_epoch: float


class JointV3EnvelopeObserver:
    """Four isolated roots/registries, two immutable generations per key.

    Cache admission validates the envelope and pair inventory only. Full family,
    numerical, ledger/publication/consumption and target checks remain mandatory
    downstream. The result always says rows_validated=False.
    """

    def __init__(self, expected_registry_sha256):
        _require(_hash(expected_registry_sha256), 'expected_registry_hash')
        self.expected_registry_sha256 = expected_registry_sha256
        self._cache = OrderedDict()
        self._lock = threading.Lock()

    def _admit(self, key, generation):
        with self._lock:
            previous = self._cache.get(key, ())
            same_epoch = [g for g in previous if
                          g.generated_epoch == generation.generated_epoch]
            _require(not same_epoch or all(g.canonical_sha256 ==
                      generation.canonical_sha256 for g in same_epoch),
                     'generation_clock_conflict')
            if not any(g.canonical_sha256 == generation.canonical_sha256
                       for g in previous):
                previous = tuple(sorted((*previous, generation), key=lambda g:
                    g.generated_epoch, reverse=True)[:MAX_GENERATIONS])
            self._cache[key] = previous
            self._cache.move_to_end(key)
            while len(self._cache) > MAX_KEYS:
                self._cache.popitem(last=False)

    def _select(self, key, digest):
        with self._lock:
            return next((g for g in self._cache.get(key, ())
                         if g.canonical_sha256 == digest), None)

    def _registry(self, root, clock):
        raw, receipt = read_stable(root / 'config' / (REGISTRY_ID + '.json'),
                                  MAX_SUMMARY_BYTES, clock=clock)
        registry = _decode(raw)
        _require(_sha(_canonical(registry)) == self.expected_registry_sha256,
                 'registry_identity_changed')
        _require(registry.get('registry_id') == REGISTRY_ID and
                 registry.get('schema_version') == 'joint_price_news_registry_v3_20260908'
                 and registry.get('collection_enabled') is True and _inert(registry),
                 'registry_schema_or_authority')
        bindings = registry.get('source_bindings')
        _require(type(bindings) is dict and set(bindings) == SOURCE_NAMES and
                 all(_hash(v) for v in bindings.values()), 'source_inventory')
        pairs = registry.get('pairs')
        _require(type(pairs) is dict and 1 <= len(pairs) <= 68 and
                 all(type(p) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', p)
                     for p in pairs), 'registry_pair_inventory')
        sources = {}
        for name in sorted(bindings):
            source, source_receipt = read_stable(root / name, MAX_SUMMARY_BYTES,
                                                clock=clock)
            _require(_sha(source) == bindings[name], 'registered_source_changed')
            sources[name] = source_receipt
        return registry, receipt, sources

    def _summary(self, raw, registry, now):
        value = _decode(raw)
        _require(value.get('schema_version') == SUMMARY_SCHEMA and
                 value.get('registry_sha256') == self.expected_registry_sha256 and
                 _inert(value), 'summary_schema_identity_or_authority')
        payload = {k: v for k, v in value.items() if k != 'payload_sha256'}
        _require(value.get('payload_sha256') == _sha(_canonical(payload)),
                 'summary_payload_seal')
        generated = _epoch(value.get('generated_epoch'))
        _require(0 <= now - generated <= MAX_AGE_SEC, 'summary_stale_or_future')
        rows = value.get('rows')
        _require(type(rows) is list and len(rows) == len(registry['pairs']) and
                 all(type(row) is dict and type(row.get('instrument')) is str
                     for row in rows) and
                 {r['instrument'] for r in rows} == set(registry['pairs']),
                 'summary_pair_inventory')
        return value, _sha(_canonical(value)), generated

    def _heartbeat(self, raw, now):
        value = _decode(raw)
        _require(value.get('schema_version') == HEARTBEAT_SCHEMA and
                 value.get('registry_sha256') == self.expected_registry_sha256 and
                 _inert(value) and _hash(value.get('summary_sha256')),
                 'heartbeat_schema_identity_or_authority')
        generated = _epoch(value.get('generated_epoch'))
        _require(0 <= now - generated <= MAX_AGE_SEC, 'heartbeat_stale_or_future')
        return value, generated

    def observe(self, project_root, *, clock=time.time, monotonic_clock=time.monotonic,
                sleep=time.sleep, max_wait_sec=0):
        """Observe with a retry/sleep budget, not a hard filesystem-I/O timeout.

        Reads and hashing are synchronous and byte-bounded. A stalled filesystem
        can exceed max_wait_sec; no later retry begins after its measured deadline.
        """
        _require(type(max_wait_sec) in (int, float) and
                 math.isfinite(max_wait_sec) and 0 <= max_wait_sec <= 6,
                 'wait_budget')
        root = Path(project_root)
        _require(root.is_absolute(), 'absolute_path_required')
        root = Path(os.path.abspath(root))
        key = (os.path.normcase(str(root)), self.expected_registry_sha256)
        started = _epoch(clock())
        mono_started = monotonic_clock()
        _require(type(mono_started) in (int, float) and math.isfinite(mono_started),
                 'invalid_monotonic_clock')
        deadline = mono_started + max_wait_sec
        result = {'schema_version': SCHEMA, **AUTHORITY, 'rows_validated': False,
                  'scope': 'Exact envelope transport only; no forecast, ledger, target, accuracy or trading acceptance.',
                  'registry_sha256': self.expected_registry_sha256,
                  'started_epoch': started, 'observed_epoch': started,
                  'status': 'unavailable', 'reason': None, 'attempts': [],
                  'selected': None, 'retained_bytes': []}
        previous_now, previous_mono = started, mono_started
        for index in range(13):
            if index:
                retry_now = monotonic_clock()
                if (type(retry_now) not in (int, float) or not math.isfinite(retry_now)
                        or retry_now < previous_mono):
                    result['reason'] = 'monotonic_clock_regression'
                    return result
                previous_mono = retry_now
                if retry_now >= deadline:
                    return result
            attempt = {'index': index, 'read_records': {}}
            try:
                registry, registry_receipt, sources = self._registry(root, clock)
                attempt['registry_read'] = registry_receipt
                attempt['source_reads'] = sources
                study = root / 'data/oanda_training_manager/joint_price_news_study_v3'
                summary_raw, summary_receipt = read_stable(study / 'summary.json',
                    MAX_SUMMARY_BYTES, clock=clock)
                attempt['read_records']['summary'] = summary_receipt
                result['retained_bytes'].append({'attempt': index, 'kind': 'summary',
                                                  'raw': summary_raw})
                heartbeat_raw, heartbeat_receipt = read_stable(study / 'heartbeat.json',
                    MAX_HEARTBEAT_BYTES, clock=clock)
                attempt['read_records']['heartbeat'] = heartbeat_receipt
                result['retained_bytes'].append({'attempt': index, 'kind': 'heartbeat',
                                                  'raw': heartbeat_raw})
                now = _epoch(clock())
                all_receipts = [registry_receipt, *sources.values(), summary_receipt,
                                heartbeat_receipt]
                _require(now >= previous_now and now >= max(
                    r['read_completed_epoch'] for r in all_receipts),
                    'consumer_clock_regression')
                _ordered_reads(all_receipts, previous_now, now)
                previous_now = now
                # Invalid current bytes are never hidden by an older cached pair.
                # Check future clocks against each actual byte-read completion;
                # a later heartbeat read or source check cannot age them into validity.
                self._summary(summary_raw, registry,
                              summary_receipt['read_completed_epoch'])
                self._heartbeat(heartbeat_raw,
                                heartbeat_receipt['read_completed_epoch'])
                summary, digest, generated = self._summary(summary_raw, registry, now)
                heartbeat, hb_generated = self._heartbeat(heartbeat_raw, now)
                # Bracket the envelope observation with the same exact registry and
                # all 20 source identities. This is an observation, not an OS-wide
                # transaction against a privileged concurrent file replacement.
                _, registry_after, sources_after = self._registry(root, clock)
                attempt['registry_read_after'] = registry_after
                attempt['source_reads_after'] = sources_after
                now = _epoch(clock())
                _ordered_reads([registry_after, *sources_after.values()], previous_now, now)
                previous_now = now
                self._summary(summary_raw, registry, now)
                self._heartbeat(heartbeat_raw, now)
                self._admit(key, _Generation(summary_raw, digest, generated,
                                             summary_receipt['read_completed_epoch']))
                selected = self._select(key, heartbeat['summary_sha256'])
                _require(selected is not None, 'heartbeat_generation_not_observed')
                # Another observer may have admitted bytes while this call waited
                # for the lock. Never claim to consume them at an earlier cutoff.
                _require(selected.first_observed_epoch <= now,
                         'selected_generation_observed_after_cutoff')
                self._summary(selected.raw, registry, now)
                _require(selected.generated_epoch <= hb_generated <= now,
                         'paired_generation_clock_order')
                result['selected'] = {
                    'canonical_summary_sha256': selected.canonical_sha256,
                    'summary_bytes_sha256': _sha(selected.raw),
                    'summary_generated_epoch': selected.generated_epoch,
                    'summary_first_observed_epoch': selected.first_observed_epoch,
                    'heartbeat_generated_epoch': hb_generated,
                    'heartbeat_bytes_sha256': _sha(heartbeat_raw),
                    'summary_age_sec': now - selected.generated_epoch,
                    'used_retained_generation': digest != selected.canonical_sha256,
                    'current_summary_sha256': digest,
                }
                result['retained_bytes'].append({'attempt': index,
                    'kind': 'selected_summary', 'raw': selected.raw})
                result.update(status='coherent_envelope_observed', reason=None,
                              observed_epoch=now)
                attempt.update(status='coherent_envelope_observed', observed_epoch=now)
                result['attempts'].append(attempt)
                return result
            except ObservationError as exc:
                now = _epoch(clock())
                reason = str(exc)
                if now < previous_now:
                    reason = 'consumer_clock_regression'
                previous_now = max(previous_now, now)
                result.update(reason=reason, observed_epoch=now)
                attempt.update(status='unavailable', reason=reason, observed_epoch=now)
                result['attempts'].append(attempt)
                if reason not in {'file_replaced_during_read',
                                  'heartbeat_generation_not_observed'}:
                    return result
            mono_now = monotonic_clock()
            if (type(mono_now) not in (int, float) or not math.isfinite(mono_now)
                    or mono_now < previous_mono):
                result['reason'] = 'monotonic_clock_regression'
                return result
            previous_mono = mono_now
            if mono_now >= deadline or index == 12:
                return result
            sleep(min(.5, deadline - mono_now))
        return result
