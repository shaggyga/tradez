"""Read-only API projection of a separately pinned joint-v3 ledger observation.

No producer envelope is replaced. Cached reports retain original observation
clocks; response-time target eligibility is a separate, explicitly dated view.
There is no import-time file observation beyond the source identity snapshot,
no worker, background thread, file publication, fitting or broker operation.
"""
import hashlib
import json
from pathlib import Path
import re
import threading
import time

import oanda_joint_v3_ledger_status_observer_v2 as observer

SCHEMA = 'joint_v3_ledger_observer_api_v2_20260909'
SPEC_SCHEMA = 'joint_v3_ledger_observer_api_specification_v2_20260909'
ROOT = Path(__file__).resolve().parent
OWN_NAME = Path(__file__).name
SOURCE_NAMES = frozenset((*observer.OWN_FILES, OWN_NAME))
_IMPORTED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
AUTHORITY = dict(observer.AUTHORITY)
MAX_REPORT_BYTES = 2 * 1024 * 1024
REFRESH_SEC = 15.0
MAX_AGE_SEC = 90.0
SPEC_KEYS = frozenset({
    'schema_version', 'registry_path', 'registry_sha256', 'study_root',
    'activation_sha256', 'primary_selection_path', 'primary_selection_sha256',
    'source_bindings', 'refresh_sec', 'max_age_sec', 'ledger_budget_sec', *AUTHORITY})


def require(ok, reason):
    observer.require(ok, reason)


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _hex(value):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None


def _sealed(value):
    return {**value, 'payload_sha256': observer.digest(value)}


class LedgerObservationAPI:
    """One explicitly configured origin and one bounded immutable report cache.

    The caller must pin the configuration independently. A current response
    means a recent verified ledger observation with an unelapsed original
    target; it does not assert the database has not changed since that read.
    Any integrity failure actually observed clears the cached report.
    """
    def __init__(self, specification, *, expected_specification_sha256,
                 clock=time.time, monotonic=time.monotonic):
        raw = observer.encoded(specification)
        require(len(raw) <= 16384 and _hex(expected_specification_sha256)
                and _sha(raw) == expected_specification_sha256, 'observer_api_specification_binding')
        spec = json.loads(raw)
        require(type(spec) is dict and set(spec) == SPEC_KEYS and spec['schema_version'] == SPEC_SCHEMA,
                'observer_api_specification_schema')
        require(all(spec.get(k) is v for k, v in AUTHORITY.items()), 'observer_api_authority')
        require(spec['registry_sha256'] == observer.ORIGIN_REGISTRY_SHA256,
                'observer_api_origin_registry')
        require(_hex(spec['activation_sha256']) and _hex(spec['primary_selection_sha256']),
                'observer_api_origin_hash')
        sources = spec['source_bindings']
        require(type(sources) is dict and set(sources) == SOURCE_NAMES
                and all(_hex(value) for value in sources.values()), 'observer_api_source_closure')
        require(sources[OWN_NAME] == _IMPORTED_SOURCE_SHA256
                and {k: sources[k] for k in observer.OWN_FILES} == observer.own_source_bindings(),
                'observer_api_loaded_sources')
        require(type(spec['refresh_sec']) in (int, float) and spec['refresh_sec'] == REFRESH_SEC
                and type(spec['max_age_sec']) in (int, float) and spec['max_age_sec'] == MAX_AGE_SEC
                and type(spec['ledger_budget_sec']) in (int, float) and spec['ledger_budget_sec'] == 8,
                'observer_api_fixed_bounds')
        for key in ('registry_path', 'study_root', 'primary_selection_path'):
            require(type(spec[key]) is str and len(spec[key]) <= 1024
                    and Path(spec[key]).is_absolute(), 'observer_api_absolute_path')
            require(str(observer.safe_path(spec[key], file=key != 'study_root')) == spec[key],
                    'observer_api_path_identity')
        self._spec_raw = raw
        self._spec_sha = expected_specification_sha256
        self._clock, self._monotonic = clock, monotonic
        self._lock = threading.Lock()
        self._cache = None
        self._cache_at = None
        self._in_flight = False
        self._integrity_generation = 0
        self._spec_file = None

    @classmethod
    def from_specification(cls, path, *, expected_file_sha256, **clocks):
        """Caller supplies the separately reviewed on-disk specification hash."""
        raw, receipt = observer.read_file(path, limit=16384, clock=clocks.get('clock', time.time))
        require(_hex(expected_file_sha256) and receipt['sha256'] == expected_file_sha256,
                'observer_api_specification_file_binding')
        value = json.loads(raw)
        instance = cls(value, expected_specification_sha256=observer.digest(value), **clocks)
        instance._spec_file = (Path(path).absolute(), expected_file_sha256)
        return instance

    def _specification(self):
        require(_sha(self._spec_raw) == self._spec_sha, 'observer_api_specification_changed')
        return json.loads(self._spec_raw)

    def _verify_inputs(self):
        """Every response verifies selection/activation and all 23 source files."""
        spec = self._specification()
        if self._spec_file is not None:
            path, expected = self._spec_file
            raw, receipt = observer.read_file(path, limit=16384, clock=self._clock)
            require(receipt['sha256'] == expected and observer.digest(json.loads(raw)) == self._spec_sha,
                    'observer_api_specification_file_changed')
        registry_raw, registry_read = observer.read_file(spec['registry_path'], clock=self._clock)
        require(registry_read['sha256'] == spec['registry_sha256'], 'observer_api_registry_changed')
        registry = json.loads(registry_raw)
        activation_raw, activation_read = observer.read_file(
            Path(spec['study_root']) / 'activation_receipt.json', clock=self._clock)
        require(activation_read['sha256'] == spec['activation_sha256'], 'observer_api_activation_changed')
        selection_raw, selection_read = observer.read_file(spec['primary_selection_path'], limit=16384, clock=self._clock)
        require(selection_read['sha256'] == spec['primary_selection_sha256'], 'observer_api_selection_changed')
        selection = json.loads(selection_raw)
        require(type(selection) is dict
                and selection.get('schema_version') == 'joint_forecast_primary_selection_v1_20260908'
                and selection.get('selected') == 'v3'
                and selection.get('registry_sha256') == observer.digest(registry)
                and selection.get('research_only') is True
                and all(selection.get(k) is False for k in ('can_place_orders', 'can_promote', 'can_authorize')),
                'observer_api_selection_identity')
        require(observer.number(selection.get('activated_epoch')) <= selection_read['read_completed_epoch'],
                'observer_api_selection_future')
        own = {key: spec['source_bindings'][key] for key in observer.OWN_FILES}
        _, source_reads = observer._closure(registry, own, self._clock)
        _, api_read = observer.read_file(ROOT / OWN_NAME, clock=self._clock)
        require(api_read['sha256'] == spec['source_bindings'][OWN_NAME] == _IMPORTED_SOURCE_SHA256,
                'observer_api_source_changed')
        completion = observer.number(self._clock())
        require(completion >= max(registry_read['read_completed_epoch'], activation_read['read_completed_epoch'],
                                  selection_read['read_completed_epoch'], api_read['read_completed_epoch'],
                                  *(r['read_completed_epoch'] for r in source_reads.values())),
                'observer_api_clock_regression')
        return spec, completion, registry

    def _validate_report(self, raw, spec, registry, now):
        require(type(raw) is bytes and len(raw) <= MAX_REPORT_BYTES, 'observer_api_report_byte_bound')
        report = json.loads(raw)
        require(type(report) is dict and report.get('schema_version') == observer.SCHEMA
                and observer.encoded(report) == raw, 'observer_api_report_schema')
        require(all(report.get(k) is v for k, v in AUTHORITY.items())
                and report.get('old_heartbeat_validated_for_forecasts') is False
                and report.get('current_input_readiness_observed') is False,
                'observer_api_report_authority_or_scope')
        require(report.get('payload_sha256') == observer.digest({k: v for k, v in report.items() if k != 'payload_sha256'}),
                'observer_api_report_seal')
        bound = report['observer_spec']
        require(observer.digest(bound) == report['observer_spec_sha256']
                and bound['origin_registry_sha256'] == spec['registry_sha256']
                and bound['origin_activation_sha256'] == spec['activation_sha256']
                and bound['origin_study_root'] == spec['study_root']
                and bound['original_source_bindings'] == registry['source_bindings']
                and bound['observer_source_bindings'] == {k: spec['source_bindings'][k] for k in observer.OWN_FILES},
                'observer_api_report_origin')
        frozen = observer.boundary.freeze_summary(report['summary'], expected_schema=observer.SUMMARY_SCHEMA,
                                                  expected_registry_sha256=report['observer_spec_sha256'])
        require(frozen.summary_sha256 == report['summary_sha256'], 'observer_api_summary_binding')
        completed = observer.number(report['completed_epoch'])
        require(observer.number(report['started_epoch']) <= completed <= now, 'observer_api_observation_future')
        require(frozen.generated_epoch <= completed, 'observer_api_summary_future')
        require(now - completed <= MAX_AGE_SEC, 'observer_api_observation_stale')
        require(report['registered_pairs'] == len(report['summary']['rows']) == 68, 'observer_api_pair_inventory')
        return report

    def _invalidate(self):
        with self._lock:
            self._cache = None
            self._cache_at = None
            self._integrity_generation += 1

    def _unavailable(self, reason, *, in_progress=False):
        return _sealed(dict(schema_version=SCHEMA, status='unavailable', reason=reason,
            observer_api_specification_sha256=self._spec_sha, observer_report=None,
            consumer=None, refresh_in_progress=in_progress, current_inputs_observed=False,
            old_heartbeat_validated_for_forecasts=False, **AUTHORITY))

    def get(self, *, compact=False):
        """Observe on demand; source failures never become a price-only fallback."""
        refreshing = False
        try:
            require(type(compact) is bool, 'observer_api_compact_boolean')
            spec, checked, registry = self._verify_inputs()
            tick = self._monotonic()
            require(type(tick) in (int, float) and 0 <= tick < 10**12, 'observer_api_monotonic_clock')
            with self._lock:
                raw, cached_at = self._cache, self._cache_at
                generation = self._integrity_generation
                in_progress = self._in_flight
                if raw is None or cached_at is None or tick - cached_at >= REFRESH_SEC:
                    if not in_progress:
                        self._in_flight = True
                        refreshing = True
                elif tick < cached_at:
                    raise observer.ObserverError('observer_api_monotonic_regression')
            if refreshing:
                result = observer.observe_joint_v3(spec['registry_path'], spec['study_root'],
                    expected_observer_source_bindings={k: spec['source_bindings'][k] for k in observer.OWN_FILES},
                    expected_activation_sha256=spec['activation_sha256'], clock=self._clock,
                    monotonic=self._monotonic, time_budget_sec=spec['ledger_budget_sec'])
                raw = observer.encoded(result)
                spec, checked, registry = self._verify_inputs()
                self._validate_report(raw, spec, registry, checked)
                completion_tick = self._monotonic()
                require(type(completion_tick) in (int, float) and tick <= completion_tick < 10**12,
                        'observer_api_monotonic_regression')
                with self._lock:
                    require(generation == self._integrity_generation, 'observer_api_concurrent_integrity_failure')
                    self._cache, self._cache_at = raw, completion_tick
                in_progress = False
            elif raw is None:
                return self._unavailable('observation_in_progress', in_progress=True)
            report = self._validate_report(raw, spec, registry, checked)
            # Check again after parsing; the actual consumer time never replaces
            # any retained ledger or producer observation/target clock.
            spec, checked, registry = self._verify_inputs()
            now = observer.number(self._clock())
            require(now >= checked, 'observer_api_clock_regression')
            report = self._validate_report(raw, spec, registry, now)
            active = []
            for row in report['summary']['rows']:
                slot = row['families'].get(observer.FAMILY, {})
                forecast = slot.get('latest_forecast')
                if slot.get('status') == 'forecast' and forecast is not None and now < forecast['target_epoch']:
                    active.append({'instrument': row['instrument'], 'decision_id': forecast['decision_id'],
                                   'forecast_sha256': forecast['forecast_sha256'],
                                   'cohort_id': forecast['forecasts'][0]['cohort_id']})
            value = dict(schema_version=SCHEMA,
                status='current_ledger_observation' if report['verified_ledger_pairs'] == 68 else 'partial_ledger_observation',
                proof_basis='independent_readonly_original_committed_ledgers',
                observer_api_specification_sha256=self._spec_sha, observer_report_sha256=_sha(raw),
                observer_report=None if compact else report,
                consumer=dict(observed_epoch=now, observation_completed_epoch=report['completed_epoch'],
                    observation_age_sec=now-report['completed_epoch'], verified_ledger_pairs=report['verified_ledger_pairs'],
                    registered_pairs=68, unvisited_pairs=report['unvisited_pairs'],
                    current_forecast_pairs=len(active), active_forecast_ids=active,
                    scope='Original ledger proof as of its retained read; original target eligibility checked at this response.'),
                original_producer_envelope=report['original_producer_envelope'], failures=report['failures'],
                refresh_in_progress=in_progress, endpoint='/api/joint-v3-ledger-observation',
                current_inputs_observed=False, old_heartbeat_validated_for_forecasts=False, **AUTHORITY)
            with self._lock:
                require(generation == self._integrity_generation, 'observer_api_concurrent_integrity_failure')
                return _sealed(value)
        except (ValueError, TypeError, KeyError, OSError, OverflowError, RecursionError) as error:
            self._invalidate()
            return self._unavailable(observer.code(error))
        finally:
            if refreshing:
                with self._lock:
                    self._in_flight = False
