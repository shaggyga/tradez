"""Offline evaluator with bounded exact-byte memoization of pure validations.

The accepted v1 evaluator and scoring algorithms are reused unchanged in a
dedicated synchronous process. No whole score, current-time check, source read,
model inference, publication or consumption observation is cached. The cache
exists only for one evaluation and never writes into a runtime directory.
"""
from collections import Counter, OrderedDict
from contextlib import ExitStack, contextmanager
from functools import wraps
import argparse
import hashlib
import json
from pathlib import Path
import sys
import threading
import time
from unittest.mock import patch

BASE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(BASE.parent))
import evaluate_prospective_pilot as original

SCHEMA = 'recovered_curve_pilot_offline_evaluation_v2_20260909'
MAX_CACHE_BYTES = 64 * 1024 * 1024
MAX_CACHE_ENTRIES = 4096
EXPECTED_SOURCES = {
    'oanda_forecast_curve_contract_v1.py': 'c03549586f922f6031790fce68a5afb5b98e4eda6157f20c4720b1adb05d0746',
    'oanda_native_curve_outcomes_v1.py': 'f90b237543aff4d86f5d3c93aeec1f738743fd6c9ba75c60f96ecfc8b605b68b',
    'oanda_forecast_curve_file_store_v1.py': '0d537f59e8c90e7957b2d9853d6a5561486ec5004f3b1ee02661c335bdf5fc4d',
    'oanda_recovered_second_curve_v1.py': 'dbc2d606afdeb23840dd9038cd1200301c2211abb4228ef9bab193763323f41d',
    'oanda_s5_mba_research_capture_v1.py': '38fe20f4e55d970f074c71f7acee5f5adaebc9ac7766a532efcd782b34abbb45',
    'oanda_research_quote_receipt_v1.py': 'ba18acd7eeac0eaa7c5cd7f8d4505fa5494c4ad3398345f009d25670169a27ef',
    'oanda_curve_management_adapter_v1.py': '8fa6cccf1e2525d0186ab7abe0c68fccc4800db7b785a8b9f79f28c320ad1241',
    'oanda_recovered_curve_bridge_v1.py': '07499ba8c5112b167b171af38a4bf69d414f901640e0301afa5babb381597c13',
    'oanda_recovered_curve_pilot_v1.py': 'fe7fdcd1f3e3c9473d393bc044721bfb48b48bdca09ee18fa8de8b9b8e02200e',
    'evaluate_prospective_pilot.py': 'd045ca7f08c44723e772d5037a624a87f9e55fdb1ac90a58700bd0e97013e737',
}
_EVALUATION_LOCK = threading.RLock()


def require(ok, reason):
    if not ok:
        raise original.contract.CurveContractError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def verify_engine_sources():
    result = {}
    for name, expected in EXPECTED_SOURCES.items():
        path = BASE.parent / name if name == 'evaluate_prospective_pilot.py' else original.ROOT / name
        raw = original.files._read(path)
        require(digest(raw) == expected, 'optimized_evaluator_engine_source_changed')
        result[name] = expected
    return result


class ValidationMemo:
    """Successful pure calls only; both key and returned value are immutable bytes."""
    def __init__(self, *, maximum_bytes=MAX_CACHE_BYTES, maximum_entries=MAX_CACHE_ENTRIES):
        require(type(maximum_bytes) is int and 1 <= maximum_bytes <= MAX_CACHE_BYTES, 'validation_cache_byte_limit')
        require(type(maximum_entries) is int and 1 <= maximum_entries <= MAX_CACHE_ENTRIES, 'validation_cache_entry_limit')
        self.maximum_bytes = maximum_bytes
        self.maximum_entries = maximum_entries
        self._entries = OrderedDict()
        self._bytes = 0
        self._peak = 0
        self._counts = Counter()
        self._lock = threading.RLock()

    def wrap(self, name, function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            try:
                key_bytes = original.contract.canonical_bytes(dict(args=list(args), kwargs=kwargs))
            except (ValueError, TypeError, OverflowError, RecursionError):
                # Preserve the original failure/acceptance behavior for inputs
                # that cannot fit the optional cache's canonical envelope.
                with self._lock:
                    self._counts[name + ':uncacheable'] += 1
                return function(*args, **kwargs)
            key = (name, function, digest(key_bytes))
            with self._lock:
                entry = self._entries.get(key)
                if entry is not None and entry[0] == key_bytes:
                    self._entries.move_to_end(key)
                    self._counts[name + ':hit'] += 1
                    return json.loads(entry[1])
                self._counts[name + ':miss'] += 1
            # The call consumes exactly the retained argument snapshot, so a
            # caller cannot mutate an admitted key while the validator runs.
            captured = json.loads(key_bytes)
            result = function(*captured['args'], **captured['kwargs'])
            try:
                value_bytes = original.contract.canonical_bytes(result)
            except (ValueError, TypeError, OverflowError, RecursionError):
                return result
            cost = len(key_bytes) + len(value_bytes)
            if cost <= self.maximum_bytes:
                with self._lock:
                    previous = self._entries.pop(key, None)
                    if previous is not None:
                        self._bytes -= len(previous[0]) + len(previous[1])
                    while self._entries and (self._bytes + cost > self.maximum_bytes
                                            or len(self._entries) >= self.maximum_entries):
                        _, evicted = self._entries.popitem(last=False)
                        self._bytes -= len(evicted[0]) + len(evicted[1])
                        self._counts['evictions'] += 1
                    self._entries[key] = (key_bytes, value_bytes)
                    self._bytes += cost
                    self._peak = max(self._peak, self._bytes)
            return json.loads(value_bytes)
        return wrapped

    def summary(self):
        with self._lock:
            return dict(counts=dict(self._counts), retained_entries=len(self._entries),
                retained_bytes=self._bytes, peak_bytes=self._peak,
                maximum_bytes=self.maximum_bytes, maximum_entries=self.maximum_entries,
                cached_errors=False, persisted_cache=False, whole_scores_cached=False)


@contextmanager
def validation_session(*, maximum_bytes=MAX_CACHE_BYTES, maximum_entries=MAX_CACHE_ENTRIES):
    """Scoped aliases affect only this separate evaluator process, then restore."""
    with _EVALUATION_LOCK:
        before = verify_engine_sources()
        own_bytes = Path(__file__).read_bytes()
        memo = ValidationMemo(maximum_bytes=maximum_bytes, maximum_entries=maximum_entries)
        with ExitStack() as stack:
            for name in ('validate_policy', 'validate_prepared', 'validate_curve', 'validate_consumption'):
                function = getattr(original.contract, name)
                wrapper = memo.wrap('contract.' + name, function)
                stack.enter_context(patch.object(original.contract, name, wrapper))
                if name == 'validate_consumption':
                    stack.enter_context(patch.object(original.outcomes, name, wrapper))
                    adapter = sys.modules[original.candidate_for_target.__module__]
                    stack.enter_context(patch.object(adapter, name, wrapper))
            for name in ('validate_candle_capture', '_validate_entry'):
                stack.enter_context(patch.object(original.outcomes, name,
                    memo.wrap('outcomes.' + name, getattr(original.outcomes, name))))
            try:
                yield memo
            finally:
                # No result is accepted after changed engine/evaluator bytes.
                require(verify_engine_sources() == before, 'optimized_evaluator_source_closure_changed')
                require(Path(__file__).read_bytes() == own_bytes, 'optimized_evaluator_source_changed_during_run')


def evaluate(registry_path, expected_registry_sha256, *, clock=time.time):
    with validation_session() as memo:
        value = original.evaluate(registry_path, expected_registry_sha256, clock=clock)
        statistics = memo.summary()
    assessment = value['assessment']
    assessment['original_evaluation_schema'] = assessment['schema_version']
    assessment['schema_version'] = SCHEMA
    assessment['original_evaluator_source_sha256'] = assessment['evaluator_source_sha256']
    assessment['evaluator_source_sha256'] = digest(Path(__file__).read_bytes())
    assessment['validation_memo'] = statistics
    assessment['optimized_engine_sources'] = dict(EXPECTED_SOURCES)
    assessment['limits'].append('Only successful pure validations are reused for exactly identical retained argument bytes within this evaluation; fresh file reads, actual clocks, source election and all scoring formulas are unchanged.')
    return value


def save(path, value):
    raw = json.dumps(value, sort_keys=True, indent=2, allow_nan=False).encode() + b'\n'
    with path.open('xb') as stream:
        stream.write(raw)
        stream.flush()
        import os
        os.fsync(stream.fileno())
    require(path.read_bytes() == raw, 'optimized_evaluator_report_readback_failed')
    return dict(path=path.name, sha256=digest(raw), bytes=len(raw))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--registry-sha256', required=True)
    parser.add_argument('--output-directory', type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output_directory.absolute()
    require(output.parent == BASE and not output.exists(), 'new_direct_v2_evaluation_directory_required')
    original.files._safe_components(BASE)
    output.mkdir()
    try:
        result = evaluate(args.registry, args.registry_sha256)
        refs = [save(output / (name + '.json'), result[name]) for name in
                ('source_captures', 'variant_attempts', 'matched_comparisons', 'manifest')]
        (output / 'curves').mkdir()
        for index, curve in enumerate(result['curves']):
            ref = save(output / 'curves' / ('curve_' + str(index).zfill(5) + '.json'), curve)
            ref['path'] = 'curves/' + ref['path']
            refs.append(ref)
        receipt = save(output / 'PROSPECTIVE_PILOT_EVALUATION.json', {**result['assessment'], 'evidence_files': refs})
        print(json.dumps(dict(status='completed', assessment=str(output / receipt['path']),
            sha256=receipt['sha256'], curve_counts=result['assessment']['curve_chain_status_counts'],
            cache=result['assessment']['validation_memo'])))
        return 0
    except Exception as error:
        receipt = save(output / 'EVALUATION_FAILED.json', dict(status='failed', observed_epoch=time.time(),
            reason_code=original.reason(error), error_type=type(error).__name__,
            registry_sha256=args.registry_sha256, evaluator_source_sha256=digest(Path(__file__).read_bytes()),
            **original.contract.AUTHORITY))
        print(json.dumps(dict(status='failed', receipt=str(output / receipt['path']), sha256=receipt['sha256'])))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
