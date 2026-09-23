"""External, single-process evaluator: immutable validated captures, unchanged scores.

No forecast or whole score is cached. The original rolling-source election is
called unchanged. Only bank-owned immutable capture validations, their semantic
hash, and real-label lookup reuse prior work. Nonbank evidence takes the original
path. No live source, registry, runner or data file is changed.
"""
from bisect import bisect_left
from collections import Counter
from contextlib import ExitStack, contextmanager
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
from types import MappingProxyType
from unittest.mock import patch
import weakref

BASE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(BASE.parent / 'scalability_v2'))
import evaluate_prospective_pilot_v2 as v2

original = v2.original
SCHEMA = 'recovered_curve_pilot_offline_evaluation_v3_20260909'
EXPECTED_V2_SHA = '138ca479515b1b22e023757f937c5887e77bcba212b7a56d94d788eed679a46c'
MAX_BANK_BYTES = 256 * 1024 * 1024
MAX_BANK_CANONICAL_BYTES = 64 * 1024 * 1024
MAX_BANK_ENTRIES = 2048
_REFERENCE_VIEWS = weakref.WeakKeyDictionary()
_ORIGINAL_VALIDATE = original.outcomes.validate_candle_capture
_ORIGINAL_HASH = original.outcomes.content_hash
_ORIGINAL_SELECT = original.outcomes._select_target
_ORIGINAL_SOURCE = original._source_capture


def require(ok, reason):
    if not ok:
        raise original.contract.CurveContractError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class _CaptureReference:
    """Opaque, nonconstructible-by-claim reference. No parsed row escapes here."""
    __slots__ = ('__weakref__',)

    def __getitem__(self, key):
        view = _REFERENCE_VIEWS.get(self)
        require(view is not None, 'v3_unrecognized_or_expired_capture_reference')
        require(key in view, 'v3_capture_reference_field_not_exposed')
        return view[key]

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    return value


def _size(value, seen=None):
    """Account owned containers/values plus proxy backing dictionaries.

    This is an accounted object-size limit, not a hard process working-set cap.
    """
    if seen is None:
        seen = set()
    if id(value) in seen:
        return 0
    seen.add(id(value))
    total = sys.getsizeof(value)
    if isinstance(value, (dict, MappingProxyType)):
        if isinstance(value, MappingProxyType):
            total += sys.getsizeof(dict(value))
        total += sum(_size(k, seen) + _size(v, seen) for k, v in value.items())
    elif isinstance(value, (tuple, list)):
        total += sum(_size(v, seen) for v in value)
    return total


class CaptureBank:
    def __init__(self, *, maximum_bytes=MAX_BANK_BYTES,
                 maximum_canonical_bytes=MAX_BANK_CANONICAL_BYTES,
                 maximum_entries=MAX_BANK_ENTRIES):
        for value, limit in ((maximum_bytes, MAX_BANK_BYTES),
                             (maximum_canonical_bytes, MAX_BANK_CANONICAL_BYTES),
                             (maximum_entries, MAX_BANK_ENTRIES)):
            require(type(value) is int and 1 <= value <= limit, 'v3_bank_budget')
        self.maximum_bytes = maximum_bytes
        self.maximum_canonical_bytes = maximum_canonical_bytes
        self.maximum_entries = maximum_entries
        self._refs = {}
        self._objects = {}
        self._rows = {}
        self._canonical = {}
        self._bytes = 0
        self._canonical_bytes = 0
        self._counts = Counter()

    def admit(self, capture):
        # Validate the actual caller value before sealing a detached snapshot.
        # No caller-provided digest or mutable identity can authorize a hit.
        verified = _ORIGINAL_VALIDATE(capture)
        canonical = original.contract.canonical_bytes(verified)
        key = digest(canonical)
        prior = self._canonical.get(key)
        if prior is not None and prior['canonical'] == canonical:
            self._counts['identical_admission'] += 1
            return prior['reference']
        parsed = _freeze(json.loads(canonical))
        labels = tuple(row['bar_start_epoch'] for row in parsed['rows'])
        semantic = _ORIGINAL_HASH({k: v for k, v in verified.items()
                                  if k not in ('capture_sha256', 'first_observed_epoch')})
        scalars = MappingProxyType({key: parsed[key] for key in (
            'coverage_start_label_epoch', 'coverage_end_label_epoch',
            'read_completed_epoch', 'capture_sha256')})
        cost = _size((canonical, parsed, labels, semantic, scalars)) + 1024
        if (self._bytes + cost > self.maximum_bytes or
                self._canonical_bytes + len(canonical) > self.maximum_canonical_bytes or
                len(self._refs) >= self.maximum_entries):
            self._counts['admission_budget_fallback'] += 1
            return verified
        reference = _CaptureReference()
        entry = dict(canonical=canonical, parsed=parsed, labels=labels,
                     semantic=semantic, reference=reference, cost=cost)
        self._refs[reference] = entry
        self._objects[id(parsed)] = entry
        self._rows[id(parsed['rows'])] = entry
        self._canonical[key] = entry
        _REFERENCE_VIEWS[reference] = scalars
        self._bytes += cost
        self._canonical_bytes += len(canonical)
        self._counts['admitted'] += 1
        return reference

    def validate(self, capture):
        entry = self._refs.get(capture) if isinstance(capture, _CaptureReference) else None
        if entry is None:
            entry = self._objects.get(id(capture))
            if entry is not None and entry['parsed'] is not capture:
                entry = None
        if entry is not None:
            self._counts['validation_hit'] += 1
            return entry['parsed']
        self._counts['validation_original_fallback'] += 1
        return _ORIGINAL_VALIDATE(capture)

    def semantic_hash(self, value):
        # Exact immutable nested-object membership, not self-declared hashes.
        if isinstance(value, dict) and 'rows' in value:
            entry = self._rows.get(id(value['rows']))
            if entry is not None:
                capture = entry['parsed']
                keys = set(capture) - {'capture_sha256', 'first_observed_epoch'}
                if set(value) == keys and all(
                    value[k] is capture[k] if isinstance(capture[k], (MappingProxyType, tuple))
                    else type(value[k]) is type(capture[k]) and value[k] == capture[k]
                    for k in keys):
                    self._counts['semantic_hash_hit'] += 1
                    return entry['semantic']
        return _ORIGINAL_HASH(value)

    def select_target(self, capture, nominal_label, maximum_delay, now):
        entry = self._objects.get(id(capture))
        if entry is None or entry['parsed'] is not capture:
            return _ORIGINAL_SELECT(capture, nominal_label, maximum_delay, now)
        self._counts['indexed_target_lookup'] += 1
        # Preserve frozen branch ordering. Maturity is rechecked on every call;
        # no current-time or negative outcome decision is cached.
        if now < nominal_label + 5:
            return None, 'nominal_target_not_mature'
        if capture['coverage_start_label_epoch'] > nominal_label:
            return None, 'source_domain_starts_after_nominal_target'
        index = bisect_left(entry['labels'], nominal_label)
        if index < len(entry['labels']) and entry['labels'][index] <= nominal_label + maximum_delay:
            return capture['rows'][index], None
        if capture['coverage_end_label_epoch'] < nominal_label + maximum_delay:
            return None, 'target_window_not_covered_by_source'
        return None, 'provider_reported_no_complete_target_bar'

    def source_capture(self, *args, **kwargs):
        record, capture, mappings = _ORIGINAL_SOURCE(*args, **kwargs)
        return record, self.admit(capture) if capture is not None else None, mappings

    def summary(self):
        return dict(counts=dict(self._counts), retained_entries=len(self._refs),
                    accounted_bytes=self._bytes, retained_canonical_bytes=self._canonical_bytes,
                    maximum_bytes=self.maximum_bytes,
                    maximum_canonical_bytes=self.maximum_canonical_bytes,
                    maximum_entries=self.maximum_entries, budget_overflow='original_validation_path',
                    whole_scores_cached=False, source_election_unchanged=True,
                    dynamic_clock_checks_cached=False, parsed_values_recursively_read_only=True,
                    size_scope='Owned Python objects including proxy backing dictionaries plus per-entry overhead; not a hard process-memory cap.')

    def close(self):
        for ref in self._refs:
            _REFERENCE_VIEWS.pop(ref, None)
        self._refs.clear(); self._objects.clear(); self._rows.clear(); self._canonical.clear()


@contextmanager
def validation_session(**bank_limits):
    require(digest(Path(v2.__file__).read_bytes()) == EXPECTED_V2_SHA, 'v3_v2_source_changed')
    own = Path(__file__).read_bytes()
    with v2.validation_session() as memo:
        bank = CaptureBank(**bank_limits)
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(original, '_source_capture', bank.source_capture))
                stack.enter_context(patch.object(original.outcomes, 'validate_candle_capture', bank.validate))
                stack.enter_context(patch.object(original.outcomes, 'content_hash', bank.semantic_hash))
                stack.enter_context(patch.object(original.outcomes, '_select_target', bank.select_target))
                yield bank, memo
        finally:
            bank.close()
            require(Path(__file__).read_bytes() == own and
                    digest(Path(v2.__file__).read_bytes()) == EXPECTED_V2_SHA, 'v3_source_changed_during_run')


def evaluate(registry_path, expected_registry_sha256, *, clock=original.time.time):
    with validation_session() as (bank, memo):
        value = original.evaluate(registry_path, expected_registry_sha256, clock=clock)
        statistics, memo_statistics = bank.summary(), memo.summary()
    assessment = value['assessment']
    assessment['original_evaluation_schema'] = assessment['schema_version']
    assessment['schema_version'] = SCHEMA
    assessment['original_evaluator_source_sha256'] = assessment['evaluator_source_sha256']
    assessment['evaluator_source_sha256'] = digest(Path(__file__).read_bytes())
    assessment['validation_memo'] = memo_statistics
    assessment['capture_bank'] = statistics
    assessment['optimized_engine_sources'] = dict(v2.EXPECTED_SOURCES)
    assessment['v2_source_sha256'] = EXPECTED_V2_SHA
    assessment['limits'].append('Bank-owned captures retain exact canonical bytes and recursively immutable parsed values. Only their validated semantics and real-label index are reused; original election, all supplied-source checks, current clocks and numerical scoring remain unchanged. Overflow takes the original path.')
    return value


def save(path, value):
    return v2.save(path, value)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--registry', type=Path, required=True)
    parser.add_argument('--registry-sha256', required=True)
    parser.add_argument('--output-directory', type=Path, required=True)
    args = parser.parse_args(argv)
    output = args.output_directory.absolute()
    require(output.parent == BASE and not output.exists(), 'new_direct_v3_evaluation_directory_required')
    original.files._safe_components(BASE); output.mkdir()
    try:
        result = evaluate(args.registry, args.registry_sha256)
        refs = [save(output / (name + '.json'), result[name]) for name in
                ('source_captures', 'variant_attempts', 'matched_comparisons', 'manifest')]
        (output / 'curves').mkdir()
        for index, curve in enumerate(result['curves']):
            ref = save(output / 'curves' / ('curve_' + str(index).zfill(5) + '.json'), curve)
            ref['path'] = 'curves/' + ref['path']; refs.append(ref)
        receipt = save(output / 'PROSPECTIVE_PILOT_EVALUATION.json', {**result['assessment'], 'evidence_files': refs})
        print(json.dumps(dict(status='completed', assessment=str(output / receipt['path']),
                              sha256=receipt['sha256'], bank=result['assessment']['capture_bank'])))
        return 0
    except Exception as error:
        receipt = save(output / 'EVALUATION_FAILED.json', dict(status='failed',
            observed_epoch=original.time.time(), reason_code=original.reason(error),
            error_type=type(error).__name__, evaluator_source_sha256=digest(Path(__file__).read_bytes()),
            **original.contract.AUTHORITY))
        print(json.dumps(dict(status='failed', receipt=receipt)))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
