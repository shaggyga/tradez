"""Opaque compact historical news sharing; no price/model/current-news operation.

The fixed I/O API constructs one complete multi-pair historical batch. Its exact
original point contract validates each shared frame once, then only small
immutable headers and cells remain. Live use requires the original healthy
session/capture generation; these objects cannot authenticate price maturity.
"""
from pathlib import Path
import bisect
import hashlib
import math
import re
import weakref

import revision_joint_point_v4 as point_contract

news_io = point_contract.news_io
need = news_io.need
SCHEMA = 'joint_revision_shared_history_v5_20260916'
PROJECTION = 'joint_revision_shared_history_projection_v5_20260916'
MAX_PAIRS = 68
MAX_ORIGINS = 256
MAX_COMPACT_BYTES = 16 * 1024**2
MAX_PROJECTION_BYTES = 8 * 1024**2
MAX_POINT_BYTES = 8192
BOUND_IO = 'b7fd784ef6ca756201c1ccddd36961e608d669a2a7e700abf4561a0a0122e51c'
BOUND_ADAPTER = '077a32c270a9f776cfc7e2c7604af1bcd671e1a530e5b6638d46f1a9c2f52c7d'
BOUND_POINT = 'ef5429c731718fb9359e07aca91dd7d9066aa11dd23087297292bf760269c998'
INERT = {'research_only': True, 'can_place_orders': False, 'can_promote': False,
         'can_authorize': False, 'account_eligible': False, 'proof_eligible': False}


class HistoryShare:
    __slots__ = ('__weakref__',)
    def __new__(cls, *args, **kwargs): raise ValueError('history_share_factory_required')
    def __setattr__(self, name, value): raise AttributeError('opaque_history_share')


def _registry():
    values = weakref.WeakKeyDictionary()
    def make(value):
        result = object.__new__(HistoryShare); values[result] = value; return result
    def get(value):
        need(type(value) is HistoryShare and value in values, 'registered_history_share_required')
        return values[value]
    return make, get


_make, _get = _registry()


def _bindings():
    kit=news_io.path_for(Path(__file__).absolute().parent,directory=True)
    need(Path(point_contract.__file__).absolute()==kit/Path(point_contract.__file__).name,'fixed_history_sibling_point_required')
    bindings = point_contract.source_bindings()
    need(bindings.get(Path(news_io.__file__).name) == BOUND_IO and
         bindings.get(Path(news_io.adapter.__file__).name) == BOUND_ADAPTER and
         bindings.get(Path(point_contract.__file__).name) == BOUND_POINT, 'fixed_history_share_sources_required')
    _, own = news_io.read_exact(__file__, 1024**2)
    return tuple(sorted({**bindings, Path(__file__).name: own['sha256']}.items()))


def _pair(value):
    need(type(value) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', value) and
         value[:3] != value[4:], 'explicit_distinct_history_pair_required')
    return value


def _origins(values):
    need(type(values) in (list, tuple) and len(values) <= MAX_ORIGINS, 'history_origin_count_bound')
    owned = tuple(values)
    need(all(type(value) is int and value > 0 and value % 900 == 0 for value in owned) and
         len(set(owned)) == len(owned), 'unique_original_history_starts_required')
    return owned


def _requests(value):
    need(type(value) is dict and 1 <= len(value) <= MAX_PAIRS, 'history_pair_count_bound')
    owned = tuple(sorted((_pair(pair), _origins(origins)) for pair, origins in value.items()))
    union = tuple(sorted({origin for _, origins in owned for origin in origins}))
    need(len(union) <= MAX_ORIGINS, 'history_origin_union_bound')
    return owned, union


def _cell(features, expires, status, available):
    need(status == ('available' if available else 'unavailable'), 'history_cell_coverage_status')
    if not available:
        need(features is None and expires is None, 'unknown_history_has_no_neutral_cell')
        return None, None, status
    need(type(features) is list and len(features) == 8 and
         all(type(value) in (int, float) and math.isfinite(value) for value in features),
         'original_eight_history_features_required')
    expiry = None if expires is None else news_io.adapter.epoch(expires)
    return tuple(features), expiry, status


def _small_point(header, cell):
    features, expiry, status = cell
    if status == 'not_requested': return None
    point = {**dict(header), 'features': None if features is None else list(features), 'expires_epoch': expiry}
    need(point['status'] == status, 'history_point_status_identity')
    news_io.encode(point, MAX_POINT_BYTES)
    return point


def _compact(batch, requests, origins, context_sha, read_completed):
    pairs = tuple(pair for pair, _ in requests)
    wanted = tuple(frozenset(values) for _, values in requests)
    need(type(batch) is dict and batch.get('schema_version') == 'joint_revision_news_origin_pair_batch_v1_20260913'
         and batch['context_sha256'] == context_sha and batch['instruments'] == list(pairs)
         and batch['origins'] == list(origins) and batch['feature_names'] == list(news_io.adapter.NEWS_FEATURES)
         and type(batch['records']) is list and len(batch['records']) == len(origins),
         'complete_exact_history_batch_required')
    rows = []; available = unavailable = unrequested = 0
    for origin, row in zip(origins, batch['records'], strict=True):
        need(type(row) is dict and type(row['origin']) is int and row['origin'] == origin and
             row['decision_epoch'] == origin + 60 and
             all(type(row[key]) is list and len(row[key]) == len(pairs) for key in ('statuses', 'features', 'expires_epoch')),
             'complete_exact_history_row_required')
        first = next(index for index, values in enumerate(wanted) if origin in values)
        point = point_contract._point({**row['shared_frame'], 'features': row['features'][first],
                              'expires_epoch': row['expires_epoch'][first]}, origin + 60, context_sha)
        observed = point['consumer_observed_epoch']
        need(observed is None or news_io.adapter.epoch(observed) <= read_completed,
             'original_history_observation_after_capture_read')
        header = tuple(sorted((key, value) for key, value in point.items() if key not in ('features', 'expires_epoch')))
        need(all(value is None or type(value) in (str, int, float, bool) for _, value in header),
             'fixed_scalar_history_header_required')
        cells = []
        for index, values in enumerate(wanted):
            status = row['statuses'][index]
            if origin not in values:
                need(status == 'not_requested' and row['features'][index] is None and row['expires_epoch'][index] is None,
                     'unrequested_history_has_no_point')
                cell = (None, None, status); unrequested += 1
            else:
                cell = _cell(row['features'][index], row['expires_epoch'][index], status, point['coverage_usable'])
                if status == 'available': available += 1
                else: unavailable += 1
            cells.append(cell)
        rows.append((origin, header, tuple(cells)))
    expected = {'requested_cell_count': sum(len(values) for values in wanted), 'available_cell_count': available,
                'unavailable_cell_count': unavailable, 'not_requested_cell_count': unrequested,
                'distinct_origin_count': len(origins)}
    need(all(type(batch[key]) is int and batch[key] == value for key, value in expected.items()) and
         available + unavailable == expected['requested_cell_count'] and
         available + unavailable + unrequested == len(pairs) * len(origins), 'exact_complete_history_accounting')
    return tuple(rows), tuple(sorted(expected.items()))


def prepare_history_share(session, news_capture, requests):
    """One fixed batch call; caller retains the returned handle for reuse."""
    owned, origins = _requests(requests)
    state = news_io._session(session)
    with state['lock']:
        _, parts = news_io._eligible(session, news_capture)
        serial = state['failure_serial']; metadata = news_io.capture_metadata(news_capture)
    body = metadata['capture']; context_sha = body['readback_sha256']
    first = news_io.adapter.epoch(body['first_observed_epoch'])
    read_completed = news_io.adapter.epoch(body['read_completed_epoch'])
    need(read_completed <= first and all(origin + 60 <= first for origin in origins), 'historical_origins_before_capture_required')
    bindings = _bindings()
    batch = news_io.historical_features(news_capture, {pair: list(values) for pair, values in owned})
    rows, counts = _compact(batch, owned, origins, context_sha, read_completed)
    del batch
    pairs = tuple(pair for pair, _ in owned)
    binding_value = {'schema_version': SCHEMA, 'source_bindings': dict(bindings),
        'capture_descriptor': metadata['descriptor'], 'context_sha256': context_sha,
        'requests': owned, 'rows': rows, 'counts': dict(counts)}
    compact = news_io.encode(binding_value, MAX_COMPACT_BYTES)
    compact_sha = hashlib.sha256(compact).hexdigest(); compact_bytes = len(compact)
    del compact, binding_value
    need(_bindings() == bindings, 'history_sources_changed_during_preparation')
    with state['lock']:
        _, current = news_io._eligible(session, news_capture)
        need(state['failure_serial'] == serial and current is parts, 'history_capture_changed_during_preparation')
        return _make((session, news_capture, serial, pairs, origins, owned, rows, counts, bindings,
                      tuple(sorted(metadata['descriptor'].items())), context_sha, compact_sha, compact_bytes))


def _live(shared, session, news_capture):
    value = _get(shared)
    need(value[0] is session and value[1] is news_capture, 'same_history_session_and_capture_required')
    state, _ = news_io._eligible(session, news_capture)
    need(state['failure_serial'] == value[2], 'history_failure_generation_required')
    return value


def project_pair_history(shared, *, session, news_capture, instrument, origins):
    """Project small points only; no source/DB/history read or freshness renewal."""
    pair = _pair(instrument); wanted = _origins(origins)
    state = news_io._session(session)
    with state['lock']:
        value = _live(shared, session, news_capture)
        pairs, union, rows = value[3], value[4], value[6]
        need(pair in pairs and all(origin in union for origin in wanted), 'history_projection_outside_prepared_union')
        index = pairs.index(pair); records = []
        for origin in wanted:
            row = rows[bisect.bisect_left(union, origin)]
            cell = row[2][index]
            records.append({'origin': origin, 'status': cell[2], 'point': _small_point(row[1], cell)})
        result = {'schema_version': PROJECTION, 'instrument': pair, 'origins': list(wanted),
            'records': records, 'history_share_sha256': value[11], 'context_sha256': value[10],
            'capture_sha256': dict(value[9])['capture_sha256'],
            'source_generation_sha256': hashlib.sha256(news_io.encode(dict(value[8]), 65536)).hexdigest(),
            'price_or_label_maturity_evaluated': False, 'fresh_health_proven': False,
            'current_decision_features_evaluated': False, 'eligibility_scope': 'same_capture_session_generation_at_this_call', **INERT}
        news_io.encode(result, MAX_PROJECTION_BYTES)
        _live(shared, session, news_capture)
        return result


def history_share_metadata(shared):
    """Fresh descriptive metadata, not authorization or current health evidence."""
    value = _get(shared)
    return {'schema_version': SCHEMA, 'source_bindings': dict(value[8]), 'capture_descriptor': dict(value[9]),
            'context_sha256': value[10], 'instruments': list(value[3]), 'origins': list(value[4]),
            **dict(value[7]), 'compact_sha256': value[11], 'compact_bytes': value[12],
            'compact_byte_limit': MAX_COMPACT_BYTES, 'price_or_label_maturity_evaluated': False,
            'fresh_health_proven': False, 'factory_batch_calls': 1, 'global_memoization_provided': False, **INERT}


def universal_origins(first_observed_epoch):
    """Exact retained-clock inequalities, not a rounded current decision."""
    first=news_io.adapter.epoch(first_observed_epoch)
    lower=first-48*3600
    # Widen by one grid cell, then apply the exact declared inequalities.
    lo=(int(lower-60)//900-1)*900;hi=(int(first-60)//900+2)*900
    candidates=range(lo,hi+1,900)
    need(len(candidates)<=197,'bounded_universal_origin_candidates')
    origins=tuple(t for t in candidates if t>0 and lower<=t+60<=first)
    need(len(origins)<=193,'universal48h_origin_bound')
    return origins


def prepare_universal_history_share(session,news_capture,instruments):
    """One historical batch for all registered pairs; no price rows are read."""
    need(type(instruments) in (list,tuple) and 1<=len(instruments)<=MAX_PAIRS,'bounded_registered_pairs_required')
    pairs=tuple(_pair(pair) for pair in instruments)
    need(len(set(pairs))==len(pairs),'unique_registered_pairs_required')
    state=news_io._session(session)
    with state['lock']:
        news_io._eligible(session,news_capture)
        first=news_io.capture_metadata(news_capture)['capture']['first_observed_epoch']
    origins=universal_origins(first)
    return prepare_history_share(session,news_capture,{pair:origins for pair in sorted(pairs)})
