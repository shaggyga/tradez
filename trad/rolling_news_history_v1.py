"""Bounded as-of projection of actual rolling observations; no raw archive reads."""
from pathlib import Path
import re
import weakref
import revision_joint_point_v6 as point_contract

news_io = point_contract.news_io
need = news_io.need
SCHEMA = 'rolling_news_history_share_v1_20260930'
PROJECTION = 'rolling_news_pair_history_v1_20260930'
_shares = weakref.WeakKeyDictionary()


class HistoryShare: pass


def _bindings():
    graph = point_contract.source_bindings()
    graph[Path(__file__).name] = news_io.read_exact(__file__, 1024**2)[1]['sha256']
    return graph


def universal_origins(first_observed_epoch):
    first = news_io.adapter.epoch(first_observed_epoch)
    lower = first-48*3600
    return tuple(t for t in range((int(lower-60)//900-1)*900, (int(first-60)//900+2)*900+1, 900)
                 if t > 0 and lower <= t+60 <= first)


def prepare_universal_history_share(session, news_capture, instruments):
    state, parts = news_io._eligible(session, news_capture)
    pairs = tuple(sorted(instruments))
    need(1 <= len(pairs) <= 68 and len(set(pairs)) == len(pairs) and
         all(type(p) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', p) for p in pairs), 'rolling_unique_pairs')
    metadata = news_io.capture_metadata(news_capture)
    origins = universal_origins(metadata['capture']['first_observed_epoch'])
    need(len(origins) <= 193, 'rolling_origin_bound')
    binding = {'schema_version': SCHEMA, 'source_bindings': _bindings(), 'pairs': pairs,
               'origins': origins, 'capture': metadata['descriptor']}
    shared = HistoryShare()
    _shares[shared] = (session, news_capture, parts[4], pairs, origins, news_io.digest(binding))
    return shared


def _live(shared, session, news_capture):
    need(type(shared) is HistoryShare and shared in _shares, 'rolling_owned_history_required')
    value = _shares[shared]
    _, parts = news_io._eligible(session, news_capture)
    need(value[0] is session and value[1] is news_capture and value[2] == parts[4], 'rolling_history_session_generation')
    return value


def project_pair_history(shared, *, session, news_capture, instrument, origins):
    value = _live(shared, session, news_capture)
    need(instrument in value[3] and list(origins) == sorted(set(origins)) and
         all(t in value[4] for t in origins), 'rolling_history_request_outside_contract')
    meta = news_io.capture_metadata(news_capture)
    context = meta['capture']['readback_sha256']
    records = []
    for origin in origins:
        result = news_io.pair_features(news_capture, instrument, origin+60)
        point = point_contract._point(result, origin+60, context)
        records.append({'origin': origin, 'status': point['status'], 'point': point})
    return {'schema_version': PROJECTION, 'instrument': instrument, 'origins': list(origins),
            'records': records, 'history_share_sha256': value[5], 'context_sha256': context,
            'capture_sha256': meta['descriptor']['capture_sha256'],
            'source_generation_sha256': news_io.digest(_bindings())}


def history_share_metadata(shared):
    need(shared in _shares, 'rolling_owned_history_required')
    value = _shares[shared]
    return {'schema_version': SCHEMA, 'compact_sha256': value[5], 'origins': list(value[4]),
            'instruments': list(value[3]), 'historical_coverage_inferred': False}
