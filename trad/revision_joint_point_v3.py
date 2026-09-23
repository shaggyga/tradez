"""Exact original compact point validation with one fixed IO owner; no input import.

Extracted from accepted input9af; no current/price/history operation is added.
The fixed flat generation uses IO11 with durable transport-health checks.
"""
from pathlib import Path
import copy,math
import revision_news_io_v11 as news_io
need=news_io.need
MAX_NEWS_AGE_SEC=300
MAX_POINT_BYTES=8192
BOUND_IO_SHA='0274a67179926db6fc6e8710bd2e41dd02d1a30c43cb5b6242425759519bb526'
ORIGINAL_INPUT_SOURCE_SHA256='9af95dea1b1a313ebe446f9e4bf73a210cbb10a2660fd5e09831605b0fe5cb5a'

def source_bindings():
    kit=news_io.path_for(Path(__file__).absolute().parent,directory=True)
    need(Path(news_io.__file__).absolute()==kit/Path(news_io.__file__).name,'fixed_point_sibling_io_required')
    graph=news_io.source_graph()
    need(graph.get(Path(news_io.__file__).name)==BOUND_IO_SHA,'fixed_point_io_source_required')
    _,own=news_io.read_exact(__file__,1024**2)
    return {**graph,Path(__file__).name:own['sha256']}

def _clock(value):
    return news_io.adapter.epoch(value() if callable(value) else value)

def _point(result, decision, context_sha):
    """Keep eight values and timing/hash evidence, not the full story frame."""
    need(result['decision_epoch'] == decision and result['context_sha256'] == context_sha,
         'exact_news_decision_and_context_required')
    timing = result['transport_timing']
    need(result['transport_timing_sha256'] == news_io.adapter.sha(timing), 'exact_selected_timing_hash')
    need(timing['decision_epoch'] == decision and timing['context_sha256'] == context_sha
         and timing['transport_selection_sha256'] == result['transport_selection_sha256']
         and timing['source_generation_sha256'] == result['source_generation_sha256'], 'selected_timing_identity')
    usable = result['coverage_usable']
    need(type(usable) is bool and timing['coverage_usable'] is usable, 'typed_selected_coverage')
    record = {'decision_epoch': decision, 'status': 'available' if usable else 'unavailable',
        'context_sha256': context_sha, 'source_generation_sha256': result['source_generation_sha256'],
        'transport_selection_sha256': result['transport_selection_sha256'],
        'transport_timing_sha256': result['transport_timing_sha256'],
        'consumer_id': timing['consumer_id'], 'index_row_ordinal': timing['index_row_ordinal'],
        'consumer_observed_epoch': timing['consumer_observed_epoch'],
        'observed_publication_prefix': timing['observed_publication_prefix'],
        'coverage_sha256': timing['coverage_sha256'], 'coverage_usable': usable,
        'features': None, 'frame_sha256': None, 'available_epoch': None,
        'scan_started_epoch': None, 'scan_completed_epoch': None, 'expires_epoch': None,
        'reason': result.get('reason')}
    if timing['coverage'] is not None:
        coverage = timing['coverage']
        need(news_io.adapter.sha(coverage) == timing['coverage_sha256'], 'selected_coverage_hash')
        start, end, observed = (_clock(coverage['read_started_epoch']), _clock(coverage['read_completed_epoch']),
                                _clock(timing['consumer_observed_epoch']))
        need(start <= end <= observed <= decision, 'per_origin_scan_and_observation_order')
        record.update(scan_started_epoch=start, scan_completed_epoch=end)
    if usable:
        need(result['status'] == 'available' and record['scan_started_epoch'] is not None
             and 0 <= decision - record['scan_started_epoch'] <= MAX_NEWS_AGE_SEC, 'current_complete_scan_required')
        values = result['features']
        need(type(values) is list and len(values) == 8 and all(type(x) in (int, float) and math.isfinite(x) for x in values),
             'original_eight_news_features_required')
        need(news_io.adapter.sha(result['frame']) == result['frame_sha256'], 'exact_news_frame_hash')
        frame_available = result['frame']['available_max_epoch']
        need(type(frame_available) in (int, float) and math.isfinite(frame_available) and frame_available >= 0,
             'typed_frame_availability')
        available = max(_clock(timing['consumer_observed_epoch']), frame_available)
        need(available <= decision, 'each_training_news_available_by_its_origin')
        expires = result.get('expires_epoch')
        if expires is not None: expires = _clock(expires)
        record.update(features=copy.deepcopy(values), frame_sha256=result['frame_sha256'],
                      available_epoch=available, expires_epoch=expires)
    else:
        need(result['frame'] is None and result.get('features') is None, 'unknown_coverage_has_no_neutral_features')
    need(len(news_io.encode(record, MAX_POINT_BYTES)) <= MAX_POINT_BYTES, 'compact_pair_news_point_bound')
    return record
