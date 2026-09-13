"""Staged joint inputs from fixed revision I/O and unchanged price/Ridge math.

The supplied news object must be an actual capture in the same healthy session.
Immutable replay is a separate historical operation and cannot issue forecasts.
No legacy current-news or governed-history route is called here.
"""
from pathlib import Path
import copy
import hashlib
import json
import math
import time

import revision_news_io_v10 as news_io
import revision_joint_point_v2 as point_contract
import shared_revision_history_v3 as history

original = news_io.adapter.original
price_inputs = original.price_inputs
SCHEMA = 'joint_revision_fixed_io_shared_history_inputs_v3_20260913'
FAMILY = 'ridge_price_news_v1'
NUMERICAL_MODULE = 'oanda_joint_price_news_models_v1'
BOUND_IO_SHA = 'b904e0b9a62d09942e98eb92b9e784413849e60bbd5d5ad301d6fdbb8554f314'
BOUND_POINT_SHA = '045a6b5684f4ea88d8ab01a14dab038e7ddd0e26c3a5b251c98a61b3a2915e60'
BOUND_HISTORY_SHA = '277ee94da6dbeaa3adeafb689a2c6aea3021ee9c25d1503114d633808072b8f9'
BOUND_NUMERIC_SHA = 'eb153acb966dc04ad950a0bbfcc730e78a9a0da1d8a24e91d641f473f5cfbc23'
HISTORY_SEC = 48 * 3600
MAX_ORIGINS = 256
MAX_INPUT_BYTES = 8 * 1024**2
MAX_POINT_BYTES = 8192
MAX_NEWS_AGE_SEC = 300
MAX_PRICE_AGE_SEC = 900
AVAILABILITY = 'actual_shared_store_capture_then_pair_price_read_and_retained_feature_decision'
TRAINING_POLICY = 'Exact original15m price anchors within48h; revision news selected at bar_start+60 only after original consumer availability; complete scan coverage required including empty frames; H1 label mature by retained price cutoff; unavailable coverage omitted, no neutral imputation; unchanged34feature_Ridge_v1.'
INERT = {'research_only': True, 'can_place_orders': False, 'can_promote': False,
         'can_authorize': False, 'account_eligible': False, 'proof_eligible': False}
need = news_io.need


def _encoded(value):
    return news_io.encode(value, MAX_INPUT_BYTES)


def _digest(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def _clock(value):
    return news_io.adapter.epoch(value() if callable(value) else value)


def _bindings():
    need(point_contract.news_io is news_io and history.news_io is news_io,'single_fixed_news_io_owner_required')
    kit=news_io.path_for(Path(__file__).absolute().parent,directory=True)
    need(all(Path(owner.__file__).absolute()==kit/Path(owner.__file__).name for owner in (news_io,point_contract,history)),'fixed_input_sibling_owners_required')
    graph=dict(history._bindings())
    need(graph.get(Path(news_io.__file__).name)==BOUND_IO_SHA,'fixed_revision_io_source_required')
    need(graph.get(Path(point_contract.__file__).name)==BOUND_POINT_SHA and graph.get(Path(history.__file__).name)==BOUND_HISTORY_SHA,'fixed_shared_history_source_required')
    need(graph.get(NUMERICAL_MODULE+'.py')==BOUND_NUMERIC_SHA,'unchanged_joint_numeric_source_required')
    _,own=news_io.read_exact(__file__,1024**2)
    return {**graph,Path(__file__).name:own['sha256']}


def _model():
    # Only the original fixed source loader is reused; it performs no news I/O.
    module = original._module(NUMERICAL_MODULE)
    need(original._source_hash(Path(module.__file__)) == BOUND_NUMERIC_SHA, 'unchanged_joint_numeric_source_required')
    return module


def _metadata(capture):
    meta = news_io.capture_metadata(capture)
    need(meta['scope'] == 'actual_capture' and meta['replay_is_not_fresh_capture'] is False,
         'actual_shared_news_capture_required')
    return meta


_point = point_contract._point


def _origins(rows, cutoff, max_close, history_start):
    origins = sorted(t for t in rows if t % 900 == 0 and t + 60 >= history_start
                     and t + 3600 <= cutoff and t + 3600 in rows and t + 3660 <= max_close)
    need(len(origins) <= MAX_ORIGINS, 'complete48h_origin_count_bound')
    return origins


def _derived(price, capture, session, instrument, pip, decision, history_share):
    rows, _ = price_inputs._verified_rows(price, instrument=instrument, pip_size=pip)
    meta = _metadata(capture); body = meta['capture']; context = body['readback_sha256']
    first = _clock(body['first_observed_epoch']); read = _clock(body['read_completed_epoch'])
    price_read = _clock(price['sources'][instrument]['first_observed_epoch'])
    price_observed = _clock(price['first_observed_epoch'])
    need(read <= first <= price_read <= price_observed <= decision, 'actual_input_observation_order')
    # Eligibility and failure-generation validation occur at the current boundary.
    current_result = news_io.current_pair_features(session, capture, instrument, decision)
    current = _point(current_result, decision, context)
    need(current['coverage_usable'], 'current_news_scan_coverage_unavailable')
    need(current['consumer_observed_epoch'] <= read, 'original_news_observation_after_actual_read')
    cutoff = price['reference_start_epoch']; max_close = price['max_bar_close_epoch']
    history_start = first - HISTORY_SEC
    origins = _origins(rows, cutoff, max_close, history_start)
    projection=history.project_pair_history(history_share,session=session,news_capture=capture,instrument=instrument,origins=origins)
    need(projection['schema_version']==history.PROJECTION and projection['instrument']==instrument
         and projection['origins']==origins and projection['context_sha256']==context
         and projection['capture_sha256']==meta['descriptor']['capture_sha256'], 'exact_pair_history_projection_required')
    frames = {}; availability = {}; points = []; unavailable = []
    need(len(projection['records'])==len(origins),'complete_historical_record_count')
    for origin,row in zip(origins,projection['records'],strict=True):
        need(row['origin']==origin and row['status'] in ('available','unavailable') and type(row['point']) is dict,
             'all_price_qualified_origins_must_be_prepared')
        point=row['point'];need(point['decision_epoch']==origin+60 and point['context_sha256']==context
             and row['status']==point['status'],'exact_historical_pair_point')
        points.append({'origin':origin,**point})
        if point['coverage_usable']:
            need(point['consumer_observed_epoch']<=read and point['available_epoch']<=origin+60,
                 'per_training_row_causal_availability')
            frames[str(origin)]=point['features'];availability[str(origin)]=point['available_epoch']
        else:unavailable.append(origin)
    need(len(frames)+len(unavailable)==len(origins),'complete_historical_coverage_accounting')
    share_descriptor={key:projection[key] for key in ('history_share_sha256','context_sha256','capture_sha256','source_generation_sha256')}
    share_descriptor['schema_version']=history.SCHEMA
    expiry = current['scan_started_epoch'] + MAX_NEWS_AGE_SEC
    if current['expires_epoch'] is not None: expiry = min(expiry, current['expires_epoch'])
    need(current['scan_started_epoch'] <= current['scan_completed_epoch'] <= first <= decision <= expiry,
         'current_original_news_window_expired')
    readiness = _model().family_readiness(rows, cutoff, instrument=instrument, pip_size=pip,
                                         news_frames=frames, current_news=current['features'])
    return {'history_share_descriptor':share_descriptor,'reference_start_epoch': cutoff, 'max_bar_close_epoch': max_close,
        'feature_decision_epoch': decision, 'news_capture_sha256': meta['descriptor']['capture_sha256'],
        'news_capture_descriptor': meta['descriptor'], 'news_context_sha256': context,
        'news_frames': frames, 'training_news_availability_by_epoch': availability,
        'training_news_points': points, 'current_news_features': current['features'], 'current_news_point': current,
        'news_evidence_epoch': current['scan_started_epoch'], 'news_generated_epoch': current['scan_completed_epoch'],
        'news_first_observed_epoch': first, 'news_available_epoch': max(first, current['available_epoch']), 'news_expires_epoch': expiry,
        'history_diagnostics': {'window_sec': HISTORY_SEC, 'history_start_epoch': history_start,
            'requested_origins': origins, 'covered_origins': len(frames), 'unavailable_origins': unavailable,
            'all_requested_origins_accounted': True, 'neutral_defaults_for_unknown_coverage': False,
            'full48h_source_coverage_claimed': False, 'price_and_label_maturity_checked': True},
        'family_readiness': readiness, 'available_families': [FAMILY] if readiness[FAMILY]['ready'] else []}


def capture_inputs(candle_root, instrument, *, pip_size, session, news_capture, history_share, clock=time.time):
    result = {'schema_version': SCHEMA, 'status': 'abstain', 'readiness_status': 'abstain', 'reasons': [],
        'pairs': [instrument], 'output_instrument': instrument, 'pip_size': pip_size,
        'model_source_sha256': BOUND_NUMERIC_SHA, 'dependency_versions': original.dependency_versions(),
        'availability_semantics': AVAILABILITY, 'training_policy': TRAINING_POLICY,
        'family_readiness': {}, 'available_families': [], **INERT}
    try:
        pair, pip = _model().validate_identity(instrument, pip_size); result.update(output_instrument=pair, pairs=[pair], pip_size=pip)
        bindings = _bindings(); _metadata(news_capture)
        price = price_inputs.capture_inputs(candle_root, pair, pip_size=pip, clock=clock)
        decision = _clock(clock); result.update(price_capture=price, first_observed_epoch=decision, source_bindings=bindings)
        need(price.get('status') == 'ready', 'verified_pair_price_input_required')
        result.update(_derived(price, news_capture, session, pair, pip, decision, history_share))
        need(_bindings() == bindings, 'joint_sources_changed_during_capture')
        result.update(status='ready', readiness_status='ready' if result['available_families'] else 'blocked')
    except Exception as exc:
        result['reasons'].append(type(exc).__name__ + ':' + str(exc)[:300])
    result['source_capture_sha256'] = _digest(result)
    return result


def validate_capture(capture, *, session, news_capture, history_share, instrument=None, pip_size=None):
    need(type(capture) is dict and capture.get('schema_version') == SCHEMA and capture.get('status') == 'ready',
         'joint_revision_input_not_ready')
    need(_digest({k: v for k, v in capture.items() if k != 'source_capture_sha256'}) == capture.get('source_capture_sha256'),
         'joint_revision_input_hash_mismatch')
    module = _model(); pair, pip = module.validate_identity(capture.get('output_instrument'), capture.get('pip_size'))
    need(instrument is None or pair == instrument, 'registered_pair_mismatch')
    need(pip_size is None or module.validate_identity(pair, pip_size)[1] == pip, 'registered_pip_mismatch')
    need(capture['source_bindings'] == _bindings() and capture['model_source_sha256'] == BOUND_NUMERIC_SHA
         and capture['dependency_versions'] == original.dependency_versions() and capture['training_policy'] == TRAINING_POLICY
         and capture['availability_semantics'] == AVAILABILITY and capture['pairs'] == [pair] and capture['reasons'] == []
         and all(capture.get(k) is v for k, v in INERT.items()), 'joint_revision_source_identity_or_scope')
    observed = _clock(capture['first_observed_epoch'])
    need(capture['feature_decision_epoch'] == observed, 'retained_feature_decision_required')
    price_inputs.validate_capture(capture['price_capture'], instrument=pair, pip_size=pip)
    derived = _derived(capture['price_capture'], news_capture, session, pair, pip, observed, history_share)
    for key, value in derived.items():
        need(_encoded(capture.get(key)) == _encoded(value), 'joint_revision_derived_mismatch:' + key)
    need(capture['readiness_status'] == ('ready' if capture['available_families'] else 'blocked'), 'joint_revision_readiness_binding')
    return None


def compute_predictions(capture, *, session, news_capture, history_share, families=None, clock=time.time):
    fields = ('news_capture_sha256', 'news_evidence_epoch', 'news_generated_epoch', 'news_first_observed_epoch',
              'news_available_epoch', 'news_expires_epoch', 'history_diagnostics', 'feature_decision_epoch')
    output = {'status': 'abstain', 'reasons': [], 'predictions': {}, 'family_readiness': {}, 'requested_families': [],
        'available_families': [], 'model_source_sha256': capture.get('model_source_sha256'),
        'source_capture_sha256': capture.get('source_capture_sha256'), 'output_instrument': capture.get('output_instrument'),
        'pip_size': capture.get('pip_size'), 'dependency_versions': original.dependency_versions(),
        **{k: capture.get(k) for k in fields}, **INERT}
    try:
        started = _clock(clock); validate_capture(capture, session=session, news_capture=news_capture, history_share=history_share)
        output['computation_started_epoch'] = started
        need(capture['first_observed_epoch'] <= started <= capture['news_expires_epoch']
             and 0 <= started - capture['news_evidence_epoch'] <= MAX_NEWS_AGE_SEC, 'joint_news_expired_before_computation')
        need(0 <= started - capture['max_bar_close_epoch'] <= MAX_PRICE_AGE_SEC, 'joint_price_stale_or_future')
        rows, _ = price_inputs._verified_rows(capture['price_capture']); module = _model(); selected = module._selected(families)
        output['requested_families'] = list(selected)
        predictions, readiness = module.predict_with_readiness(rows, capture['reference_start_epoch'],
            instrument=capture['output_instrument'], pip_size=capture['pip_size'], news_frames=capture['news_frames'],
            current_news=capture['current_news_features'], families=selected)
        output['family_readiness'] = readiness
        for family, (expected, probability, diagnostics) in predictions.items():
            training = diagnostics['training_row_start_epochs_by_pair'][capture['output_instrument']]
            for t in training:
                need(capture['training_news_availability_by_epoch'][str(t)] <= t + 60, 'training_news_future_to_own_origin')
            diagnostics['training_news_available_max_epoch'] = max(capture['training_news_availability_by_epoch'][str(t)] for t in training)
            output['predictions'][family] = {'expected_signed_pips': expected, 'probability_up': probability,
                'side': 1 if expected > 0 else -1 if expected < 0 else 0, 'diagnostics': diagnostics}
        completed = _clock(clock); output['computed_epoch'] = completed
        need(started <= completed <= capture['news_expires_epoch'] and completed - capture['news_evidence_epoch'] <= MAX_NEWS_AGE_SEC
             and completed - capture['max_bar_close_epoch'] <= MAX_PRICE_AGE_SEC, 'joint_input_expired_during_computation')
        # Detect a concurrently observed failure without selecting new features.
        news_io.current_pair_features(session, news_capture, capture['output_instrument'], capture['feature_decision_epoch'])
        need(capture['source_bindings'] == _bindings(), 'joint_sources_changed_during_computation')
        output.update(status='ready' if output['predictions'] else 'abstain', available_families=list(output['predictions']))
        if not output['predictions']: output['reasons'] = [FAMILY + ':' + reason for reason in readiness[FAMILY]['reasons']]
    except Exception as exc:
        output.update(status='abstain', predictions={}, available_families=[])
        output['reasons'].append(type(exc).__name__ + ':' + str(exc)[:300])
    _encoded(output)
    return output
