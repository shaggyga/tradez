"""Exact later native curves with model-specific recomputation before consumption."""
from decimal import Decimal, Context, localcontext
from pathlib import Path

import pyarrow.parquet as pq

from contracts import fingerprint
from historical_market_inputs_v2 import record, validate_record
from historical_native_input_v2 import panel, prepared_candidate, SCENARIOS
from native_policy_input_v2 import load_native
from publication import sha256_file


def market_inputs(root, manifest, contract):
    epochs = sorted(set(contract['policy_origins'] + [t+60 for t in contract['policy_origins']] +
        [contract['policy_origins'][0]+86400+60, contract['common_target_epoch']-60, contract['common_target_epoch']]))
    points, metadata = [], {}
    for member in manifest['members']:
        path = Path(root)/member['path']
        if path.is_symlink() or sha256_file(path) != member['sha256']:
            raise ValueError('later_slice_changed_before_consumption')
        table = pq.read_table(path)
        if set(table.column_names) != {'epoch', 'mid', 'bid', 'ask'}:
            raise ValueError('later_exact_slice_columns_required')
        wanted = {t-60 for t in epochs}; grouped = {}
        for r in table.to_pylist():
            if r['epoch'] in wanted:
                grouped.setdefault(r['epoch'], []).append({'close': r['mid'], 'bid_close': r['bid'], 'ask_close': r['ask']})
        pair = member['instrument']
        metadata[pair] = {'base_currency': pair[:3], 'quote_currency': pair[4:],
                          'pip_size': str(member['pip_size']), 'unit_increment': 1}
        points.extend(record(pair, t, grouped.get(t-60, []), member['source_member_sha256']) for t in epochs)
    return {'schema_version': 'forex_later_surface_market.v1', 'rows': points, 'metadata': metadata,
            'price_epochs': epochs, 'universe': sorted(metadata), 'slice_manifest_sha256': fingerprint(manifest),
            'execution_ready': False, 'observed_arrival': False,
            'scope': 'exact_retained_M1_close_points; scenario_quotes_not_observed_executability'}


class NativeBatch:
    """Authenticate immutable native source once at both batch boundaries."""
    def __init__(self, trad):
        self.trad = trad
        self.native, self.adapter = load_native(trad)

    def prepare(self, prediction, observation, reference, metadata, contract):
        return _prepare(prediction, observation, reference, metadata, contract, self.native)

    def verify(self):
        if load_native(self.trad) != (self.native, self.adapter):
            raise ValueError('later_native_batch_module_identity_changed')


def prepare(prediction, observation, reference, metadata, contract, trad):
    native, _ = load_native(trad)
    return _prepare(prediction, observation, reference, metadata, contract, native)


def _prepare(prediction, observation, reference, metadata, contract, native):
    """Prediction authority belongs to build_frame; this is deterministic conversion."""
    validate_record(reference)
    origin = prediction['decision_epoch']; target = contract['common_target_epoch']
    if (prediction['forecast_id'] != fingerprint({k: v for k, v in prediction.items() if k != 'forecast_id'}) or
        prediction['target_epoch'] != target or prediction['available_epoch'] != origin+2 or
        observation['origin_epoch'] != origin or observation['available_epoch'] > origin or
        prediction['observation_sha256'] != fingerprint(observation) or
        reference['status'] != 'valid_candle_close_pair' or reference['price_epoch'] != origin or
        reference['instrument'] != observation['instrument'] or prediction['instrument'] != observation['instrument'] or
        reference['source_member_sha256'] != observation['source_member_sha256']):
        raise ValueError('later_native_prediction_input_clock_binding_mismatch')
    bindings = {'prediction': prediction['forecast_id'], 'feature_observation': fingerprint(observation),
                'reference_price': reference['record_sha256'], 'signed_fit': prediction['signed_fit_id'],
                'absolute_fit': prediction['absolute_fit_id'], 'model': prediction['model_id']}
    if prediction['eligibility_snapshot_id'] is not None:
        bindings['eligibility_snapshot'] = prediction['eligibility_snapshot_id']
    with localcontext(Context(prec=192)):
        pips = Decimal(reference['reference_close'])*Decimal(str(prediction['prediction_bps']))/10000/Decimal(metadata['pip_size'])
    curve = native.prepare_curve(instrument=observation['instrument'], pip_size=metadata['pip_size'],
        forecast_cohort='later-remaining-development-'+prediction['variant']+'-'+prediction['base_method'],
        model_sha256=prediction['model_id'], feature_version='retained_technical24_current_cost2.v1', source_bindings=bindings,
        input_capture_sha256=fingerprint({'observation': observation, 'reference': reference}), input_available_epoch=origin,
        reference_epoch=origin, reference_label_epoch=origin-60, reference_price=reference['reference_close'],
        reference_price_kind='retained_M1_close_binary_roundtrip_decimal', bar_duration_sec=60,
        model_fitted_epoch=prediction['model_ready_epoch'], computation_started_epoch=prediction['computation_started_epoch'],
        computed_epoch=origin+2, points=[{'horizon_sec': target-origin, 'target_epoch': target,
            'target_label_epoch': target-60, 'model_id': prediction['model_id'], 'predicted_signed_pips': str(pips)}],
        policy=native.make_policy(native_horizons_sec=[target-origin], maximum_reference_age_sec=2,
            maximum_build_sec=2, maximum_issue_delay_sec=1, maximum_publication_delay_sec=1,
            maximum_decision_age_sec=2, minimum_remaining_sec=0),
        computation_sha256=prediction['forecast_id'], scope='engineering_replay',
        input_context={'input_tier': 'later_remaining_surface.v1', 'conditioning_kind': 'fresh_features_direct_remaining_horizon_model',
                       'conditioning_epoch': origin, 'observed_publication': False})
    packet = {'schema_version': 'forex_later_remaining_native.v1', 'prepared_curve': curve,
              'source_bindings': bindings, 'prediction': prediction, 'reference_point': reference,
              'assumed_available_epoch': origin+2, 'observed_publication': False, 'observed_execution': False}
    packet['packet_sha256'] = fingerprint(packet)
    return packet


def consume_verified(packet, expected_prediction, observation, reference, market, contract, trad):
    """Require a freshly recomputed authorized prediction, not a rehashed packet."""
    pair = observation['instrument']; metadata = market['metadata'][pair]
    if packet != prepare(expected_prediction, observation, reference, metadata, contract, trad):
        raise ValueError('later_native_model_recomputation_mismatch')
    origin = expected_prediction['decision_epoch']
    quotes = panel([r for r in market['rows'] if r['price_epoch'] == origin], origin+2)
    return prepared_candidate(packet, quotes, origin+2, contract['common_target_epoch'],
        SCENARIOS['candle_zero_slippage_financing'], metadata, trad)
