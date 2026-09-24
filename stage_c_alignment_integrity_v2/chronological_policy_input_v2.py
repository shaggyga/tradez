"""Consume exact pinned parent forecasts; rederive native packets and valuation."""
from contracts import fingerprint
from chronological_policy_scenario_v2 import PROFILE, authority, contract, scenarios, canonical_metadata
from later_surface_native_v2 import NativeBatch
from reference_accounting_adapter_v2 import DEFAULT_TRAD


def validate_frame_authority(frame, config):
    c = contract(frame['scenario']['scenario_id']); a = authority(c['cohort'])
    if frame.get('cohort') != c['cohort']: raise ValueError('chronological_policy_cohort_mismatch')
    if frame.get('model_profile') != PROFILE or frame.get('method') not in c['methods']:
        raise ValueError('later_policy_registered_profile_method_required')
    if frame['scenario'] != scenarios().get(frame['scenario'].get('scenario_id')):
        raise ValueError('later_policy_exact_dated_scenario_required')
    if fingerprint(canonical_metadata(config['metadata'])) != a['market_metadata_sha256']:
        raise ValueError('later_policy_original_metadata_required')
    points = frame['market_points']; epochs = {r['price_epoch'] for r in points}
    if len(epochs) != 1: raise ValueError('later_policy_same_market_epoch_required')
    price_epoch = next(iter(epochs))
    if fingerprint(points) != a['market_panels'].get(str(price_epoch)):
        raise ValueError('later_policy_original_market_panel_required')
    kind, epoch = frame['kind'], frame['epoch']
    if kind == 'decision':
        if frame.get('target_epoch') != c['target_epoch']: raise ValueError('later_policy_common_target_required')
        if frame.get('terminal') is True:
            if epoch != c['target_epoch']-58 or price_epoch != c['target_epoch']-60:
                raise ValueError('later_policy_terminal_clock_mismatch')
        elif epoch-2 not in c['origins'] or price_epoch != epoch-2:
            raise ValueError('later_policy_current_conditioning_clock_mismatch')
    elif kind == 'execution':
        if epoch not in [t+60 for t in c['origins']]+[c['target_epoch']] or price_epoch != epoch:
            raise ValueError('later_policy_exact_fill_clock_required')
        if set(frame['fills']) != set(c['original_policy_contract']['policies']):
            raise ValueError('later_policy_exact_fill_arm_inventory_required')
    elif kind == 'financing':
        if epoch != c['rollover_epoch'] or price_epoch != c['rollover_price_epoch']:
            raise ValueError('later_policy_shifted_rollover_clock_required')
        if (frame['provenance_id'] != 'declared-single-rollover:'+fingerprint(frame['scenario']) or
            frame['accrual_period_id'] != 'cohort-single-scenario-rollover'):
            raise ValueError('later_policy_financing_provenance_mismatch')
    else: raise ValueError('later_policy_unregistered_frame_kind')
    return a, c


def adapted_candidates(frame, config, trad_root=DEFAULT_TRAD):
    from historical_native_input_v2 import prepared_candidate
    a, c = validate_frame_authority(frame, config)
    if frame.get('terminal') is True:
        if frame.get('later_input') is not None or frame['historical_packets'] != []:
            raise ValueError('later_policy_terminal_forecasts_forbidden')
        return [], []
    origin = frame['epoch']-2; data = frame['later_input']; registered = a['frames'][str(origin)]
    if set(data) != {'observations', 'predictions', 'coverage', 'source_frame_sha256'}:
        raise ValueError('later_policy_exact_forecast_input_set_required')
    group = registered['groups'][frame['method']]
    if (data['source_frame_sha256'] != registered['source_frame_sha256'] or
        fingerprint(data['observations']) != registered['observations_sha256'] or
        fingerprint(data['predictions']) != group['predictions_sha256'] or
        fingerprint(data['coverage']) != group['coverage_sha256'] or
        fingerprint(frame['historical_packets']) != group['packets_sha256']):
        raise ValueError('later_policy_original_forecast_authority_mismatch')
    if len(data['predictions']) != len(frame['historical_packets']):
        raise ValueError('later_policy_exact_packet_prediction_inventory')
    obs = {r['record_id']: r for r in data['observations']}
    points = {r['instrument']: r for r in frame['market_points']}
    native = NativeBatch(trad_root); candidates = []; refusals = []
    metadata = canonical_metadata(config['metadata'])
    for pred, packet in zip(data['predictions'], frame['historical_packets']):
        pair = pred['instrument']; reference = points[pair]
        expected = native.prepare(pred, obs[pred['record_id']], reference, metadata[pair], a['surface_contract'])
        if packet != expected: raise ValueError('later_policy_original_native_packet_recomputation_mismatch')
        try:
            candidates.append(prepared_candidate(packet, frame['quotes'], frame['epoch'], c['target_epoch'], frame['scenario'], metadata[pair], trad_root))
        except ValueError as exc:
            if str(exc) != 'financing_conversion_unavailable': raise
            refusals.append({'instrument': pair, 'reason': str(exc), 'packet_sha256': packet['packet_sha256']})
    native.verify()
    return candidates, refusals
