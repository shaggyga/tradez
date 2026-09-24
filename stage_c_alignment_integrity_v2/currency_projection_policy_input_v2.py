"""Validate the new projection-native packets before the unchanged policy core consumes them."""
from contracts import fingerprint
from historical_native_input_v2 import prepared_candidate

PROFILE = 'currency_projection_policy.v1'

def validate_frame_authority(frame, config):
    authority = frame.get('projection_input_authority')
    if frame.get('kind') in ('execution', 'financing'):
        if frame.get('model_profile') != PROFILE:
            raise ValueError('currency_projection_policy_profile_required')
        return None
    if frame.get('model_profile') != PROFILE or not isinstance(authority, dict):
        raise ValueError('currency_projection_policy_profile_authority_required')
    if authority.get('method') != frame.get('method') or authority.get('cohort') != frame.get('cohort'):
        raise ValueError('currency_projection_policy_method_cohort_authority')
    if authority.get('origin_epoch') != frame['epoch'] - 2 or authority.get('target_epoch') != frame.get('target_epoch'):
        raise ValueError('currency_projection_policy_clock_authority')
    if authority.get('market_panel_sha256') != fingerprint(frame['market_points']):
        raise ValueError('currency_projection_policy_market_authority')
    if authority.get('packet_sha256') != fingerprint(frame.get('historical_packets')):
        raise ValueError('currency_projection_policy_packet_authority')
    return authority

def adapted_candidates(frame, config, trad_root):
    validate_frame_authority(frame, config)
    if frame.get('terminal'):
        if frame.get('historical_packets') or frame.get('candidates'):
            raise ValueError('currency_projection_policy_terminal_forecasts_forbidden')
        return [], []
    candidates, refusals, seen = [], [], set()
    for packet in frame['historical_packets']:
        pair = packet['prepared_curve']['instrument']
        if pair in seen:
            raise ValueError('currency_projection_policy_duplicate_packet')
        seen.add(pair)
        try:
            candidates.append(prepared_candidate(packet, frame['quotes'], frame['epoch'], frame['target_epoch'], frame['scenario'], config['metadata'][pair], trad_root))
        except ValueError as exc:
            if str(exc) != 'financing_conversion_unavailable':
                raise
            refusals.append({'instrument': pair, 'reason': str(exc), 'packet_sha256': packet['packet_sha256']})
    return candidates, refusals
