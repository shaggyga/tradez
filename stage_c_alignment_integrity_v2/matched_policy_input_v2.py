"""Authenticate matched fitted native inputs before existing policy valuation."""
import base64
from decimal import Decimal
from contracts import fingerprint
from matched_remaining_native_v2 import prepared_packets,ORIGIN,TARGET,EPOCHS,METHODS
from reference_accounting_adapter_v2 import DEFAULT_TRAD

PROFILE='matched26_remaining_native_policy.v1'

def adapted_candidates(frame,config,trad_root=DEFAULT_TRAD):
    if frame.get('model_profile') == 'matched26_fixed_half_remaining_policy.v1':
        from fixed_blend_remaining_policy_v2 import adapted_candidates as fixed_blend_candidates
        return fixed_blend_candidates(frame,config,trad_root)
    from historical_native_input_v2 import prepared_candidate
    if frame.get('model_profile')!=PROFILE or frame.get('method') not in METHODS:
        raise ValueError('matched_policy_profile_required')
    if frame.get('target_epoch')!=TARGET:raise ValueError('matched_original_target_required')
    if frame.get('terminal') is True:
        if frame['epoch']!=TARGET-58 or frame.get('matched_input') is not None or frame.get('historical_packets')!=[]:
            raise ValueError('matched_terminal_input_required')
        return [],[]
    origin=frame['epoch']-2
    if origin not in EPOCHS:raise ValueError('matched_conditioning_clock_required')
    data=frame['matched_input']
    if set(data)!={'fit','tree_base64','observations','references'}:raise ValueError('matched_exact_input_set_required')
    observations=data['observations'];universe=set(config['metadata'])
    if len(observations)!=68 or {o['instrument'] for o in observations}!=universe or len({o['record_id'] for o in observations})!=68:
        raise ValueError('matched_all68_observations_required')
    if any(o['origin_epoch']!=origin for o in observations):raise ValueError('matched_observation_clock_mismatch')
    meta=data['fit']
    if meta['fit_cutoff']!=ORIGIN-60 or meta['ready_epoch']!=ORIGIN-30:
        raise ValueError('matched_frozen_fit_clock_required')
    tree=base64.b64decode(data['tree_base64'],validate=True)
    packets,_=prepared_packets(meta,tree,observations,data['references'],trad_root=trad_root)
    selected=[p for p in packets if p['method']==frame['method']]
    if frame['historical_packets']!=selected:raise ValueError('matched_packet_recomputation_mismatch')
    market={r['instrument']:r for r in frame['market_points']}
    candidates=[];refusals=[]
    for packet in selected:
        pair=packet['prepared_curve']['instrument'];reference=packet['reference_point'];point=market[pair]
        # The feature slice and qualified candle panel must identify exactly the
        # same underlying close. A quoted pair cannot be silently substituted.
        try:
            if point['status']!='valid_candle_close_pair':raise ValueError('matched_reference_market_unavailable')
            if any(reference[k]!=point[k] for k in ('instrument','price_epoch','source_member_sha256','reference_close','bid','ask')):
                raise ValueError('matched_reference_market_binding_mismatch')
            metadata=config['metadata'][pair]
            if Decimal(reference['pip_size'])!=Decimal(str(metadata['pip_size'])):
                raise ValueError('matched_reference_pip_mismatch')
            envelope={**packet,'assumed_available_epoch':packet['forecast']['forecast']['available_epoch']}
            candidates.append(prepared_candidate(envelope,frame['quotes'],frame['epoch'],TARGET,frame['scenario'],metadata,trad_root))
        except (ValueError,KeyError,TypeError) as exc:
            refusals.append({'instrument':pair,'reason':str(exc),'packet_sha256':packet['packet_sha256']})
    return candidates,refusals
