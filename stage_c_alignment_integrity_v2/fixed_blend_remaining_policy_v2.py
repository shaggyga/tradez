"""Fixed common-target blend; authenticate both saved bases before valuation."""
import base64
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal, Context, localcontext

from contracts import fingerprint
from matched_remaining_native_v2 import prepared_packets, ORIGIN, TARGET, EPOCHS
from native_policy_input_v2 import load_native
from historical_native_input_v2 import prepared_candidate
from reference_accounting_adapter_v2 import DEFAULT_TRAD

PROFILE = 'matched26_fixed_half_remaining_policy.v1'
METHOD = 'fixed_equal_half_blend'


def blend_packet(ridge, hgb, trad_root=DEFAULT_TRAD):
    """Inputs are original packets recomputed by the caller, never free forecasts."""
    for packet, method in ((ridge, 'ridge'), (hgb, 'recovered_hgb')):
        if packet['method'] != method or packet['packet_sha256'] != fingerprint({k:v for k,v in packet.items() if k!='packet_sha256'}):
            raise ValueError('remaining_blend_original_packet_identity')
    for key in ('conditioning_epoch','original_target_epoch','reference_point','feature_observation_sha256','fit_id'):
        if ridge[key] != hgb[key]: raise ValueError('remaining_blend_paired_'+key)
    a,b = ridge['forecast']['forecast'],hgb['forecast']['forecast']
    for key in ('instrument','decision_epoch','target_id','available_epoch','model_ready_epoch','training_view_fingerprint'):
        if a[key] != b[key]: raise ValueError('remaining_blend_forecast_'+key)
    origin=ridge['conditioning_epoch']; target=ridge['original_target_epoch']
    if origin not in EPOCHS or target != TARGET or not a['model_ready_epoch'] < origin < a['available_epoch'] == origin+2:
        raise ValueError('remaining_blend_original_clock_required')
    native,_=load_native(trad_root); ref=ridge['reference_point']; curve=ridge['prepared_curve']
    bindings={'ridge_packet':ridge['packet_sha256'],'hgb_packet':hgb['packet_sha256'],
              'feature_observation':ridge['feature_observation_sha256'],'reference_price':ref['record_sha256'],
              'fit_pair':ridge['fit_id']}
    model=fingerprint({'method':METHOD,'weights':['0.5','0.5'],'base_models':[a['model_id'],b['model_id']]})
    with localcontext(Context(prec=192)):
        value=(Decimal(str(a['prediction']))+Decimal(str(b['prediction'])))/2
        if not value.is_finite():raise ValueError('remaining_blend_nonfinite')
        pips=Decimal(ref['reference_close'])*value/10000/Decimal(ref['pip_size'])
    prepared=native.prepare_curve(instrument=a['instrument'],pip_size=ref['pip_size'],
        forecast_cohort='matched26-two-day-development-'+METHOD,model_sha256=model,
        feature_version=curve['feature_version'],source_bindings=bindings,input_capture_sha256=curve['input_capture_sha256'],
        input_available_epoch=origin,reference_epoch=origin,reference_label_epoch=origin-60,
        reference_price=ref['reference_close'],reference_price_kind=curve['reference_price_kind'],bar_duration_sec=60,
        model_fitted_epoch=a['model_ready_epoch'],computation_started_epoch=origin,computed_epoch=a['available_epoch'],
        points=[{'horizon_sec':target-origin,'target_epoch':target,'target_label_epoch':target-60,
                 'model_id':model,'predicted_signed_pips':str(pips)}],policy=curve['policy'],
        computation_sha256=fingerprint({'bases':bindings,'prediction_bps':str(value),'method':METHOD}),scope='engineering_replay',
        input_context={'input_tier':PROFILE,'conditioning_kind':'fixed_blend_fresh_direct_remaining_models',
                       'conditioning_epoch':origin,'observed_publication':False})
    result={'schema_version':'fixed_remaining_blend_packet.v1','method':METHOD,'prepared_curve':prepared,
            'source_bindings':bindings,'reference_point':ref,'conditioning_epoch':origin,'original_target_epoch':target,
            'assumed_available_epoch':a['available_epoch'],'prediction_bps':str(value),
            'base_packet_sha256':[ridge['packet_sha256'],hgb['packet_sha256']],
            'observed_publication':False,'observed_execution':False,'base_models_refitted':False}
    result['packet_sha256']=fingerprint(result)
    return result


def combined_packets(packets,trad_root=DEFAULT_TRAD):
    groups=defaultdict(dict)
    for p in packets:
        pair=p['prepared_curve']['instrument'];method=p['method']
        if method not in ('ridge','recovered_hgb') or method in groups[pair]:
            raise ValueError('remaining_blend_duplicate_or_unknown_base')
        groups[pair][method]=p
    return [blend_packet(v['ridge'],v['recovered_hgb'],trad_root) for pair,v in sorted(groups.items())
            if set(v)=={'ridge','recovered_hgb'}]


def adapted_candidates(frame,config,trad_root=DEFAULT_TRAD):
    if frame.get('model_profile')!=PROFILE or frame.get('method')!=METHOD or frame.get('target_epoch')!=TARGET:
        raise ValueError('remaining_blend_profile_or_target_required')
    if frame.get('terminal') is True:
        if frame['epoch']!=TARGET-58 or frame.get('matched_input') is not None or frame.get('historical_packets')!=[] or frame.get('base_packets')!=[]:
            raise ValueError('remaining_blend_terminal_input_required')
        return [],[]
    origin=frame['epoch']-2;data=frame['matched_input']
    if origin not in EPOCHS or set(data)!={'fit','tree_base64','observations','references'}:
        raise ValueError('remaining_blend_exact_input_required')
    obs=data['observations'];universe=set(config['metadata'])
    if len(obs)!=68 or {o['instrument'] for o in obs}!=universe or len({o['record_id'] for o in obs})!=68 or any(o['origin_epoch']!=origin for o in obs):
        raise ValueError('remaining_blend_all68_original_observations_required')
    if data['fit']['fit_cutoff']!=ORIGIN-60 or data['fit']['ready_epoch']!=ORIGIN-30:
        raise ValueError('remaining_blend_original_fit_clock_required')
    packets,_=prepared_packets(data['fit'],base64.b64decode(data['tree_base64'],validate=True),obs,data['references'],trad_root=trad_root)
    if frame['base_packets']!=packets:raise ValueError('remaining_blend_original_base_recomputation_mismatch')
    selected=combined_packets(packets,trad_root)
    if frame['historical_packets']!=selected:raise ValueError('remaining_blend_packet_recomputation_mismatch')
    market={r['instrument']:r for r in frame['market_points']};candidates=[];refusals=[]
    for packet in selected:
        pair=packet['prepared_curve']['instrument'];ref=packet['reference_point'];point=market[pair]
        try:
            if point['status']!='valid_candle_close_pair':raise ValueError('remaining_blend_market_unavailable')
            if any(ref[k]!=point[k] for k in ('instrument','price_epoch','source_member_sha256','reference_close','bid','ask')):
                raise ValueError('remaining_blend_reference_market_mismatch')
            metadata=config['metadata'][pair]
            if Decimal(ref['pip_size'])!=Decimal(str(metadata['pip_size'])):raise ValueError('remaining_blend_pip_mismatch')
            candidates.append(prepared_candidate(packet,frame['quotes'],frame['epoch'],TARGET,frame['scenario'],metadata,trad_root))
        except (ValueError,KeyError,TypeError) as exc:
            refusals.append({'instrument':pair,'reason':str(exc),'packet_sha256':packet['packet_sha256']})
    return candidates,refusals


def fixture(inputs,scenario_id,trad_root=DEFAULT_TRAD):
    from matched_policy_fixture_v2 import fixture as parent_fixture
    from policy_continuation_v2 import PolicyReplay
    from historical_native_input_v2 import validate_historical_frame
    contract,frames,_=parent_fixture(inputs,'ridge',scenario_id,trad_root)
    config=PolicyReplay(contract,trad_root=trad_root).book.config;coverage=[]
    for f in frames:
        f.update(model_profile=PROFILE,method=METHOD)
        if f['kind']=='decision':
            origin=f['epoch']-2
            f['base_packets']=[] if f['terminal'] else [deepcopy(p) for p in inputs['packets'] if p['conditioning_epoch']==origin]
            f['historical_packets']=combined_packets(f['base_packets'],trad_root)
            f['candidates'],f['historical_refusals']=adapted_candidates(f,config,trad_root)
            if not f['terminal']:
                accepted={p['instrument'] for p in f['candidates']};refused={p['instrument']:p['reason'] for p in f['historical_refusals']}
                for pair in inputs['market']['universe']:
                    coverage.append({'instrument':pair,'method':METHOD,'conditioning_epoch':origin,'original_target_epoch':TARGET,
                                     'status':'admitted' if pair in accepted else refused.get(pair,'paired_remaining_forecasts_unavailable')})
        validate_historical_frame(f,config,trad_root)
    return contract,frames,coverage
