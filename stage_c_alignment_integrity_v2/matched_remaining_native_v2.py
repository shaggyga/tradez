"""Fresh exact remaining-target predictions in original native prepared curves."""
from decimal import Decimal,Context,localcontext
from contracts import fingerprint
from matched_campaign_models_v2 import predict,contract as campaign_contract
from native_policy_input_v2 import load_native
from reference_accounting_adapter_v2 import DEFAULT_TRAD
ORIGIN=1721606460
TARGET=1721779260
EPOCHS=list(range(ORIGIN,TARGET,21600))
NEW_MINUTES=[360,1080,1800,2160,2520]
REUSE_MINUTES=[720,1440,2880]
METHODS=('ridge','recovered_hgb')

def prepared_packets(meta,tree,observations,references,*,trad_root=DEFAULT_TRAD):
    if not observations:return [],[]
    origin=observations[0]['origin_epoch'];remaining=TARGET-origin
    if origin not in EPOCHS or remaining!=meta['target']['horizon_seconds'] or any(o['origin_epoch']!=origin for o in observations):raise ValueError('exact_remaining_horizon_and_original_target_required')
    forecasts,coverage=predict(meta,tree,observations,procedure='frozen',c=campaign_contract())
    native,_=load_native(trad_root);by={o['record_id']:o for o in observations};packets=[]
    for wrapped in forecasts:
        if wrapped['method'] not in METHODS:continue
        f=wrapped['forecast'];o=by[wrapped['record_id']];ref=references[o['instrument']]
        if (ref['instrument']!=o['instrument'] or ref['price_epoch']!=origin or ref['source_member_sha256']!=o['source_member_sha256'] or
            ref['record_sha256']!=fingerprint({k:v for k,v in ref.items() if k!='record_sha256'})):
            raise ValueError('remaining_reference_binding_mismatch')
        pip=ref['pip_size'];price=ref['reference_close']
        with localcontext(Context(prec=192)):pips=Decimal(price)*Decimal(str(f['prediction']))/10000/Decimal(pip)
        bindings={'fitted_model':f['model_id'],'fit_pair':meta['fit_id'],'feature_observation':fingerprint(o),'reference_price':ref['record_sha256'],'forecast':f['forecast_id']}
        curve=native.prepare_curve(instrument=o['instrument'],pip_size=pip,forecast_cohort='matched26-two-day-development-'+wrapped['method'],
            model_sha256=f['model_id'],feature_version='retained_technical24_current_cost2.v1',source_bindings=bindings,
            input_capture_sha256=fingerprint({'observation':o,'reference':ref}),input_available_epoch=origin,
            reference_epoch=origin,reference_label_epoch=origin-60,reference_price=price,reference_price_kind='retained_M1_close_binary_roundtrip_decimal',
            bar_duration_sec=60,model_fitted_epoch=meta['ready_epoch'],computation_started_epoch=origin,computed_epoch=f['available_epoch'],
            points=[{'horizon_sec':remaining,'target_epoch':TARGET,'target_label_epoch':TARGET-60,'model_id':f['model_id'],'predicted_signed_pips':str(pips)}],
            policy=native.make_policy(native_horizons_sec=[remaining],maximum_reference_age_sec=2,maximum_build_sec=2,maximum_issue_delay_sec=1,maximum_publication_delay_sec=1,maximum_decision_age_sec=2,minimum_remaining_sec=0),
            computation_sha256=fingerprint(wrapped),scope='engineering_replay',
            input_context={'input_tier':'matched_fitted_remaining_native_preparation.v1','conditioning_kind':'fresh_features_direct_remaining_horizon_model','conditioning_epoch':origin,'observed_publication':False})
        packet={'schema_version':'matched_remaining_native_packet.v1','method':wrapped['method'],'prepared_curve':curve,'forecast':wrapped,
            'fit_id':meta['fit_id'],'source_bindings':bindings,'reference_point':ref,'feature_observation_sha256':fingerprint(o),
            'original_target_epoch':TARGET,'conditioning_epoch':origin,'observed_publication':False,'observed_execution':False}
        packet['packet_sha256']=fingerprint(packet);packets.append(packet)
    return packets,{'forecasts':forecasts,'coverage':coverage}

def validate_packets(packets,meta,tree,observations,references,*,trad_root=DEFAULT_TRAD):
    expected,_=prepared_packets(meta,tree,observations,references,trad_root=trad_root)
    if packets!=expected:raise ValueError('remaining_native_packet_recomputation_mismatch')
    return True
