"""Fresh native qualification of saved chronological layer values, not backdating."""
import time
from contracts import fingerprint
from magnitude_layer_v2 import fit_snapshot,apply_snapshot_batch,BASES
from later_surface_layer_v2 import layer_settings


def build_frame(cohort,origin,predictor,native,observations,rows,outcomes,saved,frozen,market,c):
    started=time.monotonic();target=cohort['target'];h=(target-origin)//60
    if origin not in cohort['policy_origins'] or h not in c['horizons_minutes'] or saved['origin_epoch']!=origin or saved['horizon_minutes']!=h:
        raise ValueError('chronological_native_registered_frame_required')
    current=sorted((o for o in observations if o['origin_epoch']==origin),key=lambda o:o['instrument'])
    if [o['instrument'] for o in current]!=c['universe'] or market['universe']!=c['universe']:raise ValueError('chronological_native_all68_required')
    values=predictor.predict(h,current,origin);current_rows={r['record_id']:r for r in rows if r['decision_epoch']==origin}
    if set(values)!=set(current_rows):raise ValueError('chronological_native_fresh_base_support_changed')
    for rid,v in values.items():
        row=current_rows[rid]
        if (v['signed']['ridge']!=row['ridge_prediction_bps'] or v['signed']['recovered_hgb']!=row['recovered_hgb_prediction_bps'] or
            any(max(0.,v['absolute'][base])!=row['magnitude_prediction_bps'][base] for base in BASES)):
            raise ValueError('chronological_native_fresh_base_values_changed')
    if (frozen['layer_id']!=saved['frozen_snapshot_id'] or frozen['layer_id']!=fingerprint({k:v for k,v in frozen.items() if k!='layer_id'}) or
        frozen['contract_sha256']!=fingerprint(layer_settings(c['parent_surface_contract'])) or frozen['cutoff_epoch']!=c['parent_surface_contract']['layer_contract']['frozen_cutoff']):
        raise ValueError('chronological_native_saved_frozen_identity')
    scope=['legacy26',f'technical_endpoint_midpoint_elapsed_{h}m','frozen']
    expanded=fit_snapshot(rows,outcomes,origin,scope,h,layer_settings(c['parent_surface_contract']))
    if expanded!=saved['expanding_snapshot']:raise ValueError('chronological_native_saved_expanding_reconstruction_changed')
    fit_elapsed=time.monotonic()-started
    if fit_elapsed>c['resources']['fresh_inference_and_fit_seconds']:raise ValueError('chronological_native_fresh_fit_slot_exceeded:'+str(fit_elapsed))
    snapshots={'frozen':frozen,'expanding':expanded};old_pred={(p['record_id'],p['base_method'],p['variant']):p for p in saved['predictions']}
    old_cov={(p['record_id'],p['base_method'],p['variant']):p for p in saved['coverage']}
    if len(old_pred)!=len(saved['predictions']) or len(old_cov)!=len(saved['coverage']) or len(old_cov)!=len(c['universe'])*len(c['variants'])*2:raise ValueError('chronological_native_unique_complete_source_inventory')
    points=[r for r in market['rows'] if r['price_epoch']==origin];references={r['instrument']:r for r in points}
    if len(points)!=len(references) or set(references)!=set(c['universe']):raise ValueError('chronological_native_all_reference_slots_required')
    predictions=[];packets=[];coverage=[];native_contract={**c['parent_surface_contract'],'common_target_epoch':target,'policy_origins':cohort['policy_origins']}
    applications={}
    for mode,s in snapshots.items():
        applications[mode]={}
        if s['status']=='fitted':
            for p in apply_snapshot_batch(list(current_rows.values()),s,mode+'_prefix'):
                applications[mode].setdefault(p['record_id'],{})[p['base_method']]=p
    for obs in current:
        rid=obs['record_id'];row=current_rows.get(rid)
        for base in BASES:
            for variant in c['variants']:
                key=(rid,base,variant);prior=old_cov[key];mode=None if variant=='raw_unrestricted' else variant.rsplit('_',1)[1];snapshot=snapshots.get(mode)
                source_reason='base_unavailable' if row is None else 'insufficient_distinct_support' if snapshot is not None and snapshot['status']!='fitted' else 'eligible'
                if prior['reason']!=source_reason:raise ValueError('chronological_native_original_coverage_changed')
                reason=source_reason if source_reason!='eligible' else 'reference_'+references[obs['instrument']]['status'] if references[obs['instrument']]['status']!='valid_candle_close_pair' else 'eligible'
                cov={**prior,'reason':reason,'source_reason':source_reason,'cohort':cohort['name']};coverage.append(cov)
                if reason!='eligible':continue
                raw=variant.startswith('raw_');arm=None if raw else variant.rsplit('_',1)[0];value=values[rid]['signed'][base] if raw else applications[mode][rid][base]['predictions'][arm]
                model_id=row['base_model_ids'][base] if raw else fingerprint({'layer':snapshot['layer_id'],'base':base,'arm':arm});original=old_pred[key]
                if (original['forecast_id']!=fingerprint({k:v for k,v in original.items() if k!='forecast_id'}) or
                    original['instrument']!=obs['instrument'] or original['decision_epoch']!=origin or original['origin_epoch']!=origin or
                    original['horizon_minutes']!=h or original['target_id']!=scope[1] or
                    original['available_epoch']!=(row['available_epoch'] if mode is None else saved['reserved_ready_epoch']) or
                    original['prediction_bps']!=value or original['model_id']!=model_id or original['target_epoch']!=target or
                    original['signed_fit_id']!=row['selected_fit_id'] or original['absolute_fit_id']!=row['absolute_fit_id'] or
                    original['base_prediction_sha256']!=fingerprint(row) or original['observation_sha256']!=fingerprint(obs) or
                    original['eligibility_snapshot_id']!=(snapshot['layer_id'] if snapshot else None)):
                    raise ValueError('chronological_native_exact_diagnostic_binding_changed')
                pred={'record_id':rid,'instrument':obs['instrument'],'decision_epoch':origin,'target_epoch':target,'target_id':scope[1],
                    'available_epoch':origin+2,'base_method':base,'variant':variant,'prediction_bps':value,'absolute_prediction_bps':row['magnitude_prediction_bps'][base],
                    'model_id':model_id,'model_ready_epoch':predictor.available_epoch if raw else origin+1,'computation_started_epoch':origin if raw else origin+1,
                    'signed_fit_id':row['selected_fit_id'],'absolute_fit_id':row['absolute_fit_id'],'base_prediction_sha256':fingerprint(row),'observation_sha256':fingerprint(obs),
                    'eligibility_snapshot_id':snapshot['layer_id'] if snapshot else None,'diagnostic_forecast_id':original['forecast_id'],'cohort':cohort['name'],
                    'timing_qualification':'separate_fresh_same_origin_saved_base_and_exact_layer_reconstruction','production_available_epoch':None,'observed_publication':False}
                pred['forecast_id']=fingerprint(pred);predictions.append(pred);packets.append(native.prepare(pred,obs,references[obs['instrument']],market['metadata'][obs['instrument']],native_contract))
    native.verify();elapsed=time.monotonic()-started
    if elapsed>c['resources']['native_frame_seconds']:raise ValueError('chronological_native_complete_slot_exceeded:'+str(elapsed))
    return ({'cohort':cohort['name'],'origin_epoch':origin,'target_epoch':target,'horizon_minutes':h,'coverage':coverage,'predictions':predictions,'packets':packets,
        'snapshot_ids':{mode:s['layer_id'] for mode,s in snapshots.items()},'diagnostic_frame_sha256':fingerprint(saved),'observed_publication':False},
        {'cohort':cohort['name'],'origin_epoch':origin,'fresh_inference_and_layer_reconstruction_seconds':fit_elapsed,'complete_native_preparation_seconds':elapsed,
         'qualification_regressions':4*(expanded['status']=='fitted'),'qualification_snapshots':1,'scientific_parameters_changed':False})
