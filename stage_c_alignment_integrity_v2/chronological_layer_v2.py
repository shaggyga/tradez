"""Chronological development extension with immutable base and frozen-layer reuse."""
from collections import Counter,defaultdict
import math
from contracts import TrainingView,fingerprint
from matched_campaign_models_v2 import FEATURES,population,validate_fit
from absolute_movement_models_v2 import transform_outcomes
from magnitude_layer_v2 import fit_snapshot,apply_snapshot,BASES
from later_surface_layer_v2 import layer_settings


def verify_population(meta,binary,observations,outcomes,signed=None):
    validate_fit(meta,binary)
    view=TrainingView(**{k:v for k,v in meta['training_view'].items() if k!='schema'})
    labels=outcomes if signed is None else transform_outcomes([o for o in outcomes if o['available_epoch']<=meta['fit_cutoff']],meta['target']['horizon_seconds']//60)
    rows=population(observations,labels,meta['target'],view)
    if fingerprint(rows)!=meta['training_population_sha256'] or len(rows)!=meta['training_rows'] or meta['feature_schema_sha256']!=fingerprint(FEATURES):
        raise ValueError('chronological_saved_fit_population_or_schema_changed')
    if signed is not None:
        original=population(observations,outcomes,signed['target'],view)
        if (meta['original_signed_fit_id']!=signed['fit_id'] or fingerprint(original)!=meta['original_signed_population_sha256'] or
            [r['record_id'] for r,_ in rows]!=[r['record_id'] for r,_ in original]):
            raise ValueError('chronological_absolute_signed_lineage_changed')
    return {'fit_id':meta['fit_id'],'training_rows':len(rows),'training_population_sha256':fingerprint(rows),'feature_schema_sha256':meta['feature_schema_sha256'],'base_refit':False}


def combine(old,new,horizon,c):
    if old['horizon_minutes']!=horizon or new['horizon_minutes']!=horizon:raise ValueError('chronological_horizon_mismatch')
    for chunk,epochs in ((old,c['original_origins']),(new,c['new_origins'])):
        coverage=chunk['coverage']
        keys=[r['record_id'] for r in coverage]
        expected={pair+':'+str(t) for t in epochs for pair in c['universe']}
        if len(keys)!=len(set(keys)) or set(keys)!=expected:raise ValueError('chronological_full_base_coverage_required')
        if any(r['base_methods']!=list(BASES) for r in coverage):raise ValueError('chronological_both_base_methods_required')
        if any(r['decision_epoch'] not in epochs for r in chunk['joined_rows']):raise ValueError('chronological_wrong_origin')
    rows=old['joined_rows']+new['joined_rows']
    if len({r['record_id'] for r in rows})!=len(rows):raise ValueError('chronological_duplicate_base_row')
    return rows


def frozen_snapshot(old_frame,rows,outcomes,horizon,c):
    parent=c['parent_surface_contract'];settings=layer_settings(parent)
    snapshot=old_frame['snapshots']['frozen']
    scope=['legacy26',f'technical_endpoint_midpoint_elapsed_{horizon}m','frozen']
    if snapshot is None:
        if horizon!=2880:raise ValueError('chronological_unexpected_absent_frozen_snapshot')
        return fit_snapshot(rows,outcomes,parent['layer_contract']['frozen_cutoff'],scope,horizon,settings),False
    if (snapshot['layer_id']!=fingerprint({k:v for k,v in snapshot.items() if k!='layer_id'}) or
        snapshot['scope']!=scope or snapshot['contract_sha256']!=fingerprint(settings) or
        snapshot['cutoff_epoch']!=parent['layer_contract']['frozen_cutoff']):
        raise ValueError('chronological_original_frozen_snapshot_binding')
    # Exact original rows/labels were authenticated and reproduced; all added rows
    # begin after this cutoff. Check membership directly without another fit.
    from causal_convex_blend_v2 import label
    membership=[]
    for r in sorted(rows,key=lambda r:(r['decision_epoch'],r['instrument'])):
        if r['decision_epoch']>=snapshot['cutoff_epoch'] or r['available_epoch']>snapshot['cutoff_epoch']:continue
        o=label(r,outcomes,horizon)
        if o['available_epoch']>snapshot['cutoff_epoch'] or o['value'] is None:continue
        membership.append({'record_id':r['record_id'],'origin_epoch':r['decision_epoch'],'prediction_sha256':fingerprint(r),'outcome_sha256':fingerprint(o),'forecast_available_epoch':r['available_epoch'],'outcome_available_epoch':o['available_epoch']})
    if membership!=snapshot['training_membership']:raise ValueError('chronological_frozen_membership_changed')
    return snapshot,True


def frame(horizon,origin,rows,observations,outcomes,frozen,c):
    scope=['legacy26',f'technical_endpoint_midpoint_elapsed_{horizon}m','frozen']
    if origin not in c['new_origins'] or horizon not in c['horizons_minutes']:raise ValueError('chronological_registered_frame_required')
    current=sorted((o for o in observations if o['origin_epoch']==origin),key=lambda o:o['instrument'])
    if [o['instrument'] for o in current]!=c['universe']:raise ValueError('chronological_all68_frame_required')
    current_rows={r['record_id']:r for r in rows if r['decision_epoch']==origin}
    expanded=fit_snapshot(rows,outcomes,origin,scope,horizon,layer_settings(c['parent_surface_contract']))
    snapshots={'frozen':frozen,'expanding':expanded};coverage=[];predictions=[]
    ready=origin+16+15*(c['horizons_minutes'].index(horizon)+1)
    for obs in current:
        row=current_rows.get(obs['record_id']);applications={}
        if row:
            for mode,s in snapshots.items():
                if s['status']=='fitted':applications[mode]={p['base_method']:p for p in apply_snapshot(row,s,mode+'_prefix')}
        for base in BASES:
            for variant in c['variants']:
                mode=None if variant=='raw_unrestricted' else variant.rsplit('_',1)[1]
                snapshot=snapshots.get(mode)
                reason='base_unavailable' if row is None else 'insufficient_distinct_support' if snapshot is not None and snapshot['status']!='fitted' else 'eligible'
                cov={'record_id':obs['record_id'],'instrument':obs['instrument'],'origin_epoch':origin,'target_epoch':origin+horizon*60,'horizon_minutes':horizon,'base_method':base,'variant':variant,'reason':reason,'eligibility_snapshot_id':None if snapshot is None else snapshot['layer_id']}
                coverage.append(cov)
                if reason!='eligible':continue
                raw=variant.startswith('raw_');arm=None if raw else variant.rsplit('_',1)[0]
                value=row['ridge_prediction_bps' if base=='ridge' else 'recovered_hgb_prediction_bps'] if raw else applications[mode][base]['predictions'][arm]
                prediction={k:v for k,v in cov.items() if k!='reason'}
                prediction.update(decision_epoch=origin,target_id=scope[1],prediction_bps=value,
                    available_epoch=row['available_epoch'] if mode is None else ready,
                    model_id=row['base_model_ids'][base] if raw else fingerprint({'layer':snapshot['layer_id'],'base':base,'arm':arm}),
                    signed_fit_id=row['selected_fit_id'],absolute_fit_id=row['absolute_fit_id'],base_prediction_sha256=fingerprint(row),
                    observation_sha256=fingerprint(obs),production_available_epoch=None,native_policy_admitted=False)
                prediction['forecast_id']=fingerprint(prediction);predictions.append(prediction)
    return {'origin_epoch':origin,'horizon_minutes':horizon,'frozen_snapshot_id':frozen['layer_id'],'expanding_snapshot':expanded,'coverage':coverage,'predictions':predictions,'reserved_ready_epoch':ready,'native_policy_admitted':False}


def assessment(frames,outcomes,c):
    groups=defaultdict(list);coverage=Counter();cohorts=c['cohorts']
    for item in frames:
        coverage.update(r['reason'] for r in item['coverage'])
        for row in item['predictions']:
            o=outcomes[row['record_id'],row['target_id']]
            if o['label_end_epoch']!=row['target_epoch'] or o['available_epoch']<o['label_end_epoch']:raise ValueError('chronological_assessment_exact_maturity_required')
            labels=[('all_new',0),('utc_day',row['decision_epoch']//86400)]
            labels.extend((x['name'],x['target']) for x in cohorts if row['decision_epoch'] in x['policy_origins'] and row['target_epoch']==x['target'])
            for kind,key in labels:
                groups[kind,key,row['horizon_minutes'],row['base_method'],row['variant']].append((row,o))
    scores=[];lookup={}
    for key,part in sorted(groups.items()):
        mature=[(r,o) for r,o in part if r['available_epoch']<=c['assessment_asof'] and o['available_epoch']<=c['assessment_asof'] and o['value'] is not None]
        if any(not math.isfinite(o['value']) or not math.isfinite(r['prediction_bps']) for r,o in mature):raise ValueError('chronological_nonfinite_score')
        errors=[r['prediction_bps']-o['value'] for r,o in mature];n=len(errors)
        score={'stratum':key[0],'stratum_key':key[1],'horizon_minutes':key[2],'base_method':key[3],'variant':key[4],'prediction_rows':len(part),'mature_rows':n,
            'distinct_origins':len({r['decision_epoch'] for r,o in mature}),'distinct_pairs':len({r['instrument'] for r,o in mature}),
            'support_sha256':fingerprint(sorted(r['record_id'] for r,o in mature)),
            'mae_bps':math.fsum(map(abs,errors))/n if n else None,'mse_bps2':math.fsum(e*e for e in errors)/n if n else None,
            'bias_bps':math.fsum(errors)/n if n else None,'p95_absolute_error_bps':sorted(map(abs,errors))[math.ceil(.95*n)-1] if n else None}
        scores.append(score);lookup[key]=score
    paired=[]
    for key,a in sorted(lookup.items()):
        if not key[-1].startswith('magnitude_interaction_'):continue
        mode=key[-1].rsplit('_',1)[1]
        for control in ('raw_matched_'+mode,'signed_only_'+mode):
            b=lookup.get((*key[:-1],control))
            if b is None or any(a[k]!=b[k] for k in ('prediction_rows','mature_rows','support_sha256')):raise ValueError('chronological_exact_matched_score_support_required')
            paired.append({k:a[k] for k in ('stratum','stratum_key','horizon_minutes','base_method','mature_rows','distinct_origins','distinct_pairs','support_sha256')}|
                {'mode':mode,'control':control,'mae_delta_bps':None if not a['mature_rows'] else a['mae_bps']-b['mae_bps'],'mse_delta_bps2':None if not a['mature_rows'] else a['mse_bps2']-b['mse_bps2']})
    return {'scores':scores,'paired_differences':paired,'coverage_reasons':dict(coverage),'inference':'descriptive dependent development; no confidence interval, significance, winner or profitability claim','confirmation':False}
