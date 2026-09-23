"""Paired forecast-error panels and descriptive joint time-block resampling."""
from collections import defaultdict
import math
import numpy as np
from contracts import fingerprint

def paired_origins(candidate,baseline,outcomes,*,asof):
    """One candidate group/method/target/procedure and one baseline method."""
    if type(asof) is not int:raise ValueError('integer_evaluation_clock_required')
    def keyed(rows):
        result={}
        for r in rows:
            f=r['forecast'];key=(r['record_id'],f['target_id'])
            if key in result:raise ValueError('duplicate_paired_forecast_key')
            result[key]=r
        return result
    a,b=keyed(candidate),keyed(baseline)
    if set(a)!=set(b):raise ValueError('exact_paired_forecast_support_required')
    labels={}
    for o in outcomes:
        key=(o['record_id'],o['target_id'])
        if key in labels:raise ValueError('unique_endpoint_outcomes_required')
        labels[key]=o
    panels=defaultdict(list);label_intervals=[];excluded=defaultdict(int)
    for key,row in a.items():
        f=row['forecast'];g=b[key]['forecast']
        if (f['decision_epoch'],f['available_epoch'],f['instrument'],row['procedure'])!=(g['decision_epoch'],g['available_epoch'],g['instrument'],b[key]['procedure']):raise ValueError('paired_forecast_clocks_or_instrument_mismatch')
        if f['available_epoch']>asof or g['available_epoch']>asof:raise ValueError('forecast_not_available_at_evaluation')
        o=labels.get(key)
        if o is None:
            excluded['missing_outcome']+=1;continue
        if o['available_epoch']>asof:
            excluded['not_yet_available']+=1;continue
        if o['value'] is None:
            excluded['unavailable_endpoint_value']+=1;continue
        if o['available_epoch']<o['label_end_epoch']:raise ValueError('outcome_cannot_precede_label_end')
        values=(f['prediction'],g['prediction'],o['value'])
        if not all(isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x) for x in values):raise ValueError('finite_paired_values_required')
        e=f['prediction']-o['value'];z=g['prediction']-o['value']
        panels[f['decision_epoch']].append((key[0],f['instrument'],abs(e),abs(z),e*e,z*z))
        label_intervals.append((f['decision_epoch'],o['label_end_epoch']))
    result=[]
    for origin,rows in sorted(panels.items()):
        rows.sort();result.append({'origin_epoch':origin,'rows':len(rows),'instruments':sorted({r[1] for r in rows}),'support_sha256':fingerprint([r[0] for r in rows]),
            'candidate_absolute_error_sum':math.fsum(r[2] for r in rows),'baseline_absolute_error_sum':math.fsum(r[3] for r in rows),
            'candidate_squared_error_sum':math.fsum(r[4] for r in rows),'baseline_squared_error_sum':math.fsum(r[5] for r in rows),
            'absolute_error_delta_sum':math.fsum(r[2]-r[3] for r in rows),'squared_error_delta_sum':math.fsum(r[4]-r[5] for r in rows)})
    forecast_origins=sorted({r['forecast']['decision_epoch'] for r in a.values()})
    mature_origins=[x['origin_epoch'] for x in result]
    return result,{'raw_paired_rows':sum(x['rows'] for x in result),'unique_origins':len(result),'origin_epochs':mature_origins,
        'forecast_origin_epochs':forecast_origins,'origins_without_mature_rows':sorted(set(forecast_origins)-set(mature_origins)),
        'forecast_rows':len(a),'excluded_outcome_rows':dict(sorted(excluded.items())),
        'maximum_label_span_seconds':max((b-a for a,b in label_intervals),default=None),'paired_support_sha256':fingerprint(sorted(key[0] for key in a if key in labels and labels[key]['value'] is not None and labels[key]['available_epoch']<=asof))}

def block_indices(n,length,*,replicates=2000,seed=20260922):
    if type(n) is not int or type(length) is not int or type(replicates) is not int or n<1 or length<1 or length>n or replicates<1:raise ValueError('valid_block_dimensions_required')
    if length==n:return None
    rng=np.random.default_rng(seed+length);blocks=math.ceil(n/length)
    starts=rng.integers(0,n-length+1,size=(replicates,blocks))
    return (starts[:,:,None]+np.arange(length)).reshape(replicates,-1)[:,:n]

def describe(panels,length,*,replicates=2000,seed=20260922):
    if type(length) is not int or length<1:raise ValueError('positive_integer_block_length_required')
    if not panels:return {'status':'no_mature_paired_rows','block_length_origins':length,'interval':None}
    origins=[p['origin_epoch'] for p in panels]
    if origins!=sorted(set(origins)) or any(type(p['rows']) is not int or p['rows']<1 for p in panels):raise ValueError('ordered_nonempty_origin_panels_required')
    n=len(panels);counts=np.asarray([p['rows'] for p in panels],dtype=np.float64)
    totals={k:np.asarray([p[k] for p in panels],dtype=np.float64) for k in ('absolute_error_delta_sum','squared_error_delta_sum')}
    if not all(np.isfinite(x).all() for x in totals.values()):raise ValueError('finite_paired_error_sums_required')
    result={'block_length_origins':length,'origin_count':n,'origin_grid_sha256':fingerprint(origins),
        'resampling_scope':'joint_only_with_identical_actual_origin_grid; no_cross_target_joint_inference',
        'raw_rows':int(counts.sum()),'original_deltas':{k:float(v.sum()/counts.sum()) for k,v in totals.items()},
        'origin_spacing_seconds':sorted({b-a for a,b in zip(origins,origins[1:])}),
        'observed_block_span_seconds':sorted({origins[i+length-1]-origins[i] for i in range(n-length+1)}) if length<=n else [],
        'uncertainty_scope':'descriptive_inspected_development_sensitivity_not_confirmation','cross_pair_resampling':'whole_shared_origin_panels','row_iid_assumed':False,'effective_sample_size':None,'selected_winner':None}
    if length>=n:
        return {**result,'status':'insufficient_distinct_time_blocks','interval':None,'replicates':0}
    if len(result['origin_spacing_seconds'])!=1:
        return {**result,'status':'irregular_origin_grid_requires_calendar_block_design','interval':None,'replicates':0}
    idx=block_indices(n,length,replicates=replicates,seed=seed);denom=counts[idx].sum(axis=1)
    result.update(status='descriptive_block_sensitivity',replicates=replicates,seed=seed,indices_sha256=fingerprint(idx.tolist()),
        joint_draw_identity=fingerprint({'origin_grid':origins,'indices':idx.tolist()}),
        interval={k:{'lower_2_5':float(np.quantile(v[idx].sum(axis=1)/denom,.025)),'upper_97_5':float(np.quantile(v[idx].sum(axis=1)/denom,.975))} for k,v in totals.items()})
    return result
