"""Declared whole-day association destruction; no significance or model selection."""
from collections import defaultdict,Counter
from datetime import datetime,timezone
import math
from contracts import fingerprint

def day_key(epoch):return datetime.fromtimestamp(epoch,timezone.utc).date().isoformat()

def compare(candidate,control,outcomes,*,asof,universe):
    def keyed(rows):
        result={}
        for row in rows:
            f=row['forecast'];key=(row['record_id'],f['target_id'])
            if key in result:raise ValueError('duplicate_control_forecast')
            result[key]=row
        return result
    a,b=keyed(candidate),keyed(control)
    if not a or set(a)!=set(b):raise ValueError('exact_nonempty_control_support_required')
    labels={}
    for row in outcomes:
        key=(row['record_id'],row['target_id'])
        if key in labels:raise ValueError('duplicate_control_outcome')
        labels[key]=row
    valid={};excluded=Counter();origins=sorted({r['forecast']['decision_epoch'] for r in a.values()})
    if len({k[1] for k in a})!=1:raise ValueError('one_target_required')
    for key,row in a.items():
        f=row['forecast'];g=b[key]['forecast']
        if (f['decision_epoch'],f['available_epoch'],f['instrument'],row['procedure'])!=(g['decision_epoch'],g['available_epoch'],g['instrument'],b[key]['procedure']):raise ValueError('control_clocks_or_pair_mismatch')
        if f['instrument'] not in universe or f['available_epoch']>asof:raise ValueError('declared_available_forecast_required')
        o=labels.get(key)
        if o is None:excluded['missing_outcome']+=1;continue
        if o['available_epoch']<o['label_end_epoch']:raise ValueError('outcome_before_label_end')
        if o['available_epoch']>asof:excluded['not_mature']+=1;continue
        if o['value'] is None:excluded['missing_endpoint']+=1;continue
        if not all(type(v) in (int,float) and math.isfinite(v) for v in (f['prediction'],g['prediction'],o['value'])):raise ValueError('finite_control_values_required')
        support=(f['decision_epoch'],f['instrument'])
        if support in valid:raise ValueError('duplicate_control_origin_pair')
        valid[support]=(key[0],f['prediction'],g['prediction'],o['value'])
    by_day=defaultdict(list)
    for origin in origins:by_day[day_key(origin)].append(origin)
    # A complete declared day has the exact four UTC bands, at one common second offset.
    full=[];partial=[]
    for day,grid in sorted(by_day.items()):
        offsets=[v%86400 for v in grid]
        complete=len(grid)==4 and offsets==[offsets[0]+21600*i for i in range(4)] and 0<=offsets[0]<21600
        mature=all(any((origin,pair) in valid for pair in universe) for origin in grid)
        (full if complete and mature else partial).append(day)
    selected_origins=[v for day in full for v in by_day[day]]
    balanced=[pair for pair in universe if selected_origins and all((origin,pair) in valid for origin in selected_origins)]
    scope={'forecast_rows':len(a),'mature_rows_before_balancing':len(valid),'outcome_exclusions':dict(sorted(excluded.items())),
        'all_declared_instruments':list(universe),'balanced_instruments':balanced,'excluded_instruments':sorted(set(universe)-set(balanced)),
        'complete_utc_days':full,'excluded_utc_days':partial,'origin_grid':selected_origins,'day_count':len(full),
        'balanced_rows':len(selected_origins)*len(balanced),'balance_excluded_mature_rows':len(valid)-len(selected_origins)*len(balanced),
        'support_sha256':fingerprint(sorted(valid[o,p][0] for o in selected_origins for p in balanced)),
        'preserved':['within_day_origin_order','whole_shared_cross_currency_panels','outcome_marginal_values_on_balanced_support'],
        'destroyed':['original_forecast_outcome_day_association'],'limitations':['cyclic_wrap_is_artificial','inspected_development_dates','few_day_shifts_not_independent_replications','multiday_labels_overlap'],
        'p_value':None,'effective_sample_size':None,'selected_winner':None}
    if len(full)<2 or not balanced:return scope,[{'status':'insufficient_balanced_day_support','shift_days':None,'rows':0,'metrics':None}]
    values=[]
    for shift in range(len(full)):
        absolute=[];squared=[];cabs=[];csq=[];babs=[];bsq=[];mapping=[]
        for index,day in enumerate(full):
            source_day=full[(index+shift)%len(full)]
            for origin,source_origin in zip(by_day[day],by_day[source_day]):
                mapping.append([origin,source_origin])
                for pair in balanced:
                    _,ca,co,_=valid[origin,pair];y=valid[source_origin,pair][3]
                    ce,be=ca-y,co-y;cabs.append(abs(ce));babs.append(abs(be));csq.append(ce*ce);bsq.append(be*be)
                    absolute.append(abs(ce)-abs(be));squared.append(ce*ce-be*be)
        n=len(absolute)
        values.append({'status':'identity_on_balanced_support' if shift==0 else 'blocked_day_association_control','shift_days':shift,'rows':n,
            'outcome_origin_mapping':mapping,'mapping_sha256':fingerprint(mapping),
            'metrics':{'candidate_mae_bps':math.fsum(cabs)/n,'control_mae_bps':math.fsum(babs)/n,'candidate_mse_bps2':math.fsum(csq)/n,'control_mse_bps2':math.fsum(bsq)/n,'mae_delta_bps':math.fsum(absolute)/n,'mse_delta_bps2':math.fsum(squared)/n}})
    return scope,values
