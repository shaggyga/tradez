"""Deterministic one-worker fit and prediction reservations, independent of outcomes."""
from contracts import fingerprint
GROUPS=('legacy26','compact38_cost2','compact50_cost2','full228_cost2')
METHODS=('ridge','recovered_hgb')
PROCEDURES=('frozen','adaptive')
HORIZONS=(15,60,240,720,1440,2880,7200)

def schedule(fits,origins,cutoffs,*,fit_seconds=30,predict_seconds=2):
    if fit_seconds!=30 or predict_seconds!=2:raise ValueError('declared_reservation_budget_required')
    if origins!=sorted(set(origins)) or cutoffs!=sorted(set(cutoffs)):raise ValueError('ordered_unique_schedule_clocks_required')
    if any(type(x) is not int for x in origins+cutoffs):raise ValueError('integer_schedule_clocks_required')
    predictions=[];windows=[]
    for origin in origins:
        start=origin
        for group in GROUPS:
            for horizon in HORIZONS:
                for procedure in PROCEDURES:
                    predictions.append({'kind':'prediction','origin_epoch':origin,'group':group,'horizon_minutes':horizon,'procedure':procedure,'start_epoch':start,'end_epoch':start+predict_seconds})
                    start+=predict_seconds
        windows.append((origin,start))
    if any(a[1]>b[0] for a,b in zip(windows,windows[1:])):raise ValueError('prediction_reservations_overlap')
    expected={(g,h,t) for t in cutoffs for g in GROUPS for h in HORIZONS}
    indexed={}
    for fit in fits:
        key=(fit['group'],fit['horizon_minutes'],fit['fit_cutoff'])
        if key in indexed:raise ValueError('duplicate_scheduled_fit')
        if fit['maximum_outcome_available_epoch']>fit['fit_cutoff']:raise ValueError('unmatured_scheduled_fit')
        indexed[key]=fit
    if set(indexed)!=expected:raise ValueError('exact_joint_fit_inventory_required')
    tasks=[];previous=0
    for cutoff in cutoffs:
        for group in GROUPS:
            for horizon in HORIZONS:
                fit=indexed[group,horizon,cutoff];start=max(cutoff,previous)
                for a,b in windows:
                    if start<b and start+fit_seconds>a:start=b
                end=start+fit_seconds
                tasks.append({**fit,'kind':'fit','start_epoch':start,'end_epoch':end,'joint_ready_epoch':end})
                previous=end
    reservations=sorted(tasks+predictions,key=lambda x:(x['start_epoch'],x['end_epoch']))
    if any(a['end_epoch']>b['start_epoch'] for a,b in zip(reservations,reservations[1:])):raise ValueError('joint_worker_reservation_overlap')
    policy={'schema_version':'forex_joint_reservation_policy.v1','fit_seconds':fit_seconds,'prediction_seconds_per_batch':predict_seconds,
        'group_order':list(GROUPS),'horizons':list(HORIZONS),'procedures':list(PROCEDURES),'origins':origins,'planned_cutoffs':cutoffs}
    result={'schema_version':'forex_joint_reservations.v1','policy_sha256':fingerprint(policy),'policy':policy,'fit_seconds':fit_seconds,'prediction_seconds_per_batch':predict_seconds,'group_order':list(GROUPS),
        'prediction_selection_cutoff':'original_origin_not_later_slot_start','fit_tasks':tasks,'prediction_tasks':predictions,'modeled_only':True}
    result['schedule_id']=fingerprint(result);return result

def select_fit(s,group,horizon,procedure,origin):
    if group not in GROUPS or horizon not in HORIZONS or procedure not in PROCEDURES:raise ValueError('declared_joint_selector_required')
    candidates=[x for x in s['fit_tasks'] if x['group']==group and x['horizon_minutes']==horizon]
    if not candidates:raise ValueError('scheduled_fit_family_missing')
    initial=min(x['fit_cutoff'] for x in candidates)
    eligible=[x for x in candidates if x['joint_ready_epoch']<=origin and (procedure=='adaptive' or x['fit_cutoff']==initial)]
    return max(eligible,key=lambda x:x['fit_cutoff']) if eligible else None

def visible_forecast(row,asof,*,maximum_conditioning_age_seconds=None):
    from contracts import validate_forecast
    f=row['forecast'];validate_forecast(f)
    if type(asof) is not int:raise ValueError('integer_consumer_cutoff_required')
    if f['available_epoch']>asof:return {'status':'not_yet_available','forecast':None}
    age=asof-f['decision_epoch']
    if maximum_conditioning_age_seconds is not None:
        if type(maximum_conditioning_age_seconds) is not int or maximum_conditioning_age_seconds<0:raise ValueError('nonnegative_consumer_age_required')
        if age>maximum_conditioning_age_seconds:return {'status':'stale_conditioning','conditioning_age_seconds':age,'forecast':None}
    return {'status':'available','conditioning_age_seconds':age,'forecast':f}
