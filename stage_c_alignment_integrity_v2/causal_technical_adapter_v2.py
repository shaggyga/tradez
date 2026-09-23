"""Reuse retained technical/label functions with all68 independent origin coverage."""
from collections import Counter
import json
import numpy as np
import pandas as pd
from contracts import fingerprint
import retained_direction_features_v1 as old
from retained_endpoint_targets_v1 import endpoint_outcomes
FEATURES=list(old.TECHNICAL_COLUMNS)+['known_entry_long_cost_bps','known_entry_short_cost_bps']
HORIZONS=[15,60,240,720,1440]
ENDPOINT_HORIZONS=[15,60,240,720,1440,2880,7200]
CALENDAR=['daily_close','2_sessions','5_sessions']

def contract():
    epoch=lambda x:int(pd.Timestamp(x,tz='UTC').timestamp())
    return {'schema_version':'forex_causal_technical_inputs.v2','data_start':epoch('2024-07-01'),
        'origin_start':epoch('2024-07-08')+60,'origin_end':epoch('2024-07-27'),
        'data_end':epoch('2024-08-02'),'cadence_seconds':21600,'target_minutes':HORIZONS,'endpoint_minutes':ENDPOINT_HORIZONS,
        'fit_cutoffs':[epoch('2024-07-22'),epoch('2024-07-24')],
        'features':FEATURES,'calendar_targets':CALENDAR,'scope':'previously_inspected_development_input_preparation',
        'clock':'bar_start_plus60_assumed_close_availability_not_observed_arrival','execution_qualified':False,'broker_access':False}

def clean(frame):
    required={'epoch','mid','bid','ask'}
    if set(frame.columns)!=required:raise ValueError('exact_raw_slice_columns_required')
    if frame.epoch.dtype.kind not in 'iu' or (frame.epoch<0).any() or (frame.epoch%60!=0).any():raise ValueError('integer_minute_starts_required')
    duplicated=frame.epoch.duplicated(keep=False)
    mid_ok=np.isfinite(frame.mid)&(frame.mid>0)
    valid=frame.loc[~duplicated&mid_ok].sort_values('epoch').copy()
    quotes=np.isfinite(valid.bid)&np.isfinite(valid.ask)&(valid.bid>0)&(valid.ask>=valid.bid)&(valid.mid>=valid.bid)&(valid.mid<=valid.ask)
    valid.loc[~quotes,['bid','ask']]=np.nan
    return valid.reset_index(drop=True),{'duplicate_rows_excluded':int(duplicated.sum()),'invalid_mid_rows_excluded':int((~mid_ok).sum()),'invalid_quote_rows':int((~quotes).sum())}

def pair_records(pair,raw,pip,source_member_sha,c=None):
    c=c or contract();frame,quality=clean(raw)
    checked=old._inputs({pair:frame[['epoch','mid']]},{pair:pip},None)[pair]
    technical,_=old._technical(checked)
    technical['epoch']=checked.epoch.to_numpy();technical=technical.set_index('epoch')
    prices=frame.set_index('epoch');labels=old.build_labels({pair:frame},pip_map={pair:pip},horizons_minutes=c['target_minutes'])
    labels=labels.loc[labels.index.get_level_values('epoch').isin(range(c['origin_start']-60,c['origin_end']-60,c['cadence_seconds']))]
    lookup={(int(t),int(h)):row for (_,t,h),row in labels.iterrows()}
    endpoint_data={'time':frame.epoch.to_numpy(),'close':frame.mid.to_numpy(),'bid_close':frame.bid.to_numpy(),'ask_close':frame.ask.to_numpy()}
    endpoints={h:endpoint_outcomes(endpoint_data,h) for h in c['endpoint_minutes']} if len(frame) else {}
    positions={int(t):i for i,t in enumerate(frame.epoch)}
    observations=[];outcomes=[];path_outcomes=[];blocked=[]
    for origin in range(c['origin_start'],c['origin_end'],c['cadence_seconds']):
        raw_epoch=origin-60;key=f'{pair}:{origin}';features=None;reason='missing_exact_reference_close'
        if raw_epoch in technical.index:
            q=prices.loc[raw_epoch];x=technical.loc[raw_epoch]
            if np.isfinite([q.bid,q.ask]).all():
                features=[float(x[n]) for n in old.TECHNICAL_COLUMNS]+[float((q.ask-q.mid)/q.mid*10000),float((q.mid-q.bid)/q.mid*10000)]
                reason='feature_ready_assumed_close'
            else:reason='invalid_or_missing_current_bid_ask'
        observations.append({'record_id':key,'instrument':pair,'origin_epoch':origin,'available_epoch':origin,
            'source_bar_start_epoch':raw_epoch,'source_member_sha256':source_member_sha,
            'feature_schema_sha256':fingerprint(FEATURES),'features':features,'reason':reason})
        for h in c['target_minutes']:
            row=lookup.get((raw_epoch,h));value=None if row is None else float(row.return_bps)
            path_outcomes.append({'record_id':key,'target_id':f'technical_contiguous_midpoint_elapsed_{h}m','label_end_epoch':origin+h*60,
                'available_epoch':origin+h*60,'value':value,'status':'mature_contiguous_endpoint' if row is not None else 'no_contiguous_retained_label',
                'source_member_sha256':source_member_sha,'execution_status':'not_qualified'})
        for h in c['endpoint_minutes']:
            i=positions.get(raw_epoch);v=None if i is None else endpoints[h]['midpoint_return_bps'][i]
            value=float(v) if v is not None and np.isfinite(v) else None
            outcomes.append({'record_id':key,'target_id':f'technical_endpoint_midpoint_elapsed_{h}m','label_end_epoch':origin+h*60,
                'available_epoch':origin+h*60,'value':value,'status':'exact_endpoints_available' if value is not None else 'missing_exact_endpoint',
                'source_member_sha256':source_member_sha,'execution_status':'not_qualified','path_continuity_required':False})
        blocked.extend({'record_id':key,'target_id':target,'status':'blocked','reason':'venue_session_calendar_not_qualified'} for target in c['calendar_targets'])
    return {'instrument':pair,'observations':observations,'outcomes':outcomes,'path_outcomes':path_outcomes,'calendar_coverage':blocked,'quality':quality}

def summarize(parts,c):
    obs=[o for p in parts for o in p['observations']];out=[o for p in parts for o in p['outcomes']];by={o['record_id']:o for o in obs}
    counts={}
    for h in c['endpoint_minutes']:
        target=f'technical_endpoint_midpoint_elapsed_{h}m';rows=[o for o in out if o['target_id']==target]
        counts[str(h)]={'all_origin_rows':len(rows),'available_labels':sum(o['value'] is not None for o in rows),
            'mature_training_rows':{str(t):sum(o['value'] is not None and o['available_epoch']<=t and by[o['record_id']]['origin_epoch']<t and by[o['record_id']]['features'] is not None for o in rows) for t in c['fit_cutoffs']}}
    return {'status':'verified_causal_technical_input_preparation','instruments':len(parts),'origins_per_pair':len(range(c['origin_start'],c['origin_end'],c['cadence_seconds'])),
        'observation_rows':len(obs),'feature_ready_rows':sum(o['features'] is not None for o in obs),'outcome_rows':len(out),'target_support':counts,'contiguous_path_support':{str(h):sum(o['value'] is not None for p in parts for o in p['path_outcomes'] if o['target_id']==f'technical_contiguous_midpoint_elapsed_{h}m') for h in c['target_minutes']},
        'calendar_blocked_rows':sum(len(p['calendar_coverage']) for p in parts),'feature_count':len(FEATURES),
        'feature_reasons':dict(Counter(o['reason'] for o in obs)),'models_fitted':0,'execution_qualified':False,'independent_review':False,
        'source_reuse':'retained_direction_features_v1._technical and build_labels unchanged',
        'limitations':['retrospective_close_availability','endpoint_elapsed_targets_not_contiguous_paths_or_sessions','26technical_cost_features_no_peer_news_pair_onehots','no_forecast_or_policy_claim']}
