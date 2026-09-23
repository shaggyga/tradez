"""Fixed development ablation of a retained event-existence layer, not live issuance."""
from collections import Counter,defaultdict
import datetime as dt
import hashlib
import json
import math
import numpy as np
from threadpoolctl import threadpool_limits
from contracts import TrainingView,fingerprint
from fitted_consumer_v2 import fit_model,issue
from causal_technical_adapter_v2 import clean,FEATURES as TECHNICAL_FEATURES
from retained_direction_features_v1 import _technical
from retained_endpoint_targets_v1 import endpoint_outcomes
from macro_matched_population_v2 import decode_candles,epoch,require
from macro_version_text_v2 import currency_states

GROUPS=('technical','technical_clock','technical_clock_event')
CLOCK_NAMES=('hour_sin','hour_cos','weekday_sin','weekday_cos')
EVENT_NAMES=('visible_event_count','eligible_event_count','unique_text_count','source_count','minimum_source_age_hours_capped840','source_age_missing')
METHODS=('zero','history_mean','technical_ridge','technical_clock_ridge','technical_clock_event_ridge')


def schemas():
    base=list(TECHNICAL_FEATURES);clock=base+list(CLOCK_NAMES)
    return {'technical':base,'technical_clock':clock,'technical_clock_event':clock+[leg+'_'+n for leg in ('base','quote') for n in EVENT_NAMES]}


def calendar_values(origin):
    value=dt.datetime.fromtimestamp(origin,dt.timezone.utc)
    hour=value.hour+value.minute/60
    return [math.sin(2*math.pi*hour/24),math.cos(2*math.pi*hour/24),math.sin(2*math.pi*value.weekday()/7),math.cos(2*math.pi*value.weekday()/7)]


def event_values(state):
    age=state['minimum_source_age_seconds']
    return [float(state[n]) for n in EVENT_NAMES[:4]]+[min(age/3600,840) if age is not None else 0.,float(age is None)]


def prepare(frames,universe,bindings,cache,plan):
    start=int(epoch(plan['training_start']));end=int(epoch(plan['evaluation_end_exclusive']))
    origins=list(range(start,end,3600));currencies=sorted({c for p in universe for c in p.split('_')})
    contexts={};context_refs=[]
    for origin in origins:
        asof=origin-plan['source_asof_delay_seconds']
        states=currency_states(bindings,cache,asof,currencies)
        contexts[origin]={s['currency']:s for s in states}
        context_refs.append({'origin_epoch':origin,'source_asof_epoch':asof,'state_sha256':fingerprint(states),
            'currency_summary':[{k:s[k] for k in ('currency','visible_event_count','eligible_event_count','unique_text_count','source_count','minimum_source_age_seconds')} for s in states]})
    observations={g:[] for g in GROUPS};outcomes=[];quality=[]
    for pair in universe:
        frame,q=clean(frames[pair]);quality.append({'pair':pair,**q,'valid_bars':len(frame)})
        technical,_=_technical(frame[['epoch','mid']]);positions={int(t):i for i,t in enumerate(frame.epoch)}
        data={'time':frame.epoch.to_numpy(),'close':frame.mid.to_numpy(),'bid_close':frame.bid.to_numpy(),'ask_close':frame.ask.to_numpy()}
        endpoints={h:endpoint_outcomes(data,h) for h in plan['horizon_minutes']} if len(frame) else {}
        base_currency,quote_currency=pair.split('_')
        for origin in origins:
            position=positions.get(origin-60);features=None;reason='missing_exact_reference'
            if position is not None:
                row=frame.iloc[position];ta=technical.iloc[position]
                if np.isfinite([row.bid,row.ask]).all():
                    features=[float(ta[n]) for n in TECHNICAL_FEATURES[:-2]]+[(row.ask-row.mid)/row.mid*10000,(row.mid-row.bid)/row.mid*10000]
                    require(np.isfinite(features).all(),'nonfinite_reused_technical_features')
                    reason='retrospective_assumed_close_features'
                else:reason='missing_or_invalid_current_bidask'
            shared={'record_id':f'{pair}:{origin}','instrument':pair,'origin_epoch':origin,'available_epoch':origin,
                'source_asof_epoch':origin-plan['source_asof_delay_seconds'],'feature_reason':reason,
                'availability_basis':'modeled_source_delay_and_bar_end_not_original_issuance'}
            clock=calendar_values(origin)
            context=contexts[origin]
            event=event_values(context[base_currency])+event_values(context[quote_currency])
            for group in GROUPS:
                values=None if features is None else list(features)+(clock if group!='technical' else [])+(event if group=='technical_clock_event' else [])
                observations[group].append({**shared,'features':values})
            for h in plan['horizon_minutes']:
                value=None if position is None else float(endpoints[h]['midpoint_return_bps'][position])
                if value is not None and not math.isfinite(value):value=None
                outcomes.append({'record_id':shared['record_id'],'target_id':f'technical_endpoint_midpoint_elapsed_{h}m',
                    'label_end_epoch':origin+h*60,'available_epoch':origin+h*60,'value':value,
                    'status':'exact_endpoint_available' if value is not None else 'missing_exact_endpoint',
                    'availability_basis':'bar_end_assumption_not_historical_receipt'})
    return observations,outcomes,context_refs,quality


def support(observations,outcomes,view,target):
    lookup={o['record_id']:o for o in outcomes if o['target_id']==target['target_id'] and o['available_epoch']<=view.fit_cutoff_epoch}
    rows=[]
    for r in observations:
        o=lookup.get(r['record_id'])
        if not view.origin_start_epoch<=r['origin_epoch']<view.origin_end_epoch or r['available_epoch']>r['origin_epoch'] or r['features'] is None or o is None or o['value'] is None:continue
        if not np.isfinite(r['features']).all() or not math.isfinite(o['value']):continue
        if view.eligible(np.array([r['origin_epoch']]),np.array([o['available_epoch']]),np.array([o['label_end_epoch']]))[0]:
            rows.append({'record_id':r['record_id'],'value':o['value'],'outcome_available_epoch':o['available_epoch']})
    return sorted(rows,key=lambda x:x['record_id'])


def fit_groups(observations,outcomes,universe,plan):
    cutoff=int(epoch(plan['fit_cutoff']));start=int(epoch(plan['training_start']));end=int(epoch(plan['evaluation_end_exclusive']))
    view=TrainingView(start,cutoff,cutoff,cutoff,end);models={};attempts=[]
    for h in plan['horizon_minutes']:
        target={'target_id':f'technical_endpoint_midpoint_elapsed_{h}m','horizon_seconds':h*60};shared=None
        for group in GROUPS:
            eligible=support(observations[group],outcomes,view,target)
            if shared is None:shared=eligible
            else:require(eligible==shared,'matched_training_support_drift')
            with threadpool_limits(limits=1):
                model=fit_model(observations[group],outcomes,universe=universe,target=target,view=view,
                    ready_epoch=cutoff+plan['modeled_fit_latency_seconds'],ridge_lambda=plan['ridge_lambda'],min_rows=plan['minimum_training_rows'])
            name=f'{group}_{h}m'
            attempts.append({'attempt_id':name,'group':group,'horizon_minutes':h,'status':model['status'],
                'shared_training_rows':len(eligible),'shared_training_support_sha256':fingerprint(eligible),
                'training_origins':len({x['record_id'].rsplit(':',1)[1] for x in eligible}),
                'model_id':model.get('model_id'),'feature_names':schemas()[group],'hyperparameters':{'ridge_lambda':plan['ridge_lambda']},
                'evidence_scope':'inspected_development_model_with_hypothetical_timing'})
            if model['status']=='fitted':
                require(model['training_rows']==len(eligible),'original_fitter_support_mismatch')
                models[name]=model
    require(len(attempts)==plan['attempt_budget']['learned_fits'],'declared_attempt_count_mismatch')
    return models,attempts


def predict(models,observations,plan):
    cutoff=int(epoch(plan['fit_cutoff']));forecasts=[];coverage=[]
    maps={g:{r['record_id']:r for r in observations[g]} for g in GROUPS}
    for h in plan['horizon_minutes']:
        for row in observations['technical']:
            if row['origin_epoch']<cutoff:continue
            key=row['record_id'];origin=row['origin_epoch'];predicted={};reason=None
            for g in GROUPS:
                model=models.get(f'{g}_{h}m')
                f,state=issue(model,maps[g][key],decision_epoch=origin,available_epoch=origin+plan['modeled_prediction_latency_seconds'],procedure='frozen_development')
                if f is None:reason=state;break
                predicted[g+'_ridge']=f
            if reason is None:
                # Both simple controls use the same mature prefix and readiness.
                template=predicted['technical_ridge'];m=models[f'technical_{h}m']
                for method,value in [('zero',0.),('history_mean',m['coefficient'][0])]:
                    model_id=fingerprint({'fit':m['model_id'],'control':method})
                    predicted[method]={**template,'forecast_id':fingerprint({'model':model_id,'record_id':key}),
                        'model_id':model_id,'prediction':float(value)}
            for method in METHODS:
                f=predicted[method] if reason is None else None
                coverage.append({'record_id':key,'pair':row['instrument'],'origin_epoch':origin,'horizon_minutes':h,'method':method,
                    'reason':reason or 'eligible_modeled_development','forecast_id':f['forecast_id'] if f else None})
                if f is not None:forecasts.append({'record_id':key,'method':method,'forecast':f,
                    'scope':'retrospective_development_reconstruction_not_historical_issuance','historical_live_admission':False})
    return forecasts,coverage


def score(forecasts,outcomes,evaluation_asof,plan):
    lookup={(o['record_id'],o['target_id']):o for o in outcomes};groups=defaultdict(list)
    for row in forecasts:
        f=row['forecast'];o=lookup.get((row['record_id'],f['target_id']))
        if o is None or o['value'] is None or o['available_epoch']>evaluation_asof:continue
        groups[f['target_id'],row['method']].append((row['record_id'],f['decision_epoch'],f['prediction'],o['value']))
    result=[];supports={}
    for h in plan['horizon_minutes']:
        target=f'technical_endpoint_midpoint_elapsed_{h}m'
        for method in METHODS:
            rows=sorted(groups[target,method]);keys=[r[0] for r in rows]
            if target in supports:require(keys==supports[target],'matched_evaluation_support_drift')
            else:supports[target]=keys
            byorigin=defaultdict(list);byday=defaultdict(list)
            for key,t,p,y in rows:
                byorigin[t].append(abs(p-y));byday[dt.datetime.fromtimestamp(t,dt.timezone.utc).date().isoformat()].append(abs(p-y))
            result.append({'target_id':target,'method':method,'rows':len(rows),'support_sha256':fingerprint(keys),
                'mae_bps':float(np.mean([abs(p-y) for _,_,p,y in rows])) if rows else None,
                'mse_bps2':float(np.mean([(p-y)**2 for _,_,p,y in rows])) if rows else None,
                'origin_balanced_mae_bps':float(np.mean([np.mean(x) for x in byorigin.values()])) if byorigin else None,
                'utc_day_mae_bps':{d:float(np.mean(v)) for d,v in sorted(byday.items())},
                'distinct_origins':len(byorigin),'independent_confirmation':False})
    return result


def build(blobs):
    read=lambda n:json.loads(blobs[n]);plan=read('EVENT_FORECAST_PLAN.json');capture=read('CANDLE_CAPTURE.json');old_plan=read('MATCHED_POPULATION_PLAN.json')
    for n,h in read('EVENT_PREDECESSOR.json')['payloads'].items():require(hashlib.sha256(blobs[n]).hexdigest()==h,'event_forecast_predecessor_mismatch')
    require(plan['groups']==list(GROUPS) and plan['methods']==list(METHODS) and plan['horizon_minutes']==[60,1440],'undeclared_forecast_arms')
    universe=read('universe.json');require(len(universe)==68 and sorted(set(universe))==universe,'all68_universe_required')
    require(sum(x.get('expanded_bytes',0) for x in capture['pairs'])<=128*1024*1024,'expanded_price_total_limit')
    descriptors={r['pair']:r for r in capture['pairs']};require(sorted(descriptors)==universe,'captured_pair_population_mismatch')
    frames={p:decode_candles(blobs[descriptors[p]['input']],descriptors[p],p,old_plan) for p in universe}
    cache={c['cache_key']:c for c in read('extraction_cache.json')};bindings=read('version_bindings.json')
    observations,outcomes,contexts,quality=prepare(frames,universe,bindings,cache,plan)
    models,attempts=fit_groups(observations,outcomes,universe,plan)
    forecasts,coverage=predict(models,observations,plan)
    scores=score(forecasts,outcomes,int(epoch(old_plan['raw_interval_end_exclusive'])),plan)
    differences=[]
    for h in plan['horizon_minutes']:
        target=f'technical_endpoint_midpoint_elapsed_{h}m';rows={r['method']:r for r in scores if r['target_id']==target}
        event,clock=rows['technical_clock_event_ridge'],rows['technical_clock_ridge']
        differences.append({'horizon_minutes':h,'matched_rows':event['rows'],
            'event_minus_clock_mae_bps':event['mae_bps']-clock['mae_bps'] if event['mae_bps'] is not None else None,
            'event_minus_clock_origin_balanced_mae_bps':event['origin_balanced_mae_bps']-clock['origin_balanced_mae_bps'] if event['origin_balanced_mae_bps'] is not None else None,
            'interpretation':'negative means lower error on this inspected development slice only'})
    report={'schema':'macro_event_existence_forecast.v2','universe_count':68,'feature_counts':{g:len(v) for g,v in schemas().items()},
        'origins':len(contexts),'observation_rows_per_group':len(observations['technical']),'outcome_rows':len(outcomes),
        'base_models_fitted':len(models),'declared_fit_attempts':len(attempts),'forecasts':len(forecasts),'coverage_rows':len(coverage),
        'coverage_reasons':dict(sorted(Counter(r['reason'] for r in coverage).items())),'score_groups':len(scores),'differences':differences,
        'forecast_improvement_proven':False,'historical_live_admission':False,'independent_confirmation':False,'gpt_comparisons':False,
        'limitations':['six prespecified pooled fits on previously inspected short development data; no parameter or winner selection',
            'time-only controls retained; source/event counts may still encode collector or calendar regime',
            'row-weighted pooled models share currency events and overlapping targets; rows are not independent',
            'only a small number of UTC days; no valid independent confidence interval or confirmation',
            'source-asof30seconds, fit30seconds, forecast2seconds and bar-end clocks are hypothetical, not historical receipts',
            'calendar/headline/bootstrap counted as coverage but excluded from eligible detail counts; zero stance support remains null upstream',
            'no actual NLP stance/expectation surprise or paid/GPT calls; event existence only',
            'exact elapsed endpoints are not venue sessions, continuous paths, fills, policy returns or live readiness']}
    return {'models.json':models,'attempts.json':attempts,'observations.json':observations,'outcomes.json':outcomes,
        'context_references.json':contexts,'quality.json':quality,'forecasts.json':forecasts,'coverage.json':coverage,
        'scores.json':scores,'forecast_report.json':report}
