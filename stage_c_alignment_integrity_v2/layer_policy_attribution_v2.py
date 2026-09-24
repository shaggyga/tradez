"""Descriptive joins of immutable forecasts, decisions and closed account ledgers."""
from collections import Counter,defaultdict
from decimal import Decimal,Context,localcontext
import math
from contracts import fingerprint

D=lambda v:Decimal(str(v))


def unique(rows,key,label):
    result={}
    for row in rows:
        k=key(row)
        if k in result:raise ValueError('attribution_duplicate_'+label)
        result[k]=row
    return result


def outcome_value(pred,outcome,asof):
    if (outcome['record_id']!=pred['record_id'] or outcome['target_id']!=pred['target_id'] or
        outcome['label_end_epoch']!=pred['target_epoch'] or outcome['available_epoch']<outcome['label_end_epoch']):
        raise ValueError('attribution_exact_original_outcome_required')
    if outcome['available_epoch']>asof:return None,'not_mature_at_assessment'
    if outcome['value'] is None:return None,'original_label_unavailable'
    value=D(outcome['value'])
    if not value.is_finite():raise ValueError('attribution_nonfinite_outcome')
    return value,'mature'


def metrics(rows):
    mature=[r for r in rows if r['outcome_status']=='mature']
    with localcontext(Context(prec=80)):
        errors=[D(r['error_bps']) for r in mature];n=len(errors)
        nonzero=[r for r in mature if D(r['prediction_bps'])!=0 and D(r['outcome_bps'])!=0]
        return {'rows':len(rows),'unique_forecasts':len({r['forecast_id'] for r in rows}),
          'distinct_origins':len({r['origin_epoch'] for r in rows}),'distinct_pairs':len({r['instrument'] for r in rows}),
          'mature_rows':n,'mae_bps':str(sum(map(abs,errors))/n) if n else None,
          'mse_bps2':str(sum(x*x for x in errors)/n) if n else None,'bias_bps':str(sum(errors)/n) if n else None,
          'direction_nonzero_rows':len(nonzero),'direction_matches':sum(D(r['prediction_bps'])*D(r['outcome_bps'])>0 for r in nonzero),
          'support_sha256':fingerprint(sorted(r['forecast_id'] for r in mature))}


def forecast_table(frames,outcomes,c):
    labels=unique(outcomes,lambda r:(r['record_id'],r['target_id']),'outcome')
    predictions={};coverage=[];rows=[]
    if set(frames)!=set(map(str,c['origins'])):raise ValueError('attribution_exact_origin_inventory')
    for origin in c['origins']:
        frame=frames[str(origin)];cov=frame['coverage'];expected={(p,m) for p in c['universe'] for m in c['methods']}
        actual={(r['instrument'],r['base_method']+'__'+r['variant']) for r in cov}
        if len(cov)!=len(expected) or actual!=expected:raise ValueError('attribution_all68_coverage_required')
        coverage.extend(cov)
        current=unique(frame['predictions'],lambda r:r['forecast_id'],'forecast')
        if len(current)!=sum(r['reason']=='eligible' for r in cov):raise ValueError('attribution_eligible_prediction_inventory')
        for fid,p in current.items():
            method=p['base_method']+'__'+p['variant'];unsigned={k:v for k,v in p.items() if k!='forecast_id'}
            if (fingerprint(unsigned)!=fid or p['decision_epoch']!=origin or p['record_id']!=p['instrument']+':'+str(origin) or
                p['target_epoch']!=c['target_epoch'] or p['available_epoch']!=origin+2 or method not in c['methods']):
                raise ValueError('attribution_forecast_identity_clock_mismatch')
            if fid in predictions:raise ValueError('attribution_duplicate_forecast')
            predictions[fid]=p
            o=labels.get((p['record_id'],p['target_id']))
            if o is None:raise ValueError('attribution_required_original_outcome_missing')
            value,status=outcome_value(p,o,c['assessment_asof']);pred=D(p['prediction_bps'])
            if not pred.is_finite():raise ValueError('attribution_nonfinite_prediction')
            with localcontext(Context(prec=80)):error=pred-value if value is not None else None
            rows.append({'forecast_id':fid,'method':method,'record_id':p['record_id'],'instrument':p['instrument'],
              'origin_epoch':origin,'target_epoch':p['target_epoch'],'target_id':p['target_id'],'prediction_bps':str(pred),
              'outcome_bps':str(value) if value is not None else None,'error_bps':str(error) if error is not None else None,
              'outcome_status':status,'outcome_available_epoch':o['available_epoch'],'outcome_sha256':fingerprint(o)})
    by=unique(rows,lambda r:r['forecast_id'],'assessment')
    groups={m:metrics([r for r in rows if r['method']==m]) for m in c['methods']}
    return {'rows':rows,'coverage':coverage,'metrics':groups,'scope':'all_eligible_current_forecasts; dependent_inspected_development'},predictions,by


def bind_candidate(candidate,decision,predictions,method,target):
    payload=candidate['source_payload'];bindings=payload['source_bindings'];fid=bindings['prediction']
    if fid not in predictions:raise ValueError('attribution_unknown_selected_forecast')
    p=predictions[fid]
    expected={'model':p['model_id'],'signed_fit':p['signed_fit_id'],'absolute_fit':p['absolute_fit_id'],
      'feature_observation':p['observation_sha256'],'eligibility_snapshot':p['eligibility_snapshot_id']}
    if (any(bindings.get(k)!=v for k,v in expected.items()) or p['base_method']+'__'+p['variant']!=method or
        candidate['instrument']!=p['instrument'] or decision['epoch']!=p['available_epoch'] or
        candidate['decision_epoch']!=decision['epoch'] or candidate['original_target_epoch']!=target or p['target_epoch']!=target or
        payload['issued_epoch']!=p['available_epoch'] or payload['model_id']!=p['model_id'] or
        candidate['side']!=(1 if D(p['prediction_bps'])>0 else -1 if D(p['prediction_bps'])<0 else 0)):
        raise ValueError('attribution_selected_forecast_binding_mismatch')
    return fid


def account_reconcile(arm,events,report,tolerance):
    state=report['arms'][arm];realized=defaultdict(Decimal);fees=Decimal(0);financing=Decimal(0);opens=[];rejections=[]
    with localcontext(Context(prec=80)):
        for e in events:
            if e.get('arm')!=arm:continue
            r=e['receipt']
            if r.get('status')=='rejected':rejections.append({'epoch':e['epoch'],'kind':e['kind'],'reason':r.get('reason')})
            if e['kind']=='fill' and r['status']=='filled':
                fees+=D(r['fee_usd'])
                for leg in r['legs']:
                    if leg['kind']=='open':opens.append({'epoch':e['epoch'],**leg})
                    elif leg['kind']!='close':raise ValueError('attribution_unknown_fill_leg')
                    realized[leg['instrument']]+=D(leg['realized_usd'])
            elif e['kind']=='financing' and r['status']=='financing_applied':financing+=D(r['amount_usd'])
        gross=sum(realized.values());net=gross+financing-fees
        if state['open_lot_count']!=0 or D(state['pending_order_units'])!=0 or state['unrealized_usd'] is None or D(state['unrealized_usd'])!=0:
            raise ValueError('attribution_frozen_closed_parent_account_required')
        for actual,key in ((gross,'realized_usd'),(fees,'fees_usd'),(financing,'financing_usd'),(net,'net_account_pnl_usd')):
            if state[key] is None or abs(actual-D(state[key]))>D(tolerance):raise ValueError('attribution_ledger_reconciliation_'+key)
    return {'realized_usd_by_instrument':{k:str(v) for k,v in sorted(realized.items())},'realized_usd':str(gross),
      'fill_fees_usd':str(fees),'financing_usd':str(financing),'net_account_pnl_usd':str(net),
      'rejected_receipts':rejections,'financing_application_complete':not any(x['kind']=='financing' for x in rejections),
      'fee_allocation':'per_account_fill; no_arbitrary_instrument_fee_allocation'},opens


def bind_original_candidate(raw_candidates,candidate):
    originals=unique(raw_candidates,lambda r:r['node_id'],'native_candidate')
    original=originals.get(candidate['candidate_id'])
    if original is None or original!=candidate['source_payload'] or any(
        str(original[k])!=str(candidate[k]) for k in ('instrument','side','expected_terminal_price')):
        raise ValueError('attribution_selected_candidate_not_in_original_frame')
    return len(originals)


def path_attribution(name,inputs,decisions,events,memory,report,predictions,assessment,c):
    method,scenario=name.removesuffix('-reference').split('-later_candle_',1);scenario='later_candle_'+scenario
    if method not in c['methods'] or scenario not in c['scenarios']:raise ValueError('attribution_registered_path_required')
    bydecision=unique(decisions,lambda r:r['decision_id'],'decision')
    unique(events,lambda r:r['event_id'],'event')
    if len(decisions)!=54 or set(report['arms'])!=set(c['policies']):raise ValueError('attribution_all_decisions_arms_required')
    frames=unique(inputs['frames'],lambda r:(r['kind'],r['epoch']),'frame')
    selections=[];arms=[]
    for decision in decisions:
        if decision['action'] not in ('ENTER','REPLACE'):continue
        candidate=decision['candidate'];fid=bind_candidate(candidate,decision,predictions,method,c['target_epoch'])
        frame=frames['decision',decision['epoch']]
        count=bind_original_candidate(frame['candidates'],candidate)
        alternatives=decision['values'].get('alternatives',[])
        selections.append({'decision_id':decision['decision_id'],'arm':decision['arm'],'action':decision['action'],
          'epoch':decision['epoch'],'forecast_id':fid,'instrument':candidate['instrument'],'side':candidate['side'],
          'declared_decision_notional_usd':candidate['sizing']['decision_value_usd'],
          'expected_gross_usd':candidate['expected_gross_usd'],'round_trip_cost_usd':candidate['round_trip_cost_usd'],
          'new_entry_net_usd':candidate['new_entry_net_usd'],'available_candidate_count':count,
          'account_alternative_count':len(alternatives),'forecast_assessment':assessment[fid],
          'intent_status':decision.get('intent_receipt',{}).get('status'),
          'intent_order_action':decision.get('intent_receipt',{}).get('order',{}).get('action'),
          'filled_episode':any(e['entry_decision']==decision['decision_id'] for e in memory[decision['arm']]['episodes'])})
    for arm in c['policies']:
        ledger,opens=account_reconcile(arm,events,report,c['tolerances']['account_usd']);episodes=memory[arm]['episodes'];used=set()
        pair_counts=Counter();currency_counts=Counter();episode_rows=[]
        for episode in episodes:
            d=bydecision.get(episode['entry_decision'])
            if d is None or d['arm']!=arm or d['action'] not in ('ENTER','REPLACE'):raise ValueError('attribution_episode_selection_missing')
            candidate=d['candidate'];fid=bind_candidate(candidate,d,predictions,method,c['target_epoch'])
            matches=[i for i,o in enumerate(opens) if o['epoch']==d['epoch']+58 and o['instrument']==candidate['instrument'] and o['side']==candidate['side'] and o['base_units']==candidate['base_units']]
            if len(matches)!=1 or matches[0] in used:raise ValueError('attribution_episode_actual_fill_mismatch')
            used.add(matches[0]);pair_counts[candidate['instrument']]+=1;currency_counts.update(candidate['instrument'].split('_'))
            episode_rows.append({'entry_decision':d['decision_id'],'forecast_id':fid,'instrument':candidate['instrument'],
              'side':candidate['side'],'entry_fill_epoch':opens[matches[0]]['epoch'],'closed_epoch':episode['closed_epoch'],
              'replacement_parent':episode['replacement_parent']})
        if len(used)!=len(opens):raise ValueError('attribution_unmatched_actual_open_fill')
        selected=[s for s in selections if s['arm']==arm];ids=sorted({s['forecast_id'] for s in selected})
        filled_ids=sorted({s['forecast_id'] for s in selected if s['filled_episode']})
        reasons=Counter(d['reason'] for d in decisions if d['arm']==arm)
        n=len(episodes)
        with localcontext(Context(prec=80)):hhi=str(sum(D(v)*D(v) for v in pair_counts.values())/(n*n)) if n else None
        arms.append({'arm':arm,**ledger,'filled_episodes':episode_rows,'pair_episode_counts':dict(pair_counts),
          'currency_episode_participation':dict(currency_counts),'episode_count':n,'pair_episode_count_hhi':hhi,
          'concentration_scope':c['concentration_scope'],'selected_decisions':len(selected),'filled_selected_decisions':sum(s['filled_episode'] for s in selected),
          'selected_unique_forecast_metrics':metrics([assessment[f] for f in ids]),'decision_reasons':dict(reasons),
          'filled_unique_forecast_metrics':metrics([assessment[f] for f in filled_ids]),
          'selected_without_open_episode':len(selected)-sum(s['filled_episode'] for s in selected),
          'unavailable_liquidation_decisions':reasons['liquidation_wealth_unavailable_no_discretionary_action'],
          'rejected_events':report['arms'][arm]['rejected_events']})
    return {'path':name,'method':method,'scenario':scenario,'arms':arms,'selections':selections,
      'scope':'recorded_forecasts_decisions_and_actual_simulated_fills; no_counterfactual_or_causal_effect'}
