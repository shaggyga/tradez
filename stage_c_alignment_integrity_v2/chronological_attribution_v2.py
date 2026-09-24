"""Cohort-specific descriptive attribution; no model or policy execution."""
from collections import Counter
from decimal import Context,localcontext
from layer_policy_attribution_v2 import D,unique,metrics,forecast_table as original_forecast_table,bind_candidate as original_bind_candidate,bind_original_candidate,account_reconcile
from chronological_policy_report_v2 import execution_diagnostics


def forecast_table(frames,outcomes,c):
    for origin,frame in frames.items():
        if frame['cohort']!=c['cohort'] or frame['target_epoch']!=c['target_epoch'] or frame['origin_epoch']!=int(origin):
            raise ValueError('chronological_attribution_frame_cohort_target')
        if any(p.get('cohort')!=c['cohort'] for p in frame['predictions']):
            raise ValueError('chronological_attribution_prediction_cohort')
    return original_forecast_table(frames,outcomes,c)


def candidate_side(node):
    with localcontext(Context(prec=80)):
        quote=node['decision_quote'];mid=(D(quote['bid'])+D(quote['ask']))/2
        edge=D(node['expected_terminal_price'])-mid
    side=1 if edge>0 else -1 if edge<0 else 0
    if node['side']!=side:raise ValueError('chronological_attribution_rank_candidate_binding')
    return side


def bind_candidate(candidate,decision,predictions,method,target):
    node=candidate['source_payload'];side=candidate_side(node)
    if candidate['side']!=side:raise ValueError('chronological_attribution_selected_side_binding')
    fid=node['source_bindings']['prediction'];value=D(predictions[fid]['prediction_bps'])
    # Preserve the legacy identity checks while validating actual quote-relative
    # side above. Its sign assumption uses a different forecast reference price.
    return original_bind_candidate({**candidate,'side':1 if value>0 else -1 if value<0 else 0},decision,predictions,method,target)


def endpoint_rank(raw,candidate,predictions,assessment):
    bind_original_candidate(raw,candidate)
    values={}
    for node in raw:
        fid=node['source_bindings']['prediction']
        if fid in values:raise ValueError('chronological_attribution_duplicate_rank_forecast')
        if fid not in predictions or fid not in assessment:raise ValueError('chronological_attribution_unknown_rank_forecast')
        p=predictions[fid];a=assessment[fid]
        if node['instrument']!=p['instrument'] or node['issued_epoch']!=p['available_epoch']:
            raise ValueError('chronological_attribution_rank_candidate_binding')
        side=candidate_side(node)
        values[fid]=D(a['outcome_bps'])*side if a['outcome_status']=='mature' else None
    selected=candidate['source_payload']['source_bindings']['prediction'];value=values[selected]
    mature=[v for v in values.values() if v is not None]
    better=sum(v>value for v in mature) if value is not None else None
    ties=sum(v==value for v in mature) if value is not None else None
    return {'selected_directional_endpoint_bps':str(value) if value is not None else None,
        'rank_best_1':better+1 if better is not None else None,'strictly_better':better,'tied_including_selected':ties,
        'mature_candidates':len(mature),'unavailable_candidates':len(values)-len(mature),
        'scope':'retrospective_original_candidate_endpoint_midpoint_rank; not_fill_return_or_counterfactual_profit'}


def path_attribution(name,inputs,decisions,events,memory,report,predictions,assessment,c):
    method,scenario=name.removesuffix('-reference').split('-'+c['cohort']+'_candle_',1);scenario=c['cohort']+'_candle_'+scenario
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
          'endpoint_rank':endpoint_rank(frame['candidates'],candidate,predictions,assessment),
          'filled_episode':any(e['entry_decision']==decision['decision_id'] for e in memory[decision['arm']]['episodes'])})
    for arm in c['policies']:
        ledger,opens=account_reconcile(arm,events,report,c['tolerances']['account_usd']);episodes=memory[arm]['episodes'];used=set()
        diagnostics=execution_diagnostics([d for d in decisions if d['arm']==arm],episodes,events,arm,report['arms'][arm])
        links=unique(diagnostics['entry_fill_links'],lambda x:x['decision_id'],'entry_link')
        pair_counts=Counter();currency_counts=Counter();episode_rows=[]
        for episode in episodes:
            d=bydecision.get(episode['entry_decision'])
            if d is None or d['arm']!=arm or d['action'] not in ('ENTER','REPLACE'):raise ValueError('attribution_episode_selection_missing')
            candidate=d['candidate'];fid=bind_candidate(candidate,d,predictions,method,c['target_epoch'])
            link=links[d['decision_id']]
            matches=[i for i,o in enumerate(opens) if o['epoch']==link['fill_epoch'] and o['position']['candidate_id']==link['order_id']]
            if len(matches)!=1 or matches[0] in used:raise ValueError('attribution_episode_actual_fill_mismatch')
            used.add(matches[0]);pair_counts[candidate['instrument']]+=1;currency_counts.update(candidate['instrument'].split('_'))
            episode_rows.append({'entry_decision':d['decision_id'],'forecast_id':fid,'instrument':candidate['instrument'],
              'side':candidate['side'],'entry_fill_epoch':opens[matches[0]]['epoch'],'closed_epoch':episode['closed_epoch'],
              'replacement_parent':episode['replacement_parent'],'order_id':link['order_id'],'delay_seconds':link['delay_seconds']})
        if len(used)!=len(opens):raise ValueError('attribution_unmatched_actual_open_fill')
        selected=[s for s in selections if s['arm']==arm];ids=sorted({s['forecast_id'] for s in selected})
        filled_ids=sorted({s['forecast_id'] for s in selected if s['filled_episode']})
        reasons=Counter(d['reason'] for d in decisions if d['arm']==arm)
        n=len(episodes)
        with localcontext(Context(prec=80)):hhi=str(sum(D(v)*D(v) for v in pair_counts.values())/(n*n)) if n else None
        arms.append({'arm':arm,**ledger,'delayed_open_fills':diagnostics['delayed_open_fills'],'filled_episodes':episode_rows,'pair_episode_counts':dict(pair_counts),
          'currency_episode_participation':dict(currency_counts),'episode_count':n,'pair_episode_count_hhi':hhi,
          'concentration_scope':c['concentration_scope'],'selected_decisions':len(selected),'filled_selected_decisions':sum(s['filled_episode'] for s in selected),
          'selected_unique_forecast_metrics':metrics([assessment[f] for f in ids]),'decision_reasons':dict(reasons),
          'filled_unique_forecast_metrics':metrics([assessment[f] for f in filled_ids]),
          'selected_without_open_episode':len(selected)-sum(s['filled_episode'] for s in selected),
          'unavailable_liquidation_decisions':reasons['liquidation_wealth_unavailable_no_discretionary_action'],
          'rejected_events':report['arms'][arm]['rejected_events']})
    return {'path':name,'cohort':c['cohort'],'method':method,'scenario':scenario,'arms':arms,'selections':selections,
      'scope':'recorded_forecasts_decisions_and_actual_simulated_fills; no_counterfactual_or_causal_effect'}
