"""Offline candidate: economic validation of an unchanged prepared practice order.

No I/O, broker action, repricing, daily-counter rewrite or retry permission.
The original broker still owns account identity, durable single-use claim and
at-most-once POST/reconciliation. Caller authenticates all source/read contexts.
Stops retain their original validated ATR provenance; they are not recomputed
or moved with every fresh quote. Fresh risk, protective side and costs must pass.
"""
from copy import deepcopy
from decimal import Decimal as D, localcontext
import hashlib, json, re
import base_policy as base

SCHEMA='immutable_practice_order_economic_preflight_candidate_v2_20260913'
BASE_POLICY_SHA='3156c910db6bd3fe3bd0ac9ffe4b232a8173eec82a3da8f81f2d339f3ccfecd1'

def digest(x): return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def require(ok,reason):
    if not ok: raise base.PolicyError(reason)
def assessment(context,decision):
    return base.evaluate_entry(context['signal'],context['quote'],context['account'],context['metadata'],context['home_conversions'],context['candles'],session=context['session'],decision_epoch=decision)

def validate_immutable_order(original_context,revalidation,*,expected_account_sha256,expected_trial_id,transport_state='unknown'):
    """Caller may use a pass only in the SAME never-posted invocation.

    Historical not_submitted, attempted/unknown or terminal intents never gain
    retry permission from this result. transport_state is caller-attested, not
    an authenticated receipt. Existing broker checks remain mandatory.
    """
    try:
        owned,input_sha=base._owned({'original':original_context,'revalidation':revalidation})
        c=owned['original'];r=owned['revalidation'];p=r['prepared'];o=p['order']
        with localcontext(base.CTX):
            require(transport_state=='not_started_in_current_invocation','transport_already_attempted_or_unknown')
            base._sha(expected_account_sha256,'expected_account');require(type(expected_trial_id)is str and re.fullmatch(r'practice007_native_v7_[a-z0-9_]{1,19}',expected_trial_id) is not None,'trial_identity')
            body=deepcopy(p);seal=body.pop('intent_sha256',None)
            require(seal==digest(body),'prepared_seal')
            require(p['schema_version']=='practice_trial_broker_v1_20260909' and p['environment']=='practice' and p['trial_id']==expected_trial_id and p['account_sha256']==expected_account_sha256,'prepared_authority_identity')
            require(o['type']=='MARKET' and o['timeInForce']=='FOK' and o['positionFill']=='OPEN_ONLY' and o['stopLossOnFill']['timeInForce']=='GTC' and 'takeProfitOnFill' not in o,'prepared_order_shape')
            original=assessment(c,c['decision_epoch'])
            require(original['status']=='available' and original==c['assessment'],'original_policy_reconstruction')
            signal=c['signal'];pair=signal['instrument'];side=signal['side']
            require(o['instrument']==pair and p['original_target_epoch']==signal['original_target_epoch'],'original_pair_target')
            require(D(o['units'])==D(original['units']) and D(o['priceBound'])==D(original['entry_price_bound']) and D(o['stopLossOnFill']['price'])==D(original['stop_loss_price']),'prepared_does_not_match_original_assessment')
            identity=digest([expected_trial_id,signal['forecast_sha256'],pair,side,signal['original_target_epoch']])
            require(p['intent_id']==identity,'original_forecast_intent_identity')
            ext={'id':'pt1-'+digest([expected_trial_id,identity])[:40],'tag':'practice_trial_v1','comment':expected_trial_id+'|target='+repr(float(p['original_target_epoch']))}
            require(o['clientExtensions']==o['tradeClientExtensions']==ext,'original_client_identity')
            decision=r['check']['information_cutoff_epoch'];now=base._epoch(decision);created=base._epoch(p['prepared_epoch'])
            require(base._epoch(c['decision_epoch'])<=created<=now and now-created<=15,'prepared_age_or_order')
            for k in ('started_epoch','start_nav_usd','stop_epoch','day_utc','entries_today','last_entry_epoch_by_instrument'):
                require(r['session'][k]==c['session'][k],'original_preclaim_session_context')
            flat=r['flatness'];require(flat['open_trades']['rows']==[] and flat['pending_orders']['rows']==[],'fresh_flatness_required')
            for k in ('open_trades','pending_orders'):base._fresh(flat[k]['observed_epoch'],now,15,'flatness')
            quote=r['pricing']['quotes'][pair]
            fresh_context=dict(c,quote=quote,account=r['account'],home_conversions=r['pricing']['home_conversions'],session=r['session'])
            fresh=assessment(fresh_context,decision)
            require(fresh['status']=='available','fresh_policy_'+str(fresh.get('reason')))
            bid,ask,mid=base._quote(quote,pair,now)
            rate,minimum,maximum,position_max,increment,tick,pip=base._metadata(c['metadata'],pair)
            gain,loss,position_value=base._conversion(r['pricing']['home_conversions'],pair.split('_')[1],now)
            nav,used,available,account_rate,trades,pending=base._account(r['account'],now)
            units=abs(D(o['units']));bound=D(o['priceBound']);stop=D(o['stopLossOnFill']['price']);slip=mid*D('.1')/10000
            require(units>=minimum and units<=maximum and (position_max==0 or units<=position_max) and units%increment==0,'immutable_units_invalid')
            require(bound%tick==0 and stop%tick==0,'immutable_price_precision')
            entry=ask if side==1 else bid
            require(D(side)*(bound-entry)>=0,'immutable_price_bound_not_executable')
            require((bid-stop if side==1 else stop-ask)>0 and D(side)*(bound-stop)>0,'immutable_stop_not_protective')
            # At least the declared entry allowance; include any extra distance
            # allowed by the immutable priceBound when current price improves.
            cost=(ask-bid)+slip+max(slip,D(side)*(bound-entry))
            gross=D(side)*(D(signal['expected_terminal_price'])-mid)
            require(gross>=2*cost,'immutable_cost_hurdle')
            risk=units*(D(side)*(bound-stop)+slip)*loss
            margin=units*max(ask,bound)*position_value*max(rate,account_rate)
            require(risk<=D(fresh['risk_budget_usd']),'immutable_risk_exceeds_fresh_budget')
            require(margin<=D(fresh['margin_budget_usd']),'immutable_margin_exceeds_fresh_budget')
            result=dict(status='economically_valid_unchanged_order',reason='fresh_limits_pass_for_original_order',prepared_intent_sha256=seal,
                instrument=pair,original_target_epoch=p['original_target_epoch'],units=o['units'],price_bound=o['priceBound'],stop_loss_price=o['stopLossOnFill']['price'],
                fresh_modeled_loss_usd=str(risk),fresh_risk_budget_usd=fresh['risk_budget_usd'],fresh_modeled_margin_usd=str(margin),fresh_margin_budget_usd=fresh['margin_budget_usd'],
                fresh_expected_net_usd=str(units*(gross-cost)*gain),fresh_modeled_cost_usd=str(units*cost*gain),fresh_policy_assessment_sha256=fresh['assessment_sha256'],
                original_policy_assessment_sha256=original['assessment_sha256'],information_cutoff_epoch=decision,inputs_sha256=input_sha)
    except (KeyError,TypeError,ValueError,ArithmeticError) as exc:
        result=dict(status='refused',reason=str(exc) if isinstance(exc,base.PolicyError) else 'malformed_input')
    result.update(schema_version=SCHEMA,base_policy_source_sha256=BASE_POLICY_SHA,broker_action_performed=False,can_authorize=False,may_retry_existing_intent=False,
        scope='Offline economic candidate only; original stop/target unchanged. Caller authenticates I/O and broker enforces single-use claim/POST; no counter reset.')
    result['validation_sha256']=digest(result)
    return result
