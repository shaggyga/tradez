"""Remaining-horizon S5 momentum benchmark from retained exact source bytes.

This is a new uncalibrated management heuristic, not an old model replay. No
network/data-file reads, writes, orders or fitting occur here. The shared mapper
checks its local source hashes. Actual source consumption is a caller receipt.
"""
from decimal import Context, Decimal, localcontext
import hashlib
import json
import math
from pathlib import Path

import oanda_s5_mba_research_capture_v1 as candles

SCHEMA='observed_s5_remaining_momentum_v1_20260909'
POLICY=dict(windows_sec=[30,60],weights=['0.5','0.5'],
    horizon_scaling='weighted_pips_per_sqrt_second_times_sqrt_actual_remaining_seconds',
    midpoint_convention='official_midpoint',sampling_policy='last13_real_S5_rows_span55_to70sec',
    endpoint_policy='exact_real_30_and60_second_endpoints_no_fallback',
    maximum_reference_age_sec=30,maximum_quote_age_sec=5,
    slippage_bps_per_leg='0.1',confidence_floor='0.5',confidence_ceiling='0.9',confidence_scale='0.1',
    confidence_scope='uncalibrated_score_mapping_not_probability')
_POLICY_BYTES=json.dumps(POLICY,sort_keys=True,separators=(',',':')).encode()
FLAGS=dict(research_only=True,can_place_orders=False,can_promote=False,can_authorize=False,
    account_eligible=False,execution_eligible=False,proof_eligible=False,broker_fill_observed=False)


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def epoch(value):
    if type(value) not in (int,float) or not math.isfinite(value) or not 0<value<1e12:
        raise ValueError('momentum_invalid_clock')
    return float(value)


def price(value):
    if type(value) not in (str,int) or len(str(value))>64:
        raise ValueError('momentum_exact_decimal_required')
    try:
        answer=Decimal(value)
    except Exception:
        raise ValueError('momentum_invalid_decimal') from None
    if not answer.is_finite() or not 0<answer<Decimal('1e12'):
        raise ValueError('momentum_positive_finite_decimal_required')
    if len(answer.as_tuple().digits)>32 or not -30<=answer.as_tuple().exponent<=30:
        raise ValueError('momentum_decimal_precision_or_exponent_bound')
    return answer


def build_momentum_candidate(raw,receipt,source_consumption,*,metadata,decision_epoch,target_epoch,quote):
    """Replay inputs available at the cutoff; the plan records computation end.

    Candidate available_epoch means availability of its retained source inputs.
    It does not claim this derived heuristic was computed or published then.
    The management plan must be completed and persisted before future quotes.
    """
    decision,target=epoch(decision_epoch),epoch(target_epoch)
    if not decision<target<=decision+3600:
        raise ValueError('momentum_target_outside_remaining_hour')
    mapping=candles.map_verified_capture(raw,receipt,metadata,'official_midpoint')
    required={'raw_sha256','receipt_sha256','read_started_epoch','read_completed_epoch'}
    if not isinstance(source_consumption,dict) or set(source_consumption)!=required:
        raise ValueError('momentum_source_consumption_required')
    source_consumption=dict(source_consumption)
    observed=epoch(source_consumption['read_completed_epoch'])
    if not receipt['capture_completed_epoch']<=epoch(source_consumption['read_started_epoch'])<=observed<=decision:
        raise ValueError('momentum_source_not_consumed_before_cutoff')
    if source_consumption['raw_sha256']!=mapping['source_sha256'] or source_consumption['receipt_sha256']!=receipt['receipt_sha256']:
        raise ValueError('momentum_consumption_binding_mismatch')
    pair=metadata['instrument']
    context=dict(schema_version=SCHEMA,instrument=pair,decision_epoch=decision,
        original_target_epoch=target,available_epoch=observed,source_available_epoch=observed,
        policy=json.loads(_POLICY_BYTES),
        source_sha256=mapping['source_sha256'],source_receipt_sha256=receipt['receipt_sha256'],
        source_mapping_sha256=mapping['mapping_sha256'],source_consumption=source_consumption,
        source_bindings={**mapping['source_bindings'],Path(__file__).name:hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
        computation_clock_scope='derived_during_plan_computation_actual_completion_retained_by_plan',**FLAGS)
    def refusal(reason):
        body={**context,'status':'unavailable','reason_code':reason}
        return {**body,'refusal_sha256':digest(body)}
    feature_rows=mapping['last13_feature_rows']
    if len(feature_rows)!=13:
        return refusal('momentum_thirteen_real_rows_required')
    first,last=feature_rows[0]['bar_start_epoch'],feature_rows[-1]['bar_start_epoch']
    if not 55<=last-first<=70:
        return refusal('momentum_original_sampling_support_unavailable')
    reference=last+5
    if not reference<=mapping['first_observed_epoch']<=observed<=decision or decision-reference>30:
        return refusal('momentum_reference_stale_or_future')
    rows={r['bar_label_epoch']:r for r in mapping['complete_rows'][-13:]}
    if any(last-window not in rows for window in (30,60)):
        return refusal('momentum_exact_window_endpoint_missing')
    if quote is None:
        return refusal('momentum_current_quote_missing')
    if not isinstance(quote,dict) or quote.get('instrument')!=pair or not isinstance(quote.get('quote_id'),str) or not quote['quote_id']:
        raise ValueError('momentum_quote_identity_mismatch')
    market,available=epoch(quote.get('market_epoch')),epoch(quote.get('available_epoch'))
    if not market<=available<=decision:
        raise ValueError('momentum_quote_not_available_at_cutoff')
    context['available_epoch']=max(observed,available)
    if quote.get('tradeable') is not True:
        return refusal('momentum_current_quote_not_tradeable')
    if decision-market>5:
        return refusal('momentum_current_quote_stale')
    with localcontext(Context(prec=96)):
        bid,ask=price(quote.get('bid')),price(quote.get('ask'))
        pip=price(metadata['pip_size'])
        if bid>=ask:
            return refusal('momentum_positive_quote_spread_required')
        midpoint=(bid+ask)/2
        current=price(rows[last]['mid']['c'])
        windows=[]
        weighted=Decimal(0)
        for window in (30,60):
            prior=price(rows[last-window]['mid']['c'])
            move=(current-prior)/pip
            slope=move/Decimal(window).sqrt()
            weighted+=Decimal('0.5')*slope
            windows.append(dict(window_sec=window,start_price_epoch=last-window+5,end_price_epoch=reference,
                start_price=str(prior),end_price=str(current),signed_move_pips=str(move),
                pips_per_sqrt_second=str(slope)))
        remaining=Decimal(str(target))-Decimal(str(decision))
        expected=weighted*remaining.sqrt()
        if expected==0:
            return refusal('momentum_exactly_neutral')
        spread=(ask-bid)/pip
        slip=midpoint*Decimal('0.1')/Decimal(10000)/pip
        cost=spread+2*slip
        score=abs(expected)/cost
        confidence=min(Decimal('0.9'),max(Decimal('0.5'),Decimal('0.5')+(score-1)*Decimal('0.1')))
        body={**context,'status':'available','reference_price_epoch':reference,
            'reference_age_sec':decision-reference,'sampling_endpoint_span_sec':last-first,
            'remaining_sec':str(remaining),'windows':windows,'weighted_rate':str(weighted),
            'side':1 if expected>0 else -1,'expected_signed_move_pips':str(expected),
            'expected_move_pips':str(abs(expected)),'spread_pips':str(spread),
            'slippage_pips_per_leg':str(slip),'round_trip_cost_pips':str(cost),
            'score':str(score),'confidence':str(confidence),'pip_size':str(pip),
            'decision_quote':{k:quote[k] for k in ('instrument','quote_id','bid','ask','market_epoch','available_epoch','tradeable')}}
    body['snapshot_id']='remaining_momentum_'+digest(body)[:28]
    return {**body,'candidate_sha256':digest(body)}
