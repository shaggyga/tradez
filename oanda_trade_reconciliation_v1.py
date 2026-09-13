"""Pure exact-identity trade-state and protective-update checks; no transport.

Position-state confirmation is distinct from attribution of an order fill.
Unknown observations never imply a zero balance or permission to open a flip.
"""
from copy import deepcopy
from datetime import datetime
from decimal import Context, Decimal, localcontext
from functools import wraps
import math
import re

SCHEMA='trade_reconciliation_v1_20260909'
PROTECTIVE_QUOTE_MAX_AGE_SEC=30.0


def fixed_decimal_context(function):
    @wraps(function)
    def checked(*args,**kwargs):
        with localcontext(Context(prec=192)):
            return function(*args,**kwargs)
    return checked


def number(value):
    if isinstance(value,bool) or not isinstance(value,(str,int,float,Decimal)):
        raise ValueError('invalid_numeric_type')
    text=str(value)
    if len(text)>96:raise ValueError('numeric_bound')
    result=Decimal(text)
    if not result.is_finite() or abs(result.adjusted())>24:raise ValueError('invalid_numeric_value')
    return result


def identity(trade):
    if not isinstance(trade,dict):raise ValueError('invalid_trade_shape')
    tid=trade.get('id');instrument=trade.get('instrument')
    if not isinstance(tid,str) or not tid.strip() or len(tid)>128:raise ValueError('invalid_trade_id')
    if not isinstance(instrument,str) or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',instrument):raise ValueError('invalid_instrument')
    if instrument[:3]==instrument[4:]:raise ValueError('invalid_currency_pair')
    units=number(trade.get('currentUnits'))
    if units==0:raise ValueError('zero_open_trade_units')
    return tid,instrument,units


def unknown(reason,*,observed_epoch=None):
    return dict(schema_version=SCHEMA,status='unknown',trade=None,reason_code=reason,
                observed_epoch=observed_epoch,position_state_only=True)


def observe_trade_rows(rows,*,trade_id='',instrument='',direction='',observed_epoch):
    try:
        epoch=float(observed_epoch)
        if isinstance(observed_epoch,bool) or not math.isfinite(epoch) or epoch<=0:raise ValueError('invalid_observation_clock')
        if not isinstance(rows,list) or len(rows)>10000:raise ValueError('invalid_complete_trade_list')
        checked=[(identity(row),row) for row in rows]
        if len({item[0][0] for item in checked})!=len(checked):raise ValueError('duplicate_trade_id')
        if not isinstance(trade_id,str) or not isinstance(instrument,str) or not isinstance(direction,str):raise ValueError('invalid_lookup')
        tid=trade_id.strip();inst=instrument.strip().upper();side=direction.strip().upper()
        if side not in {'','LONG','SHORT'}:raise ValueError('invalid_direction')
        if not tid and not inst:raise ValueError('lookup_identity_required')
        matches=[]
        for (row_id,row_inst,units),row in checked:
            if tid and row_id!=tid:continue
            if inst and row_inst!=inst:
                if tid:raise ValueError('exact_id_instrument_conflict')
                continue
            if side and (units>0)!=(side=='LONG'):
                if tid:raise ValueError('exact_id_side_conflict')
                continue
            matches.append(row)
        if len(matches)>1:raise ValueError('ambiguous_instrument_lookup')
        return dict(schema_version=SCHEMA,status='confirmed_open' if matches else 'confirmed_closed',
            trade=deepcopy(matches[0]) if matches else None,reason_code='complete_exact_lookup',observed_epoch=epoch,
            requested_trade_id=tid,requested_instrument=inst,position_state_only=True)
    except (ValueError,TypeError,ArithmeticError) as error:
        return unknown(str(error),observed_epoch=observed_epoch)


@fixed_decimal_context
def reconcile_reduction(baseline,observation,*,requested_units=None):
    try:
        tid,inst,before=identity(baseline)
        requested=abs(before) if requested_units is None else number(requested_units)
        if requested<=0 or requested>abs(before):raise ValueError('invalid_requested_reduction')
        if not isinstance(observation,dict) or observation.get('schema_version')!=SCHEMA:raise ValueError('invalid_observation')
        if observation.get('status')=='unknown':raise ValueError('recheck_unknown')
        if observation.get('requested_trade_id')!=tid:raise ValueError('recheck_identity_mismatch')
        if observation.get('status')=='confirmed_closed':
            after=Decimal(0)
        elif observation.get('status')=='confirmed_open':
            after_id,after_inst,after=identity(observation.get('trade'))
            if (after_id,after_inst)!=(tid,inst) or (after>0)!=(before>0):raise ValueError('recheck_position_conflict')
        else:raise ValueError('invalid_recheck_status')
        with localcontext() as context:
            context.prec=192
            actual=abs(before)-abs(after)
        if actual!=requested:raise ValueError('observed_reduction_differs_from_requested')
        return dict(status='confirmed_reduction',requested_units=str(requested),observed_reduction_units=str(actual),
            remaining_units=str(after),original_units=str(before),trade_id=tid,instrument=inst,
            observed_epoch=observation.get('observed_epoch'),transaction_attribution=False,
            complete_close=after==0)
    except (ValueError,TypeError,ArithmeticError) as error:
        return dict(status='unknown',reason_code=str(error),transaction_attribution=False,complete_close=False)


@fixed_decimal_context
def protective_update(trade,action,quote,*,instrument,pip_size,display_precision,observed_epoch):
    """Validate the exact rounded values sent to the existing dependent-order API."""
    tid,inst,units=identity(trade)
    if inst!=instrument or not isinstance(action,dict) or not isinstance(quote,dict):raise ValueError('protective_identity')
    if quote.get('instrument')!=inst:raise ValueError('protective_quote_instrument')
    tradeable=quote.get('tradeable')
    if tradeable is not None and tradeable is not True:raise ValueError('protective_quote_not_tradeable')
    if tradeable is not True and quote.get('status')!='tradeable':raise ValueError('protective_quote_tradeability_unknown')
    if quote.get('status') not in (None,'tradeable'):raise ValueError('protective_quote_not_tradeable')
    stamp=quote.get('time')
    if not isinstance(stamp,str):raise ValueError('protective_quote_clock')
    parsed=datetime.fromisoformat(stamp.replace('Z','+00:00'))
    if parsed.tzinfo is None:raise ValueError('protective_quote_clock')
    age=float(observed_epoch)-parsed.timestamp()
    if not math.isfinite(age) or not 0<=age<=PROTECTIVE_QUOTE_MAX_AGE_SEC:raise ValueError('protective_quote_stale_or_future')
    bids=quote.get('bids');asks=quote.get('asks')
    bid=number(bids[0]['price'] if isinstance(bids,list) and bids else quote.get('closeoutBid'))
    ask=number(asks[0]['price'] if isinstance(asks,list) and asks else quote.get('closeoutAsk'))
    if not 0<bid<=ask:raise ValueError('protective_quote_prices')
    pip=number(pip_size)
    if pip<=0 or isinstance(display_precision,bool) or not isinstance(display_precision,int) or not 0<=display_precision<=12:raise ValueError('protective_metadata')
    side=1 if units>0 else -1;exit_price=bid if side>0 else ask
    stops=[]
    for key in ('stopLossOrder','guaranteedStopLossOrder'):
        order=trade.get(key)
        if order:
            if not isinstance(order,dict):raise ValueError('existing_stop_shape')
            value=number(order.get('price'))
            if value<=0:raise ValueError('existing_stop_price')
            stops.append(value)
    trailing=trade.get('trailingStopLossOrder') or {}
    if not isinstance(trailing,dict):raise ValueError('existing_trailing_shape')
    if trailing:
        old_trailing=number(trailing.get('trailingStopValue'))
        if old_trailing<=0:raise ValueError('existing_trailing_level_unknown')
        stops.append(old_trailing)
    def rounded(value):
        # Same exact float formatting convention as the existing transport fmt_price.
        return number(format(float(number(value)),f'.{display_precision}f'))
    stop=rounded(action['stop_loss']) if action.get('stop_loss') is not None else None
    tp_value=action.get('take_profit') if action.get('take_profit') is not None else action.get('tp1')
    target=rounded(tp_value) if tp_value is not None else None
    trail=rounded(number(action['trailing_stop_pips'])*pip) if action.get('trailing_stop_pips') is not None else None
    if stop is None and target is None and trail is None:raise ValueError('no_protective_update')
    if stop is not None:
        if stop<=0 or side*(exit_price-stop)<=0:raise ValueError('protective_stop_crosses_market')
        if any(side*(stop-old)<0 for old in stops):raise ValueError('protective_stop_would_loosen')
    if target is not None and (target<=0 or side*(target-exit_price)<=0):raise ValueError('protective_target_wrong_side')
    if trail is not None:
        if trail<=0:raise ValueError('protective_trailing_distance')
        if trailing and trail>number(trailing.get('distance')):raise ValueError('protective_trailing_would_loosen')
        implied=exit_price-side*trail
        if any(side*(implied-old)<0 for old in stops):raise ValueError('protective_trailing_would_loosen_existing_stop')
    return dict(trade_id=tid,instrument=inst,side=side,
        stop_loss=None if stop is None else float(stop),take_profit=None if target is None else float(target),
        trailing_distance=None if trail is None else float(trail),quote_epoch=parsed.timestamp(),observed_epoch=float(observed_epoch),
        quote_max_age_sec=PROTECTIVE_QUOTE_MAX_AGE_SEC)
