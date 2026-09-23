"""Pure exact-quote news reaction contracts; no I/O, broker, fitting or clock service."""
from __future__ import annotations
from datetime import datetime,timezone
import math
from typing import Any,Mapping

try:
    from oanda_instrument_pips_v2 import CONTRACT as PIP_CONTRACT,normalize_instrument,resolve_pip_contract
except ModuleNotFoundError:
    from trad.oanda_instrument_pips_v2 import CONTRACT as PIP_CONTRACT,normalize_instrument,resolve_pip_contract

ENDPOINT_CONTRACT='news_four_quote_endpoints_entry_mid_bps_v2_20260912'
REACTION_CONTRACT='news_category_follow_fade_bps_actual_maturity_v2_20260912'
BPS_DENOMINATOR='common_entry_mid_notional'
MAX_ENDPOINT_DELAY_SEC=300.0


def aware_time(value: Any,field: str) -> datetime:
    try:
        parsed=value if isinstance(value,datetime) else datetime.fromisoformat(str(value).replace('Z','+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError('naive')
        parsed=parsed.astimezone(timezone.utc)
        if not math.isfinite(parsed.timestamp()):raise ValueError('nonfinite')
        return parsed
    except (ValueError,TypeError,OverflowError,OSError) as exc:
        raise ValueError('aware_finite_clock_required:'+field) from exc


def finite_number(value: Any,field: str,*,positive: bool=False) -> float:
    if isinstance(value,bool) or value is None:
        raise ValueError('finite_number_required:'+field)
    try:result=float(value)
    except (ValueError,TypeError,OverflowError) as exc:raise ValueError('finite_number_required:'+field) from exc
    if not math.isfinite(result) or (positive and result<=0):raise ValueError('finite_positive_number_required:'+field if positive else 'finite_number_required:'+field)
    return result


def positive_integer(value: Any,field: str) -> int:
    if isinstance(value,bool) or not isinstance(value,int) or value<=0:
        raise ValueError('positive_integer_required:'+field)
    return value


def endpoint_returns(
    *,pair: str,direction: str,horizon_minutes: int,signal_utc: Any,entry_utc: Any,
    nominal_target_utc: Any,exit_utc: Any,as_of_utc: Any,
    entry_bid: Any,entry_ask: Any,exit_bid: Any,exit_ask: Any,
    pip_metadata: Mapping[str,Any] | None=None,
) -> dict[str,Any]:
    pair=normalize_instrument(pair)
    if direction not in ('LONG','SHORT'):raise ValueError('direction_LONG_or_SHORT_required')
    horizon=positive_integer(horizon_minutes,'horizon_minutes')
    clocks={name:aware_time(value,name) for name,value in [('signal_utc',signal_utc),('entry_utc',entry_utc),('nominal_target_utc',nominal_target_utc),('exit_utc',exit_utc),('as_of_utc',as_of_utc)]}
    signal,entry,target,exit_,asof=(clocks[k].timestamp() for k in ('signal_utc','entry_utc','nominal_target_utc','exit_utc','as_of_utc'))
    if abs(target-(signal+horizon*60))>1e-6:raise ValueError('nominal_target_must_equal_signal_plus_horizon')
    if not signal<=entry<=exit_ or target>exit_:raise ValueError('endpoint_clock_order_invalid')
    if entry-signal>MAX_ENDPOINT_DELAY_SEC or exit_-target>MAX_ENDPOINT_DELAY_SEC:raise ValueError('endpoint_delay_exceeds_contract')
    if exit_>asof:raise ValueError('actual_exit_after_as_of')
    if any(clocks[name].second or clocks[name].microsecond for name in ('entry_utc','exit_utc')):
        raise ValueError('exact_M1_open_endpoint_clock_required')
    prices={name:finite_number(value,name,positive=True) for name,value in [('entry_bid',entry_bid),('entry_ask',entry_ask),('exit_bid',exit_bid),('exit_ask',exit_ask)]}
    if prices['entry_bid']>prices['entry_ask'] or prices['exit_bid']>prices['exit_ask']:raise ValueError('crossed_endpoint_quotes')
    # A common notional denominator makes same-size pair legs comparable. This
    # is explicitly different from each side's broker return-on-margin percent.
    entry_mid=prices['entry_bid']/2+prices['entry_ask']/2
    long_bps=10000.0*(prices['exit_bid']/entry_mid-prices['entry_ask']/entry_mid)
    short_bps=10000.0*(prices['entry_bid']/entry_mid-prices['exit_ask']/entry_mid)
    if not math.isfinite(long_bps) or not math.isfinite(short_bps):raise ValueError('nonfinite_endpoint_return')
    pip=resolve_pip_contract(pair,pip_metadata)
    return {'endpoint_contract':ENDPOINT_CONTRACT,'return_unit':'bps','bps_denominator':BPS_DENOMINATOR,
      'pair':pair,'direction':direction,'horizon_minutes':horizon,**prices,
      **{name:value.isoformat() for name,value in clocks.items()},
      'signal_epoch':signal,'entry_epoch':entry,'nominal_target_epoch':target,'actual_exit_epoch':exit_,
      'outcome_maturity_epoch':exit_,'entry_delay_sec':entry-signal,'exit_delay_sec':exit_-target,
      'max_endpoint_delay_sec':MAX_ENDPOINT_DELAY_SEC,'entry_mid':entry_mid,
      'long_net_bps':long_bps,'short_net_bps':short_bps,
      'follow_net_bps':long_bps if direction=='LONG' else short_bps,
      'fade_net_bps':short_bps if direction=='LONG' else long_bps,
      'pip_size':pip['pip'],'pip_contract':pip,
      'cost_contract':'entry_and_exit_bid_ask_spread_included;no_added_commission_slippage_or_financing',
      'price_clock_convention':'M1_open_quotes_at_actual_endpoint_clocks',
      'research_only':True,'account_eligible':False}


def validate_retained_call(raw: Mapping[str,Any],*,as_of_utc: Any) -> dict[str,Any]:
    if raw.get('endpoint_contract')!=ENDPOINT_CONTRACT:
        raise ValueError('new_exact_endpoint_contract_required;legacy_pips_not_reconstructed')
    required=('pair','direction','horizon_minutes','signal_utc','entry_utc','nominal_target_utc','exit_utc','entry_bid','entry_ask','exit_bid','exit_ask')
    original_asof=aware_time(raw.get('as_of_utc'),'original_scoring_as_of_utc')
    validation_asof=aware_time(as_of_utc,'validation_as_of_utc')
    if original_asof>validation_asof:raise ValueError('original_scoring_as_of_after_as_of_validation')
    if any(name not in raw for name in required):raise ValueError('missing_explicit_endpoint_or_clock_field')
    retained_pip=raw.get('pip_contract')
    if not isinstance(retained_pip,Mapping) or retained_pip.get('contract')!=PIP_CONTRACT:
        raise ValueError('pip_v2_provenance_required')
    pair=normalize_instrument(raw['pair'])
    if normalize_instrument(retained_pip.get('instrument'))!=pair:raise ValueError('pip_provenance_pair_mismatch')
    pip=finite_number(retained_pip.get('pip'),'retained_pip',positive=True)
    computed=endpoint_returns(**{name:raw[name] for name in required},as_of_utc=original_asof,pip_metadata={'instrument':pair,'pip':pip})
    if not math.isclose(computed['pip_size'],pip,rel_tol=1e-12):raise ValueError('retained_pip_outside_contract')
    # Preserve the original pip-resolution source/rejections; do not relabel a
    # retained fallback as fresh venue metadata. Bps is recomputed from prices.
    computed['pip_contract']=dict(retained_pip)
    computed['as_of_utc']=raw['as_of_utc'] if isinstance(raw['as_of_utc'],str) else original_asof.isoformat()
    computed['validation_as_of_utc']=validation_asof.isoformat()
    return {**raw,**computed}
