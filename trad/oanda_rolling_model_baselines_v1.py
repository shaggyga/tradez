"""Causal conditional-OLS ARIMA(1,1,0), a separately identified comparator.

The retained oanda_arima_h1_baseline_grid.py used statsmodels ARIMA log-level
fits over multiple orders/trends, with stationarity enforcement disabled and
resampled/row-index targets. This is not a recreation of those MLE/SARIMAX
results: it uses exact M1 increments, zero-intercept conditional OLS and an
explicit stationarity clip. No library installation or live changes are needed.
"""
from __future__ import annotations

import hashlib
import math
import numpy as np

SCHEMA = 'rolling_arima110_conditional_ols_v1_20260915'
ENGINE = 'arima110_conditional_ols'
DEFAULT_HORIZONS = (5,15,30,60)


def _series(times, closes):
    raw=np.asarray(times)
    prices=np.asarray(closes,dtype=np.float64)
    if raw.ndim!=1 or prices.ndim!=1 or len(raw)!=len(prices) or raw.dtype.kind not in 'iuf':
        raise ValueError('aligned_one_dimensional_numeric_series_required')
    if np.any(~np.isfinite(raw)) or np.any(raw<=0) or np.any(raw>253402300740) or np.any(raw%60):
        raise ValueError('positive_original_minute_start_epochs_required')
    times=raw.astype(np.int64)
    if np.any(np.diff(times)<=0):
        raise ValueError('strictly_ordered_unique_clocks_required')
    good=np.isfinite(prices)&(prices>0)
    logs=np.full(len(prices),np.nan)
    np.log(prices,out=logs,where=good)
    increments=np.full(len(times),np.nan)
    if len(times)>1:
        valid=good[1:]&good[:-1]&(np.diff(times)==60)
        increments[1:]=np.where(valid,np.diff(logs),np.nan)
    return times,increments


def _cutoff(value):
    if type(value) is not int or value<=0 or value%60:
        raise ValueError('explicit_minute_training_cutoff_required')
    return value


def fit_arima110(times, closes, train_end_epoch, *, min_regression_pairs=100, phi_clip=.99):
    """Fit r[t]=phi*r[t-1]+error using contiguous real-minute triples.

    Every response increment ends with a completed candle whose END is strictly
    before train_end_epoch. Gaps/invalid prices are never compressed or imputed.
    All saved statistics describe training-only values, including the digest.
    """
    cutoff=_cutoff(train_end_epoch)
    if type(min_regression_pairs) is not int or min_regression_pairs<1:
        raise ValueError('positive_minimum_regression_pairs_required')
    if isinstance(phi_clip,bool) or not isinstance(phi_clip,(int,float)) or not math.isfinite(phi_clip) or not 0<phi_clip<1:
        raise ValueError('stationary_phi_clip_between_zero_and_one_required')
    times,inc=_series(times,closes)
    train=times+60<cutoff
    valid=np.zeros(len(times),dtype=bool)
    if len(times)>1:
        valid[1:]=train[1:]&np.isfinite(inc[1:])&np.isfinite(inc[:-1])&(np.diff(times)==60)
    indexes=np.flatnonzero(valid)
    x,y=inc[indexes-1],inc[indexes]
    xx=float(np.dot(x,x));xy=float(np.dot(x,y))
    digest=hashlib.sha256()
    for name,array,dtype in (('response_start',times[indexes],'<i8'),('previous_increment',x,'<f8'),('response_increment',y,'<f8')):
        digest.update(name.encode()+b'\0');digest.update(np.asarray(array,dtype=dtype).tobytes())
    result={'schema_version':SCHEMA,'engine':ENGINE,'status':'unavailable','reason':None,
        'order':[1,1,0],'price_transform':'natural_log','drift':False,'intercept':False,
        'estimator':'conditional zero-intercept OLS: sum(r_previous*r_current)/sum(r_previous^2)',
        'training_cutoff_epoch':cutoff,'training_candle_end_rule':'strictly less than cutoff',
        'training_completed_rows':int(train.sum()),'valid_training_increments':int(np.sum(train&np.isfinite(inc))),
        'valid_regression_pairs':len(indexes),'minimum_regression_pairs':min_regression_pairs,
        'first_response_start_epoch':int(times[indexes[0]]) if len(indexes) else None,
        'last_response_end_epoch':int(times[indexes[-1]]+60) if len(indexes) else None,
        'training_regression_sha256':digest.hexdigest(),'sum_previous_increment_squared':xx if math.isfinite(xx) else None,
        'sum_increment_crossproduct':xy if math.isfinite(xy) else None,
        'phi':None,'phi_unclipped':None,'phi_clip':float(phi_clip),'phi_was_clipped':False,
        'forecast':'expm1(current_log_increment * sum(phi^k,k=1..horizon_minutes))*10000',
        'forecast_location':'exponentiated conditional log-price location; no Gaussian variance/Jensen correction',
        'prediction_support':'current and immediately previous original minute; no gap compression',
        'fit_availability':'parameters only applied at origins whose bar END is at/after training cutoff',
        'historical_recipe_relation':'distinct aligned conditional-OLS comparator; not exact retained statsmodels MLE/SARIMAX recreation'}
    if len(indexes)<min_regression_pairs:
        result['reason']='insufficient_training_regression_pairs'
    elif not math.isfinite(xx) or not math.isfinite(xy):
        result['reason']='nonfinite_training_statistics'
    elif xx<=0:
        result['reason']='zero_training_regressor_energy'
    else:
        original=xy/xx
        if not math.isfinite(original):
            result['reason']='nonfinite_phi_estimate'
        else:
            phi=float(np.clip(original,-phi_clip,phi_clip))
            result.update(status='fitted',reason=None,phi=phi,phi_unclipped=original,
                          phi_was_clipped=phi!=original)
    return result


def _parameters(params):
    if not isinstance(params,dict) or params.get('schema_version')!=SCHEMA or params.get('engine')!=ENGINE:
        raise ValueError('identified_arima110_conditional_ols_parameters_required')
    cutoff=_cutoff(params.get('training_cutoff_epoch'))
    if params.get('status')=='unavailable':
        if params.get('phi') is not None:
            raise ValueError('unavailable_model_cannot_have_phi')
        return cutoff,None
    if params.get('status')!='fitted':
        raise ValueError('recognized_arima_fit_status_required')
    phi,clip=params.get('phi'),params.get('phi_clip')
    if (isinstance(phi,bool) or not isinstance(phi,(int,float)) or not math.isfinite(phi) or
            isinstance(clip,bool) or not isinstance(clip,(int,float)) or not math.isfinite(clip) or
            not 0<clip<1 or abs(phi)>clip):
        raise ValueError('finite_stationary_saved_phi_required')
    return cutoff,float(phi)


def arima110_origin_states(times, closes, params):
    times,inc=_series(times,closes)
    cutoff,phi=_parameters(params)
    states=np.full(len(times),'model_unavailable',dtype=object)
    if phi is not None:
        states[:]='available'
        states[~np.isfinite(inc)]='missing_current_increment'
        states[times+60<cutoff]='pre_fit_origin'
    return states


def predict_arima110(times, closes, params, horizons=DEFAULT_HORIZONS):
    """NaN for cold, pre-fit or unsupported origins; one array per horizon.

    Uses r[t] at the CURRENT supplied candle, so a one-minute forecast starts
    with phi*r[t], not r[t] itself or the preceding row's increment.
    """
    times,inc=_series(times,closes)
    cutoff,phi=_parameters(params)
    horizons=tuple(horizons)
    if (not horizons or any(type(h) is not int or not 1<=h<=1440 for h in horizons) or
            len(set(horizons))!=len(horizons)):
        raise ValueError('unique_positive_minute_horizons_up_to_1440_required')
    result={h:np.full(len(times),np.nan) for h in horizons}
    if phi is None:
        return result
    valid=np.isfinite(inc)&(times+60>=cutoff)
    for h in horizons:
        weight=math.fsum(phi**k for k in range(1,h+1))
        with np.errstate(over='ignore',invalid='ignore'):
            pred=np.expm1(inc[valid]*weight)*10000.
        pred[~np.isfinite(pred)]=np.nan
        result[h][valid]=pred
    return result
