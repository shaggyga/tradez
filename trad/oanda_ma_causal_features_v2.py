"""Causal revision of the existing MA feature definitions, without model loading.

The verified original feature functions are compiled in a private namespace.
Only the past-only scale fallback and crossover-age initialization change.
Original source files/globals remain untouched. These changed features require
new fitting/validation; compatibility with retained fitted weights is unproven.
"""
from __future__ import annotations

import ast
from functools import lru_cache
import hashlib
import math
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

FEATURE_VERSION = 'ma_feature_grid_causal_v2_20260909'
ORIGINAL_SOURCE_SHA256 = 'e8b277c78aee6431f945bc73ba9d6b1948e7ab0e11b370aa188891ff500b955c'
ORIGINAL_SOURCE_PATH = Path(__file__).with_name('oanda_ma_feature_grid.py')
MAX_HISTORY_ROWS = 200000
MAX_SELECTED_ROWS = 20000
_CONSTANTS = frozenset(('TIMEFRAME_SECONDS','BASE_PERIODS','PERIOD_CEILING',
                       'CANONICAL_PERIOD_PAIRS','AGGREGATE_FEATURE_NAMES'))
_FUNCTIONS = frozenset(('periods_for_timeframe','period_pairs','ma_feature_names',
    '_moving_average_arrays','_column_mean','_column_std','_column_positive_fraction',
    '_column_width','_column_disagreement','build_ma_feature_matrix'))


def _past_scale(values, pip):
    changes = np.r_[np.nan,np.abs(np.diff(values))/pip]
    scale = pd.Series(changes,copy=False).rolling(20,min_periods=5).mean().to_numpy(dtype=np.float64)
    valid = np.isfinite(scale) & (scale>1e-6)
    # The former whole-series median included future values when the local
    # window was flat/unavailable. A row can now use only earlier valid scales.
    previous = pd.Series(np.where(valid,scale,np.nan)).expanding(min_periods=1).median().shift(1).to_numpy(dtype=np.float64)
    fallback = np.maximum(.1,np.where(np.isfinite(previous),previous,.1))
    return np.where(valid,scale,fallback)


def _past_cross_age(gap):
    side = gap>=0
    valid = np.isfinite(gap)
    changes = np.zeros(gap.size,dtype=bool)
    if gap.size:
        changes[0]=valid[0]
    if gap.size>1:
        changes[1:]=valid[1:] & valid[:-1] & (side[1:]!=side[:-1])
    indexes=np.arange(gap.size,dtype=np.int64)
    last_change=np.maximum.accumulate(np.where(changes,indexes,-1))
    # No observed crossover means elapsed prefix length, not final dataset size.
    return np.where(last_change>=0,indexes-last_change,indexes+1).astype(np.float64)


def _source():
    raw=ORIGINAL_SOURCE_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=ORIGINAL_SOURCE_SHA256:
        raise ValueError('original_ma_source_changed')
    return raw


@lru_cache(maxsize=1)
def _compiled(source):
    tree=ast.parse(source.decode('utf8'),filename='<verified-original-ma-feature-functions>')
    retained=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)]
    found=set()
    for node in tree.body:
        if isinstance(node,ast.AnnAssign) and isinstance(node.target,ast.Name) and node.target.id in _CONSTANTS:
            retained.append(node);found.add(node.target.id)
        elif isinstance(node,ast.FunctionDef) and node.name in _FUNCTIONS:
            retained.append(node);found.add(node.name)
    if found != _CONSTANTS|_FUNCTIONS:
        raise ValueError('original_ma_feature_slice_missing')
    namespace={'np':np,'pd':pd,'math':math,'Any':Any,'Iterable':Iterable,'Mapping':Mapping,
               '_rolling_scale':_past_scale,'_cross_age':_past_cross_age}
    code=compile(ast.fix_missing_locations(ast.Module(body=retained,type_ignores=[])),
                 '<verified-original-ma-feature-functions>','exec')
    exec(code,namespace)
    return namespace


def _inputs(values, positions, pip, timeframe):
    namespace=_compiled(_source())
    if not isinstance(timeframe,str) or timeframe not in namespace['TIMEFRAME_SECONDS']:
        raise ValueError('unsupported_ma_timeframe')
    if type(pip) not in (int,float) or not math.isfinite(pip) or not 1e-8<=pip<=.1:
        raise ValueError('explicit_ma_pip_required')
    if not isinstance(values,(list,tuple,np.ndarray)) or not 1<=len(values)<=MAX_HISTORY_ROWS:
        raise ValueError('bounded_ma_history_required')
    raw=np.asarray(values)
    if raw.ndim!=1 or raw.dtype.kind not in 'fiu':
        raise ValueError('real_numeric_ma_history_required')
    close=raw.astype(np.float64,copy=True)
    if not np.isfinite(close).all() or np.any(close<=0) or np.any(close>1e12):
        raise ValueError('finite_positive_ma_prices_required')
    if not isinstance(positions,(list,tuple,np.ndarray)) or not 1<=len(positions)<=MAX_SELECTED_ROWS:
        raise ValueError('bounded_ma_positions_required')
    selected=np.asarray(positions)
    if selected.ndim!=1 or selected.dtype.kind not in 'iu':
        raise ValueError('integer_ma_positions_required')
    selected=selected.astype(np.int64,copy=True)
    minimum=max(namespace['periods_for_timeframe'](timeframe))+4
    if selected.min()<minimum-1 or selected.max()>=len(close):
        raise ValueError('ma_origin_outside_warmed_prefix')
    return namespace,close,selected,float(pip)


def build_feature_matrix(values, positions, pip, timeframe):
    """Use every supplied historical row; never truncate/reinitialize silently."""
    namespace,close,selected,pip=_inputs(values,positions,pip,timeframe)
    matrix,names=namespace['build_ma_feature_matrix'](close,selected,pip,timeframe)
    if not np.isfinite(matrix).all():
        raise ValueError('nonfinite_causal_ma_feature')
    return matrix,names


def build_feature_vector(values, pip, timeframe):
    # Use the same canonical batch path; avoid a second numerical implementation
    # changing crossover signs close to floating-point rounding boundaries.
    if not isinstance(values,(list,tuple,np.ndarray)):
        raise ValueError('bounded_ma_history_required')
    matrix,names=build_feature_matrix(values,[len(values)-1],pip,timeframe)
    return {name:round(float(value),8) for name,value in zip(names,matrix[0])}


def metadata(timeframe='M1'):
    namespace=_compiled(_source())
    if timeframe not in namespace['TIMEFRAME_SECONDS']:
        raise ValueError('unsupported_ma_timeframe')
    return dict(feature_version=FEATURE_VERSION,original_source_sha256=ORIGINAL_SOURCE_SHA256,
        native_timeframe=timeframe,feature_count=len(namespace['ma_feature_names'](timeframe)),
        minimum_rows=max(namespace['periods_for_timeframe'](timeframe))+4,
        scale_fallback='median_of_strictly_prior_valid_rolling_scales_floor_0_1_pip',
        no_cross_age='current_prefix_row_count',ema_initialization='all_explicitly_supplied_history',
        original_source_modified=False,retained_weight_compatibility_proven=False,
        model_loaded=False,forecast_issued=False,research_only=True,can_place_orders=False,can_promote=False)
