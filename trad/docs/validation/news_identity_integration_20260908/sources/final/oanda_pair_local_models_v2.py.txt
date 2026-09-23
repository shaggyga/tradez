"""Pure elapsed-time pair models on observed prices, with independent readiness.

Keys are actual UTC minute START epochs; each close is complete at key+60.
Gaps remain gaps. Sessions split after >30 minutes without an observed close;
neither current features nor historical H1 labels cross such a boundary.
Probabilities are deliberately shrunk, uncalibrated estimates, not accuracy.
"""
from bisect import bisect_left,bisect_right
from collections.abc import Mapping
from types import MappingProxyType
import math
import re
import numpy as np

MODEL_VERSION='pair_local_elapsed_time_independent_families_v2_20260907'
FAMILIES=('ridge_return_repaired','probabilistic_state_space')
PARAMETERS=MappingProxyType({
    'horizon_sec':3600,'maximum_real_rows':4096,'session_gap_sec':1800,
    'current_window_sec':3600,'minimum_real_returns':8,'minimum_span_sec':900,
    'feature_windows_minutes':(1,5,15,30,60),'ridge_minimum_rows':24,
    'ridge_stride_minutes':3,'ridge_alpha':20.,'ridge_shrinkage_prior_effective_rows':8.,
    'state_halflife_minutes':15.,'state_shrinkage_prior_returns':24.,
    'minimum_sigma_pips':.25,'probability_shrinkage':.5,'maximum_expected_sigma':2.,
    'label_policy':'Exact t+3600 observed endpoint in same session; no interior completeness requirement.',
    'probability_scope':'uncalibrated_shrunk_model_estimate_not_verified_accuracy',
})

def validate_identity(instrument,pip_size):
    if (type(instrument) is not str or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',instrument)
            or instrument[:3]==instrument[4:]):raise ValueError('valid_distinct_currency_pair_required')
    if type(pip_size) not in (int,float) or pip_size not in (.0001,.001,.01):
        raise ValueError('bounded_positive_finite_pip_size_required')
    return instrument,float(pip_size)

def _epoch(value):
    if type(value) not in (int,float) or not math.isfinite(value) or not 0<value<32503680000 or value%60:
        raise ValueError('positive_exact_UTC_minute_START_required')
    return int(value)

def _validated(rows,cutoff,instrument,pip_size):
    validate_identity(instrument,pip_size);cutoff=_epoch(cutoff)
    if not isinstance(rows,Mapping) or not 1<=len(rows)<=PARAMETERS['maximum_real_rows']:
        raise ValueError('bounded_nonempty_real_pair_rows_required')
    result={}
    for key,value in rows.items():
        key=_epoch(key)
        if key>cutoff:raise ValueError('future_bar_after_cutoff')
        if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
            raise ValueError('positive_finite_pair_price_required')
        result[key]=float(value)
    if max(result)!=cutoff:raise ValueError('cutoff_must_equal_latest_real_bar')
    return dict(sorted(result.items())),cutoff

def _selected(families):
    selected=FAMILIES if families is None else tuple(families)
    if not selected or len(set(selected))!=len(selected) or set(selected)-set(FAMILIES):
        raise ValueError('nonempty_unique_known_families_required')
    return selected

def _sessions(epochs):
    starts={};start=previous=None
    for t in epochs:
        if previous is None or t-previous>PARAMETERS['session_gap_sec']:start=t
        starts[t]=start;previous=t
    return starts

def _window(epochs,sessions,t):
    start=max(sessions[t],t-PARAMETERS['current_window_sec'])
    return epochs[bisect_left(epochs,start):bisect_right(epochs,t)]

def _window_ready(times):
    return len(times)-1>=PARAMETERS['minimum_real_returns'] and times[-1]-times[0]>=PARAMETERS['minimum_span_sec']

def _context(rows,cutoff):
    epochs=list(rows);sessions=_sessions(epochs);current=_window(epochs,sessions,cutoff)
    training=[]
    for t in epochs:
        target=t+PARAMETERS['horizon_sec']
        if (t//60%PARAMETERS['ridge_stride_minutes']==0 and target<=cutoff and target in rows
                and sessions[t]==sessions[target] and _window_ready(_window(epochs,sessions,t))):training.append(t)
    return epochs,sessions,current,training

def _readiness(rows,cutoff,context):
    epochs,sessions,current,training=context
    reasons=[]
    if len(current)-1<PARAMETERS['minimum_real_returns']:
        reasons.append(f"current_real_returns:{len(current)-1}<{PARAMETERS['minimum_real_returns']}")
    span=current[-1]-current[0]
    if span<PARAMETERS['minimum_span_sec']:reasons.append(f"current_elapsed_span_sec:{span}<{PARAMETERS['minimum_span_sec']}")
    common={'current_real_prices':len(current),'current_real_returns':len(current)-1,
        'current_span_sec':span,'session_start_epoch':sessions[cutoff],
        'current_window_start_epoch':current[0],'retained_real_rows':len(rows),
        'mature_exact_h1_training_rows':len(training),'required_ridge_training_rows':24,
        'maximum_current_gap_sec':max((b-a for a,b in zip(current,current[1:])),default=0)}
    output={}
    for family in FAMILIES:
        why=list(reasons)
        if family==FAMILIES[0] and len(training)<24:why.append(f'mature_exact_h1_training_rows:{len(training)}<24')
        output[family]={**common,'ready':not why,'status':'blocked' if why else 'ready','reasons':why}
    return output

def family_readiness(rows_by_epoch,cutoff_epoch,*,instrument,pip_size):
    rows,cutoff=_validated(rows_by_epoch,cutoff_epoch,instrument,pip_size)
    return _readiness(rows,cutoff,_context(rows,cutoff))

def _features(rows,epochs,sessions,t,pip):
    current=_window(epochs,sessions,t);features=[]
    for minutes in PARAMETERS['feature_windows_minutes']:
        times=current[bisect_left(current,t-minutes*60):]
        span=(times[-1]-times[0])/60
        # Zero is a feature sentinel only. Explicit availability, span and
        # missingness accompany it; no synthetic timestamp or price is added.
        available=len(times)>=2
        change=(rows[times[-1]]-rows[times[0]])/pip if available else 0.
        rate=change/span if available else 0.
        density=(len(times)-1)/minutes
        features.extend((rate,span/minutes,1.-density,float(not available)))
    dt=np.diff(np.asarray(current,dtype=float))/60
    changes=np.diff(np.asarray([rows[x] for x in current]))/pip
    features.extend((float(np.sqrt(np.mean(changes*changes/dt))),
                     float(np.max(dt)),len(current)/61.,(t-sessions[t])/3600.))
    output=np.asarray(features,dtype=float)
    if not np.all(np.isfinite(output)):raise ValueError('nonfinite_time_aware_features')
    return output

def _probability(expected,sigma):
    raw=.5*(1.+math.erf(expected/(sigma*math.sqrt(2.))))
    return .5+PARAMETERS['probability_shrinkage']*(raw-.5)

def _base_diagnostics(family,rows,cutoff,context,instrument,pip):
    epochs,sessions,current,training=context
    used=training if family==FAMILIES[0] else []
    return {'model_version':MODEL_VERSION,'family':family,'instrument':instrument,'pip_size':pip,
        'feature_cutoff_epoch':cutoff+60,'epoch_semantics':'UTC_M1_start; actual close available no earlier than start+60',
        'training_rows':len(used),'training_row_start_epochs_by_pair':{instrument:used},
        'training_label_maturity_max_epoch':max((t+3660 for t in used),default=None),
        'exact_target_offset_sec':3600,'input_scope':'single_pair_real_timestamps_no_peer_inputs',
        'current_real_price_epochs':current,'state_segment_start_epoch':sessions[cutoff],
        'session_gap_sec':1800,'synthetic_prices':0,'time_compression':False,
        'probability_scope':PARAMETERS['probability_scope'],'probability_shrinkage':.5,
        'research_only':True,'can_place_orders':False,'can_authorize':False,'can_promote':False,
        'account_eligible':False,'proof_eligible':False}

def _state(rows,cutoff,context,instrument,pip):
    current=context[2]
    dt=np.diff(np.asarray(current,dtype=float))/60
    changes=np.diff(np.asarray([rows[t] for t in current]))/pip
    age=(cutoff-np.asarray(current[1:],dtype=float))/60
    weight=dt*np.exp(-math.log(2)*age/PARAMETERS['state_halflife_minutes']);weight/=sum(weight)
    rates=changes/dt;drift=float(weight@rates)
    effective=float(1./(weight@weight))
    shrink=effective/(effective+PARAMETERS['state_shrinkage_prior_returns'])
    # Variance has units pips^2/minute. An interval's elapsed duration is
    # retained both here and in its rate; sparse updates are never one minute.
    variance=float(weight@((changes-drift*dt)**2/dt))
    sigma=max(PARAMETERS['minimum_sigma_pips'],math.sqrt(max(0.,variance)*60*(1.+60/(sum(dt)*max(effective,1.)))))
    expected=float(np.clip(drift*60*shrink,-2*sigma,2*sigma))
    diagnostics=_base_diagnostics(FAMILIES[1],rows,cutoff,context,instrument,pip)
    diagnostics.update(method='elapsed_time_weighted_local_drift',elapsed_minutes=dt.tolist(),
        observed_return_rates_pips_per_minute=rates.tolist(),raw_drift_pips_per_minute=drift,
        effective_return_count=effective,drift_shrinkage=shrink,forecast_sigma_pips=sigma)
    return expected,_probability(expected,sigma),diagnostics

def _ridge(rows,cutoff,context,instrument,pip):
    epochs,sessions,current,training=context
    x=np.asarray([_features(rows,epochs,sessions,t,pip) for t in training])
    y=np.asarray([(rows[t+3600]-rows[t])/pip for t in training])
    mean=x.mean(axis=0);scale=x.std(axis=0);scale[scale<1e-9]=1.
    design=np.column_stack((np.ones(len(x)),(x-mean)/scale))
    query=np.r_[1.,(_features(rows,epochs,sessions,cutoff,pip)-mean)/scale]
    penalty=np.eye(design.shape[1])*PARAMETERS['ridge_alpha'];penalty[0,0]=0.
    gram=design.T@design+penalty
    beta=np.linalg.solve(gram,design.T@y)
    raw_expected=float(query@beta);residual=float(np.std(y-design@beta))
    effective=len(training)*PARAMETERS['ridge_stride_minutes']/60.
    shrink=effective/(effective+PARAMETERS['ridge_shrinkage_prior_effective_rows'])
    leverage=max(0.,float(query@np.linalg.solve(gram,query)))
    sigma=max(PARAMETERS['minimum_sigma_pips'],residual,float(np.std(y)))*math.sqrt(1.+leverage+1./max(effective,1.))
    expected=float(np.clip(raw_expected*shrink,-2*sigma,2*sigma))
    diagnostics=_base_diagnostics(FAMILIES[0],rows,cutoff,context,instrument,pip)
    diagnostics.update(method='standardized_actual_window_missingness_ridge',ridge_alpha=20.,stride_minutes=3,
        feature_count=len(query)-1,residual_sigma_pips=residual,forecast_sigma_pips=sigma,
        overlap_adjusted_training_count_heuristic=effective,expected_return_shrinkage=shrink,
        raw_expected_pips=raw_expected,feature_leverage=leverage,
        label_policy=PARAMETERS['label_policy'])
    return expected,_probability(expected,sigma),diagnostics

def predict_with_readiness(rows_by_epoch,cutoff_epoch,*,instrument,pip_size,families=None):
    rows,cutoff=_validated(rows_by_epoch,cutoff_epoch,instrument,pip_size)
    selected=_selected(families);context=_context(rows,cutoff);readiness=_readiness(rows,cutoff,context);output={}
    for family in selected:
        if not readiness[family]['ready']:continue
        try:
            with np.errstate(over='raise',divide='raise',invalid='raise'):
                result=(_ridge if family==FAMILIES[0] else _state)(rows,cutoff,context,instrument,float(pip_size))
            expected,probability,diagnostics=result
            if not math.isfinite(expected) or not math.isfinite(probability) or not .25<=probability<=.75:
                raise ValueError('nonfinite_or_unbounded_prediction')
            output[family]=result
        except (ValueError,FloatingPointError,np.linalg.LinAlgError,OverflowError) as exc:
            readiness[family].update(ready=False,status='blocked',reasons=['family_numerical_failure:'+type(exc).__name__])
    return output,readiness

def predict_all(rows_by_epoch,cutoff_epoch,*,instrument,pip_size,families=None):
    """Return only available requested families; a blocked peer never gates one."""
    return predict_with_readiness(rows_by_epoch,cutoff_epoch,instrument=instrument,pip_size=pip_size,families=families)[0]
