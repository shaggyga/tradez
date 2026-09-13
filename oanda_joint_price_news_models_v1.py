"""Learned joint H1 return research model; no fixed news-to-price conversion.

Historical rows are retrospective training data, never prior prospective proof.
News inputs are already reconstructed from immutable point-in-time captures.
"""
from __future__ import annotations
import math
from bisect import bisect_left,bisect_right
import numpy as np
import oanda_pair_local_models_v2 as price

MODEL_VERSION='joint_price_news_ridge_v1_20260907'
FAMILY='ridge_price_news_v1'
FAMILIES=(FAMILY,)
NEWS_FEATURES=('context_balance','context_volume_log','context_signed_fraction','context_mean_age_hours',
               'vetted_balance','vetted_volume_log','vetted_conflict_fraction','vetted_remaining_hours')
NEUTRAL_NEWS=(0.,0.,0.,1.,0.,0.,0.,0.)
PARAMETERS={'horizon_sec':3600,'training_stride_sec':900,'minimum_training_rows':48,
            'minimum_nonzero_context_rows':12,'minimum_distinct_context_patterns':8,
            'ridge_alpha':50.,'shrinkage_prior_effective_rows':20.,'probability_shrinkage':.5,
            'probability_scope':'uncalibrated_model_estimate_not_verified_accuracy',
            'training_scope':'retrospective_original_member_archive_not_prior_prospective_performance'}

def _selected(families=None):
    selected=FAMILIES if families is None else tuple(families)
    if selected!=FAMILIES:raise ValueError('joint_single_family_required')
    return selected

def validate_identity(instrument,pip_size):return price.validate_identity(instrument,pip_size)

def _news(values):
    if not isinstance(values,(list,tuple)) or len(values)!=len(NEWS_FEATURES):raise ValueError('joint_news_feature_shape')
    if any(type(v) not in (int,float) or not math.isfinite(v) for v in values):raise ValueError('joint_nonfinite_news_features')
    a=np.asarray(values,dtype=float)
    if any(abs(a[i])>2 for i in (0,4)) or not -1<=a[2]<=1 or any(not 0<=a[i]<=1 for i in (3,6,7)) or any(not 0<=a[i]<=math.log1p(5000) for i in (1,5)):
        raise ValueError('joint_news_feature_range')
    return a

def _context(rows,cutoff,news_frames,current_news):
    epochs=list(rows);sessions=price._sessions(epochs);current=price._window(epochs,sessions,cutoff)
    if not isinstance(news_frames,dict) or len(news_frames)>256:raise ValueError('joint_news_frame_bound')
    frames={}
    for t,value in news_frames.items():
        if not isinstance(t,str) or not t.isascii() or not t.isdigit() or str(int(t))!=t or int(t) in frames:raise ValueError('joint_news_anchor_identity')
        frames[int(t)]=_news(value)
    if any(t%900 for t in frames):raise ValueError('joint_news_anchor_not_registered')
    current_values=_news(current_news);training=[]
    for t in epochs:
        if t not in frames or t%900 or t+3600>cutoff or t+3600 not in rows or sessions[t]!=sessions[t+3600]:continue
        if price._window_ready(price._window(epochs,sessions,t)):training.append(t)
    return epochs,sessions,current,training,frames,current_values

def _readiness(context):
    _,_,current,training,frames,query=context
    reasons=[];patterns={tuple(frames[t][:4]) for t in training}
    nonzero=sum(bool(frames[t][1]>0) for t in training)
    if len(current)-1<8:reasons.append(f'current_real_returns:{len(current)-1}<8')
    if current[-1]-current[0]<900:reasons.append(f'current_elapsed_span_sec:{current[-1]-current[0]}<900')
    if len(training)<48:reasons.append(f'mature_joint_h1_training_rows:{len(training)}<48')
    if nonzero<12:reasons.append(f'nonzero_news_context_training_rows:{nonzero}<12')
    if len(patterns)<8:reasons.append(f'distinct_news_context_patterns:{len(patterns)}<8')
    # A never-observed vetted dimension has no learned coefficient. Do not
    # present an unsupported new live directional feature as a trained effect.
    if training:
        historical=np.asarray([frames[t] for t in training])
        for index in (4,5,6,7):
            if np.std(historical[:,index])<1e-9 and abs(query[index]-historical[0,index])>1e-9:
                reasons.append('current_vetted_news_feature_unseen_in_training:'+NEWS_FEATURES[index])
    return {'ready':not reasons,'status':'blocked' if reasons else 'ready','reasons':reasons,
            'current_real_prices':len(current),'current_real_returns':len(current)-1,'current_span_sec':current[-1]-current[0],
            'mature_exact_h1_training_rows':len(training),'required_joint_training_rows':48,
            'nonzero_news_context_training_rows':nonzero,'distinct_news_context_patterns':len(patterns),
            'vetted_news_training_rows':sum(bool(frames[t][5]>0) for t in training),
            'training_scope':PARAMETERS['training_scope']}

def family_readiness(rows_by_epoch,cutoff_epoch,*,instrument,pip_size,news_frames,current_news):
    rows,cutoff=price._validated(rows_by_epoch,cutoff_epoch,instrument,pip_size)
    return {FAMILY:_readiness(_context(rows,cutoff,news_frames,current_news))}

def _joint_features(technical,news):
    # The first price rate is an observed change-per-minute, not a forecast.
    # Its interaction coefficient is learned alongside every other coefficient.
    return np.r_[technical,news,technical[0]*news[0],technical[0]*news[4]]

def _fit(x,y,query):
    mean=x.mean(axis=0);scale=x.std(axis=0);scale[scale<1e-9]=1.
    design=np.column_stack((np.ones(len(x)),(x-mean)/scale))
    q=np.r_[1.,(query-mean)/scale]
    penalty=np.eye(design.shape[1])*PARAMETERS['ridge_alpha'];penalty[0,0]=0.
    gram=design.T@design+penalty;beta=np.linalg.solve(gram,design.T@y)
    residual=float(np.std(y-design@beta));leverage=max(0.,float(q@np.linalg.solve(gram,q)))
    effective=len(x)/4.;shrink=effective/(effective+PARAMETERS['shrinkage_prior_effective_rows'])
    sigma=max(.1,residual,float(np.std(y)))*math.sqrt(1+leverage+1/max(effective,1.))
    expected=float(np.clip(float(q@beta)*shrink,-2*sigma,2*sigma))
    return expected,sigma,{'mean':mean.tolist(),'scale':scale.tolist(),'coefficients':beta.tolist(),
                          'raw_expected_pips':float(q@beta),'residual_sigma_pips':residual,'sigma_pips':sigma,
                          'feature_leverage':leverage,'expected_return_shrinkage':shrink,
                          'overlap_adjusted_training_count_heuristic':effective},beta,mean,scale

def predict_with_readiness(rows_by_epoch,cutoff_epoch,*,instrument,pip_size,news_frames,current_news,families=None):
    _selected(families);rows,cutoff=price._validated(rows_by_epoch,cutoff_epoch,instrument,pip_size)
    context=_context(rows,cutoff,news_frames,current_news);ready=_readiness(context);output={}
    if not ready['ready']:return output,{FAMILY:ready}
    epochs,sessions,current,training,frames,query_news=context;pip=float(pip_size)
    try:
        with np.errstate(over='raise',divide='raise',invalid='raise'):
            technical=np.asarray([price._features(rows,epochs,sessions,t,pip) for t in training])
            query_technical=price._features(rows,epochs,sessions,cutoff,pip)
            x=np.asarray([_joint_features(p,frames[t]) for p,t in zip(technical,training)])
            query=_joint_features(query_technical,query_news)
            y=np.asarray([(rows[t+3600]-rows[t])/pip for t in training])
            expected,sigma,fitted,beta,mean,scale=_fit(x,y,query)
            neutral_query=np.r_[1.,(_joint_features(query_technical,np.asarray(NEUTRAL_NEWS))-mean)/scale]
            neutral=float(np.clip(float(neutral_query@beta)*fitted['expected_return_shrinkage'],-2*sigma,2*sigma))
            baseline,_,baseline_fitted,_,_,_=_fit(technical,y,query_technical)
            probability=.5+.5*(.5*(1+math.erf(expected/(sigma*math.sqrt(2))))-.5)
            if not math.isfinite(expected) or not .25<=probability<=.75:raise ValueError('joint_prediction_invalid')
            diagnostics={**ready,'model_version':MODEL_VERSION,'feature_version':MODEL_VERSION,'family':FAMILY,'instrument':instrument,'pip_size':pip,
                'method':'standardized_price_plus_point_in_time_news_ridge','feature_count':len(query),'price_feature_count':len(query_technical),
                'news_feature_names':list(NEWS_FEATURES),'current_news_features':query_news.tolist(),'neutral_news_features':list(NEUTRAL_NEWS),
                'feature_cutoff_epoch':cutoff+60,'training_rows':len(training),'training_row_start_epochs_by_pair':{instrument:training},
                'training_label_maturity_max_epoch':max(t+3660 for t in training),'training_news_feature_cutoff_max_epoch':max(t+60 for t in training),
                'exact_target_offset_sec':3600,'training_stride_sec':900,'training_scope':PARAMETERS['training_scope'],
                'ridge_alpha':PARAMETERS['ridge_alpha'],'fitted_joint':fitted,'matched_price_only_fitted':baseline_fitted,
                'matched_price_only_expected_pips':baseline,'neutral_news_ablation_expected_pips':neutral,
                'news_ablation_difference_pips':expected-neutral,
                'news_ablation_scope':'same_fitted_model_with_documented_neutral_news_features_not_causal_impact',
                'probability_scope':PARAMETERS['probability_scope'],'probability_shrinkage':.5,
                'synthetic_prices':0,'time_compression':False,'research_only':True,'can_place_orders':False,'can_authorize':False,
                'can_promote':False,'account_eligible':False,'proof_eligible':False}
            output[FAMILY]=(expected,probability,diagnostics)
    except (ValueError,FloatingPointError,np.linalg.LinAlgError,OverflowError) as exc:
        ready.update(ready=False,status='blocked',reasons=['joint_numerical_failure:'+type(exc).__name__])
    return output,{FAMILY:ready}
