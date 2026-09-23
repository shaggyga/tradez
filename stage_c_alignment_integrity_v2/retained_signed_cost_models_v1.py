"""Conditional signed means and separate exit-cost targets; offline research only."""
from __future__ import annotations
import re

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

VERSION='signed_cost_models_v1_20260911'
TARGET_COLUMNS=('return_bps','future_long_exit_cost_bps','future_short_exit_cost_bps')
WINDOWS=(1,5,15,30,60)
TECHNICAL=tuple(f'tech_{name}_{w}m' for w in WINDOWS for name in ('rate_bps_per_min','span_fraction','missing_fraction','unavailable'))+('tech_rms_bps','tech_max_gap_minutes','tech_observed_fraction_60m','tech_session_age_hours')
PEER=tuple(f'peer_{name}_{w}m' for w in WINDOWS for name in ('base_mean_bps','quote_mean_bps','strength_gap_bps','base_up_fraction','quote_up_fraction','breadth_gap','base_std_bps','quote_std_bps','base_count','quote_count','base_missing','quote_missing','base_std_missing','quote_std_missing','strength_gap_missing'))
NEWS=tuple('news_'+name for name in ('context_balance','context_volume_log','context_signed_fraction','context_mean_age_hours','vetted_balance','vetted_volume_log','vetted_conflict_fraction','vetted_remaining_hours'))+('news_available',)
ALLOWED_FEATURES=frozenset(TECHNICAL+PEER+NEWS+('known_entry_long_cost_bps','known_entry_short_cost_bps'))


def weights(frame):
    counts=frame.groupby('epoch')['epoch'].transform('size').to_numpy(dtype=float)
    value=1./counts
    return value/value.mean()


def exit_targets(panel, current_quotes):
    """Derive original future asymmetrical exit costs as LABELS only."""
    if current_quotes.duplicated(['instrument','epoch']).any():
        raise ValueError('duplicate_quote_identity')
    frame=panel.merge(current_quotes[['instrument','epoch','mid','bid','ask']],
        on=['instrument','epoch'],how='left',validate='many_to_one')
    quotes=frame[['mid','bid','ask']].to_numpy(dtype=float)
    if not np.isfinite(quotes).all() or np.any(quotes<=0) or (frame.bid>frame.mid).any() or (frame.ask<frame.mid).any():
        raise ValueError('valid_asymmetric_current_quotes_required')
    frame['known_entry_long_cost_bps']=(frame.ask-frame.mid)/frame.mid*10000.
    frame['known_entry_short_cost_bps']=(frame.mid-frame.bid)/frame.mid*10000.
    frame['future_long_exit_cost_bps']=frame.return_bps-frame.known_entry_long_cost_bps-frame.long_net_bps
    frame['future_short_exit_cost_bps']=-frame.return_bps-frame.known_entry_short_cost_bps-frame.short_net_bps
    for key in TARGET_COLUMNS:
        if not np.isfinite(frame[key]).all():raise ValueError('finite_labels_required')
    for key in TARGET_COLUMNS[1:]:
        if (frame[key]<-1e-7).any():raise ValueError('negative_observed_exit_cost')
        frame[key]=frame[key].clip(lower=0.)
    return frame


def validate_columns(columns):
    if len(columns)!=len(set(columns)) or not columns:
        raise ValueError('unique_features_required')
    for name in columns:
        if not (name in ALLOWED_FEATURES or re.fullmatch(r'pair_[A-Z]{3}_[A-Z]{3}',name)):
            raise ValueError('undeclared_or_future_feature:'+name)


def pipeline(classifier,params):
    estimator=(HistGradientBoostingClassifier(**params) if classifier else
               HistGradientBoostingRegressor(loss='squared_error',**params))
    return make_pipeline(SimpleImputer(strategy='median',add_indicator=True,keep_empty_features=True),
                         StandardScaler(),estimator)


def fit_one(frame,columns,target,params,*,classifier=False,sample_weights=None):
    model=pipeline(classifier,params)
    w=weights(frame) if sample_weights is None else np.asarray(sample_weights,dtype=float)
    if w.shape!=(len(frame),) or not np.isfinite(w).all() or np.any(w<=0):raise ValueError('invalid_training_weights')
    w=w/w.mean()
    model.fit(frame[columns],np.asarray(target),
        **{model.steps[-1][0]+'__sample_weight':w})
    return model


def fit_platt(probability,y,sample_weights,configuration):
    p=np.asarray(probability,dtype=float); y=np.asarray(y,dtype=float)
    w=np.asarray(sample_weights,dtype=float)
    if p.shape!=y.shape or w.shape!=y.shape or not np.isfinite(p).all() or not np.isfinite(w).all() or not np.isin(y,[0,1]).all() or np.any(w<=0) or np.any((p<0)|(p>1)):
        raise ValueError('invalid_calibration_arrays')
    minimum=configuration['minimum_rows_each_class']
    if min(np.sum(y==0),np.sum(y==1))<minimum:raise ValueError('insufficient_calibration_classes')
    clip=configuration['logit_clip_probability']; p=np.clip(p,clip,1-clip)
    logit=np.log(p/(1-p));w=w/w.sum();penalty=configuration['ridge_penalty']
    def objective(theta):
        z=theta[0]*logit+theta[1]; q=expit(z)
        value=float(w@(np.logaddexp(0,z)-y*z)+penalty*((theta[0]-1)**2+theta[1]**2))
        residual=w*(q-y)
        gradient=np.array([residual@logit+2*penalty*(theta[0]-1),residual.sum()+2*penalty*theta[1]])
        return value,gradient
    result=minimize(objective,np.array([1.,0.]),jac=True,method='L-BFGS-B',
        bounds=[configuration['slope_bounds'],configuration['intercept_bounds']],
        options={'maxiter':1000,'ftol':1e-12,'gtol':1e-9})
    if not result.success or not np.isfinite(result.x).all():raise ValueError('calibration_optimizer_failed')
    return {'slope':float(result.x[0]),'intercept':float(result.x[1]),'clip':clip,
            'rows':len(y),'positive_rows':int(y.sum()),'includes_flat_nonpositive':True,
            'probability_scope':'calibrated_on_separate_past_day_not_verified_future_accuracy'}


def apply_platt(p,calibration):
    p=np.clip(np.asarray(p,dtype=float),calibration['clip'],1-calibration['clip'])
    return expit(calibration['slope']*np.log(p/(1-p))+calibration['intercept'])


def fit_bundle(train,calibration_frame,*,columns,spec):
    validate_columns(columns)
    if train.empty or calibration_frame.empty:raise ValueError('empty_model_partition')
    cuts=[]
    for key in ('training_start','calibration_start','assessment_start'):
        stamp=pd.Timestamp(spec[key])
        if stamp.tzinfo is None:raise ValueError('timezone_required')
        cuts.append(int(stamp.timestamp()))
    a,b,c=cuts
    if not a<b<c:raise ValueError('ordered_partition_cutoffs_required')
    for frame,left,right in ((train,a,b),(calibration_frame,b,c)):
        d=frame.epoch+60
        if ((d<left)|(d>=right)|(frame.label_available_epoch>=right)).any():
            raise ValueError('partition_outside_declared_clocks')
        if (frame.label_available_epoch!=d+frame.horizon_minutes*60).any():
            raise ValueError('wrong_exact_label_maturity')
        if not np.isfinite(frame[list(TARGET_COLUMNS)].to_numpy(dtype=float)).all():
            raise ValueError('finite_training_targets_required')
    if train.label_available_epoch.max()>=calibration_frame.epoch.min()+60:
        raise ValueError('training_labels_overlap_calibration')
    r=train.return_bps.to_numpy(dtype=float)
    positive=r>0;minimum=spec['minimum_training_rows_each_class']
    if min(positive.sum(),(~positive).sum())<minimum:raise ValueError('insufficient_training_classes')
    params=spec['estimator_parameters']
    w=weights(train)
    models={
        'probability':fit_one(train,columns,positive.astype(int),params,classifier=True,sample_weights=w),
        'direct':fit_one(train,columns,r,params,sample_weights=w),
        'positive':fit_one(train.loc[positive],columns,r[positive],params,sample_weights=w[positive]),
        'nonpositive':fit_one(train.loc[~positive],columns,-r[~positive],params,sample_weights=w[~positive]),
        'long_exit':fit_one(train,columns,train.future_long_exit_cost_bps,params,sample_weights=w),
        'short_exit':fit_one(train,columns,train.future_short_exit_cost_bps,params,sample_weights=w)}
    raw=models['probability'].predict_proba(calibration_frame[columns])[:,1]
    calibration=fit_platt(raw,(calibration_frame.return_bps>0).astype(int),
        weights(calibration_frame),spec['calibration'])
    return {'version':VERSION,'columns':list(columns),'models':models,'calibration':calibration,
        'trained_label_max_epoch':int(train.label_available_epoch.max()),
        'calibration_label_max_epoch':int(calibration_frame.label_available_epoch.max()),
        'training_rows':len(train),'calibration_rows':len(calibration_frame),
        'training_positive_rows':int(positive.sum()),'training_flat_rows':int((r==0).sum()),
        'research_only':True,'can_place_orders':False,'can_promote':False}


def predict_bundle(bundle,frame):
    columns=bundle['columns'];validate_columns(columns)
    models=bundle['models'];x=frame[columns]
    raw=models['probability'].predict_proba(x)[:,1];cal=apply_platt(raw,bundle['calibration'])
    values={'probability_positive_raw':raw,'probability_positive_calibrated':cal,
        'direct_signed_mean':models['direct'].predict(x)}
    clips={}
    for name in ('positive','nonpositive','long_exit','short_exit'):
        prediction=models[name].predict(x)
        clips[name]=int((prediction<0).sum());values[name]=np.maximum(0.,prediction)
    values['mixture_raw']=raw*values['positive']-(1-raw)*values['nonpositive']
    values['mixture_calibrated']=cal*values['positive']-(1-cal)*values['nonpositive']
    if not all(np.isfinite(value).all() for value in values.values()):raise ValueError('nonfinite_model_output')
    return values,clips


def decide(mean_return,long_entry_cost,short_entry_cost,long_exit_cost,short_exit_cost,margin_bps):
    arrays=[np.asarray(x,dtype=float) for x in (mean_return,long_entry_cost,short_entry_cost,long_exit_cost,short_exit_cost)]
    if len({a.shape for a in arrays})!=1 or not all(np.isfinite(a).all() for a in arrays):
        raise ValueError('invalid_decision_arrays')
    if any(np.any(a<0) for a in arrays[1:]) or not np.isfinite(margin_bps) or margin_bps<0:
        raise ValueError('negative_cost_or_margin')
    mean,a,b,c,d=arrays;long=mean-a-c;short=-mean-b-d
    # Both positive is impossible with nonnegative costs, but ties are explicit.
    side=np.where((long>short)&(long>margin_bps),1,np.where((short>long)&(short>margin_bps),-1,0))
    return side,long,short
