"""Fixed paired, chronological directional research; no order or network APIs."""
from __future__ import annotations

import hashlib
import json
import math
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from threadpoolctl import threadpool_limits

VERSION = 'direction_paired_evaluation_v1_20260911'
AUTHORITY = dict(research_only=True, can_place_orders=False, can_promote=False)


def utc(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError('timezone_required')
    return int(stamp.timestamp())


def sha_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, indent=2, allow_nan=False)


def split_rows(frame, start, end, study_start):
    """All pairs share boundaries; future labels cannot enter training."""
    decision = frame['epoch'].to_numpy(dtype=np.int64) + 60
    maturity = frame['label_available_epoch'].to_numpy(dtype=np.int64)
    train = (decision >= study_start) & (decision < start) & (maturity < start)
    valid = (decision >= start) & (decision < end)
    return np.flatnonzero(train), np.flatnonzero(valid)


def epoch_weights(frame):
    counts = frame.groupby('epoch')['epoch'].transform('size').to_numpy(dtype=float)
    weights = 1.0 / counts
    return weights / weights.mean()


def metrics(frame, probability):
    p = np.asarray(probability, dtype=float)
    if p.shape != (len(frame),) or not np.isfinite(p).all() or np.any((p < 0) | (p > 1)):
        raise ValueError('invalid_probability')
    ret = frame['return_bps'].to_numpy(dtype=float)
    side = np.sign(p - .5)
    nonzero = ret != 0
    traded = side != 0
    predicted = nonzero & traded
    y = (ret[nonzero] > 0).astype(int)
    q = np.clip(p[nonzero], 1e-12, 1 - 1e-12)
    long_net = frame['long_net_bps'].to_numpy(dtype=float)
    short_net = frame['short_net_bps'].to_numpy(dtype=float)
    net = np.where(side > 0, long_net, np.where(side < 0, short_net, 0.))
    # The same two-sided cost support is used by every arm, including abstentions.
    cost_support = np.isfinite(long_net) & np.isfinite(short_net)
    cost_covered = traded & cost_support
    recalls = [float(np.mean(side[predicted & (np.sign(ret)==s)] == s)) for s in (-1,1)
               if np.any(predicted & (np.sign(ret)==s))]
    out = dict(rows=len(frame), nonzero_outcomes=int(nonzero.sum()), flat_outcomes=int((~nonzero).sum()),
        abstentions=int((~traded).sum()), directional_decisions=int(predicted.sum()),
        accuracy=float(np.mean(side[predicted] == np.sign(ret[predicted]))) if predicted.any() else None,
        balanced_accuracy=float(np.mean(recalls)) if len(recalls)==2 else None,
        direction_coverage=float(predicted.sum()/nonzero.sum()) if nonzero.any() else None,
        brier=float(np.mean((p[nonzero]-y)**2)) if len(y) else None,
        log_loss=float(-np.mean(y*np.log(q)+(1-y)*np.log(1-q))) if len(y) else None,
        auc=float(roc_auc_score(y, p[nonzero])) if len(np.unique(y)) == 2 else None,
        mean_signed_mid_bps=float(np.mean(side*ret)) if len(ret) else None,
        measured_cost_decisions=int(cost_covered.sum()),
        measured_cost_rows=int(cost_support.sum()),
        mean_bid_ask_close_net_bps=float(np.mean(net[cost_support])) if cost_support.any() else None,
        cost_scope='hypothetical_bid_ask_candle_close_endpoints_no_slippage_financing_or_fill_proof')
    return out


def _model(kind, params):
    estimator = LogisticRegression(**params) if kind == 'logistic' else HistGradientBoostingClassifier(**params)
    return make_pipeline(SimpleImputer(strategy='median', add_indicator=True, keep_empty_features=True),
                         StandardScaler(), estimator)


def _validate_frame(frame, feature_groups):
    required = {'instrument','epoch','horizon_minutes','label_available_epoch','return_bps',
                'long_net_bps','short_net_bps','news_available'}
    if not required.issubset(frame.columns) or frame.empty:
        raise ValueError('incomplete_or_empty_panel')
    if frame.duplicated(['instrument','epoch','horizon_minutes']).any():
        raise ValueError('duplicate_pair_time_horizon')
    if not np.isfinite(frame[['epoch','label_available_epoch','return_bps']].to_numpy(dtype=float)).all():
        raise ValueError('nonfinite_label_or_time')
    if (frame.epoch % 60 != 0).any():
        raise ValueError('minute_start_required')
    if (frame.label_available_epoch != frame.epoch+frame.horizon_minutes*60+60).any():
        raise ValueError('wrong_label_maturity')
    for columns in feature_groups.values():
        if len(columns) != len(set(columns)) or not set(columns).issubset(frame.columns):
            raise ValueError('invalid_feature_group')
        if np.isinf(frame[columns].to_numpy(dtype=float)).any():
            raise ValueError('infinite_feature')
    forbidden = required-{'news_available'} | {'target_epoch','available_epoch','decision_epoch'}
    if any(forbidden.intersection(columns) for columns in feature_groups.values()):
        raise ValueError('label_or_clock_selected_as_feature')


def evaluate(panel, *, feature_groups, spec, output_root, input_manifest_sha256, declared_universe):
    """Fit eight predeclared arms, preserve all paired forecast rows and models."""
    _validate_frame(panel, feature_groups)
    if set(feature_groups) != set(spec['feature_groups']):
        raise ValueError('spec_feature_group_mismatch')
    pairs = sorted(declared_universe)
    if len(pairs) != len(set(pairs)) or not pairs or not set(panel.instrument).issubset(pairs):
        raise ValueError('declared_universe_mismatch')
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=False)
    # Receipt exists before any fitting or label summary; failed runs remain retained.
    frozen = {'version':VERSION,'spec':spec,'feature_groups':feature_groups,
              'input_manifest_sha256':input_manifest_sha256,
              'implementation_sha256':sha_file(__file__), 'declared_universe':pairs, **AUTHORITY}
    (root/'FROZEN_RUN.json').write_text(canonical(frozen)+'\n', encoding='utf-8')
    identity = pd.get_dummies(pd.Categorical(panel.instrument, categories=pairs), prefix='pair', dtype=float)
    data = pd.concat([panel.reset_index(drop=True), identity.reset_index(drop=True)], axis=1)
    pair_columns = list(identity.columns)
    reports = []; predictions = []; retained_warnings = []
    study_start = utc(spec['data_start_utc'])
    import joblib
    with threadpool_limits(limits=1):
        for horizon in spec['horizons_minutes']:
            part = data[data.horizon_minutes == horizon].reset_index(drop=True)
            for fold in spec['folds']:
                print(f"Fitting frozen H{horizon}m / {fold['name']} comparison", flush=True)
                start, end = utc(fold['validation_start']), utc(fold['validation_end'])
                train_i, valid_i = split_rows(part, start, end, study_start)
                train = part.iloc[train_i]
                train = train[train.return_bps != 0]
                valid = part.iloc[valid_i]
                key = dict(horizon_minutes=horizon, fold=fold['name'])
                if len(train) < 200 or len(valid) < 30 or train.return_bps.gt(0).nunique() != 2:
                    reports.append({**key, 'status':'insufficient_rows_or_classes',
                                    'training_rows':len(train), 'validation_rows':len(valid)})
                    continue
                weights = epoch_weights(train)
                y = train.return_bps.gt(0).astype(int).to_numpy()
                prevalence = float(np.average(y, weights=weights))
                fold_prob = {'prevalence':np.full(len(valid), prevalence)}
                pair_prevalence = {pair:float(np.average(y[positions], weights=weights[positions]))
                    for pair, positions in train.groupby('instrument').indices.items()}
                fold_prob['pair_prevalence'] = valid.instrument.map(pair_prevalence).fillna(prevalence).to_numpy()
                momentum_col = feature_groups['technical'][8]  # compact24 third (15m) quartet rate
                fold_prob['momentum15'] = np.where(valid[momentum_col] > 0, 1.,
                                                   np.where(valid[momentum_col] < 0, 0., .5))
                fitted = {}
                for group, features in feature_groups.items():
                    columns = list(features)+pair_columns
                    for kind, params in spec['learners'].items():
                        name = kind+'__'+group
                        model = _model(kind, params)
                        with warnings.catch_warnings(record=True) as caught:
                            warnings.simplefilter('always')
                            model.fit(train[columns], y,
                                **{model.steps[-1][0]+'__sample_weight':weights})
                        retained_warnings.extend({'fold':fold['name'],'horizon':horizon,'arm':name,
                            'category':type(w.message).__name__,'message':str(w.message)} for w in caught)
                        fold_prob[name] = model.predict_proba(valid[columns])[:,1]
                        artifact = root / f"{fold['name']}_h{horizon}_{name}.joblib"
                        joblib.dump({'pipeline':model,'columns':columns,'horizon_minutes':horizon,
                            'trained_label_maturity_max':int(train.label_available_epoch.max()),
                            'validation_start':start,'scope':'retrospective_discovery_only',**AUTHORITY}, artifact)
                        fitted[name] = {'artifact':artifact.name,'sha256':sha_file(artifact),
                            'selected_features':len(columns),'raw_features':len(features),
                            'transformed_features':int(model[0].transform(train[columns].iloc[:1]).shape[1])}
                scores = {name:metrics(valid, prob) for name,prob in fold_prob.items()}
                # A hard-sign momentum baseline is not a calibrated probability model.
                for metric in ('brier','log_loss','auc'):
                    scores['momentum15'][metric] = None
                by_pair = {}
                for pair, indices in valid.groupby('instrument').indices.items():
                    by_pair[pair] = {name:metrics(valid.iloc[indices], prob[indices])
                                    for name,prob in fold_prob.items()}
                    for metric in ('brier','log_loss','auc'):
                        by_pair[pair]['momentum15'][metric] = None
                out = valid[['instrument','epoch','horizon_minutes','label_available_epoch','return_bps',
                             'long_net_bps','short_net_bps','news_available']].copy()
                out['fold'] = fold['name']
                for name, prob in fold_prob.items():
                    out['p__'+name] = prob
                predictions.append(out)
                reports.append({**key,'status':'completed','training_rows':len(train),
                    'training_unique_times':int(train.epoch.nunique()),'validation_rows':len(valid),
                    'validation_unique_times':int(valid.epoch.nunique()),
                    'training_label_maturity_max':int(train.label_available_epoch.max()),
                    'validation_decision_start':int(valid.epoch.min()+60),
                    'news_available_fraction':float(valid.news_available.mean()),
                    'training_up_prevalence':prevalence, 'models':fitted,'metrics':scores,'by_pair':by_pair})
    if predictions:
        all_predictions = pd.concat(predictions, ignore_index=True)
        all_predictions.to_parquet(root/'predictions.parquet', index=False)
    else:
        all_predictions = pd.DataFrame()
    aggregate = {}; deltas = []
    for horizon in spec['horizons_minutes']:
        if all_predictions.empty:
            break
        sub = all_predictions[all_predictions.horizon_minutes == horizon]
        if sub.empty:
            continue
        aggregate[str(horizon)] = {name[3:]:metrics(sub, sub[name].to_numpy())
            for name in sub.columns if name.startswith('p__')}
        for metric in ('brier','log_loss','auc'):
            aggregate[str(horizon)]['momentum15'][metric] = None
    comparisons = [('technical_peer','technical'),('technical_news','technical'),
                   ('combined','technical'),('combined','technical_peer'),('combined','technical_news')]
    for report in reports:
        if report['status'] != 'completed':
            continue
        for kind in spec['learners']:
            for candidate, control in comparisons:
                a=report['metrics'][kind+'__'+candidate]; b=report['metrics'][kind+'__'+control]
                delta={metric:(a[metric]-b[metric] if a[metric] is not None and b[metric] is not None else None)
                       for metric in ('accuracy','balanced_accuracy','brier','log_loss','mean_bid_ask_close_net_bps')}
                deltas.append({'horizon_minutes':report['horizon_minutes'],'fold':report['fold'],
                               'learner':kind,'candidate':candidate,'control':control,**delta})
    result = dict(version=VERSION, scope='retrospective_discovery_only',
        input_manifest_sha256=input_manifest_sha256, universe=pairs,
        aggregate=aggregate, folds=reports, paired_deltas=deltas, warnings=retained_warnings,
        forecast_rows=len(all_predictions),
        limitations=['Repeated pairs and overlapping horizons are dependent.',
                     'Only three validation dates; no untouched holdout or promotion inference.',
                     'Price close availability is a historical convention, not original receipt proof.',
                     'Probabilities are uncalibrated; missing inputs remain explicit.',
                     'Endpoint bid/ask diagnostics omit slippage, financing and actual execution.'],**AUTHORITY)
    (root/'RESULTS.json').write_text(canonical(result)+'\n', encoding='utf-8')
    return result
