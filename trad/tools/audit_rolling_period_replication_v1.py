"""Independent audit for complete earlier-period replications and H1 masks.

Recomputes input-prefix statistics from accepted feature shards, original
OOF/fit memberships, saved model application and endpoint decisions/costs.
No estimator refit, raw candle read, live changes or original-artifact write.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import hashlib,json
from pathlib import Path
import sys,time
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
import joblib
import numpy as np
import pyarrow.parquet as pq
from threadpoolctl import threadpool_limits
from tools import audit_rolling_specialists_v1 as legacy
from tools import audit_rolling_model_comparison_v1 as previous_audit
from oanda_rolling_model_design_v1 import fit_normalizer,RECIPES
from oanda_rolling_technical_dataset_v1 import iter_partitions
import oanda_rolling_specialists_v1 as contract
import oanda_rolling_family_design_v1 as family_design

RESULT_SCHEMA='rolling_specialist_replication_v2_20260915'
HORIZONS=(30,60)
HEADS=legacy.HEADS
GROUPS=('compact38','compact50')
META_ARMS=('direct_ridge','direct_context_hgb')
VARIANTS=('direct','mixture_raw','mixture_calibrated')+META_ARMS
SUBDIRS=legacy.SUBDIRS
require=legacy.require
sha=legacy.sha
checked=legacy.checked
read_json=legacy.read_json
exact_arrays=legacy.exact_arrays
check_sources=legacy.check_sources
check_references=legacy.check_references
fold_population=legacy.fold_population
mature_rows=legacy.mature_rows
independent_normalize=legacy.independent_normalize
normalized_rows=legacy.normalized_rows
base_rows=legacy.base_rows
selection=legacy.selection
raw_meta=legacy.raw_meta
replay_heads=legacy.replay_heads
replay_meta=legacy.replay_meta
apply_platt_independently=legacy.apply_platt_independently
verify_probability_scores=legacy.verify_probability_scores
audit_gate_scores=legacy.audit_gate_scores
read_forecast=legacy.read_forecast


def load_full_inputs(root, metadata, pm):
    n = metadata['rows']
    data = {'raw_x': np.empty((n, 50), dtype=np.float64), 'time': np.empty(n, dtype=np.int64),
            'split': np.empty(n, dtype=np.int8), 'pair_id': np.empty(n, dtype=np.int16),
            'entry_long': np.empty(n), 'entry_short': np.empty(n), 'pair_names': sorted(metadata['pairs']),
            'feature_names': metadata['feature_names'], 'cutoffs': np.asarray(metadata['fold_cutoffs_epoch'], dtype=np.int64)}
    data['normalizers'] = {name: np.empty((5, 68, 50), dtype=bool if name == 'supported' else np.int64 if name == 'count' else float)
                           for name in ('count', 'mean', 'scale', 'supported')}
    for h in HORIZONS:
        for stem in ('y', 'long', 'short', 'valid', 'strict', 'eligible'):
            data[f'{stem}_{h}'] = np.empty(n, dtype=bool if stem in ('valid','strict','eligible') else float)
    offset = 0
    for pid, pair in enumerate(data['pair_names']):
        record = metadata['pairs'][pair]
        source = checked(root, record['path'], record['sha256'])
        old = pm['pairs'][pair]
        old_source = checked(metadata['prepared_root'], old['path'], old['sha256'])
        with np.load(source, allow_pickle=False) as a, np.load(old_source, allow_pickle=False) as b:
            size = len(a['time'])
            require(size == record['rows'] == old['rows'] and exact_arrays(a['time'], b['time']) and exact_arrays(a['split'], b['split']), 'exact_raw_prepared_pair_keys')
            require(hashlib.sha256(a['time'].astype('<i8').tobytes()).hexdigest() == record['key_sha256'], 'raw_retained_clock_digest')
            require(a['raw_x'].shape == (size, 50) and a['raw_x'].dtype == np.float64, 'raw50_float64')
            sl = slice(offset, offset + size)
            for name in ('raw_x', 'time', 'split'):
                data[name][sl] = a[name]
            data['pair_id'][sl] = pid
            for name in ('long', 'short'):
                data['entry_' + name][sl] = a['known_entry_' + name + '_bps']
            for name in data['normalizers']:
                require(a['normalizer_' + name].shape == (5, 50), 'five_prefix_normalizers')
                data['normalizers'][name][:, pid] = a['normalizer_' + name]
            for h in HORIZONS:
                for stem in ('y', 'long', 'short', 'valid', 'strict', 'eligible'):
                    data[f'{stem}_{h}'][sl] = b[f'{stem}_{h}']
        offset += size
    require(offset == n and np.all(data['time'][data['split'] == 0] % 900 == 0), 'all_retained_rows_and_original_train_clock')
    require(np.all(np.diff(data['cutoffs']) == 7 * 86400) and data['cutoffs'][-1] == metadata['boundaries']['train_end'], 'declared_weekly_prefixes')
    return data


def load_expected(pm, qm, prepared, quotes):
    """Read each prepared/quote pair once; retain only small replay feature slices."""
    pair_names = sorted(pm['pairs'])
    chunks = {name: [] for name in ('time', 'split', 'pair_id', 'spread')}
    for h in HORIZONS:
        for stem in ('y', 'long', 'short', 'valid', 'strict', 'eligible', 'prior', 'arima',
                     'momentum', 'delay_long', 'delay_short', 'delay_valid'):
            chunks[f'{stem}_{h}'] = []
    key_digests = {h: hashlib.sha256() for h in HORIZONS}
    y_digests = {h: hashlib.sha256() for h in HORIZONS}
    train_counts = {h: {} for h in HORIZONS}
    replay_indices, replay_values, representatives = [], [], []
    offset = 0
    training_clock_rows = 0
    for pair_id, pair in enumerate(pair_names):
        pr, qr = pm['pairs'][pair], qm['pairs'][pair]
        pp = checked(prepared, pr['path'], pr['sha256'])
        qp = checked(quotes, qr['path'], qr['sha256'])
        checked(quotes, qr['receipt_path'], qr['receipt_sha256'])
        columns = ['bar_start_epoch', 'origin_split', 'quote__entry_spread_bps', 'quote__mid_close']
        columns += [f'forecast__arima110_conditional_ols__{h}m_bps' for h in HORIZONS]
        columns += [f'delayed_label__{h}m__{field}' for h in HORIZONS for field in ('long_net_bps', 'short_net_bps', 'valid')]
        qt = pq.ParquetFile(qp).read(columns=columns)
        clocks = qt['bar_start_epoch'].to_numpy()
        require(len(clocks) == qr['rows'] and np.all(clocks[1:] > clocks[:-1]), 'quote_clock_count_order')
        require(hashlib.sha256(clocks.astype('<i8').tobytes()).hexdigest() == qr['key_sha256'], 'quote_clock_digest')
        with np.load(pp, allow_pickle=False) as a:
            t, split = a['time'], a['split']
            require(len(t) == pr['rows'] and np.all(t[1:] > t[:-1]), 'prepared_clock_count_order')
            require(hashlib.sha256(t.astype('<i8').tobytes()).hexdigest() == pr['key_sha256'], 'prepared_clock_digest')
            require(np.isin(split, (0, 1, 2)).all(), 'three_known_splits_only')
            train = split == 0
            require(np.all(t[train] % 900 == 0), 'original_15_minute_training_clock_required')
            training_clock_rows += int(train.sum())
            expected_split = np.where(t < pm['boundaries']['train_end'], 0,
                                      np.where(t < pm['boundaries']['validation_end'], 1, 2))
            require(np.array_equal(split, expected_split), 'time_derived_split_identity')
            loc = np.searchsorted(clocks, t)
            require(np.all(loc < len(clocks)) and np.array_equal(clocks[loc], t), 'exact_quote_join')
            quote_splits = qt['origin_split'].to_numpy()[loc]
            require(np.array_equal(split, np.where(quote_splits == 'train', 0, np.where(quote_splits == 'validation', 1, 2))),
                    'quote_split_identity')
            later = split != 0
            later_t, later_split, later_loc = t[later], split[later], loc[later]
            n = len(later_t)
            require(n == pr['validation_rows'] + pr['later_development_test_rows'], 'all_later_pair_rows')
            chunks['time'].append(later_t)
            chunks['split'].append(later_split)
            chunks['pair_id'].append(np.full(n, pair_id, dtype=np.int16))
            chunks['spread'].append(qt['quote__entry_spread_bps'].to_numpy()[later_loc])
            candidates = np.flatnonzero(later_split == 2)
            representative = int(candidates[0]) if len(candidates) else (0 if n else None)
            if representative is not None: representatives.append(offset+representative)
            sample = np.unique(np.r_[np.flatnonzero(offset+np.arange(n)<8192), [] if representative is None else [representative]]).astype(np.int64)
            z = a['x']
            require(z.dtype == np.float32 and z.shape == (len(t), len(pm['feature_names'])), 'float32_registered_matrix')
            unsupported = ~a['normalizer_supported']
            require(np.isnan(z[:, unsupported]).all(), 'unsupported_fields_remain_missing')
            replay_indices.append(offset + sample)
            replay_values.append(z[later][sample])
            mid = qt['quote__mid_close'].to_numpy()
            for h in HORIZONS:
                eligible_expected = t + (h + 1) * 60 < np.where(split == 0, pm['boundaries']['train_end'],
                    np.where(split == 1, pm['boundaries']['validation_end'], pm['boundaries']['end']))
                require(np.array_equal(a[f'eligible_{h}'], eligible_expected), 'independent_split_purge_rule')
                fit = train & a[f'valid_{h}'] & a[f'eligible_{h}']
                y = a[f'y_{h}'][fit]
                require(np.isfinite(y).all(), 'finite_mature_training_labels')
                key = np.column_stack((np.full(len(y), pair_id, dtype=np.int64), t[fit])).astype('<i8')
                key_digests[h].update(key.tobytes())
                y_digests[h].update(y.astype('<f8').tobytes())
                train_counts[h][pair] = len(y)
                prior = float(np.mean(y)) if len(y) >= 20 else np.nan
                chunks[f'prior_{h}'].append(np.full(n, prior))
                for stem in ('y', 'long', 'short', 'valid', 'strict', 'eligible'):
                    chunks[f'{stem}_{h}'].append(a[f'{stem}_{h}'][later])
                chunks[f'arima_{h}'].append(qt[f'forecast__arima110_conditional_ols__{h}m_bps'].to_numpy()[later_loc])
                past = np.searchsorted(clocks, later_t - h * 60)
                safe = np.minimum(past, len(clocks) - 1)
                available = ((past < len(clocks)) & (clocks[safe] == later_t - h * 60)
                             & np.isfinite(mid[later_loc]) & np.isfinite(mid[safe]) & (mid[safe] > 0))
                with np.errstate(invalid='ignore', divide='ignore', over='ignore'):
                    momentum = (mid[later_loc] / mid[safe] - 1.) * 10000.
                momentum[~available | ~np.isfinite(momentum)] = np.nan
                chunks[f'momentum_{h}'].append(momentum)
                for stem, field in (('delay_long', 'long_net_bps'), ('delay_short', 'short_net_bps'), ('delay_valid', 'valid')):
                    chunks[f'{stem}_{h}'].append(qt[f'delayed_label__{h}m__{field}'].to_numpy()[later_loc])
            offset += n
    data = {name: np.concatenate(values) for name, values in chunks.items()}
    data.update(pair_names=pair_names, replay_indices=np.concatenate(replay_indices),
                replay_x=np.concatenate(replay_values), representatives=np.asarray(representatives))
    training = {str(h): {'rows': sum(train_counts[h].values()), 'counts_by_pair': train_counts[h],
                        'pair_clock_sha256': key_digests[h].hexdigest(), 'target_sha256': y_digests[h].hexdigest()}
                for h in HORIZONS}
    return data, training, training_clock_rows


def audit_sources():
    names=('tools/audit_rolling_period_replication_v1.py','tools/audit_rolling_specialists_v1.py',
           'tools/audit_rolling_model_comparison_v1.py','oanda_rolling_model_design_v1.py',
           'oanda_rolling_specialists_v1.py','oanda_rolling_family_design_v1.py')
    return {name:sha(ROOT/name) for name in names}


def representatives_for(ids,eligible=None):
    ids=np.asarray(ids)
    positions=np.arange(len(ids)) if eligible is None else np.flatnonzero(eligible)
    return np.asarray([positions[np.flatnonzero(ids[positions]==pid)[0]]
                       for pid in np.unique(ids[positions])],dtype=np.int64)


def validate_grid(result):
    require(result.get('schema')==RESULT_SCHEMA and result.get('status')=='complete','complete_replication_schema')
    require(set(result['contexts'])=={f'{g}_{h}m' for g in GROUPS for h in HORIZONS},'fixed_four_replication_contexts')
    require(set(result['family_contexts'])==set(family_design.GROUPS),'eight_family_contexts_required')
    expected={f'comparator_{g}_{learner}_{h}m' for g in GROUPS for learner in ('ridge','hgb') for h in HORIZONS}
    expected|={f'comparator_{name}_{h}m' for name in previous_audit.CONTROL_NAMES for h in HORIZONS}
    require(set(result['comparators'])==expected,'eight_learned_ten_control_comparators')
    require(result['completed_base_bundles']==20 and result['completed_variants']==20
            and result['completed_family_refits']==7 and result['completed_family_mean_variants']==16
            and result['completed_comparator_fits']==8,'completed_fixed_replication_counts')
    require(set(result['component_priors'])==set(result['pair_priors'])=={'30','60'},'two_horizon_prior_sets')
    require(not result['can_place_orders'] and not result['models_promoted'],'research_only_unpromoted_result')
    reference=result['contexts']['compact50_60m'];full=result['family_contexts']['full_compact50']
    require(full['reused_context']=='compact50_60m' and full['model']==reference['final_model']
            and full['head_forecasts']==reference['head_forecasts']
            and full['component_baseline_scores']==reference['component_baseline_scores']
            and full['variants']=={name:reference['variants'][name] for name in ('direct','mixture_raw')},
            'full_family_reference_must_reuse_exact_saved_artifacts')


def validate_period(im,pm,qm,result):
    b=im['boundaries'];week=7*86400
    require(im.get('schema')=='rolling_specialist_fold_inputs_v2_20260915','generalized_preparation_schema')
    require(b==pm['boundaries']==qm['boundaries']==result['boundaries'],'same_period_boundaries')
    require(set(b)=={'start','train_end','validation_end','end'} and
            all(isinstance(t,int) and not isinstance(t,bool) and t%60==0 for t in b.values()),'integer_minute_boundaries')
    require((b['train_end']-b['start'],b['validation_end']-b['train_end'],b['end']-b['validation_end'])==(6*week,week,week),'exact_six_plus_one_plus_one_weeks')
    require(b['start']%86400==0 and datetime.fromtimestamp(b['start'],timezone.utc).weekday()==0,'monday_utc_period')
    require(b['end']<=int(datetime(2026,7,13,tzinfo=timezone.utc).timestamp()),'period_before_original_july13_study')
    cuts=[b['start']+i*week for i in (2,3,4,5,6)]
    require(im['fold_cutoffs_epoch']==result['fold_cutoffs_epoch']==cuts,'period_derived_oof_cutoffs')
    require(len(im['pairs'])==68 and set(im['pairs'])==set(pm['pairs'])==set(qm['pairs']),'same68_pairs')


def verify_prefix_inputs(data,im):
    base=Path(im['base_root']);manifest=read_json(base/'DATASET.json')
    require(sha(base/'DATASET.json')==im['base_sha256'],'exact_base_for_prefix_reconstruction')
    b=im['boundaries'];previous=None;buffered=[];done={};partitions=0
    def process(pair,tables):
        require(pair not in done,'single_contiguous_raw_pair_group')
        times=np.concatenate([table['bar_start_epoch'].to_numpy() for table in tables])
        raw=np.concatenate([np.column_stack([table[name].to_numpy() for name in data['feature_names']]) for table in tables])
        require(len(times)==manifest['pairs'][pair]['origin_rows'] and np.all(np.diff(times)>0)
                and np.all((times>=b['start'])&(times<b['end'])),'complete_original_raw_period_population')
        pid=data['pair_names'].index(pair);rows=np.flatnonzero(data['pair_id']==pid)
        keep=(times>=b['train_end'])|(times%900==0)
        require(exact_arrays(times[keep],data['time'][rows]) and exact_arrays(raw[keep],data['raw_x'][rows]),'all_retained_raw_values_and_missing_bits')
        expected=np.where(times[keep]<b['train_end'],0,np.where(times[keep]<b['validation_end'],1,2)).astype(np.int8)
        require(exact_arrays(expected,data['split'][rows]),'original_period_split_reconstruction')
        counts=[]
        for fold,cutoff in enumerate(data['cutoffs']):
            train=(times>=b['start'])&(times<cutoff);params=fit_normalizer(raw[train]);counts.append(int(train.sum()))
            for name,value in params.items():require(exact_arrays(value,data['normalizers'][name][fold,pid]),'prefix_all_raw_origin_statistics_exact')
        require(counts==im['pairs'][pair]['prefix_original_rows_by_cutoff'],'saved_prefix_origin_counts')
        done[pair]={'raw_rows':len(times),'retained_rows':len(rows),'prefix_original_rows':counts,'all_five_normalizers_exact':True}
    for pair,table in iter_partitions(base,feature_names=data['feature_names']):
        require(all(p==pair for p in table['instrument'].to_pylist()),'raw_pair_identity')
        require(np.array_equal(table['bar_end_epoch'].to_numpy(),table['bar_start_epoch'].to_numpy()+60),'raw_bar_end_identity')
        if previous is not None and pair!=previous:process(previous,buffered);buffered=[]
        previous=pair;buffered.append(table);partitions+=1
    if buffered:process(previous,buffered)
    require(set(done)==set(im['pairs']) and partitions==len(manifest['partitions']),'all_raw_partitions_and_pairs_reconstructed')
    require(sha(base/'DATASET.json')==im['base_sha256'],'base_manifest_unchanged_after_prefix_read')
    return {'pair_count':len(done),'normalizer_sets':len(done)*5,'raw_partitions':partitions,'pairs':done,
            'scope':'all original registered raw feature rows; no candle regeneration; unchanged pinned normalizer arithmetic'}


def verify_input_files(inputs,im,pm,qm):
    prepared=Path(im['prepared_root']);quotes=Path(im['quote_root']);base=Path(im['base_root'])
    require(sha(prepared/'PREPARED.json')==im['prepared_sha256'] and sha(quotes/'QUOTE_PANEL.json')==im['quote_sha256']
            and sha(base/'DATASET.json')==im['base_sha256'],'all_input_manifests_unchanged')
    for pair,item in im['pairs'].items():
        checked(inputs,item['path'],item['sha256'])
        p=pm['pairs'][pair];q=qm['pairs'][pair]
        checked(prepared,p['path'],p['sha256']);checked(quotes,q['path'],q['sha256']);checked(quotes,q['receipt_path'],q['receipt_sha256'])
    base_manifest=read_json(base/'DATASET.json')
    for partition in base_manifest['partitions']:
        for name in ('core','peers'):
            rec=partition[name];checked(base,rec['path'],rec['sha256'])


def family_rows(data,rows,group,design):
    expected=family_design.group_manifest(data['feature_names'])
    require(design==expected,'exact_canonical_family_design')
    record=design['groups'][group];keep=record['kept_indices'];removed=record['removed_indices']
    require(sorted(keep+removed)==list(range(50)) and not set(keep)&set(removed),'partitioned_technical_mask_only')
    original=base_rows(data,rows,'compact50',4);out=original.copy();out[:,removed]=np.nan
    require(np.isnan(out[:,removed]).all() and exact_arrays(out[:,keep],original[:,keep])
            and exact_arrays(out[:,50:],original[:,50:]),'masked_family_values_absent_and_quote_context_preserved')
    return out


def verify_family_tree_features(estimator,removed):
    require(estimator.is_categorical_.tolist()==[False]*52+[True]
            and all(estimator.get_params()[k]==v for k,v in contract.BASE_RECIPE.items()),'family_fixed_head_recipe')
    for trees in estimator._predictors:
        for tree in trees:
            splits=tree.nodes['feature_idx'][tree.nodes['is_leaf']==0]
            require(not np.isin(splits,removed).any(),'saved_family_trees_cannot_split_removed_columns')


def verify_comparators(root,result,data,assessment,ad):
    records={}
    for tag,rec in result['comparators'].items():
        h=rec['horizon_minutes'];table=read_forecast(root,rec['forecast'],ad);prediction=table['predicted_bps'].to_numpy()
        report={'rows':len(prediction),'scalar_cost_cases':0}
        if 'model' in rec:
            saved=joblib.load(checked(root,rec['model']['path'],rec['model']['sha256']));group=rec['group'];learner=rec['learner']
            train=selection(data,mature_rows(data,h,data['cutoffs'][-1]),h)
            require(saved['training_selection']==rec['training_selection']==train and saved['inputs_sha256']==result['inputs_sha256'],'comparator_same_prefix_training_identity')
            width=38 if group=='compact38' else 50
            require(saved['feature_names']==data['feature_names'][:width] and saved['group']==group and saved['learner']==learner and saved['horizon_minutes']==h,'comparator_feature_and_role_identity')
            model=saved['model'];require(all(model.get_params()[k]==v for k,v in RECIPES[learner].items()),'unchanged_comparator_recipe')
            if learner=='hgb':require(model.is_categorical_.tolist()==[False]*width+[True],'comparator_pair_category_only')
            def matrix(rows):return previous_audit.learner_matrix(normalized_rows(data,rows,4)[:,:width],data['pair_id'][rows],learner,68)
            first=np.arange(min(8192,len(assessment)));replay=model.predict(matrix(assessment[first]))
            require(previous_audit.finite_bits_equal(replay,prediction[first]),'comparator_first_chunk_exact')
            reps=representatives_for(ad['pair_id'],ad['split']==2);replay=model.predict(matrix(assessment[reps]))
            require(np.allclose(replay,prediction[reps],rtol=1e-12,atol=1e-10),'comparator_later_representatives')
            report.update(first_chunk_exact=len(first),later_representatives=len(reps),training=train)
        else:
            expected={'no_change':np.zeros(len(assessment)),'pair_train_mean':ad[f'prior_{h}'],
                'arima110_conditional_ols':ad[f'arima_{h}'],'momentum_exact_past_horizon':ad[f'momentum_{h}'],
                'reversal_exact_past_horizon':-ad[f'momentum_{h}']}[rec['name']]
            require(previous_audit.finite_bits_equal(prediction,expected),'control_full_population_reconstruction')
            report['entire_control_recreated']=True
        metrics=read_json(checked(root,rec['metrics']['path'],rec['metrics']['sha256']))
        report['scalar_cost_cases']=previous_audit.audit_scores(ad,prediction,h,metrics);records[tag]=report
    return records


def component_targets(data,h):
    y=data[f'y_{h}'];valid=data[f'valid_{h}'];left=y-data['entry_long']-data[f'long_{h}'];right=-y-data['entry_short']-data[f'short_{h}']
    require(np.isfinite(np.column_stack((y[valid],left[valid],right[valid]))).all() and np.all(left[valid]>=-1e-7) and np.all(right[valid]>=-1e-7),'finite_valid_component_targets')
    return {'expected_absolute_move':(np.abs(y),valid),'positive_magnitude':(y,valid&(y>0)),
        'nonpositive_magnitude':(-y,valid&(y<=0)),'long_exit_cost':(np.maximum(left,0.),valid),'short_exit_cost':(np.maximum(right,0.),valid)}


def population_hash(data,rows):
    return hashlib.sha256(np.column_stack((data['pair_id'][rows],data['time'][rows])).astype('<i8').tobytes()).hexdigest()


def verify_component_prior(data,h,record):
    fit=mature_rows(data,h,data['cutoffs'][-1]);selection_record=selection(data,fit,h)
    require(record['training_selection']=={k:selection_record[k] for k in ('rows','pair_clock_sha256','targets_y_exit_long_exit_short_sha256','maximum_target_end_epoch')},'component_reference_exact_supervised_population')
    targets=component_targets(data,h);require(set(record['parameters'])==set(targets),'exact_five_component_targets')
    for component,(values,valid) in targets.items():
        require(set(record['parameters'][component])==set(data['pair_names']),'component_all_pair_parameters')
        for pid,pair in enumerate(data['pair_names']):
            rows=fit[(data['pair_id'][fit]==pid)&valid[fit]&np.isfinite(values[fit])];v=values[rows];supported=len(v)>=20
            expected={'finite_training_labels':len(v),'supported':supported,'mean_bps':float(np.mean(v)) if supported else None,
                'median_bps':float(np.median(v)) if supported else None,'training_component_pair_clock_sha256':population_hash(data,rows)}
            require(record['parameters'][component][pair]==expected,'component_reference_parameters_recreated')
    encoded=json.dumps(record['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    require(hashlib.sha256(encoded).hexdigest()==record['parameters_sha256'],'component_parameters_digest')


def error_scores(prediction,target):
    err=np.asarray(prediction)-np.asarray(target);require(np.isfinite(err).all(),'finite_component_errors')
    return {'rows':len(err),'mae_bps':float(np.mean(np.abs(err))) if len(err) else None,
        'rmse_bps':float(np.sqrt(np.mean(err*err))) if len(err) else None,'mean_error_bps':float(np.mean(err)) if len(err) else None}


def verify_component_scores(root,record,data,assessment,heads,h,group,baseline):
    wrapper=read_json(checked(root,record['path'],record['sha256']))
    require(wrapper['schema']=='rolling_replication_component_baselines_v1' and wrapper['horizon_minutes']==h and wrapper['group']==group
            and wrapper['baseline_parameters_sha256']==baseline['parameters_sha256'],'component_wrapper_identity')
    targets=component_targets(data,h);p=heads['probability']
    forecasts={'expected_absolute_move':p*heads['positive']+(1-p)*heads['nonpositive'],'positive_magnitude':heads['positive'],
        'nonpositive_magnitude':heads['nonpositive'],'long_exit_cost':heads['long_exit'],'short_exit_cost':heads['short_exit']}
    seen=set()
    support=np.array([baseline['parameters']['expected_absolute_move'][pair]['supported'] for pair in data['pair_names']])[data['pair_id'][assessment]]
    common=np.isfinite(data[f'arima_{h}'][assessment])&np.isfinite(data[f'momentum_{h}'][assessment])&support
    for row in wrapper['rows']:
        identity=(row['split'],row['cohort'],row['component']);require(identity not in seen,'unique_component_comparison_cell');seen.add(identity)
        split,cohort,component=identity;sid={'validation':1,'later_development_test':2}[split]
        require(row['group']==group and row['horizon_minutes']==h and row['context']==f'{group}_{h}m','component_row_identity')
        mask=(data['split'][assessment]==sid)&common&data[f'valid_{h}'][assessment]&data[f'eligible_{h}'][assessment]
        if cohort=='shared_strict':mask&=data[f'strict_{h}'][assessment]
        elif cohort=='additional_endpoint_only':mask&=~data[f'strict_{h}'][assessment]
        else:require(cohort=='full_endpoint','known_component_cohort')
        values,valid=targets[component];before=mask&valid[assessment]&np.isfinite(values[assessment]);param=baseline['parameters'][component]
        supported=np.array([param[pair]['supported'] for pair in data['pair_names']])[data['pair_id'][assessment]];matched=before&supported;rows=assessment[matched]
        require(row['cohort_endpoint_origins']==int(mask.sum()) and row['component_label_rows_before_baseline_support']==int(before.sum())
                and row['excluded_insufficient_component_TRAIN_support']==int((before&~supported).sum()) and row['matched_rows']==len(rows),'component_same_row_masks')
        require(row['matched_pair_clock_sha256']==population_hash(data,rows)
                and row['matched_target_float64_sha256']==hashlib.sha256(values[rows].astype('<f8').tobytes()).hexdigest(),'component_key_and_target_hashes')
        require(row['matched_counts_by_pair']=={pair:int(np.sum(data['pair_id'][rows]==pid)) for pid,pair in enumerate(data['pair_names'])},'component_pair_counts')
        mean=np.array([param[pair]['mean_bps'] if param[pair]['supported'] else np.nan for pair in data['pair_names']])[data['pair_id'][rows]]
        median=np.array([param[pair]['median_bps'] if param[pair]['supported'] else np.nan for pair in data['pair_names']])[data['pair_id'][rows]]
        scores={'model_before_baseline_support':error_scores(forecasts[component][before],values[assessment[before]]),
                'model':error_scores(forecasts[component][matched],values[rows]),'TRAIN_pair_mean':error_scores(mean,values[rows]),'TRAIN_pair_median':error_scores(median,values[rows])}
        for name,scoreset in scores.items():
            for metric,value in scoreset.items():previous_audit.close_number(row[name][metric],value,'component_'+name+'_'+metric)
        previous_audit.close_number(row['mae_improvement_vs_TRAIN_median_bps'],None if not len(rows) else scores['TRAIN_pair_median']['mae_bps']-scores['model']['mae_bps'],'component_mae_gain')
        previous_audit.close_number(row['rmse_improvement_vs_TRAIN_mean_bps'],None if not len(rows) else scores['TRAIN_pair_mean']['rmse_bps']-scores['model']['rmse_bps'],'component_rmse_gain')
        if component in ('long_exit_cost','short_exit_cost'):
            entry='entry_short' if component=='long_exit_cost' else 'entry_long';expected=error_scores(data[entry][rows],values[rows]);persist=row['current_quote_wing_persistence']
            require(persist['current_input']==entry,'asymmetric_exit_persistence_uses_opposite_entry_wing')
            for metric,value in expected.items():previous_audit.close_number(persist['score'][metric],value,'persistence_'+metric)
            for metric in ('mae','rmse'):previous_audit.close_number(row[f'{metric}_improvement_vs_persistence_bps'],None if not len(rows) else expected[metric+'_bps']-scores['model'][metric+'_bps'],'persistence_gain')
    expected={(s,c,t) for s in ('validation','later_development_test') for c in ('full_endpoint','shared_strict','additional_endpoint_only') for t in targets}
    require(seen==expected,'all30_component_cells');return len(seen)


def verify_families(root,result,data,assessment,ad):
    design=result['family_design'];require(design==family_design.group_manifest(data['feature_names']),'registered_family_contract')
    h=60;fit=mature_rows(data,h,data['cutoffs'][-1]);training=selection(data,fit,h);report={}
    cp=read_json(checked(root,result['component_priors']['60']['path'],result['component_priors']['60']['sha256']))
    for group,ctx in result['family_contexts'].items():
        require(ctx['group']==group and ctx['horizon_minutes']==60 and set(ctx['variants'])=={'direct','mixture_raw'},'family_base_direct_and_raw_only')
        if group=='full_compact50':report[group]={'reused_exact_full_context':True,'scalar_cost_cases':0};continue
        record=ctx['model'];saved=joblib.load(checked(root,record['path'],record['sha256']))
        require(record['training']==saved['training_selection']==training and saved['inputs_sha256']==result['inputs_sha256']
                and saved['family_design']==design and saved['fit_cutoff_epoch']==data['cutoffs'][-1]
                and saved['input_columns']==53 and saved['group']==group and saved['horizon_minutes']==h,'family_same_training_and_explicit_mask_metadata')
        model=saved['bundle'];require(model['input_columns']==53 and model['registered_input_count']==50,'fixed53_family_head_layout')
        y=data[f'y_{h}'][fit]
        require(model['training_rows']==len(fit) and model['positive_rows']==int((y>0).sum()) and model['nonpositive_rows']==int((y<=0).sum()) and model['flat_rows']==int((y==0).sum()),'family_conditional_training_counts')
        removed=design['groups'][group]['removed_indices']
        for estimator in model['models'].values():verify_family_tree_features(estimator,removed)
        table=read_forecast(root,ctx['head_forecasts'],ad);heads={name:table[name].to_numpy() for name in HEADS}
        first=np.arange(min(8192,len(assessment)));reps=representatives_for(ad['pair_id'],ad['split']==2)
        for selected,exact in ((first,True),(reps,False)):
            replay=replay_heads(model,family_rows(data,assessment[selected],group,design))
            for name in HEADS:require(previous_audit.finite_bits_equal(replay[name],heads[name][selected]) if exact else np.allclose(replay[name],heads[name][selected],rtol=1e-12,atol=1e-10),'masked_family_model_replay')
        means={'direct':heads['direct'],'mixture_raw':heads['probability']*heads['positive']-(1-heads['probability'])*heads['nonpositive']};cases=0
        for name,variant in ctx['variants'].items():
            pred=read_forecast(root,variant['forecast'],ad)['predicted_bps'].to_numpy();require(previous_audit.finite_bits_equal(pred,means[name]),'every_family_mean_identity')
            metrics=read_json(checked(root,variant['metrics']['path'],variant['metrics']['sha256']))
            cases+=audit_gate_scores(ad,pred,h,metrics,data['entry_long'][assessment],data['entry_short'][assessment],heads)
        count=verify_component_scores(root,ctx['component_baseline_scores'],data,assessment,heads,h,group,cp)
        report[group]={'training':training,'first_chunk_exact_each_head':len(first),'later_pair_representatives':len(reps),'removed_columns_never_used_by_saved_trees':removed,'component_rows_verified':count,'scalar_cost_cases':cases}
    return report

def run(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    require(root.is_relative_to(ROOT / 'data') and output.is_relative_to(ROOT / 'docs' / 'validation') and not output.exists(), 'bounded_new_audit_output')
    result_path = root / 'RESULTS.json'
    result_hash = sha(result_path)
    result = read_json(result_path)
    require(result['status'] == 'complete' and result['completed_base_bundles'] == 20 and result['completed_variants'] == 20 and result.get('schema') == RESULT_SCHEMA, 'complete_20_bundle_20_variant_replication_only')
    require(set(result['contexts']) == {f'{group}_{h}m' for group in GROUPS for h in HORIZONS}, 'complete_four_contexts')
    validate_grid(result)
    check_sources(result['source_bindings'])
    audit_pins = audit_sources()
    inventory = result['artifacts']
    actual_files = {p.relative_to(root).as_posix() for sub in SUBDIRS for p in (root / sub).iterdir()}
    require(set(inventory) == actual_files, 'complete_artifact_inventory')
    for name, item in inventory.items():
        path = checked(root, name, item['sha256'])
        require(path.stat().st_size == item['bytes'], 'artifact_size_identity')
    references = sum(check_references(root,result[section],inventory) for section in ('contexts','pair_priors','family_contexts','comparators','component_priors'))
    inputs = Path(result['inputs_root'])
    require(sha(inputs / 'SPECIALIST_INPUTS.json') == result['inputs_sha256'], 'raw_fold_input_binding')
    im = read_json(inputs / 'SPECIALIST_INPUTS.json')
    require(im['status'] == 'complete' and len(im['pairs']) == 68, 'complete_68_fold_inputs')
    prepared, quotes = Path(im['prepared_root']), Path(im['quote_root'])
    require(sha(prepared / 'PREPARED.json') == im['prepared_sha256'] and sha(quotes / 'QUOTE_PANEL.json') == im['quote_sha256'], 'bound_original_prepared_quote_manifests')
    pm, qm = read_json(prepared / 'PREPARED.json'), read_json(quotes / 'QUOTE_PANEL.json')
    require(pm['base_sha256'] == qm['base_manifest_sha256'] == im['base_sha256'] and pm['overlay_sha256'] == qm['endpoint_manifest_sha256'] == im['endpoint_manifest_sha256'], 'base_endpoint_generation_identity')
    validate_period(im,pm,qm,result)
    old_path = Path(result['previous_comparison_root']) / 'RESULTS.json'
    require(sha(old_path) == result['previous_comparison_sha256'], 'previous_comparison_unchanged')
    specialist_reference=Path(result['prior_specialist_results_path'])
    require(sha(specialist_reference)==result['prior_specialist_results_sha256']=='43008a70624bec3ce6d25003c4f3dcb486b678019d01da99a7256bdc07233f1d','prior_specialist_reference_unchanged')
    endpoint_root = Path(pm['overlay'])
    require(sha(endpoint_root / 'ENDPOINT_DATASET.json') == pm['overlay_sha256'], 'old_endpoint_manifest_unchanged')
    endpoint = read_json(endpoint_root / 'ENDPOINT_DATASET.json')
    for item in endpoint['pairs'].values():
        checked(endpoint_root, item['receipt_path'], item['receipt_sha256'])
    started = time.monotonic()
    assessment_data, _, sampled_count = load_expected(pm, qm, prepared, quotes)
    data = load_full_inputs(inputs, im, pm)
    require(data['pair_names'] == result['pair_names'] and result['groups'] == {name: im['groups'][name] for name in GROUPS}, 'declared_pair_and_group_mapping')
    require(result['meta_context_names'] == list(contract.CONTEXT_NAMES), 'declared_meta_context_mapping')
    assessment = np.flatnonzero(data['split'] != 0)
    require(len(assessment) == im['assessment_rows'] == result['assessment_rows'] and sampled_count == im['sampled_training_rows'], 'manifest_assessment_and_train_sizes')
    for name in ('pair_id', 'time', 'split'):
        require(np.array_equal(data[name][assessment], assessment_data[name]), 'exact_assessment_from_retained_input_keys')
    require(np.allclose(data['entry_long'][assessment] + data['entry_short'][assessment], assessment_data['spread'], rtol=1e-12, atol=1e-10), 'known_entry_cost_actual_spread_identity')
    oof, fold_ids = fold_population(data['time'], data['split'], data['cutoffs'])
    require(len(oof) == result['oof_issued_origins'] and np.array_equal(data['cutoffs'], result['fold_cutoffs_epoch']), 'all_original_oof_origins')
    prefix_report = verify_prefix_inputs(data,im)
    data['boundaries'] = im['boundaries']
    for h in HORIZONS:
        for stem in ('arima','momentum'):
            data[f'{stem}_{h}'] = np.full(len(data['time']),np.nan)
            data[f'{stem}_{h}'][assessment] = assessment_data[f'{stem}_{h}']
    context_columns = [data['feature_names'].index(name) for name in contract.CONTEXT_NAMES]
    oof_context = np.full((len(oof), 8), np.nan, dtype=np.float32)
    for fold in range(4):
        selected = fold_ids == fold
        oof_context[selected] = normalized_rows(data, oof[selected], fold)[:, context_columns]
    final_context = normalized_rows(data, assessment, 4)[:, context_columns]
    report = {'status': 'running', 'result_root': str(root), 'results_sha256': result_hash,
              'inputs_sha256': result['inputs_sha256'], 'audit_source_sha256': sha(__file__),
              'audit_helper_sha256': sha(previous_audit.__file__), 'audit_source_bindings': audit_pins, 'prefix_preprocessing_verified': prefix_report, 'comparators': {}, 'families': {}, 'started_utc': datetime.now(timezone.utc).isoformat(),
              'artifact_inventory_files': len(inventory), 'stored_artifact_references_verified': references,
              'assessment_origins': len(assessment), 'oof_origins': len(oof), 'sampled_train_origins': sampled_count,
              'endpoint_receipts_verified': len(endpoint['pairs']), 'contexts': {}, 'scalar_cost_cases': 0,
              'limits': ['No model or calibration refit; saved parameter application is replayed on bounded rows.',
                         'No original candles or full feature regeneration repeated; accepted source-bound artifacts are used.',
                         'Every forecast key and gate decision digest is checked; every concentration ranking and every diagnostic field is not independently recomputed.',
                         'Follow-up development research on previously examined dates; computational consistency is not profitability.']}
    with threadpool_limits(limits=4):
        for tag, ctx in result['contexts'].items():
            h, group = ctx['horizon_minutes'], ctx['group']
            require(tag == f'{group}_{h}m' and group in GROUPS and h in HORIZONS, 'context_identity')
            require(len(ctx['oof_models']) == 4 and set(ctx['meta_models']) == set(META_ARMS) and set(ctx['variants']) == set(VARIANTS), 'all_context_models_and_variants')
            with np.load(checked(root, ctx['oof']['path'], ctx['oof']['sha256']), allow_pickle=False) as a:
                for name, expected in (('pair_id', data['pair_id'][oof]), ('time', data['time'][oof]), ('fold', fold_ids)):
                    require(exact_arrays(a[name], expected), 'every_original_oof_key_and_fold')
                meta_mask = data[f'valid_{h}'][oof] & (data['time'][oof] + (h + 1) * 60 < data['cutoffs'][fold_ids + 1])
                require(exact_arrays(a['meta_fit_mask'], meta_mask), 'strict_per_oof_block_meta_purge')
                oof_heads = {name: a[name].copy() for name in HEADS}
                require(all(np.isfinite(value).all() for value in oof_heads.values()), 'complete_six_head_oof_issuance')
                meta = raw_meta(oof_heads, oof_context, data['entry_long'][oof], data['entry_short'][oof])
                require(exact_arrays(a['meta_x'], meta), 'exact_uncalibrated_raw_head_meta_reconstruction')
            meta_record = selection(data, oof[meta_mask], h)
            require(ctx['oof']['meta_fit_selection'] == meta_record, 'actual_meta_training_selection')
            head_table = read_forecast(root, ctx['head_forecasts'], assessment_data)
            final_heads = {name: head_table[name].to_numpy() for name in HEADS}
            require(all(np.isfinite(value).all() for value in final_heads.values()), 'every_assessment_has_six_head_values')
            final_meta = raw_meta(final_heads, final_context, data['entry_long'][assessment], data['entry_short'][assessment])
            context_record = {'meta_training': meta_record, 'base_bundles': [], 'meta_bundles': {}, 'variants': {}}
            for fold, record in enumerate([*ctx['oof_models'], ctx['final_model']]):
                fit = mature_rows(data, h, int(data['cutoffs'][fold]))
                expected_selection = selection(data, fit, h)
                require(record['training'] == expected_selection and expected_selection['maximum_target_end_epoch'] < data['cutoffs'][fold], 'fold_training_strict_maturity_and_hashes')
                saved = joblib.load(checked(root, record['path'], record['sha256']))
                require(saved['training_selection'] == expected_selection and saved['fold_index'] == fold and saved['fit_cutoff_epoch'] == data['cutoffs'][fold], 'saved_fold_identity_and_training')
                require(saved['inputs_sha256'] == result['inputs_sha256'] and saved['group'] == group and saved['horizon_minutes'] == h, 'saved_head_input_group_horizon_binding')
                bundle = saved['bundle']
                y = data[f'y_{h}'][fit]
                require(bundle['training_rows'] == len(fit) and bundle['positive_rows'] == int((y > 0).sum()) and bundle['nonpositive_rows'] == int((y <= 0).sum()) and bundle['flat_rows'] == int((y == 0).sum()), 'actual_conditional_head_populations')
                width = 41 if group == 'compact38' else 53
                for name, model in bundle['models'].items():
                    require(model.is_categorical_.tolist() == [False] * (width - 1) + [True], 'every_saved_head_categorical_pair_mask')
                    for param, value in contract.BASE_RECIPE.items():
                        require(model.get_params()[param] == value, 'fixed_saved_head_recipe')
                predicted_rows = oof[fold_ids == fold] if fold < 4 else assessment
                expected_heads = {name: value[fold_ids == fold] for name, value in oof_heads.items()} if fold < 4 else final_heads
                require(len(predicted_rows) == record['prediction_rows'], 'all_fold_issuance_count')
                first_count = min(8192, len(predicted_rows))
                replay = replay_heads(bundle, base_rows(data, predicted_rows[:first_count], group, fold))
                for name in HEADS:
                    require(previous_audit.finite_bits_equal(replay[name], expected_heads[name][:first_count]), 'bounded_head_first_chunk_exact_bits')
                representatives = representatives_for(data['pair_id'][predicted_rows])
                replay = replay_heads(bundle, base_rows(data, predicted_rows[representatives], group, fold))
                for name in HEADS:
                    require(np.allclose(replay[name], expected_heads[name][representatives], rtol=1e-12, atol=1e-10), 'all68_head_representatives_replay')
                context_record['base_bundles'].append({'fold': fold, 'training': expected_selection, 'prediction_rows': len(predicted_rows), 'first_chunk_exact_each_head': first_count, 'pair_representatives_each_head': len(representatives)})
            calibration = read_json(checked(root, ctx['calibration']['path'], ctx['calibration']['sha256']))
            targets = data[f'y_{h}'][oof][meta_mask]
            require(calibration['rows'] == len(targets) and calibration['positive_rows'] == int((targets > 0).sum()) and calibration['flat_rows'] == int((targets == 0).sum()), 'calibration_uses_only_mature_oof_target_population')
            require(calibration['configuration'] == contract.CALIBRATION_RECIPE and 0 <= calibration['slope'] <= 8 and -8 <= calibration['intercept'] <= 8, 'fixed_bounded_calibration_recipe')
            calibrated_probability = apply_platt_independently(final_heads['probability'], calibration)
            expected_variants = {'direct': final_heads['direct'], 'mixture_raw': final_meta[:, 6],
                                 'mixture_calibrated': calibrated_probability * final_heads['positive'] - (1 - calibrated_probability) * final_heads['nonpositive']}
            prior_record = read_json(checked(root, result['pair_priors'][str(h)]['path'], result['pair_priors'][str(h)]['sha256']))
            require(set(prior_record) == set(data['pair_names']), 'all68_saved_pair_priors')
            prior_fit = mature_rows(data, h, data['cutoffs'][-1])
            for pid, pair in enumerate(data['pair_names']):
                y = data[f'y_{h}'][prior_fit[data['pair_id'][prior_fit] == pid]]
                expected_prior = {'status': 'available' if len(y) >= 20 else 'insufficient_training_labels',
                                  'training_rows': len(y), 'mean_bps': float(np.mean(y)) if len(y) else None,
                                  'p_up': float(np.mean(y > 0)) if len(y) else None,
                                  'p_down': float(np.mean(y < 0)) if len(y) else None,
                                  'p_flat': float(np.mean(y == 0)) if len(y) else None}
                require(prior_record[pair] == expected_prior, 'independent_saved_prior_parameters')
            prior_positive = np.asarray([prior_record[pair]['p_up'] if prior_record[pair]['status'] == 'available' else np.nan for pair in data['pair_names']])[assessment_data['pair_id']]
            probability_scores = read_json(checked(root, ctx['probability_calibration_scores']['path'], ctx['probability_calibration_scores']['sha256']))
            verify_probability_scores(assessment_data, h, final_heads['probability'], calibrated_probability, prior_positive, probability_scores)
            for name, entry in ctx['variants'].items():
                table = read_forecast(root, entry['forecast'], assessment_data)
                prediction = table['predicted_bps'].to_numpy()
                require(np.isfinite(prediction).all(), 'all_original_variant_forecasts_finite')
                if name in expected_variants:
                    require(previous_audit.finite_bits_equal(prediction, expected_variants[name]), 'all_row_direct_or_mixture_recreation')
                else:
                    saved = joblib.load(checked(root, ctx['meta_models'][name]['path'], ctx['meta_models'][name]['sha256']))
                    require(saved['training_selection'] == meta_record and saved['oof_sha256'] == ctx['oof']['sha256'] and saved['inputs_sha256'] == result['inputs_sha256'], 'saved_meta_training_source_binding')
                    bundle = saved['bundle']
                    require(bundle['arm'] == name and not bundle['probability_calibration_in_inputs'] and bundle['issued_oof_rows'] == len(oof) and bundle['training_rows'] == int(meta_mask.sum()), 'raw_oof_only_meta_training_metadata')
                    if name.endswith('_ridge'):
                        expected_scaler = fit_normalizer(meta[:, contract.META_COLUMNS[name]])
                        require(bundle['scaler_rows'] == len(oof) and all(exact_arrays(value, bundle['scaler'][key]) for key, value in expected_scaler.items()), 'meta_scaler_all_issued_oof_before_target_filter')
                    else:
                        width = len(contract.META_COLUMNS[name])
                        require(bundle['model'].is_categorical_.tolist() == [False] * width + [True], 'saved_meta_categorical_pair_field')
                    recipe = contract.META_RIDGE_RECIPE if name.endswith('_ridge') else contract.META_HGB_RECIPE
                    require(all(bundle['model'].get_params()[key] == value for key, value in recipe.items()), 'fixed_saved_meta_recipe')
                    first = np.arange(min(8192, len(assessment)))
                    replay = replay_meta(bundle, final_meta[first], data['pair_id'][assessment[first]])
                    require(previous_audit.finite_bits_equal(replay, prediction[first]), 'meta_first_chunk_exact_bits')
                    reps = representatives_for(assessment_data['pair_id'],assessment_data['split']==2)
                    replay = replay_meta(bundle, final_meta[reps], data['pair_id'][assessment[reps]])
                    require(np.allclose(replay, prediction[reps], rtol=1e-12, atol=1e-10), 'all68_later_meta_representatives_replay')
                    context_record['meta_bundles'][name] = {'first_chunk_exact': len(first), 'later_pair_representatives': len(reps), 'meta_training_rows': int(meta_mask.sum())}
                metrics = read_json(checked(root, entry['metrics']['path'], entry['metrics']['sha256']))
                cases = audit_gate_scores(assessment_data, prediction, h, metrics, data['entry_long'][assessment], data['entry_short'][assessment], final_heads)
                report['scalar_cost_cases'] += cases
                context_record['variants'][name] = {'rows': len(prediction), 'keys_exact': True, 'both_gate_decisions_and_cohort_costs_verified': True, 'scalar_cost_cases': cases}
            component_prior = read_json(checked(root,result['component_priors'][str(h)]['path'],result['component_priors'][str(h)]['sha256']))
            verify_component_prior(data,h,component_prior)
            context_record['component_baseline_rows_verified'] = verify_component_scores(root,ctx['component_baseline_scores'],data,assessment,final_heads,h,group,component_prior)
            report['contexts'][tag] = context_record
            print(json.dumps({'audited_context': tag, 'completed_contexts': len(report['contexts']), 'elapsed_seconds': round(time.monotonic() - started, 1)}), flush=True)
        report['comparators'] = verify_comparators(root,result,data,assessment,assessment_data)
        report['families'] = verify_families(root,result,data,assessment,assessment_data)
        report['scalar_cost_cases'] += sum(v['scalar_cost_cases'] for v in report['comparators'].values()) + sum(v['scalar_cost_cases'] for v in report['families'].values())
    verify_input_files(inputs,im,pm,qm)
    for name,item in inventory.items():
        path=checked(root,name,item['sha256']);require(path.stat().st_size==item['bytes'],'final_artifact_size_identity')
    require(sha(specialist_reference)==result['prior_specialist_results_sha256'],'prior_specialist_reference_unchanged_after_audit')
    for name,expected in audit_pins.items(): require(sha(ROOT/name)==expected,'audit_source_changed')
    require(sha(result_path) == result_hash and sha(inputs / 'SPECIALIST_INPUTS.json') == result['inputs_sha256'] and sha(old_path) == result['previous_comparison_sha256'], 'source_manifests_unchanged_after_audit')
    require(sha(endpoint_root / 'ENDPOINT_DATASET.json') == pm['overlay_sha256'], 'endpoint_unchanged_after_audit')
    for item in endpoint['pairs'].values():
        checked(endpoint_root, item['receipt_path'], item['receipt_sha256'])
    check_sources(result['source_bindings'])
    report.update(status='passed', completed_utc=datetime.now(timezone.utc).isoformat(), elapsed_seconds=round(time.monotonic() - started, 3),
                  six_head_bundles_replayed=27, individual_head_estimators_replayed=162, meta_models_replayed=8, learned_comparators_replayed=8, controls_recreated=10,
                  calibration_applications_verified=4, forecast_files_verified=52, head_forecast_files_verified=11,
                  total_assessment_key_rows_verified=len(assessment)*63, oof_context_rows_verified=len(oof)*4,
                  original_artifact_writes=False, refits_performed=0)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('x', encoding='utf-8') as stream:
        json.dump(report, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--comparison', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = run(args.comparison, args.output)
    print(json.dumps({key: result[key] for key in ('status', 'six_head_bundles_replayed', 'meta_models_replayed', 'elapsed_seconds')}))
