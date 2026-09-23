"""Recreate the fixed offline comparison from the retained local snapshot."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import time

import numpy as np
import pandas as pd

import direction_data_v1 as source
import direction_features_v1 as features
import direction_evaluation_v1 as evaluation


def prepare(snapshot, output_root, spec_path, pair_registry):
    root=Path(output_root); root.mkdir(parents=True,exist_ok=False)
    spec_bytes=source._stable_bytes(spec_path,1024*1024)
    spec=json.loads(spec_bytes)
    data=source.load_dataset(snapshot)
    registry_bytes=source._stable_bytes(pair_registry,8*1024*1024)
    registry=json.loads(registry_bytes)
    pip_map={pair:registry['pairs'][pair]['pip_size'] for pair in data['manifest']['pairs']}
    bindings={path.name:evaluation.sha_file(path) for path in Path(__file__).parent.glob('*.py')}
    receipt={'spec':spec,'spec_sha256':hashlib.sha256(spec_bytes).hexdigest(),
        'snapshot_manifest_sha256':evaluation.sha_file(Path(snapshot)/'manifest.json'),
        'snapshot_path':str(Path(snapshot).resolve()),'pair_registry_sha256':hashlib.sha256(registry_bytes).hexdigest(),
        'pip_map':pip_map,'source_bindings':bindings,'feature_metadata':features.metadata(),
        'prepared_started_epoch':time.time(),
        'runtime':{'python':platform.python_version(),**{name:importlib.metadata.version(name)
            for name in ('numpy','pandas','scikit-learn','scipy','joblib','pyarrow','threadpoolctl')}},
        'scope':'retrospective_discovery_only','can_place_orders':False,'can_promote':False}
    (root/'PREPARATION_STARTED.json').write_text(evaluation.canonical(receipt)+'\n')
    start=evaluation.utc(spec['data_start_utc']); end=evaluation.utc(spec['data_end_utc_exclusive'])
    epochs=np.arange(start,end,spec['anchor_stride_minutes']*60,dtype=np.int64)
    print('Building fixed own-price and independent-peer features',flush=True)
    technical=features.build_features(data['prices'],pip_map=pip_map).reset_index()
    technical=technical[technical.epoch.isin(epochs)].copy()
    print('Projecting original-known news at each historical decision',flush=True)
    projected=source.project_news(data,epochs.tolist())
    news=pd.concat([frame.assign(instrument=pair) for pair,frame in projected.items()],ignore_index=True)
    if (news.loc[news.available_max_epoch.notna(),'available_max_epoch'] >
        news.loc[news.available_max_epoch.notna(),'decision_epoch']).any():
        raise ValueError('news_projection_after_decision')
    news['news_available']=news.news_present.astype(float)
    names={name:'news_'+name for name in source.NEWS_FEATURES}
    news=news.rename(columns=names)
    feature_rows=technical.merge(news,on=['instrument','epoch'],how='left',validate='one_to_one')
    if feature_rows.news_available.isna().any():
        raise ValueError('news_row_join_missing')
    # Archive feature rows before labels; future path existence never changes a feature.
    feature_rows.to_parquet(root/'feature_rows.parquet',index=False)
    print('Constructing exact, contiguous future endpoint labels',flush=True)
    labels=features.build_labels(data['prices'],pip_map=pip_map,
        horizons_minutes=tuple(spec['horizons_minutes'])).reset_index()
    labels=labels[labels.epoch.isin(epochs)].copy()
    panel=labels.merge(feature_rows,on=['instrument','epoch'],how='inner',validate='many_to_one')
    if len(panel)!=len(labels):
        raise ValueError('label_missing_feature_row')
    groups={'technical':list(features.TECHNICAL_COLUMNS),
        'technical_peer':list(features.TECHNICAL_COLUMNS+features.PEER_COLUMNS),
        'technical_news':list(features.TECHNICAL_COLUMNS)+list(names.values())+['news_available'],
        'combined':list(features.TECHNICAL_COLUMNS+features.PEER_COLUMNS)+list(names.values())+['news_available']}
    evaluation._validate_frame(panel,groups)
    panel.to_parquet(root/'panel.parquet',index=False)
    coverage=[]
    for horizon in spec['horizons_minutes']:
        sub=panel[panel.horizon_minutes==horizon]
        coverage.append({'horizon_minutes':horizon,'candidate_feature_rows':len(feature_rows),
            'label_rows':len(sub),'withheld_missing_or_unmatured_path':len(feature_rows)-len(sub),
            'pairs':int(sub.instrument.nunique()),'news_available_rows':int(sub.news_available.sum()),
            'bid_ask_endpoint_rows':int((sub.long_net_bps.notna()&sub.short_net_bps.notna()).sum())})
    receipt.update(prepared_completed_epoch=time.time(),feature_groups=groups,coverage=coverage,
        feature_rows=len(feature_rows),news_status_counts=feature_rows.news_status.value_counts().to_dict(),
        source_manifest=data['manifest'],
        files={name:{'sha256':evaluation.sha_file(root/name),'bytes':(root/name).stat().st_size}
               for name in ('feature_rows.parquet','panel.parquet')})
    for name,expected in bindings.items():
        if evaluation.sha_file(Path(__file__).parent/name)!=expected:
            raise ValueError('implementation_changed_during_preparation')
    (root/'PREPARED.json').write_text(evaluation.canonical(receipt)+'\n',encoding='utf-8')
    print(json.dumps({'prepared':str(root),'feature_rows':len(feature_rows),'coverage':coverage}),flush=True)
    return receipt


def run(prepared,output_root):
    root=Path(prepared); receipt=json.loads((root/'PREPARED.json').read_text())
    for name,details in receipt['files'].items():
        if evaluation.sha_file(root/name)!=details['sha256']:
            raise ValueError('prepared_data_changed')
    for name,expected in receipt['source_bindings'].items():
        if evaluation.sha_file(Path(__file__).parent/name)!=expected:
            raise ValueError('prepared_implementation_changed:'+name)
    panel=pd.read_parquet(root/'panel.parquet')
    result=evaluation.evaluate(panel,feature_groups=receipt['feature_groups'],spec=receipt['spec'],
        output_root=output_root,input_manifest_sha256=evaluation.sha_file(root/'PREPARED.json'),
        declared_universe=receipt['source_manifest']['pairs'])
    print(json.dumps({'result':str(Path(output_root)/'RESULTS.json'),
        'forecast_rows':result['forecast_rows'],'completed_folds':sum(r['status']=='completed' for r in result['folds'])}),flush=True)


def main():
    parser=argparse.ArgumentParser()
    sub=parser.add_subparsers(dest='command',required=True)
    create=sub.add_parser('prepare'); create.add_argument('--snapshot',required=True)
    create.add_argument('--output',required=True); create.add_argument('--spec',required=True)
    create.add_argument('--pair-registry',required=True)
    score=sub.add_parser('evaluate'); score.add_argument('--prepared',required=True);score.add_argument('--output',required=True)
    args=parser.parse_args()
    if args.command=='prepare':
        prepare(args.snapshot,args.output,args.spec,args.pair_registry)
    else:
        run(args.prepared,args.output)


if __name__=='__main__':
    main()
