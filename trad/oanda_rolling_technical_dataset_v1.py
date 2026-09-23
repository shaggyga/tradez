"""Read bounded historical partitions with an explicit feature/label boundary."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA='rolling_technical_training_windows_v1_20260915'
SPLITS=('train','validation','later_development_test')


def origin_splits(times, start, train_end, validation_end, end):
    if not start<train_end<validation_end<end:
        raise ValueError('ordered_development_boundaries_required')
    t=np.asarray(times)
    if np.any(t<start) or np.any(t>=end):
        raise ValueError('origin_outside_declared_window')
    result=np.full(len(t),SPLITS[2],dtype='<U22')
    result[t<validation_end]=SPLITS[1]
    result[t<train_end]=SPLITS[0]
    return result


def split_maturity(times,horizon,start,train_end,validation_end,end):
    """Strict target END before its origin's partition boundary, not <=."""
    split=origin_splits(times,start,train_end,validation_end,end)
    boundary=np.where(split==SPLITS[0],train_end,np.where(split==SPLITS[1],validation_end,end))
    return np.asarray(times)+(horizon+1)*60<boundary


def key_hash(times):
    return hashlib.sha256(np.ascontiguousarray(times,dtype='<i8').tobytes()).hexdigest()


def file_sha(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f,'sha256').hexdigest()


def iter_partitions(root, *, split=None, feature_names=None):
    """Yield bounded Arrow tables. Preserve rows with missing future labels.

    Callers explicitly select mature training labels later; this loader does
    not make future availability an origin/forecast eligibility rule.
    """
    root=Path(root).resolve()
    manifest=json.loads((root/'DATASET.json').read_text('utf-8'))
    if manifest['schema']!=SCHEMA or manifest['status']!='complete':
        raise ValueError('complete_training_dataset_required')
    names=manifest['feature_names'] if feature_names is None else list(feature_names)
    if len(names)!=len(set(names)) or not set(names)<=set(manifest['feature_names']):
        raise ValueError('only_explicit_registered_model_inputs_allowed')
    if split is not None and split not in SPLITS:
        raise ValueError('unknown_development_split')
    core_names=[n for n in names if n.startswith('m1__')]
    peer_names=[n for n in names if n.startswith('peer__')]
    for record in manifest['partitions']:
        if split is not None and split not in record['split_counts']:
            continue
        paths=[]
        for kind in ('core','peers'):
            p=(root/record[kind]['path']).resolve()
            if not p.is_relative_to(root) or file_sha(p)!=record[kind]['sha256']:
                raise ValueError('partition_path_or_hash_mismatch')
            paths.append(p)
        metadata=['instrument','bar_start_epoch','bar_end_epoch','origin_split']
        core=pq.ParquetFile(paths[0]).read(columns=metadata+core_names+manifest['label_names'])
        peers=pq.ParquetFile(paths[1]).read(columns=['bar_start_epoch']+peer_names)
        t=core['bar_start_epoch'].to_numpy()
        if core.num_rows!=peers.num_rows or key_hash(t)!=record['key_sha256'] or not np.array_equal(t,peers['bar_start_epoch'].to_numpy()):
            raise ValueError('exact_pair_minute_sidecar_join_required')
        for name in peer_names:
            core=core.append_column(name,peers[name])
        if split is not None:
            import pyarrow.compute as pc
            core=core.filter(pc.equal(core['origin_split'],split))
        yield record['pair'],core
