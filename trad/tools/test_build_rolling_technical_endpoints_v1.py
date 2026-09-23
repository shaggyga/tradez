"""Small isolated fixtures for overlay identity, parity, readback and joining."""
import copy
import csv
from datetime import datetime,timezone
import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import oanda_rolling_technical_dataset_v1 as base_reader
import oanda_rolling_technical_endpoint_labels_v1 as endpoint
import oanda_rolling_technical_labels_v1 as strict
import oanda_rolling_technical_ranges_v1 as ranges
from tools import build_rolling_technical_endpoints_v1 as builder

START=1_800_000_000
PAIR='EUR_USD'


def _table(columns):
    return pa.table({n:pa.array(a,mask=~np.isfinite(a)) if np.asarray(a).dtype.kind=='f' else pa.array(a) for n,a in columns.items()})


def fixture(tmp_path):
    base=tmp_path/'base';(base/'shards').mkdir(parents=True);(base/'receipts').mkdir()
    output=tmp_path/'overlay';(output/'shards').mkdir(parents=True);(output/'receipts').mkdir()
    rows=[]
    for i in range(160):
        if i in (10,80): continue
        clock=datetime.fromtimestamp(START+i*60,timezone.utc).isoformat()
        price=1.2+i*.00001
        rows.append({'time':clock,'datetime':clock,'instrument':PAIR,'granularity':'M1',
            'open':price,'high':price+.001,'low':price-.001,'close':price,
            'bid_close':price-.00002,'ask_close':price+.00002,'volume':7})
    raw=tmp_path/(PAIR+'_M1.csv')
    with raw.open('w',newline='',encoding='utf-8') as handle:
        writer=csv.DictWriter(handle,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    recipe={'pair':PAIR,'canonical_path':str(raw),'reacquired_path':'not_needed.parquet','cutoff_epoch':START-60}
    data,receipt=ranges.read_range(PAIR,recipe,START,START+160*60,START+20000)
    bounds={'start':START,'train_end':START+50*60,'validation_end':START+75*60,'end':START+100*60}
    origin=data['time']<bounds['end'];times=data['time'][origin]
    outcomes=strict.compute_outcomes(data,coverage_end_epoch=START+160*60)
    columns={'instrument':np.full(len(times),PAIR),'bar_start_epoch':times,'bar_end_epoch':times+60,
        'origin_split':base_reader.origin_splits(times,**{'start':bounds['start'],'train_end':bounds['train_end'],
            'validation_end':bounds['validation_end'],'end':bounds['end']}),'m1__fixture':np.zeros(len(times))}
    columns.update({n:a[origin] for n,a in outcomes.items()})
    for h in endpoint.DEFAULT_HORIZONS:
        columns[f'label__{h}m__split_eligible']=base_reader.split_maturity(times,h,bounds['start'],bounds['train_end'],bounds['validation_end'],bounds['end'])
    core=base/'shards/core.parquet';peers=base/'shards/peers.parquet'
    pq.write_table(_table(columns),core);pq.write_table(pa.table({'bar_start_epoch':times}),peers)
    record={'pair':PAIR,'block':0,'key_sha256':base_reader.key_hash(times),
        'core':{'path':'shards/core.parquet','sha256':base_reader.file_sha(core),'rows':len(times)},
        'peers':{'path':'shards/peers.parquet','sha256':base_reader.file_sha(peers),'rows':len(times)},
        'split_counts':dict(__import__('collections').Counter(columns['origin_split']))}
    builder.save(base/'receipts/EUR_USD.json',receipt)
    manifest={'schema':base_reader.SCHEMA,'status':'complete','partitions':[record],
        'pairs':{PAIR:{'source_receipt':'receipts/EUR_USD.json'}},'origin_rows':len(times),
        'feature_names':['m1__fixture'],'label_names':[n for n in columns if n.startswith('label__')],
        'boundaries':bounds}
    builder.save(base/'DATASET.json',manifest)
    job={'base':str(base),'output':str(output),'base_manifest_sha256':base_reader.file_sha(base/'DATASET.json'),
        'pair':PAIR,'recipe':recipe,'receipt_path':'receipts/EUR_USD.json',
        'receipt_sha256':base_reader.file_sha(base/'receipts/EUR_USD.json'),'bindings':{},'boundaries':bounds,
        'partitions':[record],'minimum_free_bytes':0,'pair_byte_cap':32*1024**2,'workers':1}
    return job,data,receipt


def finish_manifest(job,result):
    output=Path(job['output'])
    overlay={'schema':endpoint.SCHEMA,'status':'complete','base_manifest_sha256':job['base_manifest_sha256'],
        'label_names':[r['name'] for r in endpoint.endpoint_registry(include_split_eligibility=True)],
        'partitions':result['partitions']}
    builder.save(output/'ENDPOINT_DATASET.json',overlay)
    return overlay


def test_worker_preserves_origins_verifies_strict_parity_and_joins(tmp_path):
    job,data,receipt=fixture(tmp_path)
    before={p:p.read_bytes() for p in Path(job['base']).rglob('*') if p.is_file()}
    result=builder._process_pair(job)
    assert result['origin_rows']==98
    assert result['strict_valid_numeric_cells_checked']>0
    assert result['outcome_counts']['60']['additional_endpoint_midpoint_valid']>0
    finish_manifest(job,result)
    tables=list(endpoint.iter_endpoint_partitions(job['base'],job['output']))
    assert len(tables)==1 and tables[0][1].num_rows==98
    assert 'endpoint_label__60m__return_bps' in tables[0][1].column_names
    assert 'm1__fixture' in tables[0][1].column_names
    assert all(p.read_bytes()==b for p,b in before.items())
    train=list(endpoint.iter_endpoint_partitions(job['base'],job['output'],split='train'))[0][1]
    assert train.num_rows==49


def test_independent_spawn_worker_has_same_supported_contract(tmp_path):
    job,_,_=fixture(tmp_path)
    with ProcessPoolExecutor(max_workers=1,mp_context=multiprocessing.get_context('spawn')) as pool:
        result=pool.submit(builder._process_pair,job).result(timeout=30)
    assert result['origin_rows']==98
    assert result['outcome_counts']['5']['endpoint_midpoint_valid']>=result['outcome_counts']['5']['strict_midpoint_valid']


@pytest.mark.parametrize('key',['normalized_arrays_sha256','observed_epoch','retained_rows'])
def test_input_identity_mismatch_refuses_mixing_revisions(tmp_path,key):
    _,_,receipt=fixture(tmp_path)
    changed=copy.deepcopy(receipt);changed[key]='changed'
    with pytest.raises(ValueError,match='base_input_identity_mismatch'):
        builder.verify_input_identity(receipt,changed)


def test_selected_original_row_identity_is_also_mandatory(tmp_path):
    _,_,receipt=fixture(tmp_path)
    changed=copy.deepcopy(receipt)
    changed['sources'][1]['selected_original_rows_sha256']='changed'
    with pytest.raises(ValueError,match='selected_original_rows_changed'):
        builder.verify_input_identity(receipt,changed)
    changed=copy.deepcopy(receipt)
    changed['sources'][1]['source_prefix_sha256']='new_append_prefix'
    assert builder.verify_input_identity(receipt,changed)['normalized_arrays_match']


def test_raw_revision_between_base_and_overlay_is_rejected(tmp_path):
    job,_,_=fixture(tmp_path)
    raw=Path(job['recipe']['canonical_path'])
    text=raw.read_text();raw.write_text(text.replace(',7\n',',8\n',1))
    with pytest.raises(ValueError,match='base_input_identity_mismatch'):
        builder._process_pair(job)
    assert not list((Path(job['output'])/'shards').iterdir())


def test_overlay_refuses_strict_numeric_disagreement(tmp_path):
    job,_,_=fixture(tmp_path)
    path=Path(job['base'])/'shards/core.parquet'
    table=pq.ParquetFile(path).read()
    name='label__5m__return_bps';values=table[name].to_numpy().copy()
    values[np.flatnonzero(np.isfinite(values))[0]]+=1
    table=table.set_column(table.schema.get_field_index(name),name,pa.array(values,mask=~np.isfinite(values)))
    pq.write_table(table,path)
    job['partitions'][0]['core']['sha256']=base_reader.file_sha(path)
    with pytest.raises(ValueError,match='numerical_parity_failed'):
        builder._process_pair(job)


def test_split_flags_are_preserved_not_relaxed(tmp_path):
    job,_,_=fixture(tmp_path)
    path=Path(job['base'])/'shards/core.parquet'
    table=pq.ParquetFile(path).read();name='label__60m__split_eligible'
    values=table[name].to_numpy().copy();values[0]=~values[0]
    table=table.set_column(table.schema.get_field_index(name),name,pa.array(values))
    pq.write_table(table,path);job['partitions'][0]['core']['sha256']=base_reader.file_sha(path)
    with pytest.raises(ValueError,match='split_boundary_flag_mismatch'):
        builder._process_pair(job)


def test_pair_budget_stops_before_writing_a_shard(tmp_path):
    job,_,_=fixture(tmp_path);job['pair_byte_cap']=1000
    with pytest.raises(ValueError,match='storage_allocation'):
        builder._process_pair(job)
    assert not list((Path(job['output'])/'shards').iterdir())


def test_numeric_signed_zero_survives_parquet_readback(tmp_path):
    job={'output':str(tmp_path),'pair_byte_cap':32*1024**2,'minimum_free_bytes':0,'workers':1}
    values=np.array([0.0,-0.0,np.nan])
    result=builder._write_sidecar(tmp_path/'zeros.parquet',
        {'bar_start_epoch':np.array([START,START+60,START+120]),'endpoint_label__5m__return_bps':values},job,0)
    actual=pq.ParquetFile(tmp_path/result['path']).read()['endpoint_label__5m__return_bps'].to_numpy()
    assert np.array_equal(values[:2].view(np.uint64),actual[:2].view(np.uint64))
    assert np.isnan(actual[2])


def test_overlay_labels_cannot_be_requested_as_registered_inputs(tmp_path):
    job,_,_=fixture(tmp_path);result=builder._process_pair(job);finish_manifest(job,result)
    with pytest.raises(ValueError,match='registered_model_inputs'):
        list(endpoint.iter_endpoint_partitions(job['base'],job['output'],feature_names=['endpoint_label__5m__return_bps']))
    with pytest.raises(ValueError,match='registered_endpoint_labels'):
        list(endpoint.iter_endpoint_partitions(job['base'],job['output'],endpoint_names=['m1__fixture']))


def test_overlay_join_detects_changed_base_or_sidecar_bytes(tmp_path):
    job,_,_=fixture(tmp_path);result=builder._process_pair(job);finish_manifest(job,result)
    path=Path(job['output'])/result['partitions'][0]['path']
    with path.open('ab') as handle:handle.write(b'changed')
    with pytest.raises(ValueError,match='hash_mismatch'):
        list(endpoint.iter_endpoint_partitions(job['base'],job['output']))
