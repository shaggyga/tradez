import json
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from tools import build_rolling_comparison_quotes_v1 as quotes
from tools import build_rolling_technical_endpoints_v1 as endpoints
from tools.test_build_rolling_technical_endpoints_v1 import fixture,finish_manifest,START,PAIR
from oanda_rolling_technical_dataset_v1 import file_sha


def quote_job(tmp_path):
    old,data,_=fixture(tmp_path)
    result=endpoints._process_pair(old);overlay=finish_manifest(old,result)
    output=tmp_path/'quotes';(output/'pairs').mkdir(parents=True);(output/'receipts').mkdir()
    return {'base':old['base'],'endpoints':old['output'],'output':str(output),'pair':PAIR,
        'base_sha256':old['base_manifest_sha256'],'endpoint_sha256':file_sha(Path(old['output'])/'ENDPOINT_DATASET.json'),
        'recipe':old['recipe'],'source_receipt':old['receipt_path'],'source_receipt_sha256':old['receipt_sha256'],
        'boundaries':old['boundaries'],'partitions':old['partitions'],
        'endpoint_partitions':{str(p['block']):p for p in overlay['partitions']},'bindings':{},'workers':1,
        'pair_byte_cap':16*1024**2,'minimum_free_bytes':0},data


def test_delayed_entry_uses_next_minute_and_original_target_and_entry_denominator():
    data={'time':START+np.array([0,1,5,6])*60,'close':np.array([1.,2.,3.,4.]),
          'bid_close':np.array([.9,1.9,2.9,3.9]),'ask_close':np.array([1.1,2.1,3.1,4.1])}
    result=quotes.delayed_endpoint_outcomes(data,horizons=(5,))
    assert result['delayed_label__5m__long_net_bps'][0]==pytest.approx((2.9-2.1)/2*10000)
    assert result['delayed_label__5m__short_net_bps'][0]==pytest.approx((1.9-3.1)/2*10000)
    assert result['delayed_label__5m__valid'][0]
    assert not result['delayed_label__5m__valid'][1]  # next exact minute absent
    assert np.isnan(result['delayed_label__5m__long_net_bps'][1])


def test_delayed_quote_support_is_not_a_continuous_path_or_origin_filter():
    data={'time':START+np.array([0,1,5])*60,'close':np.ones(3),'bid_close':np.full(3,.9999),'ask_close':np.full(3,1.0001)}
    result=quotes.delayed_endpoint_outcomes(data,horizons=(5,))
    assert len(result['delayed_label__5m__valid'])==3
    assert result['delayed_label__5m__valid'][0]
    assert result['delayed_label__5m__long_net_bps'][0]==pytest.approx(-2.)
    empty={key:value[:0] for key,value in data.items()}
    assert len(quotes.delayed_endpoint_outcomes(empty)['delayed_label__5m__valid'])==0


def test_quote_panel_retains_all_origins_and_binds_existing_endpoint_parity(tmp_path):
    job,data=quote_job(tmp_path)
    original={p:p.read_bytes() for root in ('base','endpoints') for p in Path(job[root]).rglob('*') if p.is_file()}
    result=quotes._pair(job)
    assert result['rows']==98
    assert result['accepted_endpoint_parity_cells']>0
    table=pq.ParquetFile(Path(job['output'])/result['path']).read()
    assert table.num_rows==98
    assert table.column_names==[r['name'] for r in quotes.column_registry()]
    assert table['quote__entry_spread_bps'][0].as_py()==pytest.approx(.00004/1.2*10000)
    assert result['arima_status']=='unavailable'  # fixture contains fewer than 100 training triples
    assert table['forecast__arima110_conditional_ols__5m_bps'].null_count==98
    receipt=json.loads((Path(job['output'])/result['receipt_path']).read_bytes())
    assert receipt['arima_params']['reason']=='insufficient_training_regression_pairs'
    assert all(p.read_bytes()==raw for p,raw in original.items())


def test_spawned_quote_worker_has_bounded_independent_contract(tmp_path):
    job,_=quote_job(tmp_path)
    with ProcessPoolExecutor(max_workers=1,mp_context=multiprocessing.get_context('spawn')) as pool:
        result=pool.submit(quotes._pair,job).result(timeout=30)
    assert result['rows']==98


def test_quote_panel_refuses_changed_raw_inputs(tmp_path):
    job,_=quote_job(tmp_path);path=Path(job['recipe']['canonical_path'])
    raw=path.read_text();path.write_text(raw.replace(',7\n',',9\n',1))
    with pytest.raises(ValueError,match='base_input_identity_mismatch'):
        quotes._pair(job)
    assert not list((Path(job['output'])/'pairs').iterdir())


def test_quote_panel_refuses_changed_accepted_endpoint_bytes(tmp_path):
    job,_=quote_job(tmp_path)
    path=Path(job['endpoints'])/job['endpoint_partitions']['0']['path']
    with path.open('ab') as handle:handle.write(b'revision')
    with pytest.raises(ValueError,match='hash_mismatch'):
        quotes._pair(job)


def test_signed_zero_survives_quote_write_readback(tmp_path):
    job={'output':str(tmp_path),'pair_byte_cap':16*1024**2,'minimum_free_bytes':0,'workers':1}
    values=np.array([0.,-0.,np.nan])
    result=quotes._write(tmp_path/'test.parquet',{'bar_start_epoch':np.array([START,START+60,START+120]),'x':values},job)
    saved=pq.ParquetFile(tmp_path/result['path']).read()['x'].to_numpy()
    assert np.array_equal(values[:2].view(np.uint64),saved[:2].view(np.uint64))


def test_delayed_fields_are_explicit_future_labels_not_inputs():
    registry=quotes.column_registry()
    assert len(registry)==25
    delayed=[r for r in registry if r['name'].startswith('delayed_label__')]
    assert len(delayed)==12 and all(r['future_information'] and not r['model_input'] for r in delayed)
    assert all(not r['future_information'] for r in registry if r['name'].startswith('quote__'))


def test_arima_fit_start_excludes_pre_start_context_and_preserves_recreation():
    times=START+np.arange(170)*60
    logs=np.r_[0.,np.cumsum(.01*.85**np.arange(169))]
    data={'time':times,'close':np.exp(logs)}
    bounds={'start':START+10*60,'train_end':START+155*60}
    original=quotes.fit_pair_arima(data,bounds)
    changed={'time':times,'close':data['close'].copy()}
    changed['close'][:10]=np.nan
    assert quotes.fit_pair_arima(changed,bounds)==original
    assert original['status']=='fitted'
    assert original['first_response_start_epoch']==bounds['start']+120
    assert original['last_response_end_epoch']<bounds['train_end']
    assert original['pre_start_context_used_for_fitting'] is False
