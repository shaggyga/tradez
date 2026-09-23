import numpy as np
import pytest
from oanda_rolling_technical_panel_batch_v1 import compute_peer_arrays
from oanda_rolling_technical_panel_v1 import compute_panel,HORIZONS


def test_exact_live_arithmetic_missing_clock_self_exclusion_and_sparse_topology():
    rng=np.random.default_rng(1401)
    start=1783987200
    pairs=('EUR_USD','GBP_USD','AUD_USD','USD_JPY','EUR_JPY','GBP_JPY','EUR_DKK')
    rows={}
    for pair in pairs:
        mask=rng.random(25)>.15
        t=start+np.flatnonzero(mask)*60
        v={f'm1__return_{h}_bps':rng.normal(0,10,len(t)) for h in HORIZONS}
        for arr in v.values():
            arr[rng.random(len(t))<.15]=np.nan
        rows[pair]={'time':t,'values':v}
    batch={p:(t,v,c) for p,t,v,c in compute_peer_arrays(rows,start_epoch=start,end_epoch=start+25*60)}
    for at in range(start,start+25*60,60):
        snapshots={}
        for pair,row in rows.items():
            matches=np.flatnonzero(row['time']==at)
            snapshots[pair]=None if not len(matches) else {'bar_start_epoch':at,'values':{n:float(a[matches[0]]) for n,a in row['values'].items()}}
        expected=compute_panel(snapshots,at)
        for pair,(t,values,support) in batch.items():
            positions=np.flatnonzero(t==at)
            if not len(positions):
                continue
            i=positions[0]
            for name,arr in values.items():
                want=expected['by_pair'][pair]['values'][name]
                assert (np.isnan(arr[i]) if want is None else arr[i].tobytes()==np.float64(want).tobytes())
            for h in HORIZONS:
                for side in ('base','quote'):
                    assert support[f'diagnostic__peer_{side}_{h}_count'][i]==expected['by_pair'][pair]['support'][str(h)][side]['count']


def test_empty_leg_and_origins_are_not_fabricated():
    at=1783987200
    rows={'USD_THB':{'time':np.array([at]),'values':{f'm1__return_{h}_bps':np.array([1.]) for h in HORIZONS}}}
    pair,t,values,support=next(compute_peer_arrays(rows,start_epoch=at,end_epoch=at+600))
    assert len(t)==1 and pair=='USD_THB'
    assert all(np.isnan(a).all() for a in values.values())
    assert all((a==0).all() for a in support.values())


def test_calendar_bound_and_duplicate_times_rejected():
    with pytest.raises(ValueError,match='calendar'):
        list(compute_peer_arrays({},start_epoch=0,end_epoch=100*86400))
    rows={'EUR_USD':{'time':np.array([60,60]),'values':{}}}
    with pytest.raises(ValueError,match='ordered'):
        list(compute_peer_arrays(rows,start_epoch=0,end_epoch=600))


def _rows_from_returns(times, by_pair):
    return {pair:{'time':np.asarray(times,dtype=np.int64),
        'values':{f'm1__return_{h}_bps':np.asarray(values,dtype=np.float64).copy()
                  for h in HORIZONS}} for pair,values in by_pair.items()}


def _assert_scalar_parity(rows, start, end):
    batch={p:(t,v,c) for p,t,v,c in compute_peer_arrays(rows,start_epoch=start,end_epoch=end)}
    for at in sorted({int(t) for row in rows.values() for t in row['time']}):
        snapshots={}
        for pair,row in rows.items():
            found=np.flatnonzero(row['time']==at)
            snapshots[pair]=None if not len(found) else {'bar_start_epoch':at,
                'values':{name:float(a[found[0]]) for name,a in row['values'].items()}}
        reference=compute_panel(snapshots,at)
        for pair,(times,values,support) in batch.items():
            found=np.flatnonzero(times==at)
            if not len(found):
                continue
            i=found[0]
            for name,a in values.items():
                want=reference['by_pair'][pair]['values'][name]
                assert np.isnan(a[i]) if want is None else a[i].tobytes()==np.float64(want).tobytes()
            for h in HORIZONS:
                for side in ('base','quote'):
                    assert support[f'diagnostic__peer_{side}_{h}_count'][i]==reference['by_pair'][pair]['support'][str(h)][side]['count']
    return batch


def test_cancellation_extremes_and_overflow_match_scalar_bit_for_bit():
    at=1783987200
    rows=_rows_from_returns([at,at+60],{
        'EUR_USD':[np.nan,np.nan],
        'EUR_AUD':[1e308,1.6e308],
        'EUR_CAD':[1.,1.6e308],
        'EUR_GBP':[-1e308,1.6e308],
        'USD_JPY':[-1e308,-1.6e308],
        'USD_CHF':[-1e308,-1.6e308],
        'USD_NOK':[-1e308,-1.6e308],
    })
    batch=_assert_scalar_parity(rows,at,at+120)
    values=batch['EUR_USD'][1]
    assert values['peer__base_mean_1_bps'][0]==1/3
    assert np.isfinite(values['peer__base_mean_1_bps'][1])
    assert np.isfinite(values['peer__quote_mean_1_bps'][1])
    assert np.isnan(values['peer__diff_1_bps'][1])


def test_missing_own_return_keeps_peer_context_but_missing_origin_is_not_created():
    at=1783987200
    rows=_rows_from_returns([at,at+60],{
        'EUR_USD':[np.nan,np.nan], 'EUR_AUD':[2.,4.], 'EUR_CAD':[4.,6.],
        'USD_JPY':[8.,10.], 'GBP_USD':[-10.,-12.],
    })
    rows['EUR_USD']['time']=rows['EUR_USD']['time'][:1]
    rows['EUR_USD']['values']={n:a[:1] for n,a in rows['EUR_USD']['values'].items()}
    rows['USD_THB']={'time':np.array([],dtype=np.int64),
        'values':{f'm1__return_{h}_bps':np.array([],dtype=np.float64) for h in HORIZONS}}
    batch=_assert_scalar_parity(rows,at,at+120)
    assert batch['EUR_USD'][0].tolist()==[at]
    assert batch['EUR_USD'][1]['peer__diff_15_bps'].tolist()==[-6.]
    assert len(batch['USD_THB'][0])==0
    assert all(len(a)==0 for a in batch['USD_THB'][1].values())


def test_future_returns_and_input_iteration_order_cannot_change_earlier_panel():
    at=1783987200
    original=_rows_from_returns([at,at+60],{
        'EUR_USD':[1.,2.], 'EUR_AUD':[2.,3.], 'EUR_CAD':[3.,4.],
        'USD_JPY':[4.,5.], 'GBP_USD':[5.,6.],
    })
    before=_assert_scalar_parity(original,at,at+120)
    changed={p:{'time':r['time'].copy(),'values':{n:a.copy() for n,a in r['values'].items()}}
             for p,r in reversed(list(original.items()))}
    for row in changed.values():
        for a in row['values'].values():
            a[1]=-1e100
    after=_assert_scalar_parity(changed,at,at+120)
    for pair,(_,values,support) in before.items():
        for name,a in values.items():
            np.testing.assert_array_equal(a[:1],after[pair][1][name][:1],strict=True)
        for name,a in support.items():
            np.testing.assert_array_equal(a[:1],after[pair][2][name][:1],strict=True)


def test_nonfinite_returns_are_omitted_only_from_their_explicit_horizon():
    at=1783987200
    rows=_rows_from_returns([at],{
        'EUR_USD':[np.nan], 'EUR_AUD':[2.], 'EUR_CAD':[4.],
        'USD_JPY':[8.], 'GBP_USD':[-10.],
    })
    rows['EUR_AUD']['values']['m1__return_1_bps'][0]=np.inf
    rows['EUR_CAD']['values']['m1__return_5_bps'][0]=-np.inf
    batch=_assert_scalar_parity(rows,at,at+60)
    values=batch['EUR_USD'][1]
    assert np.isnan(values['peer__base_mean_1_bps'][0])
    assert np.isnan(values['peer__base_mean_5_bps'][0])
    assert values['peer__base_mean_15_bps'][0]==3.
    assert values['peer__quote_mean_1_bps'][0]==9.


def test_unsigned_descending_times_cannot_wrap_past_order_check():
    rows={'EUR_USD':{'time':np.array([120,60],dtype=np.uint64),
        'values':{f'm1__return_{h}_bps':np.array([1.,2.]) for h in HORIZONS}}}
    with pytest.raises(ValueError,match='ordered'):
        list(compute_peer_arrays(rows,start_epoch=0,end_epoch=600))
