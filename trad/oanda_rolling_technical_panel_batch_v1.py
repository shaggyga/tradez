"""Bounded historical peer arrays with the same arithmetic as the live panel.

Only equal original minute clocks contribute. Zero in the internal scratch
array represents an omitted summand, never an imputed return or output.
"""
from __future__ import annotations

import math
import numpy as np
import oanda_rolling_technical_panel_v1 as reference


def _sum_rows(summands):
    def safe(row):
        try:
            return math.fsum(row)
        except OverflowError:
            return np.nan
    return np.fromiter((safe(row) for row in summands.T),dtype=np.float64,count=summands.shape[1])


def compute_peer_arrays(rows, *, start_epoch, end_epoch):
    """Yield ``pair,times,values,support`` for exact input origins only.

    rows maps each pair to {time: sorted integer starts, values: four return
    arrays}. Calendar bounds must be explicit, aligned and <=92 days. The
    68-pair grid is scratch space, not fabricated output candle observations.
    All pairs participate before any result is yielded.
    """
    if end_epoch<=start_epoch or start_epoch%60 or end_epoch%60 or end_epoch-start_epoch>92*86400:
        raise ValueError('bounded_aligned_peer_calendar_required')
    pairs=sorted(rows)
    if len(pairs)>68 or not pairs:
        raise ValueError('bounded_nonempty_peer_universe_required')
    for pair in pairs:
        if not reference.PAIR.fullmatch(pair) or pair[:3]==pair[4:]:
            raise ValueError('invalid_pair')
    length=(end_epoch-start_epoch)//60
    present=np.zeros((len(pairs),length),dtype=bool)
    returns={h:np.full((len(pairs),length),np.nan) for h in reference.HORIZONS}
    offsets={}
    for index,pair in enumerate(pairs):
        t=np.asarray(rows[pair]['time'])
        if t.ndim!=1 or t.dtype.kind not in 'iu' or np.any(t%60) or np.any(t[1:]<=t[:-1]) or np.any(t<start_epoch) or np.any(t>=end_epoch):
            raise ValueError('unique_ordered_bounded_peer_times_required')
        off=((t-start_epoch)//60).astype(np.int64)
        offsets[pair]=off
        present[index,off]=True
        for h in reference.HORIZONS:
            values=np.asarray(rows[pair]['values'][f'm1__return_{h}_bps'],dtype=np.float64)
            if values.shape!=t.shape:
                raise ValueError('unaligned_peer_returns')
            returns[h][index,off]=np.where(np.isfinite(values),values,np.nan)
    for index,pair in enumerate(pairs):
        base,quote=pair.split('_')
        values={};support={}
        for h in reference.HORIZONS:
            legs={}
            for side,currency in (('base',base),('quote',quote)):
                peers=[];signs=[]
                for j,other in enumerate(pairs):
                    if other==pair:
                        continue
                    left,right=other.split('_')
                    sign=1 if currency==left else -1 if currency==right else 0
                    if sign:
                        peers.append(j);signs.append(sign)
                signed=returns[h][peers]*np.asarray(signs,dtype=np.float64)[:,None]
                finite=np.isfinite(signed)
                count=finite.sum(axis=0)
                scratch=np.zeros_like(signed)
                np.divide(signed,count[None,:],out=scratch,where=finite & (count[None,:]>0))
                mean=_sum_rows(scratch)
                mean[(count<reference.MIN_PEERS)|~present[index]]=np.nan
                legs[side]=mean
                values[f'peer__{side}_mean_{h}_bps']=mean[offsets[pair]]
                support[f'diagnostic__peer_{side}_{h}_count']=count[offsets[pair]].astype(np.int16)
            with np.errstate(over='ignore',invalid='ignore'):
                difference=legs['base']-legs['quote']
            difference[~np.isfinite(difference)]=np.nan
            values[f'peer__diff_{h}_bps']=difference[offsets[pair]]
        assert list(values)==[r['name'] for r in reference.panel_registry()]
        yield pair,np.asarray(rows[pair]['time']),values,support
