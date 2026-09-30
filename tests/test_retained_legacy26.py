import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_retained_legacy26_v1 as m


def fixture():
    db=sqlite3.connect(':memory:');db.row_factory=sqlite3.Row
    db.executescript('CREATE TABLE bars(pair,t,body,input_hash,receipt_id,first_observed); CREATE TABLE receipts(id,body,pair,observed); CREATE TABLE revisions(pair);')
    times=[600]+list(range(1200,1200+90*60,60))
    rows=[]
    for i,t in enumerate(times):
        raw=m.encoded({'time':t,'close':1.1+i*.00001});receipt=m.encoded({'instrument':'EUR_USD','observed_epoch':t+62})
        rh=m.digest(receipt);db.execute('INSERT INTO receipts VALUES(?,?,?,?)',(rh,receipt,'EUR_USD',t+62))
        db.execute('INSERT INTO bars VALUES(?,?,?,?,?,?)',('EUR_USD',t,raw,m.digest(raw),rh,t+62));rows.append({'epoch':t,'mid':1.1+i*.00001})
    row={'bar_start_epoch':times[-1],'captured_epoch':times[-1]+65,'input_hash':m.digest(raw),'reference_mid':rows[-1]['mid'],'entry_long':.5,'entry_short':.4}
    return db,row,pd.DataFrame(rows)


def test_exact_original_transform_and_full_session():
    db,r,full=fixture();v=m.read_pair(db,'EUR_USD',r)
    expected=m.original._technical(full)[0].iloc[-1].tolist()+[.5,.4]
    np.testing.assert_allclose(list(v['values'].values()),expected,rtol=1e-12,atol=1e-12)
    assert v['values']['tech_session_age_hours']==89/60
    assert v['provenance']['session_start']==1200


def test_truncated_contiguous_tail_is_not_a_new_session():
    db,r,_=fixture()
    with pytest.raises(ValueError,match='session_boundary'):m.read_pair(db,'EUR_USD',r,max_rows=70)


@pytest.mark.parametrize('mutation,reason',[
    ("UPDATE bars SET first_observed=999999 WHERE t=1200",'receipt_clock'),
    ("UPDATE bars SET input_hash='bad' WHERE t=1200",'hash_mismatch'),
    ("UPDATE receipts SET pair='GBP_USD'",'receipt_clock_or_pair'),
    ("INSERT INTO revisions VALUES('EUR_USD')",'unresolved_revision'),
    ("DELETE FROM receipts",'hash_mismatch')])
def test_refuses_broken_original_evidence(mutation,reason):
    db,r,_=fixture();db.execute(mutation)
    with pytest.raises(ValueError,match=reason):m.read_pair(db,'EUR_USD',r)


def test_gaps_preserve_original_missing_flags_and_recent_window():
    db,r,full=fixture();t=r['bar_start_epoch']-120;db.execute('DELETE FROM bars WHERE t=?',(t,))
    full=full[full.epoch!=t];v=m.read_pair(db,'EUR_USD',r)
    expected=m.original._technical(full)[0].iloc[-1].tolist()+[.5,.4]
    np.testing.assert_allclose(list(v['values'].values()),expected,rtol=1e-10,atol=1e-12)
    assert v['values']['tech_unavailable_5m']==1
    assert v['values']['tech_rate_bps_per_min_5m']==0


def test_future_bar_cannot_change_features():
    db,r,_=fixture();before=m.read_pair(db,'EUR_USD',r)
    db.execute('INSERT INTO bars VALUES(?,?,?,?,?,?)',('EUR_USD',r['bar_start_epoch']+60,b'bad','bad','bad',999999))
    assert before==m.read_pair(db,'EUR_USD',r)


def test_reference_identity_is_bound():
    db,r,_=fixture();r['input_hash']='wrong'
    with pytest.raises(ValueError,match='reference_changed'):m.read_pair(db,'EUR_USD',r)
