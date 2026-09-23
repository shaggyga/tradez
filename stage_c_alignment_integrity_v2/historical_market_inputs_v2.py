"""Recover exact, authenticated candle points for a declared offline scenario.

Binary archive prices are represented by their shortest round-trip decimal text;
this does not recover original venue precision or actual arrival timestamps.
"""
import hashlib
import json
from pathlib import Path
import sys
import time
import zipfile
from math import isfinite
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT))
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from all68_neutral_runner_v2 import read_long_members, ARCHIVE, INPUT_MANIFEST
from quote_input_qualification_v2 import qualify_point
from publication import sha256_file
from contracts import fingerprint

PIP_SHA='c9464343f012e0bdd83582480a0044bf1c6774eef2622f312bcdd4599e3869c5'
PIP_PATH=ROOT.parent/'stage_b_20260921/source/config/pair_local_operational_v2_20260913.json'
TIER='retained_candle_close_scenario_inputs.v1'


def record(pair,price_epoch,rows,member_sha):
    point=qualify_point(rows,price_epoch-60,columns_present=True)
    result={'instrument':pair,'price_epoch':price_epoch,'assumed_available_epoch':price_epoch,
            'source_bar_start_epoch':price_epoch-60,'source_member_sha256':member_sha,
            'input_tier':TIER,'qualification':point,'status':point['status']}
    if point['status']=='valid_candle_close_pair':
        close=point['source_midpoint_close']
        if close is None or not isfinite(close) or close<=0:
            result['status']='invalid_reference_close'
        else:
            result.update(reference_close=repr(close),bid=repr(point['bid_close']),ask=repr(point['ask_close']))
    result['record_sha256']=fingerprint(result)
    return result


def validate_record(row):
    if row.get('record_sha256')!=fingerprint({k:v for k,v in row.items() if k!='record_sha256'}):
        raise ValueError('market_point_identity_mismatch')
    if row['input_tier']!=TIER or row['price_epoch']!=row['source_bar_start_epoch']+60 or row['assumed_available_epoch']!=row['price_epoch']:
        raise ValueError('market_point_clock_or_tier_mismatch')
    if row['status']=='valid_candle_close_pair':
        from decimal import Decimal
        close,bid,ask=map(Decimal,(row['reference_close'],row['bid'],row['ask']))
        if not all(v.is_finite() and v>0 for v in (close,bid,ask)) or bid>ask:
            raise ValueError('market_point_price_invalid')


def prepare(price_epochs,*,archive=ARCHIVE,manifest=INPUT_MANIFEST,pip_path=PIP_PATH):
    if (not price_epochs or price_epochs!=sorted(set(price_epochs)) or len(price_epochs)>256 or
        any(type(t) is not int or t%60 for t in price_epochs)):
        raise ValueError('bounded_exact_price_epochs_required')
    members,digest=read_long_members(manifest)
    if sha256_file(archive)!=digest:raise ValueError('archive_identity_mismatch')
    if sha256_file(pip_path)!=PIP_SHA:raise ValueError('pip_metadata_identity_mismatch')
    meta=json.loads(pip_path.read_text(encoding='utf-8'))['pairs']
    if set(meta)!={m['instrument'] for m in members}:raise ValueError('pip_universe_mismatch')
    rows=[];metadata={};began=time.monotonic()
    with zipfile.ZipFile(archive) as z:
        if len(z.namelist())!=68 or set(z.namelist())!={m['path'] for m in members}:raise ValueError('archive_inventory_mismatch')
        for m in members:
            if time.monotonic()-began>180:raise TimeoutError('market_input_time_limit')
            raw=z.read(m['path'])
            if len(raw)!=m['bytes'] or hashlib.sha256(raw).hexdigest()!=m['sha256']:raise ValueError('member_identity_mismatch')
            table=pq.read_table(pa.BufferReader(raw),columns=['datetime','close','bid_close','ask_close'])
            stamps=pc.divide(pc.cast(pc.cast(table['datetime'],pa.timestamp('us',tz='UTC')),pa.int64()),1000000)
            selected=table.append_column('raw_epoch',stamps).filter(pc.is_in(stamps,value_set=pa.array([t-60 for t in price_epochs],type=pa.int64())))
            by_epoch={}
            for item in selected.to_pylist():by_epoch.setdefault(int(item['raw_epoch']),[]).append(item)
            pair=m['instrument'];pip=meta[pair]['pip_size']
            if not isfinite(float(pip)) or not 0<float(pip)<1:raise ValueError('pip_metadata_invalid')
            metadata[pair]={'base_currency':pair[:3],'quote_currency':pair[4:],
                'pip_size':str(pip),'unit_increment':1}
            rows.extend(record(pair,t,by_epoch.get(t-60,[]),m['sha256']) for t in price_epochs)
            del raw,table,stamps,selected
    return {'schema_version':TIER,'price_epochs':price_epochs,'universe':sorted(metadata),'metadata':metadata,'rows':rows,
        'provenance':{'archive_sha256':digest,'manifest_sha256':sha256_file(manifest),'pip_metadata_sha256':PIP_SHA,
            'members_verified':68,'implementation_sha256':sha256_file(Path(__file__)),
            'quote_qualifier_sha256':sha256_file(ROOT/'quote_input_qualification_v2.py')},
        'assumptions':['M1 interval-start stamps, close ready at start+60','retained co-stamped bid/ask closes used as scenario quotes, not observed executability',
            'shortest round-trip decimal from stored binary prices, not original venue precision',
            'retained 2026 pip definitions held constant for 2024 research; no historical lot metadata claim',
            'integer unit increment is a research sizing assumption'],
        'execution_ready':False,'broker_access':False}


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--epochs',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    if a.output.exists():raise FileExistsError('immutable_market_input_output')
    data=prepare(json.loads(a.epochs.read_text()))
    a.output.parent.mkdir(parents=True,exist_ok=True)
    with a.output.open('x',encoding='utf-8') as f:json.dump(data,f,sort_keys=True,allow_nan=False)
    print(json.dumps({'rows':len(data['rows']),'valid':sum(r['status']=='valid_candle_close_pair' for r in data['rows']),'instruments':len(data['universe']),'execution_ready':False}))
