"""Bounded, deterministic offline replay of an authenticated retained-price slice."""
from pathlib import Path
import argparse
import hashlib
import json
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'trad'),str(ROOT)]
import oanda_exact_quote_receipts_v1 as q

SOURCES=['tools/forex_quote_receipt_replay.py','trad/oanda_exact_quote_receipts_v1.py',
         'trad/oanda_quote_transport.py','trad/oanda_research_quote_receipt_v1.py',
         'trad/oanda_curve_management_replay_v1.py','trad/src/forex_system/__init__.py',
         'trad/src/forex_system/research/sequential_portfolio_replay_v1.py']


def replay(source, expected, output):
    source,output=Path(source),Path(output)
    if source.stat().st_size>2*1024*1024:raise ValueError('input_byte_limit')
    raw=source.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('input_hash')
    data=json.loads(raw)
    if set(data['source_bindings'])!=set(SOURCES):raise ValueError('source_inventory')
    for p,h in data['source_bindings'].items():
        if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=h:raise ValueError('source_hash:'+p)
    rows=data['records']
    if not isinstance(rows,list) or not 1<=len(rows)<=128:raise ValueError('record_limit')
    pairs=data['instruments']
    if not isinstance(pairs,list) or pairs!=sorted(set(pairs)) or not 0<len(pairs)<=128:raise ValueError('instrument_inventory')
    results=[]
    for index,item in enumerate(rows):
        generation=item['generation']
        body=dict(schema=q.SCHEMA,producer='exact_raw_price_sidecar',session_id='retained_slice',
                  connection_generation=generation,snapshot_created_epoch=data['inspection_epoch'],
                  instruments=pairs,quotes={},refusals={},quote_count=0,research_only=True,can_place_orders=False)
        try:
            r=q.parse_price(item['raw'].encode(),received_epoch=item['received_epoch'],generation=generation,session_id='retained_slice')
            if r['instrument'] not in pairs:raise ValueError('instrument_outside_inventory')
            body['quotes'][r['instrument']]=r;body['quote_count']=1
            body['snapshot_sha256']=q.digest(body)
            result=q.map_receipts(body,observed_epoch=data['inspection_epoch'],decision_epoch=data['inspection_epoch'],instruments=pairs)
            results.append(dict(index=index,parsed=True,quote_id=r['quote_id'],bid=r['bid'],ask=r['ask'],
                                market_time=r['market_time_rfc3339'],received_epoch=r['received_epoch'],mapping=result))
        except ValueError as exc:
            results.append(dict(index=index,parsed=False,refusal=str(exc)))
    result=dict(input_sha256=expected,records=len(rows),parsed=sum(r['parsed'] for r in results),rows=results,
                scope='Retained payload-string replication at recorded inspection clock; no historical sidecar publication or live capture claim',
                models_fitted=0,manager_activation=False,broker_access=False)
    blob=(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n').encode()
    if output.exists():
        if output.read_bytes()!=blob:raise ValueError('existing_output_mismatch')
        status='completed_verified'
    else:
        output.parent.mkdir(parents=True,exist_ok=True)
        with output.open('xb') as f:f.write(blob)
        status='completed'
    return dict(status=status,records=len(rows),parsed=result['parsed'],sha256=hashlib.sha256(blob).hexdigest())


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',required=True);p.add_argument('--sha256',required=True);p.add_argument('--output',required=True)
    a=p.parse_args()
    print(json.dumps(replay(a.input,a.sha256,a.output)))
