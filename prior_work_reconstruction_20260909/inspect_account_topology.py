from pathlib import Path
import json,re,hashlib,time
P=Path(r'C:/Users/zmoor/Documents/forex/trad')
def safe_read(p):
    raw=p.read_bytes()
    assert len(raw)<1024*1024
    # Broker identities are replaced before decoding/inspection; never emitted.
    masked=re.sub(rb'\b\d{3}-\d{3}-\d+-\d{3}\b',lambda m:b'masked-'+m.group(0)[-3:],raw)
    return json.loads(masked),{'path':str(p),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'mtime_epoch':p.stat().st_mtime}
allowed={'name','role','label','account_key','account_id','id','alias','environment','active','enabled','status','currency','model_id','strategy','strategy_id','mode','account_role','account_suffix','execution_enabled','research_only','can_place_orders','can_authorize','can_promote','time','generated_utc','updated_utc','timestamp','ok','openTradeCount','openPositionCount','pendingOrderCount','open_trade_count','open_position_count','pending_order_count','positions_current','orders_current','account_values_current','verified_at_utc','execution_eligible','allowed_account_ids','allowed_account_keys','primary','challenger','account_type'}
def clean(v):
    if isinstance(v,dict):
        out={}
        for k,x in v.items():
            if any(y in k.lower() for y in ['token','secret','password','credential']): continue
            if k in {'account_id','id'}: continue
            if k in allowed and not isinstance(x,(list,dict)): out[k]=x
            elif isinstance(x,(list,dict)):
                t=clean(x)
                if t: out[k]=t
        return out
    if isinstance(v,list): return [clean(x) for x in v if isinstance(x,(dict,list))]
    return None
files=['config/accounts_registry.json','data/oanda_training_manager/state/account_007_dashboard_v1.json','data/oanda_training_manager/state/practice_006_supervised_account_v1.json','data/oanda_training_manager/state/practice_006_currency_rank_heartbeat_v1.json']
result={'actual_read_epoch':time.time(),'records':[]}
for f in files:
    d,meta=safe_read(P/f); result['records'].append({**meta,'top_level_keys':list(d),'metadata':clean(d)})
out=Path(__file__).with_name('ACCOUNT_TOPOLOGY_MASKED_OBSERVATION_20260909.json')
b=json.dumps(result,indent=2,sort_keys=True)+'\n'
assert not re.search(r'\b\d{3}-\d{3}-\d+-\d{3}\b',b)
out.write_text(b,encoding='utf-8')
print(json.dumps({'path':str(out),'sha256':hashlib.sha256(out.read_bytes()).hexdigest(),'record_count':len(result['records']),'account_identity_fields_omitted':True}))
