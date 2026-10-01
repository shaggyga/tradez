from pathlib import Path
import copy,json,sys
import pytest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import forex_retained_management_readiness as m

def fixture(tmp_path):
    pairs=['AAA_'+a+b+'B' for a in 'ABCD' for b in 'ABCDEFGHIJKLMNOPQ']
    entry={'id':'saved','horizon_minutes':5,'original_model_id':'model'}
    reg={'pairs':pairs,'connections':[entry]};rid=m.sha(m.encoded(reg));fs=[];old=[]
    for p in pairs:
        f={'connection':'saved','instrument':p,'horizon_minutes':5,'reference_epoch':10000,'issued_epoch':10001.,'target_epoch':10300,
          'registry_sha256':rid,'original_model_id':'model','reference_mid':1.1,'expected_return_bps':2.,'feature_hash':'a'*64,
          'input_hash':'b'*64,'panel_sha256':None,'research_only':True,'can_place_orders':False,'can_promote':False,'models_fitted':0}
        fs.append(f);old.append({**f,'reference_epoch':9880,'issued_epoch':9881.,'target_epoch':10180,'input_hash':'c'*64})
    def issue(fs,at):return {'schema':'retained_forecast_connection_v1_20260930','registry_sha256':rid,'generated_epoch':at,'connections':[entry],
       'forecasts':fs,'coverage':[{'connection':'saved','instrument':p,'status':'eligible'} for p in pairs],
       'research_only':True,'can_place_orders':False,'can_promote':False,'models_fitted':0}
    before=issue(old,9882.);body=issue(fs,10001.5);ih=m.sha(m.encoded(before));ch=m.sha(m.encoded(body))
    anchors=[]
    for f in old:
        anchors.append({'id':m.tracking.prediction_key(f),'registry':rid,'connection':'saved','pair':f['instrument'],'horizon':5,'reference':9880.,'target':10180.,'observed':9883.,
          'body':json.dumps({'forecast':f,'publication_epoch':9882.,'publication_sha256':ih,'first_observed_epoch':9883.}),
          'state':'pending','outcome':None})
    data={'registry.json':reg,'current.json':{**body,'payload_sha256':ch},'anchors.json':anchors,'quotes.json':{'quotes':{}},'issue_'+ch+'.json':body,'issue_'+ih+'.json':before}
    meta={'schema':m.SCHEMA,'registry_sha256':rid,'capture_started_epoch':10001.5,'capture_completed_epoch':10002.,'source_bindings':m.sources(),**m.FLAGS}
    return meta,data

def save(path,meta,data):
    path.mkdir();meta=copy.deepcopy(meta);meta['files']={}
    for name,value in data.items():
        raw=m.encoded(value);(path/name).write_bytes(raw);meta['files'][name]={'bytes':len(raw),'sha256':m.sha(raw)}
    raw=m.encoded(meta);(path/'MANIFEST.json').write_bytes(raw);return m.sha(raw)

def test_actual_consumer_complete_population_and_no_fabricated_management(tmp_path):
    meta,data=fixture(tmp_path);rows,report=m.inspect(meta,data)
    assert len(rows)==68 and report['changed_target_from_anchor']==68 and report['management_eligible']==0
    assert report['existing_native_policy_gate']=='native_synthetic_qualification_tier_required'
    assert all(r['changed_input'] and not r['same_original_target'] for r in rows)
    assert all(r['native_curve_receipt_status']=='not_provided_by_retained_schema' for r in rows)

@pytest.mark.parametrize('fault',['missing_group','duplicate_coverage','missing_issue','anchor_change','future_observation','changed_column','changed_outcome','future_publication','wrong_model','duplicate_forecast'])
def test_actual_consumer_rejects_corrupt_lineage_and_population(tmp_path,fault):
    meta,d=fixture(tmp_path)
    if fault=='missing_group':d['current.json']['coverage'].pop()
    elif fault=='duplicate_coverage':d['current.json']['coverage'].append(d['current.json']['coverage'][0])
    elif fault=='missing_issue':del d[next(n for n in d if n.startswith('issue_'))]
    elif fault=='anchor_change':d['anchors.json'][0]['body']=d['anchors.json'][0]['body'].replace('2.0','3.0')
    elif fault=='future_observation':d['anchors.json'][0]['observed']=11000
    elif fault=='changed_column':d['anchors.json'][0]['target']+=1
    elif fault=='changed_outcome':d['anchors.json'][0]['state']='settled'
    elif fault=='future_publication':d['current.json']['generated_epoch']=11000
    elif fault=='wrong_model':d['current.json']['forecasts'][0]['original_model_id']='wrong'
    else:d['current.json']['forecasts'].append(d['current.json']['forecasts'][0])
    if fault in ('missing_group','duplicate_coverage','future_publication','wrong_model','duplicate_forecast'):
        old=d['current.json']['payload_sha256'];body={k:v for k,v in d['current.json'].items() if k!='payload_sha256'}
        new=m.sha(m.encoded(body));del d['issue_'+old+'.json'];d['issue_'+new+'.json']=body;d['current.json']['payload_sha256']=new
    with pytest.raises((ValueError,KeyError)):m.inspect(meta,d)

def test_stale_saved_reference_is_preserved_not_admitted(tmp_path):
    meta,d=fixture(tmp_path);meta['capture_completed_epoch']=10400
    rows,r=m.inspect(meta,d);assert r['saved_forecasts']==68 and r['fresh_forecasts']==0 and r['management_eligible']==0

def test_quote_float_cache_not_silently_upgraded(tmp_path):
    meta,d=fixture(tmp_path);pair=d['registry.json']['pairs'][0]
    d['quotes.json']['quotes'][pair]={'bid':1.1,'ask':1.1001,'tradeable':True,'quote_id':'test','market_epoch':10001.,'available_epoch':10001.}
    rows,r=m.inspect(meta,d);a=next(x for x in rows if x['instrument']==pair)
    assert a['manager_quote_status']=='exact_decimal_quote_required' and a['bid_representation']=='float'

def test_replay_chunk_restart_exact_and_tampered_resume_refused(tmp_path):
    meta,d=fixture(tmp_path);inp=tmp_path/'input';pin=save(inp,meta,d)
    assert m.run(inp,pin,tmp_path/'runs','one')['status']=='completed'
    assert m.run(inp,pin,tmp_path/'runs','two',max_new=1)['status']=='checkpointed'
    assert m.run(inp,pin,tmp_path/'runs','two',resume=True)['status']=='completed'
    for p in (tmp_path/'runs/one').glob('rows_*.json'):assert p.read_bytes()==(tmp_path/'runs/two'/p.name).read_bytes()
    assert (tmp_path/'runs/one/REPORT.json').read_bytes()==(tmp_path/'runs/two/REPORT.json').read_bytes()
    assert m.run(inp,pin,tmp_path/'runs','two')['status']=='verified_completed'
    p=tmp_path/'runs/two/rows_0000.json';p.write_bytes(b'{}')
    with pytest.raises(Exception):m.run(inp,pin,tmp_path/'runs','two',resume=True)

def test_capture_trusted_hash_and_input_inventory(tmp_path):
    meta,d=fixture(tmp_path);inp=tmp_path/'input';pin=save(inp,meta,d)
    with pytest.raises(ValueError,match='trusted_manifest'):m.load_capture(inp,'0'*64)
    (inp/'quotes.json').write_bytes(b'{}')
    with pytest.raises(ValueError,match='input_hash'):m.load_capture(inp,pin)

def test_decompression_bound_and_trailing_payload():
    assert m.unpack(m.zlib.compress(b'{}'))==b'{}'
    with pytest.raises(ValueError):m.unpack(m.zlib.compress(b'{}')+b'extra')
    with pytest.raises(ValueError):m.unpack(m.zlib.compress(b'x'*(m.MAX_BYTES+1)))

def test_cross_horizon_matching_preserves_future_supported_path():
    e={'kind':'legacy26_matched','arm':'ridge','feature_names':['x']};old={'instrument':'EUR_USD','reference_epoch':10000,'target_epoch':53200,'input_hash':'old'}
    f={'reference_epoch':31600,'target_epoch':53200,'input_hash':'new'};entries={'six':e,'other':{**e,'arm':'recovered_hgb'}}
    assert m.terminal_options(old,e,entries,{('six','EUR_USD'):f,('other','EUR_USD'):f},31602)==['six']
    assert m.terminal_options(old,e,entries,{('six','EUR_USD'):{**f,'target_epoch':53260}},31602)==[]
    assert m.terminal_options(old,e,entries,{('six','EUR_USD'):f},31800)==[]
