"""One bounded local HTTP observation; raw responses remain private and unexported."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,time,urllib.request,subprocess,shutil,sys

BASE=Path(__file__).resolve().parent
ROOT=BASE.parents[2]/'trad'
OUT=BASE/'actual_live_api_table_001'
OUT.mkdir(exist_ok=False)
PRIVATE=OUT/'private_raw';PRIVATE.mkdir()
EXPECTED_HTML='da9ae85ac0efd524a59ab7f24cf5de6602b737610e996556abf4b0e783b690c6'
EXPECTED_DASHBOARD='3882467376ddace472163baa8f37d2c729d4d3fd9ba18c18c66936c2a0cbb17a'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def seal(value):return sha(canonical({k:v for k,v in value.items() if k!='payload_sha256'}))
def write(path,value):
    with path.open('xb') as out:out.write((json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n').encode())
def read(endpoint,filename,limit):
    started=time.time();mono=time.monotonic()
    request=urllib.request.Request('http://127.0.0.1:8765'+endpoint,headers={'Cache-Control':'no-cache'})
    with urllib.request.urlopen(request,timeout=15) as response:
        assert response.status==200
        raw=response.read(limit+1)
        assert len(raw)<=limit
    completed=time.time()
    with (PRIVATE/filename).open('xb') as out:out.write(raw)
    return raw,{'endpoint':endpoint,'status':200,'read_started_epoch':started,'read_completed_epoch':completed,'elapsed_sec':time.monotonic()-mono,'bytes':len(raw),'raw_sha256':sha(raw),'private_raw_path':str(PRIVATE/filename)}

assert sha((ROOT/'oanda_main_signal_dashboard.html').read_bytes())==EXPECTED_HTML
assert sha((ROOT/'oanda_practice_live_dashboard.py').read_bytes())==EXPECTED_DASHBOARD
raw_main,main_read=read('/api/main','main.json',16*1024*1024)
main=json.loads(raw_main);hint=main['joint_v3_ledger_observation']
raw_full,full_read=read('/api/joint-v3-ledger-observation','ledger_full.json',2*1024*1024)
full=json.loads(raw_full)
raw_html,html_read=read('/','served.html',1024*1024)
assert sha(raw_html)==EXPECTED_HTML
for value,receipt in ((hint,main_read),(full,full_read)):
    assert value['payload_sha256']==seal(value)
    assert value['schema_version']=='joint_v3_ledger_observer_api_v1_20260909'
    assert value['status'] in ('current_ledger_observation','partial_ledger_observation')
    assert value['proof_basis']=='independent_readonly_original_committed_ledgers'
    assert value['research_only'] is True
    for key in ('can_place_orders','can_promote','can_authorize','account_eligible','proof_eligible','historical_rows_imported','old_heartbeat_validated_for_forecasts','current_inputs_observed'):assert value[key] is False,key
    assert value['consumer']['observation_completed_epoch']<=value['consumer']['observed_epoch']<=receipt['read_completed_epoch']
    assert receipt['read_completed_epoch']-value['consumer']['observation_completed_epoch']<=90
assert hint['observer_report'] is None
report=full['observer_report'];summary=report['summary']
assert sha(canonical(report))==full['observer_report_sha256']
assert report['payload_sha256']==seal(report)
assert sha(canonical(summary))==report['summary_sha256']
assert summary['payload_sha256']==seal(summary)
assert sha(canonical(report['observer_spec']))==report['observer_spec_sha256']
assert full['consumer']['observed_epoch']>=hint['consumer']['observed_epoch']
assert full['consumer']['observation_completed_epoch']>=hint['consumer']['observation_completed_epoch']
assert full['observer_api_specification_sha256']==hint['observer_api_specification_sha256']
rows={row['instrument']:row for row in summary['rows']};assert len(rows)==68
for identity in full['consumer']['active_forecast_ids']:
    forecast=rows[identity['instrument']]['families']['ridge_price_news_v1']['latest_forecast']
    assert identity['decision_id']==forecast['decision_id'] and identity['forecast_sha256']==forecast['forecast_sha256']
    assert identity['cohort_id']==forecast['forecasts'][0]['cohort_id']
    assert full['consumer']['observed_epoch']<forecast['target_epoch']

# Evaluate the exact served selector in Node using the actual retained HTTP data.
# The fetch stub returns those bytes; it performs no second request or model work.
sys.path.insert(0,str(ROOT))
import test_oanda_joint_v3_ledger_table_visibility as fixtures
html=raw_html.decode('utf-8')
source=html[html.index('    // BEGIN independent ledger table selector.'):html.index('    function renderAvailableSignals(data)')]
checked=time.time()
body='const liveMain='+json.dumps(main)+';const full='+json.dumps(full)+f';fakeNow={checked};'+r'''
ledgerRefreshGeneration=1;global.fetch=async()=>response(full);
assert.equal(await refreshLedgerTable(liveMain,1),true);
const activity=tableForecastActivity(liveMain),original=JSON.stringify(full.observer_report);
assert.equal(activity.current,true);
const active=full.consumer.active_forecast_ids.filter(id=>{const row=full.observer_report.summary.rows.find(r=>r.instrument===id.instrument);return row.families.ridge_price_news_v1.latest_forecast.target_epoch>fakeNow});
assert.deepEqual([...activity.forecasts.keys()].sort(),active.map(id=>id.instrument).sort());
for(const [instrument,arm] of activity.forecasts){const row=full.observer_report.summary.rows.find(r=>r.instrument===instrument),p=row.families.ridge_price_news_v1.latest_forecast,a=p.forecasts[0];for(const key of ['issued_epoch','reference_epoch','target_epoch','news_evidence_epoch','news_available_epoch','news_expires_epoch','predicted_return_bps'])assert.equal(arm[key],a[key]);assert.equal(arm.publication_epoch,p.publication_epoch);assert.equal(arm.consumption_epoch,p.consumption_epoch);}
renderMarketOverview(liveMain);
const table=elements['market-overview'].innerHTML;
assert.match(table,/independently verified from original ledger/);
assert.equal(JSON.stringify(full.observer_report),original);
console.log(JSON.stringify({current:activity.current,forecast_pairs:activity.forecast,total:activity.total,basis:activity.basis,original_clocks_and_report_unchanged:true,table_has_independent_ledger_label:table.includes('independently verified from original ledger'),producer_transport_status:full.original_producer_envelope.status,producer_reported_errors:full.original_producer_envelope.producer_reported_errors,old_producer_status:jointForecastActivity(liveMain).current,missing_pairs:[...activity.states.keys()].filter(pair=>!activity.forecasts.has(pair)),examples:['EUR_USD','GBP_USD','USD_JPY'].filter(pair=>activity.forecasts.has(pair)).map(pair=>{const a=activity.forecasts.get(pair);return{instrument:pair,side:a.side,predicted_return_bps:a.predicted_return_bps,issued_epoch:a.issued_epoch,reference_epoch:a.reference_epoch,target_epoch:a.target_epoch,ledger_observed_epoch:a.ledger_observed_epoch}})}));
'''
result=subprocess.run([shutil.which('node'),'-'],input=fixtures.BOOT+'\n'+source+'\n(async()=>{'+body+'})().catch(e=>{console.error(e);process.exitCode=1});',text=True,encoding='utf-8',capture_output=True,timeout=20)
assert result.returncode==0,result.stderr
projection=json.loads(result.stdout)
assert sha((ROOT/'oanda_main_signal_dashboard.html').read_bytes())==EXPECTED_HTML
assert sha((ROOT/'oanda_practice_live_dashboard.py').read_bytes())==EXPECTED_DASHBOARD
receipt={'schema_version':'live_api_table_acceptance_v1_20260909','status':'passed','generated_utc':datetime.now(timezone.utc).isoformat(),'reads':{'main':main_read,'ledger_full':full_read,'served_html':html_read},'sources':{'served_and_local_html_sha256':EXPECTED_HTML,'dashboard_sha256':EXPECTED_DASHBOARD},'semantic_hashes_verified':{'api_full_payload_sha256':full['payload_sha256'],'api_compact_payload_sha256':hint['payload_sha256'],'observer_report_sha256':full['observer_report_sha256'],'summary_sha256':report['summary_sha256'],'observer_spec_sha256':report['observer_spec_sha256']},'consumer':{key:value for key,value in full['consumer'].items() if key!='active_forecast_ids'},'table_projection_checked_epoch':checked,'table_projection':projection,'no_reissued_forecast_clocks':True,'browser_engine_scope':'Exact served JavaScript evaluated in Node with actual retained responses; no browser layout or screenshots claimed.','private_raw_export_allowed':False,'network_scope':'Three read-only requests to the existing local dashboard only; no broker GET, source edits or restart.'}
write(OUT/'LIVE_API_TABLE_ACCEPTANCE_20260909.json',receipt)
print(json.dumps({'status':'passed','receipt':str(OUT/'LIVE_API_TABLE_ACCEPTANCE_20260909.json'),'sha256':sha((OUT/'LIVE_API_TABLE_ACCEPTANCE_20260909.json').read_bytes()),'forecast_pairs':projection['forecast_pairs'],'registered_pairs':projection['total'],'missing_pairs':projection['missing_pairs'],'producer_transport_status':projection['producer_transport_status'],'producer_reported_errors':projection['producer_reported_errors'],'request_elapsed_sec':{k:round(v['elapsed_sec'],3) for k,v in receipt['reads'].items()}}))
