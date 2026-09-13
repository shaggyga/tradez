"""Portable synthetic browser tests; no server, broker, live data or external fixture reads."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

import pytest

ROOT=Path(__file__).resolve().parent
HTML=ROOT/'oanda_main_signal_dashboard.html'
NOW=1788943200.0

BOOT=r'''
const assert=require('node:assert/strict');
let fakeNow=1788943200,fakeMono=1000,lastMainData=null;
Date.now=()=>fakeNow*1000;
global.performance={now:()=>fakeMono*1000};
let marketOverviewWindow='15m',marketOverviewAll=true,marketOverviewPair='';
const elements={};const $=key=>elements[key]||(elements[key]={innerHTML:''});
const esc=value=>String(value??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const num=(value,n=2)=>Number(value).toFixed(n),signed=(value,n=2)=>(Number(value)>=0?'+':'')+num(value,n),cls=value=>Number(value)>0?'positive':'negative';
const clone=value=>JSON.parse(JSON.stringify(value));
const hash=n=>Number(n).toString(16).padStart(64,'0');
const safe={research_only:true,can_place_orders:false,can_promote:false,can_authorize:false,account_eligible:false,proof_eligible:false,historical_rows_imported:false};
function fixture(active=1){
 const now=fakeNow,rows=[];const names=['EUR_USD','GBP_USD','USD_JPY',...Array.from({length:65},(_,i)=>`${String.fromCharCode(65+Math.floor(i/26))}${String.fromCharCode(65+i%26)}X_USD`)];
 for(let i=0;i<68;i++){
  const instrument=names[i],cohort_id=`joint_price_news_v1_20260907.${instrument}.ridge_price_news_v1.news_repair_v3_20260908.prospective`,pip_size=instrument==='USD_JPY'?.01:.0001;
  const arm={instrument,family:'ridge_price_news_v1',cohort_id,pip_size:String(pip_size),horizon_sec:3600,reference_epoch:now-1000,issued_epoch:now-900,target_epoch:now+2600,reference_mid:'1.25',predicted_return_bps:2,probability_up:.55,side:1,news_evidence_epoch:now-1000,news_generated_epoch:now-980,news_first_observed_epoch:now-950,news_available_epoch:now-950,news_expires_epoch:now-700,news_capture_sha256:hash(8),input_contributions:{neutral_news_ablation_expected_pips:2,news_ablation_difference_pips:.5,matched_price_only_expected_pips:1.8,training_rows:24,nonzero_news_context_training_rows:20,vetted_news_training_rows:0}};
  const p={instrument,family:arm.family,decision_id:hash(100+i),forecast_sha256:hash(200+i),publication_receipt_sha256:hash(300+i),consumer_receipt_sha256:hash(400+i),publication_verified:true,consumption_verified:true,reference_epoch:arm.reference_epoch,reference_available_epoch:now-920,issued_epoch:arm.issued_epoch,publication_epoch:now-899,consumption_epoch:now-898,publication_verified_epoch:now-5,horizon_sec:3600,target_epoch:arm.target_epoch,forecasts:[arm]};
  const ob={read_started_epoch:now-10,forecast_join_read_started_epoch:now-8,forecast_join_read_completed_epoch:now-7,read_completed_epoch:now-4,verification_completed_epoch:now-3,query_only:true,contract_and_activation_verified:true,original_published_forecast:i<active,original_row_identity_binding_sha256:hash(600+i),latest_original_target_epoch:arm.target_epoch};
  rows.push({instrument,pip_size,families:{ridge_price_news_v1:{status:i<active?'forecast':'unavailable',reason:i<active?'verified_original_forecast':'no_verified_published_forecast',activated_epoch:now-10000,observed_epoch:now-3,cohort_id,contract_sha256:hash(500+i),origin_registry_sha256:ledgerTablePins.registry,origin_study:'joint_v3',ledger_observation:ob,current_readiness:{status:'unavailable',reason:'current_inputs_not_observed',observed_epoch:null},last_attempt:null,latest_forecast:i<active?p:null}}});
 }
 const spec={...safe,schema_version:'joint_v3_ledger_observer_specification_v2_20260909',origin_registry_sha256:ledgerTablePins.registry,origin_activation_sha256:ledgerTablePins.activation,observer_source_bindings:{...ledgerOwnSources},original_source_bindings:Object.fromEntries(ledgerOriginSources.map((name,i)=>[name,hash(800+i)]))};
 const report={...safe,schema_version:'joint_v3_ledger_status_observer_v2_20260909',observer_spec_sha256:ledgerTablePins.observer,observer_spec:spec,started_epoch:now-15,completed_epoch:now-2,registered_pairs:68,verified_ledger_pairs:68,unvisited_pairs:0,current_input_readiness_observed:false,old_heartbeat_validated_for_forecasts:false,original_ledger_rows_written:false,original_predictions_recomputed:false,payload_sha256:hash(9),summary_sha256:hash(10),summary:{...safe,schema_version:'joint_v3_ledger_observer_summary_v2_20260909',registry_sha256:ledgerTablePins.observer,generated_epoch:now-2,payload_sha256:hash(11),rows}};
 return{...safe,schema_version:'joint_v3_ledger_observer_api_v2_20260909',status:'current_ledger_observation',proof_basis:'independent_readonly_original_committed_ledgers',observer_api_specification_sha256:ledgerTablePins.api,observer_report_sha256:hash(12),observer_report:report,payload_sha256:hash(13),current_inputs_observed:false,old_heartbeat_validated_for_forecasts:false,endpoint:'/api/joint-v3-ledger-observation',consumer:{observed_epoch:now-1,observation_completed_epoch:now-2,registered_pairs:68,verified_ledger_pairs:68,unvisited_pairs:0,current_forecast_pairs:active,active_forecast_ids:rows.slice(0,active).map(row=>{const p=row.families.ridge_price_news_v1.latest_forecast;return{instrument:row.instrument,decision_id:p.decision_id,forecast_sha256:p.forecast_sha256,cohort_id:p.forecasts[0].cohort_id};})},original_producer_envelope:{status:'generation_mismatch',producer_reported_errors:12}};
}
const compact=value=>({...clone(value),observer_report:null});
const slot=value=>value.observer_report.summary.rows[0].families.ridge_price_news_v1;
const response=value=>({ok:true,headers:{get:()=>null},text:async()=>JSON.stringify(value)});
function fakeTimers(){
 let serial=0;const pending=new Map();global.setTimeout=(fn,delay)=>{const id=++serial;pending.set(id,{fn,delay});return id};global.clearTimeout=id=>pending.delete(id);
 return{pending,fire(id){const task=pending.get(id);assert.ok(task);pending.delete(id);fakeNow+=task.delay/1000;fakeMono+=task.delay/1000;for(const other of pending.values())other.delay=Math.max(0,other.delay-task.delay);task.fn();},next(){return [...pending.keys()].sort((a,b)=>pending.get(a).delay-pending.get(b).delay)[0]}};
}
'''


def run_js(body):
    node=shutil.which('node')
    if not node:pytest.skip('Node.js is required for browser logic fixtures')
    html=HTML.read_text(encoding='utf-8')
    source=html[html.index('    // BEGIN independent ledger table selector.'):html.index('    function renderAvailableSignals(data)')]
    source+='\n'+html[html.index('    async function refresh(){'):html.index('    async function refreshResearch()')]
    completed=subprocess.run([node,'-'],input=BOOT+'\n'+source+'\n(async()=>{\n'+body+'\n})().catch(e=>{console.error(e);process.exitCode=1});',text=True,encoding='utf-8',capture_output=True,timeout=20)
    assert completed.returncode==0,completed.stdout+completed.stderr


def test_original_forecasts_survive_producer_mismatch_and_later_news_expiry():
    run_js("const f=fixture(66),before=JSON.stringify(f),r=ledgerForecastActivity(f);assert.equal(r.forecast,66);assert.equal(r.total,68);assert.equal(r.current,true);assert.equal(JSON.stringify(f),before);const a=r.forecasts.get('EUR_USD');assert.equal(a.target_epoch,slot(f).latest_forecast.target_epoch);assert.ok(a.news_expires_epoch<fakeNow);assert.equal(jointForecastActivity({}).current,false);")


@pytest.mark.parametrize('change',[
 "f.schema_version='other'","f.proof_basis='sealed_summary'","f.can_place_orders=true","f.can_promote=true","f.can_authorize=true","f.account_eligible=true","f.proof_eligible=true","f.historical_rows_imported=true","f.research_only=false",
 "f.observer_api_specification_sha256=hash(999)","f.observer_report_sha256='bad'","f.old_heartbeat_validated_for_forecasts=true","f.current_inputs_observed=true",
 "f.consumer.observed_epoch=fakeNow+1","f.consumer.observation_completed_epoch=fakeNow-91","f.consumer.observed_epoch=fakeNow-10","f.consumer.registered_pairs=67","f.consumer.current_forecast_pairs=2","f.consumer.active_forecast_ids.push(clone(f.consumer.active_forecast_ids[0]));f.consumer.current_forecast_pairs=2",
 "f.observer_report.schema_version='other'","f.observer_report.observer_spec.schema_version='other'","f.observer_report.observer_spec_sha256=hash(999)","f.observer_report.can_place_orders=true","f.observer_report.original_predictions_recomputed=true","f.observer_report.completed_epoch=fakeNow+1",
 "f.observer_report.observer_spec.origin_registry_sha256=hash(999)","f.observer_report.observer_spec.origin_activation_sha256=hash(999)","f.observer_report.observer_spec.observer_source_bindings['oanda_joint_v3_ledger_status_observer_v1.py']=hash(999)","delete f.observer_report.observer_spec.original_source_bindings[ledgerOriginSources[0]]",
 "f.observer_report.summary.schema_version='old'","f.observer_report.summary.rows.pop()","f.observer_report.summary.rows[1]=clone(f.observer_report.summary.rows[0])","f.observer_report.summary.generated_epoch-=1",
])
def test_wrong_envelope_proof_authority_identity_and_clocks_withhold(change):
    run_js('const f=fixture();'+change+";const r=ledgerForecastActivity(f);assert.equal(r.current,false);assert.equal(r.forecast,0);")


@pytest.mark.parametrize('change',[
 "s.cohort_id='other'","s.origin_study='joint_v2'","s.observed_epoch=fakeNow+1","s.observed_epoch=fakeNow-91","s.ledger_observation.query_only=false","s.ledger_observation.contract_and_activation_verified=false","s.ledger_observation.original_row_identity_binding_sha256='bad'","s.ledger_observation.forecast_join_read_completed_epoch=s.latest_forecast.consumption_epoch-1",
 "p.publication_verified=false","p.consumption_verified=false","p.consumer_receipt_sha256='bad'","p.publication_receipt_sha256='bad'","p.decision_id=hash(999)","p.forecast_sha256=hash(999)","p.reference_available_epoch=p.issued_epoch+1","p.target_epoch=fakeNow","p.publication_epoch=p.consumption_epoch+1","p.forecasts.push(clone(p.forecasts[0]))","p.forecasts[0]=null","s.ledger_observation.read_started_epoch=f.observer_report.started_epoch-1",
 "a.cohort_id='other'","a.instrument='GBP_USD'","a.pip_size='1'","a.news_evidence_epoch=a.issued_epoch-301","a.news_expires_epoch=a.issued_epoch-1","a.news_available_epoch+=1","a.predicted_return_bps=null","a.predicted_return_bps='NaN'","a.probability_up=1.1","a.side='Buy'","a.input_contributions.news_ablation_difference_pips=null",
])
def test_bad_original_row_is_withheld_without_erasing_valid_sibling(change):
    run_js('const f=fixture(2),s=slot(f),p=s.latest_forecast,a=p.forecasts[0];'+change+";const r=ledgerForecastActivity(f);assert.equal(r.current,true);assert.equal(r.forecast,1);assert.equal(r.forecasts.has('EUR_USD'),false);assert.equal(r.forecasts.has('GBP_USD'),true);")


def test_original_h1_target_is_rechecked_on_local_filter_render():
    run_js("const f=fixture();slot(f).latest_forecast.target_epoch=fakeNow+1;slot(f).latest_forecast.reference_epoch=fakeNow-3599;slot(f).latest_forecast.forecasts[0].target_epoch=fakeNow+1;slot(f).latest_forecast.forecasts[0].reference_epoch=fakeNow-3599;slot(f).ledger_observation.latest_original_target_epoch=fakeNow+1;assert.equal(ledgerForecastActivity(f).forecast,1);fakeNow+=1;assert.equal(ledgerForecastActivity(f).forecast,0);")


def test_partial_proof_and_missing_quote_do_not_claim_input_readiness_or_spread():
    run_js("const f=fixture();f.status='partial_ledger_observation';f.consumer.verified_ledger_pairs=67;f.observer_report.verified_ledger_pairs=67;const r=ledgerForecastActivity(f);assert.equal(r.forecast,1);const cell=jointForecastCell(r,{instrument:'EUR_USD'},false,fakeNow,String);assert.match(cell,/independently verified from original ledger/);assert.match(cell,/generation_mismatch/);assert.match(cell,/Current quoted spread: unavailable/);assert.match(cell,/source window has since expired/);assert.match(cell,/Same model without news/);assert.match(cell,/Separate matched price-only estimate/);const missing=jointForecastCell(r,{instrument:'GBP_USD'},false,fakeNow,String);assert.match(missing,/No verified original ledger publication/);assert.match(missing,/readiness is not observed/);assert.doesNotMatch(missing,/warming|61/);")


def test_main_table_only_uses_explicit_ledger_basis_and_does_not_change_producer_selector():
    run_js("const f=fixture(),data={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=JSON.stringify(f);assert.equal(tableForecastActivity(data).forecast,1);assert.equal(jointForecastActivity(data).forecast,0);assert.equal(availableForecastCoverage(data).combined,0);assert.equal(availableForecastCoverage(data,tableForecastActivity(data)).combined,1);renderMarketOverview(data);assert.match(elements['market-overview'].innerHTML,/independently verified from original ledger/);clearLedgerTable();assert.equal(tableForecastActivity(data).forecast,0);renderMarketOverview(data);assert.doesNotMatch(elements['market-overview'].innerHTML,/Combined: Up/);")


def test_same_hash_compact_refresh_keeps_exact_original_report_and_new_consumer():
    run_js("const f=fixture(),raw=JSON.stringify(f.observer_report);ledgerRefreshGeneration=1;global.fetch=async()=>response(f);assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:compact(f)},1),true);const hint=compact(f);hint.consumer.observed_epoch+=.5;global.fetch=async()=>{throw Error('must not fetch')};assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:hint},1),true);const saved=JSON.parse(ledgerTableRaw);assert.equal(JSON.stringify(saved.observer_report),raw);assert.equal(saved.consumer.observed_epoch,hint.consumer.observed_epoch);")


@pytest.mark.parametrize('mode',['http','parse','throw','unavailable','older','wrong_spec','oversize'])
def test_changed_generation_fetch_failure_clears_old_forecasts(mode):
    run_js("const f=fixture();ledgerRefreshGeneration=1;ledgerFullReportRaw=ledgerTableRaw=JSON.stringify(f);const h=compact(f);h.observer_report_sha256=hash(900);global.fetch=async()=>{"+{
      'http':"return{ok:false}", 'parse':"return{ok:true,text:async()=>'{'}",'throw':"throw Error('failure')",
      'unavailable':"return response({status:'unavailable'})",'older':"const n=clone(f);n.consumer.observed_epoch-=1;return response(n)",
      'wrong_spec':"const n=clone(f);n.observer_api_specification_sha256=hash(99);return response(n)",
      'oversize':"return{ok:true,headers:{get:()=>3000000},text:async()=>{throw Error('must not read')}}"
    }[mode]+"};assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:h},1),false);assert.equal(ledgerTableRaw,null);assert.equal(ledgerFullReportRaw,null);")


def test_newer_self_contained_generation_accepted_without_old_consumer_mix():
    run_js("const f=fixture(),h=compact(f),newer=clone(f);newer.observer_report_sha256=hash(55);newer.consumer.observed_epoch+=.5;newer.consumer.active_forecast_ids=[];newer.consumer.current_forecast_pairs=0;ledgerRefreshGeneration=1;global.fetch=async()=>response(newer);assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:h},1),true);const saved=JSON.parse(ledgerTableRaw);assert.equal(saved.observer_report_sha256,hash(55));assert.equal(ledgerForecastActivity(saved).forecast,0);")


def test_late_response_cannot_restore_after_newer_failure():
    run_js("const f=fixture();let release;global.fetch=()=>new Promise(resolve=>{release=resolve});ledgerRefreshGeneration=1;const first=refreshLedgerTable({joint_v3_ledger_observation:compact(f)},1);ledgerRefreshGeneration=2;assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:{status:'unavailable'}},2),false);release(response(f));assert.equal(await first,false);assert.equal(ledgerTableRaw,null);")


def test_future_or_regressed_compact_does_not_reuse_previous_cache():
    run_js("const f=fixture();ledgerRefreshGeneration=1;ledgerFullReportRaw=ledgerTableRaw=JSON.stringify(f);const h=compact(f);h.consumer.observed_epoch-=.5;assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:h},1),false);assert.equal(ledgerTableRaw,null);")


def test_old_report_with_new_consumer_clock_is_not_accepted_after_changed_hint():
    run_js("const f=fixture(),h=compact(f),old=clone(f);old.consumer.observed_epoch+=.5;old.consumer.observation_completed_epoch-=.5;old.observer_report.completed_epoch-=.5;old.observer_report.summary.generated_epoch-=.5;ledgerRefreshGeneration=1;global.fetch=async()=>response(old);assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:h},1),false);assert.equal(ledgerTableRaw,null);")


def test_observed_response_high_water_survives_failed_reads():
    run_js("const f=fixture();ledgerRefreshGeneration=1;global.fetch=async()=>response(f);assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:compact(f)},1),true);ledgerRefreshGeneration=2;await refreshLedgerTable({joint_v3_ledger_observation:{status:'unavailable'}},2);const old=compact(f);old.consumer.observed_epoch-=.5;ledgerRefreshGeneration=3;global.fetch=async()=>{throw Error('older hint must not fetch')};assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:old},3),false);assert.equal(ledgerTableRaw,null);")


def test_expiry_during_full_fetch_does_not_publish_expired_rows():
    run_js("const f=fixture();ledgerRefreshGeneration=1;global.fetch=async()=>{fakeNow+=2601;return response(f)};assert.equal(await refreshLedgerTable({joint_v3_ledger_observation:compact(f)},1),false);assert.equal(ledgerTableRaw,null);")


def test_missing_namespace_retains_original_explicit_producer_path_only():
    run_js("const original={study_version:'joint_v2',status:'current',generated_epoch:fakeNow,research_only:true,can_place_orders:false,can_promote:false,rows:[]};const data={joint_price_news_forecasts:original};assert.equal(tableForecastActivity(data).source,original);data.joint_v3_ledger_observation={status:'unavailable'};assert.equal(tableForecastActivity(data).basis,'independent_ledger');assert.equal(tableForecastActivity(data).forecast,0);")


def test_hung_main_fetch_aborts_and_clears_existing_table_without_restoring_cache():
    run_js("const t=fakeTimers(),f=fixture();global.window={setTimeout};global.renderAvailableSignals=()=>{};global.renderCollectionStatus=()=>{};global.renderAccount=()=>{};lastMainData={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(f);renderLedgerTable(lastMainData);global.fetch=async(url,options)=>new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(Error('aborted'))));const request=refresh();const timeout=[...t.pending.keys()].find(id=>t.pending.get(id).delay===10000);assert.ok(timeout);t.fire(timeout);await request;assert.equal(ledgerTableRaw,null);assert.equal(ledgerFullReportRaw,null);assert.doesNotMatch(elements['market-overview'].innerHTML,/Combined: Up/);assert.match(elements.status.textContent,/Disconnected/);")


def test_original_target_expires_in_dom_while_network_remains_unresolved():
    run_js("const t=fakeTimers(),f=fixture(),p=slot(f).latest_forecast;for(const obj of [p,p.forecasts[0]]){obj.reference_epoch=fakeNow-3595;obj.target_epoch=fakeNow+5}slot(f).ledger_observation.latest_original_target_epoch=p.target_epoch;lastMainData={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(f);renderLedgerTable(lastMainData);assert.match(elements['market-overview'].innerHTML,/Combined: Up/);global.window={setTimeout};global.renderAvailableSignals=()=>{};global.renderCollectionStatus=()=>{};global.renderAccount=()=>{};global.fetch=async(url,options)=>new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(Error('aborted'))));const pending=refresh(),before=JSON.stringify(f.observer_report);t.fire(t.next());assert.doesNotMatch(elements['market-overview'].innerHTML,/Combined: Up/);assert.match(elements['market-overview'].innerHTML,/Original H1 target elapsed/);assert.equal(JSON.stringify(JSON.parse(ledgerTableRaw).observer_report),before);t.fire(t.next());await pending;assert.equal(ledgerTableRaw,null);")


def test_report_age_expiry_clears_dom_even_without_a_network_response():
    run_js("const t=fakeTimers(),f=fixture();lastMainData={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(f);renderLedgerTable(lastMainData);assert.match(elements['market-overview'].innerHTML,/Combined: Up/);t.fire(t.next());assert.doesNotMatch(elements['market-overview'].innerHTML,/Combined: Up/);assert.ok(ledgerTableRaw);assert.equal(ledgerForecastActivity(JSON.parse(ledgerTableRaw)).current,true);assert.equal(ledgerForecastActivity(JSON.parse(ledgerTableRaw)).forecast,0);t.fire(t.next());assert.equal(ledgerTableRaw,null);assert.equal(t.pending.size,0);")


def test_cancelled_expiry_callback_cannot_clear_or_replace_newer_generation():
    run_js("const t=fakeTimers(),f=fixture();lastMainData={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(f);renderLedgerTable(lastMainData);const old=t.pending.get(t.next()).fn;clearLedgerTable();const next=fixture(2);ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(next);lastMainData={joint_v3_ledger_observation:compact(next)};renderLedgerTable(lastMainData);const current=ledgerTableRaw;old();assert.equal(ledgerTableRaw,current);assert.equal(tableForecastActivity(lastMainData).forecast,2);")


def test_visibility_resume_rechecks_original_expiry_after_background_timer_throttling():
    run_js("const t=fakeTimers(),f=fixture();lastMainData={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(f);renderLedgerTable(lastMainData);fakeNow+=91;fakeMono+=91;recheckLedgerTableClock();assert.equal(ledgerTableRaw,null);assert.doesNotMatch(elements['market-overview'].innerHTML,/Combined: Up/);assert.equal(t.pending.size,0);")


@pytest.mark.parametrize('change',['fakeNow-=.1','fakeMono-=1','fakeNow+=10;fakeMono+=1'])
def test_local_clock_anomaly_withholds_until_a_new_verified_response(change):
    run_js("const t=fakeTimers(),f=fixture();lastMainData={joint_v3_ledger_observation:compact(f)};ledgerTableRaw=ledgerFullReportRaw=JSON.stringify(f);renderLedgerTable(lastMainData);"+change+";recheckLedgerTableClock();assert.equal(ledgerTableRaw,null);assert.equal(ledgerFullReportRaw,null);assert.doesNotMatch(elements['market-overview'].innerHTML,/Combined: Up/);assert.equal(t.pending.size,0);")


def test_visibility_listener_and_bounded_refresh_use_the_clock_checked_table_path():
    html=HTML.read_text(encoding='utf-8')
    assert "document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')recheckLedgerTableClock();});" in html
    refresh=html[html.index('    async function refresh(){'):html.index('    async function refreshResearch()')]
    assert 'await fetchMainState()' in refresh and 'renderLedgerTable(data)' in refresh


def test_old_producer_selector_and_activity_functions_are_byte_unchanged():
    html=HTML.read_text(encoding='utf-8')
    expected={
      'jointForecastActivity':'ae89033970c09e4072dd82d068220a9ee6188852da15f438c6779ef06467850b',
      'renderJointActivity':'252cb38a30f04877e7e43d669352a57663cda6615a33956c18f40ee235bd643f',
    }
    # Filled with independently retained, LF-normalized before-source function hashes.
    for name,digest in expected.items():
        begin=html.index('    function '+name+'(')
        end=html.index('\n    function ',begin+1)
        assert hashlib.sha256(html[begin:end].encode('utf-8')).hexdigest()==digest


@pytest.mark.parametrize('kind',['api','report','summary','spec'])
def test_v2_table_refuses_misrouted_original_v1_schema(kind):
    paths={'api':"f.schema_version='joint_v3_ledger_observer_api_v1_20260909'",'report':"f.observer_report.schema_version='joint_v3_ledger_status_observer_v1_20260909'",'summary':"f.observer_report.summary.schema_version='joint_v3_ledger_observer_summary_v1_20260909'",'spec':"f.observer_report.observer_spec.schema_version='joint_v3_ledger_observer_specification_v1_20260909'"}
    run_js('const f=fixture();'+paths[kind]+";assert.equal(ledgerForecastActivity(f).current,false);")
