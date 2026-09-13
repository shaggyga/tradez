// Local HTTP-only browser QA. All evidence/profile files stay in a unique run.
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const {chromium}=require('C:/Users/zmoor/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const output=path.join(__dirname,new Date().toISOString().replace(/[:.]/g,'_'));
fs.mkdirSync(output,{recursive:false});
const report={schema_version:'companion_dashboard_live_qa_v1',started_utc:new Date().toISOString(),checks:[],page_errors:[],blocked_requests:[],requests:[],production_mutations:false};
const check=(name,passed,detail)=>report.checks.push({name,passed,detail});
let context;
(async()=>{
 context=await chromium.launchPersistentContext(path.join(output,'edge-profile'),{headless:true,channel:'msedge',viewport:{width:1440,height:1024},deviceScaleFactor:1});
 await context.route('**/*',route=>{
   const request=route.request(),url=new URL(request.url());
   if(url.origin!=='http://127.0.0.1:8765'||request.method()!=='GET'){
     report.blocked_requests.push({url:request.url(),method:request.method()});return route.abort();
   }
   report.requests.push({url:request.url(),method:request.method()});return route.continue();
 });
 const page=context.pages()[0]||await context.newPage();
 page.on('pageerror',error=>report.page_errors.push(String(error)));
 const response=await page.goto('http://127.0.0.1:8765/#oanda',{waitUntil:'domcontentloaded'});
 report.http_status=response.status();
 await page.getByText('Signals vs live market',{exact:true}).waitFor({timeout:30000});
 await page.waitForFunction(()=>document.querySelectorAll('#market-overview tbody tr').length>0,null,{timeout:20000});
 await page.waitForFunction(()=>lastMainData?.collection_status?.model_attempts?.latest_published_forecast?.forecasts?.length===2,null,{timeout:20000}).catch(()=>{});
 report.loaded=await page.evaluate(()=>{
   const data=lastMainData,collection=data.collection_status||{},account=data.account||{},news=data.news_sentiment||{};
   return{observed_utc:new Date().toISOString(),selected_study:collection.selected_study,forecasting:collection.forecasting,
     latest_publication:collection.model_attempts?.latest_published_forecast,collector_status:collection.status,
     news:{fresh:news.fresh,status:news.status,generated_utc:news.generated_utc,active_article_count:news.active_article_count,active_scored_pair_count:news.active_scored_pair_count},
     account:{positions_current:account.positions_current,positions:account.positions,verified_at_utc:account.verified_at_utc},
     first_row:document.querySelector('#market-overview tbody tr')?.innerText,
     eurusd_row:[...document.querySelectorAll('#market-overview tbody tr')].find(row=>row.cells[0]?.innerText.trim()==='EUR / USD')?.innerText};
 });
 check('local_http_200',report.http_status===200,report.http_status);
 check('selected_companion',report.loaded.selected_study==='eurusd_v1',report.loaded.selected_study);
 const published=report.loaded.latest_publication;
 check('two_live_published_models',published?.status==='in_progress'&&published?.forecasts?.length===2,published?.status||'No retained publication yet');
 check('eurusd_first_when_published',published?report.loaded.first_row?.startsWith('EUR / USD'):null,report.loaded.first_row);
 const eur=page.locator('#market-overview tbody tr').filter({has:page.locator('td:first-child',{hasText:/^EUR \/ USD$/})});
 if(published&&await eur.count()){
   const text=await eur.innerText();
   check('visible_two_directions_expected_probability',/Ridge: (Buy|Sell|Abstain)/.test(text)&&/State space: (Buy|Sell|Abstain)/.test(text)&&(text.match(/Expected /g)||[]).length===2&&(text.match(/p\(up\)/g)||[]).length===2,text);
   await eur.locator('details summary').click();
   const expanded=await eur.innerText();
   check('h1_original_clocks_and_unscored_live_move',expanded.includes('H1')&&expanded.includes('Issued')&&expanded.includes('Reference')&&expanded.includes('target')&&expanded.includes('in progress, unscored'),expanded);
 }
 report.windows=[];
 for(const window of ['5m','15m','60m']){
   await page.locator(`button.market-window[data-window="${window}"]`).click();
   await page.waitForFunction(window=>document.querySelector('#market-overview th:nth-child(4)')?.textContent===`Technical bias (${window})`,window);
   const state=await page.evaluate(window=>{
     const row=[...document.querySelectorAll('#market-overview tbody tr')].find(row=>row.cells[0]?.innerText.trim()==='EUR / USD')||document.querySelector('#market-overview tbody tr');
     return{window,selected:document.querySelector('button.market-window[aria-pressed="true"]')?.dataset.window,header:document.querySelector('#market-overview th:nth-child(4)')?.innerText,
       pair:row?.cells[0]?.innerText,observed:row?.cells[2]?.innerText,technical:row?.cells[3]?.innerText};
   },window);
   report.windows.push(state);
   check('matching_window_'+window,state.selected===window&&state.header===`Technical bias (${window})`&&
     (state.technical.includes(window+' momentum')||state.technical.includes('No '+window+' history anchor'))&&
     (state.observed.includes(' · '+window)||state.observed.includes('No matching history anchor')),state);
 }
 await page.locator('button.market-window[data-window="5m"]').click();
 report.news_render=await page.evaluate(()=>{
   const original=lastMainData,current=document.querySelector('#market-overview tbody tr')?.cells[4]?.innerText;
   const copy=JSON.parse(JSON.stringify(original));
   copy.news_sentiment={...(copy.news_sentiment||{}),generated_utc:new Date(Date.now()-301000).toISOString()};
   renderMarketOverview(copy);
   const simulatedStale=document.querySelector('#market-overview tbody tr')?.cells[4]?.innerText;
   renderMarketOverview(original);
   return{current,simulated_stale_news_only:simulatedStale,method:'Same current live data with only news timestamp made 301 seconds old in browser memory; original restored immediately.'};
 });
 if(report.loaded.news.fresh===true&&report.loaded.news.active_scored_pair_count===0){
   check('fresh_zero_direction_news_reports_screening',report.news_render.current?.includes('No directional news')&&report.news_render.current?.includes(report.loaded.news.active_article_count+' articles screened'),report.news_render.current);
 }
 check('stale_news_is_unavailable',report.news_render.simulated_stale_news_only?.includes('Unavailable')&&report.news_render.simulated_stale_news_only?.includes('News snapshot not current'),report.news_render.simulated_stale_news_only);
 report.positions=await page.evaluate(()=>({account_text:document.querySelector('#account')?.innerText,table_cells:[...document.querySelectorAll('#market-overview tbody tr')].map(row=>row.cells[6]?.innerText)}));
 if(report.loaded.account.positions_current===true&&Array.isArray(report.loaded.account.positions)&&report.loaded.account.positions.length===0){
   check('current_empty_positions_are_flat',report.positions.account_text?.includes('No open positions.')&&report.positions.table_cells.every(value=>value==='Flat'),report.positions);
 }
 await page.screenshot({path:path.join(output,'desktop_5m.png'),fullPage:false});
 report.desktop=await page.evaluate(()=>({width:innerWidth,scrollWidth:document.documentElement.scrollWidth,bodyScrollWidth:document.body.scrollWidth,first_row:document.querySelector('#market-overview tbody tr')?.innerText}));
 await page.setViewportSize({width:390,height:844});
 await page.screenshot({path:path.join(output,'mobile_390.png'),fullPage:false});
 report.mobile=await page.evaluate(()=>{const table=document.querySelector('#market-overview .table-wrap');return{width:innerWidth,scrollWidth:document.documentElement.scrollWidth,bodyScrollWidth:document.body.scrollWidth,table:{clientWidth:table?.clientWidth,scrollWidth:table?.scrollWidth,overflowX:table?getComputedStyle(table).overflowX:null}}});
 check('mobile_table_scroll_not_page_overflow',report.mobile.scrollWidth<=390&&report.mobile.bodyScrollWidth<=390&&report.mobile.table.scrollWidth>report.mobile.table.clientWidth&&['auto','scroll'].includes(report.mobile.table.overflowX),report.mobile);
 check('no_javascript_page_errors',report.page_errors.length===0,report.page_errors);
 check('browser_requested_only_local_get',report.blocked_requests.length===0,report.blocked_requests);
})().catch(error=>{report.exception=String(error.stack||error);process.exitCode=1;}).finally(async()=>{
 if(context)await context.close();
 report.finished_utc=new Date().toISOString();
 report.artifacts={};for(const name of ['desktop_5m.png','mobile_390.png']){const p=path.join(output,name);if(fs.existsSync(p))report.artifacts[name]=crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex');}
 const receipt=path.join(output,'receipt.json');fs.writeFileSync(receipt,JSON.stringify(report,null,2),{flag:'wx'});
 const failed=report.checks.filter(row=>row.passed===false),unverified=report.checks.filter(row=>row.passed==null);
 console.log(JSON.stringify({receipt,receipt_sha256:crypto.createHash('sha256').update(fs.readFileSync(receipt)).digest('hex'),checks:report.checks.length,failed,unverified,exception:report.exception,page_errors:report.page_errors,mobile:report.mobile},null,2));
 if(failed.length)process.exitCode=1;
});
