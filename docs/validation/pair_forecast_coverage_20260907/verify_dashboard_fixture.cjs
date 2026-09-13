// Exercise the actual market renderer and event handlers against isolated fixtures.
const {chromium}=require('C:/Users/zmoor/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const fs=require('fs');const path=require('path');
const root=path.resolve(__dirname,'../trad');
const html=fs.readFileSync(path.join(root,'oanda_main_signal_dashboard.html'),'utf8');
const css=html.slice(html.indexOf('<style>'),html.indexOf('</style>')+8);
const functions=html.slice(html.indexOf('    let marketOverviewWindow='),html.indexOf('    function renderAvailableSignals(data)'));
const eventLines=html.split('\n').filter(line=>line.includes("document.addEventListener")&&(line.includes("button.market-window")||line.includes("button.market-show-all")||line.includes("event.target.id!=='market-pair'"))).join('\n');
const now=1788800400;
const pairs=['EUR_USD','GBP_USD','USD_JPY','AUD_USD','NZD_USD','USD_CAD','USD_CHF','EUR_JPY','AUD_JPY','CAD_JPY','EUR_GBP','GBP_JPY','CHF_JPY','EUR_AUD'];
const families=['ridge_return_repaired','probabilistic_state_space'];
const forecast=pair=>({status:'in_progress',instrument:pair,publication_epoch:now-90,target_epoch:now+3500,forecasts:families.map(family=>({family,side:1,probability_up:.986,predicted_return_bps:3.75,reference_epoch:now-100,reference_mid:1.1,issued_epoch:now-95}))});
const data={pair_local_forecasts:{status:'current',generated_epoch:now-2,rows:pairs.map((instrument,index)=>({instrument,observed_epoch:now-3,status:index<4?'forecast':index<10?'warming':'unavailable',reason_label:index<10?'Needs 61 own minute bars · 12/61':'No fresh quote',latest_published_forecast:index<4?forecast(instrument):null}))},market_overview:{generated_epoch:now,rows:pairs.slice(0,10).map((instrument,index)=>({instrument,status:'current',mid:1.1003,quote_epoch:now-3,quote_age_sec:3,spread_pips:1.6,changes:Object.fromEntries(['5m','15m','60m'].map((window,i)=>[window,{return_bps:i+1,price_change_pips:(i+1)*1.1}]))}))},account:{verified_at_utc:new Date((now-2)*1000).toISOString(),positions_current:true,positions:[]},news_sentiment:{fresh:true,generated_utc:new Date((now-2)*1000).toISOString(),active_scored_pair_count:0,active_article_count:23}};
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(String(e)));
 const results=[];
 for(const width of [1440,736,360]){
  await page.goto('about:blank');
  await page.setViewportSize({width,height:1100});
  await page.setContent('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">'+css+'</head><body><main><section id="market-overview"></section></main></body></html>');
  await page.addScriptTag({content:`const $=id=>document.getElementById(id);const num=(v,d=2)=>v==null?'—':Number(v).toFixed(d);const signed=(v,d=2)=>v==null?'—':(Number(v)>=0?'+':'')+Number(v).toFixed(d);const cls=v=>v>0?'good':v<0?'bad':'';const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));Date.now=()=>${now*1000};let lastMainData=${JSON.stringify(data)};${functions}\n${eventLines}\nrenderMarketOverview(lastMainData);`});
  if(await page.locator('.market-table tbody tr').count()!==12)throw Error('Expected default12 '+JSON.stringify(errors)+' actual='+await page.locator('#market-overview').innerHTML());
  await page.locator('button.market-show-all').click();if(await page.locator('.market-table tbody tr').count()!==14)throw Error('All pairs inaccessible');
  await page.locator('#market-pair').selectOption('EUR_AUD');if(await page.locator('.market-table tbody tr').count()!==1)throw Error('Pair selection failed');
  if(!await page.locator('.market-table').getByText('No fresh quote',{exact:true}).first().isVisible())throw Error('Unavailable reason missing');
  await page.locator('#market-pair').selectOption('EUR_USD');await page.locator('button[data-window="60m"]').click();
  if(!await page.getByText('Technical bias (60m)',{exact:true}).isVisible())throw Error('Window mismatch');
  await page.locator('.market-table details summary').click();if(!await page.getByText(/Live move since that reference/).isVisible())throw Error('Forecast detail missing');
  const overflow=await page.evaluate(()=>({body:document.body.scrollWidth,viewport:document.documentElement.clientWidth}));if(overflow.body>overflow.viewport+2)throw Error('Page overflow '+JSON.stringify(overflow));
  await page.screenshot({path:path.join(__dirname,`pair-dashboard-fixture-${width}.png`),fullPage:true});
  results.push({width,all_pairs:14,pair_selection:true,window_controls:true,reference_target_detail:true,page_overflow:false});
 }
 await browser.close();if(errors.length)throw Error(errors.join('\n'));fs.writeFileSync(path.join(__dirname,'DASHBOARD_FIXTURE_BROWSER_VALIDATION.json'),JSON.stringify({status:'passed',scope:'isolated_fixture_actual_renderer_and_handlers',results,page_errors:errors},null,2)+'\n');process.stdout.write(JSON.stringify({status:'passed',layouts:results.length,page_errors:0}));
})().catch(error=>{console.error(error);process.exit(1)});
