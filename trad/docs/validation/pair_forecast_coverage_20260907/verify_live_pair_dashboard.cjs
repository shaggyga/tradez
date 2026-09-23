// Read-only live UI validation: filters only; no worker restart or order action.
const {chromium}=require('C:/Users/zmoor/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const fs=require('fs');const path=require('path');
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(String(e)));
 const apiResponse=await page.request.get('http://127.0.0.1:8765/api/main',{timeout:60000});if(!apiResponse.ok())throw Error('Main API unavailable');
 const data=await apiResponse.json(),coverage=data.pair_local_forecasts||{};
 if(coverage.status!=='current'||Number(coverage.counts?.forecast)<2)throw Error('Expected multiple current pair forecasts: '+JSON.stringify({status:coverage.status,reason:coverage.reason,counts:coverage.counts}));
 if(coverage.rows.length!==68)throw Error('Expected registered68 pair inventory');
 if(coverage.can_place_orders!==false||coverage.can_promote!==false)throw Error('Research authority boundary missing');
 const now=Date.now()/1000;
 const examples=['EUR_USD','USD_HUF'].map(instrument=>{const row=coverage.rows.find(row=>row.instrument===instrument);if(!row)throw Error('Missing example '+instrument);return {instrument,status:row.status,reason:row.reason,reason_label:row.reason_label,observed_epoch:row.observed_epoch,last_attempt_epoch:row.last_attempt_epoch,forecast:row.latest_published_forecast?{publication_epoch:row.latest_published_forecast.publication_epoch,target_epoch:row.latest_published_forecast.target_epoch,forecasts:row.latest_published_forecast.forecasts}:null};});
 for(const row of coverage.rows.filter(row=>row.latest_published_forecast)){const forecast=row.latest_published_forecast;if(!(forecast.publication_epoch<=now&&forecast.target_epoch>now))throw Error('Invalid current target');if(forecast.forecasts.length!==2||forecast.forecasts.some(arm=>arm.target_epoch-arm.reference_epoch!==3600))throw Error('Incorrect H1 timing');}
 const layouts=[];
 for(const width of [1440,360]){
  await page.setViewportSize({width,height:1100});await page.goto('http://127.0.0.1:8765/#oanda',{waitUntil:'domcontentloaded',timeout:60000});
  await page.locator('#market-pair').waitFor({timeout:60000});
  await page.getByText('* p(up) is an uncalibrated model estimate, not verified accuracy.',{exact:false}).first().waitFor();
  await page.locator('#market-pair').selectOption('');
  if((await page.locator('button.market-show-all').innerText()).startsWith('Show all'))await page.locator('button.market-show-all').click();
  await page.waitForFunction(()=>document.querySelectorAll('.market-table tbody tr').length===68);
  for(const instrument of ['EUR_USD','USD_HUF']){
   await page.locator('#market-pair').selectOption(instrument);await page.waitForFunction(()=>document.querySelectorAll('.market-table tbody tr').length===1);
   const rowText=await page.locator('.market-table tbody tr').innerText();if(!rowText.includes(instrument.replace('_',' / ')))throw Error('Selected wrong pair');
   if(/No active model forecast/.test(rowText))throw Error('Generic missing forecast label returned');
   if(instrument==='EUR_USD'&&!rowText.includes('Ridge:'))throw Error('EUR/USD live model absent');
  }
  await page.locator('button[data-window="5m"]').click();await page.getByText('Technical bias (5m)',{exact:true}).waitFor();
  await page.locator('button[data-window="60m"]').click();await page.getByText('Technical bias (60m)',{exact:true}).waitFor();
  await page.locator('#market-pair').selectOption('EUR_USD');
  const details=page.locator('#market-overview details');if(await details.count()){await details.first().locator('summary').click();await details.first().getByText(/Live move since that reference/).waitFor();}
  const positions=await page.locator('#account').innerText();if(!positions.includes('position'))throw Error('Positions status missing');
  const overflow=await page.evaluate(()=>({body:document.body.scrollWidth,viewport:document.documentElement.clientWidth}));if(overflow.body>overflow.viewport+2)throw Error('Page overflow');
  await page.locator('#market-overview').screenshot({path:path.join(__dirname,`live-pair-dashboard-${width}.png`)});
  layouts.push({width,all_pairs:68,selected_pairs:['EUR_USD','USD_HUF'],windows:['5m','60m'],original_reference_target_detail:true,positions_status_visible:true,page_overflow:false});
 }
 await browser.close();if(errors.length)throw Error(errors.join('\n'));
 const receipt={status:'passed',scope:'actual_live_local_dashboard_read_only_filters',observed_epoch:Date.now()/1000,api_observed_epoch:now,registry_sha256:coverage.registry_sha256,summary_sha256:coverage.summary_sha256,counts:coverage.counts,registered_pairs:coverage.rows.length,current_market_pairs:data.market_overview?.current_pair_count,study_orders_enabled:false,examples,layouts,page_errors:errors};
 fs.writeFileSync(path.join(__dirname,'LIVE_PAIR_DASHBOARD_VALIDATION.json'),JSON.stringify(receipt,null,2)+'\n');process.stdout.write(JSON.stringify({status:'passed',counts:coverage.counts,layouts:layouts.length,page_errors:0}));
})().catch(error=>{console.error(error);process.exit(1)});
