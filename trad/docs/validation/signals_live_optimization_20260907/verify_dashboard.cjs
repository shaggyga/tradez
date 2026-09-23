const fs=require('fs');
const path=require('path');
const {chromium}=require('C:/Users/zmoor/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
(async()=>{
 const output=path.join(__dirname,'dashboard_live_qa');fs.mkdirSync(output,{recursive:true});
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 const page=await browser.newPage({viewport:{width:1440,height:1024},deviceScaleFactor:1});
 const errors=[];page.on('pageerror',e=>errors.push(String(e)));
 const response=await page.goto('http://127.0.0.1:8765/#oanda',{waitUntil:'domcontentloaded'});
 await page.getByText('Signals vs live market',{exact:true}).waitFor({timeout:30000});
 await page.waitForFunction(()=>document.querySelectorAll('#market-overview tbody tr').length>0,{},{timeout:15000}).catch(()=>{});
 await page.screenshot({path:path.join(output,'desktop.png'),fullPage:false});
 const desktop=await page.evaluate(()=>({title:document.title,text:document.body.innerText.slice(0,9000),width:innerWidth,scrollWidth:document.documentElement.scrollWidth,openDetails:[...document.querySelectorAll('details[open]')].map(e=>e.querySelector('summary')?.innerText)}));
 await page.setViewportSize({width:390,height:844});
 await page.screenshot({path:path.join(output,'mobile.png'),fullPage:false});
 const mobile=await page.evaluate(()=>({width:innerWidth,scrollWidth:document.documentElement.scrollWidth}));
 const start=performance.now();const api=await (await fetch('http://127.0.0.1:8765/api/main')).json();
 const report={observed_utc:new Date().toISOString(),http_status:response.status(),page_errors:errors,desktop,mobile,
  api_seconds:(performance.now()-start)/1000,collection_status:api.collection_status,
  market:{status:api.market_overview?.status,current_pair_count:api.market_overview?.current_pair_count,technical_pair_count:api.market_overview?.technical_pair_count},
  account:{nav:api.account?.nav,positions:api.account?.positions,positions_current:api.account?.positions_current,pending_orders:api.account?.pending_orders,verified_at_utc:api.account?.verified_at_utc}};
 fs.writeFileSync(path.join(output,'LIVE_DASHBOARD_QA.json'),JSON.stringify(report,null,2));
 console.log(JSON.stringify({http_status:report.http_status,page_errors:errors,desktop:desktop.text.slice(0,2000),mobile,market:report.market,forecasting:api.collection_status?.forecasting}));
 await browser.close();
 if(errors.length||mobile.scrollWidth>mobile.width)process.exitCode=1;
})().catch(e=>{console.error(e);process.exit(1)});
