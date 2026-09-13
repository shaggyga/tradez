const fs=require('fs'),path=require('path'),crypto=require('crypto');
const {chromium}=require('C:/Users/zmoor/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
(async()=>{
 const output=path.join(__dirname,'h1_details_'+new Date().toISOString().replace(/[:.]/g,'_'));fs.mkdirSync(output);
 const context=await chromium.launchPersistentContext(path.join(output,'edge-profile'),{channel:'msedge',headless:true,viewport:{width:1440,height:1024}});
 const errors=[];let report;
 try{
   await context.route('**/*',route=>route.request().method()==='GET'&&new URL(route.request().url()).origin==='http://127.0.0.1:8765'?route.continue():route.abort());
   const page=context.pages()[0];page.on('pageerror',e=>errors.push(String(e)));
   await page.goto('http://127.0.0.1:8765/#oanda',{waitUntil:'domcontentloaded'});
   const summary=page.locator('#market-overview details summary');await summary.waitFor();
   if(!await summary.locator('..').evaluate(e=>e.open))await summary.click();
   await page.screenshot({path:path.join(output,'desktop_h1_details.png')});
   const text=await page.locator('#market-overview tbody tr').first().innerText();
   await page.setViewportSize({width:390,height:844});
   await page.locator('#market-overview .table-wrap').evaluate(e=>e.scrollLeft=e.scrollWidth);
   await page.screenshot({path:path.join(output,'mobile_forecasts_scrolled.png')});
   report={observed_utc:new Date().toISOString(),method:'Additional live screenshot only; expanded H1 details and horizontally scrolled mobile table.',first_row_text:text,page_errors:errors,artifacts:{}};
   for(const name of ['desktop_h1_details.png','mobile_forecasts_scrolled.png'])report.artifacts[name]=crypto.createHash('sha256').update(fs.readFileSync(path.join(output,name))).digest('hex');
 }finally{await context.close();}
 fs.writeFileSync(path.join(output,'receipt.json'),JSON.stringify(report,null,2),{flag:'wx'});console.log(path.join(output,'receipt.json'));
})().catch(error=>{console.error(error);process.exit(1)});
