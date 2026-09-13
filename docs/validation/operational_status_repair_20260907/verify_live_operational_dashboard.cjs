// Read-only browser validation after the dashboard-only reload.
const {chromium}=require('C:/Users/zmoor/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright');
const fs=require('fs'),path=require('path');
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'}),page=await browser.newPage(),errors=[],layouts=[];
 page.on('pageerror',e=>errors.push(String(e)));
 for(const width of [1440,360]){
  await page.setViewportSize({width,height:1100});
  await page.goto('http://127.0.0.1:8765/#oanda',{waitUntil:'domcontentloaded',timeout:60000});
  await page.locator('#market-pair').waitFor({timeout:60000});
  await page.waitForFunction(()=>document.querySelector('#status').textContent.toLowerCase().includes('partially operational'));
  await page.locator('#market-pair').selectOption('EUR_USD');
  await page.waitForFunction(()=>document.querySelectorAll('.market-table tbody tr').length===1);
  const rowText=await page.locator('.market-table tbody tr').innerText();
  if(rowText.includes('No current pair summary')||rowText.includes('articles screened'))throw Error('Old unscoped news label in pair row');
  if(!/Context only|News evidence not current|No current news signal|Mixed or weak current evidence|Long|Short/.test(rowText))throw Error('Missing explicit news classification');
  const bot=await page.locator('#collection-status').innerText();
  if(!/pairs|pair/i.test(bot)||!/orders disabled/i.test(bot))throw Error('Missing pair coverage/order state');
  const legacy=page.locator('details[data-state-key="companion-study-details"]');
  if(!await legacy.count())throw Error('Companion study not separately identified');
  if(!await legacy.evaluate(element=>element.open))await legacy.locator(':scope > summary').click();
  await page.getByText(/Supplemental EUR\/USD/).first().waitFor();
  const details=await legacy.innerText();
  if(!/duplicate_market_reference_epoch|duplicate reference/i.test(details))throw Error('Original scoring failure not visible');
  if(!/supplemental/i.test(details)||!/scored/i.test(details))throw Error('Supplemental score scope missing');
  const overflow=await page.evaluate(()=>({body:document.body.scrollWidth,viewport:document.documentElement.clientWidth}));
  if(overflow.body>overflow.viewport+2)throw Error('Page overflow');
  await page.screenshot({path:path.join(__dirname,`operational-dashboard-${width}.png`),fullPage:false});
  if(width===1440)await legacy.screenshot({path:path.join(__dirname,'operational-companion-details.png')});
  layouts.push({width,status:await page.locator('#status').innerText(),eurusd_row:rowText,companion_details:details,page_overflow:false});
 }
 await browser.close();if(errors.length)throw Error(errors.join('\n'));
 const receipt={status:'passed',observed_utc:new Date().toISOString(),scope:'actual_local_dashboard_read_only_pair_filter_and_details',layouts,page_errors:errors};
 fs.writeFileSync(path.join(__dirname,'LIVE_OPERATIONAL_DASHBOARD_VALIDATION.json'),JSON.stringify(receipt,null,2)+'\n',{flag:'wx'});
 process.stdout.write(JSON.stringify({status:'passed',layouts:layouts.length,page_errors:errors.length}));
})().catch(e=>{console.error(e);process.exit(1)});
