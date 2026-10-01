// Exercise the actual page function with a deterministic request clock.
const fs = require('fs');
const vm = require('vm');
const assert = require('assert/strict');
const html = fs.readFileSync('trad/oanda_main_signal_dashboard.html', 'utf8');
const source = html.match(/async function fetchMainState\(\)\{[\s\S]*?\n    \}/)[0];
async function scenario(ms, status=200) {
  let timeout, cleared=false, aborted=false;
  const context={
    AbortController:class {constructor(){this.signal={};} abort(){aborted=true;}},
    setTimeout: (fn, delay) => {timeout={fn,delay}; return 1;},
    clearTimeout: () => {cleared=true;},
    fetch: async (path, options) => {
      assert.equal(path,'/api/main');assert.equal(options.cache,'no-store');
      if(ms>=timeout.delay) timeout.fn();
      if(aborted) throw new Error('AbortError');
      return {ok:status===200,status,json:async()=>({status:'current',forecasts:960})};
    }
  };
  vm.createContext(context);vm.runInContext(source,context);
  let result,error;try{result=await context.fetchMainState();}catch(e){error=e.message;}
  assert.equal(cleared,true);assert.equal(timeout.delay,30000);
  return {result,error,aborted};
}
(async()=>{
  const slow=await scenario(13656);assert.equal(slow.result.forecasts,960);assert.equal(slow.aborted,false);
  const hung=await scenario(31000);assert.equal(hung.error,'AbortError');assert.equal(hung.aborted,true);
  const failure=await scenario(100,503);assert.equal(failure.error,'HTTP 503');
  console.log('3 dashboard request-budget cases passed; actual page function, preserved rejection/cleanup.');
})().catch(e=>{console.error(e);process.exitCode=1;});
