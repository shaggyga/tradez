from pathlib import Path

path=Path(r'C:\Users\zmoor\Documents\forex\trad\oanda_main_signal_dashboard.html')
text=path.read_text(encoding='utf-8')
def replace(old,new):
    global text
    assert text.count(old)==1,(old[:90],text.count(old))
    text=text.replace(old,new)

replace('    function renderMarketOverview(data){', '''    function availableForecastCoverage(data){
      const joint=jointForecastActivity(data),source=data.pair_local_forecasts||{},now=Date.now()/1000,age=now-Number(source.generated_epoch),rows=Array.isArray(source.rows)&&source.rows.length<=68?source.rows:[],priceForecasts=new Map();
      const priceCurrent=source.study_version===2&&source.status==='current'&&source.research_only===true&&source.can_place_orders===false&&source.can_promote===false&&age>=0&&age<=90;
      if(priceCurrent)for(const row of rows){const arms=marketPairForecasts(row,true,now,2).filter(arm=>['probabilistic_state_space','ridge_return_repaired'].includes(arm.family));if(arms.length)priceForecasts.set(row.instrument,arms);}
      const instruments=new Set([...joint.states.keys(),...rows.map(row=>row.instrument)]),priceOnly=[...priceForecasts.keys()].filter(pair=>!joint.forecasts.has(pair)).length;
      const result={joint,priceForecasts,total:instruments.size,combined:joint.forecast,priceOnly,unavailable:instruments.size-joint.forecast-priceOnly};
      result.label=`${result.combined} combined · ${result.priceOnly} price-only · ${result.unavailable} unavailable`;
      return result;
    }
    function renderMarketOverview(data){''')
replace('      const joint=jointForecastActivity(data);\n      const pairStates=', '      const available=availableForecastCoverage(data),joint=available.joint;\n      const pairStates=')
replace("const forecastCell=row=>{const forecast=forecastByPair.get(row.instrument)","const forecastCell=(row,inline=false)=>{const forecast=forecastByPair.get(row.instrument)")
replace('<div class="subvalue">Expected move ${signed(arm.predicted_return_bps,2)} bps · before costs</div></div>`).join(\'\'),waiting=', '<div class="subvalue">Expected move ${signed(arm.predicted_return_bps,2)} bps · before costs</div>${inline?`<div class="subvalue">Issued ${esc(at(arm.issued_epoch))} · H1 target ${esc(at(arm.target_epoch))}</div>`:\'\'}</div>`).join(\'\'),waiting=')
replace('      const body=rows.map(row=>{const priceCurrent=currentByPair.has(row.instrument),move=change(row)', '''      const displayForecastCell=(row,priceCurrent)=>{const combined=jointForecastCell(joint,row,priceCurrent,now,at),priceOnly=available.priceForecasts.has(row.instrument)&&!joint.forecasts.has(row.instrument);return priceOnly?`<div class="price-only-forecast"><strong>Price-only forecast · news not included</strong>${forecastCell(row,true)}</div><div class="joint-blocker overview-note"><strong>Combined model:</strong> ${combined}</div>`:`${combined}<details class="compact-details" data-state-key="price-only-models-${esc(row.instrument)}"><summary>Other price-only models</summary><div class="detail-body">${forecastCell(row)}</div></details>`;};
      const body=rows.map(row=>{const priceCurrent=currentByPair.has(row.instrument),move=change(row)''')
replace('${jointForecastCell(joint,row,priceCurrent,now,at)}<details class="compact-details" data-state-key="price-only-models-${esc(row.instrument)}"><summary>Other price-only models</summary><div class="detail-body">${forecastCell(row)}</div></details></td>', '${displayForecastCell(row,priceCurrent)}</td>')
replace("${joint.current?`${joint.forecast}/${joint.total} combined forecasts`:'Combined forecast coverage unavailable'}</strong><span", "${joint.current?available.label:`Combined forecast coverage unavailable · ${available.priceOnly} price-only · ${available.unavailable} unavailable`}</strong><span")
replace('<th>Combined forecast</th>','<th>Forecast</th>')
replace('* p(up) is an uncalibrated model estimate, not verified accuracy. Technical and news biases are descriptive.', 'Available price-only models are shown when the combined model is unavailable; they do not use news. * p(up) is an uncalibrated model estimate, not verified accuracy. Technical and news biases are descriptive.')
replace("if(joint.current)return{className:'dot collection',text:`Partially operational · ${joint.forecast}/${joint.total} combined forecasts · research only`};", "if(joint.current)return{className:'dot collection',text:`Partially operational · ${availableForecastCoverage(data).label} · research only`};")
replace('${joint.total-joint.forecast} pairs awaiting inputs, quotes or next forecast · ${Number(counts.publication||0)} published', '${availableForecastCoverage(data).label} · ${Number(counts.publication||0)} combined forecasts published')
path.write_text(text,encoding='utf-8',newline='\r\n')
