"""Compact observed-only all-68 market brief; no broker, orders or forecasts."""
from __future__ import annotations
import argparse
from collections import Counter,defaultdict
from datetime import datetime,timezone
import hashlib,json,math,os,sys,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
import oanda_feature_candle_inputs_v2 as candles
import oanda_all68_technical_availability_v1 as availability
import oanda_all68_m1_forward_updater_v2 as metadata
SCHEMA='forex_market_brief_v1_20260916'
STATE=ROOT/'data/oanda_training_manager/state'
CANDLES=ROOT/'data/oanda_training_manager/native_feature_candles_v1'
MAX_BAR_AGE=600
MAX_QUOTE_AGE=120
TAIL_ROWS=256


def utc(epoch):return datetime.fromtimestamp(epoch,timezone.utc).isoformat()
def clean(value):
    if isinstance(value,float):return round(value,5) if math.isfinite(value) else None
    if isinstance(value,dict):return {key:clean(item) for key,item in value.items()}
    if isinstance(value,list):return [clean(item) for item in value]
    return value


def technical(rows,*,now,anchor):
    """Exact elapsed return windows and gap-free completed M5 indicators."""
    if not math.isfinite(now) or not math.isfinite(anchor) or anchor%300 or anchor>now:
        raise ValueError('completed_aligned_analysis_clock_required')
    usable=[];last=None
    for row in rows:
        at=candles.stamp(row['time']).timestamp()
        # Future rows have no influence on historical calculations or validation.
        if at+300>min(now,anchor):continue
        candles.validate_candle(row,300,datetime.fromtimestamp(now,timezone.utc))
        if last is not None and at<=last:raise ValueError('duplicate_or_unsorted_M5')
        last=at;usable.append(row)
    usable=usable[-TAIL_ROWS:]
    if not usable or candles.stamp(usable[-1]['time']).timestamp()+300!=anchor:
        return {'status':'missing_analysis_endpoint','observed_end_utc':None,'returns_bps':{'15m':None,'60m':None},'indicator_rows':0}
    suffix=[usable[-1]]
    for row in reversed(usable[:-1]):
        if candles.stamp(row['time']).timestamp()+300!=candles.stamp(suffix[-1]['time']).timestamp():break
        suffix.append(row)
    suffix.reverse();n=len(suffix);prices=[row['mid']['c'] for row in suffix]
    returns={f'{minutes}m':(prices[-1]/prices[-1-minutes//5]-1)*10000 if n>minutes//5 else None for minutes in (15,60)}
    result={'status':'current' if 0<=now-anchor<=MAX_BAR_AGE else 'stale','observed_end_utc':utc(anchor),'bar_age_seconds':now-anchor,
        'returns_bps':returns,'indicator_rows':n,'ema20':None,'ema50':None,'trend':'unavailable','rsi14':None,'atr14_bps':None,'close':prices[-1]}
    def ema(period):
        if n<period:return None
        value=sum(prices[:period])/period;alpha=2/(period+1)
        for price in prices[period:]:value=alpha*price+(1-alpha)*value
        return value
    result['ema20'],result['ema50']=ema(20),ema(50)
    if result['ema50'] is not None:
        result['trend']='up' if prices[-1]>result['ema20']>result['ema50'] else 'down' if prices[-1]<result['ema20']<result['ema50'] else 'mixed'
    if n>=15:
        changes=[b-a for a,b in zip(prices,prices[1:])]
        gains=[max(change,0) for change in changes];losses=[max(-change,0) for change in changes]
        ranges=[max(row['mid']['h']-row['mid']['l'],abs(row['mid']['h']-prior),abs(row['mid']['l']-prior)) for row,prior in zip(suffix[1:],prices[:-1])]
        def wilder(values):
            value=sum(values[:14])/14
            for item in values[14:]:value=(13*value+item)/14
            return value
        gain,loss=wilder(gains),wilder(losses)
        result['rsi14']=50. if gain==loss==0 else 100. if loss==0 else 100.-100./(1.+gain/loss)
        result['atr14_bps']=wilder(ranges)/prices[-1]*10000
    return result


def quote(pair,snapshot,heartbeat,now):
    result=availability._quote(pair,snapshot,heartbeat,now,MAX_QUOTE_AGE)
    safe={key:result.get(key) for key in ('status','tradeable','age_seconds','connected','current_connection','reason')}
    safe.update(mid=None,spread_bps=None)
    try:
        raw=snapshot['quotes'][pair];bid=float(raw['bid']);ask=float(raw['ask'])
        if not (math.isfinite(bid) and math.isfinite(ask) and 0<bid<=ask):raise ValueError()
        safe.update(mid=(bid+ask)/2,spread_bps=(ask-bid)/((ask+bid)/2)*10000)
    except (KeyError,ValueError,TypeError):pass
    return safe


def build(pairs,histories,quotes,heartbeat,*,now,source_errors=None,receipts=None):
    if len(pairs)!=68 or len(set(pairs))!=68:raise ValueError('registered_unique_68_required')
    eligible=[]
    for rows in histories.values():
        ends=[candles.stamp(row['time']).timestamp()+300 for row in rows if candles.stamp(row['time']).timestamp()+300<=now]
        eligible.extend(set(end for end in ends if 0<=now-end<=MAX_BAR_AGE))
    # One shared original UTC boundary, selected from current native data.
    counts=Counter(eligible)
    anchor=max(counts,key=lambda at:(counts[at],at)) if counts else math.floor(now/300)*300
    records={}
    for pair in sorted(pairs):
        rows=histories.get(pair,[])
        technicals=technical(rows,now=now,anchor=anchor)
        technicals['quote']=quote(pair,quotes,heartbeat,now)
        technicals['source_error']=(source_errors or {}).get(pair)
        technicals['latest_native_end_utc']=max((utc(candles.stamp(row['time']).timestamp()+300) for row in rows),default=None)
        records[pair]=technicals
    strength={}
    for horizon in ('15m','60m'):
        currencies=defaultdict(list)
        for pair,record in records.items():
            move=record['returns_bps'][horizon]
            if record['status']!='current' or move is None:continue
            base,counter=pair.split('_');currencies[base].append(move);currencies[counter].append(-move)
        strength[horizon]=sorted([{'currency':ccy,'mean_cross_return_bps':sum(values)/len(values),'crosses':len(values),'positive_crosses':sum(v>0 for v in values)} for ccy,values in currencies.items()],key=lambda r:r['mean_cross_return_bps'],reverse=True)
    coverage={'registered_pairs':len(pairs),'native_histories_read':len(histories),'shared_endpoint_pairs':sum(r['status']=='current' for r in records.values()),
        'return15_pairs':sum(r['status']=='current' and r['returns_bps']['15m'] is not None for r in records.values()),'return60_pairs':sum(r['status']=='current' and r['returns_bps']['60m'] is not None for r in records.values()),
        'ema50_pairs':sum(r['status']=='current' and r.get('ema50') is not None for r in records.values()),'rsi_atr14_pairs':sum(r['status']=='current' and r.get('rsi14') is not None for r in records.values()),
        'current_tradeable_quotes':sum(r['quote']['status']=='current' for r in records.values()),'source_errors':len(source_errors or {})}
    full={'schema':SCHEMA,'observed_utc':utc(now),'analysis_end_utc':utc(anchor),'analysis_age_seconds':now-anchor,'coverage':coverage,'pairs':records,'currency_strength':strength,
        'positions':{'status':'unverified','reason':'no explicitly verified fresh account-position cache selected'},
        'scope':'Observed completed native M5 only; EMA/RSI/ATR describe history, not forecasts or entry instructions. No filling, model scoring, account query or order operation.',
        'definitions':{'returns':'Mid-close change between exact elapsed UTC endpoints with every intermediate M5 bar present. bps=percent*100.',
            'indicators':'Gap-free suffix of at most256 native M5 bars. EMA20/50 simple-average seed; RSI14 and ATR14 Wilder recursion; ATR expressed in bps of final close.',
            'currency_strength':'Equal-weight signed incident-pair observed returns at the shared clock; cross counts provided; correlated/uneven currency coverage, not independent signals.',
            'freshness_seconds':{'completed_bar_max':MAX_BAR_AGE,'quote_max':MAX_QUOTE_AGE},'tail_reader':'Existing adapter bounded512KiB/1024rawrows per pair; last256 supported rows used.'},'source_receipts':receipts or {}}
    return clean(full)


def compact(full):
    def item(pair):
        r=full['pairs'][pair];q=r['quote']
        return {'pair':pair,'15m_bps':r['returns_bps']['15m'],'60m_bps':r['returns_bps']['60m'],'trend':r.get('trend'),'RSI14':r.get('rsi14'),'ATR14_bps':r.get('atr14_bps'),'spread_bps':q['spread_bps'],'quote':q['status'],'tradeable':q['tradeable'],'quote_age_s':q['age_seconds'],'bar':r['status']}
    ranked=[p for p,r in full['pairs'].items() if r['status']=='current' and r['returns_bps']['60m'] is not None]
    ups=sorted((p for p in ranked if full['pairs'][p]['returns_bps']['60m']>0),key=lambda p:full['pairs'][p]['returns_bps']['60m'],reverse=True)[:6]
    downs=sorted((p for p in ranked if full['pairs'][p]['returns_bps']['60m']<0),key=lambda p:full['pairs'][p]['returns_bps']['60m'])[:6]
    strength_item=lambda r:[r['currency'],r['mean_cross_return_bps'],r['crosses'],r['positive_crosses']]
    strengths={h:{'strongest':list(map(strength_item,v[:3])),'weakest':list(map(strength_item,v[-3:]))} for h,v in full['currency_strength'].items()}
    columns=['pair','15m_bps','60m_bps','trend','RSI14','ATR14_bps','spread_bps','quote','tradeable','quote_age_s','bar']
    table=lambda pair:[item(pair)[key] for key in columns]
    return {'observed_utc':full['observed_utc'],'analysis_end_utc':full['analysis_end_utc'],'analysis_age_s':full['analysis_age_seconds'],'coverage':full['coverage'],'EUR_USD':item('EUR_USD') if 'EUR_USD' in full['pairs'] else None,'mover_columns':columns,'top_up':list(map(table,ups)),'top_down':list(map(table,downs)),'currency_strength_columns':['currency','mean_cross_return_bps','crosses','positive_crosses'],'currency_strength':strengths,'positions':full['positions'],'scope':'Observed M5 history, not predictions; no fill or orders. Spread is current quote, returns are mid-price and exclude costs.'}


def capture(*,now=None,candle_root=CANDLES):
    supplied_now=now
    now=time.time() if now is None else now;pairs=sorted(metadata.verified_pip_sizes());histories={};errors={};receipts={}
    for pair in pairs:
        try:
            rows,receipt=candles.read_tail(candle_root/f'{pair}_M5.csv',pair,'M5',observed_utc=utc(now),
                clock=(lambda:utc(time.time())) if supplied_now is None else None)
            histories[pair]=rows[-TAIL_ROWS:];receipts[pair]=receipt
        except (OSError,ValueError,KeyError,TypeError) as exc:errors[pair]=type(exc).__name__+':'+str(exc)[:120]
    def small(path):
        try:return availability.read_json(path,4*1024**2)[0]
        except (OSError,ValueError,TypeError):return None
    quotes,heartbeat=small(availability.DEFAULT_QUOTES),small(availability.DEFAULT_HEARTBEAT)
    # Capture the report clock after reading independently advancing live files.
    report_now=time.time() if supplied_now is None else supplied_now
    return build(pairs,histories,quotes,heartbeat,now=report_now,source_errors=errors,receipts=receipts)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--once',action='store_true',help='One read-only capture (also the default).');parser.add_argument('--output-root',type=Path,default=ROOT/'data/forex_market_briefs_v1')
    args=parser.parse_args(argv);full=capture();out=args.output_root.absolute()
    if any(p.is_symlink() or p.is_junction() for p in (out,*out.parents)):raise ValueError('output_reparse_refused')
    out.mkdir(parents=True,exist_ok=True)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ');path=out/(stamp+'.json');raw=json.dumps(full,sort_keys=True,allow_nan=False,indent=2).encode()
    with path.open('xb') as f:f.write(raw)
    latest=out/'latest.json';temp=out/f'.latest.{os.getpid()}.tmp'
    with temp.open('xb') as f:f.write(raw)
    os.replace(temp,latest)
    brief=compact(full);brief['full_report']=str(path);brief['sha256']=hashlib.sha256(raw).hexdigest()
    print(json.dumps(brief,separators=(',',':'),allow_nan=False))
    return 0


if __name__=='__main__':raise SystemExit(main())
