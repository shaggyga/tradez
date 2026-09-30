"""Prespecified descriptive archive batch; no fitting or live prediction claims."""
import argparse
from collections import Counter, defaultdict
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_news_interpretation_v1 as interpretation
import oanda_news_technical_timing_v1 as timing


def sign(x):
    return 1 if x>0 else -1 if x<0 else 0


def policy_direction(headline, pair):
    by_currency=defaultdict(set)
    for c in interpretation.interpret_headline(headline)['claims']:
        if c['kind'] in ('policy_stance','policy_stance_change','policy_rate_action') and c['direction'] in (-1,1):
            by_currency[c['currency']].add(c['direction'])
    base,quote=pair.split('_')
    if any(len(by_currency[x])>1 for x in (base,quote)):
        return 0
    return sign(sum(by_currency[base])-sum(by_currency[quote]))


def stamp(value):
    d=dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('timezone_required')
    return d.timestamp()


def summarize(rows):
    groups=defaultdict(list)
    for r in rows:
        for horizon,y in r['outcomes'].items():
            if y.get('status')!='available':continue
            for arm,direction in [('technical',r['technical_direction']),('original_news',r['original_direction']),('new_policy_interpretation',r['new_direction'])]:
                if not direction:continue
                groups[(horizon,arm,'all')].append((direction,y))
                if r['new_direction'] and r['technical_direction']:
                    relation='agree' if r['new_direction']==r['technical_direction'] else 'disagree'
                    groups[(horizon,arm,relation)].append((direction,y))
                if r['original_publish_eligible']:
                    groups[(horizon,arm,'original_article_publish_eligible')].append((direction,y))
                if r['original_direction'] and r['new_direction'] and r['technical_direction']:
                    groups[(horizon,arm,'matched_original_new_technical')].append((direction,y))
    return [{'horizon_minutes':int(h),'arm':a,'stratum':s,'n':len(v),
             'direction_hit_rate':sum(sign(y['return_bps'])==d for d,y in v)/len(v),
             'mean_net_bps':sum(y['long_net_bps'] if d>0 else y['short_net_bps'] for d,y in v)/len(v),
             'after_spread_win_rate':sum((y['long_net_bps'] if d>0 else y['short_net_bps'])>0 for d,y in v)/len(v)}
            for (h,a,s),v in sorted(groups.items())]


def run(news_path,technical_path,contract):
    news,tech=timing.readonly(news_path),timing.readonly(technical_path)
    reasons=Counter();selected=[];headlines=set();buckets=set();identities=[]
    try:
        news.execute('BEGIN');tech.execute('BEGIN')
        records=news.execute('''SELECT event_id,headline,first_seen_utc,payload_json FROM articles
          WHERE first_seen_utc>=? AND first_seen_utc<? ORDER BY first_seen_utc,event_id''',
          (contract['start'],contract['end_exclusive'])).fetchall()
        candidates=[]
        for r in records:
            key=re.sub(r'\s+-\s+[^-]+$','',r['headline']).casefold().strip()
            if key in headlines:reasons['duplicate_headline']+=1;continue
            headlines.add(key);p=json.loads(r['payload_json'])
            identities.append((r['event_id'],hashlib.sha256(r['payload_json'].encode()).hexdigest()))
            if not p.get('classification_available_utc'):
                reasons['missing_classification_clock']+=1;continue
            try:clock=max(stamp(r['first_seen_utc']),stamp(p['classification_available_utc']),stamp(p.get('causal_known_utc') or r['first_seen_utc']))
            except (ValueError,TypeError):reasons['invalid_news_clock']+=1;continue
            if not stamp(contract['start']+'T00:00:00+00:00')<=clock<stamp(contract['end_exclusive']+'T00:00:00+00:00'):
                reasons['classification_outside_window']+=1;continue
            for pair in contract['pairs']:
                base,quote=pair.split('_');scores=p.get('currency_scores') or {}
                old=sign(float(scores.get(base,0))-float(scores.get(quote,0)))
                new=policy_direction(r['headline'],pair)
                if not old and not new:reasons['no_direction_pair']+=1;continue
                candidates.append((clock,r['event_id'],pair,r,p,old,new))
        for clock,eid,pair,r,p,old,new in sorted(candidates,key=lambda x:x[:3]):
            bucket=(pair,int(clock//900))
            if bucket in buckets:reasons['pair_time_duplicate']+=1;continue
            # Select without looking at technical sign or future returns.
            buckets.add(bucket)
            j=timing.join(tech,pair,news_known=clock,decision=clock)
            if j['status']!='available':reasons[j['reason']]+=1;continue
            values=j['features'];momentum=values.get('m1__return_15_pips')
            if momentum is None or not math.isfinite(momentum):reasons['missing_technical_momentum']+=1;continue
            y=timing.evaluate(tech,pair,clock,tuple(contract['horizons']))
            if 'horizons' not in y:reasons[y['reason']]+=1;continue
            selected.append({'event_id':eid,'headline':r['headline'],'pair':pair,'decision_epoch':clock,
                'decision_utc':dt.datetime.fromtimestamp(clock,dt.timezone.utc).isoformat(),
                'feature_hash':j['feature_hash'],'feature_age_seconds':j['age_seconds'],
                'original_direction':old,'new_direction':new,'technical_direction':sign(momentum),
                'original_publish_eligible':bool(p.get('directional_publish_eligible')),
                'technical_15m_pips':momentum,'rsi14':values.get('m1__rsi_14'),
                'entry_epoch':y['entry_bar_close_epoch'],'outcomes':y['horizons']})
    finally:news.close();tech.close()
    return {'scope':'retrospective_diagnostic_not_actual_predictions_or_holdout',
        'record_count':len(records),'distinct_headlines':len(headlines),'input_payload_identities':identities,
        'exclusions':dict(reasons),'selected_pair_time_rows':len(selected),
        'distinct_selected_events':len({r['event_id'] for r in selected}),
        'article_publish_eligible_rows':sum(r['original_publish_eligible'] for r in selected),
        'metrics':summarize(selected),'rows':selected,
        'limitations':['New interpretation applied today; timing uses stored classification availability, not first immutable version proof.',
        'Technical rule is prior15minute momentum, not the trained technical model.',
        'All-stratum arms have different participation; agree/disagree strata share news/technical support.',
        'Pair/time dedup reduces duplicates, but syndicated paraphrases and cross-pair/horizon dependence remain.',
        'No slippage/financing or executable fill claim; no strategy or trading promotion.']}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('news','technicals','contract','output'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();contract=json.loads(a.contract.read_text())
    result=run(a.news,a.technicals,contract)
    result.update(contract=contract,generated_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                  source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
    with a.output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2,allow_nan=False)
