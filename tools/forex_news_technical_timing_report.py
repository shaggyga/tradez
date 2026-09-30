"""Corrected read-only diagnostics for explicit event/pair selections."""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'trad'))
import oanda_news_technical_timing_v1 as timing


def epoch(value):
    date = dt.datetime.fromisoformat(value.replace('Z','+00:00'))
    if date.tzinfo is None:
        raise ValueError('timezone_required')
    return date.timestamp()


def build(news_path, technical_path, cases):
    news, tech = timing.readonly(news_path), timing.readonly(technical_path)
    results = []
    try:
        for case in cases:
            r = news.execute('SELECT headline,first_seen_utc,payload_json FROM articles WHERE event_id=?',(case['event_id'],)).fetchone()
            if r is None:
                raise ValueError('missing_event:'+case['event_id'])
            payload = json.loads(r['payload_json'])
            receipt = epoch(r['first_seen_utc'])
            clocks = [receipt]
            for key in ('classification_available_utc','causal_known_utc'):
                if payload.get(key):
                    clocks.append(epoch(payload[key]))
            decision = max(clocks)
            joined = timing.join(tech,case['pair'],news_known=decision,decision=decision)
            if joined['status']=='available':
                fields = joined.pop('features')
                joined['features'] = {k:fields.get(k) for k in (
                    'm1__return_15_pips','m1__return_60_pips','m1__rsi_14',
                    'm1__sma_gap_20_pips','m1__sma_gap_50_pips')}
            results.append({**case,'headline':r['headline'],
                'stored_payload_sha256': hashlib.sha256(r['payload_json'].encode()).hexdigest(),
                'news_first_seen_epoch':receipt, 'classification_delay_seconds':decision-receipt,
                'news_clock_basis':'stored_classification_clock' if payload.get('classification_available_utc') else 'raw_capture_context_only_no_classification_clock',
                'original_directional_publish_eligible':payload.get('directional_publish_eligible'),
                'technical':joined,
                'outcome':timing.evaluate(tech,case['pair'],decision) if joined['status']=='available' else {'status':'not_evaluated_without_available_inputs'}})
        frequency = timing.move_frequency(tech,'EUR_USD',pip=.0001)
    finally:
        news.close();tech.close()
    return {'schema':'news_technical_availability_diagnostic_v1',
        'generated_utc':dt.datetime.now(dt.timezone.utc).isoformat(),
        'source_sha256':hashlib.sha256(Path(timing.__file__).read_bytes()).hexdigest(),
        'news_database':str(Path(news_path).resolve()),'technical_database':str(Path(technical_path).resolve()),
        'forecast_or_performance_claim':False,'events':results,'move_frequency':frequency,
        'limitations':['Selected development cases, not representative forecasting evidence.',
        'Stored classification clocks are not independently re-attested by this diagnostic.',
        'Candle close proxy starts after decision; no fill/slippage assumption is validated.',
        'Original news publication guards still apply; no new forward eligibility is granted.']}


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--news',required=True,type=Path);p.add_argument('--technicals',required=True,type=Path)
    p.add_argument('--cases',required=True,type=Path);p.add_argument('--output',required=True,type=Path)
    args=p.parse_args()
    result=build(args.news,args.technicals,json.loads(args.cases.read_text()))
    with args.output.open('x',encoding='utf-8') as f:json.dump(result,f,indent=2,allow_nan=False)
