"""One bounded foreground observation session; never sends broker requests."""
import collections
import json
import math
from pathlib import Path
import time
from urllib.request import urlopen

OUT = Path(__file__).resolve().parent
END = 1788813373.0  # 2026-09-07 20:36:13 UTC, requested hour ends here.
INTERVAL = 30.0


def utc(epoch):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(epoch))


def selected(value, names):
    value = value if isinstance(value, dict) else {}
    return {name: value.get(name) for name in names.split()}


def observe():
    before = time.time()
    with urlopen('http://127.0.0.1:8765/api/main', timeout=12) as response:
        data = json.load(response)
    now = time.time()
    pairs = data.get('pair_local_forecasts') or {}
    market = data.get('market_overview') or {}
    news = data.get('news_sentiment') or {}
    collection = data.get('collection_status') or {}
    account = data.get('account') or {}
    diagnostic = data.get('eurusd_supplemental_diagnostic') or {}
    rows = pairs.get('rows') or []
    reasons = collections.Counter(row.get('reason') or row.get('status') for row in rows)
    signals = []
    for row in rows:
        forecast = row.get('latest_published_forecast')
        if isinstance(forecast, dict):
            signals.append({'instrument': row.get('instrument'), 'status': row.get('status'),
                            **selected(forecast, 'decision_id publication_epoch target_epoch status forecasts')})
    observations = collection.get('observations') or {}
    alerts = []
    if pairs.get('status') != 'current':
        alerts.append('pair_summary:' + str(pairs.get('status')) + ':' + str(pairs.get('reason')))
    if pairs.get('can_place_orders') is not False or pairs.get('can_promote') is not False:
        alerts.append('unexpected_pair_authority_flags')
    for kind, observation in observations.items():
        if observation.get('current') is not True:
            alerts.append('collection_not_current:' + kind)
    if account.get('positions_current') is not True or account.get('orders_current') is not True:
        alerts.append('account_positions_or_orders_not_current')
    if account.get('open_trades') != 0 or account.get('pending_orders') != 0:
        alerts.append('account_activity_or_unknown_count')
    if diagnostic.get('status') != 'available' or diagnostic.get('freshness') != 'current':
        alerts.append('supplemental_diagnostic_unavailable_or_stale')
    if diagnostic.get('capacity_state') not in ('within_limits', None):
        alerts.append('supplemental_capacity:' + str(diagnostic.get('capacity_state')))
    return {
        'event': 'live_sample', 'observed_epoch': now, 'observed_utc': utc(now),
        'api_response_sec': round(now-before, 3), 'alerts': alerts,
        'pair_summary': selected(pairs, 'status reason counts generated_epoch summary_age_sec registry_sha256 summary_sha256 retained_generation publication_read_attempts publication_read_failures can_place_orders can_promote'),
        'pair_reasons': dict(reasons), 'pair_readiness': [selected(row, 'instrument status reason current_common_bars required_current_common_bars last_attempt_epoch') for row in rows],
        'forecasts': signals,
        'collection': selected(collection, 'status observations forecasting warmup model_attempts registered_study_trading_enabled'),
        'account': selected(account, 'environment snapshot_state snapshot_age_sec account_values_current positions_current orders_current open_trades pending_orders balance nav realized_pl unrealized_pl'),
        'news': selected(news, 'pair_bias_status pair_bias_reason evidence_age_sec active_topic_count direction_counts fresh active_scored_pair_count'),
        'market': selected(market, 'status reason current_pair_count quoted_pair_count technical_pair_count rows'),
        'supplemental_eurusd': selected(diagnostic, 'status generated_utc report_age_sec freshness capacity_state original_decision_count excluded_decision_count retained_decision_count paired_scored_decisions paired_summaries original_scorer'),
    }


def main():
    target = OUT / 'live_samples.jsonl'
    summary_target = OUT / 'live_session_result.json'
    if target.exists() or summary_target.exists():
        raise RuntimeError('observation_targets_already_exist')
    first = time.time()
    count = failures = 0
    next_sample = first
    with target.open('x', encoding='utf-8') as handle:
        while True:
            now = time.time()
            if now < next_sample:
                time.sleep(min(next_sample-now, 1.0))
                continue
            try:
                row = observe()
                pairs = row['pair_summary']
                concise = {'utc': row['observed_utc'], 'pairs': pairs.get('counts'),
                    'market_pairs': row['market'].get('current_pair_count'),
                    'news': row['news'].get('pair_bias_status'),
                    'news_age': row['news'].get('evidence_age_sec'),
                    'eur_scored': row['supplemental_eurusd'].get('paired_scored_decisions'),
                    'alerts': row['alerts']}
            except Exception as exc:
                failures += 1
                row = {'event':'sample_error', 'observed_epoch':time.time(),
                       'observed_utc':utc(time.time()), 'error_type':type(exc).__name__,
                       'error':str(exc)[:300]}
                concise = row
            handle.write(json.dumps(row, separators=(',', ':'), allow_nan=False)+'\n')
            handle.flush()
            count += 1
            print(json.dumps(concise, separators=(',', ':')), flush=True)
            if time.time() >= END:
                break
            next_sample = min(END, next_sample + INTERVAL)
            if next_sample < time.time():
                next_sample = min(END, time.time() + INTERVAL)
    result = {'status':'completed', 'first_persisted_sample_utc':utc(first),
              'finished_utc':utc(time.time()), 'requested_window_start_utc':'2026-09-07T19:36:13Z',
              'requested_window_end_utc':utc(END), 'persisted_samples':count,
              'api_failures':failures, 'interval_sec':INTERVAL,
              'earlier_live_checks':'Initial live API probes and parallel baseline inspections preceded this persisted sampling loop.',
              'orders_sent':0, 'processes_restarted':0, 'model_or_ledger_writes':0}
    summary_target.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,separators=(',', ':')),flush=True)


if __name__ == '__main__':
    main()
