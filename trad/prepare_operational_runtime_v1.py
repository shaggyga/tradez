"""Prepare the explicit research service profile. Does not start any service."""
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parent
DATA=ROOT/'data/oanda_training_manager'
STATE=DATA/'state'
RUN=DATA/'operational_repair_20260913_v1'
NEWS=DATA/'operational_repair_20260913_v3'
COLLECTOR=DATA/'market_open_20260913_v1'
CONFIG=ROOT/'config'


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def make():
    services=[]
    def add(name,script,args,heart,schema,age=120,grace=180,needle=None):
        services.append(dict(name=name,script=script,source_sha256=sha(ROOT/script),
            arguments=[str(v) for v in args],heartbeat=str(heart),heartbeat_schema=schema,
            max_age_sec=age,startup_grace_sec=grace,needle=needle or script))
    add('revision_news_collector_v2','oanda_local_news_sentiment.py',[
        '--config',CONFIG/'news_sources_v1.json','--output-root',COLLECTOR/'local_news_sentiment',
        '--ledger',COLLECTOR/'news_watch_ledger.csv','--event-root',COLLECTOR/'news_event_tags',
        '--state',COLLECTOR/'state/news_collector_state.json','--clock-integrity-state',STATE/'clock_integrity_v1.json',
        '--coverage-root',COLLECTOR/'reports','--interval-sec','60','--duration-sec','604800','--no-refresh-event-catalog'],
        COLLECTOR/'local_news_sentiment/collector_heartbeat_v1.json','local_fx_news_sentiment_v3',120,180,
        'oanda_local_news_sentiment.py*'+str(COLLECTOR/'local_news_sentiment'))
    add('local_news_sentiment_repair_v2','oanda_local_news_sentiment_repair_v2.py',[
        '--data-root',DATA,'--interval-sec','15','--duration-sec','604800'],
        DATA/'local_news_sentiment_repair_v2/heartbeat_v2.json','repaired_joint_news_heartbeat_v2_20260912_source_derived_clocks')
    transport=CONFIG/'revision_transport_operational_v3_20260913.json'
    add('revision_news_transport_v4','revision_transport_v4.py',['--config',transport,'--config-sha256',sha(transport)],
        NEWS/'revision_news_v1/transport_state/status.json','revision_transport_status_v1_20260913',150,300)
    for name,script,config,study,schema in [
        ('joint_price_news_study_v7','oanda_joint_price_news_forecast_study_v7.py','joint_price_news_operational_v3_20260913.json',NEWS/'joint_price_news_study_v7','joint_price_news_forecast_heartbeat_v7_20260913'),
        ('pair_local_forecast_study_v3','oanda_pair_local_forecast_study_v3.py','pair_local_operational_v2_20260913.json',RUN/'pair_local_forecast_study_v3','pair_local_forecast_heartbeat_v3_20260913')]:
        args=['--config',CONFIG/config,'--study',study,'--candle-root',DATA/'candles','--quote-path',STATE/'practice_007_market_quotes_v1.json','--duration-sec','604800']
        if name=='joint_price_news_study_v7':args+=['--news-io-config',CONFIG/'revision_news_io_operational_v3_20260913.json']
        add(name,script,args,study/'heartbeat.json',schema,120,300)
    add('retained_price_settlement_v1','oanda_retained_price_settlement_v1.py',['--duration-sec','604800'],
        STATE/'retained_price_settlement_v1.json','retained_price_settlement_v1_20260913',120,300)
    add('native_feature_candles_v1','oanda_native_feature_candle_updater_v1.py',['--duration-sec','604800'],
        STATE/'native_feature_candles_heartbeat_v1.json','native_feature_candle_cache_v1_20260913',150,300)
    add('research_feature_observations_v2','oanda_research_feature_observation_worker_v2.py',[
        '--quote-snapshot',STATE/'practice_007_market_quotes_v1.json','--candle-root',DATA/'candles',
        '--native-candle-root',DATA/'native_feature_candles_v1','--archive-root',RUN/'feature_observations_v2',
        '--heartbeat',STATE/'research_feature_observations_heartbeat_v2.json','--clock-state',STATE/'clock_integrity_v1.json',
        '--news-snapshot',DATA/'local_news_sentiment_repair_v2/current_news_v2.json',
        '--model-study',RUN/'pair_local_forecast_study_v3','--model-study',NEWS/'joint_price_news_study_v7',
        '--interval-sec','60','--max-cycle-sec','45','--duration-sec','604800'],
        STATE/'research_feature_observations_heartbeat_v2.json','research_existing_feature_observations_v2_20260913_native_calendar_events',150,180)
    forward=RUN/'feature_forward_v3'
    add('research_feature_forward_v2','oanda_feature_forward_worker_v1.py',[
        '--archive-root',RUN/'feature_observations_v2','--quote-path',STATE/'practice_007_market_quotes_v1.json',
        '--directory',forward,'--max-ledger-mib','4096','--minimum-free-mib','4096',
        '--clock-state',STATE/'clock_integrity_v1.json','--duration-sec','172800'],
        forward/'feature_forward_status_v1.json','feature_forward_worker_status_v1',150,180,
        'oanda_feature_forward_worker_v1.py*'+str(forward))
    horizon=RUN/'official_event_pair_horizon_v2'
    add('official_pair_horizon_v2','oanda_official_event_pair_horizon_capture_v2.py',[
        '--input-database',DATA/'local_news_sentiment/official_release_fast_lane_v4.sqlite',
        '--output-directory',horizon,'--quote-path',STATE/'practice_007_market_quotes_v1.json',
        '--clock-path',STATE/'clock_integrity_v1.json','--interval-sec','1','--duration-sec','0','--quiet'],
        horizon/'heartbeat.json','official_event_pair_horizon_capture_v2_heartbeat',90,180)
    return dict(schema_version='forex_operational_runtime_v1_20260913',research_only=True,can_place_orders=False,
        prepared_utc=datetime.now(timezone.utc).isoformat(),recovery_until_utc='2026-09-20T23:59:00Z',services=services,
        notes='Versioned research plumbing repair. Execution remains a separate finite practice-only trial. Native-news trials v1/v2 retain their failed provenance/identity checks without forecasts; v3 registers the verified emitted identity schema on fresh lineage. Price controls retain distinct translated-live-H1 semantics.')


if __name__=='__main__':
    path=CONFIG/'operational_runtime_v3_20260913.json'
    value=make()
    with path.open('x',encoding='utf-8') as handle:json.dump(value,handle,indent=2,allow_nan=False)
    print(json.dumps(dict(path=str(path),sha256=sha(path),services=len(value['services']))))
