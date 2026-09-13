"""One bounded read of current M1 source files; private immutable evidence only."""
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

BASE=Path('C:/Users/zmoor/Documents/forex')
PROJECT=BASE/'trad'
HERE=Path(__file__).parent
sys.path.insert(0,str(PROJECT))
import oanda_cross_window_currency_capture_v1 as m


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_new(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
    with path.open('xb') as stream:
        stream.write(raw)
    return {'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def source_checks():
    registry=PROJECT/'config/recovered_second_curve_pilot_v1_20260909.json'
    value=json.loads(registry.read_bytes())
    return {'registry_path':str(registry),'registry_sha256':sha(registry),
        'sources':{name:{'expected_sha256':expected,'actual_sha256':sha(PROJECT/name)}
            for name,expected in value['source_bindings'].items()}}


def main():
    private=HERE/'cross_window_private_001'
    private.mkdir(exist_ok=False)
    before=source_checks()
    began=time.time()
    timer=time.perf_counter()
    capture=m.capture_sources(PROJECT/'data/oanda_training_manager/candles')
    capture_ms=(time.perf_counter()-timer)*1000
    source_sha=sha(PROJECT/'oanda_cross_window_currency_capture_v1.py')
    assert source_sha==capture['implementation_sha256']
    capture_file=write_new(private/'retained_source_capture.json',capture)
    selection_epoch=time.time()
    available={}
    failures={}
    for pair in m.PEERS:
        try:
            available[pair]=m._rows(capture['sources'][pair],pair,selection_epoch)
        except m.CrossWindowError as exc:
            failures[pair]=str(exc)
    common=set.intersection(*(set(available[pair]) for pair in m.PEERS)) if len(available)==3 else set()
    cutoff=max(common) if common else None
    consumer=time.time()
    features=None
    result_file=None
    build_ms=None
    if cutoff is not None:
        timer=time.perf_counter()
        features=m.build_features(capture,common_price_epoch=cutoff,consumer_epoch=consumer,
            expected_source_sha256=source_sha)
        build_ms=(time.perf_counter()-timer)*1000
        result_file=write_new(HERE/'CROSS_WINDOW_ACTUAL_FEATURES_20260909.json',features)
    after=source_checks()
    assert before==after
    assert all(row['expected_sha256']==row['actual_sha256'] for row in after['sources'].values())
    assert source_sha==sha(PROJECT/'oanda_cross_window_currency_capture_v1.py')
    summary={'schema_version':'cross_window_engineering_observation_v1_20260909',
        'status':'completed_observation','observed_started_epoch':began,'observed_completed_epoch':time.time(),
        'capture_read_started_epoch':capture['capture_started_epoch'],
        'capture_read_completed_epoch':capture['capture_completed_epoch'],
        'source_sha256':source_sha,'test_source_sha256':sha(PROJECT/'test_oanda_cross_window_currency_capture_v1.py'),
        'lineage':m.LINEAGE,'private_capture':capture_file,'features_artifact':result_file,
        'selection_policy':'latest_exact_common_price_close_among_all_three_successfully_captured_peers',
        'common_price_epoch':cutoff,'common_price_utc':datetime.fromtimestamp(cutoff,timezone.utc).isoformat() if cutoff else None,
        'consumer_epoch':consumer,'cutoff_age_sec':consumer-cutoff if cutoff else None,
        'capture_latency_ms':capture_ms,'pure_replay_latency_ms':build_ms,
        'capture_canonical_bytes':len(m.canonical_bytes(capture)),
        'source_rows':{pair:{'rows':len(rows),'latest_price_epoch':max(rows),
            'retained_bytes_sha256':capture['sources'][pair]['retained_bytes_sha256'],
            'read_completed_epoch':capture['sources'][pair]['read_completed_epoch']} for pair,rows in available.items()},
        'source_refusals':failures,'feature_status':features['status'] if features else 'unavailable',
        'coverage':{key:{'available':value['available_pair_count'],'expected':value['expected_pair_count']}
            for key,value in features['windows'].items()} if features else None,
        'window_refusals':{pair:{key:value['reason'] for key,value in row['windows'].items() if value['reason']}
            for pair,row in features['pairs'].items()} if features else None,
        'registered_pilot_sources_before_and_after':after,'registered_pilot_sources_unchanged':True,
        'model_fitted':False,'model_inputs_injected':False,'worker_started':False,'network_called':False,
        'broker_action':False,'news_or_account_payload_present':False,'prospective_accuracy_claim':False,
        'source_observation_is_now_not_historical_availability':True}
    evidence=write_new(HERE/'CROSS_WINDOW_ENGINEERING_OBSERVATION_20260909.json',summary)
    print(json.dumps({'evidence':evidence,'feature_status':summary['feature_status'],
        'cutoff_age_sec':summary['cutoff_age_sec'],'coverage':summary['coverage'],
        'source_refusals':failures,'window_refusals':summary['window_refusals'],
        'capture_ms':capture_ms,'replay_ms':build_ms,'registered_sources_unchanged':True}))


if __name__=='__main__':
    main()
