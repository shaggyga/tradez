"""Causal and scoped-feature regressions using disposable, real CSV bytes."""
import base64
import copy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
from pathlib import Path

import pytest

import oanda_cross_window_currency_capture_v1 as m

CUTOFF=1788931260
NOW=CUTOFF+5
HEADER=b'time,instrument,granularity,open,high,low,close\n'


def stamp(epoch):
    return datetime.fromtimestamp(epoch,timezone.utc).isoformat().replace('+00:00','Z')


def rows_for(pair, *, omit=(), prices=None, count=81):
    prices=prices or {}
    base=Decimal({'EUR_USD':'1.1','GBP_USD':'1.3','USD_JPY':'150'}[pair])
    pip=Decimal(m.PIPS[pair])
    result=[]
    for index in range(count):
        epoch=CUTOFF-(count-1-index)*60
        if epoch in omit:
            continue
        close=Decimal(str(prices.get(epoch,base+Decimal(index)*pip)))
        width=Decimal(10)*pip
        result.append(f'{stamp(epoch-60)},{pair},M1,{close},{close+width},{close-width},{close}\n'.encode())
    return result


def write_sources(root, **changes):
    for pair in m.PEERS:
        (root/(pair+'_M1.csv')).write_bytes(HEADER+b''.join(rows_for(pair,**changes.get(pair,{}))))


def captured(tmp_path, **changes):
    write_sources(tmp_path,**changes)
    return m.capture_sources(tmp_path,clock=lambda:NOW)


def reseal(value):
    value['capture_sha256']=m.digest({k:v for k,v in value.items() if k!='capture_sha256'})
    return value


def replace_tail(capture,pair,raw):
    source=capture['sources'][pair]
    header=base64.b64decode(source['header_base64'])
    source.update(tail_base64=base64.b64encode(raw).decode(),file_size_bytes=len(header)+len(raw),
        tail_byte_offset=len(header),retained_bytes_sha256=hashlib.sha256(header+raw).hexdigest())
    return reseal(capture)


def build(capture,**kwargs):
    options=dict(common_price_epoch=CUTOFF,consumer_epoch=NOW,
        expected_source_sha256=capture['implementation_sha256'])
    options.update(kwargs)
    return m.build_features(capture,**options)


def window(result,pair='EUR_USD',minute=15):
    return result['pairs'][pair]['windows'][str(minute)]


def test_true_paths_are_distinct_and_original_endpoints_retained(tmp_path):
    capture=captured(tmp_path,EUR_USD={'prices':{CUTOFF:'1.10',CUTOFF-60:'1.11',
        CUTOFF-900:'1.09',CUTOFF-3600:'1.12'}})
    result=build(capture)
    assert result['status']=='available'
    assert [Decimal(window(result,minute=v)['signed_native_pips']) for v in (1,15,60)]==[-100,100,-200]
    assert [window(result,minute=v)['start_price_epoch'] for v in (1,15,60)]==[CUTOFF-60,CUTOFF-900,CUTOFF-3600]
    assert window(result)['end_bar_label_epoch']==CUTOFF-60
    assert window(result)['source_available_epoch']==NOW
    assert result['available_epoch']==NOW and result['common_price_epoch']==CUTOFF
    assert result['original_historical_availability_proven'] is False
    assert result['forecast_horizon_sec'] is None and result['model_inputs_injected'] is False


def test_missing_fifteen_minute_endpoint_does_not_fallback_to_one(tmp_path):
    result=build(captured(tmp_path,EUR_USD={'omit':{CUTOFF-900}}))
    assert window(result)['reason']=='cross_window_start_missing'
    assert window(result,minute=1)['status']=='available'
    assert window(result,minute=60)['status']=='available'
    assert result['windows']['15']['available_pair_count']==2
    assert result['pairs']['EUR_USD']['strength_gap15_x_strength_gap60'] is None


def test_missing_atr_support_does_not_bridge_gap_and_keeps_raw_delta(tmp_path):
    result=build(captured(tmp_path,EUR_USD={'omit':{CUTOFF-420}}))
    assert result['pairs']['EUR_USD']['support']['available_atr_rows']==14
    for minute in m.WINDOWS:
        value=window(result,minute=minute)
        assert value['reason']=='cross_exact_atr_support_missing'
        assert value['normalized_return'] is None
        assert value['signed_native_pips'] is not None


def test_late_peer_does_not_select_its_earlier_cutoff(tmp_path):
    result=build(captured(tmp_path,USD_JPY={'omit':{CUTOFF}}))
    assert window(result,'USD_JPY')['reason']=='cross_common_endpoint_missing'
    assert window(result)['end_price_epoch']==CUTOFF
    assert result['windows']['15']['available_pairs']==['EUR_USD','GBP_USD']
    assert result['windows']['15']['currency_strengths']['JPY'] is None


@pytest.mark.parametrize('field,value,consumer,reason',[
    ('read_completed_epoch',NOW+1,NOW+2,'cross_peer_outside_capture_observation'),
    ('read_started_epoch',NOW-1,NOW,'cross_peer_outside_capture_observation'),
    ('read_started_epoch',NOW+1,NOW,'cross_peer_observed_in_future'),
])
def test_peer_read_clocks_must_fit_actual_capture(tmp_path,field,value,consumer,reason):
    capture=captured(tmp_path)
    capture['sources']['USD_JPY'][field]=value
    result=build(reseal(capture),consumer_epoch=consumer)
    assert window(result,'USD_JPY')['reason']==reason
    assert result['windows']['15']['available_pair_count']==2


def test_future_original_bar_cannot_become_eligible_after_time_passes(tmp_path):
    capture=captured(tmp_path)
    raw=base64.b64decode(capture['sources']['USD_JPY']['tail_base64'])
    raw+=f'{stamp(CUTOFF)},USD_JPY,M1,151,152,150,151\n'.encode()
    result=build(replace_tail(capture,'USD_JPY',raw),consumer_epoch=NOW+120)
    assert window(result,'USD_JPY')['reason']=='cross_future_bar_at_observation'


def test_usdjpy_orientation_and_shared_usd_dependence(tmp_path):
    result=build(captured(tmp_path))
    normalized={pair:Decimal(window(result,pair)['normalized_return']) for pair in m.PEERS}
    strengths=result['windows']['15']['currency_strengths']
    assert Decimal(strengths['USD'])==(-normalized['EUR_USD']-normalized['GBP_USD']+normalized['USD_JPY'])/3
    assert Decimal(strengths['JPY'])==-normalized['USD_JPY']
    assert Decimal(strengths['EUR'])==normalized['EUR_USD']
    jpy=window(result,'USD_JPY')
    assert jpy['quote_strengthening_breadth_ex_self'] is None
    assert jpy['independent_currency_peer_counts']=={'base':2,'quote':0}
    assert Decimal(jpy['base_strengthening_breadth_ex_self'])==0
    assert jpy['strength_includes_own_return'] is True
    assert result['dependence']['independent_trial_count'] is None
    assert result['peer_universe']==list(m.PEERS)
    assert result['windows']['15']['expected_pair_count']==3


def test_tied_rank_uses_actual_three_pair_denominator(tmp_path):
    result=build(captured(tmp_path))
    assert Decimal(result['windows']['15']['sample_dispersion'])==0
    for pair in m.PEERS:
        with localcontext() as context:
            context.prec=96
            assert Decimal(window(result,pair)['percentile_rank'])==Decimal(2)/3
        assert window(result,pair)['rank_denominator']==3


def test_partial_peer_breadth_and_rank_denominators_are_not_full_universe(tmp_path):
    capture=captured(tmp_path)
    capture['sources']['GBP_USD']={'status':'unavailable','reason':'cross_source_unavailable','observed_epoch':NOW}
    capture['sources']['USD_JPY']={'status':'unavailable','reason':'cross_source_unavailable','observed_epoch':NOW}
    result=build(reseal(capture))
    assert result['status']=='partial'
    assert result['windows']['15']['available_pair_count']==1
    assert window(result)['rank_denominator']==1
    assert window(result)['up_breadth_ex_self'] is None
    assert result['windows']['15']['sample_dispersion'] is None


def test_decimal_ambient_context_cannot_change_features(tmp_path):
    capture=captured(tmp_path)
    expected=build(capture)
    with localcontext() as context:
        context.prec=3
        actual=build(capture)
    assert actual==expected


def test_normalizer_matches_fourteen_true_ranges_and_clips(tmp_path):
    result=build(captured(tmp_path,EUR_USD={'prices':{CUTOFF-3600:'0.8'}}))
    value=window(result,minute=60)
    assert Decimal(value['atr14_native_pips'])==20
    assert Decimal(value['normalized_return'])==10


def test_atr_uses_previous_close_gaps_not_only_bar_range(tmp_path):
    result=build(captured(tmp_path,EUR_USD={'prices':{CUTOFF-60:'1.2'}}))
    value=window(result)
    assert Decimal(value['atr14_native_pips'])>20
    assert result['pairs']['EUR_USD']['support']['atr_support_price_epochs']==list(range(CUTOFF-840,CUTOFF+1,60))


def test_zero_atr_floor_stays_finite_and_zero_direction_has_no_agreement(tmp_path):
    capture=captured(tmp_path)
    for pair in m.PEERS:
        price={'EUR_USD':'1.1','GBP_USD':'1.3','USD_JPY':'150'}[pair]
        raw=b''.join(f'{stamp(CUTOFF-i*60-60)},{pair},M1,{price},{price},{price},{price}\n'.encode()
            for i in range(80,-1,-1))
        replace_tail(capture,pair,raw)
    result=build(capture)
    for pair in m.PEERS:
        assert Decimal(window(result,pair)['normalized_return'])==0
        assert window(result,pair)['same_direction_breadth_ex_self'] is None


@pytest.mark.parametrize('age,status',[(900,'available'),(901,'unavailable')])
def test_explicit_staleness_boundary(tmp_path,age,status):
    result=build(captured(tmp_path),consumer_epoch=CUTOFF+age)
    assert result['status']==status
    if status=='unavailable':
        assert window(result)['reason']=='cross_cutoff_stale'


@pytest.mark.parametrize('option,value,reason',[
    ('common_price_epoch',CUTOFF+60,'cross_cutoff_future_or_off_grid'),
    ('common_price_epoch',CUTOFF+1,'cross_cutoff_future_or_off_grid'),
    ('consumer_epoch',CUTOFF+4,'cross_capture_not_yet_available'),
    ('maximum_age_sec',901,'cross_age_policy'),
    ('maximum_age_sec',True,'cross_age_policy'),
    ('expected_source_sha256','a'*64,'cross_implementation_binding'),
])
def test_global_cutoff_source_and_availability_contract(tmp_path,option,value,reason):
    with pytest.raises(m.CrossWindowError,match=reason):
        build(captured(tmp_path),**{option:value})


@pytest.mark.parametrize('mutation,reason',[
    (lambda c:c.update(can_place_orders=True),'cross_capture_authority'),
    (lambda c:c.update(extra='unexpected'),'cross_capture_identity'),
    (lambda c:c.update(peers=list(reversed(m.PEERS))),'cross_capture_identity'),
    (lambda c:c.update(capture_sha256='0'*64),'cross_capture_seal'),
    (lambda c:c['sources'].update(USD_JPY=[]),'cross_peer_inventory'),
])
def test_outer_tamper_withholds(tmp_path,mutation,reason):
    capture=captured(tmp_path)
    mutation(capture)
    if reason!='cross_capture_seal':
        reseal(capture)
    with pytest.raises(m.CrossWindowError,match=reason):
        build(capture)


@pytest.mark.parametrize('change,reason',[
    (lambda raw:raw+raw.splitlines(keepends=True)[-1],'cross_duplicate_or_unordered_bar'),
    (lambda raw:b''.join(reversed(raw.splitlines(keepends=True))),'cross_duplicate_or_unordered_bar'),
    (lambda raw:raw.replace(b'EUR_USD',b'GBP_USD'),'cross_row_identity'),
    (lambda raw:raw.replace(b',M1,',b',M5,'),'cross_row_identity'),
    (lambda raw:raw.replace(b'1.1010',b'0.0001',1),'cross_ohlc_range'),
])
def test_retained_csv_invalid_rows_are_not_features(tmp_path,change,reason):
    capture=captured(tmp_path)
    raw=base64.b64decode(capture['sources']['EUR_USD']['tail_base64'])
    result=build(replace_tail(capture,'EUR_USD',change(raw)))
    assert window(result)['reason']==reason
    assert result['windows']['15']['available_pair_count']==2


@pytest.mark.parametrize('field,value,reason',[
    ('retained_bytes_sha256','0'*64,'cross_source_hash_mismatch'),
    ('tail_byte_offset',1,'cross_source_byte_geometry'),
    ('tail_base64','!','cross_source_base64'),
    ('hash_scope','entire_file','cross_source_hash_scope'),
])
def test_inner_provenance_replayed_independently(tmp_path,field,value,reason):
    capture=captured(tmp_path)
    capture['sources']['EUR_USD'][field]=value
    result=build(reseal(capture))
    assert window(result)['reason']==reason


def test_malformed_failed_peer_never_leaks_exception(tmp_path):
    capture=captured(tmp_path)
    capture['sources']['EUR_USD']={'status':'unavailable','reason':['not','a','code'],'observed_epoch':NOW}
    result=build(reseal(capture))
    assert window(result)['reason']=='cross_source_failure_code'
    assert result['pairs']['EUR_USD']['source_read_reason'] is None


def test_capture_missing_source_preserves_other_peers(tmp_path):
    write_sources(tmp_path)
    (tmp_path/'EUR_USD_M1.csv').unlink()
    result=build(m.capture_sources(tmp_path,clock=lambda:NOW))
    assert window(result)['reason']=='cross_peer_read_unavailable'
    assert result['pairs']['EUR_USD']['source_read_reason']=='cross_source_unavailable'
    assert result['windows']['15']['available_pair_count']==2


def test_absent_all_peers_is_unavailable_without_invented_features(tmp_path):
    result=build(m.capture_sources(tmp_path,clock=lambda:NOW))
    assert result['status']=='unavailable'
    assert all(value['available_pair_count']==0 for value in result['windows'].values())
    assert all(value is None for value in result['windows']['15']['currency_strengths'].values())


def test_unavailable_peer_observation_cannot_postdate_capture(tmp_path):
    capture=captured(tmp_path)
    capture['sources']['EUR_USD']={'status':'unavailable','reason':'cross_source_unavailable','observed_epoch':NOW+1}
    assert window(build(reseal(capture),consumer_epoch=NOW+2))['reason']=='cross_peer_outside_capture_observation'


def test_partial_source_tail_is_withheld(tmp_path):
    write_sources(tmp_path)
    path=tmp_path/'EUR_USD_M1.csv'
    path.write_bytes(path.read_bytes()[:-1])
    capture=m.capture_sources(tmp_path,clock=lambda:NOW)
    assert capture['sources']['EUR_USD']['reason']=='cross_partial_tail'


def test_bounded_tail_retains_exact_final_rows_and_byte_geometry(tmp_path):
    write_sources(tmp_path,EUR_USD={'count':650})
    capture=m.capture_sources(tmp_path,clock=lambda:NOW)
    source=capture['sources']['EUR_USD']
    tail=base64.b64decode(source['tail_base64'])
    assert len(tail.splitlines())==m.MAX_ROWS
    assert source['tail_byte_offset']+len(tail)==source['file_size_bytes']
    assert build(capture)['status']=='available'


def test_source_change_during_read_is_explicit(tmp_path,monkeypatch):
    write_sources(tmp_path)
    original=m.os.fstat
    calls=0
    def fstat(fd):
        nonlocal calls
        result=original(fd)
        calls+=1
        if calls==2:
            from types import SimpleNamespace
            return SimpleNamespace(st_dev=result.st_dev,st_ino=result.st_ino,
                st_size=result.st_size+1,st_mtime_ns=result.st_mtime_ns)
        return result
    monkeypatch.setattr(m.os,'fstat',fstat)
    capture=m.capture_sources(tmp_path,clock=lambda:NOW)
    assert capture['sources']['EUR_USD']['reason']=='cross_source_changed_during_read'


def test_reparse_source_is_withheld(tmp_path,monkeypatch):
    write_sources(tmp_path)
    original=Path.lstat
    def lstat(path,*args,**kwargs):
        result=original(path,*args,**kwargs)
        if path.name=='EUR_USD_M1.csv':
            from types import SimpleNamespace
            return SimpleNamespace(st_mode=result.st_mode,st_file_attributes=1024)
        return result
    monkeypatch.setattr(Path,'lstat',lstat)
    capture=m.capture_sources(tmp_path,clock=lambda:NOW)
    assert capture['sources']['EUR_USD']['reason']=='cross_source_reparse_point'


def test_returned_metadata_does_not_mutate_contract_or_input(tmp_path):
    capture=captured(tmp_path)
    original=copy.deepcopy(capture)
    result=build(capture)
    result['lineage']['normalized_return_and_currency_aggregation']['lines'].append(999)
    result['pairs']['EUR_USD']['windows']['15']['start_price']='999'
    assert capture==original
    assert 999 not in m.LINEAGE['normalized_return_and_currency_aggregation']['lines']
    capture['lineage']['normalized_return_and_currency_aggregation']['lines'].append(888)
    assert 888 not in m.LINEAGE['normalized_return_and_currency_aggregation']['lines']


def test_seals_replay_exact_retained_json(tmp_path):
    capture=captured(tmp_path)
    result=build(json.loads(m.canonical_bytes(capture)))
    assert result['features_sha256']==m.digest({k:v for k,v in result.items() if k!='features_sha256'})
    assert all(result[key] is value for key,value in m.FLAGS.items())
