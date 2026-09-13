"""Local synthetic quote observations; no broker connection or live source edits."""
from copy import deepcopy
from decimal import localcontext
import hashlib
import json
from pathlib import Path
import stat

import pytest

import oanda_research_quote_receipt_v1 as q

NOW=1788920000.5
TIME='2026-09-09T02:13:20.123456789Z'


def snapshot():
    quotes={pair:dict(bid=1.1234567890123457,ask=1.1234567890123459,pip=float(meta['pip_size']),
        source='stream',time=TIME,tradeable=True) for pair,meta in q.METADATA.items()}
    return dict(schema_version=3,producer='practice_007_dedicated_quote_stream',research_only=True,
        generated_utc='2026-09-09T02:13:20.400000+00:00',connection_generation=6,quote_count=3,quotes=quotes,
        coverage=dict(connection_generation=6,current_non_tradeable_instruments=[],current_non_tradeable_quote_count=0,
            current_quote_count=3,current_tradeability_unknown_count=0,current_tradeability_unknown_instruments=[],
            current_tradeable_quote_count=3,execution_requires_independent_freshness_check=True,last_known_quote_count=3,
            retained_last_known_count=0,retained_last_known_instruments=[],retained_quotes_execution_eligible=False,
            tradeability_contract='oanda_client_price_status_boolean_v1'))


def observe(tmp_path,value=None,raw=None):
    source=tmp_path/'quote_fixture.json'
    source.write_bytes(raw if raw is not None else json.dumps(value or snapshot()).encode())
    times=iter([NOW-0.05,NOW])
    return q.capture_quote_snapshot(source,clock=lambda:next(times))


def mapped(capture,**kwargs):
    args=dict(decision_epoch=NOW+0.1)
    args.update(kwargs)
    return q.map_quote_snapshot(capture['raw_bytes'],capture['receipt'],**args)


def test_precise_json_decimals_original_ns_and_actual_read_availability(tmp_path):
    raw=json.dumps(snapshot()).replace('1.1234567890123457','1.123456789012345678901').encode()
    capture=observe(tmp_path,raw=raw)
    result=mapped(capture)
    assert set(result['quotes'])==set(q.METADATA) and not result['refusals']
    quote=result['quotes']['EUR_USD']
    assert quote['bid']=='1.123456789012345678901'
    assert quote['market_time_rfc3339']==TIME
    assert quote['available_epoch']==NOW
    assert result['metadata']==q.METADATA
    assert capture['raw_bytes']==raw
    assert capture['receipt']['raw_sha256']==hashlib.sha256(raw).hexdigest()
    assert q._hash(result['source_header'])==result['source_header_sha256']
    assert all(result[k] is v for k,v in q.FLAGS.items())


def test_decimal_context_does_not_round_prices_or_freshness(tmp_path):
    capture=observe(tmp_path)
    expected=mapped(capture)
    with localcontext() as ctx:
        ctx.prec=4
        assert mapped(capture)==expected


@pytest.mark.parametrize('mutation,reason',[
    (lambda v:v.update(schema_version=True),'quote_snapshot_identity'),
    (lambda v:v.update(producer='candle_cache'),'quote_snapshot_identity'),
    (lambda v:v.update(research_only=False),'quote_snapshot_identity'),
    (lambda v:v.update(account_id='not_allowed'),'quote_snapshot_shape'),
    (lambda v:v.update(generated_utc='2026-09-09T02:13:21Z'),'quote_snapshot_future'),
    (lambda v:v['coverage'].update(connection_generation=5),'quote_coverage_generation'),
    (lambda v:v['coverage'].update(retained_quotes_execution_eligible=True),'quote_tradeability_contract'),
    (lambda v:v['coverage'].update(tradeability_contract='invented'),'quote_tradeability_contract'),
    (lambda v:v.update(quote_count=2),'quote_inventory_count'),
    (lambda v:v['coverage'].update(current_tradeable_quote_count=2),'quote_coverage_count'),
])
def test_bad_wrapper_is_not_mapped(tmp_path,mutation,reason):
    value=snapshot()
    mutation(value)
    with pytest.raises(q.QuoteReceiptError,match=reason):
        mapped(observe(tmp_path,value))


@pytest.mark.parametrize('field,value,reason',[
    ('bid',True,'quote_numeric_json_required'),('bid','1.0','quote_numeric_json_required'),
    ('bid',0,'quote_price_invalid'),('ask',-1,'quote_price_invalid'),
    ('bid',9,'quote_crossed'),('pip',0.01,'quote_pip_metadata_mismatch'),
    ('source','candle','quote_not_stream_source'),('time','invalid','quote_timestamp_invalid'),
    ('time','2026-09-09T02:13:20.450000000Z','quote_market_future_at_observation'),
    ('time','2026-09-09T02:12:00Z','quote_market_stale'),
])
def test_bad_selected_row_isolated_from_two_valid_siblings(tmp_path,field,value,reason):
    source=snapshot()
    source['quotes']['EUR_USD'][field]=value
    result=mapped(observe(tmp_path,source))
    assert result['refusals']=={'EUR_USD':reason}
    assert set(result['quotes'])=={'GBP_USD','USD_JPY'}


@pytest.mark.parametrize('kind',['retained','nontradeable','unknown'])
def test_actual_coverage_prevents_ineligible_quote_admission(tmp_path,kind):
    source=snapshot()
    coverage=source['coverage']
    coverage['current_tradeable_quote_count']=2
    if kind=='retained':
        coverage.update(retained_last_known_instruments=['EUR_USD'],retained_last_known_count=1,current_quote_count=2)
        reason='quote_retained_previous_connection'
    elif kind=='nontradeable':
        source['quotes']['EUR_USD']['tradeable']=False
        coverage.update(current_non_tradeable_instruments=['EUR_USD'],current_non_tradeable_quote_count=1)
        reason='quote_nontradeable'
    else:
        source['quotes']['EUR_USD']['tradeable']=None
        coverage.update(current_tradeability_unknown_instruments=['EUR_USD'],current_tradeability_unknown_count=1)
        reason='quote_tradeability_unknown'
    result=mapped(observe(tmp_path,source))
    assert result['refusals']=={'EUR_USD':reason}
    assert len(result['quotes'])==2


def test_future_source_at_original_observation_stays_rejected_when_decision_advances(tmp_path):
    source=snapshot()
    source['quotes']['EUR_USD']['time']='2026-09-09T02:13:20.450000000Z'
    result=mapped(observe(tmp_path,source),decision_epoch=NOW+1)
    assert result['refusals']['EUR_USD']=='quote_market_future_at_observation'


def test_old_snapshot_and_quotes_are_not_refreshed_by_read(tmp_path):
    source=snapshot()
    source['generated_utc']='2026-09-09T02:10:00Z'
    result=mapped(observe(tmp_path,source))
    assert result['status']=='unavailable'
    assert set(result['refusals'].values())=={'quote_snapshot_stale'}


@pytest.mark.parametrize('overrides',[
    {'decision_epoch':NOW-1},{'maximum_quote_age_sec':True},{'maximum_quote_age_sec':61},
    {'instruments':['EUR_USD','EUR_USD']},{'instruments':['EUR_TRY']},
])
def test_invalid_decision_or_inventory_withheld(tmp_path,overrides):
    with pytest.raises(q.QuoteReceiptError):
        mapped(observe(tmp_path),**overrides)


def test_tampered_raw_or_receipt_cannot_map(tmp_path):
    capture=observe(tmp_path)
    with pytest.raises(q.QuoteReceiptError,match='raw_mismatch'):
        q.map_quote_snapshot(capture['raw_bytes']+b' ',capture['receipt'],decision_epoch=NOW+1)
    capture['receipt']['read_completed_epoch']=NOW-2
    with pytest.raises(q.QuoteReceiptError,match='receipt_hash'):
        mapped(capture)


@pytest.mark.parametrize('raw',[b'{"x":1,"x":2}',b'{"x":NaN}',b'{',b'\xff'])
def test_malformed_or_ambiguous_source_json_withheld(tmp_path,raw):
    with pytest.raises(q.QuoteReceiptError):
        mapped(observe(tmp_path,raw=raw))


def test_current_pair_missing_explicitly(tmp_path):
    source=snapshot()
    del source['quotes']['EUR_USD']
    source['quote_count']=2
    source['coverage'].update(current_quote_count=2,last_known_quote_count=2,current_tradeable_quote_count=2)
    assert mapped(observe(tmp_path,source))['refusals']=={'EUR_USD':'quote_pair_missing'}


def test_capture_rejects_oversized_source_and_future_read_order(tmp_path):
    source=tmp_path/'large.json'
    with source.open('wb') as handle:
        handle.truncate(q.MAX_BYTES+1)
    with pytest.raises(q.QuoteReceiptError,match='byte_limit'):
        q.capture_quote_snapshot(source,clock=lambda:NOW)
    source.write_bytes(b'{}')
    times=iter([NOW,NOW-1])
    with pytest.raises(q.QuoteReceiptError,match='read_clock_reversed'):
        q.capture_quote_snapshot(source,clock=lambda:next(times))


def test_capture_brackets_source_read_with_actual_clock(tmp_path,monkeypatch):
    source=tmp_path/'source.json'
    source.write_bytes(json.dumps(snapshot()).encode())
    calls=[]
    original=Path.open
    def opened(path,*args,**kwargs):
        calls.append('read')
        return original(path,*args,**kwargs)
    monkeypatch.setattr(Path,'open',opened)
    def timed():
        calls.append('clock')
        return NOW
    q.capture_quote_snapshot(source,clock=timed)
    assert calls==['clock','read','clock']


def test_reparse_file_is_rejected(tmp_path,monkeypatch):
    source=tmp_path/'source.json'
    source.write_bytes(b'{}')
    original=Path.lstat
    class Link:
        st_mode=stat.S_IFLNK
        st_file_attributes=0
    monkeypatch.setattr(Path,'lstat',lambda path:Link() if path==source else original(path))
    with pytest.raises(q.QuoteReceiptError,match='reparse'):
        q.capture_quote_snapshot(source,clock=lambda:NOW)
