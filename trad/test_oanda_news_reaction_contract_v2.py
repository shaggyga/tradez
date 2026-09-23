"""Exact endpoint units, retained evidence and within-run policy isolation."""
import copy
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch
import pytest
import oanda_news_reaction_contract_v2 as contract
import oanda_news_event_reaction_model as reaction
import oanda_news_research_report_io_v1 as report_io

BASE=datetime(2026,1,1,tzinfo=timezone.utc)
ASOF=datetime(2026,2,1,tzinfo=timezone.utc)

def call(index=0,shift=.001,scale=1.):
    signal=BASE+timedelta(minutes=31*index)
    return {**contract.endpoint_returns(pair='EUR_USD',direction='LONG',horizon_minutes=5,
        signal_utc=signal,entry_utc=signal,nominal_target_utc=signal+timedelta(minutes=5),
        exit_utc=signal+timedelta(minutes=5),as_of_utc=ASOF,
        entry_bid=scale*.9999,entry_ask=scale*1.0001,
        exit_bid=scale*(.9999+shift),exit_ask=scale*(1.0001+shift)),
        'topic_id':str(index),'category':'policy','version':'cleaned_current'}

@pytest.mark.parametrize('scale',[.1,1.,100.])
def test_four_quote_return_uses_common_entry_notional(scale):
    row=call(scale=scale)
    assert row['follow_net_bps']==pytest.approx(8.)
    assert row['fade_net_bps']==pytest.approx(-12.)
    assert row['return_unit']=='bps'

@pytest.mark.parametrize('clock',[None,'not-a-clock','2026-01-02T00:00:00',BASE.isoformat()])
def test_original_scoring_cutoff_cannot_be_missing_naive_or_before_exit(clock):
    row=call();row['as_of_utc']=clock
    with pytest.raises(ValueError):contract.validate_retained_call(row,as_of_utc=ASOF)

def test_later_validation_keeps_original_scoring_cutoff():
    row=call();out=contract.validate_retained_call(row,as_of_utc=ASOF+timedelta(days=1))
    assert out['as_of_utc']==row['as_of_utc']
    assert out['validation_as_of_utc']!=(out['as_of_utc'])

def test_retained_episode_numeric_claim_must_rebuild_from_legs():
    rows=reaction.build_episode_rows([call()],as_of_utc=ASOF)
    rows[0]['follow_net_bps']+=1.
    with pytest.raises(ValueError,match='episode_aggregate_mismatch'):
        reaction.fit_horizon(rows,as_of_utc=ASOF)

def test_final_block_cannot_select_or_change_policy():
    original=[call(i) for i in range(60)]
    altered=[call(i,shift=(-.001 if i>=48 else .001)) for i in range(60)]
    first=reaction.fit_horizon(reaction.build_episode_rows(original,as_of_utc=ASOF),as_of_utc=ASOF)
    second=reaction.fit_horizon(reaction.build_episode_rows(altered,as_of_utc=ASOF),as_of_utc=ASOF)
    assert first['selected_policy_sha256']==second['selected_policy_sha256']
    assert first['selected_policy']==second['selected_policy']
    assert first['final_report_block']['average_net_bps']>0
    assert second['final_report_block']['average_net_bps']<0
    assert first['account_eligible'] is False
    assert 'not_untouched_holdout' in first['final_report_block']['historical_access_provenance']

@pytest.mark.parametrize('raw',[b'{"a":1,"a":2}',b'{"a":NaN}',b'{"a":1e309}',b'[]'])
def test_exact_json_input_refuses_ambiguous_or_nonfinite_values(tmp_path,raw):
    path=tmp_path/'input.json';path.write_bytes(raw)
    with pytest.raises(ValueError):report_io.read_json_snapshot(path)

def test_existing_report_is_never_replaced(tmp_path):
    path=tmp_path/'report.json';report_io.publish_new_json_report(path,{'first':1})
    before=path.read_bytes()
    with pytest.raises(ValueError,match='refuse_overwriting'):
        report_io.publish_new_json_report(path,{'second':2})
    assert path.read_bytes()==before

def test_occupied_reaction_output_is_refused_before_reading_any_input(tmp_path):
    path=tmp_path/'occupied.json';path.write_bytes(b'not-json')
    with patch.object(Path,'open',side_effect=AssertionError('no_input_or_output_read')):
        with pytest.raises(ValueError,match='refuse_overwriting_existing_news_report'):
            reaction.run_audit(tmp_path/'unused.json',tmp_path/'unused.sqlite',path,as_of_utc=ASOF)
