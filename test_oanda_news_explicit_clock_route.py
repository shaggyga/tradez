"""Behavioral routing checks; no network or existing state is consulted."""
import datetime as dt
import json
import sys
from pathlib import Path
import pytest
import oanda_local_news_sentiment as news

NOW=dt.datetime(2026,9,13,2,0,tzinfo=dt.timezone.utc)

def test_omitted_clock_retains_original_one_argument_call(monkeypatch):
    seen=[]
    def original(value):
        seen.append(value)
        return value, {'trusted':False}
    monkeypatch.setattr(news,'normalized_observation_time',original)
    assert news.collection_observation_time(NOW)==(NOW,{'trusted':False})
    assert seen==[NOW]

def test_selected_missing_clock_cannot_fall_back_to_default(monkeypatch,tmp_path):
    selected=tmp_path/'missing-clock.json';seen=[]
    def read(path,default):
        seen.append(path)
        assert path==selected
        return {}
    monkeypatch.setattr(news,'load_json',read)
    stamp,evidence=news.collection_observation_time(NOW,clock_integrity_path=selected)
    assert stamp==NOW
    assert evidence['status']=='clock_integrity_untrusted'
    assert not news.prospective_clock_attestation(evidence)
    assert seen==[selected]

@pytest.mark.parametrize('status',['ok','mitigated'])
def test_selected_fresh_clock_attests_and_does_not_change_time(monkeypatch,tmp_path,status):
    selected=tmp_path/'fresh-clock.json'
    state={'generated_utc':NOW.isoformat(),'status':status,'timestamp_normalization_trusted':True,
           'host_clock_synchronized':True,'source_fresh':True,'broker_clock_lead_sec':0.25,
           'broker_clock_sample_count':40,'external_https_clock':{}}
    selected.write_text(json.dumps(state))
    original=news.load_json;seen=[]
    def read(path,default):
        seen.append(path);assert path==selected
        return original(path,default)
    monkeypatch.setattr(news,'load_json',read)
    stamp,evidence=news.collection_observation_time(NOW,clock_integrity_path=selected)
    assert stamp==NOW
    assert news.prospective_clock_attestation(evidence)
    assert seen==[selected]

def test_aggregation_rechecks_selected_clock_after_inputs(monkeypatch,tmp_path):
    selected=tmp_path/'clock.json';seen=[]
    monkeypatch.setattr(news,'utc_now',lambda:NOW)
    def observe(value,*,clock_integrity_path):
        seen.append((value,clock_integrity_path))
        return NOW,{'trusted':True}
    monkeypatch.setattr(news,'normalized_observation_time',observe)
    monkeypatch.setattr(news,'prospective_clock_attestation',lambda evidence:evidence['trusted'])
    assert news.refresh_pair_aggregation_clock(NOW-dt.timedelta(seconds=3),clock_integrity_path=selected)==(NOW,{'trusted':True})
    assert seen==[(NOW,selected)]

def test_cycle_missing_selected_clock_refuses_before_database(monkeypatch,tmp_path):
    config=tmp_path/'config.json';config.write_text('{"sources":[]}')
    selected=tmp_path/'missing-clock.json';out=tmp_path/'news'
    monkeypatch.setattr(news,'utc_now',lambda:NOW)
    def forbidden(*args,**kwargs):
        raise AssertionError('database_must_not_open_on_missing_selected_clock')
    monkeypatch.setattr(news,'process_database',forbidden)
    result=news.run_cycle(config_path=config,output_root=out,ledger_path=tmp_path/'ledger.csv',
        event_root=tmp_path/'events',refresh_event_catalog=False,clock_integrity_path=selected)
    assert result['status']=='blocked_clock_integrity'
    assert result['database_opened'] is False
    assert result['attempted_sources']==0

def test_cli_explicit_clock_path(monkeypatch,tmp_path):
    selected=tmp_path/'clock.json'
    monkeypatch.setattr(sys,'argv',['collector','--clock-integrity-state',str(selected),'--once'])
    assert news.parse_args().clock_integrity_state==selected

def test_cli_omitted_clock_path(monkeypatch):
    monkeypatch.setattr(sys,'argv',['collector','--once'])
    assert news.parse_args().clock_integrity_state is None
