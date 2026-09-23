"""A preliminary CPI-only listing cannot supply final CPIF numbers."""
import datetime as dt

import pytest

import oanda_local_news_sentiment as news

NOW=dt.datetime(2026,9,7,22,0,tzinfo=dt.timezone.utc)
FLASH=("<h2>Statistical news</h2><h3>Flash CPI: Inflation rate 0.3 percent in August 2026</h3>"
       "<p>The preliminary CPI inflation rate for August 2026 was 0.3 percent, "
       "which was an increase from July when the inflation rate was 0.2 percent. "
       "The monthly change for the CPI from July to August was -0.3 percent. "
       "The regular publication for August takes place on September 14.</p>").encode()
SOURCE={"source_id":"scb_test","kind":"scb_cpi_snapshot","url":"https://www.scb.se/PR0101-EN/?menu=open",
        "source_contract_id":"final_cpif_only","source_cohort_id":"final_cpif_only",
        "verified":True,"direct":True,"currencies":["SEK"]}

def test_explicit_preliminary_cpi_listing_is_typed_pending_without_numeric_rows():
    with pytest.raises(news.SourceContentPending) as raised:
        news.parse_scb_cpi_snapshot(FLASH,SOURCE)
    assert raised.value.reason=="awaiting_regular_cpif_release"
    assert raised.value.details=={"reference_period":"2026M08","regular_release_date":"2026-09-14",
                                 "source_stage":"preliminary_cpi_only","numeric_rows_emitted":0}

@pytest.mark.parametrize("payload",[
    FLASH.replace(b"Flash CPI:",b"CPI:"),
    FLASH.replace(b"preliminary",b"estimated"),
    FLASH.replace(b"The regular publication for August takes place on September 14.",b""),
    FLASH.replace(b"for August takes place",b"for July takes place"),
    FLASH.replace(b"September 14",b"Impossible 99"),
    b"<html>unrelated or malformed provider content</html>",
])
def test_unknown_or_conflicting_content_stays_a_parse_error(payload):
    with pytest.raises(ValueError) as raised:news.parse_scb_cpi_snapshot(payload,SOURCE)
    assert not isinstance(raised.value,news.SourceContentPending)

def test_fetch_retains_old_numeric_identity_and_reports_pending(monkeypatch):
    class Response:
        status=200;headers={}
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def read(self,limit):return FLASH
    monkeypatch.setattr(news.urllib.request,"urlopen",lambda request,**kwargs:Response())
    prior={"source_contract_id":"final_cpif_only","source_cohort_id":"final_cpif_only",
           "known_release_ids":["final_july_existing"],"consecutive_errors":4,
           "last_parse_success_utc":"2026-08-13T06:01:00+00:00"}
    rows,state=news.fetch_source(SOURCE,prior,timeout_sec=1,maximum_bytes=100000,now=NOW)
    assert rows==[] and state["parsed_items"]==0
    assert state["known_release_ids"]==prior["known_release_ids"]
    assert state["last_parse_success_utc"]==prior["last_parse_success_utc"]
    assert state["last_parse_status"]=="pending_source_content" and state["consecutive_errors"]==0
    assert state["last_error"]==""
    assert state["source_content_state"]["observed_utc"]==NOW.isoformat()

def test_unknown_page_keeps_error_and_does_not_advance_parse_success(monkeypatch):
    class Response:
        status=200;headers={}
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def read(self,limit):return b"<html>unexpected provider content</html>"
    monkeypatch.setattr(news.urllib.request,"urlopen",lambda request,**kwargs:Response())
    rows,state=news.fetch_source(SOURCE,{},timeout_sec=1,maximum_bytes=100000,now=NOW)
    assert rows==[] and state["last_parse_status"]=="error"
    assert state["last_error"].startswith("parse_error:")
    assert not state.get("last_parse_success_utc")

def test_valid_parser_success_clears_previous_pending_state(monkeypatch):
    class Response:
        status=200;headers={}
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def read(self,limit):return b"final valid provider content"
    monkeypatch.setattr(news.urllib.request,"urlopen",lambda request,**kwargs:Response())
    monkeypatch.setattr(news,"parse_scb_cpi_snapshot",lambda payload,source:[])
    rows,state=news.fetch_source(SOURCE,{"source_content_state":{"status":"awaiting_regular_cpif_release"}},timeout_sec=1,maximum_bytes=100000,now=NOW)
    assert rows==[] and state["last_parse_status"]=="parsed"
    assert state["last_parse_success_utc"]==NOW.isoformat() and "source_content_state" not in state
