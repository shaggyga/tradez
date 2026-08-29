import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_direct_cpi_acceleration_prospective as worker


UTC = dt.timezone.utc


def _macro_db(path: Path) -> None:
    db=sqlite3.connect(path)
    db.execute("""CREATE TABLE macro_release_revisions(
      row_id INTEGER PRIMARY KEY,release_key TEXT,source_event_id TEXT,event_series_id TEXT,
      currencies_json TEXT,reference_period TEXT,unit TEXT,actual_value REAL,previous_value REAL,
      causal_known_utc TEXT,source_id TEXT,source_url TEXT,source_verified INTEGER,
      source_direct INTEGER,payload_sha256 TEXT,payload_json TEXT)""")
    db.execute("CREATE VIEW macro_release_latest AS SELECT * FROM macro_release_revisions")
    db.commit();db.close()


def _insert(path: Path, row: tuple) -> None:
    db=sqlite3.connect(path)
    db.execute("INSERT INTO macro_release_revisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",row)
    db.commit();db.close()


def test_loader_requires_both_direct_post_cohort_legs(tmp_path: Path):
    path=tmp_path/"macro.sqlite";_macro_db(path)
    payload=json.dumps({"source_listing_bootstrap":False,"forward_signal_timely":True})
    _insert(path,(1,"a","ea","aaa_cpi",'["AAA"]',"2026M07","year_percent_change",3.0,2.0,
                  "2026-08-16T19:01:00Z","a","https://a",1,1,"ha",payload))
    _insert(path,(2,"b","eb","bbb_cpi",'["BBB"]',"2026M07","year_percent_change",1.0,1.5,
                  "2026-08-16T19:02:00Z","b","https://b",1,1,"hb",payload))
    config={"cohort_start_utc":"2026-08-16T19:00:00Z","eligible_unit":"year_percent_change",
            "eligible_series_by_currency":{"AAA":["aaa_cpi"],"BBB":["bbb_cpi"]},
            "maximum_observation_age_sec":86400,
            "minimum_absolute_acceleration_differential_pct_points":.25}
    events=worker.load_prospective_cpi_events(path,["AAA_BBB"],config,
            dt.datetime(2026,8,16,20,tzinfo=UTC))
    assert len(events)==1
    assert events[0]["acceleration_differential"]==pytest.approx(1.5)
    assert events[0]["predicted_side"]=="long"
    assert events[0]["factor_episode_id"].startswith("direct_cpi_release_")


def test_bootstrap_or_precohort_observation_is_ineligible(tmp_path: Path):
    path=tmp_path/"macro.sqlite";_macro_db(path)
    bootstrap=json.dumps({"source_listing_bootstrap":True,"forward_signal_timely":True})
    direct=json.dumps({"source_listing_bootstrap":False,"forward_signal_timely":True})
    _insert(path,(1,"a","ea","aaa_cpi",'["AAA"]',"2026M07","year_percent_change",3.0,2.0,
                  "2026-08-16T18:59:00Z","a","https://a",1,1,"ha",direct))
    _insert(path,(2,"b","eb","bbb_cpi",'["BBB"]',"2026M07","year_percent_change",1.0,1.5,
                  "2026-08-16T19:02:00Z","b","https://b",1,1,"hb",bootstrap))
    config={"cohort_start_utc":"2026-08-16T19:00:00Z","eligible_unit":"year_percent_change",
            "eligible_series_by_currency":{"AAA":["aaa_cpi"],"BBB":["bbb_cpi"]},
            "maximum_observation_age_sec":86400,
            "minimum_absolute_acceleration_differential_pct_points":.25}
    assert worker.load_prospective_cpi_events(path,["AAA_BBB"],config,
            dt.datetime(2026,8,16,20,tzinfo=UTC))==[]


def test_forecast_and_maturity_are_shadow_only(tmp_path: Path):
    observed=dt.datetime(2026,8,16,20,tzinfo=UTC)
    config={"contract_id":"test_direct_cpi","maximum_entry_spread_pips":3,
            "technical_context_horizon_sec":1800,"maximum_technical_age_sec":1800}
    events=[{"event_id":"event","factor_episode_id":"factor","pair":"AAA_BBB",
             "base_currency":"AAA","quote_currency":"BBB","signal_utc":worker.iso(observed),
             "base_acceleration":1.0,"quote_acceleration":0.0,"acceleration_differential":1.0,
             "predicted_side":"long","base_observation":{},"quote_observation":{},
             "driver_currency":"AAA"}]
    quotes={"AAA_BBB":{"bid":1.0,"ask":1.0002,"mid":1.0001,"pip":.0001,
                       "spread_pips":2.0,"time":worker.iso(observed),"fresh":True}}
    db=worker.open_database(tmp_path/"evidence.sqlite")
    assert worker.issue_forecasts(db,events,quotes,observed,config,None,None)==1
    payload=json.loads(db.execute("SELECT payload_json FROM forecasts").fetchone()[0])
    assert payload["research_only"] is True and payload["execution_eligible"] is False
    later=observed+dt.timedelta(days=7)
    later_quotes={"AAA_BBB":{"bid":1.0010,"ask":1.0012,"mid":1.0011,"pip":.0001,
                             "spread_pips":2.0,"time":worker.iso(later),"fresh":True}}
    sampled,matured=worker.sample_and_mature(db,later_quotes,604800)
    assert (sampled,matured)==(1,1)
    rule,flip=db.execute("SELECT rule_after_cost_pips,flipped_after_cost_pips FROM outcomes").fetchone()
    assert rule==pytest.approx(8.0) and flip==pytest.approx(-12.0)
    db.close()
