import datetime as dt
import json
import sqlite3
from pathlib import Path

import pytest

import oanda_rate_relative_strength_prospective as worker


UTC=dt.timezone.utc


def test_arm_contract_keeps_alignment_and_conflict_separate():
    assert worker.arm_for("long",{"direction":0})=="macro_only_technical_unavailable"
    assert worker.arm_for("long",{"direction":1})=="macro_technical_aligned"
    assert worker.arm_for("long",{"direction":-1})=="macro_technical_conflicted"


def test_issue_and_exact_maturity_are_shadow_only(tmp_path:Path):
    config={
        "contract_id":"test_rate_relative_h4","minimum_absolute_change_differential_bps":5,
        "maximum_signal_age_sec":86400,"maximum_entry_spread_pips":5,
        "technical_context_horizon_sec":1800,"maximum_technical_age_sec":1800,
    }
    observed=dt.datetime(2026,8,16,12,tzinfo=UTC)
    events=[{
        "event_id":"event-1","factor_episode_id":"factor-date","pair":"AAA_BBB",
        "base_currency":"AAA","quote_currency":"BBB","rate_date":"2026-08-16",
        "signal_utc":worker.iso(observed-dt.timedelta(minutes=1)),
        "base_observation":{},"quote_observation":{},"base_change_bps":5.0,
        "quote_change_bps":-1.0,"change_differential_bps":6.0,"predicted_side":"long",
    }]
    quotes={"AAA_BBB":{"bid":1.0,"ask":1.0002,"mid":1.0001,"pip":.0001,
                       "spread_pips":2.0,"time":worker.iso(observed),"fresh":True}}
    db=worker.open_database(tmp_path/"evidence.sqlite")
    issued=worker.issue_forecasts(db,events,quotes,observed,config,None,None)
    assert issued==1
    payload=json.loads(db.execute("SELECT payload_json FROM forecasts").fetchone()[0])
    assert payload["research_only"] is True
    assert payload["execution_eligible"] is False
    later=observed+dt.timedelta(hours=4)
    later_quotes={"AAA_BBB":{"bid":1.0010,"ask":1.0012,"mid":1.0011,"pip":.0001,
                             "spread_pips":2.0,"time":worker.iso(later),"fresh":True}}
    sampled,matured=worker.sample_and_mature(db,later_quotes,14400)
    assert sampled==1 and matured==1
    row=db.execute("SELECT exact_horizon,rule_after_cost_pips,flipped_after_cost_pips FROM outcomes").fetchone()
    assert row[0]==1
    assert row[1]==pytest.approx(8.0)
    assert row[2]==pytest.approx(-12.0)
    db.close()


def test_bootstrap_or_single_leg_rows_cannot_form_event(tmp_path:Path):
    path=tmp_path/"rates.sqlite"
    db=sqlite3.connect(path)
    db.execute("""CREATE TABLE daily_rate_observations(
      observation_id TEXT,cohort_id TEXT,currency TEXT,rate_date TEXT,rate_pct REAL,
      first_seen_utc TEXT,prospective_eligible INTEGER,bootstrap_current_view INTEGER,
      source_id TEXT,provider TEXT,source_contract_json TEXT,version INTEGER)""")
    contract=json.dumps({"comparison_group":"two_year_market_rate_context"})
    rows=[
      ("a0","ca","AAA","2026-08-01",1.0,"2026-08-01T12:00:00Z",0,1,"a","A",contract,1),
      ("a1","ca","AAA","2026-08-02",1.1,"2026-08-02T12:00:00Z",1,0,"a","A",contract,1),
      ("b0","cb","BBB","2026-08-01",2.0,"2026-08-01T12:00:00Z",0,1,"b","B",contract,1),
      ("b1","cb","BBB","2026-08-02",1.9,"2026-08-02T12:00:00Z",0,1,"b","B",contract,1),
    ]
    db.executemany("INSERT INTO daily_rate_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",rows);db.commit();db.close()
    assert worker.load_prospective_rate_events(path,["AAA_BBB"],"two_year_market_rate_context")==[]
