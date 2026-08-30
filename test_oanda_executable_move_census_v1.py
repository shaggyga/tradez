import datetime as dt
import json
from pathlib import Path
import sqlite3

import pytest

import oanda_executable_move_census_v1 as census

UTC=dt.timezone.utc


def _supervisor_managed_block(name: str, next_name: str) -> str:
    supervisor = (Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )
    start = supervisor.index(f'-Name "{name}"')
    end = supervisor.index(f'-Name "{next_name}"', start + 1)
    return supervisor[start:end]


def test_supervisor_registers_isolated_research_census_and_verifier_workers() -> None:
    producer = _supervisor_managed_block(
        "executable_move_census_v1", "executable_move_census_verifier_v1"
    )
    verifier = _supervisor_managed_block(
        "executable_move_census_verifier_v1", "major_move_gap_census"
    )
    assert producer.count('-Needle "oanda_executable_move_census_v1.py"') == 1
    assert verifier.count('-Needle "oanda_executable_move_census_v1_verifier.py"') == 1
    for block, script, heartbeat, schema in (
        (
            producer,
            "oanda_executable_move_census_v1.py",
            "executable_move_census_heartbeat_v1.json",
            "executable_move_census_heartbeat_v1",
        ),
        (
            verifier,
            "oanda_executable_move_census_v1_verifier.py",
            "executable_move_census_verifier_latest_v1.json",
            "executable_move_census_verifier_v1",
        ),
    ):
        assert f'(Join-Path $Trad "{script}")' in block
        assert '-Executable $Python' in block
        assert '-PriorityClass "BelowNormal"' in block
        assert '"--interval-sec", "30"' in block
        assert '"--duration-sec", "$ChildDurationSec"' in block
        assert f'(Join-Path $State "{heartbeat}")' in block
        assert 'ExpectedJsonField = "schema_version"' in block
        assert f'ExpectedJsonValue = "{schema}"' in block
    assert "research-only observations" in (Path(__file__).resolve().parent / "oanda_always_on_supervisor.ps1").read_text(
        encoding="utf-8"
    )


def config(tmp_path: Path) -> Path:
    value=json.loads(census.CONFIG.read_text())
    value["activation_utc"]="2026-08-30T21:05:00Z"
    path=tmp_path/"config.json"; path.write_text(json.dumps(value,sort_keys=True))
    return path


def quotes(cfg, when: dt.datetime, *, delta: float=0.0, missing: str="") -> bytes:
    rows={}
    for instrument,pip in cfg["instruments"].items():
        if instrument==missing: continue
        mid=1.0+delta
        rows[instrument]={"bid":mid-0.0001,"ask":mid+0.0001,"pip":pip,"source":"stream","time":census.iso(when)}
    return json.dumps({"schema_version":2,"producer":"practice_007_fast_executor_price_stream","generated_utc":census.iso(when),"quote_count":len(rows),"quotes":rows},sort_keys=True).encode()


def insert_at(db, cfg, when, raw):
    frame,rows=census.build_frame(cfg,raw,when,when,when)
    assert census.insert_frame(db,frame,rows,cfg,precommit=when)


def test_frozen_universe_and_reviewed_hkd_jpy_pip(tmp_path):
    cfg,_=census.load_config(config(tmp_path))
    assert len(cfg["instruments"])==68
    assert cfg["instruments"]["HKD_JPY"]==0.0001
    assert list(cfg["instruments"])==sorted(cfg["instruments"])


def test_full_136_terminal_math_and_one_slippage(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,10,10,tzinfo=UTC)
    insert_at(db,cfg,t0,quotes(cfg,t0))
    t1=t0+dt.timedelta(minutes=1)
    insert_at(db,cfg,t1,quotes(cfg,t1,delta=.001))
    payload=census.build_latest(db,cfg,t1)
    block=next(x for x in payload["horizons"] if x["horizon_min"]==1)
    assert block["row_count"]==136 and block["valid_count"]==136
    long=next(x for x in block["rows"] if x["instrument"]=="AUD_CAD" and x["side"]=="long")
    short=next(x for x in block["rows"] if x["instrument"]=="AUD_CAD" and x["side"]=="short")
    assert long["raw_executable_pips"]==8.0
    assert long["net_pips"]==7.75 and long["state"]=="cleared"
    assert short["raw_executable_pips"]==-12.0
    assert short["net_pips"]==-12.25 and short["state"]=="not_cleared"
    db.close()


def test_missing_pair_never_shrinks_denominator(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,10,10,tzinfo=UTC)
    insert_at(db,cfg,t0,quotes(cfg,t0,missing="EUR_USD"))
    insert_at(db,cfg,t0+dt.timedelta(minutes=1),quotes(cfg,t0+dt.timedelta(minutes=1)))
    block=census.build_latest(db,cfg,t0+dt.timedelta(minutes=1))["horizons"][0]
    assert block["row_count"]==136 and block["invalid_count"]==2
    assert {x["side"] for x in block["rows"] if x["instrument"]=="EUR_USD"}=={"long","short"}
    db.close()


def test_gap_is_not_hidden_by_older_complete_window(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,10,10,tzinfo=UTC)
    insert_at(db,cfg,t0,quotes(cfg,t0)); insert_at(db,cfg,t0+dt.timedelta(minutes=1),quotes(cfg,t0+dt.timedelta(minutes=1)))
    insert_at(db,cfg,t0+dt.timedelta(minutes=3),quotes(cfg,t0+dt.timedelta(minutes=3)))
    block=census.build_latest(db,cfg,t0+dt.timedelta(minutes=3))["horizons"][0]
    assert block["entry_frame_id"] is None and block["exit_frame_id"] is not None
    assert block["invalid_count"]==136
    db.close()


def test_append_only_and_manifest_identity(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); path=tmp_path/"x.sqlite"; db=census.open_db(path,cfg,raw_cfg)
    with pytest.raises(sqlite3.IntegrityError): db.execute("UPDATE cohort_manifest SET cohort_id='x'")
    db.close()
    changed=dict(cfg); changed["slippage_pips"]=9
    with pytest.raises(RuntimeError): census.open_db(path,changed,raw_cfg)


def test_closed_and_special_hours_are_explicit():
    cfg,_=census.load_config()
    sunday=dt.datetime(2026,8,30,21,6,tzinfo=UTC) # 17:06 NY
    assert census.market_open(sunday)
    assert census.instrument_market_open("EUR_USD",sunday,cfg)
    assert not census.instrument_market_open("NZD_USD",sunday,cfg)


def test_review_queue_retains_all_arms_and_dedupes_cases(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,10,10,tzinfo=UTC)
    insert_at(db,cfg,t0,quotes(cfg,t0)); insert_at(db,cfg,t0+dt.timedelta(minutes=1),quotes(cfg,t0+dt.timedelta(minutes=1),delta=.001))
    census.reconcile_window_evaluations(db,cfg,t0+dt.timedelta(minutes=2)); payload=census.build_latest(db,cfg,t0+dt.timedelta(minutes=1)); census.populate_review_queue(db,payload,cfg)
    total=payload["review_queue"]["total_cleared_arm_observations"]
    assert total > 0
    assert payload["review_queue"]["distinct_factor_episode_cases"] == 0  # episode not final until end + 60m
    assert db.execute("SELECT COUNT(*) FROM window_evaluations").fetchone()[0] > 0
    db.close()


def test_hindsight_path_clear_is_distinct_from_terminal(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,20,10,tzinfo=UTC)
    for minute,delta in enumerate((0,.001,0,0,0,0)):
        stamp=t0+dt.timedelta(minutes=minute); insert_at(db,cfg,stamp,quotes(cfg,stamp,delta=delta))
    block=next(x for x in census.build_latest(db,cfg,t0+dt.timedelta(minutes=5))["horizons"] if x["horizon_min"]==5)
    row=next(x for x in block["rows"] if x["instrument"]=="AUD_CAD" and x["side"]=="long")
    assert row["state"]=="not_cleared" and row["path_cleared"] is True
    assert row["first_clear_min"]==1 and row["path_max_net_pips"]==7.75
    assert len(row["path_points"])==5
    db.close()


def test_snapshot_extra_key_invalidates_without_shrinking(tmp_path):
    cfg,_=census.load_config(config(tmp_path)); t0=dt.datetime(2026,8,30,21,20,10,tzinfo=UTC)
    payload=json.loads(quotes(cfg,t0)); payload["quotes"]["FAKE_USD"]={"bid":1,"ask":2,"pip":.0001,"time":census.iso(t0)}; payload["quote_count"]=69
    _,rows=census.build_frame(cfg,json.dumps(payload).encode(),t0,t0,t0)
    assert len(rows)==68 and all(row["state"]=="invalid" for row in rows)


def test_atomic_replace_transient_retry_and_cleanup(tmp_path,monkeypatch):
    target=tmp_path/"state.json"; real=census.os.replace; calls={"n":0}
    def flaky(source,destination):
        calls["n"]+=1
        if calls["n"]<3: raise PermissionError(5,"busy")
        return real(source,destination)
    monkeypatch.setattr(census.os,"replace",flaky); monkeypatch.setattr(census.time,"sleep",lambda _:None)
    census.write_json_atomic(target,{"ok":True})
    assert json.loads(target.read_text())=={"ok":True} and calls["n"]==3
    assert not list(tmp_path.glob("*.tmp"))


def test_pre_activation_publishes_collecting_not_failure(tmp_path):
    cfg_path=config(tmp_path); cfg=json.loads(cfg_path.read_text()); quote_path=tmp_path/"quotes.json"
    when=dt.datetime(2026,8,30,21,4,0,tzinfo=UTC); quote_path.write_bytes(quotes(cfg,when)); output=tmp_path/"latest.json"
    result=census.run_once(config_path=cfg_path,quotes_path=quote_path,database_path=tmp_path/"db.sqlite",output_path=output,now=when)
    assert result["status"]=="collecting_pre_activation" and not (tmp_path/"db.sqlite").exists()


def test_dashboard_leads_with_fixed_executable_windows_only():
    html=(census.ROOT/"oanda_main_signal_dashboard.html").read_text(encoding="utf-8")
    assert "moverMode='exec5'" in html
    assert "exec1:'1 minute'" in html and "exec60:'60 minutes'" in html
    assert "Legacy velocity" not in html
    assert "Observed executable net path in pips" in html


def test_reconcile_cursor_does_not_lose_later_horizon(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,20,10,tzinfo=UTC)
    for minute in range(7):
        stamp=t0+dt.timedelta(minutes=minute); insert_at(db,cfg,stamp,quotes(cfg,stamp,delta=.0001*minute))
    census.reconcile_window_evaluations(db,cfg,t0+dt.timedelta(minutes=2))
    census.reconcile_window_evaluations(db,cfg,t0+dt.timedelta(minutes=7))
    assert db.execute("SELECT 1 FROM window_evaluations WHERE target_scheduled_utc=? AND declared_window_min=5",(census.iso((t0+dt.timedelta(minutes=5)).replace(second=0)),)).fetchone()
    db.close()


def test_wholly_missing_exit_has_durable_136_invalid_receipt(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    t0=dt.datetime(2026,8,30,21,20,10,tzinfo=UTC); insert_at(db,cfg,t0,quotes(cfg,t0))
    census.reconcile_window_evaluations(db,cfg,t0+dt.timedelta(minutes=2))
    target=census.iso((t0+dt.timedelta(minutes=1)).replace(second=0))
    row=db.execute("SELECT state,expected_side_count,invalid_count,exit_frame_id FROM window_evaluations WHERE target_scheduled_utc=? AND declared_window_min=1",(target,)).fetchone()
    assert tuple(row)==("terminal_invalid_missing_frame",136,136,None)
    columns={r[1] for r in db.execute("PRAGMA table_info(window_evaluations)")}
    assert "review_json" not in columns and "terminal_rows_sha256" in columns and "path_summary_rows_sha256" in columns
    db.close()


def test_finalized_empty_episode_has_explicit_receipt(tmp_path):
    cfg,raw_cfg=census.load_config(config(tmp_path)); db=census.open_db(tmp_path/"x.sqlite",cfg,raw_cfg)
    activation=census.parse_utc(cfg["activation_utc"]); episode=activation.replace(minute=0,second=0,microsecond=0)
    keys=[]
    for minute in range(5,15):
        entry=episode+dt.timedelta(minutes=minute)
        for horizon in cfg["horizons_min"]: keys.append((census.iso(entry+dt.timedelta(minutes=horizon)),horizon))
    empty_sha=census.sha(census.canonical([]))
    db.executemany("INSERT INTO window_evaluations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[(target,horizon,None,None,census.iso(episode+dt.timedelta(minutes=75)),"terminal_invalid_missing_frame",136,0,0,0,136,0,empty_sha,empty_sha,empty_sha,empty_sha) for target,horizon in keys]); db.commit()
    census.finalize_factor_episodes(db,cfg,episode+dt.timedelta(minutes=75))
    row=db.execute("SELECT expected_window_count,evaluated_window_count,summary_count,terminal_member_count,path_member_count FROM factor_episode_finalizations WHERE episode_utc=?",(census.iso(episode),)).fetchone()
    assert tuple(row)==(60,60,0,0,0)
    db.close()
