from __future__ import annotations

import datetime as dt
import json
import sqlite3
from pathlib import Path

from trad import oanda_source_governance as governance
from trad import oanda_source_governance_news_fast_lane as fast
from trad.oanda_project_integrity_audit import (
    news_source_governance_fast_lane_integrity,
)


def _proof_payload(**values: object) -> dict[str, object]:
    return {
        "headline": "Fresh market story",
        "currencies": ["USD", "ZAR"],
        "collector_contract_id": governance.LOCAL_NEWS_COLLECTOR_CONTRACT_ID,
        "collector_cohort_id": governance.LOCAL_NEWS_COLLECTOR_COHORT_ID,
        "observation_time_contract_id": governance.OBSERVATION_TIME_CONTRACT_ID,
        "observation_clock_trusted": True,
        "observation_clock_source": "test_clock",
        **values,
    }


def _news_database(path: Path, *, first_seen: str, last_seen: str) -> None:
    connection = sqlite3.connect(path)
    connection.execute(
        """CREATE TABLE articles (
            event_id TEXT,source_id TEXT,source_name TEXT,source_kind TEXT,
            source_quality REAL,source_verified INTEGER,published_utc TEXT,
            first_seen_utc TEXT,last_seen_utc TEXT,headline TEXT,summary TEXT,
            category TEXT,scope TEXT,currencies_json TEXT,
            generic_sentiment_score REAL,directional_confidence REAL,
            severity REAL,movement_potential TEXT,duplicate_count INTEGER,
            payload_json TEXT
        )"""
    )
    connection.execute(
        "INSERT INTO articles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "article-one",
            "news-one",
            "News One",
            "rss",
            0.7,
            0,
            first_seen,
            first_seen,
            last_seen,
            "Fresh market story",
            "",
            "risk_off_geopolitical_or_financial",
            "all_pairs",
            '["USD","ZAR"]',
            0.0,
            0.5,
            0.5,
            "MEDIUM",
            0,
            json.dumps(_proof_payload()),
        ),
    )
    connection.commit()
    connection.close()


def _registry(path: Path) -> None:
    source = {
        "source_id": "news-one",
        "name": "News One",
        "kind": "rss",
        "source_role": "aggregator_discovery",
        "currencies": ["USD", "ZAR"],
    }
    contract = governance.contract_for(source)
    connection = governance.connect_registry(path)
    connection.execute(
        "INSERT INTO source_contracts VALUES (?,?,?,?,?,?)",
        (
            contract["source_contract_id"],
            source["source_id"],
            contract["source_cohort_id"],
            fast.ACTIVATED_UTC.isoformat(),
            governance.stable_hash(contract),
            json.dumps(contract),
        ),
    )
    connection.commit()
    connection.close()


def _paths(tmp_path: Path) -> dict:
    paths = dict(news_database=tmp_path/"news.sqlite",database_path=tmp_path/"registry.sqlite",state_path=tmp_path/"state.json")
    _registry(paths["database_path"])
    seen=(fast.ACTIVATED_UTC+dt.timedelta(seconds=1)).isoformat()
    _news_database(paths["news_database"],first_seen=seen,last_seen=seen)
    return paths


def _append(news: Path, event_id: str, seconds: int, *, connection=None) -> None:
    owns=connection is None
    connection=connection or sqlite3.connect(news)
    row=list(connection.execute("SELECT * FROM articles LIMIT 1").fetchone())
    row[0]=event_id
    row[6]=row[7]=row[8]=(fast.ACTIVATED_UTC+dt.timedelta(seconds=seconds)).isoformat()
    connection.execute("INSERT INTO articles VALUES ("+",".join("?"*len(row))+")",row)
    if owns:
        connection.commit()
        connection.close()


def _checked(result: dict, paths: dict, seconds: int) -> dict:
    return news_source_governance_fast_lane_integrity(result,database_path=paths["database_path"],cutoff_epoch=(fast.ACTIVATED_UTC+dt.timedelta(seconds=seconds)).timestamp())


def test_waiting_does_not_open_input_or_registry(tmp_path: Path) -> None:
    result=fast.run(news_database=tmp_path/"missing.sqlite",database_path=tmp_path/"registry.sqlite",state_path=tmp_path/"state.json",now=fast.ACTIVATED_UTC-dt.timedelta(seconds=1))
    assert result["status"]=="waiting_for_activation"
    assert result["can_place_orders"] is False
    assert not (tmp_path/"registry.sqlite").exists()
    assert _checked(result,{"database_path":tmp_path/"registry.sqlite"},-1)["ok"]


def test_mapping_requires_consumer_observation_and_is_immutable(tmp_path: Path) -> None:
    import pytest
    paths=_paths(tmp_path)
    result=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    assert result["status"]=="ok" and result["input_row_count"]==result["new_receipt_count"]==1
    assert result["transaction_committed"] is True
    assert result["next_scan_cursor_rowid"]==1
    assert _checked(result,paths,2)["ok"]
    connection=sqlite3.connect(paths["database_path"])
    row=connection.execute("SELECT source_event_id,mapping_visible_utc,operational_effective_from_utc,consumer_first_observation_required FROM source_events_fast_mapped_v3").fetchone()
    assert row[1]==result["mapping_visible_utc"] and row[2] is None and row[3]==1
    assert connection.execute("SELECT COUNT(*) FROM news_fast_lane_import_receipts").fetchone()[0]==0
    for table in ("news_fast_lane_batches_v3","news_fast_lane_mappings_v3","news_fast_lane_visibility_v3"):
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(f"DELETE FROM {table}")
        connection.rollback()
    connection.close()
    ack=fast.observe_mapping(paths["database_path"],row[0],observer_id="fixture-consumer",clock=lambda:fast.ACTIVATED_UTC+dt.timedelta(seconds=5))
    assert not fast.acknowledgement_usable_at(ack,fast.ACTIVATED_UTC+dt.timedelta(seconds=4))
    assert fast.acknowledgement_usable_at(ack,fast.ACTIVATED_UTC+dt.timedelta(seconds=5))
    assert not fast.acknowledgement_usable_at(None,fast.ACTIVATED_UTC+dt.timedelta(seconds=5))


def test_empty_cycle_is_valid_without_scanning_old_rows(tmp_path: Path) -> None:
    paths=_paths(tmp_path)
    first=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    second=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=32))
    assert second["input_row_count"]==second["new_receipt_count"]==0
    assert second["total_receipt_count"]==1 and _checked(second,paths,32)["ok"]
    assert first["committed_batch_seq"]==second["committed_batch_seq"]
    connection=sqlite3.connect(paths["news_database"])
    connection.execute("DELETE FROM articles")
    connection.commit();connection.close()
    empty_paths={**paths,"database_path":tmp_path/"empty_registry.sqlite","state_path":tmp_path/"empty_state.json"}
    _registry(empty_paths["database_path"])
    empty=fast.run(**empty_paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=33))
    assert empty["input_row_count"]==empty["total_receipt_count"]==0
    assert _checked(empty,empty_paths,33)["ok"]


def test_progress_and_both_retry_paths_preserve_committed_state_and_cursor(tmp_path: Path,monkeypatch) -> None:
    paths=_paths(tmp_path)
    first=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    original_bytes=paths["state_path"].read_bytes()
    original_atomic=fast.atomic_json
    observed=[]
    def capture(path,value):
        if value["status"]=="building":
            assert path==fast.progress_path(paths["state_path"])
            assert paths["state_path"].read_bytes()==original_bytes
            assert _checked(first,paths,32)["ok"]
            observed.append(True)
        return original_atomic(path,value)
    with monkeypatch.context() as patch:
        patch.setattr(fast,"atomic_json",capture)
        second=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=32))
    assert observed
    for target,status in (("article_events","input_temporarily_unavailable"),("connect_registry","retryable_database_error")):
        before=paths["state_path"].read_bytes()
        def unavailable(*args,**kwargs): raise sqlite3.OperationalError("fixture locked")
        with monkeypatch.context() as patch:
            patch.setattr(fast.governance,target,unavailable)
            failed=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=62))
        assert failed["status"]==status and paths["state_path"].read_bytes()==before
        assert fast.read_json(fast.progress_path(paths["state_path"]))==failed
        recovered=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=92))
        assert recovered["scan_cursor_rowid"]==second["next_scan_cursor_rowid"]==1
        assert recovered["input_row_count"]==0


def test_checkpoint_survives_missing_json_and_pending_attestation(tmp_path: Path,monkeypatch) -> None:
    paths=_paths(tmp_path)
    original=fast._attest_pending
    def unavailable_after_mapping(connection,path,now):
        if connection.execute("SELECT COUNT(*) FROM news_fast_lane_batches_v3").fetchone()[0]:
            raise sqlite3.OperationalError("fixture attestation failure")
        return original(connection,path,now)
    with monkeypatch.context() as patch:
        patch.setattr(fast,"_attest_pending",unavailable_after_mapping)
        failed=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    assert failed["status"]=="retryable_database_error"
    assert not paths["state_path"].exists()
    recovered=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=32))
    assert recovered["input_row_count"]==0 and recovered["new_receipt_count"]==1
    assert recovered["scan_cursor_rowid"]==1 and _checked(recovered,paths,32)["ok"]
    paths["state_path"].unlink()
    restarted=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=62))
    assert restarted["scan_cursor_rowid"]==1 and restarted["input_row_count"]==0


def test_late_wal_commit_is_seen_after_empty_scan(tmp_path: Path) -> None:
    paths=_paths(tmp_path)
    first=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    writer=sqlite3.connect(paths["news_database"])
    writer.execute("PRAGMA journal_mode=WAL")
    _append(paths["news_database"],"late-commit",20,connection=writer)
    during=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=32))
    assert during["input_row_count"]==0 and during["next_scan_cursor_rowid"]==1
    writer.commit();writer.close()
    after=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=62))
    assert after["input_row_count"]==after["new_receipt_count"]==1
    assert after["total_receipt_count"]==2 and after["next_scan_cursor_rowid"]==2
    assert _checked(first,paths,2)["ok"] and _checked(after,paths,62)["ok"]
    (tmp_path/"wal_reproduction.json").write_text(json.dumps({
        "baseline_cursor":first["next_scan_cursor_rowid"],
        "during_uncommitted_write_cursor":during["next_scan_cursor_rowid"],
        "after_commit_cursor":after["next_scan_cursor_rowid"],
        "after_commit_new_receipts":after["new_receipt_count"],
        "after_commit_total_receipts":after["total_receipt_count"],
        "integrity_ok":_checked(after,paths,62)["ok"],
    },indent=2),encoding="utf-8")


def test_failed_mapping_transaction_rolls_back_and_releases_writer(tmp_path: Path,monkeypatch) -> None:
    paths=_paths(tmp_path)
    first=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    before=paths["state_path"].read_bytes()
    _append(paths["news_database"],"second",3)
    _append(paths["news_database"],"third",4)
    original=fast.governance.insert_source_event
    def fail_third(connection,event):
        if event["provider_event_id"]=="third":
            raise sqlite3.OperationalError("fixture partial batch failure")
        return original(connection,event)
    with monkeypatch.context() as patch:
        patch.setattr(fast.governance,"insert_source_event",fail_third)
        failed=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=5))
    assert failed["status"]=="retryable_database_error"
    assert failed["scan_cursor_rowid"]==1 and paths["state_path"].read_bytes()==before
    connection=sqlite3.connect(paths["database_path"],timeout=0)
    connection.execute("BEGIN IMMEDIATE")
    assert connection.execute("SELECT COUNT(*) FROM source_events").fetchone()[0]==1
    connection.rollback();connection.close()
    recovered=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=6))
    assert recovered["input_row_count"]==recovered["new_receipt_count"]==2
    assert recovered["total_receipt_count"]==3 and _checked(recovered,paths,6)["ok"]


def test_pending_later_batch_preserves_last_completed_integrity_snapshot(tmp_path: Path,monkeypatch) -> None:
    paths=_paths(tmp_path)
    first=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    before=paths["state_path"].read_bytes()
    _append(paths["news_database"],"pending",3)
    original=fast._attest_pending
    def refuse_new_pending(connection,path,now):
        if connection.execute("SELECT MAX(batch_seq) FROM news_fast_lane_batches_v3").fetchone()[0]>1:
            raise sqlite3.OperationalError("fixture failed attestation commit")
        return original(connection,path,now)
    with monkeypatch.context() as patch:
        patch.setattr(fast,"_attest_pending",refuse_new_pending)
        failed=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=4))
    assert failed["status"]=="retryable_database_error"
    assert failed["committed_batch_seq"]==2 and failed["scan_cursor_rowid"]==2
    assert paths["state_path"].read_bytes()==before
    checked=_checked(first,paths,4)
    assert checked["ok"] and checked["receipt_count"]==1
    assert checked["receipts_after_state_cutoff"]==1
    recovered=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=5))
    assert recovered["new_receipt_count"]==1 and recovered["input_row_count"]==0
    assert _checked(recovered,paths,5)["ok"]


def test_future_row_and_batch_limit_do_not_skip_committed_rows(tmp_path: Path,monkeypatch) -> None:
    paths=_paths(tmp_path)
    _append(paths["news_database"],"future",20)
    _append(paths["news_database"],"after-future",3)
    first=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=5))
    assert first["next_scan_cursor_rowid"]==1 and first["future_row_deferred"] is True
    monkeypatch.setattr(fast,"MAXIMUM_INPUT_ROWS",1)
    second=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=25))
    third=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=26))
    assert second["next_scan_cursor_rowid"]==2 and third["next_scan_cursor_rowid"]==3
    assert third["total_receipt_count"]==3 and _checked(third,paths,26)["ok"]


def test_pruned_reused_anchor_fails_closed_without_rewind(tmp_path: Path) -> None:
    paths=_paths(tmp_path)
    fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    before=paths["state_path"].read_bytes()
    connection=sqlite3.connect(paths["news_database"])
    connection.execute("UPDATE articles SET event_id='replacement' WHERE rowid=1")
    connection.commit();connection.close()
    failed=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=32))
    assert failed["status"]=="input_checkpoint_reconciliation_required"
    assert failed["scan_cursor_rowid"]==1 and paths["state_path"].read_bytes()==before


def test_old_first_seen_refresh_is_not_relabelled_prospective(tmp_path: Path) -> None:
    paths=_paths(tmp_path)
    connection=sqlite3.connect(paths["news_database"])
    connection.execute("UPDATE articles SET first_seen_utc=?",((fast.ACTIVATED_UTC-dt.timedelta(days=1)).isoformat(),))
    connection.commit();connection.close()
    result=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    assert result["rejected_preactivation_count"]==1 and result["new_receipt_count"]==0
    assert result["next_scan_cursor_rowid"]==1 and _checked(result,paths,2)["ok"]


def test_delayed_mapping_and_attestation_commits_require_actual_consumer_read(tmp_path: Path,monkeypatch) -> None:
    import types
    paths=_paths(tmp_path)
    clock={"now":fast.ACTIVATED_UTC+dt.timedelta(seconds=2)}
    original=fast.governance.connect_registry
    observations={}
    class VirtualDatetime(dt.datetime):
        @classmethod
        def now(cls,tz=None): return clock["now"]
    class DelayCommits:
        def __init__(self,connection): self.connection=connection;self.pending=None
        def __getattr__(self,name): return getattr(self.connection,name)
        def execute(self,sql,*args):
            value=self.connection.execute(sql,*args)
            if sql.startswith("INSERT OR IGNORE INTO news_fast_lane_mappings_v3"):
                self.pending="mapping"; observations["event_id"]=args[0][0]
            if sql.startswith("INSERT OR IGNORE INTO news_fast_lane_visibility_v3"):
                self.pending="visibility"
            return value
        def commit(self):
            if self.pending:
                kind=self.pending
                clock["now"]+=dt.timedelta(seconds=11)
                ack=fast.observe_mapping(paths["database_path"],observations["event_id"],observer_id="fixture",clock=lambda:clock["now"])
                observations[kind+"_precommit_ack"]=ack
                self.connection.commit()
                observations[kind+"_commit_utc"]=clock["now"]
                self.pending=None
            else: self.connection.commit()
    monkeypatch.setattr(fast,"dt",types.SimpleNamespace(datetime=VirtualDatetime,timezone=dt.timezone,timedelta=dt.timedelta))
    monkeypatch.setattr(fast.governance,"connect_registry",lambda path:DelayCommits(original(path)))
    result=fast.run(**paths)
    assert result["status"]=="ok"
    assert observations["mapping_precommit_ack"] is None
    assert observations["visibility_precommit_ack"] is None
    assert fast.parse_time(result["mapping_visible_utc"])>=observations["mapping_commit_utc"]
    assert fast.parse_time(result["mapping_visible_utc"])<observations["visibility_commit_utc"]
    ack=fast.observe_mapping(paths["database_path"],observations["event_id"],observer_id="fixture",clock=lambda:clock["now"])
    assert fast.parse_time(ack["consumer_observed_utc"])>=observations["visibility_commit_utc"]
    assert not fast.acknowledgement_usable_at(ack,observations["visibility_commit_utc"]-dt.timedelta(microseconds=1))
    assert fast.acknowledgement_usable_at(ack,observations["visibility_commit_utc"])
    assert _checked(result,paths,24)["ok"]
    (tmp_path/"commit_reproduction.json").write_text(json.dumps({
        "mapping_commit_utc":observations["mapping_commit_utc"].isoformat(),
        "mapping_visible_utc":result["mapping_visible_utc"],
        "attestation_commit_utc":observations["visibility_commit_utc"].isoformat(),
        "mapping_precommit_observation":observations["mapping_precommit_ack"],
        "attestation_precommit_observation":observations["visibility_precommit_ack"],
        "consumer_acknowledgement":ack,
        "pre_observation_proof_rejected":not fast.acknowledgement_usable_at(ack,observations["visibility_commit_utc"]-dt.timedelta(microseconds=1)),
        "actual_sleep_seconds":0,"virtual_delay_each_commit_seconds":11,
        "integrity_ok":_checked(result,paths,24)["ok"],
    },indent=2),encoding="utf-8")


def test_integrity_rejects_tamper_stale_and_missing_controls(tmp_path: Path) -> None:
    paths=_paths(tmp_path)
    result=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    assert _checked(result,paths,2)["ok"]
    for bad in ({**result,"can_authorize":True},{**result,"total_receipt_count":2},{**result,"consumer_first_observation_required":False},{**result,"next_scan_cursor_rowid":100}):
        assert not _checked(bad,paths,2)["ok"]
    assert not _checked(result,paths,153)["ok"]
    connection=sqlite3.connect(paths["database_path"])
    connection.execute("DROP TRIGGER news_fast_lane_mappings_v3_no_update")
    connection.execute("UPDATE news_fast_lane_mappings_v3 SET source_event_id='missing',input_first_seen_utc=?",((fast.ACTIVATED_UTC-dt.timedelta(seconds=1)).isoformat(),))
    connection.commit();connection.close()
    checked=_checked(result,paths,2)
    assert not checked["ok"] and not checked["append_only_triggers_present"]
    assert checked["preactivation_receipts"]==1 and checked["orphan_or_mismatched_source_events"]==1


def test_v2_receipts_and_view_stay_frozen(tmp_path: Path) -> None:
    paths=_paths(tmp_path)
    fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=2))
    connection=sqlite3.connect(paths["database_path"])
    sql=connection.execute("SELECT sql FROM sqlite_master WHERE name='source_events_fast_mapped_v2'").fetchone()[0]
    assert fast.LEGACY_CONTRACT_ID in sql and fast.CONTRACT_ID not in sql
    mapping=connection.execute("SELECT source_event_id,source_id,provider_event_id,raw_payload_sha256,input_first_seen_utc,input_last_seen_utc FROM news_fast_lane_mappings_v3").fetchone()
    legacy=("legacy-fixture",*mapping,(fast.ACTIVATED_UTC+dt.timedelta(seconds=2)).isoformat(),0,fast.LEGACY_CONTRACT_ID,fast.LEGACY_COHORT_ID,"2026-09-01T13:15:00+00:00",1,0,0)
    connection.execute("INSERT INTO news_fast_lane_import_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",legacy)
    connection.commit();connection.close()
    result=fast.run(**paths,now=fast.ACTIVATED_UTC+dt.timedelta(seconds=32))
    connection=sqlite3.connect(paths["database_path"])
    assert connection.execute("SELECT * FROM news_fast_lane_import_receipts").fetchone()==legacy
    assert connection.execute("SELECT sql FROM sqlite_master WHERE name='source_events_fast_mapped_v2'").fetchone()[0]==sql
    connection.close()
    assert result["retained_legacy_receipt_count"]==1 and _checked(result,paths,32)["ok"]


def test_supervisor_runs_fast_lane_separately_from_full_reconciliation() -> None:
    supervisor=(fast.ROOT/"oanda_always_on_supervisor.ps1").read_text(encoding="utf-8")
    assert '-Name "source_governance_news_fast_lane"' in supervisor
    assert '"oanda_source_governance_news_fast_lane.py"' in supervisor
    assert '"--interval-sec", "30"' in supervisor
    assert '-Name "source_governance"' in supervisor
    assert '"--interval-sec", "1800"' in supervisor
    assert "This lane is research-only and cannot route or authorize" in supervisor
