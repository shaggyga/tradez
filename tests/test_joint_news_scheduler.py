import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'trad'))
import oanda_joint_news_scheduler_v1 as scheduler
from test_joint_news_refresh_starvation import ControlledRunner


@pytest.mark.parametrize('due,completes,expected_news,expected_pair', [
    (True, True, 1, 0), (False, True, 0, 1), (True, False, 0, 0)])
def test_actual_successor_tick(due, completes, expected_news, expected_pair):
    runner = ControlledRunner(due=due, completes=completes)
    scheduler.ScheduledRunner.tick(runner)
    assert runner.news_dispatches == expected_news
    assert runner.pair_dispatches == expected_pair


def test_durable_receipts_have_original_and_operational_identity(tmp_path):
    receipt = scheduler.Receipts(tmp_path, {'registry_sha256': 'a' * 64}, clock=lambda: 123.)
    receipt.write('fit_completed', forecasts=[{'id': 'exact-id', 'sha256': 'b' * 64}])
    receipt.close()
    rows = [json.loads(s) for s in receipt.path.read_text().splitlines()]
    assert rows[0]['registry_sha256'] == 'a' * 64
    assert rows[1]['forecasts'][0]['id'] == 'exact-id'
    assert rows[0]['run_id'] == rows[1]['run_id']
    assert rows[0]['observed_epoch'] == rows[1]['observed_epoch'] == 123.


def test_only_new_rows_are_attributed_and_old_rows_unchanged(tmp_path, monkeypatch):
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE forecasts(id TEXT,sha TEXT,reference REAL,target REAL)')
    db.execute("INSERT INTO forecasts VALUES('old','old-sha',1,2)")
    old = db.execute('SELECT * FROM forecasts').fetchall()
    ledger = SimpleNamespace(db=db, contract_hash='original-contract')
    receipts = scheduler.Receipts(tmp_path, {})
    runner = object.__new__(scheduler.ScheduledRunner)
    runner.receipts = receipts
    runner.future = SimpleNamespace(done=lambda: True)
    runner.active = ('fit', 'EUR_USD', ('ridge',))
    runner.states = {'EUR_USD': {'families': {'ridge': {'ledger': ledger}}}}
    def finish(self):
        db.execute("INSERT INTO forecasts VALUES('new','new-sha',3,4)")
        self.future = None
    monkeypatch.setattr(scheduler.base.PairRunner, 'finish_work', finish)
    runner.finish_work()
    receipts.close()
    row = json.loads(receipts.path.read_text().splitlines()[-1])
    assert row['contract_sha256'] == 'original-contract'
    assert [r['id'] for r in row['forecasts']] == ['new']
    assert db.execute("SELECT * FROM forecasts WHERE id='old'").fetchall() == old


def test_binding_refuses_changed_registry_and_scope(tmp_path):
    registry = tmp_path / 'registry.json'
    registry.write_text('{}')
    config = tmp_path / 'scheduler.json'
    value = dict(schema_version=scheduler.SCHEMA, base_source_sha256=scheduler.BASE_SHA256,
        scheduler_source_sha256=scheduler.sha(scheduler.__file__),
        health_retry_source_sha256=scheduler.sha(scheduler.health_retry.__file__),
        registry_sha256=scheduler.sha(registry),
        changes=['overdue_news_before_post_poll_pair_dispatch',
                 'one_full_capture_retry_after_strict_healthy_recheck',
                 'skip_already_published_market_reference',
                 'bounded_exact_health_file_permission_retry',
                 'service_completed_work_between_settlements',
                 'bounded_round_robin_settlement_quanta'], can_place_orders=False, research_only=True)
    config.write_text(json.dumps(value))
    assert scheduler.validate_overlay(config, registry) == value
    registry.write_text('{"different":true}')
    with pytest.raises(ValueError, match='exact_source'):
        scheduler.validate_overlay(config, registry)
    value['registry_sha256'] = scheduler.sha(registry)
    value['changes'].append('relax_freshness')
    config.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='scope_changed'):
        scheduler.validate_overlay(config, registry)


def test_original_worker_source_is_preserved():
    assert scheduler.sha(scheduler.base.__file__) == scheduler.BASE_SHA256

@pytest.mark.parametrize('existing,new_reference,expected', [(True,False,0),(True,True,1),(False,False,1)])
def test_duplicate_filter_through_original_dispatch(existing,new_reference,expected,tmp_path,monkeypatch):
    db=sqlite3.connect(':memory:')
    db.execute('CREATE TABLE forecasts(id TEXT,sha TEXT,reference REAL UNIQUE)')
    if existing: db.execute("INSERT INTO forecasts VALUES('preserved','hash',960)")
    baseline=db.execute('SELECT * FROM forecasts').fetchall()
    calls=[]
    ledger=SimpleNamespace(db=db,begin_attempt=lambda *a: calls.append('attempt') or 'attempt')
    r=object.__new__(scheduler.ScheduledRunner)
    r.receipts=scheduler.Receipts(tmp_path,{})
    capture=dict(reference_start_epoch=901 if new_reference else 900,source_capture_sha256='a'*64,first_observed_epoch=990)
    slot=dict(ledger=ledger,failed_basis=None,last_success_bucket=0,last_try=0)
    r.states={'EUR_USD':dict(capture=capture,news_capture='handle',history_share='history',families={'ridge':slot})}
    r.queue=['EUR_USD'];r.current={'EUR_USD':{'ridge':{'market_epoch':995}}}
    r.news_hint=lambda h: ('b'*64,0)
    r.clock=lambda:1000.;r.news_session='session'
    r.pool=SimpleNamespace(submit=lambda *a,**k: calls.append('fit') or object())
    r.record_scheduler_event=lambda *a,**k:None
    r.error=lambda *a:pytest.fail(str(a))
    monkeypatch.setattr(scheduler.base,'input_readiness',lambda *a:{'status':'ready'})
    r.schedule_fit(1000,1)
    assert calls.count('fit')==expected
    assert calls.count('attempt')==expected
    assert db.execute('SELECT * FROM forecasts').fetchall()==baseline
    assert slot['last_success_bucket']==0
    if not expected:
        r.schedule_fit(1000,1)
        assert not calls
        assert len(r.receipts.path.read_text().splitlines())==2
        # A genuinely new reference re-enters the original dispatcher.
        capture.update(reference_start_epoch=901,source_capture_sha256='c'*64)
        r.schedule_fit(1000,1)
        assert calls==['attempt','fit']
    r.receipts.close()
