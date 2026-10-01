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
                 'one_full_capture_retry_after_strict_healthy_recheck'], can_place_orders=False, research_only=True)
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
