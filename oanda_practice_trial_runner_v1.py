"""Finite, explicitly authorized Practice007 experiment. Default command is GET-only.

Original research contracts remain unchanged. This route owns its own durable
intent ledger and account lock; a claimed intent is never submitted twice.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid
import zlib

from oanda_practice_forecast_adapter_v1 import read_candidates
from oanda_practice_trial_broker_v1 import PracticeTrialBroker, BrokerError
from oanda_practice_trial_policy_v1 import (
    HARD_STOP_EPOCH, POLICY, evaluate_entry, choose_best, session_guard, manage_position)

ROOT = Path(__file__).resolve().parent
TRIAL_ID = 'practice007_joint_v3_20260909_v1'
STATE = ROOT / 'data/oanda_training_manager' / TRIAL_ID
CONFIG = ROOT / 'config' / (TRIAL_ID + '.json')
SCHEMA = 'practice_trial_runner_v1_20260909'
TERMINAL = {'not_filled', 'not_submitted', 'filled_closed'}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def utc_day(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).date().isoformat()


def atomic_json(path, value):
    tmp = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    with tmp.open('wb') as stream:
        stream.write(encoded(value)); stream.flush(); os.fsync(stream.fileno())
    os.replace(tmp, path)


def safe_error(exc):
    # No arbitrary exception text, broker payload, account ID or credential.
    local_codes = {'account_lock_busy', 'account_lock_unavailable', 'account_lock_lost'}
    code = exc.code if isinstance(exc, BrokerError) else str(exc) if type(exc) is RuntimeError and str(exc) in local_codes else 'operation_failed'
    return {'type': type(exc).__name__, 'code': code}


def validate_config(path, *, require_enabled=False):
    raw = path.read_bytes()
    if len(raw) > 65536:
        raise ValueError('config_size')
    cfg = json.loads(raw)
    seal = cfg.pop('config_sha256', None)
    if seal != digest(cfg):
        raise ValueError('config_seal')
    if (cfg.get('schema_version') != SCHEMA or cfg.get('trial_id') != TRIAL_ID
            or cfg.get('environment') != 'practice' or cfg.get('account_label') != 'practice_007'
            or cfg.get('user_authorized_practice_trial') is not True
            or cfg.get('stop_epoch') != HARD_STOP_EPOCH or cfg.get('policy') != POLICY
            or cfg.get('strategy') != 'verified_joint_v3_main_price_news'
            or type(cfg.get('enabled')) is not bool
            or (require_enabled and cfg['enabled'] is not True)):
        raise ValueError('config_scope')
    expected = cfg.get('source_bindings', {})
    required = {'oanda_practice_trial_runner_v1.py', 'oanda_practice_trial_broker_v1.py',
                'oanda_practice_forecast_adapter_v1.py', 'oanda_practice_trial_policy_v1.py',
                'oanda_live_account_readonly_status.py', 'oanda_trade_reconciliation_v1.py',
                'src/forex_system/contracts/signed_currency_exposure.py'}
    if set(expected) != required:
        raise ValueError('source_closure_inventory')
    for name, sha in expected.items():
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != sha:
            raise ValueError('source_closure_changed')
    cfg['config_sha256'] = seal
    return cfg


class Ledger:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS evidence(sha TEXT PRIMARY KEY,epoch REAL NOT NULL,kind TEXT NOT NULL,body BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY,epoch REAL NOT NULL,kind TEXT NOT NULL,body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS intents(id TEXT PRIMARY KEY,prepared TEXT NOT NULL,context_sha TEXT NOT NULL,
            attempted INTEGER NOT NULL DEFAULT 0,attempt_epoch REAL,status TEXT NOT NULL,trade_id TEXT,result TEXT);
          CREATE TABLE IF NOT EXISTS transactions(id TEXT PRIMARY KEY,body TEXT NOT NULL);
        ''')

    def close(self):
        self.db.close()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.db:
            self.db.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                            (key, encoded(value).decode()))

    def latch(self, key):
        self.set(key, True)

    def event(self, kind, body, epoch=None):
        with self.db:
            self.db.execute('INSERT INTO events(epoch,kind,body) VALUES(?,?,?)',
                            (time.time() if epoch is None else epoch, kind, encoded(body).decode()))

    def evidence(self, kind, body, epoch=None):
        raw = encoded(body); sha = hashlib.sha256(raw).hexdigest()
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO evidence VALUES(?,?,?,?)',
                (sha, time.time() if epoch is None else epoch, kind, zlib.compress(raw, 6)))
        return sha

    def context(self, sha):
        row = self.db.execute('SELECT body FROM evidence WHERE sha=?', (sha,)).fetchone()
        if row is None:
            raise ValueError('missing_intent_context')
        raw = zlib.decompress(row[0])
        if hashlib.sha256(raw).hexdigest() != sha:
            raise ValueError('retained_evidence_identity_changed')
        return json.loads(raw)

    def prepare(self, prepared, context_sha):
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO intents(id,prepared,context_sha,status) VALUES(?,?,?,?)',
                (prepared['intent_id'], encoded(prepared).decode(), context_sha, 'prepared'))
        existing = self.db.execute('SELECT prepared FROM intents WHERE id=?', (prepared['intent_id'],)).fetchone()
        if json.loads(existing[0]) != prepared:
            raise ValueError('intent_identity_conflict')

    def claim(self, prepared, epoch):
        # FULL synchronous commit completes before the broker write callback returns.
        with self.db:
            row = self.db.execute('SELECT prepared,attempted FROM intents WHERE id=?', (prepared['intent_id'],)).fetchone()
            if row is None or json.loads(row['prepared']) != prepared:
                raise ValueError('unretained_intent')
            if row['attempted']:
                return False
            self.db.execute('UPDATE intents SET attempted=1,attempt_epoch=?,status=? WHERE id=?',
                            (epoch, 'unknown', prepared['intent_id']))
            self.db.execute('INSERT INTO events(epoch,kind,body) VALUES(?,?,?)',
                            (epoch, 'submission_claim', encoded({'intent_id': prepared['intent_id']}).decode()))
        return True

    def outcome(self, intent, result):
        with self.db:
            self.db.execute('UPDATE intents SET status=?,trade_id=COALESCE(?,trade_id),result=? WHERE id=?',
                (result['status'], result.get('trade_id'), encoded(result).decode(), intent))
        self.event('intent_reconciled', {'intent_id': intent, 'result': result})

    def intents(self, *, unresolved=False):
        rows = [dict(r) for r in self.db.execute('SELECT * FROM intents ORDER BY rowid')]
        for row in rows:
            row['prepared'] = json.loads(row['prepared'])
            row['result'] = json.loads(row['result']) if row['result'] else None
        return [r for r in rows if r['status'] not in TERMINAL] if unresolved else rows

    def session(self, now):
        initial = self.get('initial')
        if not initial:
            raise ValueError('session_not_initialized')
        claimed = list(self.db.execute('SELECT prepared,attempt_epoch FROM intents WHERE attempted=1'))
        last = {}
        for row in claimed:
            pair = json.loads(row['prepared'])['order']['instrument']
            last[pair] = max(last.get(pair, 0), row['attempt_epoch'])
        return dict(started_epoch=initial['epoch'], start_nav_usd=initial['NAV'], stop_epoch=HARD_STOP_EPOCH,
                    loss_stop_latched=self.get('loss_stop_latched', False), day_utc=utc_day(now),
                    entries_today=sum(utc_day(r['attempt_epoch']) == utc_day(now) for r in claimed),
                    last_entry_epoch_by_instrument=last)

    def retain_transactions(self, batch):
        with self.db:
            for tx in batch['rows']:
                existing = self.db.execute('SELECT body FROM transactions WHERE id=?', (tx['id'],)).fetchone()
                if existing and json.loads(existing[0]) != tx:
                    raise ValueError('retained_transaction_identity_changed')
                self.db.execute('INSERT OR IGNORE INTO transactions VALUES(?,?)', (tx['id'], encoded(tx).decode()))
            self.db.execute('INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                            ('transaction_cursor', encoded(batch['lastTransactionID']).decode()))


class AccountLock:
    """Share the existing Practice007 account marker; never steal a live PID."""
    def __init__(self, path):
        self.path = path; self.fd = None; self.stop = threading.Event(); self.thread = None
        self.nonce = uuid.uuid4().hex

    def __enter__(self):
        import psutil
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for _ in range(2):
            try:
                self.fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, 'O_BINARY', 0))
                break
            except FileExistsError:
                raw = self.path.read_text().strip().split()
                if not raw or not raw[0].isdigit() or psutil.pid_exists(int(raw[0])):
                    raise RuntimeError('account_lock_busy') from None
                # Only the explicit, verified-dead account marker is moved.
                stale = self.path.with_name(self.path.name + '.dead.' + uuid.uuid4().hex)
                os.replace(self.path, stale)
        if self.fd is None:
            raise RuntimeError('account_lock_unavailable')
        self.marker = f'{os.getpid()} {self.nonce}\n'.encode()
        os.write(self.fd, self.marker); os.fsync(self.fd)
        self.thread = threading.Thread(target=self._heartbeat, daemon=True); self.thread.start()
        return self

    def _heartbeat(self):
        while not self.stop.wait(3):
            try:
                self.check(); os.utime(self.path, None)
            except Exception:
                return

    def check(self):
        if self.fd is None or self.path.read_bytes() != self.marker:
            raise RuntimeError('account_lock_lost')

    def __exit__(self, *args):
        self.stop.set()
        if self.thread: self.thread.join(4)
        owned = self.fd is not None and self.path.exists() and self.path.read_bytes() == self.marker
        if self.fd is not None: os.close(self.fd); self.fd = None
        if owned: self.path.unlink()


def process_preflight():
    import psutil
    supervisors = []; competitors = []
    forbidden = {'oanda_practice_top_signal_executor.py', 'oanda_practice_shadow_strategy_lab.py',
                 'oanda_second_forecast_runner.py', 'oanda_practice_eurusd_micro_scalper.py',
                 'oanda_technical_account_manager_auto.py', 'oanda_gpt_exp_account_manager.py',
                 'oanda_gpt_9h_formula83_account_manager.py', 'oanda_advisor_account_manager_auto.py',
                 'oanda_arima_canary_executor.py', 'oanda_dum_canary_executor.py',
                 'oanda_gpt_technical_snapshot_account_manager.py', 'oanda_gpt_prod_live_account_manager.py',
                 'oanda_primary_live_exit_rotation_sweep.py', 'oanda_primary_forecast_rotation_bot.py',
                 'oanda_primary_challenger_live_account_manager.py', 'oanda_practice_pair_rotation_scalper.py',
                 'oanda_tech_prod_live_account_manager.py', 'oanda_spike_scout_account_manager.py',
                 'run_oanda_tech_prod_live_account_manager.py'}
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            args = p.info['cmdline'] or []
            exe = (p.info['name'] or '').lower()
            if exe.startswith('powershell') or exe.startswith('pwsh'):
                for i, arg in enumerate(args[:-1]):
                    if arg.lower() == '-file' and Path(args[i+1]).name.lower() == 'oanda_always_on_supervisor.ps1':
                        supervisors.append({'pid': p.pid, 'research_collection_only': '-researchcollectiononly' in [a.lower() for a in args]})
            if exe.startswith('python'):
                scripts = [Path(a).name for a in args[1:] if a.lower().endswith('.py')]
                if any(s in forbidden or ('primary' in s and 'rotation' in s and 'broker' in s) for s in scripts):
                    competitors.append(p.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return {'supervisors': supervisors, 'competing_execution_pids': competitors,
            'ready': len(supervisors) == 1 and supervisors[0]['research_collection_only'] and not competitors}


def flat(account, trades, pending):
    return (account.get('openTradeCount') == 0 and account.get('openPositionCount') == 0
            and account.get('pendingOrderCount') == 0 and trades['rows'] == [] and pending['rows'] == [])


def position_dto(trade, observed, signal):
    stop = trade.get('stopLossOrder') or {}
    return dict(trade_id=trade['id'], instrument=trade['instrument'], currentUnits=trade['currentUnits'],
                price=trade['price'], stop_loss_price=stop.get('price') if stop.get('state') == 'PENDING' else None,
                original_target_epoch=signal['original_target_epoch'],
                opened_epoch=datetime.fromisoformat(trade['openTime'].replace('Z', '+00:00')).timestamp(),
                observed_epoch=observed, forecast_sha256=signal['forecast_sha256'])


class Runner:
    def __init__(self, broker, ledger, config, lock, *, clock=time.time, reader=read_candidates, status_path=None):
        self.broker = broker; self.ledger = ledger; self.config = config; self.lock = lock
        self.clock = clock; self.reader = reader; self.status_path = status_path
        self.metadata = {}; self.last_scan = 0; self.last_reconcile = 0; self.last_transaction = 0
        self.last_candidates = {}; self.last_closure_check = 0

    def status(self, state, **extra):
        result = dict(schema_version=SCHEMA, trial_id=TRIAL_ID, environment='practice', account_label='practice_007',
                      observed_epoch=self.clock(), pid=os.getpid(), state=state,
                      enabled=self.config['enabled'], stop_epoch=HARD_STOP_EPOCH,
                      research_contracts_unchanged=True, performance_validated=False,
                      loss_stop_latched=self.ledger.get('loss_stop_latched', False),
                      entry_halt_latched=self.ledger.get('entry_halt_latched', False),
                      stop_requested=self.ledger.get('stop_requested', False),
                      last_candidate_scan=self.last_candidates, **extra)
        if self.status_path: atomic_json(self.status_path, result)
        return result

    def initialize(self, account, trades, pending):
        seal = self.ledger.get('config_sha256')
        if seal and seal != self.config['config_sha256']:
            raise ValueError('persisted_config_changed')
        self.ledger.set('config_sha256', self.config['config_sha256'])
        if self.ledger.get('initial') is None:
            if not flat(account, trades, pending) or account['currency'] != 'USD' or Decimal(account['NAV']) <= 0:
                raise ValueError('initial_account_not_flat_usd')
            if self.clock() >= HARD_STOP_EPOCH:
                raise ValueError('new_trial_expired')
            initial = {'epoch': self.clock(), 'NAV': account['NAV'], 'account_evidence': self.ledger.evidence('initial_account', account)}
            self.ledger.set('initial', initial)
            self.ledger.set('transaction_cursor', account['lastTransactionID'])

    def reconcile(self):
        for intent in self.ledger.intents(unresolved=True):
            # An unclaimed prepared record was never eligible for transport.
            if not intent['attempted']:
                self.ledger.outcome(intent['id'], {'status': 'not_submitted', 'reason_code': 'unclaimed_after_restart'})
                continue
            result = self.broker.reconcile_market(intent['prepared'])
            if result != intent['result']: self.ledger.outcome(intent['id'], result)
        self.last_reconcile = self.clock()

    def observe_guard(self, account):
        now = self.clock(); session = self.ledger.session(now)
        guard = session_guard(account, session, now)
        if guard.get('loss_stop_latched'):
            self.ledger.latch('loss_stop_latched'); session['loss_stop_latched'] = True
        return session

    def manage(self, account, trades, pending):
        now = self.clock(); session = self.ledger.session(now)
        guard = session_guard(account, session, now)
        if guard.get('loss_stop_latched'):
            self.ledger.latch('loss_stop_latched'); session['loss_stop_latched'] = True
        intents = self.ledger.intents()
        by_client = {r['prepared']['order']['tradeClientExtensions']['id']: r for r in intents if r['attempted']}
        positions = []; foreign = []
        for trade in trades['rows']:
            try:
                target = self.broker.owned_trade(trade)
            except BrokerError:
                foreign.append(trade['id']); continue
            intent = by_client.get((trade.get('clientExtensions') or {}).get('id'))
            if intent is None:
                self.ledger.latch('entry_halt_latched')
                self.lock.check()
                result = self.broker.close_owned_trade(trade['id'], trade['instrument'], original_target_epoch=target)
                self.ledger.event('orphan_owned_trade_close', result)
                continue
            self.broker.owned_trade(trade, expected=intent['prepared'])
            try:
                ctx = self.ledger.context(intent['context_sha'])
                position = position_dto(trade, trades['observed_epoch'], ctx['signal'])
            except Exception:
                self.ledger.latch('entry_halt_latched'); self.lock.check()
                result = self.broker.close_owned_trade(trade['id'], trade['instrument'], original_target_epoch=target)
                self.ledger.event('owned_context_failure_close', result)
                continue
            quote = None
            try: quote = self.broker.pricing([trade['instrument']])['quotes'].get(trade['instrument'])
            except BrokerError: pass
            decision = manage_position(position, quote, account, session=session, decision_epoch=self.clock())
            positions.append({'trade_id': trade['id'], 'instrument': trade['instrument'], 'units': trade['currentUnits'],
                              'original_target_epoch': target, 'decision': decision})
            self.ledger.evidence('management', {'position': position, 'quote': quote, 'account': account, 'session': session, 'decision': decision})
            if decision.get('loss_stop_latched'): self.ledger.latch('loss_stop_latched')
            if decision.get('action') == 'close':
                self.lock.check()
                result = self.broker.close_owned_trade(trade['id'], trade['instrument'], original_target_epoch=target)
                self.ledger.event('owned_trade_close', {'trade_id': trade['id'], 'decision': decision, 'result': result})
        if foreign:
            self.ledger.latch('entry_halt_latched')
        # Protective pending orders belong to open trades; every other pending order blocks entries.
        if pending['rows'] and not trades['rows']: self.ledger.latch('entry_halt_latched')
        return guard, positions, foreign

    def manage_mandatory(self, trades):
        """Deadlines/protection must not depend on account/news/context reads."""
        own = []
        for trade in trades['rows']:
            try: own.append((trade, self.broker.owned_trade(trade)))
            except BrokerError: continue
        multiple = len(own) > 1
        if multiple: self.ledger.latch('entry_halt_latched')
        closed = False
        for trade, target in own:
            stop = trade.get('stopLossOrder') or {}
            try: protected = stop.get('state') == 'PENDING' and Decimal(stop.get('price', '0')).is_finite() and Decimal(stop.get('price', '0')) > 0
            except (ValueError, ArithmeticError, TypeError): protected = False
            due = self.clock() >= min(target, HARD_STOP_EPOCH)
            if due or not protected or multiple or self.ledger.get('loss_stop_latched', False) or self.ledger.get('stop_requested', False):
                self.lock.check()
                result = self.broker.close_owned_trade(trade['id'], trade['instrument'], original_target_epoch=target)
                self.ledger.event('mandatory_owned_close', {'trade_id': trade['id'], 'target_epoch': target,
                    'deadline_due': due, 'protected': protected, 'multiple_owned': multiple, 'result': result})
                closed = True
        return closed

    def cycle(self):
        self.lock.check()
        trades = self.broker.open_trades()
        if self.manage_mandatory(trades): trades = self.broker.open_trades()
        account = self.broker.account_summary(); pending = self.broker.pending_orders()
        self.initialize(account, trades, pending)
        guard, positions, foreign = self.manage(account, trades, pending)
        now = self.clock()
        if now - self.last_reconcile >= 30: self.reconcile()
        if now - self.last_transaction >= 30:
            batch = self.broker.transactions_since(self.ledger.get('transaction_cursor'))
            self.ledger.retain_transactions(batch); self.last_transaction = self.clock()
        outstanding = self.ledger.intents(unresolved=True)
        snapshot = dict(account=account, positions=positions, foreign_trade_count=len(foreign),
                        unresolved_intents=len(outstanding), session=self.ledger.session(self.clock()))
        if self.clock() >= HARD_STOP_EPOCH or self.ledger.get('loss_stop_latched', False) or self.ledger.get('stop_requested', False):
            # Re-read after close attempts; never infer flatness from a failed read.
            account = self.broker.account_summary(); trades = self.broker.open_trades(); pending = self.broker.pending_orders()
            self.reconcile(); outstanding = self.ledger.intents(unresolved=True)
            if flat(account, trades, pending) and not outstanding:
                self.ledger.set('completed_flat', {'epoch': self.clock(), 'reason': 'manual_stop' if self.ledger.get('stop_requested', False) else guard.get('reason'), 'account': account})
                return self.status('completed_flat', account=account)
            return self.status('close_only_awaiting_confirmation', **snapshot)
        if not flat(account, trades, pending):
            return self.status('managing_position' if positions else 'account_not_flat', **snapshot)
        if outstanding or self.ledger.get('entry_halt_latched', False):
            return self.status('entries_halted_reconciliation_required', **snapshot)
        if guard.get('status') != 'within_limits':
            return self.status('account_or_session_unavailable', **snapshot)
        if self.clock() - self.last_scan >= 30:
            self.scan_and_enter()
            # A submit may have changed the account; status explicitly uses fresh reads.
            snapshot['account'] = self.broker.account_summary()
            snapshot['session'] = self.ledger.session(self.clock())
            snapshot['unresolved_intents'] = len(self.ledger.intents(unresolved=True))
        return self.status('practice_trial_enabled', **snapshot)

    def scan_and_enter(self):
        self.last_scan = self.clock()
        # Changed code/config or a competing legacy worker disables new entries;
        # the existing process continues to manage its exact owned trades.
        try:
            current = validate_config(CONFIG, require_enabled=True)
            if current != self.config or not process_preflight()['ready']:
                raise ValueError('activation_or_process_changed')
        except Exception:
            self.ledger.latch('entry_halt_latched')
            raise
        batch = self.reader(ROOT, self.clock())
        source_sha = self.ledger.evidence('forecast_observation', batch)
        signals = batch['signals']
        consumed = {r['prepared']['intent_id'] for r in self.ledger.intents()}
        signals = [s for s in signals if self.intent_id(s) not in consumed]
        if not self.metadata:
            self.metadata = {r['name']: r for r in self.broker.instruments()['rows'] if r.get('type') == 'CURRENCY' and r.get('tradeUnitsPrecision') == 0}
        signals = [s for s in signals if s['instrument'] in self.metadata]
        refusals = Counter(r['reason'] for r in batch['rejected'])
        if not signals:
            self.last_candidates = {'epoch': self.clock(), 'source_sha256': source_sha, 'eligible': 0, 'refusals': dict(refusals)}
            return
        prices = self.broker.pricing([s['instrument'] for s in signals])
        # Screen using the same pure policy with missing candles. Only a candidate
        # which reaches the ATR gate needs a candle request. No proxy score enters.
        account = self.broker.account_summary(); session = self.observe_guard(account); now = self.clock()
        screened = []
        for signal in signals:
            pair = signal['instrument']
            result = evaluate_entry(signal, prices['quotes'].get(pair), account, self.metadata[pair],
                                    prices['home_conversions'], {}, session=session, decision_epoch=now)
            if result.get('reason') == 'candles_fields': screened.append(signal)
            else: refusals[result.get('reason', 'screen_refused')] += 1
        # Candles are requested for all signal/time/account-eligible pairs, with a
        # bounded collection deadline. Refresh quotes/account after these reads.
        candles = {}; deadline = time.monotonic() + 40
        for signal in screened:
            if time.monotonic() >= deadline:
                refusals['candle_collection_deadline'] += 1; continue
            try: candles[signal['instrument']] = self.broker.completed_m1_candles(signal['instrument'])
            except BrokerError as exc: refusals['candles_' + exc.code] += 1
        selected_signals = [s for s in screened if s['instrument'] in candles]
        if selected_signals:
            prices = self.broker.pricing([s['instrument'] for s in selected_signals])
            account = self.broker.account_summary(); session = self.observe_guard(account); now = self.clock()
            contexts = {}; assessments = []
            for signal in selected_signals:
                pair = signal['instrument']
                ctx = dict(signal=signal, quote=prices['quotes'].get(pair), account=account, metadata=self.metadata[pair],
                           home_conversions=prices['home_conversions'], candles=candles[pair], session=session,
                           decision_epoch=now, source_evidence_sha256=source_sha)
                assessment = evaluate_entry(signal, ctx['quote'], account, ctx['metadata'], ctx['home_conversions'],
                                            ctx['candles'], session=session, decision_epoch=now)
                contexts[pair] = ctx; assessments.append(assessment)
                if assessment['status'] != 'available': refusals[assessment['reason']] += 1
            self.ledger.evidence('selection', {'contexts': contexts, 'assessments': assessments})
            best = choose_best(assessments)
            available = sum(a['status'] == 'available' for a in assessments)
            self.last_candidates = {'epoch': now, 'source_sha256': source_sha, 'eligible': available, 'refusals': dict(refusals)}
            if best.get('status') == 'available':
                self.enter(contexts[best['instrument']], best)
        else:
            self.last_candidates = {'epoch': self.clock(), 'source_sha256': source_sha, 'eligible': 0, 'refusals': dict(refusals)}
        self.ledger.event('candidate_scan', self.last_candidates)
        self.last_scan = self.clock()

    @staticmethod
    def intent_id(signal):
        return digest([TRIAL_ID, signal['forecast_sha256'], signal['instrument'], signal['side'], signal['original_target_epoch']])

    def enter(self, context, selection):
        pair = context['signal']['instrument']; signal = context['signal']
        self.lock.check()
        if self.ledger.get('stop_requested', False): return
        account = self.broker.account_summary(); trades = self.broker.open_trades(); pending = self.broker.pending_orders()
        if not flat(account, trades, pending) or self.ledger.intents(unresolved=True):
            self.ledger.event('entry_revalidation_refused', {'reason': 'account_or_intent_not_flat'}); return
        session = self.observe_guard(account)
        fresh = self.broker.pricing([pair]); now = self.clock()
        assessment = evaluate_entry(signal, fresh['quotes'].get(pair), account, context['metadata'], fresh['home_conversions'],
                                    context['candles'], session=session, decision_epoch=now)
        if assessment.get('status') != 'available':
            self.ledger.event('entry_revalidation_refused', assessment); return
        context = dict(context, quote=fresh['quotes'].get(pair), account=account, home_conversions=fresh['home_conversions'],
                       session=session, decision_epoch=now, assessment=assessment, selection=selection)
        context_sha = self.ledger.evidence('final_entry_context', context)
        prepared = self.broker.prepare_market_order(self.intent_id(signal), pair, assessment['units'], assessment['stop_loss_price'],
                    price_bound=assessment['entry_price_bound'], target_epoch=signal['original_target_epoch'])
        self.ledger.prepare(prepared, context_sha)
        def claim(p):
            self.lock.check()
            if self.ledger.get('stop_requested', False): return False
            # A durable baseline and absence of all other claimed active intents
            # were established before this single-use claim.
            return self.ledger.claim(p, self.clock())
        def validate_before_submit(p, fresh_account, flatness_context):
            self.lock.check()
            if self.ledger.get('stop_requested', False) or self.ledger.get('entry_halt_latched', False): return False
            try:
                if validate_config(CONFIG, require_enabled=True) != self.config or not process_preflight()['ready']:
                    raise ValueError('activation_or_process_changed')
            except Exception:
                self.ledger.latch('entry_halt_latched'); return False
            current_session = self.observe_guard(fresh_account)
            current = self.broker.pricing([pair]); cutoff = self.clock()
            # The just-claimed attempt is already counted durably. Entry gates
            # must assess the pre-claim counters for this same single attempt.
            current_session['entries_today'] = session['entries_today']
            current_session['last_entry_epoch_by_instrument'] = session['last_entry_epoch_by_instrument']
            check = evaluate_entry(signal, current['quotes'].get(pair), fresh_account, context['metadata'],
                        current['home_conversions'], context['candles'], session=current_session, decision_epoch=cutoff)
            exact = (check.get('status') == 'available' and check.get('units') == assessment['units']
                     and check.get('stop_loss_price') == assessment['stop_loss_price']
                     and check.get('entry_price_bound') == assessment['entry_price_bound']
                     and check.get('original_target_epoch') == prepared['original_target_epoch'])
            self.ledger.evidence('pretransport_revalidation', dict(prepared=p, account=fresh_account,
                flatness=flatness_context, pricing=current, session=current_session, check=check, exact_intent_pass=exact))
            if check.get('loss_stop_latched'): self.ledger.latch('loss_stop_latched')
            return exact and not self.ledger.get('loss_stop_latched', False)
        result = self.broker.submit_market(prepared, claim_submission=claim, validate_before_submit=validate_before_submit)
        self.ledger.outcome(prepared['intent_id'], result)
        self.last_candidates['last_submission'] = {k: result[k] for k in ['status', 'reason_code', 'trade_id', 'instrument', 'units'] if k in result}


def preflight(broker, config):
    process = process_preflight()
    account = broker.account_summary(); trades = broker.open_trades(); pending = broker.pending_orders()
    metadata = broker.instruments()
    prices = broker.pricing(['EUR_USD'])
    batch = read_candidates(ROOT, time.time())
    return dict(schema_version=SCHEMA, mode='read_only_preflight', epoch=time.time(), environment='practice',
                account_label='practice_007', account=account, flat=flat(account, trades, pending),
                processes=process, instrument_count=len(metadata['rows']), price_probe=prices,
                candidate_count=len(batch['signals']), rejected_count=len(batch['rejected']),
                source_evidence_sha256=digest(batch), config_sha256=config['config_sha256'],
                ready=process['ready'] and flat(account, trades, pending) and account['currency'] == 'USD'
                      and Decimal(account['NAV']) > 0 and time.time() < HARD_STOP_EPOCH)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='store_true', help='Explicitly enable the finite practice trial')
    parser.add_argument('--preflight', action='store_true', help='GET-only readiness check (default)')
    parser.add_argument('--stop', action='store_true', help='Persist a local close-and-stop request; worker confirms its owned trades flat')
    args = parser.parse_args()
    if args.stop:
        ledger = Ledger(STATE / 'trial.sqlite')
        try:
            ledger.latch('stop_requested'); ledger.event('manual_stop_requested', {'requested_epoch': time.time()})
        finally: ledger.close()
        print(encoded({'status': 'stop_requested', 'broker_action_performed_by_this_command': False}).decode())
        return 0
    config = validate_config(CONFIG, require_enabled=args.run)
    broker = PracticeTrialBroker.from_credentials(ROOT / 'creds', expected_account_sha256=config['account_sha256'], trial_id=TRIAL_ID)
    STATE.mkdir(parents=True, exist_ok=True)
    try:
        if not args.run:
            result = preflight(broker, config)
            atomic_json(STATE / 'preflight.json', result)
            print(encoded(result).decode()); return 0 if result['ready'] else 2
        with AccountLock(ROOT / 'data/oanda_training_manager/state/practice_007_order.lock') as lock:
            ledger = Ledger(STATE / 'trial.sqlite')
            try:
                if ledger.get('completed_flat'):
                    return 0
                runner = Runner(broker, ledger, config, lock, status_path=STATE / 'status.json')
                ledger.event('worker_started', {'pid': os.getpid(), 'config_sha256': config['config_sha256']})
                while True:
                    began = time.monotonic()
                    try:
                        result = runner.cycle()
                        if result['state'] == 'completed_flat': return 0
                    except Exception as exc:
                        error = safe_error(exc)
                        ledger.event('cycle_error', error)
                        runner.status('operation_unavailable_no_new_order', error=error)
                    time.sleep(max(1, 5 - (time.monotonic() - began)))
            finally:
                ledger.close()
    finally:
        broker.close()


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(encoded({'status': 'refused', 'error': safe_error(exc)}).decode())
        raise SystemExit(2)
