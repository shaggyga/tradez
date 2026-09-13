"""Bounded PRACTICE-007 trial transport; no I/O on import, no automatic write retry.

The caller owns account exclusivity, policy/sizing and a durable FULL-synchronous
intent claim before submit_market. Existing claimed intents are reconcile-only,
including after restart. Returned transaction/trade data stays in private state;
exceptions and receipts never contain credentials, account IDs or response text.
This is an explicitly authorized practice experiment, not governed proof eligibility.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Context, Decimal, localcontext
import hashlib
import json
import math
from pathlib import Path
import re
import threading
import time
from urllib.parse import quote

import requests
from oanda_live_account_readonly_status import read_creds, cfg_value
from oanda_trade_reconciliation_v1 import number, observe_trade_rows, reconcile_reduction

SCHEMA = 'practice_trial_broker_v1_20260909'
BASE_URL = 'https://api-fxpractice.oanda.com/v3'
ACCOUNT_KEY = 'OANDA_ACCOUNT_ID_DUM4'
TAG = 'practice_trial_v1'
MAX_BYTES = 2 * 1024 * 1024
MAX_ROWS = 10000
SOURCE_DEPENDENCIES = ('oanda_practice_trial_broker_v1.py',
                       'oanda_live_account_readonly_status.py',
                       'oanda_trade_reconciliation_v1.py')


class BrokerError(ValueError):
    """Bounded public code only; no URL, account or raw upstream error text."""
    def __init__(self, code, *, http_status=None):
        super().__init__(code)
        self.code = code
        self.http_status = http_status


def _fail(code):
    raise BrokerError(code) from None


def _clock(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail('invalid_clock')
    if not math.isfinite(value) or value <= 0:
        _fail('invalid_clock')
    return float(value)


def _num(value, *, positive=False):
    try:
        out = number(value)
    except (ValueError, ArithmeticError, TypeError):
        _fail('invalid_number')
    if positive and out <= 0:
        _fail('nonpositive_number')
    return out


def _decimal_text(value):
    out = _num(value, positive=True)
    return format(out, 'f')


def _instrument(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', value) or value[:3] == value[4:]:
        _fail('invalid_instrument')
    return value


def _id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{1,32}', value):
        _fail('invalid_broker_record_id')
    return value


def _json(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (ValueError, TypeError, RecursionError):
        _fail('invalid_json_value')


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _epoch(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z', value):
        _fail('invalid_market_timestamp')
    try:
        base, fraction = value[:-1].split('.', 1) if '.' in value else (value[:-1], '0')
        seconds = int(datetime.strptime(base, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
        with localcontext(Context(prec=96)):
            return float(Decimal(seconds) + Decimal('0.' + fraction))
    except (ValueError, ArithmeticError):
        _fail('invalid_market_timestamp')


def _rows(payload, key, limit=MAX_ROWS):
    rows = payload.get(key)
    if not isinstance(rows, list) or len(rows) > limit or any(not isinstance(r, dict) for r in rows):
        _fail('invalid_complete_rows')
    return rows


class _RequestsTransport:
    def __init__(self):
        self.session = requests.Session()
        self.session.trust_env = False

    def __call__(self, method, url, *, headers, params, body):
        # No redirect, proxy inheritance, HTTP retry or raw-error logging.
        deadline = time.monotonic() + 15.0
        with self.session.request(method, url, headers=headers, params=params,
                                  data=None if body is None else _json(body),
                                  timeout=(3.0, 10.0), allow_redirects=False,
                                  stream=True, verify=True) as response:
            chunks, size = [], 0
            for chunk in response.iter_content(65536):
                if time.monotonic() > deadline:
                    _fail('response_time_bound')
                size += len(chunk)
                if size > MAX_BYTES:
                    _fail('response_byte_bound')
                chunks.append(chunk)
            return response.status_code, b''.join(chunks)

    def close(self):
        self.session.close()


class PracticeTrialBroker:
    def __init__(self, *, account_id, token, expected_account_sha256, trial_id,
                 environment='practice', account_key=ACCOUNT_KEY,
                 transport=None, clock=time.time):
        if environment != 'practice' or account_key != ACCOUNT_KEY:
            _fail('practice_account_scope_required')
        if not isinstance(account_id, str) or not re.fullmatch(r'\d{3}-\d{3}-\d{1,16}-007', account_id):
            _fail('practice_007_account_required')
        if not isinstance(expected_account_sha256, str) or not re.fullmatch(r'[0-9a-f]{64}', expected_account_sha256):
            _fail('invalid_account_fingerprint')
        if _sha(account_id.encode()) != expected_account_sha256:
            _fail('account_fingerprint_mismatch')
        if not isinstance(token, str) or not token or len(token) > 1024 or any(ord(c) < 33 for c in token):
            _fail('practice_token_unavailable')
        if not isinstance(trial_id, str) or not re.fullmatch(r'[a-z0-9_]{1,40}', trial_id):
            _fail('invalid_trial_id')
        self._account_id, self._token = account_id, token
        self.account_sha256, self.trial_id = expected_account_sha256, trial_id
        self._transport = transport if transport is not None else _RequestsTransport()
        self._clock = clock
        self._last_clock = 0.0
        self._clock_failed = False
        self._attempted = set()
        self._lock = threading.RLock()

    @classmethod
    def from_credentials(cls, creds_path, *, expected_account_sha256, trial_id, clock=time.time):
        text = read_creds(Path(creds_path))
        account = cfg_value(text, ACCOUNT_KEY)
        token = cfg_value(text, 'OANDA_API_KEY', 'OANDA_API_TOKEN')
        return cls(account_id=account, token=token, expected_account_sha256=expected_account_sha256,
                   trial_id=trial_id, clock=clock)

    def close(self):
        closer = getattr(self._transport, 'close', None)
        if closer:
            closer()

    def _now(self):
        value = _clock(self._clock())
        if self._clock_failed or value < self._last_clock:
            self._clock_failed = True
            _fail('observation_clock_rollback')
        self._last_clock = value
        return value

    def _request(self, method, path, *, route, params=None, body=None, missing_ok=False, submit_not_after=None):
        # Every caller supplies a module-constructed relative endpoint, not payload paths.
        if method not in {'GET', 'POST', 'PUT'} or not path.startswith('/') or '?' in path or '..' in path:
            _fail('invalid_internal_request')
        account = re.escape(self._account_path(''))
        routes = {
            'account_summary': ('GET', account + '/summary'),
            'account_instruments': ('GET', account + '/instruments'),
            'open_trades': ('GET', account + '/openTrades'),
            'pending_orders': ('GET', account + '/pendingOrders'),
            'transactions_since': ('GET', account + '/transactions/sinceid'),
            'pricing': ('GET', account + '/pricing'),
            'completed_m1_candles': ('GET', '/instruments/[A-Z]{3}_[A-Z]{3}/candles'),
            'exact_trade': ('GET', account + '/trades/[0-9]{1,32}'),
            'order_by_client_id': ('GET', account + '/orders/@pt1-[0-9a-f]{40}'),
            'exact_fill_transaction': ('GET', account + '/transactions/[0-9]{1,32}'),
            'submit_market': ('POST', account + '/orders'),
            'close_owned_trade': ('PUT', account + '/trades/[0-9]{1,32}/close'),
        }
        allowed = routes.get(route)
        if allowed is None or allowed[0] != method or not re.fullmatch(allowed[1], path):
            _fail('request_outside_fixed_practice_scope')
        began = self._now()
        if route == 'submit_market':
            if submit_not_after is None or began >= _clock(submit_not_after):
                _fail('submit_deadline_elapsed')
        try:
            status, raw = self._transport(method, BASE_URL + path,
                headers={'Authorization': 'Bearer ' + self._token, 'Content-Type': 'application/json',
                         'Accept-Datetime-Format': 'RFC3339'}, params=params, body=body)
        except Exception:
            _fail('transport_unresolved' if method != 'GET' else 'read_transport_failed')
        ended = self._now()
        if ended < began:
            _fail('observation_clock_rollback')
        if isinstance(status, bool) or not isinstance(status, int) or not isinstance(raw, bytes) or len(raw) > MAX_BYTES:
            _fail('invalid_transport_response')
        receipt = {'schema_version': SCHEMA, 'environment': 'practice', 'account_label': 'practice_007',
                   'method': method, 'route': route, 'started_epoch': began, 'observed_epoch': ended,
                   'http_status': status, 'source_sha256': _sha(raw), 'bytes': len(raw)}
        if status == 404 and missing_ok:
            return None, receipt
        if not 200 <= status < 300:
            raise BrokerError('broker_http_refusal', http_status=status) from None
        try:
            payload = json.loads(raw, parse_constant=lambda _: _fail('nonfinite_response'))
        except Exception:
            _fail('invalid_broker_json')
        if not isinstance(payload, dict):
            _fail('invalid_broker_object')
        return payload, receipt

    def _account_path(self, suffix):
        return '/accounts/' + self._account_id + suffix

    def account_summary(self):
        p, r = self._request('GET', self._account_path('/summary'), route='account_summary')
        a = p.get('account')
        if not isinstance(a, dict) or a.get('id') != self._account_id:
            _fail('account_response_identity_mismatch')
        fields = ['currency', 'NAV', 'balance', 'marginUsed', 'marginAvailable', 'marginRate',
                  'openTradeCount', 'openPositionCount', 'pendingOrderCount', 'hedgingEnabled',
                  'pl', 'unrealizedPL', 'financing', 'commission', 'lastTransactionID',
                  'marginCloseoutPercent', 'marginCloseoutNAV', 'marginCloseoutMarginUsed']
        result = {k: deepcopy(a[k]) for k in fields if k in a}
        if not re.fullmatch(r'[A-Z]{3}', str(result.get('currency', ''))):
            _fail('invalid_account_currency')
        for k in ['NAV', 'marginUsed', 'marginAvailable', 'marginRate']:
            _num(result.get(k))
        for k in ['openTradeCount', 'pendingOrderCount']:
            if type(result.get(k)) is not int or result[k] < 0:
                _fail('invalid_account_counts')
        result.update(observed_epoch=r['observed_epoch'], receipt=r)
        return result

    def instruments(self):
        p, r = self._request('GET', self._account_path('/instruments'), route='account_instruments')
        rows = _rows(p, 'instruments', 256)
        if any(not isinstance(x.get('name'), str) for x in rows) or len({x['name'] for x in rows}) != len(rows):
            _fail('invalid_instrument_inventory')
        return {'rows': deepcopy(rows), 'observed_epoch': r['observed_epoch'], 'receipt': r}

    def open_trades(self):
        p, r = self._request('GET', self._account_path('/openTrades'), route='open_trades')
        rows = _rows(p, 'trades')
        # Validate complete collection using the existing exact reconciliation semantics.
        observation = observe_trade_rows(rows, trade_id='0', observed_epoch=r['observed_epoch'])
        if observation['status'] == 'unknown':
            _fail('invalid_complete_open_trades')
        return {'rows': deepcopy(rows), 'observed_epoch': r['observed_epoch'], 'receipt': r,
                'lastTransactionID': p.get('lastTransactionID')}

    def pending_orders(self):
        p, r = self._request('GET', self._account_path('/pendingOrders'), route='pending_orders')
        rows = _rows(p, 'orders')
        ids = [_id(x.get('id')) for x in rows]
        if len(set(ids)) != len(ids):
            _fail('duplicate_pending_order_id')
        return {'rows': deepcopy(rows), 'observed_epoch': r['observed_epoch'], 'receipt': r}

    def transactions_since(self, transaction_id):
        p, r = self._request('GET', self._account_path('/transactions/sinceid'), route='transactions_since',
                             params={'id': _id(transaction_id)})
        rows = _rows(p, 'transactions')
        clean = []
        for row in rows:
            if row.get('accountID') != self._account_id:
                _fail('transaction_account_mismatch')
            _id(row.get('id'))
            if _epoch(row.get('time')) > r['observed_epoch']:
                _fail('future_transaction')
            clean.append({k: deepcopy(v) for k, v in row.items() if k not in {'accountID', 'userID'}})
        ids = [int(x['id']) for x in clean]
        last = _id(p.get('lastTransactionID'))
        if ids != sorted(set(ids)) or any(x <= int(transaction_id) or x > int(last) for x in ids) or int(last) < int(transaction_id):
            _fail('transaction_range_conflict')
        return {'rows': clean, 'lastTransactionID': last,
                'observed_epoch': r['observed_epoch'], 'receipt': r}

    def pricing(self, instruments):
        if not isinstance(instruments, (list, tuple)) or not 1 <= len(instruments) <= 68:
            _fail('invalid_pricing_scope')
        names = sorted({_instrument(x) for x in instruments})
        if len(names) != len(instruments):
            _fail('duplicate_pricing_instrument')
        p, r = self._request('GET', self._account_path('/pricing'), route='pricing',
                             params={'instruments': ','.join(names), 'includeHomeConversions': 'true'})
        result = {}
        for row in _rows(p, 'prices', 68):
            inst = _instrument(row.get('instrument'))
            if inst not in names or inst in result or type(row.get('tradeable')) is not bool:
                _fail('invalid_price_identity_or_tradeability')
            bids, asks = _rows(row, 'bids', 64), _rows(row, 'asks', 64)
            if not bids or not asks:
                _fail('missing_executable_price')
            bid, ask = _decimal_text(bids[0].get('price')), _decimal_text(asks[0].get('price'))
            if _num(bid) > _num(ask):
                _fail('crossed_price')
            market = _epoch(row.get('time'))
            if market > r['observed_epoch']:
                _fail('future_price')
            result[inst] = {'instrument': inst, 'bid': bid, 'ask': ask, 'tradeable': row['tradeable'],
                            'market_epoch': market, 'market_time': row['time'],
                            'observed_epoch': r['observed_epoch'], 'source_sha256': r['source_sha256']}
        conversions = []
        for row in _rows(p, 'homeConversions', 32):
            ccy = row.get('currency')
            if not isinstance(ccy, str) or not re.fullmatch('[A-Z]{3}', ccy):
                _fail('invalid_conversion_currency')
            conversions.append({'currency': ccy, **{k: _decimal_text(row.get(k)) for k in ['accountGain', 'accountLoss', 'positionValue']}})
        if len({x['currency'] for x in conversions}) != len(conversions):
            _fail('duplicate_conversion_currency')
        return {'quotes': result, 'home_conversions': {'rows': conversions, 'observed_epoch': r['observed_epoch']},
                'missing_instruments': sorted(set(names) - set(result)), 'receipt': r}

    def completed_m1_candles(self, instrument, count=17):
        inst = _instrument(instrument)
        if type(count) is not int or not 16 <= count <= 32:
            _fail('invalid_candle_count')
        p, r = self._request('GET', '/instruments/' + inst + '/candles', route='completed_m1_candles',
                             params={'price': 'M', 'granularity': 'M1', 'count': str(count), 'smooth': 'false'})
        if p.get('instrument') != inst or p.get('granularity') != 'M1':
            _fail('candle_response_identity_mismatch')
        rawrows = _rows(p, 'candles', count)
        rows, labels = [], []
        for row in rawrows:
            label = _epoch(row.get('time'))
            if type(row.get('complete')) is not bool or label % 60 != 0:
                _fail('invalid_candle_clock_or_complete')
            labels.append(label)
            if row['complete']:
                if label + 60 > r['observed_epoch']:
                    _fail('future_complete_candle')
                mid = row.get('mid')
                if not isinstance(mid, dict):
                    _fail('missing_mid_candle')
                m = {k: _decimal_text(mid.get(k)) for k in ['h', 'l', 'c']}
                if not _num(m['l']) <= _num(m['c']) <= _num(m['h']):
                    _fail('invalid_candle_ohlc')
                rows.append({'label_epoch': label, 'label_time': row['time'], 'complete': True, 'mid': m})
        if labels != sorted(set(labels)):
            _fail('unordered_or_duplicate_candles')
        return {'instrument': inst, 'observed_epoch': r['observed_epoch'], 'source_sha256': r['source_sha256'],
                'rows': rows[-15:], 'input_row_count': len(rawrows), 'complete_row_count': len(rows), 'receipt': r}

    def prepare_market_order(self, intent_id, instrument, units, stop_loss, take_profit=None, *, price_bound, target_epoch):
        return self._build_prepared(intent_id, instrument, units, stop_loss, take_profit,
                                    price_bound=price_bound, target_epoch=target_epoch,
                                    prepared_epoch=self._now())

    def _build_prepared(self, intent_id, instrument, units, stop_loss, take_profit=None, *, price_bound, target_epoch, prepared_epoch):
        if not isinstance(intent_id, str) or not re.fullmatch(r'[a-zA-Z0-9_.:-]{1,128}', intent_id):
            _fail('invalid_intent_id')
        inst = _instrument(instrument)
        n = _num(units)
        units_text = format(n, 'f')
        if '.' in units_text:
            units_text = units_text.rstrip('0').rstrip('.')
        fraction = units_text.split('.', 1)[1] if '.' in units_text else ''
        if n == 0 or len(fraction) > 6 or n.copy_abs() > 100000000:
            _fail('invalid_units_precision_or_bound')
        stop, bound = _decimal_text(stop_loss), _decimal_text(price_bound)
        take = None if take_profit is None else _decimal_text(take_profit)
        if (n > 0 and _num(stop) >= _num(bound)) or (n < 0 and _num(stop) <= _num(bound)):
            _fail('stop_wrong_side')
        if take is not None and ((n > 0 and _num(take) <= _num(bound)) or (n < 0 and _num(take) >= _num(bound))):
            _fail('take_profit_wrong_side')
        now, target = _clock(prepared_epoch), _clock(target_epoch)
        if not now < target <= now + 86400:
            _fail('invalid_original_target')
        cid = 'pt1-' + _sha(_json([self.trial_id, intent_id]))[:40]
        comment = self.trial_id + '|target=' + repr(target)
        ext = {'id': cid, 'tag': TAG, 'comment': comment}
        order = {'type': 'MARKET', 'instrument': inst, 'units': units_text,
                 'timeInForce': 'FOK', 'positionFill': 'OPEN_ONLY', 'priceBound': bound,
                 'stopLossOnFill': {'price': stop, 'timeInForce': 'GTC'},
                 'clientExtensions': ext, 'tradeClientExtensions': deepcopy(ext)}
        if take is not None:
            order['takeProfitOnFill'] = {'price': take, 'timeInForce': 'GTC'}
        prepared = {'schema_version': SCHEMA, 'environment': 'practice', 'trial_id': self.trial_id,
                    'account_sha256': self.account_sha256, 'intent_id': intent_id,
                    'original_target_epoch': target, 'prepared_epoch': now, 'order': order}
        prepared['intent_sha256'] = _sha(_json(prepared))
        return prepared

    def _prepared(self, value):
        if not isinstance(value, dict) or len(_json(value)) > 16384:
            _fail('invalid_prepared_intent')
        p = deepcopy(value)
        seal = p.pop('intent_sha256', None)
        if seal != _sha(_json(p)) or p.get('schema_version') != SCHEMA or p.get('environment') != 'practice' or p.get('trial_id') != self.trial_id or p.get('account_sha256') != self.account_sha256:
            _fail('prepared_intent_binding_mismatch')
        order = p.get('order')
        if not isinstance(order, dict):
            _fail('invalid_prepared_order')
        # Exact reconstruction prevents a caller-sealed alternate order/authority.
        try:
            expected = self._build_prepared(p['intent_id'], order.get('instrument'), order.get('units'),
                (order.get('stopLossOnFill') or {}).get('price'),
                (order.get('takeProfitOnFill') or {}).get('price'), price_bound=order.get('priceBound'),
                target_epoch=p['original_target_epoch'], prepared_epoch=p['prepared_epoch'])
        except (KeyError, TypeError):
            _fail('invalid_prepared_order')
        if expected != value:
            _fail('prepared_order_reconstruction_mismatch')
        return deepcopy(value)

    def _trade(self, trade_id):
        p, r = self._request('GET', self._account_path('/trades/' + _id(trade_id)), route='exact_trade')
        trade = p.get('trade')
        if not isinstance(trade, dict) or trade.get('id') != trade_id:
            _fail('trade_response_identity_mismatch')
        return trade, r

    def exact_trade(self, trade_id):
        trade, receipt = self._trade(trade_id)
        return {'trade': deepcopy(trade), 'receipt': receipt}

    def owned_trade(self, trade, expected=None):
        prepared = None if expected is None else self._prepared(expected)
        return self._owned(trade, expected=prepared)

    def _owned(self, trade, *, instrument=None, expected=None):
        if not isinstance(trade, dict):
            _fail('invalid_trade')
        _id(trade.get('id')); _instrument(trade.get('instrument'))
        ext = trade.get('clientExtensions')
        if not isinstance(ext, dict) or ext.get('tag') != TAG or not re.fullmatch(r'pt1-[0-9a-f]{40}', str(ext.get('id', ''))):
            _fail('unowned_trade')
        prefix = self.trial_id + '|target='
        comment = ext.get('comment')
        if not isinstance(comment, str) or not comment.startswith(prefix):
            _fail('foreign_trial_trade')
        try:
            target = _clock(float(comment[len(prefix):]))
        except (ValueError, TypeError):
            _fail('invalid_owned_trade_target')
        if comment != prefix + repr(target):
            _fail('noncanonical_owned_trade_target')
        if instrument is not None and trade['instrument'] != _instrument(instrument):
            _fail('owned_trade_instrument_mismatch')
        if expected is not None and ext != expected['order']['tradeClientExtensions']:
            _fail('owned_trade_intent_mismatch')
        return target

    def reconcile_market(self, prepared):
        p = self._prepared(prepared); expected = p['order']; cid = expected['clientExtensions']['id']
        try:
            payload, receipt = self._request('GET', self._account_path('/orders/@' + quote(cid, safe='')),
                                             route='order_by_client_id', missing_ok=True)
            if payload is None:
                return {'status': 'unknown', 'reason_code': 'order_not_observed', 'intent_sha256': p['intent_sha256'], 'receipt': receipt}
            order = payload.get('order')
            if not isinstance(order, dict) or order.get('clientExtensions') != expected['clientExtensions']:
                _fail('order_identity_conflict')
            for k in ['type', 'instrument', 'positionFill', 'timeInForce', 'tradeClientExtensions']:
                if order.get(k) != expected.get(k):
                    _fail('order_contract_conflict')
            for k in ['units', 'priceBound']:
                if _num(order.get(k)) != _num(expected[k]):
                    _fail('order_numeric_contract_conflict')
            for k in ['stopLossOnFill', 'takeProfitOnFill']:
                actual = order.get(k)
                if k not in expected:
                    if actual is not None:
                        _fail('unexpected_protection_detail')
                elif not isinstance(actual, dict) or _num(actual.get('price')) != _num(expected[k]['price']) or actual.get('timeInForce') != 'GTC' or actual.get('distance') is not None or actual.get('triggerCondition', 'DEFAULT') != 'DEFAULT':
                    _fail('order_protection_contract_conflict')
            oid = _id(order.get('id'))
            created = _epoch(order.get('createTime'))
            if created > receipt['observed_epoch']:
                _fail('future_order')
            if order.get('state') == 'CANCELLED':
                if order.get('fillingTransactionID') or order.get('tradeOpenedID') or order.get('tradeReducedID') or order.get('tradeClosedIDs'):
                    _fail('cancelled_order_fill_conflict')
                return {'status': 'not_filled', 'reason_code': 'broker_order_cancelled', 'intent_sha256': p['intent_sha256'], 'receipt': receipt, 'order_id': oid}
            if order.get('state') != 'FILLED':
                return {'status': 'unknown', 'reason_code': 'order_not_terminal', 'intent_sha256': p['intent_sha256'], 'receipt': receipt}
            txid = _id(order.get('fillingTransactionID'))
            filled = _epoch(order.get('filledTime'))
            if not created <= filled <= receipt['observed_epoch']:
                _fail('order_fill_clock_conflict')
            txp, txr = self._request('GET', self._account_path('/transactions/' + txid), route='exact_fill_transaction')
            tx = txp.get('transaction')
            if not isinstance(tx, dict) or tx.get('accountID') != self._account_id or tx.get('id') != txid or tx.get('type') != 'ORDER_FILL' or tx.get('orderID') != oid or tx.get('clientOrderID') != cid or tx.get('instrument') != expected['instrument'] or _num(tx.get('units')) != _num(expected['units']):
                _fail('fill_transaction_identity_conflict')
            if tx.get('tradesClosed') or tx.get('tradeReduced') or not isinstance(tx.get('tradeOpened'), dict):
                _fail('unexpected_netting_or_missing_open')
            if _epoch(tx.get('time')) != filled or filled > txr['observed_epoch']:
                _fail('fill_transaction_clock_conflict')
            fill_price = _num(tx.get('price'), positive=True)
            if (_num(expected['units']) > 0 and fill_price > _num(expected['priceBound'])) or (_num(expected['units']) < 0 and fill_price < _num(expected['priceBound'])):
                _fail('fill_price_bound_conflict')
            trade, tr = self._trade(_id(tx['tradeOpened'].get('tradeID')))
            self._owned(trade, instrument=expected['instrument'], expected=p)
            if _epoch(trade.get('openTime')) != filled or filled > tr['observed_epoch']:
                _fail('opened_trade_clock_conflict')
            if _num(trade.get('initialUnits')) != _num(expected['units']):
                _fail('opened_trade_units_conflict')
            state = trade.get('state')
            if state == 'OPEN':
                if _num(trade.get('currentUnits')) != _num(expected['units']):
                    _fail('opened_trade_reduced_during_confirmation')
                for detail, attached in [('stopLossOnFill', 'stopLossOrder'), ('takeProfitOnFill', 'takeProfitOrder')]:
                    if detail in expected:
                        protection = trade.get(attached)
                        if not isinstance(protection, dict) or protection.get('state') != 'PENDING' or _num(protection.get('price')) != _num(expected[detail]['price']):
                            _fail('protective_order_not_confirmed')
            elif state != 'CLOSED' or _num(trade.get('currentUnits')) != 0:
                _fail('invalid_opened_trade_state')
            return {'status': 'filled_open' if state == 'OPEN' else 'filled_closed', 'intent_sha256': p['intent_sha256'],
                    'order_id': oid, 'fill_transaction_id': txid, 'trade_id': trade['id'],
                    'instrument': expected['instrument'], 'units': expected['units'],
                    'original_target_epoch': p['original_target_epoch'], 'trade': deepcopy(trade),
                    'transaction': {k: deepcopy(v) for k, v in tx.items() if k not in {'accountID', 'userID'}},
                    'receipt': tr, 'verification_receipts': [receipt, txr, tr]}
        except BrokerError as exc:
            return {'status': 'unknown', 'reason_code': exc.code, 'intent_sha256': p['intent_sha256']}

    def submit_market(self, prepared, *, claim_submission, validate_before_submit):
        if not callable(claim_submission) or not callable(validate_before_submit):
            _fail('durable_submission_claim_required')
        with self._lock:
            p = self._prepared(prepared)
            cid = p['order']['clientExtensions']['id']
            # Existing intents (also after restart) are reconciliation-only.
            if cid in self._attempted:
                return self.reconcile_market(p)
            initial = self.reconcile_market(p)
            if initial.get('reason_code') != 'order_not_observed':
                return initial
            now = self._now()
            if not p['prepared_epoch'] <= now < p['original_target_epoch'] or now - p['prepared_epoch'] > 15:
                _fail('intent_clock_or_target_expired')
            if len(self._attempted) >= 10000:
                _fail('submission_attempt_bound')
            self._attempted.add(cid)
            try:
                claimed = claim_submission(deepcopy(p))
            except Exception:
                return {'status': 'not_submitted', 'reason_code': 'durable_submission_claim_failed', 'intent_sha256': p['intent_sha256']}
            if type(claimed) is not bool:
                return {'status': 'not_submitted', 'reason_code': 'invalid_submission_claim_result', 'intent_sha256': p['intent_sha256']}
            if not claimed:
                return self.reconcile_market(p)
            # Own complete reads directly before entry. Caller additionally holds
            # the account-wide lock; these separate broker reads are not atomic.
            try:
                account, trades, pending = self.account_summary(), self.open_trades(), self.pending_orders()
            except BrokerError as exc:
                return {'status': 'not_submitted', 'reason_code': 'pretransport_' + exc.code, 'intent_sha256': p['intent_sha256']}
            if account['openTradeCount'] or account['pendingOrderCount'] or account.get('openPositionCount', 0) or trades['rows'] or pending['rows']:
                return {'status': 'not_submitted', 'reason_code': 'account_not_flat_before_submission',
                        'intent_sha256': p['intent_sha256'],
                        'flatness_receipts': [account['receipt'], trades['receipt'], pending['receipt']]}
            # Caller revalidates fixed policy, fresh quotes/conversions and exact
            # already-claimed sizing against this new account observation.
            try:
                policy_ok = validate_before_submit(deepcopy(p), deepcopy(account),
                    {'open_trades': deepcopy(trades), 'pending_orders': deepcopy(pending)})
            except Exception:
                return {'status': 'not_submitted', 'reason_code': 'final_policy_validation_failed', 'intent_sha256': p['intent_sha256']}
            if type(policy_ok) is not bool:
                return {'status': 'not_submitted', 'reason_code': 'invalid_final_policy_validation', 'intent_sha256': p['intent_sha256']}
            if not policy_ok:
                return {'status': 'not_submitted', 'reason_code': 'final_policy_refused', 'intent_sha256': p['intent_sha256']}
            # Claim completion may have consumed the final target time.
            now = self._now()
            if not p['prepared_epoch'] <= now < p['original_target_epoch'] or now - p['prepared_epoch'] > 15 or now - account['observed_epoch'] > 30 or now - min(trades['observed_epoch'], pending['observed_epoch']) > 15:
                return {'status': 'not_submitted', 'reason_code': 'intent_or_observation_expired_after_claim', 'intent_sha256': p['intent_sha256']}
            write = None
            try:
                submit_not_after = min(p['original_target_epoch'], p['prepared_epoch'] + 15,
                    account['observed_epoch'] + 30, trades['observed_epoch'] + 15, pending['observed_epoch'] + 15)
                _, write = self._request('POST', self._account_path('/orders'), route='submit_market',
                                          body={'order': p['order']}, submit_not_after=submit_not_after)
            except BrokerError as exc:
                if exc.code == 'submit_deadline_elapsed':
                    return {'status': 'not_submitted', 'reason_code': exc.code, 'intent_sha256': p['intent_sha256']}
                write = {'status': 'unresolved', 'reason_code': exc.code, 'http_status': exc.http_status}
            result = self.reconcile_market(p)
            result['submission_receipt'] = write
            return result

    def close_owned_trade(self, trade_id, instrument, *, original_target_epoch=None, horizon_due=False):
        with self._lock:
            trade, before = self._trade(_id(trade_id))
            target = self._owned(trade, instrument=instrument)
            if original_target_epoch is not None and _clock(original_target_epoch) != target:
                _fail('original_target_mismatch')
            if type(horizon_due) is not bool:
                _fail('invalid_horizon_due')
            if horizon_due and before['observed_epoch'] < target:
                _fail('original_horizon_not_due')
            if trade.get('state') == 'CLOSED' and _num(trade.get('currentUnits')) == 0:
                return {'status': 'confirmed_closed', 'complete_close': True, 'transaction_attribution': False,
                        'original_target_epoch': target, 'receipt': before, 'already_closed': True}
            if trade.get('state') != 'OPEN':
                _fail('owned_trade_not_open')
            if _num(trade.get('currentUnits')) == 0:
                _fail('zero_units_open_trade')
            write = None
            try:
                _, write = self._request('PUT', self._account_path('/trades/' + trade_id + '/close'),
                                         route='close_owned_trade', body={'units': 'ALL'})
            except BrokerError as exc:
                write = {'status': 'unresolved', 'reason_code': exc.code, 'http_status': exc.http_status}
            try:
                after = self.open_trades()
                observation = observe_trade_rows(after['rows'], trade_id=trade_id, instrument=instrument,
                                                  observed_epoch=after['observed_epoch'])
                result = reconcile_reduction(trade, observation)
                result.update(receipt=after['receipt'], original_target_epoch=target,
                              submission_receipt=write, before_receipt=before)
                return result
            except BrokerError as exc:
                return {'status': 'unknown', 'reason_code': exc.code, 'complete_close': False,
                        'transaction_attribution': False, 'original_target_epoch': target,
                        'submission_receipt': write, 'before_receipt': before}
