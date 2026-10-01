"""Opt-in bounded raw-price receipts. No network, account or order capability.

Uses the existing WAL publisher. The sidecar is separate from legacy float
snapshots. A consumer's actual read completion sets availability conservatively;
the transport publication timestamp is a write-start clock, not a commit claim.
"""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path
import time
import uuid

try:
    import oanda_research_quote_receipt_v1 as base
    from oanda_quote_transport import QuoteSnapshotPublisher, load_quote_snapshot
    from oanda_curve_management_replay_v1 import quote_at
except ModuleNotFoundError:
    from trad import oanda_research_quote_receipt_v1 as base
    from trad.oanda_quote_transport import QuoteSnapshotPublisher, load_quote_snapshot
    from trad.oanda_curve_management_replay_v1 import quote_at

SCHEMA = 'exact_stream_quote_receipts_v1'
MAX_RAW = 16384


def digest(value):
    return hashlib.sha256(base._bytes(value)).hexdigest()


def price_text(value):
    base._need(isinstance(value, str) and len(value) <= 64, 'original_decimal_string_required')
    base._need(base.re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value) is not None, 'invalid_decimal_string')
    base._number(Decimal(value))
    return value


def parse_price(raw, *, received_epoch, generation, session_id):
    """Preserve provider price strings and timestamp, before any float conversion."""
    base._need(type(raw) is bytes and 0 < len(raw) <= MAX_RAW, 'raw_price_bound')
    received = base._epoch(received_epoch)
    base._need(type(generation) is int and 0 < generation < 2**31, 'connection_generation')
    base._need(isinstance(session_id, str) and base.re.fullmatch(r'[A-Za-z0-9_-]{1,80}', session_id), 'session_identity')
    value = base._decode(raw)
    base._need(isinstance(value, dict) and value.get('type') == 'PRICE', 'not_price')
    pair = value.get('instrument')
    base._need(isinstance(pair, str) and base._PAIR.fullmatch(pair), 'invalid_instrument')
    for side in ('bids', 'asks'):
        levels = value.get(side)
        base._need(isinstance(levels, list) and 1 <= len(levels) <= 64 and isinstance(levels[0], dict), 'price_levels')
    bid, ask = price_text(value['bids'][0].get('price')), price_text(value['asks'][0].get('price'))
    base._need(Decimal(bid) <= Decimal(ask), 'crossed_quote')
    market = base._time(value.get('time'))
    base._need(market <= Decimal(str(received)), 'future_market_at_receipt')
    # Explicit OANDA boolean wins only if consistent with any supplied status.
    flag, status = value.get('tradeable'), value.get('status')
    base._need(type(flag) is bool, 'explicit_tradeability_required')
    base._need(status is None or status == ('tradeable' if flag else 'non-tradeable'), 'tradeability_conflict')
    body = dict(instrument=pair, bid=bid, ask=ask, market_time_rfc3339=value['time'],
                market_epoch=float(market), received_epoch=received, tradeable=flag,
                connection_generation=generation, session_id=session_id,
                raw_sha256=hashlib.sha256(raw).hexdigest(), raw_price_utf8=raw.decode('utf-8'))
    return {**body, 'quote_id': 'exact_quote_' + digest(body)}


class ExactQuoteReceiptPublisher:
    """Callable raw-price observer for MultiPriceStream, explicitly opt-in.

    None is a generation boundary. Invalid updates remove the previous quote,
    never leave an old tradeable price silently current. Storage retains four
    coalesced snapshots, not an unbounded tick archive. Call flush after a batch
    or on close; repeated ticks otherwise publish at most once per second.
    """
    def __init__(self, path: Path, instruments, *, clock=time.time, session_id=None):
        pairs = list(instruments)
        base._need(0 < len(pairs) <= base.MAX_QUOTES and len(set(pairs)) == len(pairs)
                   and all(isinstance(p, str) and base._PAIR.fullmatch(p) for p in pairs), 'instrument_inventory')
        self.instruments = sorted(pairs)
        self.clock = clock
        self.session_id = session_id or uuid.uuid4().hex
        self.generation = 0
        self.quotes, self.refusals = {}, {}
        self.last_published = 0.0
        self.publisher = QuoteSnapshotPublisher(path)

    def __call__(self, raw, *, received_epoch, connection_generation):
        received = base._epoch(received_epoch)
        base._need(type(connection_generation) is int and 0 < connection_generation < 2**31, 'connection_generation')
        base._need(connection_generation >= self.generation, 'generation_regression')
        if connection_generation != self.generation:
            self.generation = connection_generation
            self.quotes, self.refusals = {}, {}
        if raw is None:
            self.quotes, self.refusals = {}, {}
            return self.flush()
        pair = None
        try:
            base._need(type(raw) is bytes and 0 < len(raw) <= MAX_RAW, 'raw_price_bound')
            value = base._decode(raw)
            pair = value.get('instrument') if isinstance(value, dict) else None
            base._need(pair in self.instruments, 'instrument_outside_inventory')
            quote = parse_price(raw, received_epoch=received, generation=self.generation, session_id=self.session_id)
            old = self.quotes.get(pair)
            base._need(old is None or base._time(quote['market_time_rfc3339']) >= base._time(old['market_time_rfc3339']), 'market_clock_regression')
            self.quotes[pair] = quote
            self.refusals.pop(pair, None)
        except (base.QuoteReceiptError, TypeError) as exc:
            if isinstance(pair, str) and pair in self.instruments:
                self.quotes.pop(pair, None)
                self.refusals[pair] = str(exc)
            else:
                # An unidentifiable malformed message cannot leave the whole
                # previously published map silently eligible.
                self.quotes.clear()
                self.refusals = {p: 'unidentified_raw_price' for p in self.instruments}
        if received - self.last_published >= 1 or pair in self.refusals:
            return self.flush()
        return None

    def flush(self):
        created = base._epoch(self.clock())
        body = dict(schema=SCHEMA, producer='exact_raw_price_sidecar', session_id=self.session_id,
                    connection_generation=self.generation, snapshot_created_epoch=created,
                    instruments=self.instruments, quotes=dict(self.quotes), refusals=dict(self.refusals),
                    quote_count=len(self.quotes), research_only=True, can_place_orders=False)
        result = self.publisher.submit({**body, 'snapshot_sha256': digest(body)})
        self.last_published = created
        return result

    def close(self):
        if self.generation:
            self.flush()
        self.publisher.close()


def map_receipts(snapshot, *, observed_epoch, decision_epoch, instruments, maximum_age_sec=30):
    """Verify raw receipt content, connection and causal clocks before quote_at."""
    observed, decision = base._epoch(observed_epoch), base._epoch(decision_epoch)
    base._need(decision >= observed, 'decision_precedes_observation')
    base._need(type(maximum_age_sec) in (int, float) and 0 <= maximum_age_sec <= 60, 'quote_age_policy')
    base._need(isinstance(snapshot, dict), 'snapshot_shape')
    body = {k: v for k, v in snapshot.items() if k not in ('snapshot_sha256', 'transport')}
    base._need(snapshot.get('snapshot_sha256') == digest(body), 'snapshot_hash')
    base._need(body.get('schema') == SCHEMA and body.get('producer') == 'exact_raw_price_sidecar'
               and body.get('research_only') is True and body.get('can_place_orders') is False, 'snapshot_identity')
    selected = list(instruments)
    base._need(selected == body.get('instruments') and len(selected) == len(set(selected))
               and 0 < len(selected) <= base.MAX_QUOTES, 'exact_instrument_inventory')
    rows, refusals = body.get('quotes'), body.get('refusals')
    base._need(isinstance(rows, dict) and isinstance(refusals, dict) and set(rows) <= set(selected)
               and set(refusals) <= set(selected) and not set(rows) & set(refusals)
               and body.get('quote_count') == len(rows), 'quote_population')
    base._need(type(body.get('connection_generation')) is int and 0 < body['connection_generation'] < 2**31
               and isinstance(body.get('session_id'), str)
               and base.re.fullmatch(r'[A-Za-z0-9_-]{1,80}', body['session_id']), 'snapshot_session_generation')
    created = base._epoch(body.get('snapshot_created_epoch'))
    base._need(created <= observed, 'snapshot_future_at_observation')
    publication = (snapshot.get('transport') or {}).get('publication_started_epoch')
    if publication is not None:
        base._need(created <= base._epoch(publication) <= observed, 'publication_clock_order')
    quotes, rejected = {}, {}
    for pair in selected:
        try:
            base._need(pair in rows, str(refusals.get(pair, 'quote_missing')))
            row = rows[pair]
            rebuilt = parse_price(row['raw_price_utf8'].encode(), received_epoch=row['received_epoch'],
                                  generation=body['connection_generation'], session_id=body['session_id'])
            base._need(row == rebuilt and row['instrument'] == pair, 'raw_receipt_identity')
            base._need(row['received_epoch'] <= created, 'received_after_snapshot')
            # Compare original nanosecond market clock before its manager float representation.
            base._need(base._age(decision, base._time(row['market_time_rfc3339'])) <= Decimal(str(maximum_age_sec)), 'market_stale')
            mapped = {k: v for k, v in row.items() if k != 'raw_price_utf8'}
            mapped['available_epoch'] = observed
            quote_at({pair: mapped}, pair, decision, maximum_age_sec)
            quotes[pair] = mapped
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            rejected[pair] = str(exc)
    return dict(schema=SCHEMA, quotes=quotes, refusals=rejected, observed_epoch=observed,
                decision_epoch=decision, snapshot_sha256=snapshot['snapshot_sha256'],
                publication_started_epoch=publication,
                research_only=True, can_place_orders=False, manager_activation=False)


def read_receipts(path, *, instruments, clock=time.time, maximum_age_sec=30):
    snapshot = load_quote_snapshot(path)
    observed = base._epoch(clock())
    return map_receipts(snapshot, observed_epoch=observed, decision_epoch=observed,
                        instruments=instruments, maximum_age_sec=maximum_age_sec)
