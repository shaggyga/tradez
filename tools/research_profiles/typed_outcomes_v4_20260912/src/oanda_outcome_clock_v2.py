"""Explicit per-pair outcome clocks. No offset estimation or network activity."""
from __future__ import annotations
from datetime import datetime, timezone
import math

CONTRACT = 'per_pair_quote_receipt_maturity_v2_20260912'


def number(value):
    if value is None or isinstance(value, bool):
        raise ValueError('finite_number_required')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError('finite_number_required')
    return result


def epoch(value):
    """Require an explicit timezone for source clocks; never use local timezone."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError('explicit_source_clock_required')
    stamp = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError('source_clock_timezone_required')
    result = stamp.timestamp()
    if not math.isfinite(result) or result <= 0:
        raise ValueError('invalid_source_clock')
    return result


def iso(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def build_market_quote_snapshot(payload, *, now, max_quote_age_sec):
    """Normalize either original market quotes or timestamped feature quotes.

    `generated_utc`/`generated_epoch` is the producer's recorded capture clock.
    The compatibility name source_received_epoch denotes that capture receipt,
    not a measured transport-arrival timestamp. `now` is this process's read
    clock. Neither is a replacement for the original pair quote event clock.
    """
    read_epoch = number(now)
    maximum_age = number(max_quote_age_sec)
    if read_epoch <= 0 or maximum_age < 0:
        raise ValueError('invalid_snapshot_clock_bounds')
    if not isinstance(payload, dict):
        raise ValueError('snapshot_mapping_required')
    result = {'clock_contract': CONTRACT, 'generated_epoch': read_epoch,
              'generated_utc': iso(read_epoch), 'read_epoch': read_epoch,
              'source_received_epoch': None, 'source_received_time': payload.get('generated_utc'),
              'max_quote_age_sec': maximum_age, 'instrument_count': 0,
              'instruments': {}, 'rejections': {}, 'rejected_quote_clocks': {}, 'status': 'blocked_source_receipt',
              'source': str(payload.get('producer') or payload.get('source') or 'timestamped_quote_payload'),
              'source_clock_kind': 'producer_recorded_capture',
              'clock_offset_applied': False}
    raw_rows = payload.get('quotes') if 'quotes' in payload else payload.get('instruments')
    if not isinstance(raw_rows, dict):
        result['receipt_rejection'] = 'quote_mapping_required'
        return result
    if len(raw_rows)>256:
        raise ValueError('quote_population_bound_exceeded')
    # Even a blocked receipt retains the original bounded pair clock evidence.
    for instrument, raw in raw_rows.items():
        quote = raw.get('quote', raw) if isinstance(raw, dict) else {}
        original = quote.get('time') if isinstance(quote, dict) else None
        original = original if isinstance(original, str) else None
        try:
            parsed = epoch(original)
        except (TypeError, ValueError, OverflowError):
            parsed = None
        result['rejected_quote_clocks'][str(instrument)] = {'time': original, 'quote_epoch': parsed}
    try:
        by_text = epoch(payload['generated_utc']) if payload.get('generated_utc') else None
        by_number = number(payload['generated_epoch']) if 'generated_epoch' in payload else None
        if by_text is None and by_number is None:
            raise ValueError('source_receipt_clock_required')
        if by_text is not None and by_number is not None and abs(by_text-by_number) > 1e-6:
            raise ValueError('source_receipt_clocks_disagree')
        received = by_text if by_text is not None else by_number
        if received <= 0:
            raise ValueError('invalid_source_receipt_clock')
        result['source_received_epoch'] = received
        if received > read_epoch:
            raise ValueError('source_receipt_is_future')
        if read_epoch-received > maximum_age:
            raise ValueError('source_receipt_is_stale')
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        result['receipt_rejection'] = str(exc) if isinstance(exc, ValueError) else 'invalid_source_receipt_clock'
        return result
    for instrument, raw in raw_rows.items():
        if not isinstance(instrument, str) or not isinstance(raw, dict):
            result['rejections'][str(instrument)] = 'invalid_quote_record'
            continue
        quote = raw.get('quote') if 'quote' in raw else raw
        clock = None
        try:
            if not isinstance(quote, dict):
                raise ValueError('invalid_quote_record')
            clock = epoch(quote.get('time'))
            bid, ask = number(quote.get('bid')), number(quote.get('ask'))
            if bid <= 0 or ask < bid:
                raise ValueError('invalid_bid_ask')
            if clock > read_epoch:
                raise ValueError('quote_is_future_at_read')
            if clock > received:
                raise ValueError('quote_is_after_source_receipt')
            if read_epoch-clock > maximum_age:
                raise ValueError('quote_is_stale_at_read')
            result['instruments'][instrument] = {'quote': {
                'bid': bid, 'ask': ask, 'time': quote['time'], 'quote_epoch': clock,
                'source_received_epoch': received, 'read_epoch': read_epoch}}
            result['rejected_quote_clocks'].pop(instrument, None)
        except (TypeError, ValueError, OverflowError) as exc:
            result['rejections'][instrument] = str(exc) if isinstance(exc, ValueError) else 'invalid_quote_value'
    rows = result['instruments']
    result['instrument_count'] = len(rows)
    result['status'] = 'current_quotes' if rows else 'no_eligible_quotes'
    clocks = [row['quote']['quote_epoch'] for row in rows.values()]
    result['oldest_quote_epoch'] = min(clocks) if clocks else None
    result['newest_quote_epoch'] = max(clocks) if clocks else None
    result['oldest_quote_age_sec'] = read_epoch-min(clocks) if clocks else None
    return result


def validate_snapshot(snapshot, *, now):
    current = number(now)
    if not isinstance(snapshot, dict) or current <= 0 or snapshot.get('clock_contract') != CONTRACT:
        raise ValueError('outcome_clock_contract_required')
    read_epoch = number(snapshot.get('read_epoch'))
    maximum_age = number(snapshot.get('max_quote_age_sec'))
    if maximum_age < 0 or read_epoch > current or read_epoch <= 0:
        raise ValueError('invalid_snapshot_read_clock')
    if current-read_epoch > maximum_age:
        raise ValueError('snapshot_read_is_stale_at_evaluation')
    rows = snapshot.get('instruments')
    if not isinstance(rows, dict) or len(rows)>256:
        raise ValueError('snapshot_instruments_required')
    if (snapshot.get('clock_offset_applied') is not False or
        snapshot.get('source_clock_kind') != 'producer_recorded_capture' or
        snapshot.get('instrument_count') != len(rows)):
        raise ValueError('snapshot_metadata_contract_mismatch')
    if not isinstance(snapshot.get('rejections'),dict) or not isinstance(snapshot.get('rejected_quote_clocks'),dict):
        raise ValueError('snapshot_rejection_evidence_required')
    if snapshot.get('status') == 'blocked_source_receipt':
        if rows or not isinstance(snapshot.get('receipt_rejection'),str):
            raise ValueError('blocked_snapshot_has_eligible_quotes')
    elif snapshot.get('status') not in ('current_quotes','no_eligible_quotes') or snapshot.get('receipt_rejection'):
        raise ValueError('snapshot_status_contract_mismatch')
    if rows:
        received=number(snapshot.get('source_received_epoch'))
        if received<=0 or received>read_epoch or read_epoch-received>maximum_age:
            raise ValueError('invalid_snapshot_receipt_clock')
    return current, read_epoch, maximum_age


def quote_for_maturity(snapshot, instrument, *, target_epoch, max_delay_sec, now):
    """Select only an actually post-target quote with timely source receipt.

    Quote delay, source receipt delay and evaluation delay are different. A
    fresh retained quote can be evaluated after the deadline only if its source
    receipt and quote clocks were both within the terminal observation window.
    """
    current, read_epoch, maximum_age = validate_snapshot(snapshot, now=now)
    target, delay_limit = number(target_epoch), number(max_delay_sec)
    if target <= 0 or delay_limit < 0:
        raise ValueError('invalid_maturity_bounds')
    reason = snapshot.get('receipt_rejection') or snapshot.get('rejections', {}).get(instrument) or 'missing_pair_quote'
    record = snapshot['instruments'].get(instrument)
    if record is not None and not isinstance(record,dict):
        raise ValueError('invalid_pair_snapshot_record')
    raw = (record or {}).get('quote')
    if raw is not None and not isinstance(raw,dict):
        raise ValueError('invalid_pair_snapshot_quote')
    retained = snapshot.get('rejected_quote_clocks', {}).get(instrument) or {}
    raw_time = retained.get('time')
    raw_epoch = retained.get('quote_epoch')
    if raw is not None:
        try:
            raw_time = raw.get('time')
            quote_epoch = epoch(raw.get('time'))
            raw_epoch = quote_epoch
            if quote_epoch != number(raw.get('quote_epoch')):
                raise ValueError('pair_clock_fields_disagree')
            received = number(raw.get('source_received_epoch'))
            if received != number(snapshot.get('source_received_epoch')) or number(raw.get('read_epoch')) != read_epoch:
                raise ValueError('pair_receipt_fields_disagree')
            bid, ask = number(raw.get('bid')), number(raw.get('ask'))
            if bid <= 0 or ask < bid:
                raise ValueError('invalid_bid_ask')
            if quote_epoch > received or received > read_epoch or read_epoch > current:
                raise ValueError('invalid_quote_receipt_order')
            if current-quote_epoch > maximum_age:
                raise ValueError('quote_is_stale_at_evaluation')
            if quote_epoch < target:
                raise ValueError('pair_quote_precedes_target')
            if quote_epoch-target > delay_limit:
                raise ValueError('pair_quote_after_terminal_window')
            if received-target > delay_limit:
                raise ValueError('quote_receipt_after_terminal_window')
            return {'status':'matured', 'reason':None, 'bid':bid, 'ask':ask,
                    'quote_time':raw['time'], 'quote_epoch':quote_epoch,
                    'received_epoch':received,'evaluated_epoch':current,
                    'outcome_delay_sec':quote_epoch-target,
                    'receipt_delay_sec':received-target, 'evaluation_delay_sec':current-target}
        except (TypeError, ValueError, OverflowError) as exc:
            reason = str(exc) if isinstance(exc, ValueError) else 'invalid_quote_value'
    return {'status':'pending' if current <= target+delay_limit else 'censored',
            'reason':reason,'evaluated_epoch':current,'evaluation_delay_sec':current-target,
            'quote_epoch':raw_epoch,'received_epoch':snapshot.get('source_received_epoch'),
            'outcome_delay_sec':None,'receipt_delay_sec':None,'quote_time':raw_time,
            'bid':None,'ask':None}
