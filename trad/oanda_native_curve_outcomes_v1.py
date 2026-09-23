"""Pure post-receipt native-curve outcome scoring; no acquisition or execution.

Both nominal exact and original first-real-bar target views remain distinct.
Source-query completeness is a caller-attested fact; a registered caller must
verify its attestation and this row mapping against the retained raw response.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from decimal import Context, Decimal, localcontext
import hashlib
import json
import re
import time
from typing import Mapping

from oanda_forecast_curve_contract_v1 import (
    AUTHORITY, CurveContractError, content_hash, decimal_number, epoch,
    validate_consumption,
)

CAPTURE_SCHEMA = 'native_curve_outcome_candles_v1_20260909'
ENTRY_SCHEMA = 'native_curve_independent_entry_v1_20260909'
REPORT_SCHEMA = 'native_curve_outcomes_v1_20260909'
AGGREGATE_SCHEMA = 'native_curve_outcome_aggregate_v1_20260909'
MAX_ROWS = 4096
MAX_BYTES = 2 * 1024 * 1024
CTX = Context(prec=50)
PRICE_CONVENTIONS = ('official_midpoint', 'ba_derived_midpoint')


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{64}', value):
        raise CurveContractError('sha256_required')
    return value


def _pair(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', value) or value[:3] == value[4:]:
        raise CurveContractError('instrument_invalid')
    return value


def _text(value):
    return format(value, 'f') if isinstance(value, Decimal) else str(value)


def _seal(body, key):
    return {**body, key: content_hash(body)}


def _verify(value, schema, key):
    if not isinstance(value, dict) or value.get('schema_version') != schema:
        raise CurveContractError('record_schema')
    if any(value.get(k) is not v for k, v in AUTHORITY.items()):
        raise CurveContractError('record_authority')
    if value.get(key) != content_hash({k: v for k, v in value.items() if k != key}):
        raise CurveContractError('record_hash')


def _price(value):
    if type(value) not in (str, int):
        raise CurveContractError('exact_price_required')
    return decimal_number(value, positive=True)


def _normalize_row(row, observed):
    if not isinstance(row, dict) or row.get('complete') is not True:
        raise CurveContractError('complete_s5_row_required')
    label = epoch(row.get('bar_start_epoch'))
    if label % 5:
        raise CurveContractError('s5_label_grid_required')
    available = epoch(row.get('available_epoch'))
    if not label + 5 <= available <= observed:
        raise CurveContractError('row_unavailable_or_future_at_capture')
    mid, bid, ask = [_price(row.get(k)) for k in ('mid_close', 'bid_close', 'ask_close')]
    if bid > ask:
        raise CurveContractError('crossed_candle_close')
    # Official midpoint need not equal exact BA midpoint: both remain retained.
    return {'bar_start_epoch': label, 'price_epoch': label + 5,
            'available_epoch': available, 'complete': True,
            'mid_close': _text(mid), 'bid_close': _text(bid), 'ask_close': _text(ask)}


def capture_outcome_candles(rows, *, instrument, source_sha256,
                           coverage_start_label_epoch, coverage_end_label_epoch,
                           complete_range, read_started_epoch, read_completed_epoch,
                           source_attestation, clock=time.time):
    """Seal supplied complete MBA rows with actual read and query attestations.

    source_attestation has instrument, granularity='S5', raw_source_sha256,
    complete_provider_response=True, coverage_start_label_epoch,
    coverage_end_label_epoch, read_started_epoch, read_completed_epoch and
    source_receipt_sha256. This function cannot independently authenticate I/O.
    """
    observed = epoch(clock())
    instrument = _pair(instrument); source = _sha(source_sha256)
    first, last = map(epoch, (coverage_start_label_epoch, coverage_end_label_epoch))
    started, completed = map(epoch, (read_started_epoch, read_completed_epoch))
    if complete_range is not True or first % 5 or last % 5 or first > last:
        raise CurveContractError('explicit_complete_query_domain_required')
    if not started <= completed <= observed:
        raise CurveContractError('source_read_clock_order')
    if not isinstance(rows, (list, tuple)) or not 1 <= len(rows) <= MAX_ROWS:
        raise CurveContractError('bounded_complete_rows_required')
    if not isinstance(source_attestation, dict):
        raise CurveContractError('source_query_attestation_required')
    fields = {'instrument': instrument, 'granularity': 'S5', 'raw_source_sha256': source,
              'complete_provider_response': True, 'coverage_start_label_epoch': first,
              'coverage_end_label_epoch': last, 'read_started_epoch': started,
              'read_completed_epoch': completed}
    if any(source_attestation.get(k) != v for k, v in fields.items()) or source_attestation.get('complete_provider_response') is not True:
        raise CurveContractError('source_query_attestation_mismatch')
    fields['source_receipt_sha256'] = _sha(source_attestation.get('source_receipt_sha256'))
    normalized = [_normalize_row(r, completed) for r in rows]
    if any(r['available_epoch'] != completed for r in normalized):
        raise CurveContractError('row_availability_differs_from_source_read')
    labels = [r['bar_start_epoch'] for r in normalized]
    if labels != sorted(set(labels)) or labels[0] != first or labels[-1] != last:
        raise CurveContractError('row_domain_or_duplicate_mismatch')
    body = {'schema_version': CAPTURE_SCHEMA, 'instrument': instrument,
            'source_sha256': source, 'source_query_attestation': fields,
            'source_query_attestation_sha256': content_hash(fields),
            'coverage_start_label_epoch': first, 'coverage_end_label_epoch': last,
            'read_started_epoch': started, 'read_completed_epoch': completed,
            'first_observed_epoch': observed, 'complete_range': True,
            'rows': normalized, 'row_count': len(normalized),
            'completeness_scope': 'Complete provider response within attested first/last complete labels only; no claim before or after that domain.',
            **AUTHORITY}
    return _seal(body, 'capture_sha256')


def validate_candle_capture(capture):
    _verify(capture, CAPTURE_SCHEMA, 'capture_sha256')
    rebuilt = capture_outcome_candles(
        [{k: v for k, v in row.items() if k != 'price_epoch'} for row in capture['rows']],
        instrument=capture['instrument'], source_sha256=capture['source_sha256'],
        coverage_start_label_epoch=capture['coverage_start_label_epoch'],
        coverage_end_label_epoch=capture['coverage_end_label_epoch'],
        complete_range=capture['complete_range'], read_started_epoch=capture['read_started_epoch'],
        read_completed_epoch=capture['read_completed_epoch'],
        source_attestation=capture['source_query_attestation'],
        clock=lambda: capture['first_observed_epoch'])
    if rebuilt != capture:
        raise CurveContractError('capture_semantic_mismatch')
    return deepcopy(capture)


def capture_entry_quote(quote, *, source_sha256, clock=time.time):
    """Capture an independently observed quote now, not a retrospectively filled entry."""
    observed = epoch(clock());source = _sha(source_sha256)
    if not isinstance(quote, dict) or quote.get('tradeable') is not True:
        raise CurveContractError('tradeable_entry_required')
    instrument = _pair(quote.get('instrument'))
    market, available = map(epoch, (quote.get('market_epoch'), quote.get('available_epoch')))
    if not market <= available <= observed:
        raise CurveContractError('entry_clock_order')
    if observed - market > 60:
        raise CurveContractError('entry_quote_older_than_60_seconds_at_observation')
    bid, ask = _price(quote.get('bid')), _price(quote.get('ask'))
    if bid > ask:
        raise CurveContractError('crossed_entry')
    quote_id = quote.get('quote_id')
    if not isinstance(quote_id, str) or not re.fullmatch(r'[A-Za-z0-9_.:/+\-]{1,240}', quote_id):
        raise CurveContractError('entry_identity')
    return _seal({'schema_version': ENTRY_SCHEMA, 'instrument': instrument,
        'quote_id': quote_id, 'bid': _text(bid), 'ask': _text(ask),
        'market_epoch': market, 'available_epoch': available, 'first_observed_epoch': observed,
        'source_sha256': source, 'tradeable': True, **AUTHORITY}, 'entry_sha256')


def _validate_entry(entry):
    _verify(entry, ENTRY_SCHEMA, 'entry_sha256')
    rebuilt = capture_entry_quote({k: entry[k] for k in (
        'instrument', 'quote_id', 'bid', 'ask', 'market_epoch', 'available_epoch', 'tradeable')},
        source_sha256=entry['source_sha256'], clock=lambda: entry['first_observed_epoch'])
    if rebuilt != entry:
        raise CurveContractError('entry_semantic_mismatch')


def _base_report(now, curve):
    return {'schema_version': REPORT_SCHEMA, 'generated_epoch': now,
            'curve_id': curve.get('curve_id') if isinstance(curve, dict) else None,
            'curve_sha256': curve.get('curve_sha256') if isinstance(curve, dict) else None,
            'status': 'withheld', 'nodes': [], **AUTHORITY}


def _direction(value):
    return 1 if value > 0 else -1 if value < 0 else 0


def _unavailable(reason):
    return {'status': 'unavailable', 'reason_code': reason}


def _select_target(capture, nominal_label, maximum_delay, now):
    if now < nominal_label + 5:
        return None, 'nominal_target_not_mature'
    if capture['coverage_start_label_epoch'] > nominal_label:
        return None, 'source_domain_starts_after_nominal_target'
    candidates = [r for r in capture['rows'] if nominal_label <= r['bar_start_epoch'] <= nominal_label + maximum_delay]
    if candidates:
        return candidates[0], None
    if capture['coverage_end_label_epoch'] < nominal_label + maximum_delay:
        return None, 'target_window_not_covered_by_source'
    return None, 'provider_reported_no_complete_target_bar'


def _entry_metrics(entry, curve, node, target, pip, now):
    if entry is None:
        return _unavailable('no_independently_observed_entry')
    try:
        _validate_entry(entry)
        if entry['instrument'] != curve['prepared_curve']['instrument']:
            raise CurveContractError('entry_instrument_mismatch')
        if not (entry['market_epoch'] <= entry['available_epoch'] and
                curve['_consumed_epoch'] < entry['available_epoch'] <= entry['first_observed_epoch'] < node['original_target_epoch']):
            raise CurveContractError('entry_not_observed_after_consumption_before_original_target')
        if entry['first_observed_epoch'] > now:
            raise CurveContractError('entry_observation_in_future')
        bid, ask = decimal_number(entry['bid']), decimal_number(entry['ask'])
        terminal_bid, terminal_ask = decimal_number(target['bid_close']), decimal_number(target['ask_close'])
        long_price, short_price = terminal_bid - ask, bid - terminal_ask
        predicted_side = _direction(decimal_number(node['predicted_signed_pips']))
        long_bps, short_bps = 10000 * long_price / ask, 10000 * short_price / bid
        return {'status': 'scored_bid_ask_cost_diagnostic', 'entry_sha256': entry['entry_sha256'],
            'entry_market_epoch': entry['market_epoch'], 'entry_available_epoch': entry['available_epoch'],
            'entry_first_observed_epoch': entry['first_observed_epoch'],
            'target_price_epoch': target['price_epoch'], 'target_available_epoch': target['available_epoch'],
            'long_net_pips': _text(long_price / pip), 'short_net_pips': _text(short_price / pip),
            'long_net_bps': _text(long_bps), 'short_net_bps': _text(short_bps),
            'original_prediction_side': predicted_side,
            'original_prediction_side_net_bps': _text(long_bps if predicted_side > 0 else short_bps) if predicted_side else None,
            'original_prediction_side_positive': (long_bps if predicted_side > 0 else short_bps) > 0 if predicted_side else None,
            'neutral_prediction_is_no_position': predicted_side == 0,
            'target_tradeability_observed': False, 'broker_execution_observed': False,
            'scope': 'Hypothetical bid/ask endpoint cost using a separately observed current entry quote and completed target candle closes. Target tradeability was not observed; no executable exit, broker fill, manager policy, financing, slippage or USD sizing claim.'}
    except (CurveContractError, KeyError, TypeError) as exc:
        return _unavailable(str(exc) if isinstance(exc, CurveContractError) else 'invalid_entry_record')


def score_curve(curve, publication, consumption, candle_capture, *,
                expected_source_bindings, metadata, clock=time.time, entry_quote=None):
    """Score original issued reference-to-target values after actual target receipt."""
    now = epoch(clock());report = _base_report(now, curve)
    try:
        validate_consumption(curve, publication, consumption, expected_source_bindings=expected_source_bindings)
        prepared = curve['prepared_curve']
        if prepared['scope'] != 'current_research':
            raise CurveContractError('nonprospective_curve_scope')
        capture = validate_candle_capture(candle_capture)
        if capture['first_observed_epoch'] > now or consumption['available_epoch'] > now:
            raise CurveContractError('future_receipt_at_scoring')
        if capture['instrument'] != prepared['instrument']:
            raise CurveContractError('outcome_instrument_mismatch')
        if not isinstance(metadata, dict) or metadata.get('instrument') != prepared['instrument']:
            raise CurveContractError('instrument_metadata_required')
        if metadata.get('base_currency') != prepared['instrument'][:3] or metadata.get('quote_currency') != prepared['instrument'][4:]:
            raise CurveContractError('instrument_currency_metadata_mismatch')
        pip = decimal_number(metadata.get('pip_size'), positive=True)
        if pip >= 1:
            raise CurveContractError('pip_metadata_invalid')
        context = prepared.get('input_context', {})
        convention = context.get('price_convention')
        if convention not in PRICE_CONVENTIONS:
            raise CurveContractError('explicit_price_convention_required')
        native_pip = decimal_number(prepared['pip_size'], positive=True)
        reference = decimal_number(prepared['reference_price'], positive=True)
        scored_curve = {**curve, '_consumed_epoch': consumption['available_epoch']}
        report.update(status='evaluated', instrument=prepared['instrument'],
            model_sha256=prepared['model_sha256'], forecast_cohort=prepared['forecast_cohort'],
            reference_epoch=prepared['reference_epoch'], reference_label_epoch=prepared['reference_label_epoch'],
            reference_price=prepared['reference_price'], pip_size=_text(pip),
            native_prediction_pip_size=prepared['pip_size'], price_convention=convention,
            historical_ingestion_equivalence_proven=context.get('historical_ingestion_equivalence_proven') is True,
            input_context=deepcopy(context), source_bindings=deepcopy(prepared['source_bindings']),
            issued_epoch=curve['issued_epoch'], consumed_epoch=consumption['available_epoch'],
            publication_sha256=publication['publication_sha256'], consumption_sha256=consumption['consumption_sha256'],
            target_capture_sha256=capture['capture_sha256'], target_source_sha256=capture['source_sha256'],
            target_capture_first_observed_epoch=capture['first_observed_epoch'],
            target_source_query_attestation_sha256=capture['source_query_attestation_sha256'],
            target_selection_policy=deepcopy(prepared['target_selection_policy']))
        admissions = {r['node_id']: r for r in curve['node_admission']}
        delay = prepared['target_selection_policy']['maximum_delay_sec']
        with localcontext(CTX):
            for node in prepared['nodes']:
                row = {'node_id': node['node_id'], 'model_id': node.get('model_id'),
                    'horizon_sec': node['horizon_sec'], 'original_target_epoch': node['original_target_epoch'],
                    'target_label_epoch': node['target_label_epoch'], 'views': {}}
                for name, allowed_delay in [('nominal_exact', 0), ('retained_training_target', delay)]:
                    if node['status'] != 'forecast' or admissions[node['node_id']]['status'] != 'admitted':
                        row['views'][name] = _unavailable('node_not_issued_as_forecast')
                        continue
                    target, reason = _select_target(capture, node['target_label_epoch'], allowed_delay, now)
                    if reason:
                        row['views'][name] = _unavailable(reason);continue
                    actual = (decimal_number(target['mid_close']) if convention == 'official_midpoint' else
                              (decimal_number(target['bid_close']) + decimal_number(target['ask_close'])) / 2)
                    realized, expected = actual - reference, decimal_number(node['expected_terminal_price']) - reference
                    actual_side, predicted_side = _direction(realized), _direction(expected)
                    p = decimal_number(node['probability_up']) if node['probability_up'] is not None else None
                    view = {'status': 'scored', 'actual_selected_label_epoch': target['bar_start_epoch'],
                        'actual_selected_price_epoch': target['price_epoch'],
                        'selection_delay_sec': target['bar_start_epoch'] - node['target_label_epoch'],
                        'source_row_available_epoch': target['available_epoch'],
                        'outcome_available_epoch': max(target['available_epoch'], capture['first_observed_epoch']),
                        'actual_target_price': _text(actual), 'official_mid_close': target['mid_close'],
                        'bid_close': target['bid_close'], 'ask_close': target['ask_close'],
                        'actual_return_bps': _text(realized / reference * 10000),
                        'actual_move_pips': _text(realized / pip), 'actual_move_native_pips': _text(realized / native_pip),
                        'predicted_move_pips': _text(expected / pip),
                        'absolute_error_pips': _text(abs(expected - realized) / pip),
                        'zero_baseline_absolute_error_pips': _text(abs(realized) / pip),
                        'absolute_error_bps': _text(abs(expected - realized) / reference * 10000),
                        'zero_baseline_absolute_error_bps': _text(abs(realized) / reference * 10000),
                        'actual_direction': actual_side, 'predicted_direction': predicted_side,
                        'strict_up_event': actual_side > 0,
                        'direction_correct': predicted_side == actual_side if predicted_side and actual_side else None,
                        'direction_denominator_eligible': bool(predicted_side and actual_side),
                        'zero_outcome': actual_side == 0, 'neutral_prediction': predicted_side == 0,
                        'original_probability_up': node['probability_up'], 'original_probability_scope': node['probability_scope'],
                        'brier': _text((p - int(actual_side > 0)) ** 2) if p is not None else None,
                        'coinflip_brier': '0.25' if p is not None else None,
                        'probability_event_matches_training_target': name == 'retained_training_target' or delay == 0,
                        'original_probability_is_calibrated': False,
                        'executable': _entry_metrics(entry_quote, scored_curve, node, target, pip, now)}
                    row['views'][name] = view
                report['nodes'].append(row)
        report['limits'] = [
            'Prediction correctness and optional entry-to-target bid/ask diagnostics do not evaluate the management policy or a broker fill.',
            'Nominal exact and retained training-window views are distinct; the same outcome appearing in both is not another trial.',
            'Target convention is bound to the original research curve; historical ingestion equivalence is not inferred.',
            'Provider-response coverage and actual source-read receipt must be independently verified against retained raw bytes by the caller.',
            'Shared references, overlapping horizons and correlated currencies do not supply an independent sample count.',
            'Probabilities are original uncalibrated strict-up estimates; zero outcomes count as not-up for Brier and are separate from directional accuracy.'
        ]
    except (CurveContractError, KeyError, TypeError) as exc:
        report.update(status='withheld', reason_code=str(exc) if isinstance(exc, CurveContractError) else 'malformed_evidence', nodes=[])
    return _seal(report, 'report_sha256')


def aggregate_reports(reports):
    """Require unique curve/node views; report overlapping counts without inference."""
    if not isinstance(reports, (list, tuple)) or len(reports) > 4096:
        raise CurveContractError('bounded_report_set_required')
    groups = {};seen = set();withheld = Counter();curves = set();references = set();instruments = set()
    for report in reports:
        _verify(report, REPORT_SCHEMA, 'report_sha256')
        if report['status'] != 'evaluated':
            withheld[report.get('reason_code', 'unspecified')] += 1;continue
        curves.add(report['curve_id']);references.add(report['reference_epoch']);instruments.add(report['instrument'])
        for node in report['nodes']:
            for view_name, view in node['views'].items():
                identity = (report['curve_id'], node['node_id'], view_name)
                if identity in seen:
                    raise CurveContractError('duplicate_curve_node_view')
                seen.add(identity)
                key = (view_name, report['price_convention'], report['model_sha256'], report['forecast_cohort'], node['horizon_sec'])
                bucket = groups.setdefault(key, {'scored': [], 'unavailable': Counter()})
                if view['status'] == 'scored':bucket['scored'].append(view)
                else:bucket['unavailable'][view.get('reason_code', 'unspecified')] += 1
    summaries = []
    with localcontext(CTX):
        for key, bucket in sorted(groups.items()):
            rows = bucket['scored'];n = len(rows)
            direction = [r for r in rows if r['direction_denominator_eligible']]
            probability = [r for r in rows if r['brier'] is not None]
            executable = [r['executable'] for r in rows if r['executable'].get('original_prediction_side_net_bps') is not None]
            mean = lambda values: _text(sum((Decimal(str(v)) for v in values), Decimal(0)) / len(values)) if values else None
            summaries.append({'view': key[0], 'price_convention': key[1], 'model_sha256': key[2], 'forecast_cohort': key[3],
                'horizon_sec': key[4],
                'scored_nodes': n, 'unavailable_nodes': sum(bucket['unavailable'].values()),
                'unavailable_reasons': dict(bucket['unavailable']),
                'direction_denominator': len(direction), 'direction_correct': sum(r['direction_correct'] for r in direction),
                'direction_accuracy': mean([int(r['direction_correct']) for r in direction]),
                'neutral_predictions': sum(r['neutral_prediction'] for r in rows), 'zero_outcomes': sum(r['zero_outcome'] for r in rows),
                'mae_bps': mean([r['absolute_error_bps'] for r in rows]),
                'zero_baseline_mae_bps': mean([r['zero_baseline_absolute_error_bps'] for r in rows]),
                'brier_denominator': len(probability), 'mean_original_probability_brier': mean([r['brier'] for r in probability]),
                'probability_event_mismatch_count': sum(not r['probability_event_matches_training_target'] for r in probability),
                'executable_non_neutral_denominator': len(executable),
                'mean_original_side_net_bps': mean([r['original_prediction_side_net_bps'] for r in executable]),
                'positive_original_side_count': sum(r['original_prediction_side_positive'] for r in executable),
                'positive_original_side_rate': mean([int(r['original_prediction_side_positive']) for r in executable])})
    return _seal({'schema_version': AGGREGATE_SCHEMA, 'groups': summaries,
        'input_report_count': len(reports), 'unique_evaluated_curves': len(curves),
        'unique_reference_price_clocks': len(references), 'unique_instruments': len(instruments),
        'withheld_reports': dict(withheld), 'independent_sample_size': None,
        'source_report_hashes': [r['report_sha256'] for r in reports],
        'scope': 'Descriptive original-curve diagnostics; horizons/views overlap, no calibration, promotion or management-success claim.',
        **AUTHORITY}, 'aggregate_sha256')


def score_curve_from_captures(curve, publication, consumption, candle_captures, *,
                              expected_source_bindings, metadata, clock=time.time, entry_quote=None):
    """One per-curve report from separately attested rolling query captures.

    Selection uses original source-read completion, then raw/receipt hashes.
    Derived capture observations stay at their actual later creation clocks.
    No value, prediction error, direction or prospective profitability selects
    a source. Separate query domains are never merged into a completeness claim.
    """
    now = epoch(clock());empty = _base_report(now, curve)
    try:
        validate_consumption(curve, publication, consumption, expected_source_bindings=expected_source_bindings)
        if curve['prepared_curve']['scope'] != 'current_research':
            raise CurveContractError('nonprospective_curve_scope')
        if not isinstance(candle_captures, (list, tuple)) or not 1 <= len(candle_captures) <= 4096:
            raise CurveContractError('bounded_nonempty_source_capture_set_required')
        distinct = {}
        for original in candle_captures:
            capture = validate_candle_capture(original)
            if capture['instrument'] != curve['prepared_curve']['instrument']:
                raise CurveContractError('outcome_instrument_mismatch')
            if capture['first_observed_epoch'] > now:
                raise CurveContractError('future_receipt_at_scoring')
            attestation = capture['source_query_attestation']
            identity = (capture['source_sha256'], attestation['source_receipt_sha256'])
            semantic = content_hash({k: v for k, v in capture.items()
                                     if k not in ('capture_sha256', 'first_observed_epoch')})
            old = distinct.get(identity)
            if old is not None and old[0] != semantic:
                raise CurveContractError('same_source_receipt_conflicting_mapping')
            if old is None or (capture['first_observed_epoch'], capture['capture_sha256']) < (
                    old[1]['first_observed_epoch'], old[1]['capture_sha256']):
                distinct[identity] = (semantic, capture)
        ordered = sorted([v[1] for v in distinct.values()], key=lambda c: (
            c['read_completed_epoch'], c['source_sha256'],
            c['source_query_attestation']['source_receipt_sha256']))
        reports = {}
        def report_for(capture):
            key = capture['capture_sha256']
            if key not in reports:
                reports[key] = score_curve(curve, publication, consumption, capture,
                    expected_source_bindings=expected_source_bindings, metadata=metadata,
                    clock=lambda: now, entry_quote=entry_quote)
                if reports[key]['status'] != 'evaluated':
                    raise CurveContractError(reports[key]['reason_code'])
            return reports[key]
        result = deepcopy(report_for(ordered[0]))
        # Replace the single-source projection with exact per-view provenance.
        for key in ('target_capture_sha256', 'target_source_sha256',
                    'target_capture_first_observed_epoch', 'target_source_query_attestation_sha256'):
            result.pop(key, None)
        selected_refs = {}
        delay = curve['prepared_curve']['target_selection_policy']['maximum_delay_sec']
        for index, node in enumerate(curve['prepared_curve']['nodes']):
            for view_name, allowed_delay in [('nominal_exact', 0), ('retained_training_target', delay)]:
                if result['nodes'][index]['views'][view_name].get('reason_code') == 'node_not_issued_as_forecast':
                    continue
                selection = None;reasons = Counter()
                for capture in ordered:
                    row, reason = _select_target(capture, node['target_label_epoch'], allowed_delay, now)
                    if row is not None or reason == 'provider_reported_no_complete_target_bar':
                        selection = capture;break
                    reasons[reason] += 1
                if selection is None:
                    result['nodes'][index]['views'][view_name] = {
                        **_unavailable('no_decisive_source_capture'),
                        'source_domain_reasons': dict(sorted(reasons.items()))}
                    continue
                chosen = deepcopy(report_for(selection)['nodes'][index]['views'][view_name])
                ref = {'capture_sha256': selection['capture_sha256'],
                    'raw_source_sha256': selection['source_sha256'],
                    'source_query_attestation_sha256': selection['source_query_attestation_sha256'],
                    'source_receipt_sha256': selection['source_query_attestation']['source_receipt_sha256'],
                    'original_source_read_started_epoch': selection['read_started_epoch'],
                    'original_source_read_completed_epoch': selection['read_completed_epoch'],
                    'derived_capture_first_observed_epoch': selection['first_observed_epoch'],
                    'coverage_start_label_epoch': selection['coverage_start_label_epoch'],
                    'coverage_end_label_epoch': selection['coverage_end_label_epoch']}
                chosen['selected_source_capture'] = ref
                chosen['capture_selection_status'] = 'earliest_decisive_attested_source_read'
                result['nodes'][index]['views'][view_name] = chosen
                selected_refs[selection['capture_sha256']] = ref
        result.update(source_capture_mode='separate_rolling_queries_per_node_view',
            source_selection_policy='earliest_original_attested_read_completed_then_raw_sha_then_receipt_sha',
            source_selection_uses_outcome_values=False,
            supplied_capture_count=len(candle_captures), unique_source_observation_count=len(ordered),
            selected_source_capture_count=len(selected_refs),
            selected_source_captures=[selected_refs[k] for k in sorted(selected_refs)],
            supplied_capture_sha256=sorted(c['capture_sha256'] for c in candle_captures))
        result['limits'].append('Derived capture seals retain actual later construction clocks; source selection uses original separately attested read completion. Rolling source domains are not merged.')
        result.pop('report_sha256', None)
        return _seal(result, 'report_sha256')
    except (CurveContractError, KeyError, TypeError) as exc:
        empty['reason_code'] = str(exc) if isinstance(exc, CurveContractError) else 'malformed_evidence'
        return _seal(empty, 'report_sha256')
