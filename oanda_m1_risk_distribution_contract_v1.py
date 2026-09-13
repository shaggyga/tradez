"""Fixed, prospective M1 risk distributions from retained training quantiles.

Both predeclared methods and every label/horizon are issued together. These are
uncalibrated distributions, not direction probabilities, barrier-order forecasts,
executable fills or a sizing policy. No model fitting, filesystem or broker I/O.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time

import oanda_m1_path_risk_labels_v1 as labels

SCHEMA = 'm1_risk_distribution_issue_v1_20260909'
PUBLICATION_SCHEMA = 'm1_risk_distribution_publication_v1_20260909'
CONSUMPTION_SCHEMA = 'm1_risk_distribution_consumption_v1_20260909'
COHORT_ID = 'm1_risk_distributions_v1_20260909.prospective'
TRAINING_ARTIFACT_SHA256 = '1d3d6f6dfd946f2c251abebce78d5624ec73c6da393ee0cd68173303a1851b10'
PAIRS = ('EUR_USD', 'GBP_USD', 'USD_JPY')
METHODS = ('training_empirical', 'training_empirical_volatility_scaled')
QUANTILES = (0.1, 0.5, 0.9)
MAX_BYTES = 2 * 1024 * 1024
MAX_ENTRY_AGE_SEC = 20
AUTHORITY = dict(research_only=True, can_place_orders=False, can_promote=False,
                 can_authorize=False, execution_eligible=False, account_eligible=False,
                 proof_eligible=False, historical_forecasts_imported=False,
                 positions_managed=False)


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def canonical(value):
    try:
        result = json.dumps(value, sort_keys=True, separators=(',', ':'),
                            allow_nan=False).encode()
    except (ValueError, TypeError, RecursionError):
        raise ValueError('risk_noncanonical_value') from None
    need(len(result) <= MAX_BYTES, 'risk_object_byte_bound')
    return result


def sha(raw):
    need(type(raw) is bytes, 'risk_bytes_required')
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha(canonical(value))


def decode(raw):
    need(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES, 'risk_source_byte_bound')
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, 'risk_duplicate_json_key')
            result[key] = value
        return result
    try:
        result = json.loads(raw, object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError('risk_nonfinite_json')))
        need(type(result) is dict, 'risk_object_required')
        canonical(result)
        return result
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError('risk_invalid_json') from None


def epoch(value):
    need(type(value) in (int, float) and 0 < value < 10**11 and math.isfinite(value),
         'risk_invalid_clock')
    return value


def _hash(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def bindings(value):
    need(type(value) is dict and 1 <= len(value) <= 32 and all(
        type(k) is str and re.fullmatch('oanda_[a-z0-9_]+\\.py', k) and _hash(v)
        for k, v in value.items()), 'risk_source_bindings')
    return decode(canonical(value))


def _seal(body, key):
    return {**body, key: digest(body)}


def _unseal(value, key, schema, expected_source_bindings):
    need(type(value) is dict and value.get('schema_version') == schema and
         all(value.get(k) is v for k, v in AUTHORITY.items()), 'risk_schema_or_authority')
    need(value.get('source_bindings') == bindings(expected_source_bindings),
         'risk_source_binding_mismatch')
    need(_hash(value.get(key)) and digest({k: v for k, v in value.items() if k != key}) == value[key],
         'risk_seal_mismatch')
    return decode(canonical(value))


def _training(fit_raw, instrument, reference_epoch, input_clock):
    need(sha(fit_raw) == TRAINING_ARTIFACT_SHA256, 'risk_training_artifact_identity')
    artifact = decode(fit_raw)
    need(epoch(artifact.get('created_epoch')) <= input_clock,
         'risk_training_artifact_not_yet_available')
    need(artifact.get('research_only') is True and all(artifact.get(k) is False
         for k in ('can_place_orders', 'can_promote', 'can_authorize',
                   'execution_eligible', 'account_eligible', 'proof_eligible', 'forecast_issued')),
         'risk_training_artifact_scope')
    fits = artifact.get('fits')
    need(type(fits) is dict, 'risk_training_fit_inventory')
    selected = {}
    for horizon in labels.HORIZONS:
        for label in labels.CONTINUOUS_LABELS:
            key = f'{instrument}/{horizon}/{label}'
            fit = fits.get(key)
            need(type(fit) is dict and fit.get('status') == 'fitted_training_quantiles'
                 and fit.get('fit_scope') == 'training_only_common_support'
                 and fit.get('method') == 'inverted_cdf'
                 and fit.get('original_probability_available') is False
                 and fit.get('quantiles') == list(QUANTILES)
                 and type(fit.get('training_rows')) is int and fit['training_rows'] >= 256
                 and _hash(fit.get('training_origin_sha256')), 'risk_training_fit_contract')
            train_reference = epoch(fit.get('training_reference_max_epoch'))
            train_target = epoch(fit.get('training_target_max_epoch'))
            need(train_reference < train_target <= artifact['created_epoch']
                 and train_target < reference_epoch, 'risk_training_cutoff')
            for field in ('empirical', 'volatility_normalized'):
                values = fit.get(field)
                need(type(values) is list and len(values) == 3 and all(
                    type(v) in (int, float) and math.isfinite(v) and abs(v) <= 1e12
                    for v in values) and values == sorted(values), 'risk_training_quantiles')
                if label != 'signed_terminal_bps':
                    need(all(v >= 0 for v in values), 'risk_unsigned_quantile_negative')
            selected[key] = fit
    return artifact, selected


def _issue_body(input_raw, input_receipt, metadata, fit_raw, *, input_consumption,
                cohort_id, scheduled_reference_price_epoch, expected_source_bindings,
                computation_started_epoch, issued_epoch):
    import oanda_m1_mba_research_capture_v1 as capture
    sources = bindings(expected_source_bindings)
    started, issued = epoch(computation_started_epoch), epoch(issued_epoch)
    need(started <= issued, 'risk_computation_clock_order')
    need(cohort_id == COHORT_ID, 'risk_cohort_identity')
    reference = epoch(scheduled_reference_price_epoch)
    need(reference % 300 == 60, 'risk_original_five_minute_grid')
    need(type(input_consumption) is dict and set(input_consumption) == {
        'raw_sha256', 'receipt_sha256', 'read_started_epoch', 'read_completed_epoch'},
        'risk_input_consumption_shape')
    consume_started = epoch(input_consumption['read_started_epoch'])
    consumed = epoch(input_consumption['read_completed_epoch'])
    need(input_consumption['raw_sha256'] == sha(input_raw) and
         input_consumption['receipt_sha256'] == digest(input_receipt),
         'risk_input_consumption_identity')
    mapping = capture.map_verified_capture(input_raw, input_receipt, metadata)
    document = capture.mapping_document(mapping)
    source_observed = epoch(document['first_observed_epoch'])
    capture_completed = epoch(input_receipt['capture_completed_epoch'])
    need(reference <= source_observed <= capture_completed <= consume_started <= consumed <= started <= issued
         <= reference + MAX_ENTRY_AGE_SEC, 'risk_original_input_or_issue_clock')
    csv_raw = mapping['canonical_csv_bytes']
    instrument = document['instrument']
    need(instrument in PAIRS, 'risk_instrument_scope')
    rows, parser = labels.parse_csv(csv_raw, instrument, observed_epoch=source_observed)
    need(rows[-1]['price_epoch'] == reference, 'risk_latest_original_reference_required')
    last_segment_start, last_segment_end = labels.contiguous_segments(rows)[-1]
    need(last_segment_end - last_segment_start >= 204, 'risk_204_real_same_session_warm_rows')
    origin = rows[-1]
    need(origin['bid'] is not None and origin['ask'] is not None,
         'risk_original_bid_ask_required')
    artifact, fits = _training(fit_raw, instrument, reference, started)
    nodes = []
    for horizon in labels.HORIZONS:
        scale, refusal = labels.past_volatility_scale(rows, len(rows) - 1, horizon)
        need(refusal is None and scale is not None, refusal or 'risk_scale_unavailable')
        for label in labels.CONTINUOUS_LABELS:
            fit = fits[f'{instrument}/{horizon}/{label}']
            for method in METHODS:
                values = (list(fit['empirical']) if method == 'training_empirical'
                          else [value * scale for value in fit['volatility_normalized']])
                need(all(math.isfinite(v) for v in values), 'risk_nonfinite_issued_quantile')
                nodes.append(dict(horizon_minutes=horizon, horizon_sec=horizon * 60,
                    target_label_epoch=origin['label_epoch'] + horizon * 60,
                    target_price_epoch=reference + horizon * 60, label=label, method=method,
                    quantile_levels=list(QUANTILES), quantile_bps=values, scale_bps=scale,
                    training_rows=fit['training_rows'], training_origin_sha256=fit['training_origin_sha256'],
                    training_reference_max_epoch=fit['training_reference_max_epoch'],
                    training_target_max_epoch=fit['training_target_max_epoch']))
    return dict(schema_version=SCHEMA, **AUTHORITY, cohort_id=cohort_id,
                source_bindings=sources, instrument=instrument,
                reference_label_epoch=origin['label_epoch'], reference_price_epoch=reference,
                reference_mid=origin['mid']['close'], origin_row=origin,
                scheduled_reference_price_epoch=reference,
                input_mapping=document, input_consumption=decode(canonical(input_consumption)),
                input_parser=parser, input_metadata=decode(canonical(metadata)),
                training_artifact_sha256=TRAINING_ARTIFACT_SHA256,
                training_artifact_created_epoch=artifact['created_epoch'],
                computation_started_epoch=started, issued_epoch=issued, nodes=nodes,
                distribution_scope='fixed_training_quantiles_unconfirmed_prospective_risk_distribution',
                probability_scope='no_direction_probability_or_calibrated_coverage_claim',
                path_scope='observed_close_paths_and_separate_OHLC_envelopes_not_barrier_order_or_fills',
                historical_training_scope='frozen_training_subset_of_previously_inspected_history_no_refit',
                nominal_central_interval_coverage=0.8, quantile_interpolation=False,
                original_target_retimed=False, maximum_entry_age_sec=MAX_ENTRY_AGE_SEC)


def issue_distribution(input_raw, input_receipt, metadata, fit_raw, *, input_consumption,
                       cohort_id, scheduled_reference_price_epoch, expected_source_bindings,
                       clock=time.time):
    # Snapshot mutable input values before building; clocks are actual samples.
    receipt = decode(canonical(input_receipt))
    metadata = decode(canonical(metadata))
    consumed = decode(canonical(input_consumption))
    sources = bindings(expected_source_bindings)
    started = epoch(clock())
    # Validate and calculate at the start cutoff, then replace only the factual
    # computation completion field after all numerical work has happened.
    body = _issue_body(input_raw, receipt, metadata, fit_raw, input_consumption=consumed,
        cohort_id=cohort_id, scheduled_reference_price_epoch=scheduled_reference_price_epoch,
        expected_source_bindings=sources, computation_started_epoch=started, issued_epoch=started)
    issued = epoch(clock())
    need(started <= issued <= body['reference_price_epoch'] + MAX_ENTRY_AGE_SEC,
         'risk_computation_completion_late_or_regressed')
    body['issued_epoch'] = issued
    return _seal(body, 'issued_sha256')


def validate_issue(issued, *, input_raw, input_receipt, metadata, fit_raw,
                   expected_source_bindings):
    value = _unseal(issued, 'issued_sha256', SCHEMA, expected_source_bindings)
    rebuilt = _seal(_issue_body(input_raw, input_receipt, metadata, fit_raw,
        input_consumption=value['input_consumption'], cohort_id=value['cohort_id'],
        scheduled_reference_price_epoch=value['scheduled_reference_price_epoch'],
        expected_source_bindings=expected_source_bindings,
        computation_started_epoch=value['computation_started_epoch'], issued_epoch=value['issued_epoch']),
        'issued_sha256')
    need(rebuilt == value, 'risk_issue_numerical_or_source_replay_mismatch')
    return value


def publication_receipt(issued, *, persisted_bytes_sha256, publication_started_epoch,
                        expected_source_bindings, clock=time.time):
    value = _unseal(issued, 'issued_sha256', SCHEMA, expected_source_bindings)
    started = epoch(publication_started_epoch)
    completed = epoch(clock())
    need(persisted_bytes_sha256 == digest(value), 'risk_persisted_issue_bytes_mismatch')
    need(value['issued_epoch'] <= started <= completed <=
         value['reference_price_epoch'] + MAX_ENTRY_AGE_SEC, 'risk_publication_clock_or_delay')
    return _seal(dict(schema_version=PUBLICATION_SCHEMA, **AUTHORITY,
        source_bindings=bindings(expected_source_bindings), issued_sha256=value['issued_sha256'],
        persisted_bytes_sha256=persisted_bytes_sha256, publication_started_epoch=started,
        publication_completed_epoch=completed), 'publication_sha256')


def consumption_receipt(issued, publication, *, read_started_epoch,
                        expected_source_bindings, clock=time.time):
    value = _unseal(issued, 'issued_sha256', SCHEMA, expected_source_bindings)
    pub = _unseal(publication, 'publication_sha256', PUBLICATION_SCHEMA, expected_source_bindings)
    rebuilt = publication_receipt(value, persisted_bytes_sha256=pub['persisted_bytes_sha256'],
        publication_started_epoch=pub['publication_started_epoch'],
        expected_source_bindings=expected_source_bindings, clock=lambda: pub['publication_completed_epoch'])
    need(rebuilt == pub, 'risk_publication_binding')
    started, completed = epoch(read_started_epoch), epoch(clock())
    need(pub['publication_completed_epoch'] <= started <= completed <=
         value['reference_price_epoch'] + MAX_ENTRY_AGE_SEC, 'risk_consumer_clock_or_delay')
    return _seal(dict(schema_version=CONSUMPTION_SCHEMA, **AUTHORITY,
        source_bindings=bindings(expected_source_bindings), issued_sha256=value['issued_sha256'],
        publication_sha256=pub['publication_sha256'], consumed_issue_bytes_sha256=digest(value),
        consumed_publication_bytes_sha256=digest(pub), read_started_epoch=started,
        read_completed_epoch=completed), 'consumption_sha256')


def validate_chain(issued, publication, consumption, *, input_raw, input_receipt,
                   metadata, fit_raw, expected_source_bindings):
    value = validate_issue(issued, input_raw=input_raw, input_receipt=input_receipt,
        metadata=metadata, fit_raw=fit_raw, expected_source_bindings=expected_source_bindings)
    consume = _unseal(consumption, 'consumption_sha256', CONSUMPTION_SCHEMA,
                      expected_source_bindings)
    rebuilt = consumption_receipt(value, publication,
        read_started_epoch=consume['read_started_epoch'],
        expected_source_bindings=expected_source_bindings,
        clock=lambda: consume['read_completed_epoch'])
    need(rebuilt == consume, 'risk_consumption_binding')
    return value
