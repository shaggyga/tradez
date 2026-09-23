"""Separate practice-candidate translation of the unchanged joint-v3 research feed.

Import is side-effect free. ``build_candidates`` is pure and checks an exact
caller-attested observer report; a hash alone does not authenticate caller I/O.
``read_candidates`` supplies that provenance by invoking the pinned, read-only
observer and its original publication/consumption and source-closure checks.
Neither function fits a model, writes a research ledger, contacts a broker,
authorizes an account, or changes the research source's eligibility flags.

The original terminal and direction are preserved. Execution quote freshness,
remaining edge, costs, sizing and account authority belong to the separate
practice policy. Probabilities remain uncalibrated. A matched price-only
diagnostic is not the separately published price-v2 control or a new ensemble.
"""
from __future__ import annotations

from decimal import Context, Decimal, InvalidOperation, localcontext
import hashlib
import importlib
import importlib.util
import json
import math
from pathlib import Path
import re
import time

SCHEMA = 'practice_forecast_adapter_v1_20260909'
FAMILY = 'ridge_price_news_v1'
REGISTRY_SHA256 = 'ee075e69e80ca56dfdf45abe1f7a1a301612176e67f7622eab1adf2bd9af8771'
ACTIVATION_SHA256 = 'e3e8de4f4d1d9dcd4a2daed4cfa30a03f176fa95d0380818f87ea521766597dd'
OBSERVER_BINDINGS = {
    'oanda_joint_v3_ledger_status_observer_v2.py': '1a8b4c68377bb27314e018063a70725d645d5cd4183ffa55c5f61145e257178a',
    'oanda_joint_v3_ledger_status_observer_v1.py': 'd2dc17dfee52603ae1ccf61433f4a1a73cd1650fdf44c5f1daaa60cf133d8699',
    'oanda_immutable_summary_publication_v1.py': '53abafc379216797945543464fe218511f53b17a2d0d9d9cecb700146eff4bf7',
}
INERT_FLAGS = ('can_place_orders', 'can_promote', 'can_authorize', 'account_eligible',
               'proof_eligible', 'historical_rows_imported')
MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_REGISTRY_BYTES = 1024 * 1024


class AdapterError(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise AdapterError(reason)


def encoded(value):
    # Match the frozen observer/ledger canonical encoding, including Unicode.
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def _epoch(value, reason='invalid_clock'):
    require(type(value) in (int, float) and math.isfinite(value) and value > 0, reason)
    return value


def _hash(value, reason='invalid_hash'):
    require(type(value) is str and re.fullmatch('[a-f0-9]{64}', value) is not None, reason)
    return value


def _decimal(value, reason='invalid_decimal'):
    require(type(value) in (str, int, float) and len(str(value)) <= 128, reason)
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise AdapterError(reason) from None
    require(result.is_finite() and result.adjusted() <= 24 and result.as_tuple().exponent >= -40, reason)
    return result


def _decode(raw, limit, reason):
    require(type(raw) is bytes and 0 < len(raw) <= limit, reason + '_bytes')
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, reason + '_duplicate_key')
            result[key] = value
        return result
    try:
        result = json.loads(raw, object_pairs_hook=pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(AdapterError(reason + '_nonfinite')))
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise AdapterError(reason + '_json') from None
    require(type(result) is dict, reason + '_shape')
    return result


def _seal(value, reason):
    require(value.get('payload_sha256') == digest({k: v for k, v in value.items() if k != 'payload_sha256'}), reason)


def _authority(value):
    require(value.get('research_only') is True and all(value.get(k) is False for k in INERT_FLAGS),
            'original_research_authority_changed')


def _policy(instruments, registry, maximum_signal_age_sec, minimum_remaining_sec, maximum_observation_age_sec):
    require(type(maximum_signal_age_sec) in (int, float) and 0 < maximum_signal_age_sec <= 900,
            'maximum_signal_age_policy')
    require(type(minimum_remaining_sec) in (int, float) and 60 <= minimum_remaining_sec < 3600,
            'minimum_remaining_policy')
    require(type(maximum_observation_age_sec) in (int, float) and 0 < maximum_observation_age_sec <= 90,
            'maximum_observation_age_policy')
    wanted = sorted(registry['pairs']) if instruments is None else list(instruments)
    require(1 <= len(wanted) <= 68 and all(type(p) is str and re.fullmatch('[A-Z]{3}_[A-Z]{3}', p) for p in wanted)
            and len(set(wanted)) == len(wanted), 'instrument_policy')
    return sorted(wanted)


def _candidate(pair, row, registry, report, decision, maximum_age, minimum_remaining):
    require(pair in registry['pairs'], 'unsupported_instrument')
    require(row is not None, 'pair_not_observed')
    require(set(row['families']) == {FAMILY}, 'family_identity')
    slot = row['families'][FAMILY]
    if slot.get('status') != 'forecast':
        reason = slot.get('reason', 'forecast_unavailable')
        require(False, reason if type(reason) is str and re.fullmatch('[a-z][a-z0-9_]{1,120}', reason) else 'forecast_unavailable')
    spec = registry['pairs'][pair]['families'][FAMILY]
    contract = spec['contract']
    require(digest(contract) == spec['contract_sha256'] == slot['contract_sha256'], 'contract_identity')
    _authority(contract)
    require(slot['origin_registry_sha256'] == REGISTRY_SHA256 and slot['origin_study'] == 'joint_v3'
            and slot['cohort_id'] == contract['cohorts'][FAMILY], 'source_cohort_identity')
    ledger = slot['ledger_observation']
    require(all(ledger.get(k) is True for k in ('contract_and_activation_verified', 'query_only', 'original_published_forecast')),
            'original_ledger_not_verified')
    row_identity = _hash(ledger['original_row_identity_binding_sha256'])
    start, join_start, join_end, end, verified = [_epoch(ledger[k]) for k in
        ('read_started_epoch', 'forecast_join_read_started_epoch', 'forecast_join_read_completed_epoch',
         'read_completed_epoch', 'verification_completed_epoch')]
    require(report['started_epoch'] <= start <= join_start <= join_end <= end <= verified
            == slot['observed_epoch'] <= report['completed_epoch'] <= decision, 'ledger_observation_clock_order')
    forecast = slot['latest_forecast']
    require(forecast['publication_verified'] is True and forecast['consumption_verified'] is True,
            'original_publication_or_consumption_unverified')
    require(forecast['instrument'] == pair and forecast['family'] == FAMILY and forecast['horizon_sec'] == 3600
            and len(forecast['forecasts']) == 1, 'forecast_identity')
    arm = forecast['forecasts'][0]
    require(arm['instrument'] == pair and arm['family'] == FAMILY and arm['horizon_sec'] == 3600
            and arm['cohort_id'] == contract['cohorts'][FAMILY], 'arm_identity')
    reference, available, issue, published, consumed, verification, target = [_epoch(forecast[k]) for k in
        ('reference_epoch', 'reference_available_epoch', 'issued_epoch', 'publication_epoch',
         'consumption_epoch', 'publication_verified_epoch', 'target_epoch')]
    require(0 < slot['activated_epoch'] < available and reference <= available <= issue <= published <= consumed
            <= join_end and consumed <= verification <= verified and target == reference + 3600,
            'original_forecast_clock_order')
    require(available - reference <= 60 and issue - available <= contract['maximum_build_sec'], 'original_build_clock_window')
    require(all(arm[k] == forecast[k] for k in ('reference_epoch', 'issued_epoch', 'target_epoch')), 'arm_clock_identity')
    require(0 <= decision - issue <= maximum_age, 'original_forecast_stale')
    require(target - decision >= minimum_remaining, 'original_target_too_near_or_elapsed')
    require(ledger['latest_original_target_epoch'] == target, 'original_target_identity')
    forecast_sha = _hash(forecast['forecast_sha256'])
    decision_id = _hash(forecast['decision_id'])
    publication_sha = digest({'epoch': published, 'forecast_sha': forecast_sha})
    require(publication_sha == forecast['publication_receipt_sha256'], 'publication_receipt_identity')
    require(digest({'epoch': consumed, 'forecast_sha': forecast_sha, 'publication_sha': publication_sha})
            == forecast['consumer_receipt_sha256'], 'consumer_receipt_identity')
    price, pip = _decimal(arm['reference_mid']), _decimal(arm['pip_size'])
    require(price > 0 and pip > 0 and pip == _decimal(contract['pip_size']) == _decimal(row['pip_size']), 'price_or_pip_identity')
    bps, probability = _decimal(arm['predicted_return_bps']), _decimal(arm['probability_up'])
    side = arm['side']
    require(type(side) is int and side in (-1, 0, 1), 'side_invalid')
    require(side != 0, 'original_forecast_neutral')
    require(0 <= probability <= 1 and (bps > 0 if side == 1 else bps < 0), 'original_direction_mismatch')
    require(probability >= Decimal('.5') if side == 1 else probability <= Decimal('.5'), 'probability_direction_mismatch')
    news = {k: _epoch(arm[k]) for k in ('news_evidence_epoch', 'news_generated_epoch', 'news_first_observed_epoch',
                                      'news_available_epoch', 'news_expires_epoch')}
    require(news['news_evidence_epoch'] <= news['news_generated_epoch'] <= news['news_first_observed_epoch']
            == news['news_available_epoch'] <= issue <= news['news_expires_epoch']
            and issue - news['news_evidence_epoch'] <= contract['maximum_news_age_sec'], 'original_news_clock_window')
    with localcontext(Context(prec=192)):
        terminal = price * (1 + bps / Decimal(10000))
    require(terminal > 0, 'nonpositive_original_terminal')
    return dict(instrument=pair, side=side, direction='buy' if side == 1 else 'sell',
        reference_epoch=reference, reference_price=str(price), pip_size=str(pip),
        original_target_epoch=target, expected_terminal_price=str(terminal), expected_bps=str(bps),
        probability_up=str(probability), issued_epoch=issue, available_epoch=consumed,
        forecast_sha256=forecast_sha, trial_candidate_id=digest({'schema': SCHEMA, 'decision_id': decision_id,
            'forecast_sha256': forecast_sha, 'family': FAMILY}),
        original_decision_id=decision_id, family=FAMILY, cohort_id=arm['cohort_id'],
        original_horizon_sec=3600, origin_registry_sha256=REGISTRY_SHA256,
        model_version=contract['model_version'], feature_version=contract['feature_version'],
        numeric_model_source_sha256=contract['numeric_model_source_sha256'], contract_sha256=spec['contract_sha256'],
        publication_epoch=published, consumption_epoch=consumed, reference_available_epoch=available,
        publication_receipt_sha256=publication_sha, consumer_receipt_sha256=forecast['consumer_receipt_sha256'],
        original_row_identity_binding_sha256=row_identity, adapter_observed_epoch=report['completed_epoch'],
        decision_epoch=decision, original_news_clocks=news, news_capture_sha256=_hash(arm['news_capture_sha256']),
        uncertainty=dict(probability_scope='uncalibrated_model_estimate_not_verified_accuracy',
                         residual_scale=None, residual_scale_status='not_exposed_by_frozen_observer'),
        controls=dict(matched_price_only=arm.get('input_contributions', {}).get('matched_price_only_expected_pips'),
                      scope='same_joint_fit_diagnostic_in_pips_not_published_price_v2_not_used_for_selection',
                      published_price_v2_status='not_collected_by_this_adapter'),
        source_authority={k: contract[k] for k in ('research_only', *INERT_FLAGS)},
        experimental_practice_candidate=True, can_place_orders=False, can_authorize=False,
        interpretation='Necessary forecast input only; separate practice account policy supplies execution authority.')


def build_candidates(observer_report_bytes, registry_bytes, *, expected_report_sha256, decision_epoch,
                     instruments=None, maximum_signal_age_sec=900, minimum_remaining_sec=60,
                     maximum_observation_age_sec=90):
    """Pure exact-byte mapping; expected report hash must come from trusted I/O."""
    decision = _epoch(decision_epoch)
    require(hashlib.sha256(registry_bytes).hexdigest() == REGISTRY_SHA256, 'registry_source_identity')
    registry = _decode(registry_bytes, MAX_REGISTRY_BYTES, 'registry')
    _authority(registry)
    require(registry['registry_id'] == 'joint_price_news_study_v3_20260908' and len(registry['pairs']) == 68,
            'registry_inventory')
    wanted = _policy(instruments, registry, maximum_signal_age_sec, minimum_remaining_sec, maximum_observation_age_sec)
    require(hashlib.sha256(observer_report_bytes).hexdigest() == _hash(expected_report_sha256), 'observer_report_identity')
    report = _decode(observer_report_bytes, MAX_REPORT_BYTES, 'observer_report')
    _seal(report, 'observer_report_seal'); _authority(report)
    require(report['schema_version'] == 'joint_v3_ledger_status_observer_v2_20260909'
            and report['status'] in ('complete_observation', 'partial_observation'), 'observer_report_schema')
    begin, end = _epoch(report['started_epoch']), _epoch(report['completed_epoch'])
    require(begin <= end <= decision and decision - end <= maximum_observation_age_sec, 'observer_report_clock_window')
    spec = report['observer_spec']
    require(digest(spec) == report['observer_spec_sha256'] and spec['origin_registry_sha256'] == REGISTRY_SHA256
            and spec['origin_activation_sha256'] == ACTIVATION_SHA256
            and spec['observer_source_bindings'] == OBSERVER_BINDINGS
            and spec['original_source_bindings'] == registry['source_bindings']
            and spec['dependency_versions'] == registry['dependency_versions'] and spec['time_budget_sec'] == 8,
            'observer_source_closure_identity')
    _authority(spec)
    summary = report['summary']; _seal(summary, 'summary_seal'); _authority(summary)
    require(summary['schema_version'] == 'joint_v3_ledger_observer_summary_v2_20260909'
            and summary['registry_sha256'] == digest(spec) and summary['generated_epoch'] == end
            and digest(summary) == report['summary_sha256'], 'summary_identity')
    rows = summary['rows']
    require(type(rows) is list and len(rows) == 68 and all(type(r) is dict for r in rows), 'observer_pair_inventory')
    by_pair = {r['instrument']: r for r in rows}
    require(len(by_pair) == 68 and set(by_pair) == set(registry['pairs']), 'observer_pair_identity')
    signals, rejected = [], []
    for pair in wanted:
        try:
            signals.append(_candidate(pair, by_pair.get(pair), registry, report, decision,
                                      maximum_signal_age_sec, minimum_remaining_sec))
        except AdapterError as exc:
            rejected.append(dict(instrument=pair, reason=str(exc)))
        except (KeyError, TypeError, IndexError):
            rejected.append(dict(instrument=pair, reason='forecast_schema_incomplete'))
    result = dict(schema_version=SCHEMA, status='observed', signals=signals, rejected=rejected,
        requested_pairs=len(wanted), candidate_count=len(signals), rejection_count=len(rejected),
        decision_epoch=decision, observed_epoch=end, observer_started_epoch=begin,
        observer_report_sha256=expected_report_sha256, registry_sha256=REGISTRY_SHA256,
        observer_source_bindings=dict(OBSERVER_BINDINGS), original_research_flags_unchanged=True,
        control_policy='joint_main_only_no_ensemble_price_v2_not_collected',
        maximum_signal_age_sec=maximum_signal_age_sec, minimum_remaining_sec=minimum_remaining_sec,
        maximum_observation_age_sec=maximum_observation_age_sec, can_place_orders=False, can_authorize=False)
    result['payload_sha256'] = digest(result)
    return result


def read_candidates(project_root, now_epoch, *, instruments=None, maximum_signal_age_sec=900,
                    minimum_remaining_sec=60, maximum_observation_age_sec=90,
                    clock=time.time, monotonic=time.monotonic):
    """Read-only eight-second ledger scan plus original mandatory closure reads.

    project_root is the canonical ``trad`` directory. Errors refuse the whole
    read; individual unavailable pairs remain explicit in a successful batch.
    The returned original observer report is supplied for caller retention.
    """
    requested = _epoch(now_epoch)
    root = Path(project_root).resolve()
    require(root == Path(__file__).resolve().parent, 'canonical_project_root_required')
    last = requested
    def actual_clock():
        nonlocal last
        value = _epoch(clock())
        require(value >= last, 'adapter_clock_regression')
        last = value
        return value
    actual_clock()
    for name, expected in OBSERVER_BINDINGS.items():
        path = root / name
        require(path.stat().st_size <= MAX_REGISTRY_BYTES and hashlib.sha256(path.read_bytes()).hexdigest() == expected,
                'observer_import_source_identity')
    module_name = 'oanda_joint_v3_ledger_status_observer_v2'
    module_spec = importlib.util.find_spec(module_name)
    require(module_spec is not None and Path(module_spec.origin).resolve() == root / (module_name + '.py'),
            'observer_import_path')
    observer = importlib.import_module(module_name)
    require(Path(observer.__file__).resolve() == root / (module_name + '.py'), 'observer_loaded_path')
    registry_path = root / 'config' / 'joint_price_news_study_v3_20260908.json'
    registry_raw, registry_read = observer.read_file(registry_path, clock=actual_clock)
    require(registry_read['sha256'] == REGISTRY_SHA256, 'registry_source_identity')
    report = observer.observe_joint_v3(registry_path, root / 'data' / 'oanda_training_manager' / 'joint_price_news_study_v3',
        expected_observer_source_bindings=dict(OBSERVER_BINDINGS), expected_activation_sha256=ACTIVATION_SHA256,
        clock=actual_clock, monotonic=monotonic)
    raw = encoded(report)
    decision = actual_clock()
    result = build_candidates(raw, registry_raw, expected_report_sha256=hashlib.sha256(raw).hexdigest(),
        decision_epoch=decision, instruments=instruments, maximum_signal_age_sec=maximum_signal_age_sec,
        minimum_remaining_sec=minimum_remaining_sec, maximum_observation_age_sec=maximum_observation_age_sec)
    completed = actual_clock()
    require(completed - decision <= maximum_observation_age_sec, 'adapter_computation_expired')
    # Recheck at a later information cutoff. This is still before mapping work;
    # the separate completion receipt below must not be backdated to this clock.
    result = build_candidates(raw, registry_raw, expected_report_sha256=hashlib.sha256(raw).hexdigest(),
        decision_epoch=completed, instruments=instruments, maximum_signal_age_sec=maximum_signal_age_sec,
        minimum_remaining_sec=minimum_remaining_sec, maximum_observation_age_sec=maximum_observation_age_sec)
    result.update(requested_epoch=requested, registry_read=registry_read, observer_report=report)
    result.pop('payload_sha256')
    encoded(result)  # Complete substantive mapping/attachment/canonical preparation.
    result['adapter_completed_epoch'] = actual_clock()
    require(result['adapter_completed_epoch'] - result['observed_epoch'] <= maximum_observation_age_sec,
            'adapter_observation_expired_during_computation')
    result['decision_epoch_scope'] = 'adapter_information_cutoff_before_mapping_separate_policy_rechecks_actual_decision'
    result['payload_sha256'] = digest(result)
    return result
