"""Staged calibrated research ensemble. No imports from managers, broker or joblib.

Caller supplies retained per-row label availability and explicit estimator/calibrator
factories. This module cannot attest those source records or artifact bytes itself.
It creates a new, UNQUALIFIED fit identity; old fold thresholds are incompatible.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping

import numpy as np
import pandas as pd

CONTRACT = 'ensemble_probability_transform_v2'
CALIBRATION = 'logistic_regression_on_raw_positive_probability_v1'
COMBINATION = 'weighted_event_probability_and_direction_allocation_v2'
SHA = re.compile(r'^[0-9a-f]{64}$')
TARGET = re.compile(r'^(long_profit|short_profit|continuation_profit|reversal_profit|profitable_long_move|profitable_short_move|profitable_any_move)_([1-9][0-9]*)$')
CLOCK_COLUMNS = ('time_utc', 'feature_available_utc', 'target_end_utc', 'label_available_utc')

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()

def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('explicit aware clock required')
    return value.astimezone(timezone.utc)

def positive_probability(model, x, expected_classes=None):
    """Class label 1, never assumed column 1. Reject malformed probability rows."""
    classes = np.asarray(getattr(model, 'classes_', None))
    if classes.ndim != 1 or classes.shape != (2,) or classes.dtype.kind not in 'iu' or set(classes.tolist()) != {0, 1}:
        raise ValueError('binary integer class labels 0 and 1 required')
    if expected_classes is not None and classes.tolist() != expected_classes:
        raise ValueError('fitted class order changed')
    result = np.asarray(model.predict_proba(x), dtype=float)
    if result.shape != (len(x), 2) or not np.isfinite(result).all() or (result < 0).any() or (result > 1).any():
        raise ValueError('invalid probability shape or range')
    if not np.allclose(result.sum(axis=1), 1., rtol=0., atol=1e-8):
        raise ValueError('probability rows must sum to one')
    return result[:, classes.tolist().index(1)]

def target_contract(target, direction_target=None):
    match = TARGET.fullmatch(target) if isinstance(target, str) else None
    if match is None:
        raise ValueError('target semantics unsupported: supply a separately reviewed contract')
    family, horizon = match.group(1), int(match.group(2))
    if horizon > 1440:
        raise ValueError('unsupported horizon')
    direction = {'long_profit': 'long', 'short_profit': 'short', 'continuation_profit': 'momentum',
        'reversal_profit': 'inverse_momentum', 'profitable_long_move': 'long',
        'profitable_short_move': 'short', 'profitable_any_move': 'conditional_best_side'}[family]
    expected_direction = f'best_direction_up_{horizon}' if direction == 'conditional_best_side' else None
    if direction_target != expected_direction:
        raise ValueError('direction target does not match declared event target')
    value = {'target': target, 'horizon_minutes': horizon, 'positive_class': 1, 'direction_rule': direction,
        'event_semantics': 'profitable_move_onset' if family.startswith('profitable_') else 'specified_side_terminal_net_profit_positive',
        'direction_target': expected_direction,
        'direction_semantics': 'best_net_ATR_side_given_profitable_any_onset' if expected_direction else 'declared_side_or_observed_momentum_rule',
        'evaluation_return_unit': 'ATR_multiple', 'signed_midpoint_up_probability': False}
    return dict(value, target_contract_id=digest(value))

def feature_contract(features, units, source_schema_sha256, label_generator_sha256):
    if not isinstance(features, (list, tuple)) or not features or any(not isinstance(x, str) or not x for x in features) or len(set(features)) != len(features):
        raise ValueError('ordered unique named features required')
    if not isinstance(units, Mapping) or set(units) != set(features) or any(not isinstance(units[x], str) or not units[x] for x in features):
        raise ValueError('explicit unit for every feature required')
    for value in (source_schema_sha256, label_generator_sha256):
        if not isinstance(value, str) or not SHA.fullmatch(value):
            raise ValueError('source schema and label-generator SHA256 required')
    value = {'features': list(features), 'units': dict(units), 'source_schema_sha256': source_schema_sha256,
        'label_generator_sha256': label_generator_sha256, 'availability_basis': 'retained_per_row'}
    return dict(value, feature_contract_id=digest(value))

def _labels(frame, name, conditional=False):
    if name not in frame:
        raise ValueError('missing target labels')
    values = frame[name].to_numpy()
    if any(isinstance(x, (bool, np.bool_)) or not isinstance(x, (int, float, np.integer, np.floating)) for x in values):
        raise ValueError('integer binary target required')
    try:
        values = values.astype(float)
    except (TypeError, ValueError) as exc:
        raise ValueError('integer binary target required') from exc
    if not np.isfinite(values).all() or not np.isin(values, [0, 1]).all():
        raise ValueError('integer binary target required')
    if set(values.tolist()) != {0., 1.}:
        raise ValueError('both classes required in training and calibration')
    return values.astype(int)

def _features(frame, schema):
    features = schema['features']
    if any(name not in frame for name in features):
        raise ValueError('missing model feature')
    try:
        selected = frame.loc[:, features].astype(float)
    except (TypeError, ValueError) as exc:
        raise ValueError('invalid model feature') from exc
    if not np.isfinite(selected.to_numpy()).all():
        raise ValueError('invalid model feature')
    return selected

def _block(frame, schema, target, fit_asof, minimum):
    if not isinstance(frame, pd.DataFrame) or len(frame) < minimum or frame.columns.duplicated().any():
        raise ValueError('insufficient or invalid partition rows')
    required = {*CLOCK_COLUMNS, 'row_id', 'label_matured', 'availability_source_sha256'}
    if not required.issubset(frame.columns):
        raise ValueError('retained label maturity and partition clocks required')
    ids = frame['row_id'].tolist()
    if any(not isinstance(x, str) or not x for x in ids) or len(set(ids)) != len(ids):
        raise ValueError('unique retained row identity required')
    if any(not isinstance(x, (bool, np.bool_)) or not bool(x) for x in frame['label_matured']):
        raise ValueError('actual matured labels required')
    if any(not isinstance(x, str) or not SHA.fullmatch(x) for x in frame['availability_source_sha256']):
        raise ValueError('per-row availability source identity required')
    clocks = {name: [utc(x) for x in frame[name]] for name in CLOCK_COLUMNS}
    for origin, features_at, target_end, label_at in zip(*(clocks[x] for x in CLOCK_COLUMNS)):
        if features_at > origin or (target_end-origin).total_seconds() != target['horizon_minutes']*60:
            raise ValueError('feature or exact target clock violation')
        if label_at < target_end or label_at > fit_asof:
            raise ValueError('unmatured or future label availability')
    x = _features(frame, schema)
    y = _labels(frame, target['target'])
    return x, y, clocks

def _fit_head(engine, spec, train_x, train_y, calibration_x, calibration_y):
    model = engine.estimator(spec)
    model.fit(train_x, train_y)
    # This is the existing engine's calibrator, not a second calibration algorithm.
    raw = positive_probability(model, calibration_x)
    calibrator = engine.fit_probability_calibrator(raw, calibration_y)
    if calibrator is None:
        raise ValueError('calibration unavailable; cannot label raw probabilities calibrated')
    positive_probability(calibrator, raw.reshape(-1, 1))
    return {'model': model, 'calibrator': calibrator, 'model_classes': np.asarray(model.classes_).tolist(),
        'calibrator_classes': np.asarray(calibrator.classes_).tolist(), 'calibration_contract': CALIBRATION}

def fit_member_v2(*, engine, spec, train, calibration, schema, fit_asof, weight, min_train_rows=5000, min_calibration_rows=250):
    """No full-window refit. Explicit, disjoint mature train/calibration partitions."""
    fit_clock = utc(fit_asof)
    if type(min_train_rows) is not int or type(min_calibration_rows) is not int or min(min_train_rows, min_calibration_rows) < 2:
        raise ValueError('invalid research sample requirement')
    if not isinstance(spec, Mapping) or not isinstance(schema, Mapping):
        raise ValueError('explicit target and feature schema required')
    expected_schema = feature_contract(schema.get('features'), schema.get('units'), schema.get('source_schema_sha256'), schema.get('label_generator_sha256'))
    if dict(schema) != expected_schema:
        raise ValueError('feature contract identity mismatch')
    target = target_contract(spec.get('target'), spec.get('direction_target'))
    if spec.get('instrument_subset', 'all') != 'all':
        raise ValueError('non-all subset requires a separately bound eligibility adapter')
    if target['direction_rule'] in ('momentum', 'inverse_momentum') and 'momentum_30_atr' not in schema['features']:
        raise ValueError('direction momentum must belong to the source feature contract')
    if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight) or weight <= 0:
        raise ValueError('positive finite member weight required')
    train_x, train_y, train_clocks = _block(train, schema, target, fit_clock, min_train_rows)
    cal_x, cal_y, cal_clocks = _block(calibration, schema, target, fit_clock, min_calibration_rows)
    if set(train['row_id']) & set(calibration['row_id']):
        raise ValueError('train/calibration row identity overlap')
    if max(train_clocks['label_available_utc']) >= min(cal_clocks['time_utc']):
        raise ValueError('training labels overlap calibration decision clocks')
    direction_inputs = None
    if target['direction_target']:
        train_positive, cal_positive = train.loc[train_y == 1], calibration.loc[cal_y == 1]
        if len(train_positive) < min_train_rows or len(cal_positive) < min_calibration_rows:
            raise ValueError('insufficient conditional-direction calibration rows')
        direction_y = _labels(train_positive, target['direction_target'])
        direction_cal_y = _labels(cal_positive, target['direction_target'])
        direction_inputs = (_features(train_positive, schema), direction_y, _features(cal_positive, schema), direction_cal_y)
    head = _fit_head(engine, dict(spec), train_x, train_y, cal_x, cal_y)
    direction_head = _fit_head(engine, {**spec, 'target': target['direction_target']}, *direction_inputs) if direction_inputs else None
    def receipt(frame, clocks):
        identities = [{key: (utc(row[key]).isoformat() if key in CLOCK_COLUMNS else row[key])
            for key in ('row_id', *CLOCK_COLUMNS, 'availability_source_sha256')} for row in frame.to_dict('records')]
        return {'rows': len(frame), 'row_availability_sha256': digest(identities),
            'origin_min_utc': min(clocks['time_utc']).isoformat(), 'origin_max_utc': max(clocks['time_utc']).isoformat(),
            'label_available_max_utc': max(clocks['label_available_utc']).isoformat()}
    return {'contract': CONTRACT, 'member_fit_id': str(uuid.uuid4()), 'spec': dict(spec), 'schema': dict(schema),
        'target_contract': target, 'weight': float(weight), 'event_head': head, 'direction_head': direction_head,
        'fit_asof_utc': fit_clock.isoformat(), 'partitions': {'train': receipt(train, train_clocks), 'calibration': receipt(calibration, cal_clocks)},
        'sample_requirements': {'train': min_train_rows, 'calibration': min_calibration_rows},
        'evidence_role': 'development_fit_not_entry_qualification'}

def bundle_v2(members):
    if not isinstance(members, list) or not members:
        raise ValueError('nonempty fitted members required')
    for member in members:
        if member.get('contract') != CONTRACT or member.get('evidence_role') != 'development_fit_not_entry_qualification':
            raise ValueError('legacy or incompatible member')
        weight = member.get('weight')
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight) or weight <= 0:
            raise ValueError('positive finite member weight required')
    ids = [member['member_fit_id'] for member in members]
    horizons = {member['target_contract']['horizon_minutes'] for member in members}
    if len(set(ids)) != len(ids) or len(horizons) != 1:
        raise ValueError('duplicate member or mixed horizon')
    identity = {'contract': CONTRACT, 'combination': COMBINATION, 'member_fit_ids': ids,
        'schema_ids': [m['schema']['feature_contract_id'] for m in members],
        'target_ids': [m['target_contract']['target_contract_id'] for m in members], 'weights': [m['weight'] for m in members]}
    return {**identity, 'bundle_fit_id': digest(identity), 'members': members, 'horizon_minutes': horizons.pop(),
        'selected_threshold': None, 'qualification_status': 'unqualified_new_fit'}

def _predict_head(head, x):
    if not isinstance(head, Mapping) or head.get('calibration_contract') != CALIBRATION or head.get('calibrator') is None:
        raise ValueError('missing compatible member calibrator')
    raw = positive_probability(head['model'], x, head['model_classes'])
    return positive_probability(head['calibrator'], raw.reshape(-1, 1), head['calibrator_classes'])

def _direction(member, frame, x):
    rule = member['target_contract']['direction_rule']
    if rule in ('long', 'short'):
        return np.full(len(frame), 1. if rule == 'long' else 0.)
    if rule == 'conditional_best_side':
        return _predict_head(member['direction_head'], x)
    if 'momentum_30_atr' not in frame:
        raise ValueError('observed momentum required for side mapping')
    values = frame['momentum_30_atr'].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError('invalid observed momentum')
    return (values >= 0 if rule == 'momentum' else values < 0).astype(float)

def _member_semantics(member):
    if not isinstance(member, Mapping) or not isinstance(member.get('spec'), Mapping) or not isinstance(member.get('schema'), Mapping):
        raise ValueError('explicit fitted member semantics required')
    spec, saved = member['spec'], member['schema']
    target = target_contract(spec.get('target'), spec.get('direction_target'))
    schema = feature_contract(saved.get('features'), saved.get('units'), saved.get('source_schema_sha256'), saved.get('label_generator_sha256'))
    if target != member.get('target_contract') or schema != saved:
        raise ValueError('member semantic identity mismatch')
    if spec.get('instrument_subset', 'all') != 'all':
        raise ValueError('non-all subset requires a separately bound eligibility adapter')
    if target['direction_rule'] in ('momentum', 'inverse_momentum') and 'momentum_30_atr' not in schema['features']:
        raise ValueError('direction momentum must belong to the source feature contract')
    return target, schema


def _validated_bundle(bundle):
    if not isinstance(bundle, Mapping) or bundle.get('contract') != CONTRACT:
        raise ValueError('legacy bundle requires separate calibrated fit and evidence')
    rebuilt = bundle_v2(bundle.get('members'))
    # Rebuilding a hash is insufficient if the caller then trusts unverified
    # duplicate metadata from the outer dictionary.
    for key, value in rebuilt.items():
        if key != 'members' and (key not in bundle or bundle[key] != value):
            raise ValueError('bundle transform identity mismatch:' + key)
    for member in rebuilt['members']:
        _member_semantics(member)
    return rebuilt


def _score_vector(value, rows=None):
    if not isinstance(value, (list, tuple)) or not value or (rows is not None and len(value) != rows):
        raise ValueError('nonempty aligned score arrays required')
    if any(isinstance(x, (bool, np.bool_)) or not isinstance(x, (int, float, np.integer, np.floating)) for x in value):
        raise ValueError('numeric score arrays required')
    result = np.asarray(value, dtype=float)
    if result.ndim != 1 or not np.isfinite(result).all() or (result < 0).any() or (result > 1).any():
        raise ValueError('finite score probabilities in zero to one required')
    return result


def _validated_score(bundle, score):
    if not isinstance(score, Mapping) or score.get('contract') != CONTRACT or score.get('bundle_fit_id') != bundle['bundle_fit_id'] or score.get('score_semantics') != COMBINATION:
        raise ValueError('score transform identity mismatch')
    if score.get('entry_eligible') is not False or score.get('qualification_status') != 'unqualified_new_fit' or score.get('signed_midpoint_up_probability', 'missing') is not None or score.get('return_magnitude', 'missing') is not None:
        raise ValueError('unqualified allocation score flags required')
    event = _score_vector(score.get('event_score')); rows = len(event)
    share = _score_vector(score.get('long_allocation_score'), rows)
    agreement = _score_vector(score.get('agreement_score'), rows)
    directions = score.get('required_direction')
    if not isinstance(directions, (list, tuple)) or len(directions) != rows or any(x not in ('LONG', 'SHORT', None) for x in directions):
        raise ValueError('aligned declared score directions required')
    details = score.get('members')
    if type(score.get('member_count')) is not int or score['member_count'] != len(bundle['members']) or not isinstance(details, list) or len(details) != len(bundle['members']):
        raise ValueError('score member count differs from fitted bundle')
    probability_weighted = np.zeros(rows); long_weight = np.zeros(rows); short_weight = np.zeros(rows); total_weight = 0.
    for fitted, detail in zip(bundle['members'], details):
        if not isinstance(detail, Mapping) or detail.get('member_fit_id') != fitted['member_fit_id'] or detail.get('target_contract') != fitted['target_contract']:
            raise ValueError('score member identity differs from fitted bundle')
        weight = detail.get('weight')
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight) or weight != fitted['weight']:
            raise ValueError('score member weight differs from fitted bundle')
        probability = _score_vector(detail.get('event_probability'), rows)
        allocation = _score_vector(detail.get('long_allocation'), rows)
        rule = fitted['target_contract']['direction_rule']
        if rule in ('long', 'short') and not np.all(allocation == (1. if rule == 'long' else 0.)):
            raise ValueError('score allocation differs from declared fixed side')
        if rule in ('momentum', 'inverse_momentum') and not np.isin(allocation, [0., 1.]).all():
            raise ValueError('observed momentum allocation must be binary')
        probability_weighted += probability * weight
        long_weight += probability * allocation * weight
        short_weight += probability * (1. - allocation) * weight
        total_weight += weight
    if not math.isfinite(total_weight) or not all(np.isfinite(x).all() for x in (probability_weighted, long_weight, short_weight)):
        raise ValueError('nonfinite aggregate weight or score')
    total = long_weight + short_weight
    expected_share = np.divide(long_weight, total, out=np.full(rows, .5), where=total > 0)
    expected_agreement = np.divide(np.maximum(long_weight, short_weight), total, out=np.zeros(rows), where=total > 0)
    expected_direction = ['LONG' if x > .5 else 'SHORT' if x < .5 else None for x in expected_share]
    if not np.array_equal(event, probability_weighted / total_weight) or not np.array_equal(share, expected_share) or not np.array_equal(agreement, expected_agreement) or list(directions) != expected_direction:
        raise ValueError('score aggregates differ from retained member evidence')
    return score


def score_bundle_v2(bundle, frame, *, feature_contract_ids):
    """Identical calibrated event/direction transformation for evaluation and intake.

    Heterogeneous event probabilities produce an allocation SCORE, not calibrated
    P(midpoint up), expected return, or a qualified entry decision.
    """
    rebuilt = _validated_bundle(bundle)
    if not isinstance(frame, pd.DataFrame) or len(frame) == 0 or frame.columns.duplicated().any():
        raise ValueError('nonempty unique-column feature rows required')
    if not isinstance(feature_contract_ids, (list, tuple)) or list(feature_contract_ids) != rebuilt['schema_ids']:
        raise ValueError('inference source feature schemas do not match training')
    probability_weighted = np.zeros(len(frame)); long_weight = np.zeros(len(frame)); short_weight = np.zeros(len(frame))
    total_weight = 0.; details = []
    for member in bundle['members']:
        target = target_contract(member['spec']['target'], member['spec'].get('direction_target'))
        schema = feature_contract(member['schema']['features'], member['schema']['units'], member['schema']['source_schema_sha256'], member['schema']['label_generator_sha256'])
        if target != member['target_contract'] or schema != member['schema']:
            raise ValueError('member semantic identity mismatch')
        x = _features(frame, member['schema'])
        event_probability = _predict_head(member['event_head'], x)
        direction = _direction(member, frame, x)
        weight = member['weight']
        probability_weighted += event_probability * weight
        long_weight += event_probability * direction * weight
        short_weight += event_probability * (1. - direction) * weight
        total_weight += weight
        details.append({'member_fit_id': member['member_fit_id'], 'target_contract': member['target_contract'],
            'event_probability': event_probability.tolist(), 'long_allocation': direction.tolist(), 'weight': weight})
    if not math.isfinite(total_weight) or not all(np.isfinite(values).all() for values in (probability_weighted, long_weight, short_weight)):
        raise ValueError('nonfinite aggregate weight or score')
    total = long_weight + short_weight
    share = np.divide(long_weight, total, out=np.full(len(frame), .5), where=total > 0)
    agreement = np.divide(np.maximum(long_weight, short_weight), total, out=np.zeros(len(frame)), where=total > 0)
    return {'contract': CONTRACT, 'bundle_fit_id': bundle['bundle_fit_id'], 'score_semantics': COMBINATION,
        'event_score': (probability_weighted / total_weight).tolist(), 'long_allocation_score': share.tolist(),
        'agreement_score': agreement.tolist(), 'member_count': len(details), 'members': details,
        'required_direction': ['LONG' if x > .5 else 'SHORT' if x < .5 else None for x in share],
        'signed_midpoint_up_probability': None, 'return_magnitude': None, 'entry_eligible': False,
        'qualification_status': 'unqualified_new_fit'}

def research_decision_v2(bundle, score, policy, *, requested_direction=None):
    """Applies only explicitly matching development thresholds; cannot promote."""
    bundle = _validated_bundle(bundle)
    score = _validated_score(bundle, score)
    if not isinstance(policy, Mapping) or policy.get('contract') != CONTRACT or policy.get('bundle_fit_id') != bundle['bundle_fit_id'] or policy.get('combination') != COMBINATION:
        raise ValueError('threshold evidence belongs to a different fit or transform')
    if score.get('bundle_fit_id') != bundle['bundle_fit_id'] or score.get('score_semantics') != COMBINATION:
        raise ValueError('score transform identity mismatch')
    if policy.get('evidence_role') != 'development_thresholds_only':
        raise ValueError('entry qualification is outside this research contract')
    mode = policy.get('mode')
    if mode not in ('directional_research', 'opportunity_only'):
        raise ValueError('explicit policy target role required')
    threshold, agreement = policy.get('event_score_threshold'), policy.get('agreement_threshold')
    minimum = policy.get('min_members')
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or not 0 <= x <= 1 for x in (threshold, agreement)) or type(minimum) is not int or minimum < 1:
        raise ValueError('invalid development thresholds')
    if requested_direction is not None and requested_direction not in ('LONG', 'SHORT'):
        raise ValueError('invalid requested direction')
    passed = []
    for probability, observed_agreement, direction in zip(score['event_score'], score['agreement_score'], score['required_direction']):
        direction_ok = mode == 'opportunity_only' or (direction is not None and observed_agreement >= agreement and (requested_direction is None or requested_direction == direction))
        passed.append(bool(probability >= threshold and score['member_count'] >= minimum and direction_ok))
    return {'research_threshold_pass': passed, 'mode': mode, 'entry_eligible': False,
        'qualification_status': 'development_thresholds_only_not_signed_entry_evidence'}

def manager_research_score_v2(bundle, signal, snapshot, policy=None):
    """Pure caller adapter; no manager initialization, sync/load or live features."""
    try:
        if not isinstance(signal, Mapping) or not isinstance(snapshot, Mapping):
            raise ValueError('explicit feature snapshot required')
        # Features come from one caller-attested snapshot. No signal-over-snapshot
        # mixing; the caller must bind the exact training source schema itself.
        ids = signal.get('_ensemble_feature_contract_ids_v2')
        score = score_bundle_v2(bundle, pd.DataFrame([dict(snapshot)]), feature_contract_ids=ids)
        decision = research_decision_v2(bundle, score, policy, requested_direction=signal.get('direction')) if policy is not None else None
        return {'available': True, 'contract': CONTRACT, 'mode': 'shadow_research_v2',
            'event_score': score['event_score'][0], 'long_allocation_score': score['long_allocation_score'][0],
            'agreement_score': score['agreement_score'][0], 'required_direction': score['required_direction'][0],
            'member_count': score['member_count'], 'probability': None, 'shadow_approved': False,
            'signed_entry_qualified': False, 'research_threshold_pass': decision['research_threshold_pass'][0] if decision else None,
            'reason': 'new calibrated research fit; signed entry evidence not established'}
    except Exception:
        return {'available': False, 'contract': CONTRACT, 'shadow_approved': False, 'signed_entry_qualified': False,
            'reason': 'incompatible ensemble fit, source schema, probability or policy contract'}

def refuse_legacy_final_fit(*args, **kwargs):
    raise ValueError('v2 final fitting requires explicit retained maturity clocks and disjoint train/calibration partitions; legacy full-window inputs cannot establish calibration')
