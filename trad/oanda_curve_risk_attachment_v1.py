"""Pure exact-event curve/risk diagnostic; no I/O, fit, orders or policy routing.

Caller supplies independently retained chains, trusted registry identities and
actual downstream read receipts. Original validators replay the full contracts;
hashes alone do not authenticate I/O or prove predictive/coverage calibration.
This attachment never converts original-horizon marginals to remaining risk.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Context, localcontext
import time

import oanda_forecast_curve_contract_v1 as curves
import oanda_m1_risk_distribution_contract_v1 as risks

SCHEMA = 'exact_event_curve_risk_attachment_v1_20260909'
READ_SCHEMA = 'curve_risk_attachment_reads_v1_20260909'
POLICY = {
    'reference_match': 'exact_effective_close_epoch_official_midpoint_and_price',
    'target_match': 'exact_original_price_epoch_with_zero_selection_window',
    'risk_methods': list(risks.METHODS),
    'risk_values': 'unchanged_original_full_horizon_marginals',
    'bar_labels': 'retained_separately_never_rounded_or_relabelled',
    'side_basis': 'explicit_caller_context_not_direction_admission',
    'price_basis': 'original_reference_midpoint_bps_and_original_candle_bid_ask',
}
AUTHORITY = dict(curves.AUTHORITY, positions_managed=False,
                 conditional_remaining_risk=False, risk_probability_mixed=False,
                 stop_policy_created=False, sizing_policy_created=False,
                 forecast_issued=False, calibrated_coverage_claim=False)
COMMON_LABELS = ('signed_terminal_bps', 'absolute_terminal_bps',
                 'mid_future_range_bps', 'realized_path_variation_bps')
OBJECT_KEYS = ('curve', 'curve_publication', 'curve_consumption',
               'risk_issue', 'risk_publication', 'risk_consumption')


def need(condition, reason):
    if not condition:
        raise ValueError(reason)


def _copy(value):
    # Bound each original object separately: a real 300-row M1 mapping is much
    # larger in structural member count than the compact curve contract.
    return risks.decode(risks.canonical(value))


def _side(value):
    need(type(value) is dict and set(value) == {'role', 'side'}, 'attachment_side_context_shape')
    need(value['role'] in ('candidate', 'incumbent', 'distribution_context'), 'attachment_side_role')
    need(type(value['side']) is int and value['side'] in (-1, 0, 1), 'attachment_side_value')
    need((value['role'] == 'distribution_context') == (value['side'] == 0),
         'attachment_neutral_is_distribution_context_only')
    return _copy(value)


def _reads(receipt, objects, original_available, decision):
    need(type(receipt) is dict and set(receipt) == {'schema_version', 'objects'} and
         receipt['schema_version'] == READ_SCHEMA, 'attachment_read_schema')
    rows = receipt['objects']
    need(type(rows) is dict and set(rows) == set(OBJECT_KEYS), 'attachment_read_inventory')
    previous = None
    for name in OBJECT_KEYS:
        row = rows[name]
        need(type(row) is dict and set(row) == {
            'canonical_sha256', 'read_started_epoch', 'read_completed_epoch'}, 'attachment_read_shape')
        start, end = risks.epoch(row['read_started_epoch']), risks.epoch(row['read_completed_epoch'])
        need(original_available[name] <= start <= end <= decision,
             'attachment_original_availability_or_downstream_read_clock')
        # An explicit serial read protocol gives each original object its own
        # observation cutoff; later reads cannot age a future record into use.
        need(previous is None or previous <= start, 'attachment_read_order')
        need(row['canonical_sha256'] == risks.digest(objects[name]), 'attachment_read_identity')
        previous = end
    return _copy(receipt)


def attach_risk_for_target(curve, curve_publication, curve_consumption,
                          risk_issue, risk_publication, risk_consumption, *,
                          risk_input_raw, risk_input_receipt, metadata, fit_raw,
                          expected_curve_sources, expected_risk_sources,
                          expected_curve_identity, attachment_read_receipt,
                          decision_epoch, target_epoch, side_context, clock=time.time):
    """Return sealed exact-event attachment or explicit unavailable reasons.

    Invalid/tampered evidence raises; a valid but different/expired event is an
    inert refusal. The caller's decision_epoch is an information context, not a
    backdated claim that this new diagnostic was computed then. Actual completion
    is sampled after replay, retained separately, and must not precede decision.
    Candidate/incumbent side is caller-labelled; no live position is authenticated.
    """
    decision, target = risks.epoch(decision_epoch), risks.epoch(target_epoch)
    side = _side(side_context)
    objects = {k: _copy(v) for k, v in zip(OBJECT_KEYS, (
        curve, curve_publication, curve_consumption, risk_issue, risk_publication, risk_consumption))}
    curve, pub, consume, risk, risk_pub, risk_consume = (objects[k] for k in OBJECT_KEYS)
    cs, rs = _copy(expected_curve_sources), _copy(expected_risk_sources)
    curves.validate_consumption(curve, pub, consume, expected_source_bindings=cs)
    risk = risks.validate_chain(risk, risk_pub, risk_consume, input_raw=risk_input_raw,
        input_receipt=_copy(risk_input_receipt), metadata=_copy(metadata), fit_raw=fit_raw,
        expected_source_bindings=rs)
    prepared = curve['prepared_curve']
    expected = _copy(expected_curve_identity)
    need(set(expected) == {'forecast_cohort', 'model_sha256', 'policy_sha256'},
         'attachment_expected_curve_identity_shape')
    actual = dict(forecast_cohort=prepared['forecast_cohort'], model_sha256=prepared['model_sha256'],
                  policy_sha256=prepared['policy']['policy_sha256'])
    need(actual == expected, 'attachment_curve_registry_identity')
    # Risk validator fixes the cohort, artifact and full two-method inventory.
    need(risk['cohort_id'] == risks.COHORT_ID and
         risk['training_artifact_sha256'] == risks.TRAINING_ARTIFACT_SHA256,
         'attachment_risk_registry_identity')
    available = dict(curve=curve['issued_epoch'],
        curve_publication=pub['publication_completed_epoch'], curve_consumption=consume['available_epoch'],
        risk_issue=risk['issued_epoch'], risk_publication=risk_pub['publication_completed_epoch'],
        risk_consumption=risk_consume['read_completed_epoch'])
    reads = _reads(attachment_read_receipt, objects, available, decision)
    reasons = []
    fail = lambda code: reasons.append(code) if code not in reasons else None
    if prepared['instrument'] != risk['instrument']:
        fail('instrument_mismatch')
    md = risk['input_metadata']
    if (md['instrument'] != prepared['instrument'] or md['base_currency'] != prepared['instrument'][:3]
            or md['quote_currency'] != prepared['instrument'][-3:]):
        fail('currency_metadata_mismatch')
    with localcontext(Context(prec=192)):
        if curves.decimal_number(prepared['pip_size']) != curves.decimal_number(md['pip_size']):
            fail('pip_size_mismatch')
        if curves.decimal_number(prepared['reference_price']) != curves.decimal_number(risk['reference_mid']):
            fail('reference_price_mismatch')
    if prepared['reference_epoch'] != risk['reference_price_epoch']:
        fail('reference_price_epoch_mismatch')
    convention = prepared['input_context'].get('price_convention')
    if convention != 'official_midpoint' or risk['input_mapping']['price_convention'] != convention:
        fail('reference_price_convention_mismatch')
    # Kind must explicitly describe the same official M close, preserving the
    # bar duration. This permits exact S5/M1 close joins, not unknown-price tags.
    expected_kind = {5: 'official_midpoint_S5_close', 60: 'official_midpoint_M1_close'}.get(
        prepared['bar_duration_sec'])
    if expected_kind is None or prepared['reference_price_kind'] != expected_kind:
        fail('reference_price_kind_unproven')
    nodes = [v for v in prepared['nodes'] if v['original_target_epoch'] == target]
    node = nodes[0] if len(nodes) == 1 else None
    if node is None:
        fail('curve_native_target_unavailable')
    elif node['status'] != 'forecast':
        fail('curve_native_node_unavailable')
    elif next(v for v in curve['node_admission'] if v['node_id'] == node['node_id'])['status'] != 'admitted':
        fail('curve_node_withheld_at_original_issue')
    risk_nodes = [v for v in risk['nodes'] if v['target_price_epoch'] == target]
    if not risk_nodes:
        fail('risk_exact_native_target_unavailable')
    if prepared['target_selection_policy'] != {'kind': 'exact_price_epoch', 'maximum_delay_sec': 0}:
        fail('target_selection_window_mismatch')
    if node is not None and node['target_price_window_end_epoch'] != target:
        fail('target_selection_window_mismatch')
    if decision >= target:
        fail('original_target_elapsed_at_decision')
    elif target - decision <= prepared['policy']['minimum_remaining_sec']:
        fail('curve_minimum_remaining_not_met')
    if decision - curve['issued_epoch'] > prepared['policy']['maximum_decision_age_sec']:
        fail('curve_too_old_at_decision')
    labels = list(COMMON_LABELS)
    if side['side']:
        prefix = 'long' if side['side'] == 1 else 'short'
        labels.extend(prefix + '_' + suffix for suffix in (
            'close_MFE_bps', 'close_MAE_bps', 'envelope_MFE_bps', 'envelope_MAE_bps'))
    selected = [v for v in risk_nodes if v['label'] in labels]
    if risk_nodes:
        need(len(selected) == len(labels) * 2 and
             {(v['method'], v['label']) for v in selected} == {
                 (method, label) for method in risks.METHODS for label in labels},
             'attachment_risk_node_inventory')
    body = dict(schema_version=SCHEMA, **AUTHORITY, policy=deepcopy(POLICY),
        status='unavailable' if reasons else 'attached_original_horizon_diagnostic',
        reason_codes=reasons, decision_epoch=decision, target_epoch=target, side_context=side,
        information_available_epoch=max(v['read_completed_epoch'] for v in reads['objects'].values()),
        attachment_read_receipt=reads, original_object_hashes={k: risks.digest(v) for k, v in objects.items()},
        curve_identity=dict(**actual, curve_id=curve['curve_id'], curve_sha256=curve['curve_sha256'],
            node_id=node['node_id'] if node else None, source_bindings=cs),
        risk_identity=dict(cohort_id=risk['cohort_id'], issued_sha256=risk['issued_sha256'],
            training_artifact_sha256=risk['training_artifact_sha256'], source_bindings=rs),
        curve_event=dict(instrument=prepared['instrument'], reference_price_epoch=prepared['reference_epoch'],
            reference_label_epoch=prepared['reference_label_epoch'], bar_duration_sec=prepared['bar_duration_sec'],
            reference_price=prepared['reference_price'], price_convention=convention,
            target_selection_policy=prepared['target_selection_policy'],
            target_label_epoch=node['target_label_epoch'] if node else None,
            target_price_epoch=node['original_target_epoch'] if node else None),
        risk_event=dict(instrument=risk['instrument'], reference_price_epoch=risk['reference_price_epoch'],
            reference_label_epoch=risk['reference_label_epoch'], bar_duration_sec=60,
            reference_price=risk['reference_mid'], price_convention=risk['input_mapping']['price_convention'],
            target_selection_policy=dict(kind='exact_price_epoch', maximum_delay_sec=0),
            native_target_price_epochs=sorted({v['target_price_epoch'] for v in risk['nodes']})),
        original_curve_node=deepcopy(node), selected_labels=labels, risk_nodes=[] if reasons else selected,
        risk_origin_bid_ask=deepcopy({k: risk['origin_row'][k] for k in ('bid', 'ask')}),
        original_clocks=available, original_risk_probability_scope=risk['probability_scope'],
        original_risk_distribution_scope=risk['distribution_scope'],
        interpretation=dict(full_horizon_marginals_only=True, conditional_remaining_distribution=False,
            actual_position_entry_basis_verified=False, current_price_rebased_quantiles=False,
            candidate_direction_verified=False, live_position_verified=False,
            joint_distribution_or_direction_probability=False, joint_event_calibration_proven=False,
            historical_ingestion_equivalence_proven=False,
            missing_is_measured_zero=False, path_envelopes_are_executable_fills=False,
            attachment_is_new_forecast=False, registry_and_io_authentication='caller_responsibility'))
    completed = risks.epoch(clock())
    need(completed >= decision, 'attachment_computation_clock_precedes_information_context')
    body['attachment_computed_epoch'] = completed
    return risks._seal(_copy(body), 'attachment_sha256')


def validate_attachment(attachment, *arguments, **keywords):
    """Rebuild with full original chains; resealing an attachment is insufficient."""
    value = _copy(attachment)
    need(value.get('schema_version') == SCHEMA and all(value.get(k) is v for k, v in AUTHORITY.items()),
         'attachment_schema_or_authority')
    need('clock' not in keywords, 'attachment_validation_clock_override')
    rebuilt = attach_risk_for_target(*arguments, **keywords,
        clock=lambda: value['attachment_computed_epoch'])
    need(rebuilt == value, 'attachment_replay_mismatch')
    return rebuilt
