"""Strict prospective M1 distribution outcomes; no source requests or fitting.

Each future source is independently replayed from its retained raw response and
receipt. Query domains are never merged. The original published origin and
targets remain fixed, including when a later source revises the origin candle.
"""
from collections import Counter, defaultdict
from copy import deepcopy
from decimal import Context, Decimal, localcontext
import base64
import hashlib
import json
import math
import time

import oanda_m1_path_risk_labels_v1 as labels

SCHEMA = 'prospective_m1_risk_distribution_outcomes_v1_20260909'
CAPTURE_SCHEMA = 'prospective_m1_risk_future_capture_v1_20260909'
METHODS = ('training_empirical', 'training_empirical_volatility_scaled')
PAIRS = ('EUR_USD', 'GBP_USD', 'USD_JPY')
QUANTILES = (.1, .5, .9)
MAX_CAPTURES = 128
MAX_CAPTURE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 128 * 1024 * 1024
FLAGS = dict(research_only=True, can_place_orders=False, can_promote=False,
             can_authorize=False, account_eligible=False, execution_eligible=False,
             proof_eligible=False, model_fits=False, broker_requests=False,
             runtime_writes=False)


def need(ok, reason):
    if not ok:
        raise ValueError('m1_outcome_' + reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def epoch(value):
    need(type(value) in (int, float) and math.isfinite(value) and value > 0, 'clock')
    return value


def sealed(body, field):
    return {**body, field: digest(body)}


def capture_future_m1(raw_bytes, receipt, metadata, *, clock=time.time):
    """Derive a new seal now while retaining original provider-read availability."""
    import oanda_m1_mba_research_capture_v1 as mapper
    need(type(raw_bytes) is bytes and 0 < len(raw_bytes) <= MAX_CAPTURE_BYTES,
         'source_byte_bound')
    mapping = mapper.map_verified_capture(raw_bytes, receipt, metadata)
    document = mapper.mapping_document(mapping)
    attestation = document['source_attestation']
    completed = epoch(attestation['read_completed_epoch'])
    instrument = document['instrument']
    rows, parse = labels.parse_csv(mapping['canonical_csv_bytes'], instrument,
                                   observed_epoch=completed)
    need(1 <= len(rows) <= 300, 'source_row_bound')
    need(attestation['complete_provider_response'] is True
         and attestation['source_query_complete'] is True, 'source_domain_attestation')
    first, last = (attestation[k] for k in
                  ('first_complete_bar_label_epoch', 'last_complete_bar_label_epoch'))
    need(first == rows[0]['label_epoch'] and last == rows[-1]['label_epoch'],
         'source_domain_row_mismatch')
    body = dict(schema_version=CAPTURE_SCHEMA, instrument=instrument,
        raw_base64=base64.b64encode(raw_bytes).decode('ascii'),
        original_receipt=deepcopy(receipt), metadata=deepcopy(metadata),
        source_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        receipt_sha256=document['receipt_sha256'], mapping_document=document,
        source_read_completed_epoch=completed,
        first_label_epoch=first, last_label_epoch=last,
        price_convention='official_midpoint', rows=rows, parse_evidence=parse,
        original_availability_reconstructed=False, **FLAGS)
    # Record the new derived evidence only after mapper/parser/body work, while
    # preserving the earlier actual provider response and receipt clocks.
    observed = epoch(clock())
    need(completed <= epoch(receipt['capture_completed_epoch']) <= observed,
         'derived_observation_before_source')
    body['derived_observed_epoch'] = observed
    need(len(canonical(body)) <= MAX_CAPTURE_BYTES, 'derived_capture_byte_bound')
    return sealed(body, 'capture_sha256')


def validate_future_capture(capture):
    need(type(capture) is dict and capture.get('schema_version') == CAPTURE_SCHEMA,
         'capture_schema')
    need(len(canonical(capture)) <= MAX_CAPTURE_BYTES, 'derived_capture_byte_bound')
    need(capture.get('capture_sha256') == digest({k: v for k, v in capture.items()
         if k != 'capture_sha256'}), 'capture_seal')
    try:
        raw = base64.b64decode(capture['raw_base64'], validate=True)
    except (ValueError, TypeError):
        raise ValueError('m1_outcome_raw_encoding') from None
    rebuilt = capture_future_m1(raw, capture['original_receipt'], capture['metadata'],
                               clock=lambda: capture['derived_observed_epoch'])
    need(rebuilt == capture, 'capture_raw_mapping_replay')
    return deepcopy(capture)


def _validate_origin(origin, instrument):
    need(type(origin) is dict and set(origin) == {
        'label_epoch', 'price_epoch', 'mid', 'bid', 'ask',
        'bid_ask_unavailable_reason'}, 'origin_row_shape')
    label = epoch(origin['label_epoch'])
    need(type(label) is int and label % 60 == 0
         and origin['price_epoch'] == label + 60, 'origin_clock')
    labels.prices(origin['mid'])
    if origin['bid'] is not None or origin['ask'] is not None:
        need(origin['bid'] is not None and origin['ask'] is not None, 'origin_partial_bidask')
        labels.prices(origin['bid']); labels.prices(origin['ask'])
        need(all(labels.number(origin['bid'][k]) <= labels.number(origin['ask'][k])
                 for k in labels.FIELDS), 'origin_crossed_bidask')
    need(instrument in PAIRS, 'instrument_scope')


def _source_reference(capture, origin):
    revised = next((row for row in capture['rows']
                    if row['label_epoch'] == origin['label_epoch']), None)
    return dict(capture_sha256=capture['capture_sha256'],
        source_sha256=capture['source_sha256'], receipt_sha256=capture['receipt_sha256'],
        mapping_sha256=capture['mapping_document']['mapping_sha256'],
        original_source_read_completed_epoch=capture['source_read_completed_epoch'],
        derived_observed_epoch=capture['derived_observed_epoch'],
        first_label_epoch=capture['first_label_epoch'], last_label_epoch=capture['last_label_epoch'],
        later_origin_present=revised is not None,
        later_origin_equals_original=None if revised is None else revised == origin,
        original_origin_preserved=True)


def label_from_captures(origin, instrument, horizon_minutes, captures, *, as_of_epoch):
    """Earliest decisive source per label, selected by clocks/domain, never values.

    The caller supplies already validated immutable captures. Public scoring
    validates them first; this helper is useful for focused selection fixtures.
    """
    _validate_origin(origin, instrument)
    need(type(horizon_minutes) is int and horizon_minutes in labels.HORIZONS, 'horizon')
    now = epoch(as_of_epoch)
    target_label = origin['label_epoch'] + horizon_minutes * 60
    target_price = target_label + 60
    rows = sorted(captures, key=lambda x: (x['source_read_completed_epoch'],
                  x['source_sha256'], x['receipt_sha256'], x['capture_sha256']))
    selected = {}
    for name in labels.CONTINUOUS_LABELS:
        path_label = name not in labels.CONTINUOUS_LABELS[:2]
        domain_start = origin['label_epoch'] + 60 if path_label else target_label
        for capture in rows:
            if (capture['instrument'] != instrument
                    or capture['source_read_completed_epoch'] > now
                    or capture['derived_observed_epoch'] > now
                    or capture['source_read_completed_epoch'] < target_price
                    or capture['first_label_epoch'] > domain_start
                    or capture['last_label_epoch'] < target_label):
                continue
            future = [row for row in capture['rows']
                      if origin['label_epoch'] < row['label_epoch'] <= target_label]
            # Do not silently replace the originally observed reference candle.
            result = labels.label_origin([deepcopy(origin), *future], 0, horizon_minutes)
            value = result['values'][name]
            selected[name] = dict(status='scored' if value is not None else 'unavailable',
                value_bps=value, reason_code=result['unavailable_reasons'].get(name),
                source=_source_reference(capture, origin),
                target_label_epoch=target_label, target_price_epoch=target_price,
                future_path_rows=result['future_path_rows'],
                target_selection=result['target_selection'])
            break
        if name not in selected:
            selected[name] = dict(status='pending' if now < target_price else 'unavailable',
                value_bps=None, reason_code='target_not_mature' if now < target_price
                    else 'no_attested_source_covers_required_domain', source=None,
                target_label_epoch=target_label, target_price_epoch=target_price)
    return selected


def _quantile_scores(value, predictions):
    need(type(value) in (int, float) and math.isfinite(value), 'finite_label')
    need(type(predictions) is list and len(predictions) == 3
         and all(type(v) in (int, float) and math.isfinite(v) for v in predictions)
         and predictions == sorted(predictions), 'quantile_shape_or_order')
    with localcontext(Context(prec=96)):
        actual = Decimal(str(value)); q = list(map(lambda x: Decimal(str(x)), predictions))
        pinball = []
        for level, estimate in zip(QUANTILES, q):
            error = actual - estimate
            pinball.append(str(max(Decimal(str(level))*error,
                (Decimal(str(level))-1)*error)))
        return dict(pinball_loss_bps=pinball, median_absolute_error_bps=str(abs(actual-q[1])),
            interval_80_covered=q[0] <= actual <= q[2], interval_width_bps=str(q[2]-q[0]),
            below_lower=actual < q[0], above_upper=actual > q[2])


def _validate_chain(issued, publication, consumption, **kwargs):
    import oanda_m1_risk_distribution_contract_v1 as contract
    return contract.validate_chain(issued, publication, consumption, **kwargs)


def score_distribution_from_captures(issued, publication, consumption, captures, *,
        input_raw, input_receipt, metadata, fit_raw, expected_source_bindings,
        as_of_epoch, clock=time.time):
    """Verify the original chain and score its fixed72 distribution nodes."""
    now = epoch(as_of_epoch)
    issued = _validate_chain(issued, publication, consumption, input_raw=input_raw,
        input_receipt=input_receipt, metadata=metadata, fit_raw=fit_raw,
        expected_source_bindings=expected_source_bindings)
    need(max(epoch(issued['issued_epoch']), epoch(publication['publication_completed_epoch']),
             epoch(consumption['read_completed_epoch']),
             epoch(issued['input_consumption']['read_completed_epoch'])) <= now,
         'chain_unavailable_at_evaluation_cutoff')
    need(type(captures) in (list, tuple) and len(captures) <= MAX_CAPTURES, 'capture_count_bound')
    need(sum(len(canonical(c)) for c in captures) <= MAX_TOTAL_BYTES, 'capture_total_byte_bound')
    validated = [validate_future_capture(c) for c in captures]
    unique = {}; source_identities = {}
    for c in validated:
        need(c['derived_observed_epoch'] <= now, 'future_derived_capture')
        key = (c['source_sha256'], c['receipt_sha256'])
        identity = digest({k: v for k, v in c.items() if k not in
                          ('capture_sha256', 'derived_observed_epoch')})
        need(key not in source_identities or source_identities[key] == identity,
             'conflicting_same_source_mapping')
        source_identities[key] = identity
        prior = unique.get(key)
        if prior is None or c['derived_observed_epoch'] < prior['derived_observed_epoch']:
            unique[key] = c
    instrument = issued['instrument']; origin = issued['origin_row']
    _validate_origin(origin, instrument)
    need(issued['reference_label_epoch'] == origin['label_epoch']
         and issued['reference_price_epoch'] == origin['price_epoch']
         and issued['reference_mid'] == origin['mid']['close'], 'original_reference_binding')
    need(issued['issued_epoch'] <= now, 'future_issue')
    nodes = issued['nodes']
    need(type(nodes) is list and len(nodes) == 72, 'node_inventory')
    expected = {(h, name, method) for h in labels.HORIZONS
                for name in labels.CONTINUOUS_LABELS for method in METHODS}
    need({(n['horizon_minutes'], n['label'], n['method']) for n in nodes} == expected,
         'node_identity_inventory')
    selections = {h: label_from_captures(origin, instrument, h, list(unique.values()),
                 as_of_epoch=now) for h in labels.HORIZONS}
    scored = []
    for node in nodes:
        h, name = node['horizon_minutes'], node['label']
        target = origin['price_epoch'] + h*60
        need(node['horizon_sec'] == h*60 and node['target_price_epoch'] == target
             and node['target_label_epoch'] == target-60
             and node['quantile_levels'] == list(QUANTILES), 'original_node_target_or_levels')
        result = deepcopy(selections[h][name])
        row = dict(horizon_minutes=h, label=name, method=node['method'],
            quantile_levels=list(QUANTILES), quantile_bps=deepcopy(node['quantile_bps']),
            label_outcome=result)
        row['metrics'] = _quantile_scores(result['value_bps'], node['quantile_bps']) \
            if result['status'] == 'scored' else None
        scored.append(row)
    completed = epoch(clock()); need(completed >= now, 'evaluation_completion_clock')
    return sealed(dict(schema_version=SCHEMA, instrument=instrument,
        cohort_id=issued['cohort_id'], issued_sha256=issued['issued_sha256'],
        original_issued_epoch=issued['issued_epoch'],
        original_consumed_epoch=consumption['read_completed_epoch'],
        publication_sha256=digest(publication), consumption_sha256=digest(consumption),
        source_bindings=deepcopy(expected_source_bindings),
        reference_label_epoch=origin['label_epoch'], reference_price_epoch=origin['price_epoch'],
        as_of_epoch=now, evaluation_completed_epoch=completed, nodes=scored,
        unique_capture_count=len(unique), status_counts=dict(Counter(r['label_outcome']['status'] for r in scored)),
        scope=['Original issued risk-distribution quantiles; no direction probability or calibration claim.',
            'Earliest decisive attested query per original label; no merged query domain, interpolation or changed target.',
            'The original reference candle is preserved; any later origin revision is recorded.',
            'Bid/ask path and envelope quantities are candle diagnostics, not executable fills, barrier ordering or position-manager efficacy.',
            'Overlapping horizons, labels, methods, origins and currency pairs are not independent trials.',
            'Forecast official-midpoint inputs differ in provenance from the historically unknown midpoint convention; no equivalence claim.'],
        calibrated_probability_claim=False, manager_efficacy_claim=False, **FLAGS), 'report_sha256')


def aggregate_reports(reports, *, as_of_epoch):
    """Common origin support, separate72-node metrics; repeated origins count once."""
    now = epoch(as_of_epoch)
    need(type(reports) in (list, tuple) and len(reports) <= 2048, 'report_count_bound')
    latest = {}; sources = None; cohort = None; issued_identities = {}
    for report in reports:
        need(type(report) is dict and report.get('schema_version') == SCHEMA
             and report.get('report_sha256') == digest({k:v for k,v in report.items()
                if k != 'report_sha256'}), 'report_schema_or_seal')
        need(all(report.get(k) is v for k,v in FLAGS.items()), 'report_authority')
        need(epoch(report['original_issued_epoch']) <= epoch(report['original_consumed_epoch'])
             <= epoch(report['as_of_epoch']) <= epoch(report['evaluation_completed_epoch']) <= now,
             'future_report')
        need(report['instrument'] in PAIRS and len(report['nodes']) == 72,
             'report_scope')
        if cohort is None: cohort = report['cohort_id']
        need(type(cohort) is str and report['cohort_id'] == cohort, 'mixed_cohorts')
        expected = {(h,name,method) for h in labels.HORIZONS
                    for name in labels.CONTINUOUS_LABELS for method in METHODS}
        need({(r['horizon_minutes'],r['label'],r['method']) for r in report['nodes']} == expected,
             'aggregate_node_inventory')
        for row in report['nodes']:
            outcome = row['label_outcome']
            target = report['reference_price_epoch'] + row['horizon_minutes']*60
            need(outcome['target_price_epoch'] == target and outcome['target_label_epoch'] == target-60
                 and row['quantile_levels'] == list(QUANTILES), 'aggregate_original_target')
            if outcome['status'] == 'scored':
                need(row['metrics'] == _quantile_scores(outcome['value_bps'],row['quantile_bps'])
                     and outcome['source'] is not None, 'aggregate_scored_metric_mismatch')
            else:
                need(outcome['status'] in ('pending','unavailable') and outcome['value_bps'] is None
                     and row['metrics'] is None, 'aggregate_missingness')
        if sources is None: sources = report['source_bindings']
        need(report['source_bindings'] == sources, 'report_source_closure_changed')
        key = report['issued_sha256']
        identity = digest(dict(header={name:report[name] for name in (
            'cohort_id','instrument','reference_label_epoch','reference_price_epoch',
            'original_issued_epoch','original_consumed_epoch','publication_sha256','consumption_sha256')},
            nodes=sorted([{name:r[name] for name in ('horizon_minutes','label','method',
                  'quantile_levels','quantile_bps')} for r in report['nodes']],
                key=lambda r:(r['horizon_minutes'],r['label'],r['method']))))
        need(key not in issued_identities or issued_identities[key] == identity,
             'same_issued_identity_redefined')
        issued_identities[key] = identity
        if key not in latest or (report['evaluation_completed_epoch'], report['report_sha256']) > (
                latest[key]['evaluation_completed_epoch'], latest[key]['report_sha256']):
            latest[key] = report
    # Earliest actual issued/consumed duplicate origin, never performance selection.
    origins = {}; duplicate_issues = []
    for report in sorted(latest.values(), key=lambda r:(r['original_issued_epoch'],
            r['original_consumed_epoch'], r['issued_sha256'])):
        key = (report['cohort_id'], report['instrument'], report['reference_price_epoch'])
        if key in origins:
            duplicate_issues.append(dict(issued_sha256=report['issued_sha256'],
                retained_issued_sha256=origins[key]['issued_sha256'],
                reason_code='repeated_same_pair_origin_not_additional_sample'))
        else: origins[key] = report
    groups = defaultdict(list); paired = defaultdict(list)
    for report in origins.values():
        per_label = defaultdict(dict)
        for row in report['nodes']:
            key = (report['instrument'], row['horizon_minutes'], row['label'], row['method'])
            groups[key].append(row)
            per_label[(report['instrument'], row['horizon_minutes'], row['label'])][row['method']] = row
        for key, methods in per_label.items():
            left, right = (methods[m] for m in METHODS)
            if left['metrics'] is None or right['metrics'] is None:
                continue
            need(left['label_outcome'] == right['label_outcome'], 'paired_label_or_source_mismatch')
            paired[key].append((left, right))
    output = []; comparisons = []
    def mean(values):
        return None if not values else str(sum((Decimal(str(v)) for v in values), Decimal(0))/len(values))
    with localcontext(Context(prec=96)):
        for key, rows in sorted(groups.items()):
            valid = [row for row in rows if row['metrics'] is not None]
            output.append(dict(instrument=key[0], horizon_minutes=key[1], label=key[2], method=key[3],
                original_origin_count=len(rows), scored_count=len(valid),
                status_counts=dict(Counter(row['label_outcome']['status'] for row in rows)),
                missing_reasons=dict(Counter(row['label_outcome']['reason_code'] for row in rows
                    if row['metrics'] is None)),
                mean_pinball_loss_bps=[mean([r['metrics']['pinball_loss_bps'][i] for r in valid]) for i in range(3)],
                mean_median_absolute_error_bps=mean([r['metrics']['median_absolute_error_bps'] for r in valid]),
                interval_80_coverage=mean([int(r['metrics']['interval_80_covered']) for r in valid]),
                mean_interval_width_bps=mean([r['metrics']['interval_width_bps'] for r in valid]),
                lower_miss_count=sum(r['metrics']['below_lower'] for r in valid),
                upper_miss_count=sum(r['metrics']['above_upper'] for r in valid)))
        for key, rows in sorted(paired.items()):
            comparisons.append(dict(instrument=key[0], horizon_minutes=key[1], label=key[2],
                matched_origin_count=len(rows), difference='volatility_scaled_minus_empirical',
                mean_pinball_delta_bps=[mean([Decimal(b['metrics']['pinball_loss_bps'][i])-
                    Decimal(a['metrics']['pinball_loss_bps'][i]) for a,b in rows]) for i in range(3)],
                mean_median_MAE_delta_bps=mean([Decimal(b['metrics']['median_absolute_error_bps'])-
                    Decimal(a['metrics']['median_absolute_error_bps']) for a,b in rows])))
    return sealed(dict(schema_version=SCHEMA+'_aggregate', as_of_epoch=now,
        cohort_id=cohort,
        input_report_count=len(reports), unique_issued_count=len(latest),
        unique_pair_origin_count=len(origins), independent_sample_size=None,
        repeated_origin_issues=duplicate_issues, groups=output, paired_methods=comparisons,
        selected_report_sha256=[r['report_sha256'] for r in origins.values()],
        source_bindings=sources, scope='Consistency summary of retained scored reports, not independent source authentication. Pair/origin count is distinct from method, horizon and label counts; overlapping observations are not independent trials. Unissued scheduled attempts require separate worker dispositions. No pooled cross-currency efficacy or calibrated probability claim.',
        **FLAGS), 'aggregate_sha256')
