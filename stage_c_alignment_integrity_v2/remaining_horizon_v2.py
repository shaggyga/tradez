"""Exact residual-horizon issuance using the reviewed fitted consumer.

Outputs remain gross-midpoint forecasts, not executable native policy packets.
No endpoint rebasing, horizon scaling or interpolation is performed.
"""
from contracts import fingerprint
from fitted_consumer_v2 import issue, integer


def issue_remaining(models, observation, *, decision_epoch, target_epoch,
                    available_epoch, procedure='adaptive'):
    for key, value in [('decision', decision_epoch), ('target', target_epoch),
                       ('available', available_epoch)]:
        integer(value, key)
    if available_epoch < decision_epoch:
        raise ValueError('prediction_available_before_decision')
    if procedure not in {'frozen', 'adaptive'}:
        raise ValueError('explicit_model_selection_required')
    if target_epoch <= available_epoch:
        return None, 'target_not_future_at_availability'
    remaining = target_epoch - decision_epoch
    eligible = []
    seen = set()
    for model in models:
        if model.get('status') != 'fitted' or model.get('control'):
            continue
        if model.get('model_id') != fingerprint({k:v for k,v in model.items() if k != 'model_id'}):
            raise ValueError('fitted_model_identity_mismatch')
        if model['model_id'] in seen:
            raise ValueError('duplicate_model_identity')
        seen.add(model['model_id'])
        if model['target']['horizon_seconds'] == remaining:
            eligible.append(model)
    if not eligible:
        return None, 'exact_remaining_horizon_model_unavailable'
    eligible.sort(key=lambda m:(m['training_view']['fit_cutoff_epoch'], m['model_id']))
    # A frozen procedure keeps the first scheduled fit even if a later fit is ready.
    if procedure == 'frozen':
        first_cutoff = eligible[0]['training_view']['fit_cutoff_epoch']
        eligible = [m for m in eligible if m['training_view']['fit_cutoff_epoch'] == first_cutoff]
    ready = [m for m in eligible if m['ready_epoch'] <= decision_epoch]
    if not ready:
        return None, 'model_not_ready'
    cutoff = max(m['training_view']['fit_cutoff_epoch'] for m in ready)
    chosen = [m for m in ready if m['training_view']['fit_cutoff_epoch'] == cutoff]
    if len(chosen) != 1:
        raise ValueError('ambiguous_same_cutoff_remaining_model')
    forecast, reason = issue(chosen[0], observation, decision_epoch=decision_epoch,
                             available_epoch=available_epoch, procedure=procedure)
    if forecast is None:
        return None, reason
    body = {'schema_version':'forex_exact_remaining_forecast.v1',
            'forecast':forecast, 'conditioning_epoch':decision_epoch,
            'original_target_epoch':target_epoch, 'remaining_seconds':remaining,
            'observation_sha256':fingerprint(observation),
            'target':chosen[0]['target'], 'procedure':procedure,
            'scope':'fresh_features_direct_gross_midpoint_model_development',
            'policy_admission':'blocked_execution_and_native_packet_contract_required'}
    body['remaining_forecast_id'] = fingerprint(body)
    return body, 'eligible'


def coverage(models, observations, *, universe, decision_epochs, target_epoch,
             prediction_latency_seconds=2):
    if len(universe) != 68 or len(set(universe)) != 68:
        raise ValueError('all68_required')
    integer(prediction_latency_seconds, 'prediction_latency')
    if not decision_epochs or decision_epochs != sorted(set(decision_epochs)):
        raise ValueError('ordered_unique_decisions_required')
    lookup = {}
    for row in observations:
        key = (row['instrument'], row['origin_epoch'])
        if row['instrument'] not in universe or key in lookup:
            raise ValueError('observation_population_mismatch')
        lookup[key] = row
    forecasts, rows = [], []
    for epoch in decision_epochs:
        for procedure in ('frozen', 'adaptive'):
            for pair in sorted(universe):
                forecast, reason = issue_remaining(models, lookup.get((pair, epoch)),
                    decision_epoch=epoch, target_epoch=target_epoch,
                    available_epoch=epoch+prediction_latency_seconds, procedure=procedure)
                rows.append({'instrument':pair, 'decision_epoch':epoch,
                    'original_target_epoch':target_epoch, 'remaining_seconds':target_epoch-epoch,
                    'procedure':procedure, 'reason':reason,
                    'remaining_forecast_id':forecast['remaining_forecast_id'] if forecast else None})
                if forecast:
                    forecasts.append(forecast)
    return {'forecasts':forecasts, 'coverage':rows,
            'policy_frames':0, 'status':'gross_remaining_horizon_development_only'}
