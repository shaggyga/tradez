"""Offline prerequisite inspection over the authenticated combined consumer.

Checks declared management scenarios; never supplies missing state or estimates,
selects a trading action, or upgrades retained forecasts to policy authority.
"""
from pathlib import Path
from collections import Counter
from decimal import Decimal, localcontext
import argparse
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
import forex_retained_combined_observation as combined

prior = combined.prior
SCHEMA = 'retained_management_prerequisites.v1'
SOURCES = combined.SOURCES + ['tools/forex_retained_management_contract.py']
FLAGS = dict(combined.forecasts.FLAGS, action_eligible=False, conditional_value_qualified=False)


def sources():
    return {n: prior.sha(prior.read(ROOT / n)) for n in SOURCES}


def exact(value, *, nonnegative=False):
    prior.need(type(value) is str, 'decimal_string_required')
    result = Decimal(value)
    prior.need(result.is_finite() and (not nonnegative or result >= 0), 'finite_economic_value_required')
    return result


def authenticate(record, records):
    prior.need(type(record) is str and record in records, 'missing_evidence_record')
    body = records[record]
    prior.need(prior.sha(prior.encoded(body)) == record, 'evidence_record_hash')
    return body


def check(context, request, observations, row, contract):
    """Consistency of supplied evidence, not scientific/economic qualification."""
    records = request['evidence_records']
    state = authenticate(contract['state_sha256'], records)
    decision = request['combined_request']['decision_epoch']
    terminal = combined.forecasts.native.epoch(contract['common_terminal_epoch'])
    prior.need(terminal > decision, 'terminal_elapsed')
    prior.need(state['status'] == 'OPEN' and state['pending_orders'] == [], 'settled_open_state_required')
    prior.need(state['state_asof_epoch'] == decision, 'current_position_state_required')
    prior.need(type(state['base_units']) is int and state['base_units'] > 0 and
               type(state['side']) is int and state['side'] in (-1, 1), 'position_size_side')
    prior.need(state['provenance'] == request['evidence_tier'] and
               state['account_currency'] == 'USD', 'state_tier_or_currency')
    for k in ('position_id', 'entry_thesis_sha256', 'entry_information_sha256', 'invalidation_rule'):
        prior.need(isinstance(state[k], str) and bool(state[k]), 'position_lineage_required')
    # Lineage documents are content addressed; an arbitrary locator is insufficient.
    authenticate(state['entry_thesis_sha256'], records)
    authenticate(state['entry_information_sha256'], records)
    for k in ('paid_costs_usd', 'mfe_usd', 'mae_usd', 'exposure_usd'):
        exact(state[k], nonnegative=True)
    wealth = exact(state['liquidation_wealth_usd'])
    prior.need(state['liquidation_convention'] == 'current_executable_liquidation_net_close_cost',
               'common_liquidation_baseline_required')
    incumbent = observations[contract['incumbent']]
    prior.need(incumbent['instrument'] == state['instrument'], 'incumbent_state_identity')
    prior.need(incumbent['status'] == row['status'] == 'priced_observation', 'priced_observations_required')
    for candidate in (incumbent, row):
        prior.need(candidate['requested_target_epoch'] == terminal, 'common_terminal_required')
    receipts = {r['receipt_sha256']: r for r in context.receipts.values()}
    anchor = receipts[state['entry_receipt_sha256']]
    anchor = context.validate(anchor)
    prior.need(anchor['forecast']['instrument'] == state['instrument'] and
               anchor['forecast']['target_epoch'] == terminal and
               anchor['original_observed_epoch'] <= state['entry_epoch'] <= decision, 'entry_receipt_lineage')
    fresh = receipts[incumbent['receipt_sha256']]
    prior.need(fresh['forecast']['reference_epoch'] > anchor['forecast']['reference_epoch'] and
               fresh['forecast']['input_hash'] != anchor['forecast']['input_hash'], 'fresh_conditional_input_required')
    # Reuse the original same-family/same-terminal consumer, not a rebased price.
    quotes = combined.quotes.map_receipts(request['combined_request']['quote_snapshot'],
        observed_epoch=request['combined_request']['quote_observed_epoch'], decision_epoch=decision,
        instruments=sorted(context.registry['pairs']))['quotes']
    current = context.candidate(fresh, incumbent['consumption'], decision_epoch=decision,
        target_epoch=terminal, quote=quotes[state['instrument']], anchor=anchor)
    prior.need(current['status'] == 'forecast_observation_available' and
               current['relation'] == 'same_terminal_updated_forecast_observation', 'fresh_conditional_input_required')
    risk = authenticate(contract['risk_sha256'], records)
    prior.need(risk['state_sha256'] == contract['state_sha256'] and risk['decision_epoch'] == decision,
               'risk_state_identity')
    prior.need(type(risk['hard_exit_required']) is bool, 'explicit_risk_veto_required')
    for k in ('maximum_notional_usd', 'available_margin_usd', 'required_margin_usd', 'maximum_loss_usd'):
        exact(risk[k], nonnegative=True)
    prior.need(exact(state['exposure_usd']) <= exact(risk['maximum_notional_usd']) and
               exact(risk['required_margin_usd']) <= exact(risk['available_margin_usd']), 'risk_capacity_refused')
    branches = contract['branches']
    prior.need(set(branches) == {'HOLD', 'EXIT', 'REPLACE'}, 'hold_cash_replace_controls_required')
    values = {}
    manager = prior.load_reference(ROOT / 'trad')
    for action, record in branches.items():
        branch = authenticate(record, records)
        prior.need(branch['action'] == action and branch['state_sha256'] == contract['state_sha256'] and
                   branch['common_terminal_epoch'] == terminal and branch['decision_epoch'] == decision and
                   exact(branch['baseline_wealth_usd']) == wealth, 'branch_common_baseline')
        prior.need(branch['continuation_policy'] and branch['economic_convention'] ==
                   'increment_from_current_liquidation_future_costs_once', 'continuation_economic_convention')
        prior.need(branch['sunk_costs_recharged'] is False and branch['spread_in_gross'] is True and
                   branch['slippage_convention'] == 'separate_future_cost', 'cost_double_count_refused')
        economic = branch['future_costs_usd']
        prior.need(set(economic) == {'fees', 'slippage', 'financing_debit', 'financing_credit'}, 'all_future_costs_required')
        costs = {k: exact(v, nonnegative=True) for k, v in economic.items()}
        prior.need(branch['financing_scope'] == 'through_common_terminal_including_rollover', 'financing_scope_required')
        authenticate(branch['economic_evidence_sha256'], records)
        if action == 'EXIT':
            prior.need(branch['gross_quote_increment'] == '0' and branch['quote_currency'] == 'USD' and
                       branch['continuation_policy'] == 'cash_through_terminal', 'cash_control_required')
        else:
            selected = incumbent if action == 'HOLD' else row
            c = selected['candidate']
            prior.need(branch['receipt_sha256'] == selected['receipt_sha256'] and
                       branch['input_hash'] == c['input_hash'] and
                       branch['model_definition_sha256'] == c['model_definition_sha256'] and
                       branch['quote_id'] == selected['quote_id'], 'branch_forecast_identity')
            reference = combined.forecasts.native.epoch(branch['value_reference_epoch'])
            available = combined.forecasts.native.epoch(branch['value_available_epoch'])
            prior.need(branch['value_method'] == 'fresh_conditional_continuation' and
                       reference <= available <= decision and
                       0 <= decision - reference <= 180 and
                       reference >= c['original_reference_epoch'], 'conditional_value_timing')
            prior.need(branch['quote_currency'] == selected['instrument'][4:], 'value_currency_identity')
            prior.need(type(branch['base_units']) is int and branch['base_units'] > 0 and
                       type(branch['side']) is int and branch['side'] in (-1, 1), 'branch_size_side')
            if action == 'HOLD':
                prior.need(branch['base_units'] == state['base_units'] and branch['side'] == state['side'], 'hold_position_identity')
            prior.need(exact(branch['notional_usd'], nonnegative=True) <= exact(risk['maximum_notional_usd']) and
                       exact(branch['required_margin_usd'], nonnegative=True) <= exact(risk['available_margin_usd']) and
                       exact(branch['worst_loss_usd'], nonnegative=True) <= exact(risk['maximum_loss_usd']), 'branch_risk_refused')
        gross = exact(branch['gross_quote_increment'])
        converted, conversion = manager.convert_pnl_to_usd(gross, branch['quote_currency'], quotes, decision, 30)
        net = converted - costs['fees'] - costs['slippage'] - costs['financing_debit'] + costs['financing_credit']
        values[action] = dict(net_usd=format(net, 'f'), conversion=manager.jsonable(conversion),
                              evidence_sha256=record)
    return dict(status='declared_contract_consistent', evidence_tier=request['evidence_tier'],
        common_terminal_epoch=terminal, state_sha256=contract['state_sha256'], values=values,
        switch_advantage_usd=format(Decimal(values['REPLACE']['net_usd']) - Decimal(values['HOLD']['net_usd']), 'f'),
        hard_exit_required=risk['hard_exit_required'], optional_rotation_permitted_by_risk=not risk['hard_exit_required'],
        scope='Contract consistency only; supplied conditional values and state are not independently qualified', **FLAGS)


def inspect(context, request):
    prior.need(request['schema'] == SCHEMA and request['source_bindings'] == sources(), 'management_source_identity')
    prior.need(request['evidence_tier'] in ('synthetic_fixture', 'preserved_observation'), 'explicit_evidence_tier')
    rows, report = combined.observe(context, request['combined_request'])
    lookup = {r['connection'] + '/' + r['instrument']: r for r in rows}
    prior.need(isinstance(request['contracts'], dict) and set(request['contracts']) <= set(lookup), 'contract_population')
    prior.need(isinstance(request['evidence_records'], dict) and len(request['evidence_records']) <= 50000, 'bounded_evidence_records')
    for pin in request['evidence_records']: authenticate(pin, request['evidence_records'])
    result = []
    for key, row in lookup.items():
        contract = request['contracts'].get(key)
        outcome = dict(status='prerequisites_missing', reason='position_conditional_economic_contract_missing', **FLAGS)
        if contract is not None:
            try:
                with localcontext() as ctx:
                    ctx.prec = 80
                    outcome = check(context, request, lookup, row, contract)
            except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
                outcome = dict(status='prerequisites_refused', reason=type(exc).__name__ + ':' + str(exc), **FLAGS)
        result.append(dict(slot=key, observation_status=row['status'], **outcome))
    return result, dict(schema=SCHEMA, population=len(result), statuses=dict(Counter(r['status'] for r in result)),
        combined_report=report, evidence_tier=request['evidence_tier'], current_live_observation=False,
        scope='Offline prerequisite audit; no policy selection, execution or forecast qualification', **FLAGS)


def run(path, request_path, expected, output, run_id, *, resume=False, max_new=None):
    prior.need(max_new is None or type(max_new) is int and max_new > 0, 'positive_chunk_limit')
    raw = prior.read(Path(request_path)); prior.need(prior.sha(raw) == expected, 'request_external_hash')
    request = json.loads(raw)
    context = combined.forecasts.VerifiedCapture.load(path, request['combined_request']['capture_manifest_sha256'])
    rows, report = inspect(context, request)
    chunks = [rows[i:i+32] for i in range(0, len(rows), 32)]
    required = {f'rows_{i:04d}.json' for i in range(len(chunks))} | {'REPORT.json'}
    identity = prior.effective_run_identity(contract={'schema': SCHEMA, 'request_sha256': expected,
        'population': len(rows), 'required_payloads': sorted(required)}, dependency_hashes=sources())
    pub = prior.RunPublisher(output, run_id, identity)
    if (pub.root / 'COMPLETION_MANIFEST.json').exists():
        prior.verify_completed_run(pub.root, identity); return {'status': 'verified_completed'}
    pub.acquire(recover=resume); completed = False; payloads = []; added = 0
    try:
        for i, chunk in enumerate(chunks):
            name = f'rows_{i:04d}.json'
            if pub.read_verified_payload(name) is None:
                if max_new is not None and added >= max_new: return {'status': 'checkpointed', 'next_row': i*32}
                added += 1
            payloads.append(pub.write_or_validate_payload(name, prior.encoded(chunk)))
        payloads.append(pub.write_or_validate_payload('REPORT.json', prior.encoded(report)))
        pub.complete(payloads, required); completed = True
    finally:
        if not completed: pub.release()
    return {'status': 'completed', **report}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=Path, required=True); p.add_argument('--request', type=Path, required=True)
    p.add_argument('--sha256', required=True); p.add_argument('--output', type=Path, required=True)
    p.add_argument('--run-id', default='management'); p.add_argument('--resume', action='store_true')
    p.add_argument('--max-new', type=int)
    a = p.parse_args()
    print(json.dumps(run(a.input, a.request, a.sha256, a.output, a.run_id, resume=a.resume, max_new=a.max_new)))
