"""Pure matched management replay with explicit USD valuation. No runtime I/O.

The legacy momentum selector is reused unchanged as one named reference.
Two matched arms apply the same USD hold/exit/switch rule to momentum and curve
inputs. Prices, currencies, source availability and scheduled clocks are supplied
by a caller; this module neither infers historical publication nor obtains data.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal, Context, ROUND_HALF_EVEN, ROUND_FLOOR, localcontext
import hashlib
import json
import math
import re
from typing import Any, Mapping, Sequence

from src.forex_system.research import sequential_portfolio_replay_v1 as legacy

SCHEMA = 'curve_management_replay_v1_20260909'
CONFIG_SCHEMA = 'curve_management_replay_contract_v1_20260909'
ARMS = ('legacy_momentum_reference', 'usd_momentum_manager', 'usd_curve_manager',
        'no_trade', 'curve_hold_no_rotation')
SAFETY = {'research_only': True, 'can_place_orders': False, 'can_promote': False,
          'can_authorize': False, 'account_access': False, 'broker_access': False,
          'proof_eligible': False, 'signal_feed_write': False, 'lifecycle_write': False}
LEGACY_SOURCE_SHA256 = 'e1201fdd3387825c9295f13045aeb00e2f87cce9ff819e789faf922ea2e514a6'
CTX = Context(prec=50, rounding=ROUND_HALF_EVEN)
PAIR = re.compile(r'[A-Z]{3}_[A-Z]{3}\Z')


def number(value: Any, name: str = 'numeric') -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError('invalid_' + name)
    try:
        result = Decimal(str(value))
    except Exception as exc:
        raise ValueError('invalid_' + name) from exc
    if not result.is_finite():
        raise ValueError('nonfinite_' + name)
    return result


def clock(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError('invalid_clock')
    return float(value)


def jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, Mapping):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def digest(value: Any) -> str:
    raw = json.dumps(jsonable(value), sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def pair_name(value: Any) -> str:
    if not isinstance(value, str) or not PAIR.fullmatch(value) or value[:3] == value[4:]:
        raise ValueError('invalid_instrument')
    return value


def validate_metadata(metadata: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    if not isinstance(metadata, Mapping) or not 1 <= len(metadata) <= 68:
        raise ValueError('bounded_instrument_metadata_required')
    result = {}
    for pair, raw in metadata.items():
        pair_name(pair)
        if raw.get('base_currency') != pair[:3] or raw.get('quote_currency') != pair[4:]:
            raise ValueError('metadata_currency_mismatch')
        pip = number(raw.get('pip_size'), 'pip_size')
        if pip not in {Decimal('.0001'), Decimal('.001'), Decimal('.01')}:
            raise ValueError('unsupported_explicit_pip_size')
        if raw.get('unit_increment') != 1 or type(raw.get('unit_increment')) is not int:
            raise ValueError('integer_base_unit_contract_required')
        result[pair] = {**dict(raw), 'pip_size': pip}
    return result


def validate_config(config: Mapping[str, Any]) -> dict[str, Any]:
    if config.get('schema_version') != CONFIG_SCHEMA or any(config.get(k) is not v for k, v in SAFETY.items()):
        raise ValueError('explicit_inert_research_contract_required')
    if config.get('account_currency') != 'USD' or config.get('maximum_open_positions') != 1 or type(config.get('maximum_open_positions')) is not int:
        raise ValueError('one_position_usd_contract_required')
    if config.get('conversion_policy') != 'direct_or_inverse_executable_bid_ask_no_triangulation':
        raise ValueError('explicit_conversion_policy_required')
    if config.get('sizing_policy') != 'fixed_usd_notional_integer_base_units_at_decision':
        raise ValueError('explicit_sizing_policy_required')
    if config.get('curve_target_window_policy') not in ('exact_only', 'nominal_management_boundary'):
        raise ValueError('explicit_curve_target_window_policy_required')
    result = deepcopy(dict(config));result['metadata'] = validate_metadata(config['metadata'])
    for key in ('notional_usd', 'maximum_entry_spread_bps', 'maximum_holding_sec', 'cadence_sec',
                'execution_delay_sec', 'quote_max_age_sec', 'minimum_entry_cost_ratio'):
        result[key] = number(config.get(key), key)
        if result[key] <= 0:
            raise ValueError('positive_' + key + '_required')
    for key in ('slippage_bps_per_leg', 'switch_incremental_hurdle_usd'):
        result[key] = number(config.get(key), key)
        if result[key] < 0:
            raise ValueError('nonnegative_' + key + '_required')
    if result['execution_delay_sec'] >= result['cadence_sec'] or result['quote_max_age_sec'] > 60:
        raise ValueError('bounded_quote_and_execution_clock_required')
    if not isinstance(config.get('legacy_policy'), Mapping):
        raise ValueError('explicit_legacy_policy_required')
    return result


def quote_at(quotes: Mapping[str, Any], instrument: str, cutoff_epoch: float,
             maximum_age_sec: Any, *, observed_epoch: float | None = None) -> dict[str, Any]:
    pair_name(instrument);cutoff = clock(cutoff_epoch)
    observed = cutoff if observed_epoch is None else clock(observed_epoch)
    if observed < cutoff:
        raise ValueError('observation_before_quote_cutoff')
    raw = quotes.get(instrument)
    if not isinstance(raw, Mapping):
        raise ValueError('missing_quote:' + instrument)
    if raw.get('instrument') != instrument or not isinstance(raw.get('quote_id'), str) or not raw['quote_id']:
        raise ValueError('quote_identity_mismatch:' + instrument)
    if raw.get('tradeable') is not True:
        raise ValueError('quote_not_explicitly_tradeable:' + instrument)
    # Exact decimal quote endpoints must not pass through a binary float first.
    if type(raw.get('bid')) not in (str, int) or type(raw.get('ask')) not in (str, int):
        raise ValueError('exact_decimal_quote_required:' + instrument)
    bid, ask = number(raw['bid'], 'bid'), number(raw['ask'], 'ask')
    market, available = clock(raw.get('market_epoch')), clock(raw.get('available_epoch'))
    if not 0 < bid <= ask:
        raise ValueError('invalid_bid_ask:' + instrument)
    if not market <= cutoff or not market <= available <= observed or cutoff - market > float(number(maximum_age_sec)):
        raise ValueError('stale_or_future_quote:' + instrument)
    return {**dict(raw), 'bid': bid, 'ask': ask, 'market_epoch': market,
            'available_epoch': available, 'mid': (bid + ask) / 2}


def usd_rates(currency: str, quotes: Mapping[str, Any], cutoff_epoch: float,
              maximum_age_sec: Any, *, observed_epoch: float | None = None) -> dict[str, Any]:
    """USD proceeds for selling currency and USD cost for buying it, no chain."""
    if currency == 'USD':
        return {'sell_currency_usd': Decimal(1), 'buy_currency_usd': Decimal(1),
                'currency': 'USD', 'quote_id': 'USD_identity', 'instrument': None}
    if not isinstance(currency, str) or not re.fullmatch(r'[A-Z]{3}', currency):
        raise ValueError('invalid_conversion_currency')
    direct, inverse = currency + '_USD', 'USD_' + currency
    # Prefer direct deterministically. An invalid supplied direct quote is not
    # silently bypassed using another route; its provenance remains a failure.
    if direct in quotes:
        q = quote_at(quotes, direct, cutoff_epoch, maximum_age_sec, observed_epoch=observed_epoch)
        sell, buy = q['bid'], q['ask']
    elif inverse in quotes:
        q = quote_at(quotes, inverse, cutoff_epoch, maximum_age_sec, observed_epoch=observed_epoch)
        sell, buy = Decimal(1) / q['ask'], Decimal(1) / q['bid']
    else:
        raise ValueError('missing_usd_conversion:' + currency)
    return {'sell_currency_usd': sell, 'buy_currency_usd': buy, 'currency': currency,
            'quote_id': q['quote_id'], 'instrument': q['instrument'],
            'market_epoch': q['market_epoch'], 'available_epoch': q['available_epoch']}


def convert_pnl_to_usd(amount: Any, currency: str, quotes: Mapping[str, Any],
                       cutoff_epoch: float, maximum_age_sec: Any, *, observed_epoch: float | None = None) -> tuple[Decimal, dict[str, Any]]:
    amount = number(amount, 'currency_amount')
    rates = usd_rates(currency, quotes, cutoff_epoch, maximum_age_sec, observed_epoch=observed_epoch)
    rate = rates['sell_currency_usd'] if amount >= 0 else rates['buy_currency_usd']
    return amount * rate, {**rates, 'applied_rate': rate,
                           'conversion_side': 'sell_profit' if amount >= 0 else 'buy_loss'}


def size_at_decision(instrument: str, quotes: Mapping[str, Any], decision_epoch: float,
                     config: Mapping[str, Any]) -> tuple[int, dict[str, Any]]:
    meta = config['metadata'][instrument]
    rate = usd_rates(meta['base_currency'], quotes, decision_epoch, config['quote_max_age_sec'])
    units = int((number(config['notional_usd']) / rate['buy_currency_usd']).to_integral_value(rounding=ROUND_FLOOR))
    if units < 1:
        raise ValueError('notional_below_one_base_unit')
    return units, {'policy': config['sizing_policy'], 'decision_epoch': decision_epoch,
                   'notional_cap_usd': number(config['notional_usd']), 'base_units': units,
                   'base_currency_conversion': rate,
                   'decision_value_usd': units * rate['buy_currency_usd']}


def flat_state() -> dict[str, Any]:
    return {'realized_usd': Decimal(0), 'position': None}


def _executed_price(q: Mapping[str, Any], side: int, opening: bool,
                    config: Mapping[str, Any]) -> tuple[Decimal, Decimal]:
    raw = q['ask'] if (side > 0) == opening else q['bid']
    slip = q['mid'] * number(config['slippage_bps_per_leg']) / 10000
    sign = side if opening else -side
    price = raw + sign * slip
    if price <= 0:
        raise ValueError('nonpositive_executed_price')
    return price, slip


def apply_action(state: Mapping[str, Any], decision: Mapping[str, Any],
                 quotes: Mapping[str, Any], execution_epoch: float,
                 config: Mapping[str, Any], *, observed_epoch: float | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Every public action has a precommitted decision and exact delay."""
    try:
        decided, executed = clock(decision.get('decision_epoch')), clock(execution_epoch)
        if executed != decided + float(config['execution_delay_sec']) or executed <= decided:
            raise ValueError('execution_clock_differs_from_precommitted_latency')
        observed = executed if observed_epoch is None else clock(observed_epoch)
        if observed < executed:
            raise ValueError('execution_evidence_observed_before_execution')
        if decision.get('action') in ('enter', 'rotate'):
            target = clock(decision.get('original_target_epoch'))
            if not executed < target <= executed + float(config['maximum_holding_sec']):
                raise ValueError('entry_native_target_elapsed_or_beyond_holding_limit')
        if decision.get('action') == 'hold' and state.get('position') is not None:
            position = state['position']
            deadline = min(clock(position['original_target_epoch']),
                           clock(position['entry_epoch']) + float(config['maximum_holding_sec']))
            if executed >= deadline:
                raise ValueError('hold_reaches_common_native_deadline')
    except (ValueError, KeyError, TypeError) as exc:
        return deepcopy(dict(state)), {'status': 'rejected', 'reason': str(exc), 'legs': [], 'realized_delta_usd': Decimal(0)}
    return _apply_action(state, decision, quotes, executed, config, observed_epoch=observed)


def _apply_action(state: Mapping[str, Any], decision: Mapping[str, Any],
                  quotes: Mapping[str, Any], execution_epoch: float,
                  config: Mapping[str, Any], *, observed_epoch: float) -> tuple[dict[str, Any], dict[str, Any]]:
    """Atomic research close/open. Failure leaves both portfolio and cash intact."""
    before = deepcopy(dict(state));action = decision.get('action')
    if action not in legacy.ALLOWED_ACTIONS:
        raise ValueError('invalid_action')
    if action in {'wait', 'hold'}:
        if (action == 'wait') != (before.get('position') is None):
            raise ValueError('action_position_mismatch')
        return before, {'status': 'applied', 'legs': [], 'realized_delta_usd': Decimal(0)}
    try:
        after = deepcopy(before);legs = []
        if action in {'enter', 'rotate'}:
            pair = pair_name(decision['instrument'])
            if pair not in config['metadata'] or decision['side'] not in (-1, 1) or type(decision['side']) is not int:
                raise ValueError('invalid_open_identity')
            if type(decision.get('base_units')) is not int or decision['base_units'] < 1:
                raise ValueError('decision_time_integer_units_required')
            if clock(decision['decision_epoch']) >= clock(execution_epoch):
                raise ValueError('execution_must_follow_decision')
            qnew = quote_at(quotes, pair, execution_epoch, config['quote_max_age_sec'], observed_epoch=observed_epoch)
            if qnew['market_epoch'] != execution_epoch:
                raise ValueError('missing_exact_execution_quote:' + pair)
            if (qnew['ask'] - qnew['bid']) / qnew['mid'] * 10000 > number(config['maximum_entry_spread_bps']):
                raise ValueError('execution_spread_above_limit')
            usd_rates(config['metadata'][pair]['quote_currency'], quotes, execution_epoch, config['quote_max_age_sec'], observed_epoch=observed_epoch)
        if action in {'exit', 'rotate'}:
            position = after.get('position')
            if position is None:
                raise ValueError('cannot_close_flat')
            oldpair = position['instrument'];meta = config['metadata'][oldpair]
            q = quote_at(quotes, oldpair, execution_epoch, config['quote_max_age_sec'], observed_epoch=observed_epoch)
            if q['market_epoch'] != execution_epoch:
                raise ValueError('missing_exact_execution_quote:' + oldpair)
            exit_price, slip = _executed_price(q, position['side'], False, config)
            quote_pnl = position['side'] * position['base_units'] * (exit_price - number(position['entry_price']))
            usd, conversion = convert_pnl_to_usd(quote_pnl, meta['quote_currency'], quotes,
                                                 execution_epoch, config['quote_max_age_sec'], observed_epoch=observed_epoch)
            legs.append({'kind': 'close', 'instrument': oldpair, 'side': position['side'],
                         'base_units': position['base_units'], 'quote_id': q['quote_id'],
                         'market_epoch': q['market_epoch'], 'available_epoch': q['available_epoch'],
                         'execution_epoch': execution_epoch, 'observed_epoch': observed_epoch, 'executed_price': exit_price,
                         'slippage_price': slip, 'quote_currency_pnl': quote_pnl, 'realized_usd': usd,
                         'conversion': conversion, 'original_entry': position})
            after['realized_usd'] = number(after['realized_usd']) + usd;after['position'] = None
        if action in {'enter', 'rotate'}:
            if after.get('position') is not None:
                raise ValueError('cannot_open_while_positioned')
            entry, slip = _executed_price(qnew, decision['side'], True, config)
            position = {'instrument': pair, 'side': decision['side'], 'base_units': decision['base_units'],
                        'entry_epoch': execution_epoch, 'entry_price': entry, 'entry_quote_id': qnew['quote_id'],
                        'entry_decision_epoch': decision['decision_epoch'], 'sizing': decision['sizing'],
                        'candidate_id': decision.get('candidate_id'),
                        'original_target_epoch': decision['original_target_epoch']}
            after['position'] = position
            legs.append({'kind': 'open', 'instrument': pair, 'side': decision['side'],
                         'base_units': decision['base_units'], 'quote_id': qnew['quote_id'],
                         'market_epoch': qnew['market_epoch'], 'available_epoch': qnew['available_epoch'],
                         'execution_epoch': execution_epoch, 'observed_epoch': observed_epoch, 'executed_price': entry,
                         'slippage_price': slip, 'realized_usd': Decimal(0), 'position': position})
        return after, {'status': 'applied', 'legs': legs,
                       'realized_delta_usd': number(after['realized_usd']) - number(before['realized_usd'])}
    except (ValueError, KeyError, TypeError) as exc:
        return before, {'status': 'rejected', 'reason': str(exc), 'legs': [], 'realized_delta_usd': Decimal(0)}


def liquidation_equity(state: Mapping[str, Any], quotes: Mapping[str, Any], epoch: float,
                       config: Mapping[str, Any], *, observed_epoch: float | None = None) -> dict[str, Any]:
    if state.get('position') is None:
        return {'status': 'available_flat', 'equity_usd': number(state['realized_usd'])}
    observation = clock(epoch) if observed_epoch is None else clock(observed_epoch)
    if observation < epoch:
        return {'status': 'unavailable', 'reason': 'mark_observation_before_market_time', 'equity_usd': None}
    # A mark is an explicitly hypothetical liquidation, not a fabricated action
    # with a guessed earlier decision clock or an execution instruction.
    after, receipt = _apply_action(state, {'action': 'exit'}, quotes, epoch, config, observed_epoch=observation)
    if receipt['status'] != 'applied':
        return {'status': 'unavailable', 'reason': receipt['reason'], 'equity_usd': None}
    return {'status': 'available', 'equity_usd': after['realized_usd'], 'hypothetical_liquidation': receipt}


def _require_sha(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{64}', value):
        raise ValueError('sha256_binding_required')
    return value


def prepare_candidates(rows: Sequence[Mapping[str, Any]], kind: str,
                       quotes: Mapping[str, Any], decision_epoch: float,
                       management_target_epoch: float, config: Mapping[str, Any]) -> tuple[list[dict], list[dict]]:
    """Admit native-target point-in-time inputs, retaining refusals as refusals."""
    if kind not in ('curve', 'momentum') or not isinstance(rows, (list, tuple)) or len(rows) > 68:
        raise ValueError('bounded_named_candidate_set_required')
    accepted, rejected = [], []
    seen = CounterIdentity(rows)
    for raw in rows:
        try:
            if not isinstance(raw, Mapping):
                raise ValueError('candidate_object_required')
            pair = pair_name(raw.get('instrument'))
            if seen[pair] != 1:
                raise ValueError('ambiguous_duplicate_instrument_candidate')
            meta = config['metadata'][pair]
            if clock(raw.get('decision_epoch')) != decision_epoch:
                raise ValueError('candidate_decision_clock_mismatch')
            available = clock(raw.get('available_epoch'))
            if available > decision_epoch:
                raise ValueError('candidate_not_available_at_decision')
            target = clock(raw.get('original_target_epoch'))
            if target != management_target_epoch or target <= decision_epoch:
                raise ValueError('incomparable_or_elapsed_native_target')
            if target <= decision_epoch + float(config['execution_delay_sec']):
                raise ValueError('native_target_elapsed_before_planned_entry')
            if target > decision_epoch + float(config['execution_delay_sec']) + float(config['maximum_holding_sec']):
                raise ValueError('native_target_beyond_maximum_holding_deadline')
            side = raw.get('side')
            if type(side) is not int or side not in ((-1, 0, 1) if kind == 'curve' else (-1, 1)):
                raise ValueError('directional_candidate_required')
            q = quote_at(quotes, pair, decision_epoch, config['quote_max_age_sec'])
            units, sizing = size_at_decision(pair, quotes, decision_epoch, config)
            conversion = usd_rates(meta['quote_currency'], quotes, decision_epoch, config['quote_max_age_sec'])
            if kind == 'curve':
                if raw.get('target_window_policy') != config['curve_target_window_policy']:
                    raise ValueError('curve_target_window_policy_mismatch')
                selection = raw.get('target_selection_policy')
                if not isinstance(selection, Mapping) or set(selection) != {'kind', 'maximum_delay_sec'}:
                    raise ValueError('curve_target_selection_policy_required')
                delay = selection['maximum_delay_sec']
                if type(delay) is not int or not 0 <= delay <= 60:
                    raise ValueError('curve_target_selection_delay_invalid')
                exact = delay == 0
                if selection['kind'] != ('exact_price_epoch' if exact else 'first_complete_bar_at_or_after_nominal'):
                    raise ValueError('curve_target_selection_policy_mismatch')
                if raw.get('target_is_exact') is not exact or clock(raw.get('target_price_window_end_epoch')) != target + delay:
                    raise ValueError('curve_target_window_clock_mismatch')
                if not exact and config['curve_target_window_policy'] == 'exact_only':
                    raise ValueError('nonexact_model_target_requires_explicit_policy')
                for key in ('curve_id', 'node_id', 'model_id', 'forecast_cohort'):
                    if not isinstance(raw.get(key), str) or not raw[key]:
                        raise ValueError('curve_identity_required:' + key)
                _require_sha(raw.get('curve_sha256'))
                sources = raw.get('source_bindings')
                if not isinstance(sources, Mapping) or not sources or len(sources) > 64:
                    raise ValueError('curve_source_bindings_required')
                for value in sources.values():
                    _require_sha(value)
                bound_quote = raw.get('decision_quote')
                if bound_quote is not None:
                    if not isinstance(bound_quote, Mapping):
                        raise ValueError('curve_decision_quote_binding_mismatch')
                    if raw.get('decision_quote_sha256') != digest(bound_quote):
                        raise ValueError('curve_decision_quote_hash_mismatch')
                    for key in ('instrument', 'quote_id', 'market_epoch', 'available_epoch', 'tradeable'):
                        if bound_quote.get(key) != q.get(key):
                            raise ValueError('curve_decision_quote_binding_mismatch')
                    if number(bound_quote.get('bid')) != q['bid'] or number(bound_quote.get('ask')) != q['ask']:
                        raise ValueError('curve_decision_quote_binding_mismatch')
                reference, issued = clock(raw.get('reference_epoch')), clock(raw.get('issued_epoch'))
                if not reference <= issued <= available <= decision_epoch < target:
                    raise ValueError('curve_clock_order')
                if number(raw.get('pip_size')) != meta['pip_size']:
                    raise ValueError('curve_pip_metadata_mismatch')
                if number(raw.get('remaining_sec')) != number(target) - number(decision_epoch):
                    raise ValueError('remaining_clock_mismatch')
                terminal = number(raw.get('expected_terminal_price'))
                change = number(raw.get('expected_remaining_price_change'))
                pips = number(raw.get('expected_remaining_move_pips'))
                tolerance = Decimal('1e-12') * max(Decimal(1), abs(terminal), abs(q['mid']))
                if abs(terminal - q['mid'] - change) > tolerance or abs(pips * meta['pip_size'] - change) > tolerance:
                    raise ValueError('curve_remaining_price_basis_mismatch')
                if terminal <= 0 or (side == 0 and change != 0) or (side != 0 and side * change <= 0):
                    raise ValueError('curve_side_or_terminal_mismatch')
                candidate_id = raw['node_id']
                probability = raw.get('probability_up')
                if probability is not None and not Decimal(0) <= number(probability) <= Decimal(1):
                    raise ValueError('invalid_retained_probability')
                confidence = None  # Original probability is retained, never rebased.
                legacy_score = None
            else:
                magnitude = number(raw.get('expected_move_pips'))
                if magnitude <= 0:
                    raise ValueError('positive_momentum_magnitude_required')
                change = side * magnitude * meta['pip_size']
                terminal = q['mid'] + change
                candidate_id = raw.get('snapshot_id')
                if not isinstance(candidate_id, str) or not candidate_id:
                    raise ValueError('momentum_snapshot_required')
                confidence = number(raw.get('confidence'))
                legacy_score = number(raw.get('score'))
                if not Decimal(0) <= confidence <= Decimal(1) or legacy_score < 0:
                    raise ValueError('invalid_momentum_selector_fields')
            slip = q['mid'] * number(config['slippage_bps_per_leg']) / 10000
            # Entry+exit costs are measured prospectively from the current mid.
            gross_quote = units * side * change
            expected_usd = gross_quote * conversion['sell_currency_usd']
            cost_usd = units * (q['ask'] - q['bid'] + 2 * slip) * conversion['buy_currency_usd']
            if cost_usd <= 0:
                raise ValueError('positive_prospective_cost_required')
            accepted.append({'instrument': pair, 'side': side, 'candidate_id': candidate_id, 'kind': kind,
                             'decision_epoch': decision_epoch, 'available_epoch': available,
                             'original_target_epoch': target, 'signed_price_change': change,
                             'expected_terminal_price': terminal, 'current_mid': q['mid'],
                             'base_units': units, 'sizing': sizing, 'quote_id': q['quote_id'],
                             'expected_gross_usd': expected_usd, 'round_trip_cost_usd': cost_usd,
                             'new_entry_net_usd': expected_usd - cost_usd,
                             'score': expected_usd / cost_usd, 'confidence': confidence,
                             'legacy_score': legacy_score, 'spread_pips': (q['ask'] - q['bid']) / meta['pip_size'],
                             'source_payload': deepcopy(dict(raw))})
        except (ValueError, KeyError, TypeError) as exc:
            rejected.append({'instrument': raw.get('instrument') if isinstance(raw, Mapping) else None,
                             'kind': kind, 'reason': str(exc), 'source_payload': deepcopy(raw)})
    return sorted(accepted, key=lambda r: (-r['new_entry_net_usd'], r['instrument'], -r['side'])), rejected


def CounterIdentity(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get('instrument') if isinstance(row, Mapping) else None
        if isinstance(value, str):
            counts[value] = counts.get(value, 0) + 1
    return counts


def _empty_decision(action: str, epoch: float, reason: str) -> dict[str, Any]:
    return {'action': action, 'decision_epoch': epoch, 'reason': reason, 'base_units': 0}


def _candidate_decision(candidate: Mapping[str, Any], action: str, reason: str) -> dict[str, Any]:
    return {'action': action, 'instrument': candidate['instrument'], 'side': candidate['side'],
            'decision_epoch': candidate['decision_epoch'], 'base_units': candidate['base_units'],
            'sizing': deepcopy(candidate['sizing']), 'original_target_epoch': candidate['original_target_epoch'],
            'candidate_id': candidate['candidate_id'], 'candidate_payload_sha256': digest(candidate['source_payload']),
            'reason': reason}


def _candidate_evidence(candidate: Mapping[str, Any]) -> dict[str, Any]:
    raw = candidate['source_payload']
    keys = ('scope', 'curve_id', 'curve_sha256', 'node_id', 'node_sha256', 'model_id', 'model_sha256',
            'forecast_cohort', 'reference_epoch', 'reference_label_epoch', 'issued_epoch', 'available_epoch',
            'original_target_epoch', 'target_label_epoch', 'horizon_sec', 'remaining_sec',
            'target_selection_policy', 'target_window_policy', 'target_price_window_end_epoch', 'target_is_exact',
            'probability_up', 'probability_scope', 'original_probability_up', 'original_probability_scope',
            'original_probability_event', 'probability_is_remaining_move_probability',
            'source_bindings', 'publication_sha256', 'consumption_sha256', 'decision_quote_sha256', 'snapshot_id')
    return {'instrument': candidate['instrument'], 'side': candidate['side'], 'kind': candidate['kind'],
            'candidate_id': candidate['candidate_id'], 'source_payload_sha256': digest(raw),
            'signed_price_change': candidate['signed_price_change'],
            'expected_terminal_price': candidate['expected_terminal_price'],
            'new_entry_net_usd': candidate['new_entry_net_usd'], 'round_trip_cost_usd': candidate['round_trip_cost_usd'],
            'original_evidence': {key: deepcopy(raw[key]) for key in keys if key in raw}}


def choose_usd_action(state: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]],
                      quotes: Mapping[str, Any], epoch: float, config: Mapping[str, Any],
                      *, terminal: bool = False, hold_only: bool = False) -> tuple[dict, dict]:
    """Same explicit dollar-valued selector for momentum and curve inputs."""
    position = state.get('position')
    if terminal:
        return _empty_decision('exit' if position else 'wait', epoch, 'predeclared_terminal'), {}
    eligible = [r for r in candidates if r['score'] >= number(config['minimum_entry_cost_ratio'])]
    best = eligible[0] if eligible else None
    if position is None:
        if best is None:
            return _empty_decision('wait', epoch, 'no_candidate_clears_cost_hurdle'), {}
        return _candidate_decision(best, 'enter', 'highest_expected_net_usd'), {}
    deadline = min(float(position['entry_epoch']) + float(config['maximum_holding_sec']),
                   float(position['original_target_epoch']))
    if epoch + float(config['execution_delay_sec']) >= deadline:
        return _empty_decision('exit', epoch, 'predeclared_holding_or_native_target_deadline'), {'position_deadline': deadline}
    if hold_only:
        return _empty_decision('hold', epoch, 'hold_current_no_rotation'), {'position_deadline': deadline}
    current = next((c for c in candidates if c['instrument'] == position['instrument']), None)
    if current is None:
        return _empty_decision('exit', epoch, 'incumbent_estimate_unavailable'), {'incumbent_estimate_status': 'unavailable'}
    if current['original_target_epoch'] != deadline:
        return _empty_decision('exit', epoch, 'incumbent_native_target_incomparable'), {
            'incumbent_estimate_status': 'incomparable_deadline', 'position_deadline': deadline,
            'candidate_native_target': current['original_target_epoch']}
    try:
        pair = position['instrument'];meta = config['metadata'][pair]
        q = quote_at(quotes, pair, epoch, config['quote_max_age_sec'])
        conversion = usd_rates(meta['quote_currency'], quotes, epoch, config['quote_max_age_sec'])
        units = position['base_units']
        expected_quote = units * position['side'] * current['signed_price_change']
        rate = conversion['sell_currency_usd'] if expected_quote >= 0 else conversion['buy_currency_usd']
        gross = expected_quote * rate
        slip = q['mid'] * number(config['slippage_bps_per_leg']) / 10000
        liquidation_cost = units * ((q['ask'] - q['bid']) / 2 + slip) * conversion['buy_currency_usd']
        hold_value, exit_value = gross - liquidation_cost, -liquidation_cost
        alternatives = [c for c in eligible if c['instrument'] != pair or c['side'] != position['side']]
        best = alternatives[0] if alternatives else None
        switch_value = best['new_entry_net_usd'] - liquidation_cost if best else None
        diagnostics = {'incumbent_estimate_status': 'available_independent_of_new_entry_hurdle',
                       'hold_value_usd': hold_value, 'exit_value_usd': exit_value,
                       'switch_value_usd': switch_value, 'incumbent_quote_id': q['quote_id'],
                       'incumbent_original_side': position['side'], 'forecast_side': current['side']}
        if best and switch_value > max(hold_value + number(config['switch_incremental_hurdle_usd']), exit_value):
            return _candidate_decision(best, 'rotate', 'switch_exceeds_hold_and_exit_in_usd'), diagnostics
        return _empty_decision('hold' if hold_value >= exit_value else 'exit', epoch,
                               'hold_dominates_in_usd' if hold_value >= exit_value else 'exit_dominates_in_usd'), diagnostics
    except (ValueError, KeyError, TypeError) as exc:
        return _empty_decision('exit', epoch, 'incumbent_valuation_unavailable'), {'reason': str(exc)}


def choose_legacy_action(state: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]],
                         epoch: float, config: Mapping[str, Any], *, terminal: bool) -> tuple[dict, dict]:
    """Unchanged legacy selector within common deadline constraints and USD fills."""
    policy = config['legacy_policy'];position = state.get('position')
    if position is not None:
        deadline = min(float(position['entry_epoch']) + float(config['maximum_holding_sec']),
                       float(position['original_target_epoch']))
        if epoch + float(config['execution_delay_sec']) >= deadline:
            return _empty_decision('exit', epoch, 'common_native_target_or_holding_deadline'), {
                'position_deadline': deadline, 'common_deadline_wrapper': True,
                'legacy_selector_source_sha256': LEGACY_SOURCE_SHA256,
                'scope': 'Unchanged legacy selector within common deadline constraints; shared new USD accounting.'}
    old_position = None
    if position:
        old_position = legacy.Position(position['instrument'], position['side'], position['base_units'],
                                       int(position['entry_epoch']), float(position['entry_price']),
                                       float(position['entry_price']), 0.0, 0.0, 'research', position['candidate_id'])
    old_candidates = [legacy.Candidate(c['instrument'], c['side'], float(c['legacy_score']),
                                     float(c['confidence']), float(abs(c['signed_price_change']) / config['metadata'][c['instrument']]['pip_size']),
                                     float(c['spread_pips']), 'explicit_metadata', 0, (), c['candidate_id'])
                      for c in candidates]
    old_candidates.sort(key=lambda c: (-c.score, c.instrument, -c.side))
    old_config = {'frozen_policy': dict(policy), 'session': {'normalized_units': 1,
                  'feedback_horizon_min': int(config.get('feedback_horizon_sec', 300)) // 60}}
    result = legacy.choose_primary_decision(legacy.PortfolioState(0.0, old_position), old_candidates,
                                             int(epoch), old_config, terminal_clock=terminal)
    if result.action in {'enter', 'rotate'}:
        picked = next(c for c in candidates if c['candidate_id'] == result.candidate_snapshot_id)
        decision = _candidate_decision(picked, result.action, result.rationale)
    else:
        decision = _empty_decision(result.action, epoch, result.rationale)
    return decision, {'legacy_selector_decision': asdict(result), 'legacy_selector_source_sha256': LEGACY_SOURCE_SHA256,
                      'scope': 'Unchanged legacy selector within common deadline constraints; shared new USD accounting, not a replay of old pip accounting.'}


def replay(frames: Sequence[Mapping[str, Any]], config: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate five separate state chains on one predeclared bounded schedule."""
    with localcontext(CTX):
        return _replay(frames, config)


def _replay(frames: Sequence[Mapping[str, Any]], raw_config: Mapping[str, Any]) -> dict[str, Any]:
    config = validate_config(raw_config)
    schedule = raw_config.get('decision_epochs')
    if not isinstance(schedule, list) or not 2 <= len(schedule) <= 4096 or len(frames) != len(schedule):
        raise ValueError('bounded_predeclared_schedule_required')
    schedule = [clock(x) for x in schedule]
    if any(b - a != float(config['cadence_sec']) for a, b in zip(schedule, schedule[1:])):
        raise ValueError('uniform_unique_schedule_required')
    states = {arm: flat_state() for arm in ARMS};rows = [];branches = []
    final_execution = schedule[-1] + float(config['execution_delay_sec'])
    execution_grid = {t + float(config['execution_delay_sec']) for t in schedule}
    for index, frame in enumerate(frames):
        epoch = schedule[index];terminal = index == len(frames) - 1
        if clock(frame.get('decision_epoch')) != epoch or frame.get('terminal') is not terminal:
            raise ValueError('scheduled_frame_identity_mismatch')
        execution = epoch + float(config['execution_delay_sec'])
        if clock(frame.get('execution_epoch')) != execution:
            raise ValueError('fixed_execution_latency_required')
        execution_observed = clock(frame.get('execution_observed_epoch', execution))
        if execution_observed < execution:
            raise ValueError('execution_observed_before_market_time')
        if not terminal and execution_observed > schedule[index + 1]:
            raise ValueError('execution_state_not_observed_before_next_decision')
        target = clock(frame.get('management_target_epoch'))
        if not terminal and not execution < target <= final_execution:
            raise ValueError('native_target_outside_declared_session')
        if not terminal and target not in execution_grid:
            raise ValueError('native_target_off_exact_execution_grid')
        decision_quotes, execution_quotes = frame.get('decision_quotes', {}), frame.get('execution_quotes', {})
        if not isinstance(decision_quotes, Mapping) or not isinstance(execution_quotes, Mapping) or max(len(decision_quotes), len(execution_quotes)) > 136:
            raise ValueError('bounded_quote_maps_required')
        prepared, refusals = {}, {}
        for kind in ('momentum', 'curve'):
            prepared[kind], refusals[kind] = prepare_candidates(frame.get(kind + '_candidates', []), kind,
                decision_quotes, epoch, target, config)
            supplied = frame.get(kind + '_refusals', [])
            if not isinstance(supplied, list) or len(supplied) > 136:
                raise ValueError('bounded_explicit_refusals_required')
            refusals[kind].extend(deepcopy(supplied))
        # Selection is completed for every arm before any future fill map is read.
        decisions, diagnostics = {}, {}
        for arm in ARMS:
            if arm == 'no_trade':
                decisions[arm] = _empty_decision('wait', epoch, 'fixed_no_trade');diagnostics[arm] = {}
            elif arm == 'legacy_momentum_reference':
                decisions[arm], diagnostics[arm] = choose_legacy_action(states[arm], prepared['momentum'], epoch, config, terminal=terminal)
            else:
                kind = 'momentum' if arm == 'usd_momentum_manager' else 'curve'
                decisions[arm], diagnostics[arm] = choose_usd_action(states[arm], prepared[kind], decision_quotes,
                    epoch, config, terminal=terminal, hold_only=arm == 'curve_hold_no_rotation')
        for arm in ARMS:
            before = deepcopy(states[arm]);decision = decisions[arm]
            states[arm], receipt = apply_action(before, decision, execution_quotes, execution, config, observed_epoch=execution_observed)
            valuation = liquidation_equity(states[arm], execution_quotes, execution, config, observed_epoch=execution_observed)
            feedback_epoch = frame.get('feedback_epoch')
            if feedback_epoch is not None and not execution <= clock(feedback_epoch) <= final_execution:
                raise ValueError('feedback_clock_outside_session')
            feedback_observed = clock(frame.get('feedback_observed_epoch', feedback_epoch)) if feedback_epoch is not None else None
            if feedback_epoch is not None and feedback_observed < feedback_epoch:
                raise ValueError('feedback_observed_before_market_time')
            primary_feedback = (liquidation_equity(states[arm], frame.get('feedback_quotes', {}), feedback_epoch, config, observed_epoch=feedback_observed)
                                if feedback_epoch is not None else {'status': 'not_supplied', 'equity_usd': None})
            row = {'arm': arm, 'clock_index': index, 'decision_epoch': epoch, 'execution_epoch': execution,
                   'management_target_epoch': target, 'terminal': terminal, 'state_before': before,
                   'execution_observed_epoch': execution_observed,
                   'decision': decision, 'decision_diagnostics': diagnostics[arm], 'execution': receipt,
                   'state_after': deepcopy(states[arm]), 'liquidation_value': valuation,
                   'feedback_epoch': feedback_epoch, 'feedback_observed_epoch': feedback_observed, 'primary_feedback': primary_feedback,
                   'candidate_refusals': refusals['momentum' if arm in ('legacy_momentum_reference', 'usd_momentum_manager') else 'curve'],
                   'accepted_candidate_evidence': [_candidate_evidence(c) for c in prepared['momentum' if arm in ('legacy_momentum_reference', 'usd_momentum_manager') else 'curve']],
                   'decision_input_sha256': digest({'epoch': epoch, 'quotes': decision_quotes,
                       'candidates': frame.get('momentum_candidates' if arm in ('legacy_momentum_reference', 'usd_momentum_manager') else 'curve_candidates', [])}),
                   'counts_as_additional_market_repetition': False}
            row['row_sha256'] = digest(row);rows.append(row)
            if arm == 'usd_curve_manager' and not terminal:
                choices = ['wait'] if before['position'] is None else ['hold', 'exit']
                options = [_empty_decision(a, epoch, 'depth_one_matched_alternative') for a in choices if a != decision['action']]
                eligible = [c for c in prepared['curve'] if c['score'] >= number(config['minimum_entry_cost_ratio'])]
                if eligible:
                    best = eligible[0]
                    action = 'enter' if before['position'] is None else 'rotate'
                    same_position = before['position'] and best['instrument'] == before['position']['instrument'] and best['side'] == before['position']['side']
                    if not same_position and not (action == decision['action'] and best['candidate_id'] == decision.get('candidate_id')):
                        options.append(_candidate_decision(best, action, 'depth_one_matched_alternative'))
                for option in options:
                    after, execution_receipt = apply_action(before, option, execution_quotes, execution, config, observed_epoch=execution_observed)
                    value = liquidation_equity(after, frame.get('feedback_quotes', {}), feedback_epoch, config, observed_epoch=feedback_observed) if feedback_epoch is not None else {'status': 'not_supplied', 'equity_usd': None}
                    branch = {'clock_index': index, 'parent_row_sha256': row['row_sha256'], 'state_before': before,
                              'decision': option, 'execution': execution_receipt, 'feedback_epoch': feedback_epoch,
                              'feedback_observed_epoch': feedback_observed,
                              'feedback': value, 'primary_feedback': primary_feedback,
                              'primary_minus_branch_equity_usd': (number(primary_feedback['equity_usd']) - number(value['equity_usd'])
                                  if primary_feedback.get('equity_usd') is not None and value.get('equity_usd') is not None else None),
                              'counts_as_additional_market_repetition': False}
                    branch['row_sha256'] = digest(branch);branches.append(branch)
    summaries = {}
    for arm in ARMS:
        selected = [r for r in rows if r['arm'] == arm]
        closes = [leg for r in selected for leg in r['execution']['legs'] if leg['kind'] == 'close']
        equity = [r['liquidation_value'].get('equity_usd') for r in selected]
        known = [number(v) for v in equity if v is not None]
        peak = Decimal(0);drawdown = Decimal(0)
        for value in known:
            peak = max(peak, value);drawdown = max(drawdown, peak - value)
        summaries[arm] = {'scheduled_decisions': len(selected), 'terminal_flat': states[arm]['position'] is None,
                          'realized_usd': states[arm]['realized_usd'], 'unresolved_position': states[arm]['position'],
                          'execution_rejections': sum(r['execution']['status'] != 'applied' for r in selected),
                          'execution_legs': sum(len(r['execution']['legs']) for r in selected),
                          'completed_position_closures': len(closes),
                          'positive_closures_after_cost': sum(leg['realized_usd'] > 0 for leg in closes),
                          'mean_realized_usd_per_closure': (sum((leg['realized_usd'] for leg in closes), Decimal(0)) / len(closes)) if closes else None,
                          'mean_holding_sec_per_closure': (sum((number(leg['execution_epoch']) - number(leg['original_entry']['entry_epoch']) for leg in closes), Decimal(0)) / len(closes)) if closes else None,
                          'unavailable_liquidation_marks': sum(v is None for v in equity),
                          'observed_mark_drawdown_usd': drawdown if len(known) == len(selected) else None,
                          'actions': {a: sum(r['decision']['action'] == a for r in selected) for a in sorted(legacy.ALLOWED_ACTIONS)}}
    matched = {}
    for comparator in ('usd_momentum_manager', 'no_trade', 'curve_hold_no_rotation', 'legacy_momentum_reference'):
        a, b = summaries['usd_curve_manager'], summaries[comparator]
        matched[comparator] = {'same_scheduled_global_clocks': len(schedule),
                              'terminal_realized_delta_usd': a['realized_usd'] - b['realized_usd'] if a['terminal_flat'] and b['terminal_flat'] else None,
                              'selector_input_attribution': 'same_usd_selector_different_input' if comparator == 'usd_momentum_manager' else 'different_policy_control'}
    result = {'schema_version': SCHEMA, **SAFETY, 'status': 'mechanics_complete' if all(s['terminal_flat'] for s in summaries.values()) else 'unresolved_terminal_position',
              'config_sha256': digest(raw_config), 'input_sha256': digest(frames), 'legacy_selector_source_sha256': LEGACY_SOURCE_SHA256,
              'curve_target_window_policy': config['curve_target_window_policy'],
              'summaries': summaries, 'matched_comparisons': matched, 'rows': rows, 'counterfactuals': branches,
              'global_decision_clocks': len(schedule), 'independent_sample_size': None,
              'limits': ['No publication is inferred: supplied curve receipts must preserve their actual availability and native target clocks.',
                         'This pure boundary validates DTO semantics but does not independently reconstruct model predictions or authenticate external source bytes.',
                         'USD conversion uses contemporaneous direct/inverse bid/ask only; no triangulation, financing, broker fills, account access or leverage assumptions.',
                         'Separate policy trajectories diverge after their actions; local depth-one branches share exact predecision state and have zero additional repetition weight.',
                         'Original curve probabilities are retained but never rebased or claimed calibrated for the remaining interval.',
                         'An explicit nominal_management_boundary experiment ends at the nominal clock; it does not reproduce a fitted target that may use a later complete bar within its retained window.',
                         'Historical mechanics, overlapping horizons and shared currencies do not establish predictive improvement or a profitable policy.']}
    result['report_sha256'] = digest(result)
    return jsonable(result)
