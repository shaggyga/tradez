"""Pure three-pair research projection of current-mark FX risk-factor legs.

This is neither an account ledger nor an order/position manager. Each call
accepts exactly one virtual scenario and never changes the caller's state.
Filesystem bytes are fingerprinted once on module load; projection performs no
I/O. Original source availability is caller-attested, not reconstructed here.
"""
from __future__ import annotations

from copy import deepcopy
from decimal import Context, Decimal, ROUND_HALF_EVEN, localcontext
import hashlib
import json
import math
from pathlib import Path
import re
from types import MappingProxyType

import oanda_curve_management_replay_v1 as mechanics
from src.forex_system.contracts import signed_currency_exposure as identity

SCHEMA = 'currency_exposure_projection_v1_20260909'
PAIRS = ('EUR_USD', 'GBP_USD', 'USD_JPY')
CURRENCIES = ('EUR', 'GBP', 'JPY', 'USD')
MAX_POSITIONS = 32
MAX_INPUT_BYTES = 512 * 1024
CTX = Context(prec=192, rounding=ROUND_HALF_EVEN)
FLAGS = dict(research_only=True, orders_enabled=False, can_place_orders=False,
             can_promote=False, can_authorize=False, execution_eligible=False,
             account_eligible=False, proof_eligible=False, manager_activation=False)
_FROZEN = {
    'oanda_curve_management_replay_v1.py': '52fd04c6b087df4be775e490002afd0744caeb245604b8f577ab0fcff41de8f3',
    'src/forex_system/research/sequential_portfolio_replay_v1.py': 'e1201fdd3387825c9295f13045aeb00e2f87cce9ff819e789faf922ea2e514a6',
}
_ROOT = Path(__file__).resolve().parent
_FILES = (*_FROZEN, 'src/forex_system/contracts/signed_currency_exposure.py', Path(__file__).name)
_BOUND = MappingProxyType({name: hashlib.sha256((_ROOT/name).read_bytes()).hexdigest() for name in _FILES})
if any(_BOUND[name] != sha for name, sha in _FROZEN.items()):
    raise ValueError('exposure_frozen_dependency_changed')


def source_bindings():
    """Return module-load bytes identities; this performs no live source reads."""
    return dict(_BOUND)


def _need(ok, reason):
    if not ok:
        raise ValueError('exposure_' + reason)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _digest(value):
    return hashlib.sha256(_canonical(value)).hexdigest()


def _number(value, label, *, zero=True):
    _need(type(value) is str and len(value) <= 48 and
          re.fullmatch(r'(?:0|[1-9][0-9]{0,14})(?:\.[0-9]{1,24})?', value), label + '_exact_decimal')
    result = Decimal(value)
    _need(result.is_finite() and result <= Decimal('1e14') and (result >= 0 if zero else result > 0), label + '_bound')
    return result


def _epoch(value):
    _need(type(value) in (int, float) and math.isfinite(value) and 0 < value < 10**11, 'clock')
    return Decimal(str(value))


def _id(value, label):
    _need(type(value) is str and re.fullmatch(r'[A-Za-z0-9_.:-]{1,128}', value), label)
    return value


def _sha(value):
    _need(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value), 'source_record_sha256')


def _keys(value, expected, label):
    _need(type(value) is dict and set(value) == set(expected), label + '_fields')


def _metadata(value):
    _keys(value, PAIRS, 'metadata_scope')
    for pair, row in value.items():
        _keys(row, ('instrument', 'base_currency', 'quote_currency', 'pip_size', 'unit_increment'), 'metadata')
        base, quote = identity.split_currency_pair(pair)
        _need((row['instrument'], row['base_currency'], row['quote_currency']) == (pair, base, quote), 'metadata_identity')
        pip = _number(row['pip_size'], 'pip_size', zero=False)
        _need(pip == Decimal('0.01' if pair == 'USD_JPY' else '0.0001'), 'metadata_pip')
        _need(type(row['unit_increment']) is int and row['unit_increment'] == 1, 'integer_base_unit_contract')
    mechanics.validate_metadata(value)


def _limits(value):
    _keys(value, ('maximum_positions', 'maximum_currency_direction_positions',
                 'maximum_gross_position_notional_usd', 'maximum_currency_gross_usd',
                 'maximum_currency_abs_net_usd', 'maximum_currency_gross_leg_share',
                 'maximum_quote_age_sec'), 'limits')
    for field in ('maximum_positions', 'maximum_currency_direction_positions'):
        _need(type(value[field]) is int and 0 <= value[field] <= MAX_POSITIONS, field)
    _need(type(value['maximum_quote_age_sec']) is int and 1 <= value['maximum_quote_age_sec'] <= 60, 'maximum_quote_age_sec')
    _number(value['maximum_gross_position_notional_usd'], 'gross_position_limit')
    for field in ('maximum_currency_gross_usd', 'maximum_currency_abs_net_usd', 'maximum_currency_gross_leg_share'):
        _keys(value[field], CURRENCIES, field)
        for amount in value[field].values():
            parsed = _number(amount, field)
            if field == 'maximum_currency_gross_leg_share':
                _need(parsed <= 1, 'currency_share_limit')


def _positions(rows, candidate, scenario, decision):
    _need(type(rows) is list and len(rows) <= MAX_POSITIONS, 'position_count_bound')
    ids = set()
    result = []
    for raw, is_candidate in [(r, False) for r in rows] + ([] if candidate is None else [(candidate, True)]):
        name = 'candidate_id' if is_candidate else 'position_id'
        clocks = ('available_epoch',) if is_candidate else ('opened_epoch', 'state_available_epoch')
        _keys(raw, (name, 'scenario_id', 'instrument', 'side', 'base_units', 'source_record_sha256', *clocks), 'position')
        row_id = _id(raw[name], name)
        _need(row_id not in ids, 'duplicate_position_or_candidate_id'); ids.add(row_id)
        _need(raw['scenario_id'] == scenario, 'cross_scenario_position')
        _need(raw['instrument'] in PAIRS, 'unsupported_instrument')
        _need(type(raw['side']) is int and raw['side'] in (-1, 1), 'explicit_side')
        _need(type(raw['base_units']) is int and 1 <= raw['base_units'] <= 10**9, 'positive_integer_base_units')
        _sha(raw['source_record_sha256'])
        available = _epoch(raw['available_epoch'] if is_candidate else raw['state_available_epoch'])
        _need(available <= decision, 'position_unavailable_at_decision')
        if not is_candidate:
            _need(_epoch(raw['opened_epoch']) <= available, 'position_clock_order')
        result.append(dict(raw, projection_row_id=row_id, candidate=is_candidate))
    _need(len(result) <= MAX_POSITIONS, 'projected_position_count_bound')
    return result


def _quotes(value, decision_raw, maximum_age):
    _need(type(value) is dict and set(value) <= set(PAIRS), 'quote_scope')
    decision = _epoch(decision_raw)
    for pair, row in value.items():
        _keys(row, ('instrument', 'quote_id', 'bid', 'ask', 'market_epoch', 'available_epoch', 'tradeable', 'source_record_sha256'), 'quote')
        _id(row['quote_id'], 'quote_id'); _sha(row['source_record_sha256'])
        _need(row['instrument'] == pair and row['tradeable'] is True, 'quote_identity_or_tradeability')
        _number(row['bid'], 'bid', zero=False); _number(row['ask'], 'ask', zero=False)
        market, available = _epoch(row['market_epoch']), _epoch(row['available_epoch'])
        _need(market <= available <= decision and decision-market <= maximum_age, 'quote_not_available_or_fresh_at_decision')
        # Both market and availability clocks are bounded by the original
        # decision; a later observation never unlocks future information.
        mechanics.quote_at(value, pair, decision_raw, maximum_age)


def _plain(value):
    if isinstance(value, Decimal):
        return format(value, 'f')
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(v) for v in value]
    return value


def _snapshot(rows, quotes, metadata, limits, decision_raw):
    per_currency = {ccy: dict(long_units=Decimal(0), short_units=Decimal(0), long_position_count=0,
                             short_position_count=0) for ccy in CURRENCIES}
    valued, rates = [], {}
    maximum_age = limits['maximum_quote_age_sec']
    for row in sorted(rows, key=lambda r: r['projection_row_id']):
        pair = row['instrument']
        q = mechanics.quote_at(quotes, pair, decision_raw, maximum_age)
        base, quote = identity.split_currency_pair(pair)
        for ccy in (base, quote):
            if ccy not in rates:
                rate = mechanics.usd_rates(ccy, quotes, decision_raw, maximum_age)
                rate['mid_currency_usd'] = ((rate['sell_currency_usd'] + rate['buy_currency_usd']) / 2
                    if ccy != 'JPY' else Decimal(1) / mechanics.quote_at(quotes, 'USD_JPY', decision_raw, maximum_age)['mid'])
                rates[ccy] = rate
        units = Decimal(row['base_units'])
        signed_base = units * row['side']
        legs = []
        factors = identity.pair_currency_exposures(pair, 'long' if row['side'] == 1 else 'short')
        for ccy, amount, factor in ((base, signed_base, factors[0]), (quote, -signed_base*q['mid'], factors[1])):
            rate = rates[ccy]
            bucket = per_currency[ccy]
            key = 'long' if amount > 0 else 'short'
            bucket[key+'_units'] += abs(amount); bucket[key+'_position_count'] += 1
            legs.append(dict(currency=ccy, signed_risk_units=amount, factor_id=factor.factor_id,
                signed_mid_usd_risk_equivalent=amount*rate['mid_currency_usd'],
                conservative_absolute_usd_risk_equivalent=abs(amount)*rate['buy_currency_usd'],
                conversion=rate))
        valued.append(dict(row_id=row['projection_row_id'], instrument=pair, side=row['side'], base_units=row['base_units'],
            candidate=row['candidate'], source_record_sha256=row['source_record_sha256'],
            mark_quote_id=q['quote_id'], mark_quote_sha256=_digest(quotes[pair]), mid=q['mid'],
            mid_position_notional_usd=units*rates[base]['mid_currency_usd'],
            conservative_position_notional_usd=units*rates[base]['buy_currency_usd'], legs=legs))
    for ccy, bucket in per_currency.items():
        net = bucket['long_units']-bucket['short_units']; gross = bucket['long_units']+bucket['short_units']
        rate = rates.get(ccy)
        bucket.update(net_signed_units=net, gross_units=gross,
            net_signed_mid_usd_risk_equivalent=Decimal(0) if rate is None else net*rate['mid_currency_usd'],
            conservative_abs_net_usd=Decimal(0) if rate is None else abs(net)*rate['buy_currency_usd'],
            conservative_gross_usd=Decimal(0) if rate is None else gross*rate['buy_currency_usd'],
            conversion=rate, valuation_status='no_exposure' if rate is None else 'valued')
    gross_legs = sum((r['conservative_gross_usd'] for r in per_currency.values()), Decimal(0))
    gross_notional = sum((r['conservative_position_notional_usd'] for r in valued), Decimal(0))
    breaches = []
    def compare(name, actual, limit, currency=None, side=None):
        if actual > limit:
            breaches.append(dict(limit=name, actual=actual, maximum=limit, currency=currency, side=side))
    compare('maximum_positions', len(rows), limits['maximum_positions'])
    compare('maximum_gross_position_notional_usd', gross_notional, Decimal(limits['maximum_gross_position_notional_usd']))
    for ccy, bucket in per_currency.items():
        share = bucket['conservative_gross_usd']/gross_legs if gross_legs else Decimal(0)
        bucket['gross_leg_share'] = share
        for side in ('long', 'short'):
            compare('maximum_currency_direction_positions', bucket[side+'_position_count'], limits['maximum_currency_direction_positions'], ccy, side)
        compare('maximum_currency_gross_usd', bucket['conservative_gross_usd'], Decimal(limits['maximum_currency_gross_usd'][ccy]), ccy)
        compare('maximum_currency_abs_net_usd', bucket['conservative_abs_net_usd'], Decimal(limits['maximum_currency_abs_net_usd'][ccy]), ccy)
        compare('maximum_currency_gross_leg_share', share, Decimal(limits['maximum_currency_gross_leg_share'][ccy]), ccy)
    return dict(position_count=len(rows), positions=valued, currencies=per_currency,
        conservative_gross_position_notional_usd=gross_notional,
        conservative_gross_currency_leg_attribution_usd=gross_legs,
        within_supplied_limits=not breaches, limit_breaches=breaches,
        count_capacity_remaining=max(0, limits['maximum_positions']-len(rows)))


def project_exposure(positions, candidate=None, *, quotes, metadata, limits, scenario_id,
                     decision_epoch, observed_epoch, expected_source_bindings):
    """Validate all inputs or raise ValueError; return no partial unvalued book.

    ``positions`` must be a list from ONE virtual scenario. Candidate is an
    additive what-if, not a close/replace instruction. All clocks and record
    hashes are supplied retained evidence, never verified as broker holdings.
    Exact string financial inputs and integer base units are required.
    """
    _need(type(expected_source_bindings) is dict and expected_source_bindings == dict(_BOUND), 'source_bindings')
    _id(scenario_id, 'scenario_id')
    decision, observed = _epoch(decision_epoch), _epoch(observed_epoch)
    _need(decision <= observed, 'observation_before_decision')
    inputs = dict(positions=positions, candidate=candidate, quotes=quotes, metadata=metadata, limits=limits,
                  scenario_id=scenario_id, decision_epoch=decision_epoch, observed_epoch=observed_epoch)
    raw = _canonical(inputs)
    _need(len(raw) <= MAX_INPUT_BYTES, 'input_byte_bound')
    saved = deepcopy(inputs)
    with localcontext(CTX):
        _metadata(saved['metadata']); _limits(saved['limits'])
        rows = _positions(saved['positions'], saved['candidate'], scenario_id, decision)
        _quotes(saved['quotes'], decision_epoch, saved['limits']['maximum_quote_age_sec'])
        before = _snapshot([r for r in rows if not r['candidate']], saved['quotes'], saved['metadata'], saved['limits'], decision_epoch)
        after = _snapshot(rows, saved['quotes'], saved['metadata'], saved['limits'], decision_epoch)
        body = _plain(dict(schema_version=SCHEMA, **FLAGS, status='complete_diagnostic', scenario_id=scenario_id,
            decision_epoch=decision_epoch, observed_epoch=observed_epoch,
            source_bindings=dict(_BOUND), input_sha256=hashlib.sha256(raw).hexdigest(),
            input_evidence=saved, before=before, after=after,
            candidate_mode='none' if candidate is None else 'additive_same_scenario_what_if',
            changed_gross_position_notional_usd=after['conservative_gross_position_notional_usd']-before['conservative_gross_position_notional_usd'],
            scopes=['Current midpoint FX risk-factor sensitivity legs, not financed entry legs, account cash, NAV, margin or leverage.',
                    'Signed base units and minus base units times current pair midpoint in quote currency; no entry-price balance reconstruction.',
                    'Midpoint risk USD uses direct midpoint or inverse midpoint; conservative absolute valuation uses USD purchase cost from verified bid/ask.',
                    'Gross currency-leg attribution counts both economic legs by design; it is separate from gross position notional and is not portfolio loss.',
                    'Currency topology and concentration are not return/error covariance, VaR, probability or independent confirmation.',
                    'Single caller-attested virtual scenario; never combine counterfactual arms or separate episode accounts.',
                    'Limits are diagnostic caller inputs, not tuned thresholds, authorization or a recommendation to trade.',
                    'Financial arithmetic uses a private192-digit Decimal context; JSON clock comparisons use Decimal(str(value)) before reused clock validation.',
                    'Module-load source fingerprints bind loaded source files; no live source reread or runtime authenticity claim is made.'],
            actual_account_positions_observed=False, broker_requests=0, runtime_writes=False))
    _need(_canonical(inputs) == raw, 'caller_inputs_changed_during_projection')
    return {**body, 'projection_sha256': _digest(body)}
