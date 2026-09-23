"""Finite-JSON profit-factor contract for research evaluators.

Zero gross losses with positive gross wins is unbounded, represented by null
plus an explicit status and its sufficient statistics. No arbitrary cap, zero
replacement, or inference from legacy win-rate/summary fields is performed.
"""
from __future__ import annotations
import math

CONTRACT = 'gross_pips_profit_factor_v2_20260912'


def _amount(value):
    if isinstance(value, bool):
        raise ValueError('invalid_gross_pips')
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError('invalid_gross_pips')
    return number


def factor_fields(gross_win, gross_loss):
    win, loss = _amount(gross_win), _amount(gross_loss)
    if loss > 0:
        factor, status = win / loss, 'finite'
        if not math.isfinite(factor):
            raise ValueError('profit_factor_overflow')
    elif win > 0:
        factor, status = None, 'no_losses'
    else:
        factor, status = 0.0, 'no_gross_returns'
    return {'profit_factor': factor, 'profit_factor_status': status,
            'gross_win_pips': win, 'gross_loss_pips': loss,
            'profit_factor_contract': CONTRACT}


def normalize_profit_factor(row):
    """Carry new sufficient statistics; preserve only finite legacy values.

    A legacy non-finite value has no independently supplied denominator proof.
    It remains explicitly unavailable; this function does not rescore old rows.
    """
    has_components = 'gross_win_pips' in row or 'gross_loss_pips' in row
    if has_components:
        if row.get('gross_win_pips') is None or row.get('gross_loss_pips') is None:
            raise ValueError('incomplete_profit_factor_components')
        expected = factor_fields(row['gross_win_pips'], row['gross_loss_pips'])
        if row.get('profit_factor_contract') != CONTRACT:
            raise ValueError('profit_factor_contract_missing_or_changed')
        if row.get('profit_factor_status') != expected['profit_factor_status']:
            raise ValueError('profit_factor_status_mismatch')
        supplied = row.get('profit_factor')
        if expected['profit_factor'] is None:
            if supplied is not None:
                raise ValueError('profit_factor_value_mismatch')
        elif isinstance(supplied, bool) or supplied is None or not math.isclose(
                float(supplied), expected['profit_factor'], rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError('profit_factor_value_mismatch')
        return expected
    if row.get('profit_factor_contract') == CONTRACT:
        if (row.get('profit_factor_status') == 'unavailable_missing_cell_components'
                and row.get('profit_factor') is None):
            return {'profit_factor': None,
                    'profit_factor_status': 'unavailable_missing_cell_components',
                    'profit_factor_contract': CONTRACT}
        raise ValueError('missing_profit_factor_components')
    supplied = row.get('profit_factor')
    try:
        if supplied is None or isinstance(supplied, bool):
            raise ValueError('missing')
        value = float(supplied)
    except (TypeError, ValueError):
        value = None
    if value is None or not math.isfinite(value) or value < 0:
        return {'profit_factor': None, 'profit_factor_status': 'legacy_unavailable',
                'profit_factor_contract': 'legacy_without_gross_components'}
    return {'profit_factor': value, 'profit_factor_status': 'legacy_finite',
            'profit_factor_contract': 'legacy_without_gross_components'}


def profit_factor_at_least(row, minimum):
    threshold = _amount(minimum)
    metric = normalize_profit_factor(row)
    if metric['profit_factor_status'] == 'no_losses':
        return metric['gross_win_pips'] > 0 and metric['gross_loss_pips'] == 0
    value = metric['profit_factor']
    return value is not None and value >= threshold


def pooled_profit_factor(cells):
    rows = list(cells)
    if any(row.get('profit_factor_contract') != CONTRACT or
           row.get('gross_win_pips') is None or row.get('gross_loss_pips') is None
           for row in rows):
        return {'profit_factor': None,
                'profit_factor_status': 'unavailable_missing_cell_components',
                'profit_factor_contract': CONTRACT}
    validated = [normalize_profit_factor(row) for row in rows]
    return factor_fields(math.fsum(row['gross_win_pips'] for row in validated),
                         math.fsum(row['gross_loss_pips'] for row in validated))


def summary_metric_fields(row, field):
    """Preserve unavailable median/drawdown instead of displaying a zero."""
    if field not in ('median_net_pips', 'max_drawdown_pips'):
        raise ValueError('unsupported_nullable_summary_metric')
    value = row.get(field)
    try:
        number = float(value) if value is not None and not isinstance(value, bool) else None
    except (TypeError, ValueError):
        number = None
    if number is None or not math.isfinite(number):
        return {field: None, field+'_status': row.get(field+'_status') or 'unavailable_missing_or_nonfinite'}
    result = {field: number}
    if row.get(field+'_status'):
        result[field+'_status'] = row[field+'_status']
    return result
