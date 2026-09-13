"""FX pip resolution with auditable metadata and bounded fallback.

FALLBACK_PIPS is generated from the pinned existing shared fallback over the
68 instruments verified against retained local venue pip metadata. No broker,
account, file, model, or process action occurs when importing this module.
"""
from __future__ import annotations
from decimal import Decimal,InvalidOperation
import math
import re
from typing import Mapping
from types import MappingProxyType

CONTRACT='fx_pip_metadata_contract_v2_20260912'
MIN_PIP=Decimal('1e-10')
MAX_PIP=Decimal('1')


def normalize_instrument(instrument):
    if not isinstance(instrument,str):raise ValueError('instrument_string_required')
    normalized=instrument.strip().upper().replace('/','_')
    if re.fullmatch(r'[A-Z]{3}_[A-Z]{3}',normalized) is None:
        raise ValueError('fx_instrument_identity_required')
    if normalized[:3] == normalized[4:]:
        raise ValueError('distinct_fx_currencies_required')
    return normalized


def _decimal(value):
    if isinstance(value,bool) or value is None or not isinstance(value,(str,int,float,Decimal)):
        raise ValueError('numeric_metadata_type_required')
    if isinstance(value,int) and value.bit_length()>128:
        raise ValueError('numeric_metadata_bound_exceeded')
    text=str(value).strip()
    if not text or len(text)>64:raise ValueError('numeric_metadata_bound_exceeded')
    try:result=Decimal(text)
    except (ValueError,InvalidOperation) as exc:raise ValueError('invalid_numeric_metadata') from exc
    if not result.is_finite():raise ValueError('finite_metadata_required')
    return result


def _pip(value):
    parsed=_decimal(value)
    if not MIN_PIP<=parsed<=MAX_PIP:raise ValueError('fx_pip_out_of_bounds')
    result=float(parsed)
    if not math.isfinite(result) or result<=0:raise ValueError('finite_positive_pip_required')
    return result


def _location(value):
    parsed=_decimal(value)
    if parsed!=parsed.to_integral_value():raise ValueError('integer_pip_location_required')
    # Same explicit FX exponent bounds as the existing practice trial policy.
    if not Decimal(-10)<=parsed<=Decimal(0):raise ValueError('fx_pip_location_out_of_bounds')
    return 10.0**int(parsed)


def fallback_pip_size(instrument):
    normalized=normalize_instrument(instrument)
    if normalized not in FALLBACK_PIPS:
        raise ValueError('unknown_instrument_requires_valid_venue_metadata')
    return FALLBACK_PIPS[normalized]


def resolve_pip_contract(instrument,*metadata):
    normalized=normalize_instrument(instrument)
    rejected=[]
    for index,row in enumerate(metadata):
        if not isinstance(row,Mapping):
            if row is not None:rejected.append({'metadata_index':index,'field':None,'reason':'metadata_mapping_required'})
            continue
        identity_ok=True
        for field in ('instrument','name','pair'):
            if field not in row:continue
            try:
                if normalize_instrument(row[field])!=normalized:
                    raise ValueError('metadata_instrument_mismatch')
            except ValueError as exc:
                rejected.append({'metadata_index':index,'field':field,'reason':str(exc)})
                identity_ok=False
        if not identity_ok:continue
        valid=[]
        for field in ('pip','pip_size','pipLocation'):
            if field not in row or row[field] is None:continue
            try:
                value=_location(row[field]) if field=='pipLocation' else _pip(row[field])
                valid.append((field,value))
            except ValueError as exc:
                rejected.append({'metadata_index':index,'field':field,'reason':str(exc)})
        if not valid:continue
        if any(not math.isclose(value,valid[0][1],rel_tol=1e-12,abs_tol=0.0) for _,value in valid[1:]):
            rejected.append({'metadata_index':index,'field':None,'reason':'metadata_pip_fields_disagree'})
            continue
        field,value=valid[0]
        return {'contract':CONTRACT,'instrument':normalized,'pip':value,'source':'venue_metadata',
                'metadata_index':index,'metadata_field':field,'rejections':rejected,
                'status':'resolved_with_rejections' if rejected else 'resolved'}
    return {'contract':CONTRACT,'instrument':normalized,'pip':fallback_pip_size(normalized),
            'source':'pinned_68_pair_fallback','metadata_index':None,'metadata_field':None,
            'rejections':rejected,'status':'fallback_with_rejections' if rejected else 'fallback'}


def resolve_pip_size(instrument,*metadata):
    """Numeric compatibility API; audit-aware callers should retain the contract."""
    return resolve_pip_contract(instrument,*metadata)['pip']


def pips_multiplier(instrument):
    return 1.0/fallback_pip_size(instrument)


FALLBACK_PIPS = MappingProxyType({'AUD_CAD': 0.0001, 'AUD_CHF': 0.0001, 'AUD_HKD': 0.0001, 'AUD_JPY': 0.01, 'AUD_NZD': 0.0001, 'AUD_SGD': 0.0001, 'AUD_USD': 0.0001, 'CAD_CHF': 0.0001, 'CAD_HKD': 0.0001, 'CAD_JPY': 0.01, 'CAD_SGD': 0.0001, 'CHF_HKD': 0.0001, 'CHF_JPY': 0.01, 'CHF_ZAR': 0.0001, 'EUR_AUD': 0.0001, 'EUR_CAD': 0.0001, 'EUR_CHF': 0.0001, 'EUR_CZK': 0.0001, 'EUR_DKK': 0.0001, 'EUR_GBP': 0.0001, 'EUR_HKD': 0.0001, 'EUR_HUF': 0.01, 'EUR_JPY': 0.01, 'EUR_NOK': 0.0001, 'EUR_NZD': 0.0001, 'EUR_PLN': 0.0001, 'EUR_SEK': 0.0001, 'EUR_SGD': 0.0001, 'EUR_TRY': 0.0001, 'EUR_USD': 0.0001, 'EUR_ZAR': 0.0001, 'GBP_AUD': 0.0001, 'GBP_CAD': 0.0001, 'GBP_CHF': 0.0001, 'GBP_HKD': 0.0001, 'GBP_JPY': 0.01, 'GBP_NZD': 0.0001, 'GBP_PLN': 0.0001, 'GBP_SGD': 0.0001, 'GBP_USD': 0.0001, 'GBP_ZAR': 0.0001, 'HKD_JPY': 0.0001, 'NZD_CAD': 0.0001, 'NZD_CHF': 0.0001, 'NZD_HKD': 0.0001, 'NZD_JPY': 0.01, 'NZD_SGD': 0.0001, 'NZD_USD': 0.0001, 'SGD_CHF': 0.0001, 'SGD_JPY': 0.01, 'TRY_JPY': 0.01, 'USD_CAD': 0.0001, 'USD_CHF': 0.0001, 'USD_CNH': 0.0001, 'USD_CZK': 0.0001, 'USD_DKK': 0.0001, 'USD_HKD': 0.0001, 'USD_HUF': 0.01, 'USD_JPY': 0.01, 'USD_MXN': 0.0001, 'USD_NOK': 0.0001, 'USD_PLN': 0.0001, 'USD_SEK': 0.0001, 'USD_SGD': 0.0001, 'USD_THB': 0.01, 'USD_TRY': 0.0001, 'USD_ZAR': 0.0001, 'ZAR_JPY': 0.01})
