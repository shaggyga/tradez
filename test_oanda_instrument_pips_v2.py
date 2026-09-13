from decimal import Decimal
import json
import math
from pathlib import Path
import sys

import pytest

import oanda_instrument_pips_v2 as candidate


@pytest.mark.parametrize('instrument',['EUR_HUF','USD_HUF','USD_THB','HKD_JPY','EUR_USD','USD_JPY'])
def test_verified_map_and_normalization(instrument):
    report=json.loads((Path(__file__).resolve().parent/'test_fixtures/fx_pip_metadata_20260911.json').read_text())
    row=next(row for row in report['retained_quote_metadata']['entries'] if row['instrument']==instrument)
    assert candidate.fallback_pip_size(instrument)==row['retained_pip']['value']
    assert candidate.fallback_pip_size(' '+instrument.lower().replace('_','/')+' ')==row['retained_pip']['value']


@pytest.mark.parametrize('value',[math.inf,-math.inf,math.nan,'inf','NaN',True,False,-1,0,1e-11,1.01,{},[],2**1000,'1e99999999'])
def test_invalid_explicit_pip_never_escapes_as_numeric_unit(value):
    result=candidate.resolve_pip_contract('USD_HUF',{'pip':value})
    assert result['pip']==.01 and result['status']=='fallback_with_rejections'
    assert result['rejections'] and math.isfinite(result['pip'])
    json.dumps(result,allow_nan=False)


@pytest.mark.parametrize('value',[math.inf,math.nan,1000,-1000,-3.5,'-3.5',True,False,1,-11,'1e99999999',2**1000])
def test_pip_location_is_finite_integral_and_bounded_before_exponentiation(value):
    result=candidate.resolve_pip_contract('USD_HUF',{'pipLocation':value})
    assert result['pip']==.01 and result['source']=='pinned_68_pair_fallback'
    assert result['rejections']


@pytest.mark.parametrize('value',[-4,-4.0,'-4','-4.0',Decimal('-4')])
def test_valid_integer_location_forms_resolve_identically(value):
    result=candidate.resolve_pip_contract('EUR_USD',{'pipLocation':value})
    assert result['pip']==.0001 and result['status']=='resolved'


def test_valid_metadata_overrides_known_map_and_preserves_source():
    result=candidate.resolve_pip_contract('USD_HUF',{'name':'USD_HUF','pip':.001,'pipLocation':-3})
    assert result['pip']==.001 and result['source']=='venue_metadata' and not result['rejections']


def test_conflicting_valid_fields_reject_whole_row_instead_of_silently_preferring_one():
    result=candidate.resolve_pip_contract('USD_HUF',{'pip':.01,'pipLocation':-4})
    assert result['status']=='fallback_with_rejections'
    assert result['rejections'][0]['reason']=='metadata_pip_fields_disagree'


def test_declared_wrong_identity_is_skipped_before_a_later_valid_row():
    result=candidate.resolve_pip_contract('USD_HUF',{'instrument':'EUR_USD','pip':.0001},{'name':'USD_HUF','pipLocation':-2})
    assert result['pip']==.01 and result['metadata_index']==1
    assert result['rejections'][0]['reason']=='metadata_instrument_mismatch'


def test_invalid_field_does_not_hide_valid_sibling_but_rejection_remains_visible():
    result=candidate.resolve_pip_contract('EUR_USD',{'pip':math.inf,'pipLocation':-5})
    assert result['pip']==1e-5 and result['status']=='resolved_with_rejections'
    assert result['rejections'][0]['reason']=='finite_metadata_required'


def test_unknown_instrument_cannot_get_implicit_default_but_valid_metadata_is_supported():
    with pytest.raises(ValueError,match='unknown_instrument'):
        candidate.fallback_pip_size('AAA_BBB')
    assert candidate.resolve_pip_size('AAA_BBB',{'instrument':'AAA_BBB','pipLocation':-4})==.0001


@pytest.mark.parametrize('instrument',[None,True,'EURUSD','','../USD_HUF','US_DHUF'])
def test_malformed_instrument_identity_refused(instrument):
    with pytest.raises(ValueError):candidate.resolve_pip_size(instrument)


def test_all68_retained_metadata_entries_match_generated_existing_map():
    report=json.loads((Path(__file__).resolve().parent/'test_fixtures/fx_pip_metadata_20260911.json').read_text())
    entries=report['retained_quote_metadata']['entries']
    assert len(entries)==len(candidate.FALLBACK_PIPS)==68
    assert all(candidate.fallback_pip_size(row['instrument'])==row['retained_pip']['value'] for row in entries)


def test_pinned_unit_map_cannot_be_mutated_by_an_accidental_assignment():
    with pytest.raises(TypeError):candidate.FALLBACK_PIPS['USD_HUF']=.0001
