import pytest
import oanda_instrument_pips_v2 as units
import oanda_strategy_lab_historical_backtest as history
from test_oanda_supervised_m5_contract_v2 import m1_frame


@pytest.mark.parametrize('pair',['EUR_EUR','eur/eur',' USD_USD '])
@pytest.mark.parametrize('metadata',[None,{'pip':.0001},{'pipLocation':-2}])
def test_same_currency_refused_even_with_valid_numeric_metadata(pair,metadata):
    with pytest.raises(ValueError,match='distinct_fx_currencies_required'):
        units.resolve_pip_contract(pair,metadata)


@pytest.mark.parametrize('pair,pip',[('EUR_HUF',.01),('USD_HUF',.01),('USD_THB',.01),('HKD_JPY',.0001),('EUR_USD',.0001)])
def test_real_history_builder_retains_full_resolved_unit(pair,pip):
    result=history.build_pair_history(pair,m1_frame())
    quality=result.frame.attrs['input_quality']
    assert result.pip==pip
    assert quality['pip_resolution']==units.resolve_pip_contract(pair)
    assert quality['pip_source']=='pinned_68_pair_fallback'
    assert quality['contract']=='historical_m1_complete_m5_and_pip_provenance_v3_20260912'


def test_history_normalizes_pair_before_outputs():
    result=history.build_pair_history(' eur/usd ',m1_frame())
    assert result.instrument=='EUR_USD'
    assert result.frame.attrs['input_quality']['pip_resolution']['instrument']=='EUR_USD'


@pytest.mark.parametrize('pair',['AAA_BBB','EUR_EUR','EURUSD'])
def test_invalid_unit_identity_is_rejected_before_price_processing(pair):
    class CannotReadFrame:
        def copy(self):raise AssertionError('price processing occurred before unit validation')
    with pytest.raises(ValueError):history.build_pair_history(pair,CannotReadFrame())


def test_original_input_frame_not_mutated_by_unit_provenance():
    frame=m1_frame();before=dict(frame.attrs)
    history.build_pair_history('USD_HUF',frame)
    assert frame.attrs==before
