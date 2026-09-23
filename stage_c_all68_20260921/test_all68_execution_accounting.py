import pytest

from all68_execution_accounting import Quote, convert_quote_pnl, fill_after, financing_charge, quote_pnl


def test_long_and_short_use_opposite_executable_sides_once_each():
    entry = Quote(100, 99.0, 101.0)
    exit = Quote(200, 99.0, 101.0)
    assert quote_pnl(1, 1.0, entry, exit) == -2.0
    assert quote_pnl(-1, 1.0, entry, exit) == -2.0


def test_fill_delay_selects_post_schedule_quote_and_refuses_stale_quotes():
    quotes = [Quote(95, 1.0, 1.1), Quote(103, 1.1, 1.2), Quote(109, 1.2, 1.3)]
    assert fill_after(quotes, 100, 5).epoch == 103
    with pytest.raises(ValueError, match="executable_quote_missing_within_fill_delay"):
        fill_after(quotes, 100, 2)


def test_account_conversion_uses_bid_for_profit_and_ask_for_loss():
    conversion = Quote(100, 1.2, 1.3)
    assert convert_quote_pnl(10.0, "EUR", "USD", conversion) == 12.0
    assert convert_quote_pnl(-10.0, "EUR", "USD", conversion) == -13.0
    assert convert_quote_pnl(10.0, "USD", "USD", None) == 10.0


def test_financing_is_explicit_only_when_boundaries_are_crossed():
    assert financing_charge(0, -0.5, 100.0) == 0.0
    assert financing_charge(2, -0.5, 100.0) == -100.0
