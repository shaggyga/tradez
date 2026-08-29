from trad.oanda_live_account_readonly_status import executable_closeout_price


def test_long_trade_uses_top_bid_instead_of_deep_closeout_bid():
    price, source = executable_closeout_price(
        {
            "closeoutBid": "110.570",
            "bids": [
                {"price": "110.638", "liquidity": 500000},
                {"price": "110.570", "liquidity": 15000000},
            ],
        },
        145.0,
    )
    assert price == 110.638
    assert source == "top_of_book"


def test_short_trade_uses_top_ask_instead_of_deep_closeout_ask():
    price, source = executable_closeout_price(
        {
            "closeoutAsk": "110.732",
            "asks": [
                {"price": "110.663", "liquidity": 500000},
                {"price": "110.732", "liquidity": 15000000},
            ],
        },
        -145.0,
    )
    assert price == 110.663
    assert source == "top_of_book"


def test_closeout_field_remains_a_compatibility_fallback():
    price, source = executable_closeout_price(
        {"closeoutBid": "1.2345"},
        100.0,
    )
    assert price == 1.2345
    assert source == "deep_closeout_fallback"
