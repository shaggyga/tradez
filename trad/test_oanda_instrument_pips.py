import json
from pathlib import Path

import oanda_instrument_pips as pips


ROOT = Path(__file__).resolve().parent


def test_known_nonstandard_pip_locations_are_not_quote_currency_guesses():
    assert pips.fallback_pip_size("USD_HUF") == 0.01
    assert pips.fallback_pip_size("EUR_HUF") == 0.01
    assert pips.fallback_pip_size("USD_THB") == 0.01
    assert pips.fallback_pip_size("HKD_JPY") == 0.0001
    assert pips.fallback_pip_size("EUR_USD") == 0.0001


def test_live_68_pair_quote_metadata_matches_shared_contract():
    path = ROOT / "data" / "oanda_training_manager" / "state" / "practice_007_market_quotes_v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    quotes = payload["quotes"]
    assert len(quotes) == 68
    assert all(
        float(row["pip"]) == pips.fallback_pip_size(instrument)
        for instrument, row in quotes.items()
    )


def test_venue_metadata_overrides_fallback_when_explicit():
    assert pips.resolve_pip_size("USD_HUF", {"pip": 0.001}) == 0.001
    assert pips.resolve_pip_size("EUR_USD", {"pipLocation": -5}) == 0.00001
