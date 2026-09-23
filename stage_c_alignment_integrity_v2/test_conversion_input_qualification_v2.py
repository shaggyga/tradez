from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import conversion_input_qualification_v2 as conversion

ORIGIN, TARGET = 1719187200, 1719273600


def row(name: str, bid: float = 1.1, ask: float = 1.2) -> dict:
    return {"instrument": name, "origin": {"status": "valid_candle_close_pair", "raw_bar_start_epoch": ORIGIN,
                                           "bid_close": bid, "ask_close": ask},
            "target": {"status": "valid_candle_close_pair", "raw_bar_start_epoch": TARGET,
                       "bid_close": bid, "ask_close": ask}}


def test_direct_and_inverse_rules_use_asymmetric_candle_sides():
    direct = conversion.conversion_route("EUR", "origin", ORIGIN, {"EUR_USD": row("EUR_USD")})
    assert direct["rate_available"]
    assert direct["direction"] == "direct"
    assert direct["positive_or_zero_amount_rate"] == {"expression": "bid_close", "source_side": "bid_close"}
    assert direct["negative_amount_rate"] == {"expression": "ask_close", "source_side": "ask_close"}
    inverse = conversion.conversion_route("JPY", "target", TARGET, {"USD_JPY": row("USD_JPY", 150.0, 151.0)})
    assert inverse["rate_available"]
    assert inverse["direction"] == "inverse"
    assert inverse["positive_or_zero_amount_rate"] == {"expression": "1 / ask_close", "source_side": "ask_close"}
    assert inverse["negative_amount_rate"] == {"expression": "1 / bid_close", "source_side": "bid_close"}


def test_identity_is_reserved_for_usd_and_missing_routes_do_not_get_one():
    identity = conversion.conversion_route("USD", "origin", ORIGIN, {})
    assert identity["status"] == "usd_identity"
    assert identity["positive_or_zero_amount_rate"]["expression"] == "1"
    missing = conversion.conversion_route("XYZ", "origin", ORIGIN, {})
    assert not missing["rate_available"]
    assert missing["status"] == "missing_direct_or_inverse_usd_instrument"
    assert "positive_or_zero_amount_rate" not in missing


def test_invalid_direct_source_is_not_silently_replaced_by_inverse():
    direct = row("EUR_USD")
    direct["origin"]["status"] = "missing_exact_bar"
    result = conversion.conversion_route("EUR", "origin", ORIGIN, {"EUR_USD": direct, "USD_EUR": row("USD_EUR")})
    assert not result["rate_available"]
    assert result["conversion_instrument"] == "EUR_USD"
    assert result["status"] == "conversion_candle_pair_unavailable"


@pytest.mark.parametrize("bid,ask", [(float("inf"), 1.2), (1.1, float("nan")), (-1.0, 1.2), (1.3, 1.2)])
def test_declared_available_rows_still_require_valid_prices(bid: float, ask: float):
    result = conversion.conversion_route("EUR", "origin", ORIGIN, {"EUR_USD": row("EUR_USD", bid, ask)})
    assert not result["rate_available"]
    assert result["status"] == "invalid_conversion_candle_prices"


def test_conversion_requires_the_same_exact_clock():
    quote = row("USD_JPY")
    quote["origin"]["raw_bar_start_epoch"] -= 60
    result = conversion.conversion_route("JPY", "origin", ORIGIN, {"USD_JPY": quote})
    assert not result["rate_available"]
    assert result["status"] == "conversion_clock_mismatch"


def fixture_report(root: Path) -> tuple[Path, str]:
    rows = [row("USD_CAD"), row("EUR_USD"), row("EUR_CAD"), row("USD_JPY", 150.0, 151.0)]
    rows.extend(row("F" + chr(65 + index // 26) + chr(65 + index % 26) + "_USD") for index in range(64))
    rows[2]["target"] = {"status": "missing_exact_bar", "raw_bar_start_epoch": TARGET}
    report = {"schema_version": "forex_retained_quote_input_qualification.v2", "input_tier": "synthetic_contract_fixture.v2",
              "source_integrity": {"member_sha256_and_size_verified_count": 68, "exact_archive_member_inventory_verified": True},
              "clock_contract": {"origin_epoch": ORIGIN, "target_epoch": TARGET}, "instruments": rows}
    path = root / "quotes.json"
    raw = conversion.encoded(report)
    path.write_bytes(raw)
    return path, hashlib.sha256(raw).hexdigest()


def test_external_report_digest_rejects_tampered_prices(tmp_path: Path):
    path, expected = fixture_report(tmp_path)
    assert len(conversion.load_verified_report(path, expected)["instruments"]) == 68
    value = json.loads(path.read_bytes())
    value["instruments"][0]["origin"]["bid_close"] = 99.0
    path.write_bytes(conversion.encoded(value))
    with pytest.raises(ValueError, match="quote_report_sha256_mismatch"):
        conversion.load_verified_report(path, expected)


def test_all68_explicit_candle_and_conversion_coverage_are_distinct(tmp_path: Path, monkeypatch):
    path, expected = fixture_report(tmp_path)
    predecessor = tmp_path / "fixture_predecessor.py"
    predecessor.write_bytes(b"test fixture for source identity binding\n")
    monkeypatch.setattr(conversion, "INSPECTED_PREDECESSOR_SHA256", hashlib.sha256(predecessor.read_bytes()).hexdigest())
    result = conversion.qualify(path, expected, predecessor)
    assert len(result["instruments"]) == 68
    assert result["counts"]["origin"]["quote_currency_conversion_available_instruments"] == 68
    assert result["counts"]["target"]["quote_currency_conversion_available_instruments"] == 68
    assert result["counts"]["target"]["candle_and_conversion_available_instruments"] == 67
    missing = next(item for item in result["instruments"] if item["instrument"] == "EUR_CAD")
    assert missing["target"]["quote_currency_conversion"]["rate_available"]
    assert not missing["target"]["instrument_candle_close_available"]
    assert result["research_account_currency"] == "USD"
    assert not result["archive_reread"] and not result["execution_ready"]
    predecessor.write_bytes(b"changed rules\n")
    with pytest.raises(ValueError, match="accounting_predecessor_changed"):
        conversion.qualify(path, expected, predecessor)


def test_duplicate_instrument_even_with_recomputed_report_hash_is_rejected(tmp_path: Path):
    path, _ = fixture_report(tmp_path)
    report = json.loads(path.read_bytes())
    report["instruments"][-1] = report["instruments"][0]
    raw = conversion.encoded(report)
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="unique_all68"):
        conversion.load_verified_report(path, hashlib.sha256(raw).hexdigest())
