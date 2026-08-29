import csv
from copy import deepcopy
import json
from pathlib import Path
import sqlite3

import pytest

from oanda_spike_blurb_factor_reconstruction import (
    audit_candle_archive,
    audit_legacy_tags,
    load_contract,
)


ROOT = Path(__file__).resolve().parent
CONFIG = ROOT / "config" / "spike_blurb_factor_reconstruction_v1.json"


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _candle(path: Path, instrument: str = "EUR_USD", *, broken: bool = False) -> None:
    fields = [
        "time", "datetime", "instrument", "bid_open", "bid_high", "bid_low", "bid_close",
        "ask_open", "ask_high", "ask_low", "ask_close",
    ]
    rows = []
    for index, timestamp in enumerate(["2026-08-01T00:00:00Z", "2026-08-01T00:01:00Z"]):
        row = {
            "time": timestamp,
            "datetime": timestamp,
            "instrument": instrument,
            "bid_open": "1.1000",
            "bid_high": "1.1010",
            "bid_low": "1.0990",
            "bid_close": "1.1005",
            "ask_open": "1.1002",
            "ask_high": "1.1012",
            "ask_low": "1.0992",
            "ask_close": "1.1007",
        }
        if broken and index == 1:
            row["ask_close"] = ""
        rows.append(row)
    _write_csv(path, fields, rows)


def test_contract_freezes_68_pairs_21_currencies_and_1m_to_30d():
    contract = load_contract(CONFIG)
    assert len(contract["expected_instruments"]) == 68
    assert len(contract["expected_currencies"]) == 21
    assert contract["horizons_minutes"][0] == 1
    assert contract["horizons_minutes"][-1] == 43_200
    assert contract["research_only"] is True
    assert contract["execution_eligible"] is False
    assert contract["can_place_orders"] is False
    assert contract["supported_execution_decision"] == "no_trade"


def test_contract_rejects_any_operational_capability(tmp_path):
    payload = json.loads(CONFIG.read_text(encoding="utf-8"))
    payload["can_place_orders"] = True
    forged = tmp_path / "forged.json"
    forged.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="unsafe_contract_field:can_place_orders"):
        load_contract(forged)


def test_candle_archive_requires_complete_executable_bid_ask(tmp_path):
    _candle(tmp_path / "EUR_USD_M1.csv")
    _candle(tmp_path / "USD_JPY_M1.csv", "USD_JPY", broken=True)
    audit = audit_candle_archive(tmp_path, ["EUR_USD", "USD_JPY", "AUD_USD"])
    assert audit["actual_expected_count"] == 2
    assert audit["complete_file_count"] == 1
    assert audit["missing_instruments"] == ["AUD_USD"]
    jpy = next(row for row in audit["files"] if row["instrument"] == "USD_JPY")
    assert jpy["executable_bid_ask_rows"] == 1
    assert jpy["contract_complete"] is False


def test_legacy_reconciliation_never_invents_missing_price_history(tmp_path):
    candle_root = tmp_path / "candles"
    _candle(candle_root / "EUR_USD_M1.csv")
    candles = audit_candle_archive(candle_root, ["EUR_USD"])
    tags = tmp_path / "tags.csv"
    fields = [
        "move_id", "instrument", "start_utc", "end_utc", "news_match_status",
        "primary_predictive_eligible", "primary_expected_pair_direction",
        "primary_source_verified", "primary_causal_relation",
    ]
    _write_csv(
        tags,
        fields,
        [
            {
                "move_id": "inside", "instrument": "EUR_USD",
                "start_utc": "2026-08-01T00:00:00Z", "end_utc": "2026-08-01T00:01:00Z",
                "news_match_status": "matched", "primary_predictive_eligible": "True",
                "primary_expected_pair_direction": "LONG", "primary_source_verified": "True",
                "primary_causal_relation": "PRE_MOVE",
            },
            {
                "move_id": "outside", "instrument": "EUR_USD",
                "start_utc": "2025-08-01T00:00:00Z", "end_utc": "2025-08-01T00:01:00Z",
                "news_match_status": "matched", "primary_predictive_eligible": "True",
                "primary_expected_pair_direction": "SHORT", "primary_source_verified": "True",
                "primary_causal_relation": "PRE_MOVE",
            },
        ],
    )
    audit = audit_legacy_tags(tags, candles["files"])
    assert audit["within_current_candle_range_rows"] == 1
    assert audit["requires_historical_price_reacquisition_rows"] == 1
    assert audit["directional_mapping_rows"] == 2
    assert audit["evidence_class"] == "outcome_selected_diagnostic_not_forecast_proof"


def test_contract_material_change_cannot_reuse_pair_currency_mismatch(tmp_path):
    payload = deepcopy(json.loads(CONFIG.read_text(encoding="utf-8")))
    payload["expected_currencies"].remove("ZAR")
    payload["expected_currencies"].append("XXX")
    forged = tmp_path / "forged.json"
    forged.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="pair_currency_universe_mismatch"):
        load_contract(forged)
