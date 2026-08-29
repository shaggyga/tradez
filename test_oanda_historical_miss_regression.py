import json
from pathlib import Path

from trad.oanda_historical_miss_regression import replay_contract


def test_ten_case_contract_replays_without_reclassifying_history():
    path = Path(__file__).resolve().parent / "config" / "historical_miss_cases_v1.json"
    result = replay_contract(json.loads(path.read_text(encoding="utf-8")))
    assert result["status"] == "pass"
    assert result["case_count"] == 10
    assert result["classification_counts"] == {
        "bad_entry": 7, "giveback": 2, "invalid_maturity": 1
    }
    assert result["historical_records_rewritten"] is False
    assert result["execution_authorized"] is False
    assert all(len(value) == 64 for row in result["cases"] for value in row["hashes"].values())
