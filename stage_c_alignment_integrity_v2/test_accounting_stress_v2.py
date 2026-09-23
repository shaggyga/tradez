from decimal import Decimal
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))
from accounting_stress_v2 import run_scenarios


def test_stress_replays_change_admission_and_fill_paths_not_just_final_fees(tmp_path):
    report = run_scenarios(tmp_path, "stress")
    assert report["attempted"] == report["completed"] == 7
    assert report["failed"] == 0
    rows = {row["scenario"]: row for row in report["scenarios"]}
    assert rows["baseline"]["filled_events"] == 6
    assert rows["baseline"]["rejected_events"] == 0
    for name in ("fees_10000x", "capital_100", "latency_120s", "entry_quote_outage", "entry_spread_wide"):
        assert rows[name]["fill_path_differs_from_baseline"]
        assert rows[name]["filled_events"] < rows["baseline"]["filled_events"]
        assert rows[name]["rejected_events"] > 0
    assert rows["capital_100"]["filled_events"] == 0
    assert rows["capital_100"]["arms"]["hold"]["gross_close_profit_factor"] is None
    assert not rows["financing_5x"]["fill_path_differs_from_baseline"]
    assert Decimal(rows["financing_5x"]["arms"]["hold"]["financing_usd"]) == -5
    assert Decimal(rows["financing_5x"]["arms"]["rotate"]["financing_usd"]) == -3
    for row in rows.values():
        assert row["independent_audit"]["status"] == "verified"
        assert row["independent_audit"]["event_arm_rows_checked"] == 40
