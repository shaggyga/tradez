"""Prespecified synthetic stress paths; rerun admissions and all ledger events."""
from __future__ import annotations

import argparse
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from accounting_event_fixtures_v2 import lifecycle_fixture
from accounting_event_runner_v2 import run
from reference_accounting_adapter_v2 import DEFAULT_TRAD
from publication import sha256_file


def scenarios(trad_root=DEFAULT_TRAD):
    contract, events = lifecycle_fixture(trad_root)
    choices = {}
    choices["baseline"] = (deepcopy(contract), deepcopy(events))
    for name in ("fees_10000x", "capital_100", "financing_5x", "latency_120s", "entry_quote_outage", "entry_spread_wide"):
        c, e = deepcopy(contract), deepcopy(events)
        if name == "capital_100":
            c["initial_capital_usd"] = "100"
        elif name == "latency_120s":
            c["reference_config"]["execution_delay_sec"] = 120
        for row in e:
            if name == "fees_10000x" and row["kind"] == "fill":
                row["fee_usd"] = str(Decimal(row["fee_usd"]) * 10000)
            elif name == "financing_5x" and row["kind"] == "financing":
                for rates in row["rates"].values():
                    for side in rates:
                        rates[side] = str(Decimal(rates[side]) * 5)
            elif name == "entry_quote_outage" and row["epoch"] == 1060:
                row["quotes"].pop("EUR_USD")
            elif name == "entry_spread_wide" and row["epoch"] == 1060:
                row["quotes"]["EUR_USD"].update(bid="1.0850", ask="1.1150")
        choices[name] = (c, e)
    return choices


def run_scenarios(runs_dir, prefix, *, trad_root=DEFAULT_TRAD):
    records = []
    for name, (contract, events) in scenarios(trad_root).items():
        run_id = f"{prefix}-{name.replace('_','-')}"
        receipt = run(contract, events, run_id=run_id, runs_dir=runs_dir, trad_root=trad_root, resume=True)
        root = runs_dir / run_id
        report = json.loads((root / "run_report.json").read_text())
        ledger = [json.loads(line) for line in (root / "event_ledger.jsonl").read_text().splitlines()]
        filled = sum(row["receipt"]["status"] == "filled" for row in ledger)
        rejected = sum(row["receipt"]["status"] == "rejected" for row in ledger)
        records.append({"scenario": name, "run_id": run_id, "identity": receipt["identity"],
                        "completion_sha256": sha256_file(root / "COMPLETION_MANIFEST.json"),
                        "filled_events": filled, "rejected_events": rejected, "arms": report["arms"],
                        "independent_audit": report["accounting_oracle"]})
    baseline = records[0]
    for record in records:
        record["fill_path_differs_from_baseline"] = (record["filled_events"], record["rejected_events"]) != (baseline["filled_events"], baseline["rejected_events"])
    return {"schema_version": "forex_synthetic_full_path_stress.v2", "status": "completed_synthetic_stress_not_policy_evidence",
            "stress_source_sha256": sha256_file(Path(__file__)), "prespecified_scenario_count": len(records),
            "attempted": len(records), "completed": len(records), "failed": 0, "scenarios": records,
            "scope": "same_predeclared_intents_with_all_admission_fill_and_accounting_events_replayed",
            "limitations": ["not_a_model_conditioned_policy_reoptimization", "not_historical_cost_estimation",
                            "explicit_fills_may_be_rejected_under_stress; remaining_positions_and_orders_are_reported",
                            "synthetic_profitability_is_not_market_evidence"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-dir", type=Path, default=ROOT / "runs")
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trad-root", type=Path, default=DEFAULT_TRAD)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("stress_report_is_immutable")
    report = run_scenarios(args.runs_dir, args.prefix, trad_root=args.trad_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, sort_keys=True, indent=2, allow_nan=False)
        handle.write("\n")
    print(json.dumps({"completed": report["completed"], "path_changed": [row["scenario"] for row in report["scenarios"] if row["fill_path_differs_from_baseline"]]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
