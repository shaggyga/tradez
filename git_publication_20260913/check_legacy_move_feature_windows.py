"""Bounded synthetic audit of one legacy diagnostic; no project imports or data."""
import ast
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

SOURCE = Path(r"C:\Users\zmoor\Documents\forex\trad\oanda_move_first_news_case_audit.py")
OUTPUT = Path(__file__).with_name("LEGACY_MOVE_FEATURE_WINDOW_DIAGNOSTIC.json")
raw = SOURCE.read_bytes()
tree = ast.parse(raw, filename=str(SOURCE))
nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
         and node.name == "causal_m1_technical_state"]
assert len(nodes) == 1
namespace = {"dt": dt, "Any": Any, "Mapping": Mapping, "Sequence": Sequence}
exec(compile(ast.Module(body=nodes, type_ignores=[]), str(SOURCE), "exec"), namespace)
function = namespace["causal_m1_technical_state"]
start = dt.datetime(2026, 1, 5, 12, tzinfo=dt.timezone.utc)

def fixture(cadence_minutes):
    rows = []
    for index in range(61):
        mid = 100 + index * 0.1
        rows.append({
            "timestamp": start + dt.timedelta(minutes=index * cadence_minutes),
            "bid_open": mid - .005, "ask_open": mid + .005,
            "bid_high": mid + .005, "ask_high": mid + .015,
            "bid_low": mid - .015, "ask_low": mid - .005,
        })
    decision = rows[-1]["timestamp"] + dt.timedelta(minutes=1)
    result = function(rows, decision_time=decision, pip_size=.01)
    return rows, result

cases = []
for cadence in (1, 2):
    rows, result = fixture(cadence)
    assert result["state"] == "available"
    for label in (5, 15, 60):
        elapsed = (rows[-1]["timestamp"] - rows[-1-label]["timestamp"]).total_seconds() / 60
        cases.append({"synthetic_bar_cadence_minutes": cadence,
                      "reported_field": f"return_{label}m_pips",
                      "reported_value": result[f"return_{label}m_pips"],
                      "actual_endpoint_span_minutes": elapsed,
                      "label_matches_elapsed_span": elapsed == label})
    if cadence == 1:
        mids = [(r["bid_open"] + r["ask_open"]) / 2 for r in rows]
        true_ranges = []
        for i in range(len(rows)-14, len(rows)):
            high = (rows[i]["bid_high"] + rows[i]["ask_high"]) / 2
            low = (rows[i]["bid_low"] + rows[i]["ask_low"]) / 2
            # Synthetic bars close at their open, so previous mid is also previous close.
            true_ranges.append(max(high-low, abs(high-mids[i-1]), abs(low-mids[i-1])) / .01)
        atr_case = {"reported_atr14_pips": result["atr14_pips"],
                    "synthetic_simple_average_true_range14_pips": sum(true_ranges) / 14}

assert all(case["label_matches_elapsed_span"] for case in cases[:3])
assert not any(case["label_matches_elapsed_span"] for case in cases[3:])
assert abs(atr_case["reported_atr14_pips"] - 2) < 1e-6
assert abs(atr_case["synthetic_simple_average_true_range14_pips"] - 11) < 1e-6
report = {"schema_version": "legacy_move_feature_window_diagnostic_v1",
          "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
          "source": str(SOURCE), "source_sha256": hashlib.sha256(raw).hexdigest(),
          "function": nodes[0].name, "source_line": nodes[0].lineno,
          "scope": "synthetic pure-function extraction only; no project imports, market data, models or services",
          "window_cases": cases, "range_naming_case": atr_case,
          "conclusion": "Legacy diagnostic accepts irregular bars but labels row offsets as minute windows; atr14 is mean high-low range, omitting previous-close gaps.",
          "limitations": "Does not establish historical occurrence or applicability to the separate current pair-local/joint learner. No source was changed."}
OUTPUT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report, indent=2))
