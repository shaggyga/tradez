"""Summarize captured watch evidence; reads no live production state."""
import json
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
rows = [json.loads(line) for line in (ROOT / "samples.jsonl").read_text().splitlines() if line.strip()]

def values(*path):
    found = []
    for row in rows:
        value = row
        for key in path:
            value = value.get(key) if isinstance(value, dict) else None
        if value is not None:
            found.append(value)
    return found

def numeric(*path):
    result = [x for x in values(*path) if isinstance(x, (int, float)) and not isinstance(x, bool)]
    return {"n": len(result), "min": min(result), "max": max(result), "median": statistics.median(result), "first": result[0], "last": result[-1]} if result else None

first, last = rows[0], rows[-1]
fresh_counts = Counter(q["pair"] for row in rows for q in row["quotes"]["pairs"] if q.get("fresh"))
last_pairs = {q["pair"]: q for q in last["quotes"]["pairs"]}
pair_stats = []
for pair in sorted(last_pairs):
    observations = [q for row in rows for q in row["quotes"]["pairs"] if q["pair"] == pair and q.get("fresh")]
    a, b = (observations[0], observations[-1]) if observations else (None, None)
    pair_stats.append({"pair": pair, "fresh_samples": fresh_counts[pair], "sample_count": len(rows),
        "last_fresh": last_pairs[pair].get("fresh"),
        "first_fresh_quote_utc": a.get("market_time") if a else None,
        "last_fresh_quote_utc": b.get("market_time") if b else None,
        "mid_change_bps": (b["mid"] / a["mid"] - 1) * 10000 if a and b else None,
        "last_spread_bps": b.get("spread_bps") if b else None})
clocks = values("clock")
outcome_path = ROOT / "latest_outcomes.json"
outcomes = json.loads(outcome_path.read_text()) if outcome_path.exists() else {}
summary = {
    "generated_utc": datetime.now(timezone.utc).isoformat(),
    "watch_complete": (ROOT / "completed.json").exists(),
    "sample_count": len(rows), "first_sample_utc": first["sample_utc"], "last_sample_utc": last["sample_utc"],
    "read_error_samples": [r["sample_utc"] for r in rows if r["read_errors"]],
    "fresh_quotes": numeric("quotes", "fresh_tradeable"),
    "quote_snapshot_age_sec": numeric("quotes", "snapshot_age_sec"),
    "quote_stream_age_sec": numeric("quote_stream", "age_sec"),
    "quote_stream_pids": sorted(set(values("quote_stream", "pid"))),
    "clock_issue_samples": sum(c.get("status") != "ok" or c.get("source_fresh") is not True or c.get("clock_discontinuity_active") is True for c in clocks),
    "m1_report_age_sec": numeric("m1", "report_age_sec"),
    "m1_error_count": numeric("m1", "report", "error_count"),
    "feature_publication_age_sec": numeric("features", "last_success_age_sec"),
    "rich_fresh_pairs": numeric("features", "readiness", "rich_M1_fresh_pairs"),
    "latest_feature_readiness": last["features"]["readiness"],
    "price_forecast_pairs": numeric("price", "pairs_with_forecast"),
    "price_heartbeat_age_sec": numeric("price", "age_sec"),
    "news_snapshot_age_sec": numeric("news", "age_sec"),
    "directional_news_pairs": numeric("news", "coverage", "directional_pair_count"),
    "news_repair_errors": numeric("news_repair", "errors"),
    "news_repair_last_error": last["news_repair"].get("last_error"),
    "joint_heartbeat_age_sec": numeric("joint", "age_sec"),
    "forward_counts_first": first["forward"]["counts"], "forward_counts_last": last["forward"]["counts"],
    "open_positions": numeric("account", "openTradeCount"),
    "pending_orders": numeric("account", "pendingOrderCount"),
    "account_nav": numeric("account", "nav"),
    "account_age_sec": numeric("account", "age_sec"),
    "disk_free_gib": numeric("disk_free_gib"),
    "latest_outcome_audit": {k: outcomes.get(k) for k in ["captured_as_of_utc", "counts", "family_counts", "exclusion_reasons", "errors"]},
    "pairs": pair_stats,
    "limits": ["Local file observations; no broker orders or execution changes.", "Quote freshness means tradeable bid/ask with market age <=30 seconds.", "First automated sample follows initial manual observation; quote changes use each pair's first and last fresh samples.", "Settlement counts test recording and timing, not profitability or forecast accuracy."]
}
(ROOT / "watch_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
print(json.dumps({k: summary[k] for k in ["watch_complete", "sample_count", "last_sample_utc", "fresh_quotes", "quote_stream_age_sec", "clock_issue_samples", "m1_error_count", "price_forecast_pairs", "rich_fresh_pairs", "news_repair_errors", "account_nav", "read_error_samples"]}))
