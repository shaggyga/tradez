"""Write a factual report from captured evidence; no production operations."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parent
s = json.loads((ROOT / "watch_summary.json").read_text())
c = s["latest_outcome_audit"].get("counts") or {}
def n(key, field="last"):
    return (s.get(key) or {}).get(field, "unavailable")
state = "COMPLETED" if s["watch_complete"] else "IN PROGRESS"
text = f"""# Live Forex watch — {state}

Requested interval: September 13, 2026, 18:20:12–19:05:12 America/New_York (22:20:12–23:05:12 UTC).
Initial observation was manual. Automated 15-second sampling began at {s['first_sample_utc']}; latest capture: {s['last_sample_utc']}. Captured samples: {s['sample_count']}.

## Verdict

Price collection and price-only H1 research remained active. The full feature/news/model operation is degraded and cannot be described as fully operational. The supervisor remained in research-only mode; the watch made no production changes or order requests.

## Captured health

| Check | Observation |
|---|---|
| Fresh tradeable quotes (market age <=30 seconds) | {n('fresh_quotes','min')}–{n('fresh_quotes','max')} of 68; latest {n('fresh_quotes')} |
| Quote-stream heartbeat age | Maximum {n('quote_stream_age_sec','max')} seconds |
| Clock issue samples | {s['clock_issue_samples']} |
| M1 updater reported error count | Maximum {n('m1_error_count','max')} |
| Current price-only H1 coverage | {n('price_forecast_pairs','min')}–{n('price_forecast_pairs','max')} pairs; two model families |
| Fresh rich M1 features | {n('rich_fresh_pairs','min')}–{n('rich_fresh_pairs','max')} pairs |
| Directional general-news coverage | {n('directional_news_pairs','min')}–{n('directional_news_pairs','max')} pairs |
| News-repair cumulative errors | {n('news_repair_errors','first')} to {n('news_repair_errors')} |
| Open positions / pending orders | {n('open_positions')} / {n('pending_orders')} |
| Local practice-account NAV | ${n('account_nav','first')} to ${n('account_nav')} |
| Free disk space | Minimum {n('disk_free_gib','min')} GiB |
| Failed sample reads | {len(s['read_error_samples'])} |

## Sunday settlement audit

Read-only inspection of 136 price-model ledgers, as of {s['latest_outcome_audit'].get('captured_as_of_utc')}:

- {c.get('published', 0)} Sunday-session forecasts published; {c.get('consumed', 0)} consumed.
- {c.get('published_target_due', 0)} published targets due: {c.get('due_with_outcome', 0)} outcomes, {c.get('due_explicitly_excluded', 0)} explicit exclusions, {c.get('due_unresolved', 0)} unresolved.
- {c.get('due_plus_grace_overdue_unresolved', 0)} unresolved beyond target grace; {c.get('entry_deadline_overdue_unresolved', 0)} unresolved beyond entry deadline.
- {c.get('records_with_validation_issues', 0)} receipt/hash/clock validation issues; {len(s['latest_outcome_audit'].get('errors') or [])} read errors.

These are Sunday-session cumulative recording checks, not trade results, independent trials, or a new profitability estimate. Friday outcomes are excluded. Explicit quote exclusions must remain separate from losses or wins.

## Confirmed operational gaps

1. The supervisor launches news repair v1 against guard-v2 collector output. It repeatedly fails `guarded_member_evidence_missing`. Existing repair/reconciliation v2 should be evaluated before creating duplicate work.
2. Joint model v3 rejects the collector's changed source binding before startup. All 68 retained joint ledgers contained zero Sunday publications at the independent check. Existing joint v5 is present, but its admitted registration also requires collector-source reconciliation; switching its name alone is insufficient.
3. Rich features and structural vectors stayed unavailable. The earlier source audit found missing native M5/H1 files and contiguous-minute warm-up gaps. The observation feed also has no admitted news/model adapter. A fresh publication timestamp does not make these fields ready.
4. The forward evaluator accumulates pair outcomes while excluding all feature events. Its history loader hits the expanded-data cap; preserve this distinction in performance reporting.
5. Official release workers are current, but NZD's mapped OCR source is unsupported (retained HTTP 403 errors). Official fast-mapped research has zero publish-eligible candidates, and that ledger is separate from the current joint adapter.
6. The integrity monitor reports degraded runtime health and still classifies the two newer feature workers as unexpected. Supervisor routing and its expected-worker contract need reconciliation together.
7. The 23:00 price-only refresh showed a scheduling delay: one new publication by the 23:04:08 audit, despite 62 ready source-capture completions since 23:00. Changed-source captures take priority over fits in the single-future scheduler and defer fitting until the next full pair rotation. This is evidence of fit delay and a starvation risk, not a complete deadlock. The older H1 forecasts remained current; their presence must not be mistaken for a completed new refresh.

## Evidence and limits

- `samples.jsonl`, `watch_summary.json`, and `completed.json` record the monitoring interval and measurements. A missing completed.json means this report remains provisional.
- `latest_outcomes.json` and timestamped outcome audits retain the forecast-level read-only settlement evidence.
- `news_joint_recovery_audit_20260913_2235.json` documents existing successors and source mismatches.
- `official_release_live_audit_20260913_2240.json` documents source coverage, mapping clocks, and live-input separation.
- `quote_cadence_audit_20260913_2250.json` documents continuous connection, advancing accepted updates, and the recovery of temporarily old per-pair quotes. Connection heartbeats do not refresh quote timestamps.
- `runtime_checkpoint_2235.json` records the independent process snapshot and HTTP-200 dashboard response; `integrity_checkpoint_2238.json` records the project's own degraded status.
- `closing_operational_checkpoint_20260913_2300.json` confirms unchanged supervisor/core process IDs, research-only mode, dashboard HTTP 200, current official-worker heartbeats, and persistent news/joint failures at approximately 23:01 UTC.
- Local account snapshots remained current; no independent order placement or management was exercised. Fifteen-second file sampling can miss shorter incidents. Stable collection does not establish predictive or trading performance.
"""
(ROOT / "WATCH_REPORT.md").write_text(text, encoding="utf-8")
print(json.dumps({"report": str(ROOT / "WATCH_REPORT.md"), "state": state, "last_sample_utc": s['last_sample_utc']}))
