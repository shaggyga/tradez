# Live input repair checkpoint — October 1

The dashboard and existing nontrading services are running. This checkpoint fixes
the official-event input regression and removes two optional legacy research
loops from the live service profile. It does not claim the entire trading system
is complete.

## What changed

The shared quote reader was restored byte-for-byte to the original source required
by the preserved official-event cohort (SHA-256
`27ea11bc79a3a85cc09f00a88df697f0ecf8bba29b230c309cdf83e8967d8987`).
The optional exact-receipt write-start timestamp now belongs to the opt-in receipt
adapter. It reads only the selected SQLite sequence and verifies matching payload
bytes; missing or changed rows leave that optional timestamp unknown. Original
availability clocks and sealed event bindings were not rewritten.

The explicit boolean `enable_feature_forward_research: false` pauses only
`research_feature_forward_v2` and `research_feature_forward_cached_v2`. These
legacy research comparisons were failing their original clock-proof limit during
long processing. The current profile has 16 services. Collectors, technical inputs,
39 retained-model connections and their 21 source bindings remain unchanged.
Omitting the new flag preserves the previous selection behavior.

The first deployment exposed a supervisor bootstrap bug: paused roles were absent
from the restart-ledger allowlist. The corrected loader preserves their histories,
including exhausted budgets, and still rejects unknown roles. No restart ledger
was cleared. After regression testing, one explicit qualified supervisor launch
restored supervision; the watchdog adopted it with its original retry history.
The existing recovery task is enabled and its expiry remains October 7, 08:14:50 UTC.

## Verification actually performed

- 66 receipt, transport and retained-forecast tests passed.
- 58 native recovery tests passed after the supervisor correction, including real
  PowerShell JSON roundtrip preservation of paused-role histories.
- The deployed profile passed its source/argument/expiry validator.
- At 05:43:12 UTC, `/api/main` returned HTTP 200, 2,149 eligible forecasts across
  39 registered connections, and current currency-news context. Coverage refusals
  were retained: 27 stale, 81 not tradeable, 81 stale quote, 184 parent-origin
  mismatch, 56 missing base, 60 missing exact controls and 14 incomplete curves.
- One fresh supervisor and watchdog were observed; role liveness was current with
  no reported role failures. The official-event horizon worker returned `ok` with
  no errors. Its 16,010 outcome count is cumulative historical evidence.
- The repaired news snapshot was current with no errors. Joint news forecasting
  had 63 warming and five unavailable pairs, with no ready forecasts. Its last
  cumulative input error was `upstream_collector_stale_or_future`; a current
  healthy read does not establish that the intermittent defect is resolved.

Review is substantive same-task review, not independent review. Source diffs,
native consumer behavior, preserved source identities and deployed receipts were
checked. No model fitting, order placement, account changes or other-chat recorder
changes were performed.

## Exact remaining work

Resume `live_news_input_reliability_v1` at collector progress/read reliability.
Use the actual collector heartbeat and joint scheduler events to distinguish a
current refusal from a cumulative historical error. Investigate the long retained
evidence clustering phase and the read-before-observation clock race in
`rolling_news_io_v1._health`. Any frozen-source change needs a qualified successor
and prospective cohort boundary; never re-seal old evidence or loosen freshness
limits. Preserve real warmup and support requirements.

Then resume `retained_management_contract_qualification_v1`: common-terminal
conditional value, explicit position state, costs and risk controls remain
unqualified. No position-management activation is claimed here.

Local evidence: `evidence/news_input_reliability_20261001`. Shared packet:
`NEWS_INPUT_RELIABILITY_20261001`. Git contains runnable source and the versioned
Vault knowledge snapshot; active coordination remains in the shared Vault.
