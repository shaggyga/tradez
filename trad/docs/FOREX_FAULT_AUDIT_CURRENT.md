# Forex fault audit — independent handoff

Prepared **September 4, 2026, 22:19 ET**, after intentional runtime shutdown.
Canonical root: `C:\Users\zmoor\Documents\forex\trad`.
Detailed findings, exact paths and SHA-256 bindings are in source-root
`FOREX_FAULT_AUDIT_CURRENT.json`, copied to vault-root `FAULT_AUDIT_CURRENT.json`.

## Bottom line

**Three code defects are confirmed by isolated reproductions and remain pending.**
There is also a frozen source-response branch with no compatible current-classifier
intake and no prospective proof. The project remains stopped and supports
`no_trade`; no order or execution-gate change was made by this audit.

This is a bounded factual fault audit for the next independent reviewer—not a
claim that every historical module is correct. The initial 88-item validation predates these findings. The owner registered
all three at **02:20 UTC September 5**. The reconciled **91-item register
validates with three open repairs**.

| Finding | Priority | Confirmed consequence |
|---|---|---|
| FASTLANE-PRECOMMIT-CLOCK | P1 | A delayed commit can make claimed availability earlier than durable visibility; the checker still passes. |
| FASTLANE-BUILDING-PUBLICATION | P2 | A normal progress update replaces the committed state and triggers a degraded integrity publication. |
| FASTLANE-RETRY-CURSOR | P2 | A retry after a progressed cursor resets to activation and rereads old rows. |

Report IDs begin `FX-AUDIT-20260904-`. Corresponding issue IDs are
`FX-20260905-FASTLANE-DURABLE-AVAILABILITY`,
`FX-20260905-FASTLANE-PUBLICATION-STATE` and
`FX-20260905-FASTLANE-RETRY-CURSOR`.

## 1. P1 — availability is timestamped before commit

**Observed.** In `oanda_source_governance_news_fast_lane.py:340`, availability
is sampled as UTC plus ten seconds **before** receipt insertion (line 352) and
commit (line 376). A virtual-clock miniature SQLite fixture exercised the
production `now=None` branch with an eleven-second commit delay. The receipt
claimed **13:15:12**, but commit completed **13:15:13**; a separate read-only
connection still saw no receipt immediately before commit.

The final worker state was `ok`. The exact integrity checker also passed,
with zero optimistic/operational clock violations: its checks at
`oanda_project_integrity_audit.py:4473` and `:4515` compare availability
against raw first-seen time and the view carrying the same claim, not against
independently established committed visibility.

**Expected and impact.** Operational availability must not precede durable
visibility. Otherwise a later causal replay can credit information earlier
than it became usable. This demonstrates a contract weakness; it **does not**
show that a historical production receipt experienced such a delay, that a
forecast used it, or that an order occurred.

**Next step.** Design a separate append-only commit/publication attestation
whose timestamp follows durable commit and whose visibility is required by
consumers. Add a slow-commit regression and independent verification. Inspect
existing traces before assessing historical impact; do not rewrite immutable
receipts or award retroactive evidence credit.

## 2. P2 — progress publication masquerades as broken evidence

**Observed.** `oanda_source_governance_news_fast_lane.py:289`–297 replaces the
only state JSON with `status=building`, without the completed cutoff, receipt
counts or committed flag. The consumer at
`oanda_project_integrity_audit.py:4308` and `:4322` requires a valid cutoff,
`status=ok` and committed state.

On the same healthy miniature database, the previous completed state passed
the exact checker, while the actual captured building publication failed.
The saved **21:45:33 ET** project-integrity report has this precise pattern:
null cutoff, zero state receipts, 9,991 database receipts, and the sole failure
`news_source_governance_fast_lane_current_prospective_and_inert`.

**Expected and impact.** Progress telemetry should not replace the last
completed integrity snapshot. This causes false degraded attestations and can
leave ambiguous state after interruption. It is fail-closed—not an execution
bypass—and the count mismatch alone does not prove database corruption.

**Next step.** Separate heartbeat/progress from the committed snapshot, or
capture/retry completed publication under a bounded contract. Preserve strict
freshness and actual failure checks. Add an interleaved producer/consumer
regression; retain the historical degraded report rather than relabelling it
healthy.

## 3. P2 — error state discards the progressed cursor

**Observed.** Resume at `oanda_source_governance_news_fast_lane.py:255`–263
uses the prior `scan_started_utc` only when `status=ok`. Error/building
payloads replace the same file. A fixture progressed to **13:15:32**, injected
either an input lock or registry lock, then retried. The next actual article
query reset to **13:15:00 activation** and reread a previously processed row.
Restarting from an interrupted building publication had the same result.
Receipt deduplication prevented a new duplicate receipt.

The existing lock test at
`test_oanda_source_governance_news_fast_lane.py:110` tests only the first
failure from activation, not a second run after an already-progressed cursor.

**Expected and impact.** Retry must preserve the last committed cursor without
advancing past unread rows or replaying the whole cohort. Repeated resets add
historical scan cost, contention and latency. No skipped row or duplicate
immutable receipt was observed in these fixtures.

**Next step.** Keep a cohort-bound committed cursor independently of progress
and error status. Add success → input/registry error → retry and interrupted
building regression cases.

## Source, model and evidence limits—not hidden successful research

**Frozen source-response intake.** The last completed fast-mapper heartbeat
at 22:08:02 ET is bound to **V164**. V8's loader explicitly accepts **V152 only**
(`oanda_causal_source_factor_response_map_v8.py:48`, `:89`); rank V7
requires V8 proof (`oanda_source_conditioned_currency_rank_v7.py:46`).
V8 retains 13 preactivation diagnostic events and **zero prospective proof
events or forecasts**; rank V7 has **zero decisions or independent source
episodes**.

This is **not a lineage breach**: the frozen binding correctly prevents mixing
new classifier evidence into an old cohort. But fresh V164 mappings cannot
advance this specific branch simply by restarting it. Either label V8/V7
frozen baselines or design a separately parented prospective successor.
Do not change V8 identities or loosen its exact-version gate.

**Source coverage.** Configuration lists 192 news sources. The saved governance
publication has 193 source cards including separate populations, 182 operational
cards and 12 degraded/missing cards; these are different denominators. Missing
or disabled populations include GDELT, direct RBNZ policy/rates/reserves,
credential-gated Trading Economics, LSEG/CME and others listed in the JSON.
`not_due` is not itself an outage. Source counts do not establish usable
current content or incremental predictive value.

**Declared breadth.** The August 3 feature JSON is design inventory.
`config/shadow_runtime_retirements_v1.json` explicitly marks unavailable
book/depth inputs, 30 dormant model-gap contributors, stale excluded rule
artifacts, and allocator collection paused on an empty confirmed set.
The July 18 dependency report is not a current package audit. These are
preserved limitations, not repaired functionality or profitable alpha.

**Proof position.** The saved registry has 16 cohorts/16 transitions and four
active family identities matching the predictor snapshot. Lifecycle remains
**0 confirmed**, 43,872 collecting and 9,485 futility-rejected. No full ledger
genealogy rescan was performed. An eventual practice entry still requires
independent lifecycle confirmation plus exact fresh canary authorization.

## Expected shutdown effects and reviewed documentation

The earlier missing supervisor/watchdog incident and brief owner recovery are
recorded in [the stopped-state audit](AUDIT_STATE_CURRENT.md). Final observed
runtime was zero canonical workers/supervisors/watchdogs; the owner confirmed
no dashboard listener and disabled the Forex logon restart task. Growing ages
and frozen `running`/`current` flags in saved files are expected after this
intentional stop, not evidence that workers remain alive. Do not restart as
part of reading this report.

The new README/orientation explicitly separate current stopped records from
legacy history. Ten local Markdown targets across five current entry documents
were checked: **zero missing targets**. This does not certify every historical
path, archive member or remote link.

## Verification and completeness boundary

This audit ran **54 passing offline tests**: 14 across V8, rank V7 and the
issue-register validator, 32 proof-lineage/signed-currency-exposure tests, and
eight existing fast-lane tests. The reconciled 91-item register validated
with zero errors and three open repairs. Three
additional deterministic miniature-database experiments demonstrated the
defects above, including input/registry/interrupted-building cursor variants.
They used no production ledgers, network or real timed delay. The canonical
checker and helpers were AST-extracted; worker/governance were imported without
runtime main. Reproduce from `trad` with
`python -B tools/forex_fastlane_fault_reproduction.py`.
Exit zero means diagnostic completed, **not** healthy or repaired software;
all three known defects reproduced.

Reviewed: selected producer/integrity code, current saved contracts and
snapshots, source/feature dispositions, lineage tests and current entrypoint
links. Not reviewed exhaustively: every historical script, complete database
contents, every broker/order path, all concurrent crash interleavings, source
transport behavior, model predictive economics or recovery of full histories.

Passing tests, file hashes and source recreation **do not prove all software
bug-free**. No runtime algorithm was changed. Prioritize the three concrete
defects in the next explicitly scoped change; broader independent review and
reorientation belong to the user's next task.
