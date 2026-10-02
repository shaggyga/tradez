# Paper inputs connected and news mapping repaired — October 2, 2026

The existing exact-quote worker is deployed. A recorded prospective observation now
joins its decimal-preserving receipts, its independently read session/generation,
the retained forecasts, and the existing immutable empty paper book. This completes
the bounded `retained_management_current_state_binding_v1` observation gate, subject
to the checkpoint's final publication checks. It does **not** complete R06 position
management, qualify conditional values, establish profitability, or authorize orders.

## Changes and actual verification

The one-hour session began at 00:38:02 UTC, with the fixed deadline 01:38:02 UTC.
It uses an in-chat goal, not an automation. The user's response authorized the
previously requested quote-worker/controller restart. The cutover installed the
already tested wrapper and recovery registration/profile together. Original role
names, recovery expiry, restart history and other workers were preserved; the
other task's EUR/USD recorder was excluded. The original float quote/intensity
outputs remain active. The exact sidecar has progressed with 68 receipt entries,
matching worker generation/session and zero raw observer errors. Receipt inventory
does not mean all 68 quotes pass their freshness/tradeability gate simultaneously.

`tools/forex_paper_quote_observation.py` provides one prospective `record` command
and deterministic `replay`. Recording reads existing publications only and appends
one immutable WAIT event to the pre-existing `mabel_empty_paper_20261001` episode.
It refuses a stale, disconnected, errored or mismatched quote worker, preserves
failed attempts, and never infers a position from a forecast. The recorded book is
one flat book, not 2,652 positions or trades. All action flags remain false.

The final recorded population contains 2,652 slots: 2,223 priced observations,
245 missing forecasts, 145 refused forecasts and 39 quote refusals. Sixty-two
instrument quotes passed the original 30-second gate. These are **recorded-time**
counts; they are not current availability promises. The earlier exploratory
observation had 2,388 priced slots and is retained separately. Its source version
is not relabelled as the final one.

83 quote/state/worker consumer tests passed. All 84 published payloads matched
between uninterrupted, interrupted/resumed and isolated restored execution.
The isolated replay denied original project/Vault reads before application imports.
Its capsule includes the exact source, capture, book and request bytes.

`tools/forex_paper_entry_economics.py` adds the missing entry-cost boundary using
the original Decimal sizing and USD conversion. Each supplied paper scenario must
bind the book, quote, forecast receipt, decision and terminal; explicitly declare
fees, slippage, financing coverage and risk/margin allocation; and state the frozen
spread/conversion assumption. Margin is capacity, not an expense; spread is charged
once; profit/loss conversion sides differ. It never supplies zero costs by default.
23 tests passed, including the design's long/short USD10 arithmetic fixtures and
cross-currency loss conversion. The same 23 passed after isolated restore, and
84 actual-population refusal payloads matched uninterrupted/resumed/restored runs.
All 2,652 recorded slots lack a supplied economic declaration: **no current net
economic edge or entry qualification is claimed**. Synthetic tests remain synthetic.

## Dashboard/news regression found during the EUR/USD inspection

The dashboard selection still pinned the pre-`91806e8` versions of the all-pair
availability reader and retained forecast reader. Those files had already changed
in the accepted October 1 Windows publication-lock repair. This was an omitted
selection migration, not a failure of news collection or a new model revision.

A new test of the actual checked-in selection reproduced `dashboard_source_changed`.
Both old and new source identities were verified against the original Git commit
and its parent, including exact checkout line endings. Only those two pins and the
selection activation time were updated; study/registry/activation references and
all other pins were preserved. The original source-checking gate remains intact.
No worker restart was needed. 36 selection, mapping and publication-retry tests
passed. The existing news worker subsequently reported `current` and 91 new
news/technical/forecast mappings; the dashboard endpoint returned HTTP200.
These mappings are descriptive conjunctions, not proof of causality or accuracy.

Failed attempts remain in the evidence: isolated pytest initially omitted the
workspace package path; entry test fixtures initially had an arithmetic expectation
error and a missing conversion pair; the independent rational conversion check was
adjusted to a declared 1e-78 tolerance for the reference's 80-digit reciprocal-then-
multiplication rounding. Production accounting was not changed to make tests pass.

## Indicator/live-look findings

The 01:02:51 UTC M1 availability publication had 11,512/14,688 finite local values.
3,169 missing values lacked elapsed support, and seven were undefined/numerical.
The 200-period finite-EMA family lacked support on all 68 pairs. EUR/USD had
206/216 M1 values; its separate M5 set later had 216/216 and its aligned peer set
12/12. These are different timeframes/clocks, not one tick-aligned feature vector.

Saved models retain their original missing-value preprocessing. At the inspected
publication, eligible legacy26 inputs were complete, while some richer models
used missing-value paths. Do not equate a published prediction with complete
observed indicators. Current long-window gaps need retrieval/continuity work, not
another untested indicator or invented/forward-filled minutes. The nearby
`market_open_20260913_v1` M1 archive is dated September 13 and cannot repair current
October gaps; the native cache's M5 candles cannot reconstruct missing M1 bars.

The full timestamped EUR/USD report is `EURUSD_LIVE_LOOK.md` in the packet/evidence.
It lists all 216 M1, 12 aligned peer and 216 derived M5 values, plus the 39 retained
forecast outputs. Its 01:10:06 UTC quote was bid1.12336/ask1.12350, 1.4pips spread.
The news refusal shown at that snapshot was subsequently repaired as described above.

## Reuse, next work and limits

Evidence directory: `evidence/one_hour_20261002_003802`.
Vault packet: `PAPER_INPUTS_CONNECTED_20261002`.
Review is substantive **same-task review**, not independent review.
Delivery remains **35/45 (77.78%)**; this observation/cost boundary does not earn
the still-missing whole R06 acceptance milestone.

Exact next: `retained_management_economic_input_binding_v1`. Retrieve supported,
time-bound fee/slippage/financing and paper risk declarations under their own
provenance. Use the new entry consumer and existing management contract. Missing
conditional same-terminal values and actual position evidence remain explicit;
do not rebadge a terminal midpoint forecast as remaining opportunity. No new fits,
paid/advisor calls, broker orders or account actions are authorized here.

Eligible independent data follow-up: `rolling_m1_elapsed_support_recovery_v1` —
inspect current C-drive archived minutes under original source/availability
identities, distinguish retrievable observations from genuinely absent minutes,
and stage any repair without changing historical feature records or another task's
capture. Do not substitute M5 bars, zeros or interpolation for missing M1 evidence.

## Reproduction and rollback

Extract `paper_quote_observation_capsule.zip` or
`paper_entry_economics_capsule.zip` into separate new directories and run
`python -I -B replay.py <original-project-root> <original-vault-root>` using the
qualified numerical runtime. The latter also runs the 23 economic contract tests.
They replay the recorded observation, not current market conditions, and fit nothing.

For a new observed paper sample use `tools/forex_paper_quote_observation.py record`
with explicit `--book`, a new `--output`, `--quote-path` and `--heartbeat-path`.
Use the receipt's exact request hash with `replay`; never edit a sealed request to
match a newer source. Restore the capsule for historical replication.

Quote rollout rollback requires the identity-checked controller/quote-worker
procedure using `profile.before` and `contract.before`; never reset restart ledgers.
The dashboard selection's original bytes are `dashboard_selection.before.json`.
Restoring them alone intentionally restores the known source refusal, so it is
not an operational repair. No historical source, model or trade ledger was rewritten.
