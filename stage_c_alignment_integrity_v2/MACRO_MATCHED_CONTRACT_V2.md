# WP6 retained event/price support and no-move population

Resume of the partial macro_document_receipt_and_all_release_population_v2
package. The source population is the unchanged exact receipt checkpoint:
2,696 versions / 2,207 canonical news event IDs, including unsuccessful,
unresolved, bootstrap, calendar and headline records. The fixed cohort and
matching rules were frozen before inspecting September cohort price outcomes.
One unrelated July9 EUR_USD sample row had been read to inspect the CSV schema;
that exposure is recorded in the plan.

The fixed study uses publication and retained-source-availability anchors,
one-hour and 24-hour elapsed endpoints, and a descriptive no-move band of
abs(midpoint_return_bps)<=10. This is not a trained threshold or an economic
profitability criterion. Unmapped events remain in the event population and
all68 pair coverage. Mapped event/pair rows retain no-reference, no-target,
invalid/duplicate price, unresolved-anchor and observed outcomes separately.
Missing data never become zero return or no move.

Each canonical event keeps every immutable version. Conflicting publication
clocks do not get an arbitrary winner. Calendar/bootstrap/inferred-clock and
non-detail flags prevent release-candidate promotion. Even the remaining
candidates lack independently resolved announcement identity, verified
extraction latency and historical quote receipts. The resolution uses the full
cohort retrospectively and must not be joined into earlier feature states.
Later semantic work must use versionwise as-of documents instead.

Captured M1 inputs retain exact selected CSV bytes, whole-source file hashes,
interval and source metadata stability checks. Replay requires no source database,
broker or collector. All68 source files were scanned; full files are not duplicated
inside this bounded checkpoint. A slice proves retained research bytes, not
historical receipt or a closed query coverage boundary. Uneven source coverage
is reported without forward filling or interpreting missing tails as closures.

Reuse: causal_technical_adapter_v2.clean and retained_endpoint_targets_v1.
endpoint_outcomes remain unchanged. The reference bar starts one minute before
ceil(anchor/60)*60; the target starts horizon minutes after the reference.
Endpoint maturity is target+60, an explicit bar-end assumption. Outcome reveal
withholds values until that boundary. Exact endpoints do not establish an
uninterrupted path. Quoted bid/ask close proxies are not fills, slippage,
financing, achieved return or a policy comparison.

## Frozen operation and recovery

Use externally pinned MACRO_MATCHED_OPERATOR_RECIPE.json and
macro_matched_operator_v2.py status|run|resume|verify with --recipe,
--recipe-sha256, --inputs and --runs-dir. Runtime Python/NumPy/pandas, all source
dependencies and all consumed input bytes are pinned. Do not regenerate the
recipe during routine operation. Existing RunPublisher owns the run, publishes
completion last and validates surviving payloads during dead-process recovery.

macro_matched_checkpoint_v2.py exports/restores a scoped standalone package
through the unchanged portable reader (8MiB/member,32MiB total). CSV gzip inputs
have an additional8MiB/member and128MiB total expanded cap with exact byte
hashes. The restored operation must reproduce all71 payloads:68 pairs, events,
coverage and report. Local and restored tests remain separate from independent
review. Full raw source archives and historical trading state are not bundled.

## Remaining design gates

This creates an outcome-blind captured-publication comparison including observed
no-move cases, not a complete external all-release census or forecast experiment.
Qualify causal versionwise semantic states/readiness, retain expectation
unavailability and recover broader release/calendar evidence where possible.
Only then compare model additions on matched support with protected confirmation.
The remembered BOJ incident remains unresolved; these releases do not substitute
for that incident. Full engineering readiness remains false. GPT/advisor
comparisons, paid calls, broker/service/account actions and D-drive work are deferred.
