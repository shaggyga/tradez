# Feature forward evaluation, version 1

This is a separate exploratory study of feature-change magnitude. It does not
create fitted forecasts, infer direction from an indicator's sign, place orders,
or promote a strategy. Existing forecast cohorts and their results are unchanged.

## Frozen population and rules

Every 300 seconds, the standalone worker asks the shared feature mapper for all
comparisons at 300-, 900-, and 3600-second windows. It consumes the complete
`all_feature_changes` lane, never the displayed top 50 or a pair filter. Each
window's compressed immutable batch retains all comparison rows, current pair
coverage, archive errors and sampling disclosures, and source snapshot identities.
Repeated related-pair display links are omitted; these are not forward inputs.
An empty or unavailable collection is retained as an empty, explicitly typed batch.

An alert requires an observed numeric feature, at least five prior comparisons,
an available descriptive percentile of at least 95, and nonzero absolute change.
Other eligible rows are controls. Missing/default/stale, nonnumeric, model-output,
and insufficient or unranked-history rows are explicit exclusions. All survive
accounting. Constant past changes remain unranked under the existing mapper.

The fixed future magnitude threshold is an absolute reference-to-target midpoint
move of at least 5 basis points. Horizons are 300, 900, and 3600 seconds. These
thresholds are preregistered exploratory defaults, not optimized or validated
trading signals. Feature, window, and horizon counts overlap and are not
independent trials. Changing the source generation or protocol requires a new
ledger; source hashes include the mapper, normalizer, clock guard, worker, ledger,
protocol and exact price scorer.

## Publication, quotes, and outcomes

The ledger commits the complete immutable comparison batch before recording its
actual owner-observed publication completion and durable acknowledgement. It does
not backdate events to an archive generation timestamp. A crash before
acknowledgement may only recover the same frozen bytes with a later actual clock;
expired source evidence cannot become an eligible event. Cadence is first-published,
not the best or latest observation selected after outcomes.

A fresh, owner-admitted reference bid/ask quote is frozen before publication.
Market age is at most 30 seconds. The entry must have been observed by this owner
after publication, have a strictly later market timestamp, and fall within 60
seconds. Its admission sequence must also exceed the publication's quote high-water
mark. The target is immutable publication time plus the horizon; the first
eligible subsequently admitted quote at or after that target is used, with at
most 60 seconds of delay. Quote market timestamps are explicit; generation times
never substitute for missing market timestamps. Nontradeable, future, crossed,
malformed, stale, or conflicting quotes are refused visibly.

Quote reads occur every five seconds. Before the first study activation the
protocol explicitly declares event-demand persistence: retain fresh forced
reference candidates and the first eligible quotes needed by unresolved entry or
target windows. Other valid reads are counted as `observed_not_needed`, without
storing them as a quote history. Shared quote-read and verified-clock receipts are
stored once and referenced by hash. Run-level read counters reset on worker
restart; the retained quotes, publication receipts, and outcomes remain durable.
This ledger is not the producer's full quote archive.

Pending and unknown cases are retained. Missing reference, entry, or target support
does not count as a false alert. An unavailable deadline becomes unknown and no
longer holds a pending job. The first admitted outcome is immutable. Owner score
availability is sampled after computation and clock revalidation; it is explicitly
not a claim about another process's visibility before the final commit.

Both fixed-long and fixed-short hypothetical probes are scored with the existing
exact decimal bid/ask scorer: long buys the entry ask and exits the target bid;
short sells the entry bid and exits the target ask. They are reported separately,
without selecting the better side. One- and two-basis-point extra cost stress is
retained. The scorer's required placeholder probability and all probability-derived
fields are excluded from this study's output. Direction is measured against the
frozen reference; costs use actual admitted entry/target quotes. These are not
fills, financing-inclusive returns, guaranteed execution, or account P/L.

Magnitude accounting reports alert hits, false alerts, control missed moves and
quiet controls only where a pair outcome is scored. Exclusion, unknown, pending,
and unpublished counts remain visible in the full feature-event denominator.
Pair-level probe counts are reported separately so shared outcomes are not
presented as independent feature votes. `iter_events()` exposes all retained
event identities and supporting rows without top-N truncation.

## Operational boundary and resource limits

The standalone worker reads only the new versioned observation archive and the
dedicated practice quote snapshot. It imports no fitted models, broker clients,
old ledger owners, or network clients. Its shared clock gate must be valid before
admission and publication. The first clock proof is revalidated at completion,
and current health is read again; a replacement cannot renew an expired original.
Missing clock health yields an explicit heartbeat refusal. It does not launch or
repair the clock monitor and does not bypass an activation restriction.

An OS-held owner lock excludes a second process throughout multi-commit publication.
SQLite uses FULL synchronous DELETE journalling, an explicit page ceiling and
prewrite estimates that conservatively account for the database and rollback
journal. The library and CLI default cap is 512 MiB, with reserved room for outcomes
and refusal diagnostics plus a free-space check. No history is pruned. Capacity
refuses new admission visibly; already pending work continues if reserve permits.
This is a finite cap, not a 48-hour capacity guarantee. The explicit CLI may set
`--max-ledger-mib` up to 4096 and `--minimum-free-mib` up to 65536; these settings
are stored and checked on reopen. The research supervisor declares a 4096 MiB
total cap and 4096 MiB free-space floor. It does not migrate an existing ledger.
The shaped 66,708-comparison fixture used about 1.215 MB per compressed window;
36 such windows per hour alone would consume about 44 MB per hour. Real values,
outcomes, indexes, journals, and free-space conditions differ, so neither the
larger cap nor this synthetic measurement guarantees 48 hours of collection.

Each comparison window admits at most 100,000 rows and 64 MiB before compression;
oversized or mismatched populations are refused completely. The worker uses one
shared bounded archive decode for all three windows, and does not repeat it during
intervening quote-only ticks. Compact immutable category counts are committed with
each full comparison batch, and compact exact probe metrics with each full outcome.
Summaries aggregate these pair/job records instead of decompressing feature history;
their counters and metrics are checked against the original full-row calculation.
The worker also caches summaries between decisions or outcome changes.
Original archive sampling and unavailable reasons remain
disclosed; the study makes no full-market-history completeness claim.

The heartbeat is `feature_forward_status_v1.json` with worker
`research_feature_forward_v1`. A healthy heartbeat is not proof of fresh feeds,
eligible features, forecast quality, or permission to trade. No actual study was
started while implementing and testing this source.
