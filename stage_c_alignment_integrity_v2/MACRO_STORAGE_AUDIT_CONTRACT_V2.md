# Raw receipt versus canonical representation audit

Retain exact original upsert source fragment/hash/line references and original
ledger module. Do not import or execute the collector. Verify raw observations
are recorded before selection/binding of the canonical projection. Exercise only
the existing early detail-downgrade guard on frozen before/after source content;
do not manufacture historical active-version metadata for a full replay.

The offline latest-raw-version selector and live canonical projection are different
consumers. Raw receipt retention is correct; treating a later listing as a detail
replacement loses offline semantic context. The existing guard already rejects
the five observed enriched-to-non-enriched transitions, including both action-loss
pairs. This does not prove historical live projection state. No active source or
DB writes, prices, outcomes, fits or forecast promotion.

Next: separately versioned offline detail representation selector, preserving
raw listings, causal visibility, ambiguous detail abstention and explicit unknown
correction/retraction semantics. Reuse protection already present in the project.
