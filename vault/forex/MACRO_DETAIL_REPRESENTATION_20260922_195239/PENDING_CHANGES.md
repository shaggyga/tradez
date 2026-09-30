# WP6 offline detail representation repair

The previously frozen partial is implemented end to end. A separate offline
consumer retains enriched detail alongside latest raw observations, reusing the
original detail-protection intent and existing receipt-time resolver, semantic
cache, context screens and pair mapping. Published sources and live collector
remain unchanged. Newer genuine detail versions replace older details even when
shorter/empty; same-clock conflicting detail abstains. Both receipt clocks gate
every version; future detail is never backdated.

2696 versions/545 texts/eight cutoffs generate168 currency rows and544 pair views.
Action candidate snapshots rise from0 to8, claim candidate snapshots from5 to17.
594 eligible currency-context rows and947 raw/detail-separated event-cutoff rows
are repeated coverage, not independent samples.220 enriched flags include18
calendar contexts that remain excluded;202 are retained parsed detail versions.
The BoE and Turkish action context is retained while later raw listings and
correction/retraction uncertainty remain visible. No forecast feature, policy
fact, historical extraction readiness or forecast improvement is claimed.

20 local and20 restored tests pass. Five payloads reproduce exactly. Tests cover
detail/listing ordering, future translations/envelopes, both clocks, contradictory
same-clock detail, shorter/empty true updates, calendar/bootstrap/publication/
staleness screens, duplicate text, shared state hashes, input corruption, live
writer refusal and process-death resume. Original source-ledger guard cases pass.
Same-implementer review only; independent review remains unperformed.

No model fits, outcomes, paid/GPT calls, broker/service/account or D-drive actions.
The earlier event-feature forecast experiment remains negative and unchanged.


Checkpoint: checkpoint/forex_macro_detail_representation.zip, SHA256 bacf45ef17c580945a62633a451b6940a1a0a9cd409a46197ecb814c25ee4d3a. Exact next: review_macro_detail_representation_checkpoint_v2; then macro_numeric_units_vintage_evidence_audit_v2. Prior checkpoints remain sealed in their original packages; approvals reference them without duplicating archives.
