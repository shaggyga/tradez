# Offline news-topic identity repair candidate

The retained failure is an identity/grouping mismatch. Two context-only topic variants share one topic/story ID but carry disjoint groups of 10 and 7 original members. The frozen snapshot guard accepts each alone and rejects both together. The candidate reconciles compatible variants into one exact member union before that unchanged guard runs; it neither assigns suffix IDs nor picks one variant and discards the other.

This is an isolated candidate only. Its new source lives in `revamp_baseline_20260908/news_repair_review`; the canonical project, isolated original modules, registrations, workers and trading settings remain unchanged. There is no runtime integration or automatic file/data I/O. The caller must explicitly resolve the ordinary guard import to the preserved isolated source, and separately bind its exact source hash. The candidate's version-string check is not a substitute for source-hash verification.

## Contract

`reconcile_topic_identities(topics, *, as_of) -> list[dict]` returns current topics with refreshed guard admission. It raises `ValueError` for invalid or ambiguous collisions; the eventual caller must withhold the entire current payload on error. It must not retry after dropping the offending topic. `reconcile_topic_identities_with_provenance(...)` also returns input hashes, original-window intersection, exact union counts and output proof/hash. Its `offline_candidate` marker is not an accepted current-news schema or a newly registered classification version.

Current inclusion uses the frozen guard's original publication/window rule, including the exact expiry boundary. Expired or future publications are omitted consistently with that existing rule. Every current input proof is validated before reconciliation. Empty input remains empty. Singletons and exact duplicate topic objects refresh their current guard projection/proof while retaining complete producer identity metadata; an expired directional proof therefore cannot be returned as a current direction.

For a same-ID collision:

1. Require nonempty matching `story_cluster_id` and `topic_signature`, a real boolean `structured_event`, and matching valid scheduled timestamps for structured events. Ambiguous nonstructured schedules are rejected. These identity fields are **not authenticated by the old guard's projected-field seal**; they are compared from complete retained producer inputs and bound by the candidate's input hashes. They are preserved on the merged output for later partition reconciliation.
2. Require every pair of sealed root claims to pass the unchanged conservative `same_claim` function, and require exact agreement of other sealed semantic, currency, score, classification and safety fields. Only explicitly enumerated display, original-time and derived-count fields may differ. This does not assert that every context member supports the same claim.
3. Preserve each original compact member payload exactly. The same member ID with different content or clocks is rejected. Exact identical members shared across valid variants collapse once. A repeated member ID within an individual input remains a rejection: reconciliation must not erase the guard's original within-topic duplicate failure.
4. Retain one copy of every unique member, sorted by its event ID. Do not change published, first-seen, causal-known, detail, numeric, publication-known or observed-availability clocks; do not reclassify historical members. Collision inputs with a member not yet available at `as_of` are conservatively withheld.
5. Select display text deterministically, retain the earliest original publication, and limit topic inclusion to the **intersection of original topic deadlines**. The aggregate first-seen is the earliest actual member first-seen; aggregate known-time is the latest actual member availability. These summary values never replace member clocks. The containing future producer/consumer publication must still record its actual observation clocks.
6. Clear inherited support counts and rerun the frozen guard over the full union. The guard independently collapses repeated publishers and syndicated headlines, checks each member's original timeliness/expiry and same-claim support, and computes any permitted direction. Root display selection does not choose scoring members. If no valid direction exists, the result remains context. The final unchanged snapshot guard again validates current proofs, unique IDs and payload bounds.

The candidate bounds total inputs to 10,000/32 MiB, current unique topics to the guard's 128, unique colliding variants to 128 before pairwise claim comparisons, and the member union/evidence/current payload to the existing guard limits. No truncation path converts a bound failure to neutral evidence.

## Retained-case result and limits

The source-bound case is `runtime/NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json`, SHA-256 `7f76c161ae89eb72470c61210874372fd4c102131e76137fc761ebb46fb2284d`. At its original cutoff, `2026-09-08T14:01:14.709588+00:00`, a direct candidate smoke check retained all 17 members and produced one context-only topic through the unchanged guard. Reversing the two root inputs produced the same output. Its conservative topic deadline is `15:11:26 UTC`; the later variant's window cannot extend the earlier variant's deadline within this merged object.

Five retained members do not pass `same_claim` against their own root headline, including broader oil/ringgit/market context. The candidate does **not** rewrite this fact or claim a coherent 17-source directional event. It preserves those members and delegates eligible-direction decisions to the unchanged guard. This repair does not fix broad historical clustering, increase predictive accuracy, prove news value, or restore live joint forecasting by itself.

The intersection can deliberately withhold a merged object sooner than the newer variant alone. A subsequent snapshot may contain only that still-current original variant, preserving its own original clocks; it must not revive an expired member. Raw publisher names may remain useful context, but their count is not a corroboration count. Reconciliation provenance must remain separate from independent-source evidence.

## Required regression acceptance

- Reproduce the original two-topic rejection unchanged; candidate yields one context-only topic, all 17 member payloads/clocks retained, no false direction, unchanged input objects.
- Root and member permutations, exact duplicate inputs, and overlapping identical partitions give deterministic semantic results; a previously reconciled partition retains identity metadata and can accept a later compatible partition without losing members.
- Changed payload or clock under the same member ID rejects. Repeated member identity within one input is not laundered into support. Different root claims, negations, numeric claims, story/signature, structured-event status or scheduled timestamp reject; missing identity metadata rejects collisions.
- Tampered guard/projection, wrong classification/safety fields and invalid JSON/clocks reject. No-collision and exact-duplicate paths still refresh expired direction to context while preserving complete identity metadata.
- Known-after-cutoff, late observed availability, expired original member windows and late corroborators never create earlier evidence or renew expiry. At the merged topic's exact original deadline preserve the existing inclusive convention; immediately after it, withhold that merged object.
- Same publisher, repeated feed and exact/transitive syndicated headlines do not inflate independent support. A valid independently corroborated union may only obtain the unchanged guard's result, with original `available=max(selected member availability)` and `expires=min(selected original expiry)`.
- Exercise variant/member/byte/current-topic bounds and confirm errors withhold rather than truncate. Run the existing unchanged guard regression suite as well as the new reconciler tests.

Root owns the separate tests and source-bound acceptance receipt. A later deployment needs a coordinated, versioned producer/input/study transition because existing registered joint studies bind the old producer/guard source; this candidate does not authorize editing those registrations or rewriting prior outcomes.
