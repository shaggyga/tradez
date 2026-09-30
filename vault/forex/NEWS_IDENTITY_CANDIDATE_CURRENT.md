# News identity repair candidate — September 8, 2026

An isolated producer repair resolves the retained two-topic identity collision while preserving all 17 original article records. **89 offline tests passed. This candidate is not integrated into the live collector or registered forecast workers.** Publication recovery and improved predictions are not established by this result.

This follows the completed [baseline and recovery phase](FOREX_REVAMP_BASELINE_RECOVERY_20260908.md). Its original observations, scoring, source checkpoint and validation receipt remain unchanged. The [existing pending improvements](../FOREX_PENDING_IMPROVEMENTS.md) remains the work queue.

## Retained case and result

The replay uses the original September 8 **14:01:14.709588 UTC** cutoff, not the time this report was written. Two separately sealed groups have one topic/story identity, different member sets and different original clocks. Together, the unchanged current-news guard rejected them with `conflicting_current_topic_identity`.

| Fixed historical replay | Before | Candidate |
|---|---:|---:|
| Topic records | 2 | 1 |
| Original unique member records | 17 | 17 |
| Snapshot status at original cutoff | Unavailable | Context only |
| Directional topics | 0 | 0 |

Some members cover different claims. The repair does not assert that all 17 articles form coherent directional support. It reconciles compatible root identities, retains every member payload unchanged, then delegates admission, publisher independence and syndication checks to the original guard.

## Implemented behavior

- Reconcile only compatible full topic identities and root claims. Different scheduled releases, missing identity metadata and conflicting versions of one member ID are rejected.
- Union exact member records; preserve publication, first-seen, detail, observation and original expiry clocks. Exact repeats across variants may collapse. An internal duplicate-member rejection may not be erased by reconciliation.
- Recalculate support from the union through the unchanged admission guard. Topic inclusion uses the intersection of original deadlines; later coverage cannot extend an older topic's window.
- Refresh admission for singletons and exact repeats too. A stale directional record cannot leave this API with an expired direction merely because its display window remains open.
- Apply explicit input, member, byte and variant bounds. Unresolved ambiguity requires withholding the complete payload; callers must not retry after silently removing an offending topic.

The merged topic retains identity metadata for subsequent reconciliation. That metadata is recorded in full-input hashes; it is not falsely described as covered by the older guard's narrower projected-field seal. The helper performs no data I/O and has no runtime hooks.

## Validation and retained artifacts

The final test run passed **89/89**, with zero errors, failures or skips. It includes the original guard suite and independent regression tests for the actual failure, ordering, idempotence, subsequent partitions, conflicting evidence, publisher duplication, syndication, causal clocks, expiry and bounds. The earlier 88-test pass is retained separately; the final additional case protects within-topic duplicate rejection. The guard, collector and classification sources retained their original hashes.

The replay artifact stores before/after snapshots, exact member preservation, input/source hashes and reconciliation provenance. Its `current` status refers only to the fixed historical cutoff. It is not a current producer publication or a forecast input.

Working code and executable checks are in `C:\Users\zmoor\Documents\forex\revamp_baseline_20260908\news_repair_review`. The test runner expects the preserved source at the sibling `workspace\trad` and the retained case at sibling `runtime\NEWS_TOPIC_IDENTITY_CONFLICT_CASE_20260908.json`. The isolated source is a copy, not an operating-system sandbox. The test runner blocks Python socket connection attempts; no worker or supervisor is launched. Exact audit copies and a manifest are under `docs/validation/news_identity_candidate_20260908`; restoring the original relative layout is necessary to execute the copied helpers.

See `FOREX_NEWS_IDENTITY_CANDIDATE_VALIDATION_20260908.json` at the project root for bound artifacts and the recovery copy receipt.

## Deployment remains a separate version transition

The current joint-v2 registry binds 16 source files and 68 pair contracts. Its collector, input adapter and ledger dispatch cannot be replaced in place while preserving those contracts. The transition review identifies a separate producer binding, input adapter, ledger dispatch, worker registration and distinct forecast cohort. The unchanged numerical model and exact evaluator can be reused if their semantics remain unchanged.

This publication-only repair can retain v165 classified members and the existing governed history. Separate governance storage is needed only if a later change alters classified content or introduces a classification version unsupported by the old reader. The retained raw transition capture proposed a new history path; its assessment explicitly makes that proposal conditional.

New registration, live publication checks, fresh forecast coverage and prospective outcomes remain pending. No original ledger, registration, worker process, trading setting or live implementation changed in this candidate phase. The broader horizon-engine and news/technical comparison work remains in the existing queue.
