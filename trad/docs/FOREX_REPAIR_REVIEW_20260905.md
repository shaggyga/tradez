# Forex repair review — 5 September 2026

The independent audit findings have source and documentation repairs, validated with isolated fixtures and cross-review. **The project remains stopped and the supported decision remains `no_trade`.** Source changes do not establish live readiness or profitability. The final complete saved runtime integrity report remains degraded; no broker request or replacement live audit was made.

`FOREX_REPAIR_VALIDATION_20260905.json` records the exact repaired source hashes, final test counts, dispositions and local evidence hashes. The original independent audit is preserved in `FOREX_INDEPENDENT_AUDIT_20260905.json` and `docs/FOREX_INDEPENDENT_AUDIT_20260905.md`. Original reproduction files remain in `C:\Users\zmoor\Documents\forex\audit_20260905`; new validation is in the sibling `repair_20260905` directory.

## Repairs and boundaries

| Finding | Implemented repair | Validation and remaining limit |
|---|---|---|
| A01 — final signed-canary revocation | Bind the consumed decision to the exact signed payload. Reload and recheck authorization and clocks after preparation/proof reads, directly before POST. | Real submission code reaches a fake broker for a valid decision; signed revocation, removal, tighter limits, expiry and staleness block it. No real broker integration was run. |
| A02 — missing account-currency conversion | Return an explicit unavailable result; block governed sizing and exposure when conversion is missing, stale, future, invalid or untradeable. | Positive direct, inverse and cached rates work. Source age counts toward the 15-second cache limit. Notional remains the existing quote-based estimate, not guaranteed fill notional. |
| A03 — premature source/rank entry clocks | Separate source V9 and rank V8 contracts. Require explicit future source activation, completed forecast publication, consumer observation, immutable completed rank selection, and a later quote for its fixed pair/side. | Delayed source attestation, consumer commit and ranking/selection commit cannot backdate an entry. Restart preserves pending selections; later bid/ask outcomes are evaluated within the observation clock. Both successors ship disabled and have no prospective performance proof. |
| A04 — precommit mapping availability | Fastlane V3 commits mappings first, independently reads them, then appends an availability attestation. Consumer observation is separately required for prospective entry use. | Delayed mapping and delayed attestation commits are covered. The mapping-visible clock alone has no entry authority. Historical V1/V2 exposure remains unestablished. |
| A05 — late collector commit omitted | Read a rowid high-water boundary and matching rows in one input snapshot; checkpoint the processed prefix atomically with the registry batch. | A second WAL writer's previously uncommitted row is collected next cycle. Database identity/anchor replacement fails closed for explicit reconciliation. |
| A06 — building state replaces completed state | Publish liveness/building/error progress separately from the last completed fastlane snapshot. | Isolated interrupted-publication tests pass. A complete clean live integrity cycle is still required after a separately requested restart. |
| A07 — retry resets the cursor | Recover the durable SQLite checkpoint, rather than reconstructing it from a transient JSON state. | Input/database retry, interrupted publication and missing JSON preserve progress and avoid rescanning old fixture rows. |
| A08 — literal credentials treated as references | Grant Python-reference exemptions only for token-proven executable NAME/attribute spans; reject literals in quotes, docstrings, comments, prefixes and triple quotes. Other languages receive no reference exemption. | Synthetic literal regressions and known-private-value precedence pass. Candidate scanning and export preflight report no matches; pattern-based scanning is not recognition of every possible secret encoding. |
| A09 — source-only restore instructions | Separate offline archive/source verification and fixture tests from the validator requiring excluded canonical history. | The evidence-dependent validator stays strict; missing history is explicitly distinguished from a corrupt source archive. |

The unwired prospective-governance lock API was also hardened. Discovery snapshots freeze the full candidate definition; registration rejects altered fields and backdated clocks, and confirmation must be registered before its future window. This does not activate a statistical confirmation evaluator or confer trading authority.

Cross-review found and closed additional successor-path gaps before handoff: slow rank selection could retain an early cycle clock, inherited source publication exposed an incomplete current snapshot, and inherited outcome lookup could select a future exit bar. V9 now stages inherited output privately and publishes only a fully wrapped completed snapshot. Rank V8 freezes selections before entry quotes, preserves them across restart, and bounds outcome lookup by the observation time. The historical parent modules were preserved.

## Integration and verification

The supervisor no longer launches source V7/V8 or rank V6/V7. Their code and ledgers remain diagnostic records. It names the V9/V8 successors only behind their explicit disabled configuration gates, and names fastlane V3 with its new state path and contract. PowerShell syntax was parsed without executing the supervisor.

Project integrity separately reports retired source/rank inventory at its own saved timestamp and successor readiness. Disabled successors are explicitly `inactive`, with `operationally_ready:false` and `prospective_performance_verified:false`. An enabled successor with missing, stale, incomplete or mismatched evidence fails. Historical inventory reconciliation does not become live readiness.

The final suites passed **313 tests**: 65 execution, 68 credential/snapshot and 180 combined integration cases. The execution suite excludes five unrelated tests that start publisher threads; they are not claimed passing. The combined fixtures cover fastlane recovery, current integrity wiring, preserved source/rank contracts, successor activation and clocks, positive later-quote fills and outcome maturity, governance locks, retirement policy, canonical-record synchronization and issue-register validation. Network/worker/production-database guards were used for the relevant runtime fixture suites. Credential/export tests use synthetic files and temporary Git repositories.

Independent reviewers checked execution and conversion, credential literal contexts, source/rank clock ordering and positive restart behavior, supervisor/integrity wiring, recreation instructions and performance claims. This remains focused validation of the stated repairs, not an exhaustive correctness certificate for the entire historical project.

## Evidence preservation and handoff

No original runtime ledger, old audit output or historical source/rank module was rewritten. No Git commit was created, and pre-existing uncommitted work remains. Source V9 and rank V8 reject predecessor ledger imports; their new configurations require explicit future activation and cannot route, authorize or promote.

The issue register uses `implemented_collecting` for the remaining historical/operational acceptance of A03, A04 and A06; runtime collection is actually disabled/stopped. Other stated offline repairs have hash-bound completion receipts. Existing unconfirmed hypotheses, permanently invalid cohorts and external source gaps retain their earlier dispositions.

The vault's source ZIP, README and current records are refreshed only after the final checks. The previous source ZIP is retained, and replaced current records/pointers are preserved under `maintenance/before_repair_20260905`. The new `maintenance/REPAIR_EXPORT_VERIFICATION_20260905.json` verifies local source members and record hashes. It does not attest that OneDrive has finished uploading.

For the performance review, read [FOREX_PERFORMANCE_AUDIT_20260905.md](FOREX_PERFORMANCE_AUDIT_20260905.md). Its practical findings are unconfirmed economic performance, limited independent evidence, a 99 MB integrity publication and substantial database/log storage. No performance-driven strategy change, retention deletion, production load test or restart was performed.
