# Forex recovery handoff for another Codex machine

Prepared September 21, 2026. Start with [README.md](README.md), [MANIFEST.json](MANIFEST.json) and the included safe restore tool. The Vault's RECOVERY_CHECKPOINT_LATEST.json binds the published manifest and its separate local restore receipt. Verify again after transfer; local publication is not cloud-sync proof.

## Start with the portable checkpoint

This is an **offline research recovery checkpoint** for an existing Forex project. It preserves reviewed source, two distinct audited 68-pair input collections, audit findings, selected test receipts and a dependency snapshot. It is not a complete copy of the roughly 200-GB runtime/news/model estate, a ready-to-trade installation, or proof that the original environment has been recreated.

Use the final checkpoint manifest as the list of authoritative files. Verify archive hashes and member hashes before extracting into a new empty directory. Reject paths outside that directory, collisions and unexpected members. Keep the restored project inactive: do not run launchers, supervisor recovery, collectors, broker adapters, scheduled tasks or account commands.

The original current project is `C:\Users\zmoor\Documents\forex\trad`. On the new machine, choose a local project directory; this historical absolute path is provenance, not a requirement. Resolve packaged files relative to the checkpoint and supply local path mappings explicitly. Do not assume Git HEAD contains every reviewed file: the audit found important untracked source/configuration work.

The Vault is the explanation, history and recovery-record layer. It is not the running application or current broker/account state. Its older files named CURRENT retain their original observation dates. Newer file names do not automatically supersede the source/data identity of an older result.

## What was established

- The long-history collection has 68 files, 21 currencies and 50,321,016 M1 rows. All rows were scanned for the recorded integrity checks, with instrument-correct spread units. It is reacquired history, not an original arrival-time archive. Preserve its 917,120 unclassified pair-gap intervals and separate weekend-like gaps; do not forward-fill fictitious quotes.
- The native collection has 68 files and 4,652,361 rows, extending into September 17. Its 1,124,156 overlapping price/volume rows agree with the long histories. Treat this as a separate vintage; no blind concatenation or source overwrite.
- Native `spread_pips` is wrong on 26,824 rows for EUR_HUF, HKD_JPY, USD_HUF and USD_THB during August 9–14. Retained bid/ask prices remain usable under the audited checks. The new rolling branch recomputes spread from bid/ask using correct metadata. A legacy alignment/rotation consumer still has a JPY-only pip shortcut and consumes the stored spread; repair that consumer before reuse.
- The July archive contains 3,124 verified experiment records across six tree/boosting families. Historical success status does not mean profitability. The exact July moments run and the H1 two-hour replication package were recovered; their magnitude/cost-survival leads have explicit population and execution limitations. Do not rerun them simply because an old filename was missing.
- Saved chats comprise 1,077 unique turns from five tasks; duplicate export collections must not be counted twice. Chat claims are recovery pointers, not independent experiment evidence. Some turns have no dates.
- The August BOJ document was listed about 18 seconds after recorded publication, but usable body text arrived over 14 hours later. Saved replay outcomes were unfavorable. The separate remembered 4-p.m. incident is still unidentified. Original observation/version receipts remain missing from the bounded reconstruction.
- Dated macro state had actual release values but zero causal pre-release consensus observations and zero standardized surprises. Do not manufacture expectations from later commentary or revised data.
- Seven selected existing engineering tests and four synthetic 24-hour label checks passed in isolation. They are not a complete portfolio, forecasting or profitability gate.

Use the packaged copies of `audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md`, `MODEL_LINEAGE_AUDIT.md`, `NATIVE_SPREAD_SOURCE_TRACE.md`, both final history verdicts and `NEXT_OFFLINE_SLICE.md` for exact details. The exact [next offline slice](audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md) and [deep audit](audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md) are included. [EVIDENCE_INDEX.json](evidence/EVIDENCE_INDEX.json) maps selected primary results from original machine paths into this package.

## First substantive next step — not started by this checkpoint

1. Work in a fresh isolated source copy. Reuse the existing exact instrument-pip map to repair the legacy alignment/rotation price-delta, ATR, synthetic quote and cost calculations. Add focused exceptional-pair regression evidence. Preserve the original source and result lineage.
2. Define an explicit all-68 historical view. Start inside the internally checked long-history support; keep the newer native extension separate until its spread-correction and precedence contract is written. Any correction belongs in a derived view with the original value, corrected value, reason and source hash retained.
3. Reuse the existing causal feature/label machinery for a bounded curve through at least 24 elapsed hours. Begin with ordinary rolling/no-change/Ridge controls, not another unrestricted search. Keep signed direction, absolute movement and cost survival separate.
4. Use one global clock and independent capital/holdings per policy. Compare fixed/reference holding with the recovered rotation policy on identical eligible forecasts. Establish label maturity, prefix-only fitting, model readiness, order timing, side-specific costs, currency conversion, financing where applicable, ambiguity and resumability before interpreting performance.
5. Preserve every attempt, including failures and negative results, with source/config/data hashes, evaluation periods, budgets and a resume command. Previously inspected periods are development evidence, not untouched confirmation.

That plan is documentation of the next work, not a command to start it during restore verification. Restore verification should prove hashes, safe layout, readable records and selected offline contracts first.

## Operating boundaries

D: has fresh bad-block evidence and is excluded. Do not read, repair, scan or use it for a work directory during this recovery. C-only retained evidence is sufficient for the next offline step; missing D-only material stays explicitly deferred.

The September 21 runtime audit observed seven fresh heartbeat timestamps and eleven stale roles; one fresh role reported zero forecast pairs. The supervisor's last record reported a profile change and the recovery window had expired. These are dated local observations, not a new machine's operating state. A dependency list, heartbeat, passing smoke test or copied profile does not authorize restarting services or account execution.

No credentials or account wiring should be transferred in the research checkpoint. Demo/live orders, account changes, live recovery, paid APIs and cloud compute require their separately applicable current authorization and budget. No profitable edge or deployment readiness is claimed.

Local hash verification does not prove OneDrive upload completion or successful availability on another machine. After synchronization or transfer, verify the same manifest again on the destination before treating the checkpoint as received.
