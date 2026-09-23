# September21,2026 recovery update

Priority next: repair the legacy instrument-pip calculation in an isolated copy, then implement the bounded all68 global-clock comparison through at least24hours. Preserve existing task IDs below; this update does not silently close unrelated items or launch StageB.

[New-machine handoff](RECOVERY_CHECKPOINT_20260921/NEW_MACHINE_HANDOFF.md) · [exact next slice](RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/NEXT_OFFLINE_SLICE.md) · [deep audit](RECOVERY_CHECKPOINT_20260921/audit_20260921/deep_audit_02/DEEP_AUDIT_REPORT.md).

---

# Pending work — September 14 review disposition

This is the vault's consolidated work map, updated after the **4:07–4:25 p.m. EDT September 14 audit**. Project implementation remains paused at the user's request; existing collection was allowed to continue.

The original 37-item numbering is preserved. [Canonical queue copied verbatim](AUDIT_20260914/evidence/trad/FOREX_PENDING_IMPROVEMENTS.md) contains earlier statuses and detailed acceptance criteria. Later observations below qualify those statuses. This vault-only update does not alter the canonical queue or complete the engineering work.

## First decision: the rolling technical dataset

Define one supported per-pair schema before more fitting: completed source bars, elapsed windows, gaps/minimum history, stable feature IDs, exact duplicates, historical/live parity, persisted observations and original-clock future labels. All 68 pairs need an explicit coverage row; unavailable inputs must remain distinguishable from zero. Reuse existing builders, data and studies.

## Operations and trustworthy measurements

| ID | Disposition after audit | Remaining work / evidence needed |
|---|---|---|
| 1 | Reopened for current selection | Rebind whole-project health to the actual selected profile and current outputs. Earlier V8 reader fixes do not cover later V12 automatically. |
| 2 | Open: all-pair rich features incomplete | Remove the accidental higher-timeframe dependency from any defined M1-only set; establish supported warm-up and repeated cycle capacity. At the sampled V12 cycle all 68 structural groups failed; 38 had MA643. |
| 3 | Open: forward admission unsuccessful | Fix source/receipt/latency and history support so eligible feature changes and quiet controls enter and settle. All 212,570 sampled retained feature references were excluded; these repeat across horizons, rather than representing independent cases. Only five-minute feature changes were populated; 15/60-minute populations remain separate unfinished work. |
| 4 | Open: native news transport | Resolve actual V5 cold-bootstrap failure and backlog under ordinary bounded reads; match consumer selection. An earlier first-success receipt did not establish sustained readiness. |
| 5 | Blocked by 4 and training eligibility | Align V4/V5 consumption, accumulate genuinely mature contexts, and verify issue/publication/consumption/settlement. Last joint count was zero forecasts, 68 unavailable. |
| 6 | Earlier estimator repair retained | Clock was healthy at the later audit. Preserve original discontinuity evidence and monitor true host/external disagreement; do not treat this as permission to disable timing checks. |
| 7 | Open | Define gap/weekend/session behavior and per-pair continuity from real bars. Reuse the existing inactive weekend helper; it still lacks completed collection and prospective evaluation. |
| 8 | Partial | Verify durable publication identities, original clocks, error reporting and independent receipt checks across the full producer/consumer path. Self-consistency alone is insufficient. |
| 9 | Open: recovery mismatch | Reconcile scheduled V8, watchdog V10 and interactive V12 into one selected source-bound recovery path; verify restart reproduces intended outputs without duplicate ownership. |

## Features, history and comparisons

| ID | Disposition | Remaining work / evidence needed |
|---|---|---|
| 10 | Completed scoped input registration | Preserve its exact source/matrix identity; do not call it a current rolling feature table. |
| 11 | Open | Correct nominal 15-minute cross-currency fields that duplicate one-minute semantics; verify true elapsed-window generation. |
| 12 | Partial: exact dedupe identified | The 795→790 reduction removes five exact columns. Assess train-only redundancy and compare against the same baseline; the name count alone does not solve noise or poor signal. |
| 13 | Open: central reorientation task | Build/verify historical-live parity, warm-up, native timeframe semantics, missingness and persistent rolling observations for all 68. Validate more than the five broadly checked archive columns when using OHLC/activity inputs. |
| 14 | Not completed | Run only the materially changed-input comparison after 11–13 are settled, on matched periods with causal labels and executable costs. |
| 15 | Open | Obtain later, independent confirmation of any retained signal; do not reuse previously inspected selection/final periods as new holdouts. |
| 16 | Partial | Score actual current forecast cohorts and reconcile trades separately; compare constant/no-change and appropriate existing baselines on identical eligible endpoints. |
| 17 | Partial, selective recovery | Recover useful missing primary reports/source/weights where feasible. Preserve old magnitude leads as chat-supported until their primary evidence is found. Avoid indiscriminate rescoring. |
| 18 | Partial | Integrate typed score/probability contracts and repaired ensemble callers with real fitted artifacts and consumers; source changes alone are not a deployed calibrated ensemble. |

## Position management and practice lifecycle

| ID | Disposition | Remaining work / evidence needed |
|---|---|---|
| 19 | Open | Define and produce an updated curve with explicit horizons; current compact/native output is H1. Reuse older dense/sparse curve work without confusing their contracts. |
| 20 | Open | Estimate conditional remaining movement/risk for held positions, with uncertainty and maturity-aware labels. |
| 21 | Design exists, integration unfinished | Complete the resumable manager bridge with original signals, position state and replay; current trial does not consume it. |
| 22 | Open research | Compare entry/exit/stop/hold/rotation choices after actual spread, delay, slippage, conversion and sizing. Volatility or direction accuracy alone does not qualify management. |
| 23 | Open integration; present policy unchanged | Reconcile the practice entry halt/preflight cause; retain claim history and distinguish unsent attempts, ambiguous dispatch and fills. The present cap is eight durable claims/day. |
| 24 | Open lifecycle verification | Trial enabled but reconciliation-halted in the last sample. Preserve cutoff September 16 00:02:33 UTC; verify final positions/intents and completion. Do not reset or extend it as a documentation repair. |

## News and complementary signals — retained for later phase

| ID | Disposition | Remaining work / evidence needed |
|---|---|---|
| 25 | Implementations/corpus exist; incremental value unproven | Reuse blurbs, source factors, currency-strength maps and news-only/price-only/combined arms. Separate movement-selected attribution from original-known forecasts. |
| 26 | Partial official coverage | Verify 21 currency/agency cells by transport, parser and fresh values. Last audit: 20 policy channels, 59/68 pairs with both policy legs. RBNZ direct access, China Customs and some attachments remain gaps. |
| 27 | Open semantic defects | Repair retained stalled/reversing move and unrelated-link cases through new timed revisions; audit current context misclassification without rewriting past evidence. |
| 28 | Missing information | Obtain permitted pre-release consensus, actual/prior/revised values and intraday expected-rate changes. Current payload sample had no numerical surprises; no connected intraday repricing feed was established. |
| 29 | Partial/stale/unconnected | Restore truthful age-aware status for FRED/CFTC/rates and assess usable bridges. Options schema is not a collector; no blanket claim that configured sources are current. |
| 30 | Unresolved historical long tail | Carry original INTRA-001–010, NEWS-001–012, TAG-001–007 through the preserved crosswalk; explicitly close, defer or retire each with evidence. |

## Storage, recreation and documentation

| ID | Disposition | Remaining work / evidence needed |
|---|---|---|
| 31 | Earlier scoped cache cleanup done | Preserve the earlier 4.77 MiB receipt. It is not space reclaimed by this vault update. |
| 32 | Open; no deletion now | About 10.72 GiB of dated inactive-log candidates require writer/reference checks, verified compressed restoration and relocation receipts before removal. |
| 33 | Open assessment | Check live database/WAL ownership, coherent backups and duplicate identities before compaction or deletion. Large files are not automatically unused. |
| 34 | Partial capacity validation | Complete combined ordinary capture/readback and restart under sustained live growth. Separate partial benchmark successes do not establish 48-hour capacity. |
| 35 | Partial | Refreshed source-only snapshot and older scoped packages are available. Full environment/data/model/runtime restoration and independent backup remain incomplete. |
| 36 | **Vault navigation/record refresh completed this turn** | One current overview, history/research map, preserved previous bytes, refreshed log/source export and local verification. Canonical implementation remains unchanged. Private GitHub remote/authentication/push and cloud-sync verification remain uncompleted. |
| 37 | Appearance deferred; operational truth still open | Repair selected dashboard source binding as part of operational work; retain truthful distinctions among observations, forecasts, eligibility, positions and results. |

A completed negative experiment need not be repeated. The [research index](RESEARCH_INDEX.md) records existing ARIMA, boosting, wide/compact/peer, news, calibration and management work. No profitable result is promised by completing this engineering queue.
