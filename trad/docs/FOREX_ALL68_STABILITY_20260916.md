# All-68 operational repair and economical market reviews — September 16, 2026

At 05:53 UTC, the independent production readback passed: one V5 supervisor,
one V5 watchdog, and exactly one leaf for each of 17 registered research roles.
Current profile is `config/operational_runtime_v17_20260916.json`, SHA256
`8063162683b12ea8c046cf65db27532376bdb3abadb96ec3aad0fe02ff10b8e6`.
Original numerical contracts, archives, model cohorts and orders policy are unchanged.

## Changes actually deployed

- M1 cadence V3 selects all 68 pairs from the frozen registry, regardless of
  which quotes happen to be present at startup.
- Stalled pairs use ten-candle probes before larger catch-up requests.
- Genuine gap recovery uses the registered pip sizes, fixing the generic
  conversion for EUR_HUF, USD_HUF, USD_THB and HKD_JPY. No retained recovered
  row for those four pairs was found, so earlier corruption is not claimed.
- The successor preserves original retries, per-minute response receipts,
  single-writer locks and actual observation clocks. A stop-request file permits
  future graceful draining.
- The read-only availability service publishes all 68 rows every minute at
  `data/oanda_training_manager/state/all68_technical_availability_v1.json`.
  It distinguishes readable, partial, stale and non-tradeable data, binds original
  feature/receipt hashes, and reports specific missingness. Its smoke capture
  took 0.50 seconds; it does not recompute or fill original feature values.

## What is and is not complete

All 68 original feature vectors exactly recomputed from their retained source
data during the audit. No reader error or late-arriving recoverable row was found
in that scope. All 68 M1 tails and native M5 caches were readable.

A fresh read-only broker audit made one request per pair: 81,600 validated
complete BAM candles. Every one of the 3,542 missing minutes in the exact frozen
603-minute support windows was absent inside the returned coverage. None could
be recovered as an actual broker candle. The earlier 6,073 diagnostic count
included intervals extending outside those windows; do not reuse it as the
clipped count. Source responses and independent replay are retained.

Native M5 provides the practical observation lane: the 05:53:41 UTC brief had
65 pairs at a shared 05:50 completed endpoint with valid 15/60-minute returns,
RSI and ATR; 63 supported EMA50. All 68 histories read without errors. Quote
freshness is checked separately: 64 were current/tradeable at that exact read.
EUR_TRY, TRY_JPY and USD_TRY remain broker-nontradeable/stale; this is not fresh
data for those instruments. Original M1 features remain partial when their exact
support is missing or their denominator is undefined. Readable does not mean
every feature has a finite number.

The user subsequently authorized gap filling and M5 forecasting with M1 entry
timing. A pure, separately named derived M1/M5 feature module is implemented and
tested, but **staged only**. It makes short causal prior-price fills explicit,
keeps observed/imputed masks and feature-window quality, preserves long gaps,
and calculates calendar features from actual bar-end clocks. It never uses a
later endpoint for live interpolation. Original model weights are not compatible
with the new derived schema. No live derived dataset or new M5 forecast cohort
was deployed in this turn. Existing fitted price/joint cohorts remain M1-based.
This is a remaining integration/research step, not a completed model migration.

## Verification and deployment history

- M1 successor: 103 focused tests passed.
- Availability reader: 18 tests passed and a 68-row production smoke read.
- V5 recovery: 54 tests passed under native Windows PowerShell 5.
- Compact market brief: 12 tests passed; a full 68-pair production scan passed.
- Staged derived kernel: 19 tests passed; separate pure-module review found no blocker.

The first cutover waited for idle and restored the V4 controller when the wait
expired. The second stopped the old writer but encountered delayed process exit;
V4 recovery restored it. The third waited for verified process termination and
successfully activated V17 at 05:50:40 UTC. These attempts and their evidence were
preserved. Existing retry limits were not reset: the M1 and watchdog budgets had
three recent starts at the final readback, so another automatic restart would be
temporarily withheld until attempts age out under the unchanged 30-minute rule.
The running roles were healthy and exactly owned. Scheduled-task triggers,
principal and settings were unchanged; only the launcher/profile action changed.
Recovery's original September 20 23:59 UTC cutoff remains unchanged.

Evidence directory: `docs/validation/all68_stability_20260916`, especially
`READBACK_FINAL/INDEPENDENT_READBACK.json`, `fresh_provider_gap_audit_001/READBACK_001.json`,
`native_m5_capacity_001/AUDIT.json` and the staged-source validation receipts.
The earlier `OPERATIONAL_REPAIR_20260916` vault checkpoint is untouched.

## Requested half-hour reviews

The existing paused Forex chat automation was updated, not duplicated. Reviews
are scheduled on the hour and half-hour through Thursday September 17 at noon
America/New_York. The user selected GPT-5.5 Medium. Each run first checks account
usage and pauses at 7% weekly allowance remaining (or an unavailable usage read),
providing a buffer for the requested 5% reserve. Other chats share that allowance;
this is not a hard account-wide spending limit.

Each review runs `tools/forex_market_brief_v1.py --once`, reads only its compact
summary and gives at most 120 words about direction, technical conditions and
hypothetical enter/watch/wait/avoid decisions. Full 68-pair timestamped evidence
is saved under `data/forex_market_briefs_v1`. No fitting, broad rescans, subagents,
position sizing or orders are scheduled. Account positions remain unverified
unless a fresh account record is actually read. Local scheduling requires the
computer and Codex app to remain running.
