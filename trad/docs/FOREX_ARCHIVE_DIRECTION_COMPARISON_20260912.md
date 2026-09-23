**Current operational checkpoint - 2026-09-13T13:43:38.829786+00:00:** [Current state, evidence and recreation](FOREX_OPERATIONAL_CHECKPOINT_20260913.md). Earlier status below is historical; its original bytes are preserved.

**Current stop checkpoint — 2026-09-13T11:14:00.478497+00:00:** [Status, results and next work](FOREX_STOP_CHECKPOINT_20260913.md). Work stopped on request; earlier status below is historical and preserved.

**Current checkpoint — 2026-09-13T03:05:46.537951+00:00:** [Current state, next work and market-open setup](FOREX_MARKET_OPEN_SETUP_20260913.md). This dated update supersedes older status statements below; their original evidence remains preserved.

**Current checkpoint — 2026-09-13T01:24:06.275461+00:00:** [Current state, next work and market-open setup](FOREX_CURRENT_STATE_20260913.md). This dated update supersedes older status statements below; their original evidence remains preserved.

# Full-archive direction comparison — September 12, 2026

The fixed four-setup comparison did not establish a consistently useful trading setup. Across 36 model/horizon/period cells,32 had negative selected-position net after the declared costs, two were positive and two took no positions. Direction accuracy ranged49.42%–52.45%. Adding75 peer-currency fields worsened mean absolute forecast error in all18 matched comparisons. These conclusions concern this declared comparison, not every richer historical engine or news design.

## Inputs and comparison

Every source in the primary53,512,475-row,68-pair archive union was considered, spanning June20,2024–September11,2026. The protocol sampled hourly origins with complete past60-minute context and separate exact future-label, bid/ask, source and session requirements. This produced875,146 observed hourly feature rows,1,799,440 eligible horizon labels and1,356,327 assessment pair/horizon rows. It did not fit53million independent samples. Future gap/session filters define retrospective scoring support, not what a live forecast can know.

Four fixed arms: Ridge(alpha1000) and histogram gradient boosting, each with technical24 or technical24+peer75 inputs, plus fixed pair identity. Seventeen technical slots are structurally constant under the strict60-minute support, leaving seven varying own-price fields. This is explicitly not the795-input matrix or643-field MA engine. No news fields enter this experiment.

Direct midpoint-return targets are15,60 and240 minutes. Three expanding training periods precede assessment in2025H1,2025H2 and2026year-to-date through September11. Actual label maturity is strictly before each next period. All dates were previously opened historical development; none is described as an untouched confirmation period.

The fixed position rule acts only when absolute predicted return exceeds the current full spread plus1basis point. Scoring uses both actual bid/ask endpoints, the common entry-mid notional, and declared extra costs of0/1/2bps. A one-minute delayed-entry sensitivity preserves the original forecast, chosen side and terminal. Long/short momentum, reversal, training-mean, zero and no-trade baselines are retained. Overlapping observations and different pair/clock/day weighting must not be represented as executed account profit or independent trade counts.

## What the results mean

Only12of36 learned cells beat zero-change MAE, by at most0.1704%. No learned setup was positive after primary costs in all three periods. The two positive selected-position cells were:

| Cell | Selected / assessment rows | Mean net bps | With one-minute delayed entry |
|---|---:|---:|---:|
| H60 Ridge technical,2025H1 |44 /152,837|+5.3398|+4.1645|
| H60 Ridge technical+peer,2026YTD |51 /186,582|+0.6413|−2.0806|

These sparse positives do not establish a repeatable edge. Period variation, dependency and equal-day diagnostics remain in the detailed report. Every H240 learned cell lost after primary costs and had worse MAE than zero change. The tested always-action momentum/reversal baselines also lost after costs in every period/horizon. This does not support promoting a renamed or selectively reversed version.

Direction accuracy measures sign agreement on nonflat midpoint targets. Negative MAE improvement means worse point error; after-spread profitability is a third measure. None is a substitute for the others.

## Recreation and evidence

The protocol and11source files were frozen before preparation and fitting. All36fits completed. A separate result reviewer recomputed all81 learned/baseline report rows from exact saved endpoints and predictions. A separately reviewed verifier checked525 completed files, loaded the36 saved artifacts and reproduced all5,425,308 estimates and policy actions exactly, with maximum absolute difference0.0. It also reproduced45baseline arms and6,781,635baseline estimates/actions. This proves current-runtime reproduction, not another predictive validation or cross-platform portability.

The workpack is `C:/Users/zmoor/Documents/forex/revamp_8h_20260912/direction_archive`. Read [all36 learned results and limitations](../../revamp_8h_20260912/direction_archive/result_review_001/RESULT_REVIEW.md), [protocol](../../revamp_8h_20260912/direction_archive/PROTOCOL_CANDIDATE_002.md), [freeze](../../revamp_8h_20260912/direction_archive/PROTOCOL_FREEZE_001.json), [completion](../../revamp_8h_20260912/direction_archive/evaluation_001/COMPLETION_RECEIPT.json), and [saved-artifact recreation](../../revamp_8h_20260912/direction_archive/SAVED_ARTIFACT_RECREATION_001.json).

The next useful research change should be an explicitly timed additional input or target, reusing prior model definitions and results. The broader feature audit distinguishes631 close/clock-based formulas from OHLC/activity-dependent fields and unresolved exactv4 source parity. A source-derived adapter can be repaired before any new fit; old795/MA643 weights and their observed results remain separate. No account, service, trial deadline or trading permission changed in this experiment.
