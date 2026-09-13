# First prospective price-v2 outcomes — September7,2026

The numbers reconcile, but these first outcomes do not support trading. All136 family ledgers and188 completed outcomes agree with the frozen evaluator and stored scorecards. All758 publication chains verify.

Snapshots were read 2026-09-07T22:22:28.826619+00:00 to 2026-09-07T22:22:41.582861+00:00, separately per ledger. Scored targets span approximately22:12–22:21UTC; this is a narrow first-period sample.

| Family | Correct direction | Positive after spread | MAE(bps) | Brier | Mean net(bps) |
| --- | ---: | ---: | ---: | ---: | ---: |
| State space | 16/95 (16.8%) | 0/95 | 4.998 | 0.3097 | -13.821 |
| Ridge | 58/93 (62.4%) | 1/93 | 3.007 | 0.2338 | -10.308 |

State-space MAE is4.998bps versus2.650bps for zero move; Ridge MAE is3.007bps versus2.498bps for zero move. State-space Brier0.3097 is worse than fair-coin0.25; Ridge0.2338 is lower. The rolling class-rate baseline is still at its0.5 warm-up probability. No-trade net is0.

State space: mean signed entry-to-target midpoint move -2.286bps, less 11.535bps of retained spread cost, gives -13.821bps. These are quote-based per-decision returns, not executed trades or dollar P/L.
Ridge: mean signed entry-to-target midpoint move +1.086bps, less 11.393bps of retained spread cost, gives -10.308bps. These are quote-based per-decision returns, not executed trades or dollar P/L.

Among decisions whose original targets were already due,28 have no qualifying target quote and25 have no postpublication entry quote; reasons can overlap. For not-yet-due decisions,520 target exclusions are pending outcomes, not failed forecasts;43 also lack an entry quote. Ledger totals retain93 finalized exclusions.

The initial audit receipt check accidentally included SQL id in the publication receipt hash. Its report is preserved unchanged. The assessment rechecks the correct frozen epoch+forecast_sha projection from the same retained snapshots:758/758 pass, with no quote-selection, scoring, duplicate-reference or outcome reconciliation mismatch.

The families have different references and denominators across62 pairs each. Overlapping H1 targets and correlated currency pairs are not independent trials. This audit makes no comparison with older cohorts and no accuracy-improvement claim. Orders remain disabled.

Authoritative assessment: PRICE_V2_FIRST_OUTCOMES_ASSESSMENT_20260907.json; SHA256 a575403f245f2edbc75863166a61f653b94240724380c4e425654420869a5b4d.
