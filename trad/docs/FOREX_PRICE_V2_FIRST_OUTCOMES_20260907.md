# First prospective price-v2 outcomes — September 7, 2026

The first outcomes reconcile with the original retained quotes and scorecards, but do **not** demonstrate positive returns after spread. The sanity check passed for evidence consistency; it did not establish forecast profitability.

The read-only snapshots span **22:22:28–22:22:41 UTC (18:22 EDT)**. Scored forecasts were referenced around 21:12–21:21 UTC, with their original H1 targets around 22:12–22:21 UTC. Each ledger has its own snapshot time.

| Price-only family | Correct direction | Positive after spread | MAE | Brier | Mean net return |
| --- | ---: | ---: | ---: | ---: | ---: |
| State space | 16/95 (16.8%) | 0/95 | 4.998 bps | 0.3097 | −13.821 bps |
| Ridge | 58/93 (62.4%) | 1/93 | 3.007 bps | 0.2338 | −10.308 bps |

Each family spans 62 pairs, with different references and valid-decision denominators. Lower MAE and Brier are better. The zero-move MAE was **2.650 bps** on the state-space sample and **2.498 bps** on the Ridge sample, so both models had larger magnitude errors. Fair-coin Brier was **0.25**; the rolling class-rate baseline still used its 0.5 warm-up probability. No-trade net return was zero.

## The observed spread cost

On the same executable entry and target quotes:

| Family | Signed entry-to-target midpoint move | Spread cost | Net |
| --- | ---: | ---: | ---: |
| State space | −2.286 bps | 11.535 bps | −13.821 bps |
| Ridge | +1.086 bps | 11.393 bps | −10.308 bps |

The retained spreads outweighed Ridge's average favourable move. Mean entry spreads were about 16.1–16.4 bps and target spreads about 6.6–6.7 bps. These calculations use actual retained bid/ask endpoints; this audit does not attribute the wide spreads to a particular market event. They are per-decision research returns, not dollar P/L or executed trades, and exclude financing and additional fill costs.

## Evidence and exclusions

All **136 family ledgers**, **758 publication chains**, and **188 completed outcomes** reconcile: fresh evaluation, retained outcomes and stored scorecards each contain 188 scores. There are no duplicate-reference, selected-quote, scoring or outcome mismatches. All nine registered source hashes and registered dependencies matched before and after collection.

Among already-due decisions, 28 have no qualifying original-target quote and 25 have no qualifying postpublication entry quote; these reasons can overlap. Another 520 target exclusions belong to forecasts whose targets were not yet due, and therefore describe pending outcomes. No forecast, entry, target or outcome was backfilled.

The first audit helper incorrectly included the SQL row ID in the publication receipt hash. Its initial report remains preserved. The authoritative assessment rechecks the frozen `epoch + forecast_sha` receipt projection from the same retained snapshots: **758/758 pass**. This corrected only the audit calculation; it made no live database or product change.

## Scope and source binding

This is a narrow first-period price-v2 sample. Correlated pairs and overlapping H1 forecasts are not independent trials. It makes **no improvement claim against older cohorts** and **no claim about the new joint price/news model**, whose first H1 outcomes were not due when this report was prepared. Orders remain disabled.

- Exact copied assessment: [PRICE_V2_FIRST_OUTCOMES_ASSESSMENT_20260907.json](validation/joint_price_news_20260907/price_v2_first_outcomes/PRICE_V2_FIRST_OUTCOMES_ASSESSMENT_20260907.json).
- Assessment SHA256: `a575403f245f2edbc75863166a61f653b94240724380c4e425654420869a5b4d`.
- Frozen price-v2 registry SHA256: `f7dc675925a765c0bb369282895f2839326476a05eadb7f5df6e0f27a10ed505`.
- Frozen evaluator SHA256: `16b78433bf70bf46910effd60068ef7a43c690c3f7ad2c729a567e0620b205c2`.
- Complete local evidence: `C:\Users\zmoor\Documents\forex\joint_price_news_20260907\price_v2_first_outcomes`. The assessment binds the exact retained source-row snapshots, evaluation inputs/protocols, fresh evaluations, original scorecards, and the preserved initial report. Large model-input blobs were not copied; no private broker credentials were read.
