# Gap-aware model reuse decision

## Component inspected

`C:\Users\zmoor\Documents\forex\trad\oanda_gap_aware_four_family_models.py`

This is a pure, offline utility. It neither reads archives nor writes artifacts, and it explicitly rejects future bars, missing-minute compression, and noncontiguous feature/target paths. It produces research-only outputs and declares `can_place_orders: false`.

## Valid reusable ideas

- Actual UTC M1-start timestamp validation.
- Explicit 60-second bar-readiness representation.
- Exact consecutive path requirements for short-horizon training labels.
- Gap-driven abstention rather than invented flat candles.
- Prediction diagnostics that expose training epochs and target maturity.

## Current incompatibilities

- Fixed universe of seven pairs and EUR/USD-only forecast output; it cannot meet the all-68 requirement.
- Fixed one-hour target; it does not provide calendar daily-close or multiday targets.
- The module itself states that the caller must bind fit completion, issue, and publication clocks; current campaign needs those clocks as an enforced contract.
- Its factor graph retains legacy per-pair pip units, which must not be silently pooled with the canonical all-68 pip map.

## Decision

Do not run the component as a model comparator yet. Reuse its gap-validation and diagnostics patterns in a future isolated all-68 adapter, only after the adapter has explicit source manifest, model-readiness, all-pair coverage, target-family, and output-directory contracts. No source modification or live-service use is authorized by this review.
