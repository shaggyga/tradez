# Derived all-68 feature feed — September 16, 2026

This checkpoint closes the first operational gap after the rolling all-68 repair:
the separately tested derived M1/M5 feature kernel is now connected to a live
publisher. The immutable rolling M1 observation store, historical partitions,
old model weights, broker/account state and trading policy are unchanged.

## What changed

- Added `oanda_derived_technical_publisher_v1.py`, a versioned loop-capable publisher that
  reads native completed candle files and publishes a bounded all-68 JSON state
  file at `data/oanda_training_manager/state/all68_derived_technical_features_v1.json`.
- Added `config/derived_technical_features_v1_20260916.json`, source-bound to
  the current derived-feature kernel and native candle reader.
- The publisher uses M5 by default, with the existing causal derived-feature
  policy: short missing tails can use strictly prior flat assumptions, while
  longer gaps remain visible. It does not backfill weekends or replace original
  source rows.
- The output separates the base current feed from immediate movement readiness:
  base readiness covers spread, activity, current bar shape and UTC clock terms;
  movement readiness requires one-bar return support.

## Live readback

The live publication at anchor `2026-09-16T12:30:00+00:00` read all 68 native
M5 histories with zero source errors.

| Metric | Result |
|---|---:|
| Registered pairs | 68 |
| Histories read | 68 |
| Current pairs | 68 |
| Observed-current pairs | 67 |
| Derived-current pairs | 1 |
| Base minimal feature count | 9 |
| Base minimal feature current pairs | 68 |
| Movement feature count | 2 |
| Movement feature current pairs | 68 |
| Fully finite 216-field pairs | 41 |

The derived-current pair was `TRY_JPY`: it used two bounded short-tail fills and
had no unfilled tail. This is explicit in the state file instead of making the
whole pair unreadable or silently replacing original source rows.

## Verification

The focused feature suites passed:

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' -B -m pytest -p no:cacheprovider -q test_oanda_derived_technical_features_v1.py test_oanda_derived_technical_publisher_v1.py
```

Result: 23 passed.

The publisher supports bounded `--interval-sec` / `--duration-sec` looping, but
it is not yet registered in a V18 operational supervisor profile. Final live
publication generation: `166cf89e46c2695701ead4819f591d41d897a7694c15569cbae1f2e4849381cb`.

## Remaining work

This is a feature-feed repair, not a profitable model result. Current old model
weights still cannot consume the new derived schema. Next work is V18 supervisor
registration or another durable scheduler, then training and scoring a compatible
simple baseline/cohort on the retained rolling data. Compare local-only versus
local-plus-peer fields on the same origins, and keep movement and signed-return
profitability separate in the evaluation.
