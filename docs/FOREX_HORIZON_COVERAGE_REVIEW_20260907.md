# Forecast coverage and horizon review — September 7

The active combined model is a narrow H1 research implementation. It does use technical features, news context, and learned price/news interactions, but it does not yet provide the broader joint horizon curve the user described. Forecast availability, available news features, forecast uncertainty and trade permission need separate treatment.

## What the seven missing combined forecasts actually mean

The 23:28:46 UTC observation had 61 combined forecasts. Four other pairs already had both registered price-only H1 forecasts. Three TRY pairs had no accepted current stream quote; their last retained rows were explicitly nontradeable and timestamped around 14:59:55 UTC.

| Pair | Last complete combined-model readiness | Existing price-only forecast |
|---|---|---|
| EUR/DKK | 39 mature H1 labels; requires 48 | Both families available |
| GBP/HKD | 10 rows with news context; requires 12 | Both families available |
| GBP/PLN | 7 rows with news context; requires 12 | Both families available |
| GBP/SGD | 5 context rows and 6 patterns; requires 12 and 8 | Both families available |
| EUR/TRY, TRY/JPY, USD/TRY | No accepted current quote | Unavailable |

The GBP pairs also had a later stale-news capture rejection. Their structural training counts above come from the retained last complete readiness checks. Refreshing the news snapshot alone does not supply the missing historical examples. None of the seven had a joint fit attempt or publication at this observation; restarting the worker is not a demonstrated remedy.

The dashboard update exposes existing price-only estimates inline where a combined estimate is unavailable, with explicit price-only provenance and the combined blocker. Combined estimates remain primary where available. Coverage is divided into combined, price-only and unavailable, using verified current source generations and original forecast horizons. This changes visibility, not the model, publication ledger, scoring denominator or permission to trade.

## What is too restrictive, and what must remain valid

The joint model selects only real timestamps divisible by 900 seconds for its historical H1 training anchors. An offline audit reconstructed news available at each real candidate timestamp while preserving exact same-session t+3600 price endpoints. It did not fabricate candles, copy future news into the past, fit a new model, or publish forecasts.

| Pair | Current 15-minute anchors: labels / context rows / patterns | 3-minute anchors: labels / context rows / patterns |
|---|---|---|
| EUR/DKK | 39 / 19 / 20 | 149 / 61 / 62 |
| GBP/HKD | 92 / 10 / 11 | 445 / 41 / 42 |
| GBP/PLN | 94 / 7 / 8 | 471 / 33 / 34 |
| GBP/SGD | 86 / 5 / 6 | 421 / 23 / 24 |

All four meet the existing 48/12/8 counts with the denser anchors. This demonstrates evidence discarded by the sampling rule. It does **not** establish additional independent events or improved predictions. H1 targets overlap; the current model's `n/4` shrinkage assumes 15-minute spacing and cannot be reused unchanged at three-minute spacing. Pattern counts include a continuous news-age feature, so one ageing story can create multiple patterns. None of these four training sets contained vetted directional-news examples.

Minimum context counts, a single horizon and the requirement that every fresh fit have a current news snapshot are model-design choices. Actual observation times, mature labels, valid prices and identity checks are data-integrity requirements. The former can be redesigned and tested. Weakening the latter would corrupt what a forecast and its result mean.

## What conjunction currently establishes

The frozen joint feature vector includes technical features, eight news/context features, and momentum-by-context and momentum-by-vetted-news interactions. Its stored comparisons include a separately fitted price-only model and the same joint model evaluated with documented neutral-news features.

At the retained EUR/USD API observation at 23:26:18 UTC, the joint model's neutral-news estimate was approximately -0.272 pips and its news adjustment was -1.447 pips, yielding approximately -1.719 pips. The separately fitted price-only comparison was -0.179 pips and is not part of that addition. Those are actual stored model outputs, not proof that news caused the subsequent market move. This model had 94 training rows, 92 with context, and zero with vetted directional evidence. Broad context conjunction is implemented; learned event-specific directional impact and useful joint predictive performance remain unproven.

## Recommended next model experiment

1. Restore a genuine multi-horizon prediction curve at 1, 5, 10, 15, 30 and 60 minutes, with separately matured labels and independently reported availability at each horizon. Do not stretch the current H1 number into invented shorter-horizon predictions.
2. Keep an available technical estimate visible when news is missing. Train explicit news-availability and age handling; learn any news adjustment from causally available examples. Missing news must not be represented as observed neutral news.
3. Replace the arbitrary clock-grid bottleneck with a specified sampling policy and overlap-aware weighting/shrinkage. Report independent sessions and distinct events alongside row counts.
4. Evaluate shared currency/co-movement features and partially pooled news effects as additional predictors. Agreement among correlated pairs or models is not independent corroboration.
5. Show expected movement, an honestly qualified uncertainty range and input coverage. Fit or calibrate claimed interval coverage on chronological held-out data, with purging for overlapping outcomes. Compare technical-only, news-only and combined predictions at the same origins and targets.
6. Apply spread, risk and account execution gates after prediction. A small or uncertain prediction can remain visible without authorizing a trade.

These are pending experiments, not new live models. The frozen H1 studies and their outcomes remain unchanged. More permissive display or denser training alone is not evidence of higher accuracy.

At the separate 23:33:10 UTC outcome observation, the original joint scheduler-v1 had 18 retained/scored decisions from 19 due targets: 10/18 correct direction, 1/18 positive after spread and mean net -5.6129 bps per scored decision. This tiny, correlated first batch does not establish news value or calibration. The active scheduler-v2 had 217 lifetime forecasts and no target yet due; its first original target was 23:45:40 UTC. Neither cohort is merged into the other's results.

## Retained observations and evidence

A separate 23:31:16–25 UTC probe encountered a dashboard summary/heartbeat generation mismatch. It did not establish a new prediction failure or a current contribution value. The source checks remained enforced; a 23:33:24 UTC check had recovered to current joint and price sources with 61 combined forecasts. This transient reader-publication condition is retained separately from structural model readiness.

Four later read-only checks at 23:34:03–14 retained two coherent producer generations with valid payload seals and matching heartbeat hashes; the direct reader and API were current. Another API observation at 23:35:24 retained the mismatch again. Producer bytes from the failure moments were not captured, so a precise phase or cache cause is not established. The five-second API cache can repeat a retained status. No binding rule was relaxed to conceal this intermittent condition.

Audit evidence is retained under `C:\Users\zmoor\Documents\forex\joint_horizon_review_20260907`. Curated receipts are copied into `trad/docs/validation/horizon_coverage_20260907`; raw price/news captures remain outside that source evidence package. The final review receipt is `trad/FOREX_HORIZON_COVERAGE_REVIEW_VALIDATION_20260907.json`.

Orders and promotion remain disabled. No model threshold, registered source, cohort, original target or outcome was changed by this review.
