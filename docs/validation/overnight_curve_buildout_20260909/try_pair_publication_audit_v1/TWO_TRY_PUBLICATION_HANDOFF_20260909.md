# Why TRY_JPY and USD_TRY have no published joint-v3 forecast

At **2026-09-09 10:34:23.518–10:34:23.975 UTC**, both pairs were correctly registered and activated. Each ledger had zero attempts, diagnostics, stored inputs, forecasts, publications, consumers, entries, outcomes and exclusions. Their ledgers retained 315 and 622 quotes respectively. The original 20-source closure and registry matched before and after the bounded read.

| Pair | Original input observation (UTC) | Mature exact H1 joint rows | Required | Original family disposition |
|---|---|---:|---:|---|
| TRY_JPY | 10:22:09.907 | 17 | 48 | Blocked: 17 < 48 |
| USD_TRY | 10:26:05.708 | 40 | 48 | Blocked: 40 < 48 |

These counts are the latest original worker-recorded readiness observations in the retained log, not newly recomputed joint readiness. Both passed the recorded nonzero-news-row and context-pattern minima; the sole latest family refusal was mature H1 training support. Context patterns are not a count of independent news events.

The frozen worker checks family readiness **before** `begin_attempt` (`oanda_joint_price_news_forecast_study_v3.py:471`, attempt creation at 476). It writes compact input-capture readiness observations to `readiness.jsonl` at lines 405–413. Both ledger `inputs` tables are empty: this audit did not replay full blocked input captures. A top-level capture status of `ready` means capture construction succeeded; its nested family readiness can remain blocked.

At the later 10:34:23 read, both local stream quotes were current and explicitly tradeable, aged 8.09 and 10.48 seconds. The latest retained completed M1 prices were 10:29 and 10:31 UTC, aged 323.65 and 203.79 seconds. The two pairs therefore cannot be described as currently missing all quotes at that observation. Fresh price availability alone does not establish current complete price-plus-news training support.

The numerical model remains present. Its required 48 mature rows and exact H1/900-second anchor rules are recorded in `oanda_joint_price_news_models_v1.py:18`, `:39` and `:53`; the joint adapter preserves those conditions at `oanda_causal_forecast_inputs_joint_news_v2.py:390`. No threshold, registration, model or runtime file was changed.

The bounded audit used two separate coherent SQLite read-only/query-only transactions, 18 explicit queries per pair, a four-second query deadline per ledger, a two-MiB readiness tail, and at most 4,096 retained price rows per pair. It did not fit, rescore, rebuild news history, call the broker or write a live database. The retained readiness tail is not a full history, and the two ledger clocks are not globally atomic.

For a future separately versioned cohort, persist an explicit **pre-attempt refusal** record carrying the original input observation, capture identity, family reason and measured support. Keep it distinct from an attempted fit so zero attempts is not opaque. This is a documentary proposal; the frozen worker and 48-row requirement remain unchanged.

`TWO_TRY_PUBLICATION_COMPACT_20260909.json` binds exact clocks, counts and source hashes. The complete bounded audit and all original quote/candle/database sources remain machine-local. No focused test suite or independent peer review was performed for this one-off diagnostic; the owner checked source semantics and the completed actual result. No independent current joint-readiness recomputation is claimed.
