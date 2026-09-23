# Recovery baseline: backlog reconciliation

Static read-only observation: September 8, 2026, 13:58:44–45 UTC. The root backlog `trad/FOREX_PENDING_IMPROVEMENTS.md` is authoritative; `trad/docs/PENDING_IMPROVEMENTS.md` is only an archive pointer. This reconciliation reuses the prior engine/blurb audits and freshly checks the important source, dictionary and small model-artifact bindings. It does not report live worker health or new predictive results.

**First recovery candidate: the existing second-ridge JSON curve. First broad-feature follow-on: the MA grid. Unified795 needs its v4/v5 source/schema mismatch resolved first.** This ordering concerns reproducibility, not which model is profitable or which should trade.

## Reconciled queue

| Item | Status now | Acceptance still needed |
| --- | --- | --- |
| Feature dictionary and project-scope correction | **Completed documentation.** Nine dictionary receipt file hashes still match; coverage is 251 design entries, 34 current inputs and six historical schemas/2,169 versioned field records. | Preserve source-version checks. Documentation coverage is not proof that design-only fields are implemented or informative. |
| Current joint news/price calculation, member clocks, independent sparse-price families and fair scheduling | **Implemented; gain unproven.** Three inspected registered model/input hashes still match the earlier audit. | Root's fresh runtime/outcome baseline, followed by multiple independent sessions. Earlier dated health checks are not current telemetry. |
| Existing horizon engines | **Disconnected in parts, recoverable in parts.** Second-ridge and MA C/D sources match; their expected C model files are absent. Unified fitted weights exist in AppData but D code is a different schema version. | Isolated deterministic comparator recovery; preserve originals and never equate restored capacity with improved accuracy. |
| Duplicate 15-minute cross features and raw-pip ranking | **Confirmed open defects.** Five retained v4 cross fields duplicate their one-minute equivalents; raw native-pip cross-pair ranking is not comparable economic magnitude. | A new version with correct windows/units and paired comparisons. Preserve the old data, weights and scores. |
| Blurb/factor corpus and earlier news/technical comparisons | **Implemented research, partially disconnected.** 7,048 legacy rows, 2,935 factors and 6,465 distinct reconstruction episodes already exist; earlier base analog cohorts emitted no qualifying prequential predictions. | Reuse usable entry-time evidence and existing comparisons; separate late/ex-post attribution. Do not recollect the known corpus or describe it as nonexistent. |
| Co-movement and richer learnable news | **Partial machinery; extensions unproven.** Historical currency-strength/breadth builders exist. The current 34-vector uses scored-member news context and two interactions, but no peer-pair features or raw text. | Measure added information; test sparse pooling across factor/event families separately. More names or correlated columns do not establish more information. |
| Magnitude, probability calibration and spread cost | **Unproven acceptance.** Older directional/after-cost reports and tiny overlapping current H1 samples do not establish an edge. | Same-decision baselines, original targets, comparable bps/dollars, event/time blocks and later prospective evidence. Preserve coverage/exclusion denominators. |
| News latency, intermittent publication mismatch and provider issues | **Open operational acceptance.** The backlog retains 300-second news freshness, summary-generation mismatch and SCB/GDELT/HKMA follow-ups. | Root's new baseline and bounded reproduction. Preserve source clocks/hash checks; do not infer a current outage from an old report. |
| History breadth, input limits and storage growth | **Partially implemented.** Bounded captures/compression exist; 48-hour/5,000-row limits and longer-term growth still matter. | Track multi-session evidence and storage without deleting immutable history or silently changing registered limits. |
| Previous joint scheduler retirement | **Pending comparison review.** | Review its original outcomes and exclusions before any separately coordinated retirement. |

## Which retained comparator is reproducible first?

### 1. Second-ridge: transparent inference recovery

The three exact C/D source matches are:

- `oanda_second_forecast.py`: SHA256 `3ac9f38bbe7140aae7bf391d1778f658b599199601d811d97d3386f32d727063`.
- `oanda_second_forecast_fit.py`: `238b8e6d83536aa287dc6282a364ccbe7ba016f2e00221e3b95381edd8d4aba8`.
- `oanda_second_forecast_runner.py`: `4a362cbca7be31917f38c3e738bdb2bef734185530b008e55a1c097ad37d6d84`.

Retained artifact: `D:/forex/trad/data/oanda_training_manager/state/second_ridge_models_v1.json`, SHA256 **`85ff37fd77f85fa6ae990990ead19eb7d4e20389d99b774023f32c13bf62658c`**. All **884** surfaces have finite 14-element means/scales/coefficients, positive scales and finite intercepts. Exact ordered feature names agree with the source. The report and artifact agree on fit date, horizons and coverage: 871 pair-specific surfaces and 13 pooled TRY/JPY proxies across 13 horizons. Those proxies must remain explicitly distinguished.

`SecondRidgeSnapshot` in `oanda_second_forecast.py:659` reads JSON; `predict` at line 753 uses the saved normalized linear model, magnitude calibration and logistic probability. It does not fit. Do **not** instantiate `SecondForecastStore` at line 799 during isolated inference recovery: that class writes a database. The old model's embedded account-eligibility values are historical metadata, never permission for the new comparator.

Retained S5 input exists at `D:/forex/trad/data/oanda_training_manager/candles_s5_bam/EUR_USD_S5.parquet`. `feature_matrix` in the fitter at line 207 computes the original 14 inputs from `dt`, bid/ask/mid closes, mid high/low, spread and volume. The model was trained on S5 while its original live feature path used S1. Recovery must expose that distinction and preserve the trainer's endpoint tolerance through nominal target +7 seconds. It is not the current exact-H1 ledger contract.

**First milestone acceptance, proposed here and not executed:** preserve the source/JSON/report/input fixture in isolation; verify deterministic normalized-dot-product versus original compiled inference arithmetic for one pair at 5/15/60 minutes without fitting; retain raw and calibrated returns, original model date, source clock, explicit engineering-replay label, missingness and research-only flags. Parameter dimensions alone are not prediction parity. A deterministic replay would prove usable inference, not original issue-time availability, training reproducibility or profitability. The retained report/JSON do not attest an exact fit-time source hash/dependency lock.

### 2. MA grid: strongest broad-feature follow-on

C and D runtime/fitter hashes still match (`e8b277c7…` / `e39a979c…`). The D artifact at `data/oanda_training_manager/models/ma_feature_grid/ma_feature_grid_latest.joblib` hashes to **`faa5eb07d7643bba35054d62b025c2a56a6df29b9b545545f192583a3ba94709`**, matching its report. It retains 610 fitted and 92 unsupported cells, with up to 643 fields. Its expected C artifact is absent.

This has stronger source continuity than unified795, but opaque artifact loading, dependencies and embedded feature-schema compatibility have not been executed in this phase. The next broad-model milestone is a trusted isolated dependency/schema and deterministic inference check on a fixed M1-input horizon subset. No historical cell passed both validation and holdout; restored inference must not be called an approved trading model.

### 3. Unified795: source-version reconstruction remains open

The AppData artifact still hashes to **`46370f88ae3e51e59da4988fd7dcc0e1c46bb893a5c413e8e9736d3badf2fb52`**. Its matrix manifest identifies `unified_intrahour_features_v4_native_pip`; current D `fresh_m1_intrahour/src/unified_forecast.py` identifies `unified_intrahour_features_v5_opportunity`. The C package is absent. The matching filenames and recoverable D package do not establish an exact v4 transformer for the retained model. Keep v4 weights/matrix immutable; reconstruct compatibility before creating a separate corrected-window version. Original forecast/after-cost gate failures remain retained evidence.

## Evidence

`BACKLOG_RECONCILIATION_20260908.json`, SHA256 **`dc8ce66f8bcb8b0f85858ca4ce4fe93af010475b15560bd63d2cd80a30861600`**, binds the backlog, dictionary, prior audits, exact source comparisons and small artifacts. All nine dictionary file checks and three registered source checks passed; all 884 second-ridge parameter surfaces passed structural/finite checks. No model inference, deserialization, fitting, scoring, live code/config edits, runtime starts or database writes were performed in this reconciliation. Subsequent root work must carry its own separate acceptance receipt.
