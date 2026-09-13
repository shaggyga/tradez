# Forex audit state — stopped at market close

Current sanity check (2026-09-06T15:53:01.014364+00:00): raw counts reproduce, with eight floating-point-only
direction hits corrected offline: **4,148/8,414 = 49.30%**; after-spread positives
remain **716/8,414 = 8.51%**. The four-model EUR/USD percentages reproduce exactly.
See [the prediction sanity check](FOREX_PREDICTION_SANITY_20260906.md),
[model inventory](FOREX_MODEL_INVENTORY_20260906.md) and
`FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json`. Scoring precision follow-up is open;
the research plan is inactive and the registered collecting study is unchanged.

Earlier dated observations below retain their original counts and scope.

Current update (2026-09-06T15:32:22.899414+00:00): the initial causal study had two Windows heartbeat
replacement failures. The fixed `io_r2` worker is separately registered and
running in collection mode; the original zero-forecast registration is preserved.
Trading stays off, and improved predictions still require fresh outcomes.
See [the current operational addendum](FOREX_CAUSAL_IO_REPAIR_20260906.md)
and `FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json`.

Earlier dated observations follow; their registration details are historical.

Latest September 6 timing repair: collection now runs twelve workers, including
a separately registered four-family study. It is waiting for fresh market data;
trading remains disabled. Both prediction-clock fixes are implemented, with
future evidence still pending. See [the repair record](FOREX_CAUSAL_TIMING_REPAIR_20260906.md)
and `FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json`. Earlier entries below are dated history.

Current runtime — September 6, 2026: Forex has restarted in collection-only
mode. Eleven data/dashboard/health workers run; execution and model/proof
production remain off. See [the restart record](FOREX_RESEARCH_RESTART_20260906.md)
and `FOREX_RESEARCH_RESTART_VALIDATION_20260906.json`. Earlier stopped-state
observations below describe their recorded times and are now historical.

**September 6 fixed-evaluation update:** [the EUR/USD comparison](FOREX_FIXED_EVALUATION_20260906.md)
adds a fixed baseline assessment, strict offline scoring and a newly documented
entry/target availability issue. All four models lose to naive baselines on the
matching stored prices; missing clocks prevent causal scoring. No original
runtime snapshot was rewritten or worker started. Use the current issue register
and FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json for the remaining prerequisites.

**September 6 optimization update:** [the reporting optimization](FOREX_OPTIMIZATION_REVIEW_20260906.md)
and [prediction assessment](FOREX_PREDICTION_QUALITY_20260906.md) add measured
source improvements and an explicit scorecard. The original stopped-state
runtime JSON below and saved degraded integrity publication were not changed.
The compact publisher has offline validation; live timing remains unverified.
See `FOREX_OPTIMIZATION_VALIDATION_20260906.json` and the current backlog.

**September 5 source-repair update:** the runtime snapshot below remains
unchanged evidence of shutdown. Current source fixes and validation are in
`FOREX_REPAIR_VALIDATION_20260905.json` and
[the repair review](FOREX_REPAIR_REVIEW_20260905.md). The
[performance audit](FOREX_PERFORMANCE_AUDIT_20260905.md) finds no confirmed edge.
New source V9/rank V8 remain disabled; source V7/V8 and rank V6/V7 are retired
diagnostics. The supervisor now references fastlane V3; it has not been run.
Offline test passes do not replace the final degraded runtime publication.
Use the current issue register for repaired-source dispositions; the 88/91
counts below belong to the original snapshot and initial fault registration.

Snapshot: **2026-09-04 22:09:17 America/New_York** (2026-09-05T02:09:17.9875986Z).

The canonical source and runtime remain `C:\Users\zmoor\Documents\forex\trad`.
The Forex vault is a derived audit/recreation archive, not a live deployment.
Machine-readable observations and sampled SHA-256 bindings are in
source-root `FOREX_AUDIT_STATE_CURRENT.json`, copied to vault-root
`AUDIT_STATE_CURRENT.json`.

## Operating state

The session is **intentionally stopped** at the user's request. Independent
process inspection found zero canonical Python workers, zero supervisors and
zero watchdogs. The session owner also confirmed zero dashboard listeners on
port 8765. `ForexSafeCoreAtLogon` is disabled; the old
`ForexGptOrigConstantRotationDemoWatchdog` was already disabled. The shared
`BIGTRIAD Unified Trading Dashboard` remains enabled as a cross-project,
logon-only dashboard task, not a live Forex executor. No new automation was
created. Nothing here requests a restart.

Before shutdown, this audit found an absent supervisor/watchdog and surviving
Python children; the owner briefly restored the existing guarded supervisor
path. The user's subsequent clarification ended the runtime. Those observations
are retained in the JSON timeline, not presented as the final operating state.

Every runtime file is now **frozen evidence at its own timestamp**. A producer's
embedded `current` or `ok` flag must not be read as post-shutdown freshness.

## Account and evidence position

The last saved Practice-007 account observation was at **22:08:02 ET**: NAV and
balance **41.6042**, cumulative P/L **-8.3430**, zero open trades and zero pending
orders. This audit read the producer snapshot; it did not make a new broker
request or submit an order.

The lifecycle publication at **21:41:08 ET** contains **0 confirmed candidates**,
43,872 continuing hypotheses and 9,485 futility rejections. The supported
decision remains **`no_trade`**. The 88-item issue register has 47 completed
repairs, 24 implemented/collecting items, 11 permanently invalid cohorts,
two superseded/collecting baselines and four externally blocked gaps.
That 88-item count is the original 22:09 ET snapshot, not a fault-free claim.
The subsequent fault report is source-root `FOREX_FAULT_AUDIT_CURRENT.json`
and `docs/FOREX_FAULT_AUDIT_CURRENT.md`, copied to vault-root
`FAULT_AUDIT_CURRENT.json` and `FAULT_AUDIT_CURRENT.md`. Three defects were
registered at 02:20 UTC September 5; the reconciled register has **91 items and
three open repairs**. Runtime remains stopped.

## Unresolved operational attestation

The final complete saved project-integrity report remains the **21:45:33 ET**
`degraded` publication. Its sole failed check is
`news_source_governance_fast_lane_current_prospective_and_inert`.

That component sampled a `building` state with no valid snapshot cutoff:
zero state receipts versus 9,991 database receipts. Schema checks passed and
clock, orphan and contract/inert violations were zero. Transient publication
timing is a possible explanation, not an established diagnosis. No later
complete integrity publication was observed before intentional shutdown.
Do not replace this caveat with the earlier close report's healthy claim.

## Source, feature and lineage entrypoints

| Surface | Canonical entrypoint | Meaning |
|---|---|---|
| Queue and decisions | `FOREX_ISSUE_REGISTER_CURRENT.json`, `FOREX_PENDING_IMPROVEMENTS.md`, `FOREX_PROJECT_LOG.md` | Current queue snapshot plus historical decisions; older sections remain dated history. |
| Source coverage | `config/news_sources_v1.json`, `config/forex_source_gap_register_v1.json` | 192 configured sources; configuration is not proof of current transport, usable content or alpha. |
| Feature design | `config/model_feature_space.json` | Preserved August 3 design inventory, not live availability. |
| Runtime disposition | `config/shadow_runtime_retirements_v1.json` | Separate active, dormant, unavailable and retired contracts. |
| News classification | `oanda_news_classification_contract.py` | V164 conflict-duration-recap guard; prospective activation 20:25 UTC September 4. |
| Watchlist | `data/oanda_training_manager/state/news_technical_watchlist_v1.json` | Frozen V49/V164 observation at 22:07:30 ET: zero factors, zero entries, zero negative quote ages. |
| Source response | `oanda_causal_source_factor_response_map_v8.py`; `data/oanda_training_manager/local_news_sentiment/causal_source_factor_response_map_latest_v8.json` | Current isolated V8 ledger deliberately frozen to V152: 13 diagnostic preactivation events, **zero prospective proof events**. V1–V7 do not import into V8. |
| Source-conditioned rank | `config/source_conditioned_currency_rank_v7.json`; `data/oanda_training_manager/state/source_conditioned_currency_rank_v7.json` | V7 requires V8 input; zero decisions and zero independent source episodes. Research only. |
| Official quote capture | `config/official_event_pair_quote_capture_v3.json` | Current post-input-read quote-clock contract; older cohorts remain separately preserved. |

Direct intraday commodity data, macro consensus/rate repricing, RBNZ direct
policy transport and GDELT secure transport remain externally blocked gaps.
Unavailable book/depth contracts are not live inputs; quote-change activity is
not trade/order flow. Historical repair receipts, source mappings and large
feature counts are not predictive evidence.

## Eventual demo session

A clean archive is **not demo-entry readiness**. Resume only when requested.
Before calling the runtime operationally ready, verify intended Practice-007-only
routing, exactly one supervisor/watchdog, fresh quotes/account/clock observations
and a complete clean integrity publication, including the caveat above.

An entry still requires a genuine lifecycle `confirmed_candidate` and an exact,
fresh, unexpired one-time canary authorization matching the signal, proof cohort
and allocator cohort. Preserve activation clocks, invalidated lineage and
no-import rules. Real-money routing remains disabled. No profitability or
predictive-edge claim is made.
