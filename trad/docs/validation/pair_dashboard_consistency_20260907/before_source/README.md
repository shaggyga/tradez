# Forex research project

September 7 pair-coverage update: **20 pairs now have published forecasts (40 model predictions)**,
verified at 17:19 UTC. The new registry covers all 68 observed instruments, each
with independent ridge/state-space inputs, cadence and immutable evidence.
Other rows show their own last-attempt warm-up or missing-quote reason. The
dashboard provides pair selection, all-pairs display and explicit uncalibrated
probability labels. The research gate admits 14 workers; trading remains off.

Read the pair-coverage report and validation receipt linked below. The original
EUR/USD and shared four-family studies remain separate and unchanged. Greater
coverage does not demonstrate better predictions; new H1 outcomes were still
pending at this observation. Earlier dated runtime descriptions retain their
original scope.

[Pair-coverage report](docs/FOREX_PAIR_FORECAST_COVERAGE_20260907.md) · [Validation receipt](FOREX_PAIR_FORECAST_COVERAGE_VALIDATION_20260907.json).

Latest September 7 architecture: the separate **EUR/USD-only companion activated
at 16:29:37.1677358 UTC (12:29:37 ET)** as the primary observed study. It runs ridge and state-space
models using EUR/USD history alone, so peer-pair gaps cannot block them. Its
61-close current window, exact H1 labels and original outcome targets remain
enforced. The research allowlist has thirteen workers, adding only
`eurusd_local_forecast_study`; execution remains disabled. The **16:30:55 UTC**
check verified two forecast sets (four model records), their publications,
independent consumption and later quote matches for evaluation. There were
zero outcomes, exclusions or diagnostics. The first target is 17:29:37 UTC;
the unfinished forecasts remain unscored. See
[the current validation receipt](FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json).

The four-family `gap_v2` study remains active and unchanged as a separate cohort;
peer gaps still block its shared input window. The earlier 4/61 result is the
dated 16:09 UTC probe, not a current companion count. A bounded local EUR/USD
probe was ready in 0.0278 seconds and its two-model fit took 0.1465 seconds;
these are observed component timings, not proof of prediction quality. See
[the updated architecture and evidence](docs/FOREX_SIGNALS_LIVE_OPTIMIZATION_20260907.md).
Earlier activation/runtime descriptions below retain their recorded scope.

September 7 update: the separate **`gap_v2` research study activated at
2026-09-07T16:15:21.967284+00:00**. The twelve-worker research allowlist remains
unchanged, quote/news collection continues, and execution remains disabled.
The dashboard was reloaded with the signals/live-market comparison. Current
input gaps still cause warmup abstentions; improved prediction accuracy has
not been established.

Read [the signals and warmup repair record](docs/FOREX_SIGNALS_LIVE_OPTIMIZATION_20260907.md)
and [its validation receipt](FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json).
The new study requires 61 consecutive shared closes for current features and
uses exact-time training windows from retained valid history. It does not fill
missing prices. The previous `io_r2` study's 78 attempts, 78 abstentions and zero
forecasts are preserved in a separate snapshot; its registration and six source
files remain unchanged. These newer observations supersede the operating-state
and warmup descriptions in the dated history below. Source snapshot pointers
identify the last verified export and are refreshed after validation.

Current market-open update (2026-09-06T19:57:26.936771+00:00): **collection is running again** at user request, under one research supervisor with twelve admitted workers. This supersedes the earlier stopped runtime notes. **Practice trading remains disabled.**

[The readiness record](docs/FOREX_MARKET_OPEN_20260906.md) distinguishes healthy account/data connectivity from market-open price availability, the 335-minute research warm-up and pending predictive acceptance. Validation is `FOREX_MARKET_OPEN_VALIDATION_20260906.json`.

Earlier runtime observations below describe their recorded times.

Current update (2026-09-06T19:29:28.295035+00:00): **all Forex workers remain stopped**, and both Forex scheduled tasks are disabled. This supersedes every earlier collection/running note below.

[The entry audit and improvements](docs/FOREX_ENTRY_IMPROVEMENTS_20260906.md) confirm zero entries in the last completed week: candidates reached the executor but failed final conflict/cost rules. Exact-price scoring and joint-predictor baselines are implemented offline; predictive improvement remains unproven. Follow-up work is recorded in `FOREX_ENTRY_RESEARCH_FOLLOWUP_20260906.json` and validation in `FOREX_ENTRY_IMPROVEMENTS_VALIDATION_20260906.json`.

Earlier dated observations follow; their runtime states and backlog counts are historical.

Current sanity check (2026-09-06T15:53:01.014364+00:00): raw counts reproduce, with eight floating-point-only
direction hits corrected offline: **4,148/8,414 = 49.30%**; after-spread positives
remain **716/8,414 = 8.51%**. The four-model EUR/USD percentages reproduce exactly.
See [the prediction sanity check](docs/FOREX_PREDICTION_SANITY_20260906.md),
[model inventory](docs/FOREX_MODEL_INVENTORY_20260906.md) and
`FOREX_COMOVEMENT_RESEARCH_PLAN_20260906.json`. Scoring precision follow-up is open;
the research plan is inactive and the registered collecting study is unchanged.

Earlier dated observations below retain their original counts and scope.

Current update (2026-09-06T15:32:22.899414+00:00): the initial causal study had two Windows heartbeat
replacement failures. The fixed `io_r2` worker is separately registered and
running in collection mode; the original zero-forecast registration is preserved.
Trading stays off, and improved predictions still require fresh outcomes.
See [the current operational addendum](docs/FOREX_CAUSAL_IO_REPAIR_20260906.md)
and `FOREX_CAUSAL_IO_REPAIR_VALIDATION_20260906.json`.

Earlier dated observations follow; their registration details are historical.

Latest September 6 timing repair: collection now runs twelve workers, including
a separately registered four-family study. It is waiting for fresh market data;
trading remains disabled. Both prediction-clock fixes are implemented, with
future evidence still pending. See [the repair record](docs/FOREX_CAUSAL_TIMING_REPAIR_20260906.md)
and `FOREX_CAUSAL_TIMING_FOLLOWUP_20260906.json`. Earlier entries below are dated history.

Current runtime — September 6, 2026: Forex has restarted in collection-only
mode. Eleven data/dashboard/health workers run; execution and model/proof
production remain off. See [the restart record](docs/FOREX_RESEARCH_RESTART_20260906.md)
and `FOREX_RESEARCH_RESTART_VALIDATION_20260906.json`. Earlier stopped-state
observations below describe their recorded times and are now historical.

The fixed EUR/USD evaluation is complete: all four models underperform simple
baselines on the matching archived prices, while missing availability clocks
prevent strict causal scoring. Read [the fixed comparison](docs/FOREX_FIXED_EVALUATION_20260906.md)
and [remaining prerequisites](FOREX_FIXED_EVALUATION_FOLLOWUP_20260906.json).
The new offline evaluator passed 158 combined tests. Forex remains stopped.

September 6 optimization: the saved integrity summary shrinks from 98.9 MB
to 168 KB while retaining full detail separately; 131 offline tests passed
(one Windows symlink skip). See [optimization results](docs/FOREX_OPTIMIZATION_REVIEW_20260906.md),
[prediction quality](docs/FOREX_PREDICTION_QUALITY_20260906.md) and
[remaining priorities](FOREX_OPTIMIZATION_BACKLOG_20260906.json).
Current shadow signals had 49.39% directional accuracy and 8.51% positive
outcomes after spread. No reliable useful edge is demonstrated. Forex remains
stopped; these are source and saved-data findings.

September 5 repair and performance review: the independent audit findings have
source fixes and offline validation. The project remains stopped; the saved
degraded runtime audit is historical and has not been replaced by a live pass.
See [repair results](docs/FOREX_REPAIR_REVIEW_20260905.md) and
[performance findings](docs/FOREX_PERFORMANCE_AUDIT_20260905.md).
Source V9 and rank V8 ship disabled, with separate future activation required.

The project is intentionally stopped after the September 4 market-close
session for independent review and preparation for a future viewable demo.
Begin with [the audit guide](FOREX_AUDIT_START_HERE.md) and
[the latest recorded state](docs/AUDIT_STATE_CURRENT.md).

The canonical source/runtime is `C:\Users\zmoor\Documents\forex\trad`.
The vault is `C:\Users\zmoor\OneDrive\thevault\projects\forex`.
It holds source, configurations, tests, selected results and recovery records.
Original market/news observations and immutable runtime ledgers stay local.

The system collects executable FX prices and source observations, preserves
first-known timestamps, maps source factors to currency strength and evaluates
later bid/ask outcomes. Technical features support entry timing and cost
assessment. Independent evidence gates govern advancement to practice trades.
The latest completed audit supports `no_trade`; no confirmed edge is claimed.

- [Pending queue](FOREX_PENDING_IMPROVEMENTS.md): remaining work and evidence gates.
- [Issue register](FOREX_ISSUE_REGISTER_CURRENT.json): repair dispositions.
- [Project log](FOREX_PROJECT_LOG.md): dated changes and tests.
- [Current source/feature inventory](FOREX_AUDIT_STATE_CURRENT.json): lineages and timestamps.
- [Recreation guide](docs/VAULT_RECREATION_CURRENT.md): offline source verification and recovery limits.
- [Architecture reference](docs/SYSTEM_ORIENTATION_CURRENT.md): control flow.

The current recreation export includes uncommitted work and is explicitly a
working-tree snapshot with hashes for every file. The previous clean Git
baseline remains historical provenance. The legacy mixed-checkpoint bootstrap
does not recreate today's system. The prior README is preserved in
[historical orientation](docs/README_LEGACY_PRE_RESET_20260904.md).
