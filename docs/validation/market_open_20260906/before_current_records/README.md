# Forex research project

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
