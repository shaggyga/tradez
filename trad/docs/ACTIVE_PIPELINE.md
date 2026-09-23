# Active Forex pipeline

This is the September 7–8, 2026 documented architecture and disposition, not a fresh process census. The combined-study validation's runtime observation is September 7 at 22:48:10 UTC. Export time does not update that observation. In the vault, use `SYSTEM_GUIDE.md` and `KNOWLEDGE_INDEX.md` to resolve source members and dated evidence; source filenames below are ZIP members relative to `trad`.

Canonical project: `C:\Users\zmoor\Documents\forex\trad`.
Live view: [Signals vs live](http://127.0.0.1:8765/#oanda).

The active design is one path:

**Prices and vetted news → combined forecast → costs and risk → positions and measured results.**

## Where each stage lives

| Stage | Current owner | Evidence and purpose |
|---|---|---|
| Quotes and prices | `oanda_practice_quote_stream.py`, `oanda_all68_m1_forward_updater.py` | Actual bid/ask times and real M1 candles for 68 registered pairs; no inserted prices. |
| News collection | `oanda_local_news_sentiment.py`, official release collector/mapper, source-governance fast lane | Source publication, actual observation, immutable governed events and current news context. |
| News admission | `oanda_news_causal_aggregation_guard_v1.py` | Each member retains its own arrival and expiry. Corroboration requires independent support for the same claim. |
| Price research baseline | `oanda_pair_local_forecast_study_v2.py` | Independent Ridge and State-space family ledgers; real elapsed time; original H1 targets. |
| Combined price/news research | `oanda_joint_price_news_forecast_study_v2.py` | Active 68-pair registry, with alternating capture/fit work and separate per-class cursors. Learned price/news model, matched price-only comparison, explicit neutral-news ablation and original one-hour outcomes. |
| Display | `oanda_practice_live_dashboard.py`, `oanda_main_signal_dashboard.html` | One signals-vs-live surface: observed move, technical/news inputs, model estimate versus spread, positions and outcomes. |
| Account and positions | `oanda_account_snapshot_writer.py` | Read-only broker account/position observations. Research quote-entry records are not broker trades. |
| Operation | `oanda_always_on_supervisor.ps1` | Explicit research allowlist. Unknown workers and execution/promotion workers stay excluded. |
| Health and capacity | `oanda_project_integrity_audit.py`, `oanda_project_runtime_health.py`, `oanda_storage_headroom_guard.py` | Current worker observations, separately retained historical failures and monitored database/WAL growth. |

## Current authority

- `config/pair_forecast_primary_current.json` selects the displayed registered price study.
- `config/pair_local_forecast_study_v2_20260907.json` binds the price models, inputs, ledger and evaluator.
- `config/joint_price_news_study_v2_20260907.json` binds the active combined study. The API primary collection status follows this study; price-only collection status remains a named comparison.
- `oanda_news_classification_contract.py` identifies the active news interpretation. A new version requires a coordinated producer/mapper/display reload and fresh output verification.
- Runtime ledgers and captured news are under `data/oanda_training_manager`. Private credentials and complete runtime databases belong to the actual project, not the vault source export.
- Orders, promotion and proof eligibility remain disabled. Forecast availability is separate from measured accuracy and after-cost results.
- The scheduled `check-forex-bot-health` automation is paused. The requested one-hour watch was completed in this chat.

## Research and history

This page describes the running H1 path, not the entire implemented project. The older 227/220-feature models, 795-input eight-horizon curve, MA grid and second-ridge curves are real. See the September 8 existing-feature/horizon audit in [the research index](RESEARCH_INDEX.md) for retained results, missing C source/artifacts, D/AppData locations and the corrected recovery plan.

Use [the research index](RESEARCH_INDEX.md) for earlier cohorts and dated audits.
The previous pair-v1 study and joint scheduler-v1 continue as named comparisons. Joint-v1 remains running so its issued forecasts can receive their original one-hour outcomes. Original EUR/USD and shared-gap companions are stopped; their source, registrations, results and read-only backups are preserved.

Registered source paths remain stable because their imports, hashes and evidence refer to those paths. Navigation is organized here; versioned source and historical evidence are not silently renamed or rewritten.
