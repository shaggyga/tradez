# Spike account-space research

This package evaluates a causal movement gate followed by two pending OCO
breakout orders, then replays the resulting opportunities through conservative
account constraints. It is offline research code. It cannot place, modify, or
cancel an OANDA order.

## Neutral table contract

`evaluate_oco_candidates(decisions, bars, config)` requires one decision row
per causal signal with:

`decision_id, timestamp, instrument, movement_score, movement_threshold,
atr_pips, spread_pips, expected_net_edge_pips`

`currency_theme_cluster_id`, fold IDs, and account-economics columns are copied
through when present. Bars require UTC `timestamp`, `instrument`, `pip_size`,
and bid/ask OHLC columns named `bid_open` ... `bid_close` and `ask_open` ...
`ask_close`. Input timestamps are sorted once per instrument and windows are
located with binary search, so decisions do not repeatedly scan full histories.

The OCO result exposes the primary and sibling entry/exit timestamps, resolved
ATR/fixed stop and target distances, filled and reserved leg counts, gross and
net pips, spread/slippage estimates, MFE/MAE, exit reasons, path completeness,
and explicit ambiguity flags. Realized path fields are hindsight-only and are
marked as such in the output metadata.

## Conservative OHLC conventions

- Stop entries can use fixed-pip, spread-multiple, ATR-multiple, or combined
  buffers.
- A decision sees bars strictly after its timestamp.
- If both pending stops touch in one bar, the configured policy is `skip`,
  `double_fill`, or `adverse_first`. The last option selects the worse possible
  single fill when cancellation is immediate.
- A positive cancel latency can fill the sibling and reserves two-leg risk and
  margin from the first fill.
- The trigger bar can contain extremes from before entry. It may stop the trade
  under the adverse-first rule, but its favorable extreme can never earn a TP,
  trailing activation, or MFE. Favorable processing starts on the next bar.
- If TP and SL are both touched after entry in one OHLC bar, SL wins. A trailing
  level activated by a bar becomes executable on the following bar.
- Truncated trigger or exit paths are not executable account candidates.

These rules are deliberately pessimistic. Tick replay is required to resolve
intrabar order instead of assuming it.

## Account economics and constraints

`replay_account` accepts an optional neutral `EconomicsProvider`, or explicit
row fields:

- `risk_per_pip_account_per_unit` at entry;
- `pnl_per_pip_account_per_unit` at exit, or the preferred exact
  `realized_pnl_account_per_unit` for converted double-leg P&L;
- `margin_per_unit_account` for one filled leg.

For USD accounts only, explicit `pip_value_usd_per_unit` can be the pip-value
fallback. Missing conversion or margin data blocks the candidate. Margin is
multiplied by `reserved_legs`; this protects a cancellation-latency double fill
even when only one leg eventually fills.

The replay enforces per-trade and total risk, theme risk and position caps,
instrument and concurrency caps, new-position limits, used-margin limits,
stressed stop-risk margin buffer, daily realized-loss stops, and realized
drawdown halts. Financing and non-pip costs can be supplied per unit.

MAE has no timestamp in the neutral trade result. The replay therefore reports
a deliberately conservative diagnostic that assumes concurrent positions hit
their individual MAE together. Future MAE never decides whether a trade opens,
so it cannot leak into selection. Any MAE-stress closeout diagnostic hard-fails
the research objective. Exact historical margin-closeout timing requires a
timestamped mark-to-market path adapter.

## Search and promotion rule

`search_account_space` and `search_joint_space` always include a no-trade row
whose objective is exactly zero. Candidate ordering is restricted to a causal
score column; names resembling targets, future outcomes, P&L, MFE/MAE, or exits
are rejected. The fold objective is log growth minus drawdown, CVaR, turnover,
concentration, and sample-size penalties. Its stable score uses the fold median,
median absolute deviation, and negative-fold share. Ruin, causal margin-closeout
risk, or retrospective MAE-stress closeout receives the hard-failure score.

A configuration is only a research candidate when its stable score is above
the no-trade baseline. It is not trade-ready until frozen chronological outer
folds and subsequent prospective practice replay remain positive after all
conversion, spread, slippage, financing, latency, and margin assumptions.

Minimal usage:

```python
from trad.spike_account_space import (
    AccountConfig, OCOConfig, evaluate_oco_candidates, search_account_space,
)

outcomes = evaluate_oco_candidates(decisions, executable_bars, OCOConfig())
search = search_account_space(outcomes, [AccountConfig()], fold_column="fold")
print(search.leaderboard)
```

Dataset/cache, event clustering, purged split, and train-only threshold details
are in `README_DATASET.md`. Broker economics assumptions are in `ECONOMICS.md`.

## Reproducible project run

From the project root, use the project research interpreter:

```powershell
& '.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe' `
  .\trad\spike_account_space_research.py --stage prepare

& '.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe' `
  .\trad\spike_account_space_research.py --stage fit --bounded-fit
```

`--stage all` runs both stages. `--force` now bypasses both pair preparation
and fit checkpoints. A bounded fit is the reproducible first-pass budget; the
uncapped configured grid is intentionally much larger.

Pair checkpoints are keyed by the dataset contract and input signatures. Fit
checkpoints are keyed by the prepared dataset fingerprint, exact train/test
positions, ordered OCO and account grids, effective config, and fit contract
version. A prepare run writes `manifests/fit_state.json` as
`STALE_PENDING_REFIT`; only a completed fit changes it to
`CURRENT_RETROSPECTIVE_RESEARCH_ONLY`.

The durable evidence outputs include:

- `models/gate_trials.parquet`
- `oco/oco_trials.parquet`
- `oco/outer_test_outcomes.parquet`
- `account/account_trials.parquet`
- `reports/outer_fold_results.json`
- `reports/account_scenarios.csv`
- `manifests/dataset_manifest.json`, `fit_manifest.json`, and `fit_state.json`

Trade and OCO files are atomically replaced. If a recomputed policy selects no
trades, any older trade artifact is removed before the completion manifest is
written; it cannot be mistaken for the new run.
