# Two-hour spike account-space subsystem

The project now has two linked, research-only subsystems:

1. `significant_moves_pipeline.py` maintains the permanent 90/120/150-minute
   movement catalog for all discovered OANDA FX instruments.
2. `spike_account_space_research.py` asks whether causal vault features can
   gate those windows, whether a neutral two-pending OCO can capture them, and
   whether any risk/margin setup beats holding cash.

Neither subsystem connects to OANDA or places orders.

## Current production research snapshot

The full prepared account-space dataset contains 68 instruments, 3,205,732
valid 15-minute decision rows from 2024-07-01 through 2026-06-30 19:00 UTC,
20,521 persisted train-threshold significant labels, and 1,593 event clusters.
The completed bounded nested fit selected the no-trade baseline for every
tested starting balance: $50, $100, $1,000, and $10,000.

See `data/spike_account_space/reports/ACCOUNT_SPACE_REPORT.md` for the complete
interpretation and `spike_account_space/README.md` for the execution contract.

## Commands

```powershell
# Refresh/reuse the full 68-pair prepared dataset.
& '.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe' `
  .\trad\spike_account_space_research.py --stage prepare

# Reproducible bounded nested search.
& '.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe' `
  .\trad\spike_account_space_research.py --stage fit --bounded-fit

# Small integration check.
& '.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe' `
  .\trad\spike_account_space_research.py --stage all --smoke

# Ignore matching preparation and fit checkpoints.
& '.\trad\data\oanda_training_manager\.research_py313\Scripts\python.exe' `
  .\trad\spike_account_space_research.py --stage all --bounded-fit --force
```

The uncapped fit uses the complete configured grids and can be substantially
more expensive. A bounded fit uses 200,000 classifier training rows, 60 HGB
iterations, gate quantiles 0.95/0.99, cross-sectional top 1/3, eight OCO
configurations, and 32 account configurations per outer search.

## Interpretation rules

- Exact two-hour labels are timestamp joins on a strict UTC grid, never “next
  N observations.”
- The vault source is left-labelled, so features move five minutes to their
  first knowable timestamp. Execution waits another minute before reading the
  completed bar.
- Event clusters and future path fields are hindsight metadata. They group
  validation and analyze outcomes; they cannot enter the movement gate or
  account ordering score.
- Instrument event thresholds are fitted on chronological training data only.
- Outer folds are not touched until gate, OCO, and account choices are frozen.
- Cash/no-trade is an eligible zero-objective policy. An active policy must
  beat it; leverage never rescues negative expectancy.
- A new forward shadow period is mandatory. Retrospective outer folds are not
  authorization for practice or live trading.

## Durable outputs

All outputs live in `data/spike_account_space/`:

- `instrument_inventory.csv`
- `features/decision_features.parquet`
- `labels/exact_120m_labels.parquet`
- `clusters/event_cluster_candidates.parquet`
- `folds/fold_summary.csv`
- `models/gate_trials.parquet`
- `oco/oco_trials.parquet`
- `oco/outer_test_outcomes.parquet`
- `account/account_trials.parquet`
- `account/outer_test_trades.parquet` only when a policy actually trades
- `reports/outer_fold_results.json`
- `reports/account_scenarios.csv`
- `manifests/dataset_manifest.json`, `fit_manifest.json`, and `fit_state.json`
- resumable per-pair and per-outer-fold checkpoints under `checkpoints/`

Contract-versioned fingerprints prevent checkpoints from surviving semantic
changes to the dataset or fit engine. Artifact writes are atomic, and a
no-trade rerun explicitly removes a stale prior trade file.
