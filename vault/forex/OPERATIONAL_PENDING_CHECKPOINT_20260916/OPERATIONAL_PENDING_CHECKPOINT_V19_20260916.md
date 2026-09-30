# Operational pending checkpoint — V19 joint-news isolation

Updated: 2026-09-16T13:54:42.176462Z

## Current verdict

The no-orders operational stack is running under `config/operational_runtime_v19_20260916.json` and the watchdog reports `role_liveness_current` with no owned role failures. Trading/order promotion remains disabled (`research_only=true`, `can_place_orders=false`).

V19 does not claim the joint price/news model is repaired. It replaces the failing supervised `joint_price_news_study_v9` role with `joint_price_news_isolation_status_v1`, a fresh heartbeat publisher that records the source blocker while allowing the rest of the project to stay supervised and readable.

## Live health snapshot

- Watchdog PID: `14652`
- Supervisor PID: `17880`
- Operational profile SHA: `78b2d002fb45eb252a439a1080ae69888ca2e05ebd0c82fb4a2f933cbc1de9d9`
- Watchdog health/action: `role_liveness_current` / `none`
- Restart circuit open: `False`
- Supervisor managed roles: `18`
- Supervisor error: ``
- Owned role failures: `[]`
- Missing owned roles: `[]`

## 68-pair feature feed

`data/oanda_training_manager/state/all68_derived_technical_features_v1.json`

```json
{
  "current_pairs": 68,
  "fully_finite_pairs": 60,
  "histories_read": 68,
  "minimal_feature_count": 9,
  "minimal_feature_current_pairs": 68,
  "minimal_feature_finite_pairs": 68,
  "movement_feature_count": 2,
  "movement_feature_current_pairs": 68,
  "movement_feature_finite_pairs": 68,
  "registered_pairs": 68,
  "source_errors": 0,
  "status_counts": {
    "derived_current": 1,
    "observed_current": 67
  }
}
```

The minimum requirement is met: all 68 registered pairs have readable current minimal features and movement features, with zero source errors. `fully_finite_pairs` is `60` because some richer optional features still have expected missing/derived values; that is not the same as the minimal feed being absent.

## Forward-feature workers

- Primary M1 forward worker: `observing`, generated `2026-09-16T13:54:38.397999+00:00`.
- Cached M5 forward worker: `observing`, generated `2026-09-16T13:54:41.393356+00:00`, readiness `{'final_source_age_limit_seconds': 75, 'latest_source_epoch': 1789566848.056231, 'maximum_start_age_seconds': 40, 'prior_clock_comparisons': 21, 'ready': True, 'reason': None, 'scope': 'clock support only; each feature still needs five valid prior deltas', 'source_age_seconds': 33.12780141830444}`.

## Joint price/news isolation

`data/oanda_training_manager/operational_repair_20260916_news_v7/joint_price_news_isolation_status_v1/heartbeat.json`

```json
{
  "schema_version": "joint_price_news_isolation_status_v1_20260916",
  "worker": "joint_price_news_isolation_status_v1",
  "status": "isolated",
  "phase": "research_collection_isolated",
  "reported_failure": false,
  "isolated_reason": "bounded_validation_capture_blocker",
  "source_status": null,
  "source_last_error": "news_bootstrap:ValueError:stream_validation_time_bound",
  "generated_utc": "2026-09-16T13:54:36.887918Z"
}
```

The original joint cohort remains pending because the source heartbeat still reports `source_last_error=news_bootstrap:ValueError:stream_validation_time_bound`. The next correct fix is a successor joint cohort with bounded, chunked capture/validation and new source/config bindings, not a silent patch to the existing pinned source chain.

## Files changed for this checkpoint

- `oanda_joint_price_news_isolation_status_v1.py` — `33163e1b8ae98d2c6a3c49f70c0171a959ae84ad5a31e3882221b2bf92de1f90`
- `test_oanda_joint_price_news_isolation_status_v1.py` — `5352a375fc945d0155e9fba360179a71df24dbda2649fc351d79ab580d94fd7d`
- `oanda_operational_recovery_contract_v6.ps1` — `df9bb39dae923fa6e5c01d73c40b33a58b9b965e6c2df09b0e81338db8da4b99`
- `test_oanda_operational_recovery_v6.py` — `73ae9ddeaf8643b3f5f44e105179dfd7eab8a1f77560e4d8eb2d0d35d96b5d9e`
- `config/operational_runtime_v19_20260916.json` — `78b2d002fb45eb252a439a1080ae69888ca2e05ebd0c82fb4a2f933cbc1de9d9`

## Validation performed

- `python -m pytest -q test_oanda_joint_price_news_isolation_status_v1.py test_oanda_operational_recovery_v6.py` → 55 passed.
- `Read-OperationalRecoveryProfile` accepted V19 profile hash `78b2d002fb45eb252a439a1080ae69888ca2e05ebd0c82fb4a2f933cbc1de9d9` with recovery allowed.
- Controlled V18 → V19 cutover completed after backing up/resetting transition restart ledgers.

## Ledger/reset notes

The V18/V19 transition tripped bounded restart ledgers because the support contract changed and the old profile failed closed. Backup files were written before resets:

- `data/oanda_training_manager/state/operational_watchdog_restarts_v2.json.before_v19_profile_switch_*`
- `data/oanda_training_manager/state/operational_supervisor_restart_v2.json.before_v19_ledger_reset_*`
- `data/oanda_training_manager/state/operational_watchdog_restarts_v2.json.before_v19_ledger_reset_*`

## Remaining pending work

1. Build a successor joint price/news cohort instead of reviving `joint_price_news_study_v9` in place. Requirements: bounded chunked news capture, validation progress checkpoints, no whole-archive repeated capture, source-bound config, and replay tests against the existing archive.
2. Continue the model-space audit/research plan separately. Current operational readiness does not imply profitable prediction.
3. Optional cleanup: normalize the nested relay process presentation if desired. It is currently operationally healthy because the supervisor tracks a fresh heartbeat and the watchdog has no role failures.
4. Keep V19 in no-orders research mode until there is evidence-based authorization to change order behavior.
