# Config Directory

This directory contains the runtime JSON files that decide which account,
model stream, and risk settings each bot uses.

## Active Files

| File | Role |
|---|---|
| `canary_primary_forecast_rotation_bot.json` | Active canary demo account config. This is the current account-004 test surface. |
| `tech003_primary_forecast_rotation_bot.json` | Tech003 demo config. Execution is disabled so this account stays stopped. |
| `primary_forecast_rotation_bot.json` | Primary live-account rotation config. Do not assume it matches canary. |
| `tech_forecast_rotation_bot.json` | Tech account rotation config used by older tech-account tests. |
| `accounts_registry.json` | Alias map for account IDs and environment variables. |
| `model_feature_space.json` | Feature-set definitions used by training/research scripts. |
| `runtime_path_aliases.json` | Path indirection for local runtime artifacts. |
| `model_space_agenda.json` | Research agenda/spec grid for model-space exploration. |
| `project_layout_v1.json` | Canonical compatibility-first domain, path, safety, and migration contract. |
| `module_migration_registry_v1.json` | Critical live-module ownership and guarded migration states. |

## Current Canary Selection

`canary_primary_forecast_rotation_bot.json` is intentionally set to the best
practical strict path-quality setup found on 2026-07-08:

```text
model_stream: fresh_fullhist_m30_h1_h4_continuation_oanda_20260707
m1_overlay.hold_threshold: 0.50
max_new_positions_per_cycle: 12
atr_stop_multiplier: 1.50
take_profit_edge_capture: 0.60
take_profit_min_r_multiple: 0.50
trailing_stop_r_multiple: 1.00
```

The config also contains a `deployment_notes` object that points to the selected
backtest artifact and explains why max-new 12 was chosen over max-new 16.

## Execution Gates

Demo execution requires all of the following:

- CLI includes `--execute`
- `default_mode` or CLI mode is `demo`
- `demo_execution_enabled` is `true`
- practice account aliases resolve from local credentials/environment

Live execution has separate gates and should not be inferred from demo config.

## Stopping A Demo Account

To stop a demo account, stop its process and set:

```json
"live_new_entries_enabled": false,
"demo_execution_enabled": false
```

That is currently applied to `tech003_primary_forecast_rotation_bot.json`.
