# V18 derived supervisor cutover — September 16, 2026

This checkpoint registers the all-68 derived technical publisher into the supervised no-orders runtime and repairs the derived publisher so short internal candle gaps are retained for explicit causal fill instead of collapsing the pair history to a one-row suffix.

## Change

- Added V6 operational support scripts:
  - `oanda_operational_recovery_contract_v6.ps1`
  - `oanda_operational_supervisor_v6.ps1`
  - `oanda_supervisor_watchdog_v6.ps1`
  - `start_oanda_operational_research_v6.ps1`
  - `start_oanda_supervisor_watchdog_v6.ps1`
- Added `config/operational_runtime_v18_20260916.json` with 18 managed research roles.
- Added the new managed role `all68_derived_technical_publisher_v1`.
- Repaired the derived publisher heartbeat contract by emitting `schema_version` alongside `schema`.
- Repaired the publisher source reader for derived features: it now keeps the bounded validated raw tail across short intraday gaps so the existing causal derived kernel can fill those gaps with explicit imputation masks. This fixed the live `EUR_DKK`/`TRY_JPY` one-row suffix problem without changing the shared candle reader or fabricating original source rows.

## Live readback after final restart

- Supervisor schema: `operational_supervisor_v6_20260916`
- Supervisor PID: `6556`
- Supervisor profile: `C:\Users\zmoor\Documents\forex\trad\config\operational_runtime_v18_20260916.json`
- Supervisor profile SHA-256: `00e9f57656d662f8ca763a3f36a4bc72e006c132e213fd37d7250b374ac3debd`
- Managed roles in heartbeat: `18`
- Watchdog schema: `oanda_supervisor_watchdog_v6`
- Watchdog PID: `17992`
- Watchdog health: `degraded`
- Orders: `can_place_orders=false` / `real_money_enabled=false`
- Derived role running: `true`
- Derived role freshness: `fresh`
- Derived role observed schema: `derived_technical_publisher_v1_20260916`

Latest derived feature publication:

```json
{"current_pairs":68,"fully_finite_pairs":60,"histories_read":68,"minimal_feature_count":9,"minimal_feature_current_pairs":68,"minimal_feature_finite_pairs":68,"movement_feature_count":2,"movement_feature_current_pairs":68,"movement_feature_finite_pairs":68,"registered_pairs":68,"source_errors":0,"status_counts":{"derived_current":1,"observed_current":67}}
```

- Publication generated at: `2026-09-16T13:06:31.315832+00:00`
- Analysis anchor: `2026-09-16T13:05:00+00:00`
- Schema version: `derived_technical_publisher_v1_20260916`

## Validation

- `python -B -m pytest -p no:cacheprovider -q test_oanda_derived_technical_publisher_v1.py test_oanda_operational_recovery_v6.py`
- Final result: `60 passed in 73.15s`
- Native Windows PowerShell validation of `start_oanda_operational_research_v6.ps1 -ValidateOnly` and `oanda_operational_supervisor_v6.ps1 -ValidateOnly` passed for `config/operational_runtime_v18_20260916.json` before cutover.

## Remaining degraded runtime items

The V18 cutover did not claim model profitability or repair every existing runtime warning. The watchdog still reports the runtime as degraded because these existing roles report failures while still running/fresh:

- `joint_price_news_study_v9`: `shared_news:BootstrapPending:prefix_verified_requires_fresh_read`
- `research_feature_forward_v2`: ``
- `research_feature_forward_cached_v2`: ``

These are the next operational targets. The derived all-68 feed itself is now supervised and fresh with 68/68 minimal and 68/68 movement readiness.
