# Start here

## Current combined-observation checkpoint — October 1, 2026

[Combined quote and forecast observations](docs/RETAINED_COMBINED_OBSERVATION_20261001.md): offline integration complete; 62 tests and 84 exact resumed/restored payloads passed. All 2,652 slots covered. Preserved inputs produce zero fresh priced observations at the replay clock; live rollout and position management remain unfinished. Next `retained_management_contract_qualification_v1`. Older notices below are historical.


## Current retained-receipt checkpoint — October 1, 2026

[Retained forecast receipts](docs/RETAINED_FORECAST_RECEIPTS_20261001.md): separately named observation adapter over saved publications;69tests and84payload exact resumed/restored replay passed. Preserves4953observation receipts across2652slots;2418forecasts eligible only at the frozen capture clock. No new inference or activation. Next `retained_management_combined_observation_v1`; combine exact quote and forecast observations offline. Older notices below are historical.


## Current quote-receipt checkpoint — October 1, 2026

[Exact quote receipts](docs/EXACT_QUOTE_RECEIPTS_20261001.md): opt-in decimal-preserving stream hook, bounded sidecar and existing-manager adapter implemented offline. 137 tests passed;64 retained raw records parsed and replayed exactly after isolated restore. No live rollout or management activation. Next `retained_management_forecast_receipts_v1`; adapt saved forecast receipts under their real input tier. Older next-item notices below are historical.


## Current management-readiness checkpoint — October 1, 2026

[Retained management readiness](docs/RETAINED_MANAGEMENT_READINESS_20261001.md): full 2,652-slot inspection, 48 tests and 43-payload portable/resumed replay passed. Forecasts remain connected; management is not activated. Missing native receipts, exact quote identity/decimal clocks and economic/state contracts are explicit. Next `retained_management_quote_receipts_v1`; repair the existing quote receipt boundary offline. Older notices below are historical.


## Current full-horizon connection checkpoint — October 1, 2026

[Preserved parents and curve](docs/RETAINED_CURVE_CONNECTION_20261001.md): 39 connections across 14 elapsed horizons; exact eight-horizon Ridge/HGB panels and two negative-result curve research references. No fits. 105 Python tests, four dashboard scenarios and 2,476-output relocated replay passed. Next `retained_position_management_readiness_v1`. Position management and exact-target gaps remain open; earlier notices below are historical.


## Current learned-state checkpoint — October 1, 2026

[Preserved learned residuals](docs/RETAINED_LEARNED_RESIDUAL_CONNECTION_20261001.md): two distinct saved 18h layers connected; zero-weight 6h states alias existing projections. 25 connections, no fits. 91 Python tests, three dashboard scenarios and 1,580-output relocated replay passed. Exact next `retained_curve_parent_panel_connection_v1`. Broader work remains unfinished; earlier notices below are historical.


## Current freshness checkpoint — October 1, 2026

[Freshness continuity](docs/RETAINED_FRESHNESS_CONTINUITY_20261001.md): existing saved inference now follows technical publication changes. 82 tests passed; seven native publications and six exact intervals showed no total forecast-expiry gap during the six-minute observation. Current news and 23 tracking groups verified. Next `retained_learned_residual_state_connection_v1` qualifies preserved state reuse without fitting. Broader project work remains unfinished; older notices below are historical.


## Current projection checkpoint - October1,2026

[Currency projection connection](docs/RETAINED_PROJECTION_CONNECTION_20261001.md):23connections across11elapsed horizons; eight fixed currency layers added to fifteen unchanged saved connections. Zero fits. Historical528output replication,1456output relocated replay and native23group tracking verified. Next `retained_input_freshness_continuity_v1` repairs the observed input-expiry gap. Learned residuals and position management remain unfinished. Older notices below are historical.


## Current checkpoint - October1,2026

[Saved6h/18h connections](docs/REMAINING_CONNECTIONS_20261001.md): fifteen connections across11 elapsed horizons; original11 preserved. No new fits. Bounded target and parent reconciliation is complete with16 unqualified exact targets explicit. Next `retained_currency_projection_connection_v1`. Layer integration and position management remain unfinished. Earlier next-item notices below are historical.


## Understand the project

The goal is better Forex forecasts through technical features, news/context layers
and position-rotation comparisons, using matched controls, causal timing and costs.
Mean reversion, specialist models and forecast-error calibration already have studies.

1. Read [current findings](docs/PROJECT_STATE.md).
2. Read the [included Vault guide](vault/README.md) for the design, queue and reviews.
3. Use the [folder map](docs/DIRECTORY_MAP.md) and [artifact guide](docs/ARTIFACT_REUSE.md).

No installation is required to read the project. The Vault snapshot is included so a
fresh clone does not depend on the original Windows user folder. For an old absolute
path containing `thevault/projects/forex`, look for the same suffix under `vault/forex`.
Missing artifacts are listed in the snapshot manifest; historical paths remain provenance.

## Set up Python

On Windows, from the repository root:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-engineering.lock.txt
```

This installs software only. It does not start feeds or use broker credentials.
The engineering contract records Python 3.12.10. Saved models can require stricter
original environments: check their recipes before loading or numerical reuse.
Read-only status and snapshot verification need only the standard library and Python 3.10+.

```powershell
python tools/forex_vault_snapshot.py verify --destination vault/forex
```

## Open the dashboard

```powershell
.\.venv\Scripts\python.exe trad/oanda_practice_live_dashboard.py --host 127.0.0.1 --port 8765
```

Open **http://127.0.0.1:8765/** on that machine. Keep the terminal running; Ctrl+C stops
the server. If the port is occupied, inspect the existing dashboard first. On a phone,
`127.0.0.1` refers to the phone, not the PC.

The dashboard reads local records. A fresh clone has no live prices, account snapshots
or model outputs. Missing/stale sources display unavailable; loading the page does not
restart collectors or trading. On the original machine the September30 recovery restored
fresh quotes, news and price forecasts. Joint history validation passed; current
joint forecasts still need mature, varied prospective history. Eleven retained model connections cover 5/15/30/60 minutes and 4/12/24/48/120 elapsed hours; see [current coverage](docs/LEGACY26_CONNECTION_20260930.md). Use
[the measured status](docs/DASHBOARD_STATUS_20260930.md) and the
[research-pipeline commands](docs/PIPELINE_OPERATIONS.md), not older stale snapshots.
Crypto has been removed from the active dashboard.

## Continue engineering

Read [FOREX_HANDOFF.md](FOREX_HANDOFF.md). Use your live shared Forex Vault for current
claims and queue changes; never claim work by editing the bundled snapshot's board.

```powershell
python -I -B tools/forex_workspace.py status --vault 'C:/your/shared/thevault/projects/forex'
python -I -B tools/forex_preflight.py --vault 'C:/your/shared/thevault/projects/forex'
```

Run preflight against the current live Vault and exact shared revision. Historical
September30 pointer mismatches were repaired; a fresh clone alone does not establish
current readiness. Fix any reported prerequisite without weakening identity checks. [Retrieve existing artifacts](docs/ARTIFACT_REUSE.md)
before considering a fit.

Keep raw outputs locally, publish compact handoffs to the live Vault and link the same
identities in project logs. Refresh the Git documentation copy using the
[export procedure](vault/README.md#refreshing-the-copy).

## Current next work

The user stopped new research/fitting and requested reliable data plus the strongest
supported retained models across the entire design horizon. Read
[data and model connection](docs/DATA_AND_MODEL_CONNECTION_20260930.md).
Eleven saved-model connections across nine elapsed horizons are connected with scoped development evidence;
remaining horizons and universal best-model qualification are unfinished.
Exact next: `data_freshness_and_remaining_horizon_reconciliation_v1`.
The typed-news experiment is deferred. No profitable deployment is qualified.
