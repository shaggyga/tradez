# Start here

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
