# Start here

Current operational handoff: [feed and tracking repair](docs/FEEDS_TRACKING_REPAIR_20261002.md). Outcome tracking now rolls into bounded segments and is live; original observations and news links are preserved. Currency ranking remains live. Missing M1 support and economic management inputs remain explicit.

Earlier checkpoint notices are preserved in [START_HERE.md history](docs/history/ranker_cleanup_20261002/START_HERE.md); resolve the live Vault pointers for the current queue.

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

Tracking capacity repair is complete. Assess genuine missing M1 history under
`rolling_m1_elapsed_support_recovery_v1`; separately resume
`retained_management_economic_input_binding_v1`. Current currency ranking is an
observed-strength model, separate from retained horizon forecasts and trading readiness.
Use live Vault `PROJECT_CONTEXT_LATEST.json` and `REVIEW_QUEUE.json` for exact evidence.
New fits, advisor comparisons and order actions remain outside this repair.
