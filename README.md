# Forex research

Forex forecasting research, backtesting, and market-data analysis. The project compares
technical indicators, news context, forecast layers and position-rotation policies.
Research results and a running dashboard are not evidence of a profitable trading system.

**New here? Read [START_HERE.md](START_HERE.md).** It explains setup, current limitations
and how to continue without repeating existing experiments.

| I want to… | Start here |
|---|---|
| Understand the current work | [Project state](docs/PROJECT_STATE.md) |
| Read the design, queue and reviews | [Included Forex Vault knowledge](vault/README.md) |
| Run the local dashboard | [Quick start](START_HERE.md#open-the-dashboard) |
| Inspect or recover the local research pipeline | [Pipeline operations](docs/PIPELINE_OPERATIONS.md) |
| Find existing models and data | [Artifact retrieval](docs/ARTIFACT_REUSE.md) and [evidence index](artifacts/research_evidence_index.json) |
| Continue engineering | [Handoff](FOREX_HANDOFF.md), then the live shared Vault coordination board |
| Find a folder or older result | [Directory map](docs/DIRECTORY_MAP.md) |

```powershell
git -c core.longpaths=true clone --branch forex https://github.com/shaggyga/tradez.git forex
cd forex
```

Git is the shareable project: source, setup instructions, tests, model definitions,
artifact references and a versioned copy of Forex Vault knowledge. The live shared
Vault remains the coordination authority. The included copy is a dated reading
snapshot; its claims and old `CURRENT` filenames are not live worker status.

Large datasets, fitted model binaries, credentials and machine-local runtime state
are stored separately. The [snapshot manifest](vault/forex/SNAPSHOT_MANIFEST.json)
lists copied documents and omissions; not every restore artifact is in Git.

The active dashboard is Forex-only. Crypto Shadow integration and its Forex startup
entry have been removed; historical crypto source/results remain for reproducibility.
Cloning or opening the dashboard does not start collectors, models or trading.

Detailed setup: [shared workspace](docs/SHARED_WORKSPACE.md).
Older layouts and source coverage: [source coverage](docs/SOURCE_COVERAGE.md).
