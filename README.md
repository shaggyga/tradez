# Forex shared source

The Vault is the shared brain. This repository holds the available project source,
model definitions, configurations, tests and artifact references. Each person uses a
local clone; the current design, queue, ownership and reviewed results live in their
synced `thevault/projects/forex` folder. Resolve the live pointers before work.

## Start on another machine

```powershell
git clone https://github.com/shaggyga/tradez.git forex
cd forex
python -I -B tools/forex_workspace.py status --vault 'C:/path/to/thevault/projects/forex'
python -I -B tools/forex_workspace.py doctor
```

Git credentials are machine-local. Python 3.10+ is sufficient for status and byte
retrieval. Numerical engineering uses Python 3.12.10 and the bounded observed profile
in `requirements-engineering.lock.txt`; individual recipes retain their own exact
environment gates. Use a local virtual environment for dependencies. This publication
does not install packages, launch services or authorize broker access.

Read [shared setup and artifact retrieval](docs/SHARED_WORKSPACE.md), then
[the handoff](FOREX_HANDOFF.md). Historical absolute paths in evidence describe the
original machine; supply your own Vault path to the helper. Current scientific source
and its source hashes were preserved, including historical path limitations.

## Reuse before running

1. Read the Vault's `DESIGN_ALIGNMENT_LATEST.json`, `CHECKPOINT_REVIEW_LATEST.json`,
   `REVIEW_QUEUE.json`, `VAULT_FIRST_REUSE.md` and `CHAT_COORDINATION_BOARD.md`.
2. Search exact existing model/run identities and claim eligible work on the board.
3. Retrieve matching completed artifacts. Do not retrain because a local file is absent.
4. Record code commit, input/model identities and evidence; publish a compact handoff
   to the Vault and link it from `trad/FOREX_PROJECT_LOG.md`.

The retrieval registry currently pins one existing eight-model run and its outputs.
It verifies/copies bytes without importing or fitting models. The Vault contains
additional historical model catalogs and recoveries. The registry is not exhaustive;
the synced coordination board is not an atomic distributed lock.

## Layout and history

- `trad/`: existing system, tools, configuration and tests; its Git history is retained.
- `stage_c_alignment_integrity_v2/`: current engineering sources, contracts and recipes.
- `stage_b_20260921/`, `stage_c_all68_20260921/`: retained prerequisite sources.
- `design_alignment_20260921/`: governing design and audits.
- `direction_*`, `currency_meter_continuous_20260912/`: retained source dependencies.
- `tools/`, `artifacts/`, `docs/`, `tests/`: portable sharing/retrieval helper and checks.
- Other dated folders preserve historical Forex utility source for reuse and inspection;
  their old launch/publish scripts are historical evidence, not current instructions.

Older commits predate the root migration and have `trad` files at their repository root.
Current runtime folder paths are preserved. Raw data, active evidence, fitted weights,
credentials and local environments are excluded from new source additions. Historical
tracked evidence remains in history. See [source coverage](docs/SOURCE_COVERAGE.md).

The origin URL can move without changing model identity:
`git remote set-url origin <new-repository-url>`.
Use branches, review changes and ordinary non-force pushes. Pull/fetch before starting
work, but do not overwrite local changes or another owner's in-progress scope.
