# Recreate and inspect the current Forex source

Latest September 7 architecture: the `eurusd_v1` companion activated at
**16:29:37.1677358 UTC (12:29:37 ET)** and adds only
`eurusd_local_forecast_study` to a thirteen-worker research allowlist. It runs
two EUR/USD-only models independently of peer gaps, while the existing
four-family `gap_v2` cohort remains active and unchanged. Execution remains
disabled. It is the primary observed study. The 16:30:55 UTC receipt verified
two sets/four model records with publications, independent consumption and
later quote matches for evaluation; no outcome had matured. These are runtime
observations, not evidence recreated by extracting source. See
`SIGNALS_LIVE_OPTIMIZATION_VALIDATION_CURRENT.json`.

When present in the verified source manifest, recreation includes the separate
EUR/USD adapter, numerical models, ledger, worker and registration alongside
the preserved earlier studies. Retain each cohort's separate database and
activation clocks; a source extraction does not recreate its prospective
forecasts or original observation history. The canonical mapping remains
143 records. Earlier twelve-worker and shared-warmup descriptions below retain
their dated scope, and the source pointer is refreshed after final validation.

September 7 update: the canonical project activated the separate `gap_v2`
research study at **2026-09-07T16:15:21.967284+00:00** and reloaded the dashboard.
The twelve-worker research allowlist and quote/news collection remain unchanged;
execution is disabled. Current missing minutes still cause warmup abstentions.
This source repair does not establish improved forecasting accuracy.

Read `SIGNALS_LIVE_OPTIMIZATION_CURRENT.md` and
`SIGNALS_LIVE_OPTIMIZATION_VALIDATION_CURRENT.json` in the vault, or
`docs/FOREX_SIGNALS_LIVE_OPTIMIZATION_20260907.md` and
`FOREX_SIGNALS_LIVE_OPTIMIZATION_VALIDATION_20260907.json` in canonical source.
The canonical sync mapping now contains **143 records**, including these two
new records. Use the export receipt and `source/WORKTREE_SOURCE_LATEST.json`
to verify the actual copied versions; the source pointer is refreshed after
validation and must not be assumed to contain unexported working-tree changes.

Source recreation includes the new gap-aware adapter/models, exact-scoring
study integration, bounded updater recovery and dashboard code when present in
that verified manifest. It does not reproduce the activated runtime or its
original observation history. The predecessor's 78 attempts, 78 abstentions and
zero forecasts remain in the retained local snapshot, with its old registration
and six source files unchanged. Preserve both cohorts and their actual clocks;
do not import old rows as new prospective evidence. Earlier operating-state
notes below describe their recorded times.

Current runtime update (2026-09-06T19:57:26.936771+00:00): the canonical C project has resumed collection only. Source recreation remains an offline operation and does not recreate its databases or authorize trading. See the market-open readiness record for current observations.

Earlier runtime observations below describe their recorded times.

Current disposition (2026-09-06T19:29:28.295035+00:00): runtime stopped at user request. The latest source includes offline exact scoring, joint-return baselines and entry diagnostics; these do not authorize a restart. Begin with the vault entry-improvement records and its latest source manifest.

Earlier dated observations follow; their runtime states and backlog counts are historical.

The runtime is intentionally stopped after September 4, 2026 market close.
These steps verify and inspect source offline.

## Current source record in the vault

Read `source/WORKTREE_SOURCE_LATEST.json` in the Forex vault. It identifies a
versioned `forex_worktree_source_<id>.zip` and `.manifest.json` containing
current source, configuration, tests, docs and validation receipts. Every file
is hashed. The base Git commit and uncommitted state are explicit.

`source/SOURCE_BASELINE_LATEST.json` remains an older committed baseline.
Archived mixed-checkpoint pointers/import scripts are historical and must not
be used as today's recreation entry point.

## Offline verification and extraction

With Python 3.12+ and a local source copy:

```powershell
python -B tools/vault_worktree_snapshot.py --verify "C:\path\to\forex_worktree_source_<id>.manifest.json"
```

The verifier checks archive and member hashes, safe paths and ZIP CRC, extracts
to a temporary directory, then compiles Python syntax. It never imports the
trading modules, starts workers or contacts a broker. Syntax checks are not a
full integration test.

For inspection, verify the ZIP SHA-256 against the manifest and extract with
`Expand-Archive -LiteralPath <zip> -DestinationPath <new-empty-directory>`.
Entries are relative to the `trad` source root. Avoid extracting over live
source. Read the extracted README before installing or running anything.

## Dependencies and focused tests

`config/requirements-research-audit-20260905.txt` inventories the installed
Python 3.12 core research environment at this reset. It includes optional
packages and is not a universal platform compatibility guarantee. Narrower
profiles are in `requirements-forex-news.txt` and `config/runtime_requirements/`.
Use an isolated environment; keep private credentials outside source.

```powershell
python -m pytest -q -p no:cacheprovider test_vault_worktree_snapshot.py test_forex_vault_record_cleanup.py test_forex_model_vault_sync.py test_oanda_issue_register_validator.py
```

The fixture tests above work with recreated source. The following separate
check requires the canonical local evidence tree or a coherent restored backup:

```powershell
python oanda_issue_register_validator.py
```

Do not use this second command as a source-only restore acceptance test. The
historical issue register references runtime databases, snapshots and validation
artifacts deliberately excluded from the source ZIP. A source-only extraction
therefore reports missing evidence; that is an unavailable-history result, not
archive corruption or permission to mark the historical issues verified. Use
the ZIP/member-hash verifier above to validate source-only recovery, and retain
the missing-evidence checks when auditing the complete canonical project.

## Historical evidence and later restart

Code and formulas recreate software, not the original observation history.
Canonical runtime data remain under `data/oanda_training_manager`. Preserve
the original ledgers or a coherent database backup to reproduce exact causal
evidence and account attribution. With source alone, initialize new datasets
and new cohorts; later vendor downloads do not recover original first-seen
times or forecasts made before outcomes.

The vault `maintenance/` receipt identifies the intact local legacy archive.
Verify its file hashes before recovering old artifacts. The archive is
historical, not a supported executable image.

Shutdown disabled `ForexSafeCoreAtLogon`; re-enable it only when starting the
runtime again is wanted. Start scripts remain in source but neither a verifier
nor an offline demo should call them. The shared BIGTRIAD dashboard logon task
is managed separately and has not been changed.

Before a later practice restart, check live local account state, time,
supervision, frozen contracts and lifecycle status. A restored report cannot
authorize a trade. Real-money routing remains disabled.
