# Native compact news: permanent reproduction kit

This directory recreates 48 synthetic native-news integration, scheduling and archive regression tests against the exact source files in the selected project registry. It has no dependency on a sibling audit, worktree or `operational_repairs_20260913` directory. The source implementation remains in the project; this package contains fixtures and a runner, not another runtime implementation.

Run this after the compact 46-source registry is installed and selected. The preceding 45-source generation is intentionally refused.

```powershell
& 'C:\Users\zmoor\AppData\Local\CodexRuntimes\timeseries312\Scripts\python.exe' `
  'C:\Users\zmoor\Documents\forex\trad\docs\validation\native_news_compact_20260914\run_reproduction.py'
```

Use Python 3.12 with the project's existing numerical dependencies and `pytest`. The runner defaults to the surrounding `trad` project and its `config/operational_dashboard_current.json` joint selection. To choose an installed registry explicitly, add `--registry config/joint_price_news_operational_v4_20260913.json`. To copy and verify without running tests, add `--prepare-only`. A relocated package can use `--project-root` and `--output-parent` explicitly.

The runner verifies the selected registry digest, every one of its 46 source hashes, the worker's exact required inventory, and the fixture hashes. It copies those bytes into a fresh owned directory under `runs/`, rereads the source and registry to detect concurrent changes, and writes `SOURCE_KIT_INVENTORY_003.json`. It runs only the seven named test files and requires 48 completed passing cases in the pytest XML; missing, failed, errored or skipped cases do not count as a successful reproduction. Every invocation retains its own registry, inventory, preparation receipt, synthetic databases, pytest XML and result; it never reuses or deletes a previous test run.

The copied test interpreter disables network access and new child processes. Fixtures use temporary research ledgers and synthetic clocks/data; the numerical predictor is a test spy. They do not use real credentials, open actual runtime ledgers, start the bot, call a broker or place orders. Do not use test output as a forecast, trading authorization, accuracy estimate or profitability result.

The original 14 integration and scheduling tests cover:

- One full transport/source/consumer/candle preparation contract.
- Two native ledger integration cases, including issued/consumed origin-target preservation and target settlement when fresh news is blocked.
- Four asynchronous startup and bounded failure-cooldown cases.
- Seven completion-based news refresh and fair pair-fit handoff cases, including a virtual capture longer than 60 seconds and a 114-second case.

The additional 34 standalone archive regressions cover:

- Twelve packed-storage cases: exact lexical bytes and order, append stability, bounded reads and spool cleanup, missing evidence, altered slots and corrupt durable blocks, deferred-write verification and repeated-reference budgets.
- Eleven canonical-encoding cases: exact output parity, base64 padding, catalog boundaries, Unicode and float handling, pre-encoding size rejection and invalid field types.
- Eleven layout-reuse cases: complete-block reuse while checking every fresh object, partial-block growth, reordered members, altered compressed bytes, corrupt reused blocks, strict metadata types, failure invalidation and byte-boundary grouping.

`baseline_worker_stage006.py` is an immutable fixture used only for AST comparisons of unchanged scheduling and numerical guards. It is never imported or started. Its integration fixture adaptation replaces an external baseline path with this local file. The archive tests retain their original function and class ASTs; only the two external import-path assumptions were removed because the runner copies all 46 source owners beside the tests. `AUGMENTATION_20260914.json` records the original and portable fixture hashes.

`historical_receipts/` preserves the prior stage007 14-test pass and separate V4 core/V6 IO suite receipts. These are evidence for those exact historical source generations, not a pass against a later installed registry. Counts overlap between repeated suite runs and must not be added indiscriminately: the retained core gate contains 63 cases, its additional empty-observation check is a separate one-case run, and the V6 IO gate is 30 inherited cases plus 23 packed/canonical cases. `supplemental_test_sources/` retains their original source text for audit. Those text files retain original fixture paths and are excluded from the runner. The three portable `.py` archive fixtures are the separately verified 34-case addition; supplemental files are not extra runnable cases.

The original portable runner was syntax checked and its fixture bytes inventoried when installed. Augmentation validation uses an isolated copy of the exact final 46-source staging registry; it is not a run against the preceding live 45-source generation. The first execution against the installed successor is recorded only when `run_reproduction.py` is actually run after handover. A prior synthetic test pass, 12,000-projection volume test or cold-bootstrap pass does not establish that ordinary live capture meets its 30-second budget. Final stage008 separately passed the whole synthetic 12,000-story capture in 26.860 seconds and a growing-manifest capture in 24.734 seconds. Actual current collector/transport/clock health remains a separate deployment acceptance gate.

The augmented runner has now passed all 48 cases against both the owned staging copy (147.87 seconds) and the actual installed current selection copied after handover (259.23 seconds at BelowNormal process priority). Both used registry SHA-256 `28937ce3a6d213794397902c71c1b18eb1ca9d37648923e6a49b0a7c2246e078`. These wall times describe the synthetic test suites, not ordinary capture latency. All 46 canonical source hashes still matched after the installed-selection run. The permanent receipts are under `augmentation_validation/staged_copy/` and `augmentation_validation/installed_current/`; their inventories and hashes are recorded in `AUGMENTATION_20260914.json`.
