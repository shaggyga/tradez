# Fuzzy rule selection and actual-maturity repair — September 12, 2026

The canonical miner now selects, ranks and calibrates rules using training and selection periods only. Final-period outcomes are reported after the rule set is frozen. The old miner could change its selected rules when only final outcomes changed; those older results remain preserved and are not reclassified as untouched evaluations.

The new `fuzzy_selection_frozen_actual_maturity_v4` contract requires aware origin and actual outcome clocks, finite labels and strictly matured labels before each subsequent origin block. Equal-boundary labels are purged. Missing clocks, shared selection/final blocks and empty partitions are refused; there is no row-index or nominal-maturity fallback. Identical origin clocks stay in one block.

Published rounded thresholds and widths are used during mining, so saved conditions reproduce selection membership and calibration. Nonfinite live feature values cannot saturate into confident predictions. A v4 state must declare schema and contract at both state and rule levels. The new default artifact is `signal_combination_audit_v4.json`; writing over a legacy state or mixing legacy rules is refused. Legacy artifacts retain their explicitly identified compatibility behavior.

V4 is research-only. Repeated fits after elapsed time do not establish independent forward replication, and injected account-eligibility flags are cleared. Selection support counts, concentration diagnostics and lower-edge formulas are descriptive selection heuristics, not proof of independent samples or tradable accuracy. Legacy inference field names beginning `holdout_` are retained where needed but explicitly identify their selection-calibration basis.

Validation: 54 tests and 31 subtests passed against the real candidate modules and real project feature-domain/exit-fit dependencies. These include the actual SQLite → reader → miner → writer → refit path, final-label and unseen-feature mutation invariance, exact publication parity, delayed/equal maturity, invalid inputs, legacy compatibility and artifact-mixing refusal. One first regression run differed only because its two fits used different wall-clock `as_of` values; the fixture now binds the same explicit cutoff. Original tests and failed receipts are preserved.

No historical database was rescored and no service, account or trading configuration was activated. This fixes evaluation correctness; it does not establish a profitable model. The pre-existing general reader refresh/freshness behavior remains a separately identified follow-up.

Evidence: [integration workpack](../../revamp_8h_20260912/models/fuzzy_contract_review/README.md), [canonical before/after receipt](../../revamp_8h_20260912/models/fuzzy_contract_review/CANONICAL_INTEGRATION_001.json), [project tests](../../revamp_8h_20260912/models/fuzzy_contract_review/PROJECT_TESTS_003.xml). Canonical replay is recorded separately in that workpack.
