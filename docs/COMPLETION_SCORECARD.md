# Forex design completion scorecard

**77.8% overall delivery index (35/45 milestones).**
Active scope: 83.3% (35/42); deferred GPT work stays in the overall denominator.
Fully accepted requirements: 7/15.

This fixed, equal-weight rubric credits a working implementation, verified bounded test/replay evidence, and full requirement acceptance separately. Partial work is not full acceptance. Historical tests are reused evidence, not newly run tests. This is not an estimate of hours remaining, forecast skill or trading readiness.

The assessment is a same-task review. Evidence hashes are checked by `python tools/forex_completion.py`; a missing or changed binding invalidates the score until reviewed. Never refresh hashes or award credit merely to increase the percentage. Version changes to scope/rubric explicitly. Pair availability is reported separately.

| Requirement | Points | Remaining acceptance work |
|---|---:|---|
| R01 All 68 instruments | 3/3 | Coverage identity requirement satisfied; real-time quote availability is separate. |
| R02 Daily and multiday targets | 2/3 | 16 exact targets remain unqualified; daily-close/trading-day scope must not be substituted by elapsed targets. |
| R03 Causal adaptive learning | 2/3 | Bounded historical adaptive evidence does not complete the full target portfolio or confirm live adaptive selection. |
| R04 Feature and interaction audit | 2/3 | Complete remaining conditional interaction coverage and integrate target-specific acceptance; mixed development result is retained. |
| R05 Macro distinctions | 2/3 | Full numeric expectations/stance/change/reaction support and forecasting admission remain incomplete. |
| R06 Rotation and continuation | 2/3 | Qualify retained live position-state/common-terminal-value/economic contract and integrated management; no activation. |
| R07 Matched GPT advisor arms | 0/3 | GPT/advisor comparisons remain deferred, included in overall denominator. |
| R08 Accounting and costs | 3/3 | Engineering accounting conventions/fixtures accepted within the reference contract; live management economics are R06, not trading readiness. |
| R09 Dependence and denominators | 2/3 | Whole-campaign confirmation and dependency-aware evidence across the final combined policy remain open. |
| R10 Resume and restore | 3/3 | Required recovery mechanisms accepted with exact dependencies; not a claim that missing artifacts may be refitted. |
| R11 Local operation | 3/3 | Local nontrading launcher/preflight/status/recovery accepted within scope. Management economics remain R06; future input refusals remain explicit. |
| R12 Authorization boundaries | 3/3 | Authorized research-entrypoint boundaries accepted; no demo/live order authorization is implied. Legacy unrelated entrypoints are not being certified. |
| R13 Inspectable evidence | 2/3 | Complete unified current dashboard/management inspection across all connected target families. |
| R14 Reuse and recovery | 3/3 | Reuse and preservation gate accepted; second-machine runtime qualification remains explicitly separate. |
| R15 Negative/inconclusive results | 3/3 | Negative/inconclusive evidence is retained without promotion or profitability claims. |

## Evidence and update procedure

The complete reasons, exact paths and SHA-256 identities are in `artifacts/design_completion.json`. At every checkpoint, review affected rows against new evidence, retain the 45-point denominator, rerun the checker and update this report. A new repair ticket does not add points or change the denominator. Deferred work is not silently counted as finished.

Full-design authority: vault/forex/SCHEDULER_THROUGHPUT_20261001/specification/FOREX_CODEX_ENGINEERING_DESIGN.md

## Reviewed evidence

- design: `vault/forex/SCHEDULER_THROUGHPUT_20261001/specification/FOREX_CODEX_ENGINEERING_DESIGN.md` — Full authoritative design; R01-R15 and WP0-WP11
- runtime: `vault/forex/SCHEDULER_THROUGHPUT_20261001/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- curve: `vault/forex/RETAINED_CURVE_CONNECTION_20261001/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- adaptive: `vault/forex/CHRONOLOGICAL_LAYER_COMPACT_20260924_082500/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- features: `vault/forex/RICH_SPREAD_INCREMENT_20260925_142015/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- dependence: `vault/forex/RICH_DEPENDENCE_20260922_081507/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- macro: `vault/forex/NUMERIC_JOIN_REVIEW_REPAIRS_20260924_004443/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- rotation: `vault/forex/CURRENCY_PROJECTION_POLICY_ATTRIBUTION_20260924_114246/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- accounting: `vault/forex/ACCOUNTING_REPAIRS_20260922_000256/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- accounting_acceptance: `vault/forex/REPAIR_REVIEW_20260922_001617/REVIEW_RESULT.json` — Historical scoped report read in this review; tests/results are not rerun here
- ops: `vault/forex/OPERATIONAL_READINESS_20260923_180528/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- inspector: `vault/forex/FORECAST_OUTCOME_INSPECTOR_PROBE_20260925_084200/REVIEW.json` — Historical scoped report read in this review; tests/results are not rerun here
- universe: `trad/config/joint_price_news_rolling_v1_20260930.json` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- forecast_code: `trad/oanda_retained_forecast_connection_v1.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- forecast_tests: `tests/test_retained_curve.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- adaptive_code: `stage_c_alignment_integrity_v2/chronological_layer_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- adaptive_tests: `stage_c_alignment_integrity_v2/test_chronological_layer_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- feature_code: `stage_c_alignment_integrity_v2/feature_registry_audit_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- feature_tests: `stage_c_alignment_integrity_v2/test_rich_spread_increment_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- macro_code: `stage_c_alignment_integrity_v2/macro_numeric_evidence_join_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- macro_tests: `stage_c_alignment_integrity_v2/test_macro_numeric_evidence_join_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- rotation_code: `stage_c_alignment_integrity_v2/policy_continuation_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- rotation_tests: `stage_c_alignment_integrity_v2/test_policy_continuation_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- accounting_code: `stage_c_alignment_integrity_v2/accounting_events_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- accounting_tests: `stage_c_alignment_integrity_v2/test_accounting_review_repairs_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- dependence_tests: `stage_c_alignment_integrity_v2/test_rich_dependence_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- launcher: `tools/forex_pipeline.ps1` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- ops_code: `tools/forex_preflight.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- ops_tests: `trad/test_oanda_operational_recovery_v6.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- inspector_code: `stage_c_alignment_integrity_v2/forecast_outcome_inspector_contract_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- inspector_tests: `stage_c_alignment_integrity_v2/test_forecast_outcome_inspector_contract_v2.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- reuse: `artifacts/reuse_catalog.json` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- reuse_code: `tools/forex_vault_snapshot.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- reuse_tests: `tests/test_forex_vault_snapshot.py` — Current source/config/test locator pinned; historical reports establish their stated bounded evidence only
- local_operation_acceptance: `artifacts/local_operation_acceptance_20261001.json` — R11 local nontrading launcher/preflight/status/recovery operation; live management economics and research targets are separate requirements
