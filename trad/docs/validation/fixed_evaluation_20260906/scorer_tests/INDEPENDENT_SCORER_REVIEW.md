# Independent fixed forecast scorer review

The scorer was reviewed against the saved prediction assessment, the original calibration replay implementation, and the fixed EUR/USD one-hour protocol. This review used source reads and disposable fixtures only. No database was opened, runtime started, network contacted, or production data changed.

## Verified scope

The scorer preserves four exact cohort/version identities and joins only complete, valid common decisions. Original publication, feature, training-label maturity and training-label availability clocks are required. Training information must precede the original issue, not merely execution. Missing historical provenance is excluded rather than reconstructed from row order or file timestamps.

Prediction error uses the original reference midpoint and unchanged one-hour target. Each emitted forecast must explicitly match the common reference midpoint within an absolute tolerance of 1e-12. Execution uses the first eligible quote available after the latest common publication and the original target; later entry does not extend expiry. Executable bid/ask differences and explicit cost stress use the common entry midpoint for bps normalization.

Model economic and directional scores preserve the original emitted side, including abstention. Probability Brier remains independent of side. A disagreement between probability direction and emitted side is retained explicitly, and individual forecast IDs and abstention reasons remain in scored rows.

Rolling baseline labels are unique market target events, available strictly before the earliest original issue. Tests include equality boundaries, delayed target-quote availability, input reordering, and avoiding fourfold family duplication. The paired summaries compare identical decision sets. Coverage retains missing families and invalid decisions; no automatic promotion or collection exists.

## Corrections made during review

1. Missing or malformed decision clocks previously fell through to forecast validation and could raise an uncontrolled KeyError/TypeError. They now produce explicit decision and forecast exclusions.
2. Conflicting quote IDs were previously checked after quote validity. A malformed conflicting record could be discarded while another record with that identity survived. Identity conflicts now fail before validity filtering, in either input order.
3. Numeric conversion overflow now produces the same explicit invalid-value handling as other non-finite clocks.
4. At the root reviewer's request, the scorer now preserves emitted sides rather than substituting probability-derived trading directions, and binds each forecast to its emitted reference midpoint.

## Validation

The guarded offline harness passed **76 tests**, with zero failures or skips. The harness denies network actions, child processes, thread starts, non-fixture database access and writes outside its review output directory. Fixture output is under `evaluation_20260906/scorer_tests/`; the JUnit receipt is `integration_tests.xml`.

The tests cover original target preservation, missing and delayed publication, future features and training labels, stale/negative/future quotes, missing target quotes, conflicting quote identities, duplicate decision/forecast/reference identities, family coverage, fixed cohort/version checks, flat outcomes, original abstentions, probability/side disagreement, paired baseline and transaction-cost arithmetic, delayed labels and order-invariant rolling baselines. Source diff whitespace validation passed.

Source SHA-256 at handoff:

- `oanda_fixed_forecast_evaluation.py`: `7ffbb85a29ca414239d720de33a7d413fdc2447af2c58e2bd70e151abb8e0b0c`
- `test_oanda_fixed_forecast_evaluation.py`: `6453312c79c4c40d9026997446fb4b85ba698ac982e24b9dbd0741acc6f5dfc3`
- `integration_tests.xml`: `59b4e599d57f22956b7dbe3c378f6b2f49b57fe7320cb3c8fbb851a2950d295d`

## Limits

Passing fixtures establish scorer behavior, not the authenticity of historical availability claims. Historical scope chosen after inspection remains diagnostic. Overlapping rows and calendar blocks do not establish independent sample size. The scorer therefore always returns proof/account eligibility false and makes no portfolio-PnL or trading-activation claim. The separate historical comparison must retain its diagnostic label when original publication and training availability evidence are absent.
