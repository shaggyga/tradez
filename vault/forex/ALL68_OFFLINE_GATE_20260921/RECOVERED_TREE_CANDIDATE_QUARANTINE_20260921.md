# Recovered tree-candidate quarantine

## Candidate inspected

`C:\Users\zmoor\Documents\forex\trad\oanda_modern_tabular_gap_benchmark.py`

The candidate is a modern tabular benchmark that nominally evaluates histogram gradient boosting, extra trees, CatBoost, and NGBoost against an event-meta contract. It declares a time field, feature list, binary target, walk-forward evaluation, a purge-minute setting, and a shadow-only report field.

## Why it is not admitted to the isolated all-68 campaign

1. It imports `oanda_gpt_training_strategy_manager` directly. The current user instruction explicitly defers GPT/advisor comparisons.
2. It defaults to shared `trad\data\oanda_training_manager` datasets and writes model artifacts and latest reports into that shared tree.
3. Its target is a cost-aware event-meta binary target, not the new all-68 endpoint-only calendar return target. The population, target semantics, and target readiness need a separate mapping before any matched comparison.
4. The inspected wrapper does not itself provide a bound all-68 source manifest or prove that its shared dataset's rows obey the current raw-bar-start/decision-clock reconstruction.

## Decision

Quarantine this candidate from the current offline campaign. Do not execute it, do not import it from Stage C, and do not use its model outputs as a comparator until a non-GPT, read-only adapter binds its features, target, source set, timing, split/purge, and output location to the current campaign. This is a compatibility decision, not a claim that the historical code is defective.

The next model candidate must have no GPT/advisor dependency and write only into an isolated Stage C run directory.
