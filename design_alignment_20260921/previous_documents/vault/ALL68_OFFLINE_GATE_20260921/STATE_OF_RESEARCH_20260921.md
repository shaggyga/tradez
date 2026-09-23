# State of research — all-68 offline campaign

## What is working

The offline Stage C campaign has a bound 68-pair long-history archive, canonical pip metadata, a raw-M1-start plus 60-second decision clock, causal endpoint-only targets, all-pair coverage audits, pair-resumable extraction, immutable forecast tapes, and machine-readable status/registry/guard artifacts.

UTC calendar endpoint coverage is sufficient for forecast-skill diagnostics in the fixed July sample: 7,337 next-close, 7,297 two-trading-day, and 7,213 five-trading-day endpoint labels were available.

## What the evidence says

The exact calendar-minute audit found only 154 next-close paths with every calendar minute present. It found zero such two- or five-trading-day paths among endpoint-available cases. Missing calendar minutes may include expected session closures; they are not yet classified. Accordingly, all current calendar results are endpoint-only midpoint forecast diagnostics.

Two fitted candidates were evaluated on later held-forward blocks:

| Candidate | Evidence outcome | Current status |
|---|---|---|
| Pooled ridge | Favorable July results failed in negative August and September blocks | Retired negative benchmark |
| Histogram-gradient boosting | July five-day improvement failed in negative August block | Retired negative benchmark |

The admission guard therefore denies all evaluated models. No model is admitted, no policy is eligible, and no execution claim exists.

## Boundaries in force

- GPT/advisor comparisons are deferred by user instruction.
- No network, broker, account, order, or trading action is used by Stage C.
- `test_stage_c_offline_isolation.py` enforces that Stage C Python and
  PowerShell sources do not invoke HTTP, socket, broker-replay, or GPT/advisor
  dependencies.
- Recovered model wrappers with GPT-manager imports or shared artifact writes are quarantined.
- Strict-path labels must remain distinct from endpoint-only labels.

## Next safe implementation

Build a newly specified non-GPT all-68 model candidate in a fresh isolated run directory. Before fitting, record its target, feature formulas, source availability, train-label maturity, model-readiness, split/purge rule, and planned evaluation blocks. Compare it to no-change and both retained negative benchmarks without parameter tuning against their results. Add episode/block uncertainty before any model-admission review.

## Entry points

- `RUN_STATUS.json`: current completed/incomplete inventory and hashes.
- `MODEL_REGISTRY_20260921.json`: each candidate's authority status.
- `REQUIREMENT_TRACEABILITY_20260921.md`: requirement-level evidence.
- `REPRODUCTION_RUN_MATRIX_20260921.md`: exact retained-run settings.
- `FEATURE_REGISTRY_STAGE_C_20260921.json`: formulas, inputs, warmups, and
  limitations for the five current baseline features.
- `MODEL_ADMISSION_BOUNDARY.md`: fail-closed admission rule.
- `ALL68_RUN_PREFLIGHT.json`: passed start-up check for the next isolated
  offline experiment.
- `ALL68_STAGE_MANIFEST.json`: hashes for 28 Stage C sources and 26 primary
  published outputs.
