# Retained M1 distribution evaluation

This external helper evaluates retained research records. It does not query a provider, fit a model, issue a forecast, alter a registered source, or write inside the runtime tree.

`evaluate_retained_risk_distributions_v1.py` loads the exact registry and its eight bound sources, then checks the original session's fixed training-artifact read and durable copy. It reads immutable cycle and pair records with bounded, stable, reparse-safe file reads. The audit is not a globally atomic snapshot: every actual audit read is recorded, and changed bytes on a repeated read invalidate that evidence.

Retained issue, publication, consumption, and complete-chain counts are different quantities. Only a complete original chain with matching durable-byte receipts, original input reads, independent publication readback, and original issue/target clocks can enter scoring. A retained issue or publication without a completed pair record remains unadmitted. Failed captures retain their original failure, raw-byte absence or presence, internal receipt seal, full receipt-file hash, and declared clocks. Source receipt completeness does not imply future path continuity.

A separate inventory enumerates every predeclared reference and pair against the actual audit cutoff. It distinguishes future references, publication deadlines not yet elapsed, wholly absent scheduled cycles, missing pair completions, retained invalid or partial evidence, and verified original chains. Nonissue collection minutes remain separate. These schedule dispositions do not turn missing forecasts into zero-valued outcomes or change the scorer denominator.

For each issued distribution and each original horizon, source selection uses the earliest actual original read whose complete returned domain covers the endpoint or entire future path, separately. Sources are never merged into a fictitious query. The unchanged outcome engine preserves the original origin row, records later revisions separately, and withholds missing bars or bid/ask fields. Derived candle records receive new evaluation-time clocks. Original source reads retain their actual historical clocks.

The exact registered 72-node distribution is replayed from its original input and fixed training quantiles. Reports separate the two fixed methods, three horizons, twelve labels, missingness and matched support. Quantile interval coverage and pinball/median errors are not directional probabilities, calibrated-confidence evidence, broker fills, or management returns. Repeated observations of an issue and repeated same-pair/origin forecasts do not increase the comparison denominator; overlapping labels and origins are not independent trials.

The orchestration bounds are 512 cycle directories, 32 sessions, 20,000 unique files, and 512 MiB of cumulative reads. The file helper retains its own per-file byte bounds. Exceeding a bound refuses the audit rather than truncating evidence. These are resource bounds, not a hard filesystem timeout.

The CLI requires a new direct-child output directory beside this helper. It writes each distribution report and a compact index using exclusive creation, flush, fsync and readback. Full receipt-file hashes and internal receipt seals have separate names in the source catalog. A failed audit writes a failure record; it never emits a passed result. Session/cycle files do not carry a unique session cross-reference, so the original training read is linked using retained session and computation clocks; that limitation remains explicit.

Example (after actual registration, substitute the exact reviewed registry hash):

```powershell
python -B evaluate_retained_risk_distributions_v1.py --registry C:/Users/zmoor/Documents/forex/trad/config/m1_risk_distributions_v1_20260909.json --expected-sha256 EXACT_REGISTRY_BYTE_HASH --output-directory C:/Users/zmoor/Documents/forex/overnight_curve_buildout_20260909/richer_inputs/prospective_risk_v1/NEW_OBSERVATION_DIRECTORY
```

Acceptance tests use temporary runtime roots, mocked network responses and synthetic training quantiles. They are engineering evidence, not actual prospective outcomes. Any actual evaluation requires a separate dated invocation and result.
