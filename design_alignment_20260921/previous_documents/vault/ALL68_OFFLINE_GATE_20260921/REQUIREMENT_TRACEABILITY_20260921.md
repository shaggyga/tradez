# Offline all-68 requirement traceability

| Requirement | Current evidence | Status |
|---|---|---|
| Preserve all 68 instruments | Input manifest, endpoint coverage receipts, three all-68 forecast tapes | Implemented for source ledger and endpoint-only research |
| Daily and multiday core targets | UTC daily-close, 2-day, and 5-day contract plus all-68 coverage | Implemented endpoint-only; not executable |
| Causal information clock | Raw M1-start + 60-second decision reconstruction; global-clock tests; training maturity cutoff | Implemented for Stage C targets/baselines |
| Missingness retained | Strict M1-path audit, endpoint `missing_target` records, no synthetic fills | Implemented |
| Exact price-side/cost accounting | Synthetic accounting fixtures only | Partial; no historical path-quality support |
| Adaptive causal learning | Three fixed retrains/held-forward blocks; no completion clock for recovered candidates | Partial; baseline retired |
| Feature population audit | Five Stage C technical features are explicit | Partial; broader feature registry pending |
| GPT/advisor comparison | User-deferred; candidate import quarantined | Deferred |
| Independent policy arms | Synthetic global-clock/ledger contract | Partial; no historical execution evaluation |
| Stop/resume | Pair-level atomic slices, immutable reports/tapes, Vault hashes | Implemented for calendar baseline runs |
| No unapproved execution/spending | Stage C has no broker/network calls; model registry boundaries | Implemented |
| Inspectable reports | Receipts, run status, model registry, hashes, forecast tapes | Implemented |

## Current operational conclusion

The all-68 offline framework is functioning for data contracts, target coverage, causal endpoint-only forecast diagnostics, repeatable extraction, and evidence retention. It has **not** produced an admitted model or an execution-ready policy. The only fitted calendar baseline was retired after two negative later blocks. Strict path gaps continue to block historical execution claims.

Next implementation must build a non-GPT, all-68 model candidate in an isolated output directory, then evaluate it on preregistered repeated blocks against the retained no-change and retired-ridge benchmarks. It must keep target readiness, interval-aware purge, and episode/block uncertainty explicit.
