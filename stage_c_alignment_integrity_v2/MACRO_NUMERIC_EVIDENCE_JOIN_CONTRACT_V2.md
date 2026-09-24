# Numeric evidence join contract

This bounded Design 14.2–14.5 consumer joins retained numeric, component, unit and
provenance sidecars to the existing meter documents. It is offline-only and consumes
the exact meter, numeric-state, repaired-component, unit-binding and provenance
payloads named in `NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json`.

For every cutoff, a joined record requires identical event ID, source ID, selected
numeric version IDs and numeric material-cache identity. The sidecar cutoff population
must be one-to-one with the meter states. State reference membership supplies the
cutoff, cross-checked against document age and publication. Each original document
reference appears once. Duplicate rows, mismatched identities or mismatched sidecar
cutoffs are refused. Future/missing/nonfinite/boolean readiness remains explicitly
unavailable. A meter document whose selected source or
version is not the same as the numeric selection is retained with an explicit
unavailable status; it never borrows another version.

The structured document payload contains actual/prior/revised-prior fields as
retained in numeric cells, typed units, reference periods, component evidence and
provenance evidence. Four parent manifests bind the material/cache payloads; selected
versions, source, content, text cache, full original numeric event and cell hashes are
checked. Retrospective binding details and missing-scope reasons remain distinct
from historical numeric readiness. The existing currency/snapshot/concept/coverage
inspector is extended with numeric detail, empty-state handling and reset; predecessor
source and state/pair references remain unchanged. It renders detail with `textContent`.
It never interprets retained text as markup.

No cross-unit aggregation, consensus, surprise, FX direction, runtime adapter
activation, numeric feature admission, model fit or policy/trading action is allowed.
Every output retains `forecast_admission: false`, `numeric_surprise: null` and
`fx_direction: null`. A no-selected-event or qualification gap is evidence of
unavailability, not a zero or neutral observation.

The operator supports status/run/resume/verify with one writer per fixed run identity.
The checkpoint exports source, exact input bytes and expected payload hashes; restoring
to an empty unrelated folder must replay identical payloads and pass the portable tests.
Passing tests and independent review remain separate.

The retained meter population has no selected version covered by the unit/provenance
binding caches. This is an explicit real-data coverage gap, not evidence of an actual
unit/provenance attachment. Synthetic exact-content cases exercise resolved bindings.
