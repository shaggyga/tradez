# Numeric evidence join contract

This bounded Design 14.2–14.5 consumer joins retained numeric, component, unit and
provenance sidecars to the existing meter documents. It is offline-only and consumes
the exact meter, numeric-state, repaired-component, unit-binding and provenance
payloads named in `NUMERIC_EVIDENCE_JOIN_OPERATOR_RECIPE.json`.

For every cutoff, a joined record requires identical event ID, source ID, selected
numeric version IDs and numeric material-cache identity. The sidecar cutoff population
must be one-to-one. Duplicate rows, mismatched identities, future/missing readiness,
or mismatched sidecar cutoffs are refused. A meter document whose selected source or
version is not the same as the numeric selection is retained with an explicit
unavailable status; it never borrows another version.

The structured document payload contains actual/prior/revised-prior fields as
retained in numeric cells, typed units, reference periods, component evidence and
provenance evidence. The standalone drilldown renders that payload with `textContent`.
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
