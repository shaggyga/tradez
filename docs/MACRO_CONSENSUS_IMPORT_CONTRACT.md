# Macro consensus import contract

The live macro ledger accepts optional newline-delimited JSON at:

`data/oanda_training_manager/state/macro_consensus_import_v1.jsonl`

Each line represents a point-in-time expectation captured before a scheduled
release. Required fields are:

- `event_series_id`: stable series identifier, matching the structured release.
- `scheduled_utc`: original release timestamp with timezone.
- `captured_utc`: when this system obtained the expectation.
- `consensus_value`: numeric expectation in the release's declared unit.
- `source_verified`: `true` only after source identity has been checked.

Recommended provenance fields are `source_timestamp_utc`, `consensus`,
`source_id`, `source_name`, and `source_url`. An exact `release_key` may be
supplied; otherwise series plus scheduled timestamp is used.

The importer rejects the observation from causal use when it was captured at or
after release time, its source timestamp is later than local capture time, its
identity/value/timestamps are incomplete, or its source is unverified. Rejected
records are retained immutably with their rejection reason.

No narrative headline, revised post-release calendar value, or current web page
may be substituted for the original point-in-time consensus. Until a verified
source supplies this contract, the macro state remains
`consensus_source_unavailable` and numeric surprises cannot influence policy.

For releases with mapped currencies, the same ledger records immutable
executable quote samples near 0, 60, 300, and 3,600 seconds after the scheduled
time. These samples preserve reaction evidence but do not assign a directional
economic interpretation without verified series semantics.
