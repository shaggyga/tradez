# Macro consensus import contract

The live macro ledger accepts optional newline-delimited JSON at:

`data/oanda_training_manager/state/macro_consensus_import_v1.jsonl`

Each line represents a point-in-time expectation captured before a scheduled
release. Required fields are:

- `event_series_id`: stable series identifier, matching the structured release.
- `scheduled_utc`: original release timestamp with timezone.
- `captured_utc`: when this system obtained the expectation.
- `request_started_utc`: when the provider request began.
- `response_completed_utc`: when the full response body was locally available;
  this is the causal capture boundary and must equal `captured_utc`.
- `consensus_value`: numeric expectation in the release's declared unit.
- `source_verified`: `true` only after source identity has been checked.
- `observation_id`, `cohort_id`, `source_contract_id`, and
  `capture_contract_id`: exact identities from the currently deployed frozen
  collector; prior or foreign cohorts are rejected.
- `provider_event_id`: the provider-native stable Calendar ID. It may not be
  derived from an event name.
- `provider_event_version`: the provider-native `LastUpdate` timestamp. Missing
  update clocks are rejected rather than replaced with local capture time.
- `provider_snapshot_sha256`: lowercase SHA-256 of the archived raw response.

Recommended provenance fields are `source_timestamp_utc`, `consensus`,
`source_id`, `source_name`, and `source_url`. An exact `release_key` may be
supplied; otherwise series plus scheduled timestamp is used.

The V2 collector also records its trusted clock-contract identity, provider
event/version identifiers, exact-versus-estimated release-time state, whether
an actual value was already present, unit/reference metadata, the underlying
official release URL, and the immutable provider-payload hash. A request start
time never certifies causality: only response completion under a trusted clock
can do so.

The exact `cohort_start_utc` in the frozen configuration is the deployment
activation boundary. Captures before it remain diagnostics and cannot be
reclassified into the active cohort. Request start must be no later than both
response completion and capture.

The importer rejects the observation from causal use when it was captured at or
after release time, its source timestamp is later than local capture time, its
release clock is estimated/unreported, an actual value is already present, its
local clock is untrusted, its identity/value/timestamps are incomplete, or its
source is unverified. Rejected records are retained immutably with their
rejection reason. Consumers recompute these gates and do not trust a provider
or projection's `causal_valid` Boolean by itself.

The surprise importer binds to the current source, cohort, capture, and
observation-clock contract IDs. It verifies the lowercase payload hash against
the archived bytes and confirms that the matching raw Calendar ID, schedule,
LastUpdate, series, forecast value, exact-time flag, and absent actual are in
that payload. Missing archives or mismatches fail closed.

No narrative headline, revised post-release calendar value, or current web page
may be substituted for the original point-in-time consensus. Until a verified
source supplies this contract, the macro state remains
`consensus_source_unavailable` and numeric surprises cannot influence policy.

For releases with mapped currencies, the same ledger records immutable
executable quote samples near 0, 60, 300, and 3,600 seconds after the scheduled
time. These samples preserve reaction evidence but do not assign a directional
economic interpretation without verified series semantics.
