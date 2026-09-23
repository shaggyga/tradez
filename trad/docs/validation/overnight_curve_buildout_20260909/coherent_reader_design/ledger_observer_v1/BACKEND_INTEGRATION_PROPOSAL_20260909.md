# Minimal backend integration proposal — not implemented

Add a separately named ledger-observation endpoint and API field. Do not replace or forge the frozen producer's summary/heartbeat or feed the observer into the existing producer-health projector. This can expose the 66 forecasts verified at 08:13 UTC while continuing to report an original producer envelope mismatch when one occurs.

## Existing integration points

This review used `trad/oanda_practice_live_dashboard.py`, SHA `cc0f6698a30c59cb291dada08d73283f0484996e6873b8ce27894e0168614f6f` (523,700 bytes), and the current joint-primary selector, SHA `72aeb3dc94eb50023a4ed8b2651350751625032f897f2a6d6af1a8f5368758df`.

* `summarize_joint_price_news_forecasts` at line 774 selects v3 and reads the producer envelope through `_summarize_registered_family_forecasts` (line 827).
* `project_joint_collection_status` at line 1259 requires producer summary, worker and consumer clocks to match its registered-envelope semantics. A ledger-only observer cannot truthfully satisfy those requirements by manufacturing a worker observation.
* `build_main_state` at lines 7086–7091 currently makes that transport-dependent projection the primary collection status.
* `Handler.do_GET` at line 9320 has `/api/main` and `/api/state`; there is currently no `/api/status` handler.

## Smallest useful implementation

1. Add one new, source-bound module, tentatively `oanda_joint_v3_ledger_observer_api_v1.py`. Its configuration pins the already accepted observer/helper hashes, original registry raw-byte SHA, activation receipt SHA and the fixed study path. It also binds its own source. Do not obtain expected hashes by accepting whatever source bytes happen to be present at request time. Keep the existing selected-v3 pointer check, and reject missing, changed or incompatible selection explicitly.
2. Add `GET /api/joint-v3-ledger-observation` to the existing HTTP handler. Return an explicitly new API envelope around the observer's original sealed report. A compact `joint_v3_ledger_observation` section can also be included in `/api/main`, using the same cache and a link to the dedicated endpoint. Leave `joint_price_news_forecasts`, `joint_collection_status` and their producer transport diagnostics unchanged in this first integration. No HTML change is needed.
3. Keep one immutable canonical report in a process-local, single-flight cache, with a proposed 15-second refresh interval and no background worker. Revalidate pinned source/selection identities before any cached response. Decode a fresh object for each caller; never expose shared mutable row dictionaries. Retain the report's original ledger read and verification clocks. At response time sample a separate actual consumer clock, require an observation age of 0–90 seconds, and recheck each original target against that clock. Cache refresh errors, source changes or future clocks return a named unavailable result; do not present the failed refresh as a successful current observation.
4. Bound concurrent requests to one scan. A waiting request may use an already source-verified, unexpired cached observation with its original age; otherwise return `observation_in_progress` rather than launch another 68-ledger scan. The accepted reader's eight-second ledger budget and explicit partial/unvisited rows remain intact. Mandatory source checks can add time. The actual full pass was 0.985 seconds; that is evidence for feasibility, not a latency SLA.

Suggested outer API shape (field names are a proposal, not an accepted schema):

```json
{
  "schema_version": "joint_v3_ledger_observer_api_v1_20260909",
  "status": "current_ledger_observation",
  "proof_basis": "independent_readonly_original_committed_ledgers",
  "observer_report_sha256": "<canonical original observer report hash>",
  "observer_report": "<unchanged decoded sealed observer report>",
  "consumer": {
    "observed_epoch": "<actual response observation time>",
    "observation_age_sec": "<age of original observer completion>",
    "verified_ledger_pairs": 68,
    "current_forecast_pairs": "<rechecked at response time>",
    "active_forecast_ids": "<only original IDs whose original targets remain ahead>"
  },
  "current_inputs_observed": false,
  "old_heartbeat_validated_for_forecasts": false,
  "research_only": true,
  "can_place_orders": false,
  "can_promote": false,
  "can_authorize": false,
  "account_eligible": false,
  "proof_eligible": false
}
```

The JSON above is a shape illustration; placeholders and counts are not implementation data. An observation can be partial even while some independently verified forecasts remain available. Original `observer_report.summary.rows` and forecast IDs, cohort IDs, reference/issue/target, news evidence and publication/consumption clocks remain byte-derived from the accepted report. Do not edit that report's counts when a target expires in the cache: put the updated count and eligible ID set only in the separate consumer projection. Do not set `running: true`, declare current input readiness, or infer an accurate/profitable forecast from ledger availability.

Original producer diagnostics remain inside the unchanged report with their own raw-read clocks and hashes. Thus a new response can truthfully say “66 current original forecasts verified from ledgers” and “original producer envelope generation mismatch.” The observer never makes the latter envelope valid.

## Required narrow acceptance checks before routing

* A genuine producer hash mismatch plus a valid original ledger publication exposes the original forecast through the new namespace and retains the mismatch.
* Tampered source/registry/activation/selection, changed original receipt or future original publication fails closed; the previous cache cannot hide a current integrity failure.
* Response-time target expiry removes only that ID from the consumer eligibility set; original sealed report and target clocks remain unchanged. Stale/future cached observations are unavailable.
* Concurrent requests create one scan, return immutable independent objects, and retain original read clocks. A timed-out scan reports all missing/unvisited rows rather than silently reducing the denominator.
* Existing producer-envelope APIs keep their prior semantics, including their mismatch failures; no object with the observer schema is accepted as a producer summary, worker heartbeat or trading-readiness proof.
* A bounded actual response check confirms the new endpoint's source/spec hashes and original forecast IDs against the accepted independent reader. No worker restart, model fitting, original study write or order capability is needed. A dashboard-server reload would be a separate parent-controlled operational step after review.

The frozen worker's mutation bug would remain present. This proposal repairs the availability of independently verified backend observations while preserving that fact. A later producer successor still needs its own source closure, registration and immutable publication boundary.
