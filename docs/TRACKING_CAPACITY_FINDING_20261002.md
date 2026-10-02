# Forecast tracking capacity finding — 2026-10-02

Verified at 01:29:24 UTC during the final checkpoint of the one-hour session.
The news worker reports `ValueError:tracking_capacity_no_evidence_deleted`.
`trad/data/retained_connection_20260930/tracking.sqlite` is 2,149,490,688 bytes,
above its configured 2,147,483,648-byte cap; its last write was October 1 at
07:51:07 UTC. This is a configured evidence-store limit, not a full drive.
Quotes, forecasts and current news mapping continue; prospective outcome tracking
is unavailable. Do not describe the whole pipeline as healthy.

The recorded paper observation has no first-observed anchors. Consequently it
cannot establish newly observed forecast evolution or conditional remaining-value
qualification. The paper observation and explicit economic-input interface tests
remain valid within their stated scope; full management remains partial.

Exact next repair: implement a bounded, evidence-preserving tracking store lifecycle.
Inspect and reuse the original tracking identities before selecting rollover or
compaction. Preserve all pending forecasts, historical first observations and news
links; prove duplicate avoidance, settlement across archive boundaries, restart
recovery, capacity refusal and consumer lookup with meaningful tests. Stage source
and deployment bindings together. Do not delete records, relabel late observations
as original receipts, or merely remove the cap. No service change was made for this
finding. Resolve this dependency before claiming conditional management is ready.

Local evidence: `evidence/one_hour_20261002_003802/TRACKING_CAPACITY_FINDING.json`
and `CONDITIONAL_DEPENDENCY_CHECK.json`. Publication addendum:
`PAPER_INPUTS_CONNECTED_PUBLICATION_20261002` in the live Forex Vault.
This addendum does not modify the sealed package or its historical manifest.
