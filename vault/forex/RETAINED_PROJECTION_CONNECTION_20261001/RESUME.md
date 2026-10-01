# Current retained currency projection connection

Checkpoint: `RETAINED_PROJECTION_CONNECTION_20261001`. Step `retained_currency_projection_connection_v1`.

The original saved 6h and 18h Ridge/HGB parents now also feed their preserved fixed currency projection and fixed half-residual variants. The original fifteen connections are unchanged. Eight derived connections bring the total to 23 across the same 11 elapsed horizons. This is existing-model integration, with zero fits. The currency ranker and observed-response engine are unchanged. A fixed half-residual is not the learned residual model.

The adapter imports the original projection consumer in an isolated source namespace and calls the original authenticated currency solver. Unit weights, zero-sum currency incidence, largest supported component, minimum 6 observations/4 currencies, simple/log conversions and fixed residual factors are preserved. All 68 pairs receive a coverage record for each connection. Parents must have exact saved model/target/input identities and share the newest available origin for their horizon. Older origins are withheld, never pooled into another minute. A malformed parent refuses that horizon's derived layer without replacing direct forecasts. Actual completed clocks govern live publication; historical modeled availability is explicitly separate. Cross-pair input identities affect tracking keys; repeating an unchanged issue does not count again.

## Newly executed verification

- 104 Python tests passed across projection, original projection, saved consumers, legacy inputs, tracking and artifact capsule. Initial 21 fixture-compatibility failures were corrected; raw failed output remains in the packet.
- 5 JavaScript scenarios passed through the actual page functions: layer input labels, expired output refusal, request success, timeout and HTTP failure.
- Two authenticated historical6h/18h frames reproduce the entire original projection result structures.528 derived outputs match exactly through the new adapter. This is replication, not a new forecasting experiment.
- Current authenticated inputs produced 1456 outputs (62 pairs per layer,64 per direct connection);16 layer slots correctly refused older-minute parents. There are 1564 explicit coverage slots.
- Canonical capsule contains 24 payloads. A fresh relocated process reproduced all 1456 forecast values exactly with an audit hook refusing reads from original project/Vault roots. Input feature values were captured through the qualified reader; raw bar/receipt reconstruction remains the prior accepted base-connection evidence. The first fresh import found two missing transitive source bindings; both are now included.
- Native HTTP readback returned 1,472 current outputs,64 pairs, 23 tracking groups, current news; served page includes the layer display. Another preserved readback records an expired-input window. These are time-specific observations, not continuous availability or visual-browser QA.

## Review and limits

Same-task review: accepted within this engineering scope; independent review not performed. Review covered actual diff, original mathematical invariants and parent identities, coverage/clock/refusal paths, graph support, cross-pair deduplication, restored execution, no-order flags and native consumer integration. No new outcome score, universal best-model claim, trading readiness or order authorization follows. Existing negative/inconclusive studies remain preserved. Learned residual integration, full same-base curve inputs,16 unqualified exact short/calendar targets, news incremental value and position management remain separate.

## Replica / recovery

Use the source commit recorded in the Vault packet's REVIEW.json. Restore original inference bytes with:

```powershell
python tools/forex_retained_capsule.py restore --root . --capsule <Vault>/RETAINED_PROJECTION_CONNECTION_20261001/projection_connections_capsule_r2.zip --sha256 ec85cfbe2813139282fefa998a4bcc01893e84e3ddf0c10991daac660e8db4d5
```

Use the qualified environment in requirements-engineering.lock.txt and START_HERE.md. This restores bytes only. Do not start live collectors on another machine without reconciling ownership and local setup. The packet includes recorded input/output fixtures, the fresh-process replay runner and source identities for offline replication. Current mutable pointers are in the live shared Vault; Git vault/forex is a dated knowledge snapshot.

Rollback requires the prior Git source/config checkpoint 3037c5a04b4ce6bb3f0b8adeb0c38ac5dec703c2 and its matching 15-connection capsule. Do not mix old registry bytes with new source pins. Preserve historical issued/tracking databases. Existing bounded recovery expiry remains 2026-10-07T08:14:50Z.

## Exact next action

`retained_input_freshness_continuity_v1`: trace the recorded expired-input window against technical capture, completed-minute publication, context inference and HTTP observation clocks. Measure a bounded sequence before changing anything; reproduce the delay path. Repair scheduling/read latency only where demonstrated, preserving original numerical inputs, availability clocks and freshness gates. Retain refusals and verify multiple publication transitions rather than weakening the 180-second condition. No model fitting, new experiment, other-chat EURUSD edits or broker actions.

After freshness repair, qualify reuse of saved learned-residual states against these exact parents. Connect only if a preserved compatible causal state can be retrieved and verified without fitting. Do not treat missing state as permission to refit. Position-management connection remains a separate nontrading integration scope.
