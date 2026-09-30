# Continuous currency-news capture — September 12, 2026

The new passive service is implemented and started. **It is currently withholding captures because the upstream news collector has no fresh clock-verification record.** Automatic approval review rejected starting that clock monitor; the block was not bypassed. This is not a claim that the entire Forex project is operational or that prediction performance has improved.

The earlier signed-cost research and its 79 MB restoration ZIP remain unchanged. This addition closes implementation gaps in continuous retention and process recovery; the outstanding clock dependency prevents fresh live evidence from accumulating.

## Implemented and verified

- Every five minutes, the worker considers only the latest closed context bucket, normally at bucket time plus 90 seconds. Missed buckets are skipped. No historical state is backfilled or relabelled as newly observed.
- The worker checks the latest successful news cycle as well as the heartbeat. A current heartbeat with `blocked_clock_integrity` in the cycle report is explicitly refused before reading articles or publishing a capture.
- Every source ID must match its own configured contract and cohort. The older check admitted IDs and lineages as separate sets; the new check prevents cross-source mismatches.
- Content-addressed storage retains exact article versions, ordering, raw envelopes, all 21 currency states, source exclusions and actual post-commit availability. Identical article versions and ordered manifests are stored once. Changed versions remain separate.
- The database is capped at 512 MiB, unique logical content at 2 GiB, with 2 GiB free disk reserve. Old evidence is not silently deleted to keep collecting. Capacity refusal remains visible in status.
- Each capture runs in an owned Windows process job with a 75-second timeout, 768 MiB memory limit and bounded output. The passive supervisor uses a separate 1 GiB job, supports the venv redirectors, and restarts failed worker sessions with bounded backoff. Three rapid failures stop the restart loop.
- OS locks prevent duplicate supervisors/workers. Attempt identity and clock highwater must be durably stored before dispatch. Restart recovery uses bounded metadata reads in the parent and verifies full retained payloads in the bounded child. Failed or stale captures cannot refresh the previous successful capture's time.
- Wall rollback and wall/monotonic discontinuities are checked through publication. The consumer preserves the original five-minute context clock and unchanged 300-second age gate.
- Offline replay reconstructs the full feature bytes and admission decisions from retained raw inputs and pinned formulas. Missing inputs, excluded inputs, observed zero scores and nonzero research scores have distinct states.

The integrated suite passed **133 tests**, including actual Windows process limits, nested job limits, duplicate locks, timeout termination, source mismatches, failed upstream cycles, recovery, capacity rollback and exact capture recreation. A full synthetic source-to-capture fixture publishes and recreates a directional currency state; it is a test, not live forecast evidence.

The lossless storage benchmark used the two real retained 5,357-row snapshots. Initial storage was about 23 MB. The unchanged second capture added **8,192 database bytes**, versus approximately 3.45 MB for another full compressed snapshot. At five-minute cadence, unchanged inputs would add roughly 2.36 MB/day in this benchmark instead of 993 MB/day. Live article revisions will grow the store faster. Peak committed memory in the storage benchmark fell from 736 MB to 546 MB; runtime limits remain enforced independently.

## Source defects and actual operating state

The source audit found 299 rows whose table source ID disagrees with the payload's configured source lineage. Cross-query Google News deduplication is intentional, but the upsert replaces payload lineage while preserving the earlier table source ID. The retained records lack the per-source observation chain needed to resolve that mismatch confidently. These 299 rows are additional exclusions beyond the 59 untrusted-clock rows in the inspected snapshot, leaving 4,999 admitted rows. The upstream upsert itself has not been changed. This is an identity/provenance gap, not evidence that the articles were invented or a blanket invalidation of every historical study.

Only four late-context terms remained active in the inspected snapshot; the original formulas assign those terms zero weight. That explains the six zero channels. Numeric payload coverage remained 14 actual values, eight previous values, and no consensus, revision or surprise values. Zero scores do not prove economic neutrality.

The original news collector and its dedicated supervisor were restarted at 03:41 UTC. The collector reports a current heartbeat but zero newly attempted sources because the independent clock attestation remains stale from 02:22 UTC. The new passive meter started at 04:06 UTC and correctly refused its first capture with `upstream_latest_cycle_not_successful`. No new production captures were published at that observation. See the separately dated operational receipt for the later cutoff.

Automatic approval review rejected the attempted clock-monitor startup with **“blocked by policy”**, supplying no further reason. No monitor was started, clock threshold relaxed, system time changed, or alternate path used to bypass the rejection. The passive service will continue checking its source; it cannot resume useful collection until that dependency is restored.

No quote stream, broker worker, trading trial, numerical model or promotion was started or changed in this addition. The prior finite practice-trial deadline and records remain intact.

## Operation and reconstruction

`start_meter.ps1` starts only this passive supervisor. It does not start upstream services. The running status is `runtime/worker_status.json`; the attempt journal is `runtime/worker_events.jsonl`. `runtime/supervisor_status.json` describes the supervisor's session, while the worker heartbeat supplies current liveness. Status is a dated observation, not forecasting success.

To stop this service, create a file named `STOP` in its `runtime` directory. The current bounded attempt may finish, then the worker and supervisor stop. Review the stop marker before manually restarting; the launcher will not remove it automatically. This process supervision survives worker crashes while the supervisor remains running. No Windows sign-in task or reboot recovery was installed for this new service.

From this directory, using the retained Python 3.12 environment:

```powershell
python -m pytest tests -q -p no:cacheprovider
python src/replay_meter_v1.py --database runtime/currency_captures.sqlite --capture-id ACTUAL_CAPTURE_ID --output NEW_REPLAY_PROOF.json
```

Replay requires an actual published capture ID; there were none at the first live observation. Do not substitute fixture IDs or an empty database. `RELEASE_MANIFEST.json` binds the code, tests, profile and pinned dependencies. The restored package contains a `runtime/STOP` marker and remains inactive during verification. It includes exact source and synthetic tests; original article databases, private source snapshots and credentials remain external. The storage benchmark links the earlier retained capture identities instead of manufacturing another history.

Primary records: `SOURCE_REUSE.json`, `STORAGE_DESIGN_RECEIPT.json`, `feed_audit/FEED_AUDIT_REPORT.md`, `feed_audit/SOURCE_ID_LINEAGE_REVIEW.json`, `feed_audit/CLOCK_DEPENDENCY_SUPPLEMENT.md`, `review/CLOCK_START_BLOCKED.json`, integrated test XML and the final completion/restoration receipt.

Still pending: restore the independent clock-monitor dependency; observe real successful captures on more than one cadence; repair upstream cross-source identity with versioned provenance; retain consensus/revision/rate-surprise inputs; evaluate new currency fields on later data before changing the learner; and add separately verified sign-in recovery if this service is to span reboots.
