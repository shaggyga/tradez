# Independent compact integrity review

No concrete failure, omitted-evidence bug or compatibility regression was found in the reviewed compact publication path.

Reviewed `oanda_integrity_publication.py`, the publication boundary in `oanda_project_integrity_audit.py`, the current-integrity checkpoint closure in `forex_model_vault_sync.py`, and the saved observer locator/restore changes in `oanda_four_hour_best_improvement_pass.py`. Also checked the two other direct current-snapshot consumers: the improvement controller reads retained summary verdict fields, and the week-to-date recap reads generated time/status/failures. Neither requires removed episode rows.

The publisher modifies only a copy, removes rows from five explicit sections, preserves their other fields, stores a schema-tagged SHA-256 artifact before publishing a reference, and refuses to replace an existing mismatched content-addressed name. Empty/malformed rows remain inline for existing degraded diagnostics. The generation guard prevents stale publishers from creating artifacts or publishing an older current state.

The reader requires an exact generated relative path, strict reference metadata, exact publication membership, hash, byte count, schema and row count. It verifies the same bytes it decodes. Snapshot-relative containment rejects resolved paths outside the supplied base. Missing/corrupt content is rejected without being silently repaired.

Checkpoint collection adds only the current snapshot's verified detail closure. ZIP creation captures the snapshot plus detail bytes again, drops now-unreferenced prior closure entries, and streams those same verified bytes even when original files change afterward. It builds the manifest from actual ZIP member bytes. No uncontrolled directory traversal is added for detail collection. A checkpoint remains a current-state checkpoint, not an archive of every historical JSONL generation.

The four-hour observer preserves a fixed portable project-relative locator beside embedded compact integrity bytes. Its restore API uses an explicitly supplied project root rather than the JSONL file's directory or current snapshot bytes. Historical full snapshots remain readable. Current action selection uses retained failures/status, so it does not need to inflate row detail.

Guarded independent fixture run: **47 passed, 1 skipped in 2.80 seconds**. The skipped test is filesystem symlink creation unavailable on this Windows host. Tests include reconstruction, content reuse/history preservation, malformed metadata, missing/corrupt artifact rejection, no-overwrite behavior, publication ownership/failure ordering, checkpoint generation races, capture-versus-file mutation, restored-location compatibility and observer action checks. `run_review.py` prohibits nonfixture SQLite connections, nonfixture writes, external actions and thread starts. No worker, broker, production database or network action was performed.

Limitations: static containment review is not a hostile concurrent filesystem race or power-loss fault-injection test. Current JSON, Markdown and history are separate filesystem publications, explicitly not a multi-file transaction. Content-addressing detects mutation; it is not OS-enforced write protection against another process. Historical JSONL recovery still requires preserving its referenced artifact directory; this helper does not delete artifacts or promise that current checkpoint export includes historical logs. Existing audit computation before publication is unchanged by this optimization and was not re-audited end to end here.

## Prediction confidence-interval wording verified

The four-family lifecycle intervals are confidence sequences for **clipped(net minus economic threshold, ±60 pips)**. `oanda_hypothesis_lifecycle.py:791` subtracts each episode's economic threshold; lines 792–798 select the one-hour clip bound and pass the excess values to the confidence sequence. `oanda_edge_evidence.py:1163` clips each value before averaging. `config/prospective_replication_governance_v1.json:15` sets the 3600-second bound to 60. They are not confidence intervals on the displayed unbounded raw net mean. The existing prediction report's interval description should include this clipping distinction.

Source hashes and JUnit counts are recorded in `COMPACT_INTEGRITY_REVIEW_RECEIPT.json`.
