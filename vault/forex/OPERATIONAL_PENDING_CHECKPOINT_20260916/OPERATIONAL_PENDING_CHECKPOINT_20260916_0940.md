# Forex operational pending checkpoint — 2026-09-16 09:40 EDT

Scope: no-orders / research-only operational cleanup in C:\Users\zmoor\Documents\forex\trad. No trading authorization was changed and no order path was enabled.

## Current live verdict

- V18 supervisor is running with profile hash 820e3853c4b73d45b0f3c17eefccecca889fffbb8990dff00d95f571ebeb8288.
- Watchdog is degraded only because joint_price_news_study_v9 reports 
ews_bootstrap:CaptureReadTimeout:packed_capture_retries_exhausted.
- All-68 derived technical feed is operational: 68 registered pairs, 68 current pairs, 68 minimal-feature current pairs, 68 movement-feature current pairs, source errors 0.
- Primary feature-forward worker is operational: status observing, errors empty. Partial broker quote refusals are recorded as nonfatal diagnostics when other usable quotes exist.
- Cached M5 feature-forward worker is operational but not decision-ready yet: status waiting_for_fresh_history, errors empty, latest retained reason waiting_fresher_endpoint_for_processing_headroom.
- Joint price/news remains isolated/degraded: 0 pairs with forecast, 68 unavailable, shared history not prepared.

## Changes completed in this checkpoint

1. Repaired all-68 derived technical publisher in the previous V18 cutover so minimal/movement features remain readable across all 68 pairs.
2. Repaired forward-feature worker status semantics: quote refusals no longer make the whole worker fatal when other usable quotes exist.
3. Updated the forward-worker source binding and V18 profile hashes, then restarted V18 in no-orders mode.
4. Retired old source-generation forward ledgers and created fresh ledgers at the configured paths. Old directories are preserved under etired_source_generation_* names.
5. Rotated the large joint-news capture_archive after io_capture_duration_bound; the old archive is preserved under capture_archive.retired_io_capture_bound_20260916_092804.
6. Wrote a handoff prompt: docs/validation/operational_pending_20260916/FINISH_PENDING_PROMPT_20260916_093544.md.

## Joint-news finding

The news collector/transport itself is current and has successful publish/readback cycles. The failing component is the joint study's private news bootstrap/capture path. It alternated between:

- shared_news:ValueError:io_capture_duration_bound
- 
ews_bootstrap:ValueError:stream_validation_time_bound
- 
ews_bootstrap:CaptureReadTimeout:packed_capture_retries_exhausted

The joint validation cache showed real progress at one point (alidated_observations=599, captured_observations=599), so this is not a missing-data problem. It is a bounded validation/capture budget problem in the joint-news I/O path.

A quick patch to give the joint study's private IncrementalReader 30 seconds instead of the default 20 seconds was tested conceptually and then reverted before deployment. The reason: evision_news_io_v12.py is source-pinned by evision_joint_point_v4.py and by every joint registry contract. Changing it requires a deliberately activated successor joint-news cohort, not a silent one-file patch against the existing activated ledgers.

## Pending changes

1. Build a successor joint-news cohort if this model path is still worth keeping:
   - create a new source-bound revision of the joint news I/O path with the wider validation budget or a leaner current context assembly path;
   - update all dependent source pins intentionally;
   - prepare a new registry with new contract hashes;
   - activate a fresh absent study root;
   - update the supervisor profile and prove it with tests/readback.
2. Decide whether the joint-news model should remain in the live research profile while it has produced no usable forecasts, or be replaced by the all-68 technical/feature-forward path until a successor cohort is ready.
3. Continue monitoring cached M5 forward readiness; it is running but still awaiting fresher endpoint headroom at this checkpoint.
4. Keep prediction/profitability claims separate from feed health. None of this proves a profitable model.

## Reproducibility notes

- Old forward ledgers and the old news archive were preserved, not deleted.
- The temporary source experiment was reverted; evision_news_io_v12.py is back to SHA256 7fd784ef6ca756201c1ccddd36961e608d669a2a7e700abf4561a0a0122e51c.
- config/joint_price_news_operational_v6_20260916.json is back to SHA256 36e65075b1337402f48101025a8016c7156df76922ecc853d6f22f2a130abae8.
