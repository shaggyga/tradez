# Extra Trees comparison and live mapping completion

Completed `extra_trees_matched_development_comparison_v1`. Run `29e7d930d8ec322a649250b9483636b6d89a1c2fd2d3662dc43a239ebc43c267`; numerical source commit `56945b8`; final engineering source `981d89ae24e84ab0fa7fb6ca5eb1cf9b39440616`. Governing design remains the full engineering design, with historical coverage/addendum preserved.

The comparison used26 retained technical/cost features,68 pairs,20 six-hour origins from2024-07-22 through2024-07-26, seven elapsed horizons (15m,1h,4h,12h,24h,48h,120h) and frozen/adaptive fits.14 new Extra Trees models issued18,872 forecasts;19,040 slots retain168 missing-feature cases.15,510 forecasts had matched available outcomes;3,362 had unavailable endpoint values. No base model was refitted.

Extra Trees reduced MAE versus Ridge and HGB in12/14 target/procedure slices, versus zero and historical mean in4/14. Improvements over no-change were at24h and48h only. This is mixed inspected development evidence across five dates, with dependent pairs and overlapping horizons. No confirmed edge, policy return, live-model replacement or trading readiness is claimed. See SCIENTIFIC_RESULT.json for all scores and day-block diagnostics.

The run took27.09seconds under a600second cap; peak observed process-tree RSS456,142,848bytes, output70,152,730bytes. Saved-model replication from relocated source and inputs took12.73seconds, with original dependency roots denied, reproducing forecast/coverage/score bytes exactly. Zero replication fits. Capsule SHA256 `297353184a803ac9544f1d5cbf19e42a0eaf49e12b66b3f62441594aa337e031`. The baseline archive is reused by its original hash; already prepared inputs are shared as bytes, not regenerated.

Live repair: original native Decimal strings caused the new monitor to fail settlement. Conversion is now validated per row, without modifying original forecast records. HTTP200 and current native context/mapping verified; dated readback showed184 settled forecast proxies and302 event-pair outcomes. These are delayed M1-close proxies, not producer-original settlements, fills or independent forecasting evidence. Joint warmup remains data-limited; typed context is not yet a trained feature.

Verification actually executed:56 final source tests,14 overlapping restored-source tests,30 preflight tests; exact parent authentication, clean execution preflight, bounded actual run, restored numerical replay, completed-run reuse without changed artifacts, native/HTTP readback. Joblib emitted NumPy shape deprecation warnings; tests passed. One earlier staging test failed for an omitted config copy and was corrected. Attempt1 stopped before fitting at the lineage check; R2 resolves the reviewed successor exactly.

Review: same-task substantive review, not independent. Reviewed chronological admission, exact controls, coverage denominators, source lineage, duplicate refusal, forecast immutability, fit crash recovery, Windows child-process resource accounting, byte retrieval and relocated replay. No unresolved correctness finding remains for this scoped checkpoint; broader readiness limits remain in PENDING_CHANGES.md.

Next: `typed_currency_news_feature_cohort_qualification_v1`. Qualify the typed-news feature cohort/support contract using actual prospective interpretation availability, original model/input identities and matched price-only controls. Reuse existing joint34-field Ridge and original forecast/error studies; do not treat text counts or descriptive move mappings as incremental prediction results. Data capture remains with the other chat. GPT/advisor, paid calls, broker/account actions and D-drive work stay deferred.

# Resume and replication

This chat has stopped at a completed package, not a timed run. Next `typed_currency_news_feature_cohort_qualification_v1`; read current queue/pointers and the scoped pending list. Joint training must accumulate real support. Do not rerun Extra Trees as a new experiment.

Local completed run: `evidence/finish_research_20260930/runs/extra-trees-matched-development`.
Recipe: `evidence/finish_research_20260930/EXECUTION_RECIPE.json`, SHA256 `eb5035c01d1bd36d121c13212444396e610d794d2d90b02dad651358069a843b`.
Exact local run argv: `replay/RUN_COMMAND_R2.json`. Replace `--receipt` with a new evidence path. Add `--status` for read-only byte verification; `--resume` verifies a completed matching run without refitting; `--verify-replay` reproduces numerical outputs from saved weights. Preserve failed receipts; never delete locks to force a run.

Portable retrieval (no fit): hash-check `artifacts/extra_trees_saved.zip` against `297353184a803ac9544f1d5cbf19e42a0eaf49e12b66b3f62441594aa337e031` and `artifacts/prepared_technical_inputs_saved.zip` against `fde1cc441e46bcc4c25b4d96f08a3c13ff5d3e7db3ab40527e42387fb9eaa000` before extracting into new directories outside the Vault. The capsule's CAPSULE_MANIFEST.json lists all member hashes. Extract the input archive to INPUT. Retrieve the existing BASELINE with:

`python -I -B tools/forex_workspace.py retrieve --vault VAULT --artifact matched_campaign_saved --destination BASELINE`

Copy the capsule's `run/` directory to a new `RUNS/extra-trees-matched-development` directory, keeping all original bytes. Source, recipe and qualification are inside CAPSULE. In the original locked Python3.12.10 environment:

`python -I -B CAPSULE/source/extra_trees_operator_v1.py --input INPUT --baseline BASELINE --qualification CAPSULE/QUALIFICATION.json --runs RUNS --recipe CAPSULE/EXECUTION_RECIPE.json --recipe-sha256 eb5035c01d1bd36d121c13212444396e610d794d2d90b02dad651358069a843b --receipt NEW_RECEIPT.json --status`

Replace `--status` with `--verify-replay` for the bounded numerical replication. Optional repeated `--deny-read-root ORIGINAL_ROOT` enforces isolation; our recorded replay denied original engineering/data/run/qualification roots. Checkpoint scripts/commands document the actual test, but their machine-specific paths need remapping; do not execute the publisher script for routine retrieval. A missing artifact means retrieve it, never refit the baseline. The same-host restore is verified; another machine still verifies its own exact environment.

Four readiness dimensions: scoped engineering ready; forecast evidence mixed inspected development; policy evidence not evaluated here; demo authorization not granted. Independent review remains separate.
