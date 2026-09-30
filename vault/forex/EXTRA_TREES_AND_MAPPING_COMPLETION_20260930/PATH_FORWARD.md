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
