# Portable restore and rollback

Use the pinned engineering Python environment. Resolve all Vault paths relative to your own Vault root.
Checkpoint: checkpoint/forex_warm_operator.zip SHA256 2efe076154ef6803344328b6aac785adc72453154e3e5490636e00838b144a86.
Saved-input companion: OPERATIONS_CATALOG_20260922_125442/pending_work/forex_warm_curve_original_inputs.zip SHA25625061ff42b9dac4027cfcfb3042a3778b5db448e449428d8415c951de2b7676f.

`python -I -B stage_c_alignment_integrity_v2/warm_curve_checkpoint_v2.py restore --package <vault>/WARM_OPERATOR_20260924_004443/checkpoint/forex_warm_operator.zip --sha256 2efe076154ef6803344328b6aac785adc72453154e3e5490636e00838b144a86 --original-inputs <vault>/OPERATIONS_CATALOG_20260922_125442/pending_work/forex_warm_curve_original_inputs.zip --destination <new-empty-directory> --run-tests`

For an interrupted restored run, use restored source/warm_curve_operator_v2.py resume --recipe <restore>/source/WARM_CURVE_OPERATOR_RECIPE.json --recipe-sha256 d60a6f02d8c757a0e845f0851d7952d7f672b2483511d6fb62183b69c1f57327 --paths <restore>/original_inputs/PATHS.json --trad-root <restore>/trad --runs-dir <restore>/runs. Completed matching runs verify without recomputation. Do not regenerate the recipe.

Rollback inspection preserves the current work: `git worktree add --detach <new-inspection-directory> c14630874f8a75c637ecac31ca120e84e65cebd7`. Use warm_baseline pointers for history only; do not overwrite live Vault pointers. A rollback publication requires its own reviewed packet.

Local evidence source: timed_research_20260924_004443/warm_repaired -> packet evidence/; local warm_review -> packet warm_review/; run evidence -> packet run/; relocated RESTORE_RECEIPT/tests -> portable_restore/. Heavy outputs stay local and reproduce from checkpoint+companion. Source snapshot contains all engineering-root files, not only changed files.
