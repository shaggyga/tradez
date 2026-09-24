# Chronological forecast and policy attribution

Use the exact operator recipe and contract. Reuse both completed policy cohorts;
zero model loads/fits/layer fits, zero policy replay.56reference paths retain336
accounts,15232coverage slots and11048native forecasts. Match exact original targets,
native forecast IDs and matured outcomes. Order-bound fills include delayed retries.
Selected and actually filled forecasts have separate error metrics. Retrospective
endpoint candidate ranks are not alternative trade P&L. Costs reconcile at account
level; participation counts are not time-weighted exposure. Overlapping inspected
development evidence is not confirmation or authorization for live operation.

Operator: chronological_attribution_operator_v2.py run/resume/verify --recipe <file>
--recipe-sha256 <pinned SHA> --paths <local PATHS.json> --runs-dir <local directory>.
Use resume after interruption; original verified payloads must remain identical.
Checkpoint: chronological_attribution_checkpoint_v2.py export --package <new.zip>
--paths <PATHS.json> --runs-dir <runs>; restore --package <zip> --sha256 <pin>
--destination <new-empty-directory> --run-tests. Streaming archive operations avoid
holding the complete1.146GB input inventory in memory. Use the exact source revision
and pinned environment. Scientific outputs must reproduce exactly; timing is separate.

Read the Vault latest pointers and review queue for status and the next item.
Independent review is separate and pending/nonblocking. GPT/advisor stays deferred.
