# Qualified fitted-consumer recipe

Scope: actual pooled-ridge fits and immutable forecasts using synthetic observations
and outcomes for the preserved 68 instrument identities. Elapsed 24h, 2d and 5d
targets are explicit; they are not trading-session or daily-close contracts.

Reuses TrainingView, forecast_record, RunPublisher, the earlier pooled-ridge
normalization/penalized-fit algebra and the existing operator/checkpoint framework.
Training population is restricted by global origin bounds, feature availability,
outcome maturity and label-interval overlap. Learned means/scales belong to each
prefix model. No fitting on the old runner's unqualified endpoint-selected split.

The frozen control deploys the first fit. The adaptive procedure refits at fixed
cutoffs, uses newly matured observations, and retains the prior ready model during
a later fit's declared 60-second latency. Prediction latency is two seconds. Every
origin/procedure/target emits all 68 coverage rows independently of future endpoints.
Model identities bind permitted population and transform/coefficients; outputs
contain no future labels. Outcomes stay in separate input records.

Routine use: read FITTED_OPERATOR_APPROVAL.json in the current Vault package,
invoke its exact status command, then follow run/resume/verify or review_required.
FITTED_OPERATOR_RECIPE.json pins source, fixture universe and dependency versions.
No routine operator may regenerate approval or alter a failed scientific contract.

Resume verifies and reuses qualified_result.json before generating missing reports;
it does not refit a verified completed model. Completion hashes all model, coverage,
forecast, attempt and report payloads. Corruption or identity changes refuse resume.
The portable checkpoint runs the actual frozen recipe after relocation and includes
process-crash, future-perturbation and source-drift tests. Numerical replay requires
the locked dependency environment and exact payload agreement; cross-machine/BLAS
compatibility is not presumed before that check passes.

This is B/C engineering qualification, not the first historical research campaign.
Next gates: run an audited real-data slice, map actual trading-session/daily-close
targets, qualify the forecast-to-policy tape and historical execution assumptions,
then compare recovered models under the complete campaign contract. The existing
37 pending items and full 68 universe remain. GPT/advisor comparisons are deferred.
