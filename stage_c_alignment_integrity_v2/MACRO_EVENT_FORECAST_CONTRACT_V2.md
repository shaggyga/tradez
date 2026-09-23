# Fixed inspected-development event-existence forecast ablation

The prior source-as-of pipeline found no supported inherited stance phrases.
This experiment therefore tests event existence/count/age coverage separately;
it does not invent stance, numeric surprise or successful NLP. The fixed plan
was written before fitting. The September data had already been inspected in
the receipt/endpoint study and are not independent confirmation.

Reuse: original26 technical features, original clean/endpoint code and
fitted_consumer_v2.fit_model/issue with TrainingView. Three arms have26 fields
(technical),30 (technical plus fixed hour/weekday sine/cosine), and42 (plus six
retained source/event/age fields for each currency). Both the clock control and
original technical baseline remain visible. Age-missing is represented by an
explicit mask alongside a zero placeholder; it is not known age zero.

Six unique learned fits are declared: three arms times1h/24h elapsed targets.
All use the same mature prefix, row weights, ridge penalty20 and minimum100
pooled rows. Standardization is fitted on that prefix only. Training begins
September14, fit cutoff is September16, and hourly evaluation origins span
September16–17 acrossall68 pairs. No hyperparameter/seed search, adaptive
refit or winner promotion. Zero and prefix-history-mean controls share coverage.
Deterministic replays, crash recovery and leakage-control refits of the same
identities are verification, not additional selected scientific candidates.

Context uses source-as-of origin-minus30seconds. Feature/model/forecast timing
is explicitly hypothetical: bar-end features,30second fit and2second prediction
reservations. Actual historical extraction, receipt and issuance are unknown.
Forecast records are wrapped as retrospective development reconstructions and
are never published to a broker or admitted as historical live forecasts.

Issuance does not require a future endpoint; scoring joins exact available
labels separately. All32,640 coverage rows remain, including model-not-ready
and missing-feature cases. Matched errors, origin-balanced errors and per-UTC-
day errors are reported for every method. Event/pair dependence, overlapping
24hour targets and the very small number of inspected days preclude independent
confirmation or a meaningful confidence interval. Endpoint errors are not
profit, fills, daily venue sessions, continuous paths or policy performance.

The actual event layer is worse than the clock-controlled baseline on this
slice: MAE increases by approximately0.497bps at1h (2280 matched rows) and
2.530bps at24h (832 rows). This result is retained without parameter changes.
It neither establishes useful forecast improvement nor retires macro/text
families; broader text coverage and protected later evidence remain open.

## Frozen operation

Use externally pinned MACRO_EVENT_FORECAST_OPERATOR_RECIPE.json and
macro_event_forecast_operator_v2.py status|run|resume|verify, with --recipe,
--recipe-sha256, --inputs and --runs-dir. Runtime/source/consumed inputs are pinned.
The existing one-writer completion-last machinery preserves models and outputs
through process death. All ten payload hashes must reproduce on relocation.
The scoped checkpoint includes exact selected CSV inputs and retained context
artifacts; it does not duplicate whole source archives or trading state.

Passing local/relocated tests is separate from independent review. Full
engineering readiness and live admission remain false. GPT/advisor comparisons,
paid calls, broker/service/account actions and D-drive work remain deferred.
Next is the inherited long-body/English-rule coverage audit; no outcome-driven
phrase/rule tuning is authorized by this experiment's result.
