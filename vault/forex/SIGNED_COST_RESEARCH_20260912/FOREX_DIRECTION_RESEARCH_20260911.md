# Direction research — September 11 evening

The [completed direction build and results](../../direction_research_20260911/DIRECTION_RESEARCH_REPORT.md) add an immutable 68-pair price/news snapshot, 24 technical and 75 independent-peer fields, eight original news aggregates with explicit missingness, and direct up/down forecasts at 5/15/30/60 minutes.

Eight fixed feature/learner arms were evaluated on the same chronological rows across three dates. 65 integrated tests passed; all 96 saved models reproduced their retained probabilities. This is retrospective discovery using several days of news, not a profitable-model qualification. The report preserves all results and links source hashes, fit parameters, per-pair/fold results and recreation commands.

No active numerical model, broker policy, service, old ledger or sealed weekend release was changed. Remaining gaps include longer causally comparable news, structured official-release/consensus/rate inputs, probability calibration and integration with the existing price/risk curve and position manager.

Initial 15-minute combined boosting accuracy was 52.48% versus 52.35% technical-only on the same rows; its mean bid/ask endpoint net was −2.983 bps. All 32 learned arm/horizon aggregates remained negative. This does not establish a profitable replacement for the active model.

A separately logged post-hoc reversal control outperformed every learned arm's mean endpoint return on common rows and remained negative itself. The full report separates this diagnostic from the original frozen comparison.
