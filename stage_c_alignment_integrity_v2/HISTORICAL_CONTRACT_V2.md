# Historical development slice and operator

This is an offline, retrospective forecast-only tier over the preserved all-68
July 2024 development inputs. It is not untouched confirmation. It reuses the
reviewed TrainingView, standardized ridge fitter, forecast schema and RunPublisher.
The earlier accounting, policy and synthetic-fitted frozen recipes remain intact.

The frozen JSON contract was recorded before fitting: July 1 training start;
July 22, 24 and 26 fit cutoffs; July 22–29 six-hour decision grid; elapsed 24h,
2d and 5d gross-midpoint return targets; fixed ridge penalty20, minimum100 rows.
Features are 60/240/1440-minute returns, current spread and UTC time sine.
M1 bar-start stamps are assumed available at close (plus60 seconds); actual arrival
times are not known. Model readiness is simulated cutoff+60 seconds; measured fit
duration must fit that allowance. Forecast availability is decision+2 seconds.
These assumptions do not establish historical live deliverability.

Each fit uses only matured labels and training-prefix transformations. The frozen
arm uses the first fit; adaptive uses the latest ready scheduled fit. No-change and
training-mean controls use the frozen arm's readiness and feature-availability mask.
The standardized, unpenalized intercept equals the permitted training-label mean.
All 68 pairs receive a coverage status at every target/origin/procedure, including
missing bars/features. Issuance never consults future endpoint presence.

Outcomes are joined afterwards and censored at the fixed August3 evaluation clock.
MAE/RMSE use exactly matched matured rows across all four procedures. Counts are
dependent overlapping observations with shared currencies, not independent trials;
no significance, trading gain, model promotion or full-design completion is implied.

Each fit is atomically cached with measured timing before continuing. Resume reuses
verified cached fits; crash tests exercise several actual process boundaries.
Models, forecasts, coverage, settlements and report are deterministic scientific
payloads; wall-clock duration is verified per run but excluded from cross-run byte
parity. The checkpoint replays the actual frozen recipe after relocation and compares
all 13 scientific payload hashes. Python/NumPy identity is pinned; cross-machine
numerical portability is established only if that replay comparison passes.

Use historical_operator_v2.py status/run/resume/verify with the published recipe SHA,
input-root, contract and runs-dir. The operator checks its own and runtime source,
input, config and environment identities before runtime imports. Drift or corrupted
evidence returns review_required. Routine operators must not regenerate the recipe.
The publication helper is for engineer-controlled checkpoint creation only.

Historical policy admission is explicitly blocked: these outputs do not provide
remaining-horizon forecasts to an incumbent's original target or qualified bid/ask,
conversion, financing, latency and fill evidence. No historical policy frames or
fills are manufactured. Continue that engineering dependency in a separate package;
retain this forecast evidence unchanged. GPT/advisor comparisons remain deferred.
