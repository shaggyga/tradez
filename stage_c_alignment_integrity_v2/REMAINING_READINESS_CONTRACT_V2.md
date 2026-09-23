# Isolated remaining-target joint readiness

Reuse the exact original8 fitted ridge/HGB pairs (16 artifacts),68 technical
observation streams, reference prices and original remaining-target forecasts.
Do not fit replacements. This is the isolated8-pair workload, not a schedule for
all models, research jobs or live services sharing the machine.

Reserve eight serial30-second fit-pair slots at the original cutoff. No original
forecast may be consumed before the whole required set is ready at cutoff+240.
The first scheduled origin is before that boundary: retain all136 pair/method
abstentions without loading weights or predicting. Later origins use their own
original fresh26-feature observations and exact direct remaining-horizon fit.
The original two-day target never moves. Never subtract realized movement from an
old forecast to invent a conditional remaining forecast.

Recompute predictions from the original weights. Require exact original identity
fields and numerical agreement at the existing curve tolerance (relative1e-12,
absolute1e-10), then retain the original canonical tape value in the new
schedule-bound forecast identity. Batch-size BLAS roundoff is not new information. Preserve
the old tape unchanged. Reuse the native curve contract and all its2-second
freshness/build limits. Prepared curves remain engineering_replay; the actual
issue_curve consumer must refuse them. Do not manufacture historical issue,
publication, execution or portfolio receipts. Modeled availability is explicit.

Measure each whole-origin weight read/hash/load, prediction, native preparation,
serialization and durable write. Record authenticated-input/startup work separately.
Two seconds is a diagnostic target;30seconds is the whole-origin hard resource cap.
Timings are preserved, including existing-payload validation on resume. A matching
completed run verifies without recomputation. Full seven-target serving remains
unqualified irrespective of this narrower experiment's timing.

Pinned operator actions: status/run/resume/verify with --recipe,
--recipe-sha256, --paths (remaining and technical), --trad-root and --runs-dir.
Main300seconds/1GiB; checkpoint1200seconds/2GiB, one worker. Recovery requires the
exact forex_matched_remaining.zip companion. It reconstructs original technical,
baseline and remaining artifacts; only this new step's suite is rerun. Scientific
payloads must match exactly; host timing bytes are separately bounded.

No base-model fit in the new operation, model selection, policy replay, live action,
independent confirmation or specific cheaper-model qualification. GPT/advisor,
paid calls, broker/service/account and D-drive work remain deferred.
