# Fixed currency-factor projection: substantive same-task review

Accepted within the frozen offline diagnostic scope. Same implementing task review;
independent_review=false and independent review pending/nonblocking. No base-model
loading/training, learned layer fitting, API/broker operation or policy replay.

The Design15.1/15.3 comparison reuses the exact preserved pure currency-state solver
and contract helper, without changing the observed-response engine or intraday ranker.
Explicit ForecastEdge records feed predicted log returns through a compatibility
interface; they are not historical PairObservation records. The old solver's numerical
incidence/connected-component/zero-sum calculation is reused, while observation and
measurement-uncertainty labels are not represented as forward empirical evidence.

The frozen comparison includes both existing learners and three predeclared variants:
unchanged direct values, full currency projection and fixed50percent pair residual.
All192original chronological frames/eight horizons retain78336coverage slots.
There are51744forecast rows, including17248exact direct values. The256nonempty
factor solves were independently recomputed within this same task using80digit
Decimal reduced-Laplacian elimination, checking17248projected pair values. Maximum
factor difference is5.51e-13log-bps against the reused numpy least-squares solver.
The remaining128solver calls explicitly have insufficient observations. Missing
base pairs are not imputed and disconnected support is not invented.

Inputs retain exact original forecast/model/fit/availability identities, with every
projection tied to its complete constituent set and fixed layer definition. Conversion
uses simple-return bps to log-return bps and back, preserving original target units.
No outcomes enter the projection. Assessment retains36954mature predictions,
14598not-yet-mature and192unavailable outcomes; unavailable is not zero.
The actual maximum frame load+projection time is below0.0631seconds within the1second
diagnostic slot. This does not prove fresh model inference/native preparation or
observed historical publication; native_policy_admitted remains false.

Both fixed variants lowered matched MAE and MSE in all16learner/horizon cells.
Each lowered daily matched MAE in51of52cells and daily MSE in52of52. All cells,
including daily exceptions, remain in the report. Horizons, dates and currency edges
are dependent and inspected development. This is promising incremental forecast
evidence, not statistical confirmation, a selected winning variant or trading profit.
Both variants must continue through the next qualification rather than choosing one
after these results. No rotation, currency exclusion or cost threshold was tuned.

19local+19relocated tests cover exact direct preservation, zero-sum/triangle/full/half
residual arithmetic, input identity/clock/target/coverage, disconnected/empty graphs,
future/null/nonfinite labels, matched supports and changed preserved solver refusal.
Actual exit91 after frame2 preserved both payloads through resume. Four actual
operator refusals cover recipe/source/parent/solver drift; bad capsule pin refuses
before destination creation. The bounded streaming capsule reproduces195scientific
outputs and all tests. Source dependencies remain exact. Same-machine reproduction
does not establish another machine's environment or cloud synchronization.

Next: currency_projection_native_policy_inputs_v2. Reuse the established fresh
native-qualification machinery and saved fitted components for both original later
cohorts. Preserve all three fixed variants/both learners and their original targets.
Reproduce the base and projection values at the decision origin with measured fresh
inference/solver/native preparation timing; do not relabel diagnostic reservations
as native publication. No new base-model training or policy replay in that package.
Only after it passes should the unchanged policy/cost/risk comparison be frozen.
The full engineering design remains unfinished; GPT/advisor comparisons stay deferred.
