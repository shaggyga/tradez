# Synthetic policy integration operating contract

This extends the existing event ledger, reviewed predecessor selectors, independent
accounting arithmetic and RunPublisher. It is a bounded two-day, two-instrument
engineering fixture, not an all-68 fitted campaign or market evidence.

The six isolated arms are fixed hold, recovered legacy selector, naive top-rank,
common-horizon continuation, persistence/grace hysteresis, and cash. The recovered
selector remains a diagnostic with its original ranking rule. The continuation
arms compare executable liquidation wealth against terminal wealth at one common
target; cash has zero future yield. Original entry costs are not charged again.
Current spreads, proportional slippage and executable conversion rates are frozen
valuation assumptions. Signed remaining financing is declared per candidate and
side. Future liquidation has one fill fee; partial observed fills each pay their
actual declared fee. These assumptions require stress and historical qualification
before campaign use.

Policies create intents only. Separate synthetic execution frames provide explicit
fill evidence after latency. A replacement first submits a close, retains partial
exposure/capacity, then waits for confirmed exit and a fresh decision before opening.
Original theses and episode links remain immutable; current forecasts are separate.
Snapshot hashes link decisions to original inputs. MFE/MAE are observed marks only.
The external cancellation/expiry frame uses the same accounting lifecycle.

Use forex_operator_v2.py status/run/resume/verify with POLICY_OPERATOR_RECIPE.json,
its published SHA-256, an isolated runs directory and the reviewed trad root. The
recipe binds all runtime sources and the existing environment. Python owns the
acceptance predicates, both engines, payload parity and corruption checks. A routine
operator must not regenerate the recipe or change inputs to bypass a refusal.

Failure handling: review_required means retain the run and escalate its structured
reason; resumable means invoke resume with the same recipe/run directory; completed
means verify and record the existing receipt. Arbitrary input JSON is an engineering
runner feature, not an operator-approved recipe. No model switching or paid calls.

Checkpoint v4 includes both frozen recipes. Export/restore executes status, run and
verify for each using the actual packaged approval, then compares relocated results.
Restore --run-tests runs policy crash/restart and refusal regressions as well.

Requirements: R06/R08/R10/R11/R14 and design sections 17–18, 21, 24–25. Tests cover
TST01,25–30,48,51–53 within this declared synthetic scope. Full engineering_ready is
false, forecast evidence is unchanged, policy evidence is synthetic_only and demo
authorization is not_granted. Missing historical execution/financing and causal
fitted forecasts remain separate campaign gates.
