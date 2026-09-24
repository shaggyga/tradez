# Fixed remaining-target forecast blend and rotation comparison

This frozen development experiment adds one fixed half-weight Ridge/HGB forecast to
the existing matched two-day policy comparison. Each original saved model recomputes
its packet from the original observation before either forecast enters the blend.
The combined packet binds both identities, exact common target, reference and feature.
There is no learned endpoint-layer weight transfer, model refit, confidence invention,
policy tuning, interpolation or fallback when a paired base is unavailable.

All 68 instruments and eight original conditioning epochs remain in coverage. The
same six isolated policy ledgers, sizing, hard risk, costs, delay and full-next-minute
candle fill scenarios are reused. Cash and the unsupported legacy selector abstain.
Both accounting engines must agree, and the eight unchanged-base economic ledgers
must match their original authenticated outputs despite the new source identity.

Assumed close-plus-two-second readiness includes the fixed combination in this
scenario; it is not an observed computation/issuance receipt. Candle quotes/fills,
single rollover costs and retained metadata are research assumptions. Missing marks
remain unknown; no continuous drawdown or observed profitability claim is made.

Use `fixed_blend_policy_operator_v2.py status|run|resume|verify --recipe <recipe>
--recipe-sha256 <published-pin> --input <MATCHED_POLICY_INPUTS.json> --runs-dir <local>
--trad-root <trad>`. Identity/environment/input drift refuses before model loading.
Only engineering publication creates a successor recipe. Historical predecessor
recipes remain bound to their sealed source snapshots; this extension has its own
recipe and portable replay entry point.

There are 12 runs, one worker, no base fits or API calls, and a 900-second/1GiB memory/
4GiB output limit sampled between phases. The initial1GiB disk bound safely stopped
after five base runs; its successor uses the original eight-run1.77GB measurement
and retains the completed output identities. Scientific settings are unchanged.
Saved model inference is performed. Export
and restore use the dedicated checkpoint tool and compare all deterministic payloads.
Raw resource samples are recorded in the operator receipt separately from scientific
payloads. All methods, scenarios and policies are reported, including negative and
identical results. One inspected cohort cannot establish uncertainty or confirmation.
