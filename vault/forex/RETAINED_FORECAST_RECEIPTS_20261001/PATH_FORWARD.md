# Retained forecast receipt adapter

Queue step `retained_management_forecast_receipts_v1`; Vault packet `RETAINED_FORECAST_RECEIPTS_20261001`.

The existing retained forecasts now have a tested, separately named observation receipt and management consumer. This checkpoint is an **offline frozen-capture replay**. It does not issue new forecasts, refit models, enable collectors, call a broker or activate position management.

## Completed path

`tools/forex_retained_forecast_receipts.py` loads the already verified management capture through its original loader and full population/lineage checks. Each new receipt binds the original forecast, registry and model-definition hash, feature/input/panel identities, original publication hash, producer generation clock and actual saved observation clock. The original first-observer record and the later captured-current observation remain distinct receipts. A receipt is a projection of retained evidence, not a new native forecast issuance.

The original issuer did not record its persistence-completion time. That field remains `null`; producer generation is not renamed commit completion. The consumer requires its own supplied observation clock to be at least the original observed clock, and decisions cannot precede that consumption. Ordinary integration must supply the actual read time. The replay deliberately supplies the frozen capture time and labels its scope accordingly. A later read does not renew forecast freshness.

The implementation reuses `oanda_forecast_curve_contract_v1.py` for bounded canonical identities, numeric and causal-clock validation, the original tracker for forecast identity and admission, and the existing management `quote_at` for price validation. The native synthetic-only gate and its source are unchanged. Retained observations are admitted only by the new observation consumer under their real tier; they are never relabeled as synthetic prospective curves.

New-entry observations preserve the original signed basis-point estimate and derive only its original-reference terminal price. Saved numeric precision is preserved; it is not represented as original decimal quote precision. The consumer does not subtract realized movement, emit a conditional remaining-value estimate, infer confidence or authorize an action. Optional supplied decision quotes must satisfy the existing exact-decimal, identity, freshness and tradeability contract.

Same-terminal updates require the same instrument and terminal epoch, a later reference, changed input and the supported same-family legacy26 matched contract. Different horizon forecasts are not silently compared at different terminal times. Passing these observation checks still leaves conditional-value qualification false. Later outcome state/hash is retained separately for audit and never enters the decision receipt or candidate.

## Actual evidence and verification

The preserved capture SHA is `fc1138215b6133a69af3091baa58fdc699b04d4f94b70d0516b1a59b9107e3fd`, from `RETAINED_MANAGEMENT_READINESS_20261001`. Its recorded inspection was October1 at03:23:03UTC. No fresh-market claim is made for replaying it later.

- All 2,652 connection/pair slots are present, including missing cases.
- 4,953 receipts represent 2,535 first observations and 2,418 captured-current observations. These are not 4,953 independent forecasts or experiments.
- 2,418 forecasts pass observation eligibility at that original capture time. Their pricing remains explicitly `quote_missing` in this replay; the old float cache is not converted into exact quote evidence.
- No supported same-terminal updates were present in that capture. The positive and refusal paths are exercised by labeled synthetic fixtures.
- 69 tests passed: the new receipt/consumer tests, existing management-readiness tests, and existing policy continuation/accounting suite. The first run had one incorrect test expectation (`1.1` instead of preserved `1.1000`); the expectation was corrected without changing the decimal formatter.
- All 84 output payloads match between one-pass and stopped/resumed runs. A fresh process after actual archive extraction reproduced all 84 while original workspace/Vault reads were forbidden before application imports.

Tests exercise altered and resealed target/model/input/price/clock/tier/authority evidence; trusted input mismatch; future consumption; stale rereads; expired and nonmatching targets; exact quote integration/refusal; positive same-terminal input updates and refusals; outcome separation; interrupted publication, repeat verification and tampered output rejection. Existing native gates are checked through the actual consumer.

Same-task review completed; independent review was not performed. Engineering readiness applies to the retained observation adapter. Forecast evidence is unchanged; policy evidence is unqualified; demo authorization is not granted.

## Reproduce, resume and restore

Use the locked project Python environment. The standard command reads a previously verified capture directory:

```powershell
python -I -B tools/forex_retained_forecast_receipts.py --input <original-capture-directory> --sha256 fc1138215b6133a69af3091baa58fdc699b04d4f94b70d0516b1a59b9107e3fd --output <runs-directory> --run-id receipts
```

`--max-new N` checkpoints after N newly published 32-row batches. Resume the identical identity with `--resume`. A complete matching run returns `verified_completed`. Source/input drift, missing issue bodies, corrupted partial outputs or different target identities fail without replacing evidence. The supported operator entry point is the CLI/`VerifiedCapture.load`, which verifies source and input bytes before constructing the in-memory context.

Portable restoration reuses the already published bulk capture; it does not duplicate models or refit anything:

1. Retrieve `RETAINED_MANAGEMENT_READINESS_20261001/retained_management_evidence.zip`, SHA `cd90f812b99cf5b0fb7d594f93b4eb322ea9de549d4f3f831a0e6c620fb696dc`.
2. Retrieve this packet's `forecast_receipts_overlay.zip`, SHA `9ae01c82941ed9b5db086fcf5299101f424d12b39cf8ba0fbe42c6bc434bf725` (79,473bytes,18members). Verify both hashes and extract the overlay into a new folder.
3. Run `python -I -B <overlay>/replay.py <original-evidence-zip> <original-workspace-root> <original-vault-root>`.

The runner verifies its manifest and dependency archive, retrieves only the captured input members, then installs the original-root read guard before application imports. It executes the actual CLI and checks every expected output hash. Retrieval precedes the guard; application execution uses only the restored source/input. This proves same-host relocation, not a separately tested second machine.

Local evidence is `evidence/forecast_receipts_20261001`. The live Vault owns the review packet and queue; Git includes source and a dated knowledge snapshot. Rollback predecessor is `7aef992a5a7297418573e1f22e96e668156ac5d8`; preserve the original inputs and observation stores. Running pipeline source/configuration is unchanged.

## Exact next action

`retained_management_combined_observation_v1`: integrate the completed exact-quote and retained-forecast consumers behind one bounded observation-only entry point. Bind the active quote session/generation and both immutable input identities; preserve observed availability and exact target semantics. Exercise the complete path with matched-clock fixtures and read-only retained evidence, reporting missing exact quotes explicitly. Reuse current readers and original management/accounting contracts. Do not refit, invent current quotes from floats, treat terminal-price distance as conditional value, add economic defaults or restart/enable collectors within that offline step.

Remaining work beyond this adapter: live quote sidecar rollout with owner/session/health verification, combined current-input observation, explicit position/economic state and common-terminal conditional policy qualification. The absence of supported same-terminal updates in this single saved capture does not establish that future updates are impossible. No portfolio profitability or global best-model claim is made.
