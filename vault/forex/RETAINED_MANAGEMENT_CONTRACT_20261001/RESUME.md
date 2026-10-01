# Retained management prerequisite contract â€” October 1, 2026

Queue item `retained_management_contract_qualification_v1` is complete as an offline prerequisite validator. It is connected to the existing authenticated combined observation consumer. It does not create positions, choose trades or qualify the predictive value of caller-supplied estimates.

The consumer binds original entry receipts and content-addressed state/thesis/information; requires settled OPEN state, exact common terminal, genuinely newer same-family input, and the original candidate validator. HOLD, cash/EXIT and REPLACE branches share current liquidation wealth. Each branch must provide explicit future fees, slippage and financing, original forecast/input/model/quote identities, sizing, margin and loss limits. Original reference USD conversion handles executable gain/loss sides. Paid entry costs cannot be charged again. Hard-risk vetoes remain explicit. Missing, stale, mismatched, pending and unsupported states are refusals; no economic defaults are invented. The output's strongest status is `declared_contract_consistent`, not scientifically accepted. All action/activation flags remain false.

196 tests passed across the new contract, combined observations, forecast receipts, original management replay and policy continuation. Tests exercise valid synthetic controls and actual consumer refusals, request tampering and resume/output corruption. Two initial fixture setup errors are retained in local evidence. The final conditional clock check rejects availability before the reference.

The existing 2652-slot captured input was replayed through the new consumer. All slots explicitly lack the position/conditional/economic contract. This is a real support limitation, not a failed implementation or successful live management. Original snapshot clocks stay unchanged. All84 payloads match uninterrupted, resumed and isolated-restored execution. The restored process denies original project/Vault reads. The historical combined source closure differed from current files; exact saved bytes were retrieved from the verified original overlay instead of changing its hashes. No models or policy experiments were refitted/rerun.

Original evidence reused: September9 observed paper manager lost after costs and underperformed fixed holding; September25 `POLICY_EXPOSURE_REFERENCE_20260925_143200` is one dependent descriptive candle scenario (policy run9d1fb14472281fd499f0f6979df75cb994b45bf181e947b1c82411575e8d10a9), not current position evidence. Neither is promoted by these tests.

## Input reliability remains a specific open item

Readback at19:33UTC: dashboard HTTP200, current news,64 forecasts, three non-tradeable TRY pairs and stale USD/HKD. All68 original ledger histories/contracts/activations remain unchanged. A subsequent readback shows USD/HKD warming. Historical feature-forward pause and official-event hash restoration remain valid; collector progress and scheduler repairs retain their existing evidence.

The latest retained error is a PermissionError for `current_news_v2.json`. `rolling_news_io_v1.reobserve_before_issue` reads it using `read_exact`, whereas `ScheduledRunner.__init__` installs bounded read retries only for clock/heartbeat paths. The pipeline recovers, but this remaining failure is not repaired or hidden. The exact source fix is staged at `tools/runtime_candidates/oanda_joint_news_scheduler_current_read_v1.py`: only configured current_path is added to the existing bounded retry allowlist. Ten additional tests passed, including the actual staged constructor and original pre-issue consumer with transient/permanent locks and expired bytes. Candidate source/profile/rollback bytes and `deploy_after_authorization.ps1` are preserved in the packet. Current AGENTS scope excludes service actions, so this checkpoint does not restart or deploy the running worker.

Exact next: `live_news_input_reliability_v1` â€” apply the already tested exact-current-path candidate with the prepared source/profile-coordinated rollout after service actions are authorized; verify current receipts and unchanged histories. Do not redo the completed source repair. No broker requests or other-task capture changes are needed for the source repair. After that, bind genuine current state and conditional/economic support to this contract before any management qualification. The overall score remains34/45 (75.56%); this does not close a whole design requirement.

## Reproduction and handoff

Vault packet `RETAINED_MANAGEMENT_CONTRACT_20261001`, local evidence `evidence/management_contract_20261001`. `CAPSULE.json` pins the small management overlay and preserved input dependency. Verify both hashes, extract the overlay, then:

```powershell
python -I -B <overlay>/replay.py <retained_management_evidence.zip> <original-project-root> <original-vault-root>
```

The ordinary entry point is `python -I -B tools/forex_retained_management_contract.py --input <capture> --request <REQUEST.json> --sha256 <request-hash> --output <runs> --run-id management`. Use `--max-new N`, then `--resume` for interruption. The sealed historical request must run with its capsule dependencies; do not rebind old hashes to current source. Synthetic declarations are contract tests, not observed positions. Same-task review only. Rollback: revert only this checkpoint's new consumer/tests/navigation; runtime and historical source were not changed.
