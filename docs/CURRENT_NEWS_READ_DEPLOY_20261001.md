# Current-news read repair deployed — October 1, 2026

`live_news_input_reliability_v1` is accepted within the bounded local-operation scope. The exact configured current-news path now receives the existing four-attempt PermissionError retry (20/50/100ms waits). Every attempt still executes the original bounded/path-checked read, and all original source, expiry, availability and issue checks remain in place. Other files and other exceptions are not broadly retried. Existing scheduler/controllers were restarted under the user's keep-going authorization following the explicit restart question. Orders remain disabled and the other task's EUR/USD recorder was excluded.

## Verification and incident

106 deployment tests passed, including a new test that validates the actual deployed configuration through the scheduler startup gate. The first deployment passed105 tests but failed startup because the exact allowed-change list did not include the new configuration label. Three refused starts and their logs are preserved. Source and configuration were corrected together, staged startup validation passed, and only the affected legacy-key restart budget was released after its exact history was checked. All other role histories remain unchanged. The incomplete first observation window is retained as failed evidence, not counted as success.

The corrected source produced24 fresh heartbeat observations and5 new forecast receipts during the bounded window; later readback verified9. All68 pre-restart ledger histories, contracts and activations are unchanged. Dashboard HTTP200 and current news were verified. One stale-clock shared-news refresh was refused during recovery; the error count stayed1 during the measured window and later inputs produced forecasts. This is not a guarantee of zero future faults. No synthetic lock was injected into the live worker; transient/permanent lock behavior is supported by actual-consumer regression tests.

The latest captured dashboard has64 forecasts and4 explicit unavailable rows. Three TRY pairs are non-tradeable. USD/HKD was invalidated by the observed news-failure generation and has intermittent quote freshness; a subsequent exact raw-quote inspection found it fresh. Normal scheduling must requalify it without relaxing clocks. Neither this availability count nor the restart tests establish predictive accuracy.

## Whole input gate and measurement

The acceptance map in `artifacts/local_operation_acceptance_20261001.json` binds the prior publication repair, collector/postprocessing progress, health-read recovery, scheduler corrections, deliberate optional feature-forward pause and original official-event restoration. Current exact source, history, dashboard and readback evidence completes the local-operation gate. Historical tests are reused evidence, not newly executed tests.

R11's actual design requirement is one-click local operation with launcher/preflight/status/resume. It is now accepted separately from management economics under R06. The fixed delivery score is35/45 (77.78%), or35/42 (83.33%) excluding deferred GPT work;7/15 requirements fully accepted. This is a same-task delivery assessment, not a time estimate or trading readiness.

## Exact next work

`retained_management_current_state_binding_v1`: connect genuine current paper-state/quote/forecast evidence to the completed management contract. Reuse the original observed management state and accounting paths. Verify exact temporal/model/target overlap before reuse; old paper episodes or synthetic fixtures are not current positions. Implement explicit FLAT/WAIT and pending-state handling, retain cash/hold controls, and publish precise unsupported conditional/economic inputs rather than fabricate defaults. No broker/account actions, order activation, new fitted-model research or other-task recorder changes are authorized by this repair.

The offline prerequisite validator is complete, but live management is not qualified. The remaining research target, feature, macro, dependence and inspection requirements remain open in the scorecard.

## Checkpoint and rollback

Local evidence: `evidence/current_news_read_deploy_20261001`. Vault: `CURRENT_NEWS_READ_DEPLOY_20261001`, with publication receipt `CURRENT_NEWS_READ_DEPLOY_PUBLICATION_20261001`. The packet includes source/config bytes, actual commands/results, first-attempt incident records, exact source changes and baseline histories. Same-task review; no independent review claimed.

Rollback requires coordinated restoration of `scheduler.before`, `scheduler_config.before` and `profile.before` while the exact owned scheduler/controllers are stopped; preserve ledger files and all restart records. The existing identity-checked `deploy.ps1` captures the verified lifecycle. Never restore old ledgers or erase failed attempts. The current source and staged source now match; the candidate path is a retained deployment reference, not another active worker.
