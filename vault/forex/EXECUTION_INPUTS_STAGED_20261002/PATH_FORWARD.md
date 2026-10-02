# Exact quote worker integration — staged, not activated

The next execution-input dependency is now a concrete tested deployment. The running quote worker does not supply the already implemented raw PRICE observer. `trad/oanda_exact_quote_stream_v1.py` attaches the existing exact receipt publisher to the original no-order worker, preserves its float snapshot/intensity paths, drains the sidecar after stream stop and exposes session/generation/publication health in stream statistics. Original worker, stream implementation, receipt parser and quote transport bytes remain unchanged.

The wrapper checks the exact four attachment-source hashes before importing the original worker. It refuses replacing an existing raw observer or sharing the old float output path. Initialization/stop failures still close the sidecar. Prices retain provider decimal strings and provider timestamp; no decimal precision is reconstructed from floats.

33 distinct tests passed. The same33passed after isolated restore with original project/Vault file reads denied. The actual original worker entry point was exercised with fake read-only market components; a raw PRICE event reached the real sidecar and real receipt consumer without float conversion. No live broker request was made. Constructor/stop failure, existing-hook conflicts, path separation and original receipt/transport checks are covered.

The existing supervisor correctly refused an unregistered new worker. A staged recovery-contract candidate adds one explicit boolean `enable_exact_quote_receipts` switch, changing only the script registered for `practice_quote_stream_v1`. Both existing and selected new profiles passed the actual profile validator in a relocated source/config fixture; nonboolean selection and disabled-but-selected wrapper were refused. This is candidate validation, not live installation.

Failures preserved: initial duration0fixture was refused by the original parser (corrected to1); the first validation invocation passed a PowerShell7-converted UTC value (rerun wholly in Windows PowerShell); the current contract refused the unregistered wrapper (resolved in staged candidate); first portable test bundle omitted a hash-read-only source file (added exact original bytes; final33passed). No gates were weakened.

Deployment files:

- `trad/oanda_exact_quote_stream_v1.py`
- `tools/runtime_candidates/oanda_operational_recovery_contract_exact_quotes_v1.ps1`, intended target `trad/oanda_operational_recovery_contract_v6.ps1`
- `trad/config/exact_quote_stream_staged_20261002.json`, intended active profile `trad/config/operational_runtime_current_20260930.json`

Evidence: local `evidence/execution_inputs_20261002`; Vault `EXECUTION_INPUTS_STAGED_20261002`. `BASELINE.json`, `STAGED_PROFILE_VALIDATION.json`, `CAPSULE.json` and packet manifest bind exact active/candidate/source identities. `exact_quote_worker_capsule_final.zip` contains the isolated33-test reproduction; extract and run `python -I -B replay.py <original_project_root> <original_vault_root>` using the qualified Python runtime. The profile validation fixture is separately reproducible through `qualify_profile.py` against the captured source revision.

## Exact rollout after service authorization

Recheck active profile and recovery-contract hashes against the recorded baseline; reconcile subsequent edits rather than overwrite. Read the live coordination board. Identify only the managed quote worker and its exact recovery controller processes. Capture rollback bytes and retry ledgers. Stop those identities, install the staged contract/profile together, validate with Windows PowerShell, and restart the same managed role/controllers. Other workers and the other-task EUR/USD recorder must stay untouched. Preserve recovery expiry, role names and restart history; no budget reset or new broker/account actions.

Verify the original float stream still publishes, exact sidecar health progresses, and sidecar session/generation matches the worker heartbeat. Apply the original receipt consumer's30second clock, decimal, tradeability and generation checks. Missing/stale/nontradeable instruments remain refused. On failure, restore exact baseline profile/contract and restart the original quote role; preserve failed receipts/logs. Do not claim rollout from candidate tests.

Then resume `retained_management_current_state_binding_v1`: append a new observation to the existing prospective paper episode with matched current forecasts and exact quote receipts. Entry/risk/fee/slippage/financing and conditional-value inputs remain unbound. No invented economic defaults, fills or orders. Delivery remains35/45 (77.78%); management is partial.

Current AGENTS.md excludes broker/service/account actions from offline work. This checkpoint stages the complete narrow change and requests authorization for the quote-worker/controller restart; no service restart was performed.
