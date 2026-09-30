# Vault queue reconciliation

This packet completes the mutable Vault reconciliation and hardens the protected confirmation validator. It changes no forecasts, models, inputs, policy replay outputs, accounting outputs, or authorization.

The earlier storage-cleanup package remains immutable history. Its pointers named the policy comparison as next because it predates the completed policy work. The reconciled queue has an explicit confirmation-protocol step record, and the validator now rejects duplicate/out-of-order origins, missing policy arms and absent frozen input identities before any compute.

`currency_projection_confirmation_protocol_v2` is blocked, not unfinished implementation: the retained inputs end at epoch `1722535260`; there is no third untouched cohort with at least eight later all-68 origins and mature labels. Replaying either inspected development cohort is prohibited. When qualified offline inputs exist, freeze their origins, maturity boundary, hashes, six variants, policy arms, and resource budget before computation.

Verification: live Git HEAD and remote `forex` branch both resolve to `dc1a7768e353e06357c5b36e37d2845c3b44acba`; the prior accepted R2 capsule remains SHA-256 `37c17453a506d59c0a4c920e24f66b804999a0d5a6e62afd4f7625e134ef1d90`; source snapshot and current-document manifests are sealed here. Same-task documentation review accepted; independent source review remains pending and nonblocking.
