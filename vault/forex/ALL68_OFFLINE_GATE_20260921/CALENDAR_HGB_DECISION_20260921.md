# Isolated calendar HGB decision — 2026-09-21

## Candidate

`hist_gradient_boosting` was added to the isolated all-68 calendar endpoint-only runner with fixed parameters: 80 iterations, 15 leaf nodes, minimum leaf size 40, learning rate 0.06, L2 regularization 1.0, and random state 20260921. It uses the same causal five-feature contract, train-label maturity cutoff, and pair-resumable extraction as the retired ridge baseline.

## Held-forward evidence

| Block | One-day MAE: HGB / no-change | Two-day MAE: HGB / no-change | Five-day MAE: HGB / no-change | Result |
|---|---|---|---|---|
| July | 21.68 / 20.44 | 47.23 / 46.64 | 83.60 / 89.04 | Only five-day error improved |
| August | 25.37 / 24.78 | 42.92 / 39.50 | 63.54 / 58.76 | Negative at every target |

August directional accuracy was 49%, 44%, and 48% for one, two, and five trading days respectively.

## Decision

**Retire this HGB configuration as a negative benchmark.** It may not enter policy, execution, or model-admission work. The initial five-day improvement did not repeat in the next held-forward block. Preserve its tapes and reports for later comparison; do not tune its parameters against these results.

The targets and causal runner remain valid. Any subsequent candidate must have a separate preregistered specification and fresh, isolated run IDs.

July report SHA-256: `24030509bd7a415f1d1ea53bdce50ca5ea29808d0d21fe7560c03823f72db23e`  
August report SHA-256: `df0d7f86cbf213e830531a05e6ac9c7108abeec967468e63341094914987f1e3`

No advisor/GPT, broker, network, account, or trading action was used.
