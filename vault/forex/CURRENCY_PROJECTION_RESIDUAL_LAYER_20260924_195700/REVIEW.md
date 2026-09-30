# Currency projection residual layer

`currency_projection_residual_layer_comparison_v2` is complete and accepted within
its offline development scope. Git source commit
`c20ab71b712954d0f730d73d62f94c9dcaaa765c` contains the reusable layer, operator,
and focused tests.

The immutable source packet contributed 192 frames, 51,744 preserved controls, and
81,600 retained outcome rows. The operator produced 9,182 learned forecasts with
exact matched direct, full-projection, and half-residual controls. It retained the
absence of 2,520- and 2,880-minute learned outputs because the declared four-origin
training minimum was not met before maturity.

`assessment.json` has SHA-256
`12fd79b5cdc96905b02454d3688f82a55c163588fe4a2aca0f9d7d271f61294d`.
Focused projection plus residual-layer tests: 26 passed. The package has no base
model refits, policy replays, API calls, native issuance, or live actions.

This is dependent retrospective development evidence only. It does not support a
confirmation, policy, profitability, or trading conclusion. Same-task review passed;
independent review is still unperformed.

The exact next independent item is `forecast_curve_shape_layer_comparison_v2`.
