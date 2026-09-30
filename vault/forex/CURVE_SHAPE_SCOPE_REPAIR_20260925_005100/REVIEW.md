# Curve-shape scope repair checkpoint

Step: forecast_curve_shape_layer_comparison_v2  
Source: Git 3b59dd71978db58258e6436f831176ddb810273e  
Status: implementation and retained-data diagnostic complete; independent scientific review pending; **not accepted**.

The curve layer now obtains both its anchor target identity and direct comparison control
from the frozen contract. It has no operator-level hard-coded 360-minute target. It also
reports the complete declared grid: every origin, pair and base method has a ledger row;
absent complete parent curves are explicit unissued/unavailable entries.

The authenticated diagnostic reads the preserved 195-payload currency-projection parent
5c8cb84166076bcdba4724dc277c32e2520de4f548869adca3491807ef10b0d3. Its result is
evidence/CURVE_SHAPE_RESULT_V2.json, SHA-256
4a4de57da51fb76322eb75b673b5d85fc2725cf91a8856f74c4f3fc810f34baf, 19,031,172 bytes.
It contains 2,156 complete curve panels, 48 snapshots, 946 issued learned rows, 3,264
coverage cells, 1,108 explicit missing-parent-curve cells, and 18 paired strata. No base
model fit, API call, policy replay, broker/service/account or live-bot action occurred.

Focused validation passed: 18 tests across curve shape and residual layer/operator tests,
including an anchor-control paired-score case that rejects accidental use of another
horizon's direct forecast.

Same-task evidence review also passed: the ledger has 3,264 unique declared-scope cells;
issued coverage equals the 946 learned rows; all learned targets match the contract anchor;
no learned row is future-available; missing-parent cells are explicitly unissued and
unavailable; and paired diagnostics include overall, origin and UTC-day strata. This is
not an independent scientific review or acceptance.
The review-pending state is recorded separately and does not block eligible forecasting
siblings during the user-authorized timed session. Mutable current pointers remain
unchanged until acceptance.

Exact next action: retain this packet for independent review while selecting the next
supported design candidate through the Vault reuse and queue rules.
