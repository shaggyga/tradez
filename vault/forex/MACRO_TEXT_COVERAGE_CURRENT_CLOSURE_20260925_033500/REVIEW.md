# Macro text long-body coverage current closure

Status: **complete by existing packet reuse; no duplicate run**.

`MACRO_TEXT_COVERAGE_20260922_170622` already completed `macro_text_long_body_coverage_audit_v2`: 16 local and 16 relocated tests passed, five payloads reproduced, and the result is a semantic coverage gap rather than forecast readiness.

Key result: 127 retained texts exceed 4000 normalized characters, but full-text scanning still finds zero matching phrases. The recovered native collector fragments find action-bearing texts, but 8 of 12 native assertion challenges fail, so native semantic admission remains blocked and `rate_currency_sign` is not accepted as a forecast direction.

This closure prevents rerunning the same text audit. Next queue item: `repair_recovered_policy_action_scoping_v2`.
