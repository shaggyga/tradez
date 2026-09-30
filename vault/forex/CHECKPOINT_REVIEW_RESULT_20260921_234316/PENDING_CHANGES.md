# Required checkpoint repairs

- B05-R2 (P1, open): Bind the complete local import closure, including initializer presence/absence, or load the reviewed modules through an isolated import mechanism that cannot execute unapproved parent packages. Apply the same contract to the operator and portable package before imports.
- B05-R1 (P1, open): Track cancellation request independently from activation/trigger eligibility, or explicitly allow activation of cancel-pending orders while preserving the outstanding cancellation. Only confirmed cancellation/expiry should remove eligible pending quantities under this asynchronous contract.
- B05-R3 (P2, open): Validate the frozen packaged approval against the exact packaged source/dependencies before sealing, and execute its operator preflight/run/verify in relocated acceptance. Do not silently regenerate an approval to make the check pass.

Next: accounting_review_repairs_v2: fix B05-R2 import-closure binding, B05-R1 pre-activation cancellation race and B05-R3 packaged-approval validation; add regressions, reseal the approved recipe/checkpoint and request re-review before policy integration.
