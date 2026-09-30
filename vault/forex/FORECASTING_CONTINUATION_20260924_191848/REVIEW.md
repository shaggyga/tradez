# Timed forecasting continuation

Future timed runs keep selecting independent forecasting work when one candidate
blocks. The queue now retains the blocked confirmation branch and supplies four
development alternatives, with dependencies, reuse paths, completion gates, negative
outcome handling, resource-estimate fields and retry conditions. These candidates
require their own support checks before computation; none was run by this update.

Updated NEXT/TIMED/research entry instructions preserve the original deadline and
require a branch-by-branch eligibility review before an early stop. Versioned
experiment settings do not freeze development or require repeated user permission.
Operational work is limited to concrete necessary defects and required handoff.

The baseline preflight found two specific queue/ownership defects: the blocked
confirmation row was used as an accepted prerequisite, and completed same-task
policy/attribution claims remained HANDOFF. Selection now uses a unique accepted
forecast predecessor; the old claims are closed with preserved evidence. Duplicate
current policy/checkpoint rows were moved into history, and the omitted attribution
record was restored from its exact existing review. No scientific code changed.

Independent documentation/queue review: INDEPENDENT_REVIEW.json. It does not review
the prior numerical work or grant confirmation. Git 613cc9655c63ccf5bdb00e06f3cdb6e6faea6e5d; a fresh bundle clone
verified 4766 tracked files byte-for-byte. Final pointer/preflight readback is
kept in the sibling FORECASTING_CONTINUATION_20260924_191848_READBACK packet after publication. Both project logs link
here. Design/source/old evidence remains preserved. Next: `currency_projection_residual_layer_comparison_v2`.
