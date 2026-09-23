# Signed direction and cost research — September 12 UTC

[Completed build and measured results](../../direction_decision_20260911/DIRECTION_DECISION_REPORT.md). 130 integrated tests passed. Eight model bundles reproduced 718,074 numbers exactly; 384 replay curves / 1,536 heads matched; 720 independent metric checks reconciled. Two exact currency-state captures also recreated.

Separate-day probability calibration improves scores across every tested horizon, but signed-return optimism remains the main observed failure. Of 24 learned policies, 17 lose after endpoint costs, four are positive with small or unstable support, and three select nothing. These reused-history results do not qualify a trading replacement.

Implemented: direct/conditional signed means, asymmetric expected entry/exit costs, fixed-margin wait decisions, a strict four-head research curve consumer, and source-pinned current currency-state capture with real postcommit clocks. The old meter stopped September 5; present raw snapshots contain no numeric consensus. Continuous capture, surprise/rate inputs, later directional validation and actual manager integration remain open.

[Completion and restoration index](../../direction_decision_20260911/COMPLETION_RECEIPT.json), [full numerical interpretation](../../direction_decision_20260911/review/INDEPENDENT_RESULT_INTERPRETATION.md), [news input audit](../../direction_decision_20260911/event_bridge/EVENT_BRIDGE_REPORT.md). No active trading configuration or broker state was changed; earlier running/flat claims retain their original cutoffs.
