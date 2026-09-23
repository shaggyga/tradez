# Retained paper episode results

`summarize_verified_episodes_v1.py` reads up to eight retained reports from the accepted offline verifier. It selects each episode's latest observation by verification clock, preserves prior step and episode identity, and counts each of the three predeclared episodes once. It does not read live state or rerun the verifier.

Only episodes with verified terminal evidence, no missing slots, and all five virtual arms flat enter the common completed denominator. Missing, unresolved, failed and future support remains explicit. With no eligible completed episode, aggregate cash and matched delta are absent (`null`), including the no-trade aggregate; an observed flat no-trade arm within an episode can separately have known zero cash.

Each arm is an isolated USD 2,500 research scenario. Per-arm episode sums do not represent an actual account, simultaneous holdings or calibrated independent trials. Interim realized cash and hypothetical liquidation marks remain separate from completed outcomes. Counts do not establish predictive or management improvement.

The summary preserves original targets, action and leg counts, selected report hashes, and exact per-episode runtime file path/SHA/byte references. Those references point to original configurations, plans, settlements, states and quote bytes/receipts for subsequent independent decomposition into midpoint movement, bid/ask spread and declared slippage. This serializer does not estimate costs from leg counts or calculate that attribution.

Run from this directory with the configured Python environment:

```text
python -B summarize_verified_episodes_v1.py --report REPORT_1.json --report REPORT_2.json --report REPORT_3.json --output NEW_SUMMARY.json
```

The output must be a new file outside the canonical project. It includes input-file byte hashes and a sealed summary hash. Source and report bindings are retained evidence, not cryptographic authentication of the report's origin. Original reports, their source references and independent reviews remain necessary to audit results.

Acceptance covers 20 focused fixtures, including exact common denominators, repeated snapshots, changed/missing prior evidence, false terminal completion, unresolved marks, exact Decimal arithmetic and retained cost-attribution references. The actual 06:51 and 07:07 reports serialize successfully with zero eligible completed episodes. This is a historical snapshot of an unfinished first episode; its fixed original terminal was 2026-09-09 07:43:50 UTC.
