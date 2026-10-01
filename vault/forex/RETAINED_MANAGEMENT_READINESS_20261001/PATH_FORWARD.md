# Retained position-management readiness

Checkpoint `RETAINED_MANAGEMENT_READINESS_20261001`; queue step `retained_position_management_readiness_v1`. Capture completed at **2026-10-01T03:23:03.638347+00:00**. This report describes that captured state, not a continuing monitor.

## Completed consumer path

`tools/forex_retained_management_readiness.py` captures and authenticates current retained publications, their immutable issuance bodies and first-observed tracking anchors, then produces a complete management-readiness matrix through the existing resumable publication engine. It reads the running databases in read-only mode and writes only its separate evidence directory. It neither recomputes forecasts nor changes any running service.

The final capture includes 39 connections and all 68 pairs: 2,652 explicit slots. It contains 2,418 fresh forecasts and 2,535 first-observed anchors. Their existing tracking states were 2,411 pending and 124 settled; 117 slots had no observed anchor. These are copied forecast outcome states, not positions, fills or new performance scores. Six original issuance bodies were authenticated. The first anchor is selected deterministically by earliest actual observation and ID, never by result.

All 2,418 currently present forecasts target a later time than their same-connection first anchor. No fresh same-family cross-horizon forecast shared an anchor's exact terminal target in this capture. The tool checks that latter path explicitly: a later 6h forecast can potentially share an earlier 12h forecast's terminal time when their clocks align. This sample does not prove such updates are permanently impossible. Source family, original input identity and target comparability still matter.

## Concrete remaining input gaps

1. The existing management quote validator rejects all 68 captured cache entries for missing quote identity. Entries also contain float bid/ask values and lack the manager's explicit availability clock. The tool preserves these fields as observed; it does not stringify floats and call them original decimal prices or backdate availability.
2. Retained issuances are valid under their own schema but do not carry the native curve publication/consumption chain required by the old management adapter. Calling its existing policy gate with the real retained tier correctly yields `native_synthetic_qualification_tier_required`. No synthetic-tier relabeling or new forecast receipt is manufactured.
3. No account/position state or complete sizing, cost, financing and conversion contract was requested or supplied. Management eligibility remains zero. Valid forecasts and observed outcome tracking do not themselves authorize economic actions.

The existing entry/continuation separation and accounting engine remain reusable. September9's original observed management study was negative for active curve management versus fixed holding; its result is unchanged. The preserved September25 policy-exposure reference remains one retrospective candle scenario. Neither is newly rerun or promoted here.

## Verification and review

- 48 tests passed under the final source: the new actual-consumer lineage/population/quote/refusal checks plus the existing policy continuation/accounting suite. The latter tests use their original synthetic fixtures and verify cash/hold controls, no double costs, causal decisions, restart and settlement; they do not establish current policy profitability.
- Malformed populations were resealed as producer outputs before testing, so coverage checks were exercised beyond simple hash mismatch. Altered origins, model identities, first-observer columns, missing issuance, premature/outcome states, decompression limits and tampered resume were refused.
- The full captured population completed once, then through a stopped/resumed run. All 43 payloads (42 batches plus report) matched byte-for-byte.
- A 28-member archive was extracted into a fresh directory. A separate Python process reproduced all 43 payloads exactly while an audit hook installed before application imports refused reads from the original workspace and Vault.
- Same-task review only; independent review was not performed. The initial publisher double-release defect was fixed. An early same-connection-only census was expanded to retain supported cross-horizon alternatives. Initial failure/output evidence is preserved locally.

Engineering readiness applies to this evidence bridge and qualification gate. Forecast evidence is unchanged; policy evidence remains not qualified; demo authorization is not granted. No models were fitted, no broker/account calls were made, no positions were managed and no live worker was changed.

## Run and restore

For a deliberately new local observation, use a new directory:

```powershell
python -I -B tools/forex_retained_management_readiness.py capture --output evidence/management_capture_NEW
python -I -B tools/forex_retained_management_readiness.py replay --input evidence/management_capture_NEW --sha256 <capture-returned-manifest-sha256> --output evidence/management_replays_NEW --run-id readiness
```

For interrupted replay, repeat the exact command with `--resume`. `--max-new N` limits newly written 64-row batches for a controlled checkpoint. A completed identical run is verified rather than rerun. Source/input drift or altered partial output is refused. Replays always use the captured clock and do not claim those inputs are fresh now.

Portable evidence: live Vault `RETAINED_MANAGEMENT_READINESS_20261001/retained_management_evidence.zip`, SHA-256 `cd90f812b99cf5b0fb7d594f93b4eb322ea9de549d4f3f831a0e6c620fb696dc`. Verify that archive hash, extract into a new directory and verify `CAPSULE_MANIFEST.json` member hashes. `EXPECTED.json` binds the capture manifest and expected output hashes. The included `replay.py` runs the actual CLI with an original-root read guard; pass the original project root and Vault root as its two arguments. Git provides the final full project source as recorded in REVIEW.json. No model artifacts need retrieval for this inspection.

Rollback removes only the new offline tool/test/docs via Git predecessor `8ea0cc313187ef5d768d3c60df8434facf792b48`; preserve evidence and original observation databases. Running pipeline source/configuration is unchanged.

## Exact next action

`retained_management_quote_receipts_v1`: repair the quote-receipt boundary first. Inspect `trad/oanda_practice_quote_stream.py`, `MultiPriceStream` and snapshot publication in `trad/oanda_practice_shadow_strategy_lab.py`, and `trad/oanda_quote_transport.py`. Locate the original decimal price strings before float conversion, and retain a bounded market-data-only receipt with exact instrument/bid/ask, provider market clock, actual local observation/publication clocks and source/content identity. Keep the existing quote cache compatible for current consumers. Retrieve retained raw receipts if they already exist; do not recreate historical decimals from floats.

Implement and test the parser/publisher/management-quote adapter offline before any scoped nontrading rollout. Validate long/short USD conversion using the existing quote/economic routines, stale/future/crossed/nontradeable refusal and archive restore. Do not call broker/account APIs, restart collectors, modify another task's recorder, grant management authority or invent financing defaults within that offline step. A later separately qualified retained-forecast receipt adapter and common-terminal policy contract remain required; quote compatibility alone will not make management ready.

Local raw evidence: `evidence/retained_management_20261001`. Live Vault `RETAINED_MANAGEMENT_READINESS_20261001` is the review packet. Git `vault/forex` remains a dated knowledge snapshot rather than live coordination.
