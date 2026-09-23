# Export credential scanner performance repair — September 7, 2026

The shared source-export credential scanner now handles long encoded evidence without repeatedly searching the same identifier run. The operational dashboard validation remains unchanged: receipt `7c07ec243ca3d822a4bb56118482247a84e7545c343a515214f1d340bf8c76eb`, all 87 bound source/evidence files, and all 149 already-copied canonical vault records were verified before this separate follow-up was written. This report and its receipt are deliberately outside the canonical mapping.

## Cause and scope

The first operational publisher copied the 149 records and preserved the previous records, then spent more than 16 minutes in its initial source credential scan. The new minute-gap source capture contains a 252,696-byte uninterrupted encoded identifier run. The old assignment expression retried an unbounded greedy key prefix at each byte, producing quadratic work on that run. At 18:58:26 UTC the verified publisher child was stopped; its matching launcher exited. No other process was targeted.

The earlier observation that staging had not begun was stale by termination. The first resume preflight later found a ZIP created at 18:58:02 UTC and a manifest written at 18:58:04 UTC. This interrupted stage contains 1,985 files and the prior scanner; no successful verification or publication receipt exists for it. The ZIP and manifest were moved without deletion to `maintenance/interrupted_source_stage_20260907_185802`, with their exact hashes and member timestamps retained. The published 885144 archive and pointer remained unchanged. The first resume stopped before any publication writes.

The superseded scanner report and receipt (SHA-256 `36bf41b54c69908151e27d56ee42aca9c9ffc208554b7a2fcc75b3476c6b668d`) are preserved separately in workspace evidence and vault maintenance. The original interruption record is retained as written; the timestamped orphan-stage preservation evidence corrects its pre-staging assumption. A metadata-only correction also retains the initial preservation receipt: a lazy directory timestamp read after movement returned sentinel dates, while all member timestamps/hashes were valid; directory times now agree with both pre-move inspection and the preserved directory.

Only the greedy API-key/token branch now requires the beginning of an identifier run, with the assertion after the optional opening quote. The other alternatives retain their existing suffix matches. Replacing that expression with the prior expression makes the scanner AST identical to its saved predecessor. No detection rule, path restriction, fixture exemption, reference exemption, or archive check was removed or relaxed.

The supported equivalence is the scanner's actual full-payload `finditer` call pattern. Explicit starting positions inside an identifier are outside this equivalence claim and are not used by the scanner. Match spans, captured values, and Python-reference classification were compared using constructed data; no private credential file was read for these tests.

## Validation

- Final targeted suite: 64 pytest cases passed, with no failures, errors, or skips.
- Root's broader impacted scanner/source-vault regression suite: 54 pytest cases passed, with no failures, errors, or skips. These runs may overlap and are not added together.
- The first broader-suite invocation used the project subdirectory and failed test collection because the `trad` package was not importable from that working directory. Its original collection-error XML is preserved. Running from the Forex parent directory resolved this harness-only error; no product source changed.
- Independent generated comparisons: 50,736 full-payload span/capture cases and 1,728 Python-reference cases, with zero differences. These are generated comparisons, not additional pytest cases.
- The initial 132-case run is retained: 131 passed and one newly added test's own source failed the existing archive audit. The synthetic fixture's string construction was corrected; no scanner rule or exemption changed. The existing 68 credential/snapshot cases passed on the same final scanner source.
- A constructed 252,696-byte encoded run took approximately 0.056 seconds for the assignment scan and 0.077 seconds through all six audit rules on this machine. A credential-shaped alert placed after the run was still detected in approximately 0.063 seconds. The old expression was not rerun on that large input, so no measured speedup ratio is claimed.

The saved prior scanner, final implementation receipt, both targeted XML runs, independent review, synthetic timing result, broader regression XML, root review, interruption evidence, and exact source-only resume helper are retained under `docs/validation/export_scanner_optimization_20260907` and bound by the separate scanner receipt.

## Source-only export continuation

The resume helper checks the unchanged operational receipt and all 87 bindings, the source and target bytes of all 149 already-copied records, the original backup, the three preceding receipts, and the preceding archive `forex_worktree_source_885144f85e85de600e4470d8.zip` with SHA-256 `dbdcd79d73a761c7b8f0e1d591d483878c05eb973b2668e0913e7e5e06e3881e` before publication. It also checks its own exact source copy against this scanner receipt.

It then resumes only source-archive construction and the remaining extraction, source-hash, compilation, credential, backup, and local-record verification. It does not repeat canonical writes or recreate the backup. Successful completion is recorded separately in vault maintenance, including verified copies of this unmapped report and its receipt. This pre-export validation does not claim that the resumed archive has already succeeded; the maintenance export receipt records that outcome.

Only local OneDrive files are verified; cloud synchronization is not observed. Private databases and credentials remain outside the source archive. This export performance repair does not change the registered studies, runtime trading authorization, forecasts, or prediction-performance claims. Orders remain disabled.
