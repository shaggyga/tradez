# Macro text-layer review — changes requested

Reviewed MACRO_TEXT_LAYER_20260922_135019 against design WP6/sections8.1,14.2–14.4 and its stated contract. This is a same-implementer review; independent review remains unperformed. The earlier seven-versus-eight retained-summary documentation correction remains valid.

## Findings

**MT-R01 (P2), macro_text_layer_v2.py:42–43:** clause splitting treats the decimal point in “If inflation exceeds2.5%, the policy rate will be raised” as a sentence boundary. Actual guidance is+1.0 and conditional_only=false; the equivalent cut case returns-1.0. The fixed contract requires conditional marking and an absolute0.35 cap. Preserve decimal punctuation and conservatively retain context; add numeric/abbreviation/sentence-boundary regressions.

**MT-R02 (P2), macro_text_layer_v2.py:105–107:** same-event/class/currency variants with identical availability are sorted by document hash and the last one silently wins. Two contradictory summaries produce opposite stance signs by changing only a mirror URL (suffix26 versus27 in the preserved fixture). No source/version priority establishes that ordering. Preserve identical mirrors, strictly later revisions and unresolved conflicting variants distinctly; unresolved conflicts must abstain and retain all evidence identities. Test permutation and metadata invariance, and prevent ambiguous values from generating downstream change.

## Checks and scope

All249 active source files match the sealed snapshot. Both pointer manifests and the original completed operator verify. Four relevant existing tests pass (21 deselected); they do not cover the new failures. The bounded reproduction confirms two decimal-condition failures and the URL-dependent conflict. Its initial20-URL probe exposed the conflict but did not happen to produce both winners; the preserved deterministic follow-up finds both by suffix27. This is adversarial software evidence, not market selection or new financial results.

No active implementation, recipe, checkpoint or source data was edited. Existing25/25 test receipts remain historical evidence but do not establish acceptance against these additional cases. No live, broker, API or D-drive action. No forecast improvement or full WP6 completion claim.

## Exact next

**repair_macro_text_clause_and_revision_ambiguity_v2**. Repair these two findings as one corrective work package, retain the original evidence, issue a new pinned recipe/run identity, run new regressions and the consumer/recovery suite, restore the corrected checkpoint, and publish the code/evidence diff. Do not alter old immutable outputs or simply change expected test values. After that checkpoint review, resume **macro_document_receipt_and_all_release_population_v2**. The broader design order and older warm partial remain preserved.
