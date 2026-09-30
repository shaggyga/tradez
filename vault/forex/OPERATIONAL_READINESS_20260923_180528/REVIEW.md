# Operational cleanup and readiness checkpoint

Offline implementation and independent review are complete within this packet's scope.
Final Git publication, offline bundle and clean-checkout preflight are recorded separately
in `SHARED_GIT_REMOTE_LATEST.json`; check that receipt before treating publication as finished.

Five redundant verified checkouts were removed (807,022,922 bytes). Original models,
evidence, bundles and old Git metadata remain. Entry documents now point to one current
navigation page; exact older bytes and all project-log history remain available.
Completed coordination rows were moved to history, preserving ownership records.

Five catalog groups cover 120 saved model payloads/JSON parameter records, not 120
independent experiments. Two packages make 98 existing joblibs and all their completed
outputs retrievable without refitting. Eight historical directional payloads and six
macro JSON parameter records retain documented manual byte-copy handling.

Read-only preflight now verifies source/document/queue identity, revision, ownership,
the exact current operational prerequisite, environment and requested saved artifacts.
Independent review found and resolved GIT-REVIEW-01 (omitted Git queue record) and
OPS-PREFLIGHT-01 (an old completed gate could satisfy newer required work). Original
findings and separate resolution/retest evidence are preserved. Test and review identities
are in REVIEW_INPUTS.json and REVIEW.json. No model was loaded or fitted.

The read-only runtime observation reports degraded operation: supervisor/watchdog absent,
17 of 18 heartbeats stale, and the remaining fresh capture partial/unavailable due to
stale clock state. See evidence/runtime_observation/RESULT.md for exact timestamps and
recovery prerequisites. No service was started, stopped or reconfigured.

The full engineering design, 369 current scientific source files and all 55 scientific
queue records/order are unchanged. Forecast and policy acceptance are unchanged; demo
authorization remains ungranted. Offline readiness is not model or trading authorization.

Exact next scientific item: `macro_currency_meter_numeric_evidence_join_v2`. Return at this checkpoint;
resume that item only when the user asks. GPT/advisor comparisons remain deferred.

## Restore and resume

Read CURRENT_STATUS.md at the Vault root, then its live pointers and the final Git receipt.
Restore the bundle identified there using `git -c core.longpaths=true clone --branch forex
<bundle-path> forex`; verify its SHA256 first. Use this packet's saved_artifacts archives
through the Git registry and original run identities. Do not run old refitting recipes
merely to reconstruct already saved models. On a clean matching checkout run
`python -I -B tools/forex_preflight.py --vault <local-vault-path>`.

For rollback/recovery preserve current edits first. The before_authority and before_documents
snapshots, verbatim project history and baseline Git commit identify the previous state.
Do not overwrite sealed packages or a newer live pointer; create a reviewed successor.
The deleted checkouts contained only exact tracked source/Git metadata and can be recreated
from the retained commits/bundles. Their full retirement inventory remains local and is
hash-bound by cleanup/LOCAL_INVENTORY_REFERENCE.json. WORK_LOG/PENDING_CHANGES track this step.
If publication is interrupted, preserve the partial package and live edits. Compare each
file to PUBLICATION_AUTHORITY.json and the sealed local packet_draft; do not rerun --publish,
delete the partial package or replace a newer pointer automatically. Resolve mismatches
and complete a reviewed recovery/addendum with exact before/after identities first.
