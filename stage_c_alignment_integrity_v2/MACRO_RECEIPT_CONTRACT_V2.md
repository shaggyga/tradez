# WP6 retained source receipts and publication population

This frozen offline operation restores exact original parsed source-version and
boundary-observation bytes for the predeclared 2026-09-14 through 2026-09-20 UTC
publication interval, observed before 2026-09-21 UTC. It does not query a live
database. The acquisition selected every version carrying the retained
`source_verified=true` flag with qualifying publication/observation clocks, without
using any price, move, direction, or performance filter. This is a captured
publication population, not a complete independently verified release calendar.

The original source ledger's `reconstruct_observation` and `_clock_evidence`
functions run against an isolated in-memory SQLite database. Content, source,
version, envelope, incoming payload, sequence, lineage and clock identities are
checked. The expected collector generation is extracted without importing the
collector. Collector clock attestation remains metadata; independent publisher,
host-clock and HTTP-transport authentication has not been recovered.

Repeated observation bodies are represented by captured counts and exact first
and last sequence receipts per version/status. Intermediate receipt bodies are
not restored or independently verified by this capsule. Invalid/unproven boundary
receipts remain explicit and never acquire causal availability. SQL-unparseable
clock rows were not scanned in this acquisition. Empty unresolved_clocks.json
does not mean there are no malformed clocks in the source database.

Publication visibility uses the maximum of the exact observed receipt and its
publication, causal, numeric, detail, attachment and resolution availability
clocks. It never falls back to the publisher timestamp alone. Conflicting versions
are preserved as distinct versions; this step does not select a semantic winner.
Calendar/schedule listings are distinguished from parsed details and headlines.
Bootstrap, inferred publication clocks, missing bodies and currency mapping
provenance remain explicit. No missing value becomes neutral sentiment or no move.

Eleven hash-linked PDF files are copied from independently inventoried archive
paths. Payload-supplied paths are never opened. PDF hashes prove retained bytes,
not transport headers or the original HTTP receipt. The eleven older macro
summaries have no exact original observation match; later origin snapshots do
not change that status.

## Operator

Use the externally pinned MACRO_RECEIPT_OPERATOR_RECIPE.json SHA from the Vault.
`macro_receipt_operator_v2.py status|run|resume|verify --recipe <recipe>
--recipe-sha256 <external SHA> --inputs <frozen inputs> --runs-dir <runs>`.
Do not regenerate the recipe during routine operation. Source, input and runtime
drift returns review_required. Publication reuses the existing single-writer,
identity, immutable-payload and process-death recovery implementation. A live
owner cannot be reclaimed. Completed runs verify without rewriting artifacts.

The existing portable checkpoint reader retains its 8 MiB/member and 32 MiB/total
limits. Larger exact JSON inputs are stored as deterministic gzip data members.
The dedicated receipt consumer caps each expanded JSON at 32 MiB and their total
at 64 MiB, validates exact expanded byte hashes before parsing, and never treats
compressed content as paths or executable code. Only the separately pinned
original source_ledger.py is executed. Eleven named PDF inputs remain plain data.
The restored run must reproduce all five payload hashes. Tests and independent
review are separate statuses.

## Next evidence gate

Recover a matched release/event identity and price-support population for this
fixed cohort, including observed no-move cases and explicit no-quote cases.
Do not promote headline/listing, bootstrap, inferred-clock or unproven receipts
into release-time forecasts. Complete release calendars, causal extraction
latency, matched forecast ablation and meaningful performance evidence remain
unproven. GPT/advisor comparisons, paid calls, broker/service/account actions and
D-drive investigation remain deferred. No models are fitted, no orders emitted,
and no forecast improvement or full-design completion is claimed by this step.
