# Integrity publication consumer review and export repair

Review date: 2026-09-06. Actual project: `C:\Users\zmoor\Documents\forex\trad`. The project remained stopped. This work used source reads and disposable offline fixtures only; no production SQLite database, private credential, broker interface, network service, or live vault/checkpoint was accessed. Earlier audit and repair evidence was not edited.

## Result

Compacting only the five alignment `episode_rows` lists preserves the discovered current-reader requirements when all other fields remain inline. The full original rows remain reconstructable through the shared content-addressed artifact helper. Two required integration changes are implemented: checkpoint export includes the exact verified reference closure, and saved four-hour observer records retain an explicit portable locator for the original integrity snapshot directory.

Guarded regression run: **38 passed in 3.31 seconds**, no failures or skips. JUnit: `integrity_consumer_tests.xml`; runner: `run_consumer_tests.py`. This includes 12 new export/observer cases, the existing vault synchronization tests, and the existing four-hour observer tests.

## Consumer inventory

| Consumer | Requirement and evidence |
| --- | --- |
| Supervisor | `oanda_always_on_supervisor.ps1:3405` retains the existing current/history/report locations. `:3412` uses the current JSON for liveness and `classification_version` identity. Existing timestamp/classification fields must remain unchanged. |
| Improvement control engine | `oanda_improvement_control_engine.py:59` reads current integrity; `:110` and following logic use `status` and the complete `failures` list to preempt research branches. These fields remain inline. |
| Four-hour observer / saved history | `oanda_four_hour_best_improvement_pass.py` embeds the whole integrity object in its snapshot and appends that snapshot to `reports/four_hour_best_improvement_pass_20260808/best_improvement_watch.jsonl`. Branch selection, comparison and human reports use `status`/`failures`. Its new `integrity_snapshot_source` locator and `restore_snapshot_integrity(..., project_root=...)` preserve historical detail resolution after relocation. The caller supplies the restored `trad` root; JSONL parent is never inferred as the detail base. |
| Week-to-date saved-report reader | `oanda_week_to_date_project_event_move_recap.py:62` captures the current file. `:979` projects only `generated_utc`, `status`, and `failures`, with raw upstream artifact hashing and cutoff classification. It does not read nested alignment rows from integrity. |
| Audit's next cycle | Previous integrity JSON supplies drift counters and full-database attestation metadata. `measurements`, `checks`, `audit_finished_utc`, and the existing component attestation structures must stay inline. The change retains them. |
| Alignment validators | `oanda_project_integrity_audit.py:1583` and `:2496` use complete `episode_rows` to check causal/proof details. Those validators still receive the unchanged standalone input reports loaded in `run`; compaction happens only inside owned publication after verdict computation. Existing validator tests consume full synthetic alignment objects, not compact references. |
| Vault model checkpoint | The generic collector excludes data/reports; current integrity is included explicitly. The export repair now adds only verified artifacts referenced by that explicit current snapshot allowlist. The ZIP preserves both the snapshot's original relative member path and its neighboring detail hierarchy. |
| History rotation / log archive | Integrity JSONL parts are preserved byte-for-byte by existing rotation. New compact history rows keep references. Detail files must remain available for as long as any referring history/saved snapshot is retained; no automatic artifact deletion was added. History readers must pass the original integrity snapshot directory explicitly. |
| Dashboards and trade authorizers | Scoped searches across project Python, PowerShell and web source found no direct dashboard or trade-authorizer read of current integrity alignment rows. Integrity remains research-only/no-trade. This negative result covers available source, not unknown external/manual readers. |

## Implemented compatibility safeguards

`forex_model_vault_sync.py:1109` captures the exact included integrity JSON bytes and uses the shared `verified_detail_artifact_bytes` API. The API verifies reference schema, canonical relative path, containment, SHA-256, byte count, envelope schema and row count using the same bytes it returns. Missing or corrupt details abort collection/export; no reader regenerates or repairs source evidence.

`write_zip` repeats this coherent capture at publication preflight (`:1443`) because current integrity may advance after `collect_files`. Its archive member set is updated to that captured generation's exact closure, stale dependencies of the previous generation are removed from the candidate member set, and the exact captured bytes are streamed into the ZIP. Live current/detail changes during ZIP streaming cannot mix generations. Source files are not overwritten by export.

The artifact allowlist is narrow: only `trad/data/oanda_training_manager/state/project_integrity_audit_v1.json` can add dependencies, and only its validated references are included. There is no recursive detail-directory sweep and no arbitrary reference following from other JSON.

The four-hour observer's new source locator is relative to an explicitly supplied restored project root. Its restore helper expands the embedded historical integrity bytes without reading a newer current snapshot. Legacy full snapshots remain readable. Missing or altered compact locator metadata fails closed. Existing status/failure branch logic is unchanged.

## Shared compact publisher review

`oanda_integrity_publication.py` copies publication dictionaries, omits only nonempty row arrays in the five named sections, preserves every other field, and writes canonical UTF-8 row-envelope artifacts. Row order and values survive round-trip; hashes intentionally identify row content, allowing reuse when unrelated timestamps/counters change. The inline integrity document remains the identity-bearing summary, and normal upstream raw-file hashes bind that complete compact document.

`oanda_project_integrity_audit.py:471` builds and verifies detail artifacts after checking current publication ownership and before current JSON, Markdown or history publication. Thus an artifact error cannot publish a dangling reference or alter those current pointers. Current JSON, Markdown and history remain separate filesystem operations; this review does not claim a transaction across all three. No raw alignment report, production ledger, frozen receipt, or earlier history is rewritten.

## Validation and limitations

The new cases exercise legacy full input, scoped dependency inclusion, missing/corrupt/path/hash/size/count/schema failures preserving an old checkpoint, generation changes between collection and export, source changes during ZIP streaming, ZIP relocation and exact detail restoration, and four-hour saved-record restoration with an explicit root and no current file present. Selection logic still reacts to the same integrity failure.

An initial test-run configuration attempted pytest's default log path; the guard blocked that write before collection. The runner now explicitly locates its log inside this evidence directory. An early fixture run hit Windows MAX_PATH before export at long artifact temporary names (11 failures, 27 passes); fixture roots were shortened. The helper owner also removed the redundant 64-character hash from temporary filenames. These were resolved test/setup and path-portability issues, not waived failures or production mutations.

The source-only review did not execute the actual vault synchronization or regenerate production integrity. Full checkpoint retention/import policy is unchanged. Historical integrity details are not automatically swept into a checkpoint merely because they exist; only the current included snapshot's verified closure is exported. A separately exported historical JSONL/embedded snapshot requires its own explicit original-directory locator and referenced closure.

## Handoff identities

Before root adds canonical documentation mappings: `forex_model_vault_sync.py` SHA-256 `554916A174C874BAD924950F416AA97EFFC9271EFD6BD88FC4D7689B3CD29251`; four-hour observer `019608748072C401BEF05DC2DD3621F75069ABC5E787EA86B4AE51970B126473`; new export/observer tests `743383EB3E262C17EE377831918F434016D729B3C179EEEC78ADB7C7CACA9801`. Root owns later exporter mapping edits; execution agent owns the shared helper and compact publisher.
