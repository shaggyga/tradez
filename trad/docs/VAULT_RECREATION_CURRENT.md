# Recreate and inspect Forex from the vault

This is an inspect-only guide for the September 8, 2026 documentation snapshot, not a live runtime observation or permission to start the bot.

Start with the [system guide](SYSTEM_GUIDE.md), then use the [complete knowledge index](KNOWLEDGE_INDEX.md) and [feature-generation dictionary](FEATURE_DICTIONARY_CURRENT.md). The [worked example](FEATURE_GENERATION_WORKED_EXAMPLE.md) illustrates actual arithmetic using invented inputs.

## Three things kept separately

- **Vault records:** explanations, source declarations, contracts, results, audits and history, indexed by [the shared-state manifest](SHARED_PROJECT_STATE_CURRENT.json).
- **Source snapshot:** the exact source/configuration/tests/docs listed by [the latest worktree pointer](source/WORKTREE_SOURCE_LATEST.json). Its ZIP members are relative to `trad`; strip only the leading `trad/` from a shared-state source path when looking for its archive member.
- **External state:** private credentials, complete runtime databases/WALs, original data arrival clocks and some historical fitted artifacts or missing source versions are not automatically in the vault. Their absence is explicit, not something a reviewer should infer or synthesize.

## Vault-only inspection sequence

1. Read the system guide's purpose, pipeline, current-versus-historical distinctions and safety boundaries.
2. Verify the source pointer's manifest SHA-256 and ZIP SHA-256 against the named local files. These hashes establish byte consistency, not trust in arbitrary executable code.
3. Inspect the manifest and archive inventory. Every ZIP member must be a safe relative path. Inspect source directly in the ZIP, or extract to a new empty directory that is neither the live project nor the vault. Never overwrite live source.
4. Read the extracted code and configurations before executing any tooling. The source verifier `tools/vault_worktree_snapshot.py --verify <manifest>` does an offline hash/extraction/syntax round trip; it does not import models or start account processes. Syntax success is not an integration or profitability test.
5. Match each research result to its own registration, source version, input/label/cost rules and original observation period. A source-only extraction cannot reproduce excluded prospective ledgers. Exact historical reproduction needs coherent original backups; later downloads do not recreate first-seen clocks.
6. Use [the readability report](VAULT_READABILITY_REPORT.json) for missing references, source-layout link aliases and external dependencies. The report is generated from the vault alone and does not inspect machine-local paths.

The exact dependency inventory is [RESEARCH_DEPENDENCIES_CURRENT.txt](RESEARCH_DEPENDENCIES_CURRENT.txt); it describes a dated environment, not universal compatibility. Narrower source profiles are listed in the system guide. Keep any later test environment isolated and credential-free; do not launch the supervisor, install scheduled tasks or run old execution examples merely to inspect the project.

## Historical descriptions are preserved

The earlier recreation page mixed dated runtime updates with recovery instructions. It is preserved at [recreation history](RECREATION_HISTORY_THROUGH_20260907.md), with its old observations intact. Its running/stopped states, monitor descriptions and counts are not current instructions.

The newest exported narrative describes research collection with orders/promotion disabled. A vault export timestamp is not proof the processes are presently alive, feeds are fresh or account values unchanged. Consult the dated evidence in [the research index](RESEARCH_INDEX.md); a new runtime assessment requires the actual computer.

No historical source was restored, model refitted, runtime started/stopped or credential copied as part of this documentation repair.
