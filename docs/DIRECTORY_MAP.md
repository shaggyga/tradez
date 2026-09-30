# Workspace map

| Location | Purpose and handling |
|---|---|
| `README.md`, `START_HERE.md`, `FOREX_HANDOFF.md` | Short entry points; current state comes from live Vault pointers |
| `trad/` | Preserved runtime/source, tests and long-form project/pending logs |
| `stage_c_alignment_integrity_v2/` | Current engineering sources, contracts and frozen recipes |
| `stage_b_20260921/`, `stage_c_all68_20260921/` | Retained prerequisite sources and historical diagnostics |
| `design_alignment_20260921/` | Original design audit; current interpretation is selected by the Vault pointer |
| `direction_*`, `currency_meter_continuous_20260912/` | Preserved reusable source families |
| `tools/`, `tests/`, `artifacts/`, `docs/` | Shared operational tooling, tests, artifact catalogs and setup docs |
| Dated audit/evidence/run directories | Historical or active evidence, according to their receipt and ownership claim |
| `docs/history/` | Verbatim older entry documents; never use their old "current" labels as live status |
| `git_publication_20260923/legacy-trad.git` | Retained original Git metadata for rollback; keep it |
| Shared Vault | Queue, claims, review packets, immutable artifact/checkpoint references and larger documentation |
| `vault/forex/` | Versioned Forex Vault knowledge copy; reading snapshot, not live coordination or full data storage |

Paths in frozen recipes and manifests are part of their provenance. Organize by this
index before moving source or data directories. Keep runtime databases, credentials,
machine paths, generated data and fitted weights outside Git. New source files require
explicit review if an old generated-evidence directory is ignored by Git.

Verified temporary checkout removal has its own receipt. A dated folder name or an
ignore rule alone never proves that an entire directory is disposable.
# Current orientation

Start with [PROJECT_STATE.md](PROJECT_STATE.md) for current findings, prior model
evidence, source/forecast locations and exact scope limitations. The
[research evidence index](../artifacts/research_evidence_index.json) provides hashes
and Vault-relative references. Historical folders remain intact.
