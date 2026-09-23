# Stage C — research adapters and diagnostic evidence

Read [the governing design path](../design_alignment_20260921/PATH_FORWARD.md), [causality audit](../design_alignment_20260921/CAUSALITY_AUDIT.md) and [operations audit](../design_alignment_20260921/OPERATIONS_AUDIT.md) before running this directory.

This directory contains input audits, neutral forecast/settlement records, target coverage, fitted endpoint Ridge/HGB diagnostics and small fixtures. It has progressed beyond the original input-only README, but it is not engineering-ready.

Known defects include elapsed label maturity, future-dependent forecast population, realized outcomes embedded in fitted tapes, unresolved session-calendar semantics, implicit randomized HGB early stopping, unbound cached parts and nonexclusive/overwriteable publication. The verification launcher can mask native command failures; its output is not an acceptance certificate. Existing tapes and reports must remain as historical diagnostic evidence. Do not rerun scripts that overwrite them under the same IDs.

Next work is alignment_integrity_v2: repair and test those contracts, then a dependency-bound new run and relocated fixture restore. The old hard-coded admission/retirement outputs do not retire entire model families. GPT/advisor comparisons remain deferred. Source implementation is unchanged by this README correction.
