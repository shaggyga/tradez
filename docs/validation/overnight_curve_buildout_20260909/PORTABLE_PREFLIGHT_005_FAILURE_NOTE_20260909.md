# Portable evidence preflight 005 — failed before copy

The root invoked the accepted `prepare_portable_evidence_v1.py` in preflight mode with eight explicitly pinned selections. It failed with `ValueError: source_outside_declared_root` while validating selected payloads. No project copy or vault publication was requested or performed. The helper does not create its success receipt on this failure; the intended `PORTABLE_EVIDENCE_PREFLIGHT_005_20260909.json` was not produced.

The call occurred between the observed root clocks 2026-09-09 11:07:25 and 11:10:45 UTC. These are bounds, not a reconstructed exact start time. The original tool output remains in task history.

The selected news/capacity V1 manifest referenced four original September 8 validation/review/XML files outside the overnight evidence root and one current project test source. Those exact originals were legitimate historical evidence but were outside this curator's permitted payload roots. The curator's scope was retained. The model-input agent copied the five exact byte sequences beneath the overnight evidence root, preserved their original hashes and provenance in a transition receipt, and prepared a new V2 selection. Original evidence and the V1 selection were left unchanged.

The corrected V2 selection is `CURATED_NEWS_CAPACITY_REFRESH_SELECTION_V2_20260909.json`, SHA-256 `40bdf69d872afdbea8871f8bc33e3e7a5106ab99ea2ead28032d10dcca448aea`. A fresh complete preflight is still required before the final copy.
