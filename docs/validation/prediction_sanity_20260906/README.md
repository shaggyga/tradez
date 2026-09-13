# Frozen sanity-check evidence

The authoritative broad-signal result is broad_signal/broad_signal_final_verification.json.
The preliminary raw_verification JSON is preserved only because it contains the original
extraction metadata and is an input to the finalizer; its suffix-based pip assumptions
and derived pip means were superseded. Read the final report's explicit correction.

selected_rows.txt retains the original JSONL bytes under an archive-supported extension.
For a portable read-only count check from the source root:

```powershell
python -B docs/validation/prediction_sanity_20260906/recheck_broad_counts_offline.py docs/validation/prediction_sanity_20260906/broad_signal/selected_rows.txt
```

Expected: 8414 rows, 4148 exact direction hits, 296 flats and 716 positive bid/ask outcomes.
Other acquisition scripts record their original local paths and output locations; do not
run them in the sealed evidence folder. Use disposable copies and explicitly adapt paths
when reproducing them. No production database is needed for the portable count check.
The original four-family raw extract is under ../fixed_evaluation_20260906.
