# Rolling-source outcome wrapper

`score_curve_from_captures(curve, publication, consumption, candle_captures, *, expected_source_bindings, metadata, clock=time.time, entry_quote=None)` preserves the single-capture API and produces the same report schema with one record per original node/view. It accepts1–4,096 independently attested captures for one instrument. No raw source domains are merged.

For each exact nominal or retained-training view, it selects the earliest decisive original source observation by attested read_completed_epoch, then raw source and source receipt hashes. A source is decisive if it supplies the first eligible complete target bar or covers the full requested interval while reporting none. Choice uses clocks, domain coverage and row presence only; no price, sign, error or profitability selects the source. The earliest decisive missing window remains missing even if a later response contains a favorable bar.

Offline-derived capture seals must use the actual evaluation-time observation clock. That clock is retained independently and is never backdated to the original raw source receipt. Each selected view carries original source-read clocks, actual derived capture observation, source/query/receipt/capture hashes, and exact first/last covered labels. Identical mappings of the same raw+receipt observation collapse to the earliest actual derived seal; contradictory mappings with the same source identity are withheld.

No decisive capture produces explicit no_decisive_source_capture with the observed domain reasons. Invalid/future/mismatched evidence withholds the report. Duplicate input-derived records do not inflate horizon denominators. The report includes selected_source_captures, source_selection_policy, source_selection_uses_outcome_values=False and input/unique/selected counts. Single-capture top-level fields are replaced by this per-view provenance.

The caller should supply only verified same-instrument source responses whose domains are relevant to the native targets. The offline orchestrator may index those domains to avoid revalidating unrelated history. Original context/convention, probability-event mismatch labels, optional current-entry diagnostic and all existing missingness rules remain unchanged.

Final focused suite:54passed. The earlier47-case acceptance and review remain preserved as the pre-wrapper stage. These fixtures validate mechanics, not actual prediction success.
